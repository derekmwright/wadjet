// Package expr provides a typed expression engine for evaluating SQL expressions
// against record batches. It replaces the string-based expression parsing with
// a compiled expression tree built from the SQL parser AST.
package expr

import (
	"encoding/base32"
	"fmt"
	"math"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/publicsuffix"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// Expr evaluates an expression against a record batch row, returning a typed value.
type Expr interface {
	Eval(b *batch.RecordBatch, row int) any
}

// BoolExpr evaluates a boolean expression (used for WHERE/HAVING/JOIN
// conditions). SQL's logic is THREE-valued and EvalBool is the two-valued
// COLLAPSE a filtering context applies: it answers true only for TRUE —
// FALSE and UNKNOWN rows are both kept out of a WHERE. The third value is
// carried by BoolNullExpr, and the two protocols must agree:
// EvalBool ≡ (val && !null) of EvalBoolNull.
type BoolExpr interface {
	EvalBool(b *batch.RecordBatch, row int) bool
}

// BoolNullExpr is the three-valued boolean protocol (#370): val is the
// answer and null reports UNKNOWN, in which case val is meaningless. Every
// boolean operator implements it — it is what lets NOT distinguish UNKNOWN
// (stays UNKNOWN, row excluded) from FALSE (becomes TRUE, row kept), and
// what a projection boxes into SQL NULL.
type BoolNullExpr interface {
	EvalBoolNull(b *batch.RecordBatch, row int) (val, null bool)
}

// evalBoolNull evaluates any expression on the three-valued protocol.
// Expressions without a native implementation (a boolean column, a function
// call) go through the boxed path, where nil is the UNKNOWN.
func evalBoolNull(e Expr, b *batch.RecordBatch, row int) (bool, bool) {
	if te, ok := e.(BoolNullExpr); ok {
		return te.EvalBoolNull(b, row)
	}
	v := e.Eval(b, row)
	if v == nil {
		return false, true
	}
	return toBoolVal(v), false
}

// boolNullBox is the one place the three-valued answer becomes a boxed SQL
// value: UNKNOWN is NULL.
func boolNullBox(val, null bool) any {
	if null {
		return nil
	}
	return val
}

// VecExpr evaluates an expression for an entire batch at once, writing results
// directly to the output vector. This avoids per-row interface dispatch and boxing.
type VecExpr interface {
	EvalVec(b *batch.RecordBatch, out *batch.Vector, n int)
}

// stringInputFuncs are scalar functions whose arguments are string-typed.
// A TypeDate ColRef argument evaluates to its raw epoch-day int64 (the
// representation every comparison/arithmetic path depends on), so these
// functions must render it through batch.FormatDate first — otherwise
// SUBSTR(date_col, 1, 4) substrings the DIGITS of the day number
// (issue #273: SF100's date32 columns grouped Q07/Q08/Q09 day-granular;
// string-date test data never exercised the path). A TypeIPv4/TypeMAC
// ColRef argument has the identical shape — a raw encoded int64, not the
// dotted-quad/colon-hex address text CAST and every other function-argument
// site render — so FuncCall.Eval also runs formatNetworkArgs for every entry
// here (#500: `length(ipv4_col)` answered the DIGIT COUNT of the address's
// raw number). Keyed lowercase, the registry's convention. Timestamps are
// excluded deliberately: they have no canonical string form today (GetValue
// emits raw epoch-ms), and these functions must stay consistent with
// result-output rendering.
var stringInputFuncs = map[string]bool{
	// The `||` operator is here for the same reason `concat` is: it reads
	// every argument as text, so an IPv4/MAC/DATE column on either side has
	// to be rendered rather than read as its raw encoded integer. Splitting
	// it out of `concat` for #609 would otherwise have silently taken that
	// rendering away from `ipv4_col || '/24'` (#500's defect, restored).
	ConcatOpFunc: true,

	"upper": true, "lower": true, "concat": true, "length": true,
	"len": true, "substr": true, "substring": true, "trim": true,
	"ltrim": true, "rtrim": true, "replace": true, "reverse": true,
	"left": true, "right": true, "starts_with": true, "ends_with": true,
	"contains": true, "split_part": true, "strpos": true, "lpad": true,
	"rpad": true, "cast_string": true,
	// `format` renders its arguments, so a TIMESTAMP/DATE/IPv4/MAC column
	// reaching it must arrive as the text the wire carries rather than as its
	// raw box: `FORMAT('%s', c_ts)` answered `%!s(int64=1700000000000)` — the
	// Go verb's own complaint about being handed an integer — where
	// PostgreSQL renders the instant (#544). What this does NOT fix is that
	// the format string is read with Go's verbs rather than PostgreSQL's
	// %s/%I/%L, which is a separate and much wider divergence.
	"format": true,
}

// typedArgPositions names, for a stringInputFuncs entry, the argument
// positions that are NOT read as text — a count, an index, a width. Every
// other position of every other entry IS read as text, including all of
// concat's, which is variadic.
//
// The distinction is load-bearing at the vec dispatch in FuncCall.EvalVec: a
// text position must hold a byte-array-shaped vector before the kernel may
// index its offsets, and a typed position must NOT be checked that way or
// SUBSTR(s, 1, 4) would lose the vec path to its integer arguments. Getting
// that split wrong is not a wrong answer, it is a dead server (#509): the
// kernel indexes an offsets array a BIGINT column does not have.
//
// Absent from this table means "every argument is text". That default is the
// safe direction — the worst it costs a mis-classified function is the
// per-row path.
var typedArgPositions = map[string]map[int]bool{
	"substr":     {1: true, 2: true}, // (text, start, length)
	"substring":  {1: true, 2: true},
	"left":       {1: true}, // (text, count)
	"right":      {1: true},
	"split_part": {2: true}, // (text, delimiter, field)
	"lpad":       {1: true}, // (text, width, fill)
	"rpad":       {1: true},
}

// networkTextFuncs are the scalar functions whose bodies parse an argument
// as network-address TEXT — every function that reaches net.ParseIP,
// net.ParseMAC, net.ParseCIDR, or (mac_format) hand-rolled hex-with-
// separators parsing on a stringified argument. ColRef.Eval boxes a
// TypeIPv4/TypeMAC column as its raw encoded int64 — the representation
// arithmetic, GROUP BY keys and column-to-column ordering comparisons all
// depend on, exactly as TypeDate's epoch-day int64 does for
// stringInputFuncs above — so a function in this set that reads a typed
// network column argument saw a decimal digit string instead of a
// dotted-quad or colon-hex address and silently answered NULL (cidr_contains,
// ip_to_string, mac_vendor_oui, ...). Keyed lowercase, the registry's
// convention. TypeIPv6/TypeCIDR/TypeUUID columns are not affected: ColRef.Eval
// already renders those through Vector.GetValue's default case — for a
// FUNCTION ARGUMENT, which is all this registry is about. That is not a
// claim about comparison ORDERING against those three: see CmpNetworkLit's
// doc and tryNetworkLit (compile.go) for why `<`/`>` against a literal is a
// separate, still-open question (#492) that this rendering does not answer.
var networkTextFuncs = map[string]bool{
	"broadcast_address": true, "cidr_contains": true, "cidr_overlap": true,
	"cidr_to_range": true, "flow_direction": true,
	"geoip_asn": true, "geoip_city": true, "geoip_continent": true,
	"geoip_country": true, "geoip_country_name": true, "geoip_latitude": true,
	"geoip_longitude": true, "geoip_org": true, "geoip_postal_code": true,
	"geoip_subdivision": true, "geoip_timezone": true,
	"hosts_in_cidr": true, "ip_add": true, "ip_between": true, "ip_diff": true,
	"ip_in_range": true, "ip_netmask": true, "ip_subnet": true,
	"ip_subtract": true, "ip_to_hex": true, "ip_to_int": true,
	"ip_to_string": true, "ip_version": true,
	"ipv6_compress": true, "ipv6_expand": true, "ipv6_scope": true,
	"ipv6_to_eui64": true, "is_6to4": true, "is_ipv4": true, "is_ipv6": true,
	"is_link_local_ip": true, "is_loopback_ip": true, "is_multicast_ip": true,
	"is_private_ip": true, "is_reserved_ip": true, "is_teredo": true,
	"mac_is_local": true, "mac_is_unicast": true, "mac_to_string": true,
	"mac_vendor_oui": true, "mac_format": true, "mask_ip": true,
	"network_address": true, "prefix_length": true, "reverse_dns": true,
	"same_subnet": true, "sixto4_gateway": true, "teredo_client": true,
	"teredo_server": true,
}

// temporalInputFuncs are the scalar functions that read an argument as an
// INSTANT — every function whose body reaches parseTime/toTime/parseDateArg.
// They are the counterpart of stringInputFuncs and exist for the same reason:
// ColRef.Eval
// boxes a temporal column as a bare number (epoch DAYS for TypeDate, epoch
// MILLISECONDS for TypeTimestamp), and a bare number has lost its unit.
// parseTime reads an int64 as SECONDS, so 9568 days became 9568 seconds and
// YEAR(l_shipdate) answered 1970 for every row of a decade — no error, no
// null, one bogus GROUP BY bucket (issue #319).
//
// The unit cannot be recovered downstream: distinguishing days from seconds by
// magnitude is a guess, and a wrong guess is worse than the current bug. It
// CAN be recovered here, where the argument is still a column reference whose
// vector knows its own type — so this family, and only this family, resolves
// its column arguments through columnInstant before the function runs.
//
// Keyed lowercase, the registry's convention. Deliberately excluded:
// from_unixtime / timezone_hour / timezone_minute, whose argument is a number
// in its own right, not a column instant.
var temporalInputFuncs = map[string]bool{
	"year": true, "month": true, "day": true,
	"hour": true, "minute": true, "second": true,
	"quarter": true, "week": true,
	"day_of_week": true, "day_of_year": true,
	"last_day_of_month": true,
	"date_trunc":        true, "extract": true,
	// time_bucket reads its SOURCE and its ORIGIN as column instants for the
	// same reason date_trunc does: a TIMESTAMP column boxes epoch
	// MILLISECONDS and a DATE column epoch DAYS, and parseTime reads a bare
	// int64 as epoch SECONDS. Without this entry every bucket over a
	// timestamp column would land in 1970 (#319's shape).
	"time_bucket": true,
	"epoch":       true, "to_unixtime": true, "date_format": true,
	"at_timezone": true, "timezone": true,
	// The date-arithmetic family, held back from the #319 fix and settled
	// by issue #322. It reads its date through parseDateValue, which takes
	// a bare int64 as epoch DAYS: right for a DATE column, and off by a
	// factor of ~86.4 million for a TIMESTAMP column, which boxes epoch
	// MILLISECONDS. Resolving the column here retires the guess for good.
	"date_add": true, "date_sub": true, "date_diff": true, "to_date": true,
}

// dateArithFuncs is the subset of temporalInputFuncs whose RESULT format
// depends on WHICH temporal type the argument came from: date_add over a DATE
// renders a calendar date, over a TIMESTAMP it renders an instant with the
// input's time-of-day intact. The date-part family needs no such distinction
// — the year of a day and the year of an instant are the same number — so
// only these calls tag a resolved DATE column as a civilDate.
var dateArithFuncs = map[string]bool{
	"date_add": true, "date_sub": true, "date_diff": true, "to_date": true,
}

// memoizableFuncs: deterministic, per-row-expensive string→scalar
// functions with no vec kernel. Their batch inputs repeat heavily on real
// data (ClickBench Referer: ~3x duplication within a 2048-row batch), so
// the per-row fallback dedups inputs per batch and evaluates once per
// distinct value. Only shapes where every argument except the first is a
// literal qualify — the memo key is then just the first argument.
var memoizableFuncs = map[string]bool{
	"regexp_replace": true,
	"regexp_extract": true,
	"regexp_like":    true,
	"regexp_matches": true,
}

// memoStrPool recycles the typed memo's map storage across batches: at
// 2048 rows a freshly-made map is ~90 KB, which was the largest single
// allocation left on this path once the boxing and key clones were gone.
//
// The map is cleared before it goes back, so a checkout always starts
// empty — no result and no view into a retired batch's arena survives into
// the next batch (which would be both a stale-answer bug and a reason
// pooled batch arenas stayed reachable).
var memoStrPool = sync.Pool{
	New: func() any { return make(map[string]memoStr, batch.DefaultBatchSize/2) },
}

// DefaultRegistry is the global function registry used by the expression engine.
var DefaultRegistry = NewFuncRegistry()

func init() {
	builtins := map[string]builtin{
		// String functions
		"upper":  {fnUpper, RetString},
		"lower":  {fnLower, RetString},
		"concat": {fnConcat, RetString},
		// ConcatOpFunc is the `||` OPERATOR, not a function anyone can call:
		// its name is punctuation, which no SQL identifier — delimited or
		// not — can be. It is registered here rather than compiled inline so
		// it inherits the declared String return type and the vec dispatch
		// (#328), while keeping CONCAT's NULL rule off it (#609).
		ConcatOpFunc: {fnConcatOp, RetString},
		"length":     {fnLength, RetInt32},
		"len":        {fnLength, RetInt32},
		// length() has always counted BYTES here (see fnLength), so
		// octet_length is an exact alias and bit_length is 8x. The rune-counting
		// member of the family is char_length/character_length below. These
		// three names were reachable from the parser and typed numeric by the
		// planner but had no implementation, so they evaluated to NULL.
		"octet_length": {fnOctetLength, RetInt32},
		"bit_length":   {fnBitLength, RetInt32},
		"substr":       {fnSubstr, RetString},
		"substring":    {fnSubstr, RetString},
		"trim":         {fnTrim, RetString},
		"ltrim":        {fnLTrim, RetString},
		"rtrim":        {fnRTrim, RetString},
		"replace":      {fnReplace, RetString},
		"reverse":      {fnReverse, RetString},
		"left":         {fnLeft, RetString},
		"right":        {fnRight, RetString},

		// Math functions
		"abs":   {fnAbs, RetFloat64},
		"ceil":  {fnCeil, RetFloat64},
		"floor": {fnFloor, RetFloat64},
		"round": {fnRound, RetFloat64},
		// Half-to-even ROUND for a DOUBLE PRECISION/REAL/FLOAT operand
		// (#381). compileFuncCallNode rewrites ROUND(CAST(x AS double
		// precision)) to call this instead; it is not part of ROUND's
		// documented surface but is harmless to reach directly by name.
		"round_half_even": {fnRoundHalfEven, RetFloat64},
		"pow":             {fnPow, RetFloat64},
		"power":           {fnPow, RetFloat64},
		"sqrt":            {fnSqrt, RetFloat64},
		"mod":             {fnMod, RetFloat64},
		"log":             {fnLog, RetFloat64},
		"ln":              {fnLn, RetFloat64},
		"exp":             {fnExp, RetFloat64},

		// Conditional
		"coalesce": {fnCoalesce, RetSameAsArg(batch.TypeFloat64)},
		"nullif":   {fnNullIf, RetSameAsArg(batch.TypeFloat64, 0).TypeOverAllArgs().OperatorResolved()},
		"ifnull":   {fnIfNull, RetSameAsArg(batch.TypeString, 0, 1)},
		"if":       {fnIf, RetSameAsArg(batch.TypeString, 1, 2).Control(0)},

		// Type casting
		"cast_int":    {fnCastInt, RetInt64},
		"cast_float":  {fnCastFloat, RetFloat64},
		"cast_string": {fnCastString, RetString},

		// Network functions
		"ip_to_string":  {fnIPToString, RetString},
		"cidr_contains": {fnCIDRContains, RetBool},
		"ip_version":    {fnIPVersion, RetFloat64},
		"mask_ip":       {fnMaskIP, RetString},
		"mac_to_string": {fnMACToString, RetString},
		"ip_subnet":     {fnIPSubnet, RetString},
		"ip_netmask":    {fnIPNetmask, RetString},

		// Date/time functions
		"now":        {fnNow, RetTimestamp},
		"year":       {fnYear, RetFloat64},
		"month":      {fnMonth, RetFloat64},
		"day":        {fnDay, RetFloat64},
		"hour":       {fnHour, RetFloat64},
		"minute":     {fnMinute, RetFloat64},
		"date_trunc": {fnDateTrunc, RetTimestamp},
		// time_bucket is PostgreSQL's date_bin under the name every
		// time-series engine spells it. TIMESTAMP in, TIMESTAMP out — the
		// declaration a downsampling GROUP BY key has to carry, or every
		// client reads the bucket as text (#965).
		"time_bucket":  {fnTimeBucket, RetTimestamp},
		"extract":      {fnExtract, RetFloat64},
		"current_date": {fnCurrentDate, RetString},
		"date_diff":    {fnDateDiff, RetFloat64},

		// Session / catalog information (see the SessionUser block below)
		"current_user":     {fnCurrentUser, RetString},
		"session_user":     {fnCurrentUser, RetString},
		"user":             {fnCurrentUser, RetString},
		"current_role":     {fnCurrentUser, RetString},
		"current_catalog":  {fnCurrentCatalog, RetString},
		"current_database": {fnCurrentCatalog, RetString},
		"current_schema":   {fnCurrentSchema, RetString},
		"current_schemas":  {fnCurrentSchemas, RetString},
		"version":          {fnVersion, RetString},

		"date_add": {fnDateAdd, RetString},
		"date_sub": {fnDateSub, RetString},
		"to_date":  {fnToDate, RetString},

		// UUID functions
		"uuid_version":   {fnUUIDVersion, RetFloat64},
		"uuid_to_string": {fnUUIDToString, RetString},

		// Additional string functions
		"starts_with": {fnStartsWith, RetBool},
		"ends_with":   {fnEndsWith, RetBool},
		"contains":    {fnContains, RetBool},
		"repeat":      {fnRepeat, RetString},

		// Additional math functions
		"sign":     {fnSign, RetFloat64},
		"greatest": {fnGreatest, RetSameAsArg(batch.TypeFloat64)},
		"least":    {fnLeast, RetSameAsArg(batch.TypeFloat64)},

		// Additional date/time functions
		"second": {fnSecond, RetFloat64},

		// String: regex and parsing
		"split_part":     {fnSplitPart, RetString},
		"strpos":         {fnStrPos, RetInt32},
		"position":       {fnStrPos, RetInt32},
		"regexp_like":    {fnRegexpLike, RetBool},
		"regexp_extract": {fnRegexpExtract, RetString},
		"regexp_replace": {fnRegexpReplace, RetString},

		// Encoding
		"to_hex":      {fnToHex, RetString},
		"from_hex":    {fnFromHex, RetFloat64},
		"to_base64":   {fnToBase64, RetString},
		"from_base64": {fnFromBase64, RetString},

		// Date/time conversion
		"from_unixtime": {fnFromUnixtime, RetTimestamp},
		"to_unixtime":   {fnToUnixtime, RetFloat64},
		"date_format":   {fnDateFormat, RetString},
		"date_parse":    {fnDateParse, RetTimestamp},

		// Hash
		"md5":    {fnMD5, RetString},
		"sha256": {fnSHA256, RetString},
		"sha512": {fnSHA512, RetString},

		// Bitwise
		"bitwise_and": {fnBitwiseAnd, RetFloat64},
		"bitwise_or":  {fnBitwiseOr, RetFloat64},
		"bitwise_xor": {fnBitwiseXor, RetFloat64},
		"bitwise_not": {fnBitwiseNot, RetFloat64},

		// String: padding and character
		"lpad":             {fnLPad, RetString},
		"rpad":             {fnRPad, RetString},
		"chr":              {fnChr, RetString},
		"codepoint":        {fnCodepoint, RetInt32},
		"concat_ws":        {fnConcatWS, RetString},
		"char_length":      {fnCharLength, RetInt32},
		"character_length": {fnCharLength, RetInt32},
		"translate":        {fnTranslate, RetString},

		// Math: trigonometry
		"pi":       {fnPi, RetFloat64},
		"degrees":  {fnDegrees, RetFloat64},
		"radians":  {fnRadians, RetFloat64},
		"sin":      {fnSin, RetFloat64},
		"cos":      {fnCos, RetFloat64},
		"tan":      {fnTan, RetFloat64},
		"asin":     {fnAsin, RetFloat64},
		"acos":     {fnAcos, RetFloat64},
		"atan":     {fnAtan, RetFloat64},
		"atan2":    {fnAtan2, RetFloat64},
		"cbrt":     {fnCbrt, RetFloat64},
		"log2":     {fnLog2, RetFloat64},
		"truncate": {fnTruncate, RetFloat64},
		"rand":     {fnRandom, RetFloat64},
		"random":   {fnRandom, RetFloat64},

		// JSON
		"json_extract":        {fnJSONExtract, RetDynamic},
		"json_extract_scalar": {fnJSONExtractScalar, RetDynamic},
		"json_array_length":   {fnJSONArrayLength, RetFloat64},
		"json_valid":          {fnJSONValid, RetBool},

		// URL
		"url_extract_host":      {fnURLExtractHost, RetString},
		"url_extract_port":      {fnURLExtractPort, RetFloat64},
		"url_extract_path":      {fnURLExtractPath, RetString},
		"url_extract_protocol":  {fnURLExtractProtocol, RetString},
		"url_extract_query":     {fnURLExtractQuery, RetString},
		"url_extract_parameter": {fnURLExtractParameter, RetString},

		// Type introspection
		"typeof": {fnTypeof, RetString},

		// String: distance and utility
		"soundex":              {fnSoundex, RetString},
		"levenshtein_distance": {fnLevenshtein, RetFloat64},
		"hamming_distance":     {fnHamming, RetFloat64},
		"normalize":            {fnNormalize, RetString},
		"format":               {fnFormat, RetString},
		"lcase":                {fnLower, RetString},
		"ucase":                {fnUpper, RetString},
		"to_utf8":              {fnToUTF8, RetBytes},
		"from_utf8":            {fnFromUTF8, RetString},

		// Math: IEEE 754 and utility
		"e":            {fnE, RetFloat64},
		"log10":        {fnLog10, RetFloat64},
		"infinity":     {fnInfinity, RetFloat64},
		"nan":          {fnNaN, RetFloat64},
		"is_nan":       {fnIsNaN, RetBool},
		"is_finite":    {fnIsFinite, RetBool},
		"is_infinite":  {fnIsInfinite, RetBool},
		"width_bucket": {fnWidthBucket, RetInt32},
		"from_base":    {fnFromBase, RetFloat64},
		"to_base":      {fnToBase, RetString},
		"bit_count":    {fnBitCount, RetFloat64},

		// Hash: additional
		"sha1":        {fnSHA1, RetString},
		"crc32":       {fnCRC32, RetFloat64},
		"hmac_sha256": {fnHMACSHA256, RetString},
		"hmac_sha512": {fnHMACSHA512, RetString},

		// Date: additional accessors
		"quarter":           {fnQuarter, RetFloat64},
		"week":              {fnWeek, RetFloat64},
		"day_of_week":       {fnDayOfWeek, RetFloat64},
		"day_of_year":       {fnDayOfYear, RetFloat64},
		"last_day_of_month": {fnLastDayOfMonth, RetString},
		"current_timestamp": {fnCurrentTimestamp, RetTimestamp},
		"at_timezone":       {fnAtTimezone, RetString},
		// epoch: the rewrite target of EXTRACT(EPOCH FROM ts).
		// timezone: the rewrite target of `ts AT TIME ZONE zone`, zone first,
		// matching PostgreSQL's own canonical form.
		"epoch":                    {fnEpoch, RetFloat64},
		"timezone":                 {fnTimezone, RetTimestamp},
		"pg_postmaster_start_time": {fnPgPostmasterStartTime, RetTimestamp},
		"human_readable_seconds":   {fnHumanReadableSeconds, RetString},

		// Network: analytics
		"is_private_ip":  {fnIsPrivateIP, RetBool},
		"is_loopback_ip": {fnIsLoopbackIP, RetBool},
		"ip_to_int":      {fnIPToInt, RetFloat64},
		"int_to_ip":      {fnIntToIP, RetString},
		"is_ipv4":        {fnIsIPv4, RetBool},
		"is_ipv6":        {fnIsIPv6, RetBool},

		// Network: CIDR / subnet operations
		"network_address":   {fnNetworkAddress, RetString},
		"broadcast_address": {fnBroadcastAddress, RetString},
		"prefix_length":     {fnPrefixLength, RetInt64},
		"cidr_to_range":     {fnCIDRToRange, RetString},
		"hosts_in_cidr":     {fnHostsInCIDR, RetInt64},
		"cidr_overlap":      {fnCIDROverlap, RetBool},
		"ip_in_range":       {fnIPInRange, RetBool},
		"same_subnet":       {fnSameSubnet, RetBool},

		// Network: IP manipulation
		"ip_add":           {fnIPAdd, RetString},
		"ip_subtract":      {fnIPSubtract, RetString},
		"ip_diff":          {fnIPDiff, RetInt64},
		"ip_between":       {fnIPBetween, RetBool},
		"reverse_dns":      {fnReverseDNS, RetString},
		"is_multicast_ip":  {fnIsMulticastIP, RetBool},
		"is_link_local_ip": {fnIsLinkLocalIP, RetBool},
		"is_reserved_ip":   {fnIsReservedIP, RetBool},
		"ip_to_hex":        {fnIPToHex, RetString},

		// Network: MAC operations
		"mac_vendor_oui": {fnMACVendorOUI, RetString},
		"mac_is_unicast": {fnMACIsUnicast, RetBool},
		"mac_is_local":   {fnMACIsLocal, RetBool},
		"mac_format":     {fnMACFormat, RetString},

		// Network: port classification
		"port_name":          {fnPortName, RetString},
		"is_well_known_port": {fnIsWellKnownPort, RetBool},
		"is_registered_port": {fnIsRegisteredPort, RetBool},
		"is_ephemeral_port":  {fnIsEphemeralPort, RetBool},
		"port_class":         {fnPortClass, RetString},

		// Network: protocol
		"protocol_name":   {fnProtocolName, RetString},
		"protocol_number": {fnProtocolNumber, RetInt64},

		// Deep inspection: TCP
		"tcp_flags_to_string":   {fnTCPFlagsToString, RetString},
		"has_tcp_flag":          {fnHasTCPFlag, RetBool},
		"tcp_flags_from_string": {fnTCPFlagsFromString, RetInt64},
		"is_tcp_handshake":      {fnIsTCPHandshake, RetBool},
		"is_tcp_reset":          {fnIsTCPReset, RetBool},
		"tcp_session_id":        {fnTCPSessionID, RetString},
		"flow_direction":        {fnFlowDirection, RetString},

		// Deep inspection: DNS
		"dns_query_name":     {fnDNSQueryName, RetString},
		"dns_query_type":     {fnDNSQueryType, RetString},
		"dns_is_response":    {fnDNSIsResponse, RetBool},
		"dns_response_code":  {fnDNSResponseCode, RetString},
		"dns_question_count": {fnDNSQuestionCount, RetInt64},
		"dns_answer_count":   {fnDNSAnswerCount, RetInt64},
		"dns_transaction_id": {fnDNSTransactionID, RetInt64},

		// Deep inspection: TLS
		"tls_sni":             {fnTLSSNI, RetString},
		"tls_version":         {fnTLSVersion, RetString},
		"tls_record_type":     {fnTLSRecordType, RetString},
		"is_tls_client_hello": {fnIsTLSClientHello, RetBool},
		"tls_handshake_type":  {fnTLSHandshakeType, RetString},

		// Deep inspection: HTTP
		"http_method":         {fnHTTPMethod, RetString},
		"http_path":           {fnHTTPPath, RetString},
		"http_host":           {fnHTTPHost, RetString},
		"http_status_code":    {fnHTTPStatusCode, RetInt64},
		"http_status_class":   {fnHTTPStatusClass, RetString},
		"http_content_type":   {fnHTTPContentType, RetString},
		"http_content_length": {fnHTTPContentLength, RetInt64},
		"http_user_agent":     {fnHTTPUserAgent, RetString},
		"http_header":         {fnHTTPHeader, RetString},
		"http_version":        {fnHTTPVersion, RetString},
		"is_http_request":     {fnIsHTTPRequest, RetBool},
		"is_http_response":    {fnIsHTTPResponse, RetBool},

		// Deep inspection: packet headers
		"ip_header_length": {fnIPHeaderLength, RetInt64},
		"ip_ttl":           {fnIPTTL, RetInt64},
		"ip_total_length":  {fnIPTotalLength, RetInt64},
		"ip_dscp":          {fnIPDSCP, RetInt64},
		"ether_type":       {fnEtherType, RetString},
		"vlan_id":          {fnVLANID, RetInt64},

		// Deep inspection: payload analysis
		"payload_entropy":  {fnPayloadEntropy, RetFloat64},
		"payload_hex_dump": {fnPayloadHexDump, RetString},

		// ICMP
		"icmp_type_name": {fnICMPTypeName, RetString},
		"icmp_code_name": {fnICMPCodeName, RetString},
		"is_icmp_echo":   {fnIsICMPEcho, RetBool},
		"icmp_parse":     {fnICMPParse, RetString},
		"icmp_type":      {fnICMPType, RetInt64},
		"icmp_code":      {fnICMPCode, RetInt64},

		// IPv6
		"ipv6_scope":     {fnIPv6Scope, RetString},
		"ipv6_expand":    {fnIPv6Expand, RetString},
		"ipv6_compress":  {fnIPv6Compress, RetString},
		"ipv6_to_eui64":  {fnIPv6ToEUI64, RetString},
		"is_6to4":        {fnIs6to4, RetBool},
		"is_teredo":      {fnIsTeredo, RetBool},
		"teredo_server":  {fnTeredoServer, RetString},
		"teredo_client":  {fnTeredoClient, RetString},
		"sixto4_gateway": {fnSixto4Gateway, RetString},

		// JA3 TLS fingerprinting
		"ja3_fingerprint":  {fnJA3Fingerprint, RetString},
		"ja3_string":       {fnJA3String, RetString},
		"ja3s_fingerprint": {fnJA3SFingerprint, RetString},
		"ja3s_string":      {fnJA3SString, RetString},

		// Payload search
		"payload_contains": {fnPayloadContains, RetBool},
		"payload_matches":  {fnPayloadMatches, RetBool},
		"payload_offset":   {fnPayloadOffset, RetString},
		"payload_length":   {fnPayloadLength, RetInt64},

		// Regex: additional
		"regexp_count":       {fnRegexpCount, RetInt64},
		"regexp_extract_all": {fnRegexpExtractAll, RetString},
		"regexp_split":       {fnRegexpSplit, RetString},

		// String: additional
		"split": {fnSplit, RetString},

		// Bitwise: shifts
		"bitwise_left_shift":             {fnBitwiseLeftShift, RetInt64},
		"bitwise_right_shift":            {fnBitwiseRightShift, RetInt64},
		"bitwise_arithmetic_shift_right": {fnBitwiseArithmeticShiftRight, RetInt64},

		// UUID: generation
		"uuid": {fnUUID, RetString},

		// Encoding: additional
		"to_base32":   {fnToBase32, RetString},
		"from_base32": {fnFromBase32, RetString},
		"xxhash64":    {fnXXHash64, RetString},
		"murmur3":     {fnMurmur3, RetString},

		// Date/time: ISO 8601
		"from_iso8601_timestamp": {fnFromISO8601Timestamp, RetInt64},
		"from_iso8601_date":      {fnFromISO8601Date, RetString},
		"to_iso8601":             {fnToISO8601, RetString},
		"to_milliseconds":        {fnToMilliseconds, RetInt64},
		"timezone_hour":          {fnTimezoneHour, RetInt64},
		"timezone_minute":        {fnTimezoneMinute, RetInt64},

		// Formatting
		"format_number": {fnFormatNumber, RetString},

		// GeoIP / ASN lookup (requires MaxMind MMDB databases)
		"geoip_country":      {fnGeoipCountry, RetString},
		"geoip_country_name": {fnGeoipCountryName, RetString},
		"geoip_city":         {fnGeoipCity, RetString},
		"geoip_subdivision":  {fnGeoipSubdivision, RetString},
		"geoip_postal_code":  {fnGeoipPostalCode, RetString},
		"geoip_latitude":     {fnGeoipLatitude, RetFloat64},
		"geoip_longitude":    {fnGeoipLongitude, RetFloat64},
		"geoip_timezone":     {fnGeoipTimezone, RetString},
		"geoip_continent":    {fnGeoipContinent, RetString},
		"geoip_asn":          {fnGeoipASN, RetInt64},
		"geoip_org":          {fnGeoipOrg, RetString},

		// Byte/rate formatting
		"format_bytes": {fnFormatBytes, RetString},
		"parse_bytes":  {fnParseBytes, RetInt64},
		"format_rate":  {fnFormatRate, RetString},
		"parse_rate":   {fnParseRate, RetInt64},

		// Array/nested type functions (Trino-compatible)
		"cardinality":    {fnCardinality, RetInt32},
		"array_length":   {fnArrayLength, RetInt32},
		"element_at":     {fnElementAt, RetDynamic},
		"array_contains": {fnArrayContains, RetBool},
		"array_join":     {fnArrayJoin, RetString},
		"array_min":      {fnArrayMin, RetDynamic},
		"array_max":      {fnArrayMax, RetDynamic},

		// ROW/struct functions
		"row_field":    {fnRowField, RetDynamic},
		"struct_field": {fnRowField, RetDynamic},

		// Domain parsing (DNS threat hunting)
		"registered_domain": {fnRegisteredDomain, RetString},
		"tld":               {fnTLD, RetString},
		"subdomain":         {fnSubdomain, RetString},
		"domain_depth":      {fnDomainDepth, RetFloat64},

		// URL encoding/decoding
		"url_encode": {fnURLEncode, RetString},
		"url_decode": {fnURLDecode, RetString},

		// String analysis
		"entropy": {fnEntropy, RetFloat64},

		// MAP functions
		"map_keys":         {fnMapKeys, RetArray},
		"map_values":       {fnMapValues, RetArray},
		"map_entries":      {fnMapEntries, RetArray},
		"map_from_entries": {fnMapFromEntries, RetMap},
	}
	for name, b := range builtins {
		DefaultRegistry.Register(name, b.fn, b.ret)
	}

	// Vectorized implementations: operate on entire columns instead of per-row.
	vecBuiltins := map[string]VecScalarFunc{
		"upper":        vecUpper,
		"lower":        vecLower,
		"length":       vecCharLength,
		"len":          vecCharLength,
		"octet_length": vecOctetLength,
		"bit_length":   vecBitLength,
		// Rune counting needs the bytes — no offsets fast path exists.
		"char_length":      vecCharLength,
		"character_length": vecCharLength,
		"trim":             vecTrim,
		"ltrim":            vecLTrim,
		"rtrim":            vecRTrim,
		"substr":           vecSubstr,
		"substring":        vecSubstr,
		"replace":          vecReplace,
		"reverse":          vecReverse,
		"left":             vecLeft,
		"right":            vecRight,
		"concat":           vecConcat,
		ConcatOpFunc:       vecConcatOp,
		"starts_with":      vecStartsWith,
		"ends_with":        vecEndsWith,
		"contains":         vecContains,
		"abs":              vecAbs,
		"ceil":             vecCeil,
		"floor":            vecFloor,
		"round":            vecRound,
		"round_half_even":  vecRoundHalfEven,
		"year":             vecYear,
		"month":            vecMonth,
		"day":              vecDay,
		"hour":             vecHour,
		"extract":          vecExtract,
	}
	for name, fn := range vecBuiltins {
		DefaultRegistry.RegisterVec(name, fn)
	}
}

var (
	dateEpochDaysCache    temporalMemo // epoch days
	timestampEpochMsCache temporalMemo // epoch milliseconds
)

// regexpCache caches compiled patterns process-wide. Scalar regexp
// functions previously called regexp.Compile PER ROW — ClickBench Q29
// (REGEXP_REPLACE over 100M Referers) spent its 117s recompiling one
// pattern 100M times. sync.Map: read-mostly, a handful of distinct
// patterns per workload.
var regexpCache sync.Map

// processStart is when this process began, captured once at package
// initialization.
var processStart = time.Now()

// ── Port Classification ─────────────────────────────────────────────────────

var wellKnownPorts = map[int64]string{
	20: "ftp-data", 21: "ftp", 22: "ssh", 23: "telnet", 25: "smtp",
	53: "dns", 67: "dhcp", 68: "dhcp-client", 80: "http", 110: "pop3",
	123: "ntp", 143: "imap", 161: "snmp", 162: "snmp-trap", 443: "https",
	445: "smb", 465: "smtps", 514: "syslog", 587: "submission", 636: "ldaps",
	993: "imaps", 995: "pop3s", 1433: "mssql", 1521: "oracle", 3306: "mysql",
	3389: "rdp", 5432: "postgresql", 5900: "vnc", 6379: "redis",
	8080: "http-alt", 8443: "https-alt", 9200: "elasticsearch", 27017: "mongodb",
}

// ── Protocol ────────────────────────────────────────────────────────────────

var protocolNumToName = map[int64]string{
	1: "icmp", 2: "igmp", 6: "tcp", 17: "udp", 41: "ipv6",
	47: "gre", 50: "esp", 51: "ah", 58: "icmpv6", 89: "ospf",
	103: "pim", 132: "sctp",
}

var protocolNameToNum map[string]int64

func init() {
	protocolNameToNum = make(map[string]int64, len(protocolNumToName))
	for num, name := range protocolNumToName {
		protocolNameToNum[name] = num
	}
}

var tcpFlagNames = []struct {
	mask byte
	name string
}{
	{tcpFIN, "FIN"},
	{tcpSYN, "SYN"},
	{tcpRST, "RST"},
	{tcpPSH, "PSH"},
	{tcpACK, "ACK"},
	{tcpURG, "URG"},
	{tcpECE, "ECE"},
	{tcpCWR, "CWR"},
}

var tcpFlagLookup = map[string]byte{
	"fin": tcpFIN, "syn": tcpSYN, "rst": tcpRST, "psh": tcpPSH,
	"ack": tcpACK, "urg": tcpURG, "ece": tcpECE, "cwr": tcpCWR,
}

// --- ICMP Functions ---

var icmpTypeNames = map[int]string{
	0:  "Echo Reply",
	3:  "Destination Unreachable",
	4:  "Source Quench",
	5:  "Redirect",
	8:  "Echo Request",
	9:  "Router Advertisement",
	10: "Router Solicitation",
	11: "Time Exceeded",
	12: "Parameter Problem",
	13: "Timestamp Request",
	14: "Timestamp Reply",
	17: "Address Mask Request",
	18: "Address Mask Reply",
	30: "Traceroute",
}

var icmpUnreachableCodes = map[int]string{
	0:  "Network Unreachable",
	1:  "Host Unreachable",
	2:  "Protocol Unreachable",
	3:  "Port Unreachable",
	4:  "Fragmentation Needed",
	5:  "Source Route Failed",
	6:  "Destination Network Unknown",
	7:  "Destination Host Unknown",
	9:  "Network Administratively Prohibited",
	10: "Host Administratively Prohibited",
	13: "Communication Administratively Prohibited",
}

var icmpRedirectCodes = map[int]string{
	0: "Redirect for Network",
	1: "Redirect for Host",
	2: "Redirect for TOS and Network",
	3: "Redirect for TOS and Host",
}

var icmpTimeExceededCodes = map[int]string{
	0: "TTL Exceeded in Transit",
	1: "Fragment Reassembly Time Exceeded",
}

// ── Encoding: Additional ────────────────────────────────────────────────────

var base32Encoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// --- Byte/rate formatting functions ---

var byteUnits = []struct {
	threshold float64
	suffix    string
}{
	{1152921504606846976, "EiB"},
	{1125899906842624, "PiB"},
	{1099511627776, "TiB"},
	{1073741824, "GiB"},
	{1048576, "MiB"},
	{1024, "KiB"},
}

var byteUnitsSI = []struct {
	threshold float64
	suffix    string
}{
	{1e18, "EB"},
	{1e15, "PB"},
	{1e12, "TB"},
	{1e9, "GB"},
	{1e6, "MB"},
	{1e3, "KB"},
}

// --- Array/nested type function implementations ---

// toSlice converts a value to []any, handling both []any and []map[string]any.
func toSlice(v any) ([]any, bool) {
	switch tv := v.(type) {
	case []any:
		return tv, true
	case []map[string]any:
		out := make([]any, len(tv))
		for i, m := range tv {
			out[i] = m
		}
		return out, true
	default:
		return nil, false
	}
}

// cardinality(array) — returns the number of elements. PostgreSQL's
// `cardinality` counts every element of every dimension and answers 0 for an
// empty array, which is what this does and what it always did.
func fnCardinality(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	return int32Count(len(arr))
}

// array_length(array, dim) — the length of the ARRAY along dimension `dim`,
// which is a DIFFERENT function from cardinality and was registered as an
// alias of it (#637).
//
// Two things that alias got wrong, both measured live on postgres:17.11:
//
//	array_length(ARRAY[]::int[], 1)   NULL   -- was 0
//	array_length(ARRAY[1,2,3], 2)     NULL   -- was 3: the dimension was IGNORED
//	array_length(ARRAY[1,2,3], 0)     NULL
//	array_length(ARRAY[1,2,3], -1)    NULL
//	array_length(ARRAY[1,2,3], NULL)  NULL
//	array_length(NULL::int[], 1)      NULL
//	array_length(ARRAY[1,2,3], 1)     3
//
// NULL is PostgreSQL's answer for "that dimension does not exist", and an
// EMPTY array has no dimension 1 — which is why the first row is NULL and
// `cardinality` of the same array is 0. The two functions disagree there on
// purpose and the alias made them agree.
//
// This engine's ARRAY is one-dimensional (parquet.Column.ElementType is a
// single element type, and an ARRAY of ARRAY is a nested ELEMENT rather than a
// second dimension), so any `dim` other than 1 is NULL. The one-argument
// spelling PostgreSQL does not have keeps cardinality's answer, so nothing
// that called `array_length(a)` changes.
func fnArrayLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	if len(args) < 2 {
		// The wadjet-only one-argument spelling, unchanged.
		return int32Count(len(arr))
	}
	if args[1] == nil {
		return nil
	}
	if ToInt64(args[1]) != 1 || len(arr) == 0 {
		return nil
	}
	return int32Count(len(arr))
}

// element_at(array, index) — returns the element at 1-based index (Trino convention)
// For MAPs: element_at(map, key) returns the value for the given key.
// Negative indices count from the end.
//
// A MAP value and an ARRAY of ROW("key","value") (e.g. map_entries()'s output)
// share the same runtime shape — []any of {key,value} rows — so this
// value-only entry point CANNOT tell a MAP key lookup from an array index and
// treats every slice positionally. The MAP-vs-ARRAY choice is made from the
// COMPILED type of the first argument instead, in elementAtExpr, which the
// compiler wraps every element_at / m['k'] subscript in (#607). A genuine Go
// map (map_from_entries, constructed literals) is unambiguous and keyed here
// directly.
func fnElementAt(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	if m, ok := args[0].(map[string]any); ok {
		return m[fmt.Sprint(args[1])]
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	idx := int(ToInt64(args[1]))
	if idx > 0 {
		idx-- // convert 1-based to 0-based
	} else if idx < 0 {
		idx = len(arr) + idx // negative index from end
	} else {
		return nil // 0 is invalid in 1-based indexing
	}
	if idx < 0 || idx >= len(arr) {
		return nil
	}
	return arr[idx]
}

// array_contains(array, element) — returns true if array contains element
func fnArrayContains(args []any) any {
	if len(args) < 2 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	target := args[1]
	for _, elem := range arr {
		if elem == target || fmt.Sprint(elem) == fmt.Sprint(target) {
			return true
		}
	}
	return false
}

// array_join(array, delimiter) — joins array elements into a string
func fnArrayJoin(args []any) any {
	if len(args) < 2 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	delim := toString(args[1])
	parts := make([]string, 0, len(arr))
	for _, elem := range arr {
		if elem != nil {
			parts = append(parts, fmt.Sprint(elem))
		}
	}
	return strings.Join(parts, delim)
}

// array_min(array) — returns the minimum element
func fnArrayMin(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok || len(arr) == 0 {
		return nil
	}
	min := arr[0]
	for _, elem := range arr[1:] {
		if elem == nil {
			continue
		}
		if min == nil || fmt.Sprint(elem) < fmt.Sprint(min) {
			min = elem
		}
	}
	return min
}

// row_field(row, 'field_name') — extracts a named field from a ROW/struct value
func fnRowField(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	row, ok := args[0].(map[string]any)
	if !ok {
		return nil
	}
	field := toString(args[1])
	return row[field]
}

// --- MAP function implementations ---

// toMap extracts key-value pairs from a MAP value.
// MAPs are stored as []any where each element is map[string]any{"key":k, "value":v}
// or as map[string]any directly.
// elementAtExpr evaluates element_at(x, k) / the x[k] subscript, choosing MAP
// key lookup vs. ARRAY positional index from the COMPILED type of x rather than
// the runtime value's shape. A MAP column materializes as an
// ARRAY(ROW("key","value")) []any and map_entries() returns that identical
// shape, so a value-only heuristic misroutes one of them; the static type does
// not (#607).
type elementAtExpr struct {
	arg0, arg1 Expr
	// resolved publishes isMap and decimalKey, decided on the first Eval
	// because a ColRef needs the batch to know its column's declared type.
	// Set last under mu.
	resolved atomic.Bool
	mu       sync.Mutex
	isMap    bool
	// decimalKey is true for a MAP whose KEY is declared DECIMAL. Such a key
	// is stored at the key column's scale and boxes as that text, so
	// `element_at(mk, 12.75)` looked up "12.75" against a key rendered
	// "12.7500" at (18,4) and matched nothing on every row (#669 item 2).
	decimalKey bool
}

func (e *elementAtExpr) Eval(b *batch.RecordBatch, row int) any {
	container := e.arg0.Eval(b, row)
	key := e.arg1.Eval(b, row)
	if container == nil || key == nil {
		return nil
	}
	if !e.resolved.Load() {
		e.resolveDispatch(b)
	}
	if e.isMap {
		return mapElementAt(container, key, e.decimalKey)
	}
	return fnElementAt([]any{container, key})
}

func (e *elementAtExpr) resolveDispatch(b *batch.RecordBatch) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.resolved.Load() {
		return
	}
	e.isMap = staticallyMap(e.arg0, b)
	if k := containerKeyVector(containerVector(e.arg0, b)); k != nil {
		e.decimalKey = k.Type == batch.TypeDecimal
	}
	e.resolved.Store(true)
}

// staticallyMap reports whether e is known from its COMPILED/declared type to
// evaluate to a MAP — never from a runtime value's shape, which a MAP shares
// with an ARRAY of {key,value} rows. b supplies a ColRef its column's declared
// type. A FuncCall is a MAP when its registered return type fixes it to one
// (map_from_entries is RetMap; map_entries is RetArray, so map_entries(m)[i]
// correctly indexes). Anything else is treated as an ARRAY.
func staticallyMap(e Expr, b *batch.RecordBatch) bool {
	if a, ok := e.(*FuncCall); ok {
		t, c := DefaultRegistry.ReturnType(a.Name).Resolve(len(a.Args), nil)
		if c != Undecided && t.ID == batch.TypeMap {
			return true
		}
	}
	// Any container expression whose declared shape this batch can resolve —
	// a column, a nested element_at, a COALESCE/CASE/GREATEST over them
	// (#635). A producer that declares no container shape stays an ARRAY
	// here, which is what it was before.
	v := containerVector(e, b)
	return v != nil && v.Type == batch.TypeMap
}

// mapElementAt looks key up in a MAP value in either materialized shape: a Go
// map (map_from_entries, constructed literals) or the ARRAY(ROW("key","value"))
// entry rows a MAP column produces through batch.Vector.GetValue. Keys compare
// by string form, covering the MAP's string and integer key types. A duplicate
// key returns the LAST match, matching toMap (the shape map_keys/map_values and
// a MAP GROUP BY key go through, which last-write-wins into a Go map). An
// absent key returns NULL.
// decimalKey spells both sides through batch.CanonicalDecimalText, the
// minimal-scale form AppendDecimalKey already uses for a stored DECIMAL, so
// `element_at(mk, 12.75)` finds the key a DECIMAL(18,4) column renders
// "12.7500" — two spellings of one number are one key (ADR-0012 item 8). It is
// set only when the map's KEY column is declared DECIMAL: a STRING-keyed map
// must keep matching by its bytes, where "1.50" and "1.5" are two keys.
func mapElementAt(m, key any, decimalKey bool) any {
	norm := func(s string) string {
		if !decimalKey {
			return s
		}
		if c, ok := batch.CanonicalDecimalText(s); ok {
			return c
		}
		return s
	}
	want := norm(fmt.Sprint(key))
	if gm, ok := m.(map[string]any); ok {
		if !decimalKey {
			return gm[want]
		}
		for k, v := range gm {
			if norm(k) == want {
				return v
			}
		}
		return nil
	}
	entries, ok := toSlice(m)
	if !ok {
		return nil
	}
	var val any
	found := false
	for _, entry := range entries {
		row, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if norm(fmt.Sprint(row["key"])) == want {
			val, found = row["value"], true
		}
	}
	if !found {
		return nil
	}
	return val
}

func toMap(v any) (map[string]any, bool) {
	switch tv := v.(type) {
	case map[string]any:
		return tv, true
	case []any:
		// ARRAY(ROW("key","value")) representation
		m := make(map[string]any, len(tv))
		for _, entry := range tv {
			if row, ok := entry.(map[string]any); ok {
				key := fmt.Sprint(row["key"])
				m[key] = row["value"]
			}
		}
		return m, true
	case []map[string]any:
		m := make(map[string]any, len(tv))
		for _, row := range tv {
			key := fmt.Sprint(row["key"])
			m[key] = row["value"]
		}
		return m, true
	default:
		return nil, false
	}
}

// map_keys(map) — returns the keys as an array
func fnMapKeys(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	m, ok := toMap(args[0])
	if !ok {
		return nil
	}
	keys := make([]any, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// map_values(map) — returns the values as an array
func fnMapValues(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	m, ok := toMap(args[0])
	if !ok {
		return nil
	}
	vals := make([]any, 0, len(m))
	for _, v := range m {
		vals = append(vals, v)
	}
	return vals
}

// map_entries(map) — returns array of ROW(key, value) entries
func fnMapEntries(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	m, ok := toMap(args[0])
	if !ok {
		return nil
	}
	entries := make([]any, 0, len(m))
	for k, v := range m {
		entries = append(entries, map[string]any{"key": k, "value": v})
	}
	return entries
}

// map_from_entries(array_of_rows) — constructs a map from ROW(key, value) entries
func fnMapFromEntries(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	m := make(map[string]any, len(arr))
	for _, entry := range arr {
		if row, ok := entry.(map[string]any); ok {
			key := fmt.Sprint(row["key"])
			m[key] = row["value"]
		}
	}
	return m
}

// array_max(array) — returns the maximum element
func fnArrayMax(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok || len(arr) == 0 {
		return nil
	}
	max := arr[0]
	for _, elem := range arr[1:] {
		if elem == nil {
			continue
		}
		if max == nil || fmt.Sprint(elem) > fmt.Sprint(max) {
			max = elem
		}
	}
	return max
}

// --- Vectorized scalar function implementations ---
//
// These operate on entire columns (batch.Vector) instead of per-row values,
// eliminating interface dispatch, boxing/unboxing, and string↔[]byte conversion.

// vecReadFloat64 reads a float64 from a vector at the given index,
// handling both Float64 and Int64 source types.
func vecReadFloat64(v *batch.Vector, i int) float64 {
	switch v.Type {
	case batch.TypeFloat64:
		return v.Float64Data[i]
	case batch.TypeInt64, batch.TypeTimestamp:
		return float64(v.Int64Data[i])
	case batch.TypeInt32:
		return float64(v.Int32Data[i])
	case batch.TypeFloat32:
		return float64(v.Float32Data[i])
	default:
		return 0
	}
}

func vecUpper(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	totalBytes := int(src.BytesData.Offsets[n] - src.BytesData.Offsets[0])
	out.BytesData.PreAllocBytes(totalBytes)

	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		start := len(out.BytesData.Data)
		out.BytesData.Data = append(out.BytesData.Data, b...)
		for j := start; j < len(out.BytesData.Data); j++ {
			c := out.BytesData.Data[j]
			if c >= 'a' && c <= 'z' {
				out.BytesData.Data[j] = c - 32
			}
		}
		out.BytesData.Offsets[i+1] = uint32(len(out.BytesData.Data))
	}
}

func vecLower(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	totalBytes := int(src.BytesData.Offsets[n] - src.BytesData.Offsets[0])
	out.BytesData.PreAllocBytes(totalBytes)

	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		start := len(out.BytesData.Data)
		out.BytesData.Data = append(out.BytesData.Data, b...)
		for j := start; j < len(out.BytesData.Data); j++ {
			c := out.BytesData.Data[j]
			if c >= 'A' && c <= 'Z' {
				out.BytesData.Data[j] = c + 32
			}
		}
		out.BytesData.Offsets[i+1] = uint32(len(out.BytesData.Data))
	}
}

func vecTrim(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		// Trim ASCII whitespace from both ends
		lo, hi := 0, len(b)
		for lo < hi && (b[lo] == ' ' || b[lo] == '\t' || b[lo] == '\n' || b[lo] == '\r') {
			lo++
		}
		for hi > lo && (b[hi-1] == ' ' || b[hi-1] == '\t' || b[hi-1] == '\n' || b[hi-1] == '\r') {
			hi--
		}
		out.BytesData.Set(i, b[lo:hi])
	}
}

func vecLTrim(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		lo := 0
		for lo < len(b) && (b[lo] == ' ' || b[lo] == '\t' || b[lo] == '\n' || b[lo] == '\r') {
			lo++
		}
		out.BytesData.Set(i, b[lo:])
	}
}

func vecRTrim(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		hi := len(b)
		for hi > 0 && (b[hi-1] == ' ' || b[hi-1] == '\t' || b[hi-1] == '\n' || b[hi-1] == '\r') {
			hi--
		}
		out.BytesData.Set(i, b[:hi])
	}
}

func vecSubstr(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	hasLen := len(args) >= 3
	// bytea has no characters: substring over it indexes BYTES on the server,
	// and reading its bytes as UTF-8 replaced every invalid one with U+FFFD —
	// a value the column does not hold (#583). Same rule as fnSubstr's bytea
	// arm, because the two evaluators must not answer differently.
	if src.Type == batch.TypeBytes {
		for i := 0; i < n; i++ {
			if hasNulls && src.Nulls.IsNullFast(i) {
				out.Nulls.SetNull(i)
				out.BytesData.Set(i, nil)
				continue
			}
			raw := src.BytesData.Value(i)
			start := int(vecReadFloat64(args[1], i)) - 1
			if hasLen {
				length := int(vecReadFloat64(args[2], i))
				if length < 0 {
					raiseNegativeSubstringLength()
				}
				lo, hi := substrWindow(start, length, len(raw))
				out.BytesData.Set(i, raw[lo:hi])
				continue
			}
			if start < 0 {
				start = 0
			}
			if start >= len(raw) {
				out.BytesData.Set(i, nil)
				continue
			}
			out.BytesData.Set(i, raw[start:])
		}
		return
	}

	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		// CHARACTERS, matching fnSubstr — this kernel indexed BYTES and cut
		// multi-byte characters in half (#856), and the two implementations
		// have to agree or the answer depends on which evaluator ran.
		r := []rune(string(src.BytesData.Value(i)))
		start := int(vecReadFloat64(args[1], i)) - 1 // SQL is 1-indexed
		if hasLen {
			length := int(vecReadFloat64(args[2], i))
			if length < 0 {
				raiseNegativeSubstringLength()
			}
			// PostgreSQL's window rule; must match fnSubstr (#373).
			lo, hi := substrWindow(start, length, len(r))
			out.BytesData.Set(i, []byte(string(r[lo:hi])))
			continue
		}
		if start < 0 {
			start = 0
		}
		if start >= len(r) {
			out.BytesData.Set(i, nil)
			continue
		}
		out.BytesData.Set(i, []byte(string(r[start:])))
	}
}

func vecReplace(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls() || args[1].Nulls.HasNulls() || args[2].Nulls.HasNulls()

	for i := 0; i < n; i++ {
		if hasNulls && (src.Nulls.IsNullFast(i) || args[1].Nulls.IsNullFast(i) || args[2].Nulls.IsNullFast(i)) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		s := src.BytesData.StringValue(i)
		old := args[1].BytesData.StringValue(i)
		new := args[2].BytesData.StringValue(i)
		result := strings.ReplaceAll(s, old, new)
		out.BytesData.Set(i, []byte(result))
	}
}

func vecReverse(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		// RUNES, matching fnReverse, which has always reversed runes: this
		// kernel reversed BYTES, so REVERSE('éàü') answered mojibake through
		// the vectorized path and `üàé` through the boxed one — one function,
		// two answers, decided by which evaluator the plan reached (#856).
		rev := []rune(string(src.BytesData.Value(i)))
		for j, k := 0, len(rev)-1; j < k; j, k = j+1, k-1 {
			rev[j], rev[k] = rev[k], rev[j]
		}
		out.BytesData.Set(i, []byte(string(rev)))
	}
}

func vecLeft(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		count := int(vecReadFloat64(args[1], i))
		if count < 0 {
			out.BytesData.Set(i, nil)
		} else if count >= len(b) {
			out.BytesData.Set(i, b)
		} else {
			out.BytesData.Set(i, b[:count])
		}
	}
}

func vecRight(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		count := int(vecReadFloat64(args[1], i))
		if count < 0 {
			out.BytesData.Set(i, nil)
		} else if count >= len(b) {
			out.BytesData.Set(i, b)
		} else {
			out.BytesData.Set(i, b[len(b)-count:])
		}
	}
}

// vecConcat is fnConcat's kernel: a NULL argument contributes nothing and
// the row is never NULL. The row and vector paths are separately reachable —
// which one runs depends on whether every argument is a byte-array-shaped
// vector (see FuncCall.EvalVec) — so both carry the rule (#609).
func vecConcat(args []*batch.Vector, out *batch.Vector, n int) {
	for i := 0; i < n; i++ {
		total := 0
		for _, arg := range args {
			if arg.Nulls.HasNulls() && arg.Nulls.IsNullFast(i) {
				continue
			}
			total += int(arg.BytesData.Offsets[i+1] - arg.BytesData.Offsets[i])
		}
		buf := make([]byte, 0, total)
		for _, arg := range args {
			if arg.Nulls.HasNulls() && arg.Nulls.IsNullFast(i) {
				continue
			}
			buf = append(buf, arg.BytesData.Value(i)...)
		}
		out.BytesData.Set(i, buf)
	}
}

// vecConcatOp is fnConcatOp's kernel: the `||` operator, NULL-propagating.
func vecConcatOp(args []*batch.Vector, out *batch.Vector, n int) {
	for i := 0; i < n; i++ {
		isNull := false
		for _, arg := range args {
			if arg.Nulls.HasNulls() && arg.Nulls.IsNullFast(i) {
				isNull = true
				break
			}
		}
		if isNull {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		// Calculate total length then build in one shot
		total := 0
		for _, arg := range args {
			total += int(arg.BytesData.Offsets[i+1] - arg.BytesData.Offsets[i])
		}
		buf := make([]byte, 0, total)
		for _, arg := range args {
			buf = append(buf, arg.BytesData.Value(i)...)
		}
		out.BytesData.Set(i, buf)
	}
}

func vecStartsWith(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	prefix := args[1]
	hasNulls := src.Nulls.HasNulls() || prefix.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && (src.Nulls.IsNullFast(i) || prefix.Nulls.IsNullFast(i)) {
			out.Nulls.SetNull(i)
			continue
		}
		s := src.BytesData.Value(i)
		p := prefix.BytesData.Value(i)
		result := len(s) >= len(p)
		if result {
			for j := 0; j < len(p); j++ {
				if s[j] != p[j] {
					result = false
					break
				}
			}
		}
		out.BoolData[i] = result
	}
}

func vecEndsWith(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	suffix := args[1]
	hasNulls := src.Nulls.HasNulls() || suffix.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && (src.Nulls.IsNullFast(i) || suffix.Nulls.IsNullFast(i)) {
			out.Nulls.SetNull(i)
			continue
		}
		s := src.BytesData.Value(i)
		p := suffix.BytesData.Value(i)
		result := len(s) >= len(p)
		if result {
			off := len(s) - len(p)
			for j := 0; j < len(p); j++ {
				if s[off+j] != p[j] {
					result = false
					break
				}
			}
		}
		out.BoolData[i] = result
	}
}

func vecContains(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	sub := args[1]
	hasNulls := src.Nulls.HasNulls() || sub.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && (src.Nulls.IsNullFast(i) || sub.Nulls.IsNullFast(i)) {
			out.Nulls.SetNull(i)
			continue
		}
		s := src.BytesData.StringValue(i)
		p := sub.BytesData.StringValue(i)
		out.BoolData[i] = strings.Contains(s, p)
	}
}

// --- Vectorized math functions ---

func vecAbs(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	// The typed path, when the projection allocated the argument's own domain
	// (#768). It declines whenever the output is a float64 vector, which is
	// every argument type this rule does not cover, and the loop below runs.
	if vecAbsDomain(src, out, n) {
		return
	}
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.Abs(vecReadFloat64(src, i))
	}
}

func vecCeil(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.Ceil(vecReadFloat64(src, i))
	}
}

func vecFloor(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.Floor(vecReadFloat64(src, i))
	}
}

func vecRound(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	precision := 0
	if len(args) >= 2 {
		precision = int(vecReadFloat64(args[1], 0))
	}
	pow := math.Pow(10, float64(precision))
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.Round(vecReadFloat64(src, i)*pow) / pow
	}
}

// vecRoundHalfEven is the vectorized counterpart of fnRoundHalfEven — see
// its comment for the DOUBLE PRECISION half-to-even rule (#381).
func vecRoundHalfEven(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	precision := 0
	if len(args) >= 2 {
		precision = int(vecReadFloat64(args[1], 0))
	}
	pow := math.Pow(10, float64(precision))
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.RoundToEven(vecReadFloat64(src, i)*pow) / pow
	}
}

// --- Vectorized date/time functions ---

func vecYear(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = float64(t.Year())
	}
}

func vecMonth(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = float64(t.Month())
	}
}

func vecDay(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = float64(t.Day())
	}
}

func vecHour(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = float64(t.Hour())
	}
}

func vecExtract(args []*batch.Vector, out *batch.Vector, n int) {
	if len(args) < 2 {
		return
	}
	// First arg is the unit string (constant in practice)
	unit := strings.ToLower(args[0].BytesData.StringValue(0))
	src := args[1]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		switch unit {
		case "year":
			out.Float64Data[i] = float64(t.Year())
		case "quarter":
			out.Float64Data[i] = float64((t.Month()-1)/3 + 1)
		case "month":
			out.Float64Data[i] = float64(t.Month())
		case "week":
			_, week := t.ISOWeek()
			out.Float64Data[i] = float64(week)
		case "day":
			out.Float64Data[i] = float64(t.Day())
		case "hour":
			out.Float64Data[i] = float64(t.Hour())
		case "minute":
			out.Float64Data[i] = float64(t.Minute())
		case "second":
			out.Float64Data[i] = float64(t.Second())
		case "dow", "dayofweek":
			out.Float64Data[i] = float64(t.Weekday())
		case "doy", "dayofyear":
			out.Float64Data[i] = float64(t.YearDay())
		case "epoch":
			out.Float64Data[i] = float64(t.Unix())
		default:
			// fnExtract returns nil for an unrecognized unit. Leaving the
			// pooled vector's stale contents in place instead answered with
			// whatever the previous batch wrote there.
			out.Nulls.SetNull(i)
		}
	}
}

// --- Domain parsing functions ---
// These use the Public Suffix List (golang.org/x/net/publicsuffix) to correctly
// handle multi-part TLDs like .co.uk, .com.au, .gov.uk etc.

// cleanDomain strips any trailing dot and lowercases for consistent handling.
func cleanDomain(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.TrimSuffix(s, ".")
	return s
}

// fnRegisteredDomain extracts the registered domain (eTLD+1) from a hostname.
// registered_domain('mail.google.com') → 'google.com'
// registered_domain('sub.example.co.uk') → 'example.co.uk'
func fnRegisteredDomain(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	domain := cleanDomain(toString(args[0]))
	if domain == "" {
		return nil
	}
	rd, err := publicsuffix.EffectiveTLDPlusOne(domain)
	if err != nil {
		return nil
	}
	return rd
}

// fnTLD extracts the effective top-level domain (public suffix) from a hostname.
// tld('mail.google.com') → 'com'
// tld('sub.example.co.uk') → 'co.uk'
func fnTLD(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	domain := cleanDomain(toString(args[0]))
	if domain == "" {
		return nil
	}
	suffix, _ := publicsuffix.PublicSuffix(domain)
	return suffix
}

// fnSubdomain extracts the subdomain portion (everything before the registered domain).
// subdomain('mail.google.com') → 'mail'
// subdomain('a.b.c.example.co.uk') → 'a.b.c'
// subdomain('example.com') → ” (no subdomain)
func fnSubdomain(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	domain := cleanDomain(toString(args[0]))
	if domain == "" {
		return nil
	}
	rd, err := publicsuffix.EffectiveTLDPlusOne(domain)
	if err != nil {
		return nil
	}
	if domain == rd {
		return ""
	}
	// domain = "a.b.c.example.co.uk", rd = "example.co.uk"
	// subdomain = "a.b.c"
	return strings.TrimSuffix(domain, "."+rd)
}

// fnDomainDepth returns the number of labels (dot-separated parts) in a domain.
// domain_depth('mail.google.com') → 3
// domain_depth('a.b.c.evil.com') → 5
// Useful for detecting DGA domains which tend to have unusual depth.
func fnDomainDepth(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	domain := cleanDomain(toString(args[0]))
	if domain == "" {
		return nil
	}
	return float64(strings.Count(domain, ".") + 1)
}

// --- URL encoding/decoding ---

func fnURLEncode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return url.QueryEscape(toString(args[0]))
}

func fnURLDecode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	decoded, err := url.QueryUnescape(toString(args[0]))
	if err != nil {
		return nil
	}
	return decoded
}

// --- String entropy ---

// fnEntropy computes the Shannon entropy of a string in bits per character.
// High entropy (>4.5) suggests encoded, encrypted, or random data.
// Low entropy (<3.0) suggests natural language or repetitive patterns.
// entropy('aaaa') → 0.0
// entropy('hello world') → ~2.85
// entropy('a3f8b2c9e1d7') → ~3.58
func fnEntropy(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	if len(s) == 0 {
		return float64(0)
	}
	freq := make(map[rune]int)
	total := 0
	for _, r := range s {
		freq[r]++
		total++
	}
	entropy := 0.0
	ft := float64(total)
	for _, count := range freq {
		p := float64(count) / ft
		entropy -= p * math.Log2(p)
	}
	return entropy
}

// columnInstant resolves row i of a vector to the UTC instant it denotes, and
// reports whether it could. It is THE definition of "what time is stored in
// this column", and both evaluation paths go through it: the vectorized
// date-part kernels below call it directly, and the scalar path reaches it
// through (*FuncCall).resolveTemporalArgs. Divergence between the two paths is
// what let this defect survive — one shared resolver makes agreement
// structural rather than something a test has to keep rediscovering.
//
// A raw stored number carries no unit, and each type stores a different one:
//
//	TypeDate      Int32Data, days since the epoch
//	TypeTimestamp Int64Data, MILLISECONDS since the epoch (what the parquet
//	              writer emits — file_writer.go encodes TimestampMillis — and
//	              what the comparison path assumes, parseTemporalInt64OK)
//	String/Bytes  text, parsed
//	anything else Int64Data read as seconds, the only defensible reading of
//	              an untyped integer and what parseTime(int64) has always done
//
// Reading Int64Data unconditionally was right only for a timestamp-in-seconds
// column: a DATE column has nothing in Int64Data at all, so every row came
// back as 1970 — silently, with no error and no null, collapsing a decade of
// `GROUP BY EXTRACT(YEAR FROM d)` into one bogus bucket (issue #319).
func columnInstant(src *batch.Vector, i int) (time.Time, bool) {
	switch src.Type {
	case batch.TypeString, batch.TypeBytes:
		t := parseTime(src.BytesData.StringValue(i))
		if t.IsZero() {
			return time.Time{}, false
		}
		return t, true
	case batch.TypeDate:
		if i < len(src.Int32Data) {
			// Days since the Unix epoch.
			return time.Unix(int64(src.Int32Data[i])*86400, 0).UTC(), true
		}
	case batch.TypeTimestamp:
		if i < len(src.Int64Data) {
			// Milliseconds since the Unix epoch.
			return time.UnixMilli(src.Int64Data[i]).UTC(), true
		}
	default:
		if i < len(src.Int64Data) {
			return time.Unix(src.Int64Data[i], 0).UTC(), true
		}
	}
	return time.Time{}, false
}

// --- CAST to a temporal type, and the arithmetic that reads its result ---

// castTemporalKindT names the destination types CAST resolves to a temporal
// VALUE rather than leaving to the generic conversions. TIME is deliberately
// absent: the engine has no time-of-day type to hold one, so `TIME '10:00:00'`
// keeps its text exactly as before.
type castTemporalKindT int

const (
	castNotTemporal castTemporalKindT = iota
	castToDateKind
	castToTimestampKind
)

func castTemporalKind(destType string) castTemporalKindT {
	return castTemporalKindLower(strings.ToLower(strings.TrimSpace(destType)))
}

// castTemporalKindLower is castTemporalKind for a caller that has already
// normalized the name — Cast.Eval, which needs the lowercased form for its own
// switch and runs once per row.
func castTemporalKindLower(dest string) castTemporalKindT {
	switch dest {
	case "date":
		return castToDateKind
	case "timestamp", "datetime", "timestamptz":
		return castToTimestampKind
	}
	return castNotTemporal
}

// castTemporal is CAST(x AS DATE) / CAST(x AS TIMESTAMP).
//
// Both produce the box the corresponding COLUMN type produces: epoch DAYS for
// DATE, epoch MILLISECONDS for TIMESTAMP, both int64 — the same values
// ColRef.Eval hands out for a batch.TypeDate / batch.TypeTimestamp column, and
// the same values batch.Vector.SetValue stores back into one. That identity is
// the whole point of the fix (#340): until now the cast returned its argument
// unchanged, so `CAST('1996-01-10' AS DATE) - 1` subtracted 1 from the number
// ToFloat64 read out of the TEXT's leading digits and answered 1995.
//
// The operand resolves through temporalOperand — the #332 helper — so a DATE
// column arrives as a civilDate and a TIMESTAMP column as a time.Time, with
// the unit their bare int64 box has lost recovered from the declared column
// type; parseDateArg (#322) then reads whichever form arrived. Nothing here
// parses a column value itself, so the cast cannot disagree with date_add,
// date_diff or `date ± INTERVAL` about what a column means.
//
// TEXT that resolves to no instant at all RAISES — 22007 for text that is not
// a date, 22008 for a well-formed date naming no day — which is what
// PostgreSQL answers and what #836 and #840 are. #340 chose NULL because the
// expression layer had no per-row error channel; it has one (FatalEvalPanic,
// #347), the numeric casts have used it since #367, and #836 is the issue
// that noticed ADR-0012's residual text still said otherwise. Every non-text
// box that fails to parse keeps its NULL — see raiseTemporalCastRefusal for
// why that boundary is where PostgreSQL puts it.
func castTemporal(b *batch.RecordBatch, row int, operand Expr, v any, kind castTemporalKindT) any {
	src, ok := temporalOperand(b, row, operand, v)
	if !ok {
		src = v
	}
	// TEXT goes through the engine's ONE temporal accept-set, which answers
	// the VALUE and the refusal from the same function — see
	// castTemporalText. Every other box keeps parseDateArg's reading: a DATE
	// column arrives as a civilDate, a TIMESTAMP one as a time.Time, and a
	// bare number still reads as days since the epoch.
	if out, isText := castTemporalText(src, kind); isText {
		return out
	}
	// A NUMBER cast to DATE is a DAY COUNT, and it is answered in the DATE
	// carrier's own domain rather than through time.Date (#911).
	//
	// parseDateValue reads a bare number as `time.Date(1970,1,1).AddDate(0, 0,
	// n)`, and time.Date multiplies the day count by 86400 in an unmodulated
	// uint64: `(2^63-1)·86400 ≡ -86400 (mod 2^64)`, so the instant came back
	// at epoch minus one day and `9223372036854775807::DATE` answered
	// 1969-12-31. What reached batch.Vector.SetValue was the int64 -1, which
	// fits an int32 — so the store's own guard, which refuses 3000000000::DATE
	// with 22003, had nothing left to reject. The narrowing has to be decided
	// where the day count is still the number the query wrote.
	if kind == castToDateKind {
		if days, isNum := epochDayOperand(src); isNum {
			return castIntInRange(days, "date")
		}
	}
	t, _, ok := parseDateArg(src)
	if !ok {
		return nil
	}
	if kind == castToDateKind {
		return epochDaysOf(t)
	}
	return t.UTC().UnixMilli()
}

// epochDayOperand reads a bare NUMBER as the day count a DATE cast means, with
// no calendar arithmetic in between — the reading parseDateValue's numeric arms
// already have, minus the wrap.
//
// A float TRUNCATES toward zero, which is what `int(tv)` did there, and one
// with no int64 at all (`9223372036854775808` is parsed as a float64, being
// outside int64) raises rather than taking Go's implementation-defined
// conversion: on amd64 that yields MinInt64, whose 86400-multiple wraps to
// zero, and the cast answered 1970-01-01 for it.
func epochDayOperand(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case float64:
		return epochDayFromFloat(n), true
	case float32:
		return epochDayFromFloat(float64(n)), true
	}
	return 0, false
}

func epochDayFromFloat(f float64) int64 {
	t := math.Trunc(f)
	if math.IsNaN(t) || t >= 9223372036854775808.0 || t < -9223372036854775808.0 {
		raiseIntegerOutOfRange("date")
	}
	return int64(t)
}

// epochDaysOf floors an instant to the UTC day it falls in and returns that
// day's distance from 1970-01-01 — the DATE column representation.
func epochDaysOf(t time.Time) int64 {
	secs := t.UTC().Unix()
	days := secs / 86400
	if secs < 0 && secs%86400 != 0 {
		days--
	}
	return days
}

// dateArith is `date - date` and `date ± n`, the two shapes BinOp.Eval must
// recognize once CAST produces a real date (#340).
//
// Operands resolve through temporalOperand, so every form the engine has for a
// date is accepted on equal terms: a DATE/TIMESTAMP column, a CAST to one, and
// a date-shaped string — which is what a DATE column looks like when the
// catalog declares it VARCHAR, as the TPC-H fixtures do. `l_receiptdate -
// l_shipdate` is exactly that shape, and it answered NULL on every row.
//
//	date - date → the whole number of days between them (DuckDB: BIGINT)
//	date ± n    → the date n days away, as epoch days (DuckDB: DATE)
//
// Both are gated on the operands being whole DAYS. An instant carrying a clock
// declines and falls through to the arithmetic below, because
// timestamp-minus-timestamp is an INTERVAL in SQL and this engine has no
// interval column type to answer with — inventing a unit here is the mistake
// #319 and #322 were about. `date ± INTERVAL` is not handled here either: that
// is intervalShift, which keeps the rendered-string result #322 pinned for it.
//
// ok=false means "not date arithmetic" and leaves the caller's numeric path
// untouched — including the case where an operand is a string that does not
// parse as a date, which is how `'BUILDING' - 1` keeps its old answer.
func (e *BinOp) dateArith(b *batch.RecordBatch, row int, lv, rv any) (any, bool) {
	ld, lok := temporalOperand(b, row, e.Left, lv)
	if !lok {
		// `n + date`, the one reversed shape that means anything.
		if e.Op != "+" {
			return nil, false
		}
		n, nok := plainDayCount(lv)
		if !nok {
			return nil, false
		}
		rd, rok := temporalOperand(b, row, e.Right, rv)
		if !rok {
			return nil, false
		}
		rt, dateOnly, parsed := parseDateArg(rd)
		if !parsed || !dateOnly {
			return nil, false
		}
		return epochDaysOf(rt.AddDate(0, 0, int(n))), true
	}
	lt, lDateOnly, lparsed := parseDateArg(ld)
	if !lparsed {
		return nil, false
	}
	if rd, rok := temporalOperand(b, row, e.Right, rv); rok {
		rt, rDateOnly, rparsed := parseDateArg(rd)
		if !rparsed || e.Op != "-" || !lDateOnly || !rDateOnly {
			return nil, false
		}
		return epochDaysOf(lt) - epochDaysOf(rt), true
	}
	n, nok := plainDayCount(rv)
	if !nok || !lDateOnly {
		return nil, false
	}
	if e.Op == "-" {
		n = -n
	}
	return epochDaysOf(lt.AddDate(0, 0, int(n))), true
}

// plainDayCount reads the non-date side of `date ± n` as a whole number of
// days. A fractional float declines rather than truncating, so the caller
// falls through to ordinary arithmetic instead of quietly rounding a date.
func plainDayCount(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	case float64:
		if n == math.Trunc(n) {
			return int64(n), true
		}
	case float32:
		if float64(n) == math.Trunc(float64(n)) {
			return int64(n), true
		}
	}
	return 0, false
}
