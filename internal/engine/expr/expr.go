// Package expr provides a typed expression engine for evaluating SQL expressions
// against record batches. It replaces the string-based expression parsing with
// a compiled expression tree built from the SQL parser AST.
package expr

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base32"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"math/bits"
	"math/rand"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/net/publicsuffix"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/geoip"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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

// --- String function implementations ---

func fnUpper(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.ToUpper(toString(args[0]))
}

func fnLower(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.ToLower(toString(args[0]))
}

// ConcatOpFunc is the registry name of the `||` OPERATOR's implementation.
//
// It is punctuation on purpose. A registry entry is reachable from SQL only
// if a query can spell its name, and no SQL identifier — bare or delimited —
// is `||`: the lexer reads those two bytes as an operator token before any
// identifier rule sees them. So the operator's NULL-propagating kernels
// cannot be invoked as a function, and `CONCAT` cannot reach them (#609).
// The census carries a fixture that attempts both spellings, because
// "unspellable" is a claim and the protocol's method 10 says a claim gets a
// fixture rather than a comment.
const ConcatOpFunc = "||"

// fnConcat is the CONCAT() FUNCTION, which IGNORES NULL arguments —
// `CONCAT('a', NULL, 'b')` is `ab` and `CONCAT(NULL, NULL)` is the EMPTY
// STRING, never NULL (PostgreSQL 17; ADR-0012 makes it the authority). It is
// fnConcatWS without a separator, which is where the rule was already right
// (#609).
//
// The `||` OPERATOR is the other rule and has its own kernels below: it
// propagates NULL. The two were one function until #609, which is why
// making this one NULL-tolerant is only half the fix.
func fnConcat(args []any) any {
	var sb strings.Builder
	for _, a := range args {
		if a == nil {
			continue
		}
		sb.WriteString(toString(a))
	}
	return sb.String()
}

// fnConcatOp is the `||` OPERATOR: NULL in any operand makes the whole
// expression NULL. Registered under a name no SQL identifier can spell, so
// the operator and the function cannot be confused for one another by a
// query — compile.go lowers `||` to it (#609).
func fnConcatOp(args []any) any {
	// `bytea || bytea` is BYTEA on the server, and an unknown-typed literal
	// beside one is read as bytea too — so `b || 'x'` is the bytes of b
	// followed by 0x78, under OID 17 rather than under text's 25 (#583). The
	// raw bytes of a non-UTF-8 value under a declared `text` is exactly the
	// embedded-NUL field #570 removed for the column itself.
	bytesResult := false
	for _, a := range args {
		if _, ok := a.([]byte); ok {
			bytesResult = true
			break
		}
	}
	if bytesResult {
		out := make([]byte, 0, 16)
		for _, a := range args {
			if a == nil {
				return nil
			}
			if raw, ok := a.([]byte); ok {
				out = append(out, raw...)
				continue
			}
			out = append(out, toString(a)...)
		}
		return out
	}
	var sb strings.Builder
	for _, a := range args {
		if a == nil {
			return nil
		}
		sb.WriteString(toString(a))
	}
	return sb.String()
}

// substrBytes is fnSubstr's bytea arm: PostgreSQL's substring over bytea takes
// the same window rule and the same 22011 refusal for a negative length, over
// BYTES rather than characters.
func substrBytes(raw []byte, args []any) any {
	start := int(ToFloat64(args[1])) - 1
	if len(args) >= 3 && args[2] != nil {
		length := int(ToFloat64(args[2]))
		if length < 0 {
			raiseNegativeSubstringLength()
		}
		lo, hi := substrWindow(start, length, len(raw))
		out := make([]byte, hi-lo)
		copy(out, raw[lo:hi])
		return out
	}
	if start < 0 {
		start = 0
	}
	if start >= len(raw) {
		return []byte{}
	}
	out := make([]byte, len(raw)-start)
	copy(out, raw[start:])
	return out
}

// fnLength is LENGTH / LEN, and it counts CHARACTERS.
//
// `length` and `character_length` are synonyms in PostgreSQL and disagreed
// here: LENGTH('éàü') was 6 — a byte count — beside CHARACTER_LENGTH's 3 on
// the same input (#856). OCTET_LENGTH and BIT_LENGTH are the byte-counting
// spellings and are unchanged.
func fnLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	// bytea has no characters, so `length(bytea)` is its BYTE count on the
	// server — the same number octet_length gives — and reading its bytes as
	// UTF-8 answered 1 for the two bytes of an encoded 'e' (#583).
	if raw, ok := args[0].([]byte); ok {
		return int32Count(len(raw))
	}
	return int32Count(utf8.RuneCountInString(toString(args[0])))
}

// fnSubstr is SUBSTRING / SUBSTR, and it indexes CHARACTERS.
//
// Byte indexing did not merely mislabel: `SUBSTR('éàü', 2, 2)` cut both
// two-byte characters in half and produced a string that is not valid UTF-8
// (#856). PostgreSQL answers `àü`.
func fnSubstr(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	// `substring(bytea, ...)` exists on the server and returns BYTEA, indexed
	// by BYTES: `substring('\xff\xfe\x00A' from 1 for 1)` is `\xff`. Read
	// as text it produced the UTF-8 replacement character instead — #570's own
	// hazard coming back through a derived value (#583).
	if raw, ok := args[0].([]byte); ok {
		return substrBytes(raw, args)
	}
	r := []rune(toString(args[0]))
	start := int(ToFloat64(args[1])) - 1 // SQL is 1-indexed
	if len(args) >= 3 && args[2] != nil {
		length := int(ToFloat64(args[2]))
		// PostgreSQL refuses a NEGATIVE length with 22011 rather than
		// answering the empty string, which is what substrWindow's own doc
		// recorded as unreachable while the per-row error channel did not
		// exist. It does (#347), so this refuses like the server.
		if length < 0 {
			raiseNegativeSubstringLength()
		}
		lo, hi := substrWindow(start, length, len(r))
		return string(r[lo:hi])
	}
	if start < 0 {
		start = 0
	}
	if start >= len(r) {
		return ""
	}
	return string(r[start:])
}

// substrWindow clamps SUBSTR's [start, start+length) character window (both
// 0-based here) into valid slice bounds for a string of length n. The window
// rule is PostgreSQL's (#373): a start below position 1 consumes part of the
// length before the string begins — SUBSTR('abcdef', 0, 3) selects positions
// 0,1,2 of which only 1 and 2 exist, so 'ab' and not 'abc'. A non-positive
// or overflowed window is empty rather than an error; a NEGATIVE length is
// SQLSTATE 22011 on the server and the callers raise it before reaching here
// (#856). n is a CHARACTER count in every caller.
func substrWindow(start, length, n int) (int, int) {
	end := start + length
	if length < 0 || end < start { // negative length or overflow
		end = start
	}
	if start < 0 {
		start = 0
	}
	if start > n {
		start = n
	}
	if end < start {
		end = start
	}
	if end > n {
		end = n
	}
	return start, end
}

func fnTrim(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.TrimSpace(toString(args[0]))
}

func fnLTrim(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.TrimLeft(toString(args[0]), " \t\n\r")
}

func fnRTrim(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.TrimRight(toString(args[0]), " \t\n\r")
}

func fnReplace(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	return strings.ReplaceAll(toString(args[0]), toString(args[1]), toString(args[2]))
}

func fnReverse(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	runes := []rune(toString(args[0]))
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func fnLeft(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	// LEFT counts CHARACTERS (#856). It is not reachable from SQL today — the
	// parser reserves LEFT and RIGHT for the join keywords — and it is fixed
	// with the rest of the family anyway, because a byte-indexing sibling left
	// behind is exactly the two-implementation drift this class keeps
	// producing.
	r := []rune(toString(args[0]))
	n := int(ToFloat64(args[1]))
	if n < 0 {
		return ""
	}
	if n >= len(r) {
		return string(r)
	}
	return string(r[:n])
}

func fnRight(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	// CHARACTERS, like LEFT above (#856).
	r := []rune(toString(args[0]))
	n := int(ToFloat64(args[1]))
	if n < 0 {
		return ""
	}
	if n >= len(r) {
		return string(r)
	}
	return string(r[len(r)-n:])
}

func fnStartsWith(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return strings.HasPrefix(toString(args[0]), toString(args[1]))
}

func fnEndsWith(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return strings.HasSuffix(toString(args[0]), toString(args[1]))
}

func fnContains(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return strings.Contains(toString(args[0]), toString(args[1]))
}

func fnRepeat(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	n := int(ToFloat64(args[1]))
	if n < 0 {
		return ""
	}
	return strings.Repeat(toString(args[0]), n)
}

// --- Network function implementations ---

func fnIPToString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	ip := net.ParseIP(s)
	if ip == nil {
		return nil
	}
	// A v6 SPELLING renders through the engine's one v6 renderer, so a
	// v4-mapped address prints the way its column prints (#580). net.ParseIP
	// stores a dotted quad in the same sixteen bytes, so the input's own
	// spelling is what separates the two families — the test CidrSortKey
	// makes as well.
	if strings.Contains(s, ":") {
		return batch.FormatIPv6(ip.To16())
	}
	return ip.String()
}

func fnCIDRContains(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[1]))
	if ip == nil {
		return nil
	}
	return network.Contains(ip)
}

func fnIPVersion(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	if ip.To4() != nil {
		return float64(4)
	}
	return float64(6)
}

func fnMaskIP(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	octets := int(ToFloat64(args[1]))
	ip4 := ip.To4()
	if ip4 != nil {
		// Mask last N octets of IPv4
		if octets < 0 || octets > 4 {
			return nil
		}
		masked := make(net.IP, 4)
		copy(masked, ip4)
		for j := 0; j < octets && j < 4; j++ {
			masked[3-j] = 0
		}
		return masked.String()
	}
	return nil
}

func fnMACToString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	hw, err := net.ParseMAC(s)
	if err != nil {
		return nil
	}
	return hw.String()
}

func fnIPSubnet(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	return network.IP.String()
}

func fnIPNetmask(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	mask := network.Mask
	// Convert mask to dotted notation for IPv4
	if len(mask) == 4 {
		return fmt.Sprintf("%d.%d.%d.%d", mask[0], mask[1], mask[2], mask[3])
	}
	// For IPv6, return hex representation
	return mask.String()
}

var (
	dateEpochDaysCache    temporalMemo // epoch days
	timestampEpochMsCache temporalMemo // epoch milliseconds
)

// --- Subquery expressions ---

// SubqueryRunner executes a SQL subquery and returns its result rows.
// Each row is a map of column name to value.
type SubqueryRunner func(sql string) ([]map[string]any, error)

// ScalarSubquery evaluates a subquery that returns a single scalar value.
// Example: WHERE price > (SELECT AVG(price) FROM products)
// Uncorrelated: executed once and result cached.
//
// The cache is shared by every parallel pipeline worker — one compiled
// expression tree is captured by all of them (Pipeline.runParallel) — so it
// is published the same way ColRef publishes its resolution: written under
// resolveMu, released by an atomic store, and never read before that store
// is observed. A plain `if !cached { cached = true; ... }` raced: a worker
// that saw the flag before the value was written compared against a nil
// threshold, dropped every row of its batches, and the query answered a
// different row count on every run (#398).
type ScalarSubquery struct {
	SQL    string
	Runner SubqueryRunner
	// Cols is the subquery's SELECT-list COLUMN COUNT, resolved from its own
	// plan at compile time — see refuseMultiColumnSubqueryByPlan.
	Cols SubqueryColumnsFunc
	// Scope resolves a relation's COMPLETE column list, so the dangling-
	// reference guard can tell a ROW FIELD PATH from a lost correlation
	// (#866). Nil keeps the pre-#866 answer.
	Scope plansql.TableColumns
	// Decl is the DECLARED type of the subquery's single output column, and
	// DeclKnown says whether anything resolved it (#696). It carries no value
	// and changes no evaluation: it exists so the boxed comparison can read
	// this operand as the number it IS.
	//
	// A DECIMAL boxes as its rendered TEXT, so without a declaration the pair
	// `a > (SELECT AVG(a) FROM decpair)` had a proven DECIMAL on one side and
	// an unclassifiable box on the other, fell through to compare()'s
	// LEXICOGRAPHIC rule, and answered 0 rows for PostgreSQL's 4 because
	// "12.75" sorts below "7.570000". DecPrecision/DecScale go with a DECIMAL
	// Decl for the same reason every other declaration carries them.
	Decl                   batch.TypeID
	DeclKnown              bool
	DecPrecision, DecScale int
	// resolved publishes val: stored last under resolveMu, and the only
	// thing an evaluating goroutine reads before using val.
	resolved  atomic.Bool
	resolveMu sync.Mutex
	val       any
}

func (e *ScalarSubquery) Eval(_ *batch.RecordBatch, _ int) any {
	if !e.resolved.Load() {
		e.resolveSlow()
	}
	return e.val
}

func (e *ScalarSubquery) resolveSlow() {
	e.resolveMu.Lock()
	defer e.resolveMu.Unlock()
	if e.resolved.Load() {
		return
	}
	defer e.resolved.Store(true)
	// The same guard ExistsSubquery.resolveSlow carries, for the same reason:
	// a scalar subquery this evaluator runs ONCE has to be one that really
	// reads no outer row. `WHERE (SELECT COUNT(*) FROM dim WHERE dim.k =
	// u.did) > 0` over a CTE was planned here and answered a query-wide
	// constant 0 on all four arms (#535).
	refuseDanglingSubquery("scalar", e.SQL, e.Scope)
	// BEFORE THE RUN: PostgreSQL decides the column count during parse
	// analysis, so an EMPTY multi-column subquery is 42601 there too.
	refuseMultiColumnSubqueryByPlan(e.Cols, e.SQL, false)
	// TWO ROWS, not the whole result: `> 1` is the entire cardinality rule,
	// so the read stops where the answer is known (plansql.AppendRowLimit).
	// e.SQL — not the bounded text — is what every error below names, because
	// the bound is this engine's business and the query is the user's.
	rows, err := e.Runner(plansql.WithRowLimit(e.SQL, 2))
	if err != nil {
		failEval(subqueryRunFailed("scalar", e.SQL, err))
	}
	// COLUMNS BEFORE ROWS — PostgreSQL's order; see refuseMultiColumnSubquery.
	refuseMultiColumnSubquery(e.SQL, rows, false)
	if len(rows) > 1 {
		// Reported with no count: the read stopped on purpose, so this site
		// knows "more than one" and not how many more.
		failEval(&ScalarSubqueryRowsError{SQL: e.SQL})
	}
	v, cardErr := ScalarSubqueryValue(e.SQL, rows)
	if cardErr != nil {
		failEval(cardErr)
	}
	e.val = v
}

// MemoryAccountant is the minimal per-task memory-budget hook InSubquery
// uses to charge its uncorrelated membership set (ADR-0006, #528). It is
// declared here rather than importing internal/engine/memory: expr has no
// other reason to depend on that package, and *memory.Tracker already has
// exactly this method set, so a caller that holds one satisfies this
// interface with no adapter — the seam CompileWithBudget threads through
// costs no new package dependency.
type MemoryAccountant interface {
	// Reserve charges n bytes against the budget, returning an error (which
	// InSubquery treats as a query error, never a silent no-op) if doing so
	// would exceed it.
	Reserve(n int64) error
	// Release returns n previously reserved bytes.
	Release(n int64)
}

// InSubquery checks if a value is in the result set of a subquery.
// Example: WHERE user_id IN (SELECT user_id FROM active_users)
// Uncorrelated: executed once and result set cached in a hash set for O(1) lookup.
type InSubquery struct {
	// Cols is the subquery's SELECT-list COLUMN COUNT — see
	// refuseMultiColumnSubqueryByPlan.
	Cols   SubqueryColumnsFunc
	Expr   Expr
	SQL    string
	Runner SubqueryRunner
	Not    bool
	// Scope resolves a relation's COMPLETE column list, so the dangling-
	// reference guard can tell a ROW FIELD PATH from a lost correlation
	// (#866). Nil keeps the pre-#866 answer.
	Scope plansql.TableColumns
	// Budget charges the membership set resolveSlow builds to the caller's
	// per-task memory tracker (ADR-0006, #528). nil (CompileWithRunner,
	// CompileWithScope, etc.) keeps the map unbudgeted, exactly as before
	// #528 — every shape that decorrelates into a semi join never reaches
	// this type at all (its build side is already budgeted and spillable);
	// only tryDecorrelateInSubquery's DECLINED shapes do, and only a
	// computed inner select item is unbounded (a LIMIT/OFFSET or an
	// ungrouped-aggregate inner item is bounded by construction).
	//
	// Set in production by expr.WithBudget, which the physical planner
	// passes at every compile site that carries a subquery runner (#531).
	// The option also hands the planner each node it budgets, because the
	// release side is the half that matters: a charge with no teardown point
	// makes every uncorrelated IN-subquery in a task hold its bytes for the
	// task's lifetime, and a task that plans several of them runs out of
	// budget for work that has already finished. WithBudget therefore
	// refuses a nil release hook rather than construct that state; the
	// teardown point is PhysicalPlan.Cleanup.
	Budget MemoryAccountant
	// SetBound bounds the membership set in ROWS, refusing past it rather
	// than truncating — a set short by one row is a different answer, and on
	// a write door it deletes the wrong rows. Zero is unbounded.
	//
	// It lives on this construct and not in the runner because a runner sees
	// SQL text and cannot tell which construct asked for it. IN is the one
	// that wants a SET; EXISTS wants a row and a scalar subquery is an error
	// past one, and both read a bounded number of rows by construction now
	// (plansql.AppendRowLimit). Bounding all three in the runner charged
	// those two for a set neither builds.
	SetBound int
	// resolved publishes the set: stored last under resolveMu. Same
	// contract, and the same defect, as ScalarSubquery's (#398) — an
	// unsynchronized flag let a parallel worker probe a half-built map.
	resolved  atomic.Bool
	resolveMu sync.Mutex
	// setNull records a NULL in the subquery's result set: a probe that
	// misses such a set is UNKNOWN, not false — the `NOT IN (SELECT
	// nullable_col ...)` trap (#370).
	setNull bool
	// emptySet records that the subquery returned NOTHING — not one value,
	// not even a NULL. It is a distinct case from setNull and from a
	// populated set, because an empty set is the one where a NULL PROBE key
	// is decided rather than UNKNOWN (see EvalBoolNull).
	emptySet bool
	intSet   map[int64]struct{}
	strSet   map[string]struct{}
	// fltSet is keyed by kernel.KeyFloat64Bits, not by the raw float64: a Go
	// map can never find a NaN key (NaN != NaN), and PostgreSQL's float order
	// says NaN EQUALS itself and -0.0 equals +0.0 (ADR-0012 item 8). Keyed
	// raw, `real IN (SELECT double …)` dropped the NaN row that the same
	// predicate as a JOIN — whose key canonicalises the bits (ADR-0023
	// item 1) — matched. It is a key; it folds what the comparator folds.
	fltSet map[uint64]struct{}
	// decSet is strSet keyed by batch.CanonicalDecimalText instead of by the
	// raw rendering, and it is consulted only when the PROBE is declared
	// DECIMAL. A DECIMAL boxes as its text at its own scale, so
	// `COALESCE(a, b) IN (SELECT COALESCE(a, b) FROM t WHERE …)` compared
	// "12.75" against the set's "12.7500" and answered zero rows where
	// PostgreSQL answers four — the row-at-a-time twin of #474, and a
	// two-path split besides, since the stage DAG lowers the same predicate
	// to a semi join keyed through the columnar encoding and got it right.
	// The gate is the DECLARATION, never the box's shape: a genuine STRING
	// column holding numeric-looking text still compares AS TEXT (#504).
	decSet map[string]struct{}
	// setNumericKind records what the SET's members are, which is half of the
	// rung this predicate compares at (#615 F2). PostgreSQL resolves
	// `probe IN (SELECT …)` by the same OPERATOR ladder a join key uses —
	// int ⊕ numeric → numeric, anything ⊕ float8 → float8, real ⊕ int →
	// float8 — and this type only ever consulted the set that matched the
	// probe's own box. Every cross-rung pair therefore missed EVERY member:
	// `numeric IN (SELECT float8)` answered 0 where PostgreSQL answers 7,
	// and its NOT IN answered 7 where PostgreSQL answers 0 — inventing rows,
	// not just dropping them.
	setNumericKind inSetKind
	// probe caches the settled kind of e.Expr, the same way every other
	// declaration-driven comparison site caches its operands'.
	probe boxOperand
	vals  []any // fallback for mixed types
	// chargedBytes is exactly what was handed to Budget.Reserve, guarded by
	// resolveMu, so Release returns exactly that many bytes exactly once.
	chargedBytes int64
}

func (e *InSubquery) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *InSubquery) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

func (e *InSubquery) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	if !e.resolved.Load() {
		e.resolveSlow()
	}
	// An EMPTY set is a real answer and not an absence, and it is checked
	// BEFORE the probe's own NULL because the NULL-keyed row is exactly the
	// one the general rule gets wrong: `x IN ()` is FALSE for every row and
	// `x NOT IN ()` is TRUE for every row, the NULL-keyed one included,
	// because an empty set offers no comparison to be UNKNOWN about. This is
	// the same boundary the semi/anti lowering states (#507) and the
	// materialized IN-set renders as a constant (ADR-0021 §2); the subquery-
	// predicate route stated it nowhere and dropped the NULL-keyed row.
	if e.emptySet {
		return e.Not, false
	}
	lv := e.Expr.Eval(b, row)
	if lv == nil {
		return false, true
	}
	// The RUNG first: `x IN (SELECT y …)` is `x = y` quantified, so it is
	// resolved by PostgreSQL's operator ladder over (probe, set) — not by
	// whichever typed set happens to match the probe's Go box, which is what
	// this used to do and why every cross-rung pair missed every member
	// (#615 F2).
	if e.setNumericKind != inSetOther {
		_, probeIsInt := toInt64SafeStrict(lv)
		switch inSubqueryRung(e.probe.resolve(b), probeIsInt, e.setNumericKind) {
		case inSetDecimal:
			// EXACT, at the value's own digits: numeric ⊕ integer.
			if key, ok := inSubqueryDecimalKey(lv); ok && e.decSet != nil {
				if _, found := e.decSet[key]; found {
					return !e.Not, false
				}
				return e.missAnswer()
			}
		case inSetFloat:
			// float8, the numeric category's preferred type. A DECIMAL probe
			// reads through its exact text, which is `numeric::float8`.
			if fv, ok := inSubqueryFloat(lv); ok && e.fltSet != nil {
				if _, found := e.fltSet[kernel.KeyFloat64Bits(fv)]; found {
					return !e.Not, false
				}
				return e.missAnswer()
			}
		case inSetInt:
			if iv, ok := toInt64Safe(lv); ok && e.intSet != nil {
				if _, found := e.intSet[iv]; found {
					return !e.Not, false
				}
				return e.missAnswer()
			}
		}
	}
	// Fast path: typed hash lookup
	if e.intSet != nil {
		if iv, ok := toInt64Safe(lv); ok {
			if _, found := e.intSet[iv]; found {
				return !e.Not, false
			}
			return e.missAnswer()
		}
	}
	if e.strSet != nil {
		if sv, ok := lv.(string); ok {
			// A DECIMAL probe keys by VALUE, not by rendering: see decSet.
			if e.decSet != nil && e.probe.resolve(b) == boxDecimal {
				if key, ok := batch.CanonicalDecimalText(sv); ok {
					if _, found := e.decSet[key]; found {
						return !e.Not, false
					}
					return e.missAnswer()
				}
			}
			if _, found := e.strSet[sv]; found {
				return !e.Not, false
			}
			return e.missAnswer()
		}
	}
	if e.fltSet != nil {
		if fv, ok := toFloat64Safe(lv); ok {
			if _, found := e.fltSet[kernel.KeyFloat64Bits(fv)]; found {
				return !e.Not, false
			}
			return e.missAnswer()
		}
	}
	// Fallback: linear scan for mixed types
	for _, rv := range e.vals {
		if rv != nil && compare(lv, rv, CmpEq) {
			return !e.Not, false
		}
	}
	return e.missAnswer()
}

// resolveSlow runs the subquery once and builds the probe set. Idempotent.
// inSetKind is what an IN-subquery's materialised value set holds. It is the
// SET half of the operator ladder; boxOperand.resolve gives the probe half.
type inSetKind int8

const (
	inSetOther   inSetKind = iota
	inSetInt               // int64 members: an INTEGER in PostgreSQL's terms
	inSetFloat             // float64 members: float8
	inSetDecimal           // decimal TEXT members: numeric
)

// inSubqueryRung is the type this predicate compares at, by PostgreSQL's
// operator resolution — the same ladder physical.joinKeyCommonType applies to
// a join key, because `x IN (SELECT y …)` IS `x = y` quantified:
//
//	numeric ⊕ integer  -> numeric   (exact, at the value's own digits)
//	numeric ⊕ float8   -> float8
//	integer ⊕ float8   -> float8
//	real    ⊕ anything -> float8    (a real boxes as a float64 already)
//
// probeIsInt distinguishes the two boxNumber cases at the row: an int64 box
// against a decimal set is the exact rung, a float box against the same set is
// the float one.
func inSubqueryRung(probe boxKind, probeIsInt bool, set inSetKind) inSetKind {
	switch set {
	case inSetInt:
		switch {
		case probe == boxDecimal:
			return inSetDecimal
		case probe == boxNumber && !probeIsInt:
			return inSetFloat
		}
		return inSetInt
	case inSetDecimal:
		switch {
		case probe == boxDecimal:
			return inSetDecimal
		case probe == boxNumber && probeIsInt:
			return inSetDecimal
		case probe == boxNumber:
			return inSetFloat
		}
		return inSetOther
	case inSetFloat:
		if probe == boxDecimal || probe == boxNumber {
			return inSetFloat
		}
		return inSetOther
	}
	return inSetOther
}

// inSubqueryDecimalKey reads a probe box as a canonical DECIMAL key: a
// DECIMAL boxes as its rendered text, an integer as an int64, and the two are
// one numeric value to PostgreSQL.
func inSubqueryDecimalKey(lv any) (string, bool) {
	switch v := lv.(type) {
	case string:
		return batch.CanonicalDecimalText(v)
	default:
		if iv, ok := toInt64Safe(lv); ok {
			return batch.CanonicalDecimalText(strconv.FormatInt(iv, 10))
		}
		_ = v
	}
	return "", false
}

// inSubqueryFloat reads a probe box as the float64 PostgreSQL would compare
// at. A DECIMAL's text goes through ParseFloat, which is the correctly
// rounded `numeric::float8`.
func inSubqueryFloat(lv any) (float64, bool) {
	if sv, ok := lv.(string); ok {
		f, err := strconv.ParseFloat(sv, 64)
		return f, err == nil
	}
	return toFloat64Safe(lv)
}

// toInt64SafeStrict is toInt64Safe restricted to boxes that ARE integers, so
// a float64 box does not answer true for a whole-numbered value — the rung
// for `float8 IN (SELECT numeric)` is float8 whether or not the row happens
// to hold 2.0.
func toInt64SafeStrict(lv any) (int64, bool) {
	switch v := lv.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	}
	return 0, false
}

func (e *InSubquery) resolveSlow() {
	e.resolveMu.Lock()
	defer e.resolveMu.Unlock()
	if e.resolved.Load() {
		return
	}
	defer e.resolved.Store(true)
	// Bound here rather than at construction so every caller gets it: the
	// write is under resolveMu and published by the Store above, the same
	// release the value set itself rides.
	e.probe.expr = e.Expr
	// The same guard the other two uncorrelated evaluators carry: a set this
	// resolves ONCE has to be one that reads no outer row (#734/#679/#535).
	refuseDanglingSubquery("IN", e.SQL, e.Scope)
	refuseMultiColumnSubqueryByPlan(e.Cols, e.SQL, true)
	rows, err := e.Runner(e.SQL)
	if err != nil {
		// NOT an empty set. Treating the failure as "every probe misses" is
		// the same fold the correlated evaluators made, one construct over:
		// a membership set that could not be built has no answer, and
		// answering FALSE for every row is a confident wrong one.
		failEval(subqueryRunFailed("IN", e.SQL, err))
	}
	if e.SetBound > 0 && len(rows) > e.SetBound {
		failEval(&InSetTooLargeError{SQL: e.SQL, Rows: len(rows), Bound: e.SetBound})
	}
	// Collect values and detect predominant type for hash set
	var rawVals []any
	{
		for _, r := range rows {
			// PostgreSQL refuses a multi-column IN subquery outright (42601,
			// `subquery has too many columns`). Taking "the first column" out
			// of a Go MAP instead built the set from a DIFFERENT column on
			// different runs of the same query, because map iteration order is
			// randomized per range statement. A one-column subquery whose
			// pipeline emitted a hidden ORDER BY key beside it is #875 and is
			// trimmed where the pipeline is built, so a row with two entries
			// here is a genuine two-column SELECT list.
			if len(r) > 1 {
				failEval(&SubqueryColumnsError{SQL: e.SQL, Columns: len(r), InPredicate: true})
			}
			for _, v := range r {
				if v != nil {
					rawVals = append(rawVals, v)
				} else {
					e.setNull = true
				}
			}
		}
		// Build typed hash set. Use toInt64Safe/toFloat64Safe to normalize
		// all integer types (int32, int64, int) and float types (float32, float64).
		if len(rawVals) > 0 {
			if _, ok := toInt64Safe(rawVals[0]); ok {
				// An INTEGER set, carried in all three spellings the ladder
				// can ask for: exactly (intSet), as canonical DECIMAL text
				// for a numeric probe, and as float64 for a float one. Every
				// conversion here is exact — an int64 that does not survive
				// float64 is the 2^53 case, and PostgreSQL rounds it too.
				e.setNumericKind = inSetInt
				e.intSet = make(map[int64]struct{}, len(rawVals))
				e.decSet = make(map[string]struct{}, len(rawVals))
				e.fltSet = make(map[uint64]struct{}, len(rawVals))
				for _, v := range rawVals {
					if iv, ok := toInt64Safe(v); ok {
						e.intSet[iv] = struct{}{}
						e.fltSet[kernel.KeyFloat64Bits(float64(iv))] = struct{}{}
						if key, ok := batch.CanonicalDecimalText(strconv.FormatInt(iv, 10)); ok {
							e.decSet[key] = struct{}{}
						}
					} else {
						e.vals = rawVals
						e.intSet, e.decSet, e.fltSet = nil, nil, nil
						e.setNumericKind = inSetOther
						break
					}
				}
			} else if _, ok := rawVals[0].(string); ok {
				e.strSet = make(map[string]struct{}, len(rawVals))
				e.decSet = make(map[string]struct{}, len(rawVals))
				allDecimal := true
				for _, v := range rawVals {
					if sv, ok := v.(string); ok {
						e.strSet[sv] = struct{}{}
						if key, ok := batch.CanonicalDecimalText(sv); ok {
							e.decSet[key] = struct{}{}
						} else {
							allDecimal = false
						}
					} else {
						e.vals = rawVals
						e.strSet, e.decSet = nil, nil
						allDecimal = false
						break
					}
				}
				// A set every one of whose members is decimal text IS a
				// numeric set, and a FLOAT probe compares against it at
				// float8. ParseFloat is the correctly-rounded reading, which
				// is what `numeric::float8` does.
				if allDecimal && e.strSet != nil {
					e.setNumericKind = inSetDecimal
					e.fltSet = make(map[uint64]struct{}, len(rawVals))
					for sv := range e.strSet {
						if fv, err := strconv.ParseFloat(sv, 64); err == nil {
							e.fltSet[kernel.KeyFloat64Bits(fv)] = struct{}{}
						}
					}
				}
			} else if _, ok := toFloat64Safe(rawVals[0]); ok {
				// A FLOAT set is float8 against everything (PostgreSQL's
				// preferred type of the numeric category), so it gets NO
				// decimal view: an exact comparison against it would be a
				// different predicate.
				e.setNumericKind = inSetFloat
				e.fltSet = make(map[uint64]struct{}, len(rawVals))
				for _, v := range rawVals {
					if fv, ok := toFloat64Safe(v); ok {
						e.fltSet[kernel.KeyFloat64Bits(fv)] = struct{}{}
					} else {
						e.vals = rawVals
						e.fltSet = nil
						e.setNumericKind = inSetOther
						break
					}
				}
			} else {
				e.vals = rawVals
			}
		}
		// Nothing at all came back — not a value and not a NULL.
		e.emptySet = len(rawVals) == 0 && !e.setNull
	}
	e.chargeMemory()
}

// chargeMemory reserves the membership set's estimated heap footprint
// against Budget (ADR-0006, #528). A nil Budget (every compile path except
// CompileWithBudget) is a no-op, exactly the pre-#528 behavior — this is
// what makes the fix opt-in rather than a change to every existing caller.
//
// Called once, from inside resolveSlow's already-held resolveMu, after the
// set this query holds has been decided, so the estimate matches exactly
// what stays reachable for the life of this InSubquery.
//
// AFTER, which bounds what this can do. The map is already built and already
// resident by the time a byte is charged, so a subquery large enough to
// exhaust the machine exhausts it before Reserve is ever called: this does
// not PREVENT that OOM, and #528's issue text describing it as doing so is
// wrong. What it does do is make the set VISIBLE to the task's budget — it
// counts against every later allocation the task makes, and a set that is
// over budget on its own turns into a query error instead of a permanently
// unaccounted resident map that every other operator then has to fit
// alongside. That is worth having and is what ADR-0006 asks of a structure
// that cannot spill; it is not the same claim.
//
// Charging as the set is BUILT — a Reserve per N rows inside resolveSlow's
// accumulation loop, refusing partway — is what would actually bound peak
// resident bytes. It needs resolveSlow to have somewhere to put a partial
// failure and a caller that can act on one, so it is a change to this
// type's contract rather than to this function. Not attempted here.
func (e *InSubquery) chargeMemory() {
	if e.Budget == nil {
		return
	}
	n := inSubqueryMemBytes(e.intSet, e.strSet, e.fltSet, e.vals)
	if n <= 0 {
		return
	}
	if err := e.Budget.Reserve(n); err != nil {
		// A giant uncorrelated IN-subquery is exactly the shape ADR-0006
		// exists to refuse rather than let grow an unbudgeted map until the
		// process OOMs (#528) — raised the same way every other expr-level
		// query error is (fatal.go), since EvalBoolNull has no error return
		// to propagate one through.
		panic(fatalEval{fmt.Errorf("IN subquery membership set: %w", err)})
	}
	e.chargedBytes = n
}

// Release returns any bytes charged to Budget in resolveSlow, and is
// idempotent — a caller that does not know whether resolveSlow ever ran, or
// has already called Release, may call it any number of times.
//
// Its caller is the plan that compiled the tree: the physical planner
// registers every InSubquery compiled under a budget (expr.WithBudget's
// release hook) and PhysicalPlan.Cleanup releases them, which is the
// teardown point #531 needed and #528 left open. Wiring the charge WITHOUT
// one converts an unbudgeted map into a permanently-charged one — a worse
// failure than the one #528 set out to fix, because the bytes are returned
// to the OS by GC and never returned to the tracker — which is why
// WithBudget refuses to construct a budget with a nil release hook.
func (e *InSubquery) Release() {
	if e.Budget == nil {
		return
	}
	e.resolveMu.Lock()
	n := e.chargedBytes
	e.chargedBytes = 0
	e.resolveMu.Unlock()
	if n > 0 {
		e.Budget.Release(n)
	}
}

// inSubqueryMapEntryOverhead approximates a Go map's per-entry bookkeeping:
// a bucket holds up to 8 (key, value) pairs plus one tophash byte and an
// amortized share of an overflow-bucket pointer per entry, which rounds up
// to about this many bytes on top of the key/value payload itself. This
// does not need to be exact — Reserve's job is to catch an unbounded
// subquery before it outgrows the task's budget, not to account every byte
// precisely — and rounding up is the safe direction for a budget check.
const inSubqueryMapEntryOverhead = 16

// inSubqueryMemBytes estimates the heap footprint of whichever membership
// set resolveSlow built — at most one of intSet/strSet/fltSet/vals is
// non-nil, matching resolveSlow's mutually exclusive construction.
func inSubqueryMemBytes(intSet map[int64]struct{}, strSet map[string]struct{}, fltSet map[uint64]struct{}, vals []any) int64 {
	switch {
	case intSet != nil:
		return int64(len(intSet)) * (8 + inSubqueryMapEntryOverhead)
	case fltSet != nil:
		return int64(len(fltSet)) * (8 + inSubqueryMapEntryOverhead)
	case strSet != nil:
		var n int64
		for k := range strSet {
			n += int64(len(k)) + 16 /* string header */ + inSubqueryMapEntryOverhead
		}
		return n
	case len(vals) > 0:
		var n int64
		for _, v := range vals {
			n += 16 // interface header (type word + data word)
			if s, ok := v.(string); ok {
				n += int64(len(s))
			} else {
				n += 8 // scalar payload estimate
			}
		}
		return n
	}
	return 0
}

// missAnswer is the IN answer for a probe the set does not contain: FALSE
// (or TRUE under NOT) for a NULL-free set, UNKNOWN when the set held a NULL.
func (e *InSubquery) missAnswer() (bool, bool) {
	if e.setNull {
		return false, true
	}
	return e.Not, false
}

// ExistsSubquery evaluates to true if a subquery returns any rows.
// Example: WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
// Uncorrelated: executed once and result cached.
type ExistsSubquery struct {
	SQL    string
	Runner SubqueryRunner
	Not    bool
	// Scope resolves a relation's COMPLETE column list, so the dangling-
	// reference guard can tell a ROW FIELD PATH from a lost correlation
	// (#866). Nil keeps the pre-#866 answer.
	Scope plansql.TableColumns
	// resolved publishes exists: stored last under resolveMu. Same
	// contract, and the same defect, as ScalarSubquery's (#398).
	resolved  atomic.Bool
	resolveMu sync.Mutex
	exists    bool
}

func (e *ExistsSubquery) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *ExistsSubquery) EvalBool(_ *batch.RecordBatch, _ int) bool {
	if !e.resolved.Load() {
		e.resolveSlow()
	}
	if e.Not {
		return !e.exists
	}
	return e.exists
}

func (e *ExistsSubquery) resolveSlow() {
	e.resolveMu.Lock()
	defer e.resolveMu.Unlock()
	if e.resolved.Load() {
		return
	}
	// This evaluator runs the subquery ONCE, query-wide, and memoizes — which
	// is only sound if the subquery really is uncorrelated. When the
	// classifier missed a correlation (an EXISTS inside an aggregate ARGUMENT
	// is compiled with no outer scope at all, #734) the text still names the
	// outer relation, and standalone `ResolveColumnRef` strips the qualifier
	// and rebinds it — so this answered a query-wide CONSTANT, TRUE or FALSE
	// according to whether the two relations happened to share a column name.
	// Checked once here, where it costs one parse per query (#734/#679/#535).
	refuseDanglingSubquery("EXISTS", e.SQL, e.Scope)
	// ONE ROW. EXISTS asks whether there is a row; the first one answers it.
	rows, err := e.Runner(plansql.WithRowLimit(e.SQL, 1))
	if err != nil {
		// `err == nil && len(rows) > 0` made a failure indistinguishable
		// from an empty result. They are not the same thing.
		failEval(subqueryRunFailed("EXISTS", e.SQL, err))
	}
	e.exists = len(rows) > 0
	e.resolved.Store(true)
}

// Cast wraps an expression with explicit type conversion.
type Cast struct {
	Operand  Expr
	DestType string // "int", "float", "string", "date", "timestamp"

	// boolSrc caches the OPERAND's declared type for a cast to BOOLEAN, which
	// selects the conversion rule (cast_bool.go). Zero means "not resolved
	// yet"; nothing else in this node needs it, and no other destination
	// reads it.
	boolSrc atomic.Int32
	// decDest caches the parsed DECIMAL destination, for the same reason:
	// `DECIMAL(10, 2)` is fixed for the query and re-parsing the type name
	// per row cost a string walk on every value (cast_decimal.go).
	decDest castDecimalState
	// strDest caches the parsed VARCHAR(n) / CHAR(n) length, for the same
	// reason again (cast_string_length.go, #838).
	strDest castStringState
}

func (e *Cast) Eval(b *batch.RecordBatch, row int) any {
	v := e.Operand.Eval(b, row)
	if v == nil {
		return nil
	}
	// One normalization for both the temporal check and the switch: this
	// runs per row, and a WHERE over a typed date literal evaluates it once
	// per row of the scan.
	dest := strings.ToLower(e.DestType)
	if k := castTemporalKindLower(strings.TrimSpace(dest)); k != castNotTemporal {
		return castTemporal(b, row, e.Operand, v, k)
	}
	// A DECIMAL destination is resolved before the switch because its type
	// name CARRIES its parameters — `decimal(10, 2)` matches no case label,
	// and used to reach `default: return v`, which passed the value through
	// with the (p,s) silently ignored (ADR-0024 item 3, #555).
	if d, ok := e.decimalDestination(); ok {
		return e.castToDecimal(b, row, v, d)
	}
	// A length-carrying STRING destination has the SAME shape and had the
	// same defect: `varchar(4)` matches no case label either, so the whole
	// cast reached `default: return v` and returned six characters where
	// PostgreSQL returns four (#838).
	if n, ok := e.stringDestination(); ok {
		return truncateToChars(castStringRender(b, row, e.Operand, v), n)
	}
	// FLOAT(n), the third parameterized destination and the third one that
	// matched no case label: `float(1)` reached `default: return v` and
	// answered a double under a STRING declaration (#652). PostgreSQL
	// resolves it by WIDTH — float(1..24) is real, float(25..53) is double —
	// so the narrow half takes the REAL arm's rounding and its 22003 range
	// check rather than a second copy of them.
	if bits, err, ok := parquet.FloatTypePrecision(e.DestType); ok {
		if err != nil {
			panic(fatalEval{err})
		}
		if bits <= 24 {
			return e.castToReal(v)
		}
		if f, isText := castFloatText(v, "double precision", 64); isText {
			return f
		}
		return ToFloat64(v)
	}
	switch dest {
	// Keep this label list and IsIntegerCastDest in step: that predicate is
	// what tells the DAG's gather materialization to build an INT64 vector
	// for this destination (#813), and a label here it does not know would
	// put the same query's answer in a float64 one.
	// INT32, PORT and PROTOCOL are here because they are integer destinations
	// this engine HAS and this switch did not implement: all three fell to
	// `default: return v` and answered the operand unchanged under a STRING
	// declaration, so `3000000000::INT32` answered 3000000000 where
	// PostgreSQL raises `integer out of range` for the same magnitude, and
	// `3000000000::PORT` answered it under a type whose whole carrier is a
	// signed 32-bit field (#901). castIntInRange carries their bound; PORT
	// and PROTOCOL then reach a PORT/PROTOCOL vector, whose own int4 guard is
	// the second net (batch.IntegerRangeError).
	case "int", "integer", "int4", "int32", "bigint", "int8", "signed", "smallint", "int2",
		"port", "protocol":
		// A string that does not read as a number is refused, not coerced to
		// 0: PostgreSQL raises 22P02 invalid_text_representation and ADR-0012
		// makes it the authority on error-versus-not. The per-row error
		// channel #340 lacked exists now — FatalEvalPanic, #347 — which is
		// what this raise rides.
		//
		// A value that DOES read as a number then follows PostgreSQL's other
		// rule (#373): a fractional cast to an integer type ROUNDS, half away
		// from zero. TRUNC() is how a caller asks for truncation. An
		// already-integral value passes through untouched.
		// An exact DECIMAL is read on its own carrier, not through a double:
		// strconv.ParseFloat loses every digit past the sixteenth, so a
		// DECIMAL(38,10) holding 493827160549382.7160549350 came back as the
		// nearest double's integer part, and a value past the destination's
		// range came back as whatever the float conversion produced instead
		// of the refusal PostgreSQL gives (ADR-0024 item 4).
		if i, ok := castDecimalToInt(v, dest); ok {
			return i
		}
		if s, ok := stringOperand(v); ok {
			typ := "integer"
			if dest == "bigint" || dest == "int8" || dest == "signed" {
				typ = "bigint"
			}
			// PostgreSQL's INTEGER input grammar FIRST, which is a strict
			// superset of Go's base-10 one: `'0x1A'::integer` is 26 there,
			// `'0o17'` 15, `'0b101'` 5, `'1_000'` 1000 and `'017'` decimal
			// seventeen. kernel.IntLitText is the one reader the comparison
			// kernels, the row path and the plan-time refusal already share,
			// so the CAST door cannot disagree with them about which strings
			// name an integer (#634).
			switch n, st := kernel.IntLitText(s); st {
			case kernel.NumConstOK:
				return castIntInRange(n, dest)
			case kernel.NumConstRange:
				raiseNumericOutOfRange(typ, s)
			}
			// Not an integer under that grammar. A FRACTIONAL string still
			// casts — PostgreSQL rounds `'26.7'::integer` to 27 — so the
			// float reader is the fallback, not the first try.
			f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
			if err != nil {
				raiseInvalidTextRepresentation(typ, s)
			}
			return castIntInRange(castFloatToInt64(f, dest), dest)
		}
		// EVERY source gets the destination's range, integers included:
		// `CAST(99999 AS SMALLINT)` answered 99999 because an integer box
		// returned before the check, and PostgreSQL raises `smallint out of
		// range` for it (#555 review).
		if i, ok := toInt64Safe(v); ok {
			return castIntInRange(i, dest)
		}
		// A FLOAT source rounds HALF TO EVEN, which is PostgreSQL's rint()
		// and C's default rounding mode: `-0.5::float8::int` is 0 there and
		// this engine answered -1, `0.5` is 0 and this answered 1, `2.5` is 2
		// and this answered 3. Measured live on 17 (#768).
		//
		// A CONSTANT operand does NOT: PostgreSQL types a bare `-0.5`
		// numeric, and its numeric-to-integer cast rounds HALF AWAY FROM ZERO
		// (`CAST(-0.5 AS int)` is -1 there, `CAST(2.5 AS int)` is 3). The two
		// sources round differently on the same server and so must these. The
		// operand's box cannot tell them apart — a bare numeric literal is a
		// float64 here, which is ADR-0024's recorded literal-typing deferral
		// — so the distinction is made from the EXPRESSION: a literal, or a
		// unary sign over one, is a constant. That is the same test
		// physical.isConstNumericLitNode makes for the same reason, and it
		// covers both spellings because `-0.5` parses as a UnaryOp and `0.5`
		// as a Lit, and covering only one made the two halves of one query
		// disagree about their own type (#668's note).
		if isConstNumericOperand(e.Operand) {
			return castIntInRange(castFloatToInt64(ToFloat64(v), dest), dest)
		}
		return castIntInRange(castFloatToInt64Even(ToFloat64(v), dest), dest)
	// FLOAT32 is the same gap one family over: it is this engine's own name
	// for float4 and matched no label, so `CAST(1e40 AS FLOAT32)` answered
	// 1e+40 as TEXT where `CAST(1e40 AS REAL)` raises 22003 (#901).
	case "real", "float4", "float32":
		// REAL is float4, a NARROWER type than the float64 every other
		// numeric box in this engine carries — and this arm used to sit
		// beside "float"/"double" and answer ToFloat64, so `CAST(x AS REAL)`
		// was a NO-OP. PostgreSQL types the result float4 and rounds the
		// value to it, which changes the answer of anything that compares it:
		//
		//	r_val = CAST(3.1 AS REAL)  ->  Filter: (r_val = '3.1'::real) -> the row
		//	CAST(1.0/3 AS REAL)        ->  0.33333334, not 0.3333333333333333
		//
		// FLOAT is deliberately NOT here. PostgreSQL's bare `float` is
		// `double precision` (float(1..24) is real, float(25..53) is double,
		// and an unqualified FLOAT is the latter) — verified with pg_typeof —
		// so only the two spellings that really name float4 narrow.
		return e.castToReal(v)
	case "float", "double", "float8", "double precision", "float64":
		// Same hole, the wider destination: `CAST('abc' AS DOUBLE PRECISION)`
		// answered 0, a plausible measurement where PostgreSQL raises 22P02.
		if f, isText := castFloatText(v, "double precision", 64); isText {
			return f
		}
		return ToFloat64(v)
	case "uuid":
		return castToUUID(v)
	case "bool", "boolean":
		// The conversion the operand's DECLARATION selects, not the one its
		// Go box suggests — see cast_bool.go for what each source type
		// answers and why the box cannot decide it.
		return e.castToBool(b, v)
	case "char", "varchar", "text", "string":
		// A BYTES operand boxes as a raw []byte — both here and from
		// GetValue, since ColRef.Eval has no divergent fast path for
		// TypeBytes the way it does for the four types boxedTextOperand
		// resolves — and PostgreSQL's `bytea::text` is `\x` followed by
		// LOWERCASE hex, under the default bytea_output = hex. That is the
		// rendering, per ADR-0012 item 1: PostgreSQL gives BYTES a printed
		// form, so wadjet does not invent a second one.
		//
		// Two earlier answers were both wrong. fmt.Sprint's default verb
		// printed Go's slice-of-decimal-bytes debug notation
		// ("[98 121 116 ...]"), and the raw bytes as a Go string — which
		// agreed with kernel.likeTextRenderer but not with PostgreSQL —
		// produced, for 0xff 0xfe 0x00 0x41, a string that is invalid UTF-8
		// and holds an embedded NUL. No PostgreSQL server can put a NUL in
		// a text-format DataRow field, and libpq TRUNCATES at one, so the
		// same query answered four bytes to pgx and two to psql. The hex
		// form is pure ASCII and has neither problem (#570).
		//
		// LIKE deliberately does NOT follow it here: PostgreSQL's `~~` over
		// bytea is BYTEWISE (verified live — `'\xfffe0041'::bytea LIKE
		// '%A%'` is true, matching the 0x41 byte, not the letter in a hex
		// spelling), so kernel.likeTextRenderer keeps matching the raw
		// bytes. The two disagree in PostgreSQL, so they disagree here.
		return castStringRender(b, row, e.Operand, v)
	default:
		return v
	}
}

// castToReal narrows a value to float4, which is what `REAL`, `FLOAT4` and
// `FLOAT(1..24)` all name — one function so the three spellings cannot round
// differently (#652).
//
// REAL is a NARROWER type than the float64 every other numeric box in this
// engine carries, and this arm used to sit beside "float"/"double" and answer
// ToFloat64, so `CAST(x AS REAL)` was a NO-OP. PostgreSQL types the result
// float4 and rounds the value into it, which changes the answer of anything
// that compares it:
//
//	r_val = CAST(3.1 AS REAL)  ->  Filter: (r_val = '3.1'::real) -> the row
//	CAST(1.0/3 AS REAL)        ->  0.33333334, not 0.3333333333333333
//
// Bare FLOAT is deliberately NOT here. PostgreSQL's unqualified `float` is
// `double precision` — verified with pg_typeof — so only the spellings that
// really name float4 narrow.
func (e *Cast) castToReal(v any) any {
	// TEXT is read by real's own input function, which REFUSES what it
	// cannot read rather than answering ToFloat64's zero (#839's sibling
	// hole: `CAST('abc' AS REAL)` answered 0).
	f, isText := castFloatText(v, "real", 32)
	if !isText {
		f = ToFloat64(v)
	}
	// PostgreSQL refuses a conversion that loses the value outright rather
	// than answering an infinity or a zero (float.c's overflow and underflow
	// checks, both SQLSTATE 22003). A value that is ALREADY infinite, or
	// already zero, is representable and passes through. kernel.Float32FitOf
	// is the one place that rule lives — the IN-list refusals read it too, so
	// `CAST(x AS REAL)` and `x IN (lit)` cannot disagree about what a real can
	// hold.
	if fit := kernel.Float32FitOf(f); fit != kernel.Float32Fits {
		raiseRealConversionError(e.Operand, f, fit)
	}
	return float32(f)
}

// castStringRender is the TEXT a CAST to the string family produces, for the
// unparameterized destinations and for VARCHAR(n) / CHAR(n) alike. It is one
// function because the two must not drift: `CAST(ts AS TEXT)` and
// `CAST(ts AS VARCHAR(4))` render the same instant, and the second is the
// first cut to four characters (#838). See the arm above for what each source
// family renders as and why.
func castStringRender(b *batch.RecordBatch, row int, operand Expr, v any) string {
	text := boxedTextOperand(b, row, operand, v)
	if raw, ok := text.([]byte); ok {
		return `\x` + hex.EncodeToString(raw)
	}
	if s, ok := stringOperand(text); ok {
		return s
	}
	return fmt.Sprint(text)
}

// boxedTextOperand renders a bare-column operand as the text the column's
// own value PRINTS as — which is, by construction, the text the vectorized
// kernel matches/renders against (kernel.likeTextRenderer's default arm is
// fmt.Sprint(Vector.GetValue(i)), and its per-type arms were written to agree
// with that rendering; CAST AS STRING's other arms and every scalar function
// argument already use the same GetValue rendering for every OTHER type,
// via ColRef.Eval's own default case). Mirrors temporalOperand's contract:
// only a bare column reference is resolved, and every other operand shape
// (an already-string value, a nested expression, a literal) passes v through
// unchanged.
//
// ColRef.Eval boxes four types differently from GetValue, for speed on the
// numeric paths that dominate it, and all four made a caller here match or
// render a DIFFERENT STRING from the one the scan's kernel or the plain
// projection would — the same query answering two ways depending on which
// evaluator reached the column:
//
//	IPv4, MAC  the raw encoded int64, so `ipv4_col LIKE '10.%'` matched the
//	           digits of that integer instead of the address text, and
//	           `CAST(ipv4_col AS STRING)` stringified the number
//	DATE       the epoch DAY, so `c_date LIKE '20%'` was false for
//	           2011-02-02 and true for the day number 20123, and
//	           `CAST(c_date AS STRING)` answered "15007" instead of the date
//	FLOAT32    widened to float64, so 1/7 printed 0.1428571492433548 here and
//	           0.14285715 through the kernel or a bare projection
//
// The IPv4/MAC LIKE pair was fixed with #497; DATE and FLOAT32's LIKE
// rendering were found by the review of it. This function used to be two
// near-identical copies — likeOperand (LIKE's call site, all four types) and
// networkOperand (Cast's, IPv4/MAC only) — which is exactly the two-
// implementation drift ADR-0012 keeps calling out elsewhere (CidrSortKey,
// appendColumnValue): CAST(date_col AS STRING) and CAST(f32_col AS STRING)
// were still wrong through networkOperand's narrower list (#521) after LIKE
// had already been fixed for the identical types. One function, every
// caller, closes both at once: `wadjet.TestLikeAnswersTheSameAtBothSites`
// sweeps every flat type through the LIKE call site so a fifth type that
// starts boxing differently is a failing test rather than another quiet
// divergence.
func boxedTextOperand(b *batch.RecordBatch, row int, operand Expr, v any) any {
	// A CAST to a temporal type boxes its result exactly as the matching
	// COLUMN does (#340), so it needs the same undoing — and it did not get
	// it, because this resolver took only a bare column reference:
	// `CAST(CAST('1996-03-13 14:25:36' AS TIMESTAMP) AS TEXT)` answered
	// "826727136000" on the wire while the same cast over a COLUMN answered
	// the instant. FuncCall.formatTemporalArgs has had this arm since #273;
	// this is the same rule at the other text site (#544).
	if c, ok := operand.(*Cast); ok {
		ms, isInt := v.(int64)
		if !isInt {
			return v
		}
		switch castTemporalKind(c.DestType) {
		case castToDateKind:
			return batch.FormatDate(int32(ms))
		case castToTimestampKind:
			return batch.FormatTimestamp(ms)
		}
		return v
	}
	cr, ok := operand.(*ColRef)
	if !ok {
		return v
	}
	// A ROW FIELD PATH boxes exactly as a column of the field's type does
	// (ColRef.fieldValue), so it needs the same undoing — and gets it from
	// the same list, keyed on the FIELD's type (#568).
	switch cr.valueType() {
	case batch.TypeIPv4, batch.TypeMAC, batch.TypeDate, batch.TypeFloat32:
		dv, ok := cr.displayValue(b, row)
		if !ok {
			return v
		}
		return dv

	case batch.TypeTimestamp:
		// TIMESTAMP is the one type on this list whose display form
		// Vector.GetValue does NOT produce: it keeps the raw epoch-ms int64,
		// deliberately and with five consumers that need it (the GROUP BY
		// key, the aggregate and window spill row encoding, the window
		// comparator, the row map an UPDATE re-ingests) — see the comment on
		// that arm. So the rendering happens HERE, at the text site, from
		// the column's DECLARED type, which is the split FormatTimestamp's
		// own doc describes.
		//
		// Without it `CAST(c_ts AS STRING)` answered "1700000000000" and
		// `c_ts LIKE '2023%'` was false for 2023-11-14 (#544) — while the
		// SAME column projected over pgwire arrives as PostgreSQL's
		// `timestamp` text, because the send path converts under OID 1114
		// (#321). One connection, one column, two answers.
		//
		// displayValue is used for the read rather than Int64Data directly
		// because it goes through GetValue, which resolves a dictionary or
		// selection VIEW to its base row; the arms above rely on the same.
		dv, ok := cr.displayValue(b, row)
		if !ok {
			return v
		}
		ms, ok := dv.(int64)
		if !ok {
			return v // NULL (nil), or a box this column's type does not make
		}
		return batch.FormatTimestamp(ms)

	default:
		return v
	}
}

// stringOperand reports v's text when it is a string or byte slice — the two
// shapes a text column or literal reaches Cast.Eval in.
func stringOperand(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	}
	return "", false
}

// --- String: regex and parsing ---

func fnSplitPart(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	parts := strings.Split(toString(args[0]), toString(args[1]))
	pos := int(ToFloat64(args[2]))
	// PostgreSQL 14 and later count a NEGATIVE position from the END:
	// SPLIT_PART('a,b,c', ',', -1) is `c`, -2 is `b`, and a magnitude past the
	// field count answers the empty string exactly as a too-large positive one
	// does. Zero names nothing and is 22023, not the empty string this used to
	// answer for every non-positive position alike (#855). All measured live
	// on 17.11.
	if pos == 0 {
		raiseFieldPositionZero()
	}
	idx := pos - 1 // SQL is 1-indexed
	if pos < 0 {
		idx = len(parts) + pos
	}
	if idx < 0 || idx >= len(parts) {
		return ""
	}
	return parts[idx]
}

func fnStrPos(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := toString(args[0])
	pos := strings.Index(s, toString(args[1]))
	if pos < 0 {
		return int32(0)
	}
	// The position is a CHARACTER position, as everywhere else in this family:
	// POSITION('à' IN 'éàü') is 2 on the server and was 3 here, the byte offset
	// (#856). The needle search itself stays bytewise — on valid UTF-8 a
	// substring match is the same match either way — and only the answer is
	// converted.
	return int32Count(utf8.RuneCountInString(s[:pos]) + 1) // 1-based
}

func fnRegexpLike(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	matched, err := regexp.MatchString(toString(args[1]), toString(args[0]))
	if err != nil {
		return nil
	}
	return matched
}

func fnRegexpExtract(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re := compileRegexpCached(toString(args[1]))
	if re == nil {
		return nil
	}
	group := 0
	if len(args) >= 3 && args[2] != nil {
		group = int(ToFloat64(args[2]))
	}
	matches := re.FindStringSubmatch(toString(args[0]))
	if matches == nil || group >= len(matches) {
		return nil
	}
	return matches[group]
}

// regexpCache caches compiled patterns process-wide. Scalar regexp
// functions previously called regexp.Compile PER ROW — ClickBench Q29
// (REGEXP_REPLACE over 100M Referers) spent its 117s recompiling one
// pattern 100M times. sync.Map: read-mostly, a handful of distinct
// patterns per workload.
var regexpCache sync.Map // pattern string → *regexp.Regexp (nil for invalid)

func compileRegexpCached(pattern string) *regexp.Regexp {
	if v, ok := regexpCache.Load(pattern); ok {
		re, _ := v.(*regexp.Regexp)
		return re
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		re = nil
	}
	regexpCache.Store(pattern, re)
	return re
}

func fnRegexpReplace(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	re := compileRegexpCached(toString(args[1]))
	if re == nil {
		return nil
	}
	return re.ReplaceAllString(toString(args[0]), sqlBackrefsToGo(toString(args[2])))
}

// sqlBackrefsToGo converts SQL-style backreferences (\1 … \9, the
// POSIX/DuckDB/Postgres convention) in a replacement string to Go's ${N}
// form. \\ stays a literal backslash escape for a following digit.
func sqlBackrefsToGo(repl string) string {
	if !strings.ContainsRune(repl, '\\') {
		return repl
	}
	var b strings.Builder
	b.Grow(len(repl) + 4)
	for i := 0; i < len(repl); i++ {
		c := repl[i]
		if c == '\\' && i+1 < len(repl) {
			next := repl[i+1]
			if next >= '1' && next <= '9' {
				b.WriteString("${")
				b.WriteByte(next)
				b.WriteString("}")
				i++
				continue
			}
			if next == '\\' {
				b.WriteByte('\\')
				i++
				continue
			}
		}
		// Go's Expand treats $ specially — escape literal dollars.
		if c == '$' {
			b.WriteString("$$")
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// --- Encoding functions ---

func fnToHex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return fmt.Sprintf("%x", int64(ToFloat64(args[0])))
}

func fnFromHex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	var n int64
	_, err := fmt.Sscanf(toString(args[0]), "%x", &n)
	if err != nil {
		return nil
	}
	return float64(n)
}

func fnToBase64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return base64.StdEncoding.EncodeToString([]byte(toString(args[0])))
}

func fnFromBase64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(toString(args[0]))
	if err != nil {
		return nil
	}
	return string(decoded)
}

// --- Date/time conversion functions ---

func fnFromUnixtime(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	epoch := int64(ToFloat64(args[0]))
	return formatInstant(time.Unix(epoch, 0))
}

func fnToUnixtime(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := parseTime(args[0])
	if t.IsZero() {
		return nil
	}
	return float64(t.Unix())
}

func fnDateFormat(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t := parseTime(args[0])
	if t.IsZero() {
		return nil
	}
	return t.Format(sqlFormatToGo(toString(args[1])))
}

func fnDateParse(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t, err := time.Parse(sqlFormatToGo(toString(args[1])), toString(args[0]))
	if err != nil {
		return nil
	}
	return formatInstant(t)
}

// sqlFormatToGo converts SQL date format specifiers to Go time layout.
func sqlFormatToGo(format string) string {
	r := strings.NewReplacer(
		"%Y", "2006",
		"%m", "01",
		"%d", "02",
		"%H", "15",
		"%i", "04",
		"%s", "05",
		"%S", "05",
		"%M", "January",
		"%b", "Jan",
		"%W", "Monday",
		"%a", "Mon",
		"%p", "PM",
		"%T", "15:04:05",
	)
	return r.Replace(format)
}

// --- Hash functions ---

func fnMD5(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	h := md5.Sum([]byte(toString(args[0])))
	return hex.EncodeToString(h[:])
}

func fnSHA256(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	h := sha256.Sum256([]byte(toString(args[0])))
	return hex.EncodeToString(h[:])
}

func fnSHA512(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	h := sha512.Sum512([]byte(toString(args[0])))
	return hex.EncodeToString(h[:])
}

// --- Bitwise functions ---

func fnBitwiseAnd(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return float64(int64(ToFloat64(args[0])) & int64(ToFloat64(args[1])))
}

func fnBitwiseOr(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return float64(int64(ToFloat64(args[0])) | int64(ToFloat64(args[1])))
}

func fnBitwiseXor(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return float64(int64(ToFloat64(args[0])) ^ int64(ToFloat64(args[1])))
}

func fnBitwiseNot(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return float64(^int64(ToFloat64(args[0])))
}

// --- String: padding and character ---

func fnLPad(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	// LPAD measures WIDTH in characters, and both the truncation and the fill
	// used to measure bytes: LPAD('éàü', 5, 'x') answered two characters and
	// half of a third — invalid UTF-8 — where PostgreSQL answers `xxéàü`
	// (#856).
	r := []rune(toString(args[0]))
	n := int(ToFloat64(args[1]))
	pad := []rune(" ")
	if len(args) >= 3 && args[2] != nil {
		pad = []rune(toString(args[2]))
	}
	if len(pad) == 0 || n <= len(r) {
		if n < 0 {
			return ""
		}
		if n <= len(r) {
			return string(r[:n])
		}
		return string(r)
	}
	fill := make([]rune, 0, n-len(r))
	for len(fill) < n-len(r) {
		fill = append(fill, pad...)
	}
	return string(fill[:n-len(r)]) + string(r)
}

func fnRPad(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	// CHARACTERS, like LPAD above (#856).
	r := []rune(toString(args[0]))
	n := int(ToFloat64(args[1]))
	pad := []rune(" ")
	if len(args) >= 3 && args[2] != nil {
		pad = []rune(toString(args[2]))
	}
	if len(pad) == 0 || n <= len(r) {
		if n < 0 {
			return ""
		}
		if n <= len(r) {
			return string(r[:n])
		}
		return string(r)
	}
	out := append([]rune(nil), r...)
	for len(out) < n {
		out = append(out, pad...)
	}
	return string(out[:n])
}

func fnChr(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	code := int64(ToFloat64(args[0]))
	// PostgreSQL's three refusals, each with its own SQLSTATE (#855). Zero is
	// the one that matters most: a NUL cannot travel in a text-format DataRow
	// and libpq truncates at one, so `CHR(0)` answered a one-character string
	// here and an empty one to psql — #570's shape reached through a function.
	switch {
	case code < 0:
		raiseChrNotPositive()
	case code == 0:
		raiseChrNul()
	case code > 0x10FFFF:
		raiseChrTooLarge(code)
	}
	return string(rune(code))
}

func fnCodepoint(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	if len(s) == 0 {
		return nil
	}
	runes := []rune(s)
	return int32(runes[0])
}

func fnConcatWS(args []any) any {
	if len(args) < 2 || args[0] == nil {
		return nil
	}
	sep := toString(args[0])
	parts := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		if a == nil {
			continue // skip nulls
		}
		parts = append(parts, toString(a))
	}
	return strings.Join(parts, sep)
}

func fnCharLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	// bytea has no characters, so this is its BYTE count — the same answer
	// `length` gives, which is what keeps the two synonyms from disagreeing
	// over one value. PostgreSQL has no `char_length(bytea)` at all (42883,
	// measured), so answering is the superset ADR-0012 records for the
	// text-only family; answering two different numbers for two spellings of
	// one function is not (#583 round 2, B4).
	if raw, ok := args[0].([]byte); ok {
		return int32Count(len(raw))
	}
	return int32Count(len([]rune(toString(args[0]))))
}

func fnTranslate(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	s := toString(args[0])
	from := []rune(toString(args[1]))
	to := []rune(toString(args[2]))
	mapping := make(map[rune]rune)
	for i, r := range from {
		if i < len(to) {
			mapping[r] = to[i]
		} else {
			mapping[r] = -1 // mark for deletion
		}
	}
	var sb strings.Builder
	for _, r := range s {
		if repl, ok := mapping[r]; ok {
			if repl >= 0 {
				sb.WriteRune(repl)
			}
		} else {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// --- Math: trigonometry ---

func fnPi(args []any) any {
	return math.Pi
}

func fnDegrees(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return ToFloat64(args[0]) * 180.0 / math.Pi
}

func fnRadians(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return ToFloat64(args[0]) * math.Pi / 180.0
}

func fnSin(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Sin(ToFloat64(args[0]))
}

func fnCos(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Cos(ToFloat64(args[0]))
}

func fnTan(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Tan(ToFloat64(args[0]))
}

func fnAsin(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	raiseTrigDomain(v)
	return math.Asin(v)
}

func fnAcos(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	raiseTrigDomain(v)
	return math.Acos(v)
}

func fnAtan(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Atan(ToFloat64(args[0]))
}

func fnAtan2(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return math.Atan2(ToFloat64(args[0]), ToFloat64(args[1]))
}

func fnCbrt(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Cbrt(ToFloat64(args[0]))
}

func fnLog2(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	// PostgreSQL has no LOG2; this is a wadjet extension, and it takes LOG's
	// refusal because one engine cannot have two answers to "what is the
	// logarithm of zero" depending on the base a caller spells.
	raiseLogarithmDomain(v)
	return math.Log2(v)
}

func fnTruncate(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	decimals := 0
	if len(args) >= 2 && args[1] != nil {
		decimals = int(ToFloat64(args[1]))
	}
	pow := math.Pow(10, float64(decimals))
	return math.Trunc(v*pow) / pow
}

func fnRandom(args []any) any {
	return rand.Float64()
}

// --- JSON functions ---

func fnJSONExtract(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	jsonStr := toString(args[0])
	path := toString(args[1])
	result := jsonPathExtract(jsonStr, path)
	if result == nil {
		return nil
	}
	// Return as JSON string for non-scalar values
	switch v := result.(type) {
	case string:
		return v
	case float64:
		return v
	case bool:
		return v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		return string(b)
	}
}

func fnJSONExtractScalar(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	result := jsonPathExtract(toString(args[0]), toString(args[1]))
	if result == nil {
		return nil
	}
	switch v := result.(type) {
	case string:
		return v
	case float64:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return nil // non-scalar
	}
}

func fnJSONArrayLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	var arr []any
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		return nil
	}
	return float64(len(arr))
}

func fnJSONValid(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return json.Valid([]byte(toString(args[0])))
}

// jsonPathExtract extracts a value from a JSON string using a simple dot-path.
// Supports: $.key, $.key.nested, $.key[0], $.key[0].nested
func jsonPathExtract(jsonStr, path string) any {
	var data any
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil
	}
	// Strip leading "$." or "$"
	if strings.HasPrefix(path, "$.") {
		path = path[2:]
	} else if strings.HasPrefix(path, "$") {
		path = path[1:]
	}
	if path == "" {
		return data
	}
	return navigateJSON(data, path)
}

func navigateJSON(data any, path string) any {
	current := data
	for path != "" {
		// Parse next segment
		var segment string
		dotIdx := strings.IndexAny(path, ".[")
		if dotIdx < 0 {
			segment = path
			path = ""
		} else if path[dotIdx] == '.' {
			segment = path[:dotIdx]
			path = path[dotIdx+1:]
		} else {
			// '[' found
			segment = path[:dotIdx]
			path = path[dotIdx:]
		}

		if segment != "" {
			obj, ok := current.(map[string]any)
			if !ok {
				return nil
			}
			current = obj[segment]
			if current == nil {
				return nil
			}
		}

		// Handle array index [N]
		if strings.HasPrefix(path, "[") {
			end := strings.Index(path, "]")
			if end < 0 {
				return nil
			}
			idxStr := path[1:end]
			path = path[end+1:]
			if strings.HasPrefix(path, ".") {
				path = path[1:]
			}
			var idx int
			if _, err := fmt.Sscanf(idxStr, "%d", &idx); err != nil {
				return nil
			}
			arr, ok := current.([]any)
			if !ok || idx < 0 || idx >= len(arr) {
				return nil
			}
			current = arr[idx]
		}
	}
	return current
}

// --- URL functions ---

func fnURLExtractHost(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.Hostname()
}

func fnURLExtractPort(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil || u.Port() == "" {
		return nil
	}
	var port int
	if _, err := fmt.Sscanf(u.Port(), "%d", &port); err != nil {
		return nil
	}
	return float64(port)
}

func fnURLExtractPath(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.Path
}

func fnURLExtractProtocol(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.Scheme
}

func fnURLExtractQuery(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.RawQuery
}

func fnURLExtractParameter(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.Query().Get(toString(args[1]))
}

// --- Type introspection ---

func fnTypeof(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return "null"
	}
	switch args[0].(type) {
	case int64:
		return "bigint"
	case int32:
		return "integer"
	case int:
		return "integer"
	case float64:
		return "double"
	case float32:
		return "real"
	case string:
		return "varchar"
	case bool:
		return "boolean"
	case []byte:
		return "varbinary"
	default:
		return fmt.Sprintf("%T", args[0])
	}
}

// --- String: distance and utility ---

func fnSoundex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := strings.ToUpper(toString(args[0]))
	if len(s) == 0 {
		return ""
	}
	result := make([]byte, 4)
	result[0] = s[0]
	codes := map[byte]byte{
		'B': '1', 'F': '1', 'P': '1', 'V': '1',
		'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
		'D': '3', 'T': '3',
		'L': '4',
		'M': '5', 'N': '5',
		'R': '6',
	}
	idx := 1
	lastCode := codes[s[0]]
	for i := 1; i < len(s) && idx < 4; i++ {
		code, ok := codes[s[i]]
		if ok && code != lastCode {
			result[idx] = code
			idx++
			lastCode = code
		} else if !ok {
			lastCode = 0
		}
	}
	for idx < 4 {
		result[idx] = '0'
		idx++
	}
	return string(result)
}

func fnLevenshtein(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := []rune(toString(args[0]))
	t := []rune(toString(args[1]))
	m, n := len(s), len(t)
	if m == 0 {
		return float64(n)
	}
	if n == 0 {
		return float64(m)
	}
	prev := make([]int, n+1)
	curr := make([]int, n+1)
	for j := 0; j <= n; j++ {
		prev[j] = j
	}
	for i := 1; i <= m; i++ {
		curr[0] = i
		for j := 1; j <= n; j++ {
			cost := 1
			if s[i-1] == t[j-1] {
				cost = 0
			}
			ins := curr[j-1] + 1
			del := prev[j] + 1
			sub := prev[j-1] + cost
			min := ins
			if del < min {
				min = del
			}
			if sub < min {
				min = sub
			}
			curr[j] = min
		}
		prev, curr = curr, prev
	}
	return float64(prev[n])
}

func fnHamming(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	a := toString(args[0])
	b := toString(args[1])
	if len(a) != len(b) {
		return nil
	}
	dist := 0
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			dist++
		}
	}
	return float64(dist)
}

func fnNormalize(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	// NFC normalization: collapse combining characters
	var sb strings.Builder
	for _, r := range s {
		if unicode.IsPrint(r) {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

func fnFormat(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	format := toString(args[0])
	fmtArgs := make([]any, 0, len(args)-1)
	for _, a := range args[1:] {
		fmtArgs = append(fmtArgs, a)
	}
	return fmt.Sprintf(format, fmtArgs...)
}

func fnToUTF8(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return []byte(toString(args[0]))
}

func fnFromUTF8(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	switch v := args[0].(type) {
	case []byte:
		if utf8.Valid(v) {
			return string(v)
		}
		return nil
	case string:
		return v
	default:
		return fmt.Sprint(args[0])
	}
}

// --- Math: IEEE 754 and utility ---

func fnE(args []any) any {
	return math.E
}

func fnLog10(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v <= 0 {
		return nil
	}
	return math.Log10(v)
}

func fnInfinity(args []any) any {
	return math.Inf(1)
}

func fnNaN(args []any) any {
	return math.NaN()
}

func fnIsNaN(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.IsNaN(ToFloat64(args[0]))
}

func fnIsFinite(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	return !math.IsInf(v, 0) && !math.IsNaN(v)
}

func fnIsInfinite(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.IsInf(ToFloat64(args[0]), 0)
}

func fnWidthBucket(args []any) any {
	if len(args) < 4 || args[0] == nil || args[1] == nil || args[2] == nil || args[3] == nil {
		return nil
	}
	value := ToFloat64(args[0])
	bound1 := ToFloat64(args[1])
	bound2 := ToFloat64(args[2])
	n := int(ToFloat64(args[3]))
	// PostgreSQL refuses both of these with 2201G rather than answering NULL:
	// a non-positive count names no buckets, and equal bounds leave the width
	// zero (#855). The two messages are distinct on the server and are kept
	// distinct here.
	if n <= 0 {
		raiseWidthBucketCount()
	}
	if bound1 == bound2 {
		raiseWidthBucketBounds()
	}
	if value < bound1 {
		return int32(0)
	}
	if value >= bound2 {
		return int32(n + 1)
	}
	width := (bound2 - bound1) / float64(n)
	bucket := int((value-bound1)/width) + 1
	if bucket > n {
		bucket = n + 1
	}
	return int32(bucket)
}

func fnFromBase(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := toString(args[0])
	base := int(ToFloat64(args[1]))
	if base < 2 || base > 36 {
		return nil
	}
	n, err := strconv.ParseInt(s, base, 64)
	if err != nil {
		return nil
	}
	return float64(n)
}

func fnToBase(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	n := int64(ToFloat64(args[0]))
	base := int(ToFloat64(args[1]))
	if base < 2 || base > 36 {
		return nil
	}
	return strconv.FormatInt(n, base)
}

func fnBitCount(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	n := int64(ToFloat64(args[0]))
	return float64(bits.OnesCount64(uint64(n)))
}

// --- Hash: additional ---

func fnSHA1(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	h := sha1.Sum([]byte(toString(args[0])))
	return hex.EncodeToString(h[:])
}

func fnCRC32(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return float64(crc32.ChecksumIEEE([]byte(toString(args[0]))))
}

func fnHMACSHA256(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	mac := hmac.New(sha256.New, []byte(toString(args[1])))
	mac.Write([]byte(toString(args[0])))
	return hex.EncodeToString(mac.Sum(nil))
}

func fnHMACSHA512(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	mac := hmac.New(sha512.New, []byte(toString(args[1])))
	mac.Write([]byte(toString(args[0])))
	return hex.EncodeToString(mac.Sum(nil))
}

// --- Date: additional accessors ---

func fnQuarter(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64((int(t.Month())-1)/3 + 1)
}

func fnWeek(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	_, week := t.ISOWeek()
	return float64(week)
}

func fnDayOfWeek(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Weekday())
}

func fnDayOfYear(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.YearDay())
}

func fnLastDayOfMonth(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	firstOfNext := time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
	last := firstOfNext.AddDate(0, 0, -1)
	return last.Format("2006-01-02")
}

func fnCurrentTimestamp(args []any) any {
	return formatInstant(clockNow())
}

func fnAtTimezone(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t := parseTime(args[0])
	if t.IsZero() {
		return nil
	}
	tz := toString(args[1])
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil
	}
	// NOT formatInstant. Every other timestamp-valued function renders through
	// it (#544), and this one must not: the result is a wall clock in `loc`,
	// and the engine's one rendering has no zone, so printing it bare would
	// publish 07:00 New York as 07:00 UTC — five hours wrong the moment
	// anything parses it back. Keeping the offset is the same position
	// fnTimezone takes when it declines a non-UTC zone rather than convert
	// with a guessed sign; both wait on a naive-timestamp type.
	return t.In(loc).Format(time.RFC3339)
}

// processStart is when this process began, captured once at package
// initialization.
var processStart = time.Now()

// ProcessStart returns the instant this process began — the value
// pg_postmaster_start_time() reports. Exported so a gate can assert that the
// wire carries THIS process's start EXACTLY, instead of bounding it against
// wall-clock time at assertion time. That bound is a statement about how long
// the rest of a test binary ran, not about the server: at 300 seconds it
// failed permanently once the -race suite crossed five minutes (#563), and
// with the bound removed it passes for a server reporting 1970 (#518).
func ProcessStart() time.Time { return processStart }

// fnPgPostmasterStartTime implements pg_postmaster_start_time(). DataGrip asks
// for it while opening a connection (`select round(extract(epoch from
// pg_postmaster_start_time() at time zone 'UTC')) as startup_time`) to label
// the session with the server's uptime.
//
// PostgreSQL reports when the postmaster — the process that owns the cluster —
// started. `wadjet serve` is that process, so process start is the honest
// answer. The value is a timestamp in the representation now() and
// current_timestamp use — formatInstant text, the engine's one instant
// rendering: the scalar registry is func([]any) any with no type channel, and
// every temporal function downstream (parseTime, epoch, timezone) reads that
// form. It carries the MILLISECOND now, where RFC3339 second-truncated it.
func fnPgPostmasterStartTime(args []any) any {
	return formatInstant(processStart)
}

// fnEpoch implements EXTRACT(EPOCH FROM ts), which the parser rewrites to
// epoch(ts): seconds since 1970-01-01T00:00:00Z. It is derived from the
// resolved instant, never from the column's raw stored number — a DATE column
// holds days and a TIMESTAMP column holds milliseconds, so passing the stored
// value through unchanged answered 9568 for a 1996 date (issue #319).
// resolveTemporalArgs converts those columns to a time.Time before this runs;
// text timestamps are parsed by parseTime as they always were.
func fnEpoch(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := parseTime(args[0])
	if t.IsZero() {
		return nil
	}
	return float64(t.Unix())
}

// fnTimezone implements PostgreSQL's timezone(zone, timestamp), the canonical
// form of the `<timestamp> AT TIME ZONE <zone>` operator that the parser
// rewrites to this call.
//
// PostgreSQL gives the operator two directions, chosen by the input type:
//
//	timestamptz AT TIME ZONE zone → timestamp   (absolute instant → wall clock in zone)
//	timestamp   AT TIME ZONE zone → timestamptz (wall clock in zone → absolute instant)
//
// Wadjet has one timestamp type and its values are absolute instants
// (vectors hold epoch seconds, the scalar layer passes RFC3339 text), so only
// the first direction has a meaning here. But PostgreSQL's result for that
// direction is a *naive* timestamp, which this type system cannot represent.
// Rendering the instant in the zone instead — keeping the offset, so the
// instant is preserved — disagrees with PostgreSQL for everything downstream
// that reads the naive result as UTC: PostgreSQL's EXTRACT(EPOCH FROM ts AT
// TIME ZONE 'America/New_York') is the zone's offset away from EXTRACT(EPOCH
// FROM ts), while an instant-preserving conversion leaves it equal.
//
// So the zone is restricted to UTC, the case where the two readings coincide:
// an instant and its UTC wall clock are the same count of seconds since the
// epoch, and the EXTRACT(EPOCH FROM …) round trip is exact. Every other zone
// is rejected rather than converted with a wrong sign — as a compile-time
// error when the zone is a literal (see compileFuncCallNode) and as NULL here
// when it is not. Widening this means giving the type system a naive-timestamp
// type first.
func fnTimezone(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	if !isUTCZone(toString(args[0])) {
		return nil
	}
	t := parseTime(args[1])
	if t.IsZero() {
		return nil
	}
	return formatInstant(t)
}

// isUTCZone reports whether a zone name is one of the spellings of UTC that
// AT TIME ZONE accepts. All of these are zero offset with no DST rule, so the
// conversion fnTimezone declines to guess at does not arise for them.
func isUTCZone(zone string) bool {
	switch strings.ToUpper(strings.TrimSpace(zone)) {
	case "UTC", "GMT", "Z", "ETC/UTC", "ETC/GMT":
		return true
	}
	return false
}

func fnHumanReadableSeconds(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	total := int64(ToFloat64(args[0]))
	if total < 0 {
		total = -total
	}
	days := total / 86400
	total %= 86400
	hours := total / 3600
	total %= 3600
	minutes := total / 60
	seconds := total % 60
	var parts []string
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%d day%s", days, plural(days)))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%d hour%s", hours, plural(hours)))
	}
	if minutes > 0 {
		parts = append(parts, fmt.Sprintf("%d minute%s", minutes, plural(minutes)))
	}
	if seconds > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%d second%s", seconds, plural(seconds)))
	}
	return strings.Join(parts, ", ")
}

func plural(n int64) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// --- Network: analytics ---

func fnIsPrivateIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsPrivate()
}

func fnIsLoopbackIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsLoopback()
}

func fnIPToInt(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return nil // only IPv4
	}
	return float64(uint32(ip4[0])<<24 | uint32(ip4[1])<<16 | uint32(ip4[2])<<8 | uint32(ip4[3]))
}

func fnIntToIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	n := uint32(ToFloat64(args[0]))
	return fmt.Sprintf("%d.%d.%d.%d", n>>24&0xFF, n>>16&0xFF, n>>8&0xFF, n&0xFF)
}

func fnIsIPv4(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return false
	}
	return ip.To4() != nil
}

func fnIsIPv6(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return false
	}
	return ip.To4() == nil
}

// ── CIDR / Subnet Operations ────────────────────────────────────────────────

func fnNetworkAddress(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	return network.IP.String()
}

func fnBroadcastAddress(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ip := network.IP.To4()
	if ip == nil {
		return nil
	}
	mask := network.Mask
	broadcast := make(net.IP, len(ip))
	for i := range ip {
		broadcast[i] = ip[i] | ^mask[i]
	}
	return broadcast.String()
}

func fnPrefixLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ones, _ := network.Mask.Size()
	return int64(ones)
}

func fnCIDRToRange(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ip := network.IP.To4()
	if ip == nil {
		return nil
	}
	mask := network.Mask
	first := network.IP.String()
	broadcast := make(net.IP, len(ip))
	for i := range ip {
		broadcast[i] = ip[i] | ^mask[i]
	}
	return first + "-" + broadcast.String()
}

func fnHostsInCIDR(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ones, bits := network.Mask.Size()
	hostBits := bits - ones
	if hostBits <= 0 {
		return int64(1)
	}
	if hostBits == 1 {
		return int64(2)
	}
	return int64(1<<uint(hostBits) - 2)
}

func fnCIDROverlap(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	_, net1, err1 := net.ParseCIDR(fmt.Sprint(args[0]))
	_, net2, err2 := net.ParseCIDR(fmt.Sprint(args[1]))
	if err1 != nil || err2 != nil {
		return nil
	}
	return net1.Contains(net2.IP) || net2.Contains(net1.IP)
}

func ipToUint32(ip net.IP) uint32 {
	ip4 := ip.To4()
	if ip4 == nil {
		return 0
	}
	return uint32(ip4[0])<<24 | uint32(ip4[1])<<16 | uint32(ip4[2])<<8 | uint32(ip4[3])
}

func uint32ToIP(n uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d", n>>24&0xFF, n>>16&0xFF, n>>8&0xFF, n&0xFF)
}

func fnIPInRange(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	lo := net.ParseIP(fmt.Sprint(args[1]))
	hi := net.ParseIP(fmt.Sprint(args[2]))
	if ip == nil || lo == nil || hi == nil {
		return nil
	}
	v := ipToUint32(ip)
	vLo := ipToUint32(lo)
	vHi := ipToUint32(hi)
	return v >= vLo && v <= vHi
}

func fnSameSubnet(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	ip1 := net.ParseIP(fmt.Sprint(args[0]))
	ip2 := net.ParseIP(fmt.Sprint(args[1]))
	prefixLen := int(ToInt64(args[2]))
	if ip1 == nil || ip2 == nil {
		return nil
	}
	ip1v4 := ip1.To4()
	ip2v4 := ip2.To4()
	if ip1v4 == nil || ip2v4 == nil {
		return nil
	}
	mask := net.CIDRMask(prefixLen, 32)
	for i := 0; i < 4; i++ {
		if ip1v4[i]&mask[i] != ip2v4[i]&mask[i] {
			return false
		}
	}
	return true
}

// ── IP Manipulation ─────────────────────────────────────────────────────────

func fnIPAdd(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	offset := ToInt64(args[1])
	v := uint32(int64(ipToUint32(ip)) + offset)
	return uint32ToIP(v)
}

func fnIPSubtract(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	offset := ToInt64(args[1])
	v := uint32(int64(ipToUint32(ip)) - offset)
	return uint32ToIP(v)
}

func fnIPDiff(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	ip1 := net.ParseIP(fmt.Sprint(args[0]))
	ip2 := net.ParseIP(fmt.Sprint(args[1]))
	if ip1 == nil || ip2 == nil {
		return nil
	}
	return int64(ipToUint32(ip1)) - int64(ipToUint32(ip2))
}

func fnIPBetween(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	lo := net.ParseIP(fmt.Sprint(args[1]))
	hi := net.ParseIP(fmt.Sprint(args[2]))
	if ip == nil || lo == nil || hi == nil {
		return nil
	}
	v := ipToUint32(ip)
	vLo := ipToUint32(lo)
	vHi := ipToUint32(hi)
	return v >= vLo && v <= vHi
}

func fnReverseDNS(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	if ip4 := ip.To4(); ip4 != nil {
		return fmt.Sprintf("%d.%d.%d.%d.in-addr.arpa", ip4[3], ip4[2], ip4[1], ip4[0])
	}
	// IPv6: expand to full 32 nibbles reversed
	ip16 := ip.To16()
	nibbles := make([]string, 32)
	for i := 0; i < 16; i++ {
		nibbles[31-2*i] = fmt.Sprintf("%x", ip16[i]>>4)
		nibbles[30-2*i] = fmt.Sprintf("%x", ip16[i]&0x0f)
	}
	return strings.Join(nibbles, ".") + ".ip6.arpa"
}

func fnIsMulticastIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsMulticast()
}

func fnIsLinkLocalIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsLinkLocalUnicast()
}

func fnIsReservedIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsMulticast() || ip.IsLinkLocalUnicast()
}

func fnIPToHex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	if ip4 := ip.To4(); ip4 != nil {
		return hex.EncodeToString(ip4)
	}
	return hex.EncodeToString(ip.To16())
}

// ── MAC Operations ──────────────────────────────────────────────────────────

func fnMACVendorOUI(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	mac, err := net.ParseMAC(fmt.Sprint(args[0]))
	if err != nil || len(mac) < 3 {
		return nil
	}
	return strings.ToUpper(fmt.Sprintf("%02x:%02x:%02x", mac[0], mac[1], mac[2]))
}

func fnMACIsUnicast(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	mac, err := net.ParseMAC(fmt.Sprint(args[0]))
	if err != nil || len(mac) < 1 {
		return nil
	}
	return mac[0]&0x01 == 0
}

func fnMACIsLocal(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	mac, err := net.ParseMAC(fmt.Sprint(args[0]))
	if err != nil || len(mac) < 1 {
		return nil
	}
	return mac[0]&0x02 != 0
}

func fnMACFormat(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	input := fmt.Sprint(args[0])
	sep := ":"
	if len(args) >= 2 && args[1] != nil {
		sep = fmt.Sprint(args[1])
	}
	// Strip any existing separators to get raw hex
	raw := strings.NewReplacer(":", "", "-", "", ".", "").Replace(input)
	if len(raw) != 12 {
		return nil
	}
	// Validate hex
	for _, c := range raw {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return nil
		}
	}
	raw = strings.ToLower(raw)
	parts := make([]string, 6)
	for i := 0; i < 6; i++ {
		parts[i] = raw[i*2 : i*2+2]
	}
	return strings.Join(parts, sep)
}

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

func fnPortName(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	name, ok := wellKnownPorts[port]
	if !ok {
		return nil
	}
	return name
}

func fnIsWellKnownPort(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	return port >= 0 && port <= 1023
}

func fnIsRegisteredPort(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	return port >= 1024 && port <= 49151
}

func fnIsEphemeralPort(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	return port >= 49152 && port <= 65535
}

func fnPortClass(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	switch {
	case port >= 0 && port <= 1023:
		return "well-known"
	case port >= 1024 && port <= 49151:
		return "registered"
	case port >= 49152 && port <= 65535:
		return "ephemeral"
	default:
		return nil
	}
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

func fnProtocolName(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	num := ToInt64(args[0])
	name, ok := protocolNumToName[num]
	if !ok {
		return nil
	}
	return name
}

func fnProtocolNumber(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	name := strings.ToLower(fmt.Sprint(args[0]))
	num, ok := protocolNameToNum[name]
	if !ok {
		return nil
	}
	return num
}

// --- Protocol Deep Inspection Functions ---

// TCP flag constants (bitmask positions in TCP flags byte)
const (
	tcpFIN = 0x01
	tcpSYN = 0x02
	tcpRST = 0x04
	tcpPSH = 0x08
	tcpACK = 0x10
	tcpURG = 0x20
	tcpECE = 0x40
	tcpCWR = 0x80
)

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

// fnTCPFlagsToString converts a TCP flags bitmask to comma-separated names.
// tcp_flags_to_string(0x12) → 'SYN,ACK'
func fnTCPFlagsToString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	flags := byte(ToInt64(args[0]))
	var parts []string
	for _, f := range tcpFlagNames {
		if flags&f.mask != 0 {
			parts = append(parts, f.name)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ",")
}

// fnHasTCPFlag tests if a TCP flags bitmask has a specific flag set.
// has_tcp_flag(0x12, 'SYN') → true
func fnHasTCPFlag(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	flags := byte(ToInt64(args[0]))
	name := strings.ToLower(toString(args[1]))
	mask, ok := tcpFlagLookup[name]
	if !ok {
		return nil
	}
	return flags&mask != 0
}

// fnTCPFlagsFromString converts flag names to bitmask.
// tcp_flags_from_string('SYN,ACK') → 0x12 (18)
func fnTCPFlagsFromString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	parts := strings.Split(toString(args[0]), ",")
	var result byte
	for _, p := range parts {
		mask, ok := tcpFlagLookup[strings.ToLower(strings.TrimSpace(p))]
		if ok {
			result |= mask
		}
	}
	return int64(result)
}

// fnIsTCPHandshake tests for SYN-only (connection initiation).
// is_tcp_handshake(flags) → true if SYN is set and ACK is not
func fnIsTCPHandshake(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	flags := byte(ToInt64(args[0]))
	return flags&tcpSYN != 0 && flags&tcpACK == 0
}

// fnIsTCPReset tests for RST flag.
func fnIsTCPReset(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	flags := byte(ToInt64(args[0]))
	return flags&tcpRST != 0
}

// fnTCPSessionID generates a canonical 5-tuple session key.
// tcp_session_id(src_ip, dst_ip, src_port, dst_port, protocol)
// Orders the IP/port pair so both directions map to the same key.
func fnTCPSessionID(args []any) any {
	if len(args) < 5 {
		return nil
	}
	for _, a := range args[:5] {
		if a == nil {
			return nil
		}
	}
	srcIP := toString(args[0])
	dstIP := toString(args[1])
	srcPort := ToInt64(args[2])
	dstPort := ToInt64(args[3])
	proto := ToInt64(args[4])

	// Canonical ordering: lower IP first, break ties by port
	if srcIP > dstIP || (srcIP == dstIP && srcPort > dstPort) {
		srcIP, dstIP = dstIP, srcIP
		srcPort, dstPort = dstPort, srcPort
	}
	return fmt.Sprintf("%s:%d-%s:%d/%d", srcIP, srcPort, dstIP, dstPort, proto)
}

// fnFlowDirection classifies a flow as 'inbound', 'outbound', or 'internal'.
// flow_direction(src_ip, dst_ip) — based on RFC 1918 private ranges
func fnFlowDirection(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	srcIP := net.ParseIP(toString(args[0]))
	dstIP := net.ParseIP(toString(args[1]))
	if srcIP == nil || dstIP == nil {
		return nil
	}
	srcPriv := srcIP.IsPrivate() || srcIP.IsLoopback()
	dstPriv := dstIP.IsPrivate() || dstIP.IsLoopback()
	switch {
	case srcPriv && dstPriv:
		return "internal"
	case srcPriv && !dstPriv:
		return "outbound"
	case !srcPriv && dstPriv:
		return "inbound"
	default:
		return "transit"
	}
}

// --- DNS parsing functions ---
// These work on raw DNS payload bytes (the UDP payload after the IP/UDP headers).

// fnDNSQueryName extracts the query name from a DNS query payload.
// dns_query_name(payload_hex) → 'www.example.com'
// DNS wire format: 2-byte ID, 2-byte flags, 2-byte QDCOUNT, ..., then QNAME
// QNAME is a sequence of length-prefixed labels ending with a 0-length label.
func fnDNSQueryName(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 13 { // minimum DNS header + 1-byte name
		return nil
	}
	// Skip 12-byte DNS header
	offset := 12
	return parseDNSName(data, offset)
}

// fnDNSQueryType extracts the query type (A=1, AAAA=28, CNAME=5, MX=15, etc.).
func fnDNSQueryType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 13 {
		return nil
	}
	// Skip header, skip QNAME
	offset := 12
	for offset < len(data) {
		length := int(data[offset])
		if length == 0 {
			offset++
			break
		}
		offset += 1 + length
	}
	if offset+2 > len(data) {
		return nil
	}
	qtype := binary.BigEndian.Uint16(data[offset : offset+2])
	return dnsTypeName(qtype)
}

// fnDNSIsResponse checks if DNS packet is a response (QR bit set).
func fnDNSIsResponse(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 4 {
		return nil
	}
	// QR is bit 15 of the flags field (byte 2, bit 7)
	return data[2]&0x80 != 0
}

// fnDNSResponseCode extracts the RCODE from DNS flags.
// 0=NOERROR, 1=FORMERR, 2=SERVFAIL, 3=NXDOMAIN, 5=REFUSED
func fnDNSResponseCode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 4 {
		return nil
	}
	rcode := data[3] & 0x0F
	switch rcode {
	case 0:
		return "NOERROR"
	case 1:
		return "FORMERR"
	case 2:
		return "SERVFAIL"
	case 3:
		return "NXDOMAIN"
	case 4:
		return "NOTIMP"
	case 5:
		return "REFUSED"
	default:
		return fmt.Sprintf("RCODE_%d", rcode)
	}
}

// fnDNSQuestionCount returns the number of questions in a DNS packet.
func fnDNSQuestionCount(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 6 {
		return nil
	}
	return int64(binary.BigEndian.Uint16(data[4:6]))
}

// fnDNSAnswerCount returns the number of answers in a DNS packet.
func fnDNSAnswerCount(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 8 {
		return nil
	}
	return int64(binary.BigEndian.Uint16(data[6:8]))
}

// fnDNSTransactionID extracts the 16-bit transaction ID.
func fnDNSTransactionID(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 2 {
		return nil
	}
	return int64(binary.BigEndian.Uint16(data[0:2]))
}

// --- TLS inspection functions ---

// fnTLSSNI extracts the Server Name Indication from a TLS ClientHello.
// tls_sni(payload_hex) → 'www.example.com'
func fnTLSSNI(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	return parseTLSSNI(data)
}

// fnTLSVersion extracts the TLS version from a TLS record header.
// Returns human-readable version: 'TLS 1.0', 'TLS 1.2', 'TLS 1.3', etc.
func fnTLSVersion(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 5 {
		return nil
	}
	// TLS record: type(1) + version(2) + length(2)
	major := data[1]
	minor := data[2]
	return tlsVersionString(major, minor)
}

// fnTLSRecordType identifies the TLS record content type.
// 20=ChangeCipherSpec, 21=Alert, 22=Handshake, 23=ApplicationData
func fnTLSRecordType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 1 {
		return nil
	}
	switch data[0] {
	case 20:
		return "ChangeCipherSpec"
	case 21:
		return "Alert"
	case 22:
		return "Handshake"
	case 23:
		return "ApplicationData"
	default:
		return fmt.Sprintf("Unknown(%d)", data[0])
	}
}

// fnIsTLSClientHello tests if payload starts with a TLS ClientHello.
func fnIsTLSClientHello(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	// TLS record: type 22 (Handshake), then version, then length, then handshake type 1 (ClientHello)
	if len(data) < 6 {
		return false
	}
	return data[0] == 22 && data[5] == 1
}

// fnTLSHandshakeType returns the handshake message type.
// 1=ClientHello, 2=ServerHello, 11=Certificate, 12=ServerKeyExchange, etc.
func fnTLSHandshakeType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 6 || data[0] != 22 {
		return nil
	}
	hsType := data[5]
	switch hsType {
	case 0:
		return "HelloRequest"
	case 1:
		return "ClientHello"
	case 2:
		return "ServerHello"
	case 4:
		return "NewSessionTicket"
	case 11:
		return "Certificate"
	case 12:
		return "ServerKeyExchange"
	case 13:
		return "CertificateRequest"
	case 14:
		return "ServerHelloDone"
	case 15:
		return "CertificateVerify"
	case 16:
		return "ClientKeyExchange"
	case 20:
		return "Finished"
	default:
		return fmt.Sprintf("Unknown(%d)", hsType)
	}
}

// --- HTTP parsing functions ---
// These work on raw HTTP request/response payloads (text protocol).

// fnHTTPMethod extracts the HTTP method from a request payload.
// http_method(payload) → 'GET', 'POST', etc.
func fnHTTPMethod(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	sp := strings.IndexByte(s, ' ')
	if sp < 0 || sp > 7 {
		return nil
	}
	method := s[:sp]
	switch method {
	case "GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH", "TRACE", "CONNECT":
		return method
	default:
		return nil
	}
}

// fnHTTPPath extracts the request path from an HTTP request.
// http_path('GET /api/v1/users HTTP/1.1\r\n...') → '/api/v1/users'
func fnHTTPPath(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	sp1 := strings.IndexByte(s, ' ')
	if sp1 < 0 {
		return nil
	}
	rest := s[sp1+1:]
	sp2 := strings.IndexByte(rest, ' ')
	if sp2 < 0 {
		// try newline
		sp2 = strings.IndexByte(rest, '\r')
		if sp2 < 0 {
			sp2 = strings.IndexByte(rest, '\n')
		}
	}
	if sp2 < 0 {
		return rest
	}
	return rest[:sp2]
}

// fnHTTPHost extracts the Host header from an HTTP request.
func fnHTTPHost(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return extractHTTPHeader(toString(args[0]), "host")
}

// fnHTTPStatusCode extracts the status code from an HTTP response.
// http_status_code('HTTP/1.1 200 OK\r\n...') → 200
func fnHTTPStatusCode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	if !strings.HasPrefix(s, "HTTP/") {
		return nil
	}
	sp1 := strings.IndexByte(s, ' ')
	if sp1 < 0 {
		return nil
	}
	rest := s[sp1+1:]
	sp2 := strings.IndexAny(rest, " \r\n")
	codeStr := rest
	if sp2 >= 0 {
		codeStr = rest[:sp2]
	}
	code, err := strconv.Atoi(codeStr)
	if err != nil {
		return nil
	}
	return int64(code)
}

// fnHTTPStatusClass classifies HTTP status: '1xx', '2xx', '3xx', '4xx', '5xx'.
func fnHTTPStatusClass(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	code := int(ToInt64(args[0]))
	switch {
	case code >= 100 && code < 200:
		return "1xx"
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 300 && code < 400:
		return "3xx"
	case code >= 400 && code < 500:
		return "4xx"
	case code >= 500 && code < 600:
		return "5xx"
	default:
		return nil
	}
}

// fnHTTPContentType extracts Content-Type header value.
func fnHTTPContentType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return extractHTTPHeader(toString(args[0]), "content-type")
}

// fnHTTPContentLength extracts Content-Length header as integer.
func fnHTTPContentLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	val := extractHTTPHeader(toString(args[0]), "content-length")
	if val == nil {
		return nil
	}
	n, err := strconv.ParseInt(val.(string), 10, 64)
	if err != nil {
		return nil
	}
	return n
}

// fnHTTPUserAgent extracts User-Agent header.
func fnHTTPUserAgent(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return extractHTTPHeader(toString(args[0]), "user-agent")
}

// fnHTTPHeader extracts any HTTP header by name.
// http_header(payload, 'X-Forwarded-For')
func fnHTTPHeader(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return extractHTTPHeader(toString(args[0]), strings.ToLower(toString(args[1])))
}

// fnHTTPVersion extracts the HTTP version from a request or response.
// http_version('GET / HTTP/1.1\r\n...') → 'HTTP/1.1'
func fnHTTPVersion(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	// Response: starts with HTTP/
	if strings.HasPrefix(s, "HTTP/") {
		sp := strings.IndexAny(s, " \r\n")
		if sp < 0 {
			return s
		}
		return s[:sp]
	}
	// Request: HTTP version is after the second space on the first line
	line := s
	if nl := strings.IndexByte(s, '\r'); nl >= 0 {
		line = s[:nl]
	} else if nl := strings.IndexByte(s, '\n'); nl >= 0 {
		line = s[:nl]
	}
	sp := strings.LastIndex(line, " HTTP/")
	if sp < 0 {
		return nil
	}
	return line[sp+1:]
}

// fnIsHTTPRequest tests if payload looks like an HTTP request.
func fnIsHTTPRequest(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	for _, m := range []string{"GET ", "POST ", "PUT ", "DELETE ", "HEAD ", "OPTIONS ", "PATCH ", "TRACE ", "CONNECT "} {
		if strings.HasPrefix(s, m) {
			return true
		}
	}
	return false
}

// fnIsHTTPResponse tests if payload looks like an HTTP response.
func fnIsHTTPResponse(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.HasPrefix(toString(args[0]), "HTTP/")
}

// --- Packet header parsing ---

// fnIPHeaderLength returns the IP header length in bytes from the raw IP header.
// ip_header_length(payload_hex) — first nibble of byte 0 × 4
func fnIPHeaderLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 1 {
		return nil
	}
	ihl := int64(data[0]&0x0F) * 4
	return ihl
}

// fnIPTTL extracts the TTL field from an IPv4 header.
func fnIPTTL(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 9 {
		return nil
	}
	return int64(data[8])
}

// fnIPTotalLength extracts the total length from an IPv4 header.
func fnIPTotalLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 4 {
		return nil
	}
	return int64(binary.BigEndian.Uint16(data[2:4]))
}

// fnIPDSCP extracts the DSCP value from the IPv4 TOS byte.
// DSCP is the top 6 bits of byte 1.
func fnIPDSCP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 2 {
		return nil
	}
	return int64(data[1] >> 2)
}

// fnEtherType identifies the EtherType from an Ethernet frame header.
// ether_type(frame_hex) — bytes 12-13 of the Ethernet header
func fnEtherType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 14 {
		return nil
	}
	et := binary.BigEndian.Uint16(data[12:14])
	switch et {
	case 0x0800:
		return "IPv4"
	case 0x0806:
		return "ARP"
	case 0x86DD:
		return "IPv6"
	case 0x8100:
		return "VLAN"
	case 0x8847:
		return "MPLS"
	case 0x88CC:
		return "LLDP"
	default:
		return fmt.Sprintf("0x%04X", et)
	}
}

// fnVLANID extracts the VLAN ID from an 802.1Q tagged frame.
// Expects raw Ethernet frame; VLAN tag starts at byte 14 if EtherType is 0x8100.
func fnVLANID(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 16 {
		return nil
	}
	et := binary.BigEndian.Uint16(data[12:14])
	if et != 0x8100 {
		return nil // not a VLAN-tagged frame
	}
	// VLAN ID is the lower 12 bits of bytes 14-15
	vlanID := binary.BigEndian.Uint16(data[14:16]) & 0x0FFF
	return int64(vlanID)
}

// fnPayloadEntropy estimates the Shannon entropy of a byte payload.
// Useful for detecting encrypted/compressed traffic vs plaintext.
// payload_entropy(data) → float64 (0.0 = uniform, ~8.0 = maximum entropy)
func fnPayloadEntropy(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) == 0 {
		return float64(0)
	}
	var freq [256]int
	for _, b := range data {
		freq[b]++
	}
	n := float64(len(data))
	entropy := 0.0
	for _, f := range freq {
		if f == 0 {
			continue
		}
		p := float64(f) / n
		entropy -= p * math.Log2(p)
	}
	return entropy
}

// fnPayloadHexDump returns the first N bytes as a hex dump string.
// payload_hex_dump(data, 16) → '48 65 6c 6c 6f 20 ...'
func fnPayloadHexDump(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	maxBytes := 32
	if len(args) >= 2 && args[1] != nil {
		maxBytes = int(ToInt64(args[1]))
	}
	if maxBytes > len(data) {
		maxBytes = len(data)
	}
	if maxBytes <= 0 {
		return ""
	}
	parts := make([]string, maxBytes)
	for i := 0; i < maxBytes; i++ {
		parts[i] = fmt.Sprintf("%02x", data[i])
	}
	return strings.Join(parts, " ")
}

// --- Deep inspection helpers ---

// toBytes converts a value to a byte slice, supporting hex strings and raw []byte.
func toBytes(v any) []byte {
	switch tv := v.(type) {
	case []byte:
		return tv
	case string:
		// Try hex decoding first
		if decoded, err := hex.DecodeString(tv); err == nil && len(tv)%2 == 0 && len(tv) > 0 {
			return decoded
		}
		// Fall back to raw bytes
		return []byte(tv)
	default:
		return []byte(fmt.Sprint(v))
	}
}

// parseDNSName parses a DNS wire-format name starting at the given offset.
func parseDNSName(data []byte, offset int) any {
	var parts []string
	for offset < len(data) {
		length := int(data[offset])
		if length == 0 {
			break
		}
		// Pointer (compression)
		if length&0xC0 == 0xC0 {
			if offset+1 >= len(data) {
				break
			}
			ptr := int(binary.BigEndian.Uint16(data[offset:offset+2])) & 0x3FFF
			rest := parseDNSName(data, ptr)
			if rest != nil {
				parts = append(parts, rest.(string))
			}
			return strings.Join(parts, ".")
		}
		offset++
		if offset+length > len(data) {
			break
		}
		parts = append(parts, string(data[offset:offset+length]))
		offset += length
	}
	if len(parts) == 0 {
		return nil
	}
	return strings.Join(parts, ".")
}

// dnsTypeName maps DNS query type numbers to names.
func dnsTypeName(qtype uint16) string {
	switch qtype {
	case 1:
		return "A"
	case 2:
		return "NS"
	case 5:
		return "CNAME"
	case 6:
		return "SOA"
	case 12:
		return "PTR"
	case 15:
		return "MX"
	case 16:
		return "TXT"
	case 28:
		return "AAAA"
	case 33:
		return "SRV"
	case 43:
		return "DS"
	case 46:
		return "RRSIG"
	case 48:
		return "DNSKEY"
	case 65:
		return "HTTPS"
	case 255:
		return "ANY"
	default:
		return fmt.Sprintf("TYPE%d", qtype)
	}
}

// parseTLSSNI extracts the SNI from a TLS ClientHello message.
func parseTLSSNI(data []byte) any {
	// TLS record: type(1) version(2) length(2) [record payload]
	if len(data) < 5 || data[0] != 22 { // not handshake
		return nil
	}
	// Handshake header: type(1) length(3)
	if len(data) < 9 || data[5] != 1 { // not ClientHello
		return nil
	}
	// ClientHello: version(2) random(32) session_id_len(1) ...
	offset := 9 // start of ClientHello body
	if offset+34 > len(data) {
		return nil
	}
	offset += 34 // skip version + random

	// Session ID
	if offset >= len(data) {
		return nil
	}
	sessIDLen := int(data[offset])
	offset += 1 + sessIDLen

	// Cipher suites
	if offset+2 > len(data) {
		return nil
	}
	csLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2 + csLen

	// Compression methods
	if offset >= len(data) {
		return nil
	}
	compLen := int(data[offset])
	offset += 1 + compLen

	// Extensions
	if offset+2 > len(data) {
		return nil
	}
	extLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	extEnd := offset + extLen
	if extEnd > len(data) {
		extEnd = len(data)
	}

	for offset+4 <= extEnd {
		extType := binary.BigEndian.Uint16(data[offset : offset+2])
		eLen := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
		offset += 4
		if extType == 0 { // SNI extension
			// SNI list: total_len(2) type(1) name_len(2) name(...)
			if offset+5 > extEnd {
				return nil
			}
			// skip list length (2 bytes)
			nameType := data[offset+2]
			nameLen := int(binary.BigEndian.Uint16(data[offset+3 : offset+5]))
			if nameType != 0 { // must be hostname type
				return nil
			}
			if offset+5+nameLen > extEnd {
				return nil
			}
			return string(data[offset+5 : offset+5+nameLen])
		}
		offset += eLen
	}
	return nil
}

// tlsVersionString converts TLS major.minor to human-readable string.
func tlsVersionString(major, minor byte) string {
	if major == 3 {
		switch minor {
		case 0:
			return "SSL 3.0"
		case 1:
			return "TLS 1.0"
		case 2:
			return "TLS 1.1"
		case 3:
			return "TLS 1.2"
		case 4:
			return "TLS 1.3"
		}
	}
	return fmt.Sprintf("TLS %d.%d", major, minor)
}

// extractHTTPHeader extracts an HTTP header value by name (case-insensitive).
func extractHTTPHeader(payload, headerName string) any {
	// Find end of first line
	lines := strings.Split(payload, "\r\n")
	if len(lines) < 2 {
		lines = strings.Split(payload, "\n")
	}
	for _, line := range lines[1:] {
		if line == "" {
			break // end of headers
		}
		colon := strings.IndexByte(line, ':')
		if colon < 0 {
			continue
		}
		name := strings.TrimSpace(line[:colon])
		if strings.EqualFold(name, headerName) {
			return strings.TrimSpace(line[colon+1:])
		}
	}
	return nil
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

func fnICMPTypeName(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := int(ToInt64(args[0]))
	name, ok := icmpTypeNames[t]
	if !ok {
		return fmt.Sprintf("Type %d", t)
	}
	return name
}

func fnICMPCodeName(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t := int(ToInt64(args[0]))
	c := int(ToInt64(args[1]))
	switch t {
	case 3:
		if name, ok := icmpUnreachableCodes[c]; ok {
			return name
		}
	case 5:
		if name, ok := icmpRedirectCodes[c]; ok {
			return name
		}
	case 11:
		if name, ok := icmpTimeExceededCodes[c]; ok {
			return name
		}
	}
	if c == 0 {
		if name, ok := icmpTypeNames[t]; ok {
			return name
		}
	}
	return fmt.Sprintf("Type %d Code %d", t, c)
}

func fnIsICMPEcho(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := int(ToInt64(args[0]))
	return t == 0 || t == 8
}

func fnICMPParse(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 2 {
		return nil
	}
	return fmt.Sprintf("%d:%d", data[0], data[1])
}

func fnICMPType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 1 {
		return nil
	}
	return int64(data[0])
}

func fnICMPCode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 2 {
		return nil
	}
	return int64(data[1])
}

// --- IPv6 Functions ---

func fnIPv6Scope(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	switch {
	case ip.IsLoopback():
		return "loopback"
	case ip.IsLinkLocalUnicast():
		return "link-local"
	case ip.IsLinkLocalMulticast():
		return "link-local-multicast"
	case ip.IsMulticast():
		return "multicast"
	case ip.IsPrivate():
		if ip.To4() != nil {
			return "private"
		}
		return "unique-local"
	case ip.IsGlobalUnicast():
		return "global"
	case ip.IsUnspecified():
		return "unspecified"
	default:
		return "unknown"
	}
}

func fnIPv6Expand(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return nil
	}
	return fmt.Sprintf("%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x",
		ip16[0], ip16[1], ip16[2], ip16[3],
		ip16[4], ip16[5], ip16[6], ip16[7],
		ip16[8], ip16[9], ip16[10], ip16[11],
		ip16[12], ip16[13], ip16[14], ip16[15])
}

func fnIPv6Compress(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	// The inverse of fnIPv6Expand, which already reads a dotted quad as the
	// v4-MAPPED address it is: compressing one gives `::ffff:a.b.c.d`, not the
	// bare quad net.IP.String() would hand back (#580).
	return batch.FormatIPv6(ip.To16())
}

func fnIPv6ToEUI64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	mac, err := net.ParseMAC(toString(args[0]))
	if err != nil || len(mac) != 6 {
		return nil
	}
	eui := make([]byte, 8)
	eui[0] = mac[0] ^ 0x02
	eui[1] = mac[1]
	eui[2] = mac[2]
	eui[3] = 0xFF
	eui[4] = 0xFE
	eui[5] = mac[3]
	eui[6] = mac[4]
	eui[7] = mac[5]
	return fmt.Sprintf("%02x%02x:%02x%02x:%02x%02x:%02x%02x",
		eui[0], eui[1], eui[2], eui[3], eui[4], eui[5], eui[6], eui[7])
}

func fnIs6to4(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return false
	}
	ip16 := ip.To16()
	return ip16 != nil && ip16[0] == 0x20 && ip16[1] == 0x02
}

func fnIsTeredo(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return false
	}
	ip16 := ip.To16()
	return ip16 != nil && ip16[0] == 0x20 && ip16[1] == 0x01 && ip16[2] == 0x00 && ip16[3] == 0x00
}

func fnTeredoServer(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	ip16 := ip.To16()
	if ip16 == nil || ip16[0] != 0x20 || ip16[1] != 0x01 || ip16[2] != 0x00 || ip16[3] != 0x00 {
		return nil
	}
	return fmt.Sprintf("%d.%d.%d.%d", ip16[4], ip16[5], ip16[6], ip16[7])
}

func fnTeredoClient(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	ip16 := ip.To16()
	if ip16 == nil || ip16[0] != 0x20 || ip16[1] != 0x01 || ip16[2] != 0x00 || ip16[3] != 0x00 {
		return nil
	}
	return fmt.Sprintf("%d.%d.%d.%d", ip16[12]^0xFF, ip16[13]^0xFF, ip16[14]^0xFF, ip16[15]^0xFF)
}

func fnSixto4Gateway(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	ip16 := ip.To16()
	if ip16 == nil || ip16[0] != 0x20 || ip16[1] != 0x02 {
		return nil
	}
	return fmt.Sprintf("%d.%d.%d.%d", ip16[2], ip16[3], ip16[4], ip16[5])
}

// --- JA3 TLS Fingerprinting ---

func fnJA3Fingerprint(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ja3str := buildJA3String(toBytes(args[0]))
	if ja3str == "" {
		return nil
	}
	hash := md5.Sum([]byte(ja3str))
	return hex.EncodeToString(hash[:])
}

func fnJA3String(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	result := buildJA3String(toBytes(args[0]))
	if result == "" {
		return nil
	}
	return result
}

func fnJA3SFingerprint(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ja3s := buildJA3SString(toBytes(args[0]))
	if ja3s == "" {
		return nil
	}
	hash := md5.Sum([]byte(ja3s))
	return hex.EncodeToString(hash[:])
}

func fnJA3SString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	result := buildJA3SString(toBytes(args[0]))
	if result == "" {
		return nil
	}
	return result
}

func buildJA3String(data []byte) string {
	if len(data) < 5 || data[0] != 22 {
		return ""
	}
	if len(data) < 9 || data[5] != 1 {
		return ""
	}
	offset := 9
	if offset+2 > len(data) {
		return ""
	}
	tlsVersion := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 34 // version + random
	if offset >= len(data) {
		return ""
	}
	sessIDLen := int(data[offset])
	offset += 1 + sessIDLen
	if offset+2 > len(data) {
		return ""
	}
	csLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if offset+csLen > len(data) {
		return ""
	}
	var ciphers []string
	for i := 0; i < csLen; i += 2 {
		cs := int(binary.BigEndian.Uint16(data[offset+i : offset+i+2]))
		if !isGREASE(uint16(cs)) {
			ciphers = append(ciphers, strconv.Itoa(cs))
		}
	}
	offset += csLen
	if offset >= len(data) {
		return ""
	}
	compLen := int(data[offset])
	offset += 1 + compLen

	var extensions, ellipticCurves, ecPointFormats []string
	if offset+2 <= len(data) {
		extTotalLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		extEnd := offset + extTotalLen
		if extEnd > len(data) {
			extEnd = len(data)
		}
		for offset+4 <= extEnd {
			extType := binary.BigEndian.Uint16(data[offset : offset+2])
			extLen := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
			offset += 4
			if !isGREASE(extType) {
				extensions = append(extensions, strconv.Itoa(int(extType)))
			}
			if offset+extLen > extEnd {
				break
			}
			if extType == 10 && extLen >= 2 {
				listLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
				for j := 2; j+1 < 2+listLen && offset+j+1 < extEnd; j += 2 {
					curve := binary.BigEndian.Uint16(data[offset+j : offset+j+2])
					if !isGREASE(curve) {
						ellipticCurves = append(ellipticCurves, strconv.Itoa(int(curve)))
					}
				}
			}
			if extType == 11 && extLen >= 1 {
				fmtLen := int(data[offset])
				for j := 1; j < 1+fmtLen && offset+j < extEnd; j++ {
					ecPointFormats = append(ecPointFormats, strconv.Itoa(int(data[offset+j])))
				}
			}
			offset += extLen
		}
	}
	return fmt.Sprintf("%d,%s,%s,%s,%s",
		tlsVersion,
		strings.Join(ciphers, "-"),
		strings.Join(extensions, "-"),
		strings.Join(ellipticCurves, "-"),
		strings.Join(ecPointFormats, "-"))
}

func buildJA3SString(data []byte) string {
	if len(data) < 5 || data[0] != 22 {
		return ""
	}
	if len(data) < 9 || data[5] != 2 {
		return ""
	}
	offset := 9
	if offset+2 > len(data) {
		return ""
	}
	tlsVersion := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 34
	if offset >= len(data) {
		return ""
	}
	sessIDLen := int(data[offset])
	offset += 1 + sessIDLen
	if offset+2 > len(data) {
		return ""
	}
	cipher := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 3 // cipher + compression

	var extensions []string
	if offset+2 <= len(data) {
		extTotalLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		extEnd := offset + extTotalLen
		if extEnd > len(data) {
			extEnd = len(data)
		}
		for offset+4 <= extEnd {
			extType := binary.BigEndian.Uint16(data[offset : offset+2])
			extLen := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
			offset += 4
			if !isGREASE(extType) {
				extensions = append(extensions, strconv.Itoa(int(extType)))
			}
			offset += extLen
		}
	}
	return fmt.Sprintf("%d,%d,%s", tlsVersion, cipher, strings.Join(extensions, "-"))
}

func isGREASE(v uint16) bool {
	return v&0x0f0f == 0x0a0a
}

// --- Payload Search Functions ---

func fnPayloadContains(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	data := toBytes(args[0])
	pattern := toBytes(args[1])
	if len(pattern) == 0 {
		return true
	}
	for i := 0; i <= len(data)-len(pattern); i++ {
		found := true
		for j := 0; j < len(pattern); j++ {
			if data[i+j] != pattern[j] {
				found = false
				break
			}
		}
		if found {
			return true
		}
	}
	return false
}

func fnPayloadMatches(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	matched, err := regexp.MatchString(toString(args[1]), toString(args[0]))
	if err != nil {
		return nil
	}
	return matched
}

func fnPayloadOffset(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	data := toBytes(args[0])
	off := int(ToInt64(args[1]))
	length := int(ToInt64(args[2]))
	if off < 0 || length <= 0 || off+length > len(data) {
		return nil
	}
	return hex.EncodeToString(data[off : off+length])
}

func fnPayloadLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return int64(len(toBytes(args[0])))
}

// ── Regex: Additional ───────────────────────────────────────────────────────

func fnRegexpCount(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re := compileRegexpCached(fmt.Sprint(args[1]))
	if re == nil {
		return nil
	}
	matches := re.FindAllString(fmt.Sprint(args[0]), -1)
	return int64(len(matches))
}

func fnRegexpExtractAll(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re := compileRegexpCached(fmt.Sprint(args[1]))
	if re == nil {
		return nil
	}
	matches := re.FindAllString(fmt.Sprint(args[0]), -1)
	if matches == nil {
		return "[]"
	}
	// Return as JSON array string (no native array type yet)
	parts := make([]string, len(matches))
	for i, m := range matches {
		escaped, _ := json.Marshal(m)
		parts[i] = string(escaped)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func fnRegexpSplit(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re := compileRegexpCached(fmt.Sprint(args[1]))
	if re == nil {
		return nil
	}
	parts := re.Split(fmt.Sprint(args[0]), -1)
	jsonParts := make([]string, len(parts))
	for i, p := range parts {
		escaped, _ := json.Marshal(p)
		jsonParts[i] = string(escaped)
	}
	return "[" + strings.Join(jsonParts, ",") + "]"
}

// ── String: Additional ──────────────────────────────────────────────────────

func fnSplit(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	parts := strings.Split(fmt.Sprint(args[0]), fmt.Sprint(args[1]))
	jsonParts := make([]string, len(parts))
	for i, p := range parts {
		escaped, _ := json.Marshal(p)
		jsonParts[i] = string(escaped)
	}
	return "[" + strings.Join(jsonParts, ",") + "]"
}

// ── Bitwise: Shifts ─────────────────────────────────────────────────────────

func fnBitwiseLeftShift(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	v := int64(ToFloat64(args[0]))
	shift := int64(ToFloat64(args[1]))
	if shift < 0 || shift >= 64 {
		return nil
	}
	return int64(v << uint(shift))
}

func fnBitwiseRightShift(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	v := int64(ToFloat64(args[0]))
	shift := int64(ToFloat64(args[1]))
	if shift < 0 || shift >= 64 {
		return nil
	}
	// Logical right shift (unsigned)
	return int64(uint64(v) >> uint(shift))
}

func fnBitwiseArithmeticShiftRight(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	v := int64(ToFloat64(args[0]))
	shift := int64(ToFloat64(args[1]))
	if shift < 0 || shift >= 64 {
		return nil
	}
	// Arithmetic right shift (preserves sign bit)
	return int64(v >> uint(shift))
}

// ── UUID: Generation ────────────────────────────────────────────────────────

func fnUUID(args []any) any {
	// Generate a random UUID v4
	var buf [16]byte
	for i := range buf {
		buf[i] = byte(rand.Intn(256))
	}
	buf[6] = (buf[6] & 0x0f) | 0x40 // version 4
	buf[8] = (buf[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

// ── Encoding: Additional ────────────────────────────────────────────────────

var base32Encoding = base32.StdEncoding.WithPadding(base32.NoPadding)

func fnToBase32(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return base32Encoding.EncodeToString([]byte(fmt.Sprint(args[0])))
}

func fnFromBase32(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data, err := base32Encoding.DecodeString(fmt.Sprint(args[0]))
	if err != nil {
		// Try with padding
		data, err = base32.StdEncoding.DecodeString(fmt.Sprint(args[0]))
		if err != nil {
			return nil
		}
	}
	return string(data)
}

func fnXXHash64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	// XXHash64 implementation using FNV-like approach
	// Using a simple but correct XXHash64 implementation
	data := []byte(fmt.Sprint(args[0]))
	h := xxhash64Sum(data)
	return fmt.Sprintf("%016x", h)
}

// xxhash64Sum computes XXHash64 of data with seed 0.
func xxhash64Sum(data []byte) uint64 {
	const (
		prime1 uint64 = 0x9E3779B185EBCA87
		prime2 uint64 = 0x14DEF9DEA2F79CD6
		prime3 uint64 = 0x165667B19E3779F9
		prime4 uint64 = 0x85EBCA77C2B2AE63
		prime5 uint64 = 0x27D4EB2F165667C5
	)

	n := len(data)
	var h uint64

	if n >= 32 {
		v1 := prime1 + prime2
		v2 := prime2
		v3 := uint64(0)
		var v4 uint64
		v4 -= prime1

		for len(data) >= 32 {
			v1 = xxh64Round(v1, binary.LittleEndian.Uint64(data[0:8]))
			v2 = xxh64Round(v2, binary.LittleEndian.Uint64(data[8:16]))
			v3 = xxh64Round(v3, binary.LittleEndian.Uint64(data[16:24]))
			v4 = xxh64Round(v4, binary.LittleEndian.Uint64(data[24:32]))
			data = data[32:]
		}

		h = bits.RotateLeft64(v1, 1) + bits.RotateLeft64(v2, 7) +
			bits.RotateLeft64(v3, 12) + bits.RotateLeft64(v4, 18)
		h = xxh64MergeRound(h, v1)
		h = xxh64MergeRound(h, v2)
		h = xxh64MergeRound(h, v3)
		h = xxh64MergeRound(h, v4)
	} else {
		h = prime5
	}

	h += uint64(n)

	for len(data) >= 8 {
		k := binary.LittleEndian.Uint64(data[0:8])
		k *= prime2
		k = bits.RotateLeft64(k, 31)
		k *= prime1
		h ^= k
		h = bits.RotateLeft64(h, 27)*prime1 + prime4
		data = data[8:]
	}

	for len(data) >= 4 {
		h ^= uint64(binary.LittleEndian.Uint32(data[0:4])) * prime1
		h = bits.RotateLeft64(h, 23)*prime2 + prime3
		data = data[4:]
	}

	for len(data) > 0 {
		h ^= uint64(data[0]) * prime5
		h = bits.RotateLeft64(h, 11) * prime1
		data = data[1:]
	}

	h ^= h >> 33
	h *= prime2
	h ^= h >> 29
	h *= prime3
	h ^= h >> 32

	return h
}

func xxh64Round(acc, input uint64) uint64 {
	const prime1 uint64 = 0x9E3779B185EBCA87
	const prime2 uint64 = 0x14DEF9DEA2F79CD6
	acc += input * prime2
	acc = bits.RotateLeft64(acc, 31)
	acc *= prime1
	return acc
}

func xxh64MergeRound(acc, val uint64) uint64 {
	const prime1 uint64 = 0x9E3779B185EBCA87
	const prime4 uint64 = 0x85EBCA77C2B2AE63
	val = xxh64Round(0, val)
	acc ^= val
	acc = acc*prime1 + prime4
	return acc
}

func fnMurmur3(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := []byte(fmt.Sprint(args[0]))
	h := murmur3Hash128(data)
	return fmt.Sprintf("%016x%016x", h[0], h[1])
}

// murmur3Hash128 computes MurmurHash3 x64_128 with seed 0.
func murmur3Hash128(data []byte) [2]uint64 {
	const (
		c1 uint64 = 0x87c37b91114253d5
		c2 uint64 = 0x4cf5ad432745937f
	)

	var h1, h2 uint64
	nblocks := len(data) / 16

	for i := 0; i < nblocks; i++ {
		k1 := binary.LittleEndian.Uint64(data[i*16:])
		k2 := binary.LittleEndian.Uint64(data[i*16+8:])

		k1 *= c1
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= c2
		h1 ^= k1
		h1 = bits.RotateLeft64(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2
		k2 = bits.RotateLeft64(k2, 33)
		k2 *= c1
		h2 ^= k2
		h2 = bits.RotateLeft64(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}

	tail := data[nblocks*16:]
	var k1, k2 uint64
	switch len(tail) {
	case 15:
		k2 ^= uint64(tail[14]) << 48
		fallthrough
	case 14:
		k2 ^= uint64(tail[13]) << 40
		fallthrough
	case 13:
		k2 ^= uint64(tail[12]) << 32
		fallthrough
	case 12:
		k2 ^= uint64(tail[11]) << 24
		fallthrough
	case 11:
		k2 ^= uint64(tail[10]) << 16
		fallthrough
	case 10:
		k2 ^= uint64(tail[9]) << 8
		fallthrough
	case 9:
		k2 ^= uint64(tail[8])
		k2 *= c2
		k2 = bits.RotateLeft64(k2, 33)
		k2 *= c1
		h2 ^= k2
		fallthrough
	case 8:
		k1 ^= uint64(tail[7]) << 56
		fallthrough
	case 7:
		k1 ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		k1 ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		k1 ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		k1 ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		k1 ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint64(tail[0])
		k1 *= c1
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= uint64(len(data))
	h2 ^= uint64(len(data))

	h1 += h2
	h2 += h1

	// fmix64
	fmix := func(h uint64) uint64 {
		h ^= h >> 33
		h *= 0xff51afd7ed558ccd
		h ^= h >> 33
		h *= 0xc4ceb9fe1a85ec53
		h ^= h >> 33
		return h
	}

	h1 = fmix(h1)
	h2 = fmix(h2)

	h1 += h2
	h2 += h1

	return [2]uint64{h1, h2}
}

// ── Date/Time: ISO 8601 ─────────────────────────────────────────────────────

func fnFromISO8601Timestamp(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	// Try common ISO 8601 formats
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMilli()
		}
	}
	return nil
}

func fnFromISO8601Date(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return nil
	}
	return t.Format("2006-01-02")
}

func fnToISO8601(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ms := int64(ToFloat64(args[0]))
	t := time.UnixMilli(ms).UTC()
	return t.Format(time.RFC3339)
}

func fnToMilliseconds(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	// Try parsing as ISO 8601 first
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMilli()
		}
	}
	// If already a number, return as-is
	if v, ok := args[0].(int64); ok {
		return v
	}
	return nil
}

func fnTimezoneHour(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ms := int64(ToFloat64(args[0]))
	t := time.UnixMilli(ms)
	_, offset := t.Zone()
	return int64(offset / 3600)
}

func fnTimezoneMinute(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ms := int64(ToFloat64(args[0]))
	t := time.UnixMilli(ms)
	_, offset := t.Zone()
	return int64((offset % 3600) / 60)
}

// ── Formatting ──────────────────────────────────────────────────────────────

func fnFormatNumber(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])

	// Default: format with commas, no decimal places
	decimals := 0
	if len(args) >= 2 && args[1] != nil {
		decimals = int(ToFloat64(args[1]))
	}

	// Format the number
	negative := v < 0
	if negative {
		v = -v
	}

	// Format with specified decimal places
	s := strconv.FormatFloat(v, 'f', decimals, 64)

	// Split integer and decimal parts
	parts := strings.SplitN(s, ".", 2)
	intPart := parts[0]

	// Add comma separators to integer part
	var result strings.Builder
	if negative {
		result.WriteByte('-')
	}
	for i, c := range intPart {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			result.WriteByte(',')
		}
		result.WriteRune(c)
	}
	if len(parts) == 2 {
		result.WriteByte('.')
		result.WriteString(parts[1])
	}

	return result.String()
}

// ── GeoIP / ASN Functions ───────────────────────────────────────────────────

// geoipParseIP extracts a net.IP from the first argument.
func geoipParseIP(args []any) net.IP {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return net.ParseIP(fmt.Sprint(args[0]))
}

func fnGeoipCountry(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || rec.Country.ISOCode == "" {
		return nil
	}
	return rec.Country.ISOCode
}

func fnGeoipCountryName(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil {
		return nil
	}
	name := rec.Country.Names["en"]
	if name == "" {
		return nil
	}
	return name
}

func fnGeoipCity(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil {
		return nil
	}
	name := rec.City.Names["en"]
	if name == "" {
		return nil
	}
	return name
}

func fnGeoipSubdivision(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || len(rec.Subdivisions) == 0 {
		return nil
	}
	name := rec.Subdivisions[0].Names["en"]
	if name == "" {
		// Fall back to ISO code
		if rec.Subdivisions[0].ISOCode != "" {
			return rec.Subdivisions[0].ISOCode
		}
		return nil
	}
	return name
}

func fnGeoipPostalCode(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || rec.Postal.Code == "" {
		return nil
	}
	return rec.Postal.Code
}

func fnGeoipLatitude(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil {
		return nil
	}
	if rec.Location.Latitude == 0 && rec.Location.Longitude == 0 {
		return nil
	}
	return rec.Location.Latitude
}

func fnGeoipLongitude(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil {
		return nil
	}
	if rec.Location.Latitude == 0 && rec.Location.Longitude == 0 {
		return nil
	}
	return rec.Location.Longitude
}

func fnGeoipTimezone(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || rec.Location.TimeZone == "" {
		return nil
	}
	return rec.Location.TimeZone
}

func fnGeoipContinent(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || rec.Continent.Code == "" {
		return nil
	}
	return rec.Continent.Code
}

func fnGeoipASN(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupASN(ip)
	if rec == nil || rec.Number == 0 {
		return nil
	}
	return int64(rec.Number)
}

func fnGeoipOrg(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupASN(ip)
	if rec == nil || rec.Organization == "" {
		return nil
	}
	return rec.Organization
}

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

// fnFormatBytes formats a byte count into human-readable form.
// format_bytes(bytes)           → '1.50 GiB'  (IEC binary, default)
// format_bytes(bytes, 'si')     → '1.61 GB'   (SI decimal)
// format_bytes(bytes, 'iec')    → '1.50 GiB'  (IEC binary)
func fnFormatBytes(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v < 0 {
		return "-" + fnFormatBytes([]any{-v, safeArg(args, 1)}).(string)
	}

	units := byteUnits
	if len(args) >= 2 && args[1] != nil && strings.ToLower(toString(args[1])) == "si" {
		units = byteUnitsSI
	}

	for _, u := range units {
		if v >= u.threshold {
			val := v / u.threshold
			if val >= 100 {
				return fmt.Sprintf("%.0f %s", val, u.suffix)
			} else if val >= 10 {
				return fmt.Sprintf("%.1f %s", val, u.suffix)
			}
			return fmt.Sprintf("%.2f %s", val, u.suffix)
		}
	}
	return fmt.Sprintf("%.0f B", v)
}

// fnParseBytes parses a human-readable byte string back to numeric bytes.
// parse_bytes('1.5 GiB') → 1610612736
// parse_bytes('500 MB')   → 500000000
func fnParseBytes(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := strings.TrimSpace(toString(args[0]))
	s = strings.ToUpper(s)

	multipliers := map[string]float64{
		"B":  1,
		"KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12, "PB": 1e15, "EB": 1e18,
		"KIB": 1024, "MIB": 1048576, "GIB": 1073741824,
		"TIB": 1099511627776, "PIB": 1125899906842624, "EIB": 1152921504606846976,
		// Also handle Kbps-style (bits)
		"BPS": 0.125, "KBPS": 125, "MBPS": 125000, "GBPS": 125000000,
		"KIBPS": 128, "MIBPS": 131072, "GIBPS": 134217728,
	}

	// Try each suffix from longest to shortest
	for _, suffix := range []string{
		"GIBPS", "MIBPS", "KIBPS", "GBPS", "MBPS", "KBPS", "BPS",
		"EIB", "PIB", "TIB", "GIB", "MIB", "KIB",
		"EB", "PB", "TB", "GB", "MB", "KB", "B",
	} {
		if strings.HasSuffix(s, suffix) {
			numStr := strings.TrimSpace(s[:len(s)-len(suffix)])
			if numStr == "" {
				return nil
			}
			num := ToFloat64(numStr)
			return int64(num * multipliers[suffix])
		}
	}
	// No unit — assume raw bytes
	return int64(ToFloat64(s))
}

// fnFormatRate formats a byte-per-second rate into human-readable form.
// format_rate(bytes_per_sec)            → '1.50 Gbps'  (bits/sec, default)
// format_rate(bytes_per_sec, 'bytes')   → '192.00 MiB/s'  (bytes/sec)
// format_rate(bytes_per_sec, 'si')      → '1.50 Gbps'  (SI bits/sec)
func fnFormatRate(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	bytesPerSec := ToFloat64(args[0])

	mode := "bits"
	if len(args) >= 2 && args[1] != nil {
		mode = strings.ToLower(toString(args[1]))
	}

	if mode == "bytes" || mode == "byte" {
		// Format as bytes/sec using IEC units
		formatted := fnFormatBytes([]any{bytesPerSec})
		if formatted == nil {
			return nil
		}
		return formatted.(string) + "/s"
	}

	// Format as bits/sec (SI)
	bitsPerSec := bytesPerSec * 8

	type rateUnit struct {
		threshold float64
		suffix    string
	}
	units := []rateUnit{
		{1e12, "Tbps"},
		{1e9, "Gbps"},
		{1e6, "Mbps"},
		{1e3, "Kbps"},
	}

	for _, u := range units {
		if bitsPerSec >= u.threshold {
			val := bitsPerSec / u.threshold
			if val >= 100 {
				return fmt.Sprintf("%.0f %s", val, u.suffix)
			} else if val >= 10 {
				return fmt.Sprintf("%.1f %s", val, u.suffix)
			}
			return fmt.Sprintf("%.2f %s", val, u.suffix)
		}
	}
	return fmt.Sprintf("%.0f bps", bitsPerSec)
}

// fnParseRate parses a human-readable rate string back to bytes per second.
// parse_rate('1.5 Gbps')   → 187500000  (bytes/sec)
// parse_rate('100 Mbps')   → 12500000   (bytes/sec)
// parse_rate('10 MiB/s')   → 10485760   (bytes/sec)
func fnParseRate(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := strings.TrimSpace(toString(args[0]))
	upper := strings.ToUpper(s)

	// Bits-per-second units → convert to bytes/sec
	bitRates := []struct {
		suffix     string
		bitsPerSec float64
	}{
		{"TBPS", 1e12},
		{"GBPS", 1e9},
		{"MBPS", 1e6},
		{"KBPS", 1e3},
		{"BPS", 1},
	}

	for _, r := range bitRates {
		if strings.HasSuffix(upper, r.suffix) {
			numStr := strings.TrimSpace(upper[:len(upper)-len(r.suffix)])
			if numStr == "" {
				return nil
			}
			bps := ToFloat64(numStr) * r.bitsPerSec
			return int64(bps / 8) // bits to bytes
		}
	}

	// Bytes-per-second units (MiB/s, GB/s, etc.)
	byteRates := []struct {
		suffix      string
		bytesPerSec float64
	}{
		{"TIB/S", 1099511627776},
		{"GIB/S", 1073741824},
		{"MIB/S", 1048576},
		{"KIB/S", 1024},
		{"TB/S", 1e12},
		{"GB/S", 1e9},
		{"MB/S", 1e6},
		{"KB/S", 1e3},
		{"B/S", 1},
	}

	for _, r := range byteRates {
		if strings.HasSuffix(upper, r.suffix) {
			numStr := strings.TrimSpace(upper[:len(upper)-len(r.suffix)])
			if numStr == "" {
				return nil
			}
			return int64(ToFloat64(numStr) * r.bytesPerSec)
		}
	}

	// No unit — assume bytes/sec
	return int64(ToFloat64(s))
}

func safeArg(args []any, i int) any {
	if i < len(args) {
		return args[i]
	}
	return nil
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
