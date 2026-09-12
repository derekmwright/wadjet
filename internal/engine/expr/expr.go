// Package expr provides a typed expression engine for evaluating SQL expressions
// against record batches. It replaces the string-based expression parsing with
// a compiled expression tree built from the SQL parser AST.
package expr

import (
	"encoding/base32"
	"sync"
	"time"

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

		// TCP flag bitset semantics (#966, tcp_flags.go). The three
		// predicates are PostgreSQL's `(flags & mask) = mask` / `<> 0` /
		// `= 0` under the names RFC 9293 gives the bits; tcp_flag_mask
		// hands back the integer so the arithmetic can be written by hand
		// against the same table. The renderers answer TEXT and an ARRAY of
		// TEXT — no new type.
		"tcp_flags_has_all":  {fnTCPFlagsHasAll, RetBool},
		"tcp_flags_has_any":  {fnTCPFlagsHasAny, RetBool},
		"tcp_flags_has_none": {fnTCPFlagsHasNone, RetBool},
		"tcp_flag_mask":      {fnTCPFlagMask, RetInt32},
		"tcp_flags":          {fnTCPFlags, RetArray},
		"tcp_flags_text":     {fnTCPFlagsText, RetString},

		// Semantic Versioning 2.0.0 over STRING (#967, semver.go). No new
		// type: a version is text, and `semver_sort_key` is the text whose
		// BYTE ORDER is the specification's precedence, so ORDER BY, MIN/MAX,
		// the DAG's merge and an external sort run all order versions
		// correctly with no comparator of their own. The three components
		// declare INT64 because the specification puts no upper bound on a
		// numeric identifier; `semver_cmp` declares INT32 because its domain
		// is exactly {-1,0,1}. Both widths are the ones
		// pgIntegerResultWidths records for them.
		"semver_valid":            {fnSemverValid, RetBool},
		"semver_major":            {fnSemverMajor, RetInt64},
		"semver_minor":            {fnSemverMinor, RetInt64},
		"semver_patch":            {fnSemverPatch, RetInt64},
		"semver_prerelease":       {fnSemverPrerelease, RetString},
		"semver_build":            {fnSemverBuild, RetString},
		"semver_cmp":              {fnSemverCmp, RetInt32},
		"semver_sort_key":         {fnSemverSortKey, RetString},
		"semver_normalize":        {fnSemverNormalize, RetString},
		"semver_normalize_strict": {fnSemverNormalizeStrict, RetString},
		"semver_parse":            {fnSemverParse, RetRow(semverRowFields())},
		"semver_parse_strict":     {fnSemverParseStrict, RetRow(semverRowFields())},
		"semver_satisfies":        {fnSemverSatisfies, RetBool},

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
		"from_hex":    {fnFromHex, RetInt64},
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
		// The bitwise operators answer an INTEGER, as PostgreSQL's do, and
		// read their arguments as 64-bit patterns rather than through a
		// double (bitwise_exact.go). The three shifts below already declared
		// RetInt64; these four did not, and a BIGINT lost its low bits on the
		// way in and its high ones on the way out.
		"bitwise_and": {fnBitwiseAnd, RetInt64},
		"bitwise_or":  {fnBitwiseOr, RetInt64},
		"bitwise_xor": {fnBitwiseXor, RetInt64},
		"bitwise_not": {fnBitwiseNot, RetInt64},

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
		// from_base and bit_count answer INTEGERS. Declaring them FLOAT64
		// coerced an exact int64 through a double on the way out, so
		// FROM_BASE('4000000000000012', 16) came back 4.611686018427388e+18
		// (#966 round 2). PostgreSQL's own bit_count answers bigint.
		"from_base": {fnFromBase, RetInt64},
		"to_base":   {fnToBase, RetString},
		"bit_count": {fnBitCount, RetInt64},

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
var regexpCache sync.Map // pattern string → *regexp.Regexp (nil for invalid)

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

// The name-to-bit table the TCP flag functions read lives in tcp_flags.go
// (#966). A SECOND table stood here — eight bits wide, in a `byte` — and the
// two would have agreed only by inspection.

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
