package expr

import "strings"

// THE WIDTH POSTGRESQL DECLARES FOR A FUNCTION WHOSE RESULT IS AN INTEGER —
// which is NOT the width this engine carries that result in (#966 round 3).
//
// `Ret` says which VECTOR a function's results are stored in, and for an
// integer that is almost always `RetInt64`: every integer in this engine
// computes in an int64 (ADR-0024's widening), so a function whose Go body
// returns `int64` declares RetInt64 whether its values are transaction ids or
// TTLs. That declaration is a CARRIER, and reading it as PostgreSQL's result
// type is wrong in both directions:
//
//	regexp_count('abab','a')     PostgreSQL: integer    here: RetInt64
//	masklen('10.0.0.0/24'::cidr) PostgreSQL: integer    prefix_length: RetInt64
//	octet_length('abc')          PostgreSQL: integer    payload_length: RetInt64
//	bit_count('\x0102'::bytea)   PostgreSQL: bigint     bit_count: RetInt64
//
// There is exactly one question where the difference is the whole answer, and
// it is the same one exec.IntegerAccOutputType exists for: an ACCUMULATING
// aggregate's result type. `sum(int4)` is bigint and `sum(int8)` is numeric,
// measured on PostgreSQL 17.11 for every row of the census in
// `physical.aggInputIsWideInteger`'s comment. Reading the carrier made
// `SUM(regexp_count(…))` numeric where PostgreSQL declares bigint — twelve
// wire cells, grouped and windowed, text and binary (#966 round 3 review B1).
//
// So this file is the table, and `physical.aggInputIsWideInteger` is its only
// reader today: ONE table, and both the grouped and the windowed spelling ask
// it through the walk they already share.
//
// HOW AN ENTRY IS DECIDED, in this order:
//
//  1. PostgreSQL has the function (or this engine's function is a rename of
//     one it has): PostgreSQL's measured `pg_typeof` decides, full stop.
//     ADR-0012.
//  2. PostgreSQL does not have it: the width is the one that HOLDS THE
//     FUNCTION'S WHOLE DOMAIN — which is the criterion PostgreSQL itself
//     applied to its own. `masklen` is integer because a prefix length is
//     0..128; `bit_count` is bigint because a bytea's bit count is not
//     bounded by int4. An IP TTL is a byte and a content length is not.
//
// A function that returns an integer and is NOT in this table is a compile-
// clean way to get the old defect back, so
// `TestEveryIntegerDeclaredFunctionNamesItsPostgresResultWidth` asserts the
// table and the registry name the same set, in both directions.
type PGIntWidth uint8

const (
	// PGIntWidthNone is the zero value and never appears in the table.
	PGIntWidthNone PGIntWidth = iota
	// PGIntWidth4 — PostgreSQL declares int4, so SUM over it is bigint.
	PGIntWidth4
	// PGIntWidth8 — PostgreSQL declares int8, so SUM over it is numeric.
	PGIntWidth8
	// PGIntWidthOperands — the result is as wide as its widest integer
	// ARGUMENT, exactly as arithmetic is. The bitwise family and nothing
	// else: `int4 & int4` is integer in PostgreSQL and `int8 & int8` is
	// bigint, so `sum(f4 & 18)` is bigint and `sum(f8 & 18)` is numeric,
	// both measured. The caller recurses into the arguments this names,
	// which is what it already does for COALESCE and GREATEST.
	PGIntWidthOperands
)

// PGIntegerResult is one row of the table.
type PGIntegerResult struct {
	Width PGIntWidth
	// WidthArgs lists the argument positions whose width the result follows,
	// for PGIntWidthOperands only. Nil means every argument.
	//
	// The three SHIFTS need it: PostgreSQL's `<<` and `>>` take their count as
	// a separate int4 and the result is the SHIFTED value's type, so
	// `f4 << 2` is integer however the count is spelled.
	WidthArgs []int
}

// pgIntegerResultWidths is the table. Every function whose declared return
// type is a fixed integer is here, and nothing else is.
//
// Grouped by how the entry was decided, so a reader can see which rows are
// PostgreSQL's own and which are this engine's domain reading.
var pgIntegerResultWidths = map[string]PGIntegerResult{
	// ── PostgreSQL has the function; its measured type decides (17.11) ────
	// pg_typeof(length('abab'))                 integer
	// pg_typeof(octet_length('abab'))           integer
	// pg_typeof(bit_length('abab'))             integer
	// pg_typeof(char_length('abab'))            integer
	// pg_typeof(strpos('abab','b'))             integer
	// pg_typeof(position('b' in 'abab'))        integer
	// pg_typeof(array_length(ARRAY[1,2],1))     integer
	// pg_typeof(cardinality(ARRAY[1,2]))        integer
	// pg_typeof(width_bucket(5.0,0.0,10.0,4))   integer
	// pg_typeof(ascii('a'))                     integer
	// pg_typeof(regexp_count('abab','a'))       integer
	// pg_typeof(masklen('10.0.0.0/24'::cidr))   integer
	// pg_typeof(pg_backend_pid())               integer
	// pg_typeof(bit_count('\x0102'::bytea))     bigint
	// pg_typeof(txid_current())                 bigint
	// pg_typeof(pg_current_xact_id())           xid8   (64-bit)
	// pg_typeof(pg_my_temp_schema())            oid    (sum(oid) does not exist)
	"length":       {Width: PGIntWidth4},
	"len":          {Width: PGIntWidth4},
	"octet_length": {Width: PGIntWidth4},
	"bit_length":   {Width: PGIntWidth4},
	// payload_length IS octet_length under another name: it counts the bytes
	// of a payload. PostgreSQL says integer for that.
	"payload_length":   {Width: PGIntWidth4},
	"char_length":      {Width: PGIntWidth4},
	"character_length": {Width: PGIntWidth4},
	"strpos":           {Width: PGIntWidth4},
	"position":         {Width: PGIntWidth4},
	"array_length":     {Width: PGIntWidth4},
	"cardinality":      {Width: PGIntWidth4},
	"width_bucket":     {Width: PGIntWidth4},
	// codepoint is ascii(): a code point is 0..0x10FFFF.
	"codepoint":    {Width: PGIntWidth4},
	"regexp_count": {Width: PGIntWidth4},
	// prefix_length IS masklen: a prefix length is 0..128.
	"prefix_length":  {Width: PGIntWidth4},
	"pg_backend_pid": {Width: PGIntWidth4},
	// pg_my_temp_schema answers an oid in PostgreSQL and 0 here, and
	// PostgreSQL has no sum(oid) at all, so neither width is its answer; int4
	// is the value's own.
	"pg_my_temp_schema": {Width: PGIntWidth4},
	// vector_dims is pgvector's, and it declares integer.
	"vector_dims": {Width: PGIntWidth4},
	// bit_count is PostgreSQL's own, and it declares BIGINT even though a
	// 64-bit word's population count is 0..64. PostgreSQL decides.
	"bit_count":          {Width: PGIntWidth8},
	"txid_current":       {Width: PGIntWidth8},
	"pg_current_xact_id": {Width: PGIntWidth8},

	// ── the bitwise family: as wide as its operands, like arithmetic ──────
	// pg_typeof(f4 & 18) integer   pg_typeof(f8 & 18) bigint
	// pg_typeof(f4 | 18) integer   pg_typeof(f8 | 18) bigint
	// pg_typeof(f4 # 18) integer   pg_typeof(f8 # 18) bigint
	// pg_typeof(~f4)     integer   pg_typeof(~f8)     bigint
	// pg_typeof(f4 << 2) integer   pg_typeof(f8 << 2) bigint
	// pg_typeof(f4 >> 2) integer   pg_typeof(f8 >> 2) bigint
	"bitwise_and": {Width: PGIntWidthOperands},
	"bitwise_or":  {Width: PGIntWidthOperands},
	"bitwise_xor": {Width: PGIntWidthOperands},
	"bitwise_not": {Width: PGIntWidthOperands},
	// A shift's width is the SHIFTED value's, not the count's.
	"bitwise_left_shift":             {Width: PGIntWidthOperands, WidthArgs: []int{0}},
	"bitwise_right_shift":            {Width: PGIntWidthOperands, WidthArgs: []int{0}},
	"bitwise_arithmetic_shift_right": {Width: PGIntWidthOperands, WidthArgs: []int{0}},

	// ── PostgreSQL has no equivalent: the width that holds the domain ─────
	// A protocol header field is a byte or two, and PostgreSQL types such a
	// column integer; nothing here can exceed int4, so their SUM is bigint —
	// which is also what this walk answered before it read any declaration.
	"dns_question_count":    {Width: PGIntWidth4}, // 16-bit QDCOUNT
	"dns_answer_count":      {Width: PGIntWidth4}, // 16-bit ANCOUNT
	"dns_transaction_id":    {Width: PGIntWidth4}, // 16-bit ID
	"http_status_code":      {Width: PGIntWidth4}, // 100..599
	"icmp_type":             {Width: PGIntWidth4}, // 8-bit
	"icmp_code":             {Width: PGIntWidth4}, // 8-bit
	"ip_dscp":               {Width: PGIntWidth4}, // 6-bit
	"ip_header_length":      {Width: PGIntWidth4}, // 4-bit word count
	"ip_total_length":       {Width: PGIntWidth4}, // 16-bit
	"ip_ttl":                {Width: PGIntWidth4}, // 8-bit
	"vlan_id":               {Width: PGIntWidth4}, // 12-bit
	"protocol_number":       {Width: PGIntWidth4}, // 8-bit
	"tcp_flag_mask":         {Width: PGIntWidth4}, // 9-bit mask
	"tcp_flags_from_string": {Width: PGIntWidth4}, // 9-bit mask
	// A UTC offset in hours (-14..+14) and its minute part (0..59).
	// PostgreSQL's own `extract(timezone_hour from …)` is NUMERIC, so its
	// sum is numeric there and neither width is PostgreSQL's answer; the
	// value's own domain is int4's.
	"timezone_hour":   {Width: PGIntWidth4},
	"timezone_minute": {Width: PGIntWidth4},

	// These reach past int4, so summing them into an int64 can refuse where
	// the true total is representable — which is the whole reason
	// PostgreSQL's sum(int8) is numeric.
	"cast_int":               {Width: PGIntWidth8}, // the int64 domain, unranged
	"from_hex":               {Width: PGIntWidth8}, // a 64-bit word
	"from_base":              {Width: PGIntWidth8}, // a 64-bit word
	"from_iso8601_timestamp": {Width: PGIntWidth8}, // epoch milliseconds
	"to_milliseconds":        {Width: PGIntWidth8}, // epoch milliseconds
	"geoip_asn":              {Width: PGIntWidth8}, // a 32-bit UNSIGNED AS number
	"hosts_in_cidr":          {Width: PGIntWidth8}, // 2^32-2 for an IPv4 /0
	"http_content_length":    {Width: PGIntWidth8}, // a body is not bounded by int4
	"ip_diff":                {Width: PGIntWidth8}, // ±(2^32-1)
	"parse_bytes":            {Width: PGIntWidth8}, // '4TB' is 4398046511104
	"parse_rate":             {Width: PGIntWidth8}, // '10Gbps' is 10000000000
}

// PGIntegerResultWidth answers the table for a function name, and ok=false for
// a name whose result is not an integer at all (the caller then keeps whatever
// reading it had).
func PGIntegerResultWidth(name string) (PGIntegerResult, bool) {
	r, ok := pgIntegerResultWidths[strings.ToLower(strings.TrimSpace(name))]
	return r, ok
}

// pgIntegerResultWidthNames is the table's key set, for the gate that holds it
// against the registry.
func pgIntegerResultWidthNames() []string {
	out := make([]string, 0, len(pgIntegerResultWidths))
	for k := range pgIntegerResultWidths {
		out = append(out, k)
	}
	return out
}
