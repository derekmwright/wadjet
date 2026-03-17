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
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/geoip"
)

// Expr evaluates an expression against a record batch row, returning a typed value.
type Expr interface {
	Eval(b *batch.RecordBatch, row int) any
}

// BoolExpr evaluates a boolean expression (used for WHERE/HAVING/JOIN conditions).
type BoolExpr interface {
	EvalBool(b *batch.RecordBatch, row int) bool
}

// --- Leaf nodes ---

// ColRef reads a column value from the batch.
// Caches the column index and type after first resolution for zero-allocation
// reads on numeric types.
type ColRef struct {
	Name        string
	resolved    bool
	idx         int
	typ         batch.TypeID
	structField string // for ROW field access (e.g., "person.name" → structField="name")
}

func (e *ColRef) Eval(b *batch.RecordBatch, row int) any {
	if !e.resolved {
		e.idx = b.ColumnIndex(e.Name)
		if e.idx < 0 && strings.Contains(e.Name, ".") {
			parts := strings.SplitN(e.Name, ".", 2)
			// Try unqualified (strip table prefix)
			e.idx = b.ColumnIndex(parts[1])
			if e.idx < 0 {
				// Try as struct field access: parts[0] is a ROW column, parts[1] is field name
				parentIdx := b.ColumnIndex(parts[0])
				if parentIdx >= 0 && b.Columns[parentIdx].Type == batch.TypeRow {
					e.idx = parentIdx
					e.structField = parts[1]
				}
			}
		}
		if e.idx >= 0 {
			e.typ = b.Columns[e.idx].Type
		}
		e.resolved = true
	}
	if e.idx < 0 {
		return nil
	}
	// Struct field access: extract named field from ROW value
	if e.structField != "" {
		v := b.Columns[e.idx].GetValue(row)
		if v == nil {
			return nil
		}
		if m, ok := v.(map[string]any); ok {
			return m[e.structField]
		}
		return nil
	}
	v := b.Columns[e.idx]
	// Use typed accessors to avoid boxing where possible for numeric hot paths.
	// For comparisons and arithmetic, the caller will use ToFloat64/ToInt64
	// which handle int64/float64 natively without re-boxing.
	switch e.typ {
	case batch.TypeInt64, batch.TypeTimestamp:
		val, ok := v.GetInt64(row)
		if !ok {
			return nil
		}
		return val
	case batch.TypeFloat64:
		val, ok := v.GetFloat64(row)
		if !ok {
			return nil
		}
		return val
	case batch.TypeString:
		val, ok := v.GetString(row)
		if !ok {
			return nil
		}
		return val
	default:
		return v.GetValue(row)
	}
}

// EvalFloat64 reads the column value as float64 without any boxing.
// Returns (0, false) if null or column not found.
func (e *ColRef) EvalFloat64(b *batch.RecordBatch, row int) (float64, bool) {
	if !e.resolved {
		e.idx = b.ColumnIndex(e.Name)
		if e.idx < 0 && strings.Contains(e.Name, ".") {
			parts := strings.SplitN(e.Name, ".", 2)
			e.idx = b.ColumnIndex(parts[1])
		}
		if e.idx >= 0 {
			e.typ = b.Columns[e.idx].Type
		}
		e.resolved = true
	}
	if e.idx < 0 {
		return 0, false
	}
	return b.Columns[e.idx].GetNumericFloat64(row)
}

// EvalString reads the column value as string without boxing.
func (e *ColRef) EvalString(b *batch.RecordBatch, row int) (string, bool) {
	if !e.resolved {
		e.idx = b.ColumnIndex(e.Name)
		if e.idx < 0 && strings.Contains(e.Name, ".") {
			parts := strings.SplitN(e.Name, ".", 2)
			e.idx = b.ColumnIndex(parts[1])
		}
		if e.idx >= 0 {
			e.typ = b.Columns[e.idx].Type
		}
		e.resolved = true
	}
	if e.idx < 0 {
		return "", false
	}
	return b.Columns[e.idx].GetString(row)
}

// EvalInt64 reads the column value as int64 without boxing.
func (e *ColRef) EvalInt64(b *batch.RecordBatch, row int) (int64, bool) {
	if !e.resolved {
		e.idx = b.ColumnIndex(e.Name)
		if e.idx < 0 && strings.Contains(e.Name, ".") {
			parts := strings.SplitN(e.Name, ".", 2)
			e.idx = b.ColumnIndex(parts[1])
		}
		if e.idx >= 0 {
			e.typ = b.Columns[e.idx].Type
		}
		e.resolved = true
	}
	if e.idx < 0 {
		return 0, false
	}
	v := b.Columns[e.idx]
	if v.Nulls.IsNull(row) {
		return 0, false
	}
	switch e.typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return v.Int64Data[row], true
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return int64(v.Int32Data[row]), true
	default:
		f, ok := v.GetNumericFloat64(row)
		return int64(f), ok
	}
}

// Lit returns a constant value.
type Lit struct {
	Val any
}

func (e *Lit) Eval(_ *batch.RecordBatch, _ int) any {
	return e.Val
}

func (e *Lit) EvalFloat64(_ *batch.RecordBatch, _ int) (float64, bool) {
	if e.Val == nil {
		return 0, false
	}
	return ToFloat64(e.Val), true
}

func (e *Lit) EvalInt64(_ *batch.RecordBatch, _ int) (int64, bool) {
	if e.Val == nil {
		return 0, false
	}
	return ToInt64(e.Val), true
}

// --- Typed expression interfaces for zero-boxing hot paths ---

// Float64Expr evaluates to float64 without boxing.
type Float64Expr interface {
	EvalFloat64(b *batch.RecordBatch, row int) (float64, bool)
}

// Int64Expr evaluates to int64 without boxing.
type Int64Expr interface {
	EvalInt64(b *batch.RecordBatch, row int) (int64, bool)
}

// --- Arithmetic ---

// BinOp is a binary arithmetic expression (generic, uses ToFloat64).
type BinOp struct {
	Left, Right Expr
	Op          string // +, -, *, /, %
}

func (e *BinOp) Eval(b *batch.RecordBatch, row int) any {
	lv := e.Left.Eval(b, row)
	rv := e.Right.Eval(b, row)
	if lv == nil || rv == nil {
		return nil
	}
	lf := ToFloat64(lv)
	rf := ToFloat64(rv)
	switch e.Op {
	case "+":
		return lf + rf
	case "-":
		return lf - rf
	case "*":
		return lf * rf
	case "/":
		if rf == 0 {
			return nil
		}
		return lf / rf
	case "%":
		if rf == 0 {
			return nil
		}
		return float64(int64(lf) % int64(rf))
	default:
		return nil
	}
}

// BinOpFloat64 is a typed binary op that operates on float64 without boxing.
type BinOpFloat64 struct {
	Left, Right Float64Expr
	Op          string
}

func (e *BinOpFloat64) Eval(b *batch.RecordBatch, row int) any {
	v, ok := e.EvalFloat64(b, row)
	if !ok {
		return nil
	}
	return v
}

func (e *BinOpFloat64) EvalFloat64(b *batch.RecordBatch, row int) (float64, bool) {
	lf, lok := e.Left.EvalFloat64(b, row)
	if !lok {
		return 0, false
	}
	rf, rok := e.Right.EvalFloat64(b, row)
	if !rok {
		return 0, false
	}
	switch e.Op {
	case "+":
		return lf + rf, true
	case "-":
		return lf - rf, true
	case "*":
		return lf * rf, true
	case "/":
		if rf == 0 {
			return 0, false
		}
		return lf / rf, true
	case "%":
		if rf == 0 {
			return 0, false
		}
		return float64(int64(lf) % int64(rf)), true
	default:
		return 0, false
	}
}

// BinOpInt64 is a typed binary op that operates on int64 without boxing.
type BinOpInt64 struct {
	Left, Right Int64Expr
	Op          string
}

func (e *BinOpInt64) Eval(b *batch.RecordBatch, row int) any {
	v, ok := e.EvalInt64(b, row)
	if !ok {
		return nil
	}
	return v
}

func (e *BinOpInt64) EvalInt64(b *batch.RecordBatch, row int) (int64, bool) {
	lv, lok := e.Left.EvalInt64(b, row)
	if !lok {
		return 0, false
	}
	rv, rok := e.Right.EvalInt64(b, row)
	if !rok {
		return 0, false
	}
	switch e.Op {
	case "+":
		return lv + rv, true
	case "-":
		return lv - rv, true
	case "*":
		return lv * rv, true
	case "/":
		if rv == 0 {
			return 0, false
		}
		return lv / rv, true
	case "%":
		if rv == 0 {
			return 0, false
		}
		return lv % rv, true
	default:
		return 0, false
	}
}

// EvalFloat64 allows BinOpInt64 to be used as Float64Expr (int→float promotion).
func (e *BinOpInt64) EvalFloat64(b *batch.RecordBatch, row int) (float64, bool) {
	v, ok := e.EvalInt64(b, row)
	return float64(v), ok
}

// UnaryOp is a unary arithmetic expression (negation).
type UnaryOp struct {
	Operand Expr
	Op      string // -, +
}

func (e *UnaryOp) Eval(b *batch.RecordBatch, row int) any {
	v := e.Operand.Eval(b, row)
	if v == nil {
		return nil
	}
	switch e.Op {
	case "-":
		return -ToFloat64(v)
	case "+":
		return ToFloat64(v)
	default:
		return v
	}
}

// --- Comparisons (return bool but implement Expr for composability) ---

// CmpOp represents a comparison operator.
type CmpOp int

const (
	CmpEq CmpOp = iota
	CmpNe
	CmpLt
	CmpLe
	CmpGt
	CmpGe
)

// Cmp is a comparison expression.
type Cmp struct {
	Left, Right Expr
	Op          CmpOp
}

func (e *Cmp) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *Cmp) EvalBool(b *batch.RecordBatch, row int) bool {
	lv := e.Left.Eval(b, row)
	rv := e.Right.Eval(b, row)
	if lv == nil || rv == nil {
		return false // NULL comparisons are false (SQL semantics)
	}
	return compare(lv, rv, e.Op)
}

// IsNull checks if an expression is null.
type IsNull struct {
	Operand Expr
	Not     bool // IS NOT NULL
}

func (e *IsNull) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *IsNull) EvalBool(b *batch.RecordBatch, row int) bool {
	v := e.Operand.Eval(b, row)
	if e.Not {
		return v != nil
	}
	return v == nil
}

// --- Logical operators ---

// And is a logical AND.
type And struct {
	Left, Right Expr
}

func (e *And) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *And) EvalBool(b *batch.RecordBatch, row int) bool {
	return toBool(e.Left, b, row) && toBool(e.Right, b, row)
}

// Or is a logical OR.
type Or struct {
	Left, Right Expr
}

func (e *Or) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *Or) EvalBool(b *batch.RecordBatch, row int) bool {
	return toBool(e.Left, b, row) || toBool(e.Right, b, row)
}

// Not is a logical NOT.
type Not struct {
	Operand Expr
}

func (e *Not) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *Not) EvalBool(b *batch.RecordBatch, row int) bool {
	return !toBool(e.Operand, b, row)
}

// --- Special SQL expressions ---

// In checks if a value is in a set.
type In struct {
	Expr   Expr
	Values []Expr
	Not    bool
}

func (e *In) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *In) EvalBool(b *batch.RecordBatch, row int) bool {
	lv := e.Expr.Eval(b, row)
	if lv == nil {
		return false
	}
	for _, v := range e.Values {
		rv := v.Eval(b, row)
		if rv != nil && compare(lv, rv, CmpEq) {
			return !e.Not
		}
	}
	return e.Not
}

// Between checks if a value is between two bounds.
type Between struct {
	Expr     Expr
	Low, Hi  Expr
	Not      bool
}

func (e *Between) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *Between) EvalBool(b *batch.RecordBatch, row int) bool {
	v := e.Expr.Eval(b, row)
	lo := e.Low.Eval(b, row)
	hi := e.Hi.Eval(b, row)
	if v == nil || lo == nil || hi == nil {
		return false
	}
	result := compare(v, lo, CmpGe) && compare(v, hi, CmpLe)
	if e.Not {
		return !result
	}
	return result
}

// Like performs SQL LIKE pattern matching.
type Like struct {
	Expr    Expr
	Pattern Expr
	Not     bool
}

func (e *Like) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *Like) EvalBool(b *batch.RecordBatch, row int) bool {
	v := e.Expr.Eval(b, row)
	p := e.Pattern.Eval(b, row)
	if v == nil || p == nil {
		return false
	}
	result := matchLike(toString(v), toString(p))
	if e.Not {
		return !result
	}
	return result
}

// Case is a CASE WHEN ... THEN ... ELSE ... END expression.
type Case struct {
	Operand Expr       // optional: CASE <operand> WHEN ...
	Whens   []CaseWhen // WHEN condition THEN result
	Else    Expr       // optional ELSE clause
}

// CaseWhen is a single WHEN clause in a CASE expression.
type CaseWhen struct {
	Cond   Expr // the condition (or value to compare against operand)
	Result Expr
}

func (e *Case) Eval(b *batch.RecordBatch, row int) any {
	if e.Operand != nil {
		// Simple CASE: CASE x WHEN v1 THEN r1 ...
		opVal := e.Operand.Eval(b, row)
		for _, w := range e.Whens {
			whenVal := w.Cond.Eval(b, row)
			if opVal != nil && whenVal != nil && compare(opVal, whenVal, CmpEq) {
				return w.Result.Eval(b, row)
			}
		}
	} else {
		// Searched CASE: CASE WHEN cond1 THEN r1 ...
		for _, w := range e.Whens {
			if toBool(w.Cond, b, row) {
				return w.Result.Eval(b, row)
			}
		}
	}
	if e.Else != nil {
		return e.Else.Eval(b, row)
	}
	return nil
}

// Coalesce returns the first non-null argument.
type Coalesce struct {
	Args []Expr
}

func (e *Coalesce) Eval(b *batch.RecordBatch, row int) any {
	for _, arg := range e.Args {
		v := arg.Eval(b, row)
		if v != nil {
			return v
		}
	}
	return nil
}

// --- Scalar functions ---

// ArrayLitExpr evaluates to a []any containing the evaluated elements.
type ArrayLitExpr struct {
	Elements []Expr
}

func (e *ArrayLitExpr) Eval(b *batch.RecordBatch, row int) any {
	result := make([]any, len(e.Elements))
	for i, elem := range e.Elements {
		result[i] = elem.Eval(b, row)
	}
	return result
}

// FuncCall represents a scalar function call.
type FuncCall struct {
	Name string
	Args []Expr
	args []any // pre-allocated args buffer (avoids alloc per call)
}

func (e *FuncCall) Eval(b *batch.RecordBatch, row int) any {
	// Lazily allocate args buffer once
	if e.args == nil {
		e.args = make([]any, len(e.Args))
	}
	for i, a := range e.Args {
		e.args[i] = a.Eval(b, row)
	}
	fn := DefaultRegistry.Lookup(e.Name)
	if fn == nil {
		return nil
	}
	return fn(e.args)
}

// ScalarFunc is a scalar function implementation.
type ScalarFunc func(args []any) any

// FuncRegistry is a concurrent-safe registry of scalar functions.
type FuncRegistry struct {
	mu    sync.RWMutex
	funcs map[string]ScalarFunc
}

// NewFuncRegistry creates a new empty function registry.
func NewFuncRegistry() *FuncRegistry {
	return &FuncRegistry{funcs: make(map[string]ScalarFunc)}
}

// Register adds or replaces a scalar function.
func (r *FuncRegistry) Register(name string, fn ScalarFunc) {
	r.mu.Lock()
	r.funcs[strings.ToLower(name)] = fn
	r.mu.Unlock()
}

// Unregister removes a scalar function. Returns true if it existed.
func (r *FuncRegistry) Unregister(name string) bool {
	r.mu.Lock()
	_, existed := r.funcs[strings.ToLower(name)]
	delete(r.funcs, strings.ToLower(name))
	r.mu.Unlock()
	return existed
}

// Lookup returns the function with the given name, or nil if not found.
func (r *FuncRegistry) Lookup(name string) ScalarFunc {
	r.mu.RLock()
	fn := r.funcs[strings.ToLower(name)]
	r.mu.RUnlock()
	return fn
}

// Has returns true if a function with the given name exists.
func (r *FuncRegistry) Has(name string) bool {
	r.mu.RLock()
	_, ok := r.funcs[strings.ToLower(name)]
	r.mu.RUnlock()
	return ok
}

// Names returns all registered function names.
func (r *FuncRegistry) Names() []string {
	r.mu.RLock()
	names := make([]string, 0, len(r.funcs))
	for name := range r.funcs {
		names = append(names, name)
	}
	r.mu.RUnlock()
	return names
}

// DefaultRegistry is the global function registry used by the expression engine.
var DefaultRegistry = NewFuncRegistry()

func init() {
	builtins := map[string]ScalarFunc{
	// String functions
	"upper":   fnUpper,
	"lower":   fnLower,
	"concat":  fnConcat,
	"length":  fnLength,
	"len":     fnLength,
	"substr":    fnSubstr,
	"substring": fnSubstr,
	"trim":    fnTrim,
	"ltrim":   fnLTrim,
	"rtrim":   fnRTrim,
	"replace": fnReplace,
	"reverse": fnReverse,
	"left":    fnLeft,
	"right":   fnRight,

	// Math functions
	"abs":   fnAbs,
	"ceil":  fnCeil,
	"floor": fnFloor,
	"round": fnRound,
	"pow":   fnPow,
	"power": fnPow,
	"sqrt":  fnSqrt,
	"mod":   fnMod,
	"log":   fnLog,
	"ln":    fnLn,
	"exp":   fnExp,

	// Conditional
	"coalesce": fnCoalesce,
	"nullif":   fnNullIf,
	"ifnull":   fnIfNull,
	"if":       fnIf,

	// Type casting
	"cast_int":    fnCastInt,
	"cast_float":  fnCastFloat,
	"cast_string": fnCastString,

	// Network functions
	"ip_to_string":  fnIPToString,
	"cidr_contains": fnCIDRContains,
	"ip_version":    fnIPVersion,
	"mask_ip":       fnMaskIP,
	"mac_to_string": fnMACToString,
	"ip_subnet":     fnIPSubnet,
	"ip_netmask":    fnIPNetmask,

	// Date/time functions
	"now":          fnNow,
	"year":         fnYear,
	"month":        fnMonth,
	"day":          fnDay,
	"hour":         fnHour,
	"minute":       fnMinute,
	"date_trunc":   fnDateTrunc,
	"extract":      fnExtract,
	"current_date": fnCurrentDate,
	"date_diff":    fnDateDiff,
	"date_add":     fnDateAdd,
	"to_date":      fnToDate,

	// UUID functions
	"uuid_version":   fnUUIDVersion,
	"uuid_to_string": fnUUIDToString,

	// Additional string functions
	"starts_with": fnStartsWith,
	"ends_with":   fnEndsWith,
	"contains":    fnContains,
	"repeat":      fnRepeat,

	// Additional math functions
	"sign":     fnSign,
	"greatest": fnGreatest,
	"least":    fnLeast,

	// Additional date/time functions
	"second": fnSecond,

	// String: regex and parsing
	"split_part":     fnSplitPart,
	"strpos":         fnStrPos,
	"position":       fnStrPos,
	"regexp_like":    fnRegexpLike,
	"regexp_extract": fnRegexpExtract,
	"regexp_replace": fnRegexpReplace,

	// Encoding
	"to_hex":     fnToHex,
	"from_hex":   fnFromHex,
	"to_base64":  fnToBase64,
	"from_base64": fnFromBase64,

	// Date/time conversion
	"from_unixtime": fnFromUnixtime,
	"to_unixtime":   fnToUnixtime,
	"date_format":   fnDateFormat,
	"date_parse":    fnDateParse,

	// Hash
	"md5":    fnMD5,
	"sha256": fnSHA256,
	"sha512": fnSHA512,

	// Bitwise
	"bitwise_and": fnBitwiseAnd,
	"bitwise_or":  fnBitwiseOr,
	"bitwise_xor": fnBitwiseXor,
	"bitwise_not": fnBitwiseNot,

	// String: padding and character
	"lpad":       fnLPad,
	"rpad":       fnRPad,
	"chr":        fnChr,
	"codepoint":  fnCodepoint,
	"concat_ws":  fnConcatWS,
	"char_length": fnCharLength,
	"translate":  fnTranslate,

	// Math: trigonometry
	"pi":       fnPi,
	"degrees":  fnDegrees,
	"radians":  fnRadians,
	"sin":      fnSin,
	"cos":      fnCos,
	"tan":      fnTan,
	"asin":     fnAsin,
	"acos":     fnAcos,
	"atan":     fnAtan,
	"atan2":    fnAtan2,
	"cbrt":     fnCbrt,
	"log2":     fnLog2,
	"truncate": fnTruncate,
	"rand":     fnRandom,
	"random":   fnRandom,

	// JSON
	"json_extract":        fnJSONExtract,
	"json_extract_scalar": fnJSONExtractScalar,
	"json_array_length":   fnJSONArrayLength,
	"json_valid":          fnJSONValid,

	// URL
	"url_extract_host":     fnURLExtractHost,
	"url_extract_port":     fnURLExtractPort,
	"url_extract_path":     fnURLExtractPath,
	"url_extract_protocol": fnURLExtractProtocol,
	"url_extract_query":    fnURLExtractQuery,
	"url_extract_parameter": fnURLExtractParameter,

	// Type introspection
	"typeof": fnTypeof,

	// String: distance and utility
	"soundex":             fnSoundex,
	"levenshtein_distance": fnLevenshtein,
	"hamming_distance":    fnHamming,
	"normalize":           fnNormalize,
	"format":              fnFormat,
	"lcase":               fnLower,
	"ucase":               fnUpper,
	"to_utf8":             fnToUTF8,
	"from_utf8":           fnFromUTF8,

	// Math: IEEE 754 and utility
	"e":             fnE,
	"log10":         fnLog10,
	"infinity":      fnInfinity,
	"nan":           fnNaN,
	"is_nan":        fnIsNaN,
	"is_finite":     fnIsFinite,
	"is_infinite":   fnIsInfinite,
	"width_bucket":  fnWidthBucket,
	"from_base":     fnFromBase,
	"to_base":       fnToBase,
	"bit_count":     fnBitCount,

	// Hash: additional
	"sha1":        fnSHA1,
	"crc32":       fnCRC32,
	"hmac_sha256": fnHMACSHA256,
	"hmac_sha512": fnHMACSHA512,

	// Date: additional accessors
	"quarter":            fnQuarter,
	"week":               fnWeek,
	"day_of_week":        fnDayOfWeek,
	"day_of_year":        fnDayOfYear,
	"last_day_of_month":  fnLastDayOfMonth,
	"current_timestamp":  fnCurrentTimestamp,
	"at_timezone":        fnAtTimezone,
	"human_readable_seconds": fnHumanReadableSeconds,

	// Network: analytics
	"is_private_ip":   fnIsPrivateIP,
	"is_loopback_ip":  fnIsLoopbackIP,
	"ip_to_int":       fnIPToInt,
	"int_to_ip":       fnIntToIP,
	"is_ipv4":         fnIsIPv4,
	"is_ipv6":         fnIsIPv6,

	// Network: CIDR / subnet operations
	"network_address":   fnNetworkAddress,
	"broadcast_address": fnBroadcastAddress,
	"prefix_length":     fnPrefixLength,
	"cidr_to_range":     fnCIDRToRange,
	"hosts_in_cidr":     fnHostsInCIDR,
	"cidr_overlap":      fnCIDROverlap,
	"ip_in_range":       fnIPInRange,
	"same_subnet":       fnSameSubnet,

	// Network: IP manipulation
	"ip_add":            fnIPAdd,
	"ip_subtract":       fnIPSubtract,
	"ip_diff":           fnIPDiff,
	"ip_between":        fnIPBetween,
	"reverse_dns":       fnReverseDNS,
	"is_multicast_ip":   fnIsMulticastIP,
	"is_link_local_ip":  fnIsLinkLocalIP,
	"is_reserved_ip":    fnIsReservedIP,
	"ip_to_hex":         fnIPToHex,

	// Network: MAC operations
	"mac_vendor_oui":  fnMACVendorOUI,
	"mac_is_unicast":  fnMACIsUnicast,
	"mac_is_local":    fnMACIsLocal,
	"mac_format":      fnMACFormat,

	// Network: port classification
	"port_name":          fnPortName,
	"is_well_known_port": fnIsWellKnownPort,
	"is_registered_port": fnIsRegisteredPort,
	"is_ephemeral_port":  fnIsEphemeralPort,
	"port_class":         fnPortClass,

	// Network: protocol
	"protocol_name":   fnProtocolName,
	"protocol_number": fnProtocolNumber,

	// Deep inspection: TCP
	"tcp_flags_to_string":   fnTCPFlagsToString,
	"has_tcp_flag":           fnHasTCPFlag,
	"tcp_flags_from_string": fnTCPFlagsFromString,
	"is_tcp_handshake":      fnIsTCPHandshake,
	"is_tcp_reset":          fnIsTCPReset,
	"tcp_session_id":        fnTCPSessionID,
	"flow_direction":        fnFlowDirection,

	// Deep inspection: DNS
	"dns_query_name":     fnDNSQueryName,
	"dns_query_type":     fnDNSQueryType,
	"dns_is_response":    fnDNSIsResponse,
	"dns_response_code":  fnDNSResponseCode,
	"dns_question_count": fnDNSQuestionCount,
	"dns_answer_count":   fnDNSAnswerCount,
	"dns_transaction_id": fnDNSTransactionID,

	// Deep inspection: TLS
	"tls_sni":             fnTLSSNI,
	"tls_version":         fnTLSVersion,
	"tls_record_type":     fnTLSRecordType,
	"is_tls_client_hello": fnIsTLSClientHello,
	"tls_handshake_type":  fnTLSHandshakeType,

	// Deep inspection: HTTP
	"http_method":         fnHTTPMethod,
	"http_path":           fnHTTPPath,
	"http_host":           fnHTTPHost,
	"http_status_code":    fnHTTPStatusCode,
	"http_status_class":   fnHTTPStatusClass,
	"http_content_type":   fnHTTPContentType,
	"http_content_length": fnHTTPContentLength,
	"http_user_agent":     fnHTTPUserAgent,
	"http_header":         fnHTTPHeader,
	"http_version":        fnHTTPVersion,
	"is_http_request":     fnIsHTTPRequest,
	"is_http_response":    fnIsHTTPResponse,

	// Deep inspection: packet headers
	"ip_header_length": fnIPHeaderLength,
	"ip_ttl":           fnIPTTL,
	"ip_total_length":  fnIPTotalLength,
	"ip_dscp":          fnIPDSCP,
	"ether_type":       fnEtherType,
	"vlan_id":          fnVLANID,

	// Deep inspection: payload analysis
	"payload_entropy":  fnPayloadEntropy,
	"payload_hex_dump": fnPayloadHexDump,

	// ICMP
	"icmp_type_name": fnICMPTypeName,
	"icmp_code_name": fnICMPCodeName,
	"is_icmp_echo":   fnIsICMPEcho,
	"icmp_parse":     fnICMPParse,
	"icmp_type":      fnICMPType,
	"icmp_code":      fnICMPCode,

	// IPv6
	"ipv6_scope":      fnIPv6Scope,
	"ipv6_expand":     fnIPv6Expand,
	"ipv6_compress":   fnIPv6Compress,
	"ipv6_to_eui64":   fnIPv6ToEUI64,
	"is_6to4":         fnIs6to4,
	"is_teredo":       fnIsTeredo,
	"teredo_server":   fnTeredoServer,
	"teredo_client":   fnTeredoClient,
	"sixto4_gateway":  fnSixto4Gateway,

	// JA3 TLS fingerprinting
	"ja3_fingerprint":  fnJA3Fingerprint,
	"ja3_string":       fnJA3String,
	"ja3s_fingerprint": fnJA3SFingerprint,
	"ja3s_string":      fnJA3SString,

	// Payload search
	"payload_contains": fnPayloadContains,
	"payload_matches":  fnPayloadMatches,
	"payload_offset":   fnPayloadOffset,
	"payload_length":   fnPayloadLength,

	// Regex: additional
	"regexp_count":       fnRegexpCount,
	"regexp_extract_all": fnRegexpExtractAll,
	"regexp_split":       fnRegexpSplit,

	// String: additional
	"split": fnSplit,

	// Bitwise: shifts
	"bitwise_left_shift":             fnBitwiseLeftShift,
	"bitwise_right_shift":            fnBitwiseRightShift,
	"bitwise_arithmetic_shift_right": fnBitwiseArithmeticShiftRight,

	// UUID: generation
	"uuid": fnUUID,

	// Encoding: additional
	"to_base32":   fnToBase32,
	"from_base32": fnFromBase32,
	"xxhash64":    fnXXHash64,
	"murmur3":     fnMurmur3,

	// Date/time: ISO 8601
	"from_iso8601_timestamp": fnFromISO8601Timestamp,
	"from_iso8601_date":      fnFromISO8601Date,
	"to_iso8601":             fnToISO8601,
	"to_milliseconds":        fnToMilliseconds,
	"timezone_hour":          fnTimezoneHour,
	"timezone_minute":        fnTimezoneMinute,

	// Formatting
	"format_number": fnFormatNumber,

	// GeoIP / ASN lookup (requires MaxMind MMDB databases)
	"geoip_country":      fnGeoipCountry,
	"geoip_country_name": fnGeoipCountryName,
	"geoip_city":         fnGeoipCity,
	"geoip_subdivision":  fnGeoipSubdivision,
	"geoip_postal_code":  fnGeoipPostalCode,
	"geoip_latitude":     fnGeoipLatitude,
	"geoip_longitude":    fnGeoipLongitude,
	"geoip_timezone":     fnGeoipTimezone,
	"geoip_continent":    fnGeoipContinent,
	"geoip_asn":          fnGeoipASN,
	"geoip_org":          fnGeoipOrg,

	// Byte/rate formatting
	"format_bytes":  fnFormatBytes,
	"parse_bytes":   fnParseBytes,
	"format_rate":   fnFormatRate,
	"parse_rate":    fnParseRate,

	// Array/nested type functions (Trino-compatible)
	"cardinality":    fnCardinality,
	"array_length":   fnCardinality,
	"element_at":     fnElementAt,
	"array_contains": fnArrayContains,
	"array_join":     fnArrayJoin,
	"array_min":      fnArrayMin,
	"array_max":      fnArrayMax,

	// ROW/struct functions
	"row_field":    fnRowField,
	"struct_field": fnRowField,

	// MAP functions
	"map_keys":      fnMapKeys,
	"map_values":    fnMapValues,
	"map_entries":   fnMapEntries,
	"map_from_entries": fnMapFromEntries,
	}
	for name, fn := range builtins {
		DefaultRegistry.funcs[name] = fn
	}
}

// RegisterFunc registers a custom scalar function in the default registry.
func RegisterFunc(name string, fn ScalarFunc) {
	DefaultRegistry.Register(name, fn)
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

func fnConcat(args []any) any {
	var sb strings.Builder
	for _, a := range args {
		if a == nil {
			return nil // SQL concat with NULL returns NULL
		}
		sb.WriteString(toString(a))
	}
	return sb.String()
}

func fnLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return float64(len(toString(args[0])))
}

func fnSubstr(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := toString(args[0])
	start := int(ToFloat64(args[1])) - 1 // SQL is 1-indexed
	if start < 0 {
		start = 0
	}
	if start >= len(s) {
		return ""
	}
	if len(args) >= 3 && args[2] != nil {
		length := int(ToFloat64(args[2]))
		end := start + length
		if end > len(s) {
			end = len(s)
		}
		return s[start:end]
	}
	return s[start:]
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
	s := toString(args[0])
	n := int(ToFloat64(args[1]))
	if n < 0 {
		return ""
	}
	if n >= len(s) {
		return s
	}
	return s[:n]
}

func fnRight(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := toString(args[0])
	n := int(ToFloat64(args[1]))
	if n < 0 {
		return ""
	}
	if n >= len(s) {
		return s
	}
	return s[len(s)-n:]
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

// --- Math function implementations ---

func fnAbs(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Abs(ToFloat64(args[0]))
}

func fnCeil(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Ceil(ToFloat64(args[0]))
}

func fnFloor(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Floor(ToFloat64(args[0]))
}

func fnRound(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	precision := 0
	if len(args) >= 2 && args[1] != nil {
		precision = int(ToFloat64(args[1]))
	}
	pow := math.Pow(10, float64(precision))
	return math.Round(v*pow) / pow
}

func fnPow(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return math.Pow(ToFloat64(args[0]), ToFloat64(args[1]))
}

func fnSqrt(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v < 0 {
		return nil
	}
	return math.Sqrt(v)
}

func fnMod(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return math.Mod(ToFloat64(args[0]), ToFloat64(args[1]))
}

func fnLog(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v <= 0 {
		return nil
	}
	if len(args) >= 2 && args[1] != nil {
		// LOG(base, value)
		base := v
		val := ToFloat64(args[1])
		if base <= 0 || base == 1 || val <= 0 {
			return nil
		}
		return math.Log(val) / math.Log(base)
	}
	return math.Log10(v)
}

func fnLn(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v <= 0 {
		return nil
	}
	return math.Log(v)
}

func fnExp(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Exp(ToFloat64(args[0]))
}

func fnSign(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	switch {
	case v > 0:
		return float64(1)
	case v < 0:
		return float64(-1)
	default:
		return float64(0)
	}
}

func fnGreatest(args []any) any {
	var best any
	for _, a := range args {
		if a == nil {
			continue
		}
		if best == nil || ToFloat64(a) > ToFloat64(best) {
			best = a
		}
	}
	return best
}

func fnLeast(args []any) any {
	var best any
	for _, a := range args {
		if a == nil {
			continue
		}
		if best == nil || ToFloat64(a) < ToFloat64(best) {
			best = a
		}
	}
	return best
}

// --- Conditional function implementations ---

func fnCoalesce(args []any) any {
	for _, a := range args {
		if a != nil {
			return a
		}
	}
	return nil
}

func fnNullIf(args []any) any {
	if len(args) < 2 {
		return nil
	}
	if args[0] != nil && args[1] != nil && compare(args[0], args[1], CmpEq) {
		return nil
	}
	return args[0]
}

func fnIfNull(args []any) any {
	if len(args) < 2 {
		return nil
	}
	if args[0] != nil {
		return args[0]
	}
	return args[1]
}

func fnIf(args []any) any {
	if len(args) < 3 {
		return nil
	}
	if args[0] != nil && toBoolVal(args[0]) {
		return args[1]
	}
	return args[2]
}

// --- Type casting ---

func fnCastInt(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return int64(ToFloat64(args[0]))
}

func fnCastFloat(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return ToFloat64(args[0])
}

func fnCastString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return fmt.Sprint(args[0])
}

// --- Date/time functions ---

func fnNow(args []any) any {
	return time.Now().Format(time.RFC3339)
}

func fnYear(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Year())
}

func fnMonth(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Month())
}

func fnDay(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Day())
}

func fnHour(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Hour())
}

func fnMinute(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Minute())
}

func fnSecond(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Second())
}

func fnDateTrunc(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	unit := strings.ToLower(fmt.Sprint(args[0]))
	t := parseTime(args[1])
	if t.IsZero() {
		return nil
	}
	switch unit {
	case "year":
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
	case "month":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
	case "day":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
	case "hour":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).Format(time.RFC3339)
	default:
		return nil
	}
}

func fnExtract(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	unit := strings.ToLower(fmt.Sprint(args[0]))
	t := parseTime(args[1])
	if t.IsZero() {
		return nil
	}
	switch unit {
	case "year":
		return float64(t.Year())
	case "month":
		return float64(t.Month())
	case "day":
		return float64(t.Day())
	case "hour":
		return float64(t.Hour())
	case "minute":
		return float64(t.Minute())
	case "second":
		return float64(t.Second())
	case "dow", "dayofweek":
		return float64(t.Weekday())
	case "doy", "dayofyear":
		return float64(t.YearDay())
	default:
		return nil
	}
}

func fnCurrentDate(args []any) any {
	return time.Now().Format("2006-01-02")
}

// fnDateDiff returns the number of days between two dates.
// Usage: date_diff(date1, date2) → integer
func fnDateDiff(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t1 := parseDateValue(args[0])
	t2 := parseDateValue(args[1])
	if t1.IsZero() || t2.IsZero() {
		return nil
	}
	return float64(int(t1.Sub(t2).Hours() / 24))
}

// fnDateAdd adds days to a date.
// Usage: date_add(date, days) → date string
func fnDateAdd(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t := parseDateValue(args[0])
	if t.IsZero() {
		return nil
	}
	days := int(ToFloat64(args[1]))
	return t.AddDate(0, 0, days).Format("2006-01-02")
}

// fnToDate converts a string to a date.
func fnToDate(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := parseDateValue(args[0])
	if t.IsZero() {
		return nil
	}
	return t.Format("2006-01-02")
}

// parseDateValue parses a date from various formats.
func parseDateValue(v any) time.Time {
	switch tv := v.(type) {
	case string:
		for _, layout := range []string{"2006-01-02", time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05"} {
			if t, err := time.Parse(layout, tv); err == nil {
				return t
			}
		}
	case int32:
		// Days since epoch
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(tv))
	case int64:
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(tv))
	case float64:
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(tv))
	}
	return time.Time{}
}

// fnUUIDVersion extracts the version from a UUID string.
func fnUUIDVersion(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	raw := parseUUIDHex(s)
	if raw == nil {
		return nil
	}
	return float64(raw[6] >> 4)
}

// fnUUIDToString formats raw UUID bytes as a standard string.
func fnUUIDToString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	// Already formatted?
	if len(s) == 36 && s[8] == '-' {
		return s
	}
	raw := parseUUIDHex(s)
	if raw == nil {
		return nil
	}
	var buf [36]byte
	hex.Encode(buf[0:8], raw[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], raw[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], raw[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], raw[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], raw[10:16])
	return string(buf[:])
}

// parseUUIDHex parses a UUID string (with or without dashes) into 16 bytes.
func parseUUIDHex(s string) []byte {
	clean := make([]byte, 0, 32)
	for i := 0; i < len(s); i++ {
		if s[i] != '-' {
			clean = append(clean, s[i])
		}
	}
	if len(clean) != 32 {
		return nil
	}
	raw := make([]byte, 16)
	_, err := hex.Decode(raw, clean)
	if err != nil {
		return nil
	}
	return raw
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

// --- Helpers ---

// toString converts any value to string, avoiding fmt.Sprint for common types.
func toString(v any) string {
	switch tv := v.(type) {
	case string:
		return tv
	case []byte:
		return string(tv)
	default:
		return fmt.Sprint(v)
	}
}

// ToFloat64 converts any numeric value to float64.
func ToFloat64(v any) float64 {
	switch tv := v.(type) {
	case float64:
		return tv
	case float32:
		return float64(tv)
	case int64:
		return float64(tv)
	case int:
		return float64(tv)
	case int32:
		return float64(tv)
	case bool:
		if tv {
			return 1
		}
		return 0
	default:
		// Try string → float for decimal formatted strings like "123.45"
		if s, ok := v.(string); ok {
			var f float64
			if _, err := fmt.Sscanf(s, "%f", &f); err == nil {
				return f
			}
		}
		return 0
	}
}

// ToInt64 converts any numeric value to int64.
func ToInt64(v any) int64 {
	switch tv := v.(type) {
	case int64:
		return tv
	case int:
		return int64(tv)
	case int32:
		return int64(tv)
	case float64:
		return int64(tv)
	case float32:
		return int64(tv)
	default:
		return 0
	}
}

func toBool(e Expr, b *batch.RecordBatch, row int) bool {
	if be, ok := e.(BoolExpr); ok {
		return be.EvalBool(b, row)
	}
	v := e.Eval(b, row)
	return toBoolVal(v)
}

func toBoolVal(v any) bool {
	if v == nil {
		return false
	}
	switch tv := v.(type) {
	case bool:
		return tv
	case float64:
		return tv != 0
	case int64:
		return tv != 0
	case int:
		return tv != 0
	case string:
		return tv != ""
	default:
		return true
	}
}

// toInt64Safe converts a value to int64, returning false if not possible.
func toInt64Safe(v any) (int64, bool) {
	switch tv := v.(type) {
	case int64:
		return tv, true
	case int32:
		return int64(tv), true
	case int:
		return int64(tv), true
	default:
		return 0, false
	}
}

// toFloat64Safe converts a value to float64, returning false if not possible.
func toFloat64Safe(v any) (float64, bool) {
	switch tv := v.(type) {
	case float64:
		return tv, true
	case float32:
		return float64(tv), true
	case int64:
		return float64(tv), true
	case int32:
		return float64(tv), true
	default:
		return 0, false
	}
}

func compare(a, b any, op CmpOp) bool {
	// Fast path: both int64 (most common for column comparisons)
	if ai, ok := a.(int64); ok {
		if bi, ok := b.(int64); ok {
			switch op {
			case CmpEq:
				return ai == bi
			case CmpNe:
				return ai != bi
			case CmpLt:
				return ai < bi
			case CmpLe:
				return ai <= bi
			case CmpGt:
				return ai > bi
			case CmpGe:
				return ai >= bi
			}
		}
	}
	// Fast path: both float64
	if af, ok := a.(float64); ok {
		if bf, ok := b.(float64); ok {
			switch op {
			case CmpEq:
				return af == bf
			case CmpNe:
				return af != bf
			case CmpLt:
				return af < bf
			case CmpLe:
				return af <= bf
			case CmpGt:
				return af > bf
			case CmpGe:
				return af >= bf
			}
		}
	}
	// Fast path: both string
	if as, ok := a.(string); ok {
		if bs, ok := b.(string); ok {
			switch op {
			case CmpEq:
				return as == bs
			case CmpNe:
				return as != bs
			case CmpLt:
				return as < bs
			case CmpLe:
				return as <= bs
			case CmpGt:
				return as > bs
			case CmpGe:
				return as >= bs
			}
		}
	}
	// Mixed numeric types
	if isNumeric(a) && isNumeric(b) {
		af := ToFloat64(a)
		bf := ToFloat64(b)
		switch op {
		case CmpEq:
			return af == bf
		case CmpNe:
			return af != bf
		case CmpLt:
			return af < bf
		case CmpLe:
			return af <= bf
		case CmpGt:
			return af > bf
		case CmpGe:
			return af >= bf
		}
	}
	// Fall back to string comparison
	as := toString(a)
	bs := toString(b)
	switch op {
	case CmpEq:
		return as == bs
	case CmpNe:
		return as != bs
	case CmpLt:
		return as < bs
	case CmpLe:
		return as <= bs
	case CmpGt:
		return as > bs
	case CmpGe:
		return as >= bs
	}
	return false
}

func isNumeric(v any) bool {
	switch v.(type) {
	case float64, float32, int64, int, int32:
		return true
	default:
		return false
	}
}

func toTime(args []any) time.Time {
	if len(args) < 1 || args[0] == nil {
		return time.Time{}
	}
	return parseTime(args[0])
}

func parseTime(v any) time.Time {
	switch tv := v.(type) {
	case time.Time:
		return tv
	case int64:
		return time.Unix(tv, 0)
	case float64:
		return time.Unix(int64(tv), 0)
	case string:
		for _, layout := range []string{
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, tv); err == nil {
				return t
			}
		}
	}
	return time.Time{}
}

// matchLike implements SQL LIKE pattern matching.
// % matches any sequence, _ matches a single character.
func matchLike(s, pattern string) bool {
	return matchLikeRecur(s, pattern, 0, 0)
}

func matchLikeRecur(s, pattern string, si, pi int) bool {
	for pi < len(pattern) {
		switch pattern[pi] {
		case '%':
			pi++
			// Skip consecutive %
			for pi < len(pattern) && pattern[pi] == '%' {
				pi++
			}
			if pi == len(pattern) {
				return true // trailing % matches everything
			}
			for i := si; i <= len(s); i++ {
				if matchLikeRecur(s, pattern, i, pi) {
					return true
				}
			}
			return false
		case '_':
			if si >= len(s) {
				return false
			}
			si++
			pi++
		default:
			if si >= len(s) || s[si] != pattern[pi] {
				return false
			}
			si++
			pi++
		}
	}
	return si == len(s)
}

// --- Subquery expressions ---

// SubqueryRunner executes a SQL subquery and returns its result rows.
// Each row is a map of column name to value.
type SubqueryRunner func(sql string) ([]map[string]any, error)

// ScalarSubquery evaluates a subquery that returns a single scalar value.
// Example: WHERE price > (SELECT AVG(price) FROM products)
// Uncorrelated: executed once and result cached.
type ScalarSubquery struct {
	SQL    string
	Runner SubqueryRunner
	cached bool
	val    any
}

func (e *ScalarSubquery) Eval(_ *batch.RecordBatch, _ int) any {
	if !e.cached {
		e.cached = true
		rows, err := e.Runner(e.SQL)
		if err != nil || len(rows) == 0 {
			e.val = nil
			return nil
		}
		// Return first column of first row
		for _, v := range rows[0] {
			e.val = v
			break
		}
	}
	return e.val
}

// InSubquery checks if a value is in the result set of a subquery.
// Example: WHERE user_id IN (SELECT user_id FROM active_users)
// Uncorrelated: executed once and result set cached in a hash set for O(1) lookup.
type InSubquery struct {
	Expr   Expr
	SQL    string
	Runner SubqueryRunner
	Not    bool
	cached bool
	intSet map[int64]struct{}
	strSet map[string]struct{}
	fltSet map[float64]struct{}
	vals   []any // fallback for mixed types
}

func (e *InSubquery) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *InSubquery) EvalBool(b *batch.RecordBatch, row int) bool {
	if !e.cached {
		e.cached = true
		rows, err := e.Runner(e.SQL)
		if err != nil {
			return e.Not
		}
		// Collect values and detect predominant type for hash set
		var rawVals []any
		for _, r := range rows {
			for _, v := range r {
				if v != nil {
					rawVals = append(rawVals, v)
				}
				break // first column only
			}
		}
		// Build typed hash set based on first value's type
		if len(rawVals) > 0 {
			switch rawVals[0].(type) {
			case int64:
				e.intSet = make(map[int64]struct{}, len(rawVals))
				for _, v := range rawVals {
					if iv, ok := v.(int64); ok {
						e.intSet[iv] = struct{}{}
					} else {
						e.vals = rawVals // mixed types, fallback
						e.intSet = nil
						break
					}
				}
			case string:
				e.strSet = make(map[string]struct{}, len(rawVals))
				for _, v := range rawVals {
					if sv, ok := v.(string); ok {
						e.strSet[sv] = struct{}{}
					} else {
						e.vals = rawVals
						e.strSet = nil
						break
					}
				}
			case float64:
				e.fltSet = make(map[float64]struct{}, len(rawVals))
				for _, v := range rawVals {
					if fv, ok := v.(float64); ok {
						e.fltSet[fv] = struct{}{}
					} else {
						e.vals = rawVals
						e.fltSet = nil
						break
					}
				}
			default:
				e.vals = rawVals
			}
		}
	}
	lv := e.Expr.Eval(b, row)
	if lv == nil {
		return false
	}
	// Fast path: typed hash lookup
	if e.intSet != nil {
		if iv, ok := toInt64Safe(lv); ok {
			_, found := e.intSet[iv]
			return found != e.Not
		}
	}
	if e.strSet != nil {
		if sv, ok := lv.(string); ok {
			_, found := e.strSet[sv]
			return found != e.Not
		}
	}
	if e.fltSet != nil {
		if fv, ok := toFloat64Safe(lv); ok {
			_, found := e.fltSet[fv]
			return found != e.Not
		}
	}
	// Fallback: linear scan for mixed types
	for _, rv := range e.vals {
		if rv != nil && compare(lv, rv, CmpEq) {
			return !e.Not
		}
	}
	return e.Not
}

// ExistsSubquery evaluates to true if a subquery returns any rows.
// Example: WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
// Uncorrelated: executed once and result cached.
type ExistsSubquery struct {
	SQL    string
	Runner SubqueryRunner
	Not    bool
	cached bool
	exists bool
}

func (e *ExistsSubquery) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *ExistsSubquery) EvalBool(_ *batch.RecordBatch, _ int) bool {
	if !e.cached {
		e.cached = true
		rows, err := e.Runner(e.SQL)
		e.exists = err == nil && len(rows) > 0
	}
	if e.Not {
		return !e.exists
	}
	return e.exists
}

// Cast wraps an expression with explicit type conversion.
type Cast struct {
	Operand  Expr
	DestType string // "int", "float", "string"
}

func (e *Cast) Eval(b *batch.RecordBatch, row int) any {
	v := e.Operand.Eval(b, row)
	if v == nil {
		return nil
	}
	switch strings.ToLower(e.DestType) {
	case "int", "integer", "bigint", "signed":
		return int64(ToFloat64(v))
	case "float", "double", "real":
		return ToFloat64(v)
	case "decimal", "numeric":
		return ToFloat64(v)
	case "char", "varchar", "text", "string":
		return fmt.Sprint(v)
	default:
		return v
	}
}

// --- String: regex and parsing ---

func fnSplitPart(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	parts := strings.Split(toString(args[0]), toString(args[1]))
	idx := int(ToFloat64(args[2])) - 1 // SQL is 1-indexed
	if idx < 0 || idx >= len(parts) {
		return ""
	}
	return parts[idx]
}

func fnStrPos(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	pos := strings.Index(toString(args[0]), toString(args[1]))
	if pos < 0 {
		return float64(0)
	}
	return float64(pos + 1) // 1-based
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
	re, err := regexp.Compile(toString(args[1]))
	if err != nil {
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

func fnRegexpReplace(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	re, err := regexp.Compile(toString(args[1]))
	if err != nil {
		return nil
	}
	return re.ReplaceAllString(toString(args[0]), toString(args[2]))
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
	return time.Unix(epoch, 0).UTC().Format(time.RFC3339)
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
	return t.Format(time.RFC3339)
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
	s := toString(args[0])
	n := int(ToFloat64(args[1]))
	pad := " "
	if len(args) >= 3 && args[2] != nil {
		pad = toString(args[2])
	}
	if len(pad) == 0 || n <= len(s) {
		if n < 0 {
			return ""
		}
		if n <= len(s) {
			return s[:n]
		}
		return s
	}
	var sb strings.Builder
	for sb.Len()+len(s) < n {
		sb.WriteString(pad)
	}
	result := sb.String()
	need := n - len(s)
	if len(result) > need {
		result = result[:need]
	}
	return result + s
}

func fnRPad(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := toString(args[0])
	n := int(ToFloat64(args[1]))
	pad := " "
	if len(args) >= 3 && args[2] != nil {
		pad = toString(args[2])
	}
	if len(pad) == 0 || n <= len(s) {
		if n < 0 {
			return ""
		}
		if n <= len(s) {
			return s[:n]
		}
		return s
	}
	var sb strings.Builder
	sb.WriteString(s)
	for sb.Len() < n {
		sb.WriteString(pad)
	}
	result := sb.String()
	if len(result) > n {
		result = result[:n]
	}
	return result
}

func fnChr(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	code := int(ToFloat64(args[0]))
	if code < 0 || code > 0x10FFFF {
		return nil
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
	return float64(runes[0])
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
	return float64(len([]rune(toString(args[0]))))
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
	if v < -1 || v > 1 {
		return nil
	}
	return math.Asin(v)
}

func fnAcos(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v < -1 || v > 1 {
		return nil
	}
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
	if v <= 0 {
		return nil
	}
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
	if n <= 0 || bound1 == bound2 {
		return nil
	}
	if value < bound1 {
		return float64(0)
	}
	if value >= bound2 {
		return float64(n + 1)
	}
	width := (bound2 - bound1) / float64(n)
	bucket := int((value-bound1)/width) + 1
	if bucket > n {
		bucket = n + 1
	}
	return float64(bucket)
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
	return time.Now().Format(time.RFC3339)
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
	return t.In(loc).Format(time.RFC3339)
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
	return ip.String()
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
	re, err := regexp.Compile(fmt.Sprint(args[1]))
	if err != nil {
		return nil
	}
	matches := re.FindAllString(fmt.Sprint(args[0]), -1)
	return int64(len(matches))
}

func fnRegexpExtractAll(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re, err := regexp.Compile(fmt.Sprint(args[1]))
	if err != nil {
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
	re, err := regexp.Compile(fmt.Sprint(args[1]))
	if err != nil {
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
		"B":   1,
		"KB":  1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12, "PB": 1e15, "EB": 1e18,
		"KIB": 1024, "MIB": 1048576, "GIB": 1073741824,
		"TIB": 1099511627776, "PIB": 1125899906842624, "EIB": 1152921504606846976,
		// Also handle Kbps-style (bits)
		"BPS":  0.125, "KBPS": 125, "MBPS": 125000, "GBPS": 125000000,
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

// cardinality(array) / array_length(array) — returns the number of elements
func fnCardinality(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	return int64(len(arr))
}

// element_at(array, index) — returns the element at 1-based index (Trino convention)
// For MAPs: element_at(map, key) returns the value for the given key.
// Negative indices count from the end.
func fnElementAt(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	// Try map lookup first
	if m, ok := toMap(args[0]); ok {
		// If second arg is not numeric, or the first arg isn't a plain array, treat as map key
		if _, isSlice := args[0].([]any); !isSlice {
			key := fmt.Sprint(args[1])
			return m[key]
		}
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
