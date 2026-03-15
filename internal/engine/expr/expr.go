// Package expr provides a typed expression engine for evaluating SQL expressions
// against record batches. It replaces the string-based expression parsing with
// a compiled expression tree built from the vitess-sqlparser AST.
package expr

import (
	"fmt"
	"math"
	"net"
	"strings"
	"time"

	"github.com/derekmwright/caelum/internal/engine/batch"
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
	Name     string
	resolved bool
	idx      int
	typ      batch.TypeID
}

func (e *ColRef) Eval(b *batch.RecordBatch, row int) any {
	if !e.resolved {
		e.idx = b.ColumnIndex(e.Name)
		if e.idx >= 0 {
			e.typ = b.Columns[e.idx].Type
		}
		e.resolved = true
	}
	if e.idx < 0 {
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

// Lit returns a constant value.
type Lit struct {
	Val any
}

func (e *Lit) Eval(_ *batch.RecordBatch, _ int) any {
	return e.Val
}

// --- Arithmetic ---

// BinOp is a binary arithmetic expression.
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
	fn, ok := scalarFuncs[e.Name]
	if !ok {
		return nil
	}
	return fn(e.args)
}

// ScalarFunc is a scalar function implementation.
type ScalarFunc func(args []any) any

var scalarFuncs = map[string]ScalarFunc{
	// String functions
	"upper":   fnUpper,
	"lower":   fnLower,
	"concat":  fnConcat,
	"length":  fnLength,
	"len":     fnLength,
	"substr":  fnSubstr,
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
	"now":        fnNow,
	"year":       fnYear,
	"month":      fnMonth,
	"day":        fnDay,
	"hour":       fnHour,
	"minute":     fnMinute,
	"date_trunc": fnDateTrunc,
	"extract":    fnExtract,
}

// RegisterFunc registers a custom scalar function.
func RegisterFunc(name string, fn ScalarFunc) {
	scalarFuncs[strings.ToLower(name)] = fn
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
	case "float", "double", "decimal", "real":
		return ToFloat64(v)
	case "char", "varchar", "text", "string":
		return fmt.Sprint(v)
	default:
		return v
	}
}
