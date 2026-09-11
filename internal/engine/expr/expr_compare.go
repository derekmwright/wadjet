// This file holds expr compare; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

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
	// dec is the DECIMAL-column-against-numeric-literal binding, set by
	// NewCmp. Nil for every other operand shape — and for a Cmp assembled as
	// a struct literal, which is why the compiler builds them through NewCmp
	// (decimal_literal.go).
	dec *decimalLitCmp
	// decCols is the two-bare-columns binding, for the pair whose boxes carry
	// no way to tell a DECIMAL from a string (#477).
	decCols *decimalColCmp
	// pair is the GENERIC path's declaration-driven binding, for the operand
	// shapes dec and decCols do not cover: a COMPOSITE operand meeting a
	// DECIMAL — `GREATEST(d_2, d_4) = d_4`, the outer comparison of #506's own
	// repro, where both sides arrive as rendered text and compare()'s
	// two-strings fast path answered LEXICOGRAPHICALLY. It also carries the
	// operands' literal source texts, hoisted out of the row loop.
	//
	// It replaces the old notText cache and keeps its property: a pair whose
	// declarations can never select a rule disarms itself on the first batch
	// that settles both operands, so the generic path's per-row cost stays one
	// atomic load. Unlike notText it does not need dec or decCols to have
	// matched first, because it settles from DECLARATIONS rather than from
	// what one row's box happened to be.
	pair *boxedPair
}

// NewCmp builds a comparison, binding the two operand shapes that cannot be
// answered from the boxed values: a DECIMAL column against a numeric literal,
// and two DECIMAL columns against each other.
func NewCmp(left, right Expr, op CmpOp) *Cmp {
	return &Cmp{Left: left, Right: right, Op: op,
		dec: bindDecimalCmp(left, right), decCols: bindDecimalCols(left, right),
		pair: newBoxedPair(left, right)}
}

func (e *Cmp) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *Cmp) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

func (e *Cmp) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	if e.dec != nil {
		if vec := e.dec.vector(b); vec != nil {
			if vec.Nulls.IsNullFast(row) {
				return false, true // a comparison against NULL is UNKNOWN (#370)
			}
			return cmpOrder(e.dec.order(vec, row, 0), e.Op), false
		}
	}
	if e.decCols != nil {
		if lv, rv := e.decCols.vectors(b); lv != nil {
			if lv.Nulls.IsNullFast(row) || rv.Nulls.IsNullFast(row) {
				return false, true // a comparison against NULL is UNKNOWN (#370)
			}
			return cmpOrder(kernel.CompareDecimalAt(lv, row, rv, row), e.Op), false
		}
	}
	lv := e.Left.Eval(b, row)
	rv := e.Right.Eval(b, row)
	if lv == nil || rv == nil {
		return false, true // a comparison against NULL is UNKNOWN (#370)
	}
	// compareNull, not compare: a network comparison has a third answer for a
	// stored value that names no address, and it is the one a NULL row gets
	// (ADR-0012 item 10, #565). Every other pair answers null=false here, so
	// this is the same two values it always was.
	return e.pair.compareNull(b, lv, rv, e.Op)
}

// CmpTemporalLit compares a bare column against a string literal that
// parses as a date/timestamp, without per-row parsing, cache lookups, or
// boxing — the generic path spent 3.2% of SF100 worker CPU inside the
// date-parse memo's sync.Map.Load (interface-key hashing dominated;
// 2026-07-25 re-rank). The literal is parsed once into BOTH temporal
// units at compile time; the unit is chosen from the column's resolved
// type per batch. Every non-fast sub-case (non-temporal column, the
// epoch-zero literal guard) delegates to the generic compare() with the
// original operand order, keeping semantics bit-identical with Cmp.
type CmpTemporalLit struct {
	Col  *ColRef
	Lit  string // original literal text (generic-fallback operand)
	Op   CmpOp
	Flip bool  // literal was the LEFT operand: evaluate as (lit OP col)
	days int64 // literal as epoch days
	ms   int64 // literal as epoch milliseconds
}

func (e *CmpTemporalLit) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *CmpTemporalLit) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

func (e *CmpTemporalLit) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	e.Col.resolve(b)
	var lit int64
	switch e.Col.typ {
	case batch.TypeDate:
		lit = e.days
	case batch.TypeTimestamp:
		lit = e.ms
	default:
		// Non-temporal column: exact generic semantics (string columns
		// compare lexically, numeric columns take the numeric paths).
		return e.genericFallback(b, row)
	}
	v, ok := e.Col.EvalInt64(b, row)
	if !ok {
		return false, true // NULL / unresolved — a comparison against NULL is UNKNOWN
	}
	if lit == 0 && v != 0 {
		// The generic guard (`bi != 0 || ai == 0`) treats an epoch-zero
		// literal against a nonzero column as a parse failure and falls
		// through to stringified comparison. Preserve that bit-exactly.
		return e.genericFallback(b, row)
	}
	a, bv := v, lit
	if e.Flip {
		a, bv = lit, v
	}
	switch e.Op {
	case CmpEq:
		return a == bv, false
	case CmpNe:
		return a != bv, false
	case CmpLt:
		return a < bv, false
	case CmpLe:
		return a <= bv, false
	case CmpGt:
		return a > bv, false
	case CmpGe:
		return a >= bv, false
	}
	return false, false
}

func (e *CmpTemporalLit) genericFallback(b *batch.RecordBatch, row int) (bool, bool) {
	cv := e.Col.Eval(b, row)
	if cv == nil {
		return false, true
	}
	if e.Flip {
		return compare(e.Lit, cv, e.Op), false
	}
	return compare(cv, e.Lit, e.Op), false
}

// CmpNetworkLit compares a column with a pre-parsed network string literal
// without per-row parsing/boxing (#492). Select the encoding by the resolved
// column type: IPv4 big-endian uint32, MAC packed 48 bits, IPv6 raw bytes,
// or kernel.CidrSortKey's PostgreSQL inet structural order.
// Rendered IPv6/CIDR text does not preserve that order.
// Non-network or unmatched literal encodings use generic compare() with the
// original operand order, bit-identical to Cmp's fallback semantics.
// See docs/internals/network-comparison-encoded-order.md for the design.
type CmpNetworkLit struct {
	Col    *ColRef
	Lit    string // original literal text (generic-fallback operand)
	Op     CmpOp
	Flip   bool // literal was the LEFT operand: evaluate as (lit OP col)
	ipv4   int64
	ipv4ok bool
	mac    int64
	macok  bool
	ipv6   string // raw 16 bytes, the column's own TypeIPv6 storage form
	ipv6ok bool
	cidr   string // kernel.CidrSortKey's structural key
	cidrok bool
}

func (e *CmpNetworkLit) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *CmpNetworkLit) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

func (e *CmpNetworkLit) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	e.Col.resolve(b)
	// valueType, not typ: this node is BUILT at compile time, where
	// structField is not yet known (it is resolved on the first batch), so a
	// ROW field path always reaches here. Keying on the container's TypeRow
	// dropped every field path into genericFallback — `rw.cidr > '10.0.0.0/8'`
	// compared the stored TEXT lexically where the same value in a column is
	// compared by kernel.CidrSortKey's inet order, and a malformed literal
	// answered rows instead of 22P02 (#568).
	switch e.Col.valueType() {
	case batch.TypeIPv4:
		if !e.ipv4ok {
			return e.genericFallback(b, row)
		}
		return e.evalInt64(b, row, e.ipv4)
	case batch.TypeMAC:
		if !e.macok {
			return e.genericFallback(b, row)
		}
		return e.evalInt64(b, row, e.mac)
	case batch.TypeIPv6:
		if !e.ipv6ok {
			// The literal is an address of no family this column can hold
			// (a MAC), so there is no comparison to make. The kernel path
			// answers the same query with 22P02 (exec.networkConstError);
			// falling back to a lexical text compare here instead is the
			// two-path divergence #492 exists to close.
			raiseInvalidTextRepresentation("inet", e.Lit)
		}
		return e.evalRawBytes(b, row, e.ipv6)
	case batch.TypeCIDR:
		if !e.cidrok {
			raiseInvalidTextRepresentation("cidr", e.Lit)
		}
		return e.evalCIDR(b, row)
	default:
		// Non-network column: exact generic semantics.
		return e.genericFallback(b, row)
	}
}

// evalInt64 is the IPv4/MAC arm: the column boxes as its raw encoded int64,
// which sorts the same as the address (big-endian uint32 / packed 48 bits).
func (e *CmpNetworkLit) evalInt64(b *batch.RecordBatch, row int, lit int64) (bool, bool) {
	v, ok := e.Col.EvalInt64(b, row)
	if !ok {
		return false, true // NULL / unresolved — a comparison against NULL is UNKNOWN
	}
	a, bv := v, lit
	if e.Flip {
		a, bv = lit, v
	}
	return cmpInt64Op(a, bv, e.Op), false
}

// evalRawBytes is the IPv6 arm: the column stores the address's raw 16
// bytes (batch.Vector's TypeIPv6 storage), which a Go string comparison
// orders byte-for-byte — the address's own big-endian numeric order — unlike
// the RENDERED text ColRef.Eval would hand back instead.
func (e *CmpNetworkLit) evalRawBytes(b *batch.RecordBatch, row int, lit string) (bool, bool) {
	v, ok := e.colRawBytes(b, row)
	if !ok {
		return false, true
	}
	a, bv := v, lit
	if e.Flip {
		a, bv = lit, v
	}
	return cmpStringOp(a, bv, e.Op), false
}

// evalCIDR is the CIDR arm: the column stores plain TEXT (parquet/
// schema.go), so its own value is re-keyed through the identical
// kernel.CidrSortKey (PostgreSQL's inet order) the literal already went
// through at compile time —
// anything else risks the row and the literal disagreeing about what
// "structural order" means, which is the two-implementation defect #492's
// own doc comment warns CidrSortKey's export exists to prevent.
func (e *CmpNetworkLit) evalCIDR(b *batch.RecordBatch, row int) (bool, bool) {
	raw, ok := e.colRawBytes(b, row)
	if !ok {
		return false, true
	}
	key, ok := kernel.CidrSortKey(raw)
	if !ok {
		// The column does NOT enforce the CIDR shape — it is unvalidated
		// text (internal/storage/ingest) — so a stored value can fail to
		// re-key. A value with no place in the order has no defined
		// comparison against one that does, which is UNKNOWN, and that is
		// exactly what kernel.compareFilterCIDR answers for the same row.
		// Falling back to a LEXICAL text comparison here (what this arm did
		// when #492 introduced it) made one malformed row enough to split
		// the two paths apart again.
		return false, true
	}
	a, bv := key, e.cidr
	if e.Flip {
		a, bv = e.cidr, key
	}
	return cmpStringOp(a, bv, e.Op), false
}

// colRawBytes reads the column's raw BytesData at row — the address's own
// storage, never the rendered text ColRef.Eval would produce — and whether
// the row is non-NULL. e.Col must already be resolved.
func (e *CmpNetworkLit) colRawBytes(b *batch.RecordBatch, row int) (string, bool) {
	v, r, ok := e.Col.valueVector(b, row)
	if !ok {
		return "", false
	}
	if v.Nulls.IsNullFast(r) {
		return "", false
	}
	return v.BytesData.UnsafeStringValue(r), true
}

func (e *CmpNetworkLit) genericFallback(b *batch.RecordBatch, row int) (bool, bool) {
	cv := e.Col.Eval(b, row)
	if cv == nil {
		return false, true
	}
	if e.Flip {
		return compare(e.Lit, cv, e.Op), false
	}
	return compare(cv, e.Lit, e.Op), false
}

// cmpStringOp applies a comparison operator to two strings, byte-for-byte —
// CmpNetworkLit's IPv6/CIDR arms feed it keys already re-encoded so that
// byte order IS the intended order (raw address bytes, or CidrSortKey's
// structural key), not the general-purpose collation cmpOrder/compare() use
// for an ordinary STRING column.
func cmpStringOp(a, b string, op CmpOp) bool {
	switch op {
	case CmpEq:
		return a == b
	case CmpNe:
		return a != b
	case CmpLt:
		return a < b
	case CmpLe:
		return a <= b
	case CmpGt:
		return a > b
	case CmpGe:
		return a >= b
	}
	return false
}

// CmpInt64 is a typed comparison that operates on int64 without boxing.
type CmpInt64 struct {
	Left, Right Int64Expr
	Op          CmpOp
}

func (e *CmpInt64) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

// EvalBoolNull: a not-ok typed operand is a NULL (the operands here are
// provably int-typed at compile time), so the comparison is UNKNOWN.
func (e *CmpInt64) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	lv, lok := e.Left.EvalInt64(b, row)
	if !lok {
		return false, true
	}
	rv, rok := e.Right.EvalInt64(b, row)
	if !rok {
		return false, true
	}
	return cmpInt64Op(lv, rv, e.Op), false
}

func (e *CmpInt64) EvalBool(b *batch.RecordBatch, row int) bool {
	lv, lok := e.Left.EvalInt64(b, row)
	if !lok {
		return false
	}
	rv, rok := e.Right.EvalInt64(b, row)
	if !rok {
		return false
	}
	return cmpInt64Op(lv, rv, e.Op)
}

// IsDistinctFrom implements PostgreSQL's NULL-safe (in)equality, IS [NOT]
// DISTINCT FROM (#374). Unlike Cmp, it never answers UNKNOWN: NULL
// participates as a value here rather than propagating, so two NULLs are
// NOT DISTINCT (equal) and a NULL against a non-NULL value IS DISTINCT.
// "NULL IS DISTINCT FROM NULL" is FALSE, never NULL — the one case a
// COALESCE-based workaround gets wrong for a real sentinel value.
type IsDistinctFrom struct {
	Left, Right Expr
	Not         bool // true for IS NOT DISTINCT FROM

	// arms holds the two operand-shaped bindings this node needs: the
	// non-numeric-literal refusal (refuseArm) and the declaration-driven
	// comparison (boxedPair). Armed from the operand SHAPES on first
	// evaluation and reused for every row after.
	//
	// Armed lazily rather than at construction because this node is also
	// built by direct struct literal, and a refusal that only existed on the
	// compiler's path would be a refusal the compiler's tests alone see.
	arms atomic.Pointer[isDistinctArms]
}

// isDistinctArms is IsDistinctFrom's per-node binding, published as one
// immutable value so a row reads one atomic pointer rather than two.
type isDistinctArms struct {
	refuse *refuseArm
	pair   *boxedPair
}

func (e *IsDistinctFrom) armed() *isDistinctArms {
	if a := e.arms.Load(); a != nil {
		return a
	}
	a := &isDistinctArms{refuse: armRefusal(e.Left, e.Right), pair: newBoxedPair(e.Left, e.Right)}
	e.arms.Store(a)
	return a
}

func (e *IsDistinctFrom) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *IsDistinctFrom) EvalBool(b *batch.RecordBatch, row int) bool {
	v, _ := e.EvalBoolNull(b, row)
	return v
}

// EvalBoolNull always reports null=false: DISTINCT FROM is total over NULL
// inputs, which is the entire point of the operator.
func (e *IsDistinctFrom) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	lv := e.Left.Eval(b, row)
	rv := e.Right.Eval(b, row)
	a := e.armed()
	// The refusal runs BEFORE the NULL cases, for the reason evalNullIf runs
	// it first: it is a property of the operand pair's DECLARATIONS, not of
	// this row's values. PostgreSQL coerces the unknown-typed literal at parse
	// analysis, so `NULL IS DISTINCT FROM 'abc'` over a double column is 22P02
	// there — and leaving it in the default arm meant a column whose rows are
	// all NULL answered instead (#646).
	a.refuse.check(b)
	var distinct bool
	switch {
	case lv == nil && rv == nil:
		distinct = false
	case lv == nil || rv == nil:
		distinct = true
	default:
		// A DECIMAL column against a numeric literal is compared at the
		// literal's full precision, not through its float64 box: `d IS
		// DISTINCT FROM <a literal naming d exactly>` answered 200 rows where
		// PostgreSQL answers 199 (#465). A literal that is not a number is a
		// query ERROR against a DECIMAL column, never a value (#463) — this
		// boxed site carried the exact-text comparison but not the refusal
		// (#505): `d IS DISTINCT FROM 'abc'` answered every row instead.
		//
		// TWO DECIMAL COLUMNS are the pair no box can be dispatched on: both
		// arrive as rendered text, indistinguishable from two strings, so
		// `d_2 IS DISTINCT FROM d_4` compared them LEXICOGRAPHICALLY and
		// called two spellings of one number distinct (#506). The pair is
		// bound from the operands' declarations instead.
		distinct = !a.pair.compare(b, lv, rv, CmpEq)
	}
	if e.Not {
		return !distinct, false
	}
	return distinct, false
}

func cmpInt64Op(lv, rv int64, op CmpOp) bool {
	switch op {
	case CmpEq:
		return lv == rv
	case CmpNe:
		return lv != rv
	case CmpLt:
		return lv < rv
	case CmpLe:
		return lv <= rv
	case CmpGt:
		return lv > rv
	case CmpGe:
		return lv >= rv
	default:
		return false
	}
}

// CmpFloat64 is a typed comparison that operates on float64 without boxing.
type CmpFloat64 struct {
	Left, Right Float64Expr
	Op          CmpOp
}

func (e *CmpFloat64) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

// EvalBoolNull: a not-ok typed operand is a NULL (the operands here are
// provably float-typed at compile time), so the comparison is UNKNOWN.
func (e *CmpFloat64) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	lv, lok := e.Left.EvalFloat64(b, row)
	if !lok {
		return false, true
	}
	rv, rok := e.Right.EvalFloat64(b, row)
	if !rok {
		return false, true
	}
	return cmpFloat64Op(lv, rv, e.Op), false
}

func (e *CmpFloat64) EvalBool(b *batch.RecordBatch, row int) bool {
	lv, lok := e.Left.EvalFloat64(b, row)
	if !lok {
		return false
	}
	rv, rok := e.Right.EvalFloat64(b, row)
	if !rok {
		return false
	}
	return cmpFloat64Op(lv, rv, e.Op)
}

// cmpFloat64Op compares two float64 operands in PostgreSQL's float order —
// NaN greatest and equal to itself, -0.0 equal to +0.0 — which is the order
// the vectorized kernel, the group key, ORDER BY and the window peer groups
// all use (ADR-0012 item 8). Go's operators are IEEE754, so spelling them
// directly here made `CASE WHEN f = f` and `HAVING f > 1e300` answer
// differently from the same predicate in a WHERE clause.
func cmpFloat64Op(lv, rv float64, op CmpOp) bool {
	switch op {
	case CmpEq:
		return kernel.FloatEq(lv, rv)
	case CmpNe:
		return kernel.FloatNe(lv, rv)
	case CmpLt:
		return kernel.FloatLt(lv, rv)
	case CmpLe:
		return kernel.FloatLe(lv, rv)
	case CmpGt:
		return kernel.FloatGt(lv, rv)
	case CmpGe:
		return kernel.FloatGe(lv, rv)
	default:
		return false
	}
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

// EvalBoolNull: IS [NOT] NULL never answers UNKNOWN — it is the operator
// SQL provides to ASK about NULL.
func (e *IsNull) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	return e.EvalBool(b, row), false
}

// IsBool is `x IS [NOT] TRUE/FALSE`. Distinct from Cmp because it is a
// NULL-test like IS NULL, not a comparison: NULL IS TRUE answers FALSE and
// NULL IS NOT TRUE answers TRUE, where a comparison against NULL would be
// UNKNOWN (#370 — the Cmp spelling was right only while Cmp itself had no
// UNKNOWN).
type IsBool struct {
	Operand Expr
	Want    bool // TRUE or FALSE spelling
	Not     bool // IS NOT
}

func (e *IsBool) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *IsBool) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := evalBoolNull(e.Operand, b, row)
	match := !null && v == e.Want
	if e.Not {
		return !match
	}
	return match
}

func (e *IsBool) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	return e.EvalBool(b, row), false
}
