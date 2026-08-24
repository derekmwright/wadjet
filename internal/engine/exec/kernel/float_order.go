package kernel

import "math"

// The one float ordering, used by every comparator in the tree.
//
// # Why a function and not `if a < b / if a > b`
//
// The inline three-way form every float comparator used to carry reports 0
// for any pair involving NaN, because both `<` and `>` are false against a
// NaN. On a SCALAR column that reads as "NaN ties with everything", which is
// survivable only because a scalar comparator is asked about exactly ONE
// position and so never has two answers to reconcile. Over a VECTOR or an
// ARRAY(FLOAT) it is not an equivalence relation at all: for
//
//	a = [NaN, 0, 2]   b = [0, 1, 2]   c = [1, 0, 1]
//
// position 0 ties a against both b and c, so a < b (position 1) and b < c
// (position 0) and yet a > c (position 2) — `ResolveSortCompare` did not
// return a total order for those types whenever a NaN sat at differing
// positions, which is the property #415 set out to establish and #446
// disproved.
//
// # The order, and who decided it (ADR-0012: PostgreSQL decides semantics)
//
// PostgreSQL's float8_cmp_internal / float4_cmp_internal (utils/adt/float.c)
// give float a TOTAL order by placing NaN ABOVE every other value and equal
// to itself:
//
//	-Inf < ... < -0.0 = +0.0 < ... < +Inf < NaN,  NaN = NaN
//
// so `ORDER BY f` puts NaN last (ASC) and `GROUP BY f` collects the NaNs into
// one group, and the relation is genuinely transitive at every arity. Wadjet
// now applies exactly that, at every level: the scalar FLOAT32/FLOAT64
// columns, a VECTOR's elements, an ARRAY(FLOAT)'s elements, and the boxed
// comparator on the spill/window path (compareAny, exec/sort.go).
//
// It is a total order on VALUES, not on bit patterns: -0.0 and +0.0 compare
// equal (as `==` and PostgreSQL both say), and all NaNs compare equal
// whatever their payload. The key serializers are canonicalized to match, so
// "compares equal" and "serializes alike" stay the same relation — see
// keyFloat32bits / keyFloat64bits and appendKeyValue's float arms
// (exec/sort.go).

// CompareFloat64 orders two float64 values with NaN greatest and NaN == NaN.
func CompareFloat64(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	case a == b:
		// Includes -0.0 vs +0.0, which are the same value.
		return 0
	}
	// Unordered: at least one is NaN. NaN sorts after everything else, and
	// two NaNs are the same value.
	aNaN, bNaN := a != a, b != b
	switch {
	case aNaN && bNaN:
		return 0
	case aNaN:
		return 1
	default:
		return -1
	}
}

// CompareFloat32 orders two float32 values with NaN greatest and NaN == NaN.
//
// Native float32 comparisons, not a widen-to-float64-and-delegate: widening
// every element cost ZZSortFloat32NoNulls +2.03% (benchmarked against
// CompareFloat64(float64(a), float64(b)), the form this replaced) for a rule
// that needs nothing float64 offers — float32's `<`/`>`/`==`/self-inequality
// already carry the same PostgreSQL order this function documents for
// float64.
func CompareFloat32(a, b float32) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	case a == b:
		// Includes -0.0 vs +0.0, which are the same value.
		return 0
	}
	// Unordered: at least one is NaN. NaN sorts after everything else, and
	// two NaNs are the same value.
	aNaN, bNaN := a != a, b != b
	switch {
	case aNaN && bNaN:
		return 0
	case aNaN:
		return 1
	default:
		return -1
	}
}

// --- The same order, as the six SQL predicates ---
//
// A predicate is not free to disagree with the comparator. PostgreSQL's `=`,
// `<`, `>` … over float8/float4 are the operators of the total order above,
// not IEEE754's: `'NaN' = 'NaN'` is TRUE, `'NaN' > 'Infinity'` is TRUE, and
// `-0.0 = 0.0` is TRUE (verified against live postgres:17-alpine). Go's own
// operators are IEEE754, so `WHERE f = f` dropped the NaN rows and
// `WHERE f > 1e300` dropped them too, while `ORDER BY f` and `GROUP BY f`
// had already been taught to place NaN greatest and fold it into one value
// (#446/#459, ADR-0012 item 8).
//
// The forms below are the CHEAP spellings of that rule, not
// `CompareFloat64(a,b) <op> 0`: each is the plain IEEE operator plus, at most,
// one self-inequality test that only runs when the plain operator already said
// no. On data with no NaN — which is all of TPC-H and ClickBench — that extra
// test is a predictable never-taken branch. The self-inequality `a != a` is
// the NaN test; math.IsNaN is the same instruction behind a call, and this is
// the innermost loop of every float filter.
//
//	Eq  a = b   both equal, or both NaN
//	Ne  a <> b  the negation of Eq
//	Lt  a < b   plain, or (b is NaN and a is not: NaN is greatest)
//	Le  a <= b  plain, or b is NaN (everything is <= NaN)
//	Gt  a > b   plain, or (a is NaN and b is not)
//	Ge  a >= b  plain, or a is NaN (NaN is >= everything, itself included)
//
// Against a CONSTANT the cost is not "at most one extra test" but ZERO, and
// resolveFloatConstPred below is where that is spent: with a non-NaN c,
// `a > c || a is NaN` is exactly `!(a <= c)` and `a >= c || a is NaN` is
// exactly `!(a < c)` — one machine comparison each, the same one the IEEE
// kernel issued, with the sense flipped. (Both hold because `a <= c` and
// `a < c` are FALSE for a NaN a, which is the whole reason the naive
// spellings needed a second test.)

// FloatOrdered is the float element type the predicates below are written for.
type FloatOrdered interface{ ~float32 | ~float64 }

// FloatEq reports a = b under PostgreSQL's float order (NaN equals NaN).
func FloatEq[T FloatOrdered](a, b T) bool { return a == b || (a != a && b != b) }

// FloatNe reports a <> b under PostgreSQL's float order.
//
// `a == a || b == b` rather than `!(a != a && b != b)`: the two are the same
// predicate, but this one short-circuits on the FIRST operand for every
// non-NaN row, which is the row the branch predictor sees.
func FloatNe[T FloatOrdered](a, b T) bool { return a != b && (a == a || b == b) }

// FloatLt reports a < b under PostgreSQL's float order (NaN is greatest).
func FloatLt[T FloatOrdered](a, b T) bool { return a < b || (b != b && a == a) }

// FloatLe reports a <= b under PostgreSQL's float order.
func FloatLe[T FloatOrdered](a, b T) bool { return a <= b || b != b }

// FloatGt reports a > b under PostgreSQL's float order.
func FloatGt[T FloatOrdered](a, b T) bool { return a > b || (a != a && b == b) }

// FloatGe reports a >= b under PostgreSQL's float order.
func FloatGe[T FloatOrdered](a, b T) bool { return a >= b || a != a }

// FloatCompareOp applies one of the six predicates to a pair. The row-at-a-
// time paths (exec's ColumnCompare fallback, expr's CmpFloat64) use this so
// they answer what the vectorized kernel answers; the kernels themselves
// resolve the operator ONCE and keep the per-row form above.
func FloatCompareOp[T FloatOrdered](a, b T, op CompareOp) bool {
	switch op {
	case OpEq:
		return FloatEq(a, b)
	case OpNe:
		return FloatNe(a, b)
	case OpLt:
		return FloatLt(a, b)
	case OpLe:
		return FloatLe(a, b)
	case OpGt:
		return FloatGt(a, b)
	case OpGe:
		return FloatGe(a, b)
	}
	return false
}

// resolveFloatConstPred returns the per-row test for `column <op> constant`
// with the CONSTANT's NaN-ness already folded in, so the loop carries no test
// the constant settled.
//
// With a non-NaN constant only `>` and `>=` differ from Go's operators at all
// (a NaN column value is greater than the constant, where IEEE says the
// comparison is simply false), so four of the six kernels are byte-for-byte
// the ones that ran before this rule arrived — and the other two are one
// comparison with its sense flipped, so all six cost what they always did.
func resolveFloatConstPred[T FloatOrdered](op CompareOp, c T) func(T) bool {
	if c != c {
		// NaN constant: every row's answer is decided by whether the row is
		// itself NaN, since NaN is the maximum and equal only to itself.
		switch op {
		case OpEq, OpGe:
			return func(a T) bool { return a != a }
		case OpNe, OpLt:
			return func(a T) bool { return a == a }
		case OpLe:
			return func(T) bool { return true }
		case OpGt:
			return func(T) bool { return false }
		}
		return func(T) bool { return false }
	}
	switch op {
	case OpEq:
		return func(a T) bool { return a == c }
	case OpNe:
		return func(a T) bool { return a != c }
	case OpLt:
		return func(a T) bool { return a < c }
	case OpLe:
		return func(a T) bool { return a <= c }
	case OpGt:
		// !(a <= c), not `a > c || a != a`: identical for every a (a NaN
		// makes `a <= c` false), and one comparison instead of two.
		return func(a T) bool { return !(a <= c) }
	case OpGe:
		return func(a T) bool { return !(a < c) }
	}
	return func(T) bool { return false }
}

// resolveFloatConstPred2 is resolveFloatConstPred's two-argument form for a
// NON-NaN constant: every branch below closes over nothing at all, so the
// constant travels to each call as an ordinary loop-invariant ARGUMENT
// instead of state captured in a heap-allocated closure. compareFilterFloat
// (kernel/compare.go) depends on that distinction, not just on the
// arithmetic: resolveFloatConstPred's one-argument form captures c in the
// closure it returns, and calling THAT per row was FilterColumnCompare's
// float col-const arm going through a genuine indirect call the compiler
// could not reason about, where the equivalent integer path (resolveCompare,
// compare.go) already passes its constant as a plain argument and stays
// close to free. Measured cost of the capturing form: +28% on
// FilterColumnCompare, not the "~" the #459 commit claimed. Call this only
// when c == c; a NaN constant keeps resolveFloatConstPred's one-argument
// form (compareFilterFloat branches on it) — that path is rare enough on the
// query side, and its answer depends only on the ROW's own NaN-ness, not on
// c's value, so there was nothing to gain by changing its shape too.
func resolveFloatConstPred2[T FloatOrdered](op CompareOp) func(a, c T) bool {
	switch op {
	case OpEq:
		return func(a, c T) bool { return a == c }
	case OpNe:
		return func(a, c T) bool { return a != c }
	case OpLt:
		return func(a, c T) bool { return a < c }
	case OpLe:
		return func(a, c T) bool { return a <= c }
	case OpGt:
		// !(a <= c), not `a > c || a != a`: see resolveFloatConstPred —
		// identical for every a, one comparison instead of two.
		return func(a, c T) bool { return !(a <= c) }
	case OpGe:
		return func(a, c T) bool { return !(a < c) }
	}
	return func(a, c T) bool { return false }
}

// resolveFloatColColPred returns the per-row test for `column <op> column`.
func resolveFloatColColPred[T FloatOrdered](op CompareOp) func(a, b T) bool {
	switch op {
	case OpEq:
		return FloatEq[T]
	case OpNe:
		return FloatNe[T]
	case OpLt:
		return FloatLt[T]
	case OpLe:
		return FloatLe[T]
	case OpGt:
		return FloatGt[T]
	case OpGe:
		return FloatGe[T]
	}
	return func(a, b T) bool { return false }
}

// --- Canonical key bits ---

// CanonicalFloat64 / CanonicalFloat32 fold a value onto the one bit pattern
// the order above treats as canonical for it: every NaN payload onto one NaN,
// and -0.0 onto +0.0. CompareFloat64/32 call both of those pairs EQUAL, and
// the standing invariant (ADR-0012 item 8) is that two values the comparator
// calls equal must also SERIALIZE alike — otherwise a GROUP BY splits one
// group in two, a hash join misses a pair the comparator matches, or a shuffle
// routes two equal keys to different partitions and the distributed answer
// stops agreeing with the single-process one.
func CanonicalFloat64(f float64) float64 {
	if f != f {
		return math.NaN()
	}
	if f == 0 {
		// Also folds -0.0: `==` treats -0.0 and +0.0 as equal, and the
		// untyped constant 0 is +0.0.
		return 0
	}
	return f
}

// CanonicalFloat32 is CanonicalFloat64 for float32.
func CanonicalFloat32(f float32) float32 {
	if f != f {
		return float32(math.NaN())
	}
	if f == 0 {
		return 0
	}
	return f
}

// KeyFloat64Bits / KeyFloat32Bits are Float64bits/Float32bits over the
// canonical value — the bits any KEY, hash or partition router should use for
// a float.
func KeyFloat64Bits(f float64) uint64 { return math.Float64bits(CanonicalFloat64(f)) }

// KeyFloat32Bits is KeyFloat64Bits for float32.
func KeyFloat32Bits(f float32) uint32 { return math.Float32bits(CanonicalFloat32(f)) }
