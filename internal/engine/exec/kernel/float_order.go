package kernel

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
// appendKeyFloat64 (exec/sort.go).

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
// float32 widens to float64 exactly, so the two share one rule.
func CompareFloat32(a, b float32) int {
	return CompareFloat64(float64(a), float64(b))
}
