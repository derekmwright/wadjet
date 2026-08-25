package kernel

import (
	"math/big"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// ResolveSortCompare returns a comparison function for the given column type.
// The returned function has no type switch — the type is baked into the closure.
//
// A nil return means "this resolver cannot order that type". Callers must
// treat nil as a refusal, not as a tie: SortMergeJoin uses these kernels for
// key EQUALITY, so a comparator that reports every pair equal is not a
// degraded sort, it is a cross product presented as an inner join. Until
// #415 the default arm returned exactly such a closure and ARRAY, ROW, MAP
// and VECTOR all fell into it — `ORDER BY arr_col` was a silent no-op and the
// `cmp == nil` guard in sort_merge_join.go was dead code. All 22 types are
// enumerated now; nil is reserved for a type the engine does not have.
func ResolveSortCompare(typ batch.TypeID) SortCompareKernel {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return sortCompareInt64
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return sortCompareInt32
	case batch.TypeFloat64:
		return sortCompareFloat64
	case batch.TypeFloat32:
		return sortCompareFloat32
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		return sortCompareString
	case batch.TypeCIDR:
		// PostgreSQL's inet order, not the stored text's byte order (#520):
		// see sortCompareCIDR.
		return sortCompareCIDR
	case batch.TypeBool:
		return sortCompareBool
	case batch.TypeDecimal:
		return sortCompareDecimal
	case batch.TypeArray, batch.TypeMap, batch.TypeRow, batch.TypeVector:
		return sortCompareContainer
	default:
		// nil, not an always-equal closure: see the resolver contract note
		// above. Every one of the engine's 22 types is named above, so this
		// arm is reachable only for a type the engine does not have.
		return nil
	}
}

func sortCompareInt64(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return -1
	}
	if bN {
		return 1
	}
	av, bv := a.Int64Data[ai], b.Int64Data[bi]
	if av < bv {
		return -1
	}
	if av > bv {
		return 1
	}
	return 0
}

func sortCompareInt32(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return -1
	}
	if bN {
		return 1
	}
	av, bv := a.Int32Data[ai], b.Int32Data[bi]
	if av < bv {
		return -1
	}
	if av > bv {
		return 1
	}
	return 0
}

func sortCompareFloat64(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return -1
	}
	if bN {
		return 1
	}
	return CompareFloat64(a.Float64Data[ai], b.Float64Data[bi])
}

func sortCompareFloat32(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return -1
	}
	if bN {
		return 1
	}
	return CompareFloat32(a.Float32Data[ai], b.Float32Data[bi])
}

func sortCompareString(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return -1
	}
	if bN {
		return 1
	}
	as := a.BytesData.UnsafeStringValue(ai)
	bs := b.BytesData.UnsafeStringValue(bi)
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

// sortCompareCIDR is sortCompareString for a TypeCIDR column, ordering by
// CidrOrderKey (PostgreSQL's inet order) instead of the stored text's byte
// order (#520): text order puts "9.0.0.0/8" above "10.0.0.0/8" and
// "192.168.1.0/24" below "192.168.1.0/32" at the same address, neither of
// which is what `WHERE c_cidr < '10.0.0.0/8'` already answers (#492) —
// ORDER BY, GROUP BY's output order, DISTINCT, MIN/MAX and a sort-merge
// join key all used to disagree with the predicate they sat next to.
func sortCompareCIDR(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return -1
	}
	if bN {
		return 1
	}
	as := CidrOrderKey(a.BytesData.UnsafeStringValue(ai))
	bs := CidrOrderKey(b.BytesData.UnsafeStringValue(bi))
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

func sortCompareBool(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return -1
	}
	if bN {
		return 1
	}
	av, bv := a.BoolData[ai], b.BoolData[bi]
	if av == bv {
		return 0
	}
	if !av {
		return -1
	}
	return 1
}

// --- No-null sort compare variants ---
// These skip the per-comparison null bitmap checks for columns known to have no nulls.

// ResolveSortCompareNoNulls returns a sort compare function that skips null checks.
func ResolveSortCompareNoNulls(typ batch.TypeID) SortCompareKernel {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return sortCompareInt64NoNulls
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return sortCompareInt32NoNulls
	case batch.TypeFloat64:
		return sortCompareFloat64NoNulls
	case batch.TypeFloat32:
		return sortCompareFloat32NoNulls
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		return sortCompareStringNoNulls
	case batch.TypeCIDR:
		return sortCompareCIDRNoNulls
	case batch.TypeBool:
		return sortCompareBoolNoNulls
	case batch.TypeDecimal:
		return sortCompareDecimalNoNulls
	case batch.TypeArray, batch.TypeMap, batch.TypeRow, batch.TypeVector:
		// The COLUMN has no nulls; the container's ELEMENTS still can, and
		// CompareValuesAt checks them per element.
		return CompareValuesAt
	default:
		return ResolveSortCompare(typ)
	}
}

func sortCompareInt64NoNulls(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	av, bv := a.Int64Data[ai], b.Int64Data[bi]
	if av < bv {
		return -1
	}
	if av > bv {
		return 1
	}
	return 0
}

func sortCompareInt32NoNulls(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	av, bv := a.Int32Data[ai], b.Int32Data[bi]
	if av < bv {
		return -1
	}
	if av > bv {
		return 1
	}
	return 0
}

func sortCompareFloat64NoNulls(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	return CompareFloat64(a.Float64Data[ai], b.Float64Data[bi])
}

func sortCompareFloat32NoNulls(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	return CompareFloat32(a.Float32Data[ai], b.Float32Data[bi])
}

func sortCompareStringNoNulls(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	as := a.BytesData.UnsafeStringValue(ai)
	bs := b.BytesData.UnsafeStringValue(bi)
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

// sortCompareCIDRNoNulls is sortCompareCIDR without the null checks; see
// sortCompareCIDR.
func sortCompareCIDRNoNulls(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	as := CidrOrderKey(a.BytesData.UnsafeStringValue(ai))
	bs := CidrOrderKey(b.BytesData.UnsafeStringValue(bi))
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

func sortCompareBoolNoNulls(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	av, bv := a.BoolData[ai], b.BoolData[bi]
	if av == bv {
		return 0
	}
	if !av {
		return -1
	}
	return 1
}

// --- Nulls-last sort compare variants ---
// These use NULLS LAST ordering: null > non-null, so nulls sort after all non-null values.
// This avoids a redundant post-comparison null check in the sort loop.

// ResolveSortCompareNullsLast returns a sort compare function with NULLS LAST ordering.
func ResolveSortCompareNullsLast(typ batch.TypeID) SortCompareKernel {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return sortCompareInt64NullsLast
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return sortCompareInt32NullsLast
	case batch.TypeFloat64:
		return sortCompareFloat64NullsLast
	case batch.TypeFloat32:
		return sortCompareFloat32NullsLast
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		return sortCompareStringNullsLast
	case batch.TypeCIDR:
		return sortCompareCIDRNullsLast
	case batch.TypeBool:
		return sortCompareBoolNullsLast
	case batch.TypeDecimal:
		return sortCompareDecimalNullsLast
	case batch.TypeArray, batch.TypeMap, batch.TypeRow, batch.TypeVector:
		return sortCompareContainerNullsLast
	default:
		return nil
	}
}

func sortCompareInt64NullsLast(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return 1
	}
	if bN {
		return -1
	}
	av, bv := a.Int64Data[ai], b.Int64Data[bi]
	if av < bv {
		return -1
	}
	if av > bv {
		return 1
	}
	return 0
}

func sortCompareInt32NullsLast(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return 1
	}
	if bN {
		return -1
	}
	av, bv := a.Int32Data[ai], b.Int32Data[bi]
	if av < bv {
		return -1
	}
	if av > bv {
		return 1
	}
	return 0
}

func sortCompareFloat64NullsLast(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return 1
	}
	if bN {
		return -1
	}
	return CompareFloat64(a.Float64Data[ai], b.Float64Data[bi])
}

func sortCompareFloat32NullsLast(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return 1
	}
	if bN {
		return -1
	}
	return CompareFloat32(a.Float32Data[ai], b.Float32Data[bi])
}

func sortCompareStringNullsLast(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return 1
	}
	if bN {
		return -1
	}
	as := a.BytesData.UnsafeStringValue(ai)
	bs := b.BytesData.UnsafeStringValue(bi)
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

// sortCompareCIDRNullsLast is sortCompareCIDR with NULLS LAST ordering; see
// sortCompareCIDR.
func sortCompareCIDRNullsLast(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return 1
	}
	if bN {
		return -1
	}
	as := CidrOrderKey(a.BytesData.UnsafeStringValue(ai))
	bs := CidrOrderKey(b.BytesData.UnsafeStringValue(bi))
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

func sortCompareBoolNullsLast(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return 1
	}
	if bN {
		return -1
	}
	av, bv := a.BoolData[ai], b.BoolData[bi]
	if av == bv {
		return 0
	}
	if !av {
		return -1
	}
	return 1
}

// --- DECIMAL sort compare ---

// CompareDecimalAt orders two DECIMAL values by NUMERIC value, which is what
// PostgreSQL's `numeric` ordering means and what every other comparator in
// this file already does for its type. Before this arm existed, DECIMAL fell
// through the three resolvers' defaults to a comparator that reports every
// row equal, so `ORDER BY dec_col` was a stable no-op that returned input
// order, and a sort-merge join on a DECIMAL key matched every row against
// every row. The other path in the tree — compareAny over Vector.GetValue —
// compares the FORMATTED string instead, where "10.001" sorts before
// "2.0002". Same query, three different sequences depending on which path
// answered (#394).
//
// The comparison is EXACT at every scale. Equal scales compare the unscaled
// Int128s directly — that is every sort over one column, every sorted run and
// every k-way merge over runs. Unequal scales, reachable where two separately
// declared DECIMAL columns meet, rescale the smaller-scale operand by
// 10^(delta) and compare the unscaled integers; if that product overflows
// Int128 the two are compared as big.Int rather than approximated.
//
// Exactness is not a nicety here: SortMergeJoin uses this comparator for key
// EQUALITY (sort_merge_join.go), so an approximate answer is a spurious JOIN
// MATCH. The float64 rescale this replaced held to 2^53 unscaled units and
// then started reporting 9007199254740993 and 9007199254740992.0 — which
// differ by one unscaled unit at the common scale — as the same key.
func CompareDecimalAt(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	return CompareDecimalValues(a.DecimalData.Data[ai], a.DecimalData.Scale,
		b.DecimalData.Data[bi], b.DecimalData.Scale)
}

// CompareDecimalValues is CompareDecimalAt on values already read out of their
// columns — the form the col-col FILTER kernel needs, which reads its two
// slices once per batch rather than per row. One function so the sort
// comparator, the sort-merge join key and the filter cannot drift apart.
func CompareDecimalValues(av batch.Int128, as int, bv batch.Int128, bs int) int {
	if as == bs {
		return compareInt128(av, bv)
	}
	// Rescale the SMALLER scale up: scaling down would discard digits and
	// turn a real difference into a tie.
	if as < bs {
		if scaled, ok := av.MulPow10(bs - as); ok {
			return compareInt128(scaled, bv)
		}
	} else {
		if scaled, ok := bv.MulPow10(as - bs); ok {
			return compareInt128(av, scaled)
		}
	}
	return compareDecimalBig(av, as, bv, bs)
}

func compareInt128(av, bv batch.Int128) int {
	if av.Less(bv) {
		return -1
	}
	if bv.Less(av) {
		return 1
	}
	return 0
}

// compareDecimalBig is the exact fallback for the rare pair whose rescale
// does not fit in Int128 — a near-full-width unscaled value meeting a much
// larger scale. It allocates, and it is reached only there.
func compareDecimalBig(av batch.Int128, as int, bv batch.Int128, bs int) int {
	x, y := av.BigInt(), bv.BigInt()
	if as < bs {
		x.Mul(x, big.NewInt(0).Exp(big.NewInt(10), big.NewInt(int64(bs-as)), nil))
	} else if bs < as {
		y.Mul(y, big.NewInt(0).Exp(big.NewInt(10), big.NewInt(int64(as-bs)), nil))
	}
	return x.Cmp(y)
}

func sortCompareDecimal(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return -1
	}
	if bN {
		return 1
	}
	return CompareDecimalAt(a, ai, b, bi)
}

func sortCompareDecimalNullsLast(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return 1
	}
	if bN {
		return -1
	}
	return CompareDecimalAt(a, ai, b, bi)
}

func sortCompareDecimalNoNulls(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	return CompareDecimalAt(a, ai, b, bi)
}
