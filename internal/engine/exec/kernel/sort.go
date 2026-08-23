package kernel

import "github.com/derekmwright/wadjet/internal/engine/batch"

// ResolveSortCompare returns a comparison function for the given column type.
// The returned function has no type switch — the type is baked into the closure.
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
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		return sortCompareString
	case batch.TypeBool:
		return sortCompareBool
	case batch.TypeDecimal:
		return sortCompareDecimal
	default:
		return func(a *batch.Vector, ai int, b *batch.Vector, bi int) int { return 0 }
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
	av, bv := a.Float64Data[ai], b.Float64Data[bi]
	if av < bv {
		return -1
	}
	if av > bv {
		return 1
	}
	return 0
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
	av, bv := a.Float32Data[ai], b.Float32Data[bi]
	if av < bv {
		return -1
	}
	if av > bv {
		return 1
	}
	return 0
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
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		return sortCompareStringNoNulls
	case batch.TypeDecimal:
		return sortCompareDecimalNoNulls
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
	av, bv := a.Float64Data[ai], b.Float64Data[bi]
	if av < bv {
		return -1
	}
	if av > bv {
		return 1
	}
	return 0
}

func sortCompareFloat32NoNulls(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	av, bv := a.Float32Data[ai], b.Float32Data[bi]
	if av < bv {
		return -1
	}
	if av > bv {
		return 1
	}
	return 0
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
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		return sortCompareStringNullsLast
	case batch.TypeBool:
		return sortCompareBoolNullsLast
	case batch.TypeDecimal:
		return sortCompareDecimalNullsLast
	default:
		return func(a *batch.Vector, ai int, b *batch.Vector, bi int) int { return 0 }
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
	av, bv := a.Float64Data[ai], b.Float64Data[bi]
	if av < bv {
		return -1
	}
	if av > bv {
		return 1
	}
	return 0
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
	av, bv := a.Float32Data[ai], b.Float32Data[bi]
	if av < bv {
		return -1
	}
	if av > bv {
		return 1
	}
	return 0
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
// Equal scales compare the unscaled Int128s directly and are exact. That is
// every sort over one column, every sorted run, and every k-way merge over
// runs of one column. Unequal scales are reachable only where two separately
// declared DECIMAL columns meet — a sort-merge join key, a set operation —
// and are compared as float64, which is exact up to 2^53 unscaled units and
// loses ties above it. Exactness there needs a 128-bit rescale this engine
// has no multiply for; the float path is still a total, value-ordered
// comparison, where the old default was not.
func CompareDecimalAt(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	av, bv := a.DecimalData.Data[ai], b.DecimalData.Data[bi]
	if a.DecimalData.Scale == b.DecimalData.Scale {
		if av.Less(bv) {
			return -1
		}
		if bv.Less(av) {
			return 1
		}
		return 0
	}
	af := av.ToFloat64(a.DecimalData.Scale)
	bf := bv.ToFloat64(b.DecimalData.Scale)
	if af < bf {
		return -1
	}
	if af > bf {
		return 1
	}
	return 0
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
