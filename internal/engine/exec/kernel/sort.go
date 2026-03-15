package kernel

import "github.com/derekmwright/caelum/internal/engine/batch"

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
	as := a.BytesData.StringValue(ai)
	bs := b.BytesData.StringValue(bi)
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
