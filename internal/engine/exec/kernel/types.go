// Package kernel provides type-specialized vectorized operations for the query engine.
// Generic functions are monomorphized at compile time and dispatch is resolved once
// at query init time (not per-row), eliminating type-switch overhead from hot loops.
package kernel

import "github.com/derekmwright/caelum/internal/engine/batch"

// Numeric constrains types that support arithmetic operations.
type Numeric interface {
	~int32 | ~int64 | ~float32 | ~float64
}

// Ordered constrains types that support comparison.
type Ordered interface {
	~int32 | ~int64 | ~float32 | ~float64 | ~string
}

// Accumulator holds aggregate state with typed precision.
// Int64 sums stay int64 (no float64 precision loss); float sums use float64.
type Accumulator struct {
	SumI64  int64
	SumF64  float64
	Count   int64
	MinI64  int64
	MaxI64  int64
	MinF64  float64
	MaxF64  float64
	HasMin  bool
	HasMax  bool
	IsFloat bool // true when the source column is a float type
}

// FinalSum returns the accumulated sum as the appropriate type.
func (a *Accumulator) FinalSum() any {
	if a.Count == 0 {
		return nil
	}
	if a.IsFloat {
		return a.SumF64
	}
	return a.SumI64
}

// FinalAvg returns the accumulated average.
func (a *Accumulator) FinalAvg() any {
	if a.Count == 0 {
		return nil
	}
	if a.IsFloat {
		return a.SumF64 / float64(a.Count)
	}
	return float64(a.SumI64) / float64(a.Count)
}

// FinalMin returns the accumulated minimum.
func (a *Accumulator) FinalMin() any {
	if !a.HasMin {
		return nil
	}
	if a.IsFloat {
		return a.MinF64
	}
	return a.MinI64
}

// FinalMax returns the accumulated maximum.
func (a *Accumulator) FinalMax() any {
	if !a.HasMax {
		return nil
	}
	if a.IsFloat {
		return a.MaxF64
	}
	return a.MaxI64
}

// RowAggUpdater updates an accumulator for a single row (used in grouped aggregation).
// The type dispatch is resolved once; the function body has no type switches.
type RowAggUpdater func(acc *Accumulator, vec *batch.Vector, row int)

// BatchAggKernel processes an entire column (or selection) into an accumulator.
// Used for non-grouped aggregation or pre-aggregated groups.
type BatchAggKernel func(acc *Accumulator, vec *batch.Vector, sel []uint16, vecLen int)

// FilterKernel evaluates a column against a pre-resolved constant for all rows,
// returning the indices of matching rows.
type FilterKernel func(vec *batch.Vector, sel []uint16, vecLen int, outSel []uint16) []uint16

// SortCompareKernel compares one row from vector a against one row from vector b.
// Returns -1, 0, or 1. Null handling is included.
type SortCompareKernel func(a *batch.Vector, ai int, b *batch.Vector, bi int) int
