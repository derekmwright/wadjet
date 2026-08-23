// Package kernel provides type-specialized vectorized operations for the query engine.
// Generic functions are monomorphized at compile time and dispatch is resolved once
// at query init time (not per-row), eliminating type-switch overhead from hot loops.
package kernel

import "github.com/derekmwright/wadjet/internal/engine/batch"

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
// Decimal sums use Int128 for exact fixed-point arithmetic.
type Accumulator struct {
	SumI64    int64
	SumF64    float64
	SumDec    batch.Int128
	Count     int64
	MinI64    int64
	MaxI64    int64
	MinF64    float64
	MaxF64    float64
	MinDec    batch.Int128
	MaxDec    batch.Int128
	MinStr    string
	MaxStr    string
	HasMin    bool
	HasMax    bool
	IsFloat   bool // true when the source column is a float type (or AVG over int64, which accumulates in float64 to avoid int64 sum wraparound)
	IsDecimal bool // true when the source column is DECIMAL
	IsString  bool // true when the source column is byte-backed (MIN/MAX): STRING, BYTES, IPV6, CIDR, UUID
	IsBool    bool // true when the source column is BOOL (MIN/MAX); the value rides in MinI64/MaxI64 as 0/1
	DecScale  int  // scale for DECIMAL columns
	// StrType is the SOURCE column type behind MinStr/MaxStr. The five
	// byte-backed types share one accumulator slot but not one boxed shape:
	// IPV6 and UUID store raw 16-byte values that only round-trip into their
	// own vector as []byte, while STRING and CIDR store their own text. Boxing
	// them all as a Go string handed the IPV6 output vector 16 arbitrary bytes
	// as an ADDRESS TO PARSE, which fails and writes NULL (#417).
	StrType batch.TypeID
}

// FinalSum returns the accumulated sum as the appropriate type.
func (a *Accumulator) FinalSum() any {
	if a.Count == 0 {
		return nil
	}
	if a.IsDecimal {
		return a.SumDec.ToFloat64(a.DecScale)
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
	if a.IsDecimal {
		return a.SumDec.ToFloat64(a.DecScale) / float64(a.Count)
	}
	if a.IsFloat {
		return a.SumF64 / float64(a.Count)
	}
	return float64(a.SumI64) / float64(a.Count)
}

// minMaxBox re-boxes a byte-backed MIN/MAX into the shape the OUTPUT vector
// accepts. Vector.SetValue reads a string on an IPV6 or UUID column as TEXT
// TO PARSE and a []byte as the raw storage form; the accumulator holds the
// raw form, so those two must come back as []byte. STRING and CIDR store
// their own text (a CIDR vector's bytes ARE "192.168.0.0/24") and come back
// as a string. The []byte conversion also COPIES, which is what keeps the
// answer independent of the arena the value was read from.
func minMaxBox(typ batch.TypeID, s string) any {
	switch typ {
	case batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		return []byte(s)
	}
	return s
}

// FinalMin returns the accumulated minimum.
func (a *Accumulator) FinalMin() any {
	if !a.HasMin {
		return nil
	}
	if a.IsBool {
		return a.MinI64 != 0
	}
	if a.IsString {
		return minMaxBox(a.StrType, a.MinStr)
	}
	if a.IsDecimal {
		return a.MinDec.ToFloat64(a.DecScale)
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
	if a.IsBool {
		return a.MaxI64 != 0
	}
	if a.IsString {
		return minMaxBox(a.StrType, a.MaxStr)
	}
	if a.IsDecimal {
		return a.MaxDec.ToFloat64(a.DecScale)
	}
	if a.IsFloat {
		return a.MaxF64
	}
	return a.MaxI64
}

// Merge combines another accumulator's state into this one.
// Used for parallel aggregation: each worker builds partial state, then merges.
func (a *Accumulator) Merge(other *Accumulator) {
	a.SumI64 += other.SumI64
	a.SumF64 += other.SumF64
	a.Count += other.Count
	if other.IsFloat {
		a.IsFloat = true
	}
	if other.IsDecimal {
		a.IsDecimal = true
		a.SumDec = a.SumDec.Add(other.SumDec)
		a.DecScale = other.DecScale
	}
	if other.IsString {
		a.IsString = true
		a.StrType = other.StrType
	}
	if other.IsBool {
		a.IsBool = true
	}
	if other.HasMin {
		if !a.HasMin {
			a.MinI64 = other.MinI64
			a.MinF64 = other.MinF64
			a.MinDec = other.MinDec
			a.MinStr = other.MinStr
			a.HasMin = true
		} else {
			if other.IsString || a.IsString {
				if other.MinStr < a.MinStr {
					a.MinStr = other.MinStr
				}
			} else if other.IsFloat || a.IsFloat {
				if other.MinF64 < a.MinF64 {
					a.MinF64 = other.MinF64
				}
			} else if other.IsDecimal || a.IsDecimal {
				if other.MinDec.Less(a.MinDec) {
					a.MinDec = other.MinDec
				}
			} else {
				if other.MinI64 < a.MinI64 {
					a.MinI64 = other.MinI64
				}
			}
		}
	}
	if other.HasMax {
		if !a.HasMax {
			a.MaxI64 = other.MaxI64
			a.MaxF64 = other.MaxF64
			a.MaxDec = other.MaxDec
			a.MaxStr = other.MaxStr
			a.HasMax = true
		} else {
			if other.IsString || a.IsString {
				if other.MaxStr > a.MaxStr {
					a.MaxStr = other.MaxStr
				}
			} else if other.IsFloat || a.IsFloat {
				if other.MaxF64 > a.MaxF64 {
					a.MaxF64 = other.MaxF64
				}
			} else if other.IsDecimal || a.IsDecimal {
				if a.MaxDec.Less(other.MaxDec) {
					a.MaxDec = other.MaxDec
				}
			} else {
				if other.MaxI64 > a.MaxI64 {
					a.MaxI64 = other.MaxI64
				}
			}
		}
	}
}

// RowAggUpdater updates an accumulator for a single row (used in grouped aggregation).
// The type dispatch is resolved once; the function body has no type switches.
type RowAggUpdater func(acc *Accumulator, vec *batch.Vector, row int)

// BatchAggKernel processes an entire column (or selection) into an accumulator.
// Used for non-grouped aggregation or pre-aggregated groups.
type BatchAggKernel func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int)

// FilterKernel evaluates a column against a pre-resolved constant for all rows,
// returning the indices of matching rows.
type FilterKernel func(vec *batch.Vector, sel []uint32, vecLen int, outSel []uint32) []uint32

// SortCompareKernel compares one row from vector a against one row from vector b.
// Returns -1, 0, or 1. Null handling is included.
type SortCompareKernel func(a *batch.Vector, ai int, b *batch.Vector, bi int) int
