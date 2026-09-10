// This file holds expr vector math; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"math"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- Vectorized math functions ---

func vecAbs(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	// The typed path, when the projection allocated the argument's own domain
	// (#768). It declines whenever the output is a float64 vector, which is
	// every argument type this rule does not cover, and the loop below runs.
	if vecAbsDomain(src, out, n) {
		return
	}
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.Abs(vecReadFloat64(src, i))
	}
}

func vecCeil(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.Ceil(vecReadFloat64(src, i))
	}
}

func vecFloor(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.Floor(vecReadFloat64(src, i))
	}
}

func vecRound(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	precision := 0
	if len(args) >= 2 {
		precision = int(vecReadFloat64(args[1], 0))
	}
	pow := math.Pow(10, float64(precision))
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.Round(vecReadFloat64(src, i)*pow) / pow
	}
}

// vecRoundHalfEven is the vectorized counterpart of fnRoundHalfEven — see
// its comment for the DOUBLE PRECISION half-to-even rule (#381).
func vecRoundHalfEven(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	precision := 0
	if len(args) >= 2 {
		precision = int(vecReadFloat64(args[1], 0))
	}
	pow := math.Pow(10, float64(precision))
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.RoundToEven(vecReadFloat64(src, i)*pow) / pow
	}
}
