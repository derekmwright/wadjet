// This file holds expr typed; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- Typed expression interfaces for zero-boxing hot paths ---

// Float64Expr evaluates to float64 without boxing.
type Float64Expr interface {
	EvalFloat64(b *batch.RecordBatch, row int) (float64, bool)
}

// Int64Expr evaluates to int64 without boxing.
type Int64Expr interface {
	EvalInt64(b *batch.RecordBatch, row int) (int64, bool)
}

// VecFloat64Expr evaluates an expression for all rows [0, n) at once,
// writing results to dst. Returns true if any output is null.
// Eliminates per-row function call overhead (~5 calls/row/expression).
type VecFloat64Expr interface {
	EvalFloat64Vec(b *batch.RecordBatch, dst []float64, n int) bool
}

// arithOp is a pre-resolved opcode for arithmetic operations.
// Using an integer switch instead of string comparison eliminates
// per-row string matching in BinOpFloat64/BinOpInt64 hot paths.
type arithOp uint8

const (
	arithAdd arithOp = iota
	arithSub
	arithMul
	arithDiv
	arithMod
	arithUnknown
)

func resolveArithOp(op string) arithOp {
	switch op {
	case "+":
		return arithAdd
	case "-":
		return arithSub
	case "*":
		return arithMul
	case "/":
		return arithDiv
	case "%":
		return arithMod
	default:
		return arithUnknown
	}
}
