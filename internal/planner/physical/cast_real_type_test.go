package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestInferCastTypeRealSpellings is the planner half of the pair
// expr.TestCastDestTypeRealSpellings holds: `CAST(x AS REAL)` rounds its value
// to float32 now, so the projection has to allocate a FLOAT32 column for it.
// Declaring FLOAT64 would widen the rounded value straight back and make the
// cast look like the no-op it used to be — and it would deny the result a
// FLOAT32 column's own comparison rules (#631's literal-width rule).
//
// PostgreSQL's bare FLOAT is DOUBLE PRECISION (pg_typeof(CAST(1 AS FLOAT)) is
// `double precision`; float(1..24) is real and float(25..53) is double), so
// REAL and FLOAT4 are the only two spellings that narrow.
func TestInferCastTypeRealSpellings(t *testing.T) {
	for _, spelling := range []string{"REAL", "real", " Real ", "FLOAT4", "float4"} {
		if got := inferCastType(spelling); got != parquet.TypeFloat32 {
			t.Errorf("inferCastType(%q) = %v, want FLOAT32", spelling, got)
		}
	}
	for _, spelling := range []string{
		"FLOAT", "float", "DOUBLE", "DOUBLE PRECISION", "FLOAT8", "FLOAT64",
		"NUMERIC", "DECIMAL",
	} {
		if got := inferCastType(spelling); got != parquet.TypeFloat64 {
			t.Errorf("inferCastType(%q) = %v, want FLOAT64", spelling, got)
		}
	}
}
