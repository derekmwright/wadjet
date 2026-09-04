package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// BenchmarkLengthCharactersVsOffsets is the COST of #856, measured rather than
// asserted.
//
// LENGTH counts characters now, as PostgreSQL and this engine's own
// CHARACTER_LENGTH do, so it can no longer be answered from the offsets array:
// `length` left logical.shapeLenFuncs and expr.shapeLenMul with it, and
// ClickBench Q28's `AVG(LENGTH(URL))` no longer takes the shape-only decode.
//
// The three arms are what the query now pays and what it used to:
//
//	offsets-byte-count   the path LENGTH took before, and the one
//	                     OCTET_LENGTH still takes: two uint32 subtractions
//	rune-count-kernel    the vectorized path LENGTH takes now — it reads the
//	                     bytes, but only to count continuation bytes
//	generic-boxed        the per-row path a selection vector forces
//
// Correctness is not negotiable for this cost (ADR-0012 item 1): a byte count
// under the name PostgreSQL gives a character count is a wrong answer, and the
// optimization exists to skip bytes a query does not read — a character count
// reads them.
func BenchmarkLengthCharactersVsOffsets(b *testing.B) {
	rb := benchURLBatch(b)
	n := rb.Len
	b.Run("offsets-byte-count", func(b *testing.B) {
		e := &ColShapeLen{
			Col:      &ColRef{Name: "url"},
			Mul:      1,
			Fallback: &FuncCall{Name: "octet_length", Args: []Expr{&ColRef{Name: "url"}}},
		}
		b.ReportAllocs()
		b.ResetTimer()
		var sink float64
		for i := 0; i < b.N; i++ {
			for r := 0; r < n; r++ {
				v, _ := e.EvalFloat64(rb, r)
				sink += v
			}
		}
		_ = sink
	})
	b.Run("rune-count-kernel", func(b *testing.B) {
		args := []*batch.Vector{rb.Columns[0]}
		out := batch.NewVector(batch.TypeInt32, n)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vecCharLength(args, out, n)
		}
	})
	b.Run("generic-boxed", func(b *testing.B) {
		e := &numericFuncCall{&FuncCall{Name: "length", Args: []Expr{&ColRef{Name: "url"}}}}
		b.ReportAllocs()
		b.ResetTimer()
		var sink float64
		for i := 0; i < b.N; i++ {
			for r := 0; r < n; r++ {
				v, _ := e.EvalFloat64(rb, r)
				sink += v
			}
		}
		_ = sink
	})
}
