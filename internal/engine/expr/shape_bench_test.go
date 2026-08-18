package expr

import (
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// benchURLBatch builds a full 2048-row batch of ~90-byte URL-shaped values,
// the ClickBench Q28 column profile.
func benchURLBatch(tb testing.TB) *batch.RecordBatch {
	tb.Helper()
	rng := rand.New(rand.NewSource(1))
	const n = batch.DefaultBatchSize
	schema := []parquet.Column{{Name: "url", Type: parquet.TypeString, Nullable: true}}
	rb := batch.NewRecordBatch(schema, n)
	buf := make([]byte, 0, 128)
	for i := 0; i < n; i++ {
		l := 70 + rng.Intn(40)
		buf = append(buf[:0], "http://example.com/"...)
		for j := 0; j < l; j++ {
			buf = append(buf, byte('a'+rng.Intn(26)))
		}
		rb.Columns[0].BytesData.Set(i, buf)
	}
	return rb
}

// BenchmarkShapeLenPerRow measures the per-row path — the one a selection
// vector forces, and therefore the one AVG(LENGTH(url)) takes under a WHERE
// clause. The generic FuncCall copies every value out of the arena
// (ColRef.Eval -> GetString -> string(bc.Value(i))); the offsets node
// subtracts two uint32s.
func BenchmarkShapeLenPerRow(b *testing.B) {
	rb := benchURLBatch(b)
	n := rb.Len
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
	b.Run("offsets", func(b *testing.B) {
		e := &ColShapeLen{
			Col:      &ColRef{Name: "url"},
			Mul:      1,
			Fallback: &FuncCall{Name: "length", Args: []Expr{&ColRef{Name: "url"}}},
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
}

// BenchmarkEmptyStrCompare measures the "url is not the empty string"
// conjunct that appears in 16 of the 43 ClickBench queries.
func BenchmarkEmptyStrCompare(b *testing.B) {
	rb := benchURLBatch(b)
	n := rb.Len
	b.Run("generic-boxed", func(b *testing.B) {
		e := &Cmp{Left: &ColRef{Name: "url"}, Right: &Lit{Val: ""}, Op: CmpNe}
		benchPred(b, e, rb, n)
	})
	b.Run("offsets", func(b *testing.B) {
		e := &ColEmptyStr{
			Col:      &ColRef{Name: "url"},
			Not:      true,
			Fallback: &Cmp{Left: &ColRef{Name: "url"}, Right: &Lit{Val: ""}, Op: CmpNe},
		}
		benchPred(b, e, rb, n)
	})
}

// BenchmarkIsNullCol measures IS NOT NULL over a string column: the generic
// node copies the value out of the arena only to test it against nil.
func BenchmarkIsNullCol(b *testing.B) {
	rb := benchURLBatch(b)
	n := rb.Len
	b.Run("generic-boxed", func(b *testing.B) {
		benchPred(b, &IsNull{Operand: &ColRef{Name: "url"}, Not: true}, rb, n)
	})
	b.Run("nullmask", func(b *testing.B) {
		e := &ColIsNull{
			Col:      &ColRef{Name: "url"},
			Not:      true,
			Fallback: &IsNull{Operand: &ColRef{Name: "url"}, Not: true},
		}
		benchPred(b, e, rb, n)
	})
}

func benchPred(b *testing.B, e BoolExpr, rb *batch.RecordBatch, n int) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	hits := 0
	for i := 0; i < b.N; i++ {
		for r := 0; r < n; r++ {
			if e.EvalBool(rb, r) {
				hits++
			}
		}
	}
	_ = hits
}
