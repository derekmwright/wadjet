package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// decimalOrderBenchBatch is a 2048-row batch with one DECIMAL(18,4) column,
// "d", holding a plain ascending range of values. Unlike benchBatch's
// "amount" (FLOAT64), a comparison against this column actually reaches
// decimalLitCmp.order() — the notDecimal cache never fires, because the
// column really is TypeDecimal — which is the path FIX-1 targets.
func decimalOrderBenchBatch(n int) *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "d", Type: parquet.TypeDecimal, Precision: 18, Scale: 4},
	}
	b := batch.NewRecordBatch(schema, n)
	for i := 0; i < n; i++ {
		b.Columns[0].DecimalData.Data[i] = batch.Int128From(int64(i))
	}
	return b
}

// BenchmarkDecimalLitCmpOrder measures Cmp.EvalBool's row-at-a-time path
// against a genuine DECIMAL column: decimalLitCmp.order(), which before
// FIX-1 called lit.Numeric() every row — a full text parse
// (isDecimalText -> batch.DecimalTextAt) — instead of the cached bool
// decided once at bind time.
func BenchmarkDecimalLitCmpOrder(b *testing.B) {
	bb := decimalOrderBenchBatch(2048)
	e := NewCmp(&ColRef{Name: "d"}, &Lit{Val: float64(100), Text: "100"}, CmpGt)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.EvalBool(bb, row)
		}
	}
}

// BenchmarkDecimalLitCmpOrderExponent is BenchmarkDecimalLitCmpOrder with an
// exponent-form literal — isDecimalText's digit walk is longest for this
// shape, which is why the reviewer measured the largest per-row multiplier
// (59x) here.
func BenchmarkDecimalLitCmpOrderExponent(b *testing.B) {
	bb := decimalOrderBenchBatch(2048)
	e := NewCmp(&ColRef{Name: "d"}, &Lit{Val: float64(1e30), Text: "1e30"}, CmpLt)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.EvalBool(bb, row)
		}
	}
}

// BenchmarkDecimalInOrder is BenchmarkDecimalLitCmpOrder's IN counterpart,
// built through NewIn so bindDecimalList attaches dec and every list
// member's Numeric() answer is what FIX-1 hoists to bind time.
func BenchmarkDecimalInOrder(b *testing.B) {
	bb := decimalOrderBenchBatch(2048)
	e := NewIn(&ColRef{Name: "d"}, []Expr{
		&Lit{Val: float64(0), Text: "0"},
		&Lit{Val: float64(1), Text: "1"},
		&Lit{Val: float64(5), Text: "5"},
		&Lit{Val: float64(10), Text: "10"},
	}, false)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.EvalBool(bb, row)
		}
	}
}
