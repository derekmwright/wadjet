package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func refuseBenchBatch(n int) *batch.RecordBatch {
	schema := []parquet.Column{{Name: "d", Type: parquet.TypeDecimal, Precision: 18, Scale: 4}}
	b := batch.NewRecordBatch(schema, n)
	for i := 0; i < n; i++ {
		b.Columns[0].DecimalData.Data[i] = batch.Int128From(int64(i))
	}
	return b
}

// BenchmarkCaseDecimalWhen: simple-CASE arm over a real DECIMAL column.
func BenchmarkCaseDecimalWhen(b *testing.B) {
	bb := refuseBenchBatch(2048)
	e := &Case{
		Operand: &ColRef{Name: "d"},
		Whens: []CaseWhen{
			{Cond: &Lit{Val: float64(100), Text: "100"}, Result: &Lit{Val: int64(1)}},
			{Cond: &Lit{Val: float64(200), Text: "200"}, Result: &Lit{Val: int64(2)}},
		},
		Else: &Lit{Val: int64(0)},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.Eval(bb, row)
		}
	}
}

// BenchmarkIsDistinctFromDecimal: IS DISTINCT FROM over a real DECIMAL column.
func BenchmarkIsDistinctFromDecimal(b *testing.B) {
	bb := refuseBenchBatch(2048)
	e := &IsDistinctFrom{Left: &ColRef{Name: "d"}, Right: &Lit{Val: float64(100), Text: "100"}}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_, _ = e.EvalBoolNull(bb, row)
		}
	}
}

// BenchmarkCaseDecimalWhenExponent: the exponent-form literal shape the
// existing decimal_order_bench_test.go notes is isDecimalText's worst case.
func BenchmarkCaseDecimalWhenExponent(b *testing.B) {
	bb := refuseBenchBatch(2048)
	e := &Case{
		Operand: &ColRef{Name: "d"},
		Whens:   []CaseWhen{{Cond: &Lit{Val: float64(1e30), Text: "1e30"}, Result: &Lit{Val: int64(1)}}},
		Else:    &Lit{Val: int64(0)},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = e.Eval(bb, row)
		}
	}
}
