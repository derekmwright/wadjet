package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func benchBatch(n int) *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
		{Name: "category", Type: parquet.TypeString},
	}
	b := batch.NewRecordBatch(schema, n)
	cats := []string{"A", "B", "C", "D", "E"}
	for i := 0; i < n; i++ {
		b.Columns[0].SetValue(i, int64(i))
		b.Columns[1].SetValue(i, "user_name")
		b.Columns[2].SetValue(i, float64(i)*1.5)
		b.Columns[3].SetValue(i, cats[i%len(cats)])
	}
	return b
}

func BenchmarkFilterColumnCompare(b *testing.B) {
	bb := benchBatch(2048)
	pred := ColumnCompare("amount", OpGt, float64(1000))
	f := NewFilter(pred)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bb.Sel = nil // reset selection
		f.Execute(ctx, bb)
	}
}

func BenchmarkFilterAndPredicate(b *testing.B) {
	bb := benchBatch(2048)
	pred := And(
		ColumnCompare("id", OpGt, int64(100)),
		ColumnCompare("amount", OpLt, float64(2000)),
	)
	f := NewFilter(pred)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bb.Sel = nil
		f.Execute(ctx, bb)
	}
}

func BenchmarkProjectColumnRef(b *testing.B) {
	bb := benchBatch(2048)
	proj := NewProject([]ProjectColumn{
		{Name: "id", Type: parquet.TypeInt64, Expr: ColumnRef("id")},
		{Name: "amount", Type: parquet.TypeFloat64, Expr: ColumnRef("amount")},
	})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		proj.Execute(ctx, bb)
	}
}

func BenchmarkProjectArithExpr(b *testing.B) {
	bb := benchBatch(2048)
	proj := NewProject([]ProjectColumn{
		{Name: "doubled", Type: parquet.TypeFloat64, Expr: ArithExpr(ColumnRef("amount"), Literal(float64(2)), "*")},
	})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		proj.Execute(ctx, bb)
	}
}

func BenchmarkHashAggregate(b *testing.B) {
	bb := benchBatch(2048)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		agg := NewHashAggregate(
			[]string{"category"},
			[]AggColumn{
				{Func: AggSum, InputCol: "amount", OutputCol: "total", OutputType: parquet.TypeFloat64},
				{Func: AggCount, InputCol: "", OutputCol: "cnt", OutputType: parquet.TypeInt64},
			},
		)
		agg.Init(ctx)
		agg.Consume(ctx, bb)
		agg.Finalize(ctx)
		// Drain results
		for {
			r, _ := agg.Next(ctx)
			if r == nil {
				break
			}
		}
		agg.Close()
	}
}

func BenchmarkHashAggregateHighCardinality(b *testing.B) {
	// Each row has a unique group key — worst case for hash agg
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	bb := batch.NewRecordBatch(schema, 2048)
	for i := 0; i < 2048; i++ {
		bb.Columns[0].SetValue(i, int64(i))
		bb.Columns[1].SetValue(i, float64(i)*1.5)
	}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		agg := NewHashAggregate(
			[]string{"id"},
			[]AggColumn{
				{Func: AggSum, InputCol: "amount", OutputCol: "total", OutputType: parquet.TypeFloat64},
			},
		)
		agg.Init(ctx)
		agg.Consume(ctx, bb)
		agg.Finalize(ctx)
		for {
			r, _ := agg.Next(ctx)
			if r == nil {
				break
			}
		}
		agg.Close()
	}
}

func BenchmarkBatchColumnByName(b *testing.B) {
	bb := benchBatch(2048)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 2048; j++ {
			v := bb.ColumnByName("amount")
			_ = v.GetValue(j)
		}
	}
}

func BenchmarkBatchColumnIndex(b *testing.B) {
	bb := benchBatch(2048)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 2048; j++ {
			idx := bb.ColumnIndex("amount")
			_ = bb.Columns[idx].GetValue(j)
		}
	}
}

func BenchmarkKernelFilterColumnCompare(b *testing.B) {
	bb := benchBatch(2048)
	f := NewKernelFilter("amount", OpGt, float64(1000))
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bb.Sel = nil
		f.Execute(ctx, bb)
	}
}

func BenchmarkSortSmall(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bb := benchBatch(256)
		s := NewSort([]SortKey{{Column: "amount", Order: Descending}})
		s.Init(ctx)
		s.Consume(ctx, bb)
		s.Finalize(ctx)
		for {
			r, _ := s.Next(ctx)
			if r == nil {
				break
			}
		}
		s.Close()
	}
}
