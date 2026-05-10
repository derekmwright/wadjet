package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
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

// BenchmarkHashAggregatePartialSpillDrain measures the spillPartialState path
// — the architectural hotspot at SF100 scale. Builds N unique-key groups via
// Consume, forces a SpillSome, and times the drain (cursor build + sort +
// streaming write). Reported B/op should scale roughly linearly with N (the
// sort-key arena + index dominate) and NOT include per-group partialGroup
// allocations. Allocs/op should be near-constant across N.
func BenchmarkHashAggregatePartialSpillDrain(b *testing.B) {
	const groupsPerBatch = 2048
	const nBatches = 10 // 20480 unique groups total

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	batches := make([]*batch.RecordBatch, nBatches)
	for bi := 0; bi < nBatches; bi++ {
		bb := batch.NewRecordBatch(schema, groupsPerBatch)
		for i := 0; i < groupsPerBatch; i++ {
			bb.Columns[0].SetValue(i, int64(bi*groupsPerBatch+i))
			bb.Columns[1].SetValue(i, float64(i)*1.5)
		}
		batches[bi] = bb
	}

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Use a tracker large enough to never auto-spill mid-Consume; we drive
		// the spill manually via SpillSome to time only the drain.
		tracker := memory.NewTracker("bench", 1<<30)
		sm, err := memory.NewSpillManager(b.TempDir(), tracker)
		if err != nil {
			b.Fatal(err)
		}
		agg := NewHashAggregate(
			[]string{"k"},
			[]AggColumn{
				{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeFloat64},
			},
		)
		agg.Spill = sm
		if err := agg.Init(ctx); err != nil {
			b.Fatal(err)
		}
		for _, bb := range batches {
			if err := agg.Consume(ctx, bb); err != nil {
				b.Fatal(err)
			}
		}
		footprint := agg.SpillFootprint()
		b.StartTimer()
		if _, err := agg.SpillSome(footprint); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		agg.Close()
	}
}

// BenchmarkHashAggregatePartialSpillDrainDualInt covers the dual-int SoA
// path. Two int32 GROUP BY columns trigger the dualIntKeysA/B + chain
// hash-table layout, and the cursor's appendIntModeSortKey takes the
// 2N-typed-write branch. Confirms the typed-key optimization scales as
// well for dual-int as for single-int.
func BenchmarkHashAggregatePartialSpillDrainDualInt(b *testing.B) {
	const rowsPerBatch = 2048
	const nBatches = 10
	const numKeysA = 256 // 256 * 80 = 20480 distinct (a, b) pairs
	const numKeysB = 80

	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt32},
		{Name: "c", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	batches := make([]*batch.RecordBatch, nBatches)
	for bi := 0; bi < nBatches; bi++ {
		bb := batch.NewRecordBatch(schema, rowsPerBatch)
		for i := 0; i < rowsPerBatch; i++ {
			pos := bi*rowsPerBatch + i
			bb.Columns[0].SetValue(i, int32(pos%numKeysA))
			bb.Columns[1].SetValue(i, int32((pos/numKeysA)%numKeysB))
			bb.Columns[2].SetValue(i, float64(i)*1.5)
		}
		batches[bi] = bb
	}

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tracker := memory.NewTracker("bench", 1<<30)
		sm, err := memory.NewSpillManager(b.TempDir(), tracker)
		if err != nil {
			b.Fatal(err)
		}
		agg := NewHashAggregate(
			[]string{"a", "c"},
			[]AggColumn{
				{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeFloat64},
			},
		)
		agg.Spill = sm
		if err := agg.Init(ctx); err != nil {
			b.Fatal(err)
		}
		for _, bb := range batches {
			if err := agg.Consume(ctx, bb); err != nil {
				b.Fatal(err)
			}
		}
		footprint := agg.SpillFootprint()
		b.StartTimer()
		if _, err := agg.SpillSome(footprint); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		agg.Close()
	}
}

// BenchmarkHashAggregatePartialMergeFinalize covers finalizeViaPartialMerge
// — the cursor as a memory partialRunSource feeding the k-way merger
// alongside one spilled run. End-to-end: cursor build + heap merge + Next
// drain. Detects regressions in the streaming-emit path that the spill-only
// bench above cannot.
func BenchmarkHashAggregatePartialMergeFinalize(b *testing.B) {
	const groupsPerBatch = 2048
	const nBatches = 10

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	batches := make([]*batch.RecordBatch, nBatches)
	for bi := 0; bi < nBatches; bi++ {
		bb := batch.NewRecordBatch(schema, groupsPerBatch)
		for i := 0; i < groupsPerBatch; i++ {
			// Overlap keys across batches so finalize merges in-memory and
			// spilled groups via Accumulator.Merge for a fraction of keys.
			bb.Columns[0].SetValue(i, int64((bi*groupsPerBatch/2)+i))
			bb.Columns[1].SetValue(i, float64(i)*1.5)
		}
		batches[bi] = bb
	}

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tracker := memory.NewTracker("bench", 1<<30)
		sm, err := memory.NewSpillManager(b.TempDir(), tracker)
		if err != nil {
			b.Fatal(err)
		}
		agg := NewHashAggregate(
			[]string{"k"},
			[]AggColumn{
				{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeFloat64},
			},
		)
		agg.Spill = sm
		if err := agg.Init(ctx); err != nil {
			b.Fatal(err)
		}
		// Consume half, force a spill, then consume the rest. Finalize will
		// k-way merge the spilled run with the in-memory cursor.
		half := nBatches / 2
		for _, bb := range batches[:half] {
			if err := agg.Consume(ctx, bb); err != nil {
				b.Fatal(err)
			}
		}
		footprint := agg.SpillFootprint()
		if _, err := agg.SpillSome(footprint); err != nil {
			b.Fatal(err)
		}
		for _, bb := range batches[half:] {
			if err := agg.Consume(ctx, bb); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		if err := agg.Finalize(ctx); err != nil {
			b.Fatal(err)
		}
		for {
			out, err := agg.Next(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if out == nil {
				break
			}
		}
		b.StopTimer()
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
