package exec

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
		footprint := agg.Inspect().OwnedBytes
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
		footprint := agg.Inspect().OwnedBytes
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
		footprint := agg.Inspect().OwnedBytes
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

// BenchmarkHashAggregateDualIntNearUnique mirrors ClickBench Q33's shape:
// GROUP BY two int64 columns with COUNT(*) + SUM + AVG, at a ~1:1 group:row
// ratio so that essentially every row mints a new group. In that regime the
// per-new-group accumulator growth dominates: the old shape ran
// flatAccumArrays.appendGroup once per new group PER AGGREGATE (11 nil-checks
// + up to 3 appends each, ~33 branch evaluations per row at 3 aggregates),
// where the arrays are append-in-place off-heap slices and the branches buy
// nothing. Batch-oriented growTo replaces all of it with one length extension
// per array per batch.
func BenchmarkHashAggregateDualIntNearUnique(b *testing.B) {
	const nBatches = 16
	const rowsPerBatch = 2048

	schema := []parquet.Column{
		{Name: "wid", Type: parquet.TypeInt64},
		{Name: "ip", Type: parquet.TypeInt64},
		{Name: "refresh", Type: parquet.TypeInt64},
		{Name: "width", Type: parquet.TypeInt64},
	}
	batches := make([]*batch.RecordBatch, nBatches)
	for bi := range batches {
		bb := batch.NewRecordBatch(schema, rowsPerBatch)
		for i := 0; i < rowsPerBatch; i++ {
			row := int64(bi*rowsPerBatch + i)
			bb.Columns[0].SetValue(i, row*2654435761)
			bb.Columns[1].SetValue(i, row)
			bb.Columns[2].SetValue(i, row&1)
			bb.Columns[3].SetValue(i, 1024+row%512)
		}
		batches[bi] = bb
	}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		agg := NewHashAggregate(
			[]string{"wid", "ip"},
			[]AggColumn{
				{Func: AggCount, InputCol: "", OutputCol: "c", OutputType: parquet.TypeInt64},
				{Func: AggSum, InputCol: "refresh", OutputCol: "s", OutputType: parquet.TypeInt64},
				{Func: AggAvg, InputCol: "width", OutputCol: "a", OutputType: parquet.TypeFloat64},
			},
		)
		agg.Init(ctx)
		for _, bb := range batches {
			agg.Consume(ctx, bb)
		}
		agg.Close()
	}
}

// BenchmarkHashAggregateSumAvgSameColumn exercises the count-array sharing
// case: SUM(x) and AVG(x) over the SAME column increment their counts over an
// identical non-null predicate, so one array serves both (16 B/group saved,
// one fewer count store per row).
func BenchmarkHashAggregateSumAvgSameColumn(b *testing.B) {
	const nBatches = 16
	const rowsPerBatch = 2048

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	batches := make([]*batch.RecordBatch, nBatches)
	for bi := range batches {
		bb := batch.NewRecordBatch(schema, rowsPerBatch)
		for i := 0; i < rowsPerBatch; i++ {
			row := bi*rowsPerBatch + i
			bb.Columns[0].SetValue(i, int64(row))
			bb.Columns[1].SetValue(i, float64(row)*1.5)
		}
		batches[bi] = bb
	}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		agg := NewHashAggregate(
			[]string{"k"},
			[]AggColumn{
				{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeFloat64},
				{Func: AggAvg, InputCol: "v", OutputCol: "a", OutputType: parquet.TypeFloat64},
			},
		)
		agg.Init(ctx)
		for _, bb := range batches {
			agg.Consume(ctx, bb)
		}
		agg.Close()
	}
}

// urlKeyBatches builds `numKeys` distinct URL-shaped string keys (~90 bytes,
// the ClickBench hits URL mean) spread over 2048-row batches. Shared by the
// string-key aggregation benchmarks; built outside the timed region so input
// construction stays out of ns/op and B/op.
func urlKeyBatches(numKeys int) []*batch.RecordBatch {
	const batchRows = 2048
	schema := []parquet.Column{
		{Name: "url", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	// 34 + 40 + 4 + 12 = 90 bytes per key.
	prefix := "https://example.com/watch/section/" + strings.Repeat("x", 40) + "/id="
	batches := make([]*batch.RecordBatch, 0, numKeys/batchRows)
	for base := 0; base < numKeys; base += batchRows {
		bb := batch.NewRecordBatch(schema, batchRows)
		for i := 0; i < batchRows; i++ {
			bb.Columns[0].SetValue(i, fmt.Sprintf("%s%012d", prefix, base+i))
			bb.Columns[1].SetValue(i, float64(base+i))
		}
		batches = append(batches, bb)
	}
	return batches
}

// BenchmarkHashAggregateStringKeyConsume drives the single-column string
// GROUP BY consume path at ClickBench Q34/Q35 shape: 1M+ distinct ~90-byte
// URL keys. B/op is the headline metric — it counts key-storage
// amplification (arena growth copies plus any per-group key duplication),
// which was 24.8% (arena memmove/memclr) + 7.3% (per-group string copy) of
// Q34's profile before the chunked arena landed.
//
// The ndvhint sub-benchmarks supply the planner's GROUP-key cardinality
// estimate: they exercise the NDV pre-size branch in resolveIndices, which
// only became reachable once Init stopped hard-coding strGroupIndex. The
// x4 arms replay the input four times, so 75% of the probes are lookups
// into a full-size table — the real Q34 ratio (100M rows / 18M keys),
// where pre-sizing skips ~8 whole-table rehashes instead of paying for
// cold random access into a table the input never fills.
func BenchmarkHashAggregateStringKeyConsume(b *testing.B) {
	const numKeys = 1 << 20 // 1,048,576 distinct keys
	batches := urlKeyBatches(numKeys)
	ctx := context.Background()

	run := func(b *testing.B, ndvHint int64, passes int) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			agg := NewHashAggregate(
				[]string{"url"},
				[]AggColumn{
					{Func: AggSum, InputCol: "amount", OutputCol: "total", OutputType: parquet.TypeFloat64},
					{Func: AggCount, InputCol: "", OutputCol: "cnt", OutputType: parquet.TypeInt64},
				},
			)
			agg.GroupNDVHint = ndvHint
			if err := agg.Init(ctx); err != nil {
				b.Fatal(err)
			}
			for p := 0; p < passes; p++ {
				for _, bb := range batches {
					if err := agg.Consume(ctx, bb); err != nil {
						b.Fatal(err)
					}
				}
			}
			if got := len(agg.serializedKeys); got != numKeys {
				b.Fatalf("groups = %d, want %d", got, numKeys)
			}
			agg.Close()
		}
	}

	b.Run("nohint", func(b *testing.B) { run(b, 0, 1) })
	b.Run("ndvhint", func(b *testing.B) { run(b, numKeys, 1) })
	b.Run("nohint_x4", func(b *testing.B) { run(b, 0, 4) })
	b.Run("ndvhint_x4", func(b *testing.B) { run(b, numKeys, 4) })
}

// BenchmarkStrHashTableInsert isolates the key arena from the aggregate:
// 1M distinct ~90-byte keys inserted into a fresh table. B/op measures the
// arena's write amplification directly (plain-append growth reallocates and
// copies every live byte ~log2(N) times; chunked blocks copy each key once).
func BenchmarkStrHashTableInsert(b *testing.B) {
	const numKeys = 1 << 20
	prefix := "https://example.com/watch/section/" + strings.Repeat("x", 40) + "/id="
	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("%s%012d", prefix, i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ht := newStrHashTable(1024)
		for j, k := range keys {
			ht.GetOrInsert(k, int32(j))
		}
		if ht.Len() != numKeys {
			b.Fatalf("len = %d, want %d", ht.Len(), numKeys)
		}
	}
}
