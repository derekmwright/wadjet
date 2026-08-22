package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Conversion-cost benchmarks for the two-level group index.
//
// These exist because the SF100 CPU profile of v0.16.0-correctness found
// convertIntHashTableToTwoLevel — not two-level LOOKUPS — to be the single
// largest new CPU consumer in the release: 72.65% of
// intTwoLevelTable.GetOrInsertAt's samples came from the conversion sweep,
// ~79.6 CPU-s per suite run of rehash against ~7.7 CPU-s of two-level probe
// benefit (≈10:1). BenchmarkIntIndexConvertThreshold above sweeps a SINGLE
// long fill, which amortizes the conversion over everything that follows it
// and therefore cannot see that ratio.
//
// The two shapes it misses, both of them Q18's:
//
//   - CARDINALITY SWEEP through the real consume path with the row count
//     held fixed. A near-unique key satisfies any "still filling" test on
//     every batch, so it converts as soon as it crosses the size threshold
//     — at an arbitrary point in the fill, paying a whole-table rehash that
//     replaces nothing.
//   - CAPPED EPOCHS. The exchange's partial aggregation
//     (worker.cappedPartialAgg) finalizes and RESTARTS its HashAggregate
//     every time state passes 128 MB, so a near-unique key converts once per
//     epoch and never gets a long tail to amortize against. This is the
//     shape behind Q18's inner GROUP BY l_orderkey over 150M lineitem rows.
//
// Methodology (this machine is noisy at the ±10% level): run flat/twolevel
// as ONE interleaved window and compare minima — `-benchtime 1x -count 5`.

// benchAggIntBatches builds rows of (k, v, w) where k cycles over `groups`
// distinct scaled int64 keys. groups == rows gives the near-unique shape.
// Cached: -count N re-enters the benchmark function N times and the build is
// far more expensive than the measurement.
var benchAggBatchCache = map[[2]int][]*batch.RecordBatch{}

func benchAggIntBatches(rows, groups int) []*batch.RecordBatch {
	key := [2]int{rows, groups}
	if b, ok := benchAggBatchCache[key]; ok {
		return b
	}
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
		{Name: "w", Type: parquet.TypeInt64},
	}
	const rowsPerBatch = 2048
	n := rows / rowsPerBatch
	out := make([]*batch.RecordBatch, n)
	for bi := 0; bi < n; bi++ {
		rb := batch.NewRecordBatch(schema, rowsPerBatch)
		for i := 0; i < rowsPerBatch; i++ {
			row := bi*rowsPerBatch + i
			rb.Columns[0].SetValue(i, int64(row%groups)*2654435761)
			rb.Columns[1].SetValue(i, int64(row&1))
			rb.Columns[2].SetValue(i, int64(1024+row%512))
		}
		out[bi] = rb
	}
	benchAggBatchCache[key] = out
	return out
}

func benchAggCols() []AggColumn {
	return []AggColumn{
		{Func: AggCount, OutputCol: "c", OutputType: parquet.TypeInt64},
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	}
}

// BenchmarkAggIntCardinalitySweep holds the row count fixed and varies the
// group count across the conversion threshold, flat vs bucketed, through
// HashAggregate.Consume. near-unique is the regressing shape; low and mid
// are the controls that must not move (they never reach the threshold).
func BenchmarkAggIntCardinalitySweep(b *testing.B) {
	const rows = 16 << 20
	ctx := context.Background()
	for _, groups := range []int{100, 64 << 10, 1 << 20, 4 << 20, rows} {
		batches := benchAggIntBatches(rows, groups)
		run := func(b *testing.B, on bool) {
			prev := twoLevelToggle.Set(on)
			defer twoLevelToggle.Set(prev)
			b.ReportAllocs()
			b.ResetTimer()
			conversions := TwoLevelConversions.Load()
			for i := 0; i < b.N; i++ {
				agg := NewHashAggregate([]string{"k"}, benchAggCols())
				if err := agg.Init(ctx); err != nil {
					b.Fatal(err)
				}
				for _, rb := range batches {
					if err := agg.Consume(ctx, rb); err != nil {
						b.Fatal(err)
					}
				}
				if agg.numIntGroups != groups {
					b.Fatalf("groups = %d, want %d", agg.numIntGroups, groups)
				}
				agg.Close()
			}
			// Deterministic companion to the (noisy) wall time: how many
			// whole-table rehashes the conversion policy bought.
			b.ReportMetric(float64(TwoLevelConversions.Load()-conversions)/float64(b.N), "conv/op")
		}
		b.Run(fmt.Sprintf("groups=%d/flat", groups), func(b *testing.B) { run(b, false) })
		b.Run(fmt.Sprintf("groups=%d/twolevel", groups), func(b *testing.B) { run(b, true) })
	}
}

// benchCappedEpochs mirrors worker.cappedPartialAgg: consume until state
// passes capBytes, then finalize + drain + restart. Returns the epoch count
// so a caller can assert the shape actually cycled.
func benchCappedEpochs(tb testing.TB, ctx context.Context, batches []*batch.RecordBatch, capBytes int64) int {
	tb.Helper()
	epochs := 0
	agg := NewHashAggregate([]string{"k"}, benchAggCols())
	if err := agg.Init(ctx); err != nil {
		tb.Fatal(err)
	}
	flush := func() {
		if err := agg.Finalize(ctx); err != nil {
			tb.Fatal(err)
		}
		for {
			out, err := agg.Next(ctx)
			if err != nil {
				tb.Fatal(err)
			}
			if out == nil {
				break
			}
		}
		agg.Close()
		epochs++
	}
	for _, rb := range batches {
		if err := agg.Consume(ctx, rb); err != nil {
			tb.Fatal(err)
		}
		if agg.StateBytes() < capBytes {
			continue
		}
		flush()
		agg = NewHashAggregate([]string{"k"}, benchAggCols())
		if err := agg.Init(ctx); err != nil {
			tb.Fatal(err)
		}
	}
	flush()
	return epochs
}

// BenchmarkAggIntCappedEpochs is the Q18 partial-aggregation shape: a
// near-unique int key streamed through a HashAggregate that is finalized and
// restarted every capBytes of state. Every epoch crosses the conversion
// threshold on its own, so this is where a conversion that replaces nothing
// is paid over and over.
func BenchmarkAggIntCappedEpochs(b *testing.B) {
	const rows = 16 << 20
	ctx := context.Background()
	for _, groups := range []int{rows, 1 << 20} {
		batches := benchAggIntBatches(rows, groups)
		run := func(b *testing.B, on bool) {
			prev := twoLevelToggle.Set(on)
			defer twoLevelToggle.Set(prev)
			b.ReportAllocs()
			b.ResetTimer()
			epochs := 0
			conversions := TwoLevelConversions.Load()
			for i := 0; i < b.N; i++ {
				epochs = benchCappedEpochs(b, ctx, batches, defaultBenchPartialAggCap)
			}
			b.ReportMetric(float64(epochs), "epochs")
			b.ReportMetric(float64(TwoLevelConversions.Load()-conversions)/float64(b.N), "conv/op")
		}
		b.Run(fmt.Sprintf("groups=%d/flat", groups), func(b *testing.B) { run(b, false) })
		b.Run(fmt.Sprintf("groups=%d/twolevel", groups), func(b *testing.B) { run(b, true) })
	}
}

// defaultBenchPartialAggCap mirrors worker.defaultPartialAggCapBytes (128 MB);
// duplicated rather than exported because the worker package imports exec,
// not the other way round.
const defaultBenchPartialAggCap = 128 << 20
