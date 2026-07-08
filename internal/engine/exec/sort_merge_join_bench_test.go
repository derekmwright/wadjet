package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// benchCountSink counts joined rows without retaining batches.
type benchCountSink struct{ rows int }

func (s *benchCountSink) Init(context.Context) error { return nil }
func (s *benchCountSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	s.rows += b.ActiveLen()
	return nil
}
func (s *benchCountSink) Finalize(context.Context) error { return nil }
func (s *benchCountSink) Close() error                   { return nil }

// BenchmarkSortMergeJoinVsHash compares the full join lifecycle — build side
// intake, probe intake, output drain — between HashJoin and SortMergeJoin at
// an in-memory scale and at a spilled scale (tight budget: the hash join
// grace-partitions, the SMJ writes sorted runs on both sides). This is the
// design memo's honesty check: SMJ pays a sort on both sides, so it should
// trail hash in-memory and earn its keep only via the memory profile — the
// numbers quantify that trade at micro scale.
func BenchmarkSortMergeJoinVsHash(b *testing.B) {
	const buildN = 100_000
	const probeN = 200_000
	buildSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeInt64},
		{Name: "rv", Type: parquet.TypeString},
	}
	probeSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeInt64},
	}
	buildRows := make([]map[string]any, buildN)
	for i := range buildRows {
		buildRows[i] = map[string]any{"rk": int64(i), "rv": "payload-string-of-modest-size"}
	}
	probeRows := make([]map[string]any, probeN)
	for i := range probeRows {
		probeRows[i] = map[string]any{"k": int64(i % buildN), "amount": int64(i)}
	}

	ctx := context.Background()
	rowsToBatches := func(schema []parquet.Column, rows []map[string]any) []*batch.RecordBatch {
		var out []*batch.RecordBatch
		for i := 0; i < len(rows); i += batch.DefaultBatchSize {
			end := i + batch.DefaultBatchSize
			if end > len(rows) {
				end = len(rows)
			}
			out = append(out, batch.FromRows(schema, rows[i:end]))
		}
		return out
	}

	// A bench-scale run floor: the production 64 MiB floor would keep this
	// data set from ever writing a run.
	oldFloor := minSortRunBytes
	minSortRunBytes = 256 << 10
	b.Cleanup(func() { minSortRunBytes = oldFloor })

	const spillBudget = 1 << 20 // ~1 MiB vs ~5 MiB of data: both operators spill

	runHash := func(b *testing.B, spill bool) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			buildBatches := rowsToBatches(buildSchema, buildRows)
			probeBatches := rowsToBatches(probeSchema, probeRows)
			hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"rk"})
			if spill {
				tracker := memory.NewTracker("bench-hash", spillBudget)
				sm, err := memory.NewSpillManager(b.TempDir(), tracker)
				if err != nil {
					b.Fatal(err)
				}
				hj.MemTracker = tracker
				hj.Spill = sm
			}
			b.StartTimer()

			if err := hj.Build(ctx, NewBatchSource(buildBatches)); err != nil {
				b.Fatal(err)
			}
			sink := &benchCountSink{}
			pipe := &Pipeline{Source: NewBatchSource(probeBatches), Ops: []UnaryOperator{hj.Probe()}, Sink: sink}
			if err := pipe.Run(ctx); err != nil {
				b.Fatal(err)
			}
			if err := pipe.Close(); err != nil {
				b.Fatal(err)
			}
			if sink.rows != probeN {
				b.Fatalf("hash join produced %d rows, want %d", sink.rows, probeN)
			}
		}
	}

	runSMJ := func(b *testing.B, spill bool) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			buildBatches := rowsToBatches(buildSchema, buildRows)
			probeBatches := rowsToBatches(probeSchema, probeRows)
			j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
			if spill {
				tracker := memory.NewTracker("bench-smj", spillBudget)
				sm, err := memory.NewSpillManager(b.TempDir(), tracker)
				if err != nil {
					b.Fatal(err)
				}
				j.Spill = sm
			}
			b.StartTimer()

			if err := j.Init(ctx); err != nil {
				b.Fatal(err)
			}
			if err := j.Build(ctx, NewBatchSource(buildBatches)); err != nil {
				b.Fatal(err)
			}
			for _, pb := range probeBatches {
				if err := j.Consume(ctx, pb); err != nil {
					b.Fatal(err)
				}
			}
			if err := j.Finalize(ctx); err != nil {
				b.Fatal(err)
			}
			rows := 0
			for {
				ob, err := j.Next(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if ob == nil {
					break
				}
				rows += ob.ActiveLen()
			}
			if err := j.Close(); err != nil {
				b.Fatal(err)
			}
			if rows != probeN {
				b.Fatalf("sort-merge join produced %d rows, want %d", rows, probeN)
			}
		}
	}

	b.Run("hash/inmem", func(b *testing.B) { runHash(b, false) })
	b.Run("smj/inmem", func(b *testing.B) { runSMJ(b, false) })
	b.Run("hash/spilled", func(b *testing.B) { runHash(b, true) })
	b.Run("smj/spilled", func(b *testing.B) { runSMJ(b, true) })
}
