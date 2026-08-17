package exec

import (
	"context"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Benchmarks for G4 (parallel emit of adopted partitions). Both arms run the
// SAME work; only the WADJET_PARALLEL_EMIT toggle differs.
//
//	BenchmarkAggregateEmitPhase     — emission only (state built with the
//	                                  timer stopped): the phase G4 changes.
//	BenchmarkAggregateGroupByTopN   — full pipeline (parallel Consume →
//	                                  Finalize → emit → ORDER BY cnt DESC
//	                                  LIMIT 10), i.e. the wall-clock delta a
//	                                  query actually sees.

const (
	emitBenchUnits  = 8
	emitBenchGroups = 1 << 20 // near-unique int keys, 131072 per unit
)

var emitBenchSchema = []parquet.Column{
	{Name: "k", Type: parquet.TypeInt64},
	{Name: "v", Type: parquet.TypeInt64},
}

func emitBenchAggs() []AggColumn {
	return []AggColumn{
		{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
		{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeFloat64},
	}
}

// concurrentBatchSource is a Source that hands pre-built batches to parallel
// pipeline workers (BatchSource is not concurrency-safe).
type concurrentBatchSource struct {
	mu      sync.Mutex
	batches []*batch.RecordBatch
	idx     int
}

func (s *concurrentBatchSource) Init(context.Context) error {
	s.mu.Lock()
	s.idx = 0
	s.mu.Unlock()
	return nil
}

func (s *concurrentBatchSource) Next(context.Context) (*batch.RecordBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.idx]
	s.idx++
	return b, nil
}

func (s *concurrentBatchSource) Close() error { return nil }

// emitBenchBatches builds `groups` rows of unique int keys as detached
// batches, reusable across benchmark iterations (Consume never mutates them).
func emitBenchBatches(groups int) []*batch.RecordBatch {
	var out []*batch.RecordBatch
	for off := 0; off < groups; off += batch.DefaultBatchSize {
		n := groups - off
		if n > batch.DefaultBatchSize {
			n = batch.DefaultBatchSize
		}
		bb := batch.NewRecordBatch(emitBenchSchema, n)
		for i := 0; i < n; i++ {
			bb.Columns[0].SetValue(i, int64(off+i))
			bb.Columns[1].SetValue(i, int64(i))
		}
		bb.Detach()
		out = append(out, bb)
	}
	return out
}

// buildAdoptedEmitState builds the post-Finalize shape Pipeline.runParallel
// leaves behind: a primary that has adopted units-1 disjoint partitions, each
// owning a contiguous slice of the key space.
func buildAdoptedEmitState(tb testing.TB, units, groups int) *HashAggregate {
	tb.Helper()
	ctx := context.Background()
	prim := NewHashAggregate([]string{"k"}, emitBenchAggs())
	prim.PartitionedDisjoint = true
	if err := prim.Init(ctx); err != nil {
		tb.Fatal(err)
	}
	all := []*HashAggregate{prim}
	for i := 1; i < units; i++ {
		c := prim.CloneSink().(*HashAggregate)
		c.PartitionedDisjoint = true
		if err := c.Init(ctx); err != nil {
			tb.Fatal(err)
		}
		all = append(all, c)
	}
	per := groups / units
	for u := 0; u < units; u++ {
		base := u * per
		for off := 0; off < per; off += batch.DefaultBatchSize {
			n := per - off
			if n > batch.DefaultBatchSize {
				n = batch.DefaultBatchSize
			}
			bb := batch.NewRecordBatch(emitBenchSchema, n)
			for i := 0; i < n; i++ {
				bb.Columns[0].SetValue(i, int64(base+off+i))
				bb.Columns[1].SetValue(i, int64(i))
			}
			if err := all[u].Consume(ctx, bb); err != nil {
				tb.Fatal(err)
			}
		}
	}
	for i := 1; i < units; i++ {
		prim.MergeSink(all[i])
	}
	if err := prim.Finalize(ctx); err != nil {
		tb.Fatal(err)
	}
	return prim
}

// drainTopN streams the aggregate through ORDER BY cnt DESC LIMIT 10.
func drainTopN(tb testing.TB, h *HashAggregate) {
	tb.Helper()
	ctx := context.Background()
	s := NewSort([]SortKey{{Column: "cnt", Order: Descending}})
	s.Limit = 10
	pipe := &Pipeline{Source: &aggDrainSource{agg: h}, Sink: s}
	if err := pipe.Run(ctx); err != nil {
		tb.Fatal(err)
	}
	s.Truncate(10)
	for {
		b, err := s.Next(ctx)
		if err != nil {
			tb.Fatal(err)
		}
		if b == nil {
			break
		}
	}
	s.Close()
}

// drainOnly pulls every emitted batch and discards it — the emission cost
// with no downstream at all, which bounds what parallelising the drain can
// buy before the (serial) consumer becomes the limit.
func drainOnly(tb testing.TB, h *HashAggregate) {
	tb.Helper()
	ctx := context.Background()
	for {
		b, err := h.Next(ctx)
		if err != nil {
			tb.Fatal(err)
		}
		if b == nil {
			return
		}
	}
}

func BenchmarkAggregateEmitPhase(b *testing.B) {
	downstreams := []struct {
		name string
		fn   func(testing.TB, *HashAggregate)
	}{
		{"drain", drainOnly},
		{"topn", drainTopN},
	}
	for _, ds := range downstreams {
		for _, parallel := range []bool{false, true} {
			name := ds.name + "/serial"
			if parallel {
				name = ds.name + "/parallel"
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					h := buildAdoptedEmitState(b, emitBenchUnits, emitBenchGroups)
					prev := parallelEmitToggle.Set(parallel)
					b.StartTimer()

					ds.fn(b, h)

					b.StopTimer()
					h.Close()
					parallelEmitToggle.Set(prev)
					b.StartTimer()
				}
			})
		}
	}
}

func BenchmarkAggregateGroupByTopN(b *testing.B) {
	batches := emitBenchBatches(emitBenchGroups)
	ctx := context.Background()
	for _, parallel := range []bool{false, true} {
		name := "serial-emit"
		if parallel {
			name = "parallel-emit"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				agg := NewHashAggregate([]string{"k"}, emitBenchAggs())
				if err := agg.Init(ctx); err != nil {
					b.Fatal(err)
				}
				src := &concurrentBatchSource{batches: batches}
				prev := parallelEmitToggle.Set(parallel)
				b.StartTimer()

				pipe := &Pipeline{Source: src, Sink: agg, Workers: emitBenchUnits}
				if err := pipe.Run(ctx); err != nil {
					b.Fatal(err)
				}
				drainTopN(b, agg)

				b.StopTimer()
				agg.Close()
				parallelEmitToggle.Set(prev)
				b.StartTimer()
			}
		})
	}
}
