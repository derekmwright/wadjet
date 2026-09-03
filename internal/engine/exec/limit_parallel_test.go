package exec

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// chunkSource hands out one batch per chunk of rows, so a small table still
// produces several batches for the morsel-parallel workers to pull. SliceSource
// cannot: it emits batch.DefaultBatchSize rows at a time, so a 25-row fixture is
// a single batch and no two workers ever hold one.
type chunkSource struct {
	schema []parquet.Column
	chunks [][]map[string]any
	mu     sync.Mutex
	next   int
}

func (s *chunkSource) Init(_ context.Context) error {
	s.mu.Lock()
	s.next = 0
	s.mu.Unlock()
	return nil
}

func (s *chunkSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	s.mu.Lock()
	if s.next >= len(s.chunks) {
		s.mu.Unlock()
		return nil, nil
	}
	c := s.chunks[s.next]
	s.next++
	s.mu.Unlock()
	return batch.FromRows(s.schema, c), nil
}

func (s *chunkSource) Close() error { return nil }

var limitTestSchema = []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}

// chunkedRows returns len(sizes) chunks of consecutive int64 keys.
func chunkedRows(sizes ...int) [][]map[string]any {
	var out [][]map[string]any
	k := int64(0)
	for _, n := range sizes {
		rows := make([]map[string]any, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, map[string]any{"k": k})
			k++
		}
		out = append(out, rows)
	}
	return out
}

// Limit is SHARED by the morsel-parallel workers: Clone() returns the receiver
// so that OFFSET and LIMIT are counted once over the whole stream rather than
// once per worker (pipeline.go, "Limit.Clone() returns self to share atomic
// counters"). That contract makes every update to the two counters a
// read-modify-write ACROSS goroutines, and a read-modify-write is exactly what
// an atomic Load() followed by an Add() is not.
//
// OFFSET (#567, #765): `seen := l.seen.Load()` … `l.seen.Add(n)` let two workers
// compare their batch against the SAME stale `seen`. Over the two-path fixture's
// `nation` — 25 rows written as three files of 9/9/7 rows, hence three batches —
// two workers reading `seen == 9` each concluded their batch lay entirely inside
// `OFFSET 20` (9+9 and 9+7 are both <= 20) and dropped it whole. Every row was
// skipped, and `SELECT COUNT(*) FROM (SELECT n_nationkey FROM nation OFFSET 20) u`
// answered 0 where the answer is 5 — on the coordinator's default route, with no
// error. The same interleaving in the other direction UNDER-skips: two workers
// both take the partial-skip branch, both trim from the same stale `seen`, and
// both store `l.Offset`.
//
// LIMIT: `remaining := l.Max - l.passed.Load()` has the identical shape and
// over-DELIVERS — two workers each seeing the whole budget pass their whole
// batch, so `LIMIT 3` returns more than three rows.
//
// The barrier is what makes this deterministic rather than a one-in-thousands
// flake: runParallel really does call Execute on this instance from p.Workers
// goroutines, and the barrier only makes those calls land together instead of
// waiting for the scheduler to do it by luck. Reverting either counter to a
// Load/compare/Add pair fails this test on essentially every run.
//
// WHICH rows an unordered OFFSET/LIMIT drops or keeps stays unspecified
// (ADR-0013 classes 1 and 3); HOW MANY is not, and that is all this asserts.
func TestLimitCountsOffsetAndLimitOnceUnderConcurrentExecute(t *testing.T) {
	ctx := context.Background()

	// passedRows runs one Execute per chunk, all released together, and
	// returns how many rows the operator let through in total.
	passedRows := func(t *testing.T, max, offset int64, sizes ...int) int {
		t.Helper()
		chunks := chunkedRows(sizes...)
		l := NewLimit(max, offset)
		if err := l.Init(ctx); err != nil {
			t.Fatalf("init: %v", err)
		}
		var (
			mu     sync.Mutex
			passed int
			wg     sync.WaitGroup
		)
		// A spin barrier, not a channel close: goroutines woken by a closed
		// channel still arrive at Execute microseconds apart, and the window
		// this test has to land in is the handful of instructions between the
		// counter's Load and its Add.
		var ready atomic.Int64
		n := int64(len(chunks))
		for _, c := range chunks {
			wg.Add(1)
			go func(rows []map[string]any) {
				defer wg.Done()
				b := batch.FromRows(limitTestSchema, rows)
				ready.Add(1)
				for ready.Load() < n {
					runtime.Gosched()
				}
				out, err := l.Execute(ctx, b)
				if err != nil {
					mu.Lock()
					t.Errorf("execute: %v", err)
					mu.Unlock()
					return
				}
				if out == nil {
					return
				}
				mu.Lock()
				passed += out.ActiveLen()
				mu.Unlock()
			}(c)
		}
		wg.Wait()
		return passed
	}

	// Every cell is replicated, and the replica counts come from each shape's
	// MEASURED pre-fix failure rate rather than being picked round. The narrow
	// shapes are three batches and three goroutines, so all three have to read
	// the same stale counter — measured at 12 in 20,000 for the `nation` cell,
	// which is why #567 reads as a once-in-a-blue-moon flake instead of the
	// wrong answer it is. The wide shapes are 40 batches and fail within a
	// handful of replicas. A cell that stops failing on revert is a cell that
	// stopped testing anything, so the numbers are stated: reverting either
	// counter to a Load/decide/Add pair fails every cell below, the narrow
	// ones with probability >0.99 at these counts.
	cases := []struct {
		name     string
		max      int64
		offset   int64
		sizes    []int
		want     int
		replicas int
	}{
		// The two-path fixture's nation exactly: 25 rows in three files.
		{"OffsetAloneNation", NoLimit, 20, []int{9, 9, 7}, 5, 20000},
		{"OffsetPastEnd", NoLimit, 25, []int{9, 9, 7}, 0, 20000},
		{"OffsetOne", NoLimit, 1, []int{9, 9, 7}, 24, 20000},
		{"LimitAlone", 3, 0, []int{9, 9, 7}, 3, 2000},
		{"LimitAndOffset", 3, 5, []int{9, 9, 7}, 3, 2000},
		{"LimitPastEnd", 40, 0, []int{9, 9, 7}, 25, 2000},
		// Wider: 40 batches of 3 rows, so many goroutines contend at once.
		{"WideOffset", NoLimit, 100, repeatInt(3, 40), 20, 500},
		{"WideLimitAndOffset", 7, 100, repeatInt(3, 40), 7, 500},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < tc.replicas; i++ {
				if got := passedRows(t, tc.max, tc.offset, tc.sizes...); got != tc.want {
					t.Fatalf("replica %d: LIMIT %d OFFSET %d over %d rows passed %d rows, want %d",
						i, tc.max, tc.offset, sumInts(tc.sizes), got, tc.want)
				}
			}
		})
	}
}

func repeatInt(v, n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func sumInts(v []int) int {
	t := 0
	for _, x := range v {
		t += x
	}
	return t
}

// The same property end to end, through the pipeline that actually shares the
// operator. This is the reachability half of the gate above: it proves the
// shared-instance path is the one a plan takes (a Limit over a scan source with
// Workers > 1), so a future change that stopped sharing the operator would show
// up here as a changed row count rather than as a silently narrower gate.
//
// -short skips it: the loop is deliberately long, because the pipeline's own
// scheduling makes the losing interleaving rare — which is exactly why #567 was
// filed as a flake rather than as the wrong answer it is.
func TestParallelPipelineAppliesOffsetOnceOverTheWholeStream(t *testing.T) {
	if testing.Short() {
		t.Skip("stress loop; -short")
	}
	const iterations = 400
	for i := 0; i < iterations; i++ {
		src := &chunkSource{schema: limitTestSchema, chunks: chunkedRows(9, 9, 7)}
		sink := &CollectSink{SkipFinalizeToRows: true}
		pipe := &Pipeline{
			Source:  src,
			Ops:     []UnaryOperator{NewLimit(NoLimit, 20)},
			Sink:    sink,
			Workers: 8,
		}
		// The gate is only meaningful while this plan shape still takes
		// runParallel — otherwise it would pass by not exercising anything.
		if !pipe.allOpsCloneable() || !SinkSurvivesCloning(pipe.Sink) || pipe.Workers <= 1 {
			t.Fatal("this pipeline no longer runs morsel-parallel: the gate has stopped exercising the shared Limit")
		}
		if err := pipe.Run(context.Background()); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		rows := 0
		for _, b := range sink.Batches() {
			rows += b.ActiveLen()
		}
		if rows != 5 {
			t.Fatalf("iteration %d: OFFSET 20 over 25 rows in 3 batches returned %d rows, want 5", i, rows)
		}
	}
}
