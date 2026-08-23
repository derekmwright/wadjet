package expr

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// TestUncorrelatedSubqueryCacheIsPublishedConcurrently is the #398 regression.
//
// Pipeline.runParallel captures ONE compiled expression tree in every worker
// goroutine, so an uncorrelated subquery's once-only cache is read
// concurrently with the write that fills it. The pre-fix code set
// `cached = true` BEFORE running the subquery, so a worker that arrived in
// that window took the cached branch and read a zero value: a nil threshold
// for ScalarSubquery — every row of that worker's batches then compared
// false and was dropped — an empty probe set for InSubquery, false for
// ExistsSubquery. The query answered a different row count on every run,
// and a torn interface read could deliver a value of the wrong TYPE
// ("cannot store string into FLOAT64 vector").
//
// The test forces that interleaving instead of hoping for it: the runner
// blocks until every concurrent reader has called Eval, so the window the
// defect lives in is the whole test. It fails deterministically without the
// fix, and needs no -race to do it.
func TestUncorrelatedSubqueryCacheIsPublishedConcurrently(t *testing.T) {
	const readers = 8

	// newBlockingRunner reports when the subquery has STARTED, holds it there
	// until release is closed, and counts how many times it ran.
	newBlockingRunner := func(rows []map[string]any) (SubqueryRunner, <-chan struct{}, chan struct{}, *atomic.Int32) {
		started := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		var calls atomic.Int32
		run := func(string) ([]map[string]any, error) {
			calls.Add(1)
			once.Do(func() { close(started) })
			<-release
			return rows, nil
		}
		return run, started, release, &calls
	}

	cases := []struct {
		name string
		rows []map[string]any
		// build wires one expression over the runner and returns the answer
		// every caller — the first one and every concurrent one — must get.
		build func(SubqueryRunner) func(*batch.RecordBatch) any
		want  any
	}{
		{
			name: "ScalarSubquery",
			rows: []map[string]any{{"threshold": 42.5}},
			build: func(r SubqueryRunner) func(*batch.RecordBatch) any {
				e := &ScalarSubquery{SQL: "SELECT AVG(amount) AS threshold FROM t", Runner: r}
				return func(b *batch.RecordBatch) any { return e.Eval(b, 0) }
			},
			want: 42.5,
		},
		{
			name: "InSubquery",
			rows: []map[string]any{{"id": int64(1)}, {"id": int64(3)}},
			build: func(r SubqueryRunner) func(*batch.RecordBatch) any {
				e := &InSubquery{Expr: &ColRef{Name: "id"}, SQL: "SELECT id FROM other", Runner: r}
				// testBatch row 0 is id=1, which the set contains.
				return func(b *batch.RecordBatch) any { return e.EvalBool(b, 0) }
			},
			want: true,
		},
		{
			name: "ExistsSubquery",
			rows: []map[string]any{{"one": int64(1)}},
			build: func(r SubqueryRunner) func(*batch.RecordBatch) any {
				e := &ExistsSubquery{SQL: "SELECT 1 FROM other", Runner: r}
				return func(b *batch.RecordBatch) any { return e.EvalBool(b, 0) }
			},
			want: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			run, started, release, calls := newBlockingRunner(tc.rows)
			eval := tc.build(run)

			got := make([]any, readers+1)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				got[0] = eval(testBatch())
			}()

			<-started // the first evaluation is now inside the runner
			var ready sync.WaitGroup
			ready.Add(readers)
			for i := 1; i <= readers; i++ {
				i := i
				wg.Add(1)
				go func() {
					defer wg.Done()
					ready.Done()
					got[i] = eval(testBatch())
				}()
			}
			ready.Wait() // every reader is inside Eval
			close(release)
			wg.Wait()

			for i, v := range got {
				if v != tc.want {
					t.Errorf("caller %d read %#v (%T) from the shared subquery cache, want %#v — "+
						"the cache was marked filled before the value was written (#398)", i, v, v, tc.want)
				}
			}
			if n := calls.Load(); n != 1 {
				t.Errorf("the subquery ran %d times, want exactly 1 — an uncorrelated subquery is "+
					"evaluated once and shared by every worker", n)
			}
		})
	}
}
