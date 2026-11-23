// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1706, line 1706k, the sweep's HashAggregate row: the same stale relief call
// (selected while the aggregate was consuming, landing after its Finalize
// unregistered it) against a GROUP BY. As in join_stale_relief_test.go, a
// second grace build's refused arrival drives the registry and the only seam is
// memory.SetReliefCallHook.
//
// PostgreSQL 17.11 (postgres:17-alpine, --locale=C --encoding=UTF8):
//
//	CREATE TABLE t AS SELECT (i % 5000)::bigint AS k, i::bigint AS v FROM generate_series(0, 39999) i;
//	SELECT count(*), sum(k), sum(total) FROM (SELECT k, sum(v) AS total FROM t GROUP BY k) s;
//	→ 5000 | 12497500 | 799980000
var staleAggPG = [3]int64{5000, 12497500, 799980000}

const staleAggRows = 40000

var staleAggSchema = []parquet.Column{
	{Name: "k", Type: parquet.TypeInt64},
	{Name: "v", Type: parquet.TypeInt64},
}

func newStaleAgg() *HashAggregate {
	return NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
	})
}

func staleAggBatches() []*batch.RecordBatch {
	var out []*batch.RecordBatch
	for start := 0; start < staleAggRows; start += batch.DefaultBatchSize {
		end := min(start+batch.DefaultBatchSize, staleAggRows)
		rows := make([]map[string]any, 0, end-start)
		for i := start; i < end; i++ {
			rows = append(rows, map[string]any{"k": int64(i % 5000), "v": int64(i)})
		}
		out = append(out, batch.FromRows(staleAggSchema, rows))
	}
	return out
}

type staleAggTiming string

const (
	aggLandBeforeFinalize staleAggTiming = "before_finalize" // still consuming: must still relieve
	aggLandAfterFinalize  staleAggTiming = "after_finalize"  // Finalize unregistered, nothing emitted
	aggLandMidEmit        staleAggTiming = "mid_emit"        // first output batch already returned
	aggLandAfterClose     staleAggTiming = "after_close"
)

func TestStaleReliefHashAggregate(t *testing.T) {
	for _, timing := range []staleAggTiming{aggLandBeforeFinalize, aggLandAfterFinalize, aggLandMidEmit, aggLandAfterClose} {
		t.Run(string(timing), func(t *testing.T) {
			runStaleAgg(t, timing)
		})
	}
}

func runStaleAgg(t *testing.T, timing staleAggTiming) {
	ctx := context.Background()
	// Calibrate the aggregate's charge after consuming everything.
	var aggUsed int64
	{
		tr := memory.NewTracker("calib", 1<<30)
		sm, err := memory.NewSpillManager(t.TempDir(), tr)
		if err != nil {
			t.Fatal(err)
		}
		h := newStaleAgg()
		h.Spill = sm
		if err := h.Init(ctx); err != nil {
			t.Fatal(err)
		}
		for _, b := range staleAggBatches() {
			if err := h.Consume(ctx, b); err != nil {
				t.Fatal(err)
			}
		}
		aggUsed = tr.Used()
		h.Close()
		sm.Cleanup()
	}
	const reqPad = 3000
	reqCost := hashBuildBytes(batch.FromRows(staleReqSchema, staleReqRows(reqPad)))
	// The aggregate stays under the 40% self-drain threshold; the requester's
	// first arrival does not fit beside it.
	budget := aggUsed * 100 / 38
	if reqCost <= budget-aggUsed {
		t.Fatalf("fixture: requester arrival %d fits beside the aggregate (room %d)", reqCost, budget-aggUsed)
	}

	dir, err := os.MkdirTemp("", "stale-agg")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	tr := memory.NewTracker("stale-agg", budget)
	sm, err := memory.NewSpillManager(dir, tr)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	agg := newStaleAgg()
	agg.Spill = sm
	if err := agg.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for _, b := range staleAggBatches() {
		if err := agg.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(agg.partialSpillFiles); n != 0 {
		t.Fatalf("fixture: the aggregate drained %d runs on its own; the schedule needs it resident", n)
	}

	selected := make(chan struct{})
	release := make(chan struct{})
	var taken atomic.Bool
	victimOp := memory.AccountedOperator(agg)
	restore := memory.SetReliefCallHook(func(s memory.ReliefSite, op memory.AccountedOperator) {
		if s != memory.ReliefSiteRequest || op != victimOp || !taken.CompareAndSwap(false, true) {
			return
		}
		close(selected)
		<-release
	})
	defer restore()

	req := NewHashJoin(InnerJoin, []string{"x"}, []string{"rk"})
	req.Spill, req.MemTracker = sm, tr
	reqDone := make(chan struct{})
	var reqErr error
	var reqPanic any
	go func() {
		defer close(reqDone)
		defer func() { reqPanic = recover() }()
		reqErr = req.Build(ctx, NewSliceSource(staleReqSchema, staleReqRows(reqPad)))
	}()
	select {
	case <-selected:
	case <-reqDone:
		t.Fatalf("schedule not reached: requester finished without selecting the aggregate (err=%v panic=%v used=%d budget=%d)",
			reqErr, reqPanic, tr.Used(), budget)
	case <-time.After(60 * time.Second):
		t.Fatal("schedule not reached")
	}
	runsWritten := 0
	land := func() {
		agg.mu.Lock()
		before := len(agg.partialSpillFiles)
		agg.mu.Unlock()
		close(release)
		<-reqDone
		agg.mu.Lock()
		runsWritten = len(agg.partialSpillFiles) - before
		agg.mu.Unlock()
	}

	if timing == aggLandBeforeFinalize {
		land()
	}
	if err := agg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	if timing == aggLandAfterFinalize {
		land()
	}
	var groups, sumK, sumTotal int64
	first := true
	for {
		out, err := agg.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for _, r := range out.ToRows() {
			groups++
			sumK += r["k"].(int64)
			sumTotal += r["total"].(int64)
		}
		if first && timing == aggLandMidEmit {
			land()
		}
		first = false
	}
	agg.Close()
	if timing == aggLandAfterClose {
		land()
	}
	req.Close()
	got := [3]int64{groups, sumK, sumTotal}
	t.Logf("STALEAGG %s groups=%d Σk=%d Σtotal=%d requester{err=%v panic=%v} runs_written_by_held_call=%d used_after_close=%d",
		timing, groups, sumK, sumTotal, reqErr, reqPanic, runsWritten, tr.Used())
	if reqPanic != nil {
		t.Errorf("the held relief call panicked in the requester: %v", reqPanic)
	}
	if got != staleAggPG {
		t.Errorf("aggregate answer %v, PostgreSQL 17.11: %v", got, staleAggPG)
	}
	if u := tr.Used(); u != 0 {
		t.Errorf("tracker holds %d bytes after both operators closed", u)
	}
}
