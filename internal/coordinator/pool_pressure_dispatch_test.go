package coordinator

import (
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestExecuteStageDAG_NoPoolPressureDeadlock is a regression test for the
// 2026-04-29 SF10 Q02 deadlock. PR #65 added a dispatcher gate that blocked
// every per-stage goroutine while ClusterPoolPressure >= 0.85, polling every
// 500ms. In broadcast_join chains, the build-side hash table is held in the
// shared pool until the *probe-side* stage completes — and the probe was the
// stage being gated. Memory could only be released by running the very stage
// the gate was blocking. The query stalled silently for 17 minutes until the
// 30-minute query_timeout fired. The fix removes the gate; this test
// exercises that path with a high-pressure heartbeat injected before the
// query runs and asserts the query completes well within the gate's old
// 500ms poll cadence.
//
// If a future change re-introduces a blocking pool-pressure gate in
// execute_stage_dag.go, this test will hit the per-test timeout and fail
// loudly rather than reproducing the silent production stall.
func TestExecuteStageDAG_NoPoolPressureDeadlock(t *testing.T) {
	ctx, coord, store := setupWithNATSAndCatalog(t)
	cat := coord.catalog

	// Inject a synthetic heartbeat from a "phantom" worker that is reporting
	// 99% pool pressure. ClusterPoolPressure aggregates across ALL active
	// workers, so even though the real test worker reports 0% pressure, the
	// cluster-wide value is dragged above 0.85 — the threshold the deleted
	// gate used.
	coord.workers.record(distributed.WorkerHeartbeat{
		WorkerID:   "phantom-pressure-reporter",
		ClusterID:  "local",
		PoolUsed:   99 * 1024 * 1024 * 1024,
		PoolBudget: 100 * 1024 * 1024 * 1024,
		Timestamp:  time.Now(),
	})
	if got := coord.workers.ClusterPoolPressure(); got < 0.85 {
		t.Fatalf("phantom heartbeat did not push pressure above gate threshold: got %.3f, want >=0.85", got)
	}

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"id": int64(1), "v": 10.0},
		{"id": int64(2), "v": 20.0},
		{"id": int64(3), "v": 30.0},
	}
	ingestTestData(t, ctx, store, cat, "pp_dispatch", schema, rows)

	// Multi-stage query (aggregate forces an aggregate + final_aggregate
	// pair, exercising the per-stage dispatch loop the deleted gate lived
	// in). Wrap in our own deadline well below the previous gate's 500ms
	// poll cadence — if the gate is back, this fails loudly.
	type result struct {
		res *SQLResult
		err error
	}
	done := make(chan result, 1)
	go func() {
		r, err := coord.ExecuteSQL(ctx, "SELECT SUM(v) AS s FROM pp_dispatch")
		done <- result{r, err}
	}()

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("ExecuteSQL: %v", r.err)
		}
		if r.res == nil || r.res.Error != "" {
			t.Fatalf("query reported error: %+v", r.res)
		}
		if len(mustRows(t, r.res)) != 1 {
			t.Fatalf("expected 1 row, got %d", len(mustRows(t, r.res)))
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("query did not complete within 15s under high pool pressure — pool-pressure dispatch gate may have been reintroduced (see execute_stage_dag.go and feedback memory project_sf10_deploy_2026-04-29.md)")
	}
}
