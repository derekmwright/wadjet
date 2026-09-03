package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #531's other half: the charge is RELEASED.
//
// InSubquery.Release had no caller at all, and that was not an oversight to
// tidy up later — wiring the charge without a teardown point converts an
// unaccounted membership map into a permanently-charged one, which on the
// worker path (a SHARED, worker-lifetime tracker) is strictly worse than the
// bug: the bytes go back to the OS when the map is collected and never go back
// to the tracker, so a worker loses budget to queries that finished hours ago.
//
// The embedded API cannot see this. Plan replaces the per-query resources
// holder on entry and wadjet builds a fresh Planner per query, so a leaked
// charge dies with its tracker there. The seam is where it is observable and
// where it is asserted: the option charges, and the holder's release gives it
// back exactly.
func TestASubqueryChargeIsReleasedByItsOwner(t *testing.T) {
	tracker := memory.NewTracker("worker", 8<<20)
	p := &Planner{res: &queryResources{}, SharedTracker: tracker}
	p.subqueryRunner = func(string) ([]map[string]any, error) {
		rows := make([]map[string]any, 5000)
		for i := range rows {
			rows[i] = map[string]any{"id": int64(i)}
		}
		return rows, nil
	}

	opt := p.subqueryBudgetOption()
	if opt == nil {
		t.Fatal("a planner holding a tracker produced no budget option, so nothing would ever be " +
			"charged — that is the #531 state, not a fix")
	}

	q, err := sql.Parse("SELECT 1 FROM t WHERE id IN (SELECT id + 0 FROM t)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if q.SelectInfo == nil || q.SelectInfo.WhereExpr == nil {
		t.Fatal("fixture no longer yields a parsed WHERE expression")
	}
	compiled, err := expr.CompileWithRunner(q.SelectInfo.WhereExpr, p.subqueryRunner, opt)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	probe := batch.FromRows(
		[]parquet.Column{{Name: "id", Type: parquet.TypeInt64}},
		[]map[string]any{{"id": int64(1)}})

	base := tracker.Used()
	// Resolving is what builds the set; the charge lands inside it.
	compiled.Eval(probe, 0)
	charged := tracker.Used() - base
	if charged <= 0 {
		t.Fatalf("the membership set was built and probed and the tracker did not move (%d bytes) — "+
			"the set is ~120 KB of live map and it is the query's memory (#531)", charged)
	}

	res := p.resources()
	if !res.hasSubqueryCharges() {
		t.Fatal("the compile charged the tracker but registered nothing to release it, which is the " +
			"failure mode WithBudget refuses to construct")
	}
	res.releaseSubqueryCharges()
	if got := tracker.Used(); got != base {
		t.Fatalf("after release the tracker holds %d bytes, want the %d it started with — %d bytes "+
			"of a finished query's membership set are still charged", got, base, got-base)
	}
	// Idempotent: a second release must not drive the ledger negative.
	res.releaseSubqueryCharges()
	if got := tracker.Used(); got != base {
		t.Fatalf("a second release moved the tracker to %d, want %d", got, base)
	}
	t.Logf("charged %d bytes, released to %d", charged, tracker.Used())
}
