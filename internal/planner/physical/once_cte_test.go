// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// A VOLATILE CTE'S ONE EVALUATION IS A PIPELINE BREAKER THAT SPILLS (#1531,
// GATES.md spill gate). Two million rows read twice through two scalar
// subqueries under a 512 KiB budget: the body is evaluated once into a
// spill-backed collector (engagement: the collector wrote runs to disk), both
// references read that one result, and the two sums are equal.
func TestOnceCTEMaterializationSpillsAndBothReferencesAgree(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: two million rows under a 512 KiB budget")
	}
	cat, ctx := setupCatalog(t)
	p := NewPlanner(cat)
	p.PlanCtx = ctx
	p.MemoryBudget = 512 << 10
	p.SpillDir = t.TempDir()
	const q = `WITH s AS (SELECT g AS id, random() AS r FROM generate_series(1, 2000000) g) ` +
		`SELECT count(*) AS n FROM generate_series(1, 3) t ` +
		`WHERE (SELECT sum(r) FROM s) <> (SELECT sum(r) FROM s) ` +
		`OR (SELECT count(*) FROM s) <> 2000000`
	plan := planWithPlanner(t, p, q)
	defer func() {
		if plan.Cleanup != nil {
			plan.Cleanup()
		}
	}()
	if err := plan.Pipeline.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	sink, ok := plan.Pipeline.Sink.(*exec.CollectSink)
	if !ok {
		t.Fatalf("sink %T", plan.Pipeline.Sink)
	}
	rows := sink.ToRows()
	if len(rows) != 1 || rows[0]["n"] != int64(0) {
		t.Fatalf("got %v, want one row 0 (PostgreSQL 17.11: the two references read one evaluation)", rows)
	}
	p.onceCTEs.mu.Lock()
	defer p.onceCTEs.mu.Unlock()
	if len(p.onceCTEs.entries) != 1 {
		t.Fatalf("%d volatile CTE evaluations recorded, want exactly 1", len(p.onceCTEs.entries))
	}
	for _, e := range p.onceCTEs.entries {
		<-e.done
		if e.err != nil || e.mat == nil || e.mat.coll == nil {
			t.Fatalf("evaluation: err=%v mat=%v", e.err, e.mat)
		}
		if got := e.mat.coll.Rows(); got != 2000000 {
			t.Fatalf("the one evaluation holds %d rows, want 2000000", got)
		}
		if e.mat.coll.SpillRuns() == 0 {
			t.Fatal("the one evaluation never spilled under a 512 KiB budget: this gate compares two in-memory reads")
		}
	}
}
