package physical

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// Regression coverage for issue #334, where
//
//	SELECT COUNT(*) FROM customer c1
//	WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer)
//
// killed the process with "fatal error: concurrent map writes". Two defects
// stacked: an uncorrelated subquery was planned as CORRELATED (so it ran once
// per outer row), and each of those runs re-planned through the parent's
// planner, racing on its per-build scan-alias counter.
//
// The tests below pin both halves, and the third pins that a genuinely
// correlated subquery still runs per row — the scoping change is the kind
// that over-corrects.

// planSQL builds and plans a query the way the engine does, so the compiled
// filter carries the same outer scope the planner gives it at runtime.
func planWithPlanner(t *testing.T, p *Planner, sql string) *PhysicalPlan {
	t.Helper()
	ctx := context.Background()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	lp, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("logical build: %v", err)
	}
	p.planCtx = ctx
	p.AnnotateScanColumns(ctx, lp)
	lp = logical.Optimize(lp, func(n *logical.Node) { p.AnnotateScanColumns(ctx, n) })
	plan, err := p.Plan(ctx, lp)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return plan
}

// countingPlanner returns a planner whose subquery runner counts executions.
// The count is the plan assertion this file cares about: an UNCORRELATED
// scalar subquery runs exactly once for the whole query, a CORRELATED one runs
// once per row. Asserting the count rather than the answer catches a
// regression that merely makes the query slow again.
func countingPlanner(t *testing.T, cat *catalog.Catalog) (*Planner, *int64) {
	t.Helper()
	p := NewPlanner(cat)
	var calls int64
	inner := p.subqueryRunner
	p.subqueryRunner = func(sql string) ([]map[string]any, error) {
		atomic.AddInt64(&calls, 1)
		return inner(sql)
	}
	return p, &calls
}

// TestUncorrelatedSubqueryPlannedUncorrelated: an unqualified column inside a
// subquery that also resolves in the outer scope must bind to the subquery's
// own FROM, leaving the subquery uncorrelated and executed once.
func TestUncorrelatedSubqueryPlannedUncorrelated(t *testing.T) {
	ctx := context.Background()
	cat := scanCacheFixture(t, 200)

	for _, tc := range []struct {
		name string
		sql  string
	}{
		// The reported shape: outer table aliased, inner column unqualified.
		{"aliased outer", "SELECT id FROM items i1 WHERE i1.id > (SELECT AVG(id) FROM items)"},
		// The shape that escaped by name coincidence even before the fix.
		{"unaliased outer", "SELECT id FROM items WHERE id > (SELECT AVG(id) FROM items)"},
		// The workaround from the issue: qualifying the inner table.
		{"aliased inner", "SELECT id FROM items i1 WHERE i1.id > (SELECT AVG(sq.id) FROM items sq)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, calls := countingPlanner(t, cat)
			plan := planWithPlanner(t, p, tc.sql)
			if err := plan.Pipeline.Run(ctx); err != nil {
				t.Fatalf("run: %v", err)
			}
			plan.Pipeline.Close()

			// One execution for the whole query — not one per row.
			if got := atomic.LoadInt64(calls); got > 1 {
				t.Errorf("subquery executed %d times: planned as correlated, "+
					"but every column in it resolves in its own FROM", got)
			}
		})
	}
}

// TestCorrelatedSubqueryStaysCorrelated is the over-correction guard: a
// subquery that genuinely references the outer row must still be executed per
// row. A non-equi correlation is used deliberately — an equality correlation
// is decorrelated into a join by the logical optimizer and never reaches the
// per-row path this issue is about.
func TestCorrelatedSubqueryStaysCorrelated(t *testing.T) {
	ctx := context.Background()
	cat := scanCacheFixture(t, 200)

	p, calls := countingPlanner(t, cat)
	plan := planWithPlanner(t, p,
		"SELECT id FROM items i1 WHERE i1.id > (SELECT AVG(id2) FROM items WHERE id2 < i1.id)")
	if err := plan.Pipeline.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	plan.Pipeline.Close()

	if got := atomic.LoadInt64(calls); got <= 1 {
		t.Errorf("correlated subquery executed %d times, want one per row: the "+
			"scoping fix over-corrected and dropped a real outer reference", got)
	}
}

// TestSubqueryRunnerConcurrent drives the subquery runner from many goroutines
// at once, which is exactly what a correlated subquery does on a parallel
// pipeline (exec.Pipeline.runParallel fans out to NumCPU workers, each
// evaluating the filter predicate row by row).
//
// Run it under `go test -race` to see the defect directly: before the fix the
// concurrent builds share the parent planner's scanCounter map, which the race
// detector reports as a write/write race in Planner.buildScan — and which the
// runtime turns into an unrecoverable "fatal error: concurrent map writes"
// often enough to take the process down.
func TestSubqueryRunnerConcurrent(t *testing.T) {
	ctx := context.Background()
	cat := scanCacheFixture(t, 200)

	p := NewPlanner(cat)
	p.planCtx = ctx

	const goroutines = 8
	const perGoroutine = 4

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perGoroutine)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < perGoroutine; j++ {
				rows, err := p.subqueryRunner("SELECT AVG(id) FROM items")
				if err != nil {
					errs <- err
					return
				}
				if len(rows) != 1 {
					t.Errorf("subquery returned %d rows, want 1", len(rows))
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent subquery execution: %v", err)
	}
}

// TestForSubqueryIsolatesBuildScratch pins the shape of the fix rather than
// only its symptom: a child planner must start with its own scan-alias
// counter, must not inherit the enclosing fragment's scan-alias injections
// (they describe the outer plan's scans, and binding a subquery to a worker's
// probe-split file slice would answer it from one shard), and must share the
// query-lifetime resources so a query keeps one budget and one spill dir.
func TestForSubqueryIsolatesBuildScratch(t *testing.T) {
	p := NewPlanner(nil)
	p.scanCounter = map[string]int{"items": 3}
	p.ScanFileFilter = map[string][]string{"items": {"a.parquet"}}
	p.StreamingSources = map[string]exec.Source{}
	p.MaterializedInputs = map[string][]*batch.RecordBatch{}
	p.cteCache = map[string]*cteMaterialized{}

	sub := p.forSubquery()

	if sub.scanCounter != nil {
		t.Errorf("child inherited scanCounter %v, want fresh", sub.scanCounter)
	}
	if sub.ScanFileFilter != nil {
		t.Error("child inherited ScanFileFilter: a subquery would read only the outer fragment's file slice")
	}
	if sub.StreamingSources != nil {
		t.Error("child inherited StreamingSources")
	}
	if sub.MaterializedInputs != nil {
		t.Error("child inherited MaterializedInputs")
	}
	if sub.res != p.res {
		t.Error("child does not share the per-query resources: it would create its own spill dir that Cleanup never releases")
	}
	if sub.catalog != p.catalog {
		t.Error("child lost the catalog")
	}
	// The parent's own scratch must be untouched by spawning a child.
	if p.scanCounter["items"] != 3 {
		t.Errorf("parent scanCounter mutated to %v", p.scanCounter)
	}
}
