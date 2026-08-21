package tpch

import (
	"context"
	"testing"
)

// TestCorrelatedSelectListSchemaStability pins #360: a correlated scalar
// subquery in a SELECT list intermittently returned rows with NO columns at
// all (map[] per row) on the single-process engine — roughly half of runs,
// with -race silent.
//
// The mechanism turned out to be #378's, not the suspected batch-pool
// re-entrancy: with a top-level ORDER BY the shape runs a parallel Sort, and
// when the primary Sort finished having consumed nothing itself (warmup
// batch claimed by a clone worker — which worker wins is goroutine
// scheduling), MergeSink handed it the clones' batches but left its schema
// nil, so finalize gathered the merged rows into zero output columns. Fixed
// in 386e26d; bisect proof: this shape reproduces 5/21 runs at fcc5ad2 (the
// fix's parent) and 0/30 at the fix.
//
// The test's own shape mirrors the two-path corpus entry
// CorrelatedScalarInSelectList, but LOOPED IN ONE PROCESS: the pre-fix
// failure never hit a process's first execution (cold caches change how
// batches are claimed), so a single-run gate — every existing suite — passes
// with the bug present. Twenty in-process iterations reproduced it reliably.
// The assertion is on the SCHEMA of every row, not the values (the corpus
// entry pins those), so it catches any future empty-rows producer on this
// shape even if the mechanism differs.
func TestCorrelatedSelectListSchemaStability(t *testing.T) {
	ctx := context.Background()
	db := ingestDuckDBFixture(t, ctx, duckdbFixtureRows(t))
	const sql = `SELECT c_custkey, c_nationkey,
		CAST((SELECT AVG(c_acctbal) FROM customer c2
			WHERE c2.c_nationkey < c1.c_nationkey) AS DOUBLE) AS below_avg
		FROM customer c1 WHERE c1.c_custkey <= 25 ORDER BY c_custkey`
	for i := 0; i < 20; i++ {
		rows, _, err := runWadjet(ctx, db, sql)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if len(rows) != 25 {
			t.Fatalf("iteration %d: %d rows, want 25", i, len(rows))
		}
		for r, row := range rows {
			if len(row) == 0 {
				t.Fatalf("iteration %d row %d: NO columns at all (#360's empty-rows symptom)", i, r)
			}
			for _, col := range []string{"c_custkey", "c_nationkey", "below_avg"} {
				if _, ok := lookupCell(row, col); !ok {
					t.Fatalf("iteration %d row %d: column %s missing (row carries %d cells)", i, r, col, len(row))
				}
			}
		}
	}
}
