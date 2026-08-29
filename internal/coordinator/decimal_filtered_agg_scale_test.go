package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestFilteredDecimalAggregateScaleIsWrongOnTheDAG PINS an open, PRE-EXISTING
// defect: an UNGROUPED aggregate over a DECIMAL column, under a filter that
// actually excludes rows, comes back 10^scale too large on the stage DAG.
//
//	SELECT SUM(d92) FROM decpair WHERE id < 5
//	  PostgreSQL 38.24 · single-process 38.24 · stage DAG 3824.00
//
// It is the worst decimal defect in the tree and it predates every commit of
// #555's arc — verified identical at 9a645dc0, the base this work branched
// from. It is pinned rather than fixed because the mechanism was not found in
// the time available, and a pin that starts AGREEING fails, so the fix cannot
// land silently (ADR-0013's discipline).
//
// What the investigation established, so the next pass does not repeat it:
//
//   - The trigger is SELECTIVITY, not the filter. `WHERE id < 100` and
//     `WHERE a > -1000` match every row and are CORRECT; `WHERE id < 5`
//     matches four of nine and is wrong. A derived table around the filter
//     changes nothing.
//   - GROUP BY is CORRECT. Only the ungrouped shape is affected.
//   - Every aggregate is affected the same way — SUM, AVG, MIN and MAX — so
//     it is not accumulation: MIN carries a value it never adds to.
//   - The factor is exactly 10^(INPUT scale): scale 2 is 100x, scale 4 is
//     10^4. That is a value at scale s being read as scale 0 and coerced to
//     s — one rescale, not repeated arithmetic.
//   - The GATHER receives the correct schema (DECIMAL(38,2)) with an already
//     corrupted carrier, so the corruption is UPSTREAM of the coordinator.
//   - Excluded by instrumentation: HashAggregate.decOutputParams (never
//     reached on this path), exec.Project's two DECIMAL arms (never reached),
//     exec.DecimalCoerce (set-operation arms only), and the gather's own
//     rename/projection.
//
// The remaining suspect is the partial-to-final handoff across the shuffle,
// where a task whose filter matched nothing contributes a schema with no
// scale and ADR-0010's "the reader adopts the first batch's schema" rule
// takes it.
func TestFilteredDecimalAggregateScaleIsWrongOnTheDAG(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
		// diverges records the CURRENT, WRONG state. When the defect is
		// fixed these flip to false and the entry becomes an ordinary
		// two-path assertion; a diverging entry that starts agreeing fails
		// here, which is the fix's proof.
		diverges bool
	}{
		// The defect, across every aggregate and both wrapped and bare.
		{"bare sum, selective filter", "SELECT SUM(a) AS v FROM " + dbpTable + " WHERE id < 5", true},
		{"wrapped sum, selective filter", "SELECT SUM(b) * 2 AS v FROM " + dbpTable + " WHERE id < 5", true},
		{"avg, selective filter", "SELECT AVG(a) * 100 AS v FROM " + dbpTable + " WHERE id < 5", true},
		{"min, selective filter", "SELECT MIN(a) AS v FROM " + dbpTable + " WHERE id < 5", true},
		{"max, selective filter", "SELECT MAX(a) AS v FROM " + dbpTable + " WHERE id < 5", true},
		{"through a derived table",
			"SELECT SUM(a) AS v FROM (SELECT a FROM " + dbpTable + " WHERE id < 5) t", true},

		// The neighbours that are CORRECT, and which is why the trigger is
		// selectivity rather than the filter or the aggregate.
		{"no filter", "SELECT SUM(a) AS v FROM " + dbpTable, false},
		{"filter matching every row", "SELECT SUM(a) AS v FROM " + dbpTable + " WHERE id < 100", false},
		{"filter on the decimal itself, matching every row",
			"SELECT SUM(a) AS v FROM " + dbpTable + " WHERE a > -1000", false},
		{"grouped, selective filter",
			"SELECT s, SUM(a) AS v FROM " + dbpTable + " WHERE id < 5 GROUP BY s ORDER BY s", false},
		{"grouped avg and min, selective filter",
			"SELECT s, AVG(a) AS av, MIN(a) AS mn, MAX(b) AS mx FROM " + dbpTable +
				" WHERE id < 5 GROUP BY s ORDER BY s", false},
		{"an INTEGER aggregate under the same filter",
			"SELECT SUM(id) AS v FROM " + dbpTable + " WHERE id < 5", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			singleRows := fmt.Sprint(dtpRun(t, ctx, single, coord, tc.sql, false))
			dagRows := fmt.Sprint(dtpRun(t, ctx, single, coord, tc.sql, true))
			agree := singleRows == dagRows
			switch {
			case tc.diverges && agree:
				t.Errorf("%s AGREES now — the two paths match, so the pin is stale.\n"+
					"  both %s\n"+
					"Delete this entry's diverges:true (or the whole test once every "+
					"entry agrees): a pin that starts agreeing is the fix's proof.", tc.sql, singleRows)
			case !tc.diverges && !agree:
				t.Errorf("%s: the two paths disagree, and this shape was CORRECT\n"+
					"  single %s\n  dag    %s", tc.sql, singleRows, dagRows)
			}
		})
	}
}
