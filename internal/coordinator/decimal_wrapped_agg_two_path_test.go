package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestWrappedDecimalAggregateTwoPath is the gate for #555's review finding R1:
// every WRAPPED aggregate over a DECIMAL column returned NULL on the stage DAG
// while the single-process path answered.
//
// The DAG evaluates a wrapped aggregate at GATHER time — the stage emits
// `__agg_0` and the coordinator applies the surrounding expression to it — and
// that evaluator built a FLOAT64 vector and switched on the box's Go type. A
// DECIMAL boxes as its rendered TEXT, which the switch had no arm for, so
// every one of these fell to `default: SetNull`. Six shapes, all NULL, all
// silent: the query answered, with nothing in it.
//
// It is a two-path gate rather than a pg-oracle entry because the oracle's
// EngineSemantics arm runs through the embedded API, which is the
// single-process path — the arm that was already right. Only this harness runs
// both.
func TestWrappedDecimalAggregateTwoPath(t *testing.T) {
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
	}{
		// a is DECIMAL(9,2); the nine rows sum to 51.98 with two NULLs.
		{"sum wrapped in a multiply", "SELECT SUM(a) * 2 AS v FROM " + dbpTable},
		{"sum wrapped in an addition", "SELECT SUM(a) + 1 AS v FROM " + dbpTable},
		{"avg wrapped", "SELECT AVG(a) * 100 AS v FROM " + dbpTable},
		{"min wrapped", "SELECT MIN(a) * 2 AS v FROM " + dbpTable},
		{"max wrapped", "SELECT MAX(a) - 1 AS v FROM " + dbpTable},
		// Two aggregates in one expression: the gather sees __agg_0 and
		// __agg_1 and multiplies them.
		{"two aggregates", "SELECT SUM(a) * SUM(a) AS v FROM " + dbpTable},
		// The ARGUMENT is arithmetic, which the gather rewrites to
		// `__agg_0 * 2` — the shape that looks like a bare aggregate in the
		// SQL and is a wrapped one in the plan.
		{"aggregate over arithmetic", "SELECT SUM(a * 2) AS v FROM " + dbpTable},
		{"grouped and wrapped",
			"SELECT s, SUM(a) * 2 AS v FROM " + dbpTable + " GROUP BY s ORDER BY s"},
		{"grouped over arithmetic",
			"SELECT s, SUM(a * 2) AS v FROM " + dbpTable + " GROUP BY s ORDER BY s"},
		// A scalar math function around an aggregate takes the same route.
		{"round around a sum", "SELECT ROUND(SUM(a), 1) AS v FROM " + dbpTable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			singleRows := dtpRun(t, ctx, single, coord, tc.sql, false)
			dagRows := dtpRun(t, ctx, single, coord, tc.sql, true)
			if len(singleRows) == 0 {
				t.Fatalf("%s returned no rows on the single-process path", tc.sql)
			}
			// A NULL where the single-process path has a value is the defect
			// itself, so say so rather than leaving it to a digest mismatch.
			for i, r := range dagRows {
				if r["v"] == nil && i < len(singleRows) && singleRows[i]["v"] != nil {
					t.Fatalf("%s: the DAG answered NULL for row %d where the single-process "+
						"path answered %v — a DECIMAL box the gather could not put in its "+
						"output vector (#555 review, R1)", tc.sql, i, singleRows[i]["v"])
				}
			}
			if got, want := fmt.Sprint(dagRows), fmt.Sprint(singleRows); got != want {
				t.Errorf("%s\n  single %s\n  dag    %s", tc.sql, want, got)
			}
			// And the value really is the exact one, not a float that happens
			// to render the same: a DECIMAL result boxes as text.
			for _, r := range dagRows {
				if r["v"] == nil {
					continue
				}
				if _, ok := r["v"].(string); !ok {
					t.Errorf("%s: v = %#v (%T), want the DECIMAL text — a non-string box "+
						"means the gather materialized a float64 column", tc.sql, r["v"], r["v"])
				}
				break
			}
		})
	}
}
