package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/wadjet"
)

// The distributed half of #610: a window function nested inside a larger
// expression (SUM(x) OVER (...) + 1, COALESCE(LAG(x) OVER (...), 0), a window
// in a CASE branch). The fix lives entirely in the logical plan — the window
// is extracted into its own NodeWindow output column and the surrounding
// expression is rewritten to reference it — so BOTH paths consume it, but the
// stage DAG builds the window fragment and the projection above it from a
// separate lowering. This gate proves the DAG evaluates the outer expression
// over the window's result exactly as the single-process pipeline does, and
// that neither arm silently drops the window (#349/#558: window-on-DAG has a
// history of diverging).
func TestWindowNestedExprTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	tbl := typematrix.Table
	where := "WHERE id < 300"
	cases := []windowKeyCase{
		{"SumPlusOne", fmt.Sprintf(
			`SELECT id, SUM(c_i64) OVER (PARTITION BY g ORDER BY id) + 1 AS w FROM %s %s ORDER BY id`,
			tbl, where), "w"},
		{"SumTimesTwo", fmt.Sprintf(
			`SELECT id, SUM(c_i64) OVER (PARTITION BY g ORDER BY id) * 2 AS w FROM %s %s ORDER BY id`,
			tbl, where), "w"},
		{"LiteralPlusWindow", fmt.Sprintf(
			`SELECT id, 100 + COUNT(*) OVER (PARTITION BY g) AS w FROM %s %s ORDER BY id`,
			tbl, where), "w"},
		{"CoalesceLag", fmt.Sprintf(
			`SELECT id, COALESCE(LAG(c_i64) OVER (ORDER BY id), -7) AS w FROM %s %s ORDER BY id`,
			tbl, where), ""},
		{"TwoNestedWindows", fmt.Sprintf(
			`SELECT id, SUM(c_i64) OVER (PARTITION BY g ORDER BY id) - MIN(c_i64) OVER (PARTITION BY g) AS w `+
				`FROM %s %s ORDER BY id`, tbl, where), "w"},

		// BOOLEAN wrappers — the leak the #610 review caught. Before the widen,
		// referencesSyntheticWindow stopped at CmpExpr and never recursed into
		// Between/And/Or/In/Like/Is, so the gather saw these as a plain rename
		// and shipped the internal __win_0 (and the raw base columns) to the
		// client, dropping the predicate. The DAG must now return the SAME
		// booleans the single-process pipeline does — a real Bool column, not a
		// float 0/1 and never a leak.
		{"WindowBetween", fmt.Sprintf(
			`SELECT id, SUM(c_i64) OVER (ORDER BY id) BETWEEN 0 AND 100000000 AS w FROM %s %s ORDER BY id`,
			tbl, where), ""},
		{"WindowGreaterAnd", fmt.Sprintf(
			`SELECT id, (SUM(c_i64) OVER (PARTITION BY g ORDER BY id) > 5) AND (id > 0) AS w FROM %s %s ORDER BY id`,
			tbl, where), ""},
		{"WindowIn", fmt.Sprintf(
			`SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) IN (1, 2) AS w FROM %s %s ORDER BY id`,
			tbl, where), ""},
		{"WindowIsNull", fmt.Sprintf(
			`SELECT id, LAG(c_i64) OVER (ORDER BY id) IS NULL AS w FROM %s %s ORDER BY id`,
			tbl, where), ""},
		// NOTE: a nested window whose OUTER expression returns a NON-numeric,
		// NON-boolean value on the DAG (e.g. a CASE producing string literals)
		// is not compared here. The gather-time evaluator materializes float64
		// and bool; a string/date result lands on the float64 path and is
		// nulled — a PRE-EXISTING bound of the wrapped-expression gather path
		// that predates #610 and affects wrapped AGGREGATES identically. It is
		// no longer a LEAK (the wrapper is recognized and projected, never
		// passed through as internal columns), and the single-process pipeline
		// evaluates it correctly (TestNestedWindowInCaseSingleProcess).
	}

	runWindowTwoPath(t, ctx, single, coord, cases)
}

// TestAggregateBooleanWrapperTwoPath is the aggregate twin the #610 review
// surfaced: the SAME helper (referencesSyntheticAgg) omitted the boolean node
// types, so a nested aggregate wrapped in BETWEEN/AND/IN/comparison leaked
// [g __agg_0] on the DAG instead of evaluating the predicate. Widening the
// shared referencesSynthetic walker closes it; this gate holds it closed.
func TestAggregateBooleanWrapperTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	tbl := typematrix.Table
	where := "WHERE id < 300"
	cases := []windowKeyCase{
		{"AggBetween", fmt.Sprintf(
			`SELECT g, SUM(c_i64) BETWEEN 0 AND 100000000 AS w FROM %s %s GROUP BY g ORDER BY g`,
			tbl, where), ""},
		{"AggGreaterAnd", fmt.Sprintf(
			`SELECT g, (SUM(c_i64) > 100) AND (COUNT(*) > 0) AS w FROM %s %s GROUP BY g ORDER BY g`,
			tbl, where), ""},
		{"AggIn", fmt.Sprintf(
			`SELECT g, COUNT(*) IN (0, 1, 2) AS w FROM %s %s GROUP BY g ORDER BY g`,
			tbl, where), ""},
		// Control: the wrapped-aggregate ARITHMETIC that already worked keeps
		// working — the widen must not disturb the float path.
		{"AggArithmetic", fmt.Sprintf(
			`SELECT g, SUM(c_i64) / 2 + 1 AS w FROM %s %s GROUP BY g ORDER BY g`,
			tbl, where), ""},
	}
	runWindowTwoPath(t, ctx, single, coord, cases)
}

func runWindowTwoPath(t *testing.T, ctx context.Context, single *wadjet.DB, coord *Coordinator, cases []windowKeyCase) {
	t.Helper()
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			aRes, aErr := tmdRunSingle(ctx, single, c.sql)
			bRes, bErr := tmdRunDAG(ctx, coord, c.sql)
			if aErr != nil {
				t.Fatalf("the single-process engine refused this query: %v\n  SQL: %s", aErr, c.sql)
			}
			if bErr != nil {
				t.Fatalf("the stage DAG refused a query the single-process engine answered (%d rows): %v\n  SQL: %s",
					len(aRes.Rows), bErr, c.sql)
			}
			if diff := oracle.Compare(aRes, bRes, oracle.CompareSpec{Mode: oracle.CmpOrdered}); diff != "" {
				t.Errorf("TWO-PATH DIVERGENCE (#610)\n  SQL: %s\n  %s\n  single: %s\n  dag:    %s",
					c.sql, diff, tmdRender(aRes, 4), tmdRender(bRes, 4))
			}
			// A constant window column is #610's signature (the window ran
			// over nothing and only the literal survived). Assert on the arm
			// whose value carries the window.
			if c.distinctIn != "" {
				assertWindowPartitioned(t, aRes.Rows, c.distinctIn)
			}
		})
	}
}
