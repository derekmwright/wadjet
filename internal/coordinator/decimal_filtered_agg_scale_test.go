package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestFilteredDecimalAggregateAgreesOnBothPaths was
// TestFilteredDecimalAggregateScaleIsWrongOnTheDAG, the pin for #685: an
// UNGROUPED aggregate over a DECIMAL column, under a filter that actually
// excludes rows, came back 10^scale too large on the stage DAG.
//
//	SELECT SUM(a) FROM decpair WHERE id < 5
//	  PostgreSQL 38.24 · single-process 38.24 · stage DAG 3824.00
//
// Every entry it pinned as diverging now AGREES, which is what a pin is for
// (ADR-0013): the `diverges` flags are gone and the entries are ordinary
// two-path assertions, with the six headline shapes asserted at their VALUE as
// well — two paths through one wrong accumulator would agree with each other
// and prove nothing.
//
// The mechanism the pin's notes could not name, for the record, since those
// notes were half right: the corruption IS upstream of the coordinator and the
// factor IS one rescale of a value already at its scale, but the reader does
// not adopt the first batch's schema — each file decodes under its own header.
// The scale-0 header written by a partial whose filter matched nothing reached
// the final aggregate as a scale-0 all-NULL BATCH, and the DECIMAL batch
// kernels adopted `acc.DecScale` from it OUTSIDE their null guard, so a batch
// that contributed nothing redefined the scale of an Int128 it never touched.
// Both halves are fixed: the identity row now declares its stage's (p,s)
// (exec.AggColumn.OutputPrecision/OutputScale), and the accumulator takes a
// scale only from a batch that held a value.
//
// The gates that carry the detail: TestFilteredDecimalAggregateTwoPath (the
// value matrix, both paths), TestEmptyPartialHeaderCarriesTheDeclaredDecimalParams
// (the .wshf bytes over the whole legal (p,s) range),
// TestAllNullFileDoesNotRescaleTheAggregate (the morsel-parallel half, which no
// filter reaches), kernel.TestDecimalBatchKernelsKeepTheContributingScale and
// exec.TestScalarDecimalMergeIgnoresANonContributingClone (the accumulator,
// isolated), and wshf.TestSchemaGuardRefusesFilesThatDescribeDifferentRelations
// (the reader-side backstop).
func TestFilteredDecimalAggregateAgreesOnBothPaths(t *testing.T) {
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
		// want is the single-cell answer, where the shape has one. Empty
		// means "assert only that the two paths agree" — the multi-row
		// grouped entries, whose values TestFilteredDecimalAggregateTwoPath
		// asserts in full.
		want string
	}{
		// The defect, across every aggregate and both wrapped and bare. The
		// values are PostgreSQL 17.11's over the same nine rows, except the
		// two AVG-derived ones: AVG keeps a different number of digits by
		// contract (ADR-0012 item 9), and 956.000000 is that contract's
		// rendering of PostgreSQL's 956.0000000000000000.
		{"bare sum, selective filter", "SELECT SUM(a) AS v FROM " + dbpTable + " WHERE id < 5", "38.24"},
		{"wrapped sum, selective filter", "SELECT SUM(b) * 2 AS v FROM " + dbpTable + " WHERE id < 5", "76.4800"},
		{"avg, selective filter", "SELECT AVG(a) * 100 AS v FROM " + dbpTable + " WHERE id < 5", "956.000000"},
		{"min, selective filter", "SELECT MIN(a) AS v FROM " + dbpTable + " WHERE id < 5", "-0.01"},
		{"max, selective filter", "SELECT MAX(a) AS v FROM " + dbpTable + " WHERE id < 5", "12.75"},
		{"through a derived table",
			"SELECT SUM(a) AS v FROM (SELECT a FROM " + dbpTable + " WHERE id < 5) t", "38.24"},

		// The neighbours that were CORRECT throughout, kept because they are
		// what said the trigger was selectivity rather than the filter or the
		// aggregate — and because a fix that broke one of them would be
		// trading a defect for a defect.
		{"no filter", "SELECT SUM(a) AS v FROM " + dbpTable, "52.99"},
		{"filter matching every row", "SELECT SUM(a) AS v FROM " + dbpTable + " WHERE id < 100", "52.99"},
		{"filter on the decimal itself, matching every row",
			"SELECT SUM(a) AS v FROM " + dbpTable + " WHERE a > -1000", "52.99"},
		{"grouped, selective filter",
			"SELECT s, SUM(a) AS v FROM " + dbpTable + " WHERE id < 5 GROUP BY s ORDER BY s", ""},
		{"grouped avg and min, selective filter",
			"SELECT s, AVG(a) AS av, MIN(a) AS mn, MAX(b) AS mx FROM " + dbpTable +
				" WHERE id < 5 GROUP BY s ORDER BY s", ""},
		{"an INTEGER aggregate under the same filter",
			"SELECT SUM(id) AS v FROM " + dbpTable + " WHERE id < 5", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			singleRows := dtpRun(t, ctx, single, coord, tc.sql, false)
			dagRows := dtpRun(t, ctx, single, coord, tc.sql, true)
			if fmt.Sprint(singleRows) != fmt.Sprint(dagRows) {
				t.Fatalf("%s: the two paths disagree\n  single %v\n  dag    %v",
					tc.sql, singleRows, dagRows)
			}
			if tc.want == "" {
				return
			}
			for _, arm := range []struct {
				name string
				rows []map[string]any
			}{{"single", singleRows}, {"dag", dagRows}} {
				if len(arm.rows) != 1 {
					t.Fatalf("%s %s: %d rows, want 1", arm.name, tc.sql, len(arm.rows))
				}
				dtpCell(t, arm.name+" "+tc.sql, arm.rows[0]["v"], tc.want)
			}
		})
	}
}
