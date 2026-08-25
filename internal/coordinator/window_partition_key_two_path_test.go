package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// The distributed half of #585: a window's PARTITION BY / ORDER BY term that
// is QUALIFIED or an EXPRESSION.
//
// Neither spelling reached the operator as a key. A qualified reference
// (`PARTITION BY t.g`) missed the batch's bare `g`, and an expression
// (`PARTITION BY id % 3`) named a column nothing computed; both dropped out
// of the key list and the window ran over ONE partition spanning the input,
// silently. The repair has two halves that live in different places, so it
// needs both arms:
//
//   - the single-process pipeline computes an expression key with a
//     pre-window projection appended to the child ops (physical.buildWindow);
//   - the stage DAG ships the same terms as Stage.WindowKeyExprs and the
//     WORKER computes them, prepended to the window fragment's consume phase
//     (worker.buildWindowKeyProjection) — a second implementation, and
//     therefore a second chance to disagree.
//
// The DAG arm reaches a third thing neither in-process gate can: the
// exchange. A window stage declares RequiredClusteredOn its PARTITION BY
// keys, and a key that exists only INSIDE the window fragment cannot be one
// the exchange hash-partitions on — windowPartitionKeys declines those, and
// the stage runs Singleton. If it did not, each task would window a fragment
// of somebody else's partition, which is a wrong answer that only a
// multi-worker run can show.
//
// The comparison is against the single-process arm rather than a literal,
// because #585's own repro shows why a literal is the weaker gate: the wrong
// answer was a plausible-looking one (a continuous ROW_NUMBER, a whole-table
// SUM), and the two-path contract catches the case where only one arm learns
// the fix.
func TestWindowPartitionKeyTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, c := range windowKeyTwoPathCases() {
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
				t.Errorf("TWO-PATH DIVERGENCE\n  SQL: %s\n  %s\n  single: %s\n  dag:    %s",
					c.sql, diff, tmdRender(aRes, 4), tmdRender(bRes, 4))
			}
			// One partition spanning the input is #585's signature, and two
			// arms can share it. The window's own answer has to be checked
			// against the partitioning the query asked for, not only against
			// the other arm.
			if c.distinctIn != "" {
				assertWindowPartitioned(t, aRes.Rows, c.distinctIn)
			}
		})
	}
}

type windowKeyCase struct {
	name string
	sql  string
	// distinctIn names a result column whose values must not be identical in
	// every row. #585's whole symptom is a window computed over one
	// partition, which makes exactly that column constant.
	distinctIn string
}

func windowKeyTwoPathCases() []windowKeyCase {
	tbl := typematrix.Table
	where := "WHERE id < 300"
	return []windowKeyCase{
		// The bare-column control. It answered correctly before #585 and
		// must keep doing so: the fix rewrites key names on every path, and
		// a rewrite that broke the shape that already worked would be a
		// worse regression than the bug.
		{"BareColumnControl", fmt.Sprintf(
			`SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS rn FROM %s %s ORDER BY id`,
			tbl, where), "rn"},

		// Qualified, over every function family: ranking, aggregate, value.
		{"QualifiedRowNumber", fmt.Sprintf(
			`SELECT t.id, ROW_NUMBER() OVER (PARTITION BY t.g ORDER BY t.id) AS rn FROM %s t %s ORDER BY t.id`,
			tbl, "WHERE t.id < 300"), "rn"},
		{"QualifiedRank", fmt.Sprintf(
			`SELECT t.id, RANK() OVER (PARTITION BY t.g ORDER BY t.id) AS rk, `+
				`DENSE_RANK() OVER (PARTITION BY t.g ORDER BY t.id) AS dr FROM %s t %s ORDER BY t.id`,
			tbl, "WHERE t.id < 300"), "rk"},
		{"QualifiedSumCount", fmt.Sprintf(
			`SELECT t.id, SUM(t.m1) OVER (PARTITION BY t.g) AS s, COUNT(*) OVER (PARTITION BY t.g) AS n `+
				`FROM %s t %s ORDER BY t.id`, tbl, "WHERE t.id < 300"), "n"},
		{"QualifiedLagLead", fmt.Sprintf(
			`SELECT t.id, LAG(t.id) OVER (PARTITION BY t.g ORDER BY t.id) AS lg, `+
				`LEAD(t.id) OVER (PARTITION BY t.g ORDER BY t.id) AS ld FROM %s t %s ORDER BY t.id`,
			tbl, "WHERE t.id < 300"), "lg"},
		{"QualifiedFirstValue", fmt.Sprintf(
			`SELECT t.id, FIRST_VALUE(t.id) OVER (PARTITION BY t.g ORDER BY t.id) AS fv FROM %s t %s ORDER BY t.id`,
			tbl, "WHERE t.id < 300"), "fv"},
		// No ORDER BY inside OVER: the whole-partition shape, where a lost
		// PARTITION BY is a whole-TABLE aggregate rather than a running one.
		{"QualifiedNoOrderBy", fmt.Sprintf(
			`SELECT t.id, MIN(t.c_i64) OVER (PARTITION BY t.g) AS w FROM %s t %s ORDER BY t.id`,
			tbl, "WHERE t.id < 300"), "w"},

		// Expression keys, one per expression family the issue names.
		{"ExprModulo", fmt.Sprintf(
			`SELECT id, ROW_NUMBER() OVER (PARTITION BY id %% 7 ORDER BY id) AS rn FROM %s %s ORDER BY id`,
			tbl, where), "rn"},
		{"ExprArithmetic", fmt.Sprintf(
			`SELECT id, COUNT(*) OVER (PARTITION BY g - 1) AS n FROM %s %s ORDER BY id`,
			tbl, where), "n"},
		{"ExprFunction", fmt.Sprintf(
			`SELECT id, COUNT(*) OVER (PARTITION BY UPPER(c_str)) AS n FROM %s %s ORDER BY id`,
			tbl, where), ""},
		{"ExprCase", fmt.Sprintf(
			`SELECT id, ROW_NUMBER() OVER (PARTITION BY CASE WHEN id %% 2 = 0 THEN 'even' ELSE 'odd' END `+
				`ORDER BY id) AS rn FROM %s %s ORDER BY id`, tbl, where), "rn"},
		{"ExprInWindowOrderBy", fmt.Sprintf(
			`SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id %% 2, id) AS rn FROM %s %s ORDER BY id`,
			tbl, where), "rn"},
		{"ExprDescNullsFirst", fmt.Sprintf(
			`SELECT id, ROW_NUMBER() OVER (PARTITION BY id %% 5 ORDER BY c_i64 DESC NULLS FIRST, id) AS rn `+
				`FROM %s %s ORDER BY id`, tbl, where), "rn"},

		// A ROW field path, in every window position (#603). `rw.f` LOOKS
		// like a qualified reference and is not one: the qualifier is a ROW
		// column, so dropping it would look up a column named `f` that does
		// not exist. The ARGUMENT form is the one whose old symptom was NULL
		// rather than one partition, and the DAG is where the materialized
		// column has to travel as a stage spec rather than a compiled
		// closure.
		{"RowFieldPathPartitionBy", fmt.Sprintf(
			`SELECT id, COUNT(*) OVER (PARTITION BY c_row.b) AS n FROM %s WHERE id < 300 ORDER BY id`,
			typematrix.Nested), ""},
		{"RowFieldPathArgument", fmt.Sprintf(
			`SELECT id, SUM(c_row.b) OVER () AS s, LAG(c_row.a) OVER (ORDER BY id) AS lg, `+
				`MIN(c_row.b) OVER (PARTITION BY id %% 3) AS lo `+
				`FROM %s WHERE id < 300 ORDER BY id`, typematrix.Nested), "lo"},
		{"RowFieldPathWindowOrderBy", fmt.Sprintf(
			`SELECT id, ROW_NUMBER() OVER (ORDER BY c_row.b DESC NULLS FIRST, id) AS rn `+
				`FROM %s WHERE id < 300 ORDER BY id`, typematrix.Nested), "rn"},

		// Mixed key lists: bare beside qualified beside expression, in one
		// OVER clause and across two.
		{"MixedKeys", fmt.Sprintf(
			`SELECT t.id, ROW_NUMBER() OVER (PARTITION BY g, t.c_bool, t.id %% 3 ORDER BY t.id) AS rn `+
				`FROM %s t WHERE t.id < 300 ORDER BY t.id`, tbl), "rn"},
		{"TwoClausesShareAnExpressionKey", fmt.Sprintf(
			`SELECT id, ROW_NUMBER() OVER (PARTITION BY id %% 4 ORDER BY id) AS rn, `+
				`COUNT(*) OVER (PARTITION BY id %% 4) AS n FROM %s %s ORDER BY id`, tbl, where), "rn"},
		{"TwoClausesDifferentKeys", fmt.Sprintf(
			`SELECT id, ROW_NUMBER() OVER (PARTITION BY id %% 4 ORDER BY id) AS rn, `+
				`COUNT(*) OVER (PARTITION BY g) AS n FROM %s %s ORDER BY id`, tbl, where), "rn"},

		// NULL partition keys: PostgreSQL puts every NULL in ONE partition.
		// c_bool nulls on its own stride in the fixture, so this key really
		// carries them.
		{"NullPartitionKey", fmt.Sprintf(
			`SELECT id, COUNT(*) OVER (PARTITION BY c_bool) AS n FROM %s %s ORDER BY id`,
			tbl, where), "n"},

		// The window's key over a GROUP BY, and over a derived table — the
		// two shapes where the window's input is not a scan, so the planner
		// cannot type the key from a catalog and the stage below the window
		// is not a projectable producer (#558's territory).
		{"OverGroupBy", fmt.Sprintf(
			`SELECT g, ROW_NUMBER() OVER (PARTITION BY g %% 2 ORDER BY g) AS rn `+
				`FROM %s GROUP BY g ORDER BY g`, tbl), ""},
		{"OverGroupByHaving", fmt.Sprintf(
			`SELECT g, ROW_NUMBER() OVER (PARTITION BY g %% 2 ORDER BY g) AS rn `+
				`FROM %s GROUP BY g HAVING COUNT(*) > 1 ORDER BY g`, tbl), ""},

		// Every row of the table, so the pre-window projection runs over MANY
		// batches and the window retains all of them. The first version of
		// this fix reused one pooled vector for the computed key across
		// Execute calls, so every retained batch read the LAST batch's key —
		// #585's own symptom, reproduced by its fix, and invisible to any
		// single-batch case above.
		{"ExprKeyAcrossManyBatches", fmt.Sprintf(
			`SELECT k, COUNT(*) AS n FROM (SELECT id %% 7 AS k, `+
				`COUNT(*) OVER (PARTITION BY id %% 7) AS w FROM %s) u `+
				`GROUP BY k, w ORDER BY k`, tbl), ""},
		{"OverDerivedTable", fmt.Sprintf(
			`SELECT u.id, ROW_NUMBER() OVER (PARTITION BY u.id %% 3 ORDER BY u.id) AS rn `+
				`FROM (SELECT id FROM %s WHERE id < 300) u ORDER BY u.id`, tbl), "rn"},
	}
}

// assertWindowPartitioned fails when every row carries the same value for
// col. That is not a general property of a window function — it is the
// property these queries were written to have, and it is exactly what a
// window degraded to one partition destroys.
func assertWindowPartitioned(t *testing.T, rows []map[string]any, col string) {
	t.Helper()
	if len(rows) < 2 {
		t.Fatalf("the fixture returned %d rows, too few for %q to say anything", len(rows), col)
	}
	first := fmt.Sprint(rows[0][col])
	for _, r := range rows[1:] {
		if fmt.Sprint(r[col]) != first {
			return
		}
	}
	var sample []string
	for i, r := range rows {
		if i == 4 {
			break
		}
		sample = append(sample, fmt.Sprintf("%v", r[col]))
	}
	t.Errorf("every row has %s=%s — the window ran over ONE partition, which is #585's signature "+
		"(first rows: %s)", col, first, strings.Join(sample, ", "))
}
