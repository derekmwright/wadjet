package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// A WHERE above a JOIN naming a CTE's renamed column, on THREE arms (#700, #726).
//
// Both issues report the same silent zero on one DAG lowering only: the
// exchange that feeds a shuffled join carries the CTE's ALIAS while the
// predicate above the join has been re-spelled to the base column (or the
// other way round), so `expr.ColRef.Eval` answers nil, the predicate is
// UNKNOWN on every row, and a WHERE admits only TRUE. Nothing in the tree
// asked the question on the FORCED-SHUFFLE arm — `tmdCluster` leaves the
// broadcast threshold at its cluster-derived default, and at this fixture's
// size every build broadcasts — so the whole class was gated on one lowering.
//
// This is the sweep both issues name, with the third arm added:
// `BroadcastBytesOverride = 1` puts every build through an
// exchange-repartition, which is the arm the two filings are about. Every
// expected value below is PostgreSQL 17's, taken from a live server over this
// fixture loaded into a `--locale=C` database, not from either engine.
//
// The cross is join KIND (inner, left, right, full, semi, anti) × which side
// the CTE is on × dimension versus fact × the spelling of the reference
// (qualified, bare, conjoined with a base-table predicate) × what the CTE
// BODY is (a plain rename, a window, an aggregate, a derived table instead of
// a CTE, the CTE referenced twice). Nineteen shapes; each has to ANSWER, and
// answer PostgreSQL's number, on all three arms.
func TestCTEFilterAboveJoinThreeArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB)
	// The arm both issues are filed against: nothing may broadcast, so every
	// join reads its sides through an exchange-repartition whose payload is a
	// manifest that can drop the column the filter names.
	coordB.config.BroadcastBytesOverride = 1

	arms := []struct {
		name string
		run  func(string) (*oracle.Result, error)
	}{
		{"single", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }},
		{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }},
		{"dag-shuffled", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }},
	}

	// `c` selects ids 0..99; c_i64 is id*1_000_003 and NULL every 31st row, so
	// `c.v > 90_000_000` selects ids 90..99 less the NULL at 92 — NINE rows.
	// The literal sits INSIDE the id range on purpose: a predicate that
	// selected every row or no row would pass on an engine that dropped it.
	cte := fmt.Sprintf(`WITH c AS (SELECT id, g, c_i64 AS v FROM %s WHERE id < 100) `, typematrix.Table)

	for _, tc := range []struct {
		name string
		sql  string
		want int64 // PostgreSQL 17's COUNT(*)
	}{
		{"inner/cte-probe", cte + fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM c JOIN %s t ON c.id = t.id WHERE c.v > 90000000`,
			typematrix.Table), 9},
		{"inner/cte-build", cte + fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %s t JOIN c ON c.id = t.id WHERE c.v > 90000000`,
			typematrix.Table), 9},
		// A DIMENSION on the other side rather than the fact table: the build
		// is eight rows, which is the shape that broadcasts hardest.
		{"inner/dim", cte + fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM c JOIN %s d ON c.g = d.k WHERE c.v > 90000000`,
			typematrix.Dim), 8},
		{"inner/dim-first", cte + fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %s d JOIN c ON c.g = d.k WHERE c.v > 90000000`,
			typematrix.Dim), 8},
		{"left/cte-probe", cte + fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM c LEFT JOIN %s t ON c.id = t.id WHERE c.v > 90000000`,
			typematrix.Table), 9},
		{"left/cte-build", cte + fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %s t LEFT JOIN c ON c.id = t.id WHERE c.v > 90000000`,
			typematrix.Table), 9},
		{"right/cte-probe", cte + fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM c RIGHT JOIN %s t ON c.id = t.id WHERE c.v > 90000000`,
			typematrix.Table), 9},
		{"right/cte-build", cte + fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %s t RIGHT JOIN c ON c.id = t.id WHERE c.v > 90000000`,
			typematrix.Table), 9},
		{"full/cte-probe", cte + fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM c FULL JOIN %s t ON c.id = t.id WHERE c.v > 90000000`,
			typematrix.Table), 9},
		{"semi/in", cte + fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM c WHERE c.id IN (SELECT id FROM %s) AND c.v > 90000000`,
			typematrix.Table), 9},
		// The anti join's subquery excludes ids the CTE cannot hold, so the
		// predicate above it is what decides the answer — an anti join that
		// emptied the input would pass this for the wrong reason.
		{"anti/notin", cte + fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM c WHERE c.id NOT IN (SELECT id FROM %s WHERE id > 500) `+
				`AND c.v > 90000000`, typematrix.Table), 9},
		{"bare-name", cte + fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM c JOIN %s t ON c.id = t.id WHERE v > 90000000`,
			typematrix.Table), 9},
		{"conjoined-with-base-predicate", cte + fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM c JOIN %[1]s t ON c.id = t.id `+
				`WHERE c.v > 90000000 AND t.c_i32 > 100`, typematrix.Table), 9},
		{"derived-table", fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM (SELECT id, g, c_i64 AS v FROM %[1]s WHERE id < 100) c `+
				`JOIN %[1]s t ON c.id = t.id WHERE c.v > 90000000`, typematrix.Table), 9},
		// The CTE referenced TWICE, so the reference the predicate sits above
		// is served by the DAG's `cte-alias` dedup off the other one.
		{"cte-referenced-twice", cte +
			`SELECT COUNT(*) AS n FROM c JOIN (SELECT id AS j FROM c) x ON c.id = x.j ` +
			`WHERE c.v > 90000000`, 9},
		// A CTE whose body is a WINDOW: the predicate names the window's
		// output, which lives in a hidden slot below the join.
		{"cte-body-window", fmt.Sprintf(
			`WITH c AS (SELECT id, g, SUM(c_i64) OVER () AS v FROM %[1]s WHERE id < 100) `+
				`SELECT COUNT(*) AS n FROM c JOIN %[1]s t ON c.id = t.id WHERE c.v > 90000000`,
			typematrix.Table), 100},
		// A CTE whose body is an AGGREGATE: above it the group key is a
		// column NAME and the measure is the aggregate's output column.
		{"cte-body-aggregate", fmt.Sprintf(
			`WITH c AS (SELECT g AS gk, SUM(c_i64) AS v FROM %[1]s WHERE id < 100 GROUP BY g) `+
				`SELECT COUNT(*) AS n FROM c JOIN %[2]s d ON c.gk = d.k WHERE c.v > 90000000`,
			typematrix.Table, typematrix.Dim), 7},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused a query PostgreSQL answers %d: %v\n  SQL: %s",
						arm.name, tc.want, err, tc.sql)
				}
				got := ctrCounts(t, res)
				if len(got) != 1 {
					t.Fatalf("%s arm answered %d rows, want 1\n  SQL: %s",
						arm.name, len(res.Rows), tc.sql)
				}
				if got[0] != tc.want {
					t.Errorf("%s arm answered %d, PostgreSQL 17 answers %d — a filter above a "+
						"join naming a CTE column was dropped or mis-spelled\n  SQL: %s",
						arm.name, got[0], tc.want, tc.sql)
				}
			}
		})
	}

	// The ROW spellings, so the gate sees the values and not only a count: a
	// count cannot tell a dropped predicate from a predicate applied to the
	// wrong column when the two happen to select the same number of rows.
	wantIDs := []int64{90, 91, 93, 94, 95, 96, 97, 98, 99}
	wantV := []int64{90000270, 91000273, 93000279, 94000282, 95000285,
		96000288, 97000291, 98000294, 99000297}
	for _, tc := range []struct{ name, sql string }{
		{"rows/inner", cte + fmt.Sprintf(
			`SELECT c.id AS cid, c.v AS cv FROM c JOIN %s t ON c.id = t.id `+
				`WHERE c.v > 90000000 ORDER BY c.id`, typematrix.Table)},
		{"rows/left", cte + fmt.Sprintf(
			`SELECT c.id AS cid, c.v AS cv FROM c LEFT JOIN %s t ON c.id = t.id `+
				`WHERE c.v > 90000000 ORDER BY c.id`, typematrix.Table)},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, tc.sql)
				}
				if len(res.Rows) != len(wantIDs) {
					t.Fatalf("%s arm returned %d rows, PostgreSQL 17 returns %d\n  SQL: %s",
						arm.name, len(res.Rows), len(wantIDs), tc.sql)
				}
				for i, r := range res.Rows {
					if got := fmt.Sprintf("%v", r["cid"]); got != fmt.Sprint(wantIDs[i]) {
						t.Errorf("%s arm row %d: cid = %q, want %d\n  SQL: %s",
							arm.name, i, got, wantIDs[i], tc.sql)
					}
					if got := fmt.Sprintf("%v", r["cv"]); got != fmt.Sprint(wantV[i]) {
						t.Errorf("%s arm row %d: cv = %q, want %d — the renamed CTE column "+
							"reached the client holding another column's value\n  SQL: %s",
							arm.name, i, got, wantV[i], tc.sql)
					}
				}
			}
		})
	}
}
