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

// TestCTEFilterAboveAJoinChainThreeArms is the same question above a CHAIN of
// joins, which is where #700 and #726 actually live.
//
// The sweep next door stops at ONE join and passes everywhere, and that is
// exactly why the class stayed open: with a SECOND join the same filter is
// dropped again. The column pruner partitions a join's needed columns between
// its two sides using the names the subtree's SCANS store, and a CTE's alias
// is stored by no scan — so `v` was in neither side's available set, went into
// neither probeNeeds nor buildNeeds, and the INNER join's NeededColumns (its
// OutputFilter) dropped the column the filter above the OUTER join was about
// to read. One join hid it because there the partitioned join is the one the
// Project feeds directly, so the alias never had to survive a second
// partition.
//
// The three arms fail three different ways, which is what makes the shape
// worth gating on all of them: the single-process path raised `filter column
// "c.v" does not exist in the input schema`, the SHUFFLED DAG answered ZERO
// rows in silence, and the broadcast DAG answered correctly.
//
// The cross is chain LENGTH (2 and 3 joins) x the CTE's POSITION in it
// (first, second, last) x CTE versus derived table x the filter's spelling
// (qualified, bare, conjoined with a base-table predicate, and the row
// spelling that sees values rather than a count) x join kind. Every expected
// value is PostgreSQL 17's over the same fixture in a --locale=C database.
func TestCTEFilterAboveAJoinChainThreeArms(t *testing.T) {
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
	coordB.config.BroadcastBytesOverride = 1

	arms := []struct {
		name string
		run  func(string) (*oracle.Result, error)
	}{
		{"single", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }},
		{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }},
		{"dag-shuffled", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }},
	}

	body := fmt.Sprintf(`SELECT id, g, c_i64 AS v FROM %s WHERE id < 100`, typematrix.Table)
	cteHead := `WITH c AS (` + body + `) SELECT COUNT(*) AS n FROM `
	derHead := `SELECT COUNT(*) AS n FROM `
	tbl := typematrix.Table

	for _, sp := range []struct{ tag, head, c string }{
		{"cte", cteHead, "c"},
		{"derived", derHead, "(" + body + ") c"},
	} {
		sp := sp
		for _, tc := range []struct {
			name string
			sql  string
			want int64
			// shuffledRefuses pins a shape whose SHUFFLED arm fails LOUDLY
			// for a mechanism that is not this one — the chained-join
			// fusion's missing build output, which hits the CTE and the
			// DERIVED spelling identically and so cannot be about the alias.
			// TODO(#755): delete when that lands.
			shuffledRefuses string
		}{
			{name: "2join/cte-first", want: 9, sql: sp.head + sp.c +
				fmt.Sprintf(` JOIN %[1]s t ON c.id = t.id JOIN %[1]s u ON c.id = u.id `+
					`WHERE c.v > 90000000`, tbl)},
			{name: "2join/cte-second", want: 9, sql: sp.head +
				fmt.Sprintf(`%s t JOIN `, tbl) + sp.c +
				fmt.Sprintf(` ON c.id = t.id JOIN %s u ON c.id = u.id WHERE c.v > 90000000`, tbl)},
			{name: "2join/cte-last", want: 9, shuffledRefuses: "output not found", sql: sp.head +
				fmt.Sprintf(`%[1]s t JOIN %[1]s u ON t.id = u.id JOIN `, tbl) + sp.c +
				` ON c.id = t.id WHERE c.v > 90000000`},
			{name: "3join/cte-first", want: 9, sql: sp.head + sp.c +
				fmt.Sprintf(` JOIN %[1]s t ON c.id = t.id JOIN %[1]s u ON c.id = u.id `+
					`JOIN %[1]s w ON c.id = w.id WHERE c.v > 90000000`, tbl)},
			{name: "2join/dimension-then-fact", want: 8, sql: sp.head + sp.c +
				fmt.Sprintf(` JOIN %[1]s d ON c.g = d.k JOIN %[2]s u ON c.id = u.id `+
					`WHERE c.v > 90000000`, typematrix.Dim, tbl)},
			{name: "2join/left", want: 9, sql: sp.head + sp.c +
				fmt.Sprintf(` LEFT JOIN %[1]s t ON c.id = t.id LEFT JOIN %[1]s u ON c.id = u.id `+
					`WHERE c.v > 90000000`, tbl)},
			{name: "2join/bare-name", want: 9, sql: sp.head + sp.c +
				fmt.Sprintf(` JOIN %[1]s t ON c.id = t.id JOIN %[1]s u ON c.id = u.id `+
					`WHERE v > 90000000`, tbl)},
			{name: "2join/conjoined", want: 9, sql: sp.head + sp.c +
				fmt.Sprintf(` JOIN %[1]s t ON c.id = t.id JOIN %[1]s u ON c.id = u.id `+
					`WHERE c.v > 90000000 AND t.c_i32 > 100`, tbl)},
		} {
			tc := tc
			t.Run(sp.tag+"/"+tc.name, func(t *testing.T) {
				for _, arm := range arms {
					res, err := arm.run(tc.sql)
					if err != nil {
						if tc.shuffledRefuses != "" && arm.name == "dag-shuffled" &&
							strings.Contains(err.Error(), tc.shuffledRefuses) {
							t.Logf("tracked separately (#755), NOT gated here: %v", err)
							continue
						}
						t.Fatalf("%s arm refused a query PostgreSQL answers %d: %v\n  SQL: %s",
							arm.name, tc.want, err, tc.sql)
					}
					if tc.shuffledRefuses != "" && arm.name == "dag-shuffled" {
						t.Errorf("the shuffled arm now ANSWERS this shape, so TODO(#755) is "+
							"fixed — delete shuffledRefuses\n  SQL: %s", tc.sql)
						continue
					}
					got := ctrCounts(t, res)
					if len(got) != 1 {
						t.Fatalf("%s arm answered %d rows, want 1\n  SQL: %s",
							arm.name, len(res.Rows), tc.sql)
					}
					if got[0] != tc.want {
						t.Errorf("%s arm answered %d, PostgreSQL 17 answers %d — a filter above a "+
							"CHAIN of joins lost the CTE's renamed column\n  SQL: %s",
							arm.name, got[0], tc.want, tc.sql)
					}
				}
			})
		}
	}

	// The row spelling through a two-join chain, so the gate sees values.
	wantIDs := []int64{90, 91, 93, 94, 95, 96, 97, 98, 99}
	wantV := []int64{90000270, 91000273, 93000279, 94000282, 95000285,
		96000288, 97000291, 98000294, 99000297}
	for _, sp := range []struct{ tag, sql string }{
		{"cte", `WITH c AS (` + body + `) SELECT c.id AS cid, c.v AS cv FROM c ` +
			fmt.Sprintf(`JOIN %[1]s t ON c.id = t.id JOIN %[1]s u ON c.id = u.id `, tbl) +
			`WHERE c.v > 90000000 ORDER BY c.id`},
		{"derived", `SELECT c.id AS cid, c.v AS cv FROM (` + body + `) c ` +
			fmt.Sprintf(`JOIN %[1]s t ON c.id = t.id JOIN %[1]s u ON c.id = u.id `, tbl) +
			`WHERE c.v > 90000000 ORDER BY c.id`},
	} {
		sp := sp
		t.Run(sp.tag+"/2join/rows", func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(sp.sql)
				if err != nil {
					t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sp.sql)
				}
				if len(res.Rows) != len(wantIDs) {
					t.Fatalf("%s arm returned %d rows, PostgreSQL 17 returns %d\n  SQL: %s",
						arm.name, len(res.Rows), len(wantIDs), sp.sql)
				}
				for i, r := range res.Rows {
					if got := fmt.Sprintf("%v", r["cid"]); got != fmt.Sprint(wantIDs[i]) {
						t.Errorf("%s arm row %d: cid = %q, want %d\n  SQL: %s",
							arm.name, i, got, wantIDs[i], sp.sql)
					}
					if got := fmt.Sprintf("%v", r["cv"]); got != fmt.Sprint(wantV[i]) {
						t.Errorf("%s arm row %d: cv = %q, want %d\n  SQL: %s",
							arm.name, i, got, wantV[i], sp.sql)
					}
				}
			}
		})
	}

	// DECIMAL through the same chain, and with the CTE on the BUILD side of
	// the first join — the spelling where QualifyAllBuildCols renames the
	// build's `a` to `decpair.a` and the re-spelled filter names `a`.
	// PostgreSQL: five of the nine rows have a > 1.
	for _, tc := range []struct{ name, sql string }{
		{"decimal/2join", `WITH c AS (SELECT id, a AS v FROM ` + dbpTable + `) ` +
			`SELECT COUNT(*) AS n FROM c JOIN ` + dbpTable + ` t ON c.id = t.id ` +
			`JOIN ` + dbpTable + ` u ON c.id = u.id WHERE c.v > 1`},
		{"decimal/2join/cte-on-build", `WITH c AS (SELECT id, a AS v FROM ` + dbpTable + `) ` +
			`SELECT COUNT(*) AS n FROM ` + dbpTable + ` t JOIN c ON c.id = t.id ` +
			`JOIN ` + dbpTable + ` u ON c.id = u.id WHERE c.v > 1`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused a query PostgreSQL answers 5: %v\n  SQL: %s",
						arm.name, err, tc.sql)
				}
				got := ctrCounts(t, res)
				if len(got) != 1 || got[0] != 5 {
					t.Errorf("%s arm answered %v, PostgreSQL 17 answers 5\n  SQL: %s",
						arm.name, got, tc.sql)
				}
			}
		})
	}
}

// TestCTEComputedColumnAboveAJoinChainThreeArms is the COMPUTED-mint half of
// the two above, and it is the fixture ADR-0025's "a subtree publishes what it
// MINTS" claim was missing (correctness protocol, METHOD 10).
//
// Both chain gates beside this one publish a plain RENAME (`c_i64 AS v`).
// Every one of them passed while a CTE publishing an EXPRESSION was still
// dropped, because the two travel differently: a rename resolves back to a
// source column through every DAG resolver, and a computed output has to be
// MATERIALIZED by some fragment or it exists nowhere. `grep '\* 2'` over this
// file returned nothing before this test, which is exactly the hole METHOD 10
// describes — the claim was asserted and no fixture attempted it.
//
// Two mechanisms were live under it and both are fixed:
//
//   - the RE-SPELLED predicate a chained join carries (`(a * 2) > 1`) names a
//     source column the OutputFilter narrowed away, and the narrowing may be
//     two links down the chain rather than on the stage that evaluates it —
//     `ensureJoinCarriesEvaluatedColumns` now reads ChainedJoins/FusedJoins
//     and pushes the columns down every narrowing stage below;
//   - a computed output over a DECIMAL aggregate is DECLINED by
//     absorbAggregateOutputProjection (ADR-0024 item 2: a wrong DECIMAL
//     declaration is worse than no projection), so nothing computes it and
//     the predicate above the join was UNKNOWN on every row — a JOIN stage is
//     excluded from assertCarrierSchemaResolves by design, so this was the
//     one silent zero every check in that file was blind to.
//     assertJoinFiltersAreBacked asks the WEAK question there instead and
//     routes the query local, which answers it.
//
// The body forms cross arithmetic over a column, over an aggregate and over a
// window, with CASE, CAST, COALESCE, DECIMAL arithmetic and Project-over-
// Project, against 2- and 3-join chains, the CTE first and second, and the
// filtering, projecting and HAVING spellings. Values are PostgreSQL 17's.
func TestCTEComputedColumnAboveAJoinChainThreeArms(t *testing.T) {
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
	coordB.config.BroadcastBytesOverride = 1

	arms := []struct {
		name string
		run  func(string) (*oracle.Result, error)
	}{
		{"single", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }},
		{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }},
		{"dag-shuffled", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }},
	}

	// decpair: a is DECIMAL(9,2) with five rows above 1 once doubled — the
	// literal sits inside the range so a dropped predicate is visible.
	const tbl = dbpTable
	c2 := " JOIN " + tbl + " t ON c.id = t.id JOIN " + tbl + " u ON c.id = u.id "
	c3 := c2 + "JOIN " + tbl + " w2 ON c.id = w2.id "

	for _, body := range []struct {
		name, sel, from string
		want            int64 // PostgreSQL's COUNT(*) for `WHERE c.dv > 1`
	}{
		{"arith-over-column", "id, a * 2 AS dv", tbl, 5},
		{"case", "id, CASE WHEN a > 1 THEN a ELSE 0 END AS dv", tbl, 5},
		{"cast", "id, CAST(a AS DOUBLE PRECISION) + 1 AS dv", tbl, 5},
		{"coalesce", "id, COALESCE(a, 0) + 1 AS dv", tbl, 5},
		{"decimal-arith", "id, a * b AS dv", tbl, 4},
		{"project-over-project", "id, v * 2 AS dv", "(SELECT id, a AS v FROM " + tbl + ") z", 5},
		{"arith-over-window", "id, SUM(a) OVER () + 0 AS dv", tbl, 9},
		{"arith-over-aggregate", "id, SUM(a) * 2 AS dv", tbl + " GROUP BY id", 5},
	} {
		body := body
		cte := "WITH c AS (SELECT " + body.sel + " FROM " + body.from + ") "
		der := "(SELECT " + body.sel + " FROM " + body.from + ") c"
		for _, sh := range []struct{ name, sql string }{
			{"cte/2join", cte + "SELECT COUNT(*) AS n FROM c" + c2 + "WHERE c.dv > 1"},
			{"cte/3join", cte + "SELECT COUNT(*) AS n FROM c" + c3 + "WHERE c.dv > 1"},
			{"cte/2join/bare", cte + "SELECT COUNT(*) AS n FROM c" + c2 + "WHERE dv > 1"},
			{"derived/2join", "SELECT COUNT(*) AS n FROM " + der + c2 + "WHERE c.dv > 1"},
		} {
			sh := sh
			// The DERIVED spelling of a computed output over an AGGREGATE is
			// refused LOUDLY on both DAG arms: assertCarrierSchemaResolves
			// catches it correctly (the filter lands on the aggregate stage,
			// which the check DOES model) but its error does not wrap
			// ErrUnreachableGatherOutput and it runs at dispatch, so nothing
			// routes the query local and it reaches the client. The CTE
			// spelling of the same query is answered — assertJoinFiltersAreBacked
			// refuses it at PLAN time, where the coordinator does route local.
			// A loud failure on a query PostgreSQL answers is a defect, not a
			// wrong answer, and it is filed rather than fixed here.
			refusesLoudly := body.name == "arith-over-aggregate" && sh.name == "derived/2join"
			t.Run(body.name+"/"+sh.name, func(t *testing.T) {
				for _, arm := range arms {
					res, err := arm.run(sh.sql)
					if err != nil {
						if refusesLoudly && arm.name != "single" &&
							strings.Contains(err.Error(), "carries no") {
							t.Logf("tracked separately, NOT gated here: %v", err)
							continue
						}
						t.Fatalf("%s arm refused a query PostgreSQL answers %d: %v\n  SQL: %s",
							arm.name, body.want, err, sh.sql)
					}
					if refusesLoudly && arm.name != "single" {
						t.Errorf("the %s arm now ANSWERS this shape, so the dispatch-time refusal "+
							"is routed or repaired — delete refusesLoudly\n  SQL: %s",
							arm.name, sh.sql)
						continue
					}
					got := ctrCounts(t, res)
					if len(got) != 1 || got[0] != body.want {
						t.Errorf("%s arm answered %v, PostgreSQL 17 answers %d — a CTE publishing "+
							"a COMPUTED column lost it above a join chain\n  SQL: %s",
							arm.name, got, body.want, sh.sql)
					}
				}
			})
		}
	}

	// The PROJECTING spelling, which returns the computed value rather than
	// counting rows, so the gate compares VALUES: `a * 2` for the five rows
	// above 1 is 25.50 on the three 12.75 rows, 4.00 and … — asserted as the
	// first row's pair plus the row count.
	t.Run("projecting/2join", func(t *testing.T) {
		sql := "WITH c AS (SELECT id, a * 2 AS dv FROM " + tbl + ") " +
			"SELECT c.id AS cid, c.dv AS dv FROM c" + c2 + "WHERE c.dv > 1 ORDER BY c.id"
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			if len(res.Rows) != 5 {
				t.Fatalf("%s arm returned %d rows, PostgreSQL 17 returns 5\n  SQL: %s",
					arm.name, len(res.Rows), sql)
			}
			if got := fmt.Sprintf("%v", res.Rows[0]["cid"]); got != "1" {
				t.Errorf("%s arm: cid = %q, want 1\n  SQL: %s", arm.name, got, sql)
			}
			if got := fmt.Sprintf("%v", res.Rows[0]["dv"]); got != "25.50" {
				t.Errorf("%s arm: dv = %q, PostgreSQL 17 answers 25.50\n  SQL: %s",
					arm.name, got, sql)
			}
		}
	})

	// The HAVING spelling over the same chain.
	t.Run("having/2join", func(t *testing.T) {
		sql := "WITH c AS (SELECT id, a * 2 AS dv FROM " + tbl + ") " +
			"SELECT c.id AS cid, COUNT(*) AS n FROM c" + c2 +
			"GROUP BY c.id HAVING COUNT(*) > 0 ORDER BY c.id"
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			if len(res.Rows) != 9 {
				t.Errorf("%s arm returned %d rows, PostgreSQL 17 returns 9\n  SQL: %s",
					arm.name, len(res.Rows), sql)
			}
		}
	})

	// TODO(#762): an AGGREGATE-bodied CTE read from the BUILD side of a JOIN
	// CHAIN and filtered above answers ZERO on both DAG arms. It is a
	// different site from everything above — the two shuffle sides share ONE
	// payload manifest, so the probe nominally carries the build's `dv` too
	// and the name resolves to the wrong side, which is the "resolves to the
	// WRONG thing" class ADR-0025 says the weak reachability question cannot
	// see. Verified pre-existing when it was filed.
	//
	// The trigger is NOT what the issue reports. #762 names three conditions —
	// an aggregate body, a COMPUTED published column, and the chain — and the
	// computed column is not one of them. Measured over this fixture on all
	// three arms (single is PostgreSQL 17's 6 everywhere):
	//
	//   body        published   position   joins   dag / dag-shuffled
	//   aggregate   computed    build      1       6  6
	//   aggregate   computed    build      2       0  0   <- pinned below
	//   aggregate   computed    build      3       0  0
	//   aggregate   computed    first      2       6  6
	//   aggregate   computed    last       2       0  0
	//   aggregate   RENAME      build      1       6  6
	//   aggregate   RENAME      build      2       0  0   <- pinned below
	//   aggregate   RENAME      first      2       6  6
	//   aggregate   RENAME      last       2       0  0
	//   plain       computed    build      2       6  6
	//   plain       RENAME      build      2       6  6
	//
	// So the aggregate body and a CHAIN with the CTE anywhere but first are
	// what matter; the expression is incidental. The issue's control row for
	// the rename spelling must have been taken at ONE join, where it is
	// correct. Both spellings are pinned below, and a fix that repairs only
	// the computed one will be caught by the other pin rather than passing.
	//
	// The three controls carry the rows that bound the trigger: one join, the
	// CTE first, and a non-aggregate body. Every value here is PostgreSQL 17's
	// over this fixture in a --locale=C database, not either engine's.
	for _, tc := range []struct {
		name, sql string
		pinDAG    bool // pinned at 0 on both DAG arms rather than asserted at 6
	}{
		{
			name: "aggregate-computed/cte-on-build/2join",
			sql: "WITH c AS (SELECT id, SUM(f) * 2 AS dv FROM " + tbl + " GROUP BY id) " +
				"SELECT COUNT(*) AS n FROM " + tbl + " t JOIN c ON c.id = t.id " +
				"JOIN " + tbl + " u ON c.id = u.id WHERE c.dv > 1",
			pinDAG: true,
		},
		{
			name: "aggregate-renamed/cte-on-build/2join",
			sql: "WITH c AS (SELECT id, SUM(f) AS dv FROM " + tbl + " GROUP BY id) " +
				"SELECT COUNT(*) AS n FROM " + tbl + " t JOIN c ON c.id = t.id " +
				"JOIN " + tbl + " u ON c.id = u.id WHERE c.dv > 1",
			pinDAG: true,
		},
		{
			name: "aggregate-computed/cte-on-build/1join",
			sql: "WITH c AS (SELECT id, SUM(f) * 2 AS dv FROM " + tbl + " GROUP BY id) " +
				"SELECT COUNT(*) AS n FROM " + tbl + " t JOIN c ON c.id = t.id WHERE c.dv > 1",
		},
		{
			name: "aggregate-computed/cte-first/2join",
			sql: "WITH c AS (SELECT id, SUM(f) * 2 AS dv FROM " + tbl + " GROUP BY id) " +
				"SELECT COUNT(*) AS n FROM c JOIN " + tbl + " t ON c.id = t.id " +
				"JOIN " + tbl + " u ON c.id = u.id WHERE c.dv > 1",
		},
		{
			name: "plain-computed/cte-on-build/2join",
			sql: "WITH c AS (SELECT id, f * 2 AS dv FROM " + tbl + ") " +
				"SELECT COUNT(*) AS n FROM " + tbl + " t JOIN c ON c.id = t.id " +
				"JOIN " + tbl + " u ON c.id = u.id WHERE c.dv > 1",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			const want = 6 // PostgreSQL 17
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused a query PostgreSQL answers %d: %v\n  SQL: %s",
						arm.name, want, err, tc.sql)
				}
				got := ctrCounts(t, res)
				if arm.name == "single" || !tc.pinDAG {
					if len(got) != 1 || got[0] != want {
						t.Errorf("%s arm answered %v, PostgreSQL 17 answers %d\n  SQL: %s",
							arm.name, got, want, tc.sql)
					}
					continue
				}
				switch {
				case len(got) == 1 && got[0] == want:
					t.Errorf("the %s arm now agrees — delete this pin (#762): it answered "+
						"PostgreSQL's %d where the pin records 0, so assert %d on every arm "+
						"and drop the TODO\n  SQL: %s", arm.name, want, want, tc.sql)
				case len(got) == 1 && got[0] == 0:
					t.Logf("known defect, NOT gated [#762]: the %s arm answers 0 where "+
						"PostgreSQL 17 answers %d", arm.name, want)
				default:
					t.Errorf("the %s arm answered %v, which is neither the pinned 0 nor "+
						"PostgreSQL's %d — #762 changed shape\n  SQL: %s",
						arm.name, got, want, tc.sql)
				}
			}
		})
	}
}
