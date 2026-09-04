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

// dajArms stands up the three arms this file's gates run on: the embedded
// single-process engine, the stage DAG at its cluster-derived broadcast
// threshold, and the stage DAG with every build forced through an
// exchange-repartition.
type dajArm struct {
	name string
	run  func(string) (*oracle.Result, error)
	// coord is the arm's coordinator, nil for the single-process one. It is
	// here so a subtest can assert the MECHANISM beside the rows: a review
	// found a shape in this very file that moved from executing distributed to
	// being refused and routed while the subtest stayed green, because the
	// subtest read rows only — the answer was right either way and the
	// distribution was gone.
	coord *Coordinator
}

func dajArms(t *testing.T, ctx context.Context) []dajArm {
	t.Helper()
	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	return []dajArm{
		{name: "single", run: func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }},
		{name: "dag", run: func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }, coord: coord},
		{name: "dag-shuffled", run: func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }, coord: coordB},
	}
}

// dajDigest renders a whole result the way the PostgreSQL side of these gates
// records it: every row, in order, `|`-joined per column and `;`-terminated,
// NULL as the empty string.
//
// The WHOLE result and not a prefix. An earlier cut of this file asserted the
// first six of PostgreSQL's nine rows and stopped short of the NULL-bearing
// tail, so an extra row, a duplicated row and a wrong NULL all passed. A
// digest cannot do that: it is the row COUNT and every value at once.
func dajDigest(res *oracle.Result, cols []string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%d rows: ", len(res.Rows))
	for _, r := range res.Rows {
		for j, c := range cols {
			if j > 0 {
				sb.WriteByte('|')
			}
			if v, ok := r[c]; ok && v != nil {
				fmt.Fprintf(&sb, "%v", v)
			}
		}
		sb.WriteByte(';')
	}
	return sb.String()
}

// A join CHAIN over DERIVED arms, on three arms against PostgreSQL 17
// (#755, #766, #753).
//
// Three filings sit on one mechanism family, and each of them is a shape the
// chain gates next door could not have caught:
//
//   - #755 the SHUFFLED lowering refused a three-way join over derived arms
//     at dispatch — `chained join 0: build dep "exchange-repartition-7"
//     output not found`. `fuseScanShuffle` absorbs an exchange into the scan
//     that feeds it and rewires Dependencies, LeftDepStage and RightDepStage;
//     a chained join names its build side in a FOURTH place, and that one was
//     left pointing at the stage the pass had just deleted. Only a scan with
//     a PROJECTION or a pushed filter is absorbed, which is why the shape
//     needs a DERIVED arm: a plain base-table arm's scan is pass-through and
//     never fuses.
//
//   - #766 PROJECTING the arm's column through the chain hard-failed on the
//     shuffled arm while the COUNT twin of the identical query was correct —
//     "whatever carries the column for the predicate is not carrying it for
//     the SELECT list". Two sites: a scan's SHIPPED set (OutputColumns) is
//     narrowed by pruneScanOutputColumns before the outer SELECT list is
//     resolved back to a source column, and a join's PROJECTION could name an
//     alias no fragment computes without any check seeing it.
//
//   - #753 a derived table's COMPUTED column read above a join OF JOINS came
//     back NULL or holding another arm's value.
//
// Every expected value is PostgreSQL 17's over the decpair fixture loaded
// into a --locale=C database, not either engine's.
//
// Both arms of every same-alias pair publish at DECIMAL scale 2 on purpose. A
// cross-SCALE pair (`SUM(b) OVER ()` beside `a * 3`) renders one arm at the
// other's typmod on the single-process path — the values are right and the
// declared scale is not — and that is #754, a different mechanism this gate
// must not be about.
func TestDerivedArmsAboveAJoinChainThreeArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := dajArms(t, ctx)

	const tbl = dbpTable
	for _, tc := range []struct {
		name, sql string
		cols      []string
		want      string // PostgreSQL 17, whole result
	}{
		// --- #755: a chained join over DERIVED arms on the shuffled arm.
		{
			name: "755/base-between-two-derived-arms",
			sql: "SELECT p.w AS pw, q.w AS qw FROM (SELECT id, a + 1 AS w FROM " + tbl + ") p " +
				"JOIN " + tbl + " y ON p.id = y.id " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") q ON p.id = q.id ORDER BY p.id",
			cols: []string{"pw", "qw"},
			want: "9 rows: 13.75|38.25;13.75|38.25;13.75|38.25;0.99|-0.03;3.00|6.00;1.00|0.00;|;13.75|38.25;|;",
		},
		{
			name: "755/three-derived-arms-distinct-aliases",
			sql: "SELECT p.w1 AS pw, q.w2 AS qw, r.w3 AS rw FROM " +
				"(SELECT id, a + 1 AS w1 FROM " + tbl + ") p " +
				"JOIN (SELECT id, a * 3 AS w2 FROM " + tbl + ") q ON p.id = q.id " +
				"JOIN (SELECT id, a * 5 AS w3 FROM " + tbl + ") r ON p.id = r.id ORDER BY p.id",
			cols: []string{"pw", "qw", "rw"},
			want: "9 rows: 13.75|38.25|63.75;13.75|38.25|63.75;13.75|38.25|63.75;0.99|-0.03|-0.05;" +
				"3.00|6.00|10.00;1.00|0.00|0.00;||;13.75|38.25|63.75;||;",
		},
		{
			name: "755/window-arm-base-between-computed-arm",
			sql: "SELECT p.w AS pw, q.w AS qw FROM (SELECT id, SUM(a) OVER () AS w FROM " + tbl + ") p " +
				"JOIN " + tbl + " y ON p.id = y.id " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") q ON p.id = q.id ORDER BY p.id",
			cols: []string{"pw", "qw"},
			want: "9 rows: 52.99|38.25;52.99|38.25;52.99|38.25;52.99|-0.03;52.99|6.00;52.99|0.00;" +
				"52.99|;52.99|38.25;52.99|;",
		},
		{
			name: "755/wrapped-window-arm-base-between-computed-arm",
			sql: "SELECT x.w AS xw, z.w AS zw FROM " +
				"(SELECT id, SUM(a) OVER () + 0 AS w FROM " + tbl + ") x " +
				"JOIN " + tbl + " y ON x.id = y.id " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") z ON x.id = z.id ORDER BY x.id",
			cols: []string{"xw", "zw"},
			want: "9 rows: 52.99|38.25;52.99|38.25;52.99|38.25;52.99|-0.03;52.99|6.00;52.99|0.00;" +
				"52.99|;52.99|38.25;52.99|;",
		},
		{
			// The CONTROLS #755 names: the TWO-way spelling of the same
			// query, and a three-way join of BASE tables. Both succeeded on
			// every arm throughout, which is what said the trigger was a
			// chained join over DERIVED arms and not the chain itself.
			name: "755/control/two-way-derived",
			sql: "SELECT p.w AS pw, q.w AS qw FROM (SELECT id, a + 1 AS w FROM " + tbl + ") p " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") q ON p.id = q.id ORDER BY p.id",
			cols: []string{"pw", "qw"},
			want: "9 rows: 13.75|38.25;13.75|38.25;13.75|38.25;0.99|-0.03;3.00|6.00;1.00|0.00;|;13.75|38.25;|;",
		},
		{
			name: "755/control/three-way-base-tables",
			sql: "SELECT x.a AS xa, y.a AS ya, z.a AS za FROM " + tbl + " x " +
				"JOIN " + tbl + " y ON x.id = y.id JOIN " + tbl + " z ON x.id = z.id ORDER BY x.id",
			cols: []string{"xa", "ya", "za"},
			want: "9 rows: 12.75|12.75|12.75;12.75|12.75|12.75;12.75|12.75|12.75;-0.01|-0.01|-0.01;" +
				"2.00|2.00|2.00;0.00|0.00|0.00;||;12.75|12.75|12.75;||;",
		},

		// --- #753: a COMPUTED column above a join OF JOINS.
		{
			name: "753/two-derived-arms-plus-base-table",
			sql: "SELECT p.w AS pw, q.w AS qw FROM (SELECT id, a + 1 AS w FROM " + tbl + ") p " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") q ON p.id = q.id " +
				"JOIN " + tbl + " y ON p.id = y.id ORDER BY p.id",
			cols: []string{"pw", "qw"},
			want: "9 rows: 13.75|38.25;13.75|38.25;13.75|38.25;0.99|-0.03;3.00|6.00;1.00|0.00;|;13.75|38.25;|;",
		},
		{
			name: "753/three-derived-arms-one-alias",
			sql: "SELECT p.w AS pw, q.w AS qw, r.w AS rw FROM " +
				"(SELECT id, a + 1 AS w FROM " + tbl + ") p " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") q ON p.id = q.id " +
				"JOIN (SELECT id, a * 5 AS w FROM " + tbl + ") r ON p.id = r.id ORDER BY p.id",
			cols: []string{"pw", "qw", "rw"},
			want: "9 rows: 13.75|38.25|63.75;13.75|38.25|63.75;13.75|38.25|63.75;0.99|-0.03|-0.05;" +
				"3.00|6.00|10.00;1.00|0.00|0.00;||;13.75|38.25|63.75;||;",
		},
		{
			// Three WINDOW arms, which is how #753 was found. Three DISTINCT
			// window values, because two equal ones make the collapse
			// invisible.
			name: "753/three-window-arms-one-alias",
			sql: "SELECT p.w AS pw, q.w AS qw, r.w AS rw FROM " +
				"(SELECT id, SUM(a) OVER () AS w FROM " + tbl + ") p " +
				"JOIN (SELECT id, MIN(a) OVER () AS w FROM " + tbl + ") q ON p.id = q.id " +
				"JOIN (SELECT id, MAX(a) OVER () AS w FROM " + tbl + ") r ON p.id = r.id ORDER BY p.id",
			cols: []string{"pw", "qw", "rw"},
			want: "9 rows: 52.99|-0.01|12.75;52.99|-0.01|12.75;52.99|-0.01|12.75;52.99|-0.01|12.75;" +
				"52.99|-0.01|12.75;52.99|-0.01|12.75;52.99|-0.01|12.75;52.99|-0.01|12.75;52.99|-0.01|12.75;",
		},
		{
			// The control #753 names: three arms selecting a BASE column with
			// no rename at all.
			name: "753/control/three-arms-no-rename",
			sql: "SELECT p.a AS pw, q.a AS qw, r.a AS rw FROM " +
				"(SELECT id, a FROM " + tbl + ") p JOIN (SELECT id, a FROM " + tbl + ") q " +
				"ON p.id = q.id JOIN (SELECT id, a FROM " + tbl + ") r ON p.id = r.id ORDER BY p.id",
			cols: []string{"pw", "qw", "rw"},
			want: "9 rows: 12.75|12.75|12.75;12.75|12.75|12.75;12.75|12.75|12.75;-0.01|-0.01|-0.01;" +
				"2.00|2.00|2.00;0.00|0.00|0.00;||;12.75|12.75|12.75;||;",
		},

		// --- #766: PROJECTING the column, whose COUNT twin was already right.
		{
			name: "766/computed-over-nested-aggregate/projecting",
			sql: "WITH c AS (SELECT id, sv * 2 AS dv FROM " +
				"(SELECT id, SUM(a) AS sv FROM " + tbl + " GROUP BY id) z) " +
				"SELECT c.dv AS d FROM c JOIN " + tbl + " t ON c.id = t.id " +
				"JOIN " + tbl + " u ON c.id = u.id WHERE c.dv > 1 ORDER BY c.id",
			cols: []string{"d"},
			want: "5 rows: 25.50;25.50;25.50;4.00;25.50;",
		},
		{
			name: "766/two-arms-one-alias/projecting",
			sql: "SELECT x.w AS xw FROM (SELECT id, a AS w FROM " + tbl + ") x " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") z ON x.id = z.id " +
				"JOIN " + tbl + " u ON x.id = u.id WHERE x.w > 1 ORDER BY x.id",
			cols: []string{"xw"},
			want: "5 rows: 12.75;12.75;12.75;2.00;12.75;",
		},
		{
			name: "766/computed-over-window/projecting",
			sql: "WITH c AS (SELECT id, SUM(f) OVER () + 0 AS dv FROM " + tbl + ") " +
				"SELECT c.dv AS d FROM c JOIN " + tbl + " t ON c.id = t.id " +
				"JOIN " + tbl + " u ON c.id = u.id WHERE c.dv > 1 ORDER BY c.id",
			cols: []string{"d"},
			want: "9 rows: 138.75;138.75;138.75;138.75;138.75;138.75;138.75;138.75;138.75;",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused a query PostgreSQL 17 answers: %v\n  SQL: %s",
						arm.name, err, tc.sql)
				}
				if got := dajDigest(res, tc.cols); got != tc.want {
					t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}

	// #766's own spelling of the two-arm shape, whose arms publish at
	// DIFFERENT DECIMAL scales (`a` is (9,2), `b` is (18,4)). It is the entry
	// that exercises the scan's SHIPPED set: `x.w > 1` pushes `a > 1` onto x's
	// scan, pruneScanOutputColumns then ships only `id`, and the SELECT list
	// resolved back to `a` — `column "a" does not exist in the input schema`,
	// at dispatch, on the shuffled arm.
	//
	// It used to be kept apart from the entries above because it also carried
	// #754: on the SINGLE path, two arms publishing one alias made x's scale-2
	// column render at z's scale 4 — right values, wrong typmod, and that arm
	// was pinned at the wrong rendering. It is the same disagreement as #706
	// and it is closed: the VALUE came through `x.w` and the DECLARATION
	// through the first bare `w`, which is z's column.
	t.Run("766/two-arms-at-different-decimal-scales/projecting", func(t *testing.T) {
		sql := "SELECT x.w AS xw FROM (SELECT id, a AS w FROM " + tbl + ") x " +
			"JOIN (SELECT id, b AS w FROM " + tbl + ") z ON x.id = z.id " +
			"JOIN " + tbl + " u ON x.id = u.id WHERE x.w > 1 ORDER BY x.id"
		const want = "5 rows: 12.75;12.75;12.75;2.00;12.75;" // PostgreSQL 17
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused a query PostgreSQL 17 answers: %v\n  SQL: %s",
					arm.name, err, sql)
			}
			if got := dajDigest(res, []string{"xw"}); got != want {
				t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
					arm.name, got, want, sql)
			}
		}
	})

	// The COUNT twins of the three projecting shapes. #766's asymmetry was
	// exactly that these were CORRECT on all three arms while the projecting
	// forms failed, so they are controls rather than assertions of the fix —
	// and a repair that broke them would be the trade this gate exists to
	// refuse.
	for _, tc := range []struct {
		name, sql string
		want      int64
	}{
		{"766/control/computed-over-nested-aggregate/count",
			"WITH c AS (SELECT id, sv * 2 AS dv FROM " +
				"(SELECT id, SUM(a) AS sv FROM " + tbl + " GROUP BY id) z) " +
				"SELECT COUNT(*) AS n FROM c JOIN " + tbl + " t ON c.id = t.id " +
				"JOIN " + tbl + " u ON c.id = u.id WHERE c.dv > 1", 5},
		{"766/control/two-arms-one-alias/count",
			"SELECT COUNT(*) AS n FROM (SELECT id, a AS w FROM " + tbl + ") x " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") z ON x.id = z.id " +
				"JOIN " + tbl + " u ON x.id = u.id WHERE x.w > 1", 5},
		{"766/control/computed-over-window/count",
			"WITH c AS (SELECT id, SUM(f) OVER () + 0 AS dv FROM " + tbl + ") " +
				"SELECT COUNT(*) AS n FROM c JOIN " + tbl + " t ON c.id = t.id " +
				"JOIN " + tbl + " u ON c.id = u.id WHERE c.dv > 1", 9},
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
				if len(got) != 1 || got[0] != tc.want {
					t.Errorf("%s arm answered %v, PostgreSQL 17 answers %d\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}

// A CTE with a COMPUTED body read from the LAST position of a join chain, on
// three arms (#755 round 2, #762).
//
// This is the cell three spellings of one query disagreed about. A chained
// link's own `Columns` narrows the JOINED stream, and the residual filter the
// link carries runs AFTER it — so the build-side half of that filter has to be
// in the list, and `probeSideChainRefs` deliberately drops it. For a COMPUTED
// alias the pruner publishes `dv`, the re-spelled predicate reads `a`, the
// link dropped `a`, and the filter was UNKNOWN on every row:
//
//	WITH c AS (SELECT id, a * 2 AS dv FROM decpair)
//	SELECT COUNT(*) FROM decpair t JOIN decpair u ON t.id = u.id
//	JOIN c ON c.id = t.id WHERE c.dv > 1
//	-- PostgreSQL 5 · single 5 · DAG broadcast 5 · DAG SHUFFLED 0
//
// The RENAME body is correct because the pruner resolves `dv` back to `a` and
// the list already had it; the DERIVED spelling is correct because the
// predicate is pushed INTO the arm's scan, which a CTE's Project — a
// materialization fence — declines. Both are carried below as controls.
//
// On 376b2cac the shuffled arm REFUSED these plans outright (`chained join 0:
// build dep "exchange-repartition-7" output not found`, #755); the rewiring
// that made them runnable is what exposed the carrier. That is why the probe
// table below is a table: a COUNT cannot tell a dropped predicate from one
// that is UNKNOWN, and IS NULL / IS NOT NULL / a disjunct with a base-table
// term each answer differently under the two readings.
func TestCTEChainPositionCarriesItsFilterThreeArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := dajArms(t, ctx)

	const tbl = dbpTable
	// decpair: `a` is DECIMAL(9,2), NULL on ids 7 and 9. `a * 2 > 1` selects
	// five of the nine rows and `IS NULL` selects the two NULL ones, so a
	// predicate that is UNKNOWN on every row (0 and 9 respectively) is
	// distinguishable from a dropped one.
	cte := "WITH c AS (SELECT id, a * 2 AS dv FROM " + tbl + ") "
	chain2 := tbl + " t JOIN " + tbl + " u ON t.id = u.id JOIN c ON c.id = t.id "
	chain3 := tbl + " t JOIN " + tbl + " u ON t.id = u.id JOIN " + tbl +
		" w2 ON t.id = w2.id JOIN c ON c.id = t.id "
	chain4 := tbl + " t JOIN " + tbl + " u ON t.id = u.id JOIN " + tbl +
		" w2 ON t.id = w2.id JOIN " + tbl + " w3 ON t.id = w3.id JOIN c ON c.id = t.id "

	for _, tc := range []struct {
		name, sql string
		want      int64
	}{
		{"2join/qualified", cte + "SELECT COUNT(*) AS n FROM " + chain2 + "WHERE c.dv > 1", 5},
		{"2join/bare", cte + "SELECT COUNT(*) AS n FROM " + chain2 + "WHERE dv > 1", 5},
		{"3join/qualified", cte + "SELECT COUNT(*) AS n FROM " + chain3 + "WHERE c.dv > 1", 5},
		{"4join/qualified", cte + "SELECT COUNT(*) AS n FROM " + chain4 + "WHERE c.dv > 1", 5},
		// The reviewer's probe table: each of these answers a different
		// number under "predicate applied" and "predicate UNKNOWN".
		{"2join/is-null", cte + "SELECT COUNT(*) AS n FROM " + chain2 + "WHERE c.dv IS NULL", 2},
		{"2join/is-not-null", cte + "SELECT COUNT(*) AS n FROM " + chain2 + "WHERE c.dv IS NOT NULL", 7},
		{"2join/range", cte + "SELECT COUNT(*) AS n FROM " + chain2 + "WHERE c.dv < 1000000", 7},
		// A disjunct with a BASE-TABLE term: under UNKNOWN only the
		// base-table half survives and the answer is 1, not 6.
		{"2join/or-disjunct", cte + "SELECT COUNT(*) AS n FROM " + chain2 +
			"WHERE c.dv > 1 OR t.id = 4", 6},
		// The BODY over each of the fixture's three numeric columns, because
		// the re-spelled predicate names whichever source column the body
		// reads and the carrier has to find it whatever its type.
		{"2join/body-b", "WITH c AS (SELECT id, b * 2 AS dv FROM " + tbl + ") " +
			"SELECT COUNT(*) AS n FROM " + chain2 + "WHERE c.dv > 1", 5},
		{"2join/body-f", "WITH c AS (SELECT id, f * 2 AS dv FROM " + tbl + ") " +
			"SELECT COUNT(*) AS n FROM " + chain2 + "WHERE c.dv > 1", 6},
		// CONTROLS: the two spellings that were right throughout, which is
		// what localised the cell to CTE x computed x last x shuffled.
		{"control/2join/rename-body", "WITH c AS (SELECT id, a AS dv FROM " + tbl + ") " +
			"SELECT COUNT(*) AS n FROM " + chain2 + "WHERE c.dv > 1", 5},
		{"control/2join/derived-spelling", "SELECT COUNT(*) AS n FROM " + tbl + " t JOIN " +
			tbl + " u ON t.id = u.id JOIN (SELECT id, a * 2 AS dv FROM " + tbl + ") c " +
			"ON c.id = t.id WHERE c.dv > 1", 5},
		{"control/2join/cte-first", cte + "SELECT COUNT(*) AS n FROM c JOIN " + tbl +
			" t ON c.id = t.id JOIN " + tbl + " u ON c.id = u.id WHERE c.dv > 1", 5},
		{"control/2join/cte-build", cte + "SELECT COUNT(*) AS n FROM " + tbl +
			" t JOIN c ON c.id = t.id JOIN " + tbl + " u ON c.id = u.id WHERE c.dv > 1", 5},
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
				if len(got) != 1 || got[0] != tc.want {
					t.Errorf("%s arm answered %v, PostgreSQL 17 answers %d — a chained link "+
						"dropped the column its own residual filter reads\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}

	// The PROJECTING form, which was correct throughout because the
	// projection forces the value to be carried, and the AGGREGATE form,
	// which reads it a third way.
	t.Run("2join/projecting", func(t *testing.T) {
		sql := cte + "SELECT c.id AS cid, c.dv AS dv FROM " + chain2 +
			"WHERE c.dv > 1 ORDER BY c.id"
		const want = "5 rows: 1|25.50;2|25.50;3|25.50;5|4.00;8|25.50;"
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			if got := dajDigest(res, []string{"cid", "dv"}); got != want {
				t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
					arm.name, got, want, sql)
			}
		}
	})
	t.Run("2join/min-and-count", func(t *testing.T) {
		// The FLOAT body on purpose. Over the DECIMAL one, MIN/SUM of a
		// derived alias declares FLOAT64 for a task whose partition is empty
		// and DECIMAL for one that is not, and the shuffle read refuses the
		// two files — `column "m" is DECIMAL … but FLOAT64 in an earlier file
		// of the same stage`. That reproduces on 376b2cac for the CTE-FIRST
		// and the ONE-JOIN spellings, which this change does not touch; it is
		// an aggregate-output typing residual and not this carrier.
		sql := "WITH c AS (SELECT id, f * 2 AS dv FROM " + tbl + ") " +
			"SELECT MIN(c.dv) AS m, COUNT(*) AS n FROM " + chain2 + "WHERE c.dv > 1"
		const want = "1 rows: 3|6;"
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			if got := dajDigest(res, []string{"m", "n"}); got != want {
				t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
					arm.name, got, want, sql)
			}
		}
	})

	// GROUP BY on the QUALIFIED derived alias, which both DAG arms used to
	// collapse into ONE NULL group. Identical on 376b2cac, identical at ONE
	// join and in the DERIVED spelling, and correct in the BARE spelling — so
	// it was the qualified GROUP-BY key's own resolution and not the chain.
	//
	// The MECHANISM, and what closed it: `Stage.GroupByCols` was
	// simultaneously what the worker COMPUTES the key from and what it
	// PUBLISHES it as, and a key naming a CTE's computed alias needs those to
	// be two different strings — the defining expression `a * 2` is spelled
	// over a column the join does not carry, while the alias is what the join
	// stream really holds. ADR-0026 §2 carries both now: the CTE's projection
	// materializes `dv` on its scan's fragment, the join ships it (bare where
	// nothing contests the name, qualified `c.dv` where the shuffled arm's
	// second join does), and `resolveStageGroupKeys` names it.
	//
	// It asserts the DISPOSITION beside the rows on both DAG arms: this key is
	// EXECUTED on the DAG, and a refusal that started firing on it would be a
	// right-to-routed regression the rows cannot see.
	t.Run("2join/group-by-qualified-alias", func(t *testing.T) {
		sql := cte + "SELECT c.dv AS dv, COUNT(*) AS n FROM " + chain2 + "GROUP BY c.dv ORDER BY c.dv"
		const want = "5 rows: -0.02|1;0.00|1;4.00|1;25.50|4;|2;" // PostgreSQL 17
		for _, arm := range arms {
			routesBefore := int64(0)
			if arm.coord != nil {
				routesBefore = arm.coord.GroupKeyLocalRoutes()
			}
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			if got := dajDigest(res, []string{"dv", "n"}); got != want {
				t.Errorf("the %s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
					arm.name, got, want, sql)
			}
			if arm.coord != nil && arm.coord.GroupKeyLocalRoutes() != routesBefore {
				t.Errorf("the %s arm ROUTED this shape to the coordinator-local pipeline. The "+
					"answer is right either way, which is why this has to be asserted: the DAG "+
					"resolves this key against the CTE arm's own projection, and a group-key "+
					"refusal that starts firing on it is a right-to-routed regression in kind "+
					"(#794/#795)\n  SQL: %s", arm.name, sql)
			}
		}
	})
	// …and the BARE spelling beside it, which is correct on every arm and is
	// what says the qualifier is the whole of that residual.
	//
	// It asserts that the DAG EXECUTES it, not only that the answer is right.
	// This subtest is where a right-to-refused-routed move hid through a whole
	// review round: a group-key pass read an exchange's empty column list as
	// "the stream no longer carries this name", refused a key the DAG had been
	// evaluating correctly, and the coordinator-local pipeline answered. The
	// rows never moved. The mechanism did.
	t.Run("2join/group-by-bare-alias", func(t *testing.T) {
		sql := cte + "SELECT dv, COUNT(*) AS n FROM " + chain2 + "GROUP BY dv ORDER BY dv"
		const want = "5 rows: -0.02|1;0.00|1;4.00|1;25.50|4;|2;"
		for _, arm := range arms {
			routesBefore := int64(0)
			if arm.coord != nil {
				routesBefore = arm.coord.GroupKeyLocalRoutes()
			}
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			if arm.coord != nil && arm.coord.GroupKeyLocalRoutes() != routesBefore {
				t.Errorf("the %s arm ROUTED this shape to the coordinator-local pipeline. The "+
					"answer is right either way, which is why this has to be asserted: the DAG "+
					"executes this key, and a group-key refusal that starts firing on it is a "+
					"right-to-routed regression in kind (#795)\n  SQL: %s", arm.name, sql)
			}
			if got := dajDigest(res, []string{"dv", "n"}); got != want {
				t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
					arm.name, got, want, sql)
			}
		}
	})
}

// A ROW FIELD PATH read across a JOIN, on three arms.
//
// ADR-0022: `c_row.b` is not a column reference — the qualifier IS the column
// and the expression compiler resolves the field out of the ROW. Three lists
// spelled it as if it were a column and dropped the container with it: the
// logical pruner's join partition (neither side publishes the dotted name, so
// the need went to NEITHER side and no scan read `c_row`), the join stage's
// OutputFilter on the DAG, and the single-process probe's OutputFilter. Every
// field came back NULL, or the reference bound to whatever scalar column
// happened to end in `.b`:
//
//	SELECT x.id, c_row.b FROM typemx_nested x JOIN typemx_nested y ON x.id = y.id
//	-- PostgreSQL 17 rejects it (no FROM-clause entry for c_row); wadjet
//	--   answers the field, which is the deliberate superset
//	-- 376b2cac: single all-NULL, both DAG arms `column "c_row.b" does not
//	--   exist in the input schema`
//
// The same query with NO join answers correctly on 376b2cac, because nothing
// narrows there — which is what says the join's lists are the site.
func TestRowFieldPathSurvivesAJoinThreeArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := dajArms(t, ctx)

	nested := typematrix.Nested
	for _, tc := range []struct {
		name, sql string
		cols      []string
		want      string
		// refuse, when set, is a substring of the refusal every arm must
		// raise instead of answering — the shape PostgreSQL refuses too.
		refuse string
		// armErr pins ONE arm's loud failure where the others answer: the
		// named arm must fail with this substring and every other arm must
		// give `want`. It is a pin, so it FAILS the day that arm answers —
		// including the day it answers PostgreSQL's rows, which is the fix
		// and wants the entry deleted rather than kept.
		armErr map[string]string
	}{
		{
			// #769: a join arm that publishes a column of the FIELD's own
			// name. `decpair.b` is a DECIMAL and `c_row.b` is an INT64 field,
			// so the two values can never be confused for one another — every
			// arm answered decpair's, silently, because every resolver in the
			// engine STRIPPED the qualifier before asking whether `c_row`
			// names a ROW column of the batch.
			//
			// PostgreSQL 17 refuses the unparenthesised spelling outright
			// (`missing FROM-clause entry for table "c_row"`, 42P01) and
			// requires `(n.c_row).b`; wadjet ANSWERS it, which is the
			// deliberate superset ADR-0012 records. The Want here is what
			// PostgreSQL answers for the PARENTHESISED spelling over the same
			// rows — the field, not the arm's column.
			name: "join-arm-publishes-the-field-name",
			sql: "SELECT n.id AS nid, c_row.b AS fb FROM " + nested +
				" n JOIN decpair d ON n.id = d.id ORDER BY n.id",
			cols: []string{"nid", "fb"},
			want: "9 rows: 1|11;2|;3|;4|44;5|55;6|66;7|77;8|88;9|;",
		},
		{
			// The same collision with BOTH columns selected, so a resolver
			// that answered the arm's `b` for the field path cannot pass by
			// returning one plausible column: the two must differ per row.
			name: "join-arm-publishes-the-field-name/both-selected",
			sql: "SELECT n.id AS nid, c_row.b AS fb, d.b AS db FROM " + nested +
				" n JOIN decpair d ON n.id = d.id ORDER BY n.id",
			cols: []string{"nid", "fb", "db"},
			want: "9 rows: 1|11|12.7500;2||12.7501;3||12.7499;4|44|-0.0100;5|55|10.0000;" +
				"6|66|0.0000;7|77|1.0000;8|88|;9||;",
		},
		{
			// And the same field path in a PREDICATE. The vectorized filters
			// resolve a dotted name for themselves, and they stripped the
			// qualifier too: `WHERE c_row.b IS NULL` counted the ARM's NULLs
			// (2) where the field has three.
			name: "join-arm-publishes-the-field-name/in-a-predicate",
			sql: "SELECT COUNT(*) AS n FROM " + nested +
				" n JOIN decpair d ON n.id = d.id WHERE c_row.b IS NULL",
			cols: []string{"n"},
			want: "1 rows: 3;",
		},
		{
			// The CONTROL that bounds the reorder: a QUALIFIED reference whose
			// qualifier names a relation and not a container still resolves
			// against the relation. `d.b` and `n.id` are ordinary columns and
			// must be unaffected by the field-path rule running first.
			name: "join-arm-publishes-the-field-name/ctl-ordinary-qualified-refs",
			sql: "SELECT n.id AS nid, d.b AS db FROM " + nested +
				" n JOIN decpair d ON n.id = d.id WHERE d.b > 1 ORDER BY n.id",
			cols: []string{"nid", "db"},
			want: "4 rows: 1|12.7500;2|12.7501;3|12.7499;5|10.0000;",
		},
		{
			// And a field the container does NOT declare is REFUSED, the way
			// PostgreSQL 17 refuses `(n.c_row).nosuch` — `column "nosuch" not
			// found in data type e3_rowt`. The reorder is gated on the
			// container DECLARING the field, so this disposition is the one
			// the shape already had and the gate says so.
			name: "join-arm-publishes-the-field-name/ctl-a-field-the-row-does-not-declare",
			sql: "SELECT n.id AS nid, c_row.nosuch AS fb FROM " + nested +
				" n JOIN decpair d ON n.id = d.id WHERE n.id < 3 ORDER BY n.id",
			cols:   []string{"nid", "fb"},
			refuse: "could not identify column",
		},
		{
			// The GROUP KEY spelling, and the seventh resolver.
			//
			// A field path used as a group key is MATERIALIZED by the fragment
			// (ADR-0022 rule 2), so the CONTAINER has to survive the join's
			// OutputFilter — and it did not: `ensureJoinCarriesEvaluatedColumns`
			// declines to expand `c_row.b` into `c_row` when the dotted name
			// looks "produced", and the aggregate PUBLISHES its key under
			// exactly that name. The fragment then evaluated the field against
			// a stream with no container: one NULL group where the other arm
			// has no column of the field's name, and the ARM's column where it
			// has one — which #361's silent-write guard turned into a task
			// failure once the DECLARATION started coming from the field.
			name: "join-arm-publishes-the-field-name/as-a-group-key",
			sql: "SELECT c_row.b AS k, COUNT(*) AS n FROM " + nested +
				" n JOIN decpair d ON n.id = d.id GROUP BY c_row.b ORDER BY 1",
			cols: []string{"k", "n"},
			want: "7 rows: 11|1;44|1;55|1;66|1;77|1;88|1;|3;",
		},
		{
			// The AGGREGATE-ARGUMENT spelling of the same rule, with a HAVING
			// over it so the value is read twice.
			name: "join-arm-publishes-the-field-name/as-an-aggregate-argument",
			sql: "SELECT n.id AS i, MIN(c_row.b) AS m FROM " + nested +
				" n JOIN decpair d ON n.id = d.id GROUP BY n.id " +
				"HAVING MIN(c_row.b) IS NOT NULL ORDER BY 1",
			cols: []string{"i", "m"},
			want: "6 rows: 1|11;4|44;5|55;6|66;7|77;8|88;",
		},
		{
			// The same group key over an arm that has NO column of the field's
			// name. Nothing can be bound by accident here, so a missing
			// container shows as ONE NULL group rather than as another
			// relation's values — the shape that says the container really is
			// the thing the payload has to carry.
			name: "join-arm-publishes-the-field-name/group-key-over-an-arm-without-the-name",
			sql: "SELECT c_row.b AS k, COUNT(*) AS n FROM " + nested +
				" x JOIN " + typematrix.Dim + " d ON x.id = d.k GROUP BY c_row.b ORDER BY 1",
			cols: []string{"k", "n"},
			want: "7 rows: 0|1;11|1;44|1;55|1;66|1;77|1;|2;",
		},
		{
			// The CONTROL that bounds it: no join, right on every arm before
			// and after.
			name: "join-arm-publishes-the-field-name/ctl-group-key-with-no-join",
			sql: "SELECT c_row.b AS k, COUNT(*) AS n FROM " + nested +
				" WHERE id < 5 GROUP BY c_row.b ORDER BY 1",
			cols: []string{"k", "n"},
			want: "4 rows: 0|1;11|1;44|1;|2;",
		},
		{
			// The CONTROL that bounds it: no join, correct on 376b2cac.
			name: "no-join",
			sql:  "SELECT id AS xid, c_row.b AS fb FROM " + nested + " WHERE id < 5 ORDER BY id",
			cols: []string{"xid", "fb"},
			want: "5 rows: 0|0;1|11;2|;3|;4|44;",
		},
		{
			// A SELF-JOIN puts a container of the same name on BOTH arms, so
			// the path names no one value and it is REFUSED. PostgreSQL says
			// the same for the spelling it reads as a field path — measured:
			//
			//	SELECT x.id, (c_row).b FROM n x JOIN n y ON x.id = y.id + 1
			//	ERROR:  column reference "c_row" is ambiguous      (42702)
			//
			// It answered ONE arm's container until 2026-09-04, and which one
			// depended on the plan shape. A container is a column and the
			// ambiguity is the container's, so the message names the
			// QUALIFIER.
			name: "self-join",
			sql: "SELECT x.id AS xid, c_row.b AS fb FROM " + nested + " x JOIN " + nested +
				" y ON x.id = y.id WHERE x.id < 5 ORDER BY x.id",
			cols:   []string{"xid", "fb"},
			refuse: `column reference "c_row" is ambiguous`,
		},
		{
			// The DISCRIMINATING spelling of the same shape: the second arm's
			// rows are shifted by one id, so the two containers hold different
			// values at every row and no pick could pass by accident. It picked
			// the shifted arm's before.
			name: "self-join/shifted-arm",
			sql: "SELECT x.id AS xid, c_row.b AS fb FROM " + nested + " x JOIN " + nested +
				" y ON x.id = y.id + 1 WHERE x.id < 5 ORDER BY x.id",
			cols:   []string{"xid", "fb"},
			refuse: `column reference "c_row" is ambiguous`,
		},
		{
			// The CONTROL for the two ambiguous entries below: the same rows
			// they return, read WITHOUT a join, so their answer can be told
			// apart from x's own. x's `c_row.b` at ids 1-4 is 11, NULL, NULL,
			// 44 — and the ambiguous entries answer 0, 11, NULL, NULL.
			name: "no-join/ids-1-to-4",
			sql:  "SELECT id AS xid, c_row.b AS fb FROM " + nested + " WHERE id > 0 AND id < 5 ORDER BY id",
			cols: []string{"xid", "fb"},
			want: "4 rows: 1|11;2|;3|;4|44;",
		},
		{
			// AMBIGUOUS on purpose, and DISCRIMINATING: both arms carry a
			// `c_row`, and the derived arm's rows are SHIFTED by one id, so
			// the two containers hold different values at every row and no
			// pick could pass by accident. It picked the derived arm's — 0,
			// 11, NULL, NULL against x's own 11, NULL, NULL, 44 — until
			// 2026-09-04.
			//
			// It is REFUSED now. The earlier note here read PostgreSQL's
			// answer as "rejects the query outright (no FROM-clause entry for
			// c_row)" and concluded there was nothing to follow; the
			// PARENTHESISED spelling, which is the one PostgreSQL reads as a
			// field path, says `column reference "c_row" is ambiguous` (42702)
			// over exactly this shape. So there IS an answer to follow, and
			// four documents that already claimed the refusal now describe the
			// engine (#769 round 2).
			name: "ambiguous-container/shifted-arm-is-the-build",
			sql: "SELECT x.id AS xid, c_row.b AS fb FROM " + nested + " x JOIN " +
				"(SELECT id + 1 AS id, c_row FROM " + nested + ") y ON x.id = y.id " +
				"WHERE x.id < 5 ORDER BY x.id",
			cols:   []string{"xid", "fb"},
			refuse: `column reference "c_row" is ambiguous`,
		},
		{
			// The same query with the FROM order swapped, so the shifted arm
			// is the PROBE. The refusal does not follow probe/build either.
			name: "ambiguous-container/shifted-arm-is-the-probe",
			sql: "SELECT x.id AS xid, c_row.b AS fb FROM " +
				"(SELECT id + 1 AS id, c_row FROM " + nested + ") y JOIN " + nested +
				" x ON x.id = y.id WHERE x.id < 5 ORDER BY x.id",
			cols:   []string{"xid", "fb"},
			refuse: `column reference "c_row" is ambiguous`,
		},
		{
			// PostgreSQL's own spelling for a ROW field path, which this
			// engine now parses: `(c_row).b` is the SAME reference as the bare
			// `c_row.b` — one node, one resolver, one `batch.RowFieldPath`
			// question — rather than a second spelling with its own
			// resolution. Same rows as `join-arm-publishes-the-field-name`
			// above, which is the point: the two spellings agree.
			name: "parenthesised/unqualified-is-the-same-reference",
			sql: "SELECT n.id AS nid, (c_row).b AS fb FROM " + nested +
				" n JOIN decpair d ON n.id = d.id ORDER BY n.id",
			cols: []string{"nid", "fb"},
			want: "9 rows: 1|11;2|;3|;4|44;5|55;6|66;7|77;8|88;9|;",
		},
		{
			// The same spelling in a PREDICATE and as a GROUP KEY, because the
			// resolvers ADR-0022 binds together are per-CLAUSE and a spelling
			// that arrived at only one of them would pass the projection cell
			// alone.
			name: "parenthesised/in-a-predicate-and-a-group-key",
			sql: "SELECT (c_row).b AS fb, COUNT(*) AS n FROM " + nested +
				" WHERE id < 5 AND (c_row).b IS NOT NULL GROUP BY (c_row).b " +
				"ORDER BY (c_row).b",
			cols: []string{"fb", "n"},
			want: "3 rows: 0|1;11|1;44|1;",
		},
		{
			// A field the container does not declare, through the
			// parenthesised spelling: refused with the wording #604 settled,
			// exactly as the bare spelling is. PostgreSQL 17 refuses it too
			// (`column "nosuch" not found in data type …`).
			name:   "parenthesised/unknown-field",
			sql:    "SELECT (c_row).nosuch FROM " + nested,
			cols:   []string{"nosuch"},
			refuse: `could not identify column "nosuch" in record data type`,
		},
		{
			// PostgreSQL's ESCAPE HATCH for the 42702 refusal above is to
			// QUALIFY the container — `(x.c_row).b`, which answers
			// `0,0 | 1,11 | 2, | 3, | 4,44` on PostgreSQL 17 over these rows.
			// This engine parses it and REFUSES it, loudly, because a
			// three-part identity is not something `plansql.ColRef`
			// ({Table, Column}) can carry: measured on an attempt, the
			// reference resolves to NULL on every arm with no join in the
			// query at all, since each declaration site keys the container by
			// its BARE name and the qualifier is stripped before the field is
			// asked for.
			//
			// The refusal is the point. A silent NULL where PostgreSQL answers
			// a value is the failure mode this whole arc exists to remove, and
			// naming the workaround in the message is what keeps the 42702
			// above from being a dead end. ADR-0022 carries the mechanism.
			name: "parenthesised/a-qualified-container-is-refused-not-answered",
			sql: "SELECT x.id AS xid, (x.c_row).b AS fb FROM " + nested + " x JOIN " + nested +
				" y ON x.id = y.id WHERE x.id < 5 ORDER BY x.id",
			cols:   []string{"xid", "fb"},
			refuse: "(x.c_row).b: a ROW field path names an UNQUALIFIED container here",
		},
		{
			// The same refusal with NO join anywhere, which is what says the
			// gap is the three-part identity and not the ambiguity: the
			// container is unambiguous here and the spelling is still refused.
			name:   "parenthesised/a-qualified-container-is-refused-without-a-join",
			sql:    "SELECT (x.c_row).b AS fb FROM " + nested + " x",
			cols:   []string{"fb"},
			refuse: "(x.c_row).b: a ROW field path names an UNQUALIFIED container here",
		},
		{
			// The NESTED spelling reaches the same rule for the same reason —
			// its container is itself a path — and is refused rather than
			// silently answering the outer field.
			name:   "parenthesised/a-nested-path-is-refused-not-answered",
			sql:    "SELECT ((c_row).rw).k AS fk FROM " + nested,
			cols:   []string{"fk"},
			refuse: "(c_row.rw).k: a ROW field path names an UNQUALIFIED container here",
		},
		{
			// The CONTROL that bounds the refusal: ONE arm publishes the
			// container, so nothing is ambiguous and the field answers.
			name: "ambiguous-container/ctl-one-arm-only",
			sql: "SELECT x.id AS xid, c_row.b AS fb FROM " + nested + " x JOIN " +
				typematrix.Dim + " d ON x.id = d.k WHERE x.id < 5 ORDER BY x.id",
			cols: []string{"xid", "fb"},
			want: "5 rows: 0|0;1|11;2|;3|;4|44;",
		},
		{
			// The OUTER-join face of the field-path join key. An outer join's
			// ON cannot be lifted above the join (that deletes the preserved
			// rows), so `routeOuterJoinOnResiduals` moves it to JoinFilter and
			// the executor evaluates it per candidate — and THAT resolver
			// stripped the qualifier, binding `c_row.b` to the build's own
			// `b`. The residual read `d.b = d.b`, was true for every
			// candidate, and the join returned the full cross product (84
			// rows) on all four arms where PostgreSQL returns 12. Silent, and
			// pre-existing at base in both spellings.
			//
			// PostgreSQL 17, measured over these exact rows: 12 / 9 / 20 for
			// LEFT / RIGHT / FULL, with the single match at `c_row.b = 0`
			// against `decpair.b = 0.0000`.
			name: "outer-join/left-with-a-field-path-key",
			sql: "SELECT n.id AS nid, d.b AS db FROM " + nested + " n LEFT JOIN decpair d " +
				"ON c_row.b = d.b WHERE n.id < 12 ORDER BY n.id",
			cols: []string{"nid", "db"},
			want: "12 rows: 0|0.0000;1|;2|;3|;4|;5|;6|;7|;8|;9|;10|;11|;",
		},
		{
			name: "outer-join/right-with-a-field-path-key",
			sql: "SELECT n.id AS nid, d.b AS db FROM " + nested + " n RIGHT JOIN decpair d " +
				"ON c_row.b = d.b ORDER BY d.id",
			cols: []string{"nid", "db"},
			want: "9 rows: |12.7500;|12.7501;|12.7499;|-0.0100;|10.0000;0|0.0000;|1.0000;|;|;",
		},
		{
			name: "outer-join/full-with-a-field-path-key",
			sql: "SELECT n.id AS nid, d.b AS db FROM " + nested + " n FULL JOIN decpair d " +
				"ON c_row.b = d.b WHERE n.id < 12 OR n.id IS NULL ORDER BY n.id, d.id",
			cols: []string{"nid", "db"},
			want: "20 rows: 0|0.0000;1|;2|;3|;4|;5|;6|;7|;8|;9|;10|;11|;" +
				"|12.7500;|12.7501;|12.7499;|-0.0100;|10.0000;|1.0000;|;|;",
		},
		{
			// The ARITHMETIC spelling under an outer join. It was NOT right at
			// base — the round-3 commit body said it was, on the strength of
			// the INNER measurement — and it is the one that shows the defect
			// is the residual RESOLVER and not the key extraction: this
			// spelling never was a key pair on any join kind.
			name: "outer-join/left-with-a-field-path-in-arithmetic",
			sql: "SELECT n.id AS nid, d.b AS db FROM " + nested + " n LEFT JOIN decpair d " +
				"ON c_row.b + 0 = d.b WHERE n.id < 12 ORDER BY n.id",
			cols: []string{"nid", "db"},
			want: "12 rows: 0|0.0000;1|;2|;3|;4|;5|;6|;7|;8|;9|;10|;11|;",
		},
		{
			// The CONTROL that says the mechanism is the FIELD PATH and not
			// outer joins with residuals in general: an ordinary residual
			// under the same join kind, right on every arm before and after.
			name: "outer-join/ctl-left-with-an-ordinary-residual",
			sql: "SELECT n.id AS nid, d.b AS db FROM " + nested + " n LEFT JOIN decpair d " +
				"ON n.id + 0 = d.id WHERE n.id < 12 ORDER BY n.id",
			cols: []string{"nid", "db"},
			want: "12 rows: 0|;1|12.7500;2|12.7501;3|12.7499;4|-0.0100;5|10.0000;" +
				"6|0.0000;7|1.0000;8|;9|;10|;11|;",
		},
		{
			// A field path on BOTH sides of an INNER key. Right on the
			// single-process and broadcast arms — PostgreSQL's eight rows —
			// and PINNED on the shuffled one, where the repartition's own
			// projection asks for `z.b` as a column name. It is LOUD there,
			// which is the difference from base: at base all three arms
			// answered a cross product, silently.
			//
			// The mechanism is the shuffle's projection, not this arc's
			// resolver: `z` is the derived arm's renamed CONTAINER and the
			// exchange stage carries columns by name, so the materialized
			// field never reaches it. Closing it is the same
			// carry-the-container question `rowContainersOf` answers for the
			// join, asked of an exchange-repartition stage.
			name: "both-sides-field-path-key",
			sql: "SELECT x.id AS xid, z.b AS zb FROM " + nested + " x JOIN (SELECT id, " +
				"c_row AS z FROM " + nested + " WHERE id < 12) y ON c_row.b = z.b " +
				"WHERE x.id < 12 ORDER BY x.id",
			cols: []string{"xid", "zb"},
			want: "8 rows: 0|0;1|11;4|44;5|55;6|66;7|77;8|88;11|121;",
			armErr: map[string]string{
				"dag-shuffled": `column "z.b" does not exist in the input schema`,
			},
		},
		{
			// The WORKED EXAMPLE from docs/data-types.md — the escape hatch
			// the 42702 refusal sends the reader to — driven as a gate so the
			// documentation cannot drift from the engine again. An earlier
			// text selected a name nothing published, and it failed on wadjet
			// AND on PostgreSQL.
			//
			// PostgreSQL 17 over the same shape: `0,0 | 1,11 | 2, | 3, | 4,44`.
			name: "docs-example/rename-the-other-arms-container-away",
			sql: "SELECT x.id AS xid, c_row.b AS fb FROM " + nested + " x JOIN (SELECT id, " +
				"c_row AS y_row FROM " + nested + ") y ON x.id = y.id WHERE x.id < 5 " +
				"ORDER BY x.id",
			cols: []string{"xid", "fb"},
			want: "5 rows: 0|0;1|11;2|;3|;4|44;",
			// The 512 KiB arm refuses this at PLAN time and always has: a join
			// on an EXPRESSION is executed as a CROSS join, which does not
			// spill (ADR-0006's routed-probe amendment, #832), so its build
			// must fit the budget and refuses loudly when it does not.
			// Pre-existing, unrelated to field paths, and LOUD.
		},
		{
			// The same example in PostgreSQL's own spelling, because the docs
			// table says both are accepted.
			name: "docs-example/rename-the-other-arms-container-away-parenthesised",
			sql: "SELECT x.id AS xid, (c_row).b AS fb FROM " + nested + " x JOIN (SELECT id, " +
				"c_row AS y_row FROM " + nested + ") y ON x.id = y.id WHERE x.id < 5 " +
				"ORDER BY x.id",
			cols: []string{"xid", "fb"},
			want: "5 rows: 0|0;1|11;2|;3|;4|44;",
		},
		{
			// Redundant parentheses are the same reference (P2, round 3):
			// PostgreSQL answers `((c_row)).b` and this engine refused it as
			// "not a composite type", which was false about a container.
			name: "parenthesised/redundant-parentheses-are-the-same-reference",
			sql:  "SELECT id AS xid, ((c_row)).b AS fb FROM " + nested + " WHERE id < 5 ORDER BY id",
			cols: []string{"xid", "fb"},
			want: "5 rows: 0|0;1|11;2|;3|;4|44;",
		},
		{
			// Field notation on a SCALAR column. PostgreSQL 17 raises 42809
			// `column notation .x applied to type numeric, which is not a
			// composite type`; this engine answered one NULL per row, on every
			// arm — #604's disposition for a container that does not declare
			// the field, reached by a qualifier that is not a container at
			// all. Both spellings, because they are one reference.
			name:   "not-a-container/field-notation-on-a-scalar",
			sql:    "SELECT (b).x AS bx FROM decpair",
			cols:   []string{"bx"},
			refuse: "column notation .x applied to type numeric, which is not a composite type",
		},
		{
			name:   "not-a-container/field-notation-on-a-scalar-unparenthesised",
			sql:    "SELECT b.x AS bx FROM decpair",
			cols:   []string{"bx"},
			refuse: "column notation .x applied to type numeric, which is not a composite type",
		},
		{
			// The field on the RIGHT of a LEFT join, which is where the
			// SPILLED arm disagrees with the other three and with PostgreSQL.
			// PostgreSQL 17 (`(n.c_row).b`): nine rows, the one match at
			// `decpair.b = 0.0000` carrying the field's 0.
			//
			// PINNED on the spilled arm, and the two controls below say why it
			// is not this arc's mechanism: the CONTAINER PROJECTED under an
			// ORDINARY residual — no field path anywhere in the query — is
			// NULL on that arm too, while the same projection under a PLAIN
			// key is right. What the spilled arm loses is a ROW column of the
			// build payload once the join is KEYLESS, which is what an ON
			// residual makes it (`routeOuterJoinOnResiduals` empties JoinCond
			// and the join degenerates to one all-rows candidate chain — a
			// CROSS join, ADR-0006's non-spilling shape). This gate's field
			// path is a passenger.
			name: "in-subquery/a-field-path-as-the-outer-key",
			sql: "SELECT n.id AS nid FROM " + nested + " n WHERE c_row.b IN " +
				"(SELECT b FROM decpair) AND n.id < 20 ORDER BY n.id",
			cols: []string{"nid"},
			want: "1 rows: 0;",
		},
		{
			// NOT IN takes the same route and answered every row too.
			name: "in-subquery/a-field-path-under-not-in",
			sql: "SELECT n.id AS nid FROM " + nested + " n WHERE c_row.b NOT IN " +
				"(SELECT b FROM decpair WHERE b IS NOT NULL) AND n.id < 20 ORDER BY n.id",
			cols: []string{"nid"},
			want: "13 rows: 1;4;5;6;7;8;11;12;13;14;15;18;19;",
		},
		{
			name: "in-subquery/a-field-path-as-the-outer-key-parenthesised",
			sql: "SELECT n.id AS nid FROM " + nested + " n WHERE (c_row).b IN " +
				"(SELECT b FROM decpair) AND n.id < 20 ORDER BY n.id",
			cols: []string{"nid"},
			want: "1 rows: 0;",
		},
		{
			// The CONTROL that localised it: a LITERAL list is not a semi
			// join, and it was right before and after.
			name: "in-subquery/ctl-a-field-path-in-a-literal-list",
			sql: "SELECT n.id AS nid FROM " + nested + " n WHERE c_row.b IN (0, 11, 44) " +
				"AND n.id < 20 ORDER BY n.id",
			cols: []string{"nid"},
			want: "3 rows: 0;1;4;",
		},
		{
			// …and the control that bounds the DECLINE: an ordinary column as
			// the outer key still decorrelates into a semi join. Declining
			// that would turn every IN-subquery in the corpus into a filter.
			name: "in-subquery/ctl-an-ordinary-column-as-the-outer-key",
			sql: "SELECT n.id AS nid FROM " + nested + " n WHERE n.id IN " +
				"(SELECT id FROM decpair) AND n.id < 20 ORDER BY n.id",
			cols: []string{"nid"},
			want: "9 rows: 1;2;3;4;5;6;7;8;9;",
		},
		{
			// EXISTS correlated on a field path is REFUSED, loudly, on every
			// arm — the correlated-EXISTS lowering does not exist for this
			// shape and says so. Driven because the review asked where else a
			// field path can reach a subquery: it reaches EXISTS, and there it
			// fails rather than answering.
			name: "in-subquery/exists-correlated-on-a-field-path-refuses",
			sql: "SELECT n.id AS nid FROM " + nested + " n WHERE EXISTS " +
				"(SELECT 1 FROM decpair d WHERE d.b = c_row.b) AND n.id < 20 ORDER BY n.id",
			cols:   []string{"nid"},
			refuse: "c_row.b",
		},
		{
			name: "join-with-a-dimension",
			sql: "SELECT x.id AS xid, c_row.b AS fb FROM " + nested + " x JOIN " +
				typematrix.Dim + " d ON x.id = d.k WHERE x.id < 5 ORDER BY x.id",
			cols: []string{"xid", "fb"},
			want: "5 rows: 0|0;1|11;2|;3|;4|44;",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if tc.refuse != "" {
					if err == nil {
						t.Errorf("%s arm answered %d rows; PostgreSQL 17 refuses this shape "+
							"and so does this engine\n  SQL: %s", arm.name, len(res.Rows), tc.sql)
					} else if !strings.Contains(err.Error(), tc.refuse) {
						t.Errorf("%s arm refused with %q, want a refusal carrying %q\n  SQL: %s",
							arm.name, err.Error(), tc.refuse, tc.sql)
					}
					continue
				}
				if pinned, ok := tc.armErr[arm.name]; ok {
					switch {
					case err == nil:
						t.Errorf("the %s arm ANSWERED a shape this gate pins as a loud "+
							"failure (%d rows). Whatever it answers now, the pin is stale: "+
							"assert the rows and delete the entry\n  SQL: %s",
							arm.name, len(res.Rows), tc.sql)
					case !strings.Contains(err.Error(), pinned):
						t.Errorf("the %s arm failed with %q; this pin records a failure "+
							"carrying %q. The failure MOVED, which the next fix has to "+
							"account for\n  SQL: %s", arm.name, err.Error(), pinned, tc.sql)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, tc.sql)
				}
				if got := dajDigest(res, tc.cols); got != tc.want {
					t.Errorf("%s arm answered\n  %s\nwant\n  %s\n — a ROW field path lost its "+
						"CONTAINER across a join (ADR-0022)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}

	// The FILTER spelling, which fails LOUDLY rather than silently when the
	// container is dropped — `filter column "c_row.b" does not exist in the
	// input schema` on the single-process path of 376b2cac.
	//
	// The self-join entry is the one that cannot pass for the wrong reason:
	// its answer must equal the SAME query with no join at all, which is the
	// spelling nothing narrows and which was already right. 3537 of the
	// fixture's 5000 rows have `c_row.b > 20`, so a dropped container (every
	// row UNKNOWN, 0) and a bound-to-something-else reference are both
	// visible.
	//
	// The SELF-JOIN spelling moved to a REFUSAL on 2026-09-04: both arms
	// publish `c_row`, so the reference names no one container and
	// PostgreSQL's own answer for the parenthesised form is 42702. The
	// QUALIFIED spelling replaces it here, because the thing this loop is
	// about — a container surviving a join's narrowing — still needs a join
	// with two nested arms under it.
	for _, tc := range []struct {
		name, sql string
		want      int64
		refuse    string
	}{
		{name: "filter/no-join", want: 3537,
			sql: "SELECT COUNT(*) AS n FROM " + nested + " WHERE c_row.b > 20"},
		{name: "filter/self-join-is-ambiguous",
			sql: "SELECT COUNT(*) AS n FROM " + nested + " x JOIN " + nested +
				" y ON x.id = y.id WHERE c_row.b > 20",
			refuse: `column reference "c_row" is ambiguous`},
		// A three-part path does not parse (ADR-0022, "Not decided here"), so
		// the unambiguous spelling of a two-nested-arm join renames the other
		// arm's container away. The narrowing this loop is about is unchanged.
		{name: "filter/self-join-one-container", want: 3537,
			sql: "SELECT COUNT(*) AS n FROM " + nested + " x JOIN (SELECT id, c_row AS rw2 FROM " +
				nested + ") y ON x.id = y.id WHERE c_row.b > 20"},
		{name: "filter/dimension-join", want: 4,
			sql: "SELECT COUNT(*) AS n FROM " + nested + " x JOIN " +
				typematrix.Dim + " d ON x.id = d.k WHERE c_row.b > 20"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if tc.refuse != "" {
					if err == nil {
						t.Errorf("%s arm answered; PostgreSQL refuses this shape and so does "+
							"this engine\n  SQL: %s", arm.name, tc.sql)
					} else if !strings.Contains(err.Error(), tc.refuse) {
						t.Errorf("%s arm refused with %q, want a refusal carrying %q\n  SQL: %s",
							arm.name, err.Error(), tc.refuse, tc.sql)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, tc.sql)
				}
				got := ctrCounts(t, res)
				if len(got) != 1 || got[0] != tc.want {
					t.Errorf("%s arm answered %v, want %d\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}

// A WINDOW between the outer SELECT list and its join, on three arms.
//
// Two sites, both "resolve through the relation the reference names", and both
// reached only when a window sits in the SELECT list over a join whose two arms
// publish one alias.
//
//  1. The PROJECTION. `relationScopeSubtree` — the walk that finds the arm a
//     qualified SELECT-list item belongs to — descended Filter, Sort, Limit and
//     Distinct and stopped at Window, so a window in the SELECT list put a node
//     between the outer Project and the join that ended the walk. The whole
//     join subtree came back as the "scope" and the bare lookup inside it took
//     the first arm that answered:
//
//     SELECT x.id, x.w, y.w, SUM(y.w) OVER () AS s
//     FROM (SELECT id, a AS w FROM decpair) x
//     JOIN (SELECT id, a * 100 AS w FROM decpair) y ON x.id = y.id
//     -- PostgreSQL yw = 1275.00 · both DAG arms answered yw = 12.75
//
//  2. The window's ARGUMENT, which the projection fix does NOT reach.
//     `cleanExpr` drops a window argument's table qualifier unconditionally, so
//     `SUM(y.w)` reached the operator as `SUM(w)` and bound whichever arm's copy
//     the stream spells bare. The two execution paths spell it differently —
//     the single-process join keeps the PROBE's `w` bare, the DAG's keeps the
//     BUILD's — so the same strip was wrong on opposite arms: `SUM(y.w)`
//     answered 52.99 (Σ x.w) on the single path and `SUM(x.w)` answered 5300.00
//     (Σ y.w) on the DAG.
//
// So both directions are entries here: the window over the BUILD arm's column
// and over the PROBE arm's, projected beside each other so a capture cannot
// hide behind the pair agreeing, plus MIN/MAX (a value function, which reads
// the argument through a different branch), a PARTITION BY on the colliding
// alias, ROW_NUMBER (no argument at all), the CTE-arm spellings, and a BASE
// self-join whose arms are SHIFTED by one id so `SUM(u.a)` and `SUM(t.a)` are
// different numbers rather than the same one twice.
//
// Every expected value is PostgreSQL 17's over the decpair fixture.
func TestAWindowBetweenTheSelectListAndItsJoinThreeArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := dajArms(t, ctx)

	const tbl = dbpTable
	const (
		armX = "(SELECT id, a AS w FROM " + tbl + ") x"
		armY = "(SELECT id, a * 100 AS w FROM " + tbl + ") y"
		cteC = "WITH c AS (SELECT id, a * 2 AS dv FROM " + tbl + ") "
	)
	for _, tc := range []struct {
		name, sql string
		cols      []string
		want      string // PostgreSQL 17, whole result
		// dagPinned, when non-empty, is what the two DAG arms answer instead
		// — TODO(#780). It FAILS the day they agree with PostgreSQL, which is
		// how the pin gets deleted; the single arms are held to `want` either
		// way, because that half IS fixed here.
		dagPinned string
	}{
		{
			name: "projection/both-arms-and-a-window",
			sql: "SELECT x.id AS xid, x.w AS xw, y.w AS yw, SUM(y.w) OVER () AS s FROM " +
				armX + " JOIN " + armY + " ON x.id = y.id ORDER BY x.id",
			cols: []string{"xid", "xw", "yw", "s"},
			want: "9 rows: 1|12.75|1275.00|5299.00;2|12.75|1275.00|5299.00;3|12.75|1275.00|5299.00;" +
				"4|-0.01|-1.00|5299.00;5|2.00|200.00|5299.00;6|0.00|0.00|5299.00;7|||5299.00;" +
				"8|12.75|1275.00|5299.00;9|||5299.00;",
		},
		{
			// A CTE arm's filter and a window at once: the Filter and the
			// Window are BOTH between the outer Project and the join.
			name: "projection/window-above-a-cte-arm-filter",
			sql: cteC + "SELECT x.id AS xid, x.w AS xw, y.w AS yw, SUM(y.w) OVER () AS s FROM " +
				armX + " JOIN " + armY + " ON x.id = y.id JOIN c ON c.id = x.id " +
				"WHERE c.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw", "s"},
			want: "5 rows: 1|12.75|1275.00|5300.00;2|12.75|1275.00|5300.00;3|12.75|1275.00|5300.00;" +
				"5|2.00|200.00|5300.00;8|12.75|1275.00|5300.00;",
		},
		{
			name: "projection/window-over-a-base-table-filter",
			sql: "SELECT x.id AS xid, x.w AS xw, y.w AS yw, SUM(y.w) OVER () AS s FROM " +
				armX + " JOIN " + armY + " ON x.id = y.id JOIN " + tbl + " u ON u.id = x.id " +
				"WHERE u.a > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw", "s"},
			want: "5 rows: 1|12.75|1275.00|5300.00;2|12.75|1275.00|5300.00;3|12.75|1275.00|5300.00;" +
				"5|2.00|200.00|5300.00;8|12.75|1275.00|5300.00;",
		},
		{
			name: "projection/both-arms-are-ctes",
			sql: "WITH p AS (SELECT id, a AS w FROM " + tbl + "), " +
				"q AS (SELECT id, a * 100 AS w FROM " + tbl + ") " +
				"SELECT p.id AS xid, p.w AS xw, q.w AS yw, SUM(q.w) OVER () AS s " +
				"FROM p JOIN q ON p.id = q.id ORDER BY p.id",
			cols: []string{"xid", "xw", "yw", "s"},
			want: "9 rows: 1|12.75|1275.00|5299.00;2|12.75|1275.00|5299.00;3|12.75|1275.00|5299.00;" +
				"4|-0.01|-1.00|5299.00;5|2.00|200.00|5299.00;6|0.00|0.00|5299.00;7|||5299.00;" +
				"8|12.75|1275.00|5299.00;9|||5299.00;",
		},
		{
			// The ARGUMENT over the PROBE arm's column — the direction the
			// DAG got wrong while the single path got it right.
			name: "argument/window-over-the-probe-arms-column",
			sql: cteC + "SELECT x.id AS xid, x.w AS xw, y.w AS yw, SUM(x.w) OVER () AS s FROM " +
				armX + " JOIN " + armY + " ON x.id = y.id JOIN c ON c.id = x.id " +
				"WHERE c.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw", "s"},
			want: "5 rows: 1|12.75|1275.00|53.00;2|12.75|1275.00|53.00;3|12.75|1275.00|53.00;" +
				"5|2.00|200.00|53.00;8|12.75|1275.00|53.00;",
		},
		{
			// MIN/MAX read the argument through the VALUE-function branch,
			// and both arms are named at once so neither can borrow the
			// other's answer.
			name: "argument/min-over-one-arm-max-over-the-other",
			sql: "SELECT x.id AS xid, MIN(y.w) OVER () AS mn, MAX(x.w) OVER () AS mx FROM " +
				armX + " JOIN " + armY + " ON x.id = y.id ORDER BY x.id",
			cols: []string{"xid", "mn", "mx"},
			want: "9 rows: 1|-1.00|12.75;2|-1.00|12.75;3|-1.00|12.75;4|-1.00|12.75;5|-1.00|12.75;" +
				"6|-1.00|12.75;7|-1.00|12.75;8|-1.00|12.75;9|-1.00|12.75;",
		},
		{
			// PARTITION BY on the colliding alias: a key resolved to the
			// wrong arm changes the partitioning, not only a value.
			name: "argument/partition-by-the-other-arms-alias",
			sql: "SELECT x.id AS xid, x.w AS xw, y.w AS yw, SUM(y.w) OVER (PARTITION BY x.w) AS s FROM " +
				armX + " JOIN " + armY + " ON x.id = y.id ORDER BY x.id",
			cols: []string{"xid", "xw", "yw", "s"},
			want: "9 rows: 1|12.75|1275.00|5100.00;2|12.75|1275.00|5100.00;3|12.75|1275.00|5100.00;" +
				"4|-0.01|-1.00|-1.00;5|2.00|200.00|200.00;6|0.00|0.00|0.00;7|||;" +
				"8|12.75|1275.00|5100.00;9|||;",
		},
		{
			// No argument at all, so only the projection half can be wrong.
			name: "argument/row-number-has-none",
			sql: cteC + "SELECT x.id AS xid, y.w AS yw, ROW_NUMBER() OVER (ORDER BY x.id) AS rn FROM " +
				armX + " JOIN " + armY + " ON x.id = y.id JOIN c ON c.id = x.id " +
				"WHERE c.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "yw", "rn"},
			want: "5 rows: 1|1275.00|1;2|1275.00|2;3|1275.00|3;5|200.00|4;8|1275.00|5;",
		},
		{
			// A BASE self-join, whose arms carry the same COLUMN name rather
			// than the same derived alias — and SHIFTED by one id, so the two
			// windows are different numbers. An unshifted self-join is a
			// fixture that passes whichever arm the argument binds, which is
			// what the first cut of this entry was.
			name: "argument/base-self-join-shifted-both-windows",
			sql: "SELECT t.id AS tid, SUM(u.a) OVER () AS su, SUM(t.a) OVER () AS st FROM " +
				tbl + " t JOIN " + tbl + " u ON t.id = u.id + 1 WHERE t.id < 5 ORDER BY t.id",
			cols: []string{"tid", "su", "st"},
			want: "3 rows: 2|38.25|25.49;3|38.25|25.49;4|38.25|25.49;",
		},
		{
			name: "argument/base-self-join-shifted-build-arm-alone",
			sql: "SELECT t.id AS tid, SUM(u.a) OVER () AS su FROM " +
				tbl + " t JOIN " + tbl + " u ON t.id = u.id + 1 WHERE t.id < 5 ORDER BY t.id",
			cols: []string{"tid", "su"},
			want: "3 rows: 2|38.25;3|38.25;4|38.25;",
		},
		{
			name: "argument/base-self-join-shifted-probe-arm-alone",
			sql: "SELECT t.id AS tid, SUM(t.a) OVER () AS st FROM " +
				tbl + " t JOIN " + tbl + " u ON t.id = u.id + 1 WHERE t.id < 5 ORDER BY t.id",
			cols: []string{"tid", "st"},
			want: "3 rows: 2|25.49;3|25.49;4|25.49;",
		},

		// --- The two sites COMPOSING: a window in the SELECT list above a
		// join one of whose ARMS is itself a window (#772).
		//
		// Each site alone was closed by a7535925 and the NESTING was not. The
		// slot the arm's window mints and the slot the SELECT list's window
		// mints are the same `__win_0` — the builder's counter is per SELECT
		// BLOCK — and `renameCollidingSlots` renumbered the outer one and then
		// applied that rename DOWNWARD into the arm that had not minted it. So
		// the arm's projection read `__win_1` while its own window still wrote
		// `__win_0`: the single path failed with `column "__win_1" does not
		// exist in the input schema`, and both DAG arms — where the outer
		// window really does emit `__win_1` — answered the OUTER window's
		// value under the arm's name, silently.
		//
		// Every entry names p's window value (52.99) AND the outer window's
		// (5299.00), which are different numbers on purpose: an entry that
		// asserted only one of them passes whichever way the capture goes.
		{
			name: "compose/window-above-a-join-whose-arm-is-a-window",
			sql: "SELECT p.id AS pid, p.w AS pw, q.w AS qw, SUM(q.w) OVER () AS s FROM " +
				"(SELECT id, SUM(a) OVER () AS w FROM " + tbl + ") p JOIN " +
				"(SELECT id, a * 100 AS w FROM " + tbl + ") q ON p.id = q.id ORDER BY p.id",
			cols: []string{"pid", "pw", "qw", "s"},
			want: "9 rows: 1|52.99|1275.00|5299.00;2|52.99|1275.00|5299.00;3|52.99|1275.00|5299.00;" +
				"4|52.99|-1.00|5299.00;5|52.99|200.00|5299.00;6|52.99|0.00|5299.00;7|52.99||5299.00;" +
				"8|52.99|1275.00|5299.00;9|52.99||5299.00;",
		},
		{
			// The SUM(p.w) spelling — the outer window over the arm that is
			// ITSELF a window, which answered NULL for both columns.
			name: "compose/outer-window-over-the-window-arm",
			sql: "SELECT p.id AS pid, p.w AS pw, q.w AS qw, SUM(p.w) OVER () AS s FROM " +
				"(SELECT id, SUM(a) OVER () AS w FROM " + tbl + ") p JOIN " +
				"(SELECT id, a * 100 AS w FROM " + tbl + ") q ON p.id = q.id ORDER BY p.id",
			cols: []string{"pid", "pw", "qw", "s"},
			want: "9 rows: 1|52.99|1275.00|476.91;2|52.99|1275.00|476.91;3|52.99|1275.00|476.91;" +
				"4|52.99|-1.00|476.91;5|52.99|200.00|476.91;6|52.99|0.00|476.91;7|52.99||476.91;" +
				"8|52.99|1275.00|476.91;9|52.99||476.91;",
		},
		{
			// The window arm SECOND, so the rename walk meets the two blocks
			// in the other order.
			name: "compose/window-arm-is-the-second-one",
			sql: "SELECT p.id AS pid, p.w AS pw, q.w AS qw, SUM(q.w) OVER () AS s FROM " +
				"(SELECT id, a * 100 AS w FROM " + tbl + ") q JOIN " +
				"(SELECT id, SUM(a) OVER () AS w FROM " + tbl + ") p ON p.id = q.id ORDER BY p.id",
			cols: []string{"pid", "pw", "qw", "s"},
			want: "9 rows: 1|52.99|1275.00|5299.00;2|52.99|1275.00|5299.00;3|52.99|1275.00|5299.00;" +
				"4|52.99|-1.00|5299.00;5|52.99|200.00|5299.00;6|52.99|0.00|5299.00;7|52.99||5299.00;" +
				"8|52.99|1275.00|5299.00;9|52.99||5299.00;",
		},
		{
			// MIN reads the argument through the value-function branch.
			name: "compose/min-over-the-other-arm",
			sql: "SELECT p.id AS pid, p.w AS pw, q.w AS qw, MIN(q.w) OVER () AS s FROM " +
				"(SELECT id, SUM(a) OVER () AS w FROM " + tbl + ") p JOIN " +
				"(SELECT id, a * 100 AS w FROM " + tbl + ") q ON p.id = q.id ORDER BY p.id",
			cols: []string{"pid", "pw", "qw", "s"},
			want: "9 rows: 1|52.99|1275.00|-1.00;2|52.99|1275.00|-1.00;3|52.99|1275.00|-1.00;" +
				"4|52.99|-1.00|-1.00;5|52.99|200.00|-1.00;6|52.99|0.00|-1.00;7|52.99||-1.00;" +
				"8|52.99|1275.00|-1.00;9|52.99||-1.00;",
		},
		{
			// No argument at all: only the arm's own column can be captured,
			// and it was — by the ROW_NUMBER.
			name: "compose/row-number-above-a-window-arm",
			sql: "SELECT p.id AS pid, p.w AS pw, q.w AS qw, ROW_NUMBER() OVER (ORDER BY p.id) AS s FROM " +
				"(SELECT id, SUM(a) OVER () AS w FROM " + tbl + ") p JOIN " +
				"(SELECT id, a * 100 AS w FROM " + tbl + ") q ON p.id = q.id ORDER BY p.id",
			cols: []string{"pid", "pw", "qw", "s"},
			want: "9 rows: 1|52.99|1275.00|1;2|52.99|1275.00|2;3|52.99|1275.00|3;" +
				"4|52.99|-1.00|4;5|52.99|200.00|5;6|52.99|0.00|6;7|52.99||7;" +
				"8|52.99|1275.00|8;9|52.99||9;",
		},
		{
			// The control that says the residual is the COMPOSITION: distinct
			// aliases, so no name is contested, and the shape was already
			// right on the DAG arms and LOUD on the single one — because the
			// downward rename is about PROVENANCE and not about the alias.
			name: "compose/control-distinct-aliases",
			sql: "SELECT p.id AS pid, p.w1 AS pw, q.w2 AS qw, SUM(q.w2) OVER () AS s FROM " +
				"(SELECT id, SUM(a) OVER () AS w1 FROM " + tbl + ") p JOIN " +
				"(SELECT id, a * 100 AS w2 FROM " + tbl + ") q ON p.id = q.id ORDER BY p.id",
			cols: []string{"pid", "pw", "qw", "s"},
			want: "9 rows: 1|52.99|1275.00|5299.00;2|52.99|1275.00|5299.00;3|52.99|1275.00|5299.00;" +
				"4|52.99|-1.00|5299.00;5|52.99|200.00|5299.00;6|52.99|0.00|5299.00;7|52.99||5299.00;" +
				"8|52.99|1275.00|5299.00;9|52.99||5299.00;",
		},
		{
			// The control WITHOUT the outer window, which was already correct
			// on all three arms after a7535925: a failure here is the arm's
			// own window, not the composition.
			name: "compose/control-no-outer-window",
			sql: "SELECT p.id AS pid, p.w AS pw, q.w AS qw FROM " +
				"(SELECT id, SUM(a) OVER () AS w FROM " + tbl + ") p JOIN " +
				"(SELECT id, a * 100 AS w FROM " + tbl + ") q ON p.id = q.id ORDER BY p.id",
			cols: []string{"pid", "pw", "qw"},
			want: "9 rows: 1|52.99|1275.00;2|52.99|1275.00;3|52.99|1275.00;4|52.99|-1.00;" +
				"5|52.99|200.00;6|52.99|0.00;7|52.99|;8|52.99|1275.00;9|52.99|;",
		},

		// --- An arm that is ITSELF A JOIN (#773), and the naming DOCTRINE the
		// two engines need for one (#773 round 2).
		//
		// The arm's own Project is a real OPERATOR on the single-process
		// pipeline and emits NO STAGE on the DAG (ADR-0025), so the join is
		// handed two different streams and a name describes a stream:
		//
		//   single   the build side IS the arm's output — `id`, `w` — and no
		//            inner relation's columns are in it, so the ONE name the
		//            enclosing query writes is the only name they answer to;
		//   DAG      the build side is the arm's RAW inner columns, one per
		//            relation inside it, and the arm's name describes NONE of
		//            them: which one the arm publishes is exactly what the
		//            un-materialized Project knows and the stage does not.
		//
		// Giving the DAG the arm's name put `m.w` on the column the arm did
		// NOT select: `PROJ "g.w"` matched nothing, fell back to the bare
		// name, and took the OTHER arm's column — silently, on both DAG arms,
		// with no window anywhere. `joinArmAlias` is the materialized answer
		// and `stageBuildTableAlias` the raw one.
		//
		// EVERY entry below spells BOTH arms COMPUTED. The first cut of this
		// family spelled the probe arm `a AS w`, a plain RENAME, and a rename
		// resolves back to a source column through every resolver — so the
		// entries passed whichever name the join used and could not see the
		// defect at all. One rename control is kept, deliberately marked.
		{
			name: "arm-is-a-join/computed-both-arms",
			sql: "SELECT t.id AS tid, t.w AS tw, m.w AS mw FROM " +
				"(SELECT id, a * 2 AS w FROM " + tbl + ") t JOIN " +
				"(SELECT g.id AS id, g.w AS w FROM (SELECT id, b * 3 AS w FROM " + tbl +
				") g JOIN " + tbl + " h ON g.id = h.id) m ON t.id = m.id ORDER BY t.id",
			cols: []string{"tid", "tw", "mw"},
			want: "9 rows: 1|25.50|38.2500;2|25.50|38.2503;3|25.50|38.2497;4|-0.02|-0.0300;" +
				"5|4.00|30.0000;6|0.00|0.0000;7||3.0000;8|25.50|;9||;",
		},
		{
			// Both arms WINDOWED and wrapped, so the arm's column is a slot's
			// value and the probe's is another slot's.
			name: "arm-is-a-join/wrapped-window-in-both-arms",
			sql: "SELECT t.id AS tid, t.w AS tw, m.w AS mw FROM " +
				"(SELECT id, SUM(a) OVER () + 0 AS w FROM " + tbl + ") t JOIN " +
				"(SELECT g.id AS id, g.w AS w FROM (SELECT id, SUM(b) OVER () + 0 AS w FROM " +
				tbl + ") g JOIN " + tbl + " h ON g.id = h.id) m ON t.id = m.id ORDER BY t.id",
			cols: []string{"tid", "tw", "mw"},
			want: "9 rows: 1|52.99|49.2400;2|52.99|49.2400;3|52.99|49.2400;4|52.99|49.2400;" +
				"5|52.99|49.2400;6|52.99|49.2400;7|52.99|49.2400;8|52.99|49.2400;9|52.99|49.2400;",
		},
		{
			// The COMMA spelling of the same body: it is the arm being a JOIN
			// and nothing about the keyword.
			name: "arm-is-a-join/comma-join",
			sql: "SELECT t.id AS tid, t.w AS tw, m.w AS mw FROM " +
				"(SELECT id, SUM(a) OVER () + 0 AS w FROM " + tbl + ") t JOIN " +
				"(SELECT g.id AS id, g.w AS w FROM (SELECT id, SUM(b) OVER () + 0 AS w FROM " +
				tbl + ") g, " + tbl + " h WHERE g.id = h.id) m ON t.id = m.id ORDER BY t.id",
			cols: []string{"tid", "tw", "mw"},
			want: "9 rows: 1|52.99|49.2400;2|52.99|49.2400;3|52.99|49.2400;4|52.99|49.2400;" +
				"5|52.99|49.2400;6|52.99|49.2400;7|52.99|49.2400;8|52.99|49.2400;9|52.99|49.2400;",
		},
		{
			// BOTH relations inside the arm derived, so no scan below it
			// answers to a name the enclosing query wrote.
			name: "arm-is-a-join/both-relations-derived",
			sql: "SELECT t.id AS tid, t.w AS tw, m.w AS mw FROM " +
				"(SELECT id, SUM(a) OVER () + 0 AS w FROM " + tbl + ") t JOIN " +
				"(SELECT g.id AS id, g.w AS w FROM (SELECT id, SUM(b) OVER () + 0 AS w FROM " +
				tbl + ") g JOIN (SELECT id, f FROM " + tbl + ") h ON g.id = h.id) m " +
				"ON t.id = m.id ORDER BY t.id",
			cols: []string{"tid", "tw", "mw"},
			want: "9 rows: 1|52.99|49.2400;2|52.99|49.2400;3|52.99|49.2400;4|52.99|49.2400;" +
				"5|52.99|49.2400;6|52.99|49.2400;7|52.99|49.2400;8|52.99|49.2400;9|52.99|49.2400;",
		},
		{
			// The probe arm computed WITHOUT a window while the arm has one:
			// the two sides reach the resolver by different routes.
			name: "arm-is-a-join/probe-computed-arm-windowed",
			sql: "SELECT t.id AS tid, t.w AS tw, m.w AS mw FROM " +
				"(SELECT id, a * 2 AS w FROM " + tbl + ") t JOIN " +
				"(SELECT g.id AS id, g.w AS w FROM (SELECT id, SUM(b) OVER () + 0 AS w FROM " +
				tbl + ") g JOIN " + tbl + " h ON g.id = h.id) m ON t.id = m.id ORDER BY t.id",
			cols: []string{"tid", "tw", "mw"},
			want: "9 rows: 1|25.50|49.2400;2|25.50|49.2400;3|25.50|49.2400;4|-0.02|49.2400;" +
				"5|4.00|49.2400;6|0.00|49.2400;7||49.2400;8|25.50|49.2400;9||49.2400;",
		},
		{
			// The BARE window spelling of the arm, which mints no `__win_N`
			// reference inside a larger expression.
			name: "arm-is-a-join/bare-window-in-the-arm",
			sql: "SELECT t.id AS tid, t.w AS tw, m.w AS mw FROM " +
				"(SELECT id, a * 2 AS w FROM " + tbl + ") t JOIN " +
				"(SELECT g.id AS id, g.w AS w FROM (SELECT id, SUM(b) OVER () AS w FROM " +
				tbl + ") g JOIN " + tbl + " h ON g.id = h.id) m ON t.id = m.id ORDER BY t.id",
			cols: []string{"tid", "tw", "mw"},
			want: "9 rows: 1|25.50|49.2400;2|25.50|49.2400;3|25.50|49.2400;4|-0.02|49.2400;" +
				"5|4.00|49.2400;6|0.00|49.2400;7||49.2400;8|25.50|49.2400;9||49.2400;",
		},
		{
			// A LEFT join, so the arm's columns are null-padded as well as
			// renamed.
			name: "arm-is-a-join/left-join",
			sql: "SELECT t.id AS tid, t.w AS tw, m.w AS mw FROM " +
				"(SELECT id, SUM(a) OVER () + 0 AS w FROM " + tbl + ") t LEFT JOIN " +
				"(SELECT g.id AS id, g.w AS w FROM (SELECT id, SUM(b) OVER () + 0 AS w FROM " +
				tbl + ") g JOIN " + tbl + " h ON g.id = h.id) m ON t.id = m.id ORDER BY t.id",
			cols: []string{"tid", "tw", "mw"},
			want: "9 rows: 1|52.99|49.2400;2|52.99|49.2400;3|52.99|49.2400;4|52.99|49.2400;" +
				"5|52.99|49.2400;6|52.99|49.2400;7|52.99|49.2400;8|52.99|49.2400;9|52.99|49.2400;",
		},
		{
			// The joining arm FIRST, which was already correct on every arm:
			// the capture is directional, and this says the repair is not a
			// reorder.
			name: "arm-is-a-join/joining-arm-first",
			sql: "SELECT t.id AS tid, t.w AS tw, m.w AS mw FROM " +
				"(SELECT g.id AS id, g.w AS w FROM (SELECT id, SUM(b) OVER () + 0 AS w FROM " +
				tbl + ") g JOIN " + tbl + " h ON g.id = h.id) m JOIN " +
				"(SELECT id, SUM(a) OVER () + 0 AS w FROM " + tbl + ") t " +
				"ON t.id = m.id ORDER BY t.id",
			cols: []string{"tid", "tw", "mw"},
			want: "9 rows: 1|52.99|49.2400;2|52.99|49.2400;3|52.99|49.2400;4|52.99|49.2400;" +
				"5|52.99|49.2400;6|52.99|49.2400;7|52.99|49.2400;8|52.99|49.2400;9|52.99|49.2400;",
		},
		{
			// DISTINCT aliases: nothing is contested and it was right before.
			name: "arm-is-a-join/control-distinct-aliases",
			sql: "SELECT t.id AS tid, t.w2 AS tw, m.w AS mw FROM " +
				"(SELECT id, SUM(a) OVER () + 0 AS w2 FROM " + tbl + ") t JOIN " +
				"(SELECT g.id AS id, g.w AS w FROM (SELECT id, SUM(b) OVER () + 0 AS w FROM " +
				tbl + ") g JOIN " + tbl + " h ON g.id = h.id) m ON t.id = m.id ORDER BY t.id",
			cols: []string{"tid", "tw", "mw"},
			want: "9 rows: 1|52.99|49.2400;2|52.99|49.2400;3|52.99|49.2400;4|52.99|49.2400;" +
				"5|52.99|49.2400;6|52.99|49.2400;7|52.99|49.2400;8|52.99|49.2400;9|52.99|49.2400;",
		},
		{
			// The arm holding ONE relation, which needs no doctrine at all:
			// the arm's name and its scan's alias describe the same stream.
			name: "arm-is-a-join/control-single-relation-arm",
			sql: "SELECT t.id AS tid, t.w AS tw, m.w AS mw FROM " +
				"(SELECT id, SUM(a) OVER () AS w FROM " + tbl + ") t JOIN " +
				"(SELECT g.id AS id, g.w AS w FROM (SELECT id, SUM(b) OVER () AS w FROM " +
				tbl + ") g) m ON t.id = m.id ORDER BY t.id",
			cols: []string{"tid", "tw", "mw"},
			want: "9 rows: 1|52.99|49.2400;2|52.99|49.2400;3|52.99|49.2400;4|52.99|49.2400;" +
				"5|52.99|49.2400;6|52.99|49.2400;7|52.99|49.2400;8|52.99|49.2400;9|52.99|49.2400;",
		},
		{
			// The DOCTRINE'S EDGE, and it is the arm this family cannot
			// otherwise reach: every entry above wraps its inner relations in
			// DERIVED tables, and an arm of BARE SCANS publishing a COMPUTED
			// column is a different cell. `joinArmSoleName` finds no inner
			// derived alias to decline on, the arm's name is the only one
			// there is on either engine, and the DAG — where the arm's Project
			// emits no stage — has nothing that IS `m.a`, so the bare name
			// wins and the PROBE's column answers. Pinned as #780: the SINGLE
			// path is fixed (this arc's materialized half) and both DAG arms
			// are byte-identical to de95b3b5.
			name: "arm-is-a-join/pin780-bare-scans-computed-column",
			sql: "SELECT t.a AS tw, m.a AS mw FROM " + tbl + " t JOIN " +
				"(SELECT g.id AS id, g.a * 3 AS a FROM " + tbl + " g JOIN " + tbl +
				" h ON g.id = h.id) m ON t.id = m.id ORDER BY t.id",
			cols: []string{"tw", "mw"},
			want: "9 rows: 12.75|38.25;12.75|38.25;12.75|38.25;-0.01|-0.03;2.00|6.00;" +
				"0.00|0.00;|;12.75|38.25;|;",
			dagPinned: "9 rows: 12.75|12.75;12.75|12.75;12.75|12.75;-0.01|-0.01;2.00|2.00;" +
				"0.00|0.00;|;12.75|12.75;|;",
		},
		{
			// The same edge over two DIFFERENT tables, so the contested name
			// is not a self-join's doing.
			name: "arm-is-a-join/pin780-different-tables",
			sql: "SELECT t.d92 AS tw, m.d92 AS mw FROM zzp t JOIN " +
				"(SELECT g.id AS id, h.d92 * 2 AS d92 FROM zzp g JOIN zzj h ON g.id = h.id) m " +
				"ON t.id = m.id ORDER BY t.id",
			cols:      []string{"tw", "mw"},
			want:      "3 rows: -3.50|2.2222;0.00|24691356.2468;12.75|6.6666;",
			dagPinned: "3 rows: -3.50|-3.50;0.00|0.00;12.75|12.75;",
		},
		{
			// #780's controls, both correct on every arm and both needed. The
			// RENAME body resolves back to a source column through every
			// resolver, and the DISTINCT alias contests nothing — so the cell
			// is COMPUTED × SHARED BARE NAME, and neither half alone.
			name: "arm-is-a-join/pin780-control-rename-body-bare-scans",
			sql: "SELECT t.a AS tw, m.a AS mw FROM " + tbl + " t JOIN " +
				"(SELECT g.id AS id, h.b AS a FROM " + tbl + " g JOIN " + tbl +
				" h ON g.id = h.id) m ON t.id = m.id ORDER BY t.id",
			cols: []string{"tw", "mw"},
			want: "9 rows: 12.75|12.7500;12.75|12.7501;12.75|12.7499;-0.01|-0.0100;" +
				"2.00|10.0000;0.00|0.0000;|1.0000;12.75|;|;",
		},
		{
			name: "arm-is-a-join/pin780-control-distinct-alias-bare-scans",
			sql: "SELECT t.a AS tw, m.a2 AS mw FROM " + tbl + " t JOIN " +
				"(SELECT g.id AS id, h.a * 3 AS a2 FROM " + tbl + " g JOIN " + tbl +
				" h ON g.id = h.id) m ON t.id = m.id ORDER BY t.id",
			cols: []string{"tw", "mw"},
			want: "9 rows: 12.75|38.25;12.75|38.25;12.75|38.25;-0.01|-0.03;2.00|6.00;" +
				"0.00|0.00;|;12.75|38.25;|;",
		},
		{
			// The RENAME spelling, kept as the deliberate control: it is
			// correct on every tree whatever the join calls the arm, which is
			// exactly why a family spelled this way could not fail.
			name: "arm-is-a-join/control-rename-body",
			sql: "SELECT t.id AS tid, t.w AS tw, m.w AS mw FROM " +
				"(SELECT id, SUM(a) OVER () AS w FROM " + tbl + ") t JOIN " +
				"(SELECT g.id AS id, g.w AS w FROM (SELECT id, b AS w FROM " + tbl +
				") g JOIN " + tbl + " h ON g.id = h.id) m ON t.id = m.id ORDER BY t.id",
			cols: []string{"tid", "tw", "mw"},
			want: "9 rows: 1|52.99|12.7500;2|52.99|12.7501;3|52.99|12.7499;4|52.99|-0.0100;" +
				"5|52.99|10.0000;6|52.99|0.0000;7|52.99|1.0000;8|52.99|;9|52.99|;",
		},

		// --- The three controls that bound the family.
		{
			// The window REMOVED: if this one moves, the finding is not the
			// window.
			name: "control/no-window",
			sql: cteC + "SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM " + armX +
				" JOIN " + armY + " ON x.id = y.id JOIN c ON c.id = x.id " +
				"WHERE c.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw"},
			want: "5 rows: 1|12.75|1275.00;2|12.75|1275.00;3|12.75|1275.00;5|2.00|200.00;" +
				"8|12.75|1275.00;",
		},
		{
			// DISTINCT aliases, so there is no collision to resolve: correct
			// on every tree, and a failure here is not this mechanism.
			name: "control/arms-publish-different-aliases",
			sql: cteC + "SELECT x.id AS xid, x.w AS xw, y.w2 AS yw, SUM(y.w2) OVER () AS s FROM " +
				armX + " JOIN (SELECT id, a * 100 AS w2 FROM " + tbl + ") y ON x.id = y.id " +
				"JOIN c ON c.id = x.id WHERE c.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw", "s"},
			want: "5 rows: 1|12.75|1275.00|5300.00;2|12.75|1275.00|5300.00;3|12.75|1275.00|5300.00;" +
				"5|2.00|200.00|5300.00;8|12.75|1275.00|5300.00;",
		},
		{
			// The window in the OUTER query over a subquery that does the
			// join: no window sits between a SELECT list and a join at all,
			// and the collision is resolved one level down.
			name: "control/window-outside-the-joining-subquery",
			sql: "SELECT q.xid AS xid, q.xw AS xw, q.yw AS yw, SUM(q.yw) OVER () AS s FROM " +
				"(SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM " + armX + " JOIN " + armY +
				" ON x.id = y.id) q ORDER BY q.xid",
			cols: []string{"xid", "xw", "yw", "s"},
			want: "9 rows: 1|12.75|1275.00|5299.00;2|12.75|1275.00|5299.00;3|12.75|1275.00|5299.00;" +
				"4|-0.01|-1.00|5299.00;5|2.00|200.00|5299.00;6|0.00|0.00|5299.00;7|||5299.00;" +
				"8|12.75|1275.00|5299.00;9|||5299.00;",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused a query PostgreSQL 17 answers: %v\n  SQL: %s",
						arm.name, err, tc.sql)
				}
				got := dajDigest(res, tc.cols)
				if tc.dagPinned != "" && arm.name != "single" {
					if got == tc.want {
						t.Errorf("the %s arm now answers PostgreSQL's values — #780 is fixed, "+
							"delete this entry's dagPinned and assert `want` on every arm"+
							"\n  SQL: %s", arm.name, tc.sql)
					} else if got != tc.dagPinned {
						t.Errorf("the %s arm answered\n  %s\nwhich is neither PostgreSQL's\n  %s"+
							"\nnor the pinned\n  %s\n — #780 MOVED, re-read the pin\n  SQL: %s",
							arm.name, got, tc.want, tc.dagPinned, tc.sql)
					}
					continue
				}
				if got != tc.want {
					t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n"+
						" — a window between the SELECT list and its join resolved a "+
						"qualified reference in the WRONG arm\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}

	// METHOD 10: the impossibility this fix asserts, with a fixture that
	// ATTEMPTS it.
	//
	// `scopePreservingWrapper` admits Window and deliberately does NOT admit
	// Aggregate — an aggregate replaces its child's schema with its GROUP BY
	// keys and aggregate outputs, so a bare name resolved below it answers
	// from a schema the stream does not carry. The claim that leaving it out
	// costs no wrong answer is only worth what a fixture that tries it is
	// worth, so here are the two aggregate-between-the-SELECT-list-and-the-
	// join shapes: today they are CORRECT or LOUD on every arm, never
	// silently wrong, and this pins that. When one of them starts ANSWERING
	// on an arm that refused, the answer must be PostgreSQL's — which is what
	// makes admitting Aggregate a change that has to come with its values.
	for _, tc := range []struct {
		name, sql string
		cols      []string
		want      string
	}{
		{
			name: "aggregate-side/group-by-both-arms",
			sql: cteC + "SELECT x.w AS xw, y.w AS yw, COUNT(*) AS n FROM " + armX +
				" JOIN " + armY + " ON x.id = y.id JOIN c ON c.id = x.id " +
				"WHERE c.dv > 1 GROUP BY x.w, y.w ORDER BY xw, yw",
			cols: []string{"xw", "yw", "n"},
			want: "2 rows: 2.00|200.00|1;12.75|1275.00|4;",
		},
		{
			name: "aggregate-side/sum-of-each-arm",
			sql: cteC + "SELECT SUM(x.w) AS sxw, SUM(y.w) AS syw FROM " + armX +
				" JOIN " + armY + " ON x.id = y.id JOIN c ON c.id = x.id WHERE c.dv > 1",
			cols: []string{"sxw", "syw"},
			want: "1 rows: 53.00|5300.00;",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					// Loud is an acceptable outcome here and a wrong number is
					// not. The error is not asserted verbatim — it belongs to
					// the aggregate's own carrying, which this gate is not
					// about — but the disposition is.
					t.Logf("%s arm refuses (acceptable, not silent): %v", arm.name, err)
					continue
				}
				if got := dajDigest(res, tc.cols); got != tc.want {
					t.Errorf("%s arm ANSWERED\n  %s\nPostgreSQL 17 answers\n  %s\n"+
						" — an aggregate between the SELECT list and its join is a "+
						"scope boundary the walk stops at, and this shape may refuse "+
						"but must never answer something else\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}

// A CTE's QUALIFIED column beside a SIBLING arm's BARE column of the same
// name, on three arms.
//
// The join qualifies the build's duplicate columns with the alias the arm
// answers to, and for a CTE reference that alias came from the SCAN below it
// — the CTE's underlying TABLE — because a CTE records its name on the
// subtree ROOT instead (`Node.CTEName`, deliberately: see
// `subtreeNamesRelation`). So `c.dv` matched neither the bare `dv` a sibling
// derived arm shipped nor the `decpair.dv` the join had renamed c's column to,
// fell through to the resolver's qualifier strip, and bound the SIBLING's
// column — the same value under both output names, on every arm:
//
//	WITH c AS (SELECT id, a * 2 AS dv FROM decpair)
//	SELECT x.id AS xid, c.dv AS cdv, p.dv AS pdv
//	FROM (SELECT id, b - 100 AS dv FROM decpair) p
//	JOIN decpair x ON p.id = x.id JOIN c ON c.id = p.id
//	JOIN decpair y ON c.id = y.id ORDER BY x.id
//	-- PostgreSQL 17  cdv 25.50, pdv -87.2500
//	-- 376b2cac: single and DAG cdv == pdv; the shuffled arm refused at
//	--   dispatch, so the chain rewiring turned a loud refusal into this
//	--   silent capture until the arm was named `c`.
//
// The two arms publish `dv` at DIFFERENT DECIMAL scales on purpose — that is
// what makes the values distinguishable at all — so the single-process path
// renders one of them at the other's typmod, which is #754 and is pinned per
// entry rather than asserted. `distinct-names` is the control that says so:
// no alias collision, right values at the right scale everywhere.
func TestACTEsQualifiedColumnKeepsItsOwnArmThreeArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := dajArms(t, ctx)

	const tbl = dbpTable
	const (
		cteC = "WITH c AS (SELECT id, a * 2 AS dv FROM " + tbl + ") "
		armP = "(SELECT id, b - 100 AS dv FROM " + tbl + ") p"
		// PostgreSQL 17. cdv is c's `a * 2`; pdv is p's `b - 100`.
		pgBoth = "9 rows: 1|25.50|-87.2500;2|25.50|-87.2499;3|25.50|-87.2501;" +
			"4|-0.02|-100.0100;5|4.00|-90.0000;6|0.00|-100.0000;7||-99.0000;8|25.50|;9||;"
	)
	// Every entry used to carry a `singlePinned` rendering — the same digits
	// at the OTHER arm's DECIMAL scale, which was #754 on the single-process
	// path. It is closed: the VALUE was resolved through `c.dv` and the
	// DECLARATION through the first bare `dv`, so one output was described by
	// two arms (#706's mechanism, reached through a derived alias).
	for _, tc := range []struct {
		name, sql string
		cols      []string
		want      string // PostgreSQL 17, whole result
	}{
		{
			name: "cte-last/4-join",
			sql: cteC + "SELECT x.id AS xid, c.dv AS cdv, p.dv AS pdv FROM " + armP +
				" JOIN " + tbl + " x ON p.id = x.id JOIN c ON c.id = p.id " +
				"JOIN " + tbl + " y ON c.id = y.id ORDER BY x.id",
			cols: []string{"xid", "cdv", "pdv"}, want: pgBoth,
		},
		{
			name: "cte-last/3-join",
			sql: cteC + "SELECT x.id AS xid, c.dv AS cdv, p.dv AS pdv FROM " + armP +
				" JOIN " + tbl + " x ON p.id = x.id JOIN c ON c.id = p.id ORDER BY x.id",
			cols: []string{"xid", "cdv", "pdv"}, want: pgBoth,
		},
		{
			name: "cte-last/2-join",
			sql: cteC + "SELECT p.id AS xid, c.dv AS cdv, p.dv AS pdv FROM " + armP +
				" JOIN c ON c.id = p.id ORDER BY p.id",
			cols: []string{"xid", "cdv", "pdv"}, want: pgBoth,
		},
		{
			// c as a DERIVED table: its alias IS stamped on the scan, so this
			// spelling names the arm correctly by the old route. The control
			// that says the CTE's root-stamped name was the whole of it.
			name: "derived-c-control",
			sql: "SELECT x.id AS xid, c.dv AS cdv, p.dv AS pdv FROM " + armP +
				" JOIN " + tbl + " x ON p.id = x.id " +
				"JOIN (SELECT id, a * 2 AS dv FROM " + tbl + ") c ON c.id = p.id " +
				"JOIN " + tbl + " y ON c.id = y.id ORDER BY x.id",
			cols: []string{"xid", "cdv", "pdv"}, want: pgBoth,
		},
		{
			// Each column projected ALONE, so a capture cannot hide behind
			// the pair agreeing.
			name: "sibling-arm-alone",
			sql: cteC + "SELECT x.id AS xid, p.dv AS pdv FROM " + armP +
				" JOIN " + tbl + " x ON p.id = x.id JOIN c ON c.id = p.id " +
				"JOIN " + tbl + " y ON c.id = y.id ORDER BY x.id",
			cols: []string{"xid", "pdv"},
			want: "9 rows: 1|-87.2500;2|-87.2499;3|-87.2501;4|-100.0100;5|-90.0000;" +
				"6|-100.0000;7|-99.0000;8|;9|;",
		},
		{
			name: "cte-arm-alone",
			sql: cteC + "SELECT x.id AS xid, c.dv AS cdv FROM " + armP +
				" JOIN " + tbl + " x ON p.id = x.id JOIN c ON c.id = p.id " +
				"JOIN " + tbl + " y ON c.id = y.id ORDER BY x.id",
			cols: []string{"xid", "cdv"},
			want: "9 rows: 1|25.50;2|25.50;3|25.50;4|-0.02;5|4.00;6|0.00;7|;8|25.50;9|;",
		},
		{
			// A FILTER on the captured column: a wrong binding changes the
			// row SET here, not only a value.
			name: "filtered-on-the-cte-column",
			sql: cteC + "SELECT x.id AS xid, c.dv AS cdv, p.dv AS pdv FROM " + armP +
				" JOIN " + tbl + " x ON p.id = x.id JOIN c ON c.id = p.id " +
				"JOIN " + tbl + " y ON c.id = y.id WHERE c.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "cdv", "pdv"},
			want: "5 rows: 1|25.50|-87.2500;2|25.50|-87.2499;3|25.50|-87.2501;" +
				"5|4.00|-90.0000;8|25.50|;",
		},
		{
			// NO collision: p publishes `pv`. Right values at the right scale
			// on every arm, which is what makes the #754 pins above about the
			// duplicate alias and not about this gate's mechanism.
			name: "distinct-names",
			sql: cteC + "SELECT x.id AS xid, c.dv AS cdv, p.pv AS ppv FROM " +
				"(SELECT id, b - 100 AS pv FROM " + tbl + ") p " +
				"JOIN " + tbl + " x ON p.id = x.id JOIN c ON c.id = p.id " +
				"JOIN " + tbl + " y ON c.id = y.id ORDER BY x.id",
			cols: []string{"xid", "cdv", "ppv"}, want: pgBoth,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused a query PostgreSQL 17 answers: %v\n  SQL: %s",
						arm.name, err, tc.sql)
				}
				if got := dajDigest(res, tc.cols); got != tc.want {
					t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n — a CTE's "+
						"qualified column bound a SIBLING arm's bare one\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}

	// The CTE FIRST in the FROM list. The single-process path used to REFUSE
	// this outright — the two arms' one alias made it store p's scale-4 value
	// into c's scale-2 declaration, and ADR-0024 item 4 reported rather than
	// rounding. It was the loud face of #754 and it is closed with the silent
	// one: a qualified reference is declared by its own side, so neither arm
	// describes the other's column and every arm answers PostgreSQL's values.
	t.Run("cte-first", func(t *testing.T) {
		sql := cteC + "SELECT x.id AS xid, c.dv AS cdv, p.dv AS pdv FROM c JOIN " + armP +
			" ON c.id = p.id JOIN " + tbl + " x ON p.id = x.id " +
			"JOIN " + tbl + " y ON c.id = y.id ORDER BY x.id"
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused a query PostgreSQL 17 answers: %v\n  SQL: %s",
					arm.name, err, sql)
			}
			if got := dajDigest(res, []string{"xid", "cdv", "pdv"}); got != pgBoth {
				t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
					arm.name, got, pgBoth, sql)
			}
		}
	})
}

// A UNION of two joins on the forced-shuffle lowering.
//
// This is NOT the fixture for the `UnionArm.DepStage` rewiring in
// `fuseScanShuffle` / `elideCoPartitionedExchanges`, and an earlier version of
// this comment claimed it was. Neither of these shapes reaches either loop:
// verified by panicking inside both and watching them pass, alongside six more
// union shapes (grouped aggregates, UNION and UNION ALL, with and without a
// join above). `physical.TestFuseScanShuffleDeclinesAUnionArmsExchange` and
// `physical.TestElideCoPartitionedExchangeRewiresAUnionArm` drive the two
// passes directly and are where that claim is actually gated.
//
// What these two DO gate is that a union whose arms are joins answers
// correctly on all three arms, which is the shape a wrong rewiring would break
// loudly at dispatch (ValidateNativeDAGShape refuses a plan whose arm and
// dependency disagree).
func TestUnionOfJoinsOnTheShuffledLowering(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := dajArms(t, ctx)

	const tbl = dbpTable
	for _, tc := range []struct {
		name, sql string
		want      int64
	}{
		{
			// Each arm is a JOIN whose sides go through an
			// exchange-repartition on the forced-shuffle lowering, and each
			// arm's probe scan carries a pushed filter — the shape
			// fuseScanShuffle absorbs.
			name: "union-all-of-two-joins",
			sql: "SELECT COUNT(*) AS n FROM (" +
				"SELECT t.id AS k FROM " + tbl + " t JOIN " + tbl + " u ON t.id = u.id WHERE t.a > 1 " +
				"UNION ALL " +
				"SELECT t.id AS k FROM " + tbl + " t JOIN " + tbl + " u ON t.id = u.id WHERE t.a < 1" +
				") z",
			want: 7,
		},
		{
			// The same with a DERIVED arm on each side, so the absorbed scan
			// carries a PROJECTION as well as a filter.
			name: "union-all-of-two-derived-joins",
			sql: "SELECT COUNT(*) AS n FROM (" +
				"SELECT c.dv AS k FROM (SELECT id, a * 2 AS dv FROM " + tbl + ") c " +
				"JOIN " + tbl + " u ON c.id = u.id WHERE c.dv > 1 " +
				"UNION ALL " +
				"SELECT c.dv AS k FROM (SELECT id, a * 3 AS dv FROM " + tbl + ") c " +
				"JOIN " + tbl + " u ON c.id = u.id WHERE c.dv < 1" +
				") z",
			want: 7,
		},
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
				if len(got) != 1 || got[0] != tc.want {
					t.Errorf("%s arm answered %v, PostgreSQL 17 answers %d\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}

// A qualified reference whose QUALIFIER collides with a COLUMN name, on three
// arms.
//
// `expr.ResolveColumnRef`'s last-resort suffix scan — the one that lets `c.dv`
// find `decpair.dv` after QualifyAllBuildCols renamed the build side (#762) —
// sat behind a guard that was INVERTED. The ROW-field-path arm above it has
// already returned for a ROW container, so the guard was reached only when the
// qualifier named a column that is NOT a ROW: an ordinary scalar an arm
// happens to have called `c`. Every qualified reference whose qualifier
// collided with such a name was then refused, the predicate was UNKNOWN on
// every row, and the shuffled arm answered 0:
//
//	WITH c AS (SELECT id, a * 2 AS dv FROM decpair)
//	SELECT COUNT(*) FROM decpair t
//	JOIN (SELECT id, b AS c FROM decpair) z ON z.id = t.id
//	JOIN c ON c.id = t.id WHERE c.dv > 1
//	-- PostgreSQL 5 · single 5 · DAG broadcast 5 · DAG shuffled 0
//
// `rowColumnNamed` is the whole of the refusal the guard was meant to be: it
// finds a ROW spelled `c` or `<qualifier>.c`, which is the only case a field
// path must not fall through. The controls are the same queries with the
// colliding arm publishing `zz` instead, and the BARE predicate spelling —
// both answered 5 throughout, which is what says the collision was the whole
// of it.
func TestQualifierCollidingWithAColumnNameResolves(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := dajArms(t, ctx)

	const tbl = dbpTable
	for _, tc := range []struct {
		name, sql string
		want      int64
	}{
		{"cte/qualifier-collides", "WITH c AS (SELECT id, a * 2 AS dv FROM " + tbl + ") " +
			"SELECT COUNT(*) AS n FROM " + tbl + " t JOIN (SELECT id, b AS c FROM " + tbl +
			") z ON z.id = t.id JOIN c ON c.id = t.id WHERE c.dv > 1", 5},
		{"control/cte/no-collision", "WITH c AS (SELECT id, a * 2 AS dv FROM " + tbl + ") " +
			"SELECT COUNT(*) AS n FROM " + tbl + " t JOIN (SELECT id, b AS zz FROM " + tbl +
			") z ON z.id = t.id JOIN c ON c.id = t.id WHERE c.dv > 1", 5},
		{"control/cte/bare-predicate", "WITH c AS (SELECT id, a * 2 AS dv FROM " + tbl + ") " +
			"SELECT COUNT(*) AS n FROM " + tbl + " t JOIN (SELECT id, b AS c FROM " + tbl +
			") z ON z.id = t.id JOIN c ON c.id = t.id WHERE dv > 1", 5},
		{"derived/qualifier-collides", "SELECT COUNT(*) AS n FROM " + tbl + " t " +
			"JOIN (SELECT id, b AS x FROM " + tbl + ") z ON z.id = t.id " +
			"JOIN (SELECT id, a * 2 AS dv FROM " + tbl + ") x ON x.id = t.id WHERE x.dv > 1", 5},
		{"control/derived/no-collision", "SELECT COUNT(*) AS n FROM " + tbl + " t " +
			"JOIN (SELECT id, b AS zz FROM " + tbl + ") z ON z.id = t.id " +
			"JOIN (SELECT id, a * 2 AS dv FROM " + tbl + ") x ON x.id = t.id WHERE x.dv > 1", 5},
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
				if len(got) != 1 || got[0] != tc.want {
					t.Errorf("%s arm answered %v, PostgreSQL 17 answers %d — a qualified "+
						"reference was refused because its qualifier collided with a column "+
						"name\n  SQL: %s", arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}

	// The PROJECTING forms of the same two shapes, which read the value
	// rather than counting rows.
	for _, tc := range []struct{ name, sql string }{
		{"cte/qualifier-collides/projecting",
			"WITH c AS (SELECT id, a * 2 AS dv FROM " + tbl + ") " +
				"SELECT c.id AS cid, c.dv AS dv FROM " + tbl + " t JOIN (SELECT id, b AS c FROM " +
				tbl + ") z ON z.id = t.id JOIN c ON c.id = t.id WHERE c.dv > 1 ORDER BY c.id"},
		{"derived/qualifier-collides/projecting",
			"SELECT x.id AS cid, x.dv AS dv FROM " + tbl + " t JOIN (SELECT id, b AS x FROM " +
				tbl + ") z ON z.id = t.id JOIN (SELECT id, a * 2 AS dv FROM " + tbl +
				") x ON x.id = t.id WHERE x.dv > 1 ORDER BY x.id"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			const want = "5 rows: 1|25.50;2|25.50;3|25.50;5|4.00;8|25.50;"
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, tc.sql)
				}
				if got := dajDigest(res, []string{"cid", "dv"}); got != want {
					t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
						arm.name, got, want, tc.sql)
				}
			}
		})
	}
}

// BOTH arms of a join publishing one alias, with BOTH projected, on three
// arms.
//
// A join's OutputFilter is spelled the way the query wrote the reference —
// `y.w` — while the arm that is the PROBE ships the column BARE, because
// nothing qualified it. The filter matched neither spelling, the column was
// dropped, and the projection above failed at dispatch:
//
//	SELECT x.id, x.w, y.w FROM (SELECT id, a AS w FROM decpair) x
//	JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id
//	JOIN decpair u ON x.id = u.id WHERE x.w > 1
//	-- PostgreSQL 5 rows · single 5 · DAG broadcast 5
//	-- DAG shuffled  ERROR  column "y.w" does not exist in the input schema
//
// On 376b2cac both DAG arms answered x's `w` under BOTH names instead, which
// is #742's capture; the round-1 fix made the broadcast arm right and left the
// shuffled one loud. `joinOutputSchemaWithMapping` already carried the
// qualified→bare direction of this test (a self-join's `n2.n_name` kept by a
// filter naming `n_name`); this is the mirror, and it is the third list in
// which only one half of that asymmetry was implemented.
//
// The `cte-arm-filter/…` entries are the SECOND face of the same capture,
// found by the round-3 review: move the residual filter off the probe arm and
// onto a CTE arm and it came back on the shuffled lowering, silently, while
// the derived-table spelling of the identical query stayed right. That pair
// is the whole diagnosis — a CTE's Project is a materialization fence, so its
// predicate stays as a Filter ABOVE the join, and the scope walk that picks
// the arm a qualified reference belongs to stopped at that Filter instead of
// descending through it. The entries cross the qualified and bare predicate
// spellings, the CTE first/middle/last, a computed and a plain-rename CTE
// body, chain lengths 3 and 4, the build-arm-only projection, a LIMIT wrapper,
// and the two controls that bound it (the derived-table spelling, and one
// where the arms publish different aliases so there is no collision at all).
//
// The two arms publish at the SAME DECIMAL scale in the asserted entries, so
// the gate cannot fail for #754's reason; the issue's own cross-scale spelling
// is carried separately with the single arm pinned.
func TestBothArmsPublishOneAliasProjectBothThreeArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := dajArms(t, ctx)

	const tbl = dbpTable
	const (
		armX = "(SELECT id, a AS w FROM " + tbl + ") x"
		armY = "(SELECT id, a * 100 AS w FROM " + tbl + ") y"
		cteC = "WITH c AS (SELECT id, a * 2 AS dv FROM " + tbl + ") "
	)
	for _, tc := range []struct {
		name, sql string
		cols      []string
		want      string
	}{
		{
			name: "both-projected/3-join",
			sql: "SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM " +
				"(SELECT id, a AS w FROM " + tbl + ") x " +
				"JOIN (SELECT id, a * 100 AS w FROM " + tbl + ") y ON x.id = y.id " +
				"JOIN " + tbl + " u ON x.id = u.id WHERE x.w > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw"},
			want: "5 rows: 1|12.75|1275.00;2|12.75|1275.00;3|12.75|1275.00;5|2.00|200.00;8|12.75|1275.00;",
		},
		{
			// Only the BUILD arm's column projected, which is the direction
			// the OutputFilter used to drop.
			name: "build-arm-only/3-join",
			sql: "SELECT x.id AS xid, y.w AS yw FROM (SELECT id, a AS w FROM " + tbl + ") x " +
				"JOIN (SELECT id, a * 100 AS w FROM " + tbl + ") y ON x.id = y.id " +
				"JOIN " + tbl + " u ON x.id = u.id WHERE x.w > 1 ORDER BY x.id",
			cols: []string{"xid", "yw"},
			want: "5 rows: 1|1275.00;2|1275.00;3|1275.00;5|200.00;8|1275.00;",
		},
		{
			name: "both-projected/4-join",
			sql: "SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM " +
				"(SELECT id, a AS w FROM " + tbl + ") x " +
				"JOIN (SELECT id, a * 100 AS w FROM " + tbl + ") y ON x.id = y.id " +
				"JOIN " + tbl + " u ON x.id = u.id JOIN " + tbl + " v ON x.id = v.id " +
				"WHERE x.w > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw"},
			want: "5 rows: 1|12.75|1275.00;2|12.75|1275.00;3|12.75|1275.00;5|2.00|200.00;8|12.75|1275.00;",
		},

		// --- The same shape with the residual filter moved OFF the probe arm
		// and onto a CTE arm. That one move is what put the capture back:
		// a CTE's Project is a materialization fence the predicate cannot be
		// pushed through, so the WHERE stays as a Filter ABOVE the join,
		// where the derived-table spelling of the same query pushes it into
		// the arm's own scan and leaves the join directly below the outer
		// Project. `relationScopeSubtree` stopped at that Filter, handed the
		// caller the WHOLE join subtree as `y`'s "scope", and the bare lookup
		// inside it took the first arm that answered — x's `w`, under BOTH
		// output columns. The derived spelling three entries down is the
		// control that says the CTE is the trigger and not the filter.
		{
			name: "cte-arm-filter/qualified/cte-last",
			sql: cteC + "SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM " + armX +
				" JOIN " + armY + " ON x.id = y.id JOIN c ON c.id = x.id " +
				"WHERE c.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw"},
			want: "5 rows: 1|12.75|1275.00;2|12.75|1275.00;3|12.75|1275.00;5|2.00|200.00;8|12.75|1275.00;",
		},
		{
			name: "cte-arm-filter/bare/cte-last",
			sql: cteC + "SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM " + armX +
				" JOIN " + armY + " ON x.id = y.id JOIN c ON c.id = x.id " +
				"WHERE dv > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw"},
			want: "5 rows: 1|12.75|1275.00;2|12.75|1275.00;3|12.75|1275.00;5|2.00|200.00;8|12.75|1275.00;",
		},
		{
			name: "cte-arm-filter/qualified/cte-middle",
			sql: cteC + "SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM " + armX +
				" JOIN c ON c.id = x.id JOIN " + armY + " ON x.id = y.id " +
				"WHERE c.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw"},
			want: "5 rows: 1|12.75|1275.00;2|12.75|1275.00;3|12.75|1275.00;5|2.00|200.00;8|12.75|1275.00;",
		},
		{
			name: "cte-arm-filter/qualified/cte-first",
			sql: cteC + "SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM c JOIN " + armX +
				" ON c.id = x.id JOIN " + armY + " ON x.id = y.id " +
				"WHERE c.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw"},
			want: "5 rows: 1|12.75|1275.00;2|12.75|1275.00;3|12.75|1275.00;5|2.00|200.00;8|12.75|1275.00;",
		},
		{
			name: "cte-arm-filter/qualified/4-join",
			sql: cteC + "SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM " + armX +
				" JOIN " + armY + " ON x.id = y.id JOIN " + tbl + " u ON x.id = u.id " +
				"JOIN c ON c.id = x.id WHERE c.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw"},
			want: "5 rows: 1|12.75|1275.00;2|12.75|1275.00;3|12.75|1275.00;5|2.00|200.00;8|12.75|1275.00;",
		},
		{
			// A PLAIN-RENAME CTE body. The two bodies reach the filter by
			// different routes (the pruner resolves a rename back to its
			// source column; a computed alias is materialized under the
			// alias itself), so both spellings belong here.
			name: "cte-arm-filter/rename-body/cte-last",
			sql: "WITH c AS (SELECT id, a AS dv FROM " + tbl + ") " +
				"SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM " + armX +
				" JOIN " + armY + " ON x.id = y.id JOIN c ON c.id = x.id " +
				"WHERE c.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw"},
			want: "5 rows: 1|12.75|1275.00;2|12.75|1275.00;3|12.75|1275.00;5|2.00|200.00;8|12.75|1275.00;",
		},
		{
			// Only the BUILD arm's column projected, under the CTE filter —
			// the capture and the OutputFilter drop at once.
			name: "cte-arm-filter/build-arm-only",
			sql: cteC + "SELECT x.id AS xid, y.w AS yw FROM " + armX +
				" JOIN " + armY + " ON x.id = y.id JOIN c ON c.id = x.id " +
				"WHERE c.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "yw"},
			want: "5 rows: 1|1275.00;2|1275.00;3|1275.00;5|200.00;8|1275.00;",
		},
		{
			// The CTE arm with NO filter above it: the Filter node is what
			// the walk stopped at, so its absence is the control that says so.
			name: "cte-arm/no-filter",
			sql: cteC + "SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM " + armX +
				" JOIN " + armY + " ON x.id = y.id JOIN c ON c.id = x.id ORDER BY x.id",
			cols: []string{"xid", "xw", "yw"},
			want: "9 rows: 1|12.75|1275.00;2|12.75|1275.00;3|12.75|1275.00;4|-0.01|-1.00;" +
				"5|2.00|200.00;6|0.00|0.00;7||;8|12.75|1275.00;9||;",
		},
		{
			// THE TELL: the same query with c a DERIVED TABLE. Its predicate
			// pushes into the arm's own scan, no Filter sits above the join,
			// and this spelling was correct throughout — which is what makes
			// the CTE fence the mechanism rather than the collision.
			name: "cte-arm-filter/derived-c-control",
			sql: "SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM " + armX +
				" JOIN " + armY + " ON x.id = y.id " +
				"JOIN (SELECT id, a * 2 AS dv FROM " + tbl + ") c ON c.id = x.id " +
				"WHERE c.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yw"},
			want: "5 rows: 1|12.75|1275.00;2|12.75|1275.00;3|12.75|1275.00;5|2.00|200.00;8|12.75|1275.00;",
		},
		{
			// And the NO-COLLISION control: one CTE named `k`, the two arms
			// publishing DIFFERENT aliases. Correct on every tree, so a
			// failure here is not this mechanism.
			name: "cte-arm-filter/no-collision-control",
			sql: "WITH k AS (SELECT id, a * 2 AS dv FROM " + tbl + ") " +
				"SELECT x.id AS xid, x.w AS xw, y.v AS yv FROM " + armX +
				" JOIN (SELECT id, a * 100 AS v FROM " + tbl + ") y ON x.id = y.id " +
				"JOIN k ON k.id = x.id WHERE k.dv > 1 ORDER BY x.id",
			cols: []string{"xid", "xw", "yv"},
			want: "5 rows: 1|12.75|1275.00;2|12.75|1275.00;3|12.75|1275.00;5|2.00|200.00;8|12.75|1275.00;",
		},
		{
			// A LIMIT above the ORDER BY, so the wrapper between the outer
			// SELECT list and the join is two nodes deep rather than one.
			name: "cte-arm-filter/limit-wrapper",
			sql: cteC + "SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM " + armX +
				" JOIN " + armY + " ON x.id = y.id JOIN c ON c.id = x.id " +
				"WHERE c.dv > 1 ORDER BY x.id LIMIT 3",
			cols: []string{"xid", "xw", "yw"},
			want: "3 rows: 1|12.75|1275.00;2|12.75|1275.00;3|12.75|1275.00;",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused a query PostgreSQL 17 answers: %v\n  SQL: %s",
						arm.name, err, tc.sql)
				}
				if got := dajDigest(res, tc.cols); got != tc.want {
					t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}

	// The issue's own CROSS-SCALE spelling. It used to carry a #754 pin: the
	// single arm rendered x's scale-2 column at y's scale 4 — right digits,
	// wrong typmod. That is the same disagreement #706 is: the VALUE came
	// through `x.w` and the DECLARATION through the first bare `w`, which is
	// the other arm's column, so the two arms of one join described one
	// output. The projection reads both from the qualified spelling now and
	// every arm renders PostgreSQL's.
	t.Run("both-projected/cross-decimal-scales", func(t *testing.T) {
		sql := "SELECT x.id AS xid, x.w AS xw, y.w AS yw FROM " +
			"(SELECT id, a AS w FROM " + tbl + ") x " +
			"JOIN (SELECT id, b * 100 AS w FROM " + tbl + ") y ON x.id = y.id " +
			"JOIN " + tbl + " u ON x.id = u.id WHERE x.w > 1 ORDER BY x.id"
		const want = "5 rows: 1|12.75|1275.0000;2|12.75|1275.0100;3|12.75|1274.9900;" +
			"5|2.00|1000.0000;8|12.75|;" // PostgreSQL 17
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused a query PostgreSQL 17 answers: %v\n  SQL: %s",
					arm.name, err, sql)
			}
			if got := dajDigest(res, []string{"xid", "xw", "yw"}); got != want {
				t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n"+
					" — two arms publishing one alias described one output\n  SQL: %s",
					arm.name, got, want, sql)
			}
		}
	})
}
