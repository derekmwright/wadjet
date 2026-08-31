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
		{"single", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }},
		{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }},
		{"dag-shuffled", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }},
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
	// It is kept apart from the entries above because it also carries #754: on
	// the SINGLE path, two arms publishing one alias make x's scale-2 column
	// render at z's scale 4. The VALUES are right there and the typmod is not,
	// so that arm is PINNED at the wrong rendering and this gate FAILS the day
	// it agrees — which is that fix's proof, not this one's.
	t.Run("766/two-arms-at-different-decimal-scales/projecting", func(t *testing.T) {
		sql := "SELECT x.w AS xw FROM (SELECT id, a AS w FROM " + tbl + ") x " +
			"JOIN (SELECT id, b AS w FROM " + tbl + ") z ON x.id = z.id " +
			"JOIN " + tbl + " u ON x.id = u.id WHERE x.w > 1 ORDER BY x.id"
		const want = "5 rows: 12.75;12.75;12.75;2.00;12.75;"             // PostgreSQL 17
		const pinned = "5 rows: 12.7500;12.7500;12.7500;2.0000;12.7500;" // TODO(#754)
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused a query PostgreSQL 17 answers: %v\n  SQL: %s",
					arm.name, err, sql)
			}
			got := dajDigest(res, []string{"xw"})
			exp := want
			if arm.name == "single" {
				exp = pinned
			}
			if got == exp {
				continue
			}
			if arm.name == "single" && got == want {
				t.Errorf("the single arm now renders xw at its OWN scale — #754 is fixed, "+
					"delete this pin and assert PostgreSQL's values on every arm\n  SQL: %s", sql)
				continue
			}
			t.Errorf("%s arm answered\n  %s\nwant\n  %s\n  SQL: %s", arm.name, got, exp, sql)
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

	// GROUP BY on the QUALIFIED derived alias, which both DAG arms collapse
	// into ONE NULL group. Identical on 376b2cac, identical at ONE join and
	// in the DERIVED spelling, and correct in the BARE spelling — so it is
	// the qualified GROUP-BY key's own resolution and not the chain. Pinned
	// rather than described, so the day it agrees this gate FAILS.
	t.Run("2join/group-by-qualified-alias", func(t *testing.T) {
		sql := cte + "SELECT c.dv AS dv, COUNT(*) AS n FROM " + chain2 + "GROUP BY c.dv ORDER BY c.dv"
		const want = "5 rows: -0.02|1;0.00|1;4.00|1;25.50|4;|2;" // PostgreSQL 17
		const pinned = "1 rows: |9;"                             // TODO: unfiled residual
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			got := dajDigest(res, []string{"dv", "n"})
			if arm.name == "single" {
				if got != want {
					t.Errorf("the single arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
						got, want, sql)
				}
				continue
			}
			switch got {
			case want:
				t.Errorf("the %s arm now groups by the qualified derived alias — the residual "+
					"is fixed, delete this pin and assert PostgreSQL's rows on every arm"+
					"\n  SQL: %s", arm.name, sql)
			case pinned:
				t.Logf("known residual, NOT gated: the %s arm collapses GROUP BY c.dv into one "+
					"NULL group (identical on 376b2cac, at one join, and in the derived "+
					"spelling; the BARE spelling is correct on every arm)", arm.name)
			default:
				t.Errorf("the %s arm answered\n  %s\nwhich is neither the pinned\n  %s\n"+
					"nor PostgreSQL's\n  %s\n  SQL: %s", arm.name, got, pinned, want, sql)
			}
		}
	})
	// …and the BARE spelling beside it, which is correct on every arm and is
	// what says the qualifier is the whole of that residual.
	t.Run("2join/group-by-bare-alias", func(t *testing.T) {
		sql := cte + "SELECT dv, COUNT(*) AS n FROM " + chain2 + "GROUP BY dv ORDER BY dv"
		const want = "5 rows: -0.02|1;0.00|1;4.00|1;25.50|4;|2;"
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
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
	}{
		{
			// The CONTROL that bounds it: no join, correct on 376b2cac.
			name: "no-join",
			sql:  "SELECT id AS xid, c_row.b AS fb FROM " + nested + " WHERE id < 5 ORDER BY id",
			cols: []string{"xid", "fb"},
			want: "5 rows: 0|0;1|11;2|;3|;4|44;",
		},
		{
			name: "self-join",
			sql: "SELECT x.id AS xid, c_row.b AS fb FROM " + nested + " x JOIN " + nested +
				" y ON x.id = y.id WHERE x.id < 5 ORDER BY x.id",
			cols: []string{"xid", "fb"},
			want: "5 rows: 0|0;1|11;2|;3|;4|44;",
		},
		{
			// AMBIGUOUS on purpose: a self-join puts a `c_row` on BOTH sides.
			// PostgreSQL rejects the query (no FROM-clause entry for c_row);
			// wadjet answers the FIRST arm's container, deterministically,
			// because the resolver's exact-name lookup wins before the
			// qualified fallback is reached. That is the same rule a bare
			// column reference follows and it is recorded here rather than
			// described as a refusal — an earlier commit body claimed "two
			// ROW columns spelled alike decline rather than pick one", which
			// is true only when NEITHER is spelled bare.
			name: "ambiguous-container-picks-the-probe-side",
			sql: "SELECT x.id AS xid, c_row.b AS fb FROM " + nested + " x JOIN " + nested +
				" y ON x.id = y.id WHERE x.id < 5 ORDER BY x.id",
			cols: []string{"xid", "fb"},
			want: "5 rows: 0|0;1|11;2|;3|;4|44;",
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
	for _, tc := range []struct {
		name, sql string
		want      int64
	}{
		{"filter/no-join", "SELECT COUNT(*) AS n FROM " + nested + " WHERE c_row.b > 20", 3537},
		{"filter/self-join", "SELECT COUNT(*) AS n FROM " + nested + " x JOIN " + nested +
			" y ON x.id = y.id WHERE c_row.b > 20", 3537},
		{"filter/dimension-join", "SELECT COUNT(*) AS n FROM " + nested + " x JOIN " +
			typematrix.Dim + " d ON x.id = d.k WHERE c_row.b > 20", 4},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
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

// A UNION arm whose producer sits behind an exchange that a fusion or an
// elision DELETES (METHOD 10).
//
// `fuseScanShuffle` and `elideCoPartitionedExchanges` both remove a stage and
// rewire every reference to it. A union arm names its producer in
// `UnionArm.DepStage`, which is the same kind of second reference a chained
// join's `BuildDepStage` is, and `ValidateNativeDAGShape` asserts that it
// equals the corresponding `Dependencies` entry — so a pass that rewires one
// and not the other produces a plan that fails validation at dispatch. Both
// passes now rewire it; nothing in the corpus put a union arm in that position
// before, which is the impossibility this fixture attempts.
func TestUnionArmBehindAnAbsorbedExchange(t *testing.T) {
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
