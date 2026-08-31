package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

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

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	arms := []struct {
		name string
		run  func(string) (*oracle.Result, error)
	}{
		{"single", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }},
		{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }},
		{"dag-shuffled", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }},
	}

	const tbl = dbpTable
	// row is one expected output row rendered as `col=value` pairs.
	type row map[string]string

	for _, tc := range []struct {
		name string
		sql  string
		want []row
	}{
		// --- #755: a chained join over DERIVED arms on the shuffled arm.
		{
			name: "755/base-between-two-derived-arms",
			sql: "SELECT p.w AS pw, q.w AS qw FROM (SELECT id, a + 1 AS w FROM " + tbl + ") p " +
				"JOIN " + tbl + " y ON p.id = y.id " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") q ON p.id = q.id ORDER BY p.id",
			want: []row{{"pw": "13.75", "qw": "38.25"}, {"pw": "13.75", "qw": "38.25"},
				{"pw": "13.75", "qw": "38.25"}, {"pw": "0.99", "qw": "-0.03"},
				{"pw": "3.00", "qw": "6.00"}, {"pw": "1.00", "qw": "0.00"}},
		},
		{
			name: "755/three-derived-arms-distinct-aliases",
			sql: "SELECT p.w1 AS pw, q.w2 AS qw, r.w3 AS rw FROM " +
				"(SELECT id, a + 1 AS w1 FROM " + tbl + ") p " +
				"JOIN (SELECT id, a * 3 AS w2 FROM " + tbl + ") q ON p.id = q.id " +
				"JOIN (SELECT id, a * 5 AS w3 FROM " + tbl + ") r ON p.id = r.id ORDER BY p.id",
			want: []row{{"pw": "13.75", "qw": "38.25", "rw": "63.75"},
				{"pw": "13.75", "qw": "38.25", "rw": "63.75"},
				{"pw": "13.75", "qw": "38.25", "rw": "63.75"},
				{"pw": "0.99", "qw": "-0.03", "rw": "-0.05"},
				{"pw": "3.00", "qw": "6.00", "rw": "10.00"},
				{"pw": "1.00", "qw": "0.00", "rw": "0.00"}},
		},
		{
			name: "755/window-arm-base-between-computed-arm",
			sql: "SELECT p.w AS pw, q.w AS qw FROM (SELECT id, SUM(a) OVER () AS w FROM " + tbl + ") p " +
				"JOIN " + tbl + " y ON p.id = y.id " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") q ON p.id = q.id ORDER BY p.id",
			want: []row{{"pw": "52.99", "qw": "38.25"}, {"pw": "52.99", "qw": "38.25"},
				{"pw": "52.99", "qw": "38.25"}, {"pw": "52.99", "qw": "-0.03"},
				{"pw": "52.99", "qw": "6.00"}, {"pw": "52.99", "qw": "0.00"}},
		},
		{
			name: "755/wrapped-window-arm-base-between-computed-arm",
			sql: "SELECT x.w AS xw, z.w AS zw FROM " +
				"(SELECT id, SUM(a) OVER () + 0 AS w FROM " + tbl + ") x " +
				"JOIN " + tbl + " y ON x.id = y.id " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") z ON x.id = z.id ORDER BY x.id",
			want: []row{{"xw": "52.99", "zw": "38.25"}, {"xw": "52.99", "zw": "38.25"},
				{"xw": "52.99", "zw": "38.25"}, {"xw": "52.99", "zw": "-0.03"},
				{"xw": "52.99", "zw": "6.00"}, {"xw": "52.99", "zw": "0.00"}},
		},
		{
			// The CONTROLS #755 names: the TWO-way spelling of the same
			// query, and a three-way join of BASE tables. Both succeeded on
			// every arm throughout, which is what said the trigger was a
			// chained join over DERIVED arms and not the chain itself.
			name: "755/control/two-way-derived",
			sql: "SELECT p.w AS pw, q.w AS qw FROM (SELECT id, a + 1 AS w FROM " + tbl + ") p " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") q ON p.id = q.id ORDER BY p.id",
			want: []row{{"pw": "13.75", "qw": "38.25"}, {"pw": "13.75", "qw": "38.25"},
				{"pw": "13.75", "qw": "38.25"}, {"pw": "0.99", "qw": "-0.03"},
				{"pw": "3.00", "qw": "6.00"}, {"pw": "1.00", "qw": "0.00"}},
		},
		{
			name: "755/control/three-way-base-tables",
			sql: "SELECT x.a AS xa, y.a AS ya, z.a AS za FROM " + tbl + " x " +
				"JOIN " + tbl + " y ON x.id = y.id JOIN " + tbl + " z ON x.id = z.id ORDER BY x.id",
			want: []row{{"xa": "12.75", "ya": "12.75", "za": "12.75"},
				{"xa": "12.75", "ya": "12.75", "za": "12.75"},
				{"xa": "12.75", "ya": "12.75", "za": "12.75"},
				{"xa": "-0.01", "ya": "-0.01", "za": "-0.01"},
				{"xa": "2.00", "ya": "2.00", "za": "2.00"},
				{"xa": "0.00", "ya": "0.00", "za": "0.00"}},
		},

		// --- #753: a COMPUTED column above a join OF JOINS.
		{
			name: "753/two-derived-arms-plus-base-table",
			sql: "SELECT p.w AS pw, q.w AS qw FROM (SELECT id, a + 1 AS w FROM " + tbl + ") p " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") q ON p.id = q.id " +
				"JOIN " + tbl + " y ON p.id = y.id ORDER BY p.id",
			want: []row{{"pw": "13.75", "qw": "38.25"}, {"pw": "13.75", "qw": "38.25"},
				{"pw": "13.75", "qw": "38.25"}, {"pw": "0.99", "qw": "-0.03"},
				{"pw": "3.00", "qw": "6.00"}, {"pw": "1.00", "qw": "0.00"}},
		},
		{
			name: "753/three-derived-arms-one-alias",
			sql: "SELECT p.w AS pw, q.w AS qw, r.w AS rw FROM " +
				"(SELECT id, a + 1 AS w FROM " + tbl + ") p " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") q ON p.id = q.id " +
				"JOIN (SELECT id, a * 5 AS w FROM " + tbl + ") r ON p.id = r.id ORDER BY p.id",
			want: []row{{"pw": "13.75", "qw": "38.25", "rw": "63.75"},
				{"pw": "13.75", "qw": "38.25", "rw": "63.75"},
				{"pw": "13.75", "qw": "38.25", "rw": "63.75"},
				{"pw": "0.99", "qw": "-0.03", "rw": "-0.05"},
				{"pw": "3.00", "qw": "6.00", "rw": "10.00"},
				{"pw": "1.00", "qw": "0.00", "rw": "0.00"}},
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
			want: []row{{"pw": "52.99", "qw": "-0.01", "rw": "12.75"},
				{"pw": "52.99", "qw": "-0.01", "rw": "12.75"},
				{"pw": "52.99", "qw": "-0.01", "rw": "12.75"},
				{"pw": "52.99", "qw": "-0.01", "rw": "12.75"},
				{"pw": "52.99", "qw": "-0.01", "rw": "12.75"},
				{"pw": "52.99", "qw": "-0.01", "rw": "12.75"}},
		},
		{
			// The control #753 names: three arms selecting a BASE column with
			// no rename at all.
			name: "753/control/three-arms-no-rename",
			sql: "SELECT p.a AS pw, q.a AS qw, r.a AS rw FROM " +
				"(SELECT id, a FROM " + tbl + ") p JOIN (SELECT id, a FROM " + tbl + ") q " +
				"ON p.id = q.id JOIN (SELECT id, a FROM " + tbl + ") r ON p.id = r.id ORDER BY p.id",
			want: []row{{"pw": "12.75", "qw": "12.75", "rw": "12.75"},
				{"pw": "12.75", "qw": "12.75", "rw": "12.75"},
				{"pw": "12.75", "qw": "12.75", "rw": "12.75"},
				{"pw": "-0.01", "qw": "-0.01", "rw": "-0.01"},
				{"pw": "2.00", "qw": "2.00", "rw": "2.00"},
				{"pw": "0.00", "qw": "0.00", "rw": "0.00"}},
		},

		// --- #766: PROJECTING the column, whose COUNT twin was already right.
		{
			name: "766/computed-over-nested-aggregate/projecting",
			sql: "WITH c AS (SELECT id, sv * 2 AS dv FROM " +
				"(SELECT id, SUM(a) AS sv FROM " + tbl + " GROUP BY id) z) " +
				"SELECT c.dv AS d FROM c JOIN " + tbl + " t ON c.id = t.id " +
				"JOIN " + tbl + " u ON c.id = u.id WHERE c.dv > 1 ORDER BY c.id",
			want: []row{{"d": "25.50"}, {"d": "25.50"}, {"d": "25.50"}, {"d": "4.00"}, {"d": "25.50"}},
		},
		{
			name: "766/two-arms-one-alias/projecting",
			sql: "SELECT x.w AS xw FROM (SELECT id, a AS w FROM " + tbl + ") x " +
				"JOIN (SELECT id, a * 3 AS w FROM " + tbl + ") z ON x.id = z.id " +
				"JOIN " + tbl + " u ON x.id = u.id WHERE x.w > 1 ORDER BY x.id",
			want: []row{{"xw": "12.75"}, {"xw": "12.75"}, {"xw": "12.75"},
				{"xw": "2.00"}, {"xw": "12.75"}},
		},
		{
			name: "766/computed-over-window/projecting",
			sql: "WITH c AS (SELECT id, SUM(f) OVER () + 0 AS dv FROM " + tbl + ") " +
				"SELECT c.dv AS d FROM c JOIN " + tbl + " t ON c.id = t.id " +
				"JOIN " + tbl + " u ON c.id = u.id WHERE c.dv > 1 ORDER BY c.id",
			want: []row{{"d": "138.75"}, {"d": "138.75"}, {"d": "138.75"},
				{"d": "138.75"}, {"d": "138.75"}, {"d": "138.75"}},
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
				if len(res.Rows) < len(tc.want) {
					t.Fatalf("%s arm returned %d rows, PostgreSQL 17 returns at least %d\n  SQL: %s",
						arm.name, len(res.Rows), len(tc.want), tc.sql)
				}
				for i, want := range tc.want {
					for col, v := range want {
						got := fmt.Sprintf("%v", res.Rows[i][col])
						if got != v {
							t.Errorf("%s arm row %d: %s = %q, PostgreSQL 17 answers %q — a "+
								"derived arm's column was lost or took another arm's value "+
								"above a join chain\n  SQL: %s", arm.name, i, col, got, v, tc.sql)
						}
					}
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
		// PostgreSQL 17: five rows, 12.75 12.75 12.75 2.00 12.75.
		want := []string{"12.75", "12.75", "12.75", "2.00", "12.75"}
		pinned := []string{"12.7500", "12.7500", "12.7500", "2.0000", "12.7500"}
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused a query PostgreSQL 17 answers: %v\n  SQL: %s",
					arm.name, err, sql)
			}
			if len(res.Rows) != len(want) {
				t.Fatalf("%s arm returned %d rows, PostgreSQL 17 returns %d\n  SQL: %s",
					arm.name, len(res.Rows), len(want), sql)
			}
			exp := want
			if arm.name == "single" {
				exp = pinned // TODO(#754)
			}
			for i := range exp {
				got := fmt.Sprintf("%v", res.Rows[i]["xw"])
				if got == exp[i] {
					continue
				}
				if arm.name == "single" && got == want[i] {
					t.Errorf("the single arm now renders xw at its OWN scale (%q) — #754 is "+
						"fixed, delete this pin and assert PostgreSQL's values on every arm"+
						"\n  SQL: %s", got, sql)
					break
				}
				t.Errorf("%s arm row %d: xw = %q, want %q\n  SQL: %s", arm.name, i, got, exp[i], sql)
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
