// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// WHERE POSTGRESQL REFUSES, WADJET NEVER ANSWERS A MISLEADING VALUE — arc BR's
// five-arm table (#1249 #1073 #1205 #1061 #1060 #1065 #1233 #1236 #1216).
//
// Every refusal is PostgreSQL 17.11's SQLSTATE and sentence, measured live
// (postgres:17-alpine, --locale=C) over rows identical to `lat_ord` /
// `lat_item`, and over typed columns of the same PostgreSQL types as
// `typemx` / `typemx_nested` for the type cells. Each refusal is a property of
// the STATEMENT, decided before a row is read, so it must be the same SQLSTATE
// on all five arms: an arm that answers never asked, and an arm that fails
// with a task error decided it at run time.
//
// The controls are not decoration. Each is one edit away from a refusing cell
// — a numeric SUM beside a text one, a star over a fully grouped relation, a
// set operation ordered by its own result name, two columns of one class — and
// a table that only refuses proves a ban, not a rule. `want` is PostgreSQL's
// row set where the fixture is PostgreSQL's; `same` controls over the type
// matrix (whose rows PostgreSQL does not hold) must answer, identically on
// every arm.
type brArmCell struct {
	name, sql string
	// state and msg, when set, are PostgreSQL's refusal.
	state, msg string
	// want is PostgreSQL's row set (r1RenderRows form).
	want string
	// same marks a control over the type matrix: it must answer, and every
	// arm must answer the same.
	same bool
}

func brArmCells() []brArmCell {
	return []brArmCell{
		// --- #1205 / #1216 item 3: placement, in PostgreSQL's node order ---
		{name: "1205/rowNumberInHaving",
			sql:   "SELECT id, COUNT(*) AS n FROM lat_ord GROUP BY id HAVING row_number() OVER () = 1",
			state: "42P20", msg: "window functions are not allowed in HAVING"},
		{name: "1205/countOverInHaving",
			sql:   "SELECT id FROM lat_ord GROUP BY id HAVING COUNT(*) OVER () > 1",
			state: "42P20", msg: "window functions are not allowed in HAVING"},
		{name: "1205/notWindowInHaving",
			sql:   "SELECT id FROM lat_ord GROUP BY id HAVING NOT (row_number() OVER () = 1)",
			state: "42P20", msg: "window functions are not allowed in HAVING"},
		{name: "1205/noGroupBy",
			sql:   "SELECT COUNT(*) AS n FROM lat_ord HAVING row_number() OVER () = 1",
			state: "42P20", msg: "window functions are not allowed in HAVING"},
		{name: "1205/nestedExistsBody",
			sql:   "SELECT id FROM lat_ord GROUP BY id HAVING EXISTS (SELECT 1 FROM lat_item i GROUP BY i.id HAVING row_number() OVER () = 1)",
			state: "42P20", msg: "window functions are not allowed in HAVING"},
		{name: "1205/nameBeforeWindow",
			sql:   "SELECT id FROM lat_ord GROUP BY id HAVING zz > 0 AND row_number() OVER () = 1",
			state: "42703", msg: `"zz"`},
		{name: "1216/aggregateBeforeName",
			sql:   "SELECT id FROM lat_ord WHERE SUM(total) > 0 AND zz > 0",
			state: "42803", msg: "aggregate functions are not allowed in WHERE"},
		{name: "1216/nameBeforeAggregate",
			sql:   "SELECT id FROM lat_ord WHERE zz > 0 AND SUM(total) > 0",
			state: "42703", msg: `"zz"`},
		{name: "1205ok/windowBesideGroups",
			sql:  "SELECT id, SUM(total) OVER () AS s FROM lat_ord GROUP BY id, total HAVING SUM(total) > 1",
			want: "rows=2 1,350 | 2,350"},

		// --- #1236: a set operation's ORDER BY names a result column ------
		{name: "1236/qualifierNamesNothing",
			sql:   "SELECT a.id FROM lat_ord a UNION ALL SELECT b.id FROM lat_item b ORDER BY zz.id",
			state: "42P01", msg: `missing FROM-clause entry for table "zz"`},
		{name: "1236/qualifierNamesAnArm",
			sql:   "SELECT a.id FROM lat_ord a UNION ALL SELECT b.id FROM lat_item b ORDER BY a.id",
			state: "42P01", msg: `missing FROM-clause entry for table "a"`},
		{name: "1236/qualifierNamesATable",
			sql:   "SELECT a.id FROM lat_ord a UNION ALL SELECT b.id FROM lat_item b ORDER BY lat_ord.id",
			state: "42P01", msg: `missing FROM-clause entry for table "lat_ord"`},
		{name: "1236/intersect",
			sql:   "SELECT a.id FROM lat_ord a INTERSECT SELECT b.id FROM lat_item b ORDER BY zz.id",
			state: "42P01", msg: `missing FROM-clause entry for table "zz"`},
		{name: "1236/except",
			sql:   "SELECT a.id FROM lat_ord a EXCEPT SELECT b.id FROM lat_item b ORDER BY zz.id",
			state: "42P01", msg: `missing FROM-clause entry for table "zz"`},
		{name: "1236/starArms",
			sql:   "SELECT * FROM lat_ord a UNION ALL SELECT * FROM lat_ord b ORDER BY zz.id",
			state: "42P01", msg: `missing FROM-clause entry for table "zz"`},
		{name: "1236/insideDerived",
			sql:   "SELECT * FROM (SELECT a.id FROM lat_ord a UNION ALL SELECT b.id FROM lat_item b ORDER BY zz.id) s",
			state: "42P01", msg: `missing FROM-clause entry for table "zz"`},
		{name: "1236/unknownName",
			sql:   "SELECT a.id FROM lat_ord a UNION ALL SELECT b.id FROM lat_item b ORDER BY nosuch",
			state: "42703", msg: `column "nosuch" does not exist`},
		{name: "1236/expression",
			sql:   "SELECT a.id FROM lat_ord a UNION ALL SELECT b.id FROM lat_item b ORDER BY id + 1",
			state: "0A000", msg: "invalid UNION/INTERSECT/EXCEPT ORDER BY clause"},
		{name: "1236ok/resultName",
			sql:  "SELECT a.id FROM lat_ord a UNION ALL SELECT b.id FROM lat_item b ORDER BY id",
			want: "rows=7 1 | 1 | 2 | 2 | 3 | 3 | 4"},
		{name: "1236ok/twoColumnsNameAndPosition",
			sql:  "SELECT a.id, a.customer FROM lat_ord a UNION ALL SELECT b.id, b.product FROM lat_item b ORDER BY customer, 1",
			want: "rows=7 1,Alice | 1,Widget | 2,Bob | 2,Gadget | 3,Carol | 3,Widget | 4,Doohickey"},
		{name: "1236ok/armOwnOrderBy",
			sql:  "(SELECT a.id FROM lat_ord a ORDER BY a.id LIMIT 2) UNION ALL SELECT b.id FROM lat_item b",
			want: "rows=6 1 | 1 | 2 | 2 | 3 | 4"},

		// --- #1233: a star in a grouped query, and HAVING's one group -------
		{name: "1233/starHaving",
			sql:   "SELECT * FROM lat_ord o HAVING COUNT(*) > 0",
			state: "42803", msg: `column "o.id" must appear in the GROUP BY clause`},
		{name: "1233/qualifiedStarHaving",
			sql:   "SELECT o.* FROM lat_ord o HAVING COUNT(*) > 0",
			state: "42803", msg: `column "o.id" must appear in the GROUP BY clause`},
		{name: "1233/starHavingTrue",
			sql:   "SELECT * FROM lat_ord o HAVING true",
			state: "42803", msg: `column "o.id" must appear in the GROUP BY clause`},
		{name: "1233/starBesideAggregate",
			sql:   "SELECT *, COUNT(*) AS n FROM lat_ord",
			state: "42803", msg: `column "lat_ord.id" must appear in the GROUP BY clause`},
		{name: "1233/starOrderByAggregate",
			sql:   "SELECT * FROM lat_ord ORDER BY MAX(id)",
			state: "42803", msg: `column "lat_ord.id" must appear in the GROUP BY clause`},
		{name: "1233/starGroupedByOneColumn",
			sql:   "SELECT * FROM lat_ord GROUP BY id",
			state: "42803", msg: `column "lat_ord.customer" must appear in the GROUP BY clause`},
		{name: "1233/starOverJoin",
			sql:   "SELECT * FROM lat_ord o JOIN lat_item i ON o.id = i.order_id HAVING COUNT(*) > 0",
			state: "42803", msg: `column "o.id" must appear in the GROUP BY clause`},
		{name: "1233/starOverDerived",
			sql:   "SELECT * FROM (SELECT id FROM lat_ord) x HAVING COUNT(*) > 0",
			state: "42803", msg: `column "x.id" must appear in the GROUP BY clause`},
		{name: "1233/qualifiedStarOtherSide",
			sql:   "SELECT i.*, COUNT(*) AS n FROM lat_ord o JOIN lat_item i ON o.id = i.order_id GROUP BY o.id",
			state: "42803", msg: `column "i.id" must appear in the GROUP BY clause`},
		{name: "1233/groupByNameBeforeStar",
			sql:   "SELECT * FROM lat_ord o GROUP BY zz.id",
			state: "42P01", msg: `missing FROM-clause entry for table "zz"`},
		{name: "1233ok/starFullyGrouped",
			sql:  "SELECT *, COUNT(*) AS n FROM lat_ord GROUP BY id, customer, total",
			want: "rows=3 1,Alice,150,1 | 2,Bob,200,1 | 3,Carol,0,1"},
		{name: "1233ok/qualifiedStarFullyGrouped",
			sql:  "SELECT o.*, COUNT(*) AS n FROM lat_ord o JOIN lat_item i ON o.id = i.order_id GROUP BY o.id, o.customer, o.total",
			want: "rows=2 1,Alice,150,2 | 2,Bob,200,2"},
		// HAVING makes the input ONE group: these answered a row per input
		// row (or the HAVING was never applied) before.
		{name: "1233ok/havingCountOneGroup",
			sql: "SELECT 1 AS k FROM lat_ord HAVING COUNT(*) > 0", want: "rows=1 1"},
		{name: "1233ok/havingCountFalse",
			sql: "SELECT 1 AS k FROM lat_ord HAVING COUNT(*) > 5", want: "rows=0 "},
		{name: "1233ok/havingTrueOneGroup",
			sql: "SELECT 1 AS k FROM lat_ord HAVING true", want: "rows=1 1"},
		{name: "1233ok/havingOverEmptyInput",
			sql: "SELECT 1 AS k FROM lat_ord WHERE false HAVING true", want: "rows=1 1"},
		{name: "1233ok/havingCountOverEmptyInput",
			sql: "SELECT 1 AS k FROM lat_ord WHERE false HAVING COUNT(*) = 0", want: "rows=1 1"},
		{name: "1233ok/havingMaxOrdered",
			sql: "SELECT 'x' AS k FROM lat_ord HAVING MAX(id) > 0 ORDER BY 1", want: "rows=1 x"},
	}
}

// brRunArm runs one statement on one arm, returning the rendered rows or the
// error — keeping the error itself, not its text, so the SQLSTATE is read.
type brArm struct {
	name string
	run  func(string) (string, error)
}

func brArms(t *testing.T, ctx context.Context) []brArm {
	t.Helper()
	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM, func(w *worker.Config) { w.MorselWorkers = 4 })
	runSingle := func(db *wadjet.DB) func(string) (string, error) {
		return func(sql string) (string, error) {
			res, err := tmdRunSingle(ctx, db, sql)
			if err != nil {
				return "", err
			}
			return brRender(res), nil
		}
	}
	runDAG := func(c *Coordinator) func(string) (string, error) {
		return func(sql string) (string, error) {
			res, err := tmdRunDAG(ctx, c, sql)
			if err != nil {
				return "", err
			}
			return brRender(res), nil
		}
	}
	return []brArm{
		{"single", runSingle(single)},
		{"spilled512k", runSingle(spilled)},
		{"dag", runDAG(coord)},
		{"dag-shuffled", runDAG(coordB)},
		{"dag-morsel4", runDAG(coordM)},
	}
}

// brRender renders a result positionally: RowValues where the harness has
// them (a duplicate output name), the name-keyed rows in column order
// otherwise — the single path fills RowValues only for the first case.
func brRender(res *oracle.Result) string {
	cells := res.RowValues
	if len(cells) == 0 {
		for _, r := range res.Rows {
			row := make([]any, len(res.Columns))
			for j, c := range res.Columns {
				row[j] = r[c]
			}
			cells = append(cells, row)
		}
	}
	return r1RenderRows(cells)
}

func TestArcBRWherePostgresRefusesEveryArmRefuses(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the binder-refusal table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := brArms(t, ctx)
	controls := 0
	for _, tc := range brArmCells() {
		t.Run(tc.name, func(t *testing.T) {
			first := ""
			for i, arm := range arms {
				got, err := arm.run(tc.sql)
				if tc.state != "" {
					if err == nil {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  every arm must refuse: PostgreSQL 17.11 raises %s %s",
							tc.sql, arm.name, got, tc.state, tc.msg)
						continue
					}
					if st := sqlerr.StateOf(err); st != tc.state || !strings.Contains(err.Error(), tc.msg) {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s %q (PostgreSQL 17.11), at plan time",
							tc.sql, arm.name, st, err, tc.state, tc.msg)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v\n  PostgreSQL 17.11 answers it", tc.sql, arm.name, err)
					continue
				}
				if tc.same {
					if i == 0 {
						first = got
					} else if got != first {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  single answered %s", tc.sql, arm.name, got, first)
					}
					continue
				}
				if got != tc.want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, got, tc.want)
				}
			}
		})
		if tc.state == "" {
			controls++
		}
	}
	if controls < 10 {
		t.Fatalf("%d controls: the table must say what still answers, not only what refuses", controls)
	}
}
