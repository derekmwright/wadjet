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
	// answers marks a control that must answer on every arm, values aside.
	answers bool
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

// brAggregateCells is the aggregate seam x the 22-type matrix on five arms
// (#1249, #1061): every aggregate the parser knows over every column type,
// refused where PostgreSQL 17.11 has no overload and answered — identically
// on every arm — where it has one (or where ADR-0012 §5 records the wider
// set: MIN/MAX over BOOL, UUID, MACADDR, BYTEA, MAP and VECTOR). The accepted
// sets are written out from the measurement, not derived from the rule.
func brAggregateCells() []brArmCell {
	flat := []string{"c_bool", "c_i32", "c_i64", "c_f32", "c_f64", "c_str", "c_bytes", "c_ts",
		"c_ipv4", "c_ipv6", "c_cidr", "c_mac", "c_port", "c_proto", "c_dur", "c_uuid", "c_date", "c_dec"}
	nested := []string{"c_arr", "c_row", "c_rownest", "c_map", "c_vec"}
	numeric := map[string]bool{"c_i32": true, "c_i64": true, "c_f32": true, "c_f64": true,
		"c_dec": true, "c_port": true, "c_proto": true, "c_dur": true}
	table := func(c string) string {
		for _, n := range nested {
			if n == c {
				return "typemx_nested"
			}
		}
		return "typemx"
	}
	accepts := func(agg, c string) bool {
		switch agg {
		case "sum", "avg", "stddev", "stddev_samp", "stddev_pop", "variance", "var_samp",
			"var_pop", "median", "mode", "corr", "covar_samp", "covar_pop":
			return numeric[c]
		case "bool_and", "bool_or", "every":
			return c == "c_bool"
		case "string_agg":
			return c == "c_str"
		case "min", "max":
			return c != "c_row" && c != "c_rownest"
		}
		return true
	}
	var out []brArmCell
	for _, agg := range []string{"sum", "avg", "min", "max", "count", "stddev", "stddev_samp",
		"stddev_pop", "variance", "var_samp", "var_pop", "bool_and", "bool_or", "every",
		"approx_distinct", "median", "mode", "string_agg", "corr", "covar_samp", "covar_pop"} {
		for _, c := range append(append([]string{}, flat...), nested...) {
			call := agg + "(" + c + ")"
			switch agg {
			case "string_agg":
				call = "string_agg(" + c + ", ',')"
			case "corr", "covar_samp", "covar_pop":
				call = agg + "(" + c + ", " + c + ")"
			}
			cell := brArmCell{name: "agg/" + agg + "/" + c,
				sql: "SELECT " + call + " AS v FROM " + table(c) + " WHERE id < 20"}
			switch {
			case accepts(agg, c):
				cell.same = true
			case agg == "string_agg" && c == "c_bytes":
				cell.state, cell.msg = "0A000", "string_agg over bytea is not supported"
			default:
				cell.state, cell.msg = "42883", "function "+agg+"("
			}
			// approx_distinct answers a different count on the DAG arms, and
			// AVG over REAL a different float, at base and at this tip — both
			// PostgreSQL-valid shapes, recorded as filing candidates in arc
			// BR's notes; this table asserts only that they ANSWER.
			if agg == "approx_distinct" || (agg == "avg" && c == "c_f32") {
				cell.same, cell.answers = false, true
			}
			out = append(out, cell)
		}
	}
	// The positions an aggregate's argument can be written in, and PostgreSQL's
	// own sentence for each spelling of `unknown`.
	for _, pos := range []struct{ name, sql string }{
		{"plain", "SELECT SUM(customer) AS v FROM lat_ord"},
		{"distinct", "SELECT SUM(DISTINCT customer) AS v FROM lat_ord"},
		{"grouped", "SELECT id, SUM(customer) AS v FROM lat_ord GROUP BY id"},
		{"window", "SELECT id, SUM(customer) OVER () AS v FROM lat_ord"},
		{"having", "SELECT id FROM lat_ord GROUP BY id HAVING SUM(customer) IS NULL"},
		{"orderBy", "SELECT id FROM lat_ord GROUP BY id ORDER BY SUM(customer)"},
		{"filter", "SELECT SUM(customer) FILTER (WHERE id > 1) AS v FROM lat_ord"},
		{"concat", "SELECT SUM(customer || 'x') AS v FROM lat_ord"},
		{"upper", "SELECT SUM(upper(customer)) AS v FROM lat_ord"},
		{"castText", "SELECT SUM(CAST(id AS TEXT)) AS v FROM lat_ord"},
		{"derived", "SELECT SUM(x.c) AS v FROM (SELECT customer AS c FROM lat_ord) x"},
		{"cte", "WITH w AS (SELECT customer AS c FROM lat_ord) SELECT SUM(c) AS v FROM w"},
		{"scalarSubquery", "SELECT (SELECT SUM(customer) FROM lat_ord) AS v"},
		{"caseBranch", "SELECT SUM(CASE WHEN id > 1 THEN customer END) AS v FROM lat_ord"},
		{"underCoalesce", "SELECT COALESCE(SUM(customer), 'none') AS v FROM lat_ord"},
		{"arithmetic", "SELECT SUM(total) + SUM(customer) AS v FROM lat_ord"},
	} {
		out = append(out, brArmCell{name: "aggPos/" + pos.name, sql: pos.sql,
			state: "42883", msg: "function sum(text) does not exist"})
	}
	out = append(out,
		brArmCell{name: "aggRow/fieldOfMin", sql: "SELECT (min(c_row)).b AS v FROM typemx_nested WHERE id < 10",
			state: "42883", msg: "function min(record) does not exist"},
		brArmCell{name: "aggRow/groupedMax", sql: "SELECT g, MAX(c_row) AS v FROM typemx_nested WHERE id < 10 GROUP BY g",
			state: "42883", msg: "function max(record) does not exist"},
		brArmCell{name: "aggRow/windowMin", sql: "SELECT id, MIN(c_row) OVER () AS v FROM typemx_nested WHERE id < 3",
			state: "42883", msg: "function min(record) does not exist"},
		brArmCell{name: "aggRow/minByOrdering", sql: "SELECT min_by(id, c_row) AS v FROM typemx_nested WHERE id < 10",
			state: "42883", msg: "function min_by(bigint, record) does not exist"},
		brArmCell{name: "aggRowOk/maxByValue", sql: "SELECT max_by(c_row, id) AS v FROM typemx_nested WHERE id < 10", same: true},
		brArmCell{name: "aggUnknown/sumQuoted", sql: "SELECT SUM('5') AS v",
			state: "42725", msg: "function sum(unknown) is not unique"},
		brArmCell{name: "aggUnknown/sumNull", sql: "SELECT SUM(NULL) AS v FROM lat_ord",
			state: "42725", msg: "function sum(unknown) is not unique"},
		brArmCell{name: "aggUnknown/avgQuoted", sql: "SELECT AVG('5') AS v",
			state: "42725", msg: "function avg(unknown) is not unique"},
		brArmCell{name: "aggUnknown/boolAndNotABoolean", sql: "SELECT bool_and('5') AS v FROM lat_ord",
			state: "22P02", msg: `invalid input syntax for type boolean: "5"`},
		brArmCell{name: "aggUnknown/stddevNotANumber", sql: "SELECT stddev('t') AS v FROM lat_ord",
			state: "22P02", msg: `invalid input syntax for type double precision: "t"`},
		brArmCell{name: "aggUnknown/medianQuoted", sql: "SELECT median('5') AS v FROM lat_ord",
			state: "42883", msg: "function median(unknown) does not exist"},
		brArmCell{name: "aggLit/sumBoolean", sql: "SELECT SUM(true) AS v FROM lat_ord",
			state: "42883", msg: "function sum(boolean) does not exist"},
		brArmCell{name: "aggLit/boolAndInteger", sql: "SELECT bool_and(1) AS v FROM lat_ord",
			state: "42883", msg: "function bool_and(integer) does not exist"},
		brArmCell{name: "aggLit/stringAggInteger", sql: "SELECT string_agg(1, ',') AS v FROM lat_ord",
			state: "42883", msg: "function string_agg(integer, unknown) does not exist"},
		brArmCell{name: "aggOk/sumOne", sql: "SELECT SUM(1) AS v FROM lat_ord", want: "rows=1 3"},
		brArmCell{name: "aggOk/sumTotal", sql: "SELECT SUM(total) AS v FROM lat_ord", want: "rows=1 350"},
		brArmCell{name: "aggOk/sumCastText", sql: "SELECT SUM(CAST(CAST(id AS TEXT) AS BIGINT)) AS v FROM lat_ord", want: "rows=1 6"},
		brArmCell{name: "aggOk/minQuoted", sql: "SELECT min('5') AS v FROM lat_ord", want: "rows=1 5"},
		brArmCell{name: "aggOk/stringAggText", sql: "SELECT string_agg(DISTINCT customer, ',') AS v FROM lat_ord",
			want: "rows=1 Alice,Bob,Carol"},
		brArmCell{name: "aggOk/boolAndPredicate", sql: "SELECT bool_and(total > 0) AS v FROM lat_ord", want: "rows=1 false"},
	)
	return out
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
	for _, tc := range append(brArmCells(), brAggregateCells()...) {
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
				if tc.answers {
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
