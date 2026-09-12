package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A SUBQUERY READS THE ROW IT IS CORRELATED ON — #1044, on FIVE arms with the
// routing counters.
//
// `(SELECT u.x)` has NO FROM clause. PostgreSQL evaluates its SELECT
// expression in the ENCLOSING scope and answers `u.x`; this engine ran the
// block as a statement, where `u` names no relation, and every row read the
// EMPTY BOX under a text declaration — 18 and 1026 became "" and "" under OID
// 701 in the filing's own shape.
//
// Every want is live PostgreSQL 17.11 over the three rows c2uData writes
// (this arc's ROUND0, measured before any code changed).
//
// The DAG arms assert the routing counters beside the rows, because rows alone
// cannot tell "the DAG executed this" from "the DAG refused the plan and the
// coordinator-local pipeline answered".

// c2Deadline bounds ONE query on ONE arm. A correlated subquery is re-run once
// per outer row, so a classification change can turn a cell from fast into
// unbounded; three minutes over three rows is a decisive verdict either way.
const c2Deadline = 3 * time.Minute

const c2uTable = "c2users"

func c2uSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "name", Type: parquet.TypeString},
		{Name: "visits", Type: parquet.TypeInt64},
	}}
}

// c2uData is the three rows both issues were filed over, and the same three
// rows the PostgreSQL 17.11 oracle answered. The values are chosen so a wrong
// answer cannot look like a right one: `visits` is not `id`, not a multiple of
// it, and not sorted the same way, so a subquery that reads the first row
// instead of this one, or the inner relation instead of the outer, differs in
// every cell rather than in some of them.
func c2uData() []map[string]any {
	return []map[string]any{
		{"id": int32(1), "name": "alice", "visits": int64(100)},
		{"id": int32(2), "name": "bob", "visits": int64(42)},
		{"id": int32(3), "name": "carol", "visits": int64(200)},
	}
}

type c2Arm struct {
	name  string
	run   func(ctx context.Context, sql string) ([]string, [][]any, error)
	coord *Coordinator
}

// c2Arms is the five execution arms: the single-process pipeline, the same
// under a memory budget, and the stage DAG in its three dispatch shapes
// (default, every build through an exchange, morsel-parallel fragments).
func c2Arms(t *testing.T, ctx context.Context) []c2Arm {
	t.Helper()
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	dag := tmdCoordinator(t, ctx, infra)
	shuf := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	morsel := tmdCoordinatorWithWorkers(t, ctx, infraM,
		func(w *worker.Config) { w.MorselWorkers = 4 })
	single := tmdStandalone(t, ctx)
	spilled := e3BudgetedStandalone(t, ctx)
	runSingle := func(db *wadjet.DB) func(context.Context, string) ([]string, [][]any, error) {
		return func(c context.Context, sql string) ([]string, [][]any, error) {
			return e3PosSingle(c, db, sql)
		}
	}
	runDAG := func(co *Coordinator) func(context.Context, string) ([]string, [][]any, error) {
		return func(c context.Context, sql string) ([]string, [][]any, error) {
			return e3PosDAG(c, co, sql)
		}
	}
	return []c2Arm{
		{"single", runSingle(single), nil},
		{spilledArm, runSingle(spilled), nil},
		{"dag", runDAG(dag), dag},
		{"dag-shuffled", runDAG(shuf), shuf},
		{"dag-morsel4", runDAG(morsel), morsel},
	}
}

type c2Cell struct {
	name string
	sql  string
	// want is what PostgreSQL 17.11 answers over c2uData's three rows,
	// rendered by e3Render: "cols | r0c0,r0c1 | …".
	want string
	// wantErr, when set, names a substring of the sentence every arm must
	// raise. PostgreSQL's own answer for the cell is written beside it in the
	// comment: a REFUSAL where PostgreSQL answers is a divergence this engine
	// declares (ADR-0012), and it is still an assertion — the day the shape is
	// executable this cell fails and is rewritten to a value.
	wantErr string
	// routes is the routing delta each DAG arm must produce for this one
	// query. The zero value says the DAG EXECUTED the shape as stages.
	routes a2Routes
	// pin, when set, says this cell DIVERGES from PostgreSQL on EVERY arm and
	// records what this engine answers instead. A pinned cell that starts
	// agreeing FAILS, which is how the pin gets deleted.
	pin    string
	pinWhy string
	// pinErr is pin for a cell whose divergence is a REFUSAL rather than a
	// value: the substring the refusal must contain.
	pinErr string
}

func c2Cells() []c2Cell {
	return []c2Cell{
		// --- #1044: a FROM-less scalar subquery is its SELECT expression ---
		//
		// The filing shape with the subquery SPELLED OUT, which is what the
		// rewrite turns it into. It carries no subquery at all, so what it
		// answers is the DAG's verdict on the SHAPE — two nested derived
		// tables under a CROSS JOIN under an aggregate — rather than on the
		// correlation, and it is the control cell 01's DAG disposition is
		// read against.
		{name: "00_ctl_the_filing_shape_with_the_subquery_spelled_out",
			sql: `SELECT SUM(a.v) AS a, SUM(b.v) AS b ` +
				`FROM (SELECT u.x AS v FROM (SELECT id AS x FROM c2users) u) a ` +
				`CROSS JOIN (SELECT u.x AS v FROM (SELECT visits AS x FROM c2users) u) b`,
			want: `a,b | 18,1026`},
		//
		// The filing's own shape, both spellings. PostgreSQL: 18 and 1026,
		// declared bigint and numeric; this engine answered "" and "" under
		// OID 701 before the fix.
		{name: "01_the_filing_shape_derived_twins",
			sql: `SELECT SUM(a.v) AS a, SUM(b.v) AS b ` +
				`FROM (SELECT (SELECT u.x) AS v FROM (SELECT id AS x FROM c2users) u) a ` +
				`CROSS JOIN (SELECT (SELECT u.x) AS v FROM (SELECT visits AS x FROM c2users) u) b`,
			want: `a,b | 18,1026`},
		{name: "02_the_filing_shape_cte_twins",
			sql: `WITH a AS (SELECT id AS x FROM c2users), b AS (SELECT visits AS x FROM c2users) ` +
				`SELECT SUM(p.v) AS a, SUM(q.v) AS b ` +
				`FROM (SELECT (SELECT u.x) AS v FROM a u) p ` +
				`CROSS JOIN (SELECT (SELECT u.x) AS v FROM b u) q`,
			want: `a,b | 18,1026`},
		{name: "03_the_derived_tables_own_output_alias",
			sql:  `SELECT (SELECT u.x) AS v FROM (SELECT id AS x FROM c2users) u ORDER BY 1`,
			want: `v | 1 | 2 | 3`},
		{name: "04_the_ctes_own_output_alias",
			sql: `WITH a AS (SELECT id AS x FROM c2users) ` +
				`SELECT (SELECT u.x) AS v FROM a u ORDER BY 1`,
			want: `v | 1 | 2 | 3`},
		{name: "05_the_same_name_unqualified",
			sql:  `SELECT (SELECT x) AS v FROM (SELECT id AS x FROM c2users) u ORDER BY 1`,
			want: `v | 1 | 2 | 3`},
		{name: "06_an_expression_over_the_alias",
			sql:  `SELECT (SELECT u.x + 1) AS v FROM (SELECT id AS x FROM c2users) u ORDER BY 1`,
			want: `v | 2 | 3 | 4`},
		{name: "07_aggregated",
			sql:  `SELECT SUM((SELECT u.x)) AS v FROM (SELECT id AS x FROM c2users) u`,
			want: `v | 6`},
		{name: "08_in_the_where_clause",
			sql:  `SELECT u.x FROM (SELECT id AS x FROM c2users) u WHERE (SELECT u.x) > 1 ORDER BY 1`,
			want: `x | 2 | 3`},
		{name: "09_in_the_having_clause",
			sql: `SELECT u.x, COUNT(*) AS n FROM (SELECT id AS x FROM c2users) u ` +
				`GROUP BY u.x HAVING (SELECT u.x) > 1 ORDER BY 1`,
			want: `x,n | 2,1 | 3,1`},
		{name: "10_in_the_order_by_clause",
			sql:  `SELECT u.x FROM (SELECT id AS x FROM c2users) u ORDER BY (SELECT u.x) DESC`,
			want: `x | 3 | 2 | 1`},
		{name: "11_a_computed_alias",
			sql:  `SELECT (SELECT u.x) AS v FROM (SELECT id*10 AS x FROM c2users) u ORDER BY 1`,
			want: `v | 10 | 20 | 30`},
		{name: "12_grouped",
			sql: `SELECT u.x, (SELECT u.x) AS v FROM (SELECT id AS x FROM c2users) u ` +
				`GROUP BY u.x ORDER BY 1`,
			want: `x,v | 1,1 | 2,2 | 3,3`},
		// The mechanism is the MISSING FROM CLAUSE and not the derived table:
		// a base table's own alias answered the empty string too.
		{name: "13_a_base_tables_own_alias",
			sql:  `SELECT (SELECT u.id) AS v FROM c2users u ORDER BY 1`,
			want: `v | 1 | 2 | 3`},
		{name: "14_a_bigint_column",
			sql:  `SELECT (SELECT u.x) AS v FROM (SELECT visits AS x FROM c2users) u ORDER BY 1`,
			want: `v | 42 | 100 | 200`},
		{name: "15_a_string_column",
			sql:  `SELECT (SELECT u.nm) AS v FROM (SELECT name AS nm FROM c2users) u ORDER BY 1`,
			want: `v | alice | bob | carol`},
		{name: "16_nested_two_deep",
			sql:  `SELECT (SELECT (SELECT u.x)) AS v FROM (SELECT id AS x FROM c2users) u ORDER BY 1`,
			want: `v | 1 | 2 | 3`},
		{name: "17_over_a_set_operation_body",
			sql: `SELECT (SELECT u.x) AS v FROM ` +
				`(SELECT id AS x FROM c2users UNION ALL SELECT id AS x FROM c2users) u ORDER BY 1`,
			want: `v | 1 | 1 | 2 | 2 | 3 | 3`},
		{name: "18_over_a_column_alias_list",
			sql:  `SELECT (SELECT u.kk) AS v FROM (SELECT id FROM c2users) u(kk) ORDER BY 1`,
			want: `v | 1 | 2 | 3`},
		{name: "19_two_scopes_in_one_expression",
			sql: `SELECT (SELECT u.x + v.y) AS z FROM (SELECT id AS x FROM c2users) u ` +
				`CROSS JOIN (SELECT visits AS y FROM c2users) v ORDER BY 1`,
			want: `z | 43 | 44 | 45 | 101 | 102 | 103 | 201 | 202 | 203`},
		{name: "20_arithmetic_over_two_outer_columns",
			sql:  `SELECT id, (SELECT u.id * 2 + u.visits) AS v FROM c2users u ORDER BY id`,
			want: `id,v | 1,102 | 2,46 | 3,206`},
		{name: "21_a_case_over_the_outer_row",
			sql: `SELECT id, (SELECT CASE WHEN u.id > 1 THEN 'big' ELSE 'small' END) AS v ` +
				`FROM c2users u ORDER BY id`,
			want: `id,v | 1,small | 2,big | 3,big`},
		// --- #1044's boundary: what is NOT its SELECT expression -----------
		//
		// A FROM-less SELECT still has the clauses that can make it produce no
		// row, and each of them is PostgreSQL's NULL rather than the value.
		{name: "22_boundary_a_false_where_is_null",
			sql:    `SELECT id, (SELECT u.id WHERE 1=0) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,NULL | 2,NULL | 3,NULL`,
			routes: a2Routes{Correlated: 1}},
		{name: "23_boundary_limit_zero_is_null",
			sql:    `SELECT id, (SELECT u.id LIMIT 0) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,NULL | 2,NULL | 3,NULL`,
			routes: a2Routes{Correlated: 1}},
		{name: "24_boundary_offset_one_is_null",
			sql:    `SELECT id, (SELECT u.id OFFSET 1) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,NULL | 2,NULL | 3,NULL`,
			routes: a2Routes{Correlated: 1}},
		{name: "25_boundary_two_columns_is_refused",
			sql:     `SELECT id, (SELECT u.id, u.visits) AS v FROM c2users u ORDER BY id`,
			wantErr: `only one column`,
			routes:  a2Routes{Correlated: 1}},
		{name: "26_boundary_an_unknown_name_is_refused",
			sql:     `SELECT (SELECT u.nosuch) AS v FROM (SELECT id AS x FROM c2users) u ORDER BY 1`,
			wantErr: `nosuch`},
		// --- #1044's controls: shapes that were already right --------------
		{name: "27_ctl_a_constant",
			sql:  `SELECT id, (SELECT 1) AS v FROM c2users u ORDER BY id`,
			want: `id,v | 1,1 | 2,1 | 3,1`},
		{name: "28_ctl_a_subquery_with_its_own_from",
			sql: `SELECT (SELECT MAX(y.visits) FROM c2users y WHERE y.id = u.x) AS v ` +
				`FROM (SELECT id AS x FROM c2users) u ORDER BY 1`,
			want:   `v | 42 | 100 | 200`,
			routes: a2Routes{Correlated: 1}},
		{name: "29_ctl_the_where_clause_substitution",
			sql: `SELECT id, (SELECT x.visits FROM c2users x WHERE x.id = u.id) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,100 | 2,42 | 3,200`,
			routes: a2Routes{Correlated: 1}},
	}
}

func TestArcC2ASubqueryReadsTheRowItIsCorrelatedOn(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := c2Arms(t, ctx)

	for _, tc := range c2Cells() {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				var before a2Routes
				if arm.coord != nil {
					before = a2ReadRoutes(arm.coord)
				}
				got, err := c2Run(ctx, arm, tc.sql)
				if arm.coord != nil {
					a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.coord), tc.routes, tc.sql)
				}
				switch {
				case tc.pinErr != "":
					switch {
					case err != nil && strings.Contains(err.Error(), tc.pinErr):
						// The recorded refusal, unchanged.
					case err == nil && got == tc.want:
						t.Errorf("%s arm now AGREES with PostgreSQL (%s), so this pin is FIXED: "+
							"delete it from c2Cells.\n  pinned reason: %s\n  SQL: %s",
							arm.name, tc.want, tc.pinWhy, tc.sql)
					default:
						t.Errorf("%s arm answers %s%s, which is neither PostgreSQL's %s nor the "+
							"pinned refusal %q\n  SQL: %s",
							arm.name, got, c2ErrText(err), tc.want, tc.pinErr, tc.sql)
					}
				case tc.pin != "":
					switch {
					case err != nil:
						t.Errorf("%s arm: %v\n  SQL: %s\n  the pinned answer is %s",
							arm.name, err, tc.sql, tc.pin)
					case got == tc.want:
						t.Errorf("%s arm now AGREES with PostgreSQL (%s), so this pin is FIXED: "+
							"delete it from c2Cells.\n  pinned reason: %s\n  SQL: %s",
							arm.name, tc.want, tc.pinWhy, tc.sql)
					case got != tc.pin:
						t.Errorf("%s arm answers %s, which is neither PostgreSQL's %s nor the "+
							"pinned %s\n  SQL: %s", arm.name, got, tc.want, tc.pin, tc.sql)
					}
				case tc.wantErr != "":
					switch {
					case err == nil:
						t.Errorf("%s arm ANSWERED %s where this engine must refuse with a "+
							"sentence containing %q\n  SQL: %s", arm.name, got, tc.wantErr, tc.sql)
					case !strings.Contains(err.Error(), tc.wantErr):
						t.Errorf("%s arm raises a different refusal\n  got  %v\n  want a sentence "+
							"containing %q\n  SQL: %s", arm.name, err, tc.wantErr, tc.sql)
					}
				case err != nil:
					t.Errorf("%s arm: %v\n  SQL: %s\n  PostgreSQL 17.11 answers %s",
						arm.name, err, tc.sql, tc.want)
				case got != tc.want:
					t.Errorf("%s arm\n  got  %s\n  want %s (PostgreSQL 17.11)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}

// c2Run runs one query on one arm under c2Deadline and renders its result. A
// deadline that expires is an assertion and not an infrastructure hiccup: a
// subquery that starts being classified correlated is re-run once per outer
// row, and rows that never arrive are the shape of that.
func c2Run(ctx context.Context, arm c2Arm, sql string) (string, error) {
	qctx, cancel := context.WithTimeout(ctx, c2Deadline)
	defer cancel()
	type result struct {
		cols []string
		rows [][]any
		err  error
	}
	done := make(chan result, 1)
	go func() {
		cols, rows, err := arm.run(qctx, sql)
		done <- result{cols, rows, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return "", r.err
		}
		return e3Render(r.cols, r.rows), nil
	case <-qctx.Done():
		return "", context.DeadlineExceeded
	}
}

func c2ErrText(err error) string {
	if err == nil {
		return ""
	}
	return " (error: " + err.Error() + ")"
}
