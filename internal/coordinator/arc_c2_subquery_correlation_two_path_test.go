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

// A SUBQUERY READS THE ROW IT IS CORRELATED ON — #1044 and #1045, on FIVE arms
// with the routing counters.
//
// Two shapes, one question: where does the outer row's value enter a subquery?
//
//   - #1044. `(SELECT u.x)` has NO FROM clause. PostgreSQL evaluates its
//     SELECT expression in the ENCLOSING scope and answers `u.x`; this engine
//     ran the block as a statement, where `u` names no relation, and every row
//     read the EMPTY BOX under a text declaration — 18 and 1026 became "" and
//     "" under OID 701 in the filing's own shape.
//   - #1045. A correlated re-run substitutes the outer row's values into the
//     subquery's WHERE clause and REBUILDS the statement around it, re-emitting
//     every other clause as the text the parser recorded. A window call's
//     recorded text collapses its OVER clause, so a correlated subquery holding
//     one cannot be rebuilt at all — and the walk that finds correlated
//     references had no case for a window node, so
//     `(SELECT 1+SUM(u.id) OVER () FROM users x WHERE x.id=1)` was planned
//     UNCORRELATED, ran once, and the qualifier strip rebound `u.id` to the
//     inner relation's own `id`: 2, 2, 2 for PostgreSQL's 2, 3, 4.
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

// c2t2 is a SECOND relation whose columns do not overlap c2users'. It exists
// for ONE claim: an UNQUALIFIED name inside a subquery binds the inner
// relation when the inner supplies it, and the ENCLOSING row when it does not
// (ADR-0021 §1k). A bare outer reference is only reachable when the inner
// relation lacks the column, which a self-join of c2users can never arrange.
const c2t2Table = "c2t2"

func c2t2Schema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt32},
		{Name: "w", Type: parquet.TypeInt64},
	}}
}

func c2t2Data() []map[string]any {
	return []map[string]any{
		{"k": int32(1), "w": int64(10)},
		{"k": int32(2), "w": int64(20)},
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
	pin string
	// pinArms is pin per ARM, for a shape the arms answer DIFFERENTLY. An arm
	// absent from it is held to pin (or to want when there is none).
	pinArms map[string]string
	pinWhy  string
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
		// --- #1045: an outer reference inside a WINDOW function ------------
		//
		// Every cell below is a shape PostgreSQL 17.11 ANSWERS and this engine
		// REFUSES: the per-row re-run substitutes the outer value into the
		// subquery's WHERE and rebuilds the statement around it, and a window
		// call has no faithful rendering to rebuild (WindowFuncNode.String
		// collapses the OVER clause on purpose — see plansql.ReplaceWindowFuncs).
		// The refusal is 0A000, the sentence the three `window_*_correlated`
		// siblings already carry. The values PostgreSQL answers are written
		// beside each cell so the day the shape is executable this gate says
		// what to assert instead.
		{name: "30_the_filing_shape_window_over_the_outer_row", // PG: 2, 3, 4
			sql: `SELECT id,(SELECT 1+SUM(u.id) OVER () FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `correlated on u.id`,
			routes:  a2Routes{Correlated: 1}},
		{name: "31_the_window_call_alone", // PG: 1, 2, 3
			sql: `SELECT id,(SELECT SUM(u.id) OVER () FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `correlated on u.id`,
			routes:  a2Routes{Correlated: 1}},
		{name: "32_a_bigint_outer_column", // PG: 101, 43, 201
			sql: `SELECT id,(SELECT 1+SUM(u.visits) OVER () FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `correlated on u.visits`,
			routes:  a2Routes{Correlated: 1}},
		{name: "33_two_outer_columns_in_one_argument", // PG: 101, 44, 203
			sql: `SELECT id,(SELECT SUM(u.id + u.visits) OVER () FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `correlated on u.`,
			routes:  a2Routes{Correlated: 1}},
		{name: "34_in_a_where_clause", // PG: 2, 3
			sql: `SELECT id FROM c2users u ` +
				`WHERE (SELECT 1+SUM(u.id) OVER () FROM c2users x WHERE x.id=1) > 2 ORDER BY id`,
			wantErr: `correlated on u.id`,
			routes:  a2Routes{Correlated: 1}},
		{name: "35_over_a_cte", // PG: 2, 3, 4
			sql: `WITH a AS (SELECT id AS x FROM c2users) ` +
				`SELECT x,(SELECT 1+SUM(u.x) OVER () FROM c2users y WHERE y.id=1) AS v ` +
				`FROM a u ORDER BY x`,
			wantErr: `correlated on u.x`,
			routes:  a2Routes{Correlated: 1}},
		{name: "36_partition_by_the_outer_row", // PG: 1, 1, 1
			sql: `SELECT id,(SELECT SUM(x.id) OVER (PARTITION BY u.id) FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `correlated on u.id`,
			routes:  a2Routes{Correlated: 1}},
		{name: "37_order_by_the_outer_row", // PG: 1, 1, 1
			sql: `SELECT id,(SELECT ROW_NUMBER() OVER (ORDER BY u.id) FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `correlated on u.id`,
			routes:  a2Routes{Correlated: 1}},
		{name: "38_count_star_partitioned_by_the_outer_row", // PG: 1, 1, 1
			sql: `SELECT id,(SELECT COUNT(*) OVER (PARTITION BY u.id) FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `correlated on u.id`,
			routes:  a2Routes{Correlated: 1}},
		// THE DISCRIMINATORS. Cells 36-38 are shapes this engine answered
		// CORRECTLY before the refusal — and only because the inner relation
		// has ONE row, which makes any partitioning and any ordering of it the
		// same partition in the same order. Two inner rows and the same shapes
		// are silently wrong: PostgreSQL partitions by a constant and sums
		// BOTH rows, this engine strips the qualifier, partitions by the
		// inner `x.id`, and sums one. The refusal is therefore not a right
		// answer traded for a loud one; it is the same defect, seen.
		{name: "39_discriminator_partition_over_two_inner_rows", // PG: 3, 3, 3
			sql: `SELECT id,(SELECT SUM(x.id) OVER (PARTITION BY u.id) FROM c2users x ` +
				`WHERE x.id<3 LIMIT 1) AS v FROM c2users u ORDER BY id`,
			wantErr: `correlated on u.id`,
			routes:  a2Routes{Correlated: 1}},
		{name: "40_discriminator_count_over_two_inner_rows", // PG: 2, 2, 2
			sql: `SELECT id,(SELECT COUNT(*) OVER (PARTITION BY u.id) FROM c2users x ` +
				`WHERE x.id<3 LIMIT 1) AS v FROM c2users u ORDER BY id`,
			wantErr: `correlated on u.id`,
			routes:  a2Routes{Correlated: 1}},
		// --- #1045's controls: a window that reads NO outer row ------------
		{name: "41_ctl_an_uncorrelated_window_in_a_subquery",
			sql: `SELECT id,(SELECT 1+SUM(x.id) OVER () FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,2 | 2,2 | 3,2`,
			routes: a2Routes{ScalarProjection: 1}},
		{name: "42_ctl_an_unqualified_name_the_inner_relation_supplies",
			sql: `SELECT id,(SELECT 1+SUM(visits) OVER () FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,101 | 2,101 | 3,101`,
			routes: a2Routes{ScalarProjection: 1}},
		{name: "43_ctl_partition_by_the_inner_relation",
			sql: `SELECT id,(SELECT SUM(x.id) OVER (PARTITION BY x.id) FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,1 | 2,1 | 3,1`,
			routes: a2Routes{ScalarProjection: 1}},
		{name: "44_ctl_a_window_over_the_query_itself",
			sql:  `SELECT id, SUM(id) OVER () AS v FROM c2users u ORDER BY id`,
			want: `id,v | 1,6 | 2,6 | 3,6`},
		// --- THE REWRITE'S REACH: every position a FROM-less scalar subquery
		// can occupy relative to the scope that owns its references
		// (round-2 review, B1). The rewrite fires where the ENCLOSING BLOCK
		// supplies the row; where the enclosing block is a subquery whose own
		// text a per-row re-run rebuilds, the node stays and the outer value
		// arrives through that rebuild instead — or the shape is refused.
		{name: "45_nested_in_a_correlated_subquerys_select_list",
			sql: `SELECT id, (SELECT (SELECT u.id) FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,1 | 2,2 | 3,3`,
			routes: a2Routes{Correlated: 1}},
		{name: "46_the_same_in_an_IN_set",
			sql: `SELECT id FROM c2users u ` +
				`WHERE u.id IN (SELECT (SELECT u.id) FROM c2users x WHERE x.id=1) ORDER BY id`,
			want:   `id | 1 | 2 | 3`,
			routes: a2Routes{Correlated: 1}},
		{name: "47_the_same_in_a_HAVING",
			sql: `SELECT id, (SELECT SUM(x.visits) FROM c2users x ` +
				`HAVING SUM(x.visits) > (SELECT u.id)) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,342 | 2,342 | 3,342`,
			routes: a2Routes{Correlated: 1}},
		{name: "48_the_same_over_a_CTE",
			sql: `WITH c AS (SELECT id FROM c2users) ` +
				`SELECT id, (SELECT (SELECT u.id) FROM c x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,1 | 2,2 | 3,3`,
			routes: a2Routes{Correlated: 1}},
		{name: "49_nested_three_deep",
			sql: `SELECT id, (SELECT (SELECT (SELECT u.id)) FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,1 | 2,2 | 3,3`,
			routes: a2Routes{Correlated: 1}},
		{name: "50_under_arithmetic_beside_an_inner_column",
			sql: `SELECT id, (SELECT (SELECT u.id) + x.visits FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,101 | 2,102 | 3,103`,
			routes: a2Routes{Correlated: 1}},
		{name: "51_in_the_enclosing_subquerys_own_WHERE",
			sql: `SELECT id, (SELECT x.visits FROM c2users x WHERE x.id = (SELECT u.id)) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,100 | 2,42 | 3,200`,
			routes: a2Routes{Correlated: 1}},
		// The two positions whose substitution is CONDITIONAL. The rebuild
		// writes an ORDER BY or GROUP BY term like any other, and only a term
		// whose SUBSTITUTED rendering is a bare numeric literal is refused,
		// because that is the one rendering both engines read as a
		// select-list POSITION rather than as a value (round-3 review, P4).
		// A constant sort under a LIMIT picks an ARBITRARY row (ADR-0013), so
		// the cell carries a real tiebreak; what it asserts is that the term
		// is written into the rebuild rather than refused.
		{name: "52_an_ORDER_BY_term_the_rebuild_can_write",
			sql: `SELECT id, (SELECT x.visits FROM c2users x ORDER BY (SELECT u.id), x.id LIMIT 1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,100 | 2,100 | 3,100`,
			routes: a2Routes{Correlated: 1}},
		// The DIRECT spelling of the same two positions — an outer reference
		// written straight into the clause rather than through a nested
		// subquery — with the discriminator that says why refusing it is not
		// a right answer traded for a loud one. `ORDER BY x.id * u.id`
		// answered PostgreSQL's rows because multiplying by a POSITIVE
		// constant does not change an order; with a factor that is negative
		// for the first outer row the same mechanism answers 100 where
		// PostgreSQL answers 200.
		{name: "52a_an_outer_reference_in_an_ORDER_BY_beside_one_in_the_WHERE",
			// For the middle row the factor is zero, so the sort key is
			// constant and LIMIT 1 would pick an arbitrary row (ADR-0013);
			// the tiebreak makes the cell decide the substitution rather than
			// the arm's row order.
			sql: `SELECT id, (SELECT x.visits FROM c2users x WHERE x.id <= u.id ` +
				`ORDER BY x.id * (u.id - 2), x.id LIMIT 1) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,100 | 2,100 | 3,100`,
			routes: a2Routes{Correlated: 1}},
		{name: "52b_the_same_in_a_GROUP_BY",
			sql: `SELECT id, (SELECT SUM(x.visits) FROM c2users x WHERE x.id <= u.id ` +
				`GROUP BY u.id) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,100 | 2,142 | 3,342`,
			routes: a2Routes{Correlated: 1}},
		{name: "53_a_GROUP_BY_term_that_substitutes_to_a_constant",
			sql: `SELECT id, (SELECT SUM(x.visits) FROM c2users x GROUP BY u.id) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,342 | 2,342 | 3,342`,
			routes: a2Routes{Correlated: 1}},
		{name: "54_in_a_LATERAL_body_is_refused", // PostgreSQL: 1, 2, 3
			sql: `SELECT u.id, l.v FROM c2users u CROSS JOIN LATERAL ` +
				`(SELECT (SELECT u.id) AS v FROM c2users x WHERE x.id=1) l ORDER BY 1`,
			wantErr: `correlated on u.id`,
			routes:  a2Routes{ScalarProjection: 1}},

		// --- #1044's own shape WITH a FROM clause (round-2 review, P1).
		// The re-run substitutes the outer row into the SELECT list and the
		// HAVING now, not only the WHERE.
		{name: "55_the_issue_shape_with_a_from_clause",
			sql: `SELECT (SELECT u.x FROM c2users y WHERE y.id = 1) AS v ` +
				`FROM (SELECT id AS x FROM c2users) u ORDER BY 1`,
			want:   `v | 1 | 2 | 3`,
			routes: a2Routes{Correlated: 1}},
		{name: "56_the_same_over_a_CTE",
			sql: `WITH a AS (SELECT id AS x FROM c2users) ` +
				`SELECT (SELECT u.x FROM c2users y WHERE y.id = 1) AS v FROM a u ORDER BY 1`,
			want:   `v | 1 | 2 | 3`,
			routes: a2Routes{Correlated: 1}},
		{name: "57_the_same_on_a_base_tables_alias",
			sql: `SELECT id, (SELECT u.id FROM c2users x WHERE x.id=1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,1 | 2,2 | 3,3`,
			routes: a2Routes{Correlated: 1}},
		{name: "58_an_outer_reference_in_a_HAVING",
			sql: `SELECT id, (SELECT SUM(x.visits) FROM c2users x ` +
				`HAVING SUM(x.visits) > u.id) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,342 | 2,342 | 3,342`,
			routes: a2Routes{Correlated: 1}},
		{name: "59_the_aggregate_ARGUMENT_spelling_is_the_residual",
			sql: `SELECT SUM((SELECT u.x FROM c2users y WHERE y.id = 1)) AS v ` +
				`FROM (SELECT id AS x FROM c2users) u`,
			want: `v | 6`,
			pin:  `v | NULL`,
			pinWhy: "the AGGREGATE-ARGUMENT compile site resolves its outer scope from the " +
				"scan aliases below it (physical.collectTableAliases), which do not carry a " +
				"DERIVED TABLE's alias, so this subquery is planned UNCORRELATED and the " +
				"qualifier strip finds no `x` — ADR-0021 §1c's named gap, unchanged by this " +
				"arc and identical at bf99c56c. The same subquery one position out, in the " +
				"SELECT list, is cell 55",
			routes: a2Routes{Correlated: 1}},

		// --- an aggregate belongs to the level of the deepest variable in
		// its arguments, and this engine does not implement levels.
		{name: "60_an_aggregate_over_only_outer_refs_is_refused", // PostgreSQL: 42803
			sql:     `SELECT id, (SELECT MAX(u.id) FROM c2users x) AS v FROM c2users u ORDER BY id`,
			wantErr: `names only the enclosing query`,
			routes:  a2Routes{Correlated: 1}},
		{name: "61_the_same_through_a_nested_subquery", // PostgreSQL: 42803
			sql: `SELECT id, (SELECT MAX((SELECT u.id)) FROM c2users x) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `names only the enclosing query`,
			routes:  a2Routes{Correlated: 1}},
		{name: "62_ctl_an_aggregate_that_also_names_an_inner_column",
			sql: `SELECT id, (SELECT SUM(x.visits + u.id) FROM c2users x) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,345 | 2,348 | 3,351`,
			routes: a2Routes{Correlated: 1}},
		{name: "63_ctl_an_aggregate_over_no_column_at_all",
			sql:  `SELECT id, (SELECT MAX(1) FROM c2users x) AS v FROM c2users u ORDER BY id`,
			want: `id,v | 1,1 | 2,1 | 3,1`},
		{name: "64_ctl_the_enclosing_querys_own_aggregate",
			sql:  `SELECT MAX((SELECT u.id)) AS v FROM c2users u`,
			want: `v | 3`},

		// --- the clauses the rewrite DECLINES now answer through the re-run
		// instead of a silent NULL (round-2 review, N1), and a star with no
		// relation is PostgreSQL's 42601 (N3).
		{name: "65_a_declined_ORDER_BY_answers_through_the_rerun",
			sql:    `SELECT id, (SELECT u.id ORDER BY 1) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,1 | 2,2 | 3,3`,
			routes: a2Routes{Correlated: 1}},
		{name: "66_a_declined_LIMIT_one_answers_through_the_rerun",
			sql:    `SELECT id, (SELECT u.id LIMIT 1) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,1 | 2,2 | 3,3`,
			routes: a2Routes{Correlated: 1}},
		{name: "67_a_declined_WHERE_that_is_not_constant_false",
			sql:    `SELECT id, (SELECT u.id WHERE u.id > 1) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,NULL | 2,2 | 3,3`,
			routes: a2Routes{Correlated: 1}},
		{name: "68_a_star_with_no_relation_is_refused",
			sql:     `SELECT id, (SELECT *) AS v FROM c2users u ORDER BY id`,
			wantErr: `no tables specified`,
			routes:  a2Routes{ScalarProjection: 1}},
		{name: "69_a_fromless_subquery_as_an_IN_set",
			sql:  `SELECT id FROM c2users u WHERE u.id IN (SELECT u.id) ORDER BY id`,
			want: `id | 1 | 2 | 3`},
		{name: "70_a_LATERAL_body_that_projects_an_outer_column_is_the_residual",
			sql: `SELECT u.id, l.v FROM c2users u CROSS JOIN LATERAL ` +
				`(SELECT u.id AS v FROM c2users x WHERE x.id=1) l ORDER BY 1`,
			want: `id,v | 1,1 | 2,2 | 3,3`,
			pin:  `id,v | 1,1 | 2,1 | 3,1`,
			pinArms: map[string]string{
				// The three DAG arms also publish the item under the inner
				// expression's name rather than the lateral's alias — a
				// second, separate defect in the same shape (round-2 review,
				// N7), pinned here so this cell asserts what each arm does.
				"dag":          `id,x.id | 1,1 | 2,1 | 3,1`,
				"dag-shuffled": `id,x.id | 1,1 | 2,1 | 3,1`,
				"dag-morsel4":  `id,x.id | 1,1 | 2,1 | 3,1`,
			},
			pinWhy: "a LATERAL is decorrelated into a JOIN, and a body that PROJECTS an outer " +
				"column rather than joining on it has nothing for the lowering to respell — " +
				"J1's territory (ADR-0021 §1h), unchanged by this arc and identical at " +
				"bf99c56c"},
		// --- AN ORDER BY TERM IS NEVER REWRITTEN INTO AN ORDINAL (round-2
		// review, B1). PostgreSQL reads only an integer literal WRITTEN IN
		// THE CLAUSE as a select-list position, never one a subquery
		// evaluates to, so `ORDER BY (SELECT 1)` is a constant sort. The
		// rewrite turned it into the bare `1` — after resolvePositionalRefs,
		// with nothing left to resolve it — and the planner refused 42P10 on
		// ten shapes main answers exactly as PostgreSQL does.
		{name: "71_order_by_a_constant_subquery",
			sql:    `SELECT visits, id FROM c2users u ORDER BY (SELECT 1), id`,
			want:   `visits,id | 100,1 | 42,2 | 200,3`,
			routes: a2Routes{ScalarProjection: 1}},
		{name: "72_order_by_a_constant_subquery_naming_no_position",
			sql:    `SELECT visits, id FROM c2users u ORDER BY (SELECT 5), id`,
			want:   `visits,id | 100,1 | 42,2 | 200,3`,
			routes: a2Routes{ScalarProjection: 1}},
		{name: "73_the_same_descending",
			sql:    `SELECT visits, id FROM c2users u ORDER BY (SELECT 1) DESC, id`,
			want:   `visits,id | 100,1 | 42,2 | 200,3`,
			routes: a2Routes{ScalarProjection: 1}},
		{name: "74_the_same_with_nulls_first",
			sql:    `SELECT visits, id FROM c2users u ORDER BY (SELECT 1) NULLS FIRST, id`,
			want:   `visits,id | 100,1 | 42,2 | 200,3`,
			routes: a2Routes{ScalarProjection: 1}},
		{name: "75_beside_a_real_sort_key",
			sql:    `SELECT visits, id FROM c2users u ORDER BY id, (SELECT 1)`,
			want:   `visits,id | 100,1 | 42,2 | 200,3`,
			routes: a2Routes{ScalarProjection: 1}},
		{name: "76_over_an_aliased_item",
			sql:    `SELECT visits AS v FROM c2users u ORDER BY (SELECT 1), id`,
			want:   `v | 100 | 42 | 200`,
			routes: a2Routes{ScalarProjection: 1}},
		{name: "77_inside_a_subquerys_own_ORDER_BY",
			sql: `SELECT id, (SELECT x.id FROM c2users x ORDER BY (SELECT 1), x.id LIMIT 1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,1 | 2,1 | 3,1`,
			routes: a2Routes{ScalarProjection: 1}},
		// SELECT DISTINCT keeps PostgreSQL's OWN message, which the rewrite
		// had replaced with the ordinal one.
		{name: "78_select_distinct_keeps_postgres_message",
			sql:     `SELECT DISTINCT visits FROM c2users u ORDER BY (SELECT 1)`,
			wantErr: `for SELECT DISTINCT, ORDER BY expressions must appear in select list`},
		// The boundary: a term that does NOT render as a bare numeric literal
		// is rewritten as before, and answers.
		{name: "79_boundary_an_operator_expression_still_rewrites",
			sql:  `SELECT visits, id FROM c2users u ORDER BY (SELECT 1 + 1), id`,
			want: `visits,id | 100,1 | 42,2 | 200,3`},
		{name: "80_boundary_a_null_and_a_column_still_rewrite",
			sql:  `SELECT visits, id FROM c2users u ORDER BY (SELECT NULL), (SELECT u.id)`,
			want: `visits,id | 100,1 | 42,2 | 200,3`},
		// A CONSTANT SORT leaves the row order unspecified (ADR-0013), so
		// every cell above carries a real final key; this one asserts that
		// the term is still THERE and still a constant sort, by the only
		// thing that is deterministic about it — that it answers at all.
		{name: "80a_a_constant_sort_alone_answers_three_rows",
			sql:    `SELECT COUNT(*) AS n FROM (SELECT visits FROM c2users u ORDER BY (SELECT 1)) z`,
			want:   `n | 3`,
			routes: a2Routes{ScalarProjection: 1}},
		{name: "81_boundary_a_GROUP_BY_subquery_keeps_its_node",
			sql:  `SELECT visits, COUNT(*) AS n FROM c2users u GROUP BY visits, (SELECT 2) ORDER BY 1`,
			want: `visits,n | 42,1 | 100,1 | 200,1`,
			pinArms: map[string]string{
				// The three DAG arms cannot compile a subquery in a GROUP BY
				// term at all — the scan-agg fragment builds its group-by
				// projection without a SubqueryRunner. Keeping the node (which
				// is what B2 requires, and what main does) is what exposes it;
				// round 3 hid it by rewriting the term to an ordinal, which
				// silently grouped by a select-list position instead.
				"dag":          `subqueries require a SubqueryRunner`,
				"dag-shuffled": `subqueries require a SubqueryRunner`,
				"dag-morsel4":  `subqueries require a SubqueryRunner`,
			},
			pinWhy: "a subquery in a GROUP BY term has no lowering in the DAG's scan-agg " +
				"fragment; the single-process arms answer PostgreSQL's rows"},

		// --- AN OUTER REFERENCE WHOSE ONLY POSITION IS THE SUBQUERY'S ORDER
		// BY is seen by the classifier now, so it reaches the re-run and is
		// refused there instead of answering a constant (round-2 review, P1).
		// The `, x.id` tiebreak is load-bearing: for the middle outer row the
		// factor is zero, so without it the sort key is constant and LIMIT 1
		// picks an arbitrary row (ADR-0013). The cell decides the
		// substitution, not the arm's row order.
		{name: "82_an_outer_reference_only_in_the_subquerys_ORDER_BY",
			sql: `SELECT id, (SELECT x.visits FROM c2users x ORDER BY x.id * (u.id - 2), x.id LIMIT 1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,200 | 2,100 | 3,100`,
			routes: a2Routes{Correlated: 1}},
		{name: "83_the_same_projecting_the_sort_key",
			sql: `SELECT id, (SELECT x.id FROM c2users x ORDER BY x.id * (u.id - 2), x.id LIMIT 1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,3 | 2,1 | 3,1`,
			routes: a2Routes{Correlated: 1}},

		// --- A CORRELATED BODY THAT IS A SET OPERATION has no rendering in
		// the rebuild, and its arms were invisible to the classifier
		// (round-2 review, P3).
		{name: "84_a_correlated_set_operation_body", // PostgreSQL: 1, 2, 3
			sql: `SELECT id, (SELECT u.id FROM c2users x WHERE x.id=1 ` +
				`UNION ALL SELECT u.id FROM c2users y WHERE y.id=99) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `SET OPERATION`,
			routes:  a2Routes{Correlated: 1}},
		{name: "85_the_same_with_the_correlation_in_an_arms_WHERE", // PostgreSQL: 1, 2, 3
			sql: `SELECT id, (SELECT x.id FROM c2users x WHERE x.id=u.id ` +
				`UNION ALL SELECT y.id FROM c2users y WHERE y.id=99) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `SET OPERATION`,
			routes:  a2Routes{Correlated: 1}},

		// --- AN AGGREGATE BESIDE A NESTED SUBQUERY has no plan-time type, so
		// the item fell to FLOAT64 and an exact accumulator's DECIMAL reached
		// the client as the #361 silent-write guard's message (round-2
		// review, P4). It is one shape whether the accumulator is exact or
		// not: the int32 twins answered under OID 701 where PostgreSQL
		// declares bigint.
		{name: "86_an_aggregate_beside_a_nested_subquery_bigint", // PostgreSQL: 345, 348, 351
			sql: `SELECT id, (SELECT SUM(x.visits + (SELECT u.id)) FROM c2users x) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `aggregate beside a nested subquery`,
			routes:  a2Routes{Correlated: 1}},
		{name: "87_the_subquery_beside_rather_than_inside_the_aggregate", // PostgreSQL: 343, 344, 345
			sql: `SELECT id, (SELECT SUM(x.visits) + (SELECT u.id) FROM c2users x) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `aggregate beside a nested subquery`,
			routes:  a2Routes{Correlated: 1}},
		{name: "88_the_int32_twin_that_answered_under_the_wrong_type", // PostgreSQL: 9, 12, 15 as bigint
			sql: `SELECT id, (SELECT SUM(x.id + (SELECT u.id)) FROM c2users x) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `aggregate beside a nested subquery`,
			routes:  a2Routes{Correlated: 1}},
		{name: "89_ctl_the_same_aggregate_with_the_reference_written_directly",
			sql: `SELECT id, (SELECT SUM(x.visits + u.id) FROM c2users x) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,345 | 2,348 | 3,351`,
			routes: a2Routes{Correlated: 1}},
		{name: "90_ctl_an_aggregate_beside_a_subquery_in_a_HAVING_still_answers",
			sql: `SELECT id, (SELECT SUM(x.visits) FROM c2users x ` +
				`HAVING SUM(x.visits) > (SELECT u.id)) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,342 | 2,342 | 3,342`,
			routes: a2Routes{Correlated: 1}},

		// --- a FROM-less block's OWN aggregate or window call is the block's,
		// and answers (round-2 review, P2 — the docs said otherwise).
		{name: "91_a_fromless_blocks_own_aggregate_answers",
			sql:    `SELECT id, (SELECT MAX(1)) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,1 | 2,1 | 3,1`,
			routes: a2Routes{UnbuildableStage: 1}},
		{name: "92_a_fromless_blocks_own_count_answers",
			sql:    `SELECT id, (SELECT COUNT(*)) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,1 | 2,1 | 3,1`,
			routes: a2Routes{UnbuildableStage: 1}},
		{name: "93_a_fromless_blocks_own_window_call_answers",
			sql:    `SELECT id, (SELECT COUNT(*) OVER ()) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,1 | 2,1 | 3,1`,
			routes: a2Routes{ScalarProjection: 1}},
		// --- AN UNCORRELATED NESTED SUBQUERY BESIDE AN AGGREGATE TYPES FINE
		// (round-3 review, B1). The refusal exists for a nested subquery whose
		// own body names the ENCLOSING query — that one has no plan-time type.
		// An ordinary one does, and the correlation is in the WHERE, which the
		// re-run substitutes.
		{name: "94_an_aggregate_beside_an_uncorrelated_nested_subquery",
			sql: `SELECT id, (SELECT SUM(x.visits) + (SELECT MAX(y.id) FROM c2users y) ` +
				`FROM c2users x WHERE x.id <= u.id) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,103 | 2,145 | 3,345`,
			routes: a2Routes{Correlated: 1}},
		{name: "95_the_same_with_the_nested_subquery_inside_the_aggregate",
			sql: `SELECT id, (SELECT SUM(x.visits + (SELECT MAX(y.id) FROM c2users y)) ` +
				`FROM c2users x WHERE x.id <= u.id) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,103 | 2,148 | 3,351`,
			routes: a2Routes{Correlated: 1}},
		{name: "96_the_int32_spelling",
			sql: `SELECT id, (SELECT SUM(x.id) + (SELECT MAX(y.id) FROM c2users y) ` +
				`FROM c2users x WHERE x.id <= u.id) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,4 | 2,6 | 3,9`,
			routes: a2Routes{Correlated: 1}},
		{name: "97_the_EXISTS_position",
			sql: `SELECT id FROM c2users u WHERE EXISTS (SELECT SUM(x.visits) + ` +
				`(SELECT MAX(y.id) FROM c2users y) FROM c2users x WHERE x.id <= u.id) ORDER BY id`,
			want:   `id | 1 | 2 | 3`,
			routes: a2Routes{Correlated: 1}},
		{name: "98_a_MAX_beside_a_MIN",
			sql: `SELECT id, (SELECT MAX(x.visits) + (SELECT MIN(y.id) FROM c2users y) ` +
				`FROM c2users x WHERE x.visits <= u.visits) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,101 | 2,43 | 3,201`,
			routes: a2Routes{Correlated: 1}},
		// The discriminator: the nested subquery NAMES the enclosing query, so
		// the item has no type until the outer row is known and the refusal
		// stands (round-2 review, P4's shapes).
		{name: "99_the_nested_subquery_names_the_enclosing_query", // PostgreSQL: 343, 344, 345
			sql: `SELECT id, (SELECT SUM(x.visits) + (SELECT u.id) FROM c2users x) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `aggregate beside a nested subquery`,
			routes:  a2Routes{Correlated: 1}},
		{name: "100_the_same_inside_the_aggregate", // PostgreSQL: 642, 468, 942
			sql: `SELECT id, (SELECT SUM(x.visits + (SELECT u.visits)) FROM c2users x) AS v ` +
				`FROM c2users u ORDER BY id`,
			wantErr: `aggregate beside a nested subquery`,
			routes:  a2Routes{Correlated: 1}},

		// --- A GROUP BY TERM IS NEVER REWRITTEN INTO A SELECT-LIST POSITION
		// (round-3 review, B2). `GROUP BY (SELECT 1)` rewritten to `1` passed
		// the ungrouped-column validator as "group by item #1" and projected a
		// fabricated NULL row where PostgreSQL 17.11 and main both raise
		// 42803 — a row of NULLs beside a real aggregate.
		{name: "101_group_by_a_constant_subquery_is_42803",
			sql:     `SELECT visits FROM c2users u GROUP BY (SELECT 1)`,
			wantErr: `must appear in the GROUP BY clause`},
		{name: "102_the_same_beside_an_aggregate",
			sql:     `SELECT visits, COUNT(*) AS n FROM c2users u GROUP BY (SELECT 1)`,
			wantErr: `must appear in the GROUP BY clause`},
		{name: "103_the_same_with_an_alias",
			sql:     `SELECT visits AS vv FROM c2users u GROUP BY (SELECT 1)`,
			wantErr: `must appear in the GROUP BY clause`},
		{name: "104_two_constant_terms",
			sql:     `SELECT visits, name FROM c2users u GROUP BY (SELECT 1), (SELECT 2)`,
			wantErr: `must appear in the GROUP BY clause`},
		{name: "105_the_same_with_an_ORDER_BY",
			sql:     `SELECT visits FROM c2users u GROUP BY (SELECT 1) ORDER BY visits`,
			wantErr: `must appear in the GROUP BY clause`},
		{name: "106_the_same_with_a_SUM",
			sql:     `SELECT visits, SUM(id) AS s FROM c2users u GROUP BY (SELECT 1) ORDER BY visits`,
			wantErr: `must appear in the GROUP BY clause`},
		// The boundary: a select list that is entirely aggregates has nothing
		// to ungroup, and PostgreSQL answers it.
		{name: "107_boundary_an_all_aggregate_select_list_answers",
			sql:  `SELECT MAX(x.id) AS m FROM c2users x GROUP BY (SELECT 1)`,
			want: `m | 3`,
			pinArms: map[string]string{
				"dag":          `subqueries require a SubqueryRunner`,
				"dag-shuffled": `subqueries require a SubqueryRunner`,
				"dag-morsel4":  `subqueries require a SubqueryRunner`,
			},
			pinWhy: "cell 81's gap: a subquery in a GROUP BY term has no lowering in the DAG's " +
				"scan-agg fragment"},

		// --- A GROUP BY TERM COVERS THE SELECT ITEM WRITTEN THE SAME WAY,
		// AND NOTHING ELSE (round-5 addendum, round-3's c16). PostgreSQL
		// matches a SELECT item against a GROUP BY term AS WRITTEN: `GROUP BY
		// (SELECT u.visits)` covers `(SELECT u.visits)` and raises 42803 on
		// `visits`. The FROM-less unfold rewrote both sides to `u.visits`,
		// which made the second answer three plausible rows where PostgreSQL
		// 17.11 and bf99c56c both raise.
		{name: "126_a_subquery_GROUP_BY_term_does_not_cover_a_bare_column",
			sql:     `SELECT visits FROM c2users u GROUP BY (SELECT u.visits) ORDER BY visits`,
			wantErr: `must appear in the GROUP BY clause`},
		{name: "127_nor_the_qualified_spelling_of_the_same_column",
			sql:     `SELECT u.visits FROM c2users u GROUP BY (SELECT u.visits) ORDER BY 1`,
			wantErr: `must appear in the GROUP BY clause`},
		{name: "128_nor_a_string_column",
			sql:     `SELECT name FROM c2users u GROUP BY (SELECT u.name) ORDER BY name`,
			wantErr: `must appear in the GROUP BY clause`},
		{name: "129_it_covers_the_item_written_the_same_way",
			sql: `SELECT (SELECT u.visits) AS v FROM c2users u ` +
				`GROUP BY (SELECT u.visits) ORDER BY v`,
			want: `v | 42 | 100 | 200`},
		{name: "130_and_the_same_item_inside_arithmetic",
			sql: `SELECT (SELECT u.visits) + 0 AS v FROM c2users u ` +
				`GROUP BY (SELECT u.visits) ORDER BY v`,
			want: `v | 42 | 100 | 200`},
		{name: "131_a_DIFFERENT_subquery_in_the_item_is_not_covered", // PostgreSQL: 42803
			sql: `SELECT (SELECT u.id) AS v FROM c2users u ` +
				`GROUP BY (SELECT u.visits) ORDER BY v`,
			wantErr: `must appear in the GROUP BY clause`},
		{name: "132_boundary_an_ordinary_GROUP_BY_still_covers_the_subquery_item",
			sql:  `SELECT (SELECT u.visits) AS v FROM c2users u GROUP BY visits ORDER BY v`,
			want: `v | 42 | 100 | 200`},
		{name: "133_boundary_the_term_beside_a_real_key",
			sql: `SELECT visits, COUNT(*) AS n FROM c2users u ` +
				`GROUP BY visits, (SELECT u.visits) ORDER BY visits`,
			want: `visits,n | 42,1 | 100,1 | 200,1`},

		// --- A JOIN'S ON CONDITION IS A CLAUSE THE WALK READS (round-3
		// review, P1). It was the seventh clause, unread, and the shape was
		// planned uncorrelated and silently wrong at main too.
		{name: "108_an_outer_reference_in_a_JOINs_ON_condition",
			sql: `SELECT id, (SELECT t.visits FROM c2users x JOIN c2users t ON t.id = u.id ` +
				`WHERE x.id = 1) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,100 | 2,42 | 3,200`,
			routes: a2Routes{Correlated: 1}},

		// --- THE ORDER BY REFUSAL IS THE ORDINAL TRAP AND NOTHING WIDER
		// (round-3 review, P4). A sort with no slice cannot change which rows
		// a scalar subquery, an EXISTS or an IN set reads, so its term is left
		// as written; a term that SUBSTITUTES to an expression is written into
		// the rebuild; only a term that substitutes to a bare numeric literal
		// is refused, because both engines read that as a position.
		{name: "109_an_EXISTS_whose_ORDER_BY_names_the_outer_row",
			sql: `SELECT id FROM c2users u WHERE EXISTS ` +
				`(SELECT 1 FROM c2users x ORDER BY u.id) ORDER BY id`,
			want:   `id | 1 | 2 | 3`,
			routes: a2Routes{Correlated: 1}},
		{name: "110_a_one_row_subquery_whose_ORDER_BY_names_the_outer_row",
			sql: `SELECT id, (SELECT x.visits FROM c2users x WHERE x.id = 1 ` +
				`ORDER BY x.id * (u.id - 2)) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,100 | 2,100 | 3,100`,
			routes: a2Routes{Correlated: 1}},
		// A term that substitutes to a bare numeric literal is WRAPPED, not
		// declined (round-4 review, P2): `ORDER BY u.id` renders `ORDER BY
		// CAST(1 AS BIGINT)`, which is a constant expression on PostgreSQL
		// 17.11 and here and a select-list POSITION on neither. The `, x.id`
		// tiebreak is the cell's, not the repair's: a constant sort under a
		// LIMIT picks an arbitrary row on both engines (ADR-0013).
		{name: "111_an_ORDER_BY_term_that_substitutes_to_a_constant",
			sql: `SELECT id, (SELECT x.visits FROM c2users x ORDER BY u.id, x.id LIMIT 1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,100 | 2,100 | 3,100`,
			routes: a2Routes{Correlated: 1}},
		{name: "112_a_GROUP_BY_term_that_substitutes_to_a_constant_in_a_list",
			sql: `SELECT id, (SELECT COUNT(*) FROM c2users x GROUP BY u.id, x.id ` +
				`ORDER BY x.id LIMIT 1) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,1 | 2,1 | 3,1`,
			routes: a2Routes{Correlated: 1}},
		{name: "112a_an_ORDER_BY_naming_an_unprojected_column_substitutes_too",
			sql: `SELECT id, (SELECT x.visits FROM c2users x ORDER BY u.visits, x.id LIMIT 1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,100 | 2,100 | 3,100`,
			routes: a2Routes{Correlated: 1}},
		{name: "112b_a_GROUP_BY_term_that_substitutes_to_a_string",
			sql: `SELECT id, (SELECT SUM(x.visits) AS s FROM c2users x GROUP BY u.name) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,342 | 2,342 | 3,342`,
			routes: a2Routes{Correlated: 1}},

		// --- THE OUTER PROJECTION CARRIES EVERY COLUMN ITS SUBQUERIES
		// CORRELATE ON, WHICHEVER CLAUSE NAMES IT (round-4 review, P1). The
		// walk reads seven clauses; the pruning collector read three, so a
		// column named ONLY in a GROUP BY, an ORDER BY or a JOIN's ON was
		// pruned out of the enclosing projection and the per-row re-run then
		// read it out of a batch that does not carry it — 42703 `correlated
		// subquery references outer column u.name, which the outer query does
		// not carry (batch columns: id)`, on all five arms, for shapes main
		// answers exactly as PostgreSQL 17.11 does. The two lists are one
		// claim: whatever position can make a subquery CORRELATED can make the
		// enclosing query need the column.
		{name: "113_an_ORDER_BY_names_a_column_the_outer_list_omits",
			sql: `SELECT id, (SELECT x.visits FROM c2users x ORDER BY u.name, x.id LIMIT 1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,100 | 2,100 | 3,100`,
			routes: a2Routes{Correlated: 1}},
		{name: "114_a_GROUP_BY_names_a_column_the_outer_list_omits",
			sql: `SELECT id, (SELECT SUM(x.visits) AS s FROM c2users x ` +
				`GROUP BY u.name, x.visits ORDER BY x.visits LIMIT 1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,42 | 2,42 | 3,42`,
			routes: a2Routes{Correlated: 1}},
		{name: "115_an_EXISTS_whose_ORDER_BY_names_an_omitted_column",
			sql: `SELECT id FROM c2users u WHERE EXISTS ` +
				`(SELECT 1 FROM c2users x ORDER BY u.visits) ORDER BY id`,
			want:   `id | 1 | 2 | 3`,
			routes: a2Routes{Correlated: 1}},
		{name: "116_the_same_naming_a_string_column",
			sql: `SELECT id FROM c2users u WHERE EXISTS ` +
				`(SELECT 1 FROM c2users x ORDER BY u.name) ORDER BY id`,
			want:   `id | 1 | 2 | 3`,
			routes: a2Routes{Correlated: 1}},
		{name: "117_a_JOIN_ON_names_a_column_the_outer_list_omits",
			sql: `SELECT id, (SELECT t.visits FROM c2users x JOIN c2users t ON t.name = u.name ` +
				`WHERE x.id = 1) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,100 | 2,42 | 3,200`,
			routes: a2Routes{Correlated: 1}},
		{name: "118_the_LEFT_JOIN_twin",
			sql: `SELECT id, (SELECT t.visits FROM c2users x LEFT JOIN c2users t ` +
				`ON t.name = u.name WHERE x.id = 1) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,100 | 2,42 | 3,200`,
			routes: a2Routes{Correlated: 1}},
		// The RIGHT JOIN twin is LOUD for a reason that is not this arc's: a
		// substituted ON condition is `t.name = 'alice'`, which is not an
		// equality between two bare columns, and this engine lifts such a
		// condition into a filter above the join — legal for an INNER join
		// only. PostgreSQL answers 100, 42, 200; main answered 100, 100, 100,
		// the qualifier strip's constant, because the ON clause was not walked
		// at all. Wrong to loud, with PostgreSQL's value beside it.
		{name: "119_the_RIGHT_JOIN_twin_is_loud", // PostgreSQL: 100, 42, 200
			sql: `SELECT id, (SELECT t.visits FROM c2users x RIGHT JOIN c2users t ` +
				`ON t.name = u.name WHERE x.id = 1) AS v FROM c2users u ORDER BY id`,
			wantErr: `cannot be represented as an equi-join key`,
			routes:  a2Routes{Correlated: 1}},
		{name: "120_ctl_the_same_ORDER_BY_with_the_column_projected",
			sql: `SELECT id, name, (SELECT x.visits FROM c2users x ORDER BY u.name, x.id LIMIT 1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,name,v | 1,alice,100 | 2,bob,100 | 3,carol,100`,
			routes: a2Routes{Correlated: 1}},
		{name: "121_a_QUALIFY_names_an_omitted_column", // PostgreSQL: no QUALIFY
			sql: `SELECT id FROM c2users u WHERE EXISTS (SELECT 1 FROM c2users x ` +
				`QUALIFY ROW_NUMBER() OVER (ORDER BY x.id) <= u.id) ORDER BY id`,
			wantErr: `holds a window function`,
			routes:  a2Routes{Correlated: 1}},
		// A BARE outer name in one of the three clauses is the same claim one
		// scope down: `c2t2` has no `name` and no `visits`, so an unqualified
		// one there is the ENCLOSING row's (ADR-0021 §1k), and the outer
		// projection has to carry it just the same.
		{name: "123_a_bare_outer_name_in_a_GROUP_BY",
			sql: `SELECT id, (SELECT SUM(x.w) AS s FROM c2t2 x GROUP BY name) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,30 | 2,30 | 3,30`,
			routes: a2Routes{Correlated: 1}},
		{name: "124_a_bare_outer_name_in_an_ORDER_BY",
			sql: `SELECT id, (SELECT x.w FROM c2t2 x ORDER BY name, x.k LIMIT 1) AS v ` +
				`FROM c2users u ORDER BY id`,
			want:   `id,v | 1,10 | 2,10 | 3,10`,
			routes: a2Routes{Correlated: 1}},
		{name: "125_a_bare_outer_name_in_a_JOIN_ON",
			sql: `SELECT id, (SELECT y.w FROM c2t2 x JOIN c2t2 y ON y.k = x.k ` +
				`AND x.k = visits - 99 ORDER BY y.k LIMIT 1) AS v FROM c2users u ORDER BY id`,
			want:   `id,v | 1,10 | 2,NULL | 3,NULL`,
			routes: a2Routes{Correlated: 1}},
		{name: "122_a_set_operation_arm_names_an_omitted_column", // PostgreSQL: 1, 2, 3
			sql: `SELECT id FROM c2users u WHERE EXISTS (SELECT 1 FROM c2users x ` +
				`WHERE x.name = u.name UNION ALL SELECT 1 FROM c2users y WHERE y.id = 99) ORDER BY id`,
			wantErr: `body is a SET OPERATION`,
			routes:  a2Routes{Correlated: 1}},
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
				// A PER-ARM PIN is checked first, and it is checked whether or
				// not the cell carries a whole-cell pin: an arm may DIVERGE by
				// a refusal where the others answer, which is what a shape the
				// DAG cannot lower looks like.
				if pinned, ok := tc.pinArms[arm.name]; ok && tc.pin == "" {
					switch {
					case err != nil && strings.Contains(err.Error(), pinned):
						// The recorded per-arm refusal, unchanged.
					case err == nil && got == pinned:
						// The recorded per-arm value, unchanged.
					case err == nil && got == tc.want:
						t.Errorf("%s arm now AGREES with PostgreSQL (%s), so this per-arm pin "+
							"is FIXED: delete it from c2Cells.\n  pinned reason: %s\n  SQL: %s",
							arm.name, tc.want, tc.pinWhy, tc.sql)
					default:
						t.Errorf("%s arm answers %s%s, which is neither PostgreSQL's %s nor the "+
							"pinned %q\n  SQL: %s", arm.name, got, c2ErrText(err), tc.want,
							pinned, tc.sql)
					}
					continue
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
					pinned := tc.pin
					if p, ok := tc.pinArms[arm.name]; ok {
						pinned = p
					}
					switch {
					case err != nil:
						t.Errorf("%s arm: %v\n  SQL: %s\n  the pinned answer is %s",
							arm.name, err, tc.sql, pinned)
					case got == tc.want:
						t.Errorf("%s arm now AGREES with PostgreSQL (%s), so this pin is FIXED: "+
							"delete it from c2Cells.\n  pinned reason: %s\n  SQL: %s",
							arm.name, tc.want, tc.pinWhy, tc.sql)
					case got != pinned:
						t.Errorf("%s arm answers %s, which is neither PostgreSQL's %s nor the "+
							"pinned %s\n  SQL: %s", arm.name, got, tc.want, pinned, tc.sql)
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
