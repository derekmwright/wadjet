// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/worker"
)

// ARC PS's DISTRIBUTION arm: five constructs of PostgreSQL's grammar this
// engine did not read, answered on all five execution arms.
//
// internal/planner/sql's arc_ps_*_test.go files walk each construct's
// DOCUMENTED grammar — every accepted and rejected form, with case,
// whitespace and padding families — and assert what the parser does with it.
// This file asserts what the ENGINE ANSWERS, and it does so on five arms
// (single, spilled, DAG, DAG-shuffled, DAG-morsel) because a grammar change
// reaches every arm through the same parse but the ANSWER does not: a `JOIN …
// USING` star is merged by a logical pass that runs at BOTH planner entries,
// and the column-alias list's rename is applied a pass after the star expands
// on each of them.
//
// Every `want` is live PostgreSQL 17.11's answer over the same rows, measured
// rather than remembered.
func TestArcPSGrammarAnswersTheSameOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

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
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM,
		func(w *worker.Config) { w.MorselWorkers = 4 })

	arms := []struct {
		name string
		run  func(string) ([]string, error)
	}{
		{"single", func(sql string) ([]string, error) { return psRun(tmdRunSingle(ctx, single, sql)) }},
		{"spilled", func(sql string) ([]string, error) {
			restore := exec.ForceAggDrainEvery(1)
			restoreRuns := exec.ForceSmallSpillRuns(512)
			out, err := psRun(tmdRunSingle(ctx, spilled, sql))
			restoreRuns()
			exec.ForceAggDrainEvery(restore)
			return out, err
		}},
		{"dag", func(sql string) ([]string, error) { return psRun(tmdRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", func(sql string) ([]string, error) { return psRun(tmdRunDAG(ctx, coordB, sql)) }},
		{"dag+morsel4", func(sql string) ([]string, error) { return psRun(tmdRunDAG(ctx, coordM, sql)) }},
	}

	for _, c := range []struct {
		issue, name, sql string
		want             []string // psRun's rendering, or nil for a refusal
		// ordered marks a cell whose ANSWER IS ITS ROW ORDER. Round 1 shipped
		// this table with `sort.Strings` on both sides, so every `ORDER BY`
		// cell compared two MULTISETS and a wrong order was invisible — which
		// is how B1 (a bare `ORDER BY` on a RIGHT or FULL `JOIN … USING`
		// merged column binding the LEFT arm) reached a green table. An
		// ordered cell is compared ROW BY ROW as produced.
		ordered bool
		code    string // the SQLSTATE when want is nil
		errLike string
		pg      string
	}{
		// ---- #1154: BETWEEN [SYMMETRIC|ASYMMETRIC] -----------------------
		{issue: "#1154", name: "symmetric_in_where_reversed_bounds",
			sql:  `SELECT COUNT(*) AS c FROM psa WHERE id BETWEEN SYMMETRIC 2 AND 1`,
			want: []string{"c=int64:2"}, pg: "2"},
		{issue: "#1154", name: "not_symmetric_in_where",
			sql:  `SELECT COUNT(*) AS c FROM psa WHERE id NOT BETWEEN SYMMETRIC 2 AND 1`,
			want: []string{"c=int64:0"}, pg: "0"},
		{issue: "#1154", name: "asymmetric_is_the_plain_form",
			sql:  `SELECT COUNT(*) AS c FROM psa WHERE id BETWEEN ASYMMETRIC 2 AND 1`,
			want: []string{"c=int64:0"}, pg: "0"},
		// An AGGREGATE above it, so the predicate has to survive stage
		// generation and the spilled arm's drain, not only the scan.
		{issue: "#1154", name: "symmetric_under_an_aggregate",
			sql:  `SELECT SUM(a) AS s FROM psa WHERE a BETWEEN SYMMETRIC 20 AND 10`,
			want: []string{"s=30"},
			pg:   "30 — an integer SUM is an EXACT type here (ADR-0012), rendered without a box"},
		{issue: "#1154", name: "symmetric_projected",
			sql:     `SELECT id, (a BETWEEN SYMMETRIC 20 AND 10) AS v FROM psa ORDER BY id`,
			want:    []string{"id=int64:1|v=bool:true", "id=int64:2|v=bool:true"},
			ordered: true, pg: "t, t"},
		{issue: "#1154", name: "symmetric_with_a_null_bound_is_null",
			sql:  `SELECT (a BETWEEN SYMMETRIC NULL AND 10) AS v FROM psa WHERE id = 1`,
			want: []string{"v=NULL"},
			pg: "NULL — and NOT the `BETWEEN least(b,c) AND greatest(b,c)` reading, " +
				"which answers TRUE because least/greatest ignore NULLs"},
		{issue: "#1154", name: "symmetric_beside_a_using_join",
			sql: `SELECT COUNT(*) AS c FROM psa JOIN psb USING (id) ` +
				`WHERE psa.id BETWEEN SYMMETRIC 3 AND 1`,
			want: []string{"c=int64:1"}, pg: "1"},

		// ---- #1155: the ^ power operator ---------------------------------
		{issue: "#1155", name: "power_projected",
			sql:     `SELECT id, a ^ 2 AS p FROM psa ORDER BY id`,
			ordered: true,
			want:    []string{"id=int64:1|p=float:100", "id=int64:2|p=float:400"},
			pg:      "100, 400 (double precision over integer operands)"},
		{issue: "#1155", name: "power_under_an_aggregate",
			sql:  `SELECT MAX(a ^ 2) AS m FROM psa`,
			want: []string{"m=float:400"}, pg: "400"},
		{issue: "#1155", name: "power_is_left_associative",
			sql:  `SELECT 2 ^ 3 ^ 2 AS v FROM psa WHERE id = 1`,
			want: []string{"v=float:64"}, pg: "64 — (2^3)^2, not 512"},
		{issue: "#1155", name: "power_binds_tighter_than_multiplication",
			sql:  `SELECT 2 ^ 3 * 2 AS v FROM psa WHERE id = 1`,
			want: []string{"v=float:16"}, pg: "16"},
		{issue: "#1155", name: "unary_minus_binds_tighter_than_power",
			sql:  `SELECT -2 ^ 2 AS v FROM psa WHERE id = 1`,
			want: []string{"v=float:4"}, pg: "4"},
		{issue: "#1155", name: "power_in_a_predicate",
			sql:  `SELECT COUNT(*) AS c FROM psa WHERE a ^ 2 > 150`,
			want: []string{"c=int64:1"}, pg: "1"},
		{issue: "#1155", name: "zero_to_a_negative_power_is_undefined",
			sql:  `SELECT 0 ^ -1 AS v FROM psa WHERE id = 1`,
			code: "2201F", errLike: "zero raised to a negative power",
			pg: "2201F zero raised to a negative power is undefined"},

		// ---- #959 / #1158: the column-alias list -------------------------
		{issue: "#959", name: "base_table_star_over_a_renamed_relation",
			sql:     `SELECT * FROM psa a(k, v) ORDER BY k`,
			ordered: true,
			want:    []string{"k=int64:1|v=int64:10", "k=int64:2|v=int64:20"},
			pg:      "the two rows under k and v"},
		{issue: "#959", name: "base_table_without_as",
			sql:     `SELECT k FROM psa a(k, v) ORDER BY k`,
			ordered: true,
			want:    []string{"k=int64:1", "k=int64:2"}, pg: "1, 2"},
		{issue: "#959", name: "renamed_name_in_where",
			sql:  `SELECT COUNT(*) AS c FROM psa a(k, v) WHERE k = 1`,
			want: []string{"c=int64:1"}, pg: "1"},
		{issue: "#959", name: "renamed_name_across_a_join",
			sql:  `SELECT COUNT(*) AS c FROM psa a(k, v) JOIN psb b(m, w) ON a.k = b.m`,
			want: []string{"c=int64:1"}, pg: "1"},
		{issue: "#959", name: "qualified_star_over_a_renamed_relation",
			sql:     `SELECT a.* FROM psa a(k, v) ORDER BY k`,
			ordered: true,
			want:    []string{"k=int64:1|v=int64:10", "k=int64:2|v=int64:20"},
			pg:      "k, v — the rename is applied a pass AFTER the star expands"},
		{issue: "#959", name: "shorter_list_renames_a_prefix",
			sql:     `SELECT * FROM psa a(k) ORDER BY k`,
			ordered: true,
			want:    []string{"k=int64:1|a=int64:10", "k=int64:2|a=int64:20"},
			pg:      "k and the relation's own second name"},
		{issue: "#959", name: "renamed_away_name_is_gone",
			sql:  `SELECT id FROM psa a(k, v)`,
			code: "42703", errLike: `unknown column "id"`,
			pg: `42703 column "id" does not exist`},
		{issue: "#959", name: "overlong_list_is_42P10",
			sql:  `SELECT * FROM psa a(k, v, extra)`,
			code: "42P10", errLike: "2 columns available but 3 columns specified",
			pg: `42P10 table "a" has 2 columns available but 3 columns specified`},
		{issue: "#1158", name: "values_without_as",
			sql:     `SELECT * FROM (VALUES (1),(2)) v(n) ORDER BY n`,
			ordered: true,
			want:    []string{"n=int32:1", "n=int32:2"},
			pg:      "1, 2 — PostgreSQL's own integer literal is 32-bit too"},
		// A RESIDUE, loud. A column-alias list on a CTE REFERENCE renames the
		// query's output in the ENCLOSING scope, and this planner applies such
		// a list as a positional rename of a Project — which sits ABOVE the
		// CTE's own materialized block, so every renamed reference resolved
		// against the block's own columns and read NULL. Refused rather than
		// answered wrong; the list on the DEFINITION answers, which is the
		// cell below and is PostgreSQL's own other spelling.
		{issue: "#1158", name: "residue_cte_reference_list",
			sql:     `WITH w AS (SELECT id AS c, a AS d FROM psa) SELECT * FROM w z(x, y) ORDER BY x`,
			code:    "0A000",
			errLike: "column-alias list on a reference to WITH query",
			pg:      "ANSWERS x, y"},
		{issue: "#1158", name: "cte_definition_list_answers",
			sql:     `WITH w(x, y) AS (SELECT id AS c, a AS d FROM psa) SELECT * FROM w ORDER BY x`,
			ordered: true,
			want:    []string{"x=int64:1|y=int64:10", "x=int64:2|y=int64:20"},
			pg:      "x, y"},

		// ---- #655: JOIN … USING and the leading-dot literal ---------------
		//
		// The MERGE: the USING columns ONCE and FIRST, then each arm's
		// remaining columns. Three output columns where an ON join emits four.
		{issue: "#655", name: "star_over_an_inner_using_join",
			sql:     `SELECT * FROM psa JOIN psb USING (id) ORDER BY id`,
			ordered: true,
			want:    []string{"id=int64:2|a=int64:20|b=int64:200"},
			pg:      "one row, THREE columns: id, a, b"},
		{issue: "#655", name: "star_over_a_left_using_join",
			sql:     `SELECT * FROM psa LEFT JOIN psb USING (id) ORDER BY id`,
			ordered: true,
			want: []string{
				"id=int64:1|a=int64:10|b=NULL",
				"id=int64:2|a=int64:20|b=int64:200"},
			pg: "2 rows"},
		{issue: "#655", name: "star_over_a_right_using_join",
			sql:     `SELECT * FROM psa RIGHT JOIN psb USING (id) ORDER BY id`,
			ordered: true,
			want: []string{
				"id=int64:2|a=int64:20|b=int64:200",
				"id=int64:3|a=NULL|b=int64:300"},
			pg: "2 rows — the merged id is the RIGHT arm's, which is the side never NULL-extended"},
		// The FULL join is the cell the merge cannot answer with a plain
		// reference to either side: row 1's id lives only on the left and
		// row 3's only on the right, so the merged column is COALESCE. The
		// SET of rows is asserted here WITHOUT an ORDER BY, because ordering
		// a bare star by the merged column is refused — the cell below.
		{issue: "#655", name: "star_over_a_full_using_join_merges_with_coalesce",
			sql: `SELECT * FROM psa FULL JOIN psb USING (id)`,
			want: []string{
				"id=int64:1|a=int64:10|b=NULL",
				"id=int64:2|a=int64:20|b=int64:200",
				"id=int64:3|a=NULL|b=int64:300"},
			pg: "3 rows, and id is 1, 2, 3 — never NULL"},
		{issue: "#655", name: "star_over_a_using_join_with_the_arms_swapped",
			sql:     `SELECT * FROM psb JOIN psa USING (id) ORDER BY id`,
			ordered: true,
			want:    []string{"id=int64:2|b=int64:200|a=int64:20"},
			pg:      "id, b, a — the FROM clause's order"},
		{issue: "#655", name: "qualified_star_over_a_using_join",
			sql:     `SELECT psa.* FROM psa JOIN psb USING (id) ORDER BY id`,
			ordered: true,
			want:    []string{"id=int64:2|a=int64:20"},
			pg:      "psa's own two columns — a qualified star names one side and merges nothing"},
		{issue: "#655", name: "both_sides_still_addressable",
			sql:  `SELECT psa.id AS l, psb.id AS r FROM psa JOIN psb USING (id)`,
			want: []string{"l=int64:2|r=int64:2"}, pg: "both resolve to their own sides"},
		{issue: "#655", name: "using_over_two_columns",
			sql:  `SELECT COUNT(*) AS c FROM psa JOIN psb USING (id, a)`,
			code: "42703", errLike: "a",
			pg: `42703 column "a" specified in USING clause does not exist in right table`},
		{issue: "#655", name: "chained_using_over_the_merged_column",
			sql:  `SELECT COUNT(*) AS c FROM psa x JOIN psb y USING (id) JOIN psb z USING (id)`,
			want: []string{"c=int64:1"}, pg: "1"},
		// A GROUP BY above the merge, so the shape reaches the DAG's
		// aggregate stage and the spilled arm's drain.
		{issue: "#655", name: "using_join_grouped",
			sql: `SELECT psa.id AS id, COUNT(*) AS n FROM psa JOIN psb USING (id) ` +
				`GROUP BY psa.id ORDER BY psa.id`,
			ordered: true,
			want:    []string{"id=int64:2|n=int64:1"}, pg: "one row"},
		// ---- B1: the merged column NAMED BY A SORT KEY --------------------
		//
		// The ARMS SWAPPED is the whole of this block. In `psa FULL JOIN psb`
		// the row only the right arm has carries id 3, LARGER than every
		// left-arm key, so sorting by the left arm's `id` (NULLs last) and
		// sorting by the merged value happen to produce the same row order —
		// a fixture that cannot fail. In `psb FULL JOIN psa` the right-only
		// row carries id 1, which sorts FIRST under the merged key and LAST
		// under the left arm's. Round 1's table could not see it for a second
		// reason too: it compared SORTED renderings, so no cell here asserted
		// an order at all (see `ordered`).
		{issue: "#655", name: "b1_full_using_ordered_by_the_merged_key",
			sql:     `SELECT * FROM psb FULL JOIN psa USING (id) ORDER BY id`,
			code:    "0A000",
			errLike: "cannot be ORDERED BY the merged column",
			pg:      "ANSWERS 1, 2, 3 — the merged key, not psb.id (whose NULL is row 1)"},
		{issue: "#655", name: "b1_full_using_ordered_limit_1",
			sql:     `SELECT * FROM psb FULL JOIN psa USING (id) ORDER BY id LIMIT 1`,
			code:    "0A000",
			errLike: "cannot be ORDERED BY the merged column",
			pg:      "ANSWERS id=1 — a wrong key here is a wrong ROW, not only a wrong order"},
		{issue: "#655", name: "b1_full_using_ordered_desc",
			sql:     `SELECT * FROM psb FULL JOIN psa USING (id) ORDER BY id DESC`,
			code:    "0A000",
			errLike: "cannot be ORDERED BY the merged column",
			pg:      "ANSWERS 3, 2, 1"},
		{issue: "#655", name: "b1_full_using_ordered_desc_limit_2",
			sql:     `SELECT * FROM psb FULL JOIN psa USING (id) ORDER BY id DESC LIMIT 2`,
			code:    "0A000",
			errLike: "cannot be ORDERED BY the merged column",
			pg:      "ANSWERS 3, 2"},
		{issue: "#655", name: "b1_full_using_ordered_offset_1",
			sql:     `SELECT * FROM psb FULL JOIN psa USING (id) ORDER BY id OFFSET 1`,
			code:    "0A000",
			errLike: "cannot be ORDERED BY the merged column",
			pg:      "ANSWERS rows 2 and 3 — a wrong key here is a wrong ROW SET"},
		{issue: "#655", name: "b1_right_using_ordered_by_the_merged_key",
			sql:     `SELECT * FROM psb RIGHT JOIN psa USING (id) ORDER BY id`,
			ordered: true,
			want: []string{
				"id=int64:1|b=NULL|a=int64:10",
				"id=int64:2|b=int64:200|a=int64:20"},
			pg: "1, 2 — the merged key is the RIGHT arm's psa.id"},
		{issue: "#655", name: "b1_right_using_ordered_limit_1",
			sql:     `SELECT * FROM psb RIGHT JOIN psa USING (id) ORDER BY id LIMIT 1`,
			ordered: true,
			want:    []string{"id=int64:1|b=NULL|a=int64:10"}, pg: "id=1"},
		// The merged key NAMED but not starred: no minted star projection is
		// involved, so this is the binding alone.
		{issue: "#655", name: "b1_right_using_ordered_with_named_items",
			sql:     `SELECT psb.b AS v FROM psb RIGHT JOIN psa USING (id) ORDER BY id`,
			ordered: true,
			want:    []string{"v=NULL", "v=int64:200"}, pg: "NULL then 200"},
		{issue: "#655", name: "b1_full_using_ordered_with_named_items",
			sql:     `SELECT psb.b AS v FROM psb FULL JOIN psa USING (id) ORDER BY id`,
			ordered: true,
			want:    []string{"v=NULL", "v=int64:200", "v=int64:300"},
			pg:      "NULL, 200, 300 — ordered by the merged key"},
		// CONTROLS: an INNER and a LEFT join's merged column IS the left
		// arm's, so the binding that was wrong for RIGHT and FULL is right
		// here and must stay right.
		{issue: "#655", name: "b1_ctl_left_using_ordered",
			sql:     `SELECT * FROM psb LEFT JOIN psa USING (id) ORDER BY id`,
			ordered: true,
			want: []string{
				"id=int64:2|b=int64:200|a=int64:20",
				"id=int64:3|b=int64:300|a=NULL"},
			pg: "2, 3"},
		{issue: "#655", name: "b1_ctl_inner_using_ordered",
			sql:     `SELECT * FROM psb JOIN psa USING (id) ORDER BY id`,
			ordered: true,
			want:    []string{"id=int64:2|b=int64:200|a=int64:20"}, pg: "one row"},
		// A sort key that is NOT a merged column must keep its own binding.
		{issue: "#655", name: "b1_ctl_full_using_ordered_by_another_column",
			sql:     `SELECT * FROM psb FULL JOIN psa USING (id) ORDER BY b NULLS LAST`,
			ordered: true,
			want: []string{
				"id=int64:2|b=int64:200|a=int64:20",
				"id=int64:3|b=int64:300|a=NULL",
				"id=int64:1|b=NULL|a=int64:10"},
			pg: "200, 300, NULL"},
		// THE ORDINAL spelling of the same key. Position 1 IS the merged
		// column, and its value is minted by the projection the star expands
		// into — so it is refused at PLAN time and says so; position 2 is an
		// ordinary item and still sorts.
		{issue: "#655", name: "b1_full_using_ordered_by_position_1",
			sql:     `SELECT * FROM psb FULL JOIN psa USING (id) ORDER BY 1`,
			code:    "0A000",
			errLike: "POSITIONAL `ORDER BY` term naming the merged column",
			pg:      "ANSWERS 1, 2, 3"},
		{issue: "#655", name: "b1_ctl_full_using_ordered_by_position_2",
			sql:     `SELECT * FROM psb FULL JOIN psa USING (id) ORDER BY 2`,
			ordered: true,
			want: []string{
				"id=int64:2|b=int64:200|a=int64:20",
				"id=int64:3|b=int64:300|a=NULL",
				"id=int64:1|b=NULL|a=int64:10"},
			pg: "200, 300, NULL — position 2 is an ordinary item"},
		{issue: "#655", name: "b1_ctl_right_using_ordered_by_position_1",
			sql:     `SELECT * FROM psb RIGHT JOIN psa USING (id) ORDER BY 1`,
			ordered: true,
			want: []string{
				"id=int64:1|b=NULL|a=int64:10",
				"id=int64:2|b=int64:200|a=int64:20"},
			pg: "1, 2 — a RIGHT join's merged column is a plain reference"},
		// A WINDOW key over the merged column, which binds the same way an
		// ORDER BY key does. A FULL join's is a COALESCE and a window key
		// here is a column NAME, so that spelling is refused rather than
		// silently bound to the left arm.
		{issue: "#655", name: "b1_full_using_window_partition_by_is_refused",
			sql: `SELECT psa.a AS v, COUNT(*) OVER (PARTITION BY id) AS n ` +
				`FROM psb FULL JOIN psa USING (id)`,
			code:    "0A000",
			errLike: "window PARTITION BY key naming",
			pg:      "ANSWERS one row per merged key"},
		{issue: "#655", name: "b1_right_using_window_partition_by_the_merged_key",
			sql: `SELECT psa.a AS v, COUNT(*) OVER (PARTITION BY id) AS n ` +
				`FROM psb RIGHT JOIN psa USING (id) ORDER BY psa.a`,
			ordered: true,
			want:    []string{"v=int64:10|n=int64:1", "v=int64:20|n=int64:1"},
			pg:      "one row per partition"},

		// THE RESIDUES, each loud and each with its mechanism.
		{issue: "#655", name: "residue_natural_join",
			sql:  `SELECT COUNT(*) AS c FROM psa NATURAL JOIN psb`,
			code: "0A000", errLike: "NATURAL JOIN is not supported",
			pg: "ANSWERS 1 — the keys ARE the shared column names, which needs the catalog"},
		{issue: "#655", name: "residue_bare_reference_to_the_merged_column",
			sql:  `SELECT id FROM psa JOIN psb USING (id) ORDER BY id`,
			code: "42702", errLike: "ambiguous",
			pg: "ANSWERS 2 — USING merges the column, so a bare reference is not ambiguous " +
				"there; the scope that would know lives in internal/planner/physical"},

		// ---- the leading-dot numeric literal ------------------------------
		{issue: "#655", name: "dot_literal_in_arithmetic",
			sql:     `SELECT id, .5 + a AS v FROM psa ORDER BY id`,
			ordered: true,
			want:    []string{"id=int64:1|v=10.5", "id=int64:2|v=20.5"},
			pg:      "10.5, 20.5"},
		{issue: "#655", name: "dot_literal_exponent_form",
			sql:  `SELECT .5e1 AS v FROM psa WHERE id = 1`,
			want: []string{"v=float:5"}, pg: "5"},
		{issue: "#655", name: "trailing_dot_form",
			sql:  `SELECT 1. AS v FROM psa WHERE id = 1`,
			want: []string{"v=float:1"}, pg: "1"},
	} {
		t.Run(c.issue+"/"+c.name, func(t *testing.T) {
			want := append([]string(nil), c.want...)
			if !c.ordered {
				sort.Strings(want)
			}
			for _, arm := range arms {
				got, err := arm.run(c.sql)
				if !c.ordered {
					sort.Strings(got)
				}
				if c.want == nil {
					if err == nil {
						t.Errorf("%s arm: answered %v, but this shape is refused here\n"+
							"  want %s containing %q\n  PostgreSQL 17.11: %s\n  SQL: %s",
							arm.name, got, c.code, c.errLike, c.pg, c.sql)
						continue
					}
					if !strings.Contains(err.Error(), c.errLike) {
						t.Errorf("%s arm: error %v\n  want one containing %q\n  SQL: %s",
							arm.name, err, c.errLike, c.sql)
					}
					if s := sqlerr.StateOf(err); s != c.code {
						t.Errorf("%s arm: SQLSTATE %q, want %q\n  SQL: %s",
							arm.name, s, c.code, c.sql)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  PostgreSQL 17.11: %s\n  SQL: %s",
						arm.name, err, c.pg, c.sql)
					continue
				}
				if len(got) != len(want) {
					t.Errorf("%s arm: %d rows, want %d\n  got  %v\n  want %v (live PostgreSQL 17.11)\n  SQL: %s",
						arm.name, len(got), len(want), got, want, c.sql)
					continue
				}
				for i := range got {
					if got[i] != want[i] {
						t.Errorf("%s arm: row %d\n  got  %s\n  want %s (live PostgreSQL 17.11)\n  SQL: %s",
							arm.name, i, got[i], want[i], c.sql)
						break
					}
				}
			}
		})
	}
}

// psRun renders one result the way na2Run does — name=value per column, joined
// by `|`, one string per row — and RETURNS THE ROWS IN THE ORDER THE ENGINE
// PRODUCED THEM.
//
// That is the whole difference from na2Run, and it is why this exists: na2Run
// sorts its output, which makes every comparison a MULTISET comparison. That
// is right for a shape with no ORDER BY, where row order is unspecified
// (ADR-0013's legal classes), and it is exactly wrong for a shape whose
// question IS the order. The caller decides per cell (`ordered`), and an
// unordered cell sorts both sides itself.
func psRun(res *oracle.Result, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, 0, len(res.Columns))
		for _, c := range res.Columns {
			switch t := r[c].(type) {
			case nil:
				parts = append(parts, c+"=NULL")
			case string:
				parts = append(parts, c+"="+t)
			case float64:
				parts = append(parts, fmt.Sprintf("%s=float:%.6g", c, t))
			case float32:
				parts = append(parts, fmt.Sprintf("%s=float:%.6g", c, float64(t)))
			default:
				parts = append(parts, fmt.Sprintf("%s=%T:%v", c, r[c], r[c]))
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out, nil
}
