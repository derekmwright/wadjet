// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// ARC TF round 2 / B2 — A JOIN ARM IS A FROM ITEM.
//
// A correlated scalar subquery is re-run per outer row from a REBUILT
// statement. The rebuild learned to write a table function's FROM item as its
// CALL (#1203), and went on writing a JOIN's right arm as a bare NAME — so an
// arm re-parsed as a base table nothing declares, the arm read empty, the join
// produced no rows and the subquery answered 0 for every outer row (a MAX
// answered NULL). #1203's own defect, one clause lower. Measured by the
// round-1 review.
//
// Every `want` is PostgreSQL 17.11's answer over the same rows: `tfouter` is
// 1,2,3 and `tfjoin` is 1,2.
func TestArcTFACorrelatedSubqueryOverAJoinArmBindsTheOuterRow(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "tf.json")
	if err := os.WriteFile(jsonPath,
		[]byte("{\"k\":1,\"v\":\"x\"}\n{\"k\":2,\"v\":\"y\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, ddl := range []string{
		`CREATE TABLE tfouter (n BIGINT)`,
		`INSERT INTO tfouter VALUES (1),(2),(3)`,
		`CREATE TABLE tfjoin (k BIGINT)`,
		`INSERT INTO tfjoin VALUES (1),(2)`,
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	rj := "read_json('" + jsonPath + "')"

	for _, c := range []struct{ name, sql, want, pg string }{
		// ---- the function as the RIGHT arm --------------------------------
		{name: "a_series_as_the_right_arm",
			sql: `SELECT n, (SELECT COUNT(*) FROM tfjoin j JOIN generate_series(1,2) AS g(x) ` +
				`ON j.k = g.x WHERE g.x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		{name: "a_reader_as_the_right_arm",
			sql: `SELECT n, (SELECT COUNT(*) FROM tfjoin j JOIN ` + rj + ` AS f(k,v) ` +
				`ON j.k = f.k WHERE f.k <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		{name: "unnest_as_the_right_arm",
			sql: `SELECT n, (SELECT COUNT(*) FROM tfjoin j JOIN unnest(1,2) AS u(x) ` +
				`ON j.k = u.x WHERE u.x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		// ---- the function as the LEFT arm ---------------------------------
		{name: "a_series_as_the_left_arm",
			sql: `SELECT n, (SELECT COUNT(*) FROM generate_series(1,2) AS g(x) JOIN tfjoin j ` +
				`ON j.k = g.x WHERE g.x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		{name: "a_reader_as_the_left_arm",
			sql: `SELECT n, (SELECT COUNT(*) FROM ` + rj + ` AS f(k,v) JOIN tfjoin j ` +
				`ON j.k = f.k WHERE f.k <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		// ---- BOTH arms are table functions --------------------------------
		{name: "two_series_joined",
			sql: `SELECT n, (SELECT COUNT(*) FROM generate_series(1,2) AS g(x) ` +
				`JOIN generate_series(1,2) AS h(y) ON g.x = h.y WHERE g.x <= n) AS c ` +
				`FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		{name: "a_series_joined_to_a_reader",
			sql: `SELECT n, (SELECT COUNT(*) FROM generate_series(1,2) AS g(x) JOIN ` + rj +
				` AS f(k,v) ON g.x = f.k WHERE g.x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		// ---- the join KINDS -----------------------------------------------
		{name: "an_inner_join_written_out",
			sql: `SELECT n, (SELECT COUNT(*) FROM tfjoin j INNER JOIN generate_series(1,2) AS g(x) ` +
				`ON j.k = g.x WHERE g.x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		{name: "a_left_join_keeps_every_left_row",
			sql: `SELECT n, (SELECT COUNT(*) FROM tfjoin j LEFT JOIN generate_series(2,2) AS g(x) ` +
				`ON j.k = g.x WHERE j.k <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		// ---- a COMMA item beside one, which always worked -----------------
		{name: "a_comma_item_beside_a_table",
			sql: `SELECT n, (SELECT COUNT(*) FROM tfjoin j, generate_series(1,2) AS g(x) ` +
				`WHERE j.k = g.x AND g.x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		// ---- the arm wrapped in a derived table and in a CTE --------------
		{name: "the_join_inside_a_derived_table",
			sql: `SELECT n, (SELECT COUNT(*) FROM (SELECT g.x AS x FROM tfjoin j ` +
				`JOIN generate_series(1,2) AS g(x) ON j.k = g.x) s WHERE s.x <= n) AS c ` +
				`FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		{name: "the_join_inside_a_cte",
			sql: `SELECT n, (WITH cj AS (SELECT g.x AS x FROM tfjoin j ` +
				`JOIN generate_series(1,2) AS g(x) ON j.k = g.x) ` +
				`SELECT COUNT(*) FROM cj WHERE cj.x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		// ---- WITH ORDINALITY on the arm, which the bare name also dropped -
		{name: "an_arm_with_ordinality",
			sql: `SELECT n, (SELECT COUNT(*) FROM tfjoin j JOIN unnest(1,2) WITH ORDINALITY ` +
				`AS u(x,o) ON j.k = u.x WHERE u.x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		// ---- MAX, whose failure mode was NULL rather than 0 ---------------
		{name: "a_correlated_max_over_a_join_arm",
			sql: `SELECT n, (SELECT MAX(g.x) FROM tfjoin j JOIN generate_series(1,2) AS g(x) ` +
				`ON j.k = g.x WHERE g.x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		// ---- controls: the FROM-item spelling and an uncorrelated one -----
		{name: "control_the_function_is_the_from_item",
			sql: `SELECT n, (SELECT COUNT(*) FROM generate_series(1,2) AS g(x) ` +
				`WHERE g.x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		{name: "control_an_uncorrelated_join_arm",
			sql: `SELECT n, (SELECT COUNT(*) FROM tfjoin j JOIN generate_series(1,2) AS g(x) ` +
				`ON j.k = g.x) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|2;2|2;3|2", pg: "2 for every row"},
		// The inner `tfouter o2` SHADOWS the outer relation, so `n` inside the
		// subquery is the INNER one and the subquery is uncorrelated: 17.11
		// answers 2 for every row, measured. It is here as the control that
		// this arc did not turn a shadowed name into a correlated one.
		{name: "control_an_inner_relation_shadows_the_outer_name",
			sql: `SELECT n, (SELECT COUNT(*) FROM tfjoin j JOIN tfouter o2 ON j.k = o2.n ` +
				`WHERE j.k <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|2;2|2;3|2", pg: "2 for every row — the inner o2 shadows n"},
		// A DECLARED int4 key meeting a reader's untyped column: the pair is
		// widened to int8 at plan time, because the reader has no declaration
		// to widen FROM and an int4 key would meet an int8 vector at the
		// operator and refuse (#615). Before the series declared int4 this
		// shape answered by coincidence — both sides were int8.
		{name: "a_declared_int4_key_meets_an_untyped_reader_key",
			sql: `SELECT g.x FROM generate_series(1,2) AS g(x) JOIN ` + rj +
				` AS f(k,v) ON g.x = f.k ORDER BY g.x`,
			want: "[x] 1;2", pg: "1;2"},
		{name: "the_same_pair_with_the_arms_the_other_way",
			sql: `SELECT f.k FROM ` + rj + ` AS f(k,v) JOIN generate_series(1,2) AS g(x) ` +
				`ON g.x = f.k ORDER BY f.k`,
			want: "[k] 1;2", pg: "1;2"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := ptRenderQuery(ctx, db, c.sql)
			if err != nil {
				t.Fatalf("refused a statement PostgreSQL 17.11 answers %s: %v\n  SQL: %s",
					c.pg, err, c.sql)
			}
			if got != c.want {
				t.Errorf("answered\n  got  %s\n  want %s (PostgreSQL 17.11: %s)\n  SQL: %s",
					got, c.want, c.pg, c.sql)
			}
		})
	}
}
