package wadjet

import (
	"context"
	"testing"
)

// TestInSubquerySemiJoinAnswersEveryAliasSpelling is #516's value gate on the
// single-process path — the path the bug was on, and the one most interactive
// queries take.
//
// `SELECT COUNT(*) FROM t a WHERE a.x IN (SELECT b.x FROM t b WHERE …)`
// answered 0 for every wide type while the stage DAG answered correctly and
// PostgreSQL agreed with the DAG. The cause was in the IN → semi-join
// lowering, not in any type: the rewrite named the join's inner key `b.x`,
// which the inner plan's Scan emits as `x`, and the executor's key-repair
// heuristic then swapped a pair that was not misassigned (on a self-IN the
// bare name is present on BOTH sides, so the repair's premise does not hold).
//
// Every expectation below was read off PostgreSQL 17 over the same fixture.
func TestInSubquerySemiJoinAnswersEveryAliasSpelling(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	cases := []struct {
		name string
		sql  string
		want int64
	}{
		// ids 0..499 are the 500 rows the subquery yields.
		{"both_relations_aliased", // the #516 repro
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT b.id FROM mbtypes b WHERE b.id < 500)`, 500},
		{"inner_select_item_bare",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT id FROM mbtypes b WHERE b.id < 500)`, 500},
		{"neither_relation_aliased",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id IN (SELECT id FROM mbtypes WHERE id < 500)`, 500},
		{"inner_select_item_qualified_by_table_name",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT mbtypes.id FROM mbtypes WHERE mbtypes.id < 500)`, 500},
		{"inner_select_item_aliased",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT b.id AS bid FROM mbtypes b WHERE b.id < 500)`, 500},
		{"not_in_anti_join",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id NOT IN (SELECT b.id FROM mbtypes b WHERE b.id < 500)`, 4500},
		// g is 0,1,2 with no NULLs, so every row's g is in the subquery's set.
		{"group_by_inner",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.g IN (SELECT b.g FROM mbtypes b GROUP BY b.g)`, 5000},
		{"group_by_having_inner",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.g IN (SELECT b.g FROM mbtypes b GROUP BY b.g HAVING COUNT(*) > 1)`, 5000},
		// c_i64 is NULL every 31st row, so the subquery's set covers
		// 500 - ceil(500/31) = 484 of the 500 rows below the bound, and each
		// value is unique — 484 outer rows match.
		{"wide_typed_column",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.c_i64 IN (SELECT b.c_i64 FROM mbtypes b WHERE b.id < 500)`, 484},
		// Shapes the rewrite now DECLINES rather than lowering to a key it
		// cannot name — the IN stays a subquery predicate and is answered.
		// Both used to fail the physical planner outright ("cannot be
		// represented as an equi-join key").
		{"ungrouped_aggregate_inner",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT MAX(b.id) FROM mbtypes b)`, 1},
		{"computed_inner_select_item",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT b.id + 0 FROM mbtypes b WHERE b.id < 500)`, 500},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("query error: %v\n  SQL: %s", err, tc.sql)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("got %d rows, want 1 (scalar COUNT)\n  SQL: %s", len(res.Rows), tc.sql)
			}
			if got := res.Rows[0]["n"]; got != tc.want {
				t.Errorf("COUNT(*) = %v, want %d (PostgreSQL 17)\n  SQL: %s", got, tc.want, tc.sql)
			}
		})
	}
}

// TestInSubqueryHonorsTheSubqueryLimit is #482's value gate.
//
// `WHERE x IN (SELECT … LIMIT n)` matched against the FULL unbounded result
// set for any n, so the predicate selected every row. The semi join the IN
// lowers to has nowhere to put the bound — its build side IS the relation the
// subquery reads — so a bounded subquery is not decorrelated at all and is
// executed as written instead.
//
// Every entry carries an ORDER BY inside the subquery, because a bare LIMIT
// does not say WHICH rows it yields (ADR-0013's legal-nondeterminism list);
// the two bare-LIMIT entries below assert only what is determined regardless.
// Expectations read off PostgreSQL 17.
func TestInSubqueryHonorsTheSubqueryLimit(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	cases := []struct {
		name string
		sql  string
		want int64
	}{
		{"ordered_limit",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id IN (SELECT id FROM mbtypes ORDER BY id LIMIT 3)`, 3},
		{"ordered_limit_offset",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id IN (SELECT id FROM mbtypes ORDER BY id LIMIT 3 OFFSET 5)`, 3},
		{"ordered_limit_aliased",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT b.id FROM mbtypes b ORDER BY b.id LIMIT 3)`, 3},
		{"not_in_ordered_limit",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id NOT IN (SELECT id FROM mbtypes ORDER BY id LIMIT 3)`, 4997},
		// LIMIT 0 is a bound, not an absence (#481) — the membership set is
		// empty and nothing matches.
		{"limit_zero",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id IN (SELECT id FROM mbtypes ORDER BY id LIMIT 0)`, 0},
		// Bare LIMIT: WHICH three ids is unspecified, but id is unique, so
		// exactly three outer rows match whichever three they are.
		{"bare_limit",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id IN (SELECT id FROM mbtypes LIMIT 3)`, 3},
		{"bare_not_in_limit",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id NOT IN (SELECT id FROM mbtypes LIMIT 3)`, 4997},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("query error: %v\n  SQL: %s", err, tc.sql)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("got %d rows, want 1 (scalar COUNT)\n  SQL: %s", len(res.Rows), tc.sql)
			}
			if got := res.Rows[0]["n"]; got != tc.want {
				t.Errorf("COUNT(*) = %v, want %d (PostgreSQL 17)\n  SQL: %s", got, tc.want, tc.sql)
			}
		})
	}
}
