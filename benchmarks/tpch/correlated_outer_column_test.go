package tpch

import (
	"context"
	"testing"
)

// TestCorrelatedOuterColumnPruning is the #347 regression: a correlated
// subquery reading an outer column the outer query does not otherwise
// mention.
//
//	SELECT COUNT(*) FROM customer c1
//	WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer c2
//	                      WHERE c2.c_nationkey < c1.c_nationkey)
//
// answered 0. c_nationkey appears in neither the SELECT list nor the outer
// WHERE, so column pruning dropped it — the pruning walk had no case for a
// subquery node and never looked inside one. readOuterValues then found no
// such column in the batch and substituted NULL, every `< NULL` was UNKNOWN,
// and the predicate matched nothing. Silently: no error, no warning, just a
// zero.
//
// The correlation has to be NON-EQUI to reach that code at all. An equality
// correlation is decorrelated into a join by the logical optimizer before
// pruning runs and never executes per row, so an `=` version of any case
// below passes with the bug present and proves nothing (#334 hit this).
//
// Every expected value here is DuckDB's answer over the same SF0.01 fixture
// (benchmarks/tpch/duckdb-data), verified with /tmp/duckdb.
func TestCorrelatedOuterColumnPruning(t *testing.T) {
	ctx := context.Background()
	db := ingestDuckDBFixture(t, ctx, duckdbFixtureRows(t))

	cases := []struct {
		name string
		sql  string
		want float64
	}{
		{
			// The issue verbatim. 0 before the fix.
			name: "ScalarSubqueryUnprojectedOuterColumn",
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey)`,
			want: 726,
		},
		{
			// The control that already worked: the same correlation, with
			// the correlated column forced into the outer projection. It
			// answered 726 before the fix as well — which is what makes it
			// a control and not a second repro.
			name: "ScalarSubqueryProjectedOuterColumn",
			sql: `SELECT COUNT(*) AS n FROM (SELECT c_nationkey, c_acctbal FROM customer) c1
				WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey)`,
			want: 726,
		},
		{
			// EXISTS. 0 before the fix — the NULL made the inner predicate
			// UNKNOWN for every row, so no subquery ever found a row.
			name: "ExistsUnprojectedOuterColumn",
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE EXISTS (SELECT 1 FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey AND c2.c_acctbal > 9000)`,
			want: 1439,
		},
		{
			// NOT EXISTS: the same empty inner result, negated, so this one
			// returned every row (1500) rather than none. A row count alone
			// would have called that plausible. It is the complement of the
			// case above, and 1439 + 61 = 1500 is the whole table.
			name: "NotExistsUnprojectedOuterColumn",
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE NOT EXISTS (SELECT 1 FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey AND c2.c_acctbal > 9000)`,
			want: 61,
		},
		{
			// Correlation from two subqueries deep: c1 is bound by the
			// OUTERMOST query and read inside the inner-inner SELECT. This
			// needs the pruning walk to recurse (the column has to survive)
			// AND the correlation analysis and the value substitution to
			// recurse with it — neither walk descended into a nested
			// subquery, so the reference was never even classified as
			// correlated and the inner SQL went to the runner still naming
			// a table that is not in its FROM.
			name: "NestedTwoDeepScalar",
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE c1.c_acctbal > (SELECT AVG(c2.c_acctbal) FROM customer c2
					WHERE c2.c_acctbal > (SELECT AVG(c3.c_acctbal) FROM customer c3
						WHERE c3.c_nationkey < c1.c_nationkey))`,
			want: 368,
		},
		{
			// The same depth with an EXISTS as the inner level, over a
			// different table, so the nested scope has its own alias to
			// shadow: n and c2 are inner, c1 is not. The c_custkey bound
			// keeps the cost sane — three nested levels are O(rows³) with
			// per-row subquery execution.
			name: "NestedTwoDeepExists",
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE c1.c_custkey <= 100 AND c1.c_acctbal > (SELECT AVG(c2.c_acctbal) FROM customer c2
					WHERE EXISTS (SELECT 1 FROM nation n WHERE n.n_nationkey = c2.c_nationkey
						AND n.n_nationkey < c1.c_nationkey))`,
			want: 50,
		},
		{
			// A correlated IN whose subquery compares against an outer
			// column no other clause names.
			name: "InSubqueryUnprojectedOuterColumn",
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE c1.c_custkey IN (SELECT o.o_custkey FROM orders o
					WHERE o.o_totalprice > c1.c_acctbal * 50)`,
			want: 638,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, _, err := runWadjet(ctx, db, tc.sql)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rows))
			}
			if got := cellNum(rows[0], "n"); got != tc.want {
				t.Errorf("n = %v, want %v (DuckDB's answer over the same fixture)", got, tc.want)
			}
		})
	}
}
