//go:build !race

package coordinator

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
)

// #424, the follow-on to #390 and the shape that guard now produces.
//
// A derived table whose ORDER BY names a column its own SELECT list does not
// project fails on the DAG and answers on the single-process path:
//
//	stage sort-1 (sort): ... sort: key column "__sortkey_0" does not exist
//	in the input schema
//
// logical.resolveOrderBy materializes such a term as a hidden __sortkey_N
// projection, which the single-process pipeline evaluates in a real Project
// below the Sort. On the DAG a Project emits no stage, and the one pass that
// wrote the name onto a producing fragment — attachScanSelectProjections —
// serves the OUTERMOST select list only. So this and #390 meet here: the
// guard keeps a sort with a dependent as its own stage (folding it into a
// probe-split predecessor would answer the wrong top-N), and that is exactly
// the stage whose input schema had no key to sort on. The plan-shape half is
// asserted in physical.TestDerivedTableSortKeyResolvesToAnEmittedColumn; this
// is the answer half.
func TestDistributedDerivedTableSortKeyNotSelected(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed test skipped in -short mode")
	}
	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF001)

	// The reference top-7. This query orders at the plan ROOT, which is the
	// shape that always worked, so it establishes which suppliers the inner
	// ORDER BY + LIMIT must select without hard-coding fixture values.
	ref := mustRows(t, mustExecuteSQL(t, ctx, coord,
		`SELECT s_suppkey FROM supplier ORDER BY s_acctbal DESC, s_suppkey DESC LIMIT 7`))
	if len(ref) != 7 {
		t.Fatalf("reference top-7 returned %d rows, want 7", len(ref))
	}
	var wantSum int64
	for _, r := range ref {
		wantSum += toInt64(r["s_suppkey"])
	}

	for _, tc := range []struct {
		name string
		sql  string
		want int64
	}{
		// The issue's minimal shape. No LIMIT, so the ORDER BY selects
		// nothing — but the sort still RUNS on the DAG (wadjet executes a
		// subquery's ORDER BY rather than eliding it, which is also what
		// PostgreSQL does in practice), so its key still has to resolve.
		{
			name: "no limit",
			sql:  `SELECT COUNT(*) AS c FROM (SELECT s_suppkey FROM supplier ORDER BY s_acctbal DESC) t`,
			want: 100,
		},
		// With a LIMIT the key decides which rows survive, so the assertion
		// is the sum of the surviving keys, not their count: a sort that
		// keyed on nothing would still return seven rows.
		{
			name: "with limit",
			sql: `SELECT SUM(s_suppkey) AS c FROM (
				SELECT s_suppkey FROM supplier
				ORDER BY s_acctbal DESC, s_suppkey DESC LIMIT 7) t`,
			want: wantSum,
		},
		// The join shape from the original report.
		{
			name: "join",
			sql: `SELECT SUM(s_suppkey) AS c FROM (
				SELECT s_suppkey FROM supplier JOIN nation ON s_nationkey = n_nationkey
				ORDER BY s_acctbal DESC, s_suppkey DESC LIMIT 7) t`,
			want: wantSum,
		},
		// A COMPUTED term: no source column exists to rename the key to, so
		// the producing fragment has to project the expression itself.
		{
			name: "computed key",
			sql: `SELECT SUM(s_suppkey) AS c FROM (
				SELECT s_suppkey FROM supplier
				ORDER BY s_acctbal * 2 DESC, s_suppkey DESC LIMIT 7) t`,
			want: wantSum,
		},
		// A join consumer rather than an aggregate one — #390's second case,
		// with the sort column left out of the derived table's projection.
		{
			name: "join consumer",
			sql: `SELECT COUNT(*) AS c FROM (
				SELECT s_suppkey FROM supplier
				ORDER BY s_acctbal DESC LIMIT 7) t
				JOIN supplier s2 ON t.s_suppkey = s2.s_suppkey`,
			want: 7,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := mustRows(t, mustExecuteSQL(t, ctx, coord, tc.sql))
			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rows))
			}
			if got := toInt64(rows[0]["c"]); got != tc.want {
				t.Fatalf("got %d, want %d — the derived table's ORDER BY selected the wrong rows",
					got, tc.want)
			}
		})
	}
}

func mustExecuteSQL(t *testing.T, ctx context.Context, coord *Coordinator, sql string) *SQLResult {
	t.Helper()
	res, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("%s\n  %v", sql, err)
	}
	if res.Error != "" {
		t.Fatalf("%s\n  %s", sql, res.Error)
	}
	return res
}
