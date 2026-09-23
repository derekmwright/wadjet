// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// bxCommaFixture is the small `ord` / `item` pair every case below runs
// against — the same shape as `internal/coordinator`'s `lat_ord` / `lat_item`
// fixture, reproduced here (rather than imported) because that one lives in
// an AGPL _test.go file and this package is MIT.
//
//	ord:  (1,150) (2,200) (3,0)
//	item: (1,1,50) (2,1,100) (3,2,75) (4,2,125)   — order 3 has none
func bxCommaFixture(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		"CREATE TABLE ord (id INT64, total FLOAT64)",
		"CREATE TABLE item (id INT64, order_id INT64, amount FLOAT64)",
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	for _, dml := range []string{
		"INSERT INTO ord VALUES (1, 150)",
		"INSERT INTO ord VALUES (2, 200)",
		"INSERT INTO ord VALUES (3, 0)",
		"INSERT INTO item VALUES (1, 1, 50)",
		"INSERT INTO item VALUES (2, 1, 100)",
		"INSERT INTO item VALUES (3, 2, 75)",
		"INSERT INTO item VALUES (4, 2, 125)",
	} {
		if _, err := db.Execute(ctx, dml); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// bxRunRows runs sql over db and renders the "v" column as a SORTED string
// set, so a legal ordering difference is never read as a wrong answer.
func bxRunRows(t *testing.T, db *DB, sql string) []string {
	t.Helper()
	res, err := db.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, fmt.Sprintf("%v", r["v"]))
	}
	sort.Strings(out)
	return out
}

// A JOIN's ON condition can reference a comma-join sibling: a deliberate
// DuckDB-matching superset over PostgreSQL, which refuses the reference with
// 42P01 `invalid reference to FROM-clause entry` (ADR-0012 §5 #617, ruled on
// #617). `benchmarks/tpch/duckdb_compare_test.go`'s
// `CommaJoinOnReferencesEarlierItem` gates the same shape over the TPC-H
// fixture, live against DuckDB; this is its DuckDB-FREE twin — small
// in-memory tables, and the row set copied from that corpus's DuckDB
// baseline (`benchmarks/tpch/baseline-duckdb-sf001.json`) rather than
// compared live, so it runs without a DuckDB binary and fails loudly on its
// own if the mechanism regresses again.
//
// Arc RS (#1220) built the planner's ON-scope validation and, doing so,
// refused this shape alongside the genuinely out-of-scope "declared later"
// shapes it was built for — conflating a reference to a relation already on
// the page (a comma sibling written earlier) with one to a relation the
// parser has not read yet. This test FAILS at the tip that regression
// shipped on (main 13a45e69) with exactly this refusal; the BX hotfix
// restores the answer by making `physical.visibleAtJoin` positional over the
// whole FROM clause rather than scoped to one FROM item.
func TestCommaJoinSiblingVisibleInLaterJoinON(t *testing.T) {
	db := bxCommaFixture(t)

	// The inner-join case: for each `ord` row crossed with every `item` row,
	// keep the ones where the explicit join's ON — which names `ord`, an
	// EARLIER comma-separated FROM item — matches. `ord` id 3 has no
	// matching `item.order_id`, so it drops out entirely.
	got := bxRunRows(t, db, "SELECT a.id AS v FROM ord a, item b JOIN item c ON a.id = c.order_id")
	want := []string{"1", "1", "1", "1", "1", "1", "1", "1", "2", "2", "2", "2", "2", "2", "2", "2"}
	if !equalRowSets(got, want) {
		t.Errorf("got  %v\nwant %v\n  the ON's reference to the comma sibling `a` must resolve, matching DuckDB", got, want)
	}

	// The outer-join case: the same comma-sibling reference inside a LEFT
	// JOIN's ON. `ord` id 3's total (0) matches no `item.amount`, so its
	// `item` side is NULL-extended but the row survives (LEFT, not INNER).
	got = bxRunRows(t, db, "SELECT a.id AS v FROM ord a, item b LEFT JOIN item c ON a.total > c.amount")
	want = []string{
		"1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
		"2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2",
		"3", "3", "3", "3",
	}
	if !equalRowSets(got, want) {
		t.Errorf("got  %v\nwant %v\n  the LEFT JOIN's ON referencing the comma sibling `a` must resolve too", got, want)
	}
}

func equalRowSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
