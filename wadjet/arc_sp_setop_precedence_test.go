// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// SET-OPERATION PRECEDENCE ON THE EMBEDDED API (#1349): INTERSECT binds
// tighter than UNION and EXCEPT, as in PostgreSQL. `internal/coordinator`'s
// five-arm table holds the whole operator matrix; this is the issue's own
// shape, `A UNION C INTERSECT B`, and its siblings through DB.Query, over the
// first of that table's fixtures (reproduced here, not imported: that one
// lives in an AGPL _test.go file and this package is MIT). Every want is
// PostgreSQL 17.11's answer over these rows; the left-to-right reading the
// parser used to build answers the second value.
func TestArcSPSetOpPrecedenceOnTheEmbeddedAPI(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "x", Type: parquet.TypeInt64}}}
	for name, vals := range map[string][]int64{
		"sp1_a": {1, 1, 2, 3, 3, 3, 4, 5, 7},
		"sp1_b": {2, 3, 3, 4, 4, 6, 7, 7},
		"sp1_c": {1, 3, 4, 4, 4, 6, 8, 8, 7},
		"sp1_d": {1, 6, 6, 7, 9, 9},
	} {
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, len(vals))
		for i, v := range vals {
			rows[i] = map[string]any{"x": v}
		}
		ing := db.NewIngester(name, schema, nil, ingest.Config{MaxBufferRows: len(rows) + 1, RowGroupSize: 3})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	a, b, c, d := "SELECT x FROM sp1_a", "SELECT x FROM sp1_b", "SELECT x FROM sp1_c", "SELECT x FROM sp1_d"
	cases := []struct {
		sql, want, leftToRight string
		ordered                bool
	}{
		{a + " UNION " + b + " INTERSECT " + c, "1 2 3 4 5 6 7", "1 3 4 6 7", false},
		{a + " UNION ALL " + b + " INTERSECT ALL " + c, "1 1 2 3 3 3 3 4 4 4 5 6 7 7", "1 3 4 4 4 6 7", false},
		{a + " EXCEPT " + b + " INTERSECT " + c, "1 2 5", "1", false},
		{a + " EXCEPT ALL " + b + " INTERSECT ALL " + c, "1 1 2 3 3 5", "1 3", false},
		{a + " INTERSECT " + b + " UNION " + c + " INTERSECT " + d, "1 2 3 4 6 7", "1 6 7", false},
		{a + " UNION " + b + " INTERSECT " + c + " ORDER BY 1 LIMIT 4 OFFSET 1", "2 3 4 5", "3 4 6 7", true},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			out, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			if len(out.Columns) != 1 || out.Columns[0] != "x" {
				t.Fatalf("columns %v, want [x]", out.Columns)
			}
			got := make([]string, len(out.Rows))
			for i := range out.Rows {
				cells := out.Cells(i)
				if len(cells) != 1 {
					t.Fatalf("row %d: %v", i, cells)
				}
				v, ok := cells[0].(int64)
				if !ok {
					t.Fatalf("row %d: %T %v, want int64", i, cells[0], cells[0])
				}
				got[i] = fmt.Sprint(v)
			}
			if !tc.ordered {
				sort.Slice(got, func(i, j int) bool { return len(got[i]) < len(got[j]) || len(got[i]) == len(got[j]) && got[i] < got[j] })
			}
			if g := strings.Join(got, " "); g != tc.want {
				note := ""
				if g == tc.leftToRight {
					note = " (the left-to-right reading)"
				}
				t.Errorf("got  %s%s\nwant %s", g, note, tc.want)
			}
		})
	}
}

// A RECURSIVE TERM THAT IS AN INTERSECT CHAIN. With INTERSECT binding tighter,
// `seed UNION ALL SELECT … FROM r INTERSECT …` is `seed UNION ALL (SELECT …
// FROM r INTERSECT …)`: the recursive term is the INTERSECT, as in
// PostgreSQL. PostgreSQL then answers a DISTINCT INTERSECT and refuses (42P19)
// a self-reference under INTERSECT ALL, under EXCEPT ALL's left operand or
// under EXCEPT's right operand, through a derived table too — measured on
// 17.11 for every cell. The left-to-right parse refused the unparenthesised
// cells as "within its non-recursive term"; the parenthesised ones already
// reached the term and iterated where PostgreSQL refuses.
//
// The last cell (N3, round-2 review) is the top-level operator itself: a
// PLAIN `UNION` (not `UNION ALL`) whose term is `… INTERSECT ALL …`. The
// term's shape is checked before this engine's own UNION-vs-UNION-ALL
// capability gap (0A000), so this refuses 42P19 "within INTERSECT" — the
// SQLSTATE and sentence PostgreSQL gives — rather than 0A000.
func TestArcSPRecursiveTermSetOperationsFollowPostgres(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	const defaultHead = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
	const tail = ") SELECT count(*), max(n) FROM r"
	cases := []struct{ label, term, head, want, wantState string }{
		{"", "SELECT n+1 FROM r WHERE n<5 INTERSECT SELECT 2", "", "2 2", ""},
		{"", "SELECT 3 INTERSECT SELECT n+1 FROM r WHERE n<5", "", "1 1", ""},
		{"", "SELECT 2 INTERSECT SELECT n+1 FROM r WHERE n<5", "", "2 2", ""},
		{"", "(SELECT n+1 FROM r WHERE n<5 EXCEPT SELECT 3)", "", "2 2", ""},
		{"", "(SELECT n+1 FROM r WHERE n<5 INTERSECT (SELECT 2 EXCEPT ALL SELECT 9))", "", "2 2", ""},
		{"", "SELECT n+1 FROM r WHERE n<5 INTERSECT ALL SELECT 2", "", `ERR recursive reference to query "r" must not appear within INTERSECT`, "42P19"},
		{"", "SELECT 2 INTERSECT ALL SELECT n+1 FROM r WHERE n<5", "", `ERR recursive reference to query "r" must not appear within INTERSECT`, "42P19"},
		{"", "(SELECT n+1 FROM r WHERE n<5 INTERSECT ALL SELECT 2)", "", `ERR recursive reference to query "r" must not appear within INTERSECT`, "42P19"},
		{"", "SELECT m FROM (SELECT n+1 AS m FROM r WHERE n<5 INTERSECT ALL SELECT 2) q", "", `ERR recursive reference to query "r" must not appear within INTERSECT`, "42P19"},
		{"", "(SELECT n+1 FROM r WHERE n<5 EXCEPT ALL SELECT 3)", "", `ERR recursive reference to query "r" must not appear within EXCEPT`, "42P19"},
		{"", "(SELECT 3 EXCEPT SELECT n+1 FROM r WHERE n<5)", "", `ERR recursive reference to query "r" must not appear within EXCEPT`, "42P19"},
		{"", "SELECT n+1 FROM r WHERE n<5 INTERSECT SELECT 2 UNION ALL SELECT 7", "", `ERR recursive reference to query "r" must not appear within its non-recursive term`, "42P19"},
		{
			"N3 top-level UNION (not ALL), term INTERSECT ALL",
			"SELECT n+1 FROM r WHERE n<5 INTERSECT ALL SELECT 2",
			"WITH RECURSIVE r(n) AS (SELECT 1 UNION ",
			`ERR recursive reference to query "r" must not appear within INTERSECT`, "42P19",
		},
	}
	for _, tc := range cases {
		head := tc.head
		if head == "" {
			head = defaultHead
		}
		sql := head + tc.term + tail
		name := tc.label
		if name == "" {
			name = tc.term
		}
		t.Run(name, func(t *testing.T) {
			out, err := db.Query(ctx, sql)
			if strings.HasPrefix(tc.want, "ERR ") {
				if err == nil || !strings.Contains(err.Error(), strings.TrimPrefix(tc.want, "ERR ")) {
					t.Errorf("%s\n  got  %v %v\n  want %s", sql, err, out, tc.want)
				}
				if tc.wantState != "" {
					if got := sqlerr.StateOf(err); got != tc.wantState {
						t.Errorf("%s\n  SQLSTATE got  %s\n  SQLSTATE want %s", sql, got, tc.wantState)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			if len(out.Rows) != 1 {
				t.Fatalf("%s: %d rows", sql, len(out.Rows))
			}
			if got := fmt.Sprint(out.Cells(0)...); got != tc.want {
				t.Errorf("%s\n  got  %s\n  want %s", sql, got, tc.want)
			}
		})
	}
}
