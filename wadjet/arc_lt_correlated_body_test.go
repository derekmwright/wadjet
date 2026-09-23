// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A CORRELATED BODY IS EVALUATED PER OUTER ROW — the embedded arm of arc LT's
// seam (#1019, #1238, #1274, #1131, #1130), in CI. The five-arm table is
// coordinator.TestArcLTACorrelatedBodyIsEvaluatedPerOuterRowOnEveryArm; this
// is the single-process subset that FAILS AT 51addfb6 on every cell marked
// so, with PostgreSQL 17.11's row set beside each.
//
// lt_o holds key 1 TWICE and a key with no inner row; lt_i holds a tie. A
// body evaluated ONCE over the whole inner relation answers differently from
// one evaluated per outer row in every cell below.
func TestArcLTACorrelatedBodyIsEvaluatedPerOuterRow(t *testing.T) {
	ctx := context.Background()
	db := ltArcOpen(t, ctx)
	cases := []struct{ name, sql, want string }{
		// #1019 — the top-N-per-group idiom, every bound spelling.
		{"lateral ORDER BY LIMIT 2 per outer row",
			`SELECT o.id AS a, s.v AS v FROM lt_o o JOIN LATERAL (SELECT i.v AS v FROM lt_i i WHERE i.k = o.k ORDER BY i.v, i.id LIMIT 2) s ON true ORDER BY a, v`,
			"1,10;1,10;2,10;2,10;3,20;3,40"},
		{"LEFT lateral pads the outer rows with no inner row",
			`SELECT o.id AS a, s.v AS v FROM lt_o o LEFT JOIN LATERAL (SELECT i.v AS v FROM lt_i i WHERE i.k = o.k ORDER BY i.v DESC LIMIT 1) s ON true ORDER BY a, v`,
			"1,30;2,30;3,60;4,NULL;5,NULL"},
		{"comma lateral OFFSET 1",
			`SELECT o.id AS a, s.v AS v FROM lt_o o, LATERAL (SELECT i.v AS v FROM lt_i i WHERE i.k = o.k ORDER BY i.v, i.id OFFSET 1) s ORDER BY a, v`,
			"1,10;1,30;2,10;2,30;3,40;3,60"},
		{"LIMIT 1 OFFSET 1",
			`SELECT o.id AS a, s.v AS v FROM lt_o o JOIN LATERAL (SELECT i.v AS v FROM lt_i i WHERE i.k = o.k ORDER BY i.v, i.id LIMIT 1 OFFSET 1) s ON true ORDER BY a, v`,
			"1,10;2,10;3,40"},
		{"a tie under the bound projects the tied value",
			`SELECT o.id AS a, s.v AS v FROM lt_o o JOIN LATERAL (SELECT i.v AS v FROM lt_i i WHERE i.k = o.k ORDER BY i.v LIMIT 1) s ON true ORDER BY a, v`,
			"1,10;2,10;3,20"},
		{"a grouped body bounded by its aggregate",
			`SELECT o.id AS a, s.v AS v FROM lt_o o JOIN LATERAL (SELECT MAX(i.v) AS v FROM lt_i i WHERE i.k = o.k GROUP BY i.tag ORDER BY MAX(i.v) LIMIT 1) s ON true ORDER BY a, v`,
			"1,10;2,10;3,40"},
		{"an ORDINAL in the body's ORDER BY counts the list the query wrote",
			`SELECT o.id AS a, s.v AS v FROM lt_o o JOIN LATERAL (SELECT i.v AS v FROM lt_i i WHERE i.k = o.k ORDER BY 1 DESC LIMIT 1) s ON true ORDER BY a, v`,
			"1,30;2,30;3,60"},
		{"two bounded laterals in one statement",
			`SELECT o.id AS a, s.v AS v, t.w AS w FROM lt_o o JOIN LATERAL (SELECT i.v AS v FROM lt_i i WHERE i.k = o.k ORDER BY i.v DESC LIMIT 1) s ON true JOIN LATERAL (SELECT j.v AS w FROM lt_i j WHERE j.k = o.k ORDER BY j.v, j.id LIMIT 1) t ON true ORDER BY a, v, w`,
			"1,30,10;2,30,10;3,60,20"},
		{"an outer window over the bounded lateral",
			`SELECT o.id AS a, s.v AS v, ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY s.v) AS rn FROM lt_o o JOIN LATERAL (SELECT i.v AS v FROM lt_i i WHERE i.k = o.k ORDER BY i.v DESC, i.id LIMIT 2) s ON true ORDER BY a, v, rn`,
			"1,10,1;1,30,2;2,10,1;2,30,2;3,40,1;3,60,2"},
		{"an ungrouped aggregate with a HAVING the empty input fails drops the outer row",
			`SELECT o.id AS a, s.v AS v FROM lt_o o JOIN LATERAL (SELECT MAX(i.v) AS v FROM lt_i i WHERE i.k = o.k HAVING MAX(i.v) > 35) s ON true ORDER BY a, v`,
			"3,60"},
		// #1238 / #1274 — EXISTS keeps only a body it reproduces.
		{"EXISTS LIMIT 0 is never true",
			`SELECT o.id AS a FROM lt_o o WHERE EXISTS (SELECT 1 FROM lt_i i WHERE i.k = o.k LIMIT 0) ORDER BY a`,
			""},
		{"NOT EXISTS LIMIT 0 is always true",
			`SELECT o.id AS a FROM lt_o o WHERE NOT EXISTS (SELECT 1 FROM lt_i i WHERE i.k = o.k LIMIT 0) ORDER BY a`,
			"1;2;3;4;5"},
		{"EXISTS LIMIT 1 keeps the semi join",
			`SELECT o.id AS a FROM lt_o o WHERE EXISTS (SELECT 1 FROM lt_i i WHERE i.k = o.k LIMIT 1) ORDER BY a`,
			"1;2;3"},
		{"EXISTS over a grouped body honours the HAVING",
			`SELECT o.id AS a FROM lt_o o WHERE EXISTS (SELECT MAX(i.v) FROM lt_i i WHERE i.k = o.k GROUP BY i.tag HAVING MAX(i.v) > 35) ORDER BY a`,
			"3"},
		{"EXISTS over an ungrouped aggregate is true for every outer row",
			`SELECT o.id AS a FROM lt_o o WHERE EXISTS (SELECT MAX(i.v) FROM lt_i i WHERE i.k = o.k) ORDER BY a`,
			"1;2;3;4;5"},
		{"EXISTS OFFSET past the matches under a mixed correlation",
			`SELECT o.id AS a FROM lt_o o WHERE EXISTS (SELECT i.v FROM lt_i i WHERE i.k = o.k AND i.v > o.total ORDER BY i.v OFFSET 1) ORDER BY a`,
			"3"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ltArcRun(ctx, db, c.sql)
			if err != nil {
				t.Fatalf("%s\n  %v", c.sql, err)
			}
			if got != c.want {
				t.Fatalf("%s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.sql, got, c.want)
			}
		})
	}

	// A BODY WITH NO KEY IS REFUSED, 0A000, one sentence each — never the
	// plausible row set it answered at 51addfb6 (recorded beside each).
	refusals := []struct{ name, sql, class, base string }{
		{"a bound with an inequality correlation",
			`SELECT o.id AS a, s.v AS v FROM lt_o o JOIN LATERAL (SELECT i.v AS v FROM lt_i i WHERE i.v > o.total ORDER BY i.v LIMIT 1) s ON true ORDER BY a, v`,
			"cannot apply that bound per outer row", "rows=1 5,10 for PostgreSQL's 5"},
		{"a bound with a MIXED correlation",
			`SELECT o.id AS a, s.v AS v FROM lt_o o JOIN LATERAL (SELECT i.v AS v FROM lt_i i WHERE i.k = o.k AND i.v > o.total ORDER BY i.v LIMIT 1) s ON true ORDER BY a, v`,
			"cannot apply that bound per outer row", "rows=0 for PostgreSQL's 4"},
		{"a DISTINCT body under a bound",
			`SELECT o.id AS a, s.v AS v FROM lt_o o JOIN LATERAL (SELECT DISTINCT i.v AS v FROM lt_i i WHERE i.k = o.k ORDER BY i.v LIMIT 2) s ON true ORDER BY a, v`,
			"cannot apply that bound per outer row", "rows=4 for PostgreSQL's 6"},
		{"#1131 a DISTINCT body whose lifted predicate names another column",
			`SELECT o.id AS a, s.v AS v FROM lt_o o JOIN LATERAL (SELECT DISTINCT i.tag AS v FROM lt_i i WHERE i.v > o.total) s ON true ORDER BY a, v`,
			"which would have to publish the column it names", "rows=0 for PostgreSQL's 17"},
		{"an alias of the body colliding with the lifted column",
			`SELECT o.id AS a, s.v AS v FROM lt_o o JOIN LATERAL (SELECT i.id AS v FROM lt_i i WHERE i.v > o.total) s ON true ORDER BY a, v`,
			"which would have to publish the column it names", "rows=0 for PostgreSQL's 24"},
		{"an ungrouped aggregate with a HAVING the empty input PASSES",
			`SELECT o.id AS a, s.v AS v FROM lt_o o JOIN LATERAL (SELECT COUNT(*) AS v FROM lt_i i WHERE i.k = o.k HAVING COUNT(*) < 2) s ON true ORDER BY a, v`,
			"holds over an empty input", "1,0;2,0;3,0;4,0;5,0 for PostgreSQL's 4,0;5,0"},
		{"#1130 a lifted column the enclosing relation also publishes",
			`SELECT o.id AS a, s.v AS v FROM lt_o o LEFT JOIN LATERAL (SELECT i.v AS v FROM lt_i i WHERE i.id < o.id) s ON true ORDER BY a, v`,
			"which the enclosing relation also publishes", "five NULL pads for PostgreSQL's 11 rows"},
	}
	for _, c := range refusals {
		t.Run(c.name, func(t *testing.T) {
			got, err := ltArcRun(ctx, db, c.sql)
			if err == nil {
				t.Fatalf("%s\n  answered %s where a refusal is the disposition (at 51addfb6: %s)", c.sql, got, c.base)
			}
			if !strings.Contains(err.Error(), c.class) || sqlerr.StateOf(err) != "0A000" {
				t.Fatalf("%s\n  refused with a different sentence or class (%s): %v", c.sql, sqlerr.StateOf(err), err)
			}
		})
	}
}

func ltArcOpen(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	i64 := func(n string, null bool) parquet.Column {
		return parquet.Column{Name: n, Type: parquet.TypeInt64, Nullable: null}
	}
	for _, tb := range []struct {
		name   string
		schema parquet.Schema
		rows   []map[string]any
	}{
		{"lt_o", parquet.Schema{Columns: []parquet.Column{i64("id", false), i64("k", true), i64("total", false)}},
			[]map[string]any{
				{"id": int64(1), "k": int64(1), "total": int64(15)},
				{"id": int64(2), "k": int64(1), "total": int64(25)},
				{"id": int64(3), "k": int64(2), "total": int64(35)},
				{"id": int64(4), "k": int64(3), "total": int64(10)},
				{"id": int64(5), "k": nil, "total": int64(5)},
			}},
		{"lt_i", parquet.Schema{Columns: []parquet.Column{i64("id", false), i64("k", true), i64("v", false), {Name: "tag", Type: parquet.TypeString}}},
			[]map[string]any{
				{"id": int64(1), "k": int64(1), "v": int64(10), "tag": "a"},
				{"id": int64(2), "k": int64(1), "v": int64(10), "tag": "b"},
				{"id": int64(3), "k": int64(1), "v": int64(30), "tag": "a"},
				{"id": int64(4), "k": int64(2), "v": int64(20), "tag": "x"},
				{"id": int64(5), "k": int64(2), "v": int64(40), "tag": "x"},
				{"id": int64(6), "k": int64(2), "v": int64(60), "tag": "y"},
				{"id": int64(7), "k": nil, "v": int64(70), "tag": "n"},
			}},
	} {
		if err := db.CreateTable(ctx, tb.name, tb.schema, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(tb.name, tb.schema, nil, ingest.Config{MaxBufferRows: len(tb.rows) + 1, RowGroupSize: 3})
		if err := ing.Ingest(ctx, tb.rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// ltArcRun renders the row set sorted, `;`-joined, NULL spelled out.
func ltArcRun(ctx context.Context, db *DB, sql string) (string, error) {
	res, err := db.Query(ctx, sql)
	if err != nil {
		return "", err
	}
	rows := make([]string, 0, len(res.Rows))
	for i := range res.Rows {
		cells := res.Cells(i)
		s := make([]string, len(cells))
		for j, v := range cells {
			switch x := v.(type) {
			case nil:
				s[j] = "NULL"
			case float64:
				s[j] = strconv.FormatFloat(x, 'g', -1, 64)
			default:
				s[j] = fmt.Sprint(x)
			}
		}
		rows = append(rows, strings.Join(s, ","))
	}
	sort.Strings(rows)
	return strings.Join(rows, ";"), nil
}
