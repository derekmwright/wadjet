// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestArcPCQuantifiedComparisonOverAnArrayExpression holds `x op ANY|ALL
// (array expression)` to PostgreSQL 17.11's answers: the left operand is
// compared with each ELEMENT of the array the expression yields for that
// row — a a stored array column, or a function returning one — with SQL's
// three-valued reduction. Comparing it with the array itself answered false
// for every row, which is how `a.attnum = ANY(ix.indkey)` (SQLAlchemy),
// `oid = ANY(pol.polroles)` (psql \d+) and `nspname =
// ANY(current_schemas(true))` (pgJDBC) read the catalog (arc PC). The
// expected values were measured on PostgreSQL 17.11.
func TestArcPCQuantifiedComparisonOverAnArrayExpression(t *testing.T) {
	ctx := context.Background()
	db := pcArrayDB(t)
	for _, tc := range []struct{ sql, want string }{
		// id: k = ANY(a) / k <> ALL(a) / k < ALL(a) / k >= ANY(a)
		{`SELECT id, k = ANY(a) AS x FROM pcq ORDER BY id`,
			"1:true 2:false 3:true 4:NULL 5:NULL 6:NULL 7:false 8:false"},
		{`SELECT id, k <> ALL(a) AS x FROM pcq ORDER BY id`,
			"1:false 2:true 3:false 4:NULL 5:NULL 6:NULL 7:true 8:true"},
		{`SELECT id, k < ALL(a) AS x FROM pcq ORDER BY id`,
			"1:false 2:false 3:false 4:false 5:NULL 6:NULL 7:true 8:true"},
		{`SELECT id, k >= SOME(a) AS x FROM pcq ORDER BY id`,
			"1:true 2:true 3:true 4:true 5:NULL 6:NULL 7:false 8:false"},
		{`SELECT id, 'pg_catalog' = ANY(current_schemas(true)) AS x FROM pcq WHERE id = 1`, "1:true"},
		{`SELECT id, 'pg_catalog' = ANY(current_schemas(false)) AS x FROM pcq WHERE id = 1`, "1:false"},
		{`SELECT id, 'public' = ANY(current_schemas(false)) AS x FROM pcq WHERE id = 1`, "1:true"},
		{`SELECT id, NULL::BIGINT AS x FROM pcq WHERE 3 = ANY(a) ORDER BY id`, "1:NULL 2:NULL"},
	} {
		res, err := db.Query(ctx, tc.sql)
		if err != nil {
			t.Errorf("%s: %v", tc.sql, err)
			continue
		}
		got := ""
		for i, r := range res.Rows {
			if i > 0 {
				got += " "
			}
			v := r["x"]
			s := "NULL"
			if v != nil {
				s = fmt.Sprint(v)
			}
			got += fmt.Sprintf("%v:%s", r["id"], s)
		}
		if got != tc.want {
			t.Errorf("%s\n  got  %s\n  want %s", tc.sql, got, tc.want)
		}
	}
}

// pcArrayDB is a stored table with a nullable ARRAY(BIGINT) column: arrays
// with a NULL element, a NULL array and an empty one.
func pcArrayDB(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "a", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}},
	}}
	if err := db.CreateTable(ctx, "pcq", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("pcq", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 8})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "k": int64(2), "a": []any{int64(1), int64(2), int64(3)}},
		{"id": int64(2), "k": int64(5), "a": []any{int64(1), int64(2), int64(3)}},
		{"id": int64(3), "k": int64(2), "a": []any{int64(2), nil}},
		{"id": int64(4), "k": int64(5), "a": []any{int64(2), nil}},
		{"id": int64(5), "k": int64(2), "a": nil},
		{"id": int64(6), "k": nil, "a": []any{int64(1)}},
		{"id": int64(7), "k": int64(2), "a": []any{}},
		{"id": int64(8), "k": nil, "a": []any{}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// pcRender renders a result as "v1|v2 v1|v2", NULL for a NULL.
func pcRender(cols []string, rows []map[string]any) string {
	out := ""
	for i, r := range rows {
		if i > 0 {
			out += " "
		}
		for j, c := range cols {
			if j > 0 {
				out += "|"
			}
			if r[c] == nil {
				out += "NULL"
			} else {
				out += fmt.Sprint(r[c])
			}
		}
	}
	return out
}

// TestArcPCSetReturningFunctionsInTheSelectList holds unnest(array) and
// generate_subscripts(array, dim) as SELECT items to PostgreSQL 17.11: each
// row expands to as many rows as its longest set, a shorter set is padded
// with NULL, a row whose sets are all empty or NULL produces nothing, and
// ORDER BY / LIMIT / DISTINCT / GROUP BY above the list see the expanded
// rows. SQLAlchemy reads a primary key's columns this way; it was 42883
// (unknown function: unnest). Expected values measured on PostgreSQL 17.11.
func TestArcPCSetReturningFunctionsInTheSelectList(t *testing.T) {
	ctx := context.Background()
	db := pcArrayDB(t)
	for _, tc := range []struct{ sql, want string }{
		{`SELECT id, unnest(a) AS e, generate_subscripts(a, 1) AS o FROM pcq ORDER BY id, o`,
			"1|1|1 1|2|2 1|3|3 2|1|1 2|2|2 2|3|3 3|2|1 3|NULL|2 4|2|1 4|NULL|2 6|1|1"},
		{`SELECT count(*) AS n FROM (SELECT unnest(a) AS e FROM pcq) s`, "11"},
		{`SELECT e, count(*) AS n FROM (SELECT unnest(a) AS e FROM pcq WHERE k = 2) s GROUP BY e ORDER BY e`,
			"1|1 2|2 3|1 NULL|1"},
		{`SELECT unnest(a) AS e FROM pcq ORDER BY e LIMIT 3`, "1 1 1"},
		{`SELECT DISTINCT e FROM (SELECT unnest(a) AS e FROM pcq) s ORDER BY e`, "1 2 3 NULL"},
		{`SELECT id, unnest(a) AS e, unnest(current_schemas(true)) AS s FROM pcq WHERE id = 6 ORDER BY s`,
			"6|1|pg_catalog 6|NULL|public"},
		{`SELECT generate_subscripts(a, 2) AS o FROM pcq`, ""},
		{`SELECT unnest(ARRAY[3,1,2]) AS e`, "3 1 2"},
		{`SELECT x.attnum, x.ord FROM (SELECT unnest(ix.indkey) AS attnum, generate_subscripts(ix.indkey, 1) AS ord FROM pg_index ix) x`, ""},
	} {
		res, err := db.Query(ctx, tc.sql)
		if err != nil {
			t.Errorf("%s: %v", tc.sql, err)
			continue
		}
		if got := pcRender(res.Columns, res.Rows); got != tc.want {
			t.Errorf("%s\n  got  %s\n  want %s", tc.sql, got, tc.want)
		}
	}
	// A set-returning item beside an aggregate is evaluated AFTER the
	// aggregate by PostgreSQL; this planner does not implement that order and
	// refuses rather than answer through another one.
	for _, q := range []string{
		`SELECT unnest(a) AS e, count(*) AS n FROM pcq GROUP BY a`,
		`SELECT DISTINCT unnest(a) AS e FROM pcq`,
		`SELECT unnest(a) + 1 AS e FROM pcq`,
		`SELECT id FROM pcq WHERE unnest(a) = 1`,
	} {
		_, err := db.Query(ctx, q)
		if state := sqlerr.StateOf(err); state != "0A000" {
			t.Errorf("%s: %v (SQLSTATE %q); a set-returning call anywhere but a whole SELECT item is refused 0A000", q, err, state)
		}
	}
}
