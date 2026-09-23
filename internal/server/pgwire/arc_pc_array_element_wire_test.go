// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestArcPCAStoredArrayIsComparedByItsElementType is B7 (2) over a STORED
// array on the wire: `'x'::text = ANY(i_arr)` over a bigint[] column raised
// 42883 at the round-2 base (the pair was the array itself), answered false
// at round 2 (the array operand was skipped), and PostgreSQL 17.11 raises
// 42883 `operator does not exist: text = bigint`. The element pair decides.
func TestArcPCAStoredArrayIsComparedByItsElementType(t *testing.T) {
	db := pcCorpusDB(t)
	ctx := context.Background()
	el := parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64, Nullable: true},
		{Name: "i_arr", Type: parquet.TypeArray, Nullable: true, ElementType: &el},
	}}
	if err := db.CreateTable(ctx, "sa", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("sa", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"a": int64(1), "i_arr": []any{int64(1), int64(2)}}, {"a": int64(2)}, {"a": int64(3), "i_arr": []any{int64(3)}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	srv := startTestServer(t, db)
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet@%s/wadjet?sslmode=disable", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(ctx) })
	for _, c := range []struct{ sql, err, rows string }{
		{`SELECT a FROM sa WHERE 'x'::text = ANY(i_arr)`, "42883", ""},
		{`SELECT a, 'x'::text = ANY(sa.i_arr) FROM sa`, "42883", ""},
		{`SELECT a FROM sa WHERE 'x'::text <> ALL(i_arr)`, "42883", ""},
		{`SELECT a FROM sa WHERE 2 = ANY(i_arr) ORDER BY a`, "", "[[1]]"},
		{`SELECT a FROM sa WHERE 2.5 <> ALL(i_arr) ORDER BY a`, "", "[[1] [3]]"},
	} {
		o := pcRawText(ctx, conn, c.sql)
		if o.Err != c.err || (c.err == "" && fmt.Sprint(o.Rows) != c.rows) {
			t.Errorf("%s\n  got  err=%q rows=%v\n  want err=%q rows=%s (PostgreSQL 17.11)", c.sql, o.Err, o.Rows, c.err, c.rows)
		}
	}
}
