// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestArcPCArraysAndEmptyCatalogScansOnTheWire holds arc PC round 2's wire
// facts to PostgreSQL 17.11's measured answers:
//
//   - ARRAY(subquery) declares its element's array type and sends the
//     array's text form — 1016 and `{2,2,3,4}`, never 25 and `[2 2 3 4]`;
//   - current_schemas() is name[] on the wire (this engine: text[], 1009);
//   - a stored DECIMAL(10,2)[] column is described by the catalog as
//     numeric(10,2)[], its typmod kept (ADR-0044 decision 3);
//   - a filtered catalog scan that keeps no row still declares SELECT *'s
//     columns — a zero-row relation keeps its definitions.
func TestArcPCArraysAndEmptyCatalogScansOnTheWire(t *testing.T) {
	db := pcCorpusDB(t)
	ctx := context.Background()
	el := parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}
	dl := parquet.Column{Name: "element", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64, Nullable: true},
		{Name: "d_arr", Type: parquet.TypeArray, Nullable: true, ElementType: &dl},
		{Name: "i_arr", Type: parquet.TypeArray, Nullable: true, ElementType: &el},
	}}
	if err := db.CreateTable(ctx, "ja", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("ja", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"a": int64(1)}, {"a": int64(2)}, {"a": int64(2)}, {"a": int64(3)}, {"a": int64(4)},
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

	for _, c := range []struct {
		sql  string
		oids string // the RowDescription's type OIDs
		rows string // the text-format rows
	}{
		{`SELECT ARRAY(SELECT a FROM ja WHERE a > 1 ORDER BY a) AS v`, "1016", "{2,2,3,4}"},
		{`SELECT ARRAY(SELECT a FROM ja WHERE a > 9) AS v`, "1016", "{}"},
		{`SELECT current_schemas(true) AS v`, "1009", "{pg_catalog,public}"},
		{`SELECT format_type(atttypid, atttypmod) AS t, atttypmod AS m FROM pg_attribute ` +
			`WHERE attrelid = 'ja'::regclass AND attname = 'd_arr'`, "25,23", "numeric(10,2)[]|655366"},
		{`SELECT * FROM information_schema.key_column_usage WHERE table_name = 'absent'`,
			"", ""}, // the information_schema domains are carried as their base types (postgres-differences)
		{`SELECT * FROM pg_catalog.pg_type WHERE false`, "", ""},
	} {
		mrr := conn.PgConn().Exec(ctx, c.sql)
		var oids, rows []string
		for mrr.NextResult() {
			rr := mrr.ResultReader()
			for rr.NextRow() {
				var vals []string
				for _, v := range rr.Values() {
					vals = append(vals, string(v))
				}
				rows = append(rows, strings.Join(vals, "|"))
			}
			for _, fd := range rr.FieldDescriptions() {
				oids = append(oids, fmt.Sprint(fd.DataTypeOID))
			}
			if _, err := rr.Close(); err != nil {
				t.Errorf("%s: %v", c.sql, err)
			}
		}
		if err := mrr.Close(); err != nil {
			t.Errorf("%s: %v", c.sql, err)
			continue
		}
		if len(oids) == 0 {
			t.Errorf("%s: no columns declared; a zero-row relation keeps its column definitions", c.sql)
			continue
		}
		if c.oids != "" && strings.Join(oids, ",") != c.oids {
			t.Errorf("%s: OIDs %s, PostgreSQL 17.11 declares %s", c.sql, strings.Join(oids, ","), c.oids)
		}
		if got := strings.Join(rows, " "); got != c.rows {
			t.Errorf("%s: rows %q, PostgreSQL 17.11 sends %q", c.sql, got, c.rows)
		}
	}
}
