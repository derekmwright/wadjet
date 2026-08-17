package test

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Session information functions (current_user, current_schema,
// current_database, ...) are what a PostgreSQL client asks for before it asks
// for anything else. DataGrip 2026.1.3 opens a connection with
//
//	select current_database(), current_schema(), current_user
//
// which failed at "unknown column current_user" — the bare spelling had no
// niladic function behind it — and the pgwire layer then answered the
// three-column statement with a one-column synthetic row.

func TestSessionFunctions(t *testing.T) {
	ctx, db := openTestDB(t)

	tests := []struct {
		sql     string
		wantCol string
		wantVal string
	}{
		{"SELECT current_user", "current_user", "wadjet"},
		{"SELECT CURRENT_USER", "current_user", "wadjet"},
		{"SELECT current_user()", "current_user", "wadjet"},
		{"SELECT session_user", "session_user", "wadjet"},
		{"SELECT user", "user", "wadjet"},
		{"SELECT current_role", "current_role", "wadjet"},
		{"SELECT current_catalog", "current_catalog", "wadjet"},
		{"SELECT current_database()", "current_database", "wadjet"},
		{"SELECT current_schema", "current_schema", "public"},
		{"SELECT current_schema()", "current_schema", "public"},
		{"SELECT current_schemas(false)", "current_schemas", "{public}"},
		{"SELECT current_schemas(true)", "current_schemas", "{pg_catalog,public}"},
		{"SELECT version()", "version", "PostgreSQL 15.0 (Wadjet analytical query engine)"},
		{"SELECT version() AS v", "v", "PostgreSQL 15.0 (Wadjet analytical query engine)"},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			rows := checkedRows(t, ctx, db, tt.sql)
			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rows))
			}
			got, ok := rows[0][tt.wantCol]
			if !ok {
				t.Fatalf("column %q missing; row = %v", tt.wantCol, rows[0])
			}
			if s, _ := got.(string); s != tt.wantVal {
				t.Errorf("value = %v, want %q", got, tt.wantVal)
			}
		})
	}
}

// TestSessionFunctionsDataGripOpeningQuery is the reported statement, executed
// end to end through the embedded API: three columns, three values.
func TestSessionFunctionsDataGripOpeningQuery(t *testing.T) {
	ctx, db := openTestDB(t)

	res, err := db.Query(ctx, "select current_database(), current_schema(), current_user")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	want := []string{"current_database", "current_schema", "current_user"}
	if len(res.Columns) != 3 {
		t.Fatalf("columns = %v, want %v", res.Columns, want)
	}
	for i, w := range want {
		if res.Columns[i] != w {
			t.Errorf("column %d = %q, want %q", i, res.Columns[i], w)
		}
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}
	row := res.Rows[0]
	for col, want := range map[string]string{
		"current_database": "wadjet",
		"current_schema":   "public",
		"current_user":     "wadjet",
	} {
		if got, _ := row[col].(string); got != want {
			t.Errorf("%s = %v, want %q", col, row[col], want)
		}
	}
}

// TestSessionFunctionsWithRealColumns mixes a session function into an
// ordinary query: it has to survive projection alongside scanned columns and
// inside a WHERE clause.
func TestSessionFunctionsWithRealColumns(t *testing.T) {
	ctx, db := openTestDB(t)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "owner", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "assets", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("assets", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int32(1), "owner": "wadjet"},
		{"id": int32(2), "owner": "someone_else"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	rows := checkedRows(t, ctx, db, "SELECT id, owner, current_user FROM assets ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	for i, row := range rows {
		if got, _ := row["current_user"].(string); got != "wadjet" {
			t.Errorf("row %d current_user = %v, want wadjet", i, row["current_user"])
		}
	}

	rows = checkedRows(t, ctx, db, "SELECT id FROM assets WHERE owner = current_user")
	if len(rows) != 1 {
		t.Fatalf("filter on current_user: got %d rows, want 1", len(rows))
	}
}

// TestQuotedSessionNameIsAColumn pins #304 semantics against the new niladic
// spellings: double-quoted, the name is a column reference, so a table with a
// column literally called `user` stays reachable — and a quoted name that no
// table has still reports unknown column rather than silently answering
// "wadjet".
func TestQuotedSessionNameIsAColumn(t *testing.T) {
	ctx, db := openTestDB(t)

	if _, err := db.Query(ctx, `SELECT "current_user"`); err == nil {
		t.Fatal(`SELECT "current_user" resolved; want an unknown-column error`)
	} else if !strings.Contains(err.Error(), "current_user") {
		t.Errorf("error does not name the column: %v", err)
	}

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "user", Type: parquet.TypeString},
		{Name: "action", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "audit", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("audit", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"user": "alice", "action": "login"},
		{"user": "bob", "action": "logout"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	rows := checkedRows(t, ctx, db, `SELECT "user", action FROM audit ORDER BY "user"`)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if got, _ := rows[0]["user"].(string); got != "alice" {
		t.Errorf(`quoted "user" column = %v, want alice`, rows[0]["user"])
	}
}
