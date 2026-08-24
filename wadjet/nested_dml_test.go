package wadjet

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Regression test for #448/#449's F1/F2 hole in the DML path: ReadFileColumnar
// (internal/engine/scan/columnar.go), the entry point scanFileForDeletes and
// readParquetFile use to read a whole file for DELETE/UPDATE, called
// ReadRowGroupNative with the table's FULL schema and no projection.
// HasUnsupportedColumnarTypes — the guard #448 added to ReadRowGroupNative —
// refuses any schema with an Array/Map column, or a ROW whose field is
// itself a container, and ReadFileColumnar had no fallback for that refusal:
//
//   - DELETE on such a table errored outright ("native reader does not
//     support ..."), where it used to succeed (silently) on main.
//   - UPDATE errored too, for the same reason: readParquetFile is the same
//     entry point executeUpdate boxes every row through (RowAt) before
//     re-ingesting matched-and-unmatched rows alike.
//
// Erroring is directionally right — main's silent alternative was to answer
// the nested column as all-NULL and then re-ingest THAT — but PostgreSQL can
// UPDATE and DELETE a table with a composite column, so wadjet must too, and
// it must preserve the untouched columns' values exactly. ReadFileColumnar's
// fix falls back to the row reader for these schemas, matching what
// ReadFileBatches already does for the same shapes.
func TestDeleteAndUpdateOnRowOfContainerColumn(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "status", Type: parquet.TypeString},
		// c_rownest ROW{s: ROW{x}, l: ARRAY<STRING>} — a ROW whose fields are
		// themselves containers, the exact shape HasUnsupportedColumnarTypes
		// refuses (#448).
		{Name: "c_rownest", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "s", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
				{Name: "x", Type: parquet.TypeInt64, Nullable: true},
			}},
			{Name: "l", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		}},
	}}
	rows := func() []map[string]any {
		return []map[string]any{
			{"id": int64(0), "status": "new", "c_rownest": map[string]any{"s": map[string]any{"x": int64(100)}, "l": []any{"a0", "b0"}}},
			{"id": int64(1), "status": "new", "c_rownest": map[string]any{"s": map[string]any{"x": int64(101)}, "l": []any{"a1"}}},
			{"id": int64(2), "status": "new", "c_rownest": map[string]any{"s": map[string]any{"x": int64(102)}, "l": []any{}}},
			{"id": int64(3), "status": "new", "c_rownest": map[string]any{"s": map[string]any{"x": int64(103)}, "l": []any{"a3", "b3", "c3"}}},
		}
	}

	t.Run("delete", func(t *testing.T) {
		db, table := nestDMLOpen(t, "row_del", schema, rows())

		res, err := db.Execute(context.Background(), "DELETE FROM "+table+" WHERE id = 3")
		if err != nil {
			t.Fatalf("DELETE on a table with a ROW-of-container column: %v (#448/#449)", err)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("RowsAffected = %d, want 1", res.RowsAffected)
		}

		q, err := db.Query(context.Background(), "SELECT id, c_rownest FROM "+table+" ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		if len(q.Rows) != 3 {
			t.Fatalf("got %d rows after DELETE, want 3: %v", len(q.Rows), q.Rows)
		}
		want := rows()
		for _, r := range q.Rows {
			id := r["id"].(int64)
			if id == 3 {
				t.Fatal("row id=3 survived the DELETE")
			}
			if !reflect.DeepEqual(r["c_rownest"], want[id]["c_rownest"]) {
				t.Errorf("id=%d c_rownest = %#v, want %#v (surviving row's nested value must be untouched)",
					id, r["c_rownest"], want[id]["c_rownest"])
			}
		}
	})

	t.Run("update_preserves_nested", func(t *testing.T) {
		db, table := nestDMLOpen(t, "row_upd", schema, rows())

		// A scalar UPDATE that never names c_rownest must not disturb it —
		// on the pre-fix ReadFileColumnar, readParquetFile refused the whole
		// file (loud) or, on main pre-#448, answered it as all-NULL and
		// re-ingested that (silent data loss) for EVERY row, not just the
		// matched one.
		res, err := db.Execute(context.Background(), "UPDATE "+table+" SET status = 'archived' WHERE id = 1")
		if err != nil {
			t.Fatalf("UPDATE on a table with a ROW-of-container column: %v (#448/#449)", err)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("RowsAffected = %d, want 1", res.RowsAffected)
		}

		q, err := db.Query(context.Background(), "SELECT id, status, c_rownest FROM "+table+" ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		if len(q.Rows) != 4 {
			t.Fatalf("got %d rows, want 4: %v", len(q.Rows), q.Rows)
		}
		want := rows()
		for _, r := range q.Rows {
			id := r["id"].(int64)
			wantStatus := "new"
			if id == 1 {
				wantStatus = "archived"
			}
			if r["status"].(string) != wantStatus {
				t.Errorf("id=%d status = %q, want %q", id, r["status"], wantStatus)
			}
			if !reflect.DeepEqual(r["c_rownest"], want[id]["c_rownest"]) {
				t.Errorf("id=%d c_rownest = %#v, want %#v (nested column must survive an unrelated scalar UPDATE)",
					id, r["c_rownest"], want[id]["c_rownest"])
			}
		}
	})
}

// Same defect, the other shape: #448/#449's F1 gap in ReadFileColumnar is
// also the PRE-EXISTING hole for a table with a top-level ARRAY/MAP column
// (DELETE fails on this shape on main too, not just on this branch's new ROW
// guard).
func TestDeleteAndUpdateOnTopLevelArrayAndMapColumns(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "status", Type: parquet.TypeString},
		{Name: "tags", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		{Name: "attrs", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64, Nullable: true},
			}}},
	}}
	rows := func() []map[string]any {
		return []map[string]any{
			{"id": int64(0), "status": "new", "tags": []any{"x0"}, "attrs": map[string]any{"k0": int64(10)}},
			{"id": int64(1), "status": "new", "tags": []any{"x1a", "x1b"}, "attrs": map[string]any{"k1": int64(11)}},
			{"id": int64(2), "status": "new", "tags": []any{}, "attrs": map[string]any{}},
			{"id": int64(3), "status": "new", "tags": []any{"x3"}, "attrs": map[string]any{"k3": int64(13)}},
		}
	}
	// attrs comes back as a key-sorted []any of {"key", "value"} entries —
	// MAP is ARRAY(ROW(key,value)) in the storage layer, and a Go map has no
	// order of its own to compare against (see mbWantMap in map_column_test.go).
	wantAttrs := func(m map[string]any) any {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([]any, 0, len(keys))
		for _, k := range keys {
			out = append(out, map[string]any{"key": k, "value": m[k]})
		}
		return out
	}

	t.Run("delete", func(t *testing.T) {
		db, table := nestDMLOpen(t, "toplevel_del", schema, rows())

		res, err := db.Execute(context.Background(), "DELETE FROM "+table+" WHERE id = 3")
		if err != nil {
			t.Fatalf("DELETE on a table with top-level ARRAY/MAP columns: %v (#448/#449 F1)", err)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("RowsAffected = %d, want 1", res.RowsAffected)
		}

		q, err := db.Query(context.Background(), "SELECT id, tags, attrs FROM "+table+" ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		if len(q.Rows) != 3 {
			t.Fatalf("got %d rows after DELETE, want 3: %v", len(q.Rows), q.Rows)
		}
		want := rows()
		for _, r := range q.Rows {
			id := r["id"].(int64)
			if id == 3 {
				t.Fatal("row id=3 survived the DELETE")
			}
			if !reflect.DeepEqual(r["tags"], want[id]["tags"]) {
				t.Errorf("id=%d tags = %#v, want %#v", id, r["tags"], want[id]["tags"])
			}
			if !reflect.DeepEqual(r["attrs"], wantAttrs(want[id]["attrs"].(map[string]any))) {
				t.Errorf("id=%d attrs = %#v, want %#v", id, r["attrs"], wantAttrs(want[id]["attrs"].(map[string]any)))
			}
		}
	})

	t.Run("update_preserves_nested", func(t *testing.T) {
		db, table := nestDMLOpen(t, "toplevel_upd", schema, rows())

		res, err := db.Execute(context.Background(), "UPDATE "+table+" SET status = 'archived' WHERE id = 1")
		if err != nil {
			t.Fatalf("UPDATE on a table with top-level ARRAY/MAP columns: %v (#448/#449 F1)", err)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("RowsAffected = %d, want 1", res.RowsAffected)
		}

		q, err := db.Query(context.Background(), "SELECT id, status, tags, attrs FROM "+table+" ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		if len(q.Rows) != 4 {
			t.Fatalf("got %d rows, want 4: %v", len(q.Rows), q.Rows)
		}
		want := rows()
		for _, r := range q.Rows {
			id := r["id"].(int64)
			wantStatus := "new"
			if id == 1 {
				wantStatus = "archived"
			}
			if r["status"].(string) != wantStatus {
				t.Errorf("id=%d status = %q, want %q", id, r["status"], wantStatus)
			}
			if !reflect.DeepEqual(r["tags"], want[id]["tags"]) {
				t.Errorf("id=%d tags = %#v, want %#v (must survive an unrelated scalar UPDATE)", id, r["tags"], want[id]["tags"])
			}
			if !reflect.DeepEqual(r["attrs"], wantAttrs(want[id]["attrs"].(map[string]any))) {
				t.Errorf("id=%d attrs = %#v, want %#v (must survive an unrelated scalar UPDATE)",
					id, r["attrs"], wantAttrs(want[id]["attrs"].(map[string]any)))
			}
		}
	})
}

// nestDMLOpen opens a fresh in-memory DB, creates tableBase+"_"+t.Name()'s
// table (a stable, collision-free name per subtest) with schema, ingests
// rows as a single flushed file, and returns the DB and table name.
func nestDMLOpen(t *testing.T, tableBase string, schema parquet.Schema, rows []map[string]any) (*DB, string) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	table := "nest_" + tableBase
	if err := db.CreateTable(ctx, table, schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester(table, schema, nil, ingest.DefaultConfig())
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db, table
}
