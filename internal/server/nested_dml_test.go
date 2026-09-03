package server

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Server-path regression test for #448/#449's F1 hole, mirroring
// wadjet/nested_dml_test.go but through executeDMLDelete/executeDMLUpdate —
// the server's own DML executors, which both read a whole file for
// DELETE/UPDATE via readDMLFile (server.go:815) -> scan.ReadFileColumnar,
// exactly like the public API's scanFileForDeletes/readParquetFile do.
//
// See wadjet/nested_dml_test.go's doc comment for the full defect history:
// ReadFileColumnar called ReadRowGroupNative with no projection guard, which
// HasUnsupportedColumnarTypes (#448) refuses for a ROW-of-container column or
// a top-level ARRAY/MAP column, breaking DELETE and UPDATE on such tables.
func TestServerDeleteAndUpdateOnRowOfContainerColumn(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "status", Type: parquet.TypeString},
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
		ctx := context.Background()
		cat, filePath := nestServerDMLSetup(t, "srv_row_del", schema, rows())

		info := parseDMLOrFatal(t, "DELETE FROM srv_row_del WHERE id = 3").Delete
		res, err := executeDMLDelete(ctx, cat, info)
		if err != nil {
			t.Fatalf("executeDMLDelete on a table with a ROW-of-container column: %v (#448/#449)", err)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("rowsAffected = %d, want 1", res.RowsAffected)
		}

		surviving := nestServerSurvivingRows(t, cat, "srv_row_del", filePath, schema)
		if len(surviving) != 3 {
			t.Fatalf("got %d surviving rows, want 3: %v", len(surviving), surviving)
		}
		want := rows()
		for _, r := range surviving {
			id := r["id"].(int64)
			if id == 3 {
				t.Fatal("row id=3 survived the DELETE")
			}
			if !reflect.DeepEqual(r["c_rownest"], want[id]["c_rownest"]) {
				t.Errorf("id=%d c_rownest = %#v, want %#v", id, r["c_rownest"], want[id]["c_rownest"])
			}
		}
	})

	t.Run("update_preserves_nested", func(t *testing.T) {
		ctx := context.Background()
		cat, filePath := nestServerDMLSetup(t, "srv_row_upd", schema, rows())

		info := parseDMLOrFatal(t, "UPDATE srv_row_upd SET status = 'archived' WHERE id = 1").Update
		res, err := executeDMLUpdate(ctx, cat, info)
		if err != nil {
			t.Fatalf("executeDMLUpdate on a table with a ROW-of-container column: %v (#448/#449)", err)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("rowsAffected = %d, want 1", res.RowsAffected)
		}

		want := rows()
		all := nestServerAllRowsAfterUpdate(t, cat, "srv_row_upd", filePath, schema)
		if len(all) != 4 {
			t.Fatalf("got %d rows, want 4: %v", len(all), all)
		}
		for _, r := range all {
			id := r["id"].(int64)
			wantStatus := "new"
			if id == 1 {
				wantStatus = "archived"
			}
			if r["status"].(string) != wantStatus {
				t.Errorf("id=%d status = %q, want %q", id, r["status"], wantStatus)
			}
			if !reflect.DeepEqual(r["c_rownest"], want[id]["c_rownest"]) {
				t.Errorf("id=%d c_rownest = %#v, want %#v (must survive an unrelated scalar UPDATE)",
					id, r["c_rownest"], want[id]["c_rownest"])
			}
		}
	})
}

// Same defect through the server path, the top-level-container shape: F1 is
// also the pre-existing hole for a table with top-level ARRAY/MAP columns.
func TestServerDeleteAndUpdateOnTopLevelArrayAndMapColumns(t *testing.T) {
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
		ctx := context.Background()
		cat, filePath := nestServerDMLSetup(t, "srv_toplevel_del", schema, rows())

		info := parseDMLOrFatal(t, "DELETE FROM srv_toplevel_del WHERE id = 3").Delete
		res, err := executeDMLDelete(ctx, cat, info)
		if err != nil {
			t.Fatalf("executeDMLDelete on a table with top-level ARRAY/MAP columns: %v (#448/#449 F1)", err)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("rowsAffected = %d, want 1", res.RowsAffected)
		}

		surviving := nestServerSurvivingRows(t, cat, "srv_toplevel_del", filePath, schema)
		if len(surviving) != 3 {
			t.Fatalf("got %d surviving rows, want 3: %v", len(surviving), surviving)
		}
		want := rows()
		for _, r := range surviving {
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
		ctx := context.Background()
		cat, filePath := nestServerDMLSetup(t, "srv_toplevel_upd", schema, rows())

		info := parseDMLOrFatal(t, "UPDATE srv_toplevel_upd SET status = 'archived' WHERE id = 1").Update
		res, err := executeDMLUpdate(ctx, cat, info)
		if err != nil {
			t.Fatalf("executeDMLUpdate on a table with top-level ARRAY/MAP columns: %v (#448/#449 F1)", err)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("rowsAffected = %d, want 1", res.RowsAffected)
		}

		want := rows()
		all := nestServerAllRowsAfterUpdate(t, cat, "srv_toplevel_upd", filePath, schema)
		if len(all) != 4 {
			t.Fatalf("got %d rows, want 4: %v", len(all), all)
		}
		for _, r := range all {
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

// --- helpers ---------------------------------------------------------------

// parseDMLOrFatal parses sql via the same parser handleDML uses.
func parseDMLOrFatal(t *testing.T, sql string) *plansql.ParsedQuery {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parsing %q: %v", sql, err)
	}
	return parsed
}

// nestServerDMLSetup creates an in-memory catalog, a table named tableName
// with schema, and writes rows as a single parquet file registered in the
// manifest. Returns the catalog and that file's path.
func nestServerDMLSetup(t *testing.T, tableName string, schema parquet.Schema, rows []map[string]any) (*catalog.Catalog, string) {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable(ctx, tableName, schema, nil); err != nil {
		t.Fatalf("creating table: %v", err)
	}

	filePath := writeDMLTestParquetFile(t, ctx, store, cat, tableName, schema, rows, "chunk_0001.parquet")
	return cat, filePath
}

// writeDMLTestParquetFile writes rows to a new parquet file under
// tables/<tableName>/<name> and registers it in the manifest.
func writeDMLTestParquetFile(t *testing.T, ctx context.Context, store objstore.Store, cat *catalog.Catalog, tableName string, schema parquet.Schema, rows []map[string]any, name string) string {
	t.Helper()
	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	filePath := "tables/" + tableName + "/" + name
	if _, err := store.Put(ctx, "test", filePath, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", []catalog.FileEntry{{
		Path:      filePath,
		SizeBytes: int64(len(data)),
		NumRows:   int64(len(rows)),
		CreatedAt: time.Now(),
	}}); err != nil {
		t.Fatalf("adding file to manifest: %v", err)
	}
	return filePath
}

// nestServerSurvivingRows reads originalFile via readDMLFile (the exact
// entry point executeDMLDelete/executeDMLUpdate use) and applies the
// manifest's delete markers for it, returning the rows a query would still
// see.
func nestServerSurvivingRows(t *testing.T, cat *catalog.Catalog, tableName, originalFile string, schema parquet.Schema) []map[string]any {
	t.Helper()
	ctx := context.Background()
	b, err := readDMLFile(ctx, cat, originalFile, schema.Columns)
	if err != nil {
		t.Fatalf("readDMLFile: %v", err)
	}
	if b == nil {
		return nil
	}
	manifest, err := cat.GetManifest(ctx, tableName)
	if err != nil {
		t.Fatal(err)
	}
	deleted := map[int64]bool{}
	for _, m := range manifest.DeleteMarkers {
		if m.FilePath != originalFile {
			continue
		}
		for _, idx := range m.RowIndices {
			deleted[idx] = true
		}
	}
	var out []map[string]any
	for i := 0; i < b.Len; i++ {
		if deleted[int64(i)] {
			continue
		}
		out = append(out, b.RowAt(i))
	}
	return out
}

// nestServerAllRowsAfterUpdate reconstructs the post-UPDATE table content:
// the surviving (non-matched) rows of originalFile plus every row of every
// OTHER file the manifest now lists (the re-ingested, updated rows land in
// their own file — see executeDMLUpdate's per-file streaming).
func nestServerAllRowsAfterUpdate(t *testing.T, cat *catalog.Catalog, tableName, originalFile string, schema parquet.Schema) []map[string]any {
	t.Helper()
	ctx := context.Background()
	out := nestServerSurvivingRows(t, cat, tableName, originalFile, schema)

	manifest, err := cat.GetManifest(ctx, tableName)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			if f.Path == originalFile {
				continue
			}
			b, err := readDMLFile(ctx, cat, f.Path, schema.Columns)
			if err != nil {
				t.Fatalf("readDMLFile(%s): %v", f.Path, err)
			}
			if b == nil {
				continue
			}
			for i := 0; i < b.Len; i++ {
				out = append(out, b.RowAt(i))
			}
		}
	}
	return out
}
