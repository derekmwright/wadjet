package compaction

import (
	"context"
	"fmt"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Compaction is the one job in this system that is read → write over a whole
// table, and it DELETES its inputs. So every way a file's schema can disagree
// with the catalog's is a way for compaction to rewrite the table into
// something else — and TestCompactTable_FileSchemaDisagreesWithTheCatalog
// covered exactly one such pairing (an admissible one, asserted to succeed).
//
// This is the rest of the matrix, end to end through Compactor.CompactTable.

// evoBuild writes `files` parquet files under fileSchema into a table whose
// CATALOG schema is tableSchema, then compacts. It returns the catalog, the
// store, the file count after compaction, and the error CompactTable gave.
func evoBuild(t *testing.T, table string, tableSchema, fileSchema parquet.Schema,
	rowsFor func(file int) []map[string]any, files int) (*catalog.Catalog, objstore.Store, int, error) {
	t.Helper()
	ctx := context.Background()
	cat, store := setupTestCatalog(t)
	if err := cat.CreateTable(ctx, table, tableSchema, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < files; i++ {
		rows := rowsFor(i)
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", table, i)
		size := writeTestFile(t, store, "test-bucket", path, fileSchema, rows)
		if err := cat.AddFiles(ctx, table, nil, "", []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: int64(len(rows)), CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}
	cfg := DefaultConfig()
	cfg.MinFiles = 2
	_, err := New(cat, nil, cfg).CompactTable(ctx, table)
	m, merr := cat.GetManifest(ctx, table)
	if merr != nil {
		t.Fatal(merr)
	}
	n := 0
	for _, p := range m.Partitions {
		n += len(p.Files)
	}
	return cat, store, n, err
}

func evoRows(t *testing.T, cat *catalog.Catalog, store objstore.Store, table string,
	schema parquet.Schema) []map[string]any {
	t.Helper()
	rows := readTableRows(t, cat, store, table, schema, false)
	sort.Slice(rows, func(i, j int) bool { return idOf(rows[i]) < idOf(rows[j]) })
	return rows
}

// #441: the file's column is `Name` and the catalog's is `name`.
//
// filterSchemaColumns keyed on the exact name while retypeFromCatalog folded,
// so the column was dropped from the projection before the retype could see
// it: absent from every row map, and NativeWriter writes an absent key as
// NULL. Through compaction that is a whole column destroyed with its inputs
// deleted — the same shape as #428, one door over.
//
// Reachable for any file from a writer that preserves the author's
// capitalisation (pyarrow, parquet-mr, Spark) registered against a wadjet
// table, since wadjet's SQL identifiers are lower case.
func TestCompactTable_ColumnNameCaseDiffersFromTheCatalog(t *testing.T) {
	tableSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "amount", Type: parquet.TypeInt64, Nullable: true},
	}}
	fileSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "ID", Type: parquet.TypeInt64},
		{Name: "Name", Type: parquet.TypeString, Nullable: true},
		{Name: "AMOUNT", Type: parquet.TypeInt64, Nullable: true},
	}}
	cat, store, n, err := evoBuild(t, "casemix", tableSchema, fileSchema, func(f int) []map[string]any {
		return []map[string]any{
			{"ID": int64(f*2 + 0), "Name": fmt.Sprintf("r%d", f*2+0), "AMOUNT": int64(100 + f*2)},
			{"ID": int64(f*2 + 1), "Name": fmt.Sprintf("r%d", f*2+1), "AMOUNT": int64(101 + f*2)},
		}
	}, 3)
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if n != 1 {
		t.Fatalf("after compaction the table has %d files, want 1 — the merge did not run", n)
	}
	rows := evoRows(t, cat, store, "casemix", tableSchema)
	if len(rows) != 6 {
		t.Fatalf("got %d rows, want 6", len(rows))
	}
	for i, r := range rows {
		if want := fmt.Sprintf("r%d", i); r["name"] != want {
			t.Errorf("row %d: name = %#v, want %q — the column did not survive compaction", i, r["name"], want)
		}
		if want := int64(100 + i); r["amount"] != want {
			t.Errorf("row %d: amount = %#v, want %d", i, r["amount"], want)
		}
	}
}

// The same rule at the reader, on both read paths and for a projection: a
// column the file spells differently is the SAME column, answered under the
// name the caller asked for.
func TestReaderMatchesColumnNamesCaseInsensitively(t *testing.T) {
	fileSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "ID", Type: parquet.TypeInt64},
		{Name: "Name", Type: parquet.TypeString, Nullable: true},
	}}
	catalogCols := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}
	_, store := setupTestCatalog(t)
	writeTestFile(t, store, "test-bucket", "tables/case/chunk_0000.parquet", fileSchema, []map[string]any{
		{"ID": int64(0), "Name": "a"},
		{"ID": int64(1), "Name": "b"},
	})
	rc, _, err := store.Get(context.Background(), "test-bucket", "tables/case/chunk_0000.parquet")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}

	r, err := parquet.NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		what string
		run  func() ([]map[string]any, error)
	}{
		{"ReadRowsAs", func() ([]map[string]any, error) { return r.ReadRowsAs(catalogCols, nil) }},
		{"ReadRowsAs/projected", func() ([]map[string]any, error) {
			return r.ReadRowsAs(catalogCols, []string{"name"})
		}},
		{"ReadRowGroupAs", func() ([]map[string]any, error) {
			return r.ReadRowGroupAs(0, catalogCols, []string{"id", "name"})
		}},
	} {
		rows, err := tc.run()
		if err != nil {
			t.Errorf("%s: %v", tc.what, err)
			continue
		}
		if len(rows) != 2 {
			t.Errorf("%s: %d rows, want 2", tc.what, len(rows))
			continue
		}
		for i, want := range []string{"a", "b"} {
			if rows[i]["name"] != want {
				t.Errorf("%s row %d: name = %#v, want %q (keys %v)", tc.what, i, rows[i]["name"], want, keysOf(rows[i]))
			}
		}
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
