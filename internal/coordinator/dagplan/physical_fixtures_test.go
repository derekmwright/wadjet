// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Fixtures the moved stage-planning tests need, copied from
// internal/planner/physical's own test files rather than shared.
//
// A test helper cannot be imported across packages — a _test.go file is not
// part of the importable package — so the only alternatives were a third
// support package that both import, or this. The fixture is a catalog with
// rows in it; it is compared against nothing, so a drift between the two
// costs a different plan shape here, never a wrong assertion about one.

func ScanCacheFixture(t *testing.T, rows int) *catalog.Catalog {
	t.Helper()
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "id2", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}}
	rowMaps := make([]map[string]any, rows)
	for i := range rowMaps {
		rowMaps[i] = map[string]any{"id": int64(i), "id2": int64(i + 10), "val": "v"}
	}
	data := writeTestParquetMultiRG(t, schema, rowMaps)

	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable(ctx, "items", schema, nil); err != nil {
		t.Fatal(err)
	}
	path := "tables/items/chunk_001.parquet"
	if _, err := store.Put(ctx, cat.Bucket(), path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	entry := catalog.FileEntry{Path: path, SizeBytes: int64(len(data)), NumRows: int64(rows), CreatedAt: time.Now()}
	if err := cat.AddFiles(ctx, "items", map[string]string{}, "tables/items/", []catalog.FileEntry{entry}); err != nil {
		t.Fatal(err)
	}
	return cat
}

func writeTestParquetMultiRG(t *testing.T, schema parquet.Schema, rowSets ...[]map[string]any) []byte {
	t.Helper()

	cfg := parquet.DefaultWriterConfig()
	cfg.Compression = parquet.CompressionNone
	// Set small row group size so each rowSet becomes its own row group.
	if len(rowSets) > 0 {
		cfg.RowGroupSize = len(rowSets[0])
	}

	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, rows := range rowSets {
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
