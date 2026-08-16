package physical

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// rgMetaScanFixture builds a MemStore-backed catalog with two multi-RG
// parquet files whose id ranges are disjoint per row group, so predicate
// pruning decisions are deterministic and observable in the emitted units.
func rgMetaScanFixture(t *testing.T) (*catalog.Catalog, []catalog.FileEntry) {
	t.Helper()
	ctx := context.Background()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeString},
	}}
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	if err := cat.CreateTable(ctx, "items", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}

	var entries []catalog.FileEntry
	for fi := 0; fi < 2; fi++ {
		// 3 row groups per file: ids [base, base+100), [base+100, base+200), [base+200, base+300)
		base := fi * 1000
		data := writeTestParquetMultiRG(t, schema,
			testRows(100, base), testRows(100, base+100), testRows(100, base+200))
		path := fmt.Sprintf("tables/items/chunk_%04d.parquet", fi)
		if _, err := store.Put(ctx, cat.Bucket(), path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatalf("put file: %v", err)
		}
		entry := catalog.FileEntry{Path: path, SizeBytes: int64(len(data)), NumRows: 300, CreatedAt: time.Now()}
		if err := cat.AddFiles(ctx, "items", map[string]string{}, "tables/items/", []catalog.FileEntry{entry}); err != nil {
			t.Fatalf("add files: %v", err)
		}
		entries = append(entries, entry)
	}
	return cat, entries
}

// unitKey is the observable identity of an emitted rgUnit for parity checks.
type unitKey struct {
	path      string
	rgIndex   int
	rowOffset int64
	numRows   int64
}

func collectUnits(inner *scanSourceInner) []unitKey {
	out := make([]unitKey, 0, len(inner.rgUnits))
	for _, u := range inner.rgUnits {
		out = append(out, unitKey{
			path:      u.slot.entry.Path,
			rgIndex:   u.rgIndex,
			rowOffset: u.rgRowOffset,
			numRows:   u.numRows,
		})
	}
	return out
}

// TestBuildRGUnitsRGMetaParity verifies the catalog-blob fast path emits
// EXACTLY the units the footer-read path emits — same row groups kept,
// same offsets, same pruning decisions — across no-predicate, pruning,
// and prune-everything shapes.
func TestBuildRGUnitsRGMetaParity(t *testing.T) {
	ctx := context.Background()
	cat, entries := rgMetaScanFixture(t)

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeString},
	}

	cases := []struct {
		name      string
		preds     []scanPredicate
		wantUnits int
	}{
		// 2 files × 3 RGs, no pruning.
		{"no predicates", nil, 6},
		// id > 1150 keeps file1 rg1 (1100-1199) and rg2 (1200-1299) —
		// prunes all of file0 and file1 rg0.
		{"range prune", []scanPredicate{{Column: "id", Op: ">", Value: int64(1150)}}, 2},
		// id = 5000 matches nothing: every row group pruned.
		{"prune all", []scanPredicate{{Column: "id", Op: "=", Value: int64(5000)}}, 0},
	}

	build := func(preds []scanPredicate) *scanSourceInner {
		inner := &scanSourceInner{
			cat:       cat,
			tableName: "items",
			files:     entries,
			schema:    schema,
			scanPreds: preds,
		}
		inner.buildRGUnits(ctx)
		if inner.failedFiles > 0 {
			t.Fatalf("buildRGUnits failed files: %d (%v)", inner.failedFiles, inner.firstFileErr)
		}
		return inner
	}

	// Footer-path baseline (no blob exists yet).
	baselines := make(map[string][]unitKey)
	for _, tc := range cases {
		units := collectUnits(build(tc.preds))
		if len(units) != tc.wantUnits {
			t.Fatalf("footer path %s: %d units, want %d (%v)", tc.name, len(units), tc.wantUnits, units)
		}
		baselines[tc.name] = units
	}

	// ANALYZE persists the RG-metadata blob.
	if _, err := cat.AnalyzeTable(ctx, "items"); err != nil {
		t.Fatalf("AnalyzeTable: %v", err)
	}
	if m, err := cat.TableRGMeta(ctx, "items"); err != nil || len(m) != 2 {
		t.Fatalf("TableRGMeta: %d files (err %v), want 2", len(m), err)
	}

	// Deleting the parquet objects proves the blob path never touches
	// them during planning: buildRGUnits must still enumerate and prune
	// identically with the data files gone.
	for _, e := range entries {
		if err := cat.Store().Delete(ctx, cat.Bucket(), e.Path); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range cases {
		units := collectUnits(build(tc.preds))
		if len(units) != len(baselines[tc.name]) {
			t.Fatalf("blob path %s: %d units, want %d", tc.name, len(units), len(baselines[tc.name]))
		}
		for i, u := range units {
			if u != baselines[tc.name][i] {
				t.Errorf("blob path %s unit %d: %+v != footer %+v", tc.name, i, u, baselines[tc.name][i])
			}
		}
	}
}

// TestBuildRGUnitsRGMetaPartialCoverage verifies files added after
// ANALYZE (not in the blob) fall back to footer reads while covered
// files stay network-free — the mixed-coverage contract.
func TestBuildRGUnitsRGMetaPartialCoverage(t *testing.T) {
	ctx := context.Background()
	cat, entries := rgMetaScanFixture(t)

	if _, err := cat.AnalyzeTable(ctx, "items"); err != nil {
		t.Fatalf("AnalyzeTable: %v", err)
	}

	// A third file lands after ANALYZE — no blob entry.
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeString},
	}}
	data := writeTestParquetMultiRG(t, schema, testRows(50, 5000))
	path := "tables/items/chunk_late.parquet"
	if _, err := cat.Store().Put(ctx, cat.Bucket(), path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	late := catalog.FileEntry{Path: path, SizeBytes: int64(len(data)), NumRows: 50, CreatedAt: time.Now()}
	if err := cat.AddFiles(ctx, "items", map[string]string{}, "tables/items/", []catalog.FileEntry{late}); err != nil {
		t.Fatal(err)
	}

	// Covered files' parquet objects deleted: only the late file may be
	// touched during planning.
	for _, e := range entries {
		if err := cat.Store().Delete(ctx, cat.Bucket(), e.Path); err != nil {
			t.Fatal(err)
		}
	}

	inner := &scanSourceInner{
		cat:       cat,
		tableName: "items",
		files:     append(append([]catalog.FileEntry{}, entries...), late),
		schema:    schema.Columns,
	}
	inner.buildRGUnits(ctx)
	if inner.failedFiles > 0 {
		t.Fatalf("failed files: %d (%v)", inner.failedFiles, inner.firstFileErr)
	}
	// 2 covered files × 3 RGs from the blob + 1 late file × 1 RG via footer.
	if len(inner.rgUnits) != 7 {
		t.Fatalf("units = %d, want 7", len(inner.rgUnits))
	}
	lateUnits := 0
	for _, u := range inner.rgUnits {
		if u.slot.entry.Path == path {
			lateUnits++
			if u.numRows != 50 {
				t.Errorf("late unit numRows = %d, want 50", u.numRows)
			}
		}
	}
	if lateUnits != 1 {
		t.Errorf("late file units = %d, want 1", lateUnits)
	}
}
