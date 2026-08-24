package compaction

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestCompactTable_UnpartitionedTableOutputPath is a regression test for a
// bug where the compactor built the merged output path as
// "<part.Path>/compacted_X.parquet" — with `part.Path` empty for
// unpartitioned tables, the key began with "/" and the compacted file
// landed at the bucket root while the old chunks were deleted, orphaning
// the data (no catalog entry points to it). Repeated compactor passes
// silently emptied customer/part at SF10. The fix prepends the table
// prefix so unpartitioned output lands under tables/<name>/.
func TestCompactTable_UnpartitionedTableOutputPath(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}
	if err := cat.CreateTable(ctx, "customer", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Ingester convention for unpartitioned tables: files live directly at
	// tables/<name>/chunk_X.parquet and the PartitionEntry.Path is "".
	const partPath = ""
	var partValues map[string]string
	for i := 0; i < 15; i++ {
		path := fmt.Sprintf("tables/customer/chunk_%04d.parquet", i)
		size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{
			{"id": int64(i*10 + 1)},
			{"id": int64(i*10 + 2)},
		})
		if err := cat.AddFiles(ctx, "customer", partValues, partPath, []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: 2, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}

	cfg := DefaultConfig()
	cfg.MinFiles = 5
	c := New(cat, nil, cfg)
	if _, err := c.CompactTable(ctx, "customer"); err != nil {
		t.Fatal(err)
	}

	manifest, _ := cat.GetManifest(ctx, "customer")
	if len(manifest.Partitions[0].Files) != 1 {
		t.Fatalf("expected 1 merged file, got %d", len(manifest.Partitions[0].Files))
	}
	compactedPath := manifest.Partitions[0].Files[0].Path

	// Regression: must live under tables/customer/, not at the bucket root.
	if strings.HasPrefix(compactedPath, "/") {
		t.Fatalf("compacted file has leading slash — would land at bucket root: %q", compactedPath)
	}
	if !strings.HasPrefix(compactedPath, "tables/customer/") {
		t.Fatalf("compacted file not under tables/customer/: %q", compactedPath)
	}

	// Verify the data is actually reachable in the object store at that path.
	rc, _, err := store.Get(ctx, "test-bucket", compactedPath)
	if err != nil {
		t.Fatalf("compacted file not reachable at %q: %v", compactedPath, err)
	}
	rc.Close()
}

// TestCompactTable_MixedLegacyAndFullUUIDChunkNames is a #494 back-compat
// regression: a table compacted before the fix can carry old 8-char chunk
// names (chunk_a1b2c3d4.parquet) right alongside files ingested after it
// under the new full-UUIDv7 names, in the same partition. Compaction reads
// a manifest entry purely by its Path string — nothing parses or assumes a
// length or shape for it — so the two naming eras must merge together
// without incident.
func TestCompactTable_MixedLegacyAndFullUUIDChunkNames(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}
	if err := cat.CreateTable(ctx, "customer", schema, nil); err != nil {
		t.Fatal(err)
	}

	const partPath = ""
	legacyNames := []string{
		"tables/customer/chunk_a1b2c3d4.parquet",
		"tables/customer/chunk_deadbeef.parquet",
	}
	newNames := []string{
		"tables/customer/chunk_01a035f6-9ed5-726a-94d0-5bbcfddd79c0.parquet",
		"tables/customer/chunk_01a035f6-c2f1-7c3e-8a11-2f6e2f0b9a11.parquet",
		"tables/customer/chunk_01a035f6-e4a2-70a1-9b22-3a7f3f1c8b22.parquet",
	}
	allNames := append(append([]string{}, legacyNames...), newNames...)

	var wantRows int64
	for i, path := range allNames {
		size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{
			{"id": int64(i*10 + 1)},
			{"id": int64(i*10 + 2)},
		})
		if err := cat.AddFiles(ctx, "customer", nil, partPath, []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: 2, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
		wantRows += 2
	}

	cfg := DefaultConfig()
	cfg.MinFiles = 2
	c := New(cat, nil, cfg)
	if _, err := c.CompactTable(ctx, "customer"); err != nil {
		t.Fatalf("compacting a partition with mixed legacy/new chunk names: %v", err)
	}

	manifest, err := cat.GetManifest(ctx, "customer")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Partitions) != 1 || len(manifest.Partitions[0].Files) != 1 {
		t.Fatalf("expected 1 merged file, got %+v", manifest.Partitions)
	}
	merged := manifest.Partitions[0].Files[0]
	if merged.NumRows != wantRows {
		t.Fatalf("merged rows = %d, want %d (all legacy and new-named inputs)", merged.NumRows, wantRows)
	}
	if _, _, err := store.Get(ctx, "test-bucket", merged.Path); err != nil {
		t.Fatalf("merged file not reachable at %q: %v", merged.Path, err)
	}
}

func setupTestCatalog(t *testing.T) (*catalog.Catalog, objstore.Store) {
	t.Helper()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test-bucket")
	ctx := context.Background()
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	return cat, store
}

func writeTestFile(t *testing.T, store objstore.Store, bucket, path string, schema parquet.Schema, rows []map[string]any) int64 {
	t.Helper()
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	size := int64(buf.Len())
	_, err = store.Put(context.Background(), bucket, path, bytes.NewReader(buf.Bytes()), size, "application/octet-stream")
	if err != nil {
		t.Fatal(err)
	}
	return size
}

func TestCompactTable_MergesSmallFiles(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "name", Type: parquet.TypeString},
		},
	}
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Create 15 small files in one partition
	partValues := map[string]string{"region": "us"}
	partPath := "tables/events/region=us"
	for i := 0; i < 15; i++ {
		path := fmt.Sprintf("%s/chunk_%04d.parquet", partPath, i)
		rows := []map[string]any{
			{"id": int64(i*10 + 1), "name": fmt.Sprintf("row_%d_a", i)},
			{"id": int64(i*10 + 2), "name": fmt.Sprintf("row_%d_b", i)},
		}
		size := writeTestFile(t, store, "test-bucket", path, schema, rows)
		if err := cat.AddFiles(ctx, "events", partValues, partPath, []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: 2, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Verify pre-compaction state
	manifest, _ := cat.GetManifest(ctx, "events")
	if len(manifest.Partitions) != 1 || len(manifest.Partitions[0].Files) != 15 {
		t.Fatalf("expected 1 partition with 15 files, got %d partitions",
			len(manifest.Partitions))
	}

	// Run compaction
	cfg := DefaultConfig()
	cfg.MinFiles = 5 // lower threshold for test
	c := New(cat, nil, cfg)
	result, err := c.CompactTable(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}

	if result.PartitionsCompacted != 1 {
		t.Errorf("partitions compacted: got %d, want 1", result.PartitionsCompacted)
	}
	if result.FilesRemoved != 15 {
		t.Errorf("files removed: got %d, want 15", result.FilesRemoved)
	}
	if result.FilesCreated != 1 {
		t.Errorf("files created: got %d, want 1", result.FilesCreated)
	}
	if result.RowsMerged != 30 {
		t.Errorf("rows merged: got %d, want 30", result.RowsMerged)
	}

	// Verify post-compaction manifest
	manifest, _ = cat.GetManifest(ctx, "events")
	if len(manifest.Partitions[0].Files) != 1 {
		t.Fatalf("expected 1 file after compaction, got %d", len(manifest.Partitions[0].Files))
	}

	// Read back the compacted file and verify data
	compactedPath := manifest.Partitions[0].Files[0].Path
	rc, _, err := store.Get(ctx, "test-bucket", compactedPath)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := readAll(rc)
	rc.Close()

	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := reader.ReadRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 30 {
		t.Fatalf("compacted file: expected 30 rows, got %d", len(rows))
	}
}

func TestCompactTable_AppliesDeleteMarkers(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}
	if err := cat.CreateTable(ctx, "logs", schema, nil); err != nil {
		t.Fatal(err)
	}

	partValues := map[string]string{}
	partPath := "tables/logs/data"
	var allFiles []catalog.FileEntry
	for i := 0; i < 12; i++ {
		path := fmt.Sprintf("%s/chunk_%04d.parquet", partPath, i)
		rows := []map[string]any{
			{"id": int64(i*3 + 1)},
			{"id": int64(i*3 + 2)},
			{"id": int64(i*3 + 3)},
		}
		size := writeTestFile(t, store, "test-bucket", path, schema, rows)
		fe := catalog.FileEntry{Path: path, SizeBytes: size, NumRows: 3, CreatedAt: time.Now().UTC()}
		allFiles = append(allFiles, fe)
		if err := cat.AddFiles(ctx, "logs", partValues, partPath, []catalog.FileEntry{fe}); err != nil {
			t.Fatal(err)
		}
	}

	// Delete row 0 from file 0 and row 1 from file 5
	if err := cat.AddDeleteMarkers(ctx, "logs", []catalog.DeleteMarker{
		{FilePath: allFiles[0].Path, RowIndices: []int64{0}},
		{FilePath: allFiles[5].Path, RowIndices: []int64{1}},
	}); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.MinFiles = 5
	c := New(cat, nil, cfg)
	result, err := c.CompactTable(ctx, "logs")
	if err != nil {
		t.Fatal(err)
	}

	// 12 files × 3 rows = 36, minus 2 deleted = 34
	if result.RowsMerged != 34 {
		t.Errorf("rows merged: got %d, want 34", result.RowsMerged)
	}

	// Delete markers should be cleaned
	manifest, _ := cat.GetManifest(ctx, "logs")
	if len(manifest.DeleteMarkers) != 0 {
		t.Errorf("expected 0 delete markers after compaction, got %d", len(manifest.DeleteMarkers))
	}
}

// TestCompactTable_DeleteMarkersAcrossRowGroups: the merge reads one row
// group at a time (memory-bound fix for the 2026-08-10 coordinator OOM),
// and delete markers index rows FILE-wide — the per-group offset math
// must keep markers in later row groups landing on the right rows.
func TestCompactTable_DeleteMarkersAcrossRowGroups(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}
	if err := cat.CreateTable(ctx, "logs", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Two files, each with 3 row groups of 4 rows (RowGroupSize=4).
	partValues := map[string]string{}
	partPath := "tables/logs/data"
	var allFiles []catalog.FileEntry
	for i := 0; i < 2; i++ {
		path := fmt.Sprintf("%s/chunk_%04d.parquet", partPath, i)
		var rows []map[string]any
		for r := 0; r < 12; r++ {
			rows = append(rows, map[string]any{"id": int64(i*100 + r)})
		}
		var buf bytes.Buffer
		cfg := parquet.DefaultWriterConfig()
		cfg.RowGroupSize = 4
		w, err := parquet.NewWriter(&buf, schema, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		size := int64(buf.Len())
		if _, err := store.Put(ctx, "test-bucket", path, bytes.NewReader(buf.Bytes()), size, "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		fe := catalog.FileEntry{Path: path, SizeBytes: size, NumRows: 12, CreatedAt: time.Now().UTC()}
		allFiles = append(allFiles, fe)
		if err := cat.AddFiles(ctx, "logs", partValues, partPath, []catalog.FileEntry{fe}); err != nil {
			t.Fatal(err)
		}
	}

	// Markers in the first, middle, and last row group of file 0, and the
	// last row group of file 1: file-wide indices 0, 5, 11; and 100+8..11.
	if err := cat.AddDeleteMarkers(ctx, "logs", []catalog.DeleteMarker{
		{FilePath: allFiles[0].Path, RowIndices: []int64{0, 5, 11}},
		{FilePath: allFiles[1].Path, RowIndices: []int64{8, 9, 10, 11}},
	}); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.MinFiles = 2
	c := New(cat, nil, cfg)
	result, err := c.CompactTable(ctx, "logs")
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsMerged != 17 { // 24 - 7 deleted
		t.Fatalf("rows merged: got %d, want 17", result.RowsMerged)
	}

	// Read the merged file back and verify exactly the surviving ids.
	manifest, err := cat.GetManifest(ctx, "logs")
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]bool{}
	for _, r := range []int64{1, 2, 3, 4, 6, 7, 8, 9, 10} {
		want[r] = true
	}
	for _, r := range []int64{100, 101, 102, 103, 104, 105, 106, 107} {
		want[r] = true
	}
	got := map[int64]bool{}
	for _, part := range manifest.Partitions {
		for _, fe := range part.Files {
			rc, _, err := cat.ReadFile(ctx, fe.Path)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				t.Fatal(err)
			}
			rd, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			rows, err := rd.ReadRows([]string{"id"})
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				got[row["id"].(int64)] = true
			}
		}
	}
	if len(got) != len(want) {
		t.Fatalf("surviving ids: got %d, want %d (%v)", len(got), len(want), got)
	}
	for id := range want {
		if !got[id] {
			t.Fatalf("id %d missing from merged output", id)
		}
	}
}

func TestCompactTable_SkipsBelowThreshold(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}
	if err := cat.CreateTable(ctx, "small", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Only 3 files — below MinFiles threshold
	partPath := "tables/small/data"
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("%s/chunk_%d.parquet", partPath, i)
		size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{{"id": int64(i)}})
		if err := cat.AddFiles(ctx, "small", nil, partPath, []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: 1, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}

	c := New(cat, nil, DefaultConfig()) // MinFiles=10
	result, err := c.CompactTable(ctx, "small")
	if err != nil {
		t.Fatal(err)
	}

	if result.PartitionsCompacted != 0 {
		t.Errorf("expected no compaction, got %d partitions compacted", result.PartitionsCompacted)
	}
}

// TestCompactTable_MultiPassCompaction verifies that when a partition has more
// files than MaxFilesPerPass, multiple passes run back-to-back within a single
// CompactTable call until the partition is fully compacted.
func TestCompactTable_MultiPassCompaction(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}
	if err := cat.CreateTable(ctx, "multipass", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Create 30 files — with MaxFilesPerPass=10, needs 3+ passes
	const numFiles = 30
	partPath := "tables/multipass/data"
	for i := 0; i < numFiles; i++ {
		path := fmt.Sprintf("%s/chunk_%04d.parquet", partPath, i)
		size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{
			{"id": int64(i)},
		})
		if err := cat.AddFiles(ctx, "multipass", nil, partPath, []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: 1, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}

	cfg := Config{MinFiles: 5, MaxFileSizeBytes: 32 * 1024 * 1024, MaxFilesPerPass: 10}
	c := New(cat, nil, cfg)
	result, err := c.CompactTable(ctx, "multipass")
	if err != nil {
		t.Fatal(err)
	}

	// All 30 files should be compacted (multi-pass)
	if result.FilesRemoved != numFiles {
		t.Errorf("files removed: got %d, want %d", result.FilesRemoved, numFiles)
	}
	if result.RowsMerged != numFiles {
		t.Errorf("rows merged: got %d, want %d", result.RowsMerged, numFiles)
	}

	// Verify single file remains
	manifest, _ := cat.GetManifest(ctx, "multipass")
	if len(manifest.Partitions[0].Files) != 1 {
		t.Errorf("expected 1 file after multi-pass compaction, got %d",
			len(manifest.Partitions[0].Files))
	}
}

// TestAdaptivePassSize verifies that small files get a larger pass size.
func TestAdaptivePassSize(t *testing.T) {
	c := &Compactor{config: Config{MaxFilesPerPass: 50}}

	// 1000 tiny files at 1KB each → target 256MB / 1KB = 262144 max per pass
	files := make([]catalog.FileEntry, 1000)
	for i := range files {
		files[i] = catalog.FileEntry{SizeBytes: 1024}
	}
	part := catalog.PartitionEntry{Files: files}
	got := c.adaptivePassSize(part)
	if got < 1000 {
		t.Errorf("tiny files: expected pass size >= 1000 (all files), got %d", got)
	}

	// 100 files at 10MB each → target 256MB / 10MB = 25, but floor is MaxFilesPerPass=50
	files = make([]catalog.FileEntry, 100)
	for i := range files {
		files[i] = catalog.FileEntry{SizeBytes: 10 * 1024 * 1024}
	}
	part = catalog.PartitionEntry{Files: files}
	got = c.adaptivePassSize(part)
	if got != 50 {
		t.Errorf("10MB files: expected pass size = 50 (floor), got %d", got)
	}
}

func readAll(rc interface{ Read([]byte) (int, error) }) ([]byte, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(rc.(interface{ Read([]byte) (int, error) }))
	return buf.Bytes(), err
}

func TestBackgroundCompactor_SweepsAllTables(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}

	// Create two tables, each with enough files to trigger compaction.
	for _, table := range []string{"alpha", "beta"} {
		if err := cat.CreateTable(ctx, table, schema, nil); err != nil {
			t.Fatal(err)
		}
		partPath := fmt.Sprintf("tables/%s/data", table)
		for i := 0; i < 12; i++ {
			path := fmt.Sprintf("%s/chunk_%04d.parquet", partPath, i)
			size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{
				{"id": int64(i)},
			})
			if err := cat.AddFiles(ctx, table, nil, partPath, []catalog.FileEntry{
				{Path: path, SizeBytes: size, NumRows: 1, CreatedAt: time.Now().UTC()},
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Run the sweep directly (don't start the ticker-based loop).
	bc := NewBackgroundCompactor(cat, BackgroundConfig{
		Enabled:    true,
		Compaction: Config{MinFiles: 5, MaxFileSizeBytes: 32 * 1024 * 1024, MaxFilesPerPass: 50},
	}, nil)
	bc.sweep(ctx)

	// Both tables should now have 1 file each.
	for _, table := range []string{"alpha", "beta"} {
		manifest, err := cat.GetManifest(ctx, table)
		if err != nil {
			t.Fatal(err)
		}
		if len(manifest.Partitions) != 1 {
			t.Fatalf("table %s: expected 1 partition, got %d", table, len(manifest.Partitions))
		}
		if len(manifest.Partitions[0].Files) != 1 {
			t.Errorf("table %s: expected 1 file after compaction, got %d",
				table, len(manifest.Partitions[0].Files))
		}
	}
}

// TestPartitionedOutputPath covers the three partition-path shapes the
// manifest can carry: empty (unpartitioned), hive-relative, and already
// table-prefixed (harness datagen) — the last used to double-join into
// "tables/orders/tables/orders//compacted_*".
func TestPartitionedOutputPath(t *testing.T) {
	cases := []struct {
		table, partPath, wantPrefix string
	}{
		{"orders", "", "tables/orders/compacted_"},
		{"orders", "date=2026-01-01", "tables/orders/date=2026-01-01/compacted_"},
		{"orders", "tables/orders/", "tables/orders/compacted_"},
		{"orders", "tables/orders", "tables/orders/compacted_"},
		{"orders", "tables/orders/date=2026-01-01/", "tables/orders/date=2026-01-01/compacted_"},
	}
	for _, tc := range cases {
		got := partitionedOutputPath(tc.table, tc.partPath, "compacted")
		if !strings.HasPrefix(got, tc.wantPrefix) || strings.Contains(got, "//") {
			t.Errorf("partitionedOutputPath(%q,%q) = %q, want prefix %q with no //",
				tc.table, tc.partPath, got, tc.wantPrefix)
		}
	}
}

// TestPartitionedOutputPathUnique is a #494 regression for the nanosecond
// suffix partitionedOutputPath used to generate: RewriteTable calls it back
// to back for successive memory-bounded groups, fast enough on some
// platforms to land on the same nanosecond, and a repeated suffix isn't a
// name clash the store reports — the second Put silently OVERWRITES the
// first, so the first group's manifest entry then points at the second
// group's bytes and its rows are gone with no error anywhere. The fix
// (a UUIDv7 suffix) must hold up under the same back-to-back, highly
// concurrent call pattern.
func TestPartitionedOutputPathUnique(t *testing.T) {
	const n = 2000
	paths := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			paths[i] = partitionedOutputPath("orders", "", "compacted")
		}(i)
	}
	wg.Wait()

	seen := make(map[string]bool, n)
	for _, p := range paths {
		if seen[p] {
			t.Fatalf("partitionedOutputPath produced a duplicate: %q", p)
		}
		seen[p] = true
	}
}

// TestCompactTable_ZeroLengthUUIDValues is the production half of a two-path
// divergence: compaction reads through parquet.Reader.ReadRowGroup, the ROW
// path, and that path refused a zero-length entry in a UUID column ("UUID is
// 16 bytes per value but row 2 holds 10" for the unparseable case, and a
// non-NULL empty value for the empty one) on files wadjet had written itself.
// The native columnar reader read the same files without complaint, so the
// failure only appeared once the background sweep touched a table.
//
// A zero-length entry is an absence now, on both paths, and compaction of a
// file carrying one has to complete and carry the absence through.
func TestCompactTable_ZeroLengthUUIDValues(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "u", Type: parquet.TypeUUID, Nullable: true},
			{Name: "a", Type: parquet.TypeIPv6, Nullable: true},
		},
	}
	if err := cat.CreateTable(ctx, "sessions", schema, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		path := fmt.Sprintf("tables/sessions/chunk_%04d.parquet", i)
		size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{
			{"id": int64(i*3 + 0), "u": "550e8400-e29b-41d4-a716-446655440000", "a": "2001:db8::1"},
			// The legacy shape: a zero-length BYTE_ARRAY entry, not NULL.
			{"id": int64(i*3 + 1), "u": []byte{}, "a": []byte{}},
			{"id": int64(i*3 + 2), "u": nil, "a": nil},
		})
		if err := cat.AddFiles(ctx, "sessions", nil, "", []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: 3, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}

	cfg := DefaultConfig()
	cfg.MinFiles = 3
	c := New(cat, nil, cfg)
	if _, err := c.CompactTable(ctx, "sessions"); err != nil {
		t.Fatalf("compacting a table with zero-length UUID values: %v", err)
	}

	// And every row is still there, with the absences still absent.
	manifest, err := cat.GetManifest(ctx, "sessions")
	if err != nil {
		t.Fatal(err)
	}
	total, absent := 0, 0
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			rc, _, err := store.Get(ctx, "test-bucket", f.Path)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				t.Fatal(err)
			}
			r, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			rows, err := r.ReadRows(nil)
			if err != nil {
				t.Fatalf("reading the compacted file: %v", err)
			}
			for _, row := range rows {
				total++
				if v, ok := row["u"]; !ok || v == nil {
					absent++
				}
			}
		}
	}
	if total != 18 {
		t.Errorf("compaction produced %d rows, want 18", total)
	}
	if absent != 12 {
		t.Errorf("%d rows have no UUID, want 12 (one empty and one NULL per input file)", absent)
	}
}
