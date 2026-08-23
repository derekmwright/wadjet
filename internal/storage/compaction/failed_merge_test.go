package compaction

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// manifestPaths returns every file path in a table's manifest, sorted.
func manifestPaths(t *testing.T, cat *catalog.Catalog, table string) []string {
	t.Helper()
	m, err := cat.GetManifest(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, p := range m.Partitions {
		for _, f := range p.Files {
			paths = append(paths, f.Path)
		}
	}
	sort.Strings(paths)
	return paths
}

// TestCompactTable_KeepsGoingPastAFailedPartition is #435's over-correction.
//
// Reporting the failed merge to the caller was right; returning at the FIRST
// one was not. One partition whose file schema has drifted then froze
// compaction of every OTHER partition in the table — and, because the
// background sweep `continue`s to the next table on any error from
// CompactTable, the table's delete-marker GC with it, indefinitely.
func TestCompactTable_KeepsGoingPastAFailedPartition(t *testing.T) {
	ctx := context.Background()
	cat, store := setupTestCatalog(t)
	tableSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}}
	const table = "mixed"
	if err := cat.CreateTable(ctx, table, tableSchema, nil); err != nil {
		t.Fatal(err)
	}
	driftSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeString, Nullable: true},
	}}

	// Two partitions, three files each. The FIRST one in manifest order is
	// the broken one, so a first-failure return sees the good one not at all.
	for _, p := range []struct {
		name   string
		schema parquet.Schema
		v      any
	}{
		{"a_bad", driftSchema, "hello"},
		{"b_good", tableSchema, int64(1)},
	} {
		partPath := "tables/" + table + "/d=" + p.name
		for i := 0; i < 3; i++ {
			path := fmt.Sprintf("%s/chunk_%04d.parquet", partPath, i)
			writeTestFile(t, store, "test-bucket", path, p.schema,
				[]map[string]any{{"id": int64(i), "v": p.v}})
			if err := cat.AddFiles(ctx, table, map[string]string{"d": p.name}, partPath,
				[]catalog.FileEntry{{Path: path, SizeBytes: 1024, NumRows: 1, CreatedAt: time.Now().UTC()}}); err != nil {
				t.Fatal(err)
			}
		}
	}

	cfg := DefaultConfig()
	cfg.MinFiles = 2
	cfg.DeleteGrace = -1
	res, err := New(cat, nil, cfg).CompactTable(ctx, table)

	if err == nil {
		t.Fatalf("CompactTable reported success over a partition whose merge failed: %+v", res)
	}
	var agg *CompactionFailed
	if !errors.As(err, &agg) {
		t.Fatalf("error %T is not *CompactionFailed: %v", err, err)
	}
	if len(agg.Failures) != 1 || !strings.Contains(agg.Failures[0].Partition, "a_bad") {
		t.Fatalf("Failures = %+v, want exactly the drifted partition", agg.Failures)
	}
	if !agg.Partial() {
		t.Error("Partial() is false, but the good partition compacted in the same call")
	}
	if !strings.Contains(err.Error(), table) || !strings.Contains(err.Error(), "a_bad") {
		t.Errorf("aggregate error %q names neither the table nor the failed partition", err)
	}
	// The cause still reaches errors.As/Is through Unwrap() []error.
	for _, want := range []string{"INT64", "STRING"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("aggregate error %q does not carry the cause (%s)", err, want)
		}
	}

	// Result.Failed names the same partition, and the counters show the good
	// one DID compact.
	if len(res.Failed) != 1 {
		t.Fatalf("Result.Failed = %+v, want one", res.Failed)
	}
	if res.FilesCreated != 1 || res.FilesRemoved != 3 {
		t.Errorf("counters = %d created / %d removed, want the good partition's 3 -> 1", res.FilesCreated, res.FilesRemoved)
	}

	m, err := cat.GetManifest(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range m.Partitions {
		switch {
		case strings.Contains(p.Path, "a_bad"):
			if len(p.Files) != 3 {
				t.Errorf("failed partition holds %d files, want its 3 inputs untouched", len(p.Files))
			}
		case strings.Contains(p.Path, "b_good"):
			if len(p.Files) != 1 {
				t.Errorf("good partition holds %d files, want it compacted to 1", len(p.Files))
			}
		}
	}
}

// TestBackgroundSweep_ContinuesPastAPartialCompactionFailure: the sweep must
// run the table's delete-marker GC and move to the next table even when one
// of the table's partitions could not be compacted.
func TestBackgroundSweep_ContinuesPastAPartialCompactionFailure(t *testing.T) {
	ctx := context.Background()
	cat, store := setupTestCatalog(t)
	tableSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}}
	driftSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeString, Nullable: true},
	}}

	// Table one carries the drifted partition AND a delete marker old enough
	// to GC. Table two is ordinary and must still be swept.
	if err := cat.CreateTable(ctx, "t_broken", tableSchema, nil); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable(ctx, "t_after", tableSchema, nil); err != nil {
		t.Fatal(err)
	}

	// t_broken: a drifted partition (compaction fails on it) plus a clean
	// partition holding one file with a delete marker.
	for i := 0; i < 2; i++ {
		path := fmt.Sprintf("tables/t_broken/d=bad/chunk_%04d.parquet", i)
		writeTestFile(t, store, "test-bucket", path, driftSchema,
			[]map[string]any{{"id": int64(i), "v": "x"}})
		if err := cat.AddFiles(ctx, "t_broken", map[string]string{"d": "bad"}, "tables/t_broken/d=bad",
			[]catalog.FileEntry{{Path: path, SizeBytes: 1024, NumRows: 1, CreatedAt: time.Now().UTC()}}); err != nil {
			t.Fatal(err)
		}
	}
	const gcPath = "tables/t_broken/d=ok/chunk_0000.parquet"
	writeTestFile(t, store, "test-bucket", gcPath, tableSchema, []map[string]any{
		{"id": int64(1), "v": int64(1)},
		{"id": int64(2), "v": int64(2)},
	})
	if err := cat.AddFiles(ctx, "t_broken", map[string]string{"d": "ok"}, "tables/t_broken/d=ok",
		[]catalog.FileEntry{{Path: gcPath, SizeBytes: 1024, NumRows: 2, CreatedAt: time.Now().UTC()}}); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddDeleteMarkers(ctx, "t_broken", []catalog.DeleteMarker{{
		FilePath:   gcPath,
		RowIndices: []int64{0},
		CreatedAt:  time.Now().Add(-2 * time.Hour).UTC(),
	}}); err != nil {
		t.Fatal(err)
	}

	// t_after: two small files that must compact once the sweep gets there.
	for i := 0; i < 2; i++ {
		path := fmt.Sprintf("tables/t_after/chunk_%04d.parquet", i)
		writeTestFile(t, store, "test-bucket", path, tableSchema,
			[]map[string]any{{"id": int64(i), "v": int64(i)}})
		if err := cat.AddFiles(ctx, "t_after", nil, "",
			[]catalog.FileEntry{{Path: path, SizeBytes: 1024, NumRows: 1, CreatedAt: time.Now().UTC()}}); err != nil {
			t.Fatal(err)
		}
	}

	cfg := BackgroundConfig{
		Enabled:  true,
		GCMinAge: time.Hour,
		Compaction: Config{
			MinFiles:         2,
			MaxFileSizeBytes: 32 << 20,
			MaxFilesPerPass:  50,
			DeleteGrace:      -1,
		},
	}
	NewBackgroundCompactor(cat, cfg, nil).sweep(ctx)

	// The GC ran despite the failed partition: the marker is gone and the
	// deleted row is physically absent from the rewritten file.
	m, err := cat.GetManifest(ctx, "t_broken")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.DeleteMarkers) != 0 {
		t.Errorf("delete markers survived the sweep (%d) — the failed partition froze GC for the table",
			len(m.DeleteMarkers))
	}
	for _, p := range m.Partitions {
		if strings.Contains(p.Path, "d=ok") {
			if len(p.Files) != 1 || p.Files[0].NumRows != 1 {
				t.Errorf("GC did not rewrite the file: %+v", p.Files)
			}
		}
		if strings.Contains(p.Path, "d=bad") && len(p.Files) != 2 {
			t.Errorf("the drifted partition's inputs were disturbed: %+v", p.Files)
		}
	}

	// And the sweep reached the next table.
	if got := manifestPaths(t, cat, "t_after"); len(got) != 1 {
		t.Errorf("t_after holds %d files, want it compacted to 1 — the sweep stopped at the broken table", len(got))
	}
}
