package exec

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Regression tests for sweep finding #24: external sort/window error paths
// orphaned run files and partial merge outputs in the worker-lifetime spill
// dir (no per-task RemoveAll there), self-compounding the ENOSPC conditions
// that trigger those very errors. The contract under test: every helper
// deletes its consumed inputs and partial outputs on its own error paths.

func binFiles(tb testing.TB, dir string) []string {
	tb.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.bin"))
	if err != nil {
		tb.Fatal(err)
	}
	return matches
}

var scratchKeys = []SortKey{{Column: "val", Order: Ascending}}

func makeRun(tb testing.TB, dir string, vals ...int64) string {
	tb.Helper()
	schema := []parquet.Column{{Name: "val", Type: parquet.TypeInt64}}
	rows := make([]map[string]any, len(vals))
	for i, v := range vals {
		rows[i] = map[string]any{"val": v}
	}
	b := batch.FromRows(schema, rows)
	path, err := sortBatchesToRun(dir, schema, []*batch.RecordBatch{b}, len(vals), scratchKeys, 0)
	if err != nil {
		tb.Fatal(err)
	}
	return path
}

// corruptRunTail bumps the run's batch count and appends a truncated tail,
// so the cursor opens cleanly (first batch valid) but errors mid-merge on
// the second batch — exercising the post-writer-creation error paths.
func corruptRunTail(tb testing.TB, path string) {
	tb.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	binary.LittleEndian.PutUint32(data[:4], binary.LittleEndian.Uint32(data[:4])+1)
	data = append(data, 0xDE, 0xAD)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		tb.Fatal(err)
	}
}

func TestSpillBatchWriter_AbortRemovesFile(t *testing.T) {
	dir := t.TempDir()
	sw, err := newSpillBatchWriter(dir, "sort-merge")
	if err != nil {
		t.Fatal(err)
	}
	schema := []parquet.Column{{Name: "val", Type: parquet.TypeInt64}}
	if err := sw.writeBatch(batch.FromRows(schema, []map[string]any{{"val": int64(1)}})); err != nil {
		t.Fatal(err)
	}
	sw.abort()
	if got := binFiles(t, dir); len(got) != 0 {
		t.Fatalf("abort left files behind: %v", got)
	}
	sw.abort() // idempotent
	if path, err := sw.close(); err != nil || path != "" {
		t.Fatalf("close after abort = (%q, %v), want no-op", path, err)
	}
}

func TestMergeRunsToFile_MidMergeErrorRemovesPartial(t *testing.T) {
	dir := t.TempDir()
	good := makeRun(t, dir, 1, 2, 3)
	evil := makeRun(t, dir, 4, 5, 6)
	corruptRunTail(t, evil)

	schema := []parquet.Column{{Name: "val", Type: parquet.TypeInt64}}
	if _, err := mergeRunsToFile(dir, schema, scratchKeys, []string{good, evil}, 0); err == nil {
		t.Fatal("expected merge error from corrupt run")
	}
	// Inputs are the caller's to clean; the partial sort-merge output must
	// be gone.
	for _, f := range binFiles(t, dir) {
		if filepath.Base(f) != filepath.Base(good) && filepath.Base(f) != filepath.Base(evil) {
			t.Errorf("partial merge output left behind: %s", f)
		}
	}
}

func TestPreMergeRuns_ErrorCleansEverything(t *testing.T) {
	dir := t.TempDir()
	oldFan := maxMergeFanIn
	maxMergeFanIn = 2
	t.Cleanup(func() { maxMergeFanIn = oldFan })

	runs := []string{
		makeRun(t, dir, 1, 2),
		makeRun(t, dir, 3, 4),
		makeRun(t, dir, 5, 6),
	}
	corruptRunTail(t, runs[1])

	schema := []parquet.Column{{Name: "val", Type: parquet.TypeInt64}}
	if _, err := preMergeRuns(dir, schema, scratchKeys, runs, 2, 0); err == nil {
		t.Fatal("expected error from corrupt run")
	}
	if got := binFiles(t, dir); len(got) != 0 {
		t.Fatalf("preMergeRuns error left scratch behind: %v", got)
	}
}

func TestResortRunsByKeys_ErrorCleansEverything(t *testing.T) {
	dir := t.TempDir()
	runs := []string{
		makeRun(t, dir, 1, 2),
		makeRun(t, dir, 3, 4),
	}
	corruptRunTail(t, runs[1])

	schema := []parquet.Column{{Name: "val", Type: parquet.TypeInt64}}
	if _, err := resortRunsByKeys(dir, schema, runs, scratchKeys); err == nil {
		t.Fatal("expected error from corrupt run")
	}
	if got := binFiles(t, dir); len(got) != 0 {
		t.Fatalf("resortRunsByKeys error left scratch behind: %v", got)
	}
}

func TestOpenRunMerger_CursorErrorCleansRuns(t *testing.T) {
	dir := t.TempDir()
	good := makeRun(t, dir, 1, 2)
	// 2-byte file: openSpillBatchReader fails reading the count header.
	evil := filepath.Join(dir, "sort-run-evil.bin")
	if err := os.WriteFile(evil, []byte{0x01, 0x02}, 0o600); err != nil {
		t.Fatal(err)
	}

	schema := []parquet.Column{{Name: "val", Type: parquet.TypeInt64}}
	m, _, err := openRunMerger(dir, schema, scratchKeys, []string{good, evil})
	if err == nil {
		m.close()
		t.Fatal("expected cursor error from corrupt run")
	}
	if got := binFiles(t, dir); len(got) != 0 {
		t.Fatalf("openRunMerger error left runs behind: %v", got)
	}
}

// Sort.Finalize with a run file corrupted on disk (the ENOSPC-adjacent
// shape: a run that can't be read back) must not strand the other runs —
// finalizeExternalMerge nils s.runFiles up front, so Close's backstops
// never see them.
func TestSortFinalize_CursorErrorCleansRuns(t *testing.T) {
	forceTinyRuns(t)
	schema := []parquet.Column{{Name: "val", Type: parquet.TypeInt64}}
	s := newSortSpillHarness(t, scratchKeys, 256)

	ctx := context.Background()
	for i := 0; i < 200; i += 5 {
		rows := make([]map[string]any, 0, 5)
		for j := i; j < i+5; j++ {
			rows = append(rows, map[string]any{"val": int64(j)})
		}
		if err := s.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.runFiles) < 2 {
		t.Fatalf("need ≥2 runs, got %d", len(s.runFiles))
	}
	dir := filepath.Dir(s.runFiles[0])
	// Truncate one run so its cursor fails to open.
	if err := os.WriteFile(s.runFiles[0], []byte{0x01}, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := s.Finalize(ctx); err == nil {
		t.Fatal("expected Finalize error from corrupt run")
	}
	if got := binFiles(t, dir); len(got) != 0 {
		t.Fatalf("Finalize error left runs behind: %v", got)
	}
}
