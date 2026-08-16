package worker

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestExecuteShuffle_HappyPath verifies that a shuffle task reads source Parquet
// files, hash-partitions rows across NumPartitions output files, uploads each
// non-empty partition to MemStore, and reports the correct total row count.
func TestExecuteShuffle_HappyPath(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-shuffle-exec"
	const numParts = 4
	const numRows = 100

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}

	// Write a Parquet file with 100 rows of (k INT64, v STRING).
	rows := make([]map[string]any, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = map[string]any{
			"k": int64(i),
			"v": fmt.Sprintf("value-%d", i),
		}
	}
	writeParquetFile(t, store, bucket, "tables/src/part-0.parquet", rows)

	cache := NewLRUCache(64 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil /* js — not needed for shuffle */)
	executor.SetMemoryBudget(0, t.TempDir())

	task := distributed.Task{
		ID:            "shuf-1",
		QueryID:       "q-shuf",
		StageID:       "shuffle-0",
		Type:          distributed.TaskTypeShuffle,
		Files:         []string{"tables/src/part-0.parquet"},
		ShuffleKeys:   []string{"k"},
		NumPartitions: numParts,
		DataBucket:    bucket,
		ResultBucket:  bucket,
		ResultPrefix:  "shuffle/q-shuf/s0/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("shuffle task failed: %s", result.Error)
	}
	if result.NumRows != numRows {
		t.Errorf("expected %d rows, got %d", numRows, result.NumRows)
	}
	if len(result.ResultFiles) == 0 {
		t.Fatal("expected at least one partition file in ResultFiles")
	}
	if result.SizeBytes == 0 {
		t.Errorf("result.SizeBytes = 0; expected nonzero after writing 100 rows")
	}
	t.Logf("shuffle: %d rows → %d partition files, %d bytes",
		result.NumRows, len(result.ResultFiles), result.SizeBytes)

	// Per-partition accounting: vectors sized to NumPartitions, rows sum to
	// the task total, bytes sum to SizeBytes, and a partition has bytes iff
	// it has rows.
	if len(result.PartitionRows) != numParts || len(result.PartitionBytes) != numParts {
		t.Fatalf("partition vectors len = %d/%d, want %d",
			len(result.PartitionRows), len(result.PartitionBytes), numParts)
	}
	var vecRows, vecBytes int64
	for p := 0; p < numParts; p++ {
		vecRows += result.PartitionRows[p]
		vecBytes += result.PartitionBytes[p]
		if (result.PartitionRows[p] == 0) != (result.PartitionBytes[p] == 0) {
			t.Errorf("partition %d: rows=%d bytes=%d — must be zero/nonzero together",
				p, result.PartitionRows[p], result.PartitionBytes[p])
		}
	}
	if vecRows != numRows {
		t.Errorf("sum(PartitionRows) = %d, want %d", vecRows, numRows)
	}
	if vecBytes != result.SizeBytes {
		t.Errorf("sum(PartitionBytes) = %d, want SizeBytes %d", vecBytes, result.SizeBytes)
	}

	// Verify each ResultFile key follows the expected naming convention and exists.
	for _, key := range result.ResultFiles {
		if !strings.Contains(key, "partition=") {
			t.Errorf("result file key %q does not contain partition= prefix", key)
		}
		if !strings.HasSuffix(key, ".wshf") {
			t.Errorf("result file key %q does not end with .wshf", key)
		}
		rc, _, err := store.Get(ctx, bucket, key)
		if err != nil {
			t.Fatalf("result file %q not found in store: %v", key, err)
		}
		rc.Close()
	}

	// Read back all output files and sum rows to verify no row is lost.
	totalBack := int64(0)
	for _, key := range result.ResultFiles {
		rc, _, err := store.Get(ctx, bucket, key)
		if err != nil {
			t.Fatalf("reading back %q: %v", key, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("reading back %q data: %v", key, err)
		}
		data, err = DecompressShuffleData(data)
		if err != nil {
			t.Fatalf("decompressing %q: %v", key, err)
		}
		r, err := newShuffleChunkReader(data)
		if err != nil {
			t.Fatalf("parsing %q: %v", key, err)
		}
		for {
			b, err := r.Next()
			if err != nil {
				t.Fatalf("chunk reader Next from %q: %v", key, err)
			}
			if b == nil {
				break
			}
			totalBack += int64(b.ActiveLen())
		}
	}
	if totalBack != numRows {
		t.Errorf("read-back total rows = %d, want %d", totalBack, numRows)
	}
}

// TestExecuteShuffle_ColumnProjection verifies that Columns projection is respected:
// only the projected columns appear in the output.
func TestExecuteShuffle_ColumnProjection(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-shuffle-proj"

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}

	rows := []map[string]any{
		{"k": int64(1), "v": "a", "extra": "x"},
		{"k": int64(2), "v": "b", "extra": "y"},
		{"k": int64(3), "v": "c", "extra": "z"},
	}
	writeParquetFile(t, store, bucket, "tables/src/proj-0.parquet", rows)

	cache := NewLRUCache(64 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	executor.SetMemoryBudget(0, t.TempDir())

	task := distributed.Task{
		ID:            "shuf-proj",
		QueryID:       "q-proj",
		StageID:       "shuffle-0",
		Type:          distributed.TaskTypeShuffle,
		Files:         []string{"tables/src/proj-0.parquet"},
		Columns:       []string{"k", "v"},
		ShuffleKeys:   []string{"k"},
		NumPartitions: 2,
		DataBucket:    bucket,
		ResultBucket:  bucket,
		ResultPrefix:  "shuffle/q-proj/s0/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("shuffle (projection) failed: %s", result.Error)
	}
	if result.NumRows != 3 {
		t.Errorf("expected 3 rows, got %d", result.NumRows)
	}

	// Confirm projected columns only (k, v) appear — not "extra".
	for _, key := range result.ResultFiles {
		rc, _, err := store.Get(ctx, bucket, key)
		if err != nil {
			t.Fatalf("reading back %q: %v", key, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("reading data %q: %v", key, err)
		}
		if len(data) == 0 {
			continue
		}
		data, err = DecompressShuffleData(data)
		if err != nil {
			t.Fatalf("decompressing %q: %v", key, err)
		}
		r, err := newShuffleChunkReader(data)
		if err != nil {
			t.Fatalf("parsing %q: %v", key, err)
		}
		for _, col := range r.schema {
			if col.Name == "extra" {
				t.Errorf("unexpected column %q in projected output file %q", col.Name, key)
			}
		}
	}
}

// TestExecuteShuffle_ValidationErrors checks that missing required fields cause
// early failures with descriptive errors.
func TestExecuteShuffle_ValidationErrors(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)

	base := distributed.Task{
		ID:            "shuf-err",
		QueryID:       "q-err",
		Type:          distributed.TaskTypeShuffle,
		Files:         []string{"tables/src/f.parquet"},
		ShuffleKeys:   []string{"k"},
		NumPartitions: 4,
		DataBucket:    "b",
		ResultBucket:  "b",
		ResultPrefix:  "p/",
	}

	cases := []struct {
		name    string
		mutate  func(*distributed.Task)
		wantErr string
	}{
		{
			name:    "no_shuffle_keys",
			mutate:  func(t *distributed.Task) { t.ShuffleKeys = nil },
			wantErr: "ShuffleKeys",
		},
		{
			name:    "zero_num_partitions",
			mutate:  func(t *distributed.Task) { t.NumPartitions = 0 },
			wantErr: "NumPartitions",
		},
		{
			name:    "no_files",
			mutate:  func(t *distributed.Task) { t.Files = nil },
			wantErr: "Files",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := base
			tc.mutate(&task)
			result := executor.Execute(ctx, task, "w1")
			if result.Success {
				t.Fatal("expected failure, got success")
			}
			if !strings.Contains(result.Error, tc.wantErr) {
				t.Errorf("error %q does not mention %q", result.Error, tc.wantErr)
			}
		})
	}
}

// TestExecuteShuffle_SchemaRoundTrip verifies that the output schema matches
// the input schema (preserving column names and types).
func TestExecuteShuffle_SchemaRoundTrip(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-shuffle-schema"

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}

	rows := []map[string]any{
		{"id": int64(1), "score": float64(9.5)},
		{"id": int64(2), "score": float64(7.2)},
	}
	writeParquetFile(t, store, bucket, "tables/src/schema-0.parquet", rows)

	cache := NewLRUCache(64 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	executor.SetMemoryBudget(0, t.TempDir())

	task := distributed.Task{
		ID:            "shuf-schema",
		QueryID:       "q-schema",
		StageID:       "shuffle-0",
		Type:          distributed.TaskTypeShuffle,
		Files:         []string{"tables/src/schema-0.parquet"},
		ShuffleKeys:   []string{"id"},
		NumPartitions: 2,
		DataBucket:    bucket,
		ResultBucket:  bucket,
		ResultPrefix:  "shuffle/q-schema/s0/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("shuffle failed: %s", result.Error)
	}
	if result.NumRows != 2 {
		t.Errorf("expected 2 rows, got %d", result.NumRows)
	}

	wantSchema := map[string]parquet.TypeID{
		"id":    parquet.TypeInt64,
		"score": parquet.TypeFloat64,
	}

	for _, key := range result.ResultFiles {
		rc, _, err := store.Get(ctx, bucket, key)
		if err != nil {
			t.Fatalf("reading %q: %v", key, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("reading data %q: %v", key, err)
		}
		if len(data) == 0 {
			continue
		}
		data, err = DecompressShuffleData(data)
		if err != nil {
			t.Fatalf("decompressing %q: %v", key, err)
		}
		r, err := newShuffleChunkReader(data)
		if err != nil {
			t.Fatalf("parsing %q: %v", key, err)
		}
		for _, col := range r.schema {
			wantType, ok := wantSchema[col.Name]
			if !ok {
				t.Errorf("unexpected column %q in output", col.Name)
				continue
			}
			if col.Type != wantType {
				t.Errorf("column %q: type = %v, want %v", col.Name, col.Type, wantType)
			}
		}
	}
}

// TestExecuteShuffle_DeferredDynamicFilter: a Deferred (attach-on-arrival)
// spec whose artifact is already staged must engage on the shuffle path —
// before this test's fix, handleShuffleTask skipped Deferred specs
// entirely (no poller, no ops), leaving SF100 probe-side shuffle scans
// unfiltered at every layer. Rows outside the bloom must be dropped from
// the shuffle output (drop-only: a brief unfiltered head is legal, so the
// assertion allows a small pass-through prefix).
func TestExecuteShuffle_DeferredDynamicFilter(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-shuffle-deferred-df"
	const numRows = 5000

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	rows := make([]map[string]any, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = map[string]any{"k": int64(i), "v": fmt.Sprintf("value-%d", i)}
	}
	writeParquetFile(t, store, bucket, "tables/src/part-0.parquet", rows)

	// Stage the merged artifact BEFORE the task runs: the poll resolves on
	// its first tick, so the filter installs within the first batches.
	mergedKey := "queries/q-shuf-df/dynfilter-merged/scan-1/f1.wdf"
	stageRangedArtifact(t, store, bucket, mergedKey, 0, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

	executor := NewExecutor(store, NewLRUCache(64*1024*1024), nil)
	executor.SetMemoryBudget(0, t.TempDir())

	task := distributed.Task{
		ID:            "shuf-df-1",
		QueryID:       "q-shuf-df",
		StageID:       "shuffle-0",
		Type:          distributed.TaskTypeShuffle,
		Files:         []string{"tables/src/part-0.parquet"},
		ShuffleKeys:   []string{"k"},
		NumPartitions: 2,
		DataBucket:    bucket,
		ResultBucket:  bucket,
		ResultPrefix:  "shuffle/q-shuf-df/s0/",
		DynamicFilters: []distributed.DynamicFilterSpec{{
			FilterID: "f1", TargetColumn: "k", KeyType: "int64",
			BloomBucket: bucket, BloomKey: mergedKey, Deferred: true,
		}},
	}
	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("shuffle task failed: %s", result.Error)
	}
	// 10 keys pass the bloom. Install happens between batches, so an
	// unfiltered head may leak through — but anything close to the full
	// input means the deferred filter never engaged.
	if result.NumRows >= numRows/2 {
		t.Fatalf("deferred filter never engaged: %d of %d rows shuffled", result.NumRows, numRows)
	}
	if result.NumRows < 10 {
		t.Fatalf("in-bloom rows lost: %d rows shuffled, want >= 10", result.NumRows)
	}
	t.Logf("deferred filter engaged: %d of %d rows shuffled", result.NumRows, numRows)
}
