package worker

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// TestExecuteShuffle_WSHFInput verifies that a shuffle task can accept .wshf
// files from an upstream shuffle stage as input (Phase 3 native-DAG
// chained-shuffle support). Stages:
//  1. Shuffle parquet → partition=N/*.wshf
//  2. Shuffle the partitioned .wshf → re-partition=M/*.wshf
//
// Round-trip row count must equal the parquet input row count.
func TestExecuteShuffle_WSHFInput(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-shuffle-wshf"
	const numRows = 200

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}

	// Source parquet.
	rows := make([]map[string]any, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = map[string]any{
			"k": int64(i),
			"v": fmt.Sprintf("value-%d", i),
		}
	}
	writeParquetFile(t, store, bucket, "tables/src/part-0.parquet", rows)

	cache := NewLRUCache(64 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	executor.SetMemoryBudget(0, t.TempDir())

	// Stage 1: shuffle parquet → .wshf partitions.
	s1 := distributed.Task{
		ID:            "s1",
		QueryID:       "q-chain",
		StageID:       "shuffle-1",
		Type:          distributed.TaskTypeShuffle,
		Files:         []string{"tables/src/part-0.parquet"},
		ShuffleKeys:   []string{"k"},
		NumPartitions: 4,
		DataBucket:    bucket,
		ResultBucket:  bucket,
		ResultPrefix:  "shuffle/q-chain/s1/",
	}
	r1 := executor.Execute(ctx, s1, "w1")
	if !r1.Success {
		t.Fatalf("stage 1 failed: %s", r1.Error)
	}
	if r1.NumRows != numRows {
		t.Errorf("stage 1 rows: got %d, want %d", r1.NumRows, numRows)
	}
	if len(r1.ResultFiles) == 0 {
		t.Fatal("stage 1 produced no files")
	}
	t.Logf("stage 1 produced %d wshf files", len(r1.ResultFiles))

	// Stage 2: shuffle the .wshf output from stage 1. Feeding the
	// partitioned .wshf keys back in as Files exercises the classifier →
	// cachedFileStreamSource branch in executeShuffle.
	s2 := distributed.Task{
		ID:            "s2",
		QueryID:       "q-chain",
		StageID:       "shuffle-2",
		Type:          distributed.TaskTypeShuffle,
		Files:         r1.ResultFiles,
		ShuffleKeys:   []string{"v"},
		NumPartitions: 2,
		DataBucket:    bucket,
		ResultBucket:  bucket,
		ResultPrefix:  "shuffle/q-chain/s2/",
	}
	r2 := executor.Execute(ctx, s2, "w1")
	if !r2.Success {
		t.Fatalf("stage 2 failed: %s", r2.Error)
	}
	if r2.NumRows != numRows {
		t.Errorf("stage 2 rows: got %d, want %d (no row loss through chained shuffle)", r2.NumRows, numRows)
	}
	t.Logf("stage 2 produced %d wshf files from %d input wshf files",
		len(r2.ResultFiles), len(r1.ResultFiles))

	// Read every output file and sum rows — confirms end-to-end round trip.
	var totalBack int64
	for _, key := range r2.ResultFiles {
		rc, _, err := store.Get(ctx, bucket, key)
		if err != nil {
			t.Fatalf("get %q: %v", key, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read %q: %v", key, err)
		}
		data, err = DecompressShuffleData(data)
		if err != nil {
			t.Fatalf("decompress %q: %v", key, err)
		}
		rdr, err := newShuffleChunkReader(data)
		if err != nil {
			t.Fatalf("reader %q: %v", key, err)
		}
		for {
			b, err := rdr.Next()
			if err != nil {
				t.Fatalf("chunk %q: %v", key, err)
			}
			if b == nil {
				break
			}
			totalBack += int64(b.ActiveLen())
		}
	}
	if totalBack != numRows {
		t.Errorf("round-trip rows: got %d, want %d", totalBack, numRows)
	}
}

// TestExecuteShuffle_MixedInputKindsRejected verifies that a shuffle task
// rejects a Files list mixing .parquet and .wshf — that would indicate a
// planner bug and we'd rather fail fast than silently read one format.
func TestExecuteShuffle_MixedInputKindsRejected(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-shuffle-mixed"
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	cache := NewLRUCache(8 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	executor.SetMemoryBudget(0, t.TempDir())

	task := distributed.Task{
		ID:            "mixed",
		QueryID:       "q-mixed",
		StageID:       "shuffle-0",
		Type:          distributed.TaskTypeShuffle,
		Files:         []string{"tables/a.parquet", "partition=0000/b.wshf"},
		ShuffleKeys:   []string{"k"},
		NumPartitions: 2,
		DataBucket:    bucket,
		ResultBucket:  bucket,
		ResultPrefix:  "shuffle/q-mixed/s0/",
	}
	r := executor.Execute(ctx, task, "w1")
	if r.Success {
		t.Fatal("expected failure on mixed input kinds")
	}
}
