package worker

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// makeWshfBytes serializes rows (int64 values) into a WSHF byte slice using
// the same code path that the production sink uses.
func makeWshfBytes(t *testing.T, vals []int64) []byte {
	t.Helper()
	schema := []parquet.Column{{Name: "v", Type: parquet.TypeInt64}}
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatalf("makeWshfBytes writeHeader: %v", err)
	}
	b := batch.NewRecordBatch(schema, len(vals))
	for i, v := range vals {
		b.Columns[0].Int64Data[i] = v
		b.Columns[0].Nulls.SetValid(i)
	}
	if err := sw.writeChunk(b.Columns, nil, len(vals)); err != nil {
		t.Fatalf("makeWshfBytes writeChunk: %v", err)
	}
	// Patch chunk count in header (offset 4, uint32 LE).
	data := buf.Bytes()
	data[4] = byte(sw.numChunks)
	data[5] = byte(sw.numChunks >> 8)
	data[6] = byte(sw.numChunks >> 16)
	data[7] = byte(sw.numChunks >> 24)
	return data
}

// TestPartitionShardSource_ReadsAllFiles stages two .wshf files at a single
// partition prefix in MemStore and confirms the source streams every row
// from both.
func TestPartitionShardSource_ReadsAllFiles(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-shuffle"
	const prefix = "shuffle/q1/s0/partition=03/"

	// Build two small WSHF payloads.
	file0Data := makeWshfBytes(t, []int64{10, 20, 30})
	file1Data := makeWshfBytes(t, []int64{40, 50})

	// Stage them in MemStore.
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	key0 := prefix + "file-0.wshf"
	key1 := prefix + "file-1.wshf"
	if _, err := store.Put(ctx, bucket, key0, bytes.NewReader(file0Data), int64(len(file0Data)), "application/octet-stream"); err != nil {
		t.Fatalf("Put file-0: %v", err)
	}
	if _, err := store.Put(ctx, bucket, key1, bytes.NewReader(file1Data), int64(len(file1Data)), "application/octet-stream"); err != nil {
		t.Fatalf("Put file-1: %v", err)
	}

	// Construct a minimal Executor wired to the MemStore. Pass nil for the
	// LRU cache and JetStream — newPartitionShardSource only needs the store.
	// The cachedFileStreamSource will fall back to the in-memory WSHF path
	// (openShuffleInMemory) because spillDir is empty.
	cache := NewLRUCache(4 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil /* js — not needed */)

	src, err := newPartitionShardSource(ctx, executor, bucket, prefix)
	if err != nil {
		t.Fatalf("newPartitionShardSource: %v", err)
	}
	defer src.Close()

	if err := src.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	var totalRows int
	for {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		totalRows += b.ActiveLen()
	}

	const want = 5 // 3 from file-0 + 2 from file-1
	if totalRows != want {
		t.Errorf("total rows = %d, want %d", totalRows, want)
	}
}

// TestPartitionShardSource_EmptyPrefix verifies that listing a prefix with no
// matching files returns a source that immediately yields nil (EOF).
func TestPartitionShardSource_EmptyPrefix(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-shuffle-empty"

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}

	cache := NewLRUCache(4 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)

	src, err := newPartitionShardSource(ctx, executor, bucket, "shuffle/no-such-prefix/")
	if err != nil {
		t.Fatalf("newPartitionShardSource: %v", err)
	}
	defer src.Close()

	if err := src.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	b, err := src.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if b != nil {
		t.Errorf("expected nil batch from empty prefix, got %d rows", b.ActiveLen())
	}
}

// TestPartitionShardSource_SpillDir exercises the mmap/spill path when a spill
// directory is available (the production worker code path).
func TestPartitionShardSource_SpillDir(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-shuffle-spill"
	const prefix = "shuffle/q2/s0/partition=01/"

	file0Data := makeWshfBytes(t, []int64{100, 200, 300, 400})

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	key0 := prefix + "file-0.wshf"
	if _, err := store.Put(ctx, bucket, key0, bytes.NewReader(file0Data), int64(len(file0Data)), "application/octet-stream"); err != nil {
		t.Fatalf("Put file-0: %v", err)
	}

	spillDir := t.TempDir()
	// Verify the spill directory is writable.
	if _, err := os.Stat(spillDir); err != nil {
		t.Fatalf("spill dir: %v", err)
	}

	cache := NewLRUCache(4 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	executor.SetMemoryBudget(0, spillDir)

	src, err := newPartitionShardSource(ctx, executor, bucket, prefix)
	if err != nil {
		t.Fatalf("newPartitionShardSource: %v", err)
	}
	defer src.Close()

	if err := src.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	var totalRows int
	for {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		totalRows += b.ActiveLen()
	}

	if totalRows != 4 {
		t.Errorf("total rows = %d, want 4", totalRows)
	}
}
