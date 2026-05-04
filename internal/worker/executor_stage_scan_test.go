package worker

import (
	"bytes"
	"context"
	"hash/fnv"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// makeScanWshf writes a .wshf payload with schema (id int64, val int64) for
// use as a synthetic scan input.
func makeScanWshf(t *testing.T, rows [][2]int64) []byte {
	t.Helper()
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "val", Type: parquet.TypeInt64, Nullable: true},
	}
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatalf("writeHeader: %v", err)
	}
	b := batch.NewRecordBatch(schema, len(rows))
	for i, r := range rows {
		b.Columns[0].Int64Data[i] = r[0]
		b.Columns[0].Nulls.SetValid(i)
		b.Columns[1].Int64Data[i] = r[1]
		b.Columns[1].Nulls.SetValid(i)
	}
	if err := sw.writeChunk(b.Columns, nil, len(rows)); err != nil {
		t.Fatalf("writeChunk: %v", err)
	}
	data := buf.Bytes()
	data[4] = byte(sw.numChunks)
	return data
}

// readMemStoreInts reads a WSHF blob from MemStore at (bucket,key) and
// returns the int64 values from the named column.
func readMemStoreInts(t *testing.T, store *objstore.MemStore, bucket, key, colName string) []int64 {
	t.Helper()
	rc, _, err := store.Get(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("readMemStoreInts: get %s/%s: %v", bucket, key, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("readMemStoreInts: read %s/%s: %v", bucket, key, err)
	}
	if len(data) == 0 {
		return nil
	}
	r, err := newShuffleChunkReader(data)
	if err != nil {
		t.Fatalf("readMemStoreInts: parse %s/%s: %v", bucket, key, err)
	}
	colIdx := -1
	for i, col := range r.schema {
		if col.Name == colName {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		t.Fatalf("readMemStoreInts: column %q not in schema for %s/%s", colName, bucket, key)
	}
	var out []int64
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatalf("readMemStoreInts: chunk: %v", err)
		}
		if b == nil {
			break
		}
		vec := b.Columns[colIdx]
		for i := 0; i < b.ActiveLen(); i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			out = append(out, vec.Int64Data[row])
		}
	}
	return out
}

// TestExecuteStageScan_PartitionedStreamingOutput drives runStageScanPartitionedStreaming
// end-to-end via executeStage: a scan task with ShuffleKeys+NumPartitions reads
// a synthetic WSHF, hash-partitions its rows, and uploads per-partition WSHF
// files. The test verifies that:
//
//  1. result.NumRows equals the input row count (no rows dropped),
//  2. result.ResultFiles all match the partition=NNNN/ key shape,
//  3. each row in partition p hashes back to p (consistency with the existing
//     executeShuffle path's hashing — same hashRowsIntoPartitions kernel).
//
// Regression for the 2026-05-03 PR #85 revert: the prior (CollectSink+
// writeStageOutput) path materialised every batch in heap before
// partitioning, blowing memory at SF10 and causing the Q05 8m29s regression.
// The streaming path bounds memory at one batch + N×64 KB partition buffers.
func TestExecuteStageScan_PartitionedStreamingOutput(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-stage-scan-part"
	const numParts = 4
	const numRows = 1000

	// Synthetic input: 1000 rows of (id=i, val=i*10).
	rows := make([][2]int64, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = [2]int64{int64(i), int64(i) * 10}
	}
	scanData := makeScanWshf(t, rows)

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	scanKey := "in/scan/t0.wshf"
	if _, err := store.Put(ctx, bucket, scanKey, bytes.NewReader(scanData), int64(len(scanData)), "application/octet-stream"); err != nil {
		t.Fatalf("Put scan input: %v", err)
	}

	cache := NewLRUCache(4 * 1024 * 1024)
	executor := NewExecutor(store, cache, nil)

	task := distributed.Task{
		ID:        "scan-part-0",
		QueryID:   "q-scan",
		StageID:   "scan-1",
		Type:      distributed.TaskTypeStage,
		StageType: "scan",
		Inputs: map[string][]string{
			"t": {scanKey},
		},
		Columns:       []string{"id", "val"},
		ShuffleKeys:   []string{"id"},
		NumPartitions: numParts,
		DataBucket:    bucket,
		ResultBucket:  bucket,
		ResultPrefix:  "out/scan-1/",
	}

	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != int64(numRows) {
		t.Fatalf("NumRows = %d, want %d", result.NumRows, numRows)
	}
	if len(result.ResultFiles) == 0 {
		t.Fatalf("expected at least 1 partition file, got 0")
	}
	for _, k := range result.ResultFiles {
		if !strings.Contains(k, "partition=") || !strings.HasSuffix(k, ".wshf") {
			t.Errorf("output key %q does not match partition=NNNN/*.wshf", k)
		}
	}

	// Read every partition back. Verify (a) total rows equals input and
	// (b) each row's id hashes back to its partition. Mirrors the same
	// invariant TestPartitionedShuffleSink_RoundTrip checks for the
	// underlying sink — this test extends it to the dispatch path.
	keySchema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
	var scratch [8]byte
	totalRows := 0
	seen := make(map[int64]bool, numRows)
	for _, key := range result.ResultFiles {
		// Extract partition number from the key:  out/scan-1/partition=0003/scan-part-0.wshf
		idx := strings.Index(key, "partition=")
		if idx < 0 {
			t.Fatalf("missing partition= in key %q", key)
		}
		partition, err := strconv.Atoi(key[idx+len("partition=") : idx+len("partition=")+4])
		if err != nil {
			t.Fatalf("parsing partition from %q: %v", key, err)
		}

		ids := readMemStoreInts(t, store, bucket, key, "id")
		for _, id := range ids {
			if seen[id] {
				t.Errorf("id %d appeared in multiple partitions", id)
			}
			seen[id] = true
			keyBatch := batch.NewRecordBatch(keySchema, 1)
			keyBatch.Columns[0].Int64Data[0] = id
			keyBatch.Columns[0].Nulls.SetValid(0)
			h := fnv.New64a()
			hashVectorValue(h, keyBatch.Columns[0], 0, scratch[:])
			got := int(h.Sum64() % uint64(numParts))
			if got != partition {
				t.Errorf("id=%d landed in partition=%d, hashes to %d", id, partition, got)
			}
		}
		totalRows += len(ids)
	}
	if totalRows != numRows {
		t.Errorf("total rows across partitions = %d, want %d", totalRows, numRows)
	}
}

