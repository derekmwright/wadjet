package worker

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestCachedFileStreamSourceMultiChunk verifies that cachedFileStreamSource
// returns every chunk of every file when the input is multi-chunk WSHF
// produced by partitionedShuffleSink. This is the lead from
// q17_min_test.go: "Compare batch-by-batch each .wshf chunk's expected vs
// actual SUM(c) when read by cachedFileStreamSource."
//
// Reproduces the failing distributed flow's end of the pipe:
//
//	partitionedShuffleSink → uploaded WSHF (multi-chunk) →
//	  cachedFileStreamSource → HashAggregate(SUM(c) GROUP BY key)
//
// We feed exactly the same row set into the HashAggregate via two paths
// and compare:
//
//	in-process (control): partition-files-on-disk → shuffleChunkReader directly
//	via-source (variable): partition-files-on-disk → openShuffleFile → mmap →
//	                        shuffleChunkReader (the production path)
//
// Both paths must return the same total SUM(c) and the same set of keys.
// Any divergence localises the bug to the cachedFileStreamSource layer.
func TestCachedFileStreamSourceMultiChunk(t *testing.T) {
	schema := []parquet.Column{
		{Name: "key", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c", Type: parquet.TypeInt64, Nullable: true},
	}

	// 15000 unique keys × 1 row each = 15000 rows. Each row is 16 bytes,
	// so 5000 rows per partition × 16 = 80 KB per partition exceeds the
	// flushPartitionBytes (64 KB) threshold and produces multiple chunks
	// per partition file. This was the trigger condition in
	// chunks=1/workers=3 sum_via_ok losing 4% of keys.
	const numRows = 15000

	rng := rand.New(rand.NewSource(42))
	rows := make([]struct {
		key int64
		c   int64
	}, numRows)
	expectedSum := int64(0)
	for i := 0; i < numRows; i++ {
		rows[i].key = int64(i)
		rows[i].c = int64(rng.Intn(10) + 1)
		expectedSum += rows[i].c
	}
	rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })

	// Build batches like HashAggregate.Next would (2048 rows per batch).
	batches := make([]*batch.RecordBatch, 0)
	for off := 0; off < len(rows); off += 2048 {
		end := off + 2048
		if end > len(rows) {
			end = len(rows)
		}
		nRows := end - off
		bb := batch.NewRecordBatch(schema, nRows)
		for i, r := range rows[off:end] {
			bb.Columns[0].Int64Data[i] = r.key
			bb.Columns[1].Int64Data[i] = r.c
		}
		batches = append(batches, bb)
	}

	tmp := t.TempDir()
	sink := newPartitionedShuffleSink(tmp, []string{"key"}, 3, schema)
	ctx := context.Background()
	if err := sink.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	for _, bb := range batches {
		if err := sink.Consume(ctx, bb); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	files := sink.PartitionFiles()
	t.Logf("partition files: %v", files)

	// Path A (control): read partition files directly via shuffleChunkReader.
	controlSum, controlKeys := readSumDirect(t, files)
	if controlSum != expectedSum {
		t.Errorf("CONTROL direct read: sum=%d, want %d", controlSum, expectedSum)
	}
	if controlKeys != numRows {
		t.Errorf("CONTROL direct read: keys=%d, want %d", controlKeys, numRows)
	}

	// Path B (production): upload partition files into a MemStore, then
	// open them via cachedFileStreamSource.openShuffleFile (which
	// streams to local disk and mmaps). spillDir set => stream-to-disk path.
	store := objstore.NewMemStore()
	const bucket = "test"
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	objKeys := make([]string, 0, len(files))
	for p, f := range files {
		if f == "" {
			continue
		}
		data, err := os.ReadFile(filepath.Clean(f))
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("queries/test/partition=%04d/task.wshf", p)
		if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		objKeys = append(objKeys, key)
	}

	exec1 := &Executor{store: store, spillDir: t.TempDir()}
	src := newCachedFileStreamSource(exec1, "", bucket, objKeys)
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	srcSum := int64(0)
	srcKeys := 0
	for {
		bb, err := src.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if bb == nil {
			break
		}
		for i := 0; i < bb.Len; i++ {
			srcSum += bb.Columns[1].Int64Data[i]
			srcKeys++
		}
	}
	if srcSum != expectedSum {
		t.Errorf("via cachedFileStreamSource: sum=%d, want %d (lost %d)", srcSum, expectedSum, expectedSum-srcSum)
	}
	if srcKeys != numRows {
		t.Errorf("via cachedFileStreamSource: keys=%d, want %d (lost %d)", srcKeys, numRows, numRows-srcKeys)
	}

	// Path C: cachedFileStreamSource piped into HashAggregate exactly
	// like the production final-aggregate task.
	hashAgg := exec.NewHashAggregate(
		[]string{"key"},
		[]exec.AggColumn{{
			Func:       exec.AggSum,
			InputCol:   "c",
			OutputCol:  "c",
			OutputType: parquet.TypeInt64,
		}},
	)
	if err := hashAgg.Init(ctx); err != nil {
		t.Fatal(err)
	}
	exec2 := &Executor{store: store, spillDir: t.TempDir()}
	src2 := newCachedFileStreamSource(exec2, "", bucket, objKeys)
	if err := src2.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer src2.Close()
	consumeRows := 0
	for {
		bb, err := src2.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if bb == nil {
			break
		}
		consumeRows += bb.Len
		if err := hashAgg.Consume(ctx, bb); err != nil {
			t.Fatal(err)
		}
	}
	if err := hashAgg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	if consumeRows != numRows {
		t.Errorf("HashAggregate consumed %d rows, want %d", consumeRows, numRows)
	}
	outSum := int64(0)
	outGroups := 0
	for {
		bb, err := hashAgg.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if bb == nil {
			break
		}
		for i := 0; i < bb.Len; i++ {
			outSum += bb.Columns[1].Int64Data[i]
		}
		outGroups += bb.Len
	}
	if outSum != expectedSum {
		t.Errorf("HashAggregate output sum=%d, want %d (lost %d)", outSum, expectedSum, expectedSum-outSum)
	}
	if outGroups != numRows {
		t.Errorf("HashAggregate output groups=%d, want %d (lost %d)", outGroups, numRows, numRows-outGroups)
	}
	t.Logf("via cachedFileStreamSource: %d rows, sum=%d", srcKeys, srcSum)
	t.Logf("HashAggregate over multi-chunk source: %d groups, sum=%d", outGroups, outSum)

	// Path D: feed the same partition files DIRECTLY (no cachedFileStreamSource)
	// to HashAggregate, one shuffleChunkReader per file. Isolates whether the
	// loss is in cachedFileStreamSource specifically vs. crossing file
	// boundaries between separate shuffleChunkReader instances.
	hashAgg2 := exec.NewHashAggregate(
		[]string{"key"},
		[]exec.AggColumn{{
			Func:       exec.AggSum,
			InputCol:   "c",
			OutputCol:  "c",
			OutputType: parquet.TypeInt64,
		}},
	)
	if err := hashAgg2.Init(ctx); err != nil {
		t.Fatal(err)
	}
	directRows := 0
	for _, f := range files {
		if f == "" {
			continue
		}
		data, err := os.ReadFile(filepath.Clean(f))
		if err != nil {
			t.Fatal(err)
		}
		r, err := newShuffleChunkReader(data)
		if err != nil {
			t.Fatal(err)
		}
		for {
			bb, err := r.Next()
			if err != nil {
				t.Fatal(err)
			}
			if bb == nil {
				break
			}
			directRows += bb.Len
			if err := hashAgg2.Consume(ctx, bb); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := hashAgg2.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	directSum := int64(0)
	directGroups := 0
	for {
		bb, err := hashAgg2.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if bb == nil {
			break
		}
		for i := 0; i < bb.Len; i++ {
			directSum += bb.Columns[1].Int64Data[i]
		}
		directGroups += bb.Len
	}
	if directRows != numRows {
		t.Errorf("Path D: direct read fed %d rows, want %d", directRows, numRows)
	}
	if directSum != expectedSum {
		t.Errorf("Path D: direct read HashAggregate sum=%d, want %d (lost %d)",
			directSum, expectedSum, expectedSum-directSum)
	}
	if directGroups != numRows {
		t.Errorf("Path D: direct read HashAggregate groups=%d, want %d (lost %d)",
			directGroups, numRows, numRows-directGroups)
	}
	t.Logf("Path D direct read+HashAggregate: %d rows, %d groups, sum=%d",
		directRows, directGroups, directSum)
}

// readSumDirect reads partition files via shuffleChunkReader and returns the
// total sum of column 1 plus row count. Used to verify file content
// independent of the source/HashAggregate path.

func readSumDirect(t *testing.T, files []string) (sum int64, keys int) {
	t.Helper()
	for _, f := range files {
		if f == "" {
			continue
		}
		data, err := os.ReadFile(filepath.Clean(f))
		if err != nil {
			t.Fatal(err)
		}
		r, err := newShuffleChunkReader(data)
		if err != nil {
			t.Fatal(err)
		}
		for {
			bb, err := r.Next()
			if err != nil {
				t.Fatal(err)
			}
			if bb == nil {
				break
			}
			for i := 0; i < bb.Len; i++ {
				sum += bb.Columns[1].Int64Data[i]
				keys++
			}
		}
	}
	return sum, keys
}
