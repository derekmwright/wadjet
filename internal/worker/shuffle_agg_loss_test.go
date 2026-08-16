package worker

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestShuffleReadIntoHashAggregateMultipleReaders splits 15000 unique keys
// across 3 separate shuffleChunkReader instances (no partitionedShuffleSink,
// just hand-rolled sharding into 3 writers), and feeds them sequentially
// to a single HashAggregate. If this passes, the bug is specific to the
// way partitionedShuffleSink lays bytes out; if it fails, the kernel itself
// loses keys whenever input batches arrive from multiple readers.
func TestShuffleReadIntoHashAggregateMultipleReaders(t *testing.T) {
	schema := []parquet.Column{
		{Name: "key", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c", Type: parquet.TypeInt64, Nullable: true},
	}

	const numKeys = 15000
	rng := rand.New(rand.NewSource(42))
	rows := make([]struct {
		key int64
		c   int64
	}, numKeys)
	totalSum := int64(0)
	for i := 0; i < numKeys; i++ {
		rows[i].key = int64(i)
		rows[i].c = int64(rng.Intn(10) + 1)
		totalSum += rows[i].c
	}
	rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })

	// Round-robin shard rows into 3 buffers, each becomes a separate
	// shuffleChunkReader. No hash partitioning; round-robin means the union
	// of buffer contents is exactly all 15000 input rows with no dups and
	// no overlap between buffers.
	const numReaders = 3
	buffers := make([]*bytes.Buffer, numReaders)
	writers := make([]*shuffleWriter, numReaders)
	chunkCounts := make([]uint32, numReaders)
	for i := range buffers {
		buffers[i] = &bytes.Buffer{}
		writers[i] = newShuffleWriter(buffers[i], schema)
		if err := writers[i].writeHeader(); err != nil {
			t.Fatal(err)
		}
	}
	// 2048-row batches, round-robin to writer.
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
		w := (off / 2048) % numReaders
		if err := writers[w].writeChunk(bb.Columns, nil, nRows); err != nil {
			t.Fatal(err)
		}
		chunkCounts[w]++
	}
	// Patch per-writer chunk counts.
	datas := make([][]byte, numReaders)
	for i := range buffers {
		d := buffers[i].Bytes()
		d[4] = byte(chunkCounts[i])
		d[5] = byte(chunkCounts[i] >> 8)
		d[6] = byte(chunkCounts[i] >> 16)
		d[7] = byte(chunkCounts[i] >> 24)
		datas[i] = d
	}

	hashAgg := exec.NewHashAggregate(
		[]string{"key"},
		[]exec.AggColumn{{
			Func:       exec.AggSum,
			InputCol:   "c",
			OutputCol:  "c",
			OutputType: parquet.TypeInt64,
		}},
	)
	if err := hashAgg.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	consumed := 0
	for _, d := range datas {
		r, err := newShuffleChunkReader(d)
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
			consumed += bb.Len
			if err := hashAgg.Consume(context.Background(), bb); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := hashAgg.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if consumed != numKeys {
		t.Fatalf("consumed=%d want %d", consumed, numKeys)
	}

	outSum := int64(0)
	outGroups := 0
	for {
		bb, err := hashAgg.Next(context.Background())
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
	if outGroups != numKeys {
		t.Errorf("groups: got %d, want %d (lost %d)", outGroups, numKeys, numKeys-outGroups)
	}
	if outSum != totalSum {
		t.Errorf("sum: got %d, want %d (lost %d)", outSum, totalSum, totalSum-outSum)
	}
	t.Logf("multi-reader 15000 unique keys round-robin: %d groups, sum=%d", outGroups, outSum)
}

// TestShuffleReadIntoHashAggregateNoLossUniqueKeys models the failing
// distributed sum_via_ok shape: 15000 unique keys, 1 row per key, fed
// through a SINGLE shuffleChunkReader's chunks. This isolates whether
// the loss is from "multi-reader" file boundaries OR cumulative
// cardinality crossing the hash-table rehash threshold.
func TestShuffleReadIntoHashAggregateNoLossUniqueKeys(t *testing.T) {
	schema := []parquet.Column{
		{Name: "key", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c", Type: parquet.TypeInt64, Nullable: true},
	}

	const numKeys = 15000
	rng := rand.New(rand.NewSource(42))
	rows := make([]struct {
		key int64
		c   int64
	}, numKeys)
	totalSum := int64(0)
	for i := 0; i < numKeys; i++ {
		rows[i].key = int64(i)
		rows[i].c = int64(rng.Intn(10) + 1)
		totalSum += rows[i].c
	}
	rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })

	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}
	chunkSize := 2048
	for off := 0; off < len(rows); off += chunkSize {
		end := off + chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		nRows := end - off
		bb := batch.NewRecordBatch(schema, nRows)
		for i, r := range rows[off:end] {
			bb.Columns[0].Int64Data[i] = r.key
			bb.Columns[1].Int64Data[i] = r.c
		}
		if err := sw.writeChunk(bb.Columns, nil, nRows); err != nil {
			t.Fatal(err)
		}
	}
	data := buf.Bytes()
	data[4] = byte(sw.numChunks)
	data[5] = byte(sw.numChunks >> 8)
	data[6] = byte(sw.numChunks >> 16)
	data[7] = byte(sw.numChunks >> 24)

	hashAgg := exec.NewHashAggregate(
		[]string{"key"},
		[]exec.AggColumn{{
			Func:       exec.AggSum,
			InputCol:   "c",
			OutputCol:  "c",
			OutputType: parquet.TypeInt64,
		}},
	)
	if err := hashAgg.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	r, err := newShuffleChunkReader(data)
	if err != nil {
		t.Fatal(err)
	}
	consumed := 0
	for {
		bb, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if bb == nil {
			break
		}
		consumed += bb.Len
		if err := hashAgg.Consume(context.Background(), bb); err != nil {
			t.Fatal(err)
		}
	}
	if err := hashAgg.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if consumed != numKeys {
		t.Fatalf("consumed=%d want %d", consumed, numKeys)
	}

	outSum := int64(0)
	outGroups := 0
	for {
		bb, err := hashAgg.Next(context.Background())
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
	if outGroups != numKeys {
		t.Errorf("groups: got %d, want %d (lost %d)", outGroups, numKeys, numKeys-outGroups)
	}
	if outSum != totalSum {
		t.Errorf("sum: got %d, want %d (lost %d)", outSum, totalSum, totalSum-outSum)
	}
	t.Logf("single-reader 15000 unique keys: %d groups, sum=%d", outGroups, outSum)
}

// TestShuffleReadIntoHashAggregateNoLoss isolates whether the Q17 distributed
// row-loss bug is exposed when the input batches reach HashAggregate via the
// shuffle read path (vs. SliceSource as in agg_merge_repro_test.go).
//
// Setup mirrors the distributed final_aggregate input shape:
//   - 2000 distinct l_partkey × 8 partials = 16,000 (l_partkey, c) rows
//   - rows are shuffle-shuffled (not the .wshf shuffle, just rng.Shuffle)
//   - written through shuffleWriter as multiple chunks with NO selection
//     vector (plain partitioned-shuffle output uses sel=nil at flush time)
//   - read back via shuffleChunkReader and fed batch-by-batch to a single
//     HashAggregate configured exactly like the worker's mergeMode path:
//     GROUP BY l_partkey, SUM(c) where c was the partial COUNT(*) output
//
// If this test passes, the kernel itself is correct under shuffle-decoded
// batches and the bug must live in coordination (which final task gets which
// chunk, ordering, or boundary handling). If it fails, we have a kernel-level
// regression triggered by the specific shape of shuffle-decoded batches.
func TestShuffleReadIntoHashAggregateNoLoss(t *testing.T) {
	schema := []parquet.Column{
		{Name: "l_partkey", Type: parquet.TypeInt32, Nullable: true},
		{Name: "c", Type: parquet.TypeInt64, Nullable: true},
	}

	const numKeys = 2000
	const repsPerKey = 8
	totalRows := numKeys * repsPerKey

	rng := rand.New(rand.NewSource(42))
	type row struct {
		key int32
		c   int64
	}
	rows := make([]row, 0, totalRows)
	totalSum := int64(0)
	for k := 0; k < numKeys; k++ {
		for r := 0; r < repsPerKey; r++ {
			c := int64(rng.Intn(10) + 1)
			totalSum += c
			rows = append(rows, row{key: int32(k), c: c})
		}
	}
	rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })

	// Write rows through shuffleWriter in multiple chunks. Use sel=nil so this
	// matches the partitioned_shuffle_sink flush path (which always passes
	// sel=nil to writeChunk after gathering rows into a per-partition buffer).
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}
	chunkSize := 2048
	for off := 0; off < len(rows); off += chunkSize {
		end := off + chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		nRows := end - off
		b := batch.NewRecordBatch(schema, nRows)
		for i, r := range rows[off:end] {
			b.Columns[0].Int32Data[i] = r.key
			b.Columns[1].Int64Data[i] = r.c
		}
		if err := sw.writeChunk(b.Columns, nil, nRows); err != nil {
			t.Fatalf("writeChunk: %v", err)
		}
	}
	data := buf.Bytes()
	// Patch chunk count.
	data[4] = byte(sw.numChunks)
	data[5] = byte(sw.numChunks >> 8)
	data[6] = byte(sw.numChunks >> 16)
	data[7] = byte(sw.numChunks >> 24)

	// Now read back via shuffleChunkReader and feed to a HashAggregate
	// configured identically to the distributed final_aggregate path:
	// mergeMode rewrites COUNT→SUM and InputCol → OutputCol, so we model
	// that here as Func=Sum, InputCol=OutputCol="c".
	hashAgg := exec.NewHashAggregate(
		[]string{"l_partkey"},
		[]exec.AggColumn{{
			Func:       exec.AggSum,
			InputCol:   "c",
			OutputCol:  "c",
			OutputType: parquet.TypeInt64,
		}},
	)
	if err := hashAgg.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	r, err := newShuffleChunkReader(data)
	if err != nil {
		t.Fatal(err)
	}
	var consumedRows int
	var consumedBatches int
	for {
		bb, err := r.Next()
		if err != nil {
			t.Fatalf("reader.Next: %v", err)
		}
		if bb == nil {
			break
		}
		consumedRows += bb.Len
		consumedBatches++
		if err := hashAgg.Consume(context.Background(), bb); err != nil {
			t.Fatal(err)
		}
	}
	if err := hashAgg.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if consumedRows != totalRows {
		t.Errorf("shuffle reader emitted %d rows, want %d", consumedRows, totalRows)
	}

	// Drain output and sum c across all groups.
	var outSum int64
	var outGroups int
	for {
		bb, err := hashAgg.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if bb == nil {
			break
		}
		cIdx := -1
		for i, col := range bb.Schema {
			if col.Name == "c" {
				cIdx = i
				break
			}
		}
		if cIdx < 0 {
			t.Fatal("output missing c column")
		}
		col := bb.Columns[cIdx]
		for i := 0; i < bb.Len; i++ {
			outSum += col.Int64Data[i]
		}
		outGroups += bb.Len
	}
	if outGroups != numKeys {
		t.Errorf("output groups: got %d, want %d", outGroups, numKeys)
	}
	if outSum != totalSum {
		t.Errorf("output SUM(c): got %d, want %d (lost %d)", outSum, totalSum, totalSum-outSum)
	}
	t.Logf("rows=%d (in %d batches), groups=%d, sum=%d", consumedRows, consumedBatches, outGroups, outSum)
}
