package worker

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/wshf"
)

// TestUnpartitionedStageSink_RoundTrip writes a couple of batches into the
// streaming sink and confirms the resulting on-disk .wshf file decodes back to
// the original schema and rows. This is the core invariant: the streaming
// path produces a byte-equivalent file to the legacy in-memory path so that
// downstream readers (cachedFileStreamSource → wshf.ChunkReader) decode it
// identically.
func TestUnpartitionedStageSink_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	sink := newUnpartitionedStageSink(dir, "task-1")
	if err := sink.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer sink.Close()

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}

	b1 := makeBatchInt64String(schema, []int64{1, 2, 3}, []string{"a", "b", "c"})
	b2 := makeBatchInt64String(schema, []int64{4, 5}, []string{"d", "e"})

	if err := sink.Consume(context.Background(), b1); err != nil {
		t.Fatalf("Consume b1: %v", err)
	}
	if err := sink.Consume(context.Background(), b2); err != nil {
		t.Fatalf("Consume b2: %v", err)
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	if got := sink.TotalRows(); got != 5 {
		t.Errorf("TotalRows = %d, want 5", got)
	}
	// Chunk size is the sink's decision, not an echo of consume granularity:
	// both small consumes coalesce into one chunk (morsel-execution.md
	// §4.1.1 — chunk-per-consume fragmented stage outputs under
	// morsel-parallel callers).
	if got := sink.NumChunks(); got != 1 {
		t.Errorf("NumChunks = %d, want 1 (coalesced)", got)
	}
	if !filepath.IsAbs(sink.Path()) && !filepath.IsLocal(sink.Path()) {
		t.Errorf("Path %q is unusable", sink.Path())
	}

	// Decode the file via wshf.ChunkReader and verify rows.
	data, err := os.ReadFile(sink.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	rdr, err := wshf.NewChunkReader(data)
	if err != nil {
		t.Fatalf("wshf.NewChunkReader: %v", err)
	}

	var totalRows int
	for {
		got, err := rdr.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if got == nil {
			break
		}
		totalRows += got.ActiveLen()
	}
	if totalRows != 5 {
		t.Errorf("decoded total rows = %d, want 5", totalRows)
	}
}

// TestUnpartitionedStageSink_EmptyConsume confirms that consuming zero rows
// (either nil batches or batches with ActiveLen()=0) does not write any
// chunks and Finalize remains valid (stage-level "filter rejected every row"
// case).
func TestUnpartitionedStageSink_EmptyConsume(t *testing.T) {
	dir := t.TempDir()
	sink := newUnpartitionedStageSink(dir, "task-empty")
	if err := sink.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer sink.Close()

	if err := sink.Consume(context.Background(), nil); err != nil {
		t.Fatalf("Consume(nil): %v", err)
	}
	schema := []parquet.Column{{Name: "x", Type: parquet.TypeInt64}}
	emptyBatch := batch.NewRecordBatch(schema, 0)
	if err := sink.Consume(context.Background(), emptyBatch); err != nil {
		t.Fatalf("Consume(empty): %v", err)
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if got := sink.NumChunks(); got != 0 {
		t.Errorf("NumChunks = %d, want 0", got)
	}
	if got := sink.TotalRows(); got != 0 {
		t.Errorf("TotalRows = %d, want 0", got)
	}
}

// makeBatchInt64String constructs a 2-column batch with int64 + string data.
// Local test helper to avoid depending on test fixtures elsewhere.
func makeBatchInt64String(schema []parquet.Column, ids []int64, names []string) *batch.RecordBatch {
	if len(ids) != len(names) {
		panic("makeBatchInt64String: mismatched lengths")
	}
	b := batch.NewRecordBatch(schema, len(ids))
	b.Len = len(ids)
	b.Columns[0].Int64Data = append(b.Columns[0].Int64Data[:0], ids...)
	for i, name := range names {
		b.Columns[1].BytesData.Set(i, []byte(name))
	}
	return b
}

// TestUnpartitionedStageSink_CoalesceValueParity: many small Sel'd consumes
// (the morsel-parallel shape) coalesce into threshold-sized chunks; decoded
// rows must equal exactly the ACTIVE rows of every consume, in consume order.
func TestUnpartitionedStageSink_CoalesceValueParity(t *testing.T) {
	dir := t.TempDir()
	sink := newUnpartitionedStageSink(dir, "task-coalesce")
	if err := sink.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer sink.Close()

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}

	var wantIDs []int64
	var wantNames []string
	next := int64(0)
	for c := 0; c < 40; c++ {
		n := 100 + c
		ids := make([]int64, n)
		names := make([]string, n)
		for i := range ids {
			ids[i] = next
			names[i] = "row-" + strconv.FormatInt(next, 10)
			next++
		}
		b := makeBatchInt64String(schema, ids, names)
		// Odd consumes carry a Sel keeping only even positions — the sink
		// must copy exactly the active rows.
		if c%2 == 1 {
			sel := make([]uint32, 0, n/2)
			for i := 0; i < n; i += 2 {
				sel = append(sel, uint32(i))
			}
			b.Sel = sel
			for _, i := range sel {
				wantIDs = append(wantIDs, ids[i])
				wantNames = append(wantNames, names[i])
			}
		} else {
			wantIDs = append(wantIDs, ids...)
			wantNames = append(wantNames, names...)
		}
		if err := sink.Consume(context.Background(), b); err != nil {
			t.Fatalf("Consume %d: %v", c, err)
		}
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if got := sink.TotalRows(); got != int64(len(wantIDs)) {
		t.Fatalf("TotalRows = %d, want %d", got, len(wantIDs))
	}
	if got := sink.NumChunks(); got != 1 {
		t.Fatalf("NumChunks = %d, want 1 (all consumes below flush thresholds)", got)
	}

	data, err := os.ReadFile(sink.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	rdr, err := wshf.NewChunkReader(data)
	if err != nil {
		t.Fatalf("wshf.NewChunkReader: %v", err)
	}
	var gotIDs []int64
	var gotNames []string
	for {
		got, err := rdr.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if got == nil {
			break
		}
		for i := 0; i < got.ActiveLen(); i++ {
			gotIDs = append(gotIDs, got.Columns[0].Int64Data[i])
			gotNames = append(gotNames, string(got.Columns[1].BytesData.Value(i)))
		}
	}
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("decoded rows = %d, want %d", len(gotIDs), len(wantIDs))
	}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] || gotNames[i] != wantNames[i] {
			t.Fatalf("row %d: got (%d,%q), want (%d,%q)", i, gotIDs[i], gotNames[i], wantIDs[i], wantNames[i])
		}
	}
}

// TestUnpartitionedStageSink_CoalesceFlushThreshold: crossing the row
// threshold mid-stream produces multiple chunks with no row loss or
// reordering.
func TestUnpartitionedStageSink_CoalesceFlushThreshold(t *testing.T) {
	dir := t.TempDir()
	sink := newUnpartitionedStageSink(dir, "task-flush")
	if err := sink.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer sink.Close()

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	const per = 8192
	const consumes = 10 // 81920 rows > unpartitionedFlushRows (65536)
	next := int64(0)
	for c := 0; c < consumes; c++ {
		ids := make([]int64, per)
		names := make([]string, per)
		for i := range ids {
			ids[i] = next
			names[i] = "n"
			next++
		}
		if err := sink.Consume(context.Background(), makeBatchInt64String(schema, ids, names)); err != nil {
			t.Fatalf("Consume %d: %v", c, err)
		}
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if got := sink.NumChunks(); got != 2 {
		t.Fatalf("NumChunks = %d, want 2 (65536-row flush + 16384-row remainder)", got)
	}
	data, err := os.ReadFile(sink.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	rdr, err := wshf.NewChunkReader(data)
	if err != nil {
		t.Fatalf("wshf.NewChunkReader: %v", err)
	}
	var got []int64
	for {
		b, err := rdr.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		for i := 0; i < b.ActiveLen(); i++ {
			got = append(got, b.Columns[0].Int64Data[i])
		}
	}
	if len(got) != per*consumes {
		t.Fatalf("decoded rows = %d, want %d", len(got), per*consumes)
	}
	for i, id := range got {
		if id != int64(i) {
			t.Fatalf("row %d: id = %d, want %d (order must survive coalescing)", i, id, i)
		}
	}
}
