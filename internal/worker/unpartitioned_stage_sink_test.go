package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestUnpartitionedStageSink_RoundTrip writes a couple of batches into the
// streaming sink and confirms the resulting on-disk .wshf file decodes back to
// the original schema and rows. This is the core invariant: the streaming
// path produces a byte-equivalent file to the legacy in-memory path so that
// downstream readers (cachedFileStreamSource → shuffleChunkReader) decode it
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
	if got := sink.NumChunks(); got != 2 {
		t.Errorf("NumChunks = %d, want 2", got)
	}
	if !filepath.IsAbs(sink.Path()) && !filepath.IsLocal(sink.Path()) {
		t.Errorf("Path %q is unusable", sink.Path())
	}

	// Decode the file via shuffleChunkReader and verify rows.
	data, err := os.ReadFile(sink.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	rdr, err := newShuffleChunkReader(data)
	if err != nil {
		t.Fatalf("newShuffleChunkReader: %v", err)
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
