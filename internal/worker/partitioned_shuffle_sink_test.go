package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestPartitionedShuffleSink_RoundTrip verifies that rows hash-partitioned
// across N output files can be read back, that no row is lost, and that
// every row in partition p hashes to p.
func TestPartitionedShuffleSink_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	const numParts = 4

	schema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}
	sink := newPartitionedShuffleSink(dir, []string{"k"}, numParts, schema)
	if err := sink.Init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Build a batch of 1000 rows with sequential int64 keys.
	const n = 1000
	b := batch.NewRecordBatch(schema, n)
	for i := 0; i < n; i++ {
		b.Columns[0].Int64Data[i] = int64(i)
		b.Columns[0].Nulls.SetValid(i)
	}

	if err := sink.Consume(context.Background(), b); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	paths := sink.PartitionFiles()
	if len(paths) != numParts {
		t.Fatalf("expected %d partition files, got %d", numParts, len(paths))
	}

	// Read each partition back and verify every row's hash maps back to its partition.
	totalRows := 0
	for p, path := range paths {
		if path == "" {
			continue // empty partition
		}
		rows := readWSHFInts(t, filepath.Clean(path), "k")
		for _, k := range rows {
			got := int(hashInt64(k) % uint64(numParts))
			if got != p {
				t.Errorf("row k=%d ended up in partition %d, hash maps to %d", k, p, got)
			}
		}
		totalRows += len(rows)
	}
	if totalRows != n {
		t.Errorf("total rows across partitions = %d, want %d", totalRows, n)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestPartitionedShuffleSink_MultiColumn verifies that multi-key hashing
// assigns rows consistently and all rows are accounted for.
func TestPartitionedShuffleSink_MultiColumn(t *testing.T) {
	dir := t.TempDir()
	const numParts = 3

	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeString},
	}
	sink := newPartitionedShuffleSink(dir, []string{"a", "b"}, numParts, schema)
	if err := sink.Init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}

	const n = 100
	b := batch.NewRecordBatch(schema, n)
	for i := 0; i < n; i++ {
		b.Columns[0].Int64Data[i] = int64(i)
		b.Columns[0].Nulls.SetValid(i)
		b.Columns[1].BytesData.Set(i, []byte("val"))
		b.Columns[1].Nulls.SetValid(i)
	}

	if err := sink.Consume(context.Background(), b); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	paths := sink.PartitionFiles()
	total := 0
	for _, path := range paths {
		if path == "" {
			continue
		}
		rows := readWSHFInts(t, path, "a")
		total += len(rows)
	}
	if total != n {
		t.Errorf("total rows = %d, want %d", total, n)
	}
}

// TestPartitionedShuffleSink_EmptyPartitions verifies that PartitionFiles
// returns "" for partitions that received no rows.
func TestPartitionedShuffleSink_EmptyPartitions(t *testing.T) {
	dir := t.TempDir()
	const numParts = 8

	// Use a single row: only one partition gets a row, rest are empty.
	schema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}
	sink := newPartitionedShuffleSink(dir, []string{"k"}, numParts, schema)
	if err := sink.Init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}

	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].Int64Data[0] = int64(42)
	b.Columns[0].Nulls.SetValid(0)

	if err := sink.Consume(context.Background(), b); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	paths := sink.PartitionFiles()
	nonEmpty := 0
	for _, p := range paths {
		if p != "" {
			nonEmpty++
		}
	}
	if nonEmpty != 1 {
		t.Errorf("expected 1 non-empty partition, got %d", nonEmpty)
	}
}

// TestPartitionedShuffleSink_SelectionVector verifies that when the input
// batch has a selection vector, only active rows are written.
func TestPartitionedShuffleSink_SelectionVector(t *testing.T) {
	dir := t.TempDir()
	const numParts = 4

	schema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}
	sink := newPartitionedShuffleSink(dir, []string{"k"}, numParts, schema)
	if err := sink.Init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Batch of 10 rows, select only rows 0, 2, 4, 6 (4 active).
	b := batch.NewRecordBatch(schema, 10)
	for i := 0; i < 10; i++ {
		b.Columns[0].Int64Data[i] = int64(i * 10)
		b.Columns[0].Nulls.SetValid(i)
	}
	b.Sel = []uint32{0, 2, 4, 6}

	if err := sink.Consume(context.Background(), b); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	paths := sink.PartitionFiles()
	total := 0
	for _, path := range paths {
		if path == "" {
			continue
		}
		rows := readWSHFInts(t, path, "k")
		total += len(rows)
	}
	if total != 4 {
		t.Errorf("expected 4 rows (selection vector), got %d", total)
	}
}

// readWSHFInts reads back int64 values from the named column in a WSHF file.
func readWSHFInts(t *testing.T, path, colName string) []int64 {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("readWSHFInts: open %s: %v", path, err)
	}
	if len(data) == 0 {
		return nil
	}
	r, err := newShuffleChunkReader(data)
	if err != nil {
		t.Fatalf("readWSHFInts: parse %s: %v", path, err)
	}
	// Find column index by name.
	colIdx := -1
	for i, col := range r.schema {
		if col.Name == colName {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		t.Fatalf("readWSHFInts: column %q not found in %s", colName, path)
	}
	var out []int64
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatalf("readWSHFInts: read chunk from %s: %v", path, err)
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
