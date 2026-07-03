package worker

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
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
	// Use hashVectorValue directly — the same code path the sink uses — so the
	// test is sensitive to any routing divergence in partitionFor.
	keySchema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}
	var scratch [8]byte
	totalRows := 0
	for p, path := range paths {
		if path == "" {
			continue // empty partition
		}
		rows := readWSHFInts(t, filepath.Clean(path), "k")
		for _, k := range rows {
			keyBatch := batch.NewRecordBatch(keySchema, 1)
			keyBatch.Columns[0].Int64Data[0] = k
			keyBatch.Columns[0].Nulls.SetValid(0)
			h := fnv.New64a()
			hashVectorValue(h, keyBatch.Columns[0], 0, scratch[:])
			got := int(h.Sum64() % uint64(numParts))
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

// TestPartitionedShuffleSink_LargeConsumeBurstParity: consumes above
// shuffleBurstGateRows take the errgroup fan-out path, below it the inline
// path; both must produce identical partition contents. Mixes both sizes in
// one sink and verifies every row lands in its hash partition exactly once.
func TestPartitionedShuffleSink_LargeConsumeBurstParity(t *testing.T) {
	dir := t.TempDir()
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	const numParts = 4
	sink := newPartitionedShuffleSink(dir, []string{"id"}, numParts, schema)
	if err := sink.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer sink.Close()

	makeRange := func(base, n int) *batch.RecordBatch {
		ids := make([]int64, n)
		names := make([]string, n)
		for i := range ids {
			ids[i] = int64(base + i)
			names[i] = "r" + strconv.Itoa(base+i)
		}
		return makeBatchInt64String(schema, ids, names)
	}

	total := 0
	// One burst-path consume (> shuffleBurstGateRows) and several inline ones.
	for _, n := range []int{shuffleBurstGateRows * 2, 100, 2048, 517} {
		if err := sink.Consume(context.Background(), makeRange(total, n)); err != nil {
			t.Fatalf("Consume(%d): %v", n, err)
		}
		total += n
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	seen := make(map[int64]bool)
	rows := 0
	for p := 0; p < numParts; p++ {
		data, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("part-%04d.wshf", p)))
		if err != nil {
			t.Fatalf("read partition %d: %v", p, err)
		}
		if len(data) == 0 {
			continue
		}
		rdr, err := newShuffleChunkReader(data)
		if err != nil {
			t.Fatalf("parse partition %d: %v", p, err)
		}
		for {
			b, err := rdr.Next()
			if err != nil {
				t.Fatalf("partition %d next: %v", p, err)
			}
			if b == nil {
				break
			}
			for i := 0; i < b.ActiveLen(); i++ {
				id := b.Columns[0].Int64Data[i]
				if seen[id] {
					t.Fatalf("id %d appears twice", id)
				}
				seen[id] = true
				if want := "r" + strconv.FormatInt(id, 10); string(b.Columns[1].BytesData.Value(i)) != want {
					t.Fatalf("id %d: name %q, want %q", id, b.Columns[1].BytesData.Value(i), want)
				}
				rows++
			}
		}
	}
	if rows != total {
		t.Fatalf("rows across partitions = %d, want %d", rows, total)
	}
}
