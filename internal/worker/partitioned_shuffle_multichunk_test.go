package worker

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestPartitionedShuffleMultiChunkRoundTrip is a row-loss localizer for the
// distributed Q17 bug. The hypothesis is that partition files which exceed
// flushPartitionBytes (64 KB) produce multiple WSHF chunks per partition, and
// some path on the multi-chunk write or read side drops data.
//
// Setup mirrors the partial-aggregate output that drives the failing
// chunks=1/workers=3 case: 15000 unique (key, count) rows of (int64, int64)
// = 16 bytes each = ~80 KB per partition with 3 partitions. That hits
// flushPartition during Consume (multiple chunks per partition file),
// whereas a 2000-key version (which passes the integration test) stays
// under flushBytes and produces a single chunk per partition.
func TestPartitionedShuffleMultiChunkRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		numRows int
	}{
		// Mirrors groups_pk (no loss observed): 2000 keys × 1 row
		// per key => 2000 rows total => ~666 rows per partition
		// => ~8 KB per partition, single-chunk per file.
		{"single_chunk_2000", 2000},
		// Mirrors groups_ok (loss observed): 15000 keys × 1 row
		// per key => 15000 rows => ~5000 rows per partition
		// => ~80 KB per partition, multi-chunk per file.
		{"multi_chunk_15000", 15000},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			schema := []parquet.Column{
				{Name: "key", Type: parquet.TypeInt64, Nullable: true},
				{Name: "c", Type: parquet.TypeInt64, Nullable: true},
			}

			rng := rand.New(rand.NewSource(42))
			type row struct {
				key int64
				c   int64
			}
			rows := make([]row, c.numRows)
			expectedSum := int64(0)
			expectedKeys := make(map[int64]int64, c.numRows)
			for i := 0; i < c.numRows; i++ {
				rows[i].key = int64(i)
				rows[i].c = int64(rng.Intn(10) + 1)
				expectedSum += rows[i].c
				expectedKeys[rows[i].key] = rows[i].c
			}
			rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })

			// Build batches like HashAggregate.Next would: 2048 rows per batch.
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
			if len(files) != 3 {
				t.Fatalf("expected 3 partition files, got %d", len(files))
			}
			gotKeys := make(map[int64]int64)
			gotSum := int64(0)
			gotRows := 0
			gotChunks := 0
			for p, f := range files {
				if f == "" {
					t.Logf("partition %d: empty", p)
					continue
				}
				data, err := os.ReadFile(filepath.Clean(f))
				if err != nil {
					t.Fatal(err)
				}
				r, err := newShuffleChunkReader(data)
				if err != nil {
					t.Fatalf("partition %d: %v", p, err)
				}
				partRows := 0
				partChunks := 0
				for {
					bb, err := r.Next()
					if err != nil {
						t.Fatalf("partition %d: %v", p, err)
					}
					if bb == nil {
						break
					}
					partRows += bb.Len
					partChunks++
					for i := 0; i < bb.Len; i++ {
						k := bb.Columns[0].Int64Data[i]
						v := bb.Columns[1].Int64Data[i]
						if existing, ok := gotKeys[k]; ok {
							t.Errorf("duplicate key %d (existing=%d, new=%d) — partition collision",
								k, existing, v)
						}
						gotKeys[k] = v
						gotSum += v
						gotRows++
					}
				}
				gotChunks += partChunks
				t.Logf("partition %d: %d rows in %d chunks", p, partRows, partChunks)
			}
			if gotRows != c.numRows {
				t.Errorf("rows: got %d, want %d (lost %d)", gotRows, c.numRows, c.numRows-gotRows)
			}
			if gotSum != expectedSum {
				t.Errorf("sum: got %d, want %d (lost %d)", gotSum, expectedSum, expectedSum-gotSum)
			}
			if len(gotKeys) != len(expectedKeys) {
				t.Errorf("unique keys: got %d, want %d", len(gotKeys), len(expectedKeys))
			}
			missingKeys := 0
			for k := range expectedKeys {
				if _, ok := gotKeys[k]; !ok {
					missingKeys++
				}
			}
			if missingKeys > 0 {
				t.Errorf("missing keys: %d", missingKeys)
			}
			t.Logf("total: %d rows in %d chunks across 3 partitions", gotRows, gotChunks)
		})
	}
}
