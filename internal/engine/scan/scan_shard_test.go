package scan

import (
	"bytes"
	"testing"

	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestRowGroupRangeForShard(t *testing.T) {
	tests := []struct {
		name       string
		total      int
		shardCount int
		ranges     [][2]int // expected per-shard [start, end)
	}{
		{
			name:       "shardCount=1 reads all",
			total:      8,
			shardCount: 1,
			ranges:     [][2]int{{0, 8}},
		},
		{
			name:       "evenly divisible",
			total:      8,
			shardCount: 4,
			ranges:     [][2]int{{0, 2}, {2, 4}, {4, 6}, {6, 8}},
		},
		{
			name:       "uneven — last shard absorbs remainder",
			total:      10,
			shardCount: 3,
			ranges:     [][2]int{{0, 3}, {3, 6}, {6, 10}},
		},
		{
			name:       "more shards than row groups",
			total:      3,
			shardCount: 5,
			ranges:     [][2]int{{0, 0}, {0, 1}, {1, 1}, {1, 2}, {2, 3}},
		},
		{
			name:       "single row group, multiple shards — only one shard reads",
			total:      1,
			shardCount: 4,
			ranges:     [][2]int{{0, 0}, {0, 0}, {0, 0}, {0, 1}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Sanity: union of ranges must cover [0, total).
			covered := 0
			for i, want := range tt.ranges {
				gotStart, gotEnd := rowGroupRangeForShard(tt.total, i, tt.shardCount)
				if gotStart != want[0] || gotEnd != want[1] {
					t.Errorf("shard %d: got [%d,%d), want [%d,%d)", i, gotStart, gotEnd, want[0], want[1])
				}
				covered += gotEnd - gotStart
			}
			if covered != tt.total {
				t.Errorf("union of shard ranges = %d row groups, want %d", covered, tt.total)
			}
		})
	}
}

// TestReadFileBatchesShard_ReadsDisjointRowGroups builds a parquet file with
// 4 row groups (each with a unique value range), then verifies each shard
// reads its assigned subset and the union equals the full file.
func TestReadFileBatchesShard_ReadsDisjointRowGroups(t *testing.T) {
	schema := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
	}

	// Write 4 row groups, each with 100 rows. Force RowGroupSize=100 so each
	// 100-row chunk emits its own row group.
	cfg := pqt.DefaultWriterConfig()
	cfg.RowGroupSize = 100

	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, pqt.Schema{Columns: schema}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	const rowsPerGroup = 100
	const numGroups = 4
	for g := 0; g < numGroups; g++ {
		rows := make([]map[string]any, rowsPerGroup)
		for i := range rows {
			rows[i] = map[string]any{
				// Tag rows with g*1000+i so we can verify which group each came from.
				"id": int64(g*1000 + i),
			}
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatalf("write group %d: %v", g, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	open := func() *pqt.Reader {
		r, err := pqt.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	// Sanity: the file should have exactly numGroups row groups.
	r := open()
	if got := r.NumRowGroups(); got != numGroups {
		t.Fatalf("test fixture: expected %d row groups, got %d", numGroups, got)
	}

	// Read each shard and collect ids.
	const shardCount = 4
	seen := make(map[int64]bool)
	totalRows := 0
	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		batches, err := ReadFileBatchesShard(open(), schema, nil, shardIdx, shardCount)
		if err != nil {
			t.Fatalf("shard %d: %v", shardIdx, err)
		}
		for _, b := range batches {
			for i := 0; i < b.Len; i++ {
				id := b.Columns[0].Int64Data[i]
				if seen[id] {
					t.Errorf("shard %d: duplicate id %d", shardIdx, id)
				}
				seen[id] = true
				totalRows++
			}
		}
	}

	wantTotal := numGroups * rowsPerGroup
	if totalRows != wantTotal {
		t.Errorf("union of shards: got %d rows, want %d", totalRows, wantTotal)
	}
	for g := 0; g < numGroups; g++ {
		for i := 0; i < rowsPerGroup; i++ {
			id := int64(g*1000 + i)
			if !seen[id] {
				t.Errorf("missing id %d (group %d, row %d) — shards lost rows", id, g, i)
			}
		}
	}
}

// TestReadFileBatchesShard_BackwardCompatible ensures shardCount=1 reads the
// whole file identically to the legacy ReadFileBatches.
func TestReadFileBatchesShard_BackwardCompatible(t *testing.T) {
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	cfg := pqt.DefaultWriterConfig()
	cfg.RowGroupSize = 50
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, pqt.Schema{Columns: schema}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 200) // 4 row groups
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i)}
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r1, err := pqt.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	wholeBatches, err := ReadFileBatches(r1, schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	wholeRows := 0
	for _, b := range wholeBatches {
		wholeRows += b.Len
	}

	r2, err := pqt.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	shardBatches, err := ReadFileBatchesShard(r2, schema, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	shardRows := 0
	for _, b := range shardBatches {
		shardRows += b.Len
	}

	if wholeRows != shardRows {
		t.Errorf("ReadFileBatchesShard(0,1) read %d rows; ReadFileBatches read %d — must match",
			shardRows, wholeRows)
	}
	if shardRows != 200 {
		t.Errorf("expected 200 rows, got %d", shardRows)
	}
}
