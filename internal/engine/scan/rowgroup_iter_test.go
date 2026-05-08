package scan

import (
	"bytes"
	"testing"

	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

// fourRowGroupFile builds an in-memory parquet file with 4 row groups of 100
// rows each, ids tagged g*1000+i so callers can verify which group each row
// came from. Mirrors the fixture in scan_shard_test.go.
func fourRowGroupFile(t *testing.T) []byte {
	t.Helper()
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}
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
			rows[i] = map[string]any{"id": int64(g*1000 + i)}
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatalf("write group %d: %v", g, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestRowGroupIter_YieldsOneRowGroupPerNext verifies the iterator returns
// exactly one row group per Next call (not the whole file at once). The
// proof of streaming-vs-eager: after collecting one batch, the row count
// equals the row-group size — not the file total.
func TestRowGroupIter_YieldsOneRowGroupPerNext(t *testing.T) {
	data := fourRowGroupFile(t)
	reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	it, err := OpenRowGroupIter(reader, schema, nil, 0, 1)
	if err != nil {
		t.Fatalf("OpenRowGroupIter: %v", err)
	}
	defer it.Close()

	const rowsPerGroup = 100
	const numGroups = 4

	for g := 0; g < numGroups; g++ {
		b, err := it.Next()
		if err != nil {
			t.Fatalf("Next #%d: %v", g, err)
		}
		if b == nil {
			t.Fatalf("Next #%d: expected batch, got nil", g)
		}
		if b.Len != rowsPerGroup {
			t.Errorf("Next #%d: batch has %d rows, want %d (one row group)",
				g, b.Len, rowsPerGroup)
		}
		// First row's id reveals which group: g*1000.
		gotID := b.Columns[0].Int64Data[0]
		if gotID != int64(g*1000) {
			t.Errorf("Next #%d: first id = %d, want %d", g, gotID, g*1000)
		}
	}

	// One more call returns (nil, nil).
	b, err := it.Next()
	if err != nil {
		t.Fatalf("Next after exhaustion: %v", err)
	}
	if b != nil {
		t.Errorf("Next after exhaustion: got batch len=%d, want nil", b.Len)
	}
}

// TestRowGroupIter_RespectsShard confirms shardIdx/shardCount partition the
// row groups across iterators with no overlap and full coverage. Mirrors the
// existing TestReadFileBatchesShard_ReadsDisjointRowGroups guarantee.
func TestRowGroupIter_RespectsShard(t *testing.T) {
	data := fourRowGroupFile(t)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	const shardCount = 4
	seen := make(map[int64]bool)
	totalRows := 0
	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		it, err := OpenRowGroupIter(reader, schema, nil, shardIdx, shardCount)
		if err != nil {
			t.Fatalf("shard %d: %v", shardIdx, err)
		}
		for {
			b, err := it.Next()
			if err != nil {
				t.Fatalf("shard %d Next: %v", shardIdx, err)
			}
			if b == nil {
				break
			}
			for i := 0; i < b.Len; i++ {
				id := b.Columns[0].Int64Data[i]
				if seen[id] {
					t.Errorf("shard %d: duplicate id %d", shardIdx, id)
				}
				seen[id] = true
				totalRows++
			}
		}
		it.Close()
	}

	wantTotal := 4 * 100
	if totalRows != wantTotal {
		t.Errorf("union of shards: %d rows, want %d", totalRows, wantTotal)
	}
}

// TestRowGroupIter_OutOfRangeShardYieldsNothing confirms a shardIdx >=
// shardCount produces no batches without erroring — same edge-case
// behavior as ReadFileBatchesShard.
func TestRowGroupIter_OutOfRangeShardYieldsNothing(t *testing.T) {
	data := fourRowGroupFile(t)
	reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	// shardIdx=5 with shardCount=4 — out of range.
	it, err := OpenRowGroupIter(reader, schema, nil, 5, 4)
	if err != nil {
		t.Fatalf("expected nil err for out-of-range shard, got %v", err)
	}
	defer it.Close()
	b, err := it.Next()
	if err != nil {
		t.Fatalf("Next on empty shard: %v", err)
	}
	if b != nil {
		t.Errorf("Next on empty shard: got batch, want nil")
	}
}

// TestRowGroupIter_CloseStopsFurtherReads confirms Close is honored —
// after Close, Next returns (nil, nil) regardless of remaining row
// groups. Important so worker shutdown can cancel in-flight scans
// without leaking decode work.
func TestRowGroupIter_CloseStopsFurtherReads(t *testing.T) {
	data := fourRowGroupFile(t)
	reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	it, err := OpenRowGroupIter(reader, schema, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Read the first row group, then close.
	if _, err := it.Next(); err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Subsequent Next calls return (nil, nil).
	b, err := it.Next()
	if err != nil {
		t.Errorf("Next after Close: err=%v, want nil", err)
	}
	if b != nil {
		t.Errorf("Next after Close: got batch, want nil")
	}
}
