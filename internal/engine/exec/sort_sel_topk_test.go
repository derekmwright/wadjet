package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func sortBatchInt64(t *testing.T, vals ...int64) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{{Name: "v", Type: parquet.TypeInt64}}, len(vals))
	copy(b.Columns[0].Int64Data, vals)
	return b
}

func drainSorted(t *testing.T, s *Sort) []int64 {
	t.Helper()
	if err := s.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	var out []int64
	for {
		b, err := s.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		for _, r := range b.ToRows() {
			out = append(out, r["v"].(int64))
		}
	}
	return out
}

// Regression (ClickBench Q24 panic): Filter operators reuse ONE selection
// buffer across batches. Sort stored batches without snapshotting Sel, so a
// stored batch's selection was silently rewritten by the NEXT batch's
// filter pass — out-of-range physical indices at finalize (panic in
// sortCompareInt64NoNulls) or silently wrong sorted output.
func TestSortSnapshotsSelAgainstFilterBufferReuse(t *testing.T) {
	s := NewSort([]SortKey{{Column: "v", Order: Ascending}})

	selBuf := make([]uint32, 4) // the "filter's" shared buffer

	b1 := sortBatchInt64(t, 50, 10, 30, 20)
	copy(selBuf, []uint32{1, 3}) // select 10, 20
	b1.Sel = selBuf[:2]
	if err := s.Consume(context.Background(), b1); err != nil {
		t.Fatal(err)
	}

	// The filter reuses its buffer for the next batch — previously this
	// clobbered b1's stored selection.
	b2 := sortBatchInt64(t, 5, 6)
	copy(selBuf, []uint32{0, 1})
	b2.Sel = selBuf[:2]
	if err := s.Consume(context.Background(), b2); err != nil {
		t.Fatal(err)
	}

	got := drainSorted(t, s)
	want := []int64{5, 6, 10, 20}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// Streaming Top-K: with a known Limit the sort must compact its buffer
// during Consume instead of pinning the entire input until finalize
// (ClickBench Q24: SELECT * + LIKE straight into Sort pinned every wide
// scanned batch).
func TestSortStreamingTopKBoundsBuffer(t *testing.T) {
	s := NewSort([]SortKey{{Column: "v", Order: Ascending}})
	s.Limit = 5

	const batches = 100
	n := 0
	for i := 0; i < batches; i++ {
		vals := make([]int64, batch.DefaultBatchSize)
		for j := range vals {
			vals[j] = int64((i*batch.DefaultBatchSize + j) % 100003)
		}
		if err := s.Consume(context.Background(), sortBatchInt64(t, vals...)); err != nil {
			t.Fatal(err)
		}
		n += len(vals)
	}
	s.mu.Lock()
	buffered := len(s.batches)
	rows := s.totalRows
	s.mu.Unlock()
	if rows > s.Limit*2+4*batch.DefaultBatchSize+batch.DefaultBatchSize {
		t.Fatalf("sort buffered %d rows of %d consumed — streaming top-K not compacting", rows, n)
	}
	if buffered > 8 {
		t.Fatalf("sort holds %d batches — streaming top-K not compacting", buffered)
	}

	got := drainSorted(t, s)
	want := []int64{0, 0, 0, 1, 1}
	if len(got) != 5 {
		t.Fatalf("got %d rows, want 5: %v", len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
