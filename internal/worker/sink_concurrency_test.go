package worker

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// sinkTestSchema mixes fixed-width, bytes, and nullable columns so the
// concurrent appends exercise every accumulator arm that matters.
var sinkTestSchema = []parquet.Column{
	{Name: "k", Type: parquet.TypeInt64},
	{Name: "v", Type: parquet.TypeString},
	{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
}

// makeSinkBatch builds a batch of n rows with globally-unique keys starting
// at base. Odd rows get a NULL f; every 3rd row is dropped via Sel so the
// selection-vector path is exercised.
func makeSinkBatch(base, n int) *batch.RecordBatch {
	b := batch.NewRecordBatch(sinkTestSchema, n)
	for i := 0; i < n; i++ {
		k := int64(base + i)
		b.Columns[0].Int64Data[i] = k
		b.Columns[1].BytesData.Set(i, []byte(fmt.Sprintf("row-%d-padding", k)))
		if i%2 == 1 {
			b.Columns[2].Nulls.SetNull(i)
		} else {
			b.Columns[2].Float64Data[i] = float64(k) * 1.5
		}
	}
	var sel []uint32
	for i := 0; i < n; i++ {
		if i%3 == 0 {
			continue
		}
		sel = append(sel, uint32(i))
	}
	b.Sel = sel
	return b
}

// expectedKeys returns the key multiset makeSinkBatch(base,n) contributes.
func expectedKeys(base, n int) []int64 {
	var out []int64
	for i := 0; i < n; i++ {
		if i%3 == 0 {
			continue
		}
		out = append(out, int64(base+i))
	}
	return out
}

// TestPartitionedShuffleSinkConcurrentConsume drives k goroutines through
// Consume with distinct batches and verifies the union of all partition
// files holds exactly the input key multiset, with every key in the same
// partition a serial run puts it in (hashing is data-deterministic). Run
// with -race this is the regression gate for the per-partition locking that
// replaced the sink-wide mutex — the old code serialized concurrent
// consumers; the new code must interleave them without losing or
// duplicating rows.
func TestPartitionedShuffleSinkConcurrentConsume(t *testing.T) {
	const (
		numParts   = 8
		consumers  = 8
		batchesPer = 24
		rowsPer    = 500 // < shuffleBurstGateRows: inline per-partition path
	)
	dir := t.TempDir()
	s := newPartitionedShuffleSink(dir, []string{"k"}, numParts, sinkTestSchema)
	// Tiny flush threshold so concurrent threshold-flushes happen constantly.
	s.flushBytes = 2048
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, consumers)
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 0; i < batchesPer; i++ {
				base := (c*batchesPer + i) * rowsPer
				if err := s.Consume(context.Background(), makeSinkBatch(base, rowsPer)); err != nil {
					errs <- err
					return
				}
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent consume: %v", err)
	}
	if err := s.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Expected multiset.
	var want []int64
	for c := 0; c < consumers; c++ {
		for i := 0; i < batchesPer; i++ {
			want = append(want, expectedKeys((c*batchesPer+i)*rowsPer, rowsPer)...)
		}
	}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })

	// Read back all partitions; also assert partition placement matches a
	// fresh serial sink fed the same rows (same hash function, same file).
	var got []int64
	perPartGot := make(map[int][]int64)
	for p, path := range s.PartitionFiles() {
		if path == "" {
			continue
		}
		vals := readWSHFInts(t, path, "k")
		got = append(got, vals...)
		perPartGot[p] = vals
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if len(got) != len(want) {
		t.Fatalf("row count mismatch: got %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("key multiset diverges at %d: got %d want %d", i, got[i], want[i])
		}
	}

	// Serial reference for partition placement.
	refDir := t.TempDir()
	ref := newPartitionedShuffleSink(refDir, []string{"k"}, numParts, sinkTestSchema)
	if err := ref.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	for c := 0; c < consumers; c++ {
		for i := 0; i < batchesPer; i++ {
			base := (c*batchesPer + i) * rowsPer
			if err := ref.Consume(context.Background(), makeSinkBatch(base, rowsPer)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := ref.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer ref.Close()
	for p, path := range ref.PartitionFiles() {
		if path == "" {
			if len(perPartGot[p]) != 0 {
				t.Fatalf("partition %d: concurrent run has rows, serial reference empty", p)
			}
			continue
		}
		refVals := readWSHFInts(t, path, "k")
		gotVals := append([]int64(nil), perPartGot[p]...)
		sort.Slice(refVals, func(i, j int) bool { return refVals[i] < refVals[j] })
		sort.Slice(gotVals, func(i, j int) bool { return gotVals[i] < gotVals[j] })
		if len(refVals) != len(gotVals) {
			t.Fatalf("partition %d: row count %d != serial reference %d", p, len(gotVals), len(refVals))
		}
		for i := range refVals {
			if refVals[i] != gotVals[i] {
				t.Fatalf("partition %d placement diverges from serial reference at %d", p, i)
			}
		}
	}
}

// TestPartitionedShuffleSinkConcurrentBurst covers the large-consume path
// (n ≥ shuffleBurstGateRows) where each Consume additionally fans out its
// own errgroup burst — concurrent consumers × internal bursts must still
// keep per-partition state consistent under the per-partition locks.
func TestPartitionedShuffleSinkConcurrentBurst(t *testing.T) {
	const (
		numParts   = 4
		consumers  = 4
		batchesPer = 4
		rowsPer    = 6000 // ≥ shuffleBurstGateRows: errgroup burst path
	)
	dir := t.TempDir()
	s := newPartitionedShuffleSink(dir, []string{"k"}, numParts, sinkTestSchema)
	s.flushBytes = 8 << 10
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, consumers)
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 0; i < batchesPer; i++ {
				base := (c*batchesPer + i) * rowsPer
				if err := s.Consume(context.Background(), makeSinkBatch(base, rowsPer)); err != nil {
					errs <- err
					return
				}
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent consume: %v", err)
	}
	if err := s.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	total := 0
	for _, path := range s.PartitionFiles() {
		if path == "" {
			continue
		}
		total += len(readWSHFInts(t, path, "k"))
	}
	want := 0
	for c := 0; c < consumers; c++ {
		for i := 0; i < batchesPer; i++ {
			want += len(expectedKeys((c*batchesPer+i)*rowsPer, rowsPer))
		}
	}
	if total != want {
		t.Fatalf("total rows %d, want %d", total, want)
	}
}

// TestUnpartitionedStageSinkConcurrentConsume drives k goroutines through the
// coalescing path with thresholds small enough that double-buffered flushes
// (encode outside the lock) and the flush-in-flight backpressure wait both
// fire constantly. The decoded file must hold exactly the input multiset.
func TestUnpartitionedStageSinkConcurrentConsume(t *testing.T) {
	const (
		consumers  = 8
		batchesPer = 32
		rowsPer    = 700
	)
	dir := t.TempDir()
	s := newUnpartitionedStageSink(dir, "conc-test")
	// Force frequent flushes: every ~2 consumes trips the row threshold.
	s.flushRows = 1500
	s.flushBytesT = 64 << 10
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, consumers)
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 0; i < batchesPer; i++ {
				base := (c*batchesPer + i) * rowsPer
				if err := s.Consume(context.Background(), makeSinkBatch(base, rowsPer)); err != nil {
					errs <- err
					return
				}
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent consume: %v", err)
	}
	if err := s.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if s.NumChunks() == 0 {
		t.Fatal("expected chunks written")
	}
	var want []int64
	for c := 0; c < consumers; c++ {
		for i := 0; i < batchesPer; i++ {
			want = append(want, expectedKeys((c*batchesPer+i)*rowsPer, rowsPer)...)
		}
	}
	if s.TotalRows() != int64(len(want)) {
		t.Fatalf("TotalRows %d, want %d", s.TotalRows(), len(want))
	}
	got := readWSHFInts(t, s.Path(), "k")
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if len(got) != len(want) {
		t.Fatalf("decoded rows %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("key multiset diverges at %d: got %d want %d", i, got[i], want[i])
		}
	}
	_ = os.Remove(s.Path())
}

// BenchmarkPartitionedShuffleSinkConsume measures Consume throughput on the
// serial path (the production default — guards the per-partition-lock
// refactor against serial regression) and under 8 concurrent consumers (the
// morsel-parallel shape the refactor exists to unblock).
func BenchmarkPartitionedShuffleSinkConsume(b *testing.B) {
	const numParts = 24
	mkSink := func(b *testing.B) *partitionedShuffleSink {
		b.Helper()
		s := newPartitionedShuffleSink(b.TempDir(), []string{"k"}, numParts, sinkTestSchema)
		if err := s.Init(context.Background()); err != nil {
			b.Fatal(err)
		}
		return s
	}
	input := makeSinkBatch(0, 2048)

	b.Run("serial", func(b *testing.B) {
		s := mkSink(b)
		defer s.Close()
		b.SetBytes(int64(2048 * 40))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := s.Consume(context.Background(), input); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("parallel-8", func(b *testing.B) {
		s := mkSink(b)
		defer s.Close()
		b.SetBytes(int64(2048 * 40))
		b.SetParallelism(8)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			// Each goroutine gets a private batch — the caller contract.
			in := makeSinkBatch(0, 2048)
			for pb.Next() {
				if err := s.Consume(context.Background(), in); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
