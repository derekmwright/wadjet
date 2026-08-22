package worker

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// withStageSinkAccum pins the producer-local accumulation kill switch for a
// test and restores it on cleanup. Tests using it must not call t.Parallel.
func withStageSinkAccum(t *testing.T, on bool) {
	t.Helper()
	prev := stageSinkAccum.Set(on)
	t.Cleanup(func() { stageSinkAccum.Set(prev) })
}

// TestUnpartitionedStageSink_SlabFinalizeDrain is the row-loss gate for
// producer-local accumulation: N producers push batches of random sizes,
// every one of which can leave residual rows sitting in a slab that no
// threshold ever trips, and Finalize must turn every one of those slabs
// into chunks. A drain that misses a registered slab silently truncates a
// stage output — the classic bug this design has to be proof against.
func TestUnpartitionedStageSink_SlabFinalizeDrain(t *testing.T) {
	withStageSinkAccum(t, true)
	const (
		producers  = 8
		batchesPer = 40
	)
	dir := t.TempDir()
	s := newUnpartitionedStageSink(dir, "slab-drain")
	// Thresholds low enough that some slabs flush mid-stream and high
	// enough that every producer still ends holding a partial slab.
	s.flushRows = 4096
	s.flushBytesT = 512 << 10
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Pre-plan every batch so the expected multiset is known without
	// depending on goroutine interleaving.
	sizes := make([][]int, producers)
	rng := rand.New(rand.NewSource(7))
	base := 0
	bases := make([][]int, producers)
	for p := 0; p < producers; p++ {
		for i := 0; i < batchesPer; i++ {
			// >1 row: makeSinkBatch leaves Sel nil at n==1 (every row is
			// dropped by the i%3 rule), which expectedKeys does not model.
			n := 2 + rng.Intn(900)
			sizes[p] = append(sizes[p], n)
			bases[p] = append(bases[p], base)
			base += n
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, producers)
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i, n := range sizes[p] {
				if err := s.Consume(context.Background(), makeSinkBatch(bases[p][i], n)); err != nil {
					errs <- err
					return
				}
			}
		}(p)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("consume: %v", err)
	}
	if err := s.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var want []int64
	for p := 0; p < producers; p++ {
		for i, n := range sizes[p] {
			want = append(want, expectedKeys(bases[p][i], n)...)
		}
	}
	if got := s.TotalRows(); got != int64(len(want)) {
		t.Fatalf("TotalRows = %d, want %d", got, len(want))
	}
	got := readWSHFInts(t, s.Path(), "k")
	if len(got) != len(want) {
		t.Fatalf("decoded rows = %d, want %d (rows lost in an undrained slab)", len(got), len(want))
	}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("key multiset diverges at %d: got %d want %d", i, got[i], want[i])
		}
	}
	// Every slab's bytes must be released back to the sink's accounting once
	// its rows are in the stream — a leak here means the budget shrinks
	// chunks for the rest of the sink's life.
	if n := s.bufferedBytes.Load(); n != 0 {
		t.Fatalf("bufferedBytes = %d after Finalize, want 0", n)
	}
	if len(s.slabAll) == 0 {
		t.Fatal("no slabs registered — the producer-local path did not run")
	}
	_ = os.Remove(s.Path())
}

// TestUnpartitionedStageSink_SlabSerialParity pins the producer-local path
// to the pre-change accumulator for a serial producer: identical file
// bytes, so chunk boundaries, row order and encoding are all unchanged. The
// serial case is the one where "flush-size semantics preserved" is exactly
// checkable — LIFO slab checkout keeps a single producer on one slab, so it
// must trip the same thresholds at the same rows the shared accumulator did.
func TestUnpartitionedStageSink_SlabSerialParity(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	run := func(on bool) []byte {
		withStageSinkAccum(t, on)
		dir := t.TempDir()
		s := newUnpartitionedStageSink(dir, "parity")
		s.flushRows = 5000
		s.flushBytesT = 1 << 20
		if err := s.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
		next := int64(0)
		for c := 0; c < 30; c++ {
			n := 700 + c*13
			ids := make([]int64, n)
			names := make([]string, n)
			for i := range ids {
				ids[i] = next
				names[i] = fmt.Sprintf("row-%d", next)
				next++
			}
			b := makeBatchInt64String(schema, ids, names)
			if c%3 == 1 {
				sel := make([]uint32, 0, n/2)
				for i := 0; i < n; i += 2 {
					sel = append(sel, uint32(i))
				}
				b.Sel = sel
			}
			if err := s.Consume(context.Background(), b); err != nil {
				t.Fatalf("consume %d: %v", c, err)
			}
		}
		if err := s.Finalize(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		data, err := os.ReadFile(s.Path())
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	withAccum := run(true)
	legacy := run(false)
	if len(withAccum) != len(legacy) {
		t.Fatalf("file size %d with producer-local accumulation, %d without", len(withAccum), len(legacy))
	}
	for i := range legacy {
		if withAccum[i] != legacy[i] {
			t.Fatalf("serial output diverges at byte %d", i)
		}
	}
}

// TestUnpartitionedStageSink_SlabBudget: with many producers each holding a
// partial slab, the sink's buffered-byte budget must cap total residency by
// flushing early rather than letting per-consumer buffers grow without
// bound. Rows still all arrive.
func TestUnpartitionedStageSink_SlabBudget(t *testing.T) {
	withStageSinkAccum(t, true)
	const producers = 8
	dir := t.TempDir()
	s := newUnpartitionedStageSink(dir, "slab-budget")
	// Row threshold out of reach; the byte budget is the only thing that
	// can bound residency.
	s.flushRows = 1 << 30
	s.flushBytesT = 32 << 10
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	budget := int64(s.flushBytesT) * stageSlabBudgetFactor
	var wg sync.WaitGroup
	var peak int64
	var peakMu sync.Mutex
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < 60; i++ {
				if err := s.Consume(context.Background(), makeSinkBatch((p*60+i)*400, 400)); err != nil {
					t.Error(err)
					return
				}
				peakMu.Lock()
				if n := s.bufferedBytes.Load(); n > peak {
					peak = n
				}
				peakMu.Unlock()
			}
		}(p)
	}
	wg.Wait()
	if err := s.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// One slab may overshoot by its last append; anything beyond a slab's
	// own threshold on top of the budget means the cap is not holding.
	if limit := budget + int64(s.flushBytesT)*producers; peak > limit {
		t.Fatalf("peak buffered bytes %d exceeds budget %d (+overshoot allowance)", peak, limit)
	}
	if got, want := s.TotalRows(), int64(producers*60*len(expectedKeys(0, 400))); got != want {
		t.Fatalf("TotalRows = %d, want %d", got, want)
	}
	if n := len(readWSHFInts(t, s.Path(), "k")); int64(n) != s.TotalRows() {
		t.Fatalf("decoded rows = %d, want %d", n, s.TotalRows())
	}
	_ = os.Remove(s.Path())
}

// BenchmarkUnpartitionedSinkConsume_Producers is the contention benchmark
// for the producer-local accumulation change: k goroutines push dense
// 2048-row batches (the stage-output shape the SF100 mutex profile caught —
// unpartitionedStageSink.Consume at 32.6% of worker mutex delay with the
// row copy inside the lock). cum-ns/consume is the per-consume time
// INCLUDING lock wait, i.e. the contention signal; ns/op is wall time.
// Compare arms with WADJET_STAGE_SINK_ACCUM=0 for the pre-change path.
func BenchmarkUnpartitionedSinkConsume_Producers(b *testing.B) {
	schema := []parquet.Column{
		{Name: "l_orderkey", Type: parquet.TypeInt64},
		{Name: "l_partkey", Type: parquet.TypeInt64},
		{Name: "l_quantity", Type: parquet.TypeFloat64},
		{Name: "l_shipmode", Type: parquet.TypeString},
	}
	mkBatch := func(seed int) *batch.RecordBatch {
		rb := batch.NewRecordBatch(schema, batch.DefaultBatchSize)
		rb.Len = batch.DefaultBatchSize
		for i := 0; i < rb.Len; i++ {
			rb.Columns[0].Int64Data[i] = int64(seed + i)
			rb.Columns[1].Int64Data[i] = int64(i)
			rb.Columns[2].Float64Data[i] = float64(i) * 1.5
			rb.Columns[3].BytesData.Set(i, []byte("AIR REG"))
		}
		return rb
	}
	for _, k := range []int{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("producers=%d", k), func(b *testing.B) {
			dir := b.TempDir()
			sink := newUnpartitionedStageSink(dir, "bench-producers")
			if err := sink.Init(context.Background()); err != nil {
				b.Fatal(err)
			}
			defer func() {
				sink.Close()
				os.Remove(sink.Path())
			}()
			per := b.N/k + 1
			batches := make([]*batch.RecordBatch, k)
			for c := range batches {
				batches[c] = mkBatch(c * 1 << 20)
			}
			cum := make([]int64, k)
			b.ResetTimer()
			var wg sync.WaitGroup
			for c := 0; c < k; c++ {
				wg.Add(1)
				go func(c int) {
					defer wg.Done()
					bb := batches[c]
					t0 := time.Now()
					for i := 0; i < per; i++ {
						if err := sink.Consume(context.Background(), bb); err != nil {
							b.Error(err)
							return
						}
					}
					cum[c] = time.Since(t0).Nanoseconds()
				}(c)
			}
			wg.Wait()
			b.StopTimer()
			var total int64
			for _, v := range cum {
				total += v
			}
			b.ReportMetric(float64(total)/float64(per*k), "cum-ns/consume")
		})
	}
}
