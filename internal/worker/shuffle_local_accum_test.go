package worker

import (
	"context"
	"sort"
	"sync"
	"testing"
)

func withLocalAccum(t testing.TB, on bool) {
	t.Helper()
	prev := sinkLocalAccumEnabled
	sinkLocalAccumEnabled = on
	t.Cleanup(func() { sinkLocalAccumEnabled = prev })
}

// runTinyConsumeSink drives the q08 join-6 shape through a partitioned sink:
// many small consumes (n far below shuffleBurstGateRows) from concurrent
// consumers, then Finalize. Returns the union key multiset read back from
// the partition files, sorted.
func runTinyConsumeSink(t *testing.T, numParts, consumers, batchesPer, rowsPer int) []int64 {
	t.Helper()
	dir := t.TempDir()
	s := newPartitionedShuffleSink(dir, []string{"k"}, numParts, sinkTestSchema)
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
				base := (c*batchesPer + i) * 1000
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
	var got []int64
	for _, path := range s.PartitionFiles() {
		if path == "" {
			continue
		}
		got = append(got, readWSHFInts(t, path, "k")...)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	return got
}

// TestShuffleLocalAccumFinalizeDrain is the data-loss regression guard for
// consumer-local pre-accumulation: consumes small enough that no slab ever
// crosses localAccumFlushBytes mean every row is still sitting in a local
// slab at Finalize — the drain must land all of them, exactly once.
func TestShuffleLocalAccumFinalizeDrain(t *testing.T) {
	withLocalAccum(t, true)
	const numParts, consumers, batchesPer, rowsPer = 24, 4, 8, 13
	got := runTinyConsumeSink(t, numParts, consumers, batchesPer, rowsPer)
	var want []int64
	for c := 0; c < consumers; c++ {
		for i := 0; i < batchesPer; i++ {
			want = append(want, expectedKeys((c*batchesPer+i)*1000, rowsPer)...)
		}
	}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if len(got) != len(want) {
		t.Fatalf("row count: got %d want %d (slab residuals lost or duplicated)", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("key multiset diverges at %d: got %d want %d", i, got[i], want[i])
		}
	}
}

// TestShuffleLocalAccumParity: gate on vs off over enough tiny consumes to
// cross the slab threshold repeatedly — identical key multisets.
func TestShuffleLocalAccumParity(t *testing.T) {
	const numParts, consumers, batchesPer, rowsPer = 8, 6, 120, 40
	withLocalAccum(t, false)
	want := runTinyConsumeSink(t, numParts, consumers, batchesPer, rowsPer)
	withLocalAccum(t, true)
	got := runTinyConsumeSink(t, numParts, consumers, batchesPer, rowsPer)
	if len(got) != len(want) {
		t.Fatalf("row count: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("key multiset diverges at %d: got %d want %d", i, got[i], want[i])
		}
	}
}

// BenchmarkShuffleSinkTinyConsumes measures the q08 join-6 sink shape:
// 13-row consumes into 24 partitions from 8 concurrent consumers.
func BenchmarkShuffleSinkTinyConsumes(b *testing.B) {
	for _, mode := range []struct {
		name string
		on   bool
	}{{"localAccum", true}, {"perConsume", false}} {
		b.Run(mode.name, func(b *testing.B) {
			withLocalAccum(b, mode.on)
			dir := b.TempDir()
			s := newPartitionedShuffleSink(dir, []string{"k"}, 24, sinkTestSchema)
			if err := s.Init(context.Background()); err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			const consumers = 8
			b.ResetTimer()
			var wg sync.WaitGroup
			per := b.N/consumers + 1
			for c := 0; c < consumers; c++ {
				wg.Add(1)
				go func(c int) {
					defer wg.Done()
					bb := makeSinkBatch(c*1_000_000, 13)
					for i := 0; i < per; i++ {
						if err := s.Consume(context.Background(), bb); err != nil {
							b.Error(err)
							return
						}
					}
				}(c)
			}
			wg.Wait()
			b.StopTimer()
			if err := s.Finalize(context.Background()); err != nil {
				b.Fatal(err)
			}
		})
	}
}
