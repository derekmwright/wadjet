package worker

import (
	"os"
	"testing"
	"time"
)

// TestRangeToucher_TouchesAndCounts: enqueued ranges are walked and the
// engagement counter advances by the clamped range lengths.
func TestRangeToucher_TouchesAndCounts(t *testing.T) {
	data := make([]byte, 1<<20)
	before := touchAheadBytes.Load()
	tc := newRangeToucher(data)
	tc.enqueue(0, 4096)
	tc.enqueue(1<<19, 8192)
	// Out-of-bounds and degenerate ranges are dropped silently.
	tc.enqueue(-5, 100)
	tc.enqueue(int64(len(data))+10, 100)
	tc.enqueue(100, 0)
	// Overlong range clamps to the mapping.
	tc.enqueue(int64(len(data))-100, 1<<30)

	deadline := time.Now().Add(5 * time.Second)
	want := int64(4096 + 8192 + 100)
	for touchAheadBytes.Load()-before < want {
		if time.Now().After(deadline) {
			t.Fatalf("touch bytes = %d, want >= %d", touchAheadBytes.Load()-before, want)
		}
		time.Sleep(time.Millisecond)
	}
	tc.stop()
}

// TestRangeToucher_StopAbandonsQueue: stop must return promptly even
// with a deep queue and a huge in-flight range — munmap waits on it.
func TestRangeToucher_StopAbandonsQueue(t *testing.T) {
	data := make([]byte, 64<<20)
	tc := newRangeToucher(data)
	for i := 0; i < 900; i++ {
		tc.enqueue(0, int64(len(data)))
	}
	done := make(chan struct{})
	go func() { tc.stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("stop did not return; munmap would block behind the touch backlog")
	}
}

// TestRangeToucher_NilStop: releaseParquetIter calls stop on sources
// that never had a toucher (S3-streamed path, kill switch off).
func TestRangeToucher_NilStop(t *testing.T) {
	var tc *rangeToucher
	tc.stop()
}

// TestRangeToucher_PopulateFallback: a toucher over heap-backed (or
// otherwise madvise-rejecting) memory must flip to the byte walk on the
// first failure and still touch and count every enqueued range — the
// populate path is an accelerator, never a correctness dependency.
func TestRangeToucher_PopulateFallback(t *testing.T) {
	data := make([]byte, 1<<20)
	before := touchAheadBytes.Load()
	tc := newRangeToucher(data)
	defer tc.stop()
	tc.populate = true // force the probe regardless of env/platform
	tc.enqueue(0, 1<<20)
	deadline := time.Now().Add(5 * time.Second)
	want := int64(1 << 20)
	for touchAheadBytes.Load()-before < want {
		if time.Now().After(deadline) {
			t.Fatalf("touch bytes = %d, want %d (fallback did not complete the range)",
				touchAheadBytes.Load()-before, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestRangeToucher_PopulateChunkAccounting: a range spanning multiple
// populate chunks is fully accounted whichever path each chunk takes.
func TestRangeToucher_PopulateChunkAccounting(t *testing.T) {
	data := make([]byte, touchPopulateChunk*2+4096)
	before := touchAheadBytes.Load()
	tc := newRangeToucher(data)
	defer tc.stop()
	tc.enqueue(0, int64(len(data)))
	deadline := time.Now().Add(10 * time.Second)
	want := int64(len(data))
	for touchAheadBytes.Load()-before < want {
		if time.Now().After(deadline) {
			t.Fatalf("touch bytes = %d, want %d", touchAheadBytes.Load()-before, want)
		}
		time.Sleep(time.Millisecond)
	}
	if got := touchAheadBytes.Load() - before; got != want {
		t.Fatalf("touch bytes = %d, want exactly %d (double counting)", got, want)
	}
}

// TestRangeToucher_EnqueuePageAligns: offsets round down to a page
// boundary so the byte-per-page walk starts on the page containing off.
func TestRangeToucher_EnqueuePageAligns(t *testing.T) {
	pg := int64(os.Getpagesize())
	data := make([]byte, 4*pg)
	before := touchAheadBytes.Load()
	tc := newRangeToucher(data)
	defer tc.stop()
	tc.enqueue(pg+1, 10) // starts mid-page: aligned start pg, end pg+11
	deadline := time.Now().Add(5 * time.Second)
	want := int64(11)
	for touchAheadBytes.Load()-before < want {
		if time.Now().After(deadline) {
			t.Fatalf("touch bytes = %d, want %d (aligned-start accounting)",
				touchAheadBytes.Load()-before, want)
		}
		time.Sleep(time.Millisecond)
	}
}
