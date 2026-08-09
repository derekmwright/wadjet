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
