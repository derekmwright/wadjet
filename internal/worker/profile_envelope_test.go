package worker

import (
	"io"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestCollectProfileEnvelope_BlockMutexGated verifies the WorkerProfile
// envelope carries block/mutex profiles only when the runtime samplers are
// enabled (the WADJET_BLOCK_PROFILE_RATE / WADJET_MUTEX_PROFILE_FRACTION
// path), and that heap is always present.
func TestCollectProfileEnvelope_BlockMutexGated(t *testing.T) {
	w := &Worker{
		config: Config{WorkerID: "test-worker"},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	// Samplers off (default): no block/mutex payload. This half must run
	// first — once records accumulate they persist for the process.
	env := w.collectProfileEnvelope()
	if env.WorkerID != "test-worker" {
		t.Fatalf("WorkerID = %q, want test-worker", env.WorkerID)
	}
	if len(env.Heap) == 0 {
		t.Fatal("heap profile missing from envelope")
	}
	if len(env.Block) != 0 || len(env.Mutex) != 0 {
		t.Fatalf("block/mutex present with samplers off: block=%d mutex=%d",
			len(env.Block), len(env.Mutex))
	}

	// Samplers on: generate one blocking event and one mutex contention
	// event, then expect both profiles in the envelope.
	runtime.SetBlockProfileRate(1)
	defer runtime.SetBlockProfileRate(0)
	runtime.SetMutexProfileFraction(1)
	defer runtime.SetMutexProfileFraction(0)

	ch := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(ch)
	}()
	<-ch // channel-receive block event

	var mu sync.Mutex
	mu.Lock()
	done := make(chan struct{})
	go func() {
		mu.Lock() // contends until parent unlocks
		mu.Unlock()
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	mu.Unlock()
	<-done

	env = w.collectProfileEnvelope()
	if len(env.Block) == 0 {
		t.Fatal("block profile missing with SetBlockProfileRate(1)")
	}
	if len(env.Mutex) == 0 {
		t.Fatal("mutex profile missing with SetMutexProfileFraction(1)")
	}
}
