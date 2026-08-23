package worker

import (
	"io"
	"log/slog"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"
)

// TestCollectProfileEnvelope_BlockMutexGated verifies the WorkerProfile
// envelope carries block/mutex profiles only when the runtime samplers are
// enabled (the WADJET_BLOCK_PROFILE_RATE / WADJET_MUTEX_PROFILE_FRACTION
// path), and that heap and goroutine are always present regardless of
// sampler state — goroutine profiling needs no runtime sampler.
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
	if len(env.Goroutine) == 0 {
		t.Fatal("goroutine profile missing from envelope with samplers off")
	}
	// The envelope's gate is pprof's record COUNT, not the env knob, and a
	// non-zero count is not this test's doing: the race build's runtime
	// records contended runtime-internal locks into the mutex profile on
	// its own, which is why this assertion failed alone under -race with
	// a few hundred bytes in each payload (#421). Assert the gate the
	// collector actually applies, so the check is true of the code rather
	// than of the build it happens to run under.
	if pprof.Lookup("block").Count() == 0 && len(env.Block) != 0 {
		t.Fatalf("block payload present with an empty block profile: %d bytes", len(env.Block))
	}
	if pprof.Lookup("mutex").Count() == 0 && len(env.Mutex) != 0 {
		t.Fatalf("mutex payload present with an empty mutex profile: %d bytes", len(env.Mutex))
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
