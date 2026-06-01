package worker

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
)

// withReliefEnabled flips the relief flag on for a test and restores it + clears
// the registry afterward, so tests don't leak global state into each other.
func withReliefEnabled(t *testing.T) {
	t.Helper()
	prevEnabled := mmapReliefEnabled.Load()
	prevThresh := mmapReliefThresholdBytes.Load()
	SetMmapRelief(true, 1) // tiny threshold; tests drive relief directly
	t.Cleanup(func() {
		mmapReliefEnabled.Store(prevEnabled)
		mmapReliefThresholdBytes.Store(prevThresh)
		// drain any regions a test left registered
		mmapRegistry.mu.Lock()
		mmapRegistry.regions = make(map[*mmapRegion]struct{})
		mmapRegistry.mu.Unlock()
	})
}

func TestMmapRelief_DisabledIsNoOp(t *testing.T) {
	// With relief disabled (default), registerMmap returns nil and nothing is
	// tracked — the zero-cost dormant path.
	SetMmapRelief(false, 0)
	r := registerMmap([]byte("data"))
	if r != nil {
		t.Fatalf("registerMmap should return nil when disabled, got %v", r)
	}
	if got := liveMmapCount(); got != 0 {
		t.Fatalf("no regions should be tracked when disabled, got %d", got)
	}
	// touch on a nil region and relieveMmap when disabled are both safe no-ops.
	r.touch()
	if freed := relieveMmap(1 << 30); freed != 0 {
		t.Fatalf("relieveMmap should free 0 when disabled, got %d", freed)
	}
}

func TestMmapRelief_RegisterUnregister(t *testing.T) {
	withReliefEnabled(t)
	// Use real anonymous mmap-able slices so unix.Madvise has a valid address —
	// but for register/unregister bookkeeping plain slices suffice.
	a := registerMmap(make([]byte, 4096))
	b := registerMmap(make([]byte, 4096))
	if a == nil || b == nil {
		t.Fatal("registerMmap returned nil while enabled")
	}
	if got := liveMmapCount(); got != 2 {
		t.Fatalf("expected 2 tracked regions, got %d", got)
	}
	unregisterMmap(a)
	if got := liveMmapCount(); got != 1 {
		t.Fatalf("expected 1 after unregister, got %d", got)
	}
	unregisterMmap(a) // idempotent / nil-safe-ish (already removed)
	unregisterMmap(nil)
	unregisterMmap(b)
	if got := liveMmapCount(); got != 0 {
		t.Fatalf("expected 0 after unregistering all, got %d", got)
	}
}

func TestMmapRelief_TouchOrdersColdestFirst(t *testing.T) {
	withReliefEnabled(t)
	// relieveMmap sorts by lastTouch ascending (coldest first). Register three,
	// touch them in a known order, and assert the relief order via a custom
	// MADV that records instead of syscalling.
	r1 := registerMmap(make([]byte, 4096))
	r2 := registerMmap(make([]byte, 4096))
	r3 := registerMmap(make([]byte, 4096))
	// Make touch times strictly increasing and distinct.
	r1.lastTouch.Store(100)
	r2.lastTouch.Store(300)
	r3.lastTouch.Store(200)
	// Coldest-first order should be r1(100), r3(200), r2(300).
	order := sortRegionsByTouch()
	if len(order) != 3 || order[0] != r1 || order[1] != r3 || order[2] != r2 {
		t.Fatalf("coldest-first order wrong: got %v want [r1 r3 r2]", order)
	}
}

// TestMmapRelief_RealMadvise exercises the actual MADV_DONTNEED syscall on a
// real file mapping (mirroring the production PROT_READ MAP_SHARED path) and
// confirms relief reports the bytes freed and the data still reads correctly
// after the advise (pages re-fault from the backing file).
func TestMmapRelief_RealMadvise(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("MADV_DONTNEED path is Linux-only")
	}
	withReliefEnabled(t)

	// Create a backing file with known contents, larger than a page.
	const size = 64 * 4096
	path := filepath.Join(t.TempDir(), "mmap-test")
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	defer syscall.Munmap(data)

	r := registerMmap(data)
	if r == nil {
		t.Fatal("registerMmap returned nil while enabled")
	}

	freed := relieveMmap(size)
	if freed < size {
		t.Fatalf("relieveMmap freed %d, want >= %d", freed, size)
	}
	// Data must still be correct after MADV_DONTNEED (re-faulted from the file).
	for i := 0; i < size; i += 4096 {
		if data[i] != byte(i%251) {
			t.Fatalf("data corrupted after relief at %d: got %d want %d", i, data[i], byte(i%251))
		}
	}
	unregisterMmap(r)
}

func TestMmapRelief_ConcurrentRegisterTouch(t *testing.T) {
	withReliefEnabled(t)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := registerMmap(make([]byte, 4096))
			for j := 0; j < 100; j++ {
				r.touch()
			}
			unregisterMmap(r)
		}()
	}
	// concurrent reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 100; j++ {
			_ = liveMmapCount()
		}
	}()
	wg.Wait()
	if got := liveMmapCount(); got != 0 {
		t.Fatalf("all regions should be unregistered, got %d", got)
	}
}
