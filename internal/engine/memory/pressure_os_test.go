package memory

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakeCounter drives a refaultSensor with a scripted monotonic counter.
type fakeCounter struct {
	value int64
	ok    bool
}

func (f *fakeCounter) read() (int64, bool) { return f.value, f.ok }

// sample advances the sensor past its interval and feeds the next count.
// The sensor derives rate from wall elapsed time, so tests use a tiny
// interval and real (short) sleeps.
func sample(t *testing.T, s *refaultSensor, f *fakeCounter, next int64) bool {
	t.Helper()
	f.value = next
	time.Sleep(2 * time.Millisecond)
	return s.Active()
}

func TestRefaultSensor_TwoSampleActivationAndRelease(t *testing.T) {
	f := &fakeCounter{value: 1000, ok: true}
	// threshold 1000 pages/s; 1ms interval so 2ms sleeps always resample.
	s := newRefaultSensor(f.read, 1000, time.Millisecond)

	// One hot sample: not active yet (damping).
	if got := sample(t, s, f, f.value+1_000_000); got {
		t.Fatal("active after a single hot sample — damping missing")
	}
	// Second consecutive hot sample: active.
	if got := sample(t, s, f, f.value+1_000_000); !got {
		t.Fatal("not active after two consecutive hot samples")
	}
	if _, activations := s.Stats(); activations != 1 {
		t.Fatalf("activations = %d, want 1", activations)
	}
	// One quiet sample releases immediately.
	if got := sample(t, s, f, f.value); got {
		t.Fatal("still active after a quiet sample")
	}
	// Hot again needs two samples again.
	if got := sample(t, s, f, f.value+1_000_000); got {
		t.Fatal("re-activated after a single hot sample")
	}
	if got := sample(t, s, f, f.value+1_000_000); !got {
		t.Fatal("not re-activated after two hot samples")
	}
	if _, activations := s.Stats(); activations != 2 {
		t.Fatalf("activations = %d, want 2", activations)
	}
}

func TestRefaultSensor_BelowThresholdStaysQuiet(t *testing.T) {
	f := &fakeCounter{value: 0, ok: true}
	s := newRefaultSensor(f.read, 1000, time.Millisecond)
	for range 5 {
		// ~1 page per 2ms = 500/s < 1000/s threshold.
		if got := sample(t, s, f, f.value+1); got {
			t.Fatal("active below threshold")
		}
	}
}

func TestRefaultSensor_SourceLossDisables(t *testing.T) {
	f := &fakeCounter{value: 0, ok: true}
	s := newRefaultSensor(f.read, 1000, time.Millisecond)
	sample(t, s, f, 1_000_000)
	f.ok = false
	if got := sample(t, s, f, 2_000_000); got {
		t.Fatal("active after source loss")
	}
	// Disabled is sticky even if the source comes back.
	f.ok = true
	sample(t, s, f, 99_000_000)
	if got := sample(t, s, f, 199_000_000); got {
		t.Fatal("sensor resurrected after source loss — disable must be sticky")
	}
}

func TestRefaultSensor_NilOrZeroThresholdDisabled(t *testing.T) {
	if s := newRefaultSensor(nil, 1000, time.Millisecond); s.Active() {
		t.Fatal("nil reader active")
	}
	f := &fakeCounter{value: 0, ok: true}
	s := newRefaultSensor(f.read, 0, time.Millisecond)
	if got := sample(t, s, f, 100_000_000); got {
		t.Fatal("zero threshold active")
	}
}

func TestParseRefaultFile(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	// Modern split counters: file variant preferred.
	p := write("modern", "nr_free_pages 1\nworkingset_refault_anon 5\nworkingset_refault_file 42\npgmajfault 9\n")
	if n, ok := parseRefaultFile(p); !ok || n != 42 {
		t.Fatalf("modern: got (%d, %v), want (42, true)", n, ok)
	}
	// Pre-5.9 unified counter.
	p = write("legacy", "workingset_refault 17\npgmajfault 9\n")
	if n, ok := parseRefaultFile(p); !ok || n != 17 {
		t.Fatalf("legacy: got (%d, %v), want (17, true)", n, ok)
	}
	// No counter at all.
	p = write("none", "nr_free_pages 1\npgmajfault 9\n")
	if _, ok := parseRefaultFile(p); ok {
		t.Fatal("file without workingset counters parsed ok")
	}
	if _, ok := parseRefaultFile(filepath.Join(dir, "missing")); ok {
		t.Fatal("missing file parsed ok")
	}
}

// TestPageCachePressureActive_Smoke exercises the real singleton path —
// whatever the host provides, it must not panic and must be callable
// concurrently (the -race build is the assertion).
func TestPageCachePressureActive_Smoke(t *testing.T) {
	done := make(chan struct{})
	for range 4 {
		go func() {
			for range 100 {
				PageCachePressureActive()
			}
			done <- struct{}{}
		}()
	}
	for range 4 {
		<-done
	}
	PageCachePressureStats()
}

// sampleBounded mirrors sample for the episode-capped accessor.
func sampleBounded(t *testing.T, s *refaultSensor, f *fakeCounter, next int64, cap time.Duration) bool {
	t.Helper()
	f.value = next
	time.Sleep(2 * time.Millisecond)
	return s.ActiveBounded(cap)
}

// TestRefaultSensor_EpisodeCap pins the v3 semantics: an activation
// episode is honored only while younger than cap; an episode that
// outlives cap (collapse didn't quiet the rate → non-causal ambient
// thrash) is declined until a quiet sample ends it, and the next
// activation starts a fresh honored episode.
func TestRefaultSensor_EpisodeCap(t *testing.T) {
	f := &fakeCounter{value: 1000, ok: true}
	s := newRefaultSensor(f.read, 1000, time.Millisecond)
	const cap = 25 * time.Millisecond

	// Activate (two hot samples) — young episode is honored.
	sampleBounded(t, s, f, f.value+1_000_000, cap)
	if !sampleBounded(t, s, f, f.value+1_000_000, cap) {
		t.Fatal("young episode not honored")
	}
	if s.BoundedIgnores() != 0 {
		t.Fatalf("ignores = %d before cap elapsed", s.BoundedIgnores())
	}
	// Stay hot past the cap: raw Active stays true, bounded declines.
	deadline := time.Now().Add(2 * cap)
	for time.Now().Before(deadline) {
		sampleBounded(t, s, f, f.value+1_000_000, cap)
	}
	if !s.Active() {
		t.Fatal("raw Active false while rate is hot")
	}
	if s.ActiveBounded(cap) {
		t.Fatal("episode older than cap still honored")
	}
	if s.BoundedIgnores() == 0 {
		t.Fatal("no ignores recorded for over-cap episode")
	}
	// cap <= 0 is unbounded — v2 kill-switch semantics.
	if !s.ActiveBounded(0) {
		t.Fatal("cap=0 must behave like raw Active")
	}
	// Quiet sample ends the episode; re-activation is honored afresh.
	if sampleBounded(t, s, f, f.value, cap) {
		t.Fatal("still active after quiet sample")
	}
	sampleBounded(t, s, f, f.value+1_000_000, cap)
	if !sampleBounded(t, s, f, f.value+1_000_000, cap) {
		t.Fatal("fresh episode after quiet sample not honored")
	}
}
