package memory

import (
	"math"
	"runtime/debug"
	"testing"
)

// withMemLimit sets GOMEMLIMIT for the duration of a test and restores it,
// also wiping the spill package's cached limit so neighbouring tests don't see
// a stale value.
func withMemLimit(t *testing.T, limit int64) {
	t.Helper()
	prev := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(limit)
	t.Cleanup(func() {
		debug.SetMemoryLimit(prev)
		resetHeapBackpressureCache(t)
	})
}

func TestReservoir_Validate_PassAndFail(t *testing.T) {
	const limit = 1000 << 20 // 1000 MiB
	withMemLimit(t, limit)

	// Fits: cap 800 + min 64 + headroom 100 (10%) = 964 <= 1000.
	rr := NewReservoirRegistry()
	rr.Register(NewReservoir("pool", 800<<20))
	if err := rr.Validate(); err != nil {
		t.Fatalf("expected pass, got %v", err)
	}

	// Violates: cap 950 + 64 + 100 = 1114 > 1000.
	rr2 := NewReservoirRegistry()
	rr2.Register(NewReservoir("pool", 950<<20))
	if err := rr2.Validate(); err == nil {
		t.Fatal("expected invariant violation error, got nil")
	}
}

func TestReservoir_Validate_SumsMultipleCaps(t *testing.T) {
	const limit = 1000 << 20
	withMemLimit(t, limit)

	rr := NewReservoirRegistry()
	rr.Register(NewReservoir("a", 400<<20))
	rr.Register(NewReservoir("b", 400<<20)) // Σcaps=800; +64+100=964 <= 1000
	if err := rr.Validate(); err != nil {
		t.Fatalf("two caps within budget should pass, got %v", err)
	}
	rr.Register(NewReservoir("c", 100<<20)) // Σcaps=900; +64+100=1064 > 1000
	if err := rr.Validate(); err == nil {
		t.Fatal("third cap should push over budget")
	}
}

func TestReservoir_Validate_UnsetLimitIsNoop(t *testing.T) {
	withMemLimit(t, math.MaxInt64) // "unset"

	rr := NewReservoirRegistry()
	rr.Register(NewReservoir("huge", math.MaxInt64/2))
	if err := rr.Validate(); err != nil {
		t.Fatalf("unset GOMEMLIMIT should skip validation, got %v", err)
	}
}

func TestReservoir_Available(t *testing.T) {
	const limit = 1000 << 20
	withMemLimit(t, limit)

	rr := NewReservoirRegistry()
	r := NewReservoir("pool", 500<<20)
	rr.Register(r)
	r.Tracker().ForceReserve(200 << 20) // actual usage

	// available = 1000 − 200 (actual) − 100 (10% headroom) = 700 MiB.
	want := int64(700 << 20)
	if got := rr.Available(); got != want {
		t.Fatalf("Available = %d MiB, want %d MiB", got>>20, want>>20)
	}
}

func TestReservoir_Available_UnsetLimitIsUnlimited(t *testing.T) {
	withMemLimit(t, math.MaxInt64)
	rr := NewReservoirRegistry()
	rr.Register(NewReservoir("pool", 500<<20))
	if got := rr.Available(); got != math.MaxInt64 {
		t.Fatalf("unset GOMEMLIMIT Available = %d, want MaxInt64", got)
	}
}

func TestReservoir_Available_ClampsAtZero(t *testing.T) {
	const limit = 200 << 20
	withMemLimit(t, limit)
	rr := NewReservoirRegistry()
	r := NewReservoir("pool", 500<<20)
	rr.Register(r)
	r.Tracker().ForceReserve(300 << 20) // actual exceeds limit
	if got := rr.Available(); got != 0 {
		t.Fatalf("Available should clamp to 0, got %d", got)
	}
}

func TestReservoir_Accessors(t *testing.T) {
	r := NewReservoir("res", 123)
	if r.Name() != "res" {
		t.Fatalf("Name = %q", r.Name())
	}
	if r.Cap() != 123 {
		t.Fatalf("Cap = %d", r.Cap())
	}
	if r.Actual() != 0 {
		t.Fatalf("fresh Actual = %d, want 0", r.Actual())
	}
	r.Tracker().ForceReserve(40)
	if r.Actual() != 40 {
		t.Fatalf("Actual after reserve = %d, want 40", r.Actual())
	}
}
