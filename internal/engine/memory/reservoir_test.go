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
	const limit = 20 << 30 // 20 GiB; headroom = 0.20*20 = 4 GiB (ratio, not floor)
	withMemLimit(t, limit)

	// Fits: cap 15 GiB + min 0.0625 + headroom 4 = 19.06 <= 20.
	rr := NewReservoirRegistry()
	rr.Register(NewReservoir("pool", 15<<30))
	if err := rr.Validate(); err != nil {
		t.Fatalf("expected pass, got %v", err)
	}

	// Violates: cap 17 GiB + 0.0625 + 4 = 21.06 > 20.
	rr2 := NewReservoirRegistry()
	rr2.Register(NewReservoir("pool", 17<<30))
	if err := rr2.Validate(); err == nil {
		t.Fatal("expected invariant violation error, got nil")
	}
}

func TestReservoir_Validate_SumsMultipleCaps(t *testing.T) {
	const limit = 20 << 30 // headroom 4 GiB
	withMemLimit(t, limit)

	rr := NewReservoirRegistry()
	rr.Register(NewReservoir("a", 7<<30))
	rr.Register(NewReservoir("b", 7<<30)) // Σcaps=14; +0.06+4=18.06 <= 20
	if err := rr.Validate(); err != nil {
		t.Fatalf("two caps within budget should pass, got %v", err)
	}
	rr.Register(NewReservoir("c", 3<<30)) // Σcaps=17; +0.06+4=21.06 > 20
	if err := rr.Validate(); err == nil {
		t.Fatal("third cap should push over budget")
	}
}

func TestReservoir_Validate_SoftCapExcluded(t *testing.T) {
	const limit = 20 << 30 // headroom 4 GiB
	withMemLimit(t, limit)

	rr := NewReservoirRegistry()
	rr.Register(NewReservoir("hard", 15<<30)) // 15 + 0.06 + 4 = 19.06 <= 20: passes
	// A soft reservoir with an enormous cap must NOT push the invariant over —
	// its cap is excluded from Σcaps.
	rr.Register(NewSoftReservoir("soft", 100<<30, func() int64 { return 0 }))
	if err := rr.Validate(); err != nil {
		t.Fatalf("soft cap must be excluded from the invariant, got %v", err)
	}
	// But the soft reservoir's live actual still subtracts from Available().
	rr2 := NewReservoirRegistry()
	soft := NewSoftReservoir("soft", 0, func() int64 { return 1 << 30 })
	rr2.Register(soft)
	// Available = 20 - 1 (soft actual) - 4 (headroom) = 15 GiB.
	if got, want := rr2.Available(), int64(15<<30); got != want {
		t.Fatalf("soft actual must subtract from Available: got %d GiB want %d GiB", got>>30, want>>30)
	}
}

func TestReservoir_GCHeadroom_FloorAndRatio(t *testing.T) {
	if got, want := gcHeadroom(4<<30), int64(2<<30); got != want { // 0.20*4=0.8 < 2 → floor
		t.Fatalf("gcHeadroom(4GiB) = %d, want floor 2GiB", got)
	}
	if got, want := gcHeadroom(40<<30), int64(8<<30); got != want { // 0.20*40=8 > 2 → ratio
		t.Fatalf("gcHeadroom(40GiB) = %d, want ratio 8GiB", got)
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
	const limit = 20 << 30 // headroom 4 GiB
	withMemLimit(t, limit)

	rr := NewReservoirRegistry()
	r := NewReservoir("pool", 10<<30)
	rr.Register(r)
	r.Tracker().ForceReserve(2 << 30) // actual usage

	// available = 20 − 2 (actual) − 4 (headroom) = 14 GiB.
	want := int64(14 << 30)
	if got := rr.Available(); got != want {
		t.Fatalf("Available = %d GiB, want %d GiB", got>>30, want>>30)
	}
}

// TestReservoir_Available_LiveAccessor confirms NewReservoirFunc's Actual()
// tracks a live accessor rather than the internal tracker.
func TestReservoir_Available_LiveAccessor(t *testing.T) {
	const limit = 20 << 30
	withMemLimit(t, limit)

	live := int64(1 << 30)
	rr := NewReservoirRegistry()
	rr.Register(NewReservoirFunc("store", 5<<30, func() int64 { return live }))

	// available = 20 - 1 (live) - 4 (headroom) = 15 GiB.
	if got, want := rr.Available(), int64(15<<30); got != want {
		t.Fatalf("Available = %d GiB, want %d GiB", got>>30, want>>30)
	}
	live = 3 << 30 // mutate the live source
	if got, want := rr.Available(), int64(13<<30); got != want {
		t.Fatalf("after live change: Available = %d GiB, want %d GiB", got>>30, want>>30)
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

func TestReservoir_TotalActual(t *testing.T) {
	rr := NewReservoirRegistry()
	a := NewReservoir("a", 100)
	a.Tracker().ForceReserve(40)
	rr.Register(a)
	rr.Register(NewReservoirFunc("b", 100, func() int64 { return 25 }))
	rr.Register(NewSoftReservoir("c", 0, func() int64 { return 10 })) // soft counts too
	if got, want := rr.TotalActual(), int64(40+25+10); got != want {
		t.Fatalf("TotalActual = %d, want %d", got, want)
	}

	// Regression: Available() must equal limit − TotalActual − gcHeadroom after
	// the shared-helper refactor.
	const limit = 20 << 30
	withMemLimit(t, limit)
	want := limit - rr.TotalActual() - gcHeadroom(limit)
	if got := rr.Available(); got != want {
		t.Fatalf("Available = %d, want %d (limit − TotalActual − gcHeadroom)", got, want)
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
