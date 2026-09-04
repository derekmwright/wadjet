package memory

import (
	"strings"
	"testing"
)

// Every producer that can charge past the budget names what the charge is for,
// and every one of them gives it back under the same name. The census counts
// OUTSTANDING bytes, so an unpaired release leaves it describing memory that is
// no longer there — which is worse than not counting at all.
func TestAForcedChargeIsPairedWithItsRelease(t *testing.T) {
	for _, p := range []ForcePurpose{
		ForceUnattributed,
		ForceScanFileLoad,
		ForceScanDecodedBatch,
		ForceScanPooledBuffer,
		ForceJoinIndex,
		ForceJoinPartitionStore,
		ForceSpillTracking,
	} {
		t.Run(p.String(), func(t *testing.T) {
			tr := NewTracker("query", 1<<20)
			tr.ForceReserveFor(1000, p)
			if got := tr.ForcedFor(p); got != 1000 {
				t.Fatalf("ForcedFor(%v) = %d, want 1000", p, got)
			}
			if got := tr.Used(); got != 1000 {
				t.Fatalf("used = %d, want 1000", got)
			}
			total, top, topBytes := tr.ForcedBytes()
			if total != 1000 || top != p || topBytes != 1000 {
				t.Fatalf("ForcedBytes() = (%d, %v, %d), want (1000, %v, 1000)",
					total, top, topBytes, p)
			}
			tr.ReleaseForced(1000, p)
			if got := tr.ForcedFor(p); got != 0 {
				t.Fatalf("ForcedFor(%v) = %d after the release", p, got)
			}
			if got := tr.Used(); got != 0 {
				t.Fatalf("used = %d after the release", got)
			}
		})
	}
}

// A forced charge bubbles to the parent under the same purpose, so a query
// tracker's census is readable from the pool it charges.
func TestAForcedChargeBubblesWithItsPurpose(t *testing.T) {
	pool := NewTracker("pool", 4<<20)
	q := pool.Child("query")
	q.ForceReserveFor(2048, ForceJoinIndex)
	if got := pool.ForcedFor(ForceJoinIndex); got != 2048 {
		t.Errorf("the pool's census reports %d for the child's forced index charge", got)
	}
	q.ReleaseForced(2048, ForceJoinIndex)
	if got := pool.ForcedFor(ForceJoinIndex); got != 0 {
		t.Errorf("the pool's census still reports %d after the child released it", got)
	}
}

// A Transfer is not a new charge: counting it as forced would grow a census
// nothing decrements.
func TestATransferDoesNotEnterTheForcedCensus(t *testing.T) {
	pool := NewTracker("pool", 4<<20)
	a, b := pool.Child("a"), pool.Child("b")
	if err := a.Reserve(4096); err != nil {
		t.Fatal(err)
	}
	a.Transfer(a, b, 4096)
	if got, _, _ := pool.ForcedBytes(); got != 0 {
		t.Errorf("the forced census reports %d bytes after a transfer of clean bytes", got)
	}
	if got := pool.Used(); got != 4096 {
		t.Errorf("the pool's total is %d after a transfer that conserves it", got)
	}
}

// The refusal is the diagnostic #789's investigation did not have: `used=465738`
// with nothing to say about whose bytes those were. It names the largest
// purpose, and the total when more than one purpose is holding bytes.
func TestARefusalNamesWhatWasForced(t *testing.T) {
	const budget = 512 << 10
	tr := NewTracker("query", budget)
	tr.ForceReserveFor(412074, ForceScanFileLoad)

	err := tr.Reserve(budget)
	if err == nil {
		t.Fatal("the reservation succeeded against a budget that is already 79% forced")
	}
	if !strings.Contains(err.Error(), `of which forced=412074 by "scan file load"`) {
		t.Errorf("the refusal does not name the forced charge holding the budget:\n  %v", err)
	}

	tr.ForceReserveFor(98304, ForceJoinIndex)
	err = tr.Reserve(budget)
	if err == nil {
		t.Fatal("the reservation succeeded")
	}
	if !strings.Contains(err.Error(), `of which forced=510378 (largest: 412074 by "scan file load")`) {
		t.Errorf("with two purposes holding bytes the refusal does not report the total "+
			"and the largest:\n  %v", err)
	}
}

// A refusal on a tracker nothing forced reads exactly as it always did: the
// suffix is a diagnostic for a condition, not decoration.
func TestARefusalWithNothingForcedIsUnchanged(t *testing.T) {
	tr := NewTracker("query", 1024)
	err := tr.Reserve(2048)
	if err == nil {
		t.Fatal("the reservation succeeded")
	}
	if strings.Contains(err.Error(), "forced") {
		t.Errorf("nothing was forced, but the refusal says it was:\n  %v", err)
	}
	want := "query: memory budget exceeded (used=0, requested=2048, budget=1024)"
	if err.Error() != want {
		t.Errorf("refusal = %q, want %q", err.Error(), want)
	}
}
