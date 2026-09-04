package memory

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// #853: a reservation the budget cannot hold AT ALL spends the whole relief
// wait reaching a fallback that was inevitable at the first comparison.
//
// The wall is the assertion, and it is a hard one on purpose: the wait is two
// seconds in the caller that made this visible (physical.fileLoadReserveWait),
// so a fix that leaves any part of the wait in place fails here rather than
// being argued about.
func TestAReservationLargerThanTheBudgetDoesNotWait(t *testing.T) {
	const budget = 512 << 10
	const wait = 2 * time.Second

	tr := NewTracker("query", budget)
	start := time.Now()
	forced := ReserveOrForce(context.Background(), tr, nil, budget+1, wait, ForceScanFileLoad)
	elapsed := time.Since(start)

	if !forced {
		t.Fatal("a request larger than the budget was admitted cleanly; the tracker's " +
			"budget stopped meaning anything")
	}
	if elapsed > wait/4 {
		t.Errorf("ReserveOrForce spent %v of a %v relief wait on a reservation of %d "+
			"against a %d budget — no amount of relief makes that fit, so the wait can "+
			"only reach the ForceReserve it was already going to do (#853)",
			elapsed, wait, budget+1, budget)
	}
	if got := tr.ForcedFor(ForceScanFileLoad); got != budget+1 {
		t.Errorf("the forced census reports %d for %q; the charge was %d",
			got, ForceScanFileLoad, budget+1)
	}
}

// The wait STAYS for a request the budget could hold: that is the case where
// waiting can work, and #853's boundary is exactly "n > budget", not "the
// tracker is full".
func TestAReservationTheBudgetCouldHoldStillWaits(t *testing.T) {
	const budget = 512 << 10
	tr := NewTracker("query", budget)
	tr.ForceReserveFor(budget, ForceScanDecodedBatch) // full, but not by this request

	start := time.Now()
	forced := ReserveOrForce(context.Background(), tr, nil, budget/2, 300*time.Millisecond,
		ForceScanFileLoad)
	elapsed := time.Since(start)

	if !forced {
		t.Fatal("the reservation succeeded; nothing released the room it needed")
	}
	if elapsed < 250*time.Millisecond {
		t.Errorf("ReserveOrForce waited only %v for a request of %d against a %d budget "+
			"that is momentarily full; a request the budget CAN hold is exactly the case "+
			"the wait exists for", elapsed, budget/2, budget)
	}
}

// BOTH release paths report an over-release. The WARN is the instrument a
// query-level gate counts, and an over-release is the same accounting bug
// whichever counter it came off — so routing one path around it would leave the
// instrument blind to exactly the producer that used it. That is not
// hypothetical: `SpillManager.ReleaseTracking` moved onto `ReleaseForced` in
// this arc, and `HashAggregate.Close` releases its group memory through it.
func TestBothReleasePathsReportAnOverRelease(t *testing.T) {
	for _, tc := range []struct {
		name    string
		release func(tr *Tracker)
	}{
		{"Release", func(tr *Tracker) { tr.Release(4096) }},
		{"ReleaseForced", func(tr *Tracker) { tr.ReleaseForced(4096, ForceSpillTracking) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var warned atomic.Int64
			prev := slog.Default()
			t.Cleanup(func() { slog.SetDefault(prev) })
			slog.SetDefault(slog.New(&underflowCounter{
				inner: slog.NewTextHandler(io.Discard, nil), n: &warned,
			}))

			tr := NewTracker("query", 1<<20)
			tc.release(tr) // nothing was ever reserved
			if tr.Used() >= 0 {
				t.Fatalf("used=%d; this cell did not reach an underflow at all", tr.Used())
			}
			if warned.Load() != 1 {
				t.Errorf("%s drove used to %d and warned %d times; an over-release that "+
					"says nothing is an over-release nobody finds", tc.name, tr.Used(),
					warned.Load())
			}
			// Once per tracker: an over-release repeats on every batch after
			// the first, and a line per batch is a line nobody reads.
			tc.release(tr)
			if warned.Load() != 1 {
				t.Errorf("%s warned %d times; the WARN is once per tracker", tc.name,
					warned.Load())
			}
		})
	}
}

type underflowCounter struct {
	inner slog.Handler
	n     *atomic.Int64
}

func (h *underflowCounter) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *underflowCounter) Handle(ctx context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "released more than was reserved") {
		h.n.Add(1)
	}
	return h.inner.Handle(ctx, r)
}

func (h *underflowCounter) WithAttrs(a []slog.Attr) slog.Handler {
	return &underflowCounter{inner: h.inner.WithAttrs(a), n: h.n}
}

func (h *underflowCounter) WithGroup(name string) slog.Handler {
	return &underflowCounter{inner: h.inner.WithGroup(name), n: h.n}
}

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
