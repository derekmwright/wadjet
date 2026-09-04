package memory

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// ReserveOrForce reserves n bytes on t before a large allocation, applying
// pre-emptive backpressure instead of reactive spill: a clean Reserve is
// tried first; on a budget miss the spill manager (when present) is asked
// to free the shortfall and the reservation is retried for up to wait. If
// the budget still doesn't admit it, the bytes are force-reserved so the
// ledger stays honest — the caller's allocation proceeds either way.
//
// This is the non-deadlocking shape the accounting overhaul requires:
// per-allocation gating with a bounded wait and a forced fallback, never
// operator-entry gating (see project_admission_control_rejected_2026-05-18 —
// gating entry on reservations deadlocks under chained build/probe).
//
// # A request larger than the whole budget does not wait (#853)
//
// Relief and the retry are both conditioned on the budget being able to hold n
// AT ALL. When n exceeds the budget itself, neither can succeed: no amount of
// spilling makes a 912 KiB file fit a 512 KiB allowance, and ReserveBlocking
// polls a condition that is false for every value `used` can take, including
// zero. The whole wait is then spent to reach the ForceReserve that was
// inevitable at the first line — measured at 2 s per parquet file load, times
// one load per outer row on a re-run correlated shape
// (coordinator.TestCorrelatedRerunPaysTheFullReserveWaitPerOuterRow, 4.1 s for
// two rows against 0.1 s of work).
//
// So that case takes the documented forced path immediately. It is the SAME
// path with the same WARN and the same ledger effect — nothing about the
// query's admission changes, only the seconds spent proving what the first
// comparison already knew. Peers are not asked to spill for it either: a
// spill that cannot make the reservation succeed costs another operator its
// memory for nothing.
//
// The wait stays for n <= budget, which is every case where waiting can work.
//
// Returns true when the fallback fired and the reservation was forced.
func ReserveOrForce(ctx context.Context, t *Tracker, sm *SpillManager, n int64, wait time.Duration, purpose ForcePurpose) bool {
	if t == nil || n <= 0 {
		return false
	}
	err := t.Reserve(n)
	if err == nil {
		return false
	}
	if !errors.Is(err, ErrMemoryExceeded) {
		// No non-budget Reserve errors exist today; stay honest if one appears.
		t.ForceReserveFor(n, purpose)
		return true
	}
	// Larger than the entire budget: relief and the retry are both impossible,
	// so skip to the fallback rather than spending the wait reaching it.
	if t.budget > 0 && n > t.budget {
		return forceWithWarning(ctx, t, n, purpose)
	}
	if sm != nil {
		// Ask registered operators to spill the shortfall before waiting on
		// the budget. RequestRelief accumulates across willing operators and
		// never gates entry, so this cannot deadlock the streaming path.
		_, _ = sm.RequestRelief(n)
	}
	if wait > 0 {
		waitCtx, cancel := context.WithTimeout(ctx, wait)
		err = t.ReserveBlocking(waitCtx, n, 50*time.Millisecond)
		cancel()
		if err == nil {
			return false
		}
	}
	return forceWithWarning(ctx, t, n, purpose)
}

// forceWithWarning takes the forced path and says so once per call, naming the
// purpose the charge is attributed to.
func forceWithWarning(ctx context.Context, t *Tracker, n int64, purpose ForcePurpose) bool {
	t.ForceReserveFor(n, purpose)
	if ctx.Err() == nil {
		slog.Warn("memory reservation forced past budget",
			"tracker", t.name,
			"purpose", purpose.String(),
			"bytes", n,
			"used", t.Used(),
			"budget", t.budget,
		)
	}
	return true
}
