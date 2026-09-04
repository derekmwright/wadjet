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
