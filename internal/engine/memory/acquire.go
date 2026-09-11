package memory

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// ReserveOrForce tries Reserve before allocation, requests spill relief on a
// budget miss and retries for at most wait, then ForceReserve keeps the ledger
// honest while allocation proceeds. Return true only for forced fallback.
// If n exceeds the whole positive budget, force immediately without relief
// or waiting: neither can make admission possible (#853).
// Gate per allocation, never operator entry: chained build/probe can deadlock
// (project_admission_control_rejected_2026-05-18).
// See docs/internals/memory-bounded-reserve-or-force.md for the design.
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
