package worker

import (
	"context"
	"os"
	"sync/atomic"
	"time"
)

// morselWidthYield gates work-conserving morsel width (docs/design/
// morsel-execution.md §4.2.1). Default on; WADJET_MORSEL_YIELD=0 restores
// the fixed-width shape (tokens acquired once at fragment start and held for
// the fragment's lifetime, width frozen at whatever was free at that
// moment). Package var so tests can pin either mode.
var morselWidthYield = os.Getenv("WADJET_MORSEL_YIELD") != "0"

// widthSlot is what one morsel consumer currently holds: nothing, the
// fragment's free baseline slot, or one worker-pool cpu token.
type widthSlot uint8

const (
	slotNone widthSlot = iota
	slotBaseline
	slotToken
)

// widthGate meters a parallel fragment's ACTIVE consumer width against the
// worker-wide cpuTokens pool, work-conservingly: a consumer holds its slot
// only while it is actually processing a morsel and yields it whenever the
// dispenser runs dry. The fixed-width shape this replaces held width tokens
// through input stalls, which starved the very token takers producing that
// input — decode-ahead collapsed to cursor-only under two concurrent
// probe-split fragments (SF100 2026-07-21 ref: 45s lineitem-stage floor,
// 126s cumulative token-stall on Q09 join-6, workers ~17% CPU-utilized).
//
// One baseline slot per fragment is always claimable token-free (the
// "first consumer is free" rule), which is also the progress guarantee that
// makes blocking claims safe: token holders are always actively computing
// and release in bounded time, and no fragment can wedge with work queued
// because the baseline cannot be held by an idle consumer.
type widthGate struct {
	tokens *cpuTokens
	// baseline carries the fragment's one free slot; buffered cap 1, primed
	// at construction.
	baseline chan struct{}

	// Stats for the fragment-completion log line.
	claimWaitNs atomic.Int64
	yields      atomic.Int64
}

func newWidthGate(tokens *cpuTokens) *widthGate {
	g := &widthGate{tokens: tokens, baseline: make(chan struct{}, 1)}
	g.baseline <- struct{}{}
	return g
}

// claim blocks until the consumer may process a morsel, preferring the
// fragment baseline, then a free pool token, then parking FIFO for either.
// The caller holds an admitted morsel; see cpuTokens.enqueueWaiter for why
// parking here cannot deadlock.
func (g *widthGate) claim(ctx context.Context) (widthSlot, error) {
	select {
	case <-g.baseline:
		return slotBaseline, nil
	default:
	}
	if g.tokens.TryAcquire(1) == 1 {
		return slotToken, nil
	}
	t0 := time.Now()
	defer func() { g.claimWaitNs.Add(time.Since(t0).Nanoseconds()) }()
	w := g.tokens.enqueueWaiter()
	select {
	case <-g.baseline:
		g.tokens.cancelWaiter(w)
		return slotBaseline, nil
	case <-w.ch:
		return slotToken, nil
	case <-ctx.Done():
		g.tokens.cancelWaiter(w)
		return slotNone, ctx.Err()
	}
}

// yield returns a held slot. Safe to call with slotNone.
func (g *widthGate) yield(s widthSlot) {
	switch s {
	case slotBaseline:
		g.baseline <- struct{}{}
	case slotToken:
		g.tokens.Release(1)
	default:
		return
	}
	g.yields.Add(1)
}

// logAttrs summarizes the gate's activity for the fragment-completion line.
func (g *widthGate) logAttrs() []any {
	return []any{
		"width_wait_ms", g.claimWaitNs.Load() / 1e6,
		"width_yields", g.yields.Load(),
	}
}
