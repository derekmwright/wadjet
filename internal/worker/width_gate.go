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

// widthDonate gates producer token donation (docs/design/
// shuffle-decode-ahead.md §2.2): a consumer yielding a pool token offers it
// to its own fragment's token-stalled decode-ahead scanner before releasing
// it to the pool. WADJET_WIDTH_DONATE=0 restores unconditional
// yield-to-pool. Package var so tests can pin either mode.
var widthDonate = os.Getenv("WADJET_WIDTH_DONATE") != "0"

// widthClaimDonate gates claim-path donation (docs/design/
// shuffle-decode-ahead.md §2.3), the deep-starvation admission path: a
// consumer whose FIFO claim grant lands while its own fragment's scanner is
// token-stalled redirects that grant to the scanner and re-parks, at most
// once per claim. WADJET_CLAIM_DONATE=0 restores grant-always-sticks
// (WADJET_WIDTH_DONATE=0 disables both donation paths). Package var so
// tests can pin either mode.
var widthClaimDonate = os.Getenv("WADJET_CLAIM_DONATE") != "0"

// producerTokenDonor is implemented by fragment sources whose producer-side
// decode can accept a directly donated cpu token. tryDonateToken must be
// safe to call from any consumer goroutine; true means ownership of one
// token transferred (the caller must NOT release it to the pool).
type producerTokenDonor interface {
	tryDonateToken() bool
}

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
	// donor, when non-nil, is the fragment's own source: a pool token
	// yielded on dispenser-dry is offered to its token-stalled decode
	// scanner before the pool (§2.2 donation). Set once before consumers
	// start; the baseline slot is never donated.
	donor producerTokenDonor
	// baseline carries the fragment's one free slot; buffered cap 1, primed
	// at construction.
	baseline chan struct{}

	// Stats for the fragment-completion log line. fedParks/dryParks split
	// the FIFO parks by morsel-ring occupancy at claim time — the
	// discriminator the pool's admission policy runs on, so the next SF100
	// run can read the regime straight off the fragment line.
	claimWaitNs    atomic.Int64
	yields         atomic.Int64
	donations      atomic.Int64
	claimDonations atomic.Int64
	fedParks       atomic.Int64
	dryParks       atomic.Int64
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
//
// fed is the morsel ring's occupancy behind the morsel in hand
// (len(dispenser.ch) > 0). It rides into the FIFO because the pool's
// admission policy turns on it: a fed waiter keeps strict priority over
// decode-ahead (the original rule, in the regime it was written for), a dry
// one does not — feeding a consumer whose ring is empty cannot buy
// throughput until a decoder refills it (cpu_tokens.go).
//
// Claim-path donation (§2.3): a granted waiter whose own fragment's scanner
// is at that moment parked token-stalled cedes its grant to the scanner and
// re-enqueues, at most once per claim — the second grant always sticks, so
// per-morsel delay is bounded at one extra FIFO wait. This is the only
// producer admission path in the deep-starvation mode, where every consumer
// is a slot-less waiter and the §2.2 yield-path precondition (a HELD token
// going dry) never occurs. The redirected token never re-enters the pool:
// grant → tryDonateToken → chunk decode → worker release, the same bounded
// ownership chain as a yield donation.
func (g *widthGate) claim(ctx context.Context, fed bool) (widthSlot, error) {
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
	if fed {
		g.fedParks.Add(1)
	} else {
		g.dryParks.Add(1)
	}
	redirected := false
	for {
		w := g.tokens.enqueueWaiter(fed)
		select {
		case <-g.baseline:
			g.tokens.cancelWaiter(w)
			return slotBaseline, nil
		case <-w.ch:
			if !redirected && widthDonate && widthClaimDonate &&
				g.donor != nil && g.donor.tryDonateToken() {
				redirected = true
				g.claimDonations.Add(1)
				continue
			}
			return slotToken, nil
		case <-ctx.Done():
			g.tokens.cancelWaiter(w)
			return slotNone, ctx.Err()
		}
	}
}

// yield returns a held slot. Safe to call with slotNone. A pool token is
// first offered to the fragment's own producer (§2.2 donation) — accepted
// only while its decode scanner is parked token-stalled, i.e. exactly when
// this consumer's input starvation is producer-caused — and goes to the
// pool otherwise.
func (g *widthGate) yield(s widthSlot) {
	switch s {
	case slotBaseline:
		g.baseline <- struct{}{}
	case slotToken:
		if widthDonate && g.donor != nil && g.donor.tryDonateToken() {
			g.donations.Add(1)
		} else {
			g.tokens.Release(1)
		}
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
		"width_donations", g.donations.Load(),
		"width_claim_donations", g.claimDonations.Load(),
		"width_fed_parks", g.fedParks.Load(),
		"width_dry_parks", g.dryParks.Load(),
	}
}
