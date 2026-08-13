package worker

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"
)

// bytePacer is a token bucket that smooths background stage-output PUT
// bytes below the NIC's instantaneous allowance
// (docs/design/upload-burst-smoothing.md). The 2026-08-13 ENA×stage
// attribution measured 68% of bw_out_allowance_exceeded events inside
// join-stage windows, where a completed task wave starts every output's
// PUT simultaneously: 8 streams × full speed ≈ 2.5 Gbps against a
// ~1.875 Gbps baseline allowance. The clamp that follows throttles the
// WHOLE NIC — including peer-fetch streams and NATS liveness traffic
// that ARE on the critical path, while the PUTs themselves are pure
// fault-tolerance insurance (ADR-0007: ~425 GB PUT per suite pair, zero
// bytes ever read back). Averaged over a suite the PUT rate sits far
// below any sane cap, so pacing clips only the completion-wave peaks.
//
// Post-charge design: the caller charges AFTER each chunk lands and then
// sleeps off any debt. That keeps the hot path to one mutex hit per
// uploadChunkBytes (1 MiB ≈ 6ms of budget at typical rates) and
// guarantees progress by construction — the sleep is bounded by
// chunkBytes/rate, there is no freeze state (the v1-QoS full-freeze trap,
// SF1 209s), and demand-released (urgent) roots bypass pacing entirely at
// the call site (the q18 demand-release trap).
type bytePacer struct {
	mu      sync.Mutex
	rate    float64 // bytes per second
	burst   float64 // bucket capacity in bytes
	balance float64 // current tokens; may go negative (debt)
	last    time.Time
}

// newBytePacer returns a pacer refilling at rate bytes/sec with the given
// burst capacity, or nil when rate <= 0 (pacing off).
func newBytePacer(rate float64, burst float64) *bytePacer {
	if rate <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = rate / 4 // 250ms of headroom absorbs sub-chunk jitter
	}
	return &bytePacer{rate: rate, burst: burst, balance: burst, last: time.Now()}
}

// charge deducts n bytes and returns how long the caller must sleep for
// the bucket to climb back to zero. Never blocks itself.
func (p *bytePacer) charge(n int) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	p.balance += now.Sub(p.last).Seconds() * p.rate
	if p.balance > p.burst {
		p.balance = p.burst
	}
	p.last = now
	p.balance -= float64(n)
	if p.balance >= 0 {
		return 0
	}
	return time.Duration(-p.balance / p.rate * float64(time.Second))
}

// waitAfter charges n bytes and sleeps off any debt, honoring ctx. Returns
// false only when ctx ended. Sleep per call is bounded by n/rate.
func (p *bytePacer) waitAfter(ctx context.Context, n int, waitNs *int64) bool {
	d := p.charge(n)
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		if waitNs != nil {
			*waitNs += int64(d)
		}
		return true
	case <-ctx.Done():
		return false
	}
}

// uploadPaceRate reads WADJET_UPLOAD_PACE_MBPS: aggregate background-PUT
// budget in MB/s (decimal, wire bytes post-compression). 0/unset/invalid
// = pacing off. Sizing guide: c7gd.4xlarge baseline allowance ≈ 1.875
// Gbps ≈ 234 MB/s total; the A/B arms pace PUTs at ~150 MB/s, leaving
// ~85 MB/s of always-available headroom for peer serving, results, and
// NATS. A var (not const) only for tests.
var uploadPaceRate = func() float64 {
	v := os.Getenv("WADJET_UPLOAD_PACE_MBPS")
	if v == "" {
		return 0
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n * 1e6
}()
