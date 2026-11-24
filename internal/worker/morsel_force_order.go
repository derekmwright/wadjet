// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"sync/atomic"
)

// The forced representative race is TEST ONLY (#1713). Which of two equal
// values a dedup keeps (`7` or `7.00`) is observable through a text cast, and
// on the morsel-parallel path it used to follow goroutine timing: the
// unpartitioned stage sink gives every concurrent Consume its own slab, so
// one arm's output becomes several chunks, and the breaker's clones merge in
// index order whatever order their input arrived in. Both halves of that
// interleaving happen naturally about once in three hundred runs; the knob
// makes them happen every time, so a gate fails at the defect, not by chance.
//
// Armed, it does two things:
//
//   - the unpartitioned stage sink never reuses a slab, so every Consume of a
//     morsel-parallel producer writes its own chunk (the split the natural
//     race needs);
//   - a morsel-parallel breaker consume takes a fixed schedule instead of
//     racing its consumers: the first half of the morsels after the warmup go
//     to the LAST consumer, the second half to the primary, each consumer
//     taking its own morsels in arrival order. It is one legal interleaving
//     of the real dispatch (the last consumer took the early morsels and was
//     slow; the primary took the later ones) and the least favourable one for
//     a merge that keeps the first representative it meets.
//
// Production never arms it; unset it costs one atomic load per Consume and
// one per breaker fan-out. ForcedSlabSplits counts the slabs the knob minted,
// so a gate can assert that it engaged.
var forceRepresentativeRace atomic.Bool

// ForceRepresentativeRace arms (true) or disarms (false) the forced
// representative race for every fragment in this process and returns the
// previous setting so a test can restore it. TEST ONLY.
func ForceRepresentativeRace(on bool) bool {
	return forceRepresentativeRace.Swap(on)
}

// ForcedSlabSplits counts the slabs minted because the knob refused a free
// one. ForcedBreakerSchedules counts breaker fan-outs that took the fixed
// schedule.
var (
	ForcedSlabSplits       atomic.Int64
	ForcedBreakerSchedules atomic.Int64
)

// runForcedBreakerSchedule drains the dispenser and feeds the morsels to the
// consumers on the knob's fixed schedule, one at a time, in arrival order.
// It holds the whole input before it pushes any of it, which only a test's
// small fixture can afford.
func runForcedBreakerSchedule(ctx context.Context, d *morselDispenser, drivers []*fragmentDriver) error {
	ForcedBreakerSchedules.Add(1)
	var ms []morsel
	for m := range d.ch {
		ms = append(ms, m)
	}
	k := len(drivers)
	for j, m := range ms {
		c := 0
		if j < len(ms)/2 {
			c = k - 1
		}
		_, err := drivers[c].push(ctx, m.b)
		m.retire()
		if err != nil {
			for _, rest := range ms[j+1:] {
				rest.retire()
			}
			return err
		}
	}
	return nil
}
