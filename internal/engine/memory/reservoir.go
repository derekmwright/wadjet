package memory

import (
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"sync"
)

// gcHeadroomRatio is the fraction of GOMEMLIMIT held back as GC headroom — the
// slack the runtime needs between live heap and the hard limit to avoid GC
// thrash. It is the single home for the previously-scattered ad-hoc headroom
// math (the goMemLimit/10 cache headroom in cmd/wadjet/main.go).
const gcHeadroomRatio = 0.10

// minOperatorBytes is the smallest in-flight operator footprint the boot
// invariant reserves so a single task can always make progress.
const minOperatorBytes int64 = 64 << 20 // 64 MiB

// Reservoir is a named, capped pool of bytes carved out of the worker's
// GOMEMLIMIT envelope (e.g. the LRU file cache, the result store, codec pools).
// It is a TierSystemReservoir in the accounting model: observed and bounded at
// admission, never spilled reactively. Each reservoir wraps a Tracker so callers
// Reserve/Release against it and so its live usage feeds the effective-budget
// accessor; the recorded cap feeds the boot-time invariant.
//
// Phase 1 introduces Reservoir alongside the existing flat sharedPoolBudget
// Tracker — it does not replace it. The multi-reservoir carve-up and the hard
// refuse-to-start gate land in later phases once every reservoir is registered
// and the invariant constants are validated against the constrained-memory
// deploy envelopes.
type Reservoir struct {
	name    string
	cap     int64
	tracker *Tracker
}

// NewReservoir creates a reservoir with a hard cap, backed by a Tracker whose
// budget equals the cap.
func NewReservoir(name string, cap int64) *Reservoir {
	return &Reservoir{
		name:    name,
		cap:     cap,
		tracker: NewTracker(name, cap),
	}
}

// Name returns the reservoir name.
func (r *Reservoir) Name() string { return r.name }

// Cap returns the reservoir's hard byte cap.
func (r *Reservoir) Cap() int64 { return r.cap }

// Tracker exposes the backing tracker so callers Reserve/Release against it.
func (r *Reservoir) Tracker() *Tracker { return r.tracker }

// Actual returns the bytes currently accounted in this reservoir.
func (r *Reservoir) Actual() int64 { return r.tracker.Used() }

// ReservoirRegistry holds all reservoirs for a worker and validates the
// boot-time GOMEMLIMIT invariant. One instance lives per process.
type ReservoirRegistry struct {
	mu         sync.Mutex
	reservoirs []*Reservoir
}

// NewReservoirRegistry creates an empty registry.
func NewReservoirRegistry() *ReservoirRegistry {
	return &ReservoirRegistry{}
}

// Register adds a reservoir to the registry.
func (rr *ReservoirRegistry) Register(r *Reservoir) {
	rr.mu.Lock()
	rr.reservoirs = append(rr.reservoirs, r)
	rr.mu.Unlock()
}

// goMemLimit reads the true GOMEMLIMIT via the runtime, matching the
// heapPressureMemLimit read in spill.go (debug.SetMemoryLimit(-1)). It returns
// the math.MaxInt64 / <=0 "unset" sentinels unchanged so callers detect an
// unconfigured limit and skip validation.
func goMemLimit() int64 {
	return debug.SetMemoryLimit(-1)
}

// Validate enforces the boot-time invariant: the sum of reservoir caps plus a
// minimum operator footprint plus GC headroom must fit inside GOMEMLIMIT. It
// returns a non-nil error describing the shortfall otherwise. It is a no-op
// (returns nil) when GOMEMLIMIT is unset (<=0 or math.MaxInt64), matching the
// spill.go heap-pressure guards.
//
// Validate reports the verdict; the caller decides whether a violation is fatal.
// Phase 1 callers log it; the hard refuse-to-start lands once every reservoir is
// registered (later phases).
func (rr *ReservoirRegistry) Validate() error {
	limit := goMemLimit()
	if limit <= 0 || limit == math.MaxInt64 {
		return nil // GOMEMLIMIT not configured; nothing to validate against
	}

	rr.mu.Lock()
	defer rr.mu.Unlock()

	var sumCaps int64
	for _, r := range rr.reservoirs {
		sumCaps += r.cap
	}
	headroom := int64(float64(limit) * gcHeadroomRatio)
	required := sumCaps + minOperatorBytes + headroom

	if required > limit {
		return fmt.Errorf("memory reservoir invariant violated: "+
			"Σcaps(%d) + min_operator(%d) + gc_headroom(%d) = %d > GOMEMLIMIT(%d)",
			sumCaps, minOperatorBytes, headroom, required, limit)
	}
	return nil
}

// Available returns the effective budget still spendable by operators:
//
//	GOMEMLIMIT − Σ(reservoir actual usage) − GC headroom
//
// It uses the live runtime GOMEMLIMIT read, not a cached config value, so the
// budget floats with real reservoir occupancy. It returns math.MaxInt64 when
// GOMEMLIMIT is unset (unlimited) and clamps to 0 rather than going negative.
func (rr *ReservoirRegistry) Available() int64 {
	limit := goMemLimit()
	if limit <= 0 || limit == math.MaxInt64 {
		return math.MaxInt64
	}
	rr.mu.Lock()
	defer rr.mu.Unlock()

	var actual int64
	for _, r := range rr.reservoirs {
		actual += r.Actual()
	}
	headroom := int64(float64(limit) * gcHeadroomRatio)
	avail := limit - actual - headroom
	if avail < 0 {
		avail = 0
	}
	return avail
}

// LogInvariant validates and logs the boot-time invariant without refusing to
// start. Phase 1 wiring: surfaces the numbers on every worker boot so the
// constrained-memory deploy envelopes can be confirmed before a later phase
// makes the invariant fatal. A violation logs at WARN; satisfaction at INFO.
func (rr *ReservoirRegistry) LogInvariant(logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	limit := goMemLimit()
	if limit <= 0 || limit == math.MaxInt64 {
		logger.Info("memory reservoir invariant: GOMEMLIMIT unset, skipping")
		return
	}
	rr.mu.Lock()
	var sumCaps int64
	for _, r := range rr.reservoirs {
		sumCaps += r.cap
	}
	rr.mu.Unlock()
	headroom := int64(float64(limit) * gcHeadroomRatio)

	if err := rr.Validate(); err != nil {
		logger.Warn("memory reservoir invariant violated (advisory in this phase)",
			"error", err.Error())
		return
	}
	logger.Info("memory reservoir invariant satisfied",
		"sum_caps_mb", sumCaps>>20,
		"min_operator_mb", minOperatorBytes>>20,
		"gc_headroom_mb", headroom>>20,
		"gomemlimit_mb", limit>>20,
	)
}
