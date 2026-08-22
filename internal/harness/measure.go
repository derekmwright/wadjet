package harness

import (
	"sync"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// MeasurementCollector subscribes to worker heartbeats and aggregates
// per-query measurement windows. One Collector instance is used for the
// entire run; queries demarcate measurement windows by calling
// StartWindow / EndWindow.
type MeasurementCollector struct {
	mu      sync.Mutex
	current *windowState

	// runPeakSpill is the highest hb.SpillDiskUsed seen across every
	// heartbeat for the collector's whole lifetime, independent of any
	// per-query window. Workers heartbeat on a fixed 10s cadence
	// (internal/worker/worker.go); a local-mode query suite frequently
	// finishes a single query in well under that period, so a per-query
	// window can easily open and close between two ticks and see zero
	// heartbeats at all — not because nothing happened, but because the
	// sampling missed it. Per-window Observe() calls below are also
	// dropped outright between windows (c.current == nil). Run-level
	// assertions like ExpectSpill need "did spill happen at any point
	// in this run," which this field answers regardless of that
	// per-query timing mismatch.
	runPeakSpill int64
}

type windowState struct {
	query         string
	startedAt     time.Time
	peakHeapMB    int64
	startMallocs  uint64
	endMallocs    uint64
	totalSpill    int64
	goroutinePeak int
}

// NewCollector creates a fresh collector with no active window.
func NewCollector() *MeasurementCollector {
	return &MeasurementCollector{}
}

// StartWindow begins a new measurement window for the given query.
// Any prior window is discarded.
func (c *MeasurementCollector) StartWindow(query string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = &windowState{
		query:     query,
		startedAt: time.Now(),
	}
}

// Observe feeds a heartbeat into the active window. Safe to call from
// the heartbeat subscriber goroutine.
func (c *MeasurementCollector) Observe(hb distributed.WorkerHeartbeat) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if hb.SpillDiskUsed > c.runPeakSpill {
		c.runPeakSpill = hb.SpillDiskUsed
	}
	if c.current == nil {
		return
	}
	if mb := hb.MemoryUsed / (1024 * 1024); mb > c.current.peakHeapMB {
		c.current.peakHeapMB = mb
	}
	if c.current.startMallocs == 0 {
		c.current.startMallocs = hb.Mallocs
	}
	c.current.endMallocs = hb.Mallocs
	if hb.SpillDiskUsed > c.current.totalSpill {
		c.current.totalSpill = hb.SpillDiskUsed
	}
	if hb.NumGoroutines > c.current.goroutinePeak {
		c.current.goroutinePeak = hb.NumGoroutines
	}
}

// RunPeakSpillBytes returns the highest hb.SpillDiskUsed seen across every
// heartbeat this collector has observed, regardless of per-query window
// boundaries. See the runPeakSpill field comment for why this is the
// reliable signal for a run-level "did spill happen at all" assertion.
func (c *MeasurementCollector) RunPeakSpillBytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runPeakSpill
}

// EndWindow finalizes the active window and returns its measurement.
// Returns the zero value if there's no active window or the query name
// doesn't match.
func (c *MeasurementCollector) EndWindow(query string) QueryMeasurement {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == nil || c.current.query != query {
		return QueryMeasurement{Query: query}
	}
	w := c.current
	c.current = nil
	return QueryMeasurement{
		Query:         w.query,
		StartedAt:     w.startedAt,
		WallMs:        time.Since(w.startedAt).Milliseconds(),
		PeakHeapMB:    w.peakHeapMB,
		AllocCount:    int64(w.endMallocs - w.startMallocs),
		SpillBytes:    w.totalSpill,
		GoroutinePeak: w.goroutinePeak,
	}
}

// HangDetector watches a goroutine count series and trips when the
// count grows monotonically for longer than the threshold duration.
type HangDetector struct {
	threshold      time.Duration
	lastCount      int
	monotonicSince time.Time
	started        bool
	tripped        bool
}

// NewHangDetector creates a detector with the given threshold (e.g. 30s).
func NewHangDetector(threshold time.Duration) *HangDetector {
	return &HangDetector{threshold: threshold}
}

// Observe records one (timestamp, goroutine count) sample. Returns true
// if a hang has been detected. Once tripped, returns true forever until
// Reset is called.
func (h *HangDetector) Observe(t time.Time, count int) bool {
	if h.tripped {
		return true
	}
	if !h.started {
		h.started = true
		h.monotonicSince = t
		h.lastCount = count
		return false
	}
	if count > h.lastCount {
		h.lastCount = count
		if t.Sub(h.monotonicSince) >= h.threshold {
			h.tripped = true
			return true
		}
		return false
	}
	// count did not strictly grow — reset the monotonic window
	h.monotonicSince = t
	h.lastCount = count
	return false
}

// Reset clears the trip state and starts over.
func (h *HangDetector) Reset() {
	h.started = false
	h.monotonicSince = time.Time{}
	h.lastCount = 0
	h.tripped = false
}
