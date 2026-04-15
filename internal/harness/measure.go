package harness

import (
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// MeasurementCollector subscribes to worker heartbeats and aggregates
// per-query measurement windows. One Collector instance is used for the
// entire run; queries demarcate measurement windows by calling
// StartWindow / EndWindow.
type MeasurementCollector struct {
	mu      sync.Mutex
	current *windowState
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
