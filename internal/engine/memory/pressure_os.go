package memory

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Page-cache pressure sensor (docs/design/scan-decode-pipelining.md §9).
//
// The Go-heap pressure hooks above are blind to the failure mode that
// convicted the decode-ahead window: held heap bytes displace the page
// cache holding the file pages a scan is about to read, the kernel
// absorbs the squeeze silently, and Go-side accounting never crosses any
// threshold. The kernel DOES publish the displacement as it happens:
// workingset_refault counts faults on pages that were recently evicted —
// cache thrash by definition. Cold sequential reads are first-touch
// faults, not refaults, so a healthy streaming scan reads ~0/s while
// genuine displacement measured 15k-95k pages/s on the 2 GiB capped
// repro (2026-07-17) — four orders of magnitude of separation.
//
// The counter is read from the process's own cgroup v2 memory.stat when
// available (attributes pressure to this container), falling back to the
// host-wide /proc/vmstat (correct on dedicated workers; on shared hosts
// it may fire from a co-tenant's thrash, which only costs decode-ahead
// width at a moment when the disk is contended anyway — width is
// worthless mid-thrash: the capped repro measured window_fulls=0 once
// I/O-bound). No readable source disables the sensor permanently.
//
// A sample is taken at most once per second; the rate must exceed the
// threshold on two consecutive samples before the sensor reports active
// (one-burst damping), and one quiet sample deactivates it.

// refaultPressureRatePerSec is the default activation threshold in
// refaulted pages/sec (≈4 MB/s of cache re-reads). Quiet-box baseline is
// ~0/s and measured thrash is 15k+/s, so the constant sits well clear of
// both; it is a floor against incidental bursts, not a tuning point.
// Overridable via WADJET_REFAULT_PRESSURE_RATE (pages/sec, 0 disables).
const refaultPressureRatePerSec = 1000.0

// refaultSampleInterval is the minimum time between counter reads. Rate
// estimation over sub-second windows is noise; the sensor is consulted
// per row-group admission (~ms cadence) and must stay near-free.
const refaultSampleInterval = time.Second

// refaultPageBytes converts the streaming-discount byte rate to pages.
// workingset_refault counts 4 KiB pages on every platform we deploy to.
const refaultPageBytes = 4096

// streamingReadSource, when set, returns a monotonic cumulative byte count
// of the engine's own DESIGNED streaming re-reads — base-table cache
// hit-opens plus peer-serve reads of those same NVMe files. The sensor's
// founding premise ("healthy streaming ≈ 0 refaults") only holds while
// files are read for the FIRST time; once the NVMe cache exceeds RAM,
// every steady-state scan of a cached file is a workingset refault by
// kernel accounting, and the 2026-08-08 investigation measured the sensor
// classifying that designed behavior as thrash for 230-460s of stalls per
// worker per run. Subtracting the expected streaming page-in rate before
// thresholding restores the premise: genuine displacement thrash (15k+
// pages/s) still clears the threshold by an order of magnitude, while the
// engine's own ~5k pages/s of cache-file streaming no longer arms
// episodes. The source over-counts slightly (hit-opens are whole-file
// sizes; projection-pruned scans fault fewer pages) — acceptable, the
// separation argument needs only order-of-magnitude accuracy.
// Set once at worker startup; kill switch WADJET_REFAULT_STREAM_DISCOUNT=0.
var streamingReadSource struct {
	mu   sync.Mutex
	read func() int64
}

// SetPageCacheStreamingSource registers the designed-streaming byte
// counter the refault sensor discounts before thresholding. Disabled by
// WADJET_REFAULT_STREAM_DISCOUNT=0. Call before or after sensor
// construction; the sensor consults the registration at each sample.
func SetPageCacheStreamingSource(read func() int64) {
	if os.Getenv("WADJET_REFAULT_STREAM_DISCOUNT") == "0" {
		return
	}
	streamingReadSource.mu.Lock()
	streamingReadSource.read = read
	streamingReadSource.mu.Unlock()
}

func loadStreamingSource() func() int64 {
	streamingReadSource.mu.Lock()
	defer streamingReadSource.mu.Unlock()
	return streamingReadSource.read
}

// refaultSensor computes a refault rate from a monotonic counter source
// and applies two-sample activation damping. The zero value is unusable;
// construct with newRefaultSensor.
type refaultSensor struct {
	mu        sync.Mutex
	read      func() (int64, bool) // monotonic page count; false = source gone
	threshold float64              // pages/sec; <= 0 disables
	interval  time.Duration

	disabled   bool
	lastCount  int64
	lastSample time.Time
	lastRate   float64
	hotStreak  int // consecutive over-threshold samples
	active     bool

	// Designed-streaming discount (see streamingReadSource). streamBytes
	// is the source's cumulative value at the last sample; streamPrimed
	// distinguishes "no baseline yet" (first sample after registration
	// measures the counter's absolute value, not a delta) from zero.
	streamBytes  int64
	streamPrimed bool
	lastDiscount float64 // pages/sec subtracted at the last sample

	activations int64     // inactive->active transitions, for markers
	activeSince time.Time // start of the current activation episode

	// boundedIgnores counts ActiveBounded calls that returned false while
	// the sensor was active — episodes that outlived the caller's cap and
	// were declared non-causal. Rollout marker for the episode-cap v3.
	boundedIgnores int64
}

func newRefaultSensor(read func() (int64, bool), threshold float64, interval time.Duration) *refaultSensor {
	s := &refaultSensor{read: read, threshold: threshold, interval: interval}
	if read == nil || threshold <= 0 {
		s.disabled = true
		return s
	}
	// Prime the baseline so the first real sample measures a delta, not
	// the counter's absolute value.
	if c, ok := read(); ok {
		s.lastCount = c
		s.lastSample = time.Now()
	} else {
		s.disabled = true
	}
	return s
}

// Active reports whether the refault rate has exceeded the threshold on
// the last two samples. Cheap between sampling intervals: one mutex and
// a time check.
func (s *refaultSensor) Active() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.disabled {
		return false
	}
	now := time.Now()
	elapsed := now.Sub(s.lastSample)
	if elapsed < s.interval {
		return s.active
	}
	c, ok := s.read()
	if !ok {
		s.disabled = true
		s.active = false
		return false
	}
	s.lastRate = float64(c-s.lastCount) / elapsed.Seconds()
	s.lastCount = c
	s.lastSample = now
	s.lastDiscount = 0
	if src := loadStreamingSource(); src != nil {
		sb := src()
		if s.streamPrimed {
			s.lastDiscount = float64(sb-s.streamBytes) / elapsed.Seconds() / refaultPageBytes
		}
		s.streamBytes = sb
		s.streamPrimed = true
	}
	if s.lastRate-s.lastDiscount > s.threshold {
		s.hotStreak++
	} else {
		s.hotStreak = 0
	}
	wasActive := s.active
	s.active = s.hotStreak >= 2
	if s.active && !wasActive {
		s.activations++
		s.activeSince = now
	}
	return s.active
}

// ActiveBounded is Active with a per-episode honor budget: it reports
// true only while the current activation episode is younger than cap.
//
// Rationale (refault-sensor v3, 2026-07-22 SF100 investigation): the
// collapse this sensor gates sheds the caller's own held window bytes.
// When those bytes ARE the displacement cause (Q06-shape concurrent
// full-table scans), shedding them drops the refault rate within a
// sample or two and the episode ends — the cap never binds. When the
// refaults are ambient (suite traffic re-faulting a legitimately colder
// cache off local NVMe), collapse sheds nothing, the rate never goes
// quiet, and v2 semantics locked the throttle on for minutes — measured
// +22-40% SF100 steady suite loss for zero displacement relief. An
// episode that outlives cap despite the collapse is therefore declared
// non-causal and ignored until the sensor genuinely deactivates (one
// quiet sample), which arms a fresh episode.
//
// cap <= 0 means unbounded — identical to Active.
func (s *refaultSensor) ActiveBounded(cap time.Duration) bool {
	if !s.Active() {
		return false
	}
	if cap <= 0 {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.activeSince) > cap {
		s.boundedIgnores++
		return false
	}
	return true
}

// Stats returns the last computed rate (pages/sec) and the number of
// inactive->active transitions — the §9 rollout markers.
func (s *refaultSensor) Stats() (lastRate float64, activations int64) {
	if s == nil {
		return 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRate, s.activations
}

// Discount returns the designed-streaming rate (pages/sec) subtracted at
// the last sample — the streaming-discount rollout marker.
func (s *refaultSensor) Discount() float64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastDiscount
}

// BoundedIgnores returns how many ActiveBounded consultations declined an
// over-cap episode — the episode-cap v3 rollout marker.
func (s *refaultSensor) BoundedIgnores() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.boundedIgnores
}

// readRefaultCounter returns a reader for the best available refault
// counter and the source it bound, for the init log: the process's
// cgroup v2 memory.stat if one exists, else the host-wide /proc/vmstat.
// Returns nil when neither source has a workingset_refault field (e.g.
// pre-4.20 kernels, PSI-less builds).
func readRefaultCounter() (func() (int64, bool), string) {
	if path := cgroupV2StatPath(); path != "" {
		if _, ok := parseRefaultFile(path); ok {
			return func() (int64, bool) { return parseRefaultFile(path) }, path
		}
	}
	const vmstat = "/proc/vmstat"
	if _, ok := parseRefaultFile(vmstat); ok {
		return func() (int64, bool) { return parseRefaultFile(vmstat) }, vmstat
	}
	return nil, "none"
}

// cgroupV2StatPath resolves this process's cgroup v2 memory.stat path,
// or "" when the process is not in a non-root v2 cgroup. Mirrors the
// hierarchy walk in detectV2ProcessLimit (detect.go).
func cgroupV2StatPath() string {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return ""
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if p, ok := strings.CutPrefix(line, "0::"); ok {
			if p == "" || p == "/" {
				return ""
			}
			return "/sys/fs/cgroup" + p + "/memory.stat"
		}
	}
	return ""
}

// parseRefaultFile reads workingset_refault_file (falling back to the
// pre-5.9 unified workingset_refault) from a vmstat-format file.
func parseRefaultFile(path string) (int64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	var unified int64
	var haveUnified bool
	for line := range strings.SplitSeq(string(data), "\n") {
		if v, ok := strings.CutPrefix(line, "workingset_refault_file "); ok {
			if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
				return n, true
			}
		}
		if v, ok := strings.CutPrefix(line, "workingset_refault "); ok {
			if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
				unified, haveUnified = n, true
			}
		}
	}
	return unified, haveUnified
}

var (
	pageCacheSensorOnce sync.Once
	pageCacheSensor     *refaultSensor
)

func pageCachePressureSensor() *refaultSensor {
	pageCacheSensorOnce.Do(func() {
		threshold := refaultPressureRatePerSec
		if v := os.Getenv("WADJET_REFAULT_PRESSURE_RATE"); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
				threshold = f
			}
		}
		read, source := readRefaultCounter()
		pageCacheSensor = newRefaultSensor(read, threshold, refaultSampleInterval)
		slog.Info("page-cache pressure sensor",
			"source", source,
			"threshold_pages_per_sec", threshold,
			"enabled", !pageCacheSensor.disabled)
	})
	return pageCacheSensor
}

// PageCachePressureActive reports whether the kernel is currently
// refaulting recently-evicted file pages faster than the threshold —
// page-cache thrash the Go-heap hooks cannot see. Callers gate
// DISCRETIONARY memory use (decode-ahead width, speculative prefetch)
// on it; it must not gate correctness-bearing work.
func PageCachePressureActive() bool {
	return pageCachePressureSensor().Active()
}

// PageCachePressureActiveBounded is PageCachePressureActive with the
// per-episode honor budget (see refaultSensor.ActiveBounded). cap <= 0
// is unbounded.
func PageCachePressureActiveBounded(cap time.Duration) bool {
	return pageCachePressureSensor().ActiveBounded(cap)
}

// PageCachePressureBoundedIgnores returns the count of episode-cap
// declines — the v3 rollout marker beside PageCachePressureStats.
func PageCachePressureBoundedIgnores() int64 {
	return pageCachePressureSensor().BoundedIgnores()
}

// PageCachePressureStats returns the sensor's last sampled refault rate
// (pages/sec) and its lifetime activation count, for rollout markers.
func PageCachePressureStats() (lastRate float64, activations int64) {
	return pageCachePressureSensor().Stats()
}

// PageCachePressureDiscount returns the designed-streaming pages/sec
// subtracted from the refault rate at the last sample — the
// streaming-discount rollout marker.
func PageCachePressureDiscount() float64 {
	return pageCachePressureSensor().Discount()
}

// ReadRefaultCounterForDiagnostics reads the raw refault counter through
// the same source-selection logic the sensor uses. cmd/refault-probe
// prints it beside the sensor state to separate "counter not visible
// here" from "sensor not sampling" in the field.
func ReadRefaultCounterForDiagnostics() (int64, bool) {
	read, _ := readRefaultCounter()
	if read == nil {
		return 0, false
	}
	return read()
}
