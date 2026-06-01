package worker

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// Phase 5 — mmap relief. Workers hold many concurrent read-only MAP_SHARED
// mappings of local cache files (one per cachedFileStreamSource, ~750 MB each at
// SF100); the aggregate page-cache working set was the single largest untracked
// resident-memory consumer (project_streaming_shuffle_sf100_win_2026-05-22 saw
// ~8.78 GB live in os.readFileContents). This file tracks those mappings in a
// process-global registry and, under memory pressure, MADV_DONTNEED's the
// coldest ones — the kernel drops the resident pages and they re-fault from the
// still-present cache file on next access.
//
// The ENTIRE mechanism is gated on mmapReliefEnabled (the --mmap-relief flag,
// default false). When disabled: registerMmap returns nil, so there is no
// tracking, no per-Next lastTouch write, and no syscall — a single
// branch-predicted bool load is the only cost. Activation (and the relief
// threshold) is deploy-gated: it changes resident-memory behavior in a way only
// an SF100 memory-pressure run can validate.

// mmapReliefEnabled gates region tracking, per-Next lastTouch, and the
// MADV_DONTNEED relief. Set once at worker startup via SetMmapRelief.
var mmapReliefEnabled atomic.Bool

// mmapReliefThresholdBytes is the non-heap-resident (RSS − HeapInuse) ceiling at
// or above which the stats loop starts relieving the coldest mappings, down to
// this level. Tunable via --mmap-relief-threshold-mb; the deploy A/B sets it.
var mmapReliefThresholdBytes atomic.Int64

// SetMmapRelief configures the relief mechanism from worker config. thresholdMB
// <= 0 leaves the default. Called once before any task runs.
func SetMmapRelief(enabled bool, thresholdBytes int64) {
	mmapReliefEnabled.Store(enabled)
	if thresholdBytes > 0 {
		mmapReliefThresholdBytes.Store(thresholdBytes)
	}
}

// MmapReliefThreshold returns the current relief threshold in bytes (0 if unset).
func MmapReliefThreshold() int64 { return mmapReliefThresholdBytes.Load() }

// mmapRegion is one tracked file mapping. lastTouch is updated atomically and
// lock-free on every Next() that reads the region; the relief path sorts by it
// lazily (only at eviction time), so there is no per-Next lock or list reorder.
type mmapRegion struct {
	data      []byte // the mmap'd slice (PROT_READ MAP_SHARED file mapping)
	lastTouch atomic.Int64
}

// touch records the current time as this region's last access. Cheap: one
// time.Now().UnixNano() + atomic store, no lock.
func (r *mmapRegion) touch() {
	if r != nil {
		r.lastTouch.Store(time.Now().UnixNano())
	}
}

// mmapRegistry holds every live file mapping across cachedFileStreamSources on
// this worker so the relief path can pick the coldest under pressure.
var mmapRegistry = struct {
	mu      sync.Mutex
	regions map[*mmapRegion]struct{}
}{regions: make(map[*mmapRegion]struct{})}

// registerMmap adds a region for data and returns its handle, or nil when relief
// is disabled (so callers skip all tracking with one branch). The caller stores
// the handle and passes it to unregisterMmap BEFORE munmapping data.
func registerMmap(data []byte) *mmapRegion {
	if !mmapReliefEnabled.Load() {
		return nil
	}
	r := &mmapRegion{data: data}
	r.touch()
	mmapRegistry.mu.Lock()
	mmapRegistry.regions[r] = struct{}{}
	mmapRegistry.mu.Unlock()
	return r
}

// unregisterMmap removes a region from the registry. It MUST be called before
// the caller munmaps the region's data: holding the registry lock here, while
// relieveMmap also holds it during its MADV syscalls, guarantees relief never
// advises an address that is about to be (or has been) unmapped. nil-safe.
func unregisterMmap(r *mmapRegion) {
	if r == nil {
		return
	}
	mmapRegistry.mu.Lock()
	delete(mmapRegistry.regions, r)
	mmapRegistry.mu.Unlock()
}

// relieveMmap MADV_DONTNEEDs the coldest live regions until at least target
// bytes have been advised away or no regions remain, returning bytes advised.
// Safe on these read-only MAP_SHARED mappings: dropped pages re-fault from the
// cache file on next access. No-op when disabled or target <= 0.
//
// The registry lock is held for the whole MADV loop. unregisterMmap (called
// before any Munmap) takes the same lock, so a region being relieved cannot be
// unmapped underneath the syscall — no use-after-munmap, no ABA. Relief is rare
// (pressure-gated, ≤ once per stats tick) so the brief lock hold is acceptable.
func relieveMmap(target int64) int64 {
	if !mmapReliefEnabled.Load() || target <= 0 {
		return 0
	}
	mmapRegistry.mu.Lock()
	defer mmapRegistry.mu.Unlock()

	var freed int64
	for _, r := range snapshotColdestFirstLocked() {
		if freed >= target {
			break
		}
		if len(r.data) == 0 {
			continue
		}
		if err := unix.Madvise(r.data, unix.MADV_DONTNEED); err == nil {
			freed += int64(len(r.data))
		}
	}
	return freed
}

// snapshotColdestFirstLocked returns the live regions sorted by lastTouch
// ascending (coldest first). Caller holds mmapRegistry.mu.
func snapshotColdestFirstLocked() []*mmapRegion {
	regions := make([]*mmapRegion, 0, len(mmapRegistry.regions))
	for r := range mmapRegistry.regions {
		regions = append(regions, r)
	}
	sort.Slice(regions, func(i, j int) bool {
		return regions[i].lastTouch.Load() < regions[j].lastTouch.Load()
	})
	return regions
}

// sortRegionsByTouch returns the live regions coldest-first. Test/diagnostic
// seam over snapshotColdestFirstLocked.
func sortRegionsByTouch() []*mmapRegion {
	mmapRegistry.mu.Lock()
	defer mmapRegistry.mu.Unlock()
	return snapshotColdestFirstLocked()
}

// liveMmapCount reports the number of tracked regions (for tests/diagnostics).
func liveMmapCount() int {
	mmapRegistry.mu.Lock()
	defer mmapRegistry.mu.Unlock()
	return len(mmapRegistry.regions)
}
