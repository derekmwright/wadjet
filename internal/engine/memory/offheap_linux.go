//go:build linux

package memory

import (
	"sync"
	"syscall"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/optswitch"
)

// Off-heap growable arrays for aggregate group state (ADR-0006 amendment,
// 2026-08-17). The typed-SoA aggregation paths accumulate multi-GB
// pointer-free arrays (flat accumulators, key SoAs) whose growth used to
// ride Go's append: every doubling holds old+new live simultaneously and
// leaves the old array as garbage for the collector. At the 100M-group
// scale (ClickBench Q33) that stacked ~10GB of transients on ~12GB of
// live state between GC cycles — measured 22.3GB heap on 12GB live — and
// whether a cold try fit under GOMEMLIMIT or spiraled through the
// pressure-valve spill path was decided by GC timing (108s vs 21s cold
// on identical binaries, attributed by a same-window binary control).
//
// The fix: back these arrays with anonymous MAP_NORESERVE reservations.
// A slice is created over the reservation with len=0 and cap=the full
// reserve, so every existing append site grows it IN PLACE forever — no
// reallocation, no copy, no garbage, and the Go heap never sees the
// bytes (the engine's own tracker still accounts them exactly; that
// accounting, not heap size, drives spill decisions). Pages commit
// lazily on first touch and arrive zeroed, which append's zero-writes
// then satisfy for free.
//
// Scope guard: this is NOT the shelved BytesColumn decode arena
// (2026-06-09) — no object lifetimes, no per-value layout, no sharing.
// It manages whole pointer-free arrays with one owner each, registered
// on an OffheapRegistry whose lifetime is the owning operator's.
var offheapAggToggle = optswitch.Register("offheap-agg", "WADJET_OFFHEAP_AGG",
	"mmap-backed group-state arrays: in-place growth, no realloc transients, state invisible to GC")

// OffheapAvailable reports whether the platform path is compiled in and
// the kill switch is on.
func OffheapAvailable() bool { return offheapAggToggle.On() }

// offheapReserveBytes is the per-array address-space reservation. Virtual
// only (MAP_NORESERVE): physical pages commit on touch. 4GB/array bounds
// a single partition's array at 512M int64 entries — far beyond any
// realistic per-partition group count — while keeping worst-case virtual
// footprint (arrays × partitions × clones) in the tens-of-GB range,
// far under vm.max_map_count pressure at ~1 mapping per array.
const offheapReserveBytes = 4 << 30

// OffheapRegistry tracks the reservations owned by one operator so Reset
// and Close can return them. Not safe for concurrent mutation — owners
// are single-goroutine (each morsel clone owns its own registry), and
// adoption hands whole registries over (AdoptFrom).
type OffheapRegistry struct {
	mu    sync.Mutex
	maps  [][]byte // full mmap'd reservations
	freed bool
}

// NewOffheapRegistry returns an empty registry.
func NewOffheapRegistry() *OffheapRegistry { return &OffheapRegistry{} }

// reserve creates one reservation and records it. Returns nil when the
// mmap fails (callers fall back to heap allocation).
func (r *OffheapRegistry) reserve() []byte {
	if r == nil {
		return nil
	}
	m, err := syscall.Mmap(-1, 0, offheapReserveBytes,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS|syscall.MAP_NORESERVE)
	if err != nil {
		return nil
	}
	// Huge pages collapse ~1M first-touch faults per committed 4GB into
	// ~2k. Advisory only; failure is fine.
	_ = syscall.Madvise(m, syscall.MADV_HUGEPAGE)
	r.mu.Lock()
	r.maps = append(r.maps, m)
	r.mu.Unlock()
	return m
}

// AdoptFrom moves every reservation owned by o into r (merge/adoption
// paths transfer array ownership between aggregates; the arrays' new
// owner must also own their unmapping).
func (r *OffheapRegistry) AdoptFrom(o *OffheapRegistry) {
	if r == nil || o == nil || o == r {
		return
	}
	o.mu.Lock()
	moved := o.maps
	o.maps = nil
	o.mu.Unlock()
	if len(moved) == 0 {
		return
	}
	r.mu.Lock()
	r.maps = append(r.maps, moved...)
	r.mu.Unlock()
}

// Close unmaps every owned reservation. Idempotent. The owner must have
// dropped every slice referencing them first (aggregate Close/reset
// order guarantees this; a stale read after Close faults loudly rather
// than corrupting).
func (r *OffheapRegistry) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	maps := r.maps
	r.maps = nil
	r.freed = true
	r.mu.Unlock()
	for _, m := range maps {
		_ = syscall.Munmap(m)
	}
}

// Mappings reports how many reservations are live (tests/diagnostics).
func (r *OffheapRegistry) Mappings() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.maps)
}

// offheapSlice returns a len-0 slice over a fresh reservation with the
// reservation's full element capacity, or ok=false for heap fallback.
func offheapSlice[T any](r *OffheapRegistry) ([]T, bool) {
	if r == nil || !offheapAggToggle.On() {
		return nil, false
	}
	m := r.reserve()
	if m == nil {
		return nil, false
	}
	var zero T
	elems := offheapReserveBytes / int(unsafe.Sizeof(zero))
	return unsafe.Slice((*T)(unsafe.Pointer(unsafe.SliceData(m))), elems)[:0:elems], true
}

// Offheap returns an off-heap-backed slice of T (len 0, huge cap) whose
// appends grow in place within the reservation, or a heap slice of the
// given capacity when off-heap is unavailable (kill switch off, mmap
// failure, non-linux build). T MUST be pointer-free — the GC never scans
// these bytes.
func Offheap[T any](r *OffheapRegistry, heapCap int) []T {
	if s, ok := offheapSlice[T](r); ok {
		return s
	}
	return make([]T, 0, heapCap)
}

// OffheapToggleForTest flips the kill switch and returns the previous
// value. Test-only seam for parity runs against the heap path.
func OffheapToggleForTest(v bool) bool { return offheapAggToggle.Set(v) }
