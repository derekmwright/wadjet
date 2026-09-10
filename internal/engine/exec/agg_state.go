// This file holds group-state allocation and memory accounting.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// groupState holds per-group hash table state. The slim 24-byte base lives
// inline in the groupStatePool chunks so the SoA simple-aggs hot path
// (Q17/Q01/most TPC-H) — which never needs keyValues/accs/distinctSets/
// extraState — pays only 8 bytes for the extras pointer (nil) instead of
// 96 bytes of slice headers. Complex-agg paths (COUNT(DISTINCT), STRING_AGG,
// variance, percentile, GROUPING SETS, str-group, generic SoA) call
// ensureExtras() to lazily allocate the heavy fields.
//
// Sizing: 8 (intKey) + 4 (setID) + 4 (pad) + 8 (extras) = 24 bytes.
// The 88-byte savings vs the inline layout is 1.76 GB at SF100 scan-5
// (20M groups) and brings worker peak heap under GOMEMLIMIT at
// max_concurrent=4.
type groupState struct {
	// Pointer first so the GC pointer-scan region is 8 B, not 24 B. This struct
	// is one-per-group, allocated in contiguous []groupState chunks (millions of
	// groups at SF100), so shrinking the scanned prefix cuts GC mark work per
	// cycle. Size is unchanged at 24 B (extras 8 + intKey 8 + setID 4 + 4 tail pad).
	extras *groupStateExtras
	intKey int64 // single int64 key for int-keyed groups (avoids []any boxing)
	setID  int32 // grouping set index (-1 = not a grouping set)
}

// groupStateExtras holds the heavy per-group fields that are only needed by
// complex-agg paths or by the generic str-keyed path. Allocated lazily by
// (*groupState).ensureExtras() and pointed at from the slim base. The SoA
// simple-aggs path (consumeBatchIntGroup, consumeBatchPackedGroup) never
// touches this allocation, so the typical Q17/Q01 pattern leaves it nil.
type groupStateExtras struct {
	keyValues    []any
	accs         []kernel.Accumulator
	distinctSets []*distinctSet // per-agg distinct value sets (nil if not COUNT(DISTINCT)); typed int set for int-class columns
	extraState   []any          // per-agg custom state (string_agg builder, variance state, etc.)
}

// ensureExtras lazily allocates the extras struct on first complex-path
// access. Callers that do multiple writes should bind the result to a local
// (`ext := gs.ensureExtras(); ext.keyValues = ...`) instead of calling
// ensureExtras repeatedly.
func (gs *groupState) ensureExtras() *groupStateExtras {
	if gs.extras == nil {
		gs.extras = &groupStateExtras{}
	}
	return gs.extras
}

// groupStatePool allocates groupState objects in contiguous chunks to reduce
// heap allocations and GC pressure. With per-object allocation, 1.5M groups
// at SF1 create 1.5M heap objects; with chunk allocation, they create ~366.
// Each chunk is a single contiguous array; pointers into it remain valid
// because new chunks don't move old ones.
type groupStatePool struct {
	chunks [][]groupState
	pos    int // position within current chunk
}

const groupStateChunkSize = 4096

func (p *groupStatePool) alloc() *groupState {
	if len(p.chunks) == 0 || p.pos >= len(p.chunks[len(p.chunks)-1]) {
		p.chunks = append(p.chunks, make([]groupState, groupStateChunkSize))
		p.pos = 0
	}
	gs := &p.chunks[len(p.chunks)-1][p.pos]
	p.pos++
	return gs
}

// preAlloc pre-allocates a single large initial chunk to avoid repeated
// chunk allocations during the hot consume loop. Only useful before any
// alloc() calls (when the pool is empty).
func (p *groupStatePool) preAlloc(n int) {
	if len(p.chunks) == 0 && n > groupStateChunkSize {
		p.chunks = append(p.chunks, make([]groupState, n))
		p.pos = 0
	}
}

// groupMemoryUsage returns the estimated heap bytes consumed by the aggregate's
// group state: hash tables, flat accumulator arrays, group state pool, and key
// arrays. This does NOT include input batch data (tracked separately by
// SpillManager.TrackBatch).
func (h *HashAggregate) groupMemoryUsage() int64 {
	var size int64
	if h.intGroupIndex != nil {
		size += h.intGroupIndex.MemoryUsage()
	}
	if h.intTwoLevel != nil {
		size += h.intTwoLevel.MemoryUsage()
	}
	if h.packedIdx != nil {
		size += h.packedIdx.MemoryUsage()
	}
	if h.packedTwoLevel != nil {
		size += h.packedTwoLevel.MemoryUsage()
	}
	if h.strGroupIndex != nil {
		size += h.strGroupIndex.MemoryUsage()
	}
	if h.genKeyIdx != nil {
		size += h.genKeyIdx.MemoryUsage()
	}
	size += int64(cap(h.genKeyNext)) * 4
	// Group state pool: each chunk is a contiguous array of slim groupState
	// structs (24 bytes each: intKey + setID + extras pointer). The heavy
	// per-group state behind the extras pointer is accounted below via the
	// incremental counters + the per-generic-group constant — the former
	// "accuracy budget" (slice contents untracked) let the tracker
	// under-report by 41-100% on high-cardinality non-int-SoA GROUP BYs,
	// which is how SF100 Q17 died at GOMEMLIMIT with the spill threshold
	// never crossed (2026-07-03 postmortem).
	for _, chunk := range h.gsPool.chunks {
		size += int64(cap(chunk)) * 24
	}
	// Generic/str-path per-group state. Every group on those paths appends
	// to h.keys, so len(h.keys) counts them: each carries a groupStateExtras
	// (96 B of slice headers) plus a keyValues []any backing array (16 B
	// interface slot + ~8 B boxed scalar per group column; boxed strings
	// share the bytes counted in serializedKeyBytes).
	if n := int64(len(h.keys)); n > 0 {
		size += n * (96 + int64(len(h.GroupByCols))*24)
	}
	// Slice backing arrays for the key mirrors, plus the string bytes and
	// per-category contents tracked incrementally at their append sites.
	size += int64(cap(h.keys)) * 24 // [][]any: 24 B slice header per element
	size += int64(cap(h.serializedKeys)) * 16
	size += int64(cap(h.compactKeys)) * 16
	size += h.serializedKeyBytes
	size += h.compactKeyBytes
	size += h.distinctBytes
	size += h.extraStateBytes
	size += h.extrasAccsCount * kernelAccumulatorBytes
	// SoA flat accumulator arrays: 8 bytes per element for int64/float64
	// fields. Off-heap arrays carry a huge virtual cap, so account them by
	// len (the committed/used prefix — appends only ever touch that);
	// heap-backed arrays keep cap so allocated-but-unused doubling slack
	// stays charged.
	dim := func(c, l int) int64 {
		if h.offheap != nil {
			return int64(l)
		}
		return int64(c)
	}
	for _, fa := range h.intFlatAccs {
		size += dim(cap(fa.count), len(fa.count)) * 8
		size += dim(cap(fa.sumI64), len(fa.sumI64)) * 8
		size += dim(cap(fa.sumF64), len(fa.sumF64)) * 8
		size += dim(cap(fa.sumDec), len(fa.sumDec)) * 16 // Int128
		size += dim(cap(fa.minI64), len(fa.minI64)) * 8
		size += dim(cap(fa.maxI64), len(fa.maxI64)) * 8
		size += dim(cap(fa.minF64), len(fa.minF64)) * 8
		size += dim(cap(fa.maxF64), len(fa.maxF64)) * 8
		size += dim(cap(fa.minDec), len(fa.minDec)) * 16
		size += dim(cap(fa.maxDec), len(fa.maxDec)) * 16
		size += dim(cap(fa.hasMin), len(fa.hasMin))
		size += dim(cap(fa.hasMax), len(fa.hasMax))
	}
	// Int-key SoAs
	size += dim(cap(h.packedKeys), len(h.packedKeys)) * 16
	size += dim(cap(h.intKeys), len(h.intKeys)) * 8
	// Group state pointer slices
	size += int64(cap(h.intGroupStates)) * 8
	size += int64(cap(h.strGroupStates)) * 8
	return size
}

// reconcileGroupMemory tracks group state growth in the spill manager so that
// ShouldSpill() triggers at the correct threshold. Without this, spill only
// sees input batch cost while group states grow unbounded.
func (h *HashAggregate) reconcileGroupMemory() {
	if h.Spill == nil {
		return
	}
	actual := h.groupMemoryUsage()
	if actual > h.trackedGroupMem {
		delta := actual - h.trackedGroupMem
		h.Spill.TrackBatch(delta)
		h.trackedGroupMem = actual
		// Publish the true owned footprint so the SpillManager drift-backstop
		// (OwnedTotal vs tracker.Used) has a real number; without this the
		// published total stays 0 and the backstop misreads drift as 100%.
		if h.accInstanceID != 0 {
			h.Spill.Tracker().PublishOwned(h.accInstanceID, h.trackedGroupMem)
		}
	}
}
