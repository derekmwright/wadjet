package exec

import (
	"os"
	"strconv"
	"sync/atomic"
)

// Deterministic join-partition eviction forcing — TEST ONLY.
//
// A grace hash join evicts a build partition when
// `SpillManager.ShouldSpillFor(SpillCheap)` is true, which reads the WHOLE
// query's memory rather than the join's own. On a small fixture that reading
// is dominated by what the scan happens to be holding when the build checks,
// so whether a join spills at all is a coin toss: the same shape at the same
// budget evicted a partition in one run of `go test` and not in the next, on
// one machine, with no code change between them.
//
// That is the condition ADR-0027 decision 6 already gave the aggregate
// (`ForceAggDrainEvery`), the sort (`ForceSortSpillEvery`) and the window
// (`ForceWindowSpillEvery`) a knob for, and the join was the one pipeline
// breaker without one — so a gate for a defect that only exists AFTER an
// eviction (the nested pipeline that never drained the evicted partitions,
// #1010) had nothing to make its own trigger fire.
//
// With N set, every Nth batch a partition-on-arrival build absorbs evicts one
// in-memory partition through the real path — the same `spillOneInMemoryPartition`
// pressure calls, writing real spill files — and the pressure check is bypassed
// for those, because there is no pressure for it to be measuring. What a gate
// observes is the production eviction, the production probe routing and the
// production flush.
//
// It is read from WADJET_TEST_FORCE_JOIN_EVICT_EVERY once per process so an
// end-to-end gate at the SQL layer can arm it, and settable from Go. It is
// never set on any production path: the only cost when unset is one relaxed
// atomic load per arriving build batch, next to a partition scatter.
var forceJoinEvictEvery atomic.Int64

func init() {
	if v := os.Getenv("WADJET_TEST_FORCE_JOIN_EVICT_EVERY"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			forceJoinEvictEvery.Store(n)
		}
	}
}

// ForceJoinPartitionEvictEvery arms (n > 0) or disarms (n <= 0) forced
// eviction for every partition-on-arrival join build in this process, and
// returns the previous setting so a test can restore it. TEST ONLY.
func ForceJoinPartitionEvictEvery(n int64) int64 {
	return forceJoinEvictEvery.Swap(n)
}

// ForcedJoinEvictions counts evictions taken because of the forcing knob
// rather than because of memory pressure. A gate asserts it moved: a forcing
// knob that silently failed to engage turns the gate it arms into a no-op,
// which is what ForcedAggDrains exists to catch on the aggregate's side.
var ForcedJoinEvictions atomic.Int64

// forcedEvictDue reports whether this arriving build batch is the Nth since
// the knob was armed, and therefore owes an eviction. Caller holds h.mu.
func (h *HashJoin) forcedEvictDue() bool {
	n := forceJoinEvictEvery.Load()
	if n <= 0 {
		return false
	}
	h.forcedEvictSeen++
	return h.forcedEvictSeen%n == 0
}
