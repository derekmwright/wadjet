package exec

import (
	"os"
	"strconv"
	"sync/atomic"
)

// TEST ONLY: every Nth partition-on-arrival build batch evicts one resident
// partition through spillOneInMemoryPartition, bypassing pressure and writing
// real files. Gates observe production eviction, probe routing and flush
// (#1010; ADR-0027 decision 6).
// Whole-query memory pressure cannot guarantee engagement on small fixtures.
// Read WADJET_TEST_FORCE_JOIN_EVICT_EVERY once per process or set from Go,
// including SQL-level gates. Never set in production; unset cost is one atomic
// load per arriving build batch.
// See docs/internals/join-partition-eviction-forcing.md for the design.
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
