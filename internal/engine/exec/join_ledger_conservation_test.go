package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// A grace build's ledger is CONSERVED: every byte it releases was reserved, and
// every byte it holds is charged.
//
// It was neither. One arrival batch is charged once, as hashBuildBytes(b), but
// its rows were then released — or not — by two different formulas, neither of
// them a share of that charge:
//
//   - rows written straight to an ALREADY-SPILLED partition released
//     hashBuildBytes of a freshly minted per-partition batch, which pays the
//     per-column fixed overhead once per partition against an arrival batch
//     that paid it once. 24,932 bytes charged, 30,372 released across 63
//     partitions: 1.22x, over-releasing 5,440 bytes per batch.
//   - rows appended to an in-memory partition charge partMemory the TIGHT
//     per-row data bytes, which is less than their share, so a build that never
//     spilled leaked about 1,000 bytes per batch the other way.
//
// So the direction a build drifted followed how many partitions had spilled by
// the time each batch arrived — pressure and timing, not the query. That is
// #789's moving floor from the release side, and #823's ledger half.
//
// The measurement that made it undeniable: at 100,000 build rows in 256-row
// arrivals against a 1 MiB budget, `Tracker.Used()` reached MINUS 867,561 while
// the join's own index was 802,816 bytes of live heap.
func TestAGraceBuildsLedgerIsConserved(t *testing.T) {
	const budget = 1 << 20
	for _, rows := range []int{2000, 20000, 50000, 100000} {
		t.Run("", func(t *testing.T) {
			schema, data := arrivalBuildRows(rows, "padpadpadpadpadpadpadpadpadpadpad")
			tracker := memory.NewTracker("ledger", budget)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			defer sm.Cleanup()

			hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"k"})
			hj.Spill, hj.MemTracker = sm, tracker
			evictedBefore := JoinPartitionsEvicted.Load()
			if err := hj.Build(context.Background(), arrivalSource(schema, data, 256)); err != nil {
				t.Fatalf("build (%d rows): %v", rows, err)
			}
			evicted := JoinPartitionsEvicted.Load() - evictedBefore

			var partSum int64
			for _, m := range hj.spillState.partMemory {
				partSum += m
			}
			t.Logf("rows=%6d evicted=%2d: used=%d trackedMem=%d partMemory=%d index=%d",
				rows, evicted, tracker.Used(), hj.trackedMem, partSum, hj.hashTableOverhead())

			// The ledger says what the join says it holds, and that is the sum
			// of the two things it holds.
			if got, want := tracker.Used(), hj.trackedMem; got != want {
				t.Errorf("rows=%d: tracker used=%d but the join says it holds %d",
					rows, got, want)
			}
			if got, want := hj.trackedMem, partSum+hj.trackedHashOverhead; got != want {
				t.Errorf("rows=%d: the join holds %d, but its partitions are %d and its "+
					"index charge is %d (%d) — something is charged that nothing owns",
					rows, got, partSum, hj.trackedHashOverhead, want)
			}
			if tracker.Used() < 0 {
				t.Errorf("rows=%d: used=%d. A negative ledger means bytes were released "+
					"that were never reserved, and from here every admission is measured "+
					"against a floor lower than the memory that exists", rows, tracker.Used())
			}
			if rows >= 20000 && evicted == 0 {
				t.Errorf("rows=%d evicted nothing; this cell never reached the "+
					"already-spilled write path and proves nothing about it", rows)
			}

			// And it returns to zero, under every purpose.
			if err := hj.Close(); err != nil {
				t.Fatal(err)
			}
			if got := tracker.Used(); got != 0 {
				t.Errorf("rows=%d: after Close the tracker holds %d bytes for a join that "+
					"is gone", rows, got)
			}
			if total, top, n := tracker.ForcedBytes(); total != 0 {
				t.Errorf("rows=%d: after Close the forced census still reports %d bytes "+
					"(largest %d by %q)", rows, total, n, top)
			}
		})
	}
}
