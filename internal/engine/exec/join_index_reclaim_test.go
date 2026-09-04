package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// #823's reclaim half, closed.
//
// This file used to hold TestEvictingEveryPartitionLeavesTheIndexCharged: the
// MEASUREMENT of the defect, pinned in both directions so a fix could not land
// without deleting it. Its first branch fired the moment `used` fell below the
// index's own bytes, and it named what to do — delete the pin, update ADR-0006's
// producer table, close the issue. That is what happened; the pin's replacement
// is below, and it asserts the DERIVED floor rather than a measured one.
//
// What the pin measured, at 2,000 build rows in 256-row arrivals against a
// 1 MiB budget with all 64 partitions evicted and no build column data left:
//
//	used = 106,320   index = 98,304 (92%)
//	                   hash TABLE   65,536 (62%)
//	                   arena+chain  28,672 (27%)
//	                   bloom         4,096  (4%)

// evictAll evicts every in-memory partition, the way a pressured query does
// through spillUntilCanReserve, and returns how many it evicted.
func evictAll(t *testing.T, hj *HashJoin) int {
	t.Helper()
	evicted := 0
	for {
		freed, err := hj.spillOneInMemoryPartition()
		if err != nil {
			t.Fatalf("evict: %v", err)
		}
		if freed == 0 {
			break
		}
		evicted++
	}
	if evicted == 0 {
		t.Fatal("no partition was evicted — this test measured nothing; the build never " +
			"partitioned, so find out what changed in the build dispatch")
	}
	for _, b := range hj.buildBatches {
		if b != nil {
			t.Fatalf("a build batch is still in memory after evicting %d partitions — "+
				"the measurement below is not of a fully evicted join", evicted)
		}
	}
	return evicted
}

// A join that has evicted every partition holds no build column data, and —
// since the index is per partition — no index either, beyond the two structures
// an eviction deliberately does not free: the partition HEADERS and the BLOOM
// FILTER, which covers spilled keys on purpose.
//
// The expected floor is DERIVED from the structure sizes, not measured. Two
// build sizes an order of magnitude apart are run, and what must be IDENTICAL
// between them is the part #823 said should not scale with the build: the
// per-partition headers, and the release of every table, arena and chain.
//
// The bloom does scale, deliberately and openly — it is ~10 bits per distinct
// key by construction, it is the ONE structure an eviction must not free
// (it covers spilled keys), and at 4% of what the index used to hold it is the
// price of that. So it is named in the floor rather than hidden in it.
func TestEvictingEveryPartitionFreesTheIndex(t *testing.T) {
	const budget = 1 << 20
	headers := map[int]int64{}
	perRow := map[int]float64{}
	for _, rows := range []int{2000, 20000} {
		schema, data := arrivalBuildRows(rows, "padpadpadpadpadpadpadpadpadpadpad")
		tracker := memory.NewTracker("reclaim", budget)
		sm, err := memory.NewSpillManager(t.TempDir(), tracker)
		if err != nil {
			t.Fatal(err)
		}
		defer sm.Cleanup()

		hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"k"})
		hj.Spill, hj.MemTracker = sm, tracker
		if err := hj.Build(context.Background(), arrivalSource(schema, data, 256)); err != nil {
			t.Fatalf("build: %v", err)
		}
		beforeIndex := hj.indexBytes()
		evicted := evictAll(t, hj)

		index, want := hj.indexBytes(), hj.evictedIndexFloor()
		t.Logf("rows=%5d: %d partitions evicted; index %d -> %d (derived floor %d: %d of "+
			"partition headers + %d of bloom); tracker used=%d",
			rows, evicted, beforeIndex, index, want,
			int64(len(hj.parts))*sizeofJoinIndexPart, int64(len(hj.bloom))*8, tracker.Used())

		if index != want {
			t.Errorf("rows=%d: after evicting every partition the index holds %d bytes, "+
				"but the only structures an eviction does not free are the partition "+
				"headers and the bloom, which are %d. Something else survived the "+
				"eviction — find out what", rows, index, want)
		}
		if beforeIndex <= want {
			t.Errorf("rows=%d: the index was %d bytes BEFORE any eviction, which is not "+
				"above the floor of %d — this build indexed nothing and the assertion "+
				"above proved nothing", rows, beforeIndex, want)
		}
		headers[rows] = int64(len(hj.parts)) * sizeofJoinIndexPart
		perRow[rows] = float64(index) / float64(rows)
	}
	if headers[2000] != headers[20000] {
		t.Errorf("the per-partition header floor is %d at 2,000 build rows and %d at "+
			"20,000; it is a fixed structure and must not scale with the build",
			headers[2000], headers[20000])
	}
	// The bloom scales, so the floor is not constant — but it must fall STEEPLY
	// per build row, which is the difference between "the bloom survives" and
	// "the index survives". Before the reclaim the floor was 53 bytes per build
	// row at 2,000 rows (106,320) and 28 at 20,000 (562,359); a floor made of
	// the bloom and 64 headers is under 5.
	for rows, r := range perRow {
		if r > 5 {
			t.Errorf("rows=%d: the floor after a full eviction is %.1f bytes per build "+
				"row; a floor made of the bloom and 64 headers is under 5, and 53 is "+
				"what #823 measured when the whole index survived", rows, r)
		}
	}
}

// The reclaim has to reach the TRACKER, not just the heap: #823 is a defect of
// the ledger, and an index that is freed but still charged leaves every
// downstream Reserve measured against a floor that describes memory nobody
// holds.
func TestEvictingEveryPartitionReleasesTheIndexCharge(t *testing.T) {
	const budget = 1 << 20
	schema, data := arrivalBuildRows(2000, "padpadpadpadpadpadpadpadpadpadpad")
	tracker := memory.NewTracker("reclaim", budget)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"k"})
	hj.Spill, hj.MemTracker = sm, tracker
	if err := hj.Build(context.Background(), arrivalSource(schema, data, 256)); err != nil {
		t.Fatalf("build: %v", err)
	}
	chargedBefore := tracker.ForcedFor(memory.ForceJoinIndex)
	evictAll(t, hj)

	// Every byte the join still holds is accounted, and every byte it gave up
	// is gone from the ledger. The index charge is the derived floor.
	if got, want := tracker.ForcedFor(memory.ForceJoinIndex), hj.evictedIndexFloor(); got != want {
		t.Errorf("the tracker still carries %d bytes of hash join index after a full "+
			"eviction; the index is %d. The heap was freed and the ledger was not",
			got, want)
	}
	if chargedBefore <= tracker.ForcedFor(memory.ForceJoinIndex) {
		t.Errorf("the index charge was %d before the eviction and %d after — nothing was "+
			"released, so this test measured nothing", chargedBefore,
			tracker.ForcedFor(memory.ForceJoinIndex))
	}

	// And the whole ledger is conserved: used is exactly what the join says it
	// holds, with nothing left over and nothing over-released.
	if got, want := tracker.Used(), hj.trackedMem; got != want {
		t.Errorf("tracker used=%d but the join says it holds %d", got, want)
	}
	if err := hj.Close(); err != nil {
		t.Fatal(err)
	}
	if got := tracker.Used(); got != 0 {
		t.Errorf("after Close the tracker holds %d bytes for a join that is gone", got)
	}
	for p := memory.ForcePurpose(0); p < memory.ForcePurpose(8); p++ {
		if got := tracker.ForcedFor(p); got != 0 {
			t.Errorf("after Close the forced census still reports %d bytes for %q", got, p)
		}
	}
}
