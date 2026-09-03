package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// #823's OPEN half, measured and pinned.
//
// #823 has two halves. The first — a hash index PRE-SIZED past the budget's
// room for rows that had not arrived — is fixed and gated by
// TestHashIndexIsNotPreSizedPastTheBudgetsRoom. This is the second: the index
// charged for rows that HAVE arrived is unreleasable by a grace eviction, so
// after evicting every partition the query's floor still carries it.
//
// The mechanism is that the index is GLOBAL across the 64 partitions.
// spillOneInMemoryPartition writes a partition's batches to disk and nils
// their h.buildBatches slots, which frees the COLUMN data; the arena entries
// and chain links that point at those slots, and the hash-table slots that
// point at the arena, stay. They are unreachable — that is the eviction's own
// contract — and they are still resident, so the charge is honest and the
// memory is waste.
//
// WHAT THIS TEST IS. It is not a regression test for a fix; it is the
// measurement, pinned so that a fix cannot land without deleting it. It
// asserts the DIRECTION the defect has (used stays far above the floor after
// a full eviction) and the SHAPE of what is left (nearly all of it is index),
// and it FAILS if either stops being true — including if `used` comes down,
// which is what the fix looks like.
func TestEvictingEveryPartitionLeavesTheIndexCharged(t *testing.T) {
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

	// Evict everything this operator can, which is what a pressured query
	// does through spillUntilCanReserve.
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
	live := 0
	for _, b := range hj.buildBatches {
		if b != nil {
			live++
		}
	}
	if live != 0 {
		t.Fatalf("%d build batches are still in memory after evicting %d partitions — "+
			"the measurement below is not of a fully evicted join", live, evicted)
	}

	used, index := tracker.Used(), hj.hashTableOverhead()
	arena := int64(cap(hj.arena))*8 + int64(cap(hj.arenaNext))*4
	t.Logf("after evicting %d partitions (%d build batches freed): used=%d of a %d budget; "+
		"index=%d (arena+chain %d, hash table %d, bloom %d)",
		evicted, len(hj.buildBatches), used, int64(budget), index, arena,
		index-arena-int64(len(hj.bloom))*8, int64(len(hj.bloom))*8)

	// THE PIN, in the direction an open defect needs it. A join that has
	// evicted every partition holds no build column data at all, so its floor
	// should be near zero; it is not, and nearly all of what is left is the
	// index. Measured on this fixture: used=106,320 with index=98,304 — 92%.
	//
	// The 3/4 threshold is not a tuning knob, it is the CLAIM: "what survives
	// a full eviction is the index, not something else". A fix makes `used`
	// fall and fails the first branch; a change that leaves `used` high for a
	// DIFFERENT reason fails the second, and that is worth knowing too,
	// because it would mean this pin had stopped measuring #823.
	if used < index {
		t.Fatalf("used=%d is now BELOW the index's own %d bytes — the index has become "+
			"releasable, which is #823's reclaim half. Delete this pin, update ADR-0006's "+
			"producers table (producer 5's 'released' column) and close #823; that the "+
			"floor came down is the fix's proof", used, index)
	}
	if index*4 < used*3 {
		t.Fatalf("used=%d after a full eviction but only %d of it is index — this pin has "+
			"stopped measuring #823 and is now measuring something else; find out what "+
			"else a fully evicted join is holding", used, index)
	}
}
