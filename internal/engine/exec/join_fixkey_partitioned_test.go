package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// `FixKeyAssignment` is the runtime repair for a plan that put the join keys on
// the wrong sides. It re-indexes every stored build batch in ONE pass, so it
// rebuilds a FLAT index — and after the index became per grace partition it had
// to say so, because a rebuild that leaves `partMask` at 63 routes keys across
// 64 tables while only part 0 has been pre-grown, and `PutNoGrow` on a table
// with no headroom never returns.
//
// Nothing else in the package reaches the repair on a partitioned build: every
// other `FixKeyAssignment` fixture builds flat (no `Spill`, no `MemTracker`),
// where `parts` is already 1 and `partMask` already 0, so the repair's own
// branch is unreachable from them and re-introducing the hang would be silent.
//
// The shape that DOES reach it is a spill-eligible build — `Spill` and
// `MemTracker` both set, so `buildPartitioned` runs and creates `spillState`
// with 64 parts — at a budget large enough that NOTHING is evicted, because the
// repair returns early on the first nil `buildBatches` slot. The right key is
// spelled with a qualifier the build schema does not carry, which is what makes
// the repair decide the sides are swapped.
func TestTheKeySideRepairRebuildsAPartitionedBuildIntoOnePart(t *testing.T) {
	const budget = 64 << 20 // large on purpose: partitions, but no eviction
	schema, data := arrivalBuildRows(5000, "padpadpadpadpadpadpadpadpadpadpad")
	tracker := memory.NewTracker("fixkey", budget)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"src.k"})
	hj.Spill, hj.MemTracker = sm, tracker
	evictedBefore := JoinPartitionsEvicted.Load()
	if err := hj.Build(context.Background(), arrivalSource(schema, data, 256)); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Engagement, both halves: the build partitioned, and it evicted nothing.
	// Either one missing and the repair below never reaches its rebuild.
	if hj.spillState == nil || len(hj.parts) != numSpillPartitions || hj.partMask != spillPartMask {
		t.Fatalf("this build did not partition (spillState=%v parts=%d mask=%d); the "+
			"repair's flat-rebuild branch is unreachable and this test proves nothing",
			hj.spillState != nil, len(hj.parts), hj.partMask)
	}
	if n := JoinPartitionsEvicted.Load() - evictedBefore; n != 0 {
		t.Fatalf("%d partitions were evicted, so FixKeyAssignment returns at its nil-slot "+
			"guard and never rebuilds; raise the budget", n)
	}

	if !hj.FixKeyAssignment() {
		t.Fatal("no key swap fired; the fixture stopped reaching the repair at all")
	}

	// The rebuild is flat, and it indexed everything exactly once.
	if len(hj.parts) != 1 || hj.partMask != 0 {
		t.Errorf("after the rebuild the join has %d index parts and partMask=%d; the "+
			"rebuild writes ONE part, and a probe that reads a different one by mask "+
			"finds nothing", len(hj.parts), hj.partMask)
	}
	if hj.buildRows != int64(len(data)) {
		t.Errorf("rebuild counted %d build rows, want %d", hj.buildRows, len(data))
	}
	if got := hj.arenaRows(); got != len(data) {
		t.Errorf("rebuild left %d arena refs, want %d — a rebuild that starts from a "+
			"non-empty index indexes every key twice", got, len(data))
	}
	for i := 0; i < len(data); i++ {
		pt := hj.idxPart(int64(i))
		if pt.ints == nil {
			t.Fatalf("key %d routes to a part with no table", i)
		}
		if _, ok := pt.ints.Get(int64(i)); !ok {
			t.Fatalf("key %d is not in the rebuilt index", i)
		}
	}
}
