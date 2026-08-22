package worker

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// TestPartialAggDeclaresItsEpochCap pins the plumbing behind the
// construction-time group-index layout decision (exec/two_level_hash.go,
// twoLevelBoundedMinGroups). This operator finalizes and rebuilds its
// HashAggregate every capBytes, so the aggregate must LEARN that before Init
// and pin its index flat: SF100 Q18 measured the alternative (one
// flat->bucketed conversion per epoch, never amortized) at 3-4x on the
// exchange-repartition stage.
func TestPartialAggDeclaresItsEpochCap(t *testing.T) {
	p := newCappedPartialAgg([]string{"k"},
		[]distributed.AggSpec{{Func: "sum", InputCol: "q", OutputCol: "q"}}, 0)
	b := batch.FromRows(partialAggSchema(), []map[string]any{
		{"k": int64(1), "q": 2.0},
		{"k": int64(2), "q": 3.0},
	})
	if _, err := p.consume(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if p.agg == nil {
		t.Fatal("partial agg did not build a HashAggregate")
	}
	if !p.agg.IndexBornFlat() {
		t.Errorf("aggregate not born flat: the %d MB epoch cap did not reach it",
			p.capBytes/(1<<20))
	}
	ceiling := p.agg.GroupCeiling()
	if ceiling <= 0 {
		t.Fatalf("group ceiling = %d, want a positive bound derived from the cap", ceiling)
	}
	// The default cap is 128 MB and this shape costs ~38 B per group, so the
	// epoch tops out in the low millions - the number the Q18 stage measured
	// (2.50 M groups per flush).
	if ceiling > 8<<20 {
		t.Errorf("group ceiling = %d, want a few million for a %d MB cap",
			ceiling, p.capBytes/(1<<20))
	}
	if _, err := p.drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.conversions != 0 || !p.bornFlat {
		t.Errorf("after drain: conversions=%d bornFlat=%v, want 0/true",
			p.conversions, p.bornFlat)
	}
}
