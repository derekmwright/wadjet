package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// The aggregate-shuffle pre-compute synthesises SQL that writes each GROUP BY
// key TWICE from ONE list — once as a select item and once in the `GROUP BY`
// clause — over the BASE TABLE (`buildAggregateShuffleSQL`). That is sound
// only while a key's PUBLISHED name is also a spelling the base table can
// evaluate, and since ADR-0026 §2 those are two fields: a key naming a derived
// table's alias publishes `x.w` while the fragment evaluates `a * 3`.
//
// `AggShuffleRejectKeyNameIsNotItsSpelling` is the decline, and this file is
// the fixture that reaches it. Without one the guard was untested code on the
// default path — and it could not fire at all, because `followToAggregate`
// hands it the MERGE, which by design carries no resolution list, and the
// guard answered "the two names are the same" for every plan (#794 round 2).

// aggShuffleChain builds the canonical candidate chain — a fused
// scan-aggregate, its merge, a shuffle, and the join whose build side it is —
// with the key's two names given explicitly.
func aggShuffleChain(published string, resolve GroupKeyResolution) []Stage {
	const big = int64(8 * 1024 * 1024 * 1024)
	return []Stage{
		{
			ID: "scan-0", Type: StageScan, TableName: "lineitem", ScanAlias: "lineitem",
			Columns: []string{"a", "id"}, EstimatedBytes: big,
			FusedAggGroupBy: []string{published},
			GroupByResolve:  []GroupKeyResolution{resolve},
			FusedAggSpecs:   []AggSpec{{Func: "SUM", InputCol: "a", OutputCol: "s"}},
		},
		{
			ID: "final_aggregate-1", Type: StageFinalAggregate,
			GroupByCols:  []string{published},
			AggSpecs:     []AggSpec{{Func: "SUM", InputCol: "s", OutputCol: "s"}},
			Dependencies: []string{"scan-0"},
		},
		{
			ID: "exchange-repartition-2", Type: StageExchangeRepartition,
			Dependencies: []string{"final_aggregate-1"},
			Exchange:     &ExchangeStage{Keys: []string{published}, Count: 4},
		},
		{
			ID: "scan-3", Type: StageScan, TableName: "part", ScanAlias: "part",
			Columns: []string{"p_partkey"}, EstimatedBytes: 1024,
		},
		{
			ID: "join-4", Type: StageHashJoin,
			JoinLeftKeys: []string{"p_partkey"}, JoinRightKeys: []string{published},
			LeftDepStage: "scan-3", RightDepStage: "exchange-repartition-2",
			Dependencies: []string{"scan-3", "exchange-repartition-2"},
		},
	}
}

// TestAggregateShuffleDeclinesAKeyWhoseNameIsNotItsSpelling is the missing
// fixture: the candidate is otherwise perfect — the scan is far over the
// threshold, the join's build key IS the group key, no filters — and it is
// declined for the one reason the branch exists.
func TestAggregateShuffleDeclinesAKeyWhoseNameIsNotItsSpelling(t *testing.T) {
	const threshold = int64(4 * 1024 * 1024 * 1024)

	// The CONTROL first, so a decline that fires for some other reason cannot
	// pass as this one: the same chain with a key whose two names agree is
	// picked up.
	ctl := aggShuffleChain("a", GroupKeyResolution{Expr: "a"})
	cand, ok := PickAggregateShuffleCandidate(ctl, threshold)
	if !ok {
		t.Fatalf("the control chain produced no candidate — the fixture no longer reaches the "+
			"reject this test is about (reason %v)",
			PickAggregateShuffleCandidateDiag(ctl, threshold).Reason)
	}
	if cand.AggregateStageID != "final_aggregate-1" {
		t.Errorf("control candidate names aggregate %q, want final_aggregate-1", cand.AggregateStageID)
	}

	// And the shape the guard exists for: the key is PUBLISHED as the derived
	// table's alias and RESOLVED by an expression over the base table's
	// columns. `SELECT x.w … GROUP BY x.w` over `lineitem` names a column
	// `lineitem` does not have.
	bad := aggShuffleChain("x.w", GroupKeyResolution{Expr: "a * 3", Computed: true})
	if _, ok := PickAggregateShuffleCandidate(bad, threshold); ok {
		t.Fatalf("a key published as %q and resolved by %q was accepted — the pre-compute SQL "+
			"would emit `SELECT x.w … FROM lineitem GROUP BY x.w`, over a column the table does "+
			"not have (ADR-0026 §2)", "x.w", "a * 3")
	}
	if got := PickAggregateShuffleCandidateDiag(bad, threshold).Reason; got != AggShuffleRejectKeyNameIsNotItsSpelling {
		t.Errorf("declined for %v, want %v — a decline for another reason would leave this "+
			"branch untested", got, AggShuffleRejectKeyNameIsNotItsSpelling)
	}

	// A chain whose key-computing stage this pass cannot reach answers NO
	// rather than YES: the list it would build SQL from is one it cannot
	// vouch for.
	orphan := aggShuffleChain("a", GroupKeyResolution{Expr: "a"})
	orphan[0].GroupByResolve = nil // the fused scan stops carrying the pair
	if _, ok := PickAggregateShuffleCandidate(orphan, threshold); ok {
		t.Error("a chain with no key-computing stage was accepted — the guard must decline " +
			"what it cannot answer")
	}
}

// TestAggregateShuffleGuardReachesTheKeyComputingStage is the same claim over
// a REAL plan, and it is the half the hand-built chain cannot make: on the
// canonical Q17 chain the stage `followToAggregate` returns is the MERGE, and
// the guard has to walk past it to the fragment that resolves the keys.
func TestAggregateShuffleGuardReachesTheKeyComputingStage(t *testing.T) {
	old := logical.ScalarAggSemijoin.Load()
	logical.ScalarAggSemijoin.Store(false)
	defer logical.ScalarAggSemijoin.Store(old)
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT SUM(l_extendedprice) / 7.0 as avg_yearly
		FROM lineitem JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
		  AND l_quantity < (
		    SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey
		  )`
	stages := sqlToStages(t, cat, ctx, sql, 3)
	byID := make(map[string]Stage, len(stages))
	for _, s := range stages {
		byID[s.ID] = s
	}
	checked := 0
	for _, j := range stages {
		if !isJoinStage(j.Type) {
			continue
		}
		start := j.RightDepStage
		if start == "" && len(j.Dependencies) > 1 {
			start = j.Dependencies[1]
		}
		agg, ok := followToAggregate(byID, start)
		if !ok {
			continue
		}
		checked++
		if len(agg.GroupByResolve) != 0 {
			t.Errorf("followToAggregate returned %s, which carries a resolution list — this "+
				"test's premise (it returns the MERGE) no longer holds; re-read the walk",
				agg.ID)
		}
		c, found := followToKeyComputingStage(byID, agg)
		if !found {
			t.Fatalf("the guard could not reach a key-computing stage from %s — it would "+
				"decline a candidate the pass is built for", agg.ID)
		}
		if !stageComputesGroupKeys(&c) || len(c.GroupByResolve) == 0 {
			t.Errorf("the walk stopped at %s (%s), which does not compute its keys", c.ID, c.Type)
		}
		if !keyNamesAreTheirSpelling(byID, agg) {
			t.Errorf("Q17's key %v was declined — its two names are the same string, and "+
				"declining it costs the pre-compute this pass exists for",
				stageGroupKeyList(&c))
		}
	}
	if checked == 0 {
		t.Fatal("no join in Q17's plan resolved to an aggregate — the assertion saw nothing")
	}
}
