package physical

import (
	"reflect"
	"testing"
)

func TestExchangeVariantFor(t *testing.T) {
	cases := []struct {
		name string
		req  RequiredDistribution
		want Stage // only Type and ExchangeStage-ish fields checked
	}{
		{
			name: "broadcast -> replicate",
			req:  RequiredDistribution{Kind: RequiredBroadcast},
			want: Stage{Type: StageExchangeReplicate},
		},
		{
			name: "singleton -> gather",
			req:  RequiredDistribution{Kind: RequiredSingleton},
			want: Stage{Type: StageExchangeGather},
		},
		{
			name: "hash-partitioned -> repartition",
			req:  RequiredDistribution{Kind: RequiredHashPartitionedOn, Keys: []string{"a", "b"}, Count: 4},
			want: Stage{Type: StageExchangeRepartition, Exchange: &ExchangeStage{Keys: []string{"a", "b"}, Count: 4}},
		},
		{
			name: "clustered-on -> repartition",
			req:  RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"x"}, Count: 4},
			want: Stage{Type: StageExchangeRepartition, Exchange: &ExchangeStage{Keys: []string{"x"}, Count: 4}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := exchangeVariantFor(c.req)
			if !ok {
				t.Fatalf("exchangeVariantFor returned ok=false")
			}
			if got.Type != c.want.Type {
				t.Errorf("Type: got %q want %q", got.Type, c.want.Type)
			}
			if c.want.Type == StageExchangeRepartition {
				if got.Exchange == nil {
					t.Fatalf("Exchange: got nil, want non-nil ExchangeStage")
				}
				if !reflect.DeepEqual(got.Exchange.Keys, c.want.Exchange.Keys) {
					t.Errorf("Exchange.Keys: got %v want %v", got.Exchange.Keys, c.want.Exchange.Keys)
				}
				if got.Exchange.Count != c.want.Exchange.Count {
					t.Errorf("Exchange.Count: got %d want %d", got.Exchange.Count, c.want.Exchange.Count)
				}
			}
		})
	}
}

func TestExchangeVariantFor_Any_NoInsertion(t *testing.T) {
	_, ok := exchangeVariantFor(RequiredDistribution{Kind: RequiredAny})
	if ok {
		t.Fatal("RequiredAny should return ok=false (no exchange needed)")
	}
}

func TestEnsureDistribution_NoOp(t *testing.T) {
	// Stage DAG: one scan (Singleton) → pipeline (requires Any).
	stages := []Stage{
		{ID: "scan-0", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{ID: "pipe-0", Type: StagePipeline, Dependencies: []string{"scan-0"}, Distribution: Distribution{Kind: DistSingleton}},
	}
	got, err := EnsureDistribution(stages, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(got))
	}
}

// TestEnsureDistribution_InsertsRepartition verifies that a hash-partitioned
// stage feeding the build slot of a hash-join on a different key set triggers
// insertion of a StageExchangeRepartition.
//
// NOTE: Phase 1's RequiredChildDistribution(StageHashJoin, slot=1) returns
// RequiredClusteredOn (not RequiredBroadcast). Phase 1's Satisfies rule says
// DistSingleton satisfies RequiredClusteredOn (a single worker trivially has
// all rows co-located), so a Singleton scan does NOT trigger insertion.
// Instead, we use a DistHashPartitioned input on the wrong keys — that fails
// RequiredClusteredOn(["a"]) because the keys differ. exchangeVariantFor maps
// RequiredClusteredOn → StageExchangeRepartition.
//
// The Replicate/Broadcast variant test is deferred to Task 5 when a stage type
// returning RequiredBroadcast is wired in.
func TestEnsureDistribution_InsertsRepartition(t *testing.T) {
	// The build-side scan is already hash-partitioned on ["b"], but the
	// hash-join requires ClusteredOn(["a"]) for its build slot.
	// DistHashPartitioned{Keys:["b"]} does NOT satisfy RequiredClusteredOn{Keys:["a"]},
	// so EnsureDistribution must splice in a repartition exchange.
	stages := []Stage{
		{
			ID:           "shuffle-build",
			Type:         StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"b"}, Count: 4},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"b"}, Count: 4},
		},
		{ID: "scan-probe", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{
			ID:            "join-0",
			Type:          StageHashJoin,
			LeftDepStage:  "scan-probe",
			RightDepStage: "shuffle-build",
			JoinLeftKeys:  []string{"a"},
			JoinRightKeys: []string{"a"},
			Distribution:  Distribution{Kind: DistSingleton},
			Dependencies:  []string{"scan-probe", "shuffle-build"},
		},
	}
	got, err := EnsureDistribution(stages, 4)
	if err != nil {
		t.Fatal(err)
	}
	// Expect one additional StageExchangeRepartition inserted between shuffle-build and join-0.
	var inserted *Stage
	for i := range got {
		if got[i].Type == StageExchangeRepartition && got[i].ID != "shuffle-build" {
			inserted = &got[i]
		}
	}
	if inserted == nil {
		t.Fatalf("no new StageExchangeRepartition inserted; got types: %v", stageTypes(got))
	}
	// The join stage's RightDepStage should now point at the new exchange, not shuffle-build.
	var join *Stage
	for i := range got {
		if got[i].ID == "join-0" {
			join = &got[i]
		}
	}
	if join.RightDepStage != inserted.ID {
		t.Errorf("join-0.RightDepStage: got %q want %q", join.RightDepStage, inserted.ID)
	}
	if inserted.Distribution.Kind != DistHashPartitioned {
		t.Errorf("inserted exchange Distribution.Kind: got %v want DistHashPartitioned", inserted.Distribution.Kind)
	}
	if !reflect.DeepEqual(inserted.Distribution.Keys, []string{"a"}) {
		t.Errorf("inserted exchange Distribution.Keys: got %v want [a]", inserted.Distribution.Keys)
	}
}

func TestEnsureDistribution_InsertsGather(t *testing.T) {
	// Root stage is hash-partitioned; an implicit final Singleton is
	// required. EnsureDistribution must append a Gather above it.
	stages := []Stage{
		{ID: "scan-0", Type: StageScan,
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"a"}, Count: 4}},
		{
			ID:           "agg-0",
			Type:         StageAggregate,
			Dependencies: []string{"scan-0"},
			GroupByCols:  []string{"a"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"a"}, Count: 4},
		},
	}
	got, err := EnsureDistribution(stages, 4)
	if err != nil {
		t.Fatal(err)
	}
	last := got[len(got)-1]
	if last.Type != StageExchangeGather {
		t.Fatalf("final stage type: got %q want %q", last.Type, StageExchangeGather)
	}
	if last.Distribution.Kind != DistSingleton {
		t.Errorf("gather output distribution: got %v want Singleton", last.Distribution.Kind)
	}
}

func TestEnsureDistribution_StrictModeFailsOnGap(t *testing.T) {
	stages := []Stage{
		{ID: "scan-0", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{ID: "bad-parent", Type: "__test_unsatisfiable", Dependencies: []string{"scan-0"},
			Distribution: Distribution{Kind: DistSingleton}},
	}
	origHook := requiredChildDistributionForTest
	t.Cleanup(func() { requiredChildDistributionForTest = origHook })
	requiredChildDistributionForTest = func(s Stage, slot int) (RequiredDistribution, bool) {
		if s.Type == "__test_unsatisfiable" {
			return RequiredDistribution{Kind: RequiredKind(-1)}, true
		}
		return RequiredDistribution{}, false
	}

	_, err := EnsureDistribution(stages, 4)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func stageTypes(s []Stage) []string {
	out := make([]string, len(s))
	for i, st := range s {
		out[i] = st.Type
	}
	return out
}

func TestEnsureDistribution_MultiConsumerReuse(t *testing.T) {
	// Two joins each consume the same build-scan. Both joins have the
	// same JoinRightKeys (=["k"]), so both ask for RequiredClusteredOn{["k"]}.
	// The shared build's DistHashPartitioned on ["wrong"] fails Satisfies
	// against ["k"] → an exchange is needed. Expect ONE exchange, shared.
	stages := []Stage{
		{ID: "probe-a", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{ID: "probe-b", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{ID: "build-shared", Type: StageScan,
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"wrong"}, Count: 4}},
		{ID: "join-a", Type: StageHashJoin,
			LeftDepStage: "probe-a", RightDepStage: "build-shared",
			JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			Distribution: Distribution{Kind: DistSingleton}},
		{ID: "join-b", Type: StageHashJoin,
			LeftDepStage: "probe-b", RightDepStage: "build-shared",
			JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			Distribution: Distribution{Kind: DistSingleton}},
	}
	got, err := EnsureDistribution(stages, 4)
	if err != nil {
		t.Fatal(err)
	}
	var exchanges int
	var sharedID string
	for _, s := range got {
		if s.Type == StageExchangeRepartition {
			exchanges++
			sharedID = s.ID
		}
	}
	if exchanges != 1 {
		t.Fatalf("expected 1 shared exchange, got %d; stages: %v", exchanges, stageTypes(got))
	}
	var seen []string
	for _, s := range got {
		if s.Type == StageHashJoin {
			seen = append(seen, s.RightDepStage)
		}
	}
	if len(seen) != 2 || seen[0] != sharedID || seen[1] != sharedID {
		t.Errorf("joins should both reference %q; got %v", sharedID, seen)
	}
}

func TestEnsureDistribution_Idempotent(t *testing.T) {
	// Hash-partitioned child on wrong keys feeding a join build slot:
	// first pass inserts a repartition. Second pass should insert nothing.
	stages := []Stage{
		{ID: "scan-probe", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{ID: "scan-build", Type: StageScan,
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"wrong"}, Count: 4}},
		{ID: "join-0", Type: StageHashJoin,
			LeftDepStage: "scan-probe", RightDepStage: "scan-build",
			JoinLeftKeys: []string{"a"}, JoinRightKeys: []string{"a"},
			Distribution: Distribution{Kind: DistSingleton}},
	}
	once, err := EnsureDistribution(stages, 4)
	if err != nil {
		t.Fatal(err)
	}
	twice, err := EnsureDistribution(once, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(once) != len(twice) {
		t.Errorf("second pass changed stage count: %d -> %d", len(once), len(twice))
	}
}
