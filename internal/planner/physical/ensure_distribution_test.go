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
			want: Stage{Type: StageExchangeRepartition, ShuffleKeys: []string{"a", "b"}, NumPartitions: 4},
		},
		{
			name: "clustered-on -> repartition",
			req:  RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"x"}, Count: 4},
			want: Stage{Type: StageExchangeRepartition, ShuffleKeys: []string{"x"}, NumPartitions: 4},
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
				if !reflect.DeepEqual(got.ShuffleKeys, c.want.ShuffleKeys) {
					t.Errorf("ShuffleKeys: got %v want %v", got.ShuffleKeys, c.want.ShuffleKeys)
				}
				if got.NumPartitions != c.want.NumPartitions {
					t.Errorf("NumPartitions: got %d want %d", got.NumPartitions, c.want.NumPartitions)
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
			ShuffleKeys:  []string{"b"},
			NumPartitions: 4,
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

func stageTypes(s []Stage) []string {
	out := make([]string, len(s))
	for i, st := range s {
		out[i] = st.Type
	}
	return out
}
