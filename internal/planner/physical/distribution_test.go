package physical

import (
	"strings"
	"testing"
)

func TestDistributionEquals(t *testing.T) {
	tests := []struct {
		name string
		a, b Distribution
		want bool
	}{
		{"both broadcast", Distribution{Kind: DistBroadcast}, Distribution{Kind: DistBroadcast}, true},
		{"singleton vs broadcast", Distribution{Kind: DistSingleton}, Distribution{Kind: DistBroadcast}, false},
		{"hash same keys same count", Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 12}, Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 12}, true},
		{"hash different keys", Distribution{Kind: DistHashPartitioned, Keys: []string{"a"}, Count: 12}, Distribution{Kind: DistHashPartitioned, Keys: []string{"b"}, Count: 12}, false},
		{"hash different count", Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 12}, Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 24}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Equals(tt.b); got != tt.want {
				t.Errorf("Equals = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDistributionSatisfiesJoin(t *testing.T) {
	d := Distribution{Kind: DistHashPartitioned, Keys: []string{"orderkey"}, Count: 12}
	if !d.SatisfiesJoinKeys([]string{"orderkey"}) {
		t.Error("expected hash-on-orderkey to satisfy join on orderkey")
	}
	if d.SatisfiesJoinKeys([]string{"custkey"}) {
		t.Error("expected hash-on-orderkey to NOT satisfy join on custkey")
	}
	bcast := Distribution{Kind: DistBroadcast}
	if !bcast.SatisfiesJoinKeys([]string{"anything"}) {
		t.Error("broadcast should always satisfy join")
	}
}

func TestDistributionSatisfies(t *testing.T) {
	hashOrderkey := Distribution{Kind: DistHashPartitioned, Keys: []string{"orderkey"}, Count: 12}
	hashCustkey := Distribution{Kind: DistHashPartitioned, Keys: []string{"custkey"}, Count: 12}
	hashOrderkey24 := Distribution{Kind: DistHashPartitioned, Keys: []string{"orderkey"}, Count: 24}
	singleton := Distribution{Kind: DistSingleton}
	bcast := Distribution{Kind: DistBroadcast}

	reqAny := RequiredDistribution{Kind: RequiredAny}
	reqSingleton := RequiredDistribution{Kind: RequiredSingleton}
	reqBcast := RequiredDistribution{Kind: RequiredBroadcast}
	reqClusterOrderkey := RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"orderkey"}}
	reqClusterCustkey := RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"custkey"}}
	reqHashOrderkey12 := RequiredDistribution{Kind: RequiredHashPartitionedOn, Keys: []string{"orderkey"}, Count: 12}
	reqHashOrderkey24 := RequiredDistribution{Kind: RequiredHashPartitionedOn, Keys: []string{"orderkey"}, Count: 24}

	tests := []struct {
		name string
		dist Distribution
		req  RequiredDistribution
		want bool
	}{
		// RequiredAny is satisfied by everything
		{"singleton sat any", singleton, reqAny, true},
		{"broadcast sat any", bcast, reqAny, true},
		{"hash sat any", hashOrderkey, reqAny, true},

		// RequiredSingleton: only DistSingleton
		{"singleton sat singleton", singleton, reqSingleton, true},
		{"broadcast not sat singleton", bcast, reqSingleton, false},
		{"hash not sat singleton", hashOrderkey, reqSingleton, false},

		// RequiredBroadcast: only DistBroadcast
		{"broadcast sat broadcast", bcast, reqBcast, true},
		{"singleton not sat broadcast", singleton, reqBcast, false},
		{"hash not sat broadcast", hashOrderkey, reqBcast, false},

		// RequiredClusteredOn(K): broadcast yes, singleton yes, hash iff Keys==K
		{"broadcast sat clustered", bcast, reqClusterOrderkey, true},
		{"singleton sat clustered", singleton, reqClusterOrderkey, true},
		{"hash on K sat clustered K", hashOrderkey, reqClusterOrderkey, true},
		{"hash on K not sat clustered K2", hashOrderkey, reqClusterCustkey, false},
		{"hash on K2 not sat clustered K", hashCustkey, reqClusterOrderkey, false},

		// RequiredHashPartitionedOn(K, N): only hash with same K and N
		{"hash K N sat hash K N", hashOrderkey, reqHashOrderkey12, true},
		{"hash K N' not sat hash K N", hashOrderkey24, reqHashOrderkey12, false},
		{"hash K N sat hash K N'", hashOrderkey, reqHashOrderkey24, false},
		{"hash K not sat hash K2 N", hashCustkey, reqHashOrderkey12, false},
		{"singleton not sat hash K N", singleton, reqHashOrderkey12, false},
		{"broadcast not sat hash K N", bcast, reqHashOrderkey12, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.dist.Satisfies(tt.req)
			if got != tt.want {
				t.Errorf("(%v).Satisfies(%v) = %v, want %v", tt.dist, tt.req, got, tt.want)
			}
		})
	}
}

func TestRequiredChildDistribution(t *testing.T) {
	tests := []struct {
		name  string
		stage Stage
		slot  int
		want  RequiredDistribution
	}{
		{
			name:  "scan has no inputs returns any",
			stage: Stage{ID: "scan-0", Type: "scan"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "dual has no inputs returns any",
			stage: Stage{ID: "dual-0", Type: "dual"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "exchange-repartition accepts any input",
			stage: Stage{ID: "shuffle-0", Type: StageExchangeRepartition, Exchange: &ExchangeStage{Keys: []string{"k"}, Count: 16}},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name: "hash_join probe slot requires clustered on left keys",
			stage: Stage{
				ID: "join-0", Type: "hash_join",
				JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
				LeftDepStage: "shuffle-l", RightDepStage: "shuffle-r",
				Dependencies: []string{"shuffle-l", "shuffle-r"},
			},
			slot: 0,
			want: RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"l_orderkey"}},
		},
		{
			name: "hash_join build slot requires clustered on right keys",
			stage: Stage{
				ID: "join-0", Type: "hash_join",
				JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
				LeftDepStage: "shuffle-l", RightDepStage: "shuffle-r",
				Dependencies: []string{"shuffle-l", "shuffle-r"},
			},
			slot: 1,
			want: RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"o_orderkey"}},
		},
		{
			name: "broadcast_join probe slot requires any (Phase 1)",
			stage: Stage{
				ID: "join-0", Type: "broadcast_join",
				JoinLeftKeys: []string{"l_partkey"}, JoinRightKeys: []string{"p_partkey"},
				LeftDepStage: "scan-l", RightDepStage: "scan-r",
				Dependencies: []string{"scan-l", "scan-r"},
			},
			slot: 0,
			want: RequiredDistribution{Kind: RequiredAny},
		},
		{
			name: "broadcast_join build slot requires broadcast",
			stage: Stage{
				ID: "join-0", Type: "broadcast_join",
				JoinLeftKeys: []string{"l_partkey"}, JoinRightKeys: []string{"p_partkey"},
				LeftDepStage: "scan-l", RightDepStage: "scan-r",
				Dependencies: []string{"scan-l", "scan-r"},
			},
			slot: 1,
			want: RequiredDistribution{Kind: RequiredBroadcast},
		},
		{
			name:  "aggregate requires any (Phase 1 conservative — see Risk #1)",
			stage: Stage{ID: "aggregate-0", Type: "aggregate", GroupByCols: []string{"l_returnflag"}},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "grouped final_aggregate requires clustered_on group keys",
			stage: Stage{ID: "final_aggregate-0", Type: "final_aggregate", GroupByCols: []string{"l_returnflag"}},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"l_returnflag"}},
		},
		{
			name:  "scalar final_aggregate (no GroupByCols) requires any",
			stage: Stage{ID: "final_aggregate-1", Type: "final_aggregate"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "final_aggregate with Limit stays on RequiredAny",
			stage: Stage{ID: "final_aggregate-2", Type: "final_aggregate", GroupByCols: []string{"l_returnflag"}, Limit: 10},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "sort requires any",
			stage: Stage{ID: "sort-0", Type: "sort"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "merge_sort requires any",
			stage: Stage{ID: "merge_sort-0", Type: "merge_sort"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name: "window with PartitionBy requires clustered on partition keys",
			stage: Stage{
				ID: "window-0", Type: "window",
				WindowCols: []WindowColSpec{{Func: "row_number", PartitionBy: []string{"o_custkey"}}},
			},
			slot: 0,
			want: RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"o_custkey"}},
		},
		{
			name: "window without PartitionBy requires any",
			stage: Stage{
				ID: "window-0", Type: "window",
				WindowCols: []WindowColSpec{{Func: "row_number"}},
			},
			slot: 0,
			want: RequiredDistribution{Kind: RequiredAny},
		},
		{
			// One window stage can carry several OVER clauses. Clustering
			// on the first column's keys would slice the SECOND column's
			// partitions across tasks, and each task would compute it over
			// a fragment of its partition — a wrong answer, so the stage
			// asks for nothing and runs Singleton instead.
			name: "window with disagreeing PartitionBy requires any",
			stage: Stage{
				ID: "window-0", Type: "window",
				WindowCols: []WindowColSpec{
					{Func: "row_number", PartitionBy: []string{"o_custkey"}},
					{Func: "rank", PartitionBy: []string{"o_orderstatus"}},
				},
			},
			slot: 0,
			want: RequiredDistribution{Kind: RequiredAny},
		},
		{
			// A global window mixed in has the same effect for a stronger
			// reason: its universe is every row, so no exchange makes a
			// task self-sufficient.
			name: "window with one global column requires any",
			stage: Stage{
				ID: "window-0", Type: "window",
				WindowCols: []WindowColSpec{
					{Func: "row_number", PartitionBy: []string{"o_custkey"}},
					{Func: "rank"},
				},
			},
			slot: 0,
			want: RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "pipeline requires any",
			stage: Stage{ID: "pipeline-0", Type: "pipeline"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "table_func requires any",
			stage: Stage{ID: "tf-0", Type: "table_func"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "unknown stage type requires any",
			stage: Stage{ID: "?-0", Type: "mystery"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RequiredChildDistribution(tt.stage, tt.slot)
			if got.Kind != tt.want.Kind {
				t.Fatalf("Kind = %v, want %v", got.Kind, tt.want.Kind)
			}
			if !keysEqual(got.Keys, tt.want.Keys) {
				t.Fatalf("Keys = %v, want %v", got.Keys, tt.want.Keys)
			}
			if got.Count != tt.want.Count {
				t.Fatalf("Count = %v, want %v", got.Count, tt.want.Count)
			}
		})
	}
}

func TestOutputDistribution(t *testing.T) {
	tests := []struct {
		name        string
		stage       Stage
		deps        map[string]Distribution
		workerCount int // 0 means default to 4
		want        Distribution
	}{
		{
			name:  "scan emits singleton",
			stage: Stage{ID: "scan-0", Type: "scan", ScanAlias: "lineitem"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name:  "dual emits singleton",
			stage: Stage{ID: "dual-0", Type: "dual"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name: "exchange-repartition emits hash partitioned",
			stage: Stage{
				ID:       "shuffle-0",
				Type:     StageExchangeRepartition,
				Exchange: &ExchangeStage{Keys: []string{"l_orderkey"}, Count: 16},
			},
			deps: nil,
			want: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
		{
			name: "hash_join inherits probe distribution",
			stage: Stage{
				ID: "join-0", Type: "hash_join",
				LeftDepStage: "shuffle-l", RightDepStage: "shuffle-r",
				Dependencies: []string{"shuffle-l", "shuffle-r"},
			},
			deps: map[string]Distribution{
				"shuffle-l": {Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
				"shuffle-r": {Kind: DistHashPartitioned, Keys: []string{"o_orderkey"}, Count: 16},
			},
			want: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
		{
			name: "broadcast_join inherits probe distribution",
			stage: Stage{
				ID: "join-0", Type: "broadcast_join",
				LeftDepStage: "scan-l", RightDepStage: "scan-r",
				Dependencies: []string{"scan-l", "scan-r"},
			},
			deps: map[string]Distribution{
				"scan-l": {Kind: DistSingleton},
				"scan-r": {Kind: DistSingleton},
			},
			want: Distribution{Kind: DistSingleton},
		},
		{
			name:        "grouped aggregate multi-worker emits round-robin",
			stage:       Stage{ID: "aggregate-0", Type: "aggregate", GroupByCols: []string{"l_returnflag"}},
			deps:        nil,
			workerCount: 4,
			want:        Distribution{Kind: DistRoundRobin},
		},
		{
			name:        "grouped aggregate single-worker stays singleton",
			stage:       Stage{ID: "aggregate-0", Type: "aggregate", GroupByCols: []string{"l_returnflag"}},
			deps:        nil,
			workerCount: 1,
			want:        Distribution{Kind: DistSingleton},
		},
		{
			name:        "scalar aggregate (no GroupByCols) stays singleton even multi-worker",
			stage:       Stage{ID: "aggregate-0", Type: "aggregate"},
			deps:        nil,
			workerCount: 4,
			want:        Distribution{Kind: DistSingleton},
		},
		{
			name: "final_aggregate lone reducer emits singleton",
			stage: Stage{
				ID: "final_aggregate-0", Type: "final_aggregate",
				GroupByCols: []string{"l_returnflag"},
			},
			deps: nil,
			want: Distribution{Kind: DistSingleton},
		},
		{
			name: "final_aggregate merge group emits hash partitioned",
			stage: Stage{
				ID: "merge_aggregate-0-0", Type: "final_aggregate",
				GroupByCols:     []string{"l_returnflag"},
				MergeGroup:      0,
				MergeGroupCount: 4,
			},
			deps: nil,
			want: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_returnflag"}, Count: 4},
		},
		{
			name:  "sort emits singleton",
			stage: Stage{ID: "sort-0", Type: "sort"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name:  "merge_sort lone emits singleton",
			stage: Stage{ID: "merge_sort-0", Type: "merge_sort"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name: "merge_sort merge group emits hash partitioned",
			stage: Stage{
				ID: "merge_sort-0-0", Type: "merge_sort",
				SortKeys:        []SortKeySpec{{Column: "l_orderkey"}},
				MergeGroup:      0,
				MergeGroupCount: 4,
			},
			deps: nil,
			want: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 4},
		},
		{
			name:  "window emits singleton",
			stage: Stage{ID: "window-0", Type: "window"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			// The partition-parallel shape: the input already arrives
			// hash-partitioned on exactly the PARTITION BY keys, so every
			// window partition is whole inside one input partition and the
			// stage fans out to one task per partition.
			name: "window over a matching hash exchange mirrors it",
			stage: Stage{
				ID: "window-0", Type: "window",
				Dependencies: []string{"ex-1"},
				WindowCols: []WindowColSpec{
					{Func: "row_number", PartitionBy: []string{"o_custkey"}},
					{Func: "sum", InputCol: "o_totalprice", PartitionBy: []string{"o_custkey"}},
				},
			},
			deps: map[string]Distribution{
				"ex-1": {Kind: DistHashPartitioned, Keys: []string{"o_custkey"}, Count: 8},
			},
			want: Distribution{Kind: DistHashPartitioned, Keys: []string{"o_custkey"}, Count: 8},
		},
		{
			name: "window over an exchange on other keys stays singleton",
			stage: Stage{
				ID: "window-0", Type: "window",
				Dependencies: []string{"ex-1"},
				WindowCols:   []WindowColSpec{{Func: "row_number", PartitionBy: []string{"o_custkey"}}},
			},
			deps: map[string]Distribution{
				"ex-1": {Kind: DistHashPartitioned, Keys: []string{"o_orderkey"}, Count: 8},
			},
			want: Distribution{Kind: DistSingleton},
		},
		{
			// The gate that keeps a mixed stage correct: even over an
			// exchange matching the FIRST column's keys, the stage stays
			// Singleton — one task reads every partition, which is the
			// only grain that serves both OVER clauses.
			name: "window with disagreeing PartitionBy stays singleton over a matching exchange",
			stage: Stage{
				ID: "window-0", Type: "window",
				Dependencies: []string{"ex-1"},
				WindowCols: []WindowColSpec{
					{Func: "row_number", PartitionBy: []string{"o_custkey"}},
					{Func: "rank", PartitionBy: []string{"o_orderstatus"}},
				},
			},
			deps: map[string]Distribution{
				"ex-1": {Kind: DistHashPartitioned, Keys: []string{"o_custkey"}, Count: 8},
			},
			want: Distribution{Kind: DistSingleton},
		},
		{
			name: "global window stays singleton over a hash exchange",
			stage: Stage{
				ID: "window-0", Type: "window",
				Dependencies: []string{"ex-1"},
				WindowCols:   []WindowColSpec{{Func: "row_number"}},
			},
			deps: map[string]Distribution{
				"ex-1": {Kind: DistHashPartitioned, Keys: []string{"o_custkey"}, Count: 8},
			},
			want: Distribution{Kind: DistSingleton},
		},
		{
			name:  "pipeline emits singleton",
			stage: Stage{ID: "pipeline-0", Type: "pipeline"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name:  "table_func emits singleton",
			stage: Stage{ID: "tf-0", Type: "table_func"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name:  "unknown stage type emits singleton",
			stage: Stage{ID: "?-0", Type: "mystery"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wc := tt.workerCount
			if wc == 0 {
				wc = 4
			}
			got := OutputDistribution(tt.stage, tt.deps, wc)
			if !got.Equals(tt.want) {
				t.Fatalf("OutputDistribution = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestAssignStageDistributions(t *testing.T) {
	// Synthetic 3-stage plan: scan -> shuffle -> join (with another scan + shuffle as build)
	stages := []Stage{
		{ID: "scan-0", Type: "scan", ScanAlias: "lineitem"},
		{ID: "scan-1", Type: "scan", ScanAlias: "orders"},
		{
			ID:           "shuffle-2",
			Type:         StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"l_orderkey"}, Count: 16},
			Dependencies: []string{"scan-0"},
		},
		{
			ID:           "shuffle-3",
			Type:         StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"o_orderkey"}, Count: 16},
			Dependencies: []string{"scan-1"},
		},
		{
			ID: "join-4", Type: "hash_join",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "shuffle-2", RightDepStage: "shuffle-3",
			Dependencies: []string{"shuffle-2", "shuffle-3"},
		},
	}

	assignStageDistributions(stages, 4)

	want := map[string]Distribution{
		"scan-0":    {Kind: DistSingleton},
		"scan-1":    {Kind: DistSingleton},
		"shuffle-2": {Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		"shuffle-3": {Kind: DistHashPartitioned, Keys: []string{"o_orderkey"}, Count: 16},
		// Hash join inherits probe-side distribution (shuffle-2)
		"join-4": {Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
	}
	for _, s := range stages {
		got := s.Distribution
		w := want[s.ID]
		if !got.Equals(w) {
			t.Errorf("stage %s: Distribution = %+v, want %+v", s.ID, got, w)
		}
	}
}

func TestAssignStageDistributions_OutOfOrderInput(t *testing.T) {
	// Stages provided out of topological order. The pass must still resolve
	// dependencies (e.g. join-4 declared before its shuffle deps).
	stages := []Stage{
		{
			ID: "join-4", Type: "hash_join",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "shuffle-2", RightDepStage: "shuffle-3",
			Dependencies: []string{"shuffle-2", "shuffle-3"},
		},
		{
			ID:           "shuffle-2",
			Type:         StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"l_orderkey"}, Count: 16},
			Dependencies: []string{"scan-0"},
		},
		{
			ID:           "shuffle-3",
			Type:         StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"o_orderkey"}, Count: 16},
			Dependencies: []string{"scan-1"},
		},
		{ID: "scan-0", Type: "scan", ScanAlias: "lineitem"},
		{ID: "scan-1", Type: "scan", ScanAlias: "orders"},
	}

	assignStageDistributions(stages, 4)

	for _, s := range stages {
		if s.ID == "join-4" {
			want := Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16}
			if !s.Distribution.Equals(want) {
				t.Errorf("join-4 Distribution = %+v, want %+v (dep resolution should not depend on input order)", s.Distribution, want)
			}
		}
	}
}

func TestAssertExchangeConsistency_ConsistentPlan(t *testing.T) {
	// scan -> shuffle (on l_orderkey) -> join.probe (RequiredClusteredOn l_orderkey)
	// scan -> shuffle (on o_orderkey) -> join.build (RequiredClusteredOn o_orderkey)
	// All edges satisfy: hash-partitioned-on-K satisfies clustered-on-K.
	stages := []Stage{
		{ID: "scan-0", Type: "scan", Distribution: Distribution{Kind: DistSingleton}},
		{ID: "scan-1", Type: "scan", Distribution: Distribution{Kind: DistSingleton}},
		{
			ID:           "shuffle-2",
			Type:         StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"l_orderkey"}, Count: 16},
			Dependencies: []string{"scan-0"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
		{
			ID:           "shuffle-3",
			Type:         StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"o_orderkey"}, Count: 16},
			Dependencies: []string{"scan-1"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"o_orderkey"}, Count: 16},
		},
		{
			ID: "join-4", Type: "hash_join",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "shuffle-2", RightDepStage: "shuffle-3",
			Dependencies: []string{"shuffle-2", "shuffle-3"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
	}

	if err := AssertExchangeConsistency(stages); err != nil {
		t.Fatalf("expected no error on consistent plan, got: %v", err)
	}
}

func TestAssertExchangeConsistency_BrokenPlan_StrictMode(t *testing.T) {
	// Save and restore the package var.
	prev := BehaviorPreservingMode
	BehaviorPreservingMode = false
	defer func() { BehaviorPreservingMode = prev }()

	// join requires its build slot clustered on o_orderkey, but the build
	// dependency is hash-partitioned on c_custkey — violation.
	stages := []Stage{
		{ID: "scan-0", Type: "scan", Distribution: Distribution{Kind: DistSingleton}},
		{
			ID:           "shuffle-1",
			Type:         StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"l_orderkey"}, Count: 16},
			Dependencies: []string{"scan-0"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
		{ID: "scan-2", Type: "scan", Distribution: Distribution{Kind: DistSingleton}},
		{
			ID:           "shuffle-3",
			Type:         StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"c_custkey"}, Count: 16},
			Dependencies: []string{"scan-2"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"c_custkey"}, Count: 16},
		},
		{
			ID: "join-4", Type: "hash_join",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "shuffle-1", RightDepStage: "shuffle-3",
			Dependencies: []string{"shuffle-1", "shuffle-3"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
	}

	err := AssertExchangeConsistency(stages)
	if err == nil {
		t.Fatal("expected error on broken plan in strict mode, got nil")
	}
	if !strings.Contains(err.Error(), "join-4") {
		t.Errorf("error should mention violating consumer stage join-4, got: %v", err)
	}
	if !strings.Contains(err.Error(), "shuffle-3") {
		t.Errorf("error should mention violating producer stage shuffle-3, got: %v", err)
	}
}

func TestAssertExchangeConsistency_BrokenPlan_BehaviorPreservingMode(t *testing.T) {
	prev := BehaviorPreservingMode
	BehaviorPreservingMode = true
	defer func() { BehaviorPreservingMode = prev }()

	// Same broken plan as the strict-mode test.
	stages := []Stage{
		{ID: "scan-0", Type: "scan", Distribution: Distribution{Kind: DistSingleton}},
		{
			ID:           "shuffle-1",
			Type:         StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"l_orderkey"}, Count: 16},
			Dependencies: []string{"scan-0"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
		{ID: "scan-2", Type: "scan", Distribution: Distribution{Kind: DistSingleton}},
		{
			ID:           "shuffle-3",
			Type:         StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"c_custkey"}, Count: 16},
			Dependencies: []string{"scan-2"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"c_custkey"}, Count: 16},
		},
		{
			ID: "join-4", Type: "hash_join",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "shuffle-1", RightDepStage: "shuffle-3",
			Dependencies: []string{"shuffle-1", "shuffle-3"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
	}

	// In BehaviorPreservingMode, AssertExchangeConsistency returns nil —
	// the violation is logged at WARN but does not bubble up as an error.
	if err := AssertExchangeConsistency(stages); err != nil {
		t.Fatalf("expected nil in BehaviorPreservingMode (warn-only), got: %v", err)
	}
}

func TestPlanDistributed_PopulatesStageDistribution(t *testing.T) {
	// Confirm PlanDistributed populates Stage.Distribution on every stage
	// it emits. Uses the TPC-H test catalog from plan_tpch_test.go (same
	// package) and a synthetic 2-table join.
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT l_orderkey, o_orderdate
		FROM lineitem JOIN orders ON l_orderkey = o_orderkey
		WHERE o_orderdate >= '1995-01-01'`

	stages := sqlToStages(t, cat, ctx, sql, 4)

	for _, s := range stages {
		// Every stage must have a populated Distribution. The zero value
		// is {Kind: DistSingleton, Keys: nil, Count: 0} which is a valid
		// "populated" value for stages that emit singleton output. We
		// detect "unpopulated" by checking that the wire-up actually ran
		// — for shuffle stages, the non-zero Count is a reliable proof.
		if s.Type == StageExchangeRepartition {
			if s.Distribution.Kind != DistHashPartitioned {
				t.Errorf("exchange-repartition stage %s: Distribution.Kind = %v, want DistHashPartitioned", s.ID, s.Distribution.Kind)
			}
			if s.Distribution.Count == 0 {
				t.Errorf("exchange-repartition stage %s: Distribution.Count = 0 (assignStageDistributions not wired in)", s.ID)
			}
			if len(s.Distribution.Keys) == 0 {
				t.Errorf("exchange-repartition stage %s: Distribution.Keys empty (assignStageDistributions not wired in)", s.ID)
			}
		}
	}
}
