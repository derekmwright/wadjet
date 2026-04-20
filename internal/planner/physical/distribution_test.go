package physical

import "testing"

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
			name:  "shuffle accepts any input",
			stage: Stage{ID: "shuffle-0", Type: "shuffle", ShuffleKeys: []string{"k"}, NumPartitions: 16},
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
			name: "broadcast_join build slot requires any (Phase 1)",
			stage: Stage{
				ID: "join-0", Type: "broadcast_join",
				JoinLeftKeys: []string{"l_partkey"}, JoinRightKeys: []string{"p_partkey"},
				LeftDepStage: "scan-l", RightDepStage: "scan-r",
				Dependencies: []string{"scan-l", "scan-r"},
			},
			slot: 1,
			want: RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "aggregate requires any (Phase 1 conservative — see Risk #1)",
			stage: Stage{ID: "aggregate-0", Type: "aggregate", GroupByCols: []string{"l_returnflag"}},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "final_aggregate requires any",
			stage: Stage{ID: "final_aggregate-0", Type: "final_aggregate", GroupByCols: []string{"l_returnflag"}},
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
