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
