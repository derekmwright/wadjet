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
