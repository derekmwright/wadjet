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
