package physical

import "testing"

func TestStageTypeConstants(t *testing.T) {
	cases := []struct{ got, want string }{
		{StageScan, "scan"},
		{StageAggregate, "aggregate"},
		{StageSort, "sort"},
		{StageHashJoin, "hash_join"},
		{StageBroadcastJoin, "broadcast_join"},
		{StageWindow, "window"},
		{StagePipeline, "pipeline"},
		{StageExchangeRepartition, "exchange-repartition"},
		{StageExchangeReplicate, "exchange-replicate"},
		{StageExchangeGather, "exchange-gather"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("got %q want %q", c.got, c.want)
		}
	}
}
