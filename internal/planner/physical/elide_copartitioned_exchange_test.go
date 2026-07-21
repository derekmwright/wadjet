package physical

import (
	"testing"
)

func elideFixture() []Stage {
	return []Stage{
		{ID: "join-1", Type: StageHashJoin,
			JoinLeftKeys: []string{"o_orderkey"}, JoinRightKeys: []string{"l_orderkey"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"o_orderkey"}, Count: 24}},
		{ID: "ex-2", Type: StageExchangeRepartition, Dependencies: []string{"join-1"},
			Exchange:     &ExchangeStage{Keys: []string{"o_orderkey"}, Count: 24},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"o_orderkey"}, Count: 24}},
		{ID: "join-3", Type: StageHashJoin, Dependencies: []string{"ex-2", "other"},
			LeftDepStage: "ex-2", RightDepStage: "other",
			JoinLeftKeys: []string{"o_orderkey"}, JoinRightKeys: []string{"l_orderkey"}},
	}
}

func TestElideCoPartitionedExchange(t *testing.T) {
	out := elideCoPartitionedExchanges(elideFixture())
	if len(out) != 2 {
		t.Fatalf("expected identity exchange elided, got %d stages: %+v", len(out), stageIDs(out))
	}
	var consumer *Stage
	for i := range out {
		if out[i].ID == "join-3" {
			consumer = &out[i]
		}
		if out[i].ID == "ex-2" {
			t.Fatal("ex-2 survived elision")
		}
	}
	if consumer == nil {
		t.Fatal("consumer join-3 missing")
	}
	if consumer.Dependencies[0] != "join-1" || consumer.LeftDepStage != "join-1" {
		t.Fatalf("consumer not rewired: deps=%v leftDep=%q", consumer.Dependencies, consumer.LeftDepStage)
	}
}

func TestElideCoPartitionedExchangeNegatives(t *testing.T) {
	mutate := map[string]func(s []Stage){
		"count mismatch":   func(s []Stage) { s[1].Exchange.Count = 12 },
		"key mismatch":     func(s []Stage) { s[1].Exchange.Keys = []string{"l_orderkey"} }, // cross-name equivalence is B2, must NOT elide
		"not partitioned":  func(s []Stage) { s[0].Distribution = Distribution{Kind: DistSingleton} },
		"cluster mismatch": func(s []Stage) { s[1].ClusterID = "edge-7" },
		"multi dep":        func(s []Stage) { s[1].Dependencies = []string{"join-1", "other"} },
	}
	for name, mut := range mutate {
		t.Run(name, func(t *testing.T) {
			fix := elideFixture()
			mut(fix)
			if out := elideCoPartitionedExchanges(fix); len(out) != 3 {
				t.Fatalf("%s: exchange should have been kept, got %v", name, stageIDs(out))
			}
		})
	}
}

func TestElideKillSwitch(t *testing.T) {
	old := exchangeElide
	exchangeElide = false
	defer func() { exchangeElide = old }()
	if out := elideCoPartitionedExchanges(elideFixture()); len(out) != 3 {
		t.Fatalf("kill switch off must keep all stages, got %v", stageIDs(out))
	}
}

// TestQ18IdentityExchangeElided is the plan-level regression test for
// exchange-reuse.md §2 B: no exchange-repartition may survive whose sole
// dependency already carries the identical hash distribution.
func TestQ18IdentityExchangeElided(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	q18 := `SELECT
			c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
			SUM(l_quantity) as total_qty
		FROM customer
		JOIN orders ON c_custkey = o_custkey
		JOIN lineitem ON o_orderkey = l_orderkey
		WHERE o_orderkey IN (
			SELECT l_orderkey
			FROM lineitem
			GROUP BY l_orderkey
			HAVING SUM(l_quantity) > 300
		)
		GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
		ORDER BY o_totalprice DESC, o_orderdate
		LIMIT 100`
	stages := sqlToStages(t, cat, ctx, q18, 3)
	byID := map[string]Stage{}
	for _, s := range stages {
		byID[s.ID] = s
	}
	joinFedExchange := false
	for _, s := range stages {
		if s.Type != StageExchangeRepartition || s.Exchange == nil || len(s.Dependencies) != 1 {
			continue
		}
		d, ok := byID[s.Dependencies[0]]
		if !ok {
			continue
		}
		if d.Type == StageHashJoin {
			joinFedExchange = true
		}
		if d.Distribution.Kind == DistHashPartitioned &&
			d.Distribution.Count == s.Exchange.Count &&
			equalKeyLists(d.Distribution.Keys, s.Exchange.Keys) {
			t.Errorf("identity re-shuffle survived: %s over %s (keys=%v count=%d)",
				s.ID, d.ID, s.Exchange.Keys, s.Exchange.Count)
		}
	}
	_ = joinFedExchange // informational only; the assert above is the contract
}

func equalKeyLists(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equalFoldStr(a[i], b[i]) {
			return false
		}
	}
	return true
}

func equalFoldStr(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 32
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 32
		}
		if ca != cb {
			return false
		}
	}
	return true
}

func stageIDs(stages []Stage) []string {
	ids := make([]string, len(stages))
	for i, s := range stages {
		ids[i] = s.ID
	}
	return ids
}
