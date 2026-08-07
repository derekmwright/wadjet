package physical

import "testing"

// TestExchangeSubsume_Q21ShapeApplies pins that the dedup fires on the
// EXISTS/NOT-EXISTS lineitem pair (the e2e test's query shape): the plan
// must contain exactly ONE lineitem-scan-fed exchange with a computed flag
// col, and a consumer carrying BuildFilterExprs.
func TestExchangeSubsume_Q21ShapeApplies(t *testing.T) {
	prev := ExchangeSubsume.Load()
	t.Cleanup(func() { ExchangeSubsume.Store(prev) })
	ExchangeSubsume.Store(true)
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT COUNT(*) AS numwait
FROM supplier
JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
WHERE l1.l_receiptdate > l1.l_commitdate
AND EXISTS (SELECT 1 FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey != l1.l_suppkey)
AND NOT EXISTS (SELECT 1 FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)`
	stages := sqlToStages(t, cat, ctx, sql, 3)
	flagged, buildFiltered := 0, 0
	for _, s := range stages {
		if s.Exchange != nil && len(s.Exchange.ComputedCols) > 0 {
			flagged++
		}
		if len(s.BuildFilterExprs) > 0 {
			buildFiltered++
		}
		// Stage-chain fusion (shared-build exception) may absorb the
		// build-filtered anti into the semi; its BuildFilterExprs then
		// ride the chained spec instead of a stage of its own.
		for _, cj := range s.ChainedJoins {
			if len(cj.BuildFilterExprs) > 0 {
				buildFiltered++
			}
		}
	}
	if flagged != 1 || buildFiltered != 1 {
		for _, s := range stages {
			t.Logf("id=%s type=%s deps=%v", s.ID, s.Type, s.Dependencies)
		}
		t.Fatalf("flagged exchanges = %d, build-filtered consumers = %d; want 1 and 1 (dedup did not fire)", flagged, buildFiltered)
	}
}
