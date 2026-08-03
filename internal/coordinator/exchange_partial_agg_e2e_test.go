package coordinator

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// TestQ18ExchangePartialAggEngagesAndMatches proves the exchange partial
// aggregation path END TO END: with broadcast forced off (SF100's Q18
// runs hash joins over exchange-repartition legs; SF0.01 needs the
// override to reproduce that shape), Q18 must (a) plan at least one
// partial-agg-marked exchange, and (b) return exactly the same rows as
// the same plan with the mark disabled. This is the regression fence for
// the mechanism itself — planner-only tests can't catch a worker-side
// merge bug, and the golden suites can't tell whether the path engaged.
func TestQ18ExchangePartialAggEngagesAndMatches(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed e2e — skipped in -short")
	}
	// AtScale rather than setupTPCHDistributed: the -race build swaps in
	// the stub (nats-server upstream race), which only declares the
	// AtScale symbol and skips at runtime.
	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF001)
	coord.config.BroadcastBytesOverride = 1 // force hash-join + exchange shapes

	canon := func(rows []map[string]any) []string {
		out := make([]string, 0, len(rows))
		for _, r := range rows {
			keys := make([]string, 0, len(r))
			for k := range r {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			s := ""
			for _, k := range keys {
				s += fmt.Sprintf("%s=%v;", k, r[k])
			}
			out = append(out, s)
		}
		sort.Strings(out)
		return out
	}

	// SF0.01 has no order with SUM(l_quantity) > 300, which would make the
	// value comparison vacuous (0 rows == 0 rows proves nothing about the
	// merge). Lower the HAVING threshold so real groups survive and flow
	// through the marked exchange into both consumers.
	q18SQL := strings.Replace(tpch.TPCHQueries[18].SQL, "> 300", "> 250", 1)
	if q18SQL == tpch.TPCHQueries[18].SQL {
		t.Fatal("Q18 SQL no longer contains the '> 300' HAVING threshold — update the test")
	}

	run := func() []string {
		res, err := coord.ExecuteSQL(ctx, q18SQL)
		if err != nil {
			t.Fatalf("Q18: %v", err)
		}
		return canon(mustRows(t, res))
	}

	before := physical.ExchangePartialAggMarked.Load()
	withAgg := run()
	marked := physical.ExchangePartialAggMarked.Load() - before
	if marked == 0 {
		t.Fatalf("exchange partial agg did not engage on Q18 under BroadcastBytesOverride=1 — plan shape drifted, the mechanism is untested")
	}
	if len(withAgg) == 0 {
		t.Fatal("lowered-threshold Q18 returned 0 rows — value comparison is vacuous, lower the threshold further")
	}
	t.Logf("Q18 marked %d exchange(s) for partial agg, %d rows", marked, len(withAgg))

	origEnabled := physical.SetExchangePartialAggEnabled(false)
	defer physical.SetExchangePartialAggEnabled(origEnabled)
	withoutAgg := run()

	if len(withAgg) != len(withoutAgg) {
		t.Fatalf("row count drift: %d with partial agg, %d without", len(withAgg), len(withoutAgg))
	}
	for i := range withAgg {
		if withAgg[i] != withoutAgg[i] {
			t.Fatalf("row %d drift:\n  with:    %s\n  without: %s", i, withAgg[i], withoutAgg[i])
		}
	}
}
