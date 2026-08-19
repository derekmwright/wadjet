package physical

import "testing"

// TestAggNeedsWholeInput_Classification pins which side of the line each
// aggregate falls on. The list is not a taste question: an aggregate is
// gated exactly when its own output is not a valid input to itself and it
// has no bounded summary that merges.
func TestAggNeedsWholeInput_Classification(t *testing.T) {
	for _, fn := range []string{
		"median", "percentile_cont", "percentile_disc",
		"quantile_cont", "quantile_disc",
		"mode", "min_by", "max_by", "string_agg",
		"MEDIAN", " Min_By ",
	} {
		if !aggNeedsWholeInput(fn) {
			t.Errorf("%q is not gated, so it would run partial-then-merge and answer from partials", fn)
		}
	}
	for _, fn := range []string{
		// Self-mergeable.
		"sum", "count", "min", "max",
		// Decomposed instead, into a state that merges exactly.
		"avg", "stddev", "variance", "stddev_pop", "var_pop",
		"corr", "covar_samp", "covar_pop",
		// Already gated by their own rule (hasDistinctAgg).
		"count_distinct",
	} {
		if aggNeedsWholeInput(fn) {
			t.Errorf("%q is gated, which costs it its partial-aggregate reduction for nothing", fn)
		}
	}
}

// TestAggWholeInputGate_RawInputAggregate is the gate engaging, not just
// the classifier agreeing: a plan containing one of these aggregates must
// emit a RawInputAggregate final_aggregate — one task per group, every row
// aggregated exactly once — and must NOT emit the partial "aggregate" /
// "merge_aggregate" pair, whose merge step re-aggregates finished values.
func TestAggWholeInputGate_RawInputAggregate(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"median", "SELECT MEDIAN(o_totalprice) AS m FROM orders"},
		{"median grouped", "SELECT o_orderstatus AS k, MEDIAN(o_totalprice) AS m FROM orders GROUP BY o_orderstatus"},
		{"percentile", "SELECT PERCENTILE_CONT(0.9, o_totalprice) AS p FROM orders"},
		{"quantile", "SELECT quantile_disc(o_totalprice, 0.9) AS p FROM orders"},
		{"mode", "SELECT MODE(o_custkey) AS m FROM orders"},
		{"min_by", "SELECT MIN_BY(o_orderpriority, o_totalprice) AS m FROM orders"},
		{"max_by grouped", "SELECT o_orderstatus AS k, MAX_BY(o_orderpriority, o_totalprice) AS m FROM orders GROUP BY o_orderstatus"},
		{"string_agg", "SELECT STRING_AGG(o_orderpriority, '::') AS s FROM orders"},
		// One gated aggregate in the list is enough: the others ride the
		// same stage, so the whole aggregate has to take the gated route.
		{"mixed with sum", "SELECT SUM(o_totalprice) AS s, MEDIAN(o_totalprice) AS m FROM orders"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tc.sql, 3)
			raw := 0
			for _, s := range stages {
				switch s.Type {
				case "aggregate", "merge_aggregate":
					t.Errorf("stage %s is a %s: the aggregate was split into partials, "+
						"whose merge re-aggregates finished values", s.ID, s.Type)
				case "final_aggregate":
					if !s.RawInputAggregate {
						t.Errorf("stage %s is a final_aggregate that is not RawInputAggregate, "+
							"so its input is partial answers rather than raw rows", s.ID)
						continue
					}
					raw++
				}
				// The fused scan-aggregate path is the other way a partial
				// could be produced, and it must not be taken either.
				if len(s.FusedAggSpecs) > 0 {
					t.Errorf("stage %s carries fused partial aggregates: %+v", s.ID, s.FusedAggSpecs)
				}
			}
			if raw != 1 {
				t.Errorf("%d RawInputAggregate finals, want exactly 1", raw)
			}
		})
	}
}

// TestAggWholeInputGate_UngatedStaysSplit is the control: the gate must not
// swallow the aggregates that DO decompose, or every SUM in the engine
// loses its partial-aggregate reduction. SUM and CORR both keep the
// two-phase shape (CORR through its state decomposition, applied later at
// dispatch).
func TestAggWholeInputGate_UngatedStaysSplit(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	for _, tc := range []struct{ name, sql string }{
		{"sum", "SELECT o_orderstatus AS k, SUM(o_totalprice) AS s FROM orders GROUP BY o_orderstatus"},
		{"corr", "SELECT o_orderstatus AS k, CORR(o_totalprice, o_custkey) AS c FROM orders GROUP BY o_orderstatus"},
		{"stddev", "SELECT o_orderstatus AS k, STDDEV(o_totalprice) AS s FROM orders GROUP BY o_orderstatus"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tc.sql, 3)
			partial := false
			for _, s := range stages {
				if s.Type == "aggregate" || s.Type == "merge_aggregate" || len(s.FusedAggSpecs) > 0 {
					partial = true
				}
				if s.Type == "final_aggregate" && s.RawInputAggregate {
					t.Errorf("stage %s took the single-task gate; %s decomposes and must not", s.ID, tc.name)
				}
			}
			if !partial {
				t.Errorf("%s produced no partial aggregate stage at all", tc.name)
			}
		})
	}
}
