package physical

import (
	"os"
	"strings"
	"sync/atomic"
)

// ExchangePartialAggMarked counts exchanges marked for sender-side
// partial aggregation at plan time (observability parity with
// ElidedCoPartitionedExchanges).
var ExchangePartialAggMarked atomic.Int64

// exchangePartialAggEnabled gates markExchangePartialAgg. Kill switch
// WADJET_EXCHANGE_PARTIAL_AGG=0.
var exchangePartialAggEnabled = os.Getenv("WADJET_EXCHANGE_PARTIAL_AGG") != "0"

// SetExchangePartialAggEnabled toggles the pass (A/B tests run both arms
// in one process, where the env-var gate is already latched). Returns the
// previous value so callers can restore it.
func SetExchangePartialAggEnabled(on bool) bool {
	prev := exchangePartialAggEnabled
	exchangePartialAggEnabled = on
	return prev
}

// markExchangePartialAgg marks exchange-repartition stages whose rows can
// be pre-combined in the shuffle sender (exchange partial aggregation —
// the reduce-before-ship mechanism; SF100 Q18's rp leg ships the full
// 600M-row lineitem as raw (l_orderkey, l_quantity) pairs into a grouped
// final_aggregate AND a join probe, ~9.75 GB read twice).
//
// The marked exchange ships name-preserving partials: each payload column
// is either kept as a partial group key or replaced by a self-mergeable
// partial aggregate (SUM/MIN/MAX) written under its own name. Merged rows
// are therefore indistinguishable from raw rows on every non-aggregated
// column, which makes the reduction invisible to any downstream grouping;
// the only consumers that could observe it are ones sensitive to row
// multiplicity or to per-row values of the aggregated columns. Eligibility
// enforces exactly that:
//
//  1. At least one consumer is a grouped final/merge_aggregate, and EVERY
//     aggregate spec any such consumer computes over the payload is a
//     bare-column SUM/MIN/MAX — one function per column across all
//     consumers (COUNT/AVG/DISTINCT/expressions are row-multiplicity- or
//     shape-sensitive and disqualify the exchange).
//  2. Every other consumer is a hash/sort-merge join whose E-side keys
//     avoid aggregated columns, whose filters and chained/fused join keys
//     never reference an aggregated column, and whose dependents are all
//     grouped aggregates that themselves only re-aggregate the covered
//     columns with the same functions (row-count changes pass through a
//     join, so a downstream COUNT would double-count-proof fail).
//  3. Exchange payload is exactly declared (no ComputedCols /
//     ExtraReadCols machinery), and partition keys stay group keys.
//
// Runs after every stage-rewiring pass (fusion, elision, agg-over-
// exchange) so the consumer set is final. Kill switch
// WADJET_EXCHANGE_PARTIAL_AGG=0.
func markExchangePartialAgg(stages []Stage) {
	if !exchangePartialAggEnabled {
		return
	}
	byID := make(map[string]*Stage, len(stages))
	for i := range stages {
		byID[stages[i].ID] = &stages[i]
	}
	// Reverse edges: stage ID → consumer stage IDs (regular + join slots).
	// Scalar dependencies count as consumers too — a scalar-consumed
	// exchange output is read as a value, which pre-combination may change
	// in row count; treat as incompatible below by consumer-type check.
	consumersOf := make(map[string][]*Stage, len(stages))
	addEdge := func(dep string, s *Stage) {
		if dep != "" {
			consumersOf[dep] = append(consumersOf[dep], s)
		}
	}
	for i := range stages {
		s := &stages[i]
		seen := map[string]bool{}
		for _, d := range s.Dependencies {
			if !seen[d] {
				seen[d] = true
				addEdge(d, s)
			}
		}
		if s.LeftDepStage != "" && !seen[s.LeftDepStage] {
			addEdge(s.LeftDepStage, s)
		}
		if s.RightDepStage != "" && !seen[s.RightDepStage] {
			addEdge(s.RightDepStage, s)
		}
		for _, d := range s.ScalarDependencies {
			addEdge(d, s)
		}
	}

	for i := range stages {
		e := &stages[i]
		if e.Type != StageExchangeRepartition || e.Exchange == nil {
			continue
		}
		if len(e.Exchange.Keys) == 0 || len(e.Columns) == 0 ||
			len(e.Exchange.ComputedCols) > 0 || len(e.Exchange.ExtraReadCols) > 0 {
			continue
		}
		payload := map[string]bool{}
		var payloadOrder []string
		for _, c := range e.Columns {
			if !payload[c] {
				payload[c] = true
				payloadOrder = append(payloadOrder, c)
			}
		}
		for _, k := range e.Exchange.Keys {
			if !payload[k] {
				payload[k] = true
				payloadOrder = append(payloadOrder, k)
			}
		}
		consumers := consumersOf[e.ID]
		if len(consumers) == 0 {
			continue
		}

		// Pass 1: collect the (func, col) coverage from grouped-aggregate
		// consumers. Any incompatible spec over the payload disqualifies.
		aggFunc := map[string]string{} // payload col → sum|min|max
		eligible := true
		sawGroupedAgg := false
		collectSpecs := func(specs []AggSpec) bool {
			for _, sp := range specs {
				if !payload[sp.InputCol] && sp.InputExpr == "" {
					continue // aggregate over another input's column
				}
				fn := strings.ToLower(strings.TrimSpace(sp.Func))
				if sp.InputExpr != "" || (fn != "sum" && fn != "min" && fn != "max") {
					return false
				}
				if prev, ok := aggFunc[sp.InputCol]; ok && prev != fn {
					return false
				}
				aggFunc[sp.InputCol] = fn
			}
			return true
		}
		for _, c := range consumers {
			if isGroupedFinalAggregate(c) {
				sawGroupedAgg = true
				if !collectSpecs(c.AggSpecs) {
					eligible = false
					break
				}
			}
		}
		if !eligible || !sawGroupedAgg || len(aggFunc) == 0 {
			continue
		}
		// Partition keys must remain group keys.
		for _, k := range e.Exchange.Keys {
			if _, isAgg := aggFunc[k]; isAgg {
				eligible = false
				break
			}
		}
		if !eligible {
			continue
		}

		// Pass 2: every consumer must be provably indifferent.
		for _, c := range consumers {
			if !eligible {
				break
			}
			switch {
			case isGroupedFinalAggregate(c):
				// Specs validated in pass 1; a column both grouped and
				// aggregated would be self-contradictory — reject.
				for _, g := range c.GroupByCols {
					if _, isAgg := aggFunc[g]; isAgg {
						eligible = false
					}
				}
			case c.Type == StageHashJoin || c.Type == StageSortMergeJoin:
				eligible = joinConsumerCompatible(c, e.ID, aggFunc, byID, consumersOf, collectSpecs)
			default:
				eligible = false
			}
		}
		if !eligible {
			continue
		}

		var groupBy []string
		var specs []AggSpec
		for _, col := range payloadOrder {
			if fn, isAgg := aggFunc[col]; isAgg {
				specs = append(specs, AggSpec{Func: fn, InputCol: col, OutputCol: col})
			} else {
				groupBy = append(groupBy, col)
			}
		}
		e.Exchange.PartialAggGroupBy = groupBy
		e.Exchange.PartialAggSpecs = specs
		ExchangePartialAggMarked.Add(1)
	}
}

// isGroupedFinalAggregate reports whether the stage merges grouped
// aggregates (the consumer shape whose specs drive coverage).
func isGroupedFinalAggregate(s *Stage) bool {
	return (s.Type == "final_aggregate" || s.Type == "merge_aggregate") &&
		len(s.GroupByCols) > 0
}

// joinConsumerCompatible checks a hash/sort-merge join consuming the
// exchange on either side: its E-side keys, filters, and chained/fused
// join keys must avoid the aggregated columns, and every dependent must
// be a grouped aggregate whose specs only re-merge the covered columns.
func joinConsumerCompatible(
	j *Stage,
	exchangeID string,
	aggFunc map[string]string,
	byID map[string]*Stage,
	consumersOf map[string][]*Stage,
	collectSpecs func([]AggSpec) bool,
) bool {
	touchesAgg := func(cols []string) bool {
		for _, c := range cols {
			if _, ok := aggFunc[c]; ok {
				return true
			}
		}
		return false
	}
	refsAgg := func(exprs ...string) bool {
		for _, ex := range exprs {
			if ex == "" {
				continue
			}
			for col := range aggFunc {
				if strings.Contains(ex, col) {
					return true
				}
			}
		}
		return false
	}
	if j.LeftDepStage == exchangeID && touchesAgg(j.JoinLeftKeys) {
		return false
	}
	if j.RightDepStage == exchangeID && touchesAgg(j.JoinRightKeys) {
		return false
	}
	if refsAgg(j.JoinFilter) || refsAgg(j.FilterExprs...) || refsAgg(j.BuildFilterExprs...) {
		return false
	}
	for _, cj := range j.ChainedJoins {
		if touchesAgg(cj.JoinLeftKeys) || touchesAgg(cj.JoinRightKeys) ||
			refsAgg(cj.JoinFilter) || refsAgg(cj.FilterExprs...) || refsAgg(cj.BuildFilterExprs...) {
			return false
		}
	}
	for _, fj := range j.FusedJoins {
		if touchesAgg(fj.JoinLeftKeys) || touchesAgg(fj.JoinRightKeys) ||
			refsAgg(fj.JoinFilter) || refsAgg(fj.FilterExprs...) {
			return false
		}
	}
	deps := consumersOf[j.ID]
	if len(deps) == 0 {
		return false // join output goes straight to gather — row-shaped
	}
	for _, d := range deps {
		if !isGroupedFinalAggregate(d) {
			return false
		}
		// Every spec the dependent computes must be a re-merge of covered
		// columns: joins pass row-count changes through, so an uncovered
		// aggregate (COUNT(*), SUM over another table's column) would see
		// different multiplicity.
		for _, sp := range d.AggSpecs {
			fn := strings.ToLower(strings.TrimSpace(sp.Func))
			if sp.InputExpr != "" || aggFunc[sp.InputCol] != fn {
				return false
			}
		}
		if !collectSpecs(d.AggSpecs) {
			return false
		}
	}
	return true
}
