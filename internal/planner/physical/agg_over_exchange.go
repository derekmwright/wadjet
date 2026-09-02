package physical

import (
	"os"
	"strings"
	"sync/atomic"
)

// AggOverExchange gates rewireAggOverRawExchange. Kill switch
// WADJET_AGG_OVER_EXCHANGE=0. Exported atomic.Bool (ExchangeSubsume
// pattern) so tests can pin either arm.
var AggOverExchange atomic.Bool

func init() {
	AggOverExchange.Store(os.Getenv("WADJET_AGG_OVER_EXCHANGE") != "0")
}

// rewireAggOverRawExchange drops a fused scan-aggregate leg whose input is a
// full re-scan of a table some raw sibling exchange already shuffles on the
// aggregate's exact group keys, and feeds the aggregate's final directly
// from the sibling's partition files.
//
// The Q18 shape that motivates it: join-8's build side shuffles RAW lineitem
// (l_orderkey, l_quantity) by Hash(l_orderkey); the HAVING subquery scans
// lineitem AGAIN as a fused scan-agg (partial SUM(l_quantity) GROUP BY
// l_orderkey) whose partials a final_aggregate merges. Because the raw
// exchange hash-partitions on the group key, each of its partitions holds
// every row of the keys it covers — so a RAW (non-merge) aggregate over one
// partition produces exact groups with no cross-partition merge. The second
// full table scan (plus its partial-agg shuffle) buys nothing.
//
// Rewrite: the final_aggregate consumes the raw exchange's partitions, its
// AggSpecs switch from merge form to the fused scan's raw specs
// (Stage.RawInputAggregate makes the dispatcher build the fragment with
// MergeMode=false), and the fused scan + its exchange are dropped. The
// final's output distribution mirrors the raw exchange, which typically
// turns its downstream re-shuffle into an identity exchange that
// elideCoPartitionedExchanges removes (the caller re-runs it).
//
// HAVING is unaffected: it rides Stage.FilterExprs → PostFilterExprs and
// runs after the (now raw) aggregate exactly as it ran after the merge.
//
// Runs BEFORE fuseScanAggregateShuffle, so the fused scan-agg leg is still
// scan(FusedAgg*) → exchange-repartition → final_aggregate.
//
// Conservative eligibility (v1):
//   - F is a grouped final_aggregate: single dep, no SortKeys/Limit, no
//     merge-tree grouping, not GroupByAll.
//   - F's dep is an exchange-repartition B whose sole consumer is F and
//     whose sole dep is a scan-aggregate (FusedAggGroupBy set) of table T
//     with no filters and no security barrier.
//   - A is another exchange-repartition over a RAW scan of T (no filters,
//     no fused agg, no barrier), with Exchange.Keys exactly equal to F's
//     GroupByCols — exactness of per-partition groups needs the group keys
//     to determine the partition, and the exact-name match keeps
//     OutputDistribution's mirror rule and Satisfies() consistent.
//   - Aggregate functions are SUM/COUNT/MIN/MAX on bare columns (no AVG
//     synthetics, no derived input expressions), and every group key and
//     aggregate input is in A's scan payload.
func rewireAggOverRawExchange(stages []Stage) []Stage {
	if !AggOverExchange.Load() {
		return stages
	}
	idIndex := make(map[string]int, len(stages))
	consumers := make(map[string][]int) // stage ID -> indexes of stages depending on it
	for i := range stages {
		idIndex[stages[i].ID] = i
		for _, d := range stages[i].Dependencies {
			consumers[d] = append(consumers[d], i)
		}
	}
	scanOf := func(s *Stage) *Stage {
		if s.Type != StageExchangeRepartition || s.Exchange == nil || len(s.Dependencies) != 1 {
			return nil
		}
		idx, ok := idIndex[s.Dependencies[0]]
		if !ok || stages[idx].Type != StageScan {
			return nil
		}
		return &stages[idx]
	}

	dropped := make(map[string]bool) // dropped stage IDs (B exchange + its scan)
	for fi := range stages {
		fa := &stages[fi]
		if fa.Type != "final_aggregate" || len(fa.Dependencies) != 1 ||
			len(fa.GroupByCols) == 0 || fa.GroupByAll ||
			len(fa.SortKeys) > 0 || fa.HasLimit || fa.MergeGroupCount > 0 {
			continue
		}
		bIdx, ok := idIndex[fa.Dependencies[0]]
		if !ok || dropped[stages[bIdx].ID] {
			continue
		}
		b := &stages[bIdx]
		scanB := scanOf(b)
		if scanB == nil || len(consumers[b.ID]) != 1 || len(consumers[scanB.ID]) != 1 {
			continue
		}
		if len(b.Exchange.ComputedCols) > 0 {
			continue
		}
		if len(scanB.FusedAggGroupBy) == 0 || len(scanB.FusedAggSpecs) == 0 ||
			len(scanB.FilterExprs) > 0 || len(scanB.SecurityProjectExprs) > 0 ||
			len(scanB.PartitionFilter) > 0 || scanB.Exchange != nil {
			continue
		}
		if !keysEqual(fa.GroupByCols, scanB.FusedAggGroupBy) {
			continue
		}
		if !rawAggSpecsCompatible(fa.AggSpecs, scanB.FusedAggSpecs) {
			continue
		}
		for ai := range stages {
			a := &stages[ai]
			if ai == bIdx || dropped[a.ID] {
				continue
			}
			scanA := scanOf(a)
			if scanA == nil || scanA.TableName != scanB.TableName ||
				len(scanA.FilterExprs) > 0 || len(scanA.SecurityProjectExprs) > 0 ||
				len(scanA.PartitionFilter) > 0 ||
				len(scanA.FusedAggGroupBy) > 0 || len(scanA.FusedAggSpecs) > 0 {
				continue
			}
			if a.Exchange.Count <= 0 || !keysEqual(a.Exchange.Keys, fa.GroupByCols) {
				continue
			}
			// Over the RESOLUTION spelling: after the rewrite this final
			// computes its keys from A's RAW rows, and what it evaluates there
			// is the spelling the dropped fused scan resolved them by — the
			// published name may be a derived table's alias, which no base
			// table has (ADR-0026 §2).
			if !aggInputsCovered(resolveExprs(scanB.GroupByResolve), scanB.FusedAggSpecs, scanA.Columns) {
				continue
			}
			// Rewrite: F aggregates A's raw partitions directly. It stops being
			// a merge and becomes a fragment that COMPUTES its keys, so it
			// takes over the dropped scan's resolution list with its specs —
			// without it the worker would resolve every key by its PUBLISHED
			// name against raw table rows (#794).
			fa.AggSpecs = append([]AggSpec(nil), scanB.FusedAggSpecs...)
			fa.GroupByResolve = append([]GroupKeyResolution(nil), scanB.GroupByResolve...)
			fa.RawInputAggregate = true
			fa.Dependencies[0] = a.ID
			fa.Distribution = Distribution{
				Kind:  DistHashPartitioned,
				Keys:  append([]string(nil), a.Exchange.Keys...),
				Count: a.Exchange.Count,
			}
			dropped[b.ID] = true
			dropped[scanB.ID] = true
			break
		}
	}
	if len(dropped) == 0 {
		return stages
	}
	out := make([]Stage, 0, len(stages)-len(dropped))
	for _, s := range stages {
		if !dropped[s.ID] {
			out = append(out, s)
		}
	}
	return out
}

// rawAggSpecsCompatible checks that the final's merge-form specs and the
// fused scan's raw specs describe the same aggregates (positional: same
// function, same output column) and that the raw specs are v1-eligible:
// SUM/COUNT/MIN/MAX over a bare input column. AVG never appears in
// FusedAggSpecs at this shape today (decomposition happens at dispatch),
// but the explicit allowlist keeps any future synthetic out of the raw
// path until it's proven.
func rawAggSpecsCompatible(finalSpecs, rawSpecs []AggSpec) bool {
	if len(finalSpecs) != len(rawSpecs) || len(rawSpecs) == 0 {
		return false
	}
	for i, r := range rawSpecs {
		f := finalSpecs[i]
		if !strings.EqualFold(f.Func, r.Func) || f.OutputCol != r.OutputCol {
			return false
		}
		switch strings.ToLower(r.Func) {
		case "sum", "count", "min", "max":
		default:
			return false
		}
		if r.InputExpr != "" || r.InputCol == "" {
			return false
		}
	}
	return true
}

// aggInputsCovered checks every group key and aggregate input column is
// shipped by the raw exchange's scan payload.
func aggInputsCovered(groupBy []string, specs []AggSpec, payload []string) bool {
	cols := make(map[string]bool, len(payload))
	for _, c := range payload {
		cols[c] = true
	}
	for _, g := range groupBy {
		if !cols[g] {
			return false
		}
	}
	for _, s := range specs {
		if !cols[s.InputCol] {
			return false
		}
	}
	return true
}
