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

// rewireAggOverRawExchange replaces a redundant scan-aggregate leg with a
// raw sibling exchange on exactly the final aggregate's group-key names.
// Run before fuseScanAggregateShuffle. Switch to raw specs/MergeMode=false,
// mirror the raw distribution, re-run identity-exchange elision; keep HAVING.
// Require a grouped, single-dependency final with no sort, limit, merge-tree
// or GroupByAll; its exchange has only this consumer and one scan-agg input.
// Both scans must read the same table without filters or security barriers;
// the sibling is raw. Only bare-column SUM/COUNT/MIN/MAX are eligible,
// with all keys and inputs in its payload; no AVG synthetics or expressions.
// See docs/internals/aggregate-over-raw-exchange.md for the design.
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
