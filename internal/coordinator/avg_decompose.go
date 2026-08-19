package coordinator

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// avgSumPrefix and avgCountPrefix name the two synthetic columns that
// decomposeAvg emits in place of an AVG aggregate. The "#" separator is
// not legal in SQL identifiers, so user-written column names cannot
// collide with the synthetic ones.
const (
	avgSumPrefix   = "__avg_sum#"
	avgCountPrefix = "__avg_count#"
)

// decomposeAvg expands every AVG(c) AS out spec into a (SUM, COUNT) pair:
//
//	SUM(c)   AS __avg_sum#out
//	COUNT(c) AS __avg_count#out
//
// Non-AVG specs are passed through unchanged. This rewrite makes the
// scan-aggregate fan-out + final_aggregate merge correct for AVG without
// the single-task fallback, because SUM and COUNT both decompose across
// partials. The worker's executeStageAggregate detects the synthetic
// columns in merge mode and folds them back into the original AVG output
// (avg_fold.go in package worker).
//
// Returns the original slice unchanged when no AVG is present, so the
// non-AVG hot path pays no allocation.
func decomposeAvg(specs []distributed.AggSpec) []distributed.AggSpec {
	hasAvg := false
	for _, a := range specs {
		if isAvgFunc(a.Func) {
			hasAvg = true
			break
		}
	}
	if !hasAvg {
		return specs
	}
	out := make([]distributed.AggSpec, 0, len(specs)+1)
	for _, a := range specs {
		if !isAvgFunc(a.Func) {
			out = append(out, a)
			continue
		}
		// AVG(c) AS out → SUM(c) AS __avg_sum#out + COUNT(c) AS __avg_count#out.
		// Both legs share the same InputCol/InputExpr — the underlying
		// column or derived expression is what gets summed and counted.
		sumSpec := a
		sumSpec.Func = "sum"
		sumSpec.OutputCol = avgSumPrefix + a.OutputCol
		sumSpec.OutputType = int(parquet.TypeFloat64)
		countSpec := a
		countSpec.Func = "count"
		countSpec.OutputCol = avgCountPrefix + a.OutputCol
		// The count leg is int64 whatever AVG's own type is, and
		// applyAvgFold reads it straight out of Int64Data — inheriting
		// AVG's float64 declaration here would hand the fold the wrong
		// vector.
		countSpec.OutputType = int(parquet.TypeInt64)
		out = append(out, sumSpec, countSpec)
	}
	return out
}

// decomposeAvgPhysical is the physical.AggSpec variant for the final_aggregate
// stage's stage.AggSpecs (which uses physical.AggSpec, not distributed.AggSpec
// — the dispatcher converts to wire format separately).
func decomposeAvgPhysical(specs []physical.AggSpec) []physical.AggSpec {
	hasAvg := false
	for _, a := range specs {
		if isAvgFunc(a.Func) {
			hasAvg = true
			break
		}
	}
	if !hasAvg {
		return specs
	}
	out := make([]physical.AggSpec, 0, len(specs)+1)
	for _, a := range specs {
		if !isAvgFunc(a.Func) {
			out = append(out, a)
			continue
		}
		sumSpec := a
		sumSpec.Func = "sum"
		sumSpec.OutputCol = avgSumPrefix + a.OutputCol
		sumSpec.OutputType = parquet.TypeFloat64
		countSpec := a
		countSpec.Func = "count"
		countSpec.OutputCol = avgCountPrefix + a.OutputCol
		countSpec.OutputType = parquet.TypeInt64
		out = append(out, sumSpec, countSpec)
	}
	return out
}

func isAvgFunc(s string) bool {
	return strings.EqualFold(strings.TrimSpace(s), "avg")
}
