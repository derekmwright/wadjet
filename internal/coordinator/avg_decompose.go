package coordinator

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
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
		sumSpec.OutputType = distributed.WindowTypePtr(int(parquet.TypeFloat64))
		sumSpec.OutputPrecision, sumSpec.OutputScale = 0, 0
		// AVG over a DECIMAL sums in DECIMAL, at the INPUT's scale — SUM's own
		// rule (ADR-0024 item 2), not AVG's widened one. Declaring float64
		// here left the identity row of a partial that consumed NO rows
		// shipping a FLOAT64 column where every other partial of the same
		// stage shipped a DECIMAL: a header that contradicts its siblings, and
		// a merge that resolved its accumulator against that batch first would
		// read the DECIMAL vectors through a float kernel (#685).
		if p, s, ok := avgSumDecimalDecl(a); ok {
			sumSpec.OutputType = distributed.WindowTypePtr(int(parquet.TypeDecimal))
			sumSpec.OutputPrecision, sumSpec.OutputScale = p, s
		}
		countSpec := a
		countSpec.Func = "count"
		countSpec.OutputCol = avgCountPrefix + a.OutputCol
		// The count leg is int64 whatever AVG's own type is, and
		// applyAvgFold reads it straight out of Int64Data — inheriting
		// AVG's float64 declaration here would hand the fold the wrong
		// vector. The (p,s) goes with the type it described.
		countSpec.OutputType = distributed.WindowTypePtr(int(parquet.TypeInt64))
		countSpec.OutputPrecision, countSpec.OutputScale = 0, 0
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
		sumSpec.OutputPrecision, sumSpec.OutputScale = 0, 0
		// The same declaration decomposeAvg makes on the wire spec — see there
		// for why the SUM leg is a DECIMAL at the INPUT's scale (#685).
		if p, s, ok := avgSumDecimalDecl(distributed.AggSpec{
			OutputType:     aggOutputTypePtr(a.OutputType, a.OutputTypeKnown),
			InputPrecision: a.InputPrecision,
			InputScale:     a.InputScale,
		}); ok {
			sumSpec.OutputType = parquet.TypeDecimal
			sumSpec.OutputTypeKnown = true
			sumSpec.OutputPrecision, sumSpec.OutputScale = p, s
		}
		countSpec := a
		countSpec.Func = "count"
		countSpec.OutputCol = avgCountPrefix + a.OutputCol
		countSpec.OutputType = parquet.TypeInt64
		countSpec.OutputTypeKnown = true
		// And the (p,s) goes with the type it described — the same rule the
		// wire twin states. `countSpec := a` copies AVG's own declaration, so
		// before #784 gave AVG over an INTEGER a DECIMAL(38,4) output this
		// pair was always (0,0) and the omission could not show; after it the
		// count leg went out as an int64 column claiming four fraction digits.
		countSpec.OutputPrecision, countSpec.OutputScale = 0, 0
		out = append(out, sumSpec, countSpec)
	}
	return out
}

func isAvgFunc(s string) bool {
	return strings.EqualFold(strings.TrimSpace(s), "avg")
}

// avgSumDecimalDecl is the SUM leg's declaration for an AVG over a DECIMAL:
// the carrier's full precision at the INPUT's scale, which is SUM's rule
// (ADR-0024 item 2). ok=false means this AVG is not over a DECIMAL, or the
// planner could not resolve the input column's (p,s) — in which case the leg
// keeps its float64 declaration, and an identity row emitted for it declares
// what it always did rather than a guessed pair.
//
// The input's scale has to be CARRIED (AggSpec.InputScale) rather than
// recovered from AVG's own declared scale: batch.AvgScale adds a fixed
// increment and saturates at 38, so every input scale from 34 up declares the
// same output scale and the map is not invertible there.
func avgSumDecimalDecl(a distributed.AggSpec) (precision, scale int, ok bool) {
	if a.OutputType == nil || parquet.TypeID(*a.OutputType) != parquet.TypeDecimal {
		return 0, 0, false
	}
	if a.InputPrecision <= 0 {
		// An INTEGER input: AVG(int*) is PostgreSQL's numeric and its SUM leg
		// is numeric at SCALE 0, because the sum of integers has no fraction
		// digits — AVG's own +4 belongs to the division, not to the sum
		// (#784). The input type is carried only for a DERIVED argument, so
		// this is recognised by AVG declaring DECIMAL with no input (p,s):
		// the only other producer of that pair is a computed DECIMAL argument
		// nothing could type, which had no usable declaration here either.
		return batch.MaxDecimalPrecision, 0, true
	}
	return batch.MaxDecimalPrecision, a.InputScale, true
}

// aggOutputTypePtr mirrors physical.AggSpec's (type, known) pair onto the
// pointer convention distributed.AggSpec uses, so decomposeAvgPhysical can ask
// avgSumDecimalDecl the same question decomposeAvg does.
func aggOutputTypePtr(t parquet.TypeID, known bool) *int {
	if !known && t == 0 {
		return nil
	}
	return distributed.WindowTypePtr(int(t))
}
