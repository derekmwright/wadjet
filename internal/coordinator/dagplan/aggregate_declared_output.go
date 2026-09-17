// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// aggSpecInputDecimal is the (p,s) of a bare DECIMAL COLUMN argument — the
// declaration the aggregate READS, as opposed to the one physical.PlanContext.AggSpecOutputDecimal
// says it writes. Any aggregate, not only the six above: the pair describes the
// column, not the function.
//
// It exists because AVG is not dispatched as AVG: decomposeAvg splits it into
// SUM and COUNT legs, and the SUM leg declares the INPUT's scale where AVG
// declares batch.AvgScale of it. That increment saturates at the carrier's 38
// digits, so AVG's own declaration cannot be inverted back to the input's for
// a scale of 34 or more — the leg has to be told (#685).
//
// Declines for a computed argument, where the derived-expression branch types
// the projection instead (AggSpec.InputType/InputPrecision/InputScale).
func aggSpecInputDecimal(node *logical.Node, agg logical.AggExpr) (logical.DecimalMeta, bool) {
	if agg.InputExpr != nil {
		if _, bare := agg.InputExpr.(*plansql.ColRef); !bare {
			return logical.DecimalMeta{}, false
		}
	}
	// physical.PlanContext.AggInputColumnDecimal, not scanColumnDecimal: the same walk, in the same
	// order, that physical.PlanContext.AggSpecOutputType asks for the input's TYPE. Two functions
	// answering one question about one column with two different walks is
	// ADR-0023 item 5 one layer over, and the disagreement is a DECIMAL
	// declared with nobody's scale — for a WINDOW SLOT (`SUM(__win_0)`), whose
	// declaration lives in the emitted walk and in no scan at all, the scan-only
	// walk answered (0,0) (#775).
	return localPlanFacts.AggInputColumnDecimal(node, agg.InputCol)
}
