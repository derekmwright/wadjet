package coordinator

import (
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// wireAggSpecs converts planner aggregate specs to their wire form.
//
// One function rather than a struct literal per dispatch path: there are
// five of those, and every field added to the spec has to reach all five
// or the aggregate behaves differently depending on which shape the query
// took. That is how MIN_BY's ordering column and STRING_AGG's separator
// would go missing on exactly one path (#353).
func wireAggSpecs(specs []physical.AggSpec) []distributed.AggSpec {
	if len(specs) == 0 {
		return nil
	}
	out := make([]distributed.AggSpec, 0, len(specs))
	for _, a := range specs {
		spec := distributed.AggSpec{
			Func:       a.Func,
			InputCol:   a.InputCol,
			OutputCol:  a.OutputCol,
			InputExpr:  a.InputExpr,
			OutputType: int(a.OutputType),
			InputCol2:  a.InputCol2,
			Separator:  a.Separator,
			Percentile: a.Percentile,
		}
		// Declared exactly when there is a derived input to type: the
		// planner always resolves InputType alongside InputExpr (its
		// fallback is Float64, never absence). Keying on the expression
		// rather than a nonzero type is what lets a BOOL declaration —
		// TypeID zero — survive the wire (#371).
		if a.InputExpr != "" {
			spec.InputType = distributed.WindowTypePtr(int(a.InputType))
		}
		out = append(out, spec)
	}
	return out
}
