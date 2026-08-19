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
		out = append(out, distributed.AggSpec{
			Func:       a.Func,
			InputCol:   a.InputCol,
			OutputCol:  a.OutputCol,
			InputExpr:  a.InputExpr,
			OutputType: int(a.OutputType),
			InputType:  int(a.InputType),
			InputCol2:  a.InputCol2,
			Separator:  a.Separator,
			Percentile: a.Percentile,
		})
	}
	return out
}
