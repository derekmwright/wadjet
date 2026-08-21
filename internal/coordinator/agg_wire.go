package coordinator

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// wireGroupByTypes converts the planner's derived-group-key types to their
// wire form (OpSpec.GroupByTypes). One function for wireAggSpecs's reason:
// every partial-aggregate dispatch path needs it or the group key vector is
// typed by schema-blind inference on exactly the path a query happens to
// take (#379).
func wireGroupByTypes(m map[string]parquet.TypeID) map[string]int {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = int(v)
	}
	return out
}

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
			InputCol2:  a.InputCol2,
			Separator:  a.Separator,
			Percentile: a.Percentile,
		}
		// physical.AggSpec.OutputType (parquet.TypeID, plain int) is itself
		// ambiguous at zero: aggSpecOutputType returns TypeBool (0) as a
		// GENUINE declaration for every function except the MIN/MAX family
		// (BOOL_AND/BOOL_OR/EVERY always resolve, never guess), and returns
		// 0 for MIN/MAX/MIN_BY/MAX_BY only when it could NOT resolve the
		// input column's catalog type — minMaxDeclaredType never maps an
		// input type to TypeBool, so a MIN/MAX-family zero can only be the
		// undeclared case. Func name alone disambiguates the two (#354): a
		// distributed BOOL_AND used to read its own declaration as absent
		// and fall back to a guess.
		declared := true
		switch strings.ToLower(strings.TrimSpace(a.Func)) {
		case "min", "max", "min_by", "max_by":
			declared = a.OutputType != 0
		}
		if declared {
			spec.OutputType = distributed.WindowTypePtr(int(a.OutputType))
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
