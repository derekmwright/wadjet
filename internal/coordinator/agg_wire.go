package coordinator

import (
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/logical"
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

// wireGroupByDecimal is wireGroupByTypes' companion for the (p,s) of the
// DECIMAL keys. A key vector built from the TypeID alone comes out at scale 0
// and TRUNCATES every value written into it — #379's defect, one type over
// (ADR-0024 item 2).
func wireGroupByDecimal(m map[string]logical.DecimalMeta) map[string]distributed.DecimalMeta {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]distributed.DecimalMeta, len(m))
	for k, v := range m {
		out[k] = distributed.DecimalMeta{Precision: v.Precision, Scale: v.Scale}
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
		// ambiguous at zero: TypeBool IS zero, and it is a GENUINE
		// declaration for BOOL_AND/BOOL_OR/EVERY — and, since #392, for
		// MIN_BY/MAX_BY over a BOOL column, whose output type is its input's
		// whatever that is. The func name can no longer disambiguate the two
		// (it could while minMaxDeclaredType's six-case switch never mapped
		// anything to BOOL), so the planner says which it means outright.
		// Reading a real declaration as absent is #354: a distributed
		// BOOL_AND fell back to a guess about its own output.
		//
		// The OutputType != 0 arm keeps specs built outside
		// aggSpecOutputType — the exchange sender-side partials, older
		// plans — declaring exactly what they declared before.
		declared := a.OutputTypeKnown || a.OutputType != 0
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
			spec.InputPrecision, spec.InputScale = a.InputPrecision, a.InputScale
		}
		out = append(out, spec)
	}
	return out
}
