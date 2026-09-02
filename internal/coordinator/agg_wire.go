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

// wireGroupKeyResolve converts the planner's GROUP BY key RESOLUTION list to
// its wire form (OpSpec.GroupByResolve), index-aligned with the published
// names in OpSpec.GroupByCols.
//
// It is a function for wireGroupByTypes' reason one field over: three dispatch
// paths build a partial aggregate — the standalone stage, the fused
// scan-aggregate, and the chain-terminal one on a join — and a resolution list
// that reached only some of them would send the others back to re-deriving the
// key by parsing its published name, which is exactly the defect the second
// name exists to remove (ADR-0026 §2, #794).
//
// Only a fragment that COMPUTES its keys carries one: a merge-mode aggregate
// reads a partial's output, where every key is already a column under its
// published name, and shipping a resolution list there would say the two names
// differ where they cannot.
func wireGroupKeyResolve(resolve []physical.GroupKeyResolution) []distributed.GroupKeyResolveSpec {
	if len(resolve) == 0 {
		return nil
	}
	out := make([]distributed.GroupKeyResolveSpec, len(resolve))
	for i, r := range resolve {
		out[i] = distributed.GroupKeyResolveSpec{Expr: r.Expr, Computed: r.Computed}
	}
	return out
}

// mergeModeResolve is wireGroupKeyResolve for the aggregate fragment builder,
// which serves BOTH roles from one stage type: the standalone partial (raw
// upstream rows, keys computed here) and the merge (a partial's output, keys
// already columns). A merge carries no resolution list, and that is the whole
// of #794 — the intermediate phase and the exchange's partial both consume the
// partial's OUTPUT, so there is nothing for them to agree with.
func mergeModeResolve(stage physical.Stage, mergeMode bool) []distributed.GroupKeyResolveSpec {
	if mergeMode {
		return nil
	}
	return wireGroupKeyResolve(stage.GroupByResolve)
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
			Distinct:   a.Distinct,
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
		// The (p,s) that goes with a DECIMAL OutputType. Carried
		// unconditionally rather than under `declared`, because it is zero
		// for every other type and a DECIMAL declaration always has a
		// precision — see distributed.AggSpec.OutputPrecision (#685).
		spec.OutputPrecision, spec.OutputScale = a.OutputPrecision, a.OutputScale
		// Declared exactly when there is a derived input to type: the
		// planner always resolves InputType alongside InputExpr (its
		// fallback is Float64, never absence). Keying on the expression
		// rather than a nonzero type is what lets a BOOL declaration —
		// TypeID zero — survive the wire (#371).
		if a.InputExpr != "" {
			spec.InputType = distributed.WindowTypePtr(int(a.InputType))
		}
		// The INPUT's (p,s) travels whether or not there is an expression to
		// type: filter_compile reads it only under a declared InputType (a
		// derived argument), and decomposeAvg reads it for a BARE DECIMAL
		// column to declare the SUM leg AVG is split into (#685). Zero for
		// every non-DECIMAL input, so carrying it unconditionally says nothing
		// new about the ones that had it before.
		spec.InputPrecision, spec.InputScale = a.InputPrecision, a.InputScale
		out = append(out, spec)
	}
	return out
}
