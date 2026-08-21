package coordinator

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// varStatePrefix names the synthetic column decomposeVar emits in place of
// a STDDEV/VARIANCE aggregate. The full name is
//
//	__var_state#<kind>#<original output name>
//
// where <kind> is one of stddev_samp / var_samp / stddev_pop / var_pop —
// the function the fold has to finish with, since the partial state is the
// same (count, mean, M2) triple for all four. "#" is not legal in a SQL
// identifier, so neither the prefix nor the kind separator can collide with
// a user-written column name.
const varStatePrefix = "__var_state#"

// varStateFuncs are the wire spellings of the aggregate that carries the
// partial state (exec.AggVarState) and the one that combines partial states
// (exec.AggVarStateMerge). The merge form is never planned directly: the
// worker rewrites the partial form into it in merge mode, exactly as it
// rewrites COUNT into SUM.
const (
	varStateFunc      = "var_state"
	varStateMergeFunc = "var_state_merge"
)

// decomposeVar replaces every STDDEV/VARIANCE spec with the partial-state
// aggregate that survives a partial/final split:
//
//	STDDEV(c) AS out → VAR_STATE(c) AS __var_state#stddev_samp#out
//
// A finished standard deviation cannot be re-aggregated. Before this
// rewrite, the final_aggregate stage merged the partials by running STDDEV
// again over the per-task STDDEV values — the standard deviation of three
// nearly identical numbers, which answered `STDDEV(o_totalprice)` at SF0.01
// with 531.79 against a true 144048.14 (#339). Summing squares instead
// would merge correctly but re-introduces the cancellation Welford exists
// to avoid, so what ships is the state itself: the (count, mean, M2) triple
// each partial accumulated, combined pairwise by the merge stage
// (exec.varianceState.merge) and finished by the final stage's fold
// (worker/var_fold.go).
//
// Same shape as decomposeAvg, which splits AVG into SUM and COUNT for the
// same reason. Returns the original slice unchanged when no variance-family
// aggregate is present.
func decomposeVar(specs []distributed.AggSpec) []distributed.AggSpec {
	if !anyVarFunc(func(i int) string { return specs[i].Func }, len(specs)) {
		return specs
	}
	out := make([]distributed.AggSpec, 0, len(specs))
	for _, a := range specs {
		kind, ok := varFuncKind(a.Func)
		if !ok {
			out = append(out, a)
			continue
		}
		s := a
		s.Func = varStateFunc
		s.OutputCol = varStateColumn(kind, a.OutputCol)
		s.OutputType = distributed.WindowTypePtr(int(parquet.TypeString))
		out = append(out, s)
	}
	return out
}

// decomposeVarPhysical is the physical.AggSpec variant, for stage specs the
// dispatcher rewrites before converting to wire format.
func decomposeVarPhysical(specs []physical.AggSpec) []physical.AggSpec {
	if !anyVarFunc(func(i int) string { return specs[i].Func }, len(specs)) {
		return specs
	}
	out := make([]physical.AggSpec, 0, len(specs))
	for _, a := range specs {
		kind, ok := varFuncKind(a.Func)
		if !ok {
			out = append(out, a)
			continue
		}
		s := a
		s.Func = varStateFunc
		s.OutputCol = varStateColumn(kind, a.OutputCol)
		s.OutputType = parquet.TypeString
		out = append(out, s)
	}
	return out
}

// varStateColumn builds the synthetic column name for one decomposed spec.
func varStateColumn(kind, outputCol string) string {
	return varStatePrefix + kind + "#" + outputCol
}

func anyVarFunc(at func(int) string, n int) bool {
	for i := 0; i < n; i++ {
		if _, ok := varFuncKind(at(i)); ok {
			return true
		}
	}
	return false
}

// varFuncKind canonicalizes a variance-family function name to the kind the
// fold finishes with. STDDEV and VARIANCE are the SAMPLE forms (DuckDB and
// PostgreSQL both read the unsuffixed spellings that way); _pop is the
// population form.
func varFuncKind(fn string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(fn)) {
	case "stddev", "stddev_samp":
		return "stddev_samp", true
	case "variance", "var_samp":
		return "var_samp", true
	case "stddev_pop":
		return "stddev_pop", true
	case "var_pop":
		return "var_pop", true
	}
	return "", false
}
