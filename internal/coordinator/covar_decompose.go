package coordinator

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// covarStatePrefix names the synthetic column decomposeCovar emits in place
// of a CORR/COVAR_SAMP/COVAR_POP aggregate. The full name is
//
//	__covar_state#<kind>#<original output name>
//
// where <kind> is corr / covar_samp / covar_pop — the function the fold has
// to finish with, since the partial state is the same
// (count, meanX, meanY, C, M2x, M2y) sextuple for all three. "#" is not
// legal in a SQL identifier, so neither the prefix nor the kind separator
// can collide with a user-written column name.
const covarStatePrefix = "__covar_state#"

// covarStateFunc is the wire spelling of the aggregate that carries the
// partial state (exec.AggCovarState). Its merge counterpart
// ("covar_state_merge", exec.AggCovarStateMerge) is never planned directly:
// the worker rewrites the partial form into it in merge mode, exactly as it
// does for var_state and for COUNT→SUM.
const covarStateFunc = "covar_state"

// decomposeCovar replaces every CORR/COVAR spec with the partial-state
// aggregate that survives a partial/final split:
//
//	CORR(x, y) AS out → COVAR_STATE(x, y) AS __covar_state#corr#out
//
// Same shape and same reason as decomposeVar (var_decompose.go): a finished
// correlation coefficient cannot be re-aggregated. What crosses the wire is
// the state — count, both means, the co-moment C and both M2 terms — which
// combines pairwise (exec.covarianceState.merge) and is finished once, by
// the final stage's fold (worker/var_fold.go).
//
// On the DAG these three never even reached the re-aggregation the variance
// family did: the worker's parseAggFuncString had no case for "corr" or
// "covar_*", so they fell to its `default: AggSum` and
// CORR(o_totalprice, o_custkey) at SF0.01 answered 2.127e9 — the sum of
// o_totalprice — against a true -0.00575 (#353).
//
// Returns the original slice unchanged when no covariance-family aggregate
// is present.
func decomposeCovar(specs []distributed.AggSpec) []distributed.AggSpec {
	any := false
	for _, s := range specs {
		if _, ok := covarFuncKind(s.Func); ok {
			any = true
			break
		}
	}
	if !any {
		return specs
	}
	out := make([]distributed.AggSpec, 0, len(specs))
	for _, a := range specs {
		kind, ok := covarFuncKind(a.Func)
		if !ok {
			out = append(out, a)
			continue
		}
		s := a
		s.Func = covarStateFunc
		s.OutputCol = covarStateColumn(kind, a.OutputCol)
		s.OutputType = int(parquet.TypeString)
		out = append(out, s)
	}
	return out
}

// covarStateColumn builds the synthetic column name for one decomposed spec.
func covarStateColumn(kind, outputCol string) string {
	return covarStatePrefix + kind + "#" + outputCol
}

// covarFuncKind canonicalizes a covariance-family function name to the kind
// the fold finishes with.
func covarFuncKind(fn string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(fn)) {
	case "corr":
		return "corr", true
	case "covar_samp":
		return "covar_samp", true
	case "covar_pop":
		return "covar_pop", true
	}
	return "", false
}
