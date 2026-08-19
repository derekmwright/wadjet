package logical

import (
	"fmt"
	"strconv"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// aggArity is the argument count each multi-argument aggregate requires.
// Anything absent from this map takes exactly one argument (or none, for
// COUNT(*)) and is left alone.
var aggArity = map[string]int{
	"corr":            2,
	"covar_samp":      2,
	"covar_pop":       2,
	"min_by":          2,
	"max_by":          2,
	"percentile_cont": 2,
	"percentile_disc": 2,
	"quantile_cont":   2,
	"quantile_disc":   2,
}

// parseAggExtraArgs fills a's arguments past the first from the parsed
// call, and reports a query error when the call's arity is wrong for its
// function.
//
// Only the first argument used to reach the planner at all: the SELECT
// parser kept Args[0] and dropped the rest. Every function below then
// answered with something plausible instead of failing —
//
//	CORR(x, y)            no second column, so the covariance state was
//	                      never updated: NULL, on every path
//	MIN_BY(v, ord)        no ordering column: NULL
//	STRING_AGG(c, '::')   separator silently ",", so a 15000-row answer
//	                      was 14999 characters short of the right one
//	PERCENTILE_CONT(f, c) worse than a wrong fraction: with the fraction
//	                      written first, it was the FRACTION that became
//	                      the aggregated column. `PERCENTILE_CONT(0.9,
//	                      o_totalprice)` aggregated the constant 0.9 over
//	                      15000 rows — 13500 on the DAG, NULL in process
//
// The arity check is what keeps the fields below trustworthy downstream:
// a spec for one of these functions either carries its extra argument or
// the query did not get planned.
func parseAggExtraArgs(a *AggExpr, args []plansql.Node) error {
	fn := strings.ToLower(strings.TrimSpace(a.Func))
	if want, multi := aggArity[fn]; multi && len(args) != want {
		return fmt.Errorf("%s takes %d arguments, got %d", fn, want, len(args))
	}
	switch fn {
	case "corr", "covar_samp", "covar_pop", "min_by", "max_by":
		// (x, y) for the correlation family; (value, ordering) for
		// MIN_BY/MAX_BY, whose first argument is the one returned.
		a.InputCol2 = cleanExpr(args[1].String())
	case "percentile_cont", "percentile_disc", "quantile_cont", "quantile_disc":
		// Two spellings, one function. PERCENTILE_CONT puts the fraction
		// first (PERCENTILE_CONT(0.5, col)); QUANTILE_CONT is DuckDB's, and
		// puts the column first. Either way the fraction must be a literal
		// — it is a property of the aggregate, not a per-row value.
		fracIdx, colIdx := 0, 1
		if fn == "quantile_cont" || fn == "quantile_disc" {
			fracIdx, colIdx = 1, 0
		}
		lit, ok := args[fracIdx].(*plansql.Lit)
		if !ok || lit.Kind != plansql.LitNumber {
			return fmt.Errorf("%s: argument %d must be a numeric fraction literal, got %s", fn, fracIdx+1, args[fracIdx].String())
		}
		p, err := strconv.ParseFloat(lit.Value, 64)
		if err != nil {
			return fmt.Errorf("%s: fraction %q: %w", fn, lit.Value, err)
		}
		if p < 0 || p > 1 {
			return fmt.Errorf("%s: fraction %v is outside [0, 1]", fn, p)
		}
		a.Percentile = p
		// InputCol / InputExpr were taken from Args[0] by the caller, which
		// is the fraction in the PERCENTILE_ spelling; point both at the
		// column whichever way round the call was written.
		a.InputCol = cleanExpr(args[colIdx].String())
		a.InputExpr = args[colIdx]
	case "string_agg":
		// STRING_AGG(col) is legal and means the default separator.
		if len(args) < 2 {
			return nil
		}
		if len(args) > 2 {
			return fmt.Errorf("string_agg takes 1 or 2 arguments, got %d", len(args))
		}
		lit, ok := args[1].(*plansql.Lit)
		if !ok || lit.Kind != plansql.LitString {
			return fmt.Errorf("string_agg: separator must be a string literal, got %s", args[1].String())
		}
		a.Separator = lit.Value
	}
	return nil
}
