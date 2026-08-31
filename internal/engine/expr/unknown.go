package expr

import (
	"errors"
	"fmt"
	"os"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// strictFunctions makes an unresolvable function name a query error instead of
// a column of NULLs. It is on unless WADJET_STRICT_FUNCTIONS=0.
//
// The escape hatch exists for one reason: a BI client that today sends a
// pg_catalog function we do not implement gets NULL and carries on, and after
// this change it gets an error and may abort a connection. The compat shims in
// pgcompat.go cover every such function the introspection probe found, so the
// hatch should never be needed — but an operator who hits one we missed needs
// a way to keep working while they file the issue, and "downgrade my engine"
// is not that way. It is NOT registered in internal/optswitch: that registry is
// for optimizations feeding the invariance oracle, and this is a correctness
// gate, not an optimization.
var strictFunctions = os.Getenv("WADJET_STRICT_FUNCTIONS") != "0"

// unimplementedAggregates are aggregate names other SQL engines have and this
// one does not. They exist only to sharpen the error: without this set,
// array_agg(x) reports as an unknown scalar, which sends the reader looking for
// a spelling mistake instead of telling them the aggregate is missing.
//
// The distinction is not cosmetic. An unimplemented aggregate is the worse of
// the two failures the silent-NULL path produced: a scalar returns the wrong
// VALUE, whereas an aggregate that is never recognized as one also skips the
// grouping entirely, so `SELECT array_agg(n_name) FROM nation` came back as 25
// rows of NULL rather than one row. The SHAPE changed, not just the value.
//
// Names recognized as aggregates and implemented as such live in
// plansql.IsAggregate; these are deliberately not added there, because that
// would route them into physical.parseAggFunc, whose default arm silently
// answers COUNT.
var unimplementedAggregates = map[string]bool{
	"array_agg":         true,
	"count_if":          true,
	"listagg":           true,
	"group_concat":      true,
	"bit_and":           true,
	"bit_or":            true,
	"bit_xor":           true,
	"bool_and_agg":      true,
	"json_agg":          true,
	"jsonb_agg":         true,
	"json_object_agg":   true,
	"histogram":         true,
	"approx_percentile": true,
	"arbitrary":         true,
	"any_value":         true,
	"regr_slope":        true,
	"regr_intercept":    true,
	"regr_count":        true,
	"regr_r2":           true,
	"kurtosis":          true,
	"skewness":          true,
	"checksum":          true,
	"multimap_agg":      true,
	"map_agg":           true,
	"set_agg":           true,
	"array_union_agg":   true,
	"sum_if":            true,
	"avg_if":            true,
	"max_if":            true,
	"min_if":            true,
	"first_value_agg":   true,
	"countif":           true,
	"anyif":             true,
	"quantile":          true,
	"quantile_cont":     true,
	"quantile_disc":     true,
	"product":           true,
	"entropy_agg":       true,
}

// checkKnown reports an unresolvable function name as an error. It is the
// plan-time counterpart of FuncCall.Eval's `if e.fn == nil { return nil }`,
// which is what turned every unimplemented function into a column of NULLs:
// a typo, a Postgres builtin we lack, and a missing aggregate were all
// indistinguishable from a genuinely NULL result (#341).
//
// Aggregate names that the planner DOES handle never reach here as scalars in
// the normal path — the projection carries an output-column reference by then —
// but nested and HAVING rewrites can leave one behind, and a direct
// expr.Compile of count(*) is a supported call. Those pass through untouched;
// this function only decides the fate of names nothing claims.
func checkKnown(name string) error {
	if DefaultRegistry.Has(name) || plansql.IsAggregate(name) {
		return nil
	}
	if !strictFunctions {
		return nil
	}
	return &UnknownFuncError{Name: name, Aggregate: unimplementedAggregates[name]}
}

// ResolveFuncName reports whether a call's name is one this engine implements,
// answering with the same UnknownFuncError (42883) the compiler raises for it.
//
// It exists so a check that runs BEFORE compilation can reach the same verdict
// rather than form a second one: PostgreSQL resolves a function during parse
// analysis and only then checks grouping coverage, so a validator that walks a
// grouped query's expressions has to know that an unresolvable name is already
// settled. Routing that through this one function is what keeps the two
// decisions a single decision — the WADJET_STRICT_FUNCTIONS hatch and the
// unimplemented-aggregate wording included.
func ResolveFuncName(name string) error { return checkKnown(strings.ToLower(name)) }

// UnknownFuncError names a function the registry cannot resolve.
//
// It is a distinct type because the physical planner's compile sites are
// forgiving by design: a projection whose AST will not compile falls back to
// copying an input column of the same name, which is how an aggregate's output
// column reaches the projection. That fallback is right for every compile
// failure EXCEPT this one — a name nothing implements has no column to fall
// back to, so swallowing it converted "unknown function foo" into the far less
// actionable "column \"foo(x)\" does not exist in the input schema", or, before
// the check existed, into no message at all. Callers test for this type with
// errors.As and propagate rather than falling back.
type UnknownFuncError struct {
	Name string
	// Aggregate marks a name this engine recognizes as an aggregate from
	// other SQL dialects but does not implement. The distinction matters to
	// the reader: an unimplemented aggregate silently dropped the GROUP BY
	// as well as the value, so the result had the wrong row COUNT.
	Aggregate bool
}

func (e *UnknownFuncError) Error() string {
	if e.Aggregate {
		return fmt.Sprintf("unknown function: %s is an aggregate function that Wadjet does not implement", e.Name)
	}
	if s := nearestFunc(e.Name); s != "" {
		return fmt.Sprintf("unknown function: %s (did you mean %s?)", e.Name, s)
	}
	return fmt.Sprintf("unknown function: %s", e.Name)
}

// SQLState returns PostgreSQL's undefined_function code. sqlerr.StateOf picks
// it up through the Coder interface so the wire reports 42883 rather than the
// blanket 42000 (#366).
func (e *UnknownFuncError) SQLState() string { return "42883" }

// IsUnknownFunc reports whether err is, or wraps, an UnknownFuncError.
func IsUnknownFunc(err error) bool {
	var ufe *UnknownFuncError
	return errors.As(err, &ufe)
}

// nearestFunc returns a registered name within edit distance 2 of name, or ""
// when nothing is close. A typo is the cheapest of this bug's failure modes to
// diagnose and the most common, so the error carries the fix when it can.
func nearestFunc(name string) string {
	best, bestDist := "", 3
	for _, cand := range DefaultRegistry.Names() {
		// Length alone rules out most of a 300-entry registry.
		if d := len(cand) - len(name); d > 2 || d < -2 {
			continue
		}
		if dist := editDistance(name, cand); dist < bestDist ||
			(dist == bestDist && cand < best) {
			best, bestDist = cand, dist
		}
	}
	return best
}

// editDistance is Levenshtein over two short ASCII-ish identifiers, with a
// single rolling row. Called once per failing query, never per row.
func editDistance(a, b string) int {
	if a == b {
		return 0
	}
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, min(cur[j-1]+1, prev[j-1]+cost))
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}
