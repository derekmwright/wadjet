package logical

import (
	"github.com/derekmwright/wadjet/internal/optswitch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// The constant-arithmetic aggregate lift is EXACT-only and implemented once,
// in const_arith_agg_typed.go AFTER annotation, using the column's type.
// Lift only when declared type and manifest bounds PROVE both forms agree and
// neither refuses (#841); literal spelling cannot prove that, and IEEE addition
// is not associative. Do not restore a syntactic lift before types are known.
// A derived table, join or set operation below the aggregate prevents the typed
// pass reaching the column, so it does not lift.
// constArithAggToggle must remain registered: optswitch.All() makes the invariance
// oracle disable it for every corpus query; an unregistered rewrite escapes it (#287).
// See docs/internals/exact-aggregate-lift-toggle.md for the design.
var constArithAggToggle = optswitch.Register("const-arith-agg", "WADJET_CONST_ARITH_AGG",
	"lift a constant out of an aggregate over a column whose type proves the rewrite exact: "+
		"SUM(x*k) → SUM(x)*k, SUM(x±k) → SUM(x) ± k*COUNT(x), MIN/MAX(x±k) → MIN/MAX(x)±k. "+
		"Disabling it evaluates the aggregate's input per row.")

func stripParens(n plansql.Node) plansql.Node {
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok {
			return n
		}
		n = p.Inner
	}
}

// plainColAndLit returns (col, lit) when a is a bare column reference and b
// a numeric literal; nils otherwise.
func plainColAndLit(a, b plansql.Node) (plansql.Node, *plansql.Lit) {
	col, ok := stripParens(a).(*plansql.ColRef)
	if !ok {
		return nil, nil
	}
	lit, ok := stripParens(b).(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitNumber {
		return nil, nil
	}
	return col, lit
}
