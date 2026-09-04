package logical

import (
	"github.com/derekmwright/wadjet/internal/optswitch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// The constant-arithmetic aggregate lift is for EXACT arithmetic only, and it
// lives in ONE place: const_arith_agg_typed.go, which runs after annotation and
// asks the column's type.
//
// # Why there is no syntactic pass any more
//
// There was one, in the builder, and it is deleted rather than narrowed. It ran
// before physical.AnnotateScanColumns had put any type on a scan, so the most
// it could know was how the LITERAL was spelled, and it used that: an integer
// literal declined (#841 — `int op int` overflow is 22003 in PostgreSQL and the
// lifted form cannot make that refusal), a non-integer literal lifted.
//
// The half that lifted was wrong, and had been since before this file existed.
// A non-integer literal says nothing about the COLUMN, and over a double
// precision column the lift is not an identity: IEEE addition is not
// associative, so `SUM(f + 1.0)` and `SUM(f) + 1.0*COUNT(f)` are different
// numbers as soon as the summands span enough magnitude to cancel. Over
// `f = 1e16, 1, 1, 1, 1` PostgreSQL 17.11 answers 1.0000000000000008e+16 and
// the lifted form answers …004e+16 (round-1 review, B1). The pass had no way to
// see that — it had no types — so it could not be made safe, only removed.
//
// What is left is one rule with one implementation: lift where the column's
// declared type and the manifest's bounds PROVE the two forms agree and neither
// can refuse. Shapes the typed pass cannot reach (a derived table, a join or a
// set operation below the aggregate) do not lift at all: right and slower.
//
// # The toggle stays here
//
// constArithAggToggle is the lift's kill switch and the typed pass reads it. It
// had none until #841, which is why that defect was invisible to the
// optimization-invariance oracle for as long as it existed: the oracle
// enumerates optswitch.All() and re-runs every corpus query with each switch
// disabled, and a rewrite outside the registry is simply never disabled.
// Registering it is part of the definition of done for optimization work (#287)
// and extends the oracle for free.
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
