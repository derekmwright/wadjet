package logical

import (
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// What `rewriteConstArithAggs` lifts and what it declines (#841).
//
// The rewrite had NO test at all, and the only gate the #841 fix added asserts
// a VALUE that is identical lifted or not — so a decline broadened to every
// literal, which would cost the remaining lift everywhere on top of #850's
// ~45×, would have left every gate green. This asserts the SHAPE the rewrite
// produces, which is the only thing that can tell the two apart.
//
// The rule the shapes below encode, stated against PostgreSQL rather than
// against this engine's kernels:
//
//   - an INTEGER literal makes the per-row pair an INTEGER pair there, and
//     `int op int` overflow is 22003 — a refusal the lifted form cannot make,
//     so `+ - *` DECLINE;
//   - a NON-INTEGER literal makes the pair numeric, which PostgreSQL never
//     refuses (`SUM(a*2.0)` over bigint's maximum answers on the server), so
//     the lift STAYS.
//
// Both halves are the gate. An implementation that lifted everything fails the
// declines; one that lifted nothing fails the lifts.
func TestConstArithAggLiftsOnlyWhatCannotLoseARefusal(t *testing.T) {
	num := func(v string) *plansql.Lit { return &plansql.Lit{Value: v, Kind: plansql.LitNumber} }
	col := func(n string) *plansql.ColRef { return &plansql.ColRef{Column: n} }
	agg := func(fn string, arg plansql.Node) *plansql.FuncCallNode {
		return &plansql.FuncCallNode{Name: fn, Args: []plansql.Node{arg}}
	}
	bin := func(l plansql.Node, op string, r plansql.Node) *plansql.BinaryOp {
		return &plansql.BinaryOp{Left: l, Op: op, Right: r}
	}

	for _, c := range []struct {
		name string
		node plansql.Node
		lift bool
	}{
		// DECLINED: the per-row form is integer arithmetic and can raise.
		{"sum of a column plus an integer", agg("sum", bin(col("k"), "+", num("1"))), false},
		{"sum of a column times an integer", agg("sum", bin(col("k"), "*", num("2"))), false},
		{"sum of a column minus an integer", agg("sum", bin(col("k"), "-", num("3"))), false},
		{"an integer minus a column", agg("sum", bin(num("10"), "-", col("k"))), false},
		{"avg of a column plus an integer", agg("avg", bin(col("k"), "+", num("1"))), false},
		{"min of a column plus an integer", agg("min", bin(col("k"), "+", num("1"))), false},
		{"max of a column times an integer", agg("max", bin(col("k"), "*", num("2"))), false},
		// LIFTED: a non-integer literal makes the pair numeric, which
		// PostgreSQL never refuses, so nothing is lost by evaluating it once.
		{"sum of a column plus a decimal", agg("sum", bin(col("k"), "+", num("1.5"))), true},
		{"sum of a column times a decimal", agg("sum", bin(col("k"), "*", num("2.5"))), true},
		{"avg of a column plus a decimal", agg("avg", bin(col("k"), "+", num("1.5"))), true},
		{"min of a column plus a decimal", agg("min", bin(col("k"), "+", num("1.5"))), true},
		// Division by a non-integer stays lifted; by an INTEGER it has
		// declined since #369, because integer division truncates PER ROW.
		{"sum of a column over a decimal", agg("sum", bin(col("k"), "/", num("2.0"))), true},
		{"sum of a column over an integer", agg("sum", bin(col("k"), "/", num("2"))), false},
		// Shapes the rewrite never claimed: DISTINCT, a non-literal operand,
		// and an aggregate it does not know.
		{"a distinct aggregate", &plansql.FuncCallNode{Name: "sum", Distinct: true,
			Args: []plansql.Node{bin(col("k"), "*", num("2.5"))}}, false},
		{"two columns", agg("sum", bin(col("k"), "*", col("j"))), false},
		{"count", agg("count", bin(col("k"), "+", num("1.5"))), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := rewriteConstArithAggs(c.node) != nil
			if got != c.lift {
				verb := map[bool]string{true: "LIFTED", false: "DECLINED"}
				t.Errorf("%s was %s, want %s — see rewriteConstArithAggs's header for the rule",
					c.node.String(), verb[got], verb[c.lift])
			}
		})
	}
}

// TestConstArithAggKillSwitchDisablesTheLift is #287's half: the rewrite is
// registered in optswitch so the optimization-invariance oracle can disable it,
// which is what "registering the switch extends the oracle for free" means. It
// had none, which is why #841 was invisible to that oracle for as long as it
// existed.
func TestConstArithAggKillSwitchDisablesTheLift(t *testing.T) {
	n := &plansql.FuncCallNode{Name: "sum", Args: []plansql.Node{
		&plansql.BinaryOp{Left: &plansql.ColRef{Column: "k"}, Op: "+",
			Right: &plansql.Lit{Value: "1.5", Kind: plansql.LitNumber}},
	}}
	if rewriteConstArithAggs(n) == nil {
		t.Fatal("the lift declined a shape it must take; the switch test proves nothing")
	}
	prev := constArithAggToggle.Set(false)
	defer constArithAggToggle.Set(prev)
	if got := rewriteConstArithAggs(n); got != nil {
		t.Errorf("the lift still fired with the switch OFF: %v", got.String())
	}
}
