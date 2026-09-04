package logical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/optswitch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// constArithAggToggle is this rewrite's kill switch. It had none, which is why
// #841 was invisible to the optimization-invariance oracle for as long as it
// existed: the oracle enumerates optswitch.All() and re-runs every corpus
// query with each switch disabled, and a rewrite outside the registry is
// simply never disabled. Registering it is part of the definition of done for
// optimization work (#287) and extends the oracle for free.
var constArithAggToggle = optswitch.Register("const-arith-agg", "WADJET_CONST_ARITH_AGG",
	"lift a constant out of an aggregate: SUM(x*k) → SUM(x)*k, AVG(x±k) → AVG(x)±k, "+
		"MIN/MAX(x±k) → MIN/MAX(x)±k. Disabling it evaluates the aggregate's input per row.")

// rewriteConstArithAggs rewrites aggregates over (column op constant) into
// arithmetic over plain-column aggregates, recursively through the
// expression tree:
//
//	SUM(x + k) → SUM(x) + k * COUNT(x)      (likewise for -)
//	SUM(x * k) → SUM(x) * k                 (likewise k * x)
//	SUM(x / k) → SUM(x) / k                 (k ≠ 0)
//	AVG(x ± k) → AVG(x) ± k
//	AVG(x * k) → AVG(x) * k;  AVG(x / k) → AVG(x) / k (k ≠ 0)
//	MIN/MAX(x ± k) → MIN/MAX(x) ± k
//	MIN/MAX(x * k) → MIN/MAX(x) * k for k > 0 (order-preserving only)
//
// COUNT(x) (not COUNT(*)) keeps null semantics exact: SUM(x+k) sums over
// rows where x is non-null, which is what SUM(x) + k*COUNT(x) computes.
// DISTINCT aggregates are never rewritten. Returns nil when nothing
// changed so callers can keep the original node.
//
// # What the lift may not move: the per-row REFUSAL (#841)
//
// The lift is an identity over VALUES and not over DISPOSITIONS. `x op k` is
// evaluated once per row; `agg(x) op k` is evaluated once. When the per-row
// form can RAISE, the lifted form answers a query PostgreSQL refuses — which
// is how `SUM(big * 2)` came back as an exact 18446744073709551614 while the
// projected `big * 2` raised 22003 on the same row. One expression, two
// dispositions, and the aggregate's was the wrong one: PostgreSQL raises
// `bigint out of range` for the input expression in EVERY position, verified
// live on 17.11 for SUM, AVG, MIN, MAX, a GROUP BY key and a window.
//
// The rule is stated against PostgreSQL, not against this engine's kernels,
// because PostgreSQL is what the disposition has to match:
//
//   - An INTEGER literal makes the per-row pair an INTEGER pair there, and
//     `int op int` overflow is 22003. `+ - *` therefore DECLINE — the same
//     decline `/` has carried since #369 for the same reason (the column's
//     type is unknown at this stage, so the per-row semantics cannot be
//     reproduced by the lifted form).
//   - A NON-INTEGER literal makes the pair numeric or float there, and
//     PostgreSQL never refuses those: `SUM(a*2.0)` over bigint's maximum
//     answers 18446744073709551614.0 on the server. So the lift cannot lose a
//     refusal PostgreSQL makes, and it stays.
//
// This engine's own DECIMAL carrier can refuse where PostgreSQL answers
// (ADR-0012 records that divergence), and the lift can move that refusal too —
// in the direction of PostgreSQL's answer, never away from it, so it is not a
// disposition this rule has to preserve.
//
// # The cost, measured rather than assumed
//
// The ClickBench Q30 shape — `SUM(col + k)` ninety times over one integer
// column — no longer lifts, and that is expensive: 200 000 rows × 90
// aggregates, median of three runs of five, 7.6 ms with the lift and 342 ms
// without it, ~45×. Ninety per-row expression passes and ninety accumulators
// instead of a SUM and a COUNT.
//
// It shipped anyway, and the reason was a standing rule rather than a
// judgement call: a correctness fix is never gated on a perf A/B, and
// PostgreSQL decides what is an error (ADR-0012 item 1).
//
// # The recovery, and where it lives (#850)
//
// The loss was confined to one shape family and fully recoverable, because the
// decline above is SYNTACTIC — this pass runs in the builder, before
// physical.AnnotateScanColumns has put any type on a scan, so "the literal is
// an integer" is the most it can know. The lift is SAFE exactly when the
// per-row arithmetic cannot refuse, and that is decidable from the column's
// declared type and the manifest's min/max.
//
// const_arith_agg_typed.go is that pass. It runs inside logical.Optimize,
// after the annotators, over the built Aggregate and the Project above it, and
// it takes exactly the aggregates this one declined. This pass is left as it
// is: it is the answer for everything the second one cannot see (a derived
// table, a join, a set operation below the aggregate), and its decline is the
// safe default the typed pass narrows rather than replaces.
func rewriteConstArithAggs(node plansql.Node) plansql.Node {
	if !constArithAggToggle.On() {
		return nil
	}
	changed := false
	out := rewriteCAA(node, &changed)
	if !changed {
		return nil
	}
	return out
}

func rewriteCAA(node plansql.Node, changed *bool) plansql.Node {
	switch n := node.(type) {
	case *plansql.FuncCallNode:
		if rw := rewriteOneAgg(n); rw != nil {
			*changed = true
			return rw
		}
		newArgs := make([]plansql.Node, len(n.Args))
		for i, a := range n.Args {
			newArgs[i] = rewriteCAA(a, changed)
		}
		return &plansql.FuncCallNode{Name: n.Name, Args: newArgs, Distinct: n.Distinct, Star: n.Star}
	case *plansql.BinaryOp:
		return &plansql.BinaryOp{Op: n.Op, Left: rewriteCAA(n.Left, changed), Right: rewriteCAA(n.Right, changed)}
	case *plansql.ParenNode:
		return &plansql.ParenNode{Inner: rewriteCAA(n.Inner, changed)}
	case *plansql.UnaryOp:
		return &plansql.UnaryOp{Op: n.Op, Inner: rewriteCAA(n.Inner, changed)}
	default:
		return node
	}
}

// rewriteOneAgg returns the rewritten expression for a single qualifying
// aggregate call, or nil.
func rewriteOneAgg(fn *plansql.FuncCallNode) plansql.Node {
	name := strings.ToLower(fn.Name)
	switch name {
	case "sum", "avg", "min", "max":
	default:
		return nil
	}
	if fn.Distinct || fn.Star || len(fn.Args) != 1 {
		return nil
	}
	bin, ok := stripParens(fn.Args[0]).(*plansql.BinaryOp)
	if !ok {
		return nil
	}
	var colSide plansql.Node
	var k float64
	var kNode *plansql.Lit
	constFirst := false
	if c, lit := plainColAndLit(bin.Left, bin.Right); c != nil {
		colSide, kNode = c, lit
	} else if c, lit := plainColAndLit(bin.Right, bin.Left); c != nil {
		colSide, kNode, constFirst = c, lit, true
	} else {
		return nil
	}
	var err error
	k, err = strconv.ParseFloat(kNode.Value, 64)
	if err != nil {
		return nil
	}
	// An INTEGER literal makes the per-row pair an integer pair in PostgreSQL,
	// where overflow is 22003 — a refusal the lifted form cannot make. See the
	// header: `+ - *` decline for it, `/` already declined below for its own
	// reason (truncation), and a non-integer literal keeps the lift because
	// PostgreSQL's numeric never refuses (#841).
	_, kIsInteger := strconv.ParseInt(strings.TrimSpace(kNode.Value), 10, 64)
	if kIsInteger == nil && (bin.Op == "+" || bin.Op == "-" || bin.Op == "*") {
		return nil
	}

	aggOf := func(aggName string) plansql.Node {
		return &plansql.FuncCallNode{Name: aggName, Args: []plansql.Node{colSide}}
	}
	lit := func() plansql.Node { return kNode }

	switch bin.Op {
	case "+":
		switch name {
		case "sum":
			// SUM(x)+k*COUNT(x)  (x+k and k+x identical)
			return &plansql.BinaryOp{Op: "+", Left: aggOf("sum"),
				Right: &plansql.BinaryOp{Op: "*", Left: lit(), Right: aggOf("count")}}
		default: // avg/min/max
			return &plansql.BinaryOp{Op: "+", Left: aggOf(name), Right: lit()}
		}
	case "-":
		if name == "sum" {
			if constFirst { // SUM(k-x) = k*COUNT(x) - SUM(x)
				return &plansql.BinaryOp{Op: "-",
					Left:  &plansql.BinaryOp{Op: "*", Left: lit(), Right: aggOf("count")},
					Right: aggOf("sum")}
			}
			return &plansql.BinaryOp{Op: "-", Left: aggOf("sum"),
				Right: &plansql.BinaryOp{Op: "*", Left: lit(), Right: aggOf("count")}}
		}
		if constFirst { // MIN(k-x) = k - MAX(x); AVG(k-x) = k - AVG(x)
			inner := name
			if name == "min" {
				inner = "max"
			} else if name == "max" {
				inner = "min"
			}
			return &plansql.BinaryOp{Op: "-", Left: lit(), Right: aggOf(inner)}
		}
		return &plansql.BinaryOp{Op: "-", Left: aggOf(name), Right: lit()}
	case "*":
		if name == "min" || name == "max" {
			if k <= 0 {
				return nil // only order-preserving scaling
			}
		}
		if name == "sum" || name == "avg" || name == "min" || name == "max" {
			return &plansql.BinaryOp{Op: "*", Left: aggOf(name), Right: lit()}
		}
	case "/":
		if constFirst || k == 0 {
			return nil // k/x is not linear; /0 keeps original semantics
		}
		// An INTEGER literal divisor may mean integer division (#369): over
		// an integer column, x/k truncates PER ROW, so SUM(x/k) ≠ SUM(x)/k
		// and AVG(x/k) ≠ AVG(x)/k. The column's type is unknown at this
		// stage, so decline unless the literal itself pins float division.
		if _, err := strconv.ParseInt(kNode.Value, 10, 64); err == nil {
			return nil
		}
		if name == "min" || name == "max" {
			if k < 0 {
				return nil
			}
		}
		return &plansql.BinaryOp{Op: "/", Left: aggOf(name), Right: lit()}
	}
	return nil
}

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
