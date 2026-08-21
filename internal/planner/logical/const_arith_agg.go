package logical

import (
	"strconv"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

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
func rewriteConstArithAggs(node plansql.Node) plansql.Node {
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
