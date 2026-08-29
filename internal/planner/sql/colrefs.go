package sql

import "fmt"

// ColumnRefs returns every column reference in an expression tree.
//
// It exists for the callers that must resolve names against a schema BEFORE
// executing — the DML doors, which do not go through the planner and so never
// had a name-resolution step at all. `UPDATE t SET n = 1 WHERE nosuchcol = 1`
// answered "UPDATE 0" because the reference evaluated to NULL on every row,
// where PostgreSQL raises 42703 (#678).
//
// A node type it does not know is an ERROR, never a silent skip. That is the
// whole reason it lives beside the AST it walks: a node added to ast.go
// without a case here fails loudly at the one call site that depends on
// completeness, instead of quietly letting an unresolvable column through.
// The three nodes that carry RAW SQL rather than a parsed subtree
// (SubqueryNode, ExistsNode, WindowFuncNode) are refused for the same reason —
// their columns are not visible from here — and none of them is compilable in
// a DML clause anyway.
func ColumnRefs(n Node) ([]*ColRef, error) {
	var out []*ColRef
	if err := walkColumnRefs(n, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func walkColumnRefs(n Node, out *[]*ColRef) error {
	if n == nil {
		return nil
	}
	switch e := n.(type) {
	case *ColRef:
		*out = append(*out, e)
		return nil

	// Leaves with no children and no column references.
	case *Lit, *LiteralPlaceholder, *StarNode, *IntervalLit:
		return nil

	case *BinaryOp:
		return walkAll(out, e.Left, e.Right)
	case *UnaryOp:
		return walkAll(out, e.Inner)
	case *CmpExpr:
		return walkAll(out, e.Left, e.Right)
	case *InExpr:
		return walkAll(out, append([]Node{e.Left}, e.Values...)...)
	case *BetweenExpr:
		return walkAll(out, e.Left, e.Low, e.High)
	case *LikeExpr:
		return walkAll(out, e.Left, e.Pattern)
	case *IsExpr:
		return walkAll(out, e.Left)
	case *AndNode:
		return walkAll(out, e.Left, e.Right)
	case *OrNode:
		return walkAll(out, e.Left, e.Right)
	case *NotNode:
		return walkAll(out, e.Inner)
	case *ParenNode:
		return walkAll(out, e.Inner)
	case *FuncCallNode:
		return walkAll(out, e.Args...)
	case *CaseNode:
		nodes := []Node{e.Subject, e.Else}
		for _, w := range e.Whens {
			nodes = append(nodes, w.Cond, w.Result)
		}
		return walkAll(out, nodes...)
	case *CastNode:
		return walkAll(out, e.Inner)
	case *ArrayLitNode:
		return walkAll(out, e.Elements...)
	case *TupleNode:
		return walkAll(out, e.Elements...)
	case *AnyAllExpr:
		return walkAll(out, append([]Node{e.Left}, e.Values...)...)

	case *SubqueryNode:
		return fmt.Errorf("a subquery's columns cannot be resolved here")
	case *ExistsNode:
		return fmt.Errorf("an EXISTS subquery's columns cannot be resolved here")
	case *WindowFuncNode:
		return fmt.Errorf("a window function's columns cannot be resolved here")

	default:
		return fmt.Errorf("unhandled expression node %T: its column references cannot be resolved", n)
	}
}

func walkAll(out *[]*ColRef, nodes ...Node) error {
	for _, n := range nodes {
		if err := walkColumnRefs(n, out); err != nil {
			return err
		}
	}
	return nil
}
