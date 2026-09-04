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
// their columns are not visible from here.
func ColumnRefs(n Node) ([]*ColRef, error) {
	var out []*ColRef
	if err := walkColumnRefs(n, &out, false); err != nil {
		return nil, err
	}
	return out, nil
}

// ColumnRefsOutsideSubqueries is ColumnRefs with SubqueryNode and ExistsNode
// treated as OPAQUE leaves rather than refused.
//
// A DML predicate containing a subquery is compiled with a real subquery
// runner and an outer scope (#688), so the names INSIDE the subquery are
// resolved by the subquery's own planning and the names outside it are this
// door's to check. Refusing the whole tree because one operand is a subquery
// is what made `DELETE FROM t WHERE id IN (SELECT id FROM s)` a 0A000.
//
// WindowFuncNode stays refused: a window function is not legal in a WHERE
// clause on any door, and letting one through here would only move the
// failure later.
func ColumnRefsOutsideSubqueries(n Node) ([]*ColRef, error) {
	var out []*ColRef
	if err := walkColumnRefs(n, &out, true); err != nil {
		return nil, err
	}
	return out, nil
}

func walkColumnRefs(n Node, out *[]*ColRef, opaqueSubqueries bool) error {
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
		return walkAll(out, opaqueSubqueries, e.Left, e.Right)
	case *UnaryOp:
		return walkAll(out, opaqueSubqueries, e.Inner)
	case *CmpExpr:
		return walkAll(out, opaqueSubqueries, e.Left, e.Right)
	case *InExpr:
		return walkAll(out, opaqueSubqueries, append([]Node{e.Left}, e.Values...)...)
	case *BetweenExpr:
		return walkAll(out, opaqueSubqueries, e.Left, e.Low, e.High)
	case *LikeExpr:
		return walkAll(out, opaqueSubqueries, e.Left, e.Pattern)
	case *IsExpr:
		return walkAll(out, opaqueSubqueries, e.Left)
	case *AndNode:
		return walkAll(out, opaqueSubqueries, e.Left, e.Right)
	case *OrNode:
		return walkAll(out, opaqueSubqueries, e.Left, e.Right)
	case *NotNode:
		return walkAll(out, opaqueSubqueries, e.Inner)
	case *ParenNode:
		return walkAll(out, opaqueSubqueries, e.Inner)
	case *FuncCallNode:
		return walkAll(out, opaqueSubqueries, e.Args...)
	case *CaseNode:
		nodes := []Node{e.Subject, e.Else}
		for _, w := range e.Whens {
			nodes = append(nodes, w.Cond, w.Result)
		}
		return walkAll(out, opaqueSubqueries, nodes...)
	case *CastNode:
		return walkAll(out, opaqueSubqueries, e.Inner)
	case *ArrayLitNode:
		return walkAll(out, opaqueSubqueries, e.Elements...)
	case *TupleNode:
		return walkAll(out, opaqueSubqueries, e.Elements...)
	case *AnyAllExpr:
		return walkAll(out, opaqueSubqueries, append([]Node{e.Left}, e.Values...)...)

	case *SubqueryNode:
		if opaqueSubqueries {
			return nil
		}
		return fmt.Errorf("a subquery's columns cannot be resolved here")
	case *ExistsNode:
		if opaqueSubqueries {
			return nil
		}
		return fmt.Errorf("an EXISTS subquery's columns cannot be resolved here")
	case *WindowFuncNode:
		return fmt.Errorf("a window function's columns cannot be resolved here")

	default:
		return fmt.Errorf("unhandled expression node %T: its column references cannot be resolved", n)
	}
}

func walkAll(out *[]*ColRef, opaqueSubqueries bool, nodes ...Node) error {
	for _, n := range nodes {
		if err := walkColumnRefs(n, out, opaqueSubqueries); err != nil {
			return err
		}
	}
	return nil
}
