// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// refuseMisplacedCallInOrder checks nodes left to right, arguments before
// calls, returning the first error. A window in HAVING is 42P20 before its
// OVER clause is resolved; an aggregate in WHERE is 42803. An aggregate
// belongs to the level of its variables, with no-variable calls owned here.
// No matching construct returns nil. See ADR-0012 §5, #1205 and #1216.
func refuseMisplacedCallInOrder(node plansql.Node, scope *colScope, clause string, own *colScope) error {
	if node == nil {
		return nil
	}
	switch clause {
	case "HAVING":
		if len(plansql.FindAllWindowFuncs(node)) == 0 {
			return nil
		}
	case "WHERE":
		if len(plansql.FindAllAggregates(node)) == 0 {
			return nil
		}
	default:
		return nil
	}
	return clauseOrderWalk(node, scope, clause, own)
}

// ownsAggregate reports whether any column an aggregate reads is one of this
// block's own (own), or the aggregate reads none.
func ownsAggregate(fc *plansql.FuncCallNode, own *colScope) bool {
	if own == nil {
		return true
	}
	var refs []*plansql.ColRef
	for _, a := range fc.Args {
		walkExpr(a, &refs, nil, nil)
	}
	if len(refs) == 0 {
		return true
	}
	for _, r := range refs {
		if r.Table != "" {
			if _, ok := own.quals[strings.ToLower(r.Table)]; ok {
				return true
			}
			continue
		}
		if own.cols[strings.ToLower(r.Column)] || own.open {
			return true
		}
	}
	return false
}

func clauseOrderWalk(node plansql.Node, scope *colScope, clause string, own *colScope) error {
	switch n := node.(type) {
	case nil:
		return nil
	case *plansql.ColRef:
		if pgSystemColumns[strings.ToLower(n.Column)] {
			return nil
		}
		return scope.resolveRef(n)
	case *plansql.SubqueryNode, *plansql.ExistsNode:
		// Their own blocks, validated on their own terms.
		return nil
	case *plansql.WindowFuncNode:
		if n.Func != nil {
			for _, a := range n.Func.Args {
				if err := clauseOrderWalk(a, scope, clause, own); err != nil {
					return err
				}
			}
		}
		if clause == "HAVING" {
			return sqlerr.New("42P20", "window functions are not allowed in HAVING")
		}
		return nil
	case *plansql.FuncCallNode:
		for _, a := range n.Args {
			if err := clauseOrderWalk(a, scope, clause, own); err != nil {
				return err
			}
		}
		if clause == "WHERE" && plansql.IsAggregate(n.Name) && ownsAggregate(n, own) {
			kind := "aggregate functions"
			if strings.EqualFold(n.Name, "grouping") {
				kind = "grouping operations"
			}
			return sqlerr.New("42803", "%s are not allowed in WHERE", kind)
		}
		return nil
	}
	for _, child := range exprOperands(node) {
		if err := clauseOrderWalk(child, scope, clause, own); err != nil {
			return err
		}
	}
	return nil
}
