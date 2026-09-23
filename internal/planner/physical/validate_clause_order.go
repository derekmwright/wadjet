// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// refuseMisplacedCallInOrder is PostgreSQL's parse-analysis ORDER for the two
// refusals a clause makes about WHERE a call sits: a WINDOW function in HAVING
// (42P20, #1205) and an AGGREGATE in WHERE (42803, #1216 item 3).
//
// PostgreSQL transforms a clause node by node, left to right, and a call's
// ARGUMENTS before the call itself; the placement check is made when the call
// is reached. So the class a client sees depends on which comes first,
// measured on 17.11:
//
//	HAVING row_number() OVER () = 1 AND zz > 0   42P20
//	HAVING zz > 0 AND row_number() OVER () = 1   42703 column "zz" does not exist
//	WHERE SUM(total) > 0 AND zz > 0              42803
//	WHERE zz > 0 AND SUM(total) > 0              42703
//	WHERE SUM(zz) > 0                            42703 (the argument first)
//
// A window's OVER clause is transformed AFTER the placement check, so only its
// function's arguments are resolved first: `HAVING COUNT(*) OVER (PARTITION BY
// zz)` is 42P20.
//
// Before this, HAVING's window was never refused at all: it reached the
// executor as a filter over a column no operator produces, which is an
// internal message on one path and three failed task attempts on the DAG
// (#1205). The WHERE aggregate WAS refused, by the logical builder — after
// this binder had already reported every unknown name in the whole clause, so
// `WHERE SUM(total) > 0 AND zz > 0` was 42703 where PostgreSQL says 42803.
//
// An aggregate belongs to the query level of the variables it reads
// (PostgreSQL's agglevelsup): `WHERE d.k = SUM(typemx.g)` inside a subquery,
// over only the OUTER block's column, is the outer block's aggregate and not
// this WHERE's — it is left to the level that owns it. own reports whether
// this block owns one; an aggregate reading no column is this block's.
//
// Only the first refusal in that order is returned; a clause with neither
// shape returns nil and is checked exactly as before.
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
