// SPDX-License-Identifier: MIT

package logical

import plansql "github.com/derekmwright/wadjet/internal/planner/sql"

// subqueryBodyIn is the tree a decorrelation builds an expression subquery's
// body from, and the WITH chain it plans that body in: the chain the node
// records where it is written (plansql.StampSubqueryScopes, ADR-0047 stage 3)
// or, for a node that records none, ctes — the caller's chain for the
// enclosing block. A CTE reference in the body is planned against the items
// in scope where the subquery is written, and a nested item that reuses a
// name shadows the statement's (#1606).
//
// The tree is the node's MEMOIZED body (arc CI3 round 2): the decorrelation
// reads the one parse every other requester reads. That is safe for the
// rewrite's name-spelled terms (the join keys it derives, the HAVING and
// aggregate terms it lifts, stage 4) because a body the rewrite decorrelates
// is a CORRELATED one, and the binder clears a correlated body's bindings
// (plansql.ClearBindings) — so no block mixes the two spellings, which is what
// the binding census refuses (wadjet.TestArcCI1BindingCensusOverTheGroupKeyTable,
// c738/outerWhereGrouped). An uncorrelated IN keeps its bindings and is
// planned as a block, exactly as the planner plans it elsewhere. The
// classification of the body's references reads the bindings
// (plansql.CorrelatedRefsOf).
//
// The rewrite LIFTS an INNER join's ON conjunct that names the enclosing query
// out of the join (liftBodyOuterConditions rewrites the join's condition in
// place), and it may decline after doing so, leaving the subquery to the
// per-row re-run. So the block it is handed is a copy of the memo's SelectInfo
// with its own join list: the lift rewrites the copy's joins, and the memo —
// which every other requester reads — keeps the body as written. Nothing else
// in the block is written by the rewrite.
func subqueryBodyIn(n plansql.Node, ctes []plansql.CTEDef) (*plansql.SelectInfo, []plansql.CTEDef, error) {
	var (
		info *plansql.SelectInfo
		err  error
	)
	chain, ok := []plansql.CTEDef(nil), false
	switch q := n.(type) {
	case *plansql.SubqueryNode:
		info, err = q.Select()
		chain, ok = q.CTEScope()
	case *plansql.ExistsNode:
		info, err = q.Select()
		chain, ok = q.CTEScope()
	}
	if ok {
		ctes = chain
	}
	if info != nil {
		cp := *info
		cp.Joins = append([]plansql.JoinInfo(nil), info.Joins...)
		info = &cp
	}
	return info, ctes, err
}
