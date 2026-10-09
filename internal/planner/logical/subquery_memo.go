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
// The tree is a PRIVATE parse of the node's text, not its memoized, bound
// body: the rewrite spells the join keys it derives, and the HAVING and
// aggregate terms it lifts, by NAME (stage 4), and a block that mixes those
// with the binder's bindings is the mixed comparison the binding census
// refuses (wadjet.TestArcCI1BindingCensusOverTheGroupKeyTable,
// c738/outerWhereGrouped). The classification of the body's references
// reads the bindings (plansql.CorrelatedRefsOf).
func subqueryBodyIn(n plansql.Node, ctes []plansql.CTEDef) (*plansql.SelectInfo, []plansql.CTEDef, error) {
	var sql string
	chain, ok := []plansql.CTEDef(nil), false
	switch q := n.(type) {
	case *plansql.SubqueryNode:
		sql = q.SQL
		chain, ok = q.CTEScope()
	case *plansql.ExistsNode:
		sql = q.SQL
		chain, ok = q.CTEScope()
	}
	if ok {
		ctes = chain
	}
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return nil, ctes, err
	}
	info, err := plansql.ExtractSelect(parsed)
	return info, ctes, err
}
