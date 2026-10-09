// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// bodyOuterRefs classifies an expression subquery's body by the binder's own
// resolution (ADR-0021 §1k's 2026-10-09 amendment, ADR-0047 stage 3): every
// column reference of the body — its clauses, WHERE and a JOIN's ON included,
// its set-operation arms and the expression subqueries nested in it — resolved
// in the scope the body was validated in (binder.bodyScopes), and the ones
// whose binding reaches the query the subquery is written in returned as the
// per-row re-run spells them: a qualified one under its qualifier, a bare one
// under the qualifier of the relation instance it binds, Bare set.
//
// ok is false when a reference does not resolve with certainty (an open
// scope, a ROW field path, a dotted name, an unresolved name), reaches past
// the enclosing query, or sits in a derived table, a WITH body or a LATERAL
// item of the body (blocks the binder resolves without the enclosing levels):
// the caller then classifies by name, as before.
func (b *binder) bodyOuterRefs(body *plansql.SelectInfo) ([]plansql.OuterRef, bool) {
	var refs []plansql.OuterRef
	ok := b.blockOuterRefs(body, 0, &refs)
	return refs, ok
}

func (b *binder) blockOuterRefs(info *plansql.SelectInfo, depth int, refs *[]plansql.OuterRef) bool {
	if info == nil {
		return false
	}
	if info.Union != nil {
		return b.blockOuterRefs(info.Union.Left, depth, refs) && b.blockOuterRefs(info.Union.Right, depth, refs)
	}
	if len(info.CTEs) > 0 {
		return false
	}
	for i := range info.Tables {
		if strings.HasPrefix(info.Tables[i].Name, "(") {
			return false
		}
	}
	for i := range info.Joins {
		j := &info.Joins[i]
		if j.Lateral || strings.HasPrefix(j.RightTable, "(") ||
			(j.RightTableRef != nil && strings.HasPrefix(j.RightTableRef.Name, "(")) {
			return false
		}
	}
	scopes, known := b.bodyScopes[info]
	if !known || scopes[1] == nil {
		return false
	}
	resolve := scopes[1]
	ok := true
	ref := func(r *plansql.ColRef) {
		if !ok || r.Slot || strings.HasPrefix(r.Column, "__") {
			return
		}
		bd, cat := bindRef(resolve, r)
		switch {
		case !cat.bound():
			ok = false
		case bd.Output || bd.Level <= depth:
			// The body's own, at this nesting.
		case bd.Level == depth+1:
			if r.Table != "" {
				*refs = append(*refs, plansql.OuterRef{Table: strings.ToLower(r.Table), Column: strings.ToLower(r.Column)})
				return
			}
			qual, found := instQualifier(resolve, bd)
			if !found {
				ok = false
				return
			}
			*refs = append(*refs, plansql.OuterRef{Table: strings.ToLower(qual), Column: strings.ToLower(r.Column), Bare: true})
		default:
			ok = false
		}
	}
	visit := func(n plansql.Node) {
		if !ok || n == nil {
			return
		}
		plansql.WalkColRefs(n, ref)
		plansql.ForEachSubquery(n, func(sq plansql.Node) {
			if ok && !b.blockOuterRefs(subqueryMemoBody(sq), depth+1, refs) {
				ok = false
			}
		})
	}
	for i := range info.Columns {
		visit(info.Columns[i].ASTExpr)
		visit(info.Columns[i].AggArgExpr)
	}
	visit(info.WhereExpr)
	visit(info.HavingExpr)
	visit(info.QualifyExpr)
	for _, g := range info.GroupByExprs {
		visit(g)
	}
	for i := range info.Joins {
		visit(info.Joins[i].CondExpr)
	}
	for i := range info.OrderBy {
		if info.OrderBy[i].Ordinal == 0 {
			visit(info.OrderBy[i].Expr)
		}
	}
	return ok
}

// instQualifier is the qualifier of the relation instance a binding names, at
// the level it binds.
func instQualifier(resolve *colScope, bd plansql.Binding) (string, bool) {
	sc := resolve
	for i := 0; i < bd.Level && sc != nil; i++ {
		sc = sc.up
	}
	if sc == nil {
		return "", false
	}
	for _, in := range sc.insts {
		if in.id == bd.Rel {
			return in.qual, in.qual != ""
		}
	}
	return "", false
}

// recordSubqueryOuterRefs records the binder's classification on the node
// (SetOuterRefs) where it resolved every reference, and reports whether the
// body reads the enclosing row — or could not be told apart from one that
// does.
func recordSubqueryOuterRefs(n plansql.Node, refs []plansql.OuterRef, ok bool) (correlated bool) {
	if ok {
		switch q := n.(type) {
		case *plansql.SubqueryNode:
			q.SetOuterRefs(refs)
		case *plansql.ExistsNode:
			q.SetOuterRefs(refs)
		}
	}
	return !ok || len(refs) > 0
}
