// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// checkSubqueryRefs uses the binder's body scopes without stamping the body.
// Inside a subquery only a grouped column covers an outer reference: grouping
// by i + 1 does not cover i, even when the body writes the same i + 1 term.
func (g *groupCheck) checkSubqueryRefs(b *binder, sub *plansql.SelectInfo) error {
	if sub == nil {
		return nil
	}
	if sub.Union != nil {
		if err := g.checkSubqueryRefs(b, sub.Union.Left); err != nil {
			return err
		}
		return g.checkSubqueryRefs(b, sub.Union.Right)
	}
	resolve := b.bodyScopes[sub][1]
	if resolve == nil {
		return nil
	}
	check := func(term plansql.Node) error {
		var refs []*plansql.ColRef
		var subs []string
		g.subqueryTermRefs(term, resolve, &refs, &subs)
		for _, ref := range refs {
			binding, category := bindRef(resolve, ref)
			if category != bindOuter {
				continue
			}
			for _, own := range g.from.insts {
				if binding.Rel != own.id {
					continue
				}
				binding.Level = 0
				if !g.boundKeys[binding.BindingKey()] {
					return sqlerr.New("42803", "subquery uses ungrouped column %q from outer query", own.qual+"."+own.cols[binding.Ord])
				}
			}
		}
		for _, sql := range subs {
			if err := g.checkSubqueryRefs(b, b.validatedBody(sql, resolve)); err != nil {
				return err
			}
		}
		return nil
	}
	var terms []plansql.Node
	for _, col := range sub.Columns {
		terms = append(terms, col.ASTExpr)
	}
	terms = append(terms, sub.WhereExpr, sub.HavingExpr, sub.QualifyExpr)
	terms = append(terms, sub.GroupByExprs...)
	for _, ob := range sub.OrderBy {
		terms = append(terms, ob.Expr)
	}
	for _, join := range sub.Joins {
		terms = append(terms, join.CondExpr)
	}
	for _, term := range terms {
		if err := check(term); err != nil {
			return err
		}
	}
	// Derived and CTE bodies already have scopes from validation. A relation
	// with the same name at a nearer level has a different relation identity.
	for i := range sub.Tables {
		inner, _ := sub.Tables[i].SubSelect()
		if err := g.checkSubqueryRefs(b, inner); err != nil {
			return err
		}
	}
	for i := range sub.Joins {
		inner, _ := joinRightRef(&sub.Joins[i]).SubSelect()
		if err := g.checkSubqueryRefs(b, inner); err != nil {
			return err
		}
	}
	for i := range sub.CTEs {
		inner, _ := sub.CTEs[i].BodySelect()
		if err := g.checkSubqueryRefs(b, inner); err != nil {
			return err
		}
	}
	return nil
}

// An aggregate whose nearest argument reference belongs to the containing
// block aggregates that block's rows. Its argument is covered by the aggregate,
// just as an aggregate written directly in the containing block covers it.
func (g *groupCheck) subqueryTermRefs(node plansql.Node, scope *colScope, refs *[]*plansql.ColRef, subs *[]string) {
	if fn, ok := node.(*plansql.FuncCallNode); ok && plansql.IsAggregate(fn.Name) && !strings.EqualFold(fn.Name, "grouping") {
		var args []*plansql.ColRef
		walkExpr(fn, &args, nil, nil)
		nearest := -1
		owned := false
		certain := true
		for _, ref := range args {
			binding, category := bindRef(scope, ref)
			if !category.bound() {
				certain = false
				break
			}
			if nearest == -1 || binding.Level < nearest {
				nearest, owned = binding.Level, false
			}
			if binding.Level == nearest {
				for _, own := range g.from.insts {
					owned = owned || binding.Rel == own.id
				}
			}
		}
		if certain && nearest > 0 && owned {
			return
		}
	}
	switch n := node.(type) {
	case *plansql.ColRef, *plansql.SubqueryNode, *plansql.ExistsNode:
		walkExpr(n, refs, subs, nil)
	case *plansql.WindowFuncNode:
		// The function here is a window call, not an aggregate of outer rows.
		for _, arg := range n.Func.Args {
			g.subqueryTermRefs(arg, scope, refs, subs)
		}
		for _, term := range n.PartitionBy {
			g.subqueryTermRefs(term, scope, refs, subs)
		}
		for _, term := range n.OrderBy {
			g.subqueryTermRefs(term.Expr, scope, refs, subs)
		}
		if n.Frame != nil {
			g.subqueryTermRefs(n.Frame.Start.Offset, scope, refs, subs)
			if n.Frame.End != nil {
				g.subqueryTermRefs(n.Frame.End.Offset, scope, refs, subs)
			}
		}
	default:
		for _, child := range exprOperands(node) {
			g.subqueryTermRefs(child, scope, refs, subs)
		}
	}
}
