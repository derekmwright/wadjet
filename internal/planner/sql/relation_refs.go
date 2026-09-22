// SPDX-License-Identifier: MIT

package sql

import "strings"

// This file answers "does this block read relation X" over the PARSED tree —
// the question a recursive CTE's form, and whether a WITH RECURSIVE item is
// recursive at all, are decided by (ADR-0021 §1o-a). It is one walk for the
// parser, which stamps CTEDef.Recursive, and for the physical planner, which
// classifies the body, so the two cannot disagree about a self-reference.

// SelectNamesRelation reports whether info's FROM — at any nesting, through a
// derived table and through both arms of a set operation — names `want`.
func SelectNamesRelation(info *SelectInfo, want string) bool {
	if info == nil {
		return false
	}
	if info.Union != nil {
		if SelectNamesRelation(info.Union.Left, want) || SelectNamesRelation(info.Union.Right, want) {
			return true
		}
	}
	refNames := func(t TableRef) bool {
		if strings.HasPrefix(t.Name, "(") {
			sub, err := t.SubSelect()
			if err != nil {
				return false
			}
			return SelectNamesRelation(sub, want)
		}
		return strings.EqualFold(strings.TrimSpace(t.Name), want)
	}
	for i := range info.Tables {
		if refNames(info.Tables[i]) {
			return true
		}
	}
	for i := range info.Joins {
		ref := TableRef{Name: info.Joins[i].RightTable}
		if info.Joins[i].RightTableRef != nil {
			ref = *info.Joins[i].RightTableRef
		}
		if refNames(ref) {
			return true
		}
	}
	// A nested block's OWN `WITH` may shadow the name; this walk deliberately
	// does not, because a shadowing item is answered from the enclosing scope
	// here anyway (docs/internals/nested-with-scope-precedence.md) and a
	// false positive costs a refusal where a wrong answer would otherwise
	// stand.
	for i := range info.CTEs {
		if b, err := info.CTEs[i].BodySelect(); err == nil && SelectNamesRelation(b, want) {
			return true
		}
	}
	return false
}

// SublinkNamesRelation reports whether a subquery EXPRESSION anywhere in the
// block — its SELECT list, WHERE, HAVING, a join condition, and those of a
// derived table or a set-operation arm under it — names `want` in its FROM.
func SublinkNamesRelation(info *SelectInfo, want string) bool {
	if info == nil {
		return false
	}
	if info.Union != nil && (SublinkNamesRelation(info.Union.Left, want) || SublinkNamesRelation(info.Union.Right, want)) {
		return true
	}
	var exprs []Node
	for _, c := range info.Columns {
		exprs = append(exprs, c.ASTExpr)
	}
	exprs = append(exprs, info.WhereExpr, info.HavingExpr)
	for _, j := range info.Joins {
		exprs = append(exprs, j.CondExpr)
	}
	for _, e := range exprs {
		for _, sql := range ExprSubqueryTexts(e) {
			if subqueryTextNamesRelation(sql, want) {
				return true
			}
		}
	}
	for i := range info.Tables {
		if sub, err := info.Tables[i].SubSelect(); err == nil && SublinkNamesRelation(sub, want) {
			return true
		}
	}
	for i := range info.Joins {
		if ref := info.Joins[i].RightTableRef; ref != nil {
			if sub, err := ref.SubSelect(); err == nil && SublinkNamesRelation(sub, want) {
				return true
			}
		}
	}
	return false
}

// subqueryTextNamesRelation: the subquery's own FROM names `want`, or one of
// ITS subquery expressions does.
func subqueryTextNamesRelation(sql, want string) bool {
	parsed, err := Parse(sql)
	if err != nil {
		return false
	}
	sub, err := ExtractSelect(parsed)
	if err != nil || sub == nil {
		return false
	}
	return SelectNamesRelation(sub, want) || SublinkNamesRelation(sub, want)
}

// ExprSubqueryTexts collects the SQL of every subquery expression in n: a
// scalar subquery, an EXISTS, and the subquery of an IN or a quantified
// comparison.
func ExprSubqueryTexts(n Node) []string {
	var out []string
	var visit func(Node)
	visit = func(n Node) {
		RewriteExpr(n, func(x Node) (Node, bool) {
			switch v := x.(type) {
			case *SubqueryNode:
				out = append(out, v.SQL)
				return x, true
			case *ExistsNode:
				out = append(out, v.SQL)
				return x, true
			case *AnyAllExpr:
				visit(v.Left)
				for _, e := range v.Values {
					visit(e)
				}
				return x, true
			case *FuncCallNode:
				// RewriteExpr does not descend an aggregate's arguments.
				if IsAggregate(v.Name) {
					for _, a := range v.Args {
						visit(a)
					}
					return x, true
				}
			case *WindowFuncNode:
				if v.Func != nil {
					for _, a := range v.Func.Args {
						visit(a)
					}
				}
				for _, p := range v.PartitionBy {
					visit(p)
				}
				return x, true
			}
			return nil, false
		})
	}
	visit(n)
	return out
}
