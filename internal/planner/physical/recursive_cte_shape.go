// SPDX-License-Identifier: MIT

// This file holds the recursive-term shape rules for the physical planner,
// governed by ADR-0021 §1o-a and ADR-0012.
package physical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// refuseRecursiveTermShape is PostgreSQL's checkWellFormedRecursion over the
// RECURSIVE term, with PostgreSQL's class (42P19) and sentence for each shape,
// measured on 17.11:
//
//   - an aggregate in a block of the recursive term whose own FROM names the
//     reference — "aggregate functions are not allowed in a recursive
//     query's recursive term" (aggregateOverTheReference);
//   - the self-reference inside a subquery expression (EXISTS, IN, a scalar
//     subquery) — "… must not appear within a subquery";
//   - the self-reference on the NULLABLE side of an outer join — "… must not
//     appear within an outer join";
//   - the self-reference more than once — "… must not appear more than once".
//
// These are not style rules. Each of them is a term that, iterated the way
// this engine iterates, does not reach a fixed point or reaches the wrong one:
// `SELECT max(n) + 1 FROM r WHERE n < 3` produces a NULL row from an empty
// working table forever, and `lat_ord LEFT JOIN r` produces the same outer rows
// at every step. With the old 1000-iteration cap those answered 1000 rows of a
// truncated recursion; without it they would run until the iteration limit, so
// they are refused before anything runs.
//
// It sees the parsed tree only. A self-reference it cannot see — inside a
// nested WITH's body that shadows the name, say — is refused where it is
// conservative to (a false refusal) and never answered as something else.
func refuseRecursiveTermShape(cteName string, term *plansql.SelectInfo) error {
	name := strings.ToLower(strings.TrimSpace(cteName))
	if aggregateOverTheReference(term, name) {
		return sqlerr.New("42P19",
			"aggregate functions are not allowed in a recursive query's recursive term")
	}
	ref := func(where string) error {
		return sqlerr.New("42P19",
			"recursive reference to query %q must not appear %s", cteName, where)
	}
	if plansql.SublinkNamesRelation(term, name) {
		return ref("within a subquery")
	}
	if outerJoinNullableSideNames(term, name) {
		return ref("within an outer join")
	}
	if fromReferenceCount(term, name) > 1 {
		return ref("more than once")
	}
	return nil
}

// aggregateOverTheReference is PostgreSQL's aggregate rule for a recursive
// term, measured on 17.11: an aggregate is refused in a query block whose OWN
// FROM names the recursive reference — the term itself, or a derived table at
// any depth under it — and allowed in a block that reads the reference only
// through a derived table below it (`SELECT max(m) FROM (SELECT n+1 AS m FROM
// r …) q` answers there) or not at all (`… (SELECT count(*) FROM t) q`).
// Checking only the term's own level missed `SELECT n FROM (SELECT max(n)+1
// AS n FROM r …) q`, which PostgreSQL refuses (arc RC round 2, B2).
func aggregateOverTheReference(info *plansql.SelectInfo, want string) bool {
	if info == nil {
		return false
	}
	if info.Union != nil {
		return aggregateOverTheReference(info.Union.Left, want) ||
			aggregateOverTheReference(info.Union.Right, want)
	}
	direct := false
	var derived []*plansql.SelectInfo
	visit := func(t *plansql.TableRef) {
		if t == nil {
			return
		}
		if strings.HasPrefix(t.Name, "(") {
			if sub, err := t.SubSelect(); err == nil && sub != nil {
				derived = append(derived, sub)
			}
			return
		}
		if strings.EqualFold(strings.TrimSpace(t.Name), want) {
			direct = true
		}
	}
	for i := range info.Tables {
		visit(&info.Tables[i])
	}
	for i := range info.Joins {
		ref := info.Joins[i].RightTableRef
		if ref == nil {
			ref = &plansql.TableRef{Name: info.Joins[i].RightTable}
		}
		visit(ref)
	}
	if direct && selectHasAggregate(info) {
		return true
	}
	for _, sub := range derived {
		if aggregateOverTheReference(sub, want) {
			return true
		}
	}
	return false
}

// selectHasAggregate reports an aggregate call at THIS block's level — its
// SELECT list or HAVING, through both arms of a set operation — and not one
// inside a subquery expression, which is its own block (PostgreSQL answers
// `SELECT n + (SELECT count(*) FROM t) FROM r`).
func selectHasAggregate(info *plansql.SelectInfo) bool {
	if info == nil {
		return false
	}
	if info.Union != nil && (selectHasAggregate(info.Union.Left) || selectHasAggregate(info.Union.Right)) {
		return true
	}
	for _, c := range info.Columns {
		if c.IsAgg || exprHasAggregate(c.ASTExpr) {
			return true
		}
	}
	return exprHasAggregate(info.HavingExpr)
}

func exprHasAggregate(n plansql.Node) bool {
	found := false
	plansql.RewriteExpr(n, func(x plansql.Node) (plansql.Node, bool) {
		switch v := x.(type) {
		case *plansql.FuncCallNode:
			if plansql.IsAggregate(v.Name) {
				found = true
				return x, true
			}
		case *plansql.WindowFuncNode, *plansql.SubqueryNode, *plansql.ExistsNode:
			// A window call is not an aggregate here, and a subquery is its
			// own block.
			return x, true
		}
		return nil, false
	})
	return found
}

// outerJoinNullableSideNames reports a join that puts a relation naming `want`
// on a NULL-EXTENDED side: the right of a LEFT join, the left of a RIGHT join,
// either side of a FULL join. `r LEFT JOIN t` keeps r on the preserved side,
// which PostgreSQL answers.
func outerJoinNullableSideNames(info *plansql.SelectInfo, want string) bool {
	if info == nil {
		return false
	}
	if info.Union != nil && (outerJoinNullableSideNames(info.Union.Left, want) || outerJoinNullableSideNames(info.Union.Right, want)) {
		return true
	}
	refNames := func(t *plansql.TableRef) bool {
		if t == nil {
			return false
		}
		if strings.HasPrefix(t.Name, "(") {
			sub, err := t.SubSelect()
			return err == nil && plansql.SelectNamesRelation(sub, want)
		}
		return strings.EqualFold(strings.TrimSpace(t.Name), want)
	}
	rightOf := func(j *plansql.JoinInfo) *plansql.TableRef {
		if j.RightTableRef != nil {
			return j.RightTableRef
		}
		return &plansql.TableRef{Name: j.RightTable}
	}
	for i := range info.Joins {
		j := &info.Joins[i]
		typ := strings.ToLower(j.Type)
		left := false
		for t := range info.Tables {
			left = left || refNames(&info.Tables[t])
		}
		for k := 0; k < i; k++ {
			left = left || refNames(rightOf(&info.Joins[k]))
		}
		right := refNames(rightOf(j))
		switch {
		case strings.HasPrefix(typ, "full"):
			if left || right {
				return true
			}
		case strings.HasPrefix(typ, "left"):
			if right {
				return true
			}
		case strings.HasPrefix(typ, "right"):
			if left {
				return true
			}
		}
	}
	// A derived table's own joins are the same question one level down.
	for i := range info.Tables {
		if sub, err := info.Tables[i].SubSelect(); err == nil && outerJoinNullableSideNames(sub, want) {
			return true
		}
	}
	for i := range info.Joins {
		if ref := info.Joins[i].RightTableRef; ref != nil {
			if sub, err := ref.SubSelect(); err == nil && outerJoinNullableSideNames(sub, want) {
				return true
			}
		}
	}
	return false
}

// fromReferenceCount counts the FROM items naming `want`, through derived
// tables and set-operation arms.
func fromReferenceCount(info *plansql.SelectInfo, want string) int {
	if info == nil {
		return 0
	}
	n := 0
	if info.Union != nil {
		n += fromReferenceCount(info.Union.Left, want) + fromReferenceCount(info.Union.Right, want)
	}
	count := func(t *plansql.TableRef) int {
		if t == nil {
			return 0
		}
		if strings.HasPrefix(t.Name, "(") {
			sub, err := t.SubSelect()
			if err != nil {
				return 0
			}
			return fromReferenceCount(sub, want)
		}
		if strings.EqualFold(strings.TrimSpace(t.Name), want) {
			return 1
		}
		return 0
	}
	for i := range info.Tables {
		n += count(&info.Tables[i])
	}
	for i := range info.Joins {
		ref := info.Joins[i].RightTableRef
		if ref == nil {
			ref = &plansql.TableRef{Name: info.Joins[i].RightTable}
		}
		n += count(ref)
	}
	return n
}
