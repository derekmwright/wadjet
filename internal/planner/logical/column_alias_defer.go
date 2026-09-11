package logical

import (
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Apply column-alias lists to LEADING outputs positionally only after star expansion.
// The builder wraps the body in a star Project carrying the deferred list;
// ExpandStarProjections establishes width, then plansql.OverlayColumnAliases renames it.
// Never guess width (ADR-0012). Optimize cannot return errors, so keep failed markers:
// RefuseUnappliedColumnAliasLists refuses at BOTH plan entries, with 42P10 for an
// overlong list and 0A000 for a star expansion declined, including bare star over join
// (ADR-0012, #810). Dropping the list would make renamed references read NULL.
// See docs/internals/deferred-star-column-alias-lists.md for the design.

// deferColumnAliasesOverStar wraps plan in a Project carrying the star and
// records aliases on it, for a body whose SELECT list holds a star this layer
// cannot count. It reports false — and returns plan untouched — when the body
// has no star, which is the case the callers' own appliers already handle.
func deferColumnAliasesOverStar(plan *Node, info *plansql.SelectInfo,
	aliases []string, relName, kind string) (*Node, bool) {
	if plan == nil || info == nil || len(aliases) == 0 {
		return plan, false
	}
	cols := info
	for cols.Union != nil {
		cols = cols.Union.Left
	}
	star := false
	for _, c := range cols.Columns {
		if c.Star {
			star = true
			break
		}
	}
	if !star {
		return plan, false
	}
	wrap := NewProject(plan, []Projection{{
		Expr: "*", Column: "*", ASTExpr: &plansql.StarNode{},
	}})
	wrap.DeferredColumnAliases = append([]string(nil), aliases...)
	wrap.DeferredAliasRelation = relName
	wrap.DeferredAliasKind = kind
	return wrap, true
}

// ApplyDeferredColumnAliases renames the leading output columns of every node
// carrying a deferred column-alias list, once the star above it has expanded.
//
// A node whose star did NOT expand, and one whose list is LONGER than the
// expanded width, both KEEP their wrapper and their marker, so
// RefuseUnappliedColumnAliasLists can raise the right refusal for each.
func ApplyDeferredColumnAliases(n *Node) *Node {
	if n == nil {
		return n
	}
	for i, child := range n.Children {
		n.Children[i] = ApplyDeferredColumnAliases(child)
	}
	if len(n.DeferredColumnAliases) == 0 || n.Type != NodeProject {
		return n
	}
	if HasStarProjection(n) {
		// The expansion DECLINED this star — a bare `*` over a join, whose
		// column set the planner refuses to guess (ADR-0012, #810). The
		// wrapper and its marker stay: `RefuseUnappliedColumnAliasLists` turns
		// them into one refusal, because the alternative is what this shape
		// did before — the list silently dropped, and every reference to a
		// name it renames TO reading NULL for a query PostgreSQL answers.
		return n
	}
	visible := VisibleProjections(n.Projections)
	if len(n.DeferredColumnAliases) > len(visible) {
		return n // RefuseUnappliedColumnAliasLists raises 42P10
	}
	for i, name := range n.DeferredColumnAliases {
		for j := range n.Projections {
			if n.Projections[j] != visible[i] {
				continue
			}
			n.Projections[j].Alias = name
			n.Projections[j].PublishedName = name
			break
		}
	}
	n.DeferredColumnAliases = nil
	return n
}

// RefuseUnappliedColumnAliasLists raises the refusal for a column-alias list
// over a `SELECT *` body that ApplyDeferredColumnAliases could not apply, in
// the two ways it can fail.
//
// A list LONGER than the expanded width is PostgreSQL's own 42P10 — the two
// appliers in builder.go raise the identical sentence for every other
// spelling, and this is that refusal a pass later because the width is a pass
// later. A list over a star the expansion DECLINED is 0A000: the relation's
// column set is not knowable, so the rename cannot be made truthfully, and
// dropping it silently is what made every reference to a renamed name read
// NULL for a query PostgreSQL answers.
func RefuseUnappliedColumnAliasLists(n *Node) error {
	if n == nil {
		return nil
	}
	if len(n.DeferredColumnAliases) > 0 {
		if HasStarProjection(n) {
			return sqlerr.New("0A000",
				"%s %q renames columns of a `SELECT *` whose column list the planner "+
					"cannot count — a star over a join, or over a derived table whose own "+
					"FROM is a join, is left unexpanded, because guessing its column set "+
					"would silently change which columns the query returns. Name the "+
					"columns in the subquery",
				n.DeferredAliasKind, n.DeferredAliasRelation)
		}
		return sqlerr.New("42P10",
			"%s %q has %d columns available but %d columns specified",
			n.DeferredAliasKind, n.DeferredAliasRelation,
			len(VisibleProjections(n.Projections)), len(n.DeferredColumnAliases))
	}
	for _, child := range n.Children {
		if err := RefuseUnappliedColumnAliasLists(child); err != nil {
			return err
		}
	}
	return nil
}
