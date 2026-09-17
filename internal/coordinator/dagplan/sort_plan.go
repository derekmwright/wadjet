// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// sortKeySlotPosStage may use a SELECT-list position only when it addresses
// the producer's actual stream. Ordinary DAG Projects emit no stage; a single
// narrowed relation supplies the list, but joins/set operations need stronger proof.
// OutputColumns is populated only after walkStages; shape is the initial bound.
// A WRITTEN term takes a position only under the MEASURED proof, never the shape
// bound, because it is resolvable on far more queries than an ordinal (#1014).
// The duplicate_name_dag and collide_two_path gates test both sides of it.
// See docs/internals/dag-sort-select-list-positions.md for the design.
func sortKeySlotPosStage(ob logical.OrderExpr, sortNode *logical.Node, produced []Stage) int {
	if pos := localPlanFacts.SortKeySlotPos(ob, sortNode); pos != 0 {
		// A set operation carries its own proof (physical.SortInputSetOpWidth): its
		// stage publishes the result column list and nothing else, so the
		// subtree bound below — which exists because a JOIN stage emits both
		// arms' whole schemas — has nothing to say about it (#1022).
		if _, ok := localPlanFacts.SortInputSetOpWidth(sortNode.Children[0]); ok {
			return pos
		}
		if !subtreeJoinsRelations(sortNode) {
			return pos
		}
		// …unless the producer MATERIALIZED the select list, which is the one
		// case where the stream and the select list are the same list (#1003).
		if producerPublishesSelectList(produced, sortNode) {
			return pos
		}
		return 0
	}
	// A WRITTEN term binds the same SLOT an ordinal does (#1014). It is the
	// spelling one step over from #1003's, and it was the cell that arc PINNED:
	// `SELECT DISTINCT a.order_id AS amount, b.amount … ORDER BY 1, b.amount
	// DESC` publishes `amount` TWICE, so once the position is dropped the key
	// resolves by that name and `ColumnIndexFallback` answers with the FIRST
	// match — both keys bound column one and the DAG arms returned
	// `1,50 | 1,100 | …` where PostgreSQL 17 and both single-process arms
	// return `1,100 | 1,50 | …`. A total order is not one of ADR-0013's
	// nondeterminism classes.
	//
	// Only under the MEASURED proof, on both sides of the shape bound. An
	// ordinal may take the position on a subtree that joins no relations
	// because a single narrowed relation's stage carries the select list as
	// its ProjectExprs; a written term is resolvable on far more queries than
	// an ordinal is, so widening it by the same shape argument would put a
	// position on keys whose producer this layer has not looked at. What the
	// measurement answers is exactly the question the position needs —
	// does the producing stage publish the select list as the ordered prefix
	// of its own output — and it answers it the same way for both spellings
	// (ADR-0026 §8).
	pos := localPlanFacts.SortKeyWrittenSlotPos(ob, sortNode)
	if pos == 0 || !producerPublishesSelectList(produced, sortNode) {
		return 0
	}
	return pos
}

// producerPublishesSelectList proves the whole visible SELECT list is an ordered
// PREFIX of the producer's output, comparing both name and source expression.
// Producer kind alone is insufficient (ADR-0026 §8): reordered or narrowed lists
// fail, while materialized duplicate names can still be addressed by position
// (#1003). Falling back to the first matching name can lose a total-order key,
// which ADR-0013 does not permit. Unmaterialized lists retain name resolution.
// See docs/internals/materialized-select-list-prefix.md for the design.
func producerPublishesSelectList(produced []Stage, sortNode *logical.Node) bool {
	if len(produced) == 0 || sortNode == nil || len(sortNode.Children) == 0 {
		return false
	}
	child := sortNode.Children[0]
	if child == nil || child.Type != logical.NodeProject || logical.HasStarProjection(child) {
		return false
	}
	visible := logical.VisibleProjections(child.Projections)
	if len(visible) == 0 {
		return false
	}
	specs := produced[len(produced)-1].ProjectExprs
	if len(specs) < len(visible) {
		return false
	}
	for i, pr := range visible {
		src := localPlanFacts.CleanExpr(pr.Expr)
		if src == "" {
			src = pr.Column
		}
		if src == "" || !sameProjectionSource(specs[i].Expr, src) {
			return false
		}
		// The SOURCE is the identity; the NAME is the check, and a
		// projection has more than one legitimately — the resolution
		// spelling every pass inside the planner binds by, and the
		// PublishedName the client is told (ADR-0026 §2). The stage
		// materializes whichever the output owes.
		if !projectionAnswersToName(pr, specs[i].Name) {
			return false
		}
	}
	return true
}

// sameProjectionSource reports whether a stage spec's source expression and a
// logical projection's name the SAME input column.
//
// They may differ by a QUALIFIER and by nothing else: an aggregate publishes a
// group key under its stripped name (ADR-0026 §2) so the logical projection
// reads `order_id`, while the stage spec keeps the written `a.order_id`. Where
// both sides carry a qualifier they must agree on it, so two arms of a
// self-join are never taken for one another.
func sameProjectionSource(specExpr, projExpr string) bool {
	a := plansql.NormalizeIdentRef(localPlanFacts.CleanExpr(specExpr))
	b := plansql.NormalizeIdentRef(localPlanFacts.CleanExpr(projExpr))
	if a == "" || b == "" {
		return false
	}
	if strings.EqualFold(a, b) {
		return true
	}
	ab, bb := localPlanFacts.BlockBareName(a), localPlanFacts.BlockBareName(b)
	if !strings.EqualFold(ab, bb) {
		return false
	}
	// Exactly one side was bare; two different qualifiers are two columns.
	return a == ab || b == bb
}

// projectionAnswersToName reports whether name is one of the names this
// projection legitimately publishes.
func projectionAnswersToName(pr logical.Projection, name string) bool {
	if name == "" {
		return false
	}
	name = plansql.NormalizeIdentRef(name)
	for _, cand := range []string{pr.PublishedName, pr.Alias, pr.Column, localPlanFacts.CleanExpr(pr.Expr)} {
		if cand != "" && strings.EqualFold(plansql.NormalizeIdentRef(cand), name) {
			return true
		}
	}
	return false
}

// subtreeJoinsRelations reports whether a node's subtree combines two
// relations — a join or a set operation — anywhere below it.
func subtreeJoinsRelations(n *logical.Node) bool {
	if n == nil {
		return false
	}
	switch n.Type {
	case logical.NodeJoin, logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
		return true
	}
	for _, c := range n.Children {
		if subtreeJoinsRelations(c) {
			return true
		}
	}
	return false
}
