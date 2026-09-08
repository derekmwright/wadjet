package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// A DERIVED BLOCK A STAR READS IS A RELATION, AND SOME STAGE PUBLISHES IT
// (#984).
//
// A Project emits no stage (walkStages' `default:` arm). On the DAG a derived
// table's SELECT list is therefore not a relation of its own: the Aggregate or
// the Scan below it is what materializes, and every consumer above compensates
// per consumer — resolveShuffleKey, resolveAggInputName, resolveSortKeyColumn
// and the gather's OutputRenames each map the name the query wrote back to the
// name the stream carries.
//
// A STAR has no name to map. It reads the stream BY POSITION, so it publishes
// whatever the stage below the block emits:
//
//	SELECT * FROM lat_ord o
//	  JOIN (SELECT order_id, order_id AS oid FROM lat_item) s ON s.order_id = o.id
//	PostgreSQL        id, customer, total, order_id, oid
//	the stage's stream            …,       order_id        ← `oid` is not a column
//
// Four ways a block's projection leaves its stream behind, all four measured
// as silently wrong answers on both DAG arms at v0.18.60 and all four one
// question — is the projection, by position, the list the stage emits:
//
//   - a source column published TWICE (`order_id, order_id AS oid`): the
//     stream carries one of it;
//   - a RENAME (`order_id AS k`): the stream carries the source name, so the
//     client is handed a column it never asked for under a name the query
//     does not use;
//   - an ALIAS OVER AN AGGREGATE (`CAST(COUNT(*) AS VARCHAR) AS n`): the
//     aggregate stage emits its own `__agg_0` beside the computed `n`, and
//     the reserved slot reaches the client;
//   - a COMPUTED item (`amount * 2 AS d`): absorbComputedSubqueryProjection
//     is deliberately ADDITIVE, so the stream carries the computed column AND
//     the source it was computed from.
//
// The fix is the one the ADR names: the stage that materializes the block
// publishes the BLOCK'S PROJECTION — by position, under the block's own names
// — so the relation above the block is the relation the query wrote. Nothing
// predicts a name here: the projection becomes a real OpProject through
// Stage.ProjectExprs, exactly the machinery attachScanSelectProjections uses
// for the statement's own SELECT list, and the join operator's own naming rule
// then produces the star's columns from a relation that is already right.
//
// SCOPED TO A STAR, and the scope is the whole of why this is not a wider
// change. A named SELECT list over every one of these blocks answers
// PostgreSQL on all four arms today, because each consumer resolves its own
// column; materializing under those is churn with no defect to fix. The test
// is `projected` — a Project anywhere between the root and the block means the
// statement named its columns — and it is the same test
// refuseLateralProjection applies.

// starReadBlockProjections is the set of derived-block Project nodes a STAR
// reads by position: no Project stands between the root and the block, a JOIN
// does, and the block's projection is not already the list its stage emits.
//
// A JOIN must be on the path because a block that feeds the root directly IS
// the statement's output projection — findOutputProjectionNode answers it and
// the gather already projects it, which is why `SELECT * FROM (SELECT
// order_id, order_id AS oid FROM lat_item) s` has always been right.
func starReadBlockProjections(root *logical.Node) map[*logical.Node]bool {
	out := map[*logical.Node]bool{}
	var walk func(n *logical.Node, projected, joined bool)
	walk = func(n *logical.Node, projected, joined bool) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeProject {
			if !projected && joined && blockProjectionLeavesItsStream(n) {
				out[n] = true
			}
			projected = true
		}
		if n.Type == logical.NodeJoin {
			joined = true
		}
		for _, child := range n.Children {
			walk(child, projected, joined)
		}
	}
	walk(root, false, false)
	return out
}

// blockProjectionLeavesItsStream reports whether this block's SELECT list is a
// different relation from the one the stage below it emits — a different
// WIDTH, a name the stream does not carry, or one name published twice.
//
// A block carrying a MINTED correlation slot is excluded: the slot is dropped
// by the join that made it, by POSITION on the side the lowering built
// (ADR-0026 §3c), and materializing the projection would move that position.
// The lateral spellings are handled where the slot is known — see
// blockProjectionIsStageable's caller.
func blockProjectionLeavesItsStream(p *logical.Node) bool {
	if p == nil || p.SecurityBarrier || len(p.Children) != 1 ||
		logical.HasStarProjection(p) || len(p.Projections) == 0 {
		return false
	}
	names := emittedColumnNames(p)
	if len(names) == 0 {
		return false
	}
	for _, name := range names {
		if strings.HasPrefix(strings.ToLower(blockBareName(name)), "__") {
			// A minted slot in the block's own list: the join drops it by
			// position and this pass must not move it.
			return false
		}
	}
	stream := blockStreamNames(p)
	if len(stream) == 0 {
		// A stream this pass cannot state says nothing, exactly as
		// lateralProjectionNotInStream declines rather than guessing.
		return false
	}
	if len(names) != len(stream) {
		return true
	}
	have := make(map[string]bool, len(stream))
	for _, s := range stream {
		have[strings.ToLower(blockBareName(s))] = true
	}
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		bare := strings.ToLower(blockBareName(name))
		if bare == "" || seen[bare] || !have[bare] {
			return true
		}
		seen[bare] = true
	}
	return false
}

// blockStreamNames is what the STAGE under this block's projection emits: the
// first node below it that is not a Project, because a Project emits no stage.
func blockStreamNames(n *logical.Node) []string {
	for cur := n; cur != nil; {
		if cur.Type != logical.NodeProject {
			return emittedColumnNames(cur)
		}
		if len(cur.Children) != 1 {
			return nil
		}
		cur = cur.Children[0]
	}
	return nil
}

// blockBareName drops a qualifier: the projection spells `s.oid` where the
// stream spells `oid`, and that is not a divergence.
func blockBareName(s string) string {
	s = strings.TrimSpace(s)
	if dot := strings.LastIndexByte(s, '.'); dot >= 0 && dot < len(s)-1 {
		return s[dot+1:]
	}
	return s
}

// publishBlockProjection makes the stages this block's subtree just emitted
// publish the block's own relation, and reports whether it could.
//
// `from` is the index the subtree's stages start at, so the TERMINAL is the
// last of them — walkStages emits children first and the node's own stage
// last. The projection lands on that terminal when its fragment runs an
// OpProject above its own operator (stageAppliesProjection), and in a
// StageProject of its own otherwise; that is the same pair of placements
// attachScanSelectProjections chooses between, for the same reason.
//
// It DECLINES rather than approximating: a spec that does not resolve against
// what the terminal emits would compute NULL for the column, which is worse
// than the missing one this pass exists to restore. A decline leaves the plan
// exactly as it was, so the shape keeps whatever disposition it had.
func publishBlockProjection(node *logical.Node, stages *[]Stage, from int) bool {
	if from < 0 || from >= len(*stages) {
		return false
	}
	target := len(*stages) - 1
	specs := blockProjectionSpecs(node)
	if len(specs) == 0 {
		return false
	}
	respelled, ok := respellSpecsOverProducerOutput(*stages, target, specs)
	if !ok || !specsResolveAgainstStageOutput(*stages, target, respelled) {
		return false
	}
	s := &(*stages)[target]
	if stageAppliesProjection(s) && len(s.ProjectExprs) == 0 &&
		len(s.SecurityProjectExprs) == 0 && orderingSurvivesAProjectStage(*stages, target, respelled) {
		s.ProjectExprs = respelled
		return true
	}
	keys := (*stages)[target].SortKeys
	if !orderingSurvivesAProjectStage(*stages, target, respelled) {
		return false
	}
	*stages = insertProjectStageAbove(*stages, target, respelled)
	carryOrderingOntoProjectStage(*stages, len(*stages)-1, keys)
	return true
}

// blockProjectionSpecs is the block's SELECT list as projection specs, by
// position, named the way the block publishes them.
//
// The EXPRESSION is the AST's own rendering rather than the text the query
// wrote, for the reason declaredJoinSchema reads the AST too: the logical
// layer has already replaced each aggregate call with a reference to the slot
// the aggregate stage emits, so `CAST(COUNT(*) AS VARCHAR)` renders as a
// computation over `__agg_0` — which is what the stage's input really carries.
func blockProjectionSpecs(node *logical.Node) []ProjectExprSpec {
	var colTypes colDecls
	var strictInt map[string]bool
	if len(node.Children) == 1 {
		colTypes = inputColDecls(node.Children[0])
		strictInt = strictIntArithCols(node.Children[0])
	}
	specs := make([]ProjectExprSpec, 0, len(node.Projections))
	for _, pr := range node.Projections {
		if pr.IsAgg {
			// An aggregate SELECT item is computed by the aggregate stage,
			// not by a projection above it.
			return nil
		}
		name := pr.Alias
		if name == "" {
			name = pr.Column
		}
		if name == "" {
			name = strings.TrimSpace(pr.Expr)
		}
		if name == "" {
			return nil
		}
		spec := ProjectExprSpec{Name: name}
		switch {
		case pr.ASTExpr != nil:
			spec.Expr = pr.ASTExpr.String()
			if !isSimpleColRefForRename(pr.ASTExpr) {
				spec.Type, spec.Precision, spec.Scale = declTypeParts(
					inferProjectionDeclType(pr.ASTExpr, 0, strictInt, colTypes))
				spec.TypeKnown = spec.Type != 0
			}
		case pr.Expr != "":
			spec.Expr = strings.ToLower(pr.Expr)
		case pr.Column != "":
			spec.Expr = pr.Column
		default:
			return nil
		}
		if spec.Expr == "" {
			return nil
		}
		specs = append(specs, spec)
	}
	return specs
}
