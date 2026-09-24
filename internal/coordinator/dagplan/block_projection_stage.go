// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// starReadBlockProjections is the set of derived-block Project nodes a STAR
// reads by position: no Project stands between the root and the block, a JOIN
// does, and the block's projection is not already the list its stage emits.
//
// A JOIN must be on the path because a block that feeds the root directly IS
// the statement's output projection — physical.PlanContext.FindOutputProjectionNode answers it and
// the gather already projects it, which is why `SELECT * FROM (SELECT
// order_id, order_id AS oid FROM lat_item) s` has always been right.
func starReadBlockProjections(root *logical.Node) map[*logical.Node]blockDivergence {
	out := map[*logical.Node]blockDivergence{}
	var walk func(n *logical.Node, projected, joined bool)
	walk = func(n *logical.Node, projected, joined bool) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeProject {
			if d := blockProjectionLeavesItsStream(n); !projected && joined && d != blockAgrees {
				out[n] = d
			}
			// A STAR'S OWN EXPANSION IS THE STAR, not a named list over it.
			// `logical.StarJoinArms` marks the projection minted to publish a
			// bare `*` over a join in the FROM clause's order (ADR-0026 §9),
			// and its items are the arms' columns and nothing else — so the
			// block below is still read BY POSITION and still owes its own
			// list. Counting it as a Project un-marked every block under a
			// star join: the stage went back to shipping the stream, the
			// star's item resolved through the rename to the SOURCE column
			// (`c_str` for `c_str AS v`), and an ORDER BY on the block's
			// published `v` then named a column no stage emitted — loud, at
			// dispatch, on a query the single-process path answers.
			projected = projected || !n.StarJoinArms
		}
		if n.Type == logical.NodeJoin {
			joined = true
		}
		// A SET OPERATION NAMES ITS ARMS. `UNION`, `INTERSECT` and `EXCEPT`
		// publish the operation's own result columns and project every arm
		// onto them, so an arm's `Project` is not a relation any star reads —
		// it is an input to one. Marking an arm published its list onto the
		// arm's own stage, and the set op above then read columns that were no
		// longer there: `(SELECT order_id, ARRAY[amount] AS a FROM lat_item
		// UNION ALL …)` answered `[<nil>]` for `[50]`. The arms are treated
		// exactly as a Project above them would be.
		if n.Type == logical.NodeUnion || n.Type == logical.NodeIntersect ||
			n.Type == logical.NodeExcept {
			projected = true
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
// A RESERVED SLOT counts on neither side. The lateral lowering mints its
// correlation key into the block's list AND into the stream below it, and the
// join drops it again by POSITION on the side it built (ADR-0026 §3c) — so it
// is neither a column the block publishes nor one the stream owes, and
// counting it would call a block that IS its stream a divergence. Where the
// projection is materialized the join's drop reads its ordinal from the
// projection instead of from the stream (stageHiddenPositions).
func blockProjectionLeavesItsStream(p *logical.Node) blockDivergence {
	if p == nil || p.SecurityBarrier || len(p.Children) != 1 ||
		logical.HasStarProjection(p) || len(p.Projections) == 0 {
		return blockAgrees
	}
	names := localPlanFacts.EmittedColumnNames(p)
	if len(names) == 0 {
		return blockAgrees
	}
	stream := blockStreamNames(p)
	if len(stream) == 0 {
		// A stream this pass cannot state says nothing, exactly as
		// lateralProjectionNotInStream declines rather than guessing.
		return blockAgrees
	}
	// A RESERVED SLOT is compared on neither side. The lowering minted it
	// into both lists and the join drops it again; counting it would report a
	// divergence for a block that publishes exactly its stream.
	user := func(in []string) []string {
		out := in[:0:0]
		for _, n := range in {
			if !strings.HasPrefix(strings.ToLower(localPlanFacts.BlockBareName(n)), "__") {
				out = append(out, n)
			}
		}
		return out
	}
	names, stream = user(names), user(stream)
	// A KEY THE BLOCK MATERIALIZED FOR ITS OWN `ORDER BY` is carried by the
	// stream because the SORT below reads it, and no pruning can take it away
	// — which is exactly what the narrowing exemption below assumes. The block
	// re-projects to its visible list above that sort
	// (logical.dropBlockHiddenSlots, #991); on the DAG a Project emits no
	// stage, so without this mark the star reads the stream and the sort key's
	// SOURCE column rides out beside the block's own (`amount` beside
	// `order_id, product`).
	if blockSortCarriesAMaterializedKey(p) {
		return blockIntroduces
	}
	// NARROWING IS THE WEAK CLASS, and the difference is what happens when the
	// publish declines rather than whether the block is looked at. A block that publishes FEWER columns than
	// the stream — every one of them the stream's, once, and no name of its
	// own — is answered by the column pruning that already runs: the star sees
	// the narrowed list on every arm and always did. Marking it anyway cost an
	// OpProject on every such block and, where the projection could not then be
	// published, took a query that was RIGHT off the DAG (round-1 B1: a CTE
	// referenced twice, whose ORDER BY the local pipeline gets wrong).
	//
	// What the star really cannot see is a name the block INTRODUCES — a
	// rename, a computed item, an alias over an aggregate — or one it publishes
	// TWICE, because one stream column cannot answer to it twice.
	have := make(map[string]bool, len(stream))
	for _, s := range stream {
		have[strings.ToLower(localPlanFacts.BlockBareName(s))] = true
	}
	// WHAT THE STREAM CARRIES IS MEASURED, never inferred from the producer's
	// KIND. Round 3 read "a computed item over a scan, a window or a join is
	// materialized by absorbComputedSubqueryProjection, so it is not
	// introduced" — an allowlist, and every allowlist this arc wrote grew a
	// hole: a decorrelated LATERAL answers that test and does NOT get the
	// absorb's benefit (the star then read the correlation columns), and a
	// set-op arm fails it and did not need to be marked at all. The question
	// is only whether the stream beneath this block carries a column of this
	// name, and it is asked of the stream.
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		bare := strings.ToLower(localPlanFacts.BlockBareName(name))
		if bare == "" || seen[bare] || !have[bare] {
			return blockIntroduces
		}
		seen[bare] = true
	}
	if len(names) != len(stream) {
		return blockIntroduces
	}
	return blockAgrees
}

// blockDivergence compares the block's projection with its actual stream.
// blockAgrees requires the same names, once each. blockIntroduces covers a
// missing name, a duplicate publication or extra stream columns.
// Publish every differing projection; if it cannot be published, REFUSE and
// route it. Leaving it alone would let a star read the wrong relation.
type blockDivergence int

const (
	blockAgrees blockDivergence = iota
	blockIntroduces
)

// blockSortCarriesAMaterializedKey reports whether the block re-projects over
// a SORT of its own whose key the block does not publish — the shape
// `logical.dropBlockHiddenSlots` builds, and the one case where a block that
// publishes FEWER columns than its stream is not answered by column pruning.
func blockSortCarriesAMaterializedKey(p *logical.Node) bool {
	sorted := false
	for cur := p.Children[0]; cur != nil; {
		switch cur.Type {
		case logical.NodeSort:
			sorted = true
		case logical.NodeProject:
			if sorted && logical.HasHiddenProjection(cur.Projections) {
				return true
			}
			// A BLOCK OVER A BLOCK. The sort that materialized the key may be
			// one projection deeper — `(SELECT y.a FROM (SELECT a FROM t ORDER
			// BY b LIMIT 3) y)` — and stopping at the first Project left the
			// DAG taking the narrowing exemption and reading the stream, which
			// still carries the key's SOURCE column: six columns where the
			// single-process arms and PostgreSQL publish five (#1076). A
			// projection that carries no hidden column of its own re-publishes
			// what is under it, so the walk continues through it; one that
			// does IS the block this question is about, and it was answered
			// above.
			if logical.HasHiddenProjection(cur.Projections) {
				return false
			}
		case logical.NodeLimit, logical.NodeDistinct:
		default:
			return false
		}
		if len(cur.Children) != 1 {
			return false
		}
		cur = cur.Children[0]
	}
	return false
}

// blockStreamNames is what the STAGE under this block's projection emits: the
// first node below it that is not a Project, because a Project emits no stage.
func blockStreamNames(n *logical.Node) []string {
	for cur := n; cur != nil; {
		if cur.Type != logical.NodeProject {
			return localPlanFacts.EmittedColumnNames(cur)
		}
		if len(cur.Children) != 1 {
			return nil
		}
		cur = cur.Children[0]
	}
	return nil
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
// exactly as it was, so the shape keeps whatever disposition it had — and the
// CALLER records only the blocks that were really published, because the
// declaration and the join's key binding read that set and a block marked
// published but not materialized is the ADR-0010 disagreement again.
func publishBlockProjection(node *logical.Node, stages *[]Stage, from int,
	published map[*logical.Node]bool, subqueryDecl func(string) (parquet.Column, bool)) bool {
	if from < 0 || from >= len(*stages) {
		return false
	}
	cols, ok := localPlanFacts.BlockPublishedColumns(node, published, subqueryDecl)
	if !ok || len(cols) == 0 {
		return false
	}
	target := len(*stages) - 1
	specs := make([]physical.ProjectExprSpec, len(cols))
	for i, c := range cols {
		specs[i] = physical.ProjectExprSpec{
			Expr: c.Expr, Name: c.Name, Type: c.Decl.Type, TypeKnown: c.DeclKnown,
			Precision: c.Decl.Precision, Scale: c.Decl.Scale, Fields: c.Decl.Fields,
			ElementType: c.Decl.ElementType,
		}
	}
	respelled, ok := respellSpecsOverProducerOutput(*stages, target, specs)
	if !ok || !specsResolveAgainstStageOutput(*stages, target, respelled) {
		return false
	}
	if !orderingSurvivesAProjectStage(*stages, target, respelled) {
		return false
	}
	s := &(*stages)[target]
	if stageAppliesProjection(s) && len(s.ProjectExprs) == 0 &&
		len(s.SecurityProjectExprs) == 0 {
		s.ProjectExprs = respelled
		return true
	}
	keys := (*stages)[target].SortKeys
	*stages = insertProjectStageAbove(*stages, target, respelled)
	carryOrderingOntoProjectStage(*stages, len(*stages)-1, keys)
	return true
}

// materializedBlockUnder is the marked block at or below n, reached through the
// nodes that emit no stage of their own, or nil when this side carries none.
//
// A side's root is not always the block: a Filter or a Limit the optimizer
// left above it emits no stage either, so the projection the stage publishes
// is still the first Project below them.
func materializedBlockUnder(n *logical.Node, published map[*logical.Node]bool) *logical.Node {
	for cur := n; cur != nil && len(cur.Children) == 1; cur = cur.Children[0] {
		if published[cur] {
			return cur
		}
		// A SORT of the block's OWN is on this path too. The lateral lowering
		// puts the block's `ORDER BY` between its projection and the join, and
		// stopping here left the join reading the positions off
		// `physical.PlanContext.DeclaredJoinSchema`'s walk — which descends to the AGGREGATE and
		// answers its order (`product, __key_0`) where the stage publishes the
		// projection's (`__key_0, p`). The minted slot was then looked for at
		// the wrong ordinal and rode out to the client beside the block's own
		// column on both DAG arms (#1020). A Sort changes neither the columns
		// nor their order, so the block below it is still the relation this
		// side publishes.
		if cur.Type != logical.NodeFilter && cur.Type != logical.NodeLimit &&
			cur.Type != logical.NodeProject && cur.Type != logical.NodeDistinct &&
			cur.Type != logical.NodeSort {
			return nil
		}
	}
	return nil
}

// stageIndexByID is the index of the stage with this ID, or ok=false.
func (p *StagePlanner) stageIndexByID(stages []Stage, id string) (int, bool) {
	for i := range stages {
		if stages[i].ID == id {
			return i, true
		}
	}
	return 0, false
}

// markStarReadBlocks records every candidate block in this subtree as
// published, for a subtree whose stages another walk already emitted.
func markStarReadBlocks(n *logical.Node, candidates map[*logical.Node]blockDivergence,
	published map[*logical.Node]bool) {
	if n == nil {
		return
	}
	if candidates[n] != blockAgrees {
		published[n] = true
	}
	for _, c := range n.Children {
		markStarReadBlocks(c, candidates, published)
	}
}

// referenceIntoPublishedBlock reports whether a QUALIFIED reference names a
// column of a block whose projection a stage MATERIALIZES.
//
// Such a reference is already the name the stream carries, so resolving it
// back through the block's rename to a SOURCE column points at a column that
// projection renamed away — the rule plan.go's gather loop states for a name
// some stage already materializes, asked structurally rather than by name so
// that a qualified reference and a bare stream column still meet.
//
// It is the SELECT list's half of §7's first consequence: `resolveShuffleKey`
// and `resolveJoinNeededColumns` already stop at a materialized block and take
// the PUBLISHED name (ADR-0026 §7), and a star's own expansion is a consumer
// of exactly the same kind — `SELECT * FROM d JOIN (SELECT c_str AS v FROM t)
// s ON …` publishes `s.v`, the block's stage emits `v`, and the spec attached
// to the sort stage read `c_str`: loud, at dispatch, on one DAG arm.
func (p *StagePlanner) referenceIntoPublishedBlock(ref string, child *logical.Node) bool {
	if p == nil || len(p.publishedBlocks) == 0 || child == nil {
		return false
	}
	dot := strings.LastIndexByte(ref, '.')
	if dot <= 0 || dot == len(ref)-1 {
		return false
	}
	scope := p.PlanContext.RelationScopeSubtree(child, ref[:dot])
	if scope == nil {
		return false
	}
	return materializedBlockUnder(scope, p.publishedBlocks) != nil
}
