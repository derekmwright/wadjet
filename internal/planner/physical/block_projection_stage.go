package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
	names := emittedColumnNames(p)
	if len(names) == 0 {
		return blockAgrees
	}
	// A BLOCK WHOSE OWN ORDER BY WAS MATERIALIZED is left alone entirely. Its
	// list carries a `__sortkey_N` the sort below still needs, and a
	// projection that publishes it puts a name no query can spell on the wire
	// while one that drops it takes the key away from the operator that reads
	// it. Neither is an improvement on what the engine already does, so the
	// block is not a candidate at all: `SELECT * FROM o JOIN (SELECT order_id,
	// product FROM item ORDER BY amount LIMIT 3) s` publishes the scan's
	// `amount` beside the block's two columns exactly as it did at v0.18.60.
	sortKeyFamily := plansql.ReservedSlotFamily(plansql.SlotName(plansql.SlotSortKey, 0))
	for _, name := range names {
		if plansql.ReservedSlotFamily(strings.ToLower(blockBareName(name))) == sortKeyFamily {
			return blockAgrees
		}
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
			if !strings.HasPrefix(strings.ToLower(blockBareName(n)), "__") {
				out = append(out, n)
			}
		}
		return out
	}
	names, stream = user(names), user(stream)
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
		have[strings.ToLower(blockBareName(s))] = true
	}
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		bare := strings.ToLower(blockBareName(name))
		if bare == "" || seen[bare] || !have[bare] {
			return blockIntroduces
		}
		seen[bare] = true
	}
	if len(names) != len(stream) {
		return blockNarrows
	}
	return blockAgrees
}

// blockDivergence is HOW a block's projection differs from its stream, and the
// two classes are not degrees of confidence — they are different questions
// about what the star would see without this pass.
//
//   - blockIntroduces: a name the stream does not carry (a rename, a computed
//     item, an alias over an aggregate) or one published TWICE. The star sees
//     the wrong relation, always, so a block this pass cannot publish is
//     REFUSED and routed — it was wrong or loud before the pass existed.
//   - blockNarrows: every published name is the stream's, once, and the stream
//     carries MORE. Publishing is an improvement — column pruning removes most
//     of the extras but not the ones something else keeps alive, and a lateral
//     whose block is a bare `SELECT amount` published the scan's `order_id`
//     beside it. But NOT publishing is exactly what the engine did before, so
//     a narrowing block the pass cannot carry is left alone and never routed.
//   - blockIntroduces: a name the stream does not carry (a rename, a computed
//     item, an alias over an aggregate) or one published TWICE. The star sees
//     the wrong relation, always, so a block this pass cannot publish IS
//     refused and routed — it was wrong or loud before the pass existed.
//
// Both classes are MARKED; the class decides only what happens when the
// publish DECLINES. That asymmetry is the whole rule: the route is not
// answer-preserving — the coordinator-local pipeline's ORDER BY is wrong for
// shapes the DAG gets right — so it may carry only what was already wrong or
// loud. Refusing a narrowing block took a twice-referenced CTE that answered
// PostgreSQL exactly off the DAG (round-1 B1); not marking one at all reopened
// the lateral shapes round 1 closed.
type blockDivergence int

const (
	blockAgrees blockDivergence = iota
	blockNarrows
	blockIntroduces
)

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

// blockColumn is ONE column the block publishes, in the block's own order:
// the name a consumer above reads it under, the expression the stage's own
// input evaluates it from, and the type it is declared as.
//
// The three travel together because two of them are read by different passes
// and a disagreement between those passes is an ADR-0010 refusal, not a
// cosmetic one: the fragment computes the column from Expr and declares it
// from Decl, and an empty side of the same join declares it from Decl alone.
// Deriving both from one walk is what makes "one stage's files describe one
// relation" true by construction rather than by two rules agreeing.
type blockColumn struct {
	Name string
	Expr string
	Decl parquet.Column
	// DeclKnown is carried beside Decl because parquet.TypeBool IS the zero
	// value: `COUNT(*) = 0 AS n` reads as "no declaration" through a
	// `Type != 0` test, projectOpFromSpecs then drops the type off the wire,
	// and the worker's buildSelectProjection guesses STRING for a column that
	// is a bool (#445). The empty side of the same join declared BOOL, and
	// the two files disagreed under ADR-0010.
	DeclKnown bool
}

// blockPublishedColumns is the relation a derived block publishes, or ok=false
// when the plan cannot state it.
//
// ok=false is a real answer and not a shrug: a column with no plan-time type
// would be COMPUTED by the fragment and MISSING from the empty side's
// declaration, which is the width disagreement ADR-0010 refuses. Declining
// leaves the plan exactly as it was.
func blockPublishedColumns(p *logical.Node, published map[*logical.Node]bool) ([]blockColumn, bool) {
	if p == nil || len(p.Children) != 1 || len(p.Projections) == 0 {
		return nil, false
	}
	// The STREAM's own declaration, typed by the walk that owns each
	// producer's rule — the aggregate arm types a group key and an AggSpec,
	// the scan arm reads the catalog annotation. A minted correlation slot and
	// an `__agg_N` have a type only their producer can state, and a second
	// rule for them is the disagreement ADR-0026 exists to prevent.
	stream := declaredJoinSchema(p.Children[0], nil, published)
	byName := make(map[string]parquet.Column, len(stream))
	for _, col := range stream {
		byName[strings.ToLower(blockBareName(col.Name))] = col
	}
	// A COMPUTED item is typed against what the stage's input EMITS, not
	// against what the block's child reads: above an aggregate the operands
	// are `__agg_N` and a minted slot, and typing `COUNT(*) + 1` against the
	// scan's columns answers nothing at all.
	decls := inputColDecls(p.Children[0])
	if decls.types == nil {
		decls.types = map[string]parquet.TypeID{}
	}
	strictInt := strictIntArithCols(p.Children[0])
	if strictInt == nil {
		strictInt = map[string]bool{}
	}
	for _, col := range stream {
		lc := strings.ToLower(blockBareName(col.Name))
		if _, ok := decls.types[lc]; !ok {
			decls.types[lc] = col.Type
			if col.Type == parquet.TypeDecimal {
				if decls.dec == nil {
					decls.dec = map[string]logical.DecimalMeta{}
				}
				decls.dec[lc] = logical.DecimalMeta{
					Precision: col.Precision, Scale: col.Scale,
				}
			}
		}
		// An INTEGER the producer emits keeps integer arithmetic exact above
		// it, the same hint absorbComputedSubqueryProjection passes when it
		// materializes a computed column into a scan fragment (#297, #445):
		// without it `COUNT(*) + 1` declares FLOAT64 where every other path
		// answers a bigint (ADR-0024).
		if col.Type == parquet.TypeInt32 || col.Type == parquet.TypeInt64 {
			strictInt[lc] = true
		}
	}
	// VISIBLE items only. A hidden `__sortkey_N` is the planner's own — a
	// materialized ORDER BY term the block's SELECT list does not carry — and
	// publishing it put a reserved name on the wire that no query can spell
	// (round-1 P3). It is the same list extractOutputRenames walks for the
	// statement's own projection, and for the same reason.
	items := logical.VisibleProjections(p.Projections)
	sortKeyFamily := plansql.ReservedSlotFamily(plansql.SlotName(plansql.SlotSortKey, 0))
	out := make([]blockColumn, 0, len(items))
	for _, pr := range items {
		if pr.IsAgg {
			// An aggregate SELECT item is computed by the aggregate stage,
			// not by a projection above it.
			return nil, false
		}
		name := pr.Alias
		if name == "" {
			name = pr.Column
		}
		if name == "" {
			name = strings.TrimSpace(pr.Expr)
		}
		bare := strings.ToLower(blockBareName(name))
		if bare == "" {
			return nil, false
		}
		// A MATERIALIZED ORDER BY TERM is the planner's own and is not part of
		// the relation the block publishes. `VisibleProjections` trims the
		// ones the builder flagged hidden; a block whose own `ORDER BY amount
		// LIMIT 3` was materialized carries one as an ordinary item, and
		// publishing it put `__sortkey_0` on the wire where no query can spell
		// it. The correlation slot is NOT trimmed here — the join keys on it
		// and drops it by position (ADR-0026 §3c).
		if plansql.ReservedSlotFamily(bare) == sortKeyFamily {
			continue
		}
		if pr.ASTExpr != nil && !isSimpleColRefForRename(pr.ASTExpr) {
			// The CONFIDENCE, not the TypeID: parquet.TypeBool is the zero
			// value, so `COUNT(*) = 0 AS n` reads as "no type at all" through
			// a `t == 0` test and the whole block goes unpublished — which is
			// the ADR-0010 refusal again, for a column that was decided.
			decl, conf := inferProjectionDeclTypeConf(pr.ASTExpr, 0, strictInt, decls)
			if conf == expr.Undecided {
				// SQL's `unknown` DECIDES. A bare NULL select item names no
				// type and produces no value, and PostgreSQL declares it
				// `text` (OID 25) — which is what this engine publishes for
				// `SELECT NULL AS c` on every arm already. Declining it left
				// `SELECT order_id, NULL AS c` in the residue and took a query
				// that was RIGHT off the DAG (round-1 B2), onto a path that is
				// not answer-preserving.
				//
				// Asked of the LITERAL rather than of the inference's
				// `Untyped` flag, which this walk does not reach for a bare
				// `NULL` (it answers the zero DeclType). Anything else
				// undecided still declines: a container built from an
				// aggregate produces a value at runtime, at its own type, and
				// text is not it.
				if !astIsBareNull(pr.ASTExpr) {
					return nil, false
				}
				decl = expr.DeclType{ID: parquet.TypeString}
			}
			t, prec, scale := declTypeParts(decl)
			out = append(out, blockColumn{
				Name: name, Expr: pr.ASTExpr.String(),
				Decl: parquet.Column{
					Name: name, Type: t, Precision: prec, Scale: scale, Nullable: true,
				},
				DeclKnown: true,
			})
			continue
		}
		// A BARE item is read under the spelling the STREAM carries. The
		// source first, because that is what an ordinary rename reads; then
		// the item's OWN name, because a MINTED correlation slot's item is
		// spelled `__key_0` and reads `order_id`, and the aggregate below
		// emits the slot rather than the source it was computed from.
		src := pr.Column
		if src == "" {
			src = strings.TrimSpace(pr.Expr)
		}
		spelling := strings.ToLower(blockBareName(src))
		col, ok := byName[spelling]
		if !ok {
			spelling = bare
			col, ok = byName[spelling]
		}
		if !ok {
			return nil, false
		}
		col.Name, col.Nullable = name, true
		out = append(out, blockColumn{Name: name, Expr: spelling, Decl: col, DeclKnown: true})
	}
	return out, true
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
	published map[*logical.Node]bool) bool {
	if from < 0 || from >= len(*stages) {
		return false
	}
	cols, ok := blockPublishedColumns(node, published)
	if !ok || len(cols) == 0 {
		return false
	}
	target := len(*stages) - 1
	specs := make([]ProjectExprSpec, len(cols))
	for i, c := range cols {
		specs[i] = ProjectExprSpec{
			Expr: c.Expr, Name: c.Name, Type: c.Decl.Type, TypeKnown: c.DeclKnown,
			Precision: c.Decl.Precision, Scale: c.Decl.Scale,
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
		if cur.Type != logical.NodeFilter && cur.Type != logical.NodeLimit &&
			cur.Type != logical.NodeProject && cur.Type != logical.NodeDistinct {
			return nil
		}
	}
	return nil
}

// declaredBlockSchema is the block's published relation as a plan-time column
// list — the declaration a side of a join that delivers NO BATCH AT ALL is
// shaped by.
//
// A stage's files describe ONE relation (ADR-0010), and once a block is
// materialized that relation is the PROJECTION. declaredJoinSchema's ordinary
// walk descends past a Project and declares the scan's or the aggregate's own
// columns, so an empty build task wrote the stream's columns beside sibling
// files carrying the projection's: `declares 3 columns where an earlier file
// of the same stage input declared 4`, and where the widths happened to agree,
// `names column 3 "n" where an earlier file ... named it "__agg_0"`. That is
// #980's own sentence, and it is what the shape does the moment the lateral
// route stops standing in front of it.
func declaredBlockSchema(p *logical.Node, wantSet map[string]bool,
	published map[*logical.Node]bool) []parquet.Column {
	cols, ok := blockPublishedColumns(p, published)
	if !ok {
		return nil
	}
	out := make([]parquet.Column, 0, len(cols))
	for _, c := range cols {
		if len(wantSet) > 0 && !wantSet[strings.ToLower(blockBareName(c.Name))] {
			continue
		}
		out = append(out, c.Decl)
	}
	return out
}

// astIsBareNull reports whether this select item is the literal `NULL` and
// nothing else — SQL's `unknown`, which PostgreSQL declares as text.
func astIsBareNull(n plansql.Node) bool {
	for {
		switch t := n.(type) {
		case *plansql.ParenNode:
			n = t.Inner
		case *plansql.Lit:
			return t.Kind == plansql.LitNull
		default:
			return false
		}
	}
}

// stageIndexByID is the index of the stage with this ID, or ok=false.
func (p *Planner) stageIndexByID(stages []Stage, id string) (int, bool) {
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
