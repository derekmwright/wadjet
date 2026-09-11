package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A derived block read by a star must publish its projection by position,
// under its own names, through Stage.ProjectExprs/OpProject (#984). The join
// then names the star's columns from that relation: duplicate sources, renames,
// aggregate aliases and computed items must not expose the underlying stream.
// Scope is star-only: a Project between root and block means named columns,
// whose consumers resolve their own names (the projected test shared with
// refuseLateralProjection).
// See docs/internals/star-read-block-projections.md for the design.

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
	names := emittedColumnNames(p)
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
		bare := strings.ToLower(blockBareName(name))
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
func blockPublishedColumns(p *logical.Node, published map[*logical.Node]bool,
	subqueryDecl func(string) (parquet.Column, bool)) ([]blockColumn, bool) {
	if p == nil || len(p.Children) != 1 || len(p.Projections) == 0 {
		return nil, false
	}
	// The STREAM's own declaration, typed by the walk that owns each
	// producer's rule — the aggregate arm types a group key and an AggSpec,
	// the scan arm reads the catalog annotation. A minted correlation slot and
	// an `__agg_N` have a type only their producer can state, and a second
	// rule for them is the disagreement ADR-0026 exists to prevent.
	stream := declaredJoinSchema(p.Children[0], nil, published, subqueryDecl)
	byName := make(map[string]parquet.Column, len(stream))
	for _, col := range stream {
		byName[strings.ToLower(blockBareName(col.Name))] = col
	}
	// A COMPUTED item is typed against what the stage's input EMITS, not
	// against what the block's child reads: above an aggregate the operands
	// are `__agg_N` and a minted slot, and typing `COUNT(*) + 1` against the
	// scan's columns answers nothing at all.
	decls := inputColDecls(p.Children[0])
	// THE SCALAR-SUBQUERY RESOLVER IS PART OF THE INFERENCE, not an extra.
	// `declaredOutputSchema` hands it to the same walk for the statement's own
	// SELECT list, and without it here `(SELECT MAX(amount) FROM lat_item) AS
	// sq` fell to the STRING fallback: a zero-row `SELECT *` over a block
	// holding one declared `sq` as text where the same statement under a
	// matching predicate declares int8 and PostgreSQL declares integer. One
	// inference means one set of arguments too (round-4 B1).
	decls.subqueryDecl = subqueryDecl
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
			// ONE INFERENCE, and it is the SINGLE PATH'S. This is the call
			// `declaredJoinSchema`'s own computed-column arm makes and the
			// call `attachScanSelectProjections` makes for the statement's own
			// SELECT list: the same walk, the same `strictInt` hint, the same
			// scalar-subquery resolver, and the same STRING FALLBACK.
			//
			// A weaker second inference here is what three rounds of this arc
			// kept tripping over. It declined on `expr.Undecided` and each
			// decline became a DISPOSITION — a query the single path answers
			// with a declared type was routed off the DAG, or worse, left to
			// read the stream. There is no such thing as an item this engine
			// cannot declare: `SELECT ARRAY[c0] AS a`, an all-NULL `CASE`, a
			// bare `NULL` and `COALESCE(NULL, NULL)` all reach a client with
			// an OID today (25, 25, 25, 701), and a scalar-subquery item
			// reaches it with its own (20, through `subqueryDecl`). Whatever
			// that walk answers is what this stage publishes, so the two paths
			// describe one relation by construction.
			t, prec, scale := declTypeParts(
				inferProjectionDeclType(pr.ASTExpr, parquet.TypeString, strictInt, decls))
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
	published map[*logical.Node]bool, subqueryDecl func(string) (parquet.Column, bool)) bool {
	if from < 0 || from >= len(*stages) {
		return false
	}
	cols, ok := blockPublishedColumns(node, published, subqueryDecl)
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
	published map[*logical.Node]bool, subqueryDecl func(string) (parquet.Column, bool)) []parquet.Column {
	cols, ok := blockPublishedColumns(p, published, subqueryDecl)
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
