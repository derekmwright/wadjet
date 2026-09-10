// This file holds stage projection for the physical planner, governed by ADR-0026 and ADR-0034.
package physical

import (
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"strings"
)

// GatherOutputSchema returns the plan-declared output schema carried on a
// stage DAG's terminal gather, or nil when the plan could not declare one.
//
// The coordinator calls it for the case its own answer cannot cover: a
// zero-row result has no batch to read a schema off, so `gatherSchema` over
// the gathered batches returns nil and pgwire falls back to declaring OID 25
// (text) for every column. Names already survive that case through
// OutputRenames; this is the other half (#416).
func GatherOutputSchema(stages []Stage) []parquet.Column {
	for i := range stages {
		if stages[i].Type == StageExchangeGather {
			return stages[i].OutputSchema
		}
	}
	return nil
}

// GatherOutputWireUnconstrainedDecimal is GatherOutputSchema's companion for
// the DECIMAL output columns whose PostgreSQL wire typmod must say
// "unconstrained" (-1) regardless of whether the result has rows — an
// aggregate function call, unlike a bare column reference (FIX 2,
// #457/#458 fold-in; see declaredWireUnconstrainedDecimal).
func GatherOutputWireUnconstrainedDecimal(stages []Stage) map[string]bool {
	for i := range stages {
		if stages[i].Type == StageExchangeGather {
			return stages[i].OutputWireUnconstrainedDecimal
		}
	}
	return nil
}

// GatherOutputStringLength is the same companion for the string family's
// modifier: the declared LENGTH of each output column a parameterized string
// cast bounds (#838; see DeclaredStringLengths).
func GatherOutputStringLength(stages []Stage) map[string]int {
	for i := range stages {
		if stages[i].Type == StageExchangeGather {
			return stages[i].OutputStringLength
		}
	}
	return nil
}

// qualifySharedRenameSource re-attaches a SELECT item's own qualifier to the
// source column its rename resolved to, when another item resolves the same
// bare source under a DIFFERENT qualifier.
//
// A bare source reached under two qualifiers is not an address. `WITH cte AS
// (SELECT id AS WatchID, a FROM t) SELECT a.WatchID, b.WatchID FROM cte a JOIN
// cte b ON a.a = b.a` resolves BOTH items to the CTE's source column `id`, so
// the join fragment's projection read the same column twice and the second
// output carried the first arm's value — `1,1 | 1,1 | 1,1 | 1,1` on both DAG
// arms where PostgreSQL has `1,1 | 1,2 | 1,3 | 1,8`, silently, and right on
// both local arms (#905's ClickBench spelling; the #513/#629 duplicate-output
// -name class). It is the ALIASED CTE column that makes it reachable: without
// the rename the items are already `a.id` / `b.id` and no resolution happens.
//
// A join qualifies a column both sides carry, so `a.id` is the spelling the
// stream really has; where it carries the bare name instead, the runtime
// lookup's qualified-to-bare fallback (columnIndexFallback) finds it anyway.
// The re-qualification is therefore safe in both shapes and is applied ONLY to
// the contested case, so a single-qualifier rename — every ordinary derived
// alias, and every TPC-H plan — is left exactly as it was.
func qualifySharedRenameSource(name, src string, proj []logical.Projection, child *logical.Node) string {
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 || strings.Contains(src, ".") {
		return src
	}
	qual := name[:dot]
	for i := range proj {
		other := proj[i].Expr
		if other == "" {
			other = proj[i].Column
		}
		od := strings.LastIndexByte(other, '.')
		if od <= 0 || strings.EqualFold(other[:od], qual) {
			continue // unqualified, or this item's own qualifier
		}
		if !strings.EqualFold(resolveOutputRenameSource(other, child), src) {
			continue
		}
		return qual + "." + src
	}
	return src
}

// attachScanSelectProjections sets ProjectExprs on a leaf scan stage when
// (a) the terminal gather's sole dependency is that scan (nothing computes
// between scan and gather) and (b) the outermost SELECT list contains at
// least one scalar expression (non-column, non-aggregate, not a wrapped
// synthetic aggregate). Expression outputs are named by their lowercased
// text — exactly the source name extractOutputRenames maps to the user's
// alias — and bare columns become passthrough entries so the fragment emits
// the full SELECT-list input set.
//
// (b) has a second trigger: a SORT KEY that names a SELECT alias the producer
// does not emit. `SELECT o_orderpriority AS p FROM orders ORDER BY p` has no
// expression at all, so the pass used to decline — the scan emitted
// "o_orderpriority", the sort keyed on "p" matched no column and silently did
// nothing, and only the gather's rename made the output *look* right (#316).
// Adding any expression to the SELECT list fixed it by accident, because that
// flipped hasExpr and the alias got materialized on the way past. The alias
// naming is decided here rather than in resolveSortKeyColumn precisely because
// this pass owns it: it runs last, and only it knows whether the producing
// fragment will carry an alias-naming OpProject.
func (p *Planner) attachScanSelectProjections(root *logical.Node, stages []Stage) []Stage {
	projNode := findOutputProjectionNode(root)
	if projNode == nil {
		return stages
	}
	proj := projNode.Projections
	if len(proj) == 0 {
		return stages
	}
	// The types these specs carry are the DAG's only answer for a computed
	// output column — the worker's buildSelectProjection copies them straight
	// onto exec.ProjectColumn.Type. Resolving bare column references against
	// the catalog has to happen here too, or COALESCE(n_name, n_comment)
	// stays Float64 on arm B alone (#333).
	var colTypes colDecls
	var strictInt map[string]bool
	if len(projNode.Children) == 1 {
		colTypes = inputColDecls(projNode.Children[0])
		// The same integer-preserving-arithmetic hint the single-process
		// path resolves via emittedColTypes/declaredProjectionType (#297):
		// without it, `id + 1` over a strict-int column declares (and
		// COMPUTES) FLOAT64 here, where the single-process engine answers
		// INT64 for the identical SQL (#443, #445).
		strictInt = strictIntArithCols(projNode.Children[0])
	}
	hasExpr := false
	specs := make([]ProjectExprSpec, 0, len(proj))
	// slotPassThrough[j] marks a SELECT item the fragment cannot compute —
	// one wrapping a hidden slot the GATHER evaluates — whose spec is a
	// pass-through of that slot rather than the item's value. It is
	// index-aligned with proj, because every consumer that pairs a spec with
	// a select item does so by position.
	slotPassThrough := make([]bool, len(proj))
	var extraSlots []string
	for j, it := range proj {
		if it.IsAgg {
			return stages // aggregates compute in their own fragments
		}
		itemExpr := it.Expr
		if itemExpr == "" {
			itemExpr = it.Column
		}
		if itemExpr == "" {
			return stages
		}
		name := strings.ToLower(itemExpr)
		var typ parquet.TypeID
		var typeKnown bool
		var prec, scale int
		// A ROW FIELD PATH looks like a simple column reference and is not
		// one: no stage carries a column by that name, so the fragment has
		// to COMPUTE it, and its type has to be declared here — nothing
		// downstream can correct a spec the way exec.Project corrects a
		// placeholder (#568).
		if it.ASTExpr != nil && (!isSimpleColRefForRename(it.ASTExpr) || astIsFieldPath(it.ASTExpr, colTypes)) {
			if referencesSyntheticAgg(it.ASTExpr) || referencesSyntheticWindow(it.ASTExpr) {
				// A wrapped aggregate or window (`SUM(x) OVER (…) + 1`) is
				// evaluated at the GATHER, from an OutputRename.Expr written
				// against the synthetic output column. Its Expr text is the
				// ABBREVIATED spelling (`sum(x) OVER (...) + 1`), which no
				// parser accepts — before the window branch below existed
				// this returned by the stage-type check instead, and
				// attaching it made every task fail to compile it (#610's
				// shapes, caught by the #656 window branch).
				//
				// So this ITEM cannot be attached. Abandoning the WHOLE
				// SELECT list because of it was #776: one wrapped window
				// beside two ordinary items left the other two computed by
				// nobody, and the reachability check then refused the plan
				// (`the gather renames "plain + 1" to "s" and no stage emits
				// a column of that name`) for a query the DAG can run.
				//
				// What this item needs from the fragment is not its VALUE
				// but the SLOT the gather will evaluate it from, so it is
				// attached as a PASS-THROUGH of that slot and the rest of
				// the list is attached normally. Its alias is not applied to
				// the slot (aliasedSpecsFor / anyRenamed skip it): the
				// gather's own rename carries the alias, and its Expr is
				// what produces the value.
				slots, complete := syntheticSlotRefs(it.ASTExpr)
				if !complete || len(slots) == 0 {
					// A node kind the walk does not descend into may hide a
					// second slot, and a pass-through that names fewer slots
					// than the gather will read answers NULL. Keep today's
					// decline rather than invent a column list.
					return stages
				}
				slotPassThrough[j] = true
				extraSlots = append(extraSlots, slots[1:]...)
				specs = append(specs, ProjectExprSpec{Expr: slots[0], Name: slots[0]})
				continue
			}
			hasExpr = true
			// A SELECT-list scalar subquery lowers to the SAME producer
			// stage a predicate's does (#659): the spec carries `:scalar_N`
			// and the coordinator substitutes the producer's value before
			// dispatch. The spec's NAME stays the item's own text, because
			// extractOutputRenames reads the untouched logical projection.
			// An item this cannot rewrite keeps the whole SELECT list off
			// the DAG, which is the refusal that routes it local.
			if exprCarriesSubquery(it.ASTExpr) {
				lowered, ldecl, ldeclKnown, ok := p.lowerProjectionSubquery(&stages, &proj[j], colTypes)
				if !ok {
					return stages
				}
				if p.loweredScalarProjExprs == nil {
					p.loweredScalarProjExprs = map[*logical.Projection]bool{}
				}
				p.loweredScalarProjExprs[&proj[j]] = true
				specs = append(specs, ProjectExprSpec{Expr: lowered, Name: name,
					Type: ldecl.ID, TypeKnown: ldeclKnown,
					Precision: ldecl.Precision, Scale: ldecl.Scale})
				continue
			}
			decl := inferProjectionDeclType(it.ASTExpr, parquet.TypeString, strictInt, colTypes)
			typ = decl.ID
			prec, scale = decl.Precision, decl.Scale
			typeKnown = true
		}
		specs = append(specs, ProjectExprSpec{Expr: itemExpr, Name: name, Type: typ,
			TypeKnown: typeKnown, Precision: prec, Scale: scale})
	}
	// A wrapped item reading TWO slots needs both on the stream, and only the
	// first could take its own position. The rest ride at the END, past the
	// index range every by-position consumer walks: the gather projects to
	// exactly its rename list, so a column past that list costs a copy and
	// changes no output.
	for _, slot := range extraSlots {
		dup := false
		for _, sp := range specs {
			if strings.EqualFold(sp.Name, slot) {
				dup = true
				break
			}
		}
		if !dup {
			specs = append(specs, ProjectExprSpec{Expr: slot, Name: slot})
		}
	}
	// #386: a NESTED subquery rename never trips anyRenamed — the outer list
	// merely forwards the alias (`SELECT k FROM (SELECT r_regionkey AS k FROM
	// region) t ORDER BY k DESC`), so the pass declined, the sort keyed on a
	// column no stage emits, and the ORDER BY silently no-oped (ASC spellings
	// passed only by scan-order luck; an alias shadowing a real column sorted
	// by the WRONG one). Resolve each simple column reference through nested
	// rename-only Projects (the #385 walk): the spec's Expr becomes the
	// SOURCE column the streams actually carry, its Name keeps the outer
	// spelling, and the substitution itself is a trigger for the pass.
	var renameChild *logical.Node
	if len(projNode.Children) == 1 {
		renameChild = projNode.Children[0]
	}
	anyNestedRename := false
	for j := range specs {
		if j >= len(proj) || slotPassThrough[j] {
			// A hidden-slot pass-through — the item's own position, or one of
			// the extras appended past the select list — names a column the
			// producer computes, not a name the query wrote. Resolving it
			// through the rename chain would look for a source it has no
			// business having, and indexing proj by it is out of range
			// outright (#776).
			continue
		}
		if proj[j].ASTExpr != nil && !isSimpleColRefForRename(proj[j].ASTExpr) {
			// #387: an EXPRESSION referencing a nested rename (`k + 1` over
			// `r_regionkey AS k`) was attached verbatim, so the fragment
			// compiled it against a schema with no `k` and the task
			// hard-failed. Substitute the references in the AST (a name
			// swap on the string cannot see them), regenerate the compiled
			// text, and re-infer the type against the SOURCE schema the
			// rewritten expression now reads — the alias was invisible to
			// inputColTypes, so the spec fell back to Float64 (#333's
			// symptom one level down). The spec's NAME keeps the outer
			// text: the gather's renames and the sort's alias keys are
			// written against it. A declined rewrite (subquery/window
			// bearing, unknown node) leaves the spec untouched, keeping
			// today's loud failure over a silently different expression.
			if rewritten, ok := substituteNestedRenameRefs(proj[j].ASTExpr, renameChild); ok && rewritten != proj[j].ASTExpr {
				specs[j].Expr = rewritten.String()
				// strictIntArithColsThroughRenames mirrors the colTypes call
				// just below it: the rewritten expression names only SOURCE
				// columns, so the strict-int set to check it against is the
				// one visible BELOW the rename chain, same as #445 above.
				specs[j].Type, specs[j].Precision, specs[j].Scale = declTypeParts(
					inferProjectionDeclType(rewritten, parquet.TypeString,
						strictIntArithColsThroughRenames(renameChild),
						sourceColDeclsThroughRenames(renameChild)))
				specs[j].TypeKnown = true
				anyNestedRename = true
			}
			continue
		}
		src := resolveOutputRenameSource(specs[j].Name, renameChild)
		if strings.EqualFold(src, specs[j].Name) && strings.Contains(specs[j].Name, ".") {
			// Qualified spelling: the nested Project's alias is bare — the
			// same qualified↔bare fallback the gather applies.
			//
			// SCOPED to the relation the qualifier names when the plan says
			// which one that is. The unscoped walk takes the first arm that
			// answers, and with two derived tables publishing `w` that is
			// the OTHER arm's column: `SELECT p.w, q.w FROM (…SUM(b) OVER ()
			// AS w) p JOIN t y … JOIN (…a * 3 AS w) q` projected p's window
			// slot under both names (#742). A qualifier the scope test
			// cannot place keeps the unscoped fallback.
			if r, scoped := resolveRenameSourceInScope(specs[j].Name, renameChild); scoped {
				if r != "" {
					src = r
				}
			} else if bare := specs[j].Name[strings.LastIndexByte(specs[j].Name, '.')+1:]; bare != "" {
				if r := resolveOutputRenameSource(bare, renameChild); !strings.EqualFold(r, bare) {
					src = r
				}
			}
		}
		if !strings.EqualFold(src, specs[j].Name) {
			specs[j].Expr = qualifySharedRenameSource(specs[j].Name, src, proj, renameChild)
			anyNestedRename = true
		}
	}
	// The other trigger is a sort key naming an alias, which needs the target
	// stage's keys — decided below, once the target is known.
	if !hasExpr && !anyRenamed(proj, specs, slotPassThrough) && !anyNestedRename {
		return stages
	}
	var gather *Stage
	for i := range stages {
		if stages[i].Type == StageExchangeGather {
			gather = &stages[i]
			break
		}
	}
	if gather == nil || len(gather.Dependencies) != 1 {
		return stages
	}
	// Resolve the compute target through at most one standalone sort hop:
	// scan→sort→gather (ORDER BY over a bare expression SELECT, #288 seeds
	// 231/246) needs the projection on the SCAN so the sort can resolve an
	// expression alias in its keys — the sort stage itself computes
	// nothing. The sort's keys join the coverage check below.
	targetID := gather.Dependencies[0]
	var viaSort *Stage
	for i := range stages {
		s := &stages[i]
		if s.ID == targetID && (s.Type == "sort" || s.Type == "merge_sort") && len(s.Dependencies) == 1 {
			viaSort = s
			targetID = s.Dependencies[0]
			break
		}
	}
	for i := range stages {
		s := &stages[i]
		if s.ID != targetID {
			continue
		}
		// A producer that COLLAPSES its input — an aggregate family stage, a
		// union, a fused scan-aggregate — can neither evaluate the SELECT
		// list nor hand it down: its output is a NEW column set. Give the
		// projection a StageProject of its own, directly above the producer
		// so a sort between the two can key on what it computes (#656 F2).
		//
		// Checked BEFORE the already-carries-a-projection bail below,
		// because absorbAggregateOutputProjection has usually put one there
		// — that projection names the aggregate's outputs, and this one
		// computes over them.
		// A standalone sort ABOVE this producer keys on names the projection
		// may drop, and a StageProject inserted here would sit BELOW it. Let
		// the coverage decision downstream handle that case instead — it
		// puts the projection above the sort, where nothing needs the
		// dropped columns.
		if projectionNeedsItsOwnStage(s, aliasedSpecsFor(proj, specs, slotPassThrough)) &&
			orderingSurvivesAProjectStage(stages, i, aliasedSpecsFor(proj, specs, slotPassThrough)) &&
			(viaSort == nil || projectionCoversSortKeys(
				aliasedSpecsFor(proj, specs, slotPassThrough), viaSort.SortKeys)) {
			// Written against what the producer EMITS, not what the query
			// wrote: above an aggregate a computed group key is a column
			// NAME, and rebuilding it as arithmetic answers NULL.
			aliased, ok := respellSpecsOverProducerOutput(stages, i,
				aliasedSpecsFor(proj, specs, slotPassThrough))
			if ok && specsResolveAgainstStageOutput(stages, i, aliased) {
				// A name the producer publishes TWICE — a group key beside an
				// aggregate output aliased like it — is addressed by SLOT
				// here, or both specs read the first column of the name and
				// the second value is unreachable (ADR-0026 §3a, #785). The
				// gather's renames already carry the class of each item,
				// resolved through however many wrappers stand between.
				pinProjectSpecSlots(&stages[i], aliased, func(j int) (bool, bool) {
					if j >= len(gather.OutputRenames) {
						return false, false
					}
					return gather.OutputRenames[j].IsAgg, true
				})
				keys := stages[i].SortKeys
				stages = insertProjectStageAbove(stages, i, aliased)
				carryOrderingOntoProjectStage(stages, len(stages)-1, keys)
				repointGatherRenames(gather, aliased)
				return stages
			}
		}
		// A scan already carrying a projection (a computed subquery column
		// materialized by absorbComputedSubqueryProjection, #383) keeps it:
		// overwriting would drop the computed column the sort keys on, and
		// these SELECT-list specs are written against the subquery's
		// OUTPUT, not the scan's schema.
		if len(s.ProjectExprs) > 0 {
			return stages
		}
		isPlainScan := s.Type == StageScan && len(s.FusedAggGroupBy) == 0 && len(s.FusedAggSpecs) == 0
		isJoin := (s.Type == StageHashJoin || s.Type == StageBroadcastJoin || s.Type == StageSortMergeJoin) &&
			len(s.GroupByCols) == 0
		// A WINDOW, SORT, LIMIT or PROJECT producer takes the same aliased
		// projection, with one difference that is the whole of #656 shape g:
		// its OpProject runs ABOVE the operator, not below it. All four
		// FORWARD their input's columns, which is what makes the SELECT list
		// — written against the producer's output — evaluable there. An
		// AGGREGATE is deliberately NOT in that set: its output is group keys
		// and aggregate outputs, not its input's columns, so a SELECT list
		// written over `COALESCE(l_extendedprice, 0)` would re-evaluate
		// COALESCE against a stream that no longer carries l_extendedprice.
		// absorbAggregateOutputProjection is the aggregate's route, and it
		// spells against the OUTPUT names. The window fragment forwards
		// every input column and appends its own outputs, so the SELECT
		// list — written against exactly that — is evaluable there, and
		// without it the DAG returned the window's raw input plus the window
		// column where the query asked for `UPPER(s)`. A sort above the
		// window still has to be covered by the projection, which the check
		// below already enforces for the join path.
		isWindow := s.Type == StageWindow
		if !isPlainScan && !isJoin && !isWindow && !forwardsInputColumns(s.Type) {
			// Something else computes between here and the gather (fused
			// scan-aggregates project via their aggregate machinery).
			return stages
		}
		// Direct scan→gather keeps the original #169 convention: outputs
		// named by lowercased expression text, which the gather's
		// project-mode rename maps to the user's alias. Nothing sorts here,
		// so a rename alone is no reason to project: the gather does it.
		if isPlainScan && viaSort == nil {
			if !hasExpr {
				return stages
			}
			s.ProjectExprs = specs
			// #387: with a nested rename substituted into the specs, the
			// fragment emits the outer SELECT's names ("k", "k + 1") — but
			// the #385 resolution already pointed the gather's From at the
			// SOURCE names the stream would have carried without this
			// projection (r_regionkey), so the rename would miss and fall
			// back to full width. Re-point each From at the name the
			// fragment now emits, exactly as the aliased path below does.
			if anyNestedRename && len(gather.OutputRenames) <= len(specs) {
				for j := range gather.OutputRenames {
					if gather.OutputRenames[j].Expr == nil {
						gather.OutputRenames[j].From = specs[j].Name
					}
				}
			}
			return stages
		}
		// Join feeding the gather (the #169 class on the join path), or a
		// scan/join under a standalone sort (ORDER BY over a bare
		// expression SELECT): nothing computes the SELECT expressions —
		// the gather renamed-by-expression-text, missed, and passed raw
		// columns through. Attach the SELECT list so the producing
		// fragment projects worker-side (join fragments and the scan's
		// filter-fragment path both append the OpProject).
		//
		// Unlike the direct-scan case, outputs are named by the user's
		// ALIAS when one exists: a sort — standalone, or fused into the
		// join by fuseSortIntoPredecessor — may key on the alias, and the
		// projection must emit it under that name for the sort to resolve.
		// The gather rename then finds columns already carrying final
		// names and leaves them alone (rename-only keeps exactly the
		// projected set).
		aliased := make([]ProjectExprSpec, len(specs))
		for j, sp := range specs {
			aliased[j] = sp
			if j >= len(proj) || slotPassThrough[j] {
				// A hidden-slot pass-through is not the item's VALUE, so it
				// must not take the item's alias: the gather's own rename
				// carries the alias and evaluates the wrapped expression
				// from this column (#776).
				continue
			}
			if a := proj[j].Alias; a != "" {
				// VERBATIM, which is the whole point of the paragraph above:
				// the sort keys on the ALIAS, so the projection has to emit
				// the alias the query wrote. An alias's case is part of the
				// name a delimited identifier gives — PostgreSQL publishes
				// `Kk` for `AS "Kk"`, and so does our own gather — so
				// lower-casing it here emitted `kk` while the sort key still
				// said `Kk`, and the DAG failed with `sort: key column "Kk"
				// does not exist in the input schema` on a query the
				// single-process path answers. Consumers that match by name
				// fold case on both sides already.
				aliased[j].Name = a
			}
		}
		// A WINDOW forwards its input, and that input may be an AGGREGATE's
		// output — where a computed GROUP BY key is the NAME of one column and
		// not arithmetic over one. Spell the specs against what the producer
		// chain EMITS, which is what the StageProject branch above already does
		// for a collapsing producer. Without it the window fragment rebuilt
		// `g + 1` over a `g` the aggregate does not emit and both DAG arms
		// answered the right eight rows with a NULL key (#737).
		//
		// Only for a window: a scan or a join is its own input's columns, so
		// there is nothing to re-spell, and running the respell there would
		// make its DECLINE — which is a plan-wide bail — reachable for shapes
		// that are correct today.
		if isWindow {
			if respelled, ok := respellSpecsOverProducerOutput(stages, i, aliased); ok {
				aliased = respelled
			}
		}
		// Every sort key — the fused sort's on a join, and the standalone
		// sort stage's — must resolve among the projection's outputs:
		// OpProject narrows the schema to exactly its projections. Bail
		// (keep old behavior) when uncovered.
		// A stage that projects AFTER its own operator has already ordered
		// by its own keys before the projection runs, so those keys need not
		// survive it; a scan or a join projects BEFORE its fused ordering
		// and they must. The sort ABOVE (viaSort) always must — the
		// projection is below it either way.
		var sortKeys []SortKeySpec
		if !projectionRunsAfterStageOperator(s.Type) {
			sortKeys = append(sortKeys, s.SortKeys...)
		}
		if viaSort != nil {
			sortKeys = append(sortKeys, viaSort.SortKeys...)
		}
		// With no expression to compute, the projection only earns its place
		// when a sort key names an alias this stage does not emit under that
		// name — otherwise the gather's rename already covers the query and
		// narrowing the schema here would be pure cost (#316).
		if !hasExpr && !sortKeysNeedAlias(sortKeys, specs, aliased) {
			return stages
		}
		// A predicate ABOVE the projection has to keep its columns too: an
		// OpProject narrows, so a filter on this stage or on the sort above
		// it that names a column the projection drops becomes UNKNOWN on
		// every row. `SELECT k FROM (SELECT id AS k, g AS v FROM t ORDER BY
		// id) s WHERE s.v > 0` answered 0 rows where PostgreSQL answers 3956
		// (#656 F2).
		// Nothing may be attached that the carrier's input cannot EVALUATE.
		// Every branch below picks a carrier, and a spec naming a derived
		// alias whose definition lives in a synthetic column (`__win_0`) is
		// resolvable on none of them — attaching it anyway builds a plan
		// assertCarrierSchemaResolves then refuses, which reaches the client
		// as a hard error on a query the gather's own rename can compute.
		viaSortIdx := -1
		if viaSort != nil {
			for j := range stages {
				if stages[j].ID == viaSort.ID {
					viaSortIdx = j
					break
				}
			}
		}
		if !specsResolveAgainstStageInput(stages, i, aliased) &&
			!specsResolveAgainstStageInput(stages, viaSortIdx, aliased) &&
			!specsResolveAgainstStageOutput(stages, i, aliased) {
			return stages
		}
		filterCols := append(append([]string(nil), s.FilterExprs...), viaSortFilters(viaSort)...)
		if !projectionCoversSortKeys(aliased, sortKeys) ||
			!projectionCoversFilters(aliased, filterCols) {
			// The projection cannot go BELOW this ordering: OpProject
			// narrows the batch to its outputs, so a sort key it does not
			// emit would be gone by the time the sort ran. Put it ABOVE the
			// standalone sort instead, where the ordering has already
			// happened — `SELECT id * 2 AS d FROM (SELECT id FROM t ORDER BY
			// id LIMIT 5) s` orders by `id` and returns `d`, and declining
			// here is what returned the raw `id` column instead (#656
			// follow-up).
			// Above the standalone sort, when there is one, it is free, and
			// its own keys survive without this projection: its ordering and
			// its filter have both already run, so nothing above needs a
			// column the projection drops. A sort that keys on something
			// only the projection COMPUTES needs the opposite — the
			// projection below it — and takes the inserted stage instead.
			if viaSort != nil && len(viaSort.ProjectExprs) == 0 &&
				sortKeysSurviveWithout(stages, i, viaSort.SortKeys) {
				viaSort.ProjectExprs = aliased
				repointGatherRenames(gather, aliased)
				return stages
			}
			// Otherwise a StageProject directly above the producer: the
			// producer's own filter runs below it, and a sort above it keys
			// on what it computes. Declining instead left nothing computing
			// the SELECT list at all.
			//
			// Only when the sort above CAN key on the projection's outputs.
			// When it needs both a column the projection drops and one only
			// the projection provides, neither side of the sort works, and
			// declining is right: the gather's rename and
			// resolveDerivedAliasSortKeys settle the shape between them.
			if viaSort != nil && !projectionCoversSortKeys(aliased, viaSort.SortKeys) {
				return stages
			}
			// And never above a producer whose OWN ordering the inserted
			// stage would hide: the consumer reads the ordering off its
			// direct dependency. The rows stay right and the SEQUENCE stops
			// being the one the query asked for.
			if !orderingSurvivesAProjectStage(stages, i, aliased) {
				return stages
			}
			keys := stages[i].SortKeys
			stages = insertProjectStageAbove(stages, i, aliased)
			carryOrderingOntoProjectStage(stages, len(stages)-1, keys)
			repointGatherRenames(gather, aliased)
			return stages
		}
		s.ProjectExprs = aliased
		// This fragment now emits the SELECT list under its FINAL names, so
		// the gather's source→alias pairs are stale. Not merely redundant:
		// when one item's alias shadows another item's source column
		// ("n_name AS n_comment, n_comment AS c"), the stale pair matches the
		// column this projection already renamed and renames it a second time
		// — both outputs came back named "c". Point each source at the name
		// the stage emits; the gather still projects to exactly the
		// SELECT-list set, in order, and now resolves every source instead of
		// relying on all of them missing.
		// The rename list carries only the VISIBLE select items, so it can be
		// shorter than the projection when the plan materialized an ORDER BY
		// term (#320). Hidden columns are appended last precisely so the
		// leading indices still line up — and leaving them unnamed here is
		// what drops them: the gather projects to exactly the names it lists.
		repointGatherRenames(gather, aliased)
		return stages
	}
	return stages
}

// repointGatherRenames points each of the gather's source names at the name
// the producing fragment now emits, once a projection has been attached to
// it.
//
// Not merely redundant: when one item's alias shadows another item's source
// column ("n_name AS n_comment, n_comment AS c"), the stale pair matches the
// column this projection already renamed and renames it a second time — both
// outputs came back named "c". The rename list carries only the VISIBLE
// select items, so it can be shorter than the projection when the plan
// materialized an ORDER BY term (#320); hidden columns are appended last
// precisely so the leading indices still line up.
func repointGatherRenames(gather *Stage, aliased []ProjectExprSpec) {
	if gather == nil || len(gather.OutputRenames) > len(aliased) {
		return
	}
	for j := range gather.OutputRenames {
		if gather.OutputRenames[j].Expr == nil {
			gather.OutputRenames[j].From = aliased[j].Name
		}
	}
}

// anyRenamed reports whether any SELECT-list item carries an alias that
// differs from the name its producing stage would emit — i.e. whether an
// alias-naming projection could make any difference at all. Cheap pre-gate
// for attachScanSelectProjections: with no rename and no expression there is
// nothing for it to do, and it can decline before looking at any stage.
func anyRenamed(proj []logical.Projection, specs []ProjectExprSpec, slotPassThrough []bool) bool {
	for j, p := range proj {
		if j < len(slotPassThrough) && slotPassThrough[j] {
			// The spec names a hidden SLOT, not the item's value, and the
			// gather renames it. A pass attached only for that would be a
			// projection nothing asked for (#776).
			continue
		}
		if p.Alias != "" && !strings.EqualFold(p.Alias, specs[j].Name) {
			return true
		}
	}
	return false
}

// sortKeysNeedAlias reports whether any sort key names a SELECT-list alias
// that the producing stage does not emit under that name — the #316
// condition. specs[j].Expr carries what the stage emits without an
// alias-naming projection (the source column after nested-rename resolution,
// or the expression text); aliased[j] carries the user's alias. A key
// matching an alias whose source is spelled differently would find no column
// at all, or — when the alias shadows another column of the input — the
// WRONG one, so the sort must be given the projection that materializes the
// alias. Comparing the key against Expr rather than Name is what lets a
// NESTED rename trip the condition (#386): there the outer list has no alias
// of its own, so Name equals the key, but the stream carries the resolved
// source column.
func sortKeysNeedAlias(sortKeys []SortKeySpec, specs, aliased []ProjectExprSpec) bool {
	for _, k := range sortKeys {
		for j := range aliased {
			if strings.EqualFold(aliased[j].Name, k.Column) &&
				!strings.EqualFold(specs[j].Expr, k.Column) {
				return true
			}
		}
	}
	return false
}
