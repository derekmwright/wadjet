// This file holds dag refusals for the physical planner, governed by ADR-0026 and ADR-0034.
package physical

import (
	"context"
	"fmt"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"strings"
)

// PlanDistributed generates a stage DAG for distributed execution.
// Returns stages with dependency ordering suitable for coordinator dispatch.
// refuseUnexpandedStarBesideItems refuses a QUALIFIED star in a SELECT list
// that could not be expanded.
//
// Every consumer below resolves an output column by name, and `d.*` is not
// one: the single-process arms failed with `column "d.*" does not exist in the
// input schema` and the DAG published a column whose NAME and VALUE were both
// the string `*`. One sentence on every arm, and the shapes that CAN be
// expanded — a base table, a derived table or a CTE whose own SELECT list
// names its columns — are expanded before this runs
// (logical.ExpandStarProjections).
//
// It used to require a SECOND select item, because a star ALONE built no
// Project at all and so could not reach it. One does now (#979), and without
// this the LATERAL's own star — the shape the expansion deliberately declines —
// escaped to the executor's generic `42000 operator execute: column "s.*" does
// not exist in the input schema` instead of the planner's one sentence, which
// is what `docs/sql-reference.md` and ADR-0012 describe.
//
// A node carrying a DEFERRED column-alias list is left alone: its star is the
// wrapper `deferColumnAliasesOverStar` made, and
// `RefuseUnappliedColumnAliasLists` refuses it with a sentence about the LIST,
// which is the more specific answer for that shape (#958).
func refuseUnexpandedStarBesideItems(node *logical.Node) error {
	if node == nil || node.Type != logical.NodeProject || len(node.Projections) == 0 {
		return nil
	}
	if len(node.DeferredColumnAliases) > 0 {
		return nil
	}
	for _, pr := range logical.VisibleProjections(node.Projections) {
		name := strings.TrimSpace(pr.Expr)
		if name == "" {
			name = strings.TrimSpace(pr.Column)
		}
		if name != "*" && !strings.HasSuffix(name, ".*") {
			continue
		}
		return sqlerr.New("0A000",
			"column %q does not exist in the input schema: a `%s` expands only from a "+
				"relation whose column list is known — a base table, or a derived table "+
				"or CTE whose own SELECT list names its columns — and this one's is not; "+
				"name the columns",
			name, name)
	}
	return nil
}

// refuseUnexpandedStarAnywhere is refuseUnexpandedStarBesideItems over a whole
// plan, for the DISTRIBUTED entry: the stage planner does not go through
// buildProject, and the rule is the plan's, not one path's.
func refuseUnexpandedStarAnywhere(node *logical.Node) error {
	if node == nil {
		return nil
	}
	if err := refuseUnexpandedStarBesideItems(node); err != nil {
		return err
	}
	for _, child := range node.Children {
		if err := refuseUnexpandedStarAnywhere(child); err != nil {
			return err
		}
	}
	return nil
}

func (p *Planner) PlanDistributed(ctx context.Context, node *logical.Node) ([]Stage, error) {
	p.planCtx = ctx // store for scalar subquery evaluation during stage generation
	if len(node.CTEs) > 0 {
		p.ctes = node.CTEs
	}
	// Ensure scan nodes have column metadata — needed by assignJoinKeySides
	// to assign shuffle keys to the correct child side.
	p.AnnotateScanColumns(ctx, node)
	// A star that shares its SELECT list and could not be expanded is the
	// PLAN's defect, not one path's: this entry does not go through
	// buildProject, and without the check the DAG published a column whose
	// name and value were both `*` while the single-process arms refused.
	logical.ExpandStarProjections(node)
	if err := refuseUnexpandedStarAnywhere(node); err != nil {
		return nil, err
	}
	// …and the same for a COLUMN-ALIAS LIST longer than the `SELECT *` body it
	// renames: the width is a pass later than the builder, so PostgreSQL's
	// 42P10 is raised a pass later too (#958, column_alias_defer.go). The
	// apply is a no-op on a plan Optimize already walked and is here so this
	// entry never refuses a list it merely has not applied yet.
	node = logical.ApplyDeferredColumnAliases(node)
	if err := logical.RefuseUnappliedColumnAliasLists(node); err != nil {
		return nil, err
	}
	// Per-row correlated subqueries have no distributed lowering: refuse
	// with a typed error BEFORE stage generation so the coordinator can
	// route the query onto its local single-process engine instead of the
	// scalar-deferral path silently answering 0 (#359).
	if err := p.refuseCorrelatedSubqueries(node); err != nil {
		return nil, err
	}
	// A `SELECT * ... ORDER BY <n>` whose star never expanded (#810). This is
	// a genuine refusal of the QUERY, not of the distributed plan: it is NOT
	// one of the typed errors the coordinator routes local on, because the
	// single-process path refuses it too.
	if err := logical.RefuseUnresolvedOrdinalSortKeys(node); err != nil {
		return nil, err
	}
	// A DISTINCT with no stage and no coordinator dedup is a DROPPED
	// DISTINCT — the raw row set, returned confidently (#466). Refuse it
	// here for the same reason: loud beats silently different.
	if err := refuseUnstageableDistinct(node); err != nil {
		return nil, err
	}
	// GROUPING SETS / ROLLUP / CUBE is the same shape one construct over: the
	// DAG has no field to carry the sets, so it ran their UNION as a plain
	// GROUP BY and silently dropped every super-aggregate row (#778).
	if err := refuseGroupingSets(node); err != nil {
		return nil, err
	}
	// A GROUP BY key whose RESOLUTION name and PUBLISHED name have to differ
	// was refused HERE while `Stage.GroupByCols` was one field for both. It is
	// not refused any more: the stage carries both names, and the resolution a
	// derived alias needs is settled at the END of planning, against what the
	// producing fragment really emits (resolveStageGroupKeys, ADR-0026 §2).
	// A `real IN (...)` list holding a literal that is not a real is a
	// PLAN-time error in PostgreSQL, raised whether or not the predicate is
	// ever reached (#631 follow-up). Refuse it here so the DAG cannot answer
	// rows for a query PostgreSQL refuses.
	if err := refuseUnrepresentableRealInList(node); err != nil {
		return nil, err
	}
	// A SELECT with no FROM emits a `dual` stage with no dependencies and no
	// scan files, which the dispatcher cannot build task inputs for — so the
	// query FAILED on the DAG rather than answering (#806). Refuse before
	// stage generation so the coordinator routes it onto its local engine,
	// which is what the dual stage's own comment has always claimed happens.
	if err := refuseTableLessSelect(node); err != nil {
		return nil, err
	}
	stages := p.generateStages(node)
	// FIRST: an authorization refusal is the query's answer on every path,
	// never a reason to route it to another one (#945).
	if p.authzErr != nil {
		return nil, p.authzErr
	}
	if p.setOpErr != nil {
		return nil, p.setOpErr
	}
	if p.joinCondErr != nil {
		return nil, p.joinCondErr
	}
	if p.correlatedErr != nil {
		return nil, p.correlatedErr
	}
	if p.inSubqueryErr != nil {
		return nil, p.inSubqueryErr
	}
	if p.scalarRowsErr != nil {
		return nil, p.scalarRowsErr
	}
	// A STAR OVER A BLOCK NO STAGE COULD PUBLISH (#984). Asked AFTER stage
	// generation, because the answer is what the pass DID: every block a star
	// reads whose projection differs from its stream is materialized onto a
	// stage, and one the pass declines — a producer that cannot carry a
	// projection above it, or specs that do not resolve against what it emits
	// — would leave the star reading the stream, which is the wrong relation.
	// Routed, not answered short. A block item's TYPE is never a reason: it is
	// declared by the same inference the single-process path uses.
	//
	// AFTER the errors above and BEFORE enforceQueryLimits: an authorization
	// refusal is the query's answer on every path and must not be routed
	// around (#945), and a limit is about the plan this one has already
	// settled.
	if err := refuseUnpublishedStarBlock(p.starReadBlocks, p.publishedBlocks); err != nil {
		return nil, err
	}
	if err := p.enforceQueryLimits(ctx, stages, node); err != nil {
		return nil, err
	}
	// Phase 1 distribution-property pass: populate Stage.Distribution for
	// every stage. Phase 2 then runs EnsureDistribution to insert Exchange
	// stages where child output doesn't satisfy parent input, and asserts
	// consistency strictly.
	assignStageDistributions(stages, p.WorkerCount)
	// Collapse multi-level merge_aggregate/merge_sort trees before the
	// Exchange pass so the emitted Exchange stages are placed against
	// the final merger, not the intermediate tree levels. The tree
	// shape is a single-pipeline optimization that becomes a SF10-
	// killing N-round-trip fan-out under native-DAG dispatch.
	stages = collapseMergeTreesForNativeDAG(stages)
	// Drop redundant trailing merge_sort Singleton stages whose sole
	// dep is a Singleton sort — the merge_sort is a no-op in that
	// shape and costs a full worker round-trip per query.
	stages = collapseRedundantFinalMergeSort(stages)
	// Fuse Singleton sort into its Singleton predecessor (aggregate /
	// hash_join / broadcast_join / final_aggregate) so the worker
	// applies the sort in-process rather than serializing the
	// pre-sort output and letting a separate sort task pick it up.
	stages = fuseSortIntoPredecessor(stages, p.WorkerCount)
	var ensureErr error
	stages, ensureErr = EnsureDistribution(stages, p.WorkerCount)
	if ensureErr != nil {
		return nil, fmt.Errorf("ensure distribution: %w", ensureErr)
	}
	// Re-resolve distributions after EnsureDistribution: stages whose
	// inputs got rewritten to exchange outputs need their Distribution
	// recomputed against the new dep distributions. Without this, e.g.,
	// a grouped final_aggregate whose dep was upgraded from Singleton
	// to HashPartitioned (via an inserted exchange-repartition) would
	// keep its initial Singleton label, and dispatchComputeStage would
	// run it as one task instead of the N parallel tasks the exchange
	// is feeding (Q18 SF10 OOM trigger).
	assignStageDistributions(stages, p.WorkerCount)
	// Drop identity re-shuffles (input already hash-partitioned on the
	// exchange's exact keys/count — Q18's 40 GB repartition-15 at
	// SF100). Must run after distributions are final; consumers keep
	// valid labels because the elided exchange's output distribution
	// was by definition its input's. Kill switch WADJET_EXCHANGE_ELIDE=0.
	stages = elideCoPartitionedExchanges(stages)
	// Drop filtered scan-exchanges fully subsumed by a raw sibling over
	// the same table (Q21 l3 ⊂ l2): the raw exchange ships the filter
	// as a computed flag column and the dropped exchange's consumer
	// filters its build input on it. Kill switch
	// WADJET_EXCHANGE_SUBSUME=0.
	stages = dedupeSubsumedScanExchanges(stages)
	// Feed a grouped final_aggregate from a sibling RAW exchange
	// hash-partitioned on its exact group keys, dropping the duplicate
	// fused scan-agg leg (Q18's 2nd full lineitem scan). The rewired
	// final mirrors the raw exchange's partitioning, which typically
	// turns its downstream re-shuffle into an identity exchange — run
	// the elide pass again to collect it. Kill switch
	// WADJET_AGG_OVER_EXCHANGE=0.
	if rewired := rewireAggOverRawExchange(stages); len(rewired) != len(stages) {
		stages = elideCoPartitionedExchanges(rewired)
	}
	// Drop join subtrees that duplicate a sibling subtree (Q11's
	// scalar-subquery leg clones its main leg stage-for-stage; Q17's
	// semi lineitem⋈part ≡ its inner sibling), rewiring the clone's
	// consumers onto the survivor — stage outputs already support
	// multiple consumers. MUST run before fuseStageChains: chain
	// fusion absorbs consumers into the legs, breaking the clones'
	// structural symmetry. Kill switch WADJET_SHARED_SUBPLAN=0.
	stages = dedupeSharedSubplans(stages)
	// Fuse 1:1 same-distribution join chains (consumer task i reads
	// exactly producer task i's output) into single fragments, eliding
	// the per-link materialization — Q18's join-class 48.9 GB at
	// SF100. Runs when distributions are final so the count-equality
	// gate sees real values. No file-count amplification (task count
	// unchanged), unlike the disabled fuseScanShuffle/fuseJoinShuffle
	// below. Kill switch WADJET_STAGE_FUSION=0.
	stages = fuseStageChains(stages)
	// Narrow scan-stage OUTPUT to what consumers declare (Columns
	// stays the read set — pushed filter columns are read, applied,
	// then dropped from the payload). Runs after every stage-rewiring
	// pass so the consumer set is final. Kill switch
	// WADJET_SCAN_OUTPUT_PRUNE=0.
	pruneScanOutputColumns(stages)
	// Fuse scan→exchange-repartition pairs whose consumers all
	// partition-bind (hash/sort-merge joins, grouped finals): the scan
	// task hash-partitions its filtered output directly, deleting the
	// full write+read of the unpartitioned intermediate (2026-08-02
	// SF100 accounting: ~20 GB duplicated per cold suite run on scan
	// legs — Q03 10.0 GB, Q21 6.8 GB, Q13 2.4 GB). Re-enabled
	// 2026-08-02: the 2026-05 regressions that kept this disabled
	// (Q07 +85%, Q03 +30% — file-count amplification at broadcast
	// caches and flattening exchange readers) are excluded
	// structurally by the consumer-shape gate, and the old
	// consolidation argument no longer holds — scan and shuffle
	// fan-out are both capacity-bound now, so the fused layout's
	// per-partition file count matches the unfused two-step for
	// partition-binding consumers. Kill switch
	// WADJET_FUSE_SCAN_SHUFFLE=0.
	stages = fuseScanShuffle(stages)
	// Same treatment for join→exchange pairs (Q18 join-4→rp-6 7.6 GB,
	// Q05 join-4→rp-8 2.96 GB duplicated per SF100 run in the 08-02
	// fusion-ab pair): hash_join outputs partition directly via the
	// fragment runner's OpExchangeSender terminal, gated identically
	// (partition-binding consumers only, no computed cols; hash_join
	// only — broadcast_join fusion stays off per the 2026-05-03 Q02
	// wrong-rows history). Kill switch WADJET_FUSE_JOIN_SHUFFLE=0.
	stages = fuseJoinShuffle(stages)
	//
	// fuseScanAggregateShuffle IS enabled. Pattern: scan(FusedAgg) →
	// exchange-repartition → final_aggregate/merge_aggregate. Aggregate
	// collapses input cardinality to K rows per task, so fused output
	// scan-task-count × numPartitions matches the unfused output's
	// post-exchange-repartition file count. No amplification at
	// downstream consumers. Enables Q01/Q02/Q15/Q17/Q18/Q20-style
	// aggregation queries to skip the standalone exchange-repartition
	// stage. Gated on collapsing-consumer (final_aggregate /
	// merge_aggregate) only.
	stages = fuseScanAggregateShuffle(stages)
	// Sender-side partial aggregation on surviving exchanges (SF100 Q18's
	// 600M-row raw (l_orderkey, l_quantity) rp leg → ~4× reduction). Runs
	// after every stage-rewiring pass so the consumer set it validates is
	// final. Kill switch WADJET_EXCHANGE_PARTIAL_AGG=0.
	markExchangePartialAgg(stages)
	// Dynamic-filter pass: must run AFTER fuseScanAggregateShuffle (which
	// may absorb an exchange-repartition into a fused scan-aggregate) but
	// BEFORE AssertExchangeConsistency / ValidateNativeDAGShape so any
	// stat-dep edges we add are visible to the validators.
	stages = p.applyDynamicFilters(ctx, stages)
	// Probe-sourced build filters for semi/anti joins (Q21's raw-lineitem
	// EXISTS/NOT-EXISTS builds, Q04, Q22): the probe dep's output key set
	// prunes the build exchange before shuffle + hash build. Runs after
	// every rewiring pass (shapes final) and independent of the legacy
	// DynamicFiltersEnabled flag. Kill switch WADJET_SEMIANTI_BUILD_FILTER=0.
	stages = p.markSemiAntiBuildFilters(ctx, stages)
	// Two-hop dimension bloom cascade (nation→supplier→lineitem class):
	// transitive semijoin reduction of a fact probe scan via a tiny
	// filtered dimension riding a chained/fused build. Cardinality-capped
	// L2-resident blooms. Kill switch WADJET_DIMENSION_CASCADE=0.
	stages = p.markDimensionCascade(ctx, stages)
	// Attach-on-arrival normalization: converts consume edges whose emitter
	// chain is scan-only and whose consumer is a terminal dispatched scan
	// into non-blocking consumes (stat-dep removed; bloom installs
	// mid-scan). Must see the FINAL emit/consume state, so it runs after
	// every marking pass. Kill switch WADJET_DF_ATTACH_ON_ARRIVAL=0.
	stages = applyAttachOnArrival(stages)
	prev := BehaviorPreservingMode
	BehaviorPreservingMode = false
	defer func() { BehaviorPreservingMode = prev }()
	if err := AssertExchangeConsistency(stages); err != nil {
		// In strict mode (Phase 2 onward, or test override) this is a
		// hard failure. BehaviorPreservingMode swallows the error inside
		// AssertExchangeConsistency, so reaching this branch implies the
		// caller flipped the var.
		return nil, fmt.Errorf("exchange consistency: %w", err)
	}
	// Attach SELECT-list aliases to the Gather stage so the coordinator can
	// rename the final result schema. walkStages currently treats NodeProject
	// as a passthrough — this surfaces the user's aliases that would otherwise
	// be lost (e.g., "n1.n_name" -> "supp_nation", "substr(l_shipdate, 1, 4)"
	// -> "l_year"). Gather drives the result schema under native-DAG.
	if renames := extractOutputRenames(node); len(renames) > 0 {
		// A group key walkStages had to resolve through a subquery's rename
		// is emitted under the SOURCE column, so the rename that names the
		// result has to read from there (#355). The general case of the same
		// passthrough (#385): a source naming a NESTED Project's alias —
		// the outer SELECT merely forwarding a subquery's rename — is chased
		// to the column the streams actually carry, because no stage ever
		// applies the rename itself.
		var renameChild *logical.Node
		if pn := findOutputProjectionNode(node); pn != nil && len(pn.Children) == 1 {
			renameChild = pn.Children[0]
		}
		// A name some stage's projection already MATERIALIZES is the name
		// the stream carries; resolving it back to a source column would
		// point the gather at the column that projection renamed away.
		// absorbAggregateOutputProjection is what makes this reachable: it
		// puts `g + 1 AS gk` on the aggregate stage during stage emission,
		// so by the time the renames are computed the stream really does
		// carry `gk` (#656).
		materialized := map[string]bool{}
		for i := range stages {
			for _, n := range stageProjectionOutputs(&stages[i]) {
				materialized[n] = true
			}
		}
		for i := range renames {
			if materialized[strings.ToLower(renames[i].From)] {
				continue
			}
			if src, ok := p.aggStageRenames[strings.ToLower(renames[i].From)]; ok {
				renames[i].From = src
				continue
			}
			if renames[i].Expr == nil {
				renames[i].From = resolveOutputRenameSourceForGather(renames[i].From, renameChild)
			}
		}
		for i := range stages {
			if stages[i].Type == StageExchangeGather {
				stages[i].OutputRenames = renames
				break
			}
		}
	}
	// The projection whose names the CLIENT reads: the three plan-time
	// declarations below are all looked up by that name (#732).
	outputProj := findOutputProjectionNode(node)

	// The gather also carries the PLAN's answer for the output schema, which
	// is what a zero-row DAG result has instead of a batch to read it off:
	// OutputRenames already gave such a result its column NAMES, and this
	// gives it their TYPES, so pgwire declares the same OIDs for an empty
	// result as for a full one (#416).
	if outSchema := republishDeclaredSchema(outputProj,
		declaredOutputSchema(node, p.subqueryOutputColumn)); len(outSchema) > 0 {
		for i := range stages {
			if stages[i].Type == StageExchangeGather {
				stages[i].OutputSchema = outSchema
				break
			}
		}
	}
	// Same PLAN-time answer as the single-process path's
	// SchemaHintWireUnconstrainedDecimal (FIX 2, #457/#458 fold-in):
	// unlike OutputSchema above, consulted on every result, not only a
	// zero-row one.
	if wireUnconstrained := republishDeclaredNames(outputProj,
		declaredWireUnconstrainedDecimal(node)); len(wireUnconstrained) > 0 {
		for i := range stages {
			if stages[i].Type == StageExchangeGather {
				stages[i].OutputWireUnconstrainedDecimal = wireUnconstrained
				break
			}
		}
	}
	// The string family's modifier, on the same stage and for the same reason
	// (#838). Both paths read the same plan-time answer, so a CAST's declared
	// length cannot depend on which one ran.
	if lengths := republishDeclaredNames(outputProj,
		DeclaredStringLengths(node)); len(lengths) > 0 {
		for i := range stages {
			if stages[i].Type == StageExchangeGather {
				stages[i].OutputStringLength = lengths
				break
			}
		}
	}
	// The catalog's declared columns, BEFORE the resolution passes rather
	// than after them. `Stage.Columns` on a scan is a READ SET — names
	// ancestors asked for — and every model of "what does this stage emit"
	// reads it, so the only thing that makes a scan's emitted set a fact is
	// intersecting it with the columns the TABLE has. ADR-0026 §4b recorded
	// that `stageStreamColumns` already performs that intersection and that
	// it was INERT, because this annotation ran at the very end of planning;
	// running it here is what makes it real, and what lets
	// `stageEmittedColumns` ask the same question (#776). The late call
	// below stays: it is idempotent and it covers stages later passes add.
	p.annotateScanSchemas(ctx, stages)
	// #169: when the SELECT list carries scalar expressions and the gather
	// reads a bare leaf scan, the expressions would never be computed —
	// applyOutputRenames can rename/drop but not evaluate. Attach the
	// SELECT list to the scan so its fragment projects it worker-side.
	stages = p.attachScanSelectProjections(node, stages)
	// A SELECT-list subquery the lowering above did not rewrite has no
	// distributed lowering: the worker's expression compiler has no
	// SubqueryRunner, so every task fails (#659). It is asked HERE rather
	// than before stage generation because whether an item is lowered is
	// decided by attachScanSelectProjections, which needs the stages — and a
	// refusal that fired for an item the DAG can now compute would keep a
	// distributable query single-process. It is asked BEFORE the assert
	// battery so this shape keeps earning ITS OWN typed refusal rather than
	// the unreachable-gather-output one, which routes to the same engine but
	// records a different cost.
	if err := refuseScalarSubqueryProjections(node, p.loweredScalarProjExprs); err != nil {
		return nil, err
	}
	// A correlated subquery parked while the SELECT list was lowered: the
	// pre-pass ran before stage generation, and this is the only site that
	// can see one found after it.
	if p.authzErr != nil {
		return nil, p.authzErr
	}
	if p.correlatedErr != nil {
		return nil, p.correlatedErr
	}
	if p.scalarRowsErr != nil {
		return nil, p.scalarRowsErr
	}
	// #424: a synthetic ORDER BY key (__sortkey_N) that the pass above did
	// not materialize — every sort but the outermost query's — still names
	// no column on the DAG. Point it at one. Runs last because the repair
	// depends on what attachScanSelectProjections did.
	resolveHiddenSortKeys(stages)
	// #467/#468: and the same repair for a key naming a DERIVED table's
	// SELECT-list alias. Runs after attachScanSelectProjections for the same
	// reason — the alias is real exactly where that pass materialized it,
	// and names the wrong column (a shadowing alias) or none at all
	// everywhere else.
	resolveDerivedAliasSortKeys(stages)
	// #656 F1: an absorbed aggregate projection retired the group key's old
	// spelling, and the gather's rename, the sort keys and the shuffle keys
	// were all written against it while the plan was still being built.
	// Retarget them at what the stage emits now. Runs here, after every pass
	// that can add or move a reference, and before the filter-spelling pass
	// reads them.
	retargetAbsorbedAggregateRenames(stages, p.aggProjectionRenames)
	// #656: and the same repair for a PREDICATE that names a derived table's
	// SELECT-list alias. Runs after attachScanSelectProjections for the same
	// reason the two sort-key passes do — the alias is real exactly where
	// that pass materialized it.
	resolveFilterAliasSpelling(stages)
	// ADR-0026 §2: and the same repair for a GROUP BY KEY that names a derived
	// table's computed alias. It runs here, after the projection passes, for
	// exactly the reason the two above do — whether any fragment publishes the
	// alias is what those passes decide — and it is the half `walkStages`
	// cannot do, which is why three attempts to infer it from node kinds each
	// bound a different wrong column (§4a, #777, #781, #794).
	if err := resolveStageGroupKeys(stages); err != nil {
		return nil, err
	}
	// #656 F2: a SELECT list that became nobody's job. Refused HERE rather
	// than at dispatch so the coordinator can route the query local and
	// ANSWER it, instead of handing the client the producer's raw columns.
	// Late, after flattenCTEAliases has repointed every deduped reference:
	// an arm's projection has to be written against what its producer really
	// emits.
	respellUnionArmProjections(stages)
	// And the payload the two passes above just made a join stage read. Its
	// OutputFilter and its exchanges' manifests were built from the join
	// node's NeededColumns, before either pass existed to add a name, so a
	// column the fragment is about to evaluate could be missing from the
	// shuffle entirely (join_carried_columns.go).
	ensureJoinCarriesEvaluatedColumns(stages)
	ensureJoinCarriesGatherOutputs(stages)
	// ADR-0026 §2's two names, applied to a JOIN's consumers: a group key
	// resolves by the spelling the join PUBLISHES for the value, and the
	// payload carries only what no published spelling reaches
	// (published_identity.go, #770). Runs after both carry passes because it
	// asks what the fragment's input will really SHIP, which is what those
	// passes have just settled.
	bindConsumersToPublishedIdentity(stages)
	if err := assertJoinFiltersAreBacked(stages); err != nil {
		return nil, err
	}
	if err := assertGatherOutputIsReachable(stages); err != nil {
		return nil, err
	}
	// The same refusal for a sort key nothing supplies: loud at dispatch,
	// answerable on the local engine.
	if err := assertSortKeysResolve(stages); err != nil {
		return nil, err
	}
	// And for an aggregate ARGUMENT its own input cannot supply, which is
	// SILENT rather than loud — the pre-projection writes NULL into every row
	// and the aggregate answers a wrong number (#702). It refuses HERE, with
	// the sentinel, so the coordinator routes the query to its local engine
	// and ANSWERS it, rather than failing the client for a shape PostgreSQL
	// has no trouble with.
	if err := assertAggregateInputsResolve(stages); err != nil {
		return nil, err
	}
	// And for set-operation arms that disagree about a column's type, which
	// is a PANIC inside the consumer's fragment rather than a loud failure.
	if err := assertUnionArmsAgreeOnTypes(stages); err != nil {
		return nil, err
	}
	// And for a stage the dispatcher could not build task inputs for at all —
	// one naming neither a dependency nor a table. #806 refused ONE producer
	// of that shape (a table-less SELECT's `dual` stage); this asks the same
	// question of the FINISHED stage list, which catches the second one
	// (#812, a scalar subquery over a CTE in a WHERE clause) and any third.
	// Loud at dispatch with an internal message and no SQLSTATE; answerable
	// on the local engine.
	if err := refuseUnbuildableStages(stages); err != nil {
		return nil, err
	}
	// #423: the worker's scan reads column TYPES from the FILE, and a
	// parquet file cannot express nine of ours. Declare the catalog's
	// schema for every table this plan scans so a file written before the
	// footer key existed is still typed the way the catalog says.
	p.annotateScanSchemas(ctx, stages)
	// #491: and the same declare-on-the-plan treatment for the table's
	// merge-on-read DELETE state, replayed from the snapshot walkStages
	// took rather than a second manifest read.
	p.annotateScanDeletes(stages)
	// LAST, after every pass that can move or drop a projection: wire each
	// lowered SELECT-list placeholder to the stage that carries it, so the
	// coordinator awaits that producer and substitutes its value before
	// dispatch (#659). A placeholder no stage carries, and a producer that
	// would be awaited by a stage it READS, are both refusals the coordinator
	// routes on rather than a `:scalar_N` shipped to a worker.
	if err := p.attachProjectionScalarDependencies(stages); err != nil {
		return nil, err
	}
	// LAST, after every rewriting pass, for the reason the security-filter
	// order check below is asked last: a null-aware anti join whose build is
	// partitioned answers its NOT EXISTS twin, and no operator downstream can
	// notice (#539/#507).
	if err := assertNullAwareAntiBuildsAreReplicated(stages); err != nil {
		return nil, err
	}
	// LAST, after every rewriting pass: no predicate below a security
	// projection may read a column that projection hides. A pass that copied
	// FilterExprs without its PostSecurityFilterExprs companion would put one
	// back, and the failure mode is a per-row disclosure rather than a wrong
	// count (#859 round 2), so this refuses rather than trusting the routing.
	if err := CheckSecurityFilterOrder(ctx, stages); err != nil {
		return nil, err
	}
	return stages, nil
}
