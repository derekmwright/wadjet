// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// StagePlanner is the DISTRIBUTED planner: the stage DAG, the exchange and
// distribution assignment, the shuffle fusions and the refusals that belong to
// them. It embeds the local physical.Planner because stage emission reads the same
// catalog, the same CTE definitions and the same declared-output logic the
// single-process pipeline is built from — and because the direction of that
// dependency is the whole point: the distributed planner needs the local one,
// never the reverse (ADR-0037, LICENSING.md).
//
// Its fields are the per-build scratch stage emission keeps. They were fields
// of physical.Planner, which meant the embedded engine carried the stage emitter's
// state on every query it planned.
type StagePlanner struct {
	*physical.PlanContext

	WorkerCount int // number of distributed workers (for shuffle partitioning)

	// BroadcastBytesThreshold is the maximum estimated build-side size for
	// a join to be planned as broadcast_join. Builds above this become
	// hash_join (hash-shuffle), eliminating the N× build-cache duplication
	// every worker pays under broadcast.
	//
	// Zero = use absolute default (100 MB), preserving legacy behavior for
	// embedded callers that don't set this. Distributed callers should set
	// this from per-worker pool budget — e.g., budget * 0.3 capped at
	// 200 MB — so the broadcast/shuffle decision adapts to cluster memory
	// instead of trusting an absolute constant. Negative = never broadcast
	// (force every join to hash_join).
	//
	// Architectural rule the threshold encodes: broadcast is sound when
	// every worker can comfortably hold the full build state in memory
	// alongside concurrent hash tables. As cluster width or per-worker
	// budget shrinks, the threshold shrinks too — without changing the
	// planner's join-type logic.
	BroadcastBytesThreshold int64

	// DynamicFiltersEnabled gates the Trino-style dynamic-filter planner
	// pass. When true, applyDynamicFilters annotates eligible hash_join
	// build/probe leaf scans with Emit/Consume specs and adds the stat-dep
	// edge from build-scan to probe-scan. Off by default for v1 rollout;
	// distributed callers flip this on after the local-harness gate passes.
	DynamicFiltersEnabled bool

	// scalarPlaceholderSeq allocates unique ":scalar_N" placeholder names
	// across a single query when resolveFilterSubqueries defers CTE-
	// referencing subqueries for late coordinator-side substitution.
	scalarPlaceholderSeq int

	// projScalarProducers maps a SELECT-list placeholder to the producer
	// stage that computes it and the type that producer declares (#659).
	// The predicate path records its edges directly on the filter-carrying
	// stage; a projection's carrier is not known until every attach and
	// respell pass has run, so the edges are collected here and wired at the
	// end (attachProjectionScalarDependencies).
	projScalarProducers map[string]projScalarProducer

	// loweredScalarProjExprs is the set of SELECT-list items the lowering
	// above rewrote, keyed by the projection's own ADDRESS so that two items
	// which merely spell the same expression cannot share a verdict —
	// refuseScalarSubqueryProjections then refuses exactly the items no stage
	// will compute.
	loweredScalarProjExprs map[*logical.Projection]bool

	// ctePlannedTerminal caches the terminal stage ID emitted while walking
	// a CTE's subtree, keyed by `cteName + "|" + structuralHash(subtree)`.
	// On a second walk of a structurally-identical CTE clone, walkStages
	// links the parent's deps to the cached terminal and skips re-emitting
	// duplicate stages. Eliminates the dual-chain float drift that fails
	// Q15 ~50% under multi-file scans (project_q15_dual_chain_float_drift).
	// Reset at the start of generateStages so each query gets a fresh map.
	ctePlannedTerminal map[string]string
	// cteTerminals maps a CTE body's terminal stage ID to whether that CTE
	// is referenced MORE THAN ONCE. Presence means "this stage is a CTE
	// body's output, so a Filter above it belongs to ONE reference"; a true
	// value means every reference reads it, so nothing consumer-specific may
	// be attached to it at all — see filterCarrierIndex.
	cteTerminals map[string]bool
	// cteRefCounts is how many times each CTE name appears in the logical
	// plan, counted once before the walk because the second reference is
	// only discovered after the first has already emitted its stages.
	cteRefCounts map[string]int

	// starReadBlocks is the set of derived-block Project nodes a STAR reads
	// by POSITION, computed once before the walk because the answer depends
	// on what stands ABOVE a node and walkStages descends. walkStages'
	// `default:` arm publishes each of them onto the stage that materializes
	// it, so the relation above the block is the one the query wrote (#984).
	starReadBlocks map[*logical.Node]blockDivergence
	// publishedBlocks is the subset of starReadBlocks the pass really
	// materialized. Every CONSUMER reads this one — the join's keys, its
	// OutputFilter, the declaration for an empty side and the hidden slot's
	// ordinal — because a block the pass declined still emits its stream.
	publishedBlocks map[*logical.Node]bool

	// scanDeletes caches the merge-on-read DELETE state walkStages read for
	// each base table, table name → (file path → file-absolute deleted row
	// indices). Captured from the SAME manifest object that produced the
	// stage's ScanFiles, so the pair is a snapshot; annotateScanDeletes
	// then replays it onto the FINAL stage list, which is where a fused or
	// rewritten stage that ends up owning those files can be reached.
	// Re-reading the manifest in that late pass instead would reintroduce
	// exactly the skew the snapshot exists to avoid (see Stage.ScanDeletes).
	// Reset at the start of generateStages.
	scanDeletes map[string]map[string][]int64

	// limitStageRoot is the node generateStages was entered with — the one
	// node whose LIMIT the coordinator's post-gather pass can see, because
	// logical.ExtractMergeInfo reads the plan root and nothing below it.
	// needsLimitStage compares against it to decide which LIMITs still need
	// a stage of their own (#478). Set at the start of generateStages.
	limitStageRoot *logical.Node

	// setOpErr records a set operation walkStages cannot lower to stages.
	// walkStages has no error return (it is recursive over ~20 node kinds
	// and every other case is total), so the refusal is parked here and
	// PlanDistributed turns it into a planning error. Refusing is the
	// point: the alternative this replaces was emitting each arm's stages
	// and no merge, which returned ONE arm's raw output as the query's
	// answer (#346). Reset at the start of generateStages.
	setOpErr error

	// joinCondErr records an ON clause physical.ParseJoinKeys cannot represent as a
	// key pair, parked for the same reason setOpErr is: walkStages has no
	// error return. Refusing is the point — the alternative it replaces was
	// passing the unrepresentable operand to the executor AS A COLUMN NAME,
	// which resolves to nothing and silently matches either no rows or every
	// row (#351). Reset at the start of generateStages.
	joinCondErr error

	// correlatedErr records a per-row correlated subquery found during stage
	// generation (#359), parked for the same reason the two above are. The
	// primary detection is refuseCorrelatedSubqueries, a pre-pass over the
	// logical plan; this field is its structural backstop at the deferral
	// seam — resolveSubqueryAST refuses to defer or eagerly execute a
	// subquery that is not self-contained (plansql.DanglingTableRefs),
	// because a producer stage executing it standalone evaluates the
	// dangling outer reference to NULL and the query silently answers 0.
	// Reset at the start of generateStages.
	correlatedErr error
	// inSubqueryErr records an IN-subquery the planner could not materialize
	// into a literal set, for the same reason and by the same mechanism as
	// correlatedErr. See in_subquery_set.go.
	inSubqueryErr error
	// scalarRowsErr records a refusal this planner raised while EVALUATING a
	// subquery at plan time — a cardinality violation (21000) or an
	// authorization decision (42501). Neither is a routing refusal: both are
	// the query's answer on every path, so re-running locally would reach the
	// same error after doing the work twice.
	//
	// scalarRowsErr records a SCALAR subquery this planner executed at plan
	// time that returned more than one row. Parked rather than returned for
	// the same reason as the three above — walkStages has no error return —
	// and kept in its OWN field because it is not a routing refusal: it is
	// the query's answer (SQLSTATE 21000), and routing it to the local
	// pipeline would only reach the same error one layer down after doing
	// the work twice. Reset at the start of generateStages.
	scalarRowsErr error
	// authzErr records an AUTHORIZATION refusal raised while stages were
	// generated — a scalar subquery's producer plan asking the context lookup
	// about a relation the identity may not read (#945).
	//
	// Its own field, and the FIRST one PlanDistributed returns, because it is
	// the opposite of a routing refusal: the other four say "this planner
	// cannot express the shape here, run it somewhere else", and every site
	// that raises one falls back or declines. An authorization refusal must
	// never be fallen back on — the fallbacks re-run the same subquery on the
	// coordinator, and when that refused too the ORIGINAL subquery text was
	// spliced back into a worker's filter, which reached the client as
	// `subqueries require a SubqueryRunner` after three task attempts instead
	// of as the refusal. Reset at the start of generateStages.
	authzErr error

	// aggStageRenames maps a name an Aggregate node reads in the LOGICAL plan
	// to the name the aggregate STAGE emits for it, for every group key
	// walkStages had to resolve through a subquery's rename (#355). The
	// gather's output renames are rewritten through it, or a query grouping
	// on a renamed column answers under the source column's name while the
	// single-process path answers under the alias. Reset at the start of
	// generateStages.
	aggStageRenames map[string]string

	// attachedFilterExprs records the predicates walkStages attached to a
	// stage during the last PlanDistributed. AttachedFilterExprs exposes it
	// for the conservation gate; see filter_carrier.go.
	attachedFilterExprs []string
	// aggProjectionRenames is every rename absorbAggregateOutputProjection
	// performed on an aggregate stage, lowercased old name to new. The stage
	// stops emitting the old name, so every downstream reference written
	// against it has to be retargeted (retargetAbsorbedAggregateRenames).
	aggProjectionRenames []aggRenameSite
	// attachedProjectionOutputs records the projection OUTPUT names that
	// were on a stage the moment stage emission finished. A pass that
	// deletes the carrying stage drops the projection exactly as it drops a
	// predicate, and only tracking both catches it.
	attachedProjectionOutputs []string
}

// NewStagePlanner wraps a local planner for stage planning. The local planner
// carries the catalog, the CTE definitions, the memory configuration and the
// declared-output logic; everything the DAG adds lives on the returned value
// and is per-build scratch.
func NewStagePlanner(p *physical.Planner) *StagePlanner {
	return &StagePlanner{PlanContext: p.PlanContext()}
}
