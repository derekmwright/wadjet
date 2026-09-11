// This file holds planner config for the physical planner, governed by ADR-0034.
package physical

import (
	"context"
	"os"
	"sync"

	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Planner converts logical plans to physical plans.
type Planner struct {
	catalog        *catalog.Catalog
	subqueryRunner expr.SubqueryRunner
	planCtx        context.Context  // context from the current Plan() call, used by subquery runner
	ctes           []plansql.CTEDef // CTE definitions from the current query, for subquery resolution
	// outputProjection is the Project whose names LEAVE the engine, resolved
	// once per Plan() call. Only that projection publishes PostgreSQL's
	// FigureColname (Projection.PublishedName, #732): a nested block's names
	// are what the block above resolves against.
	outputProjection *logical.Node
	MemoryBudget     int64  // per-query memory budget in bytes (0 = unlimited)
	SpillDir         string // directory for spill files (empty = os temp dir)

	// ManifestSnapshot pins each table's manifest to one catalog read for
	// this statement (#502). NewPlanner sets a fresh one; forSubquery's
	// shallow copy shares it with every child/subquery planner. A caller
	// that builds several Planner instances for one statement must assign
	// the SAME snapshot to each — see ManifestSnapshot's doc.
	ManifestSnapshot *ManifestSnapshot

	// SharedTracker / SharedSpillMgr (if set) are used in place of per-query
	// Tracker+SpillManager creation. Workers set these to point at the
	// executor-level pool so concurrent tasks on the same worker compete for
	// ONE budget and spill cooperatively under pool pressure, matching the
	// Trino/Spark unified memory manager model. When nil, getSpillManager()
	// falls back to creating a per-query pool as before.
	SharedTracker  *memory.Tracker
	SharedSpillMgr *memory.SpillManager
	QueryLimits    *config.QueryLimits // cost-based query guard (nil = no limits)
	// subqueryDeclCache memoizes a scalar subquery's declared output column
	// by its SQL text, for annotateSubqueryColumnDecls. A nil slot means "in
	// flight" and breaks a recursion: resolving one subquery plans it, and
	// planning it annotates ITS projections in turn.
	//
	// OWNED PER BUILD, and forSubquery resets it for that reason. This map is
	// unsynchronized and `sub := *p` copies only its header, while the
	// subquery runner baked into a compiled expression is reached once per row
	// from every parallel pipeline goroutine — so an INHERITED map is N child
	// planners writing one map concurrently, which `-race` reports inside
	// scalarSubqueryColumnDecl and which the Go runtime may turn into a fatal
	// concurrent map write that no recover() can catch (#1018 round 6 review,
	// B2). Sharing it across builds bought nothing the memo promises: the
	// saving it exists for is a subquery text written TWICE IN ONE PLAN, and
	// that is a within-build question.
	subqueryDeclCache map[string]*subqueryDeclEntry
	cteCache          map[string]*cteMaterialized // materialized CTE results
	scanCache         map[string]*scanCached      // cached scan results for duplicate table scans
	res               *queryResources             // per-query spill manager + memory tracker (lazy, shared with child planners)
	WorkerCount       int                         // number of distributed workers (for shuffle partitioning)

	// builtJoins are the HashJoins this plan owns. HashJoin.Close is the
	// only thing that returns the build's tracker reservation and removes
	// the grace build's partition files, and its own doc comment says the
	// OWNER must call it — on the distributed path the fragment executor
	// does, and on the single-process path nobody did. The plan's Cleanup
	// is that owner (#625, ADR-0028). Cleanup runs after Pipeline.Close
	// (defer order at the call sites), so nothing is freed under a live
	// operator.
	builtJoins []*exec.HashJoin

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

	// SortMergeJoinBytes gates the sort-merge join path for big-vs-big inner
	// equi-joins (docs/design/sort-merge-join.md). A join takes SMJ only when
	// BOTH sides' estimated post-selectivity bytes reach this threshold —
	// small builds keep the strictly-better hash/broadcast paths. Zero (the
	// shipped default) disables SMJ entirely; the planner behaves exactly as
	// before. Distributed callers should derive it from per-worker pool
	// budget (the broadcast-threshold pattern) so "too big to sit resident"
	// tracks cluster memory.
	SortMergeJoinBytes int64

	// LateMaterialization emits inner/left hash-join output as view
	// (dictionary) columns over the probe input and build batches; the
	// gather is deferred to the first consumer needing owned storage
	// (docs/design/late-materialization.md). Off by default.
	LateMaterialization bool

	// DynamicFiltersEnabled gates the Trino-style dynamic-filter planner
	// pass. When true, applyDynamicFilters annotates eligible hash_join
	// build/probe leaf scans with Emit/Consume specs and adds the stat-dep
	// edge from build-scan to probe-scan. Off by default for v1 rollout;
	// distributed callers flip this on after the local-harness gate passes.
	DynamicFiltersEnabled bool

	// MaterializedInputs holds pre-scanned data for scan-split pipeline mode.
	// When populated, buildScan uses these batches instead of reading from the
	// object store, allowing parallel scan I/O with single-worker compute.
	// Keyed by scan alias: "table" or "table:N" for self-joins.
	MaterializedInputs map[string][]*batch.RecordBatch

	// StreamingSources holds lazy sources for scan-split pipeline mode.
	// Unlike MaterializedInputs, these yield batches on demand without
	// materializing all data upfront. Checked before MaterializedInputs.
	// Keyed by scan alias: "table" or "table:N" for self-joins.
	StreamingSources map[string]exec.Source

	// ScanFileFilter restricts which files each scan alias reads. Used in
	// probe-split pipeline mode where the probe table is partitioned across
	// workers while build tables read all files. Keyed by scan alias.
	ScanFileFilter map[string][]string

	// scanCounter tracks the N-th scan of each table for alias resolution.
	// It is per-BUILD scratch, not query state: it numbers the scans of one
	// pipeline build so MaterializedInputs / StreamingSources / ScanFileFilter
	// lookups hit the right "table" / "table:N" key. forSubquery gives every
	// subquery build a fresh one.
	scanCounter map[string]int

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

	// joinCondErr records an ON clause parseJoinKeys cannot represent as a
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

// refuseJoin parks the first refusal; PlanDistributed returns it. First one
// wins so a nested join's specific message is not overwritten by an outer
// one's.
func (p *Planner) refuseJoin(err error) {
	if p.joinCondErr == nil {
		p.joinCondErr = err
	}
}

// queryResources holds the spill manager and memory tracker for one query.
// They belong to the QUERY, not to a single pipeline build: the root planner
// and every child planner spawned by forSubquery share one instance, so a
// query has exactly one budget and exactly one spill directory for Plan's
// Cleanup to sweep.
//
// mu guards the lazy construction. A correlated subquery builds its pipeline
// from the execution goroutines (exec.Pipeline.runParallel), so two builds can
// otherwise race to create the manager — and each would create its own spill
// directory that nothing releases.
type queryResources struct {
	mu         sync.Mutex
	spillMgr   *memory.SpillManager
	memTracker *memory.Tracker
	// inSubqueries are the uncorrelated IN-subquery membership sets compiled
	// under this query's budget. They are held HERE rather than on the
	// Planner because forSubquery copies the Planner by value and shares only
	// this holder — a nested subquery's charge has to reach the same release.
	inSubqueries []*expr.InSubquery
}

// resources returns this planner's shared per-query resources, allocating the
// holder if a caller constructed the Planner as a bare struct literal rather
// than through NewPlanner. Both NewPlanner and Plan set it, so on every real
// path the holder exists well before any goroutine fan-out.
func (p *Planner) resources() *queryResources {
	if p.res == nil {
		p.res = &queryResources{}
	}
	return p.res
}

// getSpillManager returns a shared per-query spill manager, creating it on
// first call. Uses MemoryBudget if set, otherwise auto-detects from system
// memory (cgroup or physical). Returns nil if no memory limit can be determined.
func (p *Planner) getSpillManager() *memory.SpillManager {
	// Shared pool (worker-level, injected by Executor) takes precedence so
	// all concurrent tasks spill against one budget.
	if p.SharedSpillMgr != nil {
		return p.SharedSpillMgr
	}
	r := p.resources()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.spillMgr != nil {
		return r.spillMgr
	}
	budget := p.MemoryBudget
	if budget <= 0 {
		budget = memory.DetectBudget()
	}
	if budget <= 0 {
		// Fall back to 75% of physical memory
		if phys := memory.DetectPhysicalMemory(); phys > 0 {
			budget = int64(float64(phys) * 0.75)
		}
	}
	if budget <= 0 {
		return nil
	}
	tracker := memory.NewTracker("query", budget)
	dir := p.SpillDir
	if dir == "" {
		dir = os.TempDir()
	}
	sm, err := memory.NewSpillManager(dir, tracker)
	if err != nil {
		return nil
	}
	r.memTracker = tracker
	r.spillMgr = sm
	return sm
}

// getMemTracker returns the shared per-query memory tracker.
// Must be called after getSpillManager().
func (p *Planner) getMemTracker() *memory.Tracker {
	// Shared pool takes precedence (worker-level pool across tasks).
	if p.SharedTracker != nil {
		return p.SharedTracker
	}
	r := p.resources()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.memTracker
}

// subqueryBudgetOption charges an uncorrelated IN-subquery's membership set to
// this query's tracker, and records the node so the charge is released when the
// plan is torn down (#531, ADR-0006).
//
// Returns nil — a no-op option — when this query has no tracker, which keeps
// the unbudgeted behavior for embedded callers that set no MemoryBudget.
//
// Why the charge needs an owner at all. An InSubquery holds its membership map
// for the life of the compiled Expr tree: `id IN (SELECT id + 0 FROM t)`
// declines decorrelation (a computed inner item is not a semi-join key), so it
// builds a hash set of every inner row and probes it per row. Measured on the
// type-matrix fixture, that set is 120,000 bytes and the query ANSWERED at an
// 8 KiB budget — 14.6× the whole allowance, unaccounted, while the same run
// logged the scan forcing its file load past that budget. The shapes that DO
// decorrelate never reach this type; their build side is already budgeted and
// spillable.
func (p *Planner) subqueryBudgetOption() expr.CompileOption {
	tracker := p.getMemTracker()
	if tracker == nil {
		return nil
	}
	r := p.resources()
	return expr.WithBudget(tracker, func(in *expr.InSubquery) {
		r.mu.Lock()
		r.inSubqueries = append(r.inSubqueries, in)
		r.mu.Unlock()
	})
}

// releaseSubqueryCharges returns every budgeted IN-subquery membership set to
// the tracker. Idempotent, and safe on a node whose subquery never ran.
//
// This is the teardown point #531 needed and InSubquery.Release had no caller
// for: wiring the charge without one converts an unaccounted map into a
// permanently-charged one, which is worse than the bug. A compiled Expr tree's
// life ends with the plan that built it, so the plan's Cleanup owns this — the
// same ownership ADR-0028 gave the grace join's build reservation.
//
// It hangs off queryResources rather than off the Planner because Plan REPLACES
// p.res on entry: a Cleanup that reached for p.resources() at teardown time
// would release the NEXT query's charges and leave its own. The holder is
// captured when the Cleanup is built.
func (r *queryResources) releaseSubqueryCharges() {
	r.mu.Lock()
	nodes := r.inSubqueries
	r.inSubqueries = nil
	r.mu.Unlock()
	for _, in := range nodes {
		in.Release()
	}
}

// hasSubqueryCharges reports whether anything is waiting for
// releaseSubqueryCharges, so Plan can attach a Cleanup for it alone.
func (r *queryResources) hasSubqueryCharges() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.inSubqueries) > 0
}

// spillManagerIfSet returns the per-query spill manager without creating one.
func (p *Planner) spillManagerIfSet() *memory.SpillManager {
	r := p.resources()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.spillMgr
}

// cteMaterialized stores the pre-computed results of a CTE in one of two
// forms. The columnar form (coll) is the production shape: batches held in
// a tracker-charged, spill-backed collector and replayed without consuming
// it, so a multi-gigabyte CTE never sits boxed in heap (sweep finding #13 /
// issue #127). The boxed form (rows) remains for the recursive work table —
// one iteration's delta, re-seeded into the cache each fixed-point step.
type cteMaterialized struct {
	schema []parquet.Column
	rows   []map[string]any              // boxed form; nil when coll is set
	coll   *exec.SpillableBatchCollector // columnar form; nil when rows is set
}

// scanCached stores columnar scan results for a table that is scanned multiple
// times within a single query (e.g., decorrelated subqueries). The first scan
// populates the cache, and subsequent scans replay from it. Thread-safe for
// concurrent builds: subsequent scans block on ready until the first completes.
//
// The cache holds the UNION of all consumers' columns (unionCols) so one
// scan serves everyone; each consumer projects back down to its own
// columns in catalogScanSource.Next. Without the projection, every
// consumer — including hash-join BUILD sides, which store their input
// batches — carried the widest consumer's columns: Q21's two 60M-row
// lineitem semi/anti builds each stored l1's 4-column set instead of
// the 2 columns they need, and partition-on-arrival evictions spilled
// the dead columns to disk (the SF10 cold-S3 Q21 churn, 2026-07-06).
//
// The cache's vectors are shared with its consumers and are therefore
// NOT charged to the memory tracker here — see the comment at the
// append site in catalogScanSource.Next for the 2026-07-06 stall
// incident that rule prevents. Batches are dropped by
// Planner.releaseScanCache on every Plan exit path.
type scanCached struct {
	mu      sync.Mutex
	batches []*batch.RecordBatch
	schema  []parquet.Column
	done    bool          // true when the first scan is complete
	ready   chan struct{} // closed when the claiming scan FINISHES, successfully or not; nil until a scan claims the cache
	// abandoned is set when the claiming scan finished WITHOUT filling the
	// cache — it failed, or it was closed before reaching the end of the
	// table. err carries why. Waiters must not replay a cache in this state:
	// it holds only the batches read before the scan stopped, so replaying it
	// would answer a query from a TRUNCATED table.
	abandoned bool
	err       error
	// unionCols is the union of every consumer's RequiredColumns, in
	// first-seen order. nil = full schema (some consumer needs all).
	unionCols []string
}

// NewPlanner creates a new physical planner.
func NewPlanner(cat *catalog.Catalog) *Planner {
	p := &Planner{catalog: cat, res: &queryResources{}, ManifestSnapshot: NewManifestSnapshot()}
	// Create a subquery runner that re-uses this planner for nested queries
	p.subqueryRunner = p.makeSubqueryRunner()
	return p
}
