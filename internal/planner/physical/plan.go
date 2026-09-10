// Package physical converts logical plans to physical execution plans.
package physical

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	QueryLimits    *config.QueryLimits         // cost-based query guard (nil = no limits)
	cteCache       map[string]*cteMaterialized // materialized CTE results
	scanCache      map[string]*scanCached      // cached scan results for duplicate table scans
	res            *queryResources             // per-query spill manager + memory tracker (lazy, shared with child planners)
	WorkerCount    int                         // number of distributed workers (for shuffle partitioning)

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

// makeSubqueryRunner creates a SubqueryRunner that executes SQL via this planner.
// Uses the planCtx stored during Plan() so subqueries respect the parent context's
// cancellation and timeout.
//
// Each call gets its own child planner. This is the runner baked into every
// compiled expression, so a correlated subquery reaches it once per row from
// every parallel pipeline goroutine — see forSubquery for why sharing the
// parent's build scratch across those goroutines is not an option.
func (p *Planner) makeSubqueryRunner() expr.SubqueryRunner {
	return func(sql string) ([]map[string]any, error) {
		ctx := p.planCtx
		if ctx == nil {
			ctx = context.Background()
		}
		return p.forSubquery().executeSubquery(ctx, sql)
	}
}

// forSubquery returns a child planner for building and running ONE subquery
// pipeline.
//
// A correlated subquery is executed per row from the pipeline's parallel
// worker goroutines (exec.Pipeline.runParallel), and each execution runs a
// full physical build. Those builds must not share the parent's per-build
// scratch, for two independent reasons:
//
//   - It is a data race. scanCounter is a plain map written by buildScan, so
//     concurrent builds crash the process with "fatal error: concurrent map
//     writes" — an unrecoverable throw that takes down every other connection
//     in server mode (issue #334).
//   - It is wrong even when serialized. scanCounter numbers the scans of one
//     build; letting the outer build's count leak in makes a subquery's first
//     scan of customer resolve as alias "customer:1", so the ScanFileFilter /
//     MaterializedInputs / StreamingSources lookups keyed by that alias miss.
//     A mutex would hide the crash and keep the mis-keying.
//
// So the child gets fresh build scratch, and shares everything that genuinely
// belongs to the query: the catalog, the CTE definitions and their
// materialized cache, the scan cache, the memory/spill resources, and all
// configuration. Sharing those is what keeps one budget, one spill directory,
// and one materialization of each CTE per query; a per-goroutine copy would
// leak spill directories that Plan's Cleanup never sees.
//
// The shared maps (cteCache, scanCache) are populated by Plan before execution
// begins and are read-only from here on; scanCached carries its own mutex for
// the concurrent-replay case.
//
// The scan-alias injections (MaterializedInputs, StreamingSources,
// ScanFileFilter) are dropped. They describe the ENCLOSING fragment's scans —
// a worker's probe-split file slice, a scan-split pre-scan — keyed by that
// plan's aliases. A subquery is its own query over the catalog and must see
// the whole table; binding it to the fragment's file slice would answer it
// from one worker's shard. Today they are missed only because the shared
// counter happens to push the subquery's aliases past the injected keys, so
// dropping them makes the existing behavior explicit rather than incidental.
func (p *Planner) forSubquery() *Planner {
	sub := *p
	sub.scanCounter = nil
	sub.ctePlannedTerminal = nil
	sub.scanDeletes = nil
	sub.scalarPlaceholderSeq = 0
	sub.MaterializedInputs = nil
	sub.StreamingSources = nil
	sub.ScanFileFilter = nil
	sub.res = p.resources()
	// Nested subqueries inside this one recurse through the same rule.
	sub.subqueryRunner = sub.makeSubqueryRunner()
	return &sub
}

// buildSubqueryPipeline parses, plans, and builds (but does not run) the
// physical pipeline for a SQL subquery, merging the enclosing WITH clause's
// CTEs so the subquery can reference them. Shared by executeSubquery (boxed
// results) and materializeCTEColumnar (columnar collection).
// subqueryDeclOption is the expr.CompileOption that lets a compiled scalar
// subquery carry its OUTPUT DECLARATION, so a comparison against it is made at
// the type the subquery answers rather than by the bytes of the box (#696).
//
// It plans the subquery's SQL — parse, logical build, annotate — and reads
// declaredOutputSchema, the same walk the top-level statement's own output
// schema comes from. No execution: the question is the TYPE, and the value is
// resolved once at evaluation as it always was. A subquery that does not
// resolve to exactly one column answers ok=false and the comparison keeps the
// boxed rules it had.
//
// The cost is one logical build per compiled scalar subquery, at plan time.
func (p *Planner) subqueryDeclOption() expr.CompileOption {
	env := expr.WithSubqueryEnv(func(sql string) (parquet.TypeID, int, int, bool) {
		cols, ok := p.subqueryOutputColumn(sql)
		if !ok {
			return 0, 0, 0, false
		}
		return cols.Type, cols.Precision, cols.Scale, true
	}, p.subqueryOutputArity)
	// …and the RELATION resolver the dangling-reference guard needs to tell a
	// ROW FIELD PATH from a lost correlation (#866). It travels with the
	// other two plan-time answers because it is the same question asked of
	// the same plan, and a compile site that took only the first two would
	// refuse `d.b IN (SELECT c_row.b FROM t)` — a query PostgreSQL answers.
	return expr.Options(env, expr.WithSubqueryScope(p.subqueryInnerColumns()))
}

// subqueryOutputArity is how many columns a subquery's SELECT list has, from
// the subquery's OWN PLAN.
//
// It is what lets a construct requiring ONE column refuse before the subquery
// runs, which is PostgreSQL's order — `subquery must return only one column`
// is a parse-analysis error there, so it fires over an EMPTY subquery too
// (round-1 P1). Counting rows cannot reach that case and counting the row map
// cannot see two columns that share a name (round-1 B1); the declared schema
// is positional and knows both.
//
// It recovers from a panic and answers not-known for anything it cannot plan,
// exactly as subqueryOutputColumn does and for the same reason: an unplannable
// subquery must cost the refusal its evidence, never the query its answer.
func (p *Planner) subqueryOutputArity(sql string) (n int, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			n, ok = 0, false
		}
	}()
	ctx := p.planCtx
	if ctx == nil {
		ctx = context.Background()
	}
	pq, err := plansql.Parse(sql)
	if err != nil {
		return 0, false
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return 0, false
	}
	var plan *logical.Node
	if len(p.ctes) > 0 {
		plan, err = logical.BuildFromSelectWithCTEs(info,
			append(append([]plansql.CTEDef(nil), p.ctes...), info.CTEs...))
	} else {
		plan, err = logical.BuildFromSelect(info)
	}
	if err != nil || plan == nil {
		return 0, false
	}
	p.AnnotateScanColumns(ctx, plan)
	schema := declaredOutputSchema(plan, p.subqueryOutputColumn)
	if len(schema) == 0 {
		// A shape this walk cannot name — a star it could not expand, a
		// projection it cannot read. Not-known, and the row-count backstop
		// still applies.
		return 0, false
	}
	return len(schema), true
}

// DeclaredOutputSchema is the PLAN-TIME declaration of a statement's output
// columns — the same walk `Plan` stamps on a single-process pipeline as
// `Plan.OutputSchema` — for a door that assembles a result set from batches it
// may not have.
//
// The ASYNC door is that door (#1008 round 2): `SubmitSQL` plans stages and
// `GetQueryResults` reads the columns off the gathered batches, of which a
// ZERO-ROW query has none, and the DAG's own `GatherOutputSchema` describes a
// gather stage that a one-stage plan does not have. Every zero-row SELECT
// therefore came back with no columns at all on that door while the other
// three described it from exactly this walk. Calling it rather than copying it
// is what keeps the four doors' answers the same list.
//
// The caller has already annotated the plan; this does not re-annotate,
// because a second AnnotateScanColumns over an optimized plan is not free and
// the walk needs only what the first one left.
func (p *Planner) DeclaredOutputSchema(plan *logical.Node) []parquet.Column {
	if plan == nil {
		return nil
	}
	return declaredOutputSchema(plan, p.subqueryOutputColumn)
}

// subqueryOutputColumn resolves a scalar subquery's single declared output
// column. It recovers from a panic for the reason every plan-time helper on
// this path does: an unplannable subquery must cost the comparison its
// declaration, never the query.
func (p *Planner) subqueryOutputColumn(sql string) (col parquet.Column, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			col, ok = parquet.Column{}, false
		}
	}()
	ctx := p.planCtx
	if ctx == nil {
		ctx = context.Background()
	}
	pq, err := plansql.Parse(sql)
	if err != nil {
		return parquet.Column{}, false
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return parquet.Column{}, false
	}
	var plan *logical.Node
	if len(p.ctes) > 0 {
		plan, err = logical.BuildFromSelectWithCTEs(info, append(append([]plansql.CTEDef(nil), p.ctes...), info.CTEs...))
	} else {
		plan, err = logical.BuildFromSelect(info)
	}
	if err != nil || plan == nil {
		return parquet.Column{}, false
	}
	p.AnnotateScanColumns(ctx, plan)
	schema := declaredOutputSchema(plan, p.subqueryOutputColumn)
	if len(schema) != 1 {
		// Not a scalar subquery's shape. Declining is the honest answer: a
		// wrong declaration here would pick a comparison RULE, which is worse
		// than picking none (ADR-0012 item 8).
		return parquet.Column{}, false
	}
	return schema[0], true
}

// buildSubqueryPipelineScoped is buildSubqueryPipeline with the enclosing WITH
// list given explicitly. A CTE's own BODY is built with only the CTEs defined
// BEFORE it, because a non-recursive CTE's name is not in scope inside its own
// body — PostgreSQL's rule (#771). Passing the whole list made a CTE that
// SHADOWS a base table materialize a body that read ITSELF, and the query
// answered NULL for every column the CTE computes.
func (p *Planner) buildSubqueryPipelineScoped(ctx context.Context, sql string,
	ctes []plansql.CTEDef) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	saved := p.ctes
	p.ctes = ctes
	defer func() { p.ctes = saved }()
	return p.buildSubqueryPipeline(ctx, sql)
}

// buildSubqueryPipelineScopedFor is buildSubqueryPipelineScoped over an
// already-parsed block — see buildSubqueryPipelineFor.
func (p *Planner) buildSubqueryPipelineScopedFor(ctx context.Context, info *plansql.SelectInfo,
	ctes []plansql.CTEDef) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	saved := p.ctes
	p.ctes = ctes
	defer func() { p.ctes = saved }()
	return p.buildSubqueryPipelineFor(ctx, info)
}

func (p *Planner) buildSubqueryPipeline(ctx context.Context, sql string) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	// Parse using our SQL parser
	pq, err := plansql.Parse(sql)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("subquery parse error: %w", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("subquery extract error: %w", err)
	}
	return p.buildSubqueryPipelineFor(ctx, info)
}

// buildSubqueryPipelineFor is buildSubqueryPipeline over an ALREADY-PARSED
// block. A CTE body is planned from the tree the binder validated, so a
// decision recorded there — PostgreSQL's GROUP BY precedence, which needs a
// schema the parser does not have — reaches this path too (#851).
func (p *Planner) buildSubqueryPipelineFor(ctx context.Context, info *plansql.SelectInfo) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	var err error

	// Build logical plan — merge outer CTEs so subqueries can reference
	// CTE tables defined in the enclosing WITH clause.
	var logicalPlan *logical.Node
	if len(p.ctes) > 0 {
		merged := append(p.ctes, info.CTEs...)
		logicalPlan, err = logical.BuildFromSelectWithCTEs(info, merged)
	} else {
		logicalPlan, err = logical.BuildFromSelect(info)
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("subquery plan error: %w", err)
	}

	// Annotate scan nodes with column metadata so the optimizer can resolve
	// unqualified column references (needed for subquery decorrelation).
	p.AnnotateScanColumns(ctx, logicalPlan)

	// A subquery is a WHOLE SECOND QUERY, planned here and never through
	// auth.EnforcePlanPolicies — so before #859 `(SELECT MAX(ssn) FROM t)`
	// read the raw column on every door while the same column masked in the
	// enclosing SELECT list. The policies travel on the context; the
	// projection goes in before the optimizer, exactly as it does for the
	// statement's own plan.
	//
	// And the ACCESS decision with them (#945): every relation THIS plan
	// reads asks the context lookup inside applyContextColumnPolicies, which
	// refuses before a pipeline is built. The lookup alone is enough to enter
	// — a relation named only inside a subquery is in no resolved set.
	if pol := logical.ColumnPoliciesFromContext(ctx); len(pol) > 0 || logical.PolicyLookupFromContext(ctx) != nil {
		// The name-binding pass runs only when something is DENIED. A
		// correlated subquery rebuilds this pipeline once per outer row, and
		// a mask-only policy — the common case — has nothing for the binder
		// to refuse.
		if denied := pol.DeniedColumns(); len(denied) > 0 {
			// nil table hook: applyContextColumnPolicies below asks the
			// ACCESS decision for every relation this plan reads (#945), so
			// the binder's own refusal would be a second copy of it.
			if err := ValidateColumnsUnderPolicy(ctx, p.catalog, info, func(table string) map[string]bool {
				return denied[strings.ToLower(table)]
			}, nil); err != nil {
				return nil, nil, nil, err
			}
		}
		logicalPlan, err = p.applyContextColumnPolicies(ctx, logicalPlan)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	// Optimize — pass scan annotator so new scans created by IN-to-SemiJoin
	// conversion get column metadata for scalar subquery decorrelation.
	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		p.AnnotateScanColumns(ctx, plan)
	})
	// The optimizer MINTS scans, after the policy went in above (#859).
	if pol := logical.ColumnPoliciesFromContext(ctx); len(pol) > 0 || logical.PolicyLookupFromContext(ctx) != nil {
		logicalPlan, err = p.applyContextColumnPoliciesToNewScans(ctx, logicalPlan)
		if err != nil {
			return nil, nil, nil, err
		}
		// The invariant over THIS plan too. A subquery's predicates are
		// pushed here, and one that ends up between a security projection and
		// its scan reads the stored column exactly as it would in the outer
		// plan (#859 round 4).
		if err := p.checkPolicyPlanOrderFromContext(ctx, logicalPlan); err != nil {
			return nil, nil, nil, err
		}
	}

	// Build physical pipeline
	source, ops, sink, err := p.buildPipeline(ctx, logicalPlan)
	if err != nil {
		// An authorization refusal raised while the pipeline is BUILT — the
		// table-function guard `buildScan` asks (#943) is the one that gets
		// here — is the decision's own sentence, not a planning narrative
		// about the subquery (ADR-0034 item 6; round-1 P1).
		if sqlerr.StateOf(err) == "42501" {
			return nil, nil, nil, err
		}
		return nil, nil, nil, fmt.Errorf("subquery execution plan error: %w", err)
	}

	// A SUBQUERY'S RESULT IS ITS SELECT LIST, and nothing else (#875).
	//
	// The logical builder materializes an ORDER BY term the SELECT list does
	// not carry as a HIDDEN column on the projection, and `Plan` drops it
	// again before the rows reach the client (#320, the call beside
	// buildPipeline there). This path had no such trim, so a subquery's rows
	// arrived carrying `__sortkey_N` beside the one column the query asked
	// for — and every consumer that reduces a subquery's row to ONE value
	// picks that value out of a Go MAP (expr.ScalarSubqueryValue,
	// InSubquery.resolveSlow's "first column only", CorrelatedInSubquery's).
	// Map iteration order is randomized per range statement, so `SELECT c_ts
	// FROM typemx ORDER BY id LIMIT 1` answered `id` about one run in five,
	// on ALL FOUR ARMS and in silence.
	//
	// The trim is the same operator the top-level statement gets, from the
	// same plan, so the two paths cannot disagree about which columns a
	// SELECT list has.
	if trim := hiddenSortTrimOp(logicalPlan); trim != nil {
		ops = append(ops, trim)
	}
	return source, ops, sink, nil
}

// executeSubquery parses and executes a SQL subquery, returning result rows.
func (p *Planner) executeSubquery(ctx context.Context, sql string) ([]map[string]any, error) {
	rows, _, err := p.executeSubquerySchema(ctx, sql)
	return rows, err
}

// executeSubquerySchema is executeSubquery with the result's DECLARED SCHEMA
// alongside the rows.
//
// A boxed row value cannot always say what it is — a DECIMAL and a STRING both
// arrive as a Go string — so a caller that has to re-spell those values (the
// IN-set materializer, which inlines them into filter TEXT) needs the
// declaration to tell them apart. That is ADR-0012 item 8's rule applied to
// the one place the boxing happens on the PLANNER's side of the wire.
func (p *Planner) executeSubquerySchema(ctx context.Context, sql string) ([]map[string]any, []parquet.Column, error) {
	source, ops, sink, err := p.buildSubqueryPipeline(ctx, sql)
	if err != nil {
		return nil, nil, err
	}

	// Ensure a CollectSink — buildPipeline may return nil sink for
	// non-blocking plans (e.g., CTE cache lookups, table-less SELECTs).
	collectSink, ok := sink.(*exec.CollectSink)
	if !ok {
		collectSink = &exec.CollectSink{}
		sink = collectSink
	}

	// Execute
	pipeline := &exec.Pipeline{Source: source, Ops: ops, Sink: sink}
	if err := pipeline.Run(ctx); err != nil {
		return nil, nil, fmt.Errorf("subquery execution error: %w", err)
	}

	// Schema BEFORE ToRows: the sink keeps its captured schema across that
	// call, but reading it first keeps the order a local fact.
	schema := collectSink.Schema()
	return subqueryRowsPerColumn(schema, collectSink), schema, nil
}

// subqueryRowsPerColumn is the sink's rows with ONE MAP ENTRY PER OUTPUT
// COLUMN, whatever the columns are called.
//
// PostgreSQL lets two output columns share a name — `SELECT ABS(a), ABS(b)`
// is two columns both called `abs`, and so is `SELECT x AS v, y AS v` — and
// `exec.CollectSink.convert` boxes a row into a map KEYED BY NAME, so the
// second column overwrites the first and the map holds ONE entry for TWO
// columns. Its own `ToRowValues` comment records that as lossy.
//
// Every consumer that reduces a subquery's row asks the map how many columns
// there are: `expr.ScalarSubqueryValue`, `InSubquery.resolveSlow`,
// `CorrelatedInSubquery.EvalBoolNull` and `materializeInSubquery` all count
// `len(row)`. With the map collapsed, a two-column subquery counted as ONE and
// walked straight through the 42601 refusal PostgreSQL raises for it —
// `SELECT (SELECT ABS(a), ABS(b) FROM decpair WHERE id = 1)` answered
// `12.7500` on every arm and every door, and its IN twin answered a row count.
//
// The count belongs to the SCHEMA, which is positional and cannot collapse,
// so the disambiguation happens HERE, once, at the seam every one of those
// consumers is fed from — rather than in four reducers that would each need a
// schema they are not given. `CollectSink.ToRowValues` already materializes
// the positional form for exactly this case and returns nil when the names are
// unique, which is the ordinary shape and costs nothing.
//
// The suffix is `:N`, the column's position: a colon cannot appear in an
// identifier the binder resolves, so a disambiguated key can collide with
// nothing.
//
// Every consumer of THESE rows iterates rather than reading a value by name,
// and the one that comes closest (`materializeInSubquery`) refuses anything
// but a single column first — but "no consumer reads by name" is NOT true of
// the runner's rows in general, and saying so would be the false claim a
// reviewer found: the RECURSIVE-CTE materialization keys its working row by
// name, so a duplicate-name column list already collapsed there before this
// pass existed. That path does not come through here, it is broken on both
// sides of this change, and it has its own filing; recorded so the next
// reader does not take the narrow statement for the wide one.
func subqueryRowsPerColumn(schema []parquet.Column, sink *exec.CollectSink) []map[string]any {
	rows := sink.ToRows()
	vals := sink.ToRowValues()
	if vals == nil || len(schema) == 0 {
		return rows
	}
	out := make([]map[string]any, 0, len(vals))
	for _, r := range vals {
		if len(r) != len(schema) {
			// The positional form and the declared schema disagree; the map
			// form is the answer this path always gave.
			return rows
		}
		m := make(map[string]any, len(r))
		for j, v := range r {
			name := schema[j].Name
			if _, dup := m[name]; dup {
				name = fmt.Sprintf("%s:%d", name, j)
			}
			m[name] = v
		}
		out = append(out, m)
	}
	return out
}

// ScalarDeferToggle gates deferring ALL uncorrelated scalar subqueries to
// distributed producer stages (not only CTE-referencing ones). Off
// (WADJET_SCALAR_DEFER=0) reverts them to eager plan-time execution on the
// coordinator's single-process pipeline.
//
// REGISTERED rather than read with a bare os.Getenv since #659: the switch
// decides whether a SELECT-list item becomes a producer stage or is left to
// the coordinator-local route, so it changes which ENGINE answers a query --
// and the rule is that a switch which can change the row set extends the
// invariance oracle. Reading it off the registry also lets a gate flip it
// without an env round trip.
var ScalarDeferToggle = optswitch.Register("scalar-defer", "WADJET_SCALAR_DEFER",
	"defer every uncorrelated scalar subquery to a distributed producer stage")

// ScalarSubqueriesAreDeferred reports whether the toggle above is on. Exported
// for the two-path gates, which must expect a lowered SELECT-list item to
// DECLINE when it is off: with no deferral there is no producer, and the
// lowering refuses to render a value it did not get from one.
func ScalarSubqueriesAreDeferred() bool { return ScalarDeferToggle.On() }

// deferredScalar carries a single CTE-referencing subquery whose resolution
// has been deferred until coordinator dispatch. The placeholder is a colon-
// prefixed identifier rendered into the serialized filter expression; the
// coordinator substitutes it after the producer stage completes.
type deferredScalar struct {
	Placeholder string // e.g. "scalar_1" (no leading colon)
	SubquerySQL string // the subquery to execute as a producer stage
}

// resolveFilterSubqueries finds embedded SQL subqueries in a filter expression
// string, executes them using the planner's standalone pipeline, and substitutes
// the scalar results as literals. This is needed for distributed mode where
// workers don't have catalog access to execute subqueries themselves.
//
// Subqueries whose FROM clause references a CTE are rewritten with
// :scalar_N placeholders instead of being pre-computed. The returned deferredScalar list describes the producer stages
// the caller must emit, and the filter-carrying stage's ScalarDependencies
// should point to those producer stage IDs. This eliminates the float-precision
// divergence between single-process cteCache evaluation and the distributed
// pipeline's accumulation order (root cause of Q15 SF0.1 0-row bug).
//
// Non-native-DAG mode keeps the legacy behavior: CTE-referencing subqueries
// are left unresolved (worker re-executes via SubqueryRunner), others are
// pre-computed and substituted in place.
func (p *Planner) resolveFilterSubqueries(exprStr string, decls colDecls) (string, []deferredScalar) {
	// Quick check: no subquery to resolve
	if !strings.Contains(strings.ToUpper(exprStr), "SELECT") {
		return exprStr, nil
	}

	ctx := p.planCtx
	if ctx == nil {
		return exprStr, nil
	}

	// Parse the expression to find SubqueryNode elements
	ast, err := plansql.ParseExpression(exprStr)
	if err != nil {
		return exprStr, nil
	}

	var deferred []deferredScalar
	resolved := p.resolveSubqueryAST(ctx, ast, &deferred, decls)
	if resolved != nil {
		return resolved.String(), deferred
	}
	return exprStr, deferred
}

// scalarSubqueryIsOneRow reports whether a subquery yields exactly one row for
// reasons the TEXT can prove, without executing it.
//
// One shape qualifies: a single select item that CONTAINS an aggregate, with
// no GROUP BY, no GROUPING SETS and no set operation. An ungrouped aggregate
// over any input — including an empty one — is exactly one row, which is why
// `SELECT MAX(x) FROM t` is a scalar subquery and `SELECT x FROM t` is a
// cardinality violation waiting to happen. The item may WRAP the aggregate
// (Q11's `SUM(…) * 0.0001`), which is why this asks for a nested aggregate
// rather than an aggregate call at the top.
//
// Anything it cannot prove is false, and a false answer costs a plan-time
// execution rather than a producer stage — the behaviour every scalar
// subquery had before the deferral existed.
func scalarSubqueryIsOneRow(sql string) bool {
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return false
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil || info == nil {
		return false
	}
	if info.Union != nil || len(info.GroupBy) > 0 || len(info.GroupingSets) > 0 {
		return false
	}
	if len(info.Columns) != 1 {
		return false
	}
	return plansql.FindNestedAggregate(info.Columns[0].ASTExpr) != nil
}

// allocScalarPlaceholder returns the next unused placeholder name (no leading
// colon) for this planner. Names are unique per Planner instance so that
// multiple deferred subqueries in the same query can coexist.
func (p *Planner) allocScalarPlaceholder() string {
	p.scalarPlaceholderSeq++
	return fmt.Sprintf("scalar_%d", p.scalarPlaceholderSeq)
}

// subqueryReferencesCTE returns true if the expression contains a scalar
// subquery whose FROM clause references a CTE defined in the current query.
func (p *Planner) subqueryReferencesCTE(exprStr string) bool {
	upper := strings.ToUpper(exprStr)
	for _, cte := range p.ctes {
		// Check if the CTE name appears after FROM in the subquery.
		// Use case-insensitive match since SQL is case-insensitive.
		if strings.Contains(upper, "FROM "+strings.ToUpper(cte.Name)) {
			return true
		}
	}
	return false
}

// resolveSubqueryAST recursively walks an AST node, replacing SubqueryNode
// elements with either literal values obtained by executing the subquery, or
// (when the subquery references a CTE under native-DAG) a LiteralPlaceholder
// whose concrete value will be substituted by the coordinator. Any deferred
// subqueries are appended to *deferred.
func (p *Planner) resolveSubqueryAST(ctx context.Context, node plansql.Node, deferred *[]deferredScalar, decls colDecls) plansql.Node {
	if node == nil {
		return nil
	}

	switch n := node.(type) {
	case *plansql.SubqueryNode:
		// A subquery that is not self-contained must NOT be deferred to a
		// producer stage or eagerly executed: standalone, its dangling outer
		// reference resolves to no column, evaluates NULL, and the query
		// silently answers 0 (#359). refuseCorrelatedSubqueries catches
		// correlation before stage generation with full scope; this is the
		// structural backstop for any expression that reaches deferral
		// without having passed through that pre-pass (e.g. inside a scalar
		// producer's own re-walk). Parked, not returned — walkStages has no
		// error path — and PlanDistributed turns it into the typed refusal
		// the coordinator routes on.
		if dangling := plansql.DanglingTableRefs(n.SQL); len(dangling) > 0 {
			p.refuseCorrelated(fmt.Errorf("%w: a scalar subquery references outer %s"+
				" and cannot execute as a standalone producer stage",
				ErrCorrelatedSubqueryDistributed, describeOuterRefs(dangling)))
			return node
		}
		// Defer scalar subqueries to producer stages so they share the
		// distributed accumulation path instead of running as a silent
		// single-process pipeline on the coordinator at plan time (Q11's
		// partsupp⨝supplier⨝nation subquery cost ~39s/query at SF100 this
		// way, Q22's customer avg ~10s). CTE-referencing subqueries MUST
		// defer regardless of the kill switch — eager evaluation over the
		// cteCache floats-drifts vs the outer query's distributed
		// aggregate (the Q15 SF0.1 0-row bug).
		// A subquery is DEFERRED to a producer stage only when it yields ONE
		// ROW BY CONSTRUCTION. Anything else has to be executed HERE, because
		// this is the only place in the distributed path that sees the
		// subquery's whole result and can therefore apply the one-row rule
		// (ADR-0021 §5): the coordinator's extractor reads the producer's
		// OUTPUT, and a producer's rows are neither one-per-row nor
		// one-file-per-task — a single-row producer can surface in more than
		// one file, so a count taken there is unsound in both directions.
		//
		// The perf lever the deferral exists for is untouched: Q11's and
		// Q22's subqueries are ungrouped aggregates, which is exactly the
		// shape that still defers.
		if scalarSubqueryIsOneRow(n.SQL) && (ScalarDeferToggle.On() || p.subqueryReferencesCTE(n.SQL)) {
			name := p.allocScalarPlaceholder()
			*deferred = append(*deferred, deferredScalar{Placeholder: name, SubquerySQL: n.SQL})
			return &plansql.LiteralPlaceholder{Name: name}
		}
		start := time.Now()
		rows, schema, err := p.executeSubquerySchema(ctx, n.SQL)
		slog.Info("plan-time scalar subquery executed on coordinator",
			"duration", time.Since(start).Round(time.Millisecond),
			"rows", len(rows), "error", err != nil)
		if err != nil {
			return node
		}
		// NO rows is not "no answer": a scalar subquery over an empty input
		// IS SQL NULL, and every comparison against it is UNKNOWN. Leaving
		// the subquery text in the filter instead — which is what this did —
		// ships a predicate the worker's compiler cannot read, and the task
		// fails. It was unreachable while every scalar subquery deferred to a
		// producer stage; restricting the deferral to the provably-one-row
		// shapes brought this path back.
		//
		// A SCALAR subquery is at most ONE row (ADR-0021 §5). Substituting
		// `rows[0]` of a multi-row result is a wrong answer wearing a
		// plausible one, and which row it picks is whichever the producer
		// emitted first — so the same query answers differently on different
		// paths. PostgreSQL raises 21000 here and so does this; the refusal is
		// PARKED because walkStages has no error return.
		v, cardErr := expr.ScalarSubqueryValue(n.SQL, rows)
		if cardErr != nil {
			p.refuseScalarRows(cardErr)
			return node
		}
		if v == nil {
			return &plansql.Lit{Kind: plansql.LitNull}
		}
		typ, typed := scalarColType(schema)
		return scalarToLiteral(v, typ, typed)

	case *plansql.ExistsNode:
		// `EXISTS (SELECT …)` that reached here did NOT decorrelate into a
		// semi/anti join, and the worker has no SubqueryRunner either — the
		// filter shipped verbatim and every task failed with "EXISTS subquery
		// requires a SubqueryRunner". That is #524's family with the EXISTS
		// arm never written: the sibling cases have handled a scalar subquery
		// (executed here) and an IN-subquery (materialized as a SET) since,
		// and `default:` shipped this one.
		//
		// An UNCORRELATED `EXISTS` is a query-wide CONSTANT — it reads no
		// outer row, so it is TRUE or FALSE for every row of every task — so
		// it is evaluated once here and the predicate becomes that boolean.
		// A subquery that is not self-contained is not evaluated: standalone,
		// its dangling reference resolves to no column and the constant would
		// be confidently wrong, so it takes the refusal the SubqueryNode arm
		// above takes and the coordinator answers on its local pipeline
		// (ADR-0021 §1c).
		if dangling := plansql.DanglingTableRefs(n.SQL); len(dangling) > 0 {
			p.refuseCorrelated(fmt.Errorf("%w: an EXISTS subquery references outer %s"+
				" and cannot be evaluated as a query-wide constant",
				ErrCorrelatedSubqueryDistributed, describeOuterRefs(dangling)))
			return node
		}
		rows, _, err := p.executeSubquerySchema(ctx, n.SQL)
		if err != nil {
			// An AUTHORIZATION refusal is the decision's own sentence, not a
			// planning narrative, and it is the query's answer on every path
			// (ADR-0034 item 6). Swallowing it shipped the filter and the
			// task failed with "EXISTS subquery requires a SubqueryRunner"
			// where the scalar and IN siblings say `permission denied for
			// table "…"` (round-1 review P1).
			if sqlerr.StateOf(err) == "42501" {
				p.refusePlanTimeAnswer(err)
			}
			return node
		}
		exists := len(rows) > 0
		if n.Not {
			exists = !exists
		}
		if exists {
			return &plansql.Lit{Value: "true", Kind: plansql.LitBool}
		}
		return &plansql.Lit{Value: "false", Kind: plansql.LitBool}

	case *plansql.InExpr:
		// `x IN (SELECT …)` that reached here did NOT decorrelate into a
		// semi/anti join, and the worker has no SubqueryRunner to execute it
		// with — the filter used to ship verbatim and fail (#524). An
		// uncorrelated IN-subquery is a SET, so it is materialized here and
		// the predicate becomes the literal list the expression layer already
		// evaluates. See in_subquery_set.go for the two bounds and the
		// refusal that routes past them.
		if subq := findInSubqueryValue(n); subq != nil {
			if rewritten, ok := p.materializeInSubquery(ctx, n, subq, decls); ok {
				return rewritten
			}
			return node
		}
		vals := make([]plansql.Node, len(n.Values))
		for i, v := range n.Values {
			vals[i] = p.resolveSubqueryAST(ctx, v, deferred, decls)
		}
		return &plansql.InExpr{
			Left:   p.resolveSubqueryAST(ctx, n.Left, deferred, decls),
			Not:    n.Not,
			Values: vals,
		}

	case *plansql.CmpExpr:
		return &plansql.CmpExpr{
			Left:  p.resolveSubqueryAST(ctx, n.Left, deferred, decls),
			Op:    n.Op,
			Right: p.resolveSubqueryAST(ctx, n.Right, deferred, decls),
		}

	case *plansql.BinaryOp:
		return &plansql.BinaryOp{
			Left:  p.resolveSubqueryAST(ctx, n.Left, deferred, decls),
			Op:    n.Op,
			Right: p.resolveSubqueryAST(ctx, n.Right, deferred, decls),
		}

	case *plansql.UnaryOp:
		return &plansql.UnaryOp{
			Op:    n.Op,
			Inner: p.resolveSubqueryAST(ctx, n.Inner, deferred, decls),
		}

	case *plansql.ParenNode:
		inner := p.resolveSubqueryAST(ctx, n.Inner, deferred, decls)
		if inner != nil {
			return &plansql.ParenNode{Inner: inner}
		}
		return node

	// The BOOLEAN TREE. A subquery is a leaf of a predicate, not a predicate,
	// and the arms above only reach the positions a comparison or an
	// arithmetic operator puts it in. `AND` looked handled because the filter
	// is split into conjuncts BEFORE this walk; `OR` and `NOT` cannot be
	// split, so `… WHERE d.id < 2 OR EXISTS (…)` shipped verbatim and every
	// task failed with "EXISTS subquery requires a SubqueryRunner"
	// (round-1 review B2). Walking the tree is what makes the rule ADR-0021
	// §2b states — an uncorrelated EXISTS is a query-wide constant — true of
	// the predicate rather than of one shape of predicate.
	//
	// Only the EXISTS leaves. See resolveBooleanExists: a boolean connective
	// SHORT-CIRCUITS, and hoisting a scalar out of an arm the query may never
	// evaluate makes that arm's failure the query's answer.
	case *plansql.AndNode:
		return &plansql.AndNode{
			Left:  p.resolveBooleanExists(ctx, n.Left, deferred, decls),
			Right: p.resolveBooleanExists(ctx, n.Right, deferred, decls),
		}

	case *plansql.OrNode:
		return &plansql.OrNode{
			Left:  p.resolveBooleanExists(ctx, n.Left, deferred, decls),
			Right: p.resolveBooleanExists(ctx, n.Right, deferred, decls),
		}

	case *plansql.NotNode:
		return &plansql.NotNode{Inner: p.resolveBooleanExists(ctx, n.Inner, deferred, decls)}

	case *plansql.CaseNode:
		out := &plansql.CaseNode{}
		if n.Subject != nil {
			out.Subject = p.resolveBooleanExists(ctx, n.Subject, deferred, decls)
		}
		for _, w := range n.Whens {
			out.Whens = append(out.Whens, plansql.WhenClause{
				Cond:   p.resolveBooleanExists(ctx, w.Cond, deferred, decls),
				Result: p.resolveBooleanExists(ctx, w.Result, deferred, decls),
			})
		}
		if n.Else != nil {
			out.Else = p.resolveBooleanExists(ctx, n.Else, deferred, decls)
		}
		return out

	default:
		return node
	}
}

// resolveBooleanExists resolves the EXISTS leaves under a boolean connective
// and leaves every other leaf exactly as it found it.
//
// A boolean connective SHORT-CIRCUITS, and hoisting is unconditional
// evaluation: it turns a subquery the query may never reach into one the query
// always runs, so any way that subquery can FAIL becomes the query's answer.
// PostgreSQL 17 measured, and the single-process path agrees with it because
// it evaluates per row and lazily:
//
//	… WHERE d.id < 100 OR d.id > (SELECT id FROM t WHERE id < 5)   -- 9 rows
//	… WHERE d.id < 0   OR d.id > (SELECT id FROM t WHERE id < 5)   -- 21000
//
// The subquery returns five rows either way. The first answers because the
// left arm is true for every row and the right one is never needed; the second
// raises because it IS needed. Hoisting made the first 21000 as well
// (round-1 review P2) — a query PostgreSQL answers, refused.
//
// An EXISTS is the leaf where hoisting is sound: it reads no outer row, it is
// TRUE or FALSE rather than a value, and it cannot raise the cardinality
// violation that is the failure at issue. A SCALAR subquery in a
// short-circuitable position keeps whatever the path did before — which on the
// DAG is a loud task failure, pinned per arm beside PostgreSQL's answer in
// coordinator.TestArcI1AnUnqualifiedNameBindsTheInnerRelation, because
// answering it needs the DAG to evaluate a subquery lazily and that is not a
// scope repair.
func (p *Planner) resolveBooleanExists(ctx context.Context, node plansql.Node,
	deferred *[]deferredScalar, decls colDecls) plansql.Node {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case *plansql.ExistsNode:
		return p.resolveSubqueryAST(ctx, n, deferred, decls)
	case *plansql.AndNode:
		return &plansql.AndNode{
			Left:  p.resolveBooleanExists(ctx, n.Left, deferred, decls),
			Right: p.resolveBooleanExists(ctx, n.Right, deferred, decls),
		}
	case *plansql.OrNode:
		return &plansql.OrNode{
			Left:  p.resolveBooleanExists(ctx, n.Left, deferred, decls),
			Right: p.resolveBooleanExists(ctx, n.Right, deferred, decls),
		}
	case *plansql.NotNode:
		return &plansql.NotNode{Inner: p.resolveBooleanExists(ctx, n.Inner, deferred, decls)}
	case *plansql.ParenNode:
		if inner := p.resolveBooleanExists(ctx, n.Inner, deferred, decls); inner != nil {
			return &plansql.ParenNode{Inner: inner}
		}
		return node
	case *plansql.CaseNode:
		out := &plansql.CaseNode{}
		if n.Subject != nil {
			out.Subject = p.resolveBooleanExists(ctx, n.Subject, deferred, decls)
		}
		for _, w := range n.Whens {
			out.Whens = append(out.Whens, plansql.WhenClause{
				Cond:   p.resolveBooleanExists(ctx, w.Cond, deferred, decls),
				Result: p.resolveBooleanExists(ctx, w.Result, deferred, decls),
			})
		}
		if n.Else != nil {
			out.Else = p.resolveBooleanExists(ctx, n.Else, deferred, decls)
		}
		return out
	default:
		return node
	}
}

// emitScalarProducerStages parses subquerySQL, walks its logical plan, and
// appends the resulting distributed stages to *stages. Returns the terminal
// stage's ID (the one whose single-row, single-column output holds the
// scalar). The coordinator awaits this producer at dispatch time, extracts
// the value, and substitutes it into the filter-carrying stage's expression.
//
// CTE definitions from the enclosing query are merged so the subquery can
// resolve :CTE references. The terminal stage is forced to Tasks=1 so its
// output is a single unpartitioned WSHF file suitable for scalar extraction.
func (p *Planner) emitScalarProducerStages(stages *[]Stage, subquerySQL string) (string, error) {
	id, _, _, err := p.emitScalarProducerStagesTyped(stages, subquerySQL)
	return id, err
}

// emitScalarProducerStagesTyped is emitScalarProducerStages with the TYPE the
// producer's own plan says its single output column emits, and whether that
// plan named one at all.
//
// The predicate path has no use for it — a substituted literal is compared
// against a column whose declaration decides the kernel. The SELECT-list path
// uses it to DECIDE, not to declare: a value whose literal spelling does not
// read back at the same type is not lowered at all (see
// scalarProducerValueIsLiteralSafe).
func (p *Planner) emitScalarProducerStagesTyped(stages *[]Stage, subquerySQL string) (string, parquet.TypeID, bool, error) {
	pq, err := plansql.Parse(subquerySQL)
	if err != nil {
		return "", 0, false, fmt.Errorf("parse subquery: %w", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return "", 0, false, fmt.Errorf("extract subquery: %w", err)
	}
	var logicalPlan *logical.Node
	if len(p.ctes) > 0 {
		merged := append([]plansql.CTEDef(nil), p.ctes...)
		merged = append(merged, info.CTEs...)
		logicalPlan, err = logical.BuildFromSelectWithCTEs(info, merged)
	} else {
		logicalPlan, err = logical.BuildFromSelect(info)
	}
	if err != nil {
		return "", 0, false, fmt.Errorf("build subquery plan: %w", err)
	}
	ctx := p.planCtx
	if ctx == nil {
		ctx = context.Background()
	}
	p.AnnotateScanColumns(ctx, logicalPlan)

	// The DAG's counterpart of buildSubqueryPipeline: this is a whole second
	// query, planned here, and it must carry the same column policy as its
	// enclosing statement (#859) and ask the same ACCESS decision for every
	// relation it reads (#945). The barrier is absorbed into the scan stage by
	// walkStages below, exactly as it is for the outer plan; the refusal
	// happens here, before a stage is emitted, so a denied relation never
	// becomes a task.
	if pol := logical.ColumnPoliciesFromContext(ctx); len(pol) > 0 || logical.PolicyLookupFromContext(ctx) != nil {
		if denied := pol.DeniedColumns(); len(denied) > 0 {
			// nil table hook: applyContextColumnPolicies below asks the
			// ACCESS decision for every relation this plan reads (#945), so
			// the binder's own refusal would be a second copy of it.
			if err := ValidateColumnsUnderPolicy(ctx, p.catalog, info, func(table string) map[string]bool {
				return denied[strings.ToLower(table)]
			}, nil); err != nil {
				return "", 0, false, err
			}
		}
		logicalPlan, err = p.applyContextColumnPolicies(ctx, logicalPlan)
		if err != nil {
			return "", 0, false, err
		}
	}

	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		p.AnnotateScanColumns(ctx, plan)
	})
	if pol := logical.ColumnPoliciesFromContext(ctx); len(pol) > 0 || logical.PolicyLookupFromContext(ctx) != nil {
		logicalPlan, err = p.applyContextColumnPoliciesToNewScans(ctx, logicalPlan)
		if err != nil {
			return "", 0, false, err
		}
		if err := p.checkPolicyPlanOrderFromContext(ctx, logicalPlan); err != nil {
			return "", 0, false, err
		}
	}

	before := len(*stages)
	p.walkStages(logicalPlan, stages, nil)
	if len(*stages) == before {
		return "", 0, false, fmt.Errorf("subquery emitted no stages")
	}
	terminal := &(*stages)[len(*stages)-1]
	// Force Singleton: a scalar producer emits exactly one row. Tasks>1
	// here would fan out into partitioned WSHF output that the coordinator's
	// scalar extractor can't read.
	terminal.Tasks = 1
	// Pin the subquery's projection on the terminal so the scalar extractor
	// can apply post-aggregate wrappers like Q11's "SUM(...) * 0.0001". The
	// producer chain only emits the raw aggregate (e.g. __agg_0 = SUM); the
	// SELECT-level multiplier needs to be applied by the coordinator after
	// reading the producer output. Reuses Stage.OutputRenames the same way
	// Gather does — at extract time we'll detect the producer-vs-Gather
	// case via context.
	if renames := extractOutputRenames(logicalPlan); len(renames) > 0 {
		terminal.OutputRenames = renames
	}
	// The producer's own plan is the authority on the value's TYPE. One
	// output column is the whole shape a scalar producer has (the deferral
	// fires only for a single provably-one-row select item); a plan that
	// emits anything else names nothing, and the caller declines.
	var valueType parquet.TypeID
	typeKnown := false
	if emitted := emittedColTypes(logicalPlan); len(emitted) == 1 {
		for _, id := range emitted {
			valueType, typeKnown = id, true
		}
	}
	return terminal.ID, valueType, typeKnown, nil
}

// scalarToLiteral converts a Go value to an AST literal node.
//
// typ is the value's DECLARED type, for the one box that cannot say what it
// is: a DECIMAL arrives as its RENDERED TEXT, indistinguishable from a STRING
// column's value. Spelling it as a QUOTED literal makes it look like something
// a user wrote, and an unknown-typed literal is coerced with the OTHER
// operand's input function (ADR-0012 item 13) — so `HAVING COUNT(*) > (SELECT
// COUNT(*) * 0.3 …)` substituted `'0.0'` and asked bigint's input function to
// read it, which is 22P02 for a query PostgreSQL answers as numeric. A DECIMAL
// is spelled as the NUMBER it is, carrying its exact digits (item 6's carrier
// rule); every other string-boxed type keeps the quoted spelling.
func scalarToLiteral(v any, typ parquet.TypeID, typed bool) plansql.Node {
	if s, ok := v.(string); ok && typed && typ == parquet.TypeDecimal {
		return &plansql.Lit{Value: s, Kind: plansql.LitNumber}
	}
	switch val := v.(type) {
	case float64:
		return &plansql.Lit{Value: strconv.FormatFloat(val, 'f', -1, 64), Kind: plansql.LitNumber}
	case float32:
		return &plansql.Lit{Value: strconv.FormatFloat(float64(val), 'f', -1, 32), Kind: plansql.LitNumber}
	case int64:
		return &plansql.Lit{Value: fmt.Sprintf("%d", val), Kind: plansql.LitNumber}
	case int:
		return &plansql.Lit{Value: fmt.Sprintf("%d", val), Kind: plansql.LitNumber}
	case string:
		return &plansql.Lit{Value: val, Kind: plansql.LitString}
	default:
		return &plansql.Lit{Value: fmt.Sprint(v), Kind: plansql.LitNumber}
	}
}

// scalarColType is a single-column result schema's declared type, and
// typed=false when the schema does not name exactly one column (nothing to
// disambiguate a box with).
func scalarColType(schema []parquet.Column) (parquet.TypeID, bool) {
	if len(schema) != 1 {
		return 0, false
	}
	return schema[0].Type, true
}

// AnnotateScanColumns walks the logical plan tree and populates ScanColumns
// on Scan nodes from the catalog. This enables the logical optimizer to resolve
// unqualified column references for filter pushdown through joins.
func (p *Planner) AnnotateScanColumns(ctx context.Context, node *logical.Node) {
	p.annotateScanColumns(ctx, node)
	// With the catalog's real column lists now on the Scan nodes, move any of
	// the planner's own hidden slots that a STORED column already occupies.
	// It has to run here: before annotation there is no schema to collide
	// with, and a stored `__win_0` beside a window is a #694 collision with
	// the planner on the other side of it (slot_collision.go).
	renameCollidingSlots(node)
}

func (p *Planner) annotateScanColumns(ctx context.Context, node *logical.Node) {
	if node == nil {
		return
	}
	if node.Type == logical.NodeScan && node.TableName != "" && !node.IsTableFunc {
		// CANONICALIZE first, once, in place: everything below this pass keys
		// off Node.TableName — the manifest lookup, the pruner, the worker's
		// scan — so conceding at each door would be a different name at each
		// door. Resolving here means a reference that named the table in
		// another case becomes the catalog's own spelling for the whole plan.
		if p.catalog != nil {
			node.TableName = p.catalog.ResolveTableName(node.TableName)
		}
		table, err := p.catalog.GetTable(ctx, node.TableName)
		if err == nil {
			cols := make([]string, len(table.Schema.Columns))
			intCols := make(map[string]bool, len(table.Schema.Columns))
			colTypes := make(map[string]parquet.TypeID, len(table.Schema.Columns))
			colDecimal := make(map[string]logical.DecimalMeta)
			var colFields map[string][]parquet.Column
			for i, c := range table.Schema.Columns {
				cols[i] = c.Name
				colTypes[strings.ToLower(c.Name)] = c.Type
				if c.Type == parquet.TypeDecimal {
					colDecimal[strings.ToLower(c.Name)] = logical.DecimalMeta{Precision: c.Precision, Scale: c.Scale}
				}
				if c.Type == parquet.TypeRow && len(c.Fields) > 0 {
					// A ROW's fields are the only declaration a field path
					// has; they live nowhere in a map keyed by column name
					// (#568). Kept as their full parquet.Column so a
					// DECIMAL field keeps its (p,s) and a nested container
					// field keeps its own shape.
					if colFields == nil {
						colFields = make(map[string][]parquet.Column)
					}
					colFields[strings.ToLower(c.Name)] = c.Fields
				}
				switch c.Type {
				case parquet.TypeInt64, parquet.TypeInt32, parquet.TypeTimestamp,
					parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration,
					parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
					intCols[strings.ToLower(c.Name)] = true
				}
			}
			strictInt := make(map[string]bool, len(table.Schema.Columns))
			for _, c := range table.Schema.Columns {
				switch c.Type {
				case parquet.TypeInt64, parquet.TypeInt32:
					strictInt[strings.ToLower(c.Name)] = true
				}
			}
			node.ScanColumns = cols
			node.ScanIntCols = intCols
			node.ScanStrictIntCols = strictInt
			node.ScanColTypes = colTypes
			node.ScanColDecimal = colDecimal
			node.ScanColFields = colFields
		}
		// Estimate row count from manifest for join reordering
		if manifest, err := p.getManifest(ctx, node.TableName); err == nil {
			var total int64
			for _, part := range manifest.Partitions {
				for _, f := range part.Files {
					total += f.NumRows
				}
			}
			node.ScanRowEstimate = total

			// Aggregate per-column stats for CBO selectivity estimation
			if colStats, err := p.getAggregateColumnStats(ctx, node.TableName); err == nil && colStats != nil {
				scanStats := make(map[string]logical.ScanColumnStats, len(colStats))
				for col, cs := range colStats {
					var hist any
					if cs.Histogram != nil {
						hist = cs.Histogram
					}
					scanStats[col] = logical.ScanColumnStats{
						MinValue:  cs.MinValue,
						MaxValue:  cs.MaxValue,
						NullCount: cs.NullCount,
						TotalRows: cs.TotalRows,
						NDV:       cs.NDV,
						Histogram: hist,
					}
				}
				node.ScanColStats = scanStats
			}
		}
	}
	for _, child := range node.Children {
		p.annotateScanColumns(ctx, child)
	}
}

// materializeCTEs pre-computes any CTE that is referenced more than once in
// the plan tree (including inside scalar subqueries). Both the main pipeline
// and any subquery pipelines will read from the cached result, ensuring they
// see bit-identical data.
func (p *Planner) materializeCTEs(ctx context.Context, root *logical.Node) {
	if len(root.CTEs) == 0 {
		return
	}
	// Count how many times each CTE name appears as a CTEName tag.
	refCounts := map[string]int{}
	var countRefs func(n *logical.Node)
	countRefs = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.CTEName != "" {
			refCounts[n.CTEName]++
		}
		for _, c := range n.Children {
			countRefs(c)
		}
	}
	countRefs(root)

	// Also count CTE references inside scalar subquery expressions.
	// These are in predicate ASTExpr nodes that reference CTE table names.
	// A simple heuristic: if a CTE name appears in the CTE list AND has
	// at least 1 reference in the plan tree, check if any scalar subquery
	// in the plan text also references it. We conservatively materialize
	// any CTE that has >=1 ref in the plan tree AND appears in the CTE
	// list (since scalar subqueries may reference it too).
	for _, cte := range root.CTEs {
		if refCounts[cte.Name] > 0 {
			// Conservatively mark as multi-ref since scalar subqueries
			// (not visible in the logical tree) may also reference it.
			refCounts[cte.Name] = 2
		}
	}

	p.cteCache = make(map[string]*cteMaterialized)

	for i := range root.CTEs {
		cte := &root.CTEs[i]
		if cte.Recursive {
			p.materializeRecursiveCTE(ctx, *cte)
			continue
		}
		if refCounts[cte.Name] < 2 {
			continue
		}
		// EARLIER CTEs only — see buildSubqueryPipelineScoped (#771).
		//
		// Materialize columnar into a tracker-charged, spill-backed
		// collector. The previous shape boxed the whole result via
		// CollectSink.ToRows (one map[string]any per row, entirely outside
		// the budget/spill machinery) — `WITH x AS (SELECT * FROM lineitem)`
		// held the full table in coordinator-process heap.
		// The MEMOIZED body, so this materialization plans the tree the
		// binder validated rather than a private re-parse of the same text
		// (#851). BodySelect answers nil on a parse error, and the sql
		// argument keeps the old path's message for that case.
		body, _ := cte.BodySelect()
		coll, schema, err := p.materializeCTEColumnar(ctx, cte.SQL, body, root.CTEs[:i])
		if err != nil {
			continue // fall back to inline expansion
		}
		if schema == nil {
			coll.Release()
			continue
		}
		p.cteCache[cte.Name] = &cteMaterialized{schema: schema, coll: coll}
	}
}

// materializeCTEColumnar runs a CTE body into a spill-backed collector.
// Returns the collector and the schema the body's own pipeline produced (see
// cteMaterializingSink); the caller owns the collector and must Release it —
// normally via PhysicalPlan.Cleanup through releaseCTECache.
func (p *Planner) materializeCTEColumnar(ctx context.Context, sql string,
	body *plansql.SelectInfo, scope []plansql.CTEDef) (*exec.SpillableBatchCollector, []parquet.Column, error) {
	var source exec.Source
	var ops []exec.UnaryOperator
	var err error
	if body != nil {
		source, ops, _, err = p.buildSubqueryPipelineScopedFor(ctx, body, scope)
	} else {
		source, ops, _, err = p.buildSubqueryPipelineScoped(ctx, sql, scope)
	}
	if err != nil {
		return nil, nil, err
	}
	coll := &exec.SpillableBatchCollector{Spill: p.getSpillManager()}
	sink := &cteMaterializingSink{coll: coll}
	pipeline := &exec.Pipeline{Source: source, Ops: ops, Sink: sink}
	if err := pipeline.Run(ctx); err != nil {
		coll.Release()
		return nil, nil, err
	}
	schema := sink.schema
	if schema == nil {
		// Empty result: no batch ever arrived, so derive column names from
		// the SQL like the boxed path did — downstream projection still
		// needs the names to resolve.
		schema = p.inferCTESchema(sql, nil)
	}
	return coll, schema, nil
}

// closeBuiltJoins closes every HashJoin this plan built: it returns the
// build side's tracker reservation and removes the grace build's partition
// files, which the flush loop only removes when the query runs to
// completion. Idempotent — HashJoin.Close zeroes what it releases.
func (p *Planner) closeBuiltJoins() {
	for _, hj := range p.builtJoins {
		hj.Close()
	}
	p.builtJoins = nil
}

// releaseScanCache drops every duplicate-scan cache entry so cached
// batches don't outlive their query. Idempotent; wired into
// PhysicalPlan.Cleanup, Plan's error paths, and Plan's per-query reset.
func (p *Planner) releaseScanCache() {
	for _, c := range p.scanCache {
		c.mu.Lock()
		c.batches = nil
		c.mu.Unlock()
	}
	p.scanCache = nil
}

// releaseCTECache frees every columnar CTE collector (tracker charge +
// spill scratch). Idempotent; wired into PhysicalPlan.Cleanup and Plan's
// error paths.
func (p *Planner) releaseCTECache() {
	for _, mat := range p.cteCache {
		if mat.coll != nil {
			mat.coll.Release()
		}
	}
	p.cteCache = nil
}

// cteCacheHasCollectors reports whether any cached CTE holds spill-backed
// state that requires an explicit release at query end.
func (p *Planner) cteCacheHasCollectors() bool {
	for _, mat := range p.cteCache {
		if mat.coll != nil {
			return true
		}
	}
	return false
}

// cteMaterializingSink wraps the CTE collector and records the schema the CTE
// body's own pipeline produced, which IS the CTE's schema.
//
// It used to SNIFF. The boxed CTE path rebuilt a schema from `map[string]any`
// rows, where a numeric-looking string is indistinguishable from a number, so
// it read the FIRST ROW of every STRING column and rewrote the WHOLE column to
// INT64/FLOAT64 when that one value parsed; the columnar path inherited the
// rule. That is a VALUE re-read as a TYPE — ADR-0026 §2c's confusion pointed
// at data instead of at a name — and it made a column's type depend on the
// rows the body happened to emit first.
//
// Over `decpair.s`, a genuine TEXT column holding "1.50", "1.5", "abc",
// "1.500", the first row parsed. The column came back double precision:
// `WHERE s = '1.50'` compared NUMBERS and counted 4 where PostgreSQL counts 1,
// "abc" silently became NULL, and `SUM(CASE WHEN s='abc' THEN v ELSE 0 END)`
// failed with `invalid input syntax for type double precision: "abc"` on a
// query PostgreSQL answers with 25.50 (#727). The derived-table spelling of
// the same query was right throughout, so two spellings of one question
// answered two numbers. The old comment's claim — "real string data is never
// touched: CEO doesn't parse" — is the impossibility no fixture attempted
// (correctness-fix protocol rule 10), and adding `WHERE id = 3` to the body
// was enough to flip the type back.
//
// Nothing needs the coercion now: a bare column carries the catalog's
// declaration through the pipeline, and `SELECT 1` already declares INT64 at
// the projection (#369), which is where a literal's type belongs.
type cteMaterializingSink struct {
	coll   *exec.SpillableBatchCollector
	schema []parquet.Column // the first non-empty batch's schema; nil until then
}

func (s *cteMaterializingSink) Init(ctx context.Context) error { return s.coll.Init(ctx) }

func (s *cteMaterializingSink) Consume(ctx context.Context, b *batch.RecordBatch) error {
	if s.schema == nil && b.ActiveLen() > 0 {
		s.schema = append([]parquet.Column(nil), b.Schema...)
	}
	return s.coll.Consume(ctx, b)
}

func (s *cteMaterializingSink) Finalize(ctx context.Context) error { return s.coll.Finalize(ctx) }
func (s *cteMaterializingSink) Close() error                       { return s.coll.Close() }

// inferCTESchema derives column types from a CTE's SQL and data rows.
func (p *Planner) inferCTESchema(sql string, rows []map[string]any) []parquet.Column {
	pq, err := plansql.Parse(sql)
	if err != nil {
		return nil
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return nil
	}
	schema := make([]parquet.Column, len(info.Columns))
	names := make([]string, len(info.Columns))
	for i, col := range info.Columns {
		names[i] = col.Alias
		if names[i] == "" {
			names[i] = col.Expr
		}
	}
	// The key each POSITION appears under in the rows executeSubquery
	// returns. Two output columns may share a name — `SELECT 1 AS x, 10 AS x`
	// is legal SQL — and a row is a Go map, so the second would overwrite the
	// first; subqueryRowsPerColumn suffixes every later duplicate with its
	// position for exactly that reason. Reading by the bare NAME here made
	// both positions answer with the FIRST column's value, which is #957.
	keys := subqueryRowKeys(names)
	for i := range info.Columns {
		name := names[i]
		key := keys[i]
		typ := parquet.TypeString
		if len(rows) > 0 {
			if v, ok := rows[0][key]; ok {
				switch v.(type) {
				case int64:
					typ = parquet.TypeInt64
				case int32:
					typ = parquet.TypeInt32
				case float64:
					typ = parquet.TypeFloat64
				case bool:
					typ = parquet.TypeBool
				case string:
					// Check if the string value is actually a numeric literal
					// (SELECT 1 returns "1" as a string from the expression evaluator)
					s := v.(string)
					if _, err := strconv.ParseInt(s, 10, 64); err == nil {
						typ = parquet.TypeInt64
						// Convert all rows' values from string to int64
						for _, row := range rows {
							if sv, ok := row[key].(string); ok {
								if iv, err := strconv.ParseInt(sv, 10, 64); err == nil {
									row[key] = iv
								}
							}
						}
					} else if _, err := strconv.ParseFloat(s, 64); err == nil {
						typ = parquet.TypeFloat64
						for _, row := range rows {
							if sv, ok := row[key].(string); ok {
								if fv, err := strconv.ParseFloat(sv, 64); err == nil {
									row[key] = fv
								}
							}
						}
					}
				}
			}
		}
		schema[i] = parquet.Column{Name: name, Type: typ, Nullable: true}
	}
	return schema
}

const maxRecursiveIterations = 1000

// materializeRecursiveCTE executes a recursive CTE using fixed-point iteration.
// The CTE body must contain UNION ALL separating the anchor query from the
// recursive query. The recursive query references the CTE name itself.
func (p *Planner) materializeRecursiveCTE(ctx context.Context, cte plansql.CTEDef) {
	anchorSQL, recursiveSQL, ok := splitRecursiveUnion(cte.SQL)
	if !ok {
		// No UNION ALL found — fall back to non-recursive (columnar)
		// materialization.
		// A RECURSIVE CTE's name IS in scope inside its own body, which is
		// what makes it recursive, so this one keeps the whole list.
		coll, schema, err := p.materializeCTEColumnar(ctx, cte.SQL, nil, p.ctes)
		if err != nil {
			return
		}
		if schema == nil {
			coll.Release()
			return
		}
		p.cteCache[cte.Name] = &cteMaterialized{schema: schema, coll: coll}
		return
	}

	// Step 1: Execute anchor query
	anchorRows, err := p.executeSubquery(ctx, anchorSQL)
	if err != nil {
		return
	}
	if len(anchorRows) == 0 {
		schema := p.inferCTESchema(anchorSQL, nil)
		if schema != nil {
			p.cteCache[cte.Name] = &cteMaterialized{schema: schema, rows: nil}
		}
		return
	}

	// Infer schema from anchor results
	schema := p.inferCTESchema(anchorSQL, anchorRows)
	if schema == nil {
		return
	}

	// Apply column aliases if specified: WITH t(a, b) AS (...)
	//
	// POSITIONALLY, through the key each column really appears under. A
	// column list is what makes a duplicate-name body legal and useful —
	// `WITH RECURSIVE t(a, b) AS (SELECT 1 AS x, 10 AS x …)` — and reading
	// both positions by the bare name `x` gave both aliases the FIRST
	// column's value, so the working row collapsed and every later iteration
	// read it: `1,10 | 2,100 | 3,10000` in PostgreSQL 17 came back
	// `1,1 | 2,1 | 3,1` (#957).
	if len(cte.Columns) > 0 && len(cte.Columns) <= len(schema) {
		anchorRows = renameRowColumnsPositional(anchorRows, schema, cte.Columns)
		for i, name := range cte.Columns {
			schema[i].Name = name
		}
	}

	// Accumulate iteration results columnar into a tracker-charged,
	// spill-backed collector instead of an unbounded boxed slice — the
	// iteration count was bounded (1000) but the row count was not, and
	// every accumulated row lived as a map[string]any until Plan returned.
	// The per-iteration work table stays boxed: it is one iteration's
	// delta (inherent to the fixed-point algorithm) and is re-seeded into
	// the cache each step for the recursive query's self-reference.
	coll := &exec.SpillableBatchCollector{Spill: p.getSpillManager()}
	appendRowsColumnar := func(rs []map[string]any) error {
		for off := 0; off < len(rs); off += batch.DefaultBatchSize {
			end := off + batch.DefaultBatchSize
			if end > len(rs) {
				end = len(rs)
			}
			if err := coll.Consume(ctx, batch.FromRows(schema, rs[off:end])); err != nil {
				return err
			}
		}
		return nil
	}
	if err := appendRowsColumnar(anchorRows); err != nil {
		coll.Release()
		return
	}

	// Derive the expected column names from the schema (aliases already applied).
	schemaNames := make([]string, len(schema))
	for i, col := range schema {
		schemaNames[i] = col.Name
	}

	// Parse the recursive SQL to get its output column names so we can
	// positionally rename them to match the CTE schema. Strip table alias
	// prefixes (e.g., "e.id" → "id") because the Project operator outputs
	// unqualified column names.
	var recursiveColNames []string
	if rpq, err := plansql.Parse(recursiveSQL); err == nil {
		if ri, err := plansql.ExtractSelect(rpq); err == nil {
			for _, col := range ri.Columns {
				name := col.Alias
				if name == "" {
					name = cleanExpr(col.Expr)
				}
				recursiveColNames = append(recursiveColNames, name)
			}
		}
	}
	// …and the keys those names really occupy, for the same reason the anchor
	// needs them: two recursive-term outputs may share a name.
	recursiveRowKeys := subqueryRowKeys(recursiveColNames)

	// Step 2: Fixed-point iteration
	workTable := anchorRows
	for iter := 0; iter < maxRecursiveIterations; iter++ {
		// Seed the CTE cache with the current work table so the recursive
		// query's reference to the CTE name resolves to these rows.
		p.cteCache[cte.Name] = &cteMaterialized{schema: schema, rows: workTable}

		newRows, err := p.executeSubquery(ctx, recursiveSQL)
		if err != nil {
			break
		}
		if len(newRows) == 0 {
			break
		}

		// Rename output columns to match CTE schema. The recursive SQL may
		// produce different column names (e.g., "n + 1" vs "n").
		newRows = renameRowColumnsFromTo(newRows, recursiveRowKeys, schemaNames)

		if err := appendRowsColumnar(newRows); err != nil {
			// Spill scratch failure mid-iteration: abandon materialization.
			// Without a cache entry the recursive reference cannot resolve
			// and the query errors — same failure mode as an anchor error.
			coll.Release()
			delete(p.cteCache, cte.Name)
			return
		}
		workTable = newRows
	}

	// Store final accumulated results (columnar; replayed per reference).
	p.cteCache[cte.Name] = &cteMaterialized{schema: schema, coll: coll}
}

// stageTypeCTEAlias marks a phantom stage that walkStages emits in place of
// a re-computed CTE subtree when ctePlannedTerminal already has a cached
// terminal stage ID. flattenCTEAliases removes these stages from the final
// plan and rewrites every dependency edge that targets an alias to target
// the alias's underlying CTE terminal instead. The alias never reaches
// dispatch — it exists purely to give parent walkStages cases something to
// pick up via leafStages without changing every parent's child-resolution
// logic.
const stageTypeCTEAlias = "cte-alias"

// flattenCTEAliases collapses cte-alias stages: replaces every Dependencies
// reference to an alias with its target, recursing through chains of aliases,
// then drops alias stages from the slice. Idempotent on slices that contain
// no aliases.
func flattenCTEAliases(stages []Stage) []Stage {
	// Build alias → target map. Aliases have exactly one Dependencies entry
	// pointing at the cached CTE terminal (or another alias, in pathological
	// chain cases — recurse to flatten).
	aliasTarget := map[string]string{}
	for _, s := range stages {
		if s.Type == stageTypeCTEAlias && len(s.Dependencies) == 1 {
			aliasTarget[s.ID] = s.Dependencies[0]
		}
	}
	if len(aliasTarget) == 0 {
		return stages
	}
	// Resolve transitively: follow alias→alias chains until we hit a real
	// stage. Caps at len(aliasTarget) hops to defend against any cycle.
	resolve := func(id string) string {
		for i := 0; i <= len(aliasTarget); i++ {
			next, ok := aliasTarget[id]
			if !ok {
				return id
			}
			id = next
		}
		return id
	}
	// Rewrite every Dependencies / LeftDepStage / RightDepStage / FusedJoin
	// build dep that points at an alias.
	for i := range stages {
		s := &stages[i]
		for j, dep := range s.Dependencies {
			s.Dependencies[j] = resolve(dep)
		}
		if t, ok := aliasTarget[s.LeftDepStage]; ok {
			s.LeftDepStage = resolve(t)
		}
		if t, ok := aliasTarget[s.RightDepStage]; ok {
			s.RightDepStage = resolve(t)
		}
		for j, fj := range s.FusedJoins {
			if t, ok := aliasTarget[fj.BuildDepStage]; ok {
				s.FusedJoins[j].BuildDepStage = resolve(t)
			}
		}
		for ph, prod := range s.ScalarDependencies {
			if t, ok := aliasTarget[prod]; ok {
				s.ScalarDependencies[ph] = resolve(t)
			}
		}
		// A set operation's per-arm producer needs no rewrite of its own: it
		// IS Dependencies[i] (see UnionArm). It used to be a stored copy this
		// loop rewrote separately, and one that rewrote the copy without the
		// list refused a UNION ALL over a twice-referenced CTE outright —
		// `arm 1 names producer "cte-alias-1" but Dependencies[1] is
		// "scan-0"` (#660, #715).
	}
	// Drop alias stages.
	out := make([]Stage, 0, len(stages))
	for _, s := range stages {
		if s.Type == stageTypeCTEAlias {
			continue
		}
		out = append(out, s)
	}
	return out
}

// cteSubtreeHash returns a hex SHA-256 over a structural projection of the
// logical subtree rooted at n. Two CTE clones with the same hash are safe
// to dedupe in walkStages: identical scan tables, identical pushed-down
// predicates, identical projections/aggregates, identical column-pruning
// outputs, identical child shapes. A clone where the optimizer pushed
// different filters or columns has a different hash and is NOT deduped.
//
// The hash is intentionally over the *post-optimization* logical shape —
// we want bit-identical execution paths, not source-text equality.
func cteSubtreeHash(n *logical.Node) string {
	h := sha256.New()
	hashLogicalNode(h, n)
	return hex.EncodeToString(h.Sum(nil))
}

func hashLogicalNode(h io.Writer, n *logical.Node) {
	if n == nil {
		_, _ = io.WriteString(h, "<nil>|")
		return
	}
	// Order matters and so does separator — Sprint with a delimiter so a
	// field containing the same characters as another can't collide. The
	// fields chosen are the ones the physical planner actually reads from
	// when emitting stages; if a future planner change reads a new field
	// during walkStages, add it here.
	//
	// IMPORTANT: RequiredColumns is INTENTIONALLY excluded. The optimizer's
	// column-pruning analysis pushes columns referenced anywhere in the
	// outer query into the CTE's inner scan rc list — including columns
	// that don't even belong to the CTE's tables (e.g., supplier_no
	// projected by the Project ABOVE the CTE body, or s_suppkey from the
	// JOIN's other side). Two clones of the same CTE will therefore
	// disagree on RequiredColumns even though they compute byte-identical
	// data; downstream scan code already over-approximates and prunes to
	// real schema columns at execution time. Hashing RC would defeat the
	// dedup whenever a CTE is consumed by two consumers with different
	// outer column needs (i.e., always).
	fmt.Fprintf(h, "T:%v|TBL:%s|PF:%v|SP:%v|", n.Type, n.TableName, n.PartitionFilter, n.ScanPredicates)
	fmt.Fprintf(h, "Pred:%v|Proj:%v|", n.Predicates, n.Projections)
	fmt.Fprintf(h, "GB:%v|GBE:%v|Agg:%v|", n.GroupBy, n.GroupByExprs, n.AggExprs)
	fmt.Fprintf(h, "OB:%v|Lim:%d|Off:%d|", n.OrderBy, n.LimitVal, n.OffsetVal)
	fmt.Fprintf(h, "JT:%s|JC:%s|JF:%s|LK:%v|RK:%v|", n.JoinType, n.JoinCond, n.JoinFilter, n.LeftKeys, n.RightKeys)
	fmt.Fprintf(h, "Win:%v|UA:%v|", n.WindowExprs, n.UnionAll)
	// Don't fold n.CTEName into the hash — two clones of the same CTE
	// SHARE that name, that's the whole point. The cache key in walkStages
	// already uses CTEName as a separate dimension.
	_, _ = io.WriteString(h, "C:[")
	for i, c := range n.Children {
		if i > 0 {
			_, _ = io.WriteString(h, ",")
		}
		hashLogicalNode(h, c)
	}
	_, _ = io.WriteString(h, "]|")
}

// splitRecursiveUnion splits a recursive CTE body at the top-level UNION ALL.
// Returns (anchor, recursive, true) or ("", "", false) if no UNION ALL found.
func splitRecursiveUnion(sql string) (anchor, recursive string, ok bool) {
	upper := strings.ToUpper(sql)
	depth := 0
	inStr := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if inStr {
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++ // escaped quote
				} else {
					inStr = false
				}
			}
			continue
		}
		if ch == '\'' {
			inStr = true
			continue
		}
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
		}
		// Only match UNION ALL at depth 0 (not inside subqueries)
		if depth == 0 && i+9 < len(upper) {
			if upper[i:i+5] == "UNION" {
				rest := strings.TrimSpace(upper[i+5:])
				if strings.HasPrefix(rest, "ALL") {
					// Find the exact position after "UNION ALL"
					unionEnd := i + 5
					for unionEnd < len(sql) && (sql[unionEnd] == ' ' || sql[unionEnd] == '\t' || sql[unionEnd] == '\n' || sql[unionEnd] == '\r') {
						unionEnd++
					}
					unionEnd += 3 // skip "ALL"
					anchor = strings.TrimSpace(sql[:i])
					recursive = strings.TrimSpace(sql[unionEnd:])
					return anchor, recursive, true
				}
			}
		}
	}
	return "", "", false
}

// renameRowColumnsFromTo remaps row keys from srcNames[i] to dstNames[i].
func renameRowColumnsFromTo(rows []map[string]any, srcNames, dstNames []string) []map[string]any {
	if len(rows) == 0 || len(srcNames) == 0 || len(dstNames) == 0 {
		return rows
	}
	needsRename := false
	for i := range srcNames {
		if i < len(dstNames) && srcNames[i] != dstNames[i] {
			needsRename = true
			break
		}
	}
	if !needsRename {
		return rows
	}
	result := make([]map[string]any, len(rows))
	for ri, row := range rows {
		newRow := make(map[string]any, len(row))
		for k, v := range row {
			newRow[k] = v
		}
		for i, src := range srcNames {
			if i < len(dstNames) && src != dstNames[i] {
				newRow[dstNames[i]] = row[src]
				delete(newRow, src)
			}
		}
		result[ri] = newRow
	}
	return result
}

// subqueryRowKeys is the map key each POSITION of a result occupies in the
// rows executeSubquery returns.
//
// It states subqueryRowsPerColumn's rule once so the readers cannot drift from
// the writer: the first occurrence of a name keeps the name, and every later
// column of that name carries `:<position>`. A colon cannot appear in an
// identifier the binder resolves, so a disambiguated key collides with
// nothing.
func subqueryRowKeys(names []string) []string {
	keys := make([]string, len(names))
	seen := make(map[string]bool, len(names))
	for i, n := range names {
		k := n
		if seen[k] {
			k = fmt.Sprintf("%s:%d", n, i)
		}
		seen[k] = true
		keys[i] = k
	}
	return keys
}

// renameRowColumnsPositional rebuilds each row under the target aliases,
// reading column i by the key POSITION i occupies rather than by the schema's
// name. A shorter alias list renames the LEADING columns and the rest keep
// their own names, which is PostgreSQL's rule for a column list.
func renameRowColumnsPositional(rows []map[string]any, schema []parquet.Column, aliases []string) []map[string]any {
	keys := make([]string, len(schema))
	names := make([]string, len(schema))
	for i, c := range schema {
		names[i] = c.Name
	}
	copy(keys, subqueryRowKeys(names))
	out := make([]map[string]any, len(rows))
	for ri, row := range rows {
		nr := make(map[string]any, len(schema))
		for i := range schema {
			name := schema[i].Name
			if i < len(aliases) {
				name = aliases[i]
			}
			nr[name] = row[keys[i]]
		}
		out[ri] = nr
	}
	return out
}

// renameRowColumns remaps row keys from schema column names to the target aliases.
func renameRowColumns(rows []map[string]any, schema []parquet.Column, aliases []string) []map[string]any {
	// Check if rename is needed
	needsRename := false
	for i, alias := range aliases {
		if i < len(schema) && schema[i].Name != alias {
			needsRename = true
			break
		}
	}
	if !needsRename {
		return rows
	}
	result := make([]map[string]any, len(rows))
	for ri, row := range rows {
		newRow := make(map[string]any, len(row))
		for i, col := range schema {
			if i < len(aliases) {
				newRow[aliases[i]] = row[col.Name]
			} else {
				newRow[col.Name] = row[col.Name]
			}
		}
		result[ri] = newRow
	}
	return result
}

// mergeDuplicateScans detects tables scanned multiple times in a query
// (e.g., from decorrelated subqueries) and merges their required columns.
// When duplicates are found, a scanCache entry is created so the first scan
// caches its results and subsequent scans replay from memory.
func (p *Planner) mergeDuplicateScans(node *logical.Node) {
	// Collect all scan nodes grouped by table name.
	scansByTable := map[string][]*logical.Node{}
	var walkScans func(n *logical.Node)
	walkScans = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeScan && n.TableName != "" {
			scansByTable[n.TableName] = append(scansByTable[n.TableName], n)
		}
		for _, c := range n.Children {
			walkScans(c)
		}
	}
	walkScans(node)

	// For tables scanned more than once, merge RequiredColumns across all scans.
	for table, scans := range scansByTable {
		if len(scans) < 2 {
			continue
		}
		// Skip if any scan has predicates or partition filters
		// (different filters = different result sets, not cacheable)
		incompatible := false
		for _, s := range scans {
			if len(s.ScanPredicates) > 0 || len(s.PartitionFilter) > 0 {
				incompatible = true
				break
			}
		}
		if incompatible {
			continue
		}
		// Compute the union of required columns in first-seen order. The
		// union lives on the CACHE entry only — each scan node keeps its
		// own RequiredColumns and catalogScanSource projects the cached
		// (union-wide) batches back down per consumer. Rewriting the scan
		// nodes to the union, as this used to do, silently widened every
		// consumer: hash-join build sides stored the union's columns and
		// spilled them on eviction (Q21's semi/anti lineitem builds).
		// A scan with no RequiredColumns needs every column: the union
		// degrades to nil (full schema).
		var merged []string
		colSet := map[string]bool{}
		needAll := false
		for _, s := range scans {
			if len(s.RequiredColumns) == 0 {
				needAll = true
				break
			}
			for _, col := range s.RequiredColumns {
				if !colSet[col] {
					colSet[col] = true
					merged = append(merged, col)
				}
			}
		}
		if needAll {
			merged = nil
		}
		// Initialize the scan cache entry.
		if p.scanCache == nil {
			p.scanCache = make(map[string]*scanCached)
		}
		p.scanCache[table] = &scanCached{unionCols: merged}
	}
}

// Plan converts a logical plan to a physical plan for local execution.
func (p *Planner) Plan(ctx context.Context, node *logical.Node) (*PhysicalPlan, error) {
	p.planCtx = ctx           // store for subquery runner context propagation
	p.releaseScanCache()      // reset per-query scan cache (drops tracker reservation)
	p.res = &queryResources{} // reset per-query spill manager + memory tracker
	p.releaseCTECache()       // reset per-query CTE cache (frees stale spill scratch)
	// Propagate CTE definitions from the logical plan so scalar subqueries
	// (e.g., in WHERE/HAVING) can resolve CTE table references.
	if len(node.CTEs) > 0 {
		p.ctes = node.CTEs
	}

	// A star that could not be expanded, refused with the planner's own
	// sentence BEFORE the ordinal one — the order PlanDistributed uses, so
	// both engines say the same thing about `SELECT s.* … ORDER BY 1`: the
	// star is the reason and the un-countable ordinal is its consequence.
	if err := refuseUnexpandedStarAnywhere(node); err != nil {
		return nil, err
	}
	// A `SELECT * ... ORDER BY <n>` whose star never expanded (#810). Refused
	// here rather than in buildSort so this path and PlanDistributed say the
	// same thing about the same query.
	if err := logical.RefuseUnresolvedOrdinalSortKeys(node); err != nil {
		return nil, err
	}
	// …and a COLUMN-ALIAS LIST longer than the `SELECT *` body it renames, for
	// the same reason and at the same place: the width is a pass later than
	// the builder, so PostgreSQL's 42P10 is raised a pass later too (#958,
	// column_alias_defer.go).
	if err := logical.RefuseUnappliedColumnAliasLists(node); err != nil {
		return nil, err
	}

	// The projection whose names the CLIENT reads, resolved once (#732).
	p.outputProjection = findOutputProjectionNode(node)

	// Materialize CTEs referenced multiple times. Each CTE is computed once
	// and cached so that all references (main query + subqueries) see the
	// exact same data. This prevents float64 accumulation-order divergence
	// that would break exact equality comparisons (e.g., TPC-H Q15).
	p.materializeCTEs(ctx, node)

	// Detect tables scanned multiple times and merge their column needs.
	// The first scan caches decoded batches; subsequent scans replay from cache.
	p.mergeDuplicateScans(node)

	// The same plan-time refusal PlanDistributed makes, so the single-process
	// engine and the small-query fast path raise it too — and raise it for a
	// predicate no row ever reaches, which the operator-level check cannot
	// (#631 follow-up).
	if err := refuseUnrepresentableRealInList(node); err != nil {
		p.resources().releaseSubqueryCharges()
		p.releaseCTECache()
		p.releaseScanCache()
		return nil, err
	}

	source, ops, sink, err := p.buildPipeline(ctx, node)
	if err != nil {
		p.resources().releaseSubqueryCharges()
		p.releaseCTECache()  // free CTE spill scratch on the no-Cleanup path
		p.releaseScanCache() // drop the scan cache's tracker reservation
		return nil, err
	}

	// Drop the columns the logical builder materialized for its own use so the
	// client sees exactly the columns it selected (#320).
	if trim := hiddenSortTrimOp(node); trim != nil {
		ops = append(ops, trim)
	}

	// Front-load bloom filters whose key columns exist in the source scan.
	// In multi-way joins with selective semi/anti-join bloom filters (e.g.,
	// Q18's HAVING filter), this eliminates most rows at the source before
	// expensive join probes run.
	ops = frontLoadBlooms(source, ops)

	// Enable parallel pipeline execution when the source supports
	// concurrent Next() calls (channel-based scan sources).
	pipelineWorkers := 0
	switch source.(type) {
	case *catalogScanSource, *scannerExecSource, *deferredJoinBridge:
		pipelineWorkers = scanParallelism()
	}

	plan := &PhysicalPlan{
		Pipeline: &exec.Pipeline{
			Source:  source,
			Ops:     ops,
			Sink:    sink,
			Workers: pipelineWorkers,
		},
		OutputSchema: declaredOutputSchema(node, p.subqueryOutputColumn),
	}
	// Hand the sink the plan's answer for the case where no batch will ever
	// tell it: a zero-row result. It is consulted only then (#416).
	if cs, ok := sink.(*exec.CollectSink); ok {
		cs.SchemaHint = plan.OutputSchema
		// The names the CLIENT is owed, positionally: PostgreSQL's
		// FigureColname for every unaliased item (#732). Applied at the sink
		// rather than inside the plan, because inside the plan a name is also
		// a HANDLE — a sort key, a HAVING reference, an aggregate's OutputCol
		// — and the two are not the same string.
		cs.OutputNames = publishedOutputNames(p.outputProjection)
		// Unlike SchemaHint, this is consulted on EVERY result, zero-row or
		// not: which DECIMAL columns are aggregate output is a property of
		// the PLAN, not of whether a batch arrived (FIX 2, #457/#458 fold-in).
		//
		// Both maps are keyed by the name the CLIENT reads, which is the
		// PUBLISHED one (#732): filed under the resolution spelling they miss,
		// and an unaliased `s_acctbal + 1` goes out with a DECIMAL typmod
		// PostgreSQL sends -1 for.
		rawWire, rawLens := declaredWireUnconstrainedDecimal(node), DeclaredStringLengths(node)
		// POSITIONAL first, and it is the authority: a name is not an address
		// when two output columns publish one (#732, round-1 review B2).
		cs.SchemaHintWireUnconstrainedPos, cs.SchemaHintStringLengthPos =
			publishedOutputDecls(p.outputProjection, rawWire, rawLens)
		cs.SchemaHintWireUnconstrainedDecimal = republishDeclaredNames(p.outputProjection, rawWire)
		// And the string family's modifier, which is a LENGTH rather than a
		// (p,s) — same lifecycle, same reason (#838).
		cs.SchemaHintStringLength = republishDeclaredNames(p.outputProjection, rawLens)
	}

	// Attach spill file cleanup. CTE collectors and the scan cache
	// release first (tracker charge + their scratch files) so the
	// SpillManager sweep that follows never races their removal. The
	// scan cache release matters most on the shared-tracker path: its
	// reservation would otherwise outlive the query as a permanent
	// phantom on the worker-lifetime tracker.
	res := p.resources()
	if sm := p.spillManagerIfSet(); sm != nil {
		plan.Cleanup = func() {
			p.closeBuiltJoins()
			res.releaseSubqueryCharges()
			p.releaseCTECache()
			p.releaseScanCache()
			sm.Cleanup()
		}
	} else if p.cteCacheHasCollectors() || p.scanCache != nil || len(p.builtJoins) > 0 || res.hasSubqueryCharges() {
		// Shared (worker-injected) spill manager: its dir outlives this
		// query, so the collectors' scratch must be released explicitly.
		plan.Cleanup = func() {
			p.closeBuiltJoins()
			res.releaseSubqueryCharges()
			p.releaseCTECache()
			p.releaseScanCache()
		}
	}

	// Generate distributed stages for coordinator dispatch
	plan.Stages = p.generateStages(node)

	if err := p.enforceQueryLimits(ctx, plan.Stages, node); err != nil {
		p.resources().releaseSubqueryCharges()
		p.releaseCTECache()
		p.releaseScanCache()
		return nil, err
	}

	return plan, nil
}

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

// extractOutputRenames inspects the logical plan tree's outermost projection
// node and returns one (source-column → alias) pair per SELECT-list item — in
// SELECT-list order — describing the final output schema. The coordinator
// uses this list both to RENAME columns AND to DROP columns the worker
// emitted but the user didn't ask for (e.g., Q15's join output carries
// supplier/lineitem internals that the SELECT list doesn't project).
//
// For aggregate columns, the source equals the alias (planner sets
// AggSpec.OutputCol to the alias). Wrapped aggregates ("SUM(x)/7.0 AS x")
// still aren't handled here — the worker emits the raw aggregate, and
// applying the divisor needs a post-aggregate Project. Wrapped-aggregate
// projections are passed through with their source pointing at the wrapped
// expression text so that at least the rename is attempted (it'll miss
// gracefully and the column drops, surfacing the bug clearly in tests).
//
// Returns nil when the outermost emitting node isn't a projection (e.g.,
// top-level scan or aggregate without a SELECT-list rename layer).
func extractOutputRenames(root *logical.Node) []OutputRename {
	// Hidden projections are the planner's own — a materialized ORDER BY term
	// the SELECT list does not carry (#320). Leaving them out of the rename
	// list is what drops them from the client's result: the gather projects to
	// exactly the columns named here.
	proj := logical.VisibleProjections(findOutputProjectionsForRename(root))
	if len(proj) == 0 {
		return nil
	}
	// An expression the gather EVALUATES is evaluated over the producer's
	// output, where a derived GROUP BY key is one column and the input
	// columns it was computed from are gone. `(g + 1) + COUNT(*)` reached
	// here spelled over `g`, so the gather read a column the aggregate does
	// not emit and answered NULL for every row while the single-process path
	// answered correctly — the same identity gap as #723, one stage later.
	keyRefs := groupKeyByIdentity(aggregateUnderOutput(root))
	renames := make([]OutputRename, 0, len(proj))
	renameScope := findOutputProjectionNode(root)
	for _, p := range proj {
		var src, target string
		var astExpr plansql.Node
		var declType expr.DeclType
		declKnown := false
		// The CLASS of what this item refers to, not of the item itself. A
		// wrapper — one derived table or one CTE — makes an aggregate output
		// a plain column reference to the block above, and the gather pairs a
		// duplicate source name with the column of its own class (#575,
		// #785). Asking `p.IsAgg` asked about the wrapper (#785 round 2).
		isAgg := p.IsAgg
		if !isAgg && renameScope != nil && len(renameScope.Children) == 1 {
			src := p.Column
			if src == "" {
				src = strings.ToLower(strings.TrimSpace(p.Expr))
			}
			isAgg = renameIsAggregateOutput(src, renameScope.Children[0])
		}
		switch {
		case p.IsAgg:
			// AggSpec.OutputCol == alias; if no alias, fall back to expr.
			//
			// The SOURCE is the lowercased text because that is the spelling
			// the aggregate stage emits under. The TARGET is the name the
			// CLIENT is told, and it is the expression text VERBATIM —
			// `deriveColumns` (wadjet/wadjet.go) sends `col.Expr` unfolded on
			// the single-process path, and the two paths must describe one
			// query identically (#744). Case-folding here is what made
			// `SUM(a) OVER (...) + 1` arrive as `... over (...) + 1` from the
			// DAG and `... OVER (...) + 1` from the single-process engine.
			target = p.Alias
			src = target
			if target == "" {
				target = p.Expr
				src = strings.ToLower(p.Expr)
			}
		case p.ASTExpr != nil && !isSimpleColRefForRename(p.ASTExpr) &&
			(referencesSyntheticAgg(p.ASTExpr) || referencesSyntheticWindow(p.ASTExpr)):
			// Wrapped aggregate — the logical layer replaced aggregate calls
			// with ColRefs to their __agg_N synthetic columns. Compile+eval
			// at gather time so the divisor (e.g. "/7.0" in Q17's avg_yearly)
			// gets applied. We restrict this to expressions that reference
			// __agg_N because pure scalar expressions like SUBSTR(o_orderdate,
			// 1, 4) are computed by the worker's GROUP BY / project pipeline
			// and surface as a column under the expression's lowercased text
			// — those need a plain rename, not eval (and eval would mistype
			// SUBSTR's string output as float64).
			//
			// A nested WINDOW expression (#610) is the same shape: the window
			// is extracted into a __win_N column by the window stage and the
			// surrounding SUM(x) OVER (...) + 1 references it. The Project
			// above the window is a DAG passthrough, so nothing between the
			// window stage and the gather ever applies the "+ 1"; evaluating
			// it here is what keeps the DAG's answer equal to the
			// single-process pipeline's instead of emitting the bare window.
			//
			// Verbatim, not lowercased, for the same reason as the aggregate
			// arm above (#744): this is the CLIENT's name, and the source is
			// resolved separately by firstColRefName.
			target = p.Alias
			if target == "" {
				target = p.Expr
			}
			astExpr = plansql.ReplaceGroupKeyRefs(p.ASTExpr, keyRefs)
			src = firstColRefName(astExpr)
			// The declaration for the column the gather is about to build.
			// Inferred against the scope the RESPELLED expression reads —
			// the producer's output, where `__agg_N` and `__win_N` live —
			// which is the same scope attachScanSelectProjections types its
			// own specs against.
			declType, declKnown = inferRenameExprDecl(astExpr, renameScope)
		case p.Column != "" && p.Alias != "":
			// Bare column reference. Worker may emit qualified ("n1.n_name")
			// or unqualified ("n_name") depending on the upstream join chain.
			// Prefer Expr (qualified-preserving) when it's a colref, else Column.
			src = p.Column
			if p.Expr != "" {
				src = strings.ToLower(p.Expr)
			}
			target = p.Alias
		case p.Expr != "":
			src = strings.ToLower(p.Expr)
			target = p.Alias
			if target == "" {
				target = src
				// No AS on a plain column reference: SQL names the output
				// column after the COLUMN, not the qualified reference the
				// user typed — `SELECT d.label` is a column named "label",
				// which is what the single-process path returns. Projection
				// .Column is the parser's unqualified name and is set only
				// for a colref, so this branch is exactly that shape. The
				// SOURCE stays the qualified Expr; the gather resolves it
				// through the same qualified↔bare fallback everything else
				// uses.
				if p.Column != "" {
					target = p.Column
				}
			}
		case p.Column != "":
			src = p.Column
			target = src
		default:
			continue
		}
		// The TARGET is the name the CLIENT is told, and PostgreSQL's name for
		// an unaliased item is not its text: `SELECT g + 1` is `?column?`,
		// `SELECT COUNT(*)` is `count`, `SELECT CAST(g AS bigint)` is `g`
		// (#732). The SOURCE is untouched — it is how the gather FINDS the
		// column in the stage's stream, and that is still the resolution
		// spelling. This is the DAG's half of the split; the single-process
		// half is CollectSink.OutputNames.
		if p.PublishedName != "" {
			target = p.PublishedName
		}
		renames = append(renames, OutputRename{
			From: src, To: target, Expr: astExpr, IsAgg: isAgg,
			Type: declType.ID, TypeKnown: declKnown,
			Precision: declType.Precision, Scale: declType.Scale,
		})
	}
	return renames
}

// isSimpleColRefForRename is the OutputRenames-specific variant of
// isSimpleColRef. The base helper at line 4308 also returns true for Lit
// nodes; here we only want to skip the eval path when the projection is
// strictly a column reference (or a parenthesized one).
func isSimpleColRefForRename(n plansql.Node) bool {
	if n == nil {
		return false
	}
	if _, ok := n.(*plansql.ColRef); ok {
		return true
	}
	if p, ok := n.(*plansql.ParenNode); ok {
		return isSimpleColRefForRename(p.Inner)
	}
	return false
}

// referencesSynthetic reports whether an AST contains any ColRef whose name
// starts with prefix — "__agg_" for the nested-aggregate rewrite, "__win_" for
// the nested-window rewrite (#610). It MUST traverse exactly the node set the
// logical rewrites do (plansql.ReplaceAllAggregates / ReplaceWindowFuncs):
// those rewrites can bury a __agg_/__win_ ColRef under a boolean/predicate
// wrapper (BETWEEN, AND/OR, IN, LIKE, IS, ANY/ALL), and a helper that stopped
// short there told extractOutputRenames the projection was a plain column
// rename. The gather then emitted the internal synthetic column (and the raw
// base columns beside it) to the client instead of evaluating the wrapper —
// the exact "wrong answer, right shape" leak #610 set out to kill, on the DAG.
func referencesSynthetic(n plansql.Node, prefix string) bool {
	if n == nil {
		return false
	}
	switch x := n.(type) {
	case *plansql.ColRef:
		return strings.HasPrefix(x.Column, prefix)
	case *plansql.BinaryOp:
		return referencesSynthetic(x.Left, prefix) || referencesSynthetic(x.Right, prefix)
	case *plansql.UnaryOp:
		return referencesSynthetic(x.Inner, prefix)
	case *plansql.CmpExpr:
		return referencesSynthetic(x.Left, prefix) || referencesSynthetic(x.Right, prefix)
	case *plansql.ParenNode:
		return referencesSynthetic(x.Inner, prefix)
	case *plansql.CastNode:
		return referencesSynthetic(x.Inner, prefix)
	case *plansql.FuncCallNode:
		for _, a := range x.Args {
			if referencesSynthetic(a, prefix) {
				return true
			}
		}
	case *plansql.CaseNode:
		if referencesSynthetic(x.Subject, prefix) {
			return true
		}
		for _, w := range x.Whens {
			if referencesSynthetic(w.Cond, prefix) || referencesSynthetic(w.Result, prefix) {
				return true
			}
		}
		return referencesSynthetic(x.Else, prefix)
	case *plansql.IsExpr:
		return referencesSynthetic(x.Left, prefix)
	case *plansql.NotNode:
		return referencesSynthetic(x.Inner, prefix)
	case *plansql.AndNode:
		return referencesSynthetic(x.Left, prefix) || referencesSynthetic(x.Right, prefix)
	case *plansql.OrNode:
		return referencesSynthetic(x.Left, prefix) || referencesSynthetic(x.Right, prefix)
	case *plansql.InExpr:
		if referencesSynthetic(x.Left, prefix) {
			return true
		}
		for _, v := range x.Values {
			if referencesSynthetic(v, prefix) {
				return true
			}
		}
	case *plansql.BetweenExpr:
		return referencesSynthetic(x.Left, prefix) ||
			referencesSynthetic(x.Low, prefix) || referencesSynthetic(x.High, prefix)
	case *plansql.LikeExpr:
		return referencesSynthetic(x.Left, prefix) || referencesSynthetic(x.Pattern, prefix)
	case *plansql.AnyAllExpr:
		if referencesSynthetic(x.Left, prefix) {
			return true
		}
		for _, v := range x.Values {
			if referencesSynthetic(v, prefix) {
				return true
			}
		}
	case *plansql.TupleNode:
		for _, e := range x.Elements {
			if referencesSynthetic(e, prefix) {
				return true
			}
		}
	case *plansql.ArrayLitNode:
		for _, e := range x.Elements {
			if referencesSynthetic(e, prefix) {
				return true
			}
		}
	}
	return false
}

// referencesSyntheticAgg reports whether an AST references a nested-aggregate
// synthetic column (__agg_N). Lets the gather rewrite distinguish "SUM(x)/7.0"
// (rewritten to "__agg_0/7.0", needs eval) from "SUBSTR(o_orderdate, 1, 4)"
// (worker-computed, needs rename).
func referencesSyntheticAgg(n plansql.Node) bool {
	return referencesSynthetic(n, "__agg_")
}

// referencesSyntheticWindow reports whether an AST references a nested-window
// synthetic column (__win_N), the marker the logical builder's nested-window
// rewrite uses for a window extracted out of a larger expression (#610).
func referencesSyntheticWindow(n plansql.Node) bool {
	return referencesSynthetic(n, "__win_")
}

// firstColRefName returns the first column reference name in an AST, used as
// the existence anchor for the gather rewrite. Returns "" when no ColRef is
// found (very rare — pure-literal projections).
func firstColRefName(n plansql.Node) string {
	if n == nil {
		return ""
	}
	switch x := n.(type) {
	case *plansql.ColRef:
		if x.Table != "" {
			return x.Table + "." + x.Column
		}
		return x.Column
	case *plansql.BinaryOp:
		if v := firstColRefName(x.Left); v != "" {
			return v
		}
		return firstColRefName(x.Right)
	case *plansql.UnaryOp:
		return firstColRefName(x.Inner)
	case *plansql.CmpExpr:
		if v := firstColRefName(x.Left); v != "" {
			return v
		}
		return firstColRefName(x.Right)
	case *plansql.ParenNode:
		return firstColRefName(x.Inner)
	case *plansql.FuncCallNode:
		for _, a := range x.Args {
			if v := firstColRefName(a); v != "" {
				return v
			}
		}
	case *plansql.CastNode:
		return firstColRefName(x.Inner)
	}
	return ""
}

// findOutputProjectionsForRename walks down through Sort/Limit/Filter wrappers
// to the outermost NodeProject and returns its projections. Returns nil when
// the outermost emitting node is not a projection (e.g., a top-level scan or
// aggregate without a SELECT-list rename layer).
func findOutputProjectionsForRename(n *logical.Node) []logical.Projection {
	if p := findOutputProjectionNode(n); p != nil {
		return p.Projections
	}
	return nil
}

// findOutputProjectionNode is findOutputProjectionsForRename returning the
// Project node itself, for callers that also need what feeds it — typing a
// projection expression takes the input's column types (inputColTypes).
func findOutputProjectionNode(n *logical.Node) *logical.Node {
	for n != nil {
		switch n.Type {
		case logical.NodeProject:
			return n
		case logical.NodeSort, logical.NodeLimit, logical.NodeFilter, logical.NodeDistinct:
			// Descend through Distinct too: the gather must project to the
			// SELECT-list columns (the Project under the Distinct) so the
			// coordinator's distinct dedup runs over the output columns, not
			// the full upstream schema (#163).
			if len(n.Children) == 1 {
				n = n.Children[0]
				continue
			}
			return nil
		default:
			return nil
		}
	}
	return nil
}

// hiddenSortTrimOp returns the projection that drops a materialized ORDER BY
// term from the single-process pipeline's output, or nil when the plan carries
// none.
//
// The DAG drops these at the gather: extractOutputRenames lists only the
// visible select items and the gather projects to exactly that set. The
// single-process pipeline has no equivalent stage — its result columns are the
// sink's schema — so without this the Sort would hand __sortkey_N straight to
// the client alongside the columns the query asked for. The projection below
// the Sort is never elided when it carries a hidden column (its alias always
// differs from its source, which is what buildProject's needsProject test
// looks for), so the names resolved here are the ones the pipeline emits.
func hiddenSortTrimOp(root *logical.Node) exec.UnaryOperator {
	projs := findOutputProjectionsForRename(root)
	if !logical.HasHiddenProjection(projs) {
		return nil
	}
	visible := logical.VisibleProjections(projs)
	cols := make([]exec.ProjectColumn, 0, len(visible))
	for i, p := range visible {
		// Same output naming buildProject applies: the alias, else the column
		// reference, else the expression text.
		name := p.Alias
		if name == "" {
			name = p.Column
		}
		if name == "" {
			name = cleanExpr(p.Expr)
		}
		if name == "" || name == "*" || strings.HasSuffix(name, ".*") {
			// An unexpanded star (no catalog to resolve it against) has no
			// column list to trim to. Leave the plan alone: an extra column in
			// the result beats projecting every row to nulls.
			return nil
		}
		cols = append(cols, exec.ProjectColumn{
			Name: name,
			// POSITIONAL, not by name. The trim is a narrowing: hidden
			// columns go LAST and stay last (logical.resolveOrderBy keeps
			// that invariant for the gather's benefit too), so visible
			// output i IS input column i. Copying by name instead gave two
			// same-named outputs — which `SELECT abs(a), abs(b)` now
			// legitimately produces, PostgreSQL calling both `abs` — the
			// SAME input column, so the second carried the first's values.
			SourceIdx:    i,
			SourceIdxSet: true,
			// The name-based fields stay as the fallback for an input whose
			// column count does not match (nothing produces one today, and
			// an extra column beats projecting every row to nulls).
			DirectCopy: name,
			SourceCol:  name,
			// DirectCopy resolution can still miss on a qualified/bare
			// mismatch; ColumnRef resolves lazily and keeps Project.Execute
			// from invoking a nil Expr.
			Expr: exec.ColumnRef(name),
		})
	}
	return exec.NewProject(cols)
}

// QueryCost summarizes the estimated cost of a query across all scan stages.
type QueryCost struct {
	TotalBytes int64
	TotalRows  int64
	TotalFiles int
	HasFilter  bool
	HasLimit   bool
}

// EstimateCost computes the aggregate cost of a set of stages.
func EstimateCost(stages []Stage, node *logical.Node) QueryCost {
	var cost QueryCost
	for _, s := range stages {
		if s.Type == "scan" {
			cost.TotalBytes += s.EstimatedBytes
			cost.TotalRows += s.EstimatedRows
			cost.TotalFiles += len(s.ScanFiles)
		}
	}
	cost.HasFilter = hasFilterOrPartition(node)
	cost.HasLimit = hasLimit(node)
	return cost
}

// ProbeSplitMinBytes is the minimum size of the largest scan required to
// activate probe-split. Below this, the orchestration overhead exceeds the
// parallelism benefit. Exported so tests can lower it to exercise the
// distributed path on tiny datasets — otherwise every test silently runs
// the single-worker path and distributed-only bugs (like the SF100 build
// cache Q02 regression) never get caught.
var ProbeSplitMinBytes int64 = 64 * 1024 * 1024

// ReverseBloomThreshold and ReverseBloomInnerThreshold gate the reverse-bloom
// optimization (see buildJoin). Declared as vars so regression tests can lower
// them to fire on tiny SF0.x datasets — TestTPCHReverseBloomForcedSF001 does
// exactly that — and so they can be raised at runtime to turn the optimization
// off without rebuilding.
//
// These lines used to say the vars existed "to disable the optimization while
// we hunt the SF100 Q05 0-rows bug whose triggering code path is somewhere in
// this optimization", and that the semi/anti threshold stayed at 10M because
// there was "no evidence of bugs there yet". Both halves are settled now, and
// not in the direction the second one guessed.
//
// A 0-rows MECHANISM in this optimization is identified and fixed (#543):
// reverseBloomBridge installed the bloom whether or not the key column had
// been found in the probe output, so a probeKey that did not resolve produced
// an EMPTY bloom that rejected every build row — a join answering over an
// empty build side, which is 0 rows for an inner or semi join. Forcing both
// thresholds to 100 over the SF0.01 corpus fires it on exactly one query,
// Q21, whose probeKey arrives alias-qualified as "l1.l_orderkey" against
// batches carrying "l_orderkey": on the parent commit Q21 returns 0 rows
// where the answer is 1 (and 0 where it is 100 at SF1). Init now refuses to
// install a bloom whose column never resolved or that received no keys.
//
// Whether that mechanism is what produced the Q05 incident at SF100 was never
// reduced to a repro and is not claimed here: Q05's own reverse blooms resolve
// their columns at SF0.01, and the corpus-wide forced run shows Q21 as the
// only unresolved one. What IS claimed is that this optimization could return
// 0 rows for a reason that had nothing to do with the query, that the reason
// is now gone, and that a gate runs the whole corpus with both thresholds
// forced down so the next one cannot hide behind a production threshold.
//
// The semi/anti threshold's "no evidence of bugs there yet" was wrong twice
// over: #543's key-encoding divergence was semi/anti-only in practice, since
// that is where string keys appear, and the empty-bloom mechanism above fires
// on a semi/anti query. The threshold stays at 10M for COST reasons.
//
// Init reads WADJET_REVERSE_BLOOM_INNER_THRESHOLD if set, so the bench can
// disable the inner-join path on SF100 without rebuilding the binary.
var (
	ReverseBloomThreshold      int64 = 10_000_000
	ReverseBloomInnerThreshold int64 = 50_000_000
)

// reverseBloomToggle is the kill switch for the whole reverse-bloom path
// (#287's convention: WADJET_REVERSE_BLOOM=0 disables). The optimization
// removes build-side rows before they reach the hash table, so a defect in it
// is a defect in the ANSWER — #543 dropped every row of a string-keyed
// semi/anti build — and the invariance oracle can only compare against a run
// without it if there is a switch to turn it off.
var reverseBloomToggle = optswitch.Register("reverse-bloom", "WADJET_REVERSE_BLOOM",
	"reverse-bloom pushdown: build a bloom from the probe side's join keys and filter the build-side scan with it")

func init() {
	if v := os.Getenv("WADJET_REVERSE_BLOOM_INNER_THRESHOLD"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			ReverseBloomInnerThreshold = n
		}
	}
}

// CanProbeSplit returns the scan alias and file list for probe-split pipeline
// routing. Probe-split distributes the dominant probe table's files across
// workers while each worker scans build tables in full. This enables parallel
// execution for join-heavy queries where compute is the bottleneck.
//
// Returns the probe scan alias, its file list, and true if probe-split is viable.
func CanProbeSplit(stages []Stage, workerCount int) (probeAlias string, probeFiles []string, ok bool) {
	if workerCount <= 1 {
		return "", nil, false
	}

	// Pick the largest scan as the probe-split candidate. No exclusions:
	// the physical planner uses RightSemiJoin/RightAntiJoin to swap the
	// local join's build/probe when the inner table is much larger than
	// the outer. This makes it safe to partition the inner (large) table
	// across workers — each worker builds the small outer as hash table
	// and probes with its partition of the large inner table.
	var bestAlias string
	var bestFiles []string
	var bestBytes int64
	for _, s := range stages {
		if s.Type == "scan" && s.EstimatedBytes > bestBytes {
			bestAlias = s.ScanAlias
			bestFiles = s.ScanFiles
			bestBytes = s.EstimatedBytes
		}
	}

	// Need enough files to give each worker meaningful work. For large
	// datasets (> 1 GB), relax from 2 files/worker to 1 file/worker since
	// each file is substantial. At SF100, tables may have only 3-6 files
	// but each is multi-GB.
	minFiles := workerCount * 2
	if bestBytes > 1<<30 {
		minFiles = workerCount
	}
	if bestAlias == "" || len(bestFiles) < minFiles || bestBytes < ProbeSplitMinBytes {
		return "", nil, false
	}

	return bestAlias, bestFiles, true
}

// ShuffleCandidate describes a join in the plan whose build side is large
// enough to warrant the shuffle execution path instead of broadcast.
type ShuffleCandidate struct {
	JoinStageID string   // the join stage to be served by shuffled inputs
	BuildAlias  string   // which scan stage produces the build side
	ProbeAlias  string   // which scan stage produces the probe side (the largest scan)
	BuildKeys   []string // build-side join keys (the join's JoinRightKeys)
	ProbeKeys   []string // probe-side join keys (the join's JoinLeftKeys)
	JoinKeys    []string // canonical (build-side) join key names for partitioning
	BuildBytes  int64    // EstimatedBytes of the build scan (for logging)
}

// PickShuffleCandidate identifies the largest non-probe scan above
// thresholdBytes as the shuffle candidate — the table that would otherwise be
// broadcast-duplicated as the runtime build side — and returns the join stage
// that connects it to the probe.
//
// The approach deliberately does NOT read BuildTableAlias on the join stage
// because in probe-split mode the planner's logical build/probe assignment is
// inverted at runtime: the planner labels the largest scan as the build (e.g.
// "lineitem"), but probe-split partitions that scan across workers, making the
// second-largest scan (e.g. "orders") the actual broadcast hash table.
// Shuffling orders instead of broadcasting it is the correction.
//
// Algorithm:
//  1. probeAlias = largest scan (matches CanProbeSplit's heuristic).
//  2. candidate = largest non-probe scan above thresholdBytes.
//  3. Walk join stages to find one that directly references the candidate
//     scan (via LeftDepStage or RightDepStage) or via a FusedJoin entry
//     whose BuildTableAlias matches the candidate alias.
//  4. Extract build/probe keys from the matching join or fused-join entry.
//
// Phase 1: returns the single best candidate. Phase 2 (chained shuffles)
// will return all candidates.
func PickShuffleCandidate(stages []Stage, thresholdBytes int64) (ShuffleCandidate, bool) {
	// stage-id → stage lookup.
	byID := map[string]Stage{}
	for _, s := range stages {
		byID[s.ID] = s
	}

	// Step 1: identify probe alias (largest scan, mirrors CanProbeSplit).
	var probeAlias string
	var probeBytes int64
	for _, s := range stages {
		if s.Type == "scan" && s.EstimatedBytes > probeBytes {
			probeAlias = s.ScanAlias
			probeBytes = s.EstimatedBytes
		}
	}

	// Step 2: find the largest non-probe scan above the threshold.
	var candidateScan Stage
	var candidateScanID string
	var candidateFound bool
	for _, s := range stages {
		if s.Type != "scan" || s.ScanAlias == probeAlias {
			continue
		}
		if s.EstimatedBytes <= thresholdBytes {
			continue
		}
		if !candidateFound || s.EstimatedBytes > candidateScan.EstimatedBytes {
			candidateScan = s
			candidateScanID = s.ID
			candidateFound = true
		}
	}
	if !candidateFound {
		return ShuffleCandidate{}, false
	}

	// candidateCols is the set of column names the candidate scan exposes in its
	// raw Parquet files. Used to validate that join keys are actually rooted in
	// the candidate scan and not in a fused-join output (e.g. Q10: orders is the
	// left dep of join-4, but join-4's left key c_nationkey comes from the fused
	// customer join, not from orders' Parquet files).
	// If Columns is empty (not annotated), validation is skipped and the original
	// dep-match logic applies unchanged.
	candidateCols := make(map[string]bool, len(candidateScan.Columns))
	for _, c := range candidateScan.Columns {
		candidateCols[c] = true
	}
	// keysInCols returns true iff all keys are present in cols.
	// Returns true when cols is empty (no annotation = skip validation).
	keysInCols := func(keys []string, cols map[string]bool) bool {
		if len(cols) == 0 {
			// No column annotation: cannot validate, assume valid.
			return true
		}
		if len(keys) == 0 {
			return false
		}
		for _, k := range keys {
			if !cols[k] {
				return false
			}
		}
		return true
	}

	// probeCols is the set of column names the probe scan exposes in its raw
	// Parquet files. Used to validate that probe-side keys are directly readable
	// from the probe table, not from an intermediate join output.
	probeCols := make(map[string]bool)
	for _, s := range stages {
		if s.Type == "scan" && s.ScanAlias == probeAlias {
			for _, c := range s.Columns {
				probeCols[c] = true
			}
			break
		}
	}

	// Step 3: walk join stages to find the one referencing the candidate.
	for _, j := range stages {
		if j.Type != "hash_join" && j.Type != "broadcast_join" {
			continue
		}

		// Direct left-dep match: candidate is on the left (probe) side of the join.
		// Guard: the join's left keys must actually live in the candidate scan's raw
		// Parquet columns. If they don't (e.g. Q10: join-4 has LeftDepStage=orders
		// but JoinLeftKeys=[c_nationkey] which originates from the fused customer
		// join, not from orders files), skip this match and let the column-anchored
		// fallback below find the correct join.
		if j.LeftDepStage == candidateScanID && keysInCols(j.JoinLeftKeys, candidateCols) {
			return ShuffleCandidate{
				JoinStageID: j.ID,
				BuildAlias:  candidateScan.ScanAlias,
				ProbeAlias:  probeAlias,
				BuildKeys:   append([]string(nil), j.JoinLeftKeys...),
				ProbeKeys:   append([]string(nil), j.JoinRightKeys...),
				JoinKeys:    append([]string(nil), j.JoinLeftKeys...),
				BuildBytes:  candidateScan.EstimatedBytes,
			}, true
		}

		// Direct right-dep match: candidate is on the right (build) side of the join.
		// The left dep must be the probe scan directly (not an intermediate join)
		// to ensure JoinLeftKeys map to raw probe Parquet columns.
		if j.RightDepStage == candidateScanID {
			probeScanID := ""
			for _, s := range stages {
				if s.Type == "scan" && s.ScanAlias == probeAlias {
					probeScanID = s.ID
					break
				}
			}
			if j.LeftDepStage != probeScanID {
				continue
			}
			return ShuffleCandidate{
				JoinStageID: j.ID,
				BuildAlias:  candidateScan.ScanAlias,
				ProbeAlias:  probeAlias,
				BuildKeys:   append([]string(nil), j.JoinRightKeys...),
				ProbeKeys:   append([]string(nil), j.JoinLeftKeys...),
				JoinKeys:    append([]string(nil), j.JoinRightKeys...),
				BuildBytes:  candidateScan.EstimatedBytes,
			}, true
		}

		// FusedJoin match: candidate is the build of a secondary join fused into
		// this join stage. This covers Q03's shape where orders is not the top-level
		// join's direct dep but is referenced as a fused build.
		for _, fj := range j.FusedJoins {
			if fj.BuildTableAlias == candidateScan.ScanAlias {
				return ShuffleCandidate{
					JoinStageID: j.ID,
					BuildAlias:  candidateScan.ScanAlias,
					ProbeAlias:  probeAlias,
					BuildKeys:   append([]string(nil), fj.JoinRightKeys...),
					ProbeKeys:   append([]string(nil), fj.JoinLeftKeys...),
					JoinKeys:    append([]string(nil), fj.JoinRightKeys...),
					BuildBytes:  candidateScan.EstimatedBytes,
				}, true
			}
		}
	}

	// Column-anchored fallback: the candidate appears as a left dep somewhere in
	// the join chain, but the matching join's keys were not valid for the raw
	// candidate scan (e.g. Q10: orders appears as the left dep of join-4 but with
	// c_nationkey keys from a fused join output). Find any join where:
	//   - JoinLeftKeys are all present in the candidate scan's raw columns, AND
	//   - JoinRightKeys are all present in the probe scan's raw columns.
	// This anchors both sides to their actual Parquet files regardless of the
	// dep-chain topology.
	if len(candidateCols) > 0 && len(probeCols) > 0 {
		for _, j := range stages {
			if j.Type != "hash_join" && j.Type != "broadcast_join" {
				continue
			}
			if keysInCols(j.JoinLeftKeys, candidateCols) && keysInCols(j.JoinRightKeys, probeCols) {
				return ShuffleCandidate{
					JoinStageID: j.ID,
					BuildAlias:  candidateScan.ScanAlias,
					ProbeAlias:  probeAlias,
					BuildKeys:   append([]string(nil), j.JoinLeftKeys...),
					ProbeKeys:   append([]string(nil), j.JoinRightKeys...),
					JoinKeys:    append([]string(nil), j.JoinLeftKeys...),
					BuildBytes:  candidateScan.EstimatedBytes,
				}, true
			}
			// Also check if the keys are swapped: candidate provides the right
			// keys and probe provides the left keys.
			if keysInCols(j.JoinRightKeys, candidateCols) && keysInCols(j.JoinLeftKeys, probeCols) {
				return ShuffleCandidate{
					JoinStageID: j.ID,
					BuildAlias:  candidateScan.ScanAlias,
					ProbeAlias:  probeAlias,
					BuildKeys:   append([]string(nil), j.JoinRightKeys...),
					ProbeKeys:   append([]string(nil), j.JoinLeftKeys...),
					JoinKeys:    append([]string(nil), j.JoinRightKeys...),
					BuildBytes:  candidateScan.EstimatedBytes,
				}, true
			}
		}
	}

	// No join stage references the candidate.
	return ShuffleCandidate{}, false
}

// LargeBuildScans returns scan stages that are build-side (not the probe alias)
// and whose estimated size exceeds the given threshold. These are candidates for
// the build-side broadcast cache: the coordinator pre-scans them once, caches the
// result in S3, and each worker loads the shared cache instead of independently
// scanning the large source table N times.
//
// The cache provides two wins for queries with selective build-side filters
// or wide build tables:
//
//  1. Avoids decoding the source parquet on every worker (parquet decode is
//     CPU-expensive; the cached WSHF format is essentially raw typed bytes
//     and reads in a fraction of the time).
//  2. Lets the planner overlap the slow source scan with the rest of the
//     query once instead of N times.
//
// We previously gated this on len(large) >= 2 ("only cache when multiple
// large builds would compound a worker's hash table footprint"), reasoning
// that single-large-build queries can fit one hash table in memory and the
// cache only adds spill+upload latency. SF100 deploy disproved that: Q07's
// historical 3m13s was caching orders, and skipping it pushed the same
// query past 19 minutes (workers stuck spilling/scanning parquet 3 times).
// The win from caching orders comes mostly from amortising parquet decode,
// not from memory deduplication.
func LargeBuildScans(stages []Stage, probeAlias string, thresholdBytes int64) []Stage {
	var large []Stage
	for _, s := range stages {
		if s.Type != "scan" {
			continue
		}
		if s.ScanAlias == probeAlias {
			continue // skip probe table
		}
		if s.EstimatedBytes >= thresholdBytes {
			large = append(large, s)
		}
	}
	return large
}

// CountJoinStages returns the total number of joins in the stage list,
// including hash_join and broadcast_join stages plus fused joins that were
// absorbed into parent stages by fuseJoinStages().
func CountJoinStages(stages []Stage) int {
	n := 0
	for _, s := range stages {
		if s.Type == "hash_join" || s.Type == "broadcast_join" || s.Type == StageSortMergeJoin {
			n++
		}
		n += len(s.FusedJoins)
	}
	return n
}

// canFuseScanAggregate returns true when child stages are all scans (or
// filter-pushed scans). This means partial aggregation can be fused directly
// into scan tasks, eliminating the separate aggregate stage and its S3 round-trip.
// fusesIntoACTETerminal reports whether fusing an aggregate into these child
// stages would REWRITE a stage that a CTE reference is (or may be) pointed at.
//
// The scan-aggregate fusion is the one optimization that changes what an
// ALREADY-EMITTED stage emits: it stamps FusedAggSpecs onto the scan and
// prunes its output columns, so the stage stops producing the CTE's rows and
// starts producing that consumer's partial aggregates. walkStages' CTE dedup
// then points every LATER reference at it — and the later reference's own
// aggregate reads a relation that no longer exists.
//
// #876 measured it as a hard failure on both DAG arms:
//
//	WITH c AS (SELECT id, c_i64 AS v FROM typemx)
//	SELECT COUNT(*) FROM typemx
//	WHERE c_i64 < (SELECT MAX(v) FROM c) AND c_i64 > (SELECT MIN(v) FROM c)
//	  hash aggregate: aggregate input "c_i64" is not a column of its input
//	  (input has: max(v))
//
// `MAX(v)` fused into the CTE body's scan; `MIN(v)`'s producer deduped to that
// same scan and asked it for `c_i64`.
//
// The reference COUNT cannot decide this. p.cteRefCounts is computed from the
// statement's own logical plan, and a CTE named only inside a scalar
// subquery's TEXT appears there ZERO times: each producer is planned by its
// own emitScalarProducerStagesTyped walk, sharing this cache, and the second
// walk has not happened when the first one fuses. What IS knowable at the
// fusion is that the stage was recorded as a CTE terminal — which is the
// engine's own claim that another reference may be pointed at it.
//
// Declining costs one scan -> aggregate materialization for an aggregate
// whose direct child is a CTE body's scan. It is the same rule
// assertNoConsumerScopedFilterOnSharedStage states for filters and
// projections (#656), applied to the third thing a consumer can attach to a
// producer it does not own.
func (p *Planner) fusesIntoACTETerminal(childStages []Stage) bool {
	if len(p.ctePlannedTerminal) == 0 {
		return false
	}
	terminals := make(map[string]bool, len(p.ctePlannedTerminal))
	for _, id := range p.ctePlannedTerminal {
		terminals[id] = true
	}
	for _, s := range childStages {
		if terminals[s.ID] {
			return true
		}
	}
	return false
}

func canFuseScanAggregate(childStages []Stage) bool {
	if len(childStages) == 0 {
		return false
	}
	for _, s := range childStages {
		if s.Type != "scan" {
			return false
		}
	}
	return true
}

func hasFilterOrPartition(n *logical.Node) bool {
	if n == nil {
		return false
	}
	if n.Type == logical.NodeFilter {
		return true
	}
	if n.Type == logical.NodeScan && (len(n.PartitionFilter) > 0 || len(n.ScanPredicates) > 0) {
		return true
	}
	for _, c := range n.Children {
		if hasFilterOrPartition(c) {
			return true
		}
	}
	return false
}

// needsLimitStage reports whether this NodeLimit needs a StageLimit of its
// own, given whether walkStages just handed its bound to a sort stage.
//
// Exactly three things can bound a stream on the DAG, and only one of them
// applies to any given LIMIT:
//
//   - The coordinator's post-gather pass (`mi.Limit`/`mi.Offset` in
//     ExecuteSQL). It reads `logical.ExtractMergeInfo`, which inspects the
//     PLAN ROOT and nothing else — so it reaches a top-level LIMIT and no
//     other.
//   - A sort stage's top-N. It needs an ORDER BY below the LIMIT, and it
//     truncates to limit+OFFSET rather than skipping, because the OFFSET is
//     the coordinator's job in the shape it was written for. So it covers a
//     sorted LIMIT with no OFFSET that NO LOWER LIMIT has already claimed,
//     and only that.
//   - This stage.
//
// "Disjoint" is a property of the OWNERSHIP RULE, not of the shapes: two
// LIMITs in one query can both want the same sort stage, and until #525 the
// outer one took it — overwriting the inner's bound and then suppressing its
// own stage because it had just found a sort. walkStages' backwards scan
// stops at a claimed sort for that reason, so `sorted` here means "a sort
// stage carries THIS limit", never "there is a sort somewhere below".
//
// A LIMIT the first two miss bounded NOTHING before #478: `SELECT COUNT(*)
// FROM (SELECT DISTINCT k FROM t LIMIT 2) u` counted every distinct k, and
// its plain and explicit-GROUP-BY twins did the same. Silent, deterministic,
// and dependent only on how much data sits behind the query.
//
// The root case is left exactly as it was rather than moved onto this stage:
// the coordinator's pass is correct there, and emitting a stage as well would
// apply the OFFSET twice.
func (p *Planner) needsLimitStage(node *logical.Node, sorted bool) bool {
	if node == p.limitStageRoot {
		return false // the coordinator's post-gather pass owns this one
	}
	if sorted && node.OffsetVal == 0 {
		return false // the sort stage's top-N is already the global bound
	}
	// An OFFSET alone still needs a stage: nothing below skips rows.
	return node.LimitVal != logical.NoLimit || node.OffsetVal > 0
}

func hasLimit(n *logical.Node) bool {
	if n == nil {
		return false
	}
	if n.Type == logical.NodeLimit {
		return true
	}
	for _, c := range n.Children {
		if hasLimit(c) {
			return true
		}
	}
	return false
}

// QueryLimitSQLState is the SQLSTATE a cost-guard rejection carries:
// PostgreSQL class 53 (insufficient resources), 53400
// configuration_limit_exceeded. The limit is a configured bound, not a
// property of the statement, so a client can tell "your administrator caps
// this" apart from a syntax or type error and stop retrying (#803).
const QueryLimitSQLState = "53400"

// enforceQueryLimits checks estimated query cost against the limits in force:
// the deployment's (Planner.QueryLimits, from `query_limits:` and its per-role
// overrides) narrowed by the calling identity's, which an ABAC `query_limit`
// obligation puts on the context. See identity_limits.go for why the two
// arrive by different routes and meet here.
func (p *Planner) enforceQueryLimits(ctx context.Context, stages []Stage, node *logical.Node) error {
	limits := tightestLimits(p.QueryLimits, IdentityQueryLimitsFromContext(ctx))
	if limits == nil {
		return nil
	}
	cost := EstimateCost(stages, node)

	if limits.MaxScanBytes > 0 && cost.TotalBytes > limits.MaxScanBytes {
		return sqlerr.New(QueryLimitSQLState, "query would scan %s (%d bytes) across %d files, exceeding limit of %s — add a WHERE clause or partition filter",
			formatBytes(cost.TotalBytes), cost.TotalBytes, cost.TotalFiles, formatBytes(limits.MaxScanBytes))
	}
	if limits.MaxScanRows > 0 && cost.TotalRows > limits.MaxScanRows {
		return sqlerr.New(QueryLimitSQLState, "query would scan %d rows across %d files, exceeding limit of %d rows — add a WHERE clause or LIMIT",
			cost.TotalRows, cost.TotalFiles, limits.MaxScanRows)
	}
	if limits.MaxScanFiles > 0 && cost.TotalFiles > limits.MaxScanFiles {
		return sqlerr.New(QueryLimitSQLState, "query would scan %d files, exceeding limit of %d — add a partition filter",
			cost.TotalFiles, limits.MaxScanFiles)
	}
	if limits.RequireFilterAboveBytes > 0 && cost.TotalBytes > limits.RequireFilterAboveBytes && !cost.HasFilter {
		return sqlerr.New(QueryLimitSQLState, "query scans %s without a WHERE clause (filter required above %s)",
			formatBytes(cost.TotalBytes), formatBytes(limits.RequireFilterAboveBytes))
	}
	if limits.RequireLimitAboveRows > 0 && cost.TotalRows > limits.RequireLimitAboveRows && !cost.HasLimit {
		return sqlerr.New(QueryLimitSQLState, "query scans %d rows without a LIMIT (limit required above %d rows)",
			cost.TotalRows, limits.RequireLimitAboveRows)
	}
	return nil
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<40:
		return fmt.Sprintf("%.1fTB", float64(b)/float64(1<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(b)/float64(1<<20))
	default:
		return fmt.Sprintf("%dB", b)
	}
}

// maxFusedBuildBytes is the per-fused-build EstimatedBytes ceiling above
// which fuseJoinStages refuses to absorb a broadcast join. Above this size,
// the cluster-wide S3 amplification of replicating the cache to every
// probe-split shard task outweighs the savings from skipping the
// intermediate exchange-replicate materialization. Tune via SF100+ deploys
// once we have measured numbers; 1 GB is conservative.
//
// Var rather than const so tests can lower it to exercise the skip path on
// small fixtures.
var maxFusedBuildBytes int64 = 1 * 1024 * 1024 * 1024

func (p *Planner) buildPipeline(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	// If this subtree is a materialized CTE, serve from cache instead of
	// re-executing the full sub-plan. The columnar form replays from the
	// collector (disk-backed past budget) without consuming it, so every
	// reference — main pipeline, subqueries, recursive steps — streams the
	// same data; the boxed form (recursive work table) keeps SliceSource.
	if node.CTEName != "" && p.cteCache != nil {
		if mat, ok := p.cteCache[node.CTEName]; ok {
			if mat.coll != nil {
				return mat.coll.NewReplaySource(), nil, &exec.CollectSink{}, nil
			}
			source := exec.NewSliceSource(mat.schema, mat.rows)
			return source, nil, &exec.CollectSink{}, nil
		}
	}

	switch node.Type {
	case logical.NodeLimit:
		return p.buildLimit(ctx, node)
	case logical.NodeSort:
		return p.buildSort(ctx, node)
	case logical.NodeProject:
		return p.buildProject(ctx, node)
	case logical.NodeAggregate:
		return p.buildAggregate(ctx, node)
	case logical.NodeFilter:
		return p.buildFilter(ctx, node)
	case logical.NodeScan:
		return p.buildScan(ctx, node)
	case logical.NodeJoin:
		return p.buildJoin(ctx, node)
	case logical.NodeDistinct:
		return p.buildDistinct(ctx, node)
	case logical.NodeWindow:
		return p.buildWindow(ctx, node)
	case logical.NodeUnion:
		return p.buildSetOp(ctx, node, "union")
	case logical.NodeIntersect:
		return p.buildSetOp(ctx, node, "intersect")
	case logical.NodeExcept:
		return p.buildSetOp(ctx, node, "except")
	case logical.NodeDual:
		return &exec.DualSource{}, nil, &exec.CollectSink{}, nil
	default:
		return nil, nil, nil, fmt.Errorf("unsupported plan node: %s", node.Type)
	}
}

func (p *Planner) buildScan(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	// Track scan alias for both MaterializedInputs and ScanFileFilter.
	// Alias scheme matches walkStages: "table" for first, "table:N" for duplicates.
	if p.scanCounter == nil {
		p.scanCounter = make(map[string]int)
	}
	n := p.scanCounter[node.TableName]
	p.scanCounter[node.TableName] = n + 1

	scanAlias := node.TableName
	if n > 0 {
		scanAlias = fmt.Sprintf("%s:%d", node.TableName, n)
	}

	// Scan-split pipeline mode: use streaming or materialized pre-scanned data.
	// StreamingSources is preferred — yields batches lazily without upfront
	// memory allocation. Falls back to MaterializedInputs for compatibility.
	if p.StreamingSources != nil {
		if src, ok := p.StreamingSources[scanAlias]; ok {
			return src, nil, &exec.CollectSink{}, nil
		}
	}
	if p.MaterializedInputs != nil {
		if batches, ok := p.MaterializedInputs[scanAlias]; ok && len(batches) > 0 {
			return exec.NewBatchSource(batches), nil, &exec.CollectSink{}, nil
		}
	}

	// Table functions (read_json, read_csv, etc.) bypass the catalog scan
	if node.IsTableFunc {
		// ...and so they bypassed every access check, which is why they are
		// authorized HERE, at the one place a table-function source is built.
		// A subquery and a CTE body are planned as SEPARATE plans inside this
		// planner, so an authorization pass over the statement's plan alone
		// cannot see the `read_csv` inside `(SELECT COUNT(*) FROM read_csv(…))`
		// — the same bypass #859's column policies had. The decision itself
		// lives in `internal/auth`, which imports this package, so it arrives
		// as a guard on the context (#943).
		if guard := logical.TableFuncGuardFromContext(ctx); guard != nil {
			if err := guard(node.FuncName, node.FuncArgs, node.FuncNamedArgs); err != nil {
				return nil, nil, nil, err
			}
		}
		if node.FuncName == "unnest" {
			source, err := newUnnestSource(node.FuncArgs, node.WithOrdinality, node.FuncColAliases)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("unnest: %w", err)
			}
			return source, nil, &exec.CollectSink{}, nil
		}
		source, err := buildTableFunctionSource(node.FuncName, node.FuncArgs, node.FuncNamedArgs)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("table function %s: %w", node.FuncName, err)
		}
		return source, nil, &exec.CollectSink{}, nil
	}
	scanner := p.newScanner(ctx, node.TableName, node.PartitionFilter, node.RequiredColumns, node.ScanPredicates)

	// Lengths-only decode for columns the logical analysis proved are
	// consumed for their SHAPE only (logical/shape_only_columns.go). Skipped
	// for a scan feeding the multi-consumer scan cache: a replay consumer
	// was not part of the analyzed plan, exactly as scan-filter pushdown
	// excludes it.
	if len(node.ShapeOnlyColumns) > 0 {
		if cs, ok := scanner.(*catalogScanSource); ok && cs.cache == nil {
			cs.shapeOnlyCols = make(map[string]bool, len(node.ShapeOnlyColumns))
			for _, c := range node.ShapeOnlyColumns {
				cs.shapeOnlyCols[strings.ToLower(c)] = true
			}
			ShapeOnlyColumnsPlanned.Add(int64(len(node.ShapeOnlyColumns)))
		}
	}

	// Probe-split pipeline mode: restrict this scan to only allowed files.
	if p.ScanFileFilter != nil {
		if files, ok := p.ScanFileFilter[scanAlias]; ok {
			if cs, ok := scanner.(*catalogScanSource); ok {
				cs.allowedFiles = files
			}
		}
	}

	var ops []exec.UnaryOperator

	if node.SampleMethod != "" && node.SamplePercent > 0 {
		ops = append(ops, newSampleOperator(node.SampleMethod, node.SamplePercent))
	}
	return scanner, ops, &exec.CollectSink{}, nil
}

// ReverseBloomsInstalled counts reverse-bloom filters actually pushed onto a
// build-side scan. A gate that means to exercise this path asserts on it:
// without it, a test can only prove the query answered, not that the
// optimization it was written for ever engaged.
var ReverseBloomsInstalled atomic.Int64

// BuildSemiAntiFilter compiles a non-equality join filter string (e.g., "l_suppkey != l_suppkey")
// into a function that evaluates the condition on probe and build batch rows.
// Convention: left of operator = probe column, right = build column.
//
// The returned closure lazily resolves column indices on first call and caches
// them, avoiding per-row ColumnByName lookups. Comparisons use typed dispatch
// (int32, int64, float64, string) instead of fmt.Sprint conversion.
//
// HashJoin's probe runs in parallel — multiple workers call this filter
// concurrently against probe and build batches whose schemas are stable
// across the lifetime of the query (same logical plan → same projected
// columns). Use sync.Once to resolve indices safely on first call; later
// calls become a single relaxed atomic load on the once.done flag.
// SemiAntiNE gates the distinct-pair semi/anti build fast path
// (exec/join_semianti_ne.go). Kill switch WADJET_SEMIANTI_NE=0.
var SemiAntiNE atomic.Bool

func init() {
	SemiAntiNE.Store(os.Getenv("WADJET_SEMIANTI_NE") != "0")
}

// pipelineSource wraps a Source + UnaryOps into a single Source.
//
// It honours the bounded-output protocol (exec.BoundedOutputOperator, #317):
// an operator whose output for one input batch can be far larger than that
// batch — a hash-join probe fans one probe row out to every build row sharing
// its key — emits a bounded slice and suspends the rest, and a Next that finds
// pending output resumes it instead of pulling new input. Being a pull driver
// makes that natural: one Next, one batch.
//
// Resumption goes DEEPEST first. A suspended operator's pending output was
// produced from an input the operators after it have not seen yet, so it must
// drain before the operator above it is asked for its next slice.
type pipelineSource struct {
	source  exec.Source
	ops     []exec.UnaryOperator
	bounded []exec.BoundedOutputOperator // parallel to ops; nil = single-shot op
	inited  bool
	// flushIdx is the operator whose SPILLED partitions this source is
	// draining now that its input is exhausted; len(ops) means every one of
	// them has been drained. See nextFlushed (#1010).
	flushIdx int
	// drained latches the input's end. Once the source has answered nil it is
	// never pulled again: a flushed batch is a real return, so the consumer
	// calls Next once more, and an exhausted source is not owed a second
	// question.
	drained bool
}

func (ps *pipelineSource) Init(ctx context.Context) error {
	if ps.inited {
		return nil
	}
	ps.inited = true
	if err := ps.source.Init(ctx); err != nil {
		return err
	}
	for _, op := range ps.ops {
		if err := op.Init(ctx); err != nil {
			return err
		}
	}
	// The opt-in is this driver's promise to drain pending output before
	// supplying the next input batch.
	exec.EnableBoundedOutput(ps.ops)
	ps.bounded = make([]exec.BoundedOutputOperator, len(ps.ops))
	for i, op := range ps.ops {
		if bo, ok := op.(exec.BoundedOutputOperator); ok {
			ps.bounded[i] = bo
		}
	}
	return nil
}

func (ps *pipelineSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	for {
		if i := ps.pendingFrom(); i >= 0 {
			out, err := ps.bounded[i].NextOutput(ctx)
			if err != nil {
				return nil, err
			}
			if out == nil {
				continue // that operator finished; look for the next one
			}
			b, err := ps.runFrom(ctx, i+1, out)
			if err != nil {
				return nil, err
			}
			if b != nil {
				return b, nil
			}
			continue
		}
		if ps.drained {
			return ps.nextFlushed(ctx)
		}
		b, err := ps.source.Next(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			ps.drained = true
			return ps.nextFlushed(ctx)
		}
		b, err = ps.runFrom(ctx, 0, b)
		if err != nil {
			return nil, err
		}
		if b != nil {
			return b, nil
		}
	}
}

// nextFlushed drains the SPILLED partitions of every operator in this chain
// once the input is exhausted, one batch per call, pushing each through the
// operators above it.
//
// A join that evicted partitions holds rows on DISK, and the only thing that
// puts them back in the answer is its own flush. `exec.Pipeline.flushSpilledOps`
// runs that drain for the operators of the TOP pipeline — and this source is
// how a nested chain is driven: a join's build side, a set-operation arm, the
// inner side of a lateral. Its operators are in `ps.ops`, never in the outer
// pipeline's, so nothing flushed them, and a spilled join under one answered
// with the evicted partitions' rows simply missing (#1010).
//
// It is a SILENT loss and the whole result can be empty: with two decorrelated
// LATERALs joined to a fourth relation under a 512 KiB budget, every probe row
// routed to a spilled partition, `HashJoinProbe.Execute` returned nil for each
// of them, and the query answered `cols=[] rows=0` — no rows, and, being a
// star over more than one join, no declared columns either — where PostgreSQL
// 17 and the same query with a budget that does not spill answer four rows.
//
// `joinFlushSource` already carries this rule for the ONE shape it covers (a
// RIGHT or FULL join's own probe, #550) and its comment states the general
// case: "the probe sits in innerOps here, never in the outer Pipeline's Ops,
// so exec.Pipeline.flushSpilledOps never sees it". This is that sentence
// applied to every nested chain rather than to one join type. The drain is
// `exec.FlushableOperator`, the same interface and the same ascending order
// the top pipeline uses, so a flushed batch passes through the operators ABOVE
// its producer exactly as an ordinary one does.
func (ps *pipelineSource) nextFlushed(ctx context.Context) (*batch.RecordBatch, error) {
	for ps.flushIdx < len(ps.ops) {
		fo, ok := ps.ops[ps.flushIdx].(exec.FlushableOperator)
		if !ok || !fo.HasPendingFlush() {
			ps.flushIdx++
			continue
		}
		b, err := fo.NextFlush(ctx)
		if err != nil {
			return nil, fmt.Errorf("flushing spilled data: %w", err)
		}
		if b == nil {
			ps.flushIdx++
			continue
		}
		out, err := ps.runFrom(ctx, ps.flushIdx+1, b)
		if err != nil {
			return nil, err
		}
		if out == nil {
			continue
		}
		return out, nil
	}
	return nil, nil
}

// pendingFrom returns the index of the deepest operator with output still to
// emit, or -1. nil bounded (Init not run) means nothing ever suspends.
func (ps *pipelineSource) pendingFrom() int {
	for i := len(ps.bounded) - 1; i >= 0; i-- {
		if bo := ps.bounded[i]; bo != nil && bo.HasPendingOutput() {
			return i
		}
	}
	return -1
}

// runFrom pushes b through ops[i:] and returns what comes out the end. An
// operator that suspends keeps its remainder; the next Next resumes it.
func (ps *pipelineSource) runFrom(ctx context.Context, i int, b *batch.RecordBatch) (*batch.RecordBatch, error) {
	for ; i < len(ps.ops); i++ {
		op := ps.ops[i]
		exec.FlattenForConsumer(b, op)
		var err error
		b, err = op.Execute(ctx, b)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return nil, nil
		}
	}
	return b, nil
}

// Close is nil-receiver and nil-source safe. Wrappers assign their
// pipelineSource in Init and delegate their own Close to it, so a source
// closed without ever being initialized arrives here as a nil receiver —
// which used to be a segfault, i.e. the whole server (#510). Every Close in
// the teardown path has to be reachable from a half-built plan.
func (ps *pipelineSource) Close() error {
	if ps == nil || ps.source == nil {
		return nil
	}
	err := ps.source.Close()
	for _, op := range ps.ops {
		if e := op.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (p *Planner) buildFilter(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("filter has no child")
	}

	source, ops, sink, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	// Collect outer table aliases and columns for correlated subquery detection
	outerTables := collectTableAliases(node.Children[0])
	outerCols := collectOuterColumns(node.Children[0])

	// Scan-level filter pushdown: when the filter sits directly on a
	// catalog scan, eligible conjuncts move into the scan (dictionary-mask
	// evaluation, no materialization of filter-only columns) and only the
	// residue compiles into exec filter ops. See scan_filter_pushdown.go.
	preds := node.Predicates
	if css, ok := source.(*catalogScanSource); ok && len(ops) == 0 &&
		!node.PolicyFilter && !subtreeHasSecurityBarrier(node.Children[0]) {
		// NOT below a security projection. Scan-level pushdown evaluates the
		// predicate against the FILE, so pushing one that sits ABOVE a
		// barrier makes it read the STORED column — the in-process twin of
		// the DAG's single filter slot. `IN (SELECT id FROM t WHERE ssn =
		// '***')` compared the stored SSN against the mask and answered no
		// rows; `… WHERE bal > 300` over a masked `bal` answered exactly the
		// rows above the threshold (#859 round 3).
		//
		// The POLICY's own filter is exempt for the reason it always is: it
		// is supposed to read the row as stored, and it sits BELOW the
		// barrier, so pushing it into the scan is the same evaluation.
		preds = p.tryPushFilterIntoScan(ctx, node, css)
	}

	for _, pred := range preds {
		filter, err := p.buildFilterOp(pred, outerTables, outerCols)
		if err != nil {
			return nil, nil, nil, err
		}
		if filter != nil {
			ops = append(ops, filter)
		}
	}

	return source, ops, sink, nil
}

func (p *Planner) buildProject(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("project has no child")
	}

	child := node.Children[0]

	// A star sharing its SELECT list with other items — `SELECT t.*, ctid` is
	// how DataGrip opens a table — reaches the planner as a projection of the
	// literal column "*". logical.Optimize expands it (before column pruning,
	// which is what #315 turned on); this catches the unoptimized-plan case.
	p.expandStarProjections(ctx, node, child)
	if err := refuseUnexpandedStarBesideItems(node); err != nil {
		return nil, nil, nil, err
	}

	// If the child (or child chain through Filter/HAVING) leads to an Aggregate,
	// skip the projection when possible — the aggregate already produces correctly
	// named output columns (group-by cols + agg output cols).
	// Keep the projection when:
	//   1. Any non-aggregate projection has a complex AST expression (e.g., SUM(x) * 0.0001)
	//   2. Any projection renames a column via alias (e.g., l_suppkey AS supplier_no)
	if hasAggregateAncestor(child) {
		// A group key is decided against the AGGREGATE'S INPUT, which is
		// where a ROW column and its fields live — the aggregate's own
		// output carries neither.
		var elideKeyDecls colDecls
		if agg := findAggregateAncestor(child); agg != nil && len(agg.Children) == 1 {
			elideKeyDecls = inputColDecls(agg.Children[0])
		}
		needsProject := false
		for _, proj := range node.Projections {
			// A literal select item is NOT elidable: the aggregate's output
			// carries it as a synthetic __gb_expr_N key column, and only the
			// projection renames it to the select-list name.
			if proj.ASTExpr != nil && !proj.IsAgg && !isPlainGroupKey(proj.ASTExpr, elideKeyDecls) {
				needsProject = true
				break
			}
			// Aggregate projection with a wrapping scalar function
			// e.g., format_bytes(SUM(rx_bytes)) — the outer function must be
			// applied as a post-aggregate projection.
			if proj.IsAgg && proj.ASTExpr != nil {
				if fn, ok := proj.ASTExpr.(*plansql.FuncCallNode); ok {
					if !plansql.IsAggregate(fn.Name) {
						needsProject = true
						break
					}
				}
				if _, ok := proj.ASTExpr.(*plansql.BinaryOp); ok {
					needsProject = true
					break
				}
			}
			// Check for column rename on non-aggregate columns: alias differs
			// from source column/expression (aggregate columns already use
			// the alias as their OutputCol, so no rename needed).
			if !proj.IsAgg && proj.Alias != "" {
				src := proj.Column
				if src == "" {
					src = proj.Expr
				}
				if proj.Alias != src {
					needsProject = true
					break
				}
			}
		}
		if !needsProject {
			// Every check above asks whether a projection would COMPUTE
			// anything; none asks whether the aggregate's output is the
			// answer's SHAPE. It routinely is not. A HAVING over an
			// aggregate the SELECT list does not carry adds a synthetic
			// `__having_N` output column (logical/builder.go), and
			// `GROUP BY a, b` with only `a` selected adds `b` — eliding
			// the projection published both to the client, which is how
			// `SELECT k FROM g GROUP BY k HAVING BOOL_OR(flag)` answered
			// with a `__having_0` column nobody asked for (#591), and how
			// a grouped-but-unselected key reached psql. A projection over
			// an aggregate is elidable only when the aggregate already
			// emits exactly the projected columns, in order; anything this
			// cannot determine (grouping sets, an unrecognized node below)
			// keeps the projection, which is always sound and merely costs
			// a copy.
			// …and not across a node that ADDS a column. aggregateOutputNames
			// answers what the AGGREGATE publishes, which is what the #575 slot
			// pinning needs; a WINDOW below this Project appends `__win_N` to
			// that, so a projection whose list happens to equal the aggregate's
			// outputs is NOT the node's whole output and eliding it would put
			// the window's slot on the wire. Sort and LIMIT add nothing and are
			// safe to look through.
			if names, ok := aggregateOutputNames(child); ok &&
				!wrapsAWindow(child) && namesMatchProjections(names, node.Projections) {
				return p.buildPipeline(ctx, child)
			}
		}
	}

	source, ops, sink, err := p.buildPipeline(ctx, child)
	if err != nil {
		return nil, nil, nil, err
	}

	aggNode := findAggregateAncestor(child)
	isOverAggregate := aggNode != nil

	// Which column a SELECT item that IS a derived GROUP BY key reads.
	// `SUBSTR(c_phone, 1, 2)` is computed below the aggregate and published
	// under one name; above the aggregate its source columns are gone, so
	// re-evaluating the expression there answers NULL for every row.
	//
	// The lookup is by plansql.ExprIdentity, not by rendered text. The two
	// spellings of one key differ in ways SQL does not distinguish —
	// `(g + 1)` against `g + 1`, `G + 1` against `g + 1` — and comparing the
	// renderings made which spelling was used decide whether the query
	// answered or came back with a NULL key column (#723).
	gbExprToSyn := groupKeyByIdentity(aggNode)

	// Catalog types of what feeds these projections, resolved once for the
	// whole list: a bare column reference inside a projection expression
	// decides its type from them (see nodeDeclaredType, #333). The second
	// map is for a SELECT expression that maps to a synthetic group column —
	// a rename of a value computed BELOW the aggregate, so it types against
	// the aggregate's input rather than its output.
	childColTypes := emittedColDecls(child)
	// A SELECT-list scalar subquery types against its OWN plan, not against
	// this projection's input columns (#874).
	childColTypes.subqueryDecl = p.subqueryOutputColumn
	var aggInputColTypes colDecls
	if isOverAggregate && len(aggNode.Children) > 0 {
		aggInputColTypes = inputColDecls(aggNode.Children[0])
		aggInputColTypes.subqueryDecl = p.subqueryOutputColumn

	}

	// When the aggregate below emits two output columns of one NAME, a
	// name-based DirectCopy resolves both projections to the FIRST such
	// column and collapses them to one value (#575). The fix pins each such
	// projection to the physical slot its TRUE PROVENANCE names, so
	// appearance order is never assumed to equal slot order — it does not
	// when an aggregate shares its alias with a group key (`SELECT COUNT(*)
	// AS k, k AS x GROUP BY k`) or when the select list orders aggregates
	// and keys differently from the aggregate's [keys…, aggs…] output.
	//
	// keySlotByName / aggSlotByName carry the ABSOLUTE indices of the
	// duplicated names in the child's output, split by class: a group-key
	// projection consumes key slots, an aggregate projection consumes
	// aggregate slots. Only built when the child is a clean
	// [group keys…, aggregates…] aggregate output (no grouping sets, no
	// elided-literal reordering); anything else leaves every reference on
	// the existing name path.
	keySlotByName := map[string][]int{}
	aggSlotByName := map[string][]int{}
	if isOverAggregate && aggNode != nil {
		// The EMITTED names, not the planner's spelling of them: the whole
		// point of this map is to find a name TWO columns of the operator's
		// output batch answer to, and a key the planner spells `x.a` is a
		// column the operator calls `a` (#968).
		if full, ok := aggregateEmittedOutputNames(child); ok {
			nAgg := len(aggNode.AggExprs)
			nKey := len(full) - nAgg
			clean := nKey >= 0
			for j := 0; clean && j < nAgg; j++ {
				if !strings.EqualFold(strings.TrimSpace(full[nKey+j]),
					strings.TrimSpace(aggNode.AggExprs[j].OutputCol)) {
					clean = false
				}
			}
			if clean {
				total := map[string]int{}
				for _, n := range full {
					total[strings.ToLower(strings.TrimSpace(n))]++
				}
				for i, n := range full {
					key := strings.ToLower(strings.TrimSpace(n))
					if total[key] < 2 {
						continue // unambiguous by name; leave it on the name path
					}
					if i < nKey {
						keySlotByName[key] = append(keySlotByName[key], i)
					} else {
						aggSlotByName[key] = append(aggSlotByName[key], i)
					}
				}
			}
		}
	}
	keySlotSeen := map[string]int{}
	aggSlotSeen := map[string]int{}

	var projCols []exec.ProjectColumn
	for _, proj := range node.Projections {
		colRef := proj.Column
		if colRef == "" {
			colRef = cleanExpr(proj.Expr)
		}
		name := proj.Alias
		if name == "" {
			name = colRef // use unqualified column name
		}
		// When projecting over an aggregate, aggregate columns should reference
		// their output column name (the alias), not the raw expression.
		if isOverAggregate && proj.IsAgg && proj.Alias != "" {
			colRef = proj.Alias
		}

		// Try to compile from AST expression first, fall back to ColumnRef
		var expression exec.Expression

		// When projecting over an aggregate, check if this SELECT expression
		// matches a GROUP BY expression that was pre-computed into a synthetic
		// column. If so, use a ColumnRef to the synthetic column instead of
		// re-evaluating the expression (the original columns are gone).
		var synSource string
		if isOverAggregate && proj.ASTExpr != nil && !proj.IsAgg {
			if synName, ok := gbExprToSyn[plansql.ExprIdentity(proj.ASTExpr)]; ok {
				expression = exec.ColumnRef(synName)
				synSource = synName
			}
		}

		// Handle aggregate projections with wrapping scalar functions,
		// e.g., format_bytes(SUM(rx_bytes)). Replace the inner aggregate
		// AST node with a ColRef to the aggregate output column, then
		// compile the modified AST as a scalar expression.
		var compiledExpr expr.Expr
		if expression == nil && proj.IsAgg && proj.ASTExpr != nil && isOverAggregate {
			innerAgg := plansql.FindNestedAggregate(proj.ASTExpr)
			if innerAgg != nil {
				outerFn, isFunc := proj.ASTExpr.(*plansql.FuncCallNode)
				if isFunc && !plansql.IsAggregate(outerFn.Name) {
					// Build the aggregate output column name
					aggOutputCol := strings.ToLower(innerAgg.Name) + "("
					if innerAgg.Distinct {
						aggOutputCol += "distinct "
					}
					if innerAgg.Star {
						aggOutputCol += "*"
					} else if len(innerAgg.Args) > 0 {
						var argStrs []string
						for _, a := range innerAgg.Args {
							argStrs = append(argStrs, a.String())
						}
						aggOutputCol += strings.Join(argStrs, ", ")
					}
					aggOutputCol += ")"
					// Replace inner aggregate with a column reference in the AST
					rewritten := replaceAggWithColRef(proj.ASTExpr, innerAgg, aggOutputCol)
					compiled, compErr := expr.CompileWithRunner(rewritten, p.subqueryRunner, p.subqueryBudgetOption())
					if expr.IsCompileRefusal(compErr) {
						return nil, nil, nil, compErr
					}
					if compErr == nil {
						expression = wrapExpr(compiled)
						compiledExpr = compiled
					}
				}
			}
		}

		// An expression OVER a group key — `(g + 1) * 2` — is not the key, so
		// nothing above resolves it as one, and the aggregate's output does
		// not carry `g` to rebuild it from. Re-point its group-key SUBTERMS
		// at the columns the aggregate publishes, which is what the DAG's
		// requoteAggOutputRefs does for the same shape; without it the whole
		// item evaluated to NULL for every row (#723).
		astExpr := proj.ASTExpr
		if isOverAggregate && astExpr != nil && !proj.IsAgg {
			astExpr = plansql.ReplaceGroupKeyRefs(astExpr, gbExprToSyn)
		}

		if expression == nil && astExpr != nil && !proj.IsAgg {
			// CSE within a single Project operator is unsafe: prevCol below
			// is the OUTPUT column name of an earlier projection, but at
			// runtime each ColumnRef is resolved against the INPUT batch's
			// schema — which doesn't yet have the earlier output column.
			// Pointing the duplicate at prevCol resolves to NULL at every
			// row (e.g. `SELECT 1 AS n, 0 AS a, 1 AS b` produced
			// {n: 1, a: 0, b: NULL} because the second `1` literal mapped
			// to ColumnRef("n") and n wasn't in the input — regression
			// surfaced by TestRecursiveCTE_Fibonacci).
			//
			// Safe CSE for SELECT-list duplicates would require either
			// (a) materialising shared expressions as a synthetic column
			// the Project then references, or (b) compiling each
			// projection independently. (b) is what we do — recompiling
			// a literal or already-compiled expression is cheap.
			outerTables := collectTableAliases(child)
			outerCols := collectOuterColumns(child)
			var compiled expr.Expr
			var compErr error
			if len(outerTables) > 0 {
				if len(outerCols) > 0 {
					compiled, compErr = expr.CompileWithScopeResolver(astExpr, p.subqueryRunner, outerTables, outerCols, p.subqueryInnerColumns(), p.subqueryDeclOption(), p.subqueryBudgetOption())
				} else {
					compiled, compErr = expr.CompileWithScope(astExpr, p.subqueryRunner, outerTables, p.subqueryDeclOption(), p.subqueryBudgetOption())
				}
			} else {
				// With the child's DECLARED column types in hand, so a pair
				// that cannot be exact fixed-point — a FLOAT column against a
				// fractional literal — keeps the vectorized float node it has
				// always compiled to instead of deferring the question to the
				// first batch (#555 review).
				compiled, compErr = expr.CompileWithColumnTypes(
					astExpr, p.subqueryRunner, childColTypes.types, p.subqueryBudgetOption())
			}
			// A name nothing implements has no input column to fall back to,
			// so the direct-copy path below would only re-report it as a
			// missing column. Propagate instead (#341).
			if expr.IsCompileRefusal(compErr) {
				return nil, nil, nil, compErr
			}
			if compErr == nil {
				expression = wrapExpr(compiled)
				compiledExpr = compiled
			}
		}
		isDirectCopy := expression == nil
		if expression == nil {
			expression = exec.ColumnRef(colRef)
		}

		// Infer output type: TypeString is the default, resolved at runtime from
		// input schema when column names match. For arithmetic expressions that
		// won't match an input column (e.g., nested aggregate rewrites like
		// __agg_0 * 0.0001), use TypeFloat64.
		outDecl := expr.Decl(parquet.TypeString)
		if proj.ASTExpr != nil && !proj.IsAgg {
			// A select expression mapped to a synthetic group column is a
			// RENAME of a value computed BELOW the aggregate — type it
			// against the aggregate's input, or the declared Float64
			// coerces the pre-projected int64 keys on the copy (#297).
			strictInt := strictIntArithCols(child)
			colTypes := childColTypes
			// The RESPELLED expression is the one that gets evaluated, so it
			// is the one to type. `(c_dec + 1) * 2` over `GROUP BY c_dec + 1`
			// names `c_dec` — a column the aggregate's output does not carry
			// — so typing the original left the DECIMAL key unresolved and
			// the whole term fell to the float rule, which is a scale-0
			// vector over exact fixed point (ADR-0024 item 2). Typing the
			// respelled form reads the key's own declared type instead.
			typeExpr := astExpr
			if isOverAggregate {
				if _, ok := gbExprToSyn[plansql.ExprIdentity(proj.ASTExpr)]; ok {
					// The WHOLE item is a key: a rename of a value computed
					// BELOW the aggregate, so it types against the
					// aggregate's input or the declared Float64 coerces the
					// pre-projected int64 keys on the copy (#297).
					strictInt = strictIntArithCols(aggNode.Children[0])
					colTypes = aggInputColTypes
					typeExpr = proj.ASTExpr
				}
			}
			outDecl = inferProjectionDeclType(typeExpr, outDecl.ID, strictInt, colTypes)
		}
		outType := outDecl.ID
		// The planner's declaration is the AUTHORITY for this projection's
		// arithmetic mode, and the compiled tree is told it here rather than
		// deriving its own. Two walks over two representations of one
		// expression is how a float came to be computed under an INT64
		// declaration and TRUNCATED into the vector (round-1 review, B3);
		// expr.StampArithMode is the seam that makes it one decision.
		if compiledExpr != nil {
			expr.StampArithMode(compiledExpr, outType == parquet.TypeInt64)
		}

		pc := exec.ProjectColumn{
			Name: name,
			Type: outType, // Will be resolved at runtime if input column matches
			Expr: expression,
			// A computed DECIMAL's (p,s): the output column exists in no
			// input schema, so exec.Project has nothing to read the scale
			// off and a scale-0 vector reads every value back a hundredfold
			// out (ADR-0024 item 2; #529, #555).
			Precision: outDecl.Precision,
			Scale:     outDecl.Scale,
		}
		// VECTOR-returning functions (embed()) need their output dimension
		// carried so the runtime sizes the output vector. Resolve it from the
		// registry at plan time (embed() derives it from the live provider).
		if outType == parquet.TypeVector {
			if fc, ok := proj.ASTExpr.(*plansql.FuncCallNode); ok {
				if dim, ok := expr.DefaultRegistry.VecReturnDim(fc.Name); ok {
					pc.Dimension = dim
				}
			}
		}
		// For column renames (e.g., l_suppkey AS supplier_no), record the
		// source column so Project.Execute can resolve the correct type.
		if name != colRef {
			pc.SourceCol = colRef
		}
		// A QUALIFIED reference names ONE SIDE, and the DECLARATION has to be
		// read off the column the VALUE comes from.
		//
		// `proj.Column` is the BARE name — the parser records `z.d92` as the
		// column `d92` — so where both join sides carry that name the value
		// was resolved through `z.d92` (the compiled expression keeps the
		// qualifier) and the type through the first bare `d92`, which is the
		// OTHER arm's. Over two tables holding `d92` at (9,2) and (18,4) that
		// rendered one arm's digits at the other arm's scale, silently, and
		// raised 22003 in the direction where the value does not fit —
		// `numeric field overflow: 1.1111 does not fit a DECIMAL at scale 2`
		// on a query PostgreSQL answers (#706).
		//
		// This is strictly more precise rather than a different rule:
		// `columnIndexFallback` resolves a qualified name with the same
		// ladder the value path uses — exact, then bare, then the
		// unambiguous suffix — so a stream that carries only the bare name
		// still resolves. Per-side resolution is what #551 gave set-op arms
		// and #653 gave filters; this is the projection's half of it.
		if cr, isRef := bareColRefOf(proj.ASTExpr); isRef && cr.Table != "" && !proj.IsAgg {
			pc.SourceCol = cr.Table + "." + cr.Column
		}
		// A ROW FIELD PATH records the WHOLE path, qualifier included, even
		// when the output name matches the field name. It is the only
		// spelling exec.Project can resolve the field's declaration from —
		// colRef here has already lost the `rw.` through cleanExpr — and it
		// is what carries the shape a bare TypeID cannot: a DECIMAL field's
		// (p,s) and a nested ROW/ARRAY/MAP field's own structure, which
		// colRefDeclaredType declines for the same reason it declines them
		// for a column (#568).
		if fp, ok := fieldPathRef(proj.ASTExpr, childColTypes); ok {
			pc.SourceCol = fp
		}
		// A SELECT item that maps to a synthetic GROUP BY key column reads
		// that column, so it is the source exec.Project must type from. The
		// planner's own declaration cannot carry a parameterized type
		// (colRefDeclaredType declines DECIMAL and the containers), which
		// left `SELECT rw.d ... GROUP BY rw.d` declaring STRING over a
		// DECIMAL key the aggregate had already emitted correctly (#568).
		if synSource != "" {
			pc.SourceCol = synSource
		}
		// Tell the runtime this output is computed, so it does not type the
		// output vector from an input column that merely shares the alias
		// (#327). A bare column reference — the only projection whose value
		// really does come from a same-named input — is excluded.
		pc.Computed = isComputedProjection(proj.ASTExpr)
		// For simple column references (no computed expression), use bulk vector
		// copy instead of per-row evaluation.
		if isDirectCopy {
			pc.DirectCopy = colRef
		}
		// Use vectorized column evaluation when the expression supports it.
		// VecExpr handles any output type (string, numeric, etc.) and is checked
		// before the Float64-specific paths.
		if compiledExpr != nil {
			if ve, ok := compiledExpr.(expr.VecExpr); ok {
				evalVec := ve.EvalVec
				pc.VecEval = func(b *batch.RecordBatch, out *batch.Vector, n int) {
					evalVec(b, out, n)
				}
			}
			// Exact fixed-point arithmetic into a DECIMAL output: the one
			// kernel that writes DecimalData, so it is the one projection
			// that may skip exec.Project's checked per-row box (ADR-0024
			// item 3, #555). Gated on the DECLARED type, because a node whose
			// runtime mode turns out not to be decimal writes nothing.
			if outType == parquet.TypeDecimal {
				if dv, ok := compiledExpr.(expr.DecimalVecExpr); ok {
					pc.VecDecimalEval = dv.EvalDecimalVec
				}
			}
		}
		// Use typed evaluation to avoid interface{} boxing in the inner loop.
		// Only safe when the output type is explicitly Float64 (arithmetic exprs),
		// not when resolved from input schema (could be Decimal, Timestamp, etc.).
		if compiledExpr != nil && outType == parquet.TypeFloat64 {
			if ve, ok := compiledExpr.(expr.VecFloat64Expr); ok {
				pc.VecFloat64Eval = ve.EvalFloat64Vec
				if binop, ok := ve.(*expr.BinOpFloat64); ok {
					pc.VecFloat64Clone = func() exec.VecFloat64Expression {
						return binop.CloneVec().EvalFloat64Vec
					}
				}
			}
			if fe, ok := compiledExpr.(expr.Float64Expr); ok {
				pc.Float64Eval = fe.EvalFloat64
			} else if ie, ok := compiledExpr.(expr.Int64Expr); ok {
				pc.Int64Eval = ie.EvalInt64
			}
		}
		// A plain direct copy of a DUPLICATED output column: pin it to the
		// next physical slot of its PROVENANCE class — aggregate projections
		// take aggregate slots, group-key references take key slots — so two
		// projections reading `u` read the two distinct `u` columns and an
		// aggregate never reads the group-key column it happens to share a
		// name with (#575). SourceIdx is exact and beats the name path.
		if isDirectCopy {
			key := strings.ToLower(strings.TrimSpace(colRef))
			seen, slots := keySlotSeen, keySlotByName
			if proj.IsAgg {
				seen, slots = aggSlotSeen, aggSlotByName
			}
			if idxs := slots[key]; len(idxs) > 0 {
				if k := seen[key]; k < len(idxs) {
					pc.SourceIdx = idxs[k]
					pc.SourceIdxSet = true
					seen[key] = k + 1
				}
			}
		}
		projCols = append(projCols, pc)
	}

	if len(projCols) > 0 {
		ops = append(ops, exec.NewProject(projCols))
	}

	return source, ops, sink, nil
}

func (p *Planner) buildAggregate(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("aggregate has no child")
	}

	// Bare COUNT(*) over a plain scan answers from the catalog manifest —
	// no scan pipeline at all (see metadata_count.go). Un-grouped MIN/MAX
	// (optionally alongside COUNT(*)) answers the same way from parquet
	// footer statistics (see metadata_minmax.go).
	if src, ok := p.tryBuildMetadataCount(ctx, node); ok {
		return src, nil, &exec.CollectSink{}, nil
	}
	if src, ok := p.tryBuildMetadataMinMax(ctx, node); ok {
		return src, nil, &exec.CollectSink{}, nil
	}

	childSource, childOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	// Detect aggregate inputs that are expressions (not simple column refs).
	// For each, compile the expression and add a pre-aggregate projection
	// that evaluates it into a synthetic column.
	// CSE: deduplicate identical expressions by their string representation.
	var preProjectCols []exec.ProjectColumn
	var preProjectMeta []parquet.Column
	syntheticNames := make(map[int]string) // agg index → synthetic column name
	exprDedup := make(map[string]string)   // expr string → synthetic column name
	// synDecl is the DECLARATION each synthetic column is materialized under —
	// the same triple the stage DAG carries as AggSpec.InputType/Precision/
	// Scale. The aggregate's OUTPUT declaration is read off it below, so this
	// path and the DAG declare the same thing for the same query, identity row
	// included (#685; see aggOutputFromInputDecl).
	synDecl := make(map[string]parquet.Column)

	// The declarations of what feeds the aggregate, resolved once: they are
	// what tells a ROW FIELD PATH apart from a table-qualified column, and a
	// field path is NOT a simple column reference however much it looks like
	// one. exec.HashAggregate resolves its inputs by NAME through
	// columnIndexFallback, which has no ROW arm, so `MIN(rw.n)` failed with
	// `aggregate input "n" is not a column of its input` — cleanExpr having
	// dropped the `rw.` on the way. Routing it through the synthetic
	// pre-projection below is what materializes the field as a real column,
	// at its declared type (#568).
	//
	// emittedColDecls, not inputColDecls: the walk has to cross a DERIVED
	// TABLE. TPC-H Q08 is `SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0
	// END)` over `(SELECT …, l_extendedprice * (1 - l_discount) AS volume …)`,
	// and inputColTypes stops at that subquery's Project — so `volume`
	// decided nothing, the CASE declared its integer ELSE, and the branch's
	// DECIMAL text met an INT64 vector at the #361 store guard. It is the
	// same decline #529 hit one site over, where the SELECT list already
	// resolves through emittedColDecls (see TestDecimalDecidesThroughParens
	// AndDerivedTables), and the same walk declaredOutputSchema uses — so the
	// aggregate's input, the SELECT list and the plan-declared schema now
	// answer from one map.
	aggInputDecls := emittedColDecls(node.Children[0])

	for i, agg := range node.AggExprs {
		if agg.InputExpr != nil && (!isSimpleColRef(agg.InputExpr) || astIsFieldPath(agg.InputExpr, aggInputDecls)) {
			exprStr := agg.InputExpr.String()
			if existing, ok := exprDedup[exprStr]; ok {
				// Reuse previously compiled expression
				syntheticNames[i] = existing
				continue
			}
			synName := SlotName(SlotAggInput, i)
			// WITH THE OUTER SCOPE, exactly as the SELECT-list projection
			// site compiles its own expressions (see the CompileWith*
			// ladder above). Without it this site asked for none, so a
			// correlated subquery in an AGGREGATE ARGUMENT was compiled as
			// UNCORRELATED and run ONCE against no outer row: `SUM(CASE WHEN
			// EXISTS (SELECT 1 FROM y WHERE y.id = x.id * 2) THEN 1 ELSE 0
			// END)` read a query-wide constant FALSE and answered 0 for
			// PostgreSQL's 4, in silence, until v0.18.16 made the dangling
			// re-run loud (#734, ADR-0021 §1c). The identical expression one
			// level down — in a derived table's SELECT list — has always
			// answered, because that site does ask.
			aggOuterTables := collectTableAliases(node.Children[0])
			aggOuterCols := collectOuterColumns(node.Children[0])
			var compiled expr.Expr
			var compErr error
			if len(aggOuterTables) > 0 {
				compiled, compErr = expr.CompileWithScopeResolver(agg.InputExpr, p.subqueryRunner,
					aggOuterTables, aggOuterCols, p.subqueryInnerColumns(),
					p.subqueryDeclOption(), p.subqueryBudgetOption())
			} else {
				compiled, compErr = expr.CompileWithRunner(agg.InputExpr, p.subqueryRunner,
					p.subqueryDeclOption(), p.subqueryBudgetOption())
			}
			if expr.IsCompileRefusal(compErr) {
				return nil, nil, nil, compErr
			}
			if compErr == nil {
				aggDecl := inferProjectionDeclType(agg.InputExpr, parquet.TypeFloat64, nil, aggInputDecls)
				pc := exec.ProjectColumn{
					Name: synName,
					// Aggregate inputs are usually numeric, so Float64 is the
					// fallback — but MAX(UPPER(c)) is not, and declaring it
					// Float64 handed vecUpper a vector with no BytesData to
					// write into: the same process-killing mismatch as the
					// projection path (#310). MAX(COALESCE(a, b)) needs the
					// input's column types on top of that, or the polymorphic
					// declaration falls back to the same wrong Float64 (#333),
					// and a DECIMAL needs its (p,s) with the TypeID or the
					// materialized vector truncates at scale 0 (ADR-0024
					// item 2).
					Type:      aggDecl.ID,
					Precision: aggDecl.Precision,
					Scale:     aggDecl.Scale,
					Expr:      wrapExpr(compiled),
				}
				// Use general vectorized evaluation when available.
				if ve, ok := compiled.(expr.VecExpr); ok {
					evalVec := ve.EvalVec
					pc.VecEval = func(b *batch.RecordBatch, out *batch.Vector, n int) {
						evalVec(b, out, n)
					}
				}
				// Use vectorized float64 evaluation when available (entire column at once),
				// falling back to typed per-row eval.
				//
				// Gated on the DECLARED type, the way the SELECT-list
				// projection gates its own typed paths (buildProjectOp only
				// attaches them when outType is Float64). aggPreProject picks
				// its write route from WHICH eval is set, not from the
				// column, so a float writer on a non-float column writes into
				// a nil Float64Data — reached the moment a ROW field path
				// started taking this route, since a bare column reference
				// implements every one of these interfaces (#568).
				//
				// The gate applies to EVERY derived aggregate input, not only
				// to field paths, and that is deliberate: the mismatch it
				// prevents was always possible here — any expression whose
				// declared type is not Float64 while its compiled form
				// implements VecFloat64Expr had the same nil-slice write
				// available to it — and the two sites now state the same
				// rule. Nothing that was reaching a typed path with a
				// matching declared type loses it.
				if pc.Type == parquet.TypeFloat64 {
					if ve, ok := compiled.(expr.VecFloat64Expr); ok {
						pc.VecFloat64Eval = ve.EvalFloat64Vec
						if binop, ok := ve.(*expr.BinOpFloat64); ok {
							pc.VecFloat64Clone = func() exec.VecFloat64Expression {
								return binop.CloneVec().EvalFloat64Vec
							}
						}
					}
					if fe, ok := compiled.(expr.Float64Expr); ok {
						pc.Float64Eval = fe.EvalFloat64
					}
				}
				if pc.Type == parquet.TypeInt64 {
					if ie, ok := compiled.(expr.Int64Expr); ok {
						pc.Int64Eval = ie.EvalInt64
					}
				}
				// Exact fixed-point arithmetic into a DECIMAL aggregate
				// input, gated on the DECLARED type exactly like the paths
				// above and like the SELECT-list projection builder.
				//
				// The kernel existed and was reachable — BinOpNumeric
				// implements DecimalVecExpr and writes carriers straight
				// into out.DecimalData.Data — and it was attached in
				// exactly ONE place in the tree, the SELECT-list builder.
				// Every DECIMAL aggregate input therefore took the boxed
				// checked writer: 4.00 allocations per computed cell, at
				// SF1 48,026,572 for Q01's two computed columns over
				// 6,001,215 rows, 1804x the FLOAT64 arm's object count.
				// The mechanism is one round trip — Int128 →
				// FormatDecimal → FormatUint → any box →
				// SetComputedChecked → DecimalTextParts re-parse — so the
				// value is rendered to decimal TEXT and parsed back
				// between two exact kernels (#705). The BYTE ratio is a
				// different thing and is NOT a defect: the Int128 carrier
				// is 16 B against float64's 8, which is ADR-0024's
				// predicted cost.
				if pc.Type == parquet.TypeDecimal {
					if dv, ok := compiled.(expr.DecimalVecExpr); ok {
						pc.VecDecimalEval = dv.EvalDecimalVec
					}
				}
				// A ROW FIELD PATH declares the FIELD, wholesale: its (p,s),
				// its dimension, its nested shape. colRefDeclaredType above
				// declines every parameterized type — it can only answer a
				// TypeID — so MIN over a DECIMAL field fell back to the
				// Float64 default and a container field to Float64 outright
				// (#568).
				meta := parquet.Column{Name: synName, Type: pc.Type, Nullable: true,
					Precision: pc.Precision, Scale: pc.Scale}
				if fc, ok := aggInputDecls.field(fieldPathColRef(agg.InputExpr)); ok {
					meta = fc
					meta.Name, meta.Nullable = synName, true
					pc.Type = fc.Type
					pc.Dimension = fc.Dimension
					// The boxed route is the only null-correct one for a
					// field path: aggPreProject picks its writer from WHICH
					// eval is set, and the typed writers have no way to mark
					// a NULL field — MIN over a float field read the 0 they
					// leave behind instead of skipping the row.
					pc.VecEval, pc.VecFloat64Eval, pc.VecFloat64Clone = nil, nil, nil
					pc.Float64Eval, pc.Int64Eval = nil, nil
				}
				preProjectCols = append(preProjectCols, pc)
				preProjectMeta = append(preProjectMeta, meta)
				syntheticNames[i] = synName
				exprDedup[exprStr] = synName
				synDecl[synName] = meta
			}
		}
	}

	var aggCols []exec.AggColumn
	for i, agg := range node.AggExprs {
		fn := parseAggFunc(agg.Func)
		if agg.Distinct && fn == exec.AggCount {
			fn = exec.AggCountDistinct
		}
		// Preserve the table qualifier — NormalizeIdentRef only strips the
		// quotes off a delimited identifier, the way the GROUP BY keys below
		// are normalized and the way the stage-DAG AggSpec carries
		// agg.InputCol verbatim. cleanExpr here would drop the qualifier
		// ("t2.c2" -> "c2"), and a bare name binds to the FIRST column of
		// that name in the input schema. Over a join whose two sides share a
		// bare column name, that is the wrong table's column: BOOL_OR(t2.c2)
		// over `t5, t1 FULL OUTER JOIN t2 ON t1.c7` read t5.c2 (all NULL) and
		// answered NULL, and an always-true WHERE that reorders the cross
		// join flipped which wrong column it read (t1.c2, FALSE) — a
		// TLP-Aggregate self-consistency violation (#622). exec.HashAggregate's
		// columnIndexFallback still resolves the qualified name against a
		// scan that emits the column bare via its qualified->bare fallback.
		inputCol := plansql.NormalizeIdentRef(strings.TrimSpace(agg.InputCol))
		// Use synthetic column name if the input was an expression
		if synName, ok := syntheticNames[i]; ok {
			inputCol = synName
		}
		// Prefer the resolved declaration (MIN/MAX carry their input
		// column's type) so this pipeline and the stage DAG declare the
		// same thing for the same query. exec.HashAggregate overrides
		// MIN/MAX from the vector it observes anyway, so this only decides
		// the type of the identity row an empty input produces — which is
		// exactly where the two paths would otherwise disagree.
		outType, outTypeKnown := aggSpecOutputType(node, agg)
		if !outTypeKnown {
			outType = aggOutputType(agg.Func, agg.Distinct)
		}
		// The arguments past the first arrive as their own fields
		// (logical/agg_extra_args.go). They used to be recovered by
		// splitting InputCol on a comma, which only worked if something
		// had packed them there — nothing had, because the SELECT parser
		// dropped every argument after the first (#353).
		ac := exec.AggColumn{
			Func:       fn,
			InputCol:   inputCol,
			InputCol2:  agg.InputCol2,
			InputCol3:  agg.InputCol3,
			Separator:  agg.Separator,
			Percentile: agg.Percentile,
			OutputCol:  agg.OutputCol,
			OutputType: outType,
			// DISTINCT for every aggregate PostgreSQL accepts it for, not
			// only COUNT (#703). COUNT spells it as its own AggFunc above —
			// its state IS the set — so the flag would be redundant there;
			// MIN/MAX are unaffected by de-duplication and carry it only so
			// the two paths declare the same spec.
			Distinct: agg.Distinct && fn != exec.AggCountDistinct,
		}
		// And the (p,s) a bare TypeID cannot carry, for the same reason: it
		// decides what the identity row of an EMPTY input declares, which is
		// the one output no observed vector can type. On the DAG that row is
		// a whole partial task's .wshf file (#685); here it is the zero-row
		// result's schema, and the two paths declare the same thing only if
		// both read this function.
		if m, known := aggSpecOutputDecimal(node, agg); known {
			ac.OutputPrecision, ac.OutputScale = m.Precision, m.Scale
		}
		// A ROW-valued aggregate's FIELDS, which a bare TypeID cannot carry
		// either. Same function the stage spec uses, so the two paths declare
		// one bar (#965).
		if fields, ok := aggOhlcvOutputFields(node, agg); ok {
			ac.OutputFields = fields
		}
		// A COMPUTED argument is declared from the projection this path
		// materializes it under, which is the DAG's rule read off the local
		// equivalent of AggSpec.InputType/Precision/Scale. Without it a
		// zero-row SUM(a * (1 - b)) declared float64 here and DECIMAL there.
		if synName, ok := syntheticNames[i]; ok {
			if d, ok := synDecl[synName]; ok {
				if t, prec, sc, known := aggOutputFromInputDecl(
					agg.Func, agg.Distinct, d.Type, d.Precision, d.Scale,
					aggInputIsWideInteger(agg.InputExpr, aggInputDecls)); known {
					ac.OutputType = t
					ac.OutputPrecision, ac.OutputScale = prec, sc
				}
			}
		}
		aggCols = append(aggCols, ac)
	}

	// Catalog types of the aggregate's input, for typing derived GROUP BY
	// key expressions (see nodeDeclaredType, #333). Resolved once.
	aggChildStrictInt := strictIntArithCols(node.Children[0])
	aggChildColTypes := inputColDecls(node.Children[0])

	groupByCols := make([]string, len(node.GroupBy))
	for i, gb := range node.GroupBy {
		// Preserve table qualifiers for self-join disambiguation (e.g., n1.n_name vs n2.n_name).
		// The aggregate operator resolves qualified names with fallback to unqualified.
		// Delimited identifiers lose their quotes here: the operator matches
		// the batch column name itself (Zeek's flat id.orig_h).
		groupByCols[i] = plansql.NormalizeIdentRef(strings.TrimSpace(gb))
	}

	// Literal group keys (GROUP BY 1, URL — the positional ref resolves to
	// the literal select item) are constant per row: they cannot affect
	// grouping, but as synthetic key columns they widen every serialized
	// key and force the multi-column generic path over the single-column
	// fast paths (ClickBench Q35 vs Q34). Elide them from the key set and
	// re-attach the constant as a post-aggregate column under the same
	// synthetic name the downstream projection expects. Kept out of
	// grouping-sets plans (set indices reference key positions), and only
	// when a non-literal key remains — GROUP BY over literals alone must
	// still emit zero rows on empty input, which one retained key
	// preserves.
	var litPostOps []exec.UnaryOperator
	litElided := map[int]bool{}
	// The names this aggregate publishes its keys under, resolved once. The
	// literal elision below, the derived-key materialization further down,
	// aggregateOutputNames and the projection above all read them from here —
	// one rule, so the two engines' aggregate output schemas cannot drift
	// apart (#723).
	keyOuts := groupKeyOutputs(node)
	if len(node.GroupByExprs) == len(node.GroupBy) && len(node.GroupingSets) == 0 {
		nonLit := 0
		for _, gbExpr := range node.GroupByExprs {
			if gbExpr == nil {
				nonLit++
				continue
			}
			if _, isLit := gbExpr.(*plansql.Lit); !isLit {
				nonLit++
			}
		}
		if nonLit > 0 {
			for i, gbExpr := range node.GroupByExprs {
				if gbExpr == nil {
					continue
				}
				if _, isLit := gbExpr.(*plansql.Lit); !isLit {
					continue
				}
				compiled, compErr := expr.CompileWithRunner(gbExpr, p.subqueryRunner, p.subqueryDeclOption(), p.subqueryBudgetOption())
				if expr.IsCompileRefusal(compErr) {
					return nil, nil, nil, compErr
				}
				if compErr != nil {
					continue
				}
				litDecl := inferProjectionDeclType(gbExpr, parquet.TypeString, aggChildStrictInt, aggChildColTypes)
				litPostOps = append(litPostOps, &aggPreProject{computed: []exec.ProjectColumn{{
					Name:      keyOuts[i].Name,
					Type:      litDecl.ID,
					Precision: litDecl.Precision,
					Scale:     litDecl.Scale,
					Expr:      wrapExpr(compiled),
				}}})
				litElided[i] = true
			}
		}
	}

	// Handle GROUP BY expressions (e.g., SUBSTR(c_phone, 1, 2)).
	// Compile expression-valued GROUP BY entries into pre-aggregate projections
	// so the aggregate can group by the computed result.
	if len(node.GroupByExprs) == len(node.GroupBy) {
		for i, gbExpr := range node.GroupByExprs {
			if litElided[i] {
				continue
			}
			// keyOuts is the single answer to "is this key derived", shared
			// with aggregateOutputNames and with the projection above, so
			// the schema this materialization produces and the schema they
			// describe cannot disagree (ADR-0026).
			if gbExpr != nil && i < len(keyOuts) && keyOuts[i].Derived {
				// The HIDDEN SLOT, not the key's own text. The
				// pre-aggregate projection APPENDS this column to the input
				// batch and every consumer resolves by name, so a slot
				// spelled like an input column the query already carries is
				// shadowed BY it — `GROUP BY g + 1` over a table that also
				// has a column called "g + 1" grouped by the column. The
				// key is PUBLISHED under its canonical text by
				// GroupByOutNames below, which is what keeps the two
				// engines' output schemas equal (#720, ADR-0026).
				synName := keyOuts[i].Slot
				compiled, compErr := expr.CompileWithRunner(gbExpr, p.subqueryRunner, p.subqueryDeclOption(), p.subqueryBudgetOption())
				if expr.IsCompileRefusal(compErr) {
					return nil, nil, nil, compErr
				}
				if compErr == nil {
					gbDecl := derivedGroupKeyDecl(node.GroupBy[i], gbExpr, node.Children[0])
					pc := exec.ProjectColumn{
						Name: synName,
						// Numeric expressions (abs(x), x-1, …) must get a
						// numeric synthetic column: SetValue on a String
						// vector mangles float group keys.
						Type: gbDecl.ID,
						// And a DECIMAL key needs its (p,s) with the type:
						// a scale-0 vector TRUNCATES every value on the way
						// in, so `GROUP BY COALESCE(a, b)` collapsed 12.75
						// and 12.7501 into one group holding 12 (ADR-0024
						// item 2).
						Precision: gbDecl.Precision,
						Scale:     gbDecl.Scale,
						Expr:      wrapExpr(compiled),
					}
					// Batched evaluation when available — beyond the vec
					// kernels themselves, FuncCall.EvalVec is where the
					// per-batch input memo for expensive scalar functions
					// (regexp family, ClickBench Q29's GROUP BY key) lives;
					// the per-row Expr path bypasses it.
					if ve, ok := compiled.(expr.VecExpr); ok {
						pc.VecEval = ve.EvalVec
					}
					// Same rule the aggregate INPUT takes above: a ROW field
					// path declares the whole field, and its value is
					// written through the boxed route so a NULL field stays
					// NULL (#568).
					meta := parquet.Column{Name: synName, Type: pc.Type, Nullable: true,
						Precision: pc.Precision, Scale: pc.Scale}
					if fc, ok := aggChildColTypes.field(fieldPathColRef(gbExpr)); ok {
						meta = fc
						meta.Name, meta.Nullable = synName, true
						pc.Type = fc.Type
						pc.Dimension = fc.Dimension
						pc.VecEval = nil
					}
					preProjectCols = append(preProjectCols, pc)
					preProjectMeta = append(preProjectMeta, meta)
					groupByCols[i] = synName
				}
			}
		}
	}

	// If we have expression inputs or GROUP BY expressions, build a
	// pass-through projection that keeps all input columns and adds
	// the computed ones.
	if len(preProjectCols) > 0 {
		childOps = append(childOps, &aggPreProject{computed: preProjectCols, meta: preProjectMeta})
	}

	// Compact literal-elided entries out of the key set.
	if len(litElided) > 0 {
		kept := groupByCols[:0:0]
		for i, c := range groupByCols {
			if !litElided[i] {
				kept = append(kept, c)
			}
		}
		groupByCols = kept
	}

	hashAgg := exec.NewHashAggregate(groupByCols, aggCols)
	// A DERIVED key resolves by its hidden slot and publishes under its own
	// canonical text. Only then do the two engines' aggregate output schemas
	// match, which is what lets one HAVING predicate — rewritten once, in the
	// logical plan — be evaluable on both (ADR-0026). Set only when a key
	// really is derived, so every other shape keeps exec's own naming rule
	// (the qualifier strip, and the ambiguity exception for `GROUP BY
	// n1.n_name, n2.n_name`).
	if outNames, derived := publishedGroupKeyNames(keyOuts, litElided); derived {
		hashAgg.GroupByOutNames = outNames
	}
	if est := findScanRowEstimate(node.Children[0]); est > 0 {
		hashAgg.InputRowHint = est
	}
	if ndv := groupKeyNDVEstimate(node.Children[0], groupByCols); ndv > 0 {
		hashAgg.GroupNDVHint = ndv
	}
	if sm := p.getSpillManager(); sm != nil {
		hashAgg.Spill = sm
	}

	// For GROUPING SETS: single-pass mode — convert the sets' terms to key
	// POSITIONS.
	//
	// A term is looked up against `node.GroupBy`, the key list as the query
	// wrote it, and NOT against `groupByCols`, which is what the aggregate
	// actually groups on: a DERIVED key's entry there is its hidden
	// `__gb_expr_N` slot (ADR-0026 §2), which no grouping set can be spelled
	// with. Keyed on the materialized name, `ROLLUP (g + 1)` found nothing,
	// produced an EMPTY set, and every set collapsed to the grand total.
	var postOps []exec.UnaryOperator
	if len(node.GroupingSets) > 0 || len(node.GroupingCalls) > 0 {
		keyIndex := make(map[string]int, len(node.GroupBy))
		for i, c := range node.GroupBy {
			// FIRST wins. The key list is deduped upstream, so a repeat should
			// not arrive — but last-wins is the wrong reading if one ever does,
			// and it is what pointed both sets of `ROLLUP (g, g)` at position 1
			// and left position 0 grouped on nothing.
			if _, taken := keyIndex[strings.ToLower(strings.TrimSpace(c))]; !taken {
				keyIndex[strings.ToLower(strings.TrimSpace(c))] = i
			}
		}
		for i, c := range groupByCols {
			// The materialized spelling too, so a set written against a name
			// the pre-projection did not move still resolves.
			if _, taken := keyIndex[strings.ToLower(c)]; !taken {
				keyIndex[strings.ToLower(c)] = i
			}
		}
		if len(node.GroupingSets) > 0 {
			sets := make([][]int, len(node.GroupingSets))
			for i, set := range node.GroupingSets {
				indices := make([]int, 0, len(set))
				for _, col := range set {
					if idx, ok := keyIndex[strings.ToLower(strings.TrimSpace(col))]; ok {
						indices = append(indices, idx)
					}
				}
				sets[i] = indices
			}
			hashAgg.GroupingSets = sets
		}

		// GROUPING(a[, b, ...]): the same name→key-position map, but the
		// ARGUMENT ORDER is preserved and an unresolvable argument is an
		// error rather than a silently dropped bit. Dropping one would shift
		// every bit below it and answer a different number (#804); the
		// grouping-set loop above can drop a term because a set is a SET.
		for _, call := range node.GroupingCalls {
			positions := make([]int, 0, len(call.Args))
			for _, arg := range call.Args {
				idx, ok := keyIndex[strings.ToLower(strings.TrimSpace(arg))]
				if !ok {
					return nil, nil, nil, sqlerr.New("42803",
						"arguments to GROUPING must be grouping expressions of the associated query level: %q is not a group key of this aggregate", arg)
				}
				positions = append(positions, idx)
			}
			hashAgg.GroupingCalls = append(hashAgg.GroupingCalls, positions)
			hashAgg.GroupingCallNames = append(hashAgg.GroupingCallNames, call.OutputCol)
		}
	}
	if len(node.GroupingSets) == 0 && len(node.GroupingSetNulls) > 0 {
		hashAgg.NullGroupCols = node.GroupingSetNulls
	}

	// Elided literal keys re-attach as constant columns on the aggregate's
	// output, under the synthetic names the projection maps to.
	postOps = append(postOps, litPostOps...)

	// The aggregate acts as both sink and source
	// We need to run childSource -> childOps -> hashAgg(sink), then hashAgg(source) -> collectSink
	return &aggSourceAdapter{
		childSource: childSource,
		childOps:    childOps,
		agg:         hashAgg,
	}, postOps, &exec.CollectSink{}, nil
}

// isSimpleColRef returns true if the AST node is a simple column reference
// (no arithmetic, function calls, etc).
// replaceAggWithColRef returns a copy of the AST with the target aggregate
// function node replaced by a ColRef to the given column name.
func replaceAggWithColRef(node plansql.Node, target *plansql.FuncCallNode, colName string) plansql.Node {
	if node == nil {
		return nil
	}
	if fn, ok := node.(*plansql.FuncCallNode); ok && fn == target {
		return &plansql.ColRef{Column: colName}
	}
	switch n := node.(type) {
	case *plansql.FuncCallNode:
		newArgs := make([]plansql.Node, len(n.Args))
		for i, a := range n.Args {
			newArgs[i] = replaceAggWithColRef(a, target, colName)
		}
		return &plansql.FuncCallNode{Name: n.Name, Args: newArgs, Distinct: n.Distinct, Star: n.Star}
	case *plansql.BinaryOp:
		return &plansql.BinaryOp{
			Left:  replaceAggWithColRef(n.Left, target, colName),
			Op:    n.Op,
			Right: replaceAggWithColRef(n.Right, target, colName),
		}
	case *plansql.ParenNode:
		return &plansql.ParenNode{Inner: replaceAggWithColRef(n.Inner, target, colName)}
	case *plansql.CastNode:
		return &plansql.CastNode{Inner: replaceAggWithColRef(n.Inner, target, colName), TypeName: n.TypeName}
	default:
		return node
	}
}

func (p *Planner) buildSetOp(ctx context.Context, node *logical.Node, op string) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) < 2 {
		return nil, nil, nil, fmt.Errorf("%s requires two children", op)
	}
	// Arms with NO COMMON TYPE are refused HERE, at plan time, with
	// PostgreSQL's 42804 — the same refusal the stage DAG takes, from the same
	// walk, so one query has one answer (#648). Left to the runtime, this path
	// let the arms meet under the FIRST arm's box: `SELECT s FROM t UNION ALL
	// SELECT d FROM t` came back as a STRING column holding rendered decimals,
	// and the same pair the other way round failed mid-execution with 22P02 on
	// the first row of text that is not a number.
	if err := setOpArmTypeConflict(node); err != nil {
		return nil, nil, nil, err
	}

	leftSource, leftOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building %s left side: %w", op, err)
	}

	rightSource, rightOps, _, err := p.buildPipeline(ctx, node.Children[1])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building %s right side: %w", op, err)
	}

	src := &setOpSourceAdapter{
		leftSource:   leftSource,
		leftOps:      leftOps,
		rightSource:  rightSource,
		rightOps:     rightOps,
		all:          node.UnionAll,
		op:           op,
		leftLits:     setOpArmLiterals(node.Children[0]),
		rightLits:    setOpArmLiterals(node.Children[1]),
		leftUnknown:  setOpArmUnknownLits(node.Children[0]),
		rightUnknown: setOpArmUnknownLits(node.Children[1]),
	}

	return src, nil, &exec.CollectSink{}, nil
}

// setOpArmLiterals reads one arm's SELECT list and records, per OUTPUT
// POSITION, the exact DECIMAL a numeric LITERAL there names — the same
// `setOpLitArm` answer the stage DAG builds its arm projection from. A nil
// entry means "not a bare numeric literal", which is every other select item.
//
// PostgreSQL types a numeric constant `numeric` whenever it carries a decimal
// point or an exponent, so `SELECT d FROM t UNION ALL SELECT 1.23456` is a
// numeric union there (#665). The stage DAG resolves that; the single-process
// path built the literal arm's vector from the declared-type layer, which
// still answers float8 for a fractional literal everywhere, and the two paths
// then answered one query two ways: 1234567890123456.78 came back exact from
// the DAG and 1.2345678901234568e+15 from this one, and a join on the union's
// column matched nothing here because a float8 column met a DECIMAL key (#683).
func setOpArmLiterals(arm *logical.Node) []*setOpLitDecimal {
	proj := findOutputProjectionNode(arm)
	if proj == nil || len(proj.Projections) == 0 {
		return nil
	}
	out := make([]*setOpLitDecimal, len(proj.Projections))
	any := false
	for i, pr := range proj.Projections {
		if pr.ASTExpr == nil {
			continue
		}
		if d, ok := setOpLitArm(pr.ASTExpr); ok {
			lit := d
			out[i] = &lit
			any = true
		}
	}
	if !any {
		return nil
	}
	return out
}

// setOpArmUnknownLits is setOpUnknownLiteralArms for the single-process path,
// whose arms are a NESTED tree rather than the DAG's flattened list: the width
// comes from this arm's own select list, and a nested set-operation arm has no
// output projection of its own, so it contributes no mask (its columns are
// already resolved by its own adapter).
func setOpArmUnknownLits(arm *logical.Node) []bool {
	proj := findOutputProjectionNode(arm)
	if proj == nil {
		return nil
	}
	return setOpUnknownLiteralArms(arm, len(proj.Projections))
}

// setOpApplyLiteralDecls restates a literal arm's column as the DECIMAL its
// SPELLING names, so the arms reconcile through the ordinary ladder.
//
// Applied only when the arm's runtime schema has one column per select item:
// anything else means the pipeline emitted columns this walk did not count,
// and a position is only an address while the two lists line up.
func setOpApplyLiteralDecls(schema []parquet.Column, lits []*setOpLitDecimal) []parquet.Column {
	if len(lits) == 0 || len(lits) != len(schema) {
		return schema
	}
	out := append([]parquet.Column(nil), schema...)
	for i, lit := range lits {
		if lit == nil {
			continue
		}
		out[i].Type = parquet.TypeDecimal
		out[i].Precision, out[i].Scale = lit.decl.Precision, lit.decl.Scale
	}
	return out
}

// setOpLiteralRows replaces a literal column's boxed value with the literal's
// plain decimal TEXT, on every row.
//
// The text is not decoration: the evaluator folds a numeric literal into a
// float64 box, so `1234567890123456.78` is already 1234567890123456.8 by the
// time it reaches this adapter and declaring DECIMAL over that box would put
// an exact type on a rounded number. A DECIMAL arrives here as its rendered
// text anyway (Vector.GetValue), so the literal's own text is the shape every
// reader below already expects — batch.FromRowsChecked parses it at the
// resolved scale and setOpCheckedDecimalText range-checks it, which is what
// gives this path the same 22003 the stage DAG raises for a literal the
// union's own type cannot hold (ADR-0024 item 7).
func setOpLiteralRows(rows []map[string]any, lits []*setOpLitDecimal) []map[string]any {
	if len(lits) == 0 {
		return rows
	}
	for i, lit := range lits {
		if lit == nil {
			continue
		}
		slot := setOpSlotName(i)
		for _, row := range rows {
			if _, ok := row[slot]; ok {
				row[slot] = lit.text
			}
		}
	}
	return rows
}

// setOpSourceAdapter executes both child pipelines and applies the set operation
// (union, intersect, or except) to produce the result.
type setOpSourceAdapter struct {
	leftSource  exec.Source
	leftOps     []exec.UnaryOperator
	rightSource exec.Source
	rightOps    []exec.UnaryOperator
	all         bool
	op          string // "union", "intersect", "except"
	// leftLits / rightLits carry the exact DECIMAL a numeric LITERAL select
	// item names, per output position; nil where the arm has none. See
	// setOpArmLiterals.
	leftLits  []*setOpLitDecimal
	rightLits []*setOpLitDecimal
	// leftUnknown / rightUnknown mark, per output position, the select items
	// that are UNKNOWN-typed literals — a quoted string or a bare NULL, which
	// PostgreSQL types from the OTHER arm. See setOpResolveUnknownLiteralArms.
	leftUnknown  []bool
	rightUnknown []bool

	batches     []*batch.RecordBatch
	idx         int
	initialized bool
}

func (u *setOpSourceAdapter) Init(_ context.Context) error { return nil }

func (u *setOpSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !u.initialized {
		u.initialized = true

		// Run left pipeline
		leftSink := &exec.CollectSink{}
		leftPipe := &exec.Pipeline{
			Source: u.leftSource,
			Ops:    u.leftOps,
			Sink:   leftSink,
		}
		if err := leftPipe.Run(ctx); err != nil {
			return nil, fmt.Errorf("executing %s left side: %w", u.op, err)
		}

		// Run right pipeline
		rightSink := &exec.CollectSink{}
		rightPipe := &exec.Pipeline{
			Source: u.rightSource,
			Ops:    u.rightOps,
			Sink:   rightSink,
		}
		if err := rightPipe.Run(ctx); err != nil {
			return nil, fmt.Errorf("executing %s right side: %w", u.op, err)
		}

		// SQL says the arms of a set operation correspond BY POSITION and
		// the result takes the FIRST arm's column names. These rows are
		// keyed maps, so an arm whose columns are spelled differently has
		// to be re-keyed before anything compares or concatenates them —
		// `SELECT n_regionkey FROM nation UNION SELECT r_regionkey FROM
		// region` deduped nothing (every row of one arm was a distinct map
		// from every row of the other) and batch.FromRows then read the
		// right arm's values under names it does not carry and wrote NULLs.
		//
		// Schema() instead of Batches()[0].Schema — ToRows below releases the
		// sinks' batches as it boxes them — and the arms' two schemas
		// UNIFIED rather than the first one alone. Under the first arm's
		// schema the arm ORDER decided the answer: FromRows re-reads each
		// row's rendered decimal text at the schema's scale, so the first
		// arm's scale truncated the second arm's values (#532); an INTEGER
		// arm was read raw as an unscaled carrier (#547); and a DECIMAL arm
		// under a FLOAT64 first arm failed the store outright while the same
		// pair the other way round silently kept the DECIMAL type (#541).
		// unifySetOpSchemas resolves the common type through the same
		// setOpWiden / setOpDecimalTarget the stage DAG uses, so the two
		// paths cannot answer with different types for the same query.
		//
		// The type is resolved HERE rather than at the FromRows call below
		// because the DEDUP KEY needs it too: a set operation decides
		// membership by equality, so two values the comparator calls equal
		// have to produce one key — which their BOXES alone cannot say, a
		// DECIMAL being rendered text (#499).
		leftSchema := setOpApplyLiteralDecls(leftSink.Schema(), u.leftLits)
		rightSchema := setOpApplyLiteralDecls(rightSink.Schema(), u.rightLits)
		leftSchema, rightSchema = setOpResolveUnknownLiteralArms(
			leftSchema, rightSchema, u.leftUnknown, u.rightUnknown)
		schema := unifySetOpSchemas(leftSchema, rightSchema)

		// The boxes are not uniform across types — a DECIMAL is its rendered
		// TEXT, an integer a raw int64, a float a float64 — so a widened
		// column needs each arm's box MOVED into the shape the unified column
		// reads, not merely relabelled. coerceSetOpArmRows does that for every
		// rung of the ladder before the arms meet, so both the dedup key and
		// FromRows read one shape per column — and ERRORS on a value that does
		// not fit the unified DECIMAL, the same overflow the stage DAG raises
		// (exec.coerceDecimalVector), rather than saturating silently. The
		// right arm is coerced against its OWN schema, before alignSetOpRows
		// re-keys it to the result names.
		//
		// POSITIONALLY, from here to the batch. SQL says the arms of a set
		// operation correspond by POSITION and a result may legally carry two
		// output columns of the same NAME — `SELECT n_name AS u, n_comment AS
		// u FROM nation UNION ALL …` is two columns called `u` in PostgreSQL
		// too. A map keyed by name holds ONE of them, so both output columns
		// came back carrying the SECOND source column's value: every row
		// wrong, no error, and only on this path — the stage DAG answers it
		// correctly, which is what isolated the collapse as the cause (#556,
		// and #844's UNION ALL branch, which is the same map).
		//
		// The rows keep their map form — every helper below reads it, and the
		// DECIMAL, dedup and overflow rules those helpers encode are not what
		// is wrong here — but their KEYS become slot positions, which are
		// addresses. The schemas are renamed to match for the duration and
		// the result batch is renamed back at the end, so nothing outside
		// this function sees a slot name.
		posResult := setOpSlotSchema(schema)
		leftRows, err := coerceSetOpArmRows(
			setOpLiteralRows(setOpArmRows(leftSink, leftSchema), u.leftLits),
			setOpSlotSchema(leftSchema), posResult)
		if err != nil {
			return nil, fmt.Errorf("executing %s left side: %w", u.op, err)
		}
		rightRows, err := coerceSetOpArmRows(
			setOpLiteralRows(setOpArmRows(rightSink, rightSchema), u.rightLits),
			setOpSlotSchema(rightSchema), posResult)
		if err != nil {
			return nil, fmt.Errorf("executing %s right side: %w", u.op, err)
		}

		keyer := newSetOpKeyer(posResult)

		var resultRows []map[string]any

		switch u.op {
		case "intersect":
			resultRows = intersectRows(keyer, leftRows, rightRows, u.all)
		case "except":
			resultRows = exceptRows(keyer, leftRows, rightRows, u.all)
		default: // "union"
			resultRows = append(leftRows, rightRows...)
			if !u.all {
				resultRows = deduplicateRows(keyer, resultRows)
			}
		}

		if len(resultRows) > 0 {
			if schema != nil {
				// FromRowsChecked, not FromRows: this is where the operation's
				// VALUES are materialized, and the unchecked writer answered a
				// DECIMAL with no carrier at the unified scale with the
				// SATURATED end of the Int128 range — a DECIMAL(38,0) arm's
				// 10^30 came back as 17014118346046923173168730371.5884105727
				// under a DECIMAL(38,10) union, silently (#553). ADR-0024
				// item 4: at a value-producing site, no exact carrier is a
				// 22003 error, never the nearest storable number.
				b, err := batch.FromRowsChecked(posResult, resultRows)
				if err != nil {
					return nil, fmt.Errorf("building the %s result: %w", u.op, err)
				}
				// Back to the names the query publishes. The slots were an
				// internal addressing scheme for the rows above; the result
				// takes the FIRST arm's column names, duplicates included.
				for i := range b.Schema {
					if i < len(schema) {
						b.Schema[i].Name = schema[i].Name
					}
				}
				u.batches = []*batch.RecordBatch{b}
			}
		}
	}

	if u.idx >= len(u.batches) {
		return nil, nil
	}
	b := u.batches[u.idx]
	u.idx++
	return b, nil
}

func (u *setOpSourceAdapter) Close() error {
	err := u.leftSource.Close()
	if e := u.rightSource.Close(); e != nil && err == nil {
		err = e
	}
	for _, op := range u.leftOps {
		if e := op.Close(); e != nil && err == nil {
			err = e
		}
	}
	for _, op := range u.rightOps {
		if e := op.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (u *setOpSourceAdapter) RowsScanned() int64 {
	var total int64
	if sp, ok := u.leftSource.(exec.ScanStatsProvider); ok {
		total += sp.RowsScanned()
	}
	if sp, ok := u.rightSource.(exec.ScanStatsProvider); ok {
		total += sp.RowsScanned()
	}
	return total
}

// intersectRows returns rows that appear in both left and right.
// If all is true, preserves duplicate counts (min of left/right occurrences).
func intersectRows(k *setOpKeyer, left, right []map[string]any, all bool) []map[string]any {
	rightSet := make(map[string]int, len(right))
	for _, row := range right {
		rightSet[k.key(row)]++
	}

	if all {
		result := make([]map[string]any, 0)
		for _, row := range left {
			key := k.key(row)
			if rightSet[key] > 0 {
				result = append(result, row)
				rightSet[key]--
			}
		}
		return result
	}

	// INTERSECT (distinct): deduplicate, then keep only rows in both
	seen := make(map[string]struct{}, len(left))
	result := make([]map[string]any, 0)
	for _, row := range left {
		key := k.key(row)
		if _, already := seen[key]; already {
			continue
		}
		seen[key] = struct{}{}
		if rightSet[key] > 0 {
			result = append(result, row)
		}
	}
	return result
}

// exceptRows returns rows from left that do not appear in right.
// If all is true, each right occurrence removes one left occurrence.
func exceptRows(k *setOpKeyer, left, right []map[string]any, all bool) []map[string]any {
	rightSet := make(map[string]int, len(right))
	for _, row := range right {
		rightSet[k.key(row)]++
	}

	if all {
		result := make([]map[string]any, 0)
		for _, row := range left {
			key := k.key(row)
			if rightSet[key] > 0 {
				rightSet[key]--
			} else {
				result = append(result, row)
			}
		}
		return result
	}

	// EXCEPT (distinct): deduplicate left, exclude rows in right
	seen := make(map[string]struct{}, len(left))
	result := make([]map[string]any, 0)
	for _, row := range left {
		key := k.key(row)
		if _, already := seen[key]; already {
			continue
		}
		seen[key] = struct{}{}
		if rightSet[key] == 0 {
			result = append(result, row)
		}
	}
	return result
}

// alignSetOpRows re-keys one set-operation arm's rows onto the column names
// of the arm that decides the result schema (the first one). Arms correspond
// by POSITION in SQL, but these rows are name-keyed maps, so an arm selecting
// differently-spelled columns is invisible to rowHashKey and to
// batch.FromRows unless its keys are rewritten first.
//
// Returns rows unchanged when the schemas already agree, when either is
// unknown (an arm that produced nothing has no schema), or when the widths
// differ — a width mismatch is a malformed set operation, not something to
// paper over here.
// setOpSlotName is the internal address of one output column of a set
// operation. It is in the reserved hidden-slot namespace, so no query can
// spell it and it cannot collide with a column of either arm.
func setOpSlotName(i int) string { return "__setop_" + strconv.Itoa(i) }

// setOpSlotSchema is cols with every column renamed to its slot. Types,
// precision and scale are untouched — only the ADDRESS changes.
func setOpSlotSchema(cols []parquet.Column) []parquet.Column {
	out := make([]parquet.Column, len(cols))
	for i, c := range cols {
		c.Name = setOpSlotName(i)
		out[i] = c
	}
	return out
}

// setOpArmRows boxes one arm's result with its columns keyed by POSITION.
//
// CollectSink.ToRowValues is the positional form and it is non-nil exactly
// when the map form would lose a column — that is, when two of the arm's
// output columns share a name, which is the shape this exists for. When it is
// nil the map IS the positional form and the arm's own schema order supplies
// the addresses.
func setOpArmRows(sink *exec.CollectSink, schema []parquet.Column) []map[string]any {
	if vals := sink.ToRowValues(); vals != nil {
		out := make([]map[string]any, len(vals))
		for i, cells := range vals {
			m := make(map[string]any, len(schema))
			for j := range schema {
				if j < len(cells) {
					m[setOpSlotName(j)] = cells[j]
				}
			}
			out[i] = m
		}
		return out
	}
	rows := sink.ToRows()
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		m := make(map[string]any, len(schema))
		for j, c := range schema {
			m[setOpSlotName(j)] = row[c.Name]
		}
		out[i] = m
	}
	return out
}

// alignSetOpRows re-keys an arm's rows to the result's column names.
//
// It is no longer reached from the set-operation adapter, which addresses its
// arms by POSITION (setOpArmRows) and therefore needs no re-keying at all.
// Kept for the other caller and because it states the rule the slots enforce:
// the arms correspond by position, and the result takes the first arm's names.
func alignSetOpRows(want, have []parquet.Column, rows []map[string]any) []map[string]any {
	if len(want) == 0 || len(want) != len(have) {
		return rows
	}
	aligned := false
	for i := range want {
		if want[i].Name != have[i].Name {
			aligned = true
			break
		}
	}
	if !aligned {
		return rows
	}
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		re := make(map[string]any, len(want))
		for j := range want {
			re[want[j].Name] = row[have[j].Name]
		}
		out[i] = re
	}
	return out
}

// deduplicateRows removes duplicate rows from a slice of row maps.
func deduplicateRows(k *setOpKeyer, rows []map[string]any) []map[string]any {
	seen := make(map[string]struct{}, len(rows))
	result := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		key := k.key(row)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}

// rowHashKey generates a string key from a row's column values, with no types
// to consult: names sorted for determinism, values rendered with %v.
//
// It is the FALLBACK now, for a set operation whose schema cannot type the
// rows — an arm that produced nothing has none. setOpKeyer.key is the typed
// path and is what every schema-carrying set operation uses, because %v alone
// cannot say that a DECIMAL's "12.75" and "12.7500" are one value (#499).
func rowHashKey(row map[string]any) string {
	// Sort keys for deterministic hashing
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	// Simple sort for determinism
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(0)
		}
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(fmt.Sprintf("%v", row[k]))
	}
	return b.String()
}

func (p *Planner) newScanner(ctx context.Context, tableName string, partFilter map[string]string, requiredCols []string, scanPreds []logical.Predicate) exec.Source {
	// Get table schema
	tableMeta, err := p.catalog.GetTable(ctx, tableName)
	if err != nil {
		return &exec.SliceSource{}
	}
	_ = tableMeta

	// Create a scanner source that reads from the catalog
	src := &catalogScanSource{
		catalog:          p.catalog,
		tableName:        tableName,
		partitionFilter:  partFilter,
		requiredCols:     requiredCols,
		scanPreds:        scanPreds,
		manifestSnapshot: p.ManifestSnapshot,
	}
	// Attach scan cache if this table is scanned multiple times in this query.
	if p.scanCache != nil {
		if cached, ok := p.scanCache[tableName]; ok {
			src.cache = cached
		}
	}
	// Wire per-query memory tracker so parquet pooled buffers are accounted for.
	if sm := p.getSpillManager(); sm != nil {
		src.memTracker = sm.Tracker()
		src.spillMgr = sm
	}
	return src
}

// catalogScanSource adapts the scan.Scanner to exec.Source.
//
// Note: Pipeline.runParallel calls Source.Next() concurrently from multiple
// worker goroutines on a single source instance, so replayIdx must be atomic.
// Previously it was a plain int and the race detector caught it producing
// non-deterministic Q02 row counts (4/5/6 rows depending on which goroutine
// won the increment).
type catalogScanSource struct {
	catalog         *catalog.Catalog
	tableName       string
	partitionFilter map[string]string
	requiredCols    []string
	scanPreds       []logical.Predicate
	allowedFiles    []string // probe-split: only scan these files (nil = all)
	inner           exec.Source
	cache           *scanCached      // non-nil when this table is scanned multiple times
	replayIdx       atomic.Int64     // position in cache replay (atomic for parallel pipeline)
	projOnce        sync.Once        // guards projIdx/projSchema init (replay Next is concurrent)
	projIdx         []int            // cache-batch column indices for this consumer; nil = no projection
	projSchema      []parquet.Column // this consumer's projected schema
	isReplay        bool             // true when reading from cache instead of scanning; written once in Init before runParallel starts, so no synchronization needed
	// claimedCache is true when THIS source created cache.ready, i.e. it owes
	// every other consumer a release. Written once in Init, before any worker
	// goroutine exists, for the same reason isReplay is.
	claimedCache     bool
	bloomFilter      *exec.BloomScanFilter // bloom filter pushdown from hash join build side
	dynamicFilter    []exec.DynamicRange   // dynamic min/max range filter from hash join build side
	rowLimit         int64                 // LIMIT pushdown: enables lazy file downloading (0 = eager)
	memTracker       *memory.Tracker       // per-query memory tracker; wired at construction when budget>0
	spillMgr         *memory.SpillManager  // for pre-emptive relief on file-load reservations; nil-safe
	emitRowLoc       bool                  // top-N late materialization: stamp __row_loc on scan batches
	rowPreds         []scan.RowPred        // scan-level filter conjuncts (scan_filter_pushdown.go)
	shapeOnlyCols    map[string]bool       // byte-array columns decoded as lengths only (logical/shape_only_columns.go)
	manifestSnapshot *ManifestSnapshot     // pins this table's manifest to one read per statement (#502); nil-safe
}

// RefetchRows re-reads the full-width rows named by __row_loc values (see
// topn_late_mat.go), in locs order. Only valid on an emitRowLoc scan whose
// narrow phase has completed and whose source has not been closed.
func (s *catalogScanSource) RefetchRows(ctx context.Context, locs []int64) (*batch.RecordBatch, error) {
	ses, ok := s.inner.(*scannerExecSource)
	if !ok || ses.scanner == nil {
		return nil, fmt.Errorf("refetch: scan source is not a row-loc scan")
	}
	return ses.scanner.RefetchRows(ctx, locs)
}

// SetBloomFilter attaches a bloom filter for scan-level row group pruning.
func (s *catalogScanSource) SetBloomFilter(bf *exec.BloomScanFilter) {
	s.bloomFilter = bf
}

// SetDynamicFilter attaches a dynamic min/max range filter for row group pruning.
func (s *catalogScanSource) SetDynamicFilter(ranges []exec.DynamicRange) {
	s.dynamicFilter = ranges
}

func (s *catalogScanSource) Init(ctx context.Context) error {
	if s.cache != nil {
		s.cache.mu.Lock()
		if s.cache.done {
			s.cache.mu.Unlock()
			// Scan already complete — replay from cache.
			s.isReplay = true
			s.replayIdx.Store(0)
			return nil
		}
		if s.cache.ready != nil {
			// Another goroutine is populating the cache. Wait for it.
			s.cache.mu.Unlock()
			select {
			case <-s.cache.ready:
			case <-ctx.Done():
				return ctx.Err()
			}
			// The claim is released on EVERY exit, not only on success, so
			// waking up says the claiming scan is FINISHED — not that it
			// filled the cache. Replaying an abandoned cache would answer
			// from a truncated table, so this fails loudly with the reason
			// the claiming scan stopped.
			s.cache.mu.Lock()
			if !s.cache.done {
				err := s.cache.err
				s.cache.mu.Unlock()
				if err == nil {
					err = errors.New("scan ended before the table did")
				}
				return &abandonedClaimError{table: s.tableName, cause: err}
			}
			s.cache.mu.Unlock()
			s.isReplay = true
			s.replayIdx.Store(0)
			return nil
		}
		// First scan claims the cache. Whoever claims it OWES every other
		// consumer a release — see abandonCache.
		s.cache.ready = make(chan struct{})
		s.claimedCache = true
		s.cache.mu.Unlock()
	}
	// First scan (or no cache) — scan from storage. A cache-populating
	// scan reads the UNION of all consumers' columns so the cache can
	// serve every consumer; each consumer (this one included) projects
	// back down to its own columns in Next.
	scanCols := s.requiredCols
	if s.cache != nil {
		scanCols = s.cache.unionCols
	}
	sc := newScannerSource(s.catalog, s.tableName, s.partitionFilter, scanCols, s.scanPreds, s.manifestSnapshot)
	if ses, ok := sc.(*scannerExecSource); ok {
		if s.bloomFilter != nil {
			ses.bloomFilter = s.bloomFilter
		}
		if s.dynamicFilter != nil {
			ses.dynamicFilter = s.dynamicFilter
		}
		if s.allowedFiles != nil {
			ses.allowedFiles = s.allowedFiles
		}
		if s.rowLimit > 0 {
			ses.rowLimit = s.rowLimit
		}
		if s.memTracker != nil {
			ses.memTracker = s.memTracker
			ses.spillMgr = s.spillMgr
		}
		ses.emitRowLoc = s.emitRowLoc
		ses.rowPreds = s.rowPreds
		ses.shapeOnlyCols = s.shapeOnlyCols
	}
	s.inner = sc
	if err := s.inner.Init(ctx); err != nil {
		s.abandonCache(err)
		return err
	}
	return nil
}

// abandonedClaimError is what a waiter gets when the scan that claimed the
// shared cache finished without filling it.
//
// It is never a ROOT CAUSE. The claiming scan stopped because something else
// went wrong — its own read failed, or the query was already being torn down —
// so this error is always downstream of the reason the client actually needs.
// It carries that reason where the claiming scan knew it (Unwrap), and callers
// that hold BOTH this and the real failure prefer the real one; see the probe
// side of buildJoin.
type abandonedClaimError struct {
	table string
	cause error
}

func (e *abandonedClaimError) Error() string {
	return fmt.Sprintf("shared scan of %s did not complete: %v", e.table, e.cause)
}

func (e *abandonedClaimError) Unwrap() error { return e.cause }

// isAbandonedClaim reports whether err is (or wraps) a waiter's abandoned-claim
// failure — that is, whether it is a CONSEQUENCE of some other failure rather
// than a reason of its own.
func isAbandonedClaim(err error) bool {
	var a *abandonedClaimError
	return errors.As(err, &a)
}

// abandonCache releases a claim this source took but will not fill, so the
// consumers waiting on it fail with err instead of blocking forever.
//
// The claim used to be released in exactly one place — Next's end-of-table
// branch — which made every other exit a permanent block: a scan that FAILED,
// or was closed before the end of the table, left `ready` open and every other
// reader of that table's cache waited on a channel nobody would ever close.
// That is the deadlock #616 reports and it is reachable from any plan with two
// readers of one table where the first one fails (measured: two LATERALs over
// the same table, the second carrying a residual the scan cannot compile —
// 6 ms to the error on the single-process arm, an unbounded block on both DAG
// arms).
//
// Releasing is not enough on its own: a waiter that wakes on an abandoned
// claim must NOT replay, because the cache holds only what was read before the
// scan stopped. Init returns the error instead.
func (s *catalogScanSource) abandonCache(err error) {
	if s.cache == nil || !s.claimedCache {
		return
	}
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	s.abandonCacheLocked(err)
}

// abandonCacheLocked is abandonCache for callers already holding cache.mu.
func (s *catalogScanSource) abandonCacheLocked(err error) {
	if s.cache.done || s.cache.abandoned {
		return
	}
	s.cache.abandoned = true
	s.cache.err = err
	if s.cache.ready != nil {
		close(s.cache.ready)
	}
}

func (s *catalogScanSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if s.isReplay {
		// cache.batches is stable (read-only) after cache.done=true.
		// replayIdx is atomic because Pipeline.runParallel may call Next()
		// from multiple worker goroutines on this same source instance.
		idx := s.replayIdx.Add(1) - 1
		if idx >= int64(len(s.cache.batches)) {
			return nil, nil
		}
		cached := s.cache.batches[idx]
		// Return a shallow copy: shared column vectors (read-only), independent Sel.
		// This prevents downstream operators from corrupting cached data via in-place
		// Sel mutation. Sel itself is carried over (slice header copy —
		// downstream filters replace Sel rather than mutating in place):
		// dropping it, as this used to, resurrected delete-marker-filtered
		// rows on replay.
		clone := &batch.RecordBatch{
			Schema:  cached.Schema,
			Columns: make([]*batch.Vector, len(cached.Columns)),
			Len:     cached.Len,
			Sel:     cached.Sel,
		}
		copy(clone.Columns, cached.Columns)
		return s.projectForConsumer(clone), nil
	}

	// When this scan is populating a shared cache, the entire pull-and-cache
	// step must run under cache.mu — otherwise the parallel pipeline races
	// where one worker pulls nil and sets cache.done=true while OTHER workers
	// still hold real batches that they've pulled but not yet appended. Those
	// late workers see done==true and skip the append, silently dropping
	// rows that the second (replay) scanner of this same table needs. This
	// surfaced as Q02's intermittent 4-rows-instead-of-5 result at SF0.01.
	if s.cache != nil {
		s.cache.mu.Lock()
		defer s.cache.mu.Unlock()
		if s.cache.done {
			// Another worker finished the scan while we were waiting on the
			// lock. Tell our caller "no more batches" so they fall through.
			return nil, nil
		}
		b, err := s.inner.Next(ctx)
		if err != nil {
			// This scan owns the claim and is not going to fill the cache.
			s.abandonCacheLocked(err)
			return nil, err
		}
		if b == nil {
			// An ABANDONED claim is already released and its batches are a
			// partial read. Pipeline.runParallel calls Next from every worker
			// on this same source, so "worker A failed, worker B then reached
			// the end of the table" is a real interleaving — and running this
			// branch after it would close an already-closed channel (a panic)
			// and, worse, mark a TRUNCATED cache done for the next waiter to
			// replay as if it were the whole table.
			if s.cache.abandoned {
				return nil, s.cache.err
			}
			s.cache.done = true
			if s.cache.ready != nil {
				close(s.cache.ready)
			}
			return nil, nil
		}
		// Detach from pool so the pipeline's b.Release() is a no-op.
		// Without this, the pool recycles the batch and the scanner
		// overwrites the Vectors that the cache references.
		b.Detach()
		// Cache a shallow copy so the first consumer's operators don't
		// corrupt cached data by setting Sel in-place. Sel is preserved
		// (delete markers arrive from the scan as Sel) — see the replay
		// branch.
		cached := &batch.RecordBatch{
			Schema:  b.Schema,
			Columns: make([]*batch.Vector, len(b.Columns)),
			Len:     b.Len,
			Sel:     b.Sel,
		}
		copy(cached.Columns, b.Columns)
		// NOT charged to the memory tracker, deliberately. The cache's
		// vectors are SHARED with its consumers — hash-join builds
		// Reserve hashBuildBytes for these same vectors, and the scan
		// source charges them transiently in flight. Reserving them
		// again here (tried 2026-07-06) triple-counted the same physical
		// memory: the ledger hit the budget while RSS was fine, every
		// append stalled in ReserveOrForce's relief wait, and the forced
		// build spills turned SF10 Q21 from 1m28s into 8m35s on EC2
		// (CPU profile: 4.76% utilization — pure stall). Honest cache
		// accounting needs the cache to OWN spillable bytes
		// (SpillableBatchCollector, like the CTE cache) — not a second
		// charge for memory the ledger already sees.
		s.cache.batches = append(s.cache.batches, cached)
		return s.projectForConsumer(b), nil
	}

	// No cache: inner.Next() is thread-safe for channel-based scan sources.
	return s.inner.Next(ctx)
}

// projectForConsumer narrows a union-column cache batch down to this
// consumer's RequiredColumns. Shallow: shares vectors, no copies. The
// no-cache path, SELECT-* consumers (empty requiredCols), and batches
// already matching the consumer's set pass through untouched. Also
// defensive: any required column missing from the batch schema (e.g.,
// synthetic columns) disables projection rather than dropping data.
func (s *catalogScanSource) projectForConsumer(b *batch.RecordBatch) *batch.RecordBatch {
	if s.cache == nil || len(s.requiredCols) == 0 || b == nil {
		return b
	}
	s.projOnce.Do(func() {
		if len(s.requiredCols) >= len(b.Schema) {
			return
		}
		want := make(map[string]bool, len(s.requiredCols))
		for _, name := range s.requiredCols {
			want[name] = true
		}
		// Keep BATCH-SCHEMA (table) order, matching what a standalone
		// scan of this node would emit via buildReadSchema — downstream
		// operators may have bound positions against that shape.
		idx := make([]int, 0, len(s.requiredCols))
		schema := make([]parquet.Column, 0, len(s.requiredCols))
		found := 0
		for i, col := range b.Schema {
			if want[col.Name] || batch.NameSetNames(want, col.Name) {
				idx = append(idx, i)
				schema = append(schema, col)
				found++
			}
		}
		if found < len(want) {
			return // some required column missing — pass through unprojected
		}
		s.projIdx = idx
		s.projSchema = schema
	})
	if s.projIdx == nil {
		return b
	}
	nb := &batch.RecordBatch{
		Schema:  s.projSchema,
		Columns: make([]*batch.Vector, len(s.projIdx)),
		Len:     b.Len,
		Sel:     b.Sel,
	}
	for i, ci := range s.projIdx {
		nb.Columns[i] = b.Columns[ci]
	}
	return nb
}

func (s *catalogScanSource) Close() error {
	if s.isReplay {
		return nil
	}
	// A claiming scan torn down before the end of the table (a LIMIT upstream,
	// a cancelled query, an operator that failed) owes the release just as
	// much as one that errored — otherwise every other reader of this table
	// waits on a channel nobody will close.
	s.abandonCache(errors.New("scan closed before the end of the table"))
	if s.inner != nil {
		return s.inner.Close()
	}
	return nil
}

func (s *catalogScanSource) RowsScanned() int64 {
	if s.isReplay {
		var total int64
		for _, b := range s.cache.batches {
			total += int64(b.Len)
		}
		return total
	}
	if sp, ok := s.inner.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
}

// RecordBatch type alias for convenience
type RecordBatch = batch.RecordBatch

// buildFilterOp compiles one predicate into a filter operator. It returns an
// error only for a predicate naming a function that does not exist: every other
// compile failure falls through to the raw-string and column-compare paths
// below, which is what makes those fallbacks useful. An unknown function has
// nothing to fall through TO — the string parser would not recognize it either,
// so the predicate would quietly become nil and the filter would vanish,
// admitting every row (#341).
func (p *Planner) buildFilterOp(pred logical.Predicate, outerTables map[string]bool, outerCols map[string]string) (exec.UnaryOperator, error) {
	// Try to compile from AST expression first (full expression engine)
	if pred.ASTExpr != nil {
		var compiled expr.Expr
		var err error
		if len(outerTables) > 0 {
			if len(outerCols) > 0 {
				compiled, err = expr.CompileWithScopeResolver(pred.ASTExpr, p.subqueryRunner, outerTables, outerCols, p.subqueryInnerColumns(), p.subqueryDeclOption(), p.subqueryBudgetOption())
			} else {
				compiled, err = expr.CompileWithScope(pred.ASTExpr, p.subqueryRunner, outerTables, p.subqueryDeclOption(), p.subqueryBudgetOption())
			}
		} else {
			compiled, err = expr.CompileWithRunner(pred.ASTExpr, p.subqueryRunner, p.subqueryDeclOption(), p.subqueryBudgetOption())
		}
		if expr.IsCompileRefusal(err) {
			return nil, err
		}
		if err == nil {
			// Try to extract vectorized filter for simple comparison patterns.
			// First try full vectorization, then partial (vectorize what we can
			// from AND chains, keep the rest as row-at-a-time predicates).
			if vf := tryVectorizeFilter(compiled); vf != nil {
				return vf, nil
			}
			// The #147 guard for the ROW evaluator, which KernelFilter has
			// had since and this path did not: a predicate naming a column
			// the input does not carry is UNKNOWN on every row, and a WHERE
			// admits only TRUE, so it answers zero rows in silence (#653).
			// Declined for a CORRELATED predicate, whose outer references
			// resolve outside this batch by design.
			check := rowFilterColumnCheck(pred.ASTExpr, outerTables)
			if vf := tryPartialVectorize(compiled, check); vf != nil {
				return vf, nil
			}
			f := exec.NewFilter(wrapPredicate(compiled))
			f.Check = check
			return f, nil
		}
	}

	// Fall back to raw string parsing
	if pred.Raw != "" {
		p := parseSimplePredicate(pred.Raw)
		if p != nil {
			return p, nil
		}
	}

	if pred.Column != "" && pred.Op != "" {
		op := parseCompareOp(pred.Op)
		return exec.NewFilter(exec.ColumnCompareLit(pred.Column, op, pred.Value, pred.ValueText)), nil
	}

	return nil, nil
}

// tryVectorizeFilter inspects a compiled expression tree and returns a vectorized
// filter operator when the pattern is a simple comparison (col op col, col op const)
// or an AND chain of such comparisons. Returns nil for complex expressions.
func tryVectorizeFilter(e expr.Expr) exec.UnaryOperator {
	ops := extractFilterOps(e, false)
	if len(ops) == 0 {
		return nil
	}
	if len(ops) == 1 {
		return ops[0]
	}
	return exec.NewChainFilter(ops)
}

// rowFilterColumnCheck returns the first-batch column-existence check for a
// row-evaluated predicate, or nil where the guard cannot apply: a correlated
// predicate (its outer references resolve outside the batch by design) or one
// carrying a node FilterColumnRefs declines to enumerate.
func rowFilterColumnCheck(ast plansql.Node, outerTables map[string]bool) func(*batch.RecordBatch) error {
	if ast == nil || len(outerTables) > 0 {
		return nil
	}
	refs, ok := expr.FilterColumnRefs(ast)
	if !ok || len(refs) == 0 {
		return nil
	}
	return func(b *batch.RecordBatch) error { return expr.CheckFilterColumns(b, refs) }
}

// tryPartialVectorize handles AND chains where some operands are vectorizable and
// some are not. Vectorized operands run first (narrowing the selection vector),
// followed by row-at-a-time predicates for the rest. This is better than falling
// back entirely to row-at-a-time when any part of an AND chain isn't vectorizable.
func tryPartialVectorize(e expr.Expr, check func(*batch.RecordBatch) error) exec.UnaryOperator {
	parts := flattenAnds(e)
	if len(parts) < 2 {
		return nil // not an AND chain
	}
	var vectorized []exec.UnaryOperator
	var nonVectorized []expr.Expr
	for _, part := range parts {
		ops := extractFilterOps(part, false)
		if ops != nil {
			vectorized = append(vectorized, ops...)
		} else {
			nonVectorized = append(nonVectorized, part)
		}
	}
	if len(vectorized) == 0 {
		return nil
	}
	// Put vectorized filters first to narrow selection, then slow predicates
	allOps := make([]exec.UnaryOperator, 0, len(vectorized)+len(nonVectorized))
	allOps = append(allOps, vectorized...)
	for _, e := range nonVectorized {
		f := exec.NewFilter(wrapPredicate(e))
		// The whole predicate's references, not this conjunct's: every op in
		// the chain reads ONE batch, so its schema answers for all of them.
		f.Check = check
		check = nil // once is enough
		allOps = append(allOps, f)
	}
	if len(allOps) == 1 {
		return allOps[0]
	}
	return exec.NewChainFilter(allOps)
}

// flattenAnds recursively flattens nested AND expressions into a flat list.
func flattenAnds(e expr.Expr) []expr.Expr {
	if and, ok := e.(*expr.And); ok {
		return append(flattenAnds(and.Left), flattenAnds(and.Right)...)
	}
	return []expr.Expr{e}
}

// extractFilterOps recursively extracts vectorizable filter ops from AND combinations.
// kernelFilterWithRowFallback builds a typed kernel filter; for dotted
// column names ("attrs.score") it attaches the compiled comparison as a
// row-at-a-time fallback so ROW-field access works (the kernel resolves
// qualified table refs by stripping the prefix, but cannot reach into ROW
// children — issue #147).
// colColFilterWithRowFallback builds a col-col kernel filter carrying the
// compiled comparison as a row-at-a-time fallback. The kernel requires both
// columns to share a storage type; when they differ (e.g. FLOAT64 <>
// INT32), the fallback evaluates the comparison with SQL numeric coercion
// instead of the kernel indexing the wrong typed slice (issue #375).
func colColFilterWithRowFallback(left, right string, op exec.CompareOp, cmp expr.Expr) *exec.ColColFilter {
	f := exec.NewColColFilter(left, right, op)
	f.RowFallback = wrapPredicate(cmp)
	return f
}

// fieldPathColRef returns node as a *plansql.ColRef when it is one, seeing
// through parentheses — the shape colDecls.field resolves against. A nil
// answer simply resolves to no field.
func fieldPathColRef(node plansql.Node) *plansql.ColRef {
	for {
		switch n := node.(type) {
		case *plansql.ColRef:
			return n
		case *plansql.ParenNode:
			node = n.Inner
		default:
			return nil
		}
	}
}

// nullCheckWithRowFallback and likeFilterWithRowFallback are
// kernelFilterWithRowFallback for the two vectorized filters that had no
// fallback at all. Both resolved a dotted name by stripping the qualifier and
// then, finding nothing, matched NO ROWS silently — so `WHERE rw.f IS NULL`
// and `WHERE rw.s LIKE 'x%'` over a ROW field answered an empty result
// indistinguishable from real data (#568). The comparison filters have had
// this delegation since #147.
func nullCheckWithRowFallback(name string, checkNull bool, e expr.Expr) exec.UnaryOperator {
	f := exec.NewNullCheckFilter(name, checkNull)
	if strings.Contains(name, ".") {
		f.RowFallback = wrapPredicate(e)
	}
	return f
}

func likeFilterWithRowFallback(name, pattern string, negate bool, e expr.Expr) exec.UnaryOperator {
	f := exec.NewLikeFilter(name, pattern, negate)
	if strings.Contains(name, ".") {
		f.RowFallback = wrapPredicate(e)
	}
	return f
}

func kernelFilterWithRowFallback(name string, op exec.CompareOp, lit *expr.Lit, cmp expr.Expr) exec.UnaryOperator {
	if lit.Val == nil {
		// A comparison against a NULL literal is UNKNOWN for every row, so no
		// row qualifies. It cannot be lowered to a value comparison at all:
		// the kernel takes its constant as a box and every typed coercion
		// reads nil as that type's ZERO, which answered `WHERE c_i64 = NULL`
		// with the rows where the column is 0 (#450).
		return exec.NewMatchNothingFilter()
	}
	// The literal's own TEXT travels with its box. A DECIMAL column's kernel
	// converts the text at the column's scale, which is the only way a
	// literal past a float64's ~15-16 significant digits reaches the
	// comparison as the number that was written (#452).
	kf := exec.NewKernelFilterLit(name, op, lit.Val, lit.Text)
	if strings.Contains(name, ".") {
		kf.RowFallback = wrapPredicate(cmp)
	}
	return kf
}

// kernelOrNothing is the `col <op> constant` operator for a constant that did
// not arrive inside an expr.Lit — a BETWEEN bound, or a value parsed out of
// raw predicate text. Same NULL rule as kernelFilterWithRowFallback.
func kernelOrNothing(col string, op exec.CompareOp, val any, text string) exec.UnaryOperator {
	return kernelOrNothingRef(nil, col, op, val, text)
}

// kernelOrNothingRef is kernelOrNothing carrying the reference the constant is
// compared against, so a dotted name can be given the row-at-a-time fallback
// kernelFilterWithRowFallback gives the `col <op> lit` shape. Without it a
// BETWEEN over a ROW field path failed with `filter column "rw.f" does not
// exist in the input schema` — loud, but a query PostgreSQL answers (#568).
//
// Each HALF of a BETWEEN gets its own fallback comparison rather than the
// whole predicate: the two halves are separate operators, and handing both
// the same conjunction would evaluate it twice.
func kernelOrNothingRef(ref *expr.ColRef, col string, op exec.CompareOp, val any, text string) exec.UnaryOperator {
	if val == nil {
		return exec.NewMatchNothingFilter()
	}
	kf := exec.NewKernelFilterLit(col, op, val, text)
	if ref != nil && strings.Contains(col, ".") {
		kf.RowFallback = wrapPredicate(expr.NewCmp(ref, &expr.Lit{Val: val, Text: text}, execToCmpOp(op)))
	}
	return kf
}

// execToCmpOp is cmpToExecOp's inverse, for the sites that lower a comparison
// to a kernel and then have to rebuild the equivalent expression as a
// fallback.
func execToCmpOp(op exec.CompareOp) expr.CmpOp {
	switch op {
	case exec.OpEq:
		return expr.CmpEq
	case exec.OpNe:
		return expr.CmpNe
	case exec.OpLt:
		return expr.CmpLt
	case exec.OpLe:
		return expr.CmpLe
	case exec.OpGt:
		return expr.CmpGt
	default:
		return expr.CmpGe
	}
}

// inFilterForList builds the IN / NOT IN operator for a list of literals,
// applying SQL's NULL rule to the LIST — which is not the same rule as for a
// scalar comparison, and is the one that surprises people:
//
//	`x IN (a, NULL)` is TRUE where x = a and UNKNOWN everywhere else, because
//	TRUE dominates the disjunction. A NULL member therefore drops out; with
//	nothing else left the whole test is UNKNOWN and nothing qualifies.
//
//	`x NOT IN (a, NULL)` is `x <> a AND x <> NULL`, and the second conjunct is
//	UNKNOWN for every row: the result is FALSE or UNKNOWN, never TRUE. A NULL
//	anywhere in a NOT IN list empties the answer (#450).
//
// An empty list with no NULL in it is left alone — that is a different shape
// and the set kernel already answers it.
//
// RESIDUAL (real NOT IN + NULL + over-range literal only): `real NOT IN (1e40,
// NULL)` short-circuits to MatchNothing below on the NULL rule (#450) before any
// literal is examined, so PostgreSQL's 22003 for the over-range 1e40 in the
// real[] cast is not raised — wadjet answers empty. The positive `IN (1e40,
// NULL)` is NOT affected: it keeps the over-range literal, carries the syntactic
// arity of 2 (SetSyntacticLen below), narrows to real[], and raises 22003 like
// PostgreSQL. Surfacing the error on the NOT-IN path would mean checking the
// over-range literal before the MatchNothing short-circuit; left as a documented
// residual (obscure — a NULL in a NOT IN already empties the answer).
func inFilterForList(col string, values []any, texts []string, negate bool) exec.UnaryOperator {
	kept := make([]any, 0, len(values))
	keptTexts := make([]string, 0, len(texts))
	hadNull := false
	for i, v := range values {
		if v == nil {
			hadNull = true
			continue
		}
		kept = append(kept, v)
		if i < len(texts) {
			keptTexts = append(keptTexts, texts[i])
		}
	}
	if hadNull && (negate || len(kept) == 0) {
		return exec.NewMatchNothingFilter()
	}
	inf := exec.NewInFilterLit(col, kept, keptTexts, negate)
	// The comparison WIDTH of a FLOAT32 IN list is decided by the SYNTACTIC
	// element count, not the count that survives the NULL strip above: PostgreSQL
	// casts the whole `{...}` array literal — NULLs included — to real[] whenever
	// there is more than one element, so `real IN (0.1, NULL)` narrows to real
	// and matches, while `real IN (0.1)` widens to double and does not (#549).
	inf.SetSyntacticLen(len(values))
	return inf
}

// negateCmpOp inverts a comparison for an enclosing NOT. Under SQL's
// three-valued logic NOT (a = b) is TRUE exactly where a <> b is TRUE — both
// are UNKNOWN when either side is NULL, and every filter kernel already skips
// NULL rows — so the inverted operator is the whole of the negation for a
// WHERE, which admits only TRUE. The second result is false for an operator
// with no inverse in this set: the signal to leave the predicate to the row
// evaluator rather than lower it wrongly.
func negateCmpOp(op exec.CompareOp) (exec.CompareOp, bool) {
	switch op {
	case exec.OpEq:
		return exec.OpNe, true
	case exec.OpNe:
		return exec.OpEq, true
	case exec.OpLt:
		return exec.OpGe, true
	case exec.OpLe:
		return exec.OpGt, true
	case exec.OpGt:
		return exec.OpLe, true
	case exec.OpGe:
		return exec.OpLt, true
	default:
		return op, false
	}
}

// maybeNegate applies an enclosing NOT to an already-mapped comparison.
func maybeNegate(op exec.CompareOp, neg bool) (exec.CompareOp, bool) {
	if !neg {
		return op, true
	}
	return negateCmpOp(op)
}

// negatedExpr is the expression a lowered operator's row-at-a-time fallback
// must evaluate. That fallback is the ORIGINAL comparison, so under a NOT it
// has to be wrapped: handing it the un-negated node is the same dropped
// negation one layer down (#461).
func negatedExpr(e expr.Expr, neg bool) expr.Expr {
	if !neg {
		return e
	}
	return &expr.Not{Operand: e}
}

// orOfOps unions two extracted operand lists into one OR filter. Nil on
// either side means that side is not vectorizable, and an OR is only as
// vectorizable as both of its arms.
func orOfOps(leftOps, rightOps []exec.UnaryOperator) []exec.UnaryOperator {
	if leftOps == nil || rightOps == nil {
		return nil
	}
	one := func(ops []exec.UnaryOperator) exec.UnaryOperator {
		if len(ops) == 1 {
			return ops[0]
		}
		return exec.NewChainFilter(ops)
	}
	return []exec.UnaryOperator{exec.NewOrFilter(one(leftOps), one(rightOps))}
}

// extractFilterOps recursively extracts vectorizable filter ops from AND
// combinations.
//
// neg carries an enclosing NOT: what comes back is the negation of e. That is
// what the *expr.Not case used to drop — it returned the operand's own
// operators, so `WHERE NOT (k = 131)` was executed as `WHERE k = 131`, the
// complement of the answer, silently and on both engines (#461). Anything
// that cannot be negated returns nil instead, and the caller's residual
// row-at-a-time filter evaluates the NOT itself, where three-valued logic
// survives (expr.Not.EvalBoolNull).
func extractFilterOps(e expr.Expr, neg bool) []exec.UnaryOperator {
	switch v := e.(type) {
	case *expr.Cmp:
		op, ok := maybeNegate(cmpToExecOp(v.Op), neg)
		if !ok {
			return nil
		}
		fb := negatedExpr(v, neg)
		// col op col
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				if strings.Contains(lc.Name, ".") || strings.Contains(rc.Name, ".") {
					// Possible ROW-field access — the col-col kernel can't
					// evaluate it; leave this comparison row-at-a-time.
					return nil
				}
				return []exec.UnaryOperator{colColFilterWithRowFallback(lc.Name, rc.Name, op, fb)}
			}
		}
		// col op const
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if lit, rok := v.Right.(*expr.Lit); rok {
				return []exec.UnaryOperator{kernelFilterWithRowFallback(lc.Name, op, lit, fb)}
			}
		}
		// const op col → flip
		if lit, lok := v.Left.(*expr.Lit); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				return []exec.UnaryOperator{kernelFilterWithRowFallback(rc.Name, flipOp(op), lit, fb)}
			}
		}
	case *expr.CmpNetworkLit:
		// Bare column vs. a string literal compileCmp pre-parsed as an IPv4
		// or MAC address (tryNetworkLit/CmpNetworkLit in expr/compile.go).
		// This case was missing entirely, so every `ipv4_col <op> 'lit'` /
		// `mac_col <op> 'lit'` predicate fell through to nil here and ran
		// row-at-a-time, losing the vectorized kernel a plain *expr.Cmp node
		// got on this exact shape before compileCmp started emitting
		// CmpNetworkLit (measured +43% on 400k rows).
		//
		// v.Col's type isn't known here — extractFilterOps has no schema,
		// same as the *expr.Cmp arm above — so this builds the identical
		// "col op const" kernel filter that arm would have built for the
		// original `col op 'lit'`/`'lit' op col`, from v.Lit (the literal's
		// original text) rather than the pre-parsed ipv4/mac int64s on the
		// node: ResolveFilterKernel (exec/kernel/compare.go) dispatches
		// purely on the column's REAL runtime type, parsing v.Lit itself via
		// parseIPv4ToInt64/parseMACToInt64 for an actual network column and
		// falling to compareFilterString for anything else. That is also
		// why tryNetworkLit does not need to be, and cannot be, restricted
		// to network-typed columns at compile time: a STRING column whose
		// literal happens to parse as an address (`s = '10.1.2.3'`) rides
		// this same case and gets exactly its normal compareFilterString
		// kernel — using the pre-parsed int64s directly here, bypassing
		// that dispatch, would misinterpret a STRING vector as encoded
		// IPv4/MAC int64 data.
		op, ok := maybeNegate(cmpToExecOp(v.Op), neg)
		if !ok {
			return nil
		}
		fb := negatedExpr(v, neg)
		kOp := op
		if v.Flip {
			kOp = flipOp(op)
		}
		return []exec.UnaryOperator{kernelFilterWithRowFallback(v.Col.Name, kOp, &expr.Lit{Val: v.Lit}, fb)}
	case *expr.CmpInt64:
		op, ok := maybeNegate(cmpToExecOp(v.Op), neg)
		if !ok {
			return nil
		}
		fb := negatedExpr(v, neg)
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				if strings.Contains(lc.Name, ".") || strings.Contains(rc.Name, ".") {
					return nil
				}
				return []exec.UnaryOperator{colColFilterWithRowFallback(lc.Name, rc.Name, op, fb)}
			}
			if lit, rok := v.Right.(*expr.Lit); rok {
				return []exec.UnaryOperator{kernelFilterWithRowFallback(lc.Name, op, lit, fb)}
			}
		}
	case *expr.CmpFloat64:
		op, ok := maybeNegate(cmpToExecOp(v.Op), neg)
		if !ok {
			return nil
		}
		fb := negatedExpr(v, neg)
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				if strings.Contains(lc.Name, ".") || strings.Contains(rc.Name, ".") {
					return nil
				}
				return []exec.UnaryOperator{colColFilterWithRowFallback(lc.Name, rc.Name, op, fb)}
			}
			if lit, rok := v.Right.(*expr.Lit); rok {
				return []exec.UnaryOperator{kernelFilterWithRowFallback(lc.Name, op, lit, fb)}
			}
		}
	case *expr.And:
		if neg {
			// De Morgan: NOT (a AND b) is NOT a OR NOT b, which holds in
			// Kleene logic as well as Boolean.
			return orOfOps(extractFilterOps(v.Left, true), extractFilterOps(v.Right, true))
		}
		leftOps := extractFilterOps(v.Left, false)
		if leftOps == nil {
			return nil
		}
		rightOps := extractFilterOps(v.Right, false)
		if rightOps == nil {
			return nil
		}
		return append(leftOps, rightOps...)
	case *expr.Or:
		if neg {
			// De Morgan the other way: NOT (a OR b) is NOT a AND NOT b, and
			// an AND is the chained intersection of the two selections.
			leftOps := extractFilterOps(v.Left, true)
			if leftOps == nil {
				return nil
			}
			rightOps := extractFilterOps(v.Right, true)
			if rightOps == nil {
				return nil
			}
			return append(leftOps, rightOps...)
		}
		return orOfOps(extractFilterOps(v.Left, false), extractFilterOps(v.Right, false))
	case *expr.Between:
		// col BETWEEN low AND high → two kernel filters: col >= low AND col <= high
		// col NOT BETWEEN low AND high → col < low OR col > high
		if col, ok := v.Expr.(*expr.ColRef); ok {
			if lo, lok := v.Low.(*expr.Lit); lok {
				if hi, hok := v.Hi.(*expr.Lit); hok {
					// SQL defines `x NOT BETWEEN a AND b` as `NOT (x BETWEEN
					// a AND b)`, so an enclosing NOT is the same flag.
					// A NULL bound makes its own half UNKNOWN and leaves the
					// other half standing. BETWEEN then admits nothing, and
					// NOT BETWEEN reduces to the surviving comparison —
					// `x NOT BETWEEN NULL AND h` is TRUE exactly where
					// x > h, because a FALSE conjunct makes the conjunction
					// FALSE whatever the UNKNOWN one says (#450).
					if v.Not != neg {
						return []exec.UnaryOperator{exec.NewOrFilter(
							kernelOrNothingRef(col, col.Name, exec.OpLt, lo.Val, lo.Text),
							kernelOrNothingRef(col, col.Name, exec.OpGt, hi.Val, hi.Text),
						)}
					}
					return []exec.UnaryOperator{
						kernelOrNothingRef(col, col.Name, exec.OpGe, lo.Val, lo.Text),
						kernelOrNothingRef(col, col.Name, exec.OpLe, hi.Val, hi.Text),
					}
				}
			}
		}
	case *expr.In:
		// col IN (lit, lit, ...) or col NOT IN (lit, lit, ...)
		if col, ok := v.Expr.(*expr.ColRef); ok {
			values := make([]any, 0, len(v.Values))
			texts := make([]string, 0, len(v.Values))
			for _, val := range v.Values {
				if lit, ok := val.(*expr.Lit); ok {
					values = append(values, lit.Val)
					texts = append(texts, lit.Text)
				} else {
					return nil // non-literal in IN list
				}
			}
			f := inFilterForList(col.Name, values, texts, v.Not != neg)
			if inf, ok := f.(*exec.InFilter); ok && strings.Contains(col.Name, ".") {
				inf.RowFallback = wrapPredicate(negatedExpr(v, neg))
			}
			return []exec.UnaryOperator{f}
		}
	case *expr.Like:
		// col LIKE 'pattern' or col NOT LIKE 'pattern'
		if col, ok := v.Expr.(*expr.ColRef); ok {
			if pat, ok := v.Pattern.(*expr.Lit); ok {
				if pat.Val == nil {
					// `col LIKE NULL` is UNKNOWN for every row, negated or
					// not — there is no pattern to match against (#450).
					return []exec.UnaryOperator{exec.NewMatchNothingFilter()}
				}
				if s, ok := pat.Val.(string); ok {
					return []exec.UnaryOperator{likeFilterWithRowFallback(col.Name, s, v.Not != neg, negatedExpr(v, neg))}
				}
			}
		}
	case *expr.IsNull:
		// col IS NULL / col IS NOT NULL — vectorized null bitmap scan
		if col, ok := v.Operand.(*expr.ColRef); ok {
			return []exec.UnaryOperator{nullCheckWithRowFallback(col.Name, v.Not == neg, negatedExpr(v, neg))}
		}
	case *expr.ColIsNull:
		// Offsets-shape rewrite of `col IS [NOT] NULL` (expr/shape_funcs.go).
		// Same kernel the *expr.IsNull case builds — without this the filter
		// would silently drop to row-at-a-time evaluation.
		return []exec.UnaryOperator{nullCheckWithRowFallback(v.Col.Name, v.Not == neg, negatedExpr(v, neg))}
	case *expr.ColEmptyStr:
		// Offsets-shape rewrite of a column compared against the empty
		// string literal (expr/shape_funcs.go). Reproduces exactly what the
		// *expr.Cmp "col op const" branch built for the pre-rewrite node.
		op := exec.OpEq
		if v.Not != neg {
			op = exec.OpNe
		}
		return []exec.UnaryOperator{kernelFilterWithRowFallback(v.Col.Name, op, &expr.Lit{Val: ""}, negatedExpr(v, neg))}
	case *expr.Not:
		// NOT (expr) — vectorize the NEGATION of the inner expression. This
		// case used to return the inner expression's own operators, which
		// applied the predicate positively and answered the complement (#461).
		return extractFilterOps(v.Operand, !neg)
	}
	return nil
}

func cmpToExecOp(op expr.CmpOp) exec.CompareOp {
	switch op {
	case expr.CmpEq:
		return exec.OpEq
	case expr.CmpNe:
		return exec.OpNe
	case expr.CmpLt:
		return exec.OpLt
	case expr.CmpLe:
		return exec.OpLe
	case expr.CmpGt:
		return exec.OpGt
	case expr.CmpGe:
		return exec.OpGe
	default:
		return exec.OpEq
	}
}

func flipOp(op exec.CompareOp) exec.CompareOp {
	switch op {
	case exec.OpLt:
		return exec.OpGt
	case exec.OpLe:
		return exec.OpGe
	case exec.OpGt:
		return exec.OpLt
	case exec.OpGe:
		return exec.OpLe
	default:
		return op
	}
}

// collectTableAliases recursively collects all table names and aliases from
// scan nodes in a logical plan subtree. Used to provide outer scope context
// for correlated subquery detection.
func collectTableAliases(node *logical.Node) map[string]bool {
	aliases := make(map[string]bool)
	var walk func(n *logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeScan {
			// Derived-table aliases count: this is the outer scope a
			// correlated subquery's references are resolved against (#489).
			for _, name := range n.ScopeNames() {
				aliases[strings.ToLower(name)] = true
			}
		}
		// So does a CTE reference. It records its scope on the SUBTREE ROOT
		// rather than on the scans below (subtreeNamesRelation says why), so
		// a walk that reads only NodeScan never sees it and `WHERE EXISTS
		// (… WHERE t.k = u.did)` over a CTE `u` was not recognized as
		// correlated at all (#535).
		if n.CTEName != "" {
			aliases[strings.ToLower(n.CTEName)] = true
		}
		if n.CTERefAlias != "" {
			aliases[strings.ToLower(n.CTERefAlias)] = true
		}
		for _, child := range n.Children {
			walk(child)
		}
	}
	walk(node)
	return aliases
}

// frontLoadBlooms reorders the operator chain to move bloom filter operators
// whose key columns exist in the source scan schema to the front. In multi-way
// join pipelines, this allows selective bloom filters (e.g., from semi-joins
// with HAVING filters) to eliminate rows before expensive join probes.
func frontLoadBlooms(source exec.Source, ops []exec.UnaryOperator) []exec.UnaryOperator {
	if len(ops) < 3 {
		return ops // need at least bloom+probe+bloom to benefit
	}

	// Get source scan columns
	var scanCols map[string]bool
	switch s := source.(type) {
	case *catalogScanSource:
		scanCols = make(map[string]bool, len(s.requiredCols))
		for _, c := range s.requiredCols {
			scanCols[c] = true
		}
	case *scannerExecSource:
		scanCols = make(map[string]bool, len(s.requiredCols))
		for _, c := range s.requiredCols {
			scanCols[c] = true
		}
	}
	if len(scanCols) == 0 {
		return ops
	}

	// Find bloom filters that can be applied to the source scan (their key
	// columns all exist in the scan schema) and that are NOT already at the
	// front of the pipeline (i.e., there's a non-bloom op before them).
	firstNonBloom := -1
	for i, op := range ops {
		if _, ok := op.(*exec.BloomFilterOp); !ok {
			firstNonBloom = i
			break
		}
	}
	if firstNonBloom < 0 {
		return ops // all ops are blooms (unlikely)
	}

	var front, rest []exec.UnaryOperator
	for i, op := range ops {
		bf, isBF := op.(*exec.BloomFilterOp)
		if isBF && i > firstNonBloom {
			// This bloom is after a non-bloom op — check if it can be front-loaded
			allPresent := true
			for _, key := range bf.KeyColumns() {
				if !scanCols[key] {
					allPresent = false
					break
				}
			}
			if allPresent {
				front = append(front, op)
				continue
			}
		}
		rest = append(rest, op)
	}

	if len(front) == 0 {
		return ops
	}
	return append(front, rest...)
}

// attachBloomToScanSource walks the probe-side source chain to find a
// catalogScanSource and attaches a bloom filter for row-group-level pruning.
func attachBloomToScanSource(source exec.Source, bsf *exec.BloomScanFilter) {
	switch s := source.(type) {
	case *catalogScanSource:
		s.SetBloomFilter(bsf)
	case *pipelineSource:
		attachBloomToScanSource(s.source, bsf)
	}
}

// attachDynamicFilterToScanSource walks the probe-side source chain to find a
// catalogScanSource and attaches a dynamic min/max range filter for row-group pruning.
func attachDynamicFilterToScanSource(source exec.Source, ranges []exec.DynamicRange) {
	switch s := source.(type) {
	case *catalogScanSource:
		s.SetDynamicFilter(ranges)
	case *pipelineSource:
		attachDynamicFilterToScanSource(s.source, ranges)
	}
}

// findScanRowEstimate returns the total row estimate from scan nodes in a subtree.
// Used to pre-allocate hash join arena and index.
// groupKeyNDVEstimate resolves a GROUP-KEY cardinality estimate from the
// scan's merged-HLL column stats: the per-column NDVs' product (single
// column: the NDV itself), capped by the scan row estimate — the true
// group count can exceed neither. Returns 0 (no hint) when any key column
// lacks stats (synthetic __gb_expr keys, expression keys, missing HLL) or
// the input shape hides the scan (joins, subqueries): sizing then falls
// back to organic growth, which is never wrong, just slower.
func groupKeyNDVEstimate(child *logical.Node, groupByCols []string) int64 {
	if len(groupByCols) == 0 {
		return 0
	}
	n := child
	for n != nil && n.Type != logical.NodeScan {
		switch n.Type {
		case logical.NodeFilter, logical.NodeLimit, logical.NodeSort:
			if len(n.Children) != 1 {
				return 0
			}
			n = n.Children[0]
		default:
			return 0
		}
	}
	if n == nil || n.ScanColStats == nil {
		return 0
	}
	est := int64(1)
	for _, c := range groupByCols {
		cs, ok := n.ScanColStats[c]
		if !ok {
			// Stats keys carry catalog casing; group cols may be
			// SQL-normalized.
			for name, v := range n.ScanColStats {
				if strings.EqualFold(name, c) {
					cs, ok = v, true
					break
				}
			}
		}
		if !ok || cs.NDV <= 0 {
			return 0
		}
		// Overflow-safe product; anything past the row estimate is capped
		// below anyway.
		if est > (1<<62)/cs.NDV {
			est = 1 << 62
			break
		}
		est *= cs.NDV
	}
	if n.ScanRowEstimate > 0 && est > n.ScanRowEstimate {
		est = n.ScanRowEstimate
	}
	return est
}

func findScanRowEstimate(node *logical.Node) int64 {
	if node == nil {
		return 0
	}
	if node.Type == logical.NodeScan {
		return node.ScanRowEstimate
	}
	// For aggregates, row count is much smaller than scan (assume 10% or 2M max).
	// At SF100, high-cardinality GROUP BY (e.g. Q17: ~20M l_partkey values)
	// can produce millions of groups; 100K was too low and forced repeated
	// hash table doublings during execution.
	if node.Type == logical.NodeAggregate {
		est := findScanRowEstimate(node.Children[0])
		reduced := est / 10
		if reduced > 2_000_000 {
			reduced = 2_000_000
		}
		if reduced < 1 {
			reduced = 1
		}
		return reduced
	}
	var total int64
	for _, child := range node.Children {
		total += findScanRowEstimate(child)
	}
	return total
}

// extractColumnRefs extracts raw column name references from a string that
// may be a simple column ("l_shipdate"), a qualified column ("n1.n_name"),
// or an expression ("substr(l_shipdate, 1, 4)", "l_extendedprice * (1 - l_discount)").
// Returns the string itself if it's a simple/qualified column name.
func extractColumnRefs(s string) []string {
	// Simple or qualified column name: contains only alphanumerics, underscores, dots
	isSimple := true
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.') {
			isSimple = false
			break
		}
	}
	if isSimple {
		return []string{s}
	}

	// Expression: extract identifier tokens that look like column references.
	// Tokenize by splitting on non-identifier characters, then filter out
	// SQL keywords and numeric literals.
	var refs []string
	seen := make(map[string]bool)
	start := -1
	for i, c := range s {
		isIdent := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '.' || (c >= '0' && c <= '9')
		if isIdent {
			if start == -1 {
				start = i
			}
		} else {
			if start >= 0 {
				tok := s[start:i]
				start = -1
				if isColumnRef(tok) && !seen[tok] {
					refs = append(refs, tok)
					seen[tok] = true
				}
			}
		}
	}
	if start >= 0 {
		tok := s[start:]
		if isColumnRef(tok) && !seen[tok] {
			refs = append(refs, tok)
			seen[tok] = true
		}
	}
	return refs
}

// isColumnRef returns true if a token looks like a column reference:
// not a number, not a SQL keyword, contains at least one underscore or letter.
func isColumnRef(tok string) bool {
	if len(tok) == 0 {
		return false
	}
	// Pure number
	allDigit := true
	for _, c := range tok {
		if c < '0' || c > '9' {
			allDigit = false
			break
		}
	}
	if allDigit {
		return false
	}
	// SQL keywords to skip
	lower := strings.ToLower(tok)
	switch lower {
	case "case", "when", "then", "else", "end", "and", "or", "not", "in",
		"is", "null", "true", "false", "like", "between", "as", "asc", "desc":
		return false
	}
	return true
}

// collectOuterColumns recursively collects a column-name→table mapping from
// scan nodes in a logical plan subtree. Used to resolve unqualified column
// references in correlated subqueries.
func collectOuterColumns(node *logical.Node) map[string]string {
	colMap := make(map[string]string)
	var walk func(n *logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeScan {
			// What the ENCLOSING query calls this scan, which inside a
			// derived table is the derived alias (#489).
			tableID := strings.ToLower(n.OuterTableID())
			for _, col := range n.ScanColumns {
				colMap[strings.ToLower(col)] = tableID
			}
		}
		// A CTE reference's OUTPUT columns answer to the CTE's scope, and
		// those names are the CTE's own — `did`, not the `g` the scan below
		// emits. Read off the subtree root for collectTableAliases' reason
		// (#535).
		if scope := cteScopeID(n); scope != "" {
			for _, col := range cteOutputNames(n) {
				colMap[strings.ToLower(col)] = scope
			}
		}
		for _, child := range n.Children {
			walk(child)
		}
	}
	walk(node)
	return colMap
}

// cteScopeID is the name an enclosing query calls this CTE reference by: the
// reference's own alias where it has one, else the CTE's name.
func cteScopeID(n *logical.Node) string {
	if n == nil {
		return ""
	}
	if n.CTERefAlias != "" {
		return strings.ToLower(n.CTERefAlias)
	}
	return strings.ToLower(n.CTEName)
}

// cteOutputNames lists the column names a CTE subtree PUBLISHES, for the
// shapes a CTE body ends in. It answers only where the answer is exact — a
// Project's aliases and an Aggregate's keys and outputs — and nothing at all
// otherwise, because a wrong name here would attribute an outer column to a
// scope that does not carry it.
func cteOutputNames(n *logical.Node) []string {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeProject:
		out := make([]string, 0, len(n.Projections))
		for _, p := range n.Projections {
			name := p.Alias
			if name == "" {
				name = p.Column
			}
			if name != "" && name != "*" {
				out = append(out, name)
			}
		}
		return out
	case logical.NodeAggregate:
		out := make([]string, 0, len(n.GroupBy)+len(n.AggExprs))
		out = append(out, n.GroupBy...)
		for _, a := range n.AggExprs {
			if a.OutputCol != "" {
				out = append(out, a.OutputCol)
			}
		}
		return out
	case logical.NodeScan:
		return n.ScanColumns
	}
	if len(n.Children) == 1 {
		return cteOutputNames(n.Children[0])
	}
	return nil
}

// subqueryInnerColumns returns a resolver that reports a relation's columns, so
// correlation analysis can bind an unqualified name inside a subquery to the
// subquery's own FROM before considering the outer query — the SQL scoping
// rule. Without it, a name that also exists in the outer scope is claimed by
// the outer scope unless the outer table's identifier happens to be spelled the
// same as an inner table, which turns an ordinary uncorrelated subquery into a
// per-row correlated one (issue #334).
//
// A CTE reference is a relation with a schema exactly as a base table is, so
// the WITH items in scope are part of the resolver and not an exception to it
// (#955). While they were, `WITH c AS (SELECT id, … FROM t) SELECT (SELECT
// MAX(v) FROM c WHERE id < 4000) FROM d` read that `id` as d's, substituted the
// outer row's value into the predicate — making it constant TRUE — and answered
// the unfiltered aggregate on every arm in silence. A derived table needs
// nothing here: it carries its own body, and the classifier reads it.
//
// A relation this cannot name resolves to nil, which leaves the name to the
// identifier-comparison fallback rather than silently declaring it inner.
func (p *Planner) subqueryInnerColumns() plansql.TableColumns {
	return plansql.CTEColumns(p.ctes, p.catalogColumns())
}

// catalogColumns is the base of subqueryInnerColumns' resolver: a relation's
// declared schema, from the catalog.
func (p *Planner) catalogColumns() plansql.TableColumns {
	if p.catalog == nil {
		return nil
	}
	ctx := p.planCtx
	if ctx == nil {
		ctx = context.Background()
	}
	return func(table string) []string {
		t, err := p.catalog.GetTable(ctx, table)
		if err != nil || t == nil {
			return nil
		}
		cols := make([]string, len(t.Schema.Columns))
		for i, c := range t.Schema.Columns {
			cols[i] = c.Name
		}
		return cols
	}
}

func parseSimplePredicate(raw string) exec.UnaryOperator {
	// Parse "column op value" patterns
	operators := []struct {
		sql string
		op  exec.CompareOp
	}{
		{">=", exec.OpGe},
		{"<=", exec.OpLe},
		{"!=", exec.OpNe},
		{">", exec.OpGt},
		{"<", exec.OpLt},
		{"=", exec.OpEq},
	}

	for _, o := range operators {
		parts := strings.SplitN(raw, o.sql, 2)
		if len(parts) == 2 {
			col := cleanExpr(strings.TrimSpace(parts[0]))
			valStr := strings.TrimSpace(parts[1])
			val := parseValue(valStr)
			return kernelOrNothing(col, o.op, val, numericLitText(valStr))
		}
	}

	// LIKE / NOT LIKE
	upper := strings.ToUpper(raw)
	if idx := strings.Index(upper, " NOT LIKE "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		pattern := strings.TrimSpace(raw[idx+len(" NOT LIKE "):])
		pattern = strings.Trim(pattern, "'")
		return exec.NewLikeFilter(col, pattern, true)
	}
	if idx := strings.Index(upper, " LIKE "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		pattern := strings.TrimSpace(raw[idx+len(" LIKE "):])
		pattern = strings.Trim(pattern, "'")
		return exec.NewLikeFilter(col, pattern, false)
	}

	// IS NULL / IS NOT NULL — vectorized null bitmap scan
	if strings.Contains(upper, "IS NOT NULL") {
		col := cleanExpr(strings.TrimSpace(raw[:strings.Index(upper, "IS NOT NULL")]))
		return exec.NewNullCheckFilter(col, false)
	}
	if strings.Contains(upper, "IS NULL") {
		col := cleanExpr(strings.TrimSpace(raw[:strings.Index(upper, "IS NULL")]))
		return exec.NewNullCheckFilter(col, true)
	}

	// BETWEEN: "col between X and Y" → col >= X AND col <= Y
	if idx := strings.Index(upper, " BETWEEN "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		rest := strings.TrimSpace(raw[idx+len(" BETWEEN "):])
		andIdx := strings.Index(strings.ToUpper(rest), " AND ")
		if andIdx >= 0 {
			loStr := strings.TrimSpace(rest[:andIdx])
			hiStr := strings.TrimSpace(rest[andIdx+len(" AND "):])
			lo, hi := parseValue(loStr), parseValue(hiStr)
			return exec.NewChainFilter([]exec.UnaryOperator{
				kernelOrNothing(col, exec.OpGe, lo, numericLitText(loStr)),
				kernelOrNothing(col, exec.OpLe, hi, numericLitText(hiStr)),
			})
		}
	}

	// IN: "col in (v1, v2, v3)" → vectorized set membership
	if idx := strings.Index(upper, " IN "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		rest := strings.TrimSpace(raw[idx+len(" IN "):])
		rest = strings.TrimPrefix(rest, "(")
		rest = strings.TrimSuffix(rest, ")")
		parts := strings.Split(rest, ",")
		values := make([]any, 0, len(parts))
		texts := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			values = append(values, parseValue(part))
			texts = append(texts, numericLitText(part))
		}
		if len(values) > 0 {
			return inFilterForList(col, values, texts, false)
		}
	}

	return nil
}

// numericLitText returns a raw predicate operand's text when it is a plain
// decimal number, and "" otherwise. It is what lets a DECIMAL comparison
// built from raw SQL text keep the digits the float64 box drops (#452);
// exponent forms are deliberately not included, so they keep the box.
func numericLitText(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	i := 0
	if s[0] == '+' || s[0] == '-' {
		i++
	}
	digits, dot := false, false
	for ; i < len(s); i++ {
		switch {
		case s[i] >= '0' && s[i] <= '9':
			digits = true
		case s[i] == '.' && !dot:
			dot = true
		default:
			return ""
		}
	}
	if !digits {
		return ""
	}
	return s
}

func parseValue(s string) any {
	s = strings.TrimSpace(s)
	// Remove quotes
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	// An UNQUOTED null is the NULL literal. Falling through returned the
	// four-character string "null", so `WHERE c = NULL` reaching this path
	// compared the column against that text instead of answering UNKNOWN;
	// the callers turn nil into a match-nothing operator (#450).
	if strings.EqualFold(s, "null") {
		return nil
	}
	// Try integer
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	// Try float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

func parseCompareOp(op string) exec.CompareOp {
	switch op {
	case "=":
		return exec.OpEq
	case "!=", "<>":
		return exec.OpNe
	case "<":
		return exec.OpLt
	case "<=":
		return exec.OpLe
	case ">":
		return exec.OpGt
	case ">=":
		return exec.OpGe
	default:
		return exec.OpEq
	}
}

func parseAggFunc(s string) exec.AggFunc {
	switch strings.ToLower(s) {
	case "sum":
		return exec.AggSum
	case "count":
		return exec.AggCount
	case "min":
		return exec.AggMin
	case "max":
		return exec.AggMax
	case "avg":
		return exec.AggAvg
	case "string_agg":
		return exec.AggStringAgg
	case "bool_and", "every":
		return exec.AggBoolAnd
	case "bool_or":
		return exec.AggBoolOr
	case "stddev", "stddev_samp":
		return exec.AggStddev
	case "variance", "var_samp":
		return exec.AggVariance
	case "stddev_pop":
		return exec.AggStddevPop
	case "var_pop":
		return exec.AggVarPop
	case "approx_distinct":
		return exec.AggApproxDistinct
	case "corr":
		return exec.AggCorr
	case "covar_samp":
		return exec.AggCovarSamp
	case "covar_pop":
		return exec.AggCovarPop
	case "percentile_cont", "quantile_cont":
		return exec.AggPercentileCont
	case "percentile_disc", "quantile_disc":
		return exec.AggPercentileDisc
	case "mode":
		return exec.AggMode
	case "ohlcv":
		return exec.AggOhlcv
	case exec.OhlcvStateFunc:
		return exec.AggOhlcvState
	case exec.OhlcvStateMergeFunc:
		return exec.AggOhlcvStateMerge
	case "min_by":
		return exec.AggMinBy
	case "max_by":
		return exec.AggMaxBy
	case "median":
		return exec.AggMedian
	default:
		return exec.AggCount
	}
}

// parseWindowFunc maps a SQL window function name onto its operator constant.
// A name exec has no window form for still resolves to ROW_NUMBER — the zero
// value — but no plan reaches the operator with one, because refuseUnwindowable
// runs first at every site that builds a window operator.
func parseWindowFunc(s string) exec.WindowFunc {
	fn, _ := exec.ParseWindowFunc(s)
	return fn
}

// refuseUnwindowable fails the plan for a window expression whose function has
// no window form, with the SAME sentence the worker's fragment builder raises
// (exec.RefuseUnsupportedWindowFunc). Before it, such a plan reached
// exec.Window as ROW_NUMBER with a mis-typed output vector and PANICKED —
// 23 of the 28 known aggregates did, reported as "internal error in pipeline"
// with no SQLSTATE (#965's census; see exec.RefuseUnsupportedWindowFunc).
func refuseUnwindowable(exprs []logical.WindowExpr) error {
	for _, we := range exprs {
		if _, ok := exec.ParseWindowFunc(we.Func); !ok {
			return exec.RefuseUnsupportedWindowFunc(we.Func)
		}
	}
	return nil
}

// resolveNullsLast determines whether nulls should sort last for a given order
// expression. An explicit NULLS FIRST / NULLS LAST always wins; otherwise the
// engine default applies: NULLS LAST for ASC, NULLS FIRST for DESC.
//
// That is PostgreSQL's rule, chosen deliberately. SQL leaves the default
// implementation-defined and DuckDB picks NULLS LAST in both directions, but
// wadjet speaks the PostgreSQL wire protocol, so a psql/DataGrip/Superset user
// writing ORDER BY x DESC expects PostgreSQL's placement. The DuckDB gate is
// held to the same rule by setting default_null_order in the oracle rather
// than by exempting entries, so the comparison keeps its full strength.
//
// See distributed.SortKeySpec.PlaceNullsLast, which has to agree with this
// function key for key or the two execution paths sort differently.
func resolveNullsLast(ob logical.OrderExpr) bool {
	if ob.NullsFirst != nil {
		return !*ob.NullsFirst // NullsFirst=true => NullsLast=false, and vice versa
	}
	return !ob.Desc
}

// isComputedProjection reports whether a SELECT item's value is COMPUTED
// rather than read straight from an input column. Only a bare column
// reference — optionally parenthesised — reads an input column; everything
// else (function call, arithmetic, CASE, CAST, concatenation) produces a new
// value whose type comes from the expression, not from whatever input column
// happens to share the output's alias (#327).
//
// A nil AST expression is the pre-AST projection form, which is always a
// plain column.
func isComputedProjection(e plansql.Node) bool {
	for {
		switch n := e.(type) {
		case nil:
			return false
		case *plansql.ColRef:
			return false
		case *plansql.ParenNode:
			e = n.Inner
		default:
			return true
		}
	}
}

// cleanExpr drops the table qualifier from a COLUMN REFERENCE, and leaves
// everything else exactly as written.
//
// The distinction is the whole of the function. Its callers hand it text that
// is usually `t.col` and sometimes an arbitrary expression, and the second
// kind has no qualifier to strip: the first dot in `concat(t0.c0, t0.c1)`
// separates a table from a column only if you already know the text is a
// column reference. A naive SplitN on '.' does not, so it returned
// `c0, t0.c1)` — a fragment of the expression, parentheses and commas
// included, which then became the OUTPUT COLUMN NAME a client binds by
// (#513).
//
// plansql.SplitIdentRef is the test, because it is the lexer: it accepts
// `col`, `t.col` and the delimited spellings (`"id.orig_h"` is ONE name, a
// flat Zeek JSON column with no qualifier — #304) and rejects anything that
// does not end after the identifier, which is every function call, operator
// expression and literal.
func cleanExpr(s string) string {
	s = strings.TrimSpace(s)
	if _, name, ok := plansql.SplitIdentRef(s); ok {
		return name
	}
	return s
}

// scanParallelism returns the worker count for scan/decode/pipeline
// parallelism, honoring WADJET_SCAN_WORKERS when set (>0). Default is
// runtime.NumCPU(), the historical behavior — but a 2026-08-17 profiling
// pass measured the fast-query tier DOUBLING its wall time from decode
// over-subscription past the memory-bandwidth knee (24 workers 87.6ms vs
// 12 workers 43.8ms on a 12-core box, every profile symbol inflating
// uniformly with zero contention symbols). The env knob exists to A/B a
// lower default on the benchmark metal before changing it for everyone.
func scanParallelism() int {
	if v := os.Getenv("WADJET_SCAN_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return runtime.NumCPU()
}

// innerPipelineWorkers returns the number of parallel workers for an inner
// pipeline (aggregate/sort child). Returns 0 (serial) unless the source is
// a concurrent-safe scan source.
func innerPipelineWorkers(src exec.Source) int {
	switch src.(type) {
	case *catalogScanSource, *scannerExecSource, *deferredJoinBridge:
		return scanParallelism()
	}
	return 0
}

// aggSourceAdapter wraps a child pipeline + hash aggregate into a Source.
type aggSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	agg         *exec.HashAggregate
	initialized bool
	// pipe is the inner pipeline this adapter runs. It is HELD, not
	// discarded: it owns the child ops and the morsel-parallel clone
	// sinks, and only its Close reaches them. Discarding it left a
	// cancelled GROUP BY's agg-spill-*.bin files on disk for the process
	// lifetime (#625 M2).
	pipe *exec.Pipeline
}

// ServesHeldState marks the adapter's output phase as a held-state drain —
// exempt from heap-backpressure pauses (exec.HeldStateSource).
func (a *aggSourceAdapter) ServesHeldState() bool { return true }

func (a *aggSourceAdapter) Init(ctx context.Context) error {
	return nil
}

func (a *aggSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !a.initialized {
		a.initialized = true
		// Run child pipeline into aggregate
		a.pipe = &exec.Pipeline{
			Source:  a.childSource,
			Ops:     a.childOps,
			Sink:    a.agg,
			Workers: innerPipelineWorkers(a.childSource),
		}
		if err := a.pipe.Run(ctx); err != nil {
			return nil, err
		}
	}
	return a.agg.Next(ctx)
}

func (a *aggSourceAdapter) RowsScanned() int64 {
	if sp, ok := a.childSource.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
}

func (a *aggSourceAdapter) Close() error {
	if a.pipe != nil {
		// Reaches the child ops and the clone sinks as well as the
		// aggregate and the source.
		return a.pipe.Close()
	}
	a.agg.Close()
	return a.childSource.Close()
}

// sortSourceAdapter wraps a child pipeline + sort into a Source.
// When sort.Limit >= 0, it truncates results to the top N rows after
// sorting (Top-K optimization: avoids materializing the full sorted
// result). The bound lives only on sort.Limit — no separate limitN field to
// keep in sync — so a real LIMIT 0 (sort.Limit == 0) truncates correctly
// instead of colliding with sort.Limit's own "no limit" sentinel (#481).
type sortSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	sort        *exec.Sort
	initialized bool
	pipe        *exec.Pipeline // held so Close reaches the child ops and clones (#625 M2)
}

// ServesHeldState marks the adapter's output phase as a held-state drain —
// exempt from heap-backpressure pauses (exec.HeldStateSource).
func (s *sortSourceAdapter) ServesHeldState() bool { return true }

func (s *sortSourceAdapter) Init(ctx context.Context) error {
	return nil
}

func (s *sortSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !s.initialized {
		s.initialized = true
		s.pipe = &exec.Pipeline{
			Source:  s.childSource,
			Ops:     s.childOps,
			Sink:    s.sort,
			Workers: innerPipelineWorkers(s.childSource),
		}
		if err := s.pipe.Run(ctx); err != nil {
			return nil, err
		}
		// Top-K truncation: discard everything beyond sort.Limit rows. >= 0,
		// not > 0 — a real LIMIT 0 must truncate to zero rows too (#481).
		if s.sort.Limit >= 0 {
			s.sort.Truncate(s.sort.Limit)
		}
	}
	return s.sort.Next(ctx)
}

func (s *sortSourceAdapter) Close() error {
	if s.pipe != nil {
		return s.pipe.Close()
	}
	s.sort.Close()
	return s.childSource.Close()
}

func (s *sortSourceAdapter) RowsScanned() int64 {
	if sp, ok := s.childSource.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
}

// windowSourceAdapter wraps a child pipeline + window into a Source.
// ServesHeldState — see aggSourceAdapter (exec.HeldStateSource).
func (w *windowSourceAdapter) ServesHeldState() bool { return true }

type windowSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	win         *exec.Window
	initialized bool
	pipe        *exec.Pipeline // held so Close reaches the child ops and clones (#625 M2)
}

func (w *windowSourceAdapter) Init(_ context.Context) error { return nil }

func (w *windowSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !w.initialized {
		w.initialized = true
		w.pipe = &exec.Pipeline{
			Source: w.childSource,
			Ops:    w.childOps,
			Sink:   w.win,
		}
		if err := w.pipe.Run(ctx); err != nil {
			return nil, err
		}
	}
	return w.win.Next(ctx)
}

func (w *windowSourceAdapter) RowsScanned() int64 {
	if sp, ok := w.childSource.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
}

func (w *windowSourceAdapter) Close() error {
	if w.pipe != nil {
		return w.pipe.Close()
	}
	w.win.Close()
	return w.childSource.Close()
}

// newScannerSource creates a scanner exec.Source from the catalog. snap may
// be nil (falls back to an ordinary catalog.GetManifest call in Init).
func newScannerSource(cat *catalog.Catalog, tableName string, partFilter map[string]string, requiredCols []string, scanPreds []logical.Predicate, snap *ManifestSnapshot) exec.Source {
	return &scannerExecSource{
		catalog:          cat,
		tableName:        tableName,
		partitionFilter:  partFilter,
		requiredCols:     requiredCols,
		scanPreds:        scanPreds,
		manifestSnapshot: snap,
	}
}

type scannerExecSource struct {
	catalog          *catalog.Catalog
	tableName        string
	partitionFilter  map[string]string
	requiredCols     []string
	scanPreds        []logical.Predicate
	allowedFiles     []string // probe-split: only scan these files (nil = all)
	scanner          *scanSourceInner
	bloomFilter      *exec.BloomScanFilter
	dynamicFilter    []exec.DynamicRange
	rowLimit         int64                // LIMIT pushdown: enables lazy file downloading (0 = eager)
	memTracker       *memory.Tracker      // per-query memory tracker; passed to scanSourceInner at Init
	spillMgr         *memory.SpillManager // for pre-emptive relief on file-load reservations; nil-safe
	emitRowLoc       bool                 // top-N late materialization: stamp __row_loc on scan batches
	rowPreds         []scan.RowPred       // scan-level filter conjuncts
	shapeOnlyCols    map[string]bool      // byte-array columns decoded as lengths only
	manifestSnapshot *ManifestSnapshot    // pins this table's manifest to one read per statement (#502); nil-safe
}

type scanSourceInner struct {
	cat            *catalog.Catalog
	tableName      string
	files          []catalog.FileEntry
	idx            int64 // atomic index for parallel file workers (fallback path)
	schema         []parquet.Column
	requiredCols   []string
	scanPreds      []scanPredicate // converted predicates for row-group pruning
	rowsScanned    int64
	deleteMarkers  map[string]map[int64]bool // file path -> set of row indices to skip
	hasNestedTypes bool                      // true if schema has ARRAY/ROW/MAP types
	rowLimit       int64                     // >0: lazy file downloading (LIMIT pushdown)

	// row-group-level parallel scan
	rgUnits       []rgUnit        // flat list of row group work units
	rgIdx         int64           // atomic index for parallel RG workers
	emitRowLoc    bool            // stamp __row_loc (rgUnit ordinal, row) on every scan batch; disables batch pooling
	eqProbes      []scan.EqProbe  // "=" conjuncts for dictionary-probe row-group pruning (dict_prune.go)
	rowPreds      []scan.RowPred  // scan-level filter conjuncts, evaluated per row group in readRG
	shapeOnlyCols map[string]bool // lowercased names of columns decoded as lengths only (lengths_decode.go)
	countOnlyScan bool            // requiredCols is exactly the row-count sentinel: batches carry Len/Sel only
	useNative     bool            // true if native page decoder can be used (no Decimal/Array/Map)
	loadGate      *loadGate       // byte-budgeted admission for in-flight file LOADs (data, not metadata)

	// batch pooling — reuse batch allocations across row groups
	pool *batch.BatchPool

	cachedReadSchema     []parquet.Column // projected schema, computed once
	cachedReadSchemaOnce sync.Once        // guards cachedReadSchema for concurrent rgWorker access

	// parallel scan
	batchCh chan *batch.RecordBatch
	errCh   chan error
	wg      sync.WaitGroup
	cancel  context.CancelFunc

	// Bloom filter pushdown from hash join build side.
	bloomFilter *exec.BloomScanFilter

	// Dynamic min/max range filter from hash join build side.
	dynamicFilter []exec.DynamicRange

	// failedFiles counts files that failed to read during buildRGUnits.
	// When > 0, Init returns an error to prevent silent data loss.
	failedFiles  int
	firstFileErr error // sample error from the first file failure

	// fatalScanErr is a failure the scan must NOT tolerate, however many
	// other files succeeded. failedFiles is deliberately forgiving — it only
	// fails the scan when EVERY file failed, because a since-deleted object
	// is a survivable degradation. A recovered panic is not in that class: a
	// footer decoder that panicked has no idea how many row groups it should
	// have produced, so tolerating it drops that file's rows and answers a
	// wrong number (#511).
	fatalScanErr error

	// pooledBufs tracks []byte buffers obtained from readBufPool during
	// buildRGUnits. These are returned to the pool when the scan source
	// is closed, enabling cross-query buffer reuse.
	pooledBufsMu sync.Mutex
	pooledBufs   [][]byte

	// memTracker accounts for pooled buffers (parquet file []byte loads).
	// nil-safe: when nil, tracking is a no-op. Wired by the planner at
	// scan-source construction when a per-query spill manager is available.
	memTracker *memory.Tracker

	// trackedBufBytes is the cumulative bytes currently reported to memTracker
	// from pooledBufs. Released atomically in releasePooledBufs to avoid
	// double-release on idempotent close.
	trackedBufBytes atomic.Int64

	// spillMgr lets file-load reservations request operator relief before
	// waiting on the budget (memory.ReserveOrForce). nil-safe.
	spillMgr *memory.SpillManager

	// residentSlabs counts the row-group buffers this scan source is holding
	// across every file (scan_rowgroup_load.go). It is the deadlock-freedom
	// floor: a loader with nothing resident admits its next row group without
	// waiting, because there is nothing decoding that could free room for it.
	residentSlabs atomic.Int64

	// batchCharges maps decoded batches currently held by the scan source
	// (decode in progress, prefetched, or queued in batchCh) to the bytes
	// charged against memTracker when they were decoded. Released when the
	// batch leaves through next(), is dropped on a filter path, or is
	// drained at Close. LoadAndDelete makes every release idempotent.
	batchCharges sync.Map
}

// trackScanBatch charges a freshly decoded batch's footprint to the memory
// tracker until the batch leaves the scan source. No-op without a tracker.
func (inner *scanSourceInner) trackScanBatch(b *batch.RecordBatch) {
	if inner.memTracker == nil || b == nil {
		return
	}
	n := b.MemBytes()
	if n <= 0 {
		return
	}
	inner.batchCharges.Store(b, n)
	inner.memTracker.ForceReserveFor(n, memory.ForceScanDecodedBatch)
}

// releaseScanBatch releases the charge recorded by trackScanBatch.
// Idempotent: a second release for the same batch is a no-op.
func (inner *scanSourceInner) releaseScanBatch(b *batch.RecordBatch) {
	if inner.memTracker == nil || b == nil {
		return
	}
	if n, ok := inner.batchCharges.LoadAndDelete(b); ok {
		inner.memTracker.ReleaseForced(n.(int64), memory.ForceScanDecodedBatch)
	}
}

// drainSlotCharges releases the lazy fileSlot state of every slot whose row
// groups were not fully consumed — buffers, load-gate bytes and shared-tracker
// charges abandoned by an early Close (LIMIT, cancel, error). Must run after
// wg.Wait (no rg worker may still be loading) and BEFORE releasePooledBufs,
// which nils rgUnits (the only reference to the slots).
func (inner *scanSourceInner) drainSlotCharges() {
	seen := make(map[*fileSlot]bool)
	for _, u := range inner.rgUnits {
		if u.slot == nil || seen[u.slot] {
			continue
		}
		seen[u.slot] = true
		if u.slot.rgRemaining.Load() > 0 {
			u.slot.drainAbandoned(inner)
		}
	}
}

// drainBatchCharges releases every outstanding decoded-batch charge —
// batches stranded in batchCh by cancellation or never sent by an exiting
// worker. Callers must ensure the rg/scan workers have exited first (the
// charges live on a shared worker-level tracker; a racing Store here would
// leak its bytes for the worker's lifetime).
func (inner *scanSourceInner) drainBatchCharges() {
	if inner.memTracker == nil {
		return
	}
	inner.batchCharges.Range(func(k, _ any) bool {
		if n, ok := inner.batchCharges.LoadAndDelete(k); ok {
			inner.memTracker.Release(n.(int64))
		}
		return true
	})
}

// trackPooledBuf records a buffer obtained from readBufPool so it can be
// returned when the scan source is closed. Thread-safe for parallel readers.
func (inner *scanSourceInner) trackPooledBuf(buf []byte) {
	inner.pooledBufsMu.Lock()
	inner.pooledBufs = append(inner.pooledBufs, buf)
	inner.pooledBufsMu.Unlock()

	if inner.memTracker != nil {
		n := int64(cap(buf))
		inner.memTracker.ForceReserveFor(n, memory.ForceScanPooledBuffer)
		inner.trackedBufBytes.Add(n)
	}
}

// releasePooledBufs returns all tracked buffers to readBufPool.
// Safe to call multiple times — subsequent calls are no-ops.
func (inner *scanSourceInner) releasePooledBufs() {
	inner.pooledBufsMu.Lock()
	bufs := inner.pooledBufs
	inner.pooledBufs = nil
	inner.pooledBufsMu.Unlock()

	if inner.memTracker != nil {
		released := inner.trackedBufBytes.Swap(0)
		if released > 0 {
			inner.memTracker.ReleaseForced(released, memory.ForceScanPooledBuffer)
		}
	}

	// Nil out rgUnits to break pqFile → bytes.Reader → []byte reference
	// chain before returning buffers, so GC doesn't pin old data.
	inner.rgUnits = nil
	for _, buf := range bufs {
		putReadBuf(buf)
	}
}

// scanPredicate is a simple predicate for row-group stats pruning.
type scanPredicate struct {
	Column string
	Op     string
	Value  any
}

func (s *scannerExecSource) Init(ctx context.Context) error {
	manifest, err := getManifestWith(ctx, s.manifestSnapshot, s.catalog, s.tableName)
	if err != nil {
		return err
	}
	tableMeta, err := s.catalog.GetTable(ctx, s.tableName)
	if err != nil {
		return err
	}

	var files []catalog.FileEntry
	for _, p := range manifest.Partitions {
		// Prune partitions that don't match the filter
		if len(s.partitionFilter) > 0 && len(p.Values) > 0 {
			if !matchesPartitionFilter(p.Values, s.partitionFilter) {
				continue
			}
		}
		files = append(files, p.Files...)
	}

	// Probe-split: restrict to only allowed files for this scan alias.
	if len(s.allowedFiles) > 0 {
		allowed := make(map[string]bool, len(s.allowedFiles))
		for _, f := range s.allowedFiles {
			allowed[f] = true
		}
		filtered := files[:0]
		for _, f := range files {
			if allowed[f.Path] {
				filtered = append(filtered, f)
			}
		}
		files = filtered
	}

	scanCtx, cancel := context.WithCancel(ctx)
	// Convert logical predicates to scan predicates for row-group pruning.
	//
	// The literal is in the ENGINE's domain and the row group's statistics
	// and dictionary are in the FILE's, and for several types those are not
	// the same thing: a DATE is a day number against a text literal, a
	// DECIMAL's bounds are the unscaled integer against a float, an IPV6's
	// are the raw sixteen bytes against an address in text. The prune layer
	// compares two `any` values by their Go kind and cannot tell — so the
	// conversion happens HERE, the one place that still holds the column's
	// type and scale, and a predicate with no conversion is WITHHELD rather
	// than pushed down raw (#442, #438). kernel.StatsDomainValue is the same
	// conversion the filter kernel applies to the literal, so the prune and
	// the filter cannot disagree about what the predicate means.
	var sp []scanPredicate
	var eqProbes []scan.EqProbe
	for _, pred := range s.scanPreds {
		if pred.Column == "" || pred.Op == "" || pred.Value == nil {
			continue
		}
		// The predicate's column arrives as a REFERENCE — an unquoted
		// identifier folds to lower case at the lexer (#731) — while the
		// schema keeps the spelling the parquet file gave it, and CamelCase
		// column names are ordinary there (ClickBench's `hits` has
		// `EventDate`, `UserAgent`, `ResolutionWidth`). A byte-exact lookup
		// missed every column of every such table, so neither the row-group
		// statistics prune nor the dictionary probe was ever built for it:
		// the answer stayed right and the whole table was read. Resolve the
		// way the engine resolves every other reference, and carry the
		// SCHEMA's spelling forward — that name keys the row group's
		// per-column statistics (`scan.CanPruneRowGroup`) and matches the
		// file's own leaves (`scan.CanDictPruneRowGroup`), neither of which
		// has ever seen the folded spelling.
		ci := batch.ResolveSchemaIndex(tableMeta.Schema.Columns, pred.Column)
		if ci < 0 {
			continue
		}
		col := tableMeta.Schema.Columns[ci]
		// A DECIMAL bound is converted from the literal's TEXT: the float64
		// box has already dropped the digits past a double, and a bound that
		// is off by a fraction of the last place prunes the row group the
		// answer is in (#452).
		lit := pred.Value
		if col.Type == parquet.TypeDecimal && pred.ValueText != "" {
			lit = pred.ValueText
		}
		val, ok := kernel.StatsDomainValue(col.Type, int(col.Scale), lit)
		if !ok {
			continue
		}
		sp = append(sp, scanPredicate{Column: col.Name, Op: pred.Op, Value: val})
		// Equality conjuncts also feed the dictionary probe — the
		// precise prune where zonemaps are blind (point filters on
		// high-cardinality columns). Dictionary entries are raw file
		// values too, so they take the same converted literal.
		if pred.Op == "=" && scan.DictPrune.On() {
			// A DECIMAL probe's carrier is at the CATALOG scale; the file's
			// dictionary is at the file's own scale. Carry the catalog
			// declaration so the probe layer can reconcile the two before
			// comparing (dictProbeDecimalAbsent), the dictionary twin of the
			// stats-path reconcile (#707/#916). Non-DECIMAL probes leave these
			// zero and the probe layer never reads them.
			ep := scan.EqProbe{ColName: col.Name, Value: val}
			if col.Type == parquet.TypeDecimal {
				ep.Scale, ep.Precision = int(col.Scale), int(col.Precision)
			}
			eqProbes = append(eqProbes, ep)
		}
	}

	// Load delete markers for merge-on-read deletes
	var delMarkers map[string]map[int64]bool
	if len(manifest.DeleteMarkers) > 0 {
		delMarkers = make(map[string]map[int64]bool, len(manifest.DeleteMarkers))
		for _, dm := range manifest.DeleteMarkers {
			idxSet := make(map[int64]bool, len(dm.RowIndices))
			for _, idx := range dm.RowIndices {
				idxSet[idx] = true
			}
			delMarkers[dm.FilePath] = idxSet
		}
	}

	// Use a smaller batch channel for LIMIT queries to bound in-flight downloads.
	batchChSize := scanParallelism()
	if s.rowLimit > 0 {
		batchChSize = 2
	}

	inner := &scanSourceInner{
		cat:           s.catalog,
		tableName:     s.tableName,
		files:         files,
		schema:        tableMeta.Schema.Columns,
		requiredCols:  s.requiredCols,
		scanPreds:     sp,
		deleteMarkers: delMarkers,
		bloomFilter:   s.bloomFilter,
		dynamicFilter: s.dynamicFilter,
		rowLimit:      s.rowLimit,
		batchCh:       make(chan *batch.RecordBatch, batchChSize),
		errCh:         make(chan error, 1),
		cancel:        cancel,
		memTracker:    s.memTracker,
		spillMgr:      s.spillMgr,
		emitRowLoc:    s.emitRowLoc,
		eqProbes:      eqProbes,
		rowPreds:      s.rowPreds,
		shapeOnlyCols: s.shapeOnlyCols,
		countOnlyScan: len(s.requiredCols) == 1 && s.requiredCols[0] == logical.RowCountOnlyColumn,
	}
	// Nested (ARRAY/MAP/ROW) schemas must take the file-level scan whose
	// readBatchDirect falls back to the row-based reader. This MUST be
	// decided HERE: the eager branch only learned about nested types
	// inside buildRGUnits, which runs after the branch was already taken —
	// the early return left zero rgUnits and every query against a nested
	// table returned 0 rows with no error (issue #144 suite finding).
	// Decided on the columns this scan READS, matching readBatchDirect's own
	// test: one ARRAY/ROW/MAP column in a table used to put every query on
	// that table onto the row reader, which mints unpooled batches and reads
	// every column of every row group (#393).
	innerSchema := parquet.Schema{Columns: buildReadSchema(inner.schema, inner.requiredCols)}
	inner.hasNestedTypes = innerSchema.HasNestedColumns()
	s.scanner = inner

	// Row-loc stamping needs the row-group-parallel path: rgUnit ordinals
	// are the row identity. The planner's rewrite only engages on shapes
	// that take the eager branch; this is the belt-and-braces check.
	if inner.emitRowLoc && (inner.rowLimit > 0 || inner.hasNestedTypes) {
		cancel()
		return fmt.Errorf("scan %s: row-loc emission requires the row-group-parallel scan path", s.tableName)
	}
	// Same for pushed scan filters: the lazy/nested path never evaluates
	// them, and a silently dropped filter is wrong results. The planner
	// gates both conditions; fail loudly if they ever meet anyway.
	if len(inner.rowPreds) > 0 && (inner.rowLimit > 0 || inner.hasNestedTypes) {
		cancel()
		return fmt.Errorf("scan %s: pushed scan filters require the row-group-parallel scan path", s.tableName)
	}

	if inner.rowLimit > 0 || inner.hasNestedTypes {
		// Lazy file-level scan: download files on-demand, one at a time per worker.
		// Used for LIMIT pushdown (avoids downloading all files upfront) and
		// nested types (which need row-level reading).
		// Workers stop when context is cancelled (pipeline cancels after LIMIT satisfied).
		workers := scanParallelism()
		if workers > len(files) {
			workers = len(files)
		}
		if workers < 1 {
			workers = 1
		}
		inner.wg.Add(workers)
		for i := 0; i < workers; i++ {
			go inner.scanWorker(scanCtx)
		}
	} else {
		// Eager row-group-level parallel scan: download all files, enumerate
		// row groups, apply predicate pruning, then process RGs in parallel.
		inner.buildRGUnits(scanCtx)

		// A panic on a footer reader is fatal to the scan regardless of how
		// many other files parsed, because the rows it would have
		// contributed are simply missing from the answer.
		if inner.fatalScanErr != nil {
			cancel()
			return fmt.Errorf("scan %s: %w", s.tableName, inner.fatalScanErr)
		}

		// Fail the scan if all files failed to read — prevents silent 0-row
		// results that are indistinguishable from correct empty results.
		if inner.failedFiles > 0 && len(inner.rgUnits) == 0 && len(inner.files) > 0 {
			cancel()
			sampleErr := ""
			if inner.firstFileErr != nil {
				sampleErr = fmt.Sprintf(": %v", inner.firstFileErr)
			}
			return fmt.Errorf("scan %s: all %d files failed to read (%d failures)%s", s.tableName, len(inner.files), inner.failedFiles, sampleErr)
		}

		// Initialize batch pool from the LARGEST row group: GetForSize
		// falls back to a fresh unpooled allocation for any request above
		// the pool's batch size, so sizing from rgUnits[0] meant every
		// row group bigger than the first bypassed the pool entirely —
		// full vector allocation + zeroing per row group (13% of the
		// 100-part floor probe's makeslice profile).
		if len(inner.rgUnits) > 0 {
			rgSize := 0
			for _, u := range inner.rgUnits {
				if int(u.numRows) > rgSize {
					rgSize = int(u.numRows)
				}
			}
			readSchema := inner.readSchema()
			// Row-loc stamping appends a column after decode, which would
			// poison the fixed-schema pool on release — skip pooling (the
			// narrow late-mat scan allocates little anyway).
			if rgSize > 0 && len(readSchema) > 0 && !inner.emitRowLoc {
				inner.pool = batch.NewBatchPool(readSchema, rgSize)
				inner.pool.PreWarm(runtime.NumCPU())
			}
			// Pre-compute whether native page decoding can be used.
			inner.useNative = !scan.HasUnsupportedColumnarTypes(readSchema)
		}

		// Byte-aware decode parallelism: CPU-count workers with a CPU-count
		// queue are blind to batch WIDTH. On a 105-column SELECT * scan each
		// decoded row-group batch is hundreds of MB; 16 decoders + 16 queued
		// batches held multiple GB of live wide batches (plus the GC target
		// doubling that live set), which OOM-killed the c6a on ClickBench
		// Q24 even with the sort side bounded. Clamp workers + queue so
		// estimated in-flight decoded bytes stay within a budget slice;
		// narrow scans (TPC-H) still get full CPU-count parallelism.
		workers := scanParallelism()
		if len(inner.rgUnits) > 0 {
			rgRows := int(inner.rgUnits[0].numRows)
			perBatch := estimateDecodedBatchBytes(inner.readSchema(), rgRows)
			inflightCap := int64(8 << 30)
			if inner.memTracker != nil {
				if b := inner.memTracker.Budget(); b > 0 && b/3 < inflightCap {
					inflightCap = b / 3
				}
			}
			if perBatch > 0 {
				maxInflight := int(inflightCap / perBatch)
				if maxInflight < 2 {
					maxInflight = 2
				}
				if workers > maxInflight-1 {
					workers = maxInflight - 1
				}
				queue := maxInflight - workers
				if queue < 1 {
					queue = 1
				}
				if queue < cap(inner.batchCh) {
					inner.batchCh = make(chan *batch.RecordBatch, queue)
				}
			}
		}
		if workers > len(inner.rgUnits) {
			workers = len(inner.rgUnits)
		}
		if workers < 1 {
			workers = 1
		}
		inner.wg.Add(workers)
		for i := 0; i < workers; i++ {
			go inner.rgWorker(scanCtx)
		}
	}

	// Close batchCh when all workers are done
	go func() {
		defer inner.recoverWorkerPanic(ctx, "scan batch-channel closer")
		inner.wg.Wait()
		close(inner.batchCh)
	}()

	return nil
}

// estimateDecodedBatchBytes estimates the decoded in-memory size of one
// row-group batch for the projected schema. Fixed types use their storage
// width; variable-length types assume 48 B/row — a deliberate overestimate
// for short strings (safe direction: it only reduces decode parallelism).
func estimateDecodedBatchBytes(schema []parquet.Column, rows int) int64 {
	if rows <= 0 {
		return 0
	}
	perRow := 0
	for _, c := range schema {
		switch c.Type {
		case parquet.TypeBool:
			perRow += 1
		case parquet.TypeInt32, parquet.TypeDate, parquet.TypePort, parquet.TypeProtocol, parquet.TypeFloat32:
			perRow += 4
		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeFloat64, parquet.TypeIPv4,
			parquet.TypeMAC, parquet.TypeDuration:
			perRow += 8
		case parquet.TypeDecimal, parquet.TypeUUID, parquet.TypeIPv6:
			perRow += 16
		default: // strings/bytes/nested
			perRow += 48
		}
	}
	return int64(perRow) * int64(rows)
}

// matchesPartitionFilter returns true if all filter keys match the partition values.
func matchesPartitionFilter(partValues, filter map[string]string) bool {
	for k, v := range filter {
		pv, ok := partValues[k]
		if !ok {
			continue // partition doesn't have this key, skip
		}
		if pv != v {
			return false
		}
	}
	return true
}

func (s *scannerExecSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	return s.scanner.next(ctx)
}

func (s *scannerExecSource) Close() error {
	if s.scanner != nil {
		if s.scanner.cancel != nil {
			s.scanner.cancel()
		}
		// Wait for the scan workers to exit before draining: a worker racing
		// drainBatchCharges could charge a batch after the drain, leaking the
		// bytes on the shared worker-level tracker for the worker's lifetime.
		// Workers observe the cancel at the loop head and in every blocking
		// select, so this wait is bounded by one in-flight row-group decode.
		s.scanner.wg.Wait()
		s.scanner.drainBatchCharges()
		s.scanner.drainSlotCharges()
		s.scanner.releasePooledBufs()
	}
	return nil
}

func (s *scannerExecSource) RowsScanned() int64 {
	if s.scanner != nil {
		return atomic.LoadInt64(&s.scanner.rowsScanned)
	}
	return 0
}

// recoverWorkerPanic converts a panic raised on a scan goroutine into the
// scan's error instead of letting it take the process down.
//
// These goroutines are not the caller's: Pipeline.Run recovers on ITS
// goroutine, so a *batch.TypeMismatchError raised by Vector.SetValue — whose
// whole design (#361) is "a query error, never the server" — killed the
// process here, and with it every other client's query (#400, and #393 as the
// query that reaches it). Since #511 it converts ANY panic, not only the
// FatalEvalPanic class: a decoder bug on a scan worker is still one query's
// failure, not the server's.
//
// errCh is buffered, and next() selects on it, so a non-blocking send is
// enough; the cancel stops the sibling workers.
func (inner *scanSourceInner) recoverWorkerPanic(ctx context.Context, what string) {
	r := recover()
	if r == nil {
		return
	}
	err := exec.RecoverQueryPanic(ctx, what, r)
	select {
	case inner.errCh <- fmt.Errorf("%s: %w", what, err):
	default:
	}
	if inner.cancel != nil {
		inner.cancel()
	}
}

// scanWorker reads files in parallel, writing decoded batches to batchCh.
func (inner *scanSourceInner) scanWorker(ctx context.Context) {
	defer inner.wg.Done()
	defer inner.recoverWorkerPanic(ctx, "scan worker")

	for {
		idx := int(atomic.AddInt64(&inner.idx, 1) - 1)
		if idx >= len(inner.files) {
			return
		}
		if ctx.Err() != nil {
			return
		}

		file := inner.files[idx]
		var reader *parquet.Reader
		if ras, ok := inner.cat.Store().(objstore.ReaderAtStore); ok {
			rac, size, err := ras.GetReaderAt(ctx, inner.cat.Bucket(), file.Path)
			if err != nil {
				continue
			}
			reader, err = parquet.NewReader(rac, size)
			if err != nil {
				rac.Close()
				continue
			}
		} else {
			rc, _, err := inner.cat.Store().Get(ctx, inner.cat.Bucket(), file.Path)
			if err != nil {
				continue
			}
			data, err := readAllSized(rc, file.SizeBytes, true)
			rc.Close()
			if err != nil {
				continue
			}
			inner.trackPooledBuf(data)
			reader, err = parquet.NewReaderFromBytesCached(data,
				footerCacheIdentity(inner.cat, file, int64(len(data))))
			if err != nil {
				continue
			}
		}

		b, err := readBatchDirect(reader, inner.schema, inner.requiredCols, inner.scanPreds...)
		if err != nil {
			// Surface the first decode error so the scan FAILS instead of
			// dropping this file's rows: a swallowed error here is
			// indistinguishable from a file that legitimately contributed
			// nothing (the same silent-partial class readRG guards).
			select {
			case inner.errCh <- fmt.Errorf("reading %s: %w", file.Path, err):
			default:
			}
			return
		}
		if b == nil || b.Len == 0 {
			continue
		}
		inner.trackScanBatch(b)

		// Apply delete markers: skip rows marked for deletion
		if delSet := inner.deleteMarkers[file.Path]; len(delSet) > 0 {
			sel := make([]uint32, 0, b.Len)
			for i := 0; i < b.Len; i++ {
				if !delSet[int64(i)] {
					sel = append(sel, uint32(i))
				}
			}
			if len(sel) == 0 {
				inner.releaseScanBatch(b)
				continue
			}
			if len(sel) < b.Len {
				b.Sel = sel
			}
		}

		atomic.AddInt64(&inner.rowsScanned, int64(b.ActiveLen()))

		select {
		case inner.batchCh <- b:
		case <-ctx.Done():
			return
		}
	}
}

// readSchema returns the column-projected schema for this scan.
// Multiple rgWorker goroutines call this concurrently for the same source,
// so the cache is guarded by sync.Once to avoid a data race on the
// cachedReadSchema field.
func (inner *scanSourceInner) readSchema() []parquet.Column {
	inner.cachedReadSchemaOnce.Do(func() {
		inner.cachedReadSchema = buildReadSchema(inner.schema, inner.requiredCols)
	})
	return inner.cachedReadSchema
}

func (inner *scanSourceInner) next(ctx context.Context) (*batch.RecordBatch, error) {
	select {
	case b, ok := <-inner.batchCh:
		if !ok {
			// Channel closed, check for errors
			select {
			case err := <-inner.errCh:
				return nil, err
			default:
				return nil, nil
			}
		}
		// The batch leaves the scan source here — downstream operators that
		// retain it account for it themselves (TrackBatch et al).
		inner.releaseScanBatch(b)
		return b, nil
	case err := <-inner.errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// wrapExpr adapts an expr.Expr into an exec.Expression function.
func wrapExpr(e expr.Expr) exec.Expression {
	return func(b *batch.RecordBatch, row int) any {
		return e.Eval(b, row)
	}
}

// wrapPredicate adapts an expr.Expr into an exec.Predicate function. The
// typed protocol and its two-valued collapse are chosen once, in
// expr.FilterPredicate, so the row loop neither boxes nor re-dispatches.
func wrapPredicate(e expr.Expr) exec.Predicate {
	return expr.FilterPredicate(e)
}

// expandStarProjections runs logical star expansion on a plan that reached the
// physical planner without it — logical.Optimize expands stars before column
// pruning, so this only fires for plans built and planned without optimizing.
// The rewrite reads the scan's annotated schema, so annotate first; that costs
// a catalog walk, which is why it is gated on a star actually being present.
func (p *Planner) expandStarProjections(ctx context.Context, node, child *logical.Node) {
	if p.catalog == nil || !logical.HasStarProjection(node) {
		return
	}
	p.AnnotateScanColumns(ctx, child)
	logical.ExpandStarProjections(node)
	logical.ResolveOrdinalSortKeys(node)
}

// limitPushdownSafe reports whether a LIMIT may be applied independently by
// each task under it.
//
// It may when every node between the LIMIT and its scans passes rows through
// one at a time: Project and Filter qualify (a filtered task simply reaches n
// later, or never), and a scan is the base case. Anything that derives rows
// from more than one input row — join, aggregate, distinct, sort, window, set
// operation — does not: bounding its INPUT changes its OUTPUT, which would
// silently produce wrong answers rather than merely fewer rows.
//
// Multiple scans under a UNION ALL are fine: each bounds itself, and the
// coordinator trims the union to n.
func limitPushdownSafe(node *logical.Node) bool {
	if node == nil {
		return false
	}
	sawScan := false
	var walk func(n *logical.Node) bool
	walk = func(n *logical.Node) bool {
		if n == nil {
			return false
		}
		switch n.Type {
		case logical.NodeScan:
			// A table function's row count is not bounded by its input, but
			// stopping early still yields a prefix of what it would produce.
			sawScan = true
			return true
		case logical.NodeProject, logical.NodeFilter, logical.NodeLimit:
			// A nested LIMIT is at most as permissive as this one.
		default:
			return false
		}
		for _, c := range n.Children {
			if !walk(c) {
				return false
			}
		}
		return true
	}
	if !walk(node) {
		return false
	}
	return sawScan
}
