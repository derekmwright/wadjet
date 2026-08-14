package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/citc-tech/wadjet/internal/alerts"
	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/dataplane"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/telemetry"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/scan"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Config holds coordinator configuration.
type Config struct {
	NATSUrl        string
	ResultBucket   string
	MaxInflight    int           // max concurrent queries, 0 = default (64)
	QueryTimeout   time.Duration // max time for a query to complete, 0 = default (30m)
	WorkerStaleTTL time.Duration // time after which a silent worker is reaped, 0 = default (30s)
	DynamicFilters bool          // Trino-style semi-join dynamic-filter pushdown (off by default in v1)
	// GatherResultBudget caps the decoded result bytes a single query may
	// hold in coordinator heap (gather receiver). 0 = derive from
	// GOMEMLIMIT (half of it) or fall back to 2 GiB; negative = uncapped.
	// Past the budget the remaining gather payload spills as raw frames to
	// local scratch and the SQLResult replays them lazily disk→wire; only
	// a scratch-write failure fails the query (cleanly — the coordinator
	// process never OOMs for one query's result size).
	GatherResultBudget int64
	// LocalFastPathBytes routes queries whose total post-pruning catalog
	// scan bytes stay under this threshold onto the coordinator-local
	// single-process pipeline, skipping task dispatch and per-stage
	// object-store materialization. <=0 = disabled (every query runs the
	// distributed DAG — the zero value keeps library/test semantics
	// unchanged). `wadjet serve` enables it by default via its flag
	// (DefaultLocalFastPathBytes).
	LocalFastPathBytes int64
	// BroadcastBytesOverride, when non-zero, replaces the cluster-derived
	// broadcast threshold (broadcastThresholdFromCluster): >0 = fixed byte
	// threshold, <0 = never broadcast (every join takes the hash-shuffle /
	// sort-merge path). 0 = derive from worker pool budget (default).
	// Primarily a benchmarking/debugging surface: forcing the shuffled-join
	// path at small scale is how the sort-merge join gate gets exercised
	// before data is big enough to defeat broadcast on its own.
	BroadcastBytesOverride int64
	// SortMergeJoinBytes routes inner equi-joins whose sides BOTH exceed
	// this estimated size through the sort-merge join instead of the hash
	// join (docs/design/sort-merge-join.md), on the local fast path and in
	// the distributed stage DAG alike (the join stage swaps operator; its
	// exchange children are identical). 0 = disabled (default, dormant).
	SortMergeJoinBytes int64
	// LateMaterialization emits inner/left hash-join output as view
	// (dictionary) columns with the gather deferred to first touch
	// (docs/design/late-materialization.md), on the local fast path and in
	// worker fragments alike (rides the join-probe OpSpec). Off by default.
	LateMaterialization bool
	// SkewSplit enables adaptive skew-aware task layout for shuffled hash
	// joins (docs/design/skew-aware-shuffle.md): hot partition groups —
	// detected from the worker-reported per-partition shuffle output bytes
	// — split into k sub-tasks that divide the group's probe files and
	// replicate its build files, bounding the straggler task's input and
	// memory footprint. Decision logic in skew_split.go. The wadjet CLI
	// and tpch-bench default this ON (2026-07-11 SF10 A/B: −41% straggler
	// wall on the hot-key fixture, plan-identical on uniform workloads via
	// the ratio gate); this struct field's zero value stays false so
	// embedded/test constructors opt in explicitly.
	SkewSplit bool
	// AggPartialSplit enables the round-robin partial-aggregate fan-out
	// (aggregatePartialSplit in execute_stage_dag.go): partial "aggregate"
	// stages over a non-trivial multi-file upstream split into at most
	// workerCount tasks aggregating disjoint file slices. The wadjet CLI
	// and tpch-bench default this ON; --agg-partial-split=false is the
	// kill switch. Zero value stays false so embedded/test constructors
	// opt in explicitly (mirrors SkewSplit).
	AggPartialSplit bool
	// EagerDispatch enables eager consumer dispatch (docs/design/
	// eager-consumer-dispatch.md): the coordinator republishes per-
	// producer-task file manifests on EagerManifestSubject as shuffle
	// tasks complete, and (Phase C1) dispatches eligible consumer stages
	// before their producer stage fully drains. Zero value false —
	// default off until SF100 validation; requires StreamingExchange.
	EagerDispatch bool
	// StreamingExchange annotates dispatched tasks with peer-location
	// hints (Task.InputLocations) and per-query fetch tokens so consumers
	// stream stage outputs from the producing workers' local disk instead
	// of S3 (Phase A, docs/design/streaming-exchange.md). Purely additive:
	// hints only reference workers that advertise a PeerAddr, every fetch
	// failure falls through to the unchanged S3 read path, and the write
	// path (synchronous upload before stage completion) is untouched.
	// Default false: dormant, no hints, no tokens.
	StreamingExchange bool
	// ShuffleDurability is the stage-output upload policy stamped on
	// dispatched stage/shuffle tasks (docs/design/shuffle-durability.md).
	// Eager (zero value) starts every background S3 upload immediately —
	// the pre-knob behavior. Lazy queues uploads unstarted on the workers
	// and releases them only on demand (a consumer missing an input whose
	// producer is alive, a coordinator-side stage read, or worker drain);
	// scratch a query finishes without ever needing durably is elided.
	// Off never uploads scratch: producer death degrades to the one-shot
	// streaming-disabled re-execution (the ErrInputLost fallback), and
	// draining a worker mid-query loses its outputs the same way.
	// Stages whose outputs the coordinator itself reads (scalar-subquery
	// producers) always stay eager — the coordinator has no peer tier.
	// Only meaningful with StreamingExchange (the peer tier is what makes
	// the durable copy optional).
	ShuffleDurability distributed.UploadPolicy
	// LocalityPlacement places a task whose peer-location hints all point
	// at one connected worker onto that worker (docs/design/locality-
	// placement.md): 1:1 stage chains read their whole input set via
	// same-worker mmap instead of peer gRPC streams. Requires
	// StreamingExchange (the hint source) and the gRPC data plane
	// (targeted dispatch). Zero value false — default off until SF100
	// validation (mirrors EagerDispatch).
	LocalityPlacement bool
}

// SetAuthProvider wires ABAC enforcement into ExecuteSQL: with a provider
// set, every query is policy-checked at plan level (table denial, row
// filters, column deny/mask) for the identity in the request context — the
// same auth.EnforcePlanPolicies the embedded engine applies. Call before
// serving traffic (same contract as the other Set<X>-before-Start setters).
func (c *Coordinator) SetAuthProvider(p *auth.Provider) {
	c.authProvider = p
}

// EnforcesABAC reports whether ExecuteSQL enforces access policies itself.
// pgwire uses this to decide that routing authed connections through the
// coordinator is safe (canBypassDB).
func (c *Coordinator) EnforcesABAC() bool {
	return c.authProvider != nil && c.authProvider.Enabled()
}

// gatherResultBudget resolves Config.GatherResultBudget: explicit value,
// half of GOMEMLIMIT when one is set, else 2 GiB. Negative config = uncapped.
func (c *Coordinator) gatherResultBudget() int64 {
	if c.config.GatherResultBudget != 0 {
		if c.config.GatherResultBudget < 0 {
			return 0
		}
		return c.config.GatherResultBudget
	}
	if lim := debug.SetMemoryLimit(-1); lim > 0 && lim < math.MaxInt64 {
		return lim / 2
	}
	return 2 << 30
}

// queryMeta stores per-query metadata needed for later result retrieval.
type queryMeta struct {
	stages             []physical.Stage
	planStr            string
	sqlText            string // original SQL for pipeline tasks
	identityName       string // caller identity for task propagation
	identityRole       string
	trace              distributed.TraceContext // distributed tracing context
	policyDecisionJSON json.RawMessage          // pre-evaluated ABAC decisions for worker enforcement
	mergeInfo          *logical.MergeInfo       // non-nil for probe-split queries needing merge
	// prebuiltTasks, if non-nil for a given stage, supplies the publish loop's
	// task list instead of calling createTasksForStage. Set by the shuffle path
	// where each worker's task carries different PreScannedInputs.
	prebuiltTasks map[string][]distributed.Task
}

// Coordinator accepts queries, plans them, dispatches tasks, and tracks results.
type Coordinator struct {
	config     Config
	catalog    *catalog.Catalog
	nc         *nats.Conn
	js         jetstream.JetStream
	scheduler  *Scheduler
	tracker    *QueryTracker
	workers    *WorkerRegistry
	cleaner    *ResultCleaner
	leader     *LeaderElection     // nil = always leader (standalone mode)
	queryStore *QueryStateStore    // nil = no persistence (standalone mode)
	resultKV   jetstream.KeyValue  // NATS KV for fast inter-stage result transfer (nil = S3 only)
	otel       *telemetry.Provider // nil = no OTel tracing
	logger     *slog.Logger

	// BuildCacheThreshold overrides the default build cache threshold (bytes).
	// Zero means use the default (2GB). Exported for testing with small datasets.
	BuildCacheThreshold int64

	mu         sync.Mutex
	resultSubs map[string]context.CancelFunc // queryID -> cancel
	queryMetas map[string]*queryMeta         // queryID -> metadata for result retrieval
	querySem   chan struct{}                 // limits concurrent inflight queries
	localSem   chan struct{}                 // limits concurrent local fast-path executions
	localHits  atomic.Int64                  // queries served by the local fast path
	localBails atomic.Int64                  // local runs aborted over result budget, re-dispatched as DAG
	// localResultBudgetOverride replaces localResultBudget's derivation in
	// tests (0 = derive from the routing threshold).
	localResultBudgetOverride int64

	// authProvider, when set (SetAuthProvider), makes ExecuteSQL enforce
	// ABAC at plan level (auth.EnforcePlanPolicies) for identities in ctx.
	// nil = no coordinator-side enforcement; callers must gate routing
	// (pgwire canBypassDB) or pre-enforce (HTTP row-filter context).
	authProvider *auth.Provider

	// Alert scheduler fields (see alerts.go for lifecycle methods).
	alertScheduler       *alerts.Scheduler
	alertSchedulerCancel context.CancelFunc
	alertsEnabled        bool

	// Catalog snapshot fields (see catalog_snapshot.go for lifecycle methods).
	catalogSnapshotOpts     catalog.SnapshotOptions
	catalogSnapshotInterval time.Duration
	catalogSnapshotCancel   context.CancelFunc
	catalogSnapshotWG       sync.WaitGroup

	// dpSrv is the optional data-plane gRPC server. When non-nil, gather
	// receivers are registered as ResultHandlers so workers can stream
	// results over gRPC instead of NATS. nil = NATS-only delivery.
	dpSrv *dataplane.Server

	// peerFiles is the streaming-exchange location/token registry (nil
	// unless Config.StreamingExchange). Fed by noteTaskResult; drained by
	// the scheduler's task annotator and cleanupQuery.
	peerFiles *peerFileRegistry
	// uploadCompleteSub feeds peerFiles durability bits (Phase B).
	uploadCompleteSub *nats.Subscription
	// streamingDisabled holds root query IDs running the one-shot
	// ErrInputLost re-execution (pure S3 semantics: no hints, no async).
	streamingDisabled sync.Map
	// streamingReruns counts ErrInputLost re-executions (observability —
	// a non-zero rate is the signal to revisit single-level producer
	// re-run per the design memo).
	streamingReruns atomic.Int64
	// coordReadStages maps root query ID → set of stage IDs whose outputs
	// the coordinator reads directly (scalar-subquery producers,
	// fetchStageOutputData). Those stages' tasks keep eager uploads under
	// any ShuffleDurability policy: the coordinator has no peer tier, so
	// S3 is its only read path. Registered by executeStageDAG before
	// dispatch; dropped in cleanupQuery.
	coordReadStages sync.Map

	// Eager consumer dispatch (docs/design/eager-consumer-dispatch.md,
	// eager_feed.go). eagerFeeds maps eagerFeedKey → feed, created lazily
	// by consumer goroutines and producer dispatchers, dropped per query.
	// eagerStageSlot bounds in-flight eager consumer stages to ONE per
	// coordinator (v1 producer-lane reservation, stricter than the memo's
	// per-query bound): combined with numTasks ≤ workerCount eligibility
	// and the scheduler's eager-spread placement, each worker holds at
	// most one manifest-blocked task, so producers always keep
	// MaxConcurrent−1 lanes. Relax to per-query after C2 if concurrent-
	// query overlap matters.
	eagerFeedsMu   sync.Mutex
	eagerFeeds     map[string]*eagerFeed
	eagerStageSlot chan struct{}
}

// New creates a new Coordinator.
func New(cfg Config, cat *catalog.Catalog, nc *nats.Conn, js jetstream.JetStream, logger *slog.Logger) *Coordinator {
	if logger == nil {
		logger = slog.Default()
	}
	maxInflight := cfg.MaxInflight
	if maxInflight <= 0 {
		maxInflight = 64
	}
	c := &Coordinator{
		config:     cfg,
		catalog:    cat,
		nc:         nc,
		js:         js,
		scheduler:  NewScheduler(nc, logger),
		tracker:    NewQueryTracker(),
		workers:    NewWorkerRegistry(nc, logger, cfg.WorkerStaleTTL),
		logger:     logger,
		resultSubs: make(map[string]context.CancelFunc),
		queryMetas: make(map[string]*queryMeta),
		querySem:   make(chan struct{}, maxInflight),
		localSem:   make(chan struct{}, defaultLocalFastPathConcurrency),
		eagerFeeds: make(map[string]*eagerFeed),
		// v1 bound: one eager consumer stage in flight per coordinator
		// (see the field comment).
		eagerStageSlot: make(chan struct{}, 1),
	}
	// Memory-aware gRPC placement: the scheduler bin-packs estimated task
	// footprints against heartbeat pool stats. No-op on the NATS path.
	c.scheduler.SetWorkerRegistry(c.workers)
	// Input-locality placement rides the streaming-exchange hints; without
	// them no task ever carries InputLocations and the tier is inert.
	if cfg.LocalityPlacement && cfg.StreamingExchange {
		c.scheduler.SetLocalityPlacement(true)
	}

	// Streaming exchange (Phase A): annotate every dispatched task — initial
	// and retried alike, since retries re-enter PublishTasks — with peer
	// location hints and the query's fetch token. Phase B: track upload
	// durability from worker UploadComplete notifications.
	if cfg.StreamingExchange {
		c.peerFiles = newPeerFileRegistry()
		// Reap grace: the reaper defers reaping a silent worker while it
		// holds the only copy of stage outputs (pending background
		// uploads), bounded by the grace window. Wired here, before any
		// StartReaper caller runs (setter-before-start).
		c.workers.PendingNonDurable = c.peerFiles.PendingNonDurableFor
		c.scheduler.SetTaskAnnotator(c.annotateTaskPeerLocations)
		if nc != nil {
			sub, subErr := nc.Subscribe(distributed.SubjectUploadComplete, func(msg *nats.Msg) {
				var uc distributed.UploadComplete
				if err := distributed.Unmarshal(msg.Data, &uc); err != nil {
					return
				}
				if uc.Failed {
					c.logger.Warn("worker abandoned background uploads; keys stay non-durable",
						"root", uc.RootQueryID, "task_id", uc.TaskID, "worker", uc.WorkerID, "keys", len(uc.Keys))
					return
				}
				c.peerFiles.MarkDurable(uc.Keys)
			})
			if subErr != nil {
				logger.Warn("upload-complete subscription failed; durability bits stay conservative", "error", subErr)
			} else {
				c.uploadCompleteSub = sub
			}
		}
	}

	// NATS KV result cache: coordinator writes inline results here instead of S3.
	// Workers already read from this bucket (tier 2 in getFileData), so this
	// eliminates the S3 round-trip at stage boundaries for small results.
	if js != nil {
		kv, kvErr := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{
			Bucket:   "wadjet_results_data",
			TTL:      5 * time.Minute,
			MaxBytes: 1024 * 1024 * 1024, // 1 GB total
			// FileStorage (was MemoryStorage): for SF10+ workloads, the
			// in-memory KV bucket added ~1 GB of non-Go RSS that pushed the
			// coord process past the OS limit even when the Go heap was
			// well-bounded by background GC. FileStorage keeps the fast-path
			// semantics (TTL, fast key lookup) but pages bytes to disk, so
			// the bucket's MaxBytes no longer pins resident memory.
			// Latency hit is one disk seek per stage boundary on the slow
			// path — KV cache hits stay in OS page cache.
			Storage: jetstream.FileStorage,
		})
		if kvErr == nil {
			c.resultKV = kv
			logger.Info("coordinator NATS KV result cache enabled", "bucket", "wadjet_results_data")
		} else {
			logger.Debug("coordinator NATS KV result cache unavailable, using S3 only", "error", kvErr)
		}
	}

	return c
}

// SetDataPlaneServer enables gRPC result delivery, task dispatch, and
// progress signaling. When set:
//   - gather receivers register as ResultHandlers so workers can stream
//     ResultBatch messages directly instead of via NATS (Phase B),
//   - the scheduler routes TaskDispatch over the gRPC stream instead of
//     NATS publish (Phase C),
//   - the WorkerRegistry installs a global TaskProgress handler that
//     treats every progress arrival as a liveness signal (Phase E), and
//     per-query stage bridges register for stage-progress fanout
//     (also Phase E, in newStageProgressBridge).
//
// Must be called before any query runs. Pass nil (or skip) to use the
// NATS-only path.
func (c *Coordinator) SetDataPlaneServer(srv *dataplane.Server) {
	c.dpSrv = srv
	c.scheduler.SetDataPlaneServer(srv)
	c.workers.SetDataPlaneServer(srv)
}

// Workers returns the worker registry for inspecting active workers.
func (c *Coordinator) Workers() *WorkerRegistry {
	return c.workers
}

// noteTaskResult records the cross-cutting bookkeeping every stage result
// subscription performs on a ResultNotification: the publishing worker is
// alive (multi-signal liveness), the task's per-task liveness entry is
// done (it must stop scanning as stuck), and the scheduler's in-flight
// admission estimate for it is released.
func (c *Coordinator) noteTaskResult(r distributed.ResultNotification) {
	c.workers.MarkWorkerSeen(r.WorkerID)
	if c.workers.Liveness != nil {
		c.workers.Liveness.Remove(r.TaskID)
	}
	c.scheduler.TaskDone(r.TaskID)
	// Streaming exchange: the worker that reported these files wrote them
	// and adopted them into its local stage cache — record it as the peer
	// to fetch them from. Retries record the winning attempt's worker.
	if c.peerFiles != nil && r.Success {
		c.peerFiles.Record(r.ResultFiles, r.WorkerID)
		c.peerFiles.RecordPending(r.UploadPendingKeys)
	}
}

// SetTelemetry enables OpenTelemetry tracing on the coordinator.
func (c *Coordinator) SetTelemetry(tp *telemetry.Provider) {
	c.otel = tp
}

// Cleaner returns the result cleaner, creating it if needed.
func (c *Coordinator) Cleaner(store objstore.Store, bucket string) *ResultCleaner {
	if c.cleaner == nil {
		c.cleaner = NewResultCleaner(store, bucket, 0, c.logger)
		c.cleaner.SetActiveQueriesFunc(c.tracker.ActiveQueryIDs)
	}
	return c.cleaner
}

// SetLeaderElection attaches a leader election instance to the coordinator.
// When set, the coordinator will only accept queries if it is the current leader.
// If nil (default), the coordinator is always considered leader (standalone mode).
func (c *Coordinator) SetLeaderElection(le *LeaderElection) {
	c.leader = le
}

// SetQueryStateStore attaches a query state store for HA persistence.
// When set, query state transitions are persisted to NATS KV so a new leader
// can recover in-flight queries after failover.
func (c *Coordinator) SetQueryStateStore(qs *QueryStateStore) {
	c.queryStore = qs
}

// isLeaderOrStandalone returns true if this coordinator can accept queries.
// Returns true in standalone mode (no leader election) or if elected leader.
func (c *Coordinator) isLeaderOrStandalone() bool {
	if c.leader == nil {
		return true // standalone mode
	}
	return c.leader.IsLeader()
}

// RecoverQueries is called when this coordinator becomes leader after a failover.
// It reads active query states from the store and logs them for manual or
// automated recovery.
func (c *Coordinator) RecoverQueries(ctx context.Context) error {
	if c.queryStore == nil {
		return nil
	}

	active, err := c.queryStore.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("listing active queries for recovery: %w", err)
	}

	if len(active) == 0 {
		c.logger.Info("no active queries to recover after failover")
		return nil
	}

	for _, q := range active {
		c.logger.Warn("found orphaned query after failover",
			"query_id", q.ID, "status", q.Status, "sql", q.SQL,
			"started_at", q.StartedAt, "leader_id", q.LeaderID)
		q.Status = "failed"
		if err := c.queryStore.Save(ctx, q); err != nil {
			c.logger.Error("failed to mark orphaned query as failed",
				"query_id", q.ID, "error", err)
		}
	}

	c.logger.Info("failover recovery complete", "orphaned_queries", len(active))
	return nil
}

// StartLeaderWatch starts a background goroutine that watches for leadership
// changes and triggers recovery when this coordinator becomes leader.
func (c *Coordinator) StartLeaderWatch(ctx context.Context) {
	if c.leader == nil {
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case isLeader := <-c.leader.LeaderChanged():
				if isLeader {
					c.logger.Info("leadership acquired, starting recovery")
					if err := c.RecoverQueries(ctx); err != nil {
						c.logger.Error("failover recovery failed", "error", err)
					}
					c.StartAlertScheduler(ctx)
					c.StartCatalogSnapshotLoop(ctx)
				} else {
					c.logger.Warn("leadership lost, queries will fail on this instance")
					c.StopAlertScheduler()
					c.StopCatalogSnapshotLoop()
				}
			}
		}
	}()
}

// saveQueryState persists query state (best-effort, no-op if store is nil).
func (c *Coordinator) saveQueryState(ctx context.Context, queryID, sql, status string, completedStages []string) {
	if c.queryStore == nil {
		return
	}
	leaderID := ""
	if c.leader != nil {
		leaderID = c.leader.id
	}
	state := &PersistentQueryState{
		ID:              queryID,
		SQL:             sql,
		CompletedStages: completedStages,
		Status:          status,
		LeaderID:        leaderID,
		StartedAt:       time.Now(),
	}
	if err := c.queryStore.Save(ctx, state); err != nil {
		c.logger.Warn("failed to save query state", "query_id", queryID, "error", err)
	}
}

// deleteQueryState removes a query from the state store (best-effort).
func (c *Coordinator) deleteQueryState(ctx context.Context, queryID string) {
	if c.queryStore == nil {
		return
	}
	if err := c.queryStore.Delete(ctx, queryID); err != nil {
		c.logger.Warn("failed to delete query state", "query_id", queryID, "error", err)
	}
}

// persistStageCompletion updates the persisted query state with a newly completed stage.
func (c *Coordinator) persistStageCompletion(ctx context.Context, queryID, completedStageID string) {
	if c.queryStore == nil {
		return
	}
	state, err := c.queryStore.Get(ctx, queryID)
	if err != nil {
		return
	}
	state.CompletedStages = append(state.CompletedStages, completedStageID)
	if err := c.queryStore.Save(ctx, state); err != nil {
		c.logger.Warn("failed to persist stage completion",
			"query_id", queryID, "stage_id", completedStageID, "error", err)
	}
}

// QueryResult represents the outcome of a query execution.
type QueryResult struct {
	QueryID     string        `json:"query_id"`
	State       string        `json:"state"`
	ResultFiles []string      `json:"result_files,omitempty"`
	TotalRows   int64         `json:"total_rows"`
	Elapsed     time.Duration `json:"elapsed"`
	Error       string        `json:"error,omitempty"`
}

// SubmitScanQuery submits a simple scan query for distributed execution.
// This is the primary entry point before the SQL planner is available.
func (c *Coordinator) SubmitScanQuery(ctx context.Context, tableName string, columns []string, partFilter map[string]string) (*QueryResult, error) {
	if !c.isLeaderOrStandalone() {
		leaderID := ""
		if c.leader != nil {
			leaderID = c.leader.CurrentLeader(ctx)
		}
		return nil, fmt.Errorf("not leader: coordinator %s is leader", leaderID)
	}

	// Build SQL from scan parameters and delegate to ExecuteSQL.
	colList := "*"
	if len(columns) > 0 {
		colList = strings.Join(columns, ", ")
	}
	sql := fmt.Sprintf("SELECT %s FROM %s", colList, tableName)
	if len(partFilter) > 0 {
		var clauses []string
		for k, v := range partFilter {
			clauses = append(clauses, fmt.Sprintf("%s = '%s'", k, v))
		}
		sql += " WHERE " + strings.Join(clauses, " AND ")
	}

	result, err := c.ExecuteSQL(ctx, sql)
	// Only the result metadata is returned here; release the batches (and
	// any spill-backed stream) before they go out of scope.
	defer result.Close()
	if err != nil {
		return &QueryResult{
			QueryID: result.QueryID,
			State:   QueryStateFailed.String(),
			Error:   err.Error(),
		}, err
	}

	return &QueryResult{
		QueryID:     result.QueryID,
		State:       QueryStateCompleted.String(),
		ResultFiles: result.ResultFiles,
		TotalRows:   result.TotalRows,
		Elapsed:     result.Elapsed,
	}, nil
}

// SQLResult holds the result of a distributed SQL query.
// Results are kept columnar (as RecordBatches) to avoid materializing
// per-row map[string]any which causes massive heap pressure at SF10+.
//
// Results arrive in one of two forms: fully materialized in Batches, or
// as a lazy stream (gather results that exceeded the coordinator budget
// and spilled to local scratch — see gatherReceiver). Consumers should
// iterate via Stream(), which handles both forms; whoever receives an
// SQLResult owns it and must either drain the stream or call Close, or
// spill scratch leaks until process exit.
type SQLResult struct {
	QueryID     string
	Columns     []string
	Batches     []*batch.RecordBatch
	ResultFiles []string
	TotalRows   int64
	Elapsed     time.Duration
	Plan        string
	Error       string

	// stream is the lazy form: set (and Batches nil) when the gather
	// result is partially on local scratch. Accessed via Stream().
	stream BatchStream
}

// Stream returns a consuming iterator over the result batches. The first
// call detaches the result's batches (lazy stream or materialized slice);
// subsequent calls return an empty stream. The caller owns the returned
// stream and must drain it or call Close.
func (r *SQLResult) Stream() BatchStream {
	if r == nil {
		return newSliceStream(nil)
	}
	if r.stream != nil {
		s := r.stream
		r.stream = nil
		return s
	}
	b := r.Batches
	r.Batches = nil
	return newSliceStream(b)
}

// Close releases whatever the result still holds — the lazy stream's
// buffered batches and spill scratch, or the materialized slice.
// Idempotent and nil-safe.
func (r *SQLResult) Close() error {
	if r == nil {
		return nil
	}
	var err error
	if r.stream != nil {
		err = r.stream.Close()
		r.stream = nil
	}
	r.Batches = nil
	return err
}

// Rows materializes the result batches into row-oriented maps. This is
// expensive for large results — prefer Stream(). On a materialized result
// it is repeatable (Batches are left in place, matching the historical
// behavior); on a lazy result it consumes the stream, and a second call
// returns nothing.
func (r *SQLResult) Rows() ([]map[string]any, error) {
	if r == nil {
		return nil, nil
	}
	if r.stream == nil {
		var rows []map[string]any
		for _, b := range r.Batches {
			rows = append(rows, b.ToRows()...)
		}
		return rows, nil
	}
	s := r.Stream()
	defer s.Close()
	var rows []map[string]any
	for {
		b, err := s.Next(context.Background())
		if err != nil {
			return rows, err
		}
		if b == nil {
			return rows, nil
		}
		rows = append(rows, b.ToRows()...)
	}
}

// ExecuteSQL parses SQL, plans, distributes across workers, and collects results.
func (c *Coordinator) ExecuteSQL(ctx context.Context, sql string) (*SQLResult, error) {
	if !c.isLeaderOrStandalone() {
		leaderID := ""
		if c.leader != nil {
			leaderID = c.leader.CurrentLeader(ctx)
		}
		return nil, fmt.Errorf("not leader: coordinator %s is leader", leaderID)
	}

	// Backpressure: limit concurrent inflight queries.
	select {
	case c.querySem <- struct{}{}:
		defer func() { <-c.querySem }()
	case <-ctx.Done():
		return nil, fmt.Errorf("query queue full: %w", ctx.Err())
	}

	start := time.Now()
	queryID := uuid.New().String()[:8]

	// Start OTel span for the query if tracing is enabled
	if c.otel != nil {
		var span trace.Span
		ctx, span = c.otel.StartSpan(ctx, "coordinator.ExecuteSQL",
			attribute.String("query.id", queryID),
			attribute.String("query.sql", sql),
		)
		defer func() {
			span.End()
		}()
	}

	// Parse
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	// Dispatch snapshot DDL — returns a populated result row.
	if parsed.Type == plansql.QueryCreateSnapshot {
		return c.handleCreateSnapshotSQL(ctx)
	}

	// Dispatch alert DDL before attempting SELECT extraction.
	switch parsed.Type {
	case plansql.QueryCreateAlert:
		return nil, c.handleCreateAlertSQL(ctx, sql)
	case plansql.QueryDropAlert:
		return nil, c.handleDropAlertSQL(ctx, sql)
	case plansql.QueryAlterAlert:
		return nil, c.handleAlterAlertSQL(ctx, sql)
	}

	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil, fmt.Errorf("extract: %w", err)
	}

	// Reject references to columns that resolve to no source (plan-time name binding).
	if err := physical.NewPlanner(c.catalog).ValidateColumns(ctx, selectInfo); err != nil {
		return nil, err
	}

	// Build logical plan
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		return nil, fmt.Errorf("logical plan: %w", err)
	}

	// Annotate scan columns and optimize — pass scan annotator for IN decorrelation
	scanAnnotator := func(plan *logical.Node) {
		physical.NewPlanner(c.catalog).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)

	// ABAC enforcement at plan level — the same auth.EnforcePlanPolicies
	// the embedded engine applies (table denial, row filters, column
	// deny/mask), so identities see identical policy behavior on the
	// distributed and local-fast-path executions. Runs BEFORE Optimize so
	// every downstream consumer (PlanDistributed and tryLocalFastPath)
	// sees the enforced plan.
	if c.EnforcesABAC() {
		logicalPlan, err = auth.EnforcePlanPolicies(ctx, c.authProvider, selectInfo, logicalPlan, "coordinator")
		if err != nil {
			return nil, err
		}
	} else if rowFilters := auth.RowFiltersFromContext(ctx); len(rowFilters) > 0 {
		// Legacy path for deployments without a provider wired into the
		// coordinator: the HTTP server pre-evaluates policies and passes
		// row filters through context.
		for table, filter := range rowFilters {
			logicalPlan = logical.InjectRowFilter(logicalPlan, table, filter)
		}
	}

	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)
	planStr := logicalPlan.PrettyPrint(0)

	// Small-query fast path: when the plan's total post-pruning scan bytes
	// stay under the routing threshold, execute in-process on the
	// coordinator instead of dispatching a stage DAG. The DAG's fixed
	// costs (task dispatch, object-store materialization per stage
	// boundary) dominate small queries; the local pipeline answers in
	// milliseconds. Both paths consume the identical optimized logical
	// plan, so results and policy enforcement match by construction. Any
	// local failure falls through to the DAG.
	if res, handled := c.tryLocalFastPath(ctx, queryID, logicalPlan, planStr, start); handled {
		return res, nil
	}

	planner := physical.NewPlanner(c.catalog)
	planner.WorkerCount = c.workers.Count()
	planner.BroadcastBytesThreshold = broadcastThresholdFromCluster(c.workers.MinWorkerPoolBudget())
	if c.config.BroadcastBytesOverride != 0 {
		planner.BroadcastBytesThreshold = c.config.BroadcastBytesOverride
	}
	planner.SortMergeJoinBytes = c.config.SortMergeJoinBytes
	planner.LateMaterialization = c.config.LateMaterialization
	planner.DynamicFiltersEnabled = c.config.DynamicFilters
	physStages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		return nil, fmt.Errorf("physical plan: %w", err)
	}

	// Streaming-disabled re-execution (Phase B): a ctx flagged by the
	// ErrInputLost rerun below pins this query ID into the disabled set so
	// the task annotator emits no hints/tokens and no AsyncUpload — pure
	// synchronous-S3 semantics for the whole run.
	if streamingExchangeDisabled(ctx) {
		c.streamingDisabled.Store(queryID, struct{}{})
		defer c.streamingDisabled.Delete(queryID)
	}

	c.logger.Info("routing to native DAG executor",
		"query", queryID, "stages", len(physStages))
	gr, gerr := c.executeStageDAG(ctx, queryID, sql, physStages, c.workers.Count())
	if gerr != nil {
		// ErrInputLost: a producer died before its background upload landed
		// and its output is unrecoverable by task retry. Reads are
		// idempotent and nothing has reached the client (ExecuteSQL returns
		// once), so re-execute the whole query ONCE with streaming exchange
		// disabled — the fast-path bail-out pattern one level up. The ctx
		// flag caps the depth at one.
		if IsInputLostErr(gerr) && c.peerFiles != nil && !streamingExchangeDisabled(ctx) {
			c.streamingReruns.Add(1)
			c.logger.Warn("streaming-exchange input lost; re-executing query with streaming disabled",
				"query", queryID, "error", gerr)
			return c.ExecuteSQL(withStreamingExchangeDisabled(ctx), sql)
		}
		return &SQLResult{
			QueryID: queryID,
			Error:   gerr.Error(),
			Elapsed: time.Since(start),
			Plan:    planStr,
		}, fmt.Errorf("native DAG: %w", gerr)
	}
	// #163: walkStages passes NodeDistinct through (no dedup stage), so a
	// distributed SELECT DISTINCT arrives here un-deduplicated. The gather
	// result is already projected to the SELECT list (applyOutputRenames), so
	// deduplicate it now — then re-apply ORDER BY / LIMIT, since dedup does not
	// preserve order. Single-node dedup (the same shape the probe-split path
	// uses via mergeProbePartials); a sharded in-DAG dedup is the follow-up.
	if gr != nil {
		if mi := logical.ExtractMergeInfo(logicalPlan); mi != nil && mi.HasDistinct {
			if err := c.dedupGatherResult(gr, mi); err != nil {
				return &SQLResult{
					QueryID: queryID,
					Error:   err.Error(),
					Elapsed: time.Since(start),
					Plan:    planStr,
				}, fmt.Errorf("distinct dedup: %w", err)
			}
		}
	}
	res := &SQLResult{
		QueryID:   queryID,
		Columns:   gr.columns,
		TotalRows: gr.totalRows,
		Elapsed:   time.Since(start),
		Plan:      planStr,
	}
	if gr.spillPath != "" {
		// Over-budget result: the in-memory prefix plus raw frames on
		// local scratch, replayed lazily disk→wire as the consumer
		// iterates. PR #142's hard-fail became this graceful path.
		c.logger.Info("gather: result exceeded budget, replaying lazily from scratch",
			"query", queryID, "spill_path", gr.spillPath,
			"spilled_mb", gr.spillBytes>>20, "total_rows", gr.totalRows)
		res.stream = newGatherReplayStream(gr.batches, gr.spillPath, gr.renamer)
	} else {
		res.Batches = gr.batches
	}
	return res, nil
}

// createTasksForStage creates distributed tasks for a given stage.
// enrichTaskWithQueryContext propagates trace, identity, and policy fields from
// the query's metadata into a task. Call this on every task (prebuilt or not)
// before publishing, after qm is stored in c.queryMetas.
func (c *Coordinator) enrichTaskWithQueryContext(qm *queryMeta, t *distributed.Task) {
	if t.ClusterID == "" {
		t.ClusterID = c.catalog.ClusterID()
	}
	if qm == nil {
		return
	}
	t.IdentityName = qm.identityName
	t.IdentityRole = qm.identityRole
	t.TraceID = qm.trace.TraceID
	t.SpanID = qm.trace.SpanID
	t.TraceFlags = qm.trace.TraceFlags
	t.PolicyDecisionJSON = qm.policyDecisionJSON
}

func (c *Coordinator) createTasksForStage(queryID string, stage physical.Stage, depResults map[string][]string) []distributed.Task {
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)

	var tasks []distributed.Task
	switch stage.Type {
	case "pipeline":
		tasks = c.createPipelineTasks(queryID, stage, resultPrefix, depResults)
	default:
		c.logger.Error("unknown stage type", "type", stage.Type, "query_id", queryID)
		return nil
	}

	// Propagate cluster routing and identity context
	clusterID := stage.ClusterID
	c.mu.Lock()
	qm := c.queryMetas[queryID]
	c.mu.Unlock()
	for i := range tasks {
		if clusterID != "" {
			tasks[i].ClusterID = clusterID
		}
		c.enrichTaskWithQueryContext(qm, &tasks[i])
	}
	return tasks
}

// createPipelineTasks creates tasks that run the entire query as a pipeline.
// In probe-split mode, creates N tasks each with a subset of the probe table's
// files. Otherwise creates a single task for the whole query.
func (c *Coordinator) createPipelineTasks(queryID string, stage physical.Stage, resultPrefix string, depResults map[string][]string) []distributed.Task {
	c.mu.Lock()
	qm := c.queryMetas[queryID]
	c.mu.Unlock()

	sqlText := ""
	if qm != nil {
		sqlText = qm.sqlText
	}

	// Probe-split mode: create N tasks, each with a subset of the probe
	// table's files. Build tables are scanned in full by each worker.
	// When BuildCachePreScans is populated, large build tables are loaded from
	// pre-scanned S3 cache files via PreScannedInputs instead of scanning from
	// source — eliminating N× build-side duplication that causes OOM at SF100.
	if stage.ProbeSplitAlias != "" && len(stage.ProbeSplitFiles) > 0 && stage.Tasks > 1 {
		filePartitions := splitFilesEvenly(stage.ProbeSplitFiles, stage.Tasks)
		tasks := make([]distributed.Task, len(filePartitions))
		// Convert physical-plane PreComputedAggregateMeta into the wire type
		// once per task set; all probe-split tasks carry the same list.
		var precomp []distributed.PreComputedAggregate
		for _, m := range stage.PreComputedAggregates {
			specs := make([]distributed.AggSpec, len(m.AggSpecs))
			for i, s := range m.AggSpecs {
				specs[i] = distributed.AggSpec{Func: s.Func, InputCol: s.InputCol, OutputCol: s.OutputCol}
			}
			precomp = append(precomp, distributed.PreComputedAggregate{
				InputTable:  m.InputTable,
				GroupByCols: append([]string(nil), m.GroupByCols...),
				AggSpecs:    specs,
				CacheFiles:  append([]string(nil), m.CacheFiles...),
			})
		}
		for i, files := range filePartitions {
			tasks[i] = distributed.Task{
				ID:                    uuid.New().String()[:8],
				QueryID:               queryID,
				StageID:               stage.ID,
				Type:                  distributed.TaskTypePipeline,
				SQLText:               sqlText,
				DataBucket:            c.config.ResultBucket,
				ResultBucket:          c.config.ResultBucket,
				ResultPrefix:          resultPrefix,
				ScanFileFilter:        map[string][]string{stage.ProbeSplitAlias: files},
				PreScannedInputs:      stage.BuildCachePreScans,
				PreComputedAggregates: precomp,
				PartialAggregate:      qm != nil && qm.mergeInfo != nil && qm.mergeInfo.HasAggregate,
				CreatedAt:             time.Now(),
			}
		}
		return tasks
	}

	return []distributed.Task{{
		ID:           uuid.New().String()[:8],
		QueryID:      queryID,
		StageID:      stage.ID,
		Type:         distributed.TaskTypePipeline,
		SQLText:      sqlText,
		DataBucket:   c.config.ResultBucket,
		ResultBucket: c.config.ResultBucket,
		ResultPrefix: resultPrefix,
		CreatedAt:    time.Now(),
	}}
}

// splitFilesEvenly distributes files across n partitions as evenly as possible.
func splitFilesEvenly(files []string, n int) [][]string {
	if n <= 0 || len(files) == 0 {
		return nil
	}
	if n > len(files) {
		n = len(files)
	}
	parts := make([][]string, n)
	base := len(files) / n
	extra := len(files) % n
	offset := 0
	for i := 0; i < n; i++ {
		size := base
		if i < extra {
			size++
		}
		parts[i] = files[offset : offset+size]
		offset += size
	}
	return parts
}

// coalesceScanTargetBytes is the minimum total bytes per scan task.
// Files smaller than this are grouped together to reduce task overhead.
// Larger batches reduce NATS dispatch + S3 write overhead at the cost of
// slightly coarser work distribution. 32 MB balances overhead vs parallelism.
const coalesceScanTargetBytes int64 = 64 * 1024 * 1024 // 64 MB

func (c *Coordinator) cleanupQuery(queryID string) {
	// Collect NATS KV keys before dropping state — tracker still has paths.
	var kvKeys []string
	if c.resultKV != nil {
		for _, paths := range c.tracker.CollectResultPaths(queryID) {
			for _, p := range paths {
				kvKeys = append(kvKeys, natsKVKey(p))
			}
		}
	}

	c.mu.Lock()
	if cancel, ok := c.resultSubs[queryID]; ok {
		cancel()
		delete(c.resultSubs, queryID)
	}
	c.mu.Unlock()

	// Streaming exchange: drop the query's peer-location hints and fetch
	// token (nil-safe when disabled). Workers drop their side on the
	// complete/cancel broadcasts below.
	c.peerFiles.CleanupQuery(queryID)
	c.coordReadStages.Delete(queryID)

	// Broadcast cancellation FIRST: by the time cleanupQuery runs the query
	// is terminal (completed, failed, or timed out) and this function is
	// about to purge queries/<id>/* from the object store — so any task
	// still executing for it is pure waste whose uploads would recreate the
	// scratch leak. Workers mark the query cancelled (queued tasks Term at
	// pickup, running tasks abort within the ~500ms cancelTicker) and free
	// their local/broadcast caches. Without this, a query that FAILED
	// terminally left its in-flight tasks running to completion — at SF100
	// run 20260610-203304, Q21's tasks kept downloading multi-GB cache
	// files and writing partition outputs for minutes after the query
	// failed on ENOSPC, so the disk never recovered and Q22 died on Q21's
	// garbage. CancelSubject was previously only published by the
	// user-facing CancelQuery API, never on internal failure.
	if c.nc != nil {
		if err := c.nc.Publish(distributed.CancelSubject(queryID), []byte(queryID)); err != nil {
			c.logger.Debug("failed to publish query cancellation",
				"query_id", queryID, "error", err)
		}
	}

	// Notify workers that the query is finished so they can release per-
	// query state (LocalStageCache spill files). Best-effort: workers that
	// miss the message will eventually leak entries until process exit, but
	// the cache returns false on Put when full and falls through to S3.
	if c.nc != nil {
		if err := c.nc.Publish(distributed.CompleteSubject(queryID), []byte(queryID)); err != nil {
			c.logger.Debug("failed to publish query completion",
				"query_id", queryID, "error", err)
		}
	}

	// Purge KV entries async — frees NATS memory without blocking the caller.
	if len(kvKeys) > 0 {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			purged := 0
			for _, key := range kvKeys {
				if err := c.resultKV.Purge(ctx, key); err == nil {
					purged++
				}
			}
			if purged > 0 {
				c.logger.Debug("purged query KV entries",
					"query_id", queryID, "purged", purged, "total", len(kvKeys))
			}
		}()
	}

	// Delete query intermediates from the object store. Without this, every
	// completed query leaks its queries/<id>/* prefix and the data dir grows
	// unbounded across runs (~40 GB per full TPC-H SF1 sweep). The periodic
	// cleaner does eventually GC stale files via TTL, but its default 1-hour
	// TTL is too long for a back-to-back test cadence and there's no reason
	// to wait — we know the query is done. Async to keep cleanupQuery fast.
	if c.cleaner != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			n, err := c.cleaner.CleanQuery(ctx, queryID)
			if err != nil {
				c.logger.Warn("query intermediate cleanup failed",
					"query_id", queryID, "error", err)
			} else if n > 0 {
				c.logger.Debug("cleaned query intermediates",
					"query_id", queryID, "objects_deleted", n)
			}
		}()
	}
}

// queryReaperTTL is how long completed/failed/cancelled queries stay in memory
// before being reaped. This gives GetQueryResults time to be called for async queries.
const queryReaperTTL = 5 * time.Minute

// StartQueryReaper starts a background goroutine that periodically removes
// StartQueryActiveHandler subscribes to query-active check requests from workers.
// Workers ask "is query X still active?" before executing tasks pulled from
// JetStream, preventing wasted work on queries killed by the watchdog.
func (c *Coordinator) StartQueryActiveHandler() {
	c.nc.Subscribe(distributed.SubjectQueryActive, func(msg *nats.Msg) {
		queryID := string(msg.Data)
		info := c.tracker.Get(queryID)
		active := info != nil && (info.State == QueryStatePending || info.State == QueryStateRunning)
		if active {
			msg.Respond([]byte("1"))
		} else {
			msg.Respond([]byte("0"))
		}
	})
}

// StartQueryReaper periodically removes old entries for
// completed, failed, and cancelled queries from the tracker and queryMetas maps.
// This prevents unbounded memory growth from accumulated query metadata.
func (c *Coordinator) StartQueryReaper(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reaped := c.tracker.ReapCompleted(queryReaperTTL)
				if len(reaped) > 0 {
					c.mu.Lock()
					for _, id := range reaped {
						delete(c.queryMetas, id)
					}
					c.mu.Unlock()
					c.logger.Debug("reaped completed queries", "count", len(reaped))
				}
			}
		}
	}()
}

// natsKVKey converts an S3 result path to a valid NATS KV key.
// NATS KV keys don't support '.' so we replace with '_'.
func natsKVKey(path string) string {
	return strings.ReplaceAll(path, ".", "_")
}

// natsKVResultThreshold is the max result size written to NATS KV.
// Must match the worker's threshold so both sides agree on where data lives.
const natsKVResultThreshold = 4 * 1024 * 1024 // 4 MB — within NATS 8 MB max payload

// readFinalResults reads the result files from the final stage of a query.
// When fetchAll is true, all results are materialized (needed for probe-split merge).
func (c *Coordinator) readFinalResults(ctx context.Context, queryID string, stages []physical.Stage, fetchAll bool) ([]*batch.RecordBatch, []string, int64, error) {
	if len(stages) == 0 {
		return nil, nil, 0, nil
	}

	// Find the final stage (last in topological order)
	finalStage := stages[len(stages)-1]

	// Get results for the final stage
	results := c.tracker.StageResults(queryID, finalStage.ID)
	c.logger.Debug("readFinalResults", "query", queryID, "finalStage", finalStage.ID, "numResults", len(results))
	if len(results) == 0 {
		return nil, nil, 0, nil
	}

	// Separate inline results (need decompression) from S3-only results (just count rows).
	type inlineWork struct {
		idx  int
		data []byte
	}
	var pending []inlineWork
	var s3Rows int64

	type s3Fetch struct {
		idx  int
		path string
	}
	var s3Fetches []s3Fetch

	for i, r := range results {
		c.logger.Debug("result entry", "taskID", r.TaskID, "success", r.Success,
			"inlineLen", len(r.InlineData), "numRows", r.NumRows, "resultPath", r.ResultPath)
		if !r.Success {
			continue
		}
		if len(r.InlineData) == 0 {
			if fetchAll && r.ResultPath != "" {
				s3Fetches = append(s3Fetches, s3Fetch{idx: i, path: r.ResultPath})
			} else {
				s3Rows += r.NumRows
			}
			continue
		}
		pending = append(pending, inlineWork{idx: i, data: r.InlineData})
	}

	// Fetch S3-stored results for probe-split merge: bounded fan-out
	// (mirrors ReadResultFiles' maxConcurrent) and a running byte cap —
	// these are precisely the blobs that exceeded the inline threshold,
	// workers can't apply LIMIT, and the previous unbounded ReadAll
	// fan-out held every raw blob simultaneously, uncharged. Past the
	// budget the query fails cleanly instead of OOM-killing the process.
	budget := c.gatherResultBudget()
	if len(s3Fetches) > 0 {
		type fetchResult struct {
			data []byte
			err  error
		}
		fetchResults := make([]fetchResult, len(s3Fetches))
		var fetchedBytes atomic.Int64
		const maxConcurrentFetches = 8
		sem := make(chan struct{}, maxConcurrentFetches)
		var wg sync.WaitGroup
		for fi, sf := range s3Fetches {
			wg.Add(1)
			go func(i int, path string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				if budget > 0 && fetchedBytes.Load() > budget {
					fetchResults[i] = fetchResult{err: fmt.Errorf(
						"partial results exceeded the coordinator gather budget (%d MB): add a LIMIT, or raise the budget (Config.GatherResultBudget / GOMEMLIMIT)",
						budget>>20)}
					return
				}
				data, err := c.fetchResultData(ctx, queryID, path)
				fetchedBytes.Add(int64(len(data)))
				fetchResults[i] = fetchResult{data: data, err: err}
			}(fi, sf.path)
		}
		wg.Wait()
		for fi, sf := range s3Fetches {
			if fetchResults[fi].err != nil {
				return nil, nil, 0, fmt.Errorf("fetching result %s: %w", sf.path, fetchResults[fi].err)
			}
			pending = append(pending, inlineWork{idx: sf.idx, data: fetchResults[fi].data})
		}
	}

	if len(pending) == 0 {
		return nil, nil, s3Rows, nil
	}

	// Decompress and deserialize inline results concurrently
	type decoded struct {
		batches []*batch.RecordBatch
		columns []string
		rows    int64
	}
	slot := make([]decoded, len(pending))

	if len(pending) == 1 {
		b, cols, rows := c.decodeInlineResult(pending[0].data)
		slot[0] = decoded{batches: b, columns: cols, rows: rows}
	} else {
		var wg sync.WaitGroup
		for i, w := range pending {
			wg.Add(1)
			go func(idx int, data []byte) {
				defer wg.Done()
				b, cols, rows := c.decodeInlineResult(data)
				slot[idx] = decoded{batches: b, columns: cols, rows: rows}
			}(i, w.data)
		}
		wg.Wait()
	}

	var allBatches []*batch.RecordBatch
	var columns []string
	var decodedBytes int64
	totalRows := s3Rows
	for _, d := range slot {
		if len(d.columns) > 0 && len(columns) == 0 {
			columns = d.columns
		}
		totalRows += d.rows
		for _, b := range d.batches {
			decodedBytes += b.MemBytes()
		}
		allBatches = append(allBatches, d.batches...)
	}
	if budget > 0 && decodedBytes > budget {
		return nil, nil, 0, fmt.Errorf(
			"decoded partial results exceeded the coordinator gather budget (%d MB > %d MB): add a LIMIT, or raise the budget (Config.GatherResultBudget / GOMEMLIMIT)",
			decodedBytes>>20, budget>>20)
	}
	return allBatches, columns, totalRows, nil
}

// decodeInlineResult decompresses and deserializes a single inline result.
func (c *Coordinator) decodeInlineResult(data []byte) ([]*batch.RecordBatch, []string, int64) {
	inlineData, err := decompressShuffleData(data)
	if err != nil {
		c.logger.Debug("shuffle decompress error", "err", err)
		return nil, nil, 0
	}

	var batches []*batch.RecordBatch
	var columns []string
	if len(inlineData) >= 4 && string(inlineData[:4]) == "WSHF" {
		batches, err = readShuffleBatches(inlineData)
		if err != nil {
			c.logger.Debug("shuffle read error", "err", err)
			return nil, nil, 0
		}
		if len(batches) > 0 {
			columns = make([]string, len(batches[0].Schema))
			for i, col := range batches[0].Schema {
				columns[i] = col.Name
			}
		}
	} else {
		reader, err := parquet.NewReader(bytes.NewReader(inlineData), int64(len(inlineData)))
		if err != nil {
			c.logger.Debug("parquet reader error", "err", err)
			return nil, nil, 0
		}
		schema := reader.Schema().Columns
		if len(schema) > 0 {
			columns = make([]string, len(schema))
			for i, col := range schema {
				columns[i] = col.Name
			}
		}
		batches, err = scan.ReadFileBatches(reader, schema, nil)
		if err != nil {
			return nil, nil, 0
		}
	}

	var totalRows int64
	for _, b := range batches {
		totalRows += int64(b.ActiveLen())
	}
	return batches, columns, totalRows
}

// fetchResultData retrieves a result blob from NATS KV or S3.
// Used by probe-split merge when results exceed the inline threshold.
func (c *Coordinator) fetchResultData(ctx context.Context, queryID, path string) ([]byte, error) {
	// Try NATS KV first (fastest)
	if c.resultKV != nil {
		entry, err := c.resultKV.Get(ctx, natsKVKey(path))
		if err == nil {
			return entry.Value(), nil
		}
	}
	// Fall back to S3
	store := c.catalog.Store()
	reader, _, err := store.Get(ctx, c.config.ResultBucket, path)
	if err != nil {
		return nil, fmt.Errorf("fetching result from S3: %w", err)
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// mergeProbePartials re-aggregates partial results from probe-split pipeline
// workers and applies the original sort + limit. Each worker produced partial
// aggregates for its file partition; this merges them into the final result.
// Consumes (and always closes) the input stream.
//
// The DISTINCT-only path dedups streaming — each input batch is fed to the
// hash aggregate and released before the next is read. The re-aggregate path
// drains the stream up front: reAggregatePartials' group states pin their
// source batches anyway (mergedRow.sourceBatch back-pointers), so streaming
// its input would not reduce peak residency. Sort/limit inherently need the
// (already merged, small) result materialized.
func (c *Coordinator) mergeProbePartials(in BatchStream, columns []string, mi *logical.MergeInfo) ([]*batch.RecordBatch, int64, error) {
	defer in.Close()

	// Build column name → index mapping
	colIdx := make(map[string]int, len(columns))
	for i, col := range columns {
		colIdx[col] = i
	}

	var batches []*batch.RecordBatch
	if mi.HasAggregate {
		var err error
		batches, err = drainStream(in)
		if err != nil {
			return nil, 0, fmt.Errorf("reading partials: %w", err)
		}
		if len(batches) == 0 {
			return nil, 0, nil
		}
		if len(mi.GroupBy) > 0 {
			batches = c.reAggregatePartials(batches, columns, colIdx, mi)
		} else {
			// Scalar aggregate (no GROUP BY): merge N worker partials into 1 row.
			// Each worker returned 1 row with partial SUM/COUNT/MIN/MAX.
			batches = c.mergeScalarAggregates(batches, columns, colIdx, mi)
		}
	}
	if mi.HasDistinct {
		src := in
		if mi.HasAggregate {
			src = newSliceStream(batches)
		}
		var err error
		batches, err = c.deduplicatePartials(src, columns)
		if err != nil {
			return nil, 0, fmt.Errorf("deduplicating distinct partials: %w", err)
		}
	} else if !mi.HasAggregate {
		var err error
		batches, err = drainStream(in)
		if err != nil {
			return nil, 0, fmt.Errorf("reading partials: %w", err)
		}
	}

	// Apply sort + limit. When the limit is much smaller than the row count,
	// use a top-K heap select to avoid sorting the full result set.
	if len(mi.OrderBy) > 0 && mi.Limit > 0 && len(batches) == 1 && batches[0].Len > mi.Limit*4 {
		c.topKBatches(batches, columns, colIdx, mi.OrderBy, mi.Limit)
	} else {
		if len(mi.OrderBy) > 0 {
			c.sortBatches(batches, columns, colIdx, mi.OrderBy)
		}
		if mi.Limit > 0 {
			batches = limitBatches(batches, mi.Limit)
		}
	}

	var totalRows int64
	for _, b := range batches {
		totalRows += int64(b.ActiveLen())
	}
	return batches, totalRows, nil
}

// dedupGatherResult deduplicates a native-DAG gather result in place for
// SELECT DISTINCT (#163). The gather batches are already projected to the
// SELECT-list columns (applyOutputRenames), so deduplicatePartials' keys-only
// GroupByAll dedup over them yields the correct distinct set. The (possibly
// spilled) result is streamed through the dedup and replaced with the in-memory
// deduped batches; ORDER BY / LIMIT are re-applied afterward because the dedup
// does not preserve input order. Mirrors the dedup→sort→limit sequence the
// probe-split path runs in mergeProbePartials.
func (c *Coordinator) dedupGatherResult(gr *gatherResult, mi *logical.MergeInfo) error {
	var src BatchStream
	if gr.spillPath != "" {
		// Renamer applies to spilled frames only; the in-memory prefix is
		// already renamed (applyOutputRenames), so no double-rename.
		src = newGatherReplayStream(gr.batches, gr.spillPath, gr.renamer)
	} else {
		src = newSliceStream(gr.batches)
	}
	deduped, err := c.deduplicatePartials(src, gr.columns)
	if err != nil {
		return err
	}
	// The stream (incl. any spilled scratch) is consumed; the result is now a
	// small in-memory distinct set.
	gr.batches = deduped
	gr.spillPath = ""
	gr.spillBytes = 0
	gr.renamer = nil

	if len(mi.OrderBy) > 0 || mi.Limit > 0 {
		colIdx := make(map[string]int, len(gr.columns))
		for i, col := range gr.columns {
			colIdx[col] = i
		}
		if len(mi.OrderBy) > 0 {
			c.sortBatches(gr.batches, gr.columns, colIdx, mi.OrderBy)
		}
		if mi.Limit > 0 {
			gr.batches = limitBatches(gr.batches, mi.Limit)
		}
	}

	var total int64
	for _, b := range gr.batches {
		total += int64(b.ActiveLen())
	}
	gr.totalRows = total
	return nil
}

// deduplicatePartials removes duplicate rows across probe-split partial
// results. Consumes the input stream batch-by-batch — each batch's
// reference is dropped once the aggregate has absorbed it, so a lazy
// (spill-backed) input never materializes fully here.
func (c *Coordinator) deduplicatePartials(in BatchStream, columns []string) ([]*batch.RecordBatch, error) {
	defer in.Close()
	_ = columns // key set = every column, resolved from the batch schema
	// Columnar dedup via a keys-only hash aggregate (the same GroupByAll
	// machinery SELECT DISTINCT plans to). The previous implementation
	// boxed the entire merged result ×3 — ToRows maps per row (the pattern
	// documented holding 21 GB at SF10), an fmt-serialized full-row key
	// set, and a FromRows re-materialization — and its "%v\x00" key
	// collided for values containing NUL bytes. The aggregate dedups with
	// typed binary keys, columnar in and out, no boxing. Errors propagate:
	// returning the partials undeduplicated would be silent wrong results.
	agg := exec.NewHashAggregate(nil, nil)
	agg.GroupByAll = true
	ctx := context.Background()
	if err := agg.Init(ctx); err != nil {
		return nil, err
	}
	for {
		b, err := in.Next(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			break
		}
		if err := agg.Consume(ctx, b); err != nil {
			return nil, err
		}
	}
	if err := agg.Finalize(ctx); err != nil {
		return nil, err
	}
	var out []*batch.RecordBatch
	for {
		b, err := agg.Next(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			break
		}
		out = append(out, b)
	}
	return out, nil
}

// reAggregatePartials merges partial aggregate results by group-by key.
// For COUNT → SUM, SUM → SUM, MIN → MIN, MAX → MAX.
// mergeScalarAggregates merges N partial scalar aggregate rows into 1.
// For SUM/COUNT: sum all partials. For MIN: take min. For MAX: take max.
func (c *Coordinator) mergeScalarAggregates(batches []*batch.RecordBatch, columns []string, colIdx map[string]int, mi *logical.MergeInfo) []*batch.RecordBatch {
	if len(batches) == 0 || len(mi.AggExprs) == 0 {
		return batches
	}

	// Collect all rows across batches
	var rows []map[string]any
	for _, b := range batches {
		rows = append(rows, b.ToRows()...)
	}
	if len(rows) <= 1 {
		return batches
	}

	// Merge into single row
	merged := make(map[string]any, len(columns))
	for _, ae := range mi.AggExprs {
		idx, ok := colIdx[ae.OutputCol]
		if !ok {
			continue
		}
		_ = idx

		switch strings.ToLower(ae.Func) {
		case "sum", "count":
			var total float64
			for _, row := range rows {
				v := row[ae.OutputCol]
				if v != nil {
					switch tv := v.(type) {
					case float64:
						total += tv
					case int64:
						total += float64(tv)
					}
				}
			}
			merged[ae.OutputCol] = total
		case "min":
			var minVal any
			for _, row := range rows {
				v := row[ae.OutputCol]
				if v == nil {
					continue
				}
				if minVal == nil || compareAnyValues(v, minVal) < 0 {
					minVal = v
				}
			}
			merged[ae.OutputCol] = minVal
		case "max":
			var maxVal any
			for _, row := range rows {
				v := row[ae.OutputCol]
				if v == nil {
					continue
				}
				if maxVal == nil || compareAnyValues(v, maxVal) > 0 {
					maxVal = v
				}
			}
			merged[ae.OutputCol] = maxVal
		default:
			// AVG and others: take first non-nil value (imprecise but safe)
			for _, row := range rows {
				if v := row[ae.OutputCol]; v != nil {
					merged[ae.OutputCol] = v
					break
				}
			}
		}
	}

	return []*batch.RecordBatch{batch.FromRows(batches[0].Schema, []map[string]any{merged})}
}

// compareAnyValues compares two values for min/max merge.
func compareAnyValues(a, b any) int {
	switch av := a.(type) {
	case float64:
		bv, _ := b.(float64)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int64:
		bv, _ := b.(int64)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv, _ := b.(string)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	}
	return 0
}

func (c *Coordinator) reAggregatePartials(batches []*batch.RecordBatch, columns []string, colIdx map[string]int, mi *logical.MergeInfo) []*batch.RecordBatch {
	if len(batches) == 0 || len(mi.AggExprs) == 0 {
		return batches
	}

	// Resolve group-by and aggregate column indices
	groupByIdx := make([]int, len(mi.GroupBy))
	for i, col := range mi.GroupBy {
		groupByIdx[i] = colIdx[col]
	}

	// Pre-resolve aggregate column metadata + a typed merge function once per
	// aggregate. The hot row loop calls aggMergeFns[ai] directly instead of
	// switching on a "sum"/"min"/"max" string per row per aggregate.
	type aggMergeFn func(cur float64, in float64) float64
	sumMerge := func(cur, in float64) float64 { return cur + in }
	minMerge := func(cur, in float64) float64 {
		if in < cur {
			return in
		}
		return cur
	}
	maxMerge := func(cur, in float64) float64 {
		if in > cur {
			return in
		}
		return cur
	}
	type aggCol struct {
		idx     int
		mergeFn aggMergeFn
	}
	var aggCols []aggCol
	for _, ae := range mi.AggExprs {
		idx, ok := colIdx[ae.OutputCol]
		if !ok {
			continue
		}
		merge := sumMerge // COUNT, SUM → SUM
		switch strings.ToLower(ae.Func) {
		case "min":
			merge = minMerge
		case "max":
			merge = maxMerge
		}
		aggCols = append(aggCols, aggCol{idx: idx, mergeFn: merge})
	}

	if len(aggCols) == 0 {
		return batches
	}

	schema := batches[0].Schema

	// Pre-resolve key encoders once per group-by column so the hot row loop
	// dispatches via function pointer instead of switching on schema[ci].Type
	// every row. Closure captures the column index so the encoder reads
	// directly from b.Columns[ci] at the given row.
	type keyEncoder func(b *batch.RecordBatch, row int, dst []byte) []byte
	keyEncoders := make([]keyEncoder, len(groupByIdx))
	for gi, ci := range groupByIdx {
		ci := ci
		switch schema[ci].Type {
		case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
			keyEncoders[gi] = func(b *batch.RecordBatch, row int, dst []byte) []byte {
				return strconv.AppendInt(dst, int64(b.Columns[ci].Int32Data[row]), 10)
			}
		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
			keyEncoders[gi] = func(b *batch.RecordBatch, row int, dst []byte) []byte {
				return strconv.AppendInt(dst, b.Columns[ci].Int64Data[row], 10)
			}
		case parquet.TypeFloat32:
			keyEncoders[gi] = func(b *batch.RecordBatch, row int, dst []byte) []byte {
				return strconv.AppendFloat(dst, float64(b.Columns[ci].Float32Data[row]), 'g', -1, 32)
			}
		case parquet.TypeFloat64:
			keyEncoders[gi] = func(b *batch.RecordBatch, row int, dst []byte) []byte {
				return strconv.AppendFloat(dst, b.Columns[ci].Float64Data[row], 'g', -1, 64)
			}
		case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
			keyEncoders[gi] = func(b *batch.RecordBatch, row int, dst []byte) []byte {
				return append(dst, b.Columns[ci].BytesData.Value(row)...)
			}
		case parquet.TypeBool:
			keyEncoders[gi] = func(b *batch.RecordBatch, row int, dst []byte) []byte {
				if b.Columns[ci].BoolData[row] {
					return append(dst, '1')
				}
				return append(dst, '0')
			}
		default:
			typ := schema[ci].Type
			keyEncoders[gi] = func(b *batch.RecordBatch, row int, dst []byte) []byte {
				return fmt.Appendf(dst, "%v", extractValue(b.Columns[ci], row, typ))
			}
		}
	}

	// Pre-resolve aggregate float extractors once per aggregate column —
	// avoids the per-row type switch inside extractFloat64.
	type floatExtractor func(b *batch.RecordBatch, row int) float64
	aggExtractors := make([]floatExtractor, len(aggCols))
	for ai, ac := range aggCols {
		ac := ac
		switch schema[ac.idx].Type {
		case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
			aggExtractors[ai] = func(b *batch.RecordBatch, row int) float64 {
				return float64(b.Columns[ac.idx].Int32Data[row])
			}
		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
			aggExtractors[ai] = func(b *batch.RecordBatch, row int) float64 {
				return float64(b.Columns[ac.idx].Int64Data[row])
			}
		case parquet.TypeFloat32:
			aggExtractors[ai] = func(b *batch.RecordBatch, row int) float64 {
				return float64(b.Columns[ac.idx].Float32Data[row])
			}
		case parquet.TypeFloat64:
			aggExtractors[ai] = func(b *batch.RecordBatch, row int) float64 {
				return b.Columns[ac.idx].Float64Data[row]
			}
		default:
			typ := schema[ac.idx].Type
			aggExtractors[ai] = func(b *batch.RecordBatch, row int) float64 {
				return extractFloat64(b.Columns[ac.idx], row, typ)
			}
		}
	}

	// mergedRow stores a pointer back to the source (b, row) instead of
	// materializing a []any per group. The source row's typed values are
	// copied out at finalize time (one type-switch per group, not per row),
	// avoiding the boxing allocation that dominated GC pressure at SF100.
	// rank is the global flat-row index of the group's first occurrence; it
	// reproduces the sequential insertion order after the parallel merge.
	type mergedRow struct {
		sourceBatch *batch.RecordBatch
		sourceRow   int
		aggVals     []float64
		rank        int
	}

	// Per-batch global row offset, so a group's first-occurrence position has a
	// single monotonic index across all batches.
	totalRows := 0
	batchStart := make([]int, len(batches))
	for i, b := range batches {
		batchStart[i] = totalRows
		totalRows += b.ActiveLen()
	}

	// accumulate folds one source row into a shard-local group map, identical
	// for the serial and parallel paths. `owns` decides whether this scanner
	// owns the row's key (always true for the single-shard serial path); when
	// it returns false the row is skipped so another shard handles it.
	accumulate := func(groups map[string]*mergedRow, ordered *[]*mergedRow, keyBuf []byte, owns func(key []byte) bool) []byte {
		for bi, b := range batches {
			nRows := b.ActiveLen()
			sel := b.Sel
			for ri := 0; ri < nRows; ri++ {
				row := ri
				if sel != nil {
					row = int(sel[ri])
				}
				keyBuf = keyBuf[:0]
				for gi, enc := range keyEncoders {
					if gi > 0 {
						keyBuf = append(keyBuf, 0)
					}
					keyBuf = enc(b, row, keyBuf)
				}
				if owns != nil && !owns(keyBuf) {
					continue
				}
				key := string(keyBuf)
				mr, exists := groups[key]
				if !exists {
					aggVals := make([]float64, len(aggCols))
					for ai, ext := range aggExtractors {
						aggVals[ai] = ext(b, row)
					}
					mr = &mergedRow{sourceBatch: b, sourceRow: row, aggVals: aggVals, rank: batchStart[bi] + ri}
					groups[key] = mr
					*ordered = append(*ordered, mr)
					continue
				}
				for ai, ext := range aggExtractors {
					mr.aggVals[ai] = aggCols[ai].mergeFn(mr.aggVals[ai], ext(b, row))
				}
			}
		}
		return keyBuf
	}

	var ordered []*mergedRow
	shards := mergeShardCount(totalRows)
	if shards <= 1 {
		groups := make(map[string]*mergedRow)
		accumulate(groups, &ordered, make([]byte, 0, 256), nil)
	} else {
		// Parallel key-sharded merge. Each worker scans ALL rows in global
		// order but accumulates only keys it owns (hash(key)%shards == w).
		// Because every key lives in exactly one shard and is accumulated in
		// global (batch,row) order, per-key SUM/MIN/MAX order is identical to
		// the serial path → byte-identical float results. Group output order is
		// restored below by sorting on the global first-occurrence rank.
		shardOrdered := make([][]*mergedRow, shards)
		var wg sync.WaitGroup
		for w := 0; w < shards; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				groups := make(map[string]*mergedRow)
				var local []*mergedRow
				accumulate(groups, &local, make([]byte, 0, 256), func(key []byte) bool {
					return mergeShardOf(key, shards) == w
				})
				shardOrdered[w] = local
			}(w)
		}
		wg.Wait()
		for w := 0; w < shards; w++ {
			ordered = append(ordered, shardOrdered[w]...)
		}
		// ranks are unique (each group first-occurs at a distinct global row),
		// so this reproduces the exact sequential insertion order.
		slices.SortFunc(ordered, func(a, b *mergedRow) int { return a.rank - b.rank })
	}

	// Build result batch from merged groups. Group-by values are copied
	// directly from the captured source batch+row using typed copy
	// helpers — this is one type-switch per (group, column), not per row.
	result := batch.NewRecordBatch(schema, len(ordered))
	for ri, mr := range ordered {
		for _, ci := range groupByIdx {
			copyVectorValue(result.Columns[ci], ri, mr.sourceBatch.Columns[ci], mr.sourceRow, schema[ci].Type)
		}
		for ai, ac := range aggCols {
			setFloat64Value(result.Columns[ac.idx], ri, mr.aggVals[ai], schema[ac.idx].Type)
		}
		for ci := range schema {
			result.Columns[ci].Nulls.SetValid(ri)
		}
		// A NULL group key is a legitimate group (GROUP BY over nullable
		// columns); the blanket SetValid above would resurface it as a
		// zero value. Re-propagate source nulls for the key columns.
		for _, ci := range groupByIdx {
			if mr.sourceBatch.Columns[ci].Nulls.IsNullFast(mr.sourceRow) {
				result.Columns[ci].Nulls.SetNull(ri)
			}
		}
	}
	result.Len = len(ordered)

	return []*batch.RecordBatch{result}
}

// parallelMergeMinRows is the partial-row count below which reAggregatePartials
// stays single-threaded — goroutine setup isn't worth it for small merges.
const parallelMergeMinRows = 8192

// mergeShardCount returns how many parallel shards reAggregatePartials should
// use for a merge of totalRows partial rows: 1 (serial) below the threshold or
// on a single core, otherwise GOMAXPROCS.
func mergeShardCount(totalRows int) int {
	if totalRows < parallelMergeMinRows {
		return 1
	}
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		return 1
	}
	return n
}

// mergeShardOf maps a group key to one of `shards` shards via FNV-1a. It is a
// pure function of the key bytes, so a key's rows are always owned by the same
// shard — required for byte-identical per-key accumulation order. The shard
// assignment itself does not affect results (output is rank-sorted and per-key
// accumulation is global-order regardless of which shard owns the key).
func mergeShardOf(key []byte, shards int) int {
	var h uint64 = 1469598103934665603
	for _, c := range key {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return int(h % uint64(shards))
}

// copyVectorValue copies a single typed value from src[srcRow] to dst[dstRow].
// Avoids the []any boxing that setValueFromAny goes through — used by
// reAggregatePartials' finalize step where the source value is already in a
// typed vector slot.
func copyVectorValue(dst *batch.Vector, dstRow int, src *batch.Vector, srcRow int, typ parquet.TypeID) {
	switch typ {
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		dst.Int32Data[dstRow] = src.Int32Data[srcRow]
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		dst.Int64Data[dstRow] = src.Int64Data[srcRow]
	case parquet.TypeFloat32:
		dst.Float32Data[dstRow] = src.Float32Data[srcRow]
	case parquet.TypeFloat64:
		dst.Float64Data[dstRow] = src.Float64Data[srcRow]
	case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
		dst.BytesData.Set(dstRow, src.BytesData.Value(srcRow))
	case parquet.TypeBool:
		dst.BoolData[dstRow] = src.BoolData[srcRow]
	case parquet.TypeDecimal:
		// Decimal has dedicated Int128 storage; the old switch silently
		// wrote NOTHING for it, so merged Decimal group-by columns came
		// back zero.
		dst.DecimalData.Data[dstRow] = src.DecimalData.Data[srcRow]
	default:
		// Nested and any future types: the typed nested-aware copier.
		dst.CopyValueFrom(dstRow, src, srcRow)
	}
}

// sortBatches performs a simple in-memory sort of batches by the given order keys.
// Used for merging probe-split partial results (typically <100K rows).
func (c *Coordinator) sortBatches(batches []*batch.RecordBatch, columns []string, colIdx map[string]int, orderBy []logical.OrderExpr) {
	if len(batches) != 1 {
		return // reAggregatePartials produces a single batch
	}
	b := batches[0]
	nRows := b.Len
	if nRows <= 1 {
		return
	}

	schema := b.Schema

	// Build index permutation and sort it
	indices := make([]int, nRows)
	for i := range indices {
		indices[i] = i
	}

	slices.SortFunc(indices, func(i, j int) int {
		return compareBatchRows(b, i, j, orderBy, colIdx, schema)
	})

	// Apply permutation: set selection vector
	sel := make([]uint32, nRows)
	for i, idx := range indices {
		sel[i] = uint32(idx)
	}
	b.Sel = sel
}

// topKBatches selects the top-k rows by order-by keys using a min-heap,
// avoiding O(n log n) full sort when only k << n results are needed.
func (c *Coordinator) topKBatches(batches []*batch.RecordBatch, columns []string, colIdx map[string]int, orderBy []logical.OrderExpr, k int) {
	if len(batches) != 1 {
		return
	}
	b := batches[0]
	nRows := b.Len
	if nRows <= k {
		return
	}

	schema := b.Schema

	// Min-heap where "minimum" = worst in desired sort order (last to keep).
	// compareBatchRows returns < 0 when i sorts before j, so the heap's
	// "less" is reversed: the root is the row that sorts LAST among the top-k.
	h := make([]int, k)
	for i := 0; i < k; i++ {
		h[i] = i
	}

	cmp := func(a, b int) int {
		return compareBatchRows(batches[0], a, b, orderBy, colIdx, schema)
	}

	// worst-first: h[i] sorts after h[j] → "less" for heap
	hless := func(i, j int) bool { return cmp(h[i], h[j]) > 0 }

	siftDown := func(root, n int) {
		for {
			child := 2*root + 1
			if child >= n {
				break
			}
			if child+1 < n && hless(child+1, child) {
				child++
			}
			if hless(root, child) {
				break
			}
			h[root], h[child] = h[child], h[root]
			root = child
		}
	}

	// Build heap from initial k elements
	for i := k/2 - 1; i >= 0; i-- {
		siftDown(i, k)
	}

	// Process remaining rows: replace root if new row is better
	for i := k; i < nRows; i++ {
		if cmp(i, h[0]) < 0 {
			h[0] = i
			siftDown(0, k)
		}
	}

	// Sort the k winners in desired order
	slices.SortFunc(h, func(a, b int) int {
		return cmp(a, b)
	})

	// Set selection vector and truncate
	sel := make([]uint32, k)
	for i, idx := range h {
		sel[i] = uint32(idx)
	}
	b.Sel = sel
	b.Len = k
}

// compareBatchRows compares two rows in a batch by the order-by keys.
// Returns negative if row a < row b, positive if a > b, 0 if equal.
func compareBatchRows(b *batch.RecordBatch, a, bIdx int, orderBy []logical.OrderExpr, colIdx map[string]int, schema []parquet.Column) int {
	for _, ob := range orderBy {
		ci, ok := colIdx[ob.Column]
		if !ok {
			continue
		}
		va := extractFloat64(b.Columns[ci], a, schema[ci].Type)
		vb := extractFloat64(b.Columns[ci], bIdx, schema[ci].Type)

		var cmp int
		switch {
		case va < vb:
			cmp = -1
		case va > vb:
			cmp = 1
		default:
			// For string columns, compare as strings
			if schema[ci].Type == parquet.TypeString {
				sa := extractStringValue(b.Columns[ci], a)
				sb := extractStringValue(b.Columns[ci], bIdx)
				cmp = strings.Compare(sa, sb)
			}
		}
		if ob.Desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// limitBatches truncates batches to at most limit rows total.
func limitBatches(batches []*batch.RecordBatch, limit int) []*batch.RecordBatch {
	var result []*batch.RecordBatch
	remaining := limit
	for _, b := range batches {
		n := b.ActiveLen()
		if n <= remaining {
			result = append(result, b)
			remaining -= n
		} else {
			// Truncate this batch
			if b.Sel != nil {
				b.Sel = b.Sel[:remaining]
			} else {
				sel := make([]uint32, remaining)
				for i := range sel {
					sel[i] = uint32(i)
				}
				b.Sel = sel
			}
			result = append(result, b)
			break
		}
	}
	return result
}

// extractValue reads a typed value from a vector column at the given row.
func extractValue(vec *batch.Vector, row int, typ parquet.TypeID) any {
	switch typ {
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		return vec.Int32Data[row]
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		return vec.Int64Data[row]
	case parquet.TypeFloat32:
		return vec.Float32Data[row]
	case parquet.TypeFloat64:
		return vec.Float64Data[row]
	case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
		return string(vec.BytesData.Value(row))
	case parquet.TypeBool:
		return vec.BoolData[row]
	case parquet.TypeDecimal:
		// Was missing: every Decimal group key extracted as nil, encoding
		// to the SAME merge key — distinct decimal groups collapsed into
		// one, corrupting aggregates, not just the key column.
		return vec.DecimalData.Data[row]
	default:
		return vec.GetValue(row)
	}
}

// extractFloat64 converts a typed vector value to float64 for numeric operations.
func extractFloat64(vec *batch.Vector, row int, typ parquet.TypeID) float64 {
	switch typ {
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		return float64(vec.Int32Data[row])
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		return float64(vec.Int64Data[row])
	case parquet.TypeFloat32:
		return float64(vec.Float32Data[row])
	case parquet.TypeFloat64:
		return vec.Float64Data[row]
	default:
		return 0
	}
}

// extractStringValue reads a string value from a bytes-backed vector column.
func extractStringValue(vec *batch.Vector, row int) string {
	return string(vec.BytesData.Value(row))
}

// setValueFromAny writes a value to a vector column at the given row.
func setValueFromAny(vec *batch.Vector, row int, val any, typ parquet.TypeID) {
	switch typ {
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		if v, ok := val.(int32); ok {
			vec.Int32Data[row] = v
		}
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		if v, ok := val.(int64); ok {
			vec.Int64Data[row] = v
		}
	case parquet.TypeFloat32:
		if v, ok := val.(float32); ok {
			vec.Float32Data[row] = v
		}
	case parquet.TypeFloat64:
		if v, ok := val.(float64); ok {
			vec.Float64Data[row] = v
		}
	case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
		if v, ok := val.(string); ok {
			vec.BytesData.Set(row, []byte(v))
		}
	case parquet.TypeBool:
		if v, ok := val.(bool); ok {
			vec.BoolData[row] = v
		}
	}
}

// setFloat64Value writes a float64 value to a vector column in its native type.
func setFloat64Value(vec *batch.Vector, row int, val float64, typ parquet.TypeID) {
	switch typ {
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		vec.Int32Data[row] = int32(val)
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		vec.Int64Data[row] = int64(val)
	case parquet.TypeFloat32:
		vec.Float32Data[row] = float32(val)
	case parquet.TypeFloat64:
		vec.Float64Data[row] = val
	}
}

// ReadResultFiles reads result Parquet files from S3 and returns columnar batches.
// This is intended for callers (tpch-bench, CLI) that need full result data —
// they pull from S3 directly instead of routing through the coordinator's heap.
func ReadResultFiles(ctx context.Context, store objstore.Store, bucket string, paths []string) ([]*batch.RecordBatch, []string, int64, error) {
	if len(paths) == 0 {
		return nil, nil, 0, nil
	}

	// Single file: skip goroutine overhead
	if len(paths) == 1 {
		batches, cols, rows, err := readOneResultFile(ctx, store, bucket, paths[0])
		return batches, cols, rows, err
	}

	// Parallel reads with bounded concurrency
	type fileResult struct {
		batches []*batch.RecordBatch
		columns []string
		rows    int64
	}
	results := make([]fileResult, len(paths))
	const maxConcurrent = 8
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	for i, path := range paths {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, p string) {
			defer wg.Done()
			defer func() { <-sem }()
			batches, cols, rows, _ := readOneResultFile(ctx, store, bucket, p)
			results[idx] = fileResult{batches: batches, columns: cols, rows: rows}
		}(i, path)
	}
	wg.Wait()

	var allBatches []*batch.RecordBatch
	var columns []string
	var totalRows int64
	for _, r := range results {
		if len(r.columns) > 0 && len(columns) == 0 {
			columns = r.columns
		}
		totalRows += r.rows
		allBatches = append(allBatches, r.batches...)
	}
	return allBatches, columns, totalRows, nil
}

func readOneResultFile(ctx context.Context, store objstore.Store, bucket, path string) ([]*batch.RecordBatch, []string, int64, error) {
	rc, _, err := store.Get(ctx, bucket, path)
	if err != nil {
		return nil, nil, 0, err
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return nil, nil, 0, err
	}

	data, err = decompressShuffleData(data)
	if err != nil {
		return nil, nil, 0, err
	}

	var batches []*batch.RecordBatch
	var columns []string
	if len(data) >= 4 && string(data[:4]) == "WSHF" {
		batches, err = readShuffleBatches(data)
		if err != nil {
			return nil, nil, 0, err
		}
		if len(batches) > 0 {
			columns = make([]string, len(batches[0].Schema))
			for i, col := range batches[0].Schema {
				columns[i] = col.Name
			}
		}
	} else {
		reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, nil, 0, err
		}
		schema := reader.Schema().Columns
		if len(schema) > 0 {
			columns = make([]string, len(schema))
			for i, col := range schema {
				columns[i] = col.Name
			}
		}
		batches, err = scan.ReadFileBatches(reader, schema, nil)
		if err != nil {
			return nil, nil, 0, err
		}
	}

	var totalRows int64
	for _, b := range batches {
		totalRows += int64(b.ActiveLen())
	}
	return batches, columns, totalRows, nil
}

func (c *Coordinator) subscribeResults(ctx context.Context, queryID string, done chan<- struct{}) {
	subject := distributed.QueryResultSubject(queryID)
	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var result distributed.ResultNotification
		if err := distributed.Unmarshal(msg.Data, &result); err != nil {
			c.logger.Error("failed to unmarshal result", "error", err)
			return
		}

		logAttrs := []any{
			"task_id", result.TaskID,
			"query_id", result.QueryID,
			"stage_id", result.StageID,
			"success", result.Success,
			"rows", result.NumRows,
		}
		if s := result.TaskStats; s != nil {
			logAttrs = append(logAttrs,
				"mem_used", s.MemUsed,
				"mem_budget", s.MemBudget,
				"spill_files", s.SpillFiles,
				"spill_bytes", s.SpillBytes,
				"rss", s.RSS,
			)
		}
		c.logger.Debug("received result", logAttrs...)

		if c.workers.Liveness != nil {
			c.workers.Liveness.Remove(result.TaskID)
		}
		c.scheduler.TaskDone(result.TaskID)
		// Multi-signal liveness: a result publish proves the worker is
		// alive even if its heartbeat goroutine starves or the heartbeat
		// NATS conn lags. Updates LastSeen for the worker, no-op if not
		// yet registered.
		c.workers.MarkWorkerSeen(result.WorkerID)
		stageComplete := c.tracker.RecordResult(result)
		if !stageComplete {
			return
		}

		// If every task in this stage failed, abort the query.
		if errMsg := c.tracker.StageFailed(queryID, result.StageID); errMsg != "" {
			c.logger.Error("stage failed, aborting query",
				"query_id", queryID, "stage_id", result.StageID, "error", errMsg)
			c.tracker.Fail(queryID, fmt.Sprintf("stage %s: %s", result.StageID, errMsg))
			c.cleanupQuery(queryID)
			select {
			case done <- struct{}{}:
			default:
			}
			return
		}

		if c.tracker.IsComplete(queryID) {
			c.tracker.Complete(queryID)
			c.cleanupQuery(queryID)
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		c.logger.Error("failed to subscribe to results", "error", err, "subject", subject)
		return
	}
	// Same pending-limit bump as heartbeat sub — long queries (Q11 30min)
	// can accumulate many results during the per-query lifetime, and
	// drops here would silently lose stage-completion signals.
	if perr := sub.SetPendingLimits(coordSubMsgLimit, coordSubByteLimit); perr != nil {
		c.logger.Warn("failed to bump query-result sub pending limits", "error", perr)
	}
	// Propagate the interest to the server before the caller publishes
	// tasks — see subscribeTaskResults for the lost-result race this
	// closes (issue #143).
	if ferr := c.nc.Flush(); ferr != nil {
		c.logger.Error("failed to flush result subscription", "error", ferr, "subject", subject)
	}

	c.mu.Lock()
	cancelCtx, cancel := context.WithCancel(ctx)
	c.resultSubs[queryID] = cancel
	c.mu.Unlock()

	go func() {
		<-cancelCtx.Done()
		sub.Unsubscribe()
	}()
}

// Tracker returns the query tracker (for inspection).
func (c *Coordinator) Tracker() *QueryTracker {
	return c.tracker
}

// SubmitSQL parses, plans, and dispatches a query without blocking for results.
// Returns the query ID and plan string immediately.
func (c *Coordinator) SubmitSQL(ctx context.Context, sql string) (queryID string, planStr string, err error) {
	if !c.isLeaderOrStandalone() {
		leaderID := ""
		if c.leader != nil {
			leaderID = c.leader.CurrentLeader(ctx)
		}
		return "", "", fmt.Errorf("not leader: coordinator %s is leader", leaderID)
	}

	queryID = uuid.New().String()[:8]

	// Parse
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return "", "", fmt.Errorf("parse: %w", err)
	}

	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return "", "", fmt.Errorf("extract: %w", err)
	}

	// Reject references to columns that resolve to no source (plan-time name binding).
	if err := physical.NewPlanner(c.catalog).ValidateColumns(ctx, selectInfo); err != nil {
		return "", "", err
	}

	// Build logical plan
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		return "", "", fmt.Errorf("logical plan: %w", err)
	}
	explainAnnotator := func(plan *logical.Node) {
		physical.NewPlanner(c.catalog).AnnotateScanColumns(ctx, plan)
	}
	explainAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, explainAnnotator)
	planStr = logicalPlan.PrettyPrint(0)

	// Generate distributed stages and route to pipeline execution
	planner := physical.NewPlanner(c.catalog)
	planner.WorkerCount = c.workers.Count()
	planner.BroadcastBytesThreshold = broadcastThresholdFromCluster(c.workers.MinWorkerPoolBudget())
	if c.config.BroadcastBytesOverride != 0 {
		planner.BroadcastBytesThreshold = c.config.BroadcastBytesOverride
	}
	planner.SortMergeJoinBytes = c.config.SortMergeJoinBytes
	planner.LateMaterialization = c.config.LateMaterialization
	planner.DynamicFiltersEnabled = c.config.DynamicFilters
	physStages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		return "", "", fmt.Errorf("physical plan: %w", err)
	}

	// Route to probe-split or single-worker pipeline (same as ExecuteSQL)
	var probeSplitMergeInfo *logical.MergeInfo
	probeAlias, probeFiles, canProbeSplit := physical.CanProbeSplit(physStages, c.workers.Count())
	mergeInfo := logical.ExtractMergeInfo(logicalPlan)

	if canProbeSplit && mergeInfo != nil {
		probeSplitMergeInfo = mergeInfo
		buildCache, buildCacheErr := c.preScanBuildTables(ctx, queryID, sql, physStages, probeAlias)
		if buildCacheErr != nil {
			return "", "", fmt.Errorf("build cache pre-scan failed for query %s: %w", queryID, buildCacheErr)
		}
		physStages = []physical.Stage{{
			ID:                 "pipeline-0",
			Type:               "pipeline",
			Tasks:              c.workers.Count(),
			ProbeSplitAlias:    probeAlias,
			ProbeSplitFiles:    probeFiles,
			BuildCachePreScans: buildCache,
		}}
	} else {
		physStages = []physical.Stage{{
			ID:    "pipeline-0",
			Type:  "pipeline",
			Tasks: 1,
		}}
	}

	if len(physStages) == 0 {
		// No work to do — register as immediately completed
		c.tracker.Register(queryID, sql, map[string]*StageInfo{}, nil)
		c.tracker.Start(queryID)
		c.tracker.Complete(queryID)
		c.mu.Lock()
		c.queryMetas[queryID] = &queryMeta{planStr: planStr}
		c.mu.Unlock()
		return queryID, planStr, nil
	}

	// Store metadata
	c.mu.Lock()
	c.queryMetas[queryID] = &queryMeta{stages: physStages, planStr: planStr, sqlText: sql, mergeInfo: probeSplitMergeInfo}
	c.mu.Unlock()

	// Register stages with tracker
	trackerStages := make(map[string]*StageInfo, len(physStages))
	var stageOrder []string
	for _, s := range physStages {
		trackerStages[s.ID] = &StageInfo{
			StageID:      s.ID,
			Type:         distributed.TaskType(s.Type),
			TotalTasks:   s.Tasks,
			Dependencies: s.Dependencies,
		}
		stageOrder = append(stageOrder, s.ID)
	}
	c.tracker.Register(queryID, sql, trackerStages, stageOrder)
	c.tracker.Start(queryID)

	// Use a timeout context so stuck queries don't leak resources forever.
	queryTimeout := c.config.QueryTimeout
	if queryTimeout <= 0 {
		queryTimeout = 30 * time.Minute
	}
	asyncCtx, asyncCancel := context.WithTimeout(context.Background(), queryTimeout)

	// Subscribe for results (non-blocking callback)
	doneCh := make(chan struct{}, 1)
	c.subscribeResults(asyncCtx, queryID, doneCh)

	// Publish leaf stage tasks
	for _, s := range physStages {
		if len(s.Dependencies) > 0 {
			continue
		}
		tasks := c.createTasksForStage(queryID, s, nil)
		c.tracker.SetStageTasks(queryID, s.ID, len(tasks))
		c.tracker.MarkScheduled(queryID, s.ID)
		if err := c.scheduler.PublishTasks(asyncCtx, tasks); err != nil {
			asyncCancel()
			c.tracker.Fail(queryID, err.Error())
			return "", "", fmt.Errorf("publishing leaf tasks: %w", err)
		}
	}

	// Watchdog: fail the query if it exceeds the timeout, clean up resources
	// when the query completes normally.
	go func() {
		select {
		case <-doneCh:
			asyncCancel()
		case <-asyncCtx.Done():
			if asyncCtx.Err() == context.DeadlineExceeded {
				c.logger.Warn("query timed out", "query_id", queryID, "timeout", queryTimeout)
				c.tracker.Fail(queryID, fmt.Sprintf("query exceeded %s timeout", queryTimeout))
				c.cleanupQuery(queryID)
			}
		}
	}()

	return queryID, planStr, nil
}

// QueryStatus represents the current status of an async query.
type QueryStatus struct {
	QueryID   string        `json:"query_id"`
	SQL       string        `json:"sql"`
	State     string        `json:"state"`
	Stages    []StageStatus `json:"stages,omitempty"`
	Elapsed   time.Duration `json:"elapsed"`
	TotalRows int64         `json:"total_rows"`
	Error     string        `json:"error,omitempty"`
}

// StageStatus represents the progress of a single query stage.
type StageStatus struct {
	StageID     string `json:"stage_id"`
	Type        string `json:"type"`
	TotalTasks  int    `json:"total_tasks"`
	DoneTasks   int    `json:"done_tasks"`
	FailedTasks int    `json:"failed_tasks"`
}

// GetQueryStatus returns the current status of a query.
func (c *Coordinator) GetQueryStatus(queryID string) (*QueryStatus, error) {
	info := c.tracker.Get(queryID)
	if info == nil {
		return nil, fmt.Errorf("query not found: %s", queryID)
	}

	status := &QueryStatus{
		QueryID:   info.QueryID,
		SQL:       info.SQL,
		State:     info.State.String(),
		TotalRows: info.TotalRows,
		Error:     info.Error,
	}

	if !info.EndTime.IsZero() {
		status.Elapsed = info.EndTime.Sub(info.StartTime)
	} else {
		status.Elapsed = time.Since(info.StartTime)
	}

	for _, stageID := range info.StageOrder {
		stage := info.Stages[stageID]
		if stage == nil {
			continue
		}
		status.Stages = append(status.Stages, StageStatus{
			StageID:     stage.StageID,
			Type:        string(stage.Type),
			TotalTasks:  stage.TotalTasks,
			DoneTasks:   stage.DoneTasks,
			FailedTasks: stage.FailedTasks,
		})
	}

	return status, nil
}

// GetQueryResults retrieves the final results for a completed query.
func (c *Coordinator) GetQueryResults(ctx context.Context, queryID string) (*SQLResult, error) {
	info := c.tracker.Get(queryID)
	if info == nil {
		return nil, fmt.Errorf("query not found: %s", queryID)
	}

	c.mu.Lock()
	meta := c.queryMetas[queryID]
	c.mu.Unlock()

	planStr := ""
	if meta != nil {
		planStr = meta.planStr
	}

	elapsed := time.Duration(0)
	if !info.EndTime.IsZero() {
		elapsed = info.EndTime.Sub(info.StartTime)
	} else {
		elapsed = time.Since(info.StartTime)
	}

	if info.State != QueryStateCompleted {
		return &SQLResult{
			QueryID: queryID,
			Elapsed: elapsed,
			Plan:    planStr,
			Error:   fmt.Sprintf("query state is %s, not completed", info.State),
		}, nil
	}

	if meta == nil || len(meta.stages) == 0 {
		return &SQLResult{
			QueryID:     queryID,
			ResultFiles: info.ResultFiles,
			TotalRows:   info.TotalRows,
			Elapsed:     elapsed,
			Plan:        planStr,
		}, nil
	}

	needsMerge := meta.mergeInfo != nil
	batches, columns, totalRows, err := c.readFinalResults(ctx, queryID, meta.stages, needsMerge)
	if err != nil {
		return &SQLResult{
			QueryID:     queryID,
			ResultFiles: info.ResultFiles,
			TotalRows:   info.TotalRows,
			Elapsed:     elapsed,
			Plan:        planStr,
		}, nil
	}

	// Apply probe-split merge if needed (same as ExecuteSQL path)
	if meta.mergeInfo != nil && len(batches) > 0 {
		merged, mergedRows, mergeErr := c.mergeProbePartials(newSliceStream(batches), columns, meta.mergeInfo)
		if mergeErr == nil {
			batches = merged
			totalRows = mergedRows
		}
	}

	return &SQLResult{
		QueryID:     queryID,
		Columns:     columns,
		Batches:     batches,
		ResultFiles: info.ResultFiles,
		TotalRows:   totalRows,
		Elapsed:     elapsed,
		Plan:        planStr,
	}, nil
}

// CancelQuery cancels a running query.
func (c *Coordinator) CancelQuery(queryID string) error {
	info := c.tracker.Get(queryID)
	if info == nil {
		return fmt.Errorf("query not found: %s", queryID)
	}

	if info.State != QueryStateRunning && info.State != QueryStatePending {
		return fmt.Errorf("query %s is already %s", queryID, info.State)
	}

	// Cancel the result subscription which stops scheduling new stages
	c.mu.Lock()
	if cancel, ok := c.resultSubs[queryID]; ok {
		cancel()
		delete(c.resultSubs, queryID)
	}
	c.mu.Unlock()

	// Propagate cancellation to workers via NATS so they can abandon in-flight tasks
	cancelSubject := distributed.CancelSubject(queryID)
	if err := c.nc.Publish(cancelSubject, []byte(queryID)); err != nil {
		c.logger.Warn("failed to publish cancellation", "query_id", queryID, "error", err)
	}

	c.tracker.Cancel(queryID)
	c.logger.Info("query cancelled", "query_id", queryID)
	return nil
}

// ListQueries returns recent query statuses.
func (c *Coordinator) ListQueries() []QueryStatus {
	queries := c.tracker.List()
	statuses := make([]QueryStatus, 0, len(queries))
	for _, info := range queries {
		status := QueryStatus{
			QueryID:   info.QueryID,
			SQL:       info.SQL,
			State:     info.State.String(),
			TotalRows: info.TotalRows,
			Error:     info.Error,
		}
		if !info.EndTime.IsZero() {
			status.Elapsed = info.EndTime.Sub(info.StartTime)
		} else if !info.StartTime.IsZero() {
			status.Elapsed = time.Since(info.StartTime)
		}
		statuses = append(statuses, status)
	}
	return statuses
}
