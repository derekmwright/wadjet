package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/alerts"
	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/telemetry"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"github.com/citc-tech/wadjet/internal/engine/batch"
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
)

// Config holds coordinator configuration.
type Config struct {
	NATSUrl        string
	ResultBucket   string
	MaxInflight    int           // max concurrent queries, 0 = default (64)
	QueryTimeout   time.Duration // max time for a query to complete, 0 = default (30m)
	WorkerStaleTTL time.Duration // time after which a silent worker is reaped, 0 = default (30s)
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
	mergeInfo          *logical.MergeInfo // non-nil for probe-split queries needing merge
}

// Coordinator accepts queries, plans them, dispatches tasks, and tracks results.
type Coordinator struct {
	config    Config
	catalog   *catalog.Catalog
	nc        *nats.Conn
	js        jetstream.JetStream
	scheduler *Scheduler
	tracker   *QueryTracker
	workers   *WorkerRegistry
	cleaner   *ResultCleaner
	leader    *LeaderElection   // nil = always leader (standalone mode)
	queryStore *QueryStateStore // nil = no persistence (standalone mode)
	resultKV  jetstream.KeyValue // NATS KV for fast inter-stage result transfer (nil = S3 only)
	otel      *telemetry.Provider // nil = no OTel tracing
	logger    *slog.Logger

	// BuildCacheThreshold overrides the default build cache threshold (bytes).
	// Zero means use the default (2GB). Exported for testing with small datasets.
	BuildCacheThreshold int64

	mu         sync.Mutex
	resultSubs map[string]context.CancelFunc          // queryID -> cancel
	queryMetas map[string]*queryMeta                  // queryID -> metadata for result retrieval
	querySem   chan struct{}                           // limits concurrent inflight queries

	// Alert scheduler fields (see alerts.go for lifecycle methods).
	alertScheduler       *alerts.Scheduler
	alertSchedulerCancel context.CancelFunc
	alertsEnabled        bool

	// Catalog snapshot fields (see catalog_snapshot.go for lifecycle methods).
	catalogSnapshotOpts     catalog.SnapshotOptions
	catalogSnapshotInterval time.Duration
	catalogSnapshotCancel   context.CancelFunc
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
	}

	// NATS KV result cache: coordinator writes inline results here instead of S3.
	// Workers already read from this bucket (tier 2 in getFileData), so this
	// eliminates the S3 round-trip at stage boundaries for small results.
	if js != nil {
		kv, kvErr := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{
			Bucket:   "wadjet_results_data",
			TTL:      5 * time.Minute,
			MaxBytes: 1024 * 1024 * 1024, // 1 GB total
			Storage:  jetstream.MemoryStorage,
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

// Workers returns the worker registry for inspecting active workers.
func (c *Coordinator) Workers() *WorkerRegistry {
	return c.workers
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

// SQLResult holds the full result of a distributed SQL query.
// Results are kept columnar (as RecordBatches) to avoid materializing
// per-row map[string]any which causes massive heap pressure at SF10+.
type SQLResult struct {
	QueryID     string
	Columns     []string
	Batches     []*batch.RecordBatch
	ResultFiles []string
	TotalRows   int64
	Elapsed     time.Duration
	Plan        string
	Error       string
}

// Rows materializes the result batches into row-oriented maps.
// This is expensive for large results — prefer iterating Batches directly.
func (r *SQLResult) Rows() []map[string]any {
	if r == nil {
		return nil
	}
	var rows []map[string]any
	for _, b := range r.Batches {
		rows = append(rows, b.ToRows()...)
	}
	return rows
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

	// Build logical plan
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		return nil, fmt.Errorf("logical plan: %w", err)
	}

	// Inject row-level security filters from context (set by server from access policies)
	if rowFilters := auth.RowFiltersFromContext(ctx); len(rowFilters) > 0 {
		for table, filter := range rowFilters {
			logicalPlan = logical.InjectRowFilter(logicalPlan, table, filter)
		}
	}

	// Annotate scan columns and optimize — pass scan annotator for IN decorrelation
	scanAnnotator := func(plan *logical.Node) {
		physical.NewPlanner(c.catalog).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)
	planStr := logicalPlan.PrettyPrint(0)

	// Generate distributed stages
	planner := physical.NewPlanner(c.catalog)
	planner.WorkerCount = c.workers.Count()
	physStages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		return nil, fmt.Errorf("physical plan: %w", err)
	}

	// Log stage summary for debugging
	for _, s := range physStages {
		c.logger.Debug("distributed stage", "query", queryID, "stage", s.ID, "type", s.Type,
			"tasks", s.Tasks, "deps", s.Dependencies, "joinType", s.JoinType,
			"leftKeys", s.JoinLeftKeys, "rightKeys", s.JoinRightKeys,
			"filters", s.FilterExprs, "joinFilter", s.JoinFilter,
			"buildAlias", s.BuildTableAlias)
	}

	// Route all queries through pipeline execution. Two modes:
	// 1. Probe-split: partition probe table files across workers (multi-task)
	// 2. Single-worker: entire query on one worker
	var probeSplitMergeInfo *logical.MergeInfo
	probeAlias, probeFiles, canProbeSplit := physical.CanProbeSplit(physStages, c.workers.Count())
	mergeInfo := logical.ExtractMergeInfo(logicalPlan)

	if canProbeSplit && mergeInfo != nil {
		workerCount := c.workers.Count()
		probeSplitMergeInfo = mergeInfo

		// Build-side broadcast cache: pre-scan large build tables once so all
		// workers load a shared S3 result instead of independently scanning
		// large source tables N times. Eliminates the N× hash table duplication
		// that OOMs Q09 at SF100 (orders ~15GB × 3 workers = ~45GB).
		buildCache, buildCacheErr := c.preScanBuildTables(ctx, queryID, sql, physStages, probeAlias)
		if buildCacheErr != nil {
			return nil, fmt.Errorf("build cache pre-scan failed for query %s: %w", queryID, buildCacheErr)
		}

		physStages = []physical.Stage{{
			ID:                 "pipeline-0",
			Type:               "pipeline",
			Tasks:              workerCount,
			ProbeSplitAlias:    probeAlias,
			ProbeSplitFiles:    probeFiles,
			BuildCachePreScans: buildCache,
		}}
		c.logger.Info("routing to probe-split pipeline",
			"query", queryID, "probe_alias", probeAlias,
			"probe_files", len(probeFiles), "workers", workerCount,
			"has_merge", probeSplitMergeInfo != nil,
			"build_cache_tables", len(buildCache))
	} else {
		c.logger.Info("routing to single worker pipeline",
			"query", queryID)
		physStages = []physical.Stage{{
			ID:    "pipeline-0",
			Type:  "pipeline",
			Tasks: 1,
		}}
	}

	// Expand scans to target remote clusters when table exists on multiple clusters
	physStages = planner.ExpandFederatedScans(physStages)

	if len(physStages) == 0 {
		return &SQLResult{QueryID: queryID, Plan: planStr, Elapsed: time.Since(start)}, nil
	}

	// Store query metadata for task creation and result retrieval
	c.mu.Lock()
	qm := &queryMeta{stages: physStages, planStr: planStr, sqlText: sql, mergeInfo: probeSplitMergeInfo}
	// Propagate or create distributed trace context.
	// If OTel tracing is active, use the OTel span's IDs so worker spans
	// appear as children in the trace backend.
	tc := distributed.TraceFromContext(ctx)
	if tc.TraceID == "" {
		if otelTraceID := telemetry.TraceIDFromContext(ctx); otelTraceID != "" {
			tc.TraceID = otelTraceID
			tc.SpanID = telemetry.SpanIDFromContext(ctx)
			tc.TraceFlags = 0x01
		} else {
			tc = distributed.NewTraceContext()
		}
	}
	qm.trace = tc
	if id := auth.IdentityFromContext(ctx); id != nil {
		qm.identityName = id.Name
		qm.identityRole = id.Role
	}
	// Serialize ABAC table decisions for worker-side enforcement
	if td := auth.TableDecisionsFromContext(ctx); len(td) > 0 {
		sd := auth.SerializedDecision{
			Allowed:        true,
			TableDecisions: map[string]*auth.TableDecision(td),
		}
		if data, err := json.Marshal(sd); err == nil {
			qm.policyDecisionJSON = data
		}
	}
	c.queryMetas[queryID] = qm
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

	// HA: persist query state after planning
	c.saveQueryState(ctx, queryID, sql, "executing", nil)

	// Subscribe for results
	doneCh := make(chan struct{}, 1)
	c.subscribeResults(ctx, queryID, doneCh)

	// Publish leaf stage tasks (stages with no dependencies)
	for _, s := range physStages {
		if len(s.Dependencies) > 0 {
			continue
		}
		tasks := c.createTasksForStage(queryID, s, nil)
		// Update tracker with actual task count and mark as scheduled
		// (prevents GetReadyStages from re-scheduling these).
		c.tracker.SetStageTasks(queryID, s.ID, len(tasks))
		c.tracker.MarkScheduled(queryID, s.ID)
		if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
			c.tracker.Fail(queryID, err.Error())
			return nil, fmt.Errorf("publishing leaf tasks: %w", err)
		}
	}

	// Wait for all stages to complete
	select {
	case <-doneCh:
	case <-ctx.Done():
		c.tracker.Fail(queryID, ctx.Err().Error())
		return nil, ctx.Err()
	}

	info := c.tracker.Get(queryID)
	if info.State == QueryStateFailed {
		return &SQLResult{
			QueryID: queryID,
			Error:   info.Error,
			Elapsed: time.Since(start),
			Plan:    planStr,
		}, fmt.Errorf("query failed: %s", info.Error)
	}

	// Probe-split merge: check if we need to re-aggregate partial results.
	c.mu.Lock()
	qmForMerge := c.queryMetas[queryID]
	c.mu.Unlock()

	// Read results from the final stage. When probe-split merge is needed,
	// force full materialization so S3-stored results are fetched for merging.
	needsMerge := qmForMerge != nil && qmForMerge.mergeInfo != nil
	batches, columns, totalRows, err := c.readFinalResults(ctx, queryID, physStages, needsMerge)
	if qmForMerge != nil && qmForMerge.mergeInfo != nil && len(batches) > 0 {
		merged, mergedRows, mergeErr := c.mergeProbePartials(batches, columns, qmForMerge.mergeInfo)
		if mergeErr != nil {
			c.logger.Error("probe-split merge failed", "query", queryID, "error", mergeErr)
		} else {
			batches = merged
			totalRows = mergedRows
		}
	}

	// Release tracker's inline result data — it's been decoded into batches.
	// Without this, the raw compressed bytes stay in memory until the reaper
	// cleans up the tracker entry (up to 5 minutes).
	c.tracker.ClearResults(queryID)

	// Synchronous path: we have all data locally, clean up queryMetas
	// immediately. The tracker entry is kept for status/list APIs and
	// reaped by StartQueryReaper after the TTL.
	c.mu.Lock()
	delete(c.queryMetas, queryID)
	c.mu.Unlock()

	c.deleteQueryState(ctx, queryID)

	if err != nil {
		return &SQLResult{
			QueryID:     queryID,
			ResultFiles: info.ResultFiles,
			TotalRows:   info.TotalRows,
			Elapsed:     time.Since(start),
			Plan:        planStr,
		}, nil // return what we have even if reading fails
	}

	return &SQLResult{
		QueryID:     queryID,
		Columns:     columns,
		Batches:     batches,
		ResultFiles: info.ResultFiles,
		TotalRows:   totalRows,
		Elapsed:     time.Since(start),
		Plan:        planStr,
	}, nil
}

// createTasksForStage creates distributed tasks for a given stage.
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
	if clusterID == "" {
		clusterID = c.catalog.ClusterID()
	}
	c.mu.Lock()
	qm := c.queryMetas[queryID]
	c.mu.Unlock()
	for i := range tasks {
		tasks[i].ClusterID = clusterID
		if qm != nil {
			tasks[i].IdentityName = qm.identityName
			tasks[i].IdentityRole = qm.identityRole
			tasks[i].TraceID = qm.trace.TraceID
			tasks[i].SpanID = qm.trace.SpanID
			tasks[i].TraceFlags = qm.trace.TraceFlags
			tasks[i].PolicyDecisionJSON = qm.policyDecisionJSON
		}
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
		for i, files := range filePartitions {
			tasks[i] = distributed.Task{
				ID:               uuid.New().String()[:8],
				QueryID:          queryID,
				StageID:          stage.ID,
				Type:             distributed.TaskTypePipeline,
				SQLText:          sqlText,
				DataBucket:       c.config.ResultBucket,
				ResultBucket:     c.config.ResultBucket,
				ResultPrefix:     resultPrefix,
				ScanFileFilter:   map[string][]string{stage.ProbeSplitAlias: files},
				PreScannedInputs: stage.BuildCachePreScans,
				PartialAggregate: qm != nil && qm.mergeInfo != nil && qm.mergeInfo.HasAggregate,
				CreatedAt:        time.Now(),
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

	// Fetch S3-stored results in parallel for probe-split merge
	if len(s3Fetches) > 0 {
		type fetchResult struct {
			data []byte
			err  error
		}
		fetchResults := make([]fetchResult, len(s3Fetches))
		var wg sync.WaitGroup
		for fi, sf := range s3Fetches {
			wg.Add(1)
			go func(i int, path string) {
				defer wg.Done()
				data, err := c.fetchResultData(ctx, queryID, path)
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
	totalRows := s3Rows
	for _, d := range slot {
		if len(d.columns) > 0 && len(columns) == 0 {
			columns = d.columns
		}
		totalRows += d.rows
		allBatches = append(allBatches, d.batches...)
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
//
// For small result sets (typical: <100K rows), this runs in-memory on the
// coordinator with negligible overhead.
func (c *Coordinator) mergeProbePartials(batches []*batch.RecordBatch, columns []string, mi *logical.MergeInfo) ([]*batch.RecordBatch, int64, error) {
	if len(batches) == 0 {
		return nil, 0, nil
	}

	// Build column name → index mapping
	colIdx := make(map[string]int, len(columns))
	for i, col := range columns {
		colIdx[col] = i
	}

	if mi.HasAggregate {
		if len(mi.GroupBy) > 0 {
			batches = c.reAggregatePartials(batches, columns, colIdx, mi)
		} else {
			// Scalar aggregate (no GROUP BY): merge N worker partials into 1 row.
			// Each worker returned 1 row with partial SUM/COUNT/MIN/MAX.
			batches = c.mergeScalarAggregates(batches, columns, colIdx, mi)
		}
	}
	if mi.HasDistinct {
		batches = c.deduplicatePartials(batches, columns)
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

// deduplicatePartials removes duplicate rows across probe-split partial results.
func (c *Coordinator) deduplicatePartials(batches []*batch.RecordBatch, columns []string) []*batch.RecordBatch {
	if len(batches) == 0 {
		return batches
	}
	type rowKey string
	seen := make(map[rowKey]bool)
	var unique []map[string]any
	for _, b := range batches {
		rows := b.ToRows()
		for _, row := range rows {
			var key strings.Builder
			for _, col := range columns {
				fmt.Fprintf(&key, "%v\x00", row[col])
			}
			k := rowKey(key.String())
			if !seen[k] {
				seen[k] = true
				unique = append(unique, row)
			}
		}
	}
	if len(unique) == 0 {
		return nil
	}
	return []*batch.RecordBatch{batch.FromRows(batches[0].Schema, unique)}
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

	type aggCol struct {
		idx      int
		mergeOp  string // "sum", "min", "max"
		origFunc string
	}
	var aggCols []aggCol
	for _, ae := range mi.AggExprs {
		idx, ok := colIdx[ae.OutputCol]
		if !ok {
			continue
		}
		mergeOp := "sum" // COUNT, SUM → SUM
		switch strings.ToLower(ae.Func) {
		case "min":
			mergeOp = "min"
		case "max":
			mergeOp = "max"
		}
		aggCols = append(aggCols, aggCol{idx: idx, mergeOp: mergeOp, origFunc: ae.Func})
	}

	if len(aggCols) == 0 {
		return batches
	}

	// Build a map from group key → merged row values.
	// Keys are encoded as raw bytes (no fmt.Fprint overhead).
	type mergedRow struct {
		groupVals []any     // group-by column values
		aggVals   []float64 // aggregate values
	}
	groups := make(map[string]*mergedRow)
	var groupOrder []string // preserve insertion order

	schema := batches[0].Schema

	// Reusable key buffer — avoids per-row allocation
	keyBuf := make([]byte, 0, 256)

	for _, b := range batches {
		nRows := b.ActiveLen()
		sel := b.Sel
		for ri := 0; ri < nRows; ri++ {
			row := ri
			if sel != nil {
				row = int(sel[ri])
			}

			// Build group key using direct byte encoding
			keyBuf = keyBuf[:0]
			groupVals := make([]any, len(groupByIdx))
			for gi, ci := range groupByIdx {
				if gi > 0 {
					keyBuf = append(keyBuf, 0)
				}
				switch schema[ci].Type {
				case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
					v := b.Columns[ci].Int32Data[row]
					groupVals[gi] = v
					keyBuf = strconv.AppendInt(keyBuf, int64(v), 10)
				case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
					v := b.Columns[ci].Int64Data[row]
					groupVals[gi] = v
					keyBuf = strconv.AppendInt(keyBuf, v, 10)
				case parquet.TypeFloat32:
					v := b.Columns[ci].Float32Data[row]
					groupVals[gi] = v
					keyBuf = strconv.AppendFloat(keyBuf, float64(v), 'g', -1, 32)
				case parquet.TypeFloat64:
					v := b.Columns[ci].Float64Data[row]
					groupVals[gi] = v
					keyBuf = strconv.AppendFloat(keyBuf, v, 'g', -1, 64)
				case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
					v := b.Columns[ci].BytesData.Value(row)
					groupVals[gi] = string(v)
					keyBuf = append(keyBuf, v...)
				case parquet.TypeBool:
					v := b.Columns[ci].BoolData[row]
					groupVals[gi] = v
					if v {
						keyBuf = append(keyBuf, '1')
					} else {
						keyBuf = append(keyBuf, '0')
					}
				default:
					val := extractValue(b.Columns[ci], row, schema[ci].Type)
					groupVals[gi] = val
					keyBuf = fmt.Appendf(keyBuf, "%v", val)
				}
			}
			key := string(keyBuf)

			mr, exists := groups[key]
			if !exists {
				mr = &mergedRow{
					groupVals: groupVals,
					aggVals:   make([]float64, len(aggCols)),
				}
				// Initialize all aggregate values from first row
				for ai, ac := range aggCols {
					mr.aggVals[ai] = extractFloat64(b.Columns[ac.idx], row, schema[ac.idx].Type)
				}
				groups[key] = mr
				groupOrder = append(groupOrder, key)
				continue
			}

			// Merge aggregate values into existing group
			for ai, ac := range aggCols {
				v := extractFloat64(b.Columns[ac.idx], row, schema[ac.idx].Type)
				switch ac.mergeOp {
				case "sum":
					mr.aggVals[ai] += v
				case "min":
					if v < mr.aggVals[ai] {
						mr.aggVals[ai] = v
					}
				case "max":
					if v > mr.aggVals[ai] {
						mr.aggVals[ai] = v
					}
				}
			}
		}
	}

	// Build result batch(es) from merged groups
	result := batch.NewRecordBatch(schema, len(groupOrder))
	for ri, key := range groupOrder {
		mr := groups[key]

		// Set group-by columns
		for gi, ci := range groupByIdx {
			setValueFromAny(result.Columns[ci], ri, mr.groupVals[gi], schema[ci].Type)
		}

		// Set aggregate columns
		for ai, ac := range aggCols {
			setFloat64Value(result.Columns[ac.idx], ri, mr.aggVals[ai], schema[ac.idx].Type)
		}

		// Mark all columns as valid (not null)
		for ci := range schema {
			result.Columns[ci].Nulls.SetValid(ri)
		}
	}
	result.Len = len(groupOrder)

	return []*batch.RecordBatch{result}
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
	default:
		return nil
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
		merged, mergedRows, mergeErr := c.mergeProbePartials(batches, columns, meta.mergeInfo)
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
