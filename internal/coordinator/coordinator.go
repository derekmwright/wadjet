package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/distributed"
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
	MaxInflight    int // max concurrent queries, 0 = default (64)
}

// queryMeta stores per-query metadata needed for later result retrieval.
type queryMeta struct {
	stages       []physical.Stage
	planStr      string
	identityName string // caller identity for task propagation
	identityRole string
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
	logger    *slog.Logger

	mu         sync.Mutex
	resultSubs map[string]context.CancelFunc          // queryID -> cancel
	stageSpecs map[string]map[string]physical.Stage   // queryID -> stageID -> stage spec
	queryMetas map[string]*queryMeta                  // queryID -> metadata for result retrieval
	querySem   chan struct{}                           // limits concurrent inflight queries
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
		workers:    NewWorkerRegistry(nc, logger),
		logger:     logger,
		resultSubs: make(map[string]context.CancelFunc),
		queryMetas: make(map[string]*queryMeta),
		querySem:   make(chan struct{}, maxInflight),
	}
	return c
}

// Workers returns the worker registry for inspecting active workers.
func (c *Coordinator) Workers() *WorkerRegistry {
	return c.workers
}

// Cleaner returns the result cleaner, creating it if needed.
func (c *Coordinator) Cleaner(store objstore.Store, bucket string) *ResultCleaner {
	if c.cleaner == nil {
		c.cleaner = NewResultCleaner(store, bucket, 0, c.logger)
	}
	return c.cleaner
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
	queryID := uuid.New().String()[:8]

	manifest, err := c.catalog.GetManifest(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}

	// Build scan tasks: one per file
	var tasks []distributed.Task
	stageID := "scan-0"

	for _, part := range manifest.Partitions {
		if len(partFilter) > 0 {
			match := true
			for k, v := range partFilter {
				if part.Values[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		for _, file := range part.Files {
			taskID := uuid.New().String()[:8]
			tasks = append(tasks, distributed.Task{
				ID:              taskID,
				QueryID:         queryID,
				StageID:         stageID,
				Type:            distributed.TaskTypeScan,
				TableName:       tableName,
				Files:           []string{file.Path},
				PartitionFilter: partFilter,
				Columns:         columns,
				ResultBucket:    c.config.ResultBucket,
				ResultPrefix:    fmt.Sprintf("queries/%s/%s/", queryID, stageID),
				CreatedAt:       time.Now(),
			})
		}
	}

	if len(tasks) == 0 {
		return &QueryResult{
			QueryID: queryID,
			State:   QueryStateCompleted.String(),
		}, nil
	}

	// Register with tracker
	stages := map[string]*StageInfo{
		stageID: {
			StageID:    stageID,
			Type:       distributed.TaskTypeScan,
			TotalTasks: len(tasks),
		},
	}
	c.tracker.Register(queryID, fmt.Sprintf("SCAN %s", tableName), stages, []string{stageID})
	c.tracker.Start(queryID)

	// Start listening for results
	resultCh := make(chan struct{}, 1)
	c.subscribeResults(ctx, queryID, resultCh)

	// Publish tasks
	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		c.tracker.Fail(queryID, err.Error())
		return nil, fmt.Errorf("publishing tasks: %w", err)
	}

	// Wait for completion
	select {
	case <-resultCh:
		// All tasks complete
	case <-ctx.Done():
		c.tracker.Fail(queryID, ctx.Err().Error())
		return nil, ctx.Err()
	}

	info := c.tracker.Get(queryID)
	return &QueryResult{
		QueryID:     queryID,
		State:       info.State.String(),
		ResultFiles: info.ResultFiles,
		TotalRows:   info.TotalRows,
		Elapsed:     time.Since(info.StartTime),
		Error:       info.Error,
	}, nil
}

// SQLResult holds the full result of a distributed SQL query.
type SQLResult struct {
	QueryID     string
	Columns     []string
	Rows        []map[string]any
	ResultFiles []string
	TotalRows   int64
	Elapsed     time.Duration
	Plan        string
	Error       string
}

// ExecuteSQL parses SQL, plans, distributes across workers, and collects results.
func (c *Coordinator) ExecuteSQL(ctx context.Context, sql string) (*SQLResult, error) {
	// Backpressure: limit concurrent inflight queries.
	select {
	case c.querySem <- struct{}{}:
		defer func() { <-c.querySem }()
	case <-ctx.Done():
		return nil, fmt.Errorf("query queue full: %w", ctx.Err())
	}

	start := time.Now()
	queryID := uuid.New().String()[:8]

	// Parse
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
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
	physStages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		return nil, fmt.Errorf("physical plan: %w", err)
	}

	// Expand scans to target remote clusters when table exists on multiple clusters
	physStages = planner.ExpandFederatedScans(physStages)

	if len(physStages) == 0 {
		return &SQLResult{QueryID: queryID, Plan: planStr, Elapsed: time.Since(start)}, nil
	}

	// Store stage specs and query metadata for task creation and result retrieval
	c.mu.Lock()
	if c.stageSpecs == nil {
		c.stageSpecs = make(map[string]map[string]physical.Stage)
	}
	specMap := make(map[string]physical.Stage, len(physStages))
	for _, s := range physStages {
		specMap[s.ID] = s
	}
	c.stageSpecs[queryID] = specMap
	qm := &queryMeta{stages: physStages, planStr: planStr}
	if id := auth.IdentityFromContext(ctx); id != nil {
		qm.identityName = id.Name
		qm.identityRole = id.Role
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

	// Subscribe for results with multi-stage scheduling
	doneCh := make(chan struct{}, 1)
	c.subscribeResultsMultiStage(ctx, queryID, doneCh)

	// Publish leaf stage tasks (stages with no dependencies)
	for _, s := range physStages {
		if len(s.Dependencies) > 0 {
			continue
		}
		tasks := c.createTasksForStage(queryID, s, nil)
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

	// Read results from the final stage
	rows, columns, err := c.readFinalResults(ctx, queryID, physStages)
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
		Rows:        rows,
		ResultFiles: info.ResultFiles,
		TotalRows:   int64(len(rows)),
		Elapsed:     time.Since(start),
		Plan:        planStr,
	}, nil
}

// createTasksForStage creates distributed tasks for a given stage.
// For intermediate stages, depResults maps dependency stageID → result file paths.
func (c *Coordinator) createTasksForStage(queryID string, stage physical.Stage, depResults map[string][]string) []distributed.Task {
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)

	var tasks []distributed.Task
	switch stage.Type {
	case "scan":
		tasks = c.createScanTasks(queryID, stage, resultPrefix)
	case "aggregate":
		tasks = c.createAggregateTasks(queryID, stage, resultPrefix, depResults)
	case "sort":
		tasks = c.createSortTasks(queryID, stage, resultPrefix, depResults)
	case "hash_join", "broadcast_join":
		tasks = c.createJoinTasks(queryID, stage, resultPrefix, depResults)
	case "window":
		tasks = c.createWindowTasks(queryID, stage, resultPrefix, depResults)
	default:
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
		}
	}
	return tasks
}

// coalesceScanTargetBytes is the minimum total bytes per scan task.
// Files smaller than this are grouped together to reduce task overhead.
const coalesceScanTargetBytes int64 = 32 * 1024 * 1024 // 32 MB

func (c *Coordinator) createScanTasks(queryID string, stage physical.Stage, resultPrefix string) []distributed.Task {
	if len(stage.ScanFiles) == 0 {
		// Create at least one empty task so the stage completes
		return []distributed.Task{{
			ID:           uuid.New().String()[:8],
			QueryID:      queryID,
			StageID:      stage.ID,
			Type:         distributed.TaskTypeScan,
			TableName:    stage.TableName,
			ResultBucket: c.config.ResultBucket,
			ResultPrefix: resultPrefix,
			CreatedAt:    time.Now(),
		}}
	}

	// Look up file sizes from the catalog for coalescing
	fileSizes := c.getFileSizes(stage.TableName)

	// Coalesce small files into batches targeting coalesceScanTargetBytes
	var tasks []distributed.Task
	var batch []string
	var batchBytes int64

	for _, filePath := range stage.ScanFiles {
		fileSize := fileSizes[filePath] // 0 if unknown
		batch = append(batch, filePath)
		batchBytes += fileSize

		// Flush batch when target reached or file size unknown (treat as large)
		if batchBytes >= coalesceScanTargetBytes || fileSize == 0 {
			tasks = append(tasks, distributed.Task{
				ID:              uuid.New().String()[:8],
				QueryID:         queryID,
				StageID:         stage.ID,
				Type:            distributed.TaskTypeScan,
				TableName:       stage.TableName,
				Files:           batch,
				Columns:         stage.Columns,
				PartitionFilter: stage.PartitionFilter,
				FilterExprs:     stage.FilterExprs,
				ResultBucket:    c.config.ResultBucket,
				ResultPrefix:    resultPrefix,
				CreatedAt:       time.Now(),
			})
			batch = nil
			batchBytes = 0
		}
	}

	// Flush remaining
	if len(batch) > 0 {
		tasks = append(tasks, distributed.Task{
			ID:              uuid.New().String()[:8],
			QueryID:         queryID,
			StageID:         stage.ID,
			Type:            distributed.TaskTypeScan,
			TableName:       stage.TableName,
			Files:           batch,
			Columns:         stage.Columns,
			PartitionFilter: stage.PartitionFilter,
			FilterExprs:     stage.FilterExprs,
			ResultBucket:    c.config.ResultBucket,
			ResultPrefix:    resultPrefix,
			CreatedAt:       time.Now(),
		})
	}

	return tasks
}

// getFileSizes returns a map of file path → size for a table's files.
// Best-effort: returns empty map if catalog lookup fails.
func (c *Coordinator) getFileSizes(tableName string) map[string]int64 {
	if tableName == "" {
		return nil
	}
	manifest, err := c.catalog.GetManifest(context.Background(), tableName)
	if err != nil {
		return nil
	}
	sizes := make(map[string]int64)
	for _, p := range manifest.Partitions {
		for _, f := range p.Files {
			sizes[f.Path] = f.SizeBytes
		}
	}
	return sizes
}

func (c *Coordinator) createAggregateTasks(queryID string, stage physical.Stage, resultPrefix string, depResults map[string][]string) []distributed.Task {
	// Pre-count total files across dependencies
	totalFiles := 0
	for _, depID := range stage.Dependencies {
		totalFiles += len(depResults[depID])
	}
	inputFiles := make([]string, 0, totalFiles)
	for _, depID := range stage.Dependencies {
		inputFiles = append(inputFiles, depResults[depID]...)
	}

	var aggSpecs []distributed.AggSpec
	for _, a := range stage.AggSpecs {
		aggSpecs = append(aggSpecs, distributed.AggSpec{
			Func:      a.Func,
			InputCol:  a.InputCol,
			OutputCol: a.OutputCol,
		})
	}

	// Partition input files across multiple parallel aggregate tasks when the
	// input set is large enough to benefit. Each partial task produces grouped
	// partial aggregates; downstream stages re-aggregate to merge. For SUM this
	// produces correct partials (SUM of SUMs). For COUNT, the coordinator rewrites
	// to SUM-of-COUNTs at the merge stage. For MIN/MAX, partials are composable
	// directly. AVG is decomposed into SUM+COUNT during planning.
	//
	// With <= 4 input files, a single task avoids scheduling overhead.
	const minFilesForParallel = 4
	const maxPartialTasks = 8

	if len(inputFiles) <= minFilesForParallel {
		return []distributed.Task{{
			ID:           uuid.New().String()[:8],
			QueryID:      queryID,
			StageID:      stage.ID,
			Type:         distributed.TaskTypeAggregate,
			GroupByCols:  stage.GroupByCols,
			Aggregates:   aggSpecs,
			InputFiles:   inputFiles,
			ResultBucket: c.config.ResultBucket,
			ResultPrefix: resultPrefix,
			CreatedAt:    time.Now(),
		}}
	}

	// Split files into chunks for parallel partial aggregation
	numTasks := len(inputFiles) / minFilesForParallel
	if numTasks > maxPartialTasks {
		numTasks = maxPartialTasks
	}
	if numTasks < 2 {
		numTasks = 2
	}

	tasks := make([]distributed.Task, 0, numTasks)
	chunkSize := (len(inputFiles) + numTasks - 1) / numTasks

	for i := 0; i < len(inputFiles); i += chunkSize {
		end := i + chunkSize
		if end > len(inputFiles) {
			end = len(inputFiles)
		}
		tasks = append(tasks, distributed.Task{
			ID:           uuid.New().String()[:8],
			QueryID:      queryID,
			StageID:      stage.ID,
			Type:         distributed.TaskTypeAggregate,
			GroupByCols:  stage.GroupByCols,
			Aggregates:   aggSpecs,
			InputFiles:   inputFiles[i:end],
			ResultBucket: c.config.ResultBucket,
			ResultPrefix: resultPrefix,
			CreatedAt:    time.Now(),
		})
	}

	return tasks
}

func (c *Coordinator) createSortTasks(queryID string, stage physical.Stage, resultPrefix string, depResults map[string][]string) []distributed.Task {
	totalFiles := 0
	for _, depID := range stage.Dependencies {
		totalFiles += len(depResults[depID])
	}
	inputFiles := make([]string, 0, totalFiles)
	for _, depID := range stage.Dependencies {
		inputFiles = append(inputFiles, depResults[depID]...)
	}

	var sortKeys []distributed.SortKeySpec
	for _, sk := range stage.SortKeys {
		sortKeys = append(sortKeys, distributed.SortKeySpec{Column: sk.Column, Desc: sk.Desc})
	}

	return []distributed.Task{{
		ID:           uuid.New().String()[:8],
		QueryID:      queryID,
		StageID:      stage.ID,
		Type:         distributed.TaskTypeSort,
		SortKeys:     sortKeys,
		Limit:        stage.Limit,
		InputFiles:   inputFiles,
		ResultBucket: c.config.ResultBucket,
		ResultPrefix: resultPrefix,
		CreatedAt:    time.Now(),
	}}
}

func (c *Coordinator) createJoinTasks(queryID string, stage physical.Stage, resultPrefix string, depResults map[string][]string) []distributed.Task {
	var probeFiles, buildFiles []string
	if stage.LeftDepStage != "" {
		probeFiles = depResults[stage.LeftDepStage]
	}
	if stage.RightDepStage != "" {
		buildFiles = depResults[stage.RightDepStage]
	}

	return []distributed.Task{{
		ID:            uuid.New().String()[:8],
		QueryID:       queryID,
		StageID:       stage.ID,
		Type:          distributed.TaskTypeJoin,
		JoinType:      stage.JoinType,
		JoinLeftKeys:  stage.JoinLeftKeys,
		JoinRightKeys: stage.JoinRightKeys,
		InputFiles:    probeFiles,
		BuildFiles:    buildFiles,
		ResultBucket:  c.config.ResultBucket,
		ResultPrefix:  resultPrefix,
		CreatedAt:     time.Now(),
	}}
}

func (c *Coordinator) createWindowTasks(queryID string, stage physical.Stage, resultPrefix string, depResults map[string][]string) []distributed.Task {
	totalFiles := 0
	for _, depID := range stage.Dependencies {
		totalFiles += len(depResults[depID])
	}
	inputFiles := make([]string, 0, totalFiles)
	for _, depID := range stage.Dependencies {
		inputFiles = append(inputFiles, depResults[depID]...)
	}

	var winCols []distributed.WindowColSpec
	for _, wc := range stage.WindowCols {
		var orderBy []distributed.SortKeySpec
		for _, ob := range wc.OrderBy {
			orderBy = append(orderBy, distributed.SortKeySpec{Column: ob.Column, Desc: ob.Desc})
		}
		winCols = append(winCols, distributed.WindowColSpec{
			Func:        wc.Func,
			InputCol:    wc.InputCol,
			OutputCol:   wc.OutputCol,
			PartitionBy: wc.PartitionBy,
			OrderBy:     orderBy,
		})
	}

	return []distributed.Task{{
		ID:           uuid.New().String()[:8],
		QueryID:      queryID,
		StageID:      stage.ID,
		Type:         distributed.TaskTypeWindow,
		WindowCols:   winCols,
		InputFiles:   inputFiles,
		ResultBucket: c.config.ResultBucket,
		ResultPrefix: resultPrefix,
		CreatedAt:    time.Now(),
	}}
}

// subscribeResultsMultiStage handles result notifications with multi-stage DAG scheduling.
// Heavy work (materialization, task publishing) is offloaded to a background goroutine
// to avoid blocking the NATS message handler.
func (c *Coordinator) subscribeResultsMultiStage(ctx context.Context, queryID string, done chan<- struct{}) {
	subject := distributed.QueryResultSubject(queryID)

	// Channel for offloading stage scheduling from the NATS callback.
	// Buffered generously to avoid dropping stage completion events under
	// high concurrency. A dropped event causes downstream stages to never
	// schedule, deadlocking the query.
	type stageEvent struct {
		stageID string
	}
	stageCh := make(chan stageEvent, 256)

	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var result distributed.ResultNotification
		if err := distributed.Unmarshal(msg.Data, &result); err != nil {
			c.logger.Error("failed to unmarshal result", "error", err)
			return
		}

		c.logger.Debug("received result",
			"task_id", result.TaskID,
			"query_id", result.QueryID,
			"stage_id", result.StageID,
			"success", result.Success,
			"rows", result.NumRows,
		)

		stageComplete := c.tracker.RecordResult(result)
		if !stageComplete {
			return
		}

		// Check if entire query is done
		if c.tracker.IsComplete(queryID) {
			c.tracker.Complete(queryID)
			c.cleanupQuery(queryID)
			select {
			case done <- struct{}{}:
			default:
			}
			return
		}

		// Stage completed — notify the background scheduler goroutine.
		// Use blocking send to guarantee delivery. The background goroutine
		// drains this channel promptly so backpressure here is acceptable.
		stageCh <- stageEvent{stageID: result.StageID}
	})
	if err != nil {
		c.logger.Error("failed to subscribe to results", "error", err, "subject", subject)
		return
	}

	c.mu.Lock()
	cancelCtx, cancel := context.WithCancel(ctx)
	c.resultSubs[queryID] = cancel
	c.mu.Unlock()

	// Background goroutine: handles materialization + downstream task publishing
	// outside of the NATS message callback.
	go func() {
		for {
			select {
			case <-cancelCtx.Done():
				return
			case evt := <-stageCh:
				c.scheduleDownstreamStages(ctx, queryID, evt.stageID, done)
			}
		}
	}()

	go func() {
		<-cancelCtx.Done()
		sub.Unsubscribe()
	}()
}

// scheduleDownstreamStages materializes inline results and publishes tasks for
// newly-ready stages. Called from a background goroutine, not the NATS callback.
func (c *Coordinator) scheduleDownstreamStages(ctx context.Context, queryID, completedStageID string, done chan<- struct{}) {
	readyStages := c.tracker.GetReadyStages(queryID)
	if len(readyStages) == 0 {
		return
	}

	// Materialize inline results to S3 for downstream consumption
	c.materializeInlineResults(ctx, queryID, completedStageID)

	// Collect result files from completed stages
	depResults := c.collectDepResults(queryID)

	c.mu.Lock()
	specs := c.stageSpecs[queryID]
	c.mu.Unlock()

	for _, stageID := range readyStages {
		spec, ok := specs[stageID]
		if !ok {
			c.logger.Error("no stage spec found", "stage_id", stageID)
			continue
		}

		tasks := c.createTasksForStage(queryID, spec, depResults)
		c.tracker.SetStageTasks(queryID, stageID, len(tasks))

		if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
			c.logger.Error("failed to publish stage tasks",
				"stage_id", stageID, "error", err)
			c.tracker.Fail(queryID, fmt.Sprintf("publishing stage %s: %v", stageID, err))
			c.cleanupQuery(queryID)
			select {
			case done <- struct{}{}:
			default:
			}
			return
		}

		c.logger.Info("scheduled stage",
			"query_id", queryID,
			"stage_id", stageID,
			"tasks", len(tasks),
		)
	}
}

// cleanupQuery removes subscription and metadata entries for a finished query.
func (c *Coordinator) cleanupQuery(queryID string) {
	c.mu.Lock()
	if cancel, ok := c.resultSubs[queryID]; ok {
		cancel()
		delete(c.resultSubs, queryID)
	}
	delete(c.stageSpecs, queryID)
	// Keep queryMetas — needed for GetQueryResults
	c.mu.Unlock()
}

// materializeInlineResults writes inline results to S3 so downstream stages can read them.
// Updates the tracker's result entries with the materialized file paths.
func (c *Coordinator) materializeInlineResults(ctx context.Context, queryID, stageID string) {
	results := c.tracker.StageResults(queryID, stageID)
	store := c.catalog.Store()

	for i, r := range results {
		if !r.Success || len(r.InlineData) == 0 || r.ResultPath != "" {
			continue
		}

		// Write inline data to S3
		path := fmt.Sprintf("queries/%s/%s/%s.parquet", queryID, stageID, r.TaskID)
		_, err := store.Put(ctx, c.config.ResultBucket, path,
			bytes.NewReader(r.InlineData), int64(len(r.InlineData)), "application/octet-stream")
		if err != nil {
			c.logger.Error("failed to materialize inline result",
				"path", path, "error", err)
			continue
		}

		// Update tracker with the materialized path
		results[i].ResultPath = path
		c.tracker.UpdateResultPath(queryID, stageID, r.TaskID, path)
	}
}

// collectDepResults gathers result file paths from completed stages.
func (c *Coordinator) collectDepResults(queryID string) map[string][]string {
	info := c.tracker.Get(queryID)
	if info == nil {
		return nil
	}
	depResults := make(map[string][]string)
	for stageID, stage := range info.Stages {
		for _, r := range stage.Results {
			if r.Success && r.ResultPath != "" {
				depResults[stageID] = append(depResults[stageID], r.ResultPath)
			}
		}
	}
	return depResults
}

// readFinalResults reads the result files from the final (last) stage.
// Uses direct columnar page reads to avoid per-row map[string]any deserialization.
func (c *Coordinator) readFinalResults(ctx context.Context, queryID string, stages []physical.Stage) ([]map[string]any, []string, error) {
	if len(stages) == 0 {
		return nil, nil, nil
	}

	// Find the final stage (last in topological order)
	finalStage := stages[len(stages)-1]

	// Get results for the final stage
	results := c.tracker.StageResults(queryID, finalStage.ID)
	if len(results) == 0 {
		return nil, nil, nil
	}

	store := c.catalog.Store()
	var allRows []map[string]any
	var columns []string

	for _, r := range results {
		if !r.Success {
			continue
		}

		var data []byte
		if len(r.InlineData) > 0 {
			data = r.InlineData
		} else if r.ResultPath != "" {
			rc, _, err := store.Get(ctx, c.config.ResultBucket, r.ResultPath)
			if err != nil {
				c.logger.Warn("failed to read result file", "path", r.ResultPath, "error", err)
				continue
			}
			data, err = io.ReadAll(rc)
			rc.Close()
			if err != nil {
				continue
			}
		} else {
			continue
		}

		reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			continue
		}

		schema := reader.Schema().Columns

		// Extract column names from schema
		if len(columns) == 0 && len(schema) > 0 {
			columns = make([]string, len(schema))
			for i, col := range schema {
				columns[i] = col.Name
			}
		}

		// Direct columnar read — bypasses per-row map allocation in ReadRows
		batches, err := scan.ReadFileBatches(reader, schema, nil)
		if err != nil {
			continue
		}
		for _, b := range batches {
			allRows = append(allRows, b.ToRows()...)
		}
	}

	return allRows, columns, nil
}

func (c *Coordinator) subscribeResults(ctx context.Context, queryID string, done chan<- struct{}) {
	subject := distributed.QueryResultSubject(queryID)
	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var result distributed.ResultNotification
		if err := distributed.Unmarshal(msg.Data, &result); err != nil {
			c.logger.Error("failed to unmarshal result", "error", err)
			return
		}

		c.logger.Debug("received result",
			"task_id", result.TaskID,
			"query_id", result.QueryID,
			"stage_id", result.StageID,
			"success", result.Success,
		)

		stageComplete := c.tracker.RecordResult(result)
		if stageComplete && c.tracker.IsComplete(queryID) {
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

	// Generate distributed stages
	planner := physical.NewPlanner(c.catalog)
	physStages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		return "", "", fmt.Errorf("physical plan: %w", err)
	}

	// Expand scans to target remote clusters when table exists on multiple clusters
	physStages = planner.ExpandFederatedScans(physStages)

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

	// Store stage specs and metadata
	c.mu.Lock()
	if c.stageSpecs == nil {
		c.stageSpecs = make(map[string]map[string]physical.Stage)
	}
	specMap := make(map[string]physical.Stage, len(physStages))
	for _, s := range physStages {
		specMap[s.ID] = s
	}
	c.stageSpecs[queryID] = specMap
	c.queryMetas[queryID] = &queryMeta{stages: physStages, planStr: planStr}
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

	// Use a background context for the subscription and task publishing since
	// this runs asynchronously after the caller returns.
	asyncCtx := context.Background()

	// Subscribe for results with multi-stage scheduling (non-blocking callback)
	doneCh := make(chan struct{}, 1)
	c.subscribeResultsMultiStage(asyncCtx, queryID, doneCh)

	// Publish leaf stage tasks
	for _, s := range physStages {
		if len(s.Dependencies) > 0 {
			continue
		}
		tasks := c.createTasksForStage(queryID, s, nil)
		if err := c.scheduler.PublishTasks(asyncCtx, tasks); err != nil {
			c.tracker.Fail(queryID, err.Error())
			return "", "", fmt.Errorf("publishing leaf tasks: %w", err)
		}
	}

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

	rows, columns, err := c.readFinalResults(ctx, queryID, meta.stages)
	if err != nil {
		return &SQLResult{
			QueryID:     queryID,
			ResultFiles: info.ResultFiles,
			TotalRows:   info.TotalRows,
			Elapsed:     elapsed,
			Plan:        planStr,
		}, nil
	}

	return &SQLResult{
		QueryID:     queryID,
		Columns:     columns,
		Rows:        rows,
		ResultFiles: info.ResultFiles,
		TotalRows:   int64(len(rows)),
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
