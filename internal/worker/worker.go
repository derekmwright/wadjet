package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/metrics"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/telemetry"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Config holds worker configuration.
type Config struct {
	NATSUrl          string
	WorkerID         string
	ClusterID        string // cluster this worker belongs to (for federated routing)
	MaxConcurrent    int    // max concurrent tasks
	CacheBytes       int64  // local LRU cache size
	MemoryBudget     int64  // per-task memory budget in bytes (0 = unlimited, no spill); used as legacy fallback when SharedPoolBudget is unset
	SharedPoolBudget int64  // worker-wide memory pool in bytes (0 = derived as MemoryBudget*MaxConcurrent). All concurrent tasks Reserve against this pool; spill triggers fire on cumulative worker pressure.
	SpillDir         string // directory for spill files (default: os temp dir)
	ResultStoreBytes int64  // in-memory result store capacity (0 = disabled, results go to S3)
}

// DefaultConfig returns default worker configuration.
func DefaultConfig() Config {
	return Config{
		MaxConcurrent:    4,
		CacheBytes:       256 * 1024 * 1024, // 256 MB
		ResultStoreBytes: 512 * 1024 * 1024, // 512 MB — avoids S3 round-trips for inter-stage results
	}
}

// Worker pulls tasks from NATS, executes them, and publishes results.
type Worker struct {
	config   Config
	store    objstore.Store
	nc       *nats.Conn
	js       jetstream.JetStream
	executor *Executor
	logger   *slog.Logger
	metrics  *metrics.Metrics

	cancel   context.CancelFunc
	wg       sync.WaitGroup
	drainCh  chan struct{} // closed when drain is requested
	drainOnce sync.Once

	cancelledMu sync.RWMutex
	cancelled   map[string]time.Time // queryID → cancellation time

	activeTasksMu sync.RWMutex
	activeTasks   map[string]struct{} // task IDs currently executing

	// CPU profiling state — started/stopped via NATS profile requests.
	profMu  sync.Mutex
	profBuf *bytes.Buffer // nil when not profiling

	otel *telemetry.Provider // nil = no OTel tracing
}

// New creates a new Worker.
func New(cfg Config, store objstore.Store, nc *nats.Conn, js jetstream.JetStream, logger *slog.Logger) *Worker {
	if cfg.WorkerID == "" {
		cfg.WorkerID = "worker-" + uuid.New().String()[:8]
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 4
	}
	if cfg.CacheBytes <= 0 {
		cfg.CacheBytes = 256 * 1024 * 1024
	}
	if logger == nil {
		logger = slog.Default()
	}

	cache := NewLRUCache(cfg.CacheBytes)
	executor := NewExecutor(store, cache, js)
	pool := cfg.SharedPoolBudget
	if pool <= 0 && cfg.MemoryBudget > 0 && cfg.MaxConcurrent > 0 {
		// Legacy callers pass per-task MemoryBudget; reconstruct the
		// worker-wide pool size as `per-task × MaxConcurrent` so all
		// concurrent tasks share one budget. This matches what the
		// per-task budget was originally a slice of.
		pool = cfg.MemoryBudget * int64(cfg.MaxConcurrent)
	}
	executor.SetMemoryBudget(cfg.MemoryBudget, cfg.SpillDir)
	if pool > 0 {
		executor.SetSharedPoolBudget(pool)
	}
	executor.SetLogger(logger)
	executor.SetNATSConn(nc)
	if cfg.ResultStoreBytes > 0 {
		executor.SetResultStore(NewResultStore(cfg.ResultStoreBytes))
	}
	// Same-worker LocalStageCache: producers register their local spill
	// files in here after upload, downstream tasks landing on the same
	// worker mmap them directly instead of round-tripping S3. Skipped when
	// no spill dir is configured (tests / minimal embeddings).
	if cfg.SpillDir != "" {
		executor.SetLocalStageCache(NewLocalStageCache(filepath.Join(cfg.SpillDir, "stage-cache")))
	}
	// Best-effort bind to the coordinator's shared result KV bucket so small
	// stage outputs can round-trip via NATS (~10ms) instead of S3 (~500ms).
	// The coordinator creates the bucket in New(); workers just open it.
	// If unavailable (e.g., KV disabled on coord, different NATS cluster),
	// stage writes/reads silently fall back to S3.
	if kv, kvErr := js.KeyValue(context.Background(), "wadjet_results_data"); kvErr == nil {
		executor.SetResultKV(kv)
		logger.Info("worker KV fast-path enabled", "bucket", "wadjet_results_data")
	} else {
		logger.Debug("worker KV fast-path unavailable", "error", kvErr)
	}

	return &Worker{
		config:    cfg,
		store:     store,
		nc:        nc,
		js:        js,
		executor:  executor,
		logger:    logger,
		cancelled:   make(map[string]time.Time),
		activeTasks: make(map[string]struct{}),
		drainCh:   make(chan struct{}),
	}
}

// SetMetrics attaches Prometheus metrics for spill/memory tracking.
func (w *Worker) SetMetrics(m *metrics.Metrics) {
	w.metrics = m
	w.executor.SetMetrics(m)
}

// Start begins the worker task loop and heartbeat.
func (w *Worker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)

	// Sweep stale build-cache spill files left over from a previous worker
	// crash. Without this, a c7gd.4xlarge worker that crashes mid-query at
	// SF100 leaves multi-GB *.wshf files on the NVMe spill volume; subsequent
	// runs accumulate them and eventually exhaust the disk. Files are named
	// "build-cache-*.wshf" (write-side sink) and "build-cache-load-*.wshf"
	// (read-side mmap source) — both safe to delete on startup since any
	// in-flight reader/writer would be holding an open fd into them.
	if w.config.SpillDir != "" {
		w.sweepStaleBuildCacheFiles()
	}

	// Use cluster-scoped filter so this worker only pulls tasks for its cluster.
	// If ClusterID is empty, subscribe to all tasks (backward compatible).
	filterSubject := distributed.SubjectTasksAll
	if w.config.ClusterID != "" {
		filterSubject = distributed.ClusterTasksFilter(w.config.ClusterID)
	}

	// Create a shared durable consumer per cluster. All workers in the same
	// cluster pull from the same consumer — NATS distributes messages across them.
	// WorkQueuePolicy streams don't allow multiple consumers with the same filter.
	consumerName := "tasks"
	if w.config.ClusterID != "" {
		consumerName = "tasks-" + w.config.ClusterID
	}
	consumer, err := w.js.CreateOrUpdateConsumer(ctx, distributed.StreamTasks, jetstream.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       10 * time.Minute,
		MaxDeliver:    3,
	})
	if err != nil {
		return fmt.Errorf("creating consumer: %w", err)
	}

	// Subscribe to query cancellation messages
	cancelSub, err := w.nc.Subscribe(distributed.CancelSubjectAll(), func(msg *nats.Msg) {
		queryID := string(msg.Data)
		w.cancelledMu.Lock()
		w.cancelled[queryID] = time.Now()
		w.cancelledMu.Unlock()
		// Free per-query state. Cancelled queries won't read any further
		// stage outputs, so their spill files are pure leak otherwise.
		if w.executor.localCache != nil {
			w.executor.localCache.CleanupQuery(queryID)
		}
		w.logger.Debug("query cancelled", "query_id", queryID)
	})
	if err != nil {
		return fmt.Errorf("subscribing to cancellations: %w", err)
	}

	// Subscribe to query completion messages — same cleanup as cancel, but
	// for queries that finished normally. Coordinator publishes once per
	// query in cleanupQuery().
	completeSub, err := w.nc.Subscribe(distributed.CompleteSubjectAll(), func(msg *nats.Msg) {
		queryID := string(msg.Data)
		if w.executor.localCache != nil {
			n := w.executor.localCache.CleanupQuery(queryID)
			if n > 0 {
				w.logger.Debug("released local stage cache",
					"query_id", queryID, "entries", n)
			}
		}
	})
	if err != nil {
		return fmt.Errorf("subscribing to completions: %w", err)
	}

	// Subscribe to drain requests (sent by coordinator or admin)
	drainSubject := fmt.Sprintf("wadjet.worker.%s.drain", w.config.WorkerID)
	w.nc.Subscribe(drainSubject, func(msg *nats.Msg) {
		w.logger.Info("received drain request via NATS")
		w.drainOnce.Do(func() { close(w.drainCh) })
		if msg.Reply != "" {
			msg.Respond([]byte("ok"))
		}
	})

	// Subscribe to profile requests (NATS request/reply)
	w.nc.Subscribe(distributed.SubjectProfileStart, func(msg *nats.Msg) {
		w.handleProfileStart(msg)
	})
	w.nc.Subscribe(distributed.SubjectProfileCollect, func(msg *nats.Msg) {
		w.handleProfileCollect(msg)
	})

	// Unsubscribe on shutdown
	go func() {
		<-ctx.Done()
		cancelSub.Unsubscribe()
		completeSub.Unsubscribe()
	}()

	// Start task pull loop
	sem := make(chan struct{}, w.config.MaxConcurrent)

	// Start heartbeat (needs sem to report active task count)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.heartbeatLoop(ctx, sem)
	}()
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.taskLoop(ctx, consumer, sem)
	}()

	w.logger.Info("worker started",
		"worker_id", w.config.WorkerID,
		"cluster_id", w.config.ClusterID,
		"max_concurrent", w.config.MaxConcurrent,
		"filter_subject", filterSubject,
	)

	return nil
}

// Drain puts the worker into drain mode: it stops pulling new tasks and waits
// for all in-flight tasks to complete, then cancels the context so heartbeat
// and other background loops exit. Use this for zero-downtime rolling updates.
func (w *Worker) Drain() {
	w.drainOnce.Do(func() {
		w.logger.Info("worker entering drain mode", "worker_id", w.config.WorkerID)
		close(w.drainCh)
	})
	// Wait for in-flight tasks to finish, then stop.
	w.wg.Wait()
	if w.cancel != nil {
		w.cancel()
	}
	w.logger.Info("worker drained and stopped", "worker_id", w.config.WorkerID)
}

// Draining returns true if the worker is in drain mode.
func (w *Worker) Draining() bool {
	select {
	case <-w.drainCh:
		return true
	default:
		return false
	}
}

// Stop gracefully stops the worker. The shared per-cluster consumer is left in
// place so other workers can continue pulling tasks.
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()

	w.logger.Info("worker stopped", "worker_id", w.config.WorkerID)
}

// SetTelemetry enables OpenTelemetry tracing on the worker.
func (w *Worker) SetTelemetry(tp *telemetry.Provider) {
	w.otel = tp
}

// sweepStaleBuildCacheFiles deletes leftover build-cache spill files in the
// configured spill directory. Called once at startup. Errors are logged but
// not fatal — the worker can still operate (it just may run out of disk if
// crashes are frequent enough).
func (w *Worker) sweepStaleBuildCacheFiles() {
	dir := w.config.SpillDir
	entries, err := os.ReadDir(dir)
	if err != nil {
		// Spill dir may not exist yet on a fresh worker; that's fine.
		if !os.IsNotExist(err) {
			w.logger.Warn("scanning spill dir for stale build-cache files",
				"dir", dir, "error", err)
		}
		return
	}
	var removed int
	var bytesFreed int64
	for _, e := range entries {
		name := e.Name()
		// Two prefixes are used by the build-cache pipeline:
		//   build-cache-*.wshf       — write-side spill from shuffleStreamSink
		//   build-cache-load-*.wshf  — read-side mmap source download
		if !strings.HasPrefix(name, "build-cache-") || !strings.HasSuffix(name, ".wshf") {
			continue
		}
		full := filepath.Join(dir, name)
		if info, statErr := e.Info(); statErr == nil {
			bytesFreed += info.Size()
		}
		if rmErr := os.Remove(full); rmErr != nil {
			w.logger.Warn("removing stale build-cache file",
				"path", full, "error", rmErr)
			continue
		}
		removed++
	}
	if removed > 0 {
		w.logger.Info("swept stale build-cache spill files",
			"dir", dir, "files", removed, "bytes_freed", bytesFreed)
	}
}

func (w *Worker) taskLoop(ctx context.Context, consumer jetstream.Consumer, sem chan struct{}) {
	// Batch fetch: pull up to available concurrency slots at once to
	// amortize the NATS round-trip overhead.
	for {
		if ctx.Err() != nil {
			return
		}
		// In drain mode, stop pulling new tasks.
		if w.Draining() {
			return
		}

		// Count available slots (non-blocking drain)
		available := 0
		for {
			select {
			case sem <- struct{}{}:
				available++
				if available >= w.config.MaxConcurrent {
					goto fetch
				}
			default:
				goto fetch
			}
		}
	fetch:
		if available == 0 {
			// All slots occupied — wait for one to free up
			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
				available = 1
			}
		}

		msgs, err := consumer.Fetch(available, jetstream.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			for i := 0; i < available; i++ {
				<-sem
			}
			if ctx.Err() != nil {
				return
			}
			continue
		}

		dispatched := 0
		for msg := range msgs.Messages() {
			dispatched++
			w.wg.Add(1)
			go func(m jetstream.Msg) {
				defer w.wg.Done()
				defer func() { <-sem }()
				w.handleTask(ctx, m)
			}(msg)
		}

		// Return unused slots
		for i := dispatched; i < available; i++ {
			<-sem
		}

		if msgs.Error() != nil && ctx.Err() == nil {
			w.logger.Debug("fetch returned", "error", msgs.Error())
		}
	}
}

func (w *Worker) isCancelled(queryID string) bool {
	w.cancelledMu.RLock()
	_, ok := w.cancelled[queryID]
	w.cancelledMu.RUnlock()
	return ok
}

func (w *Worker) handleTask(ctx context.Context, msg jetstream.Msg) {
	var task distributed.Task
	if err := distributed.Unmarshal(msg.Data(), &task); err != nil {
		w.logger.Error("failed to unmarshal task", "error", err)
		msg.Term()
		return
	}

	// Skip tasks for cancelled queries (local cache from broadcast)
	if w.isCancelled(task.QueryID) {
		w.logger.Debug("skipping task for cancelled query",
			"task_id", task.ID, "query_id", task.QueryID)
		msg.Term()
		return
	}

	// Check with coordinator if query is still active. Catches stale tasks
	// that linger in JetStream after a query was killed (e.g., watchdog).
	// ~1ms overhead per task, prevents minutes of wasted work.
	if resp, err := w.nc.Request(distributed.SubjectQueryActive, []byte(task.QueryID), 2*time.Second); err == nil {
		if string(resp.Data) == "0" {
			w.logger.Info("skipping task for inactive query",
				"task_id", task.ID, "query_id", task.QueryID)
			msg.Term()
			return
		}
	}

	// Track active task for heartbeat progress reporting
	w.activeTasksMu.Lock()
	w.activeTasks[task.ID] = struct{}{}
	w.activeTasksMu.Unlock()
	defer func() {
		w.activeTasksMu.Lock()
		delete(w.activeTasks, task.ID)
		w.activeTasksMu.Unlock()
	}()

	// Inject trace context from the task into the Go context
	logAttrsStart := []any{
		"task_id", task.ID,
		"type", task.Type,
		"query_id", task.QueryID,
	}
	if task.TraceID != "" {
		logAttrsStart = append(logAttrsStart, "trace_id", task.TraceID)
	}
	w.logger.Info("executing task", logAttrsStart...)

	// Create a cancellable context for this task, with trace propagation
	taskCtx, taskCancel := context.WithCancel(ctx)
	if task.TraceID != "" {
		tc := distributed.TraceContext{
			TraceID:    task.TraceID,
			SpanID:     task.SpanID,
			TraceFlags: task.TraceFlags,
		}
		taskCtx = distributed.ContextWithTrace(taskCtx, tc)
	}
	defer taskCancel()

	// Start OTel child span linked to coordinator's trace
	if w.otel != nil && task.TraceID != "" {
		var span trace.Span
		taskCtx, span = w.otel.StartRemoteSpan(taskCtx, "worker.ExecuteTask",
			task.TraceID, task.SpanID,
			attribute.String("task.id", task.ID),
			attribute.String("task.type", string(task.Type)),
			attribute.String("query.id", task.QueryID),
			attribute.String("worker.id", w.config.WorkerID),
		)
		defer span.End()
	}

	// Monitor for cancellation and send NATS in-progress heartbeats.
	// The heartbeat resets the AckWait timer so that long-running tasks
	// (e.g., SF100 pipeline stages that take 10+ minutes) are not
	// redelivered to other workers.
	//
	// Bounded extensions: a wedged task (worker process alive but stuck
	// in a retry loop or kernel syscall) keeps this goroutine running
	// and would indefinitely extend AckWait via msg.InProgress(). Cap
	// at maxInProgressExtensions so a wedge is bounded — after that,
	// AckWait expires naturally and JetStream redelivers the task to
	// a healthy worker. With AckWait=10m the bound is ~30m total task
	// lifetime; option-2 progress-aware InProgress will tighten further.
	const maxInProgressExtensions = 2
	go func() {
		cancelTicker := time.NewTicker(500 * time.Millisecond)
		defer cancelTicker.Stop()
		heartbeat := time.NewTicker(2 * time.Minute)
		defer heartbeat.Stop()
		extensions := 0
		for {
			select {
			case <-taskCtx.Done():
				return
			case <-cancelTicker.C:
				if w.isCancelled(task.QueryID) {
					taskCancel()
					return
				}
			case <-heartbeat.C:
				if extensions >= maxInProgressExtensions {
					w.logger.Warn("task InProgress cap reached; allowing JetStream redelivery",
						"task_id", task.ID, "query_id", task.QueryID,
						"extensions", extensions)
					return
				}
				msg.InProgress()
				extensions++
			}
		}
	}()

	// Recover from panics in task execution to prevent crashing
	// the entire worker process on schema mismatches or other bugs.
	var result distributed.ResultNotification
	func() {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				w.logger.Error("task panicked",
					"task_id", task.ID,
					"query_id", task.QueryID,
					"panic", fmt.Sprintf("%v", r),
					"stack", stack,
				)
				result = distributed.ResultNotification{
					TaskID:    task.ID,
					QueryID:   task.QueryID,
					StageID:   task.StageID,
					WorkerID:  w.config.WorkerID,
					Error:     fmt.Sprintf("task panicked: %v\n%s", r, stack),
					Duration:  0,
					Timestamp: time.Now(),
				}
			}
		}()
		result = w.executor.Execute(taskCtx, task, w.config.WorkerID)
	}()

	// Propagate trace context to result notification
	result.TraceID = task.TraceID
	result.SpanID = task.SpanID

	// Publish result notification
	subject := distributed.ResultSubject(task.QueryID, task.StageID, task.ID)
	data, err := distributed.Marshal(result)
	if err != nil {
		w.logger.Error("failed to marshal result", "error", err)
		w.publishDLQ(task, "marshal_error", fmt.Sprintf("marshal result: %v", err))
		msg.Nak()
		return
	}

	// Request/reply: coordinator ACKs receipt so we know the result arrived.
	// Retry with backoff if no response — eliminates silent result loss that
	// previously required the stale-stage watchdog.
	var publishErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
			w.logger.Warn("retrying result publish",
				"task_id", task.ID, "attempt", attempt+1)
		}
		_, publishErr = w.nc.Request(subject, data, 10*time.Second)
		if publishErr == nil {
			break
		}
	}
	if publishErr != nil {
		w.logger.Error("failed to publish result after retries",
			"error", publishErr, "task_id", task.ID)
		w.publishDLQ(task, "publish_error", fmt.Sprintf("publish result: %v", publishErr))
		msg.Nak()
		return
	}

	// Publish failed tasks to the DLQ for inspection
	if !result.Success {
		reason := "execution_error"
		if len(result.Error) >= 13 && result.Error[:13] == "task panicked" {
			reason = "panic"
		}
		w.publishDLQ(task, reason, result.Error)
	}

	msg.Ack()

	// Force GC after memory-intensive tasks to reclaim build/probe batches
	// and hash table memory before the next task allocates. Without this,
	// garbage from the previous task can push RSS past physical memory
	// before Go's GC cycle triggers (gogc=off + GoMemLimit means GC only
	// fires at the soft limit, by which time the OS may already OOM-kill).
	//
	// Native-DAG (TaskTypeStage) was missing from this match — Q18 SF10
	// 2026-04-25 confirmed num_gc=0 for the entire 72s run, heap climbed
	// monotonically to 19 GB then OS-killed the coord. Including TaskTypeStage
	// ensures the same per-task GC discipline that legacy task types get.
	shouldGC := false
	switch task.Type {
	case "join", "aggregate", "sort", "window":
		shouldGC = true
	case distributed.TaskTypeStage:
		switch task.StageType {
		case "hash_join", "broadcast_join", "aggregate", "final_aggregate", "sort", "merge_sort", "window":
			shouldGC = true
		}
	}
	if shouldGC {
		runtime.GC()
	}

	logAttrsEnd := []any{
		"task_id", task.ID,
		"success", result.Success,
		"rows", result.NumRows,
		"duration", result.Duration,
	}
	if task.StageID != "" {
		logAttrsEnd = append(logAttrsEnd, "stage_id", task.StageID)
	}
	if task.StageType != "" {
		logAttrsEnd = append(logAttrsEnd, "stage_type", task.StageType)
	}
	// PeakHeapMB is the peak Go heap during this task — sampled at 50ms
	// cadence by taskPeakHeapTracker (executor.go:200). For Q18-class
	// memory-constrained-scaling investigations, this is the per-stage
	// signal that lets us locate which task's hash table / build cache /
	// scan output is the runaway.
	if result.TaskStats != nil && result.TaskStats.PeakHeapMB > 0 {
		logAttrsEnd = append(logAttrsEnd, "peak_heap_mb", result.TaskStats.PeakHeapMB)
	}
	if task.TraceID != "" {
		logAttrsEnd = append(logAttrsEnd, "trace_id", task.TraceID)
	}
	if result.Error != "" {
		logAttrsEnd = append(logAttrsEnd, "error", result.Error)
	}
	w.logger.Info("task completed", logAttrsEnd...)
}

// publishDLQ sends a failed task to the dead-letter queue for later inspection.
func (w *Worker) publishDLQ(task distributed.Task, reason, errMsg string) {
	taskData, _ := json.Marshal(task)
	entry := distributed.DLQEntry{
		EntryID:   uuid.NewString(),
		TaskID:    task.ID,
		QueryID:   task.QueryID,
		StageID:   task.StageID,
		WorkerID:  w.config.WorkerID,
		TaskType:  task.Type,
		Error:     errMsg,
		Reason:    reason,
		TaskData:  taskData,
		Timestamp: time.Now(),
	}
	data, err := json.Marshal(entry)
	if err != nil {
		w.logger.Error("failed to marshal DLQ entry", "task_id", task.ID, "error", err)
		return
	}
	subj := distributed.DLQSubject(task.QueryID, task.ID)
	if err := w.nc.Publish(subj, data); err != nil {
		w.logger.Error("failed to publish to DLQ", "task_id", task.ID, "error", err)
	}
}

// statsCache holds the slow-to-collect process statistics that the
// heartbeat reports. A separate goroutine refreshes it on a coarser
// cadence so the hot heartbeat path never has to call runtime.ReadMemStats
// (which does a stop-the-world pause) or walk the spill directory while
// the worker is in heavy compute. Under SF10/SF100 with GOGC=off, in-line
// ReadMemStats can stall for minutes while back-to-back GCs run; coord
// then reaps the worker as stale even though it's alive and progressing.
type statsCache struct {
	mu            sync.RWMutex
	allocBytes    int64
	sysBytes      int64
	mallocs       uint64
	rss           int64
	numGoroutines int
	spillBytes    int64
}

func (s *statsCache) refresh(spillDir string) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms) // STW; only this goroutine pays the cost
	rss := distributed.ProcessRSS()
	ng := distributed.NumGoroutines()
	spill := distributed.DirDiskUsage(spillDir)
	s.mu.Lock()
	s.allocBytes = int64(ms.Alloc)
	s.sysBytes = int64(ms.Sys)
	s.mallocs = ms.Mallocs
	s.rss = rss
	s.numGoroutines = ng
	s.spillBytes = spill
	s.mu.Unlock()
}

func (s *statsCache) snapshot() (alloc, sys int64, mallocs uint64, rss int64, ng int, spill int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.allocBytes, s.sysBytes, s.mallocs, s.rss, s.numGoroutines, s.spillBytes
}

func (w *Worker) statsRefreshLoop(ctx context.Context, cache *statsCache) {
	// Initial snapshot so the first heartbeat after startup carries real
	// values rather than zeros.
	cache.refresh(w.config.SpillDir)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cache.refresh(w.config.SpillDir)
		}
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context, sem chan struct{}) {
	cache := &statsCache{}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.statsRefreshLoop(ctx, cache)
	}()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	tickCounter := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tickCounter++

			// Read cached stats — never blocks on STW or filesystem walk,
			// so the hot heartbeat path keeps firing on its 10s cadence
			// even while the stats refresher is paused inside ReadMemStats
			// or the spill walker. Coord sees Timestamp tick forward and
			// keeps the worker marked alive.
			alloc, sys, mallocs, rss, ng, spill := cache.snapshot()

			// Snapshot active task IDs for progress reporting.
			w.activeTasksMu.RLock()
			taskIDs := make([]string, 0, len(w.activeTasks))
			for id := range w.activeTasks {
				taskIDs = append(taskIDs, id)
			}
			w.activeTasksMu.RUnlock()

			poolUsed, poolBudget := w.executor.SharedPoolStats()
			hb := distributed.WorkerHeartbeat{
				WorkerID:      w.config.WorkerID,
				ClusterID:     w.config.ClusterID,
				MaxConcurrent: w.config.MaxConcurrent,
				ActiveTasks:   len(sem),
				ActiveTaskIDs: taskIDs,
				MemoryUsed:    alloc,
				MemoryTotal:   sys,
				PoolUsed:      poolUsed,
				PoolBudget:    poolBudget,
				RSS:           rss,
				NumGoroutines: ng,
				Mallocs:       mallocs,
				SpillDiskUsed: spill,
				Draining:      w.Draining(),
				Timestamp:     time.Now(),
			}

			data, err := distributed.Marshal(hb)
			if err != nil {
				continue
			}

			w.nc.Publish(distributed.SubjectHeartbeat, data)

			// Reap old cancellation entries every ~60s (6 heartbeat ticks)
			if tickCounter%6 == 0 {
				w.reapCancelled(10 * time.Minute)
			}
		}
	}
}

// reapCancelled removes cancellation entries older than maxAge.
func (w *Worker) reapCancelled(maxAge time.Duration) {
	w.cancelledMu.Lock()
	defer w.cancelledMu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	for id, cancelledAt := range w.cancelled {
		if cancelledAt.Before(cutoff) {
			delete(w.cancelled, id)
		}
	}
}

// WorkerProfile is the JSON envelope for profile data sent over NATS.
type WorkerProfile struct {
	WorkerID string `json:"worker_id"`
	CPU      []byte `json:"cpu,omitempty"`  // pprof CPU profile (gzip-compressed)
	Heap     []byte `json:"heap,omitempty"` // pprof heap profile (gzip-compressed)
}

// handleProfileStart begins CPU profiling to an in-memory buffer.
func (w *Worker) handleProfileStart(msg *nats.Msg) {
	w.profMu.Lock()
	defer w.profMu.Unlock()

	if w.profBuf != nil {
		// Already profiling — stop the old one first
		pprof.StopCPUProfile()
	}

	w.profBuf = &bytes.Buffer{}
	if err := pprof.StartCPUProfile(w.profBuf); err != nil {
		w.logger.Error("failed to start CPU profile", "error", err)
		w.profBuf = nil
		msg.Respond([]byte("error: " + err.Error()))
		return
	}
	w.logger.Info("CPU profiling started")
	msg.Respond([]byte("ok"))
}

// handleProfileCollect stops CPU profiling, collects a heap snapshot,
// and responds with both profiles as JSON.
func (w *Worker) handleProfileCollect(msg *nats.Msg) {
	w.profMu.Lock()
	defer w.profMu.Unlock()

	var cpuData []byte
	if w.profBuf != nil {
		pprof.StopCPUProfile()
		cpuData = w.profBuf.Bytes()
		w.profBuf = nil
		w.logger.Info("CPU profiling stopped", "bytes", len(cpuData))
	}

	// Heap snapshot
	runtime.GC()
	var heapBuf bytes.Buffer
	pprof.WriteHeapProfile(&heapBuf)

	resp := WorkerProfile{
		WorkerID: w.config.WorkerID,
		CPU:      cpuData,
		Heap:     heapBuf.Bytes(),
	}
	data, err := json.Marshal(resp)
	if err != nil {
		msg.Respond([]byte("error: " + err.Error()))
		return
	}
	msg.Respond(data)
}
