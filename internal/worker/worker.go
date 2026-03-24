package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/metrics"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
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
	MemoryBudget     int64  // per-task memory budget in bytes (0 = unlimited, no spill)
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

	cancel context.CancelFunc
	wg     sync.WaitGroup

	cancelledMu sync.RWMutex
	cancelled   map[string]time.Time // queryID → cancellation time

	// CPU profiling state — started/stopped via NATS profile requests.
	profMu  sync.Mutex
	profBuf *bytes.Buffer // nil when not profiling
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
	executor.SetMemoryBudget(cfg.MemoryBudget, cfg.SpillDir)
	executor.SetLogger(logger)
	if cfg.ResultStoreBytes > 0 {
		executor.SetResultStore(NewResultStore(cfg.ResultStoreBytes))
	}

	// NATS KV result cache: enables cross-worker inter-stage result transfer
	// without S3 round-trips. Workers write small results here instead of S3.
	if js != nil {
		kv, kvErr := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{
			Bucket:   "wadjet_results_data",
			TTL:      5 * time.Minute,
			MaxBytes: 1024 * 1024 * 1024, // 1 GB total
			Storage:  jetstream.MemoryStorage,
		})
		if kvErr == nil {
			executor.SetResultKV(kv)
			logger.Info("NATS KV result cache enabled", "bucket", "wadjet_results_data")
		} else {
			logger.Debug("NATS KV result cache unavailable, using S3 only", "error", kvErr)
		}

		// NATS Object Store: handles large results (> 4 MB) that exceed KV limits.
		// Chunking is transparent — avoids S3 round-trip for scan-split results.
		obs, obsErr := js.CreateOrUpdateObjectStore(context.Background(), jetstream.ObjectStoreConfig{
			Bucket:      "wadjet_results_obj",
			TTL:         5 * time.Minute,
			MaxBytes:    2 * 1024 * 1024 * 1024, // 2 GB total
			Storage:     jetstream.MemoryStorage,
			Compression: true,
		})
		if obsErr == nil {
			executor.SetResultObjStore(obs)
			logger.Info("NATS Object Store result cache enabled", "bucket", "wadjet_results_obj")
		} else {
			logger.Debug("NATS Object Store unavailable, large results will use S3", "error", obsErr)
		}
	}

	return &Worker{
		config:    cfg,
		store:     store,
		nc:        nc,
		js:        js,
		executor:  executor,
		logger:    logger,
		cancelled: make(map[string]time.Time),
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
		AckWait:       5 * time.Minute,
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
		w.logger.Debug("query cancelled", "query_id", queryID)
	})
	if err != nil {
		return fmt.Errorf("subscribing to cancellations: %w", err)
	}

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

// Stop gracefully stops the worker. The shared per-cluster consumer is left in
// place so other workers can continue pulling tasks.
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()

	w.logger.Info("worker stopped", "worker_id", w.config.WorkerID)
}

func (w *Worker) taskLoop(ctx context.Context, consumer jetstream.Consumer, sem chan struct{}) {
	// Batch fetch: pull up to available concurrency slots at once to
	// amortize the NATS round-trip overhead.
	for {
		if ctx.Err() != nil {
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

	// Skip tasks for cancelled queries
	if w.isCancelled(task.QueryID) {
		w.logger.Debug("skipping task for cancelled query",
			"task_id", task.ID, "query_id", task.QueryID)
		msg.Term()
		return
	}

	w.logger.Info("executing task",
		"task_id", task.ID,
		"type", task.Type,
		"query_id", task.QueryID,
	)

	// Create a cancellable context for this task
	taskCtx, taskCancel := context.WithCancel(ctx)
	defer taskCancel()

	// Monitor for cancellation during execution
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-taskCtx.Done():
				return
			case <-ticker.C:
				if w.isCancelled(task.QueryID) {
					taskCancel()
					return
				}
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

	// Publish result notification
	subject := distributed.ResultSubject(task.QueryID, task.StageID, task.ID)
	data, err := distributed.Marshal(result)
	if err != nil {
		w.logger.Error("failed to marshal result", "error", err)
		msg.Nak()
		return
	}

	if err := w.nc.Publish(subject, data); err != nil {
		w.logger.Error("failed to publish result", "error", err)
		msg.Nak()
		return
	}

	msg.Ack()

	// Force GC after memory-intensive tasks to reclaim build/probe batches
	// and hash table memory before the next task allocates. Without this,
	// garbage from the previous task can push RSS past physical memory
	// before Go's GC cycle triggers.
	switch task.Type {
	case "join", "aggregate", "sort", "window":
		runtime.GC()
	}

	logAttrs := []any{
		"task_id", task.ID,
		"success", result.Success,
		"rows", result.NumRows,
		"duration", result.Duration,
	}
	if result.Error != "" {
		logAttrs = append(logAttrs, "error", result.Error)
	}
	w.logger.Info("task completed", logAttrs...)
}

func (w *Worker) heartbeatLoop(ctx context.Context, sem chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	tickCounter := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tickCounter++

			hb := distributed.WorkerHeartbeat{
				WorkerID:    w.config.WorkerID,
				ClusterID:   w.config.ClusterID,
				ActiveTasks: len(sem),
				Timestamp:   time.Now(),
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
