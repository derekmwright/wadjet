package worker

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/derekmwright/caelum/internal/distributed"
	"github.com/derekmwright/caelum/internal/metrics"
	"github.com/derekmwright/caelum/internal/storage/objstore"
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
		MaxConcurrent: 4,
		CacheBytes:    256 * 1024 * 1024, // 256 MB
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
	cancelled   map[string]struct{} // queryID → cancelled
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
	executor := NewExecutor(store, cache)
	executor.SetMemoryBudget(cfg.MemoryBudget, cfg.SpillDir)
	executor.SetLogger(logger)
	if cfg.ResultStoreBytes > 0 {
		executor.SetResultStore(NewResultStore(cfg.ResultStoreBytes))
	}

	return &Worker{
		config:    cfg,
		store:     store,
		nc:        nc,
		js:        js,
		executor:  executor,
		logger:    logger,
		cancelled: make(map[string]struct{}),
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

	// Create a durable consumer for tasks
	consumer, err := w.js.CreateOrUpdateConsumer(ctx, distributed.StreamTasks, jetstream.ConsumerConfig{
		Durable:       w.config.WorkerID,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       2 * time.Minute,
		MaxDeliver:    3,
	})
	if err != nil {
		return fmt.Errorf("creating consumer: %w", err)
	}

	// Subscribe to query cancellation messages
	cancelSub, err := w.nc.Subscribe(distributed.CancelSubjectAll(), func(msg *nats.Msg) {
		queryID := string(msg.Data)
		w.cancelledMu.Lock()
		w.cancelled[queryID] = struct{}{}
		w.cancelledMu.Unlock()
		w.logger.Debug("query cancelled", "query_id", queryID)
	})
	if err != nil {
		return fmt.Errorf("subscribing to cancellations: %w", err)
	}

	// Unsubscribe on shutdown
	go func() {
		<-ctx.Done()
		cancelSub.Unsubscribe()
	}()

	// Start heartbeat
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.heartbeatLoop(ctx)
	}()

	// Start task pull loop
	sem := make(chan struct{}, w.config.MaxConcurrent)
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

// Stop gracefully stops the worker.
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

		msgs, err := consumer.Fetch(available, jetstream.FetchMaxWait(5*time.Second))
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

	result := w.executor.Execute(taskCtx, task, w.config.WorkerID)

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

	w.logger.Info("task completed",
		"task_id", task.ID,
		"success", result.Success,
		"rows", result.NumRows,
		"duration", result.Duration,
	)
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			hb := distributed.WorkerHeartbeat{
				WorkerID:    w.config.WorkerID,
				ClusterID:   w.config.ClusterID,
				MemoryUsed:  int64(memStats.Alloc),
				MemoryTotal: int64(memStats.Sys),
				Timestamp:   time.Now(),
			}

			data, err := distributed.Marshal(hb)
			if err != nil {
				continue
			}

			w.nc.Publish(distributed.SubjectHeartbeat, data)
		}
	}
}
