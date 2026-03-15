package worker

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/derekmwright/caelum/internal/distributed"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Config holds worker configuration.
type Config struct {
	NATSUrl       string
	WorkerID      string
	MaxConcurrent int    // max concurrent tasks
	CacheBytes    int64  // local LRU cache size
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

	cancel context.CancelFunc
	wg     sync.WaitGroup
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
	return &Worker{
		config:   cfg,
		store:    store,
		nc:       nc,
		js:       js,
		executor: NewExecutor(store, cache),
		logger:   logger,
	}
}

// Start begins the worker task loop and heartbeat.
func (w *Worker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)

	// Create a durable consumer for tasks
	consumer, err := w.js.CreateOrUpdateConsumer(ctx, distributed.StreamTasks, jetstream.ConsumerConfig{
		Durable:       w.config.WorkerID,
		FilterSubject: distributed.SubjectTasksAll,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       2 * time.Minute,
		MaxDeliver:    3,
	})
	if err != nil {
		return fmt.Errorf("creating consumer: %w", err)
	}

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
		"max_concurrent", w.config.MaxConcurrent,
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
	for {
		select {
		case <-ctx.Done():
			return
		case sem <- struct{}{}:
			// Have capacity, try to fetch a message
		}

		msgs, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			<-sem
			if ctx.Err() != nil {
				return
			}
			continue
		}

		gotMsg := false
		for msg := range msgs.Messages() {
			gotMsg = true
			w.wg.Add(1)
			go func(m jetstream.Msg) {
				defer w.wg.Done()
				defer func() { <-sem }()
				w.handleTask(ctx, m)
			}(msg)
		}

		if !gotMsg {
			<-sem
		}

		if msgs.Error() != nil && ctx.Err() == nil {
			w.logger.Debug("fetch returned", "error", msgs.Error())
		}
	}
}

func (w *Worker) handleTask(ctx context.Context, msg jetstream.Msg) {
	var task distributed.Task
	if err := distributed.Unmarshal(msg.Data(), &task); err != nil {
		w.logger.Error("failed to unmarshal task", "error", err)
		msg.Term()
		return
	}

	w.logger.Info("executing task",
		"task_id", task.ID,
		"type", task.Type,
		"query_id", task.QueryID,
	)

	result := w.executor.Execute(ctx, task, w.config.WorkerID)

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
