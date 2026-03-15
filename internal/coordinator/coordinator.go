package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/derekmwright/caelum/internal/distributed"
	"github.com/derekmwright/caelum/internal/storage/catalog"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Config holds coordinator configuration.
type Config struct {
	NATSUrl      string
	ResultBucket string
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
	resultSubs map[string]context.CancelFunc // queryID -> cancel
}

// New creates a new Coordinator.
func New(cfg Config, cat *catalog.Catalog, nc *nats.Conn, js jetstream.JetStream, logger *slog.Logger) *Coordinator {
	if logger == nil {
		logger = slog.Default()
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
