package coordinator

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/derekmwright/caelum/internal/distributed"
	"github.com/nats-io/nats.go"
)

// Scheduler publishes tasks to NATS and tracks stage DAGs.
type Scheduler struct {
	nc     *nats.Conn
	logger *slog.Logger
}

// NewScheduler creates a new task scheduler.
func NewScheduler(nc *nats.Conn, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{nc: nc, logger: logger}
}

// PublishTasks publishes a set of tasks to NATS for worker consumption.
// Tasks are serialized in batch before publishing to minimize time spent
// holding the NATS connection and reduce per-message overhead.
func (s *Scheduler) PublishTasks(_ context.Context, tasks []distributed.Task) error {
	if len(tasks) == 0 {
		return nil
	}

	// Pre-serialize all tasks to avoid interleaving marshal + publish
	type prepared struct {
		subject string
		data    []byte
		task    distributed.Task
	}
	batch := make([]prepared, 0, len(tasks))
	for _, task := range tasks {
		data, err := distributed.Marshal(task)
		if err != nil {
			return fmt.Errorf("marshaling task %s: %w", task.ID, err)
		}

		// Use cluster-scoped subject when ClusterID is set
		subject := distributed.TaskSubject(string(task.Type), task.QueryID, task.StageID)
		if task.ClusterID != "" {
			subject = distributed.ClusterTaskSubject(task.ClusterID, string(task.Type), task.QueryID, task.StageID)
		}

		batch = append(batch, prepared{
			subject: subject,
			data:    data,
			task:    task,
		})
	}

	// Publish all pre-serialized messages
	for _, p := range batch {
		if err := s.nc.Publish(p.subject, p.data); err != nil {
			return fmt.Errorf("publishing task %s to %s: %w", p.task.ID, p.subject, err)
		}
	}

	if err := s.nc.Flush(); err != nil {
		return fmt.Errorf("flushing NATS: %w", err)
	}

	s.logger.Info("published tasks", "count", len(tasks))
	return nil
}
