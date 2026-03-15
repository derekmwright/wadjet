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
func (s *Scheduler) PublishTasks(_ context.Context, tasks []distributed.Task) error {
	for _, task := range tasks {
		subject := distributed.TaskSubject(string(task.Type), task.QueryID, task.StageID)

		data, err := distributed.Marshal(task)
		if err != nil {
			return fmt.Errorf("marshaling task %s: %w", task.ID, err)
		}

		if err := s.nc.Publish(subject, data); err != nil {
			return fmt.Errorf("publishing task %s to %s: %w", task.ID, subject, err)
		}

		s.logger.Debug("published task",
			"task_id", task.ID,
			"type", task.Type,
			"query_id", task.QueryID,
			"stage_id", task.StageID,
			"subject", subject,
		)
	}

	if err := s.nc.Flush(); err != nil {
		return fmt.Errorf("flushing NATS: %w", err)
	}

	s.logger.Info("published tasks",
		"count", len(tasks),
	)

	return nil
}
