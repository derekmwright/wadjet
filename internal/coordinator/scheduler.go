package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/citc-tech/wadjet/internal/dataplane"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/nats-io/nats.go"
)

// Scheduler publishes tasks to NATS and tracks stage DAGs.
type Scheduler struct {
	nc     *nats.Conn
	logger *slog.Logger

	// dpSrv is the optional data-plane gRPC server. When set, PublishTasks
	// pushes each task over a per-worker gRPC stream instead of NATS,
	// using round-robin worker selection (see Server.PickWorker). NATS is
	// not used as a fallback in this mode — gRPC dispatch is either all
	// or nothing per the Phase C design.
	dpSrv *dataplane.Server
}

// NewScheduler creates a new task scheduler.
func NewScheduler(nc *nats.Conn, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{nc: nc, logger: logger}
}

// SetDataPlaneServer enables gRPC task dispatch. When set, PublishTasks
// pushes through the data-plane server instead of NATS publish.
func (s *Scheduler) SetDataPlaneServer(srv *dataplane.Server) {
	s.dpSrv = srv
}

// preparedTask is the per-task scratch built by PublishTasks before
// dispatch. Subject is used by the NATS path; data is the
// `distributed.Marshal(Task)` blob both transports carry.
type preparedTask struct {
	subject string
	data    []byte
	task    distributed.Task
}

// PublishTasks publishes a set of tasks for worker consumption. Routes
// via gRPC TaskDispatch (data-plane server) when configured, otherwise
// falls back to NATS JetStream publish. Tasks are serialized in batch
// before publishing to minimize time spent holding the transport.
func (s *Scheduler) PublishTasks(_ context.Context, tasks []distributed.Task) error {
	if len(tasks) == 0 {
		return nil
	}

	batch := make([]preparedTask, 0, len(tasks))
	for _, task := range tasks {
		data, err := distributed.Marshal(task)
		if err != nil {
			return fmt.Errorf("marshaling task %s: %w", task.ID, err)
		}

		// Subject is only used by the NATS path; cheap to compute upfront.
		subject := distributed.TaskSubject(string(task.Type), task.QueryID, task.StageID)
		if task.ClusterID != "" {
			subject = distributed.ClusterTaskSubject(task.ClusterID, string(task.Type), task.QueryID, task.StageID)
		}

		batch = append(batch, preparedTask{
			subject: subject,
			data:    data,
			task:    task,
		})
	}

	if s.dpSrv != nil {
		return s.publishViaDataPlane(batch)
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

	s.logger.Info("published tasks", "count", len(tasks), "transport", "nats")
	return nil
}

// publishViaDataPlane pushes each task to a worker selected by
// round-robin over the data-plane server's connected set. Send blocks
// on HTTP/2 flow control when a worker is saturated, applying
// backpressure to the scheduler. The full set is rejected if no worker
// is connected — there is no NATS fallback in gRPC mode.
func (s *Scheduler) publishViaDataPlane(batch []preparedTask) error {
	for _, p := range batch {
		workerID, ok := s.dpSrv.PickWorker()
		if !ok {
			return fmt.Errorf("dispatching task %s: %w", p.task.ID, dataplane.ErrNoWorkers)
		}
		if err := s.dpSrv.SendTaskDispatch(workerID, p.task.ID, p.task.QueryID, p.task.StageID, p.data, 0); err != nil {
			if errors.Is(err, dataplane.ErrNotConnected) {
				// Worker dropped between Pick and Send; one retry on the
				// next round-robin position keeps the path lossless when
				// the cluster has more than one worker.
				workerID2, ok2 := s.dpSrv.PickWorker()
				if ok2 && workerID2 != workerID {
					if err2 := s.dpSrv.SendTaskDispatch(workerID2, p.task.ID, p.task.QueryID, p.task.StageID, p.data, 0); err2 == nil {
						continue
					}
				}
			}
			return fmt.Errorf("dispatching task %s to %s: %w", p.task.ID, workerID, err)
		}
	}
	s.logger.Info("published tasks", "count", len(batch), "transport", "grpc")
	return nil
}
