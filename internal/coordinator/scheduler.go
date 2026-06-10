package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/dataplane"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/nats-io/nats.go"
)

// schedulerInflightTTL bounds how long a dispatched task's EstimatedBytes
// counts against its worker in pickLeastLoaded. The estimate covers the
// dispatch→pool-visible blind window: once the task is admitted its real
// allocations show up in the worker's heartbeat PoolUsed (10s cadence), so
// holding the estimate past that double-counts (conservative, never
// unsafe). Result arrival clears the entry early via TaskDone; the TTL is
// the backstop for lost results.
const schedulerInflightTTL = 60 * time.Second

// inflightTask is one dispatched-not-yet-done task's admission estimate,
// charged against a worker in pickLeastLoaded.
type inflightTask struct {
	workerID string
	bytes    int64
	expires  time.Time
}

// Scheduler publishes tasks to NATS and tracks stage DAGs.
type Scheduler struct {
	nc     *nats.Conn
	logger *slog.Logger

	// dpSrv is the optional data-plane gRPC server. When set, PublishTasks
	// pushes each task over a per-worker gRPC stream instead of NATS.
	// Placement: bin-packed by estimated memory fit when the task carries
	// an EstimatedBytes and heartbeat pool stats exist (see pickWorkerFor),
	// round-robin otherwise (Server.PickWorker). NATS is not used as a
	// fallback in this mode — gRPC dispatch is either all or nothing per
	// the Phase C design.
	dpSrv *dataplane.Server

	// registry provides heartbeat pool stats (PoolUsed/PoolBudget) for
	// bin-packed placement. nil = always round-robin.
	registry *WorkerRegistry

	inflightMu sync.Mutex
	inflight   map[string]inflightTask // task ID → estimate charged to a worker
}

// NewScheduler creates a new task scheduler.
func NewScheduler(nc *nats.Conn, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{nc: nc, logger: logger, inflight: make(map[string]inflightTask)}
}

// SetDataPlaneServer enables gRPC task dispatch. When set, PublishTasks
// pushes through the data-plane server instead of NATS publish.
func (s *Scheduler) SetDataPlaneServer(srv *dataplane.Server) {
	s.dpSrv = srv
}

// SetWorkerRegistry enables memory-aware placement for gRPC dispatch:
// tasks carrying an EstimatedBytes go to the worker with the most free
// pool (heartbeat budget − heartbeat used − in-flight estimates) instead
// of round-robin.
func (s *Scheduler) SetWorkerRegistry(wr *WorkerRegistry) {
	s.registry = wr
}

// TaskDone clears the task's in-flight admission estimate. Called by the
// stage result subscriptions on every ResultNotification; the TTL covers
// lost results.
func (s *Scheduler) TaskDone(taskID string) {
	s.inflightMu.Lock()
	delete(s.inflight, taskID)
	s.inflightMu.Unlock()
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

// publishViaDataPlane pushes each task to a selected worker. Tasks
// carrying an EstimatedBytes are bin-packed onto the worker with the most
// free pool; tasks without an estimate round-robin (see pickWorkerFor).
// Send blocks on HTTP/2 flow control when a worker is saturated, applying
// backpressure to the scheduler. The full set is rejected if no worker
// is connected — there is no NATS fallback in gRPC mode.
func (s *Scheduler) publishViaDataPlane(batch []preparedTask) error {
	for _, p := range batch {
		workerID, ok := s.pickWorkerFor(p.task)
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
						s.noteInflight(p.task, workerID2)
						continue
					}
				}
			}
			return fmt.Errorf("dispatching task %s to %s: %w", p.task.ID, workerID, err)
		}
		s.noteInflight(p.task, workerID)
	}
	s.logger.Info("published tasks", "count", len(batch), "transport", "grpc")
	return nil
}

// pickWorkerFor selects a worker for one task. Memory-aware bin-packing
// applies only when the task carries an estimate AND heartbeat pool stats
// exist — otherwise placement is the pre-existing round-robin. Estimates
// of 0 deliberately round-robin: with no per-task charge, "most free"
// would be static between heartbeats and pile every task of a fan-out
// onto one worker.
func (s *Scheduler) pickWorkerFor(t distributed.Task) (string, bool) {
	if t.EstimatedBytes > 0 && s.registry != nil {
		if id, ok := s.pickLeastLoaded(); ok {
			return id, true
		}
	}
	return s.dpSrv.PickWorker()
}

// pickLeastLoaded returns the connected worker with the most free shared
// pool: heartbeat budget − heartbeat used − in-flight dispatch estimates.
// Workers without pool stats (legacy builds, pre-first-heartbeat) are
// skipped; returns ok=false when none qualify so the caller round-robins.
func (s *Scheduler) pickLeastLoaded() (string, bool) {
	return pickLeastLoadedWorker(s.dpSrv.ConnectedWorkers(), s.registry.ActiveWorkers(), s.inflightByWorker())
}

// pickLeastLoadedWorker is pickLeastLoaded's pure scoring core: max over
// connected workers of (PoolBudget − PoolUsed − in-flight estimate),
// skipping workers with no pool stats. Free space may legitimately go
// negative under burst dispatch — the least-overcommitted worker still
// wins (every placement choice exists; refusing here would just push the
// decision to round-robin, which picks blind).
func pickLeastLoadedWorker(connected []string, active []*WorkerInfo, inflight map[string]int64) (string, bool) {
	infos := make(map[string]*WorkerInfo, len(active))
	for _, w := range active {
		infos[w.WorkerID] = w
	}
	var bestID string
	var bestFree int64
	found := false
	for _, id := range connected {
		info, ok := infos[id]
		if !ok || info.PoolBudget <= 0 {
			continue
		}
		free := info.PoolBudget - info.PoolUsed - inflight[id]
		if !found || free > bestFree {
			bestID, bestFree, found = id, free, true
		}
	}
	return bestID, found
}

// inflightByWorker sums live in-flight estimates per worker, lazily
// expiring stale entries (no background goroutine).
func (s *Scheduler) inflightByWorker() map[string]int64 {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	now := time.Now()
	perWorker := make(map[string]int64)
	for id, e := range s.inflight {
		if now.After(e.expires) {
			delete(s.inflight, id)
			continue
		}
		perWorker[e.workerID] += e.bytes
	}
	return perWorker
}

// noteInflight charges a dispatched task's estimate to its worker until
// the result arrives (TaskDone) or the TTL hands accounting off to the
// worker's heartbeat PoolUsed.
func (s *Scheduler) noteInflight(t distributed.Task, workerID string) {
	if t.EstimatedBytes <= 0 {
		return
	}
	s.inflightMu.Lock()
	s.inflight[t.ID] = inflightTask{
		workerID: workerID,
		bytes:    t.EstimatedBytes,
		expires:  time.Now().Add(schedulerInflightTTL),
	}
	s.inflightMu.Unlock()
}
