package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/nats-io/nats.go"
)

// WorkerInfo tracks the state of a registered worker.
type WorkerInfo struct {
	WorkerID      string
	ClusterID     string
	MaxConcurrent int   // effective task-slot count reported in heartbeat; 0 = legacy worker (assume default)
	MemoryUsed    int64
	MemoryTotal   int64
	Draining      bool
	LastSeen      time.Time
}

// TaskLiveness tracks when each in-flight task was last reported active.
// Updated from worker heartbeats. Used to detect stuck tasks.
type TaskLiveness struct {
	mu    sync.RWMutex
	tasks map[string]time.Time // task ID → last heartbeat time
}

func NewTaskLiveness() *TaskLiveness {
	return &TaskLiveness{tasks: make(map[string]time.Time)}
}

// Update records that the given task IDs are actively running.
func (tl *TaskLiveness) Update(taskIDs []string, now time.Time) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	for _, id := range taskIDs {
		tl.tasks[id] = now
	}
}

// Remove stops tracking the given task (completed or failed).
func (tl *TaskLiveness) Remove(taskID string) {
	tl.mu.Lock()
	delete(tl.tasks, taskID)
	tl.mu.Unlock()
}

// StuckTasks returns task IDs that haven't been reported in a heartbeat
// for longer than the given threshold.
func (tl *TaskLiveness) StuckTasks(threshold time.Duration) []string {
	tl.mu.RLock()
	defer tl.mu.RUnlock()
	cutoff := time.Now().Add(-threshold)
	var stuck []string
	for id, lastSeen := range tl.tasks {
		if lastSeen.Before(cutoff) {
			stuck = append(stuck, id)
		}
	}
	return stuck
}

// WorkerRegistry tracks active workers from heartbeats.
type WorkerRegistry struct {
	mu       sync.RWMutex
	workers  map[string]*WorkerInfo
	stale    time.Duration // workers not heard from in this long are considered dead
	logger   *slog.Logger
	nc       *nats.Conn
	sub      *nats.Subscription
	Liveness *TaskLiveness // per-task progress tracking from heartbeats
}

// NewWorkerRegistry creates a worker registry that subscribes to heartbeats.
// staleTTL controls how long since the last heartbeat before a worker is
// considered dead. Pass 0 to use the default (30s).
func NewWorkerRegistry(nc *nats.Conn, logger *slog.Logger, staleTTL time.Duration) *WorkerRegistry {
	if logger == nil {
		logger = slog.Default()
	}
	if staleTTL <= 0 {
		// Default: 10 minutes. Workers under heavy compute load with
		// GOGC=off may not schedule their heartbeat goroutine for
		// 30+ seconds. At SF100, build-cache pre-scans and large hash
		// table builds can stall heartbeats for minutes. The previous
		// 5-minute TTL caused stale reaping during build cache scans
		// of ~8GB tables from S3. 10 minutes provides enough buffer
		// while still detecting truly dead workers within a reasonable
		// window.
		staleTTL = 10 * time.Minute
	}

	wr := &WorkerRegistry{
		workers:  make(map[string]*WorkerInfo),
		stale:    staleTTL,
		logger:   logger,
		nc:       nc,
		Liveness: NewTaskLiveness(),
	}

	sub, err := nc.Subscribe(distributed.SubjectHeartbeat, func(msg *nats.Msg) {
		var hb distributed.WorkerHeartbeat
		if err := distributed.Unmarshal(msg.Data, &hb); err != nil {
			return
		}
		wr.record(hb)
	})
	if err != nil {
		logger.Error("failed to subscribe to heartbeats", "error", err)
		return wr
	}
	wr.sub = sub

	return wr
}

func (wr *WorkerRegistry) record(hb distributed.WorkerHeartbeat) {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	info, exists := wr.workers[hb.WorkerID]
	if !exists {
		wr.logger.Info("worker registered", "worker_id", hb.WorkerID, "cluster_id", hb.ClusterID)
		info = &WorkerInfo{WorkerID: hb.WorkerID}
		wr.workers[hb.WorkerID] = info
	}
	info.ClusterID = hb.ClusterID
	info.MaxConcurrent = hb.MaxConcurrent
	info.MemoryUsed = hb.MemoryUsed
	info.MemoryTotal = hb.MemoryTotal
	info.Draining = hb.Draining
	// Use coordinator-side time for LastSeen. The heartbeat message
	// arriving proves the worker is alive — even if the worker's
	// goroutine was GC-frozen and its embedded timestamp is stale.
	now := time.Now()
	info.LastSeen = now

	// Update per-task liveness from heartbeat
	if len(hb.ActiveTaskIDs) > 0 && wr.Liveness != nil {
		wr.Liveness.Update(hb.ActiveTaskIDs, now)
	}
}

// ActiveWorkers returns workers that have sent a heartbeat recently and are
// not draining. Draining workers are finishing in-flight tasks and will not
// accept new work.
func (wr *WorkerRegistry) ActiveWorkers() []*WorkerInfo {
	wr.mu.RLock()
	defer wr.mu.RUnlock()

	cutoff := time.Now().Add(-wr.stale)
	var active []*WorkerInfo
	for _, w := range wr.workers {
		if w.LastSeen.After(cutoff) && !w.Draining {
			copy := *w
			active = append(active, &copy)
		}
	}
	return active
}

// Count returns the number of active workers.
func (wr *WorkerRegistry) Count() int {
	return len(wr.ActiveWorkers())
}

// ClusterCapacity returns the sum of effective task-slot counts across
// active (non-draining, non-stale) workers, taken from the most recent
// heartbeat each worker reported. Workers running pre-MaxConcurrent-in-
// heartbeat builds report 0; those are skipped here so the result reflects
// only workers we have honest capacity data for. Returns 0 when no worker
// has reported MaxConcurrent yet — callers should fall back to a
// conservative static cap (e.g. 2 * Count).
func (wr *WorkerRegistry) ClusterCapacity() int {
	wr.mu.RLock()
	defer wr.mu.RUnlock()
	cutoff := time.Now().Add(-wr.stale)
	total := 0
	for _, w := range wr.workers {
		if !w.LastSeen.After(cutoff) || w.Draining {
			continue
		}
		if w.MaxConcurrent <= 0 {
			continue
		}
		total += w.MaxConcurrent
	}
	return total
}

// ReapStale removes workers that haven't sent a heartbeat recently.
// Reaped workers receive a drain message so they stop pulling new tasks
// from the JetStream consumer. If the worker is truly dead, the drain
// is a no-op; if it's alive but GC-stalled, it will stop accepting work.
func (wr *WorkerRegistry) ReapStale() int {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	cutoff := time.Now().Add(-wr.stale)
	reaped := 0
	for id, w := range wr.workers {
		if w.LastSeen.Before(cutoff) {
			delete(wr.workers, id)
			wr.logger.Info("worker reaped (stale)", "worker_id", id)
			// Tell the worker to stop pulling tasks. Use request/reply
			// with a short timeout — if the worker is truly dead, the
			// NATS connection is gone and this times out harmlessly.
			if wr.nc != nil {
				drainSubject := "wadjet.worker." + id + ".drain"
				if _, err := wr.nc.Request(drainSubject, nil, 5*time.Second); err != nil {
					wr.logger.Debug("drain request failed (worker likely dead)",
						"worker_id", id, "error", err)
				} else {
					wr.logger.Info("worker acknowledged drain", "worker_id", id)
				}
			}
			reaped++
		}
	}
	return reaped
}

// StartReaper starts a background goroutine that periodically removes stale workers.
func (wr *WorkerRegistry) StartReaper(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				wr.ReapStale()
			}
		}
	}()
}

// Close unsubscribes from heartbeats.
func (wr *WorkerRegistry) Close() {
	if wr.sub != nil {
		wr.sub.Unsubscribe()
	}
}
