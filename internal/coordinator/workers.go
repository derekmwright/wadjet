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
	PoolUsed      int64 // shared memory pool bytes Reserved
	PoolBudget    int64 // shared memory pool capacity in bytes; pressure = PoolUsed / PoolBudget
	ActiveTaskIDs []string // task IDs in flight per the most recent heartbeat
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
// considered dead. Pass 0 to use the default (90s).
func NewWorkerRegistry(nc *nats.Conn, logger *slog.Logger, staleTTL time.Duration) *WorkerRegistry {
	if logger == nil {
		logger = slog.Default()
	}
	if staleTTL <= 0 {
		// Default: 90 seconds. Workers send heartbeats every 10s (PR #58
		// decoupled the hot path from runtime.ReadMemStats so GC-stalls
		// no longer block heartbeat emission). 90s = 9 missed heartbeats,
		// enough margin for a brief NATS hiccup but tight enough to
		// detect a wedged worker quickly.
		//
		// Was 30 minutes pre-PR #58 (workers under load could stall the
		// heartbeat goroutine for many minutes during ReadMemStats STW).
		// With heartbeat decoupled, we no longer need that headroom —
		// and a 30m TTL meant the SF10 deploy let a wedged worker hold
		// 8 tasks for the entire 30m bench-runner cap before reaping
		// (observed 92833f7).
		staleTTL = 90 * time.Second
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
	info.PoolUsed = hb.PoolUsed
	info.PoolBudget = hb.PoolBudget
	info.ActiveTaskIDs = append(info.ActiveTaskIDs[:0], hb.ActiveTaskIDs...)
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

// ClusterPoolPressure returns the cluster-wide shared memory pool pressure
// as a value in [0.0, 1.0], computed from the most recent heartbeat each
// active worker reported. Returns 0 when no worker has reported pool stats
// (legacy workers, or worker without --shared-pool-budget).
//
// The dispatcher uses this to throttle new task admission when the cluster
// is near memory exhaustion — refusing to dispatch lets in-flight operators
// spill and free pool memory before piling on more concurrent work.
func (wr *WorkerRegistry) ClusterPoolPressure() float64 {
	wr.mu.RLock()
	defer wr.mu.RUnlock()
	cutoff := time.Now().Add(-wr.stale)
	var totalUsed, totalBudget int64
	for _, w := range wr.workers {
		if !w.LastSeen.After(cutoff) || w.Draining {
			continue
		}
		if w.PoolBudget <= 0 {
			continue
		}
		totalUsed += w.PoolUsed
		totalBudget += w.PoolBudget
	}
	if totalBudget == 0 {
		return 0
	}
	p := float64(totalUsed) / float64(totalBudget)
	if p < 0 {
		p = 0
	}
	if p > 1 {
		p = 1
	}
	return p
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
			// Report which tasks the dead worker was holding. Surfaces
			// "8 tasks orphaned because worker wedged" in the coord log
			// instead of just "worker reaped." Operators reading the log
			// can correlate stuck stages with reaped workers, and the
			// JetStream-level redelivery (post worker-side InProgress
			// cap, see worker.go) eventually picks these up on a
			// healthy worker.
			lastSeenAgo := time.Since(w.LastSeen).Round(time.Second)
			if len(w.ActiveTaskIDs) > 0 {
				wr.logger.Warn("worker reaped (stale) with in-flight tasks",
					"worker_id", id,
					"last_seen_ago", lastSeenAgo,
					"in_flight_tasks", len(w.ActiveTaskIDs),
					"task_ids", w.ActiveTaskIDs)
			} else {
				wr.logger.Info("worker reaped (stale)",
					"worker_id", id, "last_seen_ago", lastSeenAgo)
			}
			delete(wr.workers, id)
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
