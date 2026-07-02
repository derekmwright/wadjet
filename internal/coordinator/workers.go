package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/dataplane"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/nats-io/nats.go"
)

// Coord-side NATS subscription pending limits. NATS defaults are 65536
// messages / 64 MB per sub; under SF10 fanout (24 task slots × multiple
// publishes per task) those limits get hit during bursts and the server
// silently drops messages. Bumping by ~16× msgs / 4× bytes gives bursts
// room to queue while the sub callback drains. Long-term fix is to move
// NATS off coord so its goroutines don't share GC + GOMAXPROCS with
// coord planning/dispatch — but that's a bigger change. See
// project_multi_signal_liveness_ec2_validation_2026-05-03.md.
const (
	coordSubMsgLimit  = 1_048_576       // 1M messages (default 65k)
	coordSubByteLimit = 256 * 1024 * 1024 // 256 MB (default 64 MB)
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
	PeerAddr      string // dialable peer-exchange address; "" = worker serves no peer fetches
	LastSeen      time.Time
}

// TaskLiveness tracks when each in-flight task was last reported active.
// Updated from worker heartbeats AND from per-task TaskProgress messages —
// either signal is proof the worker process is alive (see WorkerRegistry's
// progress subscription for why we need both). Used to detect stuck tasks.
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
	mu          sync.RWMutex
	workers     map[string]*WorkerInfo
	stale       time.Duration // workers not heard from in this long are considered dead
	logger      *slog.Logger
	nc          *nats.Conn
	sub         *nats.Subscription
	progressSub *nats.Subscription // per-task progress, used as a heartbeat-equivalent liveness signal
	Liveness    *TaskLiveness      // per-task progress tracking from heartbeats AND TaskProgress messages
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
	// Bump pending limits past NATS defaults (65536 msgs / 64 MB). 2026-05-03
	// EC2 SF10 saw 5 workers reaped within 7 seconds across 3 hosts during
	// Q11 — a cluster-wide silence consistent with NATS server-side
	// slow-consumer drops on the heartbeat sub. Headroom here lets bursts
	// queue instead of dropping silently. Sub-stats instrumentation
	// (StartSubStatsLogger) reports actual pending+drop counts; if drops
	// stay zero with these limits, the slow-consumer hypothesis is valid
	// and a deeper fix (NATS off coord) is the architectural next step.
	if err := sub.SetPendingLimits(coordSubMsgLimit, coordSubByteLimit); err != nil {
		logger.Warn("failed to bump heartbeat sub pending limits", "error", err)
	}
	wr.sub = sub

	// Per-task progress is published from a separate worker goroutine on a
	// 2s cadence (see worker.go's per-task heartbeat goroutine). Treating
	// it as a heartbeat-equivalent liveness signal decouples worker
	// liveness from the global heartbeat goroutine being scheduled. In the
	// 2026-04-29 SF10 Q03 reproduction, workers were GC-thrashing under
	// lineitem broadcast-join build pressure; the global heartbeat
	// goroutine starved past 90s, the workers were reaped, JetStream
	// redelivered tasks to other workers that thrashed identically, and
	// the cluster hot-potato'd until query timeout. With this signal,
	// even one stalled goroutine in a multi-goroutine process keeps the
	// worker alive in coord's view.
	progressSub, err := nc.Subscribe(distributed.SubjectTaskProgressAll, func(msg *nats.Msg) {
		var tp distributed.TaskProgress
		if err := distributed.Unmarshal(msg.Data, &tp); err != nil {
			return
		}
		wr.recordTaskProgress(tp)
	})
	if err != nil {
		logger.Error("failed to subscribe to task progress", "error", err)
	} else {
		if err := progressSub.SetPendingLimits(coordSubMsgLimit, coordSubByteLimit); err != nil {
			logger.Warn("failed to bump task-progress sub pending limits", "error", err)
		}
		wr.progressSub = progressSub
	}

	return wr
}

// SetDataPlaneServer installs the registry's global TaskProgress
// handler on the data-plane server. When workers send TaskProgress
// over gRPC (Phase E), this handler treats each arrival as proof of
// life for the emitting worker — same liveness semantics as the NATS
// progressSub above. Idempotent; a later SetDataPlaneServer(nil)
// installs a no-op handler. Must be called after NewWorkerRegistry.
func (wr *WorkerRegistry) SetDataPlaneServer(srv *dataplane.Server) {
	if wr == nil || srv == nil {
		return
	}
	srv.SetGlobalTaskProgressHandler(func(tp *dataplane.TaskProgress) {
		wr.recordTaskProgress(distributed.TaskProgress{
			QueryID:        tp.QueryID,
			StageID:        tp.StageID,
			TaskID:         tp.TaskID,
			WorkerID:       tp.WorkerID,
			RowsProcessed:  tp.RowsProcessed,
			BytesProcessed: tp.BytesProcessed,
			Timestamp:      time.Unix(0, tp.TimestampUnixNano),
		})
	})
}

// recordTaskProgress treats a per-task progress message as proof of life
// for the emitting worker. Only updates an EXISTING WorkerInfo's LastSeen
// — registration still requires a real heartbeat with cluster ID and pool
// stats. If the worker hasn't registered yet, the progress message is
// recorded for the task only.
func (wr *WorkerRegistry) recordTaskProgress(tp distributed.TaskProgress) {
	now := time.Now()
	wr.markWorkerSeen(tp.WorkerID, now)
	if wr.Liveness != nil {
		wr.Liveness.Update([]string{tp.TaskID}, now)
	}
}

// MarkWorkerSeen records that the given worker is alive as of `now`.
// Multi-signal liveness: any worker→coord NATS message (TaskProgress,
// ResultNotification, GatherBatchMsg) is proof the worker process is
// running and able to publish, even if its heartbeat goroutine is
// starved or its heartbeat NATS publish is lagging. Coord still requires
// a real heartbeat for initial registration (cluster ID, pool stats),
// but post-registration any of these signals counts toward LastSeen.
//
// No-op when workerID is empty (some workers don't stamp messages until
// the worker ID is plumbed end-to-end).
func (wr *WorkerRegistry) MarkWorkerSeen(workerID string) {
	if workerID == "" {
		return
	}
	wr.markWorkerSeen(workerID, time.Now())
}

func (wr *WorkerRegistry) markWorkerSeen(workerID string, now time.Time) {
	if workerID == "" {
		return
	}
	wr.mu.Lock()
	if info, ok := wr.workers[workerID]; ok {
		info.LastSeen = now
	}
	wr.mu.Unlock()
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
	info.PeerAddr = hb.PeerAddr
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

// PeerAddr returns the peer-exchange address of a live worker, or "" when
// the worker is unknown, stale, or doesn't serve peer fetches. Draining
// workers still serve fetches for files they already hold, so unlike
// ActiveWorkers this only requires freshness.
func (wr *WorkerRegistry) PeerAddr(workerID string) string {
	wr.mu.RLock()
	defer wr.mu.RUnlock()
	w, ok := wr.workers[workerID]
	if !ok || !w.LastSeen.After(time.Now().Add(-wr.stale)) {
		return ""
	}
	return w.PeerAddr
}

// Count returns the number of active workers.
func (wr *WorkerRegistry) Count() int {
	return len(wr.ActiveWorkers())
}

// broadcastThresholdFromCluster derives a build-bytes threshold for the
// planner's broadcast-vs-shuffle decision from the smallest worker's shared
// pool budget. The rule: a build can be broadcast only if it fits
// comfortably within ~30% of the smallest worker's pool, capped at an
// absolute 200 MB so a very large worker doesn't open the door to
// catastrophic broadcast (broadcast still pays N× across the cluster).
//
// minBudget == 0 means we have no info from any worker — return 0 so the
// planner falls back to its absolute default (100 MB).
func broadcastThresholdFromCluster(minBudget int64) int64 {
	if minBudget <= 0 {
		return 0 // planner uses default
	}
	t := minBudget * 30 / 100
	const cap = int64(200 * 1024 * 1024)
	if t > cap {
		t = cap
	}
	return t
}

// MinWorkerPoolBudget returns the smallest non-zero shared-pool budget
// reported by any active worker. Used by the planner to bound broadcast-vs-
// shuffle decisions: a build that wouldn't fit in the smallest worker's
// memory pool must NOT be broadcast (every worker would pay the full
// duplication). Returns 0 when no worker has reported pool stats yet
// (legacy workers, or pre-heartbeat phase) — callers should treat 0 as
// "no information; use absolute defaults."
func (wr *WorkerRegistry) MinWorkerPoolBudget() int64 {
	wr.mu.RLock()
	defer wr.mu.RUnlock()
	cutoff := time.Now().Add(-wr.stale)
	var min int64
	for _, w := range wr.workers {
		if !w.LastSeen.After(cutoff) || w.Draining {
			continue
		}
		if w.PoolBudget <= 0 {
			continue
		}
		if min == 0 || w.PoolBudget < min {
			min = w.PoolBudget
		}
	}
	return min
}

// ClusterPoolPressure returns the cluster-wide shared memory pool pressure
// as a value in [0.0, 1.0], computed from the most recent heartbeat each
// active worker reported. Returns 0 when no worker has reported pool stats
// (legacy workers, or worker without --shared-pool-budget). Observability
// only — earlier per-stage dispatch gating on this value caused a deadlock
// in broadcast_join chains (the probe-side stage that would free build-side
// memory was itself gated on memory dropping). Worker-side pool-pressure
// spill is the load-shedding primitive now.
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

// StartSubStatsLogger periodically logs PendingMsgs/PendingBytes/Dropped/
// Delivered for the long-lived subscriptions (heartbeat + TaskProgress).
// 2026-05-03 SF10 mass-reap was consistent with NATS server-side slow-
// consumer drops on these subs but we had no instrumentation to confirm.
// With this running, drops surface in the coord log as they happen.
//
// Cadence: 10s — same as worker heartbeat cadence so a stat-tick reliably
// sits between consecutive heartbeats. Stats are O(1) to read; the only
// cost is one log line per tick.
func (wr *WorkerRegistry) StartSubStatsLogger(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		var prevHBDropped, prevHBDeliv int64
		var prevTPDropped, prevTPDeliv int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if wr.sub != nil {
					pMsgs, pBytes, _ := wr.sub.Pending()
					dropped, _ := wr.sub.Dropped()
					deliv, _ := wr.sub.Delivered()
					maxMsgs, maxBytes, _ := wr.sub.PendingLimits()
					dDropped := int64(dropped) - prevHBDropped
					dDeliv := deliv - prevHBDeliv
					prevHBDropped = int64(dropped)
					prevHBDeliv = deliv
					level := slog.LevelInfo
					if dDropped > 0 {
						level = slog.LevelWarn
					}
					wr.logger.Log(ctx, level, "nats sub stats",
						"sub", "heartbeat",
						"pending_msgs", pMsgs,
						"pending_bytes", pBytes,
						"max_msgs", maxMsgs,
						"max_bytes", maxBytes,
						"delta_delivered", dDeliv,
						"delta_dropped", dDropped,
						"total_dropped", dropped)
				}
				if wr.progressSub != nil {
					pMsgs, pBytes, _ := wr.progressSub.Pending()
					dropped, _ := wr.progressSub.Dropped()
					deliv, _ := wr.progressSub.Delivered()
					maxMsgs, maxBytes, _ := wr.progressSub.PendingLimits()
					dDropped := int64(dropped) - prevTPDropped
					dDeliv := deliv - prevTPDeliv
					prevTPDropped = int64(dropped)
					prevTPDeliv = deliv
					level := slog.LevelInfo
					if dDropped > 0 {
						level = slog.LevelWarn
					}
					wr.logger.Log(ctx, level, "nats sub stats",
						"sub", "task_progress",
						"pending_msgs", pMsgs,
						"pending_bytes", pBytes,
						"max_msgs", maxMsgs,
						"max_bytes", maxBytes,
						"delta_delivered", dDeliv,
						"delta_dropped", dDropped,
						"total_dropped", dropped)
				}
			}
		}
	}()
}

// Close unsubscribes from heartbeats and task progress.
func (wr *WorkerRegistry) Close() {
	if wr.sub != nil {
		wr.sub.Unsubscribe()
	}
	if wr.progressSub != nil {
		wr.progressSub.Unsubscribe()
	}
}
