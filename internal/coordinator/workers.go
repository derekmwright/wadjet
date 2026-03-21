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
	WorkerID    string
	ClusterID   string
	MemoryUsed  int64
	MemoryTotal int64
	LastSeen    time.Time
}

// WorkerRegistry tracks active workers from heartbeats.
type WorkerRegistry struct {
	mu      sync.RWMutex
	workers map[string]*WorkerInfo
	stale   time.Duration // workers not heard from in this long are considered dead
	logger  *slog.Logger
	sub     *nats.Subscription
}

// NewWorkerRegistry creates a worker registry that subscribes to heartbeats.
func NewWorkerRegistry(nc *nats.Conn, logger *slog.Logger) *WorkerRegistry {
	if logger == nil {
		logger = slog.Default()
	}

	wr := &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
		stale:   1 * time.Hour,
		logger:  logger,
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
	info.MemoryUsed = hb.MemoryUsed
	info.MemoryTotal = hb.MemoryTotal
	// Use coordinator-side time for LastSeen. The heartbeat message
	// arriving proves the worker is alive — even if the worker's
	// goroutine was GC-frozen and its embedded timestamp is stale.
	info.LastSeen = time.Now()
}

// ActiveWorkers returns workers that have sent a heartbeat recently.
func (wr *WorkerRegistry) ActiveWorkers() []*WorkerInfo {
	wr.mu.RLock()
	defer wr.mu.RUnlock()

	cutoff := time.Now().Add(-wr.stale)
	var active []*WorkerInfo
	for _, w := range wr.workers {
		if w.LastSeen.After(cutoff) {
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

// ReapStale removes workers that haven't sent a heartbeat recently.
func (wr *WorkerRegistry) ReapStale() int {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	cutoff := time.Now().Add(-wr.stale)
	reaped := 0
	for id, w := range wr.workers {
		if w.LastSeen.Before(cutoff) {
			delete(wr.workers, id)
			wr.logger.Info("worker reaped (stale)", "worker_id", id)
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
