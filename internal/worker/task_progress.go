package worker

import (
	"sync/atomic"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/nats-io/nats.go"
)

// TaskProgress is the worker-side counter operators bump from their
// hot loop to signal forward progress. The counter is read by the
// per-task heartbeat goroutine to (a) decide whether to extend
// JetStream AckWait and (b) publish TaskProgress messages to coord.
//
// Operators reach this via exec.ProgressReporterFromContext (the
// engine layer doesn't depend on worker types). The exec interface
// has AddRows / AddBytes; this struct satisfies it.
type TaskProgress struct {
	rows           atomic.Int64
	bytes          atomic.Int64
	lastUpdateNano atomic.Int64
}

// AddRows reports that n more rows have been processed by this task.
// Updates the lastUpdate timestamp; the heartbeat goroutine reads
// these values without needing to lock.
func (p *TaskProgress) AddRows(n int64) {
	if p == nil || n <= 0 {
		return
	}
	p.rows.Add(n)
	p.lastUpdateNano.Store(time.Now().UnixNano())
}

// AddBytes reports that n more bytes have been processed by this task.
// Used for IO-bound stages (scan, shuffle write) where bytes are a
// truer measure of forward progress than row count.
func (p *TaskProgress) AddBytes(n int64) {
	if p == nil || n <= 0 {
		return
	}
	p.bytes.Add(n)
	p.lastUpdateNano.Store(time.Now().UnixNano())
}

// snapshot returns the current row, byte, and last-update timestamp.
// Cheap atomic loads only — safe to call from any goroutine.
func (p *TaskProgress) snapshot() (rows, bytes int64, lastUpdate time.Time) {
	rows = p.rows.Load()
	bytes = p.bytes.Load()
	if ns := p.lastUpdateNano.Load(); ns > 0 {
		lastUpdate = time.Unix(0, ns)
	}
	return
}

// publishTaskProgress sends a TaskProgress NATS message to the coord
// for the given task. Uses Core NATS (fire-and-forget) since
// progress messages are advisory — a missed one doesn't break
// correctness. Failures are silently dropped.
func publishTaskProgress(nc *nats.Conn, msg distributed.TaskProgress) {
	if nc == nil {
		return
	}
	data, err := distributed.Marshal(msg)
	if err != nil {
		return
	}
	_ = nc.Publish(distributed.TaskProgressSubject(msg.QueryID, msg.TaskID), data)
}
