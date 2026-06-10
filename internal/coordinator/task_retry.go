package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// maxTaskAttempts caps total executions of one task (first attempt +
// retries). Stage inputs are durable, deterministic S3 files and task
// outputs are overwrite-safe (same TaskID → same S3 key; collection uses
// the successful attempt's ResultNotification), so re-running a failed
// task is semantically safe. Mirrors Trino FTE's bounded task retry.
const maxTaskAttempts = 3

// taskRetrier tracks per-task terminal state for one stage dispatch and
// re-dispatches failed tasks up to maxTaskAttempts. It replaces the
// append-only result slice in the stage dispatchers, which also fixes a
// latent duplicate-delivery hazard: the worker's result publish retries up
// to 3x, and a duplicate notification could double-count in the old slice
// and complete the stage early with a task still outstanding.
//
// Retry policy v1: every failure is considered retriable and simply
// re-dispatched (round-robin lands it elsewhere); deterministic failures
// burn the attempt cap and then fail the stage. Two hard exclusions, both
// the caller's responsibility via the retry flag:
//   - gather-fused stages: workers stream rows to the client mid-task, so a
//     retried task would duplicate streamed output.
//   - cancelled queries: the dispatcher stops observing on ctx cancel.
type taskRetrier struct {
	mu       sync.Mutex
	states   map[string]*taskAttemptState
	tasks    map[string]distributed.Task
	order    []string // task IDs in dispatch order, for deterministic output assembly
	terminal int

	retryEnabled bool
	republish    func(distributed.Task)
	logger       *slog.Logger
	stageID      string
}

type taskAttemptState struct {
	files    []string
	partials []distributed.DynamicFilterPartialRef
	bytes    int64
	errMsg   string
	attempts int
	terminal bool
}

// newTaskRetrier registers the dispatched tasks. republish re-dispatches a
// single task; it is invoked on the caller-provided function asynchronously
// (from the NATS subscription goroutine) and must not block long.
func newTaskRetrier(tasks []distributed.Task, retryEnabled bool, republish func(distributed.Task), logger *slog.Logger, stageID string) *taskRetrier {
	tr := &taskRetrier{
		states:       make(map[string]*taskAttemptState, len(tasks)),
		tasks:        make(map[string]distributed.Task, len(tasks)),
		order:        make([]string, 0, len(tasks)),
		retryEnabled: retryEnabled && republish != nil,
		republish:    republish,
		logger:       logger,
		stageID:      stageID,
	}
	for _, t := range tasks {
		tr.states[t.ID] = &taskAttemptState{attempts: 1}
		tr.tasks[t.ID] = t
		tr.order = append(tr.order, t.ID)
	}
	return tr
}

// Observe processes one ResultNotification and returns true when every task
// has reached a terminal state (success, or failure with retries exhausted).
// Unknown task IDs and duplicates after a terminal result are ignored.
func (tr *taskRetrier) Observe(r distributed.ResultNotification) (allDone bool) {
	tr.mu.Lock()
	st, ok := tr.states[r.TaskID]
	if !ok || st.terminal {
		done := tr.terminal >= len(tr.order)
		tr.mu.Unlock()
		return done
	}
	if r.Success {
		st.files = r.ResultFiles
		st.partials = r.DynamicFilterPartials
		st.bytes = r.SizeBytes
		st.errMsg = ""
		st.terminal = true
		tr.terminal++
		done := tr.terminal >= len(tr.order)
		tr.mu.Unlock()
		return done
	}
	// Failure: retry if attempts remain, else terminal failure.
	if tr.retryEnabled && st.attempts < maxTaskAttempts {
		st.attempts++
		task := tr.growEstimateLocked(r.TaskID)
		task.Attempt = st.attempts
		tr.mu.Unlock()
		if tr.logger != nil {
			tr.logger.Warn("retrying failed task",
				"stage_id", tr.stageID, "task_id", r.TaskID,
				"attempt", task.Attempt, "max_attempts", maxTaskAttempts,
				"failed_on", r.WorkerID, "error", r.Error)
		}
		// Re-dispatch off the subscription goroutine; the scheduler's
		// publish path flushes the NATS connection.
		go tr.republish(task)
		return false
	}
	st.errMsg = r.Error
	st.terminal = true
	tr.terminal++
	done := tr.terminal >= len(tr.order)
	tr.mu.Unlock()
	if tr.logger != nil {
		tr.logger.Error("task failed terminally",
			"stage_id", tr.stageID, "task_id", r.TaskID,
			"attempts", st.attempts, "error", r.Error)
	}
	return done
}

// growEstimateLocked doubles the task's admission size estimate and returns
// the updated task (grow-on-retry, the Trino FTE pattern). A failure or a
// presumed-dead worker can't tell us whether memory was the cause, so the
// retry asks for strictly more headroom before being admitted — cheap
// insurance, bounded to 4x by maxTaskAttempts. No-op when the dispatcher
// had no estimate (0 stays 0 = unknown). Caller must hold tr.mu.
func (tr *taskRetrier) growEstimateLocked(taskID string) distributed.Task {
	task := tr.tasks[taskID]
	if task.EstimatedBytes > 0 {
		task.EstimatedBytes *= 2
		tr.tasks[taskID] = task
	}
	return task
}

// Terminal returns how many tasks have reached a terminal state.
func (tr *taskRetrier) Terminal() int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.terminal
}

// FirstError returns the first terminal failure in dispatch order, if any.
func (tr *taskRetrier) FirstError() (taskID, errMsg string, failed bool) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, id := range tr.order {
		if st := tr.states[id]; st.terminal && st.errMsg != "" {
			return id, st.errMsg, true
		}
	}
	return "", "", false
}

// Files returns per-task result files in dispatch order. Only meaningful
// after all tasks are terminal and FirstError reports none.
func (tr *taskRetrier) Files() [][]string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	out := make([][]string, len(tr.order))
	for i, id := range tr.order {
		out[i] = tr.states[id].files
	}
	return out
}

// TotalBytes sums the worker-reported output size (ResultNotification.
// SizeBytes) across all successful tasks. Feeds StageOutput.Bytes so
// downstream dispatchers can estimate their tasks' input footprints.
// 0 when workers didn't report sizes (legacy builds).
func (tr *taskRetrier) TotalBytes() int64 {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	var total int64
	for _, st := range tr.states {
		total += st.bytes
	}
	return total
}

// DynamicFilterPartials returns every successful task's dynamic-filter
// partial refs, flattened in dispatch order.
func (tr *taskRetrier) DynamicFilterPartials() []distributed.DynamicFilterPartialRef {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	var out []distributed.DynamicFilterPartialRef
	for _, id := range tr.order {
		out = append(out, tr.states[id].partials...)
	}
	return out
}

// RetryStuck re-dispatches every non-terminal task in stuck — task IDs whose
// worker has gone silent (no heartbeat ActiveTaskIDs mention and no
// TaskProgress) past the liveness threshold. Burns an attempt like a failure
// retry. Unlike Observe, exhausting the attempt cap does NOT mark the task
// terminally failed: "stuck" is a liveness inference, not a reported failure
// — the original attempt may still be alive (e.g. a long GC stall) and its
// result, arriving first, is accepted as usual. The stage idle timeout
// remains the backstop for a task stuck past its cap.
//
// Returns the re-dispatched task IDs and, separately, the stuck IDs that
// belong to this retrier but are already terminal (their liveness entries
// outlived the task; the caller should drop them from tracking).
func (tr *taskRetrier) RetryStuck(stuck []string) (redispatched, terminal []string) {
	for _, id := range stuck {
		tr.mu.Lock()
		st, ok := tr.states[id]
		if !ok {
			tr.mu.Unlock()
			continue
		}
		if st.terminal {
			tr.mu.Unlock()
			terminal = append(terminal, id)
			continue
		}
		if !tr.retryEnabled || st.attempts >= maxTaskAttempts {
			tr.mu.Unlock()
			continue
		}
		st.attempts++
		task := tr.growEstimateLocked(id)
		task.Attempt = st.attempts
		tr.mu.Unlock()
		if tr.logger != nil {
			tr.logger.Warn("re-dispatching stuck task (worker presumed dead)",
				"stage_id", tr.stageID, "task_id", id,
				"attempt", task.Attempt, "max_attempts", maxTaskAttempts)
		}
		go tr.republish(task)
		redispatched = append(redispatched, id)
	}
	return redispatched, terminal
}

// stuckTaskThreshold is how long a started task may go without any liveness
// signal (heartbeat ActiveTaskIDs mention, or a TaskProgress message) before
// the coordinator presumes its worker dead and re-dispatches it. Workers
// heartbeat every 10s and emit per-task progress every 2s, and the worker
// registry reaps workers at 90s of silence — so 2 minutes of per-task
// silence means the owning worker has already been reaped (drained), OOM-
// killed, or lost its result publish. Deliberately above the registry's
// staleTTL so worker-level reaping fires first.
//
// Only tasks a worker has REPORTED (via heartbeat or progress) are tracked:
// a task queued behind busy workers never enters liveness and is never
// falsely re-dispatched, no matter how long it queues.
const stuckTaskThreshold = 2 * time.Minute

// stuckTaskPollEvery is the watcher's poll cadence. StuckTasks is a cheap
// read-locked map scan; stages that finish faster than one tick pay nothing.
const stuckTaskPollEvery = 15 * time.Second

// watchStuckTasks starts a goroutine that polls per-task liveness and
// re-dispatches tasks whose worker has gone silent (the FTE answer to
// worker death mid-stage: stage inputs are durable S3 files, so a dead
// worker's tasks just run again elsewhere instead of the stage idling out
// after stageIdleTimeout). Returns a stop func the dispatcher must defer.
func (c *Coordinator) watchStuckTasks(ctx context.Context, retrier *taskRetrier) (stop func()) {
	if c.workers == nil || c.workers.Liveness == nil {
		return func() {}
	}
	liveness := c.workers.Liveness
	watchCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(stuckTaskPollEvery)
		defer ticker.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
				reapStuckOnce(liveness, retrier, stuckTaskThreshold)
			}
		}
	}()
	return cancel
}

// reapStuckOnce runs one stuck-task sweep for watchStuckTasks (split out
// for testing). Re-dispatches this retrier's stuck non-terminal tasks and
// drops liveness entries that are either re-dispatched (resets the clock —
// the new attempt's heartbeats re-register them; without this the next tick
// would burn another attempt immediately) or terminal (the entry outlived
// the result, e.g. an in-flight heartbeat re-added it after the result
// handler's Remove). Other stages' task IDs are left untouched.
func reapStuckOnce(liveness *TaskLiveness, retrier *taskRetrier, threshold time.Duration) (redispatched int) {
	stuck := liveness.StuckTasks(threshold)
	if len(stuck) == 0 {
		return 0
	}
	re, term := retrier.RetryStuck(stuck)
	for _, id := range re {
		liveness.Remove(id)
	}
	for _, id := range term {
		liveness.Remove(id)
	}
	return len(re)
}
