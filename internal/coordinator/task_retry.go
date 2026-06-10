package coordinator

import (
	"log/slog"
	"sync"

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
		task := tr.tasks[r.TaskID]
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
