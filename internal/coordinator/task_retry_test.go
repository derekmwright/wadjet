package coordinator

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

func retryTestTasks(n int) []distributed.Task {
	tasks := make([]distributed.Task, n)
	for i := range tasks {
		tasks[i] = distributed.Task{ID: string(rune('a' + i)), QueryID: "q", StageID: "s", Attempt: 1}
	}
	return tasks
}

func okResult(taskID string, files ...string) distributed.ResultNotification {
	return distributed.ResultNotification{TaskID: taskID, Success: true, ResultFiles: files}
}

func failResult(taskID, msg string) distributed.ResultNotification {
	return distributed.ResultNotification{TaskID: taskID, Success: false, Error: msg}
}

// collectingRepublisher records re-dispatched tasks thread-safely.
type collectingRepublisher struct {
	mu    sync.Mutex
	tasks []distributed.Task
}

func (c *collectingRepublisher) republish(t distributed.Task) {
	c.mu.Lock()
	c.tasks = append(c.tasks, t)
	c.mu.Unlock()
}

func (c *collectingRepublisher) snapshot() []distributed.Task {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]distributed.Task, len(c.tasks))
	copy(out, c.tasks)
	return out
}

// waitRepublished polls until the republisher has seen n tasks (republish is
// invoked on a goroutine from Observe).
func waitRepublished(t *testing.T, c *collectingRepublisher, n int) []distributed.Task {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := c.snapshot()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("republished %d tasks, want %d", len(got), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestTaskRetrier_AllSucceedFirstTry(t *testing.T) {
	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(3), true, rep.republish, slog.Default(), "s")
	if tr.Observe(okResult("a", "f-a")) {
		t.Fatal("done after 1/3")
	}
	if tr.Observe(okResult("b", "f-b")) {
		t.Fatal("done after 2/3")
	}
	if !tr.Observe(okResult("c", "f-c")) {
		t.Fatal("not done after 3/3")
	}
	if _, _, failed := tr.FirstError(); failed {
		t.Fatal("unexpected failure")
	}
	files := tr.Files()
	want := []string{"f-a", "f-b", "f-c"}
	for i, w := range want {
		if len(files[i]) != 1 || files[i][0] != w {
			t.Fatalf("files[%d] = %v, want [%s] (dispatch order)", i, files[i], w)
		}
	}
	if len(rep.snapshot()) != 0 {
		t.Fatal("nothing should have been republished")
	}
}

func TestTaskRetrier_FailThenRetrySucceeds(t *testing.T) {
	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(2), true, rep.republish, slog.Default(), "s")
	if tr.Observe(failResult("a", "worker died")) {
		t.Fatal("failure with retries left must not be terminal")
	}
	got := waitRepublished(t, rep, 1)
	if got[0].ID != "a" || got[0].Attempt != 2 {
		t.Fatalf("republished %+v, want task a attempt 2", got[0])
	}
	if tr.Observe(okResult("b", "f-b")) {
		t.Fatal("done with task a still outstanding")
	}
	// Retry succeeds.
	if !tr.Observe(okResult("a", "f-a2")) {
		t.Fatal("not done after retry success")
	}
	if _, _, failed := tr.FirstError(); failed {
		t.Fatal("unexpected terminal failure")
	}
	if files := tr.Files(); files[0][0] != "f-a2" {
		t.Fatalf("files[0] = %v, want retry attempt's files", files[0])
	}
}

func TestTaskRetrier_ExhaustsAttempts(t *testing.T) {
	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(1), true, rep.republish, slog.Default(), "s")
	// Attempt 1 fails → retry (attempt 2). Attempt 2 fails → retry (attempt 3).
	// Attempt 3 fails → terminal.
	if tr.Observe(failResult("a", "boom-1")) {
		t.Fatal("terminal after first failure")
	}
	waitRepublished(t, rep, 1)
	if tr.Observe(failResult("a", "boom-2")) {
		t.Fatal("terminal after second failure")
	}
	got := waitRepublished(t, rep, 2)
	if got[1].Attempt != 3 {
		t.Fatalf("second republish attempt = %d, want 3", got[1].Attempt)
	}
	if !tr.Observe(failResult("a", "boom-3")) {
		t.Fatal("not terminal after attempts exhausted")
	}
	taskID, errMsg, failed := tr.FirstError()
	if !failed || taskID != "a" || errMsg != "boom-3" {
		t.Fatalf("FirstError = (%s,%s,%v), want (a,boom-3,true)", taskID, errMsg, failed)
	}
	if len(rep.snapshot()) != 2 {
		t.Fatalf("republished %d times, want exactly 2", len(rep.snapshot()))
	}
}

func TestTaskRetrier_RetryDisabled(t *testing.T) {
	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(1), false, rep.republish, slog.Default(), "s")
	if !tr.Observe(failResult("a", "boom")) {
		t.Fatal("with retry disabled, first failure must be terminal")
	}
	if _, _, failed := tr.FirstError(); !failed {
		t.Fatal("expected terminal failure")
	}
	if len(rep.snapshot()) != 0 {
		t.Fatal("must not republish with retry disabled")
	}
}

// TestTaskRetrier_DuplicateDeliveryIgnored locks the dedup fix: the worker's
// result publish retries up to 3x, so duplicate notifications happen. The old
// append-only slice double-counted them and could complete a stage early with
// a task still outstanding.
func TestTaskRetrier_DuplicateDeliveryIgnored(t *testing.T) {
	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(2), true, rep.republish, slog.Default(), "s")
	if tr.Observe(okResult("a", "f-a")) {
		t.Fatal("done after 1/2")
	}
	// Duplicate delivery of a's result must NOT complete the stage.
	if tr.Observe(okResult("a", "f-a")) {
		t.Fatal("duplicate delivery completed the stage with b outstanding")
	}
	if tr.Terminal() != 1 {
		t.Fatalf("terminal = %d after dup, want 1", tr.Terminal())
	}
	if !tr.Observe(okResult("b", "f-b")) {
		t.Fatal("not done after both tasks")
	}
}

func TestTaskRetrier_UnknownTaskIgnored(t *testing.T) {
	tr := newTaskRetrier(retryTestTasks(1), true, nil, slog.Default(), "s")
	if tr.Observe(okResult("zzz", "f")) {
		t.Fatal("unknown task must not complete the stage")
	}
	if tr.Terminal() != 0 {
		t.Fatal("unknown task must not count")
	}
}

// TestTaskRetrier_LateDuplicateAfterRetry: attempt 1's failure notification
// arrives, retry is dispatched, then a DUPLICATE of attempt 1's failure
// arrives before the retry completes. The duplicate burns one more attempt
// (acceptable: bounded by the cap) but must never doubly-complete or panic;
// the eventual success must still win.
func TestTaskRetrier_LateDuplicateAfterRetry(t *testing.T) {
	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(1), true, rep.republish, slog.Default(), "s")
	if tr.Observe(failResult("a", "boom")) {
		t.Fatal("terminal too early")
	}
	if tr.Observe(failResult("a", "boom")) { // duplicate of the same failure
		t.Fatal("terminal too early on dup")
	}
	if !tr.Observe(okResult("a", "f-final")) {
		t.Fatal("success must complete the stage")
	}
	if _, _, failed := tr.FirstError(); failed {
		t.Fatal("success must clear failure state")
	}
	if files := tr.Files(); files[0][0] != "f-final" {
		t.Fatalf("files = %v, want f-final", files[0])
	}
}
