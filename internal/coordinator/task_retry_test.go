package coordinator

import (
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/worker"
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
	tr := newTaskRetrier(retryTestTasks(3), true, rep.republish, slog.Default(), "s", nil)
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
	tr := newTaskRetrier(retryTestTasks(2), true, rep.republish, slog.Default(), "s", nil)
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
	tr := newTaskRetrier(retryTestTasks(1), true, rep.republish, slog.Default(), "s", nil)
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
	tr := newTaskRetrier(retryTestTasks(1), false, rep.republish, slog.Default(), "s", nil)
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
	tr := newTaskRetrier(retryTestTasks(2), true, rep.republish, slog.Default(), "s", nil)
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
	tr := newTaskRetrier(retryTestTasks(1), true, nil, slog.Default(), "s", nil)
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
	tr := newTaskRetrier(retryTestTasks(1), true, rep.republish, slog.Default(), "s", nil)
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

// TestTaskRetrier_DynamicFilterPartials: partial refs from the successful
// attempt's notification are captured per task and flattened in dispatch
// order; duplicates must not double-collect.
func TestTaskRetrier_DynamicFilterPartials(t *testing.T) {
	tr := newTaskRetrier(retryTestTasks(2), true, nil, slog.Default(), "s", nil)
	rA := okResult("a", "f-a")
	rA.DynamicFilterPartials = []distributed.DynamicFilterPartialRef{
		{FilterID: "df1", Bucket: "b", Key: "a-df1"},
	}
	rB := okResult("b", "f-b")
	rB.DynamicFilterPartials = []distributed.DynamicFilterPartialRef{
		{FilterID: "df1", Bucket: "b", Key: "b-df1"},
		{FilterID: "df2", Bucket: "b", Key: "b-df2"},
	}
	tr.Observe(rB) // arrival order reversed vs dispatch order
	tr.Observe(rA)
	tr.Observe(rB) // duplicate must not double-collect
	got := tr.DynamicFilterPartials()
	if len(got) != 3 {
		t.Fatalf("partials = %d, want 3", len(got))
	}
	if got[0].Key != "a-df1" || got[1].Key != "b-df1" || got[2].Key != "b-df2" {
		t.Fatalf("partials out of dispatch order: %+v", got)
	}
}

// TestTaskRetrier_RetryStuck_Redispatches: a stuck non-terminal task burns an
// attempt and is republished; unknown IDs are ignored; terminal IDs are
// reported back for liveness cleanup.
func TestTaskRetrier_RetryStuck_Redispatches(t *testing.T) {
	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(2), true, rep.republish, slog.Default(), "s", nil)
	tr.Observe(okResult("b", "f-b")) // b terminal

	re, term := tr.RetryStuck([]string{"a", "b", "zzz"})
	if len(re) != 1 || re[0] != "a" {
		t.Fatalf("redispatched = %v, want [a]", re)
	}
	if len(term) != 1 || term[0] != "b" {
		t.Fatalf("terminal = %v, want [b]", term)
	}
	got := waitRepublished(t, rep, 1)
	if got[0].ID != "a" || got[0].Attempt != 2 {
		t.Fatalf("republished %+v, want task a attempt 2", got[0])
	}
	// The original (presumed-dead) attempt's late success still wins.
	if !tr.Observe(okResult("a", "f-a1")) {
		t.Fatal("stage must complete on the surviving attempt's result")
	}
	if _, _, failed := tr.FirstError(); failed {
		t.Fatal("unexpected terminal failure")
	}
}

// TestTaskRetrier_RetryStuck_CapDoesNotFail: a task stuck past the attempt
// cap is NOT marked terminally failed — stuck is an inference, not a
// reported failure; the stage idle timeout is the backstop.
func TestTaskRetrier_RetryStuck_CapDoesNotFail(t *testing.T) {
	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(1), true, rep.republish, slog.Default(), "s", nil)
	re, _ := tr.RetryStuck([]string{"a"}) // attempt 2
	if len(re) != 1 {
		t.Fatalf("first stuck sweep redispatched %v, want [a]", re)
	}
	re, _ = tr.RetryStuck([]string{"a"}) // attempt 3 (cap)
	if len(re) != 1 {
		t.Fatalf("second stuck sweep redispatched %v, want [a]", re)
	}
	re, term := tr.RetryStuck([]string{"a"}) // capped: no-op
	if len(re) != 0 || len(term) != 0 {
		t.Fatalf("capped sweep = (%v,%v), want empty", re, term)
	}
	if tr.Terminal() != 0 {
		t.Fatal("stuck-past-cap must not mark the task terminal")
	}
	if _, _, failed := tr.FirstError(); failed {
		t.Fatal("stuck-past-cap must not record a terminal failure")
	}
	// A surviving attempt's success still completes the stage.
	if !tr.Observe(okResult("a", "f-a")) {
		t.Fatal("success must complete the stage")
	}
}

// TestTaskRetrier_RetryStuck_RetryDisabled: gather-fused stages must never
// re-dispatch, even for stuck tasks (a retry would duplicate streamed rows).
func TestTaskRetrier_RetryStuck_RetryDisabled(t *testing.T) {
	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(1), false, rep.republish, slog.Default(), "s", nil)
	re, _ := tr.RetryStuck([]string{"a"})
	if len(re) != 0 {
		t.Fatalf("redispatched %v with retry disabled", re)
	}
	if len(rep.snapshot()) != 0 {
		t.Fatal("must not republish with retry disabled")
	}
}

// TestReapStuckOnce: one watcher sweep re-dispatches this retrier's stuck
// tasks and clears their liveness clocks, drops terminal strays, and leaves
// other stages' entries alone.
func TestReapStuckOnce(t *testing.T) {
	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(2), true, rep.republish, slog.Default(), "s", nil)
	tr.Observe(okResult("b", "f-b")) // b terminal

	liveness := NewTaskLiveness()
	stale := time.Now().Add(-5 * time.Minute)
	liveness.Update([]string{"a", "b", "other-stage-task"}, stale)

	if n := reapStuckOnce(liveness, tr, 2*time.Minute); n != 1 {
		t.Fatalf("redispatched = %d, want 1", n)
	}
	waitRepublished(t, rep, 1)

	stuck := liveness.StuckTasks(2 * time.Minute)
	if len(stuck) != 1 || stuck[0] != "other-stage-task" {
		t.Fatalf("post-sweep stuck = %v, want [other-stage-task] only", stuck)
	}

	// A fresh entry (the re-dispatched attempt heartbeating) must not be
	// swept again.
	liveness.Update([]string{"a"}, time.Now())
	if n := reapStuckOnce(liveness, tr, 2*time.Minute); n != 0 {
		t.Fatalf("fresh task redispatched %d times, want 0", n)
	}
}

// TestReapStuckOnce_DeadWorkerUnreportedTask: regression for the
// 2026-08-11 Q09/Q21-R2 coordinator wedge. A task gRPC-dispatched to a
// worker that dies before ever mentioning it in a heartbeat has no
// liveness clock — the stuck sweep can't see it, no result will ever
// arrive, and the stage waits until the query timeout. ExpireWorker
// (called by ReapStale when the worker's heartbeats stop) must force the
// dead worker's assigned tasks into the stuck set so the next sweep
// re-dispatches them.
func TestReapStuckOnce_DeadWorkerUnreportedTask(t *testing.T) {
	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(2), true, rep.republish, slog.Default(), "s", nil)

	liveness := NewTaskLiveness()
	// Task a: dispatched to w-dead, never reported (died in the worker's
	// intake before any heartbeat mentioned it). Task b: dispatched to
	// w-live and heartbeating normally.
	liveness.Assign("a", "w-dead")
	liveness.Assign("b", "w-live")
	liveness.Update([]string{"b"}, time.Now())

	// Without the death signal the sweep must see nothing: an assigned-
	// but-unreported task must NOT be treated as stuck (it may simply be
	// queued behind a busy worker's slots).
	if n := reapStuckOnce(liveness, tr, 2*time.Minute); n != 0 {
		t.Fatalf("pre-death sweep redispatched %d, want 0", n)
	}

	// Worker death: the registry reap path expires its assignments.
	expired := liveness.ExpireWorker("w-dead")
	if len(expired) != 1 || expired[0] != "a" {
		t.Fatalf("expired = %v, want [a]", expired)
	}

	// The very next sweep re-dispatches the invisible task.
	if n := reapStuckOnce(liveness, tr, 2*time.Minute); n != 1 {
		t.Fatalf("post-death sweep redispatched %d, want 1", n)
	}
	got := waitRepublished(t, rep, 1)
	if got[0].ID != "a" || got[0].Attempt != 2 {
		t.Fatalf("republished %+v, want task a attempt 2", got[0])
	}
	// The live worker's task was untouched by the expiry.
	if stuck := liveness.StuckTasks(2 * time.Minute); len(stuck) != 0 {
		t.Fatalf("post-sweep stuck = %v, want empty", stuck)
	}

	// The re-dispatched attempt's result completes the stage.
	tr.Observe(okResult("b", "f-b"))
	if !tr.Observe(okResult("a", "f-a")) {
		t.Fatal("stage must complete once the re-dispatched task reports")
	}
}

// TestTaskRetrier_PartitionAccounting: element-wise reduction across the
// final surviving attempts, retry-safe (a failed attempt's vectors never
// count — only the surviving attempt's do).
func TestTaskRetrier_PartitionAccounting(t *testing.T) {
	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(2), true, rep.republish, slog.Default(), "s", nil)

	ra := okResult("a", "f-a")
	ra.PartitionRows = []int64{10, 0, 5}
	ra.PartitionBytes = []int64{100, 0, 50}
	tr.Observe(ra)

	// Task b fails once (no vectors recorded), then its retry succeeds with
	// vectors — the reduction must reflect only the surviving attempt.
	tr.Observe(failResult("b", "worker died"))
	waitRepublished(t, rep, 1)
	rb := okResult("b", "f-b2")
	rb.PartitionRows = []int64{1, 2, 3}
	rb.PartitionBytes = []int64{10, 20, 30}
	if !tr.Observe(rb) {
		t.Fatal("not done after retry success")
	}

	rows, bytes := tr.PartitionAccounting(3)
	wantRows := []int64{11, 2, 8}
	wantBytes := []int64{110, 20, 80}
	for p := range wantRows {
		if rows[p] != wantRows[p] {
			t.Errorf("rows[%d] = %d, want %d", p, rows[p], wantRows[p])
		}
		if bytes[p] != wantBytes[p] {
			t.Errorf("bytes[%d] = %d, want %d", p, bytes[p], wantBytes[p])
		}
	}
}

// TestTaskRetrier_PartitionAccountingUnreported: nil vectors when no task
// reported (legacy workers) — callers treat nil as skew-detection-off.
func TestTaskRetrier_PartitionAccountingUnreported(t *testing.T) {
	tr := newTaskRetrier(retryTestTasks(2), false, nil, slog.Default(), "s", nil)
	tr.Observe(okResult("a", "f-a"))
	tr.Observe(okResult("b", "f-b"))
	rows, bytes := tr.PartitionAccounting(4)
	if rows != nil || bytes != nil {
		t.Fatalf("want nil vectors for unreported tasks, got rows=%v bytes=%v", rows, bytes)
	}
}

// TestTaskRetrier_StaleInputAttemptRetries pins the Phase C1 slice-4
// classification contract (docs/design/eager-consumer-dispatch.md §5): a
// consumer task that poisons itself on a superseded producer attempt
// (worker.StaleInputAttemptMarker in the failure text, NO MissingInputKey)
// is RETRIABLE — it takes the standard re-dispatch path, unlike the
// inputLostMarker family, which is terminal via the fatal classifier. By
// the retry the producer attempt set is stable, so attempt 2 succeeds.
func TestTaskRetrier_StaleInputAttemptRetries(t *testing.T) {
	// The coordinator's real fatal classifier: peerFiles set (streaming
	// exchange on) but the stale-attempt failure carries no
	// MissingInputKey, so it must never classify fatal.
	c := newEagerTestCoordinator(true)
	staleErr := worker.StaleInputAttemptMarker + ": producer task p1 attempt 2 superseded consumed attempt 1"
	stale := distributed.ResultNotification{TaskID: "a", Success: false, Error: staleErr}
	if c.classifyFatalResult(stale) {
		t.Fatal("stale-attempt failure must not classify fatal")
	}
	if IsInputLostErr(errors.New(staleErr)) {
		t.Fatal("stale-attempt marker must not match the input-lost family (that path disables streaming exchange for a full rerun)")
	}

	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(1), true, rep.republish, slog.Default(), "s", c.classifyFatalResult)
	if tr.Observe(stale) {
		t.Fatal("stale-attempt failure with retries left must not be terminal")
	}
	got := waitRepublished(t, rep, 1)
	if got[0].Attempt != 2 {
		t.Fatalf("republished attempt = %d, want 2", got[0].Attempt)
	}
	if !tr.Observe(okResult("a", "f-a2")) {
		t.Fatal("not done after retry success")
	}
	if _, _, failed := tr.FirstError(); failed {
		t.Fatal("stale-attempt retry must recover cleanly")
	}
}
