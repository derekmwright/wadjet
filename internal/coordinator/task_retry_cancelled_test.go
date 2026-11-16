// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"log/slog"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// The two failures of the #1687 gate-runner run, as a worker reports them: a
// refused filter compile, and a probe task stopped by a sibling's failure
// (Cancelled is set from errors.Is(err, context.Canceled) on the worker).
const (
	cancelRefusal = "fragment task r: unary op 1 (filter): compile filter \"(SELECT max(1) FROM cp_t q) is not null\": subqueries require a SubqueryRunner"
	cancelStopped = "fragment task s: unary op 0 (broadcast_probe): building hash table: join build cancelled: context canceled"
)

func refusedResult(taskID string) distributed.ResultNotification {
	return distributed.ResultNotification{TaskID: taskID, Error: cancelRefusal}
}

func stoppedResult(taskID string) distributed.ResultNotification {
	return distributed.ResultNotification{TaskID: taskID, Error: cancelStopped, Cancelled: true}
}

func noRepublish(distributed.Task) {}

// A STAGE WHOSE TASK WAS STOPPED BY ANOTHER TASK'S FAILURE REPORTS THE FAILURE,
// NOT THE STOP (#1687). The interleaving the gate runner hit once, forced: the
// stopped sibling is FIRST in dispatch order and burns its whole attempt
// budget — every attempt stopped — before the refusing task's error is
// recorded at all. The stage's error must still be the refusal. Before the
// fix FirstError walked dispatch order and returned the first terminal
// failure, which here is the stop.
func TestTaskRetrierCancelledSiblingNeverOutranksTheFailure(t *testing.T) {
	tasks := retryTestTasks(2) // "a" = the stopped sibling (first), "b" = the refusing task
	tr := newTaskRetrier(tasks, true, noRepublish, slog.Default(), "join-2", nil)

	// Attempt 1 of the refusing task fails; it is retried (uncoded, so not a
	// deterministic refusal under the 22023 rule — #1057's territory).
	if tr.Observe(refusedResult("b")) {
		t.Fatal("stage done after one retriable failure")
	}
	// The sibling is stopped on every attempt, and finishes them all while
	// the refusing task still has attempts left: no terminal failure exists
	// yet, so each stop is retried (a stop can be a worker shutting down).
	for i := 0; i < maxTaskAttempts; i++ {
		if tr.Observe(stoppedResult("a")) {
			t.Fatalf("stage done after the sibling's attempt %d", i+1)
		}
	}
	if !tr.IsTerminal("a") {
		t.Fatal("the stopped sibling did not finish its attempts first: the interleaving was not forced")
	}
	// Only now is the refusal recorded as terminal.
	tr.Observe(refusedResult("b"))
	if !tr.Observe(refusedResult("b")) {
		t.Fatal("stage not done once both tasks are terminal")
	}

	f, failed := tr.FirstError()
	if !failed {
		t.Fatal("stage reports no failure")
	}
	if f.TaskID != "b" || f.Message != cancelRefusal {
		t.Fatalf("stage error is task %s: %q\nwant task b's refusal %q — a cancellation another task's failure caused replaced it",
			f.TaskID, f.Message, cancelRefusal)
	}
}

// A task's own stopped attempt never replaces an earlier attempt's real
// failure as that task's recorded error — the shape of the gate run, whose
// reported task carried `context canceled` from its LAST attempt only.
func TestTaskRetrierCancelledAttemptKeepsTheTasksRealFailure(t *testing.T) {
	tr := newTaskRetrier(retryTestTasks(1), true, noRepublish, slog.Default(), "join-2", nil)
	tr.Observe(refusedResult("a"))
	tr.Observe(refusedResult("a"))
	if !tr.Observe(stoppedResult("a")) {
		t.Fatal("not done after the third attempt")
	}
	f, failed := tr.FirstError()
	if !failed || f.Message != cancelRefusal {
		t.Fatalf("task error = %q (failed=%v), want the refusal of its earlier attempts", f.Message, failed)
	}
}

// A STOPPED TASK IS NOT RETRIED ONCE THE STAGE HAS ALREADY FAILED (#1687): the
// stage's verdict is fixed, and another attempt only spends a worker slot.
// Before the fix the stop was retried to the attempt budget.
func TestTaskRetrierCancelledTaskNotRetriedAfterStageFailed(t *testing.T) {
	tr := newTaskRetrier(retryTestTasks(2), true, noRepublish, slog.Default(), "join-2", nil)
	for i := 0; i < maxTaskAttempts; i++ {
		tr.Observe(refusedResult("a"))
	}
	if !tr.IsTerminal("a") {
		t.Fatal("refusing task not terminal after its attempts")
	}
	if !tr.Observe(stoppedResult("b")) {
		t.Fatal("a stopped attempt after the stage failed was retried (stage not done)")
	}
	if got := tr.states["b"].attempts; got != 1 {
		t.Fatalf("stopped task ran %d attempts, want 1", got)
	}
	f, _ := tr.FirstError()
	if f.TaskID != "a" || f.Message != cancelRefusal {
		t.Fatalf("stage error is task %s: %q, want task a's refusal", f.TaskID, f.Message)
	}
}

// A stop is still retried while the stage can succeed: a worker that shuts
// down cancels its tasks, and they must run elsewhere.
func TestTaskRetrierCancelledTaskRetriedWhileStageHealthy(t *testing.T) {
	tr := newTaskRetrier(retryTestTasks(2), true, noRepublish, slog.Default(), "s", nil)
	if tr.Observe(stoppedResult("a")) {
		t.Fatal("first stop with a healthy stage was terminal")
	}
	tr.Observe(okResult("b", "f-b"))
	if !tr.Observe(okResult("a", "f-a")) {
		t.Fatal("not done after the retried task succeeded")
	}
	if _, failed := tr.FirstError(); failed {
		t.Fatal("stage failed though its stopped task succeeded on retry")
	}
}

// When EVERY terminal failure is a stop, the stage still reports one (the
// first in dispatch order): a stage never reads as succeeded.
func TestTaskRetrierAllStoppedReportsTheStop(t *testing.T) {
	tr := newTaskRetrier(retryTestTasks(2), false, noRepublish, slog.Default(), "s", nil)
	tr.Observe(stoppedResult("b"))
	if !tr.Observe(stoppedResult("a")) {
		t.Fatal("not done")
	}
	f, failed := tr.FirstError()
	if !failed || f.TaskID != "a" || f.Message != cancelStopped {
		t.Fatalf("got task %s %q (failed=%v), want task a's stop", f.TaskID, f.Message, failed)
	}
}

// Between two REAL failures the rule is unchanged: dispatch order decides,
// whatever order they were recorded in (the measurement the #1687 brief
// asks for; the same answer at the base).
func TestTaskRetrierTwoRealFailuresDispatchOrderWins(t *testing.T) {
	tr := newTaskRetrier(retryTestTasks(2), false, noRepublish, slog.Default(), "s", nil)
	tr.Observe(failResult("b", "second task's error, recorded first"))
	if !tr.Observe(failResult("a", "first task's error, recorded second")) {
		t.Fatal("not done")
	}
	f, _ := tr.FirstError()
	if f.TaskID != "a" || f.Message != "first task's error, recorded second" {
		t.Fatalf("got task %s %q, want task a (first in dispatch order)", f.TaskID, f.Message)
	}
}
