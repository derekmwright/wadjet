package coordinator

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A worker-side panic reaches the coordinator as a bare STRING in a
// ResultNotification: no error chain, no type, no SQLSTATE. The marker left
// in the message is the only thing that still identifies it, and two
// decisions key off it. These tests pin that contract end to end, because the
// two halves live in different packages and a reworded message would break
// both silently.

func panicText(t *testing.T) string {
	t.Helper()
	err := exec.RecoverQueryPanic(t.Context(), "worker task t-1", errors.New("boom"))
	return err.Error()
}

func TestQueryPanicMessageSurvivesStageFraming(t *testing.T) {
	raw := panicText(t)
	if !exec.IsQueryPanicMessage(raw) {
		t.Fatalf("a freshly produced panic message is not recognised: %q", raw)
	}
	// The coordinator wraps the worker's text in its own stage/task framing
	// before anything else sees it, so recognition has to survive that.
	framed := fmt.Sprintf("stage %s: task %s failed after %d attempts: %s", "join-3", "t-1", 3, raw)
	if !exec.IsQueryPanicMessage(framed) {
		t.Fatalf("the marker did not survive stage framing: %q", framed)
	}
	if exec.IsQueryPanicMessage("reading s3://bucket/key: connection reset") {
		t.Error("an ordinary I/O failure was classified as a panic")
	}
	if exec.IsQueryPanicMessage("") {
		t.Error("an empty message was classified as a panic")
	}
}

func TestStageTaskFailureCarriesInternalErrorSQLSTATE(t *testing.T) {
	raw := panicText(t)
	err := stageTaskFailure(raw, fmt.Errorf("stage join-3: task t-1 failed after 3 attempts: %s", raw))
	if got := sqlerr.StateOf(err); got != exec.SQLStateInternalError {
		t.Errorf("SQLSTATE = %q, want %q — a panic that crossed the wire must still reach "+
			"the client as internal_error, not the blanket class", got, exec.SQLStateInternalError)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error %q lost the panic value", err)
	}

	// An ordinary failure keeps whatever code it already had (usually none),
	// so this must not blanket-stamp XX000 onto everything.
	plain := stageTaskFailure("worker died", errors.New("stage join-3: task t-1 failed"))
	if got := sqlerr.StateOf(plain); got != "" {
		t.Errorf("SQLSTATE = %q on a non-panic failure, want none", got)
	}
}

// TestRecoveredPanicIsNotRetried: a panic is deterministic, so re-running the
// task cannot succeed. Spending the stage's whole retry budget on it delays
// the failure and gives whatever the panic damaged two more chances to
// happen.
func TestRecoveredPanicIsNotRetried(t *testing.T) {
	task := distributed.Task{ID: "t-1", QueryID: "q-1", StageID: "s-1"}
	republished := 0
	tr := newTaskRetrier([]distributed.Task{task}, true,
		func(distributed.Task) { republished++ },
		slog.New(slog.DiscardHandler), "s-1", nil)

	done := tr.Observe(distributed.ResultNotification{
		TaskID: "t-1", QueryID: "q-1", StageID: "s-1",
		Success: false, Error: panicText(t),
	})
	if !done {
		t.Fatal("a panicking task was not terminal on its first failure")
	}
	if republished != 0 {
		t.Errorf("the task was re-dispatched %d times; a recovered panic must not be retried",
			republished)
	}
	if _, msg, failed := tr.FirstError(); !failed || !exec.IsQueryPanicMessage(msg) {
		t.Errorf("FirstError() = (%q, %v), want the panic message and failed=true", msg, failed)
	}
}

// An ordinary failure must still retry — the check above must not have turned
// the retrier off for everything.
func TestOrdinaryFailureStillRetries(t *testing.T) {
	task := distributed.Task{ID: "t-2", QueryID: "q-1", StageID: "s-1"}
	// republish fires on its own goroutine (off the NATS subscription), so
	// the test waits for it rather than reading a counter it races.
	republished := make(chan distributed.Task, 4)
	tr := newTaskRetrier([]distributed.Task{task}, true,
		func(t distributed.Task) { republished <- t },
		slog.New(slog.DiscardHandler), "s-1", nil)

	if done := tr.Observe(distributed.ResultNotification{
		TaskID: "t-2", QueryID: "q-1", StageID: "s-1",
		Success: false, Error: "reading s3://bucket/key: connection reset",
	}); done {
		t.Fatal("an ordinary failure went terminal on its first attempt")
	}
	select {
	case got := <-republished:
		if got.ID != "t-2" {
			t.Errorf("re-dispatched %q, want t-2", got.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an ordinary failure was never re-dispatched — the panic check must not " +
			"have disabled retries for everything")
	}
}
