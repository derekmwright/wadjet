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
	err := stageTaskFailure(true, fmt.Errorf("stage join-3: task t-1 failed after 3 attempts: %s", raw))
	if got := sqlerr.StateOf(err); got != exec.SQLStateInternalError {
		t.Errorf("SQLSTATE = %q, want %q — a panic that crossed the wire must still reach "+
			"the client as internal_error, not the blanket class", got, exec.SQLStateInternalError)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error %q lost the panic value", err)
	}

	// An ordinary failure keeps whatever code it already had (usually none),
	// so this must not blanket-stamp XX000 onto everything.
	plain := stageTaskFailure(false, errors.New("stage join-3: task t-1 failed"))
	if got := sqlerr.StateOf(plain); got != "" {
		t.Errorf("SQLSTATE = %q on a non-panic failure, want none", got)
	}
}

// TestCastErrorTextIsNotMisclassifiedAsPanic is the regression for the
// substring-match bug stageTaskFailure and taskRetrier.Observe used to carry:
// keying "is this a panic" off exec.IsQueryPanicMessage(errMsg) matched on
// the free-form error TEXT, and CAST('internal error in x' AS INT) produces
// exactly `invalid input syntax for type integer: "internal error in x"` —
// a perfectly ordinary 22P02 client error whose message happens to embed the
// panic marker as user data. The old classifier called that XX000 and
// terminal-on-first-failure; it is neither.
func TestCastErrorTextIsNotMisclassifiedAsPanic(t *testing.T) {
	castErrMsg := `invalid input syntax for type integer: "internal error in x"`
	if !exec.IsQueryPanicMessage(castErrMsg) {
		t.Fatalf("test no longer reproduces the collision: %q does not contain the panic marker", castErrMsg)
	}

	// stageTaskFailure must not stamp XX000 onto it: the worker correctly
	// determined this was not a *exec.QueryPanic, so Panicked is false.
	err := stageTaskFailure(false, fmt.Errorf("stage cast-1: task t-1 failed after 1 attempts: %s", castErrMsg))
	if got := sqlerr.StateOf(err); got == exec.SQLStateInternalError {
		t.Errorf("SQLSTATE = %q, a CAST error was misclassified as an internal panic", got)
	}

	// taskRetrier.Observe must still retry it — a recovered panic skips
	// straight to terminal, but this was never one.
	task := distributed.Task{ID: "t-cast", QueryID: "q-1", StageID: "s-1"}
	republished := make(chan distributed.Task, 4)
	tr := newTaskRetrier([]distributed.Task{task}, true,
		func(t distributed.Task) { republished <- t },
		slog.New(slog.DiscardHandler), "s-1", nil)

	if done := tr.Observe(distributed.ResultNotification{
		TaskID: "t-cast", QueryID: "q-1", StageID: "s-1",
		Success: false, Error: castErrMsg, Panicked: false,
	}); done {
		t.Fatal("a CAST error went terminal on its first attempt — retryable classification changed")
	}
	select {
	case got := <-republished:
		if got.ID != "t-cast" {
			t.Errorf("re-dispatched %q, want t-cast", got.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a CAST error was never re-dispatched — it was misclassified as a fatal panic")
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
		Success: false, Error: panicText(t), Panicked: true,
	})
	if !done {
		t.Fatal("a panicking task was not terminal on its first failure")
	}
	if republished != 0 {
		t.Errorf("the task was re-dispatched %d times; a recovered panic must not be retried",
			republished)
	}
	if _, msg, panicked, failed := tr.FirstError(); !failed || !panicked {
		t.Errorf("FirstError() = (%q, panicked=%v, failed=%v), want panicked=true and failed=true", msg, panicked, failed)
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
