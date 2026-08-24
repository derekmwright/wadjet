package physical

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// TestScanWorkerPanicBecomesScanError is the #400 regression for the scan
// goroutines.
//
// scanWorker and rgWorker run on goroutines the caller does not own, so
// Pipeline.Run's recover cannot see a panic raised there and the PROCESS dies
// — in a server, every connected client's query. The panic that reaches them
// in practice is batch.TypeMismatchError from Vector.SetValue, whose whole
// design (#361) is "a query error, never the server": that is #393, where
// every read of a MAP column fell into the scan's row fallback and died.
//
// recoverWorkerPanic converts that class to its precise error. Since #511 it
// also converts everything else, rather than re-raising into a process exit:
// see TestScanWorkerPanicReportsRealBugsAsInternalErrors below.
func TestScanWorkerPanicBecomesScanError(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	cancelled := false
	inner := &scanSourceInner{
		errCh:  make(chan error, 1),
		cancel: func() { cancelled = true; cancel() },
	}

	func() {
		defer inner.recoverWorkerPanic(context.Background(), "scan worker")
		v := batch.NewVector(batch.TypeFloat64, 1)
		v.SetValue(0, "not a float") // raises the #361 guard
	}()

	select {
	case err := <-inner.errCh:
		if !strings.Contains(err.Error(), "scan worker") {
			t.Errorf("error %q does not name the goroutine it came from", err)
		}
		if !strings.Contains(err.Error(), "FLOAT64") {
			t.Errorf("error %q lost the type-mismatch detail", err)
		}
	default:
		t.Fatal("no error was delivered on errCh — the panic went nowhere and would have killed the process")
	}
	if !cancelled {
		t.Error("the sibling workers were not cancelled")
	}
}

// TestScanWorkerPanicReportsRealBugsAsInternalErrors is #511's reversal of
// the previous contract here.
//
// Re-raising a genuine bug past this recover was the right call for keeping
// the defect visible and the wrong one for the process: nothing above a scan
// goroutine catches the re-raise, so a nil dereference on one query's worker
// ended the server for every connected client. The bug stays visible without
// that price — it becomes an internal error (XX000) with the stack in the
// log, and QueryPanicsRecovered counts it so the process-killer gate can fail
// CI on a query that reaches one.
func TestScanWorkerPanicReportsRealBugsAsInternalErrors(t *testing.T) {
	inner := &scanSourceInner{errCh: make(chan error, 1)}
	before := exec.QueryPanicsRecovered()

	got := func() (r any) {
		defer func() { r = recover() }()
		func() {
			defer inner.recoverWorkerPanic(context.Background(), "scan worker")
			panic("a genuine bug")
		}()
		return nil
	}()
	if got != nil {
		t.Fatalf("the panic was re-raised (%#v) — it must not escape the goroutine", got)
	}

	select {
	case err := <-inner.errCh:
		if !strings.Contains(err.Error(), "a genuine bug") {
			t.Errorf("error %q lost the panic value", err)
		}
		if !strings.Contains(err.Error(), "scan worker") {
			t.Errorf("error %q does not name the goroutine it came from", err)
		}
		var qp *exec.QueryPanic
		if !errors.As(err, &qp) {
			t.Fatalf("error %v is not a *exec.QueryPanic, so it carries no SQLSTATE", err)
		}
		if qp.SQLState() != exec.SQLStateInternalError {
			t.Errorf("SQLSTATE = %q, want %q", qp.SQLState(), exec.SQLStateInternalError)
		}
		if qp.Stack == "" {
			t.Error("no stack was captured — the log line would name no code")
		}
	default:
		t.Fatal("no error was delivered on errCh — the panic went nowhere")
	}

	if after := exec.QueryPanicsRecovered(); after != before+1 {
		t.Errorf("QueryPanicsRecovered went %d -> %d, want +1: a recovered panic that "+
			"the gate cannot see is a defect the gate cannot fail on", before, after)
	}
}
