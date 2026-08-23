package physical

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
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
// recoverWorkerPanic must convert exactly that class and re-raise anything
// else, which is the same policy Pipeline.Run applies — a genuine bug still
// crashes rather than being reported as a query error.
func TestScanWorkerPanicBecomesScanError(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	cancelled := false
	inner := &scanSourceInner{
		errCh:  make(chan error, 1),
		cancel: func() { cancelled = true; cancel() },
	}

	func() {
		defer inner.recoverWorkerPanic("scan worker")
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

// TestScanWorkerPanicRepanicsRealBugs: a panic that is NOT a FatalEvalPanic is
// a genuine bug and must still crash, exactly as it does inside Pipeline.Run.
// Silently turning a nil dereference into "query error" would hide it.
func TestScanWorkerPanicRepanicsRealBugs(t *testing.T) {
	inner := &scanSourceInner{errCh: make(chan error, 1)}

	got := func() (r any) {
		defer func() { r = recover() }()
		func() {
			defer inner.recoverWorkerPanic("scan worker")
			panic("a genuine bug")
		}()
		return nil
	}()
	if got == nil {
		t.Fatal("a non-FatalEvalPanic was swallowed; it must be re-raised")
	}
	if s, ok := got.(string); !ok || s != "a genuine bug" {
		t.Fatalf("re-raised %#v, want the original panic value", got)
	}
	select {
	case err := <-inner.errCh:
		t.Fatalf("a genuine bug was reported as a query error: %v", err)
	default:
	}
}
