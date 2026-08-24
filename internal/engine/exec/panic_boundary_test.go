package exec

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestRecoverQueryPanicKeepsTheDesignedClass: a FatalEvalPanic is a query
// error with its own SQLSTATE, and the boundary must not flatten it into a
// generic internal error.
func TestRecoverQueryPanicKeepsTheDesignedClass(t *testing.T) {
	want := sqlerr.New("22P02", `invalid input syntax for type integer: "x"`)
	before := QueryPanicsRecovered()

	err := RecoverQueryPanic(context.Background(), "test", fatalEvalError{err: want})
	if !errors.Is(err, error(want)) {
		t.Fatalf("err = %v, want the FatalEvalPanic's own error", err)
	}
	if got := sqlerr.StateOf(err); got != "22P02" {
		t.Errorf("SQLSTATE = %q, want 22P02 — the designed class keeps its code", got)
	}
	if after := QueryPanicsRecovered(); after != before {
		t.Errorf("a designed query error was counted as a panic (%d -> %d)", before, after)
	}
}

// TestRecoverQueryPanicConvertsEverythingElse is #511's core contract: any
// other panic becomes a reportable internal error instead of ending the
// process, and the event is countable.
func TestRecoverQueryPanicConvertsEverythingElse(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  string
	}{
		{"string", "boom", "boom"},
		{"error", errors.New("wrapped boom"), "wrapped boom"},
		{"runtime", func() any {
			var s []int
			defer func() {}()
			return recoverOf(func() { _ = s[3] })
		}(), "index out of range"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := QueryPanicsRecovered()
			err := RecoverQueryPanic(context.Background(), "widget builder", tc.value)
			if err == nil {
				t.Fatal("err = nil, want an internal error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %q, want it to carry %q", err, tc.want)
			}
			if !strings.Contains(err.Error(), "widget builder") {
				t.Errorf("err = %q, does not name the boundary", err)
			}
			if got := sqlerr.StateOf(err); got != SQLStateInternalError {
				t.Errorf("SQLSTATE = %q, want %q", got, SQLStateInternalError)
			}
			var qp *QueryPanic
			if !errors.As(err, &qp) || qp.Stack == "" {
				t.Fatalf("err = %v, want a *QueryPanic carrying a stack", err)
			}
			if strings.Contains(qp.Stack, "capturePanicStack") {
				t.Error("the stack still starts inside the boundary's own frames")
			}
			if after := QueryPanicsRecovered(); after != before+1 {
				t.Errorf("QueryPanicsRecovered %d -> %d, want +1", before, after)
			}
		})
	}
}

// TestQueryPanicErrorTextExcludesTheStack: the message is what travels. A
// worker's task failure carries it across the wire as a plain string and the
// coordinator hands that to the SQL client, so 8 KB of Go frames appended
// here lands in a psql ERROR line. The stack belongs in the log, which
// RecoverQueryPanic already writes with the query id.
func TestQueryPanicErrorTextExcludesTheStack(t *testing.T) {
	err := RecoverQueryPanic(context.Background(), "worker task t-1", "boom")
	var qp *QueryPanic
	if !errors.As(err, &qp) {
		t.Fatalf("err = %v, want a *QueryPanic", err)
	}
	if qp.Stack == "" {
		t.Fatal("no stack captured — the log line would name no code")
	}
	if strings.Contains(err.Error(), "goroutine ") || strings.Contains(err.Error(), ".go:") {
		t.Errorf("Error() carries the stack, which travels to the SQL client:\n%s", err.Error())
	}
	if len(err.Error()) > 512 {
		t.Errorf("Error() is %d bytes; a client-facing message this long is a stack in "+
			"disguise", len(err.Error()))
	}
}

// TestQueryPanicUnwrapsAPanickedError lets errors.Is/As reach past the
// boundary, so a caller can still match a sentinel that was panicked.
func TestQueryPanicUnwrapsAPanickedError(t *testing.T) {
	sentinel := errors.New("sentinel")
	err := RecoverQueryPanic(context.Background(), "test", fmt.Errorf("outer: %w", sentinel))
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is lost the panicked error chain: %v", err)
	}
}

// TestRecoverQueryPanicLogsTheQueryID pins the context plumbing the log line
// depends on.
func TestRecoverQueryPanicLogsTheQueryID(t *testing.T) {
	ctx := WithQueryID(context.Background(), "abc123")
	if got := QueryIDFromContext(ctx); got != "abc123" {
		t.Errorf("QueryIDFromContext = %q, want abc123", got)
	}
	if got := QueryIDFromContext(context.Background()); got != "" {
		t.Errorf("QueryIDFromContext on a bare context = %q, want empty", got)
	}
	//nolint:staticcheck // a nil context is exactly what this guard is for
	if got := QueryIDFromContext(nil); got != "" {
		t.Errorf("QueryIDFromContext(nil) = %q, want empty", got)
	}
}

// TestCatchQueryPanicReportsOnlyOnPanic: the happy path must not call report,
// and must not cost anything beyond the one deferred call.
func TestCatchQueryPanicReportsOnlyOnPanic(t *testing.T) {
	reported := 0
	func() {
		defer CatchQueryPanic(context.Background(), "test", func(error) { reported++ })
	}()
	if reported != 0 {
		t.Fatalf("report ran %d times on a clean return", reported)
	}
	func() {
		defer CatchQueryPanic(context.Background(), "test", func(error) { reported++ })
		panic("boom")
	}()
	if reported != 1 {
		t.Fatalf("report ran %d times on a panic, want 1", reported)
	}
}

// panicOp panics with a value the FatalEvalPanic contract does NOT cover —
// the class that used to end the process.
type panicOp struct{ serial bool }

func (o *panicOp) Init(context.Context) error { return nil }

func (o *panicOp) Execute(context.Context, *batch.RecordBatch) (*batch.RecordBatch, error) {
	var offsets []int32
	_ = offsets[1] // the #509 shape: index out of range on an empty slice
	return nil, nil
}

func (o *panicOp) Close() error { return nil }

func (o *panicOp) Clone() UnaryOperator { return &panicOp{} }

// panicSink panics from Finalize — the sink half of the boundary.
type panicSink struct{ CollectSink }

func (s *panicSink) Finalize(context.Context) error { panic("sink finalize bug") }

// TestPipelineConvertsAnyPanicToAnError walks the boundary end to end for
// both drivers: an unexpected panic anywhere in a pipeline comes back as an
// internal error the caller can report, and the pipeline still closes.
func TestPipelineConvertsAnyPanicToAnError(t *testing.T) {
	schema := []parquet.Column{{Name: "a", Type: parquet.TypeInt64}}
	rows := make([]map[string]any, 0, 4*batch.DefaultBatchSize)
	for i := 0; i < 4*batch.DefaultBatchSize; i++ {
		rows = append(rows, map[string]any{"a": int64(i)})
	}

	cases := []struct {
		name string
		pipe func() *Pipeline
	}{
		{"serial operator", func() *Pipeline {
			return &Pipeline{
				Source: NewSliceSource(schema, rows),
				Ops:    []UnaryOperator{&panicOp{serial: true}},
				Sink:   &CollectSink{},
			}
		}},
		{"parallel workers", func() *Pipeline {
			return &Pipeline{
				Source:  NewSliceSource(schema, rows),
				Ops:     []UnaryOperator{&panicOp{}},
				Sink:    &CollectSink{},
				Workers: 4,
			}
		}},
		{"sink finalize", func() *Pipeline {
			return &Pipeline{
				Source: NewSliceSource(schema, rows),
				Sink:   &panicSink{},
			}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := tc.pipe()
			err := p.Run(context.Background())
			if err == nil {
				t.Fatal("Run() = nil — the panic was swallowed, which is worse than crashing")
			}
			if got := sqlerr.StateOf(err); got != SQLStateInternalError {
				t.Errorf("SQLSTATE = %q, want %q so the client sees an internal error", got, SQLStateInternalError)
			}
			// Teardown after a converted panic must not panic again.
			if cerr := p.Close(); cerr != nil {
				t.Errorf("Close() after a recovered panic = %v", cerr)
			}
		})
	}
}

// recoverOf runs fn and returns whatever it panicked with.
func recoverOf(fn func()) (r any) {
	defer func() { r = recover() }()
	fn()
	return nil
}
