package exec

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync/atomic"
)

// Every query entry point and spawned goroutine must contain unexpected panics
// so one query cannot terminate other connections (#509).
// Preserve deliberate FatalEvalPanic errors and precise SQLSTATEs;
// recoverFatalEval re-panics other classes for this outer boundary to catch.
// Unexpected panics become XX000 errors carrying the panic value and truncated
// stack, logged at error level with query id; callers cancel and drain normally.
// QueryPanicsRecovered counts each event so gates still detect the defect:
// recovery must neither swallow the client error nor hide the stack.
// See docs/internals/query-panic-containment.md for the design.

// SQLStateInternalError is PostgreSQL's internal_error, what a client is told
// when the server hit something it has no better code for.
const SQLStateInternalError = "XX000"

// QueryPanic is the error an unexpected panic becomes at a query boundary.
type QueryPanic struct {
	// Where names the boundary that caught it — "hash join build",
	// "pipeline worker", "coordinator query" — so a log line says which
	// goroutine died without needing the stack parsed.
	Where string
	// Value is the recovered panic value.
	Value any
	// Stack is the goroutine's stack at the panic, truncated.
	Stack string
}

// queryPanicPrefix opens every QueryPanic message. It is load-bearing across
// a process boundary: a worker's failure reaches the coordinator as a plain
// STRING in a ResultNotification, with no error chain and no type left, so
// this prefix is the only thing that still says "this was a panic" once the
// task result has crossed the wire. Both the retry decision and the SQLSTATE
// the client is handed key off it (IsQueryPanicMessage). Do not reword it
// without changing that matcher.
const queryPanicPrefix = "internal error in "

func (p *QueryPanic) Error() string {
	return fmt.Sprintf("%s%s: %v", queryPanicPrefix, p.Where, p.Value)
}

// IsQueryPanicMessage reports whether a failure message came from a query
// boundary — including one that crossed the distributed wire as text and was
// then wrapped by the coordinator's stage/task framing, hence Contains rather
// than HasPrefix.
func IsQueryPanicMessage(msg string) bool {
	return strings.Contains(msg, queryPanicPrefix)
}

// SQLState satisfies sqlerr.Coder, so pgwire reports XX000 rather than the
// blanket class it applies to an uncoded error.
func (p *QueryPanic) SQLState() string { return SQLStateInternalError }

// Unwrap exposes a panicked error value (a runtime.Error, say) to errors.Is
// and errors.As.
func (p *QueryPanic) Unwrap() error {
	if err, ok := p.Value.(error); ok {
		return err
	}
	return nil
}

// queryPanics counts the panics this boundary has converted. It is the gate's
// handle on "a query reached a panic at all", which recovery would otherwise
// make invisible: the server survives, so no process dies for a crash gate to
// notice. Monotonic for the process; read a delta around the work in question.
var queryPanics atomic.Int64

// QueryPanicsRecovered returns how many unexpected panics this process has
// converted at a query boundary. Deliberately raised FatalEvalPanics are query
// errors, not panics in this sense, and are not counted.
func QueryPanicsRecovered() int64 { return queryPanics.Load() }

// queryIDKey types the context slot carrying the query id.
type queryIDKey struct{}

// WithQueryID tags ctx with the id every panic logged under it will name.
func WithQueryID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, queryIDKey{}, id)
}

// QueryIDFromContext returns the id WithQueryID attached, or "".
func QueryIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(queryIDKey{}).(string)
	return id
}

// maxPanicStack bounds the captured stack. Enough to name the failing frames
// and their callers; short enough that a query storm cannot fill a disk with
// one log line each.
const maxPanicStack = 8 << 10

// RecoverQueryPanic converts a non-nil recover() value into an error and never
// re-panics.
//
//   - A FatalEvalPanic keeps its precise error and SQLSTATE — the designed
//     class, unchanged.
//   - Anything else becomes a *QueryPanic: SQLSTATE XX000, logged at error
//     level with the query id and a truncated stack, and counted.
//
// where names the boundary, for the log line and the message.
func RecoverQueryPanic(ctx context.Context, where string, r any) error {
	if fe, ok := r.(FatalEvalPanic); ok {
		return fe.FatalEvalError()
	}
	qp := &QueryPanic{Where: where, Value: r, Stack: capturePanicStack()}
	queryPanics.Add(1)
	slog.Error("recovered a panic at a query boundary — the query fails, the server does not",
		"where", where, "query_id", QueryIDFromContext(ctx), "panic", fmt.Sprint(r),
		"stack", qp.Stack)
	return qp
}

// CatchQueryPanic is the deferred form, for a goroutine that reports its error
// somewhere other than a return value:
//
//	defer CatchQueryPanic(ctx, "hash join build", func(err error) {
//	    buildErr = err
//	    cancel()
//	})
//
// report is called only when a panic was in flight, so the happy path costs
// one deferred call per goroutine and nothing per batch or per row.
func CatchQueryPanic(ctx context.Context, where string, report func(error)) {
	r := recover()
	if r == nil {
		return
	}
	report(RecoverQueryPanic(ctx, where, r))
}

// capturePanicStack renders this goroutine's stack, dropping the boundary's
// own frames so the first thing a reader sees is the code that panicked.
func capturePanicStack() string {
	buf := make([]byte, maxPanicStack)
	s := string(buf[:runtime.Stack(buf, false)])
	// runtime.Stack starts at capturePanicStack; skip it, its caller
	// (RecoverQueryPanic) and, when present, CatchQueryPanic. Each frame is
	// two lines: the function, then the file:line.
	lines := strings.Split(s, "\n")
	drop := 1 // the "goroutine N [running]:" header stays
	for drop+1 < len(lines) && isBoundaryFrame(lines[drop]) {
		drop += 2
	}
	return strings.Join(append(lines[:1:1], lines[drop:]...), "\n")
}

func isBoundaryFrame(fn string) bool {
	for _, name := range []string{
		"exec.capturePanicStack", "exec.RecoverQueryPanic", "exec.CatchQueryPanic",
	} {
		if strings.Contains(fn, name) {
			return true
		}
	}
	return false
}
