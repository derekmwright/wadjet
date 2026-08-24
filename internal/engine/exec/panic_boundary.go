package exec

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync/atomic"
)

// The query-scoped panic boundary.
//
// recoverFatalEval converts exactly one class of panic — the deliberately
// raised fatalEval / TypeMismatchError family that expression evaluation uses
// as its error channel — and re-panics everything else. That is the right
// contract for THAT conversion: a runtime panic is a bug, and turning it into
// a generic error at the point it happens would bury it.
//
// It is the wrong contract for PROCESS SURVIVAL. An unrecovered panic on any
// goroutine terminates the whole Go program, so "re-panic the rest" means an
// index-out-of-range in one connection's query kills every other connection's
// query too — and a client can reach one with ordinary SQL (#509 needed no
// join and no error condition). The soak found three independent instances in
// under two minutes, which says the interesting number is not three, it is
// "however many are left".
//
// So the class gets a boundary rather than a patch per instance. Every
// goroutine a query spawns, and every entry point a query is driven through,
// converts ANY panic into an error that carries the panic value and a
// truncated stack, logs it at error level with the query id, and lets the
// caller cancel and drain normally. The client gets SQLSTATE XX000. This is
// ADDITIVE to the FatalEvalPanic contract, not a replacement: a FatalEvalPanic
// still becomes its own precise error with its own SQLSTATE, and only what
// recoverFatalEval declines lands here.
//
// Nothing is swallowed. The error reaches the client, the stack reaches the
// log, and QueryPanicsRecovered counts the event so the process-killer gate
// can fail CI on a query that reaches one of these — a recovered panic is
// still a defect, it is just no longer an outage.

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

func (p *QueryPanic) Error() string {
	return fmt.Sprintf("internal error in %s: %v", p.Where, p.Value)
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
