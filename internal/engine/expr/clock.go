// SPDX-License-Identifier: MIT

package expr

import (
	"context"
	"sync/atomic"
	"time"
)

// The engine's CLOCK, and its one ZONE.
//
// Every clock function reads this and renders in UTC. `NOW()` and
// `CURRENT_TIMESTAMP` already did — expr.formatInstant normalizes to UTC,
// which is what "one rendering" requires of a zoneless TIMESTAMP — and
// `CURRENT_DATE` did NOT: it formatted `time.Now()` in the machine's LOCAL
// zone, so on a host west of Greenwich the two disagreed by a DAY for the
// hours between local midnight-minus-offset and UTC midnight. Found by a
// landing battery run at 20:00 ET, where `CURRENT_DATE` said 2026-09-04 and
// `NOW()` said 2026-09-05 (#870).
//
// PostgreSQL keeps them consistent through the session's TimeZone —
// `current_date = now()::date` is true there, measured on 17.11 under
// `TimeZone = UTC` — and this engine has no session zone to consult. UTC is
// the zone its rendering already commits to (ADR-0012's clock entry: a
// zoneless instant, no offset to print), so it is the zone the clock reads in
// too. If a session TimeZone is ever implemented, both move together.
var clockNow = time.Now

// SetClockForTest replaces the clock every clock function reads and returns a
// function restoring it. TEST ONLY, and NOT safe for parallel tests in the
// same process — it is a package var, like exec's spill knobs.
//
// It exists because the defect it gates is a CONDITION, not a query shape: the
// two functions agree for most of the day and disagree only inside the UTC
// offset, so a gate that reads the real clock passes on the machine that has
// the bug for sixteen hours out of twenty-four.
func SetClockForTest(f func() time.Time) func() {
	prev := clockNow
	clockNow = f
	return func() { clockNow = prev }
}

// The STATEMENT clock (#1566).
//
// PostgreSQL's now() / CURRENT_TIMESTAMP / LOCALTIMESTAMP / CURRENT_DATE are
// the transaction's start time: ONE value for the whole statement, on every
// row and in every clause (measured on 17.11: `count(DISTINCT now())` over
// 4096 rows is 1, `WHERE now() = now()` keeps every row). They read the
// clock at every EVALUATION here, so `now() = now()` was false on a row that
// straddled a millisecond and a 512-row relation held two to five values.
//
// So the clock is read ONCE, when a statement starts, and carried:
//
//   - the door that starts a statement (the embedded DB's Query / Execute,
//     the pgwire connection, the coordinator) stamps its context with
//     StartStatement — the outermost door wins, so a pgwire statement routed
//     to the coordinator and run on the embedded engine is one statement;
//   - every compile that can meet a clock function binds it to that value
//     through WithStatementClock(ctx) — the physical planner, the DML
//     evaluator, the coordinator's merge, and a worker task;
//   - on the stage DAG the task message carries the value
//     (distributed.Task.StatementTime), so a worker never reads its own
//     clock for these functions and a retried task reads the same value.
//
// The four functions keep their registered evaluators (clockFuncs reads the
// live clock) for a compile no door reached. SetUnboundClockHookForTest lets
// a gate assert that none did.

type statementClockKey struct{}

// StartStatement returns ctx carrying the statement clock: the one ctx
// already carries (an outer door started this statement), or the clock read
// now.
func StartStatement(ctx context.Context) context.Context {
	if _, ok := StatementClock(ctx); ok {
		return ctx
	}
	return context.WithValue(ctx, statementClockKey{}, clockNow())
}

// ContextWithStatementClock returns ctx carrying t as the statement clock,
// replacing any it carried — for a worker task, whose value is the one its
// coordinator stamped, never the worker's own clock.
func ContextWithStatementClock(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, statementClockKey{}, t)
}

// StatementClock is the ONE accessor: the instant ctx's statement started.
// ok=false when no door stamped ctx.
func StatementClock(ctx context.Context) (time.Time, bool) {
	if ctx == nil {
		return time.Time{}, false
	}
	t, ok := ctx.Value(statementClockKey{}).(time.Time)
	return t, ok
}

// WithStatementClock binds the clock functions this compile meets to ctx's
// statement clock. With none stamped it binds nothing.
func WithStatementClock(ctx context.Context) CompileOption {
	t, ok := StatementClock(ctx)
	if !ok {
		return nil
	}
	return func(c *compileContext) { c.clock, c.clockBound = t, true }
}

// clockFuncs are the SQL clock functions: the value each answers for a
// statement that started at t.
var clockFuncs = map[string]func(t time.Time) any{
	"now":               func(t time.Time) any { return instantBox(t) },
	"current_timestamp": func(t time.Time) any { return instantBox(t) },
	// LOCALTIMESTAMP is a different DECLARATION (timestamp without time
	// zone) and the same value: one timestamp type, rendered in UTC.
	"localtimestamp": func(t time.Time) any { return instantBox(t) },
	// UTC, the zone every other clock function renders in. It read the
	// machine's LOCAL date, so `CURRENT_DATE` and `CAST(NOW() AS DATE)` named
	// two different days for the hours between local midnight and UTC
	// midnight — #870.
	"current_date": func(t time.Time) any { return dateBox(t.UTC()) },
}

// IsClockFunc reports whether name is a SQL clock function — one value per
// statement.
func IsClockFunc(name string) bool {
	_, ok := clockFuncs[name]
	return ok
}

var unboundClockHook atomic.Pointer[func(name string)]

// SetUnboundClockHookForTest installs f, called with the function's name
// whenever a clock function is compiled with no statement clock bound — a
// compile site no door reached, which reads the live clock per evaluation.
// It returns a function restoring the previous hook. TEST ONLY.
func SetUnboundClockHookForTest(f func(name string)) func() {
	prev := unboundClockHook.Swap(&f)
	return func() { unboundClockHook.Store(prev) }
}

// bindClock binds fc, a call of a clock function, to the compile's
// statement clock.
func bindClock(fc *FuncCall, ctx *compileContext) {
	if ctx.clockBound {
		bindFuncAt(fc, ctx.clock)
		return
	}
	if IsClockFunc(fc.Name) {
		if h := unboundClockHook.Load(); h != nil && *h != nil {
			(*h)(fc.Name)
		}
	}
}

// bindFuncAt binds fc to a statement that started at t: a clock function
// answers t, and a UDF (CREATE FUNCTION) evaluates its body with its clock
// functions answering t.
func bindFuncAt(fc *FuncCall, t time.Time) {
	if read := clockFuncs[fc.Name]; read != nil {
		// Boxed at evaluation, not here: a value outside the TIMESTAMP range
		// raises through the evaluator's refusal, as the live clock's did.
		fc.clockValue = func([]any) any { return read(t) }
		return
	}
	if at := DefaultRegistry.lookupUDFAt(fc.Name); at != nil {
		fc.clockValue = at(t)
	}
}
