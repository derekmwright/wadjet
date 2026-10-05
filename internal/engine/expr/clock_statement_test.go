// SPDX-License-Identifier: MIT

package expr

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// tickingClock answers a later instant, one millisecond on, at every read —
// the live clock at its worst: every evaluation sees a different value.
func tickingClock(start time.Time) func() time.Time {
	n := 0
	return func() time.Time {
		n++
		return start.Add(time.Duration(n) * time.Millisecond)
	}
}

func compileText(t *testing.T, sql string, opts ...CompileOption) Expr {
	t.Helper()
	node, err := plansql.ParseExpression(sql)
	if err != nil {
		t.Fatal(err)
	}
	e, err := Compile(node, opts...)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return e
}

// TestStatementClockAccessor: the ONE accessor and the door's stamp. The
// outermost door wins; a worker's stamp replaces; an unstamped context has
// none and binds nothing.
func TestStatementClockAccessor(t *testing.T) {
	at := time.Date(2026, 3, 4, 5, 6, 7, 8_000_000, time.UTC)
	defer SetClockForTest(func() time.Time { return at })()

	if _, ok := StatementClock(context.Background()); ok {
		t.Fatal("an unstamped context carries a statement clock")
	}
	if _, ok := StatementClock(nil); ok || WithStatementClock(context.Background()) != nil {
		t.Fatal("WithStatementClock over an unstamped context binds something")
	}
	ctx := StartStatement(context.Background())
	if got, ok := StatementClock(ctx); !ok || !got.Equal(at) {
		t.Fatalf("StartStatement stamped %v (%v), want %v", got, ok, at)
	}
	later := at.Add(time.Hour)
	defer SetClockForTest(func() time.Time { return later })()
	if got, _ := StatementClock(StartStatement(ctx)); !got.Equal(at) {
		t.Errorf("an inner door replaced the outer door's clock: %v, want %v", got, at)
	}
	if got, _ := StatementClock(ContextWithStatementClock(ctx, later)); !got.Equal(later) {
		t.Errorf("ContextWithStatementClock did not replace: %v, want %v", got, later)
	}
}

// TestStatementClockBindsEveryClockFunction: a compile bound to a statement
// clock answers it from every clock function on every evaluation, while the
// live clock moves on; unbound, the clock is read per evaluation (the
// behaviour #1566 found) and the hook reports the compile.
func TestStatementClockBindsEveryClockFunction(t *testing.T) {
	at := time.Date(2026, 3, 4, 23, 59, 59, 999_000_000, time.UTC)
	defer SetClockForTest(tickingClock(at))()
	ctx := ContextWithStatementClock(context.Background(), at)
	b := &batch.RecordBatch{Len: 1}
	for _, c := range []struct{ sql, want string }{
		{"now()", "2026-03-04 23:59:59.999"},
		{"CURRENT_TIMESTAMP", "2026-03-04 23:59:59.999"},
		{"LOCALTIMESTAMP", "2026-03-04 23:59:59.999"},
		{"CAST(CURRENT_DATE AS TEXT)", "2026-03-04"},
	} {
		e := compileText(t, "CAST("+c.sql+" AS TEXT)", WithStatementClock(ctx))
		for i := 0; i < 3; i++ {
			if got := e.Eval(b, 0); got != c.want {
				t.Errorf("%s evaluation %d: %v, want %s (the statement's clock)", c.sql, i, got, c.want)
			}
		}
	}
	eq := "now() = now() AND now() = CURRENT_TIMESTAMP AND LOCALTIMESTAMP = now() AND CURRENT_DATE = CAST(now() AS DATE)"
	if got := compileText(t, eq, WithStatementClock(ctx)).Eval(b, 0); got != true {
		t.Errorf("bound: %s = %v, want true", eq, got)
	}
	var unbound []string
	restore := SetUnboundClockHookForTest(func(name string) { unbound = append(unbound, name) })
	defer restore()
	// Unbound, every clock function RAISES (XX000) rather than read a clock
	// of its own: there is no live-clock fallback.
	reads := 0
	defer SetClockForTest(func() time.Time { reads++; return at })()
	for _, fn := range []string{"now()", "CURRENT_TIMESTAMP", "LOCALTIMESTAMP", "CURRENT_DATE"} {
		e := compileText(t, fn)
		func() {
			defer func() {
				r := recover()
				f, ok := r.(interface{ FatalEvalError() error })
				if !ok || sqlerr.StateOf(f.FatalEvalError()) != "XX000" {
					t.Errorf("unbound %s evaluated: recovered %v, want an XX000 refusal", fn, r)
				}
			}()
			v := e.Eval(b, 0)
			t.Errorf("unbound %s answered %v", fn, v)
		}()
	}
	if reads != 0 {
		t.Errorf("an unbound clock function read the clock %d times", reads)
	}
	if len(unbound) != 4 {
		t.Errorf("the unbound hook saw %v, want four compiles", unbound)
	}
}

// TestStatementClockBindsAUDFBody: a CREATE FUNCTION body's clock answers
// the calling statement's value.
func TestStatementClockBindsAUDFBody(t *testing.T) {
	at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	defer SetClockForTest(tickingClock(at))()
	store := NewUDFStore()
	if err := store.Register(UDFDef{Name: "sc_unit_stamp", Params: []string{"x"}, Body: "now()"}, true); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Unregister("sc_unit_stamp", "", true) }()
	ctx := ContextWithStatementClock(context.Background(), at)
	e := compileText(t, "sc_unit_stamp(1) = now() AND sc_unit_stamp(2) = sc_unit_stamp(3)", WithStatementClock(ctx))
	if got := e.Eval(&batch.RecordBatch{Len: 1}, 0); got != true {
		t.Errorf("a UDF body's now() against the statement's: %v, want true", got)
	}
}

// testClock binds a compile to a statement clock read now — what a door does
// — for the tests that evaluate a clock function outside any statement.
func testClock() CompileOption {
	return WithStatementClock(StartStatement(context.Background()))
}
