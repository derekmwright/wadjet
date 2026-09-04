package expr

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestEXISTSAndAScalarSubqueryReadOnlyTheRowsTheyNeed asserts what each
// construct ASKS FOR, which is the half no answer-comparing test can see.
//
// EXISTS asks whether there is A row: the first one answers it and nothing
// above the evaluator has ever looked at a second. A scalar subquery is a
// value or a 21000: `> 1` is the entire rule, so the second row decides it.
// Both used to read the whole relation and then throw all but the part they
// needed away — invisible in every answer, and expensive in the obvious way.
//
// It became a WRONG ANSWER on the DML doors, which bound what a subquery may
// return: an EXISTS over a big relation was refused (54000) where PostgreSQL
// answers, and a multi-row scalar subquery reported 54000 — a resource
// complaint — where this engine's own rule is 21000. Both are gated on the
// three write doors in pgwire; this is the same property one layer down,
// where the reason it holds is visible.
//
// IN is the control: it wants the SET, so its read is not bounded and its
// bound is a bound on the set itself (expr.WithSetRowBound).
func TestEXISTSAndAScalarSubqueryReadOnlyTheRowsTheyNeed(t *testing.T) {
	const base = "SELECT n FROM t"

	// A runner that records what it was asked and answers three rows —
	// more than either bounded construct wants.
	var asked []string
	runner := func(sql string) ([]map[string]any, error) {
		asked = append(asked, sql)
		return []map[string]any{{"n": int64(1)}, {"n": int64(2)}, {"n": int64(3)}}, nil
	}
	reset := func() { asked = nil }

	parsed, err := plansql.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("EXISTS reads one row", func(t *testing.T) {
		reset()
		(&ExistsSubquery{SQL: base, Runner: runner}).EvalBool(nil, 0)
		wantAsked(t, asked, base+" LIMIT 1")
	})
	t.Run("correlated EXISTS reads one row", func(t *testing.T) {
		reset()
		(&CorrelatedExistsSubquery{Runner: runner, ParsedInfo: info}).EvalBool(nil, 0)
		wantAsked(t, asked, base+" LIMIT 1")
	})
	t.Run("a scalar subquery reads two rows", func(t *testing.T) {
		reset()
		err := evalPanic(func() { (&ScalarSubquery{SQL: base, Runner: runner}).Eval(nil, 0) })
		wantAsked(t, asked, base+" LIMIT 2")
		wantCardinality(t, err)
	})
	t.Run("a correlated scalar subquery reads two rows", func(t *testing.T) {
		reset()
		err := evalPanic(func() {
			(&CorrelatedScalarSubquery{Runner: runner, ParsedInfo: info}).Eval(nil, 0)
		})
		wantAsked(t, asked, base+" LIMIT 2")
		wantCardinality(t, err)
	})
	t.Run("IN reads the set", func(t *testing.T) {
		reset()
		(&InSubquery{Expr: &Lit{Val: int64(1)}, SQL: base, Runner: runner}).EvalBool(nil, 0)
		wantAsked(t, asked, base)
	})
}

func wantAsked(t *testing.T, asked []string, want string) {
	t.Helper()
	if len(asked) != 1 {
		t.Fatalf("the runner was asked %d times, want 1: %q", len(asked), asked)
	}
	if asked[0] != want {
		t.Errorf("the runner was asked\n  %q\nwant\n  %q\n"+
			"— a construct must read only the rows its own rule needs", asked[0], want)
	}
}

// wantCardinality asserts the 21000 a two-row read is there to raise. The
// count is deliberately absent from the message: the read stopped on purpose,
// so the site knows "more than one" and not how many more.
func wantCardinality(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("a scalar subquery over more than one row must raise, never take the first row")
	}
	var st interface{ SQLState() string }
	if !asSQLState(err, &st) || st.SQLState() != "21000" {
		t.Fatalf("want SQLSTATE 21000 (cardinality_violation), got %v", err)
	}
	if strings.Contains(err.Error(), "rows:") {
		t.Errorf("the message reports a row COUNT from a read that stopped early: %v", err)
	}
}

func asSQLState(err error, out *interface{ SQLState() string }) bool {
	if s, ok := err.(interface{ SQLState() string }); ok {
		*out = s
		return true
	}
	return false
}

// evalPanic runs f and returns the fatal evaluation error it raised, or nil.
func evalPanic(f func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if fe, ok := r.(interface{ FatalEvalError() error }); ok {
				err = fe.FatalEvalError()
				return
			}
			panic(r)
		}
	}()
	f()
	return nil
}
