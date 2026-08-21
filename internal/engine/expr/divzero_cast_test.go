package expr

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// wantSQLStateRaise runs fn and asserts it panics with a FatalEvalPanic whose
// error carries the given SQLSTATE (#367: statements PostgreSQL refuses must
// error, never answer). The regression these tests pin: 1/0 answered 0 and
// CAST('abc' AS integer) answered 0.
func wantSQLStateRaise(t *testing.T, state string, fn func()) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		if r == nil {
			t.Fatalf("expected a FatalEvalPanic carrying SQLSTATE %s; evaluation returned normally", state)
		}
		fe, ok := r.(interface{ FatalEvalError() error })
		if !ok {
			t.Fatalf("panic value %T does not implement FatalEvalError", r)
		}
		if got := sqlerr.StateOf(fe.FatalEvalError()); got != state {
			t.Fatalf("SQLSTATE = %q, want %q (err: %v)", got, state, fe.FatalEvalError())
		}
	}()
	fn()
}

func TestDivideByZeroRaises22012(t *testing.T) {
	t.Run("BinOp float divide", func(t *testing.T) {
		e := &BinOp{Left: &Lit{Val: float64(1)}, Right: &Lit{Val: float64(0)}, Op: "/"}
		wantSQLStateRaise(t, "22012", func() { e.Eval(nil, 0) })
	})
	t.Run("BinOp modulo", func(t *testing.T) {
		e := &BinOp{Left: &Lit{Val: int64(10)}, Right: &Lit{Val: int64(0)}, Op: "%"}
		wantSQLStateRaise(t, "22012", func() { e.Eval(nil, 0) })
	})
	t.Run("BinOpFloat64 per-row", func(t *testing.T) {
		e := &BinOpFloat64{Left: &Lit{Val: float64(10)}, Right: &Lit{Val: float64(0)}, Op: "/"}
		wantSQLStateRaise(t, "22012", func() { e.EvalFloat64(nil, 0) })
	})
	t.Run("BinOpInt64 per-row", func(t *testing.T) {
		e := &BinOpInt64{Left: &Lit{Val: int64(10)}, Right: &Lit{Val: int64(0)}, Op: "/"}
		wantSQLStateRaise(t, "22012", func() { e.EvalInt64(nil, 0) })
	})
}

// TestDivideByNullStaysNull pins the boundary of the 22012 raise: a NULL
// divisor answers NULL — only a genuine zero errors.
func TestDivideByNullStaysNull(t *testing.T) {
	e := &BinOp{Left: &Lit{Val: float64(1)}, Right: &Lit{Val: nil}, Op: "/"}
	if v := e.Eval(nil, 0); v != nil {
		t.Fatalf("1/NULL = %v, want NULL", v)
	}
	f := &BinOpFloat64{Left: &Lit{Val: float64(1)}, Right: &Lit{Val: nil}, Op: "/"}
	if v, ok := f.EvalFloat64(nil, 0); ok {
		t.Fatalf("1/NULL = %v ok=true, want NULL", v)
	}
}

// TestVecDivideByZeroForcesPerRowPass pins the vectorized kernel's contract:
// a zero divisor slot makes EvalFloat64Vec report hasNull=true so the caller
// re-checks per row (where a NULL divisor answers NULL and a genuine zero
// raises). Before this, the vec loop skipped the slot and delivered 0.
func TestVecDivideByZeroForcesPerRowPass(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeFloat64},
		{Name: "b", Type: parquet.TypeFloat64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"a": 10.0, "b": 2.0},
		{"a": 10.0, "b": 0.0},
	})
	e := &BinOpFloat64{Left: &ColRef{Name: "a"}, Right: &ColRef{Name: "b"}, Op: "/"}
	dst := make([]float64, 2)
	if hasNull := e.EvalFloat64Vec(b, dst, 2); !hasNull {
		t.Fatalf("EvalFloat64Vec over a zero divisor reported hasNull=false; the caller would deliver %v silently", dst)
	}
	if dst[0] != 5.0 {
		t.Fatalf("non-zero divisor row = %v, want 5.0", dst[0])
	}
	// The per-row pass the caller now runs raises on the genuine zero.
	wantSQLStateRaise(t, "22012", func() { e.EvalFloat64(b, 1) })
}

func TestCastInvalidTextToIntegerRaises22P02(t *testing.T) {
	cases := []struct {
		name string
		val  any
		dest string
	}{
		{"non-numeric text", "abc", "integer"},
		{"empty string", "", "integer"},
		{"trailing garbage", "12abc", "integer"},
		{"bigint spelling", "abc", "bigint"},
		{"byte slice", []byte("abc"), "int"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := &Cast{Operand: &Lit{Val: c.val}, DestType: c.dest}
			wantSQLStateRaise(t, "22P02", func() { e.Eval(nil, 0) })
		})
	}
}

func TestCastValidTextToIntegerStillConverts(t *testing.T) {
	cases := []struct {
		in   any
		dest string
		want int64
	}{
		{"12", "integer", 12},
		{" 42 ", "integer", 42},
		{"-7", "bigint", -7},
		// The lenient numeric path is unchanged: fractions truncate (#373
		// tracks the truncate-vs-round divergence).
		{"3.9", "integer", 3},
		{int64(5), "integer", 5},
		{float64(2.9), "integer", 2},
	}
	for _, c := range cases {
		e := &Cast{Operand: &Lit{Val: c.in}, DestType: c.dest}
		got := e.Eval(nil, 0)
		if got != c.want {
			t.Errorf("CAST(%v AS %s) = %v, want %d", c.in, c.dest, got, c.want)
		}
	}
	// NULL in, NULL out — never an error.
	e := &Cast{Operand: &Lit{Val: nil}, DestType: "integer"}
	if got := e.Eval(nil, 0); got != nil {
		t.Errorf("CAST(NULL AS integer) = %v, want NULL", got)
	}
}

// TestCastErrorNamesTypeAndValue pins the error text a client acts on.
func TestCastErrorNamesTypeAndValue(t *testing.T) {
	e := &Cast{Operand: &Lit{Val: "abc"}, DestType: "integer"}
	defer func() {
		r := recover()
		fe, ok := r.(interface{ FatalEvalError() error })
		if !ok {
			t.Fatalf("panic value %T does not implement FatalEvalError", r)
		}
		msg := fe.FatalEvalError().Error()
		if !strings.Contains(msg, "integer") || !strings.Contains(msg, "abc") {
			t.Fatalf("error %q names neither the type nor the value", msg)
		}
	}()
	e.Eval(nil, 0)
}
