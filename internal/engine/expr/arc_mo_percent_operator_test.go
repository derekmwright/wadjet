// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The `%` operator is MOD (#1527): PostgreSQL implements `a % b` and
// `mod(a, b)` by the same pg_proc entries, so the two spellings type, raise
// and answer alike, and here `a % b` parses to the mod() call. These rows
// compile both spellings through the expression compiler — untyped and with
// the input's declared column types, the two compile doors the planners use
// — and evaluate every row: the `%` spelling must answer exactly what the
// MOD spelling answers, value or SQLSTATE, and the cells PostgreSQL 17.11
// answers without a catalogued divergence are pinned to its answer.
//
// The five-arm table over a real plan is
// coordinator.TestArcMOPercentIsModEveryArm, the wire declaration and the
// bound parameters pgwire.TestArcMOPercentIsModOnTheWire, and what CTAS and
// INSERT … SELECT store wadjet.TestArcMOPercentIsModEmbedded.

func moBatch() *batch.RecordBatch {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
		{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
	}, 6)
	b.Len = 6
	is := []any{int32(3), int32(-7), int32(5), int32(0), int32(1), nil}
	bs := []any{int64(30), int64(-70), int64(9000000000), int64(0), int64(1), nil}
	fs := []any{1.5, -2.5, 0.25, 0.0, 100.125, nil}
	for r := 0; r < 6; r++ {
		for c, v := range []any{is[r], bs[r], fs[r]} {
			if v == nil {
				b.Columns[c].Nulls.SetNull(r)
				continue
			}
			b.Columns[c].SetValue(r, v)
		}
	}
	return b
}

// moEval compiles sql and renders every row's answer: the value, NULL, or
// "ERR <sqlstate>" for a raised error (a panic without one is an internal
// error and renders as "ERR XX000 <panic>").
func moEval(t *testing.T, sql string, typed bool) []string {
	t.Helper()
	node, err := plansql.ParseExpressionComplete(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	b := moBatch()
	var e Expr
	if typed {
		e, err = CompileWithColumnTypes(node, nil, map[string]batch.TypeID{
			"i": batch.TypeInt32, "b": batch.TypeInt64, "f": batch.TypeFloat64})
	} else {
		e, err = Compile(node)
	}
	if err != nil {
		return []string{"ERR " + sqlerr.StateOf(err)}
	}
	out := make([]string, b.Len)
	for r := 0; r < b.Len; r++ {
		out[r] = func() (s string) {
			defer func() {
				if p := recover(); p != nil {
					// A query error leaves an evaluator as fatalEval, which
					// the pipeline drivers turn back into the error.
					s = moPanicState(p)
				}
			}()
			v := e.Eval(b, r)
			if v == nil {
				return "NULL"
			}
			return fmt.Sprint(v)
		}()
	}
	return out
}

// moPanicState renders a recovered evaluator panic: a query error by its
// SQLSTATE, anything else as an internal error.
func moPanicState(p any) string {
	if fe, ok := p.(interface{ FatalEvalError() error }); ok && sqlerr.StateOf(fe.FatalEvalError()) != "" {
		return "ERR " + sqlerr.StateOf(fe.FatalEvalError())
	}
	return fmt.Sprintf("ERR XX000 %v", p)
}

// moEvalVec evaluates sql the way exec.Project drives a vectorized float
// expression — the whole batch at once, then the per-row pass for the null
// bits when the batch reports a NULL — and renders it as moEval does. ok is
// false when the compiled expression has no vectorized float path. A raised
// query error fails the batch: it renders as that one answer.
func moEvalVec(t *testing.T, sql string, typed bool) (out []string, ok bool) {
	t.Helper()
	node, err := plansql.ParseExpressionComplete(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	b := moBatch()
	var e Expr
	if typed {
		e, err = CompileWithColumnTypes(node, nil, map[string]batch.TypeID{
			"i": batch.TypeInt32, "b": batch.TypeInt64, "f": batch.TypeFloat64})
	} else {
		e, err = Compile(node)
	}
	if err != nil {
		return nil, false
	}
	ve, isVec := e.(VecFloat64Expr)
	fe, isF := e.(Float64Expr)
	if !isVec || !isF {
		return nil, false
	}
	defer func() {
		if p := recover(); p != nil {
			out, ok = []string{moPanicState(p)}, true
		}
	}()
	dst := make([]float64, b.Len)
	hasNull := ve.EvalFloat64Vec(b, dst, b.Len)
	out = make([]string, b.Len)
	for r := 0; r < b.Len; r++ {
		if hasNull {
			if _, valid := fe.EvalFloat64(b, r); !valid {
				out[r] = "NULL"
				continue
			}
		}
		out[r] = fmt.Sprint(dst[r])
	}
	return out, true
}

func TestArcMOPercentIsModPerRow(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		// pg is PostgreSQL 17.11's answer per row over the batch's six rows
		// (i = 3, -7, 5, 0, 1, NULL); empty for a cell whose PostgreSQL
		// answer is a catalogued divergence (numeric-decimal r21: a quoted
		// literal that is not an integer beside an integer; r11: a double
		// operand), where only the twin rule is asserted.
		pg string
	}{
		{"NULL", "i", "NULL NULL NULL NULL NULL NULL"},
		{"i", "NULL", "NULL NULL NULL NULL NULL NULL"},
		{"CAST(NULL AS INT)", "i", "NULL NULL NULL NULL NULL NULL"},
		{"i", "CAST(NULL AS INT)", "NULL NULL NULL NULL NULL NULL"},
		{"NULL", "b", "NULL NULL NULL NULL NULL NULL"},
		{"2.5", "NULL", "NULL NULL NULL NULL NULL NULL"},
		{"i", "3", "0 -1 2 0 1 NULL"},
		{"i", "-3", "0 -1 2 0 1 NULL"},
		{"i", "'3'", "0 -1 2 0 1 NULL"},
		{"b", "7", "2 0 5 0 1 NULL"},
		{"i", "0", "ERR 22012 ERR 22012 ERR 22012 ERR 22012 ERR 22012 NULL"},
		{"i", "'0'", "ERR 22012 ERR 22012 ERR 22012 ERR 22012 ERR 22012 NULL"},
		{"8", "i", "2 1 3 ERR 22012 0 NULL"},
		{"'8'", "i", "2 1 3 ERR 22012 0 NULL"},
		{"i", "'2.5'", ""},
		{"8", "'2.5'", ""},
		{"f", "2.5", ""},
		{"f", "NULL", ""},
		{"NULL", "f", ""},
		{"f", "i", ""},
		{"i", "f", ""},
		{"f", "0.25", ""},
	} {
		pct, mod := tc.a+" % "+tc.b, "MOD("+tc.a+", "+tc.b+")"
		for _, typed := range []bool{false, true} {
			name := pct
			if typed {
				name += " (typed)"
			}
			t.Run(name, func(t *testing.T) {
				gp, gm := moEval(t, pct, typed), moEval(t, mod, typed)
				if strings.Join(gp, " ") != strings.Join(gm, " ") {
					t.Errorf("%s answers %v, %s answers %v: the operator is MOD", pct, gp, mod, gm)
				}
				for _, g := range gp {
					if strings.HasPrefix(g, "ERR XX000") {
						t.Errorf("%s: an internal error is never an answer: %s", pct, g)
					}
				}
				if tc.pg != "" && strings.Join(gp, " ") != tc.pg {
					t.Errorf("%s = %v, PostgreSQL 17.11 answers %s", pct, gp, tc.pg)
				}
				// The vectorized pass answers what the row pass answers: a
				// value row by row, or the first error a row raises.
				if vp, ok := moEvalVec(t, pct, typed); ok {
					want := strings.Join(gm, " ")
					if len(vp) == 1 && strings.HasPrefix(vp[0], "ERR ") {
						if strings.HasPrefix(vp[0], "ERR XX000") || !strings.Contains(want, vp[0]) {
							t.Errorf("%s vectorized raised %s, MOD answers %v", pct, vp[0], gm)
						}
					} else if strings.Join(vp, " ") != want {
						t.Errorf("%s vectorized answers %v, MOD answers %v", pct, vp, gm)
					}
				}
			})
		}
	}
}

// TestArcMOFloatRemainderNodeOneFunction: BinOpFloat64's vectorized remainder
// is its row remainder. SQL's `%` parses to mod() and no longer builds this
// node, so it is constructed directly here.
func TestArcMOFloatRemainderNodeOneFunction(t *testing.T) {
	b := moBatch()
	for _, right := range []float64{2.5, 0.25, -3, 0.5} {
		e := &BinOpFloat64{Left: &ColRef{Name: "f"}, Op: "%", Right: &Lit{Val: right}}
		dst := make([]float64, b.Len)
		var hasNull bool
		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("f %% %v vectorized: %s", right, moPanicState(p))
				}
			}()
			hasNull = e.EvalFloat64Vec(b, dst, b.Len)
		}()
		for r := 0; r < b.Len; r++ {
			v, ok := e.EvalFloat64(b, r)
			if !ok {
				if !hasNull {
					t.Errorf("f %% %v row %d: the row pass answers NULL and the batch reports none", right, r)
				}
				continue
			}
			if dst[r] != v {
				t.Errorf("f %% %v row %d: vectorized %v, row %v", right, r, dst[r], v)
			}
		}
	}
}
