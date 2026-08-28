package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// intGuardCase describes one integer-family type and how a two-row batch of it
// is populated: row 0 holds ZERO, row 1 holds 42. Zero is the row #536's
// silent sentinel used to match, and 42 is the row a genuinely numeric literal
// must still find.
type intGuardCase struct {
	name     string
	typ      parquet.TypeID
	zero     any // storage-typed 0
	fortyTwo any // storage-typed 42
	pgName   string
}

func intGuardCases() []intGuardCase {
	return []intGuardCase{
		{"int64", parquet.TypeInt64, int64(0), int64(42), "bigint"},
		{"int32", parquet.TypeInt32, int32(0), int32(42), "integer"},
		{"port", parquet.TypePort, int32(0), int32(42), "port"},
		{"protocol", parquet.TypeProtocol, int32(0), int32(42), "protocol"},
		{"duration", parquet.TypeDuration, int64(0), int64(42), "duration"},
	}
}

func intGuardBatch(t *testing.T, c intGuardCase) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{{Name: "k", Type: c.typ}}, 2)
	b.Columns[0].SetValue(0, c.zero)
	b.Columns[0].SetValue(1, c.fortyTwo)
	return b
}

// TestKernelFilterIntGarbageRaises22P02 is #536's regression on the VECTORIZED
// path. Before the fix, kernel.toInt64 read a text literal through
// parseTimestampString, so `k = 'abc'` coerced to 0 and MATCHED the row
// holding zero (the issue reported COUNT(*) = 1 where PostgreSQL raises). A
// literal that names no integer must raise 22P02 (invalid_text_representation)
// on this path, the same "nil kernel, caller raises" convention the DECIMAL,
// BOOL and network arms already use.
func TestKernelFilterIntGarbageRaises22P02(t *testing.T) {
	for _, c := range intGuardCases() {
		c := c
		t.Run(c.name, func(t *testing.T) {
			f := NewKernelFilter("k", OpEq, "abc")
			_, err := f.Execute(context.Background(), intGuardBatch(t, c))
			if err == nil {
				t.Fatalf("`k = 'abc'` on a %s column answered instead of raising — the #536 silent-zero", c.name)
			}
			if got := sqlerr.StateOf(err); got != "22P02" {
				t.Fatalf("SQLSTATE = %q, want 22P02; err = %v", got, err)
			}
			if !strings.Contains(err.Error(), c.pgName) {
				t.Errorf("error %q does not name the column type %q", err.Error(), c.pgName)
			}
		})
	}
}

// TestKernelFilterIntNumericStringStillMatches guards against the fix
// over-firing: a genuinely numeric literal must still compare, and it must
// find the row holding 42 — not the row holding 0 the old silent-zero matched.
func TestKernelFilterIntNumericStringStillMatches(t *testing.T) {
	for _, c := range intGuardCases() {
		c := c
		t.Run(c.name, func(t *testing.T) {
			f := NewKernelFilter("k", OpEq, "42")
			out, err := f.Execute(context.Background(), intGuardBatch(t, c))
			if err != nil {
				t.Fatalf("`k = '42'` on a %s column raised %v — a numeric literal must compare", c.name, err)
			}
			if got := selOf(out); len(got) != 1 || got[0] != 1 {
				t.Errorf("Sel = %v, want [1] (the row holding 42, not the row holding 0)", got)
			}
		})
	}
}

// TestColumnCompareLitIntGarbageRaises22P02 is #536's regression on the
// ROW-AT-A-TIME path (ColumnCompareLit, reached from physical/plan.go and
// worker/executor_fragment.go). It shares kernel.Int64FilterConst /
// Int32FilterConst with the vectorized path, so the two cannot disagree on
// which literal is refused; having no error return of its own, it raises the
// same 22P02 through the pipeline's FatalEvalPanic shape.
func TestColumnCompareLitIntGarbageRaises22P02(t *testing.T) {
	for _, c := range intGuardCases() {
		c := c
		for _, op := range []struct {
			name string
			op   CompareOp
		}{{"eq", OpEq}, {"ne", OpNe}, {"gt", OpGt}} {
			op := op
			t.Run(c.name+"/"+op.name, func(t *testing.T) {
				b := intGuardBatch(t, c)
				pred := ColumnCompareLit("k", op.op, "abc", "")
				raised := func() (r any) {
					defer func() { r = recover() }()
					pred(b, 0)
					return nil
				}()
				if raised == nil {
					t.Fatalf("`k %v 'abc'` on a %s column answered instead of raising", op.name, c.name)
				}
				fe, ok := raised.(interface{ FatalEvalError() error })
				if !ok {
					t.Fatalf("panic value %#v is not the pipeline's FatalEvalPanic shape", raised)
				}
				err := fe.FatalEvalError()
				if err == nil || sqlerr.StateOf(err) != "22P02" {
					t.Fatalf("panic error = %v (SQLSTATE %q), want 22P02", err, sqlerr.StateOf(err))
				}
			})
		}
	}
}

// TestColumnCompareLitIntNumericStringStillMatches is the row-path
// over-firing guard: `k = '42'` must match the row holding 42 and not the row
// holding 0.
func TestColumnCompareLitIntNumericStringStillMatches(t *testing.T) {
	for _, c := range intGuardCases() {
		c := c
		t.Run(c.name, func(t *testing.T) {
			b := intGuardBatch(t, c)
			pred := ColumnCompareLit("k", OpEq, "42", "")
			if pred(b, 0) {
				t.Errorf("`k = '42'` matched the row holding 0 — the #536 silent-zero on the row path")
			}
			if !pred(b, 1) {
				t.Errorf("`k = '42'` did not match the row holding 42")
			}
		})
	}
}

// intOverflowLit is the smallest-spelled literal that overflows each
// integer-family type: the int32-backed types (int32/port/protocol) overflow
// past 2^31, the int64-backed types (int64/duration) past 2^63.
func intOverflowLit(typ parquet.TypeID) string {
	switch typ {
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		return "3000000000"
	default:
		return "99999999999999999999999"
	}
}

// TestKernelFilterIntOverflowRaises22003 pins the SQLSTATE split the #536
// review required: a literal that IS an integer but overflows the column type
// is numeric_value_out_of_range (22003), NOT invalid_text_representation
// (22P02) — PostgreSQL distinguishes them and the message reads
// `value "3000000000" is out of range for type integer`. Both comparison
// paths must agree.
func TestKernelFilterIntOverflowRaises22003(t *testing.T) {
	for _, c := range intGuardCases() {
		c := c
		lit := intOverflowLit(c.typ)
		t.Run("kernel/"+c.name, func(t *testing.T) {
			f := NewKernelFilter("k", OpEq, lit)
			_, err := f.Execute(context.Background(), intGuardBatch(t, c))
			if err == nil {
				t.Fatalf("`k = '%s'` on a %s column answered instead of raising for overflow", lit, c.name)
			}
			if got := sqlerr.StateOf(err); got != "22003" {
				t.Fatalf("SQLSTATE = %q, want 22003 (numeric_value_out_of_range); err = %v", got, err)
			}
			want := `value "` + lit + `" is out of range for type ` + c.pgName
			if err.Error() != want {
				t.Errorf("message = %q, want %q", err.Error(), want)
			}
		})
		t.Run("row/"+c.name, func(t *testing.T) {
			b := intGuardBatch(t, c)
			pred := ColumnCompareLit("k", OpEq, lit, "")
			raised := func() (r any) {
				defer func() { r = recover() }()
				pred(b, 0)
				return nil
			}()
			fe, ok := raised.(interface{ FatalEvalError() error })
			if !ok {
				t.Fatalf("overflow did not raise the FatalEvalPanic shape: %#v", raised)
			}
			if got := sqlerr.StateOf(fe.FatalEvalError()); got != "22003" {
				t.Fatalf("row-path SQLSTATE = %q, want 22003; err = %v", got, fe.FatalEvalError())
			}
		})
	}
}

// TestKernelFilterIntNBSPRaises22P02 pins that the input trims only
// PostgreSQL's whitespace (ASCII space/tab/newline/VT/FF/CR), NOT Unicode:
// a NBSP (U+00A0) before the digits is a non-whitespace byte to PostgreSQL,
// which rejects it with 22P02. strings.TrimSpace would have stripped it and
// wrongly accepted the value.
func TestKernelFilterIntNBSPRaises22P02(t *testing.T) {
	const nbsp = " " + "42" // NBSP + "42"
	for _, c := range intGuardCases() {
		c := c
		t.Run(c.name, func(t *testing.T) {
			f := NewKernelFilter("k", OpEq, nbsp)
			_, err := f.Execute(context.Background(), intGuardBatch(t, c))
			if err == nil {
				t.Fatalf("NBSP-prefixed literal was accepted on a %s column; PostgreSQL rejects it", c.name)
			}
			if got := sqlerr.StateOf(err); got != "22P02" {
				t.Fatalf("SQLSTATE = %q, want 22P02; err = %v", got, err)
			}
		})
	}
}

// TestKernelFilterIntAsciiWhitespaceStillTrims is the NBSP guard's companion:
// the ASCII whitespace PostgreSQL DOES skip must still be trimmed, so a
// tab/space-padded numeric literal compares as the number.
func TestKernelFilterIntAsciiWhitespaceStillTrims(t *testing.T) {
	for _, c := range intGuardCases() {
		c := c
		t.Run(c.name, func(t *testing.T) {
			f := NewKernelFilter("k", OpEq, "\t 42 \n")
			out, err := f.Execute(context.Background(), intGuardBatch(t, c))
			if err != nil {
				t.Fatalf("ASCII-whitespace-padded '42' raised %v on a %s column", err, c.name)
			}
			if got := selOf(out); len(got) != 1 || got[0] != 1 {
				t.Errorf("Sel = %v, want [1] (the row holding 42)", got)
			}
		})
	}
}
