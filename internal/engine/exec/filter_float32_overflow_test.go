package exec

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// realBatch is a small FLOAT32 (real) column: 0.1, +Inf, 1.5.
func realBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "f", Type: parquet.TypeFloat32},
	}, 3)
	b.Columns[0].SetValue(0, float32(0)+0.1)
	b.Columns[0].SetValue(1, float32(math.Inf(1)))
	b.Columns[0].SetValue(2, float32(1.5))
	return b
}

// TestInFilterFloat32OverflowRaises22003 is the error-surfacing half of the
// #549 fix. Narrowing an IN literal to float32 makes a finite-but-too-large
// literal (1e40) become +Inf; without a guard, `f IN (1e40)` would MATCH the
// genuine +Inf row — a false positive. PostgreSQL raises 22003
// (numeric_value_out_of_range) for the whole predicate instead (verified on
// postgres:17), so the kernel declines and InFilter raises that error, the
// same "nil kernel, caller raises" convention the DATE arm uses for 22008.
func TestInFilterFloat32OverflowRaises22003(t *testing.T) {
	f := NewInFilter("f", []any{1e40, 1.5}, false)
	_, err := f.Execute(context.Background(), realBatch(t))
	if err == nil {
		t.Fatal("want an error for a real literal out of range (1e40)")
	}
	if got := sqlerr.StateOf(err); got != "22003" {
		t.Errorf("SQLSTATE = %q, want 22003; err = %v", got, err)
	}
}

// TestInFilterFloat32InfLiteralIsNotOverflow guards against the overflow guard
// over-firing in a MULTI-element list: a literal that is ITSELF +Inf is a legal
// real value, not a finite-over-range overflow, so the narrowing kernel keeps
// it and it matches the +Inf row rather than raising 22003. Multi-element on
// purpose — that is the arity whose kernel carries the guard.
func TestInFilterFloat32InfLiteralIsNotOverflow(t *testing.T) {
	f := NewInFilter("f", []any{math.Inf(1), 2.5}, false)
	out, err := f.Execute(context.Background(), realBatch(t))
	if err != nil {
		t.Fatalf("`f IN ('Infinity', 2.5)` raised %v — +Inf is a legal real value and must compare", err)
	}
	if got := selOf(out); len(got) != 1 || got[0] != 1 {
		t.Errorf("Sel = %v, want [1] (the +Inf row)", got)
	}
}

// TestInFilterFloat32SingleOverflowNoError pins the arity split's #549
// re-review fix: a SINGLE-element `real IN (1e40)` folds to `= double` in
// PostgreSQL (1e40 is a finite double), returns 0 rows, and raises NO error.
// The pre-arity-split code declined the single-element kernel and raised a
// spurious 22003; this asserts it does not.
func TestInFilterFloat32SingleOverflowNoError(t *testing.T) {
	f := NewInFilter("f", []any{1e40}, false)
	out, err := f.Execute(context.Background(), realBatch(t))
	if err != nil {
		t.Fatalf("`f IN (1e40)` raised %v — a single-element list widens to double and must not error", err)
	}
	if got := selOf(out); len(got) != 0 {
		t.Errorf("Sel = %v, want [] (1e40 matches no real row through the widening compare)", got)
	}
}

// TestInFilterFloat32NullStrippedListKeepsSyntacticArity is the exec-side v4
// regression (#549 re-review). The planner strips a NULL member before the
// kernel, but records the pre-strip count via SetSyntacticLen so the FLOAT32
// width decision still matches PostgreSQL, which casts the whole `{...}` array
// (NULL included) to real[] on >1 element.
//
//	real IN (0.1, NULL)  → survivor [0.1], syntactic 2 → NARROW → matches
//	real IN (1e40, NULL) → survivor [1e40], syntactic 2 → over-range → 22003
func TestInFilterFloat32NullStrippedListKeepsSyntacticArity(t *testing.T) {
	// [0.1, NULL] after the planner's strip: one value, syntactic arity 2.
	f := NewInFilter("f", []any{float32(0) + 0.1}, false)
	f.SetSyntacticLen(2)
	out, err := f.Execute(context.Background(), realBatch(t))
	if err != nil {
		t.Fatalf("`f IN (0.1, NULL)` raised %v — it must narrow and match", err)
	}
	if got := selOf(out); len(got) != 1 || got[0] != 0 {
		t.Errorf("Sel = %v, want [0] (the 0.1 row, narrowed like PG real[])", got)
	}

	// [1e40, NULL] after the strip: one value, syntactic arity 2, over-range.
	f2 := NewInFilter("f", []any{1e40}, false)
	f2.SetSyntacticLen(2)
	_, err = f2.Execute(context.Background(), realBatch(t))
	if err == nil {
		t.Fatal("`f IN (1e40, NULL)` must raise 22003 (syntactic arity 2 narrows to real[])")
	}
	if got := sqlerr.StateOf(err); got != "22003" {
		t.Errorf("SQLSTATE = %q, want 22003; err = %v", got, err)
	}
}
