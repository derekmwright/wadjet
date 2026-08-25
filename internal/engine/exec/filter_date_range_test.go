package exec

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// dateBatch is a single-row DATE column for the #451 regression tests below.
func dateBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "d", Type: parquet.TypeDate},
	}, 1)
	b.Columns[0].SetValue(0, int32(0)) // 1970-01-01
	return b
}

// TestKernelFilterDateOutOfRangeRaises22008 pins #451's error-surfacing half:
// a DATE constant whose day count does not fit the column's int32 encoding
// must raise SQLSTATE 22008 (datetime_field_overflow), the same "nil
// kernel, caller raises" convention TestKernelFilterDecimalAndBytes and the
// CIDR/inet arms already use, rather than silently comparing against a
// clamped or truncated day count.
func TestKernelFilterDateOutOfRangeRaises22008(t *testing.T) {
	f := NewKernelFilter("d", OpEq, int64(math.MaxInt32)+1)
	_, err := f.Execute(context.Background(), dateBatch(t))
	if err == nil {
		t.Fatal("want an error for a DATE literal out of int32 range")
	}
	if got := sqlerr.StateOf(err); got != "22008" {
		t.Errorf("SQLSTATE = %q, want 22008; err = %v", got, err)
	}
}

// TestInFilterDateOutOfRangeRaises22008 is the IN-list counterpart.
func TestInFilterDateOutOfRangeRaises22008(t *testing.T) {
	f := NewInFilter("d", []any{int64(math.MaxInt32) + 1}, false)
	_, err := f.Execute(context.Background(), dateBatch(t))
	if err == nil {
		t.Fatal("want an error for a DATE literal out of int32 range")
	}
	if got := sqlerr.StateOf(err); got != "22008" {
		t.Errorf("SQLSTATE = %q, want 22008; err = %v", got, err)
	}
}

// TestKernelFilterDateInRangeStillWorks guards against the fix over-firing:
// an ordinary, representable DATE literal must still compare normally, not
// raise 22008.
func TestKernelFilterDateInRangeStillWorks(t *testing.T) {
	f := NewKernelFilter("d", OpEq, "1970-01-01")
	out, err := f.Execute(context.Background(), dateBatch(t))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := selOf(out); len(got) != 1 || got[0] != 0 {
		t.Errorf("Sel = %v, want [0]", got)
	}
}

// TestKernelFilterDateSentinelLiteralIsNotAnError pins what dateConstError's
// doc now says, so the doc cannot drift back: '9999-12-31' — the literal
// #451 was reported for — is not out of range and never was. It is 2,932,896
// days from the epoch, about 700x inside int32, and only the old
// time.Duration arithmetic made it look otherwise. It must COMPARE, not
// raise 22008.
//
// And no DATE string literal can reach the 22008 path at all: every layout
// kernel.parseDateToDays accepts uses time.Parse's "2006", which takes
// exactly four digits, so 9999-12-31 is the largest date a literal can spell.
// The guard stays live for a raw int64/int day count, which the two tests
// above use and which no parser bounds.
func TestKernelFilterDateSentinelLiteralIsNotAnError(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "d", Type: parquet.TypeDate},
	}, 2)
	b.Columns[0].SetValue(0, int32(0))       // 1970-01-01
	b.Columns[0].SetValue(1, int32(2932896)) // 9999-12-31

	f := NewKernelFilter("d", OpEq, "9999-12-31")
	out, err := f.Execute(context.Background(), b)
	if err != nil {
		t.Fatalf("`d = '9999-12-31'` raised %v — the literal is representable and must compare", err)
	}
	if got := selOf(out); len(got) != 1 || got[0] != 1 {
		t.Errorf("Sel = %v, want [1] (the 9999-12-31 row, not the epoch row a clamped day count would match)", got)
	}

	// The largest and smallest dates a four-digit-year literal can spell,
	// both well inside the int32 day range the guard protects.
	for _, lit := range []string{"9999-12-31", "0001-01-01"} {
		if err := dateConstError(batch.TypeDate, lit); err != nil {
			t.Errorf("dateConstError(%q) = %v, want nil — no DATE string literal can be out of range", lit, err)
		}
	}
}
