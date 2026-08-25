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
