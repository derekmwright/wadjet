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
// A VALID DATE literal must never raise, however it is spelled — #560 widened
// the accept-set (a malformed or nonexistent date now raises 22007/22008), and
// the risk of that change is over-rejection, so the two representable sentinels
// below must still resolve to a nil error.
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
			t.Errorf("dateConstError(%q) = %v, want nil — a representable date must not raise", lit, err)
		}
	}
}

// TestKernelFilterInvalidDateRaises pins #560's query half: an unparseable or
// nonexistent DATE literal must raise the PostgreSQL error (22007 for a
// malformed string, 22008 for a nonexistent calendar field), not silently
// match the epoch row — dateBatch holds exactly one row, 1970-01-01, which is
// what `d = '2026-02-30'` used to return the count of.
func TestKernelFilterInvalidDateRaises(t *testing.T) {
	tests := []struct {
		lit   string
		state string
	}{
		{"2026-02-30", "22008"}, // nonexistent calendar date
		{"2026-13-01", "22008"}, // month out of range
		{"2026-02-29", "22008"}, // 2026 is not a leap year
		{"2026/02/30", "22008"}, // slash is valid; the day is not
		{"not-a-date", "22007"}, // not a date at all
		{"5/6/7", "22007"},      // short MDY: refused rather than guessed year-first
	}
	for _, tc := range tests {
		t.Run(tc.lit, func(t *testing.T) {
			f := NewKernelFilter("d", OpEq, tc.lit)
			out, err := f.Execute(context.Background(), dateBatch(t))
			if err == nil {
				t.Fatalf("`d = '%s'` returned %d rows and no error — it matched the epoch instead of raising",
					tc.lit, len(selOf(out)))
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("SQLSTATE = %q, want %q; err = %v", got, tc.state, err)
			}
		})
	}

	// IN-list counterpart routes through the same dateConstError.
	f := NewInFilter("d", []any{"2026-02-30"}, false)
	if _, err := f.Execute(context.Background(), dateBatch(t)); err == nil || sqlerr.StateOf(err) != "22008" {
		t.Errorf("d IN ('2026-02-30'): err = %v, want SQLSTATE 22008", err)
	}

	// The accept-set must not over-reject: a PostgreSQL-valid spelling of the
	// epoch (dateBatch's one row) must MATCH it, not raise (#560).
	for _, lit := range []string{"1970/01/01", "1970-1-1", "19700101"} {
		out, err := f2Execute(t, NewKernelFilter("d", OpEq, lit))
		if err != nil {
			t.Errorf("`d = '%s'` raised %v — a PostgreSQL-valid date must compare, not raise", lit, err)
			continue
		}
		if got := selOf(out); len(got) != 1 || got[0] != 0 {
			t.Errorf("`d = '%s'` Sel = %v, want [0] (the epoch row)", lit, got)
		}
	}
}

// f2Execute runs a kernel filter over a fresh single-epoch-row DATE batch.
func f2Execute(t *testing.T, f *KernelFilter) (*batch.RecordBatch, error) {
	t.Helper()
	return f.Execute(context.Background(), dateBatch(t))
}
