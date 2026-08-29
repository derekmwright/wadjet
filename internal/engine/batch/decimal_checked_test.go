package batch

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestParseDecimalStringCheckedReportsSaturation is #553 at its source.
//
// ParseDecimalString discards ScaledDecimal.Sat, so a value with no Int128 at
// the requested scale came back as the SATURATED end of the range — 10^30 at
// scale 10 became 17014118346046923173168730371.5884105727, which is 2^127-1
// rendered at scale 10. Saturation is right for the COMPARISON it was built
// for (#462: a literal outside the column's range is a bound and orders above
// every stored value) and is a lie as a stored VALUE. The checked sibling
// reports it, with PostgreSQL's SQLSTATE (ADR-0024 item 4).
func TestParseDecimalStringCheckedReportsSaturation(t *testing.T) {
	const e30 = "1000000000000000000000000000000" // 10^30

	// The defect, still reachable through the comparison-oriented function:
	// this is the number the single-process UNION used to answer.
	if got := ParseDecimalString(e30, 10).FormatDecimal(10); got != "17014118346046923173168730371.5884105727" {
		t.Fatalf("the saturating parser answered %s; this test is anchored on the value #553 reports", got)
	}

	for _, tc := range []struct {
		name  string
		text  string
		scale int
		state string
	}{
		// No Int128 at scale 10: 10^40 unscaled.
		{"past_the_carrier", e30, 10, "22003"},
		{"past_the_carrier_negative", "-" + e30, 10, "22003"},
		// 39 digits is the widest magnitude that can fit; 10^39 cannot.
		{"exactly_one_digit_too_wide", "1000000000000000000000000000000000000000", 0, "22003"},
		// Text that names no number is 22P02, never the value zero — reading
		// it as zero made an unreadable constant compare EQUAL to every
		// stored zero (#463).
		{"not_a_number", "abc", 2, "22P02"},
		{"empty", "", 2, "22P02"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDecimalStringChecked(tc.text, tc.scale)
			if err == nil {
				t.Fatalf("%q at scale %d was accepted; it has no exact DECIMAL value", tc.text, tc.scale)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Fatalf("SQLSTATE = %q, want %q; err = %v", got, tc.state, err)
			}
		})
	}

	// Everything inside the carrier still round-trips, and agrees with the
	// unchecked parser exactly.
	for _, tc := range []struct {
		text  string
		scale int
	}{
		{"12.75", 2}, {"12.75", 10}, {"-0.0001", 4}, {"0", 0},
		{"99999999999999999999999999999999999999", 0}, // 10^38 - 1
	} {
		got, err := ParseDecimalStringChecked(tc.text, tc.scale)
		if err != nil {
			t.Errorf("%q at scale %d: unexpected error %v", tc.text, tc.scale, err)
			continue
		}
		if want := ParseDecimalString(tc.text, tc.scale); got != want {
			t.Errorf("%q at scale %d: checked %s, unchecked %s", tc.text, tc.scale, got.String(), want.String())
		}
	}
}

// TestSetValueCheckedRefusesTheIngestReinterpretation pins the R3 decision:
// SetValue's integer arm stays the ALREADY-SCALED carrier hand-off ADR-0018 §4
// specifies for ingest, and the value-producing sibling refuses it, so no path
// other than ingest can reach it with an unscaled value.
//
// The two behaviours are asserted together deliberately — the ingest contract
// is what makes the refusal necessary, and a change to either one alone is
// what this test is here to catch.
func TestSetValueCheckedRefusesTheIngestReinterpretation(t *testing.T) {
	// The ingest contract: the box IS the unscaled carrier, written verbatim.
	// At scale 2 the int64 1 therefore means 0.01, and that is correct here.
	ingest := NewVectorWithScale(TypeDecimal, 1, 2)
	ingest.SetValue(0, int64(1))
	if got := ingest.GetValue(0); got != "0.01" {
		t.Fatalf("SetValue's integer arm must keep ADR-0018 §4's already-scaled carrier contract; got %v", got)
	}

	// The same box through the value-producing sibling is a reported defect,
	// not a hundredth: a set operation's integer arm is a VALUE at scale 0
	// (#547/#541), and the caller is expected to have multiplied by 10^s.
	v := NewVectorWithScale(TypeDecimal, 1, 2)
	for _, box := range []any{int64(1), int(1), int32(1)} {
		err := v.SetValueChecked(0, box)
		if err == nil {
			t.Fatalf("an integer box (%T) into a DECIMAL column must be reported, not read as an "+
				"unscaled carrier", box)
		}
		if got := sqlerr.StateOf(err); got != "22003" {
			t.Errorf("SQLSTATE = %q, want 22003; err = %v", got, err)
		}
	}

	// A carrier box IS the storage form and passes through both.
	if err := v.SetValueChecked(0, Int128From(175)); err != nil {
		t.Fatalf("an Int128 box is the storage form and must be accepted: %v", err)
	}
	if got := v.GetValue(0); got != "1.75" {
		t.Errorf("Int128 box: got %v, want 1.75", got)
	}
}

// TestFromRowsCheckedReportsWhatFromRowsSaturates is #553 at the boundary the
// single-process set operation actually crosses: the operation's result rows
// are boxed as text and rebuilt into a batch here, and the unchecked writer
// turned the out-of-range one into Int128Max with no error.
func TestFromRowsCheckedReportsWhatFromRowsSaturates(t *testing.T) {
	schema := []parquet.Column{{Name: "v", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true}}
	rows := []map[string]any{
		{"v": "12.7500000000"},
		{"v": "1000000000000000000000000000000"}, // 10^30, no Int128 at scale 10
	}

	// The defect: no error, and a value that is not the one in the row.
	b := FromRows(schema, rows)
	if got := b.Columns[0].GetValue(1); got != "17014118346046923173168730371.5884105727" {
		t.Fatalf("FromRows answered %v; this test is anchored on #553's reported value", got)
	}

	_, err := FromRowsChecked(schema, rows)
	if err == nil {
		t.Fatal("FromRowsChecked accepted a value with no carrier at the column's scale")
	}
	if got := sqlerr.StateOf(err); got != "22003" {
		t.Errorf("SQLSTATE = %q, want 22003; err = %v", got, err)
	}
	// The message has to say WHICH column and WHICH row, or the caller cannot
	// tell the client what failed.
	for _, want := range []string{`"v"`, "row 1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q; got %q", want, err)
		}
	}

	// Rows that fit are unaffected, and the checked path builds the same
	// batch the unchecked one does.
	ok := []map[string]any{{"v": "12.7500000000"}, {"v": "-0.0000000001"}, {"v": nil}}
	cb, err := FromRowsChecked(schema, ok)
	if err != nil {
		t.Fatalf("in-range rows: %v", err)
	}
	ub := FromRows(schema, ok)
	for i := range ok {
		if cb.Columns[0].Nulls.IsNull(i) != ub.Columns[0].Nulls.IsNull(i) ||
			cb.Columns[0].DecimalData.Data[i] != ub.Columns[0].DecimalData.Data[i] {
			t.Errorf("row %d: the checked and unchecked writers disagree", i)
		}
	}
}

// TestDecimalFitsPrecisionIsTheOneHelper pins the shared bound, including the
// R12 correction: a precision past the carrier's width is CHECKED against
// 10^38 rather than skipped. Skipping admitted exactly the values with no
// carrier — a DECIMAL(50,2) declaration cannot make an Int128 hold 10^50.
func TestDecimalFitsPrecisionIsTheOneHelper(t *testing.T) {
	tenPow38, ok := Int128From(1).MulPow10(38)
	if !ok {
		t.Fatal("10^38 must have an Int128")
	}
	for _, p := range []int{38, 39, 50, 1000} {
		limit, ok := DecimalPrecisionLimit(p)
		if !ok {
			t.Errorf("DecimalPrecisionLimit(%d) declined; past 38 it clamps to the carrier's width", p)
			continue
		}
		if limit != tenPow38 && p >= 38 {
			t.Errorf("DecimalPrecisionLimit(%d) = %s, want 10^38", p, limit.String())
		}
		if DecimalFitsPrecision(tenPow38, limit) {
			t.Errorf("precision %d admitted 10^38", p)
		}
	}
	// The negative end, including the value that is its own negation.
	limit, _ := DecimalPrecisionLimit(38)
	if DecimalFitsPrecision(Int128Min, limit) {
		t.Error("Int128Min has no magnitude an Int128 can hold and fits no declared precision")
	}
	if !DecimalFitsPrecision(Int128From(-1), limit) {
		t.Error("-1 fits DECIMAL(38)")
	}
	// No limit means no bound to apply (#458's unconstrained sentinel).
	if _, ok := DecimalPrecisionLimit(0); ok {
		t.Error("precision 0 declares no bound")
	}
	if !DecimalFitsPrecision(tenPow38, Int128{}) {
		t.Error("a zero limit admits every value")
	}
}
