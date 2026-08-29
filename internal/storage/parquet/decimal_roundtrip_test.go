package parquet

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Regression tests for the DECIMAL write path (issue #144 suite finding):
// toInt64's float64 branch truncated 3.25 → 3 instead of scaling to 325,
// destroying every non-integer decimal value at write time, and the read
// side dropped Precision/Scale when rebuilding the schema from the file's
// LogicalType — so even correctly-written values decoded at the wrong
// magnitude. TPC-H stores money as Float64, so no existing gate saw it.

func writeDecimalFile(tb testing.TB, scale int, vals []any) []byte {
	tb.Helper()
	schema := Schema{Columns: []Column{
		{Name: "d", Type: TypeDecimal, Nullable: true, Precision: 18, Scale: scale},
	}}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		tb.Fatal(err)
	}
	rows := make([]map[string]any, len(vals))
	for i, v := range vals {
		rows[i] = map[string]any{"d": v}
	}
	if err := w.WriteRows(rows); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

// TestDecimalValueFromBox pins ADR-0018 §4's writer corollary box by box: an
// INTEGER box is already the unscaled value at the column's scale, a REAL or
// numeric-STRING box carries the decimal point and is scaled here.
//
// The integer rows used to expect the scaled product (int64(7) at scale 2 →
// 700, i.e. "seven point zero zero"). That is the inverse of what the READER
// hands back for the same column, so read → write multiplied the column by
// 10^scale every pass (#429).
//
// The garbage and non-finite rows used to expect the stored value ZERO — a
// number nobody wrote, indistinguishable from a real one. They are errors now
// (ADR-0024 item 4, #647); TestDecimalBoxRefusals below is their half.
func TestDecimalValueFromBox(t *testing.T) {
	tests := []struct {
		val   any
		scale int
		want  int64
	}{
		// Real and string boxes carry the point: they are scaled.
		{3.25, 2, 325},
		{-1.5, 2, -150},
		{0.0, 2, 0},
		{"12.34", 2, 1234},
		{"-0.01", 2, -1},
		{"7", 2, 700},
		{2.675, 2, 268}, // half rounds away from zero (the shortest text is "2.675")
		{1.005e10, 2, 1005000000000},
		{99.999, 2, 10000}, // rounds up across the integer boundary
		{3.25, 0, 3},       // scale 0 keeps integer part, rounded
		{float32(1.5), 1, 15},
		// A float32 is spelled at ITS width: 0.1 widened to float64 is
		// exactly 0.10000000149011612, and reading that spelling at scale 10
		// would store 1000000015 instead of 1000000000.
		{float32(0.1), 10, 1000000000},
		{" 3.50 ", 2, 350},  // PostgreSQL strips C whitespace around numeric input
		{"-0.005", 2, -1},   // half away from zero, on the negative side
		{"0.004", 2, 0},     // below half a unit: zero, and no error
		{"0.0000001", 2, 0}, // more than a place below the scale
		{"1e3", 2, 100000},  // exponent form
		{"1.5E-2", 2, 2},    // 0.015 rounds away from zero
		{"5.", 2, 500},      // one empty part is still a number
		{".5", 2, 50},       //
		{"+12.34", 2, 1234}, // leading plus
		{"-000012.34", 2, -1234},
		// Integer boxes ARE the unscaled value: stored verbatim.
		{int64(7), 2, 7},
		{int(7), 2, 7},
		{int32(-3), 2, -3},
		{int64(325), 2, 325},
		{Decimal128From(325), 2, 325},
		{Decimal128From(-150), 2, -150},
		{int64(7), 0, 7},
	}
	for _, tc := range tests {
		got, err := DecimalValueFromBox(tc.val, 18, tc.scale)
		if err != nil {
			t.Errorf("DecimalValueFromBox(%v (%T), 18, %d): %v", tc.val, tc.val, tc.scale, err)
			continue
		}
		if got != Decimal128From(tc.want) {
			t.Errorf("DecimalValueFromBox(%v (%T), 18, %d) = %s, want %d",
				tc.val, tc.val, tc.scale, got, tc.want)
		}
	}
}

// The boxes that have NO value at the declared type. Every one of them used to
// be stored as a number: garbage and NaN as 0 through decimalUnscaledInt64's
// default arms, and anything past 2^63 as the int64 wrap of it (#647).
func TestDecimalBoxRefusals(t *testing.T) {
	tests := []struct {
		name      string
		val       any
		precision int
		scale     int
		state     string
	}{
		{"unparseable text", "garbage", 18, 2, "22P02"},
		{"empty text", "", 18, 2, "22P02"},
		{"text with an interior space", "3 .5", 18, 2, "22P02"},
		{"a no-break space is not C whitespace", " 3.5", 18, 2, "22P02"},
		{"NaN text", "NaN", 18, 2, "22003"},
		{"infinity text", "Infinity", 18, 2, "22003"},
		{"negative infinity text", "-inf", 18, 2, "22003"},
		{"NaN float", nan(), 18, 2, "22003"},
		{"+Inf float", inf(1), 18, 2, "22003"},
		{"-Inf float", inf(-1), 18, 2, "22003"},
		{"past the declared precision", "99999999999999999999.99", 9, 2, "22003"},
		{"exponent past the carrier", "1e40", 9, 2, "22003"},
		{"rounding into the overflow", "9999999.999", 9, 2, "22003"},
		{"unscaled integer box past the precision", int64(1_000_000_000), 9, 2, "22003"},
		{"past 38 digits at every precision", "1" + strings.Repeat("0", 38), 38, 0, "22003"},
		{"a box that is not a number at all", []byte{1}, 18, 2, "22P02"},
		{"a bool box", true, 18, 2, "22P02"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecimalValueFromBox(tc.val, tc.precision, tc.scale)
			if err == nil {
				t.Fatalf("DecimalValueFromBox(%v (%T), %d, %d) = no error, want %s",
					tc.val, tc.val, tc.precision, tc.scale, tc.state)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Fatalf("SQLSTATE %q, want %q (err: %v)", got, tc.state, err)
			}
		})
	}
}

func inf(sign int) float64 { return math.Inf(sign) }

func nan() float64 {
	f := 0.0
	return f / f
}

func TestDecimalSchemaRoundTrip(t *testing.T) {
	data := writeDecimalFile(t, 2, []any{3.25})
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	col := r.Schema().Columns[0]
	if col.Type != TypeDecimal {
		t.Fatalf("read type = %v, want DECIMAL", col.Type)
	}
	if col.Precision != 18 || col.Scale != 2 {
		t.Fatalf("read precision/scale = %d/%d, want 18/2 (lost → values decode at the wrong magnitude)",
			col.Precision, col.Scale)
	}
}

func TestDecimalValueRoundTrip(t *testing.T) {
	// ReadRows returns the RAW SCALED integer for DECIMAL columns (325 for
	// 3.25 at scale 2); the batch layer (Vector.SetValue Int128From) stores
	// it unscaled and FormatDecimal renders with the column scale. This
	// test pins the write-side scaling: before the fix the file held the
	// TRUNCATED integer part (3, not 325) for every non-integer input.
	// int64(700) rather than int64(7): an integer box is the UNSCALED
	// value, which is also what ReadRows hands back for it (#429).
	vals := []any{3.25, -1.5, 0.0, int64(700), "12.34", nil, 0.01, -9999999.99}
	want := []float64{325, -150, 0, 700, 1234, 0, 1, -999999999}
	isNull := []bool{false, false, false, false, false, true, false, false}
	data := writeDecimalFile(t, 2, vals)
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.ReadRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(vals) {
		t.Fatalf("rows = %d, want %d", len(rows), len(vals))
	}
	for i := range vals {
		got := rows[i]["d"]
		if isNull[i] {
			if got != nil {
				t.Errorf("row %d: got %v, want NULL", i, got)
			}
			continue
		}
		if got == nil {
			t.Errorf("row %d: got NULL, want %v", i, want[i])
			continue
		}
		f, ok := toFloat(got)
		if !ok {
			t.Errorf("row %d: unboxable decimal %#v (%T)", i, got, got)
			continue
		}
		if diff := f - want[i]; diff > 1e-9 || diff < -1e-9 {
			t.Errorf("row %d: got %v, want %v (write-side scaling or read-side scale lost)", i, f, want[i])
		}
	}
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int64:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	default:
		return 0, false
	}
}
