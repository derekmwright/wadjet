package parquet

import (
	"bytes"
	"strconv"
	"testing"
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

func TestDecimalScaledInt64(t *testing.T) {
	tests := []struct {
		val   any
		scale int
		want  int64
	}{
		{3.25, 2, 325},
		{-1.5, 2, -150},
		{0.0, 2, 0},
		{int64(7), 2, 700},
		{int(7), 2, 700},
		{int32(-3), 2, -300},
		{"12.34", 2, 1234},
		{"-0.01", 2, -1},
		{"7", 2, 700},
		{2.675, 2, 268}, // half rounds away from zero (float repr permitting)
		{1.005e10, 2, 1005000000000},
		{99.999, 2, 10000}, // rounds up across the integer boundary
		{3.25, 0, 3},       // scale 0 keeps integer part, rounded
		{"garbage", 2, 0},  // unparseable stores 0 (matches toInt64 default)
		{float32(1.5), 1, 15},
	}
	for _, tc := range tests {
		if got := decimalScaledInt64(tc.val, tc.scale); got != tc.want {
			t.Errorf("decimalScaledInt64(%v, %d) = %d, want %d", tc.val, tc.scale, got, tc.want)
		}
	}
	// Non-finite floats must not poison the column.
	if got := decimalScaledInt64(nan(), 2); got != 0 {
		t.Errorf("NaN = %d, want 0", got)
	}
}

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
	vals := []any{3.25, -1.5, 0.0, int64(7), "12.34", nil, 0.01, -9999999.99}
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
