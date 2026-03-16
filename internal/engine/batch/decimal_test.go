package batch

import (
	"math"
	"testing"
)

func TestInt128From(t *testing.T) {
	tests := []struct {
		input    int64
		wantHi   int64
		wantLo   uint64
	}{
		{0, 0, 0},
		{1, 0, 1},
		{-1, -1, math.MaxUint64},
		{12345, 0, 12345},
		{-12345, -1, ^uint64(12344)},
		{math.MaxInt64, 0, uint64(math.MaxInt64)},
	}

	for _, tt := range tests {
		d := Int128From(tt.input)
		if d.Hi != tt.wantHi || d.Lo != tt.wantLo {
			t.Errorf("Int128From(%d) = {%d, %d}, want {%d, %d}", tt.input, d.Hi, d.Lo, tt.wantHi, tt.wantLo)
		}
	}
}

func TestInt128Arithmetic(t *testing.T) {
	a := Int128From(100)
	b := Int128From(50)

	sum := a.Add(b)
	if sum.ToInt64() != 150 {
		t.Errorf("100 + 50 = %d, want 150", sum.ToInt64())
	}

	diff := a.Sub(b)
	if diff.ToInt64() != 50 {
		t.Errorf("100 - 50 = %d, want 50", diff.ToInt64())
	}

	neg := a.Neg()
	if neg.ToInt64() != -100 {
		t.Errorf("-100 = %d, want -100", neg.ToInt64())
	}
}

func TestInt128Comparison(t *testing.T) {
	a := Int128From(100)
	b := Int128From(200)
	c := Int128From(100)
	neg := Int128From(-50)

	if !a.Less(b) {
		t.Error("100 should be < 200")
	}
	if b.Less(a) {
		t.Error("200 should not be < 100")
	}
	if !a.Equal(c) {
		t.Error("100 should equal 100")
	}
	if a.Equal(b) {
		t.Error("100 should not equal 200")
	}
	if !neg.Less(a) {
		t.Error("-50 should be < 100")
	}
	if !neg.IsNegative() {
		t.Error("-50 should be negative")
	}
	if a.IsNegative() {
		t.Error("100 should not be negative")
	}
}

func TestInt128FromFloat64(t *testing.T) {
	// DECIMAL(10,2): 123.45 → 12345
	d := Int128FromFloat64(123.45, 2)
	if d.ToInt64() != 12345 {
		t.Errorf("Int128FromFloat64(123.45, 2) = %d, want 12345", d.ToInt64())
	}

	// Negative
	d = Int128FromFloat64(-99.99, 2)
	if d.ToInt64() != -9999 {
		t.Errorf("Int128FromFloat64(-99.99, 2) = %d, want -9999", d.ToInt64())
	}

	// Zero scale
	d = Int128FromFloat64(42.7, 0)
	if d.ToInt64() != 43 {
		t.Errorf("Int128FromFloat64(42.7, 0) = %d, want 43", d.ToInt64())
	}
}

func TestInt128ToFloat64(t *testing.T) {
	d := Int128From(12345)
	f := d.ToFloat64(2)
	if math.Abs(f-123.45) > 0.001 {
		t.Errorf("Int128(12345).ToFloat64(2) = %f, want 123.45", f)
	}

	d = Int128From(-9999)
	f = d.ToFloat64(2)
	if math.Abs(f-(-99.99)) > 0.001 {
		t.Errorf("Int128(-9999).ToFloat64(2) = %f, want -99.99", f)
	}
}

func TestFormatDecimal(t *testing.T) {
	tests := []struct {
		value Int128
		scale int
		want  string
	}{
		{Int128From(12345), 2, "123.45"},
		{Int128From(100), 2, "1.0"},
		{Int128From(0), 2, "0.0"},
		{Int128From(12345), 0, "12345"},
		{Int128From(-9999), 2, "-99.99"},
		{Int128From(1000000), 4, "100.0"},
		{Int128From(123456789), 6, "123.456789"},
	}

	for _, tt := range tests {
		got := tt.value.FormatDecimal(tt.scale)
		if got != tt.want {
			t.Errorf("FormatDecimal(%d, %d) = %q, want %q", tt.value.ToInt64(), tt.scale, got, tt.want)
		}
	}
}

func TestParseDecimalString(t *testing.T) {
	tests := []struct {
		input string
		scale int
		want  int64
	}{
		{"123.45", 2, 12345},
		{"-99.99", 2, -9999},
		{"100", 2, 10000},
		{"0.5", 2, 50},
		{"0.005", 3, 5},
		{"  42.0  ", 1, 420},
	}

	for _, tt := range tests {
		d := ParseDecimalString(tt.input, tt.scale)
		if d.ToInt64() != tt.want {
			t.Errorf("ParseDecimalString(%q, %d) = %d, want %d", tt.input, tt.scale, d.ToInt64(), tt.want)
		}
	}
}

func TestDecimalVector(t *testing.T) {
	// Create a DECIMAL(10,2) column
	v := NewVectorWithScale(TypeDecimal, 5, 2)

	// Set values
	v.SetValue(0, float64(123.45))
	v.SetValue(1, float64(-99.99))
	v.SetValue(2, Int128From(50000)) // raw unscaled: 500.00
	v.SetValue(3, nil)               // null
	v.SetValue(4, "42.10")

	// Verify
	if got := v.GetValue(0); got != "123.45" {
		t.Errorf("row 0 = %v, want 123.45", got)
	}
	if got := v.GetValue(1); got != "-99.99" {
		t.Errorf("row 1 = %v, want -99.99", got)
	}
	if got := v.GetValue(2); got != "500.0" {
		t.Errorf("row 2 = %v, want 500.0", got)
	}
	if got := v.GetValue(3); got != nil {
		t.Errorf("row 3 = %v, want nil", got)
	}
	if got := v.GetValue(4); got != "42.1" {
		t.Errorf("row 4 = %v, want 42.1", got)
	}

	// GetNumericFloat64
	f, ok := v.GetNumericFloat64(0)
	if !ok || math.Abs(f-123.45) > 0.001 {
		t.Errorf("GetNumericFloat64(0) = %f, %v", f, ok)
	}
	_, ok = v.GetNumericFloat64(3)
	if ok {
		t.Error("GetNumericFloat64(3) should return false for null")
	}
}
