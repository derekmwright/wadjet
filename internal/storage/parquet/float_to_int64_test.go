// SPDX-License-Identifier: MIT

package parquet

import (
	"math"
	"testing"
)

// FloatToInt64 is the one range check every float → integer conversion makes
// (#1484). Its bound is half-open at 2^63 and closed at -2^63: the double
// 2^63 has no int64 (float64(math.MaxInt64) rounds UP to it, which is how a
// copy written `f > math.MaxInt64` let it through and stored MinInt64), the
// largest double below it does, and -2^63 is exact.
func TestFloatToInt64Bound(t *testing.T) {
	below63 := math.Nextafter(0x1p63, 0)              // 2^63 - 1024
	below63n := math.Nextafter(-0x1p63, math.Inf(-1)) // -2^63 - 2048
	cases := []struct {
		f    float64
		want int64
		ok   bool
	}{
		{0x1p63, 0, false},
		{below63, 9223372036854774784, true},
		{-0x1p63, math.MinInt64, true},
		{below63n, 0, false},
		{float64(math.MaxInt64), 0, false}, // IS 2^63
		{float64(math.MinInt64), math.MinInt64, true},
		{math.NaN(), 0, false},
		{math.Inf(1), 0, false},
		{math.Inf(-1), 0, false},
		{1e300, 0, false},
		{-1e300, 0, false},
		{2147483648, 2147483648, true},
		{-2147483649, -2147483649, true},
		{math.Copysign(0, -1), 0, true},
		{0, 0, true},
		{2.5, 2, true}, // the integer part: a caller rounds first
		{-2.5, -2, true},
		{0x1p52 + 1, 4503599627370497, true},
	}
	for _, c := range cases {
		got, ok := FloatToInt64(c.f)
		if ok != c.ok || got != c.want {
			t.Errorf("FloatToInt64(%v) = %d, %v; want %d, %v", c.f, got, ok, c.want, c.ok)
		}
	}
	// Every double in the top and bottom 64 steps of the range converts
	// exactly or is refused — never a wrapped int64 of the wrong sign.
	for _, start := range []float64{0x1p63, -0x1p63} {
		f := start
		for i := 0; i < 64; i++ {
			f = math.Nextafter(f, 0)
			n, ok := FloatToInt64(f)
			if !ok || float64(n) != f {
				t.Fatalf("FloatToInt64(%v) = %d, %v: an in-range double did not convert exactly", f, n, ok)
			}
		}
		f = start
		for i := 0; i < 64; i++ {
			if start < 0 {
				f = math.Nextafter(f, math.Inf(-1))
			}
			if _, ok := FloatToInt64(f); ok != (f == -0x1p63) {
				t.Fatalf("FloatToInt64(%v) ok=%v outside the range", f, ok)
			}
			if start > 0 {
				f = math.Nextafter(f, math.Inf(1))
			}
		}
	}
}

// The writer's INT64 leaf takes the same bound: 2^63 is a range error and
// -2^63 is stored.
func TestFloatToInt64LeafSharesTheBound(t *testing.T) {
	if _, err := floatToInt64Leaf(TypeInt64, 0x1p63, 0x1p63); err == nil {
		t.Fatal("the INT64 leaf stored the double 2^63")
	}
	if n, err := floatToInt64Leaf(TypeInt64, -0x1p63, -0x1p63); err != nil || n != math.MinInt64 {
		t.Fatalf("the INT64 leaf refused -2^63: %d, %v", n, err)
	}
}
