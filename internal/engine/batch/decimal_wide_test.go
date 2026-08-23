package batch

import (
	"math"
	"math/big"
	"math/rand"
	"strings"
	"testing"
)

// refFormatDecimal is the reference: exact big.Int division, not the digit
// slicing FormatDecimal does, so the two agree only if both are right.
func refFormatDecimal(t *testing.T, d Int128, scale int) string {
	t.Helper()
	b := d.BigInt()
	if scale <= 0 {
		return b.String()
	}
	div := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	q, r := new(big.Int).QuoRem(new(big.Int).Abs(b), div, new(big.Int))
	frac := r.String()
	frac = strings.Repeat("0", scale-len(frac)) + frac
	sign := ""
	if b.Sign() < 0 {
		sign = "-"
	}
	return sign + q.String() + "." + frac
}

// The values the issue pinned, three of them checked against pyarrow's own
// rendering of the same unscaled integers.
func TestFormatDecimalWideValues(t *testing.T) {
	for _, tc := range []struct {
		name  string
		d     Int128
		scale int
		want  string
	}{
		// 93468288258671214869 — needs 67 bits, so Hi is 5.
		{"hi_nonzero", Int128{Hi: 5, Lo: 0x112210f47de98115}, 10, "9346828825.8671214869"},
		{"hi_nonzero_neg", Int128{Hi: 5, Lo: 0x112210f47de98115}.Neg(), 10, "-9346828825.8671214869"},
		// Unscaled Int64Min: Hi is the sign extension, Lo has its top bit set.
		{"int64_min", Int128{Hi: -1, Lo: 0x8000000000000000}, 10, "-922337203.6854775808"},
		{"int64_min_scale0", Int128{Hi: -1, Lo: 0x8000000000000000}, 0, "-9223372036854775808"},
		// 12345678901234567890 — fits 64 bits UNSIGNED but not signed, which
		// is the case the old int64(abs.Lo) turned negative.
		{"lo_past_int64_max", Int128{Hi: 0, Lo: 0xab54a98ceb1f0ad2}, 4, "1234567890123456.7890"},
		{"lo_past_int64_max_scale0", Int128{Hi: 0, Lo: 0xab54a98ceb1f0ad2}, 0, "12345678901234567890"},
		// The Int128 extremes.
		{"max", Int128{Hi: math.MaxInt64, Lo: math.MaxUint64}, 0, "170141183460469231731687303715884105727"},
		{"min", Int128{Hi: math.MinInt64, Lo: 0}, 0, "-170141183460469231731687303715884105728"},
		{"max_scale38", Int128{Hi: math.MaxInt64, Lo: math.MaxUint64}, 38, "1.70141183460469231731687303715884105727"},
		{"min_scale38", Int128{Hi: math.MinInt64, Lo: 0}, 38, "-1.70141183460469231731687303715884105728"},
		// Narrow values: the shapes that already worked must keep working.
		{"simple", Int128From(325), 2, "3.25"},
		// The fraction is the DECLARED width, trailing zeros and all (#453):
		// 3.00 at scale 2 is "3.00", which is what PostgreSQL sends.
		{"trailing_zeros_are_the_scale", Int128From(300), 2, "3.00"},
		{"negative_simple", Int128From(-325), 2, "-3.25"},
		{"zero", Int128{}, 2, "0.00"},
		{"zero_scale0", Int128{}, 0, "0"},
		// Magnitude smaller than the scale: the leading zeros are the value.
		{"sub_one", Int128From(25), 2, "0.25"},
		{"sub_one_neg", Int128From(-25), 2, "-0.25"},
		{"deep_scale", Int128From(1), 10, "0.0000000001"},
		{"deep_scale_neg", Int128From(-1), 10, "-0.0000000001"},
		// Exact powers of ten, where a float divisor stops being exact: 10^23
		// has no float64.
		{"pow10_23", func() Int128 { v, _ := Int128From(1).MulPow10(23); return v }(), 23, "1.00000000000000000000000"},
		{"pow10_23_plus_one", func() Int128 {
			v, _ := Int128From(1).MulPow10(23)
			return v.Add(Int128From(1))
		}(), 23, "1.00000000000000000000001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.d.FormatDecimal(tc.scale); got != tc.want {
				t.Errorf("FormatDecimal(%d) = %q, want %q", tc.scale, got, tc.want)
			}
			if got, want := tc.d.FormatDecimal(tc.scale), refFormatDecimal(t, tc.d, tc.scale); got != want {
				t.Errorf("FormatDecimal(%d) = %q, big.Int reference says %q", tc.scale, got, want)
			}
		})
	}
}

// The property, over the whole 128-bit range: FormatDecimal agrees with exact
// big.Int division at every scale a DECIMAL column can declare.
func TestFormatDecimalMatchesBigIntEverywhere(t *testing.T) {
	rng := rand.New(rand.NewSource(20260823))
	vals := []Int128{
		{}, Int128From(1), Int128From(-1),
		{Hi: math.MaxInt64, Lo: math.MaxUint64},
		{Hi: math.MinInt64, Lo: 0},
		{Hi: 0, Lo: math.MaxUint64},
		{Hi: -1, Lo: 0},
		{Hi: 1, Lo: 0},
	}
	for i := 0; i < 400; i++ {
		vals = append(vals, Int128{Hi: rng.Int63() - (1 << 62), Lo: rng.Uint64()})
	}
	for _, d := range vals {
		for _, scale := range []int{0, 1, 2, 4, 10, 18, 19, 23, 38} {
			got := d.FormatDecimal(scale)
			if want := refFormatDecimal(t, d, scale); got != want {
				t.Fatalf("Int128{Hi:%d,Lo:%d}.FormatDecimal(%d) = %q, want %q", d.Hi, d.Lo, scale, got, want)
			}
		}
	}
}

// String is the scale-0 rendering on its own, and the one FormatDecimal
// delegates to. ToInt64 is deliberately NOT this and says so.
func TestInt128StringIsExactAtEveryWidth(t *testing.T) {
	for _, d := range []Int128{
		{}, Int128From(1), Int128From(-1), Int128From(math.MaxInt64), Int128From(math.MinInt64),
		{Hi: 0, Lo: math.MaxUint64}, {Hi: 5, Lo: 0x112210f47de98115},
		{Hi: math.MaxInt64, Lo: math.MaxUint64}, {Hi: math.MinInt64, Lo: 0},
	} {
		if got, want := d.String(), d.BigInt().String(); got != want {
			t.Errorf("Int128{Hi:%d,Lo:%d}.String() = %q, want %q", d.Hi, d.Lo, got, want)
		}
	}
}

// ToFloat64 was the other suspect. It reads Hi, and it is exact wherever a
// float64 can be: this pins that the sign extension arms and the wide arm all
// agree with big.Float.
func TestToFloat64MatchesBigFloat(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	vals := []Int128{
		{}, Int128From(1), Int128From(-1),
		{Hi: 0, Lo: math.MaxUint64}, {Hi: -1, Lo: 0}, {Hi: 1, Lo: 0},
		{Hi: 5, Lo: 0x112210f47de98115},
		{Hi: math.MaxInt64, Lo: math.MaxUint64}, {Hi: math.MinInt64, Lo: 0},
	}
	for i := 0; i < 200; i++ {
		vals = append(vals, Int128{Hi: rng.Int63() - (1 << 62), Lo: rng.Uint64()})
	}
	for _, d := range vals {
		for _, scale := range []int{0, 2, 10, 18} {
			got := d.ToFloat64(scale)
			ref := new(big.Float).SetPrec(200).SetInt(d.BigInt())
			div := new(big.Float).SetPrec(200).SetInt(new(big.Int).Exp(
				big.NewInt(10), big.NewInt(int64(scale)), nil))
			want, _ := new(big.Float).SetPrec(200).Quo(ref, div).Float64()
			// One ULP: the reference rounds once, ToFloat64's Hi*2^64 + Lo
			// arm rounds twice.
			if math.Abs(got-want) > math.Abs(want)*1e-15 {
				t.Errorf("Int128{Hi:%d,Lo:%d}.ToFloat64(%d) = %v, want %v",
					d.Hi, d.Lo, scale, got, want)
			}
		}
	}
}

// ParseDecimalString is FormatDecimal's inverse and was int64-wide too: a
// Sscanf whose error nobody read. Round-trip both directions.
func TestParseDecimalStringRoundTripsWideValues(t *testing.T) {
	for _, tc := range []struct {
		text  string
		scale int
		want  Int128
	}{
		{"9346828825.8671214869", 10, Int128{Hi: 5, Lo: 0x112210f47de98115}},
		{"-9346828825.8671214869", 10, Int128{Hi: 5, Lo: 0x112210f47de98115}.Neg()},
		{"12345678901234567890", 0, Int128{Hi: 0, Lo: 0xab54a98ceb1f0ad2}},
		{"1234567890123456.789", 4, Int128{Hi: 0, Lo: 0xab54a98ceb1f0ad2}},
		{"3.25", 2, Int128From(325)},
		{"-0.25", 2, Int128From(-25)},
		{"0", 2, Int128{}},
		{"1.70141183460469231731687303715884105727", 38,
			Int128{Hi: math.MaxInt64, Lo: math.MaxUint64}},
	} {
		got := ParseDecimalString(tc.text, tc.scale)
		if !got.Equal(tc.want) {
			t.Errorf("ParseDecimalString(%q, %d) = {Hi:%d,Lo:%d}, want {Hi:%d,Lo:%d}",
				tc.text, tc.scale, got.Hi, got.Lo, tc.want.Hi, tc.want.Lo)
		}
		if back := tc.want.FormatDecimal(tc.scale); ParseDecimalString(back, tc.scale) != tc.want {
			t.Errorf("round trip of {Hi:%d,Lo:%d} at scale %d through %q lost the value",
				tc.want.Hi, tc.want.Lo, tc.scale, back)
		}
	}
}

// The vector's boxed value is FormatDecimal's output, which is what makes
// this a wrong ANSWER and not only a wrong log line: GetValue is the row map,
// ToRows, the JSON encoder and the pgwire text protocol.
func TestVectorGetValueRendersWideDecimals(t *testing.T) {
	v := NewVector(TypeDecimal, 2)
	v.DecimalData = NewDecimalColumn(2, 10)
	v.DecimalData.Data[0] = Int128{Hi: 5, Lo: 0x112210f47de98115}
	v.DecimalData.Data[1] = Int128{Hi: -1, Lo: 0x8000000000000000}
	if got, want := v.GetValue(0), "9346828825.8671214869"; got != want {
		t.Errorf("GetValue(0) = %v, want %q", got, want)
	}
	if got, want := v.GetValue(1), "-922337203.6854775808"; got != want {
		t.Errorf("GetValue(1) = %v, want %q", got, want)
	}
}
