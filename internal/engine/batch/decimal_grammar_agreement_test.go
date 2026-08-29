package batch

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ONE accept-set for DECIMAL text, across the comparison reader here and the
// file writer's value reader in internal/storage/parquet.
//
// The two used to be different functions: this package parsed the text exactly
// (DecimalTextAt) while the writer ran strconv.ParseFloat and stored 0 for
// everything it refused, so `WHERE d = 'abc'` was 22P02 and `INSERT 'abc'` was
// the value zero (#647). They are one function now — the grammar lives in the
// lower package and this one reads through it — and this test is what keeps
// that true if either side is ever re-implemented.
//
// It lives here rather than in parquet because only this direction of the
// import can see both: batch imports parquet.
func TestDecimalGrammarMatchesBatch(t *testing.T) {
	if MaxDecimalPrecision != parquet.MaxDecimalDigits {
		t.Fatalf("MaxDecimalPrecision = %d but parquet.MaxDecimalDigits = %d: the carrier's width "+
			"has to be one number, or the writer and the engine bound values differently",
			MaxDecimalPrecision, parquet.MaxDecimalDigits)
	}

	texts := []string{
		"", " ", "0", "-0", "+0", "12.34", "-12.34", " 3.50 ", "\t3.50\n",
		" 3.5", "3 .5", "abc", "1e40", "1e-40", "1E2", ".5", "5.", "..", "1.2.3",
		"NaN", "nan", "+NaN", "-NaN", "Infinity", "inf", "-inf", "+INF", "Infin", "infinit",
		"9999999.99", "9999999.999", "-0.005",
		"99999999999999999999999999999999999999",
		"999999999999999999999999999999999999999",
		"170141183460469231731687303715884105727",
		"170141183460469231731687303715884105728",
	}
	for _, text := range texts {
		for _, scale := range []int{0, 2, 4, 10, 38} {
			t.Run(text+"@"+itoa(scale), func(t *testing.T) {
				// 1. The three-way classification agrees.
				special := DecimalSpecialText(text) != DecimalFinite
				if got := parquet.DecimalSpecialText(text) != parquet.DecimalFinite; got != special {
					t.Fatalf("special: batch says %v, parquet says %v", special, got)
				}

				_, ok := DecimalTextAt(text, scale)
				_, err := parquet.DecimalValueFromText(text, MaxDecimalPrecision, scale)
				code := sqlerr.StateOf(err)

				switch {
				case special:
					// A NaN or an infinity names a value neither carrier has:
					// 22003 on both sides, never 22P02 (ADR-0024 item 6).
					if DecimalSpecialValueError(text) == nil || code != "22003" {
						t.Fatalf("special %q: batch err %v, parquet SQLSTATE %q, want 22003 on both",
							text, DecimalSpecialValueError(text), code)
					}
				case !ok:
					// Not a number, on both sides.
					if code != "22P02" {
						t.Fatalf("%q names no number here but parquet answered SQLSTATE %q (%v)",
							text, code, err)
					}
				case code == "22P02":
					t.Fatalf("%q is a number here but parquet called it invalid syntax: %v", text, err)
				}
			})
		}
	}
}

// The two readers must also agree on the VALUE, wherever the comparison reader
// resolved one exactly: a literal that needs no rounding and fits the carrier
// is the same unscaled integer on both paths, or a stored row and a predicate
// over it are talking about different numbers.
func TestDecimalValueMatchesBatchWhereExact(t *testing.T) {
	for _, text := range []string{
		"0", "-0", "1", "-1", "12.34", "-12.34", " 3.50 ", "1e3", "1.5E-2",
		"9999999.99", "-9999999.99", "0.0001",
		"12345678901234567890.1234567891",
		"99999999999999999999999999999999999999",
	} {
		for _, scale := range []int{0, 2, 4, 10} {
			d, ok := DecimalTextAt(text, scale)
			if !ok || d.Sat != 0 || d.Residual != 0 {
				continue // saturated or finer than the scale: the writer rounds, this reader does not
			}
			got, err := parquet.DecimalValueFromText(text, MaxDecimalPrecision, scale)
			if err != nil {
				t.Errorf("%q at scale %d: batch resolved %s exactly, parquet refused: %v",
					text, scale, d.Unscaled, err)
				continue
			}
			if got.Hi != d.Unscaled.Hi || got.Lo != d.Unscaled.Lo {
				t.Errorf("%q at scale %d: batch %s, parquet %s", text, scale, d.Unscaled, got)
			}
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
