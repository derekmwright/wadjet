package pgwire

import (
	"math/big"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

// TestAppendBinaryNumericRoundTripsThroughPgx is the gate on declaring
// DECIMAL as OID 1700.
//
// Under OID 25 the generic string arm was RIGHT: the binary form of a text
// column is its bytes. Declaring numeric makes those same bytes a base-10000
// digit vector, so the OID change is only correct alongside an encoder — and
// the encoder is only correct if a real client decodes it back to the value.
// pgx's own numeric codec is that client, so this is a differential, not a
// restatement of the format from the same head that wrote it.
func TestAppendBinaryNumericRoundTripsThroughPgx(t *testing.T) {
	m := pgtype.NewMap()

	for _, s := range []string{
		"0.0",
		"0.00000",
		"1.0",
		"-1.0",
		"25.0",
		"-25.0",
		"0.5",
		"-0.5",
		"12.75",
		"-20.0",
		"3.1875",
		"0.0001",
		"-0.0001",
		"1234.5678",
		"12345.6",
		"99999999.99",
		// The reason a DECIMAL column exists: values past float64's exact
		// range and past int64 entirely.
		"9346828825.8671214869",
		"-9346828825.8671214869",
		"9999999999999999999999999999.9999999999",
		"-9999999999999999999999999999.9999999999",
		"12345678901234567890.1234567890",
		// No fraction at all (FormatDecimal's scale <= 0 branch).
		"42",
		"-42",
		"0",
	} {
		t.Run(s, func(t *testing.T) {
			buf := appendBinaryNumeric(nil, s)
			if len(buf) < 4 {
				t.Fatalf("encoded to %d bytes", len(buf))
			}
			// Strip the 4-byte length prefix the wire framing carries.
			n := int32(uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3]))
			if n < 0 {
				t.Fatalf("%q encoded as NULL", s)
			}
			body := buf[4:]
			if int(n) != len(body) {
				t.Fatalf("declared length %d, body %d bytes", n, len(body))
			}

			var got pgtype.Numeric
			if err := m.Scan(pgtype.NumericOID, 1 /* binary */, body, &got); err != nil {
				t.Fatalf("pgx cannot decode our binary numeric: %v", err)
			}
			if !got.Valid {
				t.Fatal("decoded to an invalid numeric")
			}

			want, ok := new(big.Rat).SetString(s)
			if !ok {
				t.Fatalf("test input %q is not a decimal", s)
			}
			// Compare as exact rationals: dscale differs from PostgreSQL's
			// (wadjet trims trailing zeros in the text form), and that is a
			// rendering difference, not a value one.
			gotRat := new(big.Rat).SetInt(got.Int)
			switch {
			case got.Exp > 0:
				gotRat.Mul(gotRat, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(got.Exp)), nil)))
			case got.Exp < 0:
				gotRat.Quo(gotRat, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-got.Exp)), nil)))
			}
			if gotRat.Cmp(want) != 0 {
				t.Fatalf("round trip: %q -> %s (Int=%s Exp=%d), want %s",
					s, gotRat.FloatString(12), got.Int, got.Exp, want.FloatString(12))
			}
		})
	}
}

// TestAppendBinaryNumericRefusesNonNumbers: the field is variable-length, so
// unlike `date` and `timestamp` there is no width check that would catch text
// written into it. A value that is not a decimal is an absence, never bytes.
func TestAppendBinaryNumericRefusesNonNumbers(t *testing.T) {
	for _, s := range []string{"", "abc", "1.2.3", "1,5", "NaN", "1e5", "  1.0", "1.0 ", "-", "."} {
		t.Run(s, func(t *testing.T) {
			buf := appendBinaryNumeric(nil, s)
			if len(buf) != 4 {
				t.Fatalf("%q encoded to %d bytes, want a 4-byte NULL", s, len(buf))
			}
			n := int32(uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3]))
			if n != -1 {
				t.Fatalf("%q encoded with length %d, want -1 (NULL)", s, n)
			}
		})
	}
}

// TestPgNumericDigitsHeader pins the header arithmetic directly, because a
// round trip through one decoder can agree on a value while both sides
// misread the weight of a number whose digit groups happen to be symmetric.
func TestPgNumericDigitsHeader(t *testing.T) {
	for _, tc := range []struct {
		in                   string
		digits               []int16
		weight, sign, dscale int16
	}{
		{"0.0", nil, 0, pgNumericPos, 1},
		{"25.0", []int16{25}, 0, pgNumericPos, 1},
		{"-25.0", []int16{25}, 0, pgNumericNeg, 1},
		{"0.5", []int16{5000}, -1, pgNumericPos, 1},
		{"1234.5678", []int16{1234, 5678}, 0, pgNumericPos, 4},
		{"12345.6", []int16{1, 2345, 6000}, 1, pgNumericPos, 1},
		{"9346828825.8671214869", []int16{93, 4682, 8825, 8671, 2148, 6900}, 2, pgNumericPos, 10},
	} {
		t.Run(tc.in, func(t *testing.T) {
			digits, weight, sign, dscale, ok := pgNumericDigits(tc.in)
			if !ok {
				t.Fatal("refused a decimal")
			}
			if weight != tc.weight || sign != tc.sign || dscale != tc.dscale {
				t.Errorf("header = weight %d sign %#04x dscale %d, want weight %d sign %#04x dscale %d",
					weight, sign, dscale, tc.weight, tc.sign, tc.dscale)
			}
			if len(digits) != len(tc.digits) {
				t.Fatalf("digits = %v, want %v", digits, tc.digits)
			}
			for i := range digits {
				if digits[i] != tc.digits[i] {
					t.Fatalf("digits = %v, want %v", digits, tc.digits)
				}
			}
		})
	}
}
