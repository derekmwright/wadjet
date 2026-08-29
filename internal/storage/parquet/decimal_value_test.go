package parquet

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The parquet-safety round trip for DECIMAL, at the boundaries of both
// encodings the writer emits: INT64 for a precision through 18, and
// FIXED_LEN_BYTE_ARRAY(16) past it (ADR-0018 §4's encoding corollary).
//
// Every case goes value -> write -> read and compares the UNSCALED integer,
// which is the only thing a DECIMAL leaf holds. The magnitudes are the ones a
// carrier gets wrong: the widest the precision allows, its negation, one past
// it, and zero from both signs.

// decimalWriteRead writes one row per value into a DECIMAL(p,s) column and
// reads the unscaled integers back through the row reader.
func decimalWriteRead(tb testing.TB, precision, scale int, vals []any) []*big.Int {
	tb.Helper()
	schema := Schema{Columns: []Column{
		{Name: "d", Type: TypeDecimal, Nullable: true, Precision: precision, Scale: scale},
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
		tb.Fatalf("DECIMAL(%d,%d) write: %v", precision, scale, err)
	}
	if err := w.Close(); err != nil {
		tb.Fatalf("DECIMAL(%d,%d) close: %v", precision, scale, err)
	}
	r, err := NewReaderFromBytes(buf.Bytes())
	if err != nil {
		tb.Fatalf("DECIMAL(%d,%d) open: %v", precision, scale, err)
	}
	got, err := r.ReadRows(nil)
	if err != nil {
		tb.Fatalf("DECIMAL(%d,%d) read: %v", precision, scale, err)
	}
	if len(got) != len(vals) {
		tb.Fatalf("DECIMAL(%d,%d): read %d rows, want %d", precision, scale, len(got), len(vals))
	}
	out := make([]*big.Int, len(got))
	for i, row := range got {
		switch v := row["d"].(type) {
		case nil:
			out[i] = nil
		case int64:
			out[i] = big.NewInt(v)
		case Decimal128:
			out[i], _ = new(big.Int).SetString(v.String(), 10)
		default:
			tb.Fatalf("DECIMAL(%d,%d) row %d: box %#v (%T) is not an unscaled integer",
				precision, scale, i, row["d"], row["d"])
		}
	}
	return out
}

// decimalUnscaledText renders the decimal TEXT whose unscaled value at `scale`
// is `mag` digits of nines, optionally negated — i.e. the widest value a
// DECIMAL(len(mag), scale) column can hold.
func decimalUnscaledText(digits string, scale int, neg bool) string {
	sign := ""
	if neg {
		sign = "-"
	}
	if scale == 0 {
		return sign + digits
	}
	if len(digits) <= scale {
		return sign + "0." + strings.Repeat("0", scale-len(digits)) + digits
	}
	return sign + digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
}

func TestDecimalRoundTripsAtTheEncodingBoundaries(t *testing.T) {
	// (precision, scale) pairs spanning both encodings and every scale the
	// task's boundary list names. 18 is the last INT64 precision, 19 the
	// first FLBA one, 38 the widest either can declare.
	for _, tc := range []struct{ p, s int }{
		{1, 0}, {9, 0}, {9, 2}, {9, 4}, {18, 0}, {18, 2}, {18, 4}, {18, 10},
		{19, 0}, {19, 4}, {19, 10}, {38, 0}, {38, 2}, {38, 4}, {38, 10}, {38, 38},
	} {
		t.Run(fmt.Sprintf("DECIMAL(%d,%d)", tc.p, tc.s), func(t *testing.T) {
			nines := strings.Repeat("9", tc.p)
			max, _ := new(big.Int).SetString(nines, 10)
			vals := []any{
				nil,                                     // NULL
				"0",                                     // zero
				"-0",                                    // negative zero is zero
				decimalUnscaledText("1", tc.s, false),   // one unit at the scale
				decimalUnscaledText("1", tc.s, true),    // and its negation
				decimalUnscaledText(nines, tc.s, false), // +(10^p - 1)
				decimalUnscaledText(nines, tc.s, true),  // -(10^p - 1)
			}
			want := []*big.Int{
				nil,
				big.NewInt(0),
				big.NewInt(0),
				big.NewInt(1),
				big.NewInt(-1),
				max,
				new(big.Int).Neg(max),
			}
			got := decimalWriteRead(t, tc.p, tc.s, vals)
			for i := range want {
				switch {
				case want[i] == nil && got[i] != nil:
					t.Errorf("row %d (%v): read back %s, want NULL", i, vals[i], got[i])
				case want[i] == nil:
				case got[i] == nil:
					t.Errorf("row %d (%v): read back NULL, want %s", i, vals[i], want[i])
				case got[i].Cmp(want[i]) != 0:
					t.Errorf("row %d (%v): unscaled %s, want %s", i, vals[i], got[i], want[i])
				}
			}

			// Exactly 10^p has no value at this precision, from either sign
			// and through the text box or the already-unscaled one.
			limit := decimalUnscaledText("1"+strings.Repeat("0", tc.p), tc.s, false)
			bads := []any{limit, "-" + limit}
			if tc.p <= 18 {
				lim, _ := new(big.Int).SetString("1"+strings.Repeat("0", tc.p), 10)
				bads = append(bads, Decimal128From(lim.Int64()), Decimal128From(-lim.Int64()))
			}
			for _, bad := range bads {
				if _, err := DecimalValueFromBox(bad, tc.p, tc.s); err == nil {
					t.Errorf("10^%d (%v) was accepted into DECIMAL(%d,%d)", tc.p, bad, tc.p, tc.s)
				} else if code := sqlerr.StateOf(err); code != "22003" {
					t.Errorf("10^%d: SQLSTATE %q, want 22003 (err: %v)", tc.p, code, err)
				}
			}
		})
	}
}

// A DECIMAL file with no rows at all still reads back as a DECIMAL column with
// its declared (p, s) — the empty case the parquet safety rules name, and the
// one where a writer that only annotates on the first value silently drops the
// type.
// The precision bound is 10^p exactly, at every p. It is built by a 128-bit
// multiply loop, and a single wrong entry admits values a column cannot hold
// (too high) or refuses ones it can (too low) — silently, in the first case.
func TestDecimalPow10Table(t *testing.T) {
	for i := 0; i <= MaxDecimalDigits; i++ {
		want := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(i)), nil)
		got := new(big.Int).Lsh(new(big.Int).SetUint64(decimalPow10[i][0]), 64)
		got.Or(got, new(big.Int).SetUint64(decimalPow10[i][1]))
		if got.Cmp(want) != 0 {
			t.Errorf("decimalPow10[%d] = %s, want %s", i, got, want)
		}
	}
}

func TestDecimalEmptyFileKeepsItsDeclaration(t *testing.T) {
	for _, tc := range []struct{ p, s int }{{9, 2}, {38, 10}} {
		schema := Schema{Columns: []Column{
			{Name: "d", Type: TypeDecimal, Nullable: true, Precision: tc.p, Scale: tc.s},
		}}
		var buf bytes.Buffer
		w, err := NewWriter(&buf, schema, DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		r, err := NewReaderFromBytes(buf.Bytes())
		if err != nil {
			t.Fatalf("DECIMAL(%d,%d) empty file: %v", tc.p, tc.s, err)
		}
		col := r.Schema().Columns[0]
		if col.Type != TypeDecimal || col.Precision != tc.p || col.Scale != tc.s {
			t.Errorf("empty DECIMAL(%d,%d) read back as %v(%d,%d)",
				tc.p, tc.s, col.Type, col.Precision, col.Scale)
		}
	}
}

// A DECIMAL column with no declared precision is 38 digits on BOTH halves of
// its definition — the physical type the writer picks and the annotation it
// writes — so the file it produces is one the format allows. It used to be an
// INT64 leaf annotated DECIMAL(38, s), which the Apache implementation refuses
// to open (R8/#647).
func TestUnconstrainedDecimalWritesAFileTheFormatAllows(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "d", Type: TypeDecimal, Nullable: true, Scale: 2},
	}}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	// A value only a 128-bit carrier holds: the old INT64 leaf could not.
	wide := "123456789012345678901234567890.12"
	if err := w.WriteRows([]map[string]any{{"d": wide}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	r, err := NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	col := r.Schema().Columns[0]
	if col.Precision != 38 || col.Scale != 2 {
		t.Fatalf("read back DECIMAL(%d,%d), want DECIMAL(38,2)", col.Precision, col.Scale)
	}
	if got := columnPhysical(Column{Type: TypeDecimal, Scale: 2}); got != PhysicalFixedLenByteArray {
		t.Fatalf("physical type %v, want FIXED_LEN_BYTE_ARRAY — an INT64 leaf cannot carry DECIMAL(38,2)", got)
	}
	rows, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	d, ok := rows[0]["d"].(Decimal128)
	if !ok {
		t.Fatalf("box %#v (%T), want Decimal128", rows[0]["d"], rows[0]["d"])
	}
	if d.String() != "12345678901234567890123456789012" {
		t.Fatalf("unscaled %s, want 12345678901234567890123456789012", d)
	}
}

// A FIXED_LEN_BYTE_ARRAY DECIMAL entry wider than the carrier is a read ERROR,
// not a truncation. Foreign files written at a precision past 38 carry them,
// and the decode loop shifted their leading bytes straight out of `hi` and
// answered a different number with err == nil (R7/#647).
func TestDecimalFromBytesRefusesAnEntryPastTheCarrier(t *testing.T) {
	for _, n := range []int{17, 20, 32} {
		b := make([]byte, n)
		for i := range b {
			b[i] = 0x7f
		}
		_, err := DecimalFromBytes(b)
		if err == nil {
			t.Errorf("a %d-byte DECIMAL entry was decoded", n)
			continue
		}
		// The refusal must name the width it REFUSED. Reading len(b) after
		// the narrowing shadowed it reported the nil the refusal returns, so
		// a 17-byte entry came back as "a DECIMAL entry of 0 bytes"
		// (#647 re-review).
		if want := fmt.Sprintf("of %d bytes", n); !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal of a %d-byte entry does not say so: %v", n, err)
		}
	}
	// A negative value whose excess is 0x00, and a positive one whose excess
	// is 0xFF: in both the excess is the sign extension of the OTHER sign, so
	// dropping it would change the number.
	for _, b := range [][]byte{
		append(bytes.Repeat([]byte{0x00}, 16), bytes.Repeat([]byte{0xff}, 16)...),
		append(bytes.Repeat([]byte{0xff}, 16), bytes.Repeat([]byte{0x00}, 16)...),
	} {
		if _, err := DecimalFromBytes(b); err == nil {
			t.Errorf("a %d-byte entry whose excess carries magnitude was decoded", len(b))
		}
	}

	// Excess that is PURE SIGN EXTENSION is narrowed and read: an FLBA's
	// width is fixed per COLUMN, so a foreign DECIMAL(20,4) leaf declared 32
	// bytes wide carries 32-byte entries whose values all fit sixteen.
	for _, tc := range []struct {
		b    []byte
		want string
	}{
		{append(bytes.Repeat([]byte{0x00}, 16), append(bytes.Repeat([]byte{0x00}, 15), 0x2a)...), "42"},
		{append(bytes.Repeat([]byte{0xff}, 16), append(bytes.Repeat([]byte{0xff}, 15), 0xd6)...), "-42"},
		{bytes.Repeat([]byte{0x00}, 32), "0"},
		{bytes.Repeat([]byte{0xff}, 32), "-1"},
		{append(bytes.Repeat([]byte{0x00}, 4), append([]byte{0x7f}, bytes.Repeat([]byte{0xff}, 15)...)...),
			"170141183460469231731687303715884105727"},
	} {
		w, err := DecimalFromBytes(tc.b)
		if err != nil {
			t.Errorf("a %d-byte entry that is sign extension over %s was refused: %v", len(tc.b), tc.want, err)
			continue
		}
		if d := (Decimal128{Hi: int64(w[0]), Lo: w[1]}); d.String() != tc.want {
			t.Errorf("%d-byte sign-extended entry decoded as %s, want %s", len(tc.b), d, tc.want)
		}
	}
	// Every width the carrier DOES hold still decodes, and a short entry is
	// sign-extended rather than refused.
	for _, tc := range []struct {
		b    []byte
		want string
	}{
		{[]byte{}, "0"},
		{[]byte{0x01}, "1"},
		{[]byte{0xff}, "-1"},
		{bytes.Repeat([]byte{0xff}, 16), "-1"},
		{append([]byte{0x7f}, bytes.Repeat([]byte{0xff}, 15)...), "170141183460469231731687303715884105727"},
	} {
		w, err := DecimalFromBytes(tc.b)
		if err != nil {
			t.Fatalf("%d-byte entry: %v", len(tc.b), err)
		}
		d := Decimal128{Hi: int64(w[0]), Lo: w[1]}
		if d.String() != tc.want {
			t.Errorf("%d-byte entry decoded as %s, want %s", len(tc.b), d, tc.want)
		}
	}
}

// The row reader names the same width the exported decoder does, and names
// the column and the entry with it.
func TestDecodeDecimalValuesNamesTheRefusedWidth(t *testing.T) {
	col := Column{Name: "d", Type: TypeDecimal, Precision: 38, Scale: 2}
	entry := make([]byte, 20)
	for i := range entry {
		entry[i] = 0x7f
	}
	data := ByteArrayValues(entry, []uint32{0, uint32(len(entry))})
	dst := make([]any, 1)
	err := decodeDecimalValues(dst, data, 1, col)
	if err == nil {
		t.Fatal("a 20-byte DECIMAL entry was decoded by the row path")
	}
	for _, want := range []string{`column "d"`, "entry 0", "of 20 bytes"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the row path's refusal does not contain %q: %v", want, err)
		}
	}
}

// FuzzDecimalValueFromText: the text a user or a foreign loader can hand a
// DECIMAL column. The properties are the ones a silent-wrong path breaks —
// never panic, never produce a value outside the declared precision, and agree
// with the exact big.Int arithmetic for the value it does produce.
func FuzzDecimalValueFromText(f *testing.F) {
	for _, s := range []string{
		"", "0", "-0", "12.34", " 3.50 ", "1e40", "abc", "NaN", "Infinity", "-inf",
		"9999999.99", "9999999.999", ".5", "5.", "+1", "-000012.34", "1.5E-2",
		"99999999999999999999999999999999999999", "1e-2147483648", "1e2147483647",
	} {
		f.Add(s, 9, 2)
	}
	f.Fuzz(func(t *testing.T, text string, precision, scale int) {
		precision = 1 + ((precision%MaxDecimalDigits)+MaxDecimalDigits)%MaxDecimalDigits
		scale = ((scale % (precision + 1)) + precision + 1) % (precision + 1)

		d, err := DecimalValueFromText(text, precision, scale)
		if err != nil {
			if code := sqlerr.StateOf(err); code != "22003" && code != "22P02" {
				t.Fatalf("%q at DECIMAL(%d,%d): SQLSTATE %q, want 22003 or 22P02",
					text, precision, scale, code)
			}
			return
		}
		// A value that came back must fit the precision it was declared at.
		mag, _ := new(big.Int).SetString(d.String(), 10)
		mag.Abs(mag)
		limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(precision)), nil)
		if mag.Cmp(limit) >= 0 {
			t.Fatalf("%q at DECIMAL(%d,%d) produced %s, which is not below 10^%d",
				text, precision, scale, d, precision)
		}
		// And it must be the exact half-away-from-zero rounding of the number
		// the text names, computed independently in big.Rat.
		neg, digits, exp, ok := DecimalTextParts(text)
		if !ok {
			t.Fatalf("%q produced a value but names no number", text)
		}
		want := decimalExpectedUnscaled(t, neg, digits, exp, scale)
		if want == nil {
			return // the exponent is past what big.Rat should be asked to build
		}
		if mag2, _ := new(big.Int).SetString(d.String(), 10); mag2.Cmp(want) != 0 {
			t.Fatalf("%q at DECIMAL(%d,%d) = %s, want %s", text, precision, scale, d, want)
		}
	})
}

// FuzzDecimalValueFromFloat pins the two instantiations of the resolver
// against each other: the float arm reads a []byte built in a stack buffer,
// the literal arm reads a string, and they are one generic function whose
// answers must not depend on which spelling reached it.
func FuzzDecimalValueFromFloat(f *testing.F) {
	for _, v := range []float64{0, 1, -1, 0.1, 3.25, 2.675, 1e14, 1e40, -1e40, 1.0 / 3.0} {
		f.Add(v, 18, 4)
	}
	f.Fuzz(func(t *testing.T, v float64, precision, scale int) {
		precision = 1 + ((precision%MaxDecimalDigits)+MaxDecimalDigits)%MaxDecimalDigits
		scale = ((scale % (precision + 1)) + precision + 1) % (precision + 1)

		got, gotErr := DecimalValueFromFloat(v, precision, scale)
		want, wantErr := DecimalValueFromText(strconv.FormatFloat(v, 'g', -1, 64), precision, scale)
		if (gotErr == nil) != (wantErr == nil) {
			t.Fatalf("%v at DECIMAL(%d,%d): float arm err=%v, text arm err=%v",
				v, precision, scale, gotErr, wantErr)
		}
		if gotErr == nil && got != want {
			t.Fatalf("%v at DECIMAL(%d,%d): float arm %s, text arm %s", v, precision, scale, got, want)
		}
	})
}

// decimalExpectedUnscaled recomputes digits x 10^(exp+scale), rounded half away
// from zero, in big.Int — the independent oracle for the fuzz property above.
// It returns nil when the exponent would build an absurd number, which the
// precision check has already refused by then.
func decimalExpectedUnscaled(tb testing.TB, neg bool, digits string, exp, scale int) *big.Int {
	tb.Helper()
	shift := exp + scale
	if shift > 64 || shift < -64-len(digits) {
		return nil
	}
	mag, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		tb.Fatalf("digit string %q is not an integer", digits)
	}
	if shift >= 0 {
		mag.Mul(mag, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(shift)), nil))
	} else {
		div := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-shift)), nil)
		q, r := new(big.Int).QuoRem(mag, div, new(big.Int))
		// Half away from zero: the remainder is non-negative here because the
		// magnitude is.
		if new(big.Int).Lsh(r, 1).Cmp(div) >= 0 {
			q.Add(q, big.NewInt(1))
		}
		mag = q
	}
	if neg {
		mag.Neg(mag)
	}
	return mag
}
