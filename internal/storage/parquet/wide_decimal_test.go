package parquet

import (
	"bytes"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A DECIMAL wider than 64 bits on the ROW path (#419).
//
// decimalBytesToInt64 accumulated all sixteen bytes of a DECIMAL(38,10) into
// one int64, shifting the top eight straight out of the register: the result
// was the low 64 bits of the unscaled value reinterpreted as signed, returned
// with no error for every value past 2^63. The native scan decoded the same
// bytes into a 128-bit value and was right, and which path a query takes is
// decided by the SHAPE of the table's schema (one ARRAY/ROW/MAP column
// anywhere in the read set sends the whole read to the row reader, #393) —
// so the same column answered differently depending on what OTHER column the
// table happened to carry.
//
// The fixtures are PyArrow's (gen_decimal_physicals.py,
// gen_wide_decimal_nested.py) and both carry the exact unscaled integer as a
// STRING alongside the decimal, so the assertion is against the reference
// writer's own encoding rather than against a float recomputed here.

// wantUnscaled compares a decoded DECIMAL box against the decimal integer
// text the fixture carries beside it.
func wantUnscaled(t *testing.T, label string, got any, want string) {
	t.Helper()
	var have *big.Int
	switch v := got.(type) {
	case Decimal128:
		have, _ = new(big.Int).SetString(v.String(), 10)
	case int64:
		have = big.NewInt(v)
	case nil:
		t.Errorf("%s: read back NULL, want %s", label, want)
		return
	default:
		t.Errorf("%s: read back %#v (%T), want the unscaled integer %s", label, got, got, want)
		return
	}
	exp, ok := new(big.Int).SetString(want, 10)
	if !ok {
		t.Fatalf("%s: fixture's expected unscaled value %q is not an integer", label, want)
	}
	if have.Cmp(exp) != 0 {
		t.Errorf("%s: unscaled value %s, want %s", label, have, exp)
	}
}

// TestWideDecimalRowPathFlat: the row reader over a DECIMAL(38,10) column in
// a flat schema. decimal_physicals.parquet's d38_10 holds the widest and
// narrowest values the precision can express.
func TestWideDecimalRowPathFlat(t *testing.T) {
	data, err := os.ReadFile("testdata/decimal_physicals.parquet")
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.ReadRows([]string{"label", "d38_10", "unscaled_38_10"})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("fixture read back empty")
	}
	sawWide := false
	for _, row := range rows {
		label, _ := row["label"].(string)
		want, present := row["unscaled_38_10"].(string)
		if !present {
			if row["d38_10"] != nil {
				t.Errorf("%s: NULL decimal read back as %#v", label, row["d38_10"])
			}
			continue
		}
		wantUnscaled(t, label, row["d38_10"], want)
		if len(strings.TrimPrefix(want, "-")) > 19 {
			sawWide = true
		}
	}
	if !sawWide {
		t.Fatal("fixture carries no value past 64 bits — it cannot gate this")
	}
}

// TestWideDecimalRowPathNested: the same column beside a nested one, which is
// the schema shape that forces the row path for every query on the table, and
// the same values one container deep, which reaches the nested assembler's
// own leaf decode.
func TestWideDecimalRowPathNested(t *testing.T) {
	data, err := os.ReadFile("testdata/wide_decimal_nested.parquet")
	if err != nil {
		t.Fatalf("fixture: %v (regenerate with gen_wide_decimal_nested.py)", err)
	}
	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	var dCol Column
	for _, c := range r.Schema().Columns {
		if c.Name == "d" {
			dCol = c
		}
	}
	if dCol.Type != TypeDecimal || dCol.Precision != 38 || dCol.Scale != 10 {
		t.Fatalf("column d recovered as %v(%d,%d), want DECIMAL(38,10)",
			dCol.Type, dCol.Precision, dCol.Scale)
	}
	rows, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if len(rows) != 6 {
		t.Fatalf("read %d rows, want 6", len(rows))
	}
	for i, row := range rows {
		want, present := row["unscaled"].(string)
		if !present {
			if row["d"] != nil {
				t.Errorf("row %d: NULL decimal read back as %#v", i, row["d"])
			}
			if row["d_nested"] != nil {
				t.Errorf("row %d: NULL list read back as %#v", i, row["d_nested"])
			}
			continue
		}
		wantUnscaled(t, "row "+string(rune('0'+i))+" d", row["d"], want)

		// The same value inside a LIST goes through the nested assembler's
		// leaf decode, which had no DECIMAL arm at all and answered nil.
		nested, ok := row["d_nested"].([]any)
		if !ok || len(nested) != 1 {
			t.Errorf("row %d: d_nested = %#v, want a one-element list", i, row["d_nested"])
			continue
		}
		wantUnscaled(t, "row "+string(rune('0'+i))+" d_nested[0]", nested[0], want)
	}
}

// TestWideDecimalIsBoxedByPrecision pins the rule: the box is decided by the
// column's DECLARED precision, not by the value, so a column's shape does not
// change from row to row.
func TestWideDecimalIsBoxedByPrecision(t *testing.T) {
	for _, tc := range []struct {
		precision int
		want128   bool
		// Precision 0 is the "unconstrained" sentinel and reads as 38 on both
		// halves of the column's definition — the physical type the writer
		// picks and the annotation it writes — so it takes the wide box too.
	}{{9, false}, {18, false}, {19, true}, {38, true}, {0, true}, {50, true}} {
		if got := decimalNeeds128(Column{Type: TypeDecimal, Precision: tc.precision}); got != tc.want128 {
			t.Errorf("precision %d: needs Decimal128 = %v, want %v", tc.precision, got, tc.want128)
		}
	}
}

func TestDecimal128Int64(t *testing.T) {
	for _, tc := range []struct {
		d    Decimal128
		want int64
		fits bool
	}{
		{Decimal128From(0), 0, true},
		{Decimal128From(1), 1, true},
		{Decimal128From(-1), -1, true},
		{Decimal128From(1<<63 - 1), 1<<63 - 1, true},
		{Decimal128From(-1 << 63), -1 << 63, true},
		{Decimal128{Hi: 0, Lo: 1 << 63}, 0, false},     // 2^63, one past int64
		{Decimal128{Hi: -1, Lo: 1<<63 - 1}, 0, false},  // -2^63-1
		{Decimal128{Hi: 1, Lo: 0}, 0, false},           // 2^64
		{Decimal128{Hi: -2, Lo: ^uint64(0)}, 0, false}, // -2^64-1
	} {
		got, fits := tc.d.Int64()
		if fits != tc.fits || (fits && got != tc.want) {
			t.Errorf("%s.Int64() = (%d, %v), want (%d, %v)", tc.d, got, fits, tc.want, tc.fits)
		}
	}
}

// TestDecimal128String is the rendering the error messages and the test
// comparisons above rely on, checked at the two's-complement edges where a
// sign-magnitude big.Int is easiest to get wrong.
func TestDecimal128String(t *testing.T) {
	for _, tc := range []struct {
		d    Decimal128
		want string
	}{
		{Decimal128{}, "0"},
		{Decimal128From(1), "1"},
		{Decimal128From(-1), "-1"},
		{Decimal128{Hi: 5421010862427522170, Lo: 0x98a223fffffffff}, "99999999999999999999999999999999999999"},
		{Decimal128{Hi: -5421010862427522171, Lo: 0xf675ddc000000001}, "-99999999999999999999999999999999999999"},
	} {
		if got := tc.d.String(); got != tc.want {
			t.Errorf("Decimal128{%d,%d}.String() = %s, want %s", tc.d.Hi, tc.d.Lo, got, tc.want)
		}
	}
}

// TestNarrowDecimalRefusesAValueItCannotHold: a file whose values contradict
// the precision it declares is an ERROR naming the column, not a different
// number. This is the ADR-0018 posture for every other self-contradicting
// number in a parquet file.
func TestNarrowDecimalRefusesAValueItCannotHold(t *testing.T) {
	data, err := os.ReadFile("testdata/wide_decimal_nested.parquet")
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	// Read the 16-byte column as if the catalog declared DECIMAL(18,10):
	// that is the narrow box, and the fixture's widest value does not fit.
	leaves := r.fr.Leaves()
	idx := -1
	for i, l := range leaves {
		if l.Name == "d" {
			idx = i
		}
	}
	if idx < 0 {
		t.Fatal("no leaf named d")
	}
	_, err = readColumnToAny(r.fr, 0, idx, int(r.fr.RowGroupNumRows(0)),
		Column{Name: "d", Type: TypeDecimal, Precision: 18, Scale: 10, Nullable: true})
	if err == nil {
		t.Fatal("a 16-byte value read as DECIMAL(18,10) was accepted; it should be refused")
	}
	if !strings.Contains(err.Error(), "does not fit") {
		t.Fatalf("error does not name the problem: %v", err)
	}
}

// TestWideDecimalWriteRoundTrip: a DECIMAL past 18 digits is written as a
// sixteen-byte FIXED_LEN_BYTE_ARRAY leaf, which is what the format requires
// and what makes the wide value storable at all.
//
// It used to be written as INT64 whatever the precision, because the physical
// type was chosen from the TypeID alone. Two things followed. The unscaled
// value had to fit 64 bits or the write was refused — the widest DECIMAL(38)
// values had no encoding here. And the file wadjet produced was one the
// Apache implementation will not OPEN:
//
//	Decimal(precision=38, scale=10) cannot be applied to primitive type INT64
//
// so every wadjet table with a DECIMAL(p > 18) column was unreadable outside
// wadjet. Found by the compaction gate's PyArrow arm
// (compaction.TestCompactionIsIdempotentOverTheTypeMatrix).
func TestWideDecimalWriteRoundTrip(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "d", Type: TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
	}}
	vals := []Decimal128{
		{Hi: 5421010862427522170, Lo: 0x98a223fffffffff}, // past 2^63
		Decimal128From(7),
		Decimal128From(-7),
		Decimal128From(0),
		{Hi: -5421010862427522171, Lo: 0xf675ddc000000001}, // its negative
	}
	rows := make([]map[string]any, len(vals)+1)
	for i, v := range vals {
		rows[i] = map[string]any{"d": v}
	}
	rows[len(vals)] = map[string]any{}

	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	// The leaf must be a sixteen-byte FIXED_LEN_BYTE_ARRAY, not an INT64.
	leaf := r.fr.Leaves()[0]
	if leaf.Type == nil || *leaf.Type != PhysicalFixedLenByteArray || leaf.TypeLength != decimalFLBAWidth {
		t.Fatalf("DECIMAL(38,0) leaf is %v/%d, want FIXED_LEN_BYTE_ARRAY/%d",
			leaf.Type, leaf.TypeLength, decimalFLBAWidth)
	}
	got, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("read %d rows, want %d", len(got), len(rows))
	}
	for i, v := range vals {
		wantUnscaled(t, "wide-write", got[i]["d"], v.String())
	}
	if v, ok := got[len(vals)]["d"]; ok {
		t.Errorf("the NULL row came back as %#v", v)
	}
}

// TestWriterRefusesADecimalItCannotStore: a DECIMAL whose precision fits an
// INT64 leaf IS stored in one, and an unscaled value past 64 bits then has no
// encoding. It must fail the WRITE rather than truncate — the file is the one
// artifact a writer cannot take back.
//
// The refusal it earns is now the DECLARED PRECISION's, which is the stricter
// and more honest of the two: a DECIMAL(18,0) column bounds its unscaled
// values below 10^18, well inside the 2^63 the encoding allows, so the value
// has already violated its own type before the encoding has an opinion
// (#647). The 64-bit encoding guard behind it is unreachable for that reason
// and stays as the leaf-write backstop ADR-0018 asks for.
func TestWriterRefusesADecimalItCannotStore(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "d", Type: TypeDecimal, Precision: 18, Scale: 0, Nullable: true},
	}}
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	wide := Decimal128{Hi: 5421010862427522170, Lo: 0x98a223fffffffff}
	err = pw.WriteRows([]map[string]any{{"d": wide}})
	if err == nil {
		err = pw.Close()
	}
	if err == nil {
		t.Fatal("writing a 128-bit DECIMAL through the INT64 encoding was accepted")
	}
	if !strings.Contains(err.Error(), "numeric field overflow") {
		t.Fatalf("error does not name the problem: %v", err)
	}
	if got := sqlerr.StateOf(err); got != "22003" {
		t.Fatalf("SQLSTATE %q, want 22003 numeric_value_out_of_range (err: %v)", got, err)
	}

	// A value that DOES fit is still written, and still reads back.
	var ok bytes.Buffer
	pw2, err := NewWriter(&ok, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw2.WriteRows([]map[string]any{{"d": Decimal128From(7)}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := pw2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	rr, err := NewReaderFromBytes(ok.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	rows, err := rr.ReadRows(nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("read %d rows, want 1", len(rows))
	}
	wantUnscaled(t, "narrow-in-wide-column", rows[0]["d"], "7")
}
