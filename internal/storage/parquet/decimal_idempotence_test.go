package parquet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

// read → write must be the identity for a DECIMAL column (#429).
//
// The reader hands back the UNSCALED integer the format stores (3.25 at
// scale 2 is 325). The writer used to read an integer box as the WHOLE
// number and multiply it by 10^scale, so the two conventions were inverses:
// every compaction pass over a DECIMAL(p, s>0) column multiplied it by 10^s
// and wrote the result back over the inputs. One generation cannot see it —
// the first write is from ingest boxes — so these run three.
//
// ADR-0018's writer corollary is the contract under test: an INTEGER box
// (int, int32, int64, Decimal128) is the unscaled value; a REAL box or a
// numeric string carries the decimal point and is scaled on the way in.

func decimalSchema(precision, scale int) Schema {
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "d", Type: TypeDecimal, Nullable: true, Precision: precision, Scale: scale},
	}}
}

func writeRows(tb testing.TB, schema Schema, rows []map[string]any) []byte {
	tb.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		tb.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

func readAllRows(tb testing.TB, data []byte, schema []Column) []map[string]any {
	tb.Helper()
	r, err := NewReaderFromBytes(data)
	if err != nil {
		tb.Fatalf("reader: %v", err)
	}
	rows, err := r.ReadRowsAs(schema, nil)
	if err != nil {
		tb.Fatalf("ReadRowsAs: %v", err)
	}
	return rows
}

func TestDecimalReadWriteIsIdempotentAcrossGenerations(t *testing.T) {
	cases := []struct {
		name             string
		precision, scale int
		unscaled         []int64
	}{
		{"9_2", 9, 2, []int64{325, -325, 0, 1, -1, 999999999, -999999999}},
		{"18_4", 18, 4, []int64{32500, -1, 0, 999999999999999999, -999999999999999999}},
		// DECIMAL(p>18) is stored as FIXED_LEN_BYTE_ARRAY(16) by this writer
		// and boxed as Decimal128 by the reader. Passing plain int64 values
		// here exercises only the fits-in-64-bits slice of that box; a value
		// that does not fit is TestDecimalFLBARoundTripsBeyond64Bits below.
		{"38_10", 38, 10, []int64{32500000000, -1, 0, 9223372036854775807, -9223372036854775807}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := decimalSchema(tc.precision, tc.scale)
			gen1 := make([]map[string]any, len(tc.unscaled)+1)
			for i, u := range tc.unscaled {
				gen1[i] = map[string]any{"id": int64(i), "d": u}
			}
			gen1[len(tc.unscaled)] = map[string]any{"id": int64(len(tc.unscaled))} // NULL

			file1 := writeRows(t, schema, gen1)
			read1 := readAllRows(t, file1, schema.Columns)

			file2 := writeRows(t, schema, read1)
			read2 := readAllRows(t, file2, schema.Columns)

			file3 := writeRows(t, schema, read2)
			read3 := readAllRows(t, file3, schema.Columns)

			for i, u := range tc.unscaled {
				for gen, rows := range [][]map[string]any{read1, read2, read3} {
					got := rows[i]["d"]
					if !decimalEquals(got, u) {
						t.Fatalf("generation %d row %d: d = %#v, want unscaled %d "+
							"(read→write is multiplying by 10^scale)", gen+1, i, got, u)
					}
				}
			}
			for gen, rows := range [][]map[string]any{read1, read2, read3} {
				if v, ok := rows[len(tc.unscaled)]["d"]; ok {
					t.Errorf("generation %d: the NULL row came back as %#v", gen+1, v)
				}
			}
			if !reflect.DeepEqual(read1, read2) || !reflect.DeepEqual(read2, read3) {
				t.Errorf("generations disagree:\n  1 %#v\n  2 %#v\n  3 %#v", read1, read2, read3)
			}
			// Bit-exact, not merely equal in value: a rewrite of the same
			// values is the same file.
			if !bytes.Equal(file1, file2) || !bytes.Equal(file2, file3) {
				t.Errorf("rewritten files differ byte for byte (%d/%d/%d bytes)",
					len(file1), len(file2), len(file3))
			}
		})
	}
}

// decimal128FromBigInt renders a signed decimal string as the two's-complement
// 128-bit hi/lo halves decimalFLBABytes writes and decimalFromBytesRaw reads
// back, so a test can hand the writer an UNSCALED value that does not fit in
// an int64 without duplicating that bit-splitting by hand.
func decimal128FromBigInt(t *testing.T, s string) Decimal128 {
	t.Helper()
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("invalid decimal string %q", s)
	}
	mod := new(big.Int).Lsh(big.NewInt(1), 128)
	u := new(big.Int).Mod(n, mod) // Euclidean mod: non-negative, i.e. two's complement.
	hi := new(big.Int).Rsh(u, 64)
	lo := new(big.Int).And(u, new(big.Int).SetUint64(^uint64(0)))
	return Decimal128{Hi: int64(hi.Uint64()), Lo: lo.Uint64()}
}

// TestDecimalFLBARoundTripsBeyond64Bits is the FLBA sibling of
// TestDecimalReadWriteIsIdempotentAcrossGenerations: every value in that
// table's "38_10" case fits in an int64, which never exercises more than the
// bottom 64 bits of the FIXED_LEN_BYTE_ARRAY(16) box a DECIMAL(p>18) column
// actually uses. This pins a value with 20 significant digits — larger than
// an int64 can hold — round-tripping through wadjet's own reader/writer and
// cross-checked against PyArrow, which decides independently how those same
// sixteen bytes denote a number.
func TestDecimalFLBARoundTripsBeyond64Bits(t *testing.T) {
	schema := decimalSchema(38, 10)
	pos := decimal128FromBigInt(t, "93468288258671214869")
	neg := decimal128FromBigInt(t, "-93468288258671214869")
	rows := []map[string]any{
		{"id": int64(0), "d": pos},
		{"id": int64(1), "d": neg},
	}

	data := writeRows(t, schema, rows)
	got := readAllRows(t, data, schema.Columns)

	checkUnscaled := func(i int, want string) {
		t.Helper()
		d, ok := got[i]["d"].(Decimal128)
		if !ok {
			t.Fatalf("row %d: d = %#v (%T), want Decimal128", i, got[i]["d"], got[i]["d"])
		}
		if d.String() != want {
			t.Errorf("row %d: d = %s, want %s", i, d.String(), want)
		}
	}
	checkUnscaled(0, "93468288258671214869")
	checkUnscaled(1, "-93468288258671214869")

	// Idempotent across a rewrite: a value beyond int64 must survive a
	// compaction-shaped read→write, not get silently truncated or refused.
	again := readAllRows(t, writeRows(t, schema, got), schema.Columns)
	if !reflect.DeepEqual(got, again) {
		t.Errorf("rewrite changed the values:\n  before %#v\n  after  %#v", got, again)
	}

	if !havePyArrow() {
		t.Skip("python3 with pyarrow not available")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "wide_decimal.parquet")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("python3", "-c", pyArrowDecimalScript, path).CombinedOutput()
	if err != nil {
		t.Fatalf("pyarrow read failed: %v\n%s", err, out)
	}
	var pyGot []any
	if err := json.Unmarshal(out, &pyGot); err != nil {
		t.Fatalf("decoding pyarrow output %q: %v", out, err)
	}
	// pyArrowDecimalScript prints the SCALED decimal string; wadjet's
	// Decimal128.String prints the UNSCALED integer above, so this compares
	// against the scale-10 form of the same two values.
	want := []any{"9346828825.8671214869", "-9346828825.8671214869"}
	if fmt.Sprint(pyGot) != fmt.Sprint(want) {
		t.Errorf("pyarrow read %v, want %v", pyGot, want)
	}
}

// decimalEquals compares against the unscaled integer whichever box the
// reader chose for the column's precision.
func decimalEquals(got any, wantUnscaled int64) bool {
	switch v := got.(type) {
	case int64:
		return v == wantUnscaled
	case Decimal128:
		u, ok := v.Int64()
		return ok && u == wantUnscaled
	default:
		return false
	}
}

// The other half of the contract: a box that CARRIES the decimal point is
// scaled on the way in. This is the ingest shape — 3.25 arrives as a float64
// or as text, never as 325.
func TestDecimalIngestScalesTheRealBoxes(t *testing.T) {
	schema := decimalSchema(9, 2)
	rows := []map[string]any{
		{"id": int64(0), "d": 3.25},
		{"id": int64(1), "d": "3.25"},
		{"id": int64(2), "d": float32(-1.5)},
		{"id": int64(3), "d": int64(325)}, // already unscaled
	}
	want := []int64{325, 325, -150, 325}
	got := readAllRows(t, writeRows(t, schema, rows), schema.Columns)
	for i, w := range want {
		if !decimalEquals(got[i]["d"], w) {
			t.Errorf("row %d (%T): stored %#v, want unscaled %d", i, rows[i]["d"], got[i]["d"], w)
		}
	}
	// And the stored value survives a rewrite unchanged.
	again := readAllRows(t, writeRows(t, schema, got), schema.Columns)
	if !reflect.DeepEqual(got, again) {
		t.Errorf("rewrite changed the values:\n  before %#v\n  after  %#v", got, again)
	}
}

// PyArrow is the cross-check: it has to read the same NUMBER out of the file
// wadjet wrote. A wadjet-only round trip cannot see a scaling error that the
// reader and the writer make in opposite directions.
func TestDecimalPyArrowReadsWadjetWrite(t *testing.T) {
	if !havePyArrow() {
		t.Skip("python3 with pyarrow not available")
	}
	schema := decimalSchema(9, 2)
	data := writeRows(t, schema, []map[string]any{
		{"id": int64(0), "d": 3.25},        // real box, scaled on write
		{"id": int64(1), "d": int64(325)},  // integer box, already unscaled
		{"id": int64(2), "d": int64(-150)}, // negative
		{"id": int64(3), "d": int64(0)},
		{"id": int64(4)}, // NULL
	})
	dir := t.TempDir()
	path := filepath.Join(dir, "wadjet_decimal.parquet")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("python3", "-c", pyArrowDecimalScript, path).CombinedOutput()
	if err != nil {
		t.Fatalf("pyarrow read failed: %v\n%s", err, out)
	}
	var got []any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decoding pyarrow output %q: %v", out, err)
	}
	want := []any{"3.25", "3.25", "-1.50", "0.00", nil}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("pyarrow read %v, want %v", got, want)
	}
}

// pyArrowDecimalScript prints a DECIMAL column as the decimal STRINGS the
// values denote, so the assertion is about the number and not about how
// either side boxes it.
const pyArrowDecimalScript = `
import json, sys, pyarrow.parquet as pq
t = pq.read_table(sys.argv[1])
json.dump([None if v is None else str(v) for v in t.column("d").to_pylist()], sys.stdout)
`
