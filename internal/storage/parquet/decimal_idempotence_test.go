package parquet

import (
	"bytes"
	"encoding/json"
	"fmt"
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
		// DECIMAL(p>18) is boxed as Decimal128 by the reader and stored as
		// INT64 by this writer, so the values that round-trip are the ones
		// an int64 holds — the rest are refused by name, not truncated.
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
