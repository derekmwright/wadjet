package scan

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Regression tests for the native row-group decoder (issue #144 suite
// findings): DECIMAL pages decoded to all-zeros (no TypeDecimal case in the
// copy switch), and ARRAY columns were silently emitted as ALL-NULL (the
// leaf-name lookup missed and fell into the schema-evolution branch)
// instead of erroring so callers fall back to the row-based reader.

func writeOneRG(tb testing.TB, schema parquet.Schema, rows []map[string]any) *parquet.Reader {
	tb.Helper()
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
	if err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	data := buf.Bytes()
	r, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		tb.Fatal(err)
	}
	return r
}

func TestReadRowGroupNative_DecimalValues(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "d", Type: parquet.TypeDecimal, Nullable: true, Precision: 12, Scale: 2},
	}}
	rows := []map[string]any{
		{"d": 3.25}, {"d": nil}, {"d": -1.5}, {"d": int64(7)}, {"d": "12.34"},
	}
	r := writeOneRG(t, schema, rows)
	b, err := ReadRowGroupNative(r.FileReader(), 0, schema.Columns, nil)
	if err != nil {
		t.Fatalf("ReadRowGroupNative: %v", err)
	}
	want := []float64{3.25, 0, -1.5, 7, 12.34}
	isNull := []bool{false, true, false, false, false}
	for i := range want {
		if isNull[i] {
			if !b.Columns[0].Nulls.IsNullFast(i) {
				t.Fatalf("row %d: want NULL, got %v", i, b.Columns[0].GetValue(i))
			}
			continue
		}
		// The null-interleaved page exercises the scatter path; values
		// must land in their own slots, not shift into null positions.
		got, err := strconv.ParseFloat(fmt.Sprintf("%v", b.Columns[0].GetValue(i)), 64)
		if err != nil || got != want[i] {
			t.Fatalf("row %d: got %v, want %v (decimal page decoded to zeros or misaligned)",
				i, b.Columns[0].GetValue(i), want[i])
		}
	}
}

func TestReadRowGroupNative_ArrayErrorsLoudly(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "tags", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
	}}
	r := writeOneRG(t, schema, []map[string]any{{"tags": []any{"a", "b"}}})
	_, err := ReadRowGroupNative(r.FileReader(), 0, schema.Columns, nil)
	if err == nil {
		t.Fatal("ARRAY through the native reader must error (callers fall back to the row reader); silent all-NULL output is wrong results")
	}
}

// TestDecimalInto covers all four physical types a DECIMAL column may use.
// Parquet allows every one of them and TypeIDFromSchemaNode answers
// TypeDecimal for each, but this decoder handed all four to Values.Int64() —
// reinterpreting bytes of the wrong width and, for the narrower physicals,
// reading past the end of the page to find enough of them. The page's own
// physical type picks the decode now, and a page that cannot back the values
// asked of it is an error rather than a short read.
func TestDecimalInto(t *testing.T) {
	decode := func(t *testing.T, data parquet.Values, srcStart, n int) []batch.Int128 {
		t.Helper()
		dst := make([]batch.Int128, n)
		if err := decimalInto(dst, data, srcStart, n); err != nil {
			t.Fatalf("decimalInto: %v", err)
		}
		return dst
	}

	t.Run("int64", func(t *testing.T) {
		got := decode(t, parquet.PlainInt64Values([]int64{-2, 0, 1 << 40}), 0, 3)
		for i, w := range []int64{-2, 0, 1 << 40} {
			if got[i] != batch.Int128From(w) {
				t.Errorf("value %d = %+v, want %+v", i, got[i], batch.Int128From(w))
			}
		}
	})

	t.Run("int32", func(t *testing.T) {
		got := decode(t, parquet.PlainInt32Values([]int32{-2, 0, 12345}), 0, 3)
		for i, w := range []int64{-2, 0, 12345} {
			if got[i] != batch.Int128From(w) {
				t.Errorf("value %d = %+v, want %+v", i, got[i], batch.Int128From(w))
			}
		}
	})

	t.Run("fixed_len_byte_array", func(t *testing.T) {
		// Big-endian two's complement, the DECIMAL byte encoding: 12345 and
		// -2 in four bytes each.
		data := []byte{0x00, 0x00, 0x30, 0x39, 0xff, 0xff, 0xff, 0xfe}
		got := decode(t, parquet.ByteArrayValues(data, []uint32{0, 4, 8}), 0, 2)
		for i, w := range []int64{12345, -2} {
			if got[i] != batch.Int128From(w) {
				t.Errorf("value %d = %+v, want %+v", i, got[i], batch.Int128From(w))
			}
		}
	})

	t.Run("srcStart_offsets_into_the_page", func(t *testing.T) {
		// The scatter path decodes a run that starts partway through the
		// page's values.
		got := decode(t, parquet.PlainInt64Values([]int64{9, 8, 7, 6}), 2, 2)
		for i, w := range []int64{7, 6} {
			if got[i] != batch.Int128From(w) {
				t.Errorf("value %d = %+v, want %+v", i, got[i], batch.Int128From(w))
			}
		}
	})

	t.Run("short_page_is_an_error", func(t *testing.T) {
		dst := make([]batch.Int128, 4)
		if err := decimalInto(dst[:3], parquet.ByteArrayValues(nil, nil), 0, 3); err == nil {
			t.Error("an empty page asked for 3 values returned no error")
		}
		if err := decimalInto(dst, parquet.PlainInt32Values([]int32{1}), 0, 4); err == nil {
			t.Error("a 1-value page asked for 4 values returned no error")
		}
		if err := decimalInto(dst[:2], parquet.PlainInt64Values([]int64{1, 2}), 1, 2); err == nil {
			t.Error("a 2-value page asked for values 1..2 returned no error")
		}
	})
}
