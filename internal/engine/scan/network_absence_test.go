package scan

import (
	"bytes"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A zero-length entry in a column whose entries are all sixteen bytes is an
// absence. Both readers used to disagree about that, in different directions:
// the row reader refused it outright once checkByteWidth arrived ("UUID is 16
// bytes per value but row 2 holds 10" — on a file wadjet had written), and
// with the carve-out that followed, both paths produced a non-NULL EMPTY
// value: false to IS NULL, equal to the empty string. Neither is what "there
// is no address here" means.
//
// The writer now writes NULL for the empty literal and refuses an unparseable
// one. Files already on disk still carry the zero-length entries, so the
// readers hold the other end: zero length in a fixed-width column is NULL, on
// both paths, identically.

// legacyAbsenceFile writes the shape older wadjet builds produced: a
// zero-length BYTE_ARRAY entry, non-NULL, in a UUID and an IPV6 column. It
// goes in as []byte{} because the string path now writes NULL for "" — which
// is the fix, and which is separately asserted below.
func legacyAbsenceFile(t *testing.T) []byte {
	t.Helper()
	schema := pqt.Schema{Columns: []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "u", Type: pqt.TypeUUID, Nullable: true},
		{Name: "a", Type: pqt.TypeIPv6, Nullable: true},
		{Name: "b", Type: pqt.TypeBytes, Nullable: true},
	}}
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := w.WriteRows([]map[string]any{
		{"id": int64(0), "u": "550e8400-e29b-41d4-a716-446655440000", "a": "2001:db8::1", "b": []byte("xy")},
		{"id": int64(1), "u": []byte{}, "a": []byte{}, "b": []byte{}},
		{"id": int64(2), "u": "", "a": "", "b": []byte("z")},
		{"id": int64(3), "u": nil, "a": nil, "b": nil},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}

func TestZeroLengthFixedWidthValueIsNullOnBothPaths(t *testing.T) {
	raw := legacyAbsenceFile(t)
	schema := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "u", Type: pqt.TypeUUID, Nullable: true},
		{Name: "a", Type: pqt.TypeIPv6, Nullable: true},
		{Name: "b", Type: pqt.TypeBytes, Nullable: true},
	}

	// Row 0 holds real values; rows 1 and 2 are the two ways an absence gets
	// onto disk (a legacy zero-length entry, and the empty literal the writer
	// now turns into NULL); row 3 is an ordinary NULL. BYTES has no fixed
	// width, so its empty value stays a value.
	wantNull := map[string][]bool{
		"u": {false, true, true, true},
		"a": {false, true, true, true},
		"b": {false, false, false, true},
	}

	t.Run("native", func(t *testing.T) {
		fr, err := pqt.OpenFileReaderFromBytes(raw)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		b, err := ReadRowGroupNative(fr, 0, schema, nil)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if b.Len != 4 {
			t.Fatalf("read %d rows, want 4", b.Len)
		}
		for ci, col := range schema {
			want, ok := wantNull[col.Name]
			if !ok {
				continue
			}
			for i := 0; i < b.Len; i++ {
				if got := b.Columns[ci].Nulls.IsNull(i); got != want[i] {
					t.Errorf("%s row %d: IS NULL = %v, want %v", col.Name, i, got, want[i])
				}
			}
		}
	})

	t.Run("row", func(t *testing.T) {
		r, err := pqt.NewReaderFromBytes(raw)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		rows, err := r.ReadRowsAs(schema, nil)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(rows) != 4 {
			t.Fatalf("read %d rows, want 4", len(rows))
		}
		for name, want := range wantNull {
			for i := range want {
				v, present := rows[i][name]
				isNull := !present || v == nil
				if isNull != want[i] {
					t.Errorf("%s row %d: IS NULL = %v (%v), want %v", name, i, isNull, v, want[i])
				}
			}
		}
	})

	// The value that IS there must survive both readers unchanged — the
	// point of the rule is absence, not silence.
	t.Run("the real values agree", func(t *testing.T) {
		fr, err := pqt.OpenFileReaderFromBytes(raw)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		b, err := ReadRowGroupNative(fr, 0, schema, nil)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		r, err := pqt.NewReaderFromBytes(raw)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		rows, err := r.ReadRowsAs(schema, nil)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		for ci, col := range schema[1:] {
			native := b.Columns[ci+1].BytesData.Value(0)
			rowv, _ := rows[0][col.Name].([]byte)
			if !bytes.Equal(native, rowv) {
				t.Errorf("%s row 0: native %x, row path %x", col.Name, native, rowv)
			}
			if col.Name != "b" && len(native) != 16 {
				t.Errorf("%s row 0: %d bytes, want 16", col.Name, len(native))
			}
		}
	})
}

// TestEmptyNetworkLiteralWritesNull is the writer half: "" is an absence on
// the way IN, so nothing downstream has to reconstruct that from a
// zero-length entry.
func TestEmptyNetworkLiteralWritesNull(t *testing.T) {
	schema := pqt.Schema{Columns: []pqt.Column{
		{Name: "u", Type: pqt.TypeUUID, Nullable: true},
		{Name: "a", Type: pqt.TypeIPv6, Nullable: true},
		{Name: "m", Type: pqt.TypeMAC, Nullable: true},
		{Name: "v4", Type: pqt.TypeIPv4, Nullable: true},
	}}
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := w.WriteRows([]map[string]any{
		{"u": "", "a": "", "m": "", "v4": ""},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	fr, err := pqt.OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	b, err := ReadRowGroupNative(fr, 0, schema.Columns, nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	for i, col := range schema.Columns {
		if !b.Columns[i].Nulls.IsNull(0) {
			t.Errorf("%s: the empty literal was stored as a value (%v)", col.Name, b.Columns[i].GetValue(0))
		}
	}

	r, err := pqt.NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	rows, err := r.ReadRowsAs(schema.Columns, nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	for _, col := range schema.Columns {
		if v, present := rows[0][col.Name]; present && v != nil {
			t.Errorf("%s: the row path read the empty literal as %v", col.Name, v)
		}
	}
}
