package scan

import (
	"bytes"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ADR-0018 §3 applied to #707: a file whose DECIMAL declaration disagrees with
// the catalog's is readable through EVERY decode path or through none, and the
// value means the same thing on each.
//
// The native columnar path (rescaleDecimalChunk) and the row path
// (parquet.readColumnToAny) reconcile through one function, and this holds them
// to the same answer over the same bytes — which is the property that was
// FALSE before the fix in the loudest possible way: the two paths did not merely
// differ, they were wrong in opposite directions (12.75 read as 1275.00 on one,
// and its sibling read as 0.1275 on the other).
func TestBothReadPathsMoveADecimalToTheCatalogsScale(t *testing.T) {
	catalog := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "a", Type: pqt.TypeDecimal, Precision: 15, Scale: 2, Nullable: true},
	}
	for _, tc := range []struct {
		name      string
		fileScale int
		unscaled  int64
		want      int64 // the carrier at the CATALOG's scale
	}{
		{"file finer than the catalog", 4, 127500, 1275},
		{"file coarser than the catalog", 0, 12, 1200},
		{"file agrees", 2, 1275, 1275},
		{"finer, rounds half away from zero", 4, 127550, 1276},
		{"finer, negative rounds away from zero", 4, -127550, -1276},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := pqt.Schema{Columns: []pqt.Column{
				{Name: "id", Type: pqt.TypeInt64},
				{Name: "a", Type: pqt.TypeDecimal, Precision: 15, Scale: tc.fileScale, Nullable: true},
			}}
			var buf bytes.Buffer
			w, err := pqt.NewWriter(&buf, file, pqt.DefaultWriterConfig())
			if err != nil {
				t.Fatal(err)
			}
			box := pqt.Decimal128From(tc.unscaled)
			if err := w.WriteRows([]map[string]any{
				{"id": int64(1), "a": box},
				{"id": int64(2), "a": nil},
			}); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			data := buf.Bytes()

			// Native columnar path.
			fr, err := pqt.OpenFileReaderFromBytes(data)
			if err != nil {
				t.Fatal(err)
			}
			b, err := ReadRowGroupNative(fr, 0, catalog, nil)
			if err != nil {
				t.Fatalf("native read: %v", err)
			}
			vec := b.Columns[1]
			if vec.DecimalData.Scale != 2 {
				t.Fatalf("the native vector declares scale %d, want the catalog's 2",
					vec.DecimalData.Scale)
			}
			if got := vec.DecimalData.Data[0]; got.Hi != pqt.Decimal128From(tc.want).Hi ||
				got.Lo != pqt.Decimal128From(tc.want).Lo {
				t.Errorf("native carrier = {%d,%d}, want %d at the catalog's scale "+
					"(the file declares scale %d and holds %d) — #707",
					got.Hi, got.Lo, tc.want, tc.fileScale, tc.unscaled)
			}
			if !vec.Nulls.IsNull(1) {
				t.Error("the native path lost the NULL row")
			}

			// Row path, over the same bytes.
			r, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			rows, err := r.ReadRowsAs(catalog, nil)
			if err != nil {
				t.Fatalf("row read: %v", err)
			}
			if len(rows) != 2 {
				t.Fatalf("%d rows, want 2", len(rows))
			}
			got, ok := rows[0]["a"].(int64)
			if !ok {
				t.Fatalf("row box is %#v, want int64 for a 15-digit column", rows[0]["a"])
			}
			if got != tc.want {
				t.Errorf("row carrier = %d, want %d — the two read paths must answer "+
					"the same bytes the same way (ADR-0018 §3)", got, tc.want)
			}
			if rows[1]["a"] != nil {
				t.Errorf("the row path lost the NULL row: %#v", rows[1]["a"])
			}
		})
	}
}

// TestNativeReadRefusesADecimalWithNoCarrierAtTheCatalogsScale: the native path
// raises the same 22003 the row path does rather than storing a wrapped number
// (protocol method 8, loud beats plausible).
func TestNativeReadRefusesADecimalWithNoCarrierAtTheCatalogsScale(t *testing.T) {
	catalog := []pqt.Column{
		{Name: "a", Type: pqt.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}
	file := pqt.Schema{Columns: []pqt.Column{
		{Name: "a", Type: pqt.TypeDecimal, Precision: 18, Scale: 6, Nullable: true},
	}}
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, file, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows([]map[string]any{
		{"a": pqt.Decimal128From(123456789012345678)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	fr, err := pqt.OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRowGroupNative(fr, 0, catalog, nil); err == nil {
		t.Fatal("a value with no carrier at the catalog's scale was decoded as a number")
	}
}
