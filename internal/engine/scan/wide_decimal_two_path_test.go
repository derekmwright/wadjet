package scan

import (
	"math/big"
	"os"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

const wideDecimalNestedFixture = "../../storage/parquet/testdata/wide_decimal_nested.parquet"

// TestBothPathsAgreeOnAWideDecimal is ADR-0018 §3 for issue #419.
//
// DECIMAL(38,10) is stored by PyArrow as a 16-byte FIXED_LEN_BYTE_ARRAY. The
// native scan decoded it into a 128-bit value and was right; the ROW reader
// accumulated all sixteen bytes into an int64 and returned the low 64 bits of
// the unscaled value, reinterpreted as signed, with no error. Which of the
// two runs is decided by the SHAPE of the table's schema — a single
// ARRAY/ROW/MAP column anywhere in the read set sends the whole read to the
// row path (#393) — so the same column answered two different numbers
// depending on what OTHER column the table carried.
//
// This drives both paths over the same bytes and requires the same value, and
// takes the row path through batch.FromRows so the vector the engine actually
// queries is what is compared, not the intermediate box.
func TestBothPathsAgreeOnAWideDecimal(t *testing.T) {
	data, err := os.ReadFile(wideDecimalNestedFixture)
	if err != nil {
		t.Skipf("fixture missing (regen with testdata/gen_wide_decimal_nested.py): %v", err)
	}
	schema := []pqt.Column{
		{Name: "d", Type: pqt.TypeDecimal, Nullable: true, Precision: 38, Scale: 10},
		{Name: "unscaled", Type: pqt.TypeString, Nullable: true},
	}

	fr, err := pqt.OpenFileReaderFromBytes(data)
	if err != nil {
		t.Fatalf("opening fixture: %v", err)
	}
	native, err := ReadRowGroupNative(fr, 0, schema, nil)
	if err != nil {
		t.Fatalf("ReadRowGroupNative: %v", err)
	}

	r, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("opening fixture: %v", err)
	}
	batches, err := readFileBatchesViaRows(r, schema, []string{"d", "unscaled"})
	if err != nil {
		t.Fatalf("row path: %v", err)
	}
	if len(batches) != 1 {
		t.Fatalf("row path produced %d batches, want 1", len(batches))
	}
	rowPath := batches[0]

	if native.Len != rowPath.Len {
		t.Fatalf("native read %d rows, row path read %d", native.Len, rowPath.Len)
	}
	sawWide := false
	for i := 0; i < native.Len; i++ {
		nNull := native.Columns[0].Nulls.IsNullFast(i)
		rNull := rowPath.Columns[0].Nulls.IsNullFast(i)
		if nNull != rNull {
			t.Fatalf("row %d: native NULL=%v, row path NULL=%v", i, nNull, rNull)
		}
		if nNull {
			continue
		}
		nv := int128ToBig(native.Columns[0].DecimalData.Data[i])
		rv := int128ToBig(rowPath.Columns[0].DecimalData.Data[i])
		if nv.Cmp(rv) != 0 {
			t.Errorf("row %d: native unscaled %s, row path %s", i, nv, rv)
		}
		// And both against the file's own text rendering of the value.
		want := new(big.Int)
		if _, ok := want.SetString(native.Columns[1].BytesData.StringValue(i), 10); !ok {
			t.Fatalf("fixture: unparsable unscaled value at row %d", i)
		}
		if nv.Cmp(want) != 0 {
			t.Errorf("row %d: decoded %s, want %s", i, nv, want)
		}
		if want.BitLen() > 63 {
			sawWide = true
		}
	}
	if !sawWide {
		t.Fatal("fixture carries no value past 64 bits — it cannot gate this")
	}
}

// TestRowPathBoxesAWideDecimalForTheVector checks the boundary the parquet
// package cannot check on its own: it hands back pqt.Decimal128 (it cannot
// name batch.Int128 — batch imports parquet, not the reverse) and the vector
// has to accept it. Without the arm the value would be dropped by
// Vector.mismatch, which is loud; the shape it replaced was an int64 holding
// the wrong number, which was not.
func TestRowPathBoxesAWideDecimalForTheVector(t *testing.T) {
	v := batch.NewVectorWithScale(pqt.TypeDecimal, 1, 10)
	wide := pqt.Decimal128{Hi: 5421010862427522170, Lo: 0x98a223fffffffff}
	v.SetValue(0, wide)
	want, _ := new(big.Int).SetString("99999999999999999999999999999999999999", 10)
	if got := int128ToBig(v.DecimalData.Data[0]); got.Cmp(want) != 0 {
		t.Errorf("stored %s, want %s", got, want)
	}
}
