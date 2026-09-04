package scan

import (
	"bytes"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The end-to-end half of round 0's N4, which is the shape the reviewer
// attacked: ONE BatchPool, a file whose every DECIMAL slot holds the widest
// carrier a DECIMAL(15,2) can, then a file declaring scale 0 whose every row is
// NULL.
//
// The second read rescales from scale 0 to scale 2 — a multiply by 100 — so a
// slot that came back from the pool still holding the first file's carrier
// would be seventeen digits, and the read would be REFUSED over a number no
// query can see. rescaleDecimalChunk rescales null slots deliberately, on the
// argument that zero is the only carrier the allocator puts there; the
// companion unit test pins that directly and this pins what it buys.
func TestAPooledScanReadsAnAllNullFileAfterAWideOne(t *testing.T) {
	schema := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "a", Type: pqt.TypeDecimal, Precision: 15, Scale: 2, Nullable: true},
	}
	const rows = 8
	shared := batch.NewBatchPool(schema, rows)
	for _, f := range []struct {
		name  string
		scale int
		null  bool
	}{
		{"values at the catalog's scale", 2, false},
		{"all NULL, declared at scale 0", 0, true},
	} {
		data := n4WriteFile(t, f.scale, f.null, rows)
		fr, err := pqt.OpenFileReaderFromBytes(data)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ReadRowGroupNative(fr, 0, schema, shared)
		if err != nil {
			t.Fatalf("%s: refused a file whose visible values are all fine: %v", f.name, err)
		}
		for i := 0; i < rows; i++ {
			if f.null != got.Columns[1].Nulls.IsNull(i) {
				t.Errorf("%s: row %d nullness moved", f.name, i)
			}
		}
		shared.Put(got)
	}
}

// n4WriteFile writes `rows` rows of one DECIMAL(15,scale) column: either the
// widest carrier that column can hold, or NULL.
func n4WriteFile(t *testing.T, scale int, null bool, rows int) []byte {
	t.Helper()
	s := pqt.Schema{Columns: []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "a", Type: pqt.TypeDecimal, Precision: 15, Scale: scale, Nullable: true},
	}}
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, s, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	rs := make([]map[string]any, 0, rows)
	for i := 0; i < rows; i++ {
		var v any
		if !null {
			v = pqt.Decimal128From(999999999999999)
		}
		rs = append(rs, map[string]any{"id": int64(i), "a": v})
	}
	if err := w.WriteRows(rs); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
