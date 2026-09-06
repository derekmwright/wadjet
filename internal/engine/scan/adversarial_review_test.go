package scan

import (
	"bytes"
	"reflect"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
	gp "github.com/parquet-go/parquet-go"
)

// #915 regression: dictionary pruning and pushed row predicates must resolve a
// top-level column by full path, consistently with the native reader — a
// nested leaf sharing the basename must never shadow the top-level column.

func adversarialSelected(sel []uint32, d FilterDecision, n int) []uint32 {
	if d == FilterNone {
		return []uint32{}
	}
	if d == FilterPartial {
		return sel
	}
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(i)
	}
	return out
}

func TestAdversarialScanShadowedColumn(t *testing.T) {
	type nested struct {
		ID int64 `parquet:"id,dict"`
	}
	type record struct {
		A  nested `parquet:"a"`
		ID int64  `parquet:"id,dict"`
	}
	var buf bytes.Buffer
	w := gp.NewGenericWriter[record](&buf)
	if _, e := w.Write([]record{{nested{99}, 42}}); e != nil {
		t.Fatal(e)
	}
	if e := w.Close(); e != nil {
		t.Fatal(e)
	}
	f, e := pqt.OpenFileReaderFromBytes(buf.Bytes())
	if e != nil {
		t.Fatal(e)
	}
	b, e := ReadRowGroupNative(f, 0, []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}, nil)
	if e != nil {
		t.Fatal(e)
	}
	if got := b.Columns[0].GetValue(0); got != int64(42) {
		t.Fatalf("unpruned id=%v", got)
	}
	for i, l := range f.Leaves() {
		t.Logf("leaf %d path=%v", i, l.Path)
	}
	if CanDictPruneRowGroup(f, 0, []EqProbe{{ColName: "id", Value: int64(42)}}) {
		t.Error("dictionary probe prunes matching top-level id=42 using nested a.id=99")
	}
	sel, d, e := EvalRowGroupPreds(f, 0, []RowPred{{Col: "id", Op: "=", Value: int64(42)}}, 1)
	if e != nil {
		t.Fatal(e)
	}
	if got := adversarialSelected(sel, d, 1); !reflect.DeepEqual(got, []uint32{0}) {
		t.Errorf("pushed filter selects %v; unpruned top-level id=42 matches row 0", got)
	}
}
