package ingest

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// checkType had no VECTOR case at all, so a value of the wrong width reached
// the writer unchecked and the fixed-width leaf appended it, moving every
// later value's boundary (#886). The width is refused here so the INSERT that
// carried it is what fails, not the flush a buffer later.
func TestTheIngestBoundaryHoldsAVectorToItsDeclaredWidth(t *testing.T) {
	col := parquet.Column{Name: "v", Type: parquet.TypeVector, Nullable: true, Dimension: 3}
	for _, box := range []any{
		[]float32{1, 2},
		[]float32{1, 2, 3, 4},
		[]float32{},
		[]byte{1, 2, 3},
		"1,2,3",
		int64(3),
	} {
		err := checkType(col, box)
		if err == nil {
			t.Errorf("ingest accepted %v (%T) for a VECTOR(3) column", box, box)
			continue
		}
		if s := sqlerr.StateOf(err); s != "22023" && s != "42804" {
			t.Errorf("refusing %v: SQLSTATE %q, want 22023 or 42804: %v", box, s, err)
		}
	}
	for _, box := range []any{[]float32{1, 2, 3}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}} {
		if err := checkType(col, box); err != nil {
			t.Errorf("ingest refused %v (%T), which is exactly VECTOR(3): %v", box, box, err)
		}
	}
}
