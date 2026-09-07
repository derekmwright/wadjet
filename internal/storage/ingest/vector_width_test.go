package ingest

import (
	"strings"
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
		// One class per rule, whichever door asks (#913): a WIDTH refusal is
		// pgvector's 22000 with pgvector's sentence — the same the SQL doors
		// raise through batch.VectorWidthError — a byte count that is not a
		// whole number of float32s is 22023, and a box that is not a vector
		// at all is 42804.
		if s := sqlerr.StateOf(err); s != "22000" && s != "22023" && s != "42804" {
			t.Errorf("refusing %v: SQLSTATE %q, want 22000, 22023 or 42804: %v", box, s, err)
		}
	}
	for _, box := range []any{[]float32{1, 2, 3}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}} {
		if err := checkType(col, box); err != nil {
			t.Errorf("ingest refused %v (%T), which is exactly VECTOR(3): %v", box, box, err)
		}
	}

	// The WORDING, not only the class (#913). The SQL doors say
	// `expected N dimensions, not M` — pgvector's own sentence, asserted by
	// wadjet.TestAVectorLiteralIsExactlyTheDeclaredWidthAtEveryDoor — and this
	// door said `column "v" is VECTOR(3); the value has 2 components` under a
	// different SQLSTATE. Same value, same rule, two answers decided only by
	// which door it arrived at.
	for _, c := range []struct {
		box  any
		frag string
	}{
		{[]float32{1, 2}, "expected 3 dimensions, not 2"},
		{[]float32{1, 2, 3, 4}, "expected 3 dimensions, not 4"},
		{[]float32{}, "expected 3 dimensions, not 0"},
		// A []byte box holding a whole number of float32s is the same rule
		// through a different spelling, and answers the same sentence.
		{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, "expected 3 dimensions, not 2"},
	} {
		err := checkType(col, c.box)
		if err == nil {
			t.Errorf("ingest accepted %v", c.box)
			continue
		}
		if s := sqlerr.StateOf(err); s != "22000" {
			t.Errorf("refusing %v: SQLSTATE %q, want pgvector's 22000: %v", c.box, s, err)
		}
		if !strings.Contains(err.Error(), c.frag) {
			t.Errorf("refusing %v: %q does not say %q, which is what every SQL door says "+
				"for the same value", c.box, err, c.frag)
		}
		// The column name still localizes the refusal: the ingest door takes
		// a whole ROW, so which column was wrong is what pgvector's own
		// message does not carry.
		if !strings.Contains(err.Error(), `"v"`) {
			t.Errorf("refusing %v: %q does not name the column", c.box, err)
		}
	}
}
