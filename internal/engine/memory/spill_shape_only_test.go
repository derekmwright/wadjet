package memory

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// A shape-only row goes to disk as a LENGTH and comes back as one.
//
// The raw-row spill writer has a default arm that renders whatever it does not
// recognise with fmt's %v, and that arm is #632: a []byte was stored as the
// text "[104 105]", handed back as a string, and accepted by BYTES SetValue as
// a value, so a spilled group came back keyed by nine ASCII bytes instead of
// its two. A batch.ShapeOnlyLen down any of the value arms would be the same
// class of defect one step worse — a column the planner proved nobody reads
// the bytes of would come back CARRYING bytes, and they would be a rendering
// of its length.
func TestSpilledRowsCarryAShapeOnlyLengthAsItself(t *testing.T) {
	sm, err := NewSpillManager(t.TempDir(), NewTracker("t", 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	rows := []map[string]any{
		{"s": batch.ShapeOnlyLen(0), "id": int64(1)},
		{"s": batch.ShapeOnlyLen(7), "id": int64(2)},
		{"s": nil, "id": int64(3)},
		{"s": batch.ShapeOnlyLen(300), "id": int64(4)},
		// A REAL string beside it in the same file, so the two encodings are
		// proven to stay apart rather than merely to work alone.
		{"s": "verbatim", "id": int64(5)},
	}
	path, err := sm.SpillRows(rows)
	if err != nil {
		t.Fatalf("SpillRows: %v", err)
	}
	back, err := ReadSpilledRows(path)
	if err != nil {
		t.Fatalf("ReadSpilledRows: %v", err)
	}
	if len(back) != len(rows) {
		t.Fatalf("%d rows back, wrote %d", len(back), len(rows))
	}
	for i, want := range rows {
		got := back[i]["s"]
		if want["s"] == nil {
			if got != nil {
				t.Errorf("row %d: NULL came back as %T:%v", i, got, got)
			}
			continue
		}
		if s, ok := want["s"].(string); ok {
			if got != s {
				t.Errorf("row %d: string %q came back as %T:%v", i, s, got, got)
			}
			continue
		}
		n, ok := got.(batch.ShapeOnlyLen)
		if !ok {
			t.Fatalf("row %d: a shape-only length came back as %T:%v — down a value arm it "+
				"becomes bytes the file never held (#632's class, #791's shape)", i, got, got)
		}
		if n != want["s"] {
			t.Errorf("row %d: length %v came back as %v", i, want["s"], n)
		}
	}
}
