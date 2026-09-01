package memory

import (
	"bytes"
	"fmt"
	"testing"
)

// SpillRows must return every box it was given AS ITSELF, type included
// (#632).
//
// The writer had typed arms for bool / int / float / string and a default arm
// that stored fmt's DISPLAY text. A []byte fell into it, so a BYTES value went
// to disk as the nine characters "[104 105]" and came back a string — which
// BYTES SetValue accepts as a value, so the group it keyed came back keyed by
// the text. Nothing rejected anything at any step.
//
// The assertion is on the Go TYPE as well as the value, because the defect's
// output prints the same as the input's under "%v": comparing rendered forms
// is how a gate passes with the value destroyed.
func TestSpillRowsRoundTripsEveryBoxAsItself(t *testing.T) {
	sm, err := NewSpillManager(t.TempDir(), NewTracker("test", 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	row := map[string]any{
		"null":    nil,
		"true":    true,
		"false":   false,
		"i64":     int64(-9_007_199_254_740_993),
		"i32":     int32(-2_147_483_648),
		"f64":     float64(-2.5e-300),
		"f32":     float32(1.5),
		"str":     "s\x00with a NUL and a ] bracket",
		"bytes":   []byte{0xff, 0x00, 0x68, 0x69, 0xfe},
		"empty":   []byte{},
		"bytelen": []byte("[104 105]"), // the display form of another value
	}
	path, err := sm.SpillRows([]map[string]any{row})
	if err != nil {
		t.Fatal(err)
	}
	back, err := ReadSpilledRows(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 1 {
		t.Fatalf("read %d rows, want 1", len(back))
	}
	for col, want := range row {
		got := back[0][col]
		if wb, ok := want.([]byte); ok {
			gb, ok := got.([]byte)
			if !ok {
				t.Errorf("%s: read back as %T (%v), want []byte", col, got, got)
				continue
			}
			if !bytes.Equal(gb, wb) {
				t.Errorf("%s: read back %v, want %v", col, gb, wb)
			}
			continue
		}
		if fmt.Sprintf("%T:%v", got, got) != fmt.Sprintf("%T:%v", want, want) {
			t.Errorf("%s: read back %T:%v, want %T:%v", col, got, got, want, want)
		}
	}
	// A BYTES value and the display text of a DIFFERENT one must not come back
	// as the same thing — the collision the display encoding created.
	if b, ok := back[0]["bytes"].([]byte); ok {
		if s, ok := back[0]["bytelen"].([]byte); ok && bytes.Equal(b, s) {
			t.Error("two distinct BYTES values came back equal")
		}
	}
}
