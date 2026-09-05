package parquet

import (
	"bytes"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// #889: a container box the writer cannot read is a LATCHED type error, never
// a NULL.
//
// decomposeArray, decomposeRow and decomposeMap each asserted one Go shape and
// treated the failure as an ABSENT SUBTREE. Measured at de5bc970, a nullable
// ARRAY(INT64) handed []int64{1,2,3} read back as NULL with WriteRows, Close
// and the read all returning nil, and ValidateNestedLeaves could not see it
// either: it walks only the shapes that assert successfully.
//
// Three dispositions, asserted separately, because the fix has to move exactly
// one of them: a malformed box is an ERROR, an intentional NULL is still NULL,
// and an EMPTY container is still empty.
func TestAContainerBoxIsReadOrRefusedButNeverSilentlyNull(t *testing.T) {
	arrayCol := Column{Name: "c", Type: TypeArray, Nullable: true,
		ElementType: &Column{Name: "element", Type: TypeInt64, Nullable: true}}
	rowCol := Column{Name: "c", Type: TypeRow, Nullable: true,
		Fields: []Column{{Name: "a", Type: TypeInt64, Nullable: true}}}
	mapCol := Column{Name: "c", Type: TypeMap, Nullable: true,
		ElementType: &Column{Name: "entry", Type: TypeRow, Fields: []Column{
			{Name: "key", Type: TypeString},
			{Name: "value", Type: TypeInt64, Nullable: true},
		}}}

	t.Run("a typed slice is the array it spells", func(t *testing.T) {
		for _, box := range []any{
			[]int64{1, 2, 3},
			[]int{1, 2, 3},
			[]int32{1, 2, 3},
			[]float64{1, 2, 3},
			[3]int64{1, 2, 3},
		} {
			got := roundTripOneValue(t, arrayCol, box)
			arr, ok := got.([]any)
			if !ok || len(arr) != 3 {
				t.Errorf("%v (%T) read back as %v (%T), want a three-element array",
					box, box, got, got)
				continue
			}
		}
	})

	t.Run("a typed map is the map it spells", func(t *testing.T) {
		got := roundTripOneValue(t, mapCol, map[string]int64{"k": 7})
		if got == nil {
			t.Error("map[string]int64{k:7} read back as NULL")
		}
	})

	t.Run("a box with no reading is refused", func(t *testing.T) {
		for _, tc := range []struct {
			col Column
			box any
		}{
			{arrayCol, int64(3)},
			{arrayCol, "1,2,3"},
			{arrayCol, true},
			{rowCol, int64(3)},
			{rowCol, "a=1"},
			{rowCol, []any{1, 2}},
			{mapCol, int64(3)},
			{mapCol, "k=1"},
			{mapCol, map[int]string{1: "a"}},
		} {
			err := writeOneValue(t, tc.col, tc.box)
			if err == nil {
				t.Errorf("%v (%T) into a %s column succeeded; it has no reading there",
					tc.box, tc.box, tc.col.Type)
				continue
			}
			if s := sqlerr.StateOf(err); s != "42804" {
				t.Errorf("refusing %v (%T) for %s: SQLSTATE %q, want 42804: %v",
					tc.box, tc.box, tc.col.Type, s, err)
			}
		}
	})

	t.Run("an intentional NULL is still NULL", func(t *testing.T) {
		for _, col := range []Column{arrayCol, rowCol, mapCol} {
			if got := roundTripOneValue(t, col, nil); got != nil {
				t.Errorf("a NULL %s read back as %v", col.Type, got)
			}
		}
	})

	t.Run("an empty container is still empty", func(t *testing.T) {
		if got := roundTripOneValue(t, arrayCol, []any{}); got == nil {
			t.Error("an empty ARRAY read back as NULL, not as an empty array")
		}
		if got := roundTripOneValue(t, mapCol, map[string]any{}); got == nil {
			t.Error("an empty MAP read back as NULL, not as an empty map")
		}
	})
}

// The refusal reaches the ingest boundary too, so a bad container fails the
// row that carried it rather than the flush a buffer later. ValidateNestedLeaves
// said nothing at all about a box it could not assert.
func TestValidateNestedLeavesRefusesAContainerBoxItCannotRead(t *testing.T) {
	col := Column{Name: "c", Type: TypeArray, Nullable: true,
		ElementType: &Column{Name: "element", Type: TypeInt64, Nullable: true}}
	if err := ValidateNestedLeaves(col, int64(3)); err == nil {
		t.Error("ValidateNestedLeaves accepted an int64 for an ARRAY column")
	}
	if err := ValidateNestedLeaves(col, []int64{1, 2}); err != nil {
		t.Errorf("ValidateNestedLeaves refused []int64 for an ARRAY(INT64) column: %v", err)
	}
	// And a bad LEAF inside a typed slice is now reachable, which it was not
	// while the walk stopped at the assertion.
	dateCol := Column{Name: "c", Type: TypeArray, Nullable: true,
		ElementType: &Column{Name: "element", Type: TypeDate, Nullable: true}}
	if err := ValidateNestedLeaves(dateCol, []string{"2026-02-30"}); err == nil {
		t.Error("ValidateNestedLeaves accepted a nonexistent calendar date inside a typed slice")
	}
}

// A container failure is LATCHED: Close reports it too, so no caller can write
// a file over a row it was told nothing about.
func TestAContainerRefusalIsLatchedIntoClose(t *testing.T) {
	col := Column{Name: "c", Type: TypeArray, Nullable: true,
		ElementType: &Column{Name: "element", Type: TypeInt64, Nullable: true}}
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf, Schema{Columns: []Column{col}}, DefaultWriterConfig())
	if err := nw.WriteMapRows([]map[string]any{{"c": int64(3)}}); err == nil {
		t.Fatal("WriteMapRows accepted an int64 for an ARRAY column")
	}
	if err := nw.Close(); err == nil {
		t.Fatal("Close returned nil after a refused container value")
	}
}
