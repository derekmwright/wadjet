package parquet

import (
	"bytes"
	"testing"
)

// A NULL field of a PRESENT struct (#449).
//
// The record assembler used to leave such a field out of the map entirely, so
// ROW(a=>NULL, b=>-3) read back as map[b:-3] here and as map[a:<nil> b:-3]
// through the columnar reader, whose ROW vector answers one entry per child
// whether or not the child is null. That is a live two-path value divergence
// — the same value, two boxes, and a comparison between the two arms sees a
// difference that is not in the data. ADR-0018 §3: a file is readable through
// all of the decode paths, and a value means the same thing on each.
//
// The scan-side half of this — the columnar box compared against the row box
// over the same file — is TestRowFieldNullsAgreeAcrossReadPaths in
// internal/engine/scan.

func nullFieldSchema() Schema {
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "r", Type: TypeRow, Nullable: true, Fields: []Column{
			{Name: "a", Type: TypeString, Nullable: true},
			{Name: "b", Type: TypeInt64, Nullable: true},
		}},
		// A struct one level in, so the rule is checked where the field set
		// is recovered recursively rather than only at the top of a column.
		{Name: "outer", Type: TypeRow, Nullable: true, Fields: []Column{
			{Name: "n", Type: TypeInt64, Nullable: true},
			{Name: "s", Type: TypeRow, Nullable: true, Fields: []Column{
				{Name: "x", Type: TypeInt64, Nullable: true},
				{Name: "y", Type: TypeString, Nullable: true},
			}},
		}},
	}}
}

// nullFieldRows covers every position a NULL can take around a struct: the
// first field, the last field, both, none, and the struct itself.
func nullFieldRows() []map[string]any {
	return []map[string]any{
		{
			"id":    int64(0),
			"r":     map[string]any{"a": "x", "b": int64(-3)},
			"outer": map[string]any{"n": int64(1), "s": map[string]any{"x": int64(7), "y": "s"}},
		},
		{ // the shape in the issue: a present ROW whose FIRST field is NULL
			"id":    int64(1),
			"r":     map[string]any{"a": nil, "b": int64(-3)},
			"outer": map[string]any{"n": nil, "s": map[string]any{"x": nil, "y": "s"}},
		},
		{ // the LAST field NULL
			"id":    int64(2),
			"r":     map[string]any{"a": "x", "b": nil},
			"outer": map[string]any{"n": int64(3), "s": map[string]any{"x": int64(7), "y": nil}},
		},
		{ // present, every field NULL — not the same value as a NULL struct
			"id":    int64(3),
			"r":     map[string]any{"a": nil, "b": nil},
			"outer": map[string]any{"n": nil, "s": map[string]any{"x": nil, "y": nil}},
		},
		{ // the inner struct NULL, and the outer one present
			"id":    int64(4),
			"r":     map[string]any{"a": "x", "b": int64(9)},
			"outer": map[string]any{"n": int64(5), "s": nil},
		},
		{ // the whole column NULL: an ABSENT key, which is the top level's
			// own spelling of absence and stays that way
			"id": int64(5),
		},
	}
}

func writeNullFieldFile(t *testing.T, rows []map[string]any) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, nullFieldSchema(), DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}

func readNullFieldFile(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	got, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	return got
}

func TestPresentRowWithNullFieldKeepsItsKey(t *testing.T) {
	want := nullFieldRows()
	data := writeNullFieldFile(t, want)

	t.Run("ReadRows", func(t *testing.T) {
		assertRowsEqual(t, readNullFieldFile(t, data), want)
	})

	// The per-row-group entry is a separate caller (compaction, ANALYZE) and
	// must answer the same maps.
	t.Run("ReadRowGroupAs", func(t *testing.T) {
		r, err := NewReaderFromBytes(data)
		if err != nil {
			t.Fatal(err)
		}
		var got []map[string]any
		for rg := 0; rg < r.NumRowGroups(); rg++ {
			rows, err := r.ReadRowGroupAs(rg, nullFieldSchema().Columns, nil)
			if err != nil {
				t.Fatalf("ReadRowGroupAs(%d): %v", rg, err)
			}
			got = append(got, rows...)
		}
		assertRowsEqual(t, got, want)
	})
}

// TestNullRowFieldWritesTheSameWhicheverBoxItCameIn is the round-trip half:
// an explicit nil field and a missing key are the same absence on the way in,
// so the box the reader now hands back re-writes to the same file the box it
// used to hand back did. Without this, changing the reader's convention would
// have been a change to what compaction WRITES (ADR-0018 §4: the writer's box
// for a value is the reader's box for it).
func TestNullRowFieldWritesTheSameWhicheverBoxItCameIn(t *testing.T) {
	explicit := nullFieldRows()
	// The same rows with every nil-valued struct field deleted — the shape
	// the assembler used to produce.
	omitted := make([]map[string]any, len(explicit))
	for i, row := range explicit {
		omitted[i] = dropNilFields(row).(map[string]any)
	}

	a := writeNullFieldFile(t, explicit)
	b := writeNullFieldFile(t, omitted)
	if !bytes.Equal(a, b) {
		t.Errorf("the two boxes wrote different files (%d vs %d bytes): an explicit nil "+
			"field and a missing key must be the same absence on the way in", len(a), len(b))
	}

	// read -> write -> read is the identity, twice over: the reader's own box
	// fed back to the writer must not drift.
	got := readNullFieldFile(t, a)
	assertRowsEqual(t, got, explicit)
	again := readNullFieldFile(t, writeNullFieldFile(t, got))
	assertRowsEqual(t, again, explicit)
}

// dropNilFields returns v with every nil-valued key of every nested map
// removed, leaving the top-level row map's own keys alone.
func dropNilFields(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			if e == nil {
				continue
			}
			out[k] = dropNilFields(e)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = dropNilFields(e)
		}
		return out
	default:
		return v
	}
}
