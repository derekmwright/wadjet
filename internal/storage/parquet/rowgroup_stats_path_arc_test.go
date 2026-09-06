package parquet

import (
	"bytes"
	"testing"
)

// #925 regression: a nested leaf's statistics overwrite a same-named top-level
// column's bounds in RowGroupStats, corrupting static row-group pruning and the
// stats persisted to the catalog. FAILS on 868e307b (Columns["id"] = 99/99);
// keyed by full leaf path after.
func TestRowGroupStatsDoNotReplaceTopLevelBoundsWithNestedLeaf(t *testing.T) {
	s := Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "r", Type: TypeRow, Fields: []Column{{Name: "id", Type: TypeInt64}}},
	}}
	var b bytes.Buffer
	w, err := NewWriter(&b, s, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err = w.WriteRows([]map[string]any{{"id": int64(42), "r": map[string]any{"id": int64(99)}}}); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	fr, err := OpenFileReaderFromBytes(b.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	st := fr.RowGroupStats(0).Columns["id"]
	if st.MinValue != int64(42) || st.MaxValue != int64(42) {
		t.Fatalf("top-level id=42 stats overwritten with nested r.id: %+v", st)
	}
}

// #925 twin: the physical leaf ordering reversed — nested leaves before the
// top-level — must still leave the top-level bounds intact, and two nested
// branches sharing the basename address distinct full paths.
func TestRowGroupStatsKeyByFullPathAcrossOrderings(t *testing.T) {
	s := Schema{Columns: []Column{
		{Name: "r", Type: TypeRow, Fields: []Column{{Name: "id", Type: TypeInt64}}},
		{Name: "q", Type: TypeRow, Fields: []Column{{Name: "id", Type: TypeInt64}}},
		{Name: "id", Type: TypeInt64},
	}}
	var b bytes.Buffer
	w, err := NewWriter(&b, s, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err = w.WriteRows([]map[string]any{{
		"r":  map[string]any{"id": int64(7)},
		"q":  map[string]any{"id": int64(8)},
		"id": int64(42),
	}}); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	fr, err := OpenFileReaderFromBytes(b.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	cols := fr.RowGroupStats(0).Columns
	if st := cols["id"]; st.MinValue != int64(42) || st.MaxValue != int64(42) {
		t.Fatalf("top-level id bounds: %+v, want 42/42", st)
	}
	if st := cols["r.id"]; st.MinValue != int64(7) || st.MaxValue != int64(7) {
		t.Fatalf("nested r.id bounds: %+v, want 7/7", st)
	}
	if st := cols["q.id"]; st.MinValue != int64(8) || st.MaxValue != int64(8) {
		t.Fatalf("nested q.id bounds: %+v, want 8/8", st)
	}
}
