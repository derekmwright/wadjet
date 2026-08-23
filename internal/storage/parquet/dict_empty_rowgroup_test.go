package parquet

import (
	"os"
	"testing"
)

// A dictionary page with ZERO entries is what pyarrow writes for a column
// chunk whose every value in that row group is NULL. The reader took the
// entry count off the BYTE_ARRAY offset table as len-1, and an empty page
// has no offset table at all, so the count came out -1 and the
// declared-vs-decoded check refused the file:
//
//	dictionary page declares 0 entries but decoded -1 as STRING
//
// Nothing about it is nested: the flat STRING column `s` below is
// unreadable in exactly the same way as the MAP's value leaf.
func TestDictionaryPageWithNoEntries(t *testing.T) {
	data, err := os.ReadFile("testdata/dict_empty_rowgroup.parquet")
	if err != nil {
		t.Fatalf("fixture: %v (regenerate with gen_dict_empty_rowgroup.py)", err)
	}
	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	if r.NumRowGroups() != 3 {
		t.Fatalf("fixture has %d row groups, want 3", r.NumRowGroups())
	}
	want := []map[string]any{
		{"id": int64(0), "s": "alpha", "m": map[string]any{"k": "v"}, "tags": []any{"a", "b"}},
		{"id": int64(1), "s": "beta", "m": map[string]any{"a": "1", "b": "2"}, "tags": []any{"a"}},
		{"id": int64(2), "m": map[string]any{"k": nil}, "tags": []any{}},
		{"id": int64(3)},
		{"id": int64(4), "s": "alpha", "m": map[string]any{"z": "9"}, "tags": []any{"c"}},
		{"id": int64(5), "s": "gamma", "m": map[string]any{}, "tags": []any{"a", "c"}},
	}
	got, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	assertRowsEqual(t, got, want)
	var byGroup []map[string]any
	for rg := 0; rg < r.NumRowGroups(); rg++ {
		rows, err := r.ReadRowGroup(rg, nil)
		if err != nil {
			t.Fatalf("ReadRowGroup(%d): %v", rg, err)
		}
		byGroup = append(byGroup, rows...)
	}
	assertRowsEqual(t, byGroup, want)

	// The middle row group on its own is the one that carries the empty
	// dictionaries; read it alone so a passing whole-file read cannot hide
	// behind the groups around it.
	mid, err := r.ReadRowGroup(1, nil)
	if err != nil {
		t.Fatalf("ReadRowGroup(1): %v", err)
	}
	assertRowsEqual(t, mid, want[2:4])
}
