package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// SortMergeJoin decides key EQUALITY with kernel.ResolveSortCompare. Until
// #415 that resolver's default arm handed back a comparator reporting every
// pair EQUAL, and ARRAY, ROW, MAP and VECTOR all landed there — so a
// sort-merge join on a container key emitted the full CROSS PRODUCT and
// called it an inner join. The `cmp == nil` guard next to the resolve call
// could not catch it: the resolver never returned nil.
//
// Two left rows share a key, two right rows share a different key, and one
// row on each side matches nothing. Under the old comparator this emitted
// 5 × 5 = 25 rows.

func TestSortMergeJoin_ArrayKeysMatchOnlyEqualArrays(t *testing.T) {
	arr := func(name string) parquet.Column {
		return parquet.Column{Name: name, Type: parquet.TypeArray,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}}
	}
	leftSchema := []parquet.Column{arr("k"), {Name: "lv", Type: parquet.TypeString}}
	rightSchema := []parquet.Column{arr("rk"), {Name: "rv", Type: parquet.TypeString}}

	// ["a"] is a PREFIX of ["a","b"]: a comparator that stopped at the
	// common prefix without breaking the tie by length would join them.
	leftRows := []map[string]any{
		{"k": []any{"a", "b"}, "lv": "l-ab-1"},
		{"k": []any{"a"}, "lv": "l-a"},
		{"k": []any{"a", "b"}, "lv": "l-ab-2"},
		{"k": []any{"z"}, "lv": "l-z-unmatched"},
		{"k": []any{}, "lv": "l-empty"},
	}
	rightRows := []map[string]any{
		{"rk": []any{"a"}, "rv": "r-a-1"},
		{"rk": []any{"a", "b"}, "rv": "r-ab"},
		{"rk": []any{"a"}, "rv": "r-a-2"},
		{"rk": []any{"q"}, "rv": "r-q-unmatched"},
		{"rk": []any{}, "rv": "r-empty"},
	}

	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	got := runSMJ(t, j,
		chunkBatches(t, rightSchema, rightRows, 2),
		chunkBatches(t, leftSchema, leftRows, 2))
	defer j.Close()

	// ["a","b"]: 2 left × 1 right = 2. ["a"]: 1 × 2 = 2. []: 1 × 1 = 1.
	want := map[string][]string{
		"l-ab-1":  {"r-ab"},
		"l-ab-2":  {"r-ab"},
		"l-a":     {"r-a-1", "r-a-2"},
		"l-empty": {"r-empty"},
	}
	if len(got) != 5 {
		t.Fatalf("row count: got %d want 5 (an always-equal comparator emits 25) — %v", len(got), got)
	}
	seen := map[string][]string{}
	for _, row := range got {
		lv, _ := row["lv"].(string)
		rv, _ := row["rv"].(string)
		seen[lv] = append(seen[lv], rv)
	}
	for lv, wantRVs := range want {
		if len(seen[lv]) != len(wantRVs) {
			t.Errorf("left %q joined %v, want %v", lv, seen[lv], wantRVs)
			continue
		}
		for _, rv := range wantRVs {
			found := false
			for _, s := range seen[lv] {
				if s == rv {
					found = true
				}
			}
			if !found {
				t.Errorf("left %q did not join %q (joined %v)", lv, rv, seen[lv])
			}
		}
	}
	for _, unmatched := range []string{"l-z-unmatched"} {
		if len(seen[unmatched]) != 0 {
			t.Errorf("%q should not have joined anything, joined %v", unmatched, seen[unmatched])
		}
	}
}

// TestSortMergeJoin_RowKeysMatchOnlyEqualRows is the same property for ROW
// keys, where the comparison is field-wise rather than element-wise.
func TestSortMergeJoin_RowKeysMatchOnlyEqualRows(t *testing.T) {
	rowType := func(name string) parquet.Column {
		return parquet.Column{Name: name, Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeString, Nullable: true},
			{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		}}
	}
	leftSchema := []parquet.Column{rowType("k"), {Name: "lv", Type: parquet.TypeString}}
	rightSchema := []parquet.Column{rowType("rk"), {Name: "rv", Type: parquet.TypeString}}

	leftRows := []map[string]any{
		{"k": map[string]any{"a": "x", "b": int64(1)}, "lv": "l-x1"},
		{"k": map[string]any{"a": "x", "b": int64(2)}, "lv": "l-x2"},
		{"k": map[string]any{"a": "y", "b": int64(1)}, "lv": "l-y1-unmatched"},
	}
	rightRows := []map[string]any{
		{"rk": map[string]any{"a": "x", "b": int64(2)}, "rv": "r-x2"},
		{"rk": map[string]any{"a": "x", "b": int64(1)}, "rv": "r-x1"},
		{"rk": map[string]any{"a": "z", "b": int64(1)}, "rv": "r-z1-unmatched"},
	}

	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	got := runSMJ(t, j,
		chunkBatches(t, rightSchema, rightRows, 2),
		chunkBatches(t, leftSchema, leftRows, 2))
	defer j.Close()

	if len(got) != 2 {
		t.Fatalf("row count: got %d want 2 (an always-equal comparator emits 9) — %v", len(got), got)
	}
	want := map[string]string{"l-x1": "r-x1", "l-x2": "r-x2"}
	for _, row := range got {
		lv, _ := row["lv"].(string)
		rv, _ := row["rv"].(string)
		if want[lv] != rv {
			t.Errorf("left %q joined right %q, want %q", lv, rv, want[lv])
		}
	}
}
