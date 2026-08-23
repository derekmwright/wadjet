package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestContainerGroupKeysAreDistinct is the #408 regression.
//
// appendColumnValue's `default: append(buf, '?')` gave every ARRAY, ROW, MAP
// and VECTOR value the SAME one-byte key. One default fed six operators —
// GROUP BY, DISTINCT, the set operations, COUNT(DISTINCT), APPROX_DISTINCT and
// hash-join keys — so a container column grouped into one group, counted one
// distinct value, and joined as a cross product.
//
// The property under test is injectivity: distinct values must produce
// distinct keys, equal values must produce equal ones. The pairs below are the
// ones a naive concatenation gets wrong — [["a"],["b"]] vs [["a","b"]] is the
// same byte stream without a length prefix, and an empty container is the same
// as a NULL one without a null flag.
func TestContainerGroupKeysAreDistinct(t *testing.T) {
	arr := containerArrayVector(t, [][]any{
		{"a"},      // 0
		{"b"},      // 1
		{"a", "b"}, // 2
		{},         // 3: empty, distinct from NULL
		nil,        // 4: NULL
		{"a"},      // 5: equal to row 0
		{"a", nil}, // 6: a NULL element, distinct from {"a"}
		{"ab"},     // 7: distinct from {"a","b"} — the length prefix
	})
	row := containerRowVector(t, []any{
		map[string]any{"a": "x", "b": int64(1)}, // 0
		map[string]any{"a": "x", "b": int64(2)}, // 1
		map[string]any{"a": "y", "b": int64(1)}, // 2
		map[string]any{"a": "x", "b": int64(1)}, // 3: equal to row 0
		map[string]any{"a": nil, "b": int64(1)}, // 4: NULL field
		nil,                                     // 5: NULL row
	})
	vec := containerVectorVector(t, [][]float32{
		{1, 2},  // 0
		{2, 1},  // 1: same multiset, different order
		{1, 2},  // 2: equal to row 0
		{1, -2}, // 3
		{0, 0},  // 4
		nil,     // 5: NULL
	})

	cases := []struct {
		name string
		v    *batch.Vector
		// equal lists row groups that must share a key; every row outside its
		// own group must differ from it.
		equal [][]int
	}{
		{"array", arr, [][]int{{0, 5}, {1}, {2}, {3}, {4}, {6}, {7}}},
		{"row", row, [][]int{{0, 3}, {1}, {2}, {4}, {5}}},
		{"vector", vec, [][]int{{0, 2}, {1}, {3}, {4}, {5}}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			keys := map[int]string{}
			for _, group := range tc.equal {
				for _, r := range group {
					keys[r] = string(keyOfRow(tc.v, r))
				}
			}
			for gi, group := range tc.equal {
				for _, r := range group {
					for _, other := range group {
						if keys[r] != keys[other] {
							t.Errorf("rows %d and %d hold the same value but got different keys %q / %q",
								r, other, keys[r], keys[other])
						}
					}
				}
				for gj, otherGroup := range tc.equal {
					if gi == gj {
						continue
					}
					for _, r := range group {
						for _, other := range otherGroup {
							if keys[r] == keys[other] {
								t.Errorf("rows %d and %d hold different values but collapsed to the same key %q",
									r, other, keys[r])
							}
						}
					}
				}
			}
		})
	}
}

// keyOfRow builds the key the group-by loops build: a null flag, then the
// encoded value.
func keyOfRow(v *batch.Vector, row int) []byte {
	if v.Nulls.IsNullFast(row) {
		return []byte{1}
	}
	return appendColumnValue([]byte{0}, v, row, v.Type)
}

func containerArrayVector(t *testing.T, rows [][]any) *batch.Vector {
	t.Helper()
	vals := make([]map[string]any, len(rows))
	for i, r := range rows {
		if r == nil {
			vals[i] = map[string]any{"c": nil}
			continue
		}
		vals[i] = map[string]any{"c": append([]any{}, r...)}
	}
	b := batch.FromRows([]parquet.Column{{
		Name: "c", Type: parquet.TypeArray, Nullable: true,
		ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true},
	}}, vals)
	return b.Columns[0]
}

func containerRowVector(t *testing.T, rows []any) *batch.Vector {
	t.Helper()
	vals := make([]map[string]any, len(rows))
	for i, r := range rows {
		vals[i] = map[string]any{"c": r}
	}
	b := batch.FromRows([]parquet.Column{{
		Name: "c", Type: parquet.TypeRow, Nullable: true,
		Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeString, Nullable: true},
			{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		},
	}}, vals)
	return b.Columns[0]
}

func containerVectorVector(t *testing.T, rows [][]float32) *batch.Vector {
	t.Helper()
	vals := make([]map[string]any, len(rows))
	for i, r := range rows {
		if r == nil {
			vals[i] = map[string]any{"c": nil}
			continue
		}
		vals[i] = map[string]any{"c": append([]float32{}, r...)}
	}
	b := batch.FromRows([]parquet.Column{{
		Name: "c", Type: parquet.TypeVector, Nullable: true, Dimension: 2,
	}}, vals)
	return b.Columns[0]
}
