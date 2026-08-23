package exec

import (
	"testing"
)

// TestMergeKeyKeepsBoxedTypesDistinct is the regression for appendKeyValue's
// `default: "<unknown>"`.
//
// serializeKey / appendSerializedKey build the k-way MERGE key for drained
// partial aggregate runs (aggregate_partial_drain_cursor.go), so a type that
// falls through the switch does not merely sort oddly — every group of that
// type merges into ONE. []byte fell through, so a BYTES group key was distinct
// while the aggregate stayed in memory and collapsed into a single group the
// moment memory pressure forced a drain: the same query, same data, a
// different answer depending on how much memory it had.
//
// The container forms fell through too, and a range over a Go map would have
// made a ROW or MAP key nondeterministic run to run. fmt prints map keys in
// sorted order, which is why the fallback is %v and not a hand-rolled walk.
func TestMergeKeyKeepsBoxedTypesDistinct(t *testing.T) {
	buf := make([]byte, 0, 64)
	key := func(vals ...any) string { return serializeKey(buf, vals) }

	cases := []struct {
		name string
		a, b []any
	}{
		{"bytes", []any{[]byte("alpha")}, []any{[]byte("beta")}},
		{"bytes vs empty", []any{[]byte("alpha")}, []any{[]byte("")}},
		{"array", []any{[]any{"a"}}, []any{[]any{"b"}}},
		{"array length", []any{[]any{"a"}}, []any{[]any{"a", "b"}}},
		{"vector", []any{[]float32{1, 2}}, []any{[]float32{2, 1}}},
		{"row", []any{map[string]any{"a": "x"}}, []any{map[string]any{"a": "y"}}},
		{"row field", []any{map[string]any{"a": "x"}}, []any{map[string]any{"b": "x"}}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ka, kb := key(tc.a...), key(tc.b...)
			if ka == kb {
				t.Fatalf("two different values produced the same merge key %q — "+
					"every group of this type merges into one after a partial drain", ka)
			}
			if again := key(tc.a...); again != ka {
				t.Fatalf("the same value produced two keys, %q then %q — a merge key "+
					"has to be stable across calls", ka, again)
			}
		})
	}

	// A BYTES key must round-trip its bytes, not a placeholder: the merge
	// compares these lexicographically, so the ordering has to be the values'.
	if got := key([]byte("alpha")); got != "alpha" {
		t.Errorf("BYTES key = %q, want the raw bytes", got)
	}
	// Determinism of the map fallback is the load-bearing property; check it
	// over a map big enough that Go's randomised iteration would show.
	wide := map[string]any{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8}
	first := key(wide)
	for i := 0; i < 20; i++ {
		if again := key(wide); again != first {
			t.Fatalf("map key rendered %q then %q — the fallback is not deterministic", first, again)
		}
	}
}
