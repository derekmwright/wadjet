package exec

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
// made a ROW or MAP key nondeterministic run to run. %v fixed the
// nondeterminism (fmt prints map keys in sorted order) and left the second
// half of the property missing: %v is deterministic but NOT injective, and
// neither is a raw byte run against a single 0x00 column separator. Distinct
// values that share bytes are one group after a drain, which is the same
// answer-depends-on-memory failure the constant caused. The `collides`
// cases below are the shapes that reached it.
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
		// %v printed both of these `[a b]`.
		{"array element boundary", []any{[]any{"a b"}}, []any{[]any{"a", "b"}}},
		// %v printed both of these `[]`.
		{"array empty vs one empty string", []any{[]any{}}, []any{[]any{""}}},
		// %v printed both of these `map[a:b c:d]`.
		{"row field boundary", []any{map[string]any{"a": "b c:d"}},
			[]any{map[string]any{"a": "b", "c": "d"}}},
		// Raw bytes plus a 0x00 separator: both were "a\x00\x00b".
		{"bytes across the separator", []any{[]byte("a\x00"), []byte("b")},
			[]any{[]byte("a"), []byte("\x00b")}},
		// The same hole in the STRING form, which shares the separator.
		{"string across the separator", []any{"a\x00", "b"}, []any{"a", "\x00b"}},
		// A literal "<null>" is a value, not the NULL marker.
		{"null vs its rendering", []any{nil}, []any{"<null>"}},
		// A nested NULL is not a nested empty string.
		{"nested null vs empty", []any{[]any{nil}}, []any{[]any{""}}},
		// Adjacent nested integers have no separator of their own.
		{"nested int boundary", []any{[]any{int64(1), int64(23)}},
			[]any{[]any{int64(12), int64(3)}}},
		// A vector is not the array of the same numbers.
		{"vector length", []any{[]float32{1, 2}}, []any{[]float32{1, 2, 0}}},
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

	// A BYTES key must carry its bytes, not a placeholder — the constant
	// "<unknown>" is what collapsed every BYTES group into one. It carries
	// them behind a uvarint length, which is what keeps a NUL inside the
	// value distinct from the column separator. The merge only needs a total
	// order over these keys, not the values' own order (an int-mode key is
	// decimal text, which does not sort numerically either).
	if got := key([]byte("alpha")); got != "\x05alpha" {
		t.Errorf("BYTES key = %q, want a length prefix and the raw bytes", got)
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

// TestMergeKeyFoldsCanonicalFloatValues is TestMergeKeyKeepsBoxedTypesDistinct's
// mirror image: values the comparator says are the SAME (kernel/
// float_order.go: -0.0 = 0.0, and every NaN payload = every other NaN
// payload) must produce the SAME merge key, at every level appendKeyValue
// reaches — the bare scalar, and a float nested inside an ARRAY or a VECTOR.
//
// Before this fix, appendKeyValue's float64/float32 arms rendered -0.0 as the
// literal text "-0" and 0.0 as "0" (strconv.AppendFloat does not canonicalize
// the sign of zero), and keyFloat32bits/keyFloat64bits (the VECTOR/ARRAY(FLOAT)
// element path) preserved -0.0's sign bit outright. So a partial aggregate
// drain merged NaN payloads together (already fixed pre-#446) but kept -0.0
// and +0.0 as two merge keys — the same "answer depends on how much memory it
// had" failure TestMergeKeyKeepsBoxedTypesDistinct documents, on the other
// side of the same invariant: P4 in kernel's container-order property test
// (cmp == 0 <=> key-equal).
func TestMergeKeyFoldsCanonicalFloatValues(t *testing.T) {
	buf := make([]byte, 0, 64)
	key := func(vals ...any) string { return serializeKey(buf, vals) }
	negZero64 := math.Copysign(0, -1)
	negZero32 := float32(math.Copysign(0, -1))
	nan1 := math.Float64frombits(0x7ff8000000000001) // quiet NaN, payload 1
	nan2 := math.Float64frombits(0x7ff8000000000002) // quiet NaN, payload 2

	cases := []struct {
		name string
		a, b []any
	}{
		{"float64 -0.0 vs 0.0", []any{negZero64}, []any{0.0}},
		{"float32 -0.0 vs 0.0", []any{negZero32}, []any{float32(0)}},
		{"float64 NaN payloads", []any{nan1}, []any{nan2}},
		{"vector -0.0 vs 0.0 element", []any{[]float32{negZero32, 1}}, []any{[]float32{0, 1}}},
		{"array float64 -0.0 vs 0.0 element", []any{[]any{negZero64}}, []any{[]any{0.0}}},
		{"array float32 -0.0 vs 0.0 element", []any{[]any{negZero32}}, []any{[]any{float32(0)}}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ka, kb := key(tc.a...), key(tc.b...)
			if ka != kb {
				t.Fatalf("values the comparator treats as equal produced different merge keys: %q vs %q", ka, kb)
			}
		})
	}
}

// TestAppendSerializedKeyReKeysNestedCIDR is the regression for
// appendSerializedKey re-keying a CIDR GROUP BY column only at the TOP
// LEVEL: an ARRAY(CIDR) column's types[i] entry is batch.TypeArray, which
// carries no element type of its own, so a CIDR leaf inside it fell all the
// way through appendKeyValue's plain-text `case string:` — unlike the
// in-memory columnar key (exec.appendColumnValue -> appendListKey ->
// appendNestedElem, which walks the real child vector's own declared type
// and already re-keys every CIDR leaf). GROUP BY arr_cidr therefore agreed
// with the un-spilled answer only until a cross-batch, cross-worker or spill
// boundary routed the same groups through this boxed path instead: a bare
// address and its own /32 host route inside the array produced two
// different spill/merge keys for one PostgreSQL inet value.
func TestAppendSerializedKeyReKeysNestedCIDR(t *testing.T) {
	meta := []parquet.Column{
		{Name: "arr_cidr", Type: parquet.TypeArray,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeCIDR}},
	}
	types := []batch.TypeID{batch.TypeArray}

	bare := appendSerializedKey(nil, []any{[]any{"10.0.0.1"}}, types, meta)
	slash32 := appendSerializedKey(nil, []any{[]any{"10.0.0.1/32"}}, types, meta)
	if string(bare) != string(slash32) {
		t.Fatalf("ARRAY(CIDR) holding a bare address vs its own /32 produced two spill/merge keys, "+
			"%q vs %q — one PostgreSQL inet value answers two GROUP BY groups after a spill", bare, slash32)
	}

	// A genuinely different address must still key differently — the fix
	// must not collapse every CIDR array into one key.
	different := appendSerializedKey(nil, []any{[]any{"10.0.0.2/32"}}, types, meta)
	if string(bare) == string(different) {
		t.Fatalf("two DIFFERENT addresses produced the same spill/merge key %q", bare)
	}

	// A ROW field one level down needs the same re-key, through meta.Fields.
	rowMeta := []parquet.Column{
		{Name: "r", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "c", Type: parquet.TypeCIDR},
		}},
	}
	rowTypes := []batch.TypeID{batch.TypeRow}
	rowBare := appendSerializedKey(nil, []any{map[string]any{"c": "10.0.0.1"}}, rowTypes, rowMeta)
	rowSlash32 := appendSerializedKey(nil, []any{map[string]any{"c": "10.0.0.1/32"}}, rowTypes, rowMeta)
	if string(rowBare) != string(rowSlash32) {
		t.Fatalf("ROW field holding a bare address vs its own /32 produced two spill/merge keys, %q vs %q",
			rowBare, rowSlash32)
	}
}
