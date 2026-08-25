package exec

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// TestContainerKeyCodecRoundTrips pins the property the drained container
// group key rests on: decode(encode(v)) is v, for every boxed shape
// Vector.GetValue can hand a GROUP BY key.
//
// "Is v" is stricter than "renders alike" here on purpose for the float
// cases. The MERGE key folds NaN payloads onto one and -0.0 onto +0.0 —
// correctly, since the comparator calls those pairs equal — which is exactly
// why the VALUE cannot be decoded back out of it. This codec must NOT fold:
// the un-spilled path emits the bits the row carried, and the spilled path
// has to emit the same ones.
func TestContainerKeyCodecRoundTrips(t *testing.T) {
	negZero := float32(math.Copysign(0, -1))
	nan := float32(math.NaN())

	for _, tc := range []struct {
		name string
		v    any
	}{
		{"empty array", []any{}},
		{"array of int64", []any{int64(1), int64(-2), int64(0)}},
		{"array with nulls", []any{nil, "x", nil}},
		{"array of bytes", []any{[]byte("ab"), []byte{}, []byte{0, 1, 0}}},
		{"array of mixed scalars", []any{true, false, int32(7), float64(1.5), float32(2.5), "s"}},
		{"nested arrays", []any{[]any{"a"}, []any{"b", "c"}, []any{}}},
		{"row", map[string]any{"a": "x", "b": int64(3)}},
		{"empty row", map[string]any{}},
		{"row with nulls", map[string]any{"a": nil, "b": nil}},
		{"row of containers", map[string]any{
			"l": []any{"e"},
			"m": []any{map[string]any{"key": "k", "value": int64(1)}},
			"s": map[string]any{"x": int64(9)},
		}},
		{"map as entry list", []any{
			map[string]any{"key": "a", "value": int64(1)},
			map[string]any{"key": "b", "value": nil},
		}},
		{"vector", []float32{1, -2.5, 0.25, 1e30}},
		{"empty vector", []float32{}},
		{"vector with negative zero and NaN", []float32{negZero, nan, 0, -1}},
		{"decimal", batch.Int128{Hi: -1, Lo: 42}},
		{"string with a NUL and a separator byte", "a\x00b"},
		{"scalars at the top level", int64(math.MinInt64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc := appendContainerKeyValue(nil, tc.v, 0)
			got, err := decodeContainerKeyValue(enc)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !ckDeepEqual(got, tc.v) {
				t.Fatalf("round trip changed the value\n  in:  %#v\n  out: %#v", tc.v, got)
			}
		})
	}

	// Distinctness: the three values that render alike and are not the same
	// value. A group key that collapsed any pair would merge two groups the
	// un-spilled path keeps apart.
	t.Run("distinct encodings", func(t *testing.T) {
		seen := map[string]string{}
		for _, tc := range []struct {
			name string
			v    any
		}{
			{"empty array", []any{}},
			{"array of one empty string", []any{""}},
			{"array of two empties", []any{"", ""}},
			{"array of a b", []any{"a", "b"}},
			{"array of one a-space-b", []any{"a b"}},
			{"array of nested a b", []any{[]any{"a", "b"}}},
			{"row a:b c:d", map[string]any{"a": "b", "c": "d"}},
			{"row a:'b c:d'", map[string]any{"a": "b c:d"}},
			{"array of null", []any{nil}},
			{"array of empty string", []any{""}},
			{"vector of one zero", []float32{0}},
			{"vector of one negative zero", []float32{negZero}},
			{"vector of two zeros", []float32{0, 0}},
		} {
			enc := string(appendContainerKeyValue(nil, tc.v, 0))
			if prior, dup := seen[enc]; dup {
				// Two names for the same VALUE are fine; two different
				// values sharing bytes are one group after a drain.
				if fmt.Sprintf("%#v", tc.v) != prior {
					t.Errorf("%s encodes identically to %s — distinct group keys would merge", tc.name, prior)
				}
				continue
			}
			seen[enc] = fmt.Sprintf("%#v", tc.v)
		}
	})

	// The depth cap has to hold at the SAME depth on both sides: anything the
	// encoder emits must decode, and anything it refuses must fail loudly
	// rather than come back as a `%v` rendering two different values share.
	t.Run("depth cap is symmetric", func(t *testing.T) {
		nest := func(d int) any {
			var v any = "leaf"
			for i := 0; i < d; i++ {
				v = []any{v}
			}
			return v
		}
		// One level inside the cap round-trips.
		deep := nest(ckMaxDepth - 1)
		got, err := decodeContainerKeyValue(appendContainerKeyValue(nil, deep, 0))
		if err != nil {
			t.Fatalf("a value at the cap must still round-trip: %v", err)
		}
		if !ckDeepEqual(got, deep) {
			t.Fatalf("a value at the cap round-tripped to something else")
		}
		// Past it, the encoder refuses and the decoder says so — it does not
		// come back as text, and it does not encode into bytes that fail to
		// decode for an unrelated reason.
		enc := appendContainerKeyValue(nil, nest(ckMaxDepth+8), 0)
		v, err := decodeContainerKeyValue(enc)
		if err == nil {
			t.Fatalf("a subtree past the cap decoded to %#v instead of erroring", v)
		}
		if !strings.Contains(err.Error(), "deeper than") {
			t.Fatalf("past-the-cap error should name the depth, got %v", err)
		}
	})

	t.Run("corruption is an error, not a value", func(t *testing.T) {
		enc := appendContainerKeyValue(nil, []any{"abc", int64(1)}, 0)
		for _, tc := range []struct {
			name string
			b    []byte
		}{
			{"truncated", enc[:len(enc)-3]},
			{"trailing bytes", append(append([]byte(nil), enc...), 0)},
			{"unknown tag", []byte{200}},
			{"length past the end", []byte{ckString, 0x7f}},
			{"empty", nil},
		} {
			if v, err := decodeContainerKeyValue(tc.b); err == nil {
				t.Errorf("%s decoded to %#v instead of erroring", tc.name, v)
			}
		}
	})
}

// ckDeepEqual compares two decoded boxes structurally, treating NaN as equal
// to itself by BITS (reflect.DeepEqual says NaN != NaN, and the whole point
// of the float cases is that the bits survive).
func ckDeepEqual(a, b any) bool {
	switch av := a.(type) {
	case nil:
		return b == nil
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !ckDeepEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			ov, present := bv[k]
			if !present || !ckDeepEqual(v, ov) {
				return false
			}
		}
		return true
	case []float32:
		bv, ok := b.([]float32)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if math.Float32bits(av[i]) != math.Float32bits(bv[i]) {
				return false
			}
		}
		return true
	case []byte:
		bv, ok := b.([]byte)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if av[i] != bv[i] {
				return false
			}
		}
		return true
	case float32:
		bv, ok := b.(float32)
		return ok && math.Float32bits(av) == math.Float32bits(bv)
	case float64:
		bv, ok := b.(float64)
		return ok && math.Float64bits(av) == math.Float64bits(bv)
	default:
		return a == b
	}
}
