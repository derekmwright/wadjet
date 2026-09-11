// This file holds expr array fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"strings"
)

// --- Array/nested type function implementations ---

// toSlice converts a value to []any, handling both []any and []map[string]any.
func toSlice(v any) ([]any, bool) {
	switch tv := v.(type) {
	case []any:
		return tv, true
	case []map[string]any:
		out := make([]any, len(tv))
		for i, m := range tv {
			out[i] = m
		}
		return out, true
	default:
		return nil, false
	}
}

// cardinality(array) — returns the number of elements. PostgreSQL's
// `cardinality` counts every element of every dimension and answers 0 for an
// empty array, which is what this does and what it always did.
func fnCardinality(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	return int32Count(len(arr))
}

// array_length(a, dim) returns length only for nonempty arrays at dim=1.
// NULL input/dimension, empty arrays and every other dimension return NULL.
// ARRAY is one-dimensional; nested ARRAY elements do not add dimensions.
// The one-argument extension keeps cardinality's answer, including zero for
// an empty array; it is deliberately different from the two-argument form (#637).
// See docs/internals/array-length-dimensional-boundary.md for the design.
func fnArrayLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	if len(args) < 2 {
		// The wadjet-only one-argument spelling, unchanged.
		return int32Count(len(arr))
	}
	if args[1] == nil {
		return nil
	}
	if ToInt64(args[1]) != 1 || len(arr) == 0 {
		return nil
	}
	return int32Count(len(arr))
}

// element_at(array, index) — returns the element at 1-based index (Trino convention)
// For MAPs: element_at(map, key) returns the value for the given key.
// Negative indices count from the end.
//
// A MAP value and an ARRAY of ROW("key","value") (e.g. map_entries()'s output)
// share the same runtime shape — []any of {key,value} rows — so this
// value-only entry point CANNOT tell a MAP key lookup from an array index and
// treats every slice positionally. The MAP-vs-ARRAY choice is made from the
// COMPILED type of the first argument instead, in elementAtExpr, which the
// compiler wraps every element_at / m['k'] subscript in (#607). A genuine Go
// map (map_from_entries, constructed literals) is unambiguous and keyed here
// directly.
func fnElementAt(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	if m, ok := args[0].(map[string]any); ok {
		return m[fmt.Sprint(args[1])]
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	idx := int(ToInt64(args[1]))
	if idx > 0 {
		idx-- // convert 1-based to 0-based
	} else if idx < 0 {
		idx = len(arr) + idx // negative index from end
	} else {
		return nil // 0 is invalid in 1-based indexing
	}
	if idx < 0 || idx >= len(arr) {
		return nil
	}
	return arr[idx]
}

// array_contains(array, element) — returns true if array contains element
func fnArrayContains(args []any) any {
	if len(args) < 2 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	target := args[1]
	for _, elem := range arr {
		if elem == target || fmt.Sprint(elem) == fmt.Sprint(target) {
			return true
		}
	}
	return false
}

// array_join(array, delimiter) — joins array elements into a string
func fnArrayJoin(args []any) any {
	if len(args) < 2 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	delim := toString(args[1])
	parts := make([]string, 0, len(arr))
	for _, elem := range arr {
		if elem != nil {
			parts = append(parts, fmt.Sprint(elem))
		}
	}
	return strings.Join(parts, delim)
}

// array_min(array) — returns the minimum element
func fnArrayMin(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok || len(arr) == 0 {
		return nil
	}
	min := arr[0]
	for _, elem := range arr[1:] {
		if elem == nil {
			continue
		}
		if min == nil || fmt.Sprint(elem) < fmt.Sprint(min) {
			min = elem
		}
	}
	return min
}

// row_field(row, 'field_name') — extracts a named field from a ROW/struct value
func fnRowField(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	row, ok := args[0].(map[string]any)
	if !ok {
		return nil
	}
	field := toString(args[1])
	return row[field]
}
