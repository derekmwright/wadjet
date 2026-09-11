package parquet

import (
	"reflect"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Container boxes must normalize or refuse, never silently become NULL (#889).
// arrayElements accepts slices/arrays of any element type; rowFields accepts
// string-keyed maps of any value type, using reflection once per container.
// Scalars, strings and structs have no container reading and raise a latched
// 42804. ValidateNestedLeaves and writer decomposition share this boundary.
// See docs/internals/parquet-container-box-normalization.md for the design.
func arrayElements(col Column, val any) ([]any, error) {
	if arr, ok := val.([]any); ok {
		return arr, nil
	}
	rv := reflect.ValueOf(val)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = rv.Index(i).Interface()
		}
		return out, nil
	}
	return nil, containerBoxError(col, val, "an ARRAY")
}

// rowFields resolves a ROW or MAP box to the string-keyed map both decompose
// functions read.
func rowFields(col Column, val any, what string) (map[string]any, error) {
	if m, ok := val.(map[string]any); ok {
		return m, nil
	}
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Map && rv.Type().Key().Kind() == reflect.String {
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			out[iter.Key().String()] = iter.Value().Interface()
		}
		return out, nil
	}
	return nil, containerBoxError(col, val, what)
}

func containerBoxError(col Column, val any, what string) error {
	return sqlerr.New("42804", "column %q is %s and a %T value is not %s",
		col.Name, col.Type, val, what)
}
