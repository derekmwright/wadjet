package parquet

import (
	"reflect"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// arrayElements and rowFields resolve a caller's box for a CONTAINER column,
// or refuse it.
//
// decomposeArray, decomposeRow and decomposeMap each asserted one Go shape and
// treated the failure as an ABSENT SUBTREE — the value became NULL, with no
// error from WriteRows, from Close or from the read. A nullable
// ARRAY(INT64) handed []int64{1,2,3} read back as NULL (#889), and
// ValidateNestedLeaves could not see it either: it walks only the shapes that
// assert successfully, so a box it does not recognise is a box it says nothing
// about.
//
// Two changes, and the order matters. First, a slice or map of ANY element
// type is normalised — []int64, []string, []float64, map[string]int and the
// rest are unambiguous spellings of the same value, and refusing them would
// make an obvious call an error where it used to be silently wrong. Second,
// what is left — a scalar, a string, a struct — has no reading as a container
// at all and is 42804 datatype_mismatch, LATCHED, so the write fails rather
// than storing a NULL nobody asked for.
//
// reflect rather than a list of element types: the list would be the thing
// that goes out of date, and this runs once per container VALUE on the write
// path, not per leaf.
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
