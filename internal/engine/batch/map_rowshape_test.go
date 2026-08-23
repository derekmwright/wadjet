package batch

import (
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The ROW→BATCH boundary must accept the ROW-LEVEL shape of a MAP — the Go
// map the parquet row reader produces and the writer consumes — as well as
// the storage shape GetValue hands back. It took only the latter, so every
// scan that fell back to the row reader raised the #361 guard on scanWorker
// and killed the process (#393).
//
// The conversion is FromRows's, not SetValue's: a bare map is also a ROW's
// box, and SetValue cannot tell the two apart — see FromRows.

func mapVectorColumn(valueType parquet.TypeID, keyType parquet.TypeID) parquet.Column {
	return parquet.Column{Name: "m", Type: parquet.TypeMap, Nullable: true,
		ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "key", Type: keyType},
			{Name: "value", Type: valueType, Nullable: true},
		}}}
}

func TestFromRowsAcceptsRowShapeMap(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want any
	}{
		{"single entry",
			map[string]any{"a": int64(1)},
			[]any{map[string]any{"key": "a", "value": int64(1)}}},
		{"entries come out in key order, whatever the map's iteration order",
			map[string]any{"c": int64(3), "a": int64(1), "b": int64(2)},
			[]any{
				map[string]any{"key": "a", "value": int64(1)},
				map[string]any{"key": "b", "value": int64(2)},
				map[string]any{"key": "c", "value": int64(3)},
			}},
		{"an empty map is an empty entry list, not a NULL",
			map[string]any{}, []any{}},
		{"a NULL value inside a present entry stays NULL",
			map[string]any{"k": nil},
			[]any{map[string]any{"key": "k", "value": nil}}},
		{"the storage shape still works",
			[]any{map[string]any{"key": "z", "value": int64(9)}},
			[]any{map[string]any{"key": "z", "value": int64(9)}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := mapVectorColumn(parquet.TypeInt64, parquet.TypeString)
			b := FromRows([]parquet.Column{col}, []map[string]any{{"m": tt.in}, {"m": nil}})
			v := b.Columns[0]
			if got := v.GetValue(0); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetValue(0) = %#v, want %#v", got, tt.want)
			}
			if got := v.GetValue(1); got != nil {
				t.Errorf("GetValue(1) = %#v, want nil (a NULL map is not an empty one)", got)
			}
		})
	}
}

// A numerically-typed key column would take the row level's string key and
// raise the guard from inside the scan — one process-killer traded for
// another. The key is coerced to what the key child can hold instead.
func TestFromRowsMapNumericKey(t *testing.T) {
	col := mapVectorColumn(parquet.TypeString, parquet.TypeInt64)
	b := FromRows([]parquet.Column{col}, []map[string]any{{"m": map[string]any{"7": "seven"}}})
	v := b.Columns[0]
	want := []any{map[string]any{"key": int64(7), "value": "seven"}}
	if got := v.GetValue(0); !reflect.DeepEqual(got, want) {
		t.Errorf("GetValue = %#v, want %#v", got, want)
	}
}

// SetValue keeps refusing a bare map into a MAP or ARRAY vector. A ROW's box
// has that exact shape, so a ROW written into a mis-derived container vector
// (#397, live on the stage DAG) must still be REPORTED — softening the guard
// here to make the row reader work would have turned that error into a
// silently reshaped value.
func TestSetValueStillRejectsBareMap(t *testing.T) {
	for _, col := range []parquet.Column{
		mapVectorColumn(parquet.TypeInt64, parquet.TypeString),
		{Name: "a", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
	} {
		t.Run(col.Type.String(), func(t *testing.T) {
			v := NewColumnVector(col, 1)
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("SetValue accepted a bare map; want the #361 guard")
				}
				if _, ok := r.(*TypeMismatchError); !ok {
					t.Fatalf("panic is %T (%v), want *TypeMismatchError", r, r)
				}
			}()
			v.SetValue(0, map[string]any{"a": "b"})
		})
	}
}
