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

// TestFromRowsAcceptsRowShapeMapAtEveryDepth: the conversion was applied only
// when the MAP was the whole COLUMN, so a MAP inside a ROW, inside an ARRAY
// or inside another MAP still reached SetValue as a bare Go map and raised
// the same guard — the same defect as #393, one level down, on every one of
// those three shapes. It became reachable on the columnar path too when #448
// routed a ROW with a container field to the row reader.
//
// The shapes that DID work are here beside them: a container with no MAP
// below it never enters the walk, and one that does must come out the same
// as before.
func TestFromRowsAcceptsRowShapeMapAtEveryDepth(t *testing.T) {
	i64 := func(n string) parquet.Column {
		return parquet.Column{Name: n, Type: parquet.TypeInt64, Nullable: true}
	}
	mp := func(name string, val parquet.Column) parquet.Column {
		val.Name = "value"
		return parquet.Column{Name: name, Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString}, val,
			}}}
	}
	arr := func(name string, elem parquet.Column) parquet.Column {
		return parquet.Column{Name: name, Type: parquet.TypeArray, Nullable: true, ElementType: &elem}
	}
	row := func(name string, fields ...parquet.Column) parquet.Column {
		return parquet.Column{Name: name, Type: parquet.TypeRow, Nullable: true, Fields: fields}
	}
	entry := func(k string, v any) map[string]any {
		return map[string]any{"key": k, "value": v}
	}

	tests := []struct {
		name string
		col  parquet.Column
		in   any
		want any
	}{
		{"map inside a row", row("c", i64("a"), mp("m", i64(""))),
			map[string]any{"a": int64(1), "m": map[string]any{"k": int64(2)}},
			map[string]any{"a": int64(1), "m": []any{entry("k", int64(2))}}},
		{"map inside an array", arr("c", mp("element", i64(""))),
			[]any{map[string]any{"k": int64(1)}},
			[]any{[]any{entry("k", int64(1))}}},
		{"map inside a map", mp("c", mp("", i64(""))),
			map[string]any{"k": map[string]any{"i": int64(1)}},
			[]any{entry("k", []any{entry("i", int64(1))})}},
		{"a NULL map inside a row stays NULL", row("c", i64("a"), mp("m", i64(""))),
			map[string]any{"a": int64(1), "m": nil},
			map[string]any{"a": int64(1), "m": nil}},
		{"an empty map inside a row is empty, not NULL", row("c", i64("a"), mp("m", i64(""))),
			map[string]any{"a": int64(1), "m": map[string]any{}},
			map[string]any{"a": int64(1), "m": []any{}}},
		// The MAP-free containers: unchanged, and not walked at all.
		{"row of row", row("c", i64("a"), row("s", i64("b"))),
			map[string]any{"a": int64(1), "s": map[string]any{"b": int64(2)}},
			map[string]any{"a": int64(1), "s": map[string]any{"b": int64(2)}}},
		{"row of array", row("c", i64("a"), arr("l", i64("element"))),
			map[string]any{"a": int64(1), "l": []any{int64(2)}},
			map[string]any{"a": int64(1), "l": []any{int64(2)}}},
		{"array of row", arr("c", row("element", i64("x"))),
			[]any{map[string]any{"x": int64(1)}},
			[]any{map[string]any{"x": int64(1)}}},
		{"map of array", mp("c", arr("", i64("element"))),
			map[string]any{"k": []any{int64(1)}},
			[]any{entry("k", []any{int64(1)})}},
		{"map of row", mp("c", row("", i64("x"))),
			map[string]any{"k": map[string]any{"x": int64(1)}},
			[]any{entry("k", map[string]any{"x": int64(1)})}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := FromRows([]parquet.Column{tt.col}, []map[string]any{{"c": tt.in}, {"c": nil}})
			if got := b.Columns[0].GetValue(0); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetValue(0) = %#v, want %#v", got, tt.want)
			}
			if got := b.Columns[0].GetValue(1); got != nil {
				t.Errorf("GetValue(1) = %#v, want nil", got)
			}
			// Idempotence: the storage shape the batch just produced must
			// survive a second pass, because ToRows→FromRows (the spill and
			// exchange round trip) hands exactly that back.
			again := FromRows([]parquet.Column{tt.col},
				[]map[string]any{{"c": b.Columns[0].GetValue(0)}})
			if got := again.Columns[0].GetValue(0); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("second pass over the storage shape = %#v, want %#v", got, tt.want)
			}
		})
	}
}
