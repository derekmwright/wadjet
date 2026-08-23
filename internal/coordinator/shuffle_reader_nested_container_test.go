package coordinator

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #425, coordinator half. readShuffleColumn is an independent copy of the
// WSHF walk, and it is the one that reads a stage's output back for the
// client — so the container's own null bit has to survive HERE too, and here
// is where it is easiest to lose: the top-level bitmap is copied into
// vec.Nulls.Words() BEFORE DecodeContainerColumn runs, and the decode ends
// with v.Nulls.Grow(n). A Grow that widened the bitmap over the copied words,
// or a decode that reset it, turns every NULL container into a present one
// made of nothing.
//
// The fixture nests: an EMPTY inner list next to a NULL inner list, a
// present ROW whose nested ROW is NULL, and a MAP entry whose value list is
// NULL rather than empty. Each of those encodes with the same offsets as its
// twin, so only a bitmap separates them.
func TestShuffleReaderNestedContainerNulls(t *testing.T) {
	const rows = 4

	schema := []parquet.Column{
		{Name: "n_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}}},
		{Name: "n_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "k", Type: parquet.TypeString, Nullable: true},
			{Name: "inner", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
				{Name: "x", Type: parquet.TypeInt64, Nullable: true},
			}},
		}},
		{Name: "n_map", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeArray, Nullable: true,
					ElementType: &parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}},
			}}},
	}
	want := map[string][]any{
		"n_arr": {
			[]any{[]any{"a"}, []any{}},  // EMPTY inner beside a live one
			[]any{},                     // EMPTY outer
			nil,                         // NULL outer
			[]any{nil, []any{"c", nil}}, // NULL inner, then a null leaf
		},
		"n_row": {
			map[string]any{"k": "x", "inner": map[string]any{"x": int64(1)}},
			map[string]any{"k": nil, "inner": nil}, // present ROW, NULL nested ROW
			nil,                                    // NULL ROW
			map[string]any{"k": "", "inner": map[string]any{"x": nil}},
		},
		"n_map": {
			[]any{map[string]any{"key": "a", "value": []any{int64(1)}}},
			[]any{}, // EMPTY map
			nil,     // NULL map
			[]any{map[string]any{"key": "b", "value": nil}}, // NULL value list
		},
	}

	cols := make([]*batch.Vector, len(schema))
	for ci, col := range schema {
		v := batch.NewColumnVector(col, rows)
		for i := 0; i < rows; i++ {
			v.SetValue(i, want[col.Name][i])
		}
		cols[ci] = v
	}

	// WSHF: magic | numChunks | numCols | (nameLen, name, type)* |
	//       numRows | (bitmapWords, bitmap, dataLen, data)*
	var buf []byte
	buf = append(buf, 'W', 'S', 'H', 'F')
	buf = binary.LittleEndian.AppendUint32(buf, 1)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(schema)))
	for _, c := range schema {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(c.Name)))
		buf = append(buf, c.Name...)
		buf = append(buf, byte(c.Type))
	}
	buf = binary.LittleEndian.AppendUint32(buf, rows)
	for _, col := range cols {
		var word uint64
		for i := 0; i < rows; i++ {
			if !col.Nulls.IsNull(i) {
				word |= 1 << uint(i)
			}
		}
		buf = binary.LittleEndian.AppendUint32(buf, 1)
		buf = binary.LittleEndian.AppendUint64(buf, word)
		payload, err := batch.EncodeContainerColumn(nil, col, rows)
		if err != nil {
			t.Fatalf("encoding %v: %v", col.Type, err)
		}
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(payload)))
		buf = append(buf, payload...)
	}

	batches, err := readShuffleBatches(buf)
	if err != nil {
		t.Fatalf("readShuffleBatches: %v", err)
	}
	if len(batches) != 1 || batches[0].Len != rows {
		t.Fatalf("got %d batches", len(batches))
	}
	for ci, col := range schema {
		gc := batches[0].Columns[ci]
		for i := 0; i < rows; i++ {
			w := want[col.Name][i]
			if (w == nil) != gc.Nulls.IsNull(i) {
				t.Errorf("%s row %d: null=%v, want null=%v", col.Name, i, gc.Nulls.IsNull(i), w == nil)
				continue
			}
			if g := gc.GetValue(i); !reflect.DeepEqual(g, w) {
				t.Errorf("%s row %d: got %#v, want %#v", col.Name, i, g, w)
			}
		}
	}
}
