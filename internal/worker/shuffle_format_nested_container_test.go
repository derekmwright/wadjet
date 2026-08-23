package worker

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #425, wire half: a NULL container and an EMPTY (or all-null-contents)
// container are two different values, and the only thing that tells them
// apart on the WSHF wire is the column's own null bitmap — the payload codec
// deliberately does not carry it (container_codec.go's header note). The flat
// four-type fixture next door proves that for one level; this one proves it
// for containers INSIDE containers, where a "present but made of nothing"
// value has three more places to be invented:
//
//	ARRAY(ARRAY)         an empty inner list vs a NULL inner list
//	ROW(.., ROW, ARRAY)  a present ROW whose nested ROW is NULL
//	MAP(STRING, ARRAY)   an entry whose VALUE list is NULL vs empty
//
// All three encode with identical offsets to their non-null twin, so a codec
// that dropped a child bitmap — or a WSHF reader that grew the top-level one
// over the payload's — collapses them.
//
// Both worker readers run (eager slice + chunk reader); the coordinator's
// independent copy of the same walk is covered by
// internal/coordinator.TestShuffleReaderNestedContainerNulls.

const nestedContainerRows = 5

func nestedContainerSchema() []parquet.Column {
	return []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		// ARRAY of ARRAY: the inner list's own nulls live in the child
		// vector's bitmap, the outer's in the column bitmap.
		{Name: "n_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}}},
		// ROW carrying a nested ROW and an ARRAY.
		{Name: "n_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "k", Type: parquet.TypeString, Nullable: true},
			{Name: "inner", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
				{Name: "x", Type: parquet.TypeInt64, Nullable: true},
				{Name: "tags", Type: parquet.TypeArray, Nullable: true,
					ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
			}},
		}},
		// MAP whose VALUE is a list.
		{Name: "n_map", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeArray, Nullable: true,
					ElementType: &parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}},
			}}},
		{Name: "n_vec", Type: parquet.TypeVector, Nullable: true, Dimension: 3},
	}
}

// nestedContainerValues is the GetValue form of every row of every column.
// Row 2 is the NULL row of all four; row 1 is the EMPTY / null-contents twin
// that must not collapse into it.
func nestedContainerValues() map[string][]any {
	return map[string][]any{
		"id": {int64(0), int64(1), int64(2), int64(3), int64(4)},
		"n_arr": {
			[]any{[]any{"a"}, []any{}},  // an EMPTY inner beside a non-empty one
			[]any{},                     // EMPTY outer, not null
			nil,                         // NULL outer
			[]any{nil, []any{"c", nil}}, // NULL inner, then a null LEAF inside a live inner
			[]any{[]any{}},              // a single empty inner
		},
		"n_row": {
			map[string]any{"k": "x", "inner": map[string]any{"x": int64(1), "tags": []any{"t1"}}},
			map[string]any{"k": nil, "inner": nil}, // present ROW, NULL nested ROW
			nil,                                    // NULL ROW
			map[string]any{"k": "", "inner": map[string]any{"x": nil, "tags": []any{}}},
			map[string]any{"k": "z", "inner": map[string]any{"x": int64(-1), "tags": nil}},
		},
		"n_map": {
			[]any{map[string]any{"key": "a", "value": []any{int64(1), int64(2)}}},
			[]any{}, // EMPTY map, not null
			nil,     // NULL map
			[]any{map[string]any{"key": "b", "value": nil}},     // NULL value list
			[]any{map[string]any{"key": "c", "value": []any{}}}, // EMPTY value list
		},
		"n_vec": {
			[]float32{1, 2, 3},
			[]float32{0, 0, 0}, // all-zero is NOT null
			nil,
			[]float32{-1.5, 2.5, 3.5},
			[]float32{1e-8, 0, 1e8},
		},
	}
}

func buildNestedContainerBatch(tb testing.TB) *batch.RecordBatch {
	tb.Helper()
	schema := nestedContainerSchema()
	vals := nestedContainerValues()
	b := batch.NewRecordBatch(schema, nestedContainerRows)
	for ci, col := range schema {
		column := vals[col.Name]
		for i := 0; i < nestedContainerRows; i++ {
			b.Columns[ci].SetValue(i, column[i])
		}
	}
	// The fixture is only worth what the builder round-trips: assert the
	// SOURCE batch before it ever reaches the wire, so a SetValue/GetValue
	// gap cannot be read as a codec pass.
	assertNestedContainerBatch(tb, b, nil, "source batch")
	return b
}

func assertNestedContainerBatch(tb testing.TB, got *batch.RecordBatch, sel []uint32, what string) {
	tb.Helper()
	schema := nestedContainerSchema()
	vals := nestedContainerValues()
	rows := sel
	if rows == nil {
		rows = make([]uint32, nestedContainerRows)
		for i := range rows {
			rows[i] = uint32(i)
		}
	}
	if got.Len != len(rows) {
		tb.Fatalf("%s: %d rows, want %d", what, got.Len, len(rows))
	}
	for ci, col := range schema {
		want := vals[col.Name]
		gc := got.Columns[ci]
		if gc.Type != col.Type {
			tb.Errorf("%s: column %s decoded as %v, want %v", what, col.Name, gc.Type, col.Type)
			continue
		}
		for i, src := range rows {
			w := want[src]
			if (w == nil) != gc.Nulls.IsNull(i) {
				tb.Errorf("%s: %s row %d (src %d): null=%v, want null=%v",
					what, col.Name, i, src, gc.Nulls.IsNull(i), w == nil)
				continue
			}
			if g := gc.GetValue(i); !reflect.DeepEqual(w, g) {
				tb.Errorf("%s: %s row %d (src %d): got %#v, want %#v", what, col.Name, i, src, g, w)
			}
		}
	}
}

func TestShuffleNestedContainerNullVsEmpty(t *testing.T) {
	b := buildNestedContainerBatch(t)
	// The selection puts the EMPTY row, the NULL row and the
	// nested-null row next to each other, so the gather path in
	// writeContainerData sees all three.
	sel := []uint32{1, 2, 3}

	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, nestedContainerSchema())
	if err := sw.writeHeader(); err != nil {
		t.Fatalf("writeHeader: %v", err)
	}
	if err := sw.writeChunk(b.Columns, sel, len(sel)); err != nil {
		t.Fatalf("writeChunk (selected): %v", err)
	}
	if err := sw.writeChunk(b.Columns, nil, nestedContainerRows); err != nil {
		t.Fatalf("writeChunk (all rows): %v", err)
	}
	data := buf.Bytes()
	binary.LittleEndian.PutUint32(data[4:], sw.numChunks)

	t.Run("eager", func(t *testing.T) {
		batches, err := shuffleReadBatches(data)
		if err != nil {
			t.Fatalf("shuffleReadBatches: %v", err)
		}
		if len(batches) != 2 {
			t.Fatalf("got %d batches, want 2", len(batches))
		}
		assertNestedContainerBatch(t, batches[0], sel, "eager selected chunk")
		assertNestedContainerBatch(t, batches[1], nil, "eager full chunk")
	})

	t.Run("chunk_reader", func(t *testing.T) {
		r, err := newShuffleChunkReader(data)
		if err != nil {
			t.Fatalf("newShuffleChunkReader: %v", err)
		}
		var got []*batch.RecordBatch
		for {
			rb, err := r.Next()
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			if rb == nil {
				break
			}
			got = append(got, rb)
		}
		if len(got) != 2 {
			t.Fatalf("got %d chunks, want 2", len(got))
		}
		assertNestedContainerBatch(t, got[0], sel, "chunk reader selected chunk")
		assertNestedContainerBatch(t, got[1], nil, "chunk reader full chunk")
	})
}

// TestNestedContainerCodecKeepsChildNulls goes straight at the payload
// codec, with the top-level bitmap supplied the way WSHF supplies it. It is
// the narrower statement: everything the PAYLOAD is responsible for — inner
// list nulls, a nested ROW's own null, a MAP value list's null — survives
// encode/decode on its own, so a WSHF failure is a framing failure and not a
// codec one.
func TestNestedContainerCodecKeepsChildNulls(t *testing.T) {
	src := buildNestedContainerBatch(t)
	schema := nestedContainerSchema()
	for ci, col := range schema {
		if !batch.IsContainerType(col.Type) {
			continue
		}
		payload, err := batch.EncodeContainerColumn(nil, src.Columns[ci], nestedContainerRows)
		if err != nil {
			t.Fatalf("%s: encode: %v", col.Name, err)
		}
		dst := batch.NewColumnVector(parquet.Column{Name: col.Name, Type: col.Type}, nestedContainerRows)
		if err := batch.DecodeContainerColumn(payload, dst, nestedContainerRows); err != nil {
			t.Fatalf("%s: decode: %v", col.Name, err)
		}
		// WSHF's separate top-level bitmap, applied the way the readers do.
		for i := 0; i < nestedContainerRows; i++ {
			if src.Columns[ci].Nulls.IsNull(i) {
				dst.Nulls.SetNull(i)
			}
		}
		want := nestedContainerValues()[col.Name]
		for i := 0; i < nestedContainerRows; i++ {
			if got := dst.GetValue(i); !reflect.DeepEqual(got, want[i]) {
				t.Errorf("%s row %d: got %#v, want %#v", col.Name, i, got, want[i])
			}
		}
	}
}
