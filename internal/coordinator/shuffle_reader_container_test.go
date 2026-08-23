package coordinator

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #397, coordinator half: readShuffleColumn is an independent copy of the
// WSHF walk (see the note on readShuffleBatches), so an encoder arm added on
// the worker side is only half a fix — the coordinator reads stage output
// too, and it refused ARRAY/ROW/MAP/VECTOR with "unsupported type".
//
// The payload bytes come from batch.EncodeContainerColumn, the same codec
// the worker's writer uses, so this test breaks if the coordinator's framing
// (the u32 length prefix and the dispatch around it) drifts from the
// worker's.
func TestShuffleReaderContainerColumns(t *testing.T) {
	const rows = 3
	// c_arr: ["a","bb"] / [] (EMPTY, not null) / NULL.
	// c_vec: a 2-dimension VECTOR whose dim exists nowhere but the payload.
	arr := batch.NewColumnVector(parquet.Column{
		Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
		ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true},
	}, rows)
	arr.SetValue(0, []any{"a", "bb"})
	arr.SetValue(1, []any{})
	arr.SetValue(2, nil)

	vec := batch.NewColumnVector(parquet.Column{
		Name: "c_vec", Type: parquet.TypeVector, Nullable: true, Dimension: 2,
	}, rows)
	vec.SetValue(0, []float32{1.5, -2.5})
	vec.SetValue(1, []float32{0, 0})
	vec.SetValue(2, nil)

	cols := []*batch.Vector{arr, vec}
	schema := []parquet.Column{
		{Name: "c_arr", Type: parquet.TypeArray},
		{Name: "c_vec", Type: parquet.TypeVector},
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
		// One bitmap word: bit set = VALID (wadjet convention).
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
	gotArr, gotVec := batches[0].Columns[0], batches[0].Columns[1]

	wantArr := []any{[]any{"a", "bb"}, []any{}, nil}
	for i, want := range wantArr {
		if got := gotArr.GetValue(i); !reflect.DeepEqual(got, want) {
			t.Errorf("c_arr row %d: got %#v, want %#v", i, got, want)
		}
	}
	if gotArr.Nulls.IsNull(1) {
		t.Error("the EMPTY array decoded as NULL")
	}
	if !gotArr.Nulls.IsNull(2) {
		t.Error("the NULL array decoded as non-null")
	}

	if gotVec.VectorDim != 2 {
		t.Errorf("c_vec dimension %d, want 2", gotVec.VectorDim)
	}
	wantVec := []any{[]float32{1.5, -2.5}, []float32{0, 0}, nil}
	for i, want := range wantVec {
		if got := gotVec.GetValue(i); !reflect.DeepEqual(got, want) {
			t.Errorf("c_vec row %d: got %#v, want %#v", i, got, want)
		}
	}
}
