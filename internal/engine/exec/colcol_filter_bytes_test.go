package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// twoBytesBatch holds two BYTES columns in the shape the pg-oracle's
// bytea_probe fixture has: equal on one row, ordered either way on the
// others, a value whose bytes are not valid UTF-8, and a NULL on each side.
func twoBytesBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	left := [][]byte{[]byte("hi"), []byte("hi"), {0xff, 0xfe, 0x00, 0x41}, nil, {}}
	right := [][]byte{[]byte("hi"), []byte("hi there"), []byte("A"), {}, nil}
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "b_val", Type: parquet.TypeBytes, Nullable: true},
		{Name: "b_other", Type: parquet.TypeBytes, Nullable: true},
	}, len(left))
	for i := range left {
		b.Columns[0].BytesData.Set(i, left[i])
		b.Columns[1].BytesData.Set(i, right[i])
	}
	b.Columns[0].Nulls.SetNull(3)
	b.Columns[1].Nulls.SetNull(4)
	return b
}

// #570: ResolveColColFilterKernel's string arm listed STRING, IPv6 and UUID
// but not BYTES, even though all four are stored in BytesData and PostgreSQL
// compares two bytea values BYTEWISE. Two BYTES columns share a TypeID, so
// ColColFilter's row-at-a-time fallback — attached only when the two column
// TYPES differ (#375's mixed-type guard) — did not apply either, and
// `WHERE b_val = b_other` FAILED THE QUERY:
//
//	ColColFilter: could not resolve kernel for b_val 0 b_other
//
// for every operator, the identical shape #477 found for two DECIMALs.
// Found by the bytea corpus #570 added to the PostgreSQL oracle, which is
// the first thing in this repository ever to compare two BYTES columns.
func TestColColFilterComparesTwoBytesColumns(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		op   CompareOp
		want []uint32
	}{
		// Row 1 is "hi" against "hi there": a PREFIX sorts BELOW the longer
		// value, which is the one ordering a length-first comparison gets
		// wrong. Row 2 is 0xff... against "A" (0x41), which is where a
		// signed byte comparison would flip the answer.
		{"eq", OpEq, []uint32{0}},
		{"ne", OpNe, []uint32{1, 2}},
		{"lt", OpLt, []uint32{1}},
		{"le", OpLe, []uint32{0, 1}},
		{"gt", OpGt, []uint32{2}},
		{"ge", OpGe, []uint32{0, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := twoBytesBatch(t)
			out, err := NewColColFilter("b_val", "b_other", tc.op).Execute(ctx, in)
			if err != nil {
				t.Fatalf("%v", err)
			}
			var got []uint32
			if out != nil {
				got = out.Sel
			}
			if len(got) != len(tc.want) {
				t.Fatalf("selected %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("selected %v, want %v", got, tc.want)
				}
			}
		})
	}
}
