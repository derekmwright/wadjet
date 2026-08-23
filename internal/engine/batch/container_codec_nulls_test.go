package batch

import (
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestDecodeContainerColumnGrowsTheNullBitmapWithLen pins Len and Nulls
// moving together.
//
// The payload does not carry the top-level column's nulls — WSHF writes that
// bitmap separately, ahead of the data — so the decoder only sets v.Len. A
// destination vector with LESS null-bitmap capacity than n then came back
// with Len = n over a shorter bitmap, and IsNull answers "null" past its end:
// every decoded row reads back NULL while the values sit right there in the
// vector. IsNullFast, which the hot paths use, indexes off the end of an
// empty one instead.
//
// A zero-length bitmap is not hypothetical: NewVectorLike, which the
// partitioned shuffle sink builds its container accumulators with
// (newShuffleAccumBatch), keeps the shape and drops the storage.
func TestDecodeContainerColumnGrowsTheNullBitmapWithLen(t *testing.T) {
	const rows = 3

	cases := []struct {
		name string
		col  parquet.Column
		vals []any
	}{
		{
			name: "array",
			col: parquet.Column{Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
			vals: []any{[]any{"a", "bb"}, []any{}, []any{"c"}},
		},
		{
			name: "vector",
			col:  parquet.Column{Name: "c_vec", Type: parquet.TypeVector, Nullable: true, Dimension: 2},
			vals: []any{[]float32{1.5, -2.5}, []float32{0, 0}, []float32{3, 4}},
		},
		{
			name: "row",
			col: parquet.Column{Name: "c_row", Type: parquet.TypeRow, Nullable: true,
				Fields: []parquet.Column{{Name: "f", Type: parquet.TypeInt64, Nullable: true}}},
			vals: []any{
				map[string]any{"f": int64(1)},
				map[string]any{"f": int64(2)},
				map[string]any{"f": int64(3)},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			src := NewColumnVector(tc.col, rows)
			for i, v := range tc.vals {
				src.SetValue(i, v)
			}
			payload, err := EncodeContainerColumn(nil, src, rows)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			// The destination carries the shape and NO storage — a
			// zero-length null bitmap, exactly what NewVectorLike produces.
			dst := NewVectorLike(src)
			if dst.Nulls.Len() != 0 {
				t.Fatalf("test setup: destination bitmap is already %d bits", dst.Nulls.Len())
			}
			if err := DecodeContainerColumn(payload, dst, rows); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if dst.Nulls.Len() < rows {
				t.Fatalf("Len is %d over a %d-bit null bitmap — every row past the end reads NULL",
					dst.Len, dst.Nulls.Len())
			}
			for i := 0; i < rows; i++ {
				if dst.Nulls.IsNull(i) {
					t.Fatalf("row %d decoded as NULL; the payload carried %v", i, tc.vals[i])
				}
				// IsNullFast is what the hot paths call: it has no bounds
				// check, so a short bitmap is an out-of-range index.
				if dst.Nulls.IsNullFast(i) {
					t.Fatalf("row %d reads NULL through IsNullFast", i)
				}
				if got := dst.GetValue(i); !reflect.DeepEqual(got, tc.vals[i]) {
					t.Errorf("row %d = %#v, want %#v", i, got, tc.vals[i])
				}
			}
		})
	}
}
