package worker

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// buildBytesBatch builds a single-string-column batch where row i holds
// value fmt("v%03d", i) except rows in nulls, which are null. When
// malformedNulls is set, null rows get the gather-hazard shape instead
// of a clean zero-length value: Offsets[i+1] == 0 while Offsets[i] > 0
// (see BytesColumn.Value) — appendBatchRowsBulk must tolerate reading
// PAST such a row's offsets without corrupting neighbors.
func buildBytesBatch(t *testing.T, n int, nulls map[int]bool, malformedNulls bool) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{{Name: "s", Type: parquet.TypeString}}
	rb := batch.NewRecordBatch(schema, n)
	col := rb.Columns[0]
	for i := 0; i < n; i++ {
		if nulls[i] {
			col.Nulls.SetNull(i)
			if malformedNulls {
				// Emulate the gather skip: no Set call at all — the
				// pre-sized Offsets slot keeps its zero value.
				col.BytesData.Offsets[i+1] = 0
			} else {
				col.BytesData.Set(i, nil)
			}
			continue
		}
		col.BytesData.Set(i, []byte(fmt.Sprintf("v%03d", i)))
	}
	rb.Len = n
	return rb
}

// TestAppendBatchRowsBulk_BytesRuns pins the bytes-arm run-copy rewrite
// to the exact per-row semantics of the old SetFrom loop across the row
// shapes the two sinks produce.
func TestAppendBatchRowsBulk_BytesRuns(t *testing.T) {
	const n = 64
	nulls := map[int]bool{5: true, 6: true, 20: true, 63: true}
	cases := []struct {
		name      string
		rows      []uint32
		nulls     map[int]bool
		malformed bool
	}{
		{"dense-no-nulls", seq(0, n), nil, false},
		{"dense-with-nulls", seq(0, n), nulls, false},
		{"dense-malformed-null-offsets", seq(0, n), nulls, true},
		{"scattered-singletons", []uint32{1, 3, 7, 9, 30, 62}, nil, false},
		{"clustered-runs", []uint32{0, 1, 2, 3, 10, 11, 12, 40, 41, 60}, nil, false},
		{"runs-broken-by-nulls", []uint32{4, 5, 6, 7, 8, 19, 20, 21}, nulls, false},
		{"runs-broken-by-malformed-nulls", []uint32{4, 5, 6, 7, 8, 19, 20, 21}, nulls, true},
		{"run-ending-before-null", []uint32{3, 4, 18, 19}, nulls, true},
		{"single-row", []uint32{42}, nil, false},
		{"empty-rows", nil, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := buildBytesBatch(t, n, tc.nulls, tc.malformed)
			dst := batch.NewRecordBatch([]parquet.Column{{Name: "s", Type: parquet.TypeString}}, 0)
			// Seed the destination so appends land mid-buffer, not at 0.
			growBatchTo(dst, 1)
			dst.Columns[0].BytesData.Set(0, []byte("seed"))
			dst.Len = 1

			added := appendBatchRowsBulk(dst, src, tc.rows)

			col, scol := dst.Columns[0], src.Columns[0]
			if dst.Len != 1+len(tc.rows) {
				t.Fatalf("dst.Len = %d, want %d", dst.Len, 1+len(tc.rows))
			}
			if got := col.BytesData.Value(0); string(got) != "seed" {
				t.Fatalf("seed row corrupted: %q", got)
			}
			wantAdded := 0
			for i, row := range tc.rows {
				di := 1 + i
				if tc.nulls[int(row)] {
					if !col.Nulls.IsNull(di) {
						t.Fatalf("row %d (src %d): expected null", di, row)
					}
					if got := col.BytesData.Value(di); len(got) != 0 {
						t.Fatalf("null row %d: non-empty value %q", di, got)
					}
					continue
				}
				want := scol.BytesData.Value(int(row))
				if got := col.BytesData.Value(di); !bytes.Equal(got, want) {
					t.Fatalf("row %d (src %d): got %q want %q", di, row, got, want)
				}
				wantAdded += len(want) + 4
			}
			if added != wantAdded {
				t.Fatalf("bytesAdded = %d, want %d", added, wantAdded)
			}
		})
	}
}

func seq(lo, hi int) []uint32 {
	out := make([]uint32, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, uint32(i))
	}
	return out
}
