package exec

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// buildGatherBatch builds one build-side batch of n string rows where row i
// holds "b<batch>-r<i>" except rows in nulls.
func buildGatherBatch(t *testing.T, batchID, n int, nulls map[int]bool) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{{Name: "s", Type: parquet.TypeString}}
	rb := batch.NewRecordBatch(schema, n)
	col := rb.Columns[0]
	for i := 0; i < n; i++ {
		if nulls[i] {
			col.Nulls.SetNull(i)
			col.BytesData.Set(i, nil)
			continue
		}
		col.BytesData.Set(i, fmt.Appendf(nil, "b%d-r%03d", batchID, i))
	}
	rb.Len = n
	return rb
}

// TestGatherBuildVectorBytesRuns pins the run-coalesced bytes gather (one
// BulkCopy per ascending-contiguous same-batch run) to the exact per-row
// semantics of the SetFrom loop it replaced, across the pair shapes hash
// joins produce. Output offsets must stay monotone with carry-forward on
// skipped rows.
func TestGatherBuildVectorBytesRuns(t *testing.T) {
	nulls0 := map[int]bool{5: true, 6: true, 20: true}
	b0 := buildGatherBatch(t, 0, 64, nulls0)
	b1 := buildGatherBatch(t, 1, 64, nil)
	builds := []*batch.RecordBatch{b0, b1}

	mp := func(bi, ri int, matched bool) matchPair {
		return matchPair{ref: buildRef{batchIdx: int32(bi), rowIdx: int32(ri)}, matched: matched}
	}
	cases := []struct {
		name       string
		pairs      []matchPair
		allMatched bool
	}{
		{"contiguous-run", []matchPair{mp(0, 10, true), mp(0, 11, true), mp(0, 12, true), mp(0, 13, true)}, true},
		{"run-broken-by-batch-switch", []matchPair{mp(0, 10, true), mp(0, 11, true), mp(1, 12, true), mp(1, 13, true)}, true},
		{"run-broken-by-source-null", []matchPair{mp(0, 3, true), mp(0, 4, true), mp(0, 5, true), mp(0, 6, true), mp(0, 7, true), mp(0, 8, true)}, true},
		{"run-broken-by-unmatched", []matchPair{mp(0, 10, true), mp(0, 11, true), mp(0, 0, false), mp(0, 12, true), mp(0, 13, true)}, false},
		{"scattered-singletons", []matchPair{mp(0, 40, true), mp(1, 2, true), mp(0, 8, true), mp(1, 63, true)}, true},
		{"repeated-same-row-n-to-1", []matchPair{mp(0, 30, true), mp(0, 30, true), mp(0, 30, true), mp(0, 31, true)}, true},
		{"descending-rows", []matchPair{mp(0, 13, true), mp(0, 12, true), mp(0, 11, true)}, true},
		{"leading-null-then-run", []matchPair{mp(0, 5, true), mp(0, 7, true), mp(0, 8, true), mp(0, 9, true)}, true},
		{"all-unmatched", []matchPair{mp(0, 0, false), mp(0, 0, false)}, false},
		{"empty-pairs", nil, true},
		{"cross-batch-interleave", []matchPair{mp(0, 1, true), mp(1, 1, true), mp(0, 2, true), mp(1, 2, true)}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := []parquet.Column{{Name: "s", Type: parquet.TypeString}}
			out := batch.NewRecordBatch(schema, len(tc.pairs))
			dst := out.Columns[0]
			gatherBuildVector(dst, 0, tc.pairs, builds, tc.allMatched)

			prevEnd := uint32(0)
			for di, pair := range tc.pairs {
				if dst.BytesData.Offsets[di+1] < prevEnd {
					t.Fatalf("row %d: offsets not monotone: %d < %d", di, dst.BytesData.Offsets[di+1], prevEnd)
				}
				prevEnd = dst.BytesData.Offsets[di+1]

				src := builds[pair.ref.batchIdx].Columns[0]
				si := int(pair.ref.rowIdx)
				wantNull := (!tc.allMatched && !pair.matched) || src.Nulls.IsNull(si)
				if wantNull {
					if !dst.Nulls.IsNull(di) {
						t.Fatalf("row %d: expected null", di)
					}
					if got := dst.BytesData.Value(di); len(got) != 0 {
						t.Fatalf("null row %d: non-empty value %q", di, got)
					}
					continue
				}
				if dst.Nulls.IsNull(di) {
					t.Fatalf("row %d: unexpected null", di)
				}
				want := src.BytesData.Value(si)
				if got := dst.BytesData.Value(di); !bytes.Equal(got, want) {
					t.Fatalf("row %d: got %q want %q", di, got, want)
				}
			}
		})
	}
}

// BenchmarkGatherBuildVectorBytes measures the bytes gather across the three
// production pair layouts: clustered (fact-side build, ~4-row ascending runs
// — the Q17/Q18/Q21 class), scattered (dimension build, hash-random rows),
// and repeated (N:1 same-row matches). ~27 B values, l_comment scale.
func BenchmarkGatherBuildVectorBytes(b *testing.B) {
	const buildRows = 8192
	schema := []parquet.Column{{Name: "s", Type: parquet.TypeString}}
	build := batch.NewRecordBatch(schema, buildRows)
	for i := 0; i < buildRows; i++ {
		build.Columns[0].BytesData.Set(i, fmt.Appendf(nil, "supplier comment padding i=%05d", i))
	}
	build.Len = buildRows
	builds := []*batch.RecordBatch{build}

	mkPairs := func(kind string) []matchPair {
		pairs := make([]matchPair, 0, 4096)
		switch kind {
		case "clustered":
			for base := 0; len(pairs) < 4096; base = (base + 24) % (buildRows - 8) {
				for k := 0; k < 4; k++ {
					pairs = append(pairs, matchPair{ref: buildRef{rowIdx: int32(base + k)}, matched: true})
				}
			}
		case "scattered":
			for i := 0; len(pairs) < 4096; i++ {
				pairs = append(pairs, matchPair{ref: buildRef{rowIdx: int32((i * 2654435761) % buildRows)}, matched: true})
			}
		case "repeated":
			for i := 0; len(pairs) < 4096; i++ {
				pairs = append(pairs, matchPair{ref: buildRef{rowIdx: int32((i / 4) % buildRows)}, matched: true})
			}
		}
		return pairs[:4096]
	}

	for _, kind := range []string{"clustered", "scattered", "repeated"} {
		b.Run(kind, func(b *testing.B) {
			pairs := mkPairs(kind)
			out := batch.NewRecordBatch(schema, len(pairs))
			b.SetBytes(int64(len(pairs)) * 33)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out.Columns[0].BytesData.Data = out.Columns[0].BytesData.Data[:0]
				gatherBuildVector(out.Columns[0], 0, pairs, builds, true)
			}
		})
	}
}
