package worker

import (
	"hash/fnv"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// hashVectorValue exists to answer, for one row, the question the sink's
// bulk hashRowsIntoPartitions answers for a batch — the tests route with the
// former and the sink routes with the latter, so the two must mix the same
// byte stream arm for arm. FLOAT32 and FLOAT64 had no arm in hashVectorValue
// at all and fell to its default, which writes a single 0x00: it said "one
// partition" for every float value while the sink spread them across all of
// them. Found alongside #543.
func TestHashVectorValueMatchesBulkHashForFloats(t *testing.T) {
	const numParts = 8
	cases := []struct {
		name string
		col  parquet.Column
		vals []any
	}{
		{"float64", parquet.Column{Name: "k", Type: parquet.TypeFloat64},
			[]any{0.0, math.Copysign(0, -1), 1.5, -1.5, 3.25e10, math.NaN(), math.Inf(1), math.Inf(-1), math.MaxFloat64, math.SmallestNonzeroFloat64}},
		{"float32", parquet.Column{Name: "k", Type: parquet.TypeFloat32},
			[]any{float32(0), float32(math.Copysign(0, -1)), float32(1.5), float32(-1.5), float32(3.25e10), float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1))}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := []parquet.Column{tc.col}
			b := batch.NewRecordBatch(schema, len(tc.vals))
			for i, v := range tc.vals {
				b.Columns[0].SetValue(i, v)
			}

			want := make([]int, len(tc.vals))
			scratchU := make([]uint64, len(tc.vals))
			if !hashRowsIntoPartitions(b, []int{0}, numParts, scratchU, want) {
				t.Fatal("hashRowsIntoPartitions declined the batch")
			}

			var scratch [8]byte
			distinct := map[int]bool{}
			for i := range tc.vals {
				h := fnv.New64a()
				hashVectorValue(h, b.Columns[0], i, scratch[:])
				got := int(h.Sum64() % uint64(numParts))
				if got != want[i] {
					t.Errorf("row %d (%v): hashVectorValue routes to %d, the sink routes to %d",
						i, tc.vals[i], got, want[i])
				}
				distinct[got] = true
			}
			// The missing arm made every float land in ONE partition. If the
			// values above still collapse to one, the comparison above could
			// pass on a bug that simply broke both sides identically.
			if len(distinct) < 2 {
				t.Errorf("all %d float values routed to one partition", len(tc.vals))
			}
		})
	}
}

// -0.0 and +0.0 are one value, and so is every NaN payload
// (kernel/float_order.go). Both sides must fold them, or a shuffle join would
// separate rows the single-process join keys alike (#459).
func TestHashVectorValueFoldsFloatKeyCanonicalForms(t *testing.T) {
	schema := []parquet.Column{{Name: "k", Type: parquet.TypeFloat64}}
	b := batch.NewRecordBatch(schema, 4)
	b.Columns[0].SetValue(0, 0.0)
	b.Columns[0].SetValue(1, math.Copysign(0, -1))
	b.Columns[0].SetValue(2, math.NaN())
	b.Columns[0].SetValue(3, math.Float64frombits(math.Float64bits(math.NaN())|0x3))

	var scratch [8]byte
	sum := func(row int) uint64 {
		h := fnv.New64a()
		hashVectorValue(h, b.Columns[0], row, scratch[:])
		return h.Sum64()
	}
	if sum(0) != sum(1) {
		t.Error("+0.0 and -0.0 hash differently")
	}
	if sum(2) != sum(3) {
		t.Error("two NaN payloads hash differently")
	}
}
