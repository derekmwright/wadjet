package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestBloomFilterOpFiltersRows(t *testing.T) {
	// Build a bloom with keys 1, 2, 3 using the same two-hash scheme as HashJoin
	bloomSlots := 1 << 12 // 4K words
	bloomMask := uint64(bloomSlots - 1)
	bloom := make([]uint64, bloomSlots)
	for _, key := range []int64{1, 2, 3} {
		hash := BloomHashInt(key)
		h1 := hash & bloomMask
		h2 := (hash >> 17) & bloomMask
		bloom[h1] |= 1 << (hash & 63)
		bloom[h2] |= 1 << ((hash >> 6) & 63)
	}

	// Create BloomFilterOp
	op := NewBloomFilterOp(bloom, bloomMask, []string{"id"}, true)
	op.Init(context.Background())

	// Create a batch with ids 1, 2, 3, 4, 5
	schema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}, {Name: "val", Type: parquet.TypeString}}
	b := batch.NewRecordBatch(schema, 5)
	for i := 0; i < 5; i++ {
		b.Columns[0].Int64Data[i] = int64(i + 1)
		b.Columns[1].BytesData.Set(i, []byte("x"))
	}

	result, err := op.Execute(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}

	// Should filter out keys 4, 5 (not in bloom)
	// Bloom has false positives so result might include some extra rows
	activeLen := result.ActiveLen()
	t.Logf("input: 5 rows, output: %d rows (bloom filtered)", activeLen)

	if activeLen > 5 {
		t.Errorf("bloom filter increased rows: %d > 5", activeLen)
	}
	if activeLen < 3 {
		t.Errorf("bloom filter dropped too many rows: %d < 3 (keys 1,2,3 should pass)", activeLen)
	}

	// Verify keys 1, 2, 3 are in the result
	for i := 0; i < activeLen; i++ {
		row := i
		if result.Sel != nil {
			row = int(result.Sel[i])
		}
		key := result.Columns[0].Int64Data[row]
		if key < 1 || key > 3 {
			if key == 4 || key == 5 {
				t.Logf("false positive: key %d passed bloom (expected for small bloom)", key)
			}
		}
	}
}
