package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestBloomFilterAdaptiveDisableWindow verifies that the bloom filter adaptive
// disable threshold uses 32 batches, not 8. With only 8 batches, partition
// skew at large scale factors (e.g., SF100) can cause all early batches to
// come from the same key range, making the bloom appear useless and disabling
// it permanently -- even though later batches would benefit from filtering.
func TestBloomFilterAdaptiveDisableWindow(t *testing.T) {
	// Build a bloom filter with a single key (key=42).
	bloomSlots := uint64(1024)
	bloom := make([]uint64, bloomSlots)
	bloomMask := bloomSlots - 1
	hash := BloomHashInt(42)
	h1 := hash & bloomMask
	h2 := (hash >> 17) & bloomMask
	bloom[h1] |= 1 << (hash & 63)
	bloom[h2] |= 1 << ((hash >> 6) & 63)

	op := &BloomFilterOp{
		bloom:     bloom,
		bloomMask: bloomMask,
		leftKeys:  []string{"id"},
		useIntKey: true,
	}

	schema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}

	// Send 8 batches where ALL rows match (0% rejection).
	// With the old threshold of 8, this would disable the filter.
	for i := 0; i < 8; i++ {
		b := batch.NewRecordBatch(schema, 1)
		b.Columns[0].Int64Data = []int64{42}
		b.Len = 1
		_, err := op.Execute(context.Background(), b)
		if err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}

	// After 8 batches with 0% rejection, the filter must NOT be disabled yet.
	if op.disabled {
		t.Fatal("bloom filter disabled after only 8 batches; sample window should be 32")
	}

	// Continue to batch 32 -- still all matching rows.
	for i := 8; i < 32; i++ {
		b := batch.NewRecordBatch(schema, 1)
		b.Columns[0].Int64Data = []int64{42}
		b.Len = 1
		_, err := op.Execute(context.Background(), b)
		if err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}

	// After 32 batches with 0% rejection, NOW it should disable.
	if !op.disabled {
		t.Fatal("bloom filter should be disabled after 32 batches with <5% rejection")
	}
}

// TestBloomFilterAdaptiveKeepsActive verifies that the bloom filter stays
// active when it's actually filtering rows effectively (>5% rejection).
func TestBloomFilterAdaptiveKeepsActive(t *testing.T) {
	// Build a bloom filter with a single key (key=42).
	bloomSlots := uint64(1024)
	bloom := make([]uint64, bloomSlots)
	bloomMask := bloomSlots - 1
	hash := BloomHashInt(42)
	h1 := hash & bloomMask
	h2 := (hash >> 17) & bloomMask
	bloom[h1] |= 1 << (hash & 63)
	bloom[h2] |= 1 << ((hash >> 6) & 63)

	op := &BloomFilterOp{
		bloom:     bloom,
		bloomMask: bloomMask,
		leftKeys:  []string{"id"},
		useIntKey: true,
	}

	schema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}

	// Send 40 batches where half the rows don't match (50% rejection).
	for i := 0; i < 40; i++ {
		b := batch.NewRecordBatch(schema, 2)
		b.Columns[0].Int64Data = []int64{42, 999} // 999 not in bloom
		b.Len = 2
		_, err := op.Execute(context.Background(), b)
		if err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}

	// With 50% rejection rate, the filter should stay active.
	if op.disabled {
		t.Fatal("bloom filter disabled despite 50% rejection rate; should stay active")
	}
}
