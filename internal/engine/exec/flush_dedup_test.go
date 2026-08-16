package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// buildWithArenaMatched calls BuildFromRows and ensures arenaMatched is allocated.
// BuildFromRows only allocates arenaMatched for RightJoin/FullOuterJoin, but
// RightSemiJoin/RightAntiJoin need it for markKeyMatched. In production, the
// full Build() method handles this, but BuildFromRows is a test convenience.
func buildWithArenaMatched(hj *HashJoin, schema []parquet.Column, rows []map[string]any) {
	hj.BuildFromRows(schema, rows)
	if (hj.JoinType == RightSemiJoin || hj.JoinType == RightAntiJoin) && len(hj.arena) > 0 {
		hj.arenaMatched = make([]bool, len(hj.arena))
	}
}

// TestFlushMatchedDedup verifies that FlushMatched deduplicates build rows
// when multiple probe rows match the same build key. Each matching probe row
// marks the same arena entry chain, but we should only emit each unique
// (batchIdx, rowIdx) once.
func TestFlushMatchedDedup(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "val", Type: parquet.TypeString},
	}

	hj := NewHashJoin(RightSemiJoin, []string{"id"}, []string{"id"})
	buildWithArenaMatched(hj, buildSchema, []map[string]any{
		{"id": int32(1), "val": "a"},
		{"id": int32(2), "val": "b"},
		{"id": int32(3), "val": "c"},
	})

	probeSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
	}
	// Multiple probe rows match build key=1, which should NOT produce
	// duplicate build rows in the output.
	probeBatch := batch.FromRows(probeSchema, []map[string]any{
		{"id": int32(1)},
		{"id": int32(1)}, // duplicate match
		{"id": int32(1)}, // another duplicate
		{"id": int32(2)},
	})

	probe := hj.Probe()
	_, err := probe.Execute(context.Background(), probeBatch)
	if err != nil {
		t.Fatalf("probe execute: %v", err)
	}

	result := probe.FlushMatched()
	if result == nil {
		t.Fatal("expected non-nil FlushMatched result")
	}

	// Should have exactly 2 unique build rows (id=1 and id=2), not 4
	if result.Len != 2 {
		t.Fatalf("expected 2 deduplicated rows, got %d", result.Len)
	}

	rows := result.ToRows()
	ids := map[int32]bool{}
	for _, r := range rows {
		ids[r["id"].(int32)] = true
	}
	if !ids[1] || !ids[2] {
		t.Fatalf("expected ids {1, 2}, got %v", ids)
	}
}

// TestFlushAntiMatchedDedup verifies that FlushAntiMatched deduplicates
// unmatched build rows when the arena has multiple entries per build row
// (hash chain collisions or duplicate keys).
func TestFlushAntiMatchedDedup(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "val", Type: parquet.TypeString},
	}

	hj := NewHashJoin(RightAntiJoin, []string{"id"}, []string{"id"})
	buildWithArenaMatched(hj, buildSchema, []map[string]any{
		{"id": int32(1), "val": "a"},
		{"id": int32(2), "val": "b"},
		{"id": int32(3), "val": "c"},
	})

	probeSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
	}
	// Probe with id=1 only -- ids 2 and 3 should appear in anti output
	probeBatch := batch.FromRows(probeSchema, []map[string]any{
		{"id": int32(1)},
		{"id": int32(1)}, // duplicate to exercise dedup path
	})

	probe := hj.Probe()
	_, err := probe.Execute(context.Background(), probeBatch)
	if err != nil {
		t.Fatalf("probe execute: %v", err)
	}

	result := probe.FlushAntiMatched()
	if result == nil {
		t.Fatal("expected non-nil FlushAntiMatched result")
	}

	// Should have exactly 2 unmatched build rows (id=2 and id=3)
	if result.Len != 2 {
		t.Fatalf("expected 2 unmatched rows, got %d", result.Len)
	}

	rows := result.ToRows()
	ids := map[int32]bool{}
	for _, r := range rows {
		ids[r["id"].(int32)] = true
	}
	if !ids[2] || !ids[3] {
		t.Fatalf("expected ids {2, 3}, got %v", ids)
	}
}

// TestFlushMatchedCrossBatchDedup is a regression test for the bug where
// FlushMatched deduplicated by rowIdx only, dropping build rows from
// different batches that happened to share the same row index.
// With two build batches, row 0 of batch 0 and row 0 of batch 1 are
// different rows and must both appear in the output.
func TestFlushMatchedCrossBatchDedup(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "val", Type: parquet.TypeString},
	}

	hj := NewHashJoin(RightSemiJoin, []string{"id"}, []string{"id"})
	// Build two separate batches so rowIdx 0 exists in both.
	buildWithArenaMatched(hj, buildSchema, []map[string]any{
		{"id": int32(1), "val": "batch0-row0"},
		{"id": int32(2), "val": "batch0-row1"},
	})
	buildWithArenaMatched(hj, buildSchema, []map[string]any{
		{"id": int32(3), "val": "batch1-row0"}, // rowIdx=0 in batch 1
		{"id": int32(4), "val": "batch1-row1"},
	})

	probeSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
	}
	// Probe all four keys -- all build rows should match.
	probeBatch := batch.FromRows(probeSchema, []map[string]any{
		{"id": int32(1)},
		{"id": int32(2)},
		{"id": int32(3)},
		{"id": int32(4)},
	})

	probe := hj.Probe()
	_, err := probe.Execute(context.Background(), probeBatch)
	if err != nil {
		t.Fatalf("probe execute: %v", err)
	}

	result := probe.FlushMatched()
	if result == nil {
		t.Fatal("expected non-nil FlushMatched result")
	}

	// All 4 build rows from 2 batches must appear.
	// The old bug (dedup by rowIdx only) would keep only 2 rows: one with
	// rowIdx=0 and one with rowIdx=1, dropping the batch 1 rows.
	if result.Len != 4 {
		t.Fatalf("expected 4 unique build rows across 2 batches, got %d", result.Len)
	}

	rows := result.ToRows()
	ids := map[int32]bool{}
	for _, r := range rows {
		ids[r["id"].(int32)] = true
	}
	for _, expected := range []int32{1, 2, 3, 4} {
		if !ids[expected] {
			t.Errorf("missing build row with id=%d", expected)
		}
	}
}

// TestFlushAntiMatchedCrossBatchDedup is a regression test for the same
// dedup-by-rowIdx bug in FlushAntiMatched. Unmatched rows from different
// batches with the same rowIdx must both appear in the output.
func TestFlushAntiMatchedCrossBatchDedup(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "val", Type: parquet.TypeString},
	}

	hj := NewHashJoin(RightAntiJoin, []string{"id"}, []string{"id"})
	// Build two batches -- probe matches only id=1 from batch 0.
	buildWithArenaMatched(hj, buildSchema, []map[string]any{
		{"id": int32(1), "val": "batch0-row0"},
		{"id": int32(2), "val": "batch0-row1"},
	})
	buildWithArenaMatched(hj, buildSchema, []map[string]any{
		{"id": int32(3), "val": "batch1-row0"}, // rowIdx=0 in batch 1
		{"id": int32(4), "val": "batch1-row1"},
	})

	probeSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
	}
	probeBatch := batch.FromRows(probeSchema, []map[string]any{
		{"id": int32(1)}, // only match id=1
	})

	probe := hj.Probe()
	_, err := probe.Execute(context.Background(), probeBatch)
	if err != nil {
		t.Fatalf("probe execute: %v", err)
	}

	result := probe.FlushAntiMatched()
	if result == nil {
		t.Fatal("expected non-nil FlushAntiMatched result")
	}

	// 3 unmatched rows: id=2 (batch0), id=3 (batch1), id=4 (batch1)
	if result.Len != 3 {
		t.Fatalf("expected 3 unmatched build rows, got %d", result.Len)
	}

	rows := result.ToRows()
	ids := map[int32]bool{}
	for _, r := range rows {
		ids[r["id"].(int32)] = true
	}
	for _, expected := range []int32{2, 3, 4} {
		if !ids[expected] {
			t.Errorf("missing unmatched row with id=%d", expected)
		}
	}
}

// TestFlushUnmatchedDedup verifies that FlushUnmatched deduplicates build rows
// when the arena has multiple entries for the same build row (hash chain entries
// for duplicate keys). Each unique unmatched build row should appear exactly once.
func TestFlushUnmatchedDedup(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "val", Type: parquet.TypeString},
	}

	hj := NewHashJoin(RightJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(buildSchema, []map[string]any{
		{"id": int32(1), "val": "a"},
		{"id": int32(2), "val": "b"},
		{"id": int32(3), "val": "c"},
	})

	probeSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "extra", Type: parquet.TypeString},
	}
	// Probe only id=1, leaving id=2 and id=3 unmatched.
	// Send duplicate probe rows to create multiple arena entries for the
	// same build row, exercising the dedup path.
	probeBatch := batch.FromRows(probeSchema, []map[string]any{
		{"id": int32(1), "extra": "x"},
		{"id": int32(1), "extra": "y"}, // duplicate to exercise dedup
	})

	probe := hj.Probe()
	_, err := probe.Execute(context.Background(), probeBatch)
	if err != nil {
		t.Fatalf("probe execute: %v", err)
	}

	result := probe.FlushUnmatched(probeSchema)
	if result == nil {
		t.Fatal("expected non-nil FlushUnmatched result")
	}

	// Should have exactly 2 unmatched build rows (id=2 and id=3), not more
	if result.Len != 2 {
		t.Fatalf("expected 2 unmatched rows, got %d", result.Len)
	}
}

// TestFlushUnmatchedCrossBatchDedup is a regression test for the cross-batch
// dedup bug in FlushUnmatched. Unmatched rows from different build batches
// with the same rowIdx must both appear in the output.
func TestFlushUnmatchedCrossBatchDedup(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "val", Type: parquet.TypeString},
	}

	hj := NewHashJoin(FullOuterJoin, []string{"id"}, []string{"id"})
	// Build two separate batches so rowIdx 0 exists in both.
	hj.BuildFromRows(buildSchema, []map[string]any{
		{"id": int32(1), "val": "batch0-row0"},
		{"id": int32(2), "val": "batch0-row1"},
	})
	hj.BuildFromRows(buildSchema, []map[string]any{
		{"id": int32(3), "val": "batch1-row0"}, // rowIdx=0 in batch 1
		{"id": int32(4), "val": "batch1-row1"},
	})

	probeSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "extra", Type: parquet.TypeString},
	}
	// Probe only id=1 -- ids 2, 3, 4 should be unmatched.
	probeBatch := batch.FromRows(probeSchema, []map[string]any{
		{"id": int32(1), "extra": "x"},
	})

	probe := hj.Probe()
	_, err := probe.Execute(context.Background(), probeBatch)
	if err != nil {
		t.Fatalf("probe execute: %v", err)
	}

	result := probe.FlushUnmatched(probeSchema)
	if result == nil {
		t.Fatal("expected non-nil FlushUnmatched result")
	}

	// 3 unmatched rows: id=2 (batch0), id=3 (batch1), id=4 (batch1)
	// A rowIdx-only dedup would incorrectly keep only 2 rows.
	if result.Len != 3 {
		t.Fatalf("expected 3 unmatched build rows across 2 batches, got %d", result.Len)
	}
}
