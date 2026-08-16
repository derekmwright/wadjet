package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestHashJoin_Close_ReleasesTracker is a regression test for the operator
// memory-tracker leak that surfaced in the 2026-04-29 SF10 deploy. PR #65
// added shared-tracker accounting on HashJoin (Reserve during Build, Release
// during spill). When a join completes WITHOUT spilling, no Release path
// fires — the reservation accumulates in the tracker for the lifetime of
// the worker process. Across many concurrent broadcast joins the worker
// reports phantom-high pool pressure to the coordinator and trips
// worker-side spill thresholds prematurely. PR #76 removed the coord-side
// gate that turned this into a deadlock; this fix removes the leak itself.
func TestHashJoin_Close_ReleasesTracker(t *testing.T) {
	tracker := memory.NewTracker("test", 128*1024*1024)
	spill, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatalf("NewSpillManager: %v", err)
	}

	leftSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	leftRows := []map[string]any{
		{"k": "a", "amount": 1.0},
		{"k": "b", "amount": 2.0},
		{"k": "c", "amount": 3.0},
	}
	rightSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
	}
	rightRows := []map[string]any{
		{"k": "a", "name": "Alice"},
		{"k": "b", "name": "Bob"},
	}

	hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"k"})
	hj.MemTracker = tracker
	hj.Spill = spill

	// Use the production Build() path — it reserves through the tracker on
	// every batch via reconcileHashMemory. BuildFromRows is a test-only
	// helper that bypasses the tracker entirely.
	if err := hj.Build(context.Background(), NewSliceSource(rightSchema, rightRows)); err != nil {
		t.Fatalf("hj.Build: %v", err)
	}
	if got := tracker.Used(); got <= 0 {
		t.Fatalf("expected tracker to register reservation after Build, got Used=%d", got)
	}

	pipe := &Pipeline{
		Source: NewSliceSource(leftSchema, leftRows),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   &CollectSink{},
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("pipeline.Run: %v", err)
	}

	if err := hj.Close(); err != nil {
		t.Fatalf("hj.Close: %v", err)
	}

	if got := tracker.Used(); got != 0 {
		t.Fatalf("HashJoin.Close did not release tracker reservation: Used=%d, want 0", got)
	}
}

// TestSort_Close_ReleasesTracker verifies the same invariant for Sort: when
// it completes without spilling, Close must release the tracker reservation
// it accumulated for buffered rows.
func TestSort_Close_ReleasesTracker(t *testing.T) {
	tracker := memory.NewTracker("test", 128*1024*1024)
	spill, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatalf("NewSpillManager: %v", err)
	}

	schema := []parquet.Column{
		{Name: "v", Type: parquet.TypeInt64},
	}
	rows := make([]map[string]any, 256)
	for i := range rows {
		rows[i] = map[string]any{"v": int64(255 - i)}
	}

	s := NewSort([]SortKey{{Column: "v", Order: Ascending}})
	s.Spill = spill

	pipe := &Pipeline{
		Source: NewSliceSource(schema, rows),
		Sink:   s,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("pipeline.Run: %v", err)
	}

	// Drain so finalize-side state is settled.
	for {
		b, err := s.Next(context.Background())
		if err != nil || b == nil {
			break
		}
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Sort.Close: %v", err)
	}

	if got := tracker.Used(); got != 0 {
		t.Fatalf("Sort.Close did not release tracker reservation: Used=%d, want 0", got)
	}
}

// TestHashAggregate_Close_ReleasesTracker verifies the same invariant for
// HashAggregate: when it completes without spilling, Close must release
// both the group-state reservation and any unspilled buffer reservation.
func TestHashAggregate_Close_ReleasesTracker(t *testing.T) {
	tracker := memory.NewTracker("test", 128*1024*1024)
	spill, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatalf("NewSpillManager: %v", err)
	}

	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"g": "a", "v": int64(1)},
		{"g": "b", "v": int64(2)},
		{"g": "a", "v": int64(3)},
		{"g": "c", "v": int64(4)},
	}

	hashAgg := NewHashAggregate(
		[]string{"g"},
		[]AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64}},
	)
	hashAgg.Spill = spill

	pipe := &Pipeline{
		Source: NewSliceSource(schema, rows),
		Sink:   hashAgg,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("pipeline.Run: %v", err)
	}

	for {
		b, err := hashAgg.Next(context.Background())
		if err != nil || b == nil {
			break
		}
	}

	if err := hashAgg.Close(); err != nil {
		t.Fatalf("HashAggregate.Close: %v", err)
	}

	if got := tracker.Used(); got != 0 {
		t.Fatalf("HashAggregate.Close did not release tracker reservation: Used=%d, want 0", got)
	}
}
