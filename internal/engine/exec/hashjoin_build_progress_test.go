package exec

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// countingReporter satisfies ProgressReporter and tallies AddRows / AddBytes
// calls atomically so concurrent build paths can update without a mutex.
type countingReporter struct {
	rows  atomic.Int64
	bytes atomic.Int64
}

func (c *countingReporter) AddRows(n int64)  { c.rows.Add(n) }
func (c *countingReporter) AddBytes(n int64) { c.bytes.Add(n) }

// TestHashJoin_Build_ReportsProgress is a regression test for the
// 2026-04-29 PM SF10 hot-potato pattern. Pipeline.runSerial bumps the
// per-task ProgressReporter for every batch, but HashJoin.Build runs its
// own source.Next loop independently of Pipeline. For long broadcast_join
// builds (60M-row lineitem), no rows-processed signal flowed; the per-task
// heartbeat goroutine emitted no TaskProgress; PR #78's multi-signal
// liveness check had nothing to fall back on; workers were reaped past
// the 90s stale TTL even though their executor goroutines were making
// progress.
//
// The fix: Build pulls the ProgressReporter from ctx and bumps it for
// each consumed batch, mirroring Pipeline.runSerial's behaviour.
func TestHashJoin_Build_ReportsProgress(t *testing.T) {
	rep := &countingReporter{}
	ctx := WithProgressReporter(context.Background(), rep)

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeString},
	}
	const numRows = 4096
	rows := make([]map[string]any, numRows)
	for i := range rows {
		rows[i] = map[string]any{"k": "key"}
	}

	hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"k"})
	if err := hj.Build(ctx, NewSliceSource(schema, rows)); err != nil {
		t.Fatalf("Build: %v", err)
	}

	if got := rep.rows.Load(); got != int64(numRows) {
		t.Fatalf("HashJoin.Build did not report progress for build-side rows: got AddRows total=%d, want %d", got, numRows)
	}
}

// TestHashJoin_Build_ReportsProgressKeyOnly mirrors the above for the
// SemiAntiKeyOnly parallel build path. Different code path, same gap if
// not patched.
func TestHashJoin_Build_ReportsProgressKeyOnly(t *testing.T) {
	rep := &countingReporter{}
	ctx := WithProgressReporter(context.Background(), rep)

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeString},
	}
	const numRows = 4096
	rows := make([]map[string]any, numRows)
	for i := range rows {
		rows[i] = map[string]any{"k": "key"}
	}

	hj := NewHashJoin(SemiJoin, []string{"k"}, []string{"k"})
	hj.SemiAntiKeyOnly = true
	if err := hj.Build(ctx, NewSliceSource(schema, rows)); err != nil {
		t.Fatalf("Build: %v", err)
	}

	if got := rep.rows.Load(); got != int64(numRows) {
		t.Fatalf("HashJoin.Build (key-only path) did not report progress: got AddRows total=%d, want %d", got, numRows)
	}
}
