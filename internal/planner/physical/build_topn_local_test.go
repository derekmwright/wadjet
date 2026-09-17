// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Local-pipeline cells that stayed with the local planner when the stage
// planner moved out: they read this package's own unexported sources.

// TestBuildTopN_WiresSpillManager: ORDER BY ... LIMIT plans must attach the
// spill manager like plain ORDER BY does — without it the pre-sort input
// buffered fully untracked (the top-K heap only runs at finalize).
func TestBuildTopN_WiresSpillManager(t *testing.T) {
	cat, ctx := setupCatalog(t)
	p := NewPlanner(cat)
	p.MemoryBudget = 1 << 30

	node := logical.NewScan("events", "e")
	sortNode := logical.NewSort(node, []logical.OrderExpr{{Column: "event_id"}})

	src, _, _, err := p.buildTopN(ctx, sortNode, 10)
	if err != nil {
		t.Fatalf("buildTopN: %v", err)
	}
	adapter, ok := src.(*sortSourceAdapter)
	if !ok {
		t.Fatalf("expected sortSourceAdapter, got %T", src)
	}
	if adapter.sort.Spill == nil {
		t.Fatal("buildTopN left Sort.Spill nil — pre-sort input is untracked and unspillable")
	}
	if adapter.sort.Limit != 10 {
		t.Fatalf("Limit = %d, want 10", adapter.sort.Limit)
	}
}

// setupCatalog is the fixture this cell plans against, copied from the
// stage-planner package for the same reason its own fixtures are copied: a
// _test.go helper is not importable.
func setupCatalog(t *testing.T) (*catalog.Catalog, context.Context) {
	t.Helper()
	ctx := context.Background()

	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "event_id", Type: parquet.TypeString},
			{Name: "user_id", Type: parquet.TypeString},
			{Name: "ts", Type: parquet.TypeTimestamp},
			{Name: "year", Type: parquet.TypeString},
		},
	}
	if err := cat.CreateTable(ctx, "events", schema, []string{"year"}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if err := cat.AddFiles(ctx, "events",
		map[string]string{"year": "2026"},
		"tables/events/year=2026/",
		[]catalog.FileEntry{
			{Path: "tables/events/year=2026/chunk_001.parquet", SizeBytes: 1024, NumRows: 100},
		},
	); err != nil {
		t.Fatalf("add files: %v", err)
	}

	return cat, ctx
}
