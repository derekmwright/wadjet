package tpch

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// TestQ21ReverseBloomSemiAnti reproduces the 2026-07-05 SF10 standalone
// Q21 crash at SF0.01 by forcing the reverse-bloom path for semi/anti
// joins (in production it fires when the build-side estimate exceeds
// 10M rows — SF10 lineitem is 60M, so every cold-S3 SF10 standalone run
// panicked in HashJoin.PruneBuildColumns with a nil build batch, killing
// the suite after Q20).
//
// Q21 stacks an EXISTS (semi) and NOT EXISTS (anti) join on lineitem,
// both with join filters (l_suppkey <>), so SemiAntiKeyOnly is false and
// the reverse-bloom build goroutine runs FixKeyAssignment +
// PruneBuildColumns after Build.
func TestQ21ReverseBloomSemiAnti(t *testing.T) {
	oldSemi := physical.ReverseBloomThreshold
	physical.ReverseBloomThreshold = 1
	defer func() { physical.ReverseBloomThreshold = oldSemi }()

	db := setupTPCH(t, SF001)
	ctx := context.Background()

	q := TPCHQueries[21]
	result, err := db.Query(ctx, q.SQL)
	if err != nil {
		t.Fatalf("Q21 failed: %v", err)
	}
	if want := expectedRowsSF001[21]; len(result.Rows) != want {
		t.Errorf("Q21 rows = %d, want %d", len(result.Rows), want)
	}
}
