package worker

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/logical"
)

// TestBuildProbePartitionBloomsSmokeTest verifies that buildProbePartitionBlooms
// reads parquet files from MemStore and produces non-nil bloom filters for
// integer join key columns. This is a smoke test — it does not verify FPR or
// exact bloom contents, just that the code path doesn't panic and produces output.
func TestBuildProbePartitionBloomsSmokeTest(t *testing.T) {
	bucket := "test-bucket"
	store := newTestStore(t, bucket)

	// Write a small parquet file with an integer column "l_orderkey"
	// using the TPC-H naming convention (lineitem → l_ prefix)
	rows := []map[string]any{
		{"l_orderkey": int64(1), "l_quantity": int64(10)},
		{"l_orderkey": int64(2), "l_quantity": int64(20)},
		{"l_orderkey": int64(3), "l_quantity": int64(30)},
		{"l_orderkey": int64(1), "l_quantity": int64(15)},
		{"l_orderkey": int64(4), "l_quantity": int64(25)},
	}
	writeParquetFile(t, store, bucket, "data/lineitem/part-0.parquet", rows)

	// Build a logical plan with a join condition
	// lineitem JOIN orders ON l_orderkey = o_orderkey
	scanLineitem := &logical.Node{
		Type:      logical.NodeScan,
		TableName: "lineitem",
	}
	scanOrders := &logical.Node{
		Type:      logical.NodeScan,
		TableName: "orders",
	}
	joinNode := &logical.Node{
		Type:     logical.NodeJoin,
		JoinCond: "l_orderkey = o_orderkey",
		Children: []*logical.Node{scanLineitem, scanOrders},
	}

	// scanFileFilter tells buildProbePartitionBlooms which files belong to the probe table
	scanFileFilter := map[string][]string{
		"lineitem": {"data/lineitem/part-0.parquet"},
	}

	exec := &Executor{
		store:  store,
		logger: slog.Default(),
	}

	ctx := context.Background()
	result := exec.buildProbePartitionBlooms(ctx, joinNode, scanFileFilter, nil, bucket)

	// Should produce a bloom for the build-side column "o_orderkey"
	if result == nil {
		t.Fatal("expected non-nil bloom filter map")
	}
	filter, ok := result["o_orderkey"]
	if !ok {
		t.Fatalf("expected bloom for 'o_orderkey', got keys: %v", keys(result))
	}
	if filter.Bloom == nil {
		t.Fatal("bloom data is nil")
	}
	if filter.BloomMask == 0 {
		t.Fatal("bloom mask is 0")
	}
	if filter.Column != "o_orderkey" {
		t.Errorf("column: got %q, want %q", filter.Column, "o_orderkey")
	}

	// Verify the bloom contains the keys we inserted (no false negatives allowed)
	for _, key := range []int64{1, 2, 3, 4} {
		hash := bloomHashIntForTest(key)
		if !bloomContainsForTest(filter.Bloom, filter.BloomMask, hash) {
			t.Errorf("key %d not found in bloom (false negative — bloom filter invariant violated)", key)
		}
	}
}

// TestBuildProbePartitionBloomsInt32Column verifies bloom works for int32 columns.
// This is the complement to the smoke test (which uses int64) — ensures the
// PhysType dispatch in buildProbePartitionBlooms handles both int widths.
func TestBuildProbePartitionBloomsInt32Column(t *testing.T) {
	bucket := "test-bucket"
	store := newTestStore(t, bucket)

	rows := []map[string]any{
		{"l_orderkey": int32(10), "l_quantity": int32(1)},
		{"l_orderkey": int32(20), "l_quantity": int32(2)},
		{"l_orderkey": int32(30), "l_quantity": int32(3)},
	}
	writeParquetFile(t, store, bucket, "data/lineitem/part-0.parquet", rows)

	joinNode := &logical.Node{
		Type:     logical.NodeJoin,
		JoinCond: "l_orderkey = o_orderkey",
		Children: []*logical.Node{
			{Type: logical.NodeScan, TableName: "lineitem"},
			{Type: logical.NodeScan, TableName: "orders"},
		},
	}
	scanFileFilter := map[string][]string{
		"lineitem": {"data/lineitem/part-0.parquet"},
	}

	exec := &Executor{store: store, logger: slog.Default()}
	result := exec.buildProbePartitionBlooms(context.Background(), joinNode, scanFileFilter, nil, bucket)

	if result == nil {
		t.Fatal("expected non-nil bloom filter map")
	}
	filter, ok := result["o_orderkey"]
	if !ok {
		t.Fatalf("expected bloom for 'o_orderkey', got keys: %v", keys(result))
	}

	// Verify all inserted keys are present (no false negatives)
	for _, key := range []int64{10, 20, 30} {
		hash := bloomHashIntForTest(key)
		if !bloomContainsForTest(filter.Bloom, filter.BloomMask, hash) {
			t.Errorf("key %d not found in bloom (false negative — bloom filter invariant violated)", key)
		}
	}
}

// TestBuildProbePartitionBloomsInt64TypeDispatch is a regression test for a bug
// where buildProbePartitionBlooms called vals.Int32() before vals.Int64().
// Since the Values unsafe accessors don't check PhysType, Int32() returned
// non-nil for int64 data — reinterpreting int64 bytes as int32 and hashing
// garbage values. This caused bloom filter false negatives → silent data loss.
func TestBuildProbePartitionBloomsInt64TypeDispatch(t *testing.T) {
	bucket := "test-bucket"
	store := newTestStore(t, bucket)

	// Use values that are NOT representable in the lower 32 bits of an int64
	// to ensure we're reading actual int64 values, not truncated halves.
	rows := []map[string]any{
		{"l_orderkey": int64(1 << 33), "l_quantity": int64(1)}, // 8589934592
		{"l_orderkey": int64(1<<33 + 1), "l_quantity": int64(2)},
		{"l_orderkey": int64(1<<33 + 2), "l_quantity": int64(3)},
	}
	writeParquetFile(t, store, bucket, "data/lineitem/part-0.parquet", rows)

	joinNode := &logical.Node{
		Type:     logical.NodeJoin,
		JoinCond: "l_orderkey = o_orderkey",
		Children: []*logical.Node{
			{Type: logical.NodeScan, TableName: "lineitem"},
			{Type: logical.NodeScan, TableName: "orders"},
		},
	}
	scanFileFilter := map[string][]string{
		"lineitem": {"data/lineitem/part-0.parquet"},
	}

	exec := &Executor{store: store, logger: slog.Default()}
	result := exec.buildProbePartitionBlooms(context.Background(), joinNode, scanFileFilter, nil, bucket)

	if result == nil {
		t.Fatal("expected non-nil bloom filter map")
	}
	filter, ok := result["o_orderkey"]
	if !ok {
		t.Fatalf("expected bloom for 'o_orderkey', got keys: %v", keys(result))
	}

	// All keys must be found — if int32 dispatch is used by mistake,
	// the lower 32 bits of 1<<33 are 0, so the bloom would contain
	// hashes of [0, 1, 2] instead of [8589934592, 8589934593, 8589934594].
	for _, key := range []int64{1 << 33, 1<<33 + 1, 1<<33 + 2} {
		hash := bloomHashIntForTest(key)
		if !bloomContainsForTest(filter.Bloom, filter.BloomMask, hash) {
			t.Errorf("key %d not found in bloom (false negative — int64 type dispatch regression)", key)
		}
	}
}

// TestBuildProbePartitionBloomsMultiJoinKeys verifies bloom filters are built
// for multiple join key columns simultaneously (e.g., composite join conditions).
func TestBuildProbePartitionBloomsMultiJoinKeys(t *testing.T) {
	bucket := "test-bucket"
	store := newTestStore(t, bucket)

	rows := []map[string]any{
		{"l_orderkey": int64(100), "l_partkey": int64(200), "l_quantity": int64(10)},
		{"l_orderkey": int64(101), "l_partkey": int64(201), "l_quantity": int64(20)},
	}
	writeParquetFile(t, store, bucket, "data/lineitem/part-0.parquet", rows)

	joinNode := &logical.Node{
		Type:     logical.NodeJoin,
		JoinCond: "l_orderkey = o_orderkey AND l_partkey = p_partkey",
		Children: []*logical.Node{
			{Type: logical.NodeScan, TableName: "lineitem"},
			{Type: logical.NodeScan, TableName: "orders"},
		},
	}
	scanFileFilter := map[string][]string{
		"lineitem": {"data/lineitem/part-0.parquet"},
	}

	exec := &Executor{store: store, logger: slog.Default()}
	result := exec.buildProbePartitionBlooms(context.Background(), joinNode, scanFileFilter, nil, bucket)

	if result == nil {
		t.Fatal("expected non-nil bloom filter map")
	}

	// Should have blooms for both build-side columns
	for _, buildCol := range []string{"o_orderkey", "p_partkey"} {
		filter, ok := result[buildCol]
		if !ok {
			t.Errorf("expected bloom for %q, got keys: %v", buildCol, keys(result))
			continue
		}
		if filter.Bloom == nil {
			t.Errorf("bloom data for %q is nil", buildCol)
		}
	}

	// Verify o_orderkey bloom contains correct keys
	okFilter := result["o_orderkey"]
	for _, key := range []int64{100, 101} {
		hash := bloomHashIntForTest(key)
		if !bloomContainsForTest(okFilter.Bloom, okFilter.BloomMask, hash) {
			t.Errorf("o_orderkey bloom missing key %d", key)
		}
	}

	// Verify p_partkey bloom contains correct keys
	pkFilter := result["p_partkey"]
	for _, key := range []int64{200, 201} {
		hash := bloomHashIntForTest(key)
		if !bloomContainsForTest(pkFilter.Bloom, pkFilter.BloomMask, hash) {
			t.Errorf("p_partkey bloom missing key %d", key)
		}
	}
}

// TestBuildProbePartitionBloomsNoJoinCond verifies nil is returned when
// the logical plan has no join conditions.
func TestBuildProbePartitionBloomsNoJoinCond(t *testing.T) {
	bucket := "test-bucket"
	store := newTestStore(t, bucket)

	scanNode := &logical.Node{
		Type:      logical.NodeScan,
		TableName: "lineitem",
	}

	exec := &Executor{
		store:  store,
		logger: slog.Default(),
	}

	scanFileFilter := map[string][]string{
		"lineitem": {"data/lineitem/part-0.parquet"},
	}

	result := exec.buildProbePartitionBlooms(context.Background(), scanNode, scanFileFilter, nil, bucket)
	if result != nil {
		t.Errorf("expected nil result with no join conditions, got %d filters", len(result))
	}
}

// TestBuildProbePartitionBloomsEmptyScanFiles verifies nil is returned when
// the probe table has no files to scan.
func TestBuildProbePartitionBloomsEmptyScanFiles(t *testing.T) {
	bucket := "test-bucket"
	store := newTestStore(t, bucket)

	joinNode := &logical.Node{
		Type:     logical.NodeJoin,
		JoinCond: "l_orderkey = o_orderkey",
		Children: []*logical.Node{
			{Type: logical.NodeScan, TableName: "lineitem"},
			{Type: logical.NodeScan, TableName: "orders"},
		},
	}

	exec := &Executor{
		store:  store,
		logger: slog.Default(),
	}

	scanFileFilter := map[string][]string{
		"lineitem": {}, // no files
	}

	result := exec.buildProbePartitionBlooms(context.Background(), joinNode, scanFileFilter, nil, bucket)
	if result != nil {
		t.Errorf("expected nil result with empty probe files, got %d filters", len(result))
	}
}

// TestBuildProbePartitionBloomsMissingFile verifies that a missing parquet file
// is silently skipped (no panic, no error) — just produces an empty bloom.
func TestBuildProbePartitionBloomsMissingFile(t *testing.T) {
	bucket := "test-bucket"
	store := newTestStore(t, bucket)

	joinNode := &logical.Node{
		Type:     logical.NodeJoin,
		JoinCond: "l_orderkey = o_orderkey",
		Children: []*logical.Node{
			{Type: logical.NodeScan, TableName: "lineitem"},
			{Type: logical.NodeScan, TableName: "orders"},
		},
	}

	exec := &Executor{
		store:  store,
		logger: slog.Default(),
	}

	scanFileFilter := map[string][]string{
		"lineitem": {"nonexistent/file.parquet"},
	}

	// Should not panic
	result := exec.buildProbePartitionBlooms(context.Background(), joinNode, scanFileFilter, nil, bucket)
	// Result may be non-nil (bloom allocated but empty) — that's fine
	if result != nil {
		filter := result["o_orderkey"]
		if filter != nil && filter.Bloom != nil {
			t.Logf("bloom allocated with %d slots (empty — no data read)", len(filter.Bloom))
		}
	}
}

// writeParquetFile is defined in executor_pipeline_test.go — this file
// uses the shared test helper.

// bloomHashIntForTest uses the same hash as exec.BloomHashInt:
// golden-ratio multiply-shift, matching the production code.
func bloomHashIntForTest(key int64) uint64 {
	x := uint64(key) * 0x9E3779B97F4A7C15
	return x ^ (x >> 32)
}

func bloomContainsForTest(bloom []uint64, mask, hash uint64) bool {
	h1 := hash & mask
	h2 := (hash >> 17) & mask
	b1 := hash & 63
	b2 := (hash >> 6) & 63
	return (bloom[h1]>>b1)&1 != 0 && (bloom[h2]>>b2)&1 != 0
}

func keys[V any](m map[string]V) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}

// Ensure writeParquetFile helper is accessible (it's in executor_pipeline_test.go)
// by importing bytes for this compilation unit.
var _ = bytes.NewReader
