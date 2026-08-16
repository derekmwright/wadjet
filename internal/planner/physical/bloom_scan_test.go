package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// buildTestBloomFilter creates a BloomScanFilter from a hash join with the given
// integer build keys. This replicates the real pipeline: build a hash table,
// extract the bloom filter op, then extract the scan filter.
func buildTestBloomFilter(t *testing.T, column string, keys []int64) *exec.BloomScanFilter {
	t.Helper()
	schema := []parquet.Column{
		{Name: column, Type: parquet.TypeInt64},
	}
	rows := make([]map[string]any, len(keys))
	for i, k := range keys {
		rows[i] = map[string]any{column: k}
	}

	hj := exec.NewHashJoin(exec.InnerJoin, []string{column}, []string{column})
	src := exec.NewSliceSource(schema, rows)
	if err := hj.Build(t.Context(), src); err != nil {
		t.Fatalf("building hash join: %v", err)
	}
	bf := hj.BloomPushdownOp()
	if bf == nil {
		t.Fatal("expected bloom filter op")
	}
	bsf := bf.BloomScanFilter()
	if bsf == nil {
		t.Fatal("expected BloomScanFilter for integer key")
	}
	return bsf
}

func TestCanBloomPruneRowGroup_SkipNonMatching(t *testing.T) {
	bsf := buildTestBloomFilter(t, "id", []int64{100, 200, 300})

	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"id": {HasStats: true, MinValue: int64(500), MaxValue: int64(510)},
		},
	}

	if !canBloomPruneRowGroup(bsf, stats) {
		t.Error("expected row group [500,510] to be pruned (no matching build keys)")
	}
}

func TestCanBloomPruneRowGroup_KeepMatching(t *testing.T) {
	bsf := buildTestBloomFilter(t, "id", []int64{100, 200, 300})

	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"id": {HasStats: true, MinValue: int64(95), MaxValue: int64(105)},
		},
	}

	if canBloomPruneRowGroup(bsf, stats) {
		t.Error("expected row group [95,105] to be KEPT (contains build key 100)")
	}
}

func TestCanBloomPruneRowGroup_ExactMatch(t *testing.T) {
	bsf := buildTestBloomFilter(t, "id", []int64{42})

	stats := parquet.RowGroupStats{
		NumRows: 1,
		Columns: map[string]parquet.ColumnStats{
			"id": {HasStats: true, MinValue: int64(42), MaxValue: int64(42)},
		},
	}

	if canBloomPruneRowGroup(bsf, stats) {
		t.Error("expected row group [42,42] to be KEPT (exact match)")
	}
}

func TestCanBloomPruneRowGroup_NoStats(t *testing.T) {
	bsf := buildTestBloomFilter(t, "id", []int64{100})

	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{},
	}

	if canBloomPruneRowGroup(bsf, stats) {
		t.Error("expected row group without stats to NOT be pruned")
	}
}

func TestCanBloomPruneRowGroup_LargeRange(t *testing.T) {
	bsf := buildTestBloomFilter(t, "id", []int64{100})

	stats := parquet.RowGroupStats{
		NumRows: 10000,
		Columns: map[string]parquet.ColumnStats{
			"id": {HasStats: true, MinValue: int64(0), MaxValue: int64(10000)},
		},
	}

	if canBloomPruneRowGroup(bsf, stats) {
		t.Error("expected row group with large range to NOT be pruned")
	}
}

func TestCanBloomPruneRowGroup_NilMinMax(t *testing.T) {
	bsf := buildTestBloomFilter(t, "id", []int64{100})

	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"id": {HasStats: true, MinValue: nil, MaxValue: nil},
		},
	}

	if canBloomPruneRowGroup(bsf, stats) {
		t.Error("expected row group with nil min/max to NOT be pruned")
	}
}

func TestCanBloomPruneRowGroup_Int32Stats(t *testing.T) {
	bsf := buildTestBloomFilter(t, "id", []int64{100, 200, 300})

	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"id": {HasStats: true, MinValue: int32(500), MaxValue: int32(510)},
		},
	}

	if !canBloomPruneRowGroup(bsf, stats) {
		t.Error("expected row group [500,510] (int32 stats) to be pruned")
	}
}

func TestCanBloomPruneRowGroup_NoFalseNegatives(t *testing.T) {
	buildKeys := []int64{10, 50, 100, 500, 999}
	bsf := buildTestBloomFilter(t, "id", buildKeys)

	for _, key := range buildKeys {
		stats := parquet.RowGroupStats{
			NumRows: 100,
			Columns: map[string]parquet.ColumnStats{
				"id": {HasStats: true, MinValue: key - 5, MaxValue: key + 5},
			},
		}
		if canBloomPruneRowGroup(bsf, stats) {
			t.Errorf("row group containing build key %d was incorrectly pruned", key)
		}
	}
}

func TestCanBloomPruneRowGroup_WrongColumn(t *testing.T) {
	bsf := buildTestBloomFilter(t, "id", []int64{100})

	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"other_id": {HasStats: true, MinValue: int64(500), MaxValue: int64(510)},
		},
	}

	if canBloomPruneRowGroup(bsf, stats) {
		t.Error("expected row group to NOT be pruned when bloom column not in stats")
	}
}

func TestCanBloomPruneRowGroup_SingleValueNoMatch(t *testing.T) {
	bsf := buildTestBloomFilter(t, "id", []int64{42})

	stats := parquet.RowGroupStats{
		NumRows: 1000,
		Columns: map[string]parquet.ColumnStats{
			"id": {HasStats: true, MinValue: int64(999), MaxValue: int64(999)},
		},
	}

	if !canBloomPruneRowGroup(bsf, stats) {
		t.Error("expected row group [999,999] to be pruned (single value not in build)")
	}
}

func TestCanBloomPruneRowGroup_StringKeyNotSupported(t *testing.T) {
	bsf := &exec.BloomScanFilter{
		Bloom:     make([]uint64, 64),
		BloomMask: 63,
		Column:    "name",
		UseIntKey: false,
	}

	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"name": {HasStats: true, MinValue: "aaa", MaxValue: "zzz"},
		},
	}

	if canBloomPruneRowGroup(bsf, stats) {
		t.Error("expected string-key bloom filter to NOT prune row groups")
	}
}

// --- Dynamic range pruning tests ---

func TestCanRangePruneRowGroup_SkipNonOverlapping(t *testing.T) {
	ranges := []exec.DynamicRange{
		{Column: "id", MinValue: int64(100), MaxValue: int64(200)},
	}
	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"id": {HasStats: true, MinValue: int64(500), MaxValue: int64(600)},
		},
	}
	if !canRangePruneRowGroup(ranges, stats) {
		t.Error("expected row group [500,600] to be pruned (build range [100,200])")
	}
}

func TestCanRangePruneRowGroup_KeepOverlapping(t *testing.T) {
	ranges := []exec.DynamicRange{
		{Column: "id", MinValue: int64(100), MaxValue: int64(200)},
	}
	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"id": {HasStats: true, MinValue: int64(150), MaxValue: int64(250)},
		},
	}
	if canRangePruneRowGroup(ranges, stats) {
		t.Error("expected row group [150,250] to be kept (overlaps build range [100,200])")
	}
}

func TestCanRangePruneRowGroup_ExactOverlap(t *testing.T) {
	ranges := []exec.DynamicRange{
		{Column: "id", MinValue: int64(100), MaxValue: int64(200)},
	}
	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"id": {HasStats: true, MinValue: int64(100), MaxValue: int64(200)},
		},
	}
	if canRangePruneRowGroup(ranges, stats) {
		t.Error("expected row group [100,200] to be kept (exact overlap)")
	}
}

func TestCanRangePruneRowGroup_BuildAboveRowGroup(t *testing.T) {
	ranges := []exec.DynamicRange{
		{Column: "id", MinValue: int64(500), MaxValue: int64(600)},
	}
	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"id": {HasStats: true, MinValue: int64(100), MaxValue: int64(200)},
		},
	}
	if !canRangePruneRowGroup(ranges, stats) {
		t.Error("expected row group [100,200] to be pruned (build range [500,600])")
	}
}

func TestCanRangePruneRowGroup_NoStats(t *testing.T) {
	ranges := []exec.DynamicRange{
		{Column: "id", MinValue: int64(100), MaxValue: int64(200)},
	}
	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"id": {HasStats: false},
		},
	}
	if canRangePruneRowGroup(ranges, stats) {
		t.Error("expected row group without stats to NOT be pruned")
	}
}

func TestCanRangePruneRowGroup_StringRange(t *testing.T) {
	ranges := []exec.DynamicRange{
		{Column: "name", MinValue: "delta", MaxValue: "foxtrot"},
	}
	// Row group range [alpha, charlie] < build min "delta" → prune
	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"name": {HasStats: true, MinValue: "alpha", MaxValue: "charlie"},
		},
	}
	if !canRangePruneRowGroup(ranges, stats) {
		t.Error("expected string row group [alpha,charlie] to be pruned (build [delta,foxtrot])")
	}

	// Row group range [echo, golf] overlaps build [delta, foxtrot] → keep
	stats2 := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"name": {HasStats: true, MinValue: "echo", MaxValue: "golf"},
		},
	}
	if canRangePruneRowGroup(ranges, stats2) {
		t.Error("expected string row group [echo,golf] to be kept (overlaps build [delta,foxtrot])")
	}
}

func TestCanRangePruneRowGroup_MissingColumn(t *testing.T) {
	ranges := []exec.DynamicRange{
		{Column: "id", MinValue: int64(100), MaxValue: int64(200)},
	}
	stats := parquet.RowGroupStats{
		NumRows: 100,
		Columns: map[string]parquet.ColumnStats{
			"other_col": {HasStats: true, MinValue: int64(500), MaxValue: int64(600)},
		},
	}
	if canRangePruneRowGroup(ranges, stats) {
		t.Error("expected row group to NOT be pruned when filter column is missing from stats")
	}
}

func TestBuildKeyRange_IntKey(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"id": int64(50)},
		{"id": int64(100)},
		{"id": int64(25)},
		{"id": int64(75)},
	}
	hj := exec.NewHashJoin(exec.InnerJoin, []string{"probe_id"}, []string{"id"})
	src := exec.NewSliceSource(schema, rows)
	if err := hj.Build(t.Context(), src); err != nil {
		t.Fatalf("build: %v", err)
	}
	ranges := hj.BuildKeyRange()
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range, got %d", len(ranges))
	}
	r := ranges[0]
	if r.Column != "probe_id" {
		t.Errorf("expected column 'probe_id', got %q", r.Column)
	}
	if r.MinValue.(int64) != 25 {
		t.Errorf("expected min 25, got %v", r.MinValue)
	}
	if r.MaxValue.(int64) != 100 {
		t.Errorf("expected max 100, got %v", r.MaxValue)
	}
}

func TestBuildKeyRange_EmptyBuild(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	hj := exec.NewHashJoin(exec.InnerJoin, []string{"probe_id"}, []string{"id"})
	src := exec.NewSliceSource(schema, nil)
	if err := hj.Build(t.Context(), src); err != nil {
		t.Fatalf("build: %v", err)
	}
	if ranges := hj.BuildKeyRange(); ranges != nil {
		t.Errorf("expected nil ranges for empty build, got %v", ranges)
	}
}

func TestBuildKeyRange_LeftJoinReturnsNil(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{{"id": int64(1)}}
	hj := exec.NewHashJoin(exec.LeftJoin, []string{"id"}, []string{"id"})
	src := exec.NewSliceSource(schema, rows)
	if err := hj.Build(t.Context(), src); err != nil {
		t.Fatalf("build: %v", err)
	}
	if ranges := hj.BuildKeyRange(); ranges != nil {
		t.Errorf("expected nil ranges for LEFT JOIN, got %v", ranges)
	}
}
