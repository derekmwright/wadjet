package scan

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestCanPruneRowGroupNe(t *testing.T) {
	// Test OpNe pruning: only prune when min == max == value
	stats := pqt.RowGroupStats{
		NumRows: 100,
		Columns: map[string]pqt.ColumnStats{
			"status": {
				HasStats:  true,
				MinValue:  "active",
				MaxValue:  "active", // all same value
				NullCount: 0,
			},
			"mixed": {
				HasStats:  true,
				MinValue:  "a",
				MaxValue:  "z",
				NullCount: 0,
			},
		},
	}

	// Can prune: all values are "active" and we want != "active"
	if !CanPruneRowGroup(StatsPredicate{"status", exec.OpNe, "active"}, stats) {
		t.Fatal("expected prune when all values equal the NE value")
	}

	// Can't prune: mixed values
	if CanPruneRowGroup(StatsPredicate{"mixed", exec.OpNe, "b"}, stats) {
		t.Fatal("should not prune when values are mixed")
	}
}

func TestCanPruneRowGroupIsNullNotNull(t *testing.T) {
	stats := pqt.RowGroupStats{
		NumRows: 100,
		Columns: map[string]pqt.ColumnStats{
			"col": {
				HasStats:  true,
				MinValue:  int64(1),
				MaxValue:  int64(100),
				NullCount: 10,
			},
		},
	}

	// IS NULL/IS NOT NULL with non-all-null column can't be pruned with min/max
	if CanPruneRowGroup(StatsPredicate{"col", exec.OpIsNull, nil}, stats) {
		t.Fatal("should not prune IS NULL for partial-null column")
	}
	if CanPruneRowGroup(StatsPredicate{"col", exec.OpIsNotNull, nil}, stats) {
		t.Fatal("should not prune IS NOT NULL for partial-null column")
	}
}

func TestCanPruneRowGroupNoStats(t *testing.T) {
	stats := pqt.RowGroupStats{
		NumRows: 100,
		Columns: map[string]pqt.ColumnStats{
			"col": {
				HasStats: false,
			},
		},
	}
	if CanPruneRowGroup(StatsPredicate{"col", exec.OpEq, int64(1)}, stats) {
		t.Fatal("should not prune when HasStats is false")
	}
}

func TestCanPruneRowGroupNilMinMax(t *testing.T) {
	stats := pqt.RowGroupStats{
		NumRows: 100,
		Columns: map[string]pqt.ColumnStats{
			"col": {
				HasStats:  true,
				MinValue:  nil,
				MaxValue:  nil,
				NullCount: 50, // not all null
			},
		},
	}
	// Has stats but nil min/max (and not all null) - can't prune comparisons
	if CanPruneRowGroup(StatsPredicate{"col", exec.OpEq, int64(1)}, stats) {
		t.Fatal("should not prune with nil min/max")
	}
}

func TestCanPruneDefaultOp(t *testing.T) {
	stats := pqt.RowGroupStats{
		NumRows: 100,
		Columns: map[string]pqt.ColumnStats{
			"col": {
				HasStats:  true,
				MinValue:  int64(1),
				MaxValue:  int64(100),
				NullCount: 0,
			},
		},
	}
	// Unknown op should not prune
	if CanPruneRowGroup(StatsPredicate{"col", exec.CompareOp(99), int64(50)}, stats) {
		t.Fatal("should not prune for unknown op")
	}
}

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name string
		a, b any
		want int
	}{
		{"nil_nil", nil, nil, 0},
		{"nil_first", nil, int64(1), -1},
		{"nil_second", int64(1), nil, 1},
		{"int64_eq", int64(5), int64(5), 0},
		{"int64_lt", int64(3), int64(5), -1},
		{"int64_gt", int64(5), int64(3), 1},
		{"int32_converts", int32(5), int64(5), 0},
		{"float64_eq", 3.14, 3.14, 0},
		{"float64_lt", 1.0, 2.0, -1},
		{"float64_gt", 2.0, 1.0, 1},
		{"float32_converts", float32(5.0), float64(5.0), 0},
		{"string_eq", "abc", "abc", 0},
		{"string_lt", "abc", "xyz", -1},
		{"string_gt", "xyz", "abc", 1},
		{"string_vs_int", "abc", int64(5), 0},
		{"unknown", struct{}{}, struct{}{}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareValues(tt.a, tt.b)
			if got != tt.want {
				t.Fatalf("compareValues(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestToInt64Scan(t *testing.T) {
	tests := []struct {
		input any
		want  int64
	}{
		{int64(42), 42},
		{int32(10), 10},
		{int(5), 5},
		{float64(3.7), 3},
		{"string", 0},
		{nil, 0},
	}
	for _, tt := range tests {
		got := toInt64(tt.input)
		if got != tt.want {
			t.Fatalf("toInt64(%v) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestToFloat64Scan(t *testing.T) {
	tests := []struct {
		input any
		want  float64
	}{
		{float64(3.14), 3.14},
		{float32(2.5), 2.5},
		{int64(10), 10.0},
		{int(5), 5.0},
		{"string", 0},
		{nil, 0},
	}
	for _, tt := range tests {
		got := toFloat64(tt.input)
		if got != tt.want {
			t.Fatalf("toFloat64(%v) = %f, want %f", tt.input, got, tt.want)
		}
	}
}

func TestCanPruneAllNullColumnComparison(t *testing.T) {
	stats := pqt.RowGroupStats{
		NumRows: 100,
		Columns: map[string]pqt.ColumnStats{
			"all_null": {
				HasStats:  true,
				MinValue:  nil,
				MaxValue:  nil,
				NullCount: 100, // all null
			},
		},
	}

	// All nulls + comparison: should prune (comparison can't match null)
	if !CanPruneRowGroup(StatsPredicate{"all_null", exec.OpGt, int64(0)}, stats) {
		t.Fatal("expected prune for comparison on all-null column")
	}

	// IS NULL: should NOT prune (all values are null, so IS NULL matches)
	if CanPruneRowGroup(StatsPredicate{"all_null", exec.OpIsNull, nil}, stats) {
		t.Fatal("should not prune IS NULL on all-null column")
	}

	// IS NOT NULL: should prune (no non-null values)
	if !CanPruneRowGroup(StatsPredicate{"all_null", exec.OpIsNotNull, nil}, stats) {
		t.Fatal("expected prune for IS NOT NULL on all-null column")
	}
}

func TestCanPruneFloatStats(t *testing.T) {
	stats := pqt.RowGroupStats{
		NumRows: 100,
		Columns: map[string]pqt.ColumnStats{
			"temp": {
				HasStats:  true,
				MinValue:  float64(0.0),
				MaxValue:  float64(100.0),
				NullCount: 0,
			},
		},
	}

	// temp > 200 should prune (max is 100)
	if !CanPruneRowGroup(StatsPredicate{"temp", exec.OpGt, float64(200.0)}, stats) {
		t.Fatal("expected prune for GT above max")
	}

	// temp < -10 should prune (min is 0)
	if !CanPruneRowGroup(StatsPredicate{"temp", exec.OpLt, float64(-10.0)}, stats) {
		t.Fatal("expected prune for LT below min")
	}

	// temp = 50 should not prune
	if CanPruneRowGroup(StatsPredicate{"temp", exec.OpEq, float64(50.0)}, stats) {
		t.Fatal("should not prune for EQ within range")
	}
}
