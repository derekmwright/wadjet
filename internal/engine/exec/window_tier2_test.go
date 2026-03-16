package exec

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func TestWindowNtile(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(3)},
		{"id": int64(4)},
		{"id": int64(5)},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:         WinNtile,
			OutputCol:    "bucket",
			OutputType:   parquet.TypeInt64,
			OrderBy:      []SortKey{{Column: "id", Order: Ascending}},
			NtileBuckets: 3,
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()

	// 5 rows into 3 buckets: 2, 2, 1
	expected := []int64{1, 1, 2, 2, 3}
	for i, row := range result {
		bucket := row["bucket"].(int64)
		if bucket != expected[i] {
			t.Errorf("row %d: bucket=%d, want %d", i, bucket, expected[i])
		}
	}
}

func TestWindowPercentRank(t *testing.T) {
	schema := []parquet.Column{
		{Name: "score", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"score": 10.0},
		{"score": 20.0},
		{"score": 30.0},
		{"score": 40.0},
		{"score": 50.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinPercentRank,
			OutputCol:  "pct_rank",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "score", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()

	// percent_rank = (rank - 1) / (n - 1), n=5
	expected := []float64{0.0, 0.25, 0.5, 0.75, 1.0}
	for i, row := range result {
		pr := row["pct_rank"].(float64)
		if math.Abs(pr-expected[i]) > 1e-10 {
			t.Errorf("row %d: percent_rank=%v, want %v", i, pr, expected[i])
		}
	}
}

func TestWindowCumeDist(t *testing.T) {
	schema := []parquet.Column{
		{Name: "score", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"score": 10.0},
		{"score": 20.0},
		{"score": 20.0}, // tie
		{"score": 30.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinCumeDist,
			OutputCol:  "cd",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "score", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()

	// cume_dist = peer_group_end / n
	// Row 0 (score=10): 1/4 = 0.25
	// Rows 1,2 (score=20): 3/4 = 0.75
	// Row 3 (score=30): 4/4 = 1.0
	expected := []float64{0.25, 0.75, 0.75, 1.0}
	for i, row := range result {
		cd := row["cd"].(float64)
		if math.Abs(cd-expected[i]) > 1e-10 {
			t.Errorf("row %d: cume_dist=%v, want %v", i, cd, expected[i])
		}
	}
}

func TestWindowNthValue(t *testing.T) {
	schema := []parquet.Column{
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"ts": int64(1), "value": 10.0},
		{"ts": int64(2), "value": 20.0},
		{"ts": int64(3), "value": 30.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinNthValue,
			InputCol:   "value",
			OutputCol:  "second_val",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "ts", Order: Ascending}},
			NthValueN:  2,
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, _ := win.Next(ctx)
	result := b.ToRows()

	// All rows should have the 2nd value (20.0)
	for i, row := range result {
		if row["second_val"] != 20.0 {
			t.Errorf("row %d: nth_value=%v, want 20.0", i, row["second_val"])
		}
	}
}
