package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestWindowLag(t *testing.T) {
	schema := []parquet.Column{
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"ts": int64(1), "value": 10.0},
		{"ts": int64(2), "value": 20.0},
		{"ts": int64(3), "value": 30.0},
		{"ts": int64(4), "value": 40.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinLag,
			InputCol:   "value",
			OutputCol:  "prev_value",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "ts", Order: Ascending}},
		},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	b, err := win.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result := b.ToRows()

	// First row should have nil (no previous row)
	if result[0]["prev_value"] != nil {
		t.Errorf("row 0: expected nil, got %v", result[0]["prev_value"])
	}
	// Second row should have 10.0
	if result[1]["prev_value"] != 10.0 {
		t.Errorf("row 1: expected 10.0, got %v", result[1]["prev_value"])
	}
	// Third row should have 20.0
	if result[2]["prev_value"] != 20.0 {
		t.Errorf("row 2: expected 20.0, got %v", result[2]["prev_value"])
	}
	// Fourth row should have 30.0
	if result[3]["prev_value"] != 30.0 {
		t.Errorf("row 3: expected 30.0, got %v", result[3]["prev_value"])
	}
}

func TestWindowLagWithOffset(t *testing.T) {
	schema := []parquet.Column{
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"ts": int64(1), "value": 10.0},
		{"ts": int64(2), "value": 20.0},
		{"ts": int64(3), "value": 30.0},
		{"ts": int64(4), "value": 40.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:           WinLag,
			InputCol:       "value",
			OutputCol:      "prev2",
			OutputType:     parquet.TypeFloat64,
			OrderBy:        []SortKey{{Column: "ts", Order: Ascending}},
			LagLeadOffset:  2,
			LagLeadDefault: 0.0,
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

	// Rows 0,1 should have default (0.0)
	if result[0]["prev2"] != 0.0 {
		t.Errorf("row 0: expected 0.0, got %v", result[0]["prev2"])
	}
	if result[1]["prev2"] != 0.0 {
		t.Errorf("row 1: expected 0.0, got %v", result[1]["prev2"])
	}
	// Row 2 should have value from row 0 (10.0)
	if result[2]["prev2"] != 10.0 {
		t.Errorf("row 2: expected 10.0, got %v", result[2]["prev2"])
	}
	// Row 3 should have value from row 1 (20.0)
	if result[3]["prev2"] != 20.0 {
		t.Errorf("row 3: expected 20.0, got %v", result[3]["prev2"])
	}
}

func TestWindowLead(t *testing.T) {
	schema := []parquet.Column{
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"ts": int64(1), "value": 10.0},
		{"ts": int64(2), "value": 20.0},
		{"ts": int64(3), "value": 30.0},
		{"ts": int64(4), "value": 40.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinLead,
			InputCol:   "value",
			OutputCol:  "next_value",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "ts", Order: Ascending}},
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

	if result[0]["next_value"] != 20.0 {
		t.Errorf("row 0: expected 20.0, got %v", result[0]["next_value"])
	}
	if result[1]["next_value"] != 30.0 {
		t.Errorf("row 1: expected 30.0, got %v", result[1]["next_value"])
	}
	if result[2]["next_value"] != 40.0 {
		t.Errorf("row 2: expected 40.0, got %v", result[2]["next_value"])
	}
	// Last row should be nil
	if result[3]["next_value"] != nil {
		t.Errorf("row 3: expected nil, got %v", result[3]["next_value"])
	}
}

func TestWindowFirstValue(t *testing.T) {
	schema := []parquet.Column{
		{Name: "dept", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
		{Name: "salary", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"dept": "eng", "name": "alice", "salary": 120.0},
		{"dept": "eng", "name": "bob", "salary": 100.0},
		{"dept": "eng", "name": "carol", "salary": 110.0},
		{"dept": "sales", "name": "dave", "salary": 90.0},
		{"dept": "sales", "name": "eve", "salary": 95.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:        WinFirstValue,
			InputCol:    "salary",
			OutputCol:   "first_sal",
			OutputType:  parquet.TypeFloat64,
			PartitionBy: []string{"dept"},
			OrderBy:     []SortKey{{Column: "salary", Order: Descending}},
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

	// All eng rows should have first_sal = 120.0 (highest salary, descending order)
	for _, row := range result {
		if row["dept"] == "eng" && row["first_sal"] != 120.0 {
			t.Errorf("eng row %v: expected first_sal=120.0, got %v", row["name"], row["first_sal"])
		}
		if row["dept"] == "sales" && row["first_sal"] != 95.0 {
			t.Errorf("sales row %v: expected first_sal=95.0, got %v", row["name"], row["first_sal"])
		}
	}
}

func TestWindowLastValue(t *testing.T) {
	schema := []parquet.Column{
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"ts": int64(1), "value": 10.0},
		{"ts": int64(2), "value": 20.0},
		{"ts": int64(3), "value": 30.0},
	}

	// Without ORDER BY: last value of entire partition
	win := NewWindow([]WindowColumn{
		{
			Func:       WinLastValue,
			InputCol:   "value",
			OutputCol:  "last_val",
			OutputType: parquet.TypeFloat64,
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

	for i, row := range result {
		if row["last_val"] != 30.0 {
			t.Errorf("row %d: expected last_val=30.0, got %v", i, row["last_val"])
		}
	}
}

func TestWindowLastValueWithOrderBy(t *testing.T) {
	schema := []parquet.Column{
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"ts": int64(1), "value": 10.0},
		{"ts": int64(2), "value": 20.0},
		{"ts": int64(3), "value": 30.0},
	}

	// With ORDER BY: running last value (= current row's value)
	win := NewWindow([]WindowColumn{
		{
			Func:       WinLastValue,
			InputCol:   "value",
			OutputCol:  "last_val",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "ts", Order: Ascending}},
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

	// Running last value = current row's value
	expected := []float64{10.0, 20.0, 30.0}
	for i, row := range result {
		if row["last_val"] != expected[i] {
			t.Errorf("row %d: expected last_val=%v, got %v", i, expected[i], row["last_val"])
		}
	}
}
