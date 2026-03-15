package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func TestWindowRowNumber(t *testing.T) {
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
			Func:        WinRowNumber,
			OutputCol:   "rn",
			OutputType:  parquet.TypeInt64,
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

	b, err := win.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b == nil {
		t.Fatal("expected non-nil batch")
	}

	result := b.ToRows()
	if len(result) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result))
	}

	// Build a map of name -> rn for checking
	rnByName := make(map[string]int64)
	for _, row := range result {
		name := row["name"].(string)
		rn := row["rn"].(int64)
		rnByName[name] = rn
	}

	// Eng partition ordered by salary DESC: alice(120)=1, carol(110)=2, bob(100)=3
	if rnByName["alice"] != 1 {
		t.Errorf("alice rn: got %d, want 1", rnByName["alice"])
	}
	if rnByName["carol"] != 2 {
		t.Errorf("carol rn: got %d, want 2", rnByName["carol"])
	}
	if rnByName["bob"] != 3 {
		t.Errorf("bob rn: got %d, want 3", rnByName["bob"])
	}

	// Sales partition ordered by salary DESC: eve(95)=1, dave(90)=2
	if rnByName["eve"] != 1 {
		t.Errorf("eve rn: got %d, want 1", rnByName["eve"])
	}
	if rnByName["dave"] != 2 {
		t.Errorf("dave rn: got %d, want 2", rnByName["dave"])
	}
}

func TestWindowRank(t *testing.T) {
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
		{Name: "score", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"name": "alice", "score": 95.0},
		{"name": "bob", "score": 90.0},
		{"name": "carol", "score": 95.0}, // tie with alice
		{"name": "dave", "score": 80.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinRank,
			OutputCol:  "rnk",
			OutputType: parquet.TypeInt64,
			OrderBy:    []SortKey{{Column: "score", Order: Descending}},
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
	rankByName := make(map[string]int64)
	for _, row := range result {
		rankByName[row["name"].(string)] = row["rnk"].(int64)
	}

	// score DESC: alice(95)=1, carol(95)=1 (tie), bob(90)=3 (gap!), dave(80)=4
	if rankByName["alice"] != 1 {
		t.Errorf("alice rank: got %d, want 1", rankByName["alice"])
	}
	if rankByName["carol"] != 1 {
		t.Errorf("carol rank: got %d, want 1", rankByName["carol"])
	}
	if rankByName["bob"] != 3 {
		t.Errorf("bob rank: got %d, want 3", rankByName["bob"])
	}
	if rankByName["dave"] != 4 {
		t.Errorf("dave rank: got %d, want 4", rankByName["dave"])
	}
}

func TestWindowDenseRank(t *testing.T) {
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
		{Name: "score", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"name": "alice", "score": 95.0},
		{"name": "bob", "score": 90.0},
		{"name": "carol", "score": 95.0}, // tie with alice
		{"name": "dave", "score": 80.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinDenseRank,
			OutputCol:  "dr",
			OutputType: parquet.TypeInt64,
			OrderBy:    []SortKey{{Column: "score", Order: Descending}},
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
	drByName := make(map[string]int64)
	for _, row := range result {
		drByName[row["name"].(string)] = row["dr"].(int64)
	}

	// DENSE_RANK: alice(95)=1, carol(95)=1, bob(90)=2 (no gap!), dave(80)=3
	if drByName["alice"] != 1 {
		t.Errorf("alice dense_rank: got %d, want 1", drByName["alice"])
	}
	if drByName["carol"] != 1 {
		t.Errorf("carol dense_rank: got %d, want 1", drByName["carol"])
	}
	if drByName["bob"] != 2 {
		t.Errorf("bob dense_rank: got %d, want 2", drByName["bob"])
	}
	if drByName["dave"] != 3 {
		t.Errorf("dave dense_rank: got %d, want 3", drByName["dave"])
	}
}

func TestWindowRunningSum(t *testing.T) {
	schema := []parquet.Column{
		{Name: "dept", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"dept": "eng", "name": "alice", "amount": 10.0},
		{"dept": "eng", "name": "bob", "amount": 20.0},
		{"dept": "eng", "name": "carol", "amount": 30.0},
		{"dept": "sales", "name": "dave", "amount": 5.0},
		{"dept": "sales", "name": "eve", "amount": 15.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:        WinSum,
			InputCol:    "amount",
			OutputCol:   "running_total",
			OutputType:  parquet.TypeFloat64,
			PartitionBy: []string{"dept"},
			OrderBy:     []SortKey{{Column: "amount", Order: Ascending}},
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
	sumByName := make(map[string]float64)
	for _, row := range result {
		sumByName[row["name"].(string)] = row["running_total"].(float64)
	}

	// Eng partition ordered by amount ASC: alice(10)=10, bob(20)=30, carol(30)=60
	if sumByName["alice"] != 10.0 {
		t.Errorf("alice running_total: got %f, want 10.0", sumByName["alice"])
	}
	if sumByName["bob"] != 30.0 {
		t.Errorf("bob running_total: got %f, want 30.0", sumByName["bob"])
	}
	if sumByName["carol"] != 60.0 {
		t.Errorf("carol running_total: got %f, want 60.0", sumByName["carol"])
	}

	// Sales partition: dave(5)=5, eve(15)=20
	if sumByName["dave"] != 5.0 {
		t.Errorf("dave running_total: got %f, want 5.0", sumByName["dave"])
	}
	if sumByName["eve"] != 20.0 {
		t.Errorf("eve running_total: got %f, want 20.0", sumByName["eve"])
	}
}

func TestWindowPartitionCount(t *testing.T) {
	schema := []parquet.Column{
		{Name: "dept", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
	}

	rows := []map[string]any{
		{"dept": "eng", "name": "alice"},
		{"dept": "eng", "name": "bob"},
		{"dept": "eng", "name": "carol"},
		{"dept": "sales", "name": "dave"},
		{"dept": "sales", "name": "eve"},
	}

	// COUNT(*) OVER (PARTITION BY dept) — no ORDER BY = partition-level count
	win := NewWindow([]WindowColumn{
		{
			Func:        WinCount,
			OutputCol:   "dept_size",
			OutputType:  parquet.TypeInt64,
			PartitionBy: []string{"dept"},
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
	for _, row := range result {
		dept := row["dept"].(string)
		cnt := row["dept_size"].(int64)
		switch dept {
		case "eng":
			if cnt != 3 {
				t.Errorf("eng dept_size: got %d, want 3", cnt)
			}
		case "sales":
			if cnt != 2 {
				t.Errorf("sales dept_size: got %d, want 2", cnt)
			}
		}
	}
}

func TestWindowMultipleFunctions(t *testing.T) {
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
		{Name: "score", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"name": "alice", "score": 95.0},
		{"name": "bob", "score": 85.0},
		{"name": "carol", "score": 90.0},
	}

	win := NewWindow([]WindowColumn{
		{
			Func:       WinRowNumber,
			OutputCol:  "rn",
			OutputType: parquet.TypeInt64,
			OrderBy:    []SortKey{{Column: "score", Order: Descending}},
		},
		{
			Func:       WinRank,
			OutputCol:  "rnk",
			OutputType: parquet.TypeInt64,
			OrderBy:    []SortKey{{Column: "score", Order: Descending}},
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
	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}

	// Verify both window columns are present
	for _, row := range result {
		if _, ok := row["rn"]; !ok {
			t.Error("missing 'rn' column")
		}
		if _, ok := row["rnk"]; !ok {
			t.Error("missing 'rnk' column")
		}
	}
}
