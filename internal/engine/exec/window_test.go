package exec

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
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

// TestWindowSpillToDisk verifies that the Window operator correctly spills
// input batches to disk under memory pressure and produces correct results.
func TestWindowSpillToDisk(t *testing.T) {
	schema := []parquet.Column{
		{Name: "dept", Type: parquet.TypeString},
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "salary", Type: parquet.TypeFloat64},
	}

	// 5000 rows across 2 departments — produces 3 batches at DefaultBatchSize=2048
	const totalRows = 5000
	var rows []map[string]any
	for i := 0; i < totalRows; i++ {
		dept := "eng"
		if i%2 == 1 {
			dept = "sales"
		}
		rows = append(rows, map[string]any{
			"dept":   dept,
			"id":     int64(i),
			"salary": float64(totalRows - i),
		})
	}

	tmpDir, err := os.MkdirTemp("", "window-spill-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Budget of 250KB — triggers spill after ~1 batch (each batch ≈200KB)
	tracker := memory.NewTracker("test", 250_000)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	win := NewWindow([]WindowColumn{
		{
			Func:        WinRowNumber,
			OutputCol:   "rn",
			OutputType:  parquet.TypeInt64,
			PartitionBy: []string{"dept"},
			OrderBy:     []SortKey{{Column: "salary", Order: Descending}},
		},
	})
	win.Spill = sm

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify spill was triggered
	spilledFiles := sm.SpilledFiles()
	if len(spilledFiles) == 0 {
		t.Fatal("expected spill to trigger, but no spill files were created")
	}
	t.Logf("spilled %d files", len(spilledFiles))

	// Collect all output batches
	var allRows []map[string]any
	for {
		b, err := win.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		allRows = append(allRows, b.ToRows()...)
	}

	if len(allRows) != totalRows {
		t.Fatalf("expected %d rows, got %d", totalRows, len(allRows))
	}

	// Verify ROW_NUMBER is correct per partition
	engCount, salesCount := 0, 0
	engRNs := make(map[int64]bool)
	salesRNs := make(map[int64]bool)
	for _, row := range allRows {
		dept := row["dept"].(string)
		rn := row["rn"].(int64)
		switch dept {
		case "eng":
			engCount++
			if engRNs[rn] {
				t.Fatalf("duplicate eng rn: %d", rn)
			}
			engRNs[rn] = true
		case "sales":
			salesCount++
			if salesRNs[rn] {
				t.Fatalf("duplicate sales rn: %d", rn)
			}
			salesRNs[rn] = true
		default:
			t.Fatalf("unexpected dept: %s", dept)
		}
	}

	// 5000 rows split evenly: 2500 eng (even ids), 2500 sales (odd ids)
	if engCount != 2500 {
		t.Errorf("eng rows: got %d, want 2500", engCount)
	}
	if salesCount != 2500 {
		t.Errorf("sales rows: got %d, want 2500", salesCount)
	}

	// ROW_NUMBER should be 1..2500 for each partition
	for i := int64(1); i <= 2500; i++ {
		if !engRNs[i] {
			t.Fatalf("eng missing rn=%d", i)
		}
		if !salesRNs[i] {
			t.Fatalf("sales missing rn=%d", i)
		}
	}
}

// TestWindowSpillRunningSum verifies spill correctness with an aggregate
// window function (SUM with ORDER BY → running total).
func TestWindowSpillRunningSum(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeFloat64},
	}

	const totalRows = 5000
	var rows []map[string]any
	var expectedSum float64
	for i := 0; i < totalRows; i++ {
		v := float64(i + 1)
		rows = append(rows, map[string]any{
			"id":  int64(i),
			"val": v,
		})
		expectedSum += v
	}

	tmpDir, err := os.MkdirTemp("", "window-spill-sum-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	tracker := memory.NewTracker("test", 250_000)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	win := NewWindow([]WindowColumn{
		{
			Func:       WinSum,
			InputCol:   "val",
			OutputCol:  "running_total",
			OutputType: parquet.TypeFloat64,
			OrderBy:    []SortKey{{Column: "id", Order: Ascending}},
		},
	})
	win.Spill = sm

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if len(sm.SpilledFiles()) == 0 {
		t.Fatal("expected spill to trigger")
	}

	// Collect results
	var allRows []map[string]any
	for {
		b, err := win.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		allRows = append(allRows, b.ToRows()...)
	}

	if len(allRows) != totalRows {
		t.Fatalf("expected %d rows, got %d", totalRows, len(allRows))
	}

	// Find the row with the last id (highest running total = sum of all)
	var maxRunning float64
	for _, row := range allRows {
		rt := row["running_total"].(float64)
		if rt > maxRunning {
			maxRunning = rt
		}
	}

	// The maximum running total should equal sum(1..5000) = 12502500
	if fmt.Sprintf("%.0f", maxRunning) != fmt.Sprintf("%.0f", expectedSum) {
		t.Errorf("max running total: got %.0f, want %.0f", maxRunning, expectedSum)
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
