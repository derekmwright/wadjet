package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestMergeableSinkGrouped(t *testing.T) {
	schema := []parquet.Column{
		{Name: "grp", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeFloat64},
	}

	// Primary aggregate gets groups a,b
	rows1 := []map[string]any{
		{"grp": "a", "val": 10.0},
		{"grp": "b", "val": 20.0},
		{"grp": "a", "val": 5.0},
	}
	// Clone gets groups b,c
	rows2 := []map[string]any{
		{"grp": "b", "val": 30.0},
		{"grp": "c", "val": 40.0},
	}

	ctx := context.Background()

	primary := NewHashAggregate([]string{"grp"}, []AggColumn{
		{Func: AggSum, InputCol: "val", OutputCol: "total", OutputType: parquet.TypeFloat64},
		{Func: AggCount, InputCol: "val", OutputCol: "cnt", OutputType: parquet.TypeInt64},
	})
	primary.Init(ctx)

	b1 := batch.FromRows(schema, rows1)
	primary.Consume(ctx, b1)

	clone := primary.CloneSink()
	clone.Init(ctx)

	b2 := batch.FromRows(schema, rows2)
	clone.(Sink).Consume(ctx, b2)

	// Merge clone into primary
	primary.MergeSink(clone)
	primary.Finalize(ctx)

	// Read results
	results := make(map[string]map[string]any)
	for {
		out, err := primary.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for _, row := range out.ToRows() {
			grp := row["grp"].(string)
			results[grp] = row
		}
	}

	// Verify
	if len(results) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(results))
	}

	// a: sum=15, count=2
	if s, ok := results["a"]["total"].(float64); !ok || s != 15.0 {
		t.Errorf("group a: expected sum=15, got %v", results["a"]["total"])
	}

	// b: sum=50 (20+30), count=2
	if s, ok := results["b"]["total"].(float64); !ok || s != 50.0 {
		t.Errorf("group b: expected sum=50, got %v", results["b"]["total"])
	}

	// c: sum=40, count=1
	if s, ok := results["c"]["total"].(float64); !ok || s != 40.0 {
		t.Errorf("group c: expected sum=40, got %v", results["c"]["total"])
	}
}

func TestMergeableSinkScalar(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}

	ctx := context.Background()

	primary := NewHashAggregate(nil, []AggColumn{
		{Func: AggSum, InputCol: "val", OutputCol: "total", OutputType: parquet.TypeFloat64},
		{Func: AggCount, InputCol: "val", OutputCol: "cnt", OutputType: parquet.TypeInt64},
	})
	primary.Init(ctx)

	b1 := batch.FromRows(schema, []map[string]any{
		{"val": 10.0},
		{"val": 20.0},
	})
	primary.Consume(ctx, b1)

	clone := primary.CloneSink()
	clone.Init(ctx)

	b2 := batch.FromRows(schema, []map[string]any{
		{"val": 30.0},
		{"val": 40.0},
	})
	clone.(Sink).Consume(ctx, b2)

	primary.MergeSink(clone)
	primary.Finalize(ctx)

	out, err := primary.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("expected output batch")
	}

	rows := out.ToRows()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if s := rows[0]["total"].(float64); s != 100.0 {
		t.Errorf("expected sum=100, got %v", s)
	}
	if c := rows[0]["cnt"].(int64); c != 4 {
		t.Errorf("expected count=4, got %v", c)
	}
}

func TestMergeableSinkParallelPipeline(t *testing.T) {
	// Test the full pipeline with MergeableSink integration
	schema := []parquet.Column{
		{Name: "grp", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeInt64},
	}

	// Generate enough rows to span multiple batches (2048 rows each)
	// so parallel workers actually consume data.
	var rows []map[string]any
	groups := []string{"a", "b", "c"}
	numRows := 10000
	for i := 0; i < numRows; i++ {
		rows = append(rows, map[string]any{
			"grp": groups[i%3],
			"val": int64(i + 1),
		})
	}

	agg := NewHashAggregate([]string{"grp"}, []AggColumn{
		{Func: AggSum, InputCol: "val", OutputCol: "total", OutputType: parquet.TypeInt64},
		{Func: AggCount, InputCol: "val", OutputCol: "cnt", OutputType: parquet.TypeInt64},
	})

	source := NewSliceSource(schema, rows)

	pipe := &Pipeline{
		Source:  source,
		Ops:    nil,
		Sink:   agg,
		Workers: 4, // Force parallel
	}

	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	// Read results via Next
	results := make(map[string]map[string]any)
	for {
		out, err := agg.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for _, row := range out.ToRows() {
			grp := row["grp"].(string)
			results[grp] = row
		}
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 groups, got %d: %v", len(results), results)
	}

	// Expected sums: a gets vals 1,4,7,...,100 (i%3==0, i=0..99)
	// a: sum of (i+1 where i%3==0) = 1+4+7+...+100
	// b: sum of (i+1 where i%3==1) = 2+5+8+...+98
	// c: sum of (i+1 where i%3==2) = 3+6+9+...+99
	expectedSums := map[string]int64{
		"a": 0, "b": 0, "c": 0,
	}
	expectedCounts := map[string]int64{
		"a": 0, "b": 0, "c": 0,
	}
	for i := 0; i < numRows; i++ {
		g := groups[i%3]
		expectedSums[g] += int64(i + 1)
		expectedCounts[g]++
	}

	for g, expected := range expectedSums {
		got := results[g]["total"].(int64)
		if got != expected {
			t.Errorf("group %s: expected sum=%d, got %d", g, expected, got)
		}
	}
	for g, expected := range expectedCounts {
		got := results[g]["cnt"].(int64)
		if got != expected {
			t.Errorf("group %s: expected count=%d, got %d", g, expected, got)
		}
	}
}

func TestMergeableSinkCompactGroupKey(t *testing.T) {
	// Test with 2 short string columns to trigger the compact GROUP BY path.
	// This matches Q01's l_returnflag + l_linestatus pattern.
	schema := []parquet.Column{
		{Name: "flag", Type: parquet.TypeString},
		{Name: "status", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeFloat64},
	}

	flags := []string{"A", "N", "R"}
	statuses := []string{"F", "O"}
	numRows := 10000

	var rows []map[string]any
	for i := 0; i < numRows; i++ {
		rows = append(rows, map[string]any{
			"flag":   flags[i%3],
			"status": statuses[i%2],
			"val":    float64(i + 1),
		})
	}

	agg := NewHashAggregate([]string{"flag", "status"}, []AggColumn{
		{Func: AggSum, InputCol: "val", OutputCol: "total", OutputType: parquet.TypeFloat64},
		{Func: AggCount, InputCol: "val", OutputCol: "cnt", OutputType: parquet.TypeInt64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{
		Source:  source,
		Ops:    nil,
		Sink:   agg,
		Workers: 4,
	}

	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	results := make(map[string]map[string]any)
	for {
		out, err := agg.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for _, row := range out.ToRows() {
			key := row["flag"].(string) + "/" + row["status"].(string)
			results[key] = row
		}
	}

	// With 3 flags x 2 statuses = 6 groups
	if len(results) != 6 {
		t.Fatalf("expected 6 groups, got %d: %v", len(results), results)
	}

	// Verify each group's sum and count
	expectedSums := make(map[string]float64)
	expectedCounts := make(map[string]int64)
	for i := 0; i < numRows; i++ {
		key := flags[i%3] + "/" + statuses[i%2]
		expectedSums[key] += float64(i + 1)
		expectedCounts[key]++
	}

	for key, expected := range expectedSums {
		got := results[key]["total"].(float64)
		if got != expected {
			t.Errorf("group %s: expected sum=%f, got %f", key, expected, got)
		}
	}
	for key, expected := range expectedCounts {
		got := results[key]["cnt"].(int64)
		if got != expected {
			t.Errorf("group %s: expected count=%d, got %d", key, expected, got)
		}
	}
}

func TestMergeableSinkSort(t *testing.T) {
	// Test Sort as MergeableSink: parallel workers accumulate batches
	// independently, merge combines them, Finalize sorts everything.
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}

	numRows := 10000
	var rows []map[string]any
	for i := 0; i < numRows; i++ {
		rows = append(rows, map[string]any{
			"id":   int64(numRows - i), // reverse order
			"name": "row" + string(rune('A'+i%26)),
		})
	}

	sortOp := NewSort([]SortKey{{Column: "id", Order: Ascending}})

	pipe := &Pipeline{
		Source:  NewSliceSource(schema, rows),
		Ops:    nil,
		Sink:   sortOp,
		Workers: 4,
	}

	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify sorted order
	var prev int64
	count := 0
	for {
		out, err := sortOp.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for i := 0; i < out.ActiveLen(); i++ {
			ri := i
			if out.Sel != nil {
				ri = int(out.Sel[i])
			}
			val := out.Columns[0].Int64Data[ri]
			if val <= prev {
				t.Fatalf("not sorted: got %d after %d at position %d", val, prev, count)
			}
			prev = val
			count++
		}
	}
	if count != numRows {
		t.Fatalf("expected %d rows, got %d", numRows, count)
	}
}

func TestMergeableSinkSortWithLimit(t *testing.T) {
	// Test Sort MergeableSink with TopN limit.
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}

	numRows := 10000
	limit := 100
	var rows []map[string]any
	for i := 0; i < numRows; i++ {
		rows = append(rows, map[string]any{
			"val": int64(numRows - i),
		})
	}

	sortOp := NewSort([]SortKey{{Column: "val", Order: Ascending}})
	sortOp.Limit = limit

	pipe := &Pipeline{
		Source:  NewSliceSource(schema, rows),
		Ops:    nil,
		Sink:   sortOp,
		Workers: 4,
	}

	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	var prev int64
	count := 0
	for {
		out, err := sortOp.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for i := 0; i < out.ActiveLen(); i++ {
			ri := i
			if out.Sel != nil {
				ri = int(out.Sel[i])
			}
			val := out.Columns[0].Int64Data[ri]
			if val <= prev {
				t.Fatalf("not sorted: got %d after %d", val, prev)
			}
			prev = val
			count++
		}
	}
	if count != limit {
		t.Fatalf("expected %d rows (limit), got %d", limit, count)
	}
	// Last value should be the limit-th smallest = 100
	if prev != int64(limit) {
		t.Fatalf("expected last value %d, got %d", limit, prev)
	}
}
