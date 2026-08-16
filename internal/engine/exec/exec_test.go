package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestFilter(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"id": int64(1), "value": 10.0},
		{"id": int64(2), "value": 25.0},
		{"id": int64(3), "value": 5.0},
		{"id": int64(4), "value": 30.0},
	}

	source := NewSliceSource(schema, rows)
	filter := NewFilter(ColumnCompare("value", OpGt, 15.0))
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{filter}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(sink.Rows))
	}
}

func TestHashAggregate(t *testing.T) {
	schema := []parquet.Column{
		{Name: "group", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"group": "a", "amount": 10.0},
		{"group": "b", "amount": 20.0},
		{"group": "a", "amount": 30.0},
		{"group": "b", "amount": 40.0},
	}

	agg := NewHashAggregate([]string{"group"}, []AggColumn{
		{Func: AggSum, InputCol: "amount", OutputCol: "total", OutputType: parquet.TypeFloat64},
		{Func: AggCount, InputCol: "amount", OutputCol: "cnt", OutputType: parquet.TypeInt64},
	})

	source := NewSliceSource(schema, rows)
	sink := &CollectSink{}

	// Run source -> agg (as sink)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Read from agg (as source)
	ctx := context.Background()
	b, err := agg.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = sink

	if b == nil {
		t.Fatal("expected non-nil batch")
	}

	resultRows := b.ToRows()
	if len(resultRows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(resultRows))
	}

	// Find group "a"
	for _, row := range resultRows {
		if row["group"] == "a" {
			total, ok := row["total"].(float64)
			if !ok || total != 40.0 {
				t.Fatalf("expected total=40.0 for group 'a', got %v", row["total"])
			}
			cnt, ok := row["cnt"].(int64)
			if !ok || cnt != 2 {
				t.Fatalf("expected cnt=2 for group 'a', got %v", row["cnt"])
			}
		}
	}
}

func TestHashAggregateGroupingSets(t *testing.T) {
	schema := []parquet.Column{
		{Name: "region", Type: parquet.TypeString},
		{Name: "product", Type: parquet.TypeString},
		{Name: "sales", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"region": "east", "product": "A", "sales": 10.0},
		{"region": "east", "product": "B", "sales": 20.0},
		{"region": "west", "product": "A", "sales": 30.0},
		{"region": "west", "product": "B", "sales": 40.0},
	}

	// GROUPING SETS ((region, product), (region), ())
	// Columns: region=0, product=1
	agg := NewHashAggregate([]string{"region", "product"}, []AggColumn{
		{Func: AggSum, InputCol: "sales", OutputCol: "total", OutputType: parquet.TypeFloat64},
	})
	agg.GroupingSets = [][]int{
		{0, 1}, // (region, product)
		{0},    // (region)
		{},     // ()
	}

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	var allRows []map[string]any
	ctx := context.Background()
	for {
		b, err := agg.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		allRows = append(allRows, b.ToRows()...)
	}

	// Expect: 4 (region,product) + 2 (region) + 1 (grand total) = 7 groups
	if len(allRows) != 7 {
		t.Fatalf("expected 7 groups, got %d: %v", len(allRows), allRows)
	}

	// Verify grand total (both region and product should be nil)
	var grandTotal float64
	var foundGrand bool
	for _, row := range allRows {
		if row["region"] == nil && row["product"] == nil {
			grandTotal = row["total"].(float64)
			foundGrand = true
		}
	}
	if !foundGrand {
		t.Fatal("expected grand total group (both cols nil)")
	}
	if grandTotal != 100.0 {
		t.Fatalf("expected grand total=100.0, got %v", grandTotal)
	}

	// Verify region-only totals (product should be nil)
	regionTotals := map[string]float64{}
	for _, row := range allRows {
		if row["region"] != nil && row["product"] == nil {
			regionTotals[row["region"].(string)] = row["total"].(float64)
		}
	}
	if regionTotals["east"] != 30.0 {
		t.Fatalf("expected east total=30.0, got %v", regionTotals["east"])
	}
	if regionTotals["west"] != 70.0 {
		t.Fatalf("expected west total=70.0, got %v", regionTotals["west"])
	}
}

func TestHashAggregateCountDistinct(t *testing.T) {
	schema := []parquet.Column{
		{Name: "group", Type: parquet.TypeString},
		{Name: "status", Type: parquet.TypeString},
	}

	rows := []map[string]any{
		{"group": "a", "status": "active"},
		{"group": "a", "status": "active"},   // duplicate
		{"group": "a", "status": "inactive"},
		{"group": "b", "status": "active"},
		{"group": "b", "status": "active"},   // duplicate
		{"group": "b", "status": "active"},   // duplicate
		{"group": "b", "status": "pending"},
	}

	agg := NewHashAggregate([]string{"group"}, []AggColumn{
		{Func: AggCountDistinct, InputCol: "status", OutputCol: "distinct_statuses", OutputType: parquet.TypeInt64},
		{Func: AggCount, InputCol: "status", OutputCol: "total_count", OutputType: parquet.TypeInt64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, err := agg.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b == nil {
		t.Fatal("expected non-nil batch")
	}

	resultRows := b.ToRows()
	if len(resultRows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(resultRows))
	}

	for _, row := range resultRows {
		g := row["group"]
		dc := row["distinct_statuses"].(int64)
		tc := row["total_count"].(int64)
		switch g {
		case "a":
			if dc != 2 {
				t.Errorf("group 'a': expected 2 distinct statuses, got %d", dc)
			}
			if tc != 3 {
				t.Errorf("group 'a': expected total count 3, got %d", tc)
			}
		case "b":
			if dc != 2 {
				t.Errorf("group 'b': expected 2 distinct statuses, got %d", dc)
			}
			if tc != 4 {
				t.Errorf("group 'b': expected total count 4, got %d", tc)
			}
		default:
			t.Errorf("unexpected group: %v", g)
		}
	}
}

func TestSort(t *testing.T) {
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
		{Name: "score", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"name": "charlie", "score": 75.0},
		{"name": "alice", "score": 95.0},
		{"name": "bob", "score": 85.0},
	}

	sortOp := NewSort([]SortKey{{Column: "score", Order: Descending}})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: sortOp}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	result, err := sortOp.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	resultRows := result.ToRows()
	if len(resultRows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(resultRows))
	}
	if resultRows[0]["score"].(float64) != 95.0 {
		t.Fatalf("expected first score=95.0, got %v", resultRows[0]["score"])
	}
	if resultRows[2]["score"].(float64) != 75.0 {
		t.Fatalf("expected last score=75.0, got %v", resultRows[2]["score"])
	}
}

func TestLimit(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}

	var rows []map[string]any
	for i := 0; i < 100; i++ {
		rows = append(rows, map[string]any{"id": int64(i)})
	}

	source := NewSliceSource(schema, rows)
	limit := NewLimit(5, 0)
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{limit}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(sink.Rows))
	}
}

func TestLimitOffset(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}

	var rows []map[string]any
	for i := 0; i < 100; i++ {
		rows = append(rows, map[string]any{"id": int64(i)})
	}

	source := NewSliceSource(schema, rows)
	limit := NewLimit(5, 10) // skip 10, take 5
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{limit}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(sink.Rows))
	}
	// First row should be id=10 (after skipping 10)
	if id, ok := sink.Rows[0]["id"].(int64); !ok || id != 10 {
		t.Errorf("expected first row id=10, got %v", sink.Rows[0]["id"])
	}
	// Last row should be id=14
	if id, ok := sink.Rows[4]["id"].(int64); !ok || id != 14 {
		t.Errorf("expected last row id=14, got %v", sink.Rows[4]["id"])
	}
}

func TestTopNSort(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeFloat64},
	}

	var rows []map[string]any
	for i := 0; i < 1000; i++ {
		rows = append(rows, map[string]any{"id": int64(i), "val": float64(1000 - i)})
	}

	source := NewSliceSource(schema, rows)
	sortOp := NewSort([]SortKey{{Column: "val", Order: Ascending}})
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Sink: sortOp}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	sortOp.Truncate(5) // Top-5

	var result []map[string]any
	for {
		b, err := sortOp.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		result = append(result, b.ToRows()...)
	}
	_ = sink

	if len(result) != 5 {
		t.Fatalf("expected 5 rows after TopN, got %d", len(result))
	}
	// Ascending sort: smallest val first. val=1 (id=999), val=2 (id=998), etc.
	if v, ok := result[0]["val"].(float64); !ok || v != 1.0 {
		t.Errorf("expected first val=1.0, got %v", result[0]["val"])
	}
	if v, ok := result[4]["val"].(float64); !ok || v != 5.0 {
		t.Errorf("expected last val=5.0, got %v", result[4]["val"])
	}
}

func TestLimit_EarlyTermination(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}

	// Create 10,000 rows — LIMIT 5 should stop pulling after first batch
	rows := make([]map[string]any, 10000)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i)}
	}

	source := &countingSource{inner: NewSliceSource(schema, rows)}
	limit := NewLimit(5, 0)
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{limit}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(sink.Rows))
	}

	// With 10,000 rows at batch size 2048, without early termination we'd
	// need 5 batches. With early termination, only 1 batch should be read
	// (first batch has 2048 rows, LIMIT 5 is satisfied).
	if source.nextCalls > 2 {
		t.Errorf("expected ≤2 source.Next() calls (early termination), got %d", source.nextCalls)
	}
}

func TestLimit_WithOffset(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}

	rows := make([]map[string]any, 100)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i)}
	}

	source := NewSliceSource(schema, rows)
	limit := NewLimit(3, 10) // OFFSET 10 LIMIT 3
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{limit}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(sink.Rows))
	}
	// First row should be id=10 (offset skips 0-9)
	if sink.Rows[0]["id"].(int64) != 10 {
		t.Errorf("expected first id=10, got %v", sink.Rows[0]["id"])
	}
}

func TestLimit_Done(t *testing.T) {
	limit := NewLimit(10, 0)
	if limit.Done() {
		t.Error("should not be done initially")
	}
}

// countingSource wraps a Source and counts Next() calls.
type countingSource struct {
	inner     Source
	nextCalls int
}

func (c *countingSource) Init(ctx context.Context) error { return c.inner.Init(ctx) }
func (c *countingSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	c.nextCalls++
	return c.inner.Next(ctx)
}
func (c *countingSource) Close() error { return c.inner.Close() }

func TestProject(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeFloat64},
		{Name: "b", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"a": 10.0, "b": 3.0},
		{"a": 20.0, "b": 7.0},
	}

	proj := NewProject([]ProjectColumn{
		{Name: "a", Type: parquet.TypeFloat64, Expr: ColumnRef("a")},
		{Name: "sum_ab", Type: parquet.TypeFloat64, Expr: ArithExpr(ColumnRef("a"), ColumnRef("b"), "+")},
	})

	source := NewSliceSource(schema, rows)
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{proj}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(sink.Rows))
	}
	if sink.Rows[0]["sum_ab"].(float64) != 13.0 {
		t.Fatalf("expected sum_ab=13.0, got %v", sink.Rows[0]["sum_ab"])
	}
}

func TestColumnLike(t *testing.T) {
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
	}

	rows := []map[string]any{
		{"name": "alice"},
		{"name": "bob"},
		{"name": "alex"},
		{"name": "carol"},
		{"name": "ali"},
	}

	tests := []struct {
		name     string
		pattern  string
		not      bool
		expected int
	}{
		{"prefix", "al%", false, 3},      // alice, alex, ali
		{"suffix", "%ob", false, 1},       // bob
		{"contains", "%li%", false, 2},    // alice, ali
		{"single char", "al_x", false, 1}, // alex
		{"exact", "bob", false, 1},
		{"not like", "al%", true, 2}, // bob, carol
		{"no match", "xyz%", false, 0},
		{"match all", "%", false, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := NewSliceSource(schema, rows)
			filter := NewFilter(ColumnLike("name", tt.pattern, tt.not))
			sink := &CollectSink{}
			pipe := &Pipeline{Source: source, Ops: []UnaryOperator{filter}, Sink: sink}
			if err := pipe.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(sink.Rows) != tt.expected {
				t.Fatalf("pattern %q not=%v: expected %d rows, got %d", tt.pattern, tt.not, tt.expected, len(sink.Rows))
			}
		})
	}
}

// TestHashAggregateMergeThenSpillFinalize reproduces a panic where MergeSink
// migrates compact-key groups to generic mode, then Finalize re-resolves
// indices and switches back to compact mode with a fresh intGroupStates,
// losing the merged groups. This caused index-out-of-bounds in Next().
// Regression test for SF100 Q09 panic: aggregate.go:1848.
func TestHashAggregateMergeThenSpillFinalize(t *testing.T) {
	// Use compact-eligible keys: two Int32 columns (4+4 = 8 bytes, fits int64).
	schema := []parquet.Column{
		{Name: "year", Type: parquet.TypeInt32},
		{Name: "region", Type: parquet.TypeInt32},
		{Name: "amount", Type: parquet.TypeFloat64},
	}

	ctx := context.Background()

	// Set up spill directory. Use a budget large enough for one batch but
	// not two — the first batch is consumed normally, the second is spilled.
	spillDir := t.TempDir()
	tracker := memory.NewTracker("test", 100_000) // 100 KB
	sm, err := memory.NewSpillManager(spillDir, tracker)
	if err != nil {
		t.Fatal(err)
	}

	// Primary aggregate with spill enabled.
	primary := NewHashAggregate([]string{"year", "region"}, []AggColumn{
		{Func: AggSum, InputCol: "amount", OutputCol: "total", OutputType: parquet.TypeFloat64},
	})
	primary.Spill = sm
	if err := primary.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Feed batch 1 to primary (consumed normally — under budget).
	b1 := batch.FromRows(schema, []map[string]any{
		{"year": int32(2024), "region": int32(1), "amount": 10.0},
		{"year": int32(2025), "region": int32(2), "amount": 20.0},
	})
	if err := primary.Consume(ctx, b1); err != nil {
		t.Fatal(err)
	}

	// Push tracker over 80% threshold so next Consume spills.
	tracker.ForceReserve(90_000)

	// Feed batch 2 to primary — tracker is now over budget, should be spilled.
	b2 := batch.FromRows(schema, []map[string]any{
		{"year": int32(2024), "region": int32(1), "amount": 30.0},
		{"year": int32(2026), "region": int32(3), "amount": 50.0},
	})
	if err := primary.Consume(ctx, b2); err != nil {
		t.Fatal(err)
	}

	// Spill exercised when any of the three sinks recorded activity:
	// legacy raw-row file, in-memory buffer (small payloads), or external-
	// merge partial-state file. The simple-aggs SoA path now routes through
	// partialSpillFiles, so a successful spill leaves spillFiles=0 here.
	if len(primary.spillFiles) == 0 && len(primary.spillBuffer) == 0 &&
		len(primary.partialSpillFiles) == 0 {
		t.Fatal("expected spill path to be exercised but nothing was written or buffered")
	}

	// Clone aggregate with different groups (simulates worker 1).
	clone := primary.CloneSink().(*HashAggregate)
	if err := clone.Init(ctx); err != nil {
		t.Fatal(err)
	}
	b3 := batch.FromRows(schema, []map[string]any{
		{"year": int32(2024), "region": int32(1), "amount": 100.0},
		{"year": int32(2027), "region": int32(4), "amount": 200.0},
	})
	if err := clone.Consume(ctx, b3); err != nil {
		t.Fatal(err)
	}

	// Merge clone into primary — this migrates compact → generic.
	primary.MergeSink(clone)

	// Finalize processes spilled rows. Before the fix, this would
	// re-resolve indices, switch back to compact mode, and lose merged groups.
	if err := primary.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	// Next() should return all groups without panicking.
	var allRows []map[string]any
	for {
		out, err := primary.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		allRows = append(allRows, out.ToRows()...)
	}

	// Should have groups from primary batch 1, spilled batch 2, and merged clone.
	// Groups: (2024,1), (2025,2), (2026,3), (2027,4) — 4 distinct groups.
	if len(allRows) < 3 {
		t.Fatalf("expected at least 3 groups, got %d: %v", len(allRows), allRows)
	}

	// Verify (2024,1) total includes all sources: 10 + 30 + 100 = 140.
	for _, row := range allRows {
		y, _ := row["year"].(int32)
		r, _ := row["region"].(int32)
		if y == 2024 && r == 1 {
			total, _ := row["total"].(float64)
			if total != 140.0 {
				t.Fatalf("expected total=140 for (2024,1), got %v", total)
			}
		}
	}
}

// TestHashAggregateMergeThenSpillFinalize_HighCardinality is the high-N
// counterpart to TestHashAggregateMergeThenSpillFinalize. It exercises the
// post-MergeSink AoS path through partialGroupCursor at a group count that
// the small (4-group) test cannot meaningfully cover: the cursor's
// useAoSAccs branch reads from gs.extras.accs (populated by migrate's
// materializeFlatAccums) instead of the now-cleared intFlatAccs.
//
// Flow: primary spills once, clone runs in parallel, MergeSink merges them
// via the generic-map path (compact→generic migration), then Finalize runs
// finalizeViaPartialMerge which builds a cursor over the post-merge state.
// Without the AoS fallback the cursor would panic at len(c.flatAccs)==0;
// without correct AoS reading every group's SUM would be zero.
func TestHashAggregateMergeThenSpillFinalize_HighCardinality(t *testing.T) {
	schema := []parquet.Column{
		{Name: "year", Type: parquet.TypeInt32},
		{Name: "region", Type: parquet.TypeInt32},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	ctx := context.Background()

	// Generate 4000 (year, region) pairs split between primary and clone with
	// a 50% overlap so MergeSink hits both the "found" and "not found"
	// branches in the generic-map path.
	const numYears = 80
	const numRegions = 50
	type expacc struct{ sum float64 }
	expected := make(map[[2]int32]*expacc, numYears*numRegions)

	primaryBatches := make([]*batch.RecordBatch, 0, 2)
	cloneBatches := make([]*batch.RecordBatch, 0, 1)

	// Primary: years [0, numYears) × regions [0, numRegions/2).
	rowsP1 := make([]map[string]any, 0, numYears*numRegions/2)
	rowsP2 := make([]map[string]any, 0, numYears*numRegions/2)
	for y := int32(0); y < int32(numYears); y++ {
		for r := int32(0); r < int32(numRegions/2); r++ {
			v1 := float64(int(y)*1000 + int(r))
			v2 := float64(int(y)*100 + int(r) + 7)
			rowsP1 = append(rowsP1, map[string]any{"year": y, "region": r, "amount": v1})
			rowsP2 = append(rowsP2, map[string]any{"year": y, "region": r, "amount": v2})
			key := [2]int32{y, r}
			if expected[key] == nil {
				expected[key] = &expacc{}
			}
			expected[key].sum += v1 + v2
		}
	}
	primaryBatches = append(primaryBatches, batch.FromRows(schema, rowsP1))
	primaryBatches = append(primaryBatches, batch.FromRows(schema, rowsP2))

	// Clone: years [0, numYears) × regions [numRegions/4, 3*numRegions/4) so
	// half overlaps with primary, half is fresh.
	rowsC := make([]map[string]any, 0, numYears*numRegions/2)
	for y := int32(0); y < int32(numYears); y++ {
		for r := int32(numRegions / 4); r < int32(3*numRegions/4); r++ {
			v := float64(int(y)*5000 + int(r)*3)
			rowsC = append(rowsC, map[string]any{"year": y, "region": r, "amount": v})
			key := [2]int32{y, r}
			if expected[key] == nil {
				expected[key] = &expacc{}
			}
			expected[key].sum += v
		}
	}
	cloneBatches = append(cloneBatches, batch.FromRows(schema, rowsC))

	// Tight tracker so primary's second batch trips SpillSome.
	tracker := memory.NewTracker("test", 100_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}

	primary := NewHashAggregate([]string{"year", "region"}, []AggColumn{
		{Func: AggSum, InputCol: "amount", OutputCol: "total", OutputType: parquet.TypeFloat64},
	})
	primary.Spill = sm
	if err := primary.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := primary.Consume(ctx, primaryBatches[0]); err != nil {
		t.Fatal(err)
	}
	// Force pressure — the next Consume must spill.
	tracker.ForceReserve(95_000)
	if err := primary.Consume(ctx, primaryBatches[1]); err != nil {
		t.Fatal(err)
	}
	if len(primary.partialSpillFiles) == 0 {
		t.Fatal("expected partial-state spill file but none was written")
	}

	// Clone runs without spill.
	clone := primary.CloneSink().(*HashAggregate)
	if err := clone.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for _, b := range cloneBatches {
		if err := clone.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}

	// MergeSink used to run the compact→generic migration on both sides —
	// an O(state) materialization at the barrier (the SF100 Q17 killer,
	// morsel-agg-partials-v2.md §3.C). The whole-state spill left the
	// primary EMPTY, so the merge now ADOPTS the clone's SoA state in O(1);
	// the primary must come out non-empty and still SoA.
	primary.MergeSink(clone)
	if primary.intFlatAccs == nil {
		t.Errorf("expected primary SoA state preserved (adopted from clone); intFlatAccs is nil")
	}
	if primary.groupCount() == 0 {
		t.Fatal("expected primary to adopt the clone's groups")
	}

	// Finalize triggers finalizeViaPartialMerge which builds the cursor over
	// the surviving in-memory state plus all runs. Drain Next() to completion.
	if err := primary.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	got := make(map[[2]int32]float64, len(expected))
	for {
		out, err := primary.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for _, r := range out.ToRows() {
			y := r["year"].(int32)
			rg := r["region"].(int32)
			got[[2]int32{y, rg}] = r["total"].(float64)
		}
	}
	if err := primary.Close(); err != nil {
		t.Fatal(err)
	}

	if len(got) != len(expected) {
		t.Fatalf("group count: got %d want %d", len(got), len(expected))
	}
	for k, want := range expected {
		if g, ok := got[k]; !ok {
			t.Errorf("missing group y=%d r=%d", k[0], k[1])
		} else if g != want.sum {
			t.Errorf("y=%d r=%d: got %v want %v", k[0], k[1], g, want.sum)
		}
	}
}

// TestHashAggregateSpillBatching feeds 200 small batches under a tight memory
// budget so every Consume after the first lands in the spill branch.
// Regression for SF100 Q03 where per-batch flushing produced millions of
// ~4 KB spill files, making Finalize unable to complete in reasonable time.
// With the fix, the number of spill files must be bounded by the total rows
// spilled divided by the per-file target, not by the number of Consume calls.
func TestHashAggregateSpillBatching(t *testing.T) {
	schema := []parquet.Column{
		{Name: "key", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeInt64},
	}

	ctx := context.Background()
	spillDir := t.TempDir()
	// 1 KB budget — any TrackBatch on a real batch will push us into the
	// spill branch for all subsequent Consume calls.
	tracker := memory.NewTracker("test", 1_000)
	sm, err := memory.NewSpillManager(spillDir, tracker)
	if err != nil {
		t.Fatal(err)
	}

	h := NewHashAggregate([]string{"key"}, []AggColumn{
		{Func: AggSum, InputCol: "val", OutputCol: "total", OutputType: parquet.TypeInt64},
	})
	h.Spill = sm
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}

	const numBatches = 200
	const rowsPerBatch = 10
	expected := make(map[int64]int64) // key → sum(val)

	for i := 0; i < numBatches; i++ {
		rows := make([]map[string]any, 0, rowsPerBatch)
		for j := 0; j < rowsPerBatch; j++ {
			k := int64(i*rowsPerBatch + j)
			v := int64(i + j + 1)
			rows = append(rows, map[string]any{"key": k, "val": v})
			expected[k] += v
		}
		b := batch.FromRows(schema, rows)
		if err := h.Consume(ctx, b); err != nil {
			t.Fatalf("Consume batch %d: %v", i, err)
		}
	}

	// Sanity: we exercised the spill path. Before the fix this manifests as
	// many spillFiles; after the fix it may manifest as buffered rows that
	// haven't yet been flushed to disk, OR (since simple-aggs external merge)
	// as partial-state files.
	if len(h.spillFiles) == 0 && len(h.spillBuffer) == 0 && len(h.partialSpillFiles) == 0 {
		t.Fatal("spill path was never exercised; tracker/budget setup is wrong for this test")
	}

	// THE REGRESSION: before the fix, len(spillFiles) was ~= numBatches-1.
	// After the fix, files accumulate only when the spill buffer crosses the
	// per-file byte target. 200 batches of 10 small rows is ~60 KB total —
	// well under any reasonable target — so we expect <=5 files.
	if got := len(h.spillFiles); got > 5 {
		t.Errorf("spill-file fragmentation: got %d files for %d batches of %d rows, want <=5",
			got, numBatches, rowsPerBatch)
	}

	// Finalize must drain both h.spillFiles and any pending buffer and produce
	// correct aggregation totals.
	if err := h.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	got := make(map[int64]int64)
	for {
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for _, row := range out.ToRows() {
			k := row["key"].(int64)
			v := row["total"].(int64)
			got[k] = v
		}
	}

	if len(got) != len(expected) {
		t.Fatalf("group count: got %d, want %d", len(got), len(expected))
	}
	for k, want := range expected {
		if got[k] != want {
			t.Errorf("key=%d: got total=%d, want %d", k, got[k], want)
		}
	}
}

// TestHashAggregateSpillBufferFlush exercises the flush path by lowering
// spillFileTargetBytes so the buffer crosses the threshold during Consume,
// producing a bounded (non-zero) number of files.
func TestHashAggregateSpillBufferFlush(t *testing.T) {
	schema := []parquet.Column{
		{Name: "key", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeInt64},
	}

	orig := spillFileTargetBytes
	spillFileTargetBytes = 10_000 // flush aggressively, but accommodate a few batches per file
	t.Cleanup(func() { spillFileTargetBytes = orig })

	ctx := context.Background()
	spillDir := t.TempDir()
	tracker := memory.NewTracker("test", 1_000) // tight budget
	sm, err := memory.NewSpillManager(spillDir, tracker)
	if err != nil {
		t.Fatal(err)
	}

	h := NewHashAggregate([]string{"key"}, []AggColumn{
		{Func: AggSum, InputCol: "val", OutputCol: "total", OutputType: parquet.TypeInt64},
	})
	h.Spill = sm
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}

	const numBatches = 50
	const rowsPerBatch = 50
	expected := make(map[int64]int64)

	for i := 0; i < numBatches; i++ {
		rows := make([]map[string]any, 0, rowsPerBatch)
		for j := 0; j < rowsPerBatch; j++ {
			k := int64(i*rowsPerBatch + j)
			v := int64(i + j + 1)
			rows = append(rows, map[string]any{"key": k, "val": v})
			expected[k] += v
		}
		b := batch.FromRows(schema, rows)
		if err := h.Consume(ctx, b); err != nil {
			t.Fatalf("Consume batch %d: %v", i, err)
		}
	}

	// With a tight budget, the simple-aggs path produces partial-state files
	// (one drain per pressure event) and the legacy path produces buffered
	// raw-row files (one per spillBuffer flush). Either is acceptable for
	// the "multiple files, far fewer than numBatches" intent of this test.
	totalFiles := len(h.spillFiles) + len(h.partialSpillFiles)
	if totalFiles == 0 {
		t.Error("expected spill path to produce at least one file")
	}
	if totalFiles >= numBatches {
		t.Errorf("spill did not batch across Consume calls: got %d files for %d batches", totalFiles, numBatches)
	}

	if err := h.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	got := make(map[int64]int64)
	for {
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for _, row := range out.ToRows() {
			k := row["key"].(int64)
			v := row["total"].(int64)
			got[k] = v
		}
	}

	if len(got) != len(expected) {
		t.Fatalf("group count: got %d, want %d", len(got), len(expected))
	}
	for k, want := range expected {
		if got[k] != want {
			t.Errorf("key=%d: got total=%d, want %d", k, got[k], want)
		}
	}
}
