package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestProfiledSource(t *testing.T) {
	schema := []Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(3)},
	}
	src := NewSliceSource(schema, rows)
	collector := NewProfileCollector()
	profiled := WrapSource(src, collector, "TestSource")

	ctx := context.Background()
	if err := profiled.Init(ctx); err != nil {
		t.Fatal(err)
	}

	totalRows := int64(0)
	calls := 0
	for {
		b, err := profiled.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		totalRows += int64(b.ActiveLen())
		calls++
	}

	stats := collector.Stats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 stat entry, got %d", len(stats))
	}
	s := stats[0]
	if s.Name != "TestSource" {
		t.Errorf("expected name TestSource, got %s", s.Name)
	}
	if s.RowsOut != totalRows {
		t.Errorf("expected %d rows out, got %d", totalRows, s.RowsOut)
	}
	if s.WallTime <= 0 {
		t.Error("expected non-zero wall time")
	}
	// Calls includes the final nil-returning call
	if s.Calls != int64(calls+1) {
		t.Errorf("expected %d calls, got %d", calls+1, s.Calls)
	}

	if err := profiled.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProfiledOperator(t *testing.T) {
	schema := []Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	collector := NewProfileCollector()
	inner := &passthroughOp{}
	profiled := WrapOperator(inner, collector, "TestOp")

	ctx := context.Background()
	if err := profiled.Init(ctx); err != nil {
		t.Fatal(err)
	}

	b := batch.FromRows(schema, []map[string]any{
		{"id": int64(1)},
		{"id": int64(2)},
	})

	out, err := profiled.Execute(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("expected non-nil output")
	}

	stats := collector.Stats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 stat entry, got %d", len(stats))
	}
	s := stats[0]
	if s.RowsIn != int64(b.ActiveLen()) {
		t.Errorf("expected %d rows in, got %d", b.ActiveLen(), s.RowsIn)
	}
	if s.RowsOut != int64(out.ActiveLen()) {
		t.Errorf("expected %d rows out, got %d", out.ActiveLen(), s.RowsOut)
	}
	if s.Calls != 1 {
		t.Errorf("expected 1 call, got %d", s.Calls)
	}

	if err := profiled.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProfiledSink(t *testing.T) {
	schema := []Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	collector := NewProfileCollector()
	inner := &CollectSink{}
	profiled := WrapSink(inner, collector, "TestSink")

	ctx := context.Background()
	if err := profiled.Init(ctx); err != nil {
		t.Fatal(err)
	}

	b := batch.FromRows(schema, []map[string]any{
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(3)},
	})

	if err := profiled.Consume(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := profiled.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	stats := collector.Stats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 stat entry, got %d", len(stats))
	}
	s := stats[0]
	if s.RowsIn != 3 {
		t.Errorf("expected 3 rows in, got %d", s.RowsIn)
	}
	if s.Calls != 1 {
		t.Errorf("expected 1 call, got %d", s.Calls)
	}
	if s.WallTime <= 0 {
		t.Error("expected non-zero wall time")
	}

	if err := profiled.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWrapPipeline(t *testing.T) {
	schema := []Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"id": int64(1)},
		{"id": int64(2)},
	}

	pipeline := &Pipeline{
		Source: NewSliceSource(schema, rows),
		Ops:    []UnaryOperator{&passthroughOp{}},
		Sink:   &CollectSink{},
	}

	collector := WrapPipeline(pipeline)

	// Verify all operators are now wrapped
	if _, ok := pipeline.Source.(*ProfiledSource); !ok {
		t.Error("source should be wrapped")
	}
	if _, ok := pipeline.Ops[0].(*ProfiledOperator); !ok {
		t.Error("operator should be wrapped")
	}
	if _, ok := pipeline.Sink.(*ProfiledSink); !ok {
		t.Error("sink should be wrapped")
	}

	// Execute the pipeline
	ctx := context.Background()
	if err := pipeline.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer pipeline.Close()

	// Verify stats were collected
	stats := collector.Stats()
	if len(stats) != 3 { // source + 1 op + sink
		t.Fatalf("expected 3 stat entries, got %d", len(stats))
	}

	// Source should have produced rows
	if stats[0].RowsOut == 0 {
		t.Error("source should have produced rows")
	}
	// Operator should have processed rows
	if stats[1].RowsIn == 0 {
		t.Error("operator should have received rows")
	}
	// Sink should have consumed rows
	if stats[2].RowsIn == 0 {
		t.Error("sink should have consumed rows")
	}
}

func TestFormatAnalyzeStats(t *testing.T) {
	stats := []*ProfileStats{
		{Name: "ScanSource", RowsIn: 0, RowsOut: 1000, WallTime: 5_500_000, Calls: 2},  // 5.5ms
		{Name: "KernelFilter", RowsIn: 1000, RowsOut: 100, WallTime: 800_000, Calls: 2}, // 800µs
		{Name: "CollectSink", RowsIn: 100, RowsOut: 0, WallTime: 200_000_000, Calls: 2},  // 200ms
	}

	lines := FormatAnalyzeStats(stats)
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}

	// Verify format contains expected fields
	if !strings.Contains(lines[0], "ScanSource") {
		t.Errorf("line 0 should contain ScanSource: %s", lines[0])
	}
	if !strings.Contains(lines[0], "rows_out=1000") {
		t.Errorf("line 0 should contain rows_out=1000: %s", lines[0])
	}
	if !strings.Contains(lines[1], "KernelFilter") {
		t.Errorf("line 1 should contain KernelFilter: %s", lines[1])
	}
	if !strings.Contains(lines[1], "rows_in=1000") {
		t.Errorf("line 1 should contain rows_in=1000: %s", lines[1])
	}
	if !strings.Contains(lines[2], "CollectSink") {
		t.Errorf("line 2 should contain CollectSink: %s", lines[2])
	}
}

// passthroughOp is a minimal UnaryOperator that passes batches through unchanged.
type passthroughOp struct{}

func (p *passthroughOp) Init(_ context.Context) error { return nil }
func (p *passthroughOp) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	return in, nil
}
func (p *passthroughOp) Close() error { return nil }
