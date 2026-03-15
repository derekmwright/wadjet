package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func TestHashJoin_Inner(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	leftRows := []map[string]any{
		{"user_id": "alice", "amount": 100.0},
		{"user_id": "bob", "amount": 200.0},
		{"user_id": "alice", "amount": 150.0},
		{"user_id": "unknown", "amount": 50.0}, // no match in right
	}

	rightSchema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
	}
	rightRows := []map[string]any{
		{"user_id": "alice", "name": "Alice Smith"},
		{"user_id": "bob", "name": "Bob Jones"},
		{"user_id": "carol", "name": "Carol White"}, // no match in left
	}

	hj := NewHashJoin(InnerJoin, []string{"user_id"}, []string{"user_id"})
	hj.BuildFromRows(rightSchema, rightRows)

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Inner join: "unknown" dropped, "carol" dropped, alice appears twice
	if len(sink.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(sink.Rows))
	}

	for _, row := range sink.Rows {
		t.Logf("  %v", row)
		if row["name"] == nil {
			t.Fatalf("expected name to be non-nil for inner join")
		}
	}
}

func TestHashJoin_Left(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	leftRows := []map[string]any{
		{"user_id": "alice", "amount": 100.0},
		{"user_id": "bob", "amount": 200.0},
		{"user_id": "unknown", "amount": 50.0},
	}

	rightSchema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
	}
	rightRows := []map[string]any{
		{"user_id": "alice", "name": "Alice Smith"},
		{"user_id": "bob", "name": "Bob Jones"},
	}

	hj := NewHashJoin(LeftJoin, []string{"user_id"}, []string{"user_id"})
	hj.BuildFromRows(rightSchema, rightRows)

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Left join: all left rows preserved, "unknown" gets null name
	if len(sink.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(sink.Rows))
	}

	for _, row := range sink.Rows {
		t.Logf("  %v", row)
	}

	// Find the "unknown" row
	for _, row := range sink.Rows {
		if row["user_id"] == "unknown" {
			if row["name"] != nil {
				t.Fatalf("expected nil name for 'unknown', got %v", row["name"])
			}
		}
	}
}

func TestRightJoin(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	leftRows := []map[string]any{
		{"user_id": "alice", "amount": 100.0},
		{"user_id": "bob", "amount": 200.0},
	}

	rightSchema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
	}
	rightRows := []map[string]any{
		{"user_id": "alice", "name": "Alice Smith"},
		{"user_id": "bob", "name": "Bob Jones"},
		{"user_id": "carol", "name": "Carol White"}, // no match in left
	}

	hj := NewHashJoin(RightJoin, []string{"user_id"}, []string{"user_id"})
	hj.BuildFromRows(rightSchema, rightRows)

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// FlushUnmatched to get unmatched right rows
	unmatched := probe.FlushUnmatched(leftSchema)
	if unmatched != nil {
		sink.Rows = append(sink.Rows, unmatched.ToRows()...)
	}

	// Right join: all right rows preserved. alice and bob match, carol has null left columns.
	if len(sink.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(sink.Rows))
	}

	for _, row := range sink.Rows {
		t.Logf("  %v", row)
	}

	// Find the "carol" row and verify null left columns
	foundCarol := false
	for _, row := range sink.Rows {
		if row["name"] == "Carol White" {
			foundCarol = true
			if row["amount"] != nil {
				t.Fatalf("expected nil amount for 'carol', got %v", row["amount"])
			}
		}
	}
	if !foundCarol {
		t.Fatal("expected to find carol in right join results")
	}
}

func TestFullOuterJoin(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	leftRows := []map[string]any{
		{"user_id": "alice", "amount": 100.0},
		{"user_id": "unknown", "amount": 50.0}, // no match in right
	}

	rightSchema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString},
	}
	rightRows := []map[string]any{
		{"user_id": "alice", "name": "Alice Smith"},
		{"user_id": "carol", "name": "Carol White"}, // no match in left
	}

	hj := NewHashJoin(FullOuterJoin, []string{"user_id"}, []string{"user_id"})
	hj.BuildFromRows(rightSchema, rightRows)

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// FlushUnmatched to get unmatched right rows
	unmatched := probe.FlushUnmatched(leftSchema)
	if unmatched != nil {
		sink.Rows = append(sink.Rows, unmatched.ToRows()...)
	}

	// Full outer join: alice matches, unknown has null right, carol has null left => 3 rows
	if len(sink.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(sink.Rows))
	}

	for _, row := range sink.Rows {
		t.Logf("  %v", row)
	}

	// Verify "unknown" row has null name (unmatched left)
	foundUnknown := false
	for _, row := range sink.Rows {
		if row["user_id"] == "unknown" {
			foundUnknown = true
			if row["name"] != nil {
				t.Fatalf("expected nil name for 'unknown', got %v", row["name"])
			}
		}
	}
	if !foundUnknown {
		t.Fatal("expected to find 'unknown' in full outer join results")
	}

	// Verify "carol" row has null amount (unmatched right)
	foundCarol := false
	for _, row := range sink.Rows {
		if row["name"] == "Carol White" {
			foundCarol = true
			if row["amount"] != nil {
				t.Fatalf("expected nil amount for 'carol', got %v", row["amount"])
			}
		}
	}
	if !foundCarol {
		t.Fatal("expected to find carol in full outer join results")
	}
}

func TestCrossJoin(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "color", Type: parquet.TypeString},
	}
	leftRows := []map[string]any{
		{"color": "red"},
		{"color": "blue"},
		{"color": "green"},
	}

	rightSchema := []parquet.Column{
		{Name: "size", Type: parquet.TypeString},
	}
	rightRows := []map[string]any{
		{"size": "small"},
		{"size": "large"},
	}

	// CrossJoin doesn't use keys for matching, but we still pass empty key slices
	hj := NewHashJoin(CrossJoin, nil, nil)
	hj.BuildFromRows(rightSchema, rightRows)

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Cross join: 3 left * 2 right = 6 rows (Cartesian product)
	if len(sink.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(sink.Rows))
	}

	for _, row := range sink.Rows {
		t.Logf("  %v", row)
		if row["color"] == nil {
			t.Fatal("expected non-nil color in cross join")
		}
		if row["size"] == nil {
			t.Fatal("expected non-nil size in cross join")
		}
	}
}

func TestHashJoin_MultiKey(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "year", Type: parquet.TypeString},
		{Name: "month", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeFloat64},
	}
	leftRows := []map[string]any{
		{"year": "2026", "month": "03", "val": 1.0},
		{"year": "2026", "month": "04", "val": 2.0},
	}

	rightSchema := []parquet.Column{
		{Name: "y", Type: parquet.TypeString},
		{Name: "m", Type: parquet.TypeString},
		{Name: "label", Type: parquet.TypeString},
	}
	rightRows := []map[string]any{
		{"y": "2026", "m": "03", "label": "march"},
	}

	hj := NewHashJoin(InnerJoin, []string{"year", "month"}, []string{"y", "m"})
	hj.BuildFromRows(rightSchema, rightRows)

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(sink.Rows))
	}
	if sink.Rows[0]["label"] != "march" {
		t.Fatalf("expected label=march, got %v", sink.Rows[0]["label"])
	}
}
