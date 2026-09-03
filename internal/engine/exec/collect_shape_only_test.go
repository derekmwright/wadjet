package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A RESULT batch may not carry a shape-only column, and this is the fixture
// that attempts it (protocol method 10 — every "cannot happen" gets one).
//
// The claim is the planner's: computeShapeOnlyColumns refuses to run at all
// unless the plan's output comes from a Project or an Aggregate, whose output
// schema is an explicit list in which a column is a VALUE use, so a shape-only
// column can never be in the output. That was enforced by accident before
// #791: the row boundary PANICKED on such a column, so one that did reach the
// client killed the query instead of answering it.
//
// #791 changed the boundary — GetValue now hands back a batch.ShapeOnlyLen —
// and that turns the same mistake from LOUD into an integer where a string
// belongs. A right-or-loud to silently-wrong move is exactly what a census
// forbids, so the claim is now checked where it can be: at the sink, which
// knows it is holding a result.
func TestACollectSinkRefusesAShapeOnlyResultColumn(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "url", Type: parquet.TypeString, Nullable: true},
	}
	mk := func(shapeOnly bool) *batch.RecordBatch {
		b := batch.NewRecordBatch(schema, 2)
		b.Len = 2
		for i := 0; i < 2; i++ {
			b.Columns[0].Nulls.SetValid(i)
			b.Columns[0].Int64Data[i] = int64(i)
			b.Columns[1].Nulls.SetValid(i)
		}
		if shapeOnly {
			b.Columns[1].BytesData.ShapeOnly = true
			b.Columns[1].BytesData.Offsets[1] = 3
			b.Columns[1].BytesData.Offsets[2] = 10
		} else {
			b.Columns[1].BytesData.Set(0, []byte("abc"))
			b.Columns[1].BytesData.Set(1, []byte("defghij"))
		}
		return b
	}

	ctx := context.Background()
	t.Run("a shape-only result column is refused by name", func(t *testing.T) {
		s := &CollectSink{}
		if err := s.Init(ctx); err != nil {
			t.Fatal(err)
		}
		err := s.Consume(ctx, mk(true))
		if err == nil {
			t.Fatal("a shape-only column reached the result and the sink accepted it — the " +
				"client would receive its LENGTHS as if they were its values")
		}
		if !strings.Contains(err.Error(), "shape-only") || !strings.Contains(err.Error(), "url") {
			t.Fatalf("refused, but the message names neither the condition nor the column: %v", err)
		}
	})

	t.Run("the ordinary column is not", func(t *testing.T) {
		s := &CollectSink{}
		if err := s.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if err := s.Consume(ctx, mk(false)); err != nil {
			t.Fatalf("a column holding real bytes must pass: %v", err)
		}
		rows := s.ToRows()
		if len(rows) != 2 || rows[0]["url"] != "abc" || rows[1]["url"] != "defghij" {
			t.Fatalf("the control result is wrong: %v", rows)
		}
	})
}
