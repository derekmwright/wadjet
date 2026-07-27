package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// #277 regression: a rows-but-no-columns batch reaching a grouped or
// column-consuming aggregate previously panicked (index out of range in
// consumeBatch's updater selection, Q18 fused-chain breaker at SF100,
// masked by task retry). It must now fail with a structured error.
// COUNT(*)-only ungrouped aggregates legitimately consume schemaless
// batches and must keep working.
func TestHashAggregateZeroColumnBatch(t *testing.T) {
	grouped := &HashAggregate{
		GroupByCols: []string{"k"},
		Aggs:        []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "s"}},
	}
	if err := grouped.Init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}
	err := grouped.Consume(context.Background(), &batch.RecordBatch{Len: 5})
	if err == nil || !strings.Contains(err.Error(), "zero columns") {
		t.Fatalf("grouped aggregate must reject schemaless batch with a structured error, got %v", err)
	}

	countStar := &HashAggregate{
		Aggs: []AggColumn{{Func: AggCount, OutputCol: "c"}},
	}
	if err := countStar.Init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := countStar.Consume(context.Background(), &batch.RecordBatch{Len: 5}); err != nil {
		t.Fatalf("COUNT(*) over a schemaless batch must keep working: %v", err)
	}
}

// The SF100 stacks showed the actual triggering shape: a completely EMPTY
// batch (0 rows, 0 columns) — the flushSpilledOps drain path feeds Consume
// without an ActiveLen gate. Empties are no-ops: no error, no panic, and
// no effect on state.
func TestHashAggregateEmptyBatchNoOp(t *testing.T) {
	agg := &HashAggregate{
		GroupByCols: []string{"k"},
		Aggs:        []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "s"}},
	}
	if err := agg.Init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := agg.Consume(context.Background(), &batch.RecordBatch{}); err != nil {
		t.Fatalf("empty batch must be a no-op, got %v", err)
	}
}
