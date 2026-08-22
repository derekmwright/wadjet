package worker

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// The coordinator's exact per-partition row accounting decides the group-index
// layout of a DAG aggregate (exec/two_level_hash.go, twoLevelAmortizeMultiple).
// It only does so if it survives the wire→operator conversion, and only if it
// is applied BEFORE Init — the layout is taken once, on the first batch.
func TestBuildFragmentHashAggregateCarriesRowBound(t *testing.T) {
	e := &Executor{}
	spec := distributed.OpSpec{
		MergeMode:   true,
		GroupByCols: []string{"l_orderkey"},
		Aggregates: []distributed.AggSpec{
			{Func: "sum", InputCol: "l_quantity", OutputCol: "sq"},
		},
		InputRowBound: 6_250_000,
	}
	agg, err := e.buildFragmentHashAggregate(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if got := agg.InputRowBound(); got != 6_250_000 {
		t.Fatalf("aggregate input row bound = %d, want 6250000", got)
	}

	// No bound on the wire (older coordinator, non-partitioned input) leaves
	// the aggregate on the adaptive path rather than declaring a zero bound.
	spec.InputRowBound = 0
	agg, err = e.buildFragmentHashAggregate(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if got := agg.InputRowBound(); got != 0 {
		t.Fatalf("aggregate input row bound = %d with none declared, want 0", got)
	}
}
