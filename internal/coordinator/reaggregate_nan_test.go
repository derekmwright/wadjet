package coordinator

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Regression coverage for #457's coordinator-side arm: the legacy
// probe-split partial merge (mergeProbePartials → reAggregatePartials for a
// GROUP BY query, mergeScalarAggregates → compareAnyValues for a scalar
// one) combines each worker's ALREADY-COMPUTED partial MIN/MAX across
// probe-split files. Each partial row here is what one worker reported for
// its own file split — itself a valid MIN/MAX pair (if a worker's split saw
// a NaN, PostgreSQL's rule makes its own MAX NaN too) — and this pins that
// the coordinator's merge of those partials picks the same answer
// PostgreSQL would, regardless of which order the partial rows arrive in.

var nanFloatSchema = []parquet.Column{
	{Name: "k", Type: parquet.TypeInt64},
	{Name: "lo", Type: parquet.TypeFloat64},
	{Name: "hi", Type: parquet.TypeFloat64},
}

func nanMergeMI() *logical.MergeInfo {
	return &logical.MergeInfo{
		GroupBy: []string{"k"},
		AggExprs: []logical.AggExpr{
			{Func: "MIN", OutputCol: "lo"},
			{Func: "MAX", OutputCol: "hi"},
		},
	}
}

// TestReAggregatePartialsMinMaxFloatNaN drives c.reAggregatePartials (the
// GROUP BY probe-split merge) with three partial rows for one group — one
// partial saw only NaN (lo=hi=NaN), one saw a finite range, one saw a NaN
// mixed with finite values (lo finite, hi=NaN) — in every arrival order.
// The merged answer must be the same regardless of order: MIN is the
// smallest finite value across every partial (NaN never wins MIN while a
// finite value exists anywhere), MAX is NaN (any partial reporting a NaN
// maximum wins MAX).
func TestReAggregatePartialsMinMaxFloatNaN(t *testing.T) {
	nan := math.NaN()
	partials := []rowVal{
		{kInt64: 1, loFloat64: nan, hiFloat64: nan},  // saw only NaN
		{kInt64: 1, loFloat64: -5.0, hiFloat64: 5.0}, // finite range
		{kInt64: 1, loFloat64: -8.0, hiFloat64: nan}, // finite min, NaN max
	}
	orders := map[string][]int{
		"nan_only_first":  {0, 1, 2},
		"nan_only_last":   {1, 2, 0},
		"nan_only_middle": {1, 0, 2},
		"reverse":         {2, 1, 0},
	}

	c := &Coordinator{}
	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			var batches []*batch.RecordBatch
			for _, i := range order {
				batches = append(batches, newRecordBatchFromRows(t, nanFloatSchema, []rowVal{partials[i]}))
			}
			columns := []string{"k", "lo", "hi"}
			colIdx := map[string]int{"k": 0, "lo": 1, "hi": 2}
			out, err := c.reAggregatePartials(batches, columns, colIdx, nanMergeMI())
			if err != nil {
				t.Fatalf("reAggregatePartials: %v", err)
			}
			if len(out) != 1 || out[0].Len != 1 {
				t.Fatalf("expected 1 batch with 1 row, got %d batches", len(out))
			}
			res := out[0]
			lo := res.Columns[colIdx["lo"]].Float64Data[0]
			hi := res.Columns[colIdx["hi"]].Float64Data[0]
			if lo != -8.0 {
				t.Errorf("MIN = %v, want -8 (the smallest finite value across every partial — a NaN "+
					"partial's minimum must never win while a finite one exists)", lo)
			}
			if !math.IsNaN(hi) {
				t.Errorf("MAX = %v, want NaN (a partial reporting a NaN maximum must win, in every "+
					"merge order)", hi)
			}
		})
	}
}

// TestReAggregatePartialsAllNaNFloat: every partial for a group saw only
// NaN — the merged MIN and MAX are both NaN (PostgreSQL, verified live).
func TestReAggregatePartialsAllNaNFloat(t *testing.T) {
	nan := math.NaN()
	c := &Coordinator{}
	batches := []*batch.RecordBatch{
		newRecordBatchFromRows(t, nanFloatSchema, []rowVal{{kInt64: 1, loFloat64: nan, hiFloat64: nan}}),
		newRecordBatchFromRows(t, nanFloatSchema, []rowVal{{kInt64: 1, loFloat64: nan, hiFloat64: nan}}),
	}
	columns := []string{"k", "lo", "hi"}
	colIdx := map[string]int{"k": 0, "lo": 1, "hi": 2}
	out, err := c.reAggregatePartials(batches, columns, colIdx, nanMergeMI())
	if err != nil {
		t.Fatalf("reAggregatePartials: %v", err)
	}
	if len(out) != 1 || out[0].Len != 1 {
		t.Fatalf("expected 1 batch with 1 row, got %d batches", len(out))
	}
	res := out[0]
	lo := res.Columns[colIdx["lo"]].Float64Data[0]
	hi := res.Columns[colIdx["hi"]].Float64Data[0]
	if !math.IsNaN(lo) {
		t.Errorf("MIN = %v, want NaN", lo)
	}
	if !math.IsNaN(hi) {
		t.Errorf("MAX = %v, want NaN", hi)
	}
}

// TestMergeScalarAggregatesMinMaxFloatNaN extends merge_scalar_agg_test.go's
// TestMergeScalarAggregatesMinMax with #457's shape: the scalar (no GROUP
// BY) probe-split merge (mergeScalarAggregates → compareAnyValues) must
// agree with PostgreSQL's NaN ordering regardless of which worker's partial
// arrives first.
func TestMergeScalarAggregatesMinMaxFloatNaN(t *testing.T) {
	nan := math.NaN()
	schema := []parquet.Column{
		{Name: "lo", Type: parquet.TypeFloat64},
		{Name: "hi", Type: parquet.TypeFloat64},
	}
	mkBatch := func(lo, hi float64) *batch.RecordBatch {
		return batch.FromRows(schema, []map[string]any{{"lo": lo, "hi": hi}})
	}
	mi := &logical.MergeInfo{
		HasAggregate: true,
		AggExprs: []logical.AggExpr{
			{Func: "min", OutputCol: "lo"},
			{Func: "max", OutputCol: "hi"},
		},
	}
	partials := []*batch.RecordBatch{
		mkBatch(nan, nan),  // worker A saw only NaN
		mkBatch(-5.0, 5.0), // worker B: finite range
		mkBatch(-8.0, nan), // worker C: finite min, NaN max
	}
	orders := map[string][]int{
		"nan_only_first": {0, 1, 2},
		"nan_only_last":  {1, 2, 0},
		"reverse":        {2, 1, 0},
	}
	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			var batches []*batch.RecordBatch
			for _, i := range order {
				batches = append(batches, partials[i])
			}
			result := mergeScalarAggHelper(t, batches, []string{"lo", "hi"}, mi)
			if len(result) != 1 {
				t.Fatalf("expected 1 batch, got %d", len(result))
			}
			rows := result[0].ToRows()
			if len(rows) != 1 {
				t.Fatalf("expected 1 merged row, got %d", len(rows))
			}
			lo, ok := rows[0]["lo"].(float64)
			if !ok || lo != -8.0 {
				t.Errorf("MIN = %#v, want -8", rows[0]["lo"])
			}
			hi, ok := rows[0]["hi"].(float64)
			if !ok || !math.IsNaN(hi) {
				t.Errorf("MAX = %#v, want NaN", rows[0]["hi"])
			}
		})
	}
}
