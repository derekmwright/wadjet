package coordinator

import (
	"runtime"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// makeAggPartial builds one partial batch: one (g, v) row per group, where
// g is the group key and v is that group's value for this partial.
func makeAggPartial(t *testing.T, groups int, valueFor func(g int) float64) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt64, Nullable: true},
		{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
	}
	b := batch.NewRecordBatch(schema, groups)
	for g := 0; g < groups; g++ {
		b.Columns[0].Int64Data[g] = int64(g)
		b.Columns[0].Nulls.SetValid(g)
		b.Columns[1].Float64Data[g] = valueFor(g)
		b.Columns[1].Nulls.SetValid(g)
	}
	b.Len = groups
	return b
}

// TestReAggregatePartials_ParallelByteIdentical is the #22 gate: the parallel
// key-sharded merge must produce output BIT-IDENTICAL to the serial path,
// including float64 SUM values (accumulation order) and group output order.
//
// The parallel path only engages at >= parallelMergeMinRows partial rows with
// GOMAXPROCS > 1 — a scale neither the SF0.01 TPC-H test nor the small-scale
// harness reaches — so this is the only thing that exercises it locally. The
// per-group values below are order-sensitive (a large term, then small terms,
// then its negation): if the parallel merge summed a group's partials in any
// order other than the global one, the float result would diverge in the low
// bits and this test would fail.
func TestReAggregatePartials_ParallelByteIdentical(t *testing.T) {
	const groups = 3000 // 4 partials * 3000 = 12000 rows >= parallelMergeMinRows
	values := [][]float64{
		func() []float64 {
			v := make([]float64, groups)
			for g := range v {
				v[g] = 1e15
			}
			return v
		}(),
		func() []float64 {
			v := make([]float64, groups)
			for g := range v {
				v[g] = float64(g) + 0.5
			}
			return v
		}(),
		func() []float64 {
			v := make([]float64, groups)
			for g := range v {
				v[g] = -1e15
			}
			return v
		}(),
		func() []float64 {
			v := make([]float64, groups)
			for g := range v {
				v[g] = float64(g) + 0.25
			}
			return v
		}(),
	}
	makeBatches := func() []*batch.RecordBatch {
		bs := make([]*batch.RecordBatch, len(values))
		for p := range values {
			vp := values[p]
			bs[p] = makeAggPartial(t, groups, func(g int) float64 { return vp[g] })
		}
		return bs
	}

	columns := []string{"g", "v"}
	colIdx := map[string]int{"g": 0, "v": 1}
	mi := &logical.MergeInfo{
		GroupBy:      []string{"g"},
		AggExprs:     []logical.AggExpr{{Func: "sum", InputCol: "v", OutputCol: "v"}},
		HasAggregate: true,
	}

	c := &Coordinator{}

	// Serial reference: force a single shard.
	prev := runtime.GOMAXPROCS(1)
	if got := mergeShardCount(groups * len(values)); got != 1 {
		runtime.GOMAXPROCS(prev)
		t.Fatalf("expected serial merge at GOMAXPROCS=1, got shards=%d", got)
	}
	serial, err := c.reAggregatePartials(makeBatches(), columns, colIdx, mi)
	if err != nil {
		runtime.GOMAXPROCS(prev)
		t.Fatalf("serial merge: %v", err)
	}

	// Parallel: ensure > 1 shard actually engages.
	runtime.GOMAXPROCS(maxInt(2, runtime.NumCPU()))
	shards := mergeShardCount(groups * len(values))
	parallel, err := c.reAggregatePartials(makeBatches(), columns, colIdx, mi)
	if err != nil {
		runtime.GOMAXPROCS(prev)
		t.Fatalf("parallel merge: %v", err)
	}
	runtime.GOMAXPROCS(prev)

	if shards <= 1 {
		t.Fatalf("parallel path did not engage (shards=%d); test would not cover it", shards)
	}

	// Both must be a single result batch of `groups` rows.
	if len(serial) != 1 || len(parallel) != 1 {
		t.Fatalf("expected 1 result batch each, got serial=%d parallel=%d", len(serial), len(parallel))
	}
	sb, pb := serial[0], parallel[0]
	if sb.Len != groups || pb.Len != groups {
		t.Fatalf("row count mismatch: serial=%d parallel=%d want=%d", sb.Len, pb.Len, groups)
	}

	// Bit-identical: same group order (g) and same SUM (v), exact float equality.
	for ri := 0; ri < groups; ri++ {
		sg := sb.Columns[0].Int64Data[ri]
		pg := pb.Columns[0].Int64Data[ri]
		if sg != pg {
			t.Fatalf("row %d: group order differs serial g=%d parallel g=%d", ri, sg, pg)
		}
		sv := sb.Columns[1].Float64Data[ri]
		pv := pb.Columns[1].Float64Data[ri]
		if sv != pv {
			t.Fatalf("row %d (g=%d): SUM differs serial=%v parallel=%v (accumulation order changed)", ri, sg, sv, pv)
		}
	}

	// Sanity: serial result for a sample group equals the global-order sum.
	want := ((1e15 + (0.0 + 0.5)) + -1e15) + (0.0 + 0.25) // group g=0
	if sb.Columns[0].Int64Data[0] != 0 {
		t.Fatalf("expected first group g=0, got %d", sb.Columns[0].Int64Data[0])
	}
	if got := sb.Columns[1].Float64Data[0]; got != want {
		t.Fatalf("group 0 sum = %v, want %v (global accumulation order)", got, want)
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
