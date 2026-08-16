package exec

import (
	"context"
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestHashAggregateMergeModeLossInProcess is a CONTROL test for the
// distributed final_aggregate row-loss bug (TestQ17MinRepro): runs the
// exact same merge shape — SUM(c) GROUP BY l_partkey over (l_partkey, c)
// pairs simulating partial-aggregate output — and confirms the in-process
// HashAggregate preserves the total of c.
//
// 2000 distinct partkeys × 8 reps each = 16000 input rows. Total SUM(c) is
// preserved through the GROUP BY exactly. Passes consistently.
//
// The distributed final_aggregate stage takes the same shape of input
// (partial output rows shuffled by l_partkey) and loses ~5% of count.
// Since this in-process test passes, the bug is NOT in the kernel —
// it is somewhere on the distributed input path: .wshf reader,
// coordinator-side fan-out, or the merge-mode rewrite that swaps
// fn=COUNT→SUM and InputCol from "*" → OutputCol.
func TestHashAggregateMergeModeLossInProcess(t *testing.T) {
	schema := []parquet.Column{
		{Name: "l_partkey", Type: parquet.TypeInt32},
		{Name: "c", Type: parquet.TypeInt64},
	}

	// Build input rows: 2000 distinct partkeys, each appearing 8 times
	// (mirrors 8 partial-aggregate outputs each with the partkey). Total
	// rows = 16,000. Total SUM(c) = sum of all c values.
	rng := rand.New(rand.NewSource(42))
	const numKeys = 2000
	const repsPerKey = 8
	totalRows := numKeys * repsPerKey
	rows := make([]map[string]any, 0, totalRows)
	totalSum := int64(0)
	for k := 0; k < numKeys; k++ {
		for r := 0; r < repsPerKey; r++ {
			c := int64(rng.Intn(10) + 1)
			totalSum += c
			rows = append(rows, map[string]any{
				"l_partkey": int32(k),
				"c":         c,
			})
		}
	}
	// Shuffle so groups intermix across batches.
	rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })

	// Feed via SliceSource (which splits into 2048-row batches) and
	// HashAggregate (Pipeline.runParallel uses N>1 workers when source
	// emits multiple batches).
	source := NewSliceSource(schema, rows)
	hashAgg := NewHashAggregate([]string{"l_partkey"}, []AggColumn{
		{Func: AggSum, InputCol: "c", OutputCol: "c", OutputType: parquet.TypeInt64},
	})
	// Default Workers (=0 → serial), mirroring distributed final_aggregate
	// path which doesn't set Pipeline.Workers.
	pipe := &Pipeline{Source: source, Sink: hashAgg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Drain output and compute SUM(c) over output rows.
	var outSum int64
	var outGroups int
	for {
		b, err := hashAgg.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		cIdx := -1
		for i, col := range b.Schema {
			if col.Name == "c" {
				cIdx = i
				break
			}
		}
		if cIdx < 0 {
			t.Fatal("output schema missing c column")
		}
		col := b.Columns[cIdx]
		for i := 0; i < b.Len; i++ {
			outSum += col.Int64Data[i]
		}
		outGroups += b.Len
	}
	if outGroups != numKeys {
		t.Errorf("output groups: got %d, want %d", outGroups, numKeys)
	}
	if outSum != totalSum {
		t.Errorf("output SUM(c): got %d, want %d (lost %d)", outSum, totalSum, totalSum-outSum)
	}
	t.Logf("input rows=%d totalSum=%d, output groups=%d outSum=%d", totalRows, totalSum, outGroups, outSum)
}

// Workers set on Pipeline to force multi-worker runParallel even on small
// inputs. Without this, Pipeline.Run picks a worker count based on batch
// availability that may serialise small tests.
var _ = batch.RecordBatch{}
