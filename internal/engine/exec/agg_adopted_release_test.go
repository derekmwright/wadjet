// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestSerialEmitReleasesEachClosedAdoptedPartition runs a real partitioned
// aggregation (eight workers, a budget) with parallel emission off, so the
// serial fallback streams each adopted partition and closes it in turn. Every
// closed partition must be released from the primary's backing array as it
// is closed: a front reslice alone keeps the array — and every closed
// partition in it — reachable until the primary's own Close (#1706, the
// registry-retention sweep).
func TestSerialEmitReleasesEachClosedAdoptedPartition(t *testing.T) {
	prev := parallelEmitToggle.Set(false)
	t.Cleanup(func() { parallelEmitToggle.Set(prev) })
	ctx := context.Background()
	const workers = 8
	const groups = 4000
	sm, err := memory.NewSpillManager(t.TempDir(), memory.NewTracker("query", 64<<20))
	if err != nil {
		t.Fatal(err)
	}
	agg := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggCount, OutputCol: "n", OutputType: parquet.TypeInt64},
	})
	agg.InputRowHint = shareEstimate
	agg.Spill = sm
	schema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64, Nullable: true}}
	var batches []*batch.RecordBatch
	for off := 0; off < groups; off += 1000 {
		rows := make([]map[string]any, 0, 1000)
		for g := off; g < off+1000; g++ {
			rows = append(rows, map[string]any{"k": int64(g)})
		}
		batches = append(batches, batch.FromRows(schema, rows))
	}
	pipe := &Pipeline{Source: &benchBatchSource{batches: batches}, Sink: agg, Workers: workers}
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer agg.Close()
	if !agg.PartitionedDisjoint || len(agg.adoptedPartitions) != workers-1 {
		t.Fatalf("the pipeline did not run partitioned (disjoint=%v adopted=%d, want %d)",
			agg.PartitionedDisjoint, len(agg.adoptedPartitions), workers-1)
	}
	// orig shares the primary's backing array, so it observes what that array
	// still holds after each front reslice.
	orig := agg.adoptedPartitions
	n := 0
	for {
		out, err := agg.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		n += out.ActiveLen()
	}
	if n != groups {
		t.Fatalf("%d groups, want %d", n, groups)
	}
	if agg.emit != nil {
		t.Fatal("emission went parallel; the serial fallback was not exercised")
	}
	for i, ap := range orig {
		if ap != nil {
			t.Fatalf("adopted partition %d was closed by the serial emit but its slot still holds it", i)
		}
	}
}
