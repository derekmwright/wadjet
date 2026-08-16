package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Regression for the embedded-pipeline clone memory hole (ClickBench c6a
// recon, Q19 OOM): Pipeline.runParallel cloned HashAggregate sinks with NO
// SpillManager — their group state was invisible to the memory ledger and
// unbounded, so a high-cardinality GROUP BY multiplied serial state by k
// clones until the kernel killed the process. Clones must charge a
// tracking-only view and self-drain past PartialDrainBytes, exactly like
// the distributed worker path.
func TestParallelCloneAggSpillWiring(t *testing.T) {
	ctx := context.Background()

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
	}
	// Sizing: total group state ~12MB across 8 workers (~1.5MB/clone).
	// Budget 8MB: the PRIMARY alone (1.5MB) never crosses it, so the only
	// way partial-state runs appear is the CLONE drain bound
	// budget/(2k)=512KB. Without the wiring the test observes zero runs.
	const n = 120000 // high-cardinality: every row its own group
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"k": int64(i), "s": "some-group-key-payload-with-a-bit-more-width-to-it"}
	}

	tracker := memory.NewTracker("test", 8<<20)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}

	agg := NewHashAggregate([]string{"k", "s"}, []AggColumn{
		{Func: AggCount, OutputCol: "c", OutputType: parquet.TypeInt64},
	})
	agg.Spill = sm

	// The wiring contract itself: a cloned aggregate must get a
	// tracking-only spill view and a budget/(2k) drain bound.
	clone := agg.CloneSink().(*HashAggregate)
	wireCloneSinkSpill(clone, agg, 8)
	if clone.Spill == nil {
		t.Fatal("clone has no spill view — its group state is invisible to the memory ledger")
	}
	if want := int64(8<<20) / 16; clone.PartialDrainBytes != want {
		t.Fatalf("clone PartialDrainBytes = %d, want %d (budget/(2k))", clone.PartialDrainBytes, want)
	}

	// End-to-end: the run must stay correct with the 512KB drain bound
	// engaging in every clone (each holds ~1.5MB of state).
	pipe := &Pipeline{
		Source:  NewSliceSource(schema, rows),
		Sink:    agg,
		Workers: 8,
	}
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}

	total := int64(0)
	groups := 0
	for {
		b, err := agg.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		for _, r := range b.ToRows() {
			groups++
			if c, ok := r["c"].(int64); ok {
				total += c
			}
		}
	}
	if groups != n || total != n {
		t.Fatalf("got %d groups / %d total count, want %d / %d", groups, total, n, n)
	}
}
