package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestEverySortedTypeSurvivesAForcedRun is the sort family's spill gate, and it
// lives here rather than in the type-matrix sweep because here it is
// DETERMINISTIC.
//
// The sweep's 18 ORDER BY cells spill only when tracker.Used() crosses 40% of
// the budget at the instant the sort checks, and in those cells the sort is not
// what puts it there: instrumenting the check over the sweep gave 293 checks
// per run with a median used of 165,152 bytes against a 209,715-byte threshold,
// and only 8 to 13 of those 293 above it — the crossings are transients in the
// SCAN's read-ahead (ADR-0013 class 4). Across six runs of unchanged code the
// family engaged 7 to 14 of 18 cells, and a 2-vCPU CI runner engaged 0 of 18
// and turned the sweep red. A gate whose trigger is that condition cannot be
// relied on to fire, which is the shape CLAUDE.md already names.
//
// So the trigger moves to exec.ForceSortSpillEvery — the sort's analogue of
// ForceAggDrainEvery (ADR-0027 decision 6) — and the coverage gets STRONGER
// rather than weaker: every flat type goes through the run writer, the run
// reader and the k-way merge on every run and every core count, where the sweep
// asserted only that SOME cell of the family had spilled.
//
// Revert the knob and this fails: nothing writes a run, so the merge never runs
// and the engagement assert fires.
func TestEverySortedTypeSurvivesAForcedRun(t *testing.T) {
	defer ForceSmallSpillRuns(4096)()

	const batches, rows = 5, 300
	for _, s := range gkpSamples() {
		t.Run(s.typ.String(), func(t *testing.T) {
			tr := memory.NewTracker("query", 512*1024)
			sm, err := memory.NewSpillManager(t.TempDir(), tr)
			if err != nil {
				t.Fatal(err)
			}
			schema := []parquet.Column{
				s.column(),
				{Name: "id", Type: parquet.TypeInt64},
			}
			// The typed column holds one value in every row, so the ORDER BY
			// is decided by id and the expected output is a total order the
			// merge cannot satisfy by luck. What the typed column is here for
			// is the RUN FORMAT: its bytes must survive the write/read pair.
			sample := s.vector(t)
			mk := func(base int) *batch.RecordBatch {
				b := batch.NewRecordBatch(schema, rows)
				for r := 0; r < rows; r++ {
					b.Columns[0].SetValue(r, sample.GetValue(0))
					b.Columns[1].Int64Data[r] = int64(base + r)
				}
				return b
			}

			s := NewSort([]SortKey{
				{Column: "k", Order: Ascending},
				{Column: "id", Order: Ascending},
			})
			s.Spill = sm
			if err := s.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			runsBefore, forcedBefore := SortRunsWritten.Load(), ForcedSortSpills.Load()
			restore := ForceSortSpillEvery(1)
			for i := 0; i < batches; i++ {
				if err := s.Consume(context.Background(), mk(i*rows)); err != nil {
					ForceSortSpillEvery(restore)
					t.Fatal(err)
				}
			}
			if err := s.Finalize(context.Background()); err != nil {
				ForceSortSpillEvery(restore)
				t.Fatal(err)
			}
			ForceSortSpillEvery(restore)

			var ids []int64
			for {
				b, err := s.Next(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if b == nil {
					break
				}
				for i := 0; i < b.ActiveLen(); i++ {
					row := i
					if b.Sel != nil {
						row = int(b.Sel[i])
					}
					ids = append(ids, b.Columns[1].Int64Data[row])
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			// ENGAGEMENT first: a gate that asserts an answer without proving
			// the spill path ran passes with that path deleted.
			if ForcedSortSpills.Load() == forcedBefore {
				t.Fatalf("the knob was armed and no run was forced — this cell sorted in memory")
			}
			if SortRunsWritten.Load() == runsBefore {
				t.Fatalf("no sorted run reached disk")
			}
			if len(ids) != batches*rows {
				t.Fatalf("%d rows came back through the merge, want %d — rows were lost between "+
					"the run writer and the merge", len(ids), batches*rows)
			}
			for i, got := range ids {
				if got != int64(i) {
					t.Fatalf("row %d is id %d, want %d — the merge did not restore the total order",
						i, got, i)
				}
			}
		})
	}
}
