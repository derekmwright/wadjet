package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestEveryWindowedTypeSurvivesAForcedRun is the window family's spill gate,
// and it is here rather than in the type-matrix sweep for the reason the sort's
// is (sort_forced_run_test.go): the sweep's 36 window cells spill only when
// tracker.Used() crosses 40% of the budget at the instant the window checks,
// and what puts it there is a transient in the SCAN's read-ahead, not the
// window. The family engaged 22 and 23 of 36 on a 24-core box and 0 of 36 on
// one core with one scan worker — the same random variable, and the same way
// for CI to go red on a thinner runner with nothing changed.
//
// So the trigger is exec.ForceWindowSpillEvery, and the coverage is per TYPE
// as a PARTITION BY key: each one goes through the run writer, the run reader
// and the external merge on every run and every core count, and the row
// multiset must come back whole.
//
// Revert the knob and this fails: nothing writes a run, and the engagement
// assert fires.
func TestEveryWindowedTypeSurvivesAForcedRun(t *testing.T) {
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
			// One PARTITION BY value across every row: the partition key's job
			// here is the RUN FORMAT — its bytes must survive the write/read
			// pair — while ROW_NUMBER over id gives an answer that a lost or
			// duplicated row changes.
			sample := s.vector(t)
			mk := func(base int) *batch.RecordBatch {
				b := batch.NewRecordBatch(schema, rows)
				for r := 0; r < rows; r++ {
					b.Columns[0].SetValue(r, sample.GetValue(0))
					b.Columns[1].Int64Data[r] = int64(base + r)
				}
				return b
			}

			w := NewWindow([]WindowColumn{{
				Func: WinRowNumber, OutputCol: "rn", OutputType: parquet.TypeInt64,
				PartitionBy: []string{"k"},
				OrderBy:     []SortKey{{Column: "id", Order: Ascending}},
			}})
			w.Spill = sm
			if err := w.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			runsBefore, forcedBefore := WindowRunsWritten.Load(), ForcedWindowSpills.Load()
			restore := ForceWindowSpillEvery(1)
			for i := 0; i < batches; i++ {
				if err := w.Consume(context.Background(), mk(i*rows)); err != nil {
					ForceWindowSpillEvery(restore)
					t.Fatal(err)
				}
			}
			if err := w.Finalize(context.Background()); err != nil {
				ForceWindowSpillEvery(restore)
				t.Fatal(err)
			}
			ForceWindowSpillEvery(restore)

			seen := make(map[int64]int64, batches*rows)
			total := 0
			for {
				b, err := w.Next(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if b == nil {
					break
				}
				idv := b.ColumnByName("id")
				rnv := b.ColumnByName("rn")
				if idv == nil || rnv == nil {
					t.Fatalf("output is missing id or rn")
				}
				for i := 0; i < b.ActiveLen(); i++ {
					row := i
					if b.Sel != nil {
						row = int(b.Sel[i])
					}
					seen[idv.Int64Data[row]] = rnv.Int64Data[row]
					total++
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			if ForcedWindowSpills.Load() == forcedBefore {
				t.Fatalf("the knob was armed and no run was forced — this cell ran in memory")
			}
			if WindowRunsWritten.Load() == runsBefore {
				t.Fatalf("no window run reached disk")
			}
			if total != batches*rows || len(seen) != batches*rows {
				t.Fatalf("%d rows came back through the merge (%d distinct ids), want %d — rows were "+
					"lost or duplicated between the run writer and the merge",
					total, len(seen), batches*rows)
			}
			// ROW_NUMBER over one partition ordered by id is id+1, so a run
			// that came back in the wrong order fails here rather than passing
			// on a row count.
			for id, rn := range seen {
				if rn != id+1 {
					t.Fatalf("id %d has ROW_NUMBER %d, want %d — the merge did not restore the "+
						"partition's order", id, rn, id+1)
				}
			}
		})
	}
}
