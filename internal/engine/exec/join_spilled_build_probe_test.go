package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A probe against a build that SPILLED resolves its key columns from the ref's
// own batch, never from `buildBatches[0]` (#863).
//
// `lookupBuild`'s dual-int-key arm primed `bcol0`/`bcol1` from
// `h.buildBatches[0]` before walking the chain, so that the loop could skip
// re-resolving them for a run of refs in batch 0. A grace eviction nils
// `buildBatches[i]` for every batch of the partition it wrote to disk, and
// slot 0 belongs to whichever partition froze first — so for ANY build large
// enough to spill, the priming dereferenced nil, whatever partition the probe
// row was in and whether or not any ref pointed at batch 0.
//
// The arm is reached by `genericProbe`, which is where outer joins and
// residual shapes go; the inline int kernels do not call `lookupBuild`, which
// is why an INNER join over the same data answers and a LEFT join panics.
func TestAProbeAgainstASpilledBuildDoesNotReadBatchZero(t *testing.T) {
	const buildN, probeN = 5000, 600
	// Fresh batches per cell: a Source hands each batch out once and the build
	// takes ownership of what it froze, so a second cell over the same slice
	// joins against a consumed one.
	newBuild := func() []*batch.RecordBatch {
		buildSchema := []parquet.Column{
			{Name: "k1", Type: parquet.TypeInt64},
			{Name: "k2", Type: parquet.TypeInt64},
			{Name: "pay", Type: parquet.TypeString},
		}
		var out []*batch.RecordBatch
		for off := 0; off < buildN; off += 1000 {
			b := batch.NewRecordBatch(buildSchema, 1000)
			for i := 0; i < 1000; i++ {
				id := int64(off + i)
				b.Columns[0].Int64Data[i] = id
				b.Columns[1].Int64Data[i] = id * 3
				// Wide enough that 5,000 rows do not fit a 250 KB budget.
				b.Columns[2].SetValue(i, f863Pay(id))
			}
			b.Len = 1000
			out = append(out, b)
		}
		return out
	}
	newProbe := func() *batch.RecordBatch {
		pb := batch.NewRecordBatch([]parquet.Column{
			{Name: "p1", Type: parquet.TypeInt64},
			{Name: "p2", Type: parquet.TypeInt64},
		}, probeN)
		for i := 0; i < probeN; i++ {
			// Rows 0..499 match a build row; 500..599 match nothing, so the
			// outer joins' padding path runs beside the matching one.
			pb.Columns[0].Int64Data[i] = int64(i)
			k2 := int64(i) * 3
			if i >= 500 {
				k2++
			}
			pb.Columns[1].Int64Data[i] = k2
		}
		pb.Len = probeN
		return pb
	}

	// All three take the dual-int-key arm and all three spill; only the OUTER
	// ones reach `lookupBuild` (the inline int kernels do not call it), so the
	// INNER cell is the CONTROL — same data, same build, the path the defect
	// never touched — and it fails if this fix moved a right answer.
	for _, jt := range []struct {
		name string
		typ  JoinType
	}{{"left", LeftJoin}, {"full", FullOuterJoin}, {"inner", InnerJoin}} {
		t.Run(jt.name, func(t *testing.T) {
			tracker := memory.NewTracker("test", 250_000)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			defer sm.Cleanup()

			hj := NewHashJoin(jt.typ, []string{"p1", "p2"}, []string{"k1", "k2"})
			hj.Spill = sm
			hj.MemTracker = tracker
			if err := hj.Build(context.Background(), NewBatchSource(newBuild())); err != nil {
				t.Fatalf("build: %v", err)
			}
			// The defect's three preconditions, asserted rather than assumed:
			// the dual-int-key arm is the one taken, the build spilled, and
			// slot 0 is one of the batches the eviction nil'd.
			if !hj.useDualIntKey {
				t.Fatalf("the join declined the dual-int-key arm, which is the one this " +
					"gate is about")
			}
			if hj.spillState == nil || len(hj.spillState.spilledParts) == 0 {
				t.Fatalf("the build did not spill, so this cell cannot reach the defect")
			}
			if len(hj.buildBatches) == 0 || hj.buildBatches[0] != nil {
				t.Fatalf("batch 0 is resident, so the nil this cell is about does not exist "+
					"(spilled %d partitions of %d batches)",
					len(hj.spillState.spilledParts), len(hj.buildBatches))
			}

			sink := &CollectSink{}
			pipe := &Pipeline{
				Source: NewBatchSource([]*batch.RecordBatch{newProbe()}),
				Ops:    []UnaryOperator{hj.Probe()},
				Sink:   sink,
			}
			if err := pipe.Run(context.Background()); err != nil {
				t.Fatalf("probing a spilled build: %v", err)
			}

			// The VALUES, not just the absence of a panic: every matched probe
			// row carries ITS OWN build row's payload — unique per key, so a
			// mis-resolved build batch is a wrong value and not only a crash —
			// every unmatched probe row is padded, and a FULL join also emits
			// each unmatched build row exactly once.
			wantProbe, wantBuild := probeN, 0
			switch jt.typ {
			case InnerJoin:
				wantProbe = 500
			case FullOuterJoin:
				wantBuild = buildN - 500
			}
			seenProbe, seenBuild := map[int64]bool{}, map[int64]bool{}
			for _, row := range sink.Rows {
				p1, ok := row["p1"].(int64)
				if !ok {
					// A FULL join's unmatched build row: no probe side at all.
					k1, isBuild := row["k1"].(int64)
					if !isBuild {
						t.Fatalf("row with neither a probe nor a build key: %v", row)
					}
					if seenBuild[k1] {
						t.Fatalf("build key %d emitted twice", k1)
					}
					seenBuild[k1] = true
					if k1 < 500 {
						t.Fatalf("build key %d matched a probe row and is emitted unmatched too", k1)
					}
					if row["pay"] != f863Pay(k1) {
						t.Fatalf("unmatched build key %d carries pay=%v", k1, row["pay"])
					}
					continue
				}
				if seenProbe[p1] {
					t.Fatalf("probe key %d emitted twice", p1)
				}
				seenProbe[p1] = true
				want := any(nil)
				if p1 < 500 {
					want = f863Pay(p1)
				}
				if row["pay"] != want {
					t.Fatalf("probe key %d carries pay=%v, want %v", p1, row["pay"], want)
				}
			}
			if len(seenProbe) != wantProbe || len(seenBuild) != wantBuild {
				t.Fatalf("got %d probe-side and %d unmatched-build rows, want %d and %d "+
					"(%d rows in all)", len(seenProbe), len(seenBuild), wantProbe, wantBuild,
					len(sink.Rows))
			}
		})
	}
}

// f863Pay is the build payload for key id — wide enough that 5,000 rows do not
// fit the cell's budget, and unique per row so a mis-resolved build batch shows
// up as a wrong VALUE and not only as a panic.
func f863Pay(id int64) string {
	return fmt.Sprintf("pay-%d-%s", id, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
}
