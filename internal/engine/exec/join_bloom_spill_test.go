package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A bloom filter's one forbidden error is a FALSE NEGATIVE, and a grace build
// that spills commits it — because the bloom is built from what the index holds
// at the end of the build, and a spilling build's index does not hold every key
// the build saw.
//
// Two things take keys out of it. A row whose partition has ALREADY spilled is
// written straight to disk and never indexed at all (partitionAndIndexBatch's
// first branch). And, since #823's reclaim, an evicted partition's table is
// freed with its columns. Both leave the bloom short.
//
// That is only a wrong ANSWER through BloomPushdownOp, which runs UPSTREAM of
// the partition router: a probe row it rejects never reaches the join, so it is
// not even written to its partition's probe file, and the spilled-partition
// replay never sees it. Measured at the operator pair on this fixture before
// the fix, with 60 partitions evicted: the filter rejected 25,262 of 40,000
// probe rows whose key IS on the build side.
//
// The bloom itself stays valid for the IN-MEMORY probe path, which only ever
// asks about rows the router has already kept in memory — their key set is
// exactly what the index holds. So the fix is not to stop building the bloom,
// it is to stop PUBLISHING it upstream of the router.
func TestASpilledBuildDoesNotPublishItsBloom(t *testing.T) {
	schema, data := arrivalBuildRows(40000, "padpadpadpadpadpadpadpadpadpadpad")

	for _, tc := range []struct {
		name        string
		budget      int64
		wantSpilled bool
	}{
		{"spilled", 1 << 20, true},
		{"resident", 1 << 30, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := memory.NewTracker("bloom", tc.budget)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			defer sm.Cleanup()
			hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"k"})
			hj.Spill, hj.MemTracker = sm, tracker
			evictedBefore := JoinPartitionsEvicted.Load()
			if err := hj.Build(context.Background(), arrivalSource(schema, data, 256)); err != nil {
				t.Fatalf("build: %v", err)
			}
			evicted := JoinPartitionsEvicted.Load() - evictedBefore
			spilled := hj.spillState != nil && len(hj.spillState.spilledParts) > 0
			if spilled != tc.wantSpilled {
				t.Fatalf("this cell needs spilled=%v; it got %v (%d partitions evicted) — "+
					"the fixture stopped reaching its own condition",
					tc.wantSpilled, spilled, evicted)
			}

			bf := hj.BloomPushdownOp()
			if tc.wantSpilled {
				if bf != nil {
					t.Fatal("a build that spilled published a bloom pushdown filter; it " +
						"runs upstream of the partition router, so every probe row it " +
						"rejects for a key the build gave to disk is a row silently lost")
				}
				if hj.bloom == nil {
					t.Error("the bloom itself is gone; the in-memory probe path still " +
						"wants it, and it is valid there — only the pushdown is not")
				}
				return
			}
			if bf == nil {
				t.Fatal("a build that never spilled published no bloom; this cell would " +
					"prove nothing about the filter's coverage")
			}

			// The claim from the other side: a published filter must not reject
			// a single key its own build side holds.
			if err := bf.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			kept, total := 0, 0
			for start := 0; start < len(data); start += 2048 {
				end := min(start+2048, len(data))
				rows := make([]map[string]any, 0, end-start)
				for i := start; i < end; i++ {
					rows = append(rows, map[string]any{"k": int64(i)})
				}
				pb := batch.FromRows([]parquet.Column{{Name: "k", Type: parquet.TypeInt64}}, rows)
				total += pb.ActiveLen()
				out, err := bf.Execute(context.Background(), pb)
				if err != nil {
					t.Fatal(err)
				}
				if out != nil {
					kept += out.ActiveLen()
				}
			}
			if kept != total {
				t.Errorf("the published bloom rejected %d of %d probe rows whose key IS "+
					"on the build side — a false negative, the one error a bloom may "+
					"not make", total-kept, total)
			}
		})
	}
}
