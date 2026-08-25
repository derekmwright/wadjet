package exec

import (
	"context"
	"os"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #496 under GRACE PARTITIONING. The flat build path is one of five that skip
// a NULL-keyed row's index insert, and the arena append it used to skip with
// it lives in a different function for the partition-on-arrival and spilled-
// replay paths (join_partition_arrival.go, join_spill.go). A fixture small
// enough to stay in memory exercises none of them.
//
// Every NULL key hashes to partition 0 by construction, so a spilled build
// puts them all in one partition and the replay has to bring them back — and
// a RIGHT join owes every one of them a NULL-padded output row.
func TestRightJoinKeepsNullKeyedBuildRowsThroughSpill(t *testing.T) {
	const buildN = 24000
	const nullEvery = 7

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "pad", Type: parquet.TypeString},
	}

	var wantNullKeyed int
	buildRows := make([]map[string]any, 0, buildN)
	for i := 0; i < buildN; i++ {
		row := map[string]any{"id": int64(i), "pad": "xxxxxxxxxxxxxxxx"}
		if i%nullEvery == 0 {
			row["k"] = nil
			wantNullKeyed++
		} else {
			row["k"] = int64(i)
		}
		buildRows = append(buildRows, row)
	}
	// Probe every SECOND non-null build key, so the answer separates three
	// groups: matched pairs, unmatched non-null build rows, and the
	// NULL-keyed build rows this test exists for.
	var wantMatched int
	probeRows := make([]map[string]any, 0, buildN/2)
	for i := 0; i < buildN; i += 2 {
		if i%nullEvery == 0 {
			continue // a NULL build key matches nothing, including a probe NULL
		}
		probeRows = append(probeRows, map[string]any{"pid": int64(i), "pk": int64(i)})
		wantMatched++
	}
	// The probe's columns are named apart from the build's on purpose: with
	// both sides carrying "k" and no alias to disambiguate by, the join drops
	// the build's copy from the output and every assertion below would be
	// reading the PROBE's NULL padding instead of the build's key.
	probeSchema := []parquet.Column{
		{Name: "pid", Type: parquet.TypeInt64},
		{Name: "pk", Type: parquet.TypeInt64, Nullable: true},
	}

	tmpDir, err := os.MkdirTemp("", "null-key-spill")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	tracker := memory.NewTracker("test", 64<<20)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(RightJoin, []string{"pk"}, []string{"k"})
	hj.Spill = sm
	hj.MemTracker = tracker
	if err := hj.Build(context.Background(), NewSliceSource(schema, buildRows)); err != nil {
		t.Fatalf("build: %v", err)
	}
	// The budget is deliberately generous: what this test needs is
	// PARTITION-ON-ARRIVAL (computeBuildPartitionRows routes every row, and
	// indexBuildBatch indexes it), not eviction. Evicting a partition nils
	// its buildBatches slots and FlushUnmatched then dereferences one — a
	// pre-existing crash for any spilled RIGHT/FULL join, with or without a
	// NULL key, pinned separately in the test below (#550).
	if hj.spillState == nil {
		t.Fatalf("the build was not partitioned on arrival — this test is not exercising the "+
			"path it exists for (spillState=%v)", hj.spillState)
	}
	if n := len(hj.spillState.spilledParts); n != 0 {
		t.Fatalf("%d partitions were EVICTED; raise the budget — this test pins the "+
			"partitioned-but-resident path", n)
	}
	t.Logf("partitioned on arrival, %d build rows, %d of them NULL-keyed",
		hj.BuildRows(), wantNullKeyed)

	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(probeSchema, probeRows),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("pipeline: %v", err)
	}

	// A RIGHT join emits one row per build row: matched pairs plus every
	// unmatched build row NULL-padded — and a NULL-keyed build row is
	// unmatched by definition.
	if got, want := len(sink.Rows), buildN; got != want {
		t.Fatalf("RIGHT join over a spilled build emitted %d rows, want %d "+
			"(%d matched + %d unmatched, of which %d are NULL-keyed)",
			got, want, wantMatched, buildN-wantMatched, wantNullKeyed)
	}
	var gotNullKeyed, gotUnpadded int
	for _, r := range sink.Rows {
		if r["k"] == nil {
			gotNullKeyed++
		}
		if r["pad"] == nil {
			gotUnpadded++
		}
	}
	if gotNullKeyed != wantNullKeyed {
		t.Errorf("%d output rows carry a NULL key, want %d — the NULL-keyed build rows are the "+
			"ones a RIGHT join must not lose", gotNullKeyed, wantNullKeyed)
	}
	if gotUnpadded != 0 {
		t.Errorf("%d output rows have no build payload; every row a RIGHT join emits comes FROM "+
			"the build side", gotUnpadded)
	}
}

// The semi/anti twins over the same shape. Their build stores no rows at all
// (SemiAntiKeyOnly), so a NULL-keyed build row must reach the index NOWHERE
// and the arena NOWHERE — but must still be counted, because #507's poison
// reads exactly that.
func TestSemiAntiOverNullKeyedBuild(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
	}
	buildRows := []map[string]any{
		{"id": int64(1), "k": int64(1)},
		{"id": int64(2), "k": nil},
		{"id": int64(3), "k": int64(3)},
	}
	probeRows := []map[string]any{
		{"id": int64(1), "k": int64(1)}, // matches
		{"id": int64(2), "k": int64(9)}, // no match
		{"id": int64(3), "k": nil},      // NULL: matches nothing, including the build's NULL
	}

	for _, tc := range []struct {
		name     string
		joinType JoinType
		keyOnly  bool
		want     int
	}{
		// A NULL key equals nothing, so the build's NULL row matches no
		// probe row and the probe's NULL row matches no build row.
		{"semi", SemiJoin, true, 1},
		{"semi_stored", SemiJoin, false, 1},
		// Plain (NOT EXISTS) anti: two probe rows have no match — the 9 and
		// the NULL. This is NOT the NOT IN rule; NullAwareAnti is off.
		{"anti", AntiJoin, true, 2},
		{"anti_stored", AntiJoin, false, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hj := NewHashJoin(tc.joinType, []string{"k"}, []string{"k"})
			hj.SemiAntiKeyOnly = tc.keyOnly
			if err := hj.Build(context.Background(), NewSliceSource(schema, buildRows)); err != nil {
				t.Fatalf("build: %v", err)
			}
			if hj.BuildRows() != int64(len(buildRows)) {
				t.Errorf("BuildRows() = %d, want %d — a NULL-keyed row is a real build row",
					hj.BuildRows(), len(buildRows))
			}
			sink := &CollectSink{}
			pipe := &Pipeline{
				Source: NewSliceSource(schema, probeRows),
				Ops:    []UnaryOperator{hj.Probe()},
				Sink:   sink,
			}
			if err := pipe.Run(context.Background()); err != nil {
				t.Fatalf("pipeline: %v", err)
			}
			if got := len(sink.Rows); got != tc.want {
				t.Errorf("%s emitted %d rows, want %d", tc.name, got, tc.want)
			}
		})
	}
}

// A pin, not a gate: a RIGHT or FULL OUTER join whose build EVICTS a partition
// to disk panics with a nil dereference in FlushUnmatched, which walks the
// arena and reads h.buildBatches[ref.batchIdx] — nil'd by
// spillOneInMemoryPartition. That function's correctness argument is written
// entirely about the PROBE path ("unreachable on the in-memory probe path");
// the flush walks the arena directly and was never in scope.
//
// It has nothing to do with NULL keys — it reproduces with none — so it is
// pre-existing and tracked in #550. This pin RUNS, so the day the flush learns
// to restore an evicted partition it starts ANSWERING and this test fails,
// which is the signal to delete it (ADR-0013 §Pins).
func TestSpilledRightJoinFlushPanicsPinned550(t *testing.T) {
	const buildN = 24000
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}
	buildRows := make([]map[string]any, 0, buildN)
	for i := 0; i < buildN; i++ {
		buildRows = append(buildRows, map[string]any{
			"id": int64(i), "k": int64(i), "pad": "xxxxxxxxxxxxxxxx",
		})
	}
	probeSchema := []parquet.Column{{Name: "pk", Type: parquet.TypeInt64}}
	probeRows := []map[string]any{{"pk": int64(2)}}

	tmpDir, err := os.MkdirTemp("", "spill-flush-pin")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	tracker := memory.NewTracker("test", 1_500_000)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(RightJoin, []string{"pk"}, []string{"k"})
	hj.Spill = sm
	hj.MemTracker = tracker
	if err := hj.Build(context.Background(), NewSliceSource(schema, buildRows)); err != nil {
		t.Skipf("build did not reach the eviction path: %v", err)
	}
	if hj.spillState == nil || len(hj.spillState.spilledParts) == 0 {
		t.Skip("no partition was evicted — the pinned condition did not arise on this machine")
	}

	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(probeSchema, probeRows),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	err = pipe.Run(context.Background())
	if err == nil {
		t.Fatalf("PIN #550 now ANSWERS (%d rows): a RIGHT join over an EVICTED build no longer "+
			"panics in FlushUnmatched. Delete this pin — it is the proof the fix landed.",
			len(sink.Rows))
	}
	t.Logf("PINNED #550: RIGHT join over an evicted build fails instead of answering: %v", err)
}
