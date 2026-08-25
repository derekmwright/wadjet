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

// #550's gate, which replaces the pin that stood here.
//
// A RIGHT or FULL OUTER join whose build EVICTS a partition used to panic with
// a nil dereference: spillOneInMemoryPartition writes the partition's batches
// to disk and nils their h.buildBatches slots, and FlushUnmatched walks the
// ARENA — every entry, including the ones pointing at the freed slots.
// spillOneInMemoryPartition's correctness argument is written entirely about
// the in-memory PROBE path, which the flush is not.
//
// The fix is not "skip the nil": that would DROP those build rows, which is a
// wrong answer where the panic was at least loud. The evicted partition is
// replayed from disk by NextFlush, and the temp join built over it emits the
// partition's own unmatched build rows — so the resident flush skips exactly
// the entries the replay owns, and every build row is emitted exactly once.
//
// The assertion is per-ROW, not a count: a right count with a duplicated row
// standing in for a dropped one is the shape ADR-0013 §Pins warns about.
func TestRightJoinOverAnEvictedBuildEmitsEveryBuildRowExactlyOnce(t *testing.T) {
	const buildN = 24000
	for _, tc := range []struct {
		name      string
		joinType  JoinType
		nullEvery int // 0 = no NULL keys
	}{
		{"right", RightJoin, 0},
		{"right_with_null_keys", RightJoin, 7},
		{"full_outer", FullOuterJoin, 0},
		{"full_outer_with_null_keys", FullOuterJoin, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := []parquet.Column{
				{Name: "id", Type: parquet.TypeInt64},
				{Name: "k", Type: parquet.TypeInt64, Nullable: true},
				{Name: "pad", Type: parquet.TypeString},
			}
			buildRows := make([]map[string]any, 0, buildN)
			nullKeyed := map[int64]bool{}
			for i := 0; i < buildN; i++ {
				row := map[string]any{"id": int64(i), "pad": "xxxxxxxxxxxxxxxx"}
				if tc.nullEvery > 0 && i%tc.nullEvery == 0 {
					row["k"] = nil
					nullKeyed[int64(i)] = true
				} else {
					row["k"] = int64(i)
				}
				buildRows = append(buildRows, row)
			}
			// Probe keys spread across the hash partitions so both resident
			// and evicted partitions carry matched rows.
			probeSchema := []parquet.Column{{Name: "pk", Type: parquet.TypeInt64}}
			var probeRows []map[string]any
			matched := map[int64]bool{}
			for i := 0; i < buildN; i += 977 {
				if nullKeyed[int64(i)] {
					continue // a NULL build key matches nothing
				}
				probeRows = append(probeRows, map[string]any{"pk": int64(i)})
				matched[int64(i)] = true
			}

			tmpDir := t.TempDir()
			tracker := memory.NewTracker("test", 1_500_000)
			sm, err := memory.NewSpillManager(tmpDir, tracker)
			if err != nil {
				t.Fatal(err)
			}
			defer sm.Cleanup()

			evictedBefore := JoinPartitionsEvicted.Load()
			hj := NewHashJoin(tc.joinType, []string{"pk"}, []string{"k"})
			hj.Spill = sm
			hj.MemTracker = tracker
			if err := hj.Build(context.Background(), NewSliceSource(schema, buildRows)); err != nil {
				t.Fatalf("build: %v", err)
			}
			// The whole point of the fixture. A build that stayed resident
			// exercises none of this, and a gate that quietly skips there is
			// how #550 survived: its pin did exactly that.
			if n := len(hj.spillState.spilledParts); n == 0 {
				t.Fatalf("no partition was EVICTED at a 1.5 MB budget over %d build rows — "+
					"this gate is not exercising the path it exists for", buildN)
			}
			if got := JoinPartitionsEvicted.Load() - evictedBefore; got == 0 {
				t.Fatal("JoinPartitionsEvicted did not move; the eviction counter is not wired")
			}

			sink := &CollectSink{}
			probe := hj.Probe()
			pipe := &Pipeline{
				Source: NewSliceSource(probeSchema, probeRows),
				Ops:    []UnaryOperator{probe},
				Sink:   sink,
			}
			if err := pipe.Run(context.Background()); err != nil {
				t.Fatalf("pipeline: %v", err)
			}

			// One output row per build row: the matched pairs plus every
			// unmatched build row NULL-padded. Counted per id so a duplicate
			// cannot cover for a loss.
			seen := make(map[int64]int, buildN)
			var padCount int
			for _, r := range sink.Rows {
				id, ok := r["id"].(int64)
				if !ok {
					t.Fatalf("output row has no build id: %v", r)
				}
				seen[id]++
				if r["pad"] == nil {
					padCount++
				}
				// The probe half is NULL exactly for the build rows nothing
				// matched, and carries the key for the ones that did.
				wantMatched := matched[id]
				gotMatched := r["pk"] != nil
				if wantMatched != gotMatched {
					t.Errorf("build id=%d: probe side matched=%v, want %v (row %v)",
						id, gotMatched, wantMatched, r)
				}
				if k := r["k"]; nullKeyed[id] != (k == nil) {
					t.Errorf("build id=%d: key NULL=%v, want %v", id, k == nil, nullKeyed[id])
				}
			}
			if padCount != 0 {
				t.Errorf("%d output rows carry no build payload; every row this join emits comes "+
					"FROM the build side", padCount)
			}
			if len(seen) != buildN {
				t.Errorf("%d distinct build rows reached the output, want %d", len(seen), buildN)
			}
			for id, n := range seen {
				if n != 1 {
					t.Fatalf("build id=%d emitted %d times, want exactly 1 — an evicted partition "+
						"is flushed by the disk replay AND must be skipped by the resident flush", id, n)
				}
			}
			if len(sink.Rows) != buildN {
				t.Errorf("emitted %d rows, want %d", len(sink.Rows), buildN)
			}
		})
	}
}
