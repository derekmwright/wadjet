package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestFixKeyAssignment_PartitionedBuildSafe: FixKeyAssignment's rebuild
// path must not touch buildBatches after a partition-on-arrival build —
// evicted partitions nil their entries (join_partition_arrival.go), the
// same hazard PruneBuildColumns hit on SF10 standalone Q21. The trigger
// needs BOTH a spilled build and a misassigned key pair: the SQL put the
// build column on the left of "=", so RightKeys carries a probe-qualified
// name that only resolves in the build schema via columnIndexFallback.
// Pre-guard, the swap set needsRebuild and the rebuild walked the nil'd
// entries. The guard keeps the swap (probe resolution needs the corrected
// names) and skips the rebuild — the arrival-time index is already keyed
// on the correct physical column, so it must survive untouched.
func TestFixKeyAssignment_PartitionedBuildSafe(t *testing.T) {
	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	const buildN = 10000
	rightRows := make([]map[string]any, buildN)
	for i := range rightRows {
		rightRows[i] = map[string]any{
			"id":  int64(i),
			"val": "build-row-padding-for-size",
		}
	}

	tmpDir := t.TempDir()
	// Budget tight enough that partitions must evict during build —
	// eviction is what nils buildBatches entries (matches
	// TestPruneBuildColumns_PartitionedBuildSafe's working point).
	tracker := memory.NewTracker("test", 400_000)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	// Misassigned pair: "id" (build column) landed in LeftKeys, and
	// RightKeys got the probe-qualified "src.id" — which
	// columnIndexFallback resolves to the build's "id" at arrival time,
	// so the build succeeds and indexes the correct column. Semi join
	// with a join filter keeps SemiAntiKeyOnly false, so batches are
	// stored and the build dispatches to buildPartitioned.
	hj := NewHashJoin(SemiJoin, []string{"id"}, []string{"src.id"})
	hj.Spill = sm
	hj.MemTracker = tracker

	if err := hj.Build(context.Background(), NewSliceSource(rightSchema, rightRows)); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if hj.spillState == nil {
		t.Fatal("test setup: build did not take the partition-on-arrival path")
	}
	nilEntries := 0
	for _, b := range hj.buildBatches {
		if b == nil {
			nilEntries++
		}
	}
	if nilEntries == 0 {
		t.Fatal("test setup: no partition was evicted — budget too generous to exercise the crash")
	}
	rowsBefore := hj.buildRows
	keyIdxBefore := append([]int(nil), hj.buildKeyIdx...)

	// The planner's post-build hook. Pre-guard this swapped the keys,
	// set needsRebuild, and dereferenced the nil'd entries.
	hj.FixKeyAssignment()

	// The swap itself must stand — probe-side resolution depends on it.
	if hj.LeftKeys[0] != "src.id" || hj.RightKeys[0] != "id" {
		t.Fatalf("keys not swapped: left=%v right=%v", hj.LeftKeys, hj.RightKeys)
	}
	// The partitioned index must be untouched: same rows, same build key
	// column — a rebuild from the nil-holed buildBatches would have
	// dropped every evicted partition's rows even if it didn't crash.
	if hj.buildRows != rowsBefore {
		t.Fatalf("buildRows changed %d → %d; partitioned index was rebuilt", rowsBefore, hj.buildRows)
	}
	for i, idx := range hj.buildKeyIdx {
		if idx != keyIdxBefore[i] {
			t.Fatalf("buildKeyIdx[%d] changed %d → %d", i, keyIdxBefore[i], idx)
		}
	}
}

// #572. A key-only build stores no rows, so FixKeyAssignment's rebuild —
// which resets buildRows and buildHasNullKey and then recomputes them by
// walking h.buildBatches — runs zero iterations and leaves both at their zero
// values, while replacing the populated key index with an empty one.
//
// buildHasNullKey is what makes NOT IN three-valued (#507): a NULL anywhere in
// the build empties the answer. Losing it flips `x NOT IN (…)` from no rows to
// EVERY row, and losing the index flips `x IN (…)` from its matches to none.
// Both silently.
//
// The repair is forced the way a real plan forces it: the pair is misassigned,
// with the build's column on the LEFT and a probe-qualified name on the right
// that only columnIndexFallback resolves in the build schema. That is the same
// premise TestFixKeyAssignment_PartitionedBuildSafe above uses, and the same
// reason the guard may keep the arrival-time index: the two spellings name the
// same physical column.
func TestFixKeyAssignmentKeepsAKeyOnlyBuildsRowsAndNullFlag(t *testing.T) {
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
		{"id": int64(1), "k": int64(1)}, // matches the build's 1
		{"id": int64(2), "k": int64(9)}, // matches nothing
		{"id": int64(3), "k": nil},      // NULL: matches nothing
	}

	for _, tc := range []struct {
		name     string
		joinType JoinType
		// nullAware selects NOT IN's rule over NOT EXISTS's.
		nullAware bool
		want      int
	}{
		// NOT IN over a build holding a NULL: UNKNOWN for every probe row
		// that did not match, so nothing survives.
		{"null_aware_anti", AntiJoin, true, 0},
		// The two-valued control. NOT EXISTS is a different predicate and
		// must not move: the 9 and the NULL both match nothing.
		{"plain_anti", AntiJoin, false, 2},
		// IN: only the row that equals a build key.
		{"semi", SemiJoin, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// LeftKeys[0]="k" is present in the build schema and
			// RightKeys[0]="s.k" is not, which is exactly the condition
			// FixKeyAssignment swaps on.
			hj := NewHashJoin(tc.joinType, []string{"k"}, []string{"s.k"})
			hj.SemiAntiKeyOnly = true
			hj.NullAwareAnti = tc.nullAware
			if err := hj.Build(context.Background(), NewSliceSource(schema, buildRows)); err != nil {
				t.Fatalf("build: %v", err)
			}
			if len(hj.buildBatches) != 0 {
				t.Fatalf("a key-only build stored %d batches; this test's premise is that it stores "+
					"none", len(hj.buildBatches))
			}
			wantRows, wantNull := hj.BuildRows(), hj.buildHasNullKey
			if wantRows != int64(len(buildRows)) || !wantNull {
				t.Fatalf("before the repair: BuildRows()=%d hasNullKey=%v, want %d/true",
					wantRows, wantNull, len(buildRows))
			}

			if !hj.FixKeyAssignment() {
				t.Fatal("FixKeyAssignment did not fire; this test needs the repair path")
			}
			if got := hj.BuildRows(); got != wantRows {
				t.Errorf("BuildRows() = %d after the key repair, want %d — the repair rebuilt from "+
					"h.buildBatches, which a key-only build never fills", got, wantRows)
			}
			if !hj.buildHasNullKey {
				t.Error("buildHasNullKey was cleared by the key repair — that flag IS NOT IN's " +
					"three-valued rule (#507), and it cannot be recomputed from a build that " +
					"stores no rows")
			}

			// The index has to survive too: an emptied one answers IN with
			// nothing and NOT IN with everything, which is the same wrong
			// answer by a different route.
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
				t.Errorf("%s emitted %d rows after the key repair, want %d", tc.name, got, tc.want)
			}
		})
	}
}
