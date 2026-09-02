package exec

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A morsel-parallel clone owns FOUR spill artifacts, and every one of them has
// to reach the primary at the merge (#790, and the raw-row half of it).
//
//	drainedRuns        partial-state runs from the PartialDrainBytes bound
//	partialSpillFiles  partial-state runs from SpillSome / the pressure branch
//	spillFiles         LEGACY raw-row files
//	spillBuffer        the in-memory half of the raw-row path
//
// The clone's Close removes the files and drops the buffer, so an artifact the
// merge does not take is not merely un-merged — it is deleted, and every row in
// it leaves the answer with no error anywhere.
//
// These gates drive the merge directly rather than through a pipeline, because
// WHICH artifact a clone fills is decided by how it was asked to spill, and one
// pipeline shape reaches one of them.

// cmaSchema is one nullable int key plus a value. A NULL key migrates the
// int-keyed path to the generic map, which is how the non-SoA merge branches
// are reached.
func cmaSchema() []parquet.Column {
	return []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64},
	}
}

// cmaBatch builds keys [lo,hi), with the row whose key would be nullAt carrying
// a NULL key instead. nullAt = -1 for no NULL.
func cmaBatch(lo, hi, nullAt int64) *batch.RecordBatch {
	b := batch.NewRecordBatch(cmaSchema(), int(hi-lo))
	for i := lo; i < hi; i++ {
		j := int(i - lo)
		if i == nullAt {
			b.Columns[0].Nulls.SetNull(j)
		} else {
			b.Columns[0].Int64Data[j] = i
			b.Columns[0].Nulls.SetValid(j)
		}
		b.Columns[1].Int64Data[j] = i * 10
		b.Columns[1].Nulls.SetValid(j)
	}
	b.Len = int(hi - lo)
	return b
}

func cmaAgg(sm *memory.SpillManager) *HashAggregate {
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	h.Spill = sm
	return h
}

// cmaCloneWithBothRunLists consumes keys [lo,hi+3) into a clone and leaves BOTH
// partial-state lists non-empty: drainedRuns through the PartialDrainBytes
// bound, partialSpillFiles through a relief request.
func cmaCloneWithBothRunLists(t *testing.T, ctx context.Context, sm *memory.SpillManager, lo, hi int64) *HashAggregate {
	t.Helper()
	c := cmaAgg(sm)
	c.PartialDrainBytes = 1 // every Consume trips the clone-partial drain
	if err := c.Init(ctx); err != nil {
		t.Fatal(err)
	}
	mid := lo + (hi-lo)/2
	if err := c.Consume(ctx, cmaBatch(lo, mid, -1)); err != nil {
		t.Fatal(err)
	}
	if len(c.drainedRuns) == 0 {
		t.Fatal("fixture: the clone filled no drainedRuns")
	}
	if err := c.Consume(ctx, cmaBatch(mid, hi, -1)); err != nil {
		t.Fatal(err)
	}
	// A relief request is what fills the OTHER list.
	c.PartialDrainBytes = 0
	if err := c.Consume(ctx, cmaBatch(hi, hi+3, -1)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SpillSome(1 << 20); err != nil {
		t.Fatalf("SpillSome: %v", err)
	}
	if len(c.partialSpillFiles) == 0 {
		t.Fatalf("fixture: the clone filled no partialSpillFiles (drainedRuns=%d)", len(c.drainedRuns))
	}
	if len(c.drainedRuns) == 0 {
		t.Fatal("fixture: drainedRuns emptied before the merge")
	}
	return c
}

func cmaDrain(t *testing.T, ctx context.Context, h *HashAggregate) map[string]int64 {
	t.Helper()
	if err := h.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	out := map[string]int64{}
	for {
		b, err := h.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		for _, r := range b.ToRows() {
			out[fmt.Sprint(r["k"])] = r["s"].(int64)
		}
	}
	return out
}

// cmaKeys builds the expected sorted key strings for [aLo,aHi) plus [bLo,bHi),
// with the row at key 22 replaced by a NULL group when withNull is set.
func cmaKeys(aLo, aHi, bLo, bHi int64, withNull bool) []string {
	var out []string
	for i := aLo; i < aHi; i++ {
		out = append(out, fmt.Sprint(i))
	}
	for i := bLo; i < bHi; i++ {
		if withNull && i == 22 {
			continue
		}
		out = append(out, fmt.Sprint(i))
	}
	if withNull {
		out = append(out, "<nil>")
	}
	sort.Strings(out)
	return out
}

// #790's transfer sits at the TOP of mergeSinkState, before every branch. This
// is the fixture for that: a clone holding BOTH run lists, driven through each
// of the four merge branches in turn.
//
// Reverting the partialSpillFiles transfer fails three of them — the SoA fast
// path (12 groups for 15), the empty-primary adoption (8 for 11) and the
// drain-to-runs fallback (12 for 15) — and leaves the fourth green, because
// partitioned-disjoint adoption keeps the clone and finalizes it rather than
// merging it. That asymmetry is the point: it is why the defect survived a
// gate that drove one branch.
func TestCloneRunListsSurviveEveryMergeBranch(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, sm *memory.SpillManager) (primary, clone *HashAggregate, wantKeys []string)
	}{
		{
			// Both int-keyed and the primary non-empty: mergeIntGroupSoA.
			"int_soa_fast_path",
			func(t *testing.T, sm *memory.SpillManager) (*HashAggregate, *HashAggregate, []string) {
				c := cmaCloneWithBothRunLists(t, ctx, sm, 0, 8)
				p := cmaAgg(sm)
				if err := p.Init(ctx); err != nil {
					t.Fatal(err)
				}
				if err := p.Consume(ctx, cmaBatch(20, 24, -1)); err != nil {
					t.Fatal(err)
				}
				if !p.useIntGroupKey || p.intFlatAccs == nil {
					t.Fatal("fixture: the primary is not on the int SoA path")
				}
				return p, c, cmaKeys(0, 11, 20, 24, false)
			},
		},
		{
			// The primary empty: adoptStateFrom.
			"empty_primary_adopt",
			func(t *testing.T, sm *memory.SpillManager) (*HashAggregate, *HashAggregate, []string) {
				c := cmaCloneWithBothRunLists(t, ctx, sm, 0, 8)
				p := cmaAgg(sm)
				if err := p.Init(ctx); err != nil {
					t.Fatal(err)
				}
				if p.groupCount() != 0 {
					t.Fatal("fixture: the primary is not empty")
				}
				return p, c, cmaKeys(0, 11, 0, 0, false)
			},
		},
		{
			// The primary migrated to the generic map by a NULL key, so the
			// SoA fast path does not fire, and non-empty, so adoption does
			// not: the drain-to-runs fallback.
			"drain_to_runs_fallback",
			func(t *testing.T, sm *memory.SpillManager) (*HashAggregate, *HashAggregate, []string) {
				c := cmaCloneWithBothRunLists(t, ctx, sm, 0, 8)
				p := cmaAgg(sm)
				if err := p.Init(ctx); err != nil {
					t.Fatal(err)
				}
				if err := p.Consume(ctx, cmaBatch(20, 24, 22)); err != nil {
					t.Fatal(err)
				}
				return p, c, cmaKeys(0, 11, 20, 24, true)
			},
		},
		{
			// Partitioned adoption: the clone keeps its state and finalizes
			// its own runs, which is why this branch was never affected.
			"partitioned_disjoint_adoption",
			func(t *testing.T, sm *memory.SpillManager) (*HashAggregate, *HashAggregate, []string) {
				c := cmaCloneWithBothRunLists(t, ctx, sm, 0, 8)
				c.PartitionedDisjoint = true
				p := cmaAgg(sm)
				p.PartitionedDisjoint = true
				if err := p.Init(ctx); err != nil {
					t.Fatal(err)
				}
				if err := p.Consume(ctx, cmaBatch(20, 24, -1)); err != nil {
					t.Fatal(err)
				}
				return p, c, cmaKeys(0, 11, 20, 24, false)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := memory.NewTracker("test", 1<<30)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			defer sm.Cleanup()
			p, c, wantKeys := tc.setup(t, sm)
			nDrained, nPartial := len(c.drainedRuns), len(c.partialSpillFiles)
			p.MergeSink(c)
			got := cmaDrain(t, ctx, p)

			gotKeys := make([]string, 0, len(got))
			for k := range got {
				gotKeys = append(gotKeys, k)
			}
			sort.Strings(gotKeys)
			if len(gotKeys) != len(wantKeys) {
				t.Fatalf("the clone held drainedRuns=%d partialSpillFiles=%d; merged %d groups, want %d\n  got:  %v\n  want: %v",
					nDrained, nPartial, len(gotKeys), len(wantKeys), gotKeys, wantKeys)
			}
			for i := range wantKeys {
				if gotKeys[i] != wantKeys[i] {
					t.Fatalf("the group set differs at %d: %q vs %q\n  got:  %v\n  want: %v",
						i, gotKeys[i], wantKeys[i], gotKeys, wantKeys)
				}
			}
			// Values are recomputable: SUM(v) for key k is k*10, each key
			// appearing once — so a run where both sides lost the same rows
			// still fails.
			for k, v := range got {
				if k == "<nil>" {
					continue
				}
				var ik int64
				fmt.Sscan(k, &ik)
				if want := ik * 10; v != want {
					t.Errorf("key %s: merged sum %d, want %d", k, v, want)
				}
			}
			_ = p.Close()
			_ = c.Close()
		})
	}
}

// The LEGACY raw-row artifacts — spillFiles and spillBuffer — must reach the
// primary too.
//
// COUNT(DISTINCT) is non-simple, so canUseExternalMerge is false and the
// pressure branch takes the raw-row path. A clone in production cannot get
// there (its tracking-only view makes ShouldSpillFor false), so the forcing
// knob is what makes the shape reachable at all — and that is exactly why the
// gate is worth having: the knob is on this branch, and the next gate to use it
// would have hit this silently.
//
// Reverting the two transfers fails this with "merged 10 groups, want 50":
// every one of the clone's 40 keys is deleted by its own Close.
func TestCloneRawRowArtifactsSurviveTheMerge(t *testing.T) {
	ctx := context.Background()
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	mkBatch := func(lo, hi int64) *batch.RecordBatch {
		b := batch.NewRecordBatch(schema, int(hi-lo))
		for i := lo; i < hi; i++ {
			j := int(i - lo)
			b.Columns[0].Int64Data[j] = i
			b.Columns[0].Nulls.SetValid(j)
			b.Columns[1].Int64Data[j] = i
			b.Columns[1].Nulls.SetValid(j)
		}
		b.Len = int(hi - lo)
		return b
	}
	mk := func(sm *memory.SpillManager) *HashAggregate {
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggCountDistinct, InputCol: "v", OutputCol: "nd", OutputType: parquet.TypeInt64},
		})
		h.Spill = sm
		return h
	}

	// A small flush target so the buffer reaches DISK as well as staying in
	// memory — the gate wants both artifacts populated, not one.
	defer ForceSmallSpillRuns(512)()
	defer ForceAggDrainEvery(ForceAggDrainEvery(1))

	tracker := memory.NewTracker("test", 1<<30)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	// The clone gets a tracking-only view, exactly as runParallel wires it.
	clone := mk(sm.TrackingOnlyView())
	if err := clone.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for lo := int64(0); lo < 40; lo += 10 {
		if err := clone.Consume(ctx, mkBatch(lo, lo+10)); err != nil {
			t.Fatal(err)
		}
	}
	files, buffered := len(clone.spillFiles), len(clone.spillBuffer)
	if files == 0 && buffered == 0 {
		t.Fatal("fixture: the clone holds neither raw-row files nor a raw-row buffer, so this gate covers nothing")
	}

	primary := mk(sm)
	if err := primary.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := primary.Consume(ctx, mkBatch(100, 110)); err != nil {
		t.Fatal(err)
	}
	primary.MergeSink(clone)
	if err := primary.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got := map[int64]int64{}
	for {
		b, err := primary.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		for _, r := range b.ToRows() {
			got[r["k"].(int64)] = r["nd"].(int64)
		}
	}
	_ = clone.Close()
	_ = primary.Close()

	if len(got) != 50 { // 40 clone keys + 10 primary keys
		t.Errorf("merged %d groups, want 50 — the clone held spillFiles=%d spillBufferRows=%d at the merge",
			len(got), files, buffered)
	}
	for k, v := range got {
		if v != 1 {
			t.Errorf("key %d: COUNT(DISTINCT v) = %d, want 1", k, v)
		}
	}
}

// The same loss through a REAL pipeline rather than a hand-built merge, which
// is how a future gate would meet it.
//
// Partitioned aggregation is switched OFF, because that is the configuration
// where clones are MERGED rather than adopted — and it is the configuration the
// optimization-invariance oracle runs (WADJET_PARTITIONED_AGG=0), so this is
// the arm that would have tripped.
//
// Reverting the two transfers fails the parallel arms with
// "sum(nd)=2048, want 4000": 49% of the rows deleted, silently.
func TestParallelRawRowAggregateKeepsEveryRowUnderAForcedDrain(t *testing.T) {
	ctx := context.Background()
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	const n, keys = 4000, 400
	rows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, map[string]any{"k": int64(i % keys), "v": int64(i)})
	}
	run := func(workers int, forced bool) (int, int64) {
		t.Helper()
		defer ForceSmallSpillRuns(512)()
		if forced {
			defer ForceAggDrainEvery(ForceAggDrainEvery(1))
		}
		tracker := memory.NewTracker("test", 1<<30)
		sm, err := memory.NewSpillManager(t.TempDir(), tracker)
		if err != nil {
			t.Fatal(err)
		}
		defer sm.Cleanup()
		agg := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggCountDistinct, InputCol: "v", OutputCol: "nd", OutputType: parquet.TypeInt64},
		})
		agg.Spill = sm
		pipe := &Pipeline{Source: NewSliceSource(schema, rows), Sink: agg, Workers: workers}
		if err := pipe.Run(ctx); err != nil {
			t.Fatalf("workers=%d forced=%v: %v", workers, forced, err)
		}
		groups, total := 0, int64(0)
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
				total += r["nd"].(int64)
			}
		}
		_ = agg.Close()
		return groups, total
	}

	restore := partitionedAggToggle.Set(false)
	defer partitionedAggToggle.Set(restore)

	for _, tc := range []struct {
		name    string
		workers int
		forced  bool
	}{
		{"serial", 1, false},
		{"parallel", 8, false},
		{"serial_forced_drain", 1, true},
		{"parallel_forced_drain", 8, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			groups, total := run(tc.workers, tc.forced)
			if groups != keys || total != int64(n) {
				t.Errorf("groups=%d sum(nd)=%d, want %d / %d — a clone's raw-row rows did not reach the primary",
					groups, total, keys, n)
			}
		})
	}
}
