// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A JOIN'S BUILD OWNS THE ROW SET IT STORES (#1189).
//
// A stored build batch's LIVE rows are its selection vector — a filter pushed
// onto the build side marks rejected rows instead of copying the survivors out
// (CLAUDE.md, "selection vectors over copying"). Four sites in this package
// rewrite or re-walk those stored batches, and each one is a position where
// the row set can be lost:
//
//	store        the flat build appends the ARRIVAL batch, whose Sel belongs
//	             to the producer (Filter.selBuf is one reusable buffer)
//	consolidate  consolidateBuild merges the batches, copying raw rows so the
//	             arena's (batchIdx, rowIdx) refs survive at new offsets
//	prune        PruneBuildColumns rebuilds each stored batch with fewer columns
//	rebuild      FixKeyAssignment re-indexes every stored batch after a key swap
//
// Three of the four lost it. It is invisible to every KEYED consumer, because
// the arena only ever indexed the selected rows, so a rejected row sitting in
// a stored batch is simply never referenced. It is not invisible to a CROSS
// join: its probe has no key to route by, so nextCrossChunk walks buildBatches
// directly and reads each batch's Sel — and a batch that lost its Sel hands
// back the whole unfiltered relation.
//
// Every cell below is driven through a REAL exec.Filter rather than a
// hand-set Sel, because the producer's buffer reuse is half of what is under
// test, and a hand-written selection vector would own its own memory and hide
// it.
//
// At 1c2b4d25 this file fails; the log is
// cj_author/gate_exec_rowset_at_base_FAILS.log.

// cjRowSetSchema is one identity column and one boolean the filter reads, so a
// cell's answer names the build rows it published rather than counting them.
func cjRowSetSchema() []parquet.Column {
	return []parquet.Column{
		{Name: "bid", Type: parquet.TypeInt64},
		{Name: "keep", Type: parquet.TypeBool},
	}
}

// cjBuildBatches makes one batch per group of ids, with `keep` true exactly
// for the ids in want. Equal batch LENGTHS are deliberate: Filter.selBuf only
// reallocates when a batch is longer than the last, so equal-length arrivals
// are what make a stored Sel alias a later batch's selections.
func cjBuildBatches(groups [][]int64, keep map[int64]bool) []*batch.RecordBatch {
	schema := cjRowSetSchema()
	out := make([]*batch.RecordBatch, 0, len(groups))
	for _, g := range groups {
		rows := make([]map[string]any, 0, len(g))
		for _, id := range g {
			rows = append(rows, map[string]any{"bid": id, "keep": keep[id]})
		}
		out = append(out, batch.FromRows(schema, rows))
	}
	return out
}

// cjFilteredSource is a build source that runs ONE Filter over every batch, as
// a pipeline's build side does. The single Filter is the point: it carries one
// reusable selection buffer across the batches it hands to the build.
type cjFilteredSource struct {
	inner Source
	f     *Filter
}

func (s *cjFilteredSource) Init(ctx context.Context) error { return s.inner.Init(ctx) }
func (s *cjFilteredSource) Close() error                   { return s.inner.Close() }

func (s *cjFilteredSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	for {
		b, err := s.inner.Next(ctx)
		if err != nil || b == nil {
			return b, err
		}
		out, err := s.f.Execute(ctx, b)
		if err != nil {
			return nil, err
		}
		if out == nil {
			continue // every row rejected: the Filter drops the batch
		}
		return out, nil
	}
}

// cjKeepFilter accepts the rows whose `keep` column is TRUE.
func cjKeepFilter(schema []parquet.Column) *Filter {
	keepIdx := -1
	for i, c := range schema {
		if c.Name == "keep" {
			keepIdx = i
		}
	}
	return NewFilter(func(b *batch.RecordBatch, row int) bool {
		col := b.Columns[keepIdx]
		return !col.Nulls.IsNullFast(row) && col.BoolData[row]
	})
}

// cjProbeSource is one probe row, so an output row names one build row.
func cjProbeSource() Source {
	return NewBatchSource([]*batch.RecordBatch{
		batch.FromRows([]parquet.Column{{Name: "pid", Type: parquet.TypeInt64}},
			[]map[string]any{{"pid": int64(1)}}),
	})
}

// cjCrossBuildIDs runs the cross join and returns the build ids it published,
// in ascending order, as "1,3,5".
func cjCrossBuildIDs(t *testing.T, hj *HashJoin) string {
	t.Helper()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: cjProbeSource(), Ops: []UnaryOperator{hj.Probe()}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("probe: %v", err)
	}
	ids := make([]int, 0, len(sink.Rows))
	for _, r := range sink.Rows {
		v, ok := r["bid"].(int64)
		if !ok {
			t.Fatalf("row %v has no bid", r)
		}
		ids = append(ids, int(v))
	}
	sort.Ints(ids)
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprint(id)
	}
	return strings.Join(parts, ",")
}

// cjStoredBuildIDs is the build's live rows read STRAIGHT off the stored
// batches, honouring each one's selection vector, as "2,4,6". It reads the
// identities rather than counting them, and it works after a column prune has
// narrowed the stored schema, which a probe-driven reading would not.
func cjStoredBuildIDs(t *testing.T, hj *HashJoin) string {
	t.Helper()
	var ids []int
	for _, b := range hj.buildBatches {
		if b == nil {
			continue
		}
		col := b.Columns[columnIndexFallback(b, "bid")]
		// The Sel is resolved here rather than through the package's own
		// buildRowAt, so this file compiles and runs VERBATIM against a tree
		// that does not have the fix (COMMON: a new gate must fail at base).
		for pos := 0; pos < b.ActiveLen(); pos++ {
			row := pos
			if b.Sel != nil {
				row = int(b.Sel[pos])
			}
			ids = append(ids, int(col.Int64Data[row]))
		}
	}
	return cjJoinIDs(ids)
}

// cjArenaBuildIDs is the build's live rows as the INDEX holds them — one entry
// per indexed row, read through its (batchIdx, rowIdx) ref. It is what a keyed
// probe would reach, so a rebuild that re-indexed raw rows shows up here as
// identities and not only as a count.
func cjArenaBuildIDs(t *testing.T, hj *HashJoin) string {
	t.Helper()
	var ids []int
	hj.forEachArenaEntry(func(_ *joinIndexPart, _ int, ref buildRef) {
		b := hj.buildBatches[ref.batchIdx]
		col := b.Columns[columnIndexFallback(b, "bid")]
		ids = append(ids, int(col.Int64Data[ref.rowIdx]))
	})
	return cjJoinIDs(ids)
}

func cjJoinIDs(ids []int) string {
	sort.Ints(ids)
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprint(id)
	}
	return strings.Join(parts, ",")
}

// cjBuiltCross builds a CROSS join over the given batches behind one Filter.
func cjBuiltCross(t *testing.T, groups [][]int64, keep map[int64]bool, tracker *memory.Tracker) *HashJoin {
	t.Helper()
	hj := NewHashJoin(CrossJoin, nil, nil)
	hj.MemTracker = tracker
	src := &cjFilteredSource{
		inner: NewBatchSource(cjBuildBatches(groups, keep)),
		f:     cjKeepFilter(cjRowSetSchema()),
	}
	if err := hj.Build(context.Background(), src); err != nil {
		t.Fatalf("build: %v", err)
	}
	return hj
}

func cjKeepSet(ids ...int64) map[int64]bool {
	m := map[int64]bool{}
	for _, id := range ids {
		m[id] = true
	}
	return m
}

// TestAJoinsBuildOwnsTheRowSetItStores is the seam table: one row per position
// that rewrites or re-walks the stored build batches.
func TestAJoinsBuildOwnsTheRowSetItStores(t *testing.T) {
	// POSITION 1+2 — store and consolidate. Two arrivals, the second longer
	// than the first so Filter.selBuf reallocates and the ALIASING of position
	// 1 cannot contribute: what this cell measures is consolidateBuild alone.
	t.Run("consolidate_keeps_the_row_set", func(t *testing.T) {
		hj := cjBuiltCross(t,
			[][]int64{{1}, {2, 3, 4}},
			cjKeepSet(1, 3), nil)
		if len(hj.buildBatches) != 1 {
			t.Fatalf("the merge did not run (%d stored batches) — this cell is measuring nothing",
				len(hj.buildBatches))
		}
		if got, want := cjCrossBuildIDs(t, hj), "1,3"; got != want {
			t.Errorf("a merged build published rows %q, want %q — the filter rejected 2 and 4", got, want)
		}
	})

	// POSITION 1 — ownership, with the merge OUT of the way. Three arrivals of
	// EQUAL length: Filter.selBuf is allocated once, at the first batch, and
	// every later batch writes over it, so a stored batch that kept the
	// producer's slice reads a later batch's selections. The merge is skipped
	// by a tracker already past its 30% mark.
	t.Run("the_stored_row_set_is_not_the_producers_buffer", func(t *testing.T) {
		tracker := memory.NewTracker("cj-rowset", 1<<20)
		if err := tracker.Reserve(900 << 10); err != nil {
			t.Fatalf("seed the tracker past 30%%: %v", err)
		}
		hj := cjBuiltCross(t,
			[][]int64{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
			cjKeepSet(1, 3, 5, 6, 7), tracker)
		if len(hj.buildBatches) != 3 {
			t.Fatalf("the merge ran anyway (%d stored batches) — it would mask what this cell "+
				"measures", len(hj.buildBatches))
		}
		if got, want := cjCrossBuildIDs(t, hj), "1,3,5,6,7"; got != want {
			t.Errorf("stored build batches published rows %q, want %q — a later batch's "+
				"selection had overwritten an earlier batch's", got, want)
		}
	})

	// The two above in ONE build: unequal first arrival (so the merge is the
	// only path that can lose the row set) is not the shape a scan produces.
	// Equal-length arrivals THROUGH the merge is, and it is the shape #1189
	// was filed on.
	t.Run("equal_arrivals_through_the_merge", func(t *testing.T) {
		hj := cjBuiltCross(t,
			[][]int64{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
			cjKeepSet(1, 3, 5, 6, 7), nil)
		if len(hj.buildBatches) != 1 {
			t.Fatalf("the merge did not run (%d stored batches)", len(hj.buildBatches))
		}
		if got, want := cjCrossBuildIDs(t, hj), "1,3,5,6,7"; got != want {
			t.Errorf("a merged build published rows %q, want %q", got, want)
		}
	})

	// A batch every row of which is rejected never reaches the build at all
	// (Filter returns nil), and one with NO filter above it has no Sel: the
	// merge has to splice both into the same row set.
	t.Run("a_mixed_build_merges_both_kinds_of_batch", func(t *testing.T) {
		hj := cjBuiltCross(t,
			[][]int64{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
			cjKeepSet(1, 3, 7, 8, 9), nil)
		if got, want := cjCrossBuildIDs(t, hj), "1,3,7,8,9"; got != want {
			t.Errorf("a build whose middle batch was dropped whole published %q, want %q", got, want)
		}
	})

	// POSITION 3 — PruneBuildColumns. Narrowing the stored columns must not
	// change which rows are live.
	t.Run("pruning_columns_keeps_the_row_set", func(t *testing.T) {
		hj := cjBuiltCross(t,
			[][]int64{{1, 2, 3}, {4, 5, 6}},
			cjKeepSet(2, 4, 6), nil)
		// The control is the ROWS, read two ways before the prune: what the
		// cross probe publishes, and what the stored batches hold.
		if got := cjCrossBuildIDs(t, hj); got != "2,4,6" {
			t.Fatalf("the pre-prune control published %q, want 2,4,6", got)
		}
		if got := cjStoredBuildIDs(t, hj); got != "2,4,6" {
			t.Fatalf("the pre-prune stored rows are %q, want 2,4,6", got)
		}
		hj.PruneBuildColumns([]string{"bid"})
		// And the ROWS after it — identities, not a count: a prune that
		// dropped the row set would answer 1,2,3,4,5,6 here, which a count of
		// live rows would also catch, but one that SHIFTED it (a Sel carried
		// onto the wrong batch) would keep the count and change these.
		if got := cjStoredBuildIDs(t, hj); got != "2,4,6" {
			t.Errorf("pruning the stored columns changed the live rows to %q, want 2,4,6", got)
		}
	})

	// POSITION 4 — the key-swap rebuild. FixKeyAssignment throws the arena
	// away and re-indexes every stored batch; it must index the row set the
	// arrival-time index held, not the raw rows behind it. Driven at the
	// HashJoin directly, because reaching it from SQL needs a plan-time side
	// assignment that missed a pair.
	t.Run("the_key_swap_rebuild_reindexes_the_row_set", func(t *testing.T) {
		ctx := context.Background()
		// RightKeys names a column the build does NOT have and LeftKeys names
		// one it does: that is the misassignment FixKeyAssignment repairs.
		hj := NewHashJoin(InnerJoin, []string{"bid"}, []string{"pid"})
		src := &cjFilteredSource{
			inner: NewBatchSource(cjBuildBatches([][]int64{{1, 2, 3}, {4, 5, 6}},
				cjKeepSet(2, 5))),
			f: cjKeepFilter(cjRowSetSchema()),
		}
		if err := hj.Build(ctx, src); err != nil {
			t.Fatalf("build: %v", err)
		}
		// The arena BEFORE the rebuild, so the cell says what it is comparing
		// against and cannot pass by indexing nothing.
		if got := cjArenaBuildIDs(t, hj); got != "2,5" {
			t.Fatalf("the arrival-time index holds %q, want 2,5 — the filter accepted 2 and 5", got)
		}
		// NOT a skip. This cell is built so the repair fires — RightKeys names
		// a column the build does not have — so a run where it does not fire
		// is a cell that has stopped measuring the rebuild, and that is a
		// failure rather than a pass.
		if !hj.FixKeyAssignment() {
			t.Fatalf("the key repair did not fire, so the rebuild under test never ran: " +
				"this cell is no longer measuring anything")
		}
		// The ROWS the rebuild indexed, not their count: re-indexing the raw
		// rows behind the row set answers 1,2,3,4,5,6 here, and re-indexing
		// the WRONG two would keep the count 2 and change these.
		if got := cjArenaBuildIDs(t, hj); got != "2,5" {
			t.Errorf("the rebuild indexed %q, want 2,5 — the filter accepted 2 and 5", got)
		}
		if hj.buildRows != 2 {
			t.Errorf("the rebuild counted %d build rows, want 2", hj.buildRows)
		}
	})
}

// A CROSS JOIN'S BUILD STILL REFUSES LOUDLY WHEN THE BUDGET CANNOT HOLD IT.
//
// ADR-0006's 2026-09-03 routed-probe amendment: a cross join's probe reads
// every build row, so its build cannot be grace-partitioned and cannot spill,
// and a build the budget cannot hold REFUSES with a message naming the reason.
// The row-set fix adds one allocation to that path — the merged selection
// vector, bounded by the LIVE row count — so the boundary is re-measured here
// rather than assumed, on a FILTERED build (the shape that now allocates) and
// replicated, because a memory verdict taken once is a coin toss (ADR-0027).
func TestACrossJoinBuildOverTheBudgetStillRefusesLoudly(t *testing.T) {
	const runs = 5
	ctx := context.Background()
	schema := []parquet.Column{{Name: "rk", Type: parquet.TypeInt64}, {Name: "pad", Type: parquet.TypeString}}
	// A filter that accepts every row: the build then stores a selection
	// vector for every batch, which is what the fix's merge walks, and the
	// BYTES it must hold are the same as the unfiltered build's.
	keepAll := func() *Filter {
		return NewFilter(func(b *batch.RecordBatch, row int) bool { return true })
	}

	for run := 0; run < runs; run++ {
		tracker := memory.NewTracker("cross-build-tiny", 64<<10) // 64 KiB
		sm, serr := memory.NewSpillManager(t.TempDir(), tracker)
		if serr != nil {
			t.Fatalf("spill manager: %v", serr)
		}
		hj := NewHashJoin(CrossJoin, nil, nil)
		hj.MemTracker = tracker
		// WITH a spill manager attached, so the refusal is the one a real
		// spill-eligible caller gets and the message names the cross join
		// rather than the flat path's older "no spill configured".
		hj.Spill = sm
		src := &cjFilteredSource{
			inner: NewBatchSource(rowsToBatchesForProbe(schema, crossRows(20000, "rk", true))),
			f:     keepAll(),
		}
		err := hj.Build(ctx, src)
		if err == nil {
			t.Fatalf("run %d: a 20k-row filtered build under a 64 KiB budget succeeded — "+
				"the budget is not being charged", run)
		}
		if !errors.Is(err, memory.ErrMemoryExceeded) {
			t.Fatalf("run %d: build failed with %v, want a memory-budget error", run, err)
		}
		if !strings.Contains(err.Error(), "cross join") {
			t.Errorf("run %d: the refusal does not name the reason: %v", run, err)
		}
	}

	// The other direction, replicated with it: a build the budget CAN hold
	// still answers, and answers the filter's rows.
	for run := 0; run < runs; run++ {
		tracker := memory.NewTracker("cross-build-room", 8<<20)
		hj := cjBuiltCross(t, [][]int64{{1, 2, 3}, {4, 5, 6}}, cjKeepSet(1, 4, 6), tracker)
		if got, want := cjCrossBuildIDs(t, hj), "1,4,6"; got != want {
			t.Fatalf("run %d: a build inside the budget published %q, want %q", run, got, want)
		}
	}
}
