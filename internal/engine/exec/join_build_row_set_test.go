// SPDX-License-Identifier: MIT

package exec

import (
	"context"
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
		before := cjCrossBuildIDs(t, hj)
		live := make([]int, 0, 8)
		for _, b := range hj.buildBatches {
			live = append(live, b.ActiveLen())
		}
		hj.PruneBuildColumns([]string{"bid"})
		after := 0
		for _, b := range hj.buildBatches {
			after += b.ActiveLen()
		}
		total := 0
		for _, n := range live {
			total += n
		}
		if after != total {
			t.Errorf("pruning changed the live row count from %d to %d", total, after)
		}
		if before != "2,4,6" {
			t.Fatalf("the pre-prune control published %q, want 2,4,6", before)
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
		if !hj.FixKeyAssignment() {
			t.Skip("the key assignment needed no repair on this build — the rebuild is unreached")
		}
		if hj.buildRows != 2 {
			t.Errorf("the rebuild indexed %d build rows, want 2 — the filter accepted 2 and 5",
				hj.buildRows)
		}
	})
}
