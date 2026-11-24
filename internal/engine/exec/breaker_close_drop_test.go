// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Sort, Window and SortMergeJoin Close drop what they hold (#1685 lines
// "Sort.Close", "Window.Close", "SortMergeJoin.Close"): the aggregate's shape
// (1685b). Each case keeps the operator object alive across Close and
// requires every buffered or materialized batch to become unreachable, the
// tracker to read zero and no run file to remain; a call after Close returns
// an explicit error instead of answering from released state.
//
// Before the fix Sort.Close released its charge but kept s.sorted (the
// materialized output) and Window.Close kept w.batches and w.result: a held
// operator kept megabytes alive while the ledger said zero, and Next after
// Close handed the released batches out again.

func bcBatches(n int) []*batch.RecordBatch {
	return lcGen("s", n, 24, func(i int) int64 { return int64((i * 7919) % n) })
}

func bcSpill(t *testing.T, budget int64) (*memory.SpillManager, *memory.Tracker) {
	t.Helper()
	tracker := memory.NewTracker("bc", budget)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	return sm, tracker
}

func bcWatchBatches(w *closeWatch, name string, bs []*batch.RecordBatch) {
	watchSlice(w, name, bs)
	for i, b := range bs {
		if b != nil {
			watchPtr(w, fmt.Sprintf("%s[%d]", name, i), b)
		}
	}
}

func bcCheckFiles(t *testing.T, sm *memory.SpillManager) {
	t.Helper()
	f := &lcFaults{dir: sm.SpillDir(), links: map[int64][]string{}}
	if n, names := f.filesLeft(t); n != 0 {
		t.Errorf("after Close, %d run files remain: %v", n, names)
	}
	if fds := lcOpenFDs(sm.SpillDir()); len(fds) != 0 {
		t.Errorf("after Close, %d run descriptors are still open: %v", len(fds), fds)
	}
}

// ---- Sort ----

func watchSortState(w *closeWatch, s *Sort) {
	bcWatchBatches(w, "sort.batches", s.batches)
	bcWatchBatches(w, "sort.sorted", s.sorted)
	watchPtr(w, "sort.merger", s.merger)
}

func TestSortCloseDropsWhatItHolds(t *testing.T) {
	cases := []struct {
		name     string
		spill    bool
		finalize bool
		emit     int // batches taken from Next before Close; -1 drains
	}{
		{name: "before-finalize"},
		{name: "after-finalize", finalize: true},
		{name: "mid-emission", finalize: true, emit: 2},
		{name: "normal-end", finalize: true, emit: -1},
		{name: "external-before-finalize", spill: true},
		{name: "external-mid-merge", spill: true, finalize: true, emit: 2},
		{name: "external-normal-end", spill: true, finalize: true, emit: -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			budget := int64(64 << 20)
			if c.spill {
				forceTinyRuns(t)
				budget = 512
			}
			sm, tracker := bcSpill(t, budget)
			ctx := context.Background()
			s := NewSort([]SortKey{{Column: "sk", Order: Ascending}})
			s.Spill = sm
			if err := s.Init(ctx); err != nil {
				t.Fatal(err)
			}
			for _, b := range bcBatches(12_000) {
				if err := s.Consume(ctx, b); err != nil {
					t.Fatal(err)
				}
			}
			if c.spill && len(s.runFiles) == 0 {
				t.Fatal("the sort never spilled")
			}
			if c.finalize {
				if err := s.Finalize(ctx); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; c.emit < 0 || i < c.emit; i++ {
				b, err := s.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if b == nil {
					break
				}
			}
			w := newCloseWatch()
			watchSortState(w, s)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("second Close: %v", err)
			}
			if used := tracker.Used(); used != 0 {
				t.Errorf("after Close the tracker holds %d bytes", used)
			}
			bcCheckFiles(t, sm)
			if len(w.names) == 0 {
				// Nothing batch-shaped was held at Close on this route; it
				// pins that Close still removes every run.
				return
			}
			w.requireUnreachable(t, "sort", s)
		})
	}
}

func TestSortUseAfterClose(t *testing.T) {
	sm, tracker := bcSpill(t, 64<<20)
	ctx := context.Background()
	s := NewSort([]SortKey{{Column: "sk", Order: Ascending}})
	s.Spill = sm
	if err := s.Init(ctx); err != nil {
		t.Fatal(err)
	}
	in := bcBatches(4096)
	for _, b := range in {
		if err := s.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if b, err := s.Next(ctx); !errors.Is(err, errSortClosed) {
		t.Errorf("Next after Close = (%v, %v), want errSortClosed", b != nil, err)
	}
	if err := s.Consume(ctx, bcBatches(16)[0]); !errors.Is(err, errSortClosed) {
		t.Errorf("Consume after Close: %v, want errSortClosed", err)
	}
	if err := s.Finalize(ctx); !errors.Is(err, errSortClosed) {
		t.Errorf("Finalize after Close: %v, want errSortClosed", err)
	}
	if err := s.Init(ctx); !errors.Is(err, errSortClosed) {
		t.Errorf("Init after Close: %v, want errSortClosed", err)
	}
	if n, err := s.SpillSome(1 << 30); n != 0 || err != nil {
		t.Errorf("SpillSome after Close = (%d, %v), want (0, nil)", n, err)
	}
	if used := tracker.Used(); used != 0 {
		t.Errorf("after Close and the calls that followed, the tracker holds %d bytes", used)
	}
}

// ---- Window ----

func bcWindowCols() []WindowColumn {
	return []WindowColumn{{
		Func:        WinRowNumber,
		OutputCol:   "rn",
		OutputType:  parquet.TypeInt64,
		PartitionBy: []string{"sv"},
		OrderBy:     []SortKey{{Column: "sk", Order: Ascending}},
	}}
}

func watchWindowState(w *closeWatch, win *Window) {
	bcWatchBatches(w, "window.batches", win.batches)
	bcWatchBatches(w, "window.result", win.result)
	watchSlice(w, "window.groups", win.groups)
	watchPtr(w, "window.ext", win.ext)
}

func TestWindowCloseDropsWhatItHolds(t *testing.T) {
	cases := []struct {
		name     string
		spill    bool
		finalize bool
		emit     int
	}{
		{name: "before-finalize"},
		{name: "after-finalize", finalize: true},
		{name: "mid-emission", finalize: true, emit: 2},
		{name: "normal-end", finalize: true, emit: -1},
		{name: "external-before-finalize", spill: true},
		{name: "external-mid-stream", spill: true, finalize: true, emit: 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			budget := int64(64 << 20)
			if c.spill {
				forceTinyRuns(t)
				budget = 512
			}
			sm, tracker := bcSpill(t, budget)
			ctx := context.Background()
			win := NewWindow(bcWindowCols())
			win.Spill = sm
			if err := win.Init(ctx); err != nil {
				t.Fatal(err)
			}
			for _, b := range bcBatches(12_000) {
				// sv groups rows into partitions of ten.
				for j := 0; j < b.Len; j++ {
					b.Columns[1].Int64Data[j] /= 10
				}
				if err := win.Consume(ctx, b); err != nil {
					t.Fatal(err)
				}
			}
			if c.spill && len(win.runFiles) == 0 {
				t.Fatal("the window never spilled")
			}
			if c.finalize {
				if err := win.Finalize(ctx); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; c.emit < 0 || i < c.emit; i++ {
				b, err := win.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if b == nil {
					break
				}
			}
			w := newCloseWatch()
			watchWindowState(w, win)
			if err := win.Close(); err != nil {
				t.Fatal(err)
			}
			if err := win.Close(); err != nil {
				t.Fatalf("second Close: %v", err)
			}
			if used := tracker.Used(); used != 0 {
				t.Errorf("after Close the tracker holds %d bytes", used)
			}
			bcCheckFiles(t, sm)
			w.requireUnreachable(t, "window", win)
		})
	}
}

func TestWindowUseAfterClose(t *testing.T) {
	sm, tracker := bcSpill(t, 64<<20)
	ctx := context.Background()
	win := NewWindow(bcWindowCols())
	win.Spill = sm
	if err := win.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for _, b := range bcBatches(4096) {
		if err := win.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if err := win.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := win.Close(); err != nil {
		t.Fatal(err)
	}
	if b, err := win.Next(ctx); !errors.Is(err, errWindowClosed) {
		t.Errorf("Next after Close = (%v, %v), want errWindowClosed", b != nil, err)
	}
	if err := win.Consume(ctx, bcBatches(16)[0]); !errors.Is(err, errWindowClosed) {
		t.Errorf("Consume after Close: %v, want errWindowClosed", err)
	}
	if err := win.Finalize(ctx); !errors.Is(err, errWindowClosed) {
		t.Errorf("Finalize after Close: %v, want errWindowClosed", err)
	}
	if err := win.Init(ctx); !errors.Is(err, errWindowClosed) {
		t.Errorf("Init after Close: %v, want errWindowClosed", err)
	}
	if n, err := win.SpillSome(1 << 30); n != 0 || err != nil {
		t.Errorf("SpillSome after Close = (%d, %v), want (0, nil)", n, err)
	}
	if used := tracker.Used(); used != 0 {
		t.Errorf("after Close and the calls that followed, the tracker holds %d bytes", used)
	}
}

// ---- SortMergeJoin ----

func watchSMJState(w *closeWatch, j *SortMergeJoin) {
	for _, side := range []*smjSide{&j.probe, &j.build} {
		bcWatchBatches(w, "smj."+side.name+".batches", side.batches)
	}
	watchPtr(w, "smj.lstream", j.lstream)
	watchPtr(w, "smj.rstream", j.rstream)
	if j.lstream != nil {
		watchPtr(w, "smj.lstream.cur", j.lstream.cur)
	}
	if j.rstream != nil {
		watchPtr(w, "smj.rstream.cur", j.rstream.cur)
	}
	watchSlice(w, "smj.group.refs", j.group.refs)
	watchSlice(w, "smj.pending", j.pending)
}

func bcSMJ(t *testing.T, spill bool) (*SortMergeJoin, *memory.SpillManager, *memory.Tracker) {
	t.Helper()
	budget := int64(64 << 20)
	if spill {
		forceTinyRuns(t)
		budget = 256 << 10
	}
	sm, tracker := bcSpill(t, budget)
	j := NewSortMergeJoin([]string{"pk"}, []string{"bk"})
	j.Spill = sm
	ctx := context.Background()
	if err := j.Init(ctx); err != nil {
		t.Fatal(err)
	}
	build := lcGen("b", 12_000, 24, func(i int) int64 { return int64(i % 3000) })
	if err := j.Build(ctx, NewBatchSource(build)); err != nil {
		t.Fatal(err)
	}
	for _, b := range lcGen("p", 12_000, 24, func(i int) int64 { return int64((i * 7919) % 6000) }) {
		if err := j.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if spill && len(j.build.runFiles)+len(j.probe.runFiles) == 0 {
		t.Fatal("the sort-merge join never spilled")
	}
	return j, sm, tracker
}

func TestSortMergeJoinCloseDropsWhatItHolds(t *testing.T) {
	cases := []struct {
		name     string
		spill    bool
		finalize bool
		emit     int
	}{
		{name: "before-finalize"},
		{name: "mid-merge", finalize: true, emit: 2},
		{name: "external-before-finalize", spill: true},
		{name: "external-mid-merge", spill: true, finalize: true, emit: 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j, sm, tracker := bcSMJ(t, c.spill)
			ctx := context.Background()
			if c.finalize {
				if err := j.Finalize(ctx); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < c.emit; i++ {
				if b, err := j.Next(ctx); err != nil || b == nil {
					t.Fatalf("Next %d: (%v, %v)", i, b != nil, err)
				}
			}
			w := newCloseWatch()
			watchSMJState(w, j)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			if err := j.Close(); err != nil {
				t.Fatalf("second Close: %v", err)
			}
			if used := tracker.Used(); used != 0 {
				t.Errorf("after Close the tracker holds %d bytes", used)
			}
			bcCheckFiles(t, sm)
			if len(w.names) == 0 {
				return // fully spilled before Finalize: nothing batch-shaped held
			}
			w.requireUnreachable(t, "sort-merge join", j)
		})
	}
}

func TestSortMergeJoinUseAfterClose(t *testing.T) {
	j, _, tracker := bcSMJ(t, false)
	ctx := context.Background()
	if err := j.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if b, err := j.Next(ctx); !errors.Is(err, errSortMergeJoinClosed) {
		t.Errorf("Next after Close = (%v, %v), want errSortMergeJoinClosed", b != nil, err)
	}
	if err := j.Consume(ctx, lcGen("p", 16, 4, func(i int) int64 { return int64(i) })[0]); !errors.Is(err, errSortMergeJoinClosed) {
		t.Errorf("Consume after Close: %v, want errSortMergeJoinClosed", err)
	}
	if err := j.Finalize(ctx); !errors.Is(err, errSortMergeJoinClosed) {
		t.Errorf("Finalize after Close: %v, want errSortMergeJoinClosed", err)
	}
	if err := j.Build(ctx, NewBatchSource(lcGen("b", 16, 4, func(i int) int64 { return int64(i) }))); !errors.Is(err, errSortMergeJoinClosed) {
		t.Errorf("Build after Close: %v, want errSortMergeJoinClosed", err)
	}
	if n, err := j.SpillSome(1 << 30); n != 0 || err != nil {
		t.Errorf("SpillSome after Close = (%d, %v), want (0, nil)", n, err)
	}
	if used := tracker.Used(); used != 0 {
		t.Errorf("after Close and the calls that followed, the tracker holds %d bytes", used)
	}
}
