// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// These tests hold the aggregate OBJECT alive across Close and require every
// piece of group state it held to become unreachable (#1685, line 1685b).
// Before the fix Close released the tracker charge but kept the hash indexes,
// the per-group state slices, the key mirrors, the groupState chunk pool and
// the per-batch scratch reachable from the operator, so anything that still
// referenced the operator — a stale relief-registry slot, a pipeline field —
// kept megabytes alive while the ledger said zero.
//
// Reachability is observed with runtime.AddCleanup on each structure, never
// with heap totals: a cleanup fires only once its object is unreachable, so
// the verdict does not depend on what else the process allocates.

// closeWatch records which watched structures have been collected.
type closeWatch struct {
	mu        sync.Mutex
	names     []string
	collected map[int]bool
}

func newCloseWatch() *closeWatch { return &closeWatch{collected: map[int]bool{}} }

// add registers a cleanup on the allocation p points into. p must not be
// retained by the caller — the cleanup argument is an index, not the pointer.
func (w *closeWatch) add(name string, p unsafe.Pointer) {
	w.mu.Lock()
	idx := len(w.names)
	w.names = append(w.names, name)
	w.mu.Unlock()
	runtime.AddCleanup((*byte)(p), func(i int) {
		w.mu.Lock()
		w.collected[i] = true
		w.mu.Unlock()
	}, idx)
}

// watchSlice watches s's backing array. Pointer-free arrays of 16 bytes or
// less may share a tiny-allocator block with live objects, so their cleanup
// is not guaranteed to run: they are skipped.
func watchSlice[T any](w *closeWatch, name string, s []T) {
	if cap(s) == 0 {
		return
	}
	var zero T
	if uintptr(cap(s))*unsafe.Sizeof(zero) <= 16 {
		return
	}
	w.add(name, unsafe.Pointer(unsafe.SliceData(s)))
}

func watchPtr[T any](w *closeWatch, name string, p *T) {
	if p == nil {
		return
	}
	w.add(name, unsafe.Pointer(p))
}

// watchAggState registers every group-state structure h currently holds. It
// must run while the off-heap path is OFF (an mmap'd array is not a heap
// object and cannot carry a cleanup); watchAggIndexes is the off-heap variant.
func watchAggState(w *closeWatch, prefix string, h *HashAggregate) {
	watchAggIndexes(w, prefix, h)
	watchSlice(w, prefix+"keys", h.keys)
	watchSlice(w, prefix+"serializedKeys", h.serializedKeys)
	watchSlice(w, prefix+"compactKeys", h.compactKeys)
	watchSlice(w, prefix+"intGroupStates", h.intGroupStates)
	watchSlice(w, prefix+"strGroupStates", h.strGroupStates)
	watchSlice(w, prefix+"genKeyNext", h.genKeyNext)
	watchSlice(w, prefix+"keySerCols", h.keySerCols)
	watchSlice(w, prefix+"keyBuf", h.keyBuf)
	watchSlice(w, prefix+"intKeys", h.intKeys)
	watchSlice(w, prefix+"packedKeys", h.packedKeys)
	watchSlice(w, prefix+"packedCols", h.packedCols)
	watchSlice(w, prefix+"groupIndexBuf", h.groupIndexBuf)
	watchSlice(w, prefix+"freeGroupIDs", h.freeGroupIDs)
	watchSlice(w, prefix+"scalarAccs", h.scalarAccs)
	watchSlice(w, prefix+"spillBuffer", h.spillBuffer)
	watchSlice(w, prefix+"emitSchema", h.emitSchema)
	watchSlice(w, prefix+"intFlatAccs", h.intFlatAccs)
	for i, fa := range h.intFlatAccs {
		watchSlice(w, fmt.Sprintf("%sintFlatAccs[%d].count", prefix, i), fa.count)
		watchSlice(w, fmt.Sprintf("%sintFlatAccs[%d].sumI64", prefix, i), fa.sumI64)
		watchSlice(w, fmt.Sprintf("%sintFlatAccs[%d].sumF64", prefix, i), fa.sumF64)
	}
	watchSlice(w, prefix+"gsPool.chunks", h.gsPool.chunks)
	for i, c := range h.gsPool.chunks {
		watchSlice(w, fmt.Sprintf("%sgsPool.chunks[%d]", prefix, i), c)
	}
}

// watchAggIndexes registers the hash-index headers, which are heap objects
// on every path (their arrays may be off-heap).
func watchAggIndexes(w *closeWatch, prefix string, h *HashAggregate) {
	watchPtr(w, prefix+"intGroupIndex", h.intGroupIndex)
	watchPtr(w, prefix+"intTwoLevel", h.intTwoLevel)
	watchPtr(w, prefix+"packedIdx", h.packedIdx)
	watchPtr(w, prefix+"packedTwoLevel", h.packedTwoLevel)
	watchPtr(w, prefix+"strGroupIndex", h.strGroupIndex)
	watchPtr(w, prefix+"genKeyIdx", h.genKeyIdx)
}

// requireCollected collects until every watched structure is gone, bounded
// at about a second, and fails naming the survivors. keep is held alive for
// the whole wait: it is the operator (or operators) Close ran on.
func (w *closeWatch) requireCollected(t *testing.T, keep ...any) {
	t.Helper()
	w.mu.Lock()
	total := len(w.names)
	w.mu.Unlock()
	if total == 0 {
		t.Fatal("watched no group state: the case did not build any")
	}
	deadline := time.Now().Add(time.Second)
	for {
		runtime.GC()
		w.mu.Lock()
		done := len(w.collected) == total
		w.mu.Unlock()
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	runtime.KeepAlive(keep)
	w.mu.Lock()
	defer w.mu.Unlock()
	var alive []string
	for i, n := range w.names {
		if !w.collected[i] {
			alive = append(alive, n)
		}
	}
	if len(alive) > 0 {
		sort.Strings(alive)
		t.Fatalf("after Close, %d of %d group-state structures are still reachable from the operator: %v",
			len(alive), total, alive)
	}
}

// withOffheapAgg sets the off-heap aggregate switch for the test.
func withOffheapAgg(t *testing.T, on bool) {
	t.Helper()
	for _, tg := range optswitch.All() {
		if tg.Name == "offheap-agg" {
			prev := tg.Set(on)
			t.Cleanup(func() { tg.Set(prev) })
			return
		}
	}
	t.Fatal("offheap-agg switch not registered")
}

type closeCase struct {
	name    string
	groupBy []string
	aggs    []AggColumn
	schema  []parquet.Column
	row     func(i int) map[string]any
	// check proves the case reached the path it names.
	check func(h *HashAggregate) bool
}

func closeCases() []closeCase {
	sumV := AggColumn{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64}
	cntV := AggColumn{Func: AggCount, InputCol: "v", OutputCol: "n", OutputType: parquet.TypeInt64}
	distinctV := AggColumn{Func: AggCountDistinct, InputCol: "v", OutputCol: "d", OutputType: parquet.TypeInt64}
	return []closeCase{
		{
			name: "int", groupBy: []string{"k"}, aggs: []AggColumn{sumV, cntV},
			schema: []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "v", Type: parquet.TypeInt64}},
			row:    func(i int) map[string]any { return map[string]any{"k": int64(i % 3000), "v": int64(i)} },
			check:  func(h *HashAggregate) bool { return h.useIntGroupKey },
		},
		{
			name: "packed", groupBy: []string{"a", "b"}, aggs: []AggColumn{sumV},
			schema: []parquet.Column{{Name: "a", Type: parquet.TypeInt64}, {Name: "b", Type: parquet.TypeInt32}, {Name: "v", Type: parquet.TypeInt64}},
			row: func(i int) map[string]any {
				return map[string]any{"a": int64(i % 1500), "b": int32(i % 2), "v": int64(i)}
			},
			check: func(h *HashAggregate) bool { return h.usePackedGroupKey },
		},
		{
			name: "compact", groupBy: []string{"f", "c"}, aggs: []AggColumn{sumV},
			schema: []parquet.Column{{Name: "f", Type: parquet.TypeBool}, {Name: "c", Type: parquet.TypeString}, {Name: "v", Type: parquet.TypeInt64}},
			row: func(i int) map[string]any {
				j := i % 676
				return map[string]any{"f": i%2 == 0, "c": string([]byte{byte('a' + j/26), byte('a' + j%26)}), "v": int64(i)}
			},
			check: func(h *HashAggregate) bool { return h.useCompactGroupKey },
		},
		{
			name: "string", groupBy: []string{"g"}, aggs: []AggColumn{sumV, cntV},
			schema: []parquet.Column{{Name: "g", Type: parquet.TypeString}, {Name: "v", Type: parquet.TypeInt64}},
			row:    func(i int) map[string]any { return map[string]any{"g": fmt.Sprintf("key-%06d", i%3000), "v": int64(i)} },
			check:  func(h *HashAggregate) bool { return h.useStrGroupKey },
		},
		{
			// Three string keys: two would fit the compact path's 8-byte estimate.
			name: "typed-generic", groupBy: []string{"g", "x", "y"}, aggs: []AggColumn{sumV},
			schema: []parquet.Column{{Name: "g", Type: parquet.TypeString}, {Name: "x", Type: parquet.TypeString}, {Name: "y", Type: parquet.TypeString}, {Name: "v", Type: parquet.TypeInt64}},
			row: func(i int) map[string]any {
				return map[string]any{"g": fmt.Sprintf("key-%06d", i%3000), "x": "same-for-all", "y": "also-same", "v": int64(i)}
			},
			check: func(h *HashAggregate) bool { return h.useGenericSoA && h.deferGenericKeyBoxing },
		},
		{
			name: "generic-map", groupBy: []string{"g"}, aggs: []AggColumn{distinctV, cntV},
			schema: []parquet.Column{{Name: "g", Type: parquet.TypeString}, {Name: "v", Type: parquet.TypeInt64}},
			row: func(i int) map[string]any {
				return map[string]any{"g": fmt.Sprintf("key-%06d", i%3000), "v": int64(i % 7)}
			},
			check: func(h *HashAggregate) bool {
				return !h.useIntGroupKey && !h.usePackedGroupKey && !h.useCompactGroupKey &&
					!h.useStrGroupKey && !h.useGenericSoA && len(h.gsPool.chunks) > 0
			},
		},
		{
			name: "scalar", groupBy: nil, aggs: []AggColumn{sumV, cntV},
			schema: []parquet.Column{{Name: "v", Type: parquet.TypeInt64}},
			row:    func(i int) map[string]any { return map[string]any{"v": int64(i)} },
			check:  func(h *HashAggregate) bool { return h.isScalarAgg },
		},
	}
}

func (c closeCase) batches(n, per int) []*batch.RecordBatch {
	var out []*batch.RecordBatch
	for b := 0; b < n; b++ {
		rows := make([]map[string]any, per)
		for r := range rows {
			rows[r] = c.row(b*per + r)
		}
		out = append(out, batch.FromRows(c.schema, rows))
	}
	return out
}

func newCloseTestSpill(t *testing.T) (*memory.SpillManager, *memory.Tracker) {
	t.Helper()
	tracker := memory.NewTracker("close-test", 1<<30)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	return sm, tracker
}

func drainAggN(t *testing.T, h *HashAggregate, limit int) {
	t.Helper()
	for i := 0; limit < 0 || i < limit; i++ {
		b, err := h.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			return
		}
	}
}

// TestHashAggregateCloseDropsGroupState covers every key path on every Close
// route a single operator can take: after a full drain, after Finalize with
// nothing emitted, mid-emission, before Finalize (the error and cancellation
// route: the pipeline closes whatever it holds), and a second Close.
func TestHashAggregateCloseDropsGroupState(t *testing.T) {
	withOffheapAgg(t, false)
	routes := []struct {
		name string
		run  func(t *testing.T, h *HashAggregate)
	}{
		{"normal-end", func(t *testing.T, h *HashAggregate) {
			if err := h.Finalize(context.Background()); err != nil {
				t.Fatal(err)
			}
			drainAggN(t, h, -1)
		}},
		{"after-finalize", func(t *testing.T, h *HashAggregate) {
			if err := h.Finalize(context.Background()); err != nil {
				t.Fatal(err)
			}
		}},
		{"mid-emission", func(t *testing.T, h *HashAggregate) {
			if err := h.Finalize(context.Background()); err != nil {
				t.Fatal(err)
			}
			drainAggN(t, h, 1)
		}},
		{"before-finalize", func(t *testing.T, h *HashAggregate) {}},
	}
	for _, c := range closeCases() {
		for _, r := range routes {
			t.Run(c.name+"/"+r.name, func(t *testing.T) {
				sm, tracker := newCloseTestSpill(t)
				h := NewHashAggregate(c.groupBy, c.aggs)
				h.Spill = sm
				ctx := context.Background()
				if err := h.Init(ctx); err != nil {
					t.Fatal(err)
				}
				for _, b := range c.batches(3, 2048) {
					if err := h.Consume(ctx, b); err != nil {
						t.Fatal(err)
					}
				}
				if !c.check(h) {
					t.Fatalf("case %s did not reach its path", c.name)
				}
				w := newCloseWatch()
				watchAggState(w, "", h)
				r.run(t, h)
				if err := h.Close(); err != nil {
					t.Fatal(err)
				}
				if err := h.Close(); err != nil {
					t.Fatalf("second Close: %v", err)
				}
				if used := tracker.Used(); used != 0 {
					t.Fatalf("tracker.Used() = %d after Close, want 0", used)
				}
				w.requireCollected(t, h)
			})
		}
	}
}

// TestHashAggregateCloseDropsSpilledState: the external-merge path (Close
// mid-drain of the k-way merger) and the raw-row path (Close before Finalize
// with spilled row files and a buffer).
func TestHashAggregateCloseDropsSpilledState(t *testing.T) {
	withOffheapAgg(t, false)
	restore := ForceAggDrainEvery(2)
	defer ForceAggDrainEvery(restore)
	for _, c := range closeCases() {
		if c.name != "int" && c.name != "generic-map" {
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			sm, tracker := newCloseTestSpill(t)
			h := NewHashAggregate(c.groupBy, c.aggs)
			h.Spill = sm
			ctx := context.Background()
			if err := h.Init(ctx); err != nil {
				t.Fatal(err)
			}
			for _, b := range c.batches(5, 2048) {
				if err := h.Consume(ctx, b); err != nil {
					t.Fatal(err)
				}
			}
			if len(h.partialSpillFiles) == 0 && len(h.spillFiles) == 0 && len(h.spillBuffer) == 0 {
				t.Fatal("the forced drain did not spill")
			}
			w := newCloseWatch()
			watchAggState(w, "", h)
			if c.name == "int" {
				if err := h.Finalize(ctx); err != nil {
					t.Fatal(err)
				}
				if h.partialMerger == nil {
					t.Fatal("the external merge was not entered")
				}
				drainAggN(t, h, 1)
			}
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			if used := tracker.Used(); used != 0 {
				t.Fatalf("tracker.Used() = %d after Close, want 0", used)
			}
			w.requireCollected(t, h)
		})
	}
}

// TestHashAggregateCloseDropsCloneState: a merged clone (state adopted by
// the primary's key merge, the route the partitioned demotion takes) and an
// adopted disjoint partition (closed by the primary's Close, serially or by
// the parallel drain). Both operators stay referenced by the test.
func TestHashAggregateCloseDropsCloneState(t *testing.T) {
	withOffheapAgg(t, false)
	c := closeCases()[0] // int
	for _, mode := range []string{"merged", "adopted-serial", "adopted-parallel"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "adopted-parallel" {
				prev := parallelEmitToggle.Set(true)
				defer parallelEmitToggle.Set(prev)
			} else if mode == "adopted-serial" {
				prev := parallelEmitToggle.Set(false)
				defer parallelEmitToggle.Set(prev)
			}
			ctx := context.Background()
			primary := NewHashAggregate(c.groupBy, c.aggs)
			disjoint := mode != "merged"
			primary.PartitionedDisjoint = disjoint
			if err := primary.Init(ctx); err != nil {
				t.Fatal(err)
			}
			var clones []*HashAggregate
			w := newCloseWatch()
			bs := c.batches(4, 2048)
			for i := 0; i < 2; i++ {
				cl := primary.CloneSink().(*HashAggregate)
				cl.PartitionedDisjoint = disjoint
				if err := cl.Init(ctx); err != nil {
					t.Fatal(err)
				}
				if err := cl.Consume(ctx, bs[i]); err != nil {
					t.Fatal(err)
				}
				watchAggState(w, fmt.Sprintf("clone%d.", i), cl)
				clones = append(clones, cl)
			}
			if err := primary.Consume(ctx, bs[2]); err != nil {
				t.Fatal(err)
			}
			watchAggState(w, "primary.", primary)
			for _, cl := range clones {
				primary.MergeSink(cl)
				if !disjoint {
					if err := cl.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := primary.Finalize(ctx); err != nil {
				t.Fatal(err)
			}
			drainAggN(t, primary, 1)
			if err := primary.Close(); err != nil {
				t.Fatal(err)
			}
			w.requireCollected(t, primary, clones)
		})
	}
}

// TestHashAggregateCloseOffheap: with the off-heap path ON, Close unmaps the
// reservations exactly once and drops the (heap) index headers.
func TestHashAggregateCloseOffheap(t *testing.T) {
	withOffheapAgg(t, true)
	if !memory.OffheapAvailable() {
		t.Skip("off-heap path not available on this platform")
	}
	c := closeCases()[0]
	sm, tracker := newCloseTestSpill(t)
	h := NewHashAggregate(c.groupBy, c.aggs)
	h.Spill = sm
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for _, b := range c.batches(3, 2048) {
		if err := h.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	reg := h.offheap
	if reg == nil || reg.Mappings() == 0 {
		t.Fatal("the off-heap path was not taken")
	}
	w := newCloseWatch()
	watchAggIndexes(w, "", h)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if n := reg.Mappings(); n != 0 {
		t.Fatalf("%d off-heap mappings remain after Close", n)
	}
	if h.offheap != nil || len(h.retiredOffheap) != 0 {
		t.Fatal("Close kept a reference to an off-heap registry")
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("tracker.Used() = %d after Close, want 0", used)
	}
	w.requireCollected(t, h)
}

// TestHashAggregateUseAfterClose: after Close every pipeline entry point
// answers errAggregateClosed rather than emitting from (or consuming into)
// state that is gone; Close itself stays idempotent and the relief entry
// points stay no-ops.
func TestHashAggregateUseAfterClose(t *testing.T) {
	c := closeCases()[0]
	sm, tracker := newCloseTestSpill(t)
	h := NewHashAggregate(c.groupBy, c.aggs)
	h.Spill = sm
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	bs := c.batches(2, 2048)
	if err := h.Consume(ctx, bs[0]); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	// Each call is bounded and recovered: before the fix a Consume after
	// Close looped forever in the int index (its arrays were gone, its
	// header was not), so an unbounded call would hang the package.
	within := func(name string, fn func() string) {
		t.Helper()
		done := make(chan string, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					done <- fmt.Sprintf("panicked: %v", r)
				}
			}()
			done <- fn()
		}()
		select {
		case msg := <-done:
			if msg != "" {
				t.Errorf("%s after Close: %s", name, msg)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s after Close: did not return within 5s", name)
		}
	}
	want := func(err error) string {
		if errors.Is(err, errAggregateClosed) {
			return ""
		}
		return fmt.Sprintf("err = %v, want errAggregateClosed", err)
	}
	within("Consume", func() string { return want(h.Consume(ctx, bs[1])) })
	within("Finalize", func() string { return want(h.Finalize(ctx)) })
	within("Next", func() string {
		b, err := h.Next(ctx)
		if b != nil {
			return "emitted a batch"
		}
		return want(err)
	})
	within("Init", func() string { return want(h.Init(ctx)) })
	within("SpillSome", func() string {
		if freed, err := h.SpillSome(1 << 20); freed != 0 || err != nil {
			return fmt.Sprintf("(%d, %v), want (0, nil)", freed, err)
		}
		return ""
	})
	within("DrainOnHeapPressure", func() string {
		if drained, err := h.DrainOnHeapPressure(ctx); drained || err != nil {
			return fmt.Sprintf("(%v, %v), want (false, nil)", drained, err)
		}
		return ""
	})
	within("StateBytes", func() string {
		if got := h.StateBytes(); got != 0 {
			return fmt.Sprintf("%d, want 0", got)
		}
		return ""
	})
	within("second Close", func() string {
		if err := h.Close(); err != nil {
			return err.Error()
		}
		return ""
	})
	if used := tracker.Used(); used != 0 {
		t.Errorf("tracker.Used() = %d after Close, want 0", used)
	}
}
