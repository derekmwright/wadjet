// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The partition share of a budgeted aggregate's presize (#1706, line 1706f).
//
// A partitioned aggregate runs k sinks, the primary (workerSinks[0]) and k-1
// clones, each owning a hash partition of the key space. Under a budget each
// one's initial capacity was capped at the WHOLE input estimate, so seven
// clones of a 5,000-row LATERAL body each force-charged a 4,096-slot
// group-state chunk for one group, and whether a hash join's first
// reservation refused followed how many of them had been released. The cap
// is now each sink's share of the estimate, ceil(estimate / k). These tests
// hold the arithmetic, that the share binds on every aggregate path that
// presizes, that a partition receiving EVERY group (the share is a sizing
// heuristic, not a bound) answers what the whole-estimate presize answered
// and is charged for its growth, and that an unbudgeted aggregate allocates
// exactly what it did.

// shareEstimate and shareWorkers are #1706's lateral body: 5,000 input rows,
// partitioned. With eight partitions the share is 625, well under the
// 4,096-slot default the whole estimate leaves in place.
const (
	shareEstimate = 5000
	shareWorkers  = 8
)

// TestAPartitionedAggregatesPresizeCapIsItsShareOfTheEstimate holds the
// arithmetic and where the division happens: once, for the primary and the
// clones alike, and only for the sizing cap.
func TestAPartitionedAggregatesPresizeCapIsItsShareOfTheEstimate(t *testing.T) {
	ctx := context.Background()
	budgeted := func(t *testing.T) *memory.SpillManager {
		sm, err := memory.NewSpillManager(t.TempDir(), memory.NewTracker("query", 512*1024))
		if err != nil {
			t.Fatal(err)
		}
		return sm
	}
	newPrimary := func(sm *memory.SpillManager, est int64, k int) *HashAggregate {
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggCount, OutputCol: "n", OutputType: parquet.TypeInt64},
		})
		h.InputRowHint = est
		h.Spill = sm
		h.cloneNDVDivisor = k // what runParallel sets on the primary before cloning
		h.SetInputRowBound(est)
		return h
	}

	for _, tc := range []struct {
		name string
		est  int64
		k    int
		n    int
		want int
	}{
		{"#1706's lateral body over 24 partitions", 5000, 24, 4096, 209}, // ceil(5000/24) = 208.33…
		{"over 8 partitions", 5000, 8, 4096, 625},
		{"an exact multiple is not rounded up", 4800, 24, 4096, 200},
		{"a one-row estimate stays a one-row cap, never the zero that means unknown", 1, 24, 4096, 1},
		{"an estimate below the partition count still rounds up to one", 7, 24, 4096, 1},
		{"an unknown estimate stays unknown: the default is kept", 0, 24, 4096, 4096},
		{"the largest estimate does not overflow", math.MaxInt64, 24, 4096, 4096},
		{"a share above the requested capacity leaves it alone", 1 << 20, 24, 4096, 4096},
		{"one partition is the whole estimate", 5000, 1, 4096, 4096},
		{"a non-partitioned aggregate is the whole estimate", 3000, 0, 4096, 3000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sm := budgeted(t)
			prim := newPrimary(sm, tc.est, tc.k)
			if err := prim.Init(ctx); err != nil {
				t.Fatal(err)
			}
			defer prim.Close()
			if got := prim.presizeBound(tc.n); got != tc.want {
				t.Fatalf("primary: presizeBound(%d) with estimate %d over %d partitions = %d, want %d",
					tc.n, tc.est, tc.k, got, tc.want)
			}
			cl := prim.CloneSink().(*HashAggregate)
			cl.Spill = sm.TrackingOnlyView()
			// A NON-partitioned pipeline's clones carry no divisor (CloneSink
			// copies it only when it is > 1): each may see the whole key set,
			// so the whole estimate stays their cap.
			if got := cl.presizeBound(tc.n); got != tc.want {
				t.Fatalf("clone: presizeBound(%d) = %d, want %d: the clone's cap must be divided exactly once, "+
					"as the primary's is", tc.n, got, tc.want)
			}
			// Only the sizing cap is divided: what else reads the estimate
			// still reads the whole of it.
			if prim.InputRowHint != tc.est || prim.InputRowBound() != tc.est {
				t.Fatalf("primary's InputRowHint/inputRowBound moved to %d/%d, want %d",
					prim.InputRowHint, prim.InputRowBound(), tc.est)
			}
			if cl.InputRowHint != 0 || cl.presizeRowCap != tc.est || cl.InputRowBound() != tc.est {
				t.Fatalf("clone carries InputRowHint=%d presizeRowCap=%d inputRowBound=%d, want 0/%d/%d",
					cl.InputRowHint, cl.presizeRowCap, cl.InputRowBound(), tc.est, tc.est)
			}
		})
	}
}

// presizePath is one aggregate path that presizes under a budget.
type presizePath struct {
	name    string
	group   []string
	aggs    []AggColumn
	schema  []parquet.Column
	row     func(g int) map[string]any // one input row of group g
	reached func(h *HashAggregate) bool
}

func presizePaths() []presizePath {
	sumV := []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64}}
	return []presizePath{
		{
			name: "typed integer key with flat accumulators", group: []string{"k"}, aggs: sumV,
			schema: []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "v", Type: parquet.TypeInt64}},
			row:    func(g int) map[string]any { return map[string]any{"k": int64(g), "v": int64(1)} },
			reached: func(h *HashAggregate) bool {
				return h.useIntGroupKey && h.intGroupIndex != nil && h.intFlatAccs != nil
			},
		},
		{
			name: "packed composite key", group: []string{"a", "b"}, aggs: sumV,
			schema: []parquet.Column{{Name: "a", Type: parquet.TypeInt64}, {Name: "b", Type: parquet.TypeInt64},
				{Name: "v", Type: parquet.TypeInt64}},
			row: func(g int) map[string]any {
				return map[string]any{"a": int64(g % 97), "b": int64(g), "v": int64(1)}
			},
			reached: func(h *HashAggregate) bool { return h.usePackedGroupKey && h.packedIdx != nil },
		},
		{
			name: "compact key", group: []string{"f", "i"}, aggs: sumV,
			schema: []parquet.Column{{Name: "f", Type: parquet.TypeBool}, {Name: "i", Type: parquet.TypeInt32},
				{Name: "v", Type: parquet.TypeInt64}},
			row: func(g int) map[string]any {
				return map[string]any{"f": g%2 == 0, "i": int32(g), "v": int64(1)}
			},
			reached: func(h *HashAggregate) bool { return h.useCompactGroupKey && h.intGroupIndex != nil },
		},
		{
			name: "string key", group: []string{"s"}, aggs: sumV,
			schema: []parquet.Column{{Name: "s", Type: parquet.TypeString}, {Name: "v", Type: parquet.TypeInt64}},
			row: func(g int) map[string]any {
				return map[string]any{"s": fmt.Sprintf("g%05d", g), "v": int64(1)}
			},
			reached: func(h *HashAggregate) bool { return h.useStrGroupKey && h.strGroupIndex != nil },
		},
		{
			name: "typed-generic key (the lateral body's STRING beside INT64)", group: []string{"s", "k"}, aggs: sumV,
			schema: []parquet.Column{{Name: "s", Type: parquet.TypeString}, {Name: "k", Type: parquet.TypeInt64},
				{Name: "v", Type: parquet.TypeInt64}},
			row: func(g int) map[string]any {
				return map[string]any{"s": fmt.Sprintf("g%05d", g), "k": int64(g % 7), "v": int64(1)}
			},
			reached: func(h *HashAggregate) bool { return h.useGenericSoA && h.genKeyIdx != nil },
		},
		{
			// A nullable key moves off the integer fast path onto the generic
			// map, whose group states come from the group-state pool.
			name: "generic map over the group-state pool", group: []string{"k"},
			aggs:   []AggColumn{{Func: AggCount, OutputCol: "n", OutputType: parquet.TypeInt64}},
			schema: []parquet.Column{{Name: "k", Type: parquet.TypeInt64, Nullable: true}},
			row: func(g int) map[string]any {
				if g == 0 {
					return map[string]any{"k": nil}
				}
				return map[string]any{"k": int64(g)}
			},
			reached: func(h *HashAggregate) bool { return h.strGroupIndex != nil && len(h.gsPool.chunks) > 0 },
		},
	}
}

func (p presizePath) batch(groups ...int) *batch.RecordBatch {
	rows := make([]map[string]any, len(groups))
	for i, g := range groups {
		rows[i] = p.row(g)
	}
	return batch.FromRows(p.schema, rows)
}

func renderAggRows(rows []map[string]any) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = fmt.Sprint(r) // fmt prints map keys sorted
	}
	sort.Strings(out)
	return out
}

// shareSink builds one partition of a partitioned aggregate the way
// runParallel wires it: the primary with the divisor set, or a clone of it
// with a tracking-only view of the same spill manager and the partition's
// drain bound. divisor 0 is the whole-estimate presize the engine had before
// (CloneSink copies no divisor, and presizeBound divides by none).
//
// finish answers the partition's rows the way the pipeline does: a clone is
// adopted by the primary (MergeSink, which hands over its drained runs) and
// the primary emits it.
func shareSink(t *testing.T, ctx context.Context, p presizePath, sm *memory.SpillManager, role string,
	divisor int) (h *HashAggregate, finish func() []map[string]any) {
	t.Helper()
	prim := NewHashAggregate(p.group, p.aggs)
	prim.InputRowHint = shareEstimate
	prim.Spill = sm
	prim.cloneNDVDivisor = divisor
	prim.PartitionedDisjoint = true
	if err := prim.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { prim.Close() })
	if role == "primary" {
		return prim, func() []map[string]any { return drainAgg(t, prim) }
	}
	cl := prim.CloneSink().(*HashAggregate)
	cl.Spill = sm.TrackingOnlyView()
	cl.PartitionedDisjoint = true
	if b := sm.Tracker().Budget(); b > 0 {
		cl.PartialDrainBytes = b * 3 / int64(4*shareWorkers)
	}
	if err := cl.Init(ctx); err != nil {
		t.Fatal(err)
	}
	adopted := false
	t.Cleanup(func() {
		if !adopted {
			cl.Close()
		}
	})
	return cl, func() []map[string]any {
		prim.MergeSink(cl) // adoption: the primary now owns and closes the clone
		adopted = true
		return drainAgg(t, prim)
	}
}

// TestAPartitionedAggregatesShareBindsOnEveryPresizingPathAndSkewStillAnswers
// runs every presizing path, as the primary and as a clone, under a budget,
// and feeds ONE partition every group: 6,000 groups, nearly ten times its
// 625-row share and past the 4,096-slot default.
//
//   - The share binds: at 64 MiB, the first batch (two groups, one of them
//     NULL) charges less than the whole-estimate presize charges for it.
//   - Nothing relies on the initial capacity covering the groups: the
//     partition answers exactly the rows the whole-estimate presize answers.
//   - Growth is charged as it is today: after every batch the aggregate's
//     tracked charge is its measured group state (reconcileGroupMemory), and
//     the tracker holds it.
//
// Two budgets: 64 MiB, where nothing drains, so the charge is the whole
// state; and 512 KiB, #1706's, where a clone's growth crosses its drain
// bound and drains to run files (the primary's state is force-charged past
// the budget; growth is charged, not refused).
func TestAPartitionedAggregatesShareBindsOnEveryPresizingPathAndSkewStillAnswers(t *testing.T) {
	ctx := context.Background()
	const groups = 6000
	for _, p := range presizePaths() {
		for _, role := range []string{"primary", "clone"} {
			for _, budget := range []int64{64 << 20, 512 * 1024} {
				t.Run(fmt.Sprintf("%s/%s/%dKiB", p.name, role, budget>>10), func(t *testing.T) {
					run := func(divisor int) (firstCharge int64, rows []string) {
						tr := memory.NewTracker("query", budget)
						sm, err := memory.NewSpillManager(t.TempDir(), tr)
						if err != nil {
							t.Fatal(err)
						}
						h, finish := shareSink(t, ctx, p, sm, role, divisor)
						check := func(when string) {
							t.Helper()
							if len(h.drainedRuns) > 0 || len(h.partialSpillFiles) > 0 {
								return // a drain released the drained state; the run files hold it
							}
							if got, want := h.trackedGroupMem, h.groupMemoryUsage(); got != want {
								t.Fatalf("divisor %d, %s: tracked %d bytes of group state, measured %d: "+
									"growth is not charged as it is", divisor, when, got, want)
							}
							if got := tr.Used(); got < h.trackedGroupMem {
								t.Fatalf("divisor %d, %s: the tracker holds %d bytes, below the aggregate's "+
									"tracked %d", divisor, when, got, h.trackedGroupMem)
							}
						}
						if err := h.Consume(ctx, p.batch(0, 1)); err != nil {
							t.Fatal(err)
						}
						if len(h.drainedRuns) == 0 && len(h.partialSpillFiles) == 0 && !p.reached(h) {
							t.Fatalf("the batch did not reach the %s path", p.name)
						}
						check("after the first batch")
						firstCharge = h.trackedGroupMem
						// Every group, twice, into this one partition.
						all := make([]int, 0, 2*groups)
						for g := 0; g < groups; g++ {
							all = append(all, g, g)
						}
						for off := 0; off < len(all); off += batch.DefaultBatchSize {
							end := min(off+batch.DefaultBatchSize, len(all))
							if err := h.Consume(ctx, p.batch(all[off:end]...)); err != nil {
								t.Fatalf("divisor %d: a partition receiving every group refused at row %d: %v",
									divisor, off, err)
							}
							check(fmt.Sprintf("after row %d", end))
						}
						return firstCharge, renderAggRows(finish())
					}
					wholeFirst, wholeRows := run(0)
					shareFirst, shareRows := run(shareWorkers)
					t.Logf("first-batch charge: whole estimate %d bytes, share %d bytes", wholeFirst, shareFirst)
					// The bind is measured where nothing drains: at 512 KiB a
					// clone's first batch may cross its drain bound and leave
					// nothing charged at all.
					if budget == 64<<20 && shareFirst >= wholeFirst {
						t.Fatalf("the first batch charged %d bytes with the %d-row share and %d with the whole "+
							"%d-row estimate: the share did not bind on this path (#1706)",
							shareFirst, (shareEstimate+shareWorkers-1)/shareWorkers, wholeFirst, shareEstimate)
					}
					if len(shareRows) != groups || len(wholeRows) != groups {
						t.Fatalf("a skewed partition answered %d groups, want %d", len(shareRows), groups)
					}
					for i := range wholeRows {
						if shareRows[i] != wholeRows[i] {
							t.Fatalf("row %d: the share presize answered %s, the whole-estimate presize %s",
								i, shareRows[i], wholeRows[i])
						}
					}
				})
			}
		}
	}
}

// TestAnUnbudgetedPartitionedAggregateAllocatesWhatItDid holds that the share
// acts only under a positive budget: on every presizing path, as primary and
// clone, with no spill manager or a zero budget, a partitioned aggregate's
// group state after its first batch and after every group is byte for byte
// the whole-estimate presize's.
func TestAnUnbudgetedPartitionedAggregateAllocatesWhatItDid(t *testing.T) {
	ctx := context.Background()
	for _, p := range presizePaths() {
		for _, role := range []string{"primary", "clone"} {
			for _, mode := range []string{"no spill manager", "zero budget"} {
				t.Run(p.name+"/"+role+"/"+mode, func(t *testing.T) {
					run := func(divisor int) []int64 {
						var sm *memory.SpillManager
						if mode == "zero budget" {
							var err error
							if sm, err = memory.NewSpillManager(t.TempDir(), memory.NewTracker("query", 0)); err != nil {
								t.Fatal(err)
							}
						}
						var h *HashAggregate
						if sm == nil {
							prim := NewHashAggregate(p.group, p.aggs)
							prim.InputRowHint = shareEstimate
							prim.cloneNDVDivisor = divisor
							h = prim
							if role == "clone" {
								h = prim.CloneSink().(*HashAggregate)
								h.PartitionedDisjoint = true
							}
							if err := h.Init(ctx); err != nil {
								t.Fatal(err)
							}
							t.Cleanup(func() { h.Close() })
						} else {
							h, _ = shareSink(t, ctx, p, sm, role, divisor)
						}
						var sizes []int64
						if err := h.Consume(ctx, p.batch(0, 1)); err != nil {
							t.Fatal(err)
						}
						if !p.reached(h) {
							t.Fatalf("the batch did not reach the %s path", p.name)
						}
						sizes = append(sizes, h.groupMemoryUsage(), int64(len(h.gsPool.chunks)))
						if len(h.gsPool.chunks) > 0 {
							sizes = append(sizes, int64(len(h.gsPool.chunks[0])))
						}
						gs := make([]int, 3000)
						for g := range gs {
							gs[g] = g
						}
						for off := 0; off < len(gs); off += batch.DefaultBatchSize {
							end := min(off+batch.DefaultBatchSize, len(gs))
							if err := h.Consume(ctx, p.batch(gs[off:end]...)); err != nil {
								t.Fatal(err)
							}
							sizes = append(sizes, h.groupMemoryUsage())
						}
						return sizes
					}
					whole, share := run(0), run(shareWorkers)
					if fmt.Sprint(whole) != fmt.Sprint(share) {
						t.Fatalf("an unbudgeted partitioned aggregate's group state moved: %v with the divisor, "+
							"%v without it; the share must act only under a positive budget", share, whole)
					}
				})
			}
		}
	}
}

// TestARealPartitionedPipelineSizesEveryPartitionToItsShare runs the pipeline
// itself (eight workers, partitioned aggregation, a budget) over the generic
// map's shape and reads every partition's first group-state chunk afterwards:
// the primary's — whose divisor runParallel sets before its first batch, so
// a clone-only division would have left it at the whole estimate — and each
// adopted clone's.
func TestARealPartitionedPipelineSizesEveryPartitionToItsShare(t *testing.T) {
	ctx := context.Background()
	const workers = 8
	const groups = 4000
	sm, err := memory.NewSpillManager(t.TempDir(), memory.NewTracker("query", 64<<20))
	if err != nil {
		t.Fatal(err)
	}
	agg := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggCount, OutputCol: "n", OutputType: parquet.TypeInt64},
	})
	agg.InputRowHint = shareEstimate
	agg.Spill = sm
	schema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64, Nullable: true}}
	var batches []*batch.RecordBatch
	for off := 0; off < groups; off += 1000 {
		rows := []map[string]any{{"k": nil}} // a NULL key in every batch: the generic map
		for g := off; g < off+1000; g++ {
			rows = append(rows, map[string]any{"k": int64(g)})
		}
		batches = append(batches, batch.FromRows(schema, rows))
	}
	pipe := &Pipeline{Source: &benchBatchSource{batches: batches}, Sink: agg, Workers: workers}
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer agg.Close()
	if !agg.PartitionedDisjoint || agg.cloneNDVDivisor != workers {
		t.Fatalf("the pipeline did not run partitioned over %d workers (disjoint=%v divisor=%d)",
			workers, agg.PartitionedDisjoint, agg.cloneNDVDivisor)
	}
	share := (shareEstimate + workers - 1) / workers
	parts := append([]*HashAggregate{agg}, agg.adoptedPartitions...)
	if len(parts) != workers {
		t.Fatalf("%d partitions, want %d", len(parts), workers)
	}
	for i, p := range parts {
		if len(p.gsPool.chunks) == 0 {
			t.Fatalf("partition %d (0 is the primary) received no group", i)
		}
		if got := len(p.gsPool.chunks[0]); got != share {
			t.Fatalf("partition %d (0 is the primary): first group-state chunk is %d slots, want its %d-row "+
				"share of the %d-row estimate (#1706)", i, got, share, shareEstimate)
		}
	}
	n := 0
	for {
		out, err := agg.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		n += out.ActiveLen()
	}
	if n != groups+1 {
		t.Fatalf("%d groups, want %d", n, groups+1)
	}
}
