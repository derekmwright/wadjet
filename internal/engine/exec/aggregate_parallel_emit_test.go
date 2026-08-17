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

// Parallel emit (aggregate_parallel_emit.go) fans the emission of adopted
// disjoint partitions across one goroutine per partition. The values it emits
// must be identical to the serial emission, group for group; only the order
// batches arrive in may differ.

var emitTestSchema = []parquet.Column{
	{Name: "k", Type: parquet.TypeInt64, Nullable: true},
	{Name: "v", Type: parquet.TypeInt64, Nullable: true},
}

func emitTestAggs() []AggColumn {
	return []AggColumn{
		{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
		{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeFloat64},
		{Func: AggMax, InputCol: "v", OutputCol: "mx", OutputType: parquet.TypeFloat64},
	}
}

// buildAdoptedAggregate builds a primary HashAggregate that has adopted
// nUnits-1 disjoint partitions, exactly the shape Pipeline.runParallel
// produces: unit u owns every key with key%nUnits == u. Returns the finalized
// primary, ready to drain. groupsPerUnit == 0 leaves a unit empty.
func buildAdoptedAggregate(tb testing.TB, nUnits, totalGroups, rowsPerGroup int) *HashAggregate {
	tb.Helper()
	ctx := context.Background()

	prim := NewHashAggregate([]string{"k"}, emitTestAggs())
	prim.PartitionedDisjoint = true
	if err := prim.Init(ctx); err != nil {
		tb.Fatal(err)
	}
	units := []*HashAggregate{prim}
	for i := 1; i < nUnits; i++ {
		c := prim.CloneSink().(*HashAggregate)
		c.PartitionedDisjoint = true
		if err := c.Init(ctx); err != nil {
			tb.Fatal(err)
		}
		units = append(units, c)
	}

	// Disjoint feed: key k belongs to unit k%nUnits.
	perUnit := make([][]map[string]any, nUnits)
	for g := 0; g < totalGroups; g++ {
		u := g % nUnits
		for r := 0; r < rowsPerGroup; r++ {
			perUnit[u] = append(perUnit[u], map[string]any{
				"k": int64(g),
				"v": int64(g*10 + r),
			})
		}
	}
	for u, rows := range perUnit {
		for off := 0; off < len(rows); off += batch.DefaultBatchSize {
			end := off + batch.DefaultBatchSize
			if end > len(rows) {
				end = len(rows)
			}
			b := batch.FromRows(emitTestSchema, rows[off:end])
			if err := units[u].Consume(ctx, b); err != nil {
				tb.Fatal(err)
			}
		}
	}

	for i := 1; i < nUnits; i++ {
		prim.MergeSink(units[i])
	}
	if err := prim.Finalize(ctx); err != nil {
		tb.Fatal(err)
	}
	return prim
}

// drainToRows drains an aggregate and returns key → row, plus the batch
// count. Closes the aggregate.
func drainToRows(tb testing.TB, h *HashAggregate) (map[string]map[string]any, int) {
	tb.Helper()
	ctx := context.Background()
	out := map[string]map[string]any{}
	nBatches := 0
	for {
		b, err := h.Next(ctx)
		if err != nil {
			tb.Fatal(err)
		}
		if b == nil {
			break
		}
		nBatches++
		for _, r := range b.ToRows() {
			key := fmt.Sprintf("%v", r["k"])
			if _, dup := out[key]; dup {
				tb.Fatalf("duplicate group %q in emitted stream", key)
			}
			out[key] = r
		}
	}
	h.Close()
	return out, nBatches
}

func assertSameGroups(t *testing.T, want, got map[string]map[string]any) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("group count: serial %d, parallel %d", len(want), len(got))
	}
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	bad := 0
	for _, k := range keys {
		gr, ok := got[k]
		if !ok {
			bad++
			if bad <= 5 {
				t.Errorf("group %q missing from parallel emit", k)
			}
			continue
		}
		for col, sv := range want[k] {
			if fmt.Sprintf("%v", gr[col]) != fmt.Sprintf("%v", sv) {
				bad++
				if bad <= 8 {
					t.Errorf("group %q col %s: parallel %v vs serial %v", k, col, gr[col], sv)
				}
			}
		}
	}
}

// withParallelEmit runs fn with the kill switch forced to v, restoring it.
func withParallelEmit(tb testing.TB, v bool, fn func()) {
	tb.Helper()
	prev := parallelEmitToggle.Set(v)
	defer parallelEmitToggle.Set(prev)
	fn()
}

func TestParallelEmitMatchesSerial(t *testing.T) {
	cases := []struct {
		units       int
		totalGroups int
		rows        int
	}{
		{units: 1, totalGroups: 5000, rows: 3},
		{units: 2, totalGroups: 5000, rows: 3},
		{units: 4, totalGroups: 1, rows: 7},      // most partitions empty
		{units: 16, totalGroups: 9, rows: 2},     // fewer groups than units
		{units: 16, totalGroups: 20000, rows: 2}, // multi-batch per unit
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("units=%d/groups=%d", tc.units, tc.totalGroups), func(t *testing.T) {
			var serialRows, parRows map[string]map[string]any
			var parBatches int
			withParallelEmit(t, false, func() {
				serialRows, _ = drainToRows(t, buildAdoptedAggregate(t, tc.units, tc.totalGroups, tc.rows))
			})
			before := ParallelEmitRuns.Load()
			withParallelEmit(t, true, func() {
				parRows, parBatches = drainToRows(t, buildAdoptedAggregate(t, tc.units, tc.totalGroups, tc.rows))
			})
			engaged := ParallelEmitRuns.Load() != before
			if tc.units > 1 && !engaged {
				t.Fatal("parallel emit did not engage with adopted partitions")
			}
			if tc.units == 1 && engaged {
				t.Fatal("parallel emit engaged with no adopted partitions")
			}
			if tc.totalGroups > 0 && parBatches == 0 {
				t.Fatal("parallel emit produced no batches")
			}
			assertSameGroups(t, serialRows, parRows)
		})
	}
}

// A spilled aggregate emits through the streaming partial-state merger.
// That path stays serial: the eligibility gate must reject it, and the
// results must still be correct.
func TestParallelEmitSpilledTakesSerialPath(t *testing.T) {
	ctx := context.Background()
	build := func() *HashAggregate {
		tracker := memory.NewTracker("emit-spill", 64<<20)
		sm, err := memory.NewSpillManager(t.TempDir(), tracker)
		if err != nil {
			t.Fatal(err)
		}
		prim := NewHashAggregate([]string{"k"}, emitTestAggs())
		prim.PartitionedDisjoint = true
		prim.Spill = sm
		if err := prim.Init(ctx); err != nil {
			t.Fatal(err)
		}
		units := []*HashAggregate{prim}
		for i := 1; i < 4; i++ {
			c := prim.CloneSink().(*HashAggregate)
			c.PartitionedDisjoint = true
			c.Spill = sm.TrackingOnlyView()
			// Force the clone-partial drain on essentially every batch so the
			// adopted partition arrives carrying partial-state runs.
			c.PartialDrainBytes = 1
			if err := c.Init(ctx); err != nil {
				t.Fatal(err)
			}
			units = append(units, c)
		}
		for g := 0; g < 4000; g++ {
			u := g % 4
			rows := []map[string]any{{"k": int64(g), "v": int64(g)}, {"k": int64(g), "v": int64(g + 1)}}
			if err := units[u].Consume(ctx, batch.FromRows(emitTestSchema, rows)); err != nil {
				t.Fatal(err)
			}
		}
		for i := 1; i < 4; i++ {
			prim.MergeSink(units[i])
		}
		return prim
	}

	h := build()
	spilledUnits := 0
	for _, ap := range h.adoptedPartitions {
		if len(ap.partialSpillFiles) > 0 {
			spilledUnits++
		}
	}
	if spilledUnits == 0 {
		t.Fatal("test setup: no adopted partition spilled — the serial-path gate is untested")
	}
	if err := h.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	withParallelEmit(t, true, func() {
		if h.parallelEmitEligible() {
			t.Fatal("spilled adopted partition must not take the parallel emit path")
		}
		before := ParallelEmitRuns.Load()
		rows, _ := drainToRows(t, h)
		if ParallelEmitRuns.Load() != before {
			t.Fatal("parallel emit engaged on a spilled aggregate")
		}
		if len(rows) != 4000 {
			t.Fatalf("spilled serial emit: got %d groups, want 4000", len(rows))
		}
		for g := 0; g < 4000; g++ {
			r := rows[fmt.Sprintf("%d", g)]
			if r == nil {
				t.Fatalf("group %d missing", g)
			}
			if fmt.Sprintf("%v", r["cnt"]) != "2" {
				t.Fatalf("group %d cnt = %v, want 2", g, r["cnt"])
			}
		}
	})
}

// Closing mid-drain must stop every producer, close every adopted unit, and
// leak no goroutine. Run under -race for the concurrent-teardown coverage.
func TestParallelEmitEarlyClose(t *testing.T) {
	withParallelEmit(t, true, func() {
		h := buildAdoptedAggregate(t, 8, 40000, 2)
		b, err := h.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			t.Fatal("expected at least one batch")
		}
		if h.emit == nil {
			t.Fatal("parallel drain did not start")
		}
		h.Close() // must join every producer
		if h.emit != nil {
			t.Fatal("drain reference survived Close")
		}
		// Second Close is a no-op, not a panic or a hang.
		h.Close()
	})
}

// Cancelling the context mid-drain surfaces an error rather than hanging or
// emitting a truncated result silently.
func TestParallelEmitContextCancel(t *testing.T) {
	withParallelEmit(t, true, func() {
		h := buildAdoptedAggregate(t, 8, 40000, 2)
		ctx, cancel := context.WithCancel(context.Background())
		if _, err := h.Next(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		// Drain until the stream ends; a cancellation error is acceptable and
		// so is a clean end (producers may have finished before the cancel).
		for {
			b, err := h.Next(ctx)
			if err != nil {
				break
			}
			if b == nil {
				break
			}
		}
		h.Close()
	})
}

// End-to-end through a downstream Top-N sort: ORDER BY cnt DESC LIMIT n over
// a high-cardinality GROUP BY, which is the shape the parallel drain exists
// for. Serial and parallel emit must select the same rows.
func TestParallelEmitTopN(t *testing.T) {
	// Group g gets (g%7)+1 rows, so counts tie heavily across groups: 1..7.
	build := func(tb testing.TB) *HashAggregate {
		tb.Helper()
		ctx := context.Background()
		const nUnits = 8
		prim := NewHashAggregate([]string{"k"}, emitTestAggs())
		prim.PartitionedDisjoint = true
		if err := prim.Init(ctx); err != nil {
			tb.Fatal(err)
		}
		units := []*HashAggregate{prim}
		for i := 1; i < nUnits; i++ {
			c := prim.CloneSink().(*HashAggregate)
			c.PartitionedDisjoint = true
			if err := c.Init(ctx); err != nil {
				tb.Fatal(err)
			}
			units = append(units, c)
		}
		perUnit := make([][]map[string]any, nUnits)
		for g := 0; g < 5000; g++ {
			u := g % nUnits
			for r := 0; r <= g%7; r++ {
				perUnit[u] = append(perUnit[u], map[string]any{"k": int64(g), "v": int64(g)})
			}
		}
		for u, rows := range perUnit {
			for off := 0; off < len(rows); off += batch.DefaultBatchSize {
				end := off + batch.DefaultBatchSize
				if end > len(rows) {
					end = len(rows)
				}
				if err := units[u].Consume(ctx, batch.FromRows(emitTestSchema, rows[off:end])); err != nil {
					tb.Fatal(err)
				}
			}
		}
		for i := 1; i < nUnits; i++ {
			prim.MergeSink(units[i])
		}
		if err := prim.Finalize(ctx); err != nil {
			tb.Fatal(err)
		}
		return prim
	}

	topN := func(tb testing.TB, h *HashAggregate, limit int, keys []SortKey) []string {
		tb.Helper()
		ctx := context.Background()
		s := NewSort(keys)
		s.Limit = limit
		pipe := &Pipeline{Source: &aggDrainSource{agg: h}, Sink: s}
		if err := pipe.Run(ctx); err != nil {
			tb.Fatal(err)
		}
		s.Truncate(limit)
		var got []string
		for {
			b, err := s.Next(ctx)
			if err != nil {
				tb.Fatal(err)
			}
			if b == nil {
				break
			}
			for _, r := range b.ToRows() {
				got = append(got, fmt.Sprintf("%v/%v/%v", r["k"], r["cnt"], r["sv"]))
			}
		}
		s.Close()
		h.Close()
		return got
	}

	// Unique total order (k DESC): rows must match EXACTLY, order included.
	byKey := []SortKey{{Column: "k", Order: Descending}}
	for _, limit := range []int{10, 5000, 999999} {
		t.Run(fmt.Sprintf("unique-order/limit=%d", limit), func(t *testing.T) {
			var want, got []string
			withParallelEmit(t, false, func() { want = topN(t, build(t), limit, byKey) })
			withParallelEmit(t, true, func() { got = topN(t, build(t), limit, byKey) })
			if len(want) != len(got) {
				t.Fatalf("row count: serial %d parallel %d", len(want), len(got))
			}
			for i := range want {
				if want[i] != got[i] {
					t.Fatalf("row %d: serial %q parallel %q", i, want[i], got[i])
				}
			}
		})
	}

	// Tie-heavy ORDER BY cnt DESC: ties are broken by input order in the
	// existing Top-N (its heap comparator is not stable and the serial
	// concatenation order was itself arrival-dependent), so the contract is
	// the multiset of SORT KEY values, not row identity.
	t.Run("ties/limit=10", func(t *testing.T) {
		byCnt := []SortKey{{Column: "cnt", Order: Descending}}
		cnts := func(rows []string) []string {
			out := make([]string, len(rows))
			for i, r := range rows {
				out[i] = splitSlash(r)[1] // the cnt field
			}
			return out
		}
		var want, got []string
		withParallelEmit(t, false, func() { want = cnts(topN(t, build(t), 10, byCnt)) })
		withParallelEmit(t, true, func() { got = cnts(topN(t, build(t), 10, byCnt)) })
		if len(want) != len(got) {
			t.Fatalf("row count: serial %d parallel %d", len(want), len(got))
		}
		for i := range want {
			if want[i] != got[i] {
				t.Fatalf("sort key at rank %d: serial %q parallel %q (%v vs %v)", i, want[i], got[i], want, got)
			}
		}
	})
}

func splitSlash(s string) [3]string {
	var out [3]string
	idx := 0
	start := 0
	for i := 0; i < len(s) && idx < 3; i++ {
		if s[i] == '/' {
			out[idx] = s[start:i]
			idx++
			start = i + 1
		}
	}
	if idx < 3 {
		out[idx] = s[start:]
	}
	return out
}

// aggDrainSource adapts a finalized HashAggregate into a pipeline Source —
// the exec-package stand-in for physical.aggSourceAdapter's output phase.
type aggDrainSource struct{ agg *HashAggregate }

func (s *aggDrainSource) ServesHeldState() bool { return true }
func (s *aggDrainSource) Init(context.Context) error {
	return nil
}
func (s *aggDrainSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	return s.agg.Next(ctx)
}
func (s *aggDrainSource) Close() error { return nil }
