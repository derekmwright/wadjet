// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The representative rule at every place two members of one group meet
// (#1713; ADR-0024's 2026-11-24 amendment): whichever order the members
// arrive in, the group keeps the one smallest under the representative
// order (batch/representative.go) — 7 before 7.00, -0 before 0 — and the
// whole tuple of ONE member.

// repRow is one input row: a key spelling per key column, plus a tag.
type repRow struct {
	keys []any
	tag  int64
}

func repSchema(keyTypes []parquet.TypeID) []parquet.Column {
	var s []parquet.Column
	for i, t := range keyTypes {
		c := parquet.Column{Name: fmt.Sprintf("k%d", i), Type: t, Nullable: true}
		if t == parquet.TypeDecimal {
			c.Precision, c.Scale = 18, 4
		}
		s = append(s, c)
	}
	return append(s, parquet.Column{Name: "tag", Type: parquet.TypeInt64})
}

func repBatch(keyTypes []parquet.TypeID, rows []repRow) *batch.RecordBatch {
	schema := repSchema(keyTypes)
	b := batch.NewRecordBatch(schema, len(rows))
	for r, row := range rows {
		for c, k := range row.keys {
			if k == nil {
				b.Columns[c].Nulls.SetNull(r)
				continue
			}
			b.Columns[c].SetValue(r, k)
			b.Columns[c].Nulls.SetValid(r)
		}
		b.Columns[len(keyTypes)].Int64Data[r] = row.tag
		b.Columns[len(keyTypes)].Nulls.SetValid(r)
	}
	return b
}

func repKeyNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("k%d", i)
	}
	return out
}

// repDrain renders every output row's keys (as their boxes print) and the
// first aggregate, sorted.
func repDrain(t *testing.T, h *HashAggregate, nKeys int) string {
	t.Helper()
	ctx := context.Background()
	var rows []string
	for {
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for i := 0; i < out.ActiveLen(); i++ {
			r := i
			if out.Sel != nil {
				r = int(out.Sel[i])
			}
			var cells []string
			for c := 0; c <= nKeys && c < len(out.Columns); c++ {
				v := out.Columns[c].GetValueOf(r, false)
				if f, ok := v.(float64); ok && f == 0 && math.Signbit(f) {
					v = "-0"
				}
				if f, ok := v.(float32); ok && f == 0 && math.Signbit(float64(f)) {
					v = "-0"
				}
				cells = append(cells, fmt.Sprint(v))
			}
			rows = append(rows, strings.Join(cells, ","))
		}
	}
	sort.Strings(rows)
	return strings.Join(rows, " | ")
}

type repCase struct {
	name     string
	keyTypes []parquet.TypeID
	aggs     func() []AggColumn
	sets     [][]int
	elig     bool
	// rows arrive as given and reversed; both must answer want.
	rows []repRow
	want string
}

var (
	negZero   = math.Copysign(0, -1)
	negZero32 = float32(math.Copysign(0, -1))
)

func repSum() []AggColumn {
	return []AggColumn{{Func: AggSum, InputCol: "tag", OutputCol: "s", OutputType: parquet.TypeInt64}}
}

// A COUNT(DISTINCT) takes the row-at-a-time path (processRow).
func repCountDistinct() []AggColumn {
	return []AggColumn{{Func: AggCountDistinct, InputCol: "tag", OutputCol: "s", OutputType: parquet.TypeInt64}}
}

func repCases() []repCase {
	dec := parquet.TypeDecimal
	return []repCase{
		{name: "processRow decimal", keyTypes: []parquet.TypeID{dec}, aggs: repCountDistinct,
			rows: []repRow{{[]any{"7.00"}, 1}, {[]any{"7"}, 2}, {[]any{"7.0"}, 3}},
			want: "7,3"},
		{name: "generic SoA decimal (eager)", keyTypes: []parquet.TypeID{dec, parquet.TypeInt64}, aggs: repSum,
			rows: []repRow{{[]any{"7.00", int64(1)}, 1}, {[]any{"7", int64(1)}, 2}, {[]any{"1.50", int64(2)}, 4}, {[]any{"1.5", int64(2)}, 8}},
			want: "1.5,2,12 | 7,1,3"},
		{name: "single decimal key (generic SoA)", keyTypes: []parquet.TypeID{dec}, aggs: repSum,
			rows: []repRow{{[]any{"7.00"}, 1}, {[]any{"7.0000"}, 2}, {[]any{"7"}, 4}},
			want: "7,7"},
		{name: "deferred float64 key", keyTypes: []parquet.TypeID{parquet.TypeFloat64}, aggs: repSum,
			rows: []repRow{{[]any{0.0}, 1}, {[]any{negZero}, 2}, {[]any{0.0}, 4}},
			want: "-0,7"},
		{name: "deferred float64 beside int", keyTypes: []parquet.TypeID{parquet.TypeInt64, parquet.TypeFloat64}, aggs: repSum,
			rows: []repRow{{[]any{int64(1), 0.0}, 1}, {[]any{int64(1), negZero}, 2}},
			want: "1,-0,3"},
		{name: "compact float32 beside bool", keyTypes: []parquet.TypeID{parquet.TypeFloat32, parquet.TypeBool}, aggs: repSum,
			rows: []repRow{{[]any{float32(0), true}, 1}, {[]any{negZero32, true}, 2}},
			want: "-0,true,3"},
		{name: "processRow float64", keyTypes: []parquet.TypeID{parquet.TypeFloat64}, aggs: repCountDistinct,
			rows: []repRow{{[]any{0.0}, 1}, {[]any{negZero}, 2}},
			want: "-0,2"},
		{name: "processRow cidr", keyTypes: []parquet.TypeID{parquet.TypeCIDR}, aggs: repCountDistinct,
			rows: []repRow{{[]any{"10.0.0.1/32"}, 1}, {[]any{"10.0.0.1"}, 2}},
			want: "10.0.0.1,2"},
		// Whole row: the two members' smaller spellings are in different
		// columns, so the survivor is the member whose FIRST key is smaller
		// — never the assembled (7, 8).
		{name: "whole row, processRow", keyTypes: []parquet.TypeID{dec, dec}, aggs: repCountDistinct,
			rows: []repRow{{[]any{"7.00", "8"}, 1}, {[]any{"7", "8.00"}, 2}},
			want: "7,8.00,2"},
		{name: "whole row, generic SoA", keyTypes: []parquet.TypeID{dec, dec}, aggs: repSum,
			rows: []repRow{{[]any{"7.00", "8"}, 1}, {[]any{"7", "8.00"}, 2}},
			want: "7,8.00,3"},
		{name: "grouping sets", keyTypes: []parquet.TypeID{dec, dec}, aggs: repSum, sets: [][]int{{0}, {0, 1}},
			rows: []repRow{{[]any{"7.00", "8"}, 1}, {[]any{"7", "8.00"}, 2}, {[]any{"7.0", "8"}, 4}},
			want: "7,8.00,7 | 7,<nil>,7"},
		// Eligibility (a set operation's counting aggregate): a left member
		// (tag 1) is the representative whenever the group has one, the
		// order deciding only within one class.
		{name: "eligibility: right member never wins", keyTypes: []parquet.TypeID{dec}, aggs: repSum, elig: true,
			rows: []repRow{{[]any{"7"}, 0}, {[]any{"7.00"}, 1}, {[]any{"7.0"}, 1}},
			want: "7.0,2"},
		{name: "eligibility: no left member", keyTypes: []parquet.TypeID{dec}, aggs: repSum, elig: true,
			rows: []repRow{{[]any{"7.00"}, 0}, {[]any{"7"}, 0}},
			want: "7,0"},
	}
}

func repNewAgg(c repCase) *HashAggregate {
	h := NewHashAggregate(repKeyNames(len(c.keyTypes)), c.aggs())
	h.GroupingSets = c.sets
	if c.elig {
		h.SetRepresentativeEligibility("s")
	}
	return h
}

func repReverse(rows []repRow) []repRow {
	out := make([]repRow, len(rows))
	for i, r := range rows {
		out[len(rows)-1-i] = r
	}
	return out
}

// Consume hit paths: the rows arrive in ONE batch (the hit happens inside
// the batch's lookup loop) and one row per batch (the hit meets a group an
// earlier batch made), each in both orders.
func TestRepresentativeConsumeHitPaths(t *testing.T) {
	ctx := context.Background()
	for _, c := range repCases() {
		t.Run(c.name, func(t *testing.T) {
			for _, order := range [][]repRow{c.rows, repReverse(c.rows)} {
				for _, perRow := range []bool{false, true} {
					h := repNewAgg(c)
					if err := h.Init(ctx); err != nil {
						t.Fatal(err)
					}
					if perRow {
						for _, r := range order {
							if err := h.Consume(ctx, repBatch(c.keyTypes, []repRow{r})); err != nil {
								t.Fatal(err)
							}
						}
					} else if err := h.Consume(ctx, repBatch(c.keyTypes, order)); err != nil {
						t.Fatal(err)
					}
					if err := h.Finalize(ctx); err != nil {
						t.Fatal(err)
					}
					if got := repDrain(t, h, len(c.keyTypes)); got != c.want {
						t.Errorf("order %v perRow=%v: got %s, want %s", order, perRow, got, c.want)
					}
				}
			}
		})
	}
}

// The clone merge (mergeSinkState): each member in its own clone, merged in
// both orders, into a primary that holds nothing and into one that holds a
// third member.
func TestRepresentativeCloneMerge(t *testing.T) {
	ctx := context.Background()
	for _, c := range repCases() {
		if c.sets != nil {
			continue // grouping sets never run cloned (not SinkSurvivesCloning-relevant here)
		}
		t.Run(c.name, func(t *testing.T) {
			for _, order := range [][]repRow{c.rows, repReverse(c.rows)} {
				parent := repNewAgg(c)
				if err := parent.Init(ctx); err != nil {
					t.Fatal(err)
				}
				// The primary consumes the first member itself (the
				// morsel-parallel warmup), the clones one member each.
				if err := parent.Consume(ctx, repBatch(c.keyTypes, order[:1])); err != nil {
					t.Fatal(err)
				}
				var clones []*HashAggregate
				for _, r := range order[1:] {
					cl := parent.CloneSink().(*HashAggregate)
					if err := cl.Init(ctx); err != nil {
						t.Fatal(err)
					}
					if err := cl.Consume(ctx, repBatch(c.keyTypes, []repRow{r})); err != nil {
						t.Fatal(err)
					}
					clones = append(clones, cl)
				}
				for _, cl := range clones {
					parent.MergeSink(cl)
				}
				if err := parent.Finalize(ctx); err != nil {
					t.Fatal(err)
				}
				if got := repDrain(t, parent, len(c.keyTypes)); got != c.want {
					t.Errorf("order %v: got %s, want %s", order, got, c.want)
				}
			}
		})
	}
}

// The spill run merge (kWayMerger) end to end: memory pressure from the
// first Consume, so every batch spills a run and the remainder is source 0;
// each member in its own batch, both orders.
func TestRepresentativeSpillRunMerge(t *testing.T) {
	ctx := context.Background()
	prev := ForceAggDrainEvery(1)
	t.Cleanup(func() { ForceAggDrainEvery(prev) })
	for _, c := range repCases() {
		if c.sets != nil {
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			for _, order := range [][]repRow{c.rows, repReverse(c.rows)} {
				tracker := memory.NewTracker("test", 1_000)
				sm, err := memory.NewSpillManager(t.TempDir(), tracker)
				if err != nil {
					t.Fatal(err)
				}
				h := repNewAgg(c)
				h.Spill = sm
				if err := h.Init(ctx); err != nil {
					t.Fatal(err)
				}
				tracker.ForceReserve(900)
				rawBefore := RawRowSpillFiles.Load()
				forcedRawBefore := ForcedRawRowDrains.Load()
				for _, r := range order {
					if err := h.Consume(ctx, repBatch(c.keyTypes, []repRow{r})); err != nil {
						t.Fatal(err)
					}
				}
				if err := h.Finalize(ctx); err != nil {
					t.Fatal(err)
				}
				// Simple aggregates spill partial-state runs the k-way merger
				// merges; the row-at-a-time ones spill raw rows that Finalize
				// replays through the consume hit path.
				spilled := h.partialMerger != nil || RawRowSpillFiles.Load() > rawBefore || ForcedRawRowDrains.Load() > forcedRawBefore
				if !spilled {
					t.Fatalf("order %v: the aggregate never spilled, so no spill path was tested", order)
				}
				if got := repDrain(t, h, len(c.keyTypes)); got != c.want {
					t.Errorf("order %v (merged runs=%v): got %s, want %s", order, spilled, got, c.want)
				}
				h.Close()
			}
		})
	}
}

// The k-way merger directly: two runs holding one group each, in both
// source orders (source 0 is the in-memory remainder in production).
func TestRepresentativeKWayMergerTie(t *testing.T) {
	mk := func(disp string, tag int64) *partialGroup {
		g := &partialGroup{SortKey: []byte("k"), KeyVals: make([]partialKeyValue, 1), Accs: make([]kernel.Accumulator, 1)}
		setPartialKeyFromAny(&g.KeyVals[0], disp)
		g.Accs[0].SumI64 = tag
		g.Accs[0].Count = 1
		return g
	}
	for _, tc := range []struct {
		a, b    string
		ta, tb  int64
		eligAgg int
		want    string
	}{
		{"7.00", "7", 1, 1, 0, "7"},
		{"7", "7.00", 1, 1, 0, "7"},
		{"7", "7.00", 0, 1, 1, "7.00"},
		{"7.00", "7", 1, 0, 1, "7.00"},
		{"7.00", "7", 0, 0, 1, "7"},
	} {
		m := newKWayMerger([]partialRunSource{
			newMemoryRunSource([]*partialGroup{mk(tc.a, tc.ta)}),
			newMemoryRunSource([]*partialGroup{mk(tc.b, tc.tb)}),
		}, []partialAggSpec{{Func: AggSum}})
		m.keyTypes = []batch.TypeID{batch.TypeDecimal}
		m.eligAgg = tc.eligAgg
		g, err := m.Next()
		if err != nil {
			t.Fatal(err)
		}
		if got := string(g.KeyVals[0].Bytes); got != tc.want {
			t.Errorf("%+v: head %q, want %q", tc, got, tc.want)
		}
		if g.Accs[0].SumI64 != tc.ta+tc.tb {
			t.Errorf("%+v: accumulators merged to %d", tc, g.Accs[0].SumI64)
		}
	}
}

// The per-batch precheck (repCanSkipBatch) may skip a batch only when no row
// of it can be a smaller representative: a batch uniform at its carrier
// scale S still beats a group printed at a WIDER scale from an earlier
// batch. The ordinary column — every batch uniform at one carrier scale — is
// skipped, so its hits pay one compare.
func TestRepresentativeBatchPrecheckNeverSkipsAWinner(t *testing.T) {
	ctx := context.Background()
	carrierBatch := func(scale int, unscaled int64) *batch.RecordBatch {
		schema := []parquet.Column{
			{Name: "k0", Type: parquet.TypeDecimal, Precision: 18, Scale: scale},
			{Name: "tag", Type: parquet.TypeInt64},
		}
		b := batch.NewRecordBatch(schema, 1)
		b.Columns[0].DecimalData.Data[0] = batch.Int128From(unscaled)
		b.Columns[0].Nulls.SetValid(0)
		b.Columns[1].Int64Data[0] = 1
		b.Columns[1].Nulls.SetValid(0)
		return b
	}
	for _, aggs := range []func() []AggColumn{repSum, repCountDistinct} {
		for _, order := range [][2]int{{4, 2}, {2, 4}} {
			h := NewHashAggregate([]string{"k0"}, aggs())
			if err := h.Init(ctx); err != nil {
				t.Fatal(err)
			}
			for _, s := range order {
				unscaled := int64(7)
				for i := 0; i < s; i++ {
					unscaled *= 10
				}
				if err := h.Consume(ctx, carrierBatch(s, unscaled)); err != nil {
					t.Fatal(err)
				}
			}
			if err := h.Finalize(ctx); err != nil {
				t.Fatal(err)
			}
			if got := repDrain(t, h, 1); !strings.HasPrefix(got, "7.00,") {
				t.Errorf("scales %v: got %s, want the scale-2 member 7.00", order, got)
			}
		}
	}
	h := NewHashAggregate([]string{"k0"}, repSum())
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		b := carrierBatch(2, 700)
		if err := h.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
		if h.repSkipFor != b {
			t.Errorf("batch %d: a uniform carrier-scale batch was not skipped", i)
		}
	}
}
