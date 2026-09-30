// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The spilled arm of the window-argument gates (#1394 #1399): the window-argument cells over the two
// evaluators a window takes past its memory budget — the partition-at-a-time
// walker over sorted runs (PARTITION BY) and the empty-PARTITION-BY streamer
// (window_global.go), each of which read LAG / LEAD's offset on its own. A
// partition here is larger than the budget: the harness fails if no run file
// was written.
//
// Every expectation is PostgreSQL 17.11's rule, computed from the fixture:
// `LAG(v, 0)` / `LEAD(v, 0)` are the current row, `LAG(v, -k)` is
// `LEAD(v, k)` and `LEAD(v, -k)` is `LAG(v, k)`. At v0.25.2 every evaluator
// read an offset <= 0 as 1, so the offset-0 cells answered the neighbour and
// the negative ones the wrong neighbour.
//
// `c` is the constant column a LITERAL argument is materialized into
// (physical.resolveWindowKeys, #1394): the spilled evaluators must answer
// over it as over any column. Those cells are controls at this level — the
// literal defect was that nothing materialized the column — and the literal
// itself is gated end to end by coordinator.TestArcWAWindowArgumentsEveryArm.

func waSpillSchema() []parquet.Column {
	return []parquet.Column{
		{Name: "grp", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c", Type: parquet.TypeInt64},
	}
}

func waSpillRows(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		r := map[string]any{"grp": int64(i % 3), "ts": int64(i), "c": int64(2)}
		if i%7 != 6 {
			r["v"] = int64(i * 10)
		}
		rows[i] = r
	}
	return rows
}

// waRunSpilled feeds rows through a spilling Window and returns its output
// keyed by ts.
func waRunSpilled(t *testing.T, cols []WindowColumn, rows []map[string]any) map[int64]map[string]any {
	t.Helper()
	forceTinyRuns(t)
	ctx := context.Background()
	w := newWindowSpillHarness(t, cols, 512)
	for i := 0; i < len(rows); i += 16 {
		end := min(i+16, len(rows))
		if err := w.Consume(ctx, batch.FromRows(waSpillSchema(), rows[i:end])); err != nil {
			t.Fatal(err)
		}
	}
	if len(w.runFiles) == 0 {
		t.Fatal("window run-spill path was never exercised; budget/floor setup is wrong")
	}
	if err := w.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	out := map[int64]map[string]any{}
	for _, r := range drainWindowRows(t, w) {
		out[r["ts"].(int64)] = r
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if len(out) != len(rows) {
		t.Fatalf("%d rows out, want %d", len(out), len(rows))
	}
	return out
}

// waShift is the value `offset` rows after row ts within its partition (a
// negative offset reads backwards), nil past the partition's edge. members
// is the partition's ts list in order.
func waShift(rows []map[string]any, members []int64, ts int64, offset int) any {
	for i, m := range members {
		if m == ts {
			j := i + offset
			if j < 0 || j >= len(members) {
				return nil
			}
			return rows[members[j]]["v"]
		}
	}
	return nil
}

func waCheck(t *testing.T, got map[int64]map[string]any, col string, want func(ts int64) any) {
	t.Helper()
	bad := 0
	for ts := int64(0); ts < int64(len(got)); ts++ {
		g, w := got[ts][col], want(ts)
		if fmt.Sprint(g) != fmt.Sprint(w) {
			if bad < 3 {
				t.Errorf("%s at ts=%d: got %v, want %v", col, ts, g, w)
			}
			bad++
		}
	}
	if bad > 0 {
		t.Errorf("%s: %d of %d rows differ", col, bad, len(got))
	}
}

func TestArcWAWindowArgumentsOnTheSpilledPaths(t *testing.T) {
	const n = 240
	rows := waSpillRows(n)
	i64 := parquet.TypeInt64
	byTS := []SortKey{{Column: "ts", Order: Ascending}}

	t.Run("partitioned_runs", func(t *testing.T) {
		part := []string{"grp"}
		got := waRunSpilled(t, []WindowColumn{
			{Func: WinLag, InputCol: "v", OutputCol: "lag0", OutputType: i64, PartitionBy: part, OrderBy: byTS, LagLeadOffset: 0},
			{Func: WinLead, InputCol: "v", OutputCol: "lead0", OutputType: i64, PartitionBy: part, OrderBy: byTS, LagLeadOffset: 0},
			{Func: WinLag, InputCol: "v", OutputCol: "lag_neg1", OutputType: i64, PartitionBy: part, OrderBy: byTS, LagLeadOffset: -1},
			{Func: WinLead, InputCol: "v", OutputCol: "lead_neg2", OutputType: i64, PartitionBy: part, OrderBy: byTS, LagLeadOffset: -2},
			{Func: WinLag, InputCol: "v", OutputCol: "lag2", OutputType: i64, PartitionBy: part, OrderBy: byTS, LagLeadOffset: 2},
			{Func: WinFirstValue, InputCol: "c", OutputCol: "first_c", OutputType: i64, PartitionBy: part, OrderBy: byTS},
			{Func: WinLag, InputCol: "c", OutputCol: "lag_c", OutputType: i64, PartitionBy: part, OrderBy: byTS, LagLeadOffset: 1},
		}, rows)
		members := map[int64][]int64{}
		for ts := int64(0); ts < n; ts++ {
			members[ts%3] = append(members[ts%3], ts)
		}
		shift := func(off int) func(int64) any {
			return func(ts int64) any { return waShift(rows, members[ts%3], ts, off) }
		}
		waCheck(t, got, "lag0", shift(0))
		waCheck(t, got, "lead0", shift(0))
		waCheck(t, got, "lag_neg1", shift(1))
		waCheck(t, got, "lead_neg2", shift(-2))
		waCheck(t, got, "lag2", shift(-2))
		waCheck(t, got, "first_c", func(int64) any { return int64(2) })
		waCheck(t, got, "lag_c", func(ts int64) any {
			if ts < 3 {
				return nil
			}
			return int64(2)
		})
	})

	// The streamer answers only the LAST spec group, so each shape it must
	// answer is its own run (see TestWindowGlobalMinMaxEveryType).
	all := make([]int64, n)
	for i := range all {
		all[i] = int64(i)
	}
	global := func(off int) func(int64) any {
		return func(ts int64) any { return waShift(rows, all, ts, off) }
	}
	for _, c := range []struct {
		name string
		fn   WindowFunc
		off  int
		want func(int64) any
	}{
		{"lag0", WinLag, 0, global(0)},
		{"lead0", WinLead, 0, global(0)},
		{"lag_neg1", WinLag, -1, global(1)},
		{"lead_neg2", WinLead, -2, global(-2)},
		{"lag2", WinLag, 2, global(-2)},
		{"lead3", WinLead, 3, global(3)},
	} {
		t.Run("global_stream/"+c.name, func(t *testing.T) {
			got := waRunSpilled(t, []WindowColumn{
				{Func: c.fn, InputCol: "v", OutputCol: c.name, OutputType: i64, OrderBy: byTS, LagLeadOffset: c.off},
			}, rows)
			waCheck(t, got, c.name, c.want)
		})
	}
	t.Run("global_stream/sum_c", func(t *testing.T) {
		got := waRunSpilled(t, []WindowColumn{
			{Func: WinSum, InputCol: "c", OutputCol: "sum_c", OutputType: parquet.TypeDecimal},
		}, rows)
		waCheck(t, got, "sum_c", func(int64) any { return "480" })
	})
}
