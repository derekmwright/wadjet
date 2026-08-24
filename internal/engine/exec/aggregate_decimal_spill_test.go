package exec

import (
	"math/big"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func decSpillInt128(n *big.Int) batch.Int128 {
	m := new(big.Int).Set(n)
	if m.Sign() < 0 {
		m.Add(m, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	var b [16]byte
	m.FillBytes(b[:])
	hi := new(big.Int).Rsh(new(big.Int).SetBytes(b[:]), 64).Uint64()
	lo := new(big.Int).And(new(big.Int).SetBytes(b[:]), new(big.Int).SetUint64(^uint64(0))).Uint64()
	return batch.Int128{Hi: int64(hi), Lo: lo}
}

func decSpillBatch(t *testing.T, keys []int64, vals []*big.Int, scale int) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: scale, Nullable: true},
	}
	b := batch.NewRecordBatch(schema, len(keys))
	for i := range keys {
		b.Columns[0].Int64Data[i] = keys[i]
		b.Columns[0].Nulls.SetValid(i)
		b.Columns[1].DecimalData.Data[i] = decSpillInt128(vals[i])
		b.Columns[1].Nulls.SetValid(i)
	}
	b.Len = len(keys)
	return b
}

// A DECIMAL aggregate must answer the same value whether or not it spilled.
//
// The spill format carries the accumulator, not the finalized cell, so a
// DECIMAL rides it as an Int128 plus a scale plus (since #455) an overflow
// bit — three fields a k-way merge has to put back together. The reference is
// big.Int over the generator, so a spilling run and a non-spilling run that
// were wrong TOGETHER still fail. The values are 23 digits wide: no float64
// holds one, so any path that boxed through a double shows up here.
func TestDecimalAggregatesSurviveSpill(t *testing.T) {
	const numBatches, rowsPerBatch, numGroups = 20, 100, 150
	step, _ := new(big.Int).SetString("97777777788777775778877", 10)
	var batches []*batch.RecordBatch
	for bi := 0; bi < numBatches; bi++ {
		keys := make([]int64, rowsPerBatch)
		vals := make([]*big.Int, rowsPerBatch)
		for ri := 0; ri < rowsPerBatch; ri++ {
			idx := bi*rowsPerBatch + ri
			keys[ri] = int64(idx % numGroups)
			vals[ri] = new(big.Int).Mul(step, big.NewInt(int64(idx-500)))
		}
		batches = append(batches, decSpillBatch(t, keys, vals, 10))
	}
	mk := func(sm *memory.SpillManager) *HashAggregate {
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggMin, InputCol: "d", OutputCol: "lo", OutputType: parquet.TypeDecimal},
			{Func: AggMax, InputCol: "d", OutputCol: "hi", OutputType: parquet.TypeDecimal},
			{Func: AggSum, InputCol: "d", OutputCol: "s", OutputType: parquet.TypeDecimal},
			{Func: AggAvg, InputCol: "d", OutputCol: "a", OutputType: parquet.TypeDecimal},
		})
		h.Spill = sm
		return h
	}
	ref := runHashAggToMap(t, mk(nil), batches)
	tracker := memory.NewTracker("test", 1_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	tracker.ForceReserve(900)
	hs := mk(sm)
	got := runHashAggToMapSpillChecked(t, hs, batches)

	if len(ref) != numGroups || len(got) != numGroups {
		t.Fatalf("groups: ref=%d spilled=%d want %d", len(ref), len(got), numGroups)
	}
	idx := func(rows []map[string]any) map[int64]map[string]any {
		m := make(map[int64]map[string]any, len(rows))
		for _, r := range rows {
			m[r["k"].(int64)] = r
		}
		return m
	}
	r0, g0 := idx(ref), idx(got)
	// big.Int reference, independent of both runs.
	for g := int64(0); g < numGroups; g++ {
		var mn, mx, sum *big.Int
		var n int64
		sum = big.NewInt(0)
		for bi := 0; bi < numBatches; bi++ {
			for ri := 0; ri < rowsPerBatch; ri++ {
				i := bi*rowsPerBatch + ri
				if int64(i%numGroups) != g {
					continue
				}
				v := new(big.Int).Mul(step, big.NewInt(int64(i-500)))
				if n == 0 {
					mn, mx = new(big.Int).Set(v), new(big.Int).Set(v)
				} else {
					if v.Cmp(mn) < 0 {
						mn = new(big.Int).Set(v)
					}
					if v.Cmp(mx) > 0 {
						mx = new(big.Int).Set(v)
					}
				}
				sum.Add(sum, v)
				n++
			}
		}
		want := map[string]string{
			"lo": decSpillText(mn, 10), "hi": decSpillText(mx, 10), "s": decSpillText(sum, 10),
		}
		for _, arm := range []struct {
			name string
			m    map[int64]map[string]any
		}{{"nospill", r0}, {"spilled", g0}} {
			row := arm.m[g]
			if row == nil {
				t.Fatalf("%s: group %d missing", arm.name, g)
			}
			for col, w := range want {
				if s, ok := row[col].(string); !ok || s != w {
					t.Errorf("%s group %d %s = %#v, want %q", arm.name, g, col, row[col], w)
				}
			}
			if row["a"] != r0[g]["a"] {
				t.Errorf("%s group %d AVG = %#v, non-spilling run says %#v", arm.name, g, row["a"], r0[g]["a"])
			}
		}
	}
}

// runHashAggToMapSpillChecked drains h and FAILS if the external-merge path
// was never entered — a spill test that silently ran in memory proves nothing.
func runHashAggToMapSpillChecked(t *testing.T, h *HashAggregate, batches []*batch.RecordBatch) []map[string]any {
	t.Helper()
	if err := h.Init(t.Context()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	for _, b := range batches {
		if err := h.Consume(t.Context(), b); err != nil {
			t.Fatalf("Consume: %v", err)
		}
	}
	if err := h.Finalize(t.Context()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if h.partialMerger == nil {
		t.Fatal("the external-merge path was never entered: this run never spilled")
	}
	var rows []map[string]any
	for {
		out, err := h.Next(t.Context())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if out == nil {
			break
		}
		rows = append(rows, out.ToRows()...)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return rows
}

func decSpillText(u *big.Int, scale int) string {
	neg := u.Sign() < 0
	d := new(big.Int).Abs(u).String()
	if len(d) <= scale {
		d = strings.Repeat("0", scale-len(d)+1) + d
	}
	out := d[:len(d)-scale] + "." + d[len(d)-scale:]
	if neg {
		out = "-" + out
	}
	return out
}

// The overflow bit must survive the spill round trip: a group whose sum
// wrapped before it spilled must still fail the query after the k-way merge.
// Without the flag in the spill record the merged accumulator comes back
// looking ordinary, and the query answers the wrapped total.
func TestDecimalSumOverflowSurvivesSpill(t *testing.T) {
	nine37, _ := new(big.Int).SetString("90000000000000000000000000000000000000", 10)
	var batches []*batch.RecordBatch
	for bi := 0; bi < 6; bi++ {
		keys := make([]int64, 40)
		vals := make([]*big.Int, 40)
		for ri := range keys {
			keys[ri] = int64(ri)
			vals[ri] = nine37
		}
		batches = append(batches, decSpillBatch(t, keys, vals, 0))
	}
	mk := func(sm *memory.SpillManager) *HashAggregate {
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggSum, InputCol: "d", OutputCol: "s", OutputType: parquet.TypeDecimal},
		})
		h.Spill = sm
		return h
	}
	tracker := memory.NewTracker("test", 1_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	tracker.ForceReserve(900)
	h := mk(sm)
	if err := h.Init(t.Context()); err != nil {
		t.Fatal(err)
	}
	var consumeErr error
	for _, b := range batches {
		if err := h.Consume(t.Context(), b); err != nil {
			consumeErr = err
			break
		}
	}
	if consumeErr == nil {
		if err := h.Finalize(t.Context()); err != nil {
			consumeErr = err
		}
	}
	if consumeErr == nil {
		for {
			out, err := h.Next(t.Context())
			if err != nil {
				consumeErr = err
				break
			}
			if out == nil {
				break
			}
		}
	}
	h.Close()
	if consumeErr == nil {
		t.Fatal("a spilled DECIMAL SUM that overflowed answered instead of failing")
	}
	if !strings.Contains(consumeErr.Error(), "overflow") {
		t.Fatalf("error does not name the overflow: %v", consumeErr)
	}
}
