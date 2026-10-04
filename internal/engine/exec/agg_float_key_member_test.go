// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Arc FO (#1489): a GROUP BY key is published as a MEMBER's own value. A float
// key's identity is canonical (kernel.KeyFloat64Bits folds -0 onto 0 and every
// NaN payload onto one NaN, so -0 and 0 are one group as PostgreSQL says), and
// that fold is lossy — so the deferred-boxing path, which rebuilds the
// published value from the key bytes, published 0 for a group whose only
// member is -0, where PostgreSQL publishes -0.

// TestDeferredKeyBoxingBoxesEveryLossyMember is the invariant the deferred
// path now keeps: every value of a deferrable type either round-trips through
// its binary key bit for bit, or floatKeyNotCanonical flags it so its group
// boxes the member at creation — and no round-tripping value is flagged, so
// the common row pays no box.
func TestDeferredKeyBoxingBoxesEveryLossyMember(t *testing.T) {
	negNaN := math.Float64frombits(math.Float64bits(math.NaN()) | 1<<63)
	cases := []struct {
		typ  batch.TypeID
		vals []any
	}{
		{batch.TypeFloat64, []any{math.Copysign(0, -1), 0.0, 1.5, -1.5, math.NaN(), negNaN, math.Float64frombits(0x7ff8000000000000), math.Inf(1), math.Inf(-1)}},
		{batch.TypeFloat32, []any{float32(math.Copysign(0, -1)), float32(0), float32(1.5), float32(math.NaN()), float32(negNaN), float32(math.Inf(-1))}},
		{batch.TypeInt64, []any{int64(-1), int64(0), int64(math.MaxInt64)}},
		{batch.TypeInt32, []any{int32(-1), int32(0)}},
		{batch.TypeBool, []any{true, false}},
		{batch.TypeString, []any{"", "a", "A"}},
	}
	lossy := 0
	for _, c := range cases {
		if !genericKeyBoxingDeferrable(c.typ) {
			t.Fatalf("%s is no longer deferrable: this gate compares nothing for it", c.typ)
		}
		col := parquet.Column{Name: "k", Type: c.typ}
		for _, v := range c.vals {
			rb := batch.FromRows([]parquet.Column{col}, []map[string]any{{"k": v}})
			vec := rb.Columns[0]
			key := appendColumnValue([]byte{0}, vec, 0, c.typ)
			got := decodeSerializedKey(string(key), []batch.TypeID{c.typ})[0]
			roundTrips := sameBits(v, got)
			flagged := floatKeyNotCanonical(buildKeySerCols(nil, rb, []int{0}, []batch.TypeID{c.typ}), 0)
			switch {
			case !roundTrips && !flagged:
				t.Errorf("%s %v: the key rebuilds %v and the group does not box its member — it publishes the canonical stand-in", c.typ, v, got)
			case roundTrips && flagged:
				t.Errorf("%s %v: round-trips yet is flagged — the common path pays a box for nothing", c.typ, v)
			}
			if !roundTrips {
				lossy++
			}
		}
	}
	if lossy < 4 {
		t.Fatalf("%d lossy values sampled: the gate must include -0 and non-canonical NaNs", lossy)
	}
}

func sameBits(a, b any) bool {
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)
		return ok && math.Float64bits(x) == math.Float64bits(y)
	case float32:
		y, ok := b.(float32)
		return ok && math.Float32bits(x) == math.Float32bits(y)
	}
	return a == b
}

// TestFloatGroupKeyPublishesItsMember runs a float GROUP BY through the three
// places a group's key is carried — the in-memory table, the morsel clones'
// MergeSink, and the spilled partial-state runs — and asserts the published
// key: a group of -0 alone publishes -0, a group of 0 alone publishes 0, and
// -0 and 0 are ONE group.
func TestFloatGroupKeyPublishesItsMember(t *testing.T) {
	negZero := math.Copysign(0, -1)
	for _, typ := range []parquet.TypeID{parquet.TypeFloat64, parquet.TypeFloat32} {
		box := func(f float64) any {
			if typ == parquet.TypeFloat32 {
				return float32(f)
			}
			return f
		}
		signOf := func(v any) (zero, neg bool) {
			switch x := v.(type) {
			case float64:
				return x == 0, math.Signbit(x)
			case float32:
				return x == 0, math.Signbit(float64(x))
			}
			return false, false
		}
		schema := []parquet.Column{
			{Name: "k", Type: typ, Nullable: true},
			{Name: "g", Type: parquet.TypeInt64},
			{Name: "v", Type: parquet.TypeInt64},
		}
		// g = 1: only -0; g = 2: only 0; g = 3: both (one group, either sign).
		mk := func(n int) []map[string]any {
			rows := make([]map[string]any, 0, 3*n)
			for i := 0; i < n; i++ {
				rows = append(rows,
					map[string]any{"k": box(negZero), "g": int64(1), "v": int64(1)},
					map[string]any{"k": box(0), "g": int64(2), "v": int64(1)},
					map[string]any{"k": box(float64(i % 2)), "g": int64(3), "v": int64(1)},
				)
				if i%2 == 1 {
					rows[len(rows)-1]["k"] = box(negZero)
				}
			}
			return rows
		}
		check := func(t *testing.T, rows []map[string]any) {
			t.Helper()
			seen := map[int64]bool{}
			for _, r := range rows {
				g := r["g"].(int64)
				if seen[g] {
					t.Fatalf("group (k=%v, g=%d) emitted twice — -0 and 0 split into two groups: %v", r["k"], g, rows)
				}
				seen[g] = true
				zero, neg := signOf(r["k"])
				if !zero {
					t.Fatalf("group g=%d published key %v, want a zero", g, r["k"])
				}
				switch {
				case g == 1 && !neg:
					t.Errorf("%s: the group whose only member is -0 published 0 (PostgreSQL: -0)", typ)
				case g == 2 && neg:
					t.Errorf("%s: the group whose only member is 0 published -0", typ)
				}
			}
			if len(seen) != 3 {
				t.Fatalf("%d groups, want 3: %v", len(seen), rows)
			}
		}

		t.Run(typ.String()+"/memory", func(t *testing.T) {
			h := NewHashAggregate([]string{"k", "g"}, []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64}})
			check(t, runHashAggToMap(t, h, []*batch.RecordBatch{batch.FromRows(schema, mk(4))}))
		})
		t.Run(typ.String()+"/singleKey", func(t *testing.T) {
			h := NewHashAggregate([]string{"k"}, []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64}})
			rows := runHashAggToMap(t, h, []*batch.RecordBatch{batch.FromRows(schema, []map[string]any{{"k": box(negZero), "g": int64(1), "v": int64(1)}, {"k": box(1.5), "g": int64(1), "v": int64(1)}, {"k": box(negZero), "g": int64(1), "v": int64(1)}})})
			for _, r := range rows {
				if zero, neg := signOf(r["k"]); zero && !neg {
					t.Errorf("%s: GROUP BY k over {-0, 1.5, -0} published 0 (PostgreSQL: -0)", typ)
				}
			}
		})
		t.Run(typ.String()+"/mergeSink", func(t *testing.T) {
			ctx := context.Background()
			primary := NewHashAggregate([]string{"k", "g"}, []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64}})
			if err := primary.Init(ctx); err != nil {
				t.Fatal(err)
			}
			clone := primary.CloneSink().(*HashAggregate)
			if err := clone.Init(ctx); err != nil {
				t.Fatal(err)
			}
			if err := clone.Consume(ctx, batch.FromRows(schema, mk(3))); err != nil {
				t.Fatal(err)
			}
			if err := primary.Consume(ctx, batch.FromRows(schema, mk(2))); err != nil {
				t.Fatal(err)
			}
			primary.MergeSink(clone)
			check(t, aggRows(t, primary))
		})
		t.Run(typ.String()+"/spilled", func(t *testing.T) {
			tracker := memory.NewTracker("test", 1_000)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			tracker.ForceReserve(900)
			h := &HashAggregate{
				GroupByCols: []string{"k", "g"},
				Aggs:        []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64}},
				Spill:       sm,
			}
			var batches []*batch.RecordBatch
			for i := 0; i < 8; i++ {
				batches = append(batches, batch.FromRows(schema, mk(50)))
			}
			check(t, runHashAggToMap(t, h, batches))
		})
	}
}
