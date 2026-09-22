// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A RIGHT SEMI / RIGHT ANTI join on TWO integer keys marks the build rows it
// matched. The two-integer key builds its own index (useDualIntKey, no string
// table), and the probe arm these joins use had no case for it, so it fell to
// the string lookup, found no table and marked nothing: RIGHT SEMI answered
// zero rows and RIGHT ANTI every build row (arc DC round 3, review N6 — a
// correlated IN whose IN key and correlation key are two pairs plans exactly
// this). Both key shapes that reach it are rows: two distinct columns, and one
// column named twice (`j = id AND j = id`).
func TestARightSemiOrAntiJoinOnTwoIntegerKeysMarksWhatItMatched(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeInt64},
	}
	buildRows := []map[string]any{
		{"a": int64(1), "b": int64(10)},
		{"a": int64(2), "b": int64(20)},
		{"a": int64(9), "b": int64(90)},
		{"a": nil, "b": int64(0)},
	}
	probeSchema := []parquet.Column{
		{Name: "x", Type: parquet.TypeInt64},
		{Name: "y", Type: parquet.TypeInt64},
	}
	probeRows := []map[string]any{
		{"x": int64(1), "y": int64(10)},
		{"x": int64(2), "y": int64(99)}, // first key matches, second does not
		{"x": int64(9), "y": int64(90)},
		{"x": int64(9), "y": int64(90)}, // a duplicate probe row marks nothing new
	}
	cases := []struct {
		name       string
		kind       JoinType
		left, rigt []string
		want       []int64 // build column a, sorted; -1 stands for NULL
	}{
		{"semi/twoColumns", RightSemiJoin, []string{"x", "y"}, []string{"a", "b"}, []int64{1, 9}},
		{"anti/twoColumns", RightAntiJoin, []string{"x", "y"}, []string{"a", "b"}, []int64{-1, 2}},
		{"semi/oneColumnTwice", RightSemiJoin, []string{"x", "x"}, []string{"a", "a"}, []int64{1, 2, 9}},
		{"anti/oneColumnTwice", RightAntiJoin, []string{"x", "x"}, []string{"a", "a"}, []int64{-1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hj := NewHashJoin(tc.kind, tc.left, tc.rigt)
			buildWithArenaMatched(hj, buildSchema, buildRows)
			if !hj.useDualIntKey {
				t.Fatalf("the fixture must take the two-integer key path (useIntKey=%v)", hj.useIntKey)
			}
			probe := hj.Probe()
			if _, err := probe.Execute(context.Background(), batch.FromRows(probeSchema, probeRows)); err != nil {
				t.Fatalf("probe: %v", err)
			}
			var out *batch.RecordBatch
			if tc.kind == RightSemiJoin {
				out = probe.FlushMatched()
			} else {
				out = probe.FlushAntiMatched()
			}
			var got []int64
			if out != nil {
				for _, r := range out.ToRows() {
					if v, ok := r["a"].(int64); ok {
						got = append(got, v)
					} else {
						got = append(got, -1)
					}
				}
			}
			sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
			if len(got) != len(tc.want) {
				t.Fatalf("got build rows %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got build rows %v, want %v", got, tc.want)
				}
			}
		})
	}
}
