// SPDX-License-Identifier: MIT

package physical

import (
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// ARC TF round 2 / B1 — THE SERIES ENDS AT THE CARRIER'S EDGE.
//
// `s.cur += s.step` WRAPS at int64's boundary, and a wrapped counter sits on
// the other side of the bound, so neither of the loop's exits is reached: the
// source emitted 2048-row batches forever, each row after the wrap a value the
// series does not contain, and the embedded query was OOM-killed at 43 s where
// PostgreSQL 17.11 answers three rows (measured by the round-1 review).
//
// Every cell here reads the source DIRECTLY and every cell is BOUNDED: a
// failure of this property is an infinite loop, so the test must not be the
// thing that hangs. Each drains through a deadline and fails loudly if the
// series has not ended.
func TestArcTFTheSeriesEndsAtTheCarriersEdge(t *testing.T) {
	const (
		i64max = int64(9223372036854775807)
		i64min = int64(-9223372036854775808)
		i32max = int64(2147483647)
		i32min = int64(-2147483648)
	)
	for _, c := range []struct {
		name  string
		args  []string
		want  []int64
		pgSQL string
	}{
		{name: "the_top_edge_ascending",
			args: []string{"9223372036854775805", "9223372036854775807"},
			want: []int64{i64max - 2, i64max - 1, i64max},
			pgSQL: "generate_series(9223372036854775805,9223372036854775807) " +
				"is three rows on 17.11"},
		{name: "the_top_edge_with_a_step_that_overshoots",
			args: []string{"9223372036854775805", "9223372036854775807", "2"},
			want: []int64{i64max - 2, i64max}},
		{name: "a_single_row_at_the_top_edge",
			args: []string{"9223372036854775807", "9223372036854775807"},
			want: []int64{i64max}},
		{name: "the_bottom_edge_descending",
			args: []string{"-9223372036854775806", "-9223372036854775808", "-1"},
			want: []int64{i64min + 2, i64min + 1, i64min}},
		{name: "the_bottom_edge_with_a_step_that_overshoots",
			args: []string{"-9223372036854775806", "-9223372036854775808", "-2"},
			want: []int64{i64min + 2, i64min}},
		{name: "a_single_row_at_the_bottom_edge",
			args: []string{"-9223372036854775808", "-9223372036854775808"},
			want: []int64{i64min}},
		// int4's edge is NOT the carrier's: the declaration is int8 the moment
		// an argument leaves int4, and the series simply continues.
		{name: "int4s_top_edge_is_not_the_end",
			args: []string{"2147483646", "2147483649"},
			want: []int64{i32max - 1, i32max, i32max + 1, i32max + 2}},
		{name: "int4s_bottom_edge_is_not_the_end",
			args: []string{"-2147483647", "-2147483650", "-1"},
			want: []int64{i32min + 1, i32min, i32min - 1, i32min - 2}},
		// The ordinary interior, so the repair cannot have ended a series early.
		{name: "an_ordinary_interior_series",
			args: []string{"1", "5"},
			want: []int64{1, 2, 3, 4, 5}},
		{name: "an_ordinary_descending_series",
			args: []string{"5", "1", "-2"},
			want: []int64{5, 3, 1}},
	} {
		t.Run(c.name, func(t *testing.T) {
			src, err := buildTableFunctionSource("generate_series", c.args, nil)
			if err != nil {
				t.Fatalf("building source: %v", err)
			}
			if err := src.Init(t.Context()); err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			var got []int64
			deadline := time.Now().Add(5 * time.Second)
			for {
				if time.Now().After(deadline) {
					t.Fatalf("the series did not END: %d rows drained in 5s and still "+
						"producing. %s", len(got), c.pgSQL)
				}
				b, err := src.Next(t.Context())
				if err != nil {
					t.Fatalf("next: %v", err)
				}
				if b == nil {
					break
				}
				got = append(got, seriesValues(b)...)
				if len(got) > 100000 {
					t.Fatalf("the series did not END: %d rows and still producing "+
						"(first %d, last %d). %s",
						len(got), got[0], got[len(got)-1], c.pgSQL)
				}
			}
			if len(got) != len(c.want) {
				t.Fatalf("%d rows, want %d\n  got  %v\n  want %v", len(got), len(c.want), got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("row %d is %d, want %d\n  got  %v\n  want %v",
						i, got[i], c.want[i], got, c.want)
				}
			}
		})
	}
}

func seriesValues(b *batch.RecordBatch) []int64 {
	out := make([]int64, 0, b.Len)
	for i := 0; i < b.Len; i++ {
		out = append(out, seriesAt(b, i))
	}
	return out
}
