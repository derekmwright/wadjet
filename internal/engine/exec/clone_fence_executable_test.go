package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The clone fence's boundary, as an ARM rather than as a predicate.
//
// The companion file pins `SinkSurvivesCloning` itself, which is necessary
// and not sufficient: a pure predicate assertion fires on ANY change to the
// predicate, safe or unsafe, so it cannot tell a correct narrowing from an
// incorrect one, and every measured wrongness lived in a comment. This file
// RUNS the shapes.
//
// Two directions, and both are required:
//
//   - Through the fence as it stands, each shape answers what the serial
//     run answers, at 1, 4 and 8 workers. That is what the fence buys.
//   - With the fence WIDENED — runParallel entered directly, which is
//     exactly what "narrow SinkSurvivesCloning to !usePartitioned" does for
//     these shapes — the answer is wrong, by the multiplier the deferral
//     records. That is what the fence costs to give up.
//
// The DECIMAL group key is this arc's own finding and the reason #793's
// narrowing is deferred: `HashAggregate.PartitionSelectors` returns nil for
// a key whose vector type it does not hash (DECIMAL, ARRAY, ROW, MAP,
// VECTOR are all outside its set), the pipeline demotes from adoption to
// the ordinary merge-by-key, and that merge ADDS accumulators each clone
// has already folded a shared value into. It had no executable form at all
// until this file.
func TestTheCloneFenceIsLoadBearingOnTheShapesItGuards(t *testing.T) {
	for _, shape := range cloneFenceArmShapes() {
		t.Run(shape.name, func(t *testing.T) {
			want := shape.run(t, 1, false)

			// 1. Through the fence: parallel agrees with serial.
			for _, w := range []int{4, 8} {
				got := shape.run(t, w, false)
				if diff := cloneFenceDiff(want, got); diff != "" {
					t.Fatalf("the FENCED path answered differently at %d workers: %s\n"+
						"  the fence exists so a DISTINCT aggregate is not split; if this "+
						"fires, the fence stopped holding", w, diff)
				}
			}

			// 2. Fence widened: the answer is wrong, by the recorded factor.
			for _, w := range []int{4, 8} {
				got := shape.run(t, w, true)
				diff := cloneFenceDiff(want, got)
				if diff == "" {
					t.Fatalf("with the fence WIDENED at %d workers the answer is still right.\n"+
						"  %s\n"+
						"  Either the state model changed and #793's deferral can be "+
						"revisited — in which case delete this arm and say why — or this "+
						"fixture stopped reaching the merge it is aimed at.", w, shape.why)
				}
				t.Logf("widened at %d workers: %s", w, diff)
			}
		})
	}
}

type cloneFenceShape struct {
	name string
	why  string
	run  func(t *testing.T, workers int, widen bool) map[string]int64
}

func cloneFenceArmShapes() []cloneFenceShape {
	return []cloneFenceShape{
		{
			name: "grouped_decimal_key_sum_distinct",
			why: "a DECIMAL group key is outside PartitionSelectors' hashable set, so every " +
				"batch falls back, the pipeline demotes to merge-by-key, and the merge adds " +
				"accumulators the clones already folded — worker-count x truth, 3/3 replicates",
			run: func(t *testing.T, workers int, widen bool) map[string]int64 {
				return runCloneFenceAgg(t, workers, widen,
					[]parquet.Column{
						{Name: "grp", Type: parquet.TypeDecimal, Precision: 10, Scale: 2},
						{Name: "v", Type: parquet.TypeInt64},
					},
					cloneFenceRows(60000, func(i int) map[string]any {
						return map[string]any{"grp": float64(i % 3), "v": int64(i % 500)}
					}),
					[]string{"grp"})
			},
		},
		{
			name: "ungrouped_sum_distinct",
			why: "each clone folds its own copy of a shared value into its accumulator and " +
				"mergeSinkState ADDS them; unioning the distinct SETS, which the merge " +
				"already does, cannot undo an addition that already happened",
			run: func(t *testing.T, workers int, widen bool) map[string]int64 {
				return runCloneFenceAgg(t, workers, widen,
					[]parquet.Column{{Name: "v", Type: parquet.TypeInt64}},
					cloneFenceRows(60000, func(i int) map[string]any {
						return map[string]any{"v": int64(i % 1000)}
					}),
					nil)
			},
		},
	}
}

func cloneFenceRows(n int, mk func(i int) map[string]any) []map[string]any {
	rows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, mk(i))
	}
	return rows
}

// runCloneFenceAgg runs SUM(DISTINCT v) over the rows. widen=false takes
// Pipeline.Run, which consults the fence; widen=true enters runParallel
// directly, which is what narrowing the fence would do for these shapes.
func runCloneFenceAgg(t *testing.T, workers int, widen bool, schema []parquet.Column,
	rows []map[string]any, groupBy []string) map[string]int64 {
	t.Helper()
	ctx := context.Background()
	agg := NewHashAggregate(groupBy, []AggColumn{
		{Func: AggSum, Distinct: true, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	pipe := &Pipeline{Source: NewSliceSource(schema, rows), Sink: agg, Workers: workers}

	if widen {
		if err := pipe.Source.Init(ctx); err != nil {
			t.Fatalf("source init: %v", err)
		}
		if err := pipe.Sink.Init(ctx); err != nil {
			t.Fatalf("sink init: %v", err)
		}
		if err := pipe.runParallel(ctx); err != nil {
			t.Fatalf("runParallel: %v", err)
		}
	} else if err := pipe.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	defer pipe.Close()

	out := map[string]int64{}
	for {
		b, err := agg.Next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if b == nil {
			break
		}
		for _, r := range b.ToRows() {
			key := "*"
			if len(groupBy) > 0 {
				key = fmt.Sprintf("%v", r[groupBy[0]])
			}
			switch s := r["s"].(type) {
			case int64:
				out[key] += s
			case float64:
				out[key] += int64(s)
			case nil:
			default:
				t.Fatalf("unexpected sum type %T", s)
			}
		}
	}
	if len(out) == 0 {
		t.Fatal("the aggregate produced no groups — this arm would compare nothing")
	}
	return out
}

func cloneFenceDiff(want, got map[string]int64) string {
	if len(want) != len(got) {
		return fmt.Sprintf("%d groups vs %d", len(want), len(got))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			return fmt.Sprintf("group %s missing", k)
		}
		if g != w {
			return fmt.Sprintf("group %s: got %d want %d (ratio %.2f)", k, g, w, float64(g)/float64(w))
		}
	}
	return ""
}
