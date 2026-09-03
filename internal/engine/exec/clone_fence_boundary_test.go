package exec

import "testing"

// The clone fence's BOUNDARY, recorded because #793 proposes to narrow it
// and this file is what a narrowing has to get past.
//
// #793 is real: a grouped `SUM(DISTINCT v)` is fenced out of morsel
// parallelism and pays for it. Measured on a 6,000,000-row fixture with
// 200,000 distinct values, allocs/op flat across worker counts (1,080 at
// w1 and at w8 — a fenced sink does not clone, and that flatness is the
// contention-immune proof of serialization):
//
//	g64 SUM(v)             control   103.2 ms w1 -> 56.5 ms w8   1.83x
//	g64 COUNT(DISTINCT v)  cloned    291.0 ms w1 -> 67.5 ms w8   4.31x
//	g64 SUM(DISTINCT v)    FENCED    310.0 ms w1 -> 273.2 ms w8  1.13x
//
// With the fence removed in a probe, the same grouped shape ran 57.4 ms at
// w8: the fence costs 4.76x there.
//
// But the fence is LOAD-BEARING on three shapes, and each is measured:
//
//  1. Ungrouped DISTINCT, cloned, answers worker-count x truth —
//     deterministically, 3/3 replicates: 499,500 became 1,998,000 at four
//     workers and 3,996,000 at eight. Each clone folds its own copy of a
//     shared value into its accumulator and mergeSinkState ADDS those
//     accumulators; unioning the distinct SETS, which the merge already
//     does, cannot undo an addition that already happened.
//  2. Grouped DISTINCT under WADJET_PARTITIONED_AGG=0, cloned: 64 of 64
//     groups wrong at four and at eight workers. That is the arm the
//     optimization-invariance oracle runs, so the fence is required there
//     whatever the default routing does.
//  3. Grouped DISTINCT whose GROUP BY key cannot be hash-routed. This one
//     is NOT in the filing and it is why the narrowing #793 proposes is
//     deferred rather than shipped. `HashAggregate.PartitionSelectors`
//     returns nil for a key whose vector type is outside its supported set
//     — DECIMAL, ARRAY, ROW, MAP and VECTOR keys all are — and the
//     pipeline's answer to an unroutable batch is to DEMOTE from adoption
//     to the ordinary merge-by-key (#338). For a DISTINCT aggregate that
//     demotion is case 2: the merge adds accumulators. So "clone when
//     partitioned aggregation is eligible" admits `GROUP BY a_decimal_col,
//     SUM(DISTINCT x)` and then answers it wrong, and eligibility is
//     decided BEFORE any batch while routability is a property of the
//     first batch's vector types (HashAggregate.groupColTypes is latched
//     in resolveIndices, from the first batch).
//
// What a shippable narrowing needs, recorded so the next attempt does not
// rediscover it: the aggregate must carry a PLAN-TIME routability fact for
// its group keys (declared types, the same set PartitionSelectors accepts),
// the fence must consult it, and the router's fallback must REFUSE — loudly
// — rather than demote when the primary carries a DISTINCT aggregate, so a
// declaration that disagrees with the runtime type fails the query instead
// of answering it wrongly. AVG(DISTINCT) staying correct in every measured
// arm is NOT evidence of safety: sum and count scale by the same factor
// under a uniform split.
func TestTheCloneFenceStaysClosedOnTheShapesThatNeedIt(t *testing.T) {
	cases := []struct {
		name string
		aggs []AggColumn
		grp  []string
		why  string
	}{
		{
			"ungrouped_sum_distinct",
			[]AggColumn{{Func: AggSum, Distinct: true, InputCol: "v"}},
			nil,
			"cloned, it answers worker-count x truth (499,500 -> 1,998,000 at w4)",
		},
		{
			"grouped_sum_distinct",
			[]AggColumn{{Func: AggSum, Distinct: true, InputCol: "v"}},
			[]string{"g"},
			"cloned under WADJET_PARTITIONED_AGG=0 it is 64/64 wrong, and under the " +
				"default routing an unroutable group key demotes to the same merge",
		},
		{
			"grouped_avg_distinct",
			[]AggColumn{{Func: AggAvg, Distinct: true, InputCol: "v"}},
			[]string{"g"},
			"AVG(DISTINCT) survived every measured arm only because sum and count " +
				"scale by the same factor under a uniform split; that is not safety",
		},
		{
			"grouped_min_distinct",
			[]AggColumn{{Func: AggMin, Distinct: true, InputCol: "v"}},
			[]string{"g"},
			"MIN/MAX(DISTINCT) are idempotent under merge and could be exempted, but " +
				"only with a fixture on every arm; none exists yet",
		},
		{
			"mixed_distinct_and_plain",
			[]AggColumn{
				{Func: AggCount, InputCol: ""},
				{Func: AggSum, Distinct: true, InputCol: "v"},
			},
			[]string{"g"},
			"one DISTINCT aggregate anywhere in the list fences the whole sink",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &HashAggregate{Aggs: tc.aggs, GroupByCols: tc.grp}
			if SinkSurvivesCloning(h) {
				t.Fatalf("the clone fence opened for %s.\n  It must stay closed: %s\n"+
					"  Widening it requires the arms named at the top of this file to "+
					"answer correctly, replicated, on single / PARTITIONED_AGG=0 / "+
					"spilled / DAG-morsel arms (#793).", tc.name, tc.why)
			}
		})
	}
}

// TestTheCloneFenceStaysOpenWhereItAlwaysWas is the mirror: the fence must
// not creep wider either. COUNT(DISTINCT) and APPROX_DISTINCT keep their
// parallelism because their whole state IS the set, so the union is the
// merge; every non-distinct aggregate is unaffected.
func TestTheCloneFenceStaysOpenWhereItAlwaysWas(t *testing.T) {
	cases := []struct {
		name string
		aggs []AggColumn
	}{
		{"plain_sum", []AggColumn{{Func: AggSum, InputCol: "v"}}},
		{"count_star", []AggColumn{{Func: AggCount, InputCol: ""}}},
		{"count_distinct", []AggColumn{{Func: AggCountDistinct, InputCol: "v"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &HashAggregate{Aggs: tc.aggs, GroupByCols: []string{"g"}}
			if !SinkSurvivesCloning(h) {
				t.Fatalf("the clone fence closed on %s, which has always been "+
					"clone-safe; this is a silent loss of morsel parallelism", tc.name)
			}
		})
	}
}
