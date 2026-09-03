package wadjet

import (
	"context"
	"fmt"
	"runtime"
	"testing"
)

// #847, PINNED WRONG. A GROUP BY whose key is an EXPRESSION, over a join whose
// ON is also an expression, answers with rows attributed to the wrong group.
//
// It is PRE-EXISTING — it reproduces at fd679ae9 with no memory budget and no
// spill anywhere — and it is pinned here rather than fixed because the site is
// outside this arc's territory. What makes it this arc's business is that
// #832's fix CHANGED ITS REACHABILITY: under a budget the shape used to panic
// on a nil build batch, and now it answers. A panic that becomes a plausible
// wrong number must not land unpinned.
//
// WHAT IS RULED OUT, so the next arc does not repeat the search:
//
//   - The JOIN is right. `SELECT a.id, a.g FROM <the cross join>` is
//     deterministic over 20 runs and its 196 pairs are exactly the equi-join's.
//   - The PROJECTION is right. `SELECT a.id AS i, a.id % 7 AS m FROM <the cross
//     join>` has zero rows where `m != i % 7`, over both arms.
//   - The AGGREGATE is right, and so is its partition ROUTER. Instrumented at
//     the sink: each of the seven partition sinks receives exactly ONE distinct
//     key (disjoint, as partitioned aggregation requires) and counts precisely
//     the rows it is handed. The counts are already wrong when they arrive —
//     sink(key 0) is handed 58 rows where 28 belong to it.
//   - The equivalent query with the expression MATERIALIZED FIRST is right:
//     `SELECT k, COUNT(*) FROM (SELECT a.id % 7 AS k FROM <cross>) t GROUP BY k`
//     answers 28 per group, 20 runs of 20.
//
// WHERE IT IS. `WADJET_PARTITIONED_AGG=0` makes the shape right and
// `WADJET_HASH_ONCE=0` does not, which names the arm. Inside it, the group
// key is computed by `aggPreProject.Execute`
// (`internal/planner/physical/plan.go:11347`), which for a numeric computed
// column takes the `canPassSelThrough` path: it KEEPS the input's selection
// vector and writes the computed values at SPARSE physical indices, into
// vectors it REUSES across Execute calls when `shareOutputs` is false
// (`plan.go:11410`, the `in.Len > a.computedCap` branch). Measured on the
// failing batch: the key vector holds exactly the 138 non-zero values the 161
// active rows should produce, but only 108 of them sit at positions the batch's
// selection vector names — so 30 active rows read a slot this batch never wrote
// and key as 0. The partitioned aggregate then routes and counts them
// faithfully into the wrong group.
//
// So the fix is a decision about sparse writes into reused buffers under a
// selection vector, in a hot operator whose comments record a measured perf
// trade for exactly that choice (0.65s vs 0.07s at SF1). That is the planner's
// territory and its own arc, not a rider here.
//
// THE PIN BOUNDS THE CONDITION rather than tolerating a split (ADR-0013,
// ADR-0027 decision 6). At the default GOMAXPROCS the shape is wrong on 14 to
// 17 runs in 20 — the extra nondeterminism is which morsel worker sees which
// batch. Pinned to one P it is wrong on every run, so this asserts loudly on
// all of them, and the FIRST run that comes out right fails the test: that is
// the fix's proof and the signal to delete this file.
//
// GOMAXPROCS is process-global, so it is restored on return — and this package
// declares no t.Parallel test, so the setting cannot leak into a sibling
// running at the same time, because none is.
func TestAComputedGroupKeyOverACrossJoinIsPinnedWrong(t *testing.T) {
	if testing.Short() {
		t.Skip("-short")
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	ctx := context.Background()
	db := spillMxOpen(t, 0)
	const from = `FROM typemx a JOIN typemx b ON UPPER(a.c_str) = UPPER(b.c_str) ` +
		`WHERE a.id < 200 AND b.id < 200`

	// The TRUTH, computed from the join's own pair list — which is itself
	// deterministic, and equal to the equi-join's. Deriving it here rather
	// than hard-coding it is what keeps the pin honest if the fixture moves.
	pairs, err := tmRun(ctx, db, `SELECT a.id AS i `+from)
	if err != nil {
		t.Fatalf("pair list: %v", err)
	}
	want := map[int64]int64{}
	for _, r := range pairs.Rows {
		want[r["i"].(int64)%7]++
	}
	if len(pairs.Rows) != 196 || len(want) != 7 {
		t.Fatalf("the fixture moved: %d pairs over %d groups, expected 196 over 7",
			len(pairs.Rows), len(want))
	}

	const sql = `SELECT a.id % 7 AS k, COUNT(*) AS n ` + from + ` GROUP BY a.id % 7`
	right := 0
	var lastDiff string
	for run := 0; run < 20; run++ {
		got, err := tmRun(ctx, db, sql)
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		agree := len(got.Rows) == len(want)
		for _, r := range got.Rows {
			k, _ := r["k"].(int64)
			n, _ := r["n"].(int64)
			if want[k] != n {
				agree = false
				lastDiff = fmt.Sprintf("group %d counted %d, want %d", k, n, want[k])
			}
		}
		if agree {
			right++
		}
	}
	if right > 0 {
		t.Fatalf("#847 is pinned WRONG here, but %d of 20 runs answered correctly. If the "+
			"computed-key path was fixed, DELETE this file, close #847, and add the shape to "+
			"the sweep's crossjoin family as a value-and-GROUP-BY cell — that it answers is "+
			"the fix's proof.\n  SQL: %s", right, sql)
	}
	t.Logf("#847 reproduces on all 20 runs (last divergence: %s); the same query with the "+
		"key materialized by a subquery answers correctly, and so does the pair list it is "+
		"derived from", lastDiff)
}

// The CONTROLS, and they are the other half of the pin: the same shape with
// the key spelled in ways that do NOT take the sparse-write path must keep
// answering. If a fix for #847 breaks either of these it has traded one wrong
// answer for another.
func TestComputedGroupKeySpellingsThatMustKeepAnswering(t *testing.T) {
	if testing.Short() {
		t.Skip("-short")
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	ctx := context.Background()
	db := spillMxOpen(t, 0)
	const from = `FROM typemx a JOIN typemx b ON UPPER(a.c_str) = UPPER(b.c_str) ` +
		`WHERE a.id < 200 AND b.id < 200`

	pairs, err := tmRun(ctx, db, `SELECT a.id AS i `+from)
	if err != nil {
		t.Fatalf("pair list: %v", err)
	}
	want := map[string]int64{}
	for _, r := range pairs.Rows {
		want[fmt.Sprint(r["i"].(int64)%7)]++
	}

	for _, tc := range []struct{ name, sql string }{
		// The expression materialized by an inner projection first: the outer
		// aggregate groups by a plain column, and the sparse write never
		// happens.
		{"materialized_by_a_subquery",
			`SELECT k, COUNT(*) AS n FROM (SELECT a.id % 7 AS k ` + from + `) t GROUP BY k`},
		// A STRING-typed computed key, which aggPreProject cannot write
		// sparsely (BytesColumn.Set needs sequential writes), so it
		// materializes the selection away and is right.
		{"a_string_computed_key",
			`SELECT CAST(a.id % 7 AS VARCHAR) AS k, COUNT(*) AS n ` + from +
				` GROUP BY CAST(a.id % 7 AS VARCHAR)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for run := 0; run < 5; run++ {
				got, err := tmRun(ctx, db, tc.sql)
				if err != nil {
					t.Fatalf("run %d: %v", run, err)
				}
				if len(got.Rows) != len(want) {
					t.Fatalf("run %d: %d groups, want %d", run, len(got.Rows), len(want))
				}
				for _, r := range got.Rows {
					k := fmt.Sprint(r["k"])
					n, _ := r["n"].(int64)
					if want[k] != n {
						t.Fatalf("run %d: group %s counted %d, want %d\n  SQL: %s",
							run, k, n, want[k], tc.sql)
					}
				}
			}
		})
	}
}
