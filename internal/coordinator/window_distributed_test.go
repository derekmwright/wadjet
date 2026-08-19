package coordinator

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// windowShuffleBroadcastThreshold forces lineitem ⋈ orders to hash-join at
// SF0.01 instead of broadcasting, which is what puts a hash-partitioned
// input under the window stage. The cluster-derived default broadcasts
// everything at this scale, and a window over a broadcast join's Singleton
// output runs one task — the fallback, not the shape under test.
const windowShuffleBroadcastThreshold = 16 << 10

// TestWindowStagePartitionParallel runs a PARTITION BY window over a shuffled
// join on a real 3-worker cluster and asserts the property that partitioning
// has to preserve: within one partition the row numbers are exactly 1..n.
//
// This is the shape #349's fix is built around. A window over PARTITION BY k
// is computable one partition at a time, so when the input already arrives
// hash-partitioned on k the stage fans out to one task per partition and each
// task windows its slice locally. If a partition were ever split across two
// tasks, both halves would start counting at 1 — a duplicate rank inside one
// order, which no row count or column list would notice.
//
// The engagement guard is not a formality: over a Singleton input (a leaf
// scan, a broadcast join) the stage runs one task, and this test would then
// assert nothing about partition-parallel execution at all.
func TestWindowStagePartitionParallel(t *testing.T) {
	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF001)
	coord.config.BroadcastBytesOverride = windowShuffleBroadcastThreshold

	const sql = `SELECT l_orderkey, l_linenumber,
		ROW_NUMBER() OVER (PARTITION BY l_orderkey ORDER BY l_linenumber) AS rn
		FROM lineitem JOIN orders ON l_orderkey = o_orderkey`

	stages := planStagesForTest(t, ctx, coord.catalog, sql, 3, windowShuffleBroadcastThreshold)
	var win *physical.Stage
	for i := range stages {
		if stages[i].Type == physical.StageWindow {
			win = &stages[i]
		}
	}
	if win == nil {
		t.Fatalf("no window stage in the plan: %+v", stages)
	}
	if win.Distribution.Kind != physical.DistHashPartitioned || win.Distribution.Count < 2 {
		t.Fatalf("window stage distribution = %+v, want hash-partitioned across ≥2 tasks — "+
			"the partition-parallel path did not engage and this test would be vacuous",
			win.Distribution)
	}
	t.Logf("window stage %s: %d tasks, hash-partitioned on %v",
		win.ID, win.Distribution.Count, win.Distribution.Keys)

	result, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("ExecuteSQL: %s", result.Error)
	}
	rows := mustRows(t, result)
	if len(rows) == 0 {
		t.Fatal("no rows — the window stage produced nothing")
	}

	// Per order key: the multiset of row numbers must be exactly 1..n.
	perKey := map[string]map[int64]int{}
	for _, r := range rows {
		key := fmt.Sprint(r["l_orderkey"])
		rn, ok := numericCell(r["rn"])
		if !ok {
			t.Fatalf("row %v: rn is not numeric", r)
		}
		if perKey[key] == nil {
			perKey[key] = map[int64]int{}
		}
		perKey[key][rn]++
	}
	for key, nums := range perKey {
		for rn, count := range nums {
			if count != 1 {
				t.Fatalf("order %s: ROW_NUMBER %d appears %d times — its partition was computed "+
					"by more than one task, each numbering from 1", key, rn, count)
			}
		}
		for want := int64(1); want <= int64(len(nums)); want++ {
			if nums[want] == 0 {
				t.Fatalf("order %s: ROW_NUMBER %d missing from %d rows — the partition reached "+
					"the operator incomplete", key, want, len(nums))
			}
		}
	}
	t.Logf("%d rows across %d partitions, every partition numbered 1..n", len(rows), len(perKey))
}

func numericCell(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float32:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}
