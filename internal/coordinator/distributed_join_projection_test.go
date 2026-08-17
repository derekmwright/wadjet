package coordinator

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
)

// Regression tests for the two distributed wrong-answer classes found by
// the standalone-vs-distributed differential (#288) on its first campaign.
// Both are pinned with literal SQL (generator seeds shift when weights
// change) and concrete expected values.

// A SELECT-list expression over a join feeding the gather directly was
// never computed: NodeProject is a stage-generation passthrough and the
// gather's rename pass can rename/drop but not evaluate, so the query
// returned raw join columns (the #169 class, on the join path). Fixed by
// attaching the SELECT list to the join stage (attachScanSelectProjections)
// and projecting worker-side in the join fragment.
func TestDistributedJoinSelectExpression(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed test skipped in -short mode")
	}
	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF001)

	res, err := coord.ExecuteSQL(ctx,
		"SELECT CASE WHEN n_nationkey = 3 THEN 1 ELSE 0 END AS e0 FROM region JOIN nation ON n_regionkey = r_regionkey")
	if err != nil {
		t.Fatal(err)
	}
	if res.Error != "" {
		t.Fatal(res.Error)
	}
	rows := mustRows(t, res)
	if len(rows) != 25 {
		t.Fatalf("got %d rows, want 25", len(rows))
	}
	// Compare canonical values, not Go types: inferProjectionType falls
	// back to string for CASE expressions (pre-existing #169-path
	// residual, shared with the scan shape), so distributed may box e0 as
	// "0"/"1" where standalone yields int64.
	ones := 0
	for _, r := range rows {
		v, ok := r["e0"]
		if !ok || v == nil {
			t.Fatalf("row missing computed column e0: %v", r)
		}
		switch fmt.Sprint(v) {
		case "1":
			ones++
		case "0":
		default:
			t.Fatalf("e0 has unexpected value %T(%v)", v, v)
		}
	}
	// Exactly one nation has n_nationkey = 3.
	if ones != 1 {
		t.Fatalf("expected exactly 1 row with e0=1, got %d", ones)
	}
}

// ORDER BY ... LIMIT over a join: fuseSortIntoPredecessor folded the sort
// and limit into a plan-time-Singleton broadcast join, the dispatcher then
// probe-split it into multiple tasks, and the gather concatenated the
// per-task sorted outputs — dropping global order AND the limit (41
// standalone rows vs all 100 distributed). Fixed by dispatching an ordered
// gather fragment ([ShuffleSource, OpSort{keys,limit}, GatherSink]) when
// the upstream compute stage carries fused SortKeys.
func TestDistributedJoinOrderByLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed test skipped in -short mode")
	}
	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF001)

	res, err := coord.ExecuteSQL(ctx,
		"SELECT n_comment FROM supplier JOIN nation ON s_nationkey = n_nationkey ORDER BY n_comment DESC LIMIT 41")
	if err != nil {
		t.Fatal(err)
	}
	if res.Error != "" {
		t.Fatal(res.Error)
	}
	rows := mustRows(t, res)
	if len(rows) != 41 {
		t.Fatalf("got %d rows, want 41 (LIMIT must survive probe-split)", len(rows))
	}
	prev := ""
	for i, r := range rows {
		v, _ := r["n_comment"].(string)
		if v == "" {
			t.Fatalf("row %d missing n_comment: %v", i, r)
		}
		if prev != "" && v > prev {
			t.Fatalf("rows not in DESC order at index %d: %q after %q", i, v, prev)
		}
		prev = v
	}
}
