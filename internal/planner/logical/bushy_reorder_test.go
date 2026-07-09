package logical

import (
	"testing"
)

// statScan builds a scan node with row estimate, columns, and per-column NDV
// stats so the DP cost model sees deterministic inputs.
func statScan(table string, rows int64, colNDV map[string]int64) *Node {
	cols := make([]string, 0, len(colNDV))
	stats := make(map[string]ScanColumnStats, len(colNDV))
	for col, ndv := range colNDV {
		cols = append(cols, col)
		stats[col] = ScanColumnStats{NDV: ndv, TotalRows: rows}
	}
	return &Node{
		Type:            NodeScan,
		TableName:       table,
		ScanColumns:     cols,
		ScanRowEstimate: rows,
		ScanColStats:    stats,
	}
}

func innerJoin(left, right *Node, cond string) *Node {
	return &Node{
		Type:     NodeJoin,
		JoinType: "inner",
		JoinCond: cond,
		Children: []*Node{left, right},
	}
}

// expandingChain builds ((a ⋈ xdim) ⋈ b) ⋈ ydim where a ⋈ b is a
// many-to-many explosion (both key NDVs 10 → Selinger 1000·1000/10 = 100K
// rows) and each fact has a small dimension. The optimal order attaches both
// dimensions BEFORE the exploding join — a shape only a bushy plan can
// express: (a ⋈ xdim) ⋈ (b ⋈ ydim).
func expandingChain() *Node {
	a := statScan("fact_a", 1000, map[string]int64{"a_id": 10, "a_x": 1000})
	xdim := statScan("dim_x", 10, map[string]int64{"x_id": 10, "x_v": 10})
	b := statScan("fact_b", 1000, map[string]int64{"b_id": 10, "b_y": 1000})
	ydim := statScan("dim_y", 10, map[string]int64{"y_id": 10, "y_v": 10})

	return innerJoin(
		innerJoin(
			innerJoin(a, xdim, "a_x = x_id"),
			b, "a_id = b_id"),
		ydim, "b_y = y_id")
}

// countJoinShape walks the join tree and reports (bushyJoins, leftDeepOK):
// a bushy join has a Join in Children[1]; leftDeepOK means no join does.
func countBushyJoins(n *Node) int {
	if n == nil || n.Type != NodeJoin {
		return 0
	}
	count := countBushyJoins(n.Children[0]) + countBushyJoins(n.Children[1])
	if len(n.Children) == 2 && n.Children[1].Type == NodeJoin {
		count++
	}
	return count
}

func TestBushyReorder_DormantByDefault(t *testing.T) {
	if BushyJoinReorder.Load() {
		t.Fatal("BushyJoinReorder must default to off")
	}
	before := BushyJoinsPlanned.Load()
	plan := reorderJoins(expandingChain())
	if got := countBushyJoins(plan); got != 0 {
		t.Fatalf("flag off: plan contains %d bushy join(s), want pure left-deep", got)
	}
	if BushyJoinsPlanned.Load() != before {
		t.Fatal("BushyJoinsPlanned moved with the flag off")
	}
}

func TestBushyReorder_ExpandingJoinDeferred(t *testing.T) {
	BushyJoinReorder.Store(true)
	defer BushyJoinReorder.Store(false)

	before := BushyJoinsPlanned.Load()
	plan := reorderJoins(expandingChain())
	if got := countBushyJoins(plan); got != 1 {
		t.Fatalf("expanding-join shape: got %d bushy joins, want exactly 1", got)
	}
	// The root must join two composites: each side is itself a join
	// (fact ⋈ its dimension).
	if plan.Children[0].Type != NodeJoin || plan.Children[1].Type != NodeJoin {
		t.Fatalf("root should join two composites, got %v ⋈ %v",
			plan.Children[0].Type, plan.Children[1].Type)
	}
	// The exploding a↔b edge must be the root condition — deferred last.
	if plan.JoinCond != "a_id = b_id" {
		t.Fatalf("root join condition = %q, want the deferred expanding edge a_id = b_id", plan.JoinCond)
	}
	if BushyJoinsPlanned.Load() != before+1 {
		t.Fatalf("BushyJoinsPlanned = %d, want %d", BushyJoinsPlanned.Load(), before+1)
	}
}

// TestBushyReorder_StarSchemaKeepsLeftDeep: when every dimension attaches
// DIRECTLY to the fact table, each bushy partition either has a
// single-relation build (identical cost to the left-deep transition) or a
// disconnected side — so the STRICT-improvement rule keeps today's
// left-deep shape. No plan churn on plain star joins.
func TestBushyReorder_StarSchemaKeepsLeftDeep(t *testing.T) {
	BushyJoinReorder.Store(true)
	defer BushyJoinReorder.Store(false)

	fact := statScan("fact", 1000, map[string]int64{"f_id": 1000, "f_d1": 100, "f_d2": 10})
	d1 := statScan("dim1", 100, map[string]int64{"d1_id": 100})
	d2 := statScan("dim2", 10, map[string]int64{"d2_id": 10})
	star := innerJoin(
		innerJoin(fact, d1, "f_d1 = d1_id"),
		d2, "f_d2 = d2_id")

	before := BushyJoinsPlanned.Load()
	plan := reorderJoins(star)
	if got := countBushyJoins(plan); got != 0 {
		t.Fatalf("star schema: got %d bushy joins, want left-deep (tie must not flip)", got)
	}
	if BushyJoinsPlanned.Load() != before {
		t.Fatal("BushyJoinsPlanned moved on a star-tie schema")
	}
}

// TestBushyReorder_SnowflakeDimChain: a dimension CHAIN hanging off the fact
// (fact→d1→d2, TPC-H's supplier→nation→region) is strictly cheaper bushy:
// pre-joining the chain (d1 ⋈ d2, probe 100) means the 1000-row fact stream
// passes ONE join instead of two. This is the memo's headline payoff shape —
// the cost model must find it.
func TestBushyReorder_SnowflakeDimChain(t *testing.T) {
	BushyJoinReorder.Store(true)
	defer BushyJoinReorder.Store(false)

	fact := statScan("fact", 1000, map[string]int64{"f_id": 1000, "f_d1": 100})
	d1 := statScan("dim1", 100, map[string]int64{"d1_id": 100, "d1_d2": 10})
	d2 := statScan("dim2", 10, map[string]int64{"d2_id": 10})
	snowflake := innerJoin(
		innerJoin(fact, d1, "f_d1 = d1_id"),
		d2, "d1_d2 = d2_id")

	before := BushyJoinsPlanned.Load()
	plan := reorderJoins(snowflake)
	if got := countBushyJoins(plan); got != 1 {
		t.Fatalf("snowflake chain: got %d bushy joins, want 1 (fact ⋈ (d1 ⋈ d2))", got)
	}
	// The composite must be the BUILD side (fact is bigger and probes).
	if plan.Children[0].Type != NodeScan || plan.Children[1].Type != NodeJoin {
		t.Fatalf("want fact ⋈ (d1 ⋈ d2), got %v ⋈ %v",
			plan.Children[0].Type, plan.Children[1].Type)
	}
	if BushyJoinsPlanned.Load() != before+1 {
		t.Fatal("BushyJoinsPlanned did not count the snowflake plan")
	}
}
