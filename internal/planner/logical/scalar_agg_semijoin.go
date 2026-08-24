package logical

import (
	"os"
	"strings"
	"sync/atomic"
)

// ScalarAggSemijoin gates reduceDecorrelatedScalarAggs.
// Kill switch: WADJET_SCALAR_AGG_SEMIJOIN=0 (mirrors WADJET_SCALAR_DEFER /
// WADJET_EXCHANGE_ELIDE). Default on. Exported as an atomic (the
// BushyJoinReorder pattern) so tests that exercise the UNreduced
// decorrelated shape — aggregate-shuffle and dynamic-filter fixtures built
// on Q17/Q20 — can flip it with a defer-restore.
var ScalarAggSemijoin atomic.Bool

func init() {
	ScalarAggSemijoin.Store(os.Getenv("WADJET_SCALAR_AGG_SEMIJOIN") != "0")
}

// reduceDecorrelatedScalarAggs semijoin-reduces the aggregate input of each
// decorrelated correlated-scalar-subquery join (marked ScalarDecorrelated by
// tryDecorrelateScalarSubquery).
//
// Decorrelation turns
//
//	WHERE l_quantity < (SELECT 0.2*avg(l_quantity) FROM lineitem
//	                    WHERE l_partkey = p_partkey)
//
// into a LEFT JOIN against "SELECT l_partkey, avg(l_quantity) FROM lineitem
// GROUP BY l_partkey" — an aggregate over the ENTIRE inner table, even when
// the outer side keeps a fraction of the keys. At SF100 Q17 this is a full
// 600M-row lineitem scan shuffled into a 20M-group aggregate of which ~0.1%
// of groups survive the part filter (~43s of Q17's 92.5s, and its 12 scan
// tasks saturate the dispatch slots, queueing the main probe join behind
// them). Q20 and Q02 have the same shape.
//
// The reduction: find the outer branch that produces the correlation
// columns (the filtered part scan in Q17/Q02, the partsupp⊳part semijoin in
// Q20), clone it, and semijoin the aggregate's input against its distinct
// keys. Because the pass runs AFTER predicate pushdown, the branch carries
// its final filters.
//
// Validity: the reduction only removes aggregate groups whose keys cannot
// appear in any outer row that survives the branch's own
// filters/semijoins. Those outer rows die regardless of the scalar's value
// (the branch is part of the outer plan; its filters apply conjunctively),
// so removed groups are never observed. This holds for every aggregate
// function including count: absent groups produce NULL through the LEFT
// JOIN either way.
func reduceDecorrelatedScalarAggs(root *Node) *Node {
	if !ScalarAggSemijoin.Load() || root == nil {
		return root
	}
	walkReduceScalarAggs(root)
	return root
}

func walkReduceScalarAggs(n *Node) {
	if n == nil {
		return
	}
	for _, child := range n.Children {
		walkReduceScalarAggs(child)
	}
	if !n.ScalarDecorrelated || n.Type != NodeJoin || len(n.Children) != 2 {
		return
	}
	outer, agg := n.Children[0], n.Children[1]
	if agg == nil || agg.Type != NodeAggregate || len(agg.Children) != 1 {
		return
	}
	keys := parseEqJoinPairs(n.JoinCond)
	if len(keys) == 0 {
		return
	}
	outerCols := make([]string, 0, len(keys))
	for _, k := range keys {
		// Same-name correlation keys would make the semijoin condition
		// ambiguous ("l_partkey = l_partkey"); bail.
		if strings.EqualFold(k.left, k.right) {
			return
		}
		outerCols = append(outerCols, k.left)
	}

	branch := findKeySourceBranch(outer, outerCols)
	if branch == nil {
		return
	}

	proj := make([]Projection, 0, len(outerCols))
	for _, c := range outerCols {
		proj = append(proj, Projection{Column: c, Alias: c})
	}
	keySource := &Node{
		Type: NodeDistinct,
		// Planner-inserted key source, not a user SELECT DISTINCT (#466).
		BuildSideDedup: true,
		Children: []*Node{{
			Type:        NodeProject,
			Projections: proj,
			Children:    []*Node{cloneSubtree(branch)},
		}},
	}

	condParts := make([]string, 0, len(keys))
	for _, k := range keys {
		condParts = append(condParts, k.right+" = "+k.left)
	}
	// Insert the semijoin BELOW any Filter chain on the aggregate input
	// (semi commutes with conjunctive filters). Placing it above a Filter
	// breaks walkStages' filter attachment: a Filter that isn't the direct
	// parent of the stage-producing subtree loses its predicates when the
	// subtree fuses into a broadcast chain (observed as Q02's inner-agg
	// leg dropping r_name='EUROPE' → wrong mins → lost result rows).
	host := agg
	for host.Children[0].Type == NodeFilter && len(host.Children[0].Children) == 1 {
		host = host.Children[0]
	}
	host.Children[0] = &Node{
		Type:     NodeJoin,
		JoinType: "semi",
		JoinCond: strings.Join(condParts, " AND "),
		Children: []*Node{host.Children[0], keySource},
	}
}

// eqPair is one "left = right" conjunct of a decorrelation join condition.
// left is the outer column, right the inner (aggregate GROUP BY) column —
// the order tryDecorrelateScalarSubquery writes them in.
type eqPair struct{ left, right string }

func parseEqJoinPairs(cond string) []eqPair {
	if cond == "" {
		return nil
	}
	var out []eqPair
	for _, part := range strings.Split(cond, " AND ") {
		sides := strings.Split(part, " = ")
		if len(sides) != 2 {
			return nil
		}
		l, r := strings.TrimSpace(sides[0]), strings.TrimSpace(sides[1])
		if l == "" || r == "" {
			return nil
		}
		out = append(out, eqPair{left: l, right: r})
	}
	return out
}

// findKeySourceBranch locates the subtree of `outer` to clone as the
// semijoin build side: the unique scan producing every outer correlation
// column, extended upward through reducing wrappers (Filter, and semi/anti
// joins where the branch is the probe side). Returns nil — no reduction —
// unless the resulting branch actually reduces the key set (contains a
// Filter or a semi/anti join): semijoining against an unfiltered scan's
// full key set costs a join and removes nothing.
func findKeySourceBranch(outer *Node, outerCols []string) *Node {
	owner := findUniqueOwnerScan(outer, outerCols)
	if owner == nil {
		return nil
	}
	branch := owner
	for {
		parent := findParent(outer, branch)
		if parent == nil {
			break
		}
		if parent.Type == NodeFilter && len(parent.Children) == 1 {
			branch = parent
			continue
		}
		if parent.Type == NodeJoin &&
			(parent.JoinType == "semi" || parent.JoinType == "anti") &&
			len(parent.Children) == 2 && parent.Children[0] == branch {
			branch = parent
			continue
		}
		break
	}
	if !subtreeReduces(branch) {
		return nil
	}
	// The reduction only pays when the key set is provably tiny enough to
	// broadcast; a shuffled key-source semijoin costs more than the group
	// reduction saves (Q20 same-window A/B 2026-07-21: its partsupp⊳part
	// key source couldn't prove tininess, the reduce leg shuffled, and Q20
	// regressed 47.9s→61.0s). Gate on the SAME RelStatsOf estimate the
	// physical broadcast sizing consults, and require real manifest stats
	// on the owner scan — estimateScanStats falls back to 10K rows for
	// unknown tables, which would wave through arbitrarily large builds.
	if owner.ScanRowEstimate <= 0 {
		return nil
	}
	if stats := RelStatsOf(branch); stats.Rows <= 0 || stats.Rows > scalarAggSemijoinMaxKeyRows {
		return nil
	}
	return branch
}

// scalarAggSemijoinMaxKeyRows caps the estimated key-source cardinality for
// the reduction: 4M keys ≈ 64MB at the physical planner's 16B/key sizing —
// comfortably under any realistic broadcast threshold, so a branch passing
// this gate will also broadcast at dispatch.
const scalarAggSemijoinMaxKeyRows = 4_000_000

// findUniqueOwnerScan returns the single scan node in `outer` whose schema
// covers every col in cols. nil when no scan or more than one scan matches
// (ambiguous — e.g. self-joined tables), or when schemas are unknown.
func findUniqueOwnerScan(outer *Node, cols []string) *Node {
	var owner *Node
	ambiguous := false
	var walk func(n *Node)
	walk = func(n *Node) {
		if n == nil || ambiguous {
			return
		}
		if n.Type == NodeScan && scanCoversColumns(n, cols) {
			if owner != nil {
				ambiguous = true
				return
			}
			owner = n
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(outer)
	if ambiguous {
		return nil
	}
	return owner
}

func scanCoversColumns(scan *Node, cols []string) bool {
	if len(scan.ScanColumns) == 0 {
		return false
	}
	have := make(map[string]bool, len(scan.ScanColumns))
	for _, c := range scan.ScanColumns {
		have[strings.ToLower(c)] = true
	}
	for _, c := range cols {
		if !have[strings.ToLower(c)] {
			return false
		}
	}
	return true
}

func findParent(root, target *Node) *Node {
	if root == nil {
		return nil
	}
	for _, c := range root.Children {
		if c == target {
			return root
		}
		if p := findParent(c, target); p != nil {
			return p
		}
	}
	return nil
}

// subtreeReduces reports whether the branch contains at least one Filter
// predicate or semi/anti join — i.e. its key set is a strict subset of the
// owner scan's.
func subtreeReduces(n *Node) bool {
	if n == nil {
		return false
	}
	if n.Type == NodeFilter && len(n.Predicates) > 0 {
		return true
	}
	if n.Type == NodeJoin && (n.JoinType == "semi" || n.JoinType == "anti") {
		return true
	}
	for _, c := range n.Children {
		if subtreeReduces(c) {
			return true
		}
	}
	return false
}

// cloneSubtree deep-copies a logical plan subtree. Node-owned slices are
// copied so later passes (predicate pushdown, column pruning,
// attachScanPredicates) can mutate one copy without corrupting the other.
// AST expression pointers are shared: passes treat them as immutable,
// building new nodes on rewrite.
func cloneSubtree(n *Node) *Node {
	if n == nil {
		return nil
	}
	c := *n
	c.Children = make([]*Node, len(n.Children))
	for i, ch := range n.Children {
		c.Children[i] = cloneSubtree(ch)
	}
	c.ScanColumns = append([]string(nil), n.ScanColumns...)
	c.RequiredColumns = append([]string(nil), n.RequiredColumns...)
	c.ScanPredicates = append([]Predicate(nil), n.ScanPredicates...)
	c.Predicates = append([]Predicate(nil), n.Predicates...)
	c.Projections = append([]Projection(nil), n.Projections...)
	c.GroupBy = append([]string(nil), n.GroupBy...)
	c.AggExprs = append([]AggExpr(nil), n.AggExprs...)
	c.OrderBy = append([]OrderExpr(nil), n.OrderBy...)
	c.LeftKeys = append([]string(nil), n.LeftKeys...)
	c.RightKeys = append([]string(nil), n.RightKeys...)
	c.NeededColumns = append([]string(nil), n.NeededColumns...)
	c.WindowExprs = append([]WindowExpr(nil), n.WindowExprs...)
	if n.PartitionFilter != nil {
		c.PartitionFilter = make(map[string]string, len(n.PartitionFilter))
		for k, v := range n.PartitionFilter {
			c.PartitionFilter[k] = v
		}
	}
	if n.ScanColStats != nil {
		c.ScanColStats = make(map[string]ScanColumnStats, len(n.ScanColStats))
		for k, v := range n.ScanColStats {
			c.ScanColStats[k] = v
		}
	}
	return &c
}
