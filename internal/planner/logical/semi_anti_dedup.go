package logical

import "strings"

// dedupSemiAntiBuildSide wraps the build side (right child) of every SEMI
// or ANTI join in a GroupBy on the join keys, so the hash-join build phase
// constructs a hash table sized to NDV rather than raw row count.
//
// Motivation: for Q04 (orders ⨝SEMI lineitem on l_orderkey) and Q21
// (lineitem self-joins on l_orderkey), the build side is a fact table
// (30M-60M rows) whose join key has much lower cardinality (~15M
// orderkeys). Without dedup the build hashtable holds 2-4× the entries
// it needs, the dynamic-filter eligibility check rejects it as too big
// (Q04/Q21 SF10 A/B audit, 2026-05-25), and the probe is slowed by
// duplicate-key probe collisions for the same orderkey.
//
// Semantics: SEMI / ANTI joins return a subset of LEFT rows based on
// existence in the RIGHT side. Whether RIGHT has duplicates is
// irrelevant to the result — only the SET of right keys matters. So
// wrapping RIGHT in GroupBy(rightKeys) is a semantics-preserving
// rewrite that bounds build cardinality by NDV.
//
// This pass runs after pushdownPredicates (so filters land on the inner
// scan before dedup) and before reorderJoins (so the dedup'd subtree's
// cost estimate flows through the join reorderer).
func dedupSemiAntiBuildSide(n *Node) *Node {
	if n == nil {
		return nil
	}
	// Recurse first so inner joins are handled before their parents.
	for i, child := range n.Children {
		n.Children[i] = dedupSemiAntiBuildSide(child)
	}
	if n.Type != NodeJoin {
		return n
	}
	switch n.JoinType {
	case "semi", "anti":
	default:
		return n
	}
	// A filtered semi/anti join (non-equality correlated condition from
	// decorrelated EXISTS) evaluates JoinFilter against build-side columns
	// at probe time. Project(keys)→Distinct would drop those columns (the
	// filter then resolves to nothing and rejects every row) and dedup on
	// keys alone discards the duplicate rows the filter must see. Skip;
	// the exec layer narrows filtered builds to keys + filter columns via
	// HashJoin.BuildStoreCols instead.
	if n.JoinFilter != "" {
		return n
	}
	if len(n.Children) < 2 {
		return n
	}
	right := n.Children[1]
	rightKeys := extractRightJoinKeys(n.JoinCond, right)
	if len(rightKeys) == 0 {
		// Non-equi semi/anti, or couldn't identify which side of each
		// equality belongs to the right child. Can't safely dedup.
		return n
	}
	// Skip if already aggregated / distinct on a superset of the right keys.
	if alreadyDedupOnKeys(right, rightKeys) {
		return n
	}
	// Wrap right in a Project(rightKeys) → Distinct chain. We need a Project
	// first to narrow the schema to just the join keys (so the Distinct
	// dedupes on those columns specifically, not on every input column).
	// Then Distinct produces one row per distinct key tuple.
	projections := make([]Projection, len(rightKeys))
	for i, k := range rightKeys {
		projections[i] = Projection{Column: k, Alias: stripQualifier(k)}
	}
	proj := NewProject(right, projections)
	dedup := NewDistinct(proj)
	// Planner-inserted, not a user SELECT DISTINCT: keep it out of
	// rewriteDistinctAsGroupBy's remit so the physical planner's
	// Distinct(Project) build-side handling still matches (#466).
	dedup.BuildSideDedup = true
	n.Children[1] = dedup
	return n
}

// extractRightJoinKeys parses a join condition like "a.k = b.k AND a.k2 = b.k2"
// and returns the keys belonging to the right subtree. Side membership is
// decided by walking the right subtree's available columns; whichever side
// of each equality resolves there is the right key.
func extractRightJoinKeys(cond string, rightSubtree *Node) []string {
	if cond == "" || rightSubtree == nil {
		return nil
	}
	rightCols := collectSubtreeColumns(rightSubtree)
	if len(rightCols) == 0 {
		return nil
	}
	var keys []string
	for _, part := range strings.Split(cond, " and ") {
		part = strings.TrimSpace(part)
		eq := strings.SplitN(part, "=", 2)
		if len(eq) != 2 {
			continue
		}
		l := strings.TrimSpace(eq[0])
		r := strings.TrimSpace(eq[1])
		lStripped := strings.ToLower(stripQualifier(l))
		rStripped := strings.ToLower(stripQualifier(r))
		// Check which side is in the right subtree (lowercase match).
		lInRight := containsColumn(rightCols, lStripped)
		rInRight := containsColumn(rightCols, rStripped)
		switch {
		case rInRight && !lInRight:
			keys = append(keys, r)
		case lInRight && !rInRight:
			keys = append(keys, l)
		default:
			// Ambiguous or neither resolved — bail on this equality.
			// Conservative: don't dedup if we can't be sure.
			return nil
		}
	}
	return keys
}

// containsColumn looks up a (lowercase, unqualified) column name in the
// subtree-columns map produced by collectSubtreeColumns. The map's keys
// are mixed-case so we lowercase both sides for comparison.
func containsColumn(cols map[string]bool, lcName string) bool {
	for k := range cols {
		if strings.EqualFold(k, lcName) {
			return true
		}
	}
	return false
}

// alreadyDedupOnKeys reports whether the subtree's root already
// guarantees distinct values on the given key set. Conservative: only
// recognizes a top-level Aggregate whose GroupBy is a superset of keys,
// or a top-level Distinct. Misses transitive guarantees (e.g. a PRIMARY
// KEY scan); that's fine — false negatives just mean an idempotent
// extra dedup wrap, no semantic issue.
func alreadyDedupOnKeys(n *Node, keys []string) bool {
	if n == nil {
		return false
	}
	switch n.Type {
	case NodeAggregate:
		need := make(map[string]bool, len(keys))
		for _, k := range keys {
			need[stripQualifier(k)] = true
		}
		for _, g := range n.GroupBy {
			delete(need, stripQualifier(g))
		}
		return len(need) == 0
	case NodeDistinct:
		// Distinct on all output columns — includes the keys by construction
		// (caller projected to keys upstream). Conservative: only recognize
		// when the immediate child output matches keys.
		return true
	}
	return false
}
