package logical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/optswitch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

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
// buildDedupToggle is the #287 kill switch. This pass CHANGES THE ROW SET when
// it is wrong, in both directions — a build side narrowed to too few columns
// makes a semi join answer nothing and an anti join answer everything (#562) —
// so it belongs in the registry the invariance oracle enumerates. Had it been
// there, the oracle would have reported #562 as a divergence under
// WADJET_SEMIANTI_BUILD_DEDUP=0 the first time a two-key correlation entered
// any corpus, instead of the shape having to be noticed by hand.
var buildDedupToggle = optswitch.Register("semianti-build-dedup", "WADJET_SEMIANTI_BUILD_DEDUP",
	"narrow a semi/anti join's build side to Project(join keys) -> Distinct, so the hash table is sized to NDV")

func dedupSemiAntiBuildSide(n *Node) *Node {
	if n == nil {
		return nil
	}
	// Recurse first so inner joins are handled before their parents.
	for i, child := range n.Children {
		n.Children[i] = dedupSemiAntiBuildSide(child)
	}
	if n.Type != NodeJoin || !buildDedupToggle.On() {
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
	// A decorrelated join whose build-side key is still unspelled would be
	// projected to a name that may not exist, and a Project of nothing emits
	// nothing (#526). The bound is not lost, only deferred:
	// repairDecorrelatedSpelling calls this pass again on the node once
	// reorderJoins has settled the spelling.
	if deferSemiAntiDedup(n) {
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

// extractRightJoinKeys reads a join condition STRUCTURALLY and returns the
// build-side key of every one of its conjuncts, or nil when even one conjunct
// cannot be attributed.
//
// All-or-nothing is the whole contract. The caller projects the build side
// down to exactly these keys, so a key list that is short by one conjunct
// deletes a column the join still compares and the join then matches NOTHING
// — a semi join answers zero rows and an anti join answers every row, both
// silently.
//
// This used to split the text on " and " and then on the first "=". The
// condition a decorrelated EXISTS/IN writes is rendered with " AND "
// (renderDecorrelatedKeys), which that split does not see: a two-key
// correlation came through as ONE part whose right operand was the literal
// text "b.k AND a.k2 = b.k2", and the only key that survived was the first
// conjunct's (#562). It is the same lexical-where-the-condition-is-structural
// defect physical.parseJoinKeys was rewritten for in #351, one layer up, so
// this reads the same way: parse, flatten the top-level ANDs, and require
// each conjunct to be an equality between two bare column references.
//
// Side membership is decided by walking the right subtree's available
// columns; whichever side of each equality resolves there is the build key.
// A name that resolves on BOTH sides (a self-join's `k = k`) is not
// attributable from the condition alone and bails — as it always has.
//
// The last decline is about the narrowing's own Project rather than the
// condition: it aliases every key to its BARE name, so a QUALIFIED key would
// be renamed out from under the join that still asks for it. See the comment
// at the check.
func extractRightJoinKeys(cond string, rightSubtree *Node) []string {
	if cond == "" || rightSubtree == nil {
		return nil
	}
	rightCols := collectSubtreeColumns(rightSubtree)
	if len(rightCols) == 0 {
		return nil
	}
	expr := tryParseExpr(cond)
	if expr == nil {
		// Unparseable: attribute nothing rather than guess from text.
		return nil
	}
	var keys []string
	stripped := make(map[string]bool)
	for _, conj := range flattenJoinCondConjuncts(expr) {
		cmp, isCmp := conj.(*plansql.CmpExpr)
		if !isCmp || cmp.Op != "=" {
			return nil // non-equi conjunct: no key to dedup on
		}
		l, lIsCol := joinCondColName(cmp.Left)
		r, rIsCol := joinCondColName(cmp.Right)
		if !lIsCol || !rIsCol {
			// A literal operand — including the optimizer's `1 = 1` ON-TRUE
			// sentinel — names no build column.
			return nil
		}
		lInRight := containsColumn(rightCols, strings.ToLower(stripQualifier(l)))
		rInRight := containsColumn(rightCols, strings.ToLower(stripQualifier(r)))
		var key string
		switch {
		case rInRight && !lInRight:
			key = r
		case lInRight && !rInRight:
			key = l
		default:
			// Ambiguous or neither resolved — bail on the whole condition.
			// Conservative: don't dedup if we can't be sure.
			return nil
		}
		// The Project the caller builds aliases every key to its BARE name
		// (Projection{Column: k, Alias: stripQualifier(k)}), so a key the
		// CONDITION spells qualified would be renamed out from under the
		// condition: the build emits `q_s` while the join still asks for
		// `b1.q_s`, which resolves to index -1 and matches nothing. That is
		// reachable whenever the build subtree has two arms sharing a bare
		// name and reorderJoins therefore qualifies one side — an EXISTS
		// whose inner self-joins. The narrowing does not get to pick a
		// spelling the condition above it already fixed, so it declines.
		if !strings.EqualFold(bare(key), key) {
			return nil
		}
		// Two keys that strip to the same name would emit one column twice
		// and the second key would read the first's values. Same reason,
		// same answer.
		if stripped[bare(key)] {
			return nil
		}
		stripped[bare(key)] = true
		keys = append(keys, key)
	}
	return keys
}

// bare is a key's unqualified, case-folded name — what the narrowing's
// Project would emit it under.
func bare(key string) string { return strings.ToLower(stripQualifier(key)) }

// flattenJoinCondConjuncts splits a parsed join condition on its top-level
// ANDs, unwrapping parentheses. Unlike a string split it cannot be fooled by
// an " and " inside a string literal, by one under an OR, or by a rendering
// that spelled the operator in a different case.
func flattenJoinCondConjuncts(expr plansql.Node) []plansql.Node {
	switch e := expr.(type) {
	case *plansql.ParenNode:
		return flattenJoinCondConjuncts(e.Inner)
	case *plansql.AndNode:
		return append(flattenJoinCondConjuncts(e.Left), flattenJoinCondConjuncts(e.Right)...)
	}
	return []plansql.Node{expr}
}

// joinCondColName renders an ON operand as the column name the plan spells it
// with, keeping any qualifier. ok reports whether the operand IS a bare column
// reference; anything else (a literal, an arithmetic expression, a function
// call) comes back false.
func joinCondColName(n plansql.Node) (string, bool) {
	switch e := n.(type) {
	case *plansql.ParenNode:
		return joinCondColName(e.Inner)
	case *plansql.ColRef:
		if e.Table != "" {
			return e.Table + "." + e.Column, true
		}
		return e.Column, true
	}
	return "", false
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
