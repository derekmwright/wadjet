package logical

import (
	"os"
	"strings"
)

// semiPushdownEnabled gates pushSemiAntiBelowInnerJoins. Kill switch
// WADJET_SEMI_PUSHDOWN=0.
var semiPushdownEnabled = os.Getenv("WADJET_SEMI_PUSHDOWN") != "0"

// SetSemiPushdownEnabled toggles the rewrite (tests that pin plan shapes
// downstream of the legacy join order run with it off). Returns the
// previous value so callers can restore it.
func SetSemiPushdownEnabled(on bool) bool {
	prev := semiPushdownEnabled
	semiPushdownEnabled = on
	return prev
}

// pushSemiAntiBelowInnerJoins rotates semi(A join B, sub) to semi(A, sub) join B
// only when all probe keys resolve unambiguously to exactly ONE inner-join side.
// SEMI/ANTI only filter probe rows: they add neither columns nor duplicates.
// Require a plain inner join without CTE refs (attribution needs scan information);
// any extra semi JoinCond must not reference the other side's columns.
// Run after pushdownPredicates and descend through remaining conjunctive Filters,
// which commute with SEMI, then before reorderJoins treats the filtered relation as a leaf.
// See docs/internals/semi-anti-inner-join-pushdown.md for the design.
func pushSemiAntiBelowInnerJoins(n *Node) *Node {
	if n == nil || !semiPushdownEnabled {
		return n
	}
	for i, child := range n.Children {
		n.Children[i] = pushSemiAntiBelowInnerJoins(child)
	}
	if !isSemiOrAnti(n) {
		return n
	}
	return pushOneSemi(n)
}

func isSemiOrAnti(n *Node) bool {
	if n == nil || n.Type != NodeJoin {
		return false
	}
	jt := strings.ToLower(n.JoinType)
	return jt == "semi" || jt == "anti"
}

// pushOneSemi repeatedly rotates the semi/anti node s downward while its
// probe child is an inner join whose one side owns every probe key.
func pushOneSemi(s *Node) *Node {
	// Descend through Filter nodes on the probe side: semi commutes with
	// conjunctive filters. `top` is what replaces s in the parent when the
	// push fires — the filter chain head, or the inner join itself.
	top := s.Children[0]
	probe := top
	for probe != nil && probe.Type == NodeFilter && len(probe.Children) > 0 {
		probe = probe.Children[0]
	}
	if probe == nil || probe.Type != NodeJoin || !isInnerJoin(probe) {
		return s
	}
	// Filtered semi/anti joins carry a non-equality JoinFilter that may
	// reference probe columns from EITHER side of the inner join — pushing
	// to one side would strip the other's columns from its input. Don't.
	if s.JoinFilter != "" {
		return s
	}
	if hasCTERef(probe.Children[0]) || hasCTERef(probe.Children[1]) {
		return s
	}
	side := semiKeySide(s, probe)
	if side < 0 {
		return s
	}
	// Rotate: s moves below the inner join, filtering its `side` child.
	// The filter chain (if any) stays where it was — probe is mutated in
	// place, so the chain still points at it.
	s.Children[0] = probe.Children[side]
	probe.Children[side] = pushOneSemi(s) // keep pushing into that side
	return top
}

// semiKeySide reports which side (0 or 1) of the inner join produces
// EVERY probe-side key of the semi join s, or -1 when no single side
// does (or attribution is ambiguous). Probe keys come from s.JoinCond:
// decorrelateInSubqueries emits "<outerKey> = <innerCol>" conjuncts
// AND-joined (the left operand is always the outer/probe column) and
// leaves Node.LeftKeys unset. A conjunct that isn't a plain equality
// makes the condition unpushable — bail.
func semiKeySide(s *Node, inner *Node) int {
	probeKeys := semiProbeKeys(s)
	if len(probeKeys) == 0 {
		return -1
	}
	_, leftCols := collectScanInfo(inner.Children[0])
	_, rightCols := collectScanInfo(inner.Children[1])
	owns := func(cols map[string]string, other map[string]string, key string) bool {
		_, in := cols[key]
		_, inOther := other[key]
		return in && !inOther
	}
	side := -1
	for _, key := range probeKeys {
		var this int
		switch {
		case owns(leftCols, rightCols, key):
			this = 0
		case owns(rightCols, leftCols, key):
			this = 1
		default:
			return -1
		}
		if side >= 0 && side != this {
			return -1
		}
		side = this
	}
	return side
}

// semiProbeKeys extracts the probe-side (outer) key columns from a
// semi/anti join's condition — the left operand of each AND-joined
// equality, lowercased and stripped of any alias qualifier. Returns nil
// when any conjunct isn't a simple equality.
func semiProbeKeys(s *Node) []string {
	cond := s.JoinCond
	if cond == "" {
		return nil
	}
	var keys []string
	for _, conjunct := range strings.Split(strings.ToLower(cond), " and ") {
		eq := strings.Index(conjunct, "=")
		if eq < 0 || strings.ContainsAny(conjunct, "<>") {
			return nil
		}
		col := strings.TrimSpace(conjunct[:eq])
		if col == "" || strings.ContainsAny(col, " ()") {
			return nil
		}
		if dot := strings.LastIndex(col, "."); dot >= 0 {
			col = col[dot+1:]
		}
		keys = append(keys, col)
	}
	return keys
}
