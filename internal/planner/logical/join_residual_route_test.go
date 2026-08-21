package logical

import (
	"strings"
	"testing"
)

// routeOuterJoinOnResiduals (#358) moves an outer join's non-key ON
// conjuncts into JoinFilter, where the physical layer compiles them as the
// executor's probe residual. What stays in JoinCond must be exactly the
// bare-column equalities the key parser can represent.
func TestRouteOuterJoinOnResiduals(t *testing.T) {
	mk := func(joinType, cond string) *Node {
		return NewJoin(
			&Node{Type: NodeScan, TableName: "nation"},
			&Node{Type: NodeScan, TableName: "region"},
			joinType, cond)
	}

	cases := []struct {
		name       string
		joinType   string
		cond       string
		wantCond   string
		wantFilter string
	}{
		{
			name:     "left cross-side inequality",
			joinType: "left join",
			cond:     "n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.r_regionkey",
			wantCond: "n.n_regionkey = r.r_regionkey", wantFilter: "n.n_nationkey > r.r_regionkey",
		},
		{
			name:     "full build-side literal conjunct",
			joinType: "full outer join",
			cond:     "n.n_regionkey = r.r_regionkey AND r.r_regionkey < 3",
			wantCond: "n.n_regionkey = r.r_regionkey", wantFilter: "r.r_regionkey < 3",
		},
		{
			name:     "expression-operand equality leaves the join keyless",
			joinType: "left join",
			cond:     "n.n_regionkey = r.r_regionkey + 3",
			wantCond: "", wantFilter: "n.n_regionkey = r.r_regionkey + 3",
		},
		{
			name:     "right join routes too",
			joinType: "right join",
			cond:     "r.r_regionkey = n.n_regionkey AND n.n_nationkey > r.r_regionkey",
			wantCond: "r.r_regionkey = n.n_regionkey", wantFilter: "n.n_nationkey > r.r_regionkey",
		},
		{
			name:     "pure key equality is untouched",
			joinType: "left join",
			cond:     "n.n_regionkey = r.r_regionkey",
			wantCond: "n.n_regionkey = r.r_regionkey", wantFilter: "",
		},
		{
			name:     "inner join is not this pass's business",
			joinType: "join",
			cond:     "n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.r_regionkey",
			wantCond: "n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.r_regionkey", wantFilter: "",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			join := mk(c.joinType, c.cond)
			got := routeOuterJoinOnResiduals(join)
			if got.JoinCond != c.wantCond {
				t.Errorf("JoinCond = %q, want %q", got.JoinCond, c.wantCond)
			}
			if got.JoinFilter != c.wantFilter {
				t.Errorf("JoinFilter = %q, want %q", got.JoinFilter, c.wantFilter)
			}
		})
	}
}

// The #376 shape: a comma-joined relation with no edge to the rest of the
// chain. The greedy reorderer used to emit it as an inner join with an EMPTY
// condition, which the physical planner refuses ("could not extract join
// keys from:" — nothing after the colon) and the DAG dispatches as a keyless
// hash_join the worker rejects. An absent condition IS a cross join.
func TestGreedyReorderSpellsDisconnectedRelationAsCross(t *testing.T) {
	scan := func(name string) *Node {
		return &Node{Type: NodeScan, TableName: name}
	}
	rels := []*Node{scan("region"), scan("nation"), scan("supplier")}
	edges := []joinEdge{{
		leftIdx: 0, rightIdx: 1,
		joinType: "join", joinCond: "t0.r_regionkey = t1.n_regionkey",
	}}

	plan := greedyJoinReorder(rels, edges)

	sawCross := false
	for j := plan; j != nil && j.Type == NodeJoin; j = j.Children[0] {
		if strings.TrimSpace(j.JoinCond) == "" {
			sawCross = true
			if joinKind(j.JoinType) != "cross" {
				t.Fatalf("condition-less spine join has type %q, want cross — this is the #376 refusal shape",
					j.JoinType)
			}
		}
	}
	if !sawCross {
		t.Fatal("expected the disconnected supplier relation to join the spine without a condition")
	}
}

// extractJoinColumnRefs reads a condition structurally: an expression operand
// contributes its COLUMN, not the expression text. The lexical splitter used
// to invent a required column named "r_regionkey + 3", which the scan's
// schema check dropped — while the real r_regionkey was never requested, so
// an outer join's residual read NULL for a column the file held (#358).
func TestExtractJoinColumnRefsStructural(t *testing.T) {
	refs := map[string]bool{}
	extractJoinColumnRefs("n.n_regionkey = r.r_regionkey + 3", refs)
	if !refs["n_regionkey"] || !refs["r_regionkey"] {
		t.Errorf("refs = %v, want both n_regionkey and r_regionkey", refs)
	}
	for ref := range refs {
		if strings.ContainsAny(ref, "+-*/ ") && !strings.Contains(ref, ".") {
			t.Errorf("ref %q is expression text, not a column name", ref)
		}
	}
}
