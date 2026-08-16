package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

func scanNode(table, alias string, cols ...string) *logical.Node {
	return &logical.Node{
		Type:        logical.NodeScan,
		TableName:   table,
		TableAlias:  alias,
		ScanColumns: cols,
	}
}

func joinNode(jt string, left, right *logical.Node, cond string) *logical.Node {
	return &logical.Node{
		Type:     logical.NodeJoin,
		JoinType: jt,
		JoinCond: cond,
		Children: []*logical.Node{left, right},
	}
}

func TestSubtreeNamingScanOwnership(t *testing.T) {
	s := subtreeNamingOf(scanNode("nation", "n1", "n_nationkey", "n_name"))

	tests := []struct {
		key  string
		want bool
	}{
		{"n_nationkey", true},
		{"n1.n_nationkey", true},
		{"N1.N_NATIONKEY", true}, // case-insensitive
		{"n2.n_nationkey", false},
		{"nation.n_nationkey", false}, // alias overrides table name
		{"r_regionkey", false},
		{"n1.r_regionkey", false},
		{"extract(year from o_orderdate)", false}, // expression key
	}
	for _, tt := range tests {
		if got := s.ownsKey(tt.key); got != tt.want {
			t.Errorf("ownsKey(%q) = %v, want %v", tt.key, got, tt.want)
		}
	}
}

func TestSubtreeNamingTableNameFallback(t *testing.T) {
	s := subtreeNamingOf(scanNode("region", "", "r_regionkey", "r_name"))
	if !s.ownsKey("region.r_regionkey") {
		t.Error("unaliased scan should own table-qualified key")
	}
	if !s.ownsKey("r_name") {
		t.Error("unaliased scan should own bare key")
	}
}

func TestSubtreeNamingSemiAntiBuildInvisible(t *testing.T) {
	// orders SEMI JOIN lineitem — lineitem columns are not in the output.
	semi := joinNode("semi",
		scanNode("orders", "", "o_orderkey", "o_custkey"),
		scanNode("lineitem", "", "l_orderkey", "l_suppkey"),
		"o_orderkey = l_orderkey")
	s := subtreeNamingOf(semi)
	if !s.ownsKey("o_custkey") {
		t.Error("semi join must own probe columns")
	}
	if s.ownsKey("l_suppkey") {
		t.Error("semi join must NOT own build-only columns")
	}
	if s.ownsKey("lineitem.l_orderkey") {
		t.Error("semi join must NOT own qualified build columns")
	}
}

func TestSubtreeNamingInnerJoinBothSidesVisible(t *testing.T) {
	inner := joinNode("inner",
		scanNode("nation", "n2", "n_nationkey", "n_name"),
		scanNode("region", "", "r_regionkey", "r_name"),
		"n_regionkey = r_regionkey")
	s := subtreeNamingOf(inner)
	for _, key := range []string{"n2.n_name", "r_name", "n_nationkey", "r_regionkey"} {
		if !s.ownsKey(key) {
			t.Errorf("inner join subtree should own %q", key)
		}
	}
	if s.ownsKey("n1.n_name") {
		t.Error("must not own the other self-join copy's qualified key")
	}
}

func TestSubtreeNamingProjectAndAggregateOutputs(t *testing.T) {
	agg := &logical.Node{
		Type:     logical.NodeAggregate,
		GroupBy:  []string{"l_suppkey"},
		AggExprs: []logical.AggExpr{{Func: "sum", InputCol: "l_extendedprice", OutputCol: "total_revenue"}},
		Children: []*logical.Node{scanNode("lineitem", "", "l_suppkey", "l_extendedprice")},
	}
	proj := &logical.Node{
		Type:        logical.NodeProject,
		Projections: []logical.Projection{{Column: "l_suppkey", Alias: "supplier_no"}},
		Children:    []*logical.Node{agg},
	}
	s := subtreeNamingOf(proj)
	if !s.ownsKey("total_revenue") {
		t.Error("aggregate output name should be owned")
	}
	if !s.ownsKey("supplier_no") {
		t.Error("projection alias should be owned")
	}
	if !s.ownsKey("l_suppkey") {
		t.Error("underlying scan column should remain owned")
	}
}

func TestSubtreeNamingBuildColOrigins(t *testing.T) {
	// Single-alias build: nil origins (BuildTableAlias suffices; keeps
	// left-deep plans byte-identical).
	single := subtreeNamingOf(scanNode("nation", "n2", "n_nationkey", "n_name"))
	if single.buildColOrigins() != nil {
		t.Fatal("single-alias subtree must return nil origins")
	}

	// Multi-table build: each bare column maps to its owning scan alias.
	bushy := joinNode("inner",
		scanNode("supplier", "", "s_suppkey", "s_nationkey"),
		joinNode("inner",
			scanNode("nation", "n2", "n_nationkey", "n_name"),
			scanNode("region", "", "r_regionkey", "r_name"),
			"n_regionkey = r_regionkey"),
		"s_nationkey = n_nationkey")
	origins := subtreeNamingOf(bushy).buildColOrigins()
	if origins == nil {
		t.Fatal("multi-alias subtree must return origins")
	}
	want := map[string]string{
		"s_suppkey":   "supplier",
		"n_name":      "n2",
		"r_regionkey": "region",
	}
	for col, alias := range want {
		if origins[col] != alias {
			t.Errorf("origins[%q] = %q, want %q", col, origins[col], alias)
		}
	}
}

func TestSubtreeNamingOriginsProbePriority(t *testing.T) {
	// Self-join inside one subtree: the probe-most copy owns the bare name.
	selfJoin := joinNode("inner",
		scanNode("nation", "n1", "n_nationkey", "n_name"),
		scanNode("nation", "n2", "n_nationkey", "n_name"),
		"n1.n_nationkey = n2.n_nationkey")
	s := subtreeNamingOf(selfJoin)
	if got := s.origins["n_name"]; got != "n1" {
		t.Errorf("probe-most scan should own bare name: got %q, want n1", got)
	}
}

func TestAssignJoinKeySides(t *testing.T) {
	probeChain := joinNode("inner",
		joinNode("inner",
			scanNode("customer", "", "c_custkey", "c_nationkey"),
			scanNode("orders", "", "o_orderkey", "o_custkey"),
			"c_custkey = o_custkey"),
		scanNode("nation", "n1", "n_nationkey", "n_name"),
		"c_nationkey = n1.n_nationkey")

	tests := []struct {
		name       string
		leftKeys   []string
		rightKeys  []string
		build      *logical.Node
		wantLeft   []string
		wantRight  []string
	}{
		{
			name:      "correct order kept",
			leftKeys:  []string{"o_orderkey"},
			rightKeys: []string{"l_orderkey"},
			build:     scanNode("lineitem", "", "l_orderkey", "l_suppkey"),
			wantLeft:  []string{"o_orderkey"},
			wantRight: []string{"l_orderkey"},
		},
		{
			name:      "reversed bare keys swapped",
			leftKeys:  []string{"l_orderkey"},
			rightKeys: []string{"o_orderkey"},
			build:     scanNode("lineitem", "", "l_orderkey", "l_suppkey"),
			wantLeft:  []string{"o_orderkey"},
			wantRight: []string{"l_orderkey"},
		},
		{
			name:      "self-join qualified keys kept",
			leftKeys:  []string{"n1.n_nationkey"},
			rightKeys: []string{"n2.n_nationkey"},
			build:     scanNode("nation", "n2", "n_nationkey", "n_name"),
			wantLeft:  []string{"n1.n_nationkey"},
			wantRight: []string{"n2.n_nationkey"},
		},
		{
			name:      "reversed self-join qualified keys swapped",
			leftKeys:  []string{"n2.n_nationkey"},
			rightKeys: []string{"n1.n_nationkey"},
			build:     scanNode("nation", "n2", "n_nationkey", "n_name"),
			wantLeft:  []string{"n1.n_nationkey"},
			wantRight: []string{"n2.n_nationkey"},
		},
		{
			name:      "expression left key with build-owned right kept",
			leftKeys:  []string{"substring(c_phone from 1 for 2)"},
			rightKeys: []string{"l_orderkey"},
			build:     scanNode("lineitem", "", "l_orderkey"),
			wantLeft:  []string{"substring(c_phone from 1 for 2)"},
			wantRight: []string{"l_orderkey"},
		},
		{
			name:      "build-owned left with expression right swapped",
			leftKeys:  []string{"l_orderkey"},
			rightKeys: []string{"substring(c_phone from 1 for 2)"},
			build:     scanNode("lineitem", "", "l_orderkey"),
			wantLeft:  []string{"substring(c_phone from 1 for 2)"},
			wantRight: []string{"l_orderkey"},
		},
		{
			// Probe chain owns n_nationkey too (via n1), so the build's
			// copy is not exclusive — but the probe-side key IS exclusive
			// to the probe. The reversed pair must still swap.
			name:      "reversed keys with bushy multi-table build swapped",
			leftKeys:  []string{"n_nationkey"},
			rightKeys: []string{"c_nationkey"},
			build: joinNode("inner",
				scanNode("nation", "n2", "n_nationkey", "n_regionkey"),
				scanNode("region", "", "r_regionkey", "r_name"),
				"n_regionkey = r_regionkey"),
			wantLeft:  []string{"c_nationkey"},
			wantRight: []string{"n_nationkey"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe := subtreeNamingOf(probeChain)
			build := subtreeNamingOf(tt.build)
			left := append([]string(nil), tt.leftKeys...)
			right := append([]string(nil), tt.rightKeys...)
			assignJoinKeySides(left, right, probe, build)
			for i := range left {
				if left[i] != tt.wantLeft[i] || right[i] != tt.wantRight[i] {
					t.Errorf("pair %d = (%q, %q), want (%q, %q)",
						i, left[i], right[i], tt.wantLeft[i], tt.wantRight[i])
				}
			}
		})
	}
}
