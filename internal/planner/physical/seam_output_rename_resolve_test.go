// SPDX-License-Identifier: MIT

package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// TestResolveOutputRenameSource covers the walk itself over constructed
// logical trees: plain rename, chained rename, self-rename, computed alias
// stop, aggregate stop, and join recursion.
func TestResolveOutputRenameSource(t *testing.T) {
	scan := func() *logical.Node { return &logical.Node{Type: logical.NodeScan, TableName: "region"} }
	renameProj := func(col, alias string, child *logical.Node) *logical.Node {
		return &logical.Node{Type: logical.NodeProject,
			Projections: []logical.Projection{{Column: col, Expr: col, Alias: alias}},
			Children:    []*logical.Node{child}}
	}

	tests := []struct {
		name  string
		in    string
		child *logical.Node
		want  string
	}{
		{name: "plain rename", in: "k",
			child: renameProj("r_regionkey", "k", scan()), want: "r_regionkey"},
		{name: "not an alias", in: "r_name",
			child: renameProj("r_regionkey", "k", scan()), want: "r_name"},
		{name: "chained through nested projects", in: "a",
			child: renameProj("b", "a", renameProj("r_regionkey", "b", scan())),
			want:  "r_regionkey"},
		{name: "self-rename stops", in: "k",
			child: renameProj("k", "k", scan()), want: "k"},
		{name: "through a filter", in: "k",
			child: &logical.Node{Type: logical.NodeFilter,
				Children: []*logical.Node{renameProj("r_regionkey", "k", scan())}},
			want: "r_regionkey"},
		{name: "computed alias stops (materialized under its own name)", in: "rk2",
			child: &logical.Node{Type: logical.NodeProject,
				Projections: []logical.Projection{{Column: "", Expr: "nullif(r_regionkey, 2)", Alias: "rk2"}},
				Children:    []*logical.Node{scan()}},
			want: "rk2"},
		{name: "aggregate stops the walk", in: "k",
			child: &logical.Node{Type: logical.NodeAggregate,
				Children: []*logical.Node{renameProj("r_regionkey", "k", scan())}},
			want: "k"},
		{name: "join recurses into build side", in: "k",
			child: &logical.Node{Type: logical.NodeJoin, JoinType: "inner",
				Children: []*logical.Node{scan(), renameProj("r_regionkey", "k", scan())}},
			want: "r_regionkey"},
		{name: "join recurses into probe side first", in: "k",
			child: &logical.Node{Type: logical.NodeJoin, JoinType: "inner",
				Children: []*logical.Node{renameProj("n_regionkey", "k", scan()), scan()}},
			want: "n_regionkey"},
		{name: "semi join skips the build side", in: "k",
			child: &logical.Node{Type: logical.NodeJoin, JoinType: "semi",
				Children: []*logical.Node{scan(), renameProj("r_regionkey", "k", scan())}},
			want: "k"},
		{name: "nil child", in: "k", child: nil, want: "k"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveOutputRenameSource(tc.in, tc.child); got != tc.want {
				t.Errorf("ResolveOutputRenameSource(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
