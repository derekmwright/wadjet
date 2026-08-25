package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// selfJoinDerived builds the plan shape #489 and #490 are about: a derived
// table whose body self-joins one relation, so both arms answer to the same
// BARE column name and only the qualifier says which.
//
//	(SELECT n1.nm AS a, n2.nm AS b FROM n n1 JOIN n n2 ON n1.r = n2.id) u
func selfJoinDerived() *logical.Node {
	arm := func(alias string) *logical.Node {
		return &logical.Node{Type: logical.NodeScan, TableName: "n", TableAlias: alias,
			ScanColumns:    []string{"id", "nm", "r"},
			DerivedAliases: []string{"u"}}
	}
	return &logical.Node{Type: logical.NodeProject,
		Projections: []logical.Projection{
			{Column: "nm", Expr: "n1.nm", Alias: "a"},
			{Column: "nm", Expr: "n2.nm", Alias: "b"},
		},
		Children: []*logical.Node{{
			Type: logical.NodeJoin, JoinType: "inner",
			Children: []*logical.Node{arm("n1"), arm("n2")},
		}},
	}
}

// TestAggInputNameKeepsTheQualifierOverASelfJoin: an aggregate argument or
// GROUP BY key naming a derived table's alias must resolve to the spelling
// that identifies the ARM, not to the bare column both arms share. Resolving
// `u.b` to `nm` bound whichever copy the stream happened to name first —
// PostgreSQL 17 answers 3 groups for the equivalent query and this engine
// answered one per row of the other arm (#489).
func TestAggInputNameKeepsTheQualifierOverASelfJoin(t *testing.T) {
	for _, tc := range []struct{ key, want string }{
		{"u.a", "n1.nm"},
		{"u.b", "n2.nm"},
		{"a", "n1.nm"},
		{"b", "n2.nm"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			got, expr, _, renamed := resolveAggInputName(tc.key, selfJoinDerived())
			if expr != nil {
				t.Fatalf("resolveAggInputName(%q) returned an expression, want a column", tc.key)
			}
			if !renamed || got != tc.want {
				t.Errorf("resolveAggInputName(%q) = %q (renamed=%v), want %q — the bare name "+
					"names BOTH arms of the self-join", tc.key, got, renamed, tc.want)
			}
		})
	}
}

// TestAggInputNameLeavesAnUnqualifiedRenameAlone: the qualifier-preserving
// preference must not invent one. Where the query wrote the source column
// bare, the resolved name stays bare — the overwhelmingly common case, and
// what every existing plan depends on.
func TestAggInputNameLeavesAnUnqualifiedRenameAlone(t *testing.T) {
	plan := &logical.Node{Type: logical.NodeProject,
		Projections: []logical.Projection{{Column: "n_nationkey", Expr: "n_nationkey", Alias: "k"}},
		Children: []*logical.Node{{
			Type: logical.NodeScan, TableName: "nation", TableAlias: "u",
			ScanColumns: []string{"n_nationkey"}, DerivedAliases: []string{"u"},
		}},
	}
	got, _, _, renamed := resolveAggInputName("u.k", plan)
	if !renamed || got != "n_nationkey" {
		t.Errorf("resolveAggInputName(%q) = %q (renamed=%v), want %q",
			"u.k", got, renamed, "n_nationkey")
	}
}
