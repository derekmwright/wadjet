// SPDX-License-Identifier: MIT

package physical

import (
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestReferencesSyntheticAgg covers the predicate's main shapes.
func TestReferencesSyntheticAgg(t *testing.T) {
	cases := []struct {
		expr string
		want bool
	}{
		{"__agg_0 / 7.0", true},
		{"__agg_0 + __agg_1", true},
		{"sum(x) / 7.0", false}, // un-rewritten — has FuncCall, not ColRef
		{"substr(o_orderdate, 1, 4)", false},
		{"x", false},
		{"x + y * 2", false},
	}
	for _, c := range cases {
		ast, err := plansql.ParseExpression(c.expr)
		if err != nil {
			t.Errorf("parse %q: %v", c.expr, err)
			continue
		}
		got := referencesSyntheticAgg(ast)
		if got != c.want {
			t.Errorf("ReferencesSyntheticAgg(%q) = %v, want %v", c.expr, got, c.want)
		}
	}
}
