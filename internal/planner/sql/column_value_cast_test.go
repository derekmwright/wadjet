// SPDX-License-Identifier: MIT

package sql

import "testing"

// A correlated re-run's outer value travels as TEXT and must come back as the
// column-typed cast it was written as (CastNode.Column), or the re-run types
// it as a CAST expression: `coalesce(o.i, x.v)` bigint, `x.m / o.i` on the
// float8 rung. The spelling is accepted over a literal only; over anything
// else it stays a call to a function nobody defines.
func TestColumnValueCastRoundTrips(t *testing.T) {
	for _, src := range []string{
		"__column_value(cast(-7 as integer))",
		"__column_value(cast(null as decimal(10, 2)))",
		"__column_value(cast('{1,2}' as array(int)))",
		"__column_value(cast('2024-03-04' as date))",
	} {
		n, err := ParseExpressionComplete(src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		c, ok := n.(*CastNode)
		if !ok || !c.Column {
			t.Fatalf("%s parsed as %T %+v, want a column-typed cast", src, n, n)
		}
		if got := c.String(); got != src {
			t.Errorf("%s renders back as %s", src, got)
		}
	}
	n, err := ParseExpressionComplete("__column_value(cast(t.i as integer))")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := n.(*FuncCallNode); !ok {
		t.Errorf("over a column reference the spelling is a function call, got %T", n)
	}
	if got := (&CastNode{Inner: &Lit{Value: "3", Kind: LitNumber}, TypeName: "integer"}).String(); got != "cast(3 as integer)" {
		t.Errorf("a plain cast renders %s", got)
	}
}
