// SPDX-License-Identifier: MIT

package sql

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A subquery failure the stage planner defers travels in a stage's filter
// TEXT and must come back as the DeferredErrorNode it was written as — its
// SQLSTATE and its sentence, a quote in the sentence included — or the
// worker compiles a call to a function nobody defines. Over anything but two
// string literals the spelling stays that call.
func TestDeferredErrorSpellingRoundTrips(t *testing.T) {
	for _, d := range []*DeferredErrorNode{
		{State: "2202H", Message: "sample percentage must be between 0 and 100"},
		{State: "22P02", Message: `invalid input syntax for type integer: "it's1"`},
		{State: "21000", Message: "more than one row returned by a subquery used as an expression"},
	} {
		src := (&CastNode{Inner: d, TypeName: "boolean"}).String()
		n, err := ParseExpressionComplete(src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		c, ok := n.(*CastNode)
		if !ok {
			t.Fatalf("%s parsed as %T", src, n)
		}
		back, ok := c.Inner.(*DeferredErrorNode)
		if !ok || *back != *d {
			t.Fatalf("%s parsed back as %T %+v, want %+v", src, c.Inner, c.Inner, d)
		}
		if got := n.String(); got != src {
			t.Errorf("%s renders back as %s", src, got)
		}
	}
	for _, src := range []string{
		"__deferred_error(t.s, 'm')",
		"__deferred_error('2202H')",
		"__deferred_error('2202H', 'm', 'x')",
	} {
		n, err := ParseExpressionComplete(src)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := n.(*FuncCallNode); !ok {
			t.Errorf("%s: the spelling over anything but two string literals is a call, got %T", src, n)
		}
	}
}

// A client's statement cannot spell a deferred failure: the doors refuse the
// call 42883 (PostgreSQL has no such function), as they refuse the
// column-typed cast's. A string literal, a comment or a relation that holds
// the name is not a call.
func TestDoorsRefuseTheDeferredErrorSpelling(t *testing.T) {
	for _, sql := range []string{
		"SELECT __deferred_error('42501', 'x') FROM t",
		"SELECT count(*) FROM t WHERE CAST(__DEFERRED_ERROR('2202H', 'x') AS BOOLEAN)",
	} {
		if got := sqlerr.StateOf(RefuseColumnValueCall(sql)); got != "42883" {
			t.Errorf("%s: refusal %q, want 42883", sql, got)
		}
	}
	for _, sql := range []string{
		"SELECT '__deferred_error(''x'')' AS t",
		"SELECT 1 -- __deferred_error('x')",
		"SELECT __deferred_error FROM t",
		"SELECT * FROM __deferred_error (a, b)",
	} {
		if err := RefuseColumnValueCall(sql); err != nil {
			t.Errorf("%s: refused %v, it is not a call", sql, err)
		}
	}
	if got := sqlerr.StateOf(RefuseColumnValueCall("SELECT __column_value(cast(1 as integer))")); got != "42883" {
		t.Errorf("the column-typed cast's spelling: %q, want 42883", got)
	}
}
