package physical

import (
	"math"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A materialized IN-set rides the filter as TEXT, so every value has to
// survive rendering AND re-parsing. This asserts the round trip rather than
// the rendering alone: an inlined value that re-parses as something else is a
// wrong answer with no error attached (#524).
func TestInSetLiteralsSurviveTheRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string // what `x in (…)` must render to
	}{
		{"int64", int64(42), "x in (42)"},
		{"int64 negative", int64(-42), "x in (-42)"},
		{"int32", int32(7), "x in (7)"},
		{"int", 7, "x in (7)"},
		{"float64", float64(1.5), "x in (1.5)"},
		// A whole float8 keeps its decimal POINT. Rendered "2" it re-parses
		// as an INTEGER literal, and an integer literal is compared exactly:
		// the float8 9007199254740992 then failed to match the bigint
		// 9007199254740993 that PostgreSQL, comparing at float8, calls equal
		// (#615 F2). The round trip below is what this case exists for, and
		// "2" survived it as text while changing TYPE.
		{"float64 whole", float64(2), "x in (2.0)"},
		{"float64 whole negative", float64(-2), "x in (-2.0)"},
		{"float64 past 2^53", float64(9007199254740992), "x in (9007199254740992.0)"},
		// A value that only renders exactly at precision -1: the round-trip
		// check in inSetLiteral is what makes this a property and not a hope.
		{"float64 many digits", float64(0.1), "x in (0.1)"},
		{"float64 negative", float64(-2.25), "x in (-2.25)"},
		{"string", "abc", "x in ('abc')"},
		// The quote is the one that turns a set into a syntax error, or
		// worse, into a different set.
		{"string with a quote", "it's", "x in ('it''s')"},
		{"empty string", "", "x in ('')"},
		{"bool true", true, "x in (true)"},
		{"bool false", false, "x in (false)"},
		// A NULL in the list is what makes NOT IN's answer UNKNOWN (#370,
		// #507), so it has to reach the expression layer as a NULL and not
		// as the four-letter string.
		{"null", nil, "x in (null)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lit, ok := inSetLiteral(tc.in)
			if !ok {
				t.Fatalf("inSetLiteral(%#v) declined a value it should render", tc.in)
			}
			expr := &plansql.InExpr{Left: &plansql.ColRef{Column: "x"}, Values: []plansql.Node{lit}}
			got := expr.String()
			if got != tc.want {
				t.Fatalf("rendered %q, want %q", got, tc.want)
			}
			// The round trip: what the worker will parse must be the same
			// expression, not merely a parseable one.
			reparsed, err := plansql.ParseExpression(got)
			if err != nil {
				t.Fatalf("the rendered predicate does not re-parse: %v\n  text: %s", err, got)
			}
			if back := reparsed.String(); back != tc.want {
				t.Errorf("re-parsed to %q, want %q — the value changed meaning in the text", back, tc.want)
			}
		})
	}
}

// A value with no honest literal spelling is REFUSED, not approximated. The
// refusal routes the query to the coordinator-local pipeline, which reads the
// value with its real type instead of through the filter's text.
func TestInSetLiteralRefusesWhatItCannotSpell(t *testing.T) {
	for _, v := range []any{
		[]byte{1, 2, 3},
		struct{ A int }{1},
		map[string]int{"a": 1},
		[]int64{1, 2},
		// NaN and the infinities have no numeric literal in this dialect:
		// FormatFloat renders "NaN" / "+Inf", which re-parse as an
		// identifier or not at all.
		math.NaN(),
		math.Inf(1),
		math.Inf(-1),
		// float32 is refused for a different reason — see inSetLiteral and
		// #549: the IN-literal-list kernel compares a FLOAT32 column in
		// float64 space, so inlining would answer WRONG rather than fail.
		float32(1.5),
		float32(0.1),
	} {
		if lit, ok := inSetLiteral(v); ok {
			t.Errorf("inSetLiteral(%T) rendered %q instead of refusing", v, lit.String())
		}
	}
}

// An empty set is a real answer, not an absence: `x IN ()` is FALSE for every
// row and `x NOT IN ()` is TRUE for every row — including a row whose key is
// NULL, because an empty set has nothing to be UNKNOWN about. Neither renders
// as an empty value list (nothing parses that), so both render as constants.
func TestEmptyInSetRendersAsTheConstantItIs(t *testing.T) {
	for _, tc := range []struct {
		not  bool
		want string
	}{
		{false, "1 = 0"},
		{true, "1 = 1"},
	} {
		got := emptyInSetPredicate(tc.not).String()
		if got != tc.want {
			t.Errorf("emptyInSetPredicate(not=%v) = %q, want %q", tc.not, got, tc.want)
		}
		if _, err := plansql.ParseExpression(got); err != nil {
			t.Errorf("the empty-set predicate does not re-parse: %v\n  text: %s", err, got)
		}
	}
}

// findInSubqueryValue is what decides whether an IN predicate is the SUBQUERY
// form at all — a literal list must not be mistaken for one, and the parser
// wraps the subquery in parentheses.
func TestFindInSubqueryValue(t *testing.T) {
	subq := &plansql.SubqueryNode{SQL: "SELECT 1"}
	cases := []struct {
		name string
		in   *plansql.InExpr
		want bool
	}{
		{"bare subquery", &plansql.InExpr{Values: []plansql.Node{subq}}, true},
		{"parenthesized subquery", &plansql.InExpr{Values: []plansql.Node{&plansql.ParenNode{Inner: subq}}}, true},
		{"literal list", &plansql.InExpr{Values: []plansql.Node{
			&plansql.Lit{Value: "1", Kind: plansql.LitNumber},
			&plansql.Lit{Value: "2", Kind: plansql.LitNumber},
		}}, false},
		{"single literal", &plansql.InExpr{Values: []plansql.Node{
			&plansql.Lit{Value: "1", Kind: plansql.LitNumber},
		}}, false},
		{"empty", &plansql.InExpr{}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := findInSubqueryValue(tc.in) != nil; got != tc.want {
				t.Errorf("findInSubqueryValue = %v, want %v", got, tc.want)
			}
		})
	}
}
