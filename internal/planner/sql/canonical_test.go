package sql

import "testing"

// TestExprIdentityErasesOnlySpelling is the contract: two spellings of ONE
// expression share an identity, and two DIFFERENT expressions never do. The
// second half is the one that matters — an identity that collapsed
// associativity would silently make `g - (1 - 2)` and `g - 1 - 2` one group
// key.
func TestExprIdentityErasesOnlySpelling(t *testing.T) {
	same := [][]string{
		{"g + 1", "(g + 1)", "((g + 1))", "g+1", "( g+1 )", "((g) + 1)", "G + 1", "(G) + 1"},
		{"g", "(g)", "((g))", "G", " g "},
		{"substr(c_str, 1, 4)", "SUBSTR(c_str, 1, 4)", "(SUBSTR(c_str, 1, 4))", "substr(C_STR, 1, 4)"},
		{"a.b", "(a.b)", "A.B"},
		{"g - 1 - 2", "(g - 1) - 2", "((g - 1)) - 2"},
		{"c_str || 'x'", "(c_str || 'x')"},
		{"cast(g as bigint)", "CAST(g AS BIGINT)", "(cast(G as BigInt))"},
		{"-g", "(-g)", "-(g)"},
	}
	for _, group := range same {
		want := identityOf(t, group[0])
		for _, spelling := range group[1:] {
			if got := identityOf(t, spelling); got != want {
				t.Errorf("%q has identity %q, want %q (same expression as %q)",
					spelling, got, want, group[0])
			}
		}
	}

	differ := [][2]string{
		{"g - 1 - 2", "g - (1 - 2)"},
		{"a * (b + c)", "a * b + c"},
		{"g + 1", "g + 2"},
		{"g + 1", "h + 1"},
		{"g + 1", `"g + 1"`},
		{"-g + 1", "-(g + 1)"},
		{"a - b - c", "a - (b - c)"},
		{"not a and b", "not (a and b)"},
	}
	for _, pair := range differ {
		if l, r := identityOf(t, pair[0]), identityOf(t, pair[1]); l == r {
			t.Errorf("%q and %q share identity %q — they are different expressions",
				pair[0], pair[1], l)
		}
	}
}

// TestExprIdentityIsStable pins the exact rendering, because it becomes a map
// key that crosses package boundaries and a change to it is a change to what
// two planners agree on.
func TestExprIdentityIsStable(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"g + 1", "g + 1"},
		{"(g + 1)", "g + 1"},
		{"g + 1 + 2", "(g + 1) + 2"},
		{"g + (1 + 2)", "g + (1 + 2)"},
		{"g", "g"},
		{`"g + 1"`, `"g + 1"`},
		{"SUBSTR(c_str, 1, 4)", "substr(c_str, 1, 4)"},
		{"g * 2 - 1", "(g * 2) - 1"},
	} {
		if got := identityOf(t, tc.in); got != tc.want {
			t.Errorf("ExprIdentity(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestGroupKeyNameStripsDelimitersAndOuterParens is #725's half: a delimited
// identifier is a NAME, and the name is what the aggregate publishes. Case is
// preserved, because a batch column is matched by bytes.
func TestGroupKeyNameStripsDelimitersAndOuterParens(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`"g + 1"`, "g + 1"},
		{`"g+1"`, "g+1"},
		{"g", "g"},
		{"(g)", "g"},
		{"(g + 1)", "g + 1"},
		{"((g + 1))", "g + 1"},
		{"g + 1", "g + 1"},
		{`"MixedCase"`, "MixedCase"},
		{"o.o_custkey", "o.o_custkey"},
		{`"id.orig_h"`, "id.orig_h"},
		// The parser already lower-cases a function name, so the rendered
		// name is the lower-cased one on every spelling.
		{"SUBSTR(c_str, 1, 4)", "substr(c_str, 1, 4)"},
	} {
		n, err := ParseExpression(tc.in)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.in, err)
		}
		if got := GroupKeyName(n); got != tc.want {
			t.Errorf("GroupKeyName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestExprIdentityIsIdempotent: the identity of an identity is itself, so a
// site that re-parses a stored key text lands on the same answer as the one
// that stored it.
func TestExprIdentityIsIdempotent(t *testing.T) {
	for _, in := range []string{
		"g + 1", "(g + 1)", "g + 1 + 2", "g + (1 + 2)", "substr(c_str, 1, 4)",
		"g", `"g + 1"`, "cast(g as bigint)", "-g", "g * 2 - 1", "c_str || 'x'",
	} {
		once := identityOf(t, in)
		twice := identityOf(t, once)
		if once != twice {
			t.Errorf("ExprIdentity(%q) = %q, but re-parsing that gives %q", in, once, twice)
		}
	}
}

func identityOf(t *testing.T, s string) string {
	t.Helper()
	n, err := ParseExpression(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return ExprIdentity(n)
}
