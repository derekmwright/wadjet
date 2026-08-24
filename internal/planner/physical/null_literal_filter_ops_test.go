package physical

import "testing"

// A comparison whose constant is a NULL literal is UNKNOWN on every row, so no
// row qualifies. The lowering handed the nil constant to a typed kernel
// instead, and every coercion there reads nil as the column type's ZERO:
// `WHERE c_i64 = NULL` answered the rows where the column is 0, `WHERE c_str =
// NULL` the rows where the string is empty, and every ordering operator was
// wrong the same way (#450).
//
// These assert the lowered operator SHAPE. internal/coordinator's
// predicate-semantics gate asserts the rows on both engines, and the kernel's
// own guard is in internal/engine/exec/kernel.

func TestComparisonAgainstNullLiteralMatchesNothing(t *testing.T) {
	cases := []struct {
		where string
		want  string
	}{
		{"k = NULL", "nothing()"},
		{"k <> NULL", "nothing()"},
		{"k != NULL", "nothing()"},
		{"k < NULL", "nothing()"},
		{"k <= NULL", "nothing()"},
		{"k > NULL", "nothing()"},
		{"k >= NULL", "nothing()"},
		{"NULL = k", "nothing()"},
		{"NULL > k", "nothing()"},
		// NOT UNKNOWN is UNKNOWN, so the negation matches nothing either.
		{"NOT (k = NULL)", "nothing()"},
		{"NOT (k <> NULL)", "nothing()"},
		// There is no pattern to match against.
		{"s LIKE NULL", "nothing()"},
		{"s NOT LIKE NULL", "nothing()"},
		// A NULL in an IN list can never make the test TRUE, so it drops out;
		// with nothing else in the list the whole test is UNKNOWN.
		{"k IN (NULL)", "nothing()"},
		{"k IN (1, NULL)", "in(k [1] negate=false)"},
		{"k IN (1, NULL, 2)", "in(k [1 2] negate=false)"},
		// NOT IN with a NULL member is FALSE or UNKNOWN for every row, never
		// TRUE — the rule that silently empties a result set.
		{"k NOT IN (NULL)", "nothing()"},
		{"k NOT IN (1, NULL)", "nothing()"},
		{"NOT (k IN (1, NULL))", "nothing()"},
		// BETWEEN with a NULL bound: that half is UNKNOWN, so the conjunction
		// is never TRUE and nothing qualifies. NOT BETWEEN keeps the other
		// half — a FALSE conjunct makes the conjunction FALSE whatever the
		// UNKNOWN one says, and the negation of FALSE is TRUE.
		{"k BETWEEN NULL AND 19", "nothing() AND kernel(k <= 19)"},
		{"k BETWEEN 10 AND NULL", "kernel(k >= 10) AND nothing()"},
		{"k NOT BETWEEN NULL AND 19", "or(nothing(), kernel(k > 19))"},
		{"k NOT BETWEEN 10 AND NULL", "or(kernel(k < 10), nothing())"},
		// An AND keeps the surviving conjunct; the match-nothing operator
		// makes the whole chain empty, which is the right answer either way.
		{"k = NULL AND k < 19", "nothing() AND kernel(k < 19)"},
		// An OR does not: `k = NULL OR k < 19` is TRUE wherever k < 19.
		{"k = NULL OR k < 19", "or(nothing(), kernel(k < 19))"},
	}
	for _, c := range cases {
		t.Run(c.where, func(t *testing.T) {
			got := describeAll(opsFor(t, c.where))
			if got != c.want {
				t.Errorf("%s lowered to %s, want %s", c.where, got, c.want)
			}
		})
	}
}

// TestNullLiteralInRawPredicateText: the raw-text predicate parser is the
// other producer of filter operators, used when a predicate carries no
// compiled AST. parseValue read an unquoted NULL as the four-character string
// "null" and compared the column against that.
func TestNullLiteralInRawPredicateText(t *testing.T) {
	for _, raw := range []string{
		"k = NULL",
		"k > null",
		"k BETWEEN NULL AND 19",
		"k IN (NULL)",
	} {
		t.Run(raw, func(t *testing.T) {
			got := describe(parseSimplePredicate(raw))
			if got == "" || !containsNothing(got) {
				t.Errorf("%s parsed to %s, want a match-nothing operator in it", raw, got)
			}
		})
	}
}

func containsNothing(s string) bool {
	for i := 0; i+len("nothing()") <= len(s); i++ {
		if s[i:i+len("nothing()")] == "nothing()" {
			return true
		}
	}
	return false
}
