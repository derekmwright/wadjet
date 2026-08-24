package physical

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// extractFilterOps lowers a compiled WHERE expression to vectorized filter
// operators, and it returned the WRONG operator for a negated predicate:
// `NOT (<vectorizable predicate>)` came back as the inner predicate's own
// operators, UNNEGATED, so `WHERE NOT (k = 131)` was executed as
// `WHERE k = 131` — the complement of the answer, on every type and on both
// engines, silently (#461).
//
// The assertions here are on the operator SHAPE, which is where the defect
// lives. internal/coordinator's two-path gate asserts the rows, on both
// engines.

// opsFor compiles a WHERE fragment and returns what the planner lowers it to.
func opsFor(t *testing.T, where string) []exec.UnaryOperator {
	t.Helper()
	node, err := plansql.ParseExpression(where)
	if err != nil {
		t.Fatalf("parsing %q: %v", where, err)
	}
	compiled, err := expr.Compile(node)
	if err != nil {
		t.Fatalf("compiling %q: %v", where, err)
	}
	return extractFilterOps(compiled, false)
}

var opNames = map[exec.CompareOp]string{
	exec.OpEq: "=", exec.OpNe: "!=", exec.OpLt: "<",
	exec.OpLe: "<=", exec.OpGt: ">", exec.OpGe: ">=",
}

// describe renders one operator as a comparable string, so a test states the
// shape it expects instead of type-switching in every case.
func describe(op exec.UnaryOperator) string {
	switch f := op.(type) {
	case *exec.KernelFilter:
		return fmt.Sprintf("kernel(%s %s %v)", f.ColName, opNames[f.Op], f.Value)
	case *exec.InFilter:
		return fmt.Sprintf("in(%s %v negate=%v)", f.ColName, f.Values, f.Negate)
	case *exec.LikeFilter:
		return fmt.Sprintf("like(%s %q negate=%v)", f.ColName, f.Pattern, f.Negate)
	case *exec.NullCheckFilter:
		return fmt.Sprintf("nullcheck(%s isnull=%v)", f.ColName, f.CheckNull)
	case *exec.MatchNothingFilter:
		return "nothing()"
	case *exec.ColColFilter:
		return fmt.Sprintf("colcol(%s %s %s)", f.LeftCol, opNames[f.Op], f.RightCol)
	case *exec.OrFilter:
		return fmt.Sprintf("or(%s, %s)", describe(f.Left), describe(f.Right))
	case *exec.ChainFilter:
		out := "chain("
		for i, inner := range f.Ops {
			if i > 0 {
				out += ", "
			}
			out += describe(inner)
		}
		return out + ")"
	default:
		return fmt.Sprintf("%T", op)
	}
}

func describeAll(ops []exec.UnaryOperator) string {
	if ops == nil {
		return "<row-at-a-time>"
	}
	out := ""
	for i, op := range ops {
		if i > 0 {
			out += " AND "
		}
		out += describe(op)
	}
	return out
}

// TestNotNegatesTheExtractedFilterOps: the operators a NOT lowers to must be
// the NEGATION of the inner predicate's. Before the fix every case here
// produced the un-negated operator, which is the complement of the answer.
func TestNotNegatesTheExtractedFilterOps(t *testing.T) {
	cases := []struct {
		where string
		want  string
	}{
		// The six comparisons invert. Under three-valued logic NOT (a = b) is
		// TRUE exactly where a <> b is TRUE (both are UNKNOWN when either
		// side is NULL), and the kernels already skip NULL rows.
		{"NOT (k = 131)", "kernel(k != 131)"},
		{"NOT (k <> 131)", "kernel(k = 131)"},
		{"NOT (k != 131)", "kernel(k = 131)"},
		{"NOT (k < 10)", "kernel(k >= 10)"},
		{"NOT (k <= 10)", "kernel(k > 10)"},
		{"NOT (k > 10)", "kernel(k <= 10)"},
		{"NOT (k >= 10)", "kernel(k < 10)"},
		// A flipped literal keeps its flip through the negation.
		{"NOT (10 < k)", "kernel(k <= 10)"},
		// Set membership, range and pattern flip their own negation flag.
		{"NOT (k IN (1, 2, 3))", "in(k [1 2 3] negate=true)"},
		{"NOT (k NOT IN (1, 2, 3))", "in(k [1 2 3] negate=false)"},
		{"NOT (k BETWEEN 10 AND 19)", "or(kernel(k < 10), kernel(k > 19))"},
		{"NOT (k NOT BETWEEN 10 AND 19)", "kernel(k >= 10) AND kernel(k <= 19)"},
		{"NOT (s LIKE 'a%')", `like(s "a%" negate=true)`},
		{"NOT (s NOT LIKE 'a%')", `like(s "a%" negate=false)`},
		{"NOT (k IS NULL)", "nullcheck(k isnull=false)"},
		{"NOT (k IS NOT NULL)", "nullcheck(k isnull=true)"},
		// De Morgan, both directions. NOT (a AND b) is NOT a OR NOT b in
		// Kleene logic too, and OR is the union of the two selections.
		{"NOT (k = 1 AND j = 2)", "or(kernel(k != 1), kernel(j != 2))"},
		{"NOT (k = 1 OR j = 2)", "kernel(k != 1) AND kernel(j != 2)"},
		// Two negations cancel.
		{"NOT (NOT (k = 131))", "kernel(k = 131)"},
		// A column-column comparison inverts the same way.
		{"NOT (k = j)", "colcol(k != j)"},
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

// TestNotOverAnUnvectorizablePredicateStaysRowAtATime: what cannot be negated
// must fall back to the row evaluator, which is three-valued-correct, rather
// than losing the negation. A nil return is the fallback signal.
func TestNotOverAnUnvectorizablePredicateStaysRowAtATime(t *testing.T) {
	for _, where := range []string{
		"NOT (ABS(k) = 1)",
		"NOT (k + 1 = 2)",
		"NOT (k = 1 AND ABS(j) = 2)",
	} {
		t.Run(where, func(t *testing.T) {
			if ops := opsFor(t, where); ops != nil {
				t.Errorf("%s lowered to %s, want the row-at-a-time fallback",
					where, describeAll(ops))
			}
		})
	}
}
