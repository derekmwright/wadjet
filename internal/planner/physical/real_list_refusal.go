package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The plan-time refusal for a `real IN (...)` list holding a literal that
// cannot be a real (#631 follow-up).
//
// PostgreSQL builds the array before it reads a row: `real IN (1e40, 3.1)`
// casts `{1e40,3.1}` to real[] during parse analysis, 1e40 does not fit, and
// the query fails with 22003 — whether or not any row would have been
// examined, and whether or not the predicate is even reachable:
//
//	WHERE r_val IS NULL AND r_val IN (1e40, 3.1)  -> ERROR 22003
//	WHERE r_key < 0     AND r_val IN (1e40, 3.1)  -> ERROR 22003
//
// Both evaluation paths raised this from inside the ROW LOOP instead, which
// makes an error that PostgreSQL guarantees depend on the data: the kernel
// resolves on the first BATCH, so an empty scan never raised, and the row
// evaluator's binding raises on the first non-NULL row, so a predicate that
// only ever meets NULLs never raised either. Both shapes above answered 0 rows
// on at least one path.
//
// Refusing here fixes both at once, and at the layer that can: the planner
// holds the catalog's declared types (AnnotateScanColumns leaves them on the
// scan nodes, and inputColDecls walks them up to the filter), which is exactly
// what decides whether the list is a real[] cast at all. It runs from Plan and
// PlanDistributed, so the single-process engine, the small-query fast path and
// the stage DAG all refuse identically, before any task is dispatched.
//
// The row-loop raises are KEPT as backstops. They cover the shapes this pass
// cannot see — a predicate whose column resolves through a projection alias the
// planner cannot type, a filter compiled from a fragment by a worker running
// an older coordinator's plan — and a second refusal of a query already
// refused costs nothing.

// refuseUnrepresentableRealInList reports the first `real IN (...)` list in the
// plan holding a finite literal past real's range.
func refuseUnrepresentableRealInList(root *logical.Node) error {
	return walkRealInLists(root)
}

func walkRealInLists(n *logical.Node) error {
	if n == nil {
		return nil
	}
	if len(n.Predicates) > 0 {
		d := inputColDecls(n)
		for i := range n.Predicates {
			if err := refuseRealInNode(n.Predicates[i].ASTExpr, d); err != nil {
				return err
			}
		}
	}
	for _, c := range n.Children {
		if err := walkRealInLists(c); err != nil {
			return err
		}
	}
	return nil
}

// refuseRealInNode walks one predicate's AST for the shape. Only the boolean
// connectives are descended: an IN list nested inside a scalar expression
// (a CASE arm, a function argument) is not lowered to the set kernel and is
// not what PostgreSQL's array cast applies to either.
func refuseRealInNode(node plansql.Node, decls colDecls) error {
	switch n := node.(type) {
	case nil:
		return nil
	case *plansql.AndNode:
		if err := refuseRealInNode(n.Left, decls); err != nil {
			return err
		}
		return refuseRealInNode(n.Right, decls)
	case *plansql.OrNode:
		if err := refuseRealInNode(n.Left, decls); err != nil {
			return err
		}
		return refuseRealInNode(n.Right, decls)
	case *plansql.NotNode:
		return refuseRealInNode(n.Inner, decls)
	case *plansql.ParenNode:
		return refuseRealInNode(n.Inner, decls)
	case *plansql.InExpr:
		return refuseRealInList(n, decls)
	}
	return nil
}

// refuseRealInList applies PostgreSQL's rule to one IN list: the array cast
// happens only when the probed operand is REAL-typed and the list has more
// than one member, all of them constants — the same conditions
// expr.bindRealLitList and kernel.ResolveInFilterKernelArity narrow under, so
// a query this refuses is exactly a query that would have narrowed.
func refuseRealInList(n *plansql.InExpr, decls colDecls) error {
	if len(n.Values) < 2 {
		// Arity 1 WIDENS to double, where a finite over-range literal is an
		// ordinary double that simply matches nothing — PostgreSQL raises
		// nothing for `real IN (1e40)`.
		return nil
	}
	if !realTypedNode(n.Left, decls) {
		return nil
	}
	for _, v := range n.Values {
		text, ok := realListLiteralText(v)
		if !ok {
			// A non-constant member takes the array away entirely:
			// PostgreSQL plans an OR of widened scalar comparisons, and no
			// cast to real[] happens for any member.
			return nil
		}
		if text == "" || !kernel.RealLitTextUnrepresentable(text) {
			continue
		}
		return sqlerr.New("22003", "%q is out of range for type real",
			kernel.RealOverflowText(text))
	}
	return nil
}

// realTypedNode reports whether an operand's own type is REAL, which is what
// decides the array cast — not whether it is a bare column.
//
// PostgreSQL resolves the list's element type over the members AND the probed
// expression, so any real-typed left operand pulls the array to real[]
// (EXPLAIN VERBOSE, postgres:17):
//
//	-r_val IN (-3.1, -7.1)          -> ((- r_val) = ANY ('{-3.1,-7.1}'::real[]))
//	CAST(d_val AS REAL) IN (3.1,…)  -> ((d_val)::real = ANY ('{3.1,7.1}'::real[]))
//	(r_val + 0) IN (3.1, 7.1)       -> (… = ANY ('{3.1,7.1}'::double precision[]))
//
// The third is why this cannot simply follow the operand down to a column: an
// integer literal added to a real gives DOUBLE PRECISION in PostgreSQL
// (pg_typeof(r_val + 0) is `double precision`), so that shape must stay
// widened. Unary ± is the one operator that preserves real.
//
// It is NOT nodeDeclaredType. That function deliberately collapses FLOAT32 to
// FLOAT64 for unary ± — it types the COLUMN a projection allocates, where the
// engine materializes `-f32col` as a float64 — and reading it here would
// answer "double" for the very shape PostgreSQL calls real. The two questions
// are different; expr.realTypedOperand is this one's runtime twin and the two
// must keep answering alike.
func realTypedNode(node plansql.Node, decls colDecls) bool {
	switch n := node.(type) {
	case *plansql.ParenNode:
		return realTypedNode(n.Inner, decls)
	case *plansql.ColRef:
		if decls.isFieldPath(n) {
			// A ROW FIELD of FLOAT32 reads through the boxed path exactly as
			// a column does, and expr.realTypedOperand has always said so.
			// The two twins disagreeing here is #654's "latent asymmetry":
			// the plan-time refusal was missed and only the row-loop backstop
			// raised.
			f, ok := decls.field(n)
			return ok && f.Type == parquet.TypeFloat32
		}
		t, ok := decls.colType(n)
		return ok && t == parquet.TypeFloat32
	case *plansql.CastNode:
		switch strings.ToLower(strings.TrimSpace(n.TypeName)) {
		case "real", "float4":
			return true
		}
	case *plansql.UnaryOp:
		if n.Op == "-" || n.Op == "+" {
			return realTypedNode(n.Inner, decls)
		}
	case *plansql.BinaryOp:
		// real OP real is real; anything else widens. `r * CAST(1 AS REAL)`
		// is real and `r * 1` is double precision — both verified with
		// pg_typeof, and they are the pair that says this must test BOTH
		// sides rather than follow one down to a column.
		switch n.Op {
		case "+", "-", "*", "/":
			return realTypedNode(n.Left, decls) && realTypedNode(n.Right, decls)
		}
	case *plansql.CaseNode:
		return realTypedChoice(caseArmNodes(n), decls)
	case *plansql.FuncCallNode:
		return realTypedFuncNode(n, decls)
	}
	return false
}

// realTypedChoice reports whether every arm of a choice construct is real,
// which is what makes the construct real: PostgreSQL resolves a CASE /
// COALESCE / GREATEST / LEAST / NULLIF / IF to the common type of its
// candidates, and `real` with `real` is `real` (pg_typeof, verified live for
// all six).
//
// An arm that is not real — including one this walk cannot type — makes the
// whole construct not-real, which is the conservative side: it leaves the list
// widened exactly as it was before.
func realTypedChoice(arms []plansql.Node, decls colDecls) bool {
	if len(arms) == 0 {
		return false
	}
	for _, a := range arms {
		if a == nil || !realTypedNode(a, decls) {
			return false
		}
	}
	return true
}

// caseArmNodes is a CASE's candidate list: the THEN results and the ELSE. A
// missing ELSE is an implicit untyped NULL, which contributes no type — so it
// is skipped here rather than counted as a non-real arm, the way
// expr.CommonDeclType skips it.
func caseArmNodes(n *plansql.CaseNode) []plansql.Node {
	arms := make([]plansql.Node, 0, len(n.Whens)+1)
	for _, w := range n.Whens {
		arms = append(arms, w.Result)
	}
	if n.Else != nil {
		arms = append(arms, n.Else)
	}
	return arms
}

// realTypedFuncNode is the FUNCTION half of the resolved-type rule.
//
// Two families, and the list is short because PostgreSQL's is: ABS is the one
// scalar function with a float4 overload (`abs(real)` is real; CEIL, FLOOR,
// SQRT, ROUND, TRUNC and SIGN over a real are double precision or numeric —
// each measured, which is the same measurement scalarFnDeclaredNumericDomain
// records for the integer domain), and the CHOICE functions the registry
// already names through Ret.SameAsArgs mirror their arguments.
//
// An AGGREGATE needs no arm: MIN/MAX/SUM over a real declare FLOAT32 for their
// output column, so the operand a HAVING sees is a bare ColRef of that column
// and the ColRef arm answers it. AVG is double precision in PostgreSQL and its
// output column is not FLOAT32 here either, so it stays widened without a rule.
func realTypedFuncNode(n *plansql.FuncCallNode, decls colDecls) bool {
	if strings.EqualFold(strings.TrimSpace(n.Name), "abs") {
		return len(n.Args) == 1 && realTypedNode(n.Args[0], decls)
	}
	idx, poly := expr.DefaultRegistry.ReturnType(n.Name).SameAsArgs(len(n.Args))
	if !poly {
		return false
	}
	arms := make([]plansql.Node, 0, len(idx))
	for _, i := range idx {
		if i >= 0 && i < len(n.Args) {
			arms = append(arms, n.Args[i])
		}
	}
	return realTypedChoice(arms, decls)
}

// realListLiteralText unwraps a member to the numeric text the refusal reads,
// mirroring expr.realListMember: a bare literal, a CAST to REAL over one, or
// either behind unary ±, keeps the list at real width; anything else is not a
// constant this rule applies to.
//
// The sign travels WITH the text. A negated member used to make the whole
// member "not a literal", which disarmed the check for the entire list —
// `r_val IN (-1.0, 1e40)` was not refused at all — and it is also what the
// 22003 message has to print: PostgreSQL names
// "-10000000000000000000000000000000000000000" for `IN (-1e40, 3.1)`.
//
// An empty text is a member with no number in it (NULL, a quoted string), which
// contributes nothing and must not be mistaken for "not a constant" — hence the
// second result rather than a nil check.
func realListLiteralText(e plansql.Node) (string, bool) {
	switch n := e.(type) {
	case *plansql.Lit:
		if n.Kind != plansql.LitNumber {
			return "", true
		}
		return n.Value, true
	case *plansql.ParenNode:
		return realListLiteralText(n.Inner)
	case *plansql.CastNode:
		switch strings.ToLower(strings.TrimSpace(n.TypeName)) {
		case "real", "float4":
			return realListLiteralText(n.Inner)
		}
	case *plansql.UnaryOp:
		switch n.Op {
		case "+":
			return realListLiteralText(n.Inner)
		case "-":
			text, ok := realListLiteralText(n.Inner)
			if !ok || text == "" {
				return text, ok
			}
			return negateNumericText(text), true
		}
	}
	return "", false
}

// negateNumericText flips a numeric literal's sign in its TEXT, which is where
// the exactness lives: the literal may be wider than a float64 (1e400), so
// negating a parsed value would lose it.
func negateNumericText(text string) string {
	t := strings.TrimSpace(text)
	switch {
	case strings.HasPrefix(t, "-"):
		return t[1:]
	case strings.HasPrefix(t, "+"):
		return "-" + t[1:]
	default:
		return "-" + t
	}
}
