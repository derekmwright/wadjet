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

// Refuse a real IN list containing an unrepresentable finite literal with
// 22003 at plan time (#631), even for empty scans, NULL-only rows or unreachable
// predicates: PostgreSQL casts the array before reading rows.
// Use AnnotateScanColumns/inputColDecls to decide whether the operand is real.
// Run from Plan and PlanDistributed before dispatch so local, fast-path and DAG
// agree. Keep row-loop backstops for untyped projection aliases and worker
// fragments compiled from older coordinator plans.
// See docs/internals/real-in-list-plan-time-refusal.md for the design.

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
		return sqlerr.New("22003", "%s is out of range for type real",
			sqlerr.Quote(kernel.RealOverflowText(text)))
	}
	return nil
}

// realTypedNode asks whether the operand itself is REAL for the IN-array cast,
// not whether it contains a real column. Unary ± preserves REAL; adding an
// integer literal widens to DOUBLE PRECISION, while CAST AS REAL names REAL.
// Do not use nodeDeclaredType: it types projection vectors and deliberately
// widens unary FLOAT32 to FLOAT64. Keep this answer aligned with the runtime
// twin expr.realTypedOperand.
// See docs/internals/real-operand-array-cast-typing.md for the design.
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

// realTypedChoice reports whether a choice construct resolves to real:
// PostgreSQL resolves a CASE / COALESCE / GREATEST / LEAST / NULLIF / IF to
// the common type of its candidates, and it needs at least one REAL candidate
// and no candidate that forces a wider one.
//
// A numeric LITERAL is neither, and that is the correction the review forced.
// The first version required EVERY arm to be real, which left the four
// spellings a BI tool actually writes — `COALESCE(col, 0)`, `GREATEST(col, 0)`,
// `LEAST(col, 100)`, `CASE … ELSE 0 END` — answering no rows. Measured with
// pg_typeof on 17.11: all four are `real` there, and so is `COALESCE(r, 0.0)`
// with a decimal literal; only an explicitly typed wider arm
// (`COALESCE(r, CAST(0 AS DOUBLE PRECISION))`) is double precision. A constant
// in the numeric category is coerced INTO the common type rather than
// widening it — the same reason an untyped literal does not widen a comparison.
//
// A non-literal arm this walk cannot type still takes the whole construct
// off real, which is the conservative side for the shapes nobody can point at.
func realTypedChoice(arms []plansql.Node, decls colDecls) bool {
	if len(arms) == 0 {
		return false
	}
	sawReal := false
	for _, a := range arms {
		if a == nil {
			return false
		}
		switch {
		case realTypedNode(a, decls):
			sawReal = true
		case isNumericLiteralNode(a):
			// Neutral: it neither makes the construct real nor widens it.
		default:
			return false
		}
	}
	return sawReal
}

// isNumericLiteralNode reports whether a choice arm is a bare numeric constant
// — a number, or one behind parentheses or a unary sign. It is deliberately
// NOT `isConstNumericLitNode`'s job: this asks only "is this a constant of the
// numeric category", which is the question PostgreSQL's common-type resolution
// asks of a candidate before deciding it coerces rather than widens.
func isNumericLiteralNode(node plansql.Node) bool {
	switch n := node.(type) {
	case *plansql.ParenNode:
		return isNumericLiteralNode(n.Inner)
	case *plansql.UnaryOp:
		return (n.Op == "-" || n.Op == "+") && isNumericLiteralNode(n.Inner)
	case *plansql.Lit:
		return n.Kind == plansql.LitNumber
	}
	return false
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
