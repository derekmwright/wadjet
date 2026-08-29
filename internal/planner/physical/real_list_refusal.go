package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
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
// happens only when the probed operand is a REAL column and the list has more
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
	col, ok := n.Left.(*plansql.ColRef)
	if !ok {
		return nil
	}
	if t, ok := decls.colType(col); !ok || t != parquet.TypeFloat32 {
		return nil
	}
	for _, v := range n.Values {
		lit, ok := realListLiteral(v)
		if !ok {
			// A non-constant member takes the array away entirely:
			// PostgreSQL plans an OR of widened scalar comparisons, and no
			// cast to real[] happens for any member.
			return nil
		}
		if lit == nil || lit.Kind != plansql.LitNumber {
			continue
		}
		if !kernel.RealLitTextOverflow(lit.Value) {
			continue
		}
		return sqlerr.New("22003", "%q is out of range for type real",
			kernel.RealOverflowText(lit.Value))
	}
	return nil
}

// realListLiteral unwraps a member to its literal, mirroring
// expr.realListMember: a bare literal, or a CAST to REAL over one, keeps the
// list at real width; anything else is not a constant this rule applies to.
// A NULL member is a literal and contributes nothing, which is why the second
// result distinguishes "not a literal" from "a literal with no number in it".
func realListLiteral(e plansql.Node) (*plansql.Lit, bool) {
	switch n := e.(type) {
	case *plansql.Lit:
		return n, true
	case *plansql.ParenNode:
		return realListLiteral(n.Inner)
	case *plansql.CastNode:
		switch strings.ToLower(strings.TrimSpace(n.TypeName)) {
		case "real", "float4":
			return realListLiteral(n.Inner)
		}
	}
	return nil, false
}
