package expr

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The row evaluator's half of the #147 guard.
//
// `ColRef.Eval` answers nil for a column it cannot resolve, so a predicate
// over a name the batch does not carry is UNKNOWN on every row — and a WHERE
// admits only TRUE, so the filter returns NOTHING. That is indistinguishable
// from genuinely empty data, which is why the VECTORIZED filter has refused
// it since #147 (`filter column %q does not exist in the input schema`,
// exec.KernelFilter). The row path had no such refusal, so every defect that
// handed a filter the wrong NAME arrived as a silent zero-row answer instead
// of an error: a renamed CTE column on the stage DAG was one (#653), and the
// next one will be a different lowering with the same symptom.
//
// The guard is a NAME test, not a value test, so it cannot fire for a column
// that is legitimately NULL: such a column is in the schema and resolves. Its
// three spellings are ColRef's own (ResolveColumnRef), so a table-qualified
// reference and a ROW field path resolve here exactly as the evaluator
// resolves them.

// CheckFilterColumns returns a 42703 error naming the first reference that
// resolves to no column of b. Callers run it once, on the first batch.
func CheckFilterColumns(b *batch.RecordBatch, refs []string) error {
	if b == nil || len(b.Columns) == 0 || len(refs) == 0 {
		return nil
	}
	for _, name := range refs {
		if idx, _ := ResolveColumnRef(b, name); idx < 0 {
			return sqlerr.New("42703",
				"filter column %q does not exist in the input schema (available: %s)",
				name, strings.Join(batchColumnNames(b), ", "))
		}
	}
	return nil
}

// FilterColumnRefs lists the column references an expression reads, spelled
// as WRITTEN (`c_row.b` stays `c_row.b`, which is what ResolveColumnRef
// expects).
//
// ok=false means the expression carries a node whose references this walker
// cannot enumerate — a subquery, EXISTS, ANY/ALL, a window function, or a node
// added since. Those are exactly the shapes where a name may legitimately
// resolve OUTSIDE the batch (a correlated outer reference, a subquery's own
// inner columns), so the caller must skip the guard rather than guess.
func FilterColumnRefs(n plansql.Node) ([]string, bool) {
	out := make([]string, 0, 4)
	if !collectRefs(n, &out) {
		return nil, false
	}
	return out, true
}

func collectRefs(n plansql.Node, out *[]string) bool {
	if n == nil {
		return true
	}
	switch v := n.(type) {
	case *plansql.ColRef:
		if name := v.String(); name != "" {
			*out = append(*out, name)
		}
		return true
	case *plansql.Lit, *plansql.IntervalLit:
		return true
	case *plansql.BinaryOp:
		return collectRefs(v.Left, out) && collectRefs(v.Right, out)
	case *plansql.CmpExpr:
		return collectRefs(v.Left, out) && collectRefs(v.Right, out)
	case *plansql.AndNode:
		return collectRefs(v.Left, out) && collectRefs(v.Right, out)
	case *plansql.OrNode:
		return collectRefs(v.Left, out) && collectRefs(v.Right, out)
	case *plansql.UnaryOp:
		return collectRefs(v.Inner, out)
	case *plansql.NotNode:
		return collectRefs(v.Inner, out)
	case *plansql.ParenNode:
		return collectRefs(v.Inner, out)
	case *plansql.CastNode:
		return collectRefs(v.Inner, out)
	case *plansql.IsExpr:
		return collectRefs(v.Left, out)
	case *plansql.LikeExpr:
		return collectRefs(v.Left, out) && collectRefs(v.Pattern, out)
	case *plansql.BetweenExpr:
		return collectRefs(v.Left, out) && collectRefs(v.Low, out) && collectRefs(v.High, out)
	case *plansql.InExpr:
		if !collectRefs(v.Left, out) {
			return false
		}
		for _, e := range v.Values {
			if !collectRefs(e, out) {
				return false
			}
		}
		return true
	case *plansql.FuncCallNode:
		for _, a := range v.Args {
			if !collectRefs(a, out) {
				return false
			}
		}
		return true
	case *plansql.CaseNode:
		if !collectRefs(v.Subject, out) || !collectRefs(v.Else, out) {
			return false
		}
		for _, w := range v.Whens {
			if !collectRefs(w.Cond, out) || !collectRefs(w.Result, out) {
				return false
			}
		}
		return true
	case *plansql.ArrayLitNode:
		for _, e := range v.Elements {
			if !collectRefs(e, out) {
				return false
			}
		}
		return true
	case *plansql.TupleNode:
		for _, e := range v.Elements {
			if !collectRefs(e, out) {
				return false
			}
		}
		return true
	default:
		// SubqueryNode, ExistsNode, AnyAllExpr, WindowFuncNode, StarNode and
		// anything added later.
		return false
	}
}
