package logical

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// LateralEmptyDefault is what one output column of an ungrouped-aggregate
// LATERAL reads for an outer row the lateral matched NOTHING for.
//
// PostgreSQL evaluates the subquery once per outer row, and an ungrouped
// aggregate over an EMPTY input still yields one row: `COUNT(*)` is 0 there,
// `SUM(x)` is NULL, and a SELECT item BUILT from them is that item evaluated
// over those values — `COUNT(*) + 1` is 1, `COUNT(*) = 0` is true,
// `COALESCE(SUM(x), 0)` is 0, `NULLIF(COUNT(*), 2)` is 0 and
// `CASE WHEN COUNT(*) > 5 THEN 1 END` is NULL.
//
// So the default is not a literal 0 on a COUNT column. It is a CONSTANT per
// output column, folded at plan time from the item's own expression with each
// aggregate replaced by its empty-input value.
//
// Text, not a Go value, because it crosses the wire to a worker: the operator
// converts it at the vector's own type, so both paths read one spelling.
type LateralEmptyDefault struct {
	// Column is the lateral's output name for this item.
	Column string
	// Text is the folded constant rendered for the operator to parse; Null
	// says the item evaluates to NULL over the empty input, which is what
	// the LEFT pad already writes and needs no stamp.
	Text string
	Null bool
}

// lateralEmptyDefaults folds one default per output column of an ungrouped
// aggregate lateral, in SELECT-list order.
//
// A column whose value cannot be folded — anything this cannot compile or
// evaluate without a row — comes back NULL, which is what the pad writes
// anyway: the operator never stamps a NULL, so an unfoldable item is left
// exactly as it was rather than given a value invented for it.
func lateralEmptyDefaults(info *plansql.SelectInfo) []LateralEmptyDefault {
	if info == nil {
		return nil
	}
	out := make([]LateralEmptyDefault, 0, len(info.Columns))
	for _, col := range info.Columns {
		if col.Star {
			return nil // a star's column list is not knowable here
		}
		name := plansql.OutputColumnName(col)
		if name == "" {
			return nil
		}
		d := LateralEmptyDefault{Column: name, Null: true}
		if node := lateralItemAST(col); node != nil {
			if text, ok := foldOverEmptyInput(node); ok {
				d.Text, d.Null = text, false
			}
		}
		out = append(out, d)
	}
	return out
}

// lateralItemAST is the expression a SELECT item computes. A bare aggregate
// item may carry only its agg fields, so it is rebuilt as the call it is.
func lateralItemAST(col plansql.SelectColumn) plansql.Node {
	if col.ASTExpr != nil {
		return col.ASTExpr
	}
	if !col.IsAgg || col.AggFunc == "" {
		return nil
	}
	call := &plansql.FuncCallNode{Name: col.AggFunc, Distinct: col.AggDistinct}
	if col.AggArg == "*" {
		call.Star = true
		return call
	}
	if col.AggArgExpr != nil {
		call.Args = []plansql.Node{col.AggArgExpr}
	}
	return call
}

// foldOverEmptyInput replaces every aggregate call in node with its
// empty-input value and evaluates what is left.
//
// The COUNT family answers 0 over an empty input and every other aggregate
// answers NULL — that is the whole substitution. What remains has no column
// reference (an ungrouped aggregate's SELECT list cannot carry one), so it
// evaluates against a batch with no columns at all.
func foldOverEmptyInput(node plansql.Node) (string, bool) {
	substituted := plansql.RewriteExpr(node, func(n plansql.Node) (plansql.Node, bool) {
		call, ok := n.(*plansql.FuncCallNode)
		if !ok || !plansql.IsAggregate(call.Name) {
			return nil, false
		}
		if strings.EqualFold(call.Name, "count") {
			return &plansql.Lit{Value: "0", Kind: plansql.LitNumber}, true
		}
		return &plansql.Lit{Kind: plansql.LitNull}, true
	})
	if substituted == nil {
		return "", false
	}
	if plansql.FindNestedAggregate(substituted) != nil {
		return "", false // a shape the substitution did not reach
	}
	compiled, err := expr.Compile(substituted)
	if err != nil {
		return "", false
	}
	empty := &batch.RecordBatch{Len: 1}
	var val any
	func() {
		// A constant fold must never take the query down: an expression this
		// cannot evaluate leaves the column at the pad's NULL.
		defer func() { _ = recover() }()
		val = compiled.Eval(empty, 0)
	}()
	if val == nil {
		return "", false
	}
	return renderConstant(val)
}

// renderConstant is a folded value as the text the operator parses back at the
// vector's own type.
func renderConstant(v any) (string, bool) {
	switch t := v.(type) {
	case bool:
		return strconv.FormatBool(t), true
	case int:
		return strconv.FormatInt(int64(t), 10), true
	case int32:
		return strconv.FormatInt(int64(t), 10), true
	case int64:
		return strconv.FormatInt(t, 10), true
	case float32:
		return strconv.FormatFloat(float64(t), 'g', -1, 32), true
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64), true
	case string:
		return t, true
	case []byte:
		return string(t), true
	case fmt.Stringer:
		return t.String(), true
	}
	return "", false
}
