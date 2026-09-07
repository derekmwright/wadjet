package logical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// LateralEmptyDefault is what one output column of an ungrouped-aggregate
// LATERAL reads for an outer row the lateral matched NOTHING for.
//
// PostgreSQL evaluates the subquery once per outer row, and an ungrouped
// aggregate over an EMPTY input still yields one row: `COUNT(*)` is 0 there,
// `SUM(x)` is NULL, and a SELECT item BUILT from them is that item evaluated
// over those values — `COUNT(*) + 1` is 1, `COUNT(*) = 0` is true,
// `CAST(COUNT(*) AS VARCHAR)` is '0' and `ARRAY[COUNT(*)]` is `{0}`.
//
// It is carried as an EXPRESSION, not as a value: `CASE WHEN <marker> IS NULL
// THEN <the item over an empty input> ELSE <the column> END`, compiled through
// the engine's own expression compiler like any other computed column. A value
// carrier needs a per-type writer, and the one this replaced could not write a
// varlen or a container vector: a STRING default emptied a MATCHED row and
// concatenated two values into the padded one.
type LateralEmptyDefault struct {
	// Column is the lateral's output name for this item.
	Column string
	// ExprSQL is the CASE above, rendered; empty when the item's empty-input
	// value is NULL, which is what the pad already writes, or when this pass
	// could not build it — in which case the column is left exactly as the
	// pad wrote it rather than given a value invented for it.
	ExprSQL string
	// Item is the item over an empty input on its own, without the CASE: the
	// value this column HAS on a padded row. `refuseUnorderedLateralOn` folds
	// a written ON over these to ask whether the padded row would pass it.
	Item plansql.Node
}

// lateralEmptyDefaults builds one rule per output column of an ungrouped
// aggregate lateral, in SELECT-list order.
func lateralEmptyDefaults(info *plansql.SelectInfo, marker string) []LateralEmptyDefault {
	if info == nil || marker == "" {
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
		d := LateralEmptyDefault{Column: name}
		if node := lateralItemAST(col); node != nil {
			if sub, ok := substituteEmptyInput(node); ok {
				d.Item = sub
				d.ExprSQL = fmt.Sprintf("CASE WHEN %s IS NULL THEN %s ELSE %s END",
					marker, sub.String(), name)
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

// refuseUnorderedLateralOn refuses a written ON over a LATERAL join this pass
// does NOT repair, unless the ON can be PROVEN to reject the padded row.
//
// PostgreSQL evaluates the lateral once per outer row, so for an outer row
// whose lateral input is empty there IS a lateral row — the item over an empty
// input, `V` — and the ON then decides that pair. This lowering has the two the
// other way round: the decorrelated join finds no build row, pads, and the ON
// never sees `V` at all. So on the unrepaired path this engine answers NULL for
// every such outer row, whatever the ON says, and PostgreSQL answers:
//
//	ON true  on (R, V)  →  V      ON false / NULL on (R, V)  →  NULL
//
// The two agree exactly where the ON REJECTS the pair. `ON s.n > 1` folds to
// `0 > 1` with no row to read, which is a definite FALSE, so it keeps
// answering as it does today. Everything else is refused:
//
//   - `ON s.n = 0` folds to TRUE — PostgreSQL's `Carol, 0` against this
//     engine's `Carol, NULL`, a wrong NUMBER on a row a pad manufactured.
//   - `ON o.id > 1` cannot be folded AT ALL: it reads an OUTER column, so
//     whether the padded row passes depends on the row. It was the loudest
//     miss of the first cut, which only looked at ONs mentioning the lateral:
//     PostgreSQL answers `Carol, 0` and this engine answered `Carol, NULL`
//     with no complaint.
//
// A join matches a pair only when its condition is TRUE, so UNKNOWN rejects
// exactly as FALSE does and `ON s.n = NULL` keeps answering. What the fold has
// to separate is "evaluated to UNKNOWN" from "could not be evaluated", and it
// does that STRUCTURALLY: a substituted condition still holding a column
// reference is not folded at all, because an evaluation that reads a column
// with no row would report the same nil either way and the second one is the
// wrong answer.
//
// It is scoped to a WRITTEN ON. `ON true` and the no-ON spelling reach
// lateralPadOnly and are repaired; an INNER join's written ON reaches
// lateralPadThenFilter, which MOVES it into the enclosing WHERE where it is
// evaluated ABOVE the default — PostgreSQL's order exactly. Only the outer
// spelling keeps the ON inside the join, below the default.
//
// The `laterNullExtends` decline is deliberately NOT refused here: it declines
// for a different reason (a later RIGHT or FULL join manufactures NULLs this
// repair cannot tell from the lateral's own) and its shapes are pinned in the
// arc D5 census with PostgreSQL's answer beside them.
func refuseUnorderedLateralOn(plan lateralEmptyInputCase, empty lateralEmptyInput, alias string,
	onExpr plansql.Node, onText string) error {
	if plan != lateralNoRepair {
		return nil
	}
	if !empty.ungroupedAggregate || len(empty.defaults) == 0 {
		return nil
	}
	if strings.TrimSpace(empty.onResidual) == "" {
		// No written ON: this is the `laterNullExtends` decline, whose shapes
		// are pinned rather than refused.
		return nil
	}
	if onExpr == nil && strings.TrimSpace(onText) == "" {
		return nil
	}
	if onExpr == nil {
		parsed, err := plansql.ParseExpression(onText)
		if err != nil {
			return nil
		}
		onExpr = parsed
	}
	items := make(map[string]plansql.Node, len(empty.defaults))
	for _, d := range empty.defaults {
		if d.Item != nil {
			items[strings.ToLower(d.Column)] = d.Item
		}
	}
	folded := plansql.RewriteExpr(onExpr, func(n plansql.Node) (plansql.Node, bool) {
		ref, ok := n.(*plansql.ColRef)
		if !ok {
			return nil, false
		}
		if ref.Table != "" && !strings.EqualFold(ref.Table, alias) {
			return nil, false
		}
		item, ok := items[strings.ToLower(ref.Column)]
		if !ok {
			return nil, false
		}
		return item, true
	})
	if onRejectsThePad(folded) {
		return nil
	}
	return sqlerr.New("0A000",
		"the join condition %q on a LATERAL subquery whose ungrouped aggregate has an "+
			"empty-input value cannot be answered: PostgreSQL evaluates the subquery per "+
			"outer row and applies the condition AFTER it, so an outer row the subquery "+
			"matched nothing for still offers the condition a row holding that value, and "+
			"this engine applies the condition first and would answer NULL where "+
			"PostgreSQL answers the value — move the condition to a WHERE clause, which "+
			"is evaluated after",
		strings.TrimSpace(onText))
}

// onRejectsThePad reports whether the folded condition PROVABLY rejects the
// padded row: a join matches only on TRUE, so both FALSE and UNKNOWN reject,
// and a rejected pair on an outer join is the outer row with the lateral's
// columns NULL — which is exactly what this lowering already produces.
//
// It folds only a CONSTANT condition. A substituted node still carrying a
// column reference is not evaluated at all: evaluating it with no row to read
// reports nil, which is indistinguishable from a genuine UNKNOWN, and reading
// that as "rejects" is how `ON o.id > 1` would silently answer NULL where
// PostgreSQL answers a value.
func onRejectsThePad(node plansql.Node) bool {
	val, ok := foldConstant(node)
	if !ok {
		return false
	}
	b, isBool := val.(bool)
	return val == nil || (isBool && !b)
}

// onFoldsToTrue reports whether a written join condition is a constant TRUE —
// `ON true`, `ON 1 = 1`, `ON 2 > 1`. Such a condition rejects nothing, so it
// says nothing about which pairs the join keeps and is not a residual the
// empty-input repair has to move or refuse.
//
// It folds through the expression compiler rather than matching the text,
// because "is this constant" is the compiler's question and a text match
// answers it for exactly one spelling.
func onFoldsToTrue(node plansql.Node, text string) bool {
	if node == nil {
		if strings.TrimSpace(text) == "" {
			return false
		}
		parsed, err := plansql.ParseExpression(text)
		if err != nil {
			return strings.EqualFold(strings.TrimSpace(text), "true")
		}
		node = parsed
	}
	val, ok := foldConstant(node)
	if !ok {
		return false
	}
	b, isBool := val.(bool)
	return isBool && b
}

// foldConstant evaluates a condition that reads no row, reporting ok=false
// when it reads one or cannot be evaluated at all. NULL comes back as a nil
// value with ok=true, which is a real answer and not a failure.
func foldConstant(node plansql.Node) (any, bool) {
	if node == nil || !isConstantCondition(node) {
		return nil, false
	}
	compiled, err := expr.Compile(node)
	if err != nil {
		return nil, false
	}
	var (
		val any
		ok  = true
	)
	func() {
		defer func() {
			if recover() != nil {
				ok = false
			}
		}()
		val = compiled.Eval(&batch.RecordBatch{Len: 1}, 0)
	}()
	return val, ok
}

// isConstantCondition reports whether node reads no column and calls no
// aggregate — the two things that need a row.
func isConstantCondition(node plansql.Node) bool {
	constant := true
	plansql.RewriteExpr(node, func(n plansql.Node) (plansql.Node, bool) {
		switch v := n.(type) {
		case *plansql.ColRef:
			constant = false
		case *plansql.FuncCallNode:
			if plansql.IsAggregate(v.Name) {
				constant = false
			}
		}
		return nil, false
	})
	return constant
}

// substituteEmptyInput replaces every aggregate call in node with its
// empty-input value: 0 for the COUNT family, NULL for everything else. What
// remains is the item over an empty input, and it carries no column reference
// — an ungrouped aggregate's SELECT list cannot.
//
// ok=false where an aggregate survives the walk (a shape the substitution does
// not reach), because a default that still reads a row is not a default.
func substituteEmptyInput(node plansql.Node) (plansql.Node, bool) {
	if node == nil {
		return nil, false
	}
	out := plansql.RewriteExpr(node, func(n plansql.Node) (plansql.Node, bool) {
		call, ok := n.(*plansql.FuncCallNode)
		if !ok || !plansql.IsAggregate(call.Name) {
			return nil, false
		}
		if strings.EqualFold(call.Name, "count") {
			return &plansql.Lit{Value: "0", Kind: plansql.LitNumber}, true
		}
		return &plansql.Lit{Kind: plansql.LitNull}, true
	})
	if out == nil || plansql.FindNestedAggregate(out) != nil {
		return nil, false
	}
	return out, true
}
