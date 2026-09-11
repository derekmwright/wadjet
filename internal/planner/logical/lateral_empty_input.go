package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// An ungrouped LATERAL aggregate yields one row PER OUTER ROW even on empty input:
// COUNT is 0, other aggregates NULL (#767 part 1).
// Decorrelation injects the correlation key into GROUP BY and creates only existing
// groups; restoring empty-input semantics requires LEFT padding plus COUNT defaults.
// Use lateralEmptyInputPlan's ON cases when choosing LEFT and default substitution;
// a forced LEFT with unconditional defaults does not preserve every written ON.
// A subquery the QUERY grouped is untouched: its empty input yields NO row,
// so an INNER join correctly drops an unmatched outer row.
// See docs/internals/lateral-ungrouped-empty-input.md for the design.
type lateralEmptyInput struct {
	// ungroupedAggregate is the whole trigger: the lateral's SELECT list
	// holds an aggregate and the QUERY wrote no GROUP BY of its own.
	ungroupedAggregate bool
	// countOutputs are the output names whose empty-input value is 0 rather
	// than NULL — the COUNT family. Kept for the shapes that ask "does this
	// lateral default anything at all"; the VALUES are in defaults.
	countOutputs []string
	// defaults is one folded constant per output column: what the item
	// evaluates to over an empty input. `COUNT(*)+1` is 1, `COUNT(*)=0` is
	// true, `COALESCE(SUM(x),0)` is 0, `NULLIF(COUNT(*),2)` is 0 and
	// `CASE WHEN COUNT(*)>5 THEN 1 END` is NULL — a literal 0 on the COUNT
	// column is none of those. See lateralEmptyDefaults.
	defaults []LateralEmptyDefault
	// padMarker is the column whose NULL says a row is the LEFT pad this
	// repair manufactures: the name the lateral publishes its correlation
	// key under, which the join keys on, so a NULL there matches nothing.
	padMarker string
	// correlationCond is the condition the DECORRELATION produced (the
	// promoted correlated equalities), and onResidual / onResidualExpr are
	// the `ON` the QUERY WROTE, `true` excluded. They are kept apart because
	// the repair may make the join LEFT on the correlation ALONE and move the
	// written ON somewhere the padded row still has to pass it.
	correlationCond string
	onResidual      string
	onResidualExpr  plansql.Node
}

// lateralEmptyInputCase preserves evaluation of the ungrouped lateral BEFORE ON.
// No written ON / ON true: LEFT on correlation and default COUNT, for INNER and LEFT
// alike (lateralPadOnly). INNER written ON: LEFT on correlation, then move ON into
// WHERE so the same default substitution tests the padded row (lateralPadThenFilter).
// OUTER written ON must retain rejected pairs with every lateral column NULL;
// this pass cannot perform that per-column CASE and leaves the join (lateralNoRepair).
// An ON rejecting the default agrees; an ON accepting it requires the separate refusal.
// A later RIGHT/FULL join disables ALL repair first: its manufactured NULLs cannot
// be distinguished from lateral padding by COALESCE or the moved ON.
// See docs/internals/lateral-empty-input-on-cases.md for the design.
type lateralEmptyInputCase int

const (
	lateralNoRepair lateralEmptyInputCase = iota
	lateralPadOnly
	lateralPadThenFilter
)

func lateralEmptyInputPlan(joinType string, empty lateralEmptyInput, laterNullExtends bool) lateralEmptyInputCase {
	if !empty.ungroupedAggregate {
		return lateralNoRepair
	}
	if laterNullExtends {
		// Decline repair when a later RIGHT/FULL join manufactures NULL lateral columns.
		// Enclosing SELECT/WHERE rewrites cannot distinguish those rows from lateral padding:
		// COALESCE would replace NULL with 0, and moved ON could delete an owed unmatched row.
		// A sound repair needs defaults at the lateral's OWN output before that later join,
		// which requires a plan-level change, not a SelectInfo rewrite.
		// Leave the query as written; the ungrouped empty-input row remains the pinned boundary.
		// See docs/internals/lateral-default-later-null-extension.md for the design.
		return lateralNoRepair
	}
	if empty.onResidual == "" {
		return lateralPadOnly
	}
	// An INNER or CROSS join's ON is a filter; anything preserving a side is
	// not, and this pass cannot express the null-fill that would need.
	switch strings.ToLower(strings.TrimSpace(joinType)) {
	case "join", "inner join", "inner", "cross join", "cross", "":
		if empty.onResidualExpr == nil {
			// No AST to move; leave the join as written rather than drop the
			// condition.
			return lateralNoRepair
		}
		return lateralPadThenFilter
	}
	return lateralNoRepair
}

// lateralJoinNullExtendsAfter reports whether any join AFTER position ji in
// the flat, left-deep join list can NULL-EXTEND what is produced to its left.
// A RIGHT or FULL join is exactly that and nothing else is: an INNER join
// only filters, a LEFT join only extends the side it is joining ON.
func lateralJoinNullExtendsAfter(joins []plansql.JoinInfo, ji int) bool {
	if ji < 0 || ji+1 > len(joins) {
		return false
	}
	for _, j := range joins[ji+1:] {
		switch strings.ToLower(strings.TrimSpace(j.Type)) {
		case "right", "right join", "right outer", "right outer join",
			"full", "full join", "full outer", "full outer join":
			return true
		}
	}
	return false
}

// andIntoWhere conjoins one more predicate onto the enclosing query's WHERE.
func andIntoWhere(info *plansql.SelectInfo, raw string, expr plansql.Node) {
	if expr == nil {
		return
	}
	if info.WhereExpr == nil {
		info.WhereExpr = expr
		info.Where = raw
		return
	}
	info.WhereExpr = &plansql.AndNode{Left: info.WhereExpr, Right: expr}
	info.Where = info.WhereExpr.String()
}

// lateralEmptyInputOf reads the subquery as WRITTEN, before the correlation
// key is injected into its SELECT list and GROUP BY.
func lateralEmptyInputOf(info *plansql.SelectInfo, hasAgg, correlated bool) lateralEmptyInput {
	if !hasAgg || !correlated || len(info.GroupBy) > 0 {
		return lateralEmptyInput{}
	}
	out := lateralEmptyInput{ungroupedAggregate: true}
	for _, col := range info.Columns {
		if !col.IsAgg || !strings.EqualFold(col.AggFunc, "count") {
			continue
		}
		name := col.Alias
		if name == "" {
			name = col.Expr
		}
		if name != "" {
			out.countOutputs = append(out.countOutputs, name)
		}
	}
	return out
}

// coalesceLateralCountRefs wraps each lateral COUNT reference in COALESCE(…, 0),
// returning the SAME node when unchanged. Walk every expression that can contain a ref:
// ColRef, ParenNode, NotNode, UnaryOp, AndNode, OrNode, BinaryOp, CmpExpr, IsExpr,
// LikeExpr, BetweenExpr, InExpr, AnyAllExpr, CastNode, FuncCallNode, CaseNode,
// ArrayLitNode, TupleNode and WindowFuncNode. Missing an arm silently leaves pad NULLs.
// StarNode and text-carrying SubqueryNode/ExistsNode are excluded. Lateral outputs
// ARE in their scope, but their SQL text cannot be walked here; both are pinned.
// ADR-0021 §1h: every enclosing expression-tree position, none inside subquery text.
// See docs/internals/lateral-count-default-expression-walk.md for the design.
func coalesceLateralCountRefs(node plansql.Node, alias string, names map[string]bool) plansql.Node {
	if node == nil {
		return nil
	}
	changed := false
	var walk func(plansql.Node) plansql.Node
	wrap := func(c *plansql.ColRef) plansql.Node {
		changed = true
		return &plansql.FuncCallNode{
			Name: "coalesce",
			Args: []plansql.Node{c, &plansql.Lit{Value: "0", Kind: plansql.LitNumber}},
		}
	}
	walk = func(n plansql.Node) plansql.Node {
		switch e := n.(type) {
		case nil:
			return nil
		case *plansql.ColRef:
			// Qualified by the lateral's own alias, or bare — the enclosing
			// query may write either, and the lateral's output names are the
			// ones it invented.
			if !names[strings.ToLower(e.Column)] {
				return e
			}
			if e.Table == "" || strings.EqualFold(e.Table, alias) {
				return wrap(e)
			}
			return e
		case *plansql.ParenNode:
			return &plansql.ParenNode{Inner: walk(e.Inner)}
		case *plansql.NotNode:
			return &plansql.NotNode{Inner: walk(e.Inner)}
		case *plansql.UnaryOp:
			return &plansql.UnaryOp{Op: e.Op, Inner: walk(e.Inner)}
		case *plansql.AndNode:
			return &plansql.AndNode{Left: walk(e.Left), Right: walk(e.Right)}
		case *plansql.OrNode:
			return &plansql.OrNode{Left: walk(e.Left), Right: walk(e.Right)}
		case *plansql.BinaryOp:
			return &plansql.BinaryOp{Left: walk(e.Left), Op: e.Op, Right: walk(e.Right)}
		case *plansql.CmpExpr:
			return &plansql.CmpExpr{Left: walk(e.Left), Op: e.Op, Right: walk(e.Right)}
		case *plansql.IsExpr:
			return &plansql.IsExpr{Left: walk(e.Left), Not: e.Not, Check: e.Check}
		case *plansql.LikeExpr:
			return &plansql.LikeExpr{Left: walk(e.Left), Not: e.Not, Pattern: walk(e.Pattern)}
		case *plansql.BetweenExpr:
			return &plansql.BetweenExpr{Left: walk(e.Left), Not: e.Not, Low: walk(e.Low), High: walk(e.High)}
		case *plansql.CastNode:
			return &plansql.CastNode{Inner: walk(e.Inner), TypeName: e.TypeName}
		case *plansql.FuncCallNode:
			args := make([]plansql.Node, len(e.Args))
			for i, a := range e.Args {
				args[i] = walk(a)
			}
			return &plansql.FuncCallNode{Name: e.Name, Args: args, Distinct: e.Distinct, Star: e.Star}
		case *plansql.CaseNode:
			cn := &plansql.CaseNode{}
			if e.Subject != nil {
				cn.Subject = walk(e.Subject)
			}
			for _, w := range e.Whens {
				cn.Whens = append(cn.Whens, plansql.WhenClause{Cond: walk(w.Cond), Result: walk(w.Result)})
			}
			if e.Else != nil {
				cn.Else = walk(e.Else)
			}
			return cn
		case *plansql.InExpr:
			vals := make([]plansql.Node, len(e.Values))
			for i, v := range e.Values {
				vals[i] = walk(v)
			}
			return &plansql.InExpr{Left: walk(e.Left), Not: e.Not, Values: vals}
		case *plansql.AnyAllExpr:
			out := *e
			out.Left = walk(e.Left)
			if e.Values != nil {
				vals := make([]plansql.Node, len(e.Values))
				for i, v := range e.Values {
					vals[i] = walk(v)
				}
				out.Values = vals
			}
			return &out
		case *plansql.ArrayLitNode:
			els := make([]plansql.Node, len(e.Elements))
			for i, v := range e.Elements {
				els[i] = walk(v)
			}
			return &plansql.ArrayLitNode{Elements: els}
		case *plansql.TupleNode:
			els := make([]plansql.Node, len(e.Elements))
			for i, v := range e.Elements {
				els[i] = walk(v)
			}
			return &plansql.TupleNode{Elements: els}
		case *plansql.WindowFuncNode:
			out := *e
			if e.Func != nil {
				if fn, ok := walk(e.Func).(*plansql.FuncCallNode); ok {
					out.Func = fn
				}
			}
			if e.PartitionBy != nil {
				pb := make([]plansql.Node, len(e.PartitionBy))
				for i, v := range e.PartitionBy {
					pb[i] = walk(v)
				}
				out.PartitionBy = pb
			}
			if e.OrderBy != nil {
				ob := make([]plansql.WindowOrderBy, len(e.OrderBy))
				copy(ob, e.OrderBy)
				for i := range ob {
					ob[i].Expr = walk(ob[i].Expr)
				}
				out.OrderBy = ob
			}
			return &out
		}
		return n
	}
	out := walk(node)
	if !changed {
		return node
	}
	return out
}
