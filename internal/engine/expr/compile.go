package expr

import (
	"fmt"
	"strconv"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// compileContext holds optional state for expression compilation.
type compileContext struct {
	runner      SubqueryRunner
	outerTables map[string]bool   // table aliases from the outer query scope
	outerCols   map[string]string // column name → table mapping for unqualified resolution
}

// Compile converts our AST Node into an Expr tree.
func Compile(node plansql.Node) (Expr, error) {
	return compileWithCtx(node, &compileContext{})
}

// CompileWithRunner converts our AST Node into an Expr tree,
// with support for subquery expressions (scalar subqueries, IN subquery, EXISTS).
func CompileWithRunner(node plansql.Node, runner SubqueryRunner) (Expr, error) {
	return compileWithCtx(node, &compileContext{runner: runner})
}

// CompileWithScope converts our AST Node into an Expr tree with full scope
// information, enabling correlated subquery detection and per-row execution.
// outerTables contains the table names and aliases from the outer query.
func CompileWithScope(node plansql.Node, runner SubqueryRunner, outerTables map[string]bool) (Expr, error) {
	return compileWithCtx(node, &compileContext{runner: runner, outerTables: outerTables})
}

// CompileWithFullScope is like CompileWithScope but also accepts a column-to-table
// mapping for resolving unqualified column references in correlated subqueries.
func CompileWithFullScope(node plansql.Node, runner SubqueryRunner, outerTables map[string]bool, outerCols map[string]string) (Expr, error) {
	return compileWithCtx(node, &compileContext{runner: runner, outerTables: outerTables, outerCols: outerCols})
}

func compileWithCtx(node plansql.Node, ctx *compileContext) (Expr, error) {
	if node == nil {
		return &Lit{Val: nil}, nil
	}
	switch n := node.(type) {
	case *plansql.ColRef:
		name := n.Column
		if n.Table != "" {
			// Preserve table qualifier for disambiguation (e.g., self-joins)
			name = n.Table + "." + n.Column
		}
		return &ColRef{Name: name}, nil

	case *plansql.Lit:
		return compileLit(n)

	case *plansql.StarNode:
		return &Lit{Val: "*"}, nil

	case *plansql.IntervalLit:
		iv := IntervalValue{}
		switch n.Unit {
		case "year":
			iv.Years = n.Value
		case "month":
			iv.Months = n.Value
		case "week":
			iv.Days = n.Value * 7
		case "day":
			iv.Days = n.Value
		case "hour":
			iv.Hours = n.Value
		case "minute":
			iv.Minutes = n.Value
		case "second":
			iv.Seconds = n.Value
		default:
			iv.Days = n.Value // fallback
		}
		return &Lit{Val: iv}, nil

	case *plansql.BinaryOp:
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithCtx(n.Right, ctx)
		if err != nil {
			return nil, err
		}
		return compileBinOp(left, right, n.Op), nil

	case *plansql.UnaryOp:
		operand, err := compileWithCtx(n.Inner, ctx)
		if err != nil {
			return nil, err
		}
		return &UnaryOp{Operand: operand, Op: n.Op}, nil

	case *plansql.CmpExpr:
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithCtx(n.Right, ctx)
		if err != nil {
			return nil, err
		}
		var op CmpOp
		switch n.Op {
		case "=":
			op = CmpEq
		case "!=", "<>":
			op = CmpNe
		case "<":
			op = CmpLt
		case "<=":
			op = CmpLe
		case ">":
			op = CmpGt
		case ">=":
			op = CmpGe
		default:
			op = CmpEq
		}
		return compileCmp(left, right, op), nil

	case *plansql.InExpr:
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		// Check for subquery IN: single SubqueryNode in values
		if len(n.Values) == 1 {
			if sq, ok := n.Values[0].(*plansql.SubqueryNode); ok {
				if ctx.runner == nil {
					return nil, fmt.Errorf("IN subquery requires a SubqueryRunner")
				}
				if len(ctx.outerTables) > 0 {
					var refs []plansql.OuterRef
					var err error
					if len(ctx.outerCols) > 0 {
						refs, err = plansql.FindCorrelatedRefsWithColumns(sq.SQL, ctx.outerTables, ctx.outerCols)
					} else {
						refs, err = plansql.FindCorrelatedRefs(sq.SQL, ctx.outerTables)
					}
					if err == nil && len(refs) > 0 {
						parsed, _ := plansql.Parse(sq.SQL)
						info, _ := plansql.ExtractSelect(parsed)
						if info != nil {
							return &CorrelatedInSubquery{
								Expr:            left,
								Runner:          ctx.runner,
								Not:             n.Not,
								OuterRefs:       refs,
								OuterTables:     ctx.outerTables,
								ParsedInfo:      info,
								UnqualOuterCols: buildUnqualOuterCols(refs, ctx.outerCols),
							}, nil
						}
					}
				}
				return &InSubquery{Expr: left, SQL: sq.SQL, Runner: ctx.runner, Not: n.Not}, nil
			}
		}
		var values []Expr
		for _, v := range n.Values {
			compiled, err := compileWithCtx(v, ctx)
			if err != nil {
				return nil, err
			}
			values = append(values, compiled)
		}
		return &In{Expr: left, Values: values, Not: n.Not}, nil

	case *plansql.BetweenExpr:
		e, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		low, err := compileWithCtx(n.Low, ctx)
		if err != nil {
			return nil, err
		}
		hi, err := compileWithCtx(n.High, ctx)
		if err != nil {
			return nil, err
		}
		return &Between{Expr: e, Low: low, Hi: hi, Not: n.Not}, nil

	case *plansql.LikeExpr:
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithCtx(n.Pattern, ctx)
		if err != nil {
			return nil, err
		}
		return &Like{Expr: left, Pattern: right, Not: n.Not}, nil

	case *plansql.IsExpr:
		operand, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		switch n.Check {
		case "null":
			return &IsNull{Operand: operand, Not: n.Not}, nil
		case "true":
			if n.Not {
				return &Cmp{Left: operand, Right: &Lit{Val: true}, Op: CmpNe}, nil
			}
			return &Cmp{Left: operand, Right: &Lit{Val: true}, Op: CmpEq}, nil
		case "false":
			if n.Not {
				return &Cmp{Left: operand, Right: &Lit{Val: false}, Op: CmpNe}, nil
			}
			return &Cmp{Left: operand, Right: &Lit{Val: false}, Op: CmpEq}, nil
		default:
			return nil, fmt.Errorf("unsupported IS check: %s", n.Check)
		}

	case *plansql.AndNode:
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithCtx(n.Right, ctx)
		if err != nil {
			return nil, err
		}
		return &And{Left: left, Right: right}, nil

	case *plansql.OrNode:
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithCtx(n.Right, ctx)
		if err != nil {
			return nil, err
		}
		return &Or{Left: left, Right: right}, nil

	case *plansql.NotNode:
		operand, err := compileWithCtx(n.Inner, ctx)
		if err != nil {
			return nil, err
		}
		return &Not{Operand: operand}, nil

	case *plansql.ParenNode:
		return compileWithCtx(n.Inner, ctx)

	case *plansql.FuncCallNode:
		return compileFuncCallNode(n, ctx)

	case *plansql.CaseNode:
		return compileCaseNode(n, ctx)

	case *plansql.CastNode:
		operand, err := compileWithCtx(n.Inner, ctx)
		if err != nil {
			return nil, err
		}
		return &Cast{Operand: operand, DestType: strings.ToLower(n.TypeName)}, nil

	case *plansql.SubqueryNode:
		// Scalar subquery: (SELECT ...)
		if ctx.runner == nil {
			return nil, fmt.Errorf("subqueries require a SubqueryRunner")
		}
		if len(ctx.outerTables) > 0 {
			var refs []plansql.OuterRef
			var err error
			if len(ctx.outerCols) > 0 {
				refs, err = plansql.FindCorrelatedRefsWithColumns(n.SQL, ctx.outerTables, ctx.outerCols)
			} else {
				refs, err = plansql.FindCorrelatedRefs(n.SQL, ctx.outerTables)
			}
			if err == nil && len(refs) > 0 {
				parsed, _ := plansql.Parse(n.SQL)
				info, _ := plansql.ExtractSelect(parsed)
				if info != nil {
					return &CorrelatedScalarSubquery{
						Runner:          ctx.runner,
						OuterRefs:       refs,
						OuterTables:     ctx.outerTables,
						ParsedInfo:      info,
						UnqualOuterCols: buildUnqualOuterCols(refs, ctx.outerCols),
					}, nil
				}
			}
		}
		return &ScalarSubquery{SQL: n.SQL, Runner: ctx.runner}, nil

	case *plansql.ExistsNode:
		if ctx.runner == nil {
			return nil, fmt.Errorf("EXISTS subquery requires a SubqueryRunner")
		}
		if len(ctx.outerTables) > 0 {
			var refs []plansql.OuterRef
			var err error
			if len(ctx.outerCols) > 0 {
				refs, err = plansql.FindCorrelatedRefsWithColumns(n.SQL, ctx.outerTables, ctx.outerCols)
			} else {
				refs, err = plansql.FindCorrelatedRefs(n.SQL, ctx.outerTables)
			}
			if err == nil && len(refs) > 0 {
				parsed, _ := plansql.Parse(n.SQL)
				info, _ := plansql.ExtractSelect(parsed)
				if info != nil {
					return &CorrelatedExistsSubquery{
						Runner:          ctx.runner,
						Not:             n.Not,
						OuterRefs:       refs,
						OuterTables:     ctx.outerTables,
						ParsedInfo:      info,
						UnqualOuterCols: buildUnqualOuterCols(refs, ctx.outerCols),
					}, nil
				}
			}
		}
		return &ExistsSubquery{SQL: n.SQL, Runner: ctx.runner, Not: n.Not}, nil

	case *plansql.ArrayLitNode:
		elems := make([]Expr, len(n.Elements))
		for i, e := range n.Elements {
			compiled, err := compileWithCtx(e, ctx)
			if err != nil {
				return nil, fmt.Errorf("compiling array element %d: %w", i, err)
			}
			elems[i] = compiled
		}
		return &ArrayLitExpr{Elements: elems}, nil

	default:
		return &Lit{Val: node.String()}, nil
	}
}

// compileCmp creates a typed comparison when both sides are provably numeric,
// falling back to the generic Cmp otherwise.
// We can't use typed paths for ColRef since column types are unknown at compile time.
func compileCmp(left, right Expr, op CmpOp) Expr {
	// Only use typed paths when both sides are provably numeric
	// (not ColRef, which implements Int64Expr/Float64Expr as fallback)
	if isProvablyInt64(left) && isProvablyInt64(right) {
		return &CmpInt64{Left: left.(Int64Expr), Right: right.(Int64Expr), Op: op}
	}
	if isProvablyFloat64(left) && isProvablyFloat64(right) {
		return &CmpFloat64{Left: left.(Float64Expr), Right: right.(Float64Expr), Op: op}
	}
	// Bare column vs a string literal that parses as a date/timestamp:
	// pre-parse the literal once (both temporal units) and pick the unit
	// per batch from the column's resolved type — the dominant pushed-
	// filter shape (`l_shipdate <= '1998-09-02'`). Column type is unknown
	// here, so CmpTemporalLit keeps a generic fallback for non-temporal
	// columns; semantics stay identical to Cmp in every sub-case.
	if col, ok := left.(*ColRef); ok && col.structField == "" {
		if tl := tryTemporalLit(col, right, op, false); tl != nil {
			return tl
		}
	}
	if col, ok := right.(*ColRef); ok && col.structField == "" {
		if tl := tryTemporalLit(col, left, op, true); tl != nil {
			return tl
		}
	}
	return &Cmp{Left: left, Right: right, Op: op}
}

// tryTemporalLit builds a CmpTemporalLit when other is a string literal
// parseable as a date or timestamp; nil otherwise.
func tryTemporalLit(col *ColRef, other Expr, op CmpOp, flip bool) *CmpTemporalLit {
	lit, ok := other.(*Lit)
	if !ok {
		return nil
	}
	s, ok := lit.Val.(string)
	if !ok {
		return nil
	}
	days, dok := parseDateToEpochDaysOK(s)
	ms, mok := parseTimestampToEpochMsOK(s)
	if !dok && !mok {
		return nil
	}
	return &CmpTemporalLit{Col: col, Lit: s, Op: op, Flip: flip, days: days, ms: ms}
}

// isProvablyInt64 returns true if the expression definitely produces int64 values.
func isProvablyInt64(e Expr) bool {
	switch v := e.(type) {
	case *Lit:
		switch v.Val.(type) {
		case int64, int32, int:
			return true
		}
	case *BinOpInt64:
		return true
	}
	return false
}

// isProvablyFloat64 returns true if the expression definitely produces float64 values.
func isProvablyFloat64(e Expr) bool {
	switch v := e.(type) {
	case *Lit:
		switch v.Val.(type) {
		case float64, float32:
			return true
		}
	case *BinOpFloat64:
		return true
	}
	return false
}

// compileBinOp creates a typed BinOp when both sides implement typed interfaces,
// falling back to the generic BinOp otherwise.
func compileBinOp(left, right Expr, op string) Expr {
	// Try float64 typed path (covers float64 columns, int64 columns via promotion, and literals)
	lf, lfOk := left.(Float64Expr)
	rf, rfOk := right.(Float64Expr)
	if lfOk && rfOk {
		// Prefer int64 path if both sides are native int64 (not float literals)
		li, liOk := left.(Int64Expr)
		ri, riOk := right.(Int64Expr)
		if liOk && riOk && op != "/" && isIntNative(left) && isIntNative(right) {
			return &BinOpInt64{Left: li, Right: ri, Op: op}
		}
		return &BinOpFloat64{Left: lf, Right: rf, Op: op}
	}
	return &BinOp{Left: left, Right: right, Op: op}
}

// isIntNative returns true if the expression natively produces int64 values
// (not a float literal or float column that happens to implement Int64Expr).
func isIntNative(e Expr) bool {
	switch v := e.(type) {
	case *Lit:
		switch v.Val.(type) {
		case int64, int32, int:
			return true
		default:
			return false
		}
	case *ColRef:
		return false // column type unknown at compile time; use float64 path for safety
	case *BinOpInt64:
		return true
	default:
		return false
	}
}

func compileLit(n *plansql.Lit) (Expr, error) {
	switch n.Kind {
	case plansql.LitNumber:
		// Try integer first
		if i, err := strconv.ParseInt(n.Value, 10, 64); err == nil {
			return &Lit{Val: i}, nil
		}
		// Try float
		if f, err := strconv.ParseFloat(n.Value, 64); err == nil {
			return &Lit{Val: f}, nil
		}
		return &Lit{Val: n.Value}, nil
	case plansql.LitString:
		return &Lit{Val: n.Value}, nil
	case plansql.LitBool:
		return &Lit{Val: strings.ToLower(n.Value) == "true"}, nil
	case plansql.LitNull:
		return &Lit{Val: nil}, nil
	default:
		return &Lit{Val: n.Value}, nil
	}
}

func compileFuncCallNode(n *plansql.FuncCallNode, ctx *compileContext) (Expr, error) {
	name := strings.ToLower(n.Name)

	var args []Expr
	if n.Star {
		args = append(args, &Lit{Val: "*"})
	} else {
		for _, arg := range n.Args {
			compiled, err := compileWithCtx(arg, ctx)
			if err != nil {
				return nil, err
			}
			args = append(args, compiled)
		}
	}

	// Check for COALESCE special form
	if name == "coalesce" {
		return &Coalesce{Args: args}, nil
	}

	fc := &FuncCall{Name: name, Args: args}
	if isNumericFunc(name) {
		return &numericFuncCall{fc}, nil
	}
	return fc, nil
}

// isNumericFunc returns true if the function is known to return a numeric value.
// These functions get wrapped in numericFuncCall to implement Float64Expr/Int64Expr,
// enabling compileBinOp to produce typed BinOpFloat64 when they are operands.
func isNumericFunc(name string) bool {
	switch name {
	case "extract", "year", "month", "day", "hour", "minute", "second",
		"length", "char_length", "character_length", "octet_length", "bit_length",
		"abs", "ceil", "ceiling", "floor", "round", "truncate", "trunc",
		"sign", "sqrt", "cbrt", "exp", "ln", "log", "log2", "log10",
		"power", "pow", "mod",
		"sin", "cos", "tan", "asin", "acos", "atan", "atan2",
		"degrees", "radians", "pi",
		"position", "strpos",
		"ascii", "crc32",
		"random",
		"epoch", "date_part",
		"width_bucket",
		"greatest", "least",
		"coalesce": // coalesce of numbers returns numbers
		return true
	}
	return false
}

func compileCaseNode(n *plansql.CaseNode, ctx *compileContext) (Expr, error) {
	c := &Case{}

	if n.Subject != nil {
		var err error
		c.Operand, err = compileWithCtx(n.Subject, ctx)
		if err != nil {
			return nil, err
		}
	}

	for _, when := range n.Whens {
		cond, err := compileWithCtx(when.Cond, ctx)
		if err != nil {
			return nil, err
		}
		result, err := compileWithCtx(when.Result, ctx)
		if err != nil {
			return nil, err
		}
		c.Whens = append(c.Whens, CaseWhen{Cond: cond, Result: result})
	}

	if n.Else != nil {
		var err error
		c.Else, err = compileWithCtx(n.Else, ctx)
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

// buildUnqualOuterCols extracts unqualified outer column mappings from detected refs.
// These are refs that were resolved via outerCols (column→table mapping) rather than
// explicit table qualifiers.
func buildUnqualOuterCols(refs []plansql.OuterRef, outerCols map[string]string) map[string]string {
	if len(outerCols) == 0 {
		return nil
	}
	var result map[string]string
	for _, ref := range refs {
		col := ref.Column
		if tbl, ok := outerCols[col]; ok && tbl == ref.Table {
			if result == nil {
				result = make(map[string]string)
			}
			result[col] = ref.Table
		}
	}
	return result
}

// CompileSelectExpr compiles a SELECT column expression from our AST.
// Returns the compiled expression and the output column name.
func CompileSelectExpr(expr plansql.Node, alias string) (Expr, string, error) {
	compiled, err := Compile(expr)
	if err != nil {
		return nil, "", err
	}

	name := alias
	if name == "" {
		if colRef, ok := expr.(*plansql.ColRef); ok {
			name = colRef.Column
		} else {
			name = expr.String()
		}
	}

	return compiled, name, nil
}
