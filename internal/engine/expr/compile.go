package expr

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

// compileContext holds optional state for expression compilation.
type compileContext struct {
	runner SubqueryRunner
}

// Compile converts a vitess sqlparser expression AST into an Expr tree.
func Compile(node sqlparser.Expr) (Expr, error) {
	return compileWithCtx(node, &compileContext{})
}

// CompileWithRunner converts a vitess sqlparser expression AST into an Expr tree,
// with support for subquery expressions (scalar subqueries, IN subquery, EXISTS).
func CompileWithRunner(node sqlparser.Expr, runner SubqueryRunner) (Expr, error) {
	return compileWithCtx(node, &compileContext{runner: runner})
}

func compileWithCtx(node sqlparser.Expr, ctx *compileContext) (Expr, error) {
	if node == nil {
		return &Lit{Val: nil}, nil
	}
	switch n := node.(type) {
	case *sqlparser.ColName:
		return compileColName(n), nil

	case *sqlparser.SQLVal:
		return compileSQLVal(n)

	case *sqlparser.NullVal:
		return &Lit{Val: nil}, nil

	case sqlparser.BoolVal:
		return &Lit{Val: bool(n)}, nil

	case *sqlparser.BinaryExpr:
		return compileBinaryExprCtx(n, ctx)

	case *sqlparser.UnaryExpr:
		return compileUnaryExprCtx(n, ctx)

	case *sqlparser.ComparisonExpr:
		return compileComparisonCtx(n, ctx)

	case *sqlparser.AndExpr:
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithCtx(n.Right, ctx)
		if err != nil {
			return nil, err
		}
		return &And{Left: left, Right: right}, nil

	case *sqlparser.OrExpr:
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithCtx(n.Right, ctx)
		if err != nil {
			return nil, err
		}
		return &Or{Left: left, Right: right}, nil

	case *sqlparser.NotExpr:
		operand, err := compileWithCtx(n.Expr, ctx)
		if err != nil {
			return nil, err
		}
		return &Not{Operand: operand}, nil

	case *sqlparser.ParenExpr:
		return compileWithCtx(n.Expr, ctx)

	case *sqlparser.IsExpr:
		return compileIsExprCtx(n, ctx)

	case *sqlparser.RangeCond:
		return compileRangeCondCtx(n, ctx)

	case *sqlparser.FuncExpr:
		return compileFuncExprCtx(n, ctx)

	case *sqlparser.CaseExpr:
		return compileCaseExprCtx(n, ctx)

	case *sqlparser.ConvertExpr:
		return compileConvertExprCtx(n, ctx)

	case *sqlparser.Subquery:
		// Scalar subquery: (SELECT ...)
		if ctx.runner == nil {
			return nil, fmt.Errorf("subqueries require a SubqueryRunner")
		}
		sql := sqlparser.String(n.Select)
		return &ScalarSubquery{SQL: sql, Runner: ctx.runner}, nil

	case *sqlparser.ExistsExpr:
		// EXISTS (SELECT ...)
		if ctx.runner == nil {
			return nil, fmt.Errorf("EXISTS subquery requires a SubqueryRunner")
		}
		sql := sqlparser.String(n.Subquery.Select)
		return &ExistsSubquery{SQL: sql, Runner: ctx.runner}, nil

	case sqlparser.ValTuple:
		// This is handled in the context of IN expressions
		return nil, fmt.Errorf("unexpected ValTuple outside of IN expression")

	default:
		// Fallback: render to SQL string and treat as literal
		return &Lit{Val: sqlparser.String(node)}, nil
	}
}

func compileColName(n *sqlparser.ColName) Expr {
	name := n.Name.String()
	// Strip table qualifier — we resolve by column name only
	return &ColRef{Name: name}
}

func compileSQLVal(n *sqlparser.SQLVal) (Expr, error) {
	switch n.Type {
	case sqlparser.IntVal:
		i, err := strconv.ParseInt(string(n.Val), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %s", n.Val)
		}
		return &Lit{Val: i}, nil
	case sqlparser.FloatVal:
		f, err := strconv.ParseFloat(string(n.Val), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float: %s", n.Val)
		}
		return &Lit{Val: f}, nil
	case sqlparser.StrVal:
		return &Lit{Val: string(n.Val)}, nil
	default:
		return &Lit{Val: string(n.Val)}, nil
	}
}

func compileBinaryExpr(n *sqlparser.BinaryExpr) (Expr, error) {
	return compileBinaryExprCtx(n, &compileContext{})
}

func compileBinaryExprCtx(n *sqlparser.BinaryExpr, ctx *compileContext) (Expr, error) {
	left, err := compileWithCtx(n.Left, ctx)
	if err != nil {
		return nil, err
	}
	right, err := compileWithCtx(n.Right, ctx)
	if err != nil {
		return nil, err
	}
	switch n.Operator {
	case sqlparser.PlusStr:
		return &BinOp{Left: left, Right: right, Op: "+"}, nil
	case sqlparser.MinusStr:
		return &BinOp{Left: left, Right: right, Op: "-"}, nil
	case sqlparser.MultStr:
		return &BinOp{Left: left, Right: right, Op: "*"}, nil
	case sqlparser.DivStr:
		return &BinOp{Left: left, Right: right, Op: "/"}, nil
	case sqlparser.ModStr:
		return &BinOp{Left: left, Right: right, Op: "%"}, nil
	default:
		return &BinOp{Left: left, Right: right, Op: n.Operator}, nil
	}
}

func compileUnaryExpr(n *sqlparser.UnaryExpr) (Expr, error) {
	return compileUnaryExprCtx(n, &compileContext{})
}

func compileUnaryExprCtx(n *sqlparser.UnaryExpr, ctx *compileContext) (Expr, error) {
	operand, err := compileWithCtx(n.Expr, ctx)
	if err != nil {
		return nil, err
	}
	switch n.Operator {
	case sqlparser.UMinusStr:
		return &UnaryOp{Operand: operand, Op: "-"}, nil
	case sqlparser.UPlusStr:
		return &UnaryOp{Operand: operand, Op: "+"}, nil
	default:
		return operand, nil
	}
}

func compileComparison(n *sqlparser.ComparisonExpr) (Expr, error) {
	return compileComparisonCtx(n, &compileContext{})
}

func compileComparisonCtx(n *sqlparser.ComparisonExpr, ctx *compileContext) (Expr, error) {
	left, err := compileWithCtx(n.Left, ctx)
	if err != nil {
		return nil, err
	}

	switch n.Operator {
	case sqlparser.InStr, sqlparser.NotInStr:
		return compileInExprCtx(left, n.Right, n.Operator == sqlparser.NotInStr, ctx)
	case sqlparser.LikeStr, sqlparser.NotLikeStr:
		right, err := compileWithCtx(n.Right, ctx)
		if err != nil {
			return nil, err
		}
		return &Like{
			Expr:    left,
			Pattern: right,
			Not:     n.Operator == sqlparser.NotLikeStr,
		}, nil
	}

	right, err := compileWithCtx(n.Right, ctx)
	if err != nil {
		return nil, err
	}

	var op CmpOp
	switch n.Operator {
	case sqlparser.EqualStr:
		op = CmpEq
	case sqlparser.NotEqualStr:
		op = CmpNe
	case sqlparser.LessThanStr:
		op = CmpLt
	case sqlparser.LessEqualStr:
		op = CmpLe
	case sqlparser.GreaterThanStr:
		op = CmpGt
	case sqlparser.GreaterEqualStr:
		op = CmpGe
	default:
		op = CmpEq
	}
	return &Cmp{Left: left, Right: right, Op: op}, nil
}

func compileInExpr(left Expr, right sqlparser.Expr, not bool) (Expr, error) {
	return compileInExprCtx(left, right, not, &compileContext{})
}

func compileInExprCtx(left Expr, right sqlparser.Expr, not bool, ctx *compileContext) (Expr, error) {
	// Check for subquery: IN (SELECT ...)
	if subq, ok := right.(*sqlparser.Subquery); ok {
		if ctx.runner == nil {
			return nil, fmt.Errorf("IN subquery requires a SubqueryRunner")
		}
		sql := sqlparser.String(subq.Select)
		return &InSubquery{Expr: left, SQL: sql, Runner: ctx.runner, Not: not}, nil
	}

	tuple, ok := right.(sqlparser.ValTuple)
	if !ok {
		return nil, fmt.Errorf("IN requires a value list or subquery, got %T", right)
	}
	var values []Expr
	for _, v := range tuple {
		compiled, err := compileWithCtx(v, ctx)
		if err != nil {
			return nil, err
		}
		values = append(values, compiled)
	}
	return &In{Expr: left, Values: values, Not: not}, nil
}

func compileIsExpr(n *sqlparser.IsExpr) (Expr, error) {
	return compileIsExprCtx(n, &compileContext{})
}

func compileIsExprCtx(n *sqlparser.IsExpr, ctx *compileContext) (Expr, error) {
	operand, err := compileWithCtx(n.Expr, ctx)
	if err != nil {
		return nil, err
	}
	switch n.Operator {
	case sqlparser.IsNullStr:
		return &IsNull{Operand: operand, Not: false}, nil
	case sqlparser.IsNotNullStr:
		return &IsNull{Operand: operand, Not: true}, nil
	case sqlparser.IsTrueStr:
		return &Cmp{Left: operand, Right: &Lit{Val: true}, Op: CmpEq}, nil
	case sqlparser.IsFalseStr:
		return &Cmp{Left: operand, Right: &Lit{Val: false}, Op: CmpEq}, nil
	case sqlparser.IsNotTrueStr:
		return &Cmp{Left: operand, Right: &Lit{Val: true}, Op: CmpNe}, nil
	case sqlparser.IsNotFalseStr:
		return &Cmp{Left: operand, Right: &Lit{Val: false}, Op: CmpNe}, nil
	default:
		return nil, fmt.Errorf("unsupported IS operator: %s", n.Operator)
	}
}

func compileRangeCond(n *sqlparser.RangeCond) (Expr, error) {
	return compileRangeCondCtx(n, &compileContext{})
}

func compileRangeCondCtx(n *sqlparser.RangeCond, ctx *compileContext) (Expr, error) {
	e, err := compileWithCtx(n.Left, ctx)
	if err != nil {
		return nil, err
	}
	low, err := compileWithCtx(n.From, ctx)
	if err != nil {
		return nil, err
	}
	hi, err := compileWithCtx(n.To, ctx)
	if err != nil {
		return nil, err
	}
	return &Between{
		Expr: e,
		Low:  low,
		Hi:   hi,
		Not:  n.Operator == sqlparser.NotBetweenStr,
	}, nil
}

func compileFuncExpr(n *sqlparser.FuncExpr) (Expr, error) {
	return compileFuncExprCtx(n, &compileContext{})
}

func compileFuncExprCtx(n *sqlparser.FuncExpr, ctx *compileContext) (Expr, error) {
	name := strings.ToLower(n.Name.String())

	// Compile arguments
	var args []Expr
	for _, selExpr := range n.Exprs {
		switch e := selExpr.(type) {
		case *sqlparser.AliasedExpr:
			compiled, err := compileWithCtx(e.Expr, ctx)
			if err != nil {
				return nil, err
			}
			args = append(args, compiled)
		case *sqlparser.StarExpr:
			// COUNT(*) — pass nil marker
			args = append(args, &Lit{Val: "*"})
		default:
			return nil, fmt.Errorf("unsupported function argument: %T", selExpr)
		}
	}

	// Check if it's a known aggregate (handled by aggregate operator, not expression engine)
	if sqlparser.Aggregates[name] {
		// Return as a func call that the planner can recognize as aggregate
		return &FuncCall{Name: name, Args: args}, nil
	}

	// Check for COALESCE special form
	if name == "coalesce" {
		return &Coalesce{Args: args}, nil
	}

	return &FuncCall{Name: name, Args: args}, nil
}

func compileCaseExpr(n *sqlparser.CaseExpr) (Expr, error) {
	return compileCaseExprCtx(n, &compileContext{})
}

func compileCaseExprCtx(n *sqlparser.CaseExpr, ctx *compileContext) (Expr, error) {
	c := &Case{}

	if n.Expr != nil {
		var err error
		c.Operand, err = compileWithCtx(n.Expr, ctx)
		if err != nil {
			return nil, err
		}
	}

	for _, when := range n.Whens {
		cond, err := compileWithCtx(when.Cond, ctx)
		if err != nil {
			return nil, err
		}
		result, err := compileWithCtx(when.Val, ctx)
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

func compileConvertExpr(n *sqlparser.ConvertExpr) (Expr, error) {
	return compileConvertExprCtx(n, &compileContext{})
}

func compileConvertExprCtx(n *sqlparser.ConvertExpr, ctx *compileContext) (Expr, error) {
	operand, err := compileWithCtx(n.Expr, ctx)
	if err != nil {
		return nil, err
	}
	destType := "string"
	if n.Type != nil {
		destType = strings.ToLower(n.Type.Type)
	}
	return &Cast{Operand: operand, DestType: destType}, nil
}

// CompileSelectExpr compiles a SELECT column expression from the vitess AST.
// Returns the compiled expression and the output column name.
func CompileSelectExpr(selExpr *sqlparser.AliasedExpr) (Expr, string, error) {
	compiled, err := Compile(selExpr.Expr)
	if err != nil {
		return nil, "", err
	}

	// Determine output name: alias > column name > expression string
	name := selExpr.As.String()
	if name == "" {
		if colName, ok := selExpr.Expr.(*sqlparser.ColName); ok {
			name = colName.Name.String()
		} else {
			name = sqlparser.String(selExpr.Expr)
		}
	}

	return compiled, name, nil
}
