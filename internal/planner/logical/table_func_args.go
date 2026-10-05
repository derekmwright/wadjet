// SPDX-License-Identifier: MIT

package logical

import (
	"context"
	"fmt"
	"strconv"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// foldTableFuncArgs evaluates a table function's EXPRESSION arguments to the
// literal text its source reads (TableRef.FuncArgExprs): `generate_series(1,
// array_upper(current_schemas(false), 1))` is `generate_series(1, 1)`. An
// argument is folded only when it is a constant — literals, operators, casts
// and scalar function calls over them. One that reads a column (a LATERAL
// reference) or a subquery is refused 0A000: the source runs once, before
// any row exists, so it has no value to read, and answering the reference's
// own spelling as the value was a silently wrong series.
func foldTableFuncArgs(fn string, args []string, exprs []plansql.Node) ([]string, error) {
	out := append([]string(nil), args...)
	for i, e := range exprs {
		if e == nil || i >= len(out) {
			continue
		}
		if !constantArg(e) {
			return nil, sqlerr.New("0A000",
				"%s: argument %d (%s) must be a constant expression here; a column or subquery reference is not supported",
				fn, i+1, e.String())
		}
		// An argument that reads the clock is not folded here: this layer
		// has no statement. It stays as written and is recorded
		// (Node.FuncClockArgs); BindClockFolds folds it with the statement's
		// clock, before anything reads the arguments (#1566).
		if readsClock(e) {
			continue
		}
		v, err := FoldTableFuncArg(e)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// FoldTableFuncArg evaluates one constant table-function argument to the text
// its source reads. opts reach the compile: the physical planner re-folds an
// argument that reads the clock with the statement's (Node.FuncClockArgs,
// #1566) — this layer has no statement, so its own fold reads the live clock
// and serves only the plan's declarations.
func FoldTableFuncArg(e plansql.Node, opts ...expr.CompileOption) (string, error) {
	c, err := expr.Compile(e, opts...)
	if err != nil {
		return "", err
	}
	v, err := evalConstant(c)
	if err != nil {
		return "", err
	}
	switch x := v.(type) {
	case nil:
		return "NULL", nil
	case string:
		return x, nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case int32:
		return strconv.FormatInt(int64(x), 10), nil
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	default:
		return fmt.Sprint(x), nil
	}
}

// tableFuncClockArgs is the arguments, by position, that read a SQL clock
// function — the ones the physical planner re-folds with the statement clock.
func tableFuncClockArgs(exprs []plansql.Node) map[int]plansql.Node {
	var out map[int]plansql.Node
	for i, e := range exprs {
		if e != nil && readsClock(e) {
			if out == nil {
				out = map[int]plansql.Node{}
			}
			out[i] = e
		}
	}
	return out
}

// evalConstant evaluates a constant expression, turning a raised SQL error
// (a panic carrying a coded error, as the scalar kernels raise them) into a
// returned one.
func evalConstant(c expr.Expr) (v any, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok && sqlerr.StateOf(e) != "" {
				err = e
				return
			}
			panic(r)
		}
	}()
	return c.Eval(nil, 0), nil
}

// constantArg reports whether e reads nothing but constants.
func constantArg(e plansql.Node) bool {
	switch n := e.(type) {
	case *plansql.Lit, *plansql.IntervalLit:
		return true
	case *plansql.ParenNode:
		return constantArg(n.Inner)
	case *plansql.UnaryOp:
		return constantArg(n.Inner)
	case *plansql.BinaryOp:
		return constantArg(n.Left) && constantArg(n.Right)
	case *plansql.CastNode:
		return constantArg(n.Inner)
	case *plansql.ArrayLitNode:
		for _, a := range n.Elements {
			if !constantArg(a) {
				return false
			}
		}
		return true
	case *plansql.FuncCallNode:
		if plansql.IsAggregate(n.Name) || n.Star {
			return false
		}
		for _, a := range n.Args {
			if !constantArg(a) {
				return false
			}
		}
		return true
	}
	return false
}

// The window functions' integer argument (plansql.WindowIntegerArgument)
// folds a constant expression with this same fold.
func init() {
	plansql.SetConstantFolder(func(ctx context.Context, n plansql.Node) (any, bool, error) {
		if !constantArg(n) {
			return nil, false, nil
		}
		if ctx == nil {
			// Parse time: no statement. A clock-reading argument waits for
			// the plan, which folds it with the statement's clock (#1566).
			if readsClock(n) {
				return plansql.ClockDeferred, true, nil
			}
			ctx = context.Background()
		}
		c, err := expr.Compile(n, expr.WithStatementClock(ctx))
		if err != nil {
			return nil, true, err
		}
		v, err := evalConstant(c)
		return v, true, err
	})
}

// BindClockFolds folds what the builder deferred on a Scan because it reads a
// clock function — its table-function arguments (FuncClockArgs) and its
// TABLESAMPLE argument (SampleClockArg) — with the statement clock opt binds
// (expr.WithStatementClock). The physical planner calls it where a context
// first reaches the plan (AnnotateScanColumns) and again where the scan is
// built, which raises its error. With no clock bound the fold raises XX000
// (expr's clock functions have no live-clock fallback). The node keeps what
// was deferred until a fold succeeds.
func BindClockFolds(n *Node, opt expr.CompileOption) error {
	if n == nil || n.Type != NodeScan {
		return nil
	}
	if len(n.FuncClockArgs) > 0 {
		args := append([]string(nil), n.FuncArgs...)
		for i, e := range n.FuncClockArgs {
			if i >= len(args) {
				continue
			}
			v, err := FoldTableFuncArg(e, opt)
			if err != nil {
				return err
			}
			args[i] = v
		}
		n.FuncArgs, n.FuncClockArgs = args, nil
	}
	if n.SampleClockArg != nil {
		if tablesampleEvaluator == nil {
			return sqlerr.New("XX000", "TABLESAMPLE: no argument evaluator is installed")
		}
		pct, isNull, err := tablesampleEvaluator(n.SampleClockArg, opt)
		if err != nil {
			return err
		}
		n.SamplePercent, n.SampleNull, n.SampleClockArg = pct, isNull, nil
	}
	return nil
}
