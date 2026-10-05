// SPDX-License-Identifier: MIT

package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func init() { logical.SetTablesampleEvaluator(TablesampleArgument) }

// TablesampleArgument is THE reading of a TABLESAMPLE argument (#1411), as
// PostgreSQL 17.11 reads it: BERNOULLI and SYSTEM each take one argument of
// type real (tsm_bernoulli / tsm_system declare FLOAT4), and the argument is
// any expression that reads no row, coerced to real once, when the statement
// is planned:
//
//   - an untyped quoted literal is read by real's input ('50', ' 50 ',
//     'Infinity', 'NaN'; 'abc' is 22P02, '1e39' 22003);
//   - a number (int, bigint, numeric, real, double precision) is converted to
//     real, so a value real cannot hold is 22003 — 1e39, a bare 1e400 or
//     CAST('1e400' AS DOUBLE PRECISION), and a nonzero 1e-46 that real
//     rounds to zero — and 100.000001 rounds to 100;
//   - NULL (bare, or CAST(NULL AS <number>)) is reported as isNull;
//   - any other type is 42804, a NULL of that type too;
//   - a column reference is 42703 (no column of the query is in scope
//     there), an aggregate 42803, a window function 42P20, and a subquery is
//     refused 0A000: the argument is evaluated here, before any row exists,
//     and PostgreSQL runs it as an InitPlan this planner does not have.
//
// The evaluation's own failure is its SQLSTATE (1/0 is 22012). The RANGE —
// NULL, NaN, below 0 or above 100 is 2202H — is NOT checked here: PostgreSQL
// checks it when the scan begins (exec.CheckSamplePercent, at a sampled
// scan's first batch: exec.NewSampledSource, on a worker too), so a scan that
// never begins answers, and EXPLAIN plans.
func TablesampleArgument(arg plansql.Node, opts ...expr.CompileOption) (pct float64, isNull bool, err error) {
	if err := tablesampleArgumentReadsNoRow(arg); err != nil {
		return 0, false, err
	}
	untypedLiteral := false
	if lit, ok := plansql.Unparen(arg).(*plansql.Lit); ok {
		switch lit.Kind {
		case plansql.LitNull:
			return 0, true, nil
		case plansql.LitString:
			untypedLiteral = true
		}
	}
	if !untypedLiteral {
		d, conf := DeclaredTypeOfNode(arg, nil)
		if d.Untyped {
			return 0, true, nil
		}
		if conf != expr.Decided || !realCoercible(d.ID) {
			name := "unknown"
			if conf == expr.Decided {
				name = PgTypeName(d.ID)
			}
			return 0, false, sqlerr.New("42804",
				"argument of TABLESAMPLE must be type real, not type %s", name)
		}
	}
	c, err := expr.Compile(&plansql.CastNode{Inner: arg, TypeName: "REAL"}, opts...)
	if err != nil {
		return 0, false, err
	}
	v, err := evalTablesampleArgument(c)
	if err != nil {
		return 0, false, err
	}
	switch x := v.(type) {
	case nil:
		return 0, true, nil
	case float32:
		return float64(x), false, nil
	case float64:
		return float64(float32(x)), false, nil
	}
	return 0, false, sqlerr.New("XX000", "TABLESAMPLE argument evaluated to %T, not real", v)
}

// realCoercible reports the types PostgreSQL coerces to real in a TABLESAMPLE
// argument: the integers, numeric and the two floats.
func realCoercible(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32, parquet.TypeFloat64, parquet.TypeDecimal:
		return true
	}
	return false
}

// tablesampleArgumentReadsNoRow refuses an argument that reads a row: what
// PostgreSQL raises for each (a column, an aggregate, a window function), and
// 0A000 for a subquery.
func tablesampleArgumentReadsNoRow(arg plansql.Node) error {
	var refusal error
	plansql.RewriteExpr(arg, func(n plansql.Node) (plansql.Node, bool) {
		if refusal != nil {
			return nil, false
		}
		switch v := n.(type) {
		case *plansql.ColRef:
			refusal = sqlerr.New("42703",
				"column %q does not exist: a TABLESAMPLE argument cannot reference a column", v.Column)
		case *plansql.WindowFuncNode:
			refusal = sqlerr.New("42P20", "window functions are not allowed in a TABLESAMPLE argument")
		case *plansql.FuncCallNode:
			if plansql.IsAggregate(v.Name) {
				refusal = sqlerr.New("42803", "aggregate functions are not allowed in a TABLESAMPLE argument")
			}
		case *plansql.SubqueryNode, *plansql.ExistsNode:
			refusal = sqlerr.New("0A000",
				"a subquery in a TABLESAMPLE argument is not supported: the argument is evaluated before any row exists")
		}
		return nil, false
	})
	return refusal
}

// evalTablesampleArgument evaluates the coerced argument once. A kernel raises
// a SQL error by panicking with a coded error; that is the argument's answer.
func evalTablesampleArgument(c expr.Expr) (v any, err error) {
	defer func() {
		if r := recover(); r != nil {
			if fe, ok := r.(exec.FatalEvalPanic); ok {
				err = fe.FatalEvalError()
				return
			}
			if e, ok := r.(error); ok && sqlerr.StateOf(e) != "" {
				err = e
				return
			}
			err = sqlerr.New("XX000", "evaluating the TABLESAMPLE argument: %v", r)
		}
	}()
	return c.Eval(&batch.RecordBatch{Len: 1}, 0), nil
}
