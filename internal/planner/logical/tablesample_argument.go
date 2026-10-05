// SPDX-License-Identifier: MIT

package logical

import (
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// tablesampleEvaluator types and evaluates a TABLESAMPLE argument as real. It
// is physical.TablesampleArgument, installed by package physical at init
// (SetTablesampleEvaluator): the expression typer it needs lives there, and
// physical imports this package, not the other way round.
var tablesampleEvaluator func(arg plansql.Node, opts ...expr.CompileOption) (pct float64, isNull bool, err error)

// SetTablesampleEvaluator installs the TABLESAMPLE argument's evaluation.
// Package physical calls it once, at init. opts reach its compile: the
// statement clock, for an argument that reads one (BindClockFolds).
func SetTablesampleEvaluator(f func(arg plansql.Node, opts ...expr.CompileOption) (float64, bool, error)) {
	tablesampleEvaluator = f
}

// tablesampleArgument evaluates the argument once, where the scan is built, so
// a value real cannot hold (22003) or that is not a real (42804) is refused
// when the statement is planned — EXPLAIN too, as PostgreSQL's planner folds
// it. The RANGE is not checked here: PostgreSQL checks it when the scan
// begins, so a scan that never begins (WHERE false, LIMIT 0, EXPLAIN) answers.
func tablesampleArgument(arg plansql.Node) (float64, bool, error) {
	if arg == nil {
		return 0, false, sqlerr.New("42601", "expected percentage in TABLESAMPLE")
	}
	if tablesampleEvaluator == nil {
		return 0, false, sqlerr.New("XX000", "TABLESAMPLE: no argument evaluator is installed")
	}
	return tablesampleEvaluator(arg)
}
