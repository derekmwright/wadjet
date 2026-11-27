// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// tdgBatch is one row: ts = 2024-01-01 00:00:00, ts2 = 2024-01-02 03:00:00,
// d = 2024-01-31, dur = 1 s, id = 1, both instants of row 1 on midnight
// (the dateArith day-count path).
func tdgBatch() *batch.RecordBatch {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "ts2", Type: parquet.TypeTimestamp},
		{Name: "d", Type: parquet.TypeDate},
		{Name: "dur", Type: parquet.TypeDuration},
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "m", Type: parquet.TypeTimestamp},
	}, 1)
	b.Columns[0].SetValue(0, "2024-01-01 00:00:00")
	b.Columns[1].SetValue(0, "2024-01-02 03:00:00")
	b.Columns[2].SetValue(0, "2024-01-31")
	b.Columns[3].SetValue(0, int64(1000000000))
	b.Columns[4].SetValue(0, int64(1))
	b.Columns[5].SetValue(0, "2024-01-03 00:00:00")
	b.Len = 1
	return b
}

// tdgRun evaluates e on row 0 through the entry point named: the boxed
// Eval, or a typed evaluator a numeric consumer calls.
func tdgRun(e Expr, b *batch.RecordBatch, entry string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			f, ok := r.(fatalEval)
			if !ok {
				panic(r)
			}
			out = "ERR " + sqlerr.StateOf(f.err)
			if strings.Contains(f.err.Error(), tsDiffGuardMessage) {
				out += " guard"
			}
		}
	}()
	switch entry {
	case "float":
		v, ok := e.(Float64Expr).EvalFloat64(b, 0)
		return fmt.Sprint(v, ok)
	case "int":
		v, ok := e.(Int64Expr).EvalInt64(b, 0)
		return fmt.Sprint(v, ok)
	}
	v := e.Eval(b, 0)
	if v == nil {
		return "NULL"
	}
	return fmt.Sprint(v)
}

// TestTimestampDifferenceGuardSites: the guard at every site where the
// expression layer can compile or evaluate `timestamp - timestamp` (#1713d
// round 8). Each site answered a number before it: milliseconds on the
// numeric paths, a whole day count on dateArith when both instants sit on
// midnight. The statement rule is not in this package, so this is the guard
// alone.
func TestTimestampDifferenceGuardSites(t *testing.T) {
	for _, c := range []struct {
		sql, node, entry, want string
	}{
		// BinOpNumeric: two column operands, decided when the mode resolves,
		// on each of its entry points.
		{"SELECT ts2 - ts", "*expr.BinOpNumeric", "eval", "ERR 0A000 guard"},
		{"SELECT ts2 - ts", "*expr.BinOpNumeric", "float", "ERR 0A000 guard"},
		{"SELECT ts2 - ts", "*expr.BinOpNumeric", "int", "ERR 0A000 guard"},
		{"SELECT m - ts", "*expr.BinOpNumeric", "eval", "ERR 0A000 guard"},
		// A consumer over the difference: the minus inside refuses.
		{"SELECT ts2 - ts > INTERVAL '1 hour'", "", "eval", "ERR 0A000 guard"},
		{"SELECT (ts2 - ts) / 1000", "", "eval", "ERR 0A000 guard"},
		{"SELECT CAST(ts2 - ts AS TEXT)", "", "eval", "ERR 0A000 guard"},
		{"SELECT extract(epoch FROM ts2 - ts)", "", "eval", "ERR 0A000 guard"},
		// BinOp (boxed): a column beside a temporal producer, decided once
		// the node meets the batch.
		{"SELECT CAST(d AS TIMESTAMP) - ts", "*expr.BinOp", "eval", "ERR 0A000 guard"},
		{"SELECT (ts2 + INTERVAL '1 day') - ts", "*expr.BinOp", "eval", "ERR 0A000 guard"},
		{"SELECT date_trunc('day', ts2) - date_trunc('day', ts)", "*expr.BinOp", "eval", "ERR 0A000 guard"},
		{"SELECT now() - ts", "*expr.BinOp", "eval", "ERR 0A000 guard"},
		// timestamp - date and date - timestamp (PostgreSQL: an interval).
		{"SELECT ts - d", "*expr.BinOpNumeric", "eval", "ERR 0A000 guard"},
		{"SELECT d - ts", "*expr.BinOpNumeric", "eval", "ERR 0A000 guard"},
		// Both producers decided at compile time: the node is built
		// refusing (the midnight pair was dateArith's day count, 1).
		{"SELECT TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'", "*expr.BinOp", "eval", "ERR 0A000 guard"},
		{"SELECT CAST(d AS TIMESTAMP) - CAST(DATE '2024-01-01' AS TIMESTAMP)", "*expr.BinOp", "eval", "ERR 0A000 guard"},
		// The quoted-literal arm (unknownTemporalArith): read as a
		// TIMESTAMP, then refused; interval input keeps 22007.
		{"SELECT ts2 - '2024-01-01'", "*expr.BinOp", "eval", "ERR 0A000 guard"},
		{"SELECT '2024-01-03' - ts2", "*expr.BinOp", "eval", "ERR 0A000 guard"},
		{"SELECT ts2 - '1 day'", "*expr.BinOp", "eval", "ERR 22007"},
		{"SELECT ts2 - 'xyz'", "*expr.BinOp", "eval", "ERR 22007"},
		// IS [NOT] NULL needs no value and answers.
		{"SELECT (ts2 - ts) IS NULL", "*expr.IsNull", "eval", "false"},
		{"SELECT (ts2 - ts) IS NOT NULL", "*expr.IsNull", "eval", "true"},
		{"SELECT (ts2 - NULL) IS NULL", "*expr.IsNull", "eval", "true"},
		{"SELECT (TIMESTAMP '2024-01-02 00:00:00' - ts) IS NULL", "*expr.IsNull", "eval", "false"},
		{"SELECT (ts2 - '2024-01-01') IS NULL", "*expr.IsNull", "eval", "false"},
		{"SELECT (ts2 - 'xyz') IS NULL", "*expr.IsNull", "eval", "ERR 22007"},
		// Controls: not a difference of instants.
		{"SELECT d - d", "", "eval", "0"},
		{"SELECT d - DATE '2024-01-01'", "", "eval", "30"},
		{"SELECT d - 1", "", "eval", "19752"},
		{"SELECT CAST(ts + INTERVAL '1 day' AS TEXT)", "", "eval", "2024-01-02 00:00:00"},
		{"SELECT CAST(ts - INTERVAL '1 day' AS TEXT)", "", "eval", "2023-12-31 00:00:00"},
		{"SELECT CAST(ts2 - CAST('1 hour' AS INTERVAL) AS TEXT)", "", "eval", "2024-01-02 02:00:00"},
		{"SELECT dur - dur", "", "eval", "0"},
		{"SELECT id - 1", "", "eval", "0"},
		{"SELECT extract(epoch FROM ts2) - extract(epoch FROM ts)", "", "eval", "97200"},
		{"SELECT ts2 > ts", "", "eval", "true"},
		{"SELECT (ts2 - NULL)", "", "eval", "NULL"},
	} {
		e := compileSelectCol(t, c.sql)
		if c.node != "" {
			if got := fmt.Sprintf("%T", e); got != c.node {
				t.Errorf("%s compiled to %s, want %s (the site under test)", c.sql, got, c.node)
			}
		}
		if got := tdgRun(e, tdgBatch(), c.entry); got != c.want {
			t.Errorf("%s [%s]: got %s, want %s", c.sql, c.entry, got, c.want)
		}
	}
}

// TestTimestampDifferenceGuardConstantFold: a constant fold of the
// statement rule (ConstantEvalError) does not report the guard as
// PostgreSQL's own refusal, so the rule's diagnostic stands; a constant
// evaluation with no batch (FoldTableFuncArg's shape) still refuses.
func TestTimestampDifferenceGuardConstantFold(t *testing.T) {
	sql := "SELECT TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'"
	e := compileSelectCol(t, sql)
	if got := tdgRun(e, nil, "eval"); got != "ERR 0A000 guard" {
		t.Errorf("no-batch evaluation: %s", got)
	}
	n := firstSelectNode(t, sql)
	if err := ConstantEvalError(n); err != nil {
		t.Errorf("ConstantEvalError reported the guard: %v", err)
	}
}

// firstSelectNode is a statement's first select item's expression.
func firstSelectNode(t *testing.T, sql string) plansql.Node {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	return info.Columns[0].ASTExpr
}
