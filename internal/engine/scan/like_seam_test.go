// SPDX-License-Identifier: MIT

package scan

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The text LIKE SEAM gate (#1209, ADR-0023's seam pattern): one pattern table,
// measured on PostgreSQL 17.11, driven through EVERY code path that matches a
// text LIKE pattern, failing when any path disagrees with PostgreSQL or with
// another path:
//
//	scan.compileLike                 the scan's pushed filter (dict and plain pages)
//	kernel.ResolveLikeFilterKernel   the exec LIKE filter's vectorized kernel
//	exec.LikeFilter                  the operator (kernel, or row by row for a raising pattern)
//	exec.ColumnLike                  the row predicate
//	expr.Like                        the evaluator: a literal pattern and a COLUMN pattern
//	expr like_escape                 LIKE ... ESCAPE
//	kernel.LikeMatch / LikePattern   the implementation itself
//
// Each path exists in one place and calls kernel.CompileLike / LikeMatch /
// LikeEscape; before #1209 there were five matchers, and the first four read
// the backslash as an ordinary character. The statement-level matrix is
// wadjet.TestTextLike1209Matrix.

type seamCell struct{ v, p string }

// seamCells are wadjet/text_like_1209_test.go's ltPairs, in order.
var seamCells = []seamCell{
	{`a%b`, `a\%b`}, {`axb`, `a\%b`}, {`a_b`, `a\_b`}, {`axb`, `a\_b`}, {`a\b`, `a\\b`},
	{`ab`, `a\\b`}, {`ab`, `a\b`}, {`a\b`, `a\b`}, {`abc`, `abc\`}, {`abcd`, `abc\`},
	{``, `\`}, {`A`, `\`}, {`A`, `%\`}, {`abc`, ``}, {``, ``},
	{``, `%`}, {`a%`, `a\%%`}, {`ab`, `a\%%`}, {`ab%cd`, `ab\%c%`}, {`ab\xc`, `ab\%c%`},
	{`%x`, `\%%`}, {`x%`, `%\%`}, {`a%b`, `%\%%`}, {`ab`, `%\%%`}, {`abc`, `%`},
	{`ABC`, `a%`}, {`a\`, `a\\`}, {`a\`, `%\\`}, {`a!%b`, `a!%b`}, {`a%b`, `a!%b`},
	{`axb`, `a!%b`}, {`a!b`, `a!!b`}, {`abc`, `abc!`}, {`abcd`, `abc!`}, {`a%b`, `aé%b`},
	{`é`, `_`}, {`é`, `é`}, {`éa`, `é\a`}, {`é%`, `é\%`}, {`aé`, `%\é`},
	{`a\xb`, `a\%b`}, {`a_b`, `a\_%`}, {`ÉCOLE`, `é%`}, {`a`, `Xa`}, {`aXb`, `aXXb`},
	{`a\%b`, `a\\\%b`}, {`google.com`, `%oo%`}, {`google`, `g_o%e`},
}

// seamPG is PostgreSQL 17.11's answer per escape spelling (no ESCAPE clause,
// and three ESCAPE clauses), one letter per seamCells entry: t true, f false,
// a 22025. Measured with wadjet.TestTextLike1209Dump (LT_PG_DSN); the same
// strings are wadjet.ltAnswers' `<op>|col|lit` rows.
var seamPG = []struct {
	esc   string // "" with def = the default (backslash) escape
	def   bool
	codes string
}{
	{"", true, "tftftftffafaaftttftftttftftttfffffftttttftfffttt"},
	{"!", false, "ffffffftffffffttffftfffftfffftftfafttffftffffftt"},
	{"", false, "ffffffftffffffttffftfffftffftffffffttffftffffftt"},
	{"é", false, "ffffffftffffffttffftfffftffftfffffttaffftffffftt"},
	{`\`, false, "tftftftffafaaftttftftttftftttfffffftttttftfffttt"},
}

// seamResidual is a cell every path answers alike and PostgreSQL answers
// otherwise: `_` matches one byte here and one character on the server, so
// `'é' LIKE '_'` is false (wadjet.ltResidual names the cause).
var seamResidual = map[int]byte{35: 'f'}

func seamCode(m bool, err error) byte {
	if err != nil {
		if sqlerr.StateOf(err) == "22025" {
			return 'a'
		}
		return '?'
	}
	if m {
		return 't'
	}
	return 'f'
}

func seamNot(c byte) byte {
	switch c {
	case 't':
		return 'f'
	case 'f':
		return 't'
	}
	return c
}

// seamBatch is one string row: v, and the pattern as column p.
func seamBatch(t *testing.T, v, p string) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{{Name: "v", Type: parquet.TypeString}, {Name: "p", Type: parquet.TypeString}}
	return batch.FromRows(schema, []map[string]any{{"v": v, "p": p}})
}

// seamPipe runs one filter over one row and reports whether the row survived.
func seamPipe(t *testing.T, v, p string, op exec.UnaryOperator) (bool, error) {
	t.Helper()
	schema := []exec.Column{{Name: "v", Type: parquet.TypeString}, {Name: "p", Type: parquet.TypeString}}
	src := exec.NewSliceSource(schema, []map[string]any{{"v": v, "p": p}})
	sink := &exec.CollectSink{}
	pipe := &exec.Pipeline{Source: src, Ops: []exec.UnaryOperator{op}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		return false, err
	}
	return len(sink.Rows) == 1, nil
}

func seamSQLLit(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// seamExpr compiles and evaluates a predicate over one row; a FatalEvalPanic
// comes back as its error, as the pipeline drivers return it.
func seamExpr(t *testing.T, b *batch.RecordBatch, sql string) (m bool, err error) {
	t.Helper()
	node, perr := plansql.ParseExpression(sql)
	if perr != nil {
		t.Fatalf("parse %q: %v", sql, perr)
	}
	e, cerr := expr.Compile(node)
	if cerr != nil {
		t.Fatalf("compile %q: %v", sql, cerr)
	}
	defer func() {
		if r := recover(); r != nil {
			err = exec.RecoverFatalEval(r)
		}
	}()
	v := e.Eval(b, 0)
	bv, ok := v.(bool)
	if !ok {
		t.Fatalf("%s: answered %v (%T), not a bool", sql, v, v)
	}
	return bv, nil
}

func TestTextLikeSeam1209(t *testing.T) {
	for _, row := range seamPG {
		if len(row.codes) != len(seamCells) {
			t.Fatalf("escape %q: %d codes for %d cells", row.esc, len(row.codes), len(seamCells))
		}
		for i, c := range seamCells {
			want := row.codes[i]
			if res, ok := seamResidual[i]; ok {
				want = res
			}
			name := fmt.Sprintf("%d %q LIKE %q", i, c.v, c.p)
			// The pattern every path sees in backslash form: the ESCAPE
			// clause's rewrite, or the pattern itself.
			pat := c.p
			if !row.def {
				name += fmt.Sprintf(" ESCAPE %q", row.esc)
				rp, err := kernel.LikeEscape(c.p, row.esc)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				pat = rp
			}
			got := map[string]byte{}
			got["kernel.LikeMatch"] = seamCode(kernel.LikeMatch(c.v, pat))
			cp := kernel.CompileLike(pat)
			got["kernel.LikePattern.Match"] = seamCode(cp.Match(c.v))
			if fn, ok := cp.Matcher(); ok {
				got["kernel.LikePattern.Matcher"] = seamCode(fn(c.v), nil)
			} else if !cp.MayRaise() {
				t.Errorf("%s: no Matcher for a pattern that cannot raise", name)
			}
			b := seamBatch(t, c.v, pat)
			for _, negate := range []bool{false, true} {
				sfx := ""
				fix := func(x byte) byte { return x }
				if negate {
					sfx, fix = " NOT", seamNot
				}
				if m, err := compileLike(pat, negate); err == nil {
					got["scan.compileLike"+sfx] = fix(seamCode(m([]byte(c.v)), nil))
				} else if !cp.MayRaise() {
					t.Errorf("%s: scan.compileLike refused a pattern that cannot raise: %v", name, err)
				}
				if k := kernel.ResolveLikeFilterKernel(batch.TypeString, pat, negate); k != nil {
					sel := k(b.Columns[0], nil, b.Len, make([]uint32, 0, 1))
					got["kernel.ResolveLikeFilterKernel"+sfx] = fix(seamCode(len(sel) == 1, nil))
				} else if !cp.MayRaise() {
					t.Errorf("%s: no kernel for a pattern that cannot raise", name)
				}
				got["exec.LikeFilter"+sfx] = fix(seamCode(seamPipe(t, c.v, pat, exec.NewLikeFilter("v", pat, negate))))
				got["exec.ColumnLike"+sfx] = fix(seamCode(seamPipe(t, c.v, pat, exec.NewFilter(exec.ColumnLike("v", pat, negate)))))
			}
			got["expr.Like literal"] = seamCode(seamExpr(t, b, "v LIKE "+seamSQLLit(pat)))
			got["expr.Like column"] = seamCode(seamExpr(t, b, "v LIKE p"))
			got["expr.Like NOT"] = seamNot(seamCode(seamExpr(t, b, "v NOT LIKE "+seamSQLLit(pat))))
			if !row.def {
				got["expr like_escape"] = seamCode(seamExpr(t, b, "v LIKE "+seamSQLLit(c.p)+" ESCAPE "+seamSQLLit(row.esc)))
			}
			for path, g := range got {
				if g != want {
					t.Errorf("%s via %s: got %c, want %c (PostgreSQL 17.11)", name, path, g, want)
				}
			}
		}
	}
}

// TestTextLikeEscapeRefusesALongEscape: an ESCAPE of more than one character is
// 22025 whatever the value is, a NULL value included (#1209).
func TestTextLikeEscapeRefusesALongEscape(t *testing.T) {
	b := seamBatch(t, "a", "a")
	for _, sql := range []string{`v LIKE 'a' ESCAPE '!!'`, `NULL LIKE 'a' ESCAPE '!!'`, `v LIKE 'a' ESCAPE 'éé'`} {
		if _, err := seamExpr(t, b, sql); sqlerr.StateOf(err) != "22025" {
			t.Errorf("%s: got %v, want 22025", sql, err)
		}
	}
}
