// SPDX-License-Identifier: MIT

package scan

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// The CASE-FOLDING seam gate (#1209 line 3): the reference (PostgreSQL 17.11,
// postgres:17-alpine, `initdb --locale=C --encoding=UTF8`) maps ASCII letters
// only, in lower(), upper(), ILIKE and the regex case-insensitive forms. The
// engine has ONE case mapping (expr/pg_case_forms.go); this gate drives the 18
// representative pairs of the 2026-11-17 fold matrix through every path that
// folds: lower / upper a value at a time and over a column, ILIKE through
// every LIKE path the seam gate drives, and `~*` / the i flag / (?i).
//
// Each code is PostgreSQL's answer, measured (tooling crew v0256
// 1209d_evidence/fold18.sql): one letter per predicate in foldPreds order,
// then the hex of lower(u), upper(l), upper(u) and lower(l).
type foldPair struct {
	u, l, codes            string
	lowerU, upperL, upperU string
	lowerL                 string
}

var foldPairs = []foldPair{
	{"É", "é", "fffftffftfffff", "c389", "c3a9", "c389", "c3a9"},                                   // Latin-1 E acute
	{"ẞ", "ß", "fffftffftfffff", "e1ba9e", "c39f", "e1ba9e", "c39f"},                               // sharp s
	{"Σ", "σ", "fffftffftfffff", "cea3", "cf83", "cea3", "cf83"},                                   // sigma
	{"Σ", "ς", "fffftffftfffff", "cea3", "cf82", "cea3", "cf82"},                                   // final sigma
	{"İ", "i", "fffftffftfffff", "c4b0", "49", "c4b0", "69"},                                       // Turkish dotted I
	{"I", "ı", "fffftffftfffff", "69", "c4b1", "49", "c4b1"},                                       // Turkish dotless i
	{"Д", "д", "fffftffftfffff", "d094", "d0b4", "d094", "d0b4"},                                   // Cyrillic De
	{"Ա", "ա", "fffftffftfffff", "d4b1", "d5a1", "d4b1", "d5a1"},                                   // Armenian Ayb
	{"Ａ", "ａ", "fffftffftfffff", "efbca1", "efbd81", "efbca1", "efbd81"},                           // fullwidth A
	{"K", "k", "fffftffftfffff", "e284aa", "4b", "e284aa", "6b"},                                   // Kelvin sign
	{"S", "ſ", "fffftffftfffff", "73", "c5bf", "53", "c5bf"},                                       // long s
	{"Ǆ", "ǆ", "fffftffftfffff", "c784", "c786", "c784", "c786"},                                   // DZ caron
	{"ǅ", "ǆ", "fffftffftfffff", "c785", "c786", "c785", "c786"},                                   // Dz titlecase
	{"\U00010400", "\U00010428", "fffftffftfffff", "f0909080", "f09090a8", "f0909080", "f09090a8"}, // Deseret
	{"A", "a", "ttttftttfttttt", "61", "41", "41", "61"},                                           // ASCII control
	{"É", "é", "ttttftttfttttt", "65cc81", "45cc81", "45cc81", "65cc81"},                         // E + combining acute
	{"Ꭰ", "ꭰ", "fffftffftfffff", "e18ea0", "eaadb0", "e18ea0", "eaadb0"},                           // Cherokee
	{"Ა", "ა", "fffftffftfffff", "e1b290", "e18390", "e1b290", "e18390"},                           // Georgian
}

// foldPreds are the predicates over the subject column v (= u) and the column
// p (= l), in the order of foldPair.codes.
var foldPreds = []string{
	"lower(v) = p", "upper(p) = v", "v ILIKE p", "p ILIKE v", "v NOT ILIKE p", "(v || 'x') ILIKE (p || '%')",
	"v ~* p", "p ~* v", "v !~* p", "regexp_like(v, p, 'i')", "v ~ ('(?i)' || p)",
	"lower(v) = lower(p)", "upper(v) = upper(p)", "v ILIKE p ESCAPE '!'",
}

// foldLit is s as a SQL string literal (the pairs hold no quote).
func foldLit(s string) string { return "'" + s + "'" }

func TestCaseFoldSeam1209(t *testing.T) {
	for i, fp := range foldPairs {
		if len(fp.codes) != len(foldPreds) {
			t.Fatalf("pair %d: %d codes for %d predicates", i+1, len(fp.codes), len(foldPreds))
		}
		b := seamBatch(t, fp.u, fp.l)
		name := fmt.Sprintf("pair %d (%q, %q)", i+1, fp.u, fp.l)

		// lower / upper, a value at a time (literal and column) and over a
		// column (the vectorized kernel), against PostgreSQL's bytes.
		for _, c := range []struct{ fn, arg, want string }{
			{"lower", fp.u, fp.lowerU}, {"upper", fp.l, fp.upperL},
			{"upper", fp.u, fp.upperU}, {"lower", fp.l, fp.lowerL},
		} {
			col := "v"
			if c.arg == fp.l {
				col = "p"
			}
			got := map[string]string{
				"row literal": foldEvalHex(t, b, c.fn+"("+foldLit(c.arg)+")"),
				"row column":  foldEvalHex(t, b, c.fn+"("+col+")"),
				"vector":      foldVecHex(t, b, c.fn, col),
			}
			for path, g := range got {
				if g != c.want {
					t.Errorf("%s: %s(%q) via %s = %s, PostgreSQL 17.11 %s", name, c.fn, c.arg, path, g, c.want)
				}
			}
		}

		// Every predicate, over the columns and over literals (constant
		// folding).
		for j, pred := range foldPreds {
			want := fp.codes[j]
			if got := seamCode(seamExpr(t, b, pred)); got != want {
				t.Errorf("%s: %s over columns = %c, PostgreSQL 17.11 %c", name, pred, got, want)
			}
			lit := plansqlReplaceCols(pred, foldLit(fp.u), foldLit(fp.l))
			if got := seamCode(seamExpr(t, b, lit)); got != want {
				t.Errorf("%s: %s = %c, PostgreSQL 17.11 %c", name, lit, got, want)
			}
		}

		// ILIKE through every LIKE path: the operands folded by the one
		// case mapping (lower over the subject and over the pattern), then
		// matched by each matcher the LIKE seam gate drives.
		lv := foldEvalStr(t, b, "lower(v)")
		lp := foldEvalStr(t, b, "lower(p)")
		want := fp.codes[2]
		got := map[string]byte{}
		got["kernel.LikeMatch"] = seamCode(kernel.LikeMatch(lv, lp))
		cp := kernel.CompileLike(lp)
		got["kernel.LikePattern.Match"] = seamCode(cp.Match(lv))
		if m, err := compileLike(lp, false); err == nil {
			got["scan.compileLike"] = seamCode(m([]byte(lv)), nil)
		} else {
			t.Errorf("%s: scan.compileLike refused %q: %v", name, lp, err)
		}
		lb := seamBatch(t, lv, lp)
		if k := kernel.ResolveLikeFilterKernel(batch.TypeString, lp, false); k != nil {
			sel := k(lb.Columns[0], nil, lb.Len, make([]uint32, 0, 1))
			got["kernel.ResolveLikeFilterKernel"] = seamCode(len(sel) == 1, nil)
		}
		got["exec.LikeFilter"] = seamCode(seamPipe(t, lv, lp, exec.NewLikeFilter("v", lp, false)))
		got["exec.ColumnLike"] = seamCode(seamPipe(t, lv, lp, exec.NewFilter(exec.ColumnLike("v", lp, false))))
		for path, g := range got {
			if g != want {
				t.Errorf("%s: u ILIKE l via lower + %s = %c, PostgreSQL 17.11 %c", name, path, g, want)
			}
		}
	}
}

// TestCaseFoldOneValuePerExpression: the same lower(v) answers the same value
// whether it is projected, compared or used as a key (#1209: `SELECT
// lower(v) = 'école', lower(v)` answered `true, École`).
func TestCaseFoldOneValuePerExpression(t *testing.T) {
	b := seamBatch(t, "ÉCOLE", "école")
	if got := foldEvalStr(t, b, "lower(v)"); got != "École" {
		t.Errorf("lower(v) row = %q, PostgreSQL 17.11 %q", got, "École")
	}
	if got := foldVecHex(t, b, "lower", "v"); got != hex.EncodeToString([]byte("École")) {
		t.Errorf("lower(v) vector = %s, PostgreSQL 17.11 %x", got, "École")
	}
	for _, c := range []struct {
		sql  string
		want byte
	}{
		{"lower(v) = 'école'", 'f'},
		{"lower(v) = 'École'", 't'},
		{"lower(v) = p", 'f'},
		{"lower(v) = lower(p)", 'f'},
		{"upper(p) = 'éCOLE'", 't'},
		{"lower('ÉCOLE') = 'école'", 'f'},
		{"v ILIKE 'é%'", 'f'},
		{"v ~* '^é'", 'f'},
		{"v ILIKE 'eCOLE'", 'f'},
		{"'ECOLE' ILIKE 'ecole'", 't'},
	} {
		if got := seamCode(seamExpr(t, b, c.sql)); got != c.want {
			t.Errorf("%s = %c, PostgreSQL 17.11 %c", c.sql, got, c.want)
		}
	}
}

// TestILikeEscapeReadsTheEscapeBeforeFolding (#1209 line 2): `x ILIKE p
// ESCAPE e` is `x ~~* like_escape(p, e)` on PostgreSQL, so the escape is read
// in the pattern as written and the rewritten pattern is folded after. Each
// answer is PostgreSQL 17.11's (--locale=C, UTF8). Rows from crew/1209b-r1's
// test, without its `_`-over-a-multi-byte-subject row (#1209 line 1, held),
// plus the non-ASCII rows the line-2 prototype alone turned wrong (1209c).
func TestILikeEscapeReadsTheEscapeBeforeFolding(t *testing.T) {
	b := seamBatch(t, "a", "a")
	for _, c := range []struct {
		sql  string
		want byte
	}{
		{`'a' ILIKE 'Xa' ESCAPE 'X'`, 't'},  // an upper-case escape names itself
		{`'xa' ILIKE 'xa' ESCAPE 'X'`, 't'}, // a lower-case x is not the escape X
		{`'xa' ILIKE 'Xa' ESCAPE 'x'`, 't'}, // nor an upper-case X the escape x
		{`'A' ILIKE 'xa' ESCAPE 'X'`, 'f'},
		{`'a' ILIKE '#a' ESCAPE '#'`, 't'}, // a non-letter escape
		{`'_' ILIKE 'X_' ESCAPE 'X'`, 't'},
		{`'b' ILIKE 'X_' ESCAPE 'X'`, 'f'},
		{`'A%' ILIKE 'aX%' ESCAPE 'X'`, 't'},
		{`'ab' ILIKE 'aX%' ESCAPE 'X'`, 'f'},
		{`'A' NOT ILIKE 'Xa' ESCAPE 'X'`, 'f'},
		{`'é' ILIKE 'é_' ESCAPE 'é'`, 'f'}, // a multi-byte escape
		{`'_' ILIKE 'é_' ESCAPE 'é'`, 't'},
		{`'éx' ILIKE 'É_' ESCAPE 'é'`, 'f'}, // É is not the escape é, and folds to nothing
		{`'aÉ' ILIKE 'Aé' ESCAPE 'a'`, 'f'},
		{`'é' ILIKE 'ÉÉ' ESCAPE 'É'`, 'f'},
		{`'É' ILIKE 'éÉ' ESCAPE 'é'`, 't'},
		{`'Éa' ILIKE 'ÉXA' ESCAPE 'X'`, 't'},
		{`'a' ILIKE 'a' ESCAPE '!!'`, 'a'},
		{`NULL ILIKE 'a' ESCAPE '!!'`, 'a'},
		{`v ILIKE 'Xa' ESCAPE 'X'`, 't'},
		{`v NOT ILIKE p ESCAPE 'A'`, 'f'},
	} {
		if got := seamCode(seamExpr(t, b, c.sql)); got != c.want {
			t.Errorf("%s: got %c, want %c (PostgreSQL 17.11)", c.sql, got, c.want)
		}
	}
	// A NULL escape is NULL, as on PostgreSQL.
	node, err := plansql.ParseExpression(`'a' ILIKE 'a' ESCAPE NULL`)
	if err != nil {
		t.Fatal(err)
	}
	e, err := expr.Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	if v := e.Eval(b, 0); v != nil {
		t.Errorf("'a' ILIKE 'a' ESCAPE NULL: got %v, want NULL", v)
	}
}

// plansqlReplaceCols writes pred with the columns v and p as the literals u
// and l: the constant-folded form of the same predicate.
func plansqlReplaceCols(pred, u, l string) string {
	out := make([]byte, 0, len(pred)+len(u)+len(l))
	for i := 0; i < len(pred); i++ {
		c := pred[i]
		word := (i == 0 || !foldIdentByte(pred[i-1])) && (i+1 == len(pred) || !foldIdentByte(pred[i+1]))
		switch {
		case c == 'v' && word:
			out = append(out, u...)
		case c == 'p' && word:
			out = append(out, l...)
		default:
			out = append(out, c)
		}
	}
	return string(out)
}

func foldIdentByte(c byte) bool {
	return c == '_' || c == '\'' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

func foldCompile(t *testing.T, sql string) expr.Expr {
	t.Helper()
	node, err := plansql.ParseExpression(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	e, err := expr.Compile(node)
	if err != nil {
		t.Fatalf("compile %q: %v", sql, err)
	}
	return e
}

func foldEvalStr(t *testing.T, b *batch.RecordBatch, sql string) string {
	t.Helper()
	v := foldCompile(t, sql).Eval(b, 0)
	s, ok := v.(string)
	if !ok {
		t.Fatalf("%s answered %v (%T), not a string", sql, v, v)
	}
	return s
}

func foldEvalHex(t *testing.T, b *batch.RecordBatch, sql string) string {
	t.Helper()
	return hex.EncodeToString([]byte(foldEvalStr(t, b, sql)))
}

// foldVecHex is fn(col) through the vectorized kernel (FuncCall.EvalVec).
func foldVecHex(t *testing.T, b *batch.RecordBatch, fn, col string) string {
	t.Helper()
	e := foldCompile(t, fn+"("+col+")")
	ve, ok := e.(expr.VecExpr)
	if !ok {
		t.Fatalf("%s(%s) compiled to %T, which has no vectorized path", fn, col, e)
	}
	if !expr.HasVecKernel(fn) {
		t.Fatalf("%s has no vectorized kernel", fn)
	}
	out := batch.NewVector(batch.TypeString, b.Len)
	ve.EvalVec(b, out, b.Len)
	return hex.EncodeToString(out.BytesData.Value(0))
}
