// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// smNumSpelling is one text of PostgreSQL's numeric input grammar, as the
// outer operand of a membership against a NUMERIC body.
//
//   - text is the literal's text, and constant its SQL numeric-constant
//     spelling ("" where the SQL grammar has none: whitespace, NaN, a
//     radix prefix, a refused text).
//   - scale is the scale of the NUMERIC(38, scale) the literal is typed as
//     (-1 where it is refused): the literal's own digits, by
//     batch.DecimalTextType of its value.
//   - disp is how this engine answers where it does not answer PostgreSQL's
//     rows: "22003" — a number no DECIMAL(38,s) holds exactly (more than 38
//     significant digits, a digit past scale 38, NaN, ±Infinity), which
//     PostgreSQL's unconstrained numeric compares and this engine refuses
//     rather than compare approximately (docs/postgres-differences.md); and
//     "22P02" — PostgreSQL 16's radix and digit-separator forms, which this
//     engine's numeric input refuses (#634). "" is PostgreSQL's own answer,
//     its refusal included.
type smNumSpelling struct {
	tag, text, constant string
	scale               int
	disp                string
}

// smNumSpellings is the grammar, enumerated once: every family PostgreSQL's
// numeric input accepts — sign, leading and trailing zeros, no integer part,
// no fraction, an exponent in either case with either sign, surrounding
// whitespace — each at three values against members 12.5 / 13.25 / 14 (the
// NUMERIC(38,0) body holds 12 / 13 / 14): H = 14 (every body's member), M =
// 14 + 1e-19 (a member's neighbour past float8's 17 significant digits:
// float8 reads it AS the member) and F = 12.5 (a fractional member); then
// the spellings whose written digits exceed 38 but whose VALUE fits once its
// zeros go, the values no DECIMAL(38,s) holds, the texts both input functions
// refuse, and the PostgreSQL 16 forms only PostgreSQL reads.
func smNumSpellings() []smNumSpelling {
	z := strings.Repeat
	sp := func(tag, text, constant string, scale int) smNumSpelling {
		return smNumSpelling{tag: tag, text: text, constant: constant, scale: scale}
	}
	div := func(tag, text, constant, state string) smNumSpelling {
		return smNumSpelling{tag: tag, text: text, constant: constant, scale: -1, disp: state}
	}
	const m = "14.0000000000000000001"
	return []smNumSpelling{
		sp("plain/H", "14", "14", 0), sp("plain/M", m, m, 19), sp("plain/F", "12.5", "12.5", 1),
		sp("plus/H", "+14", "+14", 0), sp("plus/M", "+"+m, "+"+m, 19), sp("plus/F", "+12.5", "+12.5", 1),
		sp("minus/H", "-14", "-14", 0), sp("minus/M", "-"+m, "-"+m, 19), sp("minus/F", "-12.5", "-12.5", 1),
		sp("lead0/H", "00014", "00014", 0), sp("lead0/M", "000"+m, "000"+m, 19), sp("lead0/F", "0012.5", "0012.5", 1),
		sp("trail0/H", "14.000", "14.000", 3), sp("trail0/M", m+"00", m+"00", 21), sp("trail0/F", "12.50", "12.50", 2),
		sp("noFrac/H", "14.", "14.", 0),
		sp("noInt/H", ".14e2", ".14e2", 0), sp("noInt/M", ".140000000000000000001e2", ".140000000000000000001e2", 19),
		sp("noInt/F", ".125e2", ".125e2", 1), sp("noInt/miss", ".5", ".5", 1),
		sp("exp/H", "1.4e1", "1.4e1", 0), sp("exp/M", "1.40000000000000000001e1", "1.40000000000000000001e1", 19),
		sp("exp/F", "1.25e1", "1.25e1", 1),
		sp("expUpper/H", "1.4E1", "1.4E1", 0), sp("expUpper/M", "1.40000000000000000001E1", "1.40000000000000000001E1", 19),
		sp("expUpper/F", "1.25E1", "1.25E1", 1),
		sp("expPlus/H", "0.14e+2", "0.14e+2", 0), sp("expPlus/M", "0.140000000000000000001e+2", "0.140000000000000000001e+2", 19),
		sp("expPlus/F", "0.125e+2", "0.125e+2", 1),
		sp("expMinus/H", "1400e-2", "1400e-2", 2), sp("expMinus/M", "140000000000000000001e-19", "140000000000000000001e-19", 19),
		sp("expMinus/F", "1250e-2", "1250e-2", 2),
		sp("expZero/H", "14e0", "14e0", 0), sp("expZero/F", "125E-1", "125E-1", 1),
		sp("space/H", " 14 ", "", 0), sp("space/M", "  "+m+"  ", "", 19), sp("space/F", " 12.5 ", "", 1),
		sp("tab/H", "\t14\t", "", 0),
		// Written wider than 38 digits, a value that fits once its zeros go.
		sp("trail0Wide/H", "14."+z("0", 40), "14."+z("0", 40), 0),
		sp("trail0Wide/F", "12.5"+z("0", 40), "12.5"+z("0", 40), 1),
		sp("lead0Wide/H", z("0", 40)+"14", z("0", 40)+"14", 0),
		sp("lead0Wide/M", z("0", 40)+m, z("0", 40)+m, 19),
		sp("lead0Wide/F", z("0", 40)+"12.5", z("0", 40)+"12.5", 1),
		sp("expWide/H", "14"+z("0", 40)+"e-40", "14"+z("0", 40)+"e-40", 0),
		sp("expWide/F", "125"+z("0", 40)+"e-41", "125"+z("0", 40)+"e-41", 1),
		sp("zeroWide/miss", "0."+z("0", 45), "0."+z("0", 45), 0),
		// 38 significant digits, the carrier's full width: a neighbour of 14.
		sp("width38/M", "14."+z("0", 35)+"1", "14."+z("0", 35)+"1", 36),
		// No DECIMAL(38,s) holds the value.
		div("over38Frac", "14."+z("0", 39)+"1", "14."+z("0", 39)+"1", "22003"),
		div("over38Neg", "-14."+z("0", 39)+"1", "-14."+z("0", 39)+"1", "22003"),
		div("scale39", "0."+z("0", 38)+"1", "0."+z("0", 38)+"1", "22003"),
		div("expScale", "1e-40", "1e-40", "22003"),
		div("over38Int", "1"+z("0", 40), "1"+z("0", 40), "22003"),
		div("expInt", "1e40", "1e40", "22003"),
		div("expHuge", "1e1000", "1e1000", "22003"),
		div("nan", "NaN", "", "22003"),
		div("inf", "Infinity", "", "22003"),
		div("negInf", "-Infinity", "", "22003"),
		div("infShort", "inf", "", "22003"),
		// Both input functions refuse the text (PostgreSQL's own 22P02).
		sp("bad/junk", "zz", "", -1), sp("bad/expEmpty", "1e", "", -1), sp("bad/dot", ".", "", -1),
		sp("bad/empty", "", "", -1), sp("bad/twoDots", "1.2.3", "", -1), sp("bad/innerSpace", "1 4", "", -1),
		sp("bad/twoSigns", "+-14", "", -1), sp("bad/trailJunk", "14x", "", -1), sp("bad/signedNaN", "+NaN", "", -1),
		sp("bad/nbsp", "14\u00a0", "", -1), sp("bad/twoUnderscores", "1__4", "", -1), sp("bad/hexFloat", "0x1.8p1", "", -1),
		// PostgreSQL 16's forms, which this engine's numeric input refuses.
		div("radix/hex", "0x0E", "", "22P02"), div("radix/octal", "0o16", "", "22P02"),
		div("radix/binary", "0b1110", "", "22P02"), div("radix/underscore", "1_4", "", "22P02"),
		div("radix/underscoreFrac", "1_2.5", "", "22P02"),
	}
}

// smNumSpecial is NaN or ±Infinity: numeric values with no DECIMAL bit
// pattern (ADR-0024 item 6).
func smNumSpecial(s smNumSpelling) bool {
	switch s.tag {
	case "nan", "inf", "negInf", "infShort":
		return true
	}
	return false
}

// smNumBody is one membership body: NUMERIC(18,4), NUMERIC(38,10), a
// NUMERIC with no modifier, and a bigint — numeric = bigint is numeric in
// PostgreSQL, so a numeric literal meets an integer member at its own
// digits too (a QUOTED literal alone takes bigint there, which the
// membership table's lit/ rows gate).
type smNumBody struct{ key, tbl, col string }

func smNumBodies() []smNumBody {
	return []smNumBody{{"dec", "st_pair", "v_dec"}, {"n38", "sm_num", "v_n38"}, {"n", "sm_num", "v_n"}, {"i64", "st_pair", "v_i64"}}
}

// smNumShape is one spelling of the outer operand around the literal's text.
// ruled is true where the membership's typing rule decides it (the
// literal's own digits, or 22003); an explicit NUMERIC(p,s) is the CAST's
// own reading, which PostgreSQL rounds to the typmod before it compares.
type smNumShape struct {
	key   string
	outer func(s smNumSpelling) (string, bool)
	ruled bool
	ops   map[string][]string // body key → ops
}

func smNumShapes() []smNumShape {
	quoted := func(s smNumSpelling) string { return "'" + s.text + "'" }
	all := []string{"in", "notIn", "eqAny", "neAll", "corrIn", "sel"}
	four := []string{"in", "notIn", "corrIn", "sel"}
	two := []string{"in", "corrIn"}
	return []smNumShape{
		{"q", func(s smNumSpelling) (string, bool) { return quoted(s), true }, true,
			map[string][]string{"dec": all, "n38": four, "n": four}},
		{"cast", func(s smNumSpelling) (string, bool) { return "CAST(" + quoted(s) + " AS NUMERIC)", true }, true,
			map[string][]string{"dec": four, "n38": two, "n": two, "i64": four}},
		{"castDecimal", func(s smNumSpelling) (string, bool) { return "CAST(" + quoted(s) + " AS decimal)", true }, true,
			map[string][]string{"dec": two, "i64": two}},
		{"colon", func(s smNumSpelling) (string, bool) { return quoted(s) + "::numeric", true }, true,
			map[string][]string{"dec": two, "i64": two}},
		{"const", func(s smNumSpelling) (string, bool) { return s.constant, s.constant != "" }, true,
			map[string][]string{"dec": four, "n38": four, "n": four, "i64": four}},
		{"castPS", func(s smNumSpelling) (string, bool) { return "CAST(" + quoted(s) + " AS NUMERIC(18,4))", true }, false,
			map[string][]string{"dec": two, "n38": two, "n": two, "i64": two}},
		{"castPS38", func(s smNumSpelling) (string, bool) { return "CAST(" + quoted(s) + " AS NUMERIC(38,20))", true }, false,
			map[string][]string{"dec": two}},
	}
}

// smNumGrammarCell is one generated cell and the engine's expected
// disposition where it differs from PostgreSQL's answer.
type smNumGrammarCell struct {
	name, sql, disp, text string
}

func smNumGrammarCells() []smNumGrammarCell {
	var out []smNumGrammarCell
	for _, sh := range smNumShapes() {
		for _, b := range smNumBodies() {
			ops := sh.ops[b.key]
			for _, s := range smNumSpellings() {
				x, ok := sh.outer(s)
				if !ok {
					continue
				}
				disp := s.disp
				if !sh.ruled && disp == "22003" && !smNumSpecial(s) {
					// A finite number under an explicit NUMERIC(p,s) is the
					// cast's own rounding (or PostgreSQL's own 22003), not
					// the membership's typing. NaN and ±Infinity stay this
					// carrier's refusal (ADR-0024 item 6).
					disp = ""
				}
				from := b.tbl + " a"
				body := "SELECT r." + b.col + " FROM " + b.tbl + " r"
				for _, op := range ops {
					var sql string
					switch op {
					case "in":
						sql = "SELECT a.id FROM " + from + " WHERE " + x + " IN (" + body + ")"
					case "notIn":
						sql = "SELECT a.id FROM " + from + " WHERE " + x + " NOT IN (" + body + " WHERE r.id <= 3)"
					case "eqAny":
						sql = "SELECT a.id FROM " + from + " WHERE " + x + " = ANY (" + body + ")"
					case "neAll":
						sql = "SELECT a.id FROM " + from + " WHERE " + x + " <> ALL (" + body + " WHERE r.id <= 3)"
					case "corrIn":
						sql = "SELECT a.id FROM " + from + " WHERE " + x + " IN (" + body + " WHERE r.id = a.id)"
					case "sel":
						sql = "SELECT a.id, " + x + " IN (" + body + " WHERE r.id <= 3) AS m FROM " + from
					}
					out = append(out, smNumGrammarCell{
						name: "numg/" + sh.key + "/" + op + "/" + b.key + "/" + s.tag,
						sql:  sql, disp: disp, text: strings.TrimSpace(s.text),
					})
				}
			}
		}
	}
	out = append(out, smNumWideIntCells()...)
	return append(out, smNumConstOuterCells()...)
}

// smNumConstOuterCells are the membership's CONSTANT-VALUED outer operands
// (review round 4, B9): an expression over numeric constants, not a
// literal — a CASE (with a constant condition, or one that reads a column
// but chooses among constants), COALESCE, NULLIF, GREATEST, LEAST (beside a
// NUMERIC, bigint or float8 column too), a unary
// minus of an expression, a nested bare CAST, a bare CAST over text or over
// an integer constant, a choice beside a column, arithmetic over a choice —
// at the grammar's three values (H = 14, a member of both bodies; M = 14 +
// 1e-19, which float8 reads AS 14; F = 12.5) × IN, NOT IN, correlated IN /
// NOT IN and the SELECT list, against NUMERIC(18,4) and bigint bodies.
// PostgreSQL computes each as the exact numeric its constants spell. This
// engine evaluated the constant as a double before the membership saw it
// (ADR-0024's choice and cast declarations), so M matched the member 14;
// MemberProbe now folds it at plan time and types the result by the
// literal's rule, a division at PostgreSQL's select_div_scale. A function
// the fold does not compute that evaluates as a double while a numeric
// constant feeds it (sqrt) is refused 0A000; an explicit float CAST is
// PostgreSQL's own double and matches.
func smNumConstOuterCells() []smNumGrammarCell {
	shapes := []struct {
		key string
		x   func(v string) string
	}{
		{"case", func(v string) string { return "CASE WHEN a.id > 0 THEN " + v + " END" }},
		{"caseTrue", func(v string) string { return "CASE WHEN true THEN " + v + " END" }},
		{"caseElse", func(v string) string { return "CASE WHEN a.id > 0 THEN " + v + " ELSE 0 END" }},
		{"caseCol", func(v string) string { return "CASE WHEN a.id > 0 THEN " + v + " ELSE a.v_dec END" }},
		{"coalesce", func(v string) string { return "COALESCE(" + v + ", 0)" }},
		{"coalCol", func(v string) string { return "COALESCE(" + v + ", a.v_dec)" }},
		// Beside a float8 column the choice is double precision in
		// PostgreSQL too (numeric resolves to float8), so M matches 14 there:
		// the typed constant must not make the choice exact.
		{"coalF64", func(v string) string { return "COALESCE(a.v_f64, " + v + ")" }},
		{"caseF64", func(v string) string { return "CASE WHEN a.id > 2 THEN " + v + " ELSE a.v_f64 END" }},
		{"greatestF64", func(v string) string { return "GREATEST(a.v_f64, " + v + ")" }},
		{"coalI64", func(v string) string { return "COALESCE(a.v_i64, " + v + ")" }},
		{"nullif", func(v string) string { return "NULLIF(" + v + ", 0)" }},
		{"greatest", func(v string) string { return "GREATEST(" + v + ", 1)" }},
		{"least", func(v string) string { return "LEAST(" + v + ", 20)" }},
		{"negneg", func(v string) string { return "-(-" + v + ")" }},
		{"colon2", func(v string) string { return "'" + v + "'::numeric::numeric" }},
		{"castText", func(v string) string { return "CAST(CAST('" + v + "' AS TEXT) AS NUMERIC)" }},
		{"castConst", func(v string) string { return "CAST(" + v + " AS NUMERIC)" }},
		{"coalPlus", func(v string) string { return "COALESCE(" + v + ", 0) + 0" }},
		{"castF8", func(v string) string { return "CAST(" + v + " AS DOUBLE PRECISION)" }},
		{"div", func(v string) string { return v + " / 1" }},
		// A division of numerics keeps PostgreSQL's select_div_scale
		// digits: (M / 7) * 7 is 14.0000000000000000000 (a match) and
		// (12.5 / 3.0) * 3 is 12.5000000000000001 (none), where exact
		// rationals and float8 each answer the other way.
		{"divMul7", func(v string) string { return "(" + v + " / 7) * 7" }},
		{"div3", func(v string) string { return "(" + v + " / 3.0) * 3" }},
		// A function the fold does not compute, over a numeric constant,
		// evaluated as a double: refused (PostgreSQL's sqrt(numeric)
		// answers); over integers it is PostgreSQL's sqrt(float8) too.
		{"sqrt", func(v string) string { return "sqrt(" + v + ") * sqrt(" + v + ")" }},
	}
	vals := []struct{ tag, v string }{{"H", "14"}, {"M", "14.0000000000000000001"}, {"F", "12.5"}}
	var out []smNumGrammarCell
	for _, b := range []struct{ key, col string }{{"dec", "v_dec"}, {"i64", "v_i64"}} {
		for _, sh := range shapes {
			for _, v := range vals {
				x := sh.x(v.v)
				disp := ""
				if sh.key == "sqrt" && v.tag != "H" {
					disp = "0A000"
				}
				body := "SELECT r." + b.col + " FROM st_pair r"
				for _, op := range []struct{ key, sql string }{
					{"in", "SELECT a.id FROM st_pair a WHERE " + x + " IN (" + body + ")"},
					{"notIn", "SELECT a.id FROM st_pair a WHERE " + x + " NOT IN (" + body + " WHERE r.id <= 3)"},
					{"corrIn", "SELECT a.id FROM st_pair a WHERE " + x + " IN (" + body + " WHERE r.id = a.id)"},
					{"corrNotIn", "SELECT a.id FROM st_pair a WHERE " + x + " NOT IN (" + body + " WHERE r.id = a.id)"},
					{"sel", "SELECT a.id, " + x + " IN (" + body + " WHERE r.id = a.id) AS m FROM st_pair a"},
				} {
					out = append(out, smNumGrammarCell{
						name: "numc/" + sh.key + "/" + op.key + "/" + b.key + "/" + v.tag,
						sql:  op.sql, disp: disp, text: x,
					})
				}
			}
		}
	}
	return out
}

// smNumWideIntCells are the grammar's integer-body rows past float8's exact
// integers (review round 4, B10): a body of bigint members 2^53 + {0, 2, 4}
// (and int members 2^24 + {0, 2, 4}, where a float4 would blur them), and an
// outer integer text one past a member. Every spelling is numeric = bigint,
// which PostgreSQL compares at the text's own digits. A quoted integer under
// a bare CAST AS NUMERIC | decimal or ::numeric boxed as a double, and
// MemberProbe's shortcut for an integer the set's own rung reads exactly let
// it through as one: 9007199254740993 matched the member 9007199254740992.
func smNumWideIntCells() []smNumGrammarCell {
	type body struct{ key, col, off1, hit string }
	bodies := []body{
		{"i64w", "r.v_i64 * 2 + 9007199254740968", "9007199254740993", "9007199254740992"},
		{"i32w", "r.v_i32 * 2 + 16777192", "16777217", "16777216"},
	}
	shapes := []struct{ key, pre, post string }{
		{"q", "'", "'"},
		{"cast", "CAST('", "' AS NUMERIC)"},
		{"castDecimal", "CAST('", "' AS decimal)"},
		{"colon", "'", "'::numeric"},
		{"const", "", ""},
		{"castConst", "CAST(", " AS NUMERIC)"},
	}
	var out []smNumGrammarCell
	for _, b := range bodies {
		for _, sh := range shapes {
			for _, v := range []struct{ tag, text string }{{"off1", b.off1}, {"hit", b.hit}} {
				x := sh.pre + v.text + sh.post
				body := "SELECT " + b.col + " FROM st_pair r"
				for _, op := range []struct{ key, sql string }{
					{"in", "SELECT a.id FROM st_pair a WHERE " + x + " IN (" + body + ")"},
					{"notIn", "SELECT a.id FROM st_pair a WHERE " + x + " NOT IN (" + body + " WHERE r.id <= 3)"},
					{"corrIn", "SELECT a.id FROM st_pair a WHERE " + x + " IN (" + body + " WHERE r.id = a.id)"},
					{"sel", "SELECT a.id, " + x + " = ANY (" + body + " WHERE r.id <= 3) AS m FROM st_pair a"},
				} {
					out = append(out, smNumGrammarCell{
						name: "numg/" + sh.key + "/" + op.key + "/" + b.key + "/" + v.tag,
						sql:  op.sql, text: v.text,
					})
				}
			}
		}
	}
	return out
}

// smNumGrammarPG reads PostgreSQL 17.11's answer for every grammar cell
// (testdata/arc_sm_numeric_grammar_pg17.tsv). SM_NUMG_GEN=<path> writes the
// cells' "name<TAB>sql" lines instead, for re-measuring.
func smNumGrammarPG(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_sm_numeric_grammar_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, want, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("malformed answer line %q", line)
		}
		out[name] = want
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// A NUMERIC LITERAL ON A MEMBERSHIP'S OUTER SIDE IS TYPED BY ONE RULE, IN
// EVERY SPELLING POSTGRESQL'S NUMERIC INPUT ACCEPTS (#1372). The table is
// the grammar, not examples: smNumSpellings × {quoted literal, CAST AS
// NUMERIC / decimal, ::numeric, the unquoted constant, CAST AS NUMERIC(18,4)
// and NUMERIC(38,20)} × {IN, NOT IN, = ANY, <> ALL, correlated IN, SELECT
// list} × {NUMERIC(18,4), NUMERIC(38,10), NUMERIC, bigint} bodies, on five
// arms, against PostgreSQL 17.11's full sorted rows.
//
// The literal takes NUMERIC(38, its own scale) — the value it spells, never
// float8: a bare NUMERIC boxed the literal as a double, so 14 + 1e-19 in
// every spelling the old hand scan did not read (an exponent, whitespace
// past the scan, more than 38 digits written) matched the member 14 where
// PostgreSQL answers none. A number no DECIMAL(38,s) holds is refused
// 22003 on every arm (a documented divergence: PostgreSQL's numeric is
// unconstrained and answers), never compared approximately.
func TestArcSMNumericLiteralGrammarEveryArm(t *testing.T) {
	cells := smNumGrammarCells()
	if p := os.Getenv("SM_NUMG_GEN"); p != "" {
		var b strings.Builder
		for _, c := range cells {
			b.WriteString(c.name + "\t" + c.sql + "\n")
		}
		if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Skip("wrote the cells for re-measuring")
	}
	if testing.Short() {
		t.Skip("-short: five arms over the numeric-literal grammar")
	}
	answers := smNumGrammarPG(t)
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := smArms(t, ctx)
	counts := map[string]int{}
	for _, tc := range cells {
		want := answers[tc.name]
		state := tc.disp
		divergent := state != ""
		if !divergent {
			if rest, ok := strings.CutPrefix(want, "ERR "); ok {
				state, _, _ = strings.Cut(rest, " ")
			}
		}
		switch {
		case divergent:
			counts["divergent "+state]++
		case state != "":
			counts["refused"]++
		default:
			counts["answered"]++
		}
		t.Run(tc.name, func(t *testing.T) {
			type result struct {
				res *oracle.Result
				err error
			}
			results := make([]result, len(arms))
			var wg sync.WaitGroup
			for i, arm := range arms {
				wg.Add(1)
				go func() {
					defer wg.Done()
					results[i].res, results[i].err = arm.run(tc.sql)
				}()
			}
			wg.Wait()
			for i, arm := range arms {
				res, err := results[i].res, results[i].err
				if state != "" {
					if err == nil {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11: %s)", tc.sql, arm.name, brRender(res), state, want)
						continue
					}
					if st := sqlerr.StateOf(err); st != state {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s", tc.sql, arm.name, st, err, state)
					} else if state == "22003" && divergent && !strings.Contains(err.Error(), tc.text) {
						t.Errorf("%s\n  arm  %s\n  got  %v\n  want the refusal to name the literal %q", tc.sql, arm.name, err, tc.text)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, err, want)
					continue
				}
				if got := brRender(res); strings.TrimSpace(got) != strings.TrimSpace(want) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, got, want)
				}
			}
		})
	}
	t.Logf("numeric-literal grammar: %d cells, %v", len(cells), counts)
	if counts["answered"] < 2500 || counts["divergent 22003"] < 400 || counts["refused"] < 400 {
		t.Fatalf("%v: the table must hold answered, refused and divergent cells", counts)
	}
}

// THE TYPED LITERAL IS IN THE PLAN, FOR EVERY SPELLING (#1372): EXPLAIN of
// the membership shows each representable spelling of the quoted, CAST,
// ::numeric and unquoted outer as `cast('<text>' as NUMERIC(38,<its own
// scale>))` on five arms — never a bare NUMERIC, which is a double here —
// and a number no DECIMAL(38,s) holds is 22003 before any plan exists.
func TestArcSMNumericLiteralExplainEverySpelling(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := stArms(t, ctx)
	type pin struct{ sql, must, state string }
	var pins []pin
	for _, s := range smNumSpellings() {
		if s.scale < 0 && s.disp != "22003" {
			continue
		}
		outers := []struct{ x, lit string }{
			{"'" + s.text + "'", s.text},
			{"CAST('" + s.text + "' AS NUMERIC)", s.text},
			{"'" + s.text + "'::numeric", s.text},
		}
		if s.constant != "" {
			// The parser folds a unary plus into the constant.
			outers = append(outers, struct{ x, lit string }{s.constant, strings.TrimPrefix(s.constant, "+")})
		}
		for _, o := range outers {
			p := pin{sql: "EXPLAIN SELECT a.id FROM st_pair a WHERE " + o.x + " IN (SELECT r.v_dec FROM st_pair r)"}
			if s.disp == "22003" {
				p.state = "22003"
			} else {
				p.must = "cast('" + o.lit + "' as NUMERIC(38," + strconv.Itoa(s.scale) + ")) in ("
			}
			pins = append(pins, p)
		}
	}
	for _, arm := range arms {
		for _, p := range pins {
			res, err := arm.run(p.sql)
			if p.state != "" {
				if err == nil {
					t.Errorf("%s\n  arm  %s\n  plan %s\n  want %s before any plan", p.sql, arm.name, brRenderOrdered(res), p.state)
				} else if st := sqlerr.StateOf(err); st != p.state {
					t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s", p.sql, arm.name, st, err, p.state)
				}
				continue
			}
			if err != nil {
				t.Errorf("%s\n  arm  %s\n  refused: %v", p.sql, arm.name, err)
				continue
			}
			plan := brRenderOrdered(res)
			if !strings.Contains(plan, p.must) || strings.Contains(plan, " as NUMERIC) in (") {
				t.Errorf("%s\n  arm  %s\n  plan %s\n  want %q and never a bare NUMERIC", p.sql, arm.name, plan, p.must)
			}
		}
	}
}
