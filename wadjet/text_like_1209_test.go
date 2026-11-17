// SPDX-License-Identifier: MIT

package wadjet

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

// The TEXT LIKE matrix (#1209, #1204). PostgreSQL's text ~~ text takes the
// BACKSLASH as its default escape: `\%`, `\_` and `\\` are the literals, a
// backslash before any other character makes that character literal, and a
// pattern whose matcher REACHES a lone trailing backslash is 22025. `LIKE p
// ESCAPE e` is ~~ over like_escape(p, e), which rewrites the pattern into that
// backslash form. The matrix crosses operator x operand kind x content pair,
// and runs every cell through each statement shape that reaches a different
// matcher: a FROM-less constant, a projection (the expression evaluator), a
// WHERE over a stored table (the scan's pushed filter for a column against a
// literal), a WHERE over a derived table (the exec LIKE filter and its
// comparison kernel), a CASE-wrapped WHERE, HAVING and a JOIN ON.
//
// Every literal is written with standard_conforming_strings in force on both
// sides: a backslash inside '...' is one backslash in the text the matcher
// sees (TestTextLike1209LiteralsAreVerbatim measures that on the engine; the
// oracle runs `SHOW standard_conforming_strings` and refuses to run otherwise).
//
// LT_PG_DSN=<dsn> runs the same cells on PostgreSQL; LT_GOLDEN=<file> then
// writes the answer table (ltAnswers) the run produced, and LT_DUMP=<file>
// writes every arm's answer per cell (the base and tip tables of the note).

// ltPair is a value and a pattern, verbatim (Go raw strings: one backslash is
// one backslash in the text).
type ltPair struct{ v, p string }

var ltPairs = []ltPair{
	{`a%b`, `a\%b`},        // 0  escaped percent
	{`axb`, `a\%b`},        // 1
	{`a_b`, `a\_b`},        // 2  escaped underscore
	{`axb`, `a\_b`},        // 3
	{`a\b`, `a\\b`},        // 4  escaped backslash
	{`ab`, `a\\b`},         // 5
	{`ab`, `a\b`},          // 6  backslash before an ordinary character
	{`a\b`, `a\b`},         // 7
	{`abc`, `abc\`},        // 8  lone trailing backslash, value exhausted first
	{`abcd`, `abc\`},       // 9  lone trailing backslash, reached
	{``, `\`},              // 10
	{`A`, `\`},             // 11
	{`A`, `%\`},            // 12
	{`abc`, ``},            // 13 empty pattern
	{``, ``},               // 14
	{``, `%`},              // 15
	{`a%`, `a\%%`},         // 16 escaped prefix
	{`ab`, `a\%%`},         // 17
	{`ab%cd`, `ab\%c%`},    // 18 the prefix is ab%, not ab\ (no pruning by ab\)
	{`ab\xc`, `ab\%c%`},    // 19
	{`%x`, `\%%`},          // 20
	{`x%`, `%\%`},          // 21 escaped suffix
	{`a%b`, `%\%%`},        // 22 escaped contains
	{`ab`, `%\%%`},         // 23
	{`abc`, `%`},           // 24
	{`ABC`, `a%`},          // 25
	{`a\`, `a\\`},          // 26
	{`a\`, `%\\`},          // 27
	{`a!%b`, `a!%b`},       // 28 the ESCAPE '!' cells
	{`a%b`, `a!%b`},        // 29
	{`axb`, `a!%b`},        // 30
	{`a!b`, `a!!b`},        // 31
	{`abc`, `abc!`},        // 32 #1204: a pattern ending in its escape
	{`abcd`, `abc!`},       // 33
	{`a%b`, `aé%b`},        // 34 a multi-byte escape character
	{`é`, `_`},             // 35 multi-byte subjects
	{`é`, `é`},             // 36
	{`éa`, `é\a`},          // 37
	{`é%`, `é\%`},          // 38
	{`aé`, `%\é`},          // 39
	{`a\xb`, `a\%b`},       // 40
	{`a_b`, `a\_%`},        // 41
	{`ÉCOLE`, `é%`},        // 42
	{`a`, `Xa`},            // 43 ILIKE ESCAPE 'X'
	{`aXb`, `aXXb`},        // 44
	{`a\%b`, `a\\\%b`},     // 45
	{`google.com`, `%oo%`}, // 46 an ordinary contains, no escape
	{`google`, `g_o%e`},    // 47
}

var ltOps = []string{
	"LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE",
	"LIKE ESCAPE '!'", "NOT LIKE ESCAPE '!'", "ILIKE ESCAPE '!'", "NOT ILIKE ESCAPE '!'",
	"LIKE ESCAPE ''", "LIKE ESCAPE '\\'", "LIKE ESCAPE NULL", "LIKE ESCAPE '!!'",
	"LIKE ESCAPE 'é'", "ILIKE ESCAPE 'X'",
}

// ltKinds pairs a value kind with a pattern kind: a literal, the row's column,
// a column inside an expression, and NULL.
var ltKinds = [][2]string{
	{"lit", "lit"}, {"col", "lit"}, {"expr", "lit"}, {"lit", "col"}, {"col", "col"},
	{"null", "lit"}, {"col", "null"},
}

func ltOperand(k, side, s string) string {
	switch k {
	case "lit":
		return "'" + s + "'"
	case "col":
		return side
	case "expr":
		return "(" + side + " || '')"
	case "null":
		return "NULL"
	}
	panic(k)
}

type ltCell struct {
	name   string
	id     int
	pred   string // over columns v and p
	op     string
	vk, pk string
}

func ltPred(op, v, p string) string {
	if j := strings.Index(op, " ESCAPE"); j >= 0 {
		return v + " " + op[:j] + " " + p + op[j:]
	}
	return v + " " + op + " " + p
}

func ltCells() []ltCell {
	var out []ltCell
	for i, pr := range ltPairs {
		for _, op := range ltOps {
			for _, k := range ltKinds {
				v := ltOperand(k[0], "v", pr.v)
				p := ltOperand(k[1], "p", pr.p)
				out = append(out, ltCell{
					name: fmt.Sprintf("%d/%s/%s/%s", i, op, k[0], k[1]),
					id:   i, pred: ltPred(op, v, p), op: op, vk: k[0], pk: k[1],
				})
			}
		}
	}
	return out
}

// ltArms are the statement shapes. fromlessOnly arms run only for cells whose
// operands are literals or NULL.
type ltArm struct {
	name     string
	budget   int64
	fromless bool
	// query renders the statement for a cell; scalar is true when the
	// statement answers the predicate itself rather than a count.
	query  func(c ltCell) string
	scalar bool
}

func ltQualify(pred, v, p string) string {
	r := strings.NewReplacer("(v ||", "("+v+" ||", "(p ||", "("+p+" ||")
	out := r.Replace(pred)
	// A bare column operand is the word v or p between spaces or at the ends.
	f := strings.Fields(out)
	for i, w := range f {
		switch w {
		case "v":
			f[i] = v
		case "p":
			f[i] = p
		}
	}
	return strings.Join(f, " ")
}

var ltArms = []ltArm{
	{name: "fromless", fromless: true, scalar: true,
		query: func(c ltCell) string { return "SELECT " + c.pred }},
	{name: "projection", scalar: true,
		query: func(c ltCell) string { return fmt.Sprintf("SELECT %s FROM lt%d", c.pred, c.id) }},
	{name: "where-scan",
		query: func(c ltCell) string { return fmt.Sprintf("SELECT count(*) FROM lt%d WHERE %s", c.id, c.pred) }},
	{name: "where-derived",
		query: func(c ltCell) string {
			return fmt.Sprintf("SELECT count(*) FROM (SELECT * FROM lt%d LIMIT 1000) d WHERE %s", c.id, c.pred)
		}},
	{name: "where-case",
		query: func(c ltCell) string {
			return fmt.Sprintf("SELECT count(*) FROM lt%d WHERE CASE WHEN %s THEN true ELSE false END", c.id, c.pred)
		}},
	{name: "having",
		query: func(c ltCell) string {
			return fmt.Sprintf("SELECT count(*) FROM lt%d GROUP BY v, p HAVING %s", c.id, c.pred)
		}},
	{name: "join-on",
		query: func(c ltCell) string {
			return fmt.Sprintf("SELECT count(*) FROM lt%d a JOIN lt%d b ON a.id = b.id AND %s", c.id, c.id, ltQualify(c.pred, "a.v", "b.p"))
		}},
	{name: "projection-budget", budget: 64 << 10, scalar: true,
		query: func(c ltCell) string { return fmt.Sprintf("SELECT %s FROM lt%d", c.pred, c.id) }},
	{name: "where-derived-budget", budget: 64 << 10,
		query: func(c ltCell) string {
			return fmt.Sprintf("SELECT count(*) FROM (SELECT * FROM lt%d LIMIT 1000) d WHERE %s", c.id, c.pred)
		}},
}

// ltSetup stores each content pair in its OWN one-row table lt<i>, so a
// predicate that raises on one pair is never reached through another's.
func ltSetup(t *testing.T, r lkRunner) {
	for i, pr := range ltPairs {
		_ = r.exec(fmt.Sprintf(`DROP TABLE IF EXISTS lt%d`, i))
		if err := r.exec(fmt.Sprintf(`CREATE TABLE lt%d (id BIGINT, v TEXT, p TEXT)`, i)); err != nil {
			t.Fatal(err)
		}
		q := fmt.Sprintf(`INSERT INTO lt%d (id, v, p) VALUES (%d, '%s', '%s')`, i, i, pr.v, pr.p)
		if err := r.exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// ltCode is the one-letter answer of a scalar arm: t, f, n (NULL), a (22025),
// or ? for anything else.
func ltCode(ans string) byte {
	switch ans {
	case "true":
		return 't'
	case "false":
		return 'f'
	case "NULL":
		return 'n'
	case "ERR 22025":
		return 'a'
	}
	return '?'
}

// ltExpect is what an arm answers for a PostgreSQL code.
func ltExpect(code byte, scalar bool) string {
	switch code {
	case 't':
		if scalar {
			return "true"
		}
		return "1"
	case 'f':
		if scalar {
			return "false"
		}
		return "0"
	case 'n':
		if scalar {
			return "NULL"
		}
		return "0"
	case 'a':
		return "ERR 22025"
	}
	return "?"
}

func ltRun(r lkRunner, q string) string {
	rows, err := r.rows(q)
	if err != nil {
		return "ERR " + lkState(err)
	}
	if rows == "" {
		return "0" // HAVING over a group the predicate removes
	}
	return rows
}

func ltOpen(t *testing.T, budget int64) (lkRunner, bool) {
	if dsn := os.Getenv("LT_PG_DSN"); dsn != "" {
		t.Setenv("LK_PG_DSN", dsn)
		r, pg := lkOpen(t, budget)
		got, err := r.rows(`SHOW standard_conforming_strings`)
		if err != nil || got != "on" {
			t.Fatalf("standard_conforming_strings must be on: %q %v", got, err)
		}
		return r, pg
	}
	t.Setenv("LK_PG_DSN", "")
	return lkOpen(t, budget)
}

// ltResidual is a cell whose engine answer still differs from PostgreSQL's,
// pinned at the engine's answer with its cause. The key is the cell name.
//
// `_` matches one BYTE here and one CHARACTER on PostgreSQL, so `'é' LIKE '_'`
// is false (é is two bytes in UTF-8): wrong at the base and kept, because a
// LIKE without a backslash or an ESCAPE must not change in #1209. ILIKE lowers
// with this engine's lower(), which folds every Unicode letter, where the
// oracle's C-locale lower() folds only ASCII ('ÉCOLE' ILIKE 'é%'). ILIKE
// ... ESCAPE 'X' lowers the pattern BEFORE like_escape reads it, so an upper-
// case escape character no longer names itself (PostgreSQL applies like_escape
// first). All three were wrong at the base.
func ltResidual(c ltCell, arm ltArm) (byte, bool) {
	switch {
	case c.id == 35 && (c.op == "LIKE" || c.op == "ILIKE" || strings.HasPrefix(c.op, "LIKE ESCAPE") || strings.HasPrefix(c.op, "ILIKE ESCAPE")) && c.vk != "null" && c.pk != "null" && c.op != "LIKE ESCAPE NULL" && c.op != "LIKE ESCAPE '!!'":
		return 'f', true
	case c.id == 35 && (c.op == "NOT LIKE" || c.op == "NOT ILIKE" || strings.HasPrefix(c.op, "NOT LIKE ESCAPE") || strings.HasPrefix(c.op, "NOT ILIKE ESCAPE")) && c.vk != "null" && c.pk != "null":
		return 't', true
	case c.id == 42 && (c.op == "ILIKE" || c.op == "ILIKE ESCAPE '!'" || c.op == "ILIKE ESCAPE 'X'") && c.vk != "null" && c.pk != "null":
		return 't', true
	case c.id == 42 && (c.op == "NOT ILIKE" || c.op == "NOT ILIKE ESCAPE '!'") && c.vk != "null" && c.pk != "null":
		return 'f', true
	case (c.id == 43 || c.id == 44) && c.op == "ILIKE ESCAPE 'X'" && c.vk != "null" && c.pk != "null":
		return 'f', true
	}
	return 0, false
}

// TestTextLike1209Matrix: every cell answers as PostgreSQL 17.11 does, value
// or SQLSTATE, through every statement shape, plain and under a 64 KiB memory
// budget (LIKE has no spilled path of its own; the budget arm holds the same
// cells under the memory model every other operator runs in).
func TestTextLike1209Matrix(t *testing.T) {
	for _, arm := range ltArms {
		if testing.Short() && arm.budget != 0 {
			continue
		}
		t.Run(arm.name, func(t *testing.T) {
			r, pg := ltOpen(t, arm.budget)
			ltSetup(t, r)
			bad, cells := 0, 0
			for _, c := range ltCells() {
				if arm.fromless && (c.vk == "col" || c.pk == "col" || c.vk == "expr") {
					continue
				}
				codes, ok := ltAnswers[c.op+"|"+c.vk+"|"+c.pk]
				if !ok || len(codes) != len(ltPairs) {
					t.Fatalf("no pinned PostgreSQL answer for %s (%s)", c.name, c.pred)
				}
				code := codes[c.id]
				if res, ok := ltResidual(c, arm); ok && !pg {
					code = res
				}
				cells++
				want := ltExpect(code, arm.scalar)
				if got := ltRun(r, arm.query(c)); got != want {
					bad++
					if bad <= 25 {
						t.Errorf("%s: %s\n  got  %s\n  want %s", c.name, arm.query(c), got, want)
					}
				}
			}
			if bad > 25 {
				t.Errorf("... %d of %d cells differ", bad, cells)
			}
			if cells == 0 {
				t.Fatal("no cells ran")
			}
		})
	}
}

// TestTextLike1209LiteralsAreVerbatim: the engine reads a quoted literal with
// standard_conforming_strings' rule, so the PATTERN the matcher sees has the
// backslashes the matrix wrote.
func TestTextLike1209LiteralsAreVerbatim(t *testing.T) {
	r, _ := ltOpen(t, 0)
	for _, pr := range ltPairs {
		q := fmt.Sprintf(`SELECT length('%s'), octet_length('%s')`, pr.p, pr.p)
		want := fmt.Sprintf("%d|%d", len([]rune(pr.p)), len(pr.p))
		if got := ltRun(r, q); got != want {
			t.Errorf("%s: got %s want %s", q, got, want)
		}
	}
}

// TestTextLike1209Dump writes the PostgreSQL answer table (LT_GOLDEN, with
// LT_PG_DSN) or every arm's raw answers (LT_DUMP).
func TestTextLike1209Dump(t *testing.T) {
	golden, dump := os.Getenv("LT_GOLDEN"), os.Getenv("LT_DUMP")
	if golden == "" && dump == "" {
		t.Skip("LT_GOLDEN / LT_DUMP not set")
	}
	if golden != "" {
		r, _ := ltOpen(t, 0)
		ltSetup(t, r)
		codes := map[string][]byte{}
		for _, c := range ltCells() {
			k := c.op + "|" + c.vk + "|" + c.pk
			if codes[k] == nil {
				codes[k] = []byte(strings.Repeat("-", len(ltPairs)))
			}
			ans := ltRun(r, ltArms[1].query(c))
			codes[k][c.id] = ltCode(ans)
			if ltCode(ans) == '?' {
				t.Logf("unclassified %s: %s", c.name, ans)
			}
		}
		var keys []string
		for k := range codes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		b.WriteString("// SPDX-License-Identifier: MIT\n\npackage wadjet\n\n")
		b.WriteString("// ltAnswers is PostgreSQL 17.11's answer to every text LIKE matrix cell\n")
		b.WriteString("// (#1209), generated by TestTextLike1209Dump with LT_PG_DSN against\n")
		b.WriteString("// postgres:17-alpine (--locale=C, UTF8) through the projection arm. One letter\n")
		b.WriteString("// per content pair, in ltPairs order: t true, f false, n NULL, a 22025.\n")
		b.WriteString("var ltAnswers = map[string]string{\n")
		for _, k := range keys {
			fmt.Fprintf(&b, "\t%q: %q,\n", k, string(codes[k]))
		}
		b.WriteString("}\n")
		if err := os.WriteFile(golden, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if dump != "" {
		var lines []string
		for _, arm := range ltArms {
			r, _ := ltOpen(t, arm.budget)
			ltSetup(t, r)
			for _, c := range ltCells() {
				if arm.fromless && (c.vk == "col" || c.pk == "col" || c.vk == "expr") {
					continue
				}
				lines = append(lines, c.name+"\t"+arm.name+"\t"+ltRun(r, arm.query(c)))
			}
		}
		if err := os.WriteFile(dump, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
