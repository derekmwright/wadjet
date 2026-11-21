// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1590: an unknown-typed (quoted) literal that meets a BYTES operand in a
// construct that UNIFIES types — COALESCE, NULLIF, CASE, GREATEST/LEAST, `||`,
// a set operation, VALUES, a LAG/LEAD default, an aggregate's delimiter — is
// coerced to bytea by PostgreSQL, i.e. read by byteain. The matrix below puts
// seven literals (hex, escape, `\\x41`, odd hex, bad hex, empty, NULL) against
// four operand families in every such construct and position:
//
//	bytes    the BYTES column b (the subject)
//	text     the TEXT column s           (control: must not move)
//	varchar  CAST(s AS VARCHAR)          (control: must not move)
//	lit      another quoted literal      (control: two unknowns resolve to text)
//
// plus fixed cells: a BYTES operand against a TEXT column or a text cast, and
// the bytea-declared functions over a bare literal. testdata/
// arc_1590_bytea_unknown_pg17.tsv holds PostgreSQL 17.11's answer for every
// cell (postgres:17-alpine, initdb --locale=C --encoding=UTF8; BYTES is
// CREATE DOMAIN bytes AS bytea there).

func u1590Tables() []tmdTable {
	i64 := parquet.Column{Name: "id", Type: parquet.TypeInt64}
	t := tmdTable{name: "u_t", schema: parquet.Schema{Columns: []parquet.Column{
		i64, {Name: "b", Type: parquet.TypeBytes, Nullable: true}, {Name: "s", Type: parquet.TypeString, Nullable: true}}}}
	bs := [][]byte{[]byte("hi"), []byte(`\x41`), []byte("A"), []byte(`a\\b`), nil, []byte("z")}
	ss := []any{`\x6869`, "hi", "A", `a\b`, nil, ""}
	for i := range bs {
		var b any
		if bs[i] != nil {
			b = bs[i]
		}
		t.rows = append(t.rows, map[string]any{"id": int64(i + 1), "b": b, "s": ss[i]})
	}
	k := tmdTable{name: "u_k", schema: parquet.Schema{Columns: []parquet.Column{
		i64, {Name: "kb", Type: parquet.TypeBytes, Nullable: true}, {Name: "ks", Type: parquet.TypeString, Nullable: true}}}}
	for i, v := range []string{"hi", "A", "z", `\x6869`} {
		k.rows = append(k.rows, map[string]any{"id": int64(i + 1), "kb": []byte(v), "ks": v})
	}
	return []tmdTable{t, k}
}

// u1590PGFixture is u1590Tables as PostgreSQL DDL.
const u1590PGFixture = `CREATE DOMAIN bytes AS bytea;
CREATE TABLE u_t (id bigint, b bytea, s text);
INSERT INTO u_t VALUES (1, '\x6869', '\x6869'), (2, '\x5c783431', 'hi'), (3, '\x41', 'A'), (4, '\x615c5c62', 'a\b'), (5, NULL, NULL), (6, '\x7a', '');
CREATE TABLE u_k (id bigint, kb bytea, ks text);
INSERT INTO u_k VALUES (1, '\x6869', 'hi'), (2, '\x41', 'A'), (3, '\x7a', 'z'), (4, '\x5c7836383639', '\x6869');`

// u1590Literals are the unknown-typed literals, by name.
var u1590Literals = []struct{ name, sql string }{
	{"hex", `'\x6869'`},
	{"esc", `'hi'`},
	{"dbl", `'\\x41'`},
	{"odd", `'\x686'`},
	{"badhex", `'\x68zz'`},
	{"empty", `''`},
	{"null", `NULL`},
}

// u1590Family substitutes a template's placeholders: {C} the operand,
// {CK} u_k's operand of the same type, {K} a typed constant, {N} a typed NULL,
// and <<x>> the rendering: encode(x,'hex') for BYTES, x for the text family,
// each followed by '/' and length(x). The length is what tells a BYTES value
// from its spelling: encode() itself reads a text argument with byteain, so
// encode(COALESCE(b, '\x6869'), 'hex') looked right while the COALESCE
// carried the six-character text.
type u1590Family struct{ name, c, ck, k, n string }

var u1590Families = []u1590Family{
	{"bytes", "b", "kb", `CAST('hi' AS BYTES)`, `CAST(NULL AS BYTES)`},
	{"text", "s", "ks", `CAST('hi' AS TEXT)`, `CAST(NULL AS TEXT)`},
	{"varchar", "CAST(s AS VARCHAR)", "CAST(ks AS VARCHAR)", `CAST('hi' AS VARCHAR)`, `CAST(NULL AS VARCHAR)`},
	{"lit", `'\x41'`, `'\x41'`, `'hi'`, `NULL`},
}

// u1590Templates are the constructs × positions; {L} is the literal.
var u1590Render = regexp.MustCompile(`<<.*?>>`)

var u1590Templates = []struct{ name, q string }{
	// projection
	{"p/coalesce", `SELECT id, <<COALESCE({C}, {L})>> AS v FROM u_t`},
	{"p/coalesce-litfirst", `SELECT id, <<COALESCE({L}, {C})>> AS v FROM u_t`},
	{"p/nullif-l", `SELECT id, <<NULLIF({C}, {L})>> AS v FROM u_t`},
	{"p/nullif-r", `SELECT id, <<NULLIF({L}, {C})>> AS v FROM u_t`},
	{"p/coalesce-nullif", `SELECT id, <<COALESCE(NULLIF({C}, {C}), {L})>> AS v FROM u_t`},
	{"p/case-then", `SELECT id, <<CASE WHEN id = 1 THEN {L} ELSE {C} END>> AS v FROM u_t`},
	{"p/case-else", `SELECT id, <<CASE WHEN id <> 1 THEN {C} ELSE {L} END>> AS v FROM u_t`},
	{"p/case-simple-then", `SELECT id, <<CASE id WHEN 1 THEN {L} ELSE {C} END>> AS v FROM u_t`},
	{"p/case-simple-when", `SELECT id, CASE {C} WHEN {L} THEN 1 ELSE 0 END AS v FROM u_t`},
	{"p/greatest", `SELECT id, <<GREATEST({C}, {L})>> AS v FROM u_t`},
	{"p/least", `SELECT id, <<LEAST({L}, {C})>> AS v FROM u_t`},
	{"p/concat-r", `SELECT id, <<{C} || {L}>> AS v FROM u_t`},
	{"p/concat-l", `SELECT id, <<{L} || {C}>> AS v FROM u_t`},
	{"p/length-coalesce", `SELECT id, length(COALESCE({C}, {L})) AS v FROM u_t`},
	{"p/lag-default", `SELECT id, <<lag({C}, 1, {L}) OVER (ORDER BY id)>> AS v FROM u_t`},
	{"p/lead-default", `SELECT id, <<lead({C}, 1, {L}) OVER (ORDER BY id)>> AS v FROM u_t`},
	{"p/first-value-coalesce", `SELECT id, <<first_value(COALESCE({C}, {L})) OVER (ORDER BY id DESC)>> AS v FROM u_t`},
	// WHERE (comparison and the pushed-down filter)
	{"w/eq", `SELECT id FROM u_t WHERE {C} = {L}`},
	{"w/ne", `SELECT id FROM u_t WHERE {C} <> {L}`},
	{"w/lt", `SELECT id FROM u_t WHERE {C} < {L}`},
	{"w/ge-rev", `SELECT id FROM u_t WHERE {L} >= {C}`},
	{"w/between", `SELECT id FROM u_t WHERE {C} BETWEEN {L} AND {K}`},
	{"w/in", `SELECT id FROM u_t WHERE {C} IN ({L}, {K})`},
	{"w/not-in", `SELECT id FROM u_t WHERE {C} NOT IN ({L}, {K})`},
	{"w/distinct", `SELECT id FROM u_t WHERE {C} IS DISTINCT FROM {L}`},
	{"w/like", `SELECT id FROM u_t WHERE {C} LIKE {L}`},
	{"w/not-like", `SELECT id FROM u_t WHERE {C} NOT LIKE {L}`},
	{"w/coalesce-eq", `SELECT id FROM u_t WHERE COALESCE({C}, {L}) = {K}`},
	{"w/case-eq", `SELECT id FROM u_t WHERE CASE WHEN id = 5 THEN {L} ELSE {C} END = {K}`},
	{"w/concat-eq", `SELECT id FROM u_t WHERE length({C} || {L}) = 3`},
	{"w/greatest-eq", `SELECT id FROM u_t WHERE GREATEST({C}, {L}) = {C}`},
	{"w/expr-eq-lit", `SELECT id FROM u_t WHERE COALESCE({C}, {K}) = {L}`},
	{"w/expr-in-lit", `SELECT id FROM u_t WHERE COALESCE({C}, {K}) IN ({L})`},
	{"w/expr-between-lit", `SELECT id FROM u_t WHERE COALESCE({C}, {K}) BETWEEN {L} AND {K}`},
	// the literal never read by a row: PostgreSQL coerces it while it
	// analyses the statement, so an invalid one is refused all the same
	{"z/coalesce-no-rows", `SELECT id, <<COALESCE({C}, {L})>> AS v FROM u_t WHERE id > 100`},
	{"z/coalesce-not-null", `SELECT id, <<COALESCE({C}, {L})>> AS v FROM u_t WHERE id = 1`},
	{"z/case-no-rows", `SELECT id, <<CASE WHEN id = 1 THEN {L} ELSE {C} END>> AS v FROM u_t WHERE id > 100`},
	{"z/concat-no-rows", `SELECT id, <<{C} || {L}>> AS v FROM u_t WHERE id > 100`},
	{"z/greatest-no-rows", `SELECT id, <<GREATEST({C}, {L})>> AS v FROM u_t WHERE id > 100`},
	{"z/lag-no-rows", `SELECT id, <<lag({C}, 1, {L}) OVER (ORDER BY id)>> AS v FROM u_t WHERE id > 100`},
	{"z/union-no-rows", `SELECT <<x>> AS v FROM (SELECT {C} AS x FROM u_t UNION ALL SELECT {L} FROM u_t WHERE id > 100) q`},
	// set operations and VALUES
	{"s/union-all", `SELECT <<x>> AS v FROM (SELECT {C} AS x FROM u_t UNION ALL SELECT {L}) q`},
	{"s/union-all-litfirst", `SELECT <<x>> AS v FROM (SELECT {L} AS x UNION ALL SELECT {C} FROM u_t) q`},
	{"s/union", `SELECT <<x>> AS v FROM (SELECT {C} AS x FROM u_t UNION SELECT {L}) q`},
	{"s/intersect", `SELECT <<x>> AS v FROM (SELECT {C} AS x FROM u_t INTERSECT SELECT {L}) q`},
	{"s/except", `SELECT <<x>> AS v FROM (SELECT {L} AS x EXCEPT SELECT {C} FROM u_t) q`},
	{"s/values", `SELECT <<x>> AS v FROM (VALUES ({K}), ({L})) AS vv(x)`},
	// aggregate, GROUP BY, JOIN ON
	{"a/count-distinct-coalesce", `SELECT count(DISTINCT COALESCE({C}, {L})) AS v FROM u_t`},
	{"a/string-agg-delim", `SELECT <<string_agg(x, {L})>> AS v FROM (VALUES ({K}), ({K})) AS vv(x)`},
	{"g/group-coalesce", `SELECT <<COALESCE({C}, {L})>> AS v, count(*) AS n FROM u_t GROUP BY COALESCE({C}, {L})`},
	{"g/group-count", `SELECT count(*) AS n FROM u_t GROUP BY COALESCE({C}, {L})`},
	{"g/having-count", `SELECT count(*) AS n FROM u_t GROUP BY COALESCE({C}, {L}) HAVING count(*) > 1`},
	{"g/having", `SELECT count(*) AS n FROM u_t GROUP BY COALESCE({C}, {L}) HAVING COALESCE({C}, {L}) = {K}`},
	{"g/distinct", `SELECT DISTINCT <<COALESCE({C}, {L})>> AS v FROM u_t`},
	{"o/order-by", `SELECT id FROM u_t ORDER BY COALESCE({C}, {L}) DESC, id LIMIT 2`},
	{"o/order-by-item", `SELECT id, <<COALESCE({C}, {L})>> AS v FROM u_t ORDER BY COALESCE({C}, {L}) DESC, id LIMIT 3`},
	{"d/derived-unaliased", `SELECT * FROM (SELECT id, COALESCE({C}, {L}) FROM u_t) q`},
	{"j/on-coalesce", `SELECT u_t.id, u_k.id FROM u_t JOIN u_k ON COALESCE({C}, {L}) = {CK}`},
	{"j/on-literal", `SELECT u_t.id, u_k.id FROM u_t JOIN u_k ON {C} = {CK} AND {CK} <> {L}`},
	// constant-folded (no table)
	{"c/coalesce", `SELECT <<COALESCE({N}, {L})>> AS v`},
	{"c/case", `SELECT <<CASE WHEN 1 = 1 THEN {L} ELSE {K} END>> AS v`},
	{"c/concat", `SELECT <<{K} || {L}>> AS v`},
	{"c/greatest", `SELECT <<GREATEST({K}, {L})>> AS v`},
	{"c/union-all", `SELECT <<x>> AS v FROM (SELECT {K} AS x UNION ALL SELECT {L}) q`},
	{"c/eq", `SELECT CASE WHEN {K} = {L} THEN 1 ELSE 0 END AS v`},
}

// u1590Fixed are the cells outside the template grid: the bytea-declared
// functions over a bare literal (an unknown resolves to text where text is a
// candidate), and a BYTES operand against a TEXT column or a text cast.
var u1590Fixed = []struct{ name, sql string }{
	{"f/length-lit", `SELECT length('\x6869') AS v`},
	{"f/octet-length-lit", `SELECT octet_length('\x6869') AS v`},
	{"f/encode-lit", `SELECT encode('\x6869', 'hex') AS v`},
	{"f/md5-lit", `SELECT md5('\x6869') AS v`},
	{"f/sha256-lit", `SELECT encode(sha256('\x6869'), 'hex') AS v`},
	{"f/position-lit", `SELECT id, position('\x41' in b) AS v FROM u_t`},
	{"f/like-escape", `SELECT id FROM u_t WHERE b LIKE 'a#\\%' ESCAPE '#'`},
	{"f/like-issue", `SELECT id FROM u_t WHERE b LIKE 'a\\%'`},
	{"x/coalesce-b-s", `SELECT id, COALESCE(b, s) AS v FROM u_t`},
	{"x/coalesce-b-casttext", `SELECT id, COALESCE(b, CAST('\x6869' AS TEXT)) AS v FROM u_t`},
	{"x/concat-b-s", `SELECT id, b || s AS v FROM u_t`},
	{"x/eq-b-s", `SELECT id FROM u_t WHERE b = s`},
	{"x/union-b-s", `SELECT b FROM u_t UNION ALL SELECT s FROM u_t`},
	{"x/case-b-casttext", `SELECT id, CASE WHEN id = 1 THEN CAST('x' AS TEXT) ELSE b END AS v FROM u_t`},
	{"x/greatest-b-s", `SELECT id, GREATEST(b, s) AS v FROM u_t`},
}

type u1590Cell struct {
	name, sql string
	fiveArms  bool // the representative subset run through the coordinator arms
}

func u1590Cells() []u1590Cell {
	var out []u1590Cell
	for _, f := range u1590Families {
		for _, tp := range u1590Templates {
			for _, l := range u1590Literals {
				q := u1590Render.ReplaceAllStringFunc(tp.q, func(m string) string {
					x := m[2 : len(m)-2]
					if f.name == "bytes" {
						return "encode(" + x + ", 'hex') || '/' || CAST(length(" + x + ") AS TEXT)"
					}
					return "(" + x + ") || '/' || CAST(length(" + x + ") AS TEXT)"
				})
				q = strings.NewReplacer("{CK}", f.ck, "{C}", f.c, "{K}", f.k, "{N}", f.n, "{L}", l.sql).Replace(q)
				out = append(out, u1590Cell{
					name:     f.name + "/" + tp.name + "/" + l.name,
					sql:      q,
					fiveArms: (f.name == "bytes" || f.name == "text") && (l.name == "hex" || l.name == "badhex"),
				})
			}
		}
	}
	for _, c := range u1590Fixed {
		out = append(out, u1590Cell{name: c.name, sql: c.sql, fiveArms: true})
	}
	return out
}

func u1590PGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_1590_bytea_unknown_pg17.tsv")
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
	return out
}

// u1590Residual are the cells still wrong after #1590's fix, each with its
// cause; the test holds them WRONG so a later fix has to delete the line.
var u1590Residual = func() map[string]string {
	m := map[string]string{
		// sha256 answers TEXT (hex) here, bytea there: encode() of it is 42883.
		"f/sha256-lit": "sha256 declares text, PostgreSQL bytea (loud: 42883 on encode)",
		// A BYTES operand beside a TEXT-typed one: PostgreSQL has no common
		// type (42804) for COALESCE / CASE / GREATEST and resolves `bytea ||
		// text` through text || anynonarray (the bytea's \x rendering, as
		// text); this engine answers with the bytes (silent).
		"x/case-b-casttext":     "no 42804 for CASE bytea/text",
		"x/coalesce-b-casttext": "no 42804 for COALESCE bytea/text",
		"x/coalesce-b-s":        "no 42804 for COALESCE bytea/text",
		"x/greatest-b-s":        "no 42804 for GREATEST bytea/text",
		"x/concat-b-s":          "bytea || text concatenates bytes, not text || anynonarray",
	}
	// string_agg over BYTES is refused outright here ("string_agg over bytea
	// is not supported"), whatever the delimiter: loud.
	for _, l := range u1590Literals {
		m["bytes/a/string-agg-delim/"+l.name] = "string_agg(bytea, bytea) unsupported (loud)"
	}
	// string_agg's TEXT separator: '' is read as the default ',' and NULL is
	// refused (0A000), where PostgreSQL concatenates with no separator. Not a
	// bytea cell: the TEXT control family shows it.
	for _, f := range []string{"text", "varchar", "lit"} {
		m[f+"/a/string-agg-delim/empty"] = "string_agg('' separator) uses ',' (silent)"
		m[f+"/a/string-agg-delim/null"] = "string_agg(NULL separator) refused 0A000 (loud)"
	}
	return m
}()

// u1590Judge renders one arm's answer and compares it with PostgreSQL's:
// the rows, or the refusal's SQLSTATE and message.
func u1590Judge(want string, res *oracle.Result, err error) (got string, ok bool) {
	if err != nil {
		got = "ERR " + sqlerr.StateOf(err) + " " + err.Error()
	} else {
		got = byRender(res)
	}
	if rest, isErr := strings.CutPrefix(want, "ERR "); isErr {
		state, msg, _ := strings.Cut(rest, " ")
		return got, err != nil && sqlerr.StateOf(err) == state && strings.Contains(err.Error(), msg)
	}
	return got, err == nil && got == want
}

// TestArc1590ByteaUnknownLiteralMatrix holds every cell to PostgreSQL 17.11's
// answer: every cell on the single and spilled arms, the representative subset
// (the BYTES and TEXT families with the hex and bad-hex literals, and the
// fixed cells) on the three coordinator arms as well.
//
// U1590_DUMP=<path> writes every (cell, arm) answer as right/wrong/refused
// instead of failing: the base-against-tip comparison the brief asks for.
// U1590_LIST=<path> writes the cells (name<TAB>sql) for measuring PostgreSQL.
func TestArc1590ByteaUnknownLiteralMatrix(t *testing.T) {
	cells := u1590Cells()
	if p := os.Getenv("U1590_LIST"); p != "" {
		var b strings.Builder
		for _, c := range cells {
			fmt.Fprintf(&b, "%s\t%s\n", c.name, c.sql)
		}
		if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if testing.Short() {
		t.Skip("-short: five arms over the #1590 matrix")
	}
	answers := u1590PGAnswers(t)
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	for name := range u1590Residual {
		if _, ok := answers[name]; !ok {
			t.Fatalf("residual %s names no cell", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := byArmsOver(t, ctx, u1590Tables())
	dump := os.Getenv("U1590_DUMP")
	var dumpLines []string
	var mu sync.Mutex
	compared := 0
	for _, c := range cells {
		want := answers[c.name]
		use := arms[:2]
		if c.fiveArms {
			use = arms
		}
		gots := make([]string, len(use))
		oks := make([]bool, len(use))
		var wg sync.WaitGroup
		for i, arm := range use {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := arm.run(c.sql)
				gots[i], oks[i] = u1590Judge(want, res, err)
			}()
		}
		wg.Wait()
		for i, arm := range use {
			verdict := "right"
			if !oks[i] {
				verdict = "wrong"
				if strings.HasPrefix(gots[i], "ERR ") {
					verdict = "refused"
				}
			}
			if dump != "" {
				mu.Lock()
				dumpLines = append(dumpLines, fmt.Sprintf("%s\t%s\t%s\t%s", c.name, arm.name, verdict, gots[i]))
				mu.Unlock()
				continue
			}
			_, residual := u1590Residual[c.name]
			switch {
			case oks[i] && residual:
				t.Errorf("%s [%s] now AGREES with PostgreSQL: delete its u1590Residual line", c.name, arm.name)
			case !oks[i] && !residual:
				t.Errorf("%s\n  sql  %s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, arm.name, gots[i], want)
			}
			compared++
		}
	}
	if dump != "" {
		sort.Strings(dumpLines)
		if err := os.WriteFile(dump, []byte(strings.Join(dumpLines, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if want := 2 * len(cells); compared < want {
		t.Errorf("only %d (cell, arm) comparisons, want at least %d", compared, want)
	}
}
