// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// smType is one typed column of the `st_pair` fixture (stTypes) as the
// membership-operand table spells it: the CAST target in this engine's
// spelling and in PostgreSQL's (the fixture declares PORT as integer,
// DURATION as bigint, IPV6 as inet), and rows 1 and 2's text.
type smType struct {
	key          string
	cast, pgCast string
	t1, t2       string
}

func smTypes() []smType {
	texts := map[string][2]string{}
	for _, ty := range stTypes() {
		texts[ty.key] = ty.texts
	}
	mk := func(key, cast, pgCast string) smType {
		return smType{key: key, cast: cast, pgCast: pgCast, t1: texts[key][0], t2: texts[key][1]}
	}
	return []smType{
		mk("i64", "BIGINT", "BIGINT"),
		mk("i32", "INTEGER", "INTEGER"),
		mk("dec", "NUMERIC(18,4)", "NUMERIC(18,4)"),
		mk("f64", "DOUBLE PRECISION", "DOUBLE PRECISION"),
		mk("date", "DATE", "DATE"),
		mk("ts", "TIMESTAMP", "TIMESTAMP"),
		mk("bool", "BOOLEAN", "BOOLEAN"),
		mk("uuid", "UUID", "UUID"),
		mk("ipv6", "IPV6", "INET"),
		mk("port", "PORT", "INTEGER"),
		mk("dur", "DURATION", "BIGINT"),
	}
}

// smCell is one generated cell: its SQL in this engine's spelling, and the
// spelling whose PostgreSQL 17.11 answer is the cell's expected answer. The
// two differ in a CAST target's name, and — for a KEPT `CAST(x AS TEXT)`
// body, the recorded superset where PostgreSQL has no operator at all — in
// the text cast itself, which the PostgreSQL spelling drops: the kept
// reading IS the typed comparison of the value with the value it rendered.
type smCell struct {
	name, sql, pgSQL string
}

// smOp is one membership spelling over an outer operand and a body.
type smOp struct {
	name string
	pred func(outer, sub string) string
}

func smMemberOps() []smOp {
	return []smOp{
		{"in", func(o, s string) string { return o + " IN (" + s + ")" }},
		{"notIn", func(o, s string) string { return o + " NOT IN (" + s + ")" }},
		{"eqAny", func(o, s string) string { return o + " = ANY (" + s + ")" }},
		{"neAll", func(o, s string) string { return o + " <> ALL (" + s + ")" }},
	}
}

// smBody is one subquery body: `with` is a leading CTE on the statement.
type smBody struct {
	name, with, sub string
	// allOps: the body takes = ANY and <> ALL as well as IN and NOT IN.
	allOps bool
}

// smTypedBodies are the body shapes that select a value of the outer
// column's own type: every body holds row 1's value (twice where a set
// operation or a literal repeats it) and, but for castLit, row 2's; the
// Null bodies hold a NULL beside it.
func smTypedBodies(ty smType) []smBody {
	v := "r.v_" + ty.key
	lit1 := "CAST('" + ty.t1 + "' AS " + ty.cast + ")"
	lit2 := "CAST('" + ty.t2 + "' AS " + ty.cast + ")"
	sel := func(item, filter string) string { return "SELECT " + item + " FROM st_pair r WHERE " + filter }
	out := []smBody{
		{name: "col", sub: sel(v, "r.id <= 2"), allOps: true},
		{name: "colNull", sub: sel(v, "r.id <> 3")},
		{name: "expr", sub: sel("coalesce("+v+", "+v+")", "r.id <= 2")},
		{name: "castLit", sub: sel(lit1, "r.id <= 2"), allOps: true},
		{name: "fromless", sub: "SELECT " + lit1 + " UNION ALL SELECT " + lit2},
		{name: "setop", sub: sel(v, "r.id = 1") + " UNION ALL " + sel(v, "r.id = 1") + " UNION ALL " + sel(v, "r.id = 2"), allOps: true},
		{name: "setopUnion", sub: sel(v, "r.id = 1") + " UNION " + sel(v, "r.id = 2")},
		{name: "setopLit", sub: sel(lit1, "r.id = 1") + " UNION ALL " + sel(lit2, "r.id = 2"), allOps: true},
		{name: "setopNull", sub: sel(v, "r.id = 1") + " UNION ALL " + sel(v, "r.id = 4")},
		{name: "derived", sub: "SELECT d.k FROM (" + sel(v+" AS k", "r.id <= 2") + ") d"},
		{name: "cte", with: "WITH c AS (" + sel(v+" AS k", "r.id <= 2") + ") ", sub: "SELECT c.k FROM c"},
		{name: "corr", sub: sel(v, "r.id = a.id AND r.id <= 2")},
		{name: "corrExpr", sub: sel("coalesce("+v+", "+v+")", "r.id = a.id AND r.id <= 2")},
	}
	// PostgreSQL has no max(boolean) and no max(uuid); the aggregate body
	// is measured where the aggregate exists on both sides.
	if ty.key != "bool" && ty.key != "uuid" {
		out = append(out, smBody{name: "agg", sub: sel("max("+v+")", "r.id <= 2 GROUP BY r.id")})
	}
	return out
}

// smOuters are the outer operand shapes of the value table. DURATION has
// no castLit outer: `CAST('…' AS DURATION)` is not a typed cast in this
// engine (it keeps the TEXT — a recorded filing candidate), so that outer
// is a text/duration pair, not the typed literal this row means.
func smOuters(ty smType) [][2]string {
	out := [][2]string{
		{"col", "a.v_" + ty.key},
		{"expr", "coalesce(a.v_" + ty.key + ", a.v_" + ty.key + ")"},
		{"qlit", "'" + ty.t1 + "'"},
	}
	if ty.key != "dur" {
		out = append(out, [2]string{"castLit", "CAST('" + ty.t1 + "' AS " + ty.cast + ")"})
	}
	return out
}

// smPG is a statement's PostgreSQL spelling: the fixture's own type names
// for PORT, DURATION and IPV6.
func smPG(ty smType, sql string) string {
	if ty.cast != ty.pgCast {
		sql = strings.ReplaceAll(sql, " AS "+ty.cast+")", " AS "+ty.pgCast+")")
	}
	return sql
}

// smValueCells is the typed × typed membership table: an outer operand of
// every shape against a body of every shape selecting the same type — no
// TEXT anywhere, so every cell is a value PostgreSQL answers (#1372's
// quoted-literal outer, #1373's set-operation and literal bodies).
func smValueCells() []smCell {
	var out []smCell
	for _, ty := range smTypes() {
		for _, o := range smOuters(ty) {
			for _, b := range smTypedBodies(ty) {
				for _, op := range smMemberOps() {
					if !b.allOps && (op.name == "eqAny" || op.name == "neAll") {
						continue
					}
					sql := b.with + "SELECT a.id FROM st_pair a WHERE " + op.pred(o[1], b.sub)
					out = append(out, smCell{name: "val/" + op.name + "/" + ty.key + "/" + o[0] + "/" + b.name,
						sql: sql, pgSQL: smPG(ty, sql)})
				}
			}
		}
		// An unknown literal that is not a value of the body's type, and
		// NULL, against the plain column body.
		for _, o := range [][2]string{{"qlitBad", "'zz'"}, {"null", "NULL"}} {
			for _, op := range smMemberOps()[:2] {
				sql := "SELECT a.id FROM st_pair a WHERE " + op.pred(o[1], "SELECT r.v_"+ty.key+" FROM st_pair r WHERE r.id <= 2")
				out = append(out, smCell{name: "val/" + op.name + "/" + ty.key + "/" + o[0] + "/col",
					sql: sql, pgSQL: smPG(ty, sql)})
			}
		}
	}
	return out
}

// smKeyCells is the correlated-key table: EXISTS, NOT EXISTS and a LATERAL
// body whose top-level equality pairs an outer operand with a body operand
// of the same type.
func smKeyCells() []smCell {
	var out []smCell
	for _, ty := range smTypes() {
		outers := [][2]string{{"col", "a.v_" + ty.key}, {"expr", "coalesce(a.v_" + ty.key + ", a.v_" + ty.key + ")"}}
		bodies := []struct{ name, from, key string }{
			{"col", "st_pair r", "r.v_" + ty.key},
			{"expr", "st_pair r", "coalesce(r.v_" + ty.key + ", r.v_" + ty.key + ")"},
			{"derived", "(SELECT id, v_" + ty.key + " AS k FROM st_pair) r", "r.k"},
		}
		for _, o := range outers {
			for _, b := range bodies {
				for _, op := range smKeyOps() {
					sql := op.pred(o[1], b.key, b.from, "r.id <= 2")
					out = append(out, smCell{name: "key/" + op.name + "/" + ty.key + "/" + o[0] + "/" + b.name,
						sql: sql, pgSQL: smPG(ty, sql)})
				}
			}
		}
	}
	return out
}

// smKeyOp is a correlated spelling: the statement over outer `a`, a body
// relation aliased r (filtered by filter) and the key equality.
type smKeyOp struct {
	name string
	pred func(outer, key, from, filter string) string
}

func smKeyOps() []smKeyOp {
	exists := func(not string) func(o, k, f, fl string) string {
		return func(o, k, f, fl string) string {
			return "SELECT a.id FROM st_pair a WHERE " + not + "EXISTS (SELECT 1 FROM " + f + " WHERE " + fl + " AND " + o + " = " + k + ")"
		}
	}
	return []smKeyOp{
		{"exists", exists("")},
		{"notExists", exists("NOT ")},
		{"lateral", func(o, k, f, fl string) string {
			return "SELECT a.id, l.rid FROM st_pair a, LATERAL (SELECT r.id AS rid FROM " + f + " WHERE " + fl + " AND " + o + " = " + k + ") l"
		}},
	}
}

// smTextKept is whether ADR-0012 §5 keeps a `CAST(v AS TEXT)` body of the
// compared value's own type: in a membership (IN family) for the ten kept
// types, and as a correlated key (EXISTS / LATERAL, the direct comparison's
// reading) for every type.
func smTextKept(key string, correlated bool) bool {
	return correlated || (key != "date" && key != "ts" && key != "bool")
}

// smTextCells is the typed × TEXT table: the outer operand's shape (#1369)
// and the body's (#1370) against a TEXT column, a TEXT expression and the
// kept `CAST(v AS TEXT)`, as a membership and as a correlated key (#1368's
// LATERAL). The bodies read ids 1..3 — row 3's text is 'zz', which no value
// renders as.
func smTextCells() []smCell {
	var out []smCell
	for _, ty := range smTypes() {
		v, s := "r.v_"+ty.key, "r.s_"+ty.key
		bodies := []struct{ name, key string }{
			{"col", s}, {"concat", s + " || ''"}, {"cast", "CAST(" + v + " AS TEXT)"},
		}
		outers := [][2]string{
			{"col", "a.v_" + ty.key},
			{"expr", "coalesce(a.v_" + ty.key + ", a.v_" + ty.key + ")"},
		}
		if ty.key != "dur" {
			outers = append(outers, [2]string{"castLit", "CAST('" + ty.t1 + "' AS " + ty.cast + ")"})
		}
		for _, o := range outers {
			for _, b := range bodies {
				for _, op := range smMemberOps()[:2] {
					sql := "SELECT a.id FROM st_pair a WHERE " + op.pred(o[1], "SELECT "+b.key+" FROM st_pair r WHERE r.id <= 3")
					out = append(out, smTextCell("txt/"+op.name+"/"+ty.key+"/"+o[0]+"/"+b.name, ty, sql, b.name == "cast" && smTextKept(ty.key, false), v))
				}
				if o[0] == "castLit" {
					continue
				}
				for _, op := range smKeyOps() {
					sql := op.pred(o[1], b.key, "st_pair r", "r.id <= 3")
					kept := b.name == "cast" && op.name != "lateral" && smTextKept(ty.key, true)
					out = append(out, smTextCell("txt/"+op.name+"/"+ty.key+"/"+o[0]+"/"+b.name, ty, sql, kept, v))
				}
			}
		}
	}
	return out
}

func smTextCell(name string, ty smType, sql string, kept bool, v string) smCell {
	pg := smPG(ty, sql)
	if kept {
		pg = strings.ReplaceAll(pg, "CAST("+v+" AS TEXT)", v)
	}
	return smCell{name: name, sql: sql, pgSQL: pg}
}

// smCrossCastCells are #1374's pairs: a value against `CAST(x AS TEXT)` of a
// DIFFERENT type, as a membership and as an EXISTS key — one comparison,
// which must answer once. Kept only where the two types render a shared
// value identically (two integer kinds).
func smCrossCastCells() []smCell {
	pairs := []struct {
		tl, origin string
		kept       bool
	}{
		{"dec", "i64", false}, {"dec", "i32", false}, {"dec", "f64", false},
		{"f64", "dec", false}, {"f64", "i64", false},
		{"i64", "i32", true}, {"i32", "i64", true},
	}
	var out []smCell
	for _, p := range pairs {
		cast := "CAST(r.v_" + p.origin + " AS TEXT)"
		outer := "a.v_" + p.tl
		sqls := map[string]string{
			"in":        "SELECT a.id FROM st_pair a WHERE " + outer + " IN (SELECT " + cast + " FROM st_pair r WHERE r.id <= 3)",
			"notIn":     "SELECT a.id FROM st_pair a WHERE " + outer + " NOT IN (SELECT " + cast + " FROM st_pair r WHERE r.id <= 3)",
			"exists":    "SELECT a.id FROM st_pair a WHERE EXISTS (SELECT 1 FROM st_pair r WHERE r.id <= 3 AND " + outer + " = " + cast + ")",
			"notExists": "SELECT a.id FROM st_pair a WHERE NOT EXISTS (SELECT 1 FROM st_pair r WHERE r.id <= 3 AND " + outer + " = " + cast + ")",
		}
		for _, op := range []string{"in", "notIn", "exists", "notExists"} {
			pg := sqls[op]
			if p.kept {
				pg = strings.ReplaceAll(pg, cast, "r.v_"+p.origin)
			}
			out = append(out, smCell{name: "xcast/" + op + "/" + p.tl + "From" + p.origin, sql: sqls[op], pgSQL: pg})
		}
	}
	return out
}

// smIssueCells are the six issues' own statements, verbatim.
func smIssueCells() []smCell {
	c := func(name, sql string) smCell { return smCell{name: "issue/" + name, sql: sql, pgSQL: sql} }
	const n = "SELECT count(*) AS n FROM st_pair a WHERE "
	return []smCell{
		c("1372/in", "SELECT a.id FROM st_pair a WHERE '12' IN (SELECT r.v_i64 FROM st_pair r WHERE r.id <= 3)"),
		c("1372/eqAny", "SELECT a.id FROM st_pair a WHERE '12' = ANY (SELECT r.v_i64 FROM st_pair r WHERE r.id <= 3)"),
		c("1372/notIn", "SELECT a.id FROM st_pair a WHERE '12' NOT IN (SELECT r.v_i64 FROM st_pair r WHERE r.id <= 3)"),
		c("1372/date", "SELECT a.id FROM st_pair a WHERE '2024-01-02' IN (SELECT r.v_date FROM st_pair r WHERE r.id <= 3)"),
		c("1372/uuid", "SELECT a.id FROM st_pair a WHERE '00000000-0000-4000-8000-000000000001' IN (SELECT r.v_uuid FROM st_pair r WHERE r.id <= 3)"),
		c("1373/unionAll", n+"a.v_date IN (SELECT r.v_date FROM st_pair r WHERE r.id = 1 UNION ALL SELECT r.v_date FROM st_pair r WHERE r.id = 2)"),
		c("1373/union", n+"a.v_date IN (SELECT r.v_date FROM st_pair r WHERE r.id = 1 UNION SELECT r.v_date FROM st_pair r WHERE r.id = 2)"),
		c("1373/literal", n+"a.v_date IN (SELECT DATE '2024-01-02' FROM st_pair r WHERE r.id = 1)"),
		c("1373/literalUnion", n+"a.v_date IN (SELECT DATE '2024-01-02' FROM st_pair r WHERE r.id = 1 UNION SELECT DATE '2024-03-04' FROM st_pair r WHERE r.id = 2)"),
		c("1373/castUnion", n+"a.v_date IN (SELECT CAST('2024-01-02' AS DATE) FROM st_pair r WHERE r.id = 1 UNION SELECT CAST('2024-03-04' AS DATE) FROM st_pair r WHERE r.id = 2)"),
		c("1373/fromless", n+"a.v_date IN (SELECT DATE '2024-01-02' UNION ALL SELECT DATE '2024-03-04')"),
		c("1373/eqAny", n+"a.v_date = ANY (SELECT r.v_date FROM st_pair r WHERE r.id = 1 UNION ALL SELECT r.v_date FROM st_pair r WHERE r.id = 2)"),
		c("1373/notIn", n+"a.v_date NOT IN (SELECT DATE '2024-01-02' FROM st_pair r WHERE r.id = 1 UNION ALL SELECT DATE '2024-03-04' FROM st_pair r WHERE r.id = 2)"),
		c("1373/tsLiteralUnion", n+"a.v_ts IN (SELECT TIMESTAMP '2024-01-02 03:04:05' FROM st_pair r WHERE r.id = 1 UNION SELECT TIMESTAMP '2024-03-04 03:04:05' FROM st_pair r WHERE r.id = 2)"),
		c("1369/in", n+"a.v_i64 + 0 IN (SELECT r.s_i64 FROM st_pair r WHERE r.id <= 3)"),
		c("1369/notIn", n+"a.v_i64 + 0 NOT IN (SELECT r.s_i64 FROM st_pair r WHERE r.id <= 3)"),
		c("1370/upper", n+"a.v_i64 IN (SELECT upper(r.s_i64) FROM st_pair r WHERE r.id <= 3)"),
		c("1370/concat", n+"a.v_i64 IN (SELECT r.s_i64 || '' FROM st_pair r WHERE r.id <= 3)"),
		c("1370/dateUpper", n+"a.v_date IN (SELECT upper(r.s_date) FROM st_pair r WHERE r.id <= 2)"),
		c("1370/coalesce", n+"a.v_i64 IN (SELECT coalesce(r.s_i64, '0') FROM st_pair r WHERE r.id <= 3)"),
		c("1370/substr", n+"a.v_i64 IN (SELECT substr(r.s_i64, 1, 2) FROM st_pair r WHERE r.id <= 3)"),
		c("1370/case", n+"a.v_i64 IN (SELECT CASE WHEN r.id > 0 THEN r.s_i64 END FROM st_pair r WHERE r.id <= 3)"),
		c("1370/uuidLower", n+"a.v_uuid IN (SELECT lower(r.s_uuid) FROM st_pair r WHERE r.id <= 3)"),
		c("1368/lateral", "SELECT count(*) AS n FROM st_pair a, LATERAL (SELECT r.s_i64 FROM st_pair r WHERE r.s_i64 = a.v_i64) l"),
		c("1374/in", n+"a.v_dec IN (SELECT CAST(r.v_i64 AS TEXT) FROM st_pair r WHERE r.id <= 3)"),
		c("1374/exists", n+"EXISTS (SELECT 1 FROM st_pair r WHERE r.id <= 3 AND a.v_dec = CAST(r.v_i64 AS TEXT))"),
		c("1374/notExists", n+"NOT EXISTS (SELECT 1 FROM st_pair r WHERE r.id <= 3 AND a.v_dec = CAST(r.v_i64 AS TEXT))"),
	}
}

// smCells is the whole generated table.
func smCells() []smCell {
	var out []smCell
	out = append(out, smValueCells()...)
	out = append(out, smKeyCells()...)
	out = append(out, smTextCells()...)
	out = append(out, smCrossCastCells()...)
	out = append(out, smIssueCells()...)
	out = append(out, smLiteralCells()...)
	out = append(out, smPrecisionCells()...)
	out = append(out, smRenderKeptCells()...)
	return out
}

// smPGAnswers reads PostgreSQL 17.11's answer for every generated cell,
// measured once over the same fixture (testdata/arc_sm_membership_pg17.tsv).
func smPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_sm_membership_pg17.tsv")
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

// smMessageOverride is the one place this engine's message names a
// different type than PostgreSQL's with the same SQLSTATE and the same
// decision: `CAST(… AS INTEGER)` declares bigint here (docs/
// postgres-differences.md, "Integer expressions declare bigint").
func smMessageOverride(name, msg string) string {
	if strings.Contains(name, "/i32/castLit/") {
		return strings.Replace(msg, "integer = text", "bigint = text", 1)
	}
	return msg
}

// A TYPED MEMBERSHIP'S TWO OPERANDS ARE TYPED BY ONE RULE, WHATEVER THEIR
// SHAPE (#1372 #1373 #1369 #1370 #1368 #1374). The operand-shape table over
// `st_pair`, on five arms, against PostgreSQL 17.11's own answer for every
// cell — the full sorted rows, never a count alone:
//
//   - val/: an outer column, expression, quoted literal or typed literal
//     against a body of every shape selecting the same type (column,
//     expression, typed literal with and without FROM, grouped aggregate,
//     UNION ALL / UNION of columns and of literals, a NULL beside a value,
//     derived table, CTE, correlated column and expression) as IN / NOT IN /
//     = ANY / <> ALL. At v0.25.1 a quoted-literal outer answered 0 rows on
//     the single-process arms where PostgreSQL resolves it to the body's type
//     and answers every row (#1372), a DATE against any body the planner
//     does not turn into a semi join answered 0 rows there (#1373), and an
//     un-aliased grouped-aggregate body was an internal plan error on every
//     arm.
//   - key/: EXISTS / NOT EXISTS / LATERAL whose correlated equality pairs
//     the same type — the controls the correlated-key rule must keep.
//   - txt/: an outer column, expression or typed literal against a TEXT
//     column, a TEXT expression and `CAST(v AS TEXT)`, as a membership and
//     as a correlated key: 42883 (#1369 #1370 #1368) but for the kept
//     same-type CAST, whose expected rows are PostgreSQL's answer for the
//     typed comparison the kept reading is; a LATERAL key keeps no text
//     reading.
//   - xcast/: #1374 — one comparison as IN and as EXISTS, one answer.
//   - issue/: the six issues' own statements.
func TestArcSMMembershipOperandsEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the membership operand-shape table")
	}
	answers := smPGAnswers(t)
	cells := smCells()
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := smArms(t, ctx)
	refused, answered := 0, 0
	for _, tc := range cells {
		want := answers[tc.name]
		state, msg := "", ""
		if rest, ok := strings.CutPrefix(want, "ERR "); ok {
			state, msg, _ = strings.Cut(rest, " ")
			msg = smMessageOverride(tc.name, msg)
			refused++
		} else {
			answered++
		}
		t.Run(tc.name, func(t *testing.T) {
			// The five arms are five engines: each cell runs on all of them
			// at once, and is asserted in arm order.
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
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s %q (PostgreSQL 17.11)", tc.sql, arm.name, brRender(res), state, msg)
						continue
					}
					if st := sqlerr.StateOf(err); st != state || !strings.Contains(err.Error(), msg) {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s %q", tc.sql, arm.name, st, err, state, msg)
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
	if refused < 300 || answered < 1500 {
		t.Fatalf("%d refused / %d answered cells: the table must hold both", refused, answered)
	}
}

// THE PLAN NEVER CARRIES A TEXT/TYPED KEY (#1369 #1370 #1368 #1374): EXPLAIN
// of every operand shape the rule refuses is the refusal itself, on every
// arm, before any plan exists — at v0.25.1 each planned a membership filter
// or a join whose key paired a typed column with a TEXT one
// (`Join: join ON a.v_i64 = l.s_i64` for the LATERAL body, `join ON a.v_i64
// = l.__key_0` for its CAST key). A quoted literal that is no value of the
// body's type is the literal's own refusal. The kept spellings plan: the
// same-type EXISTS CAST key is a filter carrying the CAST, and a LATERAL
// key of one type keeps its join key.
func TestArcSMExplainMembershipKeyIsOneType(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := stArms(t, ctx)
	refused := []struct{ sql, state string }{
		{"EXPLAIN SELECT a.id FROM st_pair a WHERE a.v_i64 + 0 IN (SELECT r.s_i64 FROM st_pair r WHERE r.id <= 3)", "42883"},
		{"EXPLAIN SELECT a.id FROM st_pair a WHERE a.v_i64 IN (SELECT upper(r.s_i64) FROM st_pair r WHERE r.id <= 3)", "42883"},
		{"EXPLAIN SELECT a.id FROM st_pair a WHERE a.v_date IN (SELECT r.s_date || '' FROM st_pair r WHERE r.id <= 3)", "42883"},
		{"EXPLAIN SELECT a.id, l.s_i64 FROM st_pair a, LATERAL (SELECT r.s_i64 FROM st_pair r WHERE r.s_i64 = a.v_i64) l", "42883"},
		{"EXPLAIN SELECT a.id, l.rid FROM st_pair a, LATERAL (SELECT r.id AS rid FROM st_pair r WHERE a.v_i64 = CAST(r.v_i64 AS TEXT)) l", "42883"},
		{"EXPLAIN SELECT a.id FROM st_pair a WHERE EXISTS (SELECT 1 FROM st_pair r WHERE a.v_dec = CAST(r.v_i64 AS TEXT))", "42883"},
		{"EXPLAIN SELECT a.id FROM st_pair a WHERE EXISTS (SELECT 1 FROM st_pair r WHERE coalesce(a.v_i64, 0) = r.s_i64)", "42883"},
		{"EXPLAIN SELECT a.id FROM st_pair a WHERE 'zz' IN (SELECT r.v_i64 FROM st_pair r)", "22P02"},
	}
	kept := []struct{ sql, must, never string }{
		{"EXPLAIN SELECT a.id FROM st_pair a WHERE EXISTS (SELECT 1 FROM st_pair r WHERE a.v_i64 = CAST(r.v_i64 AS TEXT))", "CAST(", "ON a.v_i64 = l."},
		{"EXPLAIN SELECT a.id, l.rid FROM st_pair a, LATERAL (SELECT r.id AS rid FROM st_pair r WHERE r.v_i64 = a.v_i64) l", "v_i64", "s_i64"},
		// The quoted-literal outer is typed IN THE PLAN every arm reads —
		// the subquery's type, never its typmod — so the DAG's inlined IN
		// list compares a typed value too (#1372).
		{"EXPLAIN SELECT a.id FROM st_pair a WHERE '2024-1-2' IN (SELECT r.v_date FROM st_pair r)", "cast('2024-1-2' as DATE) in (", "['2024-1-2' in"},
		{"EXPLAIN SELECT a.id FROM st_pair a WHERE '2001:DB8::1' NOT IN (SELECT r.v_ipv6 FROM st_pair r WHERE r.id <= 3)", "cast('2001:DB8::1' as IPV6) not in (", "['2001:DB8::1' not in"},
		{"EXPLAIN SELECT a.id FROM st_pair a WHERE '1_2' = ANY (SELECT r.v_i64 FROM st_pair r)", "cast('1_2' as BIGINT)", "['1_2' ="},
		// The literal keeps its OWN scale — NUMERIC(38,5) for 5 fractional
		// digits, never bare NUMERIC — so the per-row comparison stays an
		// exact decimal instead of float8's ~15-17 digits.
		{"EXPLAIN SELECT a.id FROM st_pair a WHERE '12.50001' IN (SELECT r.v_dec FROM st_pair r)", "cast('12.50001' as NUMERIC(38,5)) in (", "['12.50001' in"},
	}
	for _, arm := range arms {
		for _, c := range refused {
			res, err := arm.run(c.sql)
			if err == nil {
				t.Errorf("%s\n  arm  %s\n  plan %s\n  want %s before any plan", c.sql, arm.name, brRenderOrdered(res), c.state)
				continue
			}
			if st := sqlerr.StateOf(err); st != c.state {
				t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s", c.sql, arm.name, st, err, c.state)
			}
		}
		for _, c := range kept {
			res, err := arm.run(c.sql)
			if err != nil {
				t.Errorf("%s\n  arm  %s\n  refused: %v", c.sql, arm.name, err)
				continue
			}
			plan := brRenderOrdered(res)
			if !strings.Contains(plan, c.must) || strings.Contains(plan, c.never) {
				t.Errorf("%s\n  arm  %s\n  plan %s\n  want %q and never %q", c.sql, arm.name, plan, c.must, c.never)
			}
		}
	}
}
