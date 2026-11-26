// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// #1713c: INTERVAL equality and order. PostgreSQL compares two intervals by
// one normalized value (interval_cmp_value: a month is 30 days, a day 24
// hours) while printing the fields as stored, so `INTERVAL '1 day' =
// INTERVAL '24 hours'` is true and `INTERVAL '2 days' < INTERVAL '10 days'`.
// Two interval VALUES (a literal, a CAST to INTERVAL) compared each other's
// rendered TEXT here (compare()'s last resort), so the first was false and
// the second false too.
//
// The table is every consumer over every arm (single, spilled at 512 KiB,
// the DAG, the DAG shuffled, the DAG with four morsel workers, the
// coordinator's small-query fast path) against
// testdata/arc_1713c_interval_order_pg17.tsv — PostgreSQL 17.11's own
// answers (postgres:17-alpine, --locale=C --encoding=UTF8, IntervalStyle
// postgres), rewritten by TestArc1713cMeasure (IV_PG_DSN).
//
// An interval that crosses a projection is TEXT here (ADR-0012 temporal r20:
// no INTERVAL column type), so every consumer that keys or orders a
// MATERIALIZED interval — GROUP BY, DISTINCT, the set operations, a hash
// join's key, PARTITION BY, ORDER BY, min / max — reads the rendered text.
// That is the representation, not a comparison kernel, and those cells are
// pinned in ivKnown at their present answer (a pin fails the moment the arm
// starts agreeing). The header compares column NAMES only: the declared type
// of a projected interval is r20's documented text.

type ivCell struct {
	name string
	sql  string
	ddl  bool
}

const ivAnswersPath = "testdata/arc_1713c_interval_order_pg17.tsv"

// ivPairs are the operand pairs of the scalar comparison cells, as interval
// input text. Every pair is measured; PostgreSQL decides which are equal.
var ivPairs = [][2]string{
	{"1 day", "24 hours"},
	{"24 hours", "1 day"},
	{"1 day", "1 day"},
	{"1 month", "30 days"},
	{"1 month", "720 hours"},
	{"30 days", "720 hours"},
	{"1 month", "31 days"},
	{"1 month", "29 days"},
	{"1 year", "12 months"},
	{"1 year", "360 days"},
	{"1 year", "365 days"},
	{"12 months", "8640 hours"},
	{"2 days", "10 days"},
	{"10 days", "2 days"},
	{"25 hours", "1 day"},
	{"23 hours", "1 day"},
	{"1 hour", "60 minutes"},
	{"1 hour", "3600 seconds"},
	{"59 minutes", "3600 seconds"},
	{"1 week", "7 days"},
	{"1 week", "168 hours"},
	{"0 days", "0 seconds"},
	{"0 days", "0 months"},
	{"-1 day", "-24 hours"},
	{"-1 day", "1 day"},
	{"-1 month", "-30 days"},
	{"-1 month", "-29 days"},
	{"-2 hours", "-1 day"},
	{"100000 years", "1200000 months"},
	{"100000 years", "36000000 days"},
	{"100000 years", "36000001 days"},
	{"9 seconds", "10 seconds"},
}

// ivSpell is the INTERVAL literal of input text s.
func ivSpell(s string) string { return "INTERVAL '" + s + "'" }

// ivCast is CAST(text AS INTERVAL) of input text s.
func ivCast(s string) string { return "CAST('" + s + "' AS INTERVAL)" }

func ivCmpSQL(a, b string) string {
	return "SELECT " + a + " = " + b + " AS eq, " + a + " <> " + b + " AS ne, " + a + " < " + b + " AS lt, " +
		a + " <= " + b + " AS le, " + a + " > " + b + " AS gt, " + a + " >= " + b + " AS ge, " +
		a + " IS DISTINCT FROM " + b + " AS dist, " + a + " IS NOT DISTINCT FROM " + b + " AS ndist"
}

// ivRows is the fixture: id and the interval input text a row holds.
var ivRows = []string{"'1 day'", "'24 hours'", "'1 month'", "'30 days'", "'720 hours'", "'2 days'", "'10 days'",
	"'-1 day'", "NULL", "'1 year'", "'360 days'", "'25 hours'", "'1 week'", "'7 days'", "'0 days'"}

func ivCells() []ivCell {
	var out []ivCell
	add := func(name, sql string) { out = append(out, ivCell{name: name, sql: sql}) }
	out = append(out, ivCell{name: "ddl/create", sql: "CREATE TABLE iv_s (id BIGINT, s TEXT)", ddl: true})
	var vals []string
	for i, v := range ivRows {
		vals = append(vals, "("+strconv.Itoa(i+1)+", "+v+")")
	}
	out = append(out, ivCell{name: "ddl/insert", sql: "INSERT INTO iv_s VALUES " + strings.Join(vals, ", "), ddl: true})

	// Scalar comparison: two literals, two CASTs, a literal beside a CAST.
	for _, p := range ivPairs {
		k := p[0] + "~" + p[1]
		add("cmp/lit/"+k, ivCmpSQL(ivSpell(p[0]), ivSpell(p[1])))
		add("cmp/cast/"+k, ivCmpSQL(ivCast(p[0]), ivCast(p[1])))
		add("cmp/mixed/"+k, ivCmpSQL(ivSpell(p[0]), ivCast(p[1])))
	}
	// The other boxed consumers of a comparison.
	add("box/between", "SELECT INTERVAL '25 hours' BETWEEN INTERVAL '1 day' AND INTERVAL '2 days' AS a, INTERVAL '3 days' BETWEEN INTERVAL '2 days' AND INTERVAL '10 days' AS b, INTERVAL '1 month' BETWEEN INTERVAL '30 days' AND INTERVAL '720 hours' AS c, INTERVAL '1 month' NOT BETWEEN INTERVAL '29 days' AND INTERVAL '31 days' AS d")
	add("box/in", "SELECT INTERVAL '1 day' IN (INTERVAL '24 hours', INTERVAL '2 days') AS a, INTERVAL '1 month' IN (INTERVAL '30 days') AS b, INTERVAL '1 year' NOT IN (INTERVAL '360 days', INTERVAL '1 day') AS c, INTERVAL '1 day' IN (INTERVAL '2 days', NULL) AS d")
	add("box/case", "SELECT CASE INTERVAL '1 day' WHEN INTERVAL '24 hours' THEN 1 ELSE 0 END AS a, CASE WHEN INTERVAL '2 days' < INTERVAL '10 days' THEN 1 ELSE 0 END AS b")
	add("box/null", "SELECT INTERVAL '1 day' = NULL AS a, CAST(NULL AS INTERVAL) < INTERVAL '1 day' AS b, CAST(NULL AS INTERVAL) IS DISTINCT FROM INTERVAL '1 day' AS c, CAST(NULL AS INTERVAL) IS NOT DISTINCT FROM CAST(NULL AS INTERVAL) AS d")
	add("box/subsecond", "SELECT INTERVAL '0.5 seconds' = INTERVAL '500 milliseconds' AS a")
	add("box/multifield", "SELECT CAST('1 day 02:00:00' AS INTERVAL) = CAST('26 hours' AS INTERVAL) AS a, CAST('1 mon -30 days' AS INTERVAL) = INTERVAL '0 days' AS b, CAST('1 day 02:00:00' AS INTERVAL) = CAST('1 day 02:00:00' AS INTERVAL) AS c")
	add("box/where", "SELECT 1 AS one WHERE INTERVAL '1 day' = INTERVAL '24 hours'")

	// Runtime values: a CAST of a stored text column, row by row.
	c := "CAST(s AS INTERVAL)"
	add("row/eq", "SELECT id FROM iv_s WHERE "+c+" = INTERVAL '1 day' ORDER BY id")
	add("row/eqMonth", "SELECT id FROM iv_s WHERE "+c+" = INTERVAL '30 days' ORDER BY id")
	add("row/ne", "SELECT id FROM iv_s WHERE "+c+" <> INTERVAL '24 hours' ORDER BY id")
	add("row/lt", "SELECT id FROM iv_s WHERE "+c+" < INTERVAL '3 days' ORDER BY id")
	add("row/ge", "SELECT id FROM iv_s WHERE "+c+" >= INTERVAL '1 month' ORDER BY id")
	add("row/in", "SELECT id FROM iv_s WHERE "+c+" IN (INTERVAL '1 day', INTERVAL '1 month') ORDER BY id")
	add("row/notIn", "SELECT id FROM iv_s WHERE "+c+" NOT IN (INTERVAL '1 day', INTERVAL '1 month') ORDER BY id")
	add("row/between", "SELECT id FROM iv_s WHERE "+c+" BETWEEN INTERVAL '1 day' AND INTERVAL '30 days' ORDER BY id")
	add("row/notDistinct", "SELECT id FROM iv_s WHERE "+c+" IS NOT DISTINCT FROM INTERVAL '24 hours' ORDER BY id")
	add("row/distinct", "SELECT id FROM iv_s WHERE "+c+" IS DISTINCT FROM INTERVAL '7 days' ORDER BY id")
	add("row/project", "SELECT id, "+c+" = INTERVAL '30 days' AS e, "+c+" < INTERVAL '2 days' AS l FROM iv_s ORDER BY id")
	add("row/case", "SELECT id, CASE "+c+" WHEN INTERVAL '24 hours' THEN 'day' WHEN INTERVAL '1 month' THEN 'month' ELSE 'other' END AS k FROM iv_s ORDER BY id")
	add("row/crossLt", "SELECT a.id AS l, b.id AS r FROM iv_s a, iv_s b WHERE CAST(a.s AS INTERVAL) < CAST(b.s AS INTERVAL) AND a.id = 1 ORDER BY l, r")
	add("row/crossEqResidual", "SELECT a.id AS l, b.id AS r FROM iv_s a, iv_s b WHERE (CAST(a.s AS INTERVAL) = CAST(b.s AS INTERVAL) OR a.id = 0) AND a.id < b.id ORDER BY l, r")
	add("row/exists", "SELECT id FROM iv_s a WHERE EXISTS (SELECT 1 FROM iv_s b WHERE b.id <> a.id AND CAST(b.s AS INTERVAL) <= CAST(a.s AS INTERVAL) AND b.id = 1) ORDER BY id")

	// Keys and order over a MATERIALIZED interval (r20: text here).
	add("key/groupBy", "SELECT "+c+" AS k, count(*) AS n FROM iv_s GROUP BY "+c+" ORDER BY n, k")
	add("key/groupCount", "SELECT count(*) AS g FROM (SELECT "+c+" AS k FROM iv_s GROUP BY "+c+") t")
	add("key/distinctCount", "SELECT count(*) AS g FROM (SELECT DISTINCT "+c+" AS k FROM iv_s) t")
	add("key/countDistinct", "SELECT count(DISTINCT "+c+") AS n FROM iv_s")
	add("key/join", "SELECT a.id AS l, b.id AS r FROM iv_s a JOIN iv_s b ON CAST(a.s AS INTERVAL) = CAST(b.s AS INTERVAL) WHERE a.id < b.id ORDER BY l, r")
	add("key/inSubquery", "SELECT id FROM iv_s WHERE "+c+" IN (SELECT "+c+" FROM iv_s WHERE id = 2) ORDER BY id")
	add("key/existsEq", "SELECT id FROM iv_s a WHERE EXISTS (SELECT 1 FROM iv_s b WHERE CAST(b.s AS INTERVAL) = CAST(a.s AS INTERVAL) AND b.id <> a.id) ORDER BY id")
	add("key/scalarSub", "SELECT id FROM iv_s WHERE "+c+" = (SELECT "+c+" FROM iv_s WHERE id = 2) ORDER BY id")
	add("key/joinDerived", "SELECT a.id AS l, b.id AS r FROM (SELECT id, "+c+" AS k FROM iv_s) a JOIN (SELECT id, "+c+" AS k FROM iv_s) b ON a.k = b.k WHERE a.id < b.id ORDER BY l, r")
	add("key/joinMixed", "SELECT a.id AS l, b.id AS r FROM iv_s a JOIN (SELECT id, "+c+" AS k FROM iv_s) b ON CAST(a.s AS INTERVAL) = b.k WHERE a.id < b.id ORDER BY l, r")
	add("key/leftJoin", "SELECT a.id AS l, b.id AS r FROM iv_s a LEFT JOIN iv_s b ON CAST(a.s AS INTERVAL) = CAST(b.s AS INTERVAL) AND a.id < b.id ORDER BY l, r")
	add("key/derivedFilter", "SELECT id FROM (SELECT id, "+c+" AS k FROM iv_s) t WHERE k = INTERVAL '24 hours' ORDER BY id")
	add("key/partition", "SELECT id, count(*) OVER (PARTITION BY "+c+") AS n FROM iv_s ORDER BY id")
	add("key/union", "SELECT count(*) AS n FROM (SELECT INTERVAL '1 day' AS x UNION SELECT INTERVAL '24 hours') t")
	add("key/intersect", "SELECT count(*) AS n FROM (SELECT INTERVAL '1 day' AS x INTERSECT SELECT INTERVAL '24 hours') t")
	add("key/except", "SELECT count(*) AS n FROM (SELECT INTERVAL '1 month' AS x EXCEPT SELECT INTERVAL '30 days') t")
	add("key/valuesGroup", "SELECT count(*) AS g FROM (SELECT x FROM (VALUES (INTERVAL '1 day'), (INTERVAL '24 hours'), (INTERVAL '1 month'), (INTERVAL '30 days')) v(x) GROUP BY x) t")
	add("order/asc", "SELECT id FROM iv_s WHERE s IS NOT NULL AND id IN (1, 3, 6, 7, 8, 12) ORDER BY "+c+", id")
	add("order/desc", "SELECT id FROM iv_s WHERE s IS NOT NULL AND id IN (1, 3, 6, 7, 8, 12) ORDER BY "+c+" DESC, id")
	add("order/topN", "SELECT id FROM iv_s WHERE s IS NOT NULL AND id IN (1, 3, 6, 7, 8, 12) ORDER BY "+c+" DESC, id LIMIT 2")
	add("order/minMax", "SELECT min("+c+") AS lo, max("+c+") AS hi FROM iv_s WHERE id IN (1, 6, 7, 12)")
	add("order/values", "SELECT x FROM (VALUES (INTERVAL '2 days'), (INTERVAL '10 days'), (INTERVAL '25 hours')) v(x) ORDER BY x")

	// Arithmetic and printing where they expose the fields.
	add("print/fields", "SELECT INTERVAL '1 month' AS a, INTERVAL '30 days' AS b, INTERVAL '25 hours' AS c, INTERVAL '14 months' AS d, INTERVAL '-1 day' AS e")
	add("print/castText", "SELECT CAST(INTERVAL '1 month' AS TEXT) AS a, CAST(INTERVAL '720 hours' AS TEXT) AS b")
	add("arith/times", "SELECT INTERVAL '1 day' * 2 AS a")
	add("arith/div", "SELECT INTERVAL '1 day' / 2 AS a")
	add("arith/plus", "SELECT INTERVAL '1 day' + INTERVAL '1 hour' AS a")
	add("arith/neg", "SELECT -INTERVAL '1 day' AS a")
	add("arith/tsMonth", "SELECT CAST(TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '1 month' AS TEXT) AS a")
	add("arith/tsDays", "SELECT CAST(TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '30 days' AS TEXT) AS a")
	add("arith/justifyDays", "SELECT justify_days(INTERVAL '30 days') AS a")
	add("arith/extract", "SELECT extract(day FROM INTERVAL '30 days') AS a")

	// Containers.
	add("box/array", "SELECT ARRAY[INTERVAL '1 day'] = ARRAY[INTERVAL '24 hours'] AS a, ARRAY[INTERVAL '2 days'] < ARRAY[INTERVAL '10 days'] AS b, ARRAY[INTERVAL '1 month'] = ARRAY[INTERVAL '30 days'] AS c")
	add("box/arrayGroup", "SELECT count(*) AS g FROM (SELECT x FROM (VALUES (ARRAY[INTERVAL '1 day']), (ARRAY[INTERVAL '24 hours'])) v(x) GROUP BY x) t")
	return out
}

// ivNormalize reduces an answer to the form both sides compare in: column
// NAMES without types (r20) and booleans as PostgreSQL's t / f.
func ivNormalize(s string) string {
	head, body, ok := strings.Cut(s, " | ")
	if !ok {
		return s
	}
	cols := strings.Split(head, ",")
	for i, c := range cols {
		if n, _, ok := strings.Cut(c, ":"); ok {
			cols[i] = n
		}
	}
	rows := strings.Split(body, "; ")
	for i, r := range rows {
		f := strings.Split(r, ",")
		for j := range f {
			switch f[j] {
			case "true":
				f[j] = "t"
			case "false":
				f[j] = "f"
			}
		}
		rows[i] = strings.Join(f, ",")
	}
	return strings.Join(cols, ",") + " | " + strings.Join(rows, "; ")
}

func ivPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open(ivAnswersPath)
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

// TestArc1713cMeasure runs every cell against PostgreSQL (IV_PG_DSN) and
// rewrites the answer file; skipped otherwise.
func TestArc1713cMeasure(t *testing.T) {
	dsn := os.Getenv("IV_PG_DSN")
	if dsn == "" {
		t.Skip("IV_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	run := func(sql string) *pgconn.Result {
		return conn.ExecParams(ctx, sql, nil, nil, nil, nil).Read()
	}
	if r := run("SET statement_timeout = '30s'"); r.Err != nil {
		t.Fatal(r.Err)
	}
	run("DROP TABLE IF EXISTS iv_s")
	var b strings.Builder
	b.WriteString("# PostgreSQL 17.11 (postgres:17-alpine, --locale=C --encoding=UTF8, IntervalStyle postgres) answers for arc_1713c_interval_order_arms_test.go (TestArc1713cMeasure).\n")
	b.WriteString("# name<TAB>names | rows (t/f booleans) | ERR <SQLSTATE> | ok (DDL).\n")
	for _, c := range ivCells() {
		r := run(c.sql)
		var ans string
		switch {
		case r.Err != nil:
			pe, ok := r.Err.(*pgconn.PgError)
			if !ok {
				t.Fatalf("%s: %v", c.name, r.Err)
			}
			ans = "ERR " + pe.Code
		case c.ddl:
			ans = "ok"
		default:
			header := make([]string, len(r.FieldDescriptions))
			for i, fd := range r.FieldDescriptions {
				header[i] = fd.Name
			}
			rows := make([]string, len(r.Rows))
			for i, row := range r.Rows {
				f := make([]string, len(row))
				for j, v := range row {
					if v == nil {
						f[j] = "NULL"
					} else {
						f[j] = string(v)
					}
				}
				rows[i] = strings.Join(f, ",")
			}
			ans = ftRender(header, rows)
		}
		b.WriteString(c.name + "\t" + ans + "\n")
	}
	if err := os.WriteFile(ivAnswersPath, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// ivKnown is the pinned answer of a cell whose difference from PostgreSQL is
// outside this line's seam, each identical at base 2fa0beeb and at the fix.
// A key "cell@arm" overrides "cell" on that arm. A pin FAILS the moment the
// arm starts agreeing or changes in any other way — delete or re-pin it then.
//
//   - key/*, order/* (but key/inSubquery, key/joinMixed below): the interval
//     crossed a projection and is its rendered TEXT (r20), so the hash key,
//     the sort key, the set operation and min / max read text: equal
//     intervals split into groups, and "10 days" sorts before "2 days".
//     Representation, not a kernel (no INTERVAL column type).
//   - key/scalarSub, key/inSubquery: the subquery's answer is that text, met
//     by an interval value; text against interval keeps the text order. On
//     the single-process arms key/inSubquery answers no rows at all, not even
//     the row whose spelling matches (a separate defect, every arm unchanged).
//   - key/joinMixed on the three DAG arms (not the fast path): the ON
//     condition of a join between a CAST expression and a derived column is
//     lost, so every pair with a.id < b.id joins (a separate defect).
//   - box/subsecond: this engine refuses a sub-second interval (22007);
//     box/multifield: input the single-unit fields cannot hold is OPAQUE text
//     here (IntervalValue.text) and keeps the text order.
//   - arith/*: interval arithmetic boxes 0 (`* 2`, `/ 2`, `+`, unary minus),
//     `timestamp + INTERVAL '1 month'` overflows into March from January 31
//     (Go's AddDate normalization; PostgreSQL clamps to the month's last
//     day), extract of an interval is NULL, justify_days does not exist.
var ivKnown = map[string]string{
	"box/subsecond":              "ERR 22007",
	"box/multifield":             "a,b,c | f,f,t",
	"key/groupBy":                "k,n | -1 days,1; 00:00:00,1; 1 day,1; 1 mon,1; 1 year,1; 10 days,1; 2 days,1; 24:00:00,1; 25:00:00,1; 30 days,1; 360 days,1; 720:00:00,1; NULL,1; 7 days,2",
	"key/groupCount":             "g | 14",
	"key/distinctCount":          "g | 14",
	"key/countDistinct":          "n | 13",
	"key/inSubquery":             "id | 2",
	"key/inSubquery@single":      "(0 rows)",
	"key/inSubquery@spilled512k": "(0 rows)",
	"key/scalarSub":              "id | 2",
	"key/joinDerived":            "l,r | 13,14",
	"key/joinMixed":              "l,r | 13,14",
	"key/joinMixed@dag":          ivAllPairs(),
	"key/joinMixed@dag-shuffled": ivAllPairs(),
	"key/joinMixed@dag-morsel4":  ivAllPairs(),
	"key/partition":              "id,n | 1,1; 2,1; 3,1; 4,1; 5,1; 6,1; 7,1; 8,1; 9,1; 10,1; 11,1; 12,1; 13,2; 14,2; 15,1",
	"key/union":                  "n | 2",
	"key/intersect":              "n | 0",
	"key/except":                 "n | 1",
	"key/valuesGroup":            "g | 4",
	"order/asc":                  "id | 8; 1; 3; 7; 6; 12",
	"order/desc":                 "id | 12; 6; 7; 3; 1; 8",
	"order/topN":                 "id | 12; 6",
	"order/minMax":               "lo,hi | 1 day,25:00:00",
	"order/values":               "x | 10 days; 2 days; 25:00:00",
	"arith/times":                "a | 0",
	"arith/div":                  "a | 0",
	"arith/plus":                 "a | 0",
	"arith/neg":                  "a | -0",
	"arith/tsMonth":              "a | 2024-03-02 00:00:00",
	"arith/justifyDays":          "ERR 42883",
	"arith/extract":              "a | NULL",
}

// ivAllPairs is every (l, r) with l < r whose LEFT row's text is not NULL —
// key/joinMixed's DAG answer, its ON condition dropped (the right side's
// NULL row joins too).
func ivAllPairs() string {
	var pairs []string
	for l := 1; l <= len(ivRows); l++ {
		if ivRows[l-1] == "NULL" {
			continue
		}
		for r := l + 1; r <= len(ivRows); r++ {
			pairs = append(pairs, strconv.Itoa(l)+","+strconv.Itoa(r))
		}
	}
	return "l,r | " + strings.Join(pairs, "; ")
}

// ivPin is the pinned answer of cell on arm, or "".
func ivPin(cell, arm string) string {
	if p, ok := ivKnown[cell+"@"+arm]; ok {
		return p
	}
	return ivKnown[cell]
}

func TestArc1713cIntervalOrderEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: six arms over the interval order table")
	}
	answers := ivPGAnswers(t)
	cells := ivCells()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	writer, arms := ivArmsFixture(t, ctx)

	// IV_DUMP=<path> writes every (cell, arm, answer) for a base-against-tip
	// transition table.
	var dump strings.Builder
	defer func() {
		if p := os.Getenv("IV_DUMP"); p != "" {
			if err := os.WriteFile(p, []byte(dump.String()), 0o644); err != nil {
				t.Error(err)
			}
		}
	}()
	agree, pinned := 0, 0
	for _, c := range cells {
		want, ok := answers[c.name]
		if !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
		if c.ddl {
			if got := writer(c.sql); got != want {
				t.Errorf("%s\n  %s\n  writer got  %s\n  want        %s (PostgreSQL 17.11)", c.name, c.sql, got, want)
			}
			continue
		}
		for _, arm := range arms {
			got := ivNormalize(arm.run(c.sql))
			dump.WriteString(c.name + "\t" + arm.name + "\t" + got + "\n")
			if pin := ivPin(c.name, arm.name); pin != "" {
				pinned++
				if got != pin {
					t.Errorf("%s on %s: the pinned answer changed\n  %s\n  got  %s\n  pin  %s\n  PostgreSQL 17.11 %s", c.name, arm.name, c.sql, got, pin, want)
				}
				continue
			}
			if got != want {
				t.Errorf("%s\n  %s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, arm.name, got, want)
				continue
			}
			agree++
		}
	}
	t.Logf("%d cells x %d arms: %d agree with PostgreSQL 17.11, %d pinned", len(cells), len(arms), agree, pinned)
	// The table must discriminate: pairs of DIFFERENT spellings PostgreSQL
	// calls equal are the cells a text comparison gets wrong.
	equalSpellings := 0
	for _, p := range ivPairs {
		if p[0] != p[1] && strings.HasSuffix(answers["cmp/lit/"+p[0]+"~"+p[1]], "| t,f,f,t,f,t,f,t") {
			equalSpellings++
		}
	}
	if equalSpellings < 15 || pinned != 27*len(arms) {
		t.Fatalf("%d equal-but-differently-spelled pairs, %d pinned (cell, arm) answers: the table must discriminate and every pin must be read", equalSpellings, pinned)
	}
}

// ivArmsFixture is ftArmsFixture's five arms plus the coordinator's
// small-query fast path, over one shared catalog; the writer runs the DDL.
func ivArmsFixture(t *testing.T, ctx context.Context) (func(string) string, []struct {
	name string
	run  func(string) string
}) {
	t.Helper()
	infra := tmdInfra(t, ctx)
	open := func(budget int64) *wadjet.DB {
		t.Helper()
		cfg := wadjet.Config{MetaKV: infra.kv, Store: infra.store, Bucket: "test", MemoryBudget: budget}
		if budget > 0 {
			cfg.SpillDir = t.TempDir()
		}
		db, err := wadjet.Open(ctx, cfg)
		if err != nil {
			t.Fatalf("open over the shared catalog: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}
	writer := open(0)
	spilled := open(512 * 1024)
	coord := tmdCoordinator(t, ctx, infra)
	coordB := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := tmdCoordinatorWithWorkers(t, ctx, infra, func(w *worker.Config) { w.MorselWorkers = 4 })
	coordF := tmdCoordinator(t, ctx, infra, func(c *Config) { c.LocalFastPathBytes = 64 << 20 })
	return func(sql string) string {
			_, err := writer.Query(ctx, sql)
			return ftDDLRender(err)
		}, []struct {
			name string
			run  func(string) string
		}{
			{"single", func(sql string) string { return ftRunSingle(ctx, writer, sql) }},
			{"spilled512k", func(sql string) string {
				restoreDrain := exec.ForceAggDrainEvery(1)
				restoreRuns := exec.ForceSmallSpillRuns(2)
				defer func() { restoreRuns(); exec.ForceAggDrainEvery(restoreDrain) }()
				return ftRunSingle(ctx, spilled, sql)
			}},
			{"dag", func(sql string) string { return ftRunDAG(ctx, coord, sql) }},
			{"dag-shuffled", func(sql string) string { return ftRunDAG(ctx, coordB, sql) }},
			{"dag-morsel4", func(sql string) string { return ftRunDAG(ctx, coordM, sql) }},
			{"dag-fastpath", func(sql string) string { return ftRunDAG(ctx, coordF, sql) }},
		}
}
