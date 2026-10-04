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
)

// Arc FO: PostgreSQL's float TOTAL ORDER over every consumer that orders,
// groups or compares a float (#1488, #1489). NaN equals NaN (whatever its
// sign bit) and is greater than every other value, Infinity included; -0
// equals 0, and the value a group, a DISTINCT row, a set operation's row or a
// MIN / MAX publishes is a member's OWN bits — so a group whose only zero is
// -0 publishes -0.
//
// The table is generated: every consumer below over every fixture table
// (float8 and float4; NaN first, in the middle and last; one file and one
// file per row; the values alone and mixed) on the five arms, against
// testdata/arc_fo_float_total_order_pg17.tsv — PostgreSQL 17.11's own
// answers, rewritten by TestArcFOMeasure (FO_PG_DSN).
//
// ADR-0013's class of legal nondeterminism: when one group holds BOTH -0 and
// 0, PostgreSQL publishes whichever member it met first (a hash aggregate's
// first arrival; float8smaller / float8larger keep their first argument on a
// tie), so which zero it publishes depends on the plan. Cells over the
// fo_*_zeros tables — the only tables holding both — compare with the sign of
// every zero ignored, and say so; every other table holds at most one kind of
// zero, so its published zero is exact.

type foTable struct {
	key   string // fo_<type>_<key>
	rows  []string
	split bool // one INSERT (one file) per row
	zeros bool // holds both -0 and 0: the published zero's sign is plan-dependent
}

type foType struct{ key, spell string }

func foTypes() []foType {
	return []foType{{"f8", "DOUBLE PRECISION"}, {"f4", "REAL"}}
}

// foTables are the fixtures, as (id, value) row spellings over the type ty.
func foTables(ty string) []foTable {
	nan := "CAST('NaN' AS " + ty + ")"
	negNaN := "-CAST('NaN' AS " + ty + ")"
	inf := "CAST('Infinity' AS " + ty + ")"
	ninf := "CAST('-Infinity' AS " + ty + ")"
	nz := "CAST('-0' AS " + ty + ")"
	return []foTable{
		{key: "first", rows: []string{nan, inf, ninf, nz, "1.5", negNaN, "NULL"}},
		{key: "mid", rows: []string{ninf, "1.5", nan, inf, nz, "NULL", negNaN}},
		{key: "last", rows: []string{inf, "1.5", nz, ninf, "NULL", negNaN, nan}},
		{key: "multi", rows: []string{inf, nan, "1.5", nz, ninf, "NULL", negNaN}, split: true},
		{key: "naninf", rows: []string{nan, inf}},
		{key: "infnan", rows: []string{inf, nan}},
		{key: "nanonly", rows: []string{nan, negNaN, "NULL"}},
		{key: "negz", rows: []string{nz, "1.5", nz}},
		{key: "zeros", rows: []string{nz, "0", "1.5", "0", nz}, zeros: true},
	}
}

type foCell struct {
	name  string
	sql   string
	ddl   bool
	zeros bool
}

// foQueries are the consumers over one table t of type ty.
func foQueries(t, ty string) []struct{ name, sql string } {
	nan := "CAST('NaN' AS " + ty + ")"
	return []struct{ name, sql string }{
		{"minMax", "SELECT min(c) AS lo, max(c) AS hi FROM " + t},
		{"minMaxExpr", "SELECT min(c * 1) AS lo, max(c * 1) AS hi FROM " + t},
		{"grpMinMax", "SELECT id % 2 AS g, min(c) AS lo, max(c) AS hi FROM " + t + " GROUP BY id % 2 ORDER BY g"},
		{"groupBy", "SELECT c, count(*) AS n FROM " + t + " GROUP BY c ORDER BY c"},
		{"groupByDesc", "SELECT c, count(*) AS n FROM " + t + " GROUP BY c ORDER BY c DESC NULLS LAST"},
		{"groupByExpr", "SELECT c * 1 AS k, count(*) AS n FROM " + t + " GROUP BY c * 1 ORDER BY k"},
		{"groupByMax", "SELECT c, max(id) AS m FROM " + t + " GROUP BY c ORDER BY m"},
		{"groupByBool", "SELECT c, id > 3 AS b, count(*) AS n FROM " + t + " GROUP BY c, id > 3 ORDER BY c, b"},
		{"groupByText", "SELECT c, CAST(id % 2 AS TEXT) AS s, count(*) AS n FROM " + t + " GROUP BY c, CAST(id % 2 AS TEXT) ORDER BY c, s"},
		{"groupByCountDistinct", "SELECT c, count(DISTINCT id) AS n FROM " + t + " GROUP BY c ORDER BY c"},
		{"groupByKeyExpr", "SELECT c * 2 AS d, count(*) AS n FROM " + t + " GROUP BY c ORDER BY d"},
		{"rollup", "SELECT c, count(*) AS n FROM " + t + " GROUP BY ROLLUP (c) ORDER BY c, n"},
		{"distinct", "SELECT DISTINCT c FROM " + t + " ORDER BY c"},
		{"countDistinct", "SELECT count(DISTINCT c) AS n FROM " + t},
		{"orderAsc", "SELECT id FROM " + t + " ORDER BY c, id"},
		{"orderDesc", "SELECT id FROM " + t + " ORDER BY c DESC, id"},
		{"orderAscNullsFirst", "SELECT id FROM " + t + " ORDER BY c NULLS FIRST, id"},
		{"orderDescNullsLast", "SELECT id FROM " + t + " ORDER BY c DESC NULLS LAST, id"},
		{"orderExpr", "SELECT id FROM " + t + " ORDER BY c * 1 DESC, id"},
		{"topNAsc", "SELECT id FROM " + t + " ORDER BY c, id LIMIT 3"},
		{"topNDesc", "SELECT id FROM " + t + " ORDER BY c DESC, id LIMIT 3"},
		{"topNDescNullsLast", "SELECT id, c FROM " + t + " ORDER BY c DESC NULLS LAST, id LIMIT 2"},
		{"join", "SELECT a.id AS l, b.id AS r FROM " + t + " a JOIN " + t + " b ON a.c = b.c ORDER BY l, r"},
		{"joinExpr", "SELECT a.id AS l, b.id AS r FROM " + t + " a JOIN " + t + " b ON a.c * 1 = b.c ORDER BY l, r"},
		{"crossLt", "SELECT a.id AS l, b.id AS r FROM " + t + " a, " + t + " b WHERE a.c < b.c ORDER BY l, r"},
		{"crossGe", "SELECT a.id AS l, b.id AS r FROM " + t + " a, " + t + " b WHERE a.c >= b.c ORDER BY l, r"},
		{"crossNe", "SELECT a.id AS l, b.id AS r FROM " + t + " a, " + t + " b WHERE a.c <> b.c ORDER BY l, r"},
		{"crossProject", "SELECT a.id AS l, b.id AS r, a.c < b.c AS lt, a.c <= b.c AS le, a.c = b.c AS eq, a.c <> b.c AS ne, a.c > b.c AS gt, a.c >= b.c AS ge, a.c IS DISTINCT FROM b.c AS dist FROM " + t + " a, " + t + " b ORDER BY l, r"},
		{"eqNaN", "SELECT id FROM " + t + " WHERE c = 'NaN' ORDER BY id"},
		{"eqNaNCast", "SELECT id FROM " + t + " WHERE c = " + nan + " ORDER BY id"},
		{"neNaN", "SELECT id FROM " + t + " WHERE c <> 'NaN' ORDER BY id"},
		{"ltNaN", "SELECT id FROM " + t + " WHERE c < 'NaN' ORDER BY id"},
		{"leNaN", "SELECT id FROM " + t + " WHERE c <= 'NaN' ORDER BY id"},
		{"gtNaN", "SELECT id FROM " + t + " WHERE c > 'NaN' ORDER BY id"},
		{"geNaN", "SELECT id FROM " + t + " WHERE c >= 'NaN' ORDER BY id"},
		{"gt15", "SELECT id FROM " + t + " WHERE c > 1.5 ORDER BY id"},
		{"geInf", "SELECT id FROM " + t + " WHERE c >= 'Infinity' ORDER BY id"},
		{"gtInf", "SELECT id FROM " + t + " WHERE c > 'Infinity' ORDER BY id"},
		{"eqZero", "SELECT id FROM " + t + " WHERE c = 0 ORDER BY id"},
		{"eqNegZero", "SELECT id FROM " + t + " WHERE c = '-0' ORDER BY id"},
		{"ltZero", "SELECT id FROM " + t + " WHERE c < 0 ORDER BY id"},
		{"geZero", "SELECT id FROM " + t + " WHERE c >= 0 ORDER BY id"},
		{"between", "SELECT id FROM " + t + " WHERE c BETWEEN 1 AND 'NaN' ORDER BY id"},
		{"in", "SELECT id FROM " + t + " WHERE c IN (0, 'NaN') ORDER BY id"},
		{"notIn", "SELECT id FROM " + t + " WHERE c NOT IN (0, 'NaN') ORDER BY id"},
		{"distinctFrom", "SELECT id FROM " + t + " WHERE c IS DISTINCT FROM 'NaN' ORDER BY id"},
		{"notDistinctFrom", "SELECT id FROM " + t + " WHERE c IS NOT DISTINCT FROM 0 ORDER BY id"},
		{"projectCompare", "SELECT id, c > 1.5 AS gt, c = 'NaN' AS eqn, c < 'NaN' AS ltn, c = 0 AS eqz FROM " + t + " ORDER BY id"},
		{"greatestLeast", "SELECT id, greatest(c, 1.5) AS g, least(c, 1.5) AS l, greatest(c, 'NaN') AS gn, least(c, 'NaN') AS ln FROM " + t + " ORDER BY id"},
		{"rank", "SELECT id, rank() OVER (ORDER BY c) AS r, dense_rank() OVER (ORDER BY c) AS d FROM " + t + " ORDER BY id"},
		{"rankDesc", "SELECT id, rank() OVER (ORDER BY c DESC) AS r FROM " + t + " ORDER BY id"},
		{"partition", "SELECT id, count(*) OVER (PARTITION BY c) AS n FROM " + t + " ORDER BY id"},
		{"winMinMax", "SELECT id, min(c) OVER () AS lo, max(c) OVER () AS hi FROM " + t + " ORDER BY id"},
		{"winRunning", "SELECT id, min(c) OVER (ORDER BY id) AS lo, max(c) OVER (ORDER BY id) AS hi FROM " + t + " ORDER BY id"},
		{"winPartMax", "SELECT id, max(c) OVER (PARTITION BY id % 2) AS hi FROM " + t + " ORDER BY id"},
		{"union", "SELECT c FROM " + t + " UNION SELECT c FROM " + t + " ORDER BY c"},
		{"intersect", "SELECT c FROM " + t + " INTERSECT SELECT c FROM " + t + " WHERE id > 2 ORDER BY c"},
		{"except", "SELECT c FROM " + t + " EXCEPT SELECT c FROM " + t + " WHERE id > 3 ORDER BY c"},
		{"inSubquery", "SELECT id FROM " + t + " WHERE c IN (SELECT c FROM " + t + " WHERE id > 3) ORDER BY id"},
		{"exists", "SELECT id FROM " + t + " a WHERE EXISTS (SELECT 1 FROM " + t + " b WHERE b.c = a.c AND b.id <> a.id) ORDER BY id"},
		{"eqScalarMax", "SELECT id FROM " + t + " WHERE c = (SELECT max(c) FROM " + t + ") ORDER BY id"},
		{"eqScalarMaxInf", "SELECT id FROM " + t + " WHERE c = (SELECT max(c) FROM " + t + " WHERE c < 'NaN') ORDER BY id"},
		// Column origins: the same consumers over a derived table, a CTE and a
		// set-operation output.
		{"derivedMinMax", "SELECT min(c) AS lo, max(c) AS hi FROM (SELECT c FROM " + t + " WHERE id > 0) s"},
		{"cteGroupBy", "WITH s AS (SELECT c FROM " + t + ") SELECT c, count(*) AS n FROM s GROUP BY c ORDER BY c"},
		{"unionAllMinMax", "SELECT min(c) AS lo, max(c) AS hi FROM (SELECT c FROM " + t + " UNION ALL SELECT c FROM " + t + ") s"},
		{"unionAllGroupBy", "SELECT c, count(*) AS n FROM (SELECT c FROM " + t + " UNION ALL SELECT c FROM " + t + ") s GROUP BY c ORDER BY c"},
	}
}

func foCells() []foCell {
	var out []foCell
	for _, ty := range foTypes() {
		for _, tb := range foTables(ty.spell) {
			t := "fo_" + ty.key + "_" + tb.key
			out = append(out, foCell{name: "ddl/" + ty.key + "/" + tb.key + "/create", sql: "CREATE TABLE " + t + " (id BIGINT, c " + ty.spell + ")", ddl: true})
			var vals []string
			for i, v := range tb.rows {
				vals = append(vals, "("+strconv.Itoa(i+1)+", "+v+")")
			}
			if tb.split {
				for i, v := range vals {
					out = append(out, foCell{name: "ddl/" + ty.key + "/" + tb.key + "/insert" + strconv.Itoa(i+1), sql: "INSERT INTO " + t + " VALUES " + v, ddl: true})
				}
			} else {
				out = append(out, foCell{name: "ddl/" + ty.key + "/" + tb.key + "/insert", sql: "INSERT INTO " + t + " VALUES " + strings.Join(vals, ", "), ddl: true})
			}
		}
	}
	for _, ty := range foTypes() {
		for _, tb := range foTables(ty.spell) {
			t := "fo_" + ty.key + "_" + tb.key
			for _, q := range foQueries(t, ty.spell) {
				out = append(out, foCell{name: ty.key + "/" + tb.key + "/" + q.name, sql: q.sql, zeros: tb.zeros})
			}
		}
	}
	// VALUES origin: the literal spellings, no stored column at all.
	for _, ty := range foTypes() {
		v := "(VALUES (CAST('-0' AS " + ty.spell + ")), (CAST('Infinity' AS " + ty.spell + ")), (CAST('NaN' AS " + ty.spell + ")), (1.5)) v(c)"
		out = append(out,
			foCell{name: ty.key + "/values/minMax", sql: "SELECT min(c) AS lo, max(c) AS hi FROM " + v},
			foCell{name: ty.key + "/values/groupBy", sql: "SELECT c, count(*) AS n FROM " + v + " GROUP BY c ORDER BY c"},
			foCell{name: ty.key + "/values/orderDesc", sql: "SELECT c FROM " + v + " ORDER BY c DESC"},
			foCell{name: ty.key + "/values/greatest", sql: "SELECT greatest(CAST('NaN' AS " + ty.spell + "), CAST('Infinity' AS " + ty.spell + ")) AS g, least(CAST('NaN' AS " + ty.spell + "), CAST('-Infinity' AS " + ty.spell + ")) AS l"},
		)
	}
	return out
}

// foBoolText renders the engine's boolean cells as PostgreSQL's text (t / f).
func foBoolText(s string) string {
	head, body, ok := strings.Cut(s, " | ")
	if !ok || !strings.Contains(head, ":boolean") {
		return s
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
	return head + " | " + strings.Join(rows, "; ")
}

const foAnswersPath = "testdata/arc_fo_float_total_order_pg17.tsv"

// foZeroClass erases the sign of every zero field: the ADR-0013 class for a
// group holding both -0 and 0 (see the file comment).
func foZeroClass(s string) string {
	head, body, ok := strings.Cut(s, " | ")
	if !ok {
		return s
	}
	rows := strings.Split(body, "; ")
	for i, r := range rows {
		f := strings.Split(r, ",")
		for j := range f {
			if f[j] == "-0" {
				f[j] = "0"
			}
		}
		rows[i] = strings.Join(f, ",")
	}
	return head + " | " + strings.Join(rows, "; ")
}

func foPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open(foAnswersPath)
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

// TestArcFOMeasure runs every cell against PostgreSQL (FO_PG_DSN, e.g.
// postgres://wadjet:wadjet@127.0.0.1:57948/wadjet_oracle) and rewrites the
// answer file; skipped otherwise.
func TestArcFOMeasure(t *testing.T) {
	dsn := os.Getenv("FO_PG_DSN")
	if dsn == "" {
		t.Skip("FO_PG_DSN unset")
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
	for _, ty := range foTypes() {
		for _, tb := range foTables(ty.spell) {
			run("DROP TABLE IF EXISTS fo_" + ty.key + "_" + tb.key)
		}
	}
	var b strings.Builder
	b.WriteString("# PostgreSQL 17.11 answers for arc_fo_float_total_order_arms_test.go (TestArcFOMeasure).\n")
	for _, c := range foCells() {
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
				header[i] = fd.Name + ":" + ftPGTypeName(fd.DataTypeOID)
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
	if err := os.WriteFile(foAnswersPath, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// foKnown is the pinned answer of a cell whose difference from PostgreSQL is
// outside this arc's seam (a filing candidate, identical at base). A pin
// FAILS the moment the arm starts agreeing — delete it then. "" = no pin.
//
//   - eqScalarMax / eqScalarMaxInf, the three DAG arms: a scalar subquery
//     whose answer is NaN or ±Infinity is substituted into the outer filter
//     as a NULL literal (formatGoValue / the vector arm beside it,
//     scalar_extract.go), so `c = (SELECT max(c) …)` answers zero rows where
//     PostgreSQL answers the rows holding that value. The single-process arms
//     evaluate the subquery in place and agree with PostgreSQL.
func foKnown(cell, arm, want string) string {
	parts := strings.Split(cell, "/")
	if len(parts) != 3 || !strings.HasPrefix(arm, "dag") {
		return ""
	}
	// The tables whose subquery answer is NaN or Infinity; negz and zeros
	// answer 1.5, which substitutes faithfully.
	switch parts[1] {
	case "first", "mid", "last", "multi", "naninf", "infnan":
	case "nanonly":
		if parts[2] != "eqScalarMax" {
			return ""
		}
	default:
		return ""
	}
	if parts[2] == "eqScalarMax" || parts[2] == "eqScalarMaxInf" {
		return "(0 rows)"
	}
	return ""
}

func TestArcFOFloatTotalOrderEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the float total-order table")
	}
	answers := foPGAnswers(t)
	cells := foCells()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	writer, arms := ftArmsFixture(t, ctx)

	nanMax, negZero, pinned := 0, 0, 0
	for _, c := range cells {
		want, ok := answers[c.name]
		if !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
		if c.ddl {
			_, err := writer.Query(ctx, c.sql)
			if got := ftDDLRender(err); got != want {
				t.Errorf("%s\n  %s\n  writer got  %s (%v)\n  want        %s (PostgreSQL 17.11)", c.name, c.sql, got, err, want)
			}
			continue
		}
		if strings.HasSuffix(c.name, "/minMax") && strings.HasSuffix(want, ",NaN") {
			nanMax++
		}
		if !c.zeros && strings.Contains(want, "-0") {
			negZero++
		}
		for _, arm := range arms {
			got := foBoolText(arm.run(c.sql))
			if pin := foKnown(c.name, arm.name, want); pin != "" {
				pinned++
				if got != pin {
					t.Errorf("%s on %s: the pinned answer changed\n  got  %s\n  pin  %s\n  PostgreSQL 17.11 %s", c.name, arm.name, got, pin, want)
				}
				continue
			}
			g, w := got, want
			if c.zeros {
				g, w = foZeroClass(got), foZeroClass(want)
			}
			if g != w {
				t.Errorf("%s\n  %s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, arm.name, got, want)
			}
		}
	}
	t.Logf("%d cells, %d minMax cells answering NaN, %d cells publishing an exact -0, %d pinned", len(cells), nanMax, negZero, pinned)
	if len(cells) < 1000 || nanMax < 12 || negZero < 100 || pinned != 26*3 {
		t.Fatalf("%d cells, %d minMax cells answering NaN, %d publishing -0: the table must discriminate", len(cells), nanMax, negZero)
	}
}
