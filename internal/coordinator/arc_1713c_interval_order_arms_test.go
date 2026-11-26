// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
	// typed: the answer file carries each column's PostgreSQL type, and
	// the arms' declared types are checked against ivTypes.
	typed bool
}

// ivPGTypeName names a PostgreSQL result OID the way ftTypeName names ours.
func ivPGTypeName(oid uint32) string {
	switch oid {
	case 1186:
		return "interval"
	case 1114:
		return "timestamp"
	}
	return ftPGTypeName(oid)
}

// ivHeaderTypes is the "name:type,…" header of an answer, or the answer
// itself when it has none (an error, zero rows).
func ivHeaderTypes(s string) string {
	head, _, ok := strings.Cut(s, " | ")
	if !ok {
		return s
	}
	return head
}

// ivTypes is each typed cell's declared header on every arm. An interval
// declares text here (temporal r20; PostgreSQL: interval, OID 1186).
var ivTypes = map[string]string{
	"nullif/equal":       "a:text",
	"nullif/equalMonth":  "a:text",
	"nullif/unequal":     "a:text",
	"nullif/unequalText": "a:text",
	"nullif/nullSecond":  "a:text",
	"nullif/nullFirst":   "a:text",
	"nullif/cast":        "a:text,b:text",
	"nullif/unknown":     "a:text,b:text",
	"ext/greatestLeast":  "g:text,l:text",
	// Round 3: an interval aggregate argument is refused before a header
	// exists; a CASE / COALESCE interval declares text as the literal does;
	// EXTRACT is float-carried (ADR-0024 §2c: PostgreSQL's numeric, OID
	// 1700), date_part float8 on both.
	"agg/countTimes":        "ERR 0A000",
	"agg/countNeg":          "ERR 0A000",
	"agg/countDistinctNeg":  "ERR 0A000",
	"agg/countDistinctPlus": "ERR 0A000",
	"agg/countRowsTimes":    "ERR 0A000",
	"agg/countLit":          "ERR 0A000",
	"agg/countCast":         "ERR 0A000",
	"agg/maxLit":            "ERR 0A000",
	"agg/maxArith":          "ERR 0A000",
	"agg/maxCast":           "ERR 0A000",
	"agg/minLit":            "ERR 0A000",
	"agg/minArith":          "ERR 0A000",
	"agg/minCast":           "ERR 0A000",
	"agg/sumLit":            "ERR 0A000",
	"agg/sumArith":          "ERR 0A000",
	"agg/sumCast":           "ERR 0A000",
	"agg/avgLit":            "ERR 0A000",
	"agg/avgArith":          "ERR 0A000",
	"agg/avgCast":           "ERR 0A000",
	"choice/caseTimes":      "a:text",
	"choice/caseNeg":        "a:text",
	"choice/coalesceTimes":  "a:text",
	"choice/case":           "a:text",
	"choice/coalesceFirst":  "a:text",
	"obs/extractType":       "a:double precision,b:double precision",
	"obs/extractTsType":     "a:double precision,b:double precision",
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
	out = append(out, ivCellsR2()...)
	return append(out, ivCellsR3()...)
}

// ivCellsR3 is round 3's classes (#1713c r3): an interval as an AGGREGATE or
// WINDOW function argument (refused 0A000 at plan time: no interval
// accumulator or interval column type exists), interval arithmetic over a
// CASE or COALESCE, the observations on ARRAY elements, time_bucket,
// date_add and EXTRACT's type, and the rest of the store sweep (shapes that
// carry an interval value into a projection). typed cells carry PostgreSQL's
// wire type beside ours.
func ivCellsR3() []ivCell {
	var out []ivCell
	add := func(name, sql string) { out = append(out, ivCell{name: name, sql: sql}) }
	typed := func(name, sql string) { out = append(out, ivCell{name: name, sql: sql, typed: true}) }
	c := "CAST(s AS INTERVAL)"

	// Aggregate arguments.
	typed("agg/countTimes", "SELECT count(INTERVAL '1 day' * 2) AS a")
	typed("agg/countNeg", "SELECT count(-INTERVAL '1 day') AS a")
	typed("agg/countDistinctNeg", "SELECT count(DISTINCT -INTERVAL '1 day') AS a")
	typed("agg/countDistinctPlus", "SELECT count(DISTINCT INTERVAL '1 day' + INTERVAL '1 hour') AS a")
	typed("agg/countRowsTimes", "SELECT count("+c+" * 2) AS a FROM iv_s")
	add("agg/countGroupPlus", "SELECT id, count("+c+" + INTERVAL '1 hour') AS a FROM iv_s WHERE id IN (1, 3, 9) GROUP BY id ORDER BY id")
	typed("agg/countLit", "SELECT count(INTERVAL '1 day') AS a")
	typed("agg/countCast", "SELECT count("+c+") AS a FROM iv_s")
	for _, f := range []string{"max", "min", "sum", "avg"} {
		typed("agg/"+f+"Lit", "SELECT "+f+"(INTERVAL '1 day') AS a")
		typed("agg/"+f+"Arith", "SELECT "+f+"(INTERVAL '1 day' * 2) AS a")
		typed("agg/"+f+"Cast", "SELECT "+f+"("+c+") AS a FROM iv_s WHERE id IN (1, 6, 7, 12)")
	}
	add("agg/having", "SELECT count(*) AS n FROM iv_s HAVING max("+c+") > INTERVAL '1 day'")
	add("agg/caseArg", "SELECT max(CASE WHEN id = 1 THEN INTERVAL '1 day' END) AS a FROM iv_s")
	// Window function arguments.
	add("win/maxLit", "SELECT max(INTERVAL '1 day') OVER () AS a")
	add("win/countArith", "SELECT count(-INTERVAL '1 day') OVER () AS a")
	add("win/firstArith", "SELECT first_value(INTERVAL '1 day' * 2) OVER () AS a")
	add("win/lagCast", "SELECT id, lag("+c+") OVER (ORDER BY id) AS a FROM iv_s WHERE id IN (1, 2, 3) ORDER BY id")
	add("win/sumCast", "SELECT id, sum("+c+") OVER (ORDER BY id) AS a FROM iv_s WHERE id IN (1, 2, 3) ORDER BY id")
	// Not an interval argument: the window's PARTITION BY and a count(*)
	// controls stay answered.
	add("win/countStarControl", "SELECT count(*) OVER () AS a")
	add("agg/countCompareControl", "SELECT count("+c+" = INTERVAL '1 day') AS a FROM iv_s")
	add("agg/sumExtractControl", "SELECT sum(extract(day FROM "+c+")) AS a FROM iv_s")

	// Arithmetic over a CASE or COALESCE that answers an interval.
	typed("choice/caseTimes", "SELECT CASE WHEN true THEN INTERVAL '1 day' END * 2 AS a")
	typed("choice/caseNeg", "SELECT -CASE WHEN true THEN INTERVAL '1 day' END AS a")
	typed("choice/coalesceTimes", "SELECT COALESCE(INTERVAL '1 day', NULL) * 2 AS a")
	typed("choice/case", "SELECT CASE WHEN true THEN INTERVAL '1 day' END AS a")
	typed("choice/coalesceFirst", "SELECT COALESCE(INTERVAL '1 day', NULL) AS a")
	add("choice/casePlus", "SELECT CASE WHEN true THEN INTERVAL '1 day' END + INTERVAL '1 hour' AS a")
	add("choice/caseTs", "SELECT CAST(TIMESTAMP '2024-01-31 00:00:00' + CASE WHEN true THEN INTERVAL '1 month' END AS TEXT) AS a")
	add("choice/caseCompare", "SELECT CASE WHEN true THEN INTERVAL '1 day' END = INTERVAL '24 hours' AS a")

	// The reviewer's observations: answers recorded beside PostgreSQL's.
	add("obs/arrayTimes", "SELECT ARRAY[INTERVAL '1 day' * 2] AS a")
	add("obs/arrayNeg", "SELECT ARRAY[-INTERVAL '1 day'] AS a")
	add("obs/arrayLit", "SELECT ARRAY[INTERVAL '1 day'] AS a")
	add("obs/timeBucket", "SELECT time_bucket(INTERVAL '1 day' * 2, TIMESTAMP '2024-01-03 10:00:00') AS a")
	add("obs/timeBucketLit", "SELECT time_bucket(INTERVAL '2 days', TIMESTAMP '2024-01-03 10:00:00') AS a")
	add("obs/dateAdd", "SELECT CAST(date_add(DATE '2024-01-31', INTERVAL '1 day' * 2) AS TEXT) AS a")
	add("obs/dateAddLit", "SELECT CAST(date_add(DATE '2024-01-31', INTERVAL '2 days') AS TEXT) AS a")
	typed("obs/extractType", "SELECT extract(day FROM INTERVAL '30 days') AS a, date_part('day', INTERVAL '30 days') AS b")
	typed("obs/extractTsType", "SELECT extract(day FROM TIMESTAMP '2024-01-31 00:00:00') AS a, date_part('day', TIMESTAMP '2024-01-31 00:00:00') AS b")

	// The rest of the store sweep: an interval value carried into a
	// projection, a key, a container or a subquery's answer.
	add("sweep/scalarSubTimes", "SELECT (SELECT INTERVAL '1 day' * 2) AS a")
	add("sweep/derivedTimes", "SELECT x FROM (SELECT INTERVAL '1 day' * 2 AS x) t")
	add("sweep/cteNeg", "WITH t AS (SELECT -INTERVAL '1 day' AS x) SELECT x FROM t")
	add("sweep/unionAllTimes", "SELECT count(*) AS n FROM (SELECT INTERVAL '1 day' * 2 AS x UNION ALL SELECT INTERVAL '1 hour') t")
	add("sweep/valuesTimes", "SELECT x FROM (VALUES (INTERVAL '1 day' * 2)) v(x)")
	add("sweep/orderByTimes", "SELECT id FROM iv_s WHERE id IN (1, 6) ORDER BY "+c+" * 2, id")
	add("sweep/groupByTimes", "SELECT "+c+" * 2 AS k, count(*) AS n FROM iv_s WHERE id IN (1, 6) GROUP BY "+c+" * 2 ORDER BY n, k")
	add("sweep/distinctNeg", "SELECT DISTINCT -INTERVAL '1 day' AS a")
	add("sweep/concat", "SELECT 'x' || (INTERVAL '1 day' * 2) AS a")
	add("sweep/castText", "SELECT CAST(INTERVAL '1 day' * 2 AS TEXT) AS a")
	add("sweep/nullifTimes", "SELECT NULLIF(INTERVAL '1 day' * 2, INTERVAL '1 day') AS a")
	add("sweep/greatestNeg", "SELECT GREATEST(-INTERVAL '1 day', INTERVAL '1 hour') AS a")
	add("sweep/coalesceNeg", "SELECT COALESCE(-INTERVAL '1 day', INTERVAL '1 hour') AS a")
	add("sweep/caseThenTimes", "SELECT CASE WHEN true THEN INTERVAL '1 day' * 2 END AS a")
	add("sweep/rowsNeg", "SELECT id, -"+c+" AS n FROM iv_s WHERE id IN (1, 3, 9) ORDER BY id")
	add("sweep/whereTimes", "SELECT id FROM iv_s WHERE "+c+" * 2 = INTERVAL '2 days' ORDER BY id")
	add("sweep/inTimes", "SELECT INTERVAL '2 days' IN (INTERVAL '1 day' * 2, -INTERVAL '1 day') AS a")
	add("sweep/rowCtor", "SELECT ROW(INTERVAL '1 day' * 2, 1) AS a")
	// The edges of the rule: a quoted branch keeps a CASE / COALESCE off
	// it, and an aggregate over a DERIVED interval column reads that
	// column's text (r20), not an interval node.
	add("sweep/caseQuotedAgg", "SELECT max(CASE WHEN id = 1 THEN INTERVAL '1 day' ELSE '2 days' END) AS a FROM iv_s")
	add("sweep/caseQuotedTimes", "SELECT CASE WHEN true THEN INTERVAL '1 day' ELSE '2 days' END * 2 AS a")
	add("sweep/coalesceQuotedTimes", "SELECT COALESCE(NULL, INTERVAL '1 day', '2 days') * 2 AS a")
	add("sweep/caseQuoted", "SELECT CASE WHEN false THEN INTERVAL '1 day' ELSE '24 hours' END = INTERVAL '1 day' AS a, CASE WHEN false THEN INTERVAL '1 day' ELSE '2 days' END AS b")
	add("sweep/caseQuotedBad", "SELECT CASE WHEN true THEN INTERVAL '1 day' ELSE 'x' END AS a")
	add("sweep/coalesceQuotedBad", "SELECT COALESCE(INTERVAL '1 day', 'x') AS a")
	add("sweep/caseColumnControl", "SELECT id, CASE WHEN id = 1 THEN INTERVAL '1 day' ELSE CAST(s AS INTERVAL) END * 2 AS a FROM iv_s WHERE id IN (1, 2) ORDER BY id")
	add("sweep/derivedCount", "SELECT count(x) AS a FROM (SELECT INTERVAL '1 day' * 2 AS x) t")
	add("sweep/derivedMax", "SELECT max(x) AS a FROM (SELECT -INTERVAL '1 day' AS x) t")
	add("sweep/derivedSum", "SELECT sum(x) AS a FROM (SELECT CAST(s AS INTERVAL) AS x FROM iv_s) t")
	return out
}

// ivCellsR2 is round 2's classes (#1713c r2): NULLIF / GREATEST / LEAST over
// intervals, an interval beside an UNTYPED literal, the interval input
// range, the arithmetic (* / + - unary minus, the operators that do not
// exist), timestamp ± interval over month ends, and EXTRACT / date_part of
// every field. typed cells carry the declared type of every column beside
// the value (PostgreSQL's from the wire OID).
func ivCellsR2() []ivCell {
	var out []ivCell
	add := func(name, sql string) { out = append(out, ivCell{name: name, sql: sql}) }
	typed := func(name, sql string) { out = append(out, ivCell{name: name, sql: sql, typed: true}) }

	// NULLIF over two intervals: PostgreSQL's value and wire type beside ours.
	typed("nullif/equal", "SELECT NULLIF(INTERVAL '1 day', INTERVAL '24 hours') AS a")
	typed("nullif/equalMonth", "SELECT NULLIF(INTERVAL '1 month', INTERVAL '30 days') AS a")
	typed("nullif/unequal", "SELECT NULLIF(INTERVAL '1 day', INTERVAL '2 days') AS a")
	typed("nullif/unequalText", "SELECT NULLIF(INTERVAL '2 days', INTERVAL '10 days') AS a")
	typed("nullif/nullSecond", "SELECT NULLIF(INTERVAL '1 day', NULL) AS a")
	typed("nullif/nullFirst", "SELECT NULLIF(CAST(NULL AS INTERVAL), INTERVAL '1 day') AS a")
	typed("nullif/cast", "SELECT NULLIF(CAST('1 day' AS INTERVAL), CAST('24 hours' AS INTERVAL)) AS a, NULLIF(CAST('1 day' AS INTERVAL), CAST('25 hours' AS INTERVAL)) AS b")
	typed("nullif/unknown", "SELECT NULLIF(INTERVAL '1 day', '24 hours') AS a, NULLIF(INTERVAL '1 day', '2 days') AS b")
	add("nullif/rows", "SELECT id, NULLIF(CAST(s AS INTERVAL), INTERVAL '1 day') AS n FROM iv_s WHERE id IN (1, 2, 3, 6, 9) ORDER BY id")
	add("nullif/where", "SELECT id FROM iv_s WHERE NULLIF(CAST(s AS INTERVAL), INTERVAL '24 hours') IS NULL ORDER BY id")

	// GREATEST / LEAST over intervals.
	typed("ext/greatestLeast", "SELECT GREATEST(INTERVAL '2 days', INTERVAL '10 days') AS g, LEAST(INTERVAL '2 days', INTERVAL '10 days') AS l")
	add("ext/month", "SELECT GREATEST(INTERVAL '1 month', INTERVAL '29 days') AS g, LEAST(INTERVAL '1 month', INTERVAL '31 days') AS l")
	add("ext/null", "SELECT GREATEST(INTERVAL '1 day', NULL, INTERVAL '25 hours') AS g, LEAST(NULL, INTERVAL '-1 day', INTERVAL '1 hour') AS l")
	add("ext/three", "SELECT GREATEST(INTERVAL '23 hours', INTERVAL '1 day', INTERVAL '1439 minutes') AS g, LEAST(INTERVAL '1 year', INTERVAL '364 days', INTERVAL '13 months') AS l")
	add("ext/unknown", "SELECT GREATEST(INTERVAL '2 days', '10 days') AS g, LEAST(INTERVAL '2 days', '10 days') AS l")
	add("ext/unknownBad", "SELECT GREATEST(INTERVAL '1 day', '2024-01-01') AS g")
	add("ext/rows", "SELECT id, GREATEST(CAST(s AS INTERVAL), INTERVAL '2 days') AS g FROM iv_s WHERE id IN (1, 6, 7, 9, 12) ORDER BY id")
	add("ext/coalesce", "SELECT COALESCE(NULL, INTERVAL '1 day') AS a")
	add("ext/coalesceUnknown", "SELECT COALESCE(NULL, INTERVAL '1 day', '2 days') AS a")

	// An interval beside an UNTYPED literal: PostgreSQL resolves the
	// unknown to interval.
	for _, p := range [][2]string{{"1 day", "24 hours"}, {"2 days", "10 days"}, {"1 month", "30 days"}, {"1 day", "25 hours"}} {
		k := p[0] + "~" + p[1]
		add("unk/right/"+k, ivCmpSQL(ivSpell(p[0]), "'"+p[1]+"'"))
		add("unk/left/"+k, ivCmpSQL("'"+p[0]+"'", ivSpell(p[1])))
		add("unk/cast/"+k, ivCmpSQL(ivCast(p[0]), "'"+p[1]+"'"))
	}
	add("unk/in", "SELECT INTERVAL '1 day' IN ('24 hours', '2 days') AS a, '24 hours' IN (INTERVAL '1 day') AS b, INTERVAL '1 day' NOT IN ('24 hours') AS c, INTERVAL '2 days' IN ('10 days', NULL) AS d")
	add("unk/between", "SELECT INTERVAL '3 days' BETWEEN '2 days' AND '10 days' AS a, '3 days' BETWEEN INTERVAL '2 days' AND INTERVAL '10 days' AS b, INTERVAL '25 hours' BETWEEN '1 day' AND '2 days' AS c, INTERVAL '1 month' NOT BETWEEN '29 days' AND '31 days' AS d")
	add("unk/case", "SELECT CASE INTERVAL '1 day' WHEN '24 hours' THEN 1 ELSE 0 END AS a, CASE INTERVAL '2 days' WHEN '10 days' THEN 1 WHEN '48 hours' THEN 2 ELSE 0 END AS b")
	add("unk/caseSubject", "SELECT CASE '24 hours' WHEN INTERVAL '1 day' THEN 1 ELSE 0 END AS a")
	add("unk/bad", "SELECT INTERVAL '1 day' = 'abc' AS a")
	add("unk/row", "SELECT id FROM iv_s WHERE CAST(s AS INTERVAL) = '24 hours' ORDER BY id")
	add("unk/rowLt", "SELECT id FROM iv_s WHERE CAST(s AS INTERVAL) < '3 days' ORDER BY id")
	add("unk/arith", "SELECT INTERVAL '1 day' * 2 = '48 hours' AS a, -INTERVAL '1 day' < '0 days' AS b")
	add("unk/number", "SELECT INTERVAL '1 day' = 1 AS a")

	// The interval input range, at the literal (PostgreSQL's interval_in).
	for _, r := range []struct{ k, v string }{
		{"yearsOk", "178956970 years"}, {"yearsOver", "178956971 years"}, {"yearsNegOver", "-178956971 years"},
		{"yearsPast32", "2147483648 years"}, {"years32", "2147483647 years"},
		{"monthsOk", "2147483647 months"}, {"monthsOver", "2147483648 months"}, {"monthsNegOk", "-2147483648 months"},
		{"daysOk", "2147483647 days"}, {"daysOver", "2147483648 days"},
		{"weeksOk", "306783378 weeks"}, {"weeksOver", "306783379 weeks"},
		{"hoursOk", "2562047788 hours"}, {"hoursOver", "2562047789 hours"}, {"hoursNegOver", "-2562047789 hours"},
		{"minutesOver", "153722867281 minutes"}, {"secondsOk", "9223372036854 seconds"}, {"secondsOver", "9223372036855 seconds"},
		{"past64", "99999999999999999999 days"},
	} {
		add("range/"+r.k, "SELECT "+ivSpell(r.v)+" AS a")
	}
	add("range/sqlYear", "SELECT INTERVAL '178956971' YEAR AS a")
	add("range/sqlDay", "SELECT INTERVAL '2147483648' DAY AS a")
	add("range/castYears", "SELECT CAST('178956971 years' AS INTERVAL) AS a")
	add("range/castDays", "SELECT CAST('2147483648 days' AS INTERVAL) AS a")
	add("range/compare", "SELECT INTERVAL '2147483647 years' > INTERVAL '1 day' AS a")
	add("range/tsShift", "SELECT CAST(TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '100000000000 hours' AS TEXT) AS a")

	// The representation class, pinned with the others: an interval after a
	// projection is text.
	add("key/unionAllMax", "SELECT max(x) AS a FROM (SELECT INTERVAL '2 days' AS x UNION ALL SELECT INTERVAL '10 days') t")

	// Arithmetic.
	add("ar/mulInt", "SELECT INTERVAL '1 day' * 2 AS a, INTERVAL '1 day' * 0 AS b, INTERVAL '1 day' * -3 AS c, 3 * INTERVAL '2 hours' AS d")
	add("ar/mulMonths", "SELECT INTERVAL '1 month' * 2 AS a, INTERVAL '1 year' * 3 AS b, INTERVAL '1 month' * 1.5 AS c, INTERVAL '1 year' * 0.5 AS d")
	add("ar/mulFrac", "SELECT INTERVAL '1 month' * 0.1 AS a, INTERVAL '1 day' * 0.1 AS b, INTERVAL '1 day' * 1.5 AS c, INTERVAL '1 hour' * 2.5 AS d")
	add("ar/mulCascade", "SELECT INTERVAL '7 months' * 0.3 AS a, INTERVAL '1 month' * 0.7 AS b, INTERVAL '5 days' * 0.3 AS c, INTERVAL '1 year' * 1.1 AS d")
	add("ar/mulTypes", "SELECT INTERVAL '1 day' * CAST(2 AS BIGINT) AS a, INTERVAL '1 day' * CAST(2.5 AS DOUBLE PRECISION) AS b, INTERVAL '1 day' * CAST(2 AS REAL) AS c, INTERVAL '1 day' * '2' AS d")
	add("ar/mulNeg", "SELECT INTERVAL '-1 month' * 1.5 AS a, INTERVAL '1 month' * -0.5 AS b, INTERVAL '-1 day' * 0.5 AS c, INTERVAL '25 hours' * 2 AS d")
	add("ar/mulRounding", "SELECT INTERVAL '1 month' * 0.0333333333 AS a, INTERVAL '23 hours' * 2 AS b, INTERVAL '30 days' * 0.5 AS c")
	add("ar/mulSubSecond", "SELECT INTERVAL '1 second' * 1.5 AS a")
	add("ar/mulSubSecondCascade", "SELECT INTERVAL '1 month' * 1.0000001 AS a")
	add("ar/mulOverflow", "SELECT INTERVAL '2147483647 days' * 2 AS a")
	add("ar/mulHuge", "SELECT INTERVAL '1 day' * 1e300 AS a")
	add("ar/mulNaN", "SELECT INTERVAL '1 day' * CAST('NaN' AS DOUBLE PRECISION) AS a")
	add("ar/mulInf", "SELECT INTERVAL '1 day' * CAST('Infinity' AS DOUBLE PRECISION) AS a")
	add("ar/mulBadText", "SELECT INTERVAL '1 day' * 'abc' AS a")
	add("ar/mulNull", "SELECT INTERVAL '1 day' * NULL AS a, CAST(NULL AS INTERVAL) * 2 AS b, INTERVAL '1 day' + CAST(NULL AS INTERVAL) AS c")
	add("ar/divWhole", "SELECT INTERVAL '1 day' / 2 AS a, INTERVAL '1 month' / 2 AS b, INTERVAL '1 year' / 4 AS c, INTERVAL '2 hours' / 4 AS d")
	add("ar/divCascade", "SELECT INTERVAL '1 month' / 3 AS a, INTERVAL '1 day' / 3 AS b, INTERVAL '3 months' / 2 AS c, INTERVAL '3 days' / 2 AS d")
	add("ar/divFrac", "SELECT INTERVAL '1 day' / 1.5 AS a, INTERVAL '3 days' / -2 AS b, INTERVAL '1 day' / 0.5 AS c, INTERVAL '10 months' / 4 AS d")
	add("ar/divMore", "SELECT INTERVAL '1 year' / 12 AS a, INTERVAL '1 year' / 24 AS b, INTERVAL '-1 day' / 1.5 AS c, INTERVAL '1 week' / 7 AS d")
	add("ar/divText", "SELECT INTERVAL '1 day' / '2' AS a")
	add("ar/divZero", "SELECT INTERVAL '1 day' / 0 AS a")
	add("ar/divSubSecond", "SELECT INTERVAL '1 second' / 2 AS a")
	add("ar/divSubSecondCascade", "SELECT INTERVAL '1 year' / 7 AS a")
	add("ar/divTiny", "SELECT INTERVAL '1 day' / 1e-300 AS a")
	add("ar/plus", "SELECT INTERVAL '1 day' + INTERVAL '1 hour' AS a, INTERVAL '1 month' + INTERVAL '1 day' AS b, INTERVAL '1 year' + INTERVAL '1 month' AS c, INTERVAL '1 hour' + INTERVAL '30 minutes' AS d")
	add("ar/minus", "SELECT INTERVAL '1 day' - INTERVAL '1 hour' AS a, INTERVAL '1 month' - INTERVAL '30 days' AS b, INTERVAL '1 hour' - INTERVAL '2 hours' AS c, INTERVAL '-1 day' - INTERVAL '1 month' AS d")
	add("ar/plusZero", "SELECT INTERVAL '1 day' + INTERVAL '-1 day' AS a, INTERVAL '25 hours' + INTERVAL '1 day' AS b")
	add("ar/plusOverflow", "SELECT INTERVAL '2147483647 days' + INTERVAL '1 day' AS a")
	add("ar/plusOverflowMonths", "SELECT INTERVAL '178956970 years' + INTERVAL '1 year' AS a")
	add("ar/plusUnknown", "SELECT INTERVAL '1 day' + '1 hour' AS a, '1 hour' + INTERVAL '1 day' AS b, INTERVAL '1 day' - '1 hour' AS c, '2 days' - INTERVAL '1 day' AS d")
	add("ar/plusUnknownDate", "SELECT '2024-01-01' + INTERVAL '1 day' AS a")
	add("ar/plusCast", "SELECT CAST('1 day' AS INTERVAL) * 2 AS a, CAST('3 days' AS INTERVAL) - CAST('1 day' AS INTERVAL) AS b")
	add("ar/plusCompare", "SELECT (INTERVAL '1 day' * 2) = INTERVAL '48 hours' AS a, (INTERVAL '1 day' + INTERVAL '1 hour') > INTERVAL '1 day' AS b")
	add("ar/neg", "SELECT -INTERVAL '1 day' AS a, -INTERVAL '-2 hours' AS b, -INTERVAL '1 year' AS c, -INTERVAL '0 days' AS d")
	add("ar/negSum", "SELECT -(INTERVAL '1 day' + INTERVAL '1 hour') AS a")
	add("ar/negOverflow", "SELECT -INTERVAL '-2147483648 days' AS a")
	add("ar/unaryPlus", "SELECT +INTERVAL '1 day' AS a")
	add("ar/plusNumber", "SELECT INTERVAL '1 day' + 1 AS a")
	add("ar/numberMinus", "SELECT INTERVAL '1 day' - 1 AS a")
	add("ar/numberPlus", "SELECT 1 + INTERVAL '1 day' AS a")
	add("ar/timesInterval", "SELECT INTERVAL '1 day' * INTERVAL '1 day' AS a")
	add("ar/numberDiv", "SELECT 2 / INTERVAL '1 day' AS a")
	add("ar/mod", "SELECT INTERVAL '1 day' % 2 AS a")
	add("ar/timesBool", "SELECT INTERVAL '1 day' * TRUE AS a")
	add("ar/opaque", "SELECT CAST('1 day 02:00:00' AS INTERVAL) * 2 AS a")
	add("ar/opaqueNeg", "SELECT -CAST('1 day 02:00:00' AS INTERVAL) AS a")
	// A text COLUMN is not SQL's unknown: PostgreSQL has no interval ± text
	// nor interval * text.
	add("ar/textColPlus", "SELECT id, INTERVAL '1 day' + s AS a FROM iv_s WHERE id IN (1, 6) ORDER BY id")
	add("ar/textColTimes", "SELECT id, INTERVAL '1 day' * s AS a FROM iv_s WHERE id = 1")
	add("ar/rows", "SELECT id, CAST(s AS INTERVAL) * 2 AS d, CAST(s AS INTERVAL) + INTERVAL '1 hour' AS p, -CAST(s AS INTERVAL) AS n FROM iv_s WHERE id IN (1, 2, 3, 8, 9) ORDER BY id")
	add("ar/tsMul", "SELECT CAST(TIMESTAMP '2024-01-31 00:00:00' + (INTERVAL '1 day' * 2) AS TEXT) AS a")
	add("ar/tsMulBare", "SELECT CAST(TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '1 day' * 2 AS TEXT) AS a")
	add("ar/tsSum", "SELECT CAST(TIMESTAMP '2024-01-31 00:00:00' + (INTERVAL '1 month' + INTERVAL '1 day') AS TEXT) AS a")
	add("ar/tsNeg", "SELECT CAST(TIMESTAMP '2024-01-31 00:00:00' - (-INTERVAL '1 month') AS TEXT) AS a")

	// timestamp ± interval: a month is a calendar month, its day clamped.
	ts := func(from, op, iv string) string {
		return "CAST(TIMESTAMP '" + from + "' " + op + " INTERVAL '" + iv + "' AS TEXT)"
	}
	add("tsm/plusLeap", "SELECT "+ts("2024-01-31 00:00:00", "+", "1 month")+" AS a, "+ts("2023-01-31 00:00:00", "+", "1 month")+" AS b, "+ts("2024-03-31 10:00:00", "-", "1 month")+" AS c, "+ts("2023-03-31 00:00:00", "-", "1 month")+" AS d")
	add("tsm/years", "SELECT "+ts("2024-02-29 00:00:00", "+", "1 year")+" AS a, "+ts("2024-02-29 00:00:00", "-", "1 year")+" AS b, "+ts("2024-02-29 00:00:00", "+", "4 years")+" AS c, "+ts("2024-01-31 00:00:00", "+", "13 months")+" AS d")
	add("tsm/century", "SELECT "+ts("2023-02-28 00:00:00", "+", "1 year")+" AS a, "+ts("2020-02-29 00:00:00", "+", "100 years")+" AS b, "+ts("2000-02-29 00:00:00", "+", "100 years")+" AS c, "+ts("2100-01-29 00:00:00", "+", "1 month")+" AS d")
	add("tsm/months", "SELECT "+ts("2024-12-31 00:00:00", "+", "2 months")+" AS a, "+ts("2024-08-31 00:00:00", "+", "1 month")+" AS b, "+ts("2000-01-30 00:00:00", "+", "1 month")+" AS c, "+ts("2024-01-31 10:00:00", "+", "-1 month")+" AS d")
	add("tsm/negative", "SELECT "+ts("2024-03-31 00:00:00", "+", "-13 months")+" AS a, "+ts("2024-01-31 00:00:00", "-", "-1 month")+" AS b, "+ts("2024-05-31 00:00:00", "-", "3 months")+" AS c, "+ts("2024-03-30 00:00:00", "-", "1 month")+" AS d")
	add("tsm/chain", "SELECT CAST(TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '1 month' - INTERVAL '1 month' AS TEXT) AS a, CAST(TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '1 month' + INTERVAL '1 day' AS TEXT) AS b, CAST(TIMESTAMP '2024-01-31 23:00:00' + INTERVAL '1 month' + INTERVAL '2 hours' AS TEXT) AS c")
	add("tsm/date", "SELECT CAST(DATE '2024-01-31' + INTERVAL '1 month' AS TEXT) AS a, CAST(DATE '2024-05-31' - INTERVAL '1 month' AS TEXT) AS b, CAST(INTERVAL '1 month' + DATE '2024-01-31' AS TEXT) AS c, CAST(DATE '2024-03-31' + INTERVAL '-1 year' AS TEXT) AS d")
	add("tsm/castText", "SELECT CAST(TIMESTAMP '2024-01-31 00:00:00' + CAST('1 month' AS INTERVAL) AS TEXT) AS a, CAST(TIMESTAMP '2024-01-31 00:00:00' + '1 month' AS TEXT) AS b")
	add("tsm/days", "SELECT CAST(TIMESTAMP '2024-02-28 12:00:00' + INTERVAL '1 day' AS TEXT) AS a, CAST(TIMESTAMP '2023-02-28 12:00:00' + INTERVAL '1 day' AS TEXT) AS b, CAST(TIMESTAMP '2024-03-01 00:00:00' - INTERVAL '25 hours' AS TEXT) AS c")

	// EXTRACT (numeric on PostgreSQL, float-carried here: ADR-0024 §2c) and
	// date_part (float8 on both) of an interval, every field.
	add("ex/fields", "SELECT extract(day FROM INTERVAL '30 days') AS a, extract(hour FROM INTERVAL '25 hours') AS b, extract(month FROM INTERVAL '14 months') AS c, extract(year FROM INTERVAL '14 months') AS d")
	add("ex/minuteQuarter", "SELECT extract(minute FROM INTERVAL '90 minutes') AS a, extract(quarter FROM INTERVAL '5 months') AS b, extract(minute FROM INTERVAL '25 hours') AS c, extract(day FROM INTERVAL '1 month') AS d")
	add("ex/negative", "SELECT extract(day FROM INTERVAL '-30 days') AS a, extract(hour FROM INTERVAL '-25 hours') AS b, extract(month FROM INTERVAL '-14 months') AS c, extract(year FROM INTERVAL '-14 months') AS d")
	add("ex/quarter", "SELECT extract(quarter FROM INTERVAL '-5 months') AS a, extract(quarter FROM INTERVAL '0 days') AS b, extract(quarter FROM INTERVAL '11 months') AS c, extract(quarter FROM INTERVAL '-12 months') AS d")
	add("ex/zero", "SELECT extract(year FROM INTERVAL '1 day') AS a, extract(month FROM INTERVAL '1 day') AS b, extract(hour FROM INTERVAL '2 days') AS c, extract(minute FROM INTERVAL '2 hours') AS d")
	add("ex/second", "SELECT extract(second FROM INTERVAL '90 seconds') AS a, extract(second FROM INTERVAL '-90 seconds') AS b")
	add("ex/epoch", "SELECT extract(epoch FROM INTERVAL '1 day') AS a, extract(epoch FROM INTERVAL '1 month') AS b, extract(epoch FROM INTERVAL '1 year') AS c, extract(epoch FROM INTERVAL '-1 year') AS d")
	add("ex/secondTimestamp", "SELECT extract(second FROM TIMESTAMP '2024-01-01 10:00:05') AS a")
	add("ex/week", "SELECT extract(week FROM INTERVAL '30 days') AS a")
	add("ex/dow", "SELECT extract(dow FROM INTERVAL '30 days') AS a")
	add("ex/doy", "SELECT extract(doy FROM INTERVAL '30 days') AS a")
	add("ex/millisecond", "SELECT extract(millisecond FROM INTERVAL '5 seconds') AS a")
	add("ex/decade", "SELECT extract(decade FROM INTERVAL '25 years') AS a")
	add("ex/cast", "SELECT extract(day FROM CAST('30 days' AS INTERVAL)) AS a, extract(day FROM (INTERVAL '1 day' * 3)) AS b")
	add("ex/opaque", "SELECT extract(day FROM CAST('1 day 02:00:00' AS INTERVAL)) AS a")
	add("ex/rows", "SELECT id, extract(day FROM CAST(s AS INTERVAL)) AS d, extract(hour FROM CAST(s AS INTERVAL)) AS h, date_part('epoch', CAST(s AS INTERVAL)) AS e FROM iv_s WHERE id IN (1, 2, 3, 8, 9, 10) ORDER BY id")
	add("dp/fields", "SELECT date_part('day', INTERVAL '30 days') AS a, date_part('second', INTERVAL '90 seconds') AS b, date_part('epoch', INTERVAL '1 month') AS c, date_part('hour', INTERVAL '1 day') AS d")
	add("dp/small", "SELECT date_part('epoch', INTERVAL '1 year') AS a, date_part('millisecond', INTERVAL '5 seconds') AS b, date_part('microsecond', INTERVAL '5 seconds') AS c, date_part('quarter', INTERVAL '5 months') AS d")
	add("dp/large", "SELECT date_part('decade', INTERVAL '25 years') AS a, date_part('century', INTERVAL '250 years') AS b, date_part('millennium', INTERVAL '2500 years') AS c, date_part('year', INTERVAL '-14 months') AS d")
	add("dp/negative", "SELECT date_part('minute', INTERVAL '-90 minutes') AS a, date_part('second', INTERVAL '-90 seconds') AS b, date_part('hour', INTERVAL '-90 minutes') AS c, date_part('epoch', INTERVAL '14 months') AS d")
	add("dp/abbrev", "SELECT date_part('mon', INTERVAL '14 months') AS a, date_part('hrs', INTERVAL '25 hours') AS b, date_part('y', INTERVAL '14 months') AS c, date_part('epoch', INTERVAL '178956970 years') AS d")
	add("dp/week", "SELECT date_part('week', INTERVAL '30 days') AS a")
	add("dp/dow", "SELECT date_part('dow', INTERVAL '30 days') AS a")
	add("dp/bogus", "SELECT date_part('bogus', INTERVAL '30 days') AS a")
	add("dp/null", "SELECT date_part('day', CAST(NULL AS INTERVAL)) AS a")
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
				if c.typed {
					header[i] += ":" + ivPGTypeName(fd.DataTypeOID)
				}
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

// ivKnown is the pinned answer of a cell that does not give PostgreSQL's
// answer: one outside this line's seam, identical at base 2fa0beeb and at
// the fix, or a refusal the fix put where a silent wrong answer was (the
// round-2 arithmetic cells below). A key "cell@arm" overrides "cell" on that
// arm. A pin FAILS the moment the arm starts agreeing or changes in any other
// way — delete or re-pin it then.
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
//   - arith/justifyDays: justify_days does not exist (42883).
//   - key/unionAllMax: the representation again — a UNION ALL arm's
//     interval is text, and max reads "2 days" > "10 days".
//   - unk/caseSubject: PostgreSQL resolves a
//     quoted CASE subject to text and refuses `text = interval`; here it
//     compares text. unk/number: `interval = integer` is 42883 there and f
//     here. ar/unaryPlus: `+interval` is 42883 there and the interval here.
//     ar/plusUnknownDate: `'2024-01-01' + INTERVAL '1 day'` reads the
//     quoted operand as a date here (22007 there: interval input).
//     ar/textColPlus: `interval + text_column` reads the column as a date
//     here (temporal arc VL's text-date concession; NULL for '1 day') where
//     PostgreSQL has no `interval + text` (42883). All base-identical.
//   - ar/*SubSecond*, ar/mulInf: the result needs a sub-second clock part or
//     PostgreSQL 17's infinite interval, which IntervalValue (whole seconds,
//     finite) cannot hold: refused 0A000 (the fix; they boxed 0 before).
//     ar/opaque*, ex/opaque: interval input the single-unit fields cannot
//     hold (IntervalValue.text) is refused 0A000 when applied, as addInterval
//     already refused it (they boxed 0 / answered NULL before).
//   - ex/second, ex/epoch, ex/secondTimestamp: EXTRACT is numeric on
//     PostgreSQL (`30.000000`) and float-carried here (ADR-0024 §2c, the
//     timestamp's EXTRACT the same): the same number, printed as the double.
//     ex/millisecond, ex/decade: EXTRACT is rewritten to a field function and
//     those fields have none (42883, for a timestamp too); date_part answers
//     them (dp/*).
//   - agg/*, win/* (round 3), key/countDistinct, order/minMax: an interval
//     as an aggregate's or a window function's argument is refused 0A000 at
//     plan time (physical.refuseIntervalAggregateArgument): no interval
//     accumulator and no interval column type. The count cells' answers
//     were right before and the statement is refused all the same — one
//     rule for the argument, whatever the aggregate (the architect's
//     ruling); min / max / sum / avg answered 0, NULL or the text order,
//     and since round 2's arithmetic the argument met the store guard.
//   - obs/array*: an ARRAY of INTERVAL is a DURATION array (ADR-0045 §1),
//     printed here as its nanoseconds (the wire refuses it, #1268); round 2
//     made `ARRAY[INTERVAL '1 day' * 2]` and `ARRAY[-INTERVAL '1 day']`
//     the same DURATION array the bare literal already was. obs/timeBucket*:
//     PostgreSQL has no time_bucket (42883); it is this engine's extension,
//     and the arithmetic width now answers as the literal width did.
//     obs/dateAdd*: date_add is timestamptz on PostgreSQL ("+00") and a
//     timestamp here, the literal width and the arithmetic width alike.
//   - sweep/rowCtor: the ROW(...) constructor spelling is a syntax error
//     here (42601), every type alike. sweep/caseQuotedAgg: the CASE answers
//     an interval (its quoted branch is interval input), so it is an
//     interval aggregate argument, refused as agg/*. sweep/derivedSum: a
//     DERIVED interval column is its text (r20), and sum(text) is 42883 —
//     base-identical; an aggregate over a derived interval column is not an
//     interval node and keeps its base answer (key/unionAllMax, sweep/derived*).
//     sweep/caseQuotedBad, sweep/coalesceQuotedBad: PostgreSQL reads every
//     quoted constant as interval input when it resolves the statement
//     (22007 for 'x'); here a branch is cast only when it is reached, so the
//     unreached 'x' is never read (base-identical).
var ivKnown = map[string]string{
	"box/subsecond":              "ERR 22007",
	"box/multifield":             "a,b,c | f,f,t",
	"key/groupBy":                "k,n | -1 days,1; 00:00:00,1; 1 day,1; 1 mon,1; 1 year,1; 10 days,1; 2 days,1; 24:00:00,1; 25:00:00,1; 30 days,1; 360 days,1; 720:00:00,1; NULL,1; 7 days,2",
	"key/groupCount":             "g | 14",
	"key/distinctCount":          "g | 14",
	"key/countDistinct":          "ERR 0A000",
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
	"order/minMax":               "ERR 0A000",
	"order/values":               "x | 10 days; 2 days; 25:00:00",
	"arith/justifyDays":          "ERR 42883",
	"key/unionAllMax":            "a | 2 days",
	"unk/caseSubject":            "a | 0",
	"unk/number":                 "a | f",
	"ar/unaryPlus":               "a | 1 day",
	"ar/plusUnknownDate":         "a | 1704153600000",
	"ar/textColPlus":             "id,a | 1,NULL; 6,NULL",
	"ar/mulSubSecond":            "ERR 0A000",
	"ar/mulSubSecondCascade":     "ERR 0A000",
	"ar/mulInf":                  "ERR 0A000",
	"ar/divSubSecond":            "ERR 0A000",
	"ar/divSubSecondCascade":     "ERR 0A000",
	"ar/opaque":                  "ERR 0A000",
	"ar/opaqueNeg":               "ERR 0A000",
	"ex/opaque":                  "ERR 0A000",
	"ex/second":                  "a,b | 30,-30",
	"ex/epoch":                   "a,b,c,d | 86400,2592000,31557600,-31557600",
	"ex/secondTimestamp":         "a | 5",
	"ex/millisecond":             "ERR 42883",
	"ex/decade":                  "ERR 42883",
	"agg/countTimes":             "ERR 0A000",
	"agg/countNeg":               "ERR 0A000",
	"agg/countDistinctNeg":       "ERR 0A000",
	"agg/countDistinctPlus":      "ERR 0A000",
	"agg/countRowsTimes":         "ERR 0A000",
	"agg/countGroupPlus":         "ERR 0A000",
	"agg/countLit":               "ERR 0A000",
	"agg/countCast":              "ERR 0A000",
	"agg/maxLit":                 "ERR 0A000",
	"agg/maxArith":               "ERR 0A000",
	"agg/maxCast":                "ERR 0A000",
	"agg/minLit":                 "ERR 0A000",
	"agg/minArith":               "ERR 0A000",
	"agg/minCast":                "ERR 0A000",
	"agg/sumLit":                 "ERR 0A000",
	"agg/sumArith":               "ERR 0A000",
	"agg/sumCast":                "ERR 0A000",
	"agg/avgLit":                 "ERR 0A000",
	"agg/avgArith":               "ERR 0A000",
	"agg/avgCast":                "ERR 0A000",
	"agg/having":                 "ERR 0A000",
	"agg/caseArg":                "ERR 0A000",
	"win/maxLit":                 "ERR 0A000",
	"win/countArith":             "ERR 0A000",
	"win/firstArith":             "ERR 0A000",
	"win/lagCast":                "ERR 0A000",
	"win/sumCast":                "ERR 0A000",
	"obs/arrayTimes":             "a | [172800000000000]",
	"obs/arrayNeg":               "a | [-86400000000000]",
	"obs/arrayLit":               "a | [86400000000000]",
	"obs/timeBucket":             "a | 1704153600000",
	"obs/timeBucketLit":          "a | 1704153600000",
	"obs/dateAdd":                "a | 2024-02-02 00:00:00",
	"obs/dateAddLit":             "a | 2024-02-02 00:00:00",
	"sweep/rowCtor":              "ERR 42601",
	"sweep/caseQuotedAgg":        "ERR 0A000",
	"sweep/derivedSum":           "ERR 42883",
	"sweep/caseQuotedBad":        "a | 1 day",
	"sweep/coalesceQuotedBad":    "a | 1 day",
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
		wantTypes := ""
		if c.typed {
			// The declared types ride beside the value: PostgreSQL's from
			// the wire OID, ours from the result's column declaration, which
			// is what pgwire publishes. Compared against ivTypes, never
			// against PostgreSQL's (temporal r20: an interval declares text).
			wantTypes = ivHeaderTypes(want)
			want = ivNormalize(want)
		}
		for _, arm := range arms {
			raw := arm.run(c.sql)
			got := ivNormalize(raw)
			dump.WriteString(c.name + "\t" + arm.name + "\t" + got + "\n")
			if c.typed {
				gotTypes := ivHeaderTypes(raw)
				dump.WriteString(c.name + "\t" + arm.name + "\ttypes\t" + gotTypes + "\tPostgreSQL\t" + wantTypes + "\n")
				if gotTypes != ivTypes[c.name] {
					t.Errorf("%s on %s: declared %q, pinned %q (PostgreSQL 17.11 %q)", c.name, arm.name, gotTypes, ivTypes[c.name], wantTypes)
				}
			}
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
	wantPinned := 0
	for _, c := range cells {
		for _, arm := range arms {
			if !c.ddl && ivPin(c.name, arm.name) != "" {
				wantPinned++
			}
		}
	}
	for k := range ivKnown {
		name, _, _ := strings.Cut(k, "@")
		if _, ok := answers[name]; !ok {
			t.Errorf("pin %s names no cell", k)
		}
	}
	if equalSpellings < 15 || pinned != wantPinned {
		t.Fatalf("%d equal-but-differently-spelled pairs, %d pinned (cell, arm) answers: the table must discriminate and every pin must be read", equalSpellings, pinned)
	}
}

// ivStoreGuard is the batch layer's silent-write guard (batch.TypeMismatch):
// a value the plan did not declare reached a typed vector. It is a backstop,
// never an answer — the plan must declare what the expression produces or
// refuse the statement with a SQLSTATE first.
const ivStoreGuard = "cannot store"

// TestArc1713cNoStoreGuardEveryArm drives every statement of the interval
// table (#1713c), the round-3 store sweep included, on every arm and fails
// if any error's text is the store guard's: an interval value the plan
// declared as something else (float8 for an aggregate's argument, a CASE, a
// COALESCE) must be declared or refused 0A000 at plan time, identically on
// every arm. IV_ERRDUMP=<path> writes every (cell, arm, error text).
func TestArc1713cNoStoreGuardEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: six arms over the interval table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	writer, arms := ivArmsFixture(t, ctx)
	var dump strings.Builder
	defer func() {
		if p := os.Getenv("IV_ERRDUMP"); p != "" {
			if err := os.WriteFile(p, []byte(dump.String()), 0o644); err != nil {
				t.Error(err)
			}
		}
	}()
	ran := 0
	for _, c := range ivCells() {
		if c.ddl {
			if got := writer(c.sql); got != "ok" {
				t.Fatalf("%s: %s", c.name, got)
			}
			continue
		}
		for _, arm := range arms {
			_, errText := arm.exec(c.sql)
			ran++
			if errText != "" {
				dump.WriteString(c.name + "\t" + arm.name + "\t" + errText + "\n")
			}
			if strings.Contains(errText, ivStoreGuard) {
				t.Errorf("%s on %s reached the store guard\n  %s\n  %s", c.name, arm.name, c.sql, errText)
			}
		}
	}
	if ran < 6*300 {
		t.Fatalf("only %d (cell, arm) runs: the sweep must cover the table", ran)
	}
}

// ivArmsFixture is ftArmsFixture's five arms plus the coordinator's
// small-query fast path, over one shared catalog; the writer runs the DDL.
func ivArmsFixture(t *testing.T, ctx context.Context) (func(string) string, []ivArm) {
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
	spill := func(run func() (string, string)) (string, string) {
		restoreDrain := exec.ForceAggDrainEvery(1)
		restoreRuns := exec.ForceSmallSpillRuns(2)
		defer func() { restoreRuns(); exec.ForceAggDrainEvery(restoreDrain) }()
		return run()
	}
	return func(sql string) string {
			_, err := writer.Query(ctx, sql)
			return ftDDLRender(err)
		}, []ivArm{
			{"single", func(sql string) (string, string) { return ivRunSingle(ctx, writer, sql) }},
			{"spilled512k", func(sql string) (string, string) {
				return spill(func() (string, string) { return ivRunSingle(ctx, spilled, sql) })
			}},
			{"dag", func(sql string) (string, string) { return ivRunDAG(ctx, coord, sql) }},
			{"dag-shuffled", func(sql string) (string, string) { return ivRunDAG(ctx, coordB, sql) }},
			{"dag-morsel4", func(sql string) (string, string) { return ivRunDAG(ctx, coordM, sql) }},
			{"dag-fastpath", func(sql string) (string, string) { return ivRunDAG(ctx, coordF, sql) }},
		}
}

// ivArm is one execution arm. exec answers the rendered answer (ftRender's
// form, an error as "ERR <SQLSTATE>") and the error's whole TEXT, "" when
// the statement answered: the store sweep reads the text.
type ivArm struct {
	name string
	exec func(string) (string, string)
}

func (a ivArm) run(sql string) string { got, _ := a.exec(sql); return got }

// ivRunSingle is ftRunSingle that also answers the error text.
func ivRunSingle(ctx context.Context, db *wadjet.DB, sql string) (out, errText string) {
	defer func() {
		if r := recover(); r != nil {
			out, errText = fmt.Sprintf("PANIC %v", r), fmt.Sprintf("PANIC %v", r)
		}
	}()
	res, err := db.Query(ctx, sql)
	if err != nil {
		return ftErr(err), err.Error()
	}
	cols := make([]parquet.Column, len(res.ColumnMetas))
	for i, m := range res.ColumnMetas {
		cols[i] = parquet.Column{Name: m.Name, Type: m.TypeID, Precision: m.Precision, Scale: m.Scale}
	}
	cells := make([][]any, len(res.Rows))
	for i := range res.Rows {
		cells[i] = res.Cells(i)
	}
	return ftEngineRender(cols, cells), ""
}

// ivRunDAG is ftRunDAG that also answers the error text.
func ivRunDAG(ctx context.Context, c *Coordinator, sql string) (out, errText string) {
	defer func() {
		if r := recover(); r != nil {
			out, errText = fmt.Sprintf("PANIC %v", r), fmt.Sprintf("PANIC %v", r)
		}
	}()
	res, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		return ftErr(err), err.Error()
	}
	if res.Error != "" {
		return "ERR " + res.Error, res.Error
	}
	cols := append([]parquet.Column(nil), res.OutputSchema()...)
	var cells [][]any
	if st := res.Stream(); st != nil {
		defer st.Close()
		for {
			bb, berr := st.Next(ctx)
			if berr != nil {
				return ftErr(berr), berr.Error()
			}
			if bb == nil {
				break
			}
			cells = append(cells, bb.ToRowValues()...)
		}
	} else {
		rows, rerr := res.Rows()
		if rerr != nil {
			return ftErr(rerr), rerr.Error()
		}
		for _, r := range rows {
			row := make([]any, len(cols))
			for j, c := range cols {
				row[j] = r[c.Name]
			}
			cells = append(cells, row)
		}
	}
	return ftEngineRender(cols, cells), ""
}
