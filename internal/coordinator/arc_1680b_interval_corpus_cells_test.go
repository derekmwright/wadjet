// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"strconv"
	"strings"
)

// #1680b: the first-class INTERVAL's reference corpus (epic #1680). Every
// statement below is measured on PostgreSQL 17.11 (postgres:17-alpine,
// initdb --locale=C --encoding=UTF8) by TestArc1680bMeasure, which writes the
// section's statements file and its answers file (PostgreSQL's rows WITH the
// result column type OIDs) under testdata/arc_1680b/. The engine's CURRENT
// disposition of every statement, per arm, is recorded beside it by
// TestArc1680bRecordEngine and held by TestArc1680bEngineBaseline: the
// implementation items inherit that baseline and move it cell by cell. No
// cell asserts that the engine agrees with PostgreSQL: today almost every
// interval-valued result is refused (physical/validate_interval_use.go).
//
// The definition this corpus backs is docs/design/interval-execution-type.md.

// iv80Cell is one statement of the corpus.
type iv80Cell struct {
	name string
	sql  string
	// settings are PostgreSQL session settings ("IntervalStyle=iso_8601;
	// TimeZone=America/New_York") applied before the statement on the
	// reference only; this engine has none of them and runs the statement
	// as written (the engine file says so beside each such cell).
	settings string
	ddl      bool
	// write: a DML or CTAS statement PostgreSQL accepts (run like a
	// fixture, by the writer); in this stage it must be refused (the
	// manifest's interval-write).
	write bool
	// pgOnly: a PostgreSQL-only observation (EXPLAIN of the plan that
	// produced a plan-dependent answer); the engine does not run it.
	pgOnly bool
}

// iv80Section is one section of the corpus: its file stem and its cells.
type iv80Section struct {
	stem  string
	cells func() []iv80Cell
}

func iv80BaseSections() []iv80Section {
	return []iv80Section{
		{"01_input", iv80Input},
		{"02_fields", iv80Fields},
		{"03_order", iv80Order},
		{"04_keys", iv80Keys},
		{"05_arith", iv80Arith},
		{"06_calendar", iv80Calendar},
		{"07_functions", iv80Functions},
		{"09_model", iv80Model},
		{"10_definitions", iv80Definitions},
		{"11_relational", iv80Relational},
		{"12_writes", iv80Writes},
	}
}

// iv80Sections is every section: the base sections, then for each the
// cast-wrapped executable variants of its statements that publish a bare
// interval (iv80CastSection), which are separate cells: the original
// statement and its PostgreSQL transcript stay as they are.
func iv80Sections() []iv80Section {
	var out []iv80Section
	for _, s := range iv80BaseSections() {
		out = append(out, s)
		if s.stem == "09_model" || s.stem == "12_writes" {
			continue
		}
		out = append(out, iv80CastSection(s))
	}
	return out
}

// iv80Tables are every table the corpus creates (dropped first on the
// reference).
var iv80Tables = []string{"iv80_in", "iv80_k", "iv80_kr", "iv80_o", "iv80_ts", "iv80_m", "iv80_tz", "iv80_mv"}

type iv80Builder struct{ out []iv80Cell }

func (b *iv80Builder) add(name, sql string) { b.out = append(b.out, iv80Cell{name: name, sql: sql}) }
func (b *iv80Builder) set(name, settings, sql string) {
	b.out = append(b.out, iv80Cell{name: name, sql: sql, settings: settings})
}
func (b *iv80Builder) ddl(name, sql string) {
	b.out = append(b.out, iv80Cell{name: name, sql: sql, ddl: true})
}
func (b *iv80Builder) write(name, sql string) {
	b.out = append(b.out, iv80Cell{name: name, sql: sql, ddl: true, write: true})
}
func (b *iv80Builder) pg(name, settings, sql string) {
	b.out = append(b.out, iv80Cell{name: name, sql: sql, settings: settings, pgOnly: true})
}

// iv80Table is CREATE TABLE name (id BIGINT, s TEXT) and its rows: s is
// interval INPUT text (NULL for a NULL), read back with CAST(s AS INTERVAL)
// on both sides — this engine has no INTERVAL column (ADR-0012 r20), and
// persistent interval storage is out of this stage.
func (b *iv80Builder) iv80Table(name string, rows []string, reverse bool) {
	b.ddl("ddl/create/"+name, "CREATE TABLE "+name+" (id BIGINT, s TEXT)")
	var vals []string
	for i, v := range rows {
		lit := "NULL"
		if v != "NULL" {
			lit = "'" + v + "'"
		}
		vals = append(vals, "("+strconv.Itoa(i+1)+", "+lit+")")
	}
	if reverse {
		for i, j := 0, len(vals)-1; i < j; i, j = i+1, j-1 {
			vals[i], vals[j] = vals[j], vals[i]
		}
	}
	b.ddl("ddl/insert/"+name, "INSERT INTO "+name+" VALUES "+strings.Join(vals, ", "))
}

func iv80Lit(s string) string {
	if s == "NULL" {
		return "CAST(NULL AS INTERVAL)"
	}
	return "INTERVAL '" + s + "'"
}

// --- 1. Input -------------------------------------------------------------

// iv80InputForms are interval input texts PostgreSQL's interval_in reads, or
// refuses: one field and several, SQL-standard, ISO 8601 (designator and
// alternative), the postgres_verbose spellings with @ and ago, fractional
// fields (how the fraction cascades), sub-second, signs mixed per field, and
// the bounds of each field.
var iv80InputForms = []string{
	"1 day", "1 day 2 hours", "1 year 2 months 3 days 4 hours 5 minutes 6 seconds",
	"1-2", "-1-2", "3 4:05:06", "-3 -4:05:06", "1-2 3 4:05:06", "4:05:06", "4:05", "-4:05:06",
	"1 day 25:00:00", "-1 day +25:00:00", "1 mon -30 days", "-1 year -2 mons +3 days", "1 year -1 second",
	"P1Y2M3DT4H5M6S", "P1Y2M3DT4H5M6.5S", "PT36H", "P1.5M", "P0.5Y", "P1W", "-P1D", "P-1DT-1H", "P0001-02-03T04:05:06", "PT0S",
	"@ 1 year 2 mons", "@ 1 year 2 mons ago", "1 day ago", "-1 day ago", "@ 3 days 4 hours 5 mins 6 secs",
	"1.5 months", "1.5 days", "1.5 years", "0.5 weeks", "1.5 hours", "1.5 minutes", "0.1 months", "1.0333333 months",
	"-1.5 months", "-1.5 days", "1.5 centuries", "1 decade", "1 century", "1 millennium", "10 weeks",
	"1.000001 seconds", "0.0000005 seconds", "0.0000015 seconds", "0.0000025 seconds", "1 microsecond", "1 millisecond", "1.5 microseconds",
	"1:2:3.123456789", "-0.000001 seconds", "00:00:00.000001",
	"0", "0 seconds", "00:00:00", "-0 days", "100", "1.5", "-100",
	"178956970 years 7 months", "178956970 years 8 months", "-178956970 years -8 months", "-178956970 years -9 months",
	"2147483647 months", "2147483648 months", "-2147483648 months", "-2147483649 months",
	"2147483647 days", "2147483648 days", "-2147483648 days", "-2147483649 days",
	"2562047788:00:54.775807", "2562047788:00:54.775808", "-2562047788:00:54.775808", "-2562047788:00:54.775809",
	"2147483647 months 2147483647 days 2562047788:00:54.775807",
	"-2147483648 months -2147483648 days -2562047788:00:54.775808",
	"infinity", "-infinity", "+infinity", "INFINITY",
	"", " ", "garbage", "1 fortnight", "1 day 1 day", "1 2 3", "P1Y2", "1 days 2 days",
}

func iv80Input() []iv80Cell {
	b := &iv80Builder{}
	for i, s := range iv80InputForms {
		b.add("lit/"+strconv.Itoa(i)+"/"+s, "SELECT INTERVAL '"+s+"' AS a")
	}
	b.add("lit/null", "SELECT CAST(NULL AS INTERVAL) AS a")
	// SQL-standard qualifiers (the typmod's field restriction).
	for _, q := range []struct{ k, sql string }{
		{"ym", "INTERVAL '1-2' YEAR TO MONTH"},
		{"ymNeg", "INTERVAL '-1-2' YEAR TO MONTH"},
		{"ds", "INTERVAL '3 4:05:06' DAY TO SECOND"},
		{"dsFrac", "INTERVAL '3 4:05:06.789' DAY TO SECOND"},
		{"year", "INTERVAL '5' YEAR"},
		{"month", "INTERVAL '5' MONTH"},
		{"day", "INTERVAL '5' DAY"},
		{"hour", "INTERVAL '5' HOUR"},
		{"minute", "INTERVAL '5' MINUTE"},
		{"second", "INTERVAL '5' SECOND"},
		{"secondFrac", "INTERVAL '5.123456' SECOND"},
		{"dh", "INTERVAL '1 2' DAY TO HOUR"},
		{"dm", "INTERVAL '1 2:03' DAY TO MINUTE"},
		{"hm", "INTERVAL '1:30' HOUR TO MINUTE"},
		{"ms", "INTERVAL '1:30' MINUTE TO SECOND"},
		{"hs", "INTERVAL '1:30:15' HOUR TO SECOND"},
		{"dayDropsTime", "INTERVAL '1 day 2 hours 3 minutes' DAY"},
		{"yearDropsMonths", "INTERVAL '1 year 2 months' YEAR"},
		{"monthDropsDays", "INTERVAL '1 year 2 months 3 days' MONTH"},
		{"hourDropsMinutes", "INTERVAL '1 day 02:30:45' HOUR"},
		{"yearFrac", "INTERVAL '1.5' YEAR"},
		{"dayFrac", "INTERVAL '1.5' DAY"},
		{"hourNeg", "INTERVAL '-25' HOUR"},
		{"yearOver", "INTERVAL '178956971' YEAR"},
		{"dayOver", "INTERVAL '2147483648' DAY"},
	} {
		b.add("std/"+q.k, "SELECT "+q.sql+" AS a")
	}
	// Precision typmods.
	b.add("prec/iv3", "SELECT CAST(INTERVAL '1.123456 seconds' AS INTERVAL(3)) AS a")
	b.add("prec/iv0", "SELECT CAST(INTERVAL '1.5 seconds' AS INTERVAL(0)) AS a, CAST(INTERVAL '2.5 seconds' AS INTERVAL(0)) AS b, CAST(INTERVAL '-1.5 seconds' AS INTERVAL(0)) AS c")
	b.add("prec/literal3", "SELECT INTERVAL(3) '1.123456 seconds' AS a")
	b.add("prec/second3", "SELECT INTERVAL '1.123456' SECOND(3) AS a")
	b.add("prec/ds2", "SELECT INTERVAL '1 2:03:04.5678' DAY TO SECOND(2) AS a")
	b.add("prec/iv6", "SELECT CAST(INTERVAL '1.1234567 seconds' AS INTERVAL(6)) AS a")
	b.add("prec/iv7", "SELECT CAST(INTERVAL '1 second' AS INTERVAL(7)) AS a")
	b.add("prec/castDay", "SELECT CAST(INTERVAL '1 day 02:00:00' AS INTERVAL DAY) AS a")
	b.add("prec/castYear", "SELECT CAST(INTERVAL '14 months 3 days' AS INTERVAL YEAR) AS a")
	b.add("prec/castMonthTyp", "SELECT CAST(INTERVAL '14 months 3 days' AS INTERVAL YEAR TO MONTH) AS a")
	// Casts from and to text, and from other types.
	b.add("cast/fromText", "SELECT CAST('1 day 02:00:00' AS INTERVAL) AS a")
	b.add("cast/colon", "SELECT '1 day'::interval AS a")
	b.add("cast/toText", "SELECT CAST(INTERVAL '1 day 2 hours' AS TEXT) AS a, CAST(INTERVAL '-1 year +2 mons -3 days 04:05:06.7' AS TEXT) AS b")
	b.add("cast/toVarchar", "SELECT CAST(INTERVAL '1 mon' AS VARCHAR) AS a")
	b.add("cast/toTextNull", "SELECT CAST(CAST(NULL AS INTERVAL) AS TEXT) AS a")
	b.add("cast/fromInt", "SELECT CAST(5 AS INTERVAL) AS a")
	b.add("cast/fromNumeric", "SELECT CAST(1.5 AS INTERVAL) AS a")
	b.add("cast/fromTime", "SELECT CAST(TIME '01:02:03.5' AS INTERVAL) AS a")
	b.add("cast/toTime", "SELECT CAST(INTERVAL '25:30:00' AS TIME) AS a, CAST(INTERVAL '-1 hour' AS TIME) AS b")
	b.add("cast/toBigint", "SELECT CAST(INTERVAL '1 day' AS BIGINT) AS a")
	b.add("cast/fromDuration", "SELECT CAST(CAST(5 AS DURATION) AS INTERVAL) AS a")
	// make_interval.
	b.add("make/all", "SELECT make_interval(1, 2, 3, 4, 5, 6, 7.5) AS a")
	b.add("make/days", "SELECT make_interval(days => 10) AS a")
	b.add("make/secs", "SELECT make_interval(secs => 1.5) AS a, make_interval(secs => 0.0000015) AS b")
	b.add("make/negMins", "SELECT make_interval(mins => -90) AS a")
	b.add("make/weeks", "SELECT make_interval(weeks => 1, days => 1) AS a")
	b.add("make/zero", "SELECT make_interval() AS a")
	b.add("make/yearsOver", "SELECT make_interval(years => 178956971) AS a")
	b.add("make/secsInf", "SELECT make_interval(secs => CAST('infinity' AS DOUBLE PRECISION)) AS a")
	b.add("make/null", "SELECT make_interval(days => NULL) AS a")
	b.add("make/hoursOver", "SELECT make_interval(hours => 2147483647) AS a")
	// Row input: a text column cast per row, NULL row included.
	rows := []string{"1 day", "1 day 02:00:00", "1 mon -30 days", "P1Y2M", "NULL", "-1 year +2 mons", "00:00:00.000001", "garbage", "infinity"}
	b.iv80Table("iv80_in", rows, false)
	b.add("row/cast", "SELECT id, CAST(s AS INTERVAL) AS v FROM iv80_in WHERE id <> 8 ORDER BY id")
	b.add("row/castText", "SELECT id, CAST(CAST(s AS INTERVAL) AS TEXT) AS v FROM iv80_in WHERE id <> 8 ORDER BY id")
	b.add("row/bad", "SELECT id, CAST(s AS INTERVAL) AS v FROM iv80_in WHERE id = 8")
	b.add("row/badUnreached", "SELECT id FROM iv80_in WHERE id = 1 AND CAST(s AS INTERVAL) IS NOT NULL")
	return b.out
}

// --- 2. Stored fields and printing ---------------------------------------

// iv80FieldValues are values whose stored (months, days, microseconds) the
// extract cells reveal and whose text the four IntervalStyles print.
var iv80FieldValues = []string{
	"1 mon", "30 days", "720:00:00", "36 hours", "1 day 12:00:00", "1 year 2 mons 3 days 04:05:06.789",
	"-1 year -2 mons +3 days -04:05:06", "1 mon -30 days", "0", "-1 day", "00:00:00.000001", "-00:00:00.000001",
	"178956970 years 7 mons", "-178956970 years -8 mons", "2147483647 days", "2562047788:00:54.775807",
	"1.5 months", "-1 day +02:03:04.5", "14 months", "1 year -1 second", "infinity", "-infinity", "NULL",
}

func iv80Fields() []iv80Cell {
	b := &iv80Builder{}
	for i, v := range iv80FieldValues {
		l := iv80Lit(v)
		b.add("fields/"+strconv.Itoa(i)+"/"+v, "SELECT extract(year FROM "+l+") AS y, extract(month FROM "+l+") AS mo, extract(day FROM "+l+") AS d, extract(hour FROM "+l+") AS h, extract(minute FROM "+l+") AS mi, extract(second FROM "+l+") AS s, extract(microsecond FROM "+l+") AS us, extract(epoch FROM "+l+") AS ep")
	}
	var vals []string
	for i, v := range iv80FieldValues {
		vals = append(vals, "("+strconv.Itoa(i)+", "+iv80Lit(v)+")")
	}
	values := "(VALUES " + strings.Join(vals, ", ") + ") t(id, v)"
	for _, st := range []string{"postgres", "postgres_verbose", "sql_standard", "iso_8601"} {
		b.set("print/"+st, "IntervalStyle="+st, "SELECT id, v FROM "+values+" ORDER BY id")
		b.set("print/text/"+st, "IntervalStyle="+st, "SELECT id, CAST(v AS TEXT) AS t FROM "+values+" ORDER BY id")
	}
	// Each value printed alone under the default style, where the engine
	// can be judged cell by cell.
	for i, v := range iv80FieldValues {
		b.add("text/"+strconv.Itoa(i)+"/"+v, "SELECT CAST("+iv80Lit(v)+" AS TEXT) AS t")
	}
	// Input under a non-default IntervalStyle: sql_standard reads a leading
	// sign as applying to every field.
	b.set("input/sqlstd/mixed", "IntervalStyle=sql_standard", "SELECT CAST(CAST('-1 2:03:04' AS INTERVAL) AS TEXT) AS t, extract(day FROM CAST('-1 2:03:04' AS INTERVAL)) AS d, extract(hour FROM CAST('-1 2:03:04' AS INTERVAL)) AS h")
	b.set("input/postgres/mixed", "IntervalStyle=postgres", "SELECT CAST(CAST('-1 2:03:04' AS INTERVAL) AS TEXT) AS t, extract(day FROM CAST('-1 2:03:04' AS INTERVAL)) AS d, extract(hour FROM CAST('-1 2:03:04' AS INTERVAL)) AS h")
	b.set("print/dateStyleSQL", "DateStyle=SQL, DMY", "SELECT CAST(INTERVAL '1 year 2 mons 3 days 04:05:06' AS TEXT) AS t")
	return b.out
}

// --- 3. Equality and order -----------------------------------------------

// iv80OrderPairs are compared with every comparison operator. The large
// pairs need a normalized value wider than 64 bits: 2^63 microseconds is
// 106751991 days 04:00:54.775808, 2^64 is 213503982 days 08:01:49.551616.
var iv80OrderPairs = [][2]string{
	{"1 mon", "30 days"}, {"1 mon", "720:00:00"}, {"30 days", "720:00:00"}, {"1 mon -30 days", "0"},
	{"1 year", "360 days"}, {"1 year", "12 mons"}, {"1 day", "24:00:00"}, {"-1 day", "-24:00:00"},
	{"1 mon", "29 days 24:00:00.000001"}, {"1 mon", "30 days -00:00:00.000001"},
	{"00:00:00.000001", "0"}, {"-00:00:00.000001", "0"}, {"1 day -1 second", "23:59:59"},
	{"-1 mon 1 day", "-29 days"}, {"1 year -1 day", "359 days"},
	{"178956970 years 7 mons", "178956970 years 6 mons 30 days"},
	{"178956970 years 7 mons", "2147483647 days"},
	{"178956970 years 7 mons", "-178956970 years -8 mons"},
	{"2147483647 days 2562047788:00:54.775807", "178956970 years 7 mons"},
	{"106751991 days 04:00:54.775808", "0"},
	{"106751991 days 04:00:54.775807", "106751991 days 04:00:54.775808"},
	{"213503982 days 08:01:49.551616", "0"},
	{"213503982 days 08:01:49.551616", "00:00:00.000001"},
	{"-213503982 days -08:01:49.551616", "0"},
	{"infinity", "178956970 years 7 mons"}, {"-infinity", "-178956970 years -8 mons"},
	{"infinity", "infinity"}, {"infinity", "-infinity"},
	{"2147483647 months 2147483647 days 2562047788:00:54.775807", "infinity"},
}

func iv80CmpSQL(a, b string) string {
	return "SELECT " + a + " = " + b + " AS eq, " + a + " <> " + b + " AS ne, " + a + " < " + b + " AS lt, " +
		a + " <= " + b + " AS le, " + a + " > " + b + " AS gt, " + a + " >= " + b + " AS ge, " +
		a + " IS DISTINCT FROM " + b + " AS dist"
}

// iv80OrderRows is the ordering fixture: SQL-equal groups with different
// fields, the 64-bit wrap values, NULLs, infinities.
var iv80OrderRows = []string{
	"1 mon", "30 days", "720:00:00", "1 mon -30 days", "0", "-1 day", "-24:00:00", "NULL",
	"00:00:00.000001", "-00:00:00.000001", "1 year", "360 days", "213503982 days 08:01:49.551616",
	"106751991 days 04:00:54.775808", "-178956970 years -8 mons", "178956970 years 7 mons", "infinity", "-infinity", "NULL", "29 days 23:59:59.999999",
}

func iv80Order() []iv80Cell {
	b := &iv80Builder{}
	for _, p := range iv80OrderPairs {
		b.add("cmp/"+p[0]+"~"+p[1], iv80CmpSQL(iv80Lit(p[0]), iv80Lit(p[1])))
	}
	b.add("cmp/null", iv80CmpSQL("INTERVAL '1 day'", "CAST(NULL AS INTERVAL)")+", CAST(NULL AS INTERVAL) IS NOT DISTINCT FROM CAST(NULL AS INTERVAL) AS nn")
	b.add("between", "SELECT INTERVAL '1 mon' BETWEEN INTERVAL '30 days' AND INTERVAL '720:00:00' AS a, INTERVAL '1 mon -30 days' BETWEEN INTERVAL '0' AND INTERVAL '0' AS b, INTERVAL '25:00:00' BETWEEN SYMMETRIC INTERVAL '2 days' AND INTERVAL '1 day' AS c, INTERVAL '00:00:00.000001' NOT BETWEEN INTERVAL '-00:00:00.000001' AND INTERVAL '0' AS d")
	b.add("in", "SELECT INTERVAL '720:00:00' IN (INTERVAL '1 mon', INTERVAL '1 day') AS a, INTERVAL '0' IN (INTERVAL '1 mon -30 days') AS b, INTERVAL '213503982 days 08:01:49.551616' IN (INTERVAL '0') AS c, INTERVAL '1 day' IN (INTERVAL '2 days', NULL) AS d")
	b.iv80Table("iv80_o", iv80OrderRows, false)
	c := "CAST(s AS INTERVAL)"
	b.add("order/asc", "SELECT id, "+c+" AS v FROM iv80_o ORDER BY v, id")
	b.add("order/desc", "SELECT id, "+c+" AS v FROM iv80_o ORDER BY v DESC, id")
	b.add("order/ascNullsFirst", "SELECT id FROM iv80_o ORDER BY "+c+" NULLS FIRST, id")
	b.add("order/descNullsLast", "SELECT id FROM iv80_o ORDER BY "+c+" DESC NULLS LAST, id")
	b.add("order/idDesc", "SELECT id, "+c+" AS v FROM iv80_o ORDER BY v, id DESC")
	b.add("order/minMax", "SELECT min("+c+") AS lo, max("+c+") AS hi FROM iv80_o")
	b.add("order/minMaxFinite", "SELECT min("+c+") AS lo, max("+c+") AS hi FROM iv80_o WHERE id NOT IN (17, 18)")
	// Which member min / max print when the extremum is SQL-tied
	// (interval_smaller / interval_larger keep the later argument on a tie).
	b.add("order/minTieFwd", "SELECT min(v) AS lo, max(v) AS hi FROM (VALUES (1, INTERVAL '1 mon'), (2, INTERVAL '30 days'), (3, INTERVAL '720:00:00')) t(id, v)")
	b.add("order/minTieRev", "SELECT min(v) AS lo, max(v) AS hi FROM (VALUES (3, INTERVAL '720:00:00'), (2, INTERVAL '30 days'), (1, INTERVAL '1 mon')) t(id, v)")
	b.add("order/minTieTable", "SELECT min("+c+") AS lo, max("+c+") AS hi FROM iv80_o WHERE id IN (1, 2, 3)")
	b.add("order/greatestTie", "SELECT GREATEST(INTERVAL '1 mon', INTERVAL '30 days') AS g, LEAST(INTERVAL '1 mon', INTERVAL '30 days') AS l, GREATEST(INTERVAL '30 days', INTERVAL '1 mon') AS g2, LEAST(INTERVAL '30 days', INTERVAL '1 mon') AS l2")
	b.add("order/topN", "SELECT id FROM iv80_o WHERE "+c+" IS NOT NULL ORDER BY "+c+" DESC, id LIMIT 5")
	b.add("order/wrap", "SELECT id FROM iv80_o WHERE id IN (5, 13, 14, 16) ORDER BY "+c+", id")
	b.add("filter/eq", "SELECT id FROM iv80_o WHERE "+c+" = INTERVAL '30 days' ORDER BY id")
	b.add("filter/eqZero", "SELECT id FROM iv80_o WHERE "+c+" = INTERVAL '0' ORDER BY id")
	b.add("filter/lt", "SELECT id FROM iv80_o WHERE "+c+" < INTERVAL '0' ORDER BY id")
	b.add("filter/gtWrap", "SELECT id FROM iv80_o WHERE "+c+" > INTERVAL '100000 years' ORDER BY id")
	return b.out
}

// --- 4. Hashing and keys -------------------------------------------------

// iv80KeyRows: SQL-equal groups of different fields (1-3, 4-6, 7-8, 11-12,
// 15-17), NULLs, and the two values whose normalized value differs from 0 /
// from each other only above bit 63.
var iv80KeyRows = []string{
	"1 mon", "30 days", "720:00:00", "1 mon -30 days", "0", "00:00:00", "-1 day", "-24:00:00", "NULL", "NULL",
	"1 day", "24:00:00", "213503982 days 08:01:49.551616", "106751991 days 04:00:54.775808", "1 year", "360 days", "12 mons",
}

func iv80Keys() []iv80Cell {
	b := &iv80Builder{}
	b.iv80Table("iv80_k", iv80KeyRows, false)
	b.iv80Table("iv80_kr", iv80KeyRows, true)
	c := "CAST(s AS INTERVAL)"
	plans := []struct{ k, set string }{{"default", ""}, {"sortagg", "enable_hashagg=off"}, {"hashagg", "enable_sort=off"}}
	for _, t := range []string{"iv80_k", "iv80_kr"} {
		for _, p := range plans {
			sfx := "/" + t + "/" + p.k
			b.set("group"+sfx, p.set, "SELECT "+c+" AS k, count(*) AS n, min(id) AS lo FROM "+t+" GROUP BY "+c+" ORDER BY lo NULLS FIRST")
			b.pg("explain/group"+sfx, p.set, "EXPLAIN (COSTS OFF) SELECT "+c+" AS k, count(*) AS n, min(id) AS lo FROM "+t+" GROUP BY "+c+" ORDER BY lo NULLS FIRST")
			b.set("distinct"+sfx, p.set, "SELECT k FROM (SELECT DISTINCT "+c+" AS k FROM "+t+") d ORDER BY k NULLS FIRST")
			b.pg("explain/distinct"+sfx, p.set, "EXPLAIN (COSTS OFF) SELECT k FROM (SELECT DISTINCT "+c+" AS k FROM "+t+") d ORDER BY k NULLS FIRST")
			b.set("union"+sfx, p.set, "SELECT k FROM (SELECT "+c+" AS k FROM "+t+" WHERE id <= 8 UNION SELECT "+c+" FROM "+t+" WHERE id > 8) u ORDER BY k NULLS FIRST")
			b.set("intersect"+sfx, p.set, "SELECT k FROM (SELECT "+c+" AS k FROM "+t+" WHERE id IN (2, 5, 8, 12, 16) INTERSECT SELECT "+c+" FROM "+t+" WHERE id IN (1, 3, 4, 7, 11, 15)) u ORDER BY k")
			b.set("exceptAll"+sfx, p.set, "SELECT k FROM (SELECT "+c+" AS k FROM "+t+" WHERE id <= 8 EXCEPT ALL SELECT "+c+" FROM "+t+" WHERE id IN (1, 4)) u ORDER BY k")
		}
		sfx := "/" + t
		b.add("groupCount"+sfx, "SELECT count(*) AS g FROM (SELECT 1 AS one FROM "+t+" GROUP BY "+c+") x")
		b.add("distinctCount"+sfx, "SELECT count(*) AS g FROM (SELECT DISTINCT "+c+" AS k FROM "+t+") x")
		b.add("countDistinct"+sfx, "SELECT count(DISTINCT "+c+") AS n FROM "+t)
		b.add("partition"+sfx, "SELECT id, count(*) OVER (PARTITION BY "+c+") AS n, first_value(id) OVER (PARTITION BY "+c+" ORDER BY id) AS f FROM "+t+" ORDER BY id")
		b.add("peers"+sfx, "SELECT id, rank() OVER (ORDER BY "+c+") AS r, dense_rank() OVER (ORDER BY "+c+") AS dr, count(*) OVER (ORDER BY "+c+") AS cnt FROM "+t+" ORDER BY id")
		b.add("orderTie"+sfx, "SELECT id, "+c+" AS v FROM "+t+" ORDER BY v, id")
		b.add("withTies"+sfx, "SELECT id FROM "+t+" ORDER BY "+c+" FETCH FIRST 2 ROWS WITH TIES")
		b.add("withTiesCount"+sfx, "SELECT count(*) AS n FROM (SELECT id FROM "+t+" WHERE "+c+" IS NOT NULL ORDER BY "+c+" DESC FETCH FIRST 1 ROWS WITH TIES) x")
	}
	// Joins: hash, merge, nested loop.
	join := "SELECT a.id AS l, b.id AS r FROM iv80_k a JOIN iv80_k b ON CAST(a.s AS INTERVAL) = CAST(b.s AS INTERVAL) WHERE a.id < b.id ORDER BY l, r"
	for _, p := range []struct{ k, set string }{
		{"default", ""},
		{"hash", "enable_mergejoin=off;enable_nestloop=off"},
		{"merge", "enable_hashjoin=off;enable_nestloop=off"},
		{"nestloop", "enable_hashjoin=off;enable_mergejoin=off"},
	} {
		b.set("join/"+p.k, p.set, join)
		b.pg("explain/join/"+p.k, p.set, "EXPLAIN (COSTS OFF) "+join)
	}
	b.add("join/left", "SELECT a.id AS l, b.id AS r FROM iv80_k a LEFT JOIN iv80_k b ON CAST(a.s AS INTERVAL) = CAST(b.s AS INTERVAL) AND a.id <> b.id ORDER BY l, r NULLS FIRST")
	b.add("join/semi", "SELECT id FROM iv80_k a WHERE CAST(s AS INTERVAL) IN (SELECT CAST(s AS INTERVAL) FROM iv80_k WHERE id IN (2, 13)) ORDER BY id")
	b.add("join/anti", "SELECT id FROM iv80_k a WHERE NOT EXISTS (SELECT 1 FROM iv80_k b WHERE CAST(b.s AS INTERVAL) = CAST(a.s AS INTERVAL) AND b.id <> a.id) ORDER BY id")
	// Set operations over literals: which spelling survives.
	for _, p := range []struct{ k, set string }{{"default", ""}, {"sortagg", "enable_hashagg=off"}} {
		b.set("setLit/union/"+p.k, p.set, "SELECT INTERVAL '1 mon' AS x UNION SELECT INTERVAL '30 days' UNION SELECT INTERVAL '720:00:00'")
		b.set("setLit/unionRev/"+p.k, p.set, "SELECT INTERVAL '720:00:00' AS x UNION SELECT INTERVAL '30 days' UNION SELECT INTERVAL '1 mon'")
		b.set("setLit/intersect/"+p.k, p.set, "SELECT INTERVAL '30 days' AS x INTERSECT SELECT INTERVAL '1 mon'")
		b.set("setLit/except/"+p.k, p.set, "SELECT INTERVAL '30 days' AS x EXCEPT SELECT INTERVAL '1 mon'")
		b.set("setLit/exceptAll/"+p.k, p.set, "SELECT x FROM (SELECT x FROM (VALUES (INTERVAL '1 mon'), (INTERVAL '30 days'), (INTERVAL '720:00:00')) v(x) EXCEPT ALL SELECT INTERVAL '30 days') u")
		b.set("setLit/intersectAll/"+p.k, p.set, "SELECT x FROM (SELECT x FROM (VALUES (INTERVAL '1 mon'), (INTERVAL '30 days'), (INTERVAL '720:00:00')) v(x) INTERSECT ALL SELECT y FROM (VALUES (INTERVAL '720:00:00'), (INTERVAL '1 mon')) w(y)) u")
		b.set("setLit/valuesDistinct/"+p.k, p.set, "SELECT DISTINCT x FROM (VALUES (INTERVAL '30 days'), (INTERVAL '1 mon'), (INTERVAL '720:00:00')) v(x)")
	}
	// The low-64-bit collision: PostgreSQL hashes only the low 64 bits of
	// the 128-bit normalized value, but compares all 128.
	b.add("wrap/group", "SELECT count(*) AS g FROM (SELECT x FROM (VALUES (INTERVAL '213503982 days 08:01:49.551616'), (INTERVAL '0'), (INTERVAL '0')) v(x) GROUP BY x) t")
	b.add("wrap/join", "SELECT count(*) AS n FROM (VALUES (INTERVAL '213503982 days 08:01:49.551616')) a(x) JOIN (VALUES (INTERVAL '0')) b(y) ON x = y")
	b.add("wrap/hashCollide", "SELECT interval_hash(INTERVAL '213503982 days 08:01:49.551616') = interval_hash(INTERVAL '0') AS a, interval_hash(INTERVAL '1 mon') = interval_hash(INTERVAL '720:00:00') AS b, interval_hash_extended(INTERVAL '1 mon', 0) = interval_hash_extended(INTERVAL '30 days', 0) AS c")
	// ADR-0024 section 12 item 4 says PostgreSQL's INTERSECT representative
	// is a left-arm member under both measured plans; the sorted SetOp sorts
	// on the key alone, so a right-arm member can lead its group. The same
	// cell for NUMERIC (7 / 7.00), the type that ADR measured.
	b.set("adr0024/intersectNumericSort", "enable_hashagg=off", "SELECT k FROM (SELECT x AS k FROM (VALUES (1.00), (2.00), (3.00), (4.00), (5.00), (6.00)) a(x) INTERSECT SELECT y FROM (VALUES (6), (5), (4), (3), (2), (1), (1), (2)) b(y)) u ORDER BY k")
	b.pg("explain/adr0024/intersectNumericSort", "enable_hashagg=off", "EXPLAIN (COSTS OFF) SELECT k FROM (SELECT x AS k FROM (VALUES (1.00), (2.00), (3.00), (4.00), (5.00), (6.00)) a(x) INTERSECT SELECT y FROM (VALUES (6), (5), (4), (3), (2), (1), (1), (2)) b(y)) u ORDER BY k")
	b.set("adr0024/intersectNumericHash", "", "SELECT k FROM (SELECT x AS k FROM (VALUES (1.00), (2.00), (3.00), (4.00), (5.00), (6.00)) a(x) INTERSECT SELECT y FROM (VALUES (6), (5), (4), (3), (2), (1), (1), (2)) b(y)) u ORDER BY k")
	b.pg("explain/intersect/iv80_kr/sortagg", "enable_hashagg=off", "EXPLAIN (COSTS OFF) SELECT k FROM (SELECT CAST(s AS INTERVAL) AS k FROM iv80_kr WHERE id IN (2, 5, 8, 12, 16) INTERSECT SELECT CAST(s AS INTERVAL) FROM iv80_kr WHERE id IN (1, 3, 4, 7, 11, 15)) u ORDER BY k")
	// A container key.
	b.add("array/group", "SELECT count(*) AS g FROM (SELECT x FROM (VALUES (ARRAY[INTERVAL '1 mon']), (ARRAY[INTERVAL '30 days'])) v(x) GROUP BY x) t")
	b.add("array/eq", "SELECT ARRAY[INTERVAL '1 mon'] = ARRAY[INTERVAL '30 days'] AS a, ARRAY[INTERVAL '1 mon', INTERVAL '1 day'] < ARRAY[INTERVAL '30 days', INTERVAL '25:00:00'] AS b")
	b.add("row/group", "SELECT count(*) AS g FROM (SELECT a, b FROM (VALUES (INTERVAL '1 mon', 1), (INTERVAL '30 days', 1), (INTERVAL '30 days', 2)) v(a, b) GROUP BY a, b) t")
	return b.out
}

// --- 5. Interval arithmetic ----------------------------------------------

func iv80Arith() []iv80Cell {
	b := &iv80Builder{}
	t := func(e string) string { return "CAST(" + e + " AS TEXT)" }
	two := func(name, x, y string) {
		b.add(name, "SELECT "+x+" AS a, "+y+" AS b")
	}
	two("plus/fields", "INTERVAL '1 mon 2 days 03:00:00' + INTERVAL '1 year 1 day 23:00:00'", "INTERVAL '1 mon' + INTERVAL '-30 days'")
	two("plus/noCarry", "INTERVAL '20 hours' + INTERVAL '20 hours'", "INTERVAL '25 days' + INTERVAL '10 days'")
	two("plus/subsec", "INTERVAL '00:00:00.000001' + INTERVAL '00:00:00.999999'", "INTERVAL '-00:00:00.000001' + INTERVAL '0'")
	two("minus/fields", "INTERVAL '1 mon' - INTERVAL '1 day'", "INTERVAL '1 day' - INTERVAL '25 hours'")
	two("minus/zero", "INTERVAL '1 mon' - INTERVAL '30 days'", "(INTERVAL '1 mon' - INTERVAL '30 days') = INTERVAL '0'")
	two("plus/null", "INTERVAL '1 day' + CAST(NULL AS INTERVAL)", "CAST(NULL AS INTERVAL) - INTERVAL '1 day'")
	b.add("plus/overflowMonths", "SELECT INTERVAL '178956970 years 7 mons' + INTERVAL '1 mon' AS a")
	b.add("plus/overflowDays", "SELECT INTERVAL '2147483647 days' + INTERVAL '1 day' AS a")
	b.add("plus/overflowTime", "SELECT INTERVAL '2562047788:00:54.775807' + INTERVAL '00:00:00.000001' AS a")
	b.add("plus/toInfinityFields", "SELECT INTERVAL '2147483647 months 2147483647 days 2562047788:00:54.775806' + INTERVAL '00:00:00.000001' AS a")
	b.add("plus/inf", "SELECT INTERVAL 'infinity' + INTERVAL '1 day' AS a, INTERVAL '-infinity' - INTERVAL '1 day' AS b, INTERVAL 'infinity' + INTERVAL 'infinity' AS c")
	b.add("plus/infMinusInf", "SELECT INTERVAL 'infinity' - INTERVAL 'infinity' AS a")
	b.add("plus/infPlusNegInf", "SELECT INTERVAL 'infinity' + INTERVAL '-infinity' AS a")
	two("neg/basic", "-INTERVAL '1 year -2 mons 3 days -04:05:06'", "-INTERVAL '0'")
	two("neg/inf", "-INTERVAL 'infinity'", "-INTERVAL '-infinity'")
	b.add("neg/minMonths", "SELECT -INTERVAL '-2147483648 months' AS a")
	b.add("neg/minDays", "SELECT -INTERVAL '-2147483648 days' AS a")
	b.add("neg/minTime", "SELECT -INTERVAL '-2562047788:00:54.775808' AS a")
	// The INT64_MIN time field is finite unless months and days are at
	// their minimum too; the literal above cannot spell it, the arithmetic can.
	b.add("neg/minTimeBuilt", "SELECT INTERVAL '-2562047788:00:54.775807' - INTERVAL '00:00:00.000001' AS a, -(INTERVAL '-2562047788:00:54.775807' - INTERVAL '00:00:00.000001') AS b")
	b.add("neg/minTimeBuiltAlone", "SELECT INTERVAL '-2562047788:00:54.775807' - INTERVAL '00:00:00.000001' AS a")
	// Multiplication: the fraction cascade (months to days at 30, days to
	// seconds at 86400), rounding to the microsecond (rint: half to even).
	for _, m := range []struct{ k, iv, f string }{
		{"int", "1 mon 1 day 01:00:00", "3"},
		{"zero", "1 mon 1 day 01:00:00", "0"},
		{"neg", "1 mon 1 day 01:00:00", "-2"},
		{"halfMonth", "1 mon", "0.5"},
		{"thirdMonth", "1 mon", "0.333333"},
		{"monthCascade", "7 mons", "0.3"},
		{"monthToTime", "1 mon", "0.1"},
		{"dayFrac", "1 day", "1.5"},
		{"dayCascade", "5 days", "0.3"},
		{"yearFrac", "1 year", "1.1"},
		{"mixedSign", "1 mon -1 day", "1.5"},
		{"negFrac", "-1 mon", "1.5"},
		{"timeOver24", "20:00:00", "2"},
		{"usHalfEven1", "00:00:00.000001", "1.5"},
		{"usHalfEven2", "00:00:00.000001", "2.5"},
		{"usHalfEven3", "00:00:00.000001", "0.5"},
		{"usNeg", "-00:00:00.000001", "1.5"},
		{"tinyMonth", "1 mon", "0.0000001"},
		{"float8", "1 day", "CAST(2.5 AS DOUBLE PRECISION)"},
		{"real", "1 day", "CAST(0.1 AS REAL)"},
		{"bigint", "1 day", "CAST(3 AS BIGINT)"},
		{"numeric", "1 day", "CAST(1.25 AS NUMERIC)"},
		{"unknown", "1 day", "'2'"},
		{"null", "1 day", "NULL"},
		{"overflowDays", "2147483647 days", "2"},
		{"overflowMonths", "178956970 years", "2"},
		{"overflowTime", "2562047788:00:00", "2"},
		{"huge", "1 day", "1e300"},
		{"nan", "1 day", "CAST('NaN' AS DOUBLE PRECISION)"},
		{"infFactor", "1 day", "CAST('Infinity' AS DOUBLE PRECISION)"},
		{"negInfFactor", "1 day", "CAST('-Infinity' AS DOUBLE PRECISION)"},
		{"zeroTimesInf", "0", "CAST('Infinity' AS DOUBLE PRECISION)"},
		{"infTimesZero", "infinity", "0"},
		{"infTimesNeg", "infinity", "-1"},
	} {
		b.add("mul/"+m.k, "SELECT INTERVAL '"+m.iv+"' * "+m.f+" AS a, "+t("INTERVAL '"+m.iv+"' * "+m.f)+" AS t")
	}
	b.add("mul/commuted", "SELECT 2 * INTERVAL '1 mon 1 day' AS a, 0.5 * INTERVAL '1 mon' AS b")
	// Division.
	for _, d := range []struct{ k, iv, f string }{
		{"int", "1 mon 1 day 01:00:00", "2"},
		{"thirdMonth", "1 mon", "3"},
		{"thirdDay", "1 day", "3"},
		{"monthCascade", "3 mons", "2"},
		{"yearBy7", "1 year", "7"},
		{"neg", "3 days", "-2"},
		{"frac", "1 day", "1.5"},
		{"half", "1 day", "0.5"},
		{"us", "00:00:00.000001", "2"},
		{"us3", "00:00:00.000003", "2"},
		{"usNeg", "-00:00:00.000001", "2"},
		{"zero", "1 day", "0"},
		{"zeroFloat", "1 day", "CAST(0 AS DOUBLE PRECISION)"},
		{"tiny", "1 day", "1e-300"},
		{"nan", "1 day", "CAST('NaN' AS DOUBLE PRECISION)"},
		{"infDivisor", "1 day", "CAST('Infinity' AS DOUBLE PRECISION)"},
		{"infDividend", "infinity", "2"},
		{"infByInf", "infinity", "CAST('Infinity' AS DOUBLE PRECISION)"},
		{"unknown", "1 day", "'2'"},
		{"null", "1 day", "NULL"},
	} {
		b.add("div/"+d.k, "SELECT INTERVAL '"+d.iv+"' / "+d.f+" AS a, "+t("INTERVAL '"+d.iv+"' / "+d.f)+" AS t")
	}
	b.add("div/roundTrip", "SELECT (INTERVAL '1 mon' / 3) * 3 AS a, (INTERVAL '1 mon' / 3) * 3 = INTERVAL '1 mon' AS b, (INTERVAL '1 day' / 7) * 7 AS c")
	// justify_*.
	for _, j := range []struct{ k, iv string }{
		{"pos", "1 mon 35 days 27:00:00"}, {"neg", "-1 mon -35 days -27:00:00"}, {"mixed1", "1 mon -1 day"},
		{"mixed2", "-1 mon 35 days"}, {"mixed3", "1 day -25:00:00"}, {"mixed4", "-1 day 25:00:00"},
		{"zero", "0"}, {"us", "-00:00:00.000001"}, {"bigHours", "2562047788:00:54.775807"},
		{"bigDays", "2147483647 days 24:00:00"}, {"monthOverflow", "178956970 years 7 mons 30 days"},
		{"inf", "infinity"}, {"null", "NULL"},
	} {
		l := iv80Lit(j.iv)
		b.add("justify/"+j.k, "SELECT justify_days("+l+") AS d, justify_hours("+l+") AS h, justify_interval("+l+") AS i")
	}
	// sum / avg: field-wise accumulation (sum keeps fields; avg divides
	// each field and cascades).
	b.iv80Table("iv80_m", []string{"1 mon", "30 days", "720:00:00", "-1 day", "00:00:00.000001", "NULL", "1 mon -30 days", "1 year 25:00:00"}, false)
	c := "CAST(s AS INTERVAL)"
	b.add("agg/sumAvg", "SELECT sum("+c+") AS s, avg("+c+") AS a, count("+c+") AS n FROM iv80_m")
	b.add("agg/sumAvgSubset", "SELECT sum("+c+") AS s, avg("+c+") AS a FROM iv80_m WHERE id IN (1, 4)")
	b.add("agg/avgCascade", "SELECT avg("+c+") AS a FROM iv80_m WHERE id IN (1, 5)")
	b.add("agg/avgThirds", "SELECT avg(v) AS a FROM (VALUES (INTERVAL '1 mon'), (INTERVAL '0'), (INTERVAL '0')) t(v)")
	b.add("agg/avgUs", "SELECT avg(v) AS a FROM (VALUES (INTERVAL '00:00:00.000001'), (INTERVAL '0')) t(v)")
	b.add("agg/avgUs3", "SELECT avg(v) AS a FROM (VALUES (INTERVAL '00:00:00.000003'), (INTERVAL '0')) t(v)")
	b.add("agg/sumEmpty", "SELECT sum("+c+") AS s, avg("+c+") AS a FROM iv80_m WHERE id < 0")
	b.add("agg/sumNull", "SELECT sum("+c+") AS s, avg("+c+") AS a FROM iv80_m WHERE id = 6")
	b.add("agg/sumGroup", "SELECT id % 2 AS g, sum("+c+") AS s, avg("+c+") AS a FROM iv80_m GROUP BY id % 2 ORDER BY g")
	b.add("agg/sumOverflowDays", "SELECT sum(v) AS s FROM (VALUES (INTERVAL '2147483647 days'), (INTERVAL '1 day')) t(v)")
	b.add("agg/sumOverflowThenBack", "SELECT sum(v) AS s FROM (VALUES (INTERVAL '2147483647 days'), (INTERVAL '1 day'), (INTERVAL '-1 day')) t(v)")
	b.add("agg/avgBig", "SELECT avg(v) AS a FROM (VALUES (INTERVAL '2147483647 days'), (INTERVAL '2147483647 days')) t(v)")
	b.add("agg/sumInf", "SELECT sum(v) AS s, avg(v) AS a FROM (VALUES (INTERVAL 'infinity'), (INTERVAL '1 day')) t(v)")
	b.add("agg/sumInfNegInf", "SELECT sum(v) AS s FROM (VALUES (INTERVAL 'infinity'), (INTERVAL '-infinity')) t(v)")
	b.add("agg/avgInfNegInf", "SELECT avg(v) AS a FROM (VALUES (INTERVAL 'infinity'), (INTERVAL '-infinity')) t(v)")
	b.add("agg/sumWindow", "SELECT id, sum("+c+") OVER (ORDER BY id) AS s FROM iv80_m WHERE id <= 4 ORDER BY id")
	b.add("agg/avgWindowSliding", "SELECT id, avg("+c+") OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a FROM iv80_m WHERE id <= 5 ORDER BY id")
	return b.out
}

// --- 6. Calendar arithmetic ----------------------------------------------

func iv80Calendar() []iv80Cell {
	b := &iv80Builder{}
	ts := func(name, from, op, iv string) {
		b.add(name, "SELECT TIMESTAMP '"+from+"' "+op+" INTERVAL '"+iv+"' AS a")
	}
	ts("ts/monthClampLeap", "2024-01-31 10:00:00", "+", "1 mon")
	ts("ts/monthClamp", "2023-01-31 10:00:00", "+", "1 mon")
	ts("ts/monthBack", "2024-03-31 00:00:00", "-", "1 mon")
	ts("ts/yearLeap", "2024-02-29 00:00:00", "+", "1 year")
	ts("ts/negMonths", "2024-03-31 00:00:00", "+", "-13 mons")
	// Order of application: months, then days, then the time.
	ts("ts/orderMD", "2024-01-30 00:00:00", "+", "1 mon 2 days")
	ts("ts/orderMDNeg", "2024-03-01 00:00:00", "-", "1 mon 1 day")
	ts("ts/orderDT", "2024-02-28 23:00:00", "+", "1 day 02:00:00")
	ts("ts/mixedSign", "2024-01-31 00:00:00", "+", "1 mon -1 day")
	ts("ts/mixedSign2", "2024-03-01 00:00:00", "+", "-1 mon +30 days")
	ts("ts/hours36", "2024-01-31 00:00:00", "+", "36 hours")
	ts("ts/us", "2024-01-01 00:00:00", "+", "00:00:00.000001")
	ts("ts/usNeg", "2024-01-01 00:00:00", "-", "00:00:00.000001")
	ts("ts/zero", "2024-01-01 00:00:00", "+", "0")
	ts("ts/1monVs30d", "2024-02-01 00:00:00", "+", "30 days")
	ts("ts/overflow", "294276-12-31 00:00:00", "+", "1 year")
	ts("ts/inf", "2024-01-01 00:00:00", "+", "infinity")
	ts("ts/negInf", "2024-01-01 00:00:00", "+", "-infinity")
	b.add("ts/infTs", "SELECT TIMESTAMP 'infinity' + INTERVAL '1 day' AS a, TIMESTAMP 'infinity' + INTERVAL 'infinity' AS b")
	b.add("ts/infMinusInf", "SELECT TIMESTAMP 'infinity' - INTERVAL 'infinity' AS a")
	b.add("ts/null", "SELECT TIMESTAMP '2024-01-01 00:00:00' + CAST(NULL AS INTERVAL) AS a, CAST(NULL AS TIMESTAMP) + INTERVAL '1 day' AS b")
	b.add("ts/commuted", "SELECT INTERVAL '1 mon' + TIMESTAMP '2024-01-31 00:00:00' AS a")
	// Date ± interval is a timestamp.
	b.add("date/plus", "SELECT DATE '2024-01-31' + INTERVAL '1 mon' AS a, DATE '2024-01-31' - INTERVAL '1 day 01:00:00' AS b, INTERVAL '1 day' + DATE '2024-02-28' AS c")
	b.add("date/plusInt", "SELECT DATE '2024-01-31' + 30 AS a, DATE '2024-03-01' - DATE '2024-02-01' AS b, DATE '2024-03-01' - 1 AS c")
	b.add("date/minusDate", "SELECT DATE '2024-03-01' - DATE '2023-03-01' AS a, DATE '2023-03-01' - DATE '2024-03-01' AS b")
	// timestamptz under two zones, across a DST transition (2024-03-10 in
	// America/New_York): a day is a calendar day, 24 hours is 24 hours.
	for _, z := range []string{"UTC", "America/New_York"} {
		set := "TimeZone=" + z
		b.set("tz/"+z+"/day", set, "SELECT TIMESTAMPTZ '2024-03-09 12:00:00-05' + INTERVAL '1 day' AS a, TIMESTAMPTZ '2024-03-09 12:00:00-05' + INTERVAL '24 hours' AS b")
		b.set("tz/"+z+"/month", set, "SELECT TIMESTAMPTZ '2024-02-10 12:00:00-05' + INTERVAL '1 mon' AS a, TIMESTAMPTZ '2024-01-31 23:30:00-05' + INTERVAL '1 mon' AS b")
		b.set("tz/"+z+"/fallBack", set, "SELECT TIMESTAMPTZ '2024-11-02 12:00:00-04' + INTERVAL '1 day' AS a, TIMESTAMPTZ '2024-11-02 12:00:00-04' + INTERVAL '24 hours' AS b")
		b.set("tz/"+z+"/minus", set, "SELECT TIMESTAMPTZ '2024-03-11 12:00:00-04' - TIMESTAMPTZ '2024-03-10 00:00:00-05' AS a, TIMESTAMPTZ '2024-03-10 12:00:00-04' - TIMESTAMPTZ '2024-03-09 12:00:00-05' AS b")
		b.set("tz/"+z+"/roundTrip", set, "SELECT TIMESTAMPTZ '2024-03-09 12:00:00-05' + (TIMESTAMPTZ '2024-03-10 12:00:00-04' - TIMESTAMPTZ '2024-03-09 12:00:00-05') AS a")
		b.set("tz/"+z+"/age", set, "SELECT age(TIMESTAMPTZ '2024-03-10 12:00:00-04', TIMESTAMPTZ '2024-01-31 12:00:00-05') AS a")
		b.set("tz/"+z+"/addFn", set, "SELECT date_add(TIMESTAMPTZ '2024-03-09 12:00:00-05', INTERVAL '1 day', 'America/New_York') AS a, date_add(TIMESTAMPTZ '2024-03-09 12:00:00-05', INTERVAL '1 day', 'UTC') AS b")
	}
	// time ± interval: wraps modulo 24 hours; months and days ignored.
	b.add("time/plus", "SELECT TIME '23:00:00' + INTERVAL '2 hours' AS a, TIME '01:00:00' - INTERVAL '2 hours' AS b, TIME '12:00:00' + INTERVAL '1 day 1 mon 00:00:01' AS c, TIME '12:00:00' + INTERVAL '00:00:00.000001' AS d")
	b.add("time/minusTime", "SELECT TIME '01:00:00' - TIME '23:00:00' AS a, TIME '23:00:00' - TIME '01:00:00' AS b")
	b.add("time/timetz", "SELECT TIMETZ '23:00:00+02' + INTERVAL '2 hours' AS a")
	// timestamp - timestamp: days and time, never months (interval_justify_hours).
	for _, d := range []struct{ k, a, b string }{
		{"days", "2024-03-01 00:00:00", "2024-01-01 00:00:00"},
		{"time", "2024-01-02 01:00:00", "2024-01-01 02:00:00"},
		{"over24", "2024-01-02 03:00:00", "2024-01-01 02:00:00"},
		{"neg", "2024-01-01 02:00:00", "2024-01-02 03:00:00"},
		{"year", "2025-01-01 00:00:00", "2024-01-01 00:00:00"},
		{"us", "2024-01-01 00:00:00.000001", "2024-01-01 00:00:00"},
		{"mixedNeg", "2024-01-02 01:00:00", "2024-01-03 02:00:00"},
		{"huge", "294276-01-01 00:00:00", "4713-01-01 00:00:00 BC"},
	} {
		b.add("tsdiff/"+d.k, "SELECT TIMESTAMP '"+d.a+"' - TIMESTAMP '"+d.b+"' AS a, extract(day FROM TIMESTAMP '"+d.a+"' - TIMESTAMP '"+d.b+"') AS d, extract(month FROM TIMESTAMP '"+d.a+"' - TIMESTAMP '"+d.b+"') AS m")
	}
	b.add("tsdiff/inf", "SELECT TIMESTAMP 'infinity' - TIMESTAMP '2024-01-01 00:00:00' AS a, TIMESTAMP '2024-01-01 00:00:00' - TIMESTAMP 'infinity' AS b")
	b.add("tsdiff/infInf", "SELECT TIMESTAMP 'infinity' - TIMESTAMP 'infinity' AS a")
	// age: years and months.
	b.add("age/basic", "SELECT age(TIMESTAMP '2024-03-31 00:00:00', TIMESTAMP '2024-01-31 00:00:00') AS a, age(TIMESTAMP '2024-03-01 00:00:00', TIMESTAMP '2024-01-31 00:00:00') AS b, age(TIMESTAMP '2024-01-31 00:00:00', TIMESTAMP '2024-03-01 00:00:00') AS c")
	b.add("age/time", "SELECT age(TIMESTAMP '2025-02-28 01:00:00', TIMESTAMP '2024-02-29 02:00:00') AS a, age(TIMESTAMP '2024-01-01 00:00:00.000001', TIMESTAMP '2024-01-01 00:00:00') AS b")
	// Round trips that do not hold.
	b.add("trip/monthEnd", "SELECT (TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '1 mon') - INTERVAL '1 mon' AS a, (TIMESTAMP '2024-03-31 00:00:00' - INTERVAL '1 mon') + INTERVAL '1 mon' AS b")
	b.add("trip/diff", "SELECT TIMESTAMP '2024-01-31 00:00:00' + (TIMESTAMP '2024-03-01 00:00:00' - TIMESTAMP '2024-01-31 00:00:00') AS a")
	b.add("trip/age", "SELECT TIMESTAMP '2024-01-31 00:00:00' + age(TIMESTAMP '2024-03-30 00:00:00', TIMESTAMP '2024-01-31 00:00:00') AS a")
	b.add("trip/compareVsApply", "SELECT INTERVAL '1 mon' = INTERVAL '30 days' AS eq, TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '1 mon' = TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '30 days' AS same")
	b.add("trip/assoc", "SELECT TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '1 mon' + INTERVAL '1 mon' AS a, TIMESTAMP '2024-01-31 00:00:00' + (INTERVAL '1 mon' + INTERVAL '1 mon') AS b")
	// Rows: a timestamp column ± a row's interval.
	b.ddl("ddl/create/iv80_ts", "CREATE TABLE iv80_ts (id BIGINT, ts TIMESTAMP, s TEXT)")
	b.ddl("ddl/insert/iv80_ts", "INSERT INTO iv80_ts VALUES (1, TIMESTAMP '2024-01-31 10:00:00', '1 mon'), (2, TIMESTAMP '2024-02-29 23:00:00', '1 day 02:00:00'), (3, TIMESTAMP '2023-12-31 00:00:00', '-1 year'), (4, TIMESTAMP '2024-03-01 12:00:00', '00:00:00.000001'), (5, NULL, '1 day'), (6, TIMESTAMP '2024-01-01 00:00:00', NULL)")
	b.add("rows/plus", "SELECT id, ts + CAST(s AS INTERVAL) AS a, ts - CAST(s AS INTERVAL) AS b FROM iv80_ts ORDER BY id")
	b.add("rows/diff", "SELECT a.id AS l, b.id AS r, a.ts - b.ts AS d FROM iv80_ts a, iv80_ts b WHERE a.id < b.id AND a.id <= 2 ORDER BY l, r")
	return b.out
}

// --- 7. Functions and operators ------------------------------------------

func iv80Functions() []iv80Cell {
	b := &iv80Builder{}
	full := "INTERVAL '1 year 14 mons 40 days 26:07:08.5'"
	// extract (numeric) and date_part (float8) of every field.
	for _, f := range []string{"millennium", "century", "decade", "year", "quarter", "month", "week", "day", "hour", "minute", "second", "millisecond", "microsecond", "epoch", "dow", "doy", "isoyear", "julian", "timezone"} {
		b.add("extract/"+f, "SELECT extract("+f+" FROM "+full+") AS e, date_part('"+f+"', "+full+") AS p, extract("+f+" FROM INTERVAL '-1 year -1 mon -1 day -00:00:01.5') AS n")
	}
	b.add("extract/inf", "SELECT extract(epoch FROM INTERVAL 'infinity') AS e, extract(day FROM INTERVAL 'infinity') AS d, extract(year FROM INTERVAL '-infinity') AS y, date_part('hour', INTERVAL 'infinity') AS h")
	b.add("extract/null", "SELECT extract(day FROM CAST(NULL AS INTERVAL)) AS e")
	b.add("extract/bigEpoch", "SELECT extract(epoch FROM INTERVAL '178956970 years 7 mons 2147483647 days 2562047788:00:54.775807') AS e")
	// date_trunc over an interval.
	for _, u := range []string{"millennium", "century", "decade", "year", "quarter", "month", "week", "day", "hour", "minute", "second", "milliseconds", "microseconds"} {
		b.add("trunc/"+u, "SELECT date_trunc('"+u+"', "+full+") AS a, date_trunc('"+u+"', INTERVAL '-1 year -14 mons -40 days -26:07:08.5') AS b")
	}
	b.add("trunc/inf", "SELECT date_trunc('day', INTERVAL 'infinity') AS a")
	b.add("trunc/bogus", "SELECT date_trunc('bogus', INTERVAL '1 day') AS a")
	// date_bin.
	b.add("bin/15min", "SELECT date_bin(INTERVAL '15 minutes', TIMESTAMP '2024-01-01 10:37:00', TIMESTAMP '2024-01-01 00:00:00') AS a, date_bin(INTERVAL '15 minutes', TIMESTAMP '2023-12-31 23:52:00', TIMESTAMP '2024-01-01 00:00:00') AS b")
	b.add("bin/days", "SELECT date_bin(INTERVAL '2 days 12:00:00', TIMESTAMP '2024-01-06 00:00:00', TIMESTAMP '2024-01-01 00:00:00') AS a")
	b.add("bin/month", "SELECT date_bin(INTERVAL '1 mon', TIMESTAMP '2024-01-06 00:00:00', TIMESTAMP '2024-01-01 00:00:00') AS a")
	b.add("bin/zero", "SELECT date_bin(INTERVAL '0', TIMESTAMP '2024-01-06 00:00:00', TIMESTAMP '2024-01-01 00:00:00') AS a")
	b.add("bin/neg", "SELECT date_bin(INTERVAL '-1 hour', TIMESTAMP '2024-01-06 00:30:00', TIMESTAMP '2024-01-01 00:00:00') AS a")
	b.add("bin/us", "SELECT date_bin(INTERVAL '00:00:00.000003', TIMESTAMP '2024-01-01 00:00:00.000005', TIMESTAMP '2024-01-01 00:00:00') AS a")
	// generate_series with an interval step.
	b.add("series/month", "SELECT g FROM generate_series(TIMESTAMP '2024-01-31 00:00:00', TIMESTAMP '2024-06-30 00:00:00', INTERVAL '1 mon') AS t(g) ORDER BY g")
	b.add("series/mixed", "SELECT g FROM generate_series(TIMESTAMP '2024-01-30 00:00:00', TIMESTAMP '2024-04-30 00:00:00', INTERVAL '1 mon 1 day') AS t(g) ORDER BY g")
	b.add("series/neg", "SELECT g FROM generate_series(TIMESTAMP '2024-01-02 00:00:00', TIMESTAMP '2024-01-01 00:00:00', INTERVAL '-12 hours') AS t(g) ORDER BY g")
	b.add("series/date", "SELECT g FROM generate_series(DATE '2024-02-27', DATE '2024-03-01', INTERVAL '1 day') AS t(g) ORDER BY g")
	b.add("series/zero", "SELECT count(*) AS n FROM generate_series(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-02 00:00:00', INTERVAL '0')")
	b.add("series/zeroNormalized", "SELECT count(*) AS n FROM generate_series(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-03-02 00:00:00', INTERVAL '1 mon -30 days')")
	b.add("series/inf", "SELECT count(*) AS n FROM generate_series(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-02 00:00:00', INTERVAL 'infinity')")
	b.set("series/tzDst", "TimeZone=America/New_York", "SELECT g FROM generate_series(TIMESTAMPTZ '2024-03-09 12:00:00-05', TIMESTAMPTZ '2024-03-11 12:00:00-04', INTERVAL '1 day') AS t(g) ORDER BY g")
	// to_char over an interval.
	b.add("tochar/hms", "SELECT to_char(INTERVAL '15 hours 2 minutes 12 seconds', 'HH24:MI:SS') AS a, to_char(INTERVAL '40 hours', 'HH24') AS b, to_char(INTERVAL '40 hours', 'HH12') AS c")
	b.add("tochar/days", "SELECT to_char(INTERVAL '1 year 2 mons 3 days', 'YYYY MM DD') AS a, to_char(INTERVAL '-1 day -02:00:00', 'DD HH24') AS b")
	b.add("tochar/us", "SELECT to_char(INTERVAL '00:00:01.123456', 'SS.US') AS a, to_char(INTERVAL '00:00:01.123456', 'MS') AS b")
	b.add("tochar/inf", "SELECT to_char(INTERVAL 'infinity', 'HH24') AS a")
	// isfinite and infinity.
	b.add("inf/isfinite", "SELECT isfinite(INTERVAL '1 day') AS a, isfinite(INTERVAL 'infinity') AS b, isfinite(INTERVAL '-infinity') AS c, isfinite(CAST(NULL AS INTERVAL)) AS d")
	b.add("inf/order", "SELECT v FROM (VALUES (INTERVAL 'infinity'), (INTERVAL '-infinity'), (INTERVAL '178956970 years 7 mons'), (INTERVAL '0')) t(v) ORDER BY v")
	b.add("inf/fields", "SELECT extract(month FROM INTERVAL 'infinity') AS m, justify_days(INTERVAL 'infinity') AS j, CAST(INTERVAL 'infinity' AS TEXT) AS t")
	// OVERLAPS with an interval end.
	b.add("overlaps/interval", "SELECT (TIMESTAMP '2024-01-01 00:00:00', INTERVAL '1 day') OVERLAPS (TIMESTAMP '2024-01-01 23:00:00', INTERVAL '1 hour') AS a, (TIMESTAMP '2024-01-01 00:00:00', INTERVAL '1 day') OVERLAPS (TIMESTAMP '2024-01-02 00:00:00', INTERVAL '1 hour') AS b, (DATE '2024-01-01', INTERVAL '-1 day') OVERLAPS (DATE '2023-12-31', DATE '2023-12-31') AS c")
	b.add("overlaps/null", "SELECT (TIMESTAMP '2024-01-01 00:00:00', CAST(NULL AS INTERVAL)) OVERLAPS (TIMESTAMP '2024-01-01 00:00:00', INTERVAL '1 hour') AS a")
	// RANGE frames with an interval offset.
	b.ddl("ddl/create/iv80_tz", "CREATE TABLE iv80_tz (id BIGINT, ts TIMESTAMP, s TEXT)")
	b.ddl("ddl/insert/iv80_tz", "INSERT INTO iv80_tz VALUES (1, TIMESTAMP '2024-01-01 00:00:00', '1 day'), (2, TIMESTAMP '2024-01-01 12:00:00', '24:00:00'), (3, TIMESTAMP '2024-01-02 00:00:00', '1 mon'), (4, TIMESTAMP '2024-01-31 00:00:00', '30 days'), (5, TIMESTAMP '2024-03-01 00:00:00', '2 days'), (6, NULL, NULL)")
	b.add("frame/tsDay", "SELECT id, count(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) AS n FROM iv80_tz ORDER BY id")
	b.add("frame/tsMonth", "SELECT id, count(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1 mon' PRECEDING AND CURRENT ROW) AS n FROM iv80_tz ORDER BY id")
	b.add("frame/tsFollowing", "SELECT id, count(*) OVER (ORDER BY ts RANGE BETWEEN CURRENT ROW AND INTERVAL '12 hours' FOLLOWING) AS n FROM iv80_tz ORDER BY id")
	b.add("frame/interval", "SELECT id, count(*) OVER (ORDER BY CAST(s AS INTERVAL) RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) AS n FROM iv80_tz ORDER BY id")
	b.add("frame/negOffset", "SELECT id, count(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '-1 day' PRECEDING AND CURRENT ROW) AS n FROM iv80_tz ORDER BY id")
	b.add("frame/date", "SELECT d, count(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) AS n FROM (VALUES (DATE '2024-01-01'), (DATE '2024-01-02'), (DATE '2024-01-04')) t(d) ORDER BY d")
	b.add("widthBucket", "SELECT width_bucket(INTERVAL '5 days', INTERVAL '0', INTERVAL '10 days', 2) AS a")
	b.add("widthBucketArray", "SELECT width_bucket(INTERVAL '5 days', ARRAY[INTERVAL '1 day', INTERVAL '1 mon']) AS a")
	// Aggregates over an interval argument.
	b.add("agg/all", "SELECT count(v) AS c, min(v) AS lo, max(v) AS hi, sum(v) AS s, avg(v) AS a FROM (VALUES (INTERVAL '1 mon'), (INTERVAL '-1 day 02:00:00'), (CAST(NULL AS INTERVAL)), (INTERVAL '00:00:00.000001')) t(v)")
	b.add("agg/stringAgg", "SELECT string_agg(CAST(v AS TEXT), ',' ORDER BY v) AS a FROM (VALUES (INTERVAL '1 mon'), (INTERVAL '29 days')) t(v)")
	b.add("agg/arrayAgg", "SELECT array_agg(v ORDER BY v) AS a FROM (VALUES (INTERVAL '1 mon'), (INTERVAL '29 days')) t(v)")
	b.add("agg/modeTie", "SELECT mode() WITHIN GROUP (ORDER BY v) AS a FROM (VALUES (INTERVAL '30 days'), (INTERVAL '1 mon')) t(v)")
	// Non-interval operands: implicit casts PostgreSQL accepts, and refusals.
	for _, o := range []struct{ k, sql string }{
		{"eqInt", "INTERVAL '1 day' = 1"},
		{"plusInt", "INTERVAL '1 day' + 1"},
		{"plusNumeric", "INTERVAL '1 day' + 1.5"},
		{"timesInterval", "INTERVAL '1 day' * INTERVAL '1 day'"},
		{"divInterval", "INTERVAL '1 day' / INTERVAL '1 hour'"},
		{"numberDiv", "2 / INTERVAL '1 day'"},
		{"mod", "INTERVAL '1 day' % 2"},
		{"plusDate", "INTERVAL '1 day' + DATE '2024-01-01'"},
		{"plusTime", "INTERVAL '1 hour' + TIME '23:30:00'"},
		{"plusTimestamptz", "INTERVAL '1 day' + TIMESTAMPTZ '2024-01-01 00:00:00+00'"},
		{"eqUnknown", "INTERVAL '1 day' = '24 hours'"},
		{"eqUnknownBad", "INTERVAL '1 day' = 'abc'"},
		{"timesUnknown", "INTERVAL '1 day' * '2'"},
		{"concatText", "'x' || INTERVAL '1 day'"},
		{"abs", "abs(INTERVAL '-1 day')"},
		{"greatestTs", "GREATEST(INTERVAL '1 day', TIMESTAMP '2024-01-01 00:00:00')"},
		{"coalesceText", "COALESCE(INTERVAL '1 day', CAST('2 days' AS TEXT))"},
		{"eqText", "INTERVAL '1 day' = CAST('24 hours' AS TEXT)"},
		{"eqTime", "INTERVAL '01:00:00' = TIME '01:00:00'"},
		{"minusFromDate", "INTERVAL '1 day' - DATE '2024-01-01'"},
		{"unaryPlus", "+INTERVAL '1 day'"},
		{"castBool", "CAST(INTERVAL '1 day' AS BOOLEAN)"},
	} {
		b.add("operand/"+o.k, "SELECT "+o.sql+" AS a")
	}
	return b.out
}

// --- 9. Generated values for the reference model -------------------------

// iv80ModelValue is one generated interval: its fields and its input text.
type iv80ModelValue struct {
	months, days int32
	micros       int64
}

func (v iv80ModelValue) text() string {
	return strconv.FormatInt(int64(v.months), 10) + " mons " + strconv.FormatInt(int64(v.days), 10) + " days " +
		strconv.FormatInt(v.micros, 10) + " microseconds"
}

// iv80ModelValues are deterministic generated intervals built to collide
// under normalization: each base normalized value is split into fields in
// several ways (months against 30 days, days against 86400 s), with mixed
// signs, microseconds, and values beyond 2^63 microseconds. A splitmix64
// stream keeps the set fixed without math/rand's version drift.
func iv80ModelValues() []iv80ModelValue {
	state := uint64(0x1680b)
	next := func() uint64 {
		state += 0x9e3779b97f4a7c15
		z := state
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		return z ^ (z >> 31)
	}
	rng := func(n int64) int64 { return int64(next() % uint64(n)) }
	var out []iv80ModelValue
	for len(out) < 96 {
		m := int32(rng(41) - 20)
		d := int32(rng(121) - 60)
		us := (rng(200001) - 100000) * 1000000 / 7
		if rng(4) == 0 {
			us = rng(3) - 1
		}
		base := iv80ModelValue{m, d, us}
		out = append(out, base)
		// Re-split: move k months into 30k days, j days into j*86400e6 us.
		k := int32(rng(5) - 2)
		j := int32(rng(5) - 2)
		out = append(out, iv80ModelValue{base.months - k, base.days + 30*k - j, base.micros + int64(j)*86400000000})
		if rng(3) == 0 {
			out = append(out, iv80ModelValue{base.months + 1, base.days - 30, base.micros})
		}
	}
	// Large values: months near the bound against days and microseconds.
	out = append(out,
		iv80ModelValue{2147483647, 0, 0}, iv80ModelValue{2147483646, 30, 0}, iv80ModelValue{2147483646, 29, 86400000000},
		iv80ModelValue{-2147483647, 0, 0}, iv80ModelValue{-2147483646, -30, 0}, iv80ModelValue{0, 213503982, 28909551616},
		iv80ModelValue{0, 0, 0}, iv80ModelValue{0, 106751991, 14454775808}, iv80ModelValue{0, 106751991, 14454775807},
		iv80ModelValue{7116612, 15, 14454775808}, iv80ModelValue{0, -213503982, -28909551616},
	)
	return out
}

func iv80ModelTable() string {
	var vals []string
	for i, v := range iv80ModelValues() {
		vals = append(vals, "("+strconv.Itoa(i+1)+", '"+v.text()+"')")
	}
	return strings.Join(vals, ", ")
}

func iv80Model() []iv80Cell {
	b := &iv80Builder{}
	b.ddl("ddl/create/iv80_mv", "CREATE TABLE iv80_mv (id BIGINT, s TEXT)")
	b.ddl("ddl/insert/iv80_mv", "INSERT INTO iv80_mv VALUES "+iv80ModelTable())
	c := "CAST(s AS INTERVAL)"
	b.add("model/fields", "SELECT id, extract(month FROM "+c+") + 12 * extract(year FROM "+c+") AS m, extract(day FROM "+c+") AS d, extract(hour FROM "+c+") * 3600000000 + extract(minute FROM "+c+") * 60000000 + extract(microsecond FROM "+c+") AS us FROM iv80_mv ORDER BY id")
	b.add("model/order", "SELECT id FROM iv80_mv ORDER BY "+c+", id")
	b.add("model/groups", "SELECT min(id) AS lo, count(*) AS n, string_agg(CAST(id AS TEXT), ' ' ORDER BY id) AS ids FROM iv80_mv GROUP BY "+c+" ORDER BY lo")
	b.set("model/groupsSort", "enable_hashagg=off", "SELECT min(id) AS lo, count(*) AS n, string_agg(CAST(id AS TEXT), ' ' ORDER BY id) AS ids FROM iv80_mv GROUP BY "+c+" ORDER BY lo")
	b.add("model/hash", "SELECT id, interval_hash("+c+") AS h FROM iv80_mv ORDER BY id")
	return b.out
}
