// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"os"
	"regexp"
	"strings"
)

// #1680b round 2: the cells the judge's review asked for (1680b_codex_judge.md
// item 2), the cast-wrapped executable variants of every statement that
// publishes a bare interval, and the write statements. Measured and recorded
// like the rest of the corpus (arc_1680b_interval_corpus_test.go).

// iv80WriteTables are the tables the write cells create (dropped first on
// the reference).
var iv80WriteTables = []string{"iv80_wt", "iv80_wc1", "iv80_wc2", "iv80_wc3", "iv80_wc4", "iv80_wc5", "iv80_wc6", "iv80_wc7"}

// --- Cast-wrapped variants ------------------------------------------------

// iv80CastSection is the section of base's cast-wrapped variants: for every
// statement of base whose PostgreSQL answer publishes an interval column, the
// same statement with each such select-list item wrapped in CAST(… AS TEXT),
// named "<name>~text", same settings. The bare statement stays a cell of its
// own (refused in this stage: a bare interval result is not published,
// architect's ruling 1); the variant is the executable form a stage can
// support. Only a statement whose select list can be rewritten without
// changing its meaning gets one (iv80CastVariant).
func iv80CastSection(base iv80Section) iv80Section {
	return iv80Section{stem: base.stem + "_cast", cells: func() []iv80Cell {
		raw, err := os.ReadFile(iv80Path(base.stem, "_pg17.tsv"))
		if err != nil {
			return nil
		}
		answers := map[string]string{}
		for _, line := range strings.Split(string(raw), "\n") {
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if name, ans, ok := strings.Cut(line, "\t"); ok {
				answers[name] = iv80Unescape(ans)
			}
		}
		var out []iv80Cell
		for _, c := range base.cells() {
			if c.ddl || c.pgOnly {
				continue
			}
			if sql, ok := iv80CastVariant(c.sql, answers[c.name]); ok {
				out = append(out, iv80Cell{name: c.name + "~text", sql: sql, settings: c.settings})
			}
		}
		return out
	}}
}

// iv80HeaderTypes is the column types of a PostgreSQL answer's header ("" for
// an error).
func iv80HeaderTypes(ans string) []string {
	ans, _ = iv80Notice(ans)
	if strings.HasPrefix(ans, "ERR ") {
		return nil
	}
	head, _, ok := strings.Cut(ans, " | ")
	if !ok {
		return nil
	}
	var out []string
	for _, col := range strings.Split(head, ",") {
		_, typ, _ := strings.Cut(col, ":")
		if i := strings.LastIndexByte(typ, '('); i >= 0 {
			typ = typ[:i]
		}
		out = append(out, typ)
	}
	return out
}

// iv80TopLevel splits s at sep where sep is outside parentheses and quotes.
func iv80TopLevel(s, sep string) []string {
	var out []string
	depth, start := 0, 0
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch ch := s[i]; {
		case ch == '\'':
			inQuote = !inQuote
		case inQuote:
		case ch == '(':
			depth++
		case ch == ')':
			depth--
		case depth == 0 && strings.HasPrefix(s[i:], sep):
			out = append(out, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	return append(out, s[start:])
}

// iv80CastVariant rewrites "SELECT item AS alias, … [FROM …]" so that each
// item PostgreSQL types interval (or interval[]) reads CAST(item AS TEXT) AS
// alias. It declines when the rewrite could change the statement's meaning:
// no answer or an error answer, no interval column, SELECT DISTINCT, an item
// without an alias, a column count that does not match, or an alias the rest
// of the statement names (an ORDER BY or GROUP BY of the output column would
// then sort or group text).
func iv80CastVariant(sql, ans string) (string, bool) {
	types := iv80HeaderTypes(ans)
	if len(types) == 0 || !strings.HasPrefix(sql, "SELECT ") || strings.HasPrefix(sql, "SELECT DISTINCT") {
		return "", false
	}
	for _, setOp := range []string{" UNION ", " INTERSECT ", " EXCEPT "} {
		if len(iv80TopLevel(sql, setOp)) > 1 {
			// Wrapping one arm's column would change the set operation's
			// column type, not only its display.
			return "", false
		}
	}
	body := strings.TrimPrefix(sql, "SELECT ")
	parts := iv80TopLevel(body, " FROM ")
	list, rest := parts[0], ""
	if len(parts) > 1 {
		rest = " FROM " + strings.Join(parts[1:], " FROM ")
	}
	items := iv80TopLevel(list, ", ")
	if len(items) != len(types) {
		return "", false
	}
	changed := false
	for i, it := range items {
		if types[i] != "interval" && types[i] != "interval[]" {
			continue
		}
		pieces := iv80TopLevel(it, " AS ")
		if len(pieces) < 2 {
			return "", false
		}
		alias := pieces[len(pieces)-1]
		expr := strings.Join(pieces[:len(pieces)-1], " AS ")
		if rest != "" && regexp.MustCompile(`\b`+regexp.QuoteMeta(alias)+`\b`).MatchString(rest) {
			return "", false
		}
		items[i] = "CAST(" + expr + " AS TEXT) AS " + alias
		changed = true
	}
	if !changed {
		return "", false
	}
	return "SELECT " + strings.Join(items, ", ") + rest, true
}

// --- 10. Definitions: the discriminating scalar cells ---------------------

func iv80Definitions() []iv80Cell {
	b := &iv80Builder{}
	lit := func(name, s string) { b.add(name, "SELECT INTERVAL '"+s+"' AS a") }
	pair := func(name, x, y string) { b.add(name, "SELECT INTERVAL '"+x+"' AS a, INTERVAL '"+y+"' AS b") }
	inf := "CAST('Infinity' AS DOUBLE PRECISION)"

	// Input boundaries: the signed integer unit path against the signed
	// clock path; the reserved triples spelled as fields.
	lit("in/usMinUnit", "-9223372036854775808 microseconds")
	lit("in/usMaxUnit", "9223372036854775807 microseconds")
	lit("in/usOverUnit", "9223372036854775808 microseconds")
	lit("in/usUnderUnit", "-9223372036854775809 microseconds")
	lit("in/usMinUnitAgo", "9223372036854775807 microseconds ago")
	lit("in/allMin", "-2147483648 months -2147483648 days -9223372036854775808 microseconds")
	lit("in/allMinNear", "-2147483648 months -2147483648 days -9223372036854775807 microseconds")
	lit("in/allMaxUnits", "2147483647 months 2147483647 days 9223372036854775807 microseconds")
	lit("in/allMaxNear", "2147483647 months 2147483647 days 9223372036854775806 microseconds")
	b.add("in/allMinIsNegInf", "SELECT INTERVAL '-2147483648 months -2147483648 days -9223372036854775808 microseconds' = INTERVAL '-infinity' AS eq, "+
		"isfinite(INTERVAL '-2147483648 months -2147483648 days -9223372036854775808 microseconds') AS fin, "+
		"isfinite(INTERVAL '-2147483648 months -2147483648 days -9223372036854775807 microseconds') AS fin_near")
	b.add("in/allMinCastText", "SELECT CAST('-2147483648 months -2147483648 days -9223372036854775808 microseconds' AS INTERVAL) AS a")
	b.add("in/allMinMake", "SELECT make_interval(months => -2147483648, days => -2147483648) AS a")

	// Input rounding: a unit's fraction (AdjustFractMicroseconds, > 0.5
	// strictly) against a clock's fraction (ParseFractionalSecond, rint).
	pair("frac/1.5us", "00:00:00.0000015", "0.0000015 seconds")
	pair("frac/-1.5us", "-00:00:00.0000015", "-0.0000015 seconds")
	pair("frac/2.5us", "00:00:00.0000025", "0.0000025 seconds")
	pair("frac/3.5us", "00:00:00.0000035", "0.0000035 seconds")
	pair("frac/0.5us", "00:00:00.0000005", "0.0000005 seconds")
	pair("frac/clockNearSecond", "00:00:01.4999995", "1.4999995 seconds")
	pair("frac/minuteUnit", "0.0000000125 minutes", "00:00:00.00000075")
	// Year fractions round to whole months by rint (half to even); a month
	// fraction becomes days and the day fraction microseconds.
	pair("frac/years1", "0.125 years", "0.375 years")
	pair("frac/years2", "-0.125 years", "0.04 years")
	pair("frac/decade", "0.0125 decades", "0.00125 centuries")
	pair("frac/months", "0.5 months", "-0.0333333 months")
	// A week's fraction becomes days first (AdjustFractDays(fval, 7)).
	pair("frac/weeks1", "1.5 weeks", "0.1 weeks")
	pair("frac/weeks2", "-0.5 weeks", "1 week 0.5 days")

	// ago: after every form?
	for _, s := range []string{"P1D ago", "P1Y2M ago", "infinity ago", "-infinity ago", "1 day 02:00:00 ago",
		"1-2 ago", "3 4:05:06 ago", "-1 day +02:00:00 ago", "1 day ago ago", "ago", "2147483647 days ago",
		"-2147483648 days ago", "-2147483648 months ago", "@ 1 mon ago"} {
		lit("ago/"+s, s)
	}

	// Typmods change interpretation, not only truncation.
	for _, q := range []struct{ k, sql string }{
		{"bareHM", "INTERVAL '1:30'"},
		{"hm", "INTERVAL '1:30' HOUR TO MINUTE"},
		{"ms", "INTERVAL '1:30' MINUTE TO SECOND"},
		{"dm", "INTERVAL '1:30' DAY TO MINUTE"},
		{"bareNumber", "INTERVAL '90'"},
		{"minute", "INTERVAL '90' MINUTE"},
		{"dayToHour", "INTERVAL '2' DAY TO HOUR"},
		{"yearToMonth", "INTERVAL '2' YEAR TO MONTH"},
		{"hourFrac", "INTERVAL '1.5' HOUR TO MINUTE"},
		{"secondTyp5", "INTERVAL '5' MINUTE TO SECOND"},
	} {
		b.add("typmod/"+q.k, "SELECT "+q.sql+" AS a")
	}
	// The precision clamp raises a WARNING (recorded with the answer).
	b.add("typmod/prec7", "SELECT CAST(INTERVAL '1.1234567 seconds' AS INTERVAL(7)) AS a")
	b.add("typmod/prec7Literal", "SELECT INTERVAL(7) '1.1234567 seconds' AS a")
	b.add("typmod/prec7Half", "SELECT CAST(INTERVAL '0.0000015 seconds' AS INTERVAL(7)) AS a, CAST(INTERVAL '00:00:00.0000015' AS INTERVAL(7)) AS b")

	// Arithmetic discriminators.
	b.add("ar/mulMixed", "SELECT INTERVAL '1 mon -1 day' * 1.5 AS a")
	b.add("ar/divMixed2", "SELECT INTERVAL '1 mon -1 day' / 2 AS a")
	b.add("ar/divMixed3", "SELECT INTERVAL '1 mon -1 day' / 3 AS a, INTERVAL '-1 mon 1 day' / 1.5 AS b")
	b.add("ar/mulCarry", "SELECT INTERVAL '1 mon 1 day' * 0.75 AS a, INTERVAL '1 mon 1 day' / (4.0 / 3.0) AS b")
	b.add("ar/infFactorZeroSign", "SELECT INTERVAL '1 mon -30 days' * "+inf+" AS a")
	b.add("ar/infFactorNegSign", "SELECT INTERVAL '1 mon -31 days' * "+inf+" AS a")
	b.add("ar/infFactorPosSign", "SELECT INTERVAL '-1 mon 31 days' * "+inf+" AS a")
	b.add("ar/negInfFactorNegSign", "SELECT INTERVAL '1 mon -31 days' * CAST('-Infinity' AS DOUBLE PRECISION) AS a")
	b.add("ar/infDivFiniteSign", "SELECT INTERVAL '1 mon -31 days' / "+inf+" AS a")
	// Division divides each component; it is not a multiplication by the
	// reciprocal (the months pair discriminates, the judge's pair does not).
	b.add("ar/divRecipMonths", "SELECT INTERVAL '49 mons' / 49 AS a, INTERVAL '49 mons' * (CAST(1 AS DOUBLE PRECISION) / 49) AS b")
	b.add("ar/divRecipDays", "SELECT INTERVAL '49 days' / 49 AS a, INTERVAL '49 days' * (CAST(1 AS DOUBLE PRECISION) / 49) AS b")
	b.add("ar/divRecipJudge", "SELECT INTERVAL '9007199254740993 microseconds' / 3 AS a, INTERVAL '9007199254740993 microseconds' * (CAST(1 AS DOUBLE PRECISION) / 3) AS b")
	// justify_interval is its own algorithm (the pre-normalization of days
	// before hours avoids an overflow the compositions hit).
	for _, j := range []struct{ k, iv string }{
		{"bigDays24h", "2147483647 days 24 hours"},
		{"negBigDays24h", "-2147483648 days -24 hours"},
		{"monMinus25h", "1 mon -25 hours"},
		{"negMonPlus25h", "-1 mon 25 hours"},
	} {
		l := "INTERVAL '" + j.iv + "'"
		b.add("justify/"+j.k, "SELECT justify_interval("+l+") AS i")
		b.add("justify/"+j.k+"/hoursOfDays", "SELECT justify_hours(justify_days("+l+")) AS hd")
		b.add("justify/"+j.k+"/daysOfHours", "SELECT justify_days(justify_hours("+l+")) AS dh")
	}
	// Opposing infinities error; one infinity wins over a finite operand.
	for _, c := range []struct{ k, sql string }{
		{"tsInfPlusNegInf", "TIMESTAMP 'infinity' + INTERVAL '-infinity'"},
		{"tsNegInfPlusInf", "TIMESTAMP '-infinity' + INTERVAL 'infinity'"},
		{"tsNegInfMinusNegInf", "TIMESTAMP '-infinity' - INTERVAL '-infinity'"},
		{"tsNegInfMinusInf", "TIMESTAMP '-infinity' - INTERVAL 'infinity'"},
		{"tsInfPlusFinite", "TIMESTAMP 'infinity' + INTERVAL '-1 day'"},
		{"tsInfPlusPosFinite", "TIMESTAMP 'infinity' + INTERVAL '1 day'"},
		{"tsNegInfMinusFinite", "TIMESTAMP '-infinity' - INTERVAL '1 mon'"},
		{"tsInfAlone", "TIMESTAMP 'infinity'"},
		{"dateInfPlusFinite", "DATE 'infinity' - INTERVAL '1 day'"},
		{"dateInfPlusNegInf", "DATE 'infinity' + INTERVAL '-infinity'"},
		{"dateFinitePlusInf", "DATE '2024-01-01' + INTERVAL 'infinity'"},
		{"ivInfMinusNegInf", "INTERVAL 'infinity' - INTERVAL '-infinity'"},
	} {
		b.add("infop/"+c.k, "SELECT "+c.sql+" AS a")
	}
	// EXTRACT epoch is not the comparison value: a year is 365.25 days in
	// the epoch, 360 days in N.
	b.add("epoch/yearVs360", "SELECT extract(epoch FROM INTERVAL '1 year') AS y, extract(epoch FROM INTERVAL '360 days') AS d, INTERVAL '1 year' = INTERVAL '360 days' AS eq, "+
		"extract(epoch FROM INTERVAL '12 mons') AS m12, extract(epoch FROM INTERVAL '13 mons') AS m13, extract(epoch FROM INTERVAL '1 mon') AS m1, extract(epoch FROM INTERVAL '-13 mons') AS mneg")
	b.add("epoch/yearVs360Float", "SELECT date_part('epoch', INTERVAL '1 year') AS y, date_part('epoch', INTERVAL '360 days') AS d")
	b.add("epoch/bigFinite", "SELECT extract(epoch FROM INTERVAL '2147483647 months 2147483647 days 9223372036854775806 microseconds') AS e, date_part('epoch', INTERVAL '2147483647 months 2147483647 days 9223372036854775806 microseconds') AS p")
	b.add("epoch/bigFiniteNeg", "SELECT extract(epoch FROM INTERVAL '-2147483648 months -2147483648 days -9223372036854775807 microseconds') AS e")
	b.add("epoch/negInf", "SELECT extract(epoch FROM INTERVAL '-infinity') AS e, date_part('epoch', INTERVAL '-infinity') AS p")
	// Finite arithmetic that would produce the negative sentinel refuses.
	b.add("sentinel/minusToNegInf", "SELECT INTERVAL '-2147483648 months -2147483648 days -9223372036854775807 microseconds' - INTERVAL '00:00:00.000001' AS a")
	b.add("sentinel/plusToNegInf", "SELECT INTERVAL '-2147483648 months -2147483648 days' + INTERVAL '-9223372036854775808 microseconds' AS a")
	b.add("sentinel/sumToNegInf", "SELECT sum(v) AS s FROM (VALUES (INTERVAL '-2147483648 months -2147483648 days'), (INTERVAL '-9223372036854775808 microseconds')) t(v)")
	b.add("sentinel/negOfNearMax", "SELECT -INTERVAL '2147483647 months 2147483647 days 9223372036854775806 microseconds' AS a")
	b.add("sentinel/negOfNearMin", "SELECT -INTERVAL '-2147483648 months -2147483647 days -9223372036854775807 microseconds' AS a")
	b.add("sentinel/negOfInfinityTriple", "SELECT -INTERVAL '2147483647 months 2147483647 days 9223372036854775807 microseconds' AS a")
	b.add("sentinel/mulNegInfinityTriple", "SELECT INTERVAL '-2147483648 months -2147483648 days -9223372036854775808 microseconds' * 1 AS a")

	// age: borrowing from the EARLIER timestamp's month, sign restoration,
	// infinities, NULL, BC, the date and timestamptz overloads.
	ts := func(s string) string { return "TIMESTAMP '" + s + "'" }
	b.add("age/borrow", "SELECT age("+ts("2023-03-01")+", "+ts("2023-01-31")+") AS a, age("+ts("2023-01-31")+", "+ts("2023-03-01")+") AS b, "+
		"age("+ts("2024-03-01")+", "+ts("2024-02-15")+") AS c, age("+ts("2024-02-15")+", "+ts("2024-03-01")+") AS d, age("+ts("2024-03-15")+", "+ts("2024-01-31")+") AS e")
	b.add("age/timeBorrow", "SELECT age("+ts("2024-03-01 00:00:00")+", "+ts("2024-02-29 23:00:00")+") AS a, age("+ts("2024-02-29 23:00:00")+", "+ts("2024-03-01 00:00:00")+") AS b, "+
		"age("+ts("2024-01-01 00:00:00")+", "+ts("2023-12-31 23:59:59.999999")+") AS c")
	b.add("age/null", "SELECT age(CAST(NULL AS TIMESTAMP), "+ts("2024-01-01")+") AS a, age("+ts("2024-01-01")+", CAST(NULL AS TIMESTAMP)) AS b")
	b.add("age/infLeft", "SELECT age(TIMESTAMP 'infinity', "+ts("2024-01-01")+") AS a, age(TIMESTAMP '-infinity', "+ts("2024-01-01")+") AS b")
	b.add("age/infRight", "SELECT age("+ts("2024-01-01")+", TIMESTAMP 'infinity') AS a, age("+ts("2024-01-01")+", TIMESTAMP '-infinity') AS b")
	b.add("age/infOpposed", "SELECT age(TIMESTAMP 'infinity', TIMESTAMP '-infinity') AS a, age(TIMESTAMP '-infinity', TIMESTAMP 'infinity') AS b")
	b.add("age/infInf", "SELECT age(TIMESTAMP 'infinity', TIMESTAMP 'infinity') AS a")
	b.add("age/negInfNegInf", "SELECT age(TIMESTAMP '-infinity', TIMESTAMP '-infinity') AS a")
	b.add("age/bc", "SELECT age("+ts("0001-03-01 BC")+", "+ts("0001-01-31")+") AS a, age("+ts("0001-01-01")+", "+ts("0001-12-31 BC")+") AS b")
	b.add("age/date", "SELECT age(DATE '2024-03-01', DATE '2024-01-31') AS a")
	b.set("age/dateNY", "TimeZone=America/New_York", "SELECT age(DATE '2024-03-11', DATE '2024-03-09') AS a")
	// Round 3 (the second judge record, correction 1): the same date pairs
	// under both zones, and the overload PostgreSQL resolves: age(date,
	// date) is the timestamptz age of two local midnights, so a zone whose
	// midnight falls in a DST gap (America/Havana, 2024-03-10) answers
	// differently from UTC for one pair.
	b.set("age/dateUTC", "TimeZone=UTC", "SELECT age(DATE '2024-03-11', DATE '2024-03-09') AS a")
	b.set("age/dateMonthNY", "TimeZone=America/New_York", "SELECT age(DATE '2024-03-01', DATE '2024-01-31') AS a")
	b.set("age/dateGapUTC", "TimeZone=UTC", "SELECT age(DATE '2024-03-11', DATE '2024-03-10') AS a")
	b.set("age/dateGapHavana", "TimeZone=America/Havana", "SELECT age(DATE '2024-03-11', DATE '2024-03-10') AS a")
	b.pg("explain/age/date", "", "EXPLAIN (VERBOSE, COSTS OFF) SELECT age(DATE '2024-03-01', DATE '2024-01-31')")
	for _, z := range []string{"UTC", "America/New_York"} {
		b.set("age/tz/"+z+"/fallBack", "TimeZone="+z, "SELECT age(TIMESTAMPTZ '2024-11-03 12:00:00-05', TIMESTAMPTZ '2024-11-02 12:00:00-04') AS a, age(TIMESTAMPTZ '2024-11-02 12:00:00-04', TIMESTAMPTZ '2024-11-03 12:00:00-05') AS b")
	}

	// The result type of each date / timestamp / time / interval operator
	// pair (the header carries it), and the pairs PostgreSQL refuses.
	for _, c := range []struct{ k, sql string }{
		{"datePlusInt", "DATE '2024-01-31' + 1"},
		{"dateMinusInt", "DATE '2024-01-31' - 1"},
		{"intPlusDate", "1 + DATE '2024-01-31'"},
		{"dateMinusDate", "DATE '2024-03-01' - DATE '2024-01-31'"},
		{"datePlusInterval", "DATE '2024-01-31' + INTERVAL '1 mon'"},
		{"dateMinusInterval", "DATE '2024-01-31' - INTERVAL '1 day'"},
		{"intervalPlusDate", "INTERVAL '1 day' + DATE '2024-01-31'"},
		{"intervalMinusDate", "INTERVAL '1 day' - DATE '2024-01-31'"},
		{"datePlusTime", "DATE '2024-01-31' + TIME '01:02:03'"},
		{"dateMinusTime", "DATE '2024-01-31' - TIME '01:02:03'"},
		{"tsMinusTs", "TIMESTAMP '2024-01-02 01:00:00' - TIMESTAMP '2024-01-01 00:00:00'"},
		{"tsMinusDate", "TIMESTAMP '2024-01-02 01:00:00' - DATE '2024-01-01'"},
		{"dateMinusTs", "DATE '2024-01-01' - TIMESTAMP '2024-01-02 01:00:00'"},
		{"tsPlusInterval", "TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '1 mon'"},
		{"intervalPlusTs", "INTERVAL '1 mon' + TIMESTAMP '2024-01-31 00:00:00'"},
		{"tsMinusInterval", "TIMESTAMP '2024-01-31 00:00:00' - INTERVAL '1 mon'"},
		{"intervalMinusTs", "INTERVAL '1 mon' - TIMESTAMP '2024-01-31 00:00:00'"},
		{"tsPlusTs", "TIMESTAMP '2024-01-31 00:00:00' + TIMESTAMP '2024-01-31 00:00:00'"},
		{"datePlusDate", "DATE '2024-01-31' + DATE '2024-01-31'"},
		{"dateTimesInt", "DATE '2024-01-31' * 2"},
		{"tsPlusInt", "TIMESTAMP '2024-01-31 00:00:00' + 1"},
		{"timeMinusTime", "TIME '01:00:00' - TIME '23:00:00'"},
		{"timePlusInterval", "TIME '23:00:00' + INTERVAL '2 hours'"},
		{"intervalPlusTime", "INTERVAL '2 hours' + TIME '23:00:00'"},
		{"timeMinusInterval", "TIME '01:00:00' - INTERVAL '2 hours'"},
		{"tstzMinusTstz", "TIMESTAMPTZ '2024-01-02 00:00:00+00' - TIMESTAMPTZ '2024-01-01 00:00:00+00'"},
		{"tstzMinusTs", "TIMESTAMPTZ '2024-01-02 00:00:00+00' - TIMESTAMP '2024-01-01 00:00:00'"},
		{"tstzPlusInterval", "TIMESTAMPTZ '2024-01-02 00:00:00+00' + INTERVAL '1 day'"},
		{"intervalTimesInt", "INTERVAL '1 day' * 2"},
		{"intTimesInterval", "2 * INTERVAL '1 day'"},
		{"intervalDivInt", "INTERVAL '1 day' / 2"},
		{"negInterval", "-INTERVAL '1 day'"},
	} {
		b.add("types/"+c.k, "SELECT "+c.sql+" AS a")
	}

	// Operator and type closure (the judge's cells).
	b.add("closure/tsMinusDate", "SELECT TIMESTAMP '2024-01-02 01:00' - DATE '2024-01-01' AS a")
	b.add("closure/dateMinusTs", "SELECT DATE '2024-01-01' - TIMESTAMP '2024-01-02 01:00' AS a")
	b.add("closure/coalesceText", "SELECT CAST(COALESCE(CAST(NULL AS INTERVAL), '1 mon') AS TEXT) AS a")
	b.add("closure/coalesce", "SELECT COALESCE(CAST(NULL AS INTERVAL), '1 mon') AS a")
	b.add("closure/caseText", "SELECT CAST(CASE WHEN true THEN INTERVAL '1 mon' ELSE '30 days' END AS TEXT) AS a")
	b.add("closure/case", "SELECT CASE WHEN true THEN INTERVAL '1 mon' ELSE '30 days' END AS a")
	b.add("closure/nullifText", "SELECT CAST(NULLIF(INTERVAL '1 mon', INTERVAL '30 days') AS TEXT) AS a")
	b.add("closure/nullif", "SELECT NULLIF(INTERVAL '1 mon', INTERVAL '30 days') AS a, NULLIF(INTERVAL '1 mon', INTERVAL '31 days') AS b")
	b.add("closure/unionNull", "SELECT CAST(x AS TEXT) AS t FROM (SELECT CAST(NULL AS INTERVAL) AS x UNION ALL SELECT INTERVAL '1 day') q ORDER BY t NULLS FIRST")
	b.add("closure/whereFalse", "SELECT x FROM (SELECT CAST(NULL AS INTERVAL) AS x) q WHERE false")
	b.add("closure/whereFalseText", "SELECT CAST(x AS TEXT) AS t FROM (SELECT CAST(NULL AS INTERVAL) AS x) q WHERE false")
	b.add("closure/greatestMixed", "SELECT CAST(GREATEST(INTERVAL '1 mon', '30 days', NULL) AS TEXT) AS a, CAST(LEAST(INTERVAL '30 days', '1 mon') AS TEXT) AS b")

	// DST gap and fold TARGET times (timestamptz; refused in this program,
	// ruling 7, measured so the refusal is judged against PostgreSQL).
	ny := "TimeZone=America/New_York"
	b.set("dst/gapTarget", ny, "SELECT TIMESTAMPTZ '2024-03-09 02:30:00' + INTERVAL '1 day' AS a")
	b.set("dst/foldTarget", ny, "SELECT TIMESTAMPTZ '2024-11-02 01:30:00' + INTERVAL '1 day' AS a")
	b.set("dst/foldMinus", ny, "SELECT TIMESTAMPTZ '2024-11-04 01:30:00' - INTERVAL '1 day' AS a")
	b.set("dst/gapInput", ny, "SELECT TIMESTAMPTZ '2024-03-10 02:30:00' AS a")
	b.set("dst/monthIntoGap", ny, "SELECT TIMESTAMPTZ '2024-02-10 02:30:00' + INTERVAL '1 mon' AS a")
	b.set("dst/hoursAcrossFold", ny, "SELECT TIMESTAMPTZ '2024-11-03 00:30:00' + INTERVAL '1 hour' AS a, TIMESTAMPTZ '2024-11-03 00:30:00' + INTERVAL '2 hours' AS b")
	b.set("dst/dateAddZone", "TimeZone=UTC", "SELECT date_add(TIMESTAMPTZ '2024-03-09 02:30:00-05', INTERVAL '1 day', 'America/New_York') AS a, date_subtract(TIMESTAMPTZ '2024-11-04 01:30:00-05', INTERVAL '1 day', 'America/New_York') AS b")
	b.set("dst/seriesZone", "TimeZone=UTC", "SELECT g FROM generate_series(TIMESTAMPTZ '2024-11-02 01:30:00-04', TIMESTAMPTZ '2024-11-04 01:30:00-05', INTERVAL '1 day', 'America/New_York') AS t(g) ORDER BY g")
	b.set("dst/seriesNoZone", ny, "SELECT g FROM generate_series(TIMESTAMPTZ '2024-03-09 02:30:00', TIMESTAMPTZ '2024-03-11 02:30:00', INTERVAL '1 day') AS t(g) ORDER BY g")

	// The functions the definition assigns implement-or-refuse.
	b.add("fn/binInfStride", "SELECT date_bin(INTERVAL 'infinity', TIMESTAMP '2024-01-01 10:00:00', TIMESTAMP '2024-01-01 00:00:00') AS a")
	b.add("fn/binInfSource", "SELECT date_bin(INTERVAL '1 hour', TIMESTAMP 'infinity', TIMESTAMP '2024-01-01 00:00:00') AS a")
	b.add("fn/binDaysStride", "SELECT date_bin(INTERVAL '1 day 1 hour', TIMESTAMP '2024-01-03 00:00:00', TIMESTAMP '2024-01-01 00:00:00') AS a")
	b.add("fn/binBeforeOrigin", "SELECT date_bin(INTERVAL '1 hour', TIMESTAMP '2023-12-31 22:30:00', TIMESTAMP '2024-01-01 00:00:00') AS a")
	b.add("fn/truncMixedSign", "SELECT date_trunc('day', INTERVAL '1 mon -1 day 02:00:00') AS a, date_trunc('month', INTERVAL '-1 year 1 mon') AS b")
	b.add("fn/tocharNeg", "SELECT to_char(INTERVAL '-1 day -02:03:04', 'DD HH24:MI:SS') AS a, to_char(INTERVAL '1 mon 2 days', 'MM DD') AS b")
	b.add("fn/overlapsNegEnd", "SELECT (TIMESTAMP '2024-01-02 00:00:00', INTERVAL '-1 day') OVERLAPS (TIMESTAMP '2024-01-01 12:00:00', INTERVAL '1 hour') AS a")
	b.add("fn/overlapsInf", "SELECT (TIMESTAMP '2024-01-01 00:00:00', INTERVAL 'infinity') OVERLAPS (TIMESTAMP '9999-01-01 00:00:00', INTERVAL '1 hour') AS a")
	b.add("fn/seriesTsMonthEnd", "SELECT g FROM generate_series(TIMESTAMP '2024-01-31 00:00:00', TIMESTAMP '2024-04-30 00:00:00', INTERVAL '1 mon') AS t(g) ORDER BY g")
	b.add("fn/seriesNegStepWrongWay", "SELECT count(*) AS n FROM generate_series(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-02 00:00:00', INTERVAL '-1 hour')")
	b.add("fn/widthBucketNumeric", "SELECT width_bucket(extract(epoch FROM INTERVAL '5 days'), 0, 864000, 2) AS a")
	return b.out
}

// --- 11. Relational: runtime columns, keys, aggregates, windows ----------

var iv80RelTables = []string{"iv80_r", "iv80_rt", "iv80_ov", "iv80_n", "iv80_pm", "iv80_sw", "iv80_fr", "iv80_tp"}

func iv80Relational() []iv80Cell {
	b := &iv80Builder{}
	c := "CAST(s AS INTERVAL)"
	txt := func(e string) string { return "CAST(" + e + " AS TEXT)" }
	hashSort := []struct{ k, set string }{{"default", ""}, {"sortagg", "enable_hashagg=off"}}

	b.ddl("ddl/create/iv80_r", "CREATE TABLE iv80_r (id BIGINT, g BIGINT, s TEXT, ts TIMESTAMP)")
	b.ddl("ddl/insert/iv80_r", "INSERT INTO iv80_r VALUES (1, 1, '1 mon', TIMESTAMP '2024-01-31 00:00:00'), (2, 1, '30 days', TIMESTAMP '2024-01-31 12:00:00'), "+
		"(3, 1, '720:00:00', TIMESTAMP '2024-02-01 00:00:00'), (4, 2, '1 mon -30 days', TIMESTAMP '2024-02-29 00:00:00'), (5, 2, '0', TIMESTAMP '2024-03-01 00:00:00'), "+
		"(6, 2, NULL, NULL), (7, 3, 'infinity', TIMESTAMP '2024-03-31 00:00:00'), (8, 3, '-infinity', TIMESTAMP '2024-04-01 00:00:00'), "+
		"(9, 4, '-1 day', TIMESTAMP '2024-04-30 00:00:00'), (10, 4, '-24:00:00', TIMESTAMP '2024-05-31 00:00:00'), (11, 5, '1 day 02:00:00', TIMESTAMP '2024-06-01 00:00:00'), "+
		"(12, 5, '1 year -1 second', TIMESTAMP '2024-06-02 00:00:00')")

	// Runtime-column variants of the scalar operations (constant folding
	// cannot answer these: every value comes from a row).
	for _, r := range []struct{ k, e string }{
		{"mul", txt(c + " * 1.5")},
		{"div", txt(c + " / 3")},
		{"neg", txt("-" + c)},
		{"plus", txt(c + " + INTERVAL '1 day 01:00:00'")},
		{"justify", txt("justify_interval(" + c + ")")},
		{"cmp", c + " = INTERVAL '30 days'"},
		{"lt", c + " < INTERVAL '0'"},
		{"tsPlus", txt("ts + " + c)},
		{"tsMinus", txt("ts - " + c)},
		{"isfinite", "isfinite(" + c + ")"},
		{"extractDay", "extract(day FROM " + c + ")"},
		{"extractEpoch", "extract(epoch FROM " + c + ")"},
		{"datePart", "date_part('hour', " + c + ")"},
		{"text", txt(c)},
	} {
		b.add("rt/"+r.k, "SELECT id, "+r.e+" AS x FROM iv80_r ORDER BY id")
	}
	b.add("rt/bare", "SELECT id, "+c+" AS v FROM iv80_r ORDER BY id")
	// Opaque inputs (multi-field text this engine boxes as an opaque
	// IntervalValue): applied, compared, rendered.
	b.add("opaque/apply", "SELECT id, "+txt("ts + "+c)+" AS x FROM iv80_r WHERE id IN (11, 12) ORDER BY id")
	b.add("opaque/compare", "SELECT id FROM iv80_r WHERE "+c+" > INTERVAL '1 day' ORDER BY id")
	b.add("opaque/text", "SELECT id, "+txt(c)+" AS x FROM iv80_r WHERE id IN (11, 12) ORDER BY id")
	b.add("opaque/arith", "SELECT id, "+txt(c+" * 2")+" AS x FROM iv80_r WHERE id IN (11, 12) ORDER BY id")

	// Order and filter with a text display.
	b.add("ord/text", "SELECT id, "+txt(c)+" AS t FROM iv80_r ORDER BY "+c+", id")
	b.add("ord/textDescNullsLast", "SELECT id, "+txt(c)+" AS t FROM iv80_r ORDER BY "+c+" DESC NULLS LAST, id")
	b.add("ord/topN", "SELECT id FROM iv80_r ORDER BY "+c+" DESC NULLS LAST, id LIMIT 3")
	b.add("filt/text", "SELECT id, "+txt(c)+" AS t FROM iv80_r WHERE "+c+" >= INTERVAL '30 days' ORDER BY id")
	// Grouping with a text display of the key, and the derived form.
	for _, p := range hashSort {
		b.set("grp/text/"+p.k, p.set, "SELECT "+txt(c)+" AS k, count(*) AS n FROM iv80_r GROUP BY "+c+" ORDER BY min(id)")
		b.set("grp/derived/"+p.k, p.set, "SELECT "+txt("k")+" AS k, n FROM (SELECT "+c+" AS k, count(*) AS n, min(id) AS lo FROM iv80_r GROUP BY "+c+") q ORDER BY lo")
		b.set("distinct/text/"+p.k, p.set, "SELECT "+txt("k")+" AS k FROM (SELECT DISTINCT "+c+" AS k FROM iv80_r) q ORDER BY k NULLS FIRST")
	}

	// Whole-tuple selection: neither column chooses its member alone.
	tuple := "(VALUES (INTERVAL '1 mon', INTERVAL '24 hours'), (INTERVAL '30 days', INTERVAL '1 day')) v(a, b)"
	tupleRev := "(VALUES (INTERVAL '30 days', INTERVAL '1 day'), (INTERVAL '1 mon', INTERVAL '24 hours')) v(a, b)"
	for _, p := range hashSort {
		b.set("tuple/distinct/"+p.k, p.set, "SELECT "+txt("a")+" AS a, "+txt("b")+" AS b FROM (SELECT DISTINCT a, b FROM "+tuple+") q")
		b.set("tuple/distinctRev/"+p.k, p.set, "SELECT "+txt("a")+" AS a, "+txt("b")+" AS b FROM (SELECT DISTINCT a, b FROM "+tupleRev+") q")
		b.set("tuple/group/"+p.k, p.set, "SELECT "+txt("a")+" AS a, "+txt("b")+" AS b, count(*) AS n FROM "+tuple+" GROUP BY a, b")
	}
	b.ddl("ddl/create/iv80_tp", "CREATE TABLE iv80_tp (id BIGINT, a TEXT, b TEXT)")
	b.ddl("ddl/insert/iv80_tp", "INSERT INTO iv80_tp VALUES (1, '1 mon', '24 hours'), (2, '30 days', '1 day'), (3, '720:00:00', '24:00:00'), (4, '30 days', '1 mon')")
	for _, p := range hashSort {
		b.set("tuple/table/"+p.k, p.set, "SELECT "+txt("CAST(a AS INTERVAL)")+" AS a, "+txt("CAST(b AS INTERVAL)")+" AS b, count(*) AS n, min(id) AS lo FROM iv80_tp GROUP BY CAST(a AS INTERVAL), CAST(b AS INTERVAL) ORDER BY lo")
	}
	// Multi-column keys with DECIMAL and float representation ties.
	mixed := "(VALUES (INTERVAL '1 mon', 1.0, CAST(0.0 AS DOUBLE PRECISION)), (INTERVAL '30 days', 1.00, CAST('-0' AS DOUBLE PRECISION)), (INTERVAL '720:00:00', 1.000, CAST(0.0 AS DOUBLE PRECISION))) v(i, d, f)"
	for _, p := range hashSort {
		b.set("mixkey/group/"+p.k, p.set, "SELECT "+txt("i")+" AS i, "+txt("d")+" AS d, "+txt("f")+" AS f, count(*) AS n FROM "+mixed+" GROUP BY i, d, f")
		b.set("mixkey/distinct/"+p.k, p.set, "SELECT "+txt("i")+" AS i, "+txt("d")+" AS d, "+txt("f")+" AS f FROM (SELECT DISTINCT i, d, f FROM "+mixed+") q")
	}
	// Explicitly ordered aggregate ties, unordered for contrast, DISTINCT
	// aggregate transitions.
	ordered := "(VALUES (1, INTERVAL '1 mon'), (2, INTERVAL '30 days')) q(id, v)"
	b.add("ordagg/minMax", "SELECT "+txt("min(v ORDER BY id)")+" AS lo, "+txt("max(v ORDER BY id DESC)")+" AS hi FROM "+ordered)
	b.add("ordagg/minMaxRev", "SELECT "+txt("min(v ORDER BY id DESC)")+" AS lo, "+txt("max(v ORDER BY id)")+" AS hi FROM "+ordered)
	b.add("ordagg/unordered", "SELECT "+txt("min(v)")+" AS lo, "+txt("max(v)")+" AS hi FROM "+ordered)
	b.add("ordagg/sumOrdered", "SELECT "+txt("sum(v ORDER BY id)")+" AS s FROM "+ordered)
	b.add("ordagg/percentile", "SELECT "+txt("percentile_disc(0.5) WITHIN GROUP (ORDER BY v)")+" AS p FROM "+ordered)
	distinctAgg := "(VALUES (INTERVAL '1 mon'), (INTERVAL '30 days'), (INTERVAL '1 day')) q(v)"
	distinctAggRev := "(VALUES (INTERVAL '1 day'), (INTERVAL '30 days'), (INTERVAL '1 mon')) q(v)"
	for _, p := range hashSort {
		b.set("distagg/sumAvg/"+p.k, p.set, "SELECT "+txt("sum(DISTINCT v)")+" AS s, "+txt("avg(DISTINCT v)")+" AS a, count(DISTINCT v) AS n FROM "+distinctAgg)
		b.set("distagg/sumAvgRev/"+p.k, p.set, "SELECT "+txt("sum(DISTINCT v)")+" AS s, "+txt("avg(DISTINCT v)")+" AS a, count(DISTINCT v) AS n FROM "+distinctAggRev)
	}
	b.add("distagg/countRows", "SELECT count(DISTINCT "+c+") AS n FROM iv80_r")
	// The representative has consequences beyond printing.
	repArith := "(VALUES (INTERVAL '1 mon'), (INTERVAL '30 days')) q(v)"
	repArithRev := "(VALUES (INTERVAL '30 days'), (INTERVAL '1 mon')) q(v)"
	for _, p := range hashSort {
		b.set("reparith/"+p.k, p.set, "SELECT TIMESTAMP '2024-01-31' + v AS t FROM (SELECT DISTINCT v FROM "+repArith+") d")
		b.set("reparithRev/"+p.k, p.set, "SELECT TIMESTAMP '2024-01-31' + v AS t FROM (SELECT DISTINCT v FROM "+repArithRev+") d")
	}

	// A typed default and the timestamp-difference producer; timestamp -
	// date.
	b.ddl("ddl/create/iv80_rt", "CREATE TABLE iv80_rt (id BIGINT, ts TIMESTAMP, d DATE)")
	b.ddl("ddl/insert/iv80_rt", "INSERT INTO iv80_rt VALUES (1, TIMESTAMP '2024-01-31 10:00:00', DATE '2024-01-01'), (2, TIMESTAMP '2024-02-29 23:00:00', DATE '2024-03-01'), (3, NULL, DATE '2024-01-01'), (4, TIMESTAMP '2024-03-01 00:00:00', NULL)")
	b.add("lag/tsDiff", "SELECT id, CAST(lag(NULL, 1, ts) OVER (ORDER BY id) - ts AS TEXT) AS t FROM iv80_rt ORDER BY id")
	b.add("lag/tsDiffBare", "SELECT id, lag(NULL, 1, ts) OVER (ORDER BY id) - ts AS t FROM iv80_rt ORDER BY id")
	b.add("lag/tsDiffCompare", "SELECT id FROM (SELECT id, lag(NULL, 1, ts) OVER (ORDER BY id) - ts AS t FROM iv80_rt) q WHERE t = INTERVAL '0' ORDER BY id")
	b.add("tsdate/text", "SELECT id, "+txt("ts - d")+" AS a, "+txt("d - ts")+" AS b FROM iv80_rt ORDER BY id")
	b.add("tsdate/bare", "SELECT id, ts - d AS a FROM iv80_rt ORDER BY id")
	b.add("tsdate/compare", "SELECT id FROM iv80_rt WHERE ts - d > INTERVAL '30 days' ORDER BY id")

	// Set operations where the right representation is absent from the left,
	// with asymmetric cardinality, under both SetOp strategies (and the plan).
	small := "(VALUES (INTERVAL '1 mon')) l(x)"
	big := "(VALUES (INTERVAL '30 days'), (INTERVAL '720:00:00'), (INTERVAL '1 day'), (INTERVAL '2 days'), (INTERVAL '3 days')) r(x)"
	dup := "(VALUES (INTERVAL '1 mon'), (INTERVAL '1 mon'), (INTERVAL '30 days')) l(x)"
	right := "(VALUES (INTERVAL '720:00:00'), (INTERVAL '720:00:00')) r(x)"
	right1 := "(VALUES (INTERVAL '720:00:00')) r(x)"
	for _, p := range hashSort {
		for _, s := range []struct{ k, l, op, r string }{
			{"intersect/smallBig", small, "INTERSECT", big},
			{"intersect/bigSmall", big, "INTERSECT", small},
			{"intersectAll/smallBig", small, "INTERSECT ALL", big},
			{"intersectAll/bigSmall", big, "INTERSECT ALL", small},
			{"intersectAll/dupRight", dup, "INTERSECT ALL", right},
			{"exceptAll/dupRight1", dup, "EXCEPT ALL", right1},
			{"except/bigSmall", big, "EXCEPT", small},
		} {
			q := "SELECT x FROM " + s.l + " " + s.op + " SELECT x FROM " + s.r
			b.set("set/"+s.k+"/"+p.k, p.set, "SELECT "+txt("x")+" AS x FROM ("+q+") u")
			b.pg("explain/set/"+s.k+"/"+p.k, p.set, "EXPLAIN (COSTS OFF) "+q)
		}
	}

	// GROUPING SETS and ROLLUP over an interval key.
	b.add("gsets/sets", "SELECT "+txt("k")+" AS k, count(*) AS n FROM (SELECT "+c+" AS k FROM iv80_r) q GROUP BY GROUPING SETS ((k), ()) ORDER BY n, "+txt("k")+" NULLS FIRST")
	b.add("gsets/rollup", "SELECT g, "+txt(c)+" AS k, count(*) AS n FROM iv80_r GROUP BY ROLLUP (g, "+c+") ORDER BY g NULLS LAST, min(id), n")

	// Subquery forms.
	b.add("sub/scalarEq", "SELECT id FROM iv80_r WHERE "+c+" = (SELECT "+c+" FROM iv80_r WHERE id = 2) ORDER BY id")
	b.add("sub/scalarMax", "SELECT id FROM iv80_r WHERE "+c+" = (SELECT max("+c+") FROM iv80_r WHERE id <= 3) ORDER BY id")
	b.add("sub/in", "SELECT id FROM iv80_r WHERE "+c+" IN (SELECT "+c+" FROM iv80_r WHERE id = 3) ORDER BY id")
	b.add("sub/notIn", "SELECT id FROM iv80_r WHERE "+c+" NOT IN (SELECT "+c+" FROM iv80_r WHERE id IN (3, 9)) ORDER BY id")
	b.add("sub/any", "SELECT id FROM iv80_r WHERE "+c+" > ANY (SELECT "+c+" FROM iv80_r WHERE id IN (5, 9)) ORDER BY id")
	b.add("sub/all", "SELECT id FROM iv80_r WHERE "+c+" < ALL (SELECT "+c+" FROM iv80_r WHERE id IN (1, 12)) ORDER BY id")
	b.add("sub/correlatedCount", "SELECT id, (SELECT count(*) FROM iv80_r b WHERE CAST(b.s AS INTERVAL) = CAST(a.s AS INTERVAL)) AS n FROM iv80_r a ORDER BY id")
	b.add("sub/correlatedMin", "SELECT id, (SELECT "+txt("min(CAST(b.s AS INTERVAL))")+" FROM iv80_r b WHERE b.g = a.g) AS m FROM iv80_r a ORDER BY id")
	b.add("sub/exists", "SELECT id FROM iv80_r a WHERE EXISTS (SELECT 1 FROM iv80_r b WHERE CAST(b.s AS INTERVAL) = CAST(a.s AS INTERVAL) AND b.id <> a.id) ORDER BY id")
	b.add("sub/scalarPublish", "SELECT (SELECT "+c+" FROM iv80_r WHERE id = 1) AS a")
	b.add("sub/scalarText", "SELECT "+txt("(SELECT "+c+" FROM iv80_r WHERE id = 1)")+" AS a")

	// Shared CTEs.
	b.add("cte/materializedJoin", "WITH m AS MATERIALIZED (SELECT id, "+c+" AS v FROM iv80_r) SELECT a.id AS l, b.id AS r FROM m a JOIN m b ON a.v = b.v AND a.id < b.id ORDER BY l, r")
	b.add("cte/text", "WITH m AS (SELECT id, "+c+" AS v FROM iv80_r) SELECT id, "+txt("v")+" AS t FROM m ORDER BY id")
	b.add("cte/twice", "WITH m AS MATERIALIZED (SELECT "+c+" AS v FROM iv80_r) SELECT count(*) AS n FROM (SELECT v FROM m UNION SELECT v FROM m) u")

	// Outer-join padding.
	b.add("oj/leftText", "SELECT a.id, "+txt("CAST(b.s AS INTERVAL)")+" AS t, CAST(b.s AS INTERVAL) IS NULL AS n FROM iv80_r a LEFT JOIN iv80_r b ON a.id = b.id + 6 ORDER BY a.id")
	b.add("oj/leftDerived", "SELECT a.id, "+txt("b.v")+" AS t FROM iv80_r a LEFT JOIN (SELECT id, "+c+" AS v FROM iv80_r) b ON a.id = b.id + 6 ORDER BY a.id")
	b.add("oj/fullKey", "SELECT a.id AS l, b.id AS r FROM iv80_r a FULL JOIN iv80_r b ON CAST(a.s AS INTERVAL) = CAST(b.s AS INTERVAL) AND a.id < b.id ORDER BY l NULLS FIRST, r NULLS FIRST")

	// Empty and all-NULL inputs: a zero-row result keeps its types.
	b.ddl("ddl/create/iv80_n", "CREATE TABLE iv80_n (id BIGINT, s TEXT)")
	b.ddl("ddl/insert/iv80_n", "INSERT INTO iv80_n VALUES (1, NULL), (2, NULL)")
	b.add("empty/text", "SELECT "+txt(c)+" AS t FROM iv80_r WHERE false")
	b.add("empty/bare", "SELECT "+c+" AS v FROM iv80_r WHERE false")
	b.add("empty/agg", "SELECT "+txt("sum("+c+")")+" AS s, "+txt("min("+c+")")+" AS lo, count("+c+") AS n FROM iv80_r WHERE false")
	b.add("empty/group", "SELECT "+txt(c)+" AS k, count(*) AS n FROM iv80_r WHERE false GROUP BY "+c)
	b.add("null/agg", "SELECT "+txt("sum("+c+")")+" AS s, "+txt("avg("+c+")")+" AS a, "+txt("min("+c+")")+" AS lo, "+txt("max("+c+")")+" AS hi, count("+c+") AS n FROM iv80_n")
	b.add("null/group", "SELECT "+txt(c)+" AS k, count(*) AS n FROM iv80_n GROUP BY "+c)
	b.add("null/order", "SELECT id, "+txt(c)+" AS t FROM iv80_n ORDER BY "+c+", id")

	// Ordered sum overflow and cancellation, both directions.
	b.ddl("ddl/create/iv80_ov", "CREATE TABLE iv80_ov (id BIGINT, s TEXT)")
	b.ddl("ddl/insert/iv80_ov", "INSERT INTO iv80_ov VALUES (1, '2147483647 days'), (2, '1 day'), (3, '-1 day'), (4, '9223372036854775807 microseconds'), (5, '-1 microsecond')")
	b.add("ov/winForward", "SELECT id, "+txt("sum("+c+") OVER (ORDER BY id ROWS UNBOUNDED PRECEDING)")+" AS s FROM iv80_ov WHERE id <= 3 ORDER BY id")
	b.add("ov/winBackward", "SELECT id, "+txt("sum("+c+") OVER (ORDER BY id DESC ROWS UNBOUNDED PRECEDING)")+" AS s FROM iv80_ov WHERE id <= 3 ORDER BY id")
	b.add("ov/aggAsc", "SELECT "+txt("sum("+c+" ORDER BY id)")+" AS s FROM iv80_ov WHERE id <= 3")
	b.add("ov/aggDesc", "SELECT "+txt("sum("+c+" ORDER BY id DESC)")+" AS s FROM iv80_ov WHERE id <= 3")
	b.add("ov/aggUnordered", "SELECT "+txt("sum("+c+")")+" AS s FROM iv80_ov WHERE id <= 3")
	b.add("ov/timeForward", "SELECT id, "+txt("sum("+c+") OVER (ORDER BY id ROWS UNBOUNDED PRECEDING)")+" AS s FROM iv80_ov WHERE id >= 4 ORDER BY id")
	b.add("ov/timeBackward", "SELECT id, "+txt("sum("+c+") OVER (ORDER BY id DESC ROWS UNBOUNDED PRECEDING)")+" AS s FROM iv80_ov WHERE id >= 4 ORDER BY id")

	// Partial states holding NULL and each infinity sign.
	b.ddl("ddl/create/iv80_pm", "CREATE TABLE iv80_pm (id BIGINT, g BIGINT, s TEXT)")
	b.ddl("ddl/insert/iv80_pm", "INSERT INTO iv80_pm VALUES (1, 1, NULL), (2, 1, '1 day'), (3, 2, 'infinity'), (4, 2, '1 day'), (5, 2, NULL), "+
		"(6, 3, '-infinity'), (7, 3, '2 days'), (8, 4, 'infinity'), (9, 4, '-infinity'), (10, 5, NULL), (11, 6, 'infinity'), (12, 6, 'infinity')")
	agg := txt("sum("+c+")") + " AS s, " + txt("avg("+c+")") + " AS a, count(" + c + ") AS n, " + txt("min("+c+")") + " AS lo, " + txt("max("+c+")") + " AS hi"
	for _, g := range []struct{ k, where string }{{"null", "g = 1"}, {"posInf", "g = 2"}, {"negInf", "g = 3"}, {"bothInf", "g = 4"}, {"nullOnly", "g = 5"}, {"posInfTwice", "g = 6"}} {
		b.add("pm/"+g.k, "SELECT "+agg+" FROM iv80_pm WHERE "+g.where)
	}
	b.add("pm/groups", "SELECT g, "+agg+" FROM iv80_pm WHERE g <> 4 GROUP BY g ORDER BY g")

	// Sliding windows where infinities enter and leave the frame.
	b.ddl("ddl/create/iv80_sw", "CREATE TABLE iv80_sw (id BIGINT, s TEXT)")
	b.ddl("ddl/insert/iv80_sw", "INSERT INTO iv80_sw VALUES (1, '1 day'), (2, 'infinity'), (3, '2 days'), (4, '3 days'), (5, '-infinity'), (6, '4 days'), (7, '5 days')")
	for _, w := range []struct{ k, frame string }{{"rows2", "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"}, {"rows3", "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"}, {"following", "ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING"}} {
		b.add("sw/"+w.k, "SELECT id, "+txt("sum("+c+") OVER (ORDER BY id "+w.frame+")")+" AS s, "+txt("avg("+c+") OVER (ORDER BY id "+w.frame+")")+" AS a FROM iv80_sw ORDER BY id")
	}
	b.add("sw/all", "SELECT id, "+txt("sum("+c+") OVER (ORDER BY id ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)")+" AS s FROM iv80_sw ORDER BY id")

	// RANGE frames with an interval offset.
	b.ddl("ddl/create/iv80_fr", "CREATE TABLE iv80_fr (id BIGINT, ts TIMESTAMP)")
	b.ddl("ddl/insert/iv80_fr", "INSERT INTO iv80_fr VALUES (1, TIMESTAMP '2024-01-31 00:00:00'), (2, TIMESTAMP '2024-02-29 00:00:00'), (3, TIMESTAMP '2024-03-01 00:00:00'), "+
		"(4, TIMESTAMP '2024-03-31 00:00:00'), (5, TIMESTAMP '2024-04-30 00:00:00'), (6, NULL), (7, TIMESTAMP 'infinity'), (8, TIMESTAMP '-infinity')")
	for _, f := range []struct{ k, frame string }{
		{"monthPreceding", "ORDER BY ts RANGE BETWEEN INTERVAL '1 mon' PRECEDING AND CURRENT ROW"},
		{"monthFollowing", "ORDER BY ts RANGE BETWEEN CURRENT ROW AND INTERVAL '1 mon' FOLLOWING"},
		{"desc", "ORDER BY ts DESC RANGE BETWEEN INTERVAL '1 mon' PRECEDING AND CURRENT ROW"},
		{"descNullsLast", "ORDER BY ts DESC NULLS LAST RANGE BETWEEN CURRENT ROW AND INTERVAL '30 days' FOLLOWING"},
		{"nullOffset", "ORDER BY ts RANGE BETWEEN CAST(NULL AS INTERVAL) PRECEDING AND CURRENT ROW"},
		{"zeroNormalized", "ORDER BY ts RANGE BETWEEN INTERVAL '1 mon -30 days' PRECEDING AND CURRENT ROW"},
		{"mixedFields", "ORDER BY ts RANGE BETWEEN INTERVAL '-1 mon 31 days' PRECEDING AND CURRENT ROW"},
		{"infOffset", "ORDER BY ts RANGE BETWEEN INTERVAL 'infinity' PRECEDING AND CURRENT ROW"},
		{"infFollowing", "ORDER BY ts RANGE BETWEEN CURRENT ROW AND INTERVAL 'infinity' FOLLOWING"},
		{"negInfOffset", "ORDER BY ts RANGE BETWEEN INTERVAL '-infinity' PRECEDING AND CURRENT ROW"},
		{"negOffset", "ORDER BY ts RANGE BETWEEN INTERVAL '-1 day' PRECEDING AND CURRENT ROW"},
	} {
		b.add("frame/"+f.k, "SELECT id, count(*) OVER ("+f.frame+") AS n FROM iv80_fr ORDER BY id")
	}
	b.add("frame/tsInfRowsShift", "SELECT id, CAST(ts + INTERVAL '-1 day' AS TEXT) AS a, CAST(ts - INTERVAL '1 mon' AS TEXT) AS b, ts + INTERVAL '1 day' > TIMESTAMP '2024-01-01' AS c FROM iv80_fr WHERE id IN (5, 7, 8) ORDER BY id")
	b.add("frame/intervalOrderDesc", "SELECT id, count(*) OVER (ORDER BY "+c+" DESC RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) AS n FROM iv80_r ORDER BY id")
	b.add("frame/intervalOrderAsc", "SELECT id, count(*) OVER (ORDER BY "+c+" RANGE BETWEEN CURRENT ROW AND INTERVAL '1 day' FOLLOWING) AS n FROM iv80_r ORDER BY id")

	// Round 3 (the second judge record, correction 3): an interval COLUMN
	// reaching an unsupported consumer through each relational boundary.
	// Each must be an intentional, named refusal at every stage (owned by A),
	// on every arm (the spilled and shuffled arms are the spill and shuffle
	// boundaries). iv80_r's finite rows only (g <> 3), so no consumer meets an
	// infinity.
	for _, nb := range iv80NegBoundaries() {
		for _, nc := range iv80NegConsumers() {
			b.add("neg/"+nb.k+"/"+nc.k, nc.sql(nb.from))
		}
	}
	return b.out
}

// iv80NegBoundary is a FROM clause that hands an interval column v (beside
// id) across one relational boundary ("p" is the CTE p).
type iv80NegBoundary struct{ k, from string }

const iv80NegCTE = "WITH p AS (SELECT id, CAST(s AS INTERVAL) AS v FROM iv80_r WHERE g <> 3) "

func iv80NegBoundaries() []iv80NegBoundary {
	c := "CAST(s AS INTERVAL)"
	return []iv80NegBoundary{
		{"projection", "(SELECT id, w AS v FROM (SELECT id, " + c + " AS w FROM iv80_r WHERE g <> 3) p0) p"},
		{"derived", "(SELECT id, " + c + " AS v FROM iv80_r WHERE g <> 3) p"},
		{"cte", "p"},
		{"subquery", "(SELECT a.id, (SELECT " + c + " FROM iv80_r b WHERE b.id = a.id) AS v FROM iv80_r a WHERE a.g <> 3) p"},
		{"setop", "(SELECT id, " + c + " AS v FROM iv80_r WHERE g IN (1, 2) UNION ALL SELECT id, " + c + " FROM iv80_r WHERE g IN (4, 5)) p"},
		{"join", "(SELECT a.id, b.v FROM iv80_r a JOIN (SELECT id, " + c + " AS v FROM iv80_r WHERE g <> 3) b ON a.id = b.id) p"},
	}
}

// iv80NegConsumer is an unsupported consumer of the column v.
type iv80NegConsumer struct {
	k   string
	sql func(from string) string
}

func iv80NegConsumers() []iv80NegConsumer {
	sel := func(from, list string) string {
		if from == "p" {
			return iv80NegCTE + list + " FROM p"
		}
		return list + " FROM " + from
	}
	return []iv80NegConsumer{
		{"toChar", func(f string) string { return sel(f, "SELECT id, to_char(v, 'HH24:MI') AS x") + " ORDER BY id" }},
		{"dateTrunc", func(f string) string {
			return sel(f, "SELECT id, CAST(date_trunc('hour', v) AS TEXT) AS x") + " ORDER BY id"
		}},
		{"castTime", func(f string) string { return sel(f, "SELECT id, CAST(CAST(v AS TIME) AS TEXT) AS x") + " ORDER BY id" }},
		{"orderedAgg", func(f string) string { return sel(f, "SELECT CAST(mode() WITHIN GROUP (ORDER BY v) AS TEXT) AS x") }},
		{"distinctSum", func(f string) string { return sel(f, "SELECT CAST(sum(DISTINCT v) AS TEXT) AS x") }},
		{"arrayAgg", func(f string) string { return sel(f, "SELECT CAST(array_agg(v ORDER BY id) AS TEXT) AS x") }},
		{"arrayCtor", func(f string) string { return sel(f, "SELECT id, CAST(ARRAY[v] AS TEXT) AS x") + " ORDER BY id" }},
	}
}

// --- 12. Writes: DML and CTAS through derived columns -------------------

func iv80Writes() []iv80Cell {
	b := &iv80Builder{}
	c := "CAST(s AS INTERVAL)"
	b.ddl("ddl/create/iv80_wt", "CREATE TABLE iv80_wt (id BIGINT, s TEXT)")
	b.ddl("ddl/insert/iv80_wt", "INSERT INTO iv80_wt VALUES (1, 'x'), (2, 'y')")
	b.write("w/insertSelect", "INSERT INTO iv80_wt SELECT id, "+c+" FROM iv80_r WHERE id = 1")
	b.write("w/insertDerived", "INSERT INTO iv80_wt SELECT id, v FROM (SELECT id, "+c+" AS v FROM iv80_r) q WHERE id = 2")
	b.write("w/insertValues", "INSERT INTO iv80_wt VALUES (100, INTERVAL '1 day')")
	b.write("w/insertZero", "INSERT INTO iv80_wt SELECT id, "+c+" FROM iv80_r WHERE false")
	b.write("w/insertSubquery", "INSERT INTO iv80_wt VALUES (101, (SELECT "+c+" FROM iv80_r WHERE id = 1))")
	b.write("w/insertTsDiff", "INSERT INTO iv80_wt SELECT id, ts - ts FROM iv80_rt WHERE id = 1")
	b.write("w/update", "UPDATE iv80_wt SET s = INTERVAL '1 day' WHERE id = 1")
	b.write("w/updateDerived", "UPDATE iv80_wt SET s = (SELECT "+c+" FROM iv80_r WHERE id = 2) WHERE id = 2")
	b.write("w/updateZero", "UPDATE iv80_wt SET s = INTERVAL '1 day' WHERE false")
	b.write("w/merge", "MERGE INTO iv80_wt t USING iv80_r r ON t.id = r.id + 1000 WHEN NOT MATCHED AND r.id = 3 THEN INSERT VALUES (r.id + 1000, CAST(r.s AS INTERVAL))")
	b.write("w/ctas", "CREATE TABLE iv80_wc1 AS SELECT "+c+" AS v FROM iv80_r")
	b.write("w/ctasDerived", "CREATE TABLE iv80_wc2 AS SELECT v FROM (SELECT "+c+" AS v FROM iv80_r) q")
	b.write("w/ctasNoData", "CREATE TABLE iv80_wc3 AS SELECT "+c+" AS v FROM iv80_r WITH NO DATA")
	b.write("w/ctasNoDataDerived", "CREATE TABLE iv80_wc4 AS SELECT v FROM (SELECT "+c+" AS v FROM iv80_r) q WITH NO DATA")
	b.write("w/ctasZero", "CREATE TABLE iv80_wc5 AS SELECT "+c+" AS v FROM iv80_r WHERE false")
	b.write("w/ctasText", "CREATE TABLE iv80_wc6 AS SELECT CAST("+c+" AS TEXT) AS v FROM iv80_r")
	b.write("w/ctasTsDiffNoData", "CREATE TABLE iv80_wc7 AS SELECT ts - ts AS v FROM iv80_rt WITH NO DATA")
	// Round 3: an interval column written through each relational boundary
	// (beside 11_relational:neg/*).
	for _, nb := range iv80NegBoundaries() {
		sel := "SELECT id + 2000, v FROM " + nb.from
		if nb.from == "p" {
			sel = iv80NegCTE + "SELECT id + 2000, v FROM p"
		}
		b.write("w/neg/"+nb.k, "INSERT INTO iv80_wt "+sel)
	}
	return b.out
}
