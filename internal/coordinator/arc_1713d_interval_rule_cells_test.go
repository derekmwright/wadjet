// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import "strings"

// #1713d: the INTERVAL rule (physical/validate_interval_use.go, the
// maintainer's A+ interim). An interval-valued expression is consumed inline
// or refused; it never leaves the expression that computes it. These cells
// join the interval table (arc_1713c_interval_order_arms_test.go): the
// reviewer's regression list for 1713c round 3 (rv/*), every wrong-answer
// FAMILY the ruling names in each spelling (direct, alias, derived table,
// CTE, set-operation arm, CASE / COALESCE, container), the controls that must
// NOT be refused (an explicit text cast, a TEXT column that looks like an
// interval, timestamp shifts as keys), the clause positions, the opaque
// runtime guard, the INTERVAL DDL refusal and timestamp subtraction's class
// (an interval-valued producer since round 6, ivCellsTD).
//
// The fixtures beside iv_s: iv_t (instants, for shifts read by a worker),
// iv_o (text that is interval input this engine's fields cannot hold).

var ivTRows = []string{
	"(1, TIMESTAMP '2024-01-31 10:00:00', DATE '2024-01-31')",
	"(2, TIMESTAMP '2024-02-29 23:30:00', DATE '2024-02-29')",
	"(3, TIMESTAMP '2023-12-31 00:00:00', DATE '2023-12-31')",
	"(4, TIMESTAMP '2024-03-01 12:00:00', DATE '2024-03-01')",
	"(5, NULL, NULL)",
}

func ivFixturesD() []ivCell {
	return []ivCell{
		{name: "ddl/createT", sql: "CREATE TABLE iv_t (id BIGINT, ts TIMESTAMP, d DATE)", ddl: true},
		{name: "ddl/insertT", sql: "INSERT INTO iv_t VALUES " + strings.Join(ivTRows, ", "), ddl: true},
		{name: "ddl/createO", sql: "CREATE TABLE iv_o (id BIGINT, s TEXT)", ddl: true},
		{name: "ddl/insertO", sql: "INSERT INTO iv_o VALUES (1, '1 day 02:00:00'), (2, '26 hours'), (3, '1 day')", ddl: true},
		// A NEW column spelled INTERVAL is refused (the ruling's point 3);
		// PostgreSQL creates it. The tables are dropped on PostgreSQL by
		// TestArc1713cMeasure before the run.
		{name: "ddl/intervalColumn", sql: "CREATE TABLE iv_d1 (id BIGINT, d INTERVAL)", ddl: true},
		{name: "ddl/intervalArray", sql: "CREATE TABLE iv_d2 (id BIGINT, d INTERVAL[])", ddl: true},
	}
}

// ivDDLTables are the tables the cells create, dropped first on PostgreSQL.
var ivDDLTables = []string{"iv_s", "iv_t", "iv_o", "iv_d1", "iv_d2"}

func ivCellsD() []ivCell {
	var out []ivCell
	add := func(name, sql string) { out = append(out, ivCell{name: name, sql: sql, typed: true}) }
	c := "CAST(s AS INTERVAL)"
	d := "(SELECT id, " + c + " AS k FROM iv_s)"

	// The 1713c round-3 review's seven findings (rv/*), table-backed so a
	// DAG worker builds the projection and the aggregate.
	add("rv/derivedCountTimes", "SELECT count(a) AS n FROM (SELECT "+c+" * 2 AS a FROM iv_s) t")
	add("rv/derivedCountCoalesce", "SELECT count(a) AS n FROM (SELECT COALESCE("+c+", NULL) AS a FROM iv_s) t")
	add("rv/derivedCountCase", "SELECT count(a) AS n FROM (SELECT CASE WHEN id = 1 THEN INTERVAL '1 day' END * 2 AS a FROM iv_s) t")
	add("rv/derivedMaxTimes", "SELECT max(a) AS m FROM (SELECT "+c+" * 2 AS a FROM iv_s) t")
	add("rv/derivedMaxCoalesce", "SELECT max(a) AS m FROM (SELECT COALESCE("+c+", NULL) AS a FROM iv_s) t")
	add("rv/derivedCountNullif", "SELECT count(a) AS n FROM (SELECT NULLIF("+c+", INTERVAL '1 day') AS a FROM iv_s) t")
	add("rv/mixedCaseConst", "SELECT CASE WHEN true THEN INTERVAL '1 day' * 2 ELSE 1 END AS a")
	add("rv/mixedCaseColumn", "SELECT CASE WHEN id = 1 THEN -INTERVAL '1 day' ELSE id END AS a FROM iv_s")
	add("rv/mixedCoalesceConst", "SELECT COALESCE(INTERVAL '1 day' * 2, 1) AS a")
	add("rv/mixedCoalesceColumn", "SELECT COALESCE(INTERVAL '1 day' * 2, id) AS a FROM iv_s")
	add("rv/ifnull", "SELECT ifnull(INTERVAL '1 day', NULL) * 2 AS a")
	add("rv/gatherGroup", "SELECT id, count(*) * INTERVAL '1 hour' AS h FROM iv_s WHERE id IN (1, 2) GROUP BY id ORDER BY id")
	add("rv/gatherGlobal", "SELECT max(id) AS m, count(*) * INTERVAL '1 hour' AS h FROM iv_s")
	add("rv/gatherCase", "SELECT CASE WHEN count(*) > 0 THEN INTERVAL '1 day' * 2 END AS a FROM iv_s")
	add("rv/orderTimes", "SELECT id FROM iv_s WHERE id IN (1, 6, 7) ORDER BY "+c+" * 2, id")
	add("rv/windowOrderTimes", "SELECT id, row_number() OVER (ORDER BY "+c+" * 2, id) AS r FROM iv_s WHERE id IN (1, 6, 7) ORDER BY id")
	// Gather-side expression columns over non-interval values: the same
	// coordinator materialization, answered (controls).
	add("rv/gatherNumericControl", "SELECT id, count(*) * 2 AS h FROM iv_s WHERE id IN (1, 2) GROUP BY id ORDER BY id")
	add("rv/gatherTextControl", "SELECT CASE WHEN count(*) > 0 THEN 'some' END AS a FROM iv_s")

	// The families, each spelling.
	add("fam/groupDirect", "SELECT count(*) AS n FROM iv_s GROUP BY "+c+" ORDER BY n")
	add("fam/groupDerived", "SELECT count(*) AS g FROM (SELECT k FROM "+d+" x GROUP BY k) t")
	add("fam/groupCTE", "WITH x AS "+d+" SELECT count(*) AS g FROM (SELECT k FROM x GROUP BY k) t")
	add("fam/groupSetOp", "SELECT count(*) AS g FROM (SELECT k FROM (SELECT "+c+" AS k FROM iv_s UNION ALL SELECT INTERVAL '1 day') u GROUP BY k) t")
	add("fam/groupCase", "SELECT count(*) AS g FROM (SELECT 1 AS x FROM iv_s GROUP BY CASE WHEN id > 0 THEN "+c+" END) t")
	add("fam/groupContainer", "SELECT count(*) AS g FROM (SELECT 1 AS x FROM iv_s GROUP BY ARRAY["+c+"]) t")
	add("fam/distinctDerived", "SELECT count(*) AS g FROM (SELECT DISTINCT k FROM "+d+" x) t")
	add("fam/distinctCTE", "WITH x AS "+d+" SELECT count(*) AS g FROM (SELECT DISTINCT k FROM x) t")
	add("fam/distinctCase", "SELECT count(*) AS g FROM (SELECT DISTINCT COALESCE("+c+", INTERVAL '0 days') AS k FROM iv_s) t")
	add("fam/setOpDerived", "SELECT count(*) AS n FROM (SELECT k FROM "+d+" a WHERE id = 1 UNION SELECT k FROM "+d+" b WHERE id = 2) t")
	add("fam/setOpCTE", "WITH x AS "+d+" SELECT count(*) AS n FROM (SELECT k FROM x WHERE id = 3 INTERSECT SELECT k FROM x WHERE id = 4) t")
	add("fam/setOpCoalesce", "SELECT count(*) AS n FROM (SELECT COALESCE(INTERVAL '1 day', NULL) AS x UNION SELECT INTERVAL '24 hours') t")
	add("fam/partitionDerived", "SELECT id, count(*) OVER (PARTITION BY k) AS n FROM "+d+" x WHERE id IN (1, 2, 3) ORDER BY id")
	add("fam/partitionCase", "SELECT id, count(*) OVER (PARTITION BY CASE WHEN id > 0 THEN "+c+" END) AS n FROM iv_s WHERE id IN (1, 2) ORDER BY id")
	add("fam/orderAlias", "SELECT id, "+c+" AS k FROM iv_s WHERE id IN (1, 6, 7) ORDER BY k, id")
	add("fam/orderDerived", "SELECT id FROM "+d+" x WHERE id IN (1, 6, 7) ORDER BY k, id")
	add("fam/orderCTE", "WITH x AS "+d+" SELECT id FROM x WHERE id IN (1, 6, 7) ORDER BY k, id")
	add("fam/orderCoalesce", "SELECT id FROM iv_s WHERE id IN (1, 6, 7) ORDER BY COALESCE("+c+", INTERVAL '0 days'), id")
	add("fam/orderWindow", "SELECT id, row_number() OVER (ORDER BY "+c+", id) AS r FROM iv_s WHERE id IN (1, 6, 7) ORDER BY id")
	add("fam/minMaxDerived", "SELECT max(k) AS m FROM "+d+" x WHERE id IN (1, 6, 7, 12)")
	add("fam/minMaxCTE", "WITH x AS "+d+" SELECT min(k) AS m FROM x WHERE id IN (1, 6, 7, 12)")
	add("fam/minMaxCase", "SELECT max(CASE WHEN id > 0 THEN "+c+" END) AS m FROM iv_s WHERE id IN (1, 6, 7, 12)")
	add("fam/joinCTE", "WITH x AS "+d+" SELECT a.id AS l, b.id AS r FROM x a JOIN x b ON a.k = b.k WHERE a.id < b.id ORDER BY l, r")
	add("fam/subAny", "SELECT id FROM iv_s WHERE "+c+" = ANY (SELECT "+c+" FROM iv_s WHERE id = 2) ORDER BY id")
	add("fam/subInText", "SELECT id FROM iv_s WHERE "+c+" IN (SELECT s FROM iv_s WHERE id = 2) ORDER BY id")
	add("fam/subScalarCTE", "WITH x AS "+d+" SELECT id FROM iv_s WHERE "+c+" = (SELECT k FROM x WHERE id = 2) ORDER BY id")
	add("fam/opaqueRowCompare", "SELECT id FROM iv_o WHERE CAST(s AS INTERVAL) = INTERVAL '26 hours' ORDER BY id")
	add("fam/opaqueRowText", "SELECT id, CAST(CAST(s AS INTERVAL) AS TEXT) AS t FROM iv_o ORDER BY id")
	add("fam/opaqueRowShift", "SELECT id, CAST(TIMESTAMP '2024-01-01 00:00:00' + CAST(s AS INTERVAL) AS TEXT) AS t FROM iv_o ORDER BY id")
	add("fam/opaqueEmpty", "SELECT id FROM iv_o WHERE CAST(s AS INTERVAL) = INTERVAL '26 hours' AND id < 0")
	add("fam/opaqueSingleRowsControl", "SELECT id FROM iv_o WHERE id = 3 AND CAST(s AS INTERVAL) = INTERVAL '24 hours'")

	// Controls the rule must NOT refuse.
	add("ctl/castTextGroup", "SELECT CAST("+c+" AS TEXT) AS k, count(*) AS n FROM iv_s GROUP BY CAST("+c+" AS TEXT) ORDER BY k")
	add("ctl/castTextOrder", "SELECT id FROM iv_s WHERE id IN (1, 6, 7) ORDER BY CAST("+c+" AS TEXT), id")
	add("ctl/castTextDerived", "SELECT count(*) AS g FROM (SELECT DISTINCT CAST("+c+" * 2 AS TEXT) AS k FROM iv_s) t")
	add("ctl/textGroup", "SELECT s, count(*) AS n FROM iv_s GROUP BY s ORDER BY s")
	add("ctl/textOrder", "SELECT id FROM iv_s WHERE id IN (1, 6, 7) ORDER BY s, id")
	add("ctl/textUnion", "SELECT count(*) AS n FROM (SELECT s FROM iv_s WHERE id = 1 UNION SELECT s FROM iv_s WHERE id = 2) t")
	add("ctl/textMinMax", "SELECT min(s) AS lo, max(s) AS hi FROM iv_s")
	add("ctl/tsShiftOrder", "SELECT id FROM iv_t WHERE ts IS NOT NULL ORDER BY ts + INTERVAL '1 month' DESC, id")
	add("ctl/tsShiftTopN", "SELECT id FROM iv_t WHERE ts IS NOT NULL ORDER BY ts - INTERVAL '1 day', id LIMIT 2")
	add("ctl/tsShiftGroup", "SELECT CAST(d + INTERVAL '1 month' AS TEXT) AS k, count(*) AS n FROM iv_t GROUP BY d + INTERVAL '1 month' ORDER BY k")
	add("ctl/tsShiftProject", "SELECT id, CAST(ts + INTERVAL '1 month' AS TEXT) AS a, CAST(d - INTERVAL '1 year' AS TEXT) AS b FROM iv_t ORDER BY id")
	add("ctl/tsShiftType", "SELECT ts + INTERVAL '1 month' AS a, d - INTERVAL '1 year' AS b FROM iv_t WHERE id = 5")
	add("ctl/tsShiftWhere", "SELECT id FROM iv_t WHERE ts > TIMESTAMP '2024-02-01 00:00:00' - INTERVAL '1 day' ORDER BY id")
	add("ctl/tsShiftJoin", "SELECT a.id AS l, b.id AS r FROM iv_t a JOIN iv_t b ON a.d + INTERVAL '1 day' = b.d + INTERVAL '1 day' ORDER BY l")
	add("ctl/nowMinus", "SELECT count(*) AS n FROM iv_t WHERE ts > now() - INTERVAL '100 years'")
	add("ctl/having", "SELECT count(*) AS n FROM iv_s HAVING count(*) > 0 AND INTERVAL '1 day' = INTERVAL '24 hours'")
	add("ctl/filter", "SELECT count(*) FILTER (WHERE "+c+" > INTERVAL '1 day') AS n FROM iv_s")
	add("ctl/caseCompare", "SELECT id, CASE WHEN "+c+" > INTERVAL '1 day' THEN 'big' ELSE 'small' END AS k FROM iv_s WHERE id IN (1, 6, 8, 9) ORDER BY id")
	add("ctl/datePart", "SELECT id, date_part('day', "+c+") AS a FROM iv_s WHERE id IN (1, 4, 8) ORDER BY id")
	add("ctl/timeBucket", "SELECT id, CAST(time_bucket(INTERVAL '1 hour', ts) AS TEXT) AS b FROM iv_t ORDER BY id")
	add("ctl/arithInline", "SELECT id FROM iv_s WHERE "+c+" * 2 > INTERVAL '2 days' ORDER BY id")
	add("ctl/dateAdd", "SELECT id, CAST(date_add(d, INTERVAL '1 month') AS TEXT) AS a FROM iv_t ORDER BY id")

	// Clause positions and dispositions.
	add("pos/generateSeries", "SELECT * FROM generate_series(1, 3, INTERVAL '1 day')")
	add("pos/rangeFrame", "SELECT id, count(*) OVER (ORDER BY id RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) AS n FROM iv_s")
	add("pos/dateTrunc", "SELECT date_trunc('day', INTERVAL '1 day') AS a")
	add("pos/limit0", "SELECT INTERVAL '1 day' AS a LIMIT 0")
	add("pos/emptyInput", "SELECT "+c+" AS a FROM iv_s WHERE id < 0")
	add("pos/nullLiteral", "SELECT CAST(NULL AS INTERVAL) AS a")
	add("pos/deadCaseBad", "SELECT CASE WHEN false THEN CAST('x' AS INTERVAL) END IS NULL AS a")
	add("pos/castBadEmpty", "SELECT id FROM iv_s WHERE CAST('bogus' AS INTERVAL) = INTERVAL '1 day' AND id < 0")
	add("pos/concat", "SELECT 'x' || INTERVAL '1 day' AS a")
	add("pos/castInteger", "SELECT CAST(INTERVAL '1 day' AS INTEGER) AS a")
	add("pos/insertSelectText", "SELECT count(*) AS n FROM (SELECT CAST(INTERVAL '1 day' AS TEXT) AS t) x")
	add("pos/tsPlusUnknown", "SELECT CAST(TIMESTAMP '2024-01-31 00:00:00' + '1 month' AS TEXT) AS a")
	add("pos/scalarSubquery", "SELECT (SELECT INTERVAL '1 day') = INTERVAL '24 hours' AS a")
	add("pos/castTextOfSubquery", "SELECT CAST((SELECT INTERVAL '1 day') AS TEXT) AS a")
	add("pos/rowCompare", "SELECT (INTERVAL '1 day', 1) = (INTERVAL '24 hours', 1) AS a")
	// Not an interval: a row-dependent error under a conjunct that rejects
	// every row — the evaluation-order class fam/opaqueEmpty belongs to.
	add("pos/divZeroEmpty", "SELECT id FROM iv_s WHERE 1 / (id - id) = 1 AND id < 0")
	add("pos/whereBoolean", "SELECT id FROM iv_s WHERE INTERVAL '1 day' ORDER BY id")

	// Timestamp subtraction (temporal r16): an interval-valued producer since
	// #1713d round 6 (ivCellsTD), refused wherever it is used.
	add("tsub/tsTs", "SELECT TIMESTAMP '2024-01-02 03:00:00' - TIMESTAMP '2024-01-01 00:00:00' AS a")
	add("tsub/dateDate", "SELECT DATE '2024-03-01' - DATE '2024-02-01' AS a")
	add("tsub/tsDate", "SELECT TIMESTAMP '2024-01-02 03:00:00' - DATE '2024-01-01' AS a")
	add("tsub/rows", "SELECT a.id AS l, b.id AS r, a.ts - b.ts AS dt FROM iv_t a JOIN iv_t b ON a.id = b.id + 1 ORDER BY l")
	add("tsub/vsInterval", "SELECT id FROM iv_t WHERE ts - TIMESTAMP '2024-01-01 00:00:00' > INTERVAL '30 days' ORDER BY id")
	add("tsub/vsNumber", "SELECT id FROM iv_t WHERE ts - TIMESTAMP '2024-01-01 00:00:00' > 1000 ORDER BY id")
	add("tsub/groupKey", "SELECT ts - TIMESTAMP '2024-01-01 00:00:00' AS k, count(*) AS n FROM iv_t WHERE ts IS NOT NULL GROUP BY ts - TIMESTAMP '2024-01-01 00:00:00' ORDER BY k")
	add("tsub/sum", "SELECT sum(ts - TIMESTAMP '2024-01-01 00:00:00') AS s FROM iv_t")
	add("tsub/epoch", "SELECT id, extract(epoch FROM ts - TIMESTAMP '2024-01-01 00:00:00') AS e FROM iv_t ORDER BY id")

	// #1713d round 3: a timestamp difference beside an interval. PostgreSQL
	// answers every one (the difference is its interval); here the difference
	// is a millisecond double, so each is refused 0A000 (tsDiffRefusal), not
	// PostgreSQL-absent 42883 / 42804. `date - date` is an integer on both
	// sides, and PostgreSQL's own 42883 stays (tsub/dateVsInterval).
	t0 := "TIMESTAMP '2024-01-01 00:00:00'"
	df := "ts - " + t0
	for _, op := range []struct{ name, op string }{{"Eq", "="}, {"Ne", "<>"}, {"Lt", "<"}, {"Le", "<="}, {"Gt", ">"}, {"Ge", ">="}} {
		add("tsub/vsInterval"+op.name, "SELECT id FROM iv_t WHERE "+df+" "+op.op+" INTERVAL '1452 hours' ORDER BY id")
	}
	add("tsub/vsIntervalRev", "SELECT id FROM iv_t WHERE INTERVAL '30 days' < "+df+" ORDER BY id")
	add("tsub/vsIntervalSelect", "SELECT id, "+df+" >= INTERVAL '60 days' AS b FROM iv_t ORDER BY id")
	add("tsub/vsIntervalCols", "SELECT a.id AS l, b.id AS r FROM iv_t a JOIN iv_t b ON a.ts - b.ts > INTERVAL '30 days' ORDER BY l, r")
	add("tsub/vsIntervalColsWhere", "SELECT a.id AS l, b.id AS r FROM iv_t a, iv_t b WHERE INTERVAL '30 days' < a.ts - b.ts ORDER BY l, r")
	add("tsub/vsIntervalBetween", "SELECT id FROM iv_t WHERE "+df+" BETWEEN INTERVAL '30 days' AND INTERVAL '61 days' ORDER BY id")
	add("tsub/vsIntervalIn", "SELECT id FROM iv_t WHERE "+df+" IN (INTERVAL '-1 day', INTERVAL '1452 hours') ORDER BY id")
	add("tsub/vsIntervalCase", "SELECT id, CASE "+df+" WHEN INTERVAL '-1 day' THEN 'neg' ELSE 'other' END AS k FROM iv_t ORDER BY id")
	// The workaround the refusal names, answered (controls).
	add("tsub/workaround", "SELECT id FROM iv_t WHERE ts > "+t0+" + INTERVAL '30 days' ORDER BY id")
	add("tsub/workaroundCols", "SELECT a.id AS l, b.id AS r FROM iv_t a JOIN iv_t b ON a.ts > b.ts + INTERVAL '30 days' ORDER BY l, r")
	add("tsub/dateVsInterval", "SELECT id FROM iv_t WHERE d - DATE '2024-01-01' > INTERVAL '30 days' ORDER BY id")
	add("tsub/tsDateVsInterval", "SELECT id FROM iv_t WHERE ts - DATE '2024-01-01' > INTERVAL '30 days' ORDER BY id")
	// The rest of the mixed class.
	add("tsub/plusInterval", "SELECT id, ("+df+") + INTERVAL '1 day' AS a FROM iv_t WHERE id = 1")
	add("tsub/minusInterval", "SELECT id, ("+df+") - INTERVAL '1 day' AS a FROM iv_t WHERE id = 1")
	add("tsub/intervalPlus", "SELECT id, INTERVAL '1 day' + ("+df+") AS a FROM iv_t WHERE id = 1")
	add("tsub/intervalMinus", "SELECT id, INTERVAL '1 day' - ("+df+") AS a FROM iv_t WHERE id = 1")
	add("tsub/plusIntervalWhere", "SELECT id FROM iv_t WHERE ("+df+") + INTERVAL '1 day' > INTERVAL '31 days' ORDER BY id")
	add("tsub/intervalPlusWhere", "SELECT id FROM iv_t WHERE INTERVAL '1 day' + ("+df+") > INTERVAL '31 days' ORDER BY id")
	add("tsub/coalesce", "SELECT id FROM iv_t WHERE COALESCE("+df+", INTERVAL '0 days') > INTERVAL '30 days' ORDER BY id")
	add("tsub/greatest", "SELECT id FROM iv_t WHERE GREATEST("+df+", INTERVAL '40 days') > INTERVAL '50 days' ORDER BY id")
	add("tsub/least", "SELECT id FROM iv_t WHERE LEAST("+df+", INTERVAL '40 days') < INTERVAL '35 days' ORDER BY id")
	// The same LEAST over other bounds: base answered tsub/least and
	// tsub/leastGt right by coincidence and tsub/leastWide wrong (every row).
	add("tsub/leastGt", "SELECT id FROM iv_t WHERE LEAST("+df+", INTERVAL '40 days') > INTERVAL '35 days' ORDER BY id")
	add("tsub/leastWide", "SELECT id FROM iv_t WHERE LEAST("+df+", INTERVAL '100 days') < INTERVAL '31 days' ORDER BY id")
	add("tsub/nullif", "SELECT id FROM iv_t WHERE NULLIF("+df+", INTERVAL '-1 day') IS NULL ORDER BY id")
	add("tsub/case", "SELECT id FROM iv_t WHERE CASE WHEN id > 2 THEN "+df+" ELSE INTERVAL '100 days' END > INTERVAL '30 days' ORDER BY id")
	add("tsub/tsMinusDiff", "SELECT id, ts - (ts - "+t0+") AS a FROM iv_t ORDER BY id")
	add("tsub/epochVsNumber", "SELECT id FROM iv_t WHERE extract(epoch FROM "+df+") > 3000000 ORDER BY id")
	add("tsub/intervalTimesDiff", "SELECT id FROM iv_t WHERE INTERVAL '1 day' * ("+df+") > INTERVAL '1 day' ORDER BY id")
	out = append(out, ivCellsTD()...)
	// The reviewer's lesser points (behaviour unchanged, measured).
	add("lp/plusNull", "SELECT INTERVAL '1 day' + NULL AS a")
	add("lp/casePlusNull", "SELECT CASE WHEN true THEN INTERVAL '1 day' + NULL END AS a")
	add("lp/multiField", "SELECT CAST(INTERVAL '1 day 2 hours' AS TEXT) AS a")
	return out
}

// ivTypeNames is a declared header ("name:type,…") with this engine's type
// names spelled as PostgreSQL's (TIMESTAMP is timestamp).
func ivTypeNames(h string) string {
	cols := strings.Split(h, ",")
	for i, c := range cols {
		if n, t, ok := strings.Cut(c, ":"); ok {
			cols[i] = n + ":" + strings.ToLower(t)
		}
	}
	return strings.Join(cols, ",")
}

// ivCellsTD is the timestamp difference as an INTERVAL-valued producer
// (#1713d round 6, the maintainer's ruling): PostgreSQL types `timestamp -
// timestamp` interval, this engine computes a double of milliseconds
// (temporal r16), so no use of it may answer from that number. Each use is
// decided as PostgreSQL decides it for an interval: a publication, key,
// aggregate or function argument is refused 0A000; a number beside it is
// PostgreSQL's own 42883 / 42804 / 42846; IS [NOT] NULL answers. The td/ctl*
// cells are the controls the rule must not reach (single-timestamp epoch
// arithmetic, date - date, timestamp ± interval as a key).
func ivCellsTD() []ivCell {
	var out []ivCell
	add := func(name, sql string) { out = append(out, ivCell{name: name, sql: sql, typed: true}) }
	t0 := "TIMESTAMP '2024-01-01 00:00:00'"
	df := "ts - " + t0
	add("td/groupBy", "SELECT count(*) AS n FROM iv_t GROUP BY "+df+" ORDER BY n")
	add("td/orderBy", "SELECT id FROM iv_t ORDER BY "+df+", id")
	add("td/distinct", "SELECT DISTINCT "+df+" AS v FROM iv_t ORDER BY v")
	add("td/avg", "SELECT avg("+df+") AS v FROM iv_t")
	add("td/min", "SELECT min("+df+") AS v FROM iv_t")
	add("td/max", "SELECT max("+df+") AS v FROM iv_t")
	add("td/count", "SELECT count("+df+") AS v FROM iv_t")
	add("td/ltNumber", "SELECT id FROM iv_t WHERE "+df+" < 1000 ORDER BY id")
	add("td/eqNumber", "SELECT id FROM iv_t WHERE "+df+" = 2628000000 ORDER BY id")
	add("td/div", "SELECT id, ("+df+") / 1000 AS v FROM iv_t ORDER BY id")
	add("td/divWhere", "SELECT id FROM iv_t WHERE ("+df+") / 1000 > 3000000 ORDER BY id")
	add("td/times2", "SELECT id, ("+df+") * 2 AS v FROM iv_t ORDER BY id")
	add("td/plus1", "SELECT id, ("+df+") + 1 AS v FROM iv_t ORDER BY id")
	add("td/onePlus", "SELECT id, 1 + ("+df+") AS v FROM iv_t ORDER BY id")
	add("td/minus1", "SELECT id, ("+df+") - 1 AS v FROM iv_t ORDER BY id")
	add("td/negate", "SELECT id, -("+df+") AS v FROM iv_t ORDER BY id")
	add("td/datePartEpoch", "SELECT id, date_part('epoch', "+df+") AS e FROM iv_t ORDER BY id")
	add("td/extractDay", "SELECT id, extract(day FROM "+df+") AS e FROM iv_t ORDER BY id")
	add("td/castBigint", "SELECT id, CAST("+df+" AS BIGINT) AS v FROM iv_t ORDER BY id")
	add("td/castDouble", "SELECT id, CAST("+df+" AS DOUBLE PRECISION) AS v FROM iv_t ORDER BY id")
	add("td/castText", "SELECT id, CAST("+df+" AS TEXT) AS v FROM iv_t ORDER BY id")
	add("td/caseNumber", "SELECT id, CASE WHEN id = 1 THEN "+df+" ELSE 0 END AS v FROM iv_t ORDER BY id")
	add("td/caseAlone", "SELECT id, CASE WHEN id = 1 THEN "+df+" END AS v FROM iv_t ORDER BY id")
	add("td/coalesceNumber", "SELECT id, COALESCE("+df+", 0) AS v FROM iv_t ORDER BY id")
	add("td/nullifNumber", "SELECT id, NULLIF("+df+", 0) AS v FROM iv_t ORDER BY id")
	add("td/greatestNumber", "SELECT id, GREATEST("+df+", 0) AS v FROM iv_t ORDER BY id")
	add("td/leastInterval", "SELECT id, LEAST("+df+", INTERVAL '40 days') AS v FROM iv_t ORDER BY id")
	add("td/abs", "SELECT id, abs("+df+") AS v FROM iv_t ORDER BY id")
	add("td/round", "SELECT id, round("+df+") AS v FROM iv_t ORDER BY id")
	add("td/toChar", "SELECT id, to_char("+df+", 'DD HH24') AS v FROM iv_t ORDER BY id")
	add("td/joinKey", "SELECT a.id AS l, b.id AS r FROM iv_t a JOIN iv_t b ON (a.ts - "+t0+") = (b.ts - "+t0+") ORDER BY l, r")
	add("td/joinNumber", "SELECT a.id AS l, b.id AS r FROM iv_t a JOIN iv_t b ON a.id = b.id AND a.ts - b.ts > 1000 ORDER BY l, r")
	add("td/windowArg", "SELECT id, lag("+df+") OVER (ORDER BY id) AS v FROM iv_t ORDER BY id")
	add("td/windowSum", "SELECT id, sum("+df+") OVER (ORDER BY id) AS v FROM iv_t ORDER BY id")
	add("td/windowOrder", "SELECT id, sum(id) OVER (ORDER BY "+df+") AS v FROM iv_t ORDER BY id")
	add("td/windowPartition", "SELECT id, count(*) OVER (PARTITION BY "+df+") AS v FROM iv_t ORDER BY id")
	add("td/havingInterval", "SELECT id FROM iv_t GROUP BY id HAVING sum("+df+") > INTERVAL '40 days' ORDER BY id")
	add("td/derived", "SELECT count(*) AS n FROM (SELECT "+df+" AS d FROM iv_t) x")
	add("td/derivedWhere", "SELECT count(*) AS n FROM (SELECT "+df+" AS d FROM iv_t) x WHERE d > INTERVAL '40 days'")
	add("td/cte", "WITH c AS (SELECT "+df+" AS d FROM iv_t) SELECT count(*) AS n FROM c")
	add("td/cteUnused", "WITH c AS (SELECT "+df+" AS d FROM iv_t) SELECT count(*) AS n FROM iv_t")
	add("td/scalarSubquery", "SELECT (SELECT max("+df+") FROM iv_t) AS v")
	add("td/unionArm", "SELECT v FROM (SELECT "+df+" AS v FROM iv_t UNION ALL SELECT INTERVAL '1 day') x ORDER BY v")
	add("td/array", "SELECT ARRAY["+df+"] AS v FROM iv_t WHERE id = 1")
	add("td/concat", "SELECT id, ("+df+") || 'x' AS v FROM iv_t WHERE id = 1")
	add("td/vsUnknown", "SELECT id FROM iv_t WHERE "+df+" > '30 days' ORDER BY id")
	add("td/unknownRight", "SELECT id, ts - '2024-01-01' AS v FROM iv_t ORDER BY id")
	add("td/unknownLeft", "SELECT id, '2024-03-01' - ts AS v FROM iv_t ORDER BY id")
	add("td/nullRight", "SELECT id, ts - NULL AS v FROM iv_t ORDER BY id")
	add("td/tzCast", "SELECT id, CAST(ts AS TIMESTAMPTZ) - "+t0+" AS v FROM iv_t ORDER BY id")
	add("td/dateCast", "SELECT id, CAST(d AS TIMESTAMP) - ts AS v FROM iv_t ORDER BY id")
	add("td/shiftedMinus", "SELECT id, (ts + INTERVAL '1 day') - ts AS v FROM iv_t ORDER BY id")
	add("td/isNull", "SELECT id, ("+df+") IS NULL AS b FROM iv_t ORDER BY id")
	add("td/isNotNull", "SELECT id FROM iv_t WHERE ("+df+") IS NOT NULL ORDER BY id")
	// #1713d round 7: a difference whose TIMESTAMP operand is an aggregate, a
	// window function or a scalar subquery (the declared-type walk could not
	// type them, so these answered the millisecond number through round 6).
	ag := "max(ts) - min(ts)"
	add("td/aggPublish", "SELECT "+ag+" AS v FROM iv_t")
	add("td/aggEpoch", "SELECT extract(epoch FROM "+ag+") AS e FROM iv_t")
	add("td/aggDatePart", "SELECT date_part('epoch', "+ag+") AS e FROM iv_t")
	add("td/aggDiv", "SELECT ("+ag+") / 1000 AS v FROM iv_t")
	add("td/aggCastText", "SELECT CAST("+ag+" AS TEXT) AS v FROM iv_t")
	add("td/aggHavingNumber", "SELECT count(*) AS n FROM iv_t HAVING "+ag+" > 1000")
	add("td/aggHavingInterval", "SELECT count(*) AS n FROM iv_t HAVING "+ag+" > INTERVAL '1 day'")
	add("td/aggOrderBy", "SELECT id FROM iv_t GROUP BY id ORDER BY "+ag+", id")
	add("td/aggGrouped", "SELECT id, "+ag+" AS v FROM iv_t GROUP BY id ORDER BY id")
	add("td/aggMinusLit", "SELECT max(ts) - "+t0+" AS v FROM iv_t")
	add("td/aggMinusUnknown", "SELECT max(ts) - '2024-01-01' AS v FROM iv_t")
	add("td/aggCoalesce", "SELECT COALESCE(max(ts), "+t0+") - min(ts) AS v FROM iv_t")
	add("td/aggDateTrunc", "SELECT date_trunc('day', max(ts)) - min(ts) AS v FROM iv_t")
	add("td/aggShifted", "SELECT (max(ts) + INTERVAL '1 day') - min(ts) AS v FROM iv_t")
	add("td/aggIsNull", "SELECT ("+ag+") IS NULL AS b FROM iv_t")
	lagd := "ts - lag(ts) OVER (ORDER BY id)"
	add("td/winLag", "SELECT id, "+lagd+" AS v FROM iv_t ORDER BY id")
	add("td/winLead", "SELECT id, lead(ts) OVER (ORDER BY id) - ts AS v FROM iv_t ORDER BY id")
	add("td/winFirst", "SELECT id, ts - first_value(ts) OVER (ORDER BY id) AS v FROM iv_t ORDER BY id")
	add("td/winLast", "SELECT id, ts - last_value(ts) OVER (ORDER BY id) AS v FROM iv_t ORDER BY id")
	add("td/winNth", "SELECT id, ts - nth_value(ts, 1) OVER (ORDER BY id) AS v FROM iv_t ORDER BY id")
	add("td/winMaxOver", "SELECT id, ts - max(ts) OVER () AS v FROM iv_t ORDER BY id")
	add("td/winLagNumber", "SELECT id, "+lagd+" > 1000 AS b FROM iv_t ORDER BY id")
	add("td/winLagInterval", "SELECT id, "+lagd+" > INTERVAL '1 day' AS b FROM iv_t ORDER BY id")
	add("td/winLagEpoch", "SELECT id, extract(epoch FROM "+lagd+") AS e FROM iv_t ORDER BY id")
	add("td/winOfAgg", "SELECT id, max(ts) - lag(max(ts)) OVER (ORDER BY id) AS v FROM iv_t GROUP BY id ORDER BY id")
	add("td/winLagIsNull", "SELECT id, ("+lagd+") IS NULL AS b FROM iv_t ORDER BY id")
	add("td/subPublish", "SELECT id, (SELECT max(ts) FROM iv_t) - ts AS v FROM iv_t ORDER BY id")
	add("td/subWhereNumber", "SELECT id FROM iv_t WHERE ts - (SELECT min(ts) FROM iv_t) > 1000 ORDER BY id")
	add("td/subWhereInterval", "SELECT id FROM iv_t WHERE ts - (SELECT min(ts) FROM iv_t) > INTERVAL '30 days' ORDER BY id")
	add("td/subBoth", "SELECT (SELECT max(ts) FROM iv_t) - (SELECT min(ts) FROM iv_t) AS v")
	add("td/subUnion", "SELECT id, ts - (SELECT max(ts) FROM iv_t UNION SELECT max(ts) FROM iv_t) AS v FROM iv_t ORDER BY id")
	add("td/subWindow", "SELECT id, (SELECT lead(ts) OVER (ORDER BY id) FROM iv_t ORDER BY id LIMIT 1) - ts AS v FROM iv_t ORDER BY id")
	// Controls over the same producers: no timestamp subtraction.
	add("td/ctlAggCompare", "SELECT max(ts) > min(ts) AS b FROM iv_t")
	// (A shifted timestamp is cast to text, as ctl/tsShiftProject is: the
	// arms render a published timestamp by its carrier.)
	add("td/ctlAggShift", "SELECT CAST(max(ts) - INTERVAL '1 day' AS TEXT) AS v FROM iv_t")
	add("td/ctlAggEpochDiff", "SELECT extract(epoch FROM max(ts)) - extract(epoch FROM min(ts)) > 1000 AS b FROM iv_t")
	add("td/ctlAggDateDiff", "SELECT max(d) - min(d) AS v FROM iv_t")
	add("td/ctlWinCompare", "SELECT id, ts > lag(ts) OVER (ORDER BY id) AS b FROM iv_t ORDER BY id")
	add("td/ctlWinShift", "SELECT id, CAST(lag(ts) OVER (ORDER BY id) + INTERVAL '1 day' AS TEXT) AS v FROM iv_t ORDER BY id")
	add("td/ctlSubCompare", "SELECT id, ts > (SELECT min(ts) FROM iv_t) AS b FROM iv_t ORDER BY id")
	// Controls: not a timestamp difference.
	add("td/ctlEpochDiff", "SELECT id, extract(epoch FROM ts) - extract(epoch FROM "+t0+") > 3000000 AS b FROM iv_t ORDER BY id")
	add("td/ctlDateDiffDays", "SELECT id, d - DATE '2024-01-01' AS v FROM iv_t ORDER BY id")
	add("td/ctlShiftKey", "SELECT id FROM iv_t ORDER BY ts + INTERVAL '1 day', id")
	add("td/ctlUnknownInterval", "SELECT id, CAST(ts - '1 day' AS TEXT) AS v FROM iv_t ORDER BY id")
	add("td/ctlCompareTs", "SELECT a.id AS l, b.id AS r FROM iv_t a JOIN iv_t b ON a.ts > b.ts + INTERVAL '60 days' ORDER BY l, r")
	return out
}

// ivTDNumberFree are the td/* and tsub/* cells whose statement uses a
// timestamp difference and that may ANSWER (IS [NOT] NULL: the number and the
// interval are NULL on the same rows). Every other such cell must be a
// refusal on every arm: TestArc1713dTimestampDifferenceIsNeverANumber.
var ivTDNumberFree = map[string]bool{"td/isNull": true, "td/isNotNull": true,
	"td/aggIsNull": true, "td/winLagIsNull": true}
