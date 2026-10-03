// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A NUMERIC VALUE IS EXACT WHEREVER A NUMERIC OPERAND MEETS AN INTEGER OR A
// CONSTANT (#1386, #1392, #1450). PostgreSQL types three operand kinds
// `numeric` that this engine carried on the float8 rung:
//
//   - an integer CAST beside a numeric (`CAST(t.i AS INTEGER) / t.n`,
//     `CAST(t.i AS INTEGER) * 0.1`), which PostgreSQL computes as int4/int8
//     promoted to numeric — the same value the bare integer column gives;
//   - a numeric constant inside a choice (CASE / COALESCE / NULLIF /
//     GREATEST / LEAST), under unary minus, or as a scalar subquery's answer,
//     which keeps its own digits (`COALESCE(14.0000000000000000001, 0) = 14`
//     is false);
//   - an explicit integer CAST of a numeric expression, which rounds the
//     exact value half away from zero (`CAST(5 / 2.0 AS INTEGER)` is 3).
//
// The table is operand kind × consumer, every cell on five arms against
// PostgreSQL 17.11's full sorted rows and declared type class
// (testdata/arc_nx_numeric_carrier_pg17.tsv). A consumer that writes a table
// (INSERT … SELECT, CREATE TABLE AS) runs on the two single-process arms:
// the coordinator refuses a query-sourced write by name
// (TestTheCoordinatorRefusesAQuerySourcedWriteByName).
//
// The fixture is arc SS's ss_t / ss_i (ssTables): i int, b bigint (row 3 is
// 9000000000, past 2^53 once multiplied), n numeric(10,2) with 2.25, -3.50,
// 10.00, 0.00, 0.01 and a NULL row, f double.

type nxCell struct {
	name, sql string
	ordered   bool
	embedded  bool // a write: single-process arms only
}

// nxOperand is one operand kind: its SQL over the alias t (an ss_t row) and
// a discriminating value v the comparison / membership / join consumers test
// it against — chosen so the exact value and the float8 carrier answer
// differently.
type nxOperand struct {
	key, e, v string
}

const nxK = "14.0000000000000000001"

func nxOperands() []nxOperand {
	return []nxOperand{
		// An integer CAST beside a numeric (#1450). The bare integer column is
		// the control: the CAST must answer what the column answers.
		{"icColDivCtl", "t.i / NULLIF(t.n, 0)", "1.3333333333333333"},
		{"icColDiv", "CAST(t.i AS INTEGER) / NULLIF(t.n, 0)", "1.3333333333333333"},
		{"icColMod", "CAST(t.i AS INTEGER) % NULLIF(t.n, 0)", "0.00"},
		{"icBigDiv", "t.n / NULLIF(CAST(t.b AS BIGINT), 0)", "0.0000000011111111111111111111"},
		{"icMulLitCtl", "t.i * 0.1", "0.3"},
		{"icMulLit", "CAST(t.i AS INTEGER) * 0.1", "0.3"},
		{"icBigMulCtl", "t.b * 10000000 * t.n - 3", "899999999999999997.00"},
		{"icBigMul", "CAST(t.b AS BIGINT) * 10000000 * t.n - 3", "899999999999999997.00"},
		{"icSmallMul", "CAST(t.i AS SMALLINT) * 0.1", "0.3"},
		{"icLitMul", "CAST(3 AS INTEGER) * 0.1 + t.n", "2.55"},
		{"icLitBig", "CAST(9000000000 AS BIGINT) * 10000000 * CAST(10.00 AS NUMERIC(10,2)) - 3 + t.n", "899999999999999999.25"},
		{"icExprMul", "CAST(t.i + 1 AS INTEGER) * 0.1", "0.4"},
		{"icFloatSrc", "CAST(t.f AS INTEGER) * 0.1", "0.2"},
		{"icNumSrc", "CAST(t.n AS INTEGER) * 0.1", "0.2"},
		{"icPlusNum", "CAST(t.i AS INTEGER) + t.n", "5.25"},
		// A numeric constant inside a choice, under unary minus, in a scalar
		// subquery (#1386). The bare constant is the control.
		{"ncBareCtl", nxK, "14"},
		{"ncCase", "CASE WHEN t.id < 100 THEN " + nxK + " ELSE 0 END", "14"},
		{"ncCaseConst", "CASE WHEN true THEN " + nxK + " END", "14"},
		{"ncCoalesce", "COALESCE(" + nxK + ", 0)", "14"},
		{"ncCoalesceCol", "COALESCE(t.b, " + nxK + ")", "14"},
		{"ncNullif", "NULLIF(" + nxK + ", 0)", "14"},
		{"ncGreatestCol", "GREATEST(t.n, " + nxK + ")", "14"},
		{"ncLeast", "LEAST(" + nxK + ", 20)", "14"},
		{"ncNeg", "-(" + nxK + ")", "-14"},
		{"ncNegNeg", "-(-" + nxK + ")", "14"},
		{"ncSub", "(SELECT " + nxK + ")", "14"},
		{"ncSubCoalesce", "(SELECT COALESCE(" + nxK + ", 0))", "14"},
		{"ncCastNum", "CAST(" + nxK + " AS NUMERIC)", "14"},
		{"ncCastNumText", "CAST('" + nxK + "' AS NUMERIC)", "14"},
		{"ncCaseChoice", "CASE WHEN " + nxK + " > 14 THEN 14 ELSE 13.25 END", "14"},
		// An explicit integer CAST of a numeric expression (#1392); the bare
		// literal is the control.
		{"rcDiv", "CAST(5 / 2.0 AS INTEGER)", "3"},
		{"rcSqrt", "CAST(SQRT(6.25) AS INTEGER)", "3"},
		{"rcPower", "CAST(POWER(2.5, 1) AS INTEGER)", "3"},
		{"rcMul", "CAST(2.5 * 1 AS INTEGER)", "3"},
		{"rcLitCtl", "CAST(2.5 AS INTEGER)", "3"},
		{"rcAbs", "CAST(ABS(2.5) AS INTEGER)", "3"},
		{"rcNegDiv", "CAST(-(5 / 2.0) AS INTEGER)", "-3"},
		{"rcNegMul", "CAST(-(2.5 * 1) AS INTEGER)", "-3"},
		{"rcSmallDiv", "CAST(5 / 2.0 AS SMALLINT)", "3"},
		{"rcBigDiv", "CAST(5 / 2.0 AS BIGINT)", "3"},
		{"rcBigMul", "CAST(2.5 * 1 AS BIGINT)", "3"},
		{"rcCol", "CAST(t.n + 0.25 AS INTEGER)", "3"},
		{"rcColDiv", "CAST(t.n / 0.9 AS INTEGER)", "3"},
		{"rcCoalesce", "CAST(COALESCE(2.5, 0) AS INTEGER)", "3"},
		// A QUOTIENT whose integer operand is an integer CAST under a
		// construct an integer's type passes through: NULLIF,
		// COALESCE, CASE, GREATEST, abs, unary minus, `+ 0`, `* 1`. Each makes
		// the decision the bare cast makes (icColDiv, icBigDiv): PostgreSQL
		// types all of them alike.
		{"wqBigPlus0", "t.n / NULLIF(CAST(t.b AS BIGINT) + 0, 0)", "0.0000000011111111111111111111"},
		{"wqBigMul1", "t.n / NULLIF(CAST(t.b AS BIGINT) * 1, 0)", "0.0000000011111111111111111111"},
		{"wqBigCoalesce", "t.n / NULLIF(COALESCE(CAST(t.b AS BIGINT), 1), 0)", "0.0000000011111111111111111111"},
		{"wqBigNeg", "-t.n / NULLIF(-CAST(t.b AS BIGINT), 0)", "0.0000000011111111111111111111"},
		{"wqBigAbs", "t.n / NULLIF(ABS(CAST(t.b AS BIGINT)), 0)", "0.0000000011111111111111111111"},
		{"wqColPlus0", "(CAST(t.i AS INTEGER) + 0) / NULLIF(t.n, 0)", "1.3333333333333333"},
		{"wqColCoalesce", "COALESCE(CAST(t.i AS INTEGER), 0) / NULLIF(t.n, 0)", "1.3333333333333333"},
		{"wqColCase", "CASE WHEN t.id > 0 THEN CAST(t.i AS INTEGER) END / NULLIF(t.n, 0)", "1.3333333333333333"},
		{"wqColGreatest", "GREATEST(CAST(t.i AS INTEGER), -100) / NULLIF(t.n, 0)", "1.3333333333333333"},
		// A QUOTIENT over an operand a CAST made exact through a NUMERIC
		// construct: the integer CAST under numeric arithmetic,
		// a bare NUMERIC cast of the cast, of an integer column and of a
		// numeric column. Each keeps the double the quotient computed before
		// the cast was exact (expr.castMadeExactIn), as the bare integer CAST
		// does; the one-scale quotient answered 1.333333 and moved every
		// comparison off PostgreSQL.
		{"wnColMul10", "(CAST(t.i AS INTEGER) * 1.0) / NULLIF(t.n, 0)", "1.3333333333333333"},
		{"wnColPlus00", "(CAST(t.i AS INTEGER) + 0.0) / NULLIF(t.n, 0)", "1.3333333333333333"},
		{"wnColCastNum", "CAST(CAST(t.i AS INTEGER) AS NUMERIC) / NULLIF(t.n, 0)", "1.3333333333333333"},
		{"wnColBareNum", "CAST(t.i AS NUMERIC) / NULLIF(t.n, 0)", "1.3333333333333333"},
		{"wnBigMul10", "t.n / NULLIF(CAST(t.b AS BIGINT) * 1.0, 0)", "0.0000000011111111111111111111"},
		{"wnBigCastNum", "t.n / NULLIF(CAST(CAST(t.b AS BIGINT) AS NUMERIC), 0)", "0.0000000011111111111111111111"},
		{"wnBigBareNum", "t.n / NULLIF(CAST(t.b AS NUMERIC), 0)", "0.0000000011111111111111111111"},
		{"wnBigPlusN", "t.n / NULLIF(CAST(t.b AS BIGINT) + t.n * 0, 0)", "0.0000000011111111111111111111"},
		{"wnNumBareNum", "CAST(t.n AS NUMERIC) / 3", "0.00333333333333333333"},
	}
}

// nxCells is the whole table: every operand × every consumer, then the
// issues' own statements and the column-origin rows.
func nxCells() []nxCell {
	var out []nxCell
	add := func(name, sql string) { out = append(out, nxCell{name: name, sql: sql}) }
	addOrd := func(name, sql string) { out = append(out, nxCell{name: name, sql: sql, ordered: true}) }
	addW := func(name, sql string) { out = append(out, nxCell{name: name, sql: sql, embedded: true}) }
	for k, o := range nxOperands() {
		e := o.e
		eb := strings.ReplaceAll(e, "t.", "b.")
		base := "nx/" + o.key + "/"
		add(base+"proj", "SELECT t.id, "+e+" AS x FROM ss_t t")
		add(base+"cmp", "SELECT t.id FROM ss_t t WHERE "+e+" = "+o.v)
		add(base+"plusN", "SELECT t.id, "+e+" + t.n AS x FROM ss_t t")
		add(base+"castText", "SELECT t.id, CAST("+e+" AS TEXT) AS x FROM ss_t t")
		add(base+"castInt", "SELECT t.id, CAST("+e+" AS INTEGER) AS x FROM ss_t t WHERE t.id <> 3")
		add(base+"agg", "SELECT sum("+e+") AS s, max("+e+") AS m FROM ss_t t WHERE t.id <> 3")
		add(base+"groupBy", "SELECT "+e+" AS k, count(*) AS c FROM ss_t t GROUP BY 1")
		addOrd(base+"orderBy", "SELECT t.id FROM ss_t t ORDER BY "+e+", t.id")
		add(base+"winIn", "SELECT t.id, sum("+e+") OVER (ORDER BY t.id) AS x FROM ss_t t WHERE t.id <> 3")
		add(base+"winKey", "SELECT t.id, count(*) OVER (PARTITION BY "+e+" = "+o.v+") AS x FROM ss_t t")
		add(base+"join", "SELECT t.id, b.id FROM ss_t t JOIN ss_t b ON "+e+" = "+eb+" + 0 * b.n AND t.id = b.id WHERE "+e+" = "+o.v)
		add(base+"inList", "SELECT t.id FROM ss_t t WHERE "+e+" IN ("+o.v+", -999)")
		add(base+"inSub", "SELECT t.id FROM ss_t t WHERE "+e+" IN (SELECT CAST("+o.v+" AS NUMERIC) FROM ss_i)")
		add(base+"derived", "SELECT s.id, s.x FROM (SELECT t.id, "+e+" AS x FROM ss_t t) s WHERE s.x = "+o.v)
		add(base+"json", "SELECT t.id, CAST(json_build_object('v', "+e+") AS TEXT) AS x FROM ss_t t WHERE t.id <> 3")
		// The target table: INTEGER and NUMERIC(10,2) by DDL, DOUBLE
		// PRECISION as an empty CTAS of ss_t.f (the one double spelling both
		// engines' DDL take).
		for _, dst := range [][2]string{
			{"Int", "(id bigint, x integer)"},
			{"Num", "(id bigint, x numeric(10,2))"},
			{"Dbl", "AS SELECT t.id, t.f AS x FROM ss_t t WHERE t.id < 0"},
		} {
			tbl := fmt.Sprintf("nx_w%d_%s", k, strings.ToLower(dst[0]))
			addW(base+"insert"+dst[0], "DROP TABLE IF EXISTS "+tbl+" ;; CREATE TABLE "+tbl+" "+dst[1]+
				" ;; INSERT INTO "+tbl+" SELECT t.id, "+e+" FROM ss_t t WHERE t.id <> 3 ;; SELECT id, x FROM "+tbl)
		}
		ct := fmt.Sprintf("nx_c%d", k)
		addW(base+"ctas", "DROP TABLE IF EXISTS "+ct+" ;; CREATE TABLE "+ct+" AS SELECT t.id, "+e+" AS x FROM ss_t t ;; SELECT id, x FROM "+ct)
	}
	// THE ISSUES' OWN STATEMENTS.
	add("issue/1392/six", "SELECT CAST(5 / 2.0 AS INTEGER), CAST(SQRT(6.25) AS INTEGER), CAST(POWER(2.5, 1) AS INTEGER), CAST(2.5 * 1 AS INTEGER), CAST(2.5 AS INTEGER), CAST(ABS(2.5) AS INTEGER)")
	add("issue/1392/sixNeg", "SELECT CAST(-(5 / 2.0) AS INTEGER), CAST(-SQRT(6.25) AS INTEGER), CAST(-POWER(2.5, 1) AS INTEGER), CAST(-(2.5 * 1) AS INTEGER), CAST(-2.5 AS INTEGER), CAST(-ABS(2.5) AS INTEGER)")
	add("issue/1392/sixSmallint", "SELECT CAST(5 / 2.0 AS SMALLINT), CAST(SQRT(6.25) AS SMALLINT), CAST(POWER(2.5, 1) AS SMALLINT), CAST(2.5 * 1 AS SMALLINT), CAST(2.5 AS SMALLINT), CAST(ABS(2.5) AS SMALLINT)")
	add("issue/1392/sixBigint", "SELECT CAST(5 / 2.0 AS BIGINT), CAST(SQRT(6.25) AS BIGINT), CAST(POWER(2.5, 1) AS BIGINT), CAST(2.5 * 1 AS BIGINT), CAST(2.5 AS BIGINT), CAST(ABS(2.5) AS BIGINT)")
	add("issue/1386/coalesceWhere", "SELECT a.id FROM ss_t a WHERE COALESCE("+nxK+", 0) = 14")
	add("issue/1386/case", "SELECT CASE WHEN true THEN "+nxK+" END = 14 AS x")
	add("issue/1386/least", "SELECT LEAST("+nxK+", 20) = 14 AS x")
	add("issue/1386/negNeg", "SELECT -(-"+nxK+") = 14 AS x")
	add("issue/1386/subCoalesce", "SELECT (SELECT COALESCE("+nxK+", 0)) AS x")
	add("issue/1386/caseCond", "SELECT a.id FROM ss_t a WHERE CASE WHEN "+nxK+" > 14 THEN 14 ELSE 13.25 END = 14")
	add("issue/1386/greatestCol", "SELECT a.id, GREATEST(a.n, "+nxK+") AS x FROM ss_t a")
	add("issue/1386/coalesceCol", "SELECT a.id, COALESCE(a.b, "+nxK+") AS x FROM ss_t a")
	add("issue/1386/subIn", "SELECT count(*) AS n FROM ss_t a WHERE (SELECT COALESCE("+nxK+", 0)) IN (SELECT r.n FROM ss_t r)")
	add("issue/1450/div", "SELECT t.id, CAST(t.i AS INTEGER) / t.n AS x FROM ss_t t WHERE t.id IN (1, 2, 3, 5)")
	add("issue/1450/mod", "SELECT t.id, CAST(t.i AS INTEGER) % t.n AS x FROM ss_t t WHERE t.id = 5")
	add("issue/1450/bigDiv", "SELECT t.id, t.n / CAST(t.b AS BIGINT) AS x FROM ss_t t WHERE t.id = 3")
	// COLUMN ORIGINS: the integer CAST's column read from a derived table, a
	// CTE, a set operation, VALUES and a window output, beside a numeric.
	origins := [][2]string{
		{"stored", "ss_t t"},
		{"derived", "(SELECT id, i, b, n FROM ss_t) t"},
		{"cte", "w t"},
		{"union", "(SELECT id, i, b, n FROM ss_t WHERE id <= 3 UNION ALL SELECT id, i, b, n FROM ss_t WHERE id > 3) t"},
		{"values", "(VALUES (1, 3, 30, CAST(2.25 AS NUMERIC(10,2))), (3, 5, 9000000000, CAST(10.00 AS NUMERIC(10,2)))) AS t(id, i, b, n)"},
		{"window", "(SELECT id, max(i) OVER (PARTITION BY id) AS i, max(b) OVER (PARTITION BY id) AS b, n FROM ss_t) t"},
		{"subquery", "(SELECT id, (SELECT u.i FROM ss_t u WHERE u.id = s.id) AS i, b, n FROM ss_t s) t"},
	}
	for _, og := range origins {
		pre := ""
		if og[0] == "cte" {
			pre = "WITH w AS (SELECT id, i, b, n FROM ss_t) "
		}
		add("origin/"+og[0]+"/mulLit", pre+"SELECT t.id, CAST(t.i AS INTEGER) * 0.1 AS x FROM "+og[1])
		add("origin/"+og[0]+"/div", pre+"SELECT t.id, CAST(t.i AS INTEGER) / NULLIF(t.n, 0) AS x FROM "+og[1])
		add("origin/"+og[0]+"/bigMul", pre+"SELECT t.id, CAST(t.b AS BIGINT) * 10000000 * t.n - 3 AS x FROM "+og[1])
		add("origin/"+og[0]+"/roundCast", pre+"SELECT t.id, CAST(t.n + 0.25 AS INTEGER) AS x FROM "+og[1])
	}
	// THE ROUNDING CAST'S OPERAND ORIGIN (#1392): a value PostgreSQL types
	// numeric that this engine carries in a double, read by an explicit
	// integer CAST from the expression itself and from a column a derived
	// table, a DISTINCT, an aggregate, a CTE, a set operation, VALUES, a window
	// and a scalar subquery materialized.
	for _, og := range [][2]string{
		{"inline", "SELECT t.id, CAST(5 / 2.0 + t.id * 0 AS INTEGER) AS y FROM ss_t t WHERE t.id < 3"},
		{"inlineSqrtN", "SELECT t.id, CAST(SQRT(t.n * 0 + 6.25) AS INTEGER) AS y FROM ss_t t WHERE t.id < 3"},
		{"inlineSqrtF", "SELECT t.id, CAST(SQRT(t.f * 0 + 6.25) AS INTEGER) AS y FROM ss_t t WHERE t.id < 3"},
		{"inlineSqrtI", "SELECT t.id, CAST(SQRT(t.i * 0 + 6.25) AS INTEGER) AS y FROM ss_t t WHERE t.id < 3"},
		{"derived", "SELECT s.id, CAST(s.x AS INTEGER) AS y FROM (SELECT t.id, 5 / 2.0 + t.id * 0 AS x FROM ss_t t WHERE t.id < 3) s"},
		{"distinct", "SELECT CAST(s.x AS INTEGER) AS y FROM (SELECT DISTINCT 5 / 2.0 + t.id * 0 AS x FROM ss_t t) s"},
		{"aggregate", "SELECT CAST(max(5 / 2.0 + t.id * 0) AS INTEGER) AS y FROM ss_t t"},
		{"groupBy", "SELECT t.o, CAST(max(SQRT(6.25 + t.id * 0)) AS INTEGER) AS y FROM ss_t t GROUP BY t.o"},
		{"cte", "WITH w AS (SELECT t.id, 5 / 2.0 + t.id * 0 AS x FROM ss_t t WHERE t.id < 3) SELECT w.id, CAST(w.x AS INTEGER) AS y FROM w"},
		{"union", "SELECT CAST(s.x AS INTEGER) AS y FROM (SELECT 5 / 2.0 AS x UNION ALL SELECT 7 / 2.0) s"},
		{"values", "SELECT CAST(v.x AS INTEGER) AS y FROM (VALUES (5 / 2.0), (7 / 2.0)) AS v(x)"},
		{"window", "SELECT t.id, CAST(max(5 / 2.0 + t.id * 0) OVER () AS INTEGER) AS y FROM ss_t t WHERE t.id < 3"},
		{"subquery", "SELECT CAST((SELECT 5 / 2.0) AS INTEGER) AS y"},
		{"join", "SELECT t.id, CAST(s.x AS INTEGER) AS y FROM ss_t t JOIN (SELECT u.id, 5 / 2.0 + u.id * 0 AS x FROM ss_t u) s ON s.id = t.id WHERE t.id < 3"},
		// EXTRACT over a temporal column inside the operand: a DAG stage
		// re-parses `EXTRACT(year FROM t.d)` as `year(t.d)`.
		{"extractDate", "SELECT t.id, CAST(extract(year FROM t.d) * 0 + 2.5 AS INTEGER) AS y FROM ss_t t WHERE t.id < 3"},
		{"extractTs", "SELECT t.id, CAST(5 / 2.0 + extract(second FROM t.ts) * 0 AS INTEGER) AS y FROM ss_t t WHERE t.id < 3"},
	} {
		add("roundOrigin/"+og[0], og[1])
	}
	// The integer ASSIGNMENT of the same materialized value (the DML door
	// reads the plan's category, ADR-0024 §2c), beside the CAST above.
	addW("roundOrigin/insertDistinct", "DROP TABLE IF EXISTS nx_rd ;; CREATE TABLE nx_rd (x integer) ;; "+
		"INSERT INTO nx_rd SELECT DISTINCT 5 / 2.0 + t.id * 0 FROM ss_t t ;; SELECT x FROM nx_rd")
	// ARC SS's PIN (coordinator.TestArcSSPinIntegerCastBesideNumericIsDouble,
	// which this arc's fix made agree with PostgreSQL and so deleted): an
	// integer CAST past 2^53 beside a numeric in a query's own SELECT list, and
	// a correlated re-run's outer value (spelled `CAST(v AS BIGINT)`) in the
	// subquery's own WHERE / NOT EXISTS predicate.
	addOrd("ssPin/notExistsBin", "SELECT t.id FROM ss_t t WHERE NOT EXISTS (SELECT 1 FROM ss_t u WHERE u.id = t.id AND u.b * 10000000 * u.n - 3 = t.b * 10000000 * t.n) ORDER BY t.id")
	addOrd("ssPin/corrWhereBin", "SELECT t.id, (SELECT count(*) FROM ss_i q WHERE q.id = 1 AND t.b * 10000000 * t.n - 3 = 900000000000000000) FROM ss_t t WHERE t.id = 3")
	addOrd("ssPin/constCastSelect", "SELECT CAST(9000000000 AS BIGINT) * 10000000 * CAST(10.00 AS NUMERIC(10,2)) - 3")
	// An INTEGER literal past int64: PostgreSQL types it numeric, and folds
	// a minus into the constant before typing it (doNegate), so
	// -9223372036854775808 is bigint and -(-9223372036854775808) the numeric
	// again (expr.FoldedNegatedLiteral). Its quotient keeps the double the
	// float64 box computed (castMadeExactIn).
	add("negLit/int64Min", "SELECT -9223372036854775808 AS x")
	add("negLit/int64MinPlus1", "SELECT -9223372036854775808 + 1 AS x")
	add("negLit/int64MinTimesN", "SELECT t.id, -9223372036854775808 * t.n AS x FROM ss_t t WHERE t.id < 3")
	add("negLit/pastInt64Min", "SELECT -9223372036854775809 AS x")
	add("negLit/pastInt64", "SELECT 9223372036854775808 AS x")
	add("negLit/wide20", "SELECT -99999999999999999999 AS x")
	add("negLit/int64MinMinus1", "SELECT - 9223372036854775808 - 1 AS x")
	add("negLit/pastInt64ToBigint", "SELECT CAST((-9223372036854775809) AS BIGINT) AS x")
	add("negLit/int64MinTimesMinus1", "SELECT -9223372036854775808 * -1 AS x")
	add("negLit/doubleNeg", "SELECT -(-9223372036854775808) AS x")
	add("negLit/parenNeg", "SELECT -(9223372036854775808) AS x")
	add("negLit/pastInt64Cmp", "SELECT 9223372036854775808 = 9223372036854775807 AS x")
	add("negLit/pastInt64Quot", "SELECT 9223372036854775808 / 2 AS x")
	add("negLit/pastInt64QuotCmp", "SELECT 9223372036854775808 / 7 = 1317624576693539401 AS x")
	add("negLit/pastInt64QuotCol", "SELECT t.id, 9223372036854775808 / NULLIF(t.n, 0) AS x FROM ss_t t WHERE t.id < 3")
	add("negLit/colOverPastInt64", "SELECT t.id, t.n / 9223372036854775808 AS x FROM ss_t t WHERE t.id < 3")
	add("bareCast/floatOperand", "SELECT t.id, CAST(t.f AS NUMERIC) * 0.1 AS x FROM ss_t t WHERE t.id IN (1, 5)")
	add("bareCast/intOperand", "SELECT t.id, CAST(t.i AS NUMERIC) * 0.1 AS x FROM ss_t t WHERE t.id IN (1, 5)")
	add("bareCast/numOperand", "SELECT t.id, CAST(t.n AS NUMERIC) / 3 AS x FROM ss_t t WHERE t.id IN (1, 5)")
	// The quotient over a NUMERIC-wrapped integer
	// CAST or a bare NUMERIC cast (a Ctl row has the integer
	// COLUMN where the cell has the CAST: the one-scale quotient, r19).
	for _, q := range [][2]string{
		{"mul10", "(CAST(t.i AS INTEGER) * 1.0) / t.n"}, {"mul10Ctl", "(t.i * 1.0) / t.n"},
		{"plus00", "(CAST(t.i AS INTEGER) + 0.0) / t.n"}, {"plus00Ctl", "(t.i + 0.0) / t.n"},
		{"castNum", "CAST(CAST(t.i AS INTEGER) AS NUMERIC) / t.n"}, {"castNumCtl", "CAST(t.i AS NUMERIC) / t.n"},
	} {
		addOrd("qn/"+q[0]+"/proj", "SELECT t.id, "+q[1]+" FROM ss_t t WHERE t.n <> 0 ORDER BY t.id")
		addOrd("qn/"+q[0]+"/cmp", "SELECT t.id FROM ss_t t WHERE t.n <> 0 AND "+q[1]+" = 1.3333333333333333 ORDER BY t.id")
		if !strings.HasSuffix(q[0], "Ctl") {
			addOrd("qn/"+q[0]+"/win", "SELECT t.id, count(*) OVER (PARTITION BY "+q[1]+" = 1.3333333333333333) FROM ss_t t WHERE t.n <> 0 ORDER BY t.id")
		}
	}
	for _, q := range [][2]string{
		{"bmul10", "t.n / (CAST(t.b AS BIGINT) * 1.0)"}, {"bmul10Ctl", "t.n / (t.b * 1.0)"},
		{"bcastNum", "t.n / CAST(CAST(t.b AS BIGINT) AS NUMERIC)"}, {"bcastNumCtl", "t.n / CAST(t.b AS NUMERIC)"},
		{"bplusN", "t.n / (CAST(t.b AS BIGINT) + t.n * 0)"}, {"bplusNCtl", "t.n / (t.b + t.n * 0)"},
	} {
		addOrd("qn/"+q[0]+"/proj", "SELECT t.id, "+q[1]+" FROM ss_t t WHERE t.b <> 0 ORDER BY t.id")
		addOrd("qn/"+q[0]+"/cmp", "SELECT t.id FROM ss_t t WHERE t.b <> 0 AND "+q[1]+" = 0.0000000011111111111111111111 ORDER BY t.id")
	}
	// A QUOTIENT OVER A FUNCTION THE EXACTNESS WALK PASSES THROUGH
	// (expr.CastExactnessArgs, the one list both halves read): round / ceil /
	// ceiling / floor / trunc / abs / sign over a bare NUMERIC cast or an
	// integer CAST under numeric arithmetic answer the argument's exact
	// DECIMAL, so the quotient keeps the double the argument's quotient keeps.
	// The runtime walk stopped at these five while the plan's passed some, and
	// the one-scale quotient moved every comparison off PostgreSQL. A (p,s)
	// cast, sqrt and an integer column are the controls.
	for _, f := range [][3]string{
		{"ceil", "ceil(X)", "1.3333333333333333"}, {"ceiling", "ceiling(X)", "1.3333333333333333"},
		{"floor", "floor(X)", "1.3333333333333333"}, {"round", "round(X)", "1.3333333333333333"},
		{"round2", "round(X, 2)", "1.3333333333333333"}, {"trunc", "trunc(X)", "1.3333333333333333"},
		{"absRound", "abs(round(X))", "1.3333333333333333"}, {"sign", "sign(X) * 4", "1.7777777777777778"},
	} {
		for _, o := range [][2]string{
			{"bareNum", "CAST(t.i AS NUMERIC)"}, {"icMul", "CAST(t.i AS INTEGER) * 1.0"}, {"icPlus", "CAST(t.i AS INTEGER) + 0.0"},
		} {
			e := strings.ReplaceAll(f[1], "X", o[1])
			addOrd("qf/"+f[0]+"/"+o[0]+"/cmp", "SELECT t.id FROM ss_t t WHERE t.n <> 0 AND "+e+" / t.n = "+f[2]+" ORDER BY t.id")
			if o[0] == "bareNum" {
				addOrd("qf/"+f[0]+"/"+o[0]+"/proj", "SELECT t.id, "+e+" / t.n AS x FROM ss_t t WHERE t.n <> 0 ORDER BY t.id")
			}
		}
	}
	for _, f := range [][2]string{{"ceil", "ceil(CAST(t.i AS NUMERIC))"}, {"round", "round(CAST(t.i AS NUMERIC))"}} {
		addOrd("qf/"+f[0]+"/win", "SELECT t.id, count(*) OVER (PARTITION BY "+f[1]+" / t.n = 1.3333333333333333) AS x FROM ss_t t WHERE t.n <> 0 ORDER BY t.id")
		addOrd("qf/"+f[0]+"/group", "SELECT "+f[1]+" / t.n = 1.3333333333333333 AS k, count(*) AS c FROM ss_t t WHERE t.n <> 0 GROUP BY 1 ORDER BY 1")
		addOrd("qf/"+f[0]+"/inList", "SELECT t.id FROM ss_t t WHERE t.n <> 0 AND "+f[1]+" / t.n IN (1.3333333333333333, -999) ORDER BY t.id")
	}
	for _, f := range [][2]string{
		{"roundB", "round(CAST(t.b AS NUMERIC))"}, {"ceilB", "ceil(CAST(t.b AS NUMERIC))"},
		{"roundBigMul", "round(CAST(t.b AS BIGINT) * 1.0)"}, {"floorBigPlus", "floor(CAST(t.b AS BIGINT) + 0.0)"},
	} {
		addOrd("qf/"+f[0]+"/cmp", "SELECT t.id FROM ss_t t WHERE t.b <> 0 AND t.n / "+f[1]+" = 0.0000000011111111111111111111 ORDER BY t.id")
		addOrd("qf/"+f[0]+"/proj", "SELECT t.id, t.n / "+f[1]+" AS x FROM ss_t t WHERE t.b <> 0 ORDER BY t.id")
	}
	addOrd("qf/mod/bareNum/cmp", "SELECT t.id FROM ss_t t WHERE t.n <> 0 AND mod(CAST(t.i AS NUMERIC), 10) / t.n = 1.3333333333333333 ORDER BY t.id")
	addOrd("qf/roundColCtl/cmp", "SELECT t.id FROM ss_t t WHERE t.n <> 0 AND round(t.i) / t.n = 1.3333333333333333 ORDER BY t.id")
	addOrd("qf/floorIcCtl/cmp", "SELECT t.id FROM ss_t t WHERE t.n <> 0 AND floor(CAST(t.i AS INTEGER)) / t.n = 1.3333333333333333 ORDER BY t.id")
	addOrd("qf/sqrtCtl/cmp", "SELECT t.id FROM ss_t t WHERE t.n <> 0 AND sqrt(CAST(t.i AS NUMERIC) * CAST(t.i AS NUMERIC)) / t.n = 1.3333333333333333 ORDER BY t.id")
	addOrd("qf/roundNumScaleCtl/cmp", "SELECT t.id FROM ss_t t WHERE t.n <> 0 AND round(CAST(t.i AS NUMERIC(10,0))) / t.n = 1.3333333333333333 ORDER BY t.id")
	// A BARE NUMERIC CAST AS A CHOICE'S ARM: the cast is
	// classified by the type it declares — a DECIMAL when its operand has an
	// exact type, as exact arithmetic reads it — so a choice and a comparison
	// order it as a number and never by its rendered text. Four operand kinds
	// (a quoted literal, an integer literal, an integer column and an integer
	// expression, plus a numeric column) × the choosing constructs (the bare
	// cast, CASE, COALESCE, LEAST, GREATEST, NULLIF) × every consumer that
	// compares or orders the value. The fixtures discriminate: 3 sorts above
	// 25, 100 and 10 as text and below them as a number.
	for _, sj := range [][2]string{
		{"bare", "{X}"},
		{"case", "CASE WHEN t.id = 1 THEN {X} ELSE t.b END"},
		{"coal", "COALESCE(CASE WHEN t.id <> 1 THEN t.b END, {X})"},
		{"least", "LEAST({X}, t.b + 70)"},
		{"greatest", "GREATEST({X}, t.i * 10)"},
		{"nullif", "NULLIF({X}, 3)"},
	} {
		for _, a := range [][2]string{
			{"lt", "CAST('3' AS NUMERIC)"}, {"il", "CAST(3 AS NUMERIC)"}, {"col", "CAST(t.i AS NUMERIC)"},
			{"ex", "CAST(t.i + 20 AS NUMERIC)"}, {"colN", "CAST(t.n AS NUMERIC)"},
		} {
			e := strings.ReplaceAll(sj[1], "{X}", a[1])
			base := "ch/" + sj[0] + "/" + a[0] + "/"
			addOrd(base+"proj", "SELECT t.id, "+e+" FROM ss_t t ORDER BY t.id")
			addOrd(base+"cmpGt", "SELECT t.id FROM ss_t t WHERE "+e+" > 25 ORDER BY t.id")
			addOrd(base+"cmpEq", "SELECT t.id FROM ss_t t WHERE "+e+" = 3 ORDER BY t.id")
			addOrd(base+"cmpNum", "SELECT t.id FROM ss_t t WHERE "+e+" < t.n * 10 ORDER BY t.id")
			addOrd(base+"div", "SELECT t.id, "+e+" / 2.25 FROM ss_t t ORDER BY t.id")
			addOrd(base+"plus", "SELECT t.id, "+e+" + 1 FROM ss_t t ORDER BY t.id")
			addOrd(base+"order", "SELECT t.id FROM ss_t t ORDER BY "+e+", t.id")
			addOrd(base+"orderDesc", "SELECT t.id FROM ss_t t ORDER BY "+e+" DESC, t.id")
			add(base+"group", "SELECT "+e+", COUNT(*) FROM ss_t t GROUP BY 1")
			add(base+"distinct", "SELECT DISTINCT "+e+" FROM ss_t t")
			addOrd(base+"win", "SELECT t.id, row_number() OVER (ORDER BY "+e+", t.id) FROM ss_t t ORDER BY t.id")
			addOrd(base+"minmax", "SELECT MIN("+e+"), MAX("+e+") FROM ss_t t")
			addOrd(base+"inList", "SELECT t.id FROM ss_t t WHERE "+e+" IN (25, 100, 3) ORDER BY t.id")
			addOrd(base+"between", "SELECT t.id FROM ss_t t WHERE "+e+" BETWEEN 25 AND 1000 ORDER BY t.id")
		}
	}
	// LEAST / GREATEST over a bare NUMERIC cast's arm, which a choice once
	// chose by byte order, and its controls.
	for _, c := range [][2]string{
		{"lg/least100/proj", "SELECT t.id, least(CAST(t.i AS NUMERIC), 100) FROM ss_t t ORDER BY t.id"},
		{"lg/greatest100/proj", "SELECT t.id, greatest(CAST(t.i AS NUMERIC), 100) FROM ss_t t ORDER BY t.id"},
		{"lg/greatestNeg/proj", "SELECT t.id, greatest(CAST(t.i AS NUMERIC), -100) FROM ss_t t ORDER BY t.id"},
		{"lg/least100frac/proj", "SELECT t.id, least(CAST(t.i AS NUMERIC), 100.5) FROM ss_t t ORDER BY t.id"},
		{"lg/leastCol/proj", "SELECT t.id, least(t.n, 100) FROM ss_t t ORDER BY t.id"},
		{"lg/leastTyped/proj", "SELECT t.id, least(CAST(t.i AS NUMERIC(10,0)), 100) FROM ss_t t ORDER BY t.id"},
		{"lg/leastIc/proj", "SELECT t.id, least(CAST(t.i AS INTEGER), 100) FROM ss_t t ORDER BY t.id"},
		{"lg/leastIcMul/proj", "SELECT t.id, least(CAST(t.i AS INTEGER) * 1.0, 100) FROM ss_t t ORDER BY t.id"},
		{"lg/leastNumB/proj", "SELECT t.id, least(CAST(t.b AS NUMERIC), 100) FROM ss_t t ORDER BY t.id"},
		{"lg/least100/cmp", "SELECT t.id FROM ss_t t WHERE least(CAST(t.i AS NUMERIC), 100) = 3 ORDER BY t.id"},
		{"lg/least100/plus", "SELECT t.id, least(CAST(t.i AS NUMERIC), 100) + 0 FROM ss_t t ORDER BY t.id"},
		{"lg/least100/mul", "SELECT t.id, least(CAST(t.i AS NUMERIC), 100) * t.n FROM ss_t t ORDER BY t.id"},
		{"lg/least100/div", "SELECT t.id, least(CAST(t.i AS NUMERIC), 100) / t.n FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"lg/least100/order", "SELECT t.id FROM ss_t t ORDER BY least(CAST(t.i AS NUMERIC), 100), t.id"},
		{"lg/least100/group", "SELECT least(CAST(t.i AS NUMERIC), 100) AS k, count(*) FROM ss_t t GROUP BY 1 ORDER BY 1"},
		{"lg/greatest100/div", "SELECT t.id, greatest(CAST(t.i AS NUMERIC), 100) / t.n FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"lg/coalesce/div", "SELECT t.id, coalesce(CAST(t.i AS NUMERIC), 100) / t.n FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"lg/leastLit/proj", "SELECT least(CAST(3 AS NUMERIC), 100), greatest(CAST(3 AS NUMERIC), 100), least(CAST(3 AS NUMERIC), 25)"},
		{"lg/leastLitDiv/proj", "SELECT least(CAST(3 AS NUMERIC), 100) / 2.25"},
		{"lg/leastNumText/proj", "SELECT t.id, least(CAST('3' AS NUMERIC), 100) FROM ss_t t WHERE t.id = 1"},
		{"lg/least100/divCmp", "SELECT t.id FROM ss_t t WHERE t.n <> 0 AND least(CAST(t.i AS NUMERIC), 100) / t.n = 1.3333333333333333 ORDER BY t.id"},
	} {
		addOrd(c[0], c[1])
	}
	// AN ALIASED INTEGER-LITERAL QUOTIENT OVER A NUMERIC COLUMN and the same
	// quotient without its alias: both answer the one-scale quotient on every
	// arm. The aliased item is not typed from the column `n` its text names
	// after the first dot (that typed `7 / t.n AS x` numeric(10,2) and made
	// the literal past int64's exact quotient refuse 22003).
	for _, a := range [][2]string{
		{"pastInt64", "9223372036854775808"}, {"int64", "9223372036854775807"}, {"small", "7"},
	} {
		addOrd("alias/"+a[0]+"Quot", "SELECT t.id, "+a[1]+" / t.n AS x FROM ss_t t WHERE t.n <> 0 ORDER BY t.id")
		addOrd("alias/"+a[0]+"QuotNoAlias", "SELECT t.id, "+a[1]+" / t.n FROM ss_t t WHERE t.n <> 0 ORDER BY t.id")
	}
	// AN ALIASED COMPUTED ITEM IS TYPED AS IT IS WITHOUT ITS ALIAS: each
	// expression aliased, bare and unqualified, so the
	// projection's type comes from the declaration walk of the expression and
	// never from the column its text names after the first dot (`7 / t.n`
	// is not the column `n`); and the GROUP BY key identity that text match
	// still serves (`t.i + 1 AS k` over `GROUP BY i + 1`, #738), aliased,
	// bare and spelled as the key.
	for _, c := range [][3]string{
		{"al/pastInt64Quot/alias", "true", "SELECT t.id, 9223372036854775808 / t.n AS x FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"al/pastInt64Quot/bare", "true", "SELECT t.id, 9223372036854775808 / t.n FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"al/pastInt64Quot/unq", "true", "SELECT t.id, 9223372036854775808 / n AS x FROM ss_t t WHERE n <> 0 ORDER BY t.id"},
		{"al/int64Quot/alias", "true", "SELECT t.id, 9223372036854775807 / t.n AS x FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"al/int64Quot/bare", "true", "SELECT t.id, 9223372036854775807 / t.n FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"al/int64Quot/unq", "true", "SELECT t.id, 9223372036854775807 / n AS x FROM ss_t t WHERE n <> 0 ORDER BY t.id"},
		{"al/smallQuot/alias", "true", "SELECT t.id, 7 / t.n AS x FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"al/smallQuot/bare", "true", "SELECT t.id, 7 / t.n FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"al/smallQuot/unq", "true", "SELECT t.id, 7 / n AS x FROM ss_t t WHERE n <> 0 ORDER BY t.id"},
		{"al/absMul/alias", "true", "SELECT t.id, ABS(-1) * t.n AS x FROM ss_t t ORDER BY t.id"},
		{"al/absMul/bare", "true", "SELECT t.id, ABS(-1) * t.n FROM ss_t t ORDER BY t.id"},
		{"al/absMul/unq", "true", "SELECT t.id, ABS(-1) * n AS x FROM ss_t t ORDER BY t.id"},
		{"al/absMulB/alias", "true", "SELECT t.id, ABS(-1) * t.b AS x FROM ss_t t ORDER BY t.id"},
		{"al/absMulB/bare", "true", "SELECT t.id, ABS(-1) * t.b FROM ss_t t ORDER BY t.id"},
		{"al/absMulB/unq", "true", "SELECT t.id, ABS(-1) * b AS x FROM ss_t t ORDER BY t.id"},
		{"al/absMulF/alias", "true", "SELECT t.id, ABS(-1) * t.f AS x FROM ss_t t ORDER BY t.id"},
		{"al/absMulF/bare", "true", "SELECT t.id, ABS(-1) * t.f FROM ss_t t ORDER BY t.id"},
		{"al/absMulF/unq", "true", "SELECT t.id, ABS(-1) * f AS x FROM ss_t t ORDER BY t.id"},
		{"al/litMulN/alias", "true", "SELECT t.id, 7 * t.n AS x FROM ss_t t ORDER BY t.id"},
		{"al/litMulN/bare", "true", "SELECT t.id, 7 * t.n FROM ss_t t ORDER BY t.id"},
		{"al/litMulN/unq", "true", "SELECT t.id, 7 * n AS x FROM ss_t t ORDER BY t.id"},
		{"al/litSubN/alias", "true", "SELECT t.id, 7 - t.n AS x FROM ss_t t ORDER BY t.id"},
		{"al/litSubN/bare", "true", "SELECT t.id, 7 - t.n FROM ss_t t ORDER BY t.id"},
		{"al/litSubN/unq", "true", "SELECT t.id, 7 - n AS x FROM ss_t t ORDER BY t.id"},
		{"al/litModN/alias", "true", "SELECT t.id, 7 % t.n AS x FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"al/litModN/bare", "true", "SELECT t.id, 7 % t.n FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"al/litModN/unq", "true", "SELECT t.id, 7 % n AS x FROM ss_t t WHERE n <> 0 ORDER BY t.id"},
		{"al/negN/alias", "true", "SELECT t.id, -t.n AS x FROM ss_t t ORDER BY t.id"},
		{"al/negN/bare", "true", "SELECT t.id, -t.n FROM ss_t t ORDER BY t.id"},
		{"al/negN/unq", "true", "SELECT t.id, -n AS x FROM ss_t t ORDER BY t.id"},
		{"al/negI/alias", "true", "SELECT t.id, -t.i AS x FROM ss_t t ORDER BY t.id"},
		{"al/negI/bare", "true", "SELECT t.id, -t.i FROM ss_t t ORDER BY t.id"},
		{"al/negI/unq", "true", "SELECT t.id, -i AS x FROM ss_t t ORDER BY t.id"},
		{"al/litMulI/alias", "true", "SELECT t.id, 7 * t.i AS x FROM ss_t t ORDER BY t.id"},
		{"al/litMulI/bare", "true", "SELECT t.id, 7 * t.i FROM ss_t t ORDER BY t.id"},
		{"al/litMulI/unq", "true", "SELECT t.id, 7 * i AS x FROM ss_t t ORDER BY t.id"},
		{"al/litDivI/alias", "true", "SELECT t.id, 7 / t.i AS x FROM ss_t t WHERE t.i <> 0 ORDER BY t.id"},
		{"al/litDivI/bare", "true", "SELECT t.id, 7 / t.i FROM ss_t t WHERE t.i <> 0 ORDER BY t.id"},
		{"al/litDivI/unq", "true", "SELECT t.id, 7 / i AS x FROM ss_t t WHERE i <> 0 ORDER BY t.id"},
		{"al/litDivB/alias", "true", "SELECT t.id, 7 / t.b AS x FROM ss_t t WHERE t.b <> 0 ORDER BY t.id"},
		{"al/litDivB/bare", "true", "SELECT t.id, 7 / t.b FROM ss_t t WHERE t.b <> 0 ORDER BY t.id"},
		{"al/litDivB/unq", "true", "SELECT t.id, 7 / b AS x FROM ss_t t WHERE b <> 0 ORDER BY t.id"},
		{"al/litDivF/alias", "true", "SELECT t.id, 7 / t.f AS x FROM ss_t t WHERE t.f <> 0 ORDER BY t.id"},
		{"al/litDivF/bare", "true", "SELECT t.id, 7 / t.f FROM ss_t t WHERE t.f <> 0 ORDER BY t.id"},
		{"al/litDivF/unq", "true", "SELECT t.id, 7 / f AS x FROM ss_t t WHERE f <> 0 ORDER BY t.id"},
		{"al/litPlusD/alias", "true", "SELECT t.id, 1 + t.d AS x FROM ss_t t ORDER BY t.id"},
		{"al/litPlusD/bare", "true", "SELECT t.id, 1 + t.d FROM ss_t t ORDER BY t.id"},
		{"al/litPlusD/unq", "true", "SELECT t.id, 1 + d AS x FROM ss_t t ORDER BY t.id"},
		{"al/castPlusB/alias", "true", "SELECT t.id, CAST(1 AS INT) + t.b AS x FROM ss_t t ORDER BY t.id"},
		{"al/castPlusB/bare", "true", "SELECT t.id, CAST(1 AS INT) + t.b FROM ss_t t ORDER BY t.id"},
		{"al/castPlusB/unq", "true", "SELECT t.id, CAST(1 AS INT) + b AS x FROM ss_t t ORDER BY t.id"},
		{"al/sqrtN/alias", "true", "SELECT t.id, SQRT(4) * t.n AS x FROM ss_t t ORDER BY t.id"},
		{"al/sqrtN/bare", "true", "SELECT t.id, SQRT(4) * t.n FROM ss_t t ORDER BY t.id"},
		{"al/sqrtN/unq", "true", "SELECT t.id, SQRT(4) * n AS x FROM ss_t t ORDER BY t.id"},
		{"al/roundMulN/alias", "true", "SELECT t.id, ROUND(2) * t.n AS x FROM ss_t t ORDER BY t.id"},
		{"al/roundMulN/bare", "true", "SELECT t.id, ROUND(2) * t.n FROM ss_t t ORDER BY t.id"},
		{"al/roundMulN/unq", "true", "SELECT t.id, ROUND(2) * n AS x FROM ss_t t ORDER BY t.id"},
		{"al/lenMulN/alias", "true", "SELECT t.id, LENGTH('ab') * t.n AS x FROM ss_t t ORDER BY t.id"},
		{"al/lenMulN/bare", "true", "SELECT t.id, LENGTH('ab') * t.n FROM ss_t t ORDER BY t.id"},
		{"al/lenMulN/unq", "true", "SELECT t.id, LENGTH('ab') * n AS x FROM ss_t t ORDER BY t.id"},
		{"al/concatS/alias", "true", "SELECT t.id, 'a' || t.s AS x FROM ss_t t ORDER BY t.id"},
		{"al/concatS/bare", "true", "SELECT t.id, 'a' || t.s FROM ss_t t ORDER BY t.id"},
		{"al/concatS/unq", "true", "SELECT t.id, 'a' || s AS x FROM ss_t t ORDER BY t.id"},
		{"al/icDivN/alias", "true", "SELECT t.id, CAST(t.i AS INTEGER) / t.n AS x FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"al/icDivN/bare", "true", "SELECT t.id, CAST(t.i AS INTEGER) / t.n FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"al/icDivN/unq", "true", "SELECT t.id, CAST(i AS INTEGER) / n AS x FROM ss_t t WHERE n <> 0 ORDER BY t.id"},
		{"al/nDivLit/alias", "true", "SELECT t.id, t.n / 7 AS x FROM ss_t t ORDER BY t.id"},
		{"al/nDivLit/bare", "true", "SELECT t.id, t.n / 7 FROM ss_t t ORDER BY t.id"},
		{"al/nDivLit/unq", "true", "SELECT t.id, n / 7 AS x FROM ss_t t ORDER BY t.id"},
		{"al/nMulLit/alias", "true", "SELECT t.id, t.n * 7 AS x FROM ss_t t ORDER BY t.id"},
		{"al/nMulLit/bare", "true", "SELECT t.id, t.n * 7 FROM ss_t t ORDER BY t.id"},
		{"al/nMulLit/unq", "true", "SELECT t.id, n * 7 AS x FROM ss_t t ORDER BY t.id"},
		{"al/nPlusI/alias", "true", "SELECT t.id, t.n + t.i AS x FROM ss_t t ORDER BY t.id"},
		{"al/nPlusI/bare", "true", "SELECT t.id, t.n + t.i FROM ss_t t ORDER BY t.id"},
		{"al/nPlusI/unq", "true", "SELECT t.id, n + i AS x FROM ss_t t ORDER BY t.id"},
		{"al/fDivN/alias", "true", "SELECT t.id, 7.5 / t.n AS x FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"al/fDivN/bare", "true", "SELECT t.id, 7.5 / t.n FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"al/fDivN/unq", "true", "SELECT t.id, 7.5 / n AS x FROM ss_t t WHERE n <> 0 ORDER BY t.id"},
		{"al/bigMulN/alias", "true", "SELECT t.id, 100000000000 * t.n AS x FROM ss_t t ORDER BY t.id"},
		{"al/bigMulN/bare", "true", "SELECT t.id, 100000000000 * t.n FROM ss_t t ORDER BY t.id"},
		{"al/bigMulN/unq", "true", "SELECT t.id, 100000000000 * n AS x FROM ss_t t ORDER BY t.id"},
		{"al/ssiQuot/alias", "true", "SELECT x.id, 7 / x.m AS y FROM ss_i x ORDER BY x.id"},
		{"al/ssiQuot/bare", "true", "SELECT x.id, 7 / x.m FROM ss_i x ORDER BY x.id"},
		{"al/joinQuot/alias", "true", "SELECT t.id, 7 / x.m AS y FROM ss_t t JOIN ss_i x ON x.id = t.id ORDER BY t.id"},
		{"al/joinNegM/alias", "true", "SELECT t.id, -x.m AS y, -t.n AS z FROM ss_t t JOIN ss_i x ON x.id = t.id ORDER BY t.id"},
		{"gk/qualKey/alias", "false", "SELECT t.i + 1 AS k, COUNT(*) FROM ss_t t GROUP BY i + 1"},
		{"gk/qualKey/bare", "false", "SELECT t.i + 1, COUNT(*) FROM ss_t t GROUP BY i + 1"},
		{"gk/qualKey/same", "false", "SELECT t.i + 1 AS k, COUNT(*) FROM ss_t t GROUP BY t.i + 1"},
		{"gk/tableKey/alias", "false", "SELECT ss_t.i + 1 AS k, COUNT(*) FROM ss_t t GROUP BY i + 1"},
		{"gk/tableKey/bare", "false", "SELECT ss_t.i + 1, COUNT(*) FROM ss_t t GROUP BY i + 1"},
		{"gk/tableKey/same", "false", "SELECT ss_t.i + 1 AS k, COUNT(*) FROM ss_t t GROUP BY ss_t.i + 1"},
		{"gk/bKey/alias", "false", "SELECT t.b + 1 AS k, COUNT(*) FROM ss_t t GROUP BY b + 1"},
		{"gk/bKey/bare", "false", "SELECT t.b + 1, COUNT(*) FROM ss_t t GROUP BY b + 1"},
		{"gk/bKey/same", "false", "SELECT t.b + 1 AS k, COUNT(*) FROM ss_t t GROUP BY t.b + 1"},
		{"gk/nKey/alias", "false", "SELECT t.n + 1 AS k, COUNT(*) FROM ss_t t GROUP BY n + 1"},
		{"gk/nKey/bare", "false", "SELECT t.n + 1, COUNT(*) FROM ss_t t GROUP BY n + 1"},
		{"gk/nKey/same", "false", "SELECT t.n + 1 AS k, COUNT(*) FROM ss_t t GROUP BY t.n + 1"},
		{"gk/nKeyMul/alias", "false", "SELECT 2 * t.n AS k, COUNT(*) FROM ss_t t GROUP BY 2 * n"},
		{"gk/nKeyMul/bare", "false", "SELECT 2 * t.n, COUNT(*) FROM ss_t t GROUP BY 2 * n"},
		{"gk/nKeyMul/same", "false", "SELECT 2 * t.n AS k, COUNT(*) FROM ss_t t GROUP BY 2 * t.n"},
		{"gk/quotKey/alias", "false", "SELECT 7 / t.n AS k, COUNT(*) FROM ss_t t WHERE t.n <> 0 GROUP BY 7 / n"},
		{"gk/quotKey/bare", "false", "SELECT 7 / t.n, COUNT(*) FROM ss_t t WHERE t.n <> 0 GROUP BY 7 / n"},
		{"gk/quotKey/same", "false", "SELECT 7 / t.n AS k, COUNT(*) FROM ss_t t WHERE t.n <> 0 GROUP BY 7 / t.n"},
		{"gk/absKey/alias", "false", "SELECT ABS(-1) * t.n AS k, COUNT(*) FROM ss_t t GROUP BY ABS(-1) * n"},
		{"gk/absKey/bare", "false", "SELECT ABS(-1) * t.n, COUNT(*) FROM ss_t t GROUP BY ABS(-1) * n"},
		{"gk/absKey/same", "false", "SELECT ABS(-1) * t.n AS k, COUNT(*) FROM ss_t t GROUP BY ABS(-1) * t.n"},
		{"gk/pastKey/alias", "false", "SELECT 9223372036854775808 / t.n AS k, COUNT(*) FROM ss_t t WHERE t.n <> 0 GROUP BY 9223372036854775808 / n"},
		{"gk/pastKey/bare", "false", "SELECT 9223372036854775808 / t.n, COUNT(*) FROM ss_t t WHERE t.n <> 0 GROUP BY 9223372036854775808 / n"},
		{"gk/pastKey/same", "false", "SELECT 9223372036854775808 / t.n AS k, COUNT(*) FROM ss_t t WHERE t.n <> 0 GROUP BY 9223372036854775808 / t.n"},
	} {
		if c[1] == "true" {
			addOrd(c[0], c[2])
		} else {
			add(c[0], c[2])
		}
	}
	// ABS and MOD over an INTEGER constant answer in the integer domain:
	// `ABS(-1)` is integer and `ABS(-1) * t.n` numeric on PostgreSQL, bare
	// and aliased.
	for _, c := range [][3]string{
		{"ac/abs", "true", "SELECT t.id, ABS(-1) FROM ss_t t ORDER BY t.id"},
		{"ac/absA", "true", "SELECT t.id, ABS(-1) AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absDiv", "true", "SELECT t.id, ABS(-1) / 2 FROM ss_t t ORDER BY t.id"},
		{"ac/absDivA", "true", "SELECT t.id, ABS(-1) / 2 AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absMulN", "true", "SELECT t.id, ABS(-1) * t.n FROM ss_t t ORDER BY t.id"},
		{"ac/absMulNA", "true", "SELECT t.id, ABS(-1) * t.n AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absMulI", "true", "SELECT t.id, ABS(-1) * t.i FROM ss_t t ORDER BY t.id"},
		{"ac/absMulIA", "true", "SELECT t.id, ABS(-1) * t.i AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absPlusFrac", "true", "SELECT t.id, ABS(-1) + 0.5 FROM ss_t t ORDER BY t.id"},
		{"ac/absPlusFracA", "true", "SELECT t.id, ABS(-1) + 0.5 AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absMulF", "true", "SELECT t.id, ABS(-1) * t.f FROM ss_t t ORDER BY t.id"},
		{"ac/absMulFA", "true", "SELECT t.id, ABS(-1) * t.f AS x FROM ss_t t ORDER BY t.id"},
		{"ac/mod", "true", "SELECT t.id, MOD(7, 3) FROM ss_t t ORDER BY t.id"},
		{"ac/modA", "true", "SELECT t.id, MOD(7, 3) AS x FROM ss_t t ORDER BY t.id"},
		{"ac/modDiv", "true", "SELECT t.id, MOD(7, 3) / 2 FROM ss_t t ORDER BY t.id"},
		{"ac/modDivA", "true", "SELECT t.id, MOD(7, 3) / 2 AS x FROM ss_t t ORDER BY t.id"},
		{"ac/modMulN", "true", "SELECT t.id, MOD(-7, 3) * t.n FROM ss_t t ORDER BY t.id"},
		{"ac/modMulNA", "true", "SELECT t.id, MOD(-7, 3) * t.n AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absText", "true", "SELECT t.id, CAST(ABS(-1) AS TEXT) FROM ss_t t ORDER BY t.id"},
		{"ac/absTextA", "true", "SELECT t.id, CAST(ABS(-1) AS TEXT) AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absMin", "true", "SELECT t.id, ABS(-2147483648) FROM ss_t t ORDER BY t.id"},
		{"ac/absMinA", "true", "SELECT t.id, ABS(-2147483648) AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absBig", "true", "SELECT t.id, ABS(-9223372036854775807) FROM ss_t t ORDER BY t.id"},
		{"ac/absBigA", "true", "SELECT t.id, ABS(-9223372036854775807) AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absNeg", "true", "SELECT t.id, -ABS(-1) FROM ss_t t ORDER BY t.id"},
		{"ac/absNegA", "true", "SELECT t.id, -ABS(-1) AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absPos", "true", "SELECT t.id, ABS(1) * t.n FROM ss_t t ORDER BY t.id"},
		{"ac/absPosA", "true", "SELECT t.id, ABS(1) * t.n AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absDivN", "true", "SELECT t.id, ABS(-7) / t.n FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"ac/absDivNA", "true", "SELECT t.id, ABS(-7) / t.n AS x FROM ss_t t WHERE t.n <> 0 ORDER BY t.id"},
		{"ac/absCmp", "true", "SELECT t.id, ABS(-1) * t.n = 2.25 FROM ss_t t ORDER BY t.id"},
		{"ac/absCmpA", "true", "SELECT t.id, ABS(-1) * t.n = 2.25 AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absCoal", "true", "SELECT t.id, COALESCE(ABS(-1), t.n) FROM ss_t t ORDER BY t.id"},
		{"ac/absCoalA", "true", "SELECT t.id, COALESCE(ABS(-1), t.n) AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absGreat", "true", "SELECT t.id, GREATEST(ABS(-3), t.i) FROM ss_t t ORDER BY t.id"},
		{"ac/absGreatA", "true", "SELECT t.id, GREATEST(ABS(-3), t.i) AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absFrac", "true", "SELECT t.id, ABS(-2.5) * t.n FROM ss_t t ORDER BY t.id"},
		{"ac/absFracA", "true", "SELECT t.id, ABS(-2.5) * t.n AS x FROM ss_t t ORDER BY t.id"},
		{"ac/absI", "true", "SELECT t.id, ABS(t.i) * t.n FROM ss_t t ORDER BY t.id"},
		{"ac/absIA", "true", "SELECT t.id, ABS(t.i) * t.n AS x FROM ss_t t ORDER BY t.id"},
	} {
		if c[1] == "true" {
			addOrd(c[0], c[2])
		} else {
			add(c[0], c[2])
		}
	}
	// A NUMERIC OPERAND OF A CAST WITH NO CONVERSION FROM NUMERIC: a
	// wide literal is an exact DECIMAL now, boxed as its text, and the
	// BOOLEAN / DATE / TIMESTAMP / INTERVAL / UUID / array arms read a string
	// box by their input grammar. PostgreSQL refuses each type pair, 42846;
	// the narrow literal (a double here) and the integer literal past int64
	// are the controls, the numeric column and a typed NUMERIC cast the same
	// box reached another way.
	for _, src := range [][2]string{
		{"wide", nxK},
		{"wideNeg", "-" + nxK},
		{"wideExp", "1.40000000000000000001e1"},
		{"wideChoice", "COALESCE(" + nxK + ", 0)"},
		{"pastInt64", "9223372036854775808"},
		{"pastInt64Neg", "(-9223372036854775809)"},
		{"narrow", "14.5"},
		{"numCast", "CAST(1.5 AS NUMERIC(10,2))"},
		{"numCol", "(SELECT t.n FROM ss_t t WHERE t.id = 1)"},
	} {
		for _, to := range [][2]string{{"BOOLEAN", "BOOLEAN"}, {"DATE", "DATE"}, {"TIMESTAMP", "TIMESTAMP"},
			{"INTERVAL", "INTERVAL"}, {"UUID", "UUID"}, {"INTEGERArray", "INTEGER[]"}, {"VECTOR", "VECTOR(1)"}} {
			add("castRefusal/"+src[0]+"/"+to[0], "SELECT CAST("+src[1]+" AS "+to[1]+") AS x")
		}
	}
	add("castRefusal/numColWhere/DATE", "SELECT t.id, CAST(t.n AS DATE) AS x FROM ss_t t WHERE t.id = 1")
	add("castRefusal/numColWhere/BOOLEAN", "SELECT t.id, CAST(t.n AS BOOLEAN) AS x FROM ss_t t WHERE t.id = 1")
	add("castRefusal/wideColon/DATE", "SELECT "+nxK+"::DATE AS x")
	add("castRefusal/pastInt64Colon/DATE", "SELECT 9223372036854775808::DATE AS x")
	// THE WIDE CONSTANT'S ORIGIN (#1386): the literal handed on by a derived
	// table, a CTE, a set operation, VALUES, a DISTINCT and a join.
	for _, og := range [][2]string{
		{"derived", "SELECT s.x = 14 AS y, s.x FROM (SELECT " + nxK + " AS x) s"},
		{"cte", "WITH w AS (SELECT " + nxK + " AS x) SELECT w.x = 14 AS y, w.x FROM w"},
		{"union", "SELECT s.x FROM (SELECT " + nxK + " AS x UNION ALL SELECT 13.5) s"},
		{"values", "SELECT v.x FROM (VALUES (" + nxK + "), (13.5)) AS v(x)"},
		{"distinct", "SELECT DISTINCT " + nxK + " AS x FROM ss_t"},
		{"join", "SELECT t.id, s.x FROM ss_t t JOIN (SELECT u.id, " + nxK + " AS x FROM ss_t u) s ON s.id = t.id WHERE t.id < 3"},
	} {
		add("wideOrigin/"+og[0], og[1])
	}
	return out
}

// nxRunCell runs one cell on one arm: a cell's statements are joined by
// " ;; "; the answer is the first error of a non-DROP statement, else the
// last statement's result.
func nxRunCell(arm ssArm, c nxCell) string {
	var res, firstErr string
	for _, st := range strings.Split(c.sql, " ;; ") {
		r, err := arm.run(st, c.ordered)
		if err != nil {
			r = "ERR " + sqlerr.StateOf(err) + " " + strings.ReplaceAll(err.Error(), "\n", " ")
			if firstErr == "" && !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(st)), "DROP") {
				firstErr = r
			}
		}
		res = r
	}
	if firstErr != "" {
		return firstErr
	}
	return res
}

// TestArcNXGenerate dumps the cells as name<TAB>ordered<TAB>sql for the
// oracle run (NX_GEN=<path>), and the PostgreSQL fixture beside it.
func TestArcNXGenerate(t *testing.T) {
	path := os.Getenv("NX_GEN")
	if path == "" {
		t.Skip("NX_GEN unset")
	}
	var b strings.Builder
	for _, c := range nxCells() {
		fmt.Fprintf(&b, "%s\t%t\t%s\n", c.name, c.ordered, c.sql)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".fixture.sql", []byte(ssPGFixture()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func nxReadTSV(t *testing.T, path string, cols int) map[string][][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string][][]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "\t", cols)
		if len(parts) != cols {
			t.Fatalf("%s: malformed line %q", path, line)
		}
		out[parts[0]] = append(out[parts[0]], parts[1:])
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestArcNXNumericCarrierEveryArm is the seam table on five arms. A cell in
// testdata/arc_nx_numeric_carrier_kept.tsv answers a CATALOGUED divergence
// or a recorded filing candidate (name, arms all|dag|single, this engine's
// answer, why; a cell whose single-process and DAG arms answer differently
// has a line for each); it is asserted as it stands, so a change FAILS and
// is re-measured.
func TestArcNXNumericCarrierEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the numeric-carrier table")
	}
	answers := nxReadTSV(t, "testdata/arc_nx_numeric_carrier_pg17.tsv", 2)
	kept := nxReadTSV(t, "testdata/arc_nx_numeric_carrier_kept.tsv", 4)
	cells := nxCells()
	names := map[string]bool{}
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
		names[c.name] = true
	}
	for name, ks := range kept {
		if !names[name] {
			t.Fatalf("kept cell %s is not in the table", name)
		}
		for _, k := range ks {
			if k[0] != "all" && k[0] != "dag" && k[0] != "single" {
				t.Fatalf("kept cell %s: arms %q", name, k[0])
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := ssArms(t, ctx)
	var dump *os.File
	if p := os.Getenv("NX_DUMP"); p != "" {
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		dump = f
	}
	var dumpMu sync.Mutex
	for _, tc := range cells {
		pgWant := answers[tc.name][0][0]
		ks := kept[tc.name]
		t.Run(tc.name, func(t *testing.T) {
			got := make([]string, len(arms))
			var wg sync.WaitGroup
			for i, arm := range arms {
				if tc.embedded && strings.HasPrefix(arm.name, "dag") {
					continue
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					got[i] = nxRunCell(arm, tc)
				}()
			}
			wg.Wait()
			if dump != nil {
				dumpMu.Lock()
				for i, arm := range arms {
					if got[i] != "" {
						fmt.Fprintf(dump, "%s\t%s\t%s\n", tc.name, arm.name, got[i])
					}
				}
				dumpMu.Unlock()
			}
			for i, arm := range arms {
				if tc.embedded && strings.HasPrefix(arm.name, "dag") {
					continue
				}
				want, why := pgWant, "PostgreSQL 17.11"
				dag := strings.HasPrefix(arm.name, "dag")
				for _, k := range ks {
					if k[0] == "all" || k[0] == "dag" && dag || k[0] == "single" && !dag {
						want, why = k[1], "kept: "+k[2]
					}
				}
				if !nxMatches(got[i], want) && !(strings.HasSuffix(tc.name, "/agg") && nxFloatSumClose(got[i], want)) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (%s)", tc.sql, arm.name, got[i], want, why)
				}
			}
		})
	}
}

// nxFloatSumClose accepts an aggregate cell whose wanted answer is a
// double (a kept line: the quotient's float8 rung) when the arm's answer
// differs from it only in the last bits of a double: the SUM of doubles
// depends on the order the partial states meet (ADR-0013, float aggregation
// order), so `sum(CAST(t.n AS NUMERIC) / 3)` is -0.4133333333333334 or
// -0.41333333333333344 run to run on the single-process arms. Classes, row
// counts and every non-double field must match exactly; a double within four
// units in the last place of the wanted one matches.
func nxFloatSumClose(got, want string) bool {
	if !strings.HasPrefix(want, "{") || !strings.Contains(want, "float") {
		return false
	}
	gh, gr, gok := strings.Cut(strings.TrimSpace(got), " rows=")
	wh, wr, wok := strings.Cut(strings.TrimSpace(want), " rows=")
	if !gok || !wok || gh != wh {
		return false
	}
	gn, gv, _ := strings.Cut(gr, " ")
	wn, wv, _ := strings.Cut(wr, " ")
	if gn != wn {
		return false
	}
	gf := strings.FieldsFunc(gv, func(r rune) bool { return r == ',' || r == '|' || r == ' ' })
	wf := strings.FieldsFunc(wv, func(r rune) bool { return r == ',' || r == '|' || r == ' ' })
	if len(gf) != len(wf) {
		return false
	}
	for k := range gf {
		if gf[k] == wf[k] {
			continue
		}
		a, aerr := strconv.ParseFloat(gf[k], 64)
		b, berr := strconv.ParseFloat(wf[k], 64)
		if aerr != nil || berr != nil || math.Signbit(a) != math.Signbit(b) {
			return false
		}
		ua, ub := math.Float64bits(math.Abs(a)), math.Float64bits(math.Abs(b))
		if ua > ub+4 || ub > ua+4 {
			return false
		}
	}
	return true
}

// TestNXFloatSumCloseIsNarrow holds the tolerance to the last bits of a
// double: the two answers the single-process arms give for nx/wnNumBareNum/agg
// match, an exact numeric, another row count or a digit further up do not.
func TestNXFloatSumCloseIsNarrow(t *testing.T) {
	want := "{float,float} rows=1 -0.4133333333333334,0.75"
	for _, tc := range []struct {
		got  string
		want bool
	}{
		{"{float,float} rows=1 -0.41333333333333344,0.75", true},
		{"{float,float} rows=1 -0.4133333333333334,0.75", true},
		{"{numeric,numeric} rows=1 -0.41333333333333333334,0.75000000000000000000", false},
		{"{float,float} rows=1 -0.4133333333333,0.75", false},
		{"{float,float} rows=1 -0.4133333333333334,0.76", false},
		{"{float,float} rows=2 -0.4133333333333334,0.75 | 1,1", false},
	} {
		if got := nxFloatSumClose(tc.got, want); got != tc.want {
			t.Errorf("nxFloatSumClose(%q) = %v, want %v", tc.got, got, tc.want)
		}
	}
	if nxFloatSumClose("{int} rows=1 3", "{int} rows=1 3") {
		t.Error("a non-double answer must go through nxMatches, not the tolerance")
	}
}

// nxMatches compares an arm's answer with the wanted one: a refusal by its
// SQLSTATE alone (the message text is not this seam's dimension), a result by
// its rendering.
func nxMatches(got, want string) bool {
	if rest, ok := strings.CutPrefix(want, "ERR "); ok {
		state, _, _ := strings.Cut(rest, " ")
		g, ok := strings.CutPrefix(got, "ERR ")
		if !ok {
			return false
		}
		gs, _, _ := strings.Cut(g, " ")
		return gs == state
	}
	return strings.TrimSpace(got) == strings.TrimSpace(want)
}
