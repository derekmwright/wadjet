// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// THE TABLESAMPLE ARGUMENT IS PostgreSQL's REAL, RANGE-CHECKED WHERE THE
// SCAN BEGINS, AND THE SAMPLE IS DRAWN WHERE THE SCAN RUNS (#1411), on six
// arms.
//
// PostgreSQL 17.11 reads the argument of BERNOULLI / SYSTEM as ANY constant
// expression coerced to real (float4: tsm_bernoulli / tsm_system declare a
// FLOAT4 parameter). The coercion happens when the statement is planned — a
// value float4 cannot hold (1e39, 1e400, a nonzero 1e-46) is 22003, text is
// 42804, text real's input refuses is 22P02 — and the RANGE is checked when
// the scan begins: NULL, NaN, below 0 or above 100 is 2202H. 0 samples no row
// and 100 every row. A scan PostgreSQL never begins (under WHERE false, a
// constant-false HAVING, LIMIT 0) answers, and EXPLAIN shows the plan.
//
// At 6184761c the argument was a number token read with strconv.ParseFloat
// and no check: BERNOULLI (0) and SYSTEM (0) answered every row, every
// percentage over 100 answered every row, and every other expression
// PostgreSQL takes was 42601. On the DAG arms no stage fragment carried the
// sampler at all: every file was read whole, so BERNOULLI (50) over 20 000
// rows answered 20 000 — in a FROM item, a CTE body, and a WHERE / HAVING /
// ON / SELECT-list scalar subquery alike. Round 1 (a38dba67) routed every
// sampled statement to the coordinator-local pipeline instead, which kept
// the DAG scan unsampled for an expression subquery, refused a 100 %
// sampled 20 000-row join under a 64 KiB fast path, and — with the local
// sampler discarding the scan's selection vector — returned DELETEd rows.
// Round 2 reverts that route: the worker's scan fragment applies the same
// sampler kernel the single-process scan does (exec.NewSampledSource), over
// the rows the scan selects.
//
// The fixture is tb_p (3 rows), tb_e (no rows), tb_big (20 000 rows), tb_d
// (8 rows, the even ids DELETEd) and tb_bd (20 000 rows, the even ids
// DELETEd) — on the DAG arms four files each, so every DELETE is a marker
// on a file that is still read. Every want is PostgreSQL 17.11 over the same
// rows (tb_author/pg.txt, tb_author/r2/pg_newcells.txt). A cell PostgreSQL
// answers with a random count asserts the count's range (`RANGE lo hi`); 0 %
// and 100 % are exact. A cell pinned on every arm is a kept divergence named
// in docs/adr/0012-divergences/other.md: the engine must answer the pin, and
// a pin that starts agreeing FAILS. dagPin is the three DAG arms' answer
// where it is a separate open defect's (#1190: a relation with no files has
// no distributed scan stage), not this arc's.
func TestArcTBTablesampleArgumentOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: three DAG arms stand up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := tbArms(t, ctx)
	for _, c := range tbCells() {
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				var routesBefore int64
				if arm.coord != nil {
					routesBefore = arm.coord.TableLessLocalRoutes()
				}
				got := arm.run(c.sql)
				want, why := c.want, ""
				switch {
				case arm.dag && c.dagPin != "":
					want, why = c.dagPin, " (the DAG arms' pinned answer: re-measure it)"
				case arm.coord == nil && c.localPin != "":
					want, why = c.localPin, " (the embedded arms' pinned answer, other r21: re-measure it)"
				case c.pinned:
					why = " (a pinned divergence: re-measure it)"
				}
				if !tbMatches(got, want) {
					t.Errorf("%s: %s\n  got  %s\n  want %s%s", arm.name, c.sql, got, want, why)
				}
				// A sampled scan runs ON THE DAG (round 2): the statement is
				// never routed to the coordinator-local pipeline for it.
				if c.onDAG && arm.dag && arm.coord.TableLessLocalRoutes() != routesBefore {
					t.Errorf("%s: %s was routed to the coordinator-local pipeline; the DAG's scan fragment samples it",
						arm.name, c.sql)
				}
			}
		})
	}
}

type tbCell struct {
	name, sql, want string
	// pinned marks a kept divergence: want is the ENGINE's answer, and
	// PostgreSQL answers otherwise (the catalog row names both).
	pinned bool
	// dagPin is the answer on the three DAG arms (LocalFastPathBytes 0)
	// where another open defect decides it (#1190), and want is
	// PostgreSQL's everywhere else.
	dagPin string
	// onDAG: on the three DAG arms the statement runs on the stage DAG —
	// no table-less route to the coordinator-local pipeline is taken.
	onDAG bool
	// localPin is the answer on the two embedded arms where it is a kept
	// divergence (other r21), and want is PostgreSQL's everywhere else.
	localPin string
}

// tbMatches compares an answer with a want: the exact rendering, or `RANGE
// lo hi` — one integer row within [lo, hi] — or PLAN, any answer that is not
// an error, or `CONTAINS s`, an answer whose rendering contains s.
func tbMatches(got, want string) bool {
	switch {
	case strings.HasPrefix(want, "CONTAINS "):
		return strings.Contains(got, strings.TrimPrefix(want, "CONTAINS "))
	case want == "PLAN":
		return !strings.HasPrefix(got, "ERR ") && !strings.HasPrefix(got, "PANIC ")
	case strings.HasPrefix(want, "RANGE "):
		f := strings.Fields(want)
		lo, _ := strconv.Atoi(f[1])
		hi, _ := strconv.Atoi(f[2])
		n, err := strconv.Atoi(got)
		return err == nil && n >= lo && n <= hi
	}
	return got == want
}

func tbCells() []tbCell {
	var cells []tbCell
	add := func(name, sql, want string) { cells = append(cells, tbCell{name: name, sql: sql, want: want}) }
	pin := func(name, sql, want string) {
		cells = append(cells, tbCell{name: name, sql: sql, want: want, pinned: true})
	}
	// dag: a cell whose statement must run on the stage DAG on the DAG arms.
	dag := func(name, sql, want string) {
		cells = append(cells, tbCell{name: name, sql: sql, want: want, onDAG: true})
	}
	// empty1190: a cell over the empty table, which the three DAG arms
	// answer with #1190's refusal whatever the sample (base-identical).
	empty1190 := func(name, sql, want string) {
		cells = append(cells, tbCell{name: name, sql: sql, want: want,
			dagPin: "ERR (uncoded) native DAG: stage scan-0 (scan): stage scan-0 worker 0: " +
				"stage scan-0 has no dependencies and no ScanFiles"})
	}
	count := func(method, arg string) string {
		return "SELECT count(*) FROM tb_p TABLESAMPLE " + method + " (" + arg + ")"
	}

	// The argument, BERNOULLI over tb_p (3 rows), COUNT(*).
	for _, a := range []struct{ name, arg, want string }{
		// exact: 0 % and 100 %, through every spelling of the number
		{"zero", "0", "0"},
		{"zero_point", "0.0", "0"},
		{"minus_zero", "-0.0", "0"},
		{"minus_zero_text", "'-0'", "0"},
		{"hundred", "100", "3"},
		{"hundred_point", "100.0", "3"},
		{"hundred_float8", "CAST('100' AS DOUBLE PRECISION)", "3"},
		// float4 rounds these to 100 (measured: PostgreSQL answers every row)
		{"hundred_e6_numeric", "100.000001", "3"},
		{"hundred_e7_numeric", "100.0000001", "3"},
		{"hundred_e6_text", "'100.000001'", "3"},
		{"hundred_e6_float8", "CAST('100.000001' AS DOUBLE PRECISION)", "3"},
		{"hundred_numeric_scale2", "CAST(100.00001 AS NUMERIC(10,2))", "3"},
		{"hundred_parenthesized", "((100))", "3"},
		// a sample: any count is PostgreSQL's
		{"fifty", "50", "RANGE 0 3"},
		{"fifty_real", "CAST('50' AS REAL)", "RANGE 0 3"},
		{"fifty_numeric", "CAST(50 AS NUMERIC)", "RANGE 0 3"},
		{"fifty_smallint", "CAST(50 AS SMALLINT)", "RANGE 0 3"},
		{"fifty_bigint", "CAST(50 AS BIGINT)", "RANGE 0 3"},
		{"fifty_text_literal", "'50'", "RANGE 0 3"},
		{"fifty_text_padded", "' 50 '", "RANGE 0 3"},
		{"fifty_product", "25 * 2", "RANGE 0 3"},
		{"fifty_double_negation", "-(-50)", "RANGE 0 3"},
		{"fifty_unary_plus", "+50", "RANGE 0 3"},
		{"fifty_abs", "abs(-50)", "RANGE 0 3"},
		{"fifty_case", "CASE WHEN true THEN 50 END", "RANGE 0 3"},
		{"denormal_1e45", "1e-45", "RANGE 0 3"},
		{"denormal_1e40_float8", "CAST('1e-40' AS DOUBLE PRECISION)", "RANGE 0 3"},
		// out of range: 2202H
		{"over_101", "101", "ERR 2202H"},
		{"over_100_00001", "100.00001", "ERR 2202H"},
		{"minus_one", "-1", "ERR 2202H"},
		{"minus_fifty_spaced", "- 50", "ERR 2202H"},
		{"minus_five_int", "CAST(-5 AS INTEGER)", "ERR 2202H"},
		{"e20", "1e20", "ERR 2202H"},
		{"twenty_nines", "99999999999999999999", "ERR 2202H"},
		{"int8_max", "9223372036854775807", "ERR 2202H"},
		{"real_max_ish", "3.4e38", "ERR 2202H"},
		{"e20_float8", "CAST('1e20' AS DOUBLE PRECISION)", "ERR 2202H"},
		{"nan_float8", "CAST('NaN' AS DOUBLE PRECISION)", "ERR 2202H"},
		{"inf_float8", "CAST('Infinity' AS DOUBLE PRECISION)", "ERR 2202H"},
		{"minus_inf_float8", "CAST('-Infinity' AS DOUBLE PRECISION)", "ERR 2202H"},
		{"inf_text", "'Infinity'", "ERR 2202H"},
		{"inf_text_short", "'inf'", "ERR 2202H"},
		{"nan_text", "'NaN'", "ERR 2202H"},
		{"null", "NULL", "ERR 2202H"},
		{"null_int", "CAST(NULL AS INTEGER)", "ERR 2202H"},
		{"null_numeric", "CAST(NULL AS NUMERIC)", "ERR 2202H"},
		{"null_float8", "CAST(NULL AS DOUBLE PRECISION)", "ERR 2202H"},
		// float4 cannot hold the value: 22003 at the coercion
		{"e400", "1e400", "ERR 22003"},
		{"e39", "1e39", "ERR 22003"},
		{"e_minus46", "1e-46", "ERR 22003"},
		{"e400_float8", "CAST('1e400' AS DOUBLE PRECISION)", "ERR 22003"},
		{"e39_float8", "CAST('1e39' AS DOUBLE PRECISION)", "ERR 22003"},
		{"e_minus46_float8", "CAST('1e-46' AS DOUBLE PRECISION)", "ERR 22003"},
		{"e39_real", "CAST('1e39' AS REAL)", "ERR 22003"},
		{"e400_text", "'1e400'", "ERR 22003"},
		{"e39_text", "'1e39'", "ERR 22003"},
		// real's input refuses what float4in refuses (review r1 B3): a `_`
		// digit separator and a nonzero value real rounds to zero (base 9420d256
		// refused these 42601; round 1 answered a 10 % and a 0 % sample)
		{"text_underflow", "'1e-46'", "ERR 22003"},
		{"text_underflow_negative", "'-1e-46'", "ERR 22003"},
		{"text_subnormal", "'1e-45'", "RANGE 0 3"},
		{"text_underscore", "'1_0'", "ERR 22P02"},
		{"text_underscore_exponent", "'1e1_0'", "ERR 22P02"},
		{"float8_underscore", "CAST('1_0' AS DOUBLE PRECISION)", "ERR 22P02"},
		{"float8_underflow", "CAST('1e-400' AS DOUBLE PRECISION)", "ERR 22003"},
		// the argument's own failure
		{"text_abc", "'abc'", "ERR 22P02"},
		{"text_empty", "''", "ERR 22P02"},
		{"div_zero", "1/0", "ERR 22012"},
		{"sqrt_negative", "sqrt(-1)", "ERR 2201F"},
		// not a real: 42804
		{"text_cast", "CAST('50' AS TEXT)", "ERR 42804"},
		{"text_concat", "'5' || '0'", "ERR 42804"},
		{"varchar_cast", "CAST('1' AS VARCHAR)", "ERR 42804"},
		{"bool", "true", "ERR 42804"},
		{"null_text", "CAST(NULL AS TEXT)", "ERR 42804"},
		{"null_bool", "CAST(NULL AS BOOLEAN)", "ERR 42804"},
		{"date", "DATE '2024-01-01'", "ERR 42804"},
		// a column is not in scope there: 42703; an aggregate: 42803
		{"column", "id", "ERR 42703"},
		{"aggregate", "count(*)", "ERR 42803"},
	} {
		add("bernoulli/"+a.name, count("BERNOULLI", a.arg), a.want)
	}
	// A subquery is folded by PostgreSQL (an InitPlan) and answers; the
	// argument is evaluated here before any row exists, so it is refused.
	pin("bernoulli/subquery", count("BERNOULLI", "(SELECT 50)"), "ERR 0A000")

	// SYSTEM: the same argument rule.
	for _, a := range []struct{ name, arg, want string }{
		{"zero", "0", "0"},
		{"hundred", "100", "3"},
		{"over_101", "101", "ERR 2202H"},
		{"minus_one", "-1", "ERR 2202H"},
		{"null", "NULL", "ERR 2202H"},
		{"nan_float8", "CAST('NaN' AS DOUBLE PRECISION)", "ERR 2202H"},
		{"e400", "1e400", "ERR 22003"},
		{"text_cast", "CAST('50' AS TEXT)", "ERR 42804"},
	} {
		add("system/"+a.name, count("SYSTEM", a.arg), a.want)
	}

	// The table: no rows (the range is still checked — the scan begins) and
	// 20 000 rows (0 % none, 100 % all, 50 % a count within bounds).
	// On the DAG arms the range is checked when the coordinator dispatches the
	// scan stage — over a table with no files too.
	add("empty/bernoulli_101", "SELECT count(*) FROM tb_e TABLESAMPLE BERNOULLI (101)", "ERR 2202H")
	add("empty/bernoulli_minus_one", "SELECT count(*) FROM tb_e TABLESAMPLE BERNOULLI (-1)", "ERR 2202H")
	add("empty/bernoulli_null", "SELECT count(*) FROM tb_e TABLESAMPLE BERNOULLI (NULL)", "ERR 2202H")
	empty1190("empty/bernoulli_zero", "SELECT count(*) FROM tb_e TABLESAMPLE BERNOULLI (0)", "0")
	add("empty/bernoulli_e400", "SELECT count(*) FROM tb_e TABLESAMPLE BERNOULLI (CAST('1e400' AS DOUBLE PRECISION))", "ERR 22003")
	add("empty/system_101", "SELECT count(*) FROM tb_e TABLESAMPLE SYSTEM (101)", "ERR 2202H")
	empty1190("empty/system_zero", "SELECT count(*) FROM tb_e TABLESAMPLE SYSTEM (0)", "0")
	empty1190("empty/unsampled_control", "SELECT count(*) FROM tb_e", "0")
	dag("big/bernoulli_zero", "SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (0)", "0")
	dag("big/bernoulli_hundred", "SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (100)", "20000")
	dag("big/bernoulli_fifty", "SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (50)", "RANGE 9000 11000")
	dag("big/bernoulli_fifty_rows", "SELECT count(*) FROM (SELECT id, v FROM tb_big TABLESAMPLE BERNOULLI (50)) s", "RANGE 9000 11000")
	dag("big/bernoulli_zero_grouped", "SELECT count(*) FROM (SELECT id % 4 AS k, count(*) AS c FROM tb_big TABLESAMPLE BERNOULLI (0) GROUP BY id % 4) s", "0")
	dag("big/bernoulli_101", "SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (101)", "ERR 2202H")
	dag("big/system_zero", "SELECT count(*) FROM tb_big TABLESAMPLE SYSTEM (0)", "0")
	dag("big/system_hundred", "SELECT count(*) FROM tb_big TABLESAMPLE SYSTEM (100)", "20000")
	// SYSTEM keeps or drops 2048-row blocks (r20), on every arm: a count
	// is a sum of whole blocks.
	dag("big/system_fifty", "SELECT count(*) FROM tb_big TABLESAMPLE SYSTEM (50)", "RANGE 0 20000")

	// A DELETE narrows the scan's selection, and the sample is drawn from
	// what is left (review r1 B1: round 1 returned the deleted rows under
	// BERNOULLI (100) on every arm — on the DAG arms, where base answered
	// PostgreSQL's rows — and SYSTEM (0) answered every selected row).
	dag("delete/bernoulli_hundred_rows", "SELECT id FROM tb_d TABLESAMPLE BERNOULLI (100) ORDER BY id", "1; 3; 5; 7")
	dag("delete/bernoulli_hundred", "SELECT count(*) FROM tb_d TABLESAMPLE BERNOULLI (100)", "4")
	dag("delete/system_hundred_rows", "SELECT id FROM tb_d TABLESAMPLE SYSTEM (100) ORDER BY id", "1; 3; 5; 7")
	dag("delete/system_zero", "SELECT count(*) FROM tb_d TABLESAMPLE SYSTEM (0)", "0")
	dag("delete/bernoulli_zero", "SELECT count(*) FROM tb_d TABLESAMPLE BERNOULLI (0)", "0")
	dag("delete/big_bernoulli_hundred", "SELECT count(*) FROM tb_bd TABLESAMPLE BERNOULLI (100)", "10000")
	// half of the 10 000 surviving rows; half of all 20 000 would be the
	// deleted rows coming back
	dag("delete/big_bernoulli_fifty", "SELECT count(*) FROM tb_bd TABLESAMPLE BERNOULLI (50)", "RANGE 4500 5500")
	dag("delete/big_system_hundred", "SELECT count(*) FROM tb_bd TABLESAMPLE SYSTEM (100)", "10000")
	dag("delete/big_system_zero", "SELECT count(*) FROM tb_bd TABLESAMPLE SYSTEM (0)", "0")
	dag("delete/big_deleted_ids", "SELECT count(*) FROM tb_bd TABLESAMPLE BERNOULLI (100) WHERE id % 2 = 0", "0")
	dag("delete/big_join", "SELECT count(*) FROM tb_bd TABLESAMPLE BERNOULLI (100) JOIN tb_big ON tb_bd.id = tb_big.id", "10000")

	// A sampled scan inside an expression subquery is sampled where it runs —
	// a producer stage on the DAG — not routed (review r1 B2: round 1 read it
	// whole on the DAG arms, where base read it whole on every arm).
	add("subquery/where_zero", "SELECT count(*) FROM tb_big WHERE id < (SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (0))", "0")
	add("subquery/where_101", "SELECT count(*) FROM tb_big WHERE id < (SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (101))", "ERR 2202H")
	add("subquery/where_system_zero", "SELECT count(*) FROM tb_big WHERE id < (SELECT count(*) FROM tb_p TABLESAMPLE SYSTEM (0))", "0")
	add("subquery/where_max_zero", "SELECT count(*) FROM tb_big WHERE id <= (SELECT max(id) FROM tb_p TABLESAMPLE BERNOULLI (0))", "0")
	add("subquery/where_big_fifty", "SELECT count(*) FROM tb_p WHERE 15000 < (SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (50))", "0")
	add("subquery/where_e400", "SELECT count(*) FROM tb_big WHERE id < (SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (1e400))", "ERR 22003")
	add("subquery/where_text", "SELECT count(*) FROM tb_big WHERE id < (SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI ('abc'))", "ERR 22P02")
	add("subquery/where_in_zero", "SELECT count(*) FROM tb_big WHERE id IN (SELECT id FROM tb_p TABLESAMPLE BERNOULLI (0))", "0")
	add("subquery/where_exists_zero", "SELECT count(*) FROM tb_big WHERE EXISTS (SELECT 1 FROM tb_p TABLESAMPLE BERNOULLI (0))", "0")
	add("subquery/having_zero", "SELECT id % 2 AS k, count(*) FROM tb_big GROUP BY 1 HAVING count(*) > (SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (0)) * 5000 ORDER BY 1", "0,10000; 1,10000")
	add("subquery/select_list_zero", "SELECT (SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (0)) AS c FROM tb_big LIMIT 1", "0")
	add("subquery/select_list_101", "SELECT (SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (101)) AS c FROM tb_big LIMIT 1", "ERR 2202H")
	add("subquery/select_list_e400", "SELECT (SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (1e400)) AS c FROM tb_big LIMIT 1", "ERR 22003")
	add("subquery/cte_body_zero", "WITH s AS (SELECT * FROM tb_p TABLESAMPLE BERNOULLI (0)) SELECT count(*) FROM tb_big WHERE id < (SELECT count(*) FROM s)", "0")
	add("subquery/join_on_zero", "SELECT count(*) FROM tb_p a JOIN tb_big b ON a.id = b.id AND b.id > (SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (0))", "3")

	// A 20 000-row sampled join runs on the DAG (review r1 P2: under a 64 KiB
	// fast path round 1 refused the 100 % join on the local budget, where
	// base answered it), and two samples of one table are two draws.
	dag("bigjoin/hundred", "SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (100) JOIN tb_big b2 ON tb_big.id = b2.id", "20000")
	dag("bigjoin/hundred_rows", "SELECT count(*) FROM (SELECT tb_big.id, b2.v FROM tb_big TABLESAMPLE BERNOULLI (100) JOIN tb_big b2 ON tb_big.id = b2.id) s", "20000")
	dag("bigjoin/fifty", "SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (50) JOIN tb_big b2 ON tb_big.id = b2.id", "RANGE 9000 11000")
	dag("bigjoin/zero", "SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (0) JOIN tb_big b2 ON tb_big.id = b2.id", "0")
	dag("bigjoin/system_hundred", "SELECT count(*) FROM tb_big TABLESAMPLE SYSTEM (100) JOIN tb_big b2 ON tb_big.id = b2.id", "20000")
	// The engine's alias position (other r19): PostgreSQL's spelling is
	// `tb_big a TABLESAMPLE …`, which answers the same.
	dag("bigjoin/two_samples_one_table", "SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (100) a JOIN tb_big TABLESAMPLE BERNOULLI (0) b ON a.id = b.id", "0")
	dag("bigjoin/grouped", "SELECT k, count(*) FROM (SELECT id % 4 AS k FROM tb_big TABLESAMPLE BERNOULLI (100)) s GROUP BY k ORDER BY k", "0,5000; 1,5000; 2,5000; 3,5000")
	dag("bigjoin/union_all", "SELECT count(*) FROM (SELECT id FROM tb_big TABLESAMPLE BERNOULLI (50) UNION ALL SELECT id FROM tb_big TABLESAMPLE BERNOULLI (0)) s", "RANGE 9000 11000")

	// The consumers above a sampled scan.
	add("projection/zero", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (0) ORDER BY id", "(0 rows)")
	add("projection/hundred", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (100) ORDER BY id", "1; 2; 3")
	add("projection/101", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (101) ORDER BY id", "ERR 2202H")
	add("projection/101_filtered", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (101) WHERE id > 5", "ERR 2202H")
	add("join/zero", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (0) JOIN tb_p q ON tb_p.id = q.id", "0")
	add("join/hundred", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (100) JOIN tb_p q ON tb_p.id = q.id", "3")
	add("join/101", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (101) JOIN tb_p q ON tb_p.id = q.id", "ERR 2202H")
	add("join/comma_zero", "SELECT count(*) FROM tb_p q, tb_p TABLESAMPLE BERNOULLI (0)", "0")
	add("cte/zero", "WITH c AS (SELECT * FROM tb_p TABLESAMPLE BERNOULLI (0)) SELECT count(*) FROM c", "0")
	add("cte/101", "WITH c AS (SELECT * FROM tb_p TABLESAMPLE BERNOULLI (101)) SELECT count(*) FROM c", "ERR 2202H")
	add("cte/e400", "WITH c AS (SELECT * FROM tb_p TABLESAMPLE BERNOULLI (1e400)) SELECT count(*) FROM c", "ERR 22003")
	add("subquery/exists_101", "SELECT 1 WHERE EXISTS (SELECT 1 FROM tb_p TABLESAMPLE BERNOULLI (101))", "ERR 2202H")
	add("subquery/scalar_101", "SELECT (SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (101))", "ERR 2202H")
	add("union/101", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (101) UNION ALL SELECT 1", "ERR 2202H")
	// The range is a scan-time check, and PostgreSQL never begins a scan under
	// a constant-false WHERE or HAVING or a LIMIT 0, so it answers (review r1
	// B4: base 9420d256 answered these; round 1 raised 2202H on every arm).
	// A filter that reads a row begins the scan on PostgreSQL too.
	add("never_scanned/where_false", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (101) WHERE false", "(0 rows)")
	add("never_scanned/where_false_count", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (101) WHERE false", "0")
	add("never_scanned/where_one_is_two", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (101) WHERE 1 = 2", "(0 rows)")
	add("never_scanned/where_null", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (101) WHERE NULL", "(0 rows)")
	add("never_scanned/limit_zero", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (101) LIMIT 0", "(0 rows)")
	add("never_scanned/having_false", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (101) HAVING false", "(0 rows)")
	add("never_scanned/derived_limit_zero", "SELECT count(*) FROM (SELECT * FROM tb_p TABLESAMPLE BERNOULLI (101) LIMIT 0) s", "0")
	add("never_scanned/cte_where_false", "WITH c AS (SELECT * FROM tb_p TABLESAMPLE BERNOULLI (101)) SELECT count(*) FROM c WHERE false", "0")
	add("never_scanned/null_where_false", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (NULL) WHERE false", "(0 rows)")
	add("never_scanned/system_minus_one_limit_zero", "SELECT id FROM tb_p TABLESAMPLE SYSTEM (-1) LIMIT 0", "(0 rows)")
	add("never_scanned/big_where_false", "SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (101) WHERE false", "0")
	empty1190("never_scanned/empty_limit_zero", "SELECT count(*) FROM tb_e TABLESAMPLE BERNOULLI (101) LIMIT 0", "(0 rows)")
	add("scanned/where_id_negative", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (101) WHERE id < 0", "ERR 2202H")
	// A subquery a constant short-circuits is never planned or run, as on
	// PostgreSQL, which folds `false AND …` / `true OR …` and a top-level
	// NULL conjunct before it plans a sublink (review r2 B1: the DAG arms
	// failed uncoded, or raised the subquery's 2202H, where base and
	// PostgreSQL answered; the reversed spellings raised on the embedded arms
	// too). A connective the constant does not decide keeps its subquery.
	sc := func(where string) string { return "SELECT count(*) FROM tb_p WHERE " + where }
	const ex101 = "EXISTS (SELECT 1 FROM tb_big TABLESAMPLE BERNOULLI (101))"
	const lt101 = "id < (SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (101))"
	add("short_circuit/false_and_exists", sc("false AND "+ex101), "0")
	add("short_circuit/true_or_exists", sc("true OR "+ex101), "3")
	add("short_circuit/false_and_exists_e400", sc("false AND EXISTS (SELECT 1 FROM tb_big TABLESAMPLE BERNOULLI (1e400))"), "0")
	add("short_circuit/false_and_exists_null", sc("false AND EXISTS (SELECT 1 FROM tb_big TABLESAMPLE BERNOULLI (NULL))"), "0")
	add("short_circuit/false_and_scalar", sc("false AND "+lt101), "0")
	add("short_circuit/exists_and_false", sc(ex101+" AND false"), "0")
	add("short_circuit/scalar_and_false", sc(lt101+" AND false"), "0")
	add("short_circuit/null_and_exists", sc("NULL AND "+ex101), "0")
	add("short_circuit/null_and_scalar", sc("NULL AND "+lt101), "0")
	add("short_circuit/one_is_two_and_scalar", sc("(1 = 2) AND "+lt101), "0")
	add("short_circuit/true_or_scalar", sc("true OR "+lt101), "3")
	add("short_circuit/not_true_or_exists", sc("NOT (true OR "+ex101+")"), "0")
	add("short_circuit/nested_false_and", sc("id > 0 AND (false AND "+ex101+")"), "0")
	add("short_circuit/nested_true_or", sc("id > 0 OR (true OR "+ex101+")"), "3")
	add("short_circuit/having_false_and_exists", "SELECT id % 2, count(*) FROM tb_p GROUP BY 1 HAVING false AND "+ex101, "(0 rows)")
	add("short_circuit/false_and_in", sc("false AND id IN (SELECT id FROM tb_big TABLESAMPLE BERNOULLI (101))"), "0")
	add("short_circuit/false_and_exists_div_zero_control", sc("false AND EXISTS (SELECT 1 FROM tb_big WHERE id < 1/0)"), "0")
	add("short_circuit/false_and_exists_unsampled_control", sc("false AND EXISTS (SELECT 1 FROM tb_big)"), "0")
	add("short_circuit/false_or_exists", sc("false OR "+ex101), "ERR 2202H")
	add("short_circuit/null_or_exists", sc("NULL OR "+ex101), "ERR 2202H")
	add("short_circuit/true_and_exists", sc("true AND "+ex101), "ERR 2202H")
	// The DAG's EXISTS arm answers the subquery's SQLSTATE, as the scalar
	// arm does (review r2 B1: every coded refusal but 42501 was swallowed
	// and the task failed uncoded).
	add("exists_coded/exists_101", sc(ex101), "ERR 2202H")
	add("exists_coded/not_exists_101", sc("NOT "+ex101), "ERR 2202H")
	add("exists_coded/exists_e400", sc("EXISTS (SELECT 1 FROM tb_big TABLESAMPLE BERNOULLI (1e400))"), "ERR 22003")
	add("exists_coded/exists_null", sc("EXISTS (SELECT 1 FROM tb_big TABLESAMPLE BERNOULLI (NULL))"), "ERR 2202H")
	// An uncorrelated EXISTS beside a filter no row passes: PostgreSQL runs
	// it once as an InitPlan and raises; the embedded pipeline evaluates the
	// conjunction per row and never reaches it (other r21).
	cells = append(cells, tbCell{name: "exists_coded/id_negative_and_exists", sql: sc("id < 0 AND " + ex101),
		want: "ERR 2202H", localPin: "0"})
	// Two failures in one statement: PostgreSQL folds the constant 1/0 when
	// it plans the statement and raises 22012 before any subquery runs; here
	// the conjunct is evaluated after the sample's range check (other r23).
	cells = append(cells, tbCell{name: "exists_coded/exists_and_division_by_zero",
		sql: sc(ex101 + " AND 1/0 = 1"), want: "ERR 2202H", pinned: true})
	// A sampled scan beside an empty join input: PostgreSQL begins it only
	// when its plan reads the sampled side first, so its answer follows its
	// join order (other r21); here the sample begins in either order. The
	// embedded arms raise 2202H; every coordinator arm raises it or #1190's
	// refusal of the empty relation (the fast path's local 2202H falls back
	// to the DAG, which meets #1190), so they are held to "an error".
	cells = append(cells, tbCell{name: "empty_join/empty_first", sql: "SELECT count(*) FROM tb_e JOIN tb_p TABLESAMPLE BERNOULLI (101) ON tb_e.id = tb_p.id",
		want: "CONTAINS ERR ", localPin: "ERR 2202H", pinned: true})
	cells = append(cells, tbCell{name: "empty_join/sampled_first", sql: "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (101) JOIN tb_e ON tb_e.id = tb_p.id",
		want: "CONTAINS ERR ", localPin: "ERR 2202H"})

	// EXPLAIN plans without scanning, shows the sample on the scan line
	// (review r1 N3), and raises the coercion's own failure.
	add("explain/fifty", "EXPLAIN SELECT * FROM tb_p TABLESAMPLE BERNOULLI (50)", "CONTAINS TABLESAMPLE BERNOULLI (50)")
	add("explain/101", "EXPLAIN SELECT * FROM tb_p TABLESAMPLE BERNOULLI (101)", "PLAN")
	add("explain/null", "EXPLAIN SELECT * FROM tb_p TABLESAMPLE BERNOULLI (NULL)", "PLAN")
	add("explain/minus_one", "EXPLAIN SELECT * FROM tb_p TABLESAMPLE BERNOULLI (-1)", "PLAN")
	add("explain/e400", "EXPLAIN SELECT * FROM tb_p TABLESAMPLE BERNOULLI (1e400)", "ERR 22003")
	add("explain/div_zero", "EXPLAIN SELECT * FROM tb_p TABLESAMPLE BERNOULLI (1/0)", "ERR 22012")

	// REPEATABLE is not parsed: its refusal is kept (PostgreSQL answers
	// (1), (0), (-1); raises 22003 for (1e400) and 2202G for (NULL)).
	for _, s := range []string{"1", "0", "-1", "1e400", "NULL"} {
		pin("repeatable/"+s, "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (100) REPEATABLE ("+s+")", "ERR 42601")
	}
	// The alias position (other r19, review r1 N4): PostgreSQL takes the
	// alias BEFORE the clause and refuses it after; this parser the reverse.
	pin("alias/before_clause", "SELECT count(*) FROM tb_p x TABLESAMPLE BERNOULLI (100)", "ERR 42601")
	pin("alias/after_clause", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (100) x", "3")

	// real's input function, outside TABLESAMPLE (review r1 B3).
	add("real_input/underflow", "SELECT CAST('1e-46' AS REAL)", "ERR 22003")
	add("real_input/underscore", "SELECT CAST('1_0' AS REAL)", "ERR 22P02")
	add("real_input/float8_underflow", "SELECT CAST('1e-400' AS DOUBLE PRECISION)", "ERR 22003")
	add("real_input/float8_underscore", "SELECT CAST('1_0' AS DOUBLE PRECISION)", "ERR 22P02")
	add("real_input/hex_underflow", "SELECT CAST('0xAp-2000' AS REAL)", "ERR 22003")
	add("real_input/hex_underflow_e_digit", "SELECT CAST('0xep-2000' AS DOUBLE PRECISION)", "ERR 22003")
	add("real_input/hex_zero", "SELECT CAST('0x0p-2000' AS REAL)", "0")
	add("bernoulli/hex_underflow", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI ('0xAp-2000')", "ERR 22003")
	return cells
}

func tbSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}}
}

func tbRows(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i + 1), "v": float64(i+1) * 1.5}
	}
	return rows
}

func tbTables() []tmdTable {
	return []tmdTable{
		{"tb_p", tbSchema(), tbRows(3)},
		{"tb_e", tbSchema(), nil},
		{"tb_big", tbSchema(), tbRows(20000)},
		{"tb_d", tbSchema(), tbRows(8)},
		{"tb_bd", tbSchema(), tbRows(20000)},
	}
}

// tbDeletes leave every odd id of tb_d and tb_bd. Each file of either table
// keeps rows, so the DELETE is a marker the scan applies (a narrowed
// selection), never a dropped file.
var tbDeletes = []string{
	"DELETE FROM tb_d WHERE id % 2 = 0",
	"DELETE FROM tb_bd WHERE id % 2 = 0",
}

func tbDelete(t *testing.T, ctx context.Context, db *wadjet.DB) {
	t.Helper()
	for _, d := range tbDeletes {
		if _, err := db.Execute(ctx, d); err != nil {
			t.Fatalf("%s: %v", d, err)
		}
	}
}

func tbStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := wadjet.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tb := range tbTables() {
		if err := db.CreateTable(ctx, tb.name, tb.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tb.name, err)
		}
		if len(tb.rows) == 0 {
			continue
		}
		ing := db.NewIngester(tb.name, tb.schema, nil, ingest.Config{MaxBufferRows: len(tb.rows) + 1, RowGroupSize: 4096})
		if err := ing.Ingest(ctx, tb.rows); err != nil {
			t.Fatalf("ingest %s: %v", tb.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tb.name, err)
		}
	}
	tbDelete(t, ctx, db)
	return db
}

type tbArm struct {
	name  string
	run   func(sql string) string
	coord *Coordinator // nil for an embedded arm
	// dag: every statement takes the stage DAG (LocalFastPathBytes 0).
	dag bool
}

func tbArms(t *testing.T, ctx context.Context) []tbArm {
	t.Helper()
	single := tbStandalone(t, ctx, 0)
	spilled := tbStandalone(t, ctx, 512*1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, tbTables())
		// The DELETEs go through a DB over the coordinator's own catalog, so
		// the markers are the ones the stage planner reads.
		db, err := wadjet.Open(ctx, wadjet.Config{
			Store: infra.store, Bucket: "test", MetaKV: infra.kv, Logger: infra.logger,
		})
		if err != nil {
			t.Fatalf("open DB over the coordinator's KV: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		tbDelete(t, ctx, db)
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	// A small fast path: tb_p and tb_d run on the coordinator's local
	// pipeline, tb_big and tb_bd (past 64 KiB) on the DAG — and a sampled
	// 20 000-row join is answered there, not refused on a local budget.
	coordS := stand(nil, func(c *Config) { c.LocalFastPathBytes = 64 << 10 })
	return []tbArm{
		{"single", func(s string) string { return tbRunSingle(ctx, single, s) }, nil, false},
		{"spilled512k", func(s string) string { return tbRunSingle(ctx, spilled, s) }, nil, false},
		{"dag", func(s string) string { return tbRunDAG(ctx, coord, s) }, coord, true},
		{"dag-shuffled", func(s string) string { return tbRunDAG(ctx, coordB, s) }, coordB, true},
		{"dag-morsel4", func(s string) string { return tbRunDAG(ctx, coordM, s) }, coordM, true},
		{"dag-fastpath64k", func(s string) string { return tbRunDAG(ctx, coordS, s) }, coordS, false},
	}
}

func tbErr(err error) string {
	if s := sqlerr.StateOf(err); s != "" {
		return "ERR " + s
	}
	return "ERR (uncoded) " + err.Error()
}

func tbRender(cells [][]any) string {
	if len(cells) == 0 {
		return "(0 rows)"
	}
	rows := make([]string, len(cells))
	for i, r := range cells {
		f := make([]string, len(r))
		for j, v := range r {
			if v == nil {
				f[j] = "NULL"
			} else {
				f[j] = fmt.Sprint(v)
			}
		}
		rows[i] = strings.Join(f, ",")
	}
	return strings.Join(rows, "; ")
}

func tbRunSingle(ctx context.Context, db *wadjet.DB, sql string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("PANIC %v", r)
		}
	}()
	res, err := db.Query(ctx, sql)
	if err != nil {
		return tbErr(err)
	}
	cells := make([][]any, len(res.Rows))
	for i := range res.Rows {
		cells[i] = res.Cells(i)
	}
	return tbRender(cells)
}

func tbRunDAG(ctx context.Context, c *Coordinator, sql string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("PANIC %v", r)
		}
	}()
	res, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		return tbErr(err)
	}
	if res.Error != "" {
		return "ERR (uncoded) " + res.Error
	}
	var cells [][]any
	if st := res.Stream(); st != nil {
		defer st.Close()
		for {
			bb, berr := st.Next(ctx)
			if berr != nil {
				return tbErr(berr)
			}
			if bb == nil {
				break
			}
			cells = append(cells, bb.ToRowValues()...)
		}
	} else {
		rows, rerr := res.Rows()
		if rerr != nil {
			return tbErr(rerr)
		}
		cols := res.OutputSchema()
		for _, r := range rows {
			row := make([]any, len(cols))
			for j, col := range cols {
				row[j] = r[col.Name]
			}
			cells = append(cells, row)
		}
	}
	return tbRender(cells)
}
