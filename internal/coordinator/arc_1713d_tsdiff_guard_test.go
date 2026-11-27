// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/expr"
)

// #1713d round 8: the timestamp-difference GUARD (expr's minus operator,
// timestamp_difference_guard.go) and the STATEMENT RULE
// (physical/validate_interval_use.go), on the six arms.
//
// PostgreSQL types `timestamp - timestamp` interval; this engine has no
// interval value for it and computed a number of milliseconds (ADR-0012
// temporal r16). The statement rule refuses a difference at plan time with
// PostgreSQL's diagnostics wherever it can type both operands; the guard is
// the layer that makes "no execution path turns a timestamp difference into
// a number" hold where it cannot. Each cell runs twice on every arm:
//
//   - the rule ENABLED: the disposition it gives (rule), PostgreSQL 17.11's
//     own SQLSTATE where PostgreSQL refuses, else the rule's 0A000;
//   - the rule DISABLED (expr.DisableTimestampDifferenceStatementRuleForTest): the
//     guard alone must refuse every difference-using cell with its 0A000 and
//     its own sentence — the proof that the guard is complete.
//
// Controls answer the same value in both modes (each is PostgreSQL's value;
// tdgControls). pg is PostgreSQL 17.11's answer (postgres:17-alpine,
// --locale=C, UTF8, TimeZone UTC, IntervalStyle postgres; 1713d_r8.md) over
// the same fixture, kept beside our disposition.

const tdgGuardSentence = "a timestamp difference (timestamp - timestamp) cannot yet be computed"

const tdgRuleSentence = "a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as "

type tdgCell struct {
	name, sql string
	// pg is PostgreSQL 17.11's answer.
	pg string
	// rule is the disposition with the statement rule enabled: "<SQLSTATE>|<reason>".
	rule string
	// guardOff, when set, is the disposition with the rule disabled where it
	// is not the guard's (a refusal PostgreSQL makes that precedes the minus).
	guardOff string
}

// tdgFixture is the tables the cells read (the reviewers' p_t / p_s).
var tdgFixture = []string{
	"CREATE TABLE tg_t (id BIGINT, ts TIMESTAMP, ts2 TIMESTAMP, d DATE, dur DURATION, s TEXT)",
	"INSERT INTO tg_t VALUES (1, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-02 03:00:00', DATE '2024-01-31', 1000000000, '2024-01-05 00:00:00'), " +
		"(2, TIMESTAMP '2024-01-01 00:00:00.5', TIMESTAMP '2024-01-01 00:00:01', DATE '2024-02-29', 2000000000, '2024-01-01'), " +
		"(3, NULL, TIMESTAMP '2024-01-01 00:00:00', DATE '2023-12-31', NULL, NULL)",
	"CREATE TABLE tg_s (id BIGINT, s TEXT)",
	"INSERT INTO tg_s VALUES (1, 'a'), (2, 'b'), (3, 'c')",
}

const (
	tdgResult = "0A000|" + tdgRuleSentence + "a query result column"
	tdgText   = "0A000|" + tdgRuleSentence + "the operand of a cast to text"
	tdgExtr   = "0A000|" + tdgRuleSentence + "an argument of extract"
	tdgNumber = "42883|operator does not exist: interval > integer"
)

func tdgCells() []tdgCell {
	return []tdgCell{
		// Round 7 review, item 2: the correlated outer column in a body with
		// its own FROM, and the star-bodied subquery. Both answered their
		// millisecond numbers at b0a3e59d and at 26a4ed70.
		{"corrOuter", "SELECT id, (SELECT tg_t.ts2 FROM tg_s LIMIT 1) - ts AS r FROM tg_t ORDER BY id",
			"1 day 03:00:00; 00:00:00.5; NULL", tdgResult, ""},
		{"corrOuterText", "SELECT id, CAST((SELECT tg_t.ts2 FROM tg_s LIMIT 1) - ts AS TEXT) AS r FROM tg_t ORDER BY id",
			"1 day 03:00:00; 00:00:00.5; NULL", tdgText, ""},
		{"corrOuterEpoch", "SELECT id, extract(epoch FROM (SELECT tg_t.ts2 FROM tg_s LIMIT 1) - ts) AS r FROM tg_t ORDER BY id",
			"97200.000000; 0.500000; NULL", tdgExtr, ""},
		{"starAgg", "SELECT id, ts2 - (SELECT * FROM (SELECT max(ts) FROM tg_t) z) AS r FROM tg_t ORDER BY id",
			"1 day 02:59:59.5; 00:00:00.5; -00:00:00.5", tdgResult, ""},
		{"starAggEpoch", "SELECT id, extract(epoch FROM ts2 - (SELECT * FROM (SELECT max(ts) FROM tg_t) z)) AS r FROM tg_t ORDER BY id",
			"97199.500000; 0.500000; -0.500000", tdgExtr, ""},
		{"starPlain", "SELECT id, ts2 - (SELECT * FROM (SELECT ts FROM tg_t WHERE id = 1) z) AS r FROM tg_t ORDER BY id",
			"1 day 03:00:00; 00:00:01; 00:00:00", tdgResult, ""},
		{"starPublish", "SELECT z.* FROM (SELECT ts2 - (SELECT * FROM (SELECT max(ts) FROM tg_t) w) AS k FROM tg_t) z",
			"1 day 02:59:59.5; 00:00:00.5; -00:00:00.5", tdgResult, ""},
		{"starPublishText", "SELECT CAST(z.* AS TEXT) AS r FROM (SELECT ts2 - (SELECT * FROM (SELECT max(ts) FROM tg_t) w) AS k FROM tg_t) z",
			"(\"1 day 02:59:59.5\"); (00:00:00.5); (-00:00:00.5)", tdgResult, ""},
		// Round 7's open case: the TIMESTAMP is the window function's
		// default. It answered NULL text (no number) at both bases; the
		// statement rule types it at round 8. The guard cannot see it: the
		// window evaluator declares lag over a NULL value STRING, so the
		// minus meets a text operand (tdgNotGuarded).
		{"lagNullDefault", "SELECT id, lag(NULL, 1, ts2) OVER (ORDER BY id) - ts AS r FROM tg_t ORDER BY id",
			"1 day 03:00:00; NULL; NULL", tdgResult, "notGuarded"},
		// Round 6 review: aggregate, window and scalar-subquery operands.
		{"aggPublish", "SELECT max(ts2) - min(ts) AS r FROM tg_t", "1 day 03:00:00", tdgResult, ""},
		{"aggEpoch", "SELECT extract(epoch FROM max(ts2) - min(ts)) AS r FROM tg_t", "97200.000000", tdgExtr, ""},
		{"aggHavingNumber", "SELECT count(*) AS n FROM tg_t HAVING max(ts2) - min(ts) > 1000", "ERR 42883", tdgNumber, ""},
		{"winLag", "SELECT id, ts2 - lag(ts2) OVER (ORDER BY id) AS r FROM tg_t ORDER BY id",
			"NULL; -1 days -02:59:59; -00:00:01", tdgResult, ""},
		{"winLagText", "SELECT id, CAST(ts2 - lag(ts2) OVER (ORDER BY id) AS TEXT) AS r FROM tg_t ORDER BY id",
			"NULL; -1 days -02:59:59; -00:00:01", tdgText, ""},
		{"subMax", "SELECT id, (SELECT max(ts2) FROM tg_t) - ts AS r FROM tg_t ORDER BY id",
			"1 day 03:00:00; 1 day 02:59:59.5; NULL", tdgResult, ""},
		{"subWhereNumber", "SELECT id FROM tg_t WHERE ts2 - (SELECT min(ts) FROM tg_t) > 1000", "ERR 42883", tdgNumber, ""},
		// Plain forms.
		{"plain", "SELECT id, ts2 - ts AS r FROM tg_t ORDER BY id", "1 day 03:00:00; 00:00:00.5; NULL", tdgResult, ""},
		{"plainNumber", "SELECT id, ts2 - ts > 1000 AS r FROM tg_t ORDER BY id", "ERR 42883", tdgNumber, ""},
		{"plainText", "SELECT id, CAST(ts2 - ts AS TEXT) AS r FROM tg_t ORDER BY id", "1 day 03:00:00; 00:00:00.5; NULL", tdgText, ""},
		{"plainEpoch", "SELECT id, extract(epoch FROM ts2 - ts) AS r FROM tg_t ORDER BY id", "97200.000000; 0.500000; NULL", tdgExtr, ""},
		// With the rule disabled the difference is typed as r16's double, and
		// the comparison rule refuses `double precision > interval` before
		// execution: no path reaches the minus (the guard's own refusal of
		// this shape is in expr.TestTimestampDifferenceGuardSites).
		{"plainWhere", "SELECT id FROM tg_t WHERE ts2 - ts > INTERVAL '1 hour' ORDER BY id", "1",
			"0A000|" + tdgRuleSentence + "an operand of a comparison (>)",
			"42883|operator does not exist: double precision > interval"},
		{"quoted", "SELECT id, ts2 - '2024-01-01' AS r FROM tg_t ORDER BY id", "1 day 03:00:00; 00:00:01; 00:00:00", tdgResult, ""},
		{"castDate", "SELECT id, CAST(d AS TIMESTAMP) - ts AS r FROM tg_t ORDER BY id", "30 days; 58 days 23:59:59.5; NULL", tdgResult, ""},
		{"literals", "SELECT TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00' AS r", "1 day", tdgResult, ""},
		{"clock", "SELECT now() - ts AS r FROM tg_t", "an interval per row", tdgResult, ""},
		{"arrayElement", "SELECT id, (ARRAY[ts2])[1] - ts AS r FROM tg_t ORDER BY id", "1 day 03:00:00; 00:00:00.5; NULL", tdgResult, ""},
		// A CTE-backed scalar subquery (the DAG defers it to a producer
		// stage and substitutes its answer) and a correlated one.
		{"cteSub", "WITH m AS (SELECT max(ts2) AS x FROM tg_t) SELECT id, (SELECT x FROM m) - ts AS r FROM tg_t ORDER BY id",
			"1 day 03:00:00; 1 day 02:59:59.5; NULL", tdgResult, ""},
		{"corrSub", "SELECT id, (SELECT max(b.ts2) FROM tg_t b WHERE b.id >= a.id) - ts AS r FROM tg_t a ORDER BY id",
			"1 day 03:00:00; 00:00:00.5; NULL", tdgResult, ""},
		{"shifted", "SELECT id, (ts2 + INTERVAL '1 day') - ts AS r FROM tg_t ORDER BY id", "2 days 03:00:00; 1 day 00:00:00.5; NULL", tdgResult, ""},
		{"dateTrunc", "SELECT id, date_trunc('day', ts2) - date_trunc('day', ts) AS r FROM tg_t ORDER BY id", "1 day; 00:00:00; NULL", tdgResult, ""},
		{"midnight", "SELECT id, date_trunc('day', ts2) - date_trunc('day', ts) > 0 AS r FROM tg_t ORDER BY id", "ERR 42883",
			"42883|operator does not exist: interval > integer", ""},
		{"groupKey", "SELECT count(*) AS n FROM tg_t GROUP BY ts2 - ts", "3 groups",
			"0A000|" + tdgRuleSentence + "a GROUP BY key", ""},
		{"orderKey", "SELECT id FROM tg_t ORDER BY ts2 - ts", "2; 1; 3",
			"0A000|" + tdgRuleSentence + "an ORDER BY key", ""},
		{"sum", "SELECT sum(ts2 - ts) AS r FROM tg_t", "1 day 03:00:00.5",
			"0A000|" + tdgRuleSentence + "an argument of sum", ""},
		{"joinKey", "SELECT a.id FROM tg_t a JOIN tg_t b ON a.ts2 - a.ts = b.ts2 - b.ts ORDER BY a.id", "1; 2",
			"0A000|" + tdgRuleSentence, ""},
		// With the rule disabled, a DAG plan that pushes `k IS NOT NULL`
		// through the projection evaluates `(ts2 - ts) IS NOT NULL`, which
		// needs no value (IsNull.tsDiffNull) and answers PostgreSQL's 2; a
		// plan that projects k meets the guard. Either is no number.
		{"derived", "SELECT count(*) AS n FROM (SELECT ts2 - ts AS k FROM tg_t) q WHERE k IS NOT NULL", "2", tdgResult,
			"guardOr:n:bigint | 2"},
		// The guard's limit: it is decided when the minus meets a batch, so
		// a statement that evaluates no row is not refused by it (zero rows,
		// no number; the column is declared double, r16's declaration). The
		// statement rule refuses it.
		{"noRows", "SELECT ts2 - ts AS r FROM tg_t WHERE id > 10", "(0 rows)", tdgResult, "guardOr:(0 rows)"},
		// PostgreSQL's own refusals stay theirs on both modes: the literal is
		// interval input, so the minus is no timestamp difference.
		{"quotedInterval", "SELECT id, ts2 - '1 day' AS r FROM tg_t ORDER BY id", "ERR 22007",
			"22007|invalid input syntax for type timestamp", "22007|invalid input syntax for type timestamp"},
		// IS [NOT] NULL needs no value: it answers, on both modes (the guard
		// reads the operands, IsNull.tsDiffNull).
		{"isNull", "SELECT id, (ts2 - ts) IS NULL AS b FROM tg_t ORDER BY id", "f; f; t", "answer", "answer"},
		{"isNotNullWhere", "SELECT id FROM tg_t WHERE (ts2 - ts) IS NOT NULL ORDER BY id", "1; 2", "answer", "answer"},
		{"aggIsNull", "SELECT (max(ts2) - min(ts)) IS NULL AS b FROM tg_t", "f", "answer", "answer"},
	}
}

// tdgControls must answer, the same on both modes and every arm: each answer
// is PostgreSQL 17.11's value (the measured outputs are in 1713d_r8.md; a
// type gap — bigint for integer, double for numeric — is the existing one).
func tdgControls() []tdgCell {
	return []tdgCell{
		{name: "ctl/dateDate", sql: "SELECT id, d - DATE '2024-01-01' AS r FROM tg_t ORDER BY id", pg: "30; 59; -1"},
		{name: "ctl/dateCol", sql: "SELECT id, d - d AS r FROM tg_t ORDER BY id", pg: "0; 0; 0"},
		{name: "ctl/castDates", sql: "SELECT id, CAST(ts2 AS DATE) - CAST(ts AS DATE) AS r FROM tg_t ORDER BY id", pg: "1; 0; NULL"},
		{name: "ctl/shifts", sql: "SELECT id, CAST(ts + INTERVAL '1 day' AS TEXT) AS a, CAST(ts - INTERVAL '1 day' AS TEXT) AS b FROM tg_t ORDER BY id",
			pg: "2024-01-02 00:00:00,2023-12-31 00:00:00; 2024-01-02 00:00:00.5,2023-12-31 00:00:00.5; NULL,NULL"},
		{name: "ctl/duration", sql: "SELECT id, dur - dur AS a, dur + dur AS b, dur > 1500000000 AS c FROM tg_t ORDER BY id",
			pg: "0,2000000000,f; 0,4000000000,t; NULL (bigint stand-in for DURATION)"},
		{name: "ctl/epoch", sql: "SELECT id, extract(epoch FROM ts) AS r FROM tg_t ORDER BY id", pg: "1704067200.000000; 1704067200.500000; NULL"},
		{name: "ctl/epochDiff", sql: "SELECT id, extract(epoch FROM ts2) - extract(epoch FROM ts) AS r FROM tg_t ORDER BY id", pg: "97200.000000; 0.500000; NULL"},
		{name: "ctl/engineFns", sql: "SELECT id, date_diff(ts2, ts) AS a, to_milliseconds(ts) AS b, to_unixtime(ts) AS c FROM tg_t ORDER BY id",
			pg: "engine extensions (no PostgreSQL function): whole days, epoch ms, epoch s"},
		{name: "ctl/timeBucket", sql: "SELECT id, CAST(time_bucket(INTERVAL '1 hour', ts2) AS TEXT) AS r FROM tg_t ORDER BY id",
			pg: "engine extension: 2024-01-02 03:00:00; 2024-01-01 00:00:00; 2024-01-01 00:00:00"},
		{name: "ctl/between", sql: "SELECT id FROM tg_t WHERE ts BETWEEN TIMESTAMP '2023-01-01 00:00:00' AND TIMESTAMP '2025-01-01 00:00:00' ORDER BY id", pg: "1; 2"},
		{name: "ctl/recent", sql: "SELECT id FROM tg_t WHERE ts > now() - INTERVAL '1 day' ORDER BY id", pg: "(no rows)"},
		{name: "ctl/shiftCompare", sql: "SELECT id FROM tg_t WHERE ts2 > ts + INTERVAL '1 hour' ORDER BY id", pg: "1"},
		{name: "ctl/orderShift", sql: "SELECT id FROM tg_t ORDER BY ts + INTERVAL '1 day', id", pg: "1; 2; 3"},
		{name: "ctl/windowFrame", sql: "SELECT id, count(*) OVER (ORDER BY ts2 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS n FROM tg_t ORDER BY id", pg: "2; 2; 1"},
		{name: "ctl/numeric", sql: "SELECT id, id - 1 AS a, CAST(id AS DOUBLE PRECISION) - 0.5 AS b FROM tg_t ORDER BY id", pg: "0,0.5; 1,1.5; 2,2.5"},
		{name: "ctl/tsCompare", sql: "SELECT id, ts2 > ts AS r FROM tg_t ORDER BY id", pg: "t; t; NULL"},
		{name: "ctl/minMax", sql: "SELECT CAST(max(ts2) AS TEXT) AS a, CAST(min(ts) AS TEXT) AS b FROM tg_t", pg: "2024-01-02 03:00:00,2024-01-01 00:00:00"},
	}
}

// tdgControlWant is each control's answer on this engine (equal to pg in
// value; see tdgControls).
var tdgControlWant = map[string]string{
	"ctl/between":      "id:bigint | 1; 2",
	"ctl/castDates":    "id:bigint,r:bigint | 1,1; 2,0; 3,NULL",
	"ctl/dateCol":      "id:bigint,r:bigint | 1,0; 2,0; 3,0",
	"ctl/dateDate":     "id:bigint,r:bigint | 1,30; 2,59; 3,-1",
	"ctl/duration":     "id:bigint,a:double precision,b:double precision,c:boolean | 1,0,2000000000,false; 2,0,4000000000,true; 3,NULL,NULL,NULL",
	"ctl/engineFns":    "id:bigint,a:double precision,b:bigint,c:double precision | 1,1,1704067200000,1704067200; 2,0,1704067200500,1704067200.5; 3,NULL,NULL,NULL",
	"ctl/epoch":        "id:bigint,r:double precision | 1,1704067200; 2,1704067200.5; 3,NULL",
	"ctl/epochDiff":    "id:bigint,r:double precision | 1,97200; 2,0.5; 3,NULL",
	"ctl/minMax":       "a:text,b:text | 2024-01-02 03:00:00,2024-01-01 00:00:00",
	"ctl/numeric":      "id:bigint,a:bigint,b:double precision | 1,0,0.5; 2,1,1.5; 3,2,2.5",
	"ctl/orderShift":   "id:bigint | 1; 2; 3",
	"ctl/recent":       "(0 rows)",
	"ctl/shiftCompare": "id:bigint | 1",
	"ctl/shifts":       "id:bigint,a:text,b:text | 1,2024-01-02 00:00:00,2023-12-31 00:00:00; 2,2024-01-02 00:00:00.5,2023-12-31 00:00:00.5; 3,NULL,NULL",
	"ctl/timeBucket":   "id:bigint,r:text | 1,2024-01-02 03:00:00; 2,2024-01-01 00:00:00; 3,2024-01-01 00:00:00",
	"ctl/tsCompare":    "id:bigint,r:boolean | 1,true; 2,true; 3,NULL",
	"ctl/windowFrame":  "id:bigint,n:bigint | 1,2; 2,2; 3,1",
}

// tdgCheck judges one run against a disposition "<SQLSTATE>|<reason>" or
// "answer".
func tdgCheck(want, got, errText string) bool {
	if want == "answer" {
		return !strings.HasPrefix(got, "ERR ") && !strings.HasPrefix(got, "PANIC")
	}
	state, reason, _ := strings.Cut(want, "|")
	return got == "ERR "+state && strings.Contains(errText, reason)
}

func TestArc1713dTimestampDifferenceGuardEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: six arms over the timestamp-difference guard cells")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	writer, arms := ivArmsFixture(t, ctx)
	for _, s := range tdgFixture {
		if got := writer(s); got != "ok" {
			t.Fatalf("%s: %s", s, got)
		}
	}
	cells := tdgCells()
	ruleOn := map[string]string{}
	for _, mode := range []string{"rule", "guard"} {
		restore := func() {}
		if mode == "guard" {
			restore = expr.DisableTimestampDifferenceStatementRuleForTest()
		}
		for _, c := range cells {
			want := c.rule
			if mode == "guard" {
				want = c.guardOff
				if want == "" {
					want = "0A000|" + tdgGuardSentence
				}
			}
			for _, arm := range arms {
				got, errText := arm.exec(c.sql)
				if mode == "rule" {
					ruleOn[c.name+"/"+arm.name] = got
				}
				if mode == "guard" && strings.HasPrefix(c.guardOff, "guardOr:") {
					if got != strings.TrimPrefix(c.guardOff, "guardOr:") && !tdgCheck("0A000|"+tdgGuardSentence, got, errText) {
						t.Errorf("%s [%s] on %s: %s\n  got %s %s", c.name, mode, arm.name, c.sql, got, errText)
					}
					continue
				}
				if c.guardOff == "notGuarded" && mode == "guard" {
					// Not guarded (lagNullDefault): no NUMBER may come out.
					if strings.HasPrefix(got, "ERR ") || tdgHasNumber(got) {
						t.Errorf("%s [%s] on %s: %s\n  got %s %s", c.name, mode, arm.name, c.sql, got, errText)
					}
					continue
				}
				if !tdgCheck(want, got, errText) {
					t.Errorf("%s [%s] on %s: %s\n  got  %s %s\n  want %s\n  PostgreSQL 17.11: %s",
						c.name, mode, arm.name, c.sql, got, errText, want, c.pg)
				}
				if mode == "guard" && c.rule == "answer" && got != ruleOn[c.name+"/"+arm.name] {
					t.Errorf("%s on %s: the guard moved an answer\n  rule  %s\n  guard %s", c.name, arm.name, ruleOn[c.name+"/"+arm.name], got)
				}
			}
		}
		for _, c := range tdgControls() {
			for _, arm := range arms {
				got, errText := arm.exec(c.sql)
				if strings.HasPrefix(got, "ERR ") || strings.HasPrefix(got, "PANIC") {
					t.Errorf("control %s [%s] on %s refused: %s %s", c.name, mode, arm.name, got, errText)
					continue
				}
				if w := tdgControlWant[c.name]; got != w {
					t.Errorf("control %s [%s] on %s\n  got  %s\n  want %s (PostgreSQL 17.11: %s)", c.name, mode, arm.name, got, w, c.pg)
				}
			}
		}
		restore()
	}
}

// tdgHasNumber reports a rendered answer holding a non-NULL value in its
// last column (the r column of the not-guarded cells).
func tdgHasNumber(got string) bool {
	_, rows, _ := strings.Cut(got, " | ")
	for _, r := range strings.Split(rows, "; ") {
		f := strings.Split(r, ",")
		if v := f[len(f)-1]; v != "" && v != "NULL" {
			return true
		}
	}
	return false
}
