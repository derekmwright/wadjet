// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/worker"
)

// ARC PT on FIVE ARMS.
//
// The grammar tables in internal/planner/sql assert what PARSES and the
// wadjet package asserts what each spelling ANSWERS against live PostgreSQL
// 17.11. This file asserts the property neither can see: that the answer is
// the SAME on single / single+budget / dag / dag-shuffled / dag+morsel4.
//
// It is not a formality for this arc. A predicate is evaluated by the
// vectorized kernel in one process and compiled again inside a worker
// fragment on the DAG arms (worker/filter_compile.go has its own walk over
// the same AST), so a construct the parser now produces — `like_escape`,
// `similar_to`, `bitwise_xor` from `#`, a BETWEEN under a comparison — is
// carried by a second compiler that has never seen it. A rename applied at a
// table function's SOURCE (#1184) is applied by whichever process opens that
// source.
//
// Counts are computed from the FIXTURE rather than copied from a run, so a
// wrong expectation cannot be inherited from a wrong engine.
func TestArcPTGrammarAnswersTheSameOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM,
		func(w *worker.Config) { w.MorselWorkers = 4 })

	arms := []struct {
		name string
		run  func(string) ([]string, error)
	}{
		{"single", func(sql string) ([]string, error) { return ptArmRun(tmdRunSingle(ctx, single, sql)) }},
		{"single+budget", func(sql string) ([]string, error) { return ptArmRun(tmdRunSingle(ctx, spilled, sql)) }},
		{"dag", func(sql string) ([]string, error) { return ptArmRun(tmdRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", func(sql string) ([]string, error) { return ptArmRun(tmdRunDAG(ctx, coordB, sql)) }},
		{"dag+morsel4", func(sql string) ([]string, error) { return ptArmRun(tmdRunDAG(ctx, coordM, sql)) }},
	}

	// The fixture's own truth: c_str is `s-%06d` and NULL on a stride, so a
	// SIMILAR TO / LIKE predicate over it has a count this test can derive.
	nonNullStr := 0
	strWithA1 := 0
	for _, r := range typematrix.Data(typematrix.Rows) {
		s, ok := r["c_str"].(string)
		if !ok {
			continue
		}
		nonNullStr++
		if strings.HasSuffix(s, "1") {
			strWithA1++
		}
	}

	for _, tc := range []struct {
		issue, name, sql string
		want             []string
		state            string // when the statement REFUSES on every arm
		// dagPin is a PRE-EXISTING `distributed` divergence: the three DAG
		// arms refuse with this text where the single arms answer. It is
		// PINNED with its mechanism rather than chased (engine first), and at
		// least one DAG arm must produce it — a pin that closes FAILS, which
		// is its proof.
		dagPin string
		pg     string
	}{
		// ---- #1168: SIMILAR TO is the SQL pattern language ---------------
		{issue: "#1168", name: "similar_percent_over_a_column",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_str SIMILAR TO 's-%'`,
			want: []string{fmt.Sprintf("n=int64:%d", nonNullStr)},
			pg:   "every non-NULL c_str — `%` is LIKE's wildcard, not a regex quantifier"},
		{issue: "#1168", name: "similar_anchored_over_a_column",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_str SIMILAR TO 's-'`,
			want: []string{"n=int64:0"},
			pg:   "0 — SIMILAR TO matches the WHOLE string"},
		{issue: "#1168", name: "similar_underscore_over_a_column",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_str SIMILAR TO 's-______'`,
			want: []string{fmt.Sprintf("n=int64:%d", nonNullStr)},
			pg:   "every non-NULL c_str: six digits"},
		{issue: "#1168", name: "similar_alternation_over_a_column",
			sql: `SELECT COUNT(*) AS n FROM typemx WHERE c_str SIMILAR TO '%(1|2)'`,
			want: []string{fmt.Sprintf("n=int64:%d",
				typematrixSuffixCount(t, "1")+typematrixSuffixCount(t, "2"))},
			pg: "the rows whose last digit is 1 or 2"},
		{issue: "#1168", name: "similar_dot_is_a_literal_over_a_column",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_str SIMILAR TO 's.%'`,
			want: []string{"n=int64:0"},
			pg:   "0 — `.` is a literal dot in this language"},
		{issue: "#1168", name: "not_similar_over_a_column",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_str NOT SIMILAR TO 's-%'`,
			want: []string{"n=int64:0"}, pg: "0"},
		{issue: "#1168", name: "similar_beside_a_group_by",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_str SIMILAR TO 's-00000%'`,
			want: []string{"n=int64:10"},
			pg:   "the ten rows s-000000..s-000009"},
		{issue: "#1168", name: "similar_in_a_case_in_the_select_list",
			sql: `SELECT CASE WHEN c_str SIMILAR TO 's-000001' THEN 'hit' ELSE 'miss' END AS v ` +
				`FROM typemx WHERE id = 1`,
			want: []string{"v=hit"}, pg: "hit"},
		{issue: "#1168", name: "similar_with_an_escape_clause",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_str SIMILAR TO 's#-%' ESCAPE '#'`,
			want: []string{fmt.Sprintf("n=int64:%d", nonNullStr)},
			pg:   "the escape makes `-` literal, which it already was"},
		// A DANGLING escape contributes NOTHING on the server, so a pattern
		// that ends in one matches what the rest of it matches. This engine
		// matched NOTHING, and the WHERE below selected no row where 17.11
		// selects every non-NULL one — a wrong ROW SET on all five arms
		// (round-1 review, B2).
		{issue: "#1168", name: "dangling_escape_selects_every_non_null_row",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_str SIMILAR TO '%!' ESCAPE '!'`,
			want: []string{fmt.Sprintf("n=int64:%d", nonNullStr)},
			pg:   "every non-NULL c_str"},
		{issue: "#1168", name: "dangling_default_escape_over_a_column",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_str SIMILAR TO 's-%\'`,
			want: []string{fmt.Sprintf("n=int64:%d", nonNullStr)},
			pg:   "every non-NULL c_str"},
		{issue: "#1168", name: "dangling_escape_projected",
			sql:  `SELECT ('abc' SIMILAR TO 'abc!' ESCAPE '!') AS v FROM typemx WHERE id = 1`,
			want: []string{"v=bool:true"}, pg: "t"},
		{issue: "#1168", name: "dangling_escape_over_the_empty_string",
			sql:  `SELECT ('' SIMILAR TO '!' ESCAPE '!') AS v FROM typemx WHERE id = 1`,
			want: []string{"v=bool:true"}, pg: "t"},
		{issue: "#1168", name: "not_similar_dangling_escape",
			sql:  `SELECT ('abc' NOT SIMILAR TO 'abc!' ESCAPE '!') AS v FROM typemx WHERE id = 1`,
			want: []string{"v=bool:false"}, pg: "f"},
		{issue: "#1168", name: "escaped_escape_is_a_literal",
			sql:  `SELECT ('a!b' SIMILAR TO 'a!!b' ESCAPE '!') AS v FROM typemx WHERE id = 1`,
			want: []string{"v=bool:true"}, pg: "t"},
		{issue: "#1168", name: "escape_before_a_wildcard",
			sql:  `SELECT ('a%b' SIMILAR TO 'a!%b' ESCAPE '!') AS v FROM typemx WHERE id = 1`,
			want: []string{"v=bool:true"}, pg: "t"},
		{issue: "#1168", name: "escape_before_an_underscore",
			sql:  `SELECT ('a_b' SIMILAR TO 'a!_b' ESCAPE '!') AS v FROM typemx WHERE id = 1`,
			want: []string{"v=bool:true"}, pg: "t"},
		{issue: "#1168", name: "escape_before_a_metacharacter",
			sql:  `SELECT ('a(b' SIMILAR TO 'a!(b' ESCAPE '!') AS v FROM typemx WHERE id = 1`,
			want: []string{"v=bool:true"}, pg: "t"},
		{issue: "#1168", name: "a_null_escape_makes_the_predicate_null",
			sql:  `SELECT (('abc' SIMILAR TO 'abc' ESCAPE NULL) IS NULL) AS v FROM typemx WHERE id = 1`,
			want: []string{"v=bool:true"}, pg: "NULL"},
		{issue: "#1168", name: "a_pattern_that_is_not_a_pattern_refuses",
			sql:   `SELECT COUNT(*) AS n FROM typemx WHERE c_str SIMILAR TO '*'`,
			state: "2201B",
			pg:    "2201B invalid regular expression: quantifier operand invalid"},

		// ---- #1169: LIKE … ESCAPE, and the standard spellings ------------
		{issue: "#1169", name: "like_escape_over_a_column",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_str LIKE 's!-%' ESCAPE '!'`,
			want: []string{fmt.Sprintf("n=int64:%d", nonNullStr)}, pg: "every non-NULL c_str"},
		{issue: "#1169", name: "like_escape_makes_the_wildcard_literal",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_str LIKE 's-!%' ESCAPE '!'`,
			want: []string{"n=int64:0"}, pg: "0 — `!%` is a literal per cent sign"},
		{issue: "#1169", name: "not_like_escape_over_a_column",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_str NOT LIKE 's-!%' ESCAPE '!'`,
			want: []string{fmt.Sprintf("n=int64:%d", nonNullStr)}, pg: "every non-NULL c_str"},
		{issue: "#1169", name: "like_escape_in_a_having",
			sql: `SELECT g, COUNT(*) AS n FROM typemx WHERE g = 0 GROUP BY g ` +
				`HAVING MIN(c_str) LIKE 's!-%' ESCAPE '!'`,
			want: []string{fmt.Sprintf("g=int32:0|n=int64:%d", typematrixGroupCount(t, 0))},
			pg:   "the group survives the HAVING"},
		{issue: "#1169", name: "like_escape_too_long_refuses",
			sql:   `SELECT COUNT(*) AS n FROM typemx WHERE c_str LIKE 's!-%' ESCAPE '!!'`,
			state: "22025", pg: "22025 invalid escape string — the server's class for BOTH " +
				"escape-string failures, which this gate pinned as 22019 (round-1 review, P1)"},
		{issue: "#1169", name: "substring_from_for_over_a_column",
			sql:  `SELECT SUBSTRING(c_str FROM 1 FOR 2) AS v FROM typemx WHERE id = 1`,
			want: []string{"v=s-"}, pg: "s-"},
		{issue: "#1169", name: "substring_regex_over_a_column",
			sql:  `SELECT SUBSTRING(c_str FROM '[0-9]+') AS v FROM typemx WHERE id = 1`,
			want: []string{"v=000001"}, pg: "000001"},
		{issue: "#1169", name: "substring_in_a_group_key",
			sql: `SELECT SUBSTRING(c_str FROM 1 FOR 2) AS k, COUNT(*) AS n FROM typemx ` +
				`WHERE c_str IS NOT NULL GROUP BY SUBSTRING(c_str FROM 1 FOR 2) ORDER BY k`,
			want: []string{fmt.Sprintf("k=s-|n=int64:%d", nonNullStr)},
			pg:   "one group: every non-NULL c_str starts `s-`"},
		{issue: "#1169", name: "overlay_over_a_column",
			sql:  `SELECT OVERLAY(c_str PLACING 'X' FROM 1) AS v FROM typemx WHERE id = 1`,
			want: []string{"v=X-000001"}, pg: "X-000001"},
		{issue: "#1169", name: "overlay_from_zero_refuses",
			sql:   `SELECT OVERLAY(c_str PLACING 'X' FROM 0) AS v FROM typemx WHERE id = 1`,
			state: "22011", pg: "22011 negative substring length not allowed"},
		{issue: "#1169", name: "left_over_a_column",
			sql:  `SELECT LEFT(c_str, 2) AS v FROM typemx WHERE id = 1`,
			want: []string{"v=s-"}, pg: "s-"},
		{issue: "#1169", name: "right_over_a_column_negative_count",
			sql:  `SELECT RIGHT(c_str, -2) AS v FROM typemx WHERE id = 1`,
			want: []string{"v=000001"}, pg: "000001"},
		{issue: "#1169", name: "left_in_a_predicate",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE LEFT(c_str, 2) = 's-'`,
			want: []string{fmt.Sprintf("n=int64:%d", nonNullStr)}, pg: "every non-NULL c_str"},
		{issue: "#1169", name: "normalize_over_a_column",
			sql:  `SELECT NORMALIZE(c_str, NFC) AS v FROM typemx WHERE id = 1`,
			want: []string{"v=s-000001"}, pg: "s-000001"},
		// LOCALTIMESTAMP in a PREDICATE, which is the position that made the
		// DAG arms re-parse the planner's RENDERED text (`localtimestamp()`)
		// — a spelling the parser refused, so the query failed on three arms
		// and answered on two. The predicate is a NULL test rather than a
		// self-comparison: this engine evaluates the clock PER ROW, so
		// `LOCALTIMESTAMP >= LOCALTIMESTAMP` is false for the rows that
		// straddle a millisecond (recorded, a filing candidate: PostgreSQL
		// answers the statement's start time once).
		{issue: "#1169", name: "localtimestamp_in_a_predicate",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE LOCALTIMESTAMP IS NOT NULL`,
			want: []string{fmt.Sprintf("n=int64:%d", typematrix.Rows)}, pg: "every row"},

		// ---- #1179: the `#` operator ------------------------------------
		{issue: "#1179", name: "hash_xor_projected",
			sql:  `SELECT c_i64 # 1 AS v FROM typemx WHERE id = 1`,
			want: []string{"v=int64:" + fmt.Sprint(typematrixI64(t, 1)^1)}, pg: "the XOR"},
		{issue: "#1179", name: "hash_xor_in_a_predicate",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_i64 # 0 = c_i64`,
			want: []string{fmt.Sprintf("n=int64:%d", typematrixNonNullI64(t))},
			pg:   "XOR with zero is the value itself"},
		{issue: "#1179", name: "hash_xor_in_a_group_key",
			sql: `SELECT (g # 1) AS k, COUNT(*) AS n FROM typemx WHERE g IS NOT NULL ` +
				`GROUP BY (g # 1) ORDER BY k`,
			want: typematrixXorGroups(t), pg: "one group per XORed group key"},
		// The ORDER BY is the ANSWER here, so this cell is compared row by
		// row as produced: ids 0..3 XOR 1 are 1, 0, 3, 2, which orders the
		// ids 1, 0, 3, 2.
		{issue: "#1179", name: "hash_xor_in_an_order_by",
			sql:  `SELECT id FROM typemx WHERE id <= 3 ORDER BY id # 1, id`,
			want: []string{"id=int64:1", "id=int64:0", "id=int64:3", "id=int64:2"},
			pg:   "1, 0, 3, 2"},

		// ---- #1180 / #1183: the predicate band --------------------------
		{issue: "#1180", name: "between_then_comparison_in_a_where",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE (id BETWEEN 1 AND 10) = true`,
			want: []string{"n=int64:10"}, pg: "10"},
		{issue: "#1180", name: "between_then_comparison_unparenthesized",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE id BETWEEN 1 AND 10 = true`,
			want: []string{"n=int64:10"}, pg: "10 — BETWEEN binds tighter than `=`"},
		{issue: "#1180", name: "between_then_is_true_in_a_having",
			sql: `SELECT g, COUNT(*) AS n FROM typemx WHERE g = 0 GROUP BY g ` +
				`HAVING COUNT(*) BETWEEN 1 AND 100000 IS TRUE`,
			want: []string{fmt.Sprintf("g=int32:0|n=int64:%d", typematrixGroupCount(t, 0))},
			pg:   "the group survives"},
		{issue: "#1183", name: "is_not_unknown_over_a_parenthesized_predicate",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE ((c_bool) AND (id > 1)) IS NOT UNKNOWN`,
			want: []string{fmt.Sprintf("n=int64:%d", typematrixIsNotUnknownCount(t))},
			pg:   "the rows where neither operand is NULL, plus the FALSE ones"},
		{issue: "#1183", name: "is_true_in_a_join_on_clause",
			sql:  `SELECT COUNT(*) AS n FROM typemx a JOIN typemx_dim d ON (a.g = d.k) IS TRUE`,
			want: []string{fmt.Sprintf("n=int64:%d", typematrixJoinCount(t))},
			pg:   "the equi-join's own row count"},
		{issue: "#1183", name: "is_not_unknown_projected",
			sql:  `SELECT ((c_bool) IS NOT UNKNOWN) AS v FROM typemx WHERE id = 1`,
			want: []string{"v=bool:true"}, pg: "t"},
		{issue: "#1183", name: "comparison_then_is_true_in_a_where",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE id = 1 IS TRUE`,
			want: []string{"n=int64:1"}, pg: "1"},

		// ---- the TRUTH CONTEXT, which the DML gate found through `#` ----
		// A refusal that depends on which evaluator ran is the two-path class
		// these arms exist for: the worker's fragment compiles the filter
		// again, so a refusal made at the planner has to be made at BOTH
		// entries.
		{issue: "#1179", name: "an_integer_conjunct_is_refused",
			sql:   `SELECT COUNT(*) AS n FROM typemx WHERE id > 0 AND c_i64 # 3`,
			state: "42804", pg: "42804 argument of AND must be type boolean, not type bigint"},
		{issue: "#1179", name: "a_text_call_in_a_where_is_refused",
			sql:   `SELECT COUNT(*) AS n FROM typemx WHERE UPPER(c_str)`,
			state: "42804", pg: "42804 argument of WHERE must be type boolean, not type text"},
		{issue: "#1179", name: "a_power_operator_conjunct_is_refused",
			sql:   `SELECT COUNT(*) AS n FROM typemx WHERE id > 0 AND 2 ^ 3`,
			state: "42804", pg: "42804 argument of AND must be type boolean, not type double precision"},

		// ---- #1184: a table function's column-alias list ----------------
		// A table function as the ONLY FROM item is not a DAG stage on this
		// engine — `stage scan-0 has no dependencies and no ScanFiles` — and
		// that is true with no alias list at all, which the CONTROL cell
		// below measures. The alias list is applied at the SOURCE, so
		// whichever process opens the function applies it; the pin is the
		// stage planner's, not this arc's, and it is recorded `distributed`.
		{issue: "#1184", name: "control_a_table_function_with_no_list",
			sql:    `SELECT generate_series AS v FROM generate_series(1,2) ORDER BY v`,
			want:   []string{"v=int64:1", "v=int64:2"},
			dagPin: "no dependencies and no ScanFiles",
			pg:     "1;2"},
		{issue: "#1184", name: "generate_series_renamed",
			sql:    `SELECT x FROM generate_series(1,3) AS gs(x) ORDER BY x`,
			want:   []string{"x=int64:1", "x=int64:2", "x=int64:3"},
			dagPin: "no dependencies and no ScanFiles", pg: "1;2;3"},
		// COUNT rather than SUM: an integer SUM over a TABLE FUNCTION's column
		// is float64 here with or without the alias list — a numeric
		// DECLARATION gap (ADR-0024 says an integer SUM is bigint) that this
		// arc measured and recorded rather than widened a cell to hide.
		{issue: "#1184", name: "generate_series_renamed_without_as",
			sql:    `SELECT COUNT(x) AS n FROM generate_series(1,3) gs(x)`,
			want:   []string{"n=int64:3"},
			dagPin: "no dependencies and no ScanFiles", pg: "3"},
		{issue: "#1184", name: "too_many_names_refuses",
			sql: `SELECT * FROM generate_series(1,3) AS gs(x, y)`, state: "42P10",
			dagPin: "no dependencies and no ScanFiles",
			pg:     `42P10 table "gs" has 1 columns available but 2 columns specified`},
	} {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			pinnedArms := 0
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
				if tc.dagPin != "" && strings.HasPrefix(arm.name, "dag") {
					if err == nil {
						t.Errorf("%s arm ANSWERED %v: the pinned distributed refusal (%q) is "+
							"gone, so delete the pin — that is its proof\n  SQL: %s",
							arm.name, got, tc.dagPin, tc.sql)
						continue
					}
					if !strings.Contains(err.Error(), tc.dagPin) {
						t.Errorf("%s arm refused with something other than the pinned "+
							"mechanism %q: %v\n  SQL: %s", arm.name, tc.dagPin, err, tc.sql)
						continue
					}
					pinnedArms++
					continue
				}
				if tc.state != "" {
					if err == nil {
						t.Errorf("%s arm ANSWERED %v; PostgreSQL 17.11 refuses this with %s\n  SQL: %s",
							arm.name, got, tc.state, tc.sql)
						continue
					}
					if state := sqlerr.StateOf(err); state != tc.state {
						t.Errorf("%s arm raised SQLSTATE %s, want %s\n  err: %v\n  SQL: %s",
							arm.name, state, tc.state, err, tc.sql)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  PostgreSQL 17.11: %s\n  SQL: %s",
						arm.name, err, tc.pg, tc.sql)
					continue
				}
				if strings.Join(got, ";") != strings.Join(tc.want, ";") {
					t.Errorf("%s arm answered\n  got  %v\n  want %v (PostgreSQL 17.11: %s)\n  SQL: %s",
						arm.name, got, tc.want, tc.pg, tc.sql)
				}
			}
			if tc.dagPin != "" && pinnedArms == 0 {
				t.Errorf("the pinned distributed refusal %q was produced by NO arm; delete the pin",
					tc.dagPin)
			}
		})
	}
}

func typematrixSuffixCount(t *testing.T, suffix string) int {
	t.Helper()
	n := 0
	for _, r := range typematrix.Data(typematrix.Rows) {
		if s, ok := r["c_str"].(string); ok && strings.HasSuffix(s, suffix) {
			n++
		}
	}
	return n
}

func typematrixGroupCount(t *testing.T, g int32) int {
	t.Helper()
	n := 0
	for _, r := range typematrix.Data(typematrix.Rows) {
		if v, ok := r["g"].(int32); ok && v == g {
			n++
		}
	}
	return n
}

// ptArmRun renders a result ROW BY ROW as produced. It is not na2Run: that
// one sorts, which turns an ORDER BY cell into a multiset comparison and hides
// a wrong order (arc PS's round-1 review). Every cell in this file carries its
// own total order, is a single row, or is a count.
func ptArmRun(res *oracle.Result, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(res.Rows))
	for i, r := range res.Rows {
		parts := make([]string, 0, len(res.Columns))
		for ci, c := range res.Columns {
			v := r[c]
			if res.RowValues != nil && i < len(res.RowValues) && ci < len(res.RowValues[i]) {
				v = res.RowValues[i][ci]
			}
			switch t := v.(type) {
			case nil:
				parts = append(parts, c+"=NULL")
			case string:
				parts = append(parts, c+"="+t)
			default:
				parts = append(parts, fmt.Sprintf("%s=%T:%v", c, v, v))
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out, nil
}

func typematrixI64(t *testing.T, id int64) int64 {
	t.Helper()
	for _, r := range typematrix.Data(typematrix.Rows) {
		if v, ok := r["id"].(int64); ok && v == id {
			if x, ok := r["c_i64"].(int64); ok {
				return x
			}
			t.Fatalf("c_i64 is NULL for id %d", id)
		}
	}
	t.Fatalf("no row with id %d", id)
	return 0
}

func typematrixNonNullI64(t *testing.T) int {
	t.Helper()
	n := 0
	for _, r := range typematrix.Data(typematrix.Rows) {
		if _, ok := r["c_i64"].(int64); ok {
			n++
		}
	}
	return n
}

// typematrixXorGroups is the group key `g # 1` and its count, in the order
// the statement's ORDER BY produces.
func typematrixXorGroups(t *testing.T) []string {
	t.Helper()
	counts := map[int32]int{}
	for _, r := range typematrix.Data(typematrix.Rows) {
		g, ok := r["g"].(int32)
		if !ok {
			continue
		}
		counts[g^1]++
	}
	keys := make([]int32, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, fmt.Sprintf("k=int32:%d|n=int64:%d", k, counts[k]))
	}
	return out
}

// typematrixIsNotUnknownCount is `((c_bool) AND (id > 1)) IS NOT UNKNOWN`
// computed over the fixture: the conjunction is UNKNOWN only when c_bool is
// NULL and the other operand is TRUE.
func typematrixIsNotUnknownCount(t *testing.T) int {
	t.Helper()
	n := 0
	for _, r := range typematrix.Data(typematrix.Rows) {
		id, _ := r["id"].(int64)
		b, ok := r["c_bool"].(bool)
		switch {
		case ok && !b:
			n++ // FALSE AND anything is FALSE
		case !ok && id <= 1:
			n++ // NULL AND FALSE is FALSE
		case ok:
			n++ // TRUE AND a known value
		}
	}
	return n
}

// typematrixJoinCount is the row count of `typemx a JOIN typemx_dim d ON a.g = d.k`.
func typematrixJoinCount(t *testing.T) int {
	t.Helper()
	dim := map[int32]int{}
	for _, r := range typematrix.DimData() {
		if k, ok := r["k"].(int32); ok {
			dim[k]++
		}
	}
	n := 0
	for _, r := range typematrix.Data(typematrix.Rows) {
		if g, ok := r["g"].(int32); ok {
			n += dim[g]
		}
	}
	return n
}
