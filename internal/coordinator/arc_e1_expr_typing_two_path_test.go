package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Arc E1's shapes on the DISTRIBUTED arms, which is where each of them reaches
// a mechanism the single-process path does not have.
//
// #855's refusals travel through a WORKER's per-row channel and back over
// NATS: the question there is whether a client is handed the SQLSTATE or a
// stalled query. #850's lift REWRITES the aggregate set, and the DAG splits
// every aggregate into a partial and a final — `SUM(x) + k*COUNT(x)` is the
// shape where COUNT is per-partition and has to be summed rather than
// re-counted. #652's refusal is raised at PLAN time, so the arms answer the
// question of whether it survives the coordinator's own planning.
//
// Every expectation is live PostgreSQL 17.11 (the arc's ROUND0 carries the
// transcripts); the single arms are here beside the DAG ones so a divergence
// names which path moved.
func TestArcE1ExprTypingOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	arms := func() []struct {
		name string
		run  func(string) ([]string, error)
	} {
		return []struct {
			name string
			run  func(string) ([]string, error)
		}{
			{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
			{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
			{"dag-shuffled", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
		}
	}

	// ---------------------------------------------------------------- #855
	// A per-row refusal raised inside a WORKER. The failure mode this guards
	// is not a wrong code, it is a query that never ends: the refusal has to
	// travel out of the evaluator, out of the task, and back to the caller as
	// an error.
	for _, tc := range []struct{ name, sql, state, msg string }{
		{"date_trunc_unknown_unit",
			`SELECT DATE_TRUNC('bogus', c_ts) AS v FROM typemx WHERE id < 100`,
			"22023", `unit "bogus" not recognized for type timestamp without time zone`},
		{"width_bucket_zero_count",
			`SELECT WIDTH_BUCKET(c_f64, 0.0, 10.0, 0) AS v FROM typemx WHERE id < 100`,
			"2201G", "count must be greater than zero"},
		{"split_part_zero_position",
			`SELECT SPLIT_PART(c_str, '-', 0) AS v FROM typemx WHERE id < 100`,
			"22023", "field position must not be zero"},
		{"chr_nul",
			`SELECT CHR(c_i32 - c_i32) AS v FROM typemx WHERE id < 100`,
			"54000", "null character not permitted"},
		{"substring_negative_length",
			`SELECT SUBSTRING(c_str, 2, -1) AS v FROM typemx WHERE id < 100`,
			"22011", "negative substring length not allowed"},
	} {
		t.Run("#855/"+tc.name, func(t *testing.T) {
			for _, arm := range arms() {
				got, err := arm.run(tc.sql)
				if err == nil {
					t.Errorf("%s arm ANSWERED %v; PostgreSQL 17.11 raises [%s] %s",
						arm.name, got, tc.state, tc.msg)
					continue
				}
				if state := sqlerr.StateOf(err); state != tc.state {
					t.Errorf("%s arm raised SQLSTATE %s, want %s\n  err: %v",
						arm.name, state, tc.state, err)
				}
				if !strings.Contains(err.Error(), tc.msg) {
					t.Errorf("%s arm: %q does not carry PostgreSQL's message %q",
						arm.name, err, tc.msg)
				}
			}
		})
	}

	// ---------------------------------------------------------------- #856
	// The character-indexing family through a shuffle.
	//
	// `c_str` is pure ASCII ("s-000001"), where a byte count and a character
	// count agree — so every cell here appends a MULTI-BYTE suffix to it. That
	// is the whole point: over the ASCII column alone these would pass for a
	// kernel that still indexed bytes, which is a fixture that cannot fail.
	// `c_str || 'éàü'` is 11 characters and 14 octets.
	for _, tc := range []struct {
		name, sql string
		want      []string
	}{
		{"length_of_a_multibyte_expression",
			`SELECT LENGTH(c_str || 'éàü') AS v FROM typemx WHERE id = 1`,
			[]string{"v=int32:11"}},
		{"octet_length_is_the_control",
			`SELECT OCTET_LENGTH(c_str || 'éàü') AS v FROM typemx WHERE id = 1`,
			[]string{"v=int32:14"}},
		{"substr_across_the_boundary",
			`SELECT SUBSTR(c_str || 'éàü', 8, 3) AS v FROM typemx WHERE id = 1`,
			[]string{"v=1éà"}},
		{"strpos_past_the_multibyte_part",
			`SELECT STRPOS('éàü' || c_str, '-') AS v FROM typemx WHERE id = 1`,
			[]string{"v=int32:5"}},
		{"reverse_of_a_multibyte_expression",
			`SELECT REVERSE(c_str || 'éàü') AS v FROM typemx WHERE id = 1`,
			[]string{"v=üàé100000-s"}},
		// The character count as a shuffle KEY: a path that counted bytes here
		// would partition on a different number.
		{"length_grouped",
			`SELECT LENGTH(c_str || 'éàü') AS k, COUNT(*) AS n FROM typemx WHERE id < 10 ` +
				`GROUP BY LENGTH(c_str || 'éàü')`,
			[]string{"k=int32:11|n=int64:10"}},
	} {
		t.Run("#856/"+tc.name, func(t *testing.T) {
			for _, arm := range arms() {
				got, err := arm.run(tc.sql)
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				if len(got) != len(tc.want) || (len(got) > 0 && got[0] != tc.want[0]) {
					t.Errorf("%s arm: got %v, want %v\n  SQL: %s", arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}

	// ---------------------------------------------------------------- #652
	// A destination that names no type is refused at PLAN time, so the
	// coordinator's own planning is the thing under test — including inside a
	// derived table, where PostgreSQL raises at parse time too.
	for _, tc := range []struct{ name, sql string }{
		{"projected", `SELECT CAST(c_i64 AS bogustype) AS v FROM typemx WHERE id = 1`},
		{"in_a_derived_table",
			`SELECT v FROM (SELECT CAST(c_i64 AS bogustype) AS v FROM typemx WHERE id = 1) x`},
		{"in_an_aggregate_argument", `SELECT MAX(CAST(c_i64 AS bogustype)) AS v FROM typemx`},
	} {
		t.Run("#652/"+tc.name, func(t *testing.T) {
			for _, arm := range arms() {
				got, err := arm.run(tc.sql)
				if err == nil {
					t.Errorf("%s arm ANSWERED %v; PostgreSQL 17.11 raises 42704 "+
						"`type \"bogustype\" does not exist`", arm.name, got)
					continue
				}
				if state := sqlerr.StateOf(err); state != "42704" {
					t.Errorf("%s arm raised SQLSTATE %s, want 42704\n  err: %v",
						arm.name, state, err)
				}
			}
		})
	}

	// ---------------------------------------------------------------- #544
	// A TIMESTAMP coerced to text, through a shuffle. The GROUPED cell is the
	// one that matters: the rendered text becomes a shuffle KEY, so a path
	// that rendered the epoch would partition on different bytes.
	for _, tc := range []struct {
		name, sql string
		want      []string
	}{
		{"cast_to_text", `SELECT CAST(c_ts AS TEXT) AS v FROM typemx WHERE id = 0`,
			[]string{"v=2023-11-14 22:13:20"}},
		{"concat_operator", `SELECT c_ts || '' AS v FROM typemx WHERE id = 0`,
			[]string{"v=2023-11-14 22:13:20"}},
		{"upper", `SELECT UPPER(c_ts) AS v FROM typemx WHERE id = 0`,
			[]string{"v=2023-11-14 22:13:20"}},
		{"grouped_by_the_rendering",
			`SELECT CAST(c_ts AS TEXT) AS k, COUNT(*) AS n FROM typemx WHERE id = 0 GROUP BY CAST(c_ts AS TEXT)`,
			[]string{"k=2023-11-14 22:13:20|n=int64:1"}},
		{"like_over_a_timestamp",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_ts LIKE '2023-11-14%'`,
			[]string{"n=int64:104"}},
		// The text equals the literal it renders as — a value oracle's version
		// of "the two doors agree".
		{"text_equals_its_own_literal",
			`SELECT COUNT(*) AS n FROM typemx WHERE CAST(c_ts AS TEXT) = '2023-11-14 22:13:20'`,
			[]string{"n=int64:1"}},
	} {
		t.Run("#544/"+tc.name, func(t *testing.T) {
			for _, arm := range arms() {
				got, err := arm.run(tc.sql)
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				if len(got) != len(tc.want) || (len(got) > 0 && got[0] != tc.want[0]) {
					t.Errorf("%s arm: got %v, want %v (live PostgreSQL 17.11)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}

	// ---------------------------------------------------------------- #849
	// A choice construct with a FRACTIONAL literal arm, on every arm. The
	// integer rung declared an INT64 vector for it and the 1.5 the evaluator
	// produced was TRUNCATED into that vector — `LEAST(c_i64, 1.5) * 3` was 4
	// for the server's 4.5, and the same shape times int8's maximum was
	// MinInt64 (round-1 review, B3).
	//
	// The DAG arms matter here beyond the usual reason: the declared type is
	// what a stage's output schema carries, so a choice typed INT64 on one
	// side of a shuffle and DECIMAL on the other is ADR-0010's consistency
	// refusal rather than a value.
	for _, tc := range []struct {
		name, sql string
		want      []string
	}{
		{"least", `SELECT LEAST(c_i64, 1.5) AS v FROM typemx WHERE id = 1`,
			[]string{"v=1.5"}},
		{"least_plus_one", `SELECT LEAST(c_i64, 1.5) + 1 AS v FROM typemx WHERE id = 1`,
			[]string{"v=2.5"}},
		{"least_times_three", `SELECT LEAST(c_i64, 1.5) * 3 AS v FROM typemx WHERE id = 1`,
			[]string{"v=4.5"}},
		{"case_else", `SELECT (CASE WHEN id > 900 THEN c_i64 ELSE 1.5 END) + 1 AS v ` +
			`FROM typemx WHERE id = 1`, []string{"v=2.5"}},
		{"coalesce_nullif", `SELECT COALESCE(NULLIF(c_i64, 1000003), 2.5) + 1 AS v ` +
			`FROM typemx WHERE id = 1`, []string{"v=3.5"}},
		{"greatest", `SELECT GREATEST(0 - c_i64, 1.5) + 1 AS v FROM typemx WHERE id = 1`,
			[]string{"v=2.5"}},
		// The choice as a GROUP BY key, which is where the declared type
		// becomes a shuffle key rather than an output column.
		// id 0 holds c_i64 = 0, which IS the least — so the range starts at 1,
		// to keep this cell about the KEY's type rather than about which arm
		// each row picks.
		{"grouped_by_the_choice",
			`SELECT LEAST(c_i64, 1.5) AS k, COUNT(*) AS n FROM typemx WHERE id > 0 AND id < 4 ` +
				`GROUP BY LEAST(c_i64, 1.5)`, []string{"k=1.5|n=int64:3"}},
		// The BOUNDARY: a WHOLE-number literal keeps the integer domain.
		{"ctl_whole_literal", `SELECT LEAST(c_i64, 2) + 1 AS v FROM typemx WHERE id = 1`,
			[]string{"v=int64:3"}},
		{"ctl_whole_literal_case",
			`SELECT (CASE WHEN id > 0 THEN c_i64 ELSE 1 END) * 2 AS v FROM typemx WHERE id = 1`,
			[]string{"v=int64:2000006"}},
		// The values that say the KERNEL followed the declaration, not only
		// that the declaration moved (round-2 review, B1r2). Every one of
		// these is outside float64's exact integers, so a float64 computation
		// under the numeric declaration answers a DIFFERENT NUMBER rather than
		// a differently-spelled one — which the cells above, all inside 2^53,
		// could not see.
		//
		// PostgreSQL 17.11, measured:
		//   COALESCE(9007199254740993::bigint,1.5)+1        9007199254740994
		//   COALESCE(1000003::bigint,1.5)*999999999999      1000002999998999997
		//   (CASE … ELSE 1.5 END)*12345678901               12345715938036703
		//   (CASE … ELSE 1.5 END)+12345678901234567         12345678902234570
		// The trailing `.0` is ADR-0024's recorded per-value scale (#764).
		{"exact_past_2_53",
			`SELECT COALESCE(CAST(9007199254740993 AS BIGINT), 1.5) + 1 AS v ` +
				`FROM typemx WHERE id = 1`, []string{"v=9007199254740994.0"}},
		{"exact_wide_product",
			`SELECT COALESCE(c_i64, 1.5) * 999999999999 AS v FROM typemx WHERE id = 1`,
			[]string{"v=1000002999998999997.0"}},
		{"exact_wide_product_case",
			`SELECT (CASE WHEN id > 0 THEN c_i64 ELSE 1.5 END) * 12345678901 AS v ` +
				`FROM typemx WHERE id = 1`, []string{"v=12345715938036703.0"}},
		{"exact_wide_sum_case",
			`SELECT (CASE WHEN id > 0 THEN c_i64 ELSE 1.5 END) + 12345678901234567 AS v ` +
				`FROM typemx WHERE id = 1`, []string{"v=12345678902234570.0"}},
	} {
		t.Run("#849/fractional_arm/"+tc.name, func(t *testing.T) {
			for _, arm := range arms() {
				got, err := arm.run(tc.sql)
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				if len(got) != len(tc.want) || (len(got) > 0 && got[0] != tc.want[0]) {
					t.Errorf("%s arm: got %v, want %v (live PostgreSQL 17.11) — a choice with "+
						"a fractional literal arm is numeric, and an integer claim TRUNCATES "+
						"the value\n  SQL: %s", arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}

	// ---------------------------------------------------------------- #850
	// The lift REWRITES the aggregate set, and the DAG splits every aggregate
	// into a partial and a final. `SUM(x) + k*COUNT(x)` is the shape where
	// COUNT is per-partition and has to be SUMMED across partials rather than
	// re-counted — a merge that got that wrong would answer k times too many.
	// The grouped cells put the rewrite under a shuffle key as well.
	for _, tc := range []struct {
		name, sql string
		want      []string
	}{
		{"sum_plus_k", `SELECT SUM(c_i32 + 3) AS s FROM typemx WHERE id < 100`,
			[]string{"s=int64:14628"}},
		{"sum_minus_k", `SELECT SUM(c_i32 - 3) AS s FROM typemx WHERE id < 100`,
			[]string{"s=int64:14046"}},
		{"sum_k_minus_col", `SELECT SUM(3 - c_i32) AS s FROM typemx WHERE id < 100`,
			[]string{"s=int64:-14046"}},
		{"sum_times_k", `SELECT SUM(c_i32 * 2) AS s FROM typemx WHERE id < 100`,
			[]string{"s=int64:28674"}},
		{"avg_plus_k", `SELECT AVG(c_i32 + 3) AS s FROM typemx WHERE id < 100`,
			[]string{"s=150.8041"}},
		{"min_max_plus_k",
			`SELECT MIN(c_i32 + 3) AS a, MAX(c_i32 + 3) AS b FROM typemx WHERE id < 100`,
			[]string{"a=int64:3|b=int64:300"}},
		{"grouped_sum_plus_k",
			`SELECT g AS k, SUM(c_i32 + 3) AS s FROM typemx WHERE id < 10 GROUP BY g ORDER BY k`,
			[]string{"k=int32:0|s=int64:27", "k=int32:1|s=int64:33", "k=int32:2|s=int64:39",
				"k=int32:3|s=int64:12", "k=int32:4|s=int64:15", "k=int32:5|s=int64:18",
				"k=int32:6|s=int64:21"}},
		// Several aggregates over one column, which is the shape the dedup
		// collapses — and the shape a merge that mixed up two slots would
		// answer wrongly for.
		{"several_over_one_column",
			`SELECT SUM(c_i32 + 3) AS a, SUM(c_i32 + 4) AS b, SUM(c_i32 * 2) AS c, ` +
				`COUNT(*) AS n FROM typemx WHERE id < 100`,
			[]string{"a=int64:14628|b=int64:14725|c=int64:28674|n=int64:100"}},
	} {
		t.Run("#850/"+tc.name, func(t *testing.T) {
			for _, arm := range arms() {
				got, err := arm.run(tc.sql)
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				if len(got) != len(tc.want) {
					t.Errorf("%s arm: %d rows, want %d\n  got %v\n  SQL: %s",
						arm.name, len(got), len(tc.want), got, tc.sql)
					continue
				}
				for i := range got {
					if got[i] != tc.want[i] {
						t.Errorf("%s arm: row %d\n  got  %s\n  want %s (live PostgreSQL 17.11)"+
							"\n  SQL: %s", arm.name, i, got[i], tc.want[i], tc.sql)
						break
					}
				}
			}
		})
	}
	// #850's refusals, on the same arms: the lift may not move a per-row
	// 22003, and the DAG is where a lifted form would be evaluated once per
	// PARTITION rather than once per row.
	for _, tc := range []struct{ name, sql string }{
		{"sum_times_int8_max", `SELECT SUM(c_i64 * 9223372036854775807) AS v FROM typemx WHERE id = 1`},
		{"sum_plus_int8_max", `SELECT SUM(c_i64 + 9223372036854775807) AS v FROM typemx WHERE id = 3`},
		{"avg_times_int8_max", `SELECT AVG(c_i64 * 9223372036854775807) AS v FROM typemx WHERE id = 1`},
	} {
		t.Run("#850/refuses/"+tc.name, func(t *testing.T) {
			for _, arm := range arms() {
				got, err := arm.run(tc.sql)
				if err == nil {
					t.Errorf("%s arm ANSWERED %v; the lift may not move the per-row 22003 "+
						"(#841, #850)", arm.name, got)
					continue
				}
				if state := sqlerr.StateOf(err); state != "22003" {
					t.Errorf("%s arm raised SQLSTATE %s, want 22003: %v", arm.name, state, err)
				}
			}
		})
	}
}
