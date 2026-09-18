// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/worker"
)

// Arc EX on FIVE ARMS. Each of the five items changes what a query MEANS, and
// three of the five change it at a site whose evaluator differs by arm: a
// predicate literal is read by the vectorized kernel in one process and by the
// boxed path inside a CASE, a wrong-arity call is refused by the binder on
// every arm and by the compiler on only one, and a CAST's operand reaches the
// evaluator with the same Go box whichever arm built it.
//
// A single-process gate cannot see any of that. This one runs the same
// statement on single / single+budget / dag / dag-shuffled / dag+morsel4 and
// asserts ONE answer, which is the property; the values themselves are gated
// against PostgreSQL 17.11 in the wadjet package.
func TestArcEXAnswersTheSameOnEveryArm(t *testing.T) {
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

	arms := func() []struct {
		name string
		run  func(string) ([]string, error)
	} {
		return []struct {
			name string
			run  func(string) ([]string, error)
		}{
			{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
			{"single+budget", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, spilled, sql)) }},
			{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
			{"dag-shuffled", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
			{"dag+morsel4", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordM, sql)) }},
		}
	}

	// THE REFUSALS. Each is a statement PostgreSQL 17.11 refuses with the
	// named SQLSTATE, and each one is a statement this engine ANSWERED before
	// this arc — so an arm that still answers is the defect, and an arm that
	// refuses with a different code is the two-path class.
	for _, tc := range []struct {
		issue, name, sql, state string
	}{
		// #1141: the CAST's operand declaration, at the SELECT list and at a
		// WHERE, where the DAG's fragment compiles at task time.
		{"#1141", "cast_quoted_fraction_projected",
			`SELECT CAST('2.5' AS INTEGER) AS v FROM typemx WHERE id = 1`, "22P02"},
		{"#1141", "cast_quoted_fraction_in_a_predicate",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 = CAST('2.5' AS INTEGER)`, "22P02"},
		{"#1141", "cast_text_column_fraction",
			`SELECT CAST(c_str AS INTEGER) AS v FROM typemx WHERE id = 1`, "22P02"},
		{"#1141", "cast_quoted_fraction_in_a_group_key",
			`SELECT CAST('2.5' AS BIGINT) AS v FROM typemx GROUP BY CAST('2.5' AS BIGINT)`, "22P02"},
		// #1053: the ARITY, in the four positions #1018 showed a stage may
		// never compile — HAVING, an ORDER BY key, a set-operation arm and a
		// projection above a GROUP BY.
		{"#1053", "wrong_arity_projected",
			`SELECT UPPER('a','b') AS v FROM typemx WHERE id = 1`, "42883"},
		{"#1053", "wrong_arity_in_having",
			`SELECT g, COUNT(*) AS n FROM typemx GROUP BY g HAVING UPPER('a','b') = 'A'`, "42883"},
		{"#1053", "wrong_arity_in_order_by",
			`SELECT g FROM typemx GROUP BY g ORDER BY UPPER('a','b')`, "42883"},
		{"#1053", "wrong_arity_in_a_set_operation_arm",
			`SELECT c_str FROM typemx WHERE id = 1 UNION ALL SELECT UPPER('a','b') FROM typemx WHERE id = 2`, "42883"},
		{"#1053", "wrong_arity_above_a_group_by",
			`SELECT UPPER(MIN(c_str),'x') AS v FROM typemx GROUP BY g`, "42883"},
		{"#1053", "wrong_arity_under_an_unreachable_predicate",
			`SELECT UPPER('a','b') AS v FROM typemx WHERE id < 0`, "42883"},
		// #1056: a numeric literal in a declared-text position.
		{"#1056", "numeric_literal_in_a_text_position",
			`SELECT STARTS_WITH(c_str, 1) AS v FROM typemx WHERE id = 1`, "42883"},
		{"#1056", "numeric_literal_in_a_text_position_in_having",
			`SELECT g FROM typemx GROUP BY g HAVING REPLACE(MIN(c_str), 1, 'x') = 'y'`, "42883"},
		// #583: a BYTES operand in a text-only position.
		{"#583", "text_only_function_over_bytes",
			`SELECT UPPER(c_bytes) AS v FROM typemx WHERE id = 1`, "42883"},
		{"#583", "text_only_function_over_bytes_in_a_predicate",
			`SELECT COUNT(*) AS n FROM typemx WHERE UPPER(c_bytes) = 'X'`, "42883"},
		// #1137: a literal beside a network column that names no value of its
		// type. `'0x1bb'` is int4's grammar, not PORT's, and MATCHED before.
		{"#1137", "port_literal_in_int4s_grammar",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_port = '0x1bb'`, "22P02"},
		{"#1137", "protocol_literal_that_names_no_protocol",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_proto = 'nosuchproto'`, "22P02"},
	} {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			for _, arm := range arms() {
				got, err := arm.run(tc.sql)
				if err == nil {
					t.Errorf("%s arm ANSWERED %v; PostgreSQL 17.11 refuses this with %s, and "+
						"every other arm does. A refusal that depends on which evaluator ran "+
						"is the two-path class this gate exists for.\n  SQL: %s",
						arm.name, got, tc.state, tc.sql)
					continue
				}
				if state := sqlerr.StateOf(err); state != tc.state {
					t.Errorf("%s arm raised SQLSTATE %s, want %s\n  err: %v\n  SQL: %s",
						arm.name, state, tc.state, err, tc.sql)
				}
				if strings.Contains(err.Error(), "internal error") {
					t.Errorf("%s arm raised an INTERNAL ERROR, which tells the client nothing "+
						"about its own statement: %v\n  SQL: %s", arm.name, err, tc.sql)
				}
			}
		})
	}

	// THE ANSWERS. The other half of every refusal: the shapes that must keep
	// working, and the ones whose VALUE this arc changed.
	for _, tc := range []struct {
		issue, name, sql string
		want             []string
	}{
		{"#1137", "protocol_name_matches_the_rows_holding_it",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_proto = 'udp'`, []string{"n=int64:20"}},
		{"#1137", "protocol_name_in_an_in_list",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_proto IN ('udp','tcp')`, []string{"n=int64:40"}},
		{"#1137", "protocol_name_beside_a_column_inside_a_case",
			`SELECT COUNT(*) AS n FROM typemx WHERE CASE WHEN c_proto = 'udp' THEN true ELSE false END`,
			[]string{"n=int64:20"}},
		{"#1137", "port_number_still_reads",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_port = '1443'`, []string{"n=int64:1"}},
		{"#1141", "a_decimal_column_still_rounds",
			`SELECT CAST(c_dec AS BIGINT) AS v FROM typemx WHERE id = 1`, []string{"v=int64:1"}},
		{"#1141", "a_whole_number_string_still_converts",
			`SELECT CAST('0x1A' AS BIGINT) AS v FROM typemx WHERE id = 1`, []string{"v=int64:26"}},
		{"#1056", "concat_renders_a_number",
			`SELECT CONCAT(1, 'x') AS v FROM typemx WHERE id = 1`, []string{"v=1x"}},
		{"#583", "length_over_bytes_is_the_byte_count",
			`SELECT LENGTH(c_bytes) AS v FROM typemx WHERE id = 1`, []string{"v=int32:14"}},
		{"#583", "encode_renders_the_bytes",
			`SELECT ENCODE(c_bytes,'hex') AS v FROM typemx WHERE id = 1`, []string{"v=62797465732d3030303030312d78"}},
		{"#1053", "an_optional_trailing_argument_still_answers",
			`SELECT SUBSTR('abcdef',2) AS v FROM typemx WHERE id = 1`, []string{"v=bcdef"}},
	} {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			for _, arm := range arms() {
				got, err := arm.run(tc.sql)
				if err != nil {
					t.Errorf("%s arm: %v — this shape answers on every other arm\n  SQL: %s",
						arm.name, err, tc.sql)
					continue
				}
				if strings.Join(got, "|") != strings.Join(tc.want, "|") {
					t.Errorf("%s arm: %v, want %v\n  SQL: %s", arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}
