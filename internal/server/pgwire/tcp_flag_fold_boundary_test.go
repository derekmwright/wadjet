package pgwire

import (
	"context"
	"strings"
	"testing"
)

// THE BOUNDARY OF THE CONSTANT FLAG-NAME FOLD, FROM BOTH SIDES (#1018 round 5
// review, promoted round 6).
//
// A plan-time refusal is only as good as the queries it does NOT refuse: a
// false positive breaks a working query, which is the binder's standing
// contract. These are the adversarial reviewer's edge and false-positive
// probes, which it ran as LOGS; here each one carries its disposition, so a
// fold that widens or narrows fails rather than being read by a human.
//
// Every cell runs over an EMPTY input (`WHERE id < 0`) unless it says
// otherwise, because that is where a per-row refusal and a plan-time one give
// different answers.
func TestTheFlagNameFoldRefusesOnlyWhatPostgresWould(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	for _, tc := range []struct {
		name, sql, msg string
	}{
		// An ARITY is known without rows, so an empty name list is refused
		// before them for all four spellings.
		{"empty_list_has_tcp_flag", `SELECT has_tcp_flag(visits) AS v FROM users WHERE id<0`,
			"has_tcp_flag requires at least one TCP flag name"},
		{"empty_list_has_all", `SELECT tcp_flags_has_all(visits) AS v FROM users WHERE id<0`,
			"tcp_flags_has_all requires at least one TCP flag name"},
		{"empty_list_mask", `SELECT tcp_flag_mask() AS v FROM users WHERE id<0`,
			"tcp_flag_mask requires at least one TCP flag name"},
		{"empty_list_over_rows", `SELECT has_tcp_flag(visits) AS v FROM users`,
			"has_tcp_flag requires at least one TCP flag name"},
		// One bad name among good ones is the bad one.
		{"one_bad_among_good", `SELECT tcp_flags_has_all(visits,'SYN','ACKK','FIN') AS v FROM users WHERE id<0`,
			`TCP flag name "ACKK" not recognized`},
		{"mask_second_name_bad", `SELECT tcp_flag_mask('SYN','BOGUS') AS v FROM users WHERE id<0`,
			`TCP flag name "BOGUS" not recognized`},
		// PARENTHESES carry no meaning past grouping, so a parenthesized
		// literal is a literal.
		{"parenthesized_literal_is_folded", `SELECT tcp_flags_has_all(visits, ('BOGUS')) AS v FROM users WHERE id<0`,
			`TCP flag name "BOGUS" not recognized`},
		// has_tcp_flag reads exactly ONE name, so the misspelling it reports
		// is the first — the extra argument is not a name it looks at.
		{"legacy_spelling_reads_one_name", `SELECT has_tcp_flag(visits,'BOGUS','EXTRA') AS v FROM users WHERE id<0`,
			`TCP flag name "BOGUS" not recognized`},
		// The refusal does not care how the row set was emptied, or whether
		// the call sits under a NOT, or whether there is a table at all.
		{"under_a_not", `SELECT COUNT(*) AS n FROM users WHERE NOT tcp_flags_has_all(visits,'BOGUS') AND id<0`,
			`TCP flag name "BOGUS" not recognized`},
		{"where_false", `SELECT tcp_flags_has_all(visits,'BOGUS') AS v FROM users WHERE FALSE`,
			`TCP flag name "BOGUS" not recognized`},
		{"table_less", `SELECT 1 AS v WHERE tcp_flags_has_all(1,'BOGUS')`,
			`TCP flag name "BOGUS" not recognized`},
		// tcp_flags_from_string reads a comma-separated LIST, and an empty
		// ELEMENT names the position.
		{"from_string_empty_element", `SELECT tcp_flags_from_string('SYN,') AS v FROM users WHERE id<0`,
			`empty TCP flag name at position 2`},
	} {
		t.Run("refuses/"+tc.name, func(t *testing.T) {
			res := conn.ExecParams(context.Background(), tc.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err == nil {
				t.Fatalf("ANSWERED %d rows; 22023 is due\n  SQL: %s", len(res.Rows), tc.sql)
			}
			if got := pgErrCode(res.Err); got != "22023" {
				t.Errorf("SQLSTATE %s, want 22023\n  err: %v\n  SQL: %s", got, res.Err, tc.sql)
			}
			if !strings.Contains(res.Err.Error(), tc.msg) {
				t.Errorf("%q does not carry %q\n  SQL: %s", res.Err, tc.msg, tc.sql)
			}
		})
	}

	// EVERY SPELLING THE EVALUATOR ACCEPTS PER ROW STILL ANSWERS BEFORE THE
	// ROWS EXIST. The fold and the evaluator read names through the SAME
	// expr.TCPFlagMask, so surrounding space, either case, `NS` for `AE` and
	// a spaced comma list are names — and a name the query does not spell as
	// a CONSTANT is not knowable here at all and keeps the per-row refusal.
	for _, tc := range []struct {
		name, sql string
		rows      int
	}{
		{"surrounding_space_empty", `SELECT tcp_flags_has_all(visits,' SYN ') AS v FROM users WHERE id<0`, 0},
		{"surrounding_space_over_rows", `SELECT tcp_flags_has_all(visits,' SYN ') AS v FROM users`, 3},
		{"ae_is_a_name", `SELECT tcp_flags_has_all(visits,'AE') AS v FROM users WHERE id<0`, 0},
		{"ns_is_a_spelling_of_ae", `SELECT tcp_flags_has_all(visits,'NS') AS v FROM users WHERE id<0`, 0},
		{"mixed_case_names", `SELECT tcp_flag_mask('syn','Ack','AE') AS v FROM users WHERE id<0`, 0},
		{"from_string_spaced_list_over_rows", `SELECT tcp_flags_from_string('SYN , ACK') AS v FROM users`, 3},
		{"from_string_spaced_list_empty", `SELECT tcp_flags_from_string('SYN , ACK') AS v FROM users WHERE id<0`, 0},
		// The EMPTY STRING is a list of NO names and answers the mask of
		// none, which is what a telemetry column spelling "no flags" needs.
		{"from_string_empty_string_over_rows", `SELECT tcp_flags_from_string('') AS v FROM users`, 3},
		{"from_string_empty_string_empty", `SELECT tcp_flags_from_string('') AS v FROM users WHERE id<0`, 0},
		{"legacy_spelling_valid", `SELECT has_tcp_flag(visits,'SYN') AS v FROM users WHERE id<0`, 0},
		// NOT CONSTANTS: a call and a column. Both would name nothing if they
		// were folded, and both must answer.
		{"a_call_is_not_a_constant", `SELECT tcp_flags_has_all(visits, UPPER('bogus')) AS v FROM users WHERE id<0`, 0},
		{"a_column_is_not_a_constant", `SELECT tcp_flags_has_all(visits, name) AS v FROM users WHERE id<0`, 0},
	} {
		t.Run("answers/"+tc.name, func(t *testing.T) {
			res := conn.ExecParams(context.Background(), tc.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("refused a query it must answer: %v\n  SQL: %s", res.Err, tc.sql)
			}
			if len(res.Rows) != tc.rows {
				t.Errorf("got %d rows, want %d\n  SQL: %s", len(res.Rows), tc.rows, tc.sql)
			}
		})
	}
}
