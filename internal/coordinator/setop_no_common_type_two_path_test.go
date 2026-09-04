package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A set operation whose arms have NO COMMON TYPE is refused at PLAN time, with
// PostgreSQL's SQLSTATE and wording, on every path and in either arm order
// (#648, ADR-0012 item 12).
//
// Before this, the two paths disagreed about what "nothing outside the numeric
// family widens" means. The stage DAG refused with a message of its own that
// carried no SQLSTATE; the single-process path let the arms meet at RUNTIME
// under whichever box the first arm happened to produce:
//
//	SELECT s FROM decpair UNION ALL SELECT a FROM decpair
//	  -> a STRING column holding rendered decimals, silently (PostgreSQL: 42804)
//	SELECT a FROM decpair UNION ALL SELECT s FROM decpair
//	  -> 22P02 mid-execution, on the first row of text that is not a number
//	SELECT a FROM decpair INTERSECT SELECT s FROM decpair
//	  -> one row, from comparing decimals against text as text
//
// The refusal is deliberately NARROWER than "the numeric ladder declines the
// pair". Wadjet has TypeIDs PostgreSQL does not — PORT and PROTOCOL declare
// int4 on the wire, DURATION int8, IPv4/IPv6/CIDR all inet (#834) — and DATE ∪
// TIMESTAMP resolves to timestamp there. Refusing those would claim PostgreSQL
// rejects queries it answers, and would turn six shapes the single-process
// path ANSWERS today into hard errors. They keep the disposition they have and
// are pinned at the bottom of this file.
type setOpNoTypeCell struct {
	issue, name, sql string
	// wantErr is the substring of PostgreSQL's own message every arm's refusal
	// must carry. Empty means the cell is a control that must ANSWER.
	wantErr string
	// wantRows is the row count both paths must return for a control.
	wantRows int
	// singleRows / dagErr describe a PINNED two-path split: a pair PostgreSQL
	// matches that this engine has no common carrier for, so the
	// single-process path answers and the stage DAG refuses. A cell that stops
	// splitting FAILS, which is how the pin gets deleted.
	singleRows int
	dagErr     string
	wantRoutes a2Routes
}

func setOpNoTypeCells() []setOpNoTypeCell {
	return []setOpNoTypeCell{
		// --- PostgreSQL refuses these; so do we now, everywhere ----------
		{issue: "#648", name: "numeric_then_text",
			sql:     `SELECT a FROM decpair UNION ALL SELECT s FROM decpair`,
			wantErr: `UNION types numeric and text cannot be matched`},
		{issue: "#648", name: "text_then_numeric",
			sql:     `SELECT s FROM decpair UNION ALL SELECT a FROM decpair`,
			wantErr: `UNION types text and numeric cannot be matched`},
		{issue: "#648", name: "numeric_union_distinct_text",
			sql:     `SELECT a FROM decpair UNION SELECT s FROM decpair`,
			wantErr: `UNION types numeric and text cannot be matched`},
		{issue: "#648", name: "numeric_intersect_text",
			sql:     `SELECT a FROM decpair INTERSECT SELECT s FROM decpair`,
			wantErr: `INTERSECT types numeric and text cannot be matched`},
		{issue: "#648", name: "numeric_except_text",
			sql:     `SELECT a FROM decpair EXCEPT SELECT s FROM decpair`,
			wantErr: `EXCEPT types numeric and text cannot be matched`},
		{issue: "#648", name: "bigint_then_text",
			sql:     `SELECT id FROM decpair UNION ALL SELECT s FROM decpair`,
			wantErr: `UNION types bigint and text cannot be matched`},
		{issue: "#648", name: "double_precision_then_text",
			sql:     `SELECT f FROM decpair UNION ALL SELECT s FROM decpair`,
			wantErr: `UNION types double precision and text cannot be matched`},
		{issue: "#648", name: "boolean_then_bigint",
			sql:     `SELECT c_bool AS v FROM typemx UNION ALL SELECT c_i64 FROM typemx`,
			wantErr: `UNION types boolean and bigint cannot be matched`},
		{issue: "#648", name: "uuid_then_text",
			sql:     `SELECT c_uuid AS v FROM typemx UNION ALL SELECT c_str FROM typemx`,
			wantErr: `UNION types uuid and text cannot be matched`},
		{issue: "#648", name: "timestamp_then_text",
			sql:     `SELECT c_ts AS v FROM typemx UNION ALL SELECT c_str FROM typemx`,
			wantErr: `UNION types timestamp without time zone and text cannot be matched`},
		{issue: "#648", name: "text_then_bytea",
			sql:     `SELECT c_str AS v FROM typemx UNION ALL SELECT c_bytes FROM typemx`,
			wantErr: `UNION types text and bytea cannot be matched`},

		// EVERY COLUMN is resolved before any disposition. The date/timestamp
		// pair is one this engine has no carrier for and PostgreSQL matches,
		// so it is exempt from the 42804 — and it used to abandon the check
		// for the columns to its RIGHT, which left #648's own filed symptom
		// reachable: the numeric/text pair in column 2 was never seen and the
		// single-process path failed mid-execution with 22P02 on the first row
		// of text that is not a number. The mirror, with the two columns
		// swapped, refused at plan time all along — a disposition that depends
		// on column ORDER is not a rule.
		{issue: "#648", name: "an_exempt_column_left_of_a_refusable_one",
			sql: `SELECT c_date AS d, c_dec AS v FROM typemx WHERE id < 3 ` +
				`UNION ALL SELECT c_ts, c_str FROM typemx WHERE id < 3`,
			wantErr: `UNION types numeric and text cannot be matched`},
		{issue: "#648", name: "ctl_the_same_two_columns_swapped",
			sql: `SELECT c_dec AS v, c_date AS d FROM typemx WHERE id < 3 ` +
				`UNION ALL SELECT c_str, c_ts FROM typemx WHERE id < 3`,
			wantErr: `UNION types numeric and text cannot be matched`},

		// --- the ladder itself, which must keep answering -----------------
		{issue: "#648", name: "ctl_text_union_text",
			sql:      `SELECT c_str AS v FROM typemx UNION ALL SELECT c_str FROM typemx`,
			wantRows: 10000},
		{issue: "#648", name: "ctl_integer_union_bigint",
			sql:      `SELECT c_i32 AS v FROM typemx UNION ALL SELECT c_i64 FROM typemx`,
			wantRows: 10000},
		{issue: "#648", name: "ctl_numeric_union_bigint",
			sql:      `SELECT a AS v FROM decpair UNION ALL SELECT id FROM decpair`,
			wantRows: 18},
		{issue: "#648", name: "ctl_numeric_union_double",
			sql:      `SELECT a AS v FROM decpair UNION ALL SELECT f FROM decpair`,
			wantRows: 18},
		{issue: "#648", name: "ctl_real_union_numeric",
			sql:      `SELECT r AS v FROM decpair UNION ALL SELECT a FROM decpair`,
			wantRows: 18},

		// --- the numeric CATEGORY, which PostgreSQL resolves and so do we --
		//
		// PORT and PROTOCOL declare int4 on the wire and DURATION int8 (#834),
		// so PostgreSQL sees these as ordinary integer unions and answers
		// them. The first version of this refusal EXEMPTED them from the
		// 42804 and left the stage DAG refusing; they are on the numeric
		// ladder now and every arm answers.
		{issue: "#648", name: "port_union_integer",
			sql: `SELECT c_port AS v FROM typemx UNION ALL SELECT c_i32 FROM typemx`, wantRows: 10000},
		{issue: "#648", name: "integer_union_port",
			sql: `SELECT c_i32 AS v FROM typemx UNION ALL SELECT c_port FROM typemx`, wantRows: 10000},
		{issue: "#648", name: "port_union_bigint",
			sql: `SELECT c_port AS v FROM typemx UNION ALL SELECT c_i64 FROM typemx`, wantRows: 10000},
		{issue: "#648", name: "protocol_union_integer",
			sql: `SELECT c_proto AS v FROM typemx UNION ALL SELECT c_i32 FROM typemx`, wantRows: 10000},
		{issue: "#648", name: "duration_union_bigint",
			sql: `SELECT c_dur AS v FROM typemx UNION ALL SELECT c_i64 FROM typemx`, wantRows: 10000},
		{issue: "#648", name: "duration_union_double_precision",
			sql: `SELECT c_dur AS v FROM typemx UNION ALL SELECT c_f64 FROM typemx`, wantRows: 10000},

		// --- an UNKNOWN literal takes the other arm's type -----------------
		//
		// PostgreSQL gives a quoted literal and NULL no type of their own
		// (its algorithm's steps 3 and 5), so each of these resolves to the
		// COLUMN's type and answers. Reading the literal as TEXT made all of
		// them 42804 — a right → loud move on eight idioms, measured against
		// 2d4220c9 and live 17.11.
		{issue: "#648", name: "inet_and_an_unknown_literal",
			sql:      `SELECT c_ipv4 AS v FROM typemx WHERE id < 3 UNION ALL SELECT '10.0.0.9'`,
			wantRows: 4, wantRoutes: a2Routes{TableLess: 1}},
		{issue: "#648", name: "macaddr_and_an_unknown_literal",
			sql:      `SELECT c_mac AS v FROM typemx WHERE id < 3 UNION ALL SELECT 'aa:bb:cc:00:00:01'`,
			wantRows: 4, wantRoutes: a2Routes{TableLess: 1}},
		{issue: "#648", name: "date_and_an_unknown_literal",
			sql:      `SELECT c_date AS v FROM typemx WHERE id < 3 UNION ALL SELECT '2010-01-01'`,
			wantRows: 4, wantRoutes: a2Routes{TableLess: 1}},
		{issue: "#648", name: "uuid_and_an_unknown_literal",
			sql: `SELECT c_uuid AS v FROM typemx WHERE id < 3 UNION ALL ` +
				`SELECT '00000000-0000-0000-0000-000000000000'`,
			wantRows: 4, wantRoutes: a2Routes{TableLess: 1}},
		{issue: "#648", name: "numeric_and_an_unknown_literal",
			sql:      `SELECT c_dec AS v FROM typemx WHERE id < 3 UNION ALL SELECT '0'`,
			wantRows: 4, wantRoutes: a2Routes{TableLess: 1}},
		{issue: "#648", name: "inet_and_a_null_literal",
			sql:      `SELECT c_ipv4 AS v FROM typemx WHERE id < 3 UNION ALL SELECT NULL`,
			wantRows: 4, wantRoutes: a2Routes{TableLess: 1}},
		{issue: "#648", name: "port_and_an_integer_literal",
			sql:      `SELECT c_port AS v FROM typemx WHERE id < 3 UNION ALL SELECT 443`,
			wantRows: 4, wantRoutes: a2Routes{TableLess: 1}},
		// The literal in the FIRST arm, with a FROM so the DAG plans it.
		{issue: "#648", name: "an_unknown_literal_arm_first",
			sql: `SELECT '1.5' AS v FROM decpair WHERE id = 1 UNION ALL SELECT a FROM decpair`,
			// The DAG routes this to the coordinator-local pipeline on the
			// unreachable-output refusal, which is a standing disposition for
			// a literal-only arm and not this rule's business; the counter
			// says so rather than the rows implying a second engine.
			wantRows: 10, wantRoutes: a2Routes{UnreachableOutput: 1}},
		{issue: "#648", name: "two_unknown_literal_arms_are_text",
			sql: `SELECT 'a' AS v FROM decpair WHERE id = 1 UNION ALL ` +
				`SELECT 'b' FROM decpair WHERE id = 1`,
			wantRows: 2},

		// --- PostgreSQL matches the pair and this engine has NO CARRIER ----
		//
		// DATE beside TIMESTAMP and two members of the inet family are one
		// PostgreSQL category, so they are NOT 42804 — and wadjet cannot
		// concatenate the two arms' carriers. They were ANSWERED on the
		// single-process path and the answer was CORRUPT: measured at
		// febf0435, `c_date ∪ c_ts` rendered every timestamp as
		// `-2207656-04-19` and `c_ipv4 ∪ c_ipv6` / `c_ipv4 ∪ c_cidr` rendered
		// every row of the second arm as `0.0.0.0`. Silent wrong → loud, on
		// every arm, with a message that says what is true rather than
		// claiming PostgreSQL would refuse.
		{issue: "#648", name: "date_union_timestamp_has_no_carrier",
			sql:     `SELECT c_date AS v FROM typemx UNION ALL SELECT c_ts FROM typemx`,
			wantErr: `is DATE in one arm and TIMESTAMP in another`},
		{issue: "#648", name: "timestamp_union_date_has_no_carrier",
			sql:     `SELECT c_ts AS v FROM typemx UNION ALL SELECT c_date FROM typemx`,
			wantErr: `is TIMESTAMP in one arm and DATE in another`},
		{issue: "#648", name: "ipv4_union_ipv6_has_no_carrier",
			sql:     `SELECT c_ipv4 AS v FROM typemx UNION ALL SELECT c_ipv6 FROM typemx`,
			wantErr: `is IPV4 in one arm and IPV6 in another`},
		{issue: "#648", name: "ipv4_union_cidr_has_no_carrier",
			sql:     `SELECT c_ipv4 AS v FROM typemx UNION ALL SELECT c_cidr FROM typemx`,
			wantErr: `is IPV4 in one arm and CIDR in another`},
	}
}

func TestASetOperationWithNoCommonTypeIsRefusedAtPlanTime(t *testing.T) {
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

	for _, tc := range setOpNoTypeCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			check := func(arm string, isDAG bool, res *oracle.Result, err error) {
				t.Helper()
				switch {
				case tc.dagErr != "":
					// A pinned split: the single-process path answers, the DAG
					// refuses with the carrier message.
					if isDAG {
						if err == nil {
							t.Errorf("%s arm now ANSWERS a pair the DAG had no carrier for (%d rows) — "+
								"the split is closed, delete this pin\n  SQL: %s",
								arm, len(res.Rows), tc.sql)
							return
						}
						if !strings.Contains(err.Error(), tc.dagErr) {
							t.Errorf("%s arm refused with %q, want the carrier message %q\n  SQL: %s",
								arm, err.Error(), tc.dagErr, tc.sql)
						}
						return
					}
					if err != nil {
						t.Errorf("the single-process path now REFUSES a pair it answered "+
							"(PostgreSQL answers it too): %v\n  SQL: %s", err, tc.sql)
						return
					}
					if len(res.Rows) != tc.singleRows {
						t.Errorf("the single-process path returned %d rows, want %d\n  SQL: %s",
							len(res.Rows), tc.singleRows, tc.sql)
					}
					return
				case tc.wantErr != "":
					if err == nil {
						t.Fatalf("%s arm ANSWERED %d rows for a pair PostgreSQL refuses with 42804\n"+
							"  SQL: %s", arm, len(res.Rows), tc.sql)
					}
					if !strings.Contains(err.Error(), tc.wantErr) {
						t.Errorf("%s arm refused with\n  %q\nwant a refusal containing PostgreSQL's\n  %q\n  SQL: %s",
							arm, err.Error(), tc.wantErr, tc.sql)
					}
					// The SQLSTATE travels on the single-process path, which is
					// the door the wire oracle drives; the DAG's coordinator
					// result carries the message as text. Only PostgreSQL's own
					// refusals carry 42804 — a CARRIER gap is wadjet's, and
					// saying 42804 there would claim PostgreSQL refuses a query
					// it answers.
					if !isDAG && strings.Contains(tc.wantErr, "cannot be matched") {
						if got := sqlerr.StateOf(err); got != "42804" {
							t.Errorf("the refusal carries SQLSTATE %q, want 42804 "+
								"(PostgreSQL's datatype_mismatch)\n  SQL: %s", got, tc.sql)
						}
					}
				default:
					if err != nil {
						t.Fatalf("%s arm: %v\n  SQL: %s", arm, err, tc.sql)
					}
					if len(res.Rows) != tc.wantRows {
						t.Errorf("%s arm returned %d rows, want %d\n  SQL: %s",
							arm, len(res.Rows), tc.wantRows, tc.sql)
					}
				}
			}
			sres, serr := tmdRunSingle(ctx, single, tc.sql)
			check("single", false, sres, serr)
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := a2ReadRoutes(arm.c)
				dres, derr := tmdRunDAG(ctx, arm.c, tc.sql)
				a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.c), tc.wantRoutes, tc.sql)
				check(arm.name, true, dres, derr)
			}
		})
	}
}
