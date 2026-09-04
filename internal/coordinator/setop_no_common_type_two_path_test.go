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

		// --- PINNED: PostgreSQL matches the pair, this engine has no
		// common CARRIER for it. The single-process path answers, the stage
		// DAG refuses. Nothing here MOVED — the refusal above deliberately
		// declines to fire on them, because doing so would have made six
		// answered shapes hard errors — and each cell fails when the split
		// closes, which is how these pins get deleted.
		{issue: "#648", name: "pin_date_union_timestamp",
			sql:        `SELECT c_date AS v FROM typemx UNION ALL SELECT c_ts FROM typemx`,
			singleRows: 10000, dagErr: `is DATE in one arm and TIMESTAMP in another`},
		{issue: "#648", name: "pin_port_union_integer",
			sql:        `SELECT c_port AS v FROM typemx UNION ALL SELECT c_i32 FROM typemx`,
			singleRows: 10000, dagErr: `is PORT in one arm and INT32 in another`},
		{issue: "#648", name: "pin_duration_union_bigint",
			sql:        `SELECT c_dur AS v FROM typemx UNION ALL SELECT c_i64 FROM typemx`,
			singleRows: 10000, dagErr: `is DURATION in one arm and INT64 in another`},
		{issue: "#648", name: "pin_ipv4_union_ipv6",
			sql:        `SELECT c_ipv4 AS v FROM typemx UNION ALL SELECT c_ipv6 FROM typemx`,
			singleRows: 10000, dagErr: `is IPV4 in one arm and IPV6 in another`},
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
					// result carries the message as text.
					if !isDAG {
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
