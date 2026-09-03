package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Every failure a client sees carries its SQLSTATE, on every path.
//
// #649: a RUNTIME task error crossed the stage DAG as a bare string.
// `ResultNotification` had no field for a class, the coordinator rebuilt the
// error with `fmt.Errorf(... %s, errMsg)` at six sites, and `stageTaskFailure`
// attached a class only for `Panicked`. So a DECIMAL overflow that is 22003 on
// the single-process path, a division by zero that is 22012 and an invalid
// network literal that is 22P02 all reached the client with NO class through
// the DAG. A PostgreSQL client branches on the class — a data error is
// retryable with different input, a syntax error is not — and it could not
// tell them apart, or tell any of them from an internal failure.
//
// PLAN-time refusals were never affected: they are raised on the coordinator
// and never cross a worker boundary. They are the controls here, and the point
// of including them is that they localize the change — a fix that started
// stamping a class onto everything would move them.
//
// The single-process arm is the REFERENCE for each shape, not a fourth
// opinion: PostgreSQL 17's own class is recorded in `pg` and asserted against
// every arm including that one, so a shape whose reference is itself wrong
// cannot make the DAG arms look right.
type a2StateCell struct {
	issue, name, sql string
	// want is the SQLSTATE every arm must carry. It is PostgreSQL 17's class
	// for the same statement, measured live.
	want string
	// runtime marks a failure raised inside a worker task rather than by the
	// planner. Those are the ones #649 is about; the others are the controls.
	runtime bool
}

func a2StateCells() []a2StateCell {
	return []a2StateCell{
		// ---- RUNTIME failures: the three classes #649 lost ----------------
		{issue: "#649", name: "decimal_overflow_22003", runtime: true,
			sql:  `SELECT CAST('1e39' AS DECIMAL(38,10)) AS v FROM typemx WHERE id < 3`,
			want: "22003"},
		{issue: "#649", name: "division_by_zero_22012", runtime: true,
			sql:  `SELECT c_i64 / (c_i64 - c_i64) AS v FROM typemx WHERE id < 3`,
			want: "22012"},
		{issue: "#649", name: "bad_cidr_literal_22P02", runtime: true,
			sql:  `SELECT COUNT(*) FROM typemx WHERE c_cidr = '192.168/16'`,
			want: "22P02"},
		{issue: "#649", name: "bad_uuid_literal_22P02", runtime: true,
			sql:  `SELECT COUNT(*) FROM typemx WHERE c_uuid = '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'`,
			want: "22P02"},
		{issue: "#649", name: "bad_uuid_literal_in_list_22P02", runtime: true,
			sql:  `SELECT COUNT(*) FROM typemx WHERE c_uuid IN ('{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}')`,
			want: "22P02"},
		{issue: "#649", name: "bad_mac_literal_22P02", runtime: true,
			sql:  `SELECT COUNT(*) FROM typemx WHERE c_mac = 'not-a-mac-at-all'`,
			want: "22P02"},
		// A runtime failure reached through a shape that puts the error in a
		// LATER stage than the scan, so the fix is not "the scan stage's
		// error path" — every stage runner shares stageTaskFailure and all
		// six had the same gap.
		{issue: "#649", name: "decimal_overflow_above_a_group_by", runtime: true,
			sql: `SELECT g, MAX(CAST('1e39' AS DECIMAL(38,10))) AS v FROM typemx ` +
				`WHERE id < 100 GROUP BY g ORDER BY g`,
			want: "22003"},

		// ---- PLAN-time controls: they already worked and must not move ----
		{issue: "#649", name: "ctl_plan_time_bad_literal_22P02",
			sql: `SELECT COUNT(*) FROM typemx WHERE c_dec = 'zzz'`, want: "22P02"},
		{issue: "#649", name: "ctl_unknown_column_42703",
			sql: `SELECT nosuchcolumn FROM typemx`, want: "42703"},
		{issue: "#649", name: "ctl_unknown_relation_42P01",
			sql: `SELECT * FROM nosuchtable`, want: "42P01"},
	}
}

func TestATaskErrorCarriesItsSQLStateOverTheDAG(t *testing.T) {
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

	for _, tc := range a2StateCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			check := func(arm string, err error) {
				t.Helper()
				if err == nil {
					t.Errorf("%s arm: answered, but PostgreSQL 17 refuses this with %s\n  SQL: %s",
						arm, tc.want, tc.sql)
					return
				}
				if got := sqlerr.StateOf(err); got != tc.want {
					kind := "a plan-time refusal"
					if tc.runtime {
						kind = "a RUNTIME task failure (#649's class)"
					}
					t.Errorf("%s arm: SQLSTATE %q, want %q — %s\n  error: %v\n  SQL: %s",
						arm, got, tc.want, kind, err, tc.sql)
				}
			}

			_, serr := tmdRunSingle(ctx, single, tc.sql)
			check("single", serr)
			for i := 0; i < 5; i++ {
				_, err := tmdRunSingle(ctx, spilled, tc.sql)
				check("spilled", err)
				if t.Failed() {
					break
				}
			}
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				_, err := tmdRunDAG(ctx, arm.c, tc.sql)
				check(arm.name, err)
			}
		})
	}
}
