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
	// dagOnly marks a shape the SINGLE path ANSWERS and only the DAG refuses
	// — the #807/#658 residual. Its class is asserted on the DAG arms alone,
	// because there is no failure on the other two to classify.
	dagOnly bool
	// wantRoutes is the routing delta each DAG arm must show. The zero value
	// means the DAG EXECUTED this shape; anything else names the refusal it
	// took, and says that the two DAG arms are the coordinator-local pipeline
	// for this cell (rule 11).
	wantRoutes a2Routes
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
		// The literals here have to be ones PostgreSQL ACTUALLY refuses. The
		// first version of these three cells used the BRACED UUID
		// `'{a0eebc99-...}'` — which PostgreSQL accepts — and passed only
		// because wadjet refused it too; #627's widening made the premise
		// visible by making the cell fail. Measured now, not assumed.
		{issue: "#649", name: "bad_uuid_literal_22P02", runtime: true,
			sql:  `SELECT COUNT(*) FROM typemx WHERE c_uuid = 'not-a-uuid'`,
			want: "22P02"},
		{issue: "#649", name: "bad_uuid_literal_in_list_22P02", runtime: true,
			sql:  `SELECT COUNT(*) FROM typemx WHERE c_uuid IN ('not-a-uuid')`,
			want: "22P02"},
		// `'zzz'` and not `'not-a-mac-at-all'`: the second is refused by the
		// single-process path and ANSWERS 0 on the DAG — a two-path
		// disposition split of #579's own class, pinned by
		// TestANetworkLiteralHasOneDispositionAtEverySite rather than
		// asserted here, because this gate is about the SQLSTATE of a
		// refusal and not about which engine refuses.
		{issue: "#649", name: "bad_mac_literal_22P02", runtime: true,
			sql:  `SELECT COUNT(*) FROM typemx WHERE c_mac = 'zzz'`,
			want: "22P02"},
		// A runtime failure reached through a shape that puts the error in a
		// LATER stage than the scan, so the fix is not "the scan stage's
		// error path" — every stage runner shares stageTaskFailure and all
		// six had the same gap.
		{issue: "#649", name: "decimal_overflow_above_a_group_by", runtime: true,
			sql: `SELECT g, MAX(CAST('1e39' AS DECIMAL(38,10))) AS v FROM typemx ` +
				`WHERE id < 100 GROUP BY g ORDER BY g`,
			want: "22003"},

		// ---- the STAGE-BUILDER failures, which had no class at all --------
		//
		// The invariant this gate states is "every failure a client sees
		// carries its SQLSTATE", and it was FALSE for the two loudest DAG
		// failures when this file was written — the #807/#658 shapes. They
		// arrived `ERR[]` while the three runtime classes above carried
		// theirs, and a ten-cell census that covers only the classes it chose
		// is not a census. 0A000, because PostgreSQL ANSWERS both queries, and
		// classified at the OPERATOR (exec.unresolvedSortKey,
		// exec.boundWindowKey, exec.unresolvedAggColumn) so both engines carry
		// it.
		//
		// The two cells that carried them are GONE, and where they went
		// matters: the planner no longer builds a plan that reaches either
		// operator with a key its input cannot resolve — the phantom scan
		// column that produced those plans is closed (#776) — so both queries
		// now ANSWER, and a cell asserting their SQLSTATE would have been a
		// gate that cannot fire. The classification is asserted at the
		// operator instead, with no plan and no query in the way:
		// `exec.TestAnUnresolvableKeyCarriesTheNotImplementedClass`. The DAG's
		// own job here — carrying a task failure's class across the wire — is
		// what the three runtime cells above still hold.

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

			if !tc.dagOnly {
				_, serr := tmdRunSingle(ctx, single, tc.sql)
				check("single", serr)
				for i := 0; i < 5; i++ {
					_, err := tmdRunSingle(ctx, spilled, tc.sql)
					check("spilled", err)
					if t.Failed() {
						break
					}
				}
			} else if _, serr := tmdRunSingle(ctx, single, tc.sql); serr != nil {
				t.Errorf("single arm: %v — this cell records a DAG-only refusal, so the "+
					"single-process path must ANSWER\n  SQL: %s", serr, tc.sql)
			}
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := a2ReadRoutes(arm.c)
				_, err := tmdRunDAG(ctx, arm.c, tc.sql)
				check(arm.name, err)
				a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.c), tc.wantRoutes, tc.sql)
			}
		})
	}
}
