package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// A set-operation arm's QUALIFIED names are the ones SQL puts in scope (#682).
//
// The arm type walk keys each relation's columns under its qualifiers as well
// as bare, so a JOIN's two sides can be told apart at two different (p,s) —
// that is #551's whole mechanism. It claimed two qualifiers SQL cannot write,
// and both of them collided with a qualifier a query legally CAN write, at
// which point joinArmDecls deleted the contested key (as it must), the arm came
// back untyped and the set operation was REFUSED for a query PostgreSQL
// answers:
//
//   - an INNER derived table's alias was stamped onto every scan below it and
//     collected for every enclosing level, so over `(SELECT … FROM (SELECT …) a)
//     s JOIN ja a` the inner `a` was keyed on the LEFT side beside the join's
//     own right-side `a`;
//   - a scan's TABLE NAME was keyed even when an ALIAS hides it, so over
//     `(SELECT … FROM jb) ja JOIN ja a` the derived table's legal `ja` met the
//     aliased base table's hidden `ja`.
//
// The fixture is zzp/zzj rather than the DECIMAL pair the issue used: both of
// those hold 12.75 at two scales, so a reference resolved through the WRONG
// side renders the same number and only the refusal is visible. zzp.d92 is
// (9,2) holding -3.50 / 0.00 / 12.75 and zzj.d92 is (18,4) holding 1.1111 /
// 12345678.1234 / 3.3333, so each cell's rows say WHICH relation answered.
type setOpQualCell struct {
	issue, name, sql string
	// want is PostgreSQL 17.11's row multiset, canonical, measured live.
	want []string
	// wantErr, when set, is a substring of the refusal every arm must give —
	// the cells where PostgreSQL refuses too.
	wantErr    string
	wantRoutes a2Routes
}

func setOpQualCells() []setOpQualCell {
	zzp := []string{"-3.5", "-3.5", "0", "0", "12.75", "12.75"}
	zzj := []string{"1.1111", "1.1111", "3.3333", "3.3333", "12345678.1234", "12345678.1234"}
	return []setOpQualCell{
		// #682's first shape: the only legal `a` is the join's RIGHT side, so
		// the rows are zzp's. An inner derived alias leaking onto the left
		// side made this a refusal.
		{issue: "#682", name: "an_inner_derived_alias_does_not_reach_the_outer_level",
			sql: `SELECT a.d92 AS v FROM (SELECT id, d92 FROM (SELECT id, d92 FROM zzj) a) s ` +
				`JOIN zzp a ON s.id = a.id UNION ALL SELECT d92 FROM zzp`,
			want: zzp},
		// #682's second shape: a derived table ALIASED with a base table's
		// name, joined to that base table under an alias. `zzp` names the
		// derived table, so the rows are zzj's.
		{issue: "#682", name: "a_derived_alias_equal_to_a_base_table_name",
			sql: `SELECT zzp.d92 AS v FROM (SELECT id, d92 FROM zzj) zzp JOIN zzp a ` +
				`ON zzp.id = a.id UNION ALL SELECT d92 FROM zzj`,
			want: zzj},

		// --- the resolutions #551 added, which must stay ------------------
		{issue: "#551", name: "ctl_a_derived_alias_resolves",
			sql: `SELECT s.d92 AS v FROM (SELECT id, d92 FROM zzj) s JOIN zzp a ON s.id = a.id ` +
				`UNION ALL SELECT d92 FROM zzj`,
			want: zzj},
		// The alias is recorded on the subtree ROOT, which for this spelling
		// is the Sort and not the Project — so the scope has to travel down
		// through it.
		{issue: "#551", name: "ctl_a_derived_alias_over_a_sort",
			sql: `SELECT s.d92 AS v FROM (SELECT id, d92 FROM zzj ORDER BY id) s JOIN zzp a ` +
				`ON s.id = a.id UNION ALL SELECT d92 FROM zzj`,
			want: zzj},
		{issue: "#551", name: "ctl_a_joins_two_sides_told_apart_left",
			sql:  `SELECT t.d92 AS v FROM zzp t JOIN zzj z ON t.id = z.id UNION ALL SELECT d92 FROM zzp`,
			want: zzp},
		{issue: "#551", name: "ctl_a_joins_two_sides_told_apart_right",
			sql:  `SELECT z.d92 AS v FROM zzp t JOIN zzj z ON t.id = z.id UNION ALL SELECT d92 FROM zzj`,
			want: zzj},
		// An UNALIASED relation still answers to its own name.
		{issue: "#682", name: "ctl_an_unaliased_tables_own_name",
			sql: `SELECT zzp.d92 AS v FROM zzp JOIN zzj z ON zzp.id = z.id ` +
				`UNION ALL SELECT d92 FROM zzp`,
			want: zzp},

		// --- the boundary, attempted -------------------------------------
		//
		// A table name BEHIND an alias is not a spelling either engine
		// executes: PostgreSQL 17.11 says "invalid reference to FROM-clause
		// entry for table zzp" and wadjet says "missing FROM-clause entry".
		// The cell is here because the fix above stops DECLARING a type for
		// it, and a declaration for a spelling that cannot run is what made
		// the two shapes at the top refuse.
		{issue: "#682", name: "boundary_a_table_name_behind_an_alias_is_refused",
			sql:     `SELECT zzp.d92 AS v FROM zzp a UNION ALL SELECT d92 FROM zzj`,
			wantErr: `missing FROM-clause entry for table "zzp"`},

		// --- the issue's own spellings ------------------------------------
		//
		// Kept as filed, over the fixture the issue used, so the reproduction
		// in #682 is executable as written. Both arms hold 12.75, so these
		// assert that the query is ANSWERED and not which side answered it;
		// the zz cells above carry that half.
		{issue: "#682", name: "as_filed_inner_alias",
			sql: `SELECT a.dx AS v FROM (SELECT id, dx FROM (SELECT id, dx FROM setopdecjb) a) s ` +
				`JOIN setopdecja a ON s.id = a.id UNION ALL SELECT dx FROM setopdecjb`,
			want: []string{"12.75", "12.75", "12.75", "12.75", "12.75", "12.75", "12.75", "12.75"}},
		{issue: "#682", name: "as_filed_derived_alias_equal_to_a_base_table",
			sql: `SELECT setopdecja.dx AS v FROM (SELECT id, dx FROM setopdecjb) setopdecja ` +
				`JOIN setopdecja a ON setopdecja.id = a.id UNION ALL SELECT dx FROM setopdecjb`,
			want: []string{"12.75", "12.75", "12.75", "12.75", "12.75", "12.75", "12.75", "12.75"}},
	}
}

func TestASetOpArmsQualifiersAreTheOnesInScope(t *testing.T) {
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

	for _, tc := range setOpQualCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			check := func(arm string, res *oracle.Result, err error) {
				t.Helper()
				if tc.wantErr != "" {
					if err == nil {
						t.Fatalf("%s arm ANSWERED a reference PostgreSQL refuses (%v)\n  SQL: %s",
							arm, setOpCanonRows(res), tc.sql)
					}
					if !strings.Contains(err.Error(), tc.wantErr) {
						t.Errorf("%s arm refused with %q, want a refusal containing %q\n  SQL: %s",
							arm, err.Error(), tc.wantErr, tc.sql)
					}
					return
				}
				if err != nil {
					t.Fatalf("%s arm: %v\n  SQL: %s\n  PostgreSQL 17 answers %v", arm, err, tc.sql, want)
				}
				if got := strings.Join(setOpCanonRows(res), " "); got != strings.Join(want, " ") {
					t.Errorf("%s arm rows\n  got  %v\n  want %v (PostgreSQL 17)\n  SQL: %s",
						arm, got, want, tc.sql)
				}
			}
			sres, serr := tmdRunSingle(ctx, single, tc.sql)
			check("single", sres, serr)
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := a2ReadRoutes(arm.c)
				dres, derr := tmdRunDAG(ctx, arm.c, tc.sql)
				a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.c), tc.wantRoutes, tc.sql)
				check(arm.name, dres, derr)
			}
		})
	}
}
