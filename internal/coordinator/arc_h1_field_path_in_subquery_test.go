package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A ROW FIELD PATH AS THE INNER KEY OF AN IN-SUBQUERY IS NEVER A WRONG COUNT
// — #866, four arms.
//
// `SELECT d.id FROM decpair d WHERE d.b IN (SELECT c_row.b FROM typemx_nested)`
// measured against live PostgreSQL 17 over the same rows, before this arc:
//
//	PostgreSQL 17         did = 6                  (one row)
//	single / spilled      did = 8, 9               (SILENT, and the complement
//	                                                of the truth: 8 and 9 are
//	                                                decpair's NULL-keyed rows,
//	                                                which `NULL IN (…)` must
//	                                                exclude, while the one row
//	                                                that matches is dropped)
//	dag                   no rows                  (SILENT)
//	dag-shuffled          `partitioned shuffle: key "c_row.b" not in schema`
//
// and the NOT IN twin was wrong on ALL FOUR arms: `1..7` for PostgreSQL's
// `1,2,3,4,5,7`.
//
// The decorrelation lowers the IN to a semi join whose BUILD side is the
// subquery's own plan. That plan emits the ROW column `c_row` and no column
// called `b`, so `exec.HashJoin` resolved the build key to index -1 — the
// degenerate all-rows-equal key — and every build row served as a match for
// nothing. It is the mirror of the OUTER-key case E3 closed (#769), which
// declines the lowering for the same reason.
//
// THE DISPOSITION IS THE FILING'S SECOND OPTION, and it is the one this arc
// can reach: "PostgreSQL's 6 rows on all four arms **or a loud refusal with a
// routing counter, never a wrong count**". The lowering declines, the IN stays
// a filter predicate, and the uncorrelated-subquery guard then refuses it
// loudly on every arm, naming `c_row.b`, with `CorrelatedLocalRoutes` moving
// on both DAG arms. Answering it needs the field path MATERIALIZED into the
// subquery's output under a hidden slot — ADR-0022 / ADR-0026 3a, the same
// mechanism E3 deferred for the aliased-key case — which is a plan-shape
// change and is written up in this arc's REPORT.
//
// The refusal's SENTENCE calls the reference "correlated", which it is not:
// the guard reads a qualifier that names no relation as an outer table, and it
// has no schema to tell a ROW column from one. That is the established text
// for this family — the D5 census pins the same message for a correlated
// EXISTS on a field path — and it is recorded here rather than left to be
// discovered.
func TestArcH1AFieldPathInnerKeyIsNeverAWrongCount(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	t.Run("refused-on-every-arm", func(t *testing.T) {
		for _, tc := range []struct{ name, sql string }{
			{"in", `SELECT d.id AS did FROM decpair d WHERE d.b IN ` +
				`(SELECT c_row.b FROM typemx_nested) ORDER BY d.id`},
			{"in-parenthesised", `SELECT d.id AS did FROM decpair d WHERE d.b IN ` +
				`(SELECT (c_row).b FROM typemx_nested) ORDER BY d.id`},
			{"in-aliased", `SELECT d.id AS did FROM decpair d WHERE d.b IN ` +
				`(SELECT c_row.b AS fb FROM typemx_nested) ORDER BY d.id`},
			{"in-with-an-inner-filter", `SELECT d.id AS did FROM decpair d WHERE d.b IN ` +
				`(SELECT c_row.b FROM typemx_nested WHERE id < 20) ORDER BY d.id`},
			{"not-in", `SELECT d.id AS did FROM decpair d WHERE d.b NOT IN ` +
				`(SELECT c_row.b FROM typemx_nested WHERE c_row.b IS NOT NULL) ORDER BY d.id`},
		} {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				for _, arm := range arms {
					before := int64(0)
					if arm.coord != nil {
						before = arm.coord.CorrelatedLocalRoutes()
					}
					cols, rows, err := arm.run(tc.sql)
					if err == nil {
						t.Errorf("%s arm ANSWERED %s — a field path as the inner key of an "+
							"IN-subquery has no lowering that can NAME it, and answering means "+
							"the semi join keyed on a column the build side does not carry\n"+
							"  SQL: %s", arm.name, e3Render(cols, rows), tc.sql)
						continue
					}
					if !strings.Contains(err.Error(), "c_row.b") {
						t.Errorf("%s arm refused with %q, want a refusal naming c_row.b\n  SQL: %s",
							arm.name, err.Error(), tc.sql)
					}
					if arm.coord == nil {
						continue
					}
					if got := arm.coord.CorrelatedLocalRoutes() - before; got != 1 {
						t.Errorf("%s arm: CorrelatedLocalRoutes moved by %d, want 1 — a refusal "+
							"routes, and rows alone cannot tell a route from an execution\n"+
							"  SQL: %s", arm.name, got, tc.sql)
					}
				}
			})
		}
	})

	// THE CONTROLS, and they bound the decline from both sides. An ORDINARY
	// inner key still decorrelates and answers; the OUTER-key field path
	// keeps E3's own answer; and the FIELD PATH itself is perfectly readable
	// everywhere else, which is what says the decline is about the semi
	// join's key and not about ROW columns.
	for _, tc := range []struct{ name, sql, want string }{
		{"ctl-ordinary-inner-key",
			`SELECT d.id AS did FROM decpair d WHERE d.id IN ` +
				`(SELECT id FROM typemx_nested WHERE id < 20) ORDER BY d.id`,
			`did | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9`},
		{"ctl-outer-key-field-path",
			`SELECT n.id AS nid FROM typemx_nested n WHERE c_row.b IN ` +
				`(SELECT b FROM decpair) AND n.id < 20 ORDER BY n.id`,
			`nid | 0`},
		{"ctl-the-field-path-reads-fine-on-its-own",
			`SELECT COUNT(*) AS n, MIN(c_row.b) AS mn, MAX(c_row.b) AS mx FROM typemx_nested`,
			`n,mn,mx | 5000,0,54989`},
		{"ctl-the-field-path-in-a-literal-list",
			`SELECT n.id AS nid FROM typemx_nested n WHERE c_row.b IN (0, 11, 44) ` +
				`AND n.id < 20 ORDER BY n.id`,
			`nid | 0 | 1 | 4`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				cols, rows, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm: %v\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, err, tc.want, tc.sql)
				}
				if got := e3Render(cols, rows); got != tc.want {
					t.Fatalf("%s arm: %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}
