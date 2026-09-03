package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// `JOIN ... USING (c)` and the leading-dot numeric literal (#655).
//
// Both were 42601 on every arm — a parser defect with no path dependence, so
// the four arms add only the confirmation that there is none. They are gated
// here rather than in internal/planner/sql because what a client cares about
// is the ANSWER, and the USING desugaring is only correct if the rows come out
// right on the engines that run it.
//
// The lexer had the USING token since MERGE was added; the join-clause parser
// had an ON arm and no USING arm, so the clause fell through to the
// end-of-statement guard and was reported as trailing input. The desugaring is
// `<left>.c = <right>.c` per column, which is what USING means and needs no
// catalog.
//
// What this does NOT fix, and refuses instead of answering wrong:
//
//   - `SELECT *` over a USING join. USING merges the joined column into ONE
//     output column — three columns for two two-column tables where an ON join
//     emits four — and the star's column set over a join is not resolvable in
//     the layer that expands stars (ExpandStarProjections declines a star whose
//     source is not a lone scan, by design). Answering four would be a wrong
//     answer in kind. 0A000.
//   - A USING clause following another join on the same FROM item, where the
//     column could come from either relation on the left and picking one
//     without the catalog is a guess that changes the answer. 0A000.
//   - NATURAL JOIN, whose keys ARE the shared columns and so need the catalog
//     outright. Still refused; its class moves from 42601 to 0A000, because
//     PostgreSQL answers it and a client is owed "not implemented here".
//
// #655 stays open on those three. Every expectation below is live
// PostgreSQL 17's, measured rather than remembered.
type a2JoinCell struct {
	issue, name, sql string
	want             []string
	wantErrLike      string
	wantState        string
	pgSays           string
	// wantRoutes is the routing delta each DAG arm must show. The zero value
	// means the DAG EXECUTED this shape; anything else names the refusal it
	// took, and says that the two DAG arms are the coordinator-local pipeline
	// for this cell (rule 11).
	wantRoutes a2Routes
}

func a2JoinCells() []a2JoinCell {
	return []a2JoinCell{
		// ---- JOIN ... USING, the shapes that now answer -------------------
		{issue: "#655", name: "using_count",
			sql:  `SELECT COUNT(*) AS c FROM zzp JOIN zzj USING (id)`,
			want: []string{"c=int64:3"}, pgSays: "3"},
		{issue: "#655", name: "using_left_join",
			sql:  `SELECT COUNT(*) AS c FROM zzp LEFT JOIN zzj USING (id)`,
			want: []string{"c=int64:3"}, pgSays: "3"},
		{issue: "#655", name: "using_self_join",
			sql:  `SELECT COUNT(*) AS c FROM zzp a JOIN zzp b USING (id)`,
			want: []string{"c=int64:3"}, pgSays: "3"},
		// Qualified references still reach each side, which is PostgreSQL's
		// rule: USING merges the column for `*` and for unqualified reads,
		// and leaves both underlying columns addressable.
		{issue: "#655", name: "using_qualified_projection",
			sql: `SELECT zzp.id AS pid, zzj.d92 AS jd FROM zzp JOIN zzj USING (id) ORDER BY zzp.id`,
			want: []string{
				"pid=int64:1|jd=1.1111", "pid=int64:2|jd=12345678.1234", "pid=int64:3|jd=3.3333"},
			pgSays: "3 rows: 1.1111, 12345678.1234, 3.3333"},
		// A GROUP BY above it, so the shape reaches the DAG's aggregate stage
		// rather than only its scan — the desugared condition has to survive
		// stage generation, not just the parser.
		{issue: "#655", name: "using_grouped",
			sql: `SELECT zzp.id AS id, COUNT(*) AS n FROM zzp JOIN zzj USING (id) GROUP BY zzp.id ORDER BY zzp.id`,
			want: []string{
				"id=int64:1|n=int64:1", "id=int64:2|n=int64:1", "id=int64:3|n=int64:1"},
			pgSays: "3 rows, one per id"},
		// TWO columns, whose values differ between the two tables, so the
		// second conjunct has to be present AND correct: with only the first
		// this answers 3.
		{issue: "#655", name: "using_two_columns",
			sql:  `SELECT COUNT(*) AS c FROM zzp JOIN zzj USING (id, d92)`,
			want: []string{"c=int64:0"}, pgSays: "0 — no row agrees on both"},

		// ---- the leading-dot numeric literal ------------------------------
		//
		// The bar these cells hold is EQUIVALENCE with the zero-prefixed
		// spelling, not agreement with PostgreSQL on the literal's TYPE: a
		// bare `0.5` boxes as a float64 here and is `numeric` there, which is
		// a standing literal-typing divergence (ADR-0012) that this lexer
		// change neither causes nor fixes. The `ctl_zero_prefixed_form`
		// control below carries the same box for the same number written the
		// other way, which is what makes "the two spellings are one literal"
		// the assertion rather than a coincidence.
		// `FROM zzp WHERE id = 1` and not a bare `SELECT .5`: a table-less
		// SELECT ROUTES on #806's refusal, so its two DAG arms would be the
		// coordinator-local pipeline the `single` arm already ran — three
		// arms, one engine. The literal is unchanged by the FROM (the lexer
		// is what this cell is about), and the row keeps the answer a single
		// value. The table-less spelling is kept below with its route
		// ASSERTED, so both states are covered.
		{issue: "#655", name: "dot_literal_projection",
			sql:  `SELECT .5 AS v FROM zzp WHERE id = 1`,
			want: []string{"v=float:0.5"}, pgSays: "0.5 (numeric)"},
		{issue: "#655", name: "dot_literal_arithmetic",
			sql:  `SELECT .5 + 1 AS v FROM zzp WHERE id = 1`,
			want: []string{"v=1.5"}, pgSays: "1.5"},
		{issue: "#655", name: "dot_literal_tableless_routes",
			sql:  `SELECT .5 AS v`,
			want: []string{"v=float:0.5"}, wantRoutes: a2Routes{TableLess: 1},
			pgSays: "0.5 — and on the DAG this ROUTES (#806), so both DAG arms " +
				"here are the coordinator-local pipeline"},
		{issue: "#655", name: "dot_literal_in_where",
			sql: `SELECT COUNT(*) AS c FROM zzp WHERE d92 > .5`, want: []string{"c=int64:1"},
			pgSays: "1 — only 12.75 exceeds 0.5"},
		// The control the lexer change must not move: a qualifier dot is
		// still a dot. `.` followed by a digit is a number; `.` followed by
		// anything else is unchanged.
		{issue: "#655", name: "ctl_qualifier_dot",
			sql:  `SELECT zzp.id AS v FROM zzp ORDER BY zzp.id LIMIT 1`,
			want: []string{"v=int64:1"}, pgSays: "1"},
		{issue: "#655", name: "ctl_zero_prefixed_form_unchanged",
			sql:    `SELECT 0.5 AS v, 0.5 + 1 AS w FROM zzp WHERE id = 1`,
			want:   []string{"v=float:0.5|w=1.5"},
			pgSays: "0.5, 1.5 — byte-identical to the leading-dot spelling above"},

		// ---- the three shapes still refused, with their classes -----------
		{issue: "#655", name: "boundary_star_over_using",
			sql:         `SELECT * FROM zzp JOIN zzj USING (id) ORDER BY id`,
			wantErrLike: "USING merges the joined column into ONE output column",
			wantState:   "0A000",
			pgSays:      "PostgreSQL ANSWERS: 3 rows, THREE columns (id, zzp.d92, zzj.d92)"},
		{issue: "#655", name: "boundary_using_after_another_join",
			sql:         `SELECT COUNT(*) AS c FROM zzp a JOIN zzj b USING (id) JOIN zzj c USING (id)`,
			wantErrLike: "follows another join on the same FROM item",
			wantState:   "0A000",
			pgSays: "PostgreSQL ANSWERS 3: after the first USING there is one merged `id` " +
				"on the left, so the second resolves against it"},
		// PostgreSQL refuses the OTHER chained shape, and for the reason this
		// bound exists: `zzp a JOIN zzp b ON ... JOIN zzj d USING (id)` is
		// `common column name "id" appears more than once in left table`
		// there. So the bound is not merely conservative — it covers a case
		// PostgreSQL also declines, and one it answers. Recorded here so the
		// deferral is not read as covering more than it does.
		{issue: "#655", name: "boundary_using_after_join_ambiguous_left",
			sql: `SELECT COUNT(*) AS c FROM zzp a JOIN zzp b ON a.id = b.id ` +
				`JOIN zzj d USING (id)`,
			wantErrLike: "follows another join on the same FROM item",
			wantState:   "0A000",
			pgSays:      `PostgreSQL REFUSES: common column name "id" appears more than once in left table`},
		{issue: "#655", name: "boundary_natural_join",
			sql:         `SELECT COUNT(*) AS c FROM zzp NATURAL JOIN zzj`,
			wantErrLike: "NATURAL JOIN is not supported",
			wantState:   "0A000",
			pgSays:      "PostgreSQL ANSWERS 0: NATURAL joins on id AND d92, which no row agrees on"},
		// A qualified star names ONE side and needs no merge, so it is
		// admitted — the refusal above must be about the BARE star only.
		{issue: "#655", name: "ctl_qualified_star_over_using",
			sql:  `SELECT COUNT(*) AS c FROM (SELECT zzj.* FROM zzp JOIN zzj USING (id)) z`,
			want: []string{"c=int64:3"}, pgSays: "3"},
	}
}

func TestJoinUsingAndTheLeadingDotLiteral(t *testing.T) {
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

	for _, tc := range a2JoinCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			check := func(arm string, got []string, err error) {
				t.Helper()
				sort.Strings(got)
				if tc.wantErrLike != "" {
					if err == nil {
						t.Errorf("%s arm: answered %v, but this shape is refused here\n"+
							"  want an error containing %q\n  PostgreSQL 17: %s\n  SQL: %s",
							arm, got, tc.wantErrLike, tc.pgSays, tc.sql)
						return
					}
					if !strings.Contains(err.Error(), tc.wantErrLike) {
						t.Errorf("%s arm: error %v\n  want one containing %q\n  SQL: %s",
							arm, err, tc.wantErrLike, tc.sql)
					}
					if s := sqlerr.StateOf(err); s != tc.wantState {
						t.Errorf("%s arm: SQLSTATE %q, want %q\n  SQL: %s", arm, s, tc.wantState, tc.sql)
					}
					return
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  PostgreSQL 17: %s\n  SQL: %s", arm, err, tc.pgSays, tc.sql)
					return
				}
				if len(got) != len(want) {
					t.Errorf("%s arm: %d rows, want %d\n  got  %v\n  want %v (live PostgreSQL 17)\n  SQL: %s",
						arm, len(got), len(want), got, want, tc.sql)
					return
				}
				for i := range got {
					if got[i] != want[i] {
						t.Errorf("%s arm: row %d\n  got  %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
							arm, i, got[i], want[i], tc.sql)
						return
					}
				}
			}

			sgot, serr := na2Run(tmdRunSingle(ctx, single, tc.sql))
			check("single", sgot, serr)
			for i := 0; i < 5; i++ {
				got, err := na2Run(tmdRunSingle(ctx, spilled, tc.sql))
				check("spilled", got, err)
				if t.Failed() {
					break
				}
			}
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := a2ReadRoutes(arm.c)
				got, err := na2Run(tmdRunDAG(ctx, arm.c, tc.sql))
				check(arm.name, got, err)
				a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.c), tc.wantRoutes, tc.sql)
			}
		})
	}
}
