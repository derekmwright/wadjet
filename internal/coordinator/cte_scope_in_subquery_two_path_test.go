package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// A block's own WITH is in scope inside that block (#684).
//
// Every door that re-parses a nested block's SQL — a derived table, a CTE
// body, a LATERAL subquery — handed the logical builder the ENCLOSING CTE
// list and never read the parsed SelectInfo's own `CTEs`. So `SELECT v FROM
// (WITH c AS (…) SELECT dx AS v FROM c) t` planned a SCAN of a table called
// `c`; since no such table exists, the single-process path answered NO ROWS
// (or NULL rows, through a set operation) where PostgreSQL answers four, and
// the stage DAG failed with "stage scan-0 has no dependencies and no
// ScanFiles" — a wrong answer on one path and an unrunnable plan on the other.
//
// The boundary claims this fix makes, each with a fixture that ATTEMPTS it
// (correctness protocol, method 10 / rule 11):
//
//   - a WITH item is not in scope inside ITS OWN body, so an inner item named
//     after a BASE TABLE reads the base table there. That is #771's hazard —
//     passing the whole list let such a CTE read itself without bound and took
//     the process down with a stack overflow — reached through the newly
//     opened door, and `base_table_shadow_reads_the_base_table` is the cell
//     that attempts it.
//
//   - the ENCLOSING scope stays visible, so an outer CTE is still nameable
//     inside a derived table that has a WITH of its own.
//
// One claim it does NOT make is pinned below: an item whose name equals an
// ENCLOSING item's does not yet shadow it.
type cteScopeCell struct {
	issue, name, sql string
	// want is the row multiset PostgreSQL 17.11 answers, measured live over
	// these fixture rows, one canonical string per row.
	want []string
	// pin, when set, says this cell DIVERGES from PostgreSQL and records what
	// wadjet answers instead. The gate then asserts the divergence: a cell
	// that starts agreeing FAILS, which is how the pin gets deleted.
	pin        []string
	pinWhy     string
	wantRoutes a2Routes
}

func cteScopeCells() []cteScopeCell {
	rep := func(n int, s string) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = s
		}
		return out
	}
	return []cteScopeCell{
		// #684's own query: a WITH inside a derived table that is a set
		// operation. setopdecjb is DECIMAL(18,4) and setopdecja DECIMAL(9,2),
		// both holding 12.75, so the union resolves to scale 4 and every row
		// is the same number at either scale.
		{issue: "#684", name: "with_inside_a_derived_set_operation_arm",
			sql: `SELECT v FROM (WITH c AS (SELECT dx FROM setopdecjb) SELECT dx AS v FROM c ` +
				`UNION ALL SELECT dx FROM setopdecja) t`,
			want: rep(8, "12.75")},
		{issue: "#684", name: "with_inside_a_plain_derived_table",
			sql:  `SELECT v FROM (WITH c AS (SELECT dx FROM setopdecjb) SELECT dx AS v FROM c) t`,
			want: rep(4, "12.75")},
		{issue: "#684", name: "intersect_sibling",
			sql: `SELECT v FROM (WITH c AS (SELECT dx FROM setopdecjb) SELECT dx AS v FROM c ` +
				`INTERSECT SELECT dx FROM setopdecja) t`,
			want: rep(1, "12.75")},
		{issue: "#684", name: "except_sibling",
			sql: `SELECT v FROM (WITH c AS (SELECT dx FROM setopdecjb) SELECT dx AS v FROM c ` +
				`EXCEPT SELECT dx FROM setopdecja) t`,
			want: nil},
		{issue: "#684", name: "with_inside_a_cte_body",
			sql:  `WITH o AS (WITH d AS (SELECT dx FROM setopdecjb) SELECT dx FROM d) SELECT dx FROM o`,
			want: rep(4, "12.75")},
		// The LATERAL door. This cell compares VALUES: the single-process path
		// declares the MAX at scale 2 here (12.75) where the DAG and
		// PostgreSQL declare scale 4 (12.7500) — the same number under two
		// declarations, and only when a CTE sits inside the LATERAL (the same
		// aggregate over a CTE with no LATERAL, and over a LATERAL with no
		// CTE, agrees on both paths). Before this fix the shape was not
		// reachable at all: the single path answered four NULL rows and the
		// DAG failed to build a stage.
		{issue: "#684", name: "with_inside_a_lateral_subquery",
			sql: `SELECT t.dx FROM setopdecja a, LATERAL (WITH c AS (SELECT dx FROM setopdecjb) ` +
				`SELECT MAX(dx) AS dx FROM c) t`,
			want: rep(4, "12.75")},

		// --- the boundary claims, attempted -------------------------------
		//
		// A WITH item is NOT in scope inside its own body: the inner `c` reads
		// the BASE TABLE of that name, so the row count is the base table's
		// filtered to two and not the CTE's own. Anything else here is either
		// a wrong answer or #771's unbounded re-entry.
		{issue: "#771", name: "base_table_shadow_reads_the_base_table",
			sql: `SELECT COUNT(*) AS n FROM (WITH setopdecja AS ` +
				`(SELECT id, dx FROM setopdecja WHERE id <= 2) SELECT dx AS dv FROM setopdecja) t`,
			want: []string{"2"}},
		// And it DOES shadow the base table for the enclosing block: three
		// rows from setopdecjb, not four from the base table it is named after.
		{issue: "#684", name: "base_table_shadow_is_what_the_block_reads",
			sql: `SELECT COUNT(*) AS n FROM (WITH setopdecja AS ` +
				`(SELECT id, dx FROM setopdecjb WHERE id <= 3) SELECT dx AS v FROM setopdecja) t`,
			want: []string{"3"}},
		// The enclosing scope stays visible from inside a block that has a
		// WITH of its own.
		{issue: "#684", name: "an_outer_cte_is_visible_beside_an_inner_one",
			sql: `WITH o AS (SELECT id, dx FROM setopdecjb WHERE id <= 3) ` +
				`SELECT COUNT(*) AS n FROM (WITH c AS (SELECT id, dx FROM o) SELECT dx AS v FROM c) t`,
			want: []string{"3"}},
		// A CTE body's own nested WITH of the SAME name resolves to the inner
		// one — the outer `c` is not in scope inside its own body at all, so
		// there is nothing for it to lose to.
		{issue: "#684", name: "nested_with_of_the_same_name_in_a_cte_body",
			sql: `WITH c AS (WITH c AS (SELECT id, dx FROM setopdecjb WHERE id <= 2) SELECT dx FROM c) ` +
				`SELECT COUNT(*) AS n FROM c`,
			want: []string{"2"}},

		// --- the claim this fix does NOT make -----------------------------
		//
		// A block's own item does not yet SHADOW an enclosing item of the same
		// name: `resolveTableOrCTE` takes the FIRST match of a flat list whose
		// enclosing entries come first. Reversing that search fixes the DAG
		// arms and NOT the single-process one — measured, both DAG arms answer
		// PostgreSQL's 2 with the search reversed and the single-process arm
		// still answers 4 — because that path materializes CTEs into
		// `Planner.cteCache` keyed by NAME over the statement's top-level
		// list, so the shadowing subtree, tagged with the same CTEName, reads
		// the enclosing item's materialization whatever the builder resolved.
		// One query answered two ways is worse than one answered wrongly the
		// same way on both, so the divergence is pinned here whole: correct
		// shadowing is that cache becoming scope-aware AND the search being
		// reversed.
		{issue: "#684", name: "an_inner_cte_does_not_yet_shadow_an_outer_one_of_the_same_name",
			sql: `WITH o AS (SELECT id, dx FROM setopdecja) SELECT COUNT(*) AS n FROM ` +
				`(WITH o AS (SELECT id, dx FROM setopdecjb WHERE id <= 2) SELECT dx AS v FROM o) t`,
			want: []string{"2"},
			pin:  []string{"4"},
			pinWhy: "the enclosing item wins the flat first-match search, and the single-process " +
				"cteCache is keyed by NAME over the statement's top-level list besides",
		},
		// The neighbouring shape that AGREES with PostgreSQL either way, and
		// must keep agreeing: an item whose body names the enclosing item it
		// would shadow reads that enclosing item — four rows — and not itself,
		// which is #771's unbounded re-entry.
		{issue: "#684", name: "a_shadowing_cte_body_reads_the_outer_item_it_shadows",
			sql: `WITH o AS (SELECT id, dx FROM setopdecja) SELECT COUNT(*) AS n FROM ` +
				`(WITH o AS (SELECT id, dx FROM o) SELECT dx AS v FROM o) t`,
			want: []string{"4"}},
	}
}

func TestAWithInsideASubqueryBlockIsInScopeThere(t *testing.T) {
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

	for _, tc := range cteScopeCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			pin := append([]string(nil), tc.pin...)
			sort.Strings(pin)
			check := func(arm string, res *oracle.Result, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("%s arm: %v\n  SQL: %s\n  PostgreSQL 17 answers %v", arm, err, tc.sql, want)
				}
				got := strings.Join(setOpCanonRows(res), " ")
				if tc.pin != nil {
					switch got {
					case strings.Join(want, " "):
						t.Errorf("%s arm now AGREES with PostgreSQL (%v), so this pin is FIXED: "+
							"delete it from cteScopeCells.\n  pinned reason: %s\n  SQL: %s",
							arm, want, tc.pinWhy, tc.sql)
					case strings.Join(pin, " "):
						// The recorded divergence, unchanged.
					default:
						t.Errorf("%s arm answers %v, which is neither PostgreSQL's %v nor the "+
							"pinned %v\n  SQL: %s", arm, got, want, pin, tc.sql)
					}
					return
				}
				if got != strings.Join(want, " ") {
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
