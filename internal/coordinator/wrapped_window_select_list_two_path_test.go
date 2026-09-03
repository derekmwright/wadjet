package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// A SELECT LIST IS NOT ABANDONED BECAUSE ONE ITEM WRAPS A HIDDEN SLOT (#776).
//
// `attachScanSelectProjections` is what makes a fragment compute the outer
// SELECT list. A wrapped window or aggregate — `SUM(x) OVER () + 0` — cannot
// be computed there: the gather evaluates it from the `__win_N` / `__agg_N`
// slot, and the item's recorded text is the abbreviated spelling no parser
// accepts. The pass answered that by abandoning the WHOLE list, so
//
//	SELECT id, SUM(plain) OVER () + 0 AS w, plain + 1 AS s FROM wintab0
//
// left `plain + 1` computed by nobody, and `assertGatherOutputIsReachable`
// then refused the plan — `the gather renames "plain + 1" to "s" and no stage
// emits a column of that name` — for a query the DAG can run. Right by
// ROUTING, which no row assertion can see.
//
// The item is now attached as a PASS-THROUGH of the slot it reads, and the
// rest of the list is attached normally. Every cell therefore asserts
// `UnreachableOutputLocalRoutes` beside the rows: the whole move this makes is
// routed → executed, and it is invisible without the counter (rule 11).
//
// The boundary is a claim and it has fixtures on both sides. An item reading
// TWO slots takes the first in its own position and the rest ride past the end
// of the select list; an expression the walk does not fully understand
// (`syntheticSlotRefs` returning `complete == false`) keeps the old decline,
// because a pass-through naming fewer slots than the gather will read answers
// NULL. `slot_in_subquery_arg` is the shape that puts a subquery in the same
// query, and `three_slots` the widest slot count in the corpus.
type wwslCell struct {
	name string
	sql  string
	// want is the ordered answer PostgreSQL 17 gives, which is also the
	// single-process pipeline's.
	want []string
	// wantUnreach is the UnreachableOutputLocalRoutes delta each DAG arm must
	// show: 0 = the DAG planned and ran the shape.
	wantUnreach int64
	pgSays      string
}

func wwslCells() []wwslCell {
	return []wwslCell{
		{name: "wrapped_window_beside_expression",
			sql: `SELECT id, SUM(plain) OVER () + 0 AS w, plain + 1 AS s FROM wintab0 ORDER BY id`,
			want: []string{
				"id=int64:1|w=float:10000|s=int64:1001", "id=int64:2|w=float:10000|s=int64:2001",
				"id=int64:3|w=float:10000|s=int64:3001", "id=int64:4|w=float:10000|s=int64:4001"},
			pgSays: "4 rows, w=10000 on each"},
		// The wrapped item beside a STRING expression: the attachable item is
		// what the gather cannot compute for itself.
		{name: "wrapped_window_beside_string_expression",
			sql: `SELECT id, SUM(plain) OVER () + 0 AS w, 'x' || CAST(plain AS STRING) AS s ` +
				`FROM wintab0 ORDER BY id`,
			want: []string{
				"id=int64:1|w=float:10000|s=x1000", "id=int64:2|w=float:10000|s=x2000",
				"id=int64:3|w=float:10000|s=x3000", "id=int64:4|w=float:10000|s=x4000"},
			pgSays: "4 rows, s = x1000 … x4000"},
		{name: "two_wrapped_windows_beside_expression",
			sql: `SELECT id, SUM(plain) OVER () + 0 AS w1, MAX(id) OVER () + 0 AS w2, ` +
				`plain + 1 AS s FROM wintab0 ORDER BY id`,
			want: []string{
				"id=int64:1|w1=float:10000|w2=int64:4|s=int64:1001",
				"id=int64:2|w1=float:10000|w2=int64:4|s=int64:2001",
				"id=int64:3|w1=float:10000|w2=int64:4|s=int64:3001",
				"id=int64:4|w1=float:10000|w2=int64:4|s=int64:4001"},
			pgSays: "4 rows, w1=10000 w2=4"},
		// TWO slots in ONE item: the first takes the item's position and the
		// second rides past the end of the select list. Getting this wrong is
		// not a wrong answer but a PANIC — the first cut indexed the select
		// list by a spec index past its end — so the cell is here to attempt
		// the boundary, not to decorate it.
		{name: "two_slots_in_one_item",
			sql: `SELECT id, SUM(plain) OVER () + MAX(id) OVER () AS w, plain + 1 AS s ` +
				`FROM wintab0 ORDER BY id`,
			want: []string{
				"id=int64:1|w=float:10004|s=int64:1001", "id=int64:2|w=float:10004|s=int64:2001",
				"id=int64:3|w=float:10004|s=int64:3001", "id=int64:4|w=float:10004|s=int64:4001"},
			pgSays: "4 rows, w=10004"},
		{name: "two_slots_in_one_item_reversed",
			sql: `SELECT id, plain + 1 AS s, MAX(id) OVER () - SUM(plain) OVER () AS w ` +
				`FROM wintab0 ORDER BY id`,
			want: []string{
				"id=int64:1|s=int64:1001|w=float:-9996", "id=int64:2|s=int64:2001|w=float:-9996",
				"id=int64:3|s=int64:3001|w=float:-9996", "id=int64:4|s=int64:4001|w=float:-9996"},
			pgSays: "4 rows, w=-9996"},
		{name: "three_slots_in_one_item",
			sql: `SELECT id, SUM(plain) OVER () + MAX(id) OVER () + MIN(id) OVER () AS w, ` +
				`plain + 1 AS s FROM wintab0 ORDER BY id`,
			want: []string{
				"id=int64:1|w=float:10005|s=int64:1001", "id=int64:2|w=float:10005|s=int64:2001",
				"id=int64:3|w=float:10005|s=int64:3001", "id=int64:4|w=float:10005|s=int64:4001"},
			pgSays: "4 rows, w=10005"},
		// The __agg_ half of the same slot family.
		{name: "wrapped_aggregate_beside_expression",
			sql:    `SELECT MAX(c_i32) + 0 AS w, 1 + 1 AS s FROM typemx WHERE id < 5`,
			want:   []string{"w=int64:12|s=int64:2"},
			pgSays: "12 | 2"},
		{name: "wrapped_window_partitioned",
			sql: `SELECT id, SUM(plain) OVER (PARTITION BY id) + 0 AS w, plain + 1 AS s ` +
				`FROM wintab0 ORDER BY id`,
			want: []string{
				"id=int64:1|w=float:1000|s=int64:1001", "id=int64:2|w=float:2000|s=int64:2001",
				"id=int64:3|w=float:3000|s=int64:3001", "id=int64:4|w=float:4000|s=int64:4001"},
			pgSays: "4 rows, one partition each"},
		{name: "wrapped_window_over_a_string_column",
			sql: `SELECT id, SUM(id) OVER () + 0 AS w, c_str || 'z' AS s FROM typemx ` +
				`WHERE id < 5 ORDER BY id`,
			want: []string{
				"id=int64:0|w=float:10|s=s-000000z", "id=int64:1|w=float:10|s=s-000001z",
				"id=int64:2|w=float:10|s=s-000002z", "id=int64:3|w=float:10|s=s-000003z",
				"id=int64:4|w=float:10|s=s-000004z"},
			pgSays: "5 rows, s = s-00000Nz"},
		// A subquery elsewhere in the query: the walk that lists an item's
		// slots refuses to guess inside one, and this shape proves the refusal
		// is not reached by a subquery that is not IN the item.
		{name: "slot_beside_an_in_subquery",
			sql: `SELECT id, SUM(plain) OVER () + 0 AS w, plain + 1 AS s FROM wintab0 ` +
				`WHERE id IN (SELECT id FROM wintab0) ORDER BY id`,
			want: []string{
				"id=int64:1|w=float:10000|s=int64:1001", "id=int64:2|w=float:10000|s=int64:2001",
				"id=int64:3|w=float:10000|s=int64:3001", "id=int64:4|w=float:10000|s=int64:4001"},
			pgSays: "4 rows, w=10000"},
		// #776's own headline shape, and the DEFERRAL under it. The wrapped
		// window inside a DERIVED TABLE, read through a bare forward of its
		// alias: the outer item `x.w` is a plain column reference to a
		// COMPUTED alias, so the rename walk answers the name itself — "the
		// stage that evaluates it emits it under this very name" — and on the
		// DAG no stage does. The client used to get the window stage's raw
		// columns, `__win_N` included, for a query that asked for three;
		// `assertGatherOutputIsReachable` refuses the plan now and the
		// coordinator-local pipeline answers it, which is why these two cells
		// assert the counter rather than an error.
		//
		// The repair is one line of the same two-name shape a sort key over
		// this alias already takes — give the spec the DEFINITION as its Expr
		// and keep the alias as its Name, through `derivedAliasDefinition` —
		// and it was BUILT, MEASURED, and WITHDRAWN. It makes the one-level
		// shape execute and it makes `s` come back FLOAT64 where the
		// single-process arm and PostgreSQL both say bigint: the SIBLING item
		// `x.plain + 1` is typed by `sourceColDeclsThroughRenames`, which
		// bails on the WHOLE Project as soon as one of its items is computed
		// — and the alias `w` is computed — so the item falls to the float
		// rule. Right-by-routing becoming executed-with-the-wrong-wire-type is
		// a right-to-anything-else move, so it is not shipped (protocol
		// rule 11).
		//
		// What it needs first is ADR-0026 §5's walk: `namingScopeDecls`
		// descends per REFERENCE under a coverage test instead of bailing per
		// Project, and it is what typed a GROUP BY key and an aggregate
		// argument correctly through the same shape. Applying it to
		// `attachScanSelectProjections`' companion items is numeric-typing
		// territory and its own change; with it, the substitution is a
		// one-liner and #717 falls with these two.
		{name: "wrapped_window_inside_a_derived_table_routes",
			sql: `SELECT x.id AS zid, x.w AS zw, x.plain + 1 AS s ` +
				`FROM (SELECT id, plain, SUM(plain) OVER () + 0 AS w FROM wintab0) x ORDER BY x.id`,
			want: []string{
				"zid=int64:1|zw=float:10000|s=int64:1001", "zid=int64:2|zw=float:10000|s=int64:2001",
				"zid=int64:3|zw=float:10000|s=int64:3001", "zid=int64:4|zw=float:10000|s=int64:4001"},
			wantUnreach: 1,
			pgSays:      "4 rows — [zid|zw|s], which is what #776 asked for"},
		{name: "two_derived_tables_over_one_wrapped_window_routes",
			sql: `SELECT z.id AS zid, z.w AS zw, z.s AS s FROM (SELECT x.id, x.w, x.plain + 1 AS s ` +
				`FROM (SELECT id, plain, SUM(plain) OVER () + 0 AS w FROM wintab0) x) z ORDER BY z.id`,
			want: []string{
				"zid=int64:1|zw=float:10000|s=int64:1001", "zid=int64:2|zw=float:10000|s=int64:2001",
				"zid=int64:3|zw=float:10000|s=int64:3001", "zid=int64:4|zw=float:10000|s=int64:4001"},
			wantUnreach: 1,
			pgSays:      "the same 4 rows"},
		// Controls, both right before this change and required to stay right.
		// The first has no wrapper at all (the window's output IS the item),
		// the second no window; both were already executing on the DAG, and a
		// projection attached where none is needed would show as the counter
		// moving or as a narrowed result.
		{name: "ctl_unwrapped_window_beside_expression",
			sql: `SELECT id, SUM(plain) OVER () AS w, plain + 1 AS s FROM wintab0 ORDER BY id`,
			want: []string{
				"id=int64:1|w=float:10000|s=int64:1001", "id=int64:2|w=float:10000|s=int64:2001",
				"id=int64:3|w=float:10000|s=int64:3001", "id=int64:4|w=float:10000|s=int64:4001"},
			pgSays: "4 rows"},
		{name: "ctl_no_window_at_all",
			sql: `SELECT id, plain + 1 AS s FROM wintab0 ORDER BY id`,
			want: []string{
				"id=int64:1|s=int64:1001", "id=int64:2|s=int64:2001",
				"id=int64:3|s=int64:3001", "id=int64:4|s=int64:4001"},
			pgSays: "4 rows"},
		{name: "ctl_wrapped_window_alone",
			sql: `SELECT id, SUM(plain) OVER () + 0 AS w FROM wintab0 ORDER BY id`,
			want: []string{
				"id=int64:1|w=float:10000", "id=int64:2|w=float:10000",
				"id=int64:3|w=float:10000", "id=int64:4|w=float:10000"},
			pgSays: "4 rows — nothing to attach, and nothing attached"},
	}
}

func TestAWrappedWindowDoesNotAbandonTheSelectList(t *testing.T) {
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

	for _, tc := range wwslCells() {
		t.Run(tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)

			got, err := na2Run(tmdRunSingle(ctx, single, tc.sql))
			if err != nil {
				t.Fatalf("single arm: %v\n  PostgreSQL 17: %s\n  SQL: %s", err, tc.pgSays, tc.sql)
			}
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Errorf("single arm\n  got  %v\n  want %v (live PostgreSQL 17: %s)\n  SQL: %s",
					got, want, tc.pgSays, tc.sql)
			}

			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := arm.c.UnreachableOutputLocalRoutes()
				dgot, derr := na2Run(tmdRunDAG(ctx, arm.c, tc.sql))
				if derr != nil {
					t.Errorf("%s arm: %v\n  PostgreSQL 17: %s\n  SQL: %s",
						arm.name, derr, tc.pgSays, tc.sql)
				} else if strings.Join(dgot, "\n") != strings.Join(want, "\n") {
					t.Errorf("%s arm\n  got  %v\n  want %v\n  SQL: %s",
						arm.name, dgot, want, tc.sql)
				}
				if d := arm.c.UnreachableOutputLocalRoutes() - before; d != tc.wantUnreach {
					t.Errorf("%s arm: UnreachableOutputLocalRoutes moved by %d, want %d\n"+
						"  (0 = the DAG planned and RAN this shape; 1 = it refused the plan and\n"+
						"  the coordinator-local pipeline answered — the same rows either way,\n"+
						"  which is why the counter is asserted)\n  SQL: %s",
						arm.name, d, tc.wantUnreach, tc.sql)
				}
			}
		})
	}
}
