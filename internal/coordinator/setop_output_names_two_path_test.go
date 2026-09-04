package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A set operation publishes its LEFTMOST ARM's SELECT-list names (#743).
//
// `setOpOutputNames` built a naming rule of its own — alias, else the arm's
// RENDERED EXPRESSION, lower-cased — where the rest of the planner publishes
// declaredProjectionName's (alias, else the COLUMN's own name, else the
// rendered expression, with no fold). Two differences, each of them a split
// between the two execution paths, which describe ONE query to ONE client:
//
//   - a plain reference's rendered expression is its QUALIFIED spelling, so a
//     set operation over a join published `x.id | x.w`, over a comma join
//     `clt1.c0 | clt2.c1 | clt2.c0`, over a table-qualified reference
//     `decpair.id`, and over a ROW field path `rd.d`, all on the DAG only;
//   - the fold is the LEXER's job since #731, and repeating it here could only
//     damage a name the lexer had deliberately left alone: `AS "MyId"` came
//     back `myid` from the DAG and `MyId` from the single-process engine, and
//     an unaliased expression `sum(a) over (...) + 1` against that path's
//     `sum(a) OVER (...) + 1`.
//
// `pgName` records PostgreSQL 17.11's own names, measured live. Where wadjet
// diverges it is #732 — PostgreSQL names an unaliased expression `?column?`
// and an unaliased CAST after its argument — a naming RULE and a product
// decision; what this gate holds is that the two wadjet paths agree, which is
// what a client keying a result set by column name depends on.
type setOpNameCell struct {
	issue, name, sql string
	want             []string
	pgName           []string
	wantRoutes       a2Routes
}

func setOpNameCells() []setOpNameCell {
	return []setOpNameCell{
		{issue: "#743", name: "over_a_join",
			sql: `SELECT x.id, x.w FROM (SELECT id, SUM(a) OVER () AS w FROM decpair) x ` +
				`JOIN decpair y ON x.id = y.id UNION ALL SELECT id, a FROM decpair`,
			want: []string{"id", "w"}, pgName: []string{"id", "w"}},
		{issue: "#743", name: "over_a_join_without_a_window",
			sql: `SELECT x.id, x.b FROM (SELECT id, b FROM decpair) x ` +
				`JOIN decpair y ON x.id = y.id UNION ALL SELECT id, a FROM decpair`,
			want: []string{"id", "b"}, pgName: []string{"id", "b"}},
		{issue: "#743", name: "over_a_comma_join",
			sql: `SELECT t1.c0, t2.c1, t2.c0 FROM clt1 t1, clt2 t2 WHERE t1.c0 = t2.c0 ` +
				`UNION ALL SELECT c0, c1, c0 FROM clt1`,
			want: []string{"c0", "c1", "c0"}, pgName: []string{"c0", "c1", "c0"}},
		{issue: "#743", name: "table_qualified_reference",
			sql:  `SELECT decpair.id, decpair.a FROM decpair UNION ALL SELECT id, a FROM decpair`,
			want: []string{"id", "a"}, pgName: []string{"id", "a"}},
		{issue: "#743", name: "row_field_path",
			sql:  `SELECT rd.d FROM setopdec UNION ALL SELECT e2 FROM setopdec`,
			want: []string{"d"}, pgName: []string{"d"}},
		{issue: "#743", name: "qualified_reference_in_the_SECOND_arm_only",
			sql:  `SELECT id, a FROM decpair UNION ALL SELECT y.id, y.a FROM decpair y`,
			want: []string{"id", "a"}, pgName: []string{"id", "a"}},
		{issue: "#731", name: "delimited_alias_keeps_its_bytes",
			sql:  `SELECT id AS "MyId", a FROM decpair UNION ALL SELECT id, a FROM decpair`,
			want: []string{"MyId", "a"}, pgName: []string{"MyId", "a"}},
		{issue: "#731", name: "unaliased_expression_keeps_its_spelling",
			sql:  `SELECT id, SUM(a) OVER () + 1 FROM decpair UNION ALL SELECT id, a FROM decpair`,
			want: []string{"id", "sum(a) OVER (...) + 1"}, pgName: []string{"id", "?column?"}},
		// The controls that already agreed and must keep agreeing: an alias, a
		// bare reference, a star, an unaliased window, an unaliased cast, an
		// unaliased arithmetic expression, and a NESTED set operation, whose
		// names come from the leftmost arm of the whole chain.
		{issue: "#743", name: "ctl_alias",
			sql:  `SELECT id AS k, a AS v FROM decpair UNION ALL SELECT id, a FROM decpair`,
			want: []string{"k", "v"}, pgName: []string{"k", "v"}},
		{issue: "#743", name: "ctl_star",
			sql:  `SELECT * FROM setopdecja UNION ALL SELECT * FROM setopdecja`,
			want: []string{"id", "dx"}, pgName: []string{"id", "dx"}},
		{issue: "#743", name: "ctl_unaliased_window",
			sql:  `SELECT id, SUM(a) OVER () FROM decpair UNION ALL SELECT id, a FROM decpair`,
			want: []string{"id", "sum"}, pgName: []string{"id", "sum"}},
		{issue: "#743", name: "ctl_unaliased_cast",
			sql:  `SELECT CAST(id AS BIGINT), a FROM decpair UNION ALL SELECT id, a FROM decpair`,
			want: []string{"cast(id as bigint)", "a"}, pgName: []string{"id", "a"}},
		{issue: "#743", name: "ctl_unaliased_arithmetic",
			sql:  `SELECT id + 1, a FROM decpair UNION ALL SELECT id, a FROM decpair`,
			want: []string{"id + 1", "a"}, pgName: []string{"?column?", "a"}},
		{issue: "#743", name: "ctl_union_chain_takes_the_leftmost_arms_names",
			sql: `SELECT x.id AS q FROM decpair x UNION ALL SELECT id FROM decpair ` +
				`UNION ALL SELECT id FROM decpair`,
			want: []string{"q"}, pgName: []string{"q"}},
		{issue: "#743", name: "ctl_intersect",
			sql:  `SELECT x.id, x.a FROM decpair x INTERSECT SELECT id, a FROM decpair`,
			want: []string{"id", "a"}, pgName: []string{"id", "a"}},
		{issue: "#743", name: "ctl_except",
			sql:  `SELECT x.id, x.a FROM decpair x EXCEPT SELECT id, b FROM decpair`,
			want: []string{"id", "a"}, pgName: []string{"id", "a"}},
	}
}

// Duplicate output NAMES keep their own VALUES, read POSITIONALLY.
//
// The names gate above compares column LISTS, and every row comparator in this
// package reads a row through a map keyed by name — which holds ONE column of a
// duplicated name, so neither can see the values of a result whose subject is
// duplicate names. `wadjet.QueryResult.Cells` is the positional form and this
// is the only reader of it here: `SELECT id AS n, a AS n …` publishes two
// columns called `n`, and they carry decpair's id and its DECIMAL a, not one of
// them twice (#556/#557, slot identity by POSITION).
//
// The stage DAG REFUSES this shape — the two arms' shuffle files collide on the
// column name — and that split is pinned in TestKnownSetOperationTwoPathSplits
// with its mechanism.
func TestDuplicateSetOperationOutputNamesKeepTheirOwnValues(t *testing.T) {
	if testing.Short() {
		t.Skip("-short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	db := tmdStandalone(t, ctx)

	const sql = `SELECT id AS n, a AS n FROM decpair WHERE id = 1 ` +
		`UNION ALL SELECT id, b FROM decpair WHERE id = 1`
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("%v\n  SQL: %s", err, sql)
	}
	if strings.Join(res.Columns, ",") != "n,n" {
		t.Fatalf("columns %v, want two columns called n", res.Columns)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("%d rows, want 2", len(res.Rows))
	}
	// PostgreSQL 17.11 answers (1, 12.75) and (1, 12.7500) — its numeric is
	// unconstrained, so each arm keeps its own scale — where wadjet renders
	// both at the union's resolved scale 4. The VALUES are what this compares,
	// so what it asserts is that column 1 is the id and column 2 the DECIMAL,
	// and not one of them twice.
	for i, want := range [][2]string{{"1", "12.75"}, {"1", "12.75"}} {
		cells := res.Cells(i)
		if len(cells) != 2 {
			t.Fatalf("row %d has %d cells, want 2 — the positional form is what makes a "+
				"duplicate name readable at all", i, len(cells))
		}
		got := [2]string{setOpCanonValue(cells[0]), setOpCanonValue(cells[1])}
		if got != want {
			t.Errorf("row %d is %v, want %v — the two columns share a NAME and must not "+
				"share a VALUE", i, got, want)
		}
	}
}

func TestASetOperationPublishesItsLeftmostArmsNames(t *testing.T) {
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

	for _, tc := range setOpNameCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			names := func(arm string, cols []string, err error) []string {
				t.Helper()
				if err != nil {
					t.Fatalf("%s arm: %v\n  SQL: %s", arm, err, tc.sql)
				}
				if strings.Join(cols, "\x00") != strings.Join(tc.want, "\x00") {
					t.Errorf("%s arm names\n  got  %q\n  want %q\n  PostgreSQL 17 sends %q\n  SQL: %s",
						arm, cols, tc.want, tc.pgName, tc.sql)
				}
				return cols
			}
			sres, serr := tmdRunSingle(ctx, single, tc.sql)
			var s []string
			if serr == nil {
				s = names("single", sres.Columns, nil)
			} else {
				names("single", nil, serr)
			}
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := a2ReadRoutes(arm.c)
				dres, derr := tmdRunDAG(ctx, arm.c, tc.sql)
				a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.c), tc.wantRoutes, tc.sql)
				var d []string
				if derr == nil {
					d = names(arm.name, dres.Columns, nil)
				} else {
					names(arm.name, nil, derr)
				}
				// Asserted against the SINGLE arm too, so a change that moves
				// both paths in step is caught by `want` and one that moves
				// only one is caught here even if `want` were updated
				// carelessly.
				if strings.Join(d, "\x00") != strings.Join(s, "\x00") {
					t.Errorf("the two wadjet paths describe one set operation differently\n"+
						"  single       %q\n  %-12s %q\n  SQL: %s", s, arm.name, d, tc.sql)
				}
			}
		})
	}
}
