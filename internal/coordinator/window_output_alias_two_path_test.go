package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// The gate for a WINDOW function's OUTPUT NAME, on both execution paths.
//
// exec.Window APPENDS its result to the input batch, and a bare window column
// used to write under the user's ALIAS. So a query whose alias happened to
// spell an input column's name handed the SELECT-list projection two columns
// of that name — and the projection resolves by NAME, which cannot tell them
// apart. `SELECT id, SUM(a) OVER () AS s FROM decpair` came back with
// decpair.s, the TEXT column, on BOTH paths and in silence; `AS a` came back
// with the window's own ARGUMENT column; and an unaliased window named nothing
// at all, which the single-process path answered NULL and the stage DAG
// dropped from the result entirely (#694).
//
// The repair is provenance: the window writes into a synthetic `__win_N` slot
// of its own — the slot the NESTED-window rewrite has used since #610, which
// is why `SUM(a) OVER () + 0 AS s` was right the whole time — and the
// projection reads THAT and publishes it under the requested name. The
// collision then cannot arise, rather than being detected and worked around.
//
// Both arms are asserted against PostgreSQL 17's answers over the same nine
// decpair rows, not against each other: this defect was identical on the two
// paths, so an arm-to-arm comparison saw nothing.
func TestWindowOutputAliasTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	// SUM(a) over the nine rows, and SUM(a * 2). decpair.s is TEXT and
	// decpair.a is DECIMAL(9,2), so reading the wrong column is visible as a
	// different TYPE as well as a different value — which is the point of
	// choosing `s` for the collision.
	const sumA = "52.99"
	const sumA2 = "105.98"

	for _, tc := range []struct {
		name string
		sql  string
		// col is the output column to read, want its value on every row.
		col  string
		want []string
	}{
		{
			// The filing's own shape. Before the fix this answered
			// "1.50","1.5","abc","10",… — decpair.s itself.
			name: "the alias spells a base column of another type",
			sql:  "SELECT id, SUM(a) OVER () AS s FROM " + dbpTable + " ORDER BY id",
			col:  "s", want: rep(sumA, 9),
		},
		{
			// Same, with an EXPRESSION argument: the two defects (#672's
			// materialization and this one) meet here, and the argument's
			// hidden slot must not be confused with the output's.
			name: "the alias spells a base column, over an expression argument",
			sql:  "SELECT id, SUM(a * 2) OVER () AS s FROM " + dbpTable + " ORDER BY id",
			col:  "s", want: rep(sumA2, 9),
		},
		{
			// The alias spells the window's OWN ARGUMENT. Reading by name
			// here answered the argument column, so every row got its own
			// input value back instead of the aggregate.
			name: "the alias spells the window's argument column",
			sql:  "SELECT id, SUM(a) OVER () AS a FROM " + dbpTable + " ORDER BY id",
			col:  "a", want: rep(sumA, 9),
		},
		{
			// The alias spells the PARTITION BY key, which the fragment also
			// carries. PostgreSQL partitions by the BASE column: row 7's
			// partition ('0') holds one row whose a is NULL, so the sum is
			// NULL and not 0.
			name: "the alias spells the PARTITION BY column",
			sql:  "SELECT id, SUM(a) OVER (PARTITION BY s) AS s FROM " + dbpTable + " ORDER BY id",
			col:  "s",
			want: []string{"12.75", "12.75", "12.75", "-0.01", "2.00", "0.00", "<nil>", "12.75", "12.75"},
		},
		{
			// A ranking function, which takes no argument at all: the defect
			// is in the OUTPUT name and so it fires here too.
			name: "ROW_NUMBER under a colliding alias",
			sql:  "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS s FROM " + dbpTable + " ORDER BY id",
			col:  "s", want: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
		},
		{
			// The colliding alias BESIDE the column it collides with. The
			// window's value and the base column both have to survive, which
			// a "replace the input column" repair would have broken.
			name: "the colliding alias beside the base column it shadows",
			sql:  "SELECT id, SUM(a) OVER () AS s, s AS orig FROM " + dbpTable + " ORDER BY id",
			col:  "s", want: rep(sumA, 9),
		},
		{
			name: "the shadowed base column is still itself",
			sql:  "SELECT id, SUM(a) OVER () AS s, s AS orig FROM " + dbpTable + " ORDER BY id",
			col:  "orig",
			want: []string{"1.50", "1.5", "abc", "10", "9", "1.500", "0", "-1", "1.5"},
		},
		{
			// A window with NO alias. Nothing named the output: the
			// projection asked for "" and the single-process path answered
			// NULL while the DAG dropped the column. The name is
			// PostgreSQL's — `sum`, not the window call's text — and the
			// first repair's text spelling was wrong twice over: no client
			// recognises `sum(a) OVER (...)`, and `ORDER BY 1` rewrites to it
			// and then cannot resolve a key with parentheses in it.
			name: "an unaliased window",
			sql:  "SELECT id, SUM(a) OVER () FROM " + dbpTable + " ORDER BY id",
			col:  "sum", want: rep(sumA, 9),
		},
		{
			name: "an unaliased ranking function",
			sql:  "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM " + dbpTable + " ORDER BY id",
			col:  "row_number", want: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
		},
		{
			// ORDER BY a POSITION over an unaliased window: the positional
			// resolver rewrites `1` to the select item's NAME, so it has to
			// make the same choice the projection does.
			name: "ORDER BY a position over an unaliased window",
			sql:  "SELECT SUM(a) OVER () FROM " + dbpTable + " ORDER BY 1",
			col:  "sum", want: rep(sumA, 9),
		},
		{
			// Two unaliased windows of DIFFERENT functions, so a namer that
			// collapsed them would be visible.
			name: "two unaliased windows of different functions",
			sql:  "SELECT id, MIN(a) OVER (), MAX(a) OVER () FROM " + dbpTable + " ORDER BY id",
			col:  "min", want: rep("-0.01", 9),
		},
		{
			name: "the second of two unaliased windows",
			sql:  "SELECT id, MIN(a) OVER (), MAX(a) OVER () FROM " + dbpTable + " ORDER BY id",
			col:  "max", want: rep("12.75", 9),
		},
		{
			// Through a DERIVED TABLE, consumed above: the collision is
			// inside the subquery and the outer reference to `s` has to find
			// the window's value, not the base column that reached the same
			// name.
			name: "the colliding alias consumed through a derived table",
			sql: "SELECT id, s FROM (SELECT id, SUM(a) OVER () AS s FROM " +
				dbpTable + ") x ORDER BY id",
			col: "s", want: rep(sumA, 9),
		},
		{
			// The same with a WHERE above it, which on the DAG puts a
			// StageProject between the window and the gather (ADR-0025).
			name: "the colliding alias filtered above a derived table",
			sql: "SELECT id, s FROM (SELECT id, SUM(a) OVER () AS s FROM " +
				dbpTable + ") x WHERE id < 4 ORDER BY id",
			col: "s", want: rep(sumA, 3),
		},
		{
			// ORDER BY the colliding alias. The sort ran on the base TEXT
			// column, so the rows came back in byte order of decpair.s.
			name: "ORDER BY the colliding alias",
			sql:  "SELECT id, SUM(a) OVER () AS s FROM " + dbpTable + " ORDER BY s, id",
			col:  "id", want: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
		},
		{
			// The CONTROL that says the repair did not simply hide the input
			// column: a non-colliding alias beside the base column, both
			// present and both right.
			name: "control, a non-colliding alias",
			sql:  "SELECT s, SUM(a) OVER () AS s2, id FROM " + dbpTable + " ORDER BY id",
			col:  "s",
			want: []string{"1.50", "1.5", "abc", "10", "9", "1.500", "0", "-1", "1.5"},
		},
		{
			name: "control, the non-colliding window output",
			sql:  "SELECT s, SUM(a) OVER () AS s2, id FROM " + dbpTable + " ORDER BY id",
			col:  "s2", want: rep(sumA, 9),
		},
		{
			// The other control: the NESTED spelling, which took the
			// synthetic route already and was right before the fix. It is
			// here so a failure localizes to the bare-window lowering.
			name: "control, the nested spelling of the colliding alias",
			sql:  "SELECT id, SUM(a) OVER () + 0 AS s FROM " + dbpTable + " ORDER BY id",
			col:  "s", want: rep(sumA, 9),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range sfcArms(ctx, single, coord) {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused %q: %v", arm.name, tc.sql, err)
				}
				if len(res.Rows) != len(tc.want) {
					t.Fatalf("%s arm returned %d rows, want %d\n  SQL: %s",
						arm.name, len(res.Rows), len(tc.want), tc.sql)
				}
				if !columnPresent(res.Columns, tc.col) {
					t.Fatalf("%s arm answered columns %v, which do not include %q — a window "+
						"output the result does not carry is #694's unaliased arm\n  SQL: %s",
						arm.name, res.Columns, tc.col, tc.sql)
				}
				for i, r := range res.Rows {
					if got := fmt.Sprintf("%v", r[tc.col]); got != tc.want[i] {
						t.Errorf("%s arm row %d: %s = %q, want %q — PostgreSQL 17 answers %q "+
							"and a window output resolved by NAME answers the input column "+
							"(#694)\n  SQL: %s",
							arm.name, i, tc.col, got, tc.want[i], tc.want[i], tc.sql)
					}
				}
			}
		})
	}
}

// TestWindowOutputAliasShuffledTwoPath is the same question asked of the DAG's
// OTHER join lowering.
//
// `BroadcastBytesOverride = 1` puts every build side through an
// exchange-repartition instead of replicating it, so the window's input arrives
// as a shuffled stream rather than as one task's scan — a different fragment,
// a different schema to resolve `__win_N` against, and the arm ADR-0018 §3
// means by "two engines". The value matrix answered identically under both
// thresholds when this was measured; this keeps that a property rather than a
// measurement.
func TestWindowOutputAliasShuffledTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range []struct {
		name string
		sql  string
		col  string
		want []string
	}{
		{
			// The window's alias spells a base column, over a JOIN whose
			// build side now shuffles.
			name: "the colliding alias over a shuffled join",
			sql: "SELECT x.id, SUM(x.a) OVER () AS s FROM " + dbpTable + " x JOIN " + dbpTable +
				" y ON x.id = y.id ORDER BY x.id",
			col: "s", want: rep("52.99", 9),
		},
		{
			// The window INSIDE the derived table, under the join — the shape
			// the shuffled lowering lost entirely. The exchange's payload
			// list carried the ALIAS while the window stage emitted the slot,
			// so the column never reached the join, and the gather's rename
			// fell back to the producer's raw columns: `[id, y.id]` for a
			// query that asked for `[id, w]`.
			name: "a window inside a derived table under a shuffled join",
			sql: "SELECT x.id, x.w FROM (SELECT id, SUM(a) OVER () AS w FROM " + dbpTable +
				") x JOIN " + dbpTable + " y ON x.id = y.id ORDER BY x.id",
			col: "w", want: rep("52.99", 9),
		},
		{
			// The ONE-column form, where the column asked for is the one that
			// disappeared: the result came back with two columns, neither of
			// them `w`.
			name: "only the window column, through a shuffled join",
			sql: "SELECT x.w FROM (SELECT id, SUM(a) OVER () AS w FROM " + dbpTable +
				") x JOIN " + dbpTable + " y ON x.id = y.id",
			col: "w", want: rep("52.99", 9),
		},
		{
			name: "a ranking function inside a derived table under a shuffled join",
			sql: "SELECT x.id, x.w FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS w FROM " +
				dbpTable + ") x JOIN " + dbpTable + " y ON x.id = y.id ORDER BY x.id",
			col: "w", want: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
		},
		{
			// The NESTED spelling, which reached the same hole by the route
			// that has always used a `__win_N` slot — so it was broken on
			// both DAG arms before this work and is the proof the hole is
			// about the SLOT, not about #694's change.
			name: "a nested window inside a derived table under a shuffled join",
			sql: "SELECT x.id, x.w FROM (SELECT id, SUM(a) OVER () + 0 AS w FROM " + dbpTable +
				") x JOIN " + dbpTable + " y ON x.id = y.id ORDER BY x.id",
			col: "w", want: rep("52.99", 9),
		},
		{
			// An aggregate over a derived column, over the same join: #702's
			// shape on the shuffle lowering.
			name: "an aggregate over a derived column, over a shuffled join",
			sql: "SELECT SUM(CASE WHEN x.s = '1.50' THEN twice ELSE 0 END) AS v FROM " +
				"(SELECT id, s, id * 2 AS twice FROM " + dbpTable + ") x JOIN " + dbpTable +
				" y ON x.id = y.id",
			col: "v", want: []string{"2"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tmdRunDAG(ctx, coord, tc.sql)
			if err != nil {
				t.Fatalf("shuffled DAG refused %q: %v", tc.sql, err)
			}
			if len(res.Rows) != len(tc.want) {
				t.Fatalf("shuffled DAG returned %d rows, want %d\n  SQL: %s",
					len(res.Rows), len(tc.want), tc.sql)
			}
			for i, r := range res.Rows {
				if got := fmt.Sprintf("%v", r[tc.col]); got != tc.want[i] {
					t.Errorf("shuffled DAG row %d: %s = %q, want %q — PostgreSQL 17 answers "+
						"%q\n  SQL: %s", i, tc.col, got, tc.want[i], tc.want[i], tc.sql)
				}
			}
		})
	}
}

func rep(v string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func columnPresent(cols []string, name string) bool {
	for _, c := range cols {
		if c == name {
			return true
		}
	}
	return false
}
