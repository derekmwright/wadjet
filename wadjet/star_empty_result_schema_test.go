package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// #846: `SELECT *` returning ZERO rows had no declared schema at all.
//
// #416 gave every zero-row result its columns from the PLAN, and
// TestEmptyResultDeclaresSameColumnsAsNonEmpty asserts the invariant for a
// written-out SELECT list. A bare star was the hole: logical.BuildFromSelect
// builds NO Project node for it (builder.go's `if !isStarOnly(info.Columns)`),
// so findOutputProjectionNode found nothing, declaredOutputSchema answered nil
// and the result carried neither names nor types. Through pgwire that is no
// RowDescription — psql prints nothing and pgJDBC's executeQuery throws "No
// results were returned by the query" — and `SELECT * FROM t` is how a BI tool
// opens a table.
//
// The assertion is the same invariant, over the SAME statement with a
// predicate that matches and one that matches nothing, over all 22 types: a
// client asking for the shape of a result gets one answer whether or not there
// are rows in it. It runs over the star specifically, and over every node that
// sits between a star and its scan without changing the column set.
func TestStarEmptyResultDeclaresSameColumnsAsNonEmpty(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	// One %s: the WHERE predicate. Every template's select list is a bare
	// star, which is the shape with no Project node.
	for _, tc := range []struct{ name, tmpl string }{
		{"star", "SELECT * FROM mbtypes WHERE %s"},
		{"star_qualified", "SELECT mbtypes.* FROM mbtypes WHERE %s"},
		{"star_ordered", "SELECT * FROM mbtypes WHERE %s ORDER BY id"},
		{"star_limit", "SELECT * FROM mbtypes WHERE %s LIMIT 5"},
		{"star_ordered_limit", "SELECT * FROM mbtypes WHERE %s ORDER BY id DESC LIMIT 5"},
		// A star over an aliased table: the alias must not change the names
		// the catalog publishes.
		{"star_aliased", "SELECT * FROM mbtypes m WHERE %s"},
		// UNION ALL of two star arms — setOpDeclaredOutputSchema resolves
		// each arm through the same walk, so an arm that declares nothing
		// nils the whole set operation.
		{"star_union_all", "SELECT * FROM mbtypes WHERE %s UNION ALL SELECT * FROM mbtypes WHERE %[1]s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			full, err := db.Query(ctx, fmt.Sprintf(tc.tmpl, "id < 100"))
			if err != nil {
				t.Fatalf("non-empty arm: %v", err)
			}
			if len(full.Rows) == 0 {
				t.Fatal("the non-empty arm returned no rows; the reference is meaningless")
			}
			empty, err := db.Query(ctx, fmt.Sprintf(tc.tmpl, "id < 0"))
			if err != nil {
				t.Fatalf("empty arm: %v", err)
			}
			if len(empty.Rows) != 0 {
				t.Fatalf("the empty arm returned %d rows", len(empty.Rows))
			}
			if len(empty.Columns) == 0 {
				t.Fatalf("the empty arm declared NO columns at all — through pgwire that is "+
					"no RowDescription, which psql prints as nothing and pgJDBC reports as "+
					"\"No results were returned by the query\" (#846). Full arm: %v", full.Columns)
			}
			if strings.Join(empty.Columns, ",") != strings.Join(full.Columns, ",") {
				t.Errorf("column NAMES differ:\n empty %v\n full  %v", empty.Columns, full.Columns)
			}
			if got, want := describeMetas(empty), describeMetas(full); got != want {
				t.Errorf("column TYPES differ between the empty and non-empty arms:\n"+
					" empty %s\n full  %s", got, want)
			}
		})
	}
}

// TestStarOverAGroupingDeclaresItsGroupKeys is the two shapes where a node
// BETWEEN the star and its scan narrows the columns the star publishes: an
// explicit GROUP BY over every column, and `SELECT DISTINCT *`, which
// logical.rewriteStarDistinct turns into exactly that aggregate.
//
// They are separated from the sweep above because their reference is not the
// same statement's non-empty arm over the whole table — a grouping over 22
// columns of a 3000-row fixture is a slow way to ask a question about column
// NAMES — so the fixture is narrowed to a handful of rows on both arms.
func TestStarOverAGroupingDeclaresItsGroupKeys(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	for _, tc := range []struct{ name, tmpl string }{
		{"group_by_all", "SELECT * FROM mbtypes WHERE %s GROUP BY id, g, c_str"},
		{"distinct_star", "SELECT DISTINCT * FROM mbtypes WHERE %s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			full, err := db.Query(ctx, fmt.Sprintf(tc.tmpl, "id < 4"))
			if err != nil {
				t.Fatalf("non-empty arm: %v", err)
			}
			if len(full.Rows) == 0 {
				t.Fatal("the non-empty arm returned no rows; the reference is meaningless")
			}
			empty, err := db.Query(ctx, fmt.Sprintf(tc.tmpl, "id < 0"))
			if err != nil {
				t.Fatalf("empty arm: %v", err)
			}
			if len(empty.Rows) != 0 {
				t.Fatalf("the empty arm returned %d rows", len(empty.Rows))
			}
			if len(empty.Columns) == 0 {
				t.Fatalf("the empty arm declared NO columns at all (#846). Full arm: %v", full.Columns)
			}
			if strings.Join(empty.Columns, ",") != strings.Join(full.Columns, ",") {
				t.Errorf("column NAMES differ:\n empty %v\n full  %v", empty.Columns, full.Columns)
			}
			if got, want := describeMetas(empty), describeMetas(full); got != want {
				t.Errorf("column TYPES differ between the empty and non-empty arms:\n"+
					" empty %s\n full  %s", got, want)
			}
		})
	}
}

// TestStarOverAJoinIsStillUndeclared is #846's DEFERRED cell, and it is a pin:
// it fails the day the declaration arrives, which is when it must be deleted.
//
// A star over a JOIN is not expanded at plan time — logical.
// ExpandStarProjections declines the same shape for the same reason, that its
// column set is not knowable from one scan — and the executed schema qualifies
// the right side's columns under a rule that lives in the join operator
// ("b.c0", exec/join.go's qualCol). Declaring it here would mean a second
// namer for the same column (ADR-0026), and the names it would have to
// reproduce are themselves a divergence: PostgreSQL describes `SELECT * FROM t
// a JOIN t b ON …` as c0, c1, c0, c1 — four columns, duplicate names kept by
// POSITION (#556/#557) — where wadjet answers c0, c1, b.c0, b.c1 whether or
// not there are rows.
//
// So the zero-row cell is deferred with the naming, not fixed under it. What
// the door still guarantees is that the client gets an empty RESULT SET rather
// than no result set: pgwire sends a zero-field RowDescription for this, never
// EmptyQueryResponse or NoData (see internal/server/pgwire's
// TestZeroRowSelectAlwaysSendsARowDescription).
func TestStarOverAJoinIsStillUndeclared(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	res, err := db.Query(ctx,
		"SELECT * FROM mbtypes a JOIN mbtypes b ON a.id = b.id WHERE a.id < 0")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected zero rows, got %d", len(res.Rows))
	}
	if len(res.Columns) != 0 {
		t.Fatalf("a zero-row `SELECT *` over a JOIN now declares %v.\n"+
			"That is the deferred half of #846 and it is FIXED — delete this pin, and "+
			"assert the invariant in TestStarEmptyResultDeclaresSameColumnsAsNonEmpty "+
			"instead, together with whether the names agree with PostgreSQL's.",
			res.Columns)
	}
}
