package coordinator

import (
	"context"
	"testing"
	"time"
)

// THE BODY-CLASS TABLE for a table-less LATERAL, enumerated ONCE: every clause
// class x whether the body READS THE OUTER ROW, one cell each on five arms.
//
// The lowering's classifier asked one question — has the body a FROM clause? —
// and that answer decided both that the projection lowering ran AND that nine
// clause classes were refused. But a table-less body that names no column is
// not correlated at all: nothing about it depends on the outer row, the
// ordinary build already produced `Project(Dual, ...)`, and the cross join with
// its one row is exactly PostgreSQL's answer. Classifying by the FROM clause
// alone took twelve such shapes from PostgreSQL's own rows to a 0A000 refusal —
// 60 cells across five arms (round-2 review, B1).
//
// So the classifier asks BOTH questions, and this table is the enumeration the
// review asked for rather than one position per round:
//
//	body reads the outer row?    disposition
//	  no                         the BASE path: `Project(Dual, ...)` cross-joined,
//	                             every clause class, exactly as before this arc
//	  yes, and a projection      LOWERED (exec.LateralOuterProject)
//	  yes, and not a projection  REFUSED 0A000 naming the class
//
// In a table-less body every column reference IS an outer reference — there is
// no relation of its own for a name to resolve to — which is what makes the
// question exact rather than a heuristic. A SUBQUERY is opaque (its references
// are its own FROM's) and the body's OWN output names are not outer columns
// (this parser resolves `ORDER BY 1` to the item's alias).
//
// Every PostgreSQL answer below was measured live on 17.11 before the code
// changed.
func TestC1EATheBodyClassTable(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up three embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := c1Arms(t, ctx)

	// The refusal, without the door's own wrapper (see c1Run).
	const refused = "ERR logical plan: a LATERAL subquery with no FROM clause is " +
		"computed as a projection over the outer row"

	c1Run(t, arms, []c1Case{
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "plain / no outer reference",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// LOWERED - a projection over the outer row
			name:   "plain / reads the outer row",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			routed: c1TableLess,
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "LIMIT / no outer reference",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v LIMIT 1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// REFUSED - the body reads the outer row and is not a projection over it
			name:   "LIMIT / reads the outer row",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v LIMIT 1) l ORDER BY 1",
			want:   refused,
			why:    "PostgreSQL answers 1,2,3; the base answered three NULLs",
			routed: map[string]string{},
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "OFFSET / no outer reference",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v OFFSET 1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=0",
			routed: c1TableLess,
		},
		{
			// REFUSED - the body reads the outer row and is not a projection over it
			name:   "OFFSET / reads the outer row",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v OFFSET 1) l ORDER BY 1",
			want:   refused,
			why:    "PostgreSQL answers zero rows; the base answered three NULLs",
			routed: map[string]string{},
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "DISTINCT / no outer reference",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT DISTINCT 7 AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// REFUSED - the body reads the outer row and is not a projection over it
			name:   "DISTINCT / reads the outer row",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT DISTINCT u.id AS v) l ORDER BY 1",
			want:   refused,
			routed: map[string]string{},
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "ORDER BY / no outer reference",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v ORDER BY 1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// REFUSED - the body reads the outer row and is not a projection over it
			name:   "ORDER BY / reads the outer row",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v ORDER BY 1) l ORDER BY 1",
			want:   refused,
			routed: map[string]string{},
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "aggregate / no outer reference",
			sql:    "SELECT l.c FROM lat_ord u, LATERAL (SELECT COUNT(*) AS c) l ORDER BY 1",
			want:   "cols=[c:INT64] rows=3 | 1 | 1 | 1",
			routed: c1TableLess,
		},
		{
			// REFUSED - the body reads the outer row and is not a projection over it
			name:   "aggregate / reads the outer row",
			sql:    "SELECT l.c FROM lat_ord u, LATERAL (SELECT COUNT(u.id) AS c) l ORDER BY 1",
			want:   refused,
			why:    "PostgreSQL refuses this one too (42803: an aggregate over the outer row at its own query level), so it is loud beside loud",
			routed: map[string]string{},
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "GROUP BY / no outer reference",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v GROUP BY 1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// REFUSED - the body reads the outer row and is not a projection over it
			name:   "GROUP BY / reads the outer row",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v GROUP BY u.id) l ORDER BY 1",
			want:   refused,
			routed: map[string]string{},
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "HAVING / no outer reference",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v HAVING 1=1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// REFUSED - the body reads the outer row and is not a projection over it
			name:   "HAVING / reads the outer row",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v HAVING u.id > 1) l ORDER BY 1",
			want:   refused,
			routed: map[string]string{},
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "window / no outer reference",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT ROW_NUMBER() OVER () AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 1 | 1",
			routed: c1TableLess,
		},
		{
			// REFUSED - the body reads the outer row and is not a projection over it
			name:   "window / reads the outer row",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT ROW_NUMBER() OVER (ORDER BY u.id) AS v) l ORDER BY 1",
			want:   refused,
			routed: map[string]string{},
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "set operation / no outer reference",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 1 AS v UNION ALL SELECT 2) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=6 | 1 | 1 | 1 | 2 | 2 | 2",
			routed: c1TableLess,
		},
		{
			// REFUSED - the body reads the outer row and is not a projection over it
			name:   "set operation / reads the outer row",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v UNION ALL SELECT 9) l ORDER BY 1",
			want:   refused,
			routed: map[string]string{},
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "WITH / no outer reference",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (WITH n AS (SELECT 5 AS x) SELECT 7 AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// REFUSED - the body reads the outer row and is not a projection over it
			name:   "WITH / reads the outer row",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (WITH n AS (SELECT 5 AS x) SELECT u.id AS v) l ORDER BY 1",
			want:   refused,
			routed: map[string]string{},
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "a subquery item / no outer reference",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT (SELECT MAX(id) FROM lat_ord) AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 3 | 3 | 3",
			routed: c1TableLess,
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name: "body WHERE / no outer reference",
			sql:  "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v WHERE 1=1) l ORDER BY 1",
			// PINNED, and it is the BASE path's own gap: a WHERE made only of
			// constants over a `Dual` reaches the filter as the column name
			// "1". Identical at bf99c56c — the body is uncorrelated, so the
			// decorrelation left the predicate as a local WHERE there too —
			// and the same for `WHERE 1=0`. Recorded as a filing candidate; the
			// class table is where it becomes visible, not what introduced it.
			want:   "ERR filter column \"1\" does not exist in the input schema",
			why:    "PostgreSQL answers 7,7,7; a constant-only WHERE over a Dual is loud at the base too",
			routed: c1TableLess,
		},
		{
			// LOWERED - a projection over the outer row
			name:   "body WHERE / reads the outer row",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v WHERE u.id > 1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=2 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "LEFT JOIN ON false / no outer reference",
			sql:    "SELECT u.id, l.v FROM lat_ord u LEFT JOIN LATERAL (SELECT 7 AS v) l ON false ORDER BY 1",
			want:   "cols=[id:INT64 v:INT64] rows=3 | 1,NULL | 2,NULL | 3,NULL",
			routed: c1TableLess,
		},
		{
			// REFUSED - the body reads the outer row and is not a projection over it
			name:   "LEFT JOIN ON false / reads the outer row",
			sql:    "SELECT u.id, l.v FROM lat_ord u LEFT JOIN LATERAL (SELECT u.id AS v) l ON false ORDER BY 1",
			want:   refused,
			why:    "PostgreSQL pads every row; a projection cannot manufacture the pad",
			routed: map[string]string{},
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "LEFT JOIN ON an outer column / no outer reference",
			sql:    "SELECT u.id, l.v FROM lat_ord u LEFT JOIN LATERAL (SELECT 7 AS v) l ON u.id > 1 ORDER BY 1",
			want:   "cols=[id:INT64 v:INT64] rows=3 | 1,NULL | 2,7 | 3,7",
			routed: c1TableLess,
		},
		{
			// REFUSED - the body reads the outer row and is not a projection over it
			name:   "LEFT JOIN ON an outer column / reads the outer row",
			sql:    "SELECT u.id, l.v FROM lat_ord u LEFT JOIN LATERAL (SELECT u.id AS v) l ON u.id > 1 ORDER BY 1",
			want:   refused,
			routed: map[string]string{},
		},
		{
			// BASE PATH - the body reads nothing, so nothing depends on the outer row
			name:   "INNER JOIN ON an outer column / no outer reference",
			sql:    "SELECT l.v FROM lat_ord u JOIN LATERAL (SELECT 7 AS v) l ON u.id > 1 ORDER BY 1",
			want:   "cols=[v:INT64] rows=2 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// LOWERED - a projection over the outer row
			name:   "INNER JOIN ON the lateral column / reads the outer row",
			sql:    "SELECT l.v FROM lat_ord u JOIN LATERAL (SELECT u.id AS v) l ON l.v > 1 ORDER BY 1",
			want:   "cols=[v:INT64] rows=2 | 2 | 3",
			routed: c1TableLess,
		},
	})
}
