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
			// LOWERED - a ONE-ROW SORT IS THE IDENTITY, so an ORDER BY is no
			// longer a refusal class for a table-less body at all: it is
			// dropped and the item is lowered (round-3 brief, B1).
			name:   "ORDER BY / reads the outer row",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v ORDER BY 1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			routed: c1TableLess,
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

// THE PREDICATE-INPUT TABLE (round-3 brief, B1): for every clause of a
// table-less LATERAL body that can hold a TERM, what that term is made of and
// what the body therefore is.
//
// The outer-read question is answered by RESOLVING each term, not by testing it
// for being non-empty. `windowSpecReadsAColumn` read a window's PARTITION BY /
// ORDER BY terms as TEXT and counted any non-empty one as a column read, so
// `OVER (ORDER BY 1)` and `OVER (PARTITION BY 1)` — integer LITERALS — were
// "reads the outer row", and a window body is not a projection, so the shape
// was refused where the base answered PostgreSQL's own rows. A term is now
// parsed and walked for column references, and a name that resolves to the
// body's own output is not an outer read either.
//
// And a SORT term is not asked at all: a table-less body yields at most one
// row, so its ORDER BY is the identity whatever it names. `(SELECT 7 AS v ORDER
// BY u.id)` is the base's answer and PostgreSQL's; `(SELECT u.id AS v ORDER BY
// 1)` is lowered, with the sort dropped.
//
// Every PostgreSQL answer was measured live on 17.11 before the code changed.
func TestC1EBThePredicateInputTable(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up three embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := c1Arms(t, ctx)

	const refused = "ERR logical plan: a LATERAL subquery with no FROM clause is " +
		"computed as a projection over the outer row"

	c1Run(t, arms, []c1Case{
		{
			// item / predicate inputs: literal
			name:   "item_literal (literal)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// item / predicate inputs: constant expression
			name:   "item_constexpr (constant expression)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 3+4 AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// item / predicate inputs: outer name
			name:   "item_outer (outer name)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			routed: c1TableLess,
		},
		{
			// item / predicate inputs: mixed
			name:   "item_mixed (mixed)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id + 7 AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 8 | 9 | 10",
			routed: c1TableLess,
		},
		{
			// where / predicate inputs: literal
			name:   "where_literal (literal)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v WHERE true) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// where / predicate inputs: constant expression
			name:   "where_constexpr (constant expression)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v WHERE 3 > 2) l ORDER BY 1",
			want:   "ERR filter column \"3\" does not exist in the input schema",
			why:    "PostgreSQL answers 7,7,7; a constant-only WHERE over a Dual is loud at the base too (filing candidate 8)",
			routed: c1TableLess,
		},
		{
			// where / predicate inputs: outer name
			name:   "where_outer (outer name)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v WHERE u.id > 1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=2 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// where / predicate inputs: body-local value
			name:   "where_bodylocal (body-local value)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v WHERE 7 = 7) l ORDER BY 1",
			want:   "ERR filter column \"7\" does not exist in the input schema",
			why:    "the same pre-existing constant-WHERE gap",
			routed: c1TableLess,
		},
		{
			// orderby / predicate inputs: ordinal
			name:   "orderby_ordinal (ordinal)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v ORDER BY 1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// orderby / predicate inputs: literal
			name:   "orderby_literal (literal)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v ORDER BY 'x') l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			why:    "PostgreSQL REFUSES a non-integer constant in ORDER BY; answering it is a superset, unchanged from the base (ADR-0012)",
			routed: c1TableLess,
		},
		{
			// orderby / predicate inputs: body-local name
			name:   "orderby_bodylocal (body-local name)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v ORDER BY v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// orderby / predicate inputs: outer name
			name:   "orderby_outer (outer name)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v ORDER BY u.id) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			why:    "a one-row sort is the IDENTITY, so an outer name here does not make the body correlated",
			routed: c1TableLess,
		},
		{
			// orderby / predicate inputs: outer name
			name:   "orderby_outer_corr (outer name)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v ORDER BY u.id) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			why:    "the ITEM is what correlates; the sort is dropped as the identity it is",
			routed: c1TableLess,
		},
		{
			// groupby / predicate inputs: ordinal
			name:   "groupby_ordinal (ordinal)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v GROUP BY 1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// groupby / predicate inputs: body-local name
			name:   "groupby_bodylocal (body-local name)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v GROUP BY v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// groupby / predicate inputs: outer name
			name:   "groupby_outer (outer name)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v GROUP BY u.id) l ORDER BY 1",
			want:   refused,
			why:    "PostgreSQL answers 1,2,3; a GROUP BY is not a projection over the outer row",
			routed: map[string]string{},
		},
		{
			// having / predicate inputs: constant expression
			name:   "having_constexpr (constant expression)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v HAVING 1=1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// having / predicate inputs: outer name
			name:   "having_outer (outer name)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v HAVING u.id > 1) l ORDER BY 1",
			want:   refused,
			why:    "PostgreSQL answers 7,7",
			routed: map[string]string{},
		},
		{
			// winpart / predicate inputs: literal
			name:   "winpart_literal (literal)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT ROW_NUMBER() OVER (PARTITION BY 1) AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 1 | 1",
			routed: c1TableLess,
		},
		{
			// winpart / predicate inputs: constant expression
			name:   "winpart_constexpr (constant expression)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT ROW_NUMBER() OVER (PARTITION BY 2+3) AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 1 | 1",
			routed: c1TableLess,
		},
		{
			// winpart / predicate inputs: outer name
			name:   "winpart_outer (outer name)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT ROW_NUMBER() OVER (PARTITION BY u.id) AS v) l ORDER BY 1",
			want:   refused,
			why:    "PostgreSQL answers 1,1,1",
			routed: map[string]string{},
		},
		{
			// winord / predicate inputs: literal
			name:   "winord_literal (literal)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT ROW_NUMBER() OVER (ORDER BY 1) AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 1 | 1",
			routed: c1TableLess,
		},
		{
			// winord / predicate inputs: outer name
			name:   "winord_outer (outer name)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT ROW_NUMBER() OVER (ORDER BY u.id) AS v) l ORDER BY 1",
			want:   refused,
			why:    "PostgreSQL answers 1,1,1",
			routed: map[string]string{},
		},
		{
			// winnone / predicate inputs: no term
			name:   "winnone (no term)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT ROW_NUMBER() OVER () AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 1 | 1",
			routed: c1TableLess,
		},
		{
			// winframe / predicate inputs: literal frame bound
			name:   "winframe_literal (literal frame bound)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT COUNT(*) OVER (ORDER BY 1 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 1 | 1",
			routed: c1TableLess,
		},
		{
			// limit / predicate inputs: literal
			name:   "limit_literal (literal)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v LIMIT 1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// limit / predicate inputs: literal
			name:   "limit_zero (literal)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v LIMIT 0) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=0",
			routed: c1TableLess,
		},
		{
			// offset / predicate inputs: literal
			name:   "offset_literal (literal)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v OFFSET 1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=0",
			routed: c1TableLess,
		},
		{
			// limit / predicate inputs: outer name
			name:   "limit_outer (outer name)",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v LIMIT u.id) l ORDER BY 1",
			want:   "ERR expected number after LIMIT",
			why:    "PostgreSQL answers 7,7,7; this parser takes only a number after LIMIT, loud and pre-existing",
			routed: map[string]string{},
		},
	})
}
