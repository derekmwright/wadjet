package coordinator

import (
	"context"
	"testing"
	"time"
)

// A SET OPERATION'S ARMS SUPPLY THE OPERATION'S RESULT COLUMNS — #961, four
// arms, every answer measured on live postgres:17-alpine.
//
// `UNION`, `INTERSECT` and `EXCEPT` match their arms BY POSITION over the
// operation's whole result row: the result column list is the FIRST arm's,
// every arm is projected onto it, and for every spelling but `UNION ALL` that
// whole row is also the DEDUP KEY. Nothing above the operation can therefore
// say an arm may stop producing a column.
//
// `logical.pushColumnNeeds` had no set-op arm, so an outer need fell through
// the generic recursion straight into both arms. Two star arms then read one
// column each while the union stage's arm projection — built from the arms'
// DECLARED output lists, which is what the operation publishes — still asked
// for the table's whole list, and both DAG arms failed with `column "g" does
// not exist in the input schema`. On the single-process path there is no
// name-based arm projection to fail, and the narrowing landed on the DEDUP
// KEY instead: `INTERSECT`, `EXCEPT` and a distinct `UNION` over two star arms
// answered 0 where PostgreSQL answers 2, 1 and 3.
//
// THE BOUNDARY IS A CLAIM: an arm with an explicit SELECT list is a `Project`,
// which builds its own needs set from its own items and is narrowed exactly as
// before — the controls below hold it, so the widening is scoped to the STAR
// arm, which has no `Project` at all.
func TestM1ASetOperationsArmsSupplyItsResultColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	f1Run(t, arms, []f1Case{
		{
			// #961's exact shape, over the 5000-row type-matrix table so the
			// DAG really fans the arms across tasks. The outer filter names
			// `id`; `g` is the SECOND catalog column and the first one the
			// narrowed arm stream could not supply, which is why the message
			// named it.
			name: "961 a UNION ALL of two star arms inside a subquery",
			sql: `SELECT (SELECT COUNT(*) FROM (SELECT * FROM typemx WHERE id < 2000 ` +
				`UNION ALL SELECT * FROM typemx WHERE id >= 2000) t WHERE id < 10) AS n ` +
				`FROM decpair WHERE id < 2`,
			want: "cols=[n:INT64] rows=1 | 10",
		},
		{
			// The same without the correlated wrapper: the defect is the set
			// operation's arms, not the subquery.
			name: "961 the same as a plain derived table",
			sql: `SELECT COUNT(*) AS n FROM (SELECT * FROM typemx WHERE id < 2000 ` +
				`UNION ALL SELECT * FROM typemx WHERE id >= 2000) t WHERE id < 10`,
			want: "cols=[n:INT64] rows=1 | 10",
		},
		{
			// A DEDUPLICATING union. Its key is the whole result row, so a
			// narrowed arm is a wrong ROW COUNT and not only a missing column:
			// the single-process arms answered 0 here for PostgreSQL's 30.
			name: "961 a distinct UNION of two overlapping star arms",
			sql: `SELECT COUNT(*) AS n FROM (SELECT * FROM typemx WHERE id < 20 ` +
				`UNION SELECT * FROM typemx WHERE id < 30) t`,
			want: "cols=[n:INT64] rows=1 | 30",
		},
		{
			// INTERSECT — the same key, from the other side. 0 → 20.
			name: "961 an INTERSECT of two star arms",
			sql: `SELECT COUNT(*) AS n FROM (SELECT * FROM typemx WHERE id < 20 ` +
				`INTERSECT SELECT * FROM typemx WHERE id < 30) t`,
			want: "cols=[n:INT64] rows=1 | 20",
		},
		{
			// EXCEPT. 0 → 10.
			name: "961 an EXCEPT of two star arms",
			sql: `SELECT COUNT(*) AS n FROM (SELECT * FROM typemx WHERE id < 30 ` +
				`EXCEPT SELECT * FROM typemx WHERE id < 20) t`,
			want: "cols=[n:INT64] rows=1 | 10",
		},
		{
			// A DIFFERENT table, so the fix is not a property of typemx's
			// column list, and a filter on a column the arms publish under a
			// name the outer query names.
			name: "961 a star UNION ALL filtered on a second column",
			sql: `SELECT COUNT(*) AS n FROM (SELECT * FROM typemx_dim ` +
				`UNION ALL SELECT * FROM typemx_dim) u WHERE k < 3`,
			want: "cols=[n:INT64] rows=1 | 6",
		},
		{
			// The set operation's own columns reach the CLIENT: a star over
			// the union publishes the arms' declared list, in the first arm's
			// order. PostgreSQL publishes `id, customer, total`.
			name: "961 a star over a UNION ALL of two star arms",
			sql: "SELECT * FROM (SELECT * FROM lat_ord WHERE id < 2 " +
				"UNION ALL SELECT * FROM lat_ord WHERE id >= 2) u ORDER BY id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64] rows=3 | " +
				"1,Alice,150 | 2,Bob,200 | 3,Carol,0",
		},
		{
			// A filter on a column NEITHER the outer query nor the arms'
			// predicates name: `product` is needed only by the outer WHERE,
			// so the arm read set has to carry it through the union.
			name: "961 a filter above the union on a column no arm predicate names",
			sql: "SELECT COUNT(*) AS n FROM (SELECT * FROM lat_item WHERE id < 3 " +
				"UNION ALL SELECT * FROM lat_item WHERE id >= 3) t WHERE product = 'Widget'",
			want: "cols=[n:INT64] rows=1 | 2",
		},
		{
			// An aggregate over a column the union publishes.
			name: "961 an aggregate over a UNION ALL of two star arms",
			sql: "SELECT SUM(amount) AS s FROM (SELECT * FROM lat_item WHERE id < 3 " +
				"UNION ALL SELECT * FROM lat_item WHERE id >= 3) t",
			want: "cols=[s:FLOAT64] rows=1 | 350",
		},
		{
			// CONTROL: EXPLICIT select lists on both arms. Each arm is a
			// Project, which builds its own needs set, and the narrowing that
			// the star arms lost stays exactly as it was — a one-column arm
			// reads one column. Right at base and unmoved.
			name: "961 control: explicit arm lists are narrowed as before",
			sql: `SELECT COUNT(*) AS n FROM (SELECT id FROM typemx WHERE id < 2000 ` +
				`UNION ALL SELECT id FROM typemx WHERE id >= 2000) t WHERE id < 10`,
			want: "cols=[n:INT64] rows=1 | 10",
		},
		{
			// CONTROL: a deduplicating union of explicit one-column arms. Its
			// dedup key really IS one column, so this is the shape a narrowed
			// arm would have been right for; it must not change.
			name: "961 control: a distinct UNION of explicit one-column arms",
			sql: `SELECT COUNT(*) AS n FROM (SELECT id FROM typemx WHERE id < 20 ` +
				`UNION SELECT id FROM typemx WHERE id < 30) t`,
			want: "cols=[n:INT64] rows=1 | 30",
		},
		{
			// CONTROL: no set operation at all. A derived star that is
			// filtered and counted keeps every narrowing it had.
			name: "961 control: a derived star with no set operation is narrowed as before",
			sql:  `SELECT COUNT(*) AS n FROM (SELECT * FROM typemx) t WHERE id < 10`,
			want: "cols=[n:INT64] rows=1 | 10",
		},
	})
}
