// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// These RC cells require the complete closure or a statement error.
// They compare depth, seed types, iteration errors and refused recursive
// forms on single-process and limited-memory paths with PostgreSQL 17.11.
// The iteration and working-table limits must fail without a partial result.
// See ADR-0021 §1o-b.
func TestArcRCRecursiveCTEAnswersItsWholeClosureOrFails(t *testing.T) {
	type cell struct {
		name, sql string
		// want is the rendered answer, or "ERR <sqlstate>" optionally
		// followed by a substring of the message.
		want string
		// spilledOnly runs the cell on the budgeted arm alone: a recursion
		// whose rows grow is refused by the budget, and on an unbudgeted
		// engine the budget is most of the machine.
		spilledOnly bool
	}
	cells := []cell{
		// ---- DEPTH (#1246)
		{name: "depth 5", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5) SELECT count(*), max(n) FROM r",
			want: "count:INT64,max:INT32 => 5,5"},
		{name: "depth 1000", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<1000) SELECT count(*), max(n) FROM r",
			want: "count:INT64,max:INT32 => 1000,1000"},
		{name: "depth 1001", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<1001) SELECT count(*), max(n) FROM r",
			want: "count:INT64,max:INT32 => 1001,1001"},
		{name: "depth 1002, one past the old cap", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<1002) SELECT count(*), max(n) FROM r",
			want: "count:INT64,max:INT32 => 1002,1002"},
		{name: "depth 5000, the issue's repro", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5000) SELECT COUNT(*), MAX(n) FROM r",
			want: "count:INT64,max:INT32 => 5000,5000"},
		{name: "depth 100000", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<100000) SELECT count(*), max(n), sum(n) FROM r",
			want: "count:INT64,max:INT32,sum:INT64 => 100000,100000,5000050000"},
		{name: "depth 5000 with a growing string", sql: "WITH RECURSIVE r(n, s) AS (SELECT 1, 'x' UNION ALL SELECT n+1, s || 'y' FROM r WHERE n<5000) SELECT count(*), max(n), max(length(s)) FROM r",
			want: "count:INT64,max:INT32,max:INT32 => 5000,5000,5000"},
		{name: "depth 1500 with a three-row fan-out", sql: "WITH RECURSIVE r(n, k) AS (SELECT 1, id FROM lat_ord UNION ALL SELECT n+1, k FROM r WHERE n<1500) SELECT count(*) AS c, max(n) AS m, max(k) AS d FROM r",
			want: "c:INT64,m:INT32,d:INT64 => 4500,1500,3"},
		{name: "depth 5000 nested in a derived table", sql: "SELECT count(*) FROM (WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5000) SELECT n FROM r) q",
			want: "count:INT64 => 5000"},
		{name: "a five-year date series", sql: "WITH RECURSIVE d(x) AS (SELECT DATE '2020-01-01' UNION ALL SELECT (x + 1) FROM d WHERE x < DATE '2024-12-31') SELECT count(*), min(x), max(x) FROM d",
			want: "count:INT64,min:DATE,max:DATE => 1827,2020-01-01,2024-12-31"},
		{name: "a 1200-step walk that joins a table every step", sql: "WITH RECURSIVE r(n, k) AS (SELECT 1, 1::bigint UNION ALL SELECT r.n+1, o.id FROM r JOIN lat_ord o ON o.id = r.k WHERE r.n < 1200) SELECT count(*), max(n) FROM r",
			want: "count:INT64,max:INT32 => 1200,1200"},
		// An expression join is a CROSS join, whose build cannot spill and
		// must fit the budget: every iteration builds one, and the base held
		// each iteration's build until the statement ended, so at 512 KiB the
		// walk was refused a few thousand steps in.
		{name: "a 20000-step walk joining a table on an expression", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT r.n+1 FROM r JOIN lat_ord o ON o.id = r.n % 3 + 1 WHERE r.n < 20000) SELECT count(*), max(n) FROM r",
			want: "count:INT64,max:INT32 => 20000,20000"},

		// ---- ERRORS (#1041). The issue's own repro never divides by zero;
		// these do, on the third step, after two rows were produced.
		{name: "division by zero in the recursive term's SELECT list", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 + 0 * (1 / (3 - n)) FROM r WHERE n < 5) SELECT n FROM r ORDER BY 1",
			want: "ERR 22012 division by zero"},
		{name: "division by zero in the recursive term's WHERE", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5 AND 1 / (3 - n) > -100) SELECT n FROM r ORDER BY 1",
			want: "ERR 22012 division by zero"},
		{name: "division by zero in the seed", sql: "WITH RECURSIVE r(n) AS (SELECT 1 / 0 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n FROM r ORDER BY 1",
			want: "ERR 22012 division by zero"},
		{name: "an invalid cast on the third step", sql: "WITH RECURSIVE r(n, s) AS (SELECT 1, '1' UNION ALL SELECT n+1, CASE WHEN n = 2 THEN 'x' ELSE s END FROM r WHERE n < 4 AND s::int > 0) SELECT n FROM r ORDER BY 1",
			want: "ERR 22P02"},
		{name: "an integer overflow in the recursive term", sql: "WITH RECURSIVE r(n) AS (SELECT 2147483646 UNION ALL SELECT n+1 FROM r WHERE n > 0) SELECT count(*) FROM r",
			want: "ERR 22003 integer out of range"},

		// ---- TYPES: the non-recursive term decides.
		{name: "a zero-row seed declares the seed's type", sql: "WITH RECURSIVE r AS (SELECT id FROM lat_ord WHERE id > 99 UNION ALL SELECT id+1 FROM r WHERE id<5) SELECT id FROM r",
			want: "id:INT64 =>"},
		{name: "a NULL seed is a row", sql: "WITH RECURSIVE r(n) AS (SELECT NULL::int UNION ALL SELECT n+1 FROM r WHERE n IS NOT NULL) SELECT count(*) AS c, count(n) AS cn FROM r",
			want: "c:INT64,cn:INT64 => 1,0"},
		{name: "a text seed stays text", sql: "WITH RECURSIVE r(s) AS (SELECT 'a' UNION ALL SELECT s || 'a' FROM r WHERE length(s)<3) SELECT s FROM r ORDER BY 1",
			want: "s:STRING => a|aa|aaa"},
		{name: "a text seed that spells a number stays text", sql: "WITH RECURSIVE r(n, s) AS (SELECT 1, '1' UNION ALL SELECT n+1, s || '0' FROM r WHERE n < 3) SELECT s FROM r ORDER BY 1",
			want: "s:STRING => 1|10|100"},
		{name: "a bigint seed", sql: "WITH RECURSIVE r(n) AS (SELECT 1::bigint UNION ALL SELECT n+1 FROM r WHERE n<3) SELECT n FROM r ORDER BY 1",
			want: "n:INT64 => 1|2|3"},
		{name: "a float8 seed", sql: "WITH RECURSIVE r(n) AS (SELECT 1.5::float8 UNION ALL SELECT n+1 FROM r WHERE n<3) SELECT n FROM r ORDER BY 1",
			want: "n:FLOAT64 => 1.5|2.5|3.5"},
		{name: "a NULL column carried through", sql: "WITH RECURSIVE r(n, s) AS (SELECT 1, NULL::text UNION ALL SELECT n+1, s FROM r WHERE n<3) SELECT n, s IS NULL AS z FROM r ORDER BY 1",
			want: "n:INT32,z:BOOL => 1,true|2,true|3,true"},
		// Round 2 (B1): PostgreSQL's UNION resolution with the seed first —
		// an integer or NULL term the seed's type accepts is converted, never
		// refused. Round 1 answered 42804 for both of the first two.
		{name: "an integer term under a numeric seed", sql: "WITH RECURSIVE r(n,k) AS (SELECT 1::numeric,1 UNION ALL SELECT 2,k+1 FROM r WHERE k<3) SELECT n FROM r ORDER BY k",
			want: "n:DECIMAL => 1|2|2"},
		{name: "a NULL term under an integer seed", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT NULL FROM r WHERE n IS NOT NULL) SELECT n FROM r ORDER BY n",
			want: "n:INT32 => 1|<nil>"},
		{name: "a numeric term under a double precision seed", sql: "WITH RECURSIVE r(n,k) AS (SELECT 1::float8,1 UNION ALL SELECT 2.5::numeric,k+1 FROM r WHERE k<3) SELECT n FROM r ORDER BY k",
			want: "n:FLOAT64 => 1|2.5|2.5"},
		{name: "an unconstrained numeric seed widens to its term's scale", sql: "WITH RECURSIVE r(n) AS (SELECT 1::numeric UNION ALL SELECT n + 0.5 FROM r WHERE n < 3) SELECT n FROM r ORDER BY n",
			want: "n:DECIMAL => 1.0|1.5|2.0|2.5|3.0"},
		{name: "a product's scale does not widen the column without end", sql: "WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT n * 0.5, k + 1 FROM r WHERE k < 4) SELECT n FROM r ORDER BY k",
			want: "n:DECIMAL => 1.000|0.500|0.250|0.125"},
		{name: "a quoted term read by the seed type's input function", sql: "WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n FROM r ORDER BY k",
			want: "n:INT32 => 1|7"},
		{name: "a quoted term the seed type does not read", sql: "WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT 'x', k+1 FROM r WHERE k < 2) SELECT n FROM r ORDER BY k",
			want: `ERR 22P02 invalid input syntax for type integer: "x"`},
		{name: "a constrained numeric seed accepts only its own typmod", sql: "WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n FROM r ORDER BY k",
			want: "ERR 42804"},
		{name: "control: a constrained numeric seed and its own typmod", sql: "WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n FROM r ORDER BY k",
			want: "n:DECIMAL => 1.50|2.25"},
		{name: "an integer seed with a fractional term is 42804", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+0.5 FROM r WHERE n<3) SELECT n FROM r",
			want: `ERR 42804 recursive query "r" column 1 has type integer in non-recursive term`},

		// ---- NAMES (#1074, #1193)
		{name: "#1074 a name only the recursive term spells", sql: "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 AS w FROM r WHERE v < 3) SELECT w FROM r",
			want: `ERR 42703 "w"`},
		{name: "#1074 the same under a column list", sql: "WITH RECURSIVE r(v) AS (SELECT 1 AS v UNION ALL SELECT v+1 AS w FROM r WHERE v < 3) SELECT w FROM r",
			want: `ERR 42703 "w"`},
		{name: "#1074 the same nested", sql: "SELECT q.w FROM (WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 AS w FROM r WHERE v < 3) SELECT w FROM r) q",
			want: `ERR 42703 "w"`},
		{name: "#1074 the same qualified", sql: "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 AS w FROM r WHERE v < 3) SELECT r.w FROM r",
			want: `ERR 42703 "r.w"`},
		{name: "#1074 the recursive term names a column the CTE does not publish", sql: "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT w+1 FROM r WHERE v < 3) SELECT v FROM r",
			want: `ERR 42703 "w"`},
		{name: "#1074 a column list renames the seed's name away", sql: "WITH RECURSIVE r(x) AS (SELECT 1 AS v UNION ALL SELECT x+1 FROM r WHERE x < 3) SELECT v FROM r",
			want: `ERR 42703 "v"`},
		{name: "#1074 control: the published name answers", sql: "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 AS w FROM r WHERE v < 3) SELECT * FROM r ORDER BY 1",
			want: "v:INT32 => 1|2|3"},
		{name: "#1193 a non-recursive item under WITH RECURSIVE", sql: "WITH RECURSIVE c AS (SELECT total + 1 FROM lat_ord UNION ALL SELECT total + 2 FROM lat_ord) SELECT * FROM c ORDER BY 1",
			want: "?column?:FLOAT64 => 1|2|151|152|201|202"},
		{name: "#1193 a non-recursive item beside a recursive one", sql: "WITH RECURSIVE a AS (SELECT total + 1 FROM lat_ord WHERE id = 1), r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<2) SELECT * FROM a, r ORDER BY 2",
			want: "?column?:FLOAT64,n:INT32 => 151,1|151,2"},
		{name: "#1193 a non-recursive item read twice", sql: "WITH RECURSIVE c AS (SELECT total + 1 FROM lat_ord WHERE id < 3) SELECT * FROM c x JOIN c y ON true ORDER BY 1, 2",
			want: "?column?:FLOAT64,?column?:FLOAT64 => 151,151|151,201|201,151|201,201"},
		{name: "a short column list renames the leading column only", sql: "WITH RECURSIVE r(a) AS (SELECT 1, 10 UNION ALL SELECT a+1, 20 FROM r WHERE a<3) SELECT * FROM r ORDER BY 1",
			want: "a:INT32,?column?:INT32 => 1,10|2,20|3,20"},

		// ---- REFERENCES
		{name: "two references joined on an expression", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<4) SELECT a.n, b.n FROM r a JOIN r b ON b.n = a.n + 1 ORDER BY 1",
			want: "n:INT32,n:INT32 => 1,2|2,3|3,4"},
		{name: "two references joined on a column", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<4) SELECT a.n, b.n FROM r a JOIN r b ON b.n = a.n ORDER BY 1",
			want: "n:INT32,n:INT32 => 1,1|2,2|3,3|4,4"},
		{name: "a reference in a scalar subquery", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<4) SELECT (SELECT max(n) FROM r) AS m",
			want: "m:INT32 => 4"},
		{name: "a reference in an IN subquery", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<2) SELECT id FROM lat_ord WHERE id IN (SELECT n FROM r) ORDER BY 1",
			want: "id:INT64 => 1|2"},
		{name: "a walk joining a table", sql: "WITH RECURSIVE r AS (SELECT id, customer FROM lat_ord WHERE id=1 UNION ALL SELECT o.id, o.customer FROM lat_ord o JOIN r ON o.id = r.id + 1) SELECT id, customer FROM r ORDER BY 1",
			want: "id:INT64,customer:STRING => 1,Alice|2,Bob|3,Carol"},
		{name: "the recursive term reads the working table through a derived table", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM lat_ord o JOIN (SELECT n FROM r) q ON o.id = q.n WHERE n<3) SELECT n FROM r ORDER BY 1",
			want: "n:INT32 => 1|2|3"},
		{name: "the recursive term correlates an EXISTS over a table", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3 AND EXISTS (SELECT 1 FROM lat_ord WHERE id = n)) SELECT n FROM r ORDER BY 1",
			want: "n:INT32 => 1|2|3"},
		{name: "a recursive item reading another recursive item", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3), s(m) AS (SELECT n FROM r UNION ALL SELECT m + 10 FROM s WHERE m < 20) SELECT count(*) AS c, max(m) AS m FROM s",
			want: "c:INT64,m:INT32 => 9,23"},
		{name: "a recursive term that produces nothing", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<0) SELECT n FROM r",
			want: "n:INT32 => 1"},

		// ---- SHAPES PostgreSQL refuses (42P19), and the ones it answers.
		{name: "an aggregate in the recursive term", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT max(n)+1 FROM r WHERE n<3) SELECT n FROM r",
			want: "ERR 42P19 aggregate functions are not allowed in a recursive query's recursive term"},
		{name: "COUNT(*) in the recursive term", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT count(*) + 1 FROM r WHERE n<3) SELECT n FROM r",
			want: "ERR 42P19 aggregate functions are not allowed"},
		{name: "the self-reference twice", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT a.n+1 FROM r a JOIN r b ON a.n = b.n WHERE a.n<3) SELECT n FROM r",
			want: `ERR 42P19 recursive reference to query "r" must not appear more than once`},
		{name: "the self-reference twice through derived tables", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT a.n + 1 FROM (SELECT n FROM r) a, (SELECT n FROM r) b WHERE a.n < 3) SELECT n FROM r",
			want: "ERR 42P19 must not appear more than once"},
		{name: "the self-reference twice through an inner set operation", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL (SELECT n+1 FROM r WHERE n<3 UNION ALL SELECT n+10 FROM r WHERE n<2)) SELECT n FROM r ORDER BY 1",
			want: "ERR 42P19 must not appear more than once"},
		{name: "the self-reference on the nullable side of a LEFT join", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT o.id+1 FROM lat_ord o LEFT JOIN r ON o.id = r.n WHERE o.id<3) SELECT count(*) FROM r",
			want: `ERR 42P19 recursive reference to query "r" must not appear within an outer join`},
		{name: "the self-reference on the nullable side of a RIGHT join", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT o.id+1 FROM r RIGHT JOIN lat_ord o ON o.id = r.n WHERE o.id<3) SELECT count(*) FROM r",
			want: "ERR 42P19 must not appear within an outer join"},
		{name: "the self-reference in a FULL join", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r FULL JOIN lat_ord o ON o.id = r.n WHERE n<3) SELECT count(*) FROM r",
			want: "ERR 42P19 must not appear within an outer join"},
		{name: "the self-reference in an EXISTS", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT id FROM lat_ord WHERE id < 3 AND EXISTS (SELECT 1 FROM r WHERE r.n = lat_ord.id - 1)) SELECT count(*) FROM r",
			want: `ERR 42P19 recursive reference to query "r" must not appear within a subquery`},
		{name: "the self-reference in an IN", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT id FROM lat_ord WHERE id IN (SELECT n+1 FROM r)) SELECT n FROM r ORDER BY 1",
			want: "ERR 42P19 must not appear within a subquery"},
		{name: "the self-reference in a scalar subquery", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT (SELECT max(n) FROM r) + 1 FROM lat_ord WHERE id = 1 AND false) SELECT n FROM r ORDER BY 1",
			want: "ERR 42P19 must not appear within a subquery"},
		// Round 2 (B2): the aggregate rule is per QUERY BLOCK — refused in a
		// block whose own FROM names the reference, at any depth; allowed in
		// one that reads it only through a derived table.
		{name: "an aggregate in a derived table over the reference", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n FROM (SELECT max(n)+1 AS n FROM r WHERE n<3) q WHERE n IS NOT NULL) SELECT n FROM r ORDER BY n",
			want: "ERR 42P19 aggregate functions are not allowed"},
		{name: "an aggregate two derived tables down", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT m FROM (SELECT x.m FROM (SELECT count(*)+n AS m FROM r WHERE n<3 GROUP BY n) x) q) SELECT n FROM r",
			want: "ERR 42P19 aggregate functions are not allowed"},
		{name: "an aggregate in a joined derived table", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT o.id FROM lat_ord o JOIN (SELECT max(n) AS mx FROM r) q ON o.id = q.mx + 1) SELECT n FROM r",
			want: "ERR 42P19 aggregate functions are not allowed"},
		{name: "control: an aggregate over a derived table that reads the reference", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT max(m) FROM (SELECT n+1 AS m FROM r WHERE n < 3) q HAVING max(m) IS NOT NULL) SELECT n FROM r ORDER BY 1",
			want: "n:INT32 => 1|2|3"},
		{name: "control: an aggregate in a derived table that does not read it", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + q.c::int FROM r, (SELECT count(*) AS c FROM lat_ord) q WHERE n < 5) SELECT n FROM r ORDER BY 1",
			want: "n:INT32 => 1|4|7"},
		{name: "control: the self-reference on the preserved side of a LEFT join", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r LEFT JOIN lat_ord o ON o.id = r.n WHERE n<3) SELECT n FROM r ORDER BY 1",
			want: "n:INT32 => 1|2|3"},
		{name: "control: a GROUP BY without an aggregate", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3 GROUP BY n) SELECT n FROM r ORDER BY 1",
			want: "n:INT32 => 1|2|3"},
		{name: "control: an aggregate inside a subquery of the recursive term", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + (SELECT count(*) FROM lat_ord)::int FROM r WHERE n<5) SELECT n FROM r ORDER BY 1",
			want: "n:INT32 => 1|4|7"},
		{name: "control: INTERSECT inside the recursive term", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL (SELECT n+1 FROM r WHERE n<3 INTERSECT SELECT 2)) SELECT n FROM r ORDER BY 1",
			want: "n:INT32 => 1|2"},

		// ---- BOUNDS: loud, never the rows so far.
		{name: "a recursion with no stop is refused at the iteration limit", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r) SELECT count(*) FROM r",
			want: `ERR 54000 recursive query "r" did not reach a fixed point within 1000000 iterations`},
		{name: "a recursion whose rows double is refused by the budget", sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r CROSS JOIN (SELECT 1 AS k UNION ALL SELECT 2) d WHERE n < 60) SELECT count(*) FROM r",
			want: "ERR 53200 one iteration produced", spilledOnly: true},
	}

	arms := []struct {
		name   string
		budget int64
	}{{"single", 0}, {"spilled512k", 512 * 1024}}
	for _, arm := range arms {
		db := rcFixtureDB(t, arm.budget)
		for _, c := range cells {
			if c.spilledOnly && arm.budget == 0 {
				continue
			}
			t.Run(arm.name+"/"+c.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				got := rcRender(db.Query(ctx, c.sql))
				if !rcMatches(got, c.want) {
					t.Errorf("%s\n  got  %s\n  want %s", c.sql, got, c.want)
				}
			})
		}
		// A CANCELLED statement ends between iterations with the cancellation
		// and never with the rows it had: the old loop broke out of the
		// fixed point on ANY term error, the context's included, and
		// returned the partial closure as the answer.
		t.Run(arm.name+"/a cancelled recursion answers no rows", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			res, err := db.Query(ctx, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r) SELECT count(*) FROM r")
			if err == nil {
				t.Fatalf("a cancelled recursion answered %v", res.Rows)
			}
			if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "context deadline exceeded") {
				t.Errorf("want the cancellation, got %v", err)
			}
		})
	}
}

// rcFixtureDB opens an embedded engine at the given budget over lat_ord and
// lat_item, the rows every PostgreSQL measurement above was taken over.
func rcFixtureDB(t *testing.T, budget int64) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: budget, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	tables := []struct {
		name   string
		schema parquet.Schema
		rows   []map[string]any
	}{
		{"lat_ord", parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "customer", Type: parquet.TypeString},
			{Name: "total", Type: parquet.TypeFloat64},
		}}, []map[string]any{
			{"id": int64(1), "customer": "Alice", "total": 150.0},
			{"id": int64(2), "customer": "Bob", "total": 200.0},
			{"id": int64(3), "customer": "Carol", "total": 0.0},
		}},
		{"lat_item", parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "order_id", Type: parquet.TypeInt64},
			{Name: "product", Type: parquet.TypeString},
			{Name: "amount", Type: parquet.TypeFloat64},
		}}, []map[string]any{
			{"id": int64(1), "order_id": int64(1), "product": "Widget", "amount": 50.0},
			{"id": int64(2), "order_id": int64(1), "product": "Gadget", "amount": 100.0},
			{"id": int64(3), "order_id": int64(2), "product": "Widget", "amount": 75.0},
			{"id": int64(4), "order_id": int64(2), "product": "Doohickey", "amount": 125.0},
		}},
	}
	for _, tb := range tables {
		if err := db.CreateTable(ctx, tb.name, tb.schema, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(tb.name, tb.schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 100})
		if err := ing.Ingest(ctx, tb.rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// rcRender is `name:TYPE,… => v,v|v,v` from the POSITIONAL row form, or
// `ERR <sqlstate> <message>`.
func rcRender(res *QueryResult, err error) string {
	if err != nil {
		return "ERR " + sqlerr.StateOf(err) + " " + err.Error()
	}
	var b strings.Builder
	for i, c := range res.Columns {
		if i > 0 {
			b.WriteString(",")
		}
		typ := "?"
		if i < len(res.OutputSchema) {
			typ = res.OutputSchema[i].Type.String()
		}
		fmt.Fprintf(&b, "%s:%s", c, typ)
	}
	b.WriteString(" =>")
	for ri, r := range res.Rows {
		if ri > 0 {
			b.WriteString("|")
		} else {
			b.WriteString(" ")
		}
		for i, c := range res.Columns {
			if i > 0 {
				b.WriteString(",")
			}
			var v any
			if res.RowValues != nil {
				v = res.RowValues[ri][i]
			} else {
				v = r[c]
			}
			fmt.Fprintf(&b, "%v", v)
		}
	}
	return b.String()
}

// rcMatches: an answer matches exactly; a refusal matches its SQLSTATE and,
// when the cell names one, a substring of its message.
func rcMatches(got, want string) bool {
	if !strings.HasPrefix(want, "ERR ") {
		return got == want
	}
	state, msg, _ := strings.Cut(strings.TrimPrefix(want, "ERR "), " ")
	if !strings.HasPrefix(got, "ERR "+state+" ") {
		return false
	}
	return msg == "" || strings.Contains(got, msg)
}
