// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"
)

// ARC RC ON FIVE ARMS: a recursive CTE answers its whole closure or fails.
//
// wadjet.TestArcRCRecursiveCTEAnswersItsWholeClosureOrFails holds the full
// table on the embedded engine, unbudgeted and at 512 KiB; this is the same
// seam on the five arms the coordinator stands up, one cell per family, every
// answer measured on PostgreSQL 17.11 over `lat_ord` first.
//
// The DAG arms cannot run a recursive CTE at all (#1042): the tagged scan
// becomes a stage with no scan files and the dispatcher fails it, loudly. That
// is pinned per arm, not chased. A NON-recursive item under WITH RECURSIVE is
// an ordinary CTE since #1193, and the DAG answers it like one — those cells
// carry no pin.
func TestArcRCRecursiveCTEAnswersItsWholeClosureOrFailsOnFiveArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up three embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := c1Arms(t, ctx)

	rec := func(name, sql, want, why string) c1Case {
		return c1Case{name: name, sql: sql, want: want, pin: c1RecDAGPins(),
			why: why + "; #1042: the DAG cannot run any recursive CTE", routed: c1RecRoutes}
	}
	c1FormRun(t, arms, []c1Case{
		// DEPTH (#1246): the base answered 1001 for all three.
		rec("depth 1002", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<1002) SELECT count(*), max(n) FROM r",
			"cols=[count:INT64 max:INT32] rows=1 | 1002,1002", "PostgreSQL 17.11: 1002 | 1002"),
		rec("depth 5000", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5000) SELECT COUNT(*), MAX(n) FROM r",
			"cols=[count:INT64 max:INT32] rows=1 | 5000,5000", "PostgreSQL 17.11: 5000 | 5000"),
		rec("a five-year date series", "WITH RECURSIVE d(x) AS (SELECT DATE '2020-01-01' UNION ALL SELECT (x + 1) FROM d WHERE x < DATE '2024-12-31') SELECT count(*), min(x), max(x) FROM d",
			"cols=[count:INT64 min:DATE max:DATE] rows=1 | 1827,2020-01-01,2024-12-31", "PostgreSQL 17.11: 1827 | 2020-01-01 | 2024-12-31; the base added 1 to a string"),
		// ERRORS (#1041): the base answered 1, 2, 3.
		rec("division by zero on the third step", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5 AND 1 / (3 - n) > -100) SELECT n FROM r ORDER BY 1",
			"ERR division by zero", "PostgreSQL 17.11: 22012"),
		// TYPES
		rec("a zero-row seed declares the seed's type", "WITH RECURSIVE r AS (SELECT id FROM lat_ord WHERE id > 99 UNION ALL SELECT id+1 FROM r WHERE id<5) SELECT id FROM r",
			"cols=[id:INT64] rows=0", "PostgreSQL 17.11: id bigint, 0 rows; the base declared text"),
		rec("an integer seed with a fractional term", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+0.5 FROM r WHERE n<3) SELECT n FROM r",
			`ERR recursive query "r" column 1 has type integer in non-recursive term`, "PostgreSQL 17.11: 42804; the base truncated 1.5 to 1 and recursed"),
		// NAMES (#1074, #1193)
		// The binder refuses it before any plan exists, so the DAG arms agree
		// with PostgreSQL too: no pin.
		{name: "#1074 a name only the recursive term spells",
			sql:  "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 AS w FROM r WHERE v < 3) SELECT w FROM r",
			want: `ERR unknown column "w"`,
			why:  "PostgreSQL 17.11: 42703; the base answered three NULLs"},
		{name: "#1193 a non-recursive item under WITH RECURSIVE",
			sql:  "WITH RECURSIVE c AS (SELECT total + 1 FROM lat_ord UNION ALL SELECT total + 2 FROM lat_ord) SELECT * FROM c ORDER BY 1",
			want: "cols=[?column?:FLOAT64] rows=6 | 1 | 2 | 151 | 152 | 201 | 202",
			why:  "PostgreSQL 17.11 publishes ?column?; the base published `total + 1`"},
		// REFERENCES: the base answered zero rows.
		rec("two references joined on an expression", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<4) SELECT a.n, b.n FROM r a JOIN r b ON b.n = a.n + 1 ORDER BY 1",
			"cols=[n:INT32 n:INT32] rows=3 | 1,2 | 2,3 | 3,4", "PostgreSQL 17.11: 1,2 | 2,3 | 3,4"),
		// SHAPES (42P19)
		rec("an aggregate in the recursive term", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT max(n)+1 FROM r WHERE n<3) SELECT n FROM r",
			"ERR aggregate functions are not allowed in a recursive query's recursive term", "PostgreSQL 17.11: 42P19; the base answered 1, 2, 3 and 998 NULLs"),
		rec("the self-reference on the nullable side of an outer join", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT o.id+1 FROM lat_ord o LEFT JOIN r ON o.id = r.n WHERE o.id<3) SELECT count(*) FROM r",
			`ERR recursive reference to query "r" must not appear within an outer join`, "PostgreSQL 17.11: 42P19; the base answered 2001"),
		rec("the self-reference in a subquery", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT (SELECT max(n) FROM r) + 1 FROM lat_ord WHERE id = 1 AND false) SELECT n FROM r ORDER BY 1",
			`ERR recursive reference to query "r" must not appear within a subquery`, "PostgreSQL 17.11: 42P19; the base answered one NULL"),
	})
}
