// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
)

// AN EXPRESSION SUBQUERY'S BODY IS PARSED ONCE AND PLANNED IN THE WITH CHAIN
// WHERE IT IS WRITTEN, ON EVERY ARM (ADR-0047 stage 3; #1602, #1603, #1606,
// #1599).
//
// At 542b4f37 every expression subquery was planned from its TEXT against the
// statement's WITH list: a subquery in a nested block that read that block's
// WITH item planned a scan of a relation that does not exist and read zero
// rows (NULL for PostgreSQL 17.11's 0, #1602); a nested WITH reusing the
// statement's name read the statement's item (#1606); the declaration of
// `sum((SELECT g FROM s …))` was planned with no WITH list at all and the sum
// read NULL (#1603); and a volatile WITH item read from a correlated subquery,
// or declared in one, or read by a recursive term, was evaluated once per run
// (50 and 4 for PostgreSQL's 1, #1599). A correlated subquery whose own WITH
// shadows an enclosing item was refused 0A000.
//
// Each cell asserts PostgreSQL 17.11's answer (measured on postgres:17-alpine
// over the same rows: tb_p 3 rows, tb_big 20000, id bigint, v float8 = 1.5*id;
// the volatile cells assert the SHAPE of the answer — two reads agree, so a
// difference is 0 and a distinct count is 1) or, as KEEP, the base answer an
// arm keeps for a mechanism this stage does not touch: the stage DAG has no
// lowering for a recursive CTE (ADR-0021 §1b) nor for a correlated subquery
// in an aggregate's argument under HAVING, and a WITH item that reads the
// enclosing row is refused 42P01 on every arm (filing candidate CI3-F1). A KEEP
// cell that starts answering PostgreSQL FAILS: delete the KEEP.
func TestArcCI3SubqueryBodyInItsScopeEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: three DAG arms stand up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := cmArms(t, ctx)[:5]
	for _, c := range ci3ArmCells {
		t.Run(c.name, func(t *testing.T) {
			for i, arm := range arms {
				want := c.expect[i]
				for rep := 0; rep < 2; rep++ {
					got := arm.answer(c.sql)
					if strings.HasPrefix(want, "KEEP ") {
						if ci3SameAnswer(got, c.pg) {
							t.Errorf("%s now answers PostgreSQL's %s: delete its KEEP\n  %s", arm.name, c.pg, c.sql)
						} else if !strings.HasPrefix(got, strings.TrimPrefix(want, "KEEP ")) {
							t.Errorf("%s: %s\n  got  %s\n  want the kept %s (PostgreSQL 17.11: %s)", arm.name, c.sql, got, want, c.pg)
						}
						break
					}
					if !ci3SameAnswer(got, c.pg) {
						t.Errorf("%s rep %d: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", arm.name, rep, c.sql, got, c.pg)
						break
					}
				}
			}
		})
	}
}

// ci3SameAnswer compares a rendered answer (rows "; ", fields ",") with
// PostgreSQL's psql text: field by field, a number by its value (a float8
// column renders 3.00015e+08 here and 300015000 there).
func ci3SameAnswer(got, pg string) bool {
	if got == pg {
		return true
	}
	gr, pr := strings.Split(got, "; "), strings.Split(pg, "; ")
	if len(gr) != len(pr) {
		return false
	}
	for i := range gr {
		gf, pf := strings.Split(gr[i], ","), strings.Split(pr[i], ",")
		if len(gf) != len(pf) {
			return false
		}
		for j := range gf {
			if gf[j] == pf[j] {
				continue
			}
			a, errA := strconv.ParseFloat(gf[j], 64)
			b, errB := strconv.ParseFloat(pf[j], 64)
			if errA != nil || errB != nil || a != b {
				return false
			}
		}
	}
	return true
}

type ci3ArmCell struct {
	name, sql, pg string
	expect        [5]string
}

// ci3ArmCells: name, SQL over the tb_ fixture, PostgreSQL 17.11's answer, and
// per arm (single, spilled512k, dag, dag-shuffled, dag-morsel4) PG or KEEP
// <the base answer's prefix>.
var ci3ArmCells = []ci3ArmCell{
	{"i1602/issue", `WITH s AS (SELECT sum(random()) r FROM tb_big) SELECT x.d FROM (WITH t AS (SELECT r FROM s) SELECT (SELECT r FROM t) - (SELECT r FROM t) AS d) x`, "0", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1602/det", `WITH s AS (SELECT sum(v) r FROM tb_big) SELECT x.d FROM (WITH t AS (SELECT r FROM s) SELECT (SELECT r FROM t) - (SELECT r FROM t) AS d) x`, "0", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1602/detVal", `WITH s AS (SELECT sum(v) r FROM tb_big) SELECT x.d FROM (WITH t AS (SELECT r FROM s) SELECT (SELECT r FROM t) AS d) x`, "300015000", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1602/detFrom", `WITH s AS (SELECT sum(v) r FROM tb_big) SELECT x.d FROM (WITH t AS (SELECT r FROM s) SELECT r AS d FROM t) x`, "300015000", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1602/detRootSub", `WITH s AS (SELECT sum(v) r FROM tb_big) SELECT x.d FROM (SELECT (SELECT r FROM s) AS d) x`, "300015000", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1602/L12", `SELECT * FROM (WITH s AS (SELECT sum(random()) AS r FROM tb_big) SELECT (SELECT r FROM s) - (SELECT r FROM s) AS d) x ORDER BY 1`, "0", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1602/L12det", `SELECT * FROM (WITH s AS (SELECT sum(v) AS r FROM tb_big) SELECT (SELECT r FROM s) - (SELECT r FROM s) AS d) x ORDER BY 1`, "0", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1602/L12detVal", `SELECT * FROM (WITH s AS (SELECT sum(v) AS r FROM tb_big) SELECT (SELECT r FROM s) AS d) x ORDER BY 1`, "300015000", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1603/Q4", `WITH s AS (SELECT g FROM generate_series(1, 100) g) SELECT sum((SELECT g FROM s WHERE g >= t.id LIMIT 1)) FROM tb_p t`, "6", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1603/S6d", `WITH s AS (SELECT g, random() AS r FROM generate_series(1, 100000) g) SELECT sum((SELECT g FROM s WHERE g >= t.id LIMIT 1)) FROM tb_p t WHERE (SELECT count(*) FROM s) = 100000`, "6", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1603/Q1", `SELECT sum((SELECT g FROM generate_series(1, 100) g WHERE g >= t.id LIMIT 1)) FROM tb_p t`, "6", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1603/tblCte", `WITH s AS (SELECT id AS g FROM tb_big) SELECT sum((SELECT min(g) FROM s WHERE g >= t.id)) FROM tb_p t`, "6", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1603/tblNoCte", `SELECT sum((SELECT min(id) FROM tb_big b WHERE b.id >= t.id)) FROM tb_p t`, "6", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1603/maxCte", `WITH s AS (SELECT id AS g FROM tb_big) SELECT max((SELECT min(g) FROM s WHERE g >= t.id)) FROM tb_p t`, "3", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1603/countCte", `WITH s AS (SELECT id AS g FROM tb_big) SELECT count((SELECT min(g) FROM s WHERE g > t.id + 1)) FROM tb_p t`, "3", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1603/listCte", `WITH s AS (SELECT id AS g FROM tb_big) SELECT t.id, (SELECT min(g) FROM s WHERE g >= t.id) FROM tb_p t ORDER BY 1`, "1,1; 2,2; 3,3", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1603/argExprCte", `WITH s AS (SELECT id AS g FROM tb_big) SELECT sum(1 + (SELECT min(g) FROM s WHERE g >= t.id)) FROM tb_p t`, "9", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1603/havingCte", `WITH s AS (SELECT id AS g FROM tb_big) SELECT count(*) FROM tb_p t HAVING sum((SELECT min(g) FROM s WHERE g >= t.id)) = 6`, "3", [5]string{"PG", "PG", "KEEP ERR (uncoded) native DAG: stage scan-0 (scan): scan-agg stage scan-0: task", "KEEP ERR (uncoded) native DAG: stage scan-0 (scan): scan-agg stage scan-0: task", "KEEP ERR (uncoded) native DAG: stage scan-0 (scan): scan-agg stage scan-0: task"}},
	{"i1606/N05", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id, (SELECT max(id) FROM c) AS m FROM c) d WHERE id = (SELECT max(id) FROM c) + 1 ORDER BY 1`, "2,2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/N05b", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM c) d WHERE id > (SELECT max(id) FROM c) LIMIT 3`, "2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/N05c", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM c) d UNION ALL SELECT max(id) FROM c UNION ALL SELECT (SELECT max(id) FROM c)`, "2; 1; 1", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/N05d", `WITH c AS (SELECT sum(random()) AS r FROM tb_big) SELECT * FROM (WITH c AS (SELECT 5.0 AS r) SELECT r FROM c) d WHERE r <> (SELECT r FROM c) + (SELECT r FROM c) - (SELECT r FROM c) ORDER BY 1`, "5.0", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/L06", `WITH c AS (SELECT id FROM tb_p WHERE id = 1) SELECT * FROM (WITH c AS (SELECT id FROM tb_p WHERE id = 2) SELECT id FROM c) d WHERE id > (SELECT max(id) FROM c) ORDER BY 1`, "2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/innerSub", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT (SELECT max(id) FROM c) AS m) d`, "2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/innerExists", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT count(*) AS n FROM tb_p WHERE EXISTS (SELECT 1 FROM c WHERE c.id = tb_p.id)) d`, "1", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/innerIn", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM tb_p WHERE id IN (SELECT id FROM c)) d ORDER BY 1`, "2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/subWith", `WITH c AS (SELECT 1 AS id) SELECT (WITH c AS (SELECT 2 AS id) SELECT max(id) FROM c) AS m, (SELECT max(id) FROM c) AS o`, "2,1", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/subWithCorr", `WITH c AS (SELECT 1 AS id) SELECT t.id, (WITH c AS (SELECT 2 AS id) SELECT max(id) + t.id FROM c) AS m FROM tb_p t ORDER BY 1`, "1,3; 2,4; 3,5", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1599/issue", `WITH s AS (SELECT random() r) SELECT count(DISTINCT (SELECT r + t.id*0 FROM s)) FROM tb_big t WHERE t.id <= 50`, "1", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1599/inner", `SELECT count(DISTINCT (WITH s AS (SELECT random() r) SELECT r + t.id*0 FROM s)) FROM tb_big t WHERE t.id <= 50`, "1", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1599/where", `WITH s AS (SELECT random() r) SELECT count(*) FROM tb_big t WHERE t.id <= 50 AND (SELECT r + t.id*0 FROM s) = (SELECT r FROM s)`, "50", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1599/rec", `WITH RECURSIVE s AS (SELECT random() AS v), r(n, v) AS (SELECT 1, 0.0::float8 UNION ALL SELECT n + 1, (SELECT v FROM s) FROM r WHERE n < 5) SELECT count(DISTINCT v) FROM r WHERE n > 1`, "1", [5]string{"PG", "PG", "KEEP ERR (uncoded) native DAG: stage scan-0 (scan): stage scan-0 worker 0: stag", "KEEP ERR (uncoded) native DAG: stage scan-0 (scan): stage scan-0 worker 0: stag", "KEEP ERR (uncoded) native DAG: stage scan-0 (scan): stage scan-0 worker 0: stag"}},
	{"i1599/recFrom", `WITH RECURSIVE s AS (SELECT random() AS v), r(n, v) AS (SELECT 1, 0.0::float8 UNION ALL SELECT n + 1, s.v FROM r, s WHERE n < 5) SELECT count(DISTINCT v) FROM r WHERE n > 1`, "1", [5]string{"PG", "PG", "KEEP ERR (uncoded) native DAG: stage scan-0 (scan): stage scan-0 worker 0: stag", "KEEP ERR (uncoded) native DAG: stage scan-0 (scan): stage scan-0 worker 0: stag", "KEEP ERR (uncoded) native DAG: stage scan-0 (scan): stage scan-0 worker 0: stag"}},
	{"i1599/det", `WITH s AS (SELECT sum(v) r FROM tb_big) SELECT count(DISTINCT (SELECT r + t.id*0 FROM s)) FROM tb_big t WHERE t.id <= 50`, "1", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1599/ctlOuterDep", `SELECT count(DISTINCT (WITH s AS (SELECT random() + t.id*0 AS r) SELECT r FROM s)) FROM tb_big t WHERE t.id <= 50`, "50", [5]string{"KEEP ERR 42P01", "KEEP ERR 42P01", "KEEP ERR 42P01", "KEEP ERR 42P01", "KEEP ERR 42P01"}},
	{"i1599/ctlFiltered", `WITH s AS (SELECT g, random() AS r FROM generate_series(1, 10) g) SELECT count(DISTINCT (SELECT r FROM s WHERE g = 1 AND t.id > 0)) FROM tb_big t WHERE t.id <= 50`, "1", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1599/ctlUncorrelated", `WITH s AS (SELECT random() AS r) SELECT count(DISTINCT (SELECT r FROM s)) FROM tb_big t WHERE t.id <= 50`, "1", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1599/recDet", `WITH RECURSIVE s AS (SELECT 7 AS v), r(n, v) AS (SELECT 1, 0 UNION ALL SELECT n + 1, (SELECT v FROM s) FROM r WHERE n < 5) SELECT count(DISTINCT v) FROM r WHERE n > 1`, "1", [5]string{"PG", "PG", "KEEP ERR (uncoded) native DAG: stage scan-0 (scan): stage scan-0 worker 0: stag", "KEEP ERR (uncoded) native DAG: stage scan-0 (scan): stage scan-0 worker 0: stag", "KEEP ERR (uncoded) native DAG: stage scan-0 (scan): stage scan-0 worker 0: stag"}},
	{"i1606/rootFromShadow", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM c) d`, "2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/bodyReadsOuter", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT id + 1 AS id FROM c) SELECT id FROM c) d`, "2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/cteBodyNested", `WITH c AS (SELECT 1 AS id), d AS (SELECT id FROM c) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT (SELECT max(id) FROM d) AS m, (SELECT max(id) FROM c) AS n) x`, "1,2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/subOwnWith", `WITH c AS (SELECT 1 AS id) SELECT (WITH c AS (SELECT 2 AS id) SELECT max(id) FROM c) AS m`, "2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1603/corrCte", `WITH s AS (SELECT id AS g FROM tb_big) SELECT t.id FROM tb_p t WHERE (SELECT min(g) FROM s WHERE g > t.id) = t.id + 1 ORDER BY 1`, "1; 2; 3", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/existsOwnWith", `WITH d AS (SELECT 1 AS k) SELECT t.id FROM tb_p t WHERE EXISTS (WITH d AS (SELECT 2 AS k) SELECT 1 FROM d WHERE d.k = t.id) ORDER BY 1`, "2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/inOwnWith", `WITH d AS (SELECT 1 AS k) SELECT t.id FROM tb_p t WHERE t.id IN (WITH d AS (SELECT 2 AS k UNION ALL SELECT 3) SELECT k FROM d WHERE k >= t.id) ORDER BY 1`, "2; 3", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"i1606/ownWithDiffSchema", `WITH d AS (SELECT 1 AS k) SELECT t.id, (WITH d AS (SELECT 5 AS z) SELECT z + t.id FROM d) AS m FROM tb_p t ORDER BY 1`, "1,6; 2,7; 3,8", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	// Folded from the closure review's 152 statements (arc CI3 round 2,
	// ci3_review_r1/ci3rev1_stmts.tsv s06–s17, v04, v07, f05–f08): twelve
	// shadowing shapes, a statement CTE read through a derived table in a
	// correlated body, and four more lifted-refusal forms; each moved from a
	// wrong value or 0A000 at 542b4f37 to PostgreSQL's answer on all five
	// arms. v07 (a volatile WITH inside a DERIVED TABLE inside a correlated
	// subquery) is evaluated per outer row at 542b4f37 and here: KEEP 50,
	// filing candidate CI3-F3 (PostgreSQL 1).
	{"r2/shadow/subInSub", `WITH c AS (SELECT 1 AS id) SELECT (SELECT max(id) FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM c) z), (SELECT max(id) FROM c)`, "2,1", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/shadow/threeNested", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT (SELECT max(id) FROM (WITH c AS (SELECT 3 AS id) SELECT id FROM c) q) AS a, (SELECT max(id) FROM c) AS b) x`, "3,2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/shadow/inOwnWith", `WITH c AS (SELECT id FROM tb_d) SELECT t.id FROM tb_p t WHERE t.id IN (WITH c AS (SELECT id FROM tb_p WHERE id > 1) SELECT id FROM c) ORDER BY 1`, "2; 3", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/shadow/notExists", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM tb_p WHERE NOT EXISTS (SELECT 1 FROM c WHERE c.id = tb_p.id)) d ORDER BY 1`, "1; 3", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/shadow/eqAny", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM tb_p WHERE id = ANY (SELECT id FROM c)) d ORDER BY 1`, "2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/shadow/having", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id, count(*) AS n FROM tb_p GROUP BY id HAVING id = (SELECT max(id) FROM c)) d ORDER BY 1`, "2,1", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/shadow/orderBy", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM tb_p ORDER BY abs(id - (SELECT max(id) FROM c)), id LIMIT 1) d`, "2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/shadow/aggArg", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT sum((SELECT max(id) FROM c)) AS s FROM tb_p) d`, "6", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/shadow/caseArm", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id, CASE WHEN id = (SELECT max(id) FROM c) THEN 'hit' ELSE 'miss' END AS h FROM tb_p) d ORDER BY 1`, "1,miss; 2,hit; 3,miss", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/shadow/correlatedInShadowed", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT t.id, (SELECT count(*) FROM c WHERE c.id <= t.id) AS n FROM tb_p t) d ORDER BY 1`, "1,0; 2,1; 3,1", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/shadow/rootBesideDerived", `WITH c AS (SELECT 1 AS id) SELECT id, (SELECT max(id) FROM c) AS o FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM c) d`, "2,1", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/perRun/cteThroughDerived", `WITH s AS (SELECT random() r) SELECT count(DISTINCT (SELECT r + t.id*0 FROM (SELECT r FROM s) d)) FROM tb_big t WHERE t.id <= 50`, "1", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/perRun/withInsideDerived", `SELECT count(DISTINCT (SELECT max(r) + t.id*0 FROM (WITH q AS (SELECT random() AS r) SELECT r FROM q) d)) FROM tb_big t WHERE t.id <= 50`, "1", [5]string{"KEEP 50", "KEEP 50", "KEEP 50", "KEEP 50", "KEEP 50"}},
	{"r2/lifted/notExists", `WITH d AS (SELECT 1 AS k) SELECT t.id FROM tb_p t WHERE NOT EXISTS (WITH d AS (SELECT 2 AS k) SELECT 1 FROM d WHERE d.k = t.id) ORDER BY 1`, "1; 3", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/lifted/countOwnItem", `WITH d AS (SELECT id AS k FROM tb_d) SELECT t.id, (WITH d AS (SELECT id AS k FROM tb_p) SELECT count(*) FROM d WHERE d.k < t.id) AS m FROM tb_p t ORDER BY 1`, "1,0; 2,1; 3,2", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/lifted/ownPlusOuter", `WITH d AS (SELECT 1 AS k) SELECT t.id, (WITH d AS (SELECT 2 AS k) SELECT max(k) FROM d WHERE k > t.id) + (SELECT max(k) FROM d WHERE k >= t.id) AS m FROM tb_p t ORDER BY 1`, "1,3; 2,NULL; 3,NULL", [5]string{"PG", "PG", "PG", "PG", "PG"}},
	{"r2/lifted/text", `WITH d AS (SELECT 1 AS k) SELECT t.id, (WITH d AS (SELECT 'x' AS k) SELECT max(k) || t.id::text FROM d) AS m FROM tb_p t ORDER BY 1`, "1,x1; 2,x2; 3,x3", [5]string{"PG", "PG", "PG", "PG", "PG"}},
}
