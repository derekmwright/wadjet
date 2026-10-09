// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// AN EXPRESSION SUBQUERY'S BODY IS PLANNED IN THE WITH CHAIN WHERE IT IS
// WRITTEN, ON THE EMBEDDED DOOR (ADR-0047 stage 3; #1602, #1603, #1606,
// #1599): the four issues' statements and their controls, PostgreSQL 17.11's
// answers measured over the same rows (cm_p 3 rows, cm_big 20000; id bigint,
// v float8 = 1.5*id), at an unbounded budget and at 512 KiB, three times each
// (the volatile cells assert the shape of the answer: two reads of one WITH
// item agree). keep names the base answer a cell keeps for a mechanism this
// stage does not touch; a kept cell that starts answering PostgreSQL FAILS.
// coordinator.TestArcCI3SubqueryBodyInItsScopeEveryArm carries the same
// cells on five arms; pgwire.TestArcCI3SubqueryScopeOnTheWire on the wire.
func TestArcCI3EmbeddedSubqueryBodyInItsScope(t *testing.T) {
	ctx := context.Background()
	for _, budget := range []int64{0, 512 << 10} {
		db := cmOpen(t, budget)
		for _, c := range ci3EmbeddedCells {
			for rep := 0; rep < 3; rep++ {
				got := ci3EmbeddedAnswer(ctx, db, c.sql)
				if c.keep != "" {
					if ci3EmbeddedSame(got, c.pg) {
						t.Errorf("%s budget %d now answers PostgreSQL's %s: delete its keep\n  %s", c.name, budget, c.pg, c.sql)
					} else if got != c.keep {
						t.Errorf("%s budget %d: %s\n  got  %s\n  want the kept %s (PostgreSQL 17.11: %s)", c.name, budget, c.sql, got, c.keep, c.pg)
					}
					break
				}
				if !ci3EmbeddedSame(got, c.pg) {
					t.Errorf("%s budget %d rep %d: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, budget, rep, c.sql, got, c.pg)
					break
				}
			}
		}
	}
}

// ci3EmbeddedAnswer renders a result as PostgreSQL's psql text is compared:
// rows "; ", fields ",", NULL for a null; an error as its SQLSTATE.
func ci3EmbeddedAnswer(ctx context.Context, db *DB, sql string) string {
	res, err := db.Query(ctx, sql)
	if err != nil {
		return "ERR " + sqlerr.StateOf(err)
	}
	if len(res.Rows) == 0 {
		return "(0 rows)"
	}
	rows := make([]string, len(res.Rows))
	for i := range res.Rows {
		cells := res.Cells(i)
		f := make([]string, len(cells))
		for j, v := range cells {
			if v == nil {
				f[j] = "NULL"
			} else {
				f[j] = fmt.Sprint(v)
			}
		}
		rows[i] = strings.Join(f, ",")
	}
	return strings.Join(rows, "; ")
}

// ci3EmbeddedSame compares field by field, a number by its value.
func ci3EmbeddedSame(got, pg string) bool {
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

var ci3EmbeddedCells = []struct{ name, sql, pg, keep string }{
	{"i1602/issue", `WITH s AS (SELECT sum(random()) r FROM cm_big) SELECT x.d FROM (WITH t AS (SELECT r FROM s) SELECT (SELECT r FROM t) - (SELECT r FROM t) AS d) x`, "0", ""},
	{"i1602/det", `WITH s AS (SELECT sum(v) r FROM cm_big) SELECT x.d FROM (WITH t AS (SELECT r FROM s) SELECT (SELECT r FROM t) - (SELECT r FROM t) AS d) x`, "0", ""},
	{"i1602/detVal", `WITH s AS (SELECT sum(v) r FROM cm_big) SELECT x.d FROM (WITH t AS (SELECT r FROM s) SELECT (SELECT r FROM t) AS d) x`, "300015000", ""},
	{"i1602/detFrom", `WITH s AS (SELECT sum(v) r FROM cm_big) SELECT x.d FROM (WITH t AS (SELECT r FROM s) SELECT r AS d FROM t) x`, "300015000", ""},
	{"i1602/detRootSub", `WITH s AS (SELECT sum(v) r FROM cm_big) SELECT x.d FROM (SELECT (SELECT r FROM s) AS d) x`, "300015000", ""},
	{"i1602/L12", `SELECT * FROM (WITH s AS (SELECT sum(random()) AS r FROM cm_big) SELECT (SELECT r FROM s) - (SELECT r FROM s) AS d) x ORDER BY 1`, "0", ""},
	{"i1602/L12det", `SELECT * FROM (WITH s AS (SELECT sum(v) AS r FROM cm_big) SELECT (SELECT r FROM s) - (SELECT r FROM s) AS d) x ORDER BY 1`, "0", ""},
	{"i1602/L12detVal", `SELECT * FROM (WITH s AS (SELECT sum(v) AS r FROM cm_big) SELECT (SELECT r FROM s) AS d) x ORDER BY 1`, "300015000", ""},
	{"i1603/Q4", `WITH s AS (SELECT g FROM generate_series(1, 100) g) SELECT sum((SELECT g FROM s WHERE g >= t.id LIMIT 1)) FROM cm_p t`, "6", ""},
	{"i1603/S6d", `WITH s AS (SELECT g, random() AS r FROM generate_series(1, 100000) g) SELECT sum((SELECT g FROM s WHERE g >= t.id LIMIT 1)) FROM cm_p t WHERE (SELECT count(*) FROM s) = 100000`, "6", ""},
	{"i1603/Q1", `SELECT sum((SELECT g FROM generate_series(1, 100) g WHERE g >= t.id LIMIT 1)) FROM cm_p t`, "6", ""},
	{"i1603/tblCte", `WITH s AS (SELECT id AS g FROM cm_big) SELECT sum((SELECT min(g) FROM s WHERE g >= t.id)) FROM cm_p t`, "6", ""},
	{"i1603/tblNoCte", `SELECT sum((SELECT min(id) FROM cm_big b WHERE b.id >= t.id)) FROM cm_p t`, "6", ""},
	{"i1603/maxCte", `WITH s AS (SELECT id AS g FROM cm_big) SELECT max((SELECT min(g) FROM s WHERE g >= t.id)) FROM cm_p t`, "3", ""},
	{"i1603/countCte", `WITH s AS (SELECT id AS g FROM cm_big) SELECT count((SELECT min(g) FROM s WHERE g > t.id + 1)) FROM cm_p t`, "3", ""},
	{"i1603/listCte", `WITH s AS (SELECT id AS g FROM cm_big) SELECT t.id, (SELECT min(g) FROM s WHERE g >= t.id) FROM cm_p t ORDER BY 1`, "1,1; 2,2; 3,3", ""},
	{"i1603/argExprCte", `WITH s AS (SELECT id AS g FROM cm_big) SELECT sum(1 + (SELECT min(g) FROM s WHERE g >= t.id)) FROM cm_p t`, "9", ""},
	{"i1603/havingCte", `WITH s AS (SELECT id AS g FROM cm_big) SELECT count(*) FROM cm_p t HAVING sum((SELECT min(g) FROM s WHERE g >= t.id)) = 6`, "3", ""},
	{"i1606/N05", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id, (SELECT max(id) FROM c) AS m FROM c) d WHERE id = (SELECT max(id) FROM c) + 1 ORDER BY 1`, "2,2", ""},
	{"i1606/N05b", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM c) d WHERE id > (SELECT max(id) FROM c) LIMIT 3`, "2", ""},
	{"i1606/N05c", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM c) d UNION ALL SELECT max(id) FROM c UNION ALL SELECT (SELECT max(id) FROM c)`, "2; 1; 1", ""},
	{"i1606/N05d", `WITH c AS (SELECT sum(random()) AS r FROM cm_big) SELECT * FROM (WITH c AS (SELECT 5.0 AS r) SELECT r FROM c) d WHERE r <> (SELECT r FROM c) + (SELECT r FROM c) - (SELECT r FROM c) ORDER BY 1`, "5.0", ""},
	{"i1606/L06", `WITH c AS (SELECT id FROM cm_p WHERE id = 1) SELECT * FROM (WITH c AS (SELECT id FROM cm_p WHERE id = 2) SELECT id FROM c) d WHERE id > (SELECT max(id) FROM c) ORDER BY 1`, "2", ""},
	{"i1606/innerSub", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT (SELECT max(id) FROM c) AS m) d`, "2", ""},
	{"i1606/innerExists", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT count(*) AS n FROM cm_p WHERE EXISTS (SELECT 1 FROM c WHERE c.id = cm_p.id)) d`, "1", ""},
	{"i1606/innerIn", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM cm_p WHERE id IN (SELECT id FROM c)) d ORDER BY 1`, "2", ""},
	{"i1606/subWith", `WITH c AS (SELECT 1 AS id) SELECT (WITH c AS (SELECT 2 AS id) SELECT max(id) FROM c) AS m, (SELECT max(id) FROM c) AS o`, "2,1", ""},
	{"i1606/subWithCorr", `WITH c AS (SELECT 1 AS id) SELECT t.id, (WITH c AS (SELECT 2 AS id) SELECT max(id) + t.id FROM c) AS m FROM cm_p t ORDER BY 1`, "1,3; 2,4; 3,5", ""},
	{"i1599/issue", `WITH s AS (SELECT random() r) SELECT count(DISTINCT (SELECT r + t.id*0 FROM s)) FROM cm_big t WHERE t.id <= 50`, "1", ""},
	{"i1599/inner", `SELECT count(DISTINCT (WITH s AS (SELECT random() r) SELECT r + t.id*0 FROM s)) FROM cm_big t WHERE t.id <= 50`, "1", ""},
	{"i1599/where", `WITH s AS (SELECT random() r) SELECT count(*) FROM cm_big t WHERE t.id <= 50 AND (SELECT r + t.id*0 FROM s) = (SELECT r FROM s)`, "50", ""},
	{"i1599/rec", `WITH RECURSIVE s AS (SELECT random() AS v), r(n, v) AS (SELECT 1, 0.0::float8 UNION ALL SELECT n + 1, (SELECT v FROM s) FROM r WHERE n < 5) SELECT count(DISTINCT v) FROM r WHERE n > 1`, "1", ""},
	{"i1599/recFrom", `WITH RECURSIVE s AS (SELECT random() AS v), r(n, v) AS (SELECT 1, 0.0::float8 UNION ALL SELECT n + 1, s.v FROM r, s WHERE n < 5) SELECT count(DISTINCT v) FROM r WHERE n > 1`, "1", ""},
	{"i1599/det", `WITH s AS (SELECT sum(v) r FROM cm_big) SELECT count(DISTINCT (SELECT r + t.id*0 FROM s)) FROM cm_big t WHERE t.id <= 50`, "1", ""},
	{"i1599/ctlOuterDep", `SELECT count(DISTINCT (WITH s AS (SELECT random() + t.id*0 AS r) SELECT r FROM s)) FROM cm_big t WHERE t.id <= 50`, "50", "ERR 42P01"},
	{"i1599/ctlFiltered", `WITH s AS (SELECT g, random() AS r FROM generate_series(1, 10) g) SELECT count(DISTINCT (SELECT r FROM s WHERE g = 1 AND t.id > 0)) FROM cm_big t WHERE t.id <= 50`, "1", ""},
	{"i1599/ctlUncorrelated", `WITH s AS (SELECT random() AS r) SELECT count(DISTINCT (SELECT r FROM s)) FROM cm_big t WHERE t.id <= 50`, "1", ""},
	{"i1599/recDet", `WITH RECURSIVE s AS (SELECT 7 AS v), r(n, v) AS (SELECT 1, 0 UNION ALL SELECT n + 1, (SELECT v FROM s) FROM r WHERE n < 5) SELECT count(DISTINCT v) FROM r WHERE n > 1`, "1", ""},
	{"i1606/rootFromShadow", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM c) d`, "2", ""},
	{"i1606/bodyReadsOuter", `WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT id + 1 AS id FROM c) SELECT id FROM c) d`, "2", ""},
	{"i1606/cteBodyNested", `WITH c AS (SELECT 1 AS id), d AS (SELECT id FROM c) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT (SELECT max(id) FROM d) AS m, (SELECT max(id) FROM c) AS n) x`, "1,2", ""},
	{"i1606/subOwnWith", `WITH c AS (SELECT 1 AS id) SELECT (WITH c AS (SELECT 2 AS id) SELECT max(id) FROM c) AS m`, "2", ""},
	{"i1603/corrCte", `WITH s AS (SELECT id AS g FROM cm_big) SELECT t.id FROM cm_p t WHERE (SELECT min(g) FROM s WHERE g > t.id) = t.id + 1 ORDER BY 1`, "1; 2; 3", ""},
	{"i1606/existsOwnWith", `WITH d AS (SELECT 1 AS k) SELECT t.id FROM cm_p t WHERE EXISTS (WITH d AS (SELECT 2 AS k) SELECT 1 FROM d WHERE d.k = t.id) ORDER BY 1`, "2", ""},
	{"i1606/inOwnWith", `WITH d AS (SELECT 1 AS k) SELECT t.id FROM cm_p t WHERE t.id IN (WITH d AS (SELECT 2 AS k UNION ALL SELECT 3) SELECT k FROM d WHERE k >= t.id) ORDER BY 1`, "2; 3", ""},
	{"i1606/ownWithDiffSchema", `WITH d AS (SELECT 1 AS k) SELECT t.id, (WITH d AS (SELECT 5 AS z) SELECT z + t.id FROM d) AS m FROM cm_p t ORDER BY 1`, "1,6; 2,7; 3,8", ""},
}
