package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A LATERAL BODY, A CORRELATED SUBQUERY AND A WINDOW SEE THE ROWS AND THE
// OUTER VALUES POSTGRESQL GIVES THEM — #1019, #1111, #1045, #1028, on FIVE
// ARMS.
//
// This is arc L1's table and it is enumerated rather than sampled, because
// every defect it found was invisible to every existing gate:
//
//	{LATERAL (correlated / uncorrelated / grouped), scalar subquery,
//	 EXISTS body, IN body}
//	x {where the outer reference sits, or what the body holds: WHERE, the
//	   SELECT list, a window ARGUMENT, a window PARTITION BY / ORDER BY, a
//	   frame, a CTE in the body, ORDER BY ... LIMIT / OFFSET in the body, an
//	   aggregate in the body}
//	x {INNER, LEFT, comma, a join BELOW the lateral, a star over it}
//	x {single, spilled512k, dag, dag-shuffled, dag-morsel4}
//
// EVERY want is live PostgreSQL 17.11 (postgres:17-alpine, --locale=C) over
// rows identical to this package's `lat_ord` / `lat_item` fixtures, measured
// BEFORE any code changed and re-measured at the tip. What the arc's fixes
// moved, in cells of this table:
//
//	#1019  19 cells — a correlated LATERAL's own ORDER BY / LIMIT / OFFSET
//	       applied to the WHOLE inner relation instead of per outer row, so
//	       the top-N-per-group idiom answered one row for PostgreSQL's two,
//	       silently, on every arm. The bound now travels with the correlation
//	       key as a per-key top-N.
//	#1111   2 cells (and the issue's own DECIMAL shape) — an uncorrelated
//	       LATERAL whose body publishes a name the ENCLOSING relation also
//	       carries: the join had no alias to qualify the duplicate by, DROPPED
//	       it, and the reference bound the outer row's own column.
//	 new    6 cells — an outer reference OUTSIDE the body's WHERE, which the
//	       decorrelation does not carry: NULL, or the inner relation's column
//	       of that name. Now 0A000.
//
// The PINS below are boundaries, not answers. A pin that starts agreeing
// FAILS: the cell is asserted against PostgreSQL — which is recorded for it
// either way — and the pin is deleted.

type l1Case struct{ name, sql string }

func l1LateralCases() []l1Case {
	return []l1Case{
		// THE REFUSAL'S REACH — an outer reference in each clause of a
		// correlated body other than its WHERE. Every one answered a SILENT
		// wrong value before it (lateral_outer_reference.go's table); gated
		// here because a fix's reach is a claim, and an ungated claim is one
		// nobody re-checks.
		{"OUTERREF/selectExpr", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT o.total + i.amount AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"OUTERREF/selectBare", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT o.id AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"OUTERREF/selectCase", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT CASE WHEN o.id > 1 THEN 1 ELSE 0 END AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"OUTERREF/groupBy", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY o.id) s ON true ORDER BY a, m"},
		{"OUTERREF/having", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id HAVING SUM(i.amount) > o.total) s ON true ORDER BY a, m"},
		{"OUTERREF/orderBy", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount * o.total LIMIT 1) s ON true ORDER BY a, m"},
		{"OUTERREF/aggArg", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT SUM(i.amount + o.total) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		// AN UNCORRELATED LATERAL PUBLISHING A NAME THE ENCLOSING RELATION
		// ALSO CARRIES — #1111's shape without its CTE, which was never the
		// mechanism. `collidingName` answered the OUTER row's own `total` on
		// ALL FIVE arms before the alias stamp; the stamp closes the two
		// single-process arms and the three DAG arms still bind the outer
		// column, so it is pinned PER ARM with PostgreSQL's answer beside it.
		// `nonCollidingName` beside it is the discriminator: the same body
		// under an alias the outer relation does not carry was right before
		// and after.
		{"UNCORRLAT/collidingName", "SELECT o.id AS a, t.total AS m FROM lat_ord o, LATERAL (SELECT SUM(i.amount) AS total FROM lat_item i) t ORDER BY a"},
		{"UNCORRLAT/nonCollidingName", "SELECT o.id AS a, t.tot AS m FROM lat_ord o, LATERAL (SELECT SUM(i.amount) AS tot FROM lat_item i) t ORDER BY a"},
		{"UNCORRLAT/issue1111", "SELECT t.dx AS v FROM setopdecja a, LATERAL (WITH c AS (SELECT dx FROM setopdecjb) SELECT SUM(dx) AS dx FROM c) t"},

		// ROUND 2 — the shapes the adversarial review found the table did not
		// hold. `collide*` is a window over a decorrelated LATERAL whose body
		// publishes a name the enclosing relation also carries; `boundLifted*`
		// is a bound BESIDE a lifted non-equality predicate; `twoBounds` is two
		// bounded laterals in one statement; `limitAll` is the spelling the
		// decline list named and the parser cannot read; `winargNested` is
		// #1045's silent constant one level down; `shadow*` is a body whose own
		// FROM item is named like an enclosing relation, with its control.
		{"R2/collideWinBound", "SELECT o.id AS a, s.m AS m, ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY s.m) AS rn FROM lat_ord o JOIN LATERAL (SELECT i.id AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 2) s ON true ORDER BY a, m"},
		{"R2/collideWinNoBound", "SELECT o.id AS a, s.m AS m, ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY s.m) AS rn FROM lat_ord o JOIN LATERAL (SELECT i.id AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"R2/noCollideWinBound", "SELECT o.id AS a, s.m AS m, ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY s.m) AS rn FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 2) s ON true ORDER BY a, m"},
		{"R2/collideWinArg", "SELECT o.id AS a, SUM(s.m) OVER (PARTITION BY o.customer) AS n FROM lat_ord o JOIN LATERAL (SELECT i.id AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 2) s ON true ORDER BY a, n"},
		{"R2/boundLiftedFrac", "SELECT o.id AS a, s.amount AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount FROM lat_item i WHERE i.order_id = o.id AND i.amount < o.total * 0.6 ORDER BY i.amount DESC LIMIT 1) s ON true ORDER BY a, m"},
		{"R2/boundLiftedPlain", "SELECT o.id AS a, s.amount AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount FROM lat_item i WHERE i.order_id = o.id AND i.amount < o.total ORDER BY i.amount DESC LIMIT 1) s ON true ORDER BY a, m"},
		{"R2/twoBounds", "SELECT o.id AS a, s.m AS m, t.p AS p FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 1) s ON true JOIN LATERAL (SELECT j.product AS p FROM lat_item j WHERE j.order_id = o.id ORDER BY j.amount LIMIT 1) t ON true ORDER BY a, m, p"},
		{"R2/limitAll", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount LIMIT ALL) s ON true ORDER BY a, m"},
		{"R2/winargNested", "SELECT u.id AS a, (SELECT MAX(y.v) FROM (SELECT SUM(u.id) OVER () AS v FROM lat_ord x WHERE x.id = 1) y) AS v FROM lat_ord u ORDER BY a"},
		{"R2/shadowSelect", "SELECT x.id AS a, s.m AS m FROM lat_ord x JOIN LATERAL (SELECT x.amount AS m FROM lat_item x WHERE x.order_id = 1) s ON true ORDER BY a, m"},
		{"R2/shadowOrderBy", "SELECT x.id AS a, s.m AS m FROM lat_ord x JOIN LATERAL (SELECT x.amount AS m FROM lat_item x ORDER BY x.amount LIMIT 2) s ON true ORDER BY a, m"},
		{"R2/shadowGroupBy", "SELECT x.id AS a, s.m AS m FROM lat_ord x, LATERAL (SELECT SUM(x.amount) AS m FROM lat_item x GROUP BY x.order_id) s ORDER BY a, m"},
		{"R2/shadowCte", "SELECT x.id AS a, s.m AS m FROM lat_ord x, LATERAL (WITH x AS (SELECT amount FROM lat_item) SELECT SUM(x.amount) AS m FROM x) s ORDER BY a, m"},
		{"R2/ctlNoShadow", "SELECT x.id AS a, s.m AS m FROM lat_ord x, LATERAL (SELECT SUM(w.amount) AS m FROM lat_item w GROUP BY w.order_id) s ORDER BY a, m"},

		// A LIFTED CORRELATED PREDICATE THAT IS NOT AN EQUALITY. It is
		// evaluated over the body's OUTPUT, so the inner column it names has to
		// be published there UNDER THAT NAME — `publishedSource` and
		// `localInequality` are the two spellings where it is, and they answer
		// PostgreSQL on all five arms. The rest are refused; before the
		// refusal they answered ZERO rows on the single-process arms, three
		// NULLs for the aggregated body, and a loud join failure on the DAG.
		{"OUTERREF/whereInequality", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id AND i.amount < o.total) s ON true ORDER BY a, m"},
		{"LIFTED/inequalityAlone", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.amount < o.total) s ON true ORDER BY a, m"},
		{"LIFTED/publishedSource", "SELECT o.id AS a, s.amount AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount FROM lat_item i WHERE i.amount < o.total) s ON true ORDER BY a, m"},
		{"LIFTED/localInequality", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id AND i.amount < 120) s ON true ORDER BY a, m"},
		{"LIFTED/aggregated", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.amount < o.total) s ON true ORDER BY a, m"},
		{"LIFTED/leftArm", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.amount < o.total) s ON true ORDER BY a, m"},
		{"LIFTED/twoColumns", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m, i.id AS k FROM lat_item i WHERE i.amount < o.total AND i.id > 1) s ON true ORDER BY a, m"},
		{"LIFTED/exprBothSides", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.amount * 2 < o.total) s ON true ORDER BY a, m"},

		// CONTROL: a FROM-less body, which is a projection over the outer row
		// (ADR-0021 s1n) and never reaches the decorrelation at all. The other
		// control is `LAT/inner/where`: the outer reference in the WHERE
		// EQUALITY the decorrelation reads, which still answers.
		{"OUTERREF/fromlessControl", "SELECT o.id AS a, s.m AS m FROM lat_ord o, LATERAL (SELECT o.total AS m) s ORDER BY a, m"},
		{"LAT/inner/where", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/inner/selectlist", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT o.total + i.amount AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/inner/winarg", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT SUM(o.total) OVER () AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/inner/winpart", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT SUM(i.amount) OVER (PARTITION BY o.id) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/inner/winord", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT SUM(i.amount) OVER (ORDER BY o.id) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/inner/frame", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT SUM(i.amount) OVER (ORDER BY i.id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/inner/cteUncorr", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (WITH c AS (SELECT amount FROM lat_item) SELECT SUM(c.amount) AS m FROM c) s ON true ORDER BY a, m"},
		{"LAT/inner/cteCorr", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (WITH c AS (SELECT amount, order_id FROM lat_item) SELECT SUM(c.amount) AS m FROM c WHERE c.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/inner/orderLimit", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 1) s ON true ORDER BY a, m"},
		{"LAT/inner/orderLimit2", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 2) s ON true ORDER BY a, m"},
		{"LAT/inner/orderOffset", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC OFFSET 1) s ON true ORDER BY a, m"},
		{"LAT/inner/limitOffset", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount LIMIT 1 OFFSET 1) s ON true ORDER BY a, m"},
		{"LAT/inner/limitOnly", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id LIMIT 1) s ON true ORDER BY a, m"},
		{"LAT/inner/limitBig", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount LIMIT 10) s ON true ORDER BY a, m"},
		{"LAT/inner/agg", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/inner/uncorr", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i) s ON true ORDER BY a, m"},
		{"LAT/inner/uncorrLimit", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i ORDER BY i.amount LIMIT 1) s ON true ORDER BY a, m"},
		{"LAT/inner/grouped", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true ORDER BY a, p, m"},
		{"LAT/inner/groupedLimit", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product ORDER BY i.product LIMIT 1) s ON true ORDER BY a, p, m"},
		{"LAT/inner/groupedOffset", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product ORDER BY i.product OFFSET 1) s ON true ORDER BY a, p, m"},
		{"LAT/inner/groupedHaving", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product HAVING SUM(i.amount) > 60) s ON true ORDER BY a, p, m"},
		{"LAT/left/where", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/left/selectlist", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT o.total + i.amount AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/left/winarg", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT SUM(o.total) OVER () AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/left/winpart", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT SUM(i.amount) OVER (PARTITION BY o.id) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/left/winord", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT SUM(i.amount) OVER (ORDER BY o.id) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/left/frame", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT SUM(i.amount) OVER (ORDER BY i.id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/left/cteUncorr", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (WITH c AS (SELECT amount FROM lat_item) SELECT SUM(c.amount) AS m FROM c) s ON true ORDER BY a, m"},
		{"LAT/left/cteCorr", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (WITH c AS (SELECT amount, order_id FROM lat_item) SELECT SUM(c.amount) AS m FROM c WHERE c.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/left/orderLimit", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 1) s ON true ORDER BY a, m"},
		{"LAT/left/orderLimit2", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 2) s ON true ORDER BY a, m"},
		{"LAT/left/orderOffset", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC OFFSET 1) s ON true ORDER BY a, m"},
		{"LAT/left/limitOffset", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount LIMIT 1 OFFSET 1) s ON true ORDER BY a, m"},
		{"LAT/left/limitOnly", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id LIMIT 1) s ON true ORDER BY a, m"},
		{"LAT/left/limitBig", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount LIMIT 10) s ON true ORDER BY a, m"},
		{"LAT/left/agg", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/left/uncorr", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i) s ON true ORDER BY a, m"},
		{"LAT/left/uncorrLimit", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT i.amount AS m FROM lat_item i ORDER BY i.amount LIMIT 1) s ON true ORDER BY a, m"},
		{"LAT/left/grouped", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true ORDER BY a, p, m"},
		{"LAT/left/groupedLimit", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product ORDER BY i.product LIMIT 1) s ON true ORDER BY a, p, m"},
		{"LAT/left/groupedOffset", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product ORDER BY i.product OFFSET 1) s ON true ORDER BY a, p, m"},
		{"LAT/left/groupedHaving", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product HAVING SUM(i.amount) > 60) s ON true ORDER BY a, p, m"},
		{"LAT/comma/where", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id) s ORDER BY a, m"},
		{"LAT/comma/selectlist", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT o.total + i.amount AS m FROM lat_item i WHERE i.order_id = o.id) s ORDER BY a, m"},
		{"LAT/comma/winarg", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT SUM(o.total) OVER () AS m FROM lat_item i WHERE i.order_id = o.id) s ORDER BY a, m"},
		{"LAT/comma/winpart", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT SUM(i.amount) OVER (PARTITION BY o.id) AS m FROM lat_item i WHERE i.order_id = o.id) s ORDER BY a, m"},
		{"LAT/comma/winord", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT SUM(i.amount) OVER (ORDER BY o.id) AS m FROM lat_item i WHERE i.order_id = o.id) s ORDER BY a, m"},
		{"LAT/comma/frame", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT SUM(i.amount) OVER (ORDER BY i.id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS m FROM lat_item i WHERE i.order_id = o.id) s ORDER BY a, m"},
		{"LAT/comma/cteUncorr", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (WITH c AS (SELECT amount FROM lat_item) SELECT SUM(c.amount) AS m FROM c) s ORDER BY a, m"},
		{"LAT/comma/cteCorr", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (WITH c AS (SELECT amount, order_id FROM lat_item) SELECT SUM(c.amount) AS m FROM c WHERE c.order_id = o.id) s ORDER BY a, m"},
		{"LAT/comma/orderLimit", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 1) s ORDER BY a, m"},
		{"LAT/comma/orderLimit2", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 2) s ORDER BY a, m"},
		{"LAT/comma/orderOffset", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC OFFSET 1) s ORDER BY a, m"},
		{"LAT/comma/limitOffset", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount LIMIT 1 OFFSET 1) s ORDER BY a, m"},
		{"LAT/comma/limitOnly", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id LIMIT 1) s ORDER BY a, m"},
		{"LAT/comma/limitBig", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount LIMIT 10) s ORDER BY a, m"},
		{"LAT/comma/agg", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id) s ORDER BY a, m"},
		{"LAT/comma/uncorr", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i) s ORDER BY a, m"},
		{"LAT/comma/uncorrLimit", "SELECT o.id AS a, s.m AS m FROM lat_ord o , LATERAL (SELECT i.amount AS m FROM lat_item i ORDER BY i.amount LIMIT 1) s ORDER BY a, m"},
		{"LAT/comma/grouped", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o , LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ORDER BY a, p, m"},
		{"LAT/comma/groupedLimit", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o , LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product ORDER BY i.product LIMIT 1) s ORDER BY a, p, m"},
		{"LAT/comma/groupedOffset", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o , LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product ORDER BY i.product OFFSET 1) s ORDER BY a, p, m"},
		{"LAT/comma/groupedHaving", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o , LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product HAVING SUM(i.amount) > 60) s ORDER BY a, p, m"},
		{"LAT/joinInner/where", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/joinInner/orderLimit", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 1) s ON true ORDER BY a, m"},
		{"LAT/joinInner/agg", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/joinInner/cteUncorr", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (WITH c AS (SELECT amount FROM lat_item) SELECT SUM(c.amount) AS m FROM c) s ON true ORDER BY a, m"},
		{"LAT/joinInner/cteCorr", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (WITH c AS (SELECT amount, order_id FROM lat_item) SELECT SUM(c.amount) AS m FROM c WHERE c.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/joinInner/winarg", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (SELECT SUM(o.total) OVER () AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/joinInner/grouped", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true ORDER BY a, p, m"},
		{"LAT/joinInner/groupedLimit", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product ORDER BY i.product LIMIT 1) s ON true ORDER BY a, p, m"},
		{"LAT/joinLeft/where", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/joinLeft/orderLimit", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 1) s ON true ORDER BY a, m"},
		{"LAT/joinLeft/agg", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/joinLeft/cteUncorr", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (WITH c AS (SELECT amount FROM lat_item) SELECT SUM(c.amount) AS m FROM c) s ON true ORDER BY a, m"},
		{"LAT/joinLeft/cteCorr", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (WITH c AS (SELECT amount, order_id FROM lat_item) SELECT SUM(c.amount) AS m FROM c WHERE c.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/joinLeft/winarg", "SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (SELECT SUM(o.total) OVER () AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY a, m"},
		{"LAT/joinLeft/grouped", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true ORDER BY a, p, m"},
		{"LAT/joinLeft/groupedLimit", "SELECT o.id AS a, s.p AS p, s.m AS m FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product ORDER BY i.product LIMIT 1) s ON true ORDER BY a, p, m"},
		{"LAT/star/where", "SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY 1, 2, 3, 4"},
		{"LAT/qstar/where", "SELECT s.* FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY 1"},
		{"LAT/star/orderLimit", "SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 1) s ON true ORDER BY 1, 2, 3, 4"},
		{"LAT/qstar/orderLimit", "SELECT s.* FROM lat_ord o JOIN LATERAL (SELECT i.amount AS m FROM lat_item i WHERE i.order_id = o.id ORDER BY i.amount DESC LIMIT 1) s ON true ORDER BY 1"},
		{"LAT/star/agg", "SELECT * FROM lat_ord o JOIN LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY 1, 2, 3, 4"},
		{"LAT/qstar/agg", "SELECT s.* FROM lat_ord o JOIN LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true ORDER BY 1"},
		{"LAT/star/grouped", "SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true ORDER BY 1, 2, 3, 4"},
		{"LAT/qstar/grouped", "SELECT s.* FROM lat_ord o JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true ORDER BY 1"},
		{"LAT/star/groupedLimit", "SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product ORDER BY i.product LIMIT 1) s ON true ORDER BY 1, 2, 3, 4"},
		{"LAT/qstar/groupedLimit", "SELECT s.* FROM lat_ord o JOIN LATERAL (SELECT i.product AS p, SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product ORDER BY i.product LIMIT 1) s ON true ORDER BY 1"},
		{"SCALAR/noJoin/where", "SELECT o.id AS a, (SELECT MAX(z.amount) FROM lat_item z WHERE z.order_id = o.id) AS v FROM lat_ord o ORDER BY a, v"},
		{"SCALAR/noJoin/winarg", "SELECT o.id AS a, (SELECT 1 + SUM(o.id) OVER () FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o ORDER BY a, v"},
		{"SCALAR/noJoin/winargBare", "SELECT o.id AS a, (SELECT SUM(o.id) OVER () FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o ORDER BY a, v"},
		{"SCALAR/noJoin/winpart", "SELECT o.id AS a, (SELECT MAX(z.amount) OVER (PARTITION BY o.id) FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o ORDER BY a, v"},
		{"SCALAR/noJoin/winord", "SELECT o.id AS a, (SELECT MAX(z.amount) OVER (ORDER BY o.id) FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o ORDER BY a, v"},
		{"SCALAR/noJoin/winpart2", "SELECT o.id AS a, (SELECT COUNT(*) OVER (PARTITION BY o.id) FROM lat_item z WHERE z.order_id = 1) AS v FROM lat_ord o ORDER BY a, v"},
		{"SCALAR/noJoin/winUncorr", "SELECT o.id AS a, (SELECT MAX(z.amount) OVER () FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o ORDER BY a, v"},
		{"SCALAR/noJoin/cte", "SELECT o.id AS a, (WITH c AS (SELECT amount, order_id FROM lat_item) SELECT MAX(c.amount) FROM c WHERE c.order_id = o.id) AS v FROM lat_ord o ORDER BY a, v"},
		{"SCALAR/noJoin/orderLimit", "SELECT o.id AS a, (SELECT z.amount FROM lat_item z WHERE z.order_id = o.id ORDER BY z.amount DESC LIMIT 1) AS v FROM lat_ord o ORDER BY a, v"},
		{"SCALAR/noJoin/agg", "SELECT o.id AS a, (SELECT COUNT(*) FROM lat_item z WHERE z.order_id = o.id) AS v FROM lat_ord o ORDER BY a, v"},
		{"SCALAR/noJoin/winInOrder", "SELECT o.id AS a, (SELECT z.amount FROM lat_item z WHERE z.order_id = o.id ORDER BY ROW_NUMBER() OVER () LIMIT 1) AS v FROM lat_ord o ORDER BY a, v"},
		{"SCALAR/inner/where", "SELECT o.id AS a, (SELECT MAX(z.amount) FROM lat_item z WHERE z.order_id = o.id) AS v FROM lat_ord o JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/inner/winarg", "SELECT o.id AS a, (SELECT 1 + SUM(o.id) OVER () FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/inner/winargBare", "SELECT o.id AS a, (SELECT SUM(o.id) OVER () FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/inner/winpart", "SELECT o.id AS a, (SELECT MAX(z.amount) OVER (PARTITION BY o.id) FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/inner/winord", "SELECT o.id AS a, (SELECT MAX(z.amount) OVER (ORDER BY o.id) FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/inner/winpart2", "SELECT o.id AS a, (SELECT COUNT(*) OVER (PARTITION BY o.id) FROM lat_item z WHERE z.order_id = 1) AS v FROM lat_ord o JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/inner/winUncorr", "SELECT o.id AS a, (SELECT MAX(z.amount) OVER () FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/inner/cte", "SELECT o.id AS a, (WITH c AS (SELECT amount, order_id FROM lat_item) SELECT MAX(c.amount) FROM c WHERE c.order_id = o.id) AS v FROM lat_ord o JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/inner/orderLimit", "SELECT o.id AS a, (SELECT z.amount FROM lat_item z WHERE z.order_id = o.id ORDER BY z.amount DESC LIMIT 1) AS v FROM lat_ord o JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/inner/agg", "SELECT o.id AS a, (SELECT COUNT(*) FROM lat_item z WHERE z.order_id = o.id) AS v FROM lat_ord o JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/inner/winInOrder", "SELECT o.id AS a, (SELECT z.amount FROM lat_item z WHERE z.order_id = o.id ORDER BY ROW_NUMBER() OVER () LIMIT 1) AS v FROM lat_ord o JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/left/where", "SELECT o.id AS a, (SELECT MAX(z.amount) FROM lat_item z WHERE z.order_id = o.id) AS v FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/left/winarg", "SELECT o.id AS a, (SELECT 1 + SUM(o.id) OVER () FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/left/winargBare", "SELECT o.id AS a, (SELECT SUM(o.id) OVER () FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/left/winpart", "SELECT o.id AS a, (SELECT MAX(z.amount) OVER (PARTITION BY o.id) FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/left/winord", "SELECT o.id AS a, (SELECT MAX(z.amount) OVER (ORDER BY o.id) FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/left/winpart2", "SELECT o.id AS a, (SELECT COUNT(*) OVER (PARTITION BY o.id) FROM lat_item z WHERE z.order_id = 1) AS v FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/left/winUncorr", "SELECT o.id AS a, (SELECT MAX(z.amount) OVER () FROM lat_item z WHERE z.id = 1) AS v FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/left/cte", "SELECT o.id AS a, (WITH c AS (SELECT amount, order_id FROM lat_item) SELECT MAX(c.amount) FROM c WHERE c.order_id = o.id) AS v FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/left/orderLimit", "SELECT o.id AS a, (SELECT z.amount FROM lat_item z WHERE z.order_id = o.id ORDER BY z.amount DESC LIMIT 1) AS v FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/left/agg", "SELECT o.id AS a, (SELECT COUNT(*) FROM lat_item z WHERE z.order_id = o.id) AS v FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"SCALAR/left/winInOrder", "SELECT o.id AS a, (SELECT z.amount FROM lat_item z WHERE z.order_id = o.id ORDER BY ROW_NUMBER() OVER () LIMIT 1) AS v FROM lat_ord o LEFT JOIN lat_item j ON j.order_id = o.id ORDER BY a, v"},
		{"EXISTS/noJoin/plain", "SELECT o.id AS a FROM lat_ord o WHERE EXISTS (SELECT 1 FROM lat_item z WHERE z.order_id = o.id) ORDER BY a"},
		{"EXISTS/inner/plain", "SELECT o.id AS a, j.id AS b FROM lat_ord o JOIN lat_item j ON j.order_id = o.id WHERE EXISTS (SELECT 1 FROM lat_item z WHERE z.order_id = o.id) ORDER BY a, b"},
		{"EXISTS/noJoin/winarg", "SELECT o.id AS a FROM lat_ord o WHERE EXISTS (SELECT 1 FROM lat_item z WHERE z.order_id = o.id AND SUM(o.id) OVER () > 0) ORDER BY a"},
		{"EXISTS/inner/winarg", "SELECT o.id AS a, j.id AS b FROM lat_ord o JOIN lat_item j ON j.order_id = o.id WHERE EXISTS (SELECT 1 FROM lat_item z WHERE z.order_id = o.id AND SUM(o.id) OVER () > 0) ORDER BY a, b"},
		{"EXISTS/noJoin/winsel", "SELECT o.id AS a FROM lat_ord o WHERE EXISTS (SELECT SUM(o.id) OVER () FROM lat_item z WHERE z.order_id = o.id) ORDER BY a"},
		{"EXISTS/inner/winsel", "SELECT o.id AS a, j.id AS b FROM lat_ord o JOIN lat_item j ON j.order_id = o.id WHERE EXISTS (SELECT SUM(o.id) OVER () FROM lat_item z WHERE z.order_id = o.id) ORDER BY a, b"},
		{"EXISTS/noJoin/winord", "SELECT o.id AS a FROM lat_ord o WHERE EXISTS (SELECT 1 FROM lat_item z WHERE z.order_id = o.id ORDER BY ROW_NUMBER() OVER ()) ORDER BY a"},
		{"EXISTS/inner/winord", "SELECT o.id AS a, j.id AS b FROM lat_ord o JOIN lat_item j ON j.order_id = o.id WHERE EXISTS (SELECT 1 FROM lat_item z WHERE z.order_id = o.id ORDER BY ROW_NUMBER() OVER ()) ORDER BY a, b"},
		{"EXISTS/noJoin/cte", "SELECT o.id AS a FROM lat_ord o WHERE EXISTS (WITH c AS (SELECT order_id FROM lat_item) SELECT 1 FROM c WHERE c.order_id = o.id) ORDER BY a"},
		{"EXISTS/inner/cte", "SELECT o.id AS a, j.id AS b FROM lat_ord o JOIN lat_item j ON j.order_id = o.id WHERE EXISTS (WITH c AS (SELECT order_id FROM lat_item) SELECT 1 FROM c WHERE c.order_id = o.id) ORDER BY a, b"},
		{"EXISTS/noJoin/limit", "SELECT o.id AS a FROM lat_ord o WHERE EXISTS (SELECT 1 FROM lat_item z WHERE z.order_id = o.id ORDER BY z.amount DESC LIMIT 1) ORDER BY a"},
		{"EXISTS/inner/limit", "SELECT o.id AS a, j.id AS b FROM lat_ord o JOIN lat_item j ON j.order_id = o.id WHERE EXISTS (SELECT 1 FROM lat_item z WHERE z.order_id = o.id ORDER BY z.amount DESC LIMIT 1) ORDER BY a, b"},
		{"IN/noJoin/plain", "SELECT o.id AS a FROM lat_ord o WHERE o.id IN (SELECT z.order_id FROM lat_item z WHERE z.order_id = o.id) ORDER BY a"},
		{"IN/noJoin/winsel", "SELECT o.id AS a FROM lat_ord o WHERE o.id IN (SELECT SUM(o.id) OVER () FROM lat_item z WHERE z.order_id = o.id) ORDER BY a"},
		{"IN/noJoin/cte", "SELECT o.id AS a FROM lat_ord o WHERE o.id IN (WITH c AS (SELECT order_id FROM lat_item) SELECT c.order_id FROM c WHERE c.order_id = o.id) ORDER BY a"},
		{"IN/noJoin/limit", "SELECT o.id AS a FROM lat_ord o WHERE o.id IN (SELECT z.order_id FROM lat_item z WHERE z.order_id = o.id ORDER BY z.order_id LIMIT 1) ORDER BY a"},
	}
}

// The refusal classes this table pins, each by a substring of the sentence it
// says. A cell lists every class its five arms produce, because a refusal
// raised at plan time on the single-process arms is raised by the DAG's own
// route-local check on the distributed ones and the two say different things.
const (
	// A window inside a CORRELATED LATERAL body. The correlation is lowered
	// into a join, so the window would be computed over the whole inner
	// relation rather than per outer row (arc J1, v0.18.60). PostgreSQL
	// answers all fourteen of these.
	l1WindowInLateral = `window function "m" inside a LATERAL subquery`
	// A correlated scalar / IN subquery holding a window call. The per-row
	// re-run rebuilds the subquery's TEXT and `WindowFuncNode.String()`
	// renders `OVER (...)` — three literal dots — so the rebuilt statement
	// does not parse (ADR-0021 s1m, #1045). Closing it is a faithful
	// rendering of the OVER clause, which is a defect of its own.
	l1WindowInSubquery    = `holds a window function`
	l1WindowInSubqueryDAG = `requires per-row execution, which the stage DAG does not support`
	// The same fact reached through a clause `HoldsWindowCall` does not read:
	// a window in the correlated body's own ORDER BY is re-emitted as TEXT
	// and the rebuild dies in the parser instead. Same root cause, different
	// door — recorded separately so the day the rendering is fixed, both
	// move together.
	l1OverClauseRebuild = `expected ')' after OVER clause`
	// `ORDER BY <ordinal>` over a `SELECT *` whose FROM is a JOIN — which a
	// LATERAL always is once it is lowered. The star is not expanded over a
	// join, so there is no position to count. It reproduces with an ordinary
	// join in place of the lateral and is not a lateral defect.
	l1StarOrdinal = `was not expanded into a column list`
	// A LEFT JOIN LATERAL over an UNCORRELATED body: the join has no keys,
	// and the physical planner, the DAG's SELECT-list check and the join
	// operator each refuse it in their own words. PostgreSQL answers the
	// cross product with no padding. The INNER and comma spellings of the
	// same body answer.
	l1KeylessLeftJoin     = `could not extract join keys from`
	l1KeylessLeftJoinDAG  = `no stage computes requires single-process execution`
	l1KeylessLeftJoinTask = `LeftKeys and RightKeys required`
	// A QUALIFIED star over a correlated LATERAL whose own bound is not
	// applied per outer row: the body's COLUMNS are knowable, its ROW COUNT is
	// not the one the query wrote, so the one consumer that must state the
	// relation declines (#1079, arc O2).
	l1QStarBoundNotPerRow = `column "s.*" does not exist in`
	// `LIMIT ALL` — which the decline list named as a shape the per-key
	// rewrite declines — does not PARSE. PostgreSQL accepts it and means "no
	// bound"; this parser wants a number (round 2, P1).
	l1LimitAllUnparsed = `expected number after LIMIT`
	// An outer reference in a clause of a correlated body OTHER than its
	// WHERE. The decorrelation carries the outer row into that clause and no
	// other, so the reference bound the inner relation's column of the same
	// name, or nothing (lateral_outer_reference.go).
	l1OuterRefOutsideWhere = `from the enclosing query`
	// A lifted correlated predicate that is not an equality, naming an inner
	// column the body does not publish under that name
	// (lateral_correlated_refs.go).
	l1LiftedRefNotPublished = `is not an equality and reads`
)

// l1Postgres is PostgreSQL 17.11's answer for every cell, rendered by
// r1RenderRows (a sorted ROW SET, so a legal ordering difference between arms
// is never read as a wrong answer).
var l1Postgres = map[string]string{
	"R2/shadowSelect":            "rows=6 1,100 | 1,50 | 2,100 | 2,50 | 3,100 | 3,50",
	"R2/shadowOrderBy":           "rows=6 1,50 | 1,75 | 2,50 | 2,75 | 3,50 | 3,75",
	"R2/collideWinBound":         "rows=4 1,1,1 | 1,2,2 | 2,3,1 | 2,4,2",
	"R2/collideWinNoBound":       "rows=4 1,1,1 | 1,2,2 | 2,3,1 | 2,4,2",
	"R2/noCollideWinBound":       "rows=4 1,100,2 | 1,50,1 | 2,125,2 | 2,75,1",
	"R2/collideWinArg":           "rows=4 1,3 | 1,3 | 2,7 | 2,7",
	"R2/boundLiftedFrac":         "rows=2 1,50 | 2,75",
	"R2/boundLiftedPlain":        "rows=2 1,100 | 2,125",
	"R2/twoBounds":               "rows=2 1,100,Widget | 2,125,Widget",
	"R2/limitAll":                "rows=4 1,100 | 1,50 | 2,125 | 2,75",
	"R2/winargNested":            "rows=3 1,1 | 2,2 | 3,3",
	"R2/shadowGroupBy":           "rows=6 1,150 | 1,200 | 2,150 | 2,200 | 3,150 | 3,200",
	"R2/shadowCte":               "rows=3 1,350 | 2,350 | 3,350",
	"R2/ctlNoShadow":             "rows=6 1,150 | 1,200 | 2,150 | 2,200 | 3,150 | 3,200",
	"UNCORRLAT/collidingName":    "rows=3 1,350 | 2,350 | 3,350",
	"UNCORRLAT/nonCollidingName": "rows=3 1,350 | 2,350 | 3,350",
	"UNCORRLAT/issue1111":        "rows=4 51.0000 | 51.0000 | 51.0000 | 51.0000",
	"OUTERREF/selectExpr":        "rows=4 1,200 | 1,250 | 2,275 | 2,325",
	"OUTERREF/selectBare":        "rows=4 1,1 | 1,1 | 2,2 | 2,2",
	"OUTERREF/selectCase":        "rows=4 1,0 | 1,0 | 2,1 | 2,1",
	"OUTERREF/groupBy":           "rows=2 1,150 | 2,200",
	"OUTERREF/having":            "rows=0 ",
	"OUTERREF/orderBy":           "rows=2 1,50 | 2,75",
	"OUTERREF/aggArg":            "rows=3 1,450 | 2,600 | 3,NULL",
	"OUTERREF/fromlessControl":   "rows=3 1,150 | 2,200 | 3,0",
	"OUTERREF/whereInequality":   "rows=4 1,100 | 1,50 | 2,125 | 2,75",
	"LIFTED/inequalityAlone":     "rows=8 1,100 | 1,125 | 1,50 | 1,75 | 2,100 | 2,125 | 2,50 | 2,75",
	"LIFTED/publishedSource":     "rows=8 1,100 | 1,125 | 1,50 | 1,75 | 2,100 | 2,125 | 2,50 | 2,75",
	"LIFTED/localInequality":     "rows=3 1,100 | 1,50 | 2,75",
	"LIFTED/aggregated":          "rows=3 1,350 | 2,350 | 3,NULL",
	"LIFTED/leftArm":             "rows=9 1,100 | 1,125 | 1,50 | 1,75 | 2,100 | 2,125 | 2,50 | 2,75 | 3,NULL",
	"LIFTED/twoColumns":          "rows=6 1,100 | 1,125 | 1,75 | 2,100 | 2,125 | 2,75",
	"LIFTED/exprBothSides":       "rows=3 1,50 | 2,50 | 2,75",
	"LAT/inner/where":            "rows=4 1,100 | 1,50 | 2,125 | 2,75",
	"LAT/inner/selectlist":       "rows=4 1,200 | 1,250 | 2,275 | 2,325",
	"LAT/inner/winarg":           "rows=4 1,300 | 1,300 | 2,400 | 2,400",
	"LAT/inner/winpart":          "rows=4 1,150 | 1,150 | 2,200 | 2,200",
	"LAT/inner/winord":           "rows=4 1,150 | 1,150 | 2,200 | 2,200",
	"LAT/inner/frame":            "rows=4 1,150 | 1,50 | 2,200 | 2,75",
	"LAT/inner/cteUncorr":        "rows=3 1,350 | 2,350 | 3,350",
	"LAT/inner/cteCorr":          "rows=3 1,150 | 2,200 | 3,NULL",
	"LAT/inner/orderLimit":       "rows=2 1,100 | 2,125",
	"LAT/inner/orderLimit2":      "rows=4 1,100 | 1,50 | 2,125 | 2,75",
	"LAT/inner/orderOffset":      "rows=2 1,50 | 2,75",
	"LAT/inner/limitOffset":      "rows=2 1,100 | 2,125",
	"LAT/inner/limitOnly":        "rows=2 1,50 | 2,75",
	"LAT/inner/limitBig":         "rows=4 1,100 | 1,50 | 2,125 | 2,75",
	"LAT/inner/agg":              "rows=3 1,150 | 2,200 | 3,NULL",
	"LAT/inner/uncorr":           "rows=3 1,350 | 2,350 | 3,350",
	"LAT/inner/uncorrLimit":      "rows=3 1,50 | 2,50 | 3,50",
	"LAT/inner/grouped":          "rows=4 1,Gadget,100 | 1,Widget,50 | 2,Doohickey,125 | 2,Widget,75",
	"LAT/inner/groupedLimit":     "rows=2 1,Gadget,100 | 2,Doohickey,125",
	"LAT/inner/groupedOffset":    "rows=2 1,Widget,50 | 2,Widget,75",
	"LAT/inner/groupedHaving":    "rows=3 1,Gadget,100 | 2,Doohickey,125 | 2,Widget,75",
	"LAT/left/where":             "rows=5 1,100 | 1,50 | 2,125 | 2,75 | 3,NULL",
	"LAT/left/selectlist":        "rows=5 1,200 | 1,250 | 2,275 | 2,325 | 3,NULL",
	"LAT/left/winarg":            "rows=5 1,300 | 1,300 | 2,400 | 2,400 | 3,NULL",
	"LAT/left/winpart":           "rows=5 1,150 | 1,150 | 2,200 | 2,200 | 3,NULL",
	"LAT/left/winord":            "rows=5 1,150 | 1,150 | 2,200 | 2,200 | 3,NULL",
	"LAT/left/frame":             "rows=5 1,150 | 1,50 | 2,200 | 2,75 | 3,NULL",
	"LAT/left/cteUncorr":         "rows=3 1,350 | 2,350 | 3,350",
	"LAT/left/cteCorr":           "rows=3 1,150 | 2,200 | 3,NULL",
	"LAT/left/orderLimit":        "rows=3 1,100 | 2,125 | 3,NULL",
	"LAT/left/orderLimit2":       "rows=5 1,100 | 1,50 | 2,125 | 2,75 | 3,NULL",
	"LAT/left/orderOffset":       "rows=3 1,50 | 2,75 | 3,NULL",
	"LAT/left/limitOffset":       "rows=3 1,100 | 2,125 | 3,NULL",
	"LAT/left/limitOnly":         "rows=3 1,50 | 2,75 | 3,NULL",
	"LAT/left/limitBig":          "rows=5 1,100 | 1,50 | 2,125 | 2,75 | 3,NULL",
	"LAT/left/agg":               "rows=3 1,150 | 2,200 | 3,NULL",
	"LAT/left/uncorr":            "rows=3 1,350 | 2,350 | 3,350",
	"LAT/left/uncorrLimit":       "rows=3 1,50 | 2,50 | 3,50",
	"LAT/left/grouped":           "rows=5 1,Gadget,100 | 1,Widget,50 | 2,Doohickey,125 | 2,Widget,75 | 3,NULL,NULL",
	"LAT/left/groupedLimit":      "rows=3 1,Gadget,100 | 2,Doohickey,125 | 3,NULL,NULL",
	"LAT/left/groupedOffset":     "rows=3 1,Widget,50 | 2,Widget,75 | 3,NULL,NULL",
	"LAT/left/groupedHaving":     "rows=4 1,Gadget,100 | 2,Doohickey,125 | 2,Widget,75 | 3,NULL,NULL",
	"LAT/comma/where":            "rows=4 1,100 | 1,50 | 2,125 | 2,75",
	"LAT/comma/selectlist":       "rows=4 1,200 | 1,250 | 2,275 | 2,325",
	"LAT/comma/winarg":           "rows=4 1,300 | 1,300 | 2,400 | 2,400",
	"LAT/comma/winpart":          "rows=4 1,150 | 1,150 | 2,200 | 2,200",
	"LAT/comma/winord":           "rows=4 1,150 | 1,150 | 2,200 | 2,200",
	"LAT/comma/frame":            "rows=4 1,150 | 1,50 | 2,200 | 2,75",
	"LAT/comma/cteUncorr":        "rows=3 1,350 | 2,350 | 3,350",
	"LAT/comma/cteCorr":          "rows=3 1,150 | 2,200 | 3,NULL",
	"LAT/comma/orderLimit":       "rows=2 1,100 | 2,125",
	"LAT/comma/orderLimit2":      "rows=4 1,100 | 1,50 | 2,125 | 2,75",
	"LAT/comma/orderOffset":      "rows=2 1,50 | 2,75",
	"LAT/comma/limitOffset":      "rows=2 1,100 | 2,125",
	"LAT/comma/limitOnly":        "rows=2 1,50 | 2,75",
	"LAT/comma/limitBig":         "rows=4 1,100 | 1,50 | 2,125 | 2,75",
	"LAT/comma/agg":              "rows=3 1,150 | 2,200 | 3,NULL",
	"LAT/comma/uncorr":           "rows=3 1,350 | 2,350 | 3,350",
	"LAT/comma/uncorrLimit":      "rows=3 1,50 | 2,50 | 3,50",
	"LAT/comma/grouped":          "rows=4 1,Gadget,100 | 1,Widget,50 | 2,Doohickey,125 | 2,Widget,75",
	"LAT/comma/groupedLimit":     "rows=2 1,Gadget,100 | 2,Doohickey,125",
	"LAT/comma/groupedOffset":    "rows=2 1,Widget,50 | 2,Widget,75",
	"LAT/comma/groupedHaving":    "rows=3 1,Gadget,100 | 2,Doohickey,125 | 2,Widget,75",
	"LAT/joinInner/where":        "rows=8 1,100 | 1,100 | 1,50 | 1,50 | 2,125 | 2,125 | 2,75 | 2,75",
	"LAT/joinInner/orderLimit":   "rows=4 1,100 | 1,100 | 2,125 | 2,125",
	"LAT/joinInner/agg":          "rows=4 1,150 | 1,150 | 2,200 | 2,200",
	"LAT/joinInner/cteUncorr":    "rows=4 1,350 | 1,350 | 2,350 | 2,350",
	"LAT/joinInner/cteCorr":      "rows=4 1,150 | 1,150 | 2,200 | 2,200",
	"LAT/joinInner/winarg":       "rows=8 1,300 | 1,300 | 1,300 | 1,300 | 2,400 | 2,400 | 2,400 | 2,400",
	"LAT/joinInner/grouped":      "rows=8 1,Gadget,100 | 1,Gadget,100 | 1,Widget,50 | 1,Widget,50 | 2,Doohickey,125 | 2,Doohickey,125 | 2,Widget,75 | 2,Widget,75",
	"LAT/joinInner/groupedLimit": "rows=4 1,Gadget,100 | 1,Gadget,100 | 2,Doohickey,125 | 2,Doohickey,125",
	"LAT/joinLeft/where":         "rows=8 1,100 | 1,100 | 1,50 | 1,50 | 2,125 | 2,125 | 2,75 | 2,75",
	"LAT/joinLeft/orderLimit":    "rows=4 1,100 | 1,100 | 2,125 | 2,125",
	"LAT/joinLeft/agg":           "rows=5 1,150 | 1,150 | 2,200 | 2,200 | 3,NULL",
	"LAT/joinLeft/cteUncorr":     "rows=5 1,350 | 1,350 | 2,350 | 2,350 | 3,350",
	"LAT/joinLeft/cteCorr":       "rows=5 1,150 | 1,150 | 2,200 | 2,200 | 3,NULL",
	"LAT/joinLeft/winarg":        "rows=8 1,300 | 1,300 | 1,300 | 1,300 | 2,400 | 2,400 | 2,400 | 2,400",
	"LAT/joinLeft/grouped":       "rows=8 1,Gadget,100 | 1,Gadget,100 | 1,Widget,50 | 1,Widget,50 | 2,Doohickey,125 | 2,Doohickey,125 | 2,Widget,75 | 2,Widget,75",
	"LAT/joinLeft/groupedLimit":  "rows=4 1,Gadget,100 | 1,Gadget,100 | 2,Doohickey,125 | 2,Doohickey,125",
	"LAT/star/where":             "rows=4 1,Alice,150,100 | 1,Alice,150,50 | 2,Bob,200,125 | 2,Bob,200,75",
	"LAT/qstar/where":            "rows=4 100 | 125 | 50 | 75",
	"LAT/star/orderLimit":        "rows=2 1,Alice,150,100 | 2,Bob,200,125",
	"LAT/qstar/orderLimit":       "rows=2 100 | 125",
	"LAT/star/agg":               "rows=3 1,Alice,150,150 | 2,Bob,200,200 | 3,Carol,0,NULL",
	"LAT/qstar/agg":              "rows=3 150 | 200 | NULL",
	"LAT/star/grouped":           "rows=4 1,Alice,150,Gadget,100 | 1,Alice,150,Widget,50 | 2,Bob,200,Doohickey,125 | 2,Bob,200,Widget,75",
	"LAT/qstar/grouped":          "rows=4 Doohickey,125 | Gadget,100 | Widget,50 | Widget,75",
	"LAT/star/groupedLimit":      "rows=2 1,Alice,150,Gadget,100 | 2,Bob,200,Doohickey,125",
	"LAT/qstar/groupedLimit":     "rows=2 Doohickey,125 | Gadget,100",
	"SCALAR/noJoin/where":        "rows=3 1,100 | 2,125 | 3,NULL",
	"SCALAR/noJoin/winarg":       "rows=3 1,2 | 2,3 | 3,4",
	"SCALAR/noJoin/winargBare":   "rows=3 1,1 | 2,2 | 3,3",
	"SCALAR/noJoin/winpart":      "rows=3 1,50 | 2,50 | 3,50",
	"SCALAR/noJoin/winord":       "rows=3 1,50 | 2,50 | 3,50",
	"SCALAR/noJoin/winpart2":     "ERR ERROR: more than one row returned by a subquery used as an expression",
	"SCALAR/noJoin/winUncorr":    "rows=3 1,50 | 2,50 | 3,50",
	"SCALAR/noJoin/cte":          "rows=3 1,100 | 2,125 | 3,NULL",
	"SCALAR/noJoin/orderLimit":   "rows=3 1,100 | 2,125 | 3,NULL",
	"SCALAR/noJoin/agg":          "rows=3 1,2 | 2,2 | 3,0",
	"SCALAR/noJoin/winInOrder":   "rows=3 1,50 | 2,75 | 3,NULL",
	"SCALAR/inner/where":         "rows=4 1,100 | 1,100 | 2,125 | 2,125",
	"SCALAR/inner/winarg":        "rows=4 1,2 | 1,2 | 2,3 | 2,3",
	"SCALAR/inner/winargBare":    "rows=4 1,1 | 1,1 | 2,2 | 2,2",
	"SCALAR/inner/winpart":       "rows=4 1,50 | 1,50 | 2,50 | 2,50",
	"SCALAR/inner/winord":        "rows=4 1,50 | 1,50 | 2,50 | 2,50",
	"SCALAR/inner/winpart2":      "ERR ERROR: more than one row returned by a subquery used as an expression",
	"SCALAR/inner/winUncorr":     "rows=4 1,50 | 1,50 | 2,50 | 2,50",
	"SCALAR/inner/cte":           "rows=4 1,100 | 1,100 | 2,125 | 2,125",
	"SCALAR/inner/orderLimit":    "rows=4 1,100 | 1,100 | 2,125 | 2,125",
	"SCALAR/inner/agg":           "rows=4 1,2 | 1,2 | 2,2 | 2,2",
	"SCALAR/inner/winInOrder":    "rows=4 1,50 | 1,50 | 2,75 | 2,75",
	"SCALAR/left/where":          "rows=5 1,100 | 1,100 | 2,125 | 2,125 | 3,NULL",
	"SCALAR/left/winarg":         "rows=5 1,2 | 1,2 | 2,3 | 2,3 | 3,4",
	"SCALAR/left/winargBare":     "rows=5 1,1 | 1,1 | 2,2 | 2,2 | 3,3",
	"SCALAR/left/winpart":        "rows=5 1,50 | 1,50 | 2,50 | 2,50 | 3,50",
	"SCALAR/left/winord":         "rows=5 1,50 | 1,50 | 2,50 | 2,50 | 3,50",
	"SCALAR/left/winpart2":       "ERR ERROR: more than one row returned by a subquery used as an expression",
	"SCALAR/left/winUncorr":      "rows=5 1,50 | 1,50 | 2,50 | 2,50 | 3,50",
	"SCALAR/left/cte":            "rows=5 1,100 | 1,100 | 2,125 | 2,125 | 3,NULL",
	"SCALAR/left/orderLimit":     "rows=5 1,100 | 1,100 | 2,125 | 2,125 | 3,NULL",
	"SCALAR/left/agg":            "rows=5 1,2 | 1,2 | 2,2 | 2,2 | 3,0",
	"SCALAR/left/winInOrder":     "rows=5 1,50 | 1,50 | 2,75 | 2,75 | 3,NULL",
	"EXISTS/noJoin/plain":        "rows=2 1 | 2",
	"EXISTS/inner/plain":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"EXISTS/noJoin/winarg":       "ERR ERROR: window functions are not allowed in WHERE LINE 1: ...ECT 1 FROM lat_item z WHERE z.order_id = o.id AND SUM(o.id) ... ^",
	"EXISTS/inner/winarg":        "ERR ERROR: window functions are not allowed in WHERE LINE 1: ...ECT 1 FROM lat_item z WHERE z.order_id = o.id AND SUM(o.id) ... ^",
	"EXISTS/noJoin/winsel":       "rows=2 1 | 2",
	"EXISTS/inner/winsel":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"EXISTS/noJoin/winord":       "rows=2 1 | 2",
	"EXISTS/inner/winord":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"EXISTS/noJoin/cte":          "rows=2 1 | 2",
	"EXISTS/inner/cte":           "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"EXISTS/noJoin/limit":        "rows=2 1 | 2",
	"EXISTS/inner/limit":         "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"IN/noJoin/plain":            "rows=2 1 | 2",
	"IN/noJoin/winsel":           "rows=0 ",
	"IN/noJoin/cte":              "rows=2 1 | 2",
	"IN/noJoin/limit":            "rows=2 1 | 2",
}

// l1RefusalPins names the refusal classes a cell's arms may raise. Every arm's
// answer must contain one of them.
var l1RefusalPins = map[string][]string{
	"R2/limitAll":              {l1LimitAllUnparsed},
	"IN/noJoin/winsel":         {l1WindowInSubquery},
	"LAT/comma/frame":          {l1WindowInLateral},
	"LAT/comma/selectlist":     {l1OuterRefOutsideWhere},
	"LAT/comma/winarg":         {l1OuterRefOutsideWhere},
	"LAT/comma/winord":         {l1OuterRefOutsideWhere},
	"LAT/comma/winpart":        {l1OuterRefOutsideWhere},
	"LAT/inner/frame":          {l1WindowInLateral},
	"LAT/inner/selectlist":     {l1OuterRefOutsideWhere},
	"LAT/inner/winarg":         {l1OuterRefOutsideWhere},
	"LAT/inner/winord":         {l1OuterRefOutsideWhere},
	"LAT/inner/winpart":        {l1OuterRefOutsideWhere},
	"LAT/joinInner/winarg":     {l1OuterRefOutsideWhere},
	"LAT/joinLeft/winarg":      {l1OuterRefOutsideWhere},
	"LAT/left/cteUncorr":       {l1KeylessLeftJoin},
	"LAT/left/frame":           {l1WindowInLateral},
	"LAT/left/selectlist":      {l1OuterRefOutsideWhere},
	"LAT/left/uncorr":          {l1KeylessLeftJoin},
	"LAT/left/uncorrLimit":     {l1KeylessLeftJoin, l1KeylessLeftJoinTask},
	"LAT/left/winarg":          {l1OuterRefOutsideWhere},
	"LAT/left/winord":          {l1OuterRefOutsideWhere},
	"LAT/left/winpart":         {l1OuterRefOutsideWhere},
	"LAT/qstar/groupedLimit":   {l1QStarBoundNotPerRow},
	"LAT/qstar/orderLimit":     {l1QStarBoundNotPerRow},
	"LAT/star/agg":             {l1StarOrdinal},
	"LAT/star/grouped":         {l1StarOrdinal},
	"LAT/star/groupedLimit":    {l1StarOrdinal},
	"LAT/star/orderLimit":      {l1StarOrdinal},
	"LAT/star/where":           {l1StarOrdinal},
	"LIFTED/aggregated":        {l1LiftedRefNotPublished},
	"LIFTED/exprBothSides":     {l1LiftedRefNotPublished},
	"LIFTED/inequalityAlone":   {l1LiftedRefNotPublished},
	"LIFTED/leftArm":           {l1LiftedRefNotPublished},
	"LIFTED/twoColumns":        {l1LiftedRefNotPublished},
	"OUTERREF/aggArg":          {l1OuterRefOutsideWhere},
	"OUTERREF/groupBy":         {l1OuterRefOutsideWhere},
	"OUTERREF/having":          {l1OuterRefOutsideWhere},
	"OUTERREF/orderBy":         {l1OuterRefOutsideWhere},
	"OUTERREF/selectBare":      {l1OuterRefOutsideWhere},
	"OUTERREF/selectCase":      {l1OuterRefOutsideWhere},
	"OUTERREF/selectExpr":      {l1OuterRefOutsideWhere},
	"OUTERREF/whereInequality": {l1LiftedRefNotPublished},
	"SCALAR/inner/winInOrder":  {l1OverClauseRebuild},
	"SCALAR/inner/winarg":      {l1WindowInSubquery},
	"SCALAR/inner/winargBare":  {l1WindowInSubquery},
	"SCALAR/inner/winord":      {l1WindowInSubquery},
	"SCALAR/inner/winpart":     {l1WindowInSubquery},
	"SCALAR/inner/winpart2":    {l1WindowInSubquery},
	"SCALAR/left/winInOrder":   {l1OverClauseRebuild},
	"SCALAR/left/winarg":       {l1WindowInSubquery},
	"SCALAR/left/winargBare":   {l1WindowInSubquery},
	"SCALAR/left/winord":       {l1WindowInSubquery},
	"SCALAR/left/winpart":      {l1WindowInSubquery},
	"SCALAR/left/winpart2":     {l1WindowInSubquery},
	"SCALAR/noJoin/winInOrder": {l1OverClauseRebuild},
	"SCALAR/noJoin/winarg":     {l1WindowInSubquery},
	"SCALAR/noJoin/winargBare": {l1WindowInSubquery},
	"SCALAR/noJoin/winord":     {l1WindowInSubquery},
	"SCALAR/noJoin/winpart":    {l1WindowInSubquery},
	"SCALAR/noJoin/winpart2":   {l1WindowInSubquery},
}

// l1ValuePins are the cells that answer a wrong VALUE rather than refusing,
// with PostgreSQL's answer recorded above beside them. Each is a boundary with
// a mechanism, not a shrug.
// The two EXISTS cells are an ENGINE SUPERSET and not a wrong value: a window
// function in a WHERE clause is an ERROR in PostgreSQL ("window functions are
// not allowed in WHERE") and this engine evaluates it. Recorded here rather
// than in a refusal list because the cell's disposition is a ROW SET.
// l1ArmPins is a divergence that is NOT the same on every arm, so it is
// recorded per arm. A pin that starts agreeing FAILS.
var l1ArmPins = map[string]map[string]string{
	// A body whose own FROM item is named like an enclosing relation. SQL
	// scoping resolves the qualifier to the INNER item and the two
	// single-process arms do; the three DAG arms bind the outer relation's
	// column of that name, which is the same seam ADR-0026 8j records.
	// Identical at c34cdbcb for `shadowOrderBy`; `shadowSelect` was REFUSED
	// there and on the arc's round-1 tip, and answers on two arms now.
	"R2/shadowSelect": {
		"dag":          "rows=6 1,50 | 1,50 | 1,50 | 2,100 | 2,100 | 2,100",
		"dag-shuffled": "rows=6 1,50 | 1,50 | 1,50 | 2,100 | 2,100 | 2,100",
		"dag-morsel4":  "rows=6 1,50 | 1,50 | 1,50 | 2,100 | 2,100 | 2,100",
	},
	"R2/shadowOrderBy": {
		"dag":          "rows=6 1,50 | 1,50 | 1,50 | 3,75 | 3,75 | 3,75",
		"dag-shuffled": "rows=6 1,50 | 1,50 | 1,50 | 3,75 | 3,75 | 3,75",
		"dag-morsel4":  "rows=6 1,50 | 1,50 | 1,50 | 3,75 | 3,75 | 3,75",
	},
	"R2/collideWinArg": {
		"single":       "rows=2 1,2 | 2,4",
		"spilled512k":  "rows=2 1,2 | 2,4",
		"dag":          "rows=2 1,NULL | 2,NULL",
		"dag-shuffled": "rows=2 1,NULL | 2,NULL",
		"dag-morsel4":  "rows=2 1,NULL | 2,NULL",
	},
	"R2/collideWinBound": {
		"single":       "rows=2 1,2,1 | 2,4,1",
		"spilled512k":  "rows=2 1,2,1 | 2,4,1",
		"dag":          "ERR ~window: ORDER BY \"s.m\" is not a column of its input",
		"dag-shuffled": "ERR ~window: ORDER BY \"s.m\" is not a column of its input",
		"dag-morsel4":  "ERR ~window: ORDER BY \"s.m\" is not a column of its input",
	},
	"R2/collideWinNoBound": {
		"dag":          "rows=4 1,1,1 | 1,1,2 | 2,2,1 | 2,2,2",
		"dag-shuffled": "rows=4 1,1,1 | 1,1,2 | 2,2,1 | 2,2,2",
		"dag-morsel4":  "rows=4 1,1,1 | 1,1,2 | 2,2,1 | 2,2,2",
	},
	"UNCORRLAT/collidingName": {
		"dag":          "rows=3 1,150 | 2,200 | 3,0",
		"dag-morsel4":  "rows=3 1,150 | 2,200 | 3,0",
		"dag-shuffled": "rows=3 1,150 | 2,200 | 3,0",
	},
}

var l1ValuePins = map[string]string{
	"R2/boundLiftedFrac":         "rows=0 ",
	"R2/boundLiftedPlain":        "rows=1 2,125",
	"R2/noCollideWinBound":       "rows=2 1,100,1 | 2,125,1",
	"R2/twoBounds":               "rows=0 ",
	"R2/winargNested":            "rows=3 1,1 | 2,1 | 3,1",
	"EXISTS/inner/winarg":        "rows=4 1,1 | 1,2 | 2,3 | 2,4",
	"EXISTS/noJoin/winarg":       "rows=2 1 | 2",
	"LAT/comma/groupedLimit":     "rows=1 2,Doohickey,125",
	"LAT/comma/groupedOffset":    "rows=3 1,Gadget,100 | 1,Widget,50 | 2,Widget,75",
	"LAT/comma/limitOffset":      "rows=1 2,75",
	"LAT/comma/limitOnly":        "rows=1 1,50",
	"LAT/comma/orderLimit":       "rows=1 2,125",
	"LAT/comma/orderLimit2":      "rows=2 1,100 | 2,125",
	"LAT/comma/orderOffset":      "rows=3 1,100 | 1,50 | 2,75",
	"LAT/inner/groupedLimit":     "rows=1 2,Doohickey,125",
	"LAT/inner/groupedOffset":    "rows=3 1,Gadget,100 | 1,Widget,50 | 2,Widget,75",
	"LAT/inner/limitOffset":      "rows=1 2,75",
	"LAT/inner/limitOnly":        "rows=1 1,50",
	"LAT/inner/orderLimit":       "rows=1 2,125",
	"LAT/inner/orderLimit2":      "rows=2 1,100 | 2,125",
	"LAT/inner/orderOffset":      "rows=3 1,100 | 1,50 | 2,75",
	"LAT/joinInner/groupedLimit": "rows=2 2,Doohickey,125 | 2,Doohickey,125",
	"LAT/joinInner/orderLimit":   "rows=2 2,125 | 2,125",
	"LAT/joinLeft/groupedLimit":  "rows=2 2,Doohickey,125 | 2,Doohickey,125",
	"LAT/joinLeft/orderLimit":    "rows=2 2,125 | 2,125",
	"LAT/left/groupedLimit":      "rows=3 1,NULL,NULL | 2,Doohickey,125 | 3,NULL,NULL",
	"LAT/left/groupedOffset":     "rows=4 1,Gadget,100 | 1,Widget,50 | 2,Widget,75 | 3,NULL,NULL",
	"LAT/left/limitOffset":       "rows=3 1,NULL | 2,75 | 3,NULL",
	"LAT/left/limitOnly":         "rows=3 1,50 | 2,NULL | 3,NULL",
	"LAT/left/orderLimit":        "rows=3 1,NULL | 2,125 | 3,NULL",
	"LAT/left/orderLimit2":       "rows=3 1,100 | 2,125 | 3,NULL",
	"LAT/left/orderOffset":       "rows=4 1,100 | 1,50 | 2,75 | 3,NULL",
}

func TestArcL1LateralAndWindowScopeAnswersPostgresOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the lateral/window scope table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := r1Arms(t, ctx)
	seen := map[string]bool{}
	for _, tc := range l1LateralCases() {
		want, ok := l1Postgres[tc.name]
		if !ok {
			t.Fatalf("%s: no PostgreSQL row set recorded — the table and the "+
				"measurement have diverged", tc.name)
		}
		seen[tc.name] = true
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got := arm.run(tc.sql)
				if classes, pinned := l1RefusalPins[tc.name]; pinned {
					hit := false
					for _, c := range classes {
						if strings.Contains(got, c) {
							hit = true
							break
						}
					}
					if !hit {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  none of the pinned refusals %q: "+
							"if it answers now, assert PostgreSQL's row set %s and delete this cell's pin",
							tc.sql, arm.name, got, classes, want)
					}
					continue
				}
				if arms, pinned := l1ArmPins[tc.name]; pinned {
					if pin, has := arms[arm.name]; has {
						// A pin beginning `ERR ~` is a SUBSTRING: a DAG task
						// id is in the message and changes every run.
						if strings.HasPrefix(pin, "ERR ~") {
							if !strings.Contains(got, strings.TrimPrefix(pin, "ERR ~")) {
								t.Errorf("%s\n  arm  %s\n  got  %s\n  the pinned refusal (%q) is gone: "+
									"if it answers now, assert PostgreSQL's %s and delete this arm's pin",
									tc.sql, arm.name, got, pin, want)
							}
							continue
						}
						if got == want {
							t.Errorf("%s\n  arm  %s\n  the pinned divergence is GONE and the arm "+
								"answers PostgreSQL's %s: delete this arm's pin", tc.sql, arm.name, want)
							continue
						}
						if got != pin {
							t.Errorf("%s\n  arm  %s\n  got  %s\n  pinned %s (PostgreSQL answers %s)",
								tc.sql, arm.name, got, pin, want)
						}
						continue
					}
				}
				if pin, pinned := l1ValuePins[tc.name]; pinned {
					if got == want {
						t.Errorf("%s\n  arm  %s\n  the pinned divergence is GONE and the cell "+
							"answers PostgreSQL's %s: delete this cell's pin", tc.sql, arm.name, want)
						continue
					}
					if got != pin {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  pinned %s (PostgreSQL answers %s)",
							tc.sql, arm.name, got, pin, want)
					}
					continue
				}
				if got != want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)",
						tc.sql, arm.name, got, want)
				}
			}
		})
	}
	for name := range l1Postgres {
		if !seen[name] {
			t.Errorf("%s: a PostgreSQL row set is recorded for a cell the table "+
				"no longer writes", name)
		}
	}
}
