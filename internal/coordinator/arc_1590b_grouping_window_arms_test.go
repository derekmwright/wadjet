// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1590 (the P1 line): GROUPING(...) written inside a WINDOW's spec over a
// grouping-set query — `count(*) OVER (PARTITION BY GROUPING(k))` — answered
// wrong rows on every arm, for a plain column key and a computed one alike.
// The builder's window-spec aggregate hoist took GROUPING for an aggregate
// (it is in the parser's aggregate list so it can be found inside an
// expression) and published it as one; the physical planner maps an
// aggregate name it does not know to COUNT, so the window partitioned on
// count(k) per group, not on which grouping set produced the row. The SELECT
// list, HAVING and ORDER BY already gave GROUPING its own slot.
//
// The matrix crosses the GROUPING argument (a plain column, COALESCE, ||,
// lower(), integer arithmetic, a CAST, an alias, an ordinal, a respaced
// repetition, two arguments, a key missing from a set), the grouping construct
// (ROLLUP of one, two and three keys, CUBE, GROUPING SETS with and without the
// empty set, plain GROUP BY), the position (window PARTITION BY, ORDER BY,
// both, the window's argument, beside the window in the SELECT list, HAVING,
// the query's ORDER BY, the CASE total-row idiom, a derived table) and the
// data (a real NULL group, duplicates, one group, no rows), with the window
// replaced by a plain projection as controls. Round 3 adds the query's ORDER BY
// GROUPING (alone, beside a window-spec GROUPING, with and without the call in
// the SELECT list, DESC / NULLS / secondary / ordinal / alias, compared in
// order), GROUPING in QUALIFY, nested windows, CTEs and derived tables, the
// statements PostgreSQL refuses, and every statement the round-2 review probed
// outside the first matrix (e*, f*, g*, h*). An ORDER BY GROUPING(k) with no
// GROUPING(k) in the SELECT list sorts on GROUPING's slot; it used to be
// refused, or — beside a window-spec GROUPING — sorted on count(k). testdata/
// arc_1590b_grouping_window_pg17.tsv holds PostgreSQL 17.11's answer for every
// cell (postgres:17-alpine, initdb --locale=C --encoding=UTF8).

func u1590bTables() []tmdTable {
	t := tmdTable{name: "gw_t", schema: parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "t", Type: parquet.TypeString, Nullable: true},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
	}}}
	type r struct {
		id   int64
		s, t any
		n    any
	}
	for _, x := range []r{
		{1, "hi", "x", int64(10)}, {2, "hx", "y", int64(20)}, {3, "A", "x", int64(30)},
		{4, "ab", "y", int64(40)}, {5, nil, "x", nil}, {6, "z", nil, int64(60)}, {7, "hi", "y", int64(70)},
	} {
		t.rows = append(t.rows, map[string]any{"id": x.id, "s": x.s, "t": x.t, "n": x.n})
	}
	return []tmdTable{t}
}

// u1590bPGFixture is u1590bTables as PostgreSQL DDL.
const u1590bPGFixture = `CREATE TABLE gw_t (id bigint, s text, t text, n bigint);
INSERT INTO gw_t VALUES (1,'hi','x',10),(2,'hx','y',20),(3,'A','x',30),(4,'ab','y',40),(5,NULL,'x',NULL),(6,'z',NULL,60),(7,'hi','y',70);`

// u1590bKeys are the GROUPING arguments: {K} is written in GROUP BY, {G} in
// GROUPING() — the same expression, except where the cell is about a
// different spelling of it.
var u1590bKeys = []struct{ name, k, g string }{
	{"col", "s", "s"},
	{"coalesce", "COALESCE(s,'hi')", "COALESCE(s,'hi')"},
	{"respaced", "COALESCE(s,'hi')", "COALESCE( s , 'hi' )"},
	{"concat", "s || 'a'", "s || 'a'"},
	{"lower", "lower(s)", "lower(s)"},
	{"arith", "n + 1", "n + 1"},
	{"cast", "CAST(n AS TEXT)", "CAST(n AS TEXT)"},
}

// u1590bKeyTemplates run per key.
var u1590bKeyTemplates = []u1590bCell{
	// the failing position, with and without GROUPING in the SELECT list
	{name: "w/part", sql: `SELECT GROUPING({G}) AS g, count(*) AS n, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY ROLLUP ({K})`},
	{name: "w/part-only", sql: `SELECT count(*) AS n, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY ROLLUP ({K})`},
	{name: "w/part-key", sql: `SELECT {K} AS k, count(*) AS n, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY ROLLUP ({K})`},
	{name: "w/order", sql: `SELECT {K} AS k, row_number() OVER (ORDER BY GROUPING({G}), {K}) AS r FROM gw_t GROUP BY ROLLUP ({K})`},
	{name: "w/sum-count", sql: `SELECT {K} AS k, sum(count(*)) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY ROLLUP ({K})`},
	{name: "w/arg-sum", sql: `SELECT {K} AS k, sum(GROUPING({G})) OVER () AS c FROM gw_t GROUP BY ROLLUP ({K})`},
	{name: "w/cube", sql: `SELECT {K} AS k, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY CUBE ({K})`},
	{name: "w/sets-empty", sql: `SELECT {K} AS k, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY GROUPING SETS (({K}), ())`},
	{name: "w/plain", sql: `SELECT {K} AS k, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY {K}`},
	// controls: GROUPING() projected, no window
	{name: "p/proj", sql: `SELECT {K} AS k, GROUPING({G}) AS g, count(*) AS n FROM gw_t GROUP BY ROLLUP ({K})`},
	{name: "p/having", sql: `SELECT {K} AS k, count(*) AS n FROM gw_t GROUP BY ROLLUP ({K}) HAVING GROUPING({G}) = 0`},
	{name: "p/order", sql: `SELECT GROUPING({G}) AS g, count(*) AS n FROM gw_t GROUP BY ROLLUP ({K}) ORDER BY GROUPING({G}) DESC, count(*) DESC LIMIT 2`},
	// Round 3. The query's ORDER BY, alone and beside a window-spec
	// GROUPING, with and without the same call in the SELECT list: compared
	// in ORDER, every order total.
	{name: "o/only", ordered: true, sql: `SELECT {K} AS k, count(*) AS n FROM gw_t GROUP BY ROLLUP ({K}) ORDER BY GROUPING({G}) DESC, {K} NULLS FIRST`},
	{name: "o/beside-win", ordered: true, sql: `SELECT {K} AS k, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY ROLLUP ({K}) ORDER BY GROUPING({G}), {K} NULLS FIRST`},
	{name: "o/beside-win-sel", ordered: true, sql: `SELECT {K} AS k, GROUPING({G}) AS g, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY ROLLUP ({K}) ORDER BY GROUPING({G}), {K} NULLS FIRST`},
	{name: "o/secondary", ordered: true, sql: `SELECT {K} AS k, count(*) AS n FROM gw_t GROUP BY ROLLUP ({K}) ORDER BY {K} NULLS FIRST, GROUPING({G}) DESC`},
	// QUALIFY: PostgreSQL has none, so its answer is measured on the
	// derived-table spelling of the same filter.
	{name: "q/part", sql: `SELECT {K} AS k FROM gw_t GROUP BY ROLLUP ({K}) QUALIFY count(*) OVER (PARTITION BY GROUPING({G})) > 1`,
		pg: `SELECT k FROM (SELECT {K} AS k, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY ROLLUP ({K})) q WHERE c > 1`},
	// a window nested in an expression; a CTE carrying the grouped rows with
	// the window above it; a CTE with the window inside it
	{name: "n/nested-win", sql: `SELECT {K} AS k, 1 + count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY ROLLUP ({K})`},
	{name: "c/cte-above", sql: `WITH q AS (SELECT {K} AS k, count(*) AS n, GROUPING({G}) AS g FROM gw_t GROUP BY ROLLUP ({K})) SELECT k, n, count(*) OVER (PARTITION BY g) AS c FROM q`},
	{name: "c/cte-inside", sql: `WITH q AS (SELECT {K} AS k, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY ROLLUP ({K})) SELECT c, count(*) AS m FROM q GROUP BY c`},
}

// u1590bFixed are the cells outside the per-key grid.
var u1590bFixed = []u1590bCell{
	// two and three keys
	{name: "x/rollup2-part-s", sql: `SELECT s, t, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s, t)`},
	{name: "x/rollup2-part-t", sql: `SELECT COALESCE(s,'hi') AS k, t, count(*) OVER (PARTITION BY GROUPING(t)) AS c FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'), t)`},
	{name: "x/rollup2-part-two-args", sql: `SELECT GROUPING(s, t) AS g, count(*) OVER (PARTITION BY GROUPING(s, t)) AS c FROM gw_t GROUP BY ROLLUP (s, t)`},
	{name: "x/rollup2-part-two-args-rev", sql: `SELECT GROUPING(t, s) AS g, count(*) OVER (PARTITION BY GROUPING(t, s)) AS c FROM gw_t GROUP BY ROLLUP (s, t)`},
	{name: "x/rollup2-part-two-computed", sql: `SELECT count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'), lower(t))) AS c, count(*) AS n FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'), lower(t))`},
	{name: "x/rollup3-part", sql: `SELECT t, id % 2 AS m, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'))) AS c FROM gw_t GROUP BY ROLLUP (t, COALESCE(s,'hi'), id % 2)`},
	{name: "x/rollup3-part-proj", sql: `SELECT t, id % 2 AS m, count(*) AS n, GROUPING(COALESCE(s,'hi')) AS g FROM gw_t GROUP BY ROLLUP (t, COALESCE(s,'hi'), id % 2)`},
	{name: "x/sets-missing-key", sql: `SELECT s, t, count(*) OVER (PARTITION BY GROUPING(t)) AS c FROM gw_t GROUP BY GROUPING SETS ((s), (t))`},
	{name: "x/sets-missing-key-proj", sql: `SELECT s, t, GROUPING(t) AS g FROM gw_t GROUP BY GROUPING SETS ((s), (t))`},
	{name: "x/sets-no-empty", sql: `SELECT COALESCE(s,'hi') AS k, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'))) AS c FROM gw_t GROUP BY GROUPING SETS ((COALESCE(s,'hi')))`},
	{name: "x/cube2-part", sql: `SELECT s, t, count(*) OVER (PARTITION BY GROUPING(s, t)) AS c FROM gw_t GROUP BY CUBE (s, t)`},
	// alias, ordinal
	{name: "x/alias-group", sql: `SELECT COALESCE(s,'hi') AS k, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'))) AS c FROM gw_t GROUP BY ROLLUP (k)`},
	{name: "x/alias-grouping", sql: `SELECT COALESCE(s,'hi') AS k, count(*) OVER (PARTITION BY GROUPING(k)) AS c FROM gw_t GROUP BY ROLLUP (k)`},
	{name: "x/ordinal-group", sql: `SELECT COALESCE(s,'hi') AS k, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'))) AS c FROM gw_t GROUP BY ROLLUP (1)`},
	{name: "x/ordinal-group-proj", sql: `SELECT COALESCE(s,'hi') AS k, GROUPING(COALESCE(s,'hi')) AS g FROM gw_t GROUP BY ROLLUP (1)`},
	// both positions, rank, first_value, a frame, the argument
	{name: "x/both-rank", sql: `SELECT COALESCE(s,'hi') AS k, rank() OVER (PARTITION BY GROUPING(COALESCE(s,'hi')) ORDER BY count(*) DESC) AS r FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'))`},
	{name: "x/both-row-number", sql: `SELECT s, row_number() OVER (PARTITION BY GROUPING(s) ORDER BY GROUPING(s), s) AS r FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "x/first-value", sql: `SELECT s, first_value(count(*)) OVER (PARTITION BY GROUPING(s) ORDER BY count(*) DESC, s) AS f FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "x/first-value-arg", sql: `SELECT s, first_value(GROUPING(s)) OVER (PARTITION BY s ORDER BY count(*)) AS f FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "x/frame", sql: `SELECT s, sum(count(*)) OVER (PARTITION BY GROUPING(s) ORDER BY s ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS f FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "x/max-arg-part", sql: `SELECT s, max(GROUPING(s)) OVER (PARTITION BY s) AS m FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "x/arg-plus", sql: `SELECT s, sum(GROUPING(s) + 1) OVER () AS m FROM gw_t GROUP BY ROLLUP (s)`},
	// GROUPING inside an expression in the window spec, and the CASE idiom
	{name: "x/case-part", sql: `SELECT s, count(*) OVER (PARTITION BY CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE 'one' END) AS c FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "x/expr-part", sql: `SELECT COALESCE(s,'hi') AS k, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi')) + 1) AS c FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'))`},
	{name: "x/case-label", sql: `SELECT CASE WHEN GROUPING(COALESCE(s,'hi')) = 1 THEN 'ALL' ELSE COALESCE(s,'hi') END AS k, count(*) AS n FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'))`},
	{name: "x/case-label-col", sql: `SELECT CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE s END AS k, count(*) AS n FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "x/case-label-window", sql: `SELECT CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE s END AS k, count(*) AS n, sum(count(*)) OVER (PARTITION BY GROUPING(s)) AS tot FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "x/case-label-share", sql: `SELECT CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE s END AS k, count(*) * 100 / sum(count(*)) OVER (PARTITION BY GROUPING(s)) AS pct FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "x/case-label-share-int", sql: `SELECT CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE s END AS k, count(*) * 100 / CAST(sum(count(*)) OVER (PARTITION BY GROUPING(s)) AS BIGINT) AS pct FROM gw_t GROUP BY ROLLUP (s)`},
	// a derived table over the grouped rows
	{name: "x/derived-agg", sql: `SELECT g, count(*) AS rows_, sum(n) AS n FROM (SELECT GROUPING(COALESCE(s,'hi')) AS g, count(*) AS n FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'))) q GROUP BY g`},
	{name: "x/derived-window", sql: `SELECT g, n, count(*) OVER (PARTITION BY g) AS c FROM (SELECT GROUPING(s) AS g, count(*) AS n FROM gw_t GROUP BY ROLLUP (s)) q`},
	// data: one group, a NULL-only group, no rows
	{name: "d/one-group", sql: `SELECT s, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t WHERE id IN (1, 7) GROUP BY ROLLUP (s)`},
	{name: "d/null-group", sql: `SELECT s, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t WHERE id = 5 GROUP BY ROLLUP (s)`},
	{name: "d/null-group-proj", sql: `SELECT s, count(*) AS n, GROUPING(s) AS g FROM gw_t WHERE id = 5 GROUP BY ROLLUP (s)`},
	{name: "d/empty", sql: `SELECT s, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t WHERE id > 100 GROUP BY ROLLUP (s)`},
	{name: "d/empty-proj", sql: `SELECT s, count(*) AS n, GROUPING(s) AS g FROM gw_t WHERE id > 100 GROUP BY ROLLUP (s)`},
	{name: "d/empty-plain", sql: `SELECT s, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t WHERE id > 100 GROUP BY s`},
	// the issue's statement, over TEXT
	{name: "i/issue-text", sql: `SELECT GROUPING(COALESCE(s,'\x6869')) AS g, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'\x6869'))) AS c FROM gw_t GROUP BY ROLLUP (COALESCE(s,'\x6869'))`},
	{name: "i/issue-cast", sql: `SELECT GROUPING(COALESCE(s,CAST('hi' AS TEXT))) AS g, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,CAST('hi' AS TEXT)))) AS c FROM gw_t GROUP BY ROLLUP (COALESCE(s,CAST('hi' AS TEXT)))`},

	// Round 3: every statement the round-2 review probed outside the first
	// matrix (e*, f*, g*, h*), then the positions it did not cover.
	// No GROUP BY: PostgreSQL raises 42803 for every one.
	{name: "e01-nogroup", sql: `SELECT count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t`},
	{name: "e02-nogroup-agg", sql: `SELECT count(*) AS n, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t`},
	{name: "e03-nonkey", sql: `SELECT s, count(*) OVER (PARTITION BY GROUPING(t)) AS c FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "e04-nested-win", sql: `SELECT s, 1 + count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "e05-named-win", sql: `SELECT s, count(*) OVER w AS c FROM gw_t GROUP BY ROLLUP (s) WINDOW w AS (PARTITION BY GROUPING(s))`},
	{name: "e06-having", sql: `SELECT s, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s) HAVING GROUPING(s) = 1 OR count(*) > 1`},
	{name: "e07-cte", sql: `WITH q AS (SELECT s, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s)) SELECT c, count(*) AS m FROM q GROUP BY c`},
	{name: "e08-orderby", ordered: true, sql: `SELECT s, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(s) DESC, s LIMIT 3`},
	{name: "e09-two-windows", sql: `SELECT s, t, count(*) OVER (PARTITION BY GROUPING(s)) AS a, count(*) OVER (PARTITION BY GROUPING(t)) AS b FROM gw_t GROUP BY ROLLUP (s, t)`},
	{name: "e11-distinct", sql: `SELECT DISTINCT count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "e12-case-spell", sql: `SELECT Grouping(s) AS g, count(*) OVER (PARTITION BY grouping(s)) AS c FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "e13-qualify", sql: `SELECT s FROM gw_t GROUP BY ROLLUP (s) QUALIFY count(*) OVER (PARTITION BY GROUPING(s)) > 1`,
		pg: `SELECT s FROM (SELECT s, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s)) q WHERE c > 1`},
	{name: "e14-qualified", sql: `SELECT gw_t.s, count(*) OVER (PARTITION BY GROUPING(gw_t.s)) AS c FROM gw_t GROUP BY ROLLUP (gw_t.s)`},
	{name: "e15-join", sql: `SELECT a.s, count(*) OVER (PARTITION BY GROUPING(a.s)) AS c FROM gw_t a JOIN gw_t b ON a.id = b.id GROUP BY ROLLUP (a.s)`},
	{name: "e16-plain-nokey", sql: `SELECT count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY s`},
	{name: "e17-lag", sql: `SELECT s, lag(GROUPING(s)) OVER (ORDER BY GROUPING(s), s) AS l FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "e18-count-arg", sql: `SELECT s, count(GROUPING(s)) OVER () AS c FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "e19-sel-agg-grouping-key", sql: `SELECT s, sum(n) AS sn, sum(sum(n)) OVER (PARTITION BY GROUPING(s)) AS tot FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "e20-key-alias-in-select-and-grouping-of-other", sql: `SELECT s AS k, count(*) OVER (PARTITION BY GROUPING(s), s) AS c FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "f1-orderby-nowin", ordered: true, sql: `SELECT s, count(*) AS n FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(s) DESC, s LIMIT 3`},
	{name: "f2-orderby-win-sel", ordered: true, sql: `SELECT s, GROUPING(s) AS g, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(s) DESC, s LIMIT 3`},
	{name: "f3-orderby-win-alias", ordered: true, sql: `SELECT s, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s) ORDER BY c, s LIMIT 3`},
	{name: "f4-orderby-win-plain", ordered: true, sql: `SELECT s, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY s ORDER BY GROUPING(s), s LIMIT 3`},
	{name: "f5-orderby-count-win", ordered: true, sql: `SELECT s, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s) ORDER BY count(*) DESC, s LIMIT 3`},
	{name: "f6-orderby-nowin-plain", ordered: true, sql: `SELECT s FROM gw_t GROUP BY s ORDER BY GROUPING(s), s LIMIT 3`},
	{name: "f7-orderby-win-desc-only", sql: `SELECT s, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(s) DESC`},
	{name: "g1-coincide-orderby", ordered: true, sql: `SELECT id % 2 AS m, count(*) OVER (PARTITION BY GROUPING(id % 2)) AS c FROM gw_t WHERE id IN (1, 2, 3, 4) GROUP BY ROLLUP (id % 2) ORDER BY GROUPING(id % 2), id % 2`},
	{name: "g2-coincide-orderby-only", sql: `SELECT id % 2 AS m, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(id % 2)) AS c FROM gw_t WHERE id IN (1, 2, 3, 4) GROUP BY ROLLUP (id % 2) ORDER BY GROUPING(id % 2) DESC`},
	{name: "g3-plain-nonnull-orderby", ordered: true, sql: `SELECT id AS k, count(*) OVER (PARTITION BY GROUPING(id)) AS c FROM gw_t GROUP BY id ORDER BY GROUPING(id), id`},
	{name: "h1-label-orderby", ordered: true, sql: `SELECT CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE s END AS k, count(*) AS n FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(s), s`},
	{name: "h2-label-window-orderby", ordered: true, sql: `SELECT CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE s END AS k, count(*) AS n, sum(count(*)) OVER (PARTITION BY GROUPING(s)) AS tot FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(s), s`},
	{name: "h3-window-orderby", ordered: true, sql: `SELECT s, count(*) AS n, sum(count(*)) OVER (PARTITION BY GROUPING(s)) AS tot FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(s), s`},
	{name: "h4-label-window-orderby-coalesce", sql: `SELECT CASE WHEN GROUPING(COALESCE(s,'hi')) = 1 THEN 'ALL' ELSE COALESCE(s,'hi') END AS k, sum(count(*)) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'))) AS tot FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi')) ORDER BY GROUPING(COALESCE(s,'hi'))`},
	// ORDER BY GROUPING: DESC / NULLS FIRST, as a secondary key, by ordinal
	// and by alias, two arguments, CUBE, OFFSET, a computed key
	{name: "o/desc-nulls-first", ordered: true, sql: `SELECT s, count(*) AS n FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(s) DESC NULLS FIRST, s NULLS FIRST`},
	{name: "o/asc-nulls-last", ordered: true, sql: `SELECT s, count(*) AS n FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(s) ASC NULLS LAST, s DESC`},
	{name: "o/secondary-asc", ordered: true, sql: `SELECT s, count(*) AS n FROM gw_t GROUP BY ROLLUP (s) ORDER BY s NULLS FIRST, GROUPING(s)`},
	{name: "o/secondary-beside-win", ordered: true, sql: `SELECT s, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s) ORDER BY s NULLS FIRST, GROUPING(s) DESC`},
	{name: "o/ordinal", ordered: true, sql: `SELECT s, GROUPING(s) AS g FROM gw_t GROUP BY ROLLUP (s) ORDER BY 2 DESC, 1`},
	{name: "o/alias", ordered: true, sql: `SELECT s, GROUPING(s) AS g FROM gw_t GROUP BY ROLLUP (s) ORDER BY g DESC, s`},
	{name: "o/alias-beside-win", ordered: true, sql: `SELECT s, GROUPING(s) AS g, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s) ORDER BY g, s`},
	{name: "o/ordinal-beside-win", ordered: true, sql: `SELECT s, GROUPING(s) AS g, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s) ORDER BY 2, 1`},
	{name: "o/two-args", ordered: true, sql: `SELECT s, t, count(*) AS n FROM gw_t GROUP BY ROLLUP (s, t) ORDER BY GROUPING(s, t), s, t`},
	{name: "o/two-calls-cube", ordered: true, sql: `SELECT s, t FROM gw_t GROUP BY CUBE (s, t) ORDER BY GROUPING(t), GROUPING(s), s, t`},
	{name: "o/sets-missing-key", ordered: true, sql: `SELECT s, t FROM gw_t GROUP BY GROUPING SETS ((s), (t)) ORDER BY GROUPING(t), s, t`},
	{name: "o/offset", ordered: true, sql: `SELECT s, count(*) AS n FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(s) DESC, s LIMIT 2 OFFSET 1`},
	{name: "o/computed-two-keys", ordered: true, sql: `SELECT COALESCE(s,'hi') AS k, lower(t) AS u, count(*) AS n FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'), lower(t)) ORDER BY GROUPING(lower(t)) DESC, GROUPING(COALESCE(s,'hi')), k, u`},
	{name: "o/computed-ordinal-group", ordered: true, sql: `SELECT COALESCE(s,'hi') AS k, count(*) AS n FROM gw_t GROUP BY ROLLUP (1) ORDER BY GROUPING(COALESCE(s,'hi')) DESC, k`},
	{name: "o/plain-computed", ordered: true, sql: `SELECT n + 1 AS k FROM gw_t GROUP BY n + 1 ORDER BY GROUPING(n + 1), k`},
	{name: "o/empty-input", ordered: true, sql: `SELECT s, count(*) AS n FROM gw_t WHERE id > 100 GROUP BY ROLLUP (s) ORDER BY GROUPING(s)`},
	// ORDER BY GROUPING inside a larger expression (PostgreSQL answers)
	{name: "o/expr-plus", ordered: true, sql: `SELECT s, count(*) AS n FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(s) + 0 DESC, s`},
	{name: "o/expr-case", ordered: true, sql: `SELECT s, count(*) AS n FROM gw_t GROUP BY ROLLUP (s) ORDER BY CASE WHEN GROUPING(s) = 1 THEN 0 ELSE 1 END, s`},
	// ORDER BY GROUPING that PostgreSQL refuses
	{name: "o/no-group-by", sql: `SELECT count(*) AS n FROM gw_t ORDER BY GROUPING(s)`},
	{name: "o/nonkey", sql: `SELECT s FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(t)`},
	{name: "o/distinct", sql: `SELECT DISTINCT s FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(s)`},
	// GROUPING inside QUALIFY (outside a window call, and in the ORDER BY of
	// one), measured on the derived-table spelling
	{name: "q/outside-window", sql: `SELECT s FROM gw_t GROUP BY ROLLUP (s) QUALIFY GROUPING(s) = 0 AND row_number() OVER () > 0`,
		pg: `SELECT s FROM (SELECT s, GROUPING(s) AS g, row_number() OVER () AS r FROM gw_t GROUP BY ROLLUP (s)) q WHERE g = 0 AND r > 0`},
	{name: "q/order-rn", sql: `SELECT s FROM gw_t GROUP BY ROLLUP (s) QUALIFY row_number() OVER (PARTITION BY GROUPING(s) ORDER BY count(*) DESC, s) = 1`,
		pg: `SELECT s FROM (SELECT s, row_number() OVER (PARTITION BY GROUPING(s) ORDER BY count(*) DESC, s) AS r FROM gw_t GROUP BY ROLLUP (s)) q WHERE r = 1`},
	{name: "q/computed", sql: `SELECT lower(s) AS k FROM gw_t GROUP BY ROLLUP (lower(s)) QUALIFY sum(count(*)) OVER (PARTITION BY GROUPING(lower(s))) = 7 AND count(*) > 1`,
		pg: `SELECT k FROM (SELECT lower(s) AS k, count(*) AS n, sum(count(*)) OVER (PARTITION BY GROUPING(lower(s))) AS w FROM gw_t GROUP BY ROLLUP (lower(s))) q WHERE w = 7 AND n > 1`},
	// nested windows
	{name: "n/two-in-expr", sql: `SELECT s, sum(count(*)) OVER (PARTITION BY GROUPING(s)) - count(*) OVER (PARTITION BY GROUPING(s)) AS d FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "n/coalesce-lag", sql: `SELECT s, COALESCE(lag(count(*)) OVER (PARTITION BY GROUPING(s) ORDER BY s), 0) AS l FROM gw_t GROUP BY ROLLUP (s)`},
	{name: "n/case-of-window", sql: `SELECT COALESCE(s,'hi') AS k, CASE WHEN count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'))) > 1 THEN 'many' ELSE 'one' END AS w FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'))`},
	// derived tables carrying the grouped rows, the window above or inside
	{name: "c/derived-computed-above", sql: `SELECT k, g, count(*) OVER (PARTITION BY g) AS c FROM (SELECT COALESCE(s,'hi') AS k, GROUPING(COALESCE(s,'hi')) AS g FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'))) q`},
	{name: "c/derived-inside-ordered", ordered: true, sql: `SELECT k, c FROM (SELECT s AS k, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s)) q ORDER BY c, k NULLS FIRST`},
	{name: "c/cte-orderby-inside", ordered: true, sql: `WITH q AS (SELECT s, GROUPING(s) AS g FROM gw_t GROUP BY ROLLUP (s)) SELECT s, g, count(*) OVER (PARTITION BY g) AS c FROM q ORDER BY g DESC, s`},
	// GROUPING with no GROUP BY outside a window, and an aggregate in a
	// window spec with no GROUP BY (the e01 neighbour): measured, not fixed
	{name: "z/nogroup-proj", sql: `SELECT GROUPING(s) AS g FROM gw_t`},
	{name: "z/win-agg-nogroup", sql: `SELECT count(*) OVER (PARTITION BY count(*)) AS c FROM gw_t`},
}

// u1590bCell is one statement. pg, when set, is the spelling measured on
// PostgreSQL (QUALIFY, which it lacks, as a derived table); ordered cells are
// compared row for row in the order they arrive, every one of them with a
// total ORDER BY.
type u1590bCell struct {
	name, sql, pg string
	ordered       bool
}

func u1590bCells() []u1590bCell {
	var out []u1590bCell
	for _, k := range u1590bKeys {
		r := strings.NewReplacer("{K}", k.k, "{G}", k.g)
		for _, tp := range u1590bKeyTemplates {
			c := u1590bCell{name: k.name + "/" + tp.name, sql: r.Replace(tp.sql), ordered: tp.ordered}
			if tp.pg != "" {
				c.pg = r.Replace(tp.pg)
			}
			out = append(out, c)
		}
	}
	return append(out, u1590bFixed...)
}

// u1590bOrderedRender is byRender without the sort: the rows in arrival order.
func u1590bOrderedRender(res *oracle.Result) string {
	var rows []string
	for _, r := range brCells(res) {
		cells := make([]string, len(r))
		for i, v := range r {
			if v == nil {
				cells[i] = "NULL"
				continue
			}
			cells[i] = fmt.Sprint(v)
		}
		rows = append(rows, strings.Join(cells, ","))
	}
	return fmt.Sprintf("ordered rows=%d %s", len(rows), strings.Join(rows, " | "))
}

func u1590bPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_1590b_grouping_window_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, want, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("malformed answer line %q", line)
		}
		out[name] = want
	}
	return out
}

// u1590bResidual are cells still wrong at this fix, each with its cause; the
// test holds them WRONG so a later fix has to delete the line.
// The first two are not GROUPING defects; every one answers the same at the
// base (0ff2efdf) as here.
var u1590bResidual = map[string]string{
	// bigint / numeric keeps ADR-0024's fixed division scale (21 fraction
	// digits here), where PostgreSQL's select_div_scale keeps 16: the
	// partitions and values agree (x/case-label-share-int is right).
	"x/case-label-share": "fixed numeric division scale (ADR-0024), not PostgreSQL's select_div_scale",
	// GROUPING(<output alias>) is refused 42703 on both sides; the message
	// reads `unknown column "k"` here, `column "k" does not exist` there.
	"x/alias-grouping": "42703 on both sides; message wording differs",
	// Round 3, each the same at the base and at this fix (lines for #1590):
	// the WINDOW clause is not parsed (42601 where PostgreSQL answers).
	"e05-named-win": "named WINDOW clause unsupported: 42601",
	// ORDER BY a value COMPUTED from GROUPING(k) is #597's bound (0A000):
	// only a bare call reads its slot.
	"o/expr-plus": "ORDER BY GROUPING(k) + 0: computed sort key over an aggregate refused 0A000 (#597)",
	"o/expr-case": "ORDER BY CASE WHEN GROUPING(k) ...: computed sort key over an aggregate refused 0A000 (#597)",
	// GROUPING in QUALIFY outside a window call is skipped by the QUALIFY
	// outer-aggregate hoist: no rows where PostgreSQL's derived-table
	// spelling answers six, silently.
	"q/outside-window": "QUALIFY GROUPING(k) outside a window call: rows=0, silently",
	// an aggregate inside a window spec with no GROUP BY does not make the
	// query aggregated: a row per input row where PostgreSQL answers one.
	"z/win-agg-nogroup": "count(*) OVER (PARTITION BY count(*)) with no GROUP BY: 7 rows where PostgreSQL answers 1",
}

// TestArc1590bGroupingInWindowEveryArm holds every cell to PostgreSQL 17.11's
// answer on all five arms. U1590B_DUMP=<path> writes every (cell, arm)
// verdict instead of failing (the base-against-tip comparison);
// U1590B_LIST=<path> writes the cells for measuring PostgreSQL.
func TestArc1590bGroupingInWindowEveryArm(t *testing.T) {
	cells := u1590bCells()
	if p := os.Getenv("U1590B_LIST"); p != "" {
		var b strings.Builder
		for _, c := range cells {
			sql, ord := c.sql, 0
			if c.pg != "" {
				sql = c.pg
			}
			if c.ordered {
				ord = 1
			}
			fmt.Fprintf(&b, "%s\t%d\t%s\n", c.name, ord, sql)
		}
		if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if testing.Short() {
		t.Skip("-short: five arms over the #1590 GROUPING-in-window matrix")
	}
	answers := u1590bPGAnswers(t)
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	for name := range u1590bResidual {
		if _, ok := answers[name]; !ok {
			t.Fatalf("residual %s names no cell", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := byArmsOver(t, ctx, u1590bTables())
	dump := os.Getenv("U1590B_DUMP")
	var dumpLines []string
	compared := 0
	for _, c := range cells {
		want := answers[c.name]
		gots := make([]string, len(arms))
		oks := make([]bool, len(arms))
		var wg sync.WaitGroup
		for i, arm := range arms {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := arm.run(c.sql)
				if c.ordered && err == nil {
					gots[i] = u1590bOrderedRender(res)
					oks[i] = gots[i] == want
					return
				}
				gots[i], oks[i] = u1590Judge(want, res, err)
			}()
		}
		wg.Wait()
		for i, arm := range arms {
			if dump != "" {
				verdict := "right"
				if !oks[i] {
					verdict = "wrong"
					if strings.HasPrefix(gots[i], "ERR ") {
						verdict = "refused"
					}
				}
				dumpLines = append(dumpLines, fmt.Sprintf("%s\t%s\t%s\t%s", c.name, arm.name, verdict, gots[i]))
				continue
			}
			_, residual := u1590bResidual[c.name]
			switch {
			case oks[i] && residual:
				t.Errorf("%s [%s] now AGREES with PostgreSQL: delete its u1590bResidual line", c.name, arm.name)
			case !oks[i] && !residual:
				t.Errorf("%s\n  sql  %s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, arm.name, gots[i], want)
			}
			compared++
		}
	}
	if dump != "" {
		sort.Strings(dumpLines)
		if err := os.WriteFile(dump, []byte(strings.Join(dumpLines, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if want := len(arms) * len(cells); compared != want {
		t.Errorf("%d (cell, arm) comparisons, want %d", compared, want)
	}
}
