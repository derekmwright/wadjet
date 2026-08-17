package sql

import (
	"strings"
	"testing"
)

// FuzzParseSQL covers the hand-written recursive-descent parser (#290).
// The parser accepts external input over pgwire, HTTP, gRPC, and MCP; the
// existing fuzz targets are all storage-decoder-layer.
//
// Invariants:
//  1. Always terminate without panicking, whatever the bytes. Incomplete
//     or malformed input must return a parse error, never loop forever
//     (the fuzzer flags a panic or a hang).
//  2. Expression fixed-point: for a successfully parsed SELECT with a
//     WHERE clause, printing the expression AST and re-parsing it must
//     reach a fixed point — print(parse(print(e))) == print(e). A parse
//     that silently drops part of a clause (the #281 class: CTE joins
//     silently dropped) breaks the fixed point. This is scoped to
//     expressions because they are the only ASTs with a printer today;
//     extending to full statements needs a statement printer first.
//
// Seed corpus: representative shapes from the ClickBench and TPC-H
// dialects plus known-tricky constructs (CTEs, grouping sets, window
// frames, nested parens, quoted identifiers, unicode, deeply nested
// expressions).
func FuzzParseSQL(f *testing.F) {
	seeds := []string{
		"SELECT COUNT(*) FROM hits",
		"SELECT COUNT(DISTINCT UserID) FROM hits",
		"SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
		"SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
		"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(LENGTH(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
		"SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10",
		"SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
		"SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty FROM lineitem WHERE l_shipdate <= DATE '1998-12-01' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus",
		"WITH revenue AS (SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue FROM lineitem GROUP BY l_suppkey) SELECT s_suppkey FROM supplier, revenue WHERE s_suppkey = supplier_no",
		"SELECT c_count, COUNT(*) AS custdist FROM (SELECT c_custkey, COUNT(o_orderkey) AS c_count FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%' GROUP BY c_custkey) AS c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC",
		"SELECT SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) FROM lineitem, part WHERE l_partkey = p_partkey",
		"SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
		"SELECT * FROM t WHERE a IN (1, 2, 3) AND b BETWEEN 4 AND 5 AND c IS NOT NULL AND NOT (d = 6 OR e <> 7)",
		"SELECT CAST(x AS BIGINT), COALESCE(y, 0), CASE x WHEN 1 THEN 'a' ELSE 'b' END FROM t GROUP BY GROUPING SETS ((x), (y), ())",
		"INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')",
		"UPDATE t SET a = a + 1 WHERE b = 'z'",
		"DELETE FROM t WHERE a >= 400",
		"SELECT \"quoted col\", t.\"another\" FROM \"my table\" t",
		"SELECT * FROM t WHERE s = 'it''s' AND u = 'üñî'",
		"SELECT ((((((1))))))",
		"EXPLAIN SELECT 1",
		"",
		";;;",
		"SELECT",
		"SELECT * FROM",
		"'",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		// Unbounded inputs make the fuzzer chase parser-stack depth
		// rather than logic; the wire layers bound statement size far
		// below this anyway.
		if len(sql) > 1<<16 {
			return
		}
		pq, err := Parse(sql)
		if err != nil || pq == nil {
			return // rejecting is always fine; not panicking is the invariant
		}

		// Expression fixed-point on the WHERE clause.
		info := pq.SelectInfo
		if info == nil || info.WhereExpr == nil {
			return
		}
		printed := info.WhereExpr.String()
		if strings.TrimSpace(printed) == "" {
			t.Fatalf("non-nil WHERE printed empty: input %q", sql)
		}
		pq2, err2 := Parse("SELECT 1 FROM t WHERE " + printed)
		if err2 != nil || pq2 == nil || pq2.SelectInfo == nil || pq2.SelectInfo.WhereExpr == nil {
			t.Fatalf("printed WHERE does not re-parse: %q (from input %q, err %v)", printed, sql, err2)
		}
		reprinted := pq2.SelectInfo.WhereExpr.String()
		if reprinted != printed {
			t.Fatalf("expression round-trip not a fixed point:\n input:     %q\n printed:   %q\n reprinted: %q", sql, printed, reprinted)
		}
	})
}
