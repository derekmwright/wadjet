package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// QUALIFY ANSWERS WHAT DUCKDB ANSWERS, ON FIVE ARMS — #1076.
//
// The clause was PARSED and then IGNORED: `SelectInfo.Qualify` was filled in
// and nothing consumed it, so every row came back. A plausible superset is the
// one wrong answer a client cannot detect, and this table is the measurement
// that says so cell by cell — every one of the 31 below answered the
// UNFILTERED rows at c34cdbcb.
//
// PostgreSQL 17.11 has no QUALIFY and so cannot be the oracle here. DuckDB
// 1.1.3 is, and ADR-0012 records that; every `want` was measured there over
// rows identical to this package's `lat_ord` / `lat_item` fixtures. The
// mechanism the cells hold the engine to is in
// `logical/qualify.go`: a FILTER over the window's output, below the
// projection, whose bare names bind the INPUT relation first and a SELECT-list
// alias second.
//
// Two cells are not row sets:
//
//   - `noWindow` is a REFUSAL both engines raise. DuckDB's binder says "at
//     least one window function must appear in the SELECT column or QUALIFY
//     clause"; wadjet says the same thing in its own words, and the assertion
//     is that it refuses rather than filtering, because a QUALIFY no window
//     reaches means something WHERE already means.
//   - `overJoin` is a PIN, and it is not a QUALIFY defect at all: a window
//     whose PARTITION BY names a join arm's column binds the other arm's
//     column of that bare name, which reproduces with no QUALIFY in the
//     query. See TestArcL1AWindowKeyBindsItsOwnJoinArm.

type l1QCase struct{ name, sql string }

func l1QualifyCases() []l1QCase {
	return []l1QCase{
		{"aliasRn", "SELECT u.id AS id, ROW_NUMBER() OVER (ORDER BY u.id) AS rn FROM lat_ord u QUALIFY rn = 1 ORDER BY id"},
		{"aliasRnLe", "SELECT u.id AS id, ROW_NUMBER() OVER (ORDER BY u.id) AS rn FROM lat_ord u QUALIFY rn <= 2 ORDER BY id"},
		{"inline", "SELECT u.id AS id FROM lat_ord u QUALIFY ROW_NUMBER() OVER (ORDER BY u.id) = 1 ORDER BY id"},
		{"inlinePart", "SELECT i.order_id AS g, i.product AS p FROM lat_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount DESC) = 1 ORDER BY g, p"},
		{"noWindow", "SELECT u.id AS id FROM lat_ord u QUALIFY u.id = 1 ORDER BY id"},
		{"noWindowSel", "SELECT u.id AS id, ROW_NUMBER() OVER (ORDER BY u.id) AS rn FROM lat_ord u QUALIFY u.id = 1 ORDER BY id"},
		{"nonSelected", "SELECT i.product AS p FROM lat_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount DESC) = 1 ORDER BY p"},
		{"withWhere", "SELECT i.product AS p, i.amount AS m FROM lat_item i WHERE i.amount > 50 QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount) = 1 ORDER BY p"},
		{"withGroupBy", "SELECT i.order_id AS g, SUM(i.amount) AS m FROM lat_item i GROUP BY i.order_id QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(i.amount) DESC) = 1 ORDER BY g"},
		{"withHaving", "SELECT i.order_id AS g, SUM(i.amount) AS m FROM lat_item i GROUP BY i.order_id HAVING SUM(i.amount) > 100 QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(i.amount)) = 1 ORDER BY g"},
		{"withOrderLimit", "SELECT i.id AS id FROM lat_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount) <= 2 ORDER BY id DESC LIMIT 2"},
		{"withDistinct", "SELECT DISTINCT i.order_id AS g FROM lat_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount) = 1 ORDER BY g"},
		{"andPredicate", "SELECT i.id AS id FROM lat_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount) = 1 AND i.amount > 60 ORDER BY id"},
		{"orPredicate", "SELECT i.id AS id FROM lat_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount) = 1 OR i.amount > 100 ORDER BY id"},
		{"notPredicate", "SELECT i.id AS id FROM lat_item i QUALIFY NOT (ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount) = 1) ORDER BY id"},
		{"aggWindow", "SELECT i.id AS id FROM lat_item i QUALIFY SUM(i.amount) OVER (PARTITION BY i.order_id) > 150 ORDER BY id"},
		{"twoWindows", "SELECT i.id AS id FROM lat_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount) = 1 AND COUNT(*) OVER (PARTITION BY i.order_id) = 2 ORDER BY id"},
		{"rank", "SELECT i.id AS id FROM lat_item i QUALIFY RANK() OVER (PARTITION BY i.order_id ORDER BY i.amount DESC) = 1 ORDER BY id"},
		{"lag", "SELECT i.id AS id FROM lat_item i QUALIFY LAG(i.amount) OVER (ORDER BY i.id) IS NOT NULL ORDER BY id"},
		{"exprAlias", "SELECT i.id AS id, ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount) * 10 AS rn FROM lat_item i QUALIFY rn = 10 ORDER BY id"},
		{"aliasInExpr", "SELECT i.id AS id, ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount) AS rn FROM lat_item i QUALIFY rn + 0 = 1 ORDER BY id"},
		{"inDerived", "SELECT d.id AS id FROM (SELECT i.id AS id FROM lat_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount) = 1) d ORDER BY id"},
		{"inCTE", "WITH d AS (SELECT i.id AS id FROM lat_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount) = 1) SELECT d.id AS id FROM d ORDER BY id"},
		{"overJoin", "SELECT o.id AS a, i.id AS b FROM lat_ord o JOIN lat_item i ON i.order_id = o.id QUALIFY ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY i.amount DESC) = 1 ORDER BY a, b"},
		{"overLeftJoin", "SELECT o.id AS a, i.id AS b FROM lat_ord o LEFT JOIN lat_item i ON i.order_id = o.id QUALIFY ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY i.amount DESC) = 1 ORDER BY a, b"},
		{"overLateral", "SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.order_id = o.id) s ON true QUALIFY ROW_NUMBER() OVER (ORDER BY o.id) <= 2 ORDER BY a"},
		{"star", "SELECT * FROM lat_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount DESC) = 1 ORDER BY 1"},
		{"emptyResult", "SELECT i.id AS id FROM lat_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount) = 99 ORDER BY id"},
		{"setopArm", "SELECT i.id AS id FROM lat_item i QUALIFY ROW_NUMBER() OVER (ORDER BY i.id) = 1 UNION ALL SELECT 99 ORDER BY id"},
		{"frameWindow", "SELECT i.id AS id FROM lat_item i QUALIFY SUM(i.amount) OVER (ORDER BY i.id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) > 150 ORDER BY id"},
		{"windowOnly", "SELECT COUNT(*) AS n FROM lat_item i QUALIFY COUNT(*) OVER () = 1"},
	}
}

// l1QualifyDuckDB is DuckDB 1.1.3's answer for each cell, rendered by
// r1RenderRows (a sorted ROW SET, so a legal ordering difference is never read
// as a wrong answer).
var l1QualifyDuckDB = map[string]string{
	"aliasRn":        "rows=1 1,1",
	"aliasRnLe":      "rows=2 1,1 | 2,2",
	"inline":         "rows=1 1",
	"inlinePart":     "rows=2 1,Gadget | 2,Doohickey",
	"noWindow":       "ERR Binder Error: at least one window function must appear in the SELECT column or QUALIFY clause",
	"noWindowSel":    "rows=1 1,1",
	"nonSelected":    "rows=2 Doohickey | Gadget",
	"withWhere":      "rows=2 Gadget,100 | Widget,75",
	"withGroupBy":    "rows=1 2,200",
	"withHaving":     "rows=1 1,150",
	"withOrderLimit": "rows=2 3 | 4",
	"withDistinct":   "rows=2 1 | 2",
	"andPredicate":   "rows=1 3",
	"orPredicate":    "rows=3 1 | 3 | 4",
	"notPredicate":   "rows=2 2 | 4",
	"aggWindow":      "rows=2 3 | 4",
	"twoWindows":     "rows=2 1 | 3",
	"rank":           "rows=2 2 | 4",
	"lag":            "rows=3 2 | 3 | 4",
	"exprAlias":      "rows=2 1,10 | 3,10",
	"aliasInExpr":    "rows=2 1,1 | 3,1",
	"inDerived":      "rows=2 1 | 3",
	"inCTE":          "rows=2 1 | 3",
	"overJoin":       "rows=2 1,2 | 2,4",
	"overLeftJoin":   "rows=3 1,2 | 2,4 | 3,NULL",
	"overLateral":    "rows=2 1,150 | 2,200",
	"star":           "rows=2 2,1,Gadget,100 | 4,2,Doohickey,125",
	"emptyResult":    "rows=0 ",
	"setopArm":       "rows=2 1 | 99",
	"frameWindow":    "rows=2 3 | 4",
	"windowOnly":     "rows=1 4",
}

// l1QualifyRefuses holds the cells where the answer is an ERROR, by the
// substring the refusal must carry. A pin that starts agreeing FAILS.
var l1QualifyRefuses = map[string]string{
	"noWindow": "QUALIFY requires a window function",
}

// l1QualifyPins holds a cell whose divergence is a DIFFERENT defect, with the
// measurement that localises it. A pin that starts agreeing FAILS.
var l1QualifyPins = map[string]string{
	// A window PARTITION BY naming a join arm's column binds the arm the
	// reorderer emitted BARE, so every row lands in its own partition and
	// `ROW_NUMBER() = 1` admits all four. It reproduces with no QUALIFY
	// (TestArcL1AWindowKeyBindsItsOwnJoinArm) and the fix belongs there.
	"overJoin": "rows=4 1,1 | 1,2 | 2,3 | 2,4",
}

func TestArcL1QualifyAnswersDuckDBOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the QUALIFY table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := r1Arms(t, ctx)
	seen := map[string]bool{}
	for _, tc := range l1QualifyCases() {
		want, ok := l1QualifyDuckDB[tc.name]
		if !ok {
			t.Fatalf("%s: no DuckDB row set recorded — the table and the "+
				"measurement have diverged", tc.name)
		}
		seen[tc.name] = true
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got := arm.run(tc.sql)
				if refusal, pinned := l1QualifyRefuses[tc.name]; pinned {
					if !strings.Contains(got, refusal) {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  the pinned refusal (%q) is gone: "+
							"assert DuckDB's answer %s and delete this cell's pin",
							tc.sql, arm.name, got, refusal, want)
					}
					continue
				}
				if pin, pinned := l1QualifyPins[tc.name]; pinned {
					if got != pin {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  the pinned divergence is gone: "+
							"assert DuckDB's answer %s and delete this cell's pin",
							tc.sql, arm.name, got, want)
					}
					continue
				}
				if got != want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (DuckDB 1.1.3)",
						tc.sql, arm.name, got, want)
				}
			}
		})
	}
	for name := range l1QualifyDuckDB {
		if !seen[name] {
			t.Errorf("%s: a DuckDB row set is recorded for a cell the table no longer writes", name)
		}
	}
}
