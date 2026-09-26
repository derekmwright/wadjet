// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A REPEATED CORRELATED EQUALITY ON ONE INNER COLUMN KEEPS ITS FIRST
// PUBLICATION — arc JP round 6 closure review, B1 (#1299/#1302).
//
// `q.qk = p.k AND q.qk = p.oid` — two correlated equalities naming the SAME
// inner column — published the key ONCE as `__key_0` (builder.go's
// key-injection loop, ~line 2290), but the SECOND equality then found `q.qk`
// already in the injected list and OVERWROTE keyRename["qk"] with the bare
// `qk`, so BOTH join conditions were respelled to read `s.qk` — a name the
// arm no longer publishes. Single-process: no row matches (or every
// aggregate reads NULL). Stage DAG: `key "s.qk" not in schema`. Round 5
// opened two doors onto it: lateralWindowsPerOuterRow answers a window body
// the old rule refused, and the JPID guard routes some of these DAG
// spellings onto the wrong single-process answer, unpinned.
//
// Fix: builder.go's key-injection loop skips a correlated part whose inner
// key keyRename ALREADY records — the first pass's publication stands, and
// every equality naming that key (however many name it) resolves to the
// SAME published slot.
//
// Residual, unrelated to this defect and UNCHANGED by the fix (measured
// identically at base a1892b54 and at the fixed tip, single/spilled/fastpath
// already right at both): p2/sameColExprWin's EXPRESSION-keyed second
// correlated part (`q.qk + 0 = p.oid`) takes the LIFTED-key mint path
// (builder.go's "a slot the join cannot key on is read above it", #1302),
// and its shuffle-stage carrier loses that slot's name on dag/dag-shuffled —
// a loud shuffle failure ("key not in schema"), never a wrong value, and out
// of this fix's scope.
func TestArcJP6RepeatedCorrelatedKeyPublishesOnceOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the arc JP round-6 B1 (p2) corpus")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := jpArms(t, ctx)
	for _, tc := range jp6P2Cells {
		w, ok := jp6P2Postgres[tc.name]
		if !ok {
			t.Fatalf("%s: no PostgreSQL row set recorded", tc.name)
		}
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got := arm.run(tc.sql)
				if class, isLoud := jp6P2Loud[tc.name+"\x00"+arm.name]; isLoud {
					// A residual, PRE-EXISTING loud failure, unrelated to and
					// unchanged by this fix: not a wrong VALUE. A cell whose
					// pin starts answering PostgreSQL's rows FAILS — delete
					// its entry.
					if got == w {
						t.Errorf("%s\n  arm  %s\n  answers PostgreSQL's %s now: delete its jp6P2Loud entry", tc.sql, arm.name, w)
					} else if !strings.HasPrefix(got, "ERR ") || !strings.Contains(got, class) {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want an ERR containing %q, or PostgreSQL's %s", tc.sql, arm.name, got, class, w)
					}
					continue
				}
				if got != w {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, got, w)
				}
			}
		})
	}
}

var jp6P2Cells = []jpCell{
	{"p2/sameColWinRn", "SELECT p.id, s.qid, s.w FROM jp_o p JOIN LATERAL (SELECT q.qid, row_number() OVER (ORDER BY q.qid) AS w FROM jp_q q WHERE q.qk = p.k AND q.qk = p.oid) s ON true"},
	{"p2/sameColWinRev", "SELECT p.id, s.qid, s.w FROM jp_o p JOIN LATERAL (SELECT q.qid, count(*) OVER () AS w FROM jp_q q WHERE q.qk = p.k AND p.oid = q.qk) s ON true"},
	{"p2/sameColWinLeft", "SELECT p.id, s.qid, s.w FROM jp_o p LEFT JOIN LATERAL (SELECT q.qid, sum(q.qv) OVER () AS w FROM jp_q q WHERE q.qk = p.k AND q.qk = p.oid) s ON true"},
	{"p2/sameColQualify", "SELECT p.id, s.qid FROM jp_o p JOIN LATERAL (SELECT q.qid FROM jp_q q WHERE q.qk = p.k AND q.qk = p.oid QUALIFY row_number() OVER (ORDER BY q.qv DESC, q.qid) = 1) s ON true"},
	{"p2/sameColAgg", "SELECT p.id, s.n FROM jp_o p JOIN LATERAL (SELECT count(*) AS n FROM jp_q q WHERE q.qk = p.k AND q.qk = p.oid) s ON true"},
	{"p2/sameColAggLeft", "SELECT p.id, s.n FROM jp_o p LEFT JOIN LATERAL (SELECT sum(q.qv) AS n FROM jp_q q WHERE q.qk = p.k AND q.qk = p.oid) s ON true"},
	{"p2/sameColGroup", "SELECT p.id, s.qtag, s.n FROM jp_o p JOIN LATERAL (SELECT q.qtag, count(*) AS n FROM jp_q q WHERE q.qk = p.k AND q.qk = p.oid GROUP BY q.qtag) s ON true"},
	{"p2/sameColDistinct", "SELECT p.id, s.qtag FROM jp_o p JOIN LATERAL (SELECT DISTINCT q.qtag FROM jp_q q WHERE q.qk = p.k AND q.qk = p.oid) s ON true"},
	{"p2/sameColLimit", "SELECT p.id, s.qid FROM jp_o p JOIN LATERAL (SELECT q.qid FROM jp_q q WHERE q.qk = p.k AND q.qk = p.oid ORDER BY q.qid LIMIT 1) s ON true"},
	{"p2/sameColPlain", "SELECT p.id, s.qid FROM jp_o p JOIN LATERAL (SELECT q.qid FROM jp_q q WHERE q.qk = p.k AND q.qk = p.oid) s ON true"},
	{"p2/sameColLtI", "SELECT o.id, s.v FROM lt_o o JOIN LATERAL (SELECT i.v FROM lt_i i WHERE i.k = o.k AND i.k = o.k) s ON true"},
	{"p2/sameColLtIWin", "SELECT o.id, s.v, s.w FROM lt_o o JOIN LATERAL (SELECT i.v, rank() OVER (ORDER BY i.v) AS w FROM lt_i i WHERE i.k = o.k AND i.k = o.k) s ON true"},
	{"p2/diffColsWin", "SELECT p.id, s.qid, s.w FROM jp_o p JOIN LATERAL (SELECT q.qid, count(*) OVER () AS w FROM jp_q q WHERE q.qk = p.k AND q.qid = p.id) s ON true"},
	{"p2/diffColsPlain", "SELECT p.id, s.qid FROM jp_o p JOIN LATERAL (SELECT q.qid FROM jp_q q WHERE q.qk = p.k AND q.qid = p.id) s ON true"},
	{"p2/twoInnerOneOuter", "SELECT p.id, s.qid FROM jp_o p JOIN LATERAL (SELECT q.qid FROM jp_q q WHERE q.qk = p.k AND q.qid = p.k) s ON true"},
	{"p2/twoInnerOneOuterWin", "SELECT p.id, s.qid, s.w FROM jp_o p JOIN LATERAL (SELECT q.qid, count(*) OVER () AS w FROM jp_q q WHERE q.qk = p.k AND q.qid = p.k) s ON true"},
	{"p2/sameColExprWin", "SELECT p.id, s.qid, s.w FROM jp_o p JOIN LATERAL (SELECT q.qid, count(*) OVER () AS w FROM jp_q q WHERE q.qk = p.k AND q.qk + 0 = p.oid) s ON true"},
}

// jp6P2Postgres: PostgreSQL 17.11 over arc_jp_fixture_test.go (jp_o, jp_q,
// lt_o, lt_i) — measured live, wadjet-pg-jp6.
var jp6P2Postgres = map[string]string{
	"p2/sameColWinRn":        "rows=9 1,1,1 | 1,2,2 | 1,3,3 | 2,1,1 | 2,2,2 | 2,3,3 | 3,4,1 | 3,5,2 | 3,6,3",
	"p2/sameColWinRev":       "rows=9 1,1,3 | 1,2,3 | 1,3,3 | 2,1,3 | 2,2,3 | 2,3,3 | 3,4,3 | 3,5,3 | 3,6,3",
	"p2/sameColWinLeft":      "rows=11 1,1,50 | 1,2,50 | 1,3,50 | 2,1,50 | 2,2,50 | 2,3,50 | 3,4,120 | 3,5,120 | 3,6,120 | 4,NULL,NULL | 5,NULL,NULL",
	"p2/sameColQualify":      "rows=3 1,3 | 2,3 | 3,6",
	"p2/sameColAgg":          "rows=5 1,3 | 2,3 | 3,3 | 4,0 | 5,0",
	"p2/sameColAggLeft":      "rows=5 1,50 | 2,50 | 3,120 | 4,NULL | 5,NULL",
	"p2/sameColGroup":        "rows=6 1,a,2 | 1,b,1 | 2,a,2 | 2,b,1 | 3,x,2 | 3,y,1",
	"p2/sameColDistinct":     "rows=6 1,a | 1,b | 2,a | 2,b | 3,x | 3,y",
	"p2/sameColLimit":        "rows=3 1,1 | 2,1 | 3,4",
	"p2/sameColPlain":        "rows=9 1,1 | 1,2 | 1,3 | 2,1 | 2,2 | 2,3 | 3,4 | 3,5 | 3,6",
	"p2/sameColLtI":          "rows=9 1,10 | 1,10 | 1,30 | 2,10 | 2,10 | 2,30 | 3,20 | 3,40 | 3,60",
	"p2/sameColLtIWin":       "rows=9 1,10,1 | 1,10,1 | 1,30,3 | 2,10,1 | 2,10,1 | 2,30,3 | 3,20,1 | 3,40,2 | 3,60,3",
	"p2/diffColsWin":         "rows=2 1,1,1 | 2,2,1",
	"p2/diffColsPlain":       "rows=2 1,1 | 2,2",
	"p2/twoInnerOneOuter":    "rows=2 1,1 | 2,1",
	"p2/twoInnerOneOuterWin": "rows=2 1,1,1 | 2,1,1",
	"p2/sameColExprWin":      "rows=9 1,1,3 | 1,2,3 | 1,3,3 | 2,1,3 | 2,2,3 | 2,3,3 | 3,4,3 | 3,5,3 | 3,6,3",
}

// jp6P2Loud: the ONE pre-existing, unrelated DAG shuffle residual (see the
// test's doc comment) — a loud failure, never a wrong value, present
// identically at base a1892b54 and unchanged by this round's fix.
var jp6P2Loud = map[string]string{
	"p2/sameColExprWin\x00dag":          `key "__key_1" not in schema`,
	"p2/sameColExprWin\x00dag-shuffled": `key "__key_1" not in schema`,
}
