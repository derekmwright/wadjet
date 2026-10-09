// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// AN EXPRESSION SUBQUERY'S BODY IS PARSED ONCE PER STATEMENT (ADR-0032's
// 2026-10-09 amendment, ADR-0047 stage 3, arc CI3 round 2): the parse the
// statement's reader makes of a body (bodySyntax) is the memo on its node,
// and every requester — the binder, the aggregate-placement check, the
// outer-column candidates, the dangling-reference guard, the read bound, the
// FROM-less unfold, the declaration, the decorrelators, the compiler — reads
// that one tree. Counted with plansql.SetParseProbe over the closure review's
// statements (tooling/arcs/ci3_subquery_binding/ci3_review_r1,
// ci3rev1_parsecount_*.tsv) on the embedded door, cm_p (3 rows) / cm_big
// (20000), answers PostgreSQL 17.11's over the same rows.
//
//   - UNCORRELATED: each body text is parsed exactly as many times as the
//     statement writes a body so spelled (u08 writes two). At a4054c18 each
//     was parsed 7–16 times, at 542b4f37 11–29.
//   - CORRELATED: the whole statement's parses (the per-row re-run reads
//     substituted TEXT, so it parses privately, ADR-0032's exception) stay at
//     or under 542b4f37's recorded count; k07/k09/k10 hold a derived table
//     or a LATERAL item in the body, which parsed MORE at a4054c18 (B2).
//   - NESTED: the reviewer's shapes A (a derived table holding its own WITH
//     and a scalar subquery, per level) and B (the same without the nested
//     WITH) at depth 1–6: the innermost body parses once, and the statement's
//     parse count grows with a fitted log-log degree ≤ 2 in depth. At
//     a4054c18 it grew ×3 per level (10200 parses at depth 6), at 542b4f37
//     ×1.6 (1126).
//
// k09's answer is the LATERAL-in-a-correlated-body family (#1275), base
// identical and not this stage's; the row counts its parses only.
func TestArcCI3SubqueryBodyParsesOnce(t *testing.T) {
	db := cmOpen(t, 0)
	var mu sync.Mutex
	counts := map[string]int{}
	restore := plansql.SetParseProbe(func(sql string) {
		k := strings.Join(strings.Fields(sql), " ")
		mu.Lock()
		counts[k]++
		mu.Unlock()
	})
	t.Cleanup(restore)
	run := func(name, sql, want string) (map[string]int, int) {
		t.Helper()
		mu.Lock()
		counts = map[string]int{}
		mu.Unlock()
		got := cmAnswer(t, db, sql)
		mu.Lock()
		c := counts
		counts = map[string]int{}
		mu.Unlock()
		total := 0
		for _, n := range c {
			total += n
		}
		switch {
		case strings.HasPrefix(got, "ERR "):
			t.Errorf("%s: %s", name, got)
		case want != "" && got != want:
			t.Errorf("%s answer\n  got  %s\n  want %s", name, got, want)
		}
		return c, total
	}

	type body struct {
		text  string
		count int
	}
	uncorrelated := []struct {
		name, sql, want string
		bodies          []body
	}{
		{"u01", "SELECT id, (SELECT max(id) FROM cm_p) FROM cm_p ORDER BY 1", "1 3; 2 3; 3 3",
			[]body{{"SELECT max(id) FROM cm_p", 1}}},
		{"u02", "SELECT id FROM cm_big WHERE id IN (SELECT id + 2 FROM cm_p) ORDER BY 1", "3; 4; 5",
			[]body{{"SELECT id + 2 FROM cm_p", 1}}},
		{"u03", "SELECT count(*) FROM cm_big WHERE EXISTS (SELECT 1 FROM cm_p WHERE id = 2)", "20000",
			[]body{{"SELECT 1 FROM cm_p WHERE id = 2", 1}}},
		{"u04", "SELECT count(*) FROM cm_big WHERE id > (SELECT avg(id) FROM cm_p) AND id < (SELECT max(id) * 10 FROM cm_p)", "27",
			[]body{{"SELECT avg(id) FROM cm_p", 1}, {"SELECT max(id) * 10 FROM cm_p", 1}}},
		{"u05", "WITH s AS (SELECT id FROM cm_p) SELECT (SELECT max(id) FROM s)", "3",
			[]body{{"SELECT max(id) FROM s", 1}}},
		{"u06", "WITH s AS (SELECT id FROM cm_p) SELECT (SELECT (SELECT max(id) FROM s) + 1)", "4",
			[]body{{"SELECT (SELECT max(id) FROM s) + 1", 1}, {"SELECT max(id) FROM s", 1}}},
		{"u07", "SELECT x.d FROM (WITH t AS (SELECT 7 AS r) SELECT (SELECT r FROM t) AS d) x", "7",
			[]body{{"SELECT r FROM t", 1}}},
		{"u08", "WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id, (SELECT max(id) FROM c) AS m FROM c) d WHERE id = (SELECT max(id) FROM c) + 1", "2 2",
			[]body{{"SELECT max(id) FROM c", 2}}},
		{"u09", "SELECT sum((SELECT max(id) FROM cm_p)) FROM cm_big", "60000",
			[]body{{"SELECT max(id) FROM cm_p", 1}}},
		{"u10", "SELECT id % 2, count(*) FROM cm_big GROUP BY id % 2 HAVING count(*) > (SELECT count(*) FROM cm_p) ORDER BY 1", "0 10000; 1 10000",
			[]body{{"SELECT count(*) FROM cm_p", 1}}},
		{"u11", "SELECT id FROM cm_p ORDER BY id * (SELECT max(id) FROM cm_p) DESC", "3; 2; 1",
			[]body{{"SELECT max(id) FROM cm_p", 1}}},
		{"u12", "SELECT id, CASE WHEN id > 1 THEN (SELECT min(v) FROM cm_big) END FROM cm_p ORDER BY 1", "1 <nil>; 2 1.5; 3 1.5",
			[]body{{"SELECT min(v) FROM cm_big", 1}}},
	}
	for _, c := range uncorrelated {
		got, total := run(c.name, c.sql, c.want)
		for _, b := range c.bodies {
			if n := got[b.text]; n != b.count {
				t.Errorf("%s: body %q parsed %d times, want %d (statement total %d)", c.name, b.text, n, b.count, total)
			}
		}
	}

	// base542 is the statement's whole parse count at 542b4f37 (the closure
	// review's evidence/pc_base.txt and pc3_base.txt, the same counter).
	correlated := []struct {
		name, sql, want string
		base542         int
	}{
		{"k01", "SELECT t.id, (SELECT count(*) FROM cm_p d WHERE d.id <= t.id) FROM cm_p t ORDER BY 1", "1 1; 2 2; 3 3", 39},
		{"k02", "SELECT count(*) FROM cm_big t WHERE t.id <= 50 AND EXISTS (SELECT 1 FROM cm_p p WHERE p.id = t.id)", "3", 12},
		{"k03", "SELECT sum((SELECT min(id) FROM cm_big b WHERE b.id >= t.id)) FROM cm_p t", "6", 49},
		{"k04", "WITH s AS (SELECT id AS g FROM cm_big) SELECT sum((SELECT min(g) FROM s WHERE g >= t.id)) FROM cm_p t", "6", 48},
		{"k05", "SELECT t.id FROM cm_p t WHERE t.v < (SELECT max(v) FROM cm_big c WHERE c.id < 10 AND c.id <> t.id) ORDER BY 1", "1; 2; 3", 27},
		{"k06", "WITH s AS (SELECT random() r) SELECT count(DISTINCT (SELECT r + t.id*0 FROM s)) FROM cm_big t WHERE t.id <= 50", "1", 188},
		{"k07", "SELECT t.id, (SELECT count(*) FROM (SELECT id AS x FROM cm_p) t2 WHERE t2.x > t.id) FROM cm_p t ORDER BY 1", "1 2; 2 1; 3 0", 61},
		{"k08", "SELECT t.id, (SELECT max(d.id) FROM cm_p d WHERE d.id <= (SELECT max(p.id) FROM cm_p p WHERE p.id <= t.id) + 1) FROM cm_p t ORDER BY 1", "1 2; 2 3; 3 3", 179},
		{"k07a", "SELECT t.id, (SELECT count(*) FROM (SELECT id AS x FROM cm_p) t2 WHERE t2.x > t.id) FROM cm_big t WHERE t.id <= 30 ORDER BY 1", "", 196},
		{"k07b", "SELECT t.id, (SELECT count(*) FROM (SELECT id AS x FROM cm_p) t2 WHERE t2.x > t.id) FROM cm_big t WHERE t.id <= 300 ORDER BY 1", "", 1546},
		{"k01a", "SELECT t.id, (SELECT count(*) FROM cm_p d WHERE d.id <= t.id) FROM cm_big t WHERE t.id <= 30 ORDER BY 1", "", 120},
		{"k01b", "SELECT t.id, (SELECT count(*) FROM cm_p d WHERE d.id <= t.id) FROM cm_big t WHERE t.id <= 300 ORDER BY 1", "", 930},
		{"k09", "SELECT t.id, (SELECT count(*) FROM cm_p d, LATERAL (SELECT d.id AS z) l WHERE l.z > t.id) FROM cm_big t WHERE t.id <= 30 ORDER BY 1", "", 264},
		{"k10", "WITH s AS (SELECT id AS g FROM cm_p) SELECT t.id, (SELECT count(*) FROM (SELECT g FROM s) q WHERE q.g > t.id) FROM cm_big t WHERE t.id <= 30 ORDER BY 1", "", 197},
	}
	for _, c := range correlated {
		_, total := run(c.name, c.sql, c.want)
		t.Logf("%s parses=%d (542b4f37: %d)", c.name, total, c.base542)
		if total > c.base542 {
			t.Errorf("%s: %d parses, want <= %d (542b4f37's count)", c.name, total, c.base542)
		}
	}

	const inner = "SELECT max(id) FROM c"
	shapes := map[string]func(int) string{
		"A": func(d int) string {
			s := "SELECT (" + inner + ") AS m"
			for i := 0; i < d; i++ {
				s = fmt.Sprintf("SELECT (SELECT max(m) FROM (WITH c AS (SELECT %d AS id) %s) x%d) AS m", i, s, i)
			}
			return "WITH c AS (SELECT 99 AS id) " + s
		},
		"B": func(d int) string {
			s := "SELECT (" + inner + ") AS m"
			for i := 0; i < d; i++ {
				s = fmt.Sprintf("SELECT (SELECT max(m) FROM (%s) x%d) AS m", s, i)
			}
			return "WITH c AS (SELECT 99 AS id) " + s
		},
	}
	for _, name := range []string{"A", "B"} {
		want := map[string]string{"A": "0", "B": "99"}[name]
		var xs, ys []float64
		var line []string
		for d := 1; d <= 6; d++ {
			got, total := run(fmt.Sprintf("%s%d", name, d), shapes[name](d), want)
			if n := got[inner]; n != 1 {
				t.Errorf("%s%d: the innermost body parsed %d times, want 1", name, d, n)
			}
			xs, ys = append(xs, math.Log(float64(d))), append(ys, math.Log(float64(total)))
			line = append(line, fmt.Sprint(total))
		}
		degree := fittedSlope(xs, ys)
		t.Logf("shape %s parses at depth 1..6: %s (fitted degree %.2f)", name, strings.Join(line, ", "), degree)
		if degree > 2 {
			t.Errorf("shape %s: parses grow with fitted degree %.2f in depth (%s), want <= 2", name, degree, strings.Join(line, ", "))
		}
	}
}

// fittedSlope is the least-squares slope of ys over xs.
func fittedSlope(xs, ys []float64) float64 {
	var sx, sy, sxx, sxy float64
	n := float64(len(xs))
	for i := range xs {
		sx += xs[i]
		sy += ys[i]
		sxx += xs[i] * xs[i]
		sxy += xs[i] * ys[i]
	}
	return (n*sxy - sx*sy) / (n*sxx - sx*sx)
}

// PLANNING A NESTED SUBQUERY COSTS NO HIGHER POWER OF ITS DEPTH THAN AT BASE
// (arc CI3 round 2, B3): the closure review's shape A — each level a derived
// table holding its own WITH and a scalar subquery over it — planned
// (EXPLAIN) at depth 1–8 on the embedded door, the best of three runs per
// depth as wadjet.TestArcCI2DeclarationPlanningBound takes it. The fitted
// growth per level (least squares of log time over depth) was 2.07 at
// 542b4f37 and 3.1 at a4054c18, whose declaration planned each body three
// times with a fresh memo per level (depth 8: 59 ms → 5.1 s). The limit is
// 2.5 per level, between the two, and depth 8 must plan inside two seconds.
func TestArcCI3SubqueryNestingPlanningBound(t *testing.T) {
	db := cmOpen(t, 0)
	ctx := context.Background()
	shape := func(d int) string {
		s := "SELECT (SELECT max(id) FROM c) AS m"
		for i := 0; i < d; i++ {
			s = fmt.Sprintf("SELECT (SELECT max(m) FROM (WITH c AS (SELECT %d AS id) %s) x%d) AS m", i, s, i)
		}
		return "WITH c AS (SELECT 99 AS id) " + s
	}
	const perLevelLimit = 2.5
	const depth8Limit = 2 * time.Second
	var xs, ys []float64
	var line []string
	for d := 1; d <= 8; d++ {
		q := "EXPLAIN " + shape(d)
		best := time.Duration(math.MaxInt64)
		for rep := 0; rep < 3; rep++ {
			start := time.Now()
			_, err := db.Query(ctx, q)
			if el := time.Since(start); el < best {
				best = el
			}
			if err != nil {
				t.Fatalf("depth %d: %v", d, err)
			}
		}
		xs = append(xs, float64(d))
		ys = append(ys, math.Log(float64(best)))
		line = append(line, fmt.Sprintf("%d:%.2fms", d, float64(best.Microseconds())/1000))
		if d == 8 && best > depth8Limit {
			t.Errorf("depth 8 planned in %s, want < %s", best, depth8Limit)
		}
		if best > 4*depth8Limit {
			t.Fatalf("depth %d planned in %s: stopped (%s)", d, best, strings.Join(line, " "))
		}
	}
	perLevel := math.Exp(fittedSlope(xs, ys))
	t.Logf("shape A planning, best of 3: %s (fitted ×%.2f per level)", strings.Join(line, " "), perLevel)
	if perLevel > perLevelLimit {
		t.Errorf("planning grows ×%.2f per nesting level (%s), want <= ×%.1f (542b4f37: ×2.07)", perLevel, strings.Join(line, " "), perLevelLimit)
	}
	if got := cmAnswer(t, db, shape(4)); got != "0" {
		t.Errorf("depth 4 answered %s, want 0 (PostgreSQL 17.11: the innermost c is its level's)", got)
	}
}
