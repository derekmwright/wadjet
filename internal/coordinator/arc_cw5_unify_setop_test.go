// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Arc CW round 5. The round-4 closure review's B1 and B5 were one defect in two
// spellings: wherever two container operands MEET, their element types were
// unified per meeting point — `=` widened int ⊕ float8 to a double, while the
// hash-join key, the set operations and the FULL join's matched set keyed each
// side under its own element type (`int[] JOIN float8[]` 0 rows for 49, UNION 98
// for 49), the sort-merge key compared an INT64 child with the INT64 kernel over
// a FLOAT64 one (XX000), and CASE / COALESCE / GREATEST / LEAST / ARRAY[a, b]
// declared the FIRST operand's DECIMAL scale and rounded the others' elements
// into it (`{0.043333}` → `{0.04}`). Round 5 unifies the pair ONCE
// (batch.CommonContainerColumn), and every meeting point reads it.
//
// TestArcCW5ElementTypesUnifyAtEveryMeetingPoint is element pair × meeting
// point × six arms (single, spilled, DAG, DAG-shuffled; the join and set-op
// meeting points on the two sort-merge arms too). Every expected answer is
// PostgreSQL 17.11's over the explicit coercion it needs (cw_author5/): the
// pairs are built over typemx's first 20 ids so an equal pair is 20 equal rows
// and every wrong reading (a per-side key, a rounded element, an integer
// written as an unscaled DECIMAL carrier) answers a different count.
//
// TestArcCW5SetOperationArityDeclaresEveryArm is review B2: a set operation of
// THREE or more arms parses left-deep, so arm 1 of the outer operation IS a set
// operation, and the enclosing stage read it through its result NAME — for an
// unaliased first arm the TEXT of an expression (`array[d]`, `id + 0`), which
// the stage re-parsed and evaluated over the nested result's rows: a constant
// for every row, or a column the nested result does not carry. Arity 2, 3, 4 ×
// element × {UNION, UNION ALL, INTERSECT, EXCEPT} × unaliased / aliased.

type cw5Pair struct {
	name   string
	ta, tb string // element type spellings of the two sides
}

func cw5Pairs() []cw5Pair {
	return []cw5Pair{
		{"int-bigint", "INT", "BIGINT"},
		{"int-numeric", "INT", "DECIMAL(9,2)"},
		{"numeric-scales", "DECIMAL(5,2)", "DECIMAL(9,4)"},
		{"int-float8", "INT", "DOUBLE"},
		{"numeric-float8", "DECIMAL(9,2)", "DOUBLE"},
		{"float4-float8", "REAL", "DOUBLE"},
	}
}

type cw5Point struct {
	name string
	join bool // runs on the sort-merge arms too
	sql  func(p cw5Pair) string
	want string
	// singleOnly: the stage DAG answers this SHAPE wrong for every type,
	// integers included, identically at main 6cbe2041 — a filed distributed
	// defect, not this gate's element question.
	singleOnly bool
}

func cw5Points() []cw5Point {
	side := func(t, alias string) string {
		return "(SELECT ARRAY[CAST(id AS " + t + ")] AS " + alias + " FROM typemx WHERE id BETWEEN 1 AND 20)"
	}
	count := func(body string) string { return "SELECT COUNT(*) AS r FROM " + body }
	return []cw5Point{
		// Filters over ONE relation: a comma join of two derived tables under
		// a non-equality filter answers 0 rows on the DAG for every type, at
		// main too (review N1, filed with arc JP's FC-JP-3 family).
		{"eq-filter", false, func(p cw5Pair) string {
			return count("(SELECT ARRAY[CAST(id AS " + p.ta + ")] AS a, ARRAY[CAST(id AS " + p.tb +
				")] AS b FROM typemx WHERE id BETWEEN 1 AND 20) q WHERE a = b")
		}, "20", false},
		{"lt-filter", false, func(p cw5Pair) string {
			return count("(SELECT ARRAY[CAST(id AS " + p.ta + ")] AS a, ARRAY[CAST(id + 1 AS " + p.tb +
				")] AS b FROM typemx WHERE id BETWEEN 1 AND 20) q WHERE a < b")
		}, "20", false},
		{"hash-join", true, func(p cw5Pair) string {
			return count(side(p.ta, "a") + " x JOIN " + side(p.tb, "b") + " y ON x.a = y.b")
		}, "20", false},
		// COUNT(x.a), not COUNT(*): a bare COUNT(*) over an outer join of
		// array-valued derived tables refuses on the DAG for every element
		// type, at main too (`__rowcount_only__`, filed). The anti-join
		// spelling (`LEFT JOIN … WHERE y.b IS NULL` over two derived tables)
		// answers 0 rows on the DAG for integers too, at main (filed (w)):
		// single-process arms only.
		{"left-join-anti", true, func(p cw5Pair) string {
			return "SELECT COUNT(x.a) AS r FROM " + side(p.ta, "a") + " x LEFT JOIN (SELECT ARRAY[CAST(id AS " + p.tb +
				")] AS b FROM typemx WHERE id BETWEEN 1 AND 10) y ON x.a = y.b WHERE y.b IS NULL"
		}, "10", true},
		// The FULL join's UNMATCHED rows, not COUNT(*): a keyed-apart pair
		// answers 40 NULL-extended rows, and a bare COUNT(*) over a FULL join
		// of two array-valued derived tables refuses on the DAG for every
		// element type, at main too (`__rowcount_only__`, a filed defect).
		{"full-join", false, func(p cw5Pair) string {
			return "SELECT SUM(CASE WHEN x.a IS NULL OR y.b IS NULL THEN 1 ELSE 0 END) AS r FROM " +
				side(p.ta, "a") + " x FULL JOIN " + side(p.tb, "b") + " y ON x.a = y.b"
		}, "0", false},
		{"in-subquery", false, func(p cw5Pair) string {
			return count(side(p.ta, "a") + " x WHERE x.a IN (SELECT b FROM " + side(p.tb, "b") + " y)")
		}, "20", false},
		{"any-subquery", false, func(p cw5Pair) string {
			return count(side(p.ta, "a") + " x WHERE x.a = ANY (SELECT b FROM " + side(p.tb, "b") + " y)")
		}, "20", false},
		{"union", true, func(p cw5Pair) string {
			return count("(SELECT a FROM " + side(p.ta, "a") + " x UNION SELECT b FROM " + side(p.tb, "b") + " y) z")
		}, "20", false},
		{"intersect", true, func(p cw5Pair) string {
			return count("(SELECT a FROM " + side(p.ta, "a") + " x INTERSECT SELECT b FROM " + side(p.tb, "b") + " y) z")
		}, "20", false},
		{"except", true, func(p cw5Pair) string {
			return count("(SELECT a FROM " + side(p.ta, "a") + " x EXCEPT SELECT b FROM " + side(p.tb, "b") + " y) z")
		}, "0", false},
		{"distinct-over-union-all", false, func(p cw5Pair) string {
			return count("(SELECT DISTINCT v FROM (SELECT a AS v FROM " + side(p.ta, "a") + " x UNION ALL SELECT b FROM " +
				side(p.tb, "b") + " y) u) z")
		}, "20", false},
		{"group-by-over-union-all", false, func(p cw5Pair) string {
			return count("(SELECT v FROM (SELECT a AS v FROM " + side(p.ta, "a") + " x UNION ALL SELECT b FROM " +
				side(p.tb, "b") + " y) u GROUP BY v) z")
		}, "20", false},
		// The CHOICE constructs: the value a branch of the other element type
		// answers is the same number (an integer is never an unscaled DECIMAL
		// carrier, a DECIMAL is never rounded to the first branch's scale).
		{"case-value", false, func(p cw5Pair) string {
			return count("typemx WHERE id BETWEEN 1 AND 20 AND (CASE WHEN id % 2 = 0 THEN ARRAY[CAST(id AS " + p.ta +
				")] ELSE ARRAY[CAST(id AS " + p.tb + ")] END) = ARRAY[CAST(id AS " + p.tb + ")]")
		}, "20", false},
		{"coalesce-value", false, func(p cw5Pair) string {
			return count("typemx WHERE id BETWEEN 1 AND 20 AND COALESCE(CASE WHEN id % 2 = 0 THEN ARRAY[CAST(id AS " + p.ta +
				")] END, ARRAY[CAST(id AS " + p.tb + ")]) = ARRAY[CAST(id AS " + p.tb + ")]")
		}, "20", false},
		{"greatest-value", false, func(p cw5Pair) string {
			return count("typemx WHERE id BETWEEN 1 AND 20 AND GREATEST(ARRAY[CAST(id AS " + p.ta + ")], ARRAY[CAST(id - 1 AS " +
				p.tb + ")]) = ARRAY[CAST(id AS " + p.tb + ")]")
		}, "20", false},
		{"least-value", false, func(p cw5Pair) string {
			return count("typemx WHERE id BETWEEN 1 AND 20 AND LEAST(ARRAY[CAST(id AS " + p.ta + ")], ARRAY[CAST(id + 1 AS " +
				p.tb + ")]) = ARRAY[CAST(id AS " + p.tb + ")]")
		}, "20", false},
		{"array-constructor", false, func(p cw5Pair) string {
			return count("typemx WHERE id BETWEEN 1 AND 20 AND ARRAY[ARRAY[CAST(id AS " + p.ta + ")], ARRAY[CAST(id AS " +
				p.tb + ")]] = ARRAY[ARRAY[CAST(id AS " + p.tb + ")], ARRAY[CAST(id AS " + p.tb + ")]]")
		}, "20", false},
		{"nullif", false, func(p cw5Pair) string {
			return count("typemx WHERE id BETWEEN 1 AND 20 AND NULLIF(ARRAY[CAST(id AS " + p.ta + ")], ARRAY[CAST(id AS " +
				p.tb + ")]) IS NULL")
		}, "20", false},
	}
}

func TestArcCW5ElementTypesUnifyAtEveryMeetingPoint(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := cw4ArmsWithSortMerge(t, ctx)
	answered, want := 0, 0
	for _, arm := range arms {
		for _, p := range cw5Pairs() {
			for _, pt := range cw5Points() {
				if arm.sortMerge && !pt.join {
					continue
				}
				if pt.singleOnly && strings.HasPrefix(arm.name, "dag") {
					continue
				}
				sql := pt.sql(p)
				want++
				_, rows, err := arm.run(sql)
				if err != nil {
					t.Errorf("%s / %s / %s: %s refused: %v", arm.name, p.name, pt.name, sql, err)
					continue
				}
				if len(rows) != 1 || len(rows[0]) != 1 {
					t.Errorf("%s / %s / %s: %s answered %v", arm.name, p.name, pt.name, sql, rows)
					continue
				}
				answered++
				if got := fmt.Sprint(rows[0][0]); got != pt.want {
					t.Errorf("%s / %s / %s: %s\n  got %s, want %s (PostgreSQL 17.11)", arm.name, p.name, pt.name, sql, got, pt.want)
				}
			}
		}
	}
	t.Logf("element unification: %d of %d (cell, arm) answered", answered, want)
	if answered != want {
		t.Errorf("only %d of %d (cell, arm) pairs answered", answered, want)
	}
}

// cw5ArmElement is one element type a set-operation arm constructs.
type cw5ArmElement struct {
	name string
	// arm i's constructed value, over typemx id = 1 (arms 0 and 1 are equal,
	// every later arm differs from both)
	value func(i int) string
}

func cw5ArmElements() []cw5ArmElement {
	return []cw5ArmElement{
		{"int", func(i int) string { return fmt.Sprintf("ARRAY[CAST(%d AS INT)]", max(i, 1)) }},
		{"date", func(i int) string { return fmt.Sprintf("ARRAY[CAST('2024-01-%02d' AS DATE)]", max(i, 1)+9) }},
		{"text", func(i int) string { return fmt.Sprintf("ARRAY['v%d']", max(i, 1)) }},
		// Two DECIMAL scales: arm 1 is arm 0's value at another scale.
		{"decimal-scales", func(i int) string {
			switch i {
			case 0:
				return "ARRAY[CAST('1.01' AS DECIMAL(5,2))]"
			case 1:
				return "ARRAY[CAST('1.0100' AS DECIMAL(9,4))]"
			}
			return fmt.Sprintf("ARRAY[CAST('1.01%02d' AS DECIMAL(9,4))]", i)
		}},
		{"scalar-computed", func(i int) string { return fmt.Sprintf("id + %d", max(i, 1)-1) }},
	}
}

func TestArcCW5SetOperationArityDeclaresEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := cw4ArmsWithSortMerge(t, ctx)
	type opCase struct {
		name string
		ops  func(n int) []string // the n-1 operators joining n arms
		want func(n int) int      // PostgreSQL 17.11's row count (arms 0, 1 equal; the rest distinct)
	}
	ops := []opCase{
		{"union", func(n int) []string { return cw5Repeat("UNION", n-1) }, func(n int) int { return n - 1 }},
		{"union-all", func(n int) []string { return cw5Repeat("UNION ALL", n-1) }, func(n int) int { return n }},
		// A UNION of arms 0..n-2, then INTERSECT arm 1 (equal to arm 0): {arm 0}.
		{"intersect-last", func(n int) []string { return append(cw5Repeat("UNION", n-2), "INTERSECT") },
			func(n int) int { return 1 }},
		// A UNION of arms 0..n-2, then EXCEPT the last arm (distinct): n-2 rows.
		{"except-last", func(n int) []string { return append(cw5Repeat("UNION", n-2), "EXCEPT") },
			func(n int) int { return n - 2 }},
	}
	answered, want := 0, 0
	for _, arm := range arms {
		if arm.sortMerge {
			continue
		}
		for _, el := range cw5ArmElements() {
			for _, alias := range []bool{false, true} {
				for _, op := range ops {
					for n := 2; n <= 4; n++ {
						if op.name != "union" && op.name != "union-all" && n < 3 {
							continue
						}
						var b strings.Builder
						b.WriteString("SELECT COUNT(*) AS r FROM (")
						joins := op.ops(n)
						for i := 0; i < n; i++ {
							v := el.value(i)
							if op.name == "intersect-last" && i == n-1 {
								v = el.value(1)
							}
							if i > 0 {
								b.WriteString(" " + joins[i-1] + " ")
							}
							b.WriteString("SELECT " + v)
							if alias && i == 0 {
								b.WriteString(" AS v")
							}
							b.WriteString(" FROM typemx WHERE id = 1")
						}
						b.WriteString(") z")
						sql := b.String()
						want++
						_, rows, err := arm.run(sql)
						if err != nil {
							t.Errorf("%s / %s / alias=%v / %s / %d arms: %s refused: %v", arm.name, el.name, alias, op.name, n, sql, err)
							continue
						}
						answered++
						if got, exp := fmt.Sprint(rows[0][0]), fmt.Sprint(op.want(n)); got != exp {
							t.Errorf("%s / %s / alias=%v / %s / %d arms: %s\n  got %s, want %s (PostgreSQL 17.11)",
								arm.name, el.name, alias, op.name, n, sql, got, exp)
						}
					}
				}
			}
		}
	}
	t.Logf("set-operation arity: %d of %d (cell, arm) answered", answered, want)
	if answered != want {
		t.Errorf("only %d of %d (cell, arm) pairs answered", answered, want)
	}
}

func cw5Repeat(s string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = s
	}
	return out
}
