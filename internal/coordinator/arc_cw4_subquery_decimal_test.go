// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestArcCW4SubqueryOperandCarriesItsDeclaredElement is round 4's B1: a
// subquery whose VALUE is the container — a scalar subquery, ARRAY(subquery),
// a correlated scalar subquery — reached a CAST with no declared element
// (physical.declaredShapeOf built its declaration from the subquery's TypeID,
// precision and scale only), so the renderer read the box: a TIMESTAMP element
// printed its epoch milliseconds, `AS VECTOR(1)` converted them to a float, and
// CREATE TABLE AS stored the number. Every cell is pinned to PostgreSQL 17.11's
// own text (to_json for JSON; pgvector's 42846 for a non-numeric element), on
// the six arms; each producer reads a table so no constant folding answers in
// the cast's place.
func TestArcCW4SubqueryOperandCarriesItsDeclaredElement(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := cw4ArmsWithSortMerge(t, ctx)

	type element struct {
		name, e             string
		text, json, textArr string // PostgreSQL 17.11
		vector              string // "" = pgvector's 42846 refusal
	}
	elements := []element{
		{"date", "CAST('2024-01-10' AS DATE)", "{2024-01-10}", `["2024-01-10"]`, "{2024-01-10}", ""},
		{"timestamp", "CAST('2024-01-01 09:00:00' AS TIMESTAMP)", `{"2024-01-01 09:00:00"}`, `["2024-01-01T09:00:00"]`, `{"2024-01-01 09:00:00"}`, ""},
		{"decimal", "CAST(10.5 AS DECIMAL(5,2))", "{10.50}", "[10.50]", "{10.50}", "[10.5]"},
		{"bool", "true", "{t}", "[true]", "{true}", ""},
		// PostgreSQL's inet[]::text[] is `{10.0.0.10/32}`; an IPv4 element is
		// this engine's own address type, whose text is the bare address — the
		// recorded network-type divergence (postgres-differences), the same for
		// the scalar cast.
		{"ipv4", "CAST('10.0.0.10' AS IPV4)", "{10.0.0.10}", `["10.0.0.10"]`, "{10.0.0.10}", ""},
		// A multi-dimensional value: its leaves cast, its dimensions kept (B2).
		{"nested", "ARRAY[1,2]", "{{1,2}}", "[[1,2]]", "{{1,2}}", ""},
	}
	producers := []struct{ name, sql, from string }{
		{"scalar-subquery", "(SELECT ARRAY[{e}] FROM typemx WHERE id = 1)", ""},
		{"array-subquery", "ARRAY(SELECT {e} FROM typemx WHERE id = 1)", ""},
		{"correlated", "(SELECT ARRAY[{e}] FROM typemx i WHERE i.id = o.id)", " FROM typemx o WHERE o.id = 1"},
		{"coalesce-of-subquery", "COALESCE((SELECT ARRAY[{e}] FROM typemx WHERE id = 1), ARRAY[{e}])", ""},
	}
	answered, want := 0, 0
	for _, arm := range arms {
		if arm.sortMerge {
			continue // no join here
		}
		for _, el := range elements {
			for _, pr := range producers {
				operand := strings.ReplaceAll(pr.sql, "{e}", el.e)
				for _, target := range []struct{ spell, want string }{
					{"TEXT", el.text}, {"JSON", el.json}, {"TEXT[]", el.textArr}, {"VARCHAR(40)", el.text}, {"VECTOR(1)", el.vector},
				} {
					sql := "SELECT CAST(" + operand + " AS " + target.spell + ") AS r" + pr.from
					want++
					_, rows, err := arm.run(sql)
					if target.want == "" {
						if err == nil || !strings.Contains(err.Error(), "cannot cast type") {
							t.Errorf("%s / %s / %s / %s: %s\n  want pgvector's 42846 refusal, got %v %v",
								arm.name, el.name, pr.name, target.spell, sql, rows, err)
							continue
						}
						answered++
						continue
					}
					if err != nil {
						t.Errorf("%s / %s / %s / %s: %s refused: %v", arm.name, el.name, pr.name, target.spell, sql, err)
						continue
					}
					if len(rows) != 1 || len(rows[0]) != 1 {
						t.Errorf("%s / %s / %s / %s: %s answered %v", arm.name, el.name, pr.name, target.spell, sql, rows)
						continue
					}
					answered++
					if got := cw4Text(rows[0][0]); got != target.want {
						t.Errorf("%s / %s / %s / %s: %s\n  got %s, want %s (PostgreSQL 17.11)",
							arm.name, el.name, pr.name, target.spell, sql, got, target.want)
					}
				}
			}
		}
	}
	if answered != want {
		t.Errorf("%d of %d (cell, arm) answered", answered, want)
	}
}

// cw4Text is a cell's text as the client reads it: a TEXT[] cell is its
// PostgreSQL array text.
func cw4Text(v any) string {
	if elems, ok := v.([]any); ok {
		parts := make([]string, len(elems))
		for i, e := range elems {
			switch x := e.(type) {
			case nil:
				parts[i] = "NULL"
			case []any:
				parts[i] = cw4Text(x)
			default:
				s := fmt.Sprint(x)
				if strings.ContainsAny(s, " ,{}\"\\") || s == "" {
					s = `"` + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`) + `"`
				}
				parts[i] = s
			}
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	return fmt.Sprint(v)
}

// TestArcCW4DecimalElementsKeyByValueOnEveryKeyPath is round 4's B3: arrays
// whose DECIMAL elements differ only in scale are one value to every key path.
// `CAST(v AS DECIMAL(9,4)[])` declared text[] — its elements were hashed and
// equated as their TEXT, `10.0000` ≠ `10.00` — so a hash join over the two
// scales answered 2 pairs where PostgreSQL answers 16 (the review's m10), the
// IN / `= ANY` membership and the set operations likewise; the DAG refused the
// set operations outright (0A000). X holds ten DECIMAL(5,2) arrays with a
// duplicate, a NULL element, an empty array and a NULL; Y is X cast to
// DECIMAL(9,4)[]. Every count is PostgreSQL 17.11's, on six arms (the two
// sort-merge arms run every join and set operation too).
func TestArcCW4DecimalElementsKeyByValueOnEveryKeyPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := cw4ArmsWithSortMerge(t, ctx)

	d := func(v string) string { return "CAST(" + v + " AS DECIMAL(5,2))" }
	x := "(SELECT 1 AS k, ARRAY[" + d("10") + "] AS v UNION ALL SELECT 2, ARRAY[" + d("9.5") + "] " +
		"UNION ALL SELECT 3, ARRAY[" + d("9.5") + "] UNION ALL SELECT 4, ARRAY[" + d("1") + ", " + d("2") + "] " +
		"UNION ALL SELECT 5, ARRAY[" + d("1") + ", " + d("NULL") + "] UNION ALL SELECT 6, CAST(ARRAY[] AS DECIMAL(5,2)[]) " +
		"UNION ALL SELECT 7, CAST(NULL AS DECIMAL(5,2)[]) UNION ALL SELECT 8, ARRAY[" + d("100.01") + "] " +
		"UNION ALL SELECT 9, ARRAY[" + d("10") + "] UNION ALL SELECT 10, ARRAY[" + d("NULL") + "])"
	y := "(SELECT k, CAST(v AS DECIMAL(9,4)[]) AS w FROM " + x + " x0)"
	cells := []struct {
		name, sql string
		want      int64 // PostgreSQL 17.11
	}{
		{"hash-join", "SELECT COUNT(*) FROM " + x + " x JOIN " + y + " y ON x.v = y.w", 13},
		{"in-subquery", "SELECT COUNT(*) FROM " + x + " x WHERE x.v IN (SELECT w FROM " + y + " y WHERE y.k <= 5)", 6},
		{"any-subquery", "SELECT COUNT(*) FROM " + x + " x WHERE x.v = ANY (SELECT w FROM " + y + " y WHERE y.k <= 5)", 6},
		{"all-subquery", "SELECT COUNT(*) FROM " + x + " x WHERE x.v <> ALL (SELECT w FROM " + y + " y WHERE y.k <= 5 AND y.w IS NOT NULL)", 3},
		{"union", "SELECT COUNT(*) FROM (SELECT v FROM " + x + " x UNION SELECT w FROM " + y + " y) z", 8},
		{"intersect", "SELECT COUNT(*) FROM (SELECT v FROM " + x + " x INTERSECT SELECT w FROM " + y + " y WHERE y.k <= 4) z", 3},
		{"except", "SELECT COUNT(*) FROM (SELECT v FROM " + x + " x EXCEPT SELECT w FROM " + y + " y WHERE y.k <= 4) z", 5},
		{"distinct", "SELECT COUNT(*) FROM (SELECT DISTINCT v FROM (SELECT v FROM " + x + " x UNION ALL SELECT w FROM " + y + " y) u) z", 8},
		{"group-by", "SELECT COUNT(*) FROM (SELECT v, COUNT(*) AS c FROM (SELECT v FROM " + x + " x UNION ALL SELECT w FROM " + y + " y) u GROUP BY v) z", 8},
		{"window-partition", "SELECT MAX(c) FROM (SELECT COUNT(*) OVER (PARTITION BY v) AS c FROM (SELECT v FROM " + x + " x UNION ALL SELECT w FROM " + y + " y) u) z", 4},
		{"join-filter", "SELECT COUNT(*) FROM " + x + " x, " + y + " y WHERE x.v = y.w AND x.k <> y.k", 4},
		{"lt", "SELECT COUNT(*) FROM " + x + " x, " + y + " y WHERE x.v < y.w", 34},
	}
	for _, arm := range arms {
		for _, c := range cells {
			_, rows, err := arm.run(c.sql)
			if err != nil {
				t.Errorf("%s / %s: refused: %v", arm.name, c.name, err)
				continue
			}
			if len(rows) != 1 || len(rows[0]) != 1 {
				t.Errorf("%s / %s: answered %v", arm.name, c.name, rows)
				continue
			}
			if got := fmt.Sprint(rows[0][0]); got != fmt.Sprint(c.want) {
				t.Errorf("%s / %s: got %s, want %d (PostgreSQL 17.11)", arm.name, c.name, got, c.want)
			}
		}
	}
}
