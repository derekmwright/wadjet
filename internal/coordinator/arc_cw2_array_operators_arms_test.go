// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Arc CW round 2, P3: the comparison OPERATORS over two arrays compare
// element-wise — #1021's second spelling. Round 1 moved ORDER BY, MIN, MAX and
// DISTINCT onto the element-wise kernel; `<`, `>`, `=` still compared the two
// boxes' Go text, so `ARRAY[10] > ARRAY[9]` was false, a prefix never ordered
// before its extension, and a `WHERE a < ARRAY[…]` filter kept the wrong rows.
// The operator now hands the pair to kernel.CompareValuesAt, the sort's own.
//
// The constant cells are PostgreSQL 17.11's answers; the column cells are
// identities with no container on the right-hand side (a one-element array
// orders as its element; `ARRAY[k, x] > ARRAY[k, 9]` is `x > 9`).
// At the round-1 tip 8b50409c the prefix, empty/NULL-element and numeric
// cells answered f and the two numeric filters kept the wrong count, on every
// arm; the nested, bool, timestamp and text cells are controls whose text
// order happens to agree.
func TestArcCW2ArrayComparisonOperatorsAreElementWiseOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	constants := []struct{ name, sql, want string }{
		{"prefix", `SELECT ARRAY[1,2] < ARRAY[1,2,0] AS a, ARRAY[1] < ARRAY[1,0] AS b, ARRAY[1,2,0] > ARRAY[1,2] AS c, ARRAY[1,2] <= ARRAY[1,2,0] AS d`,
			"[[true true true true]]"},
		{"empty-text-null", `SELECT CAST(ARRAY[] AS INT[]) < ARRAY[1] AS a, ARRAY['a'] < ARRAY['a','b'] AS b, ARRAY[1,NULL] < ARRAY[1,NULL,3] AS c`,
			"[[true true true]]"},
		{"numeric-not-text", `SELECT ARRAY[10] > ARRAY[9] AS a, ARRAY[1,NULL] = ARRAY[1,NULL] AS b, ARRAY[1,NULL] < ARRAY[1,2] AS c, ARRAY[2] <> ARRAY[2] AS d`,
			"[[true true false false]]"},
		{"nested-bool", `SELECT ARRAY[ARRAY[1,2]] < ARRAY[ARRAY[1,3]] AS a, ARRAY[true] > ARRAY[false] AS b`, "[[true true]]"},
	}
	pairs := []struct{ name, got, want string }{
		{"filter-lt", `SELECT COUNT(*) AS n FROM typemx WHERE c_i64 IS NOT NULL AND ARRAY[c_i64] < ARRAY[CAST(10 AS BIGINT)]`,
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i64 < 10`},
		{"filter-two-elements", `SELECT COUNT(*) AS n FROM typemx WHERE c_i32 IS NOT NULL AND c_i64 IS NOT NULL AND ARRAY[CAST(c_i32 AS BIGINT), c_i64] > ARRAY[CAST(c_i32 AS BIGINT), CAST(9 AS BIGINT)]`,
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 IS NOT NULL AND c_i64 > 9`},
		{"filter-timestamp", `SELECT id FROM typemx WHERE c_ts IS NOT NULL AND ARRAY[c_ts] >= ARRAY[CAST('2024-06-01 00:00:00' AS TIMESTAMP)] ORDER BY id LIMIT 30`,
			`SELECT id FROM typemx WHERE c_ts >= CAST('2024-06-01 00:00:00' AS TIMESTAMP) ORDER BY id LIMIT 30`},
		{"case-text", `SELECT id, CASE WHEN ARRAY[c_str] > ARRAY['s-000100'] THEN 1 ELSE 0 END AS c FROM typemx WHERE c_str IS NOT NULL ORDER BY id LIMIT 30`,
			`SELECT id, CASE WHEN c_str > 's-000100' THEN 1 ELSE 0 END AS c FROM typemx WHERE c_str IS NOT NULL ORDER BY id LIMIT 30`},
	}
	answered := 0
	for _, arm := range arms {
		for _, c := range constants {
			_, rows, err := arm.run(c.sql)
			if err != nil {
				t.Errorf("%s / %s: %s refused: %v", c.name, arm.name, c.sql, err)
				continue
			}
			answered++
			if got := fmt.Sprint(rows); got != c.want {
				t.Errorf("%s / %s: %s\n  got  %s\n  want %s", c.name, arm.name, c.sql, got, c.want)
			}
		}
		for _, p := range pairs {
			_, got, err := arm.run(p.got)
			if err != nil {
				t.Errorf("%s / %s: %s refused: %v", p.name, arm.name, p.got, err)
				continue
			}
			_, want, err := arm.run(p.want)
			if err != nil {
				t.Errorf("%s / %s: the oracle refused: %v", p.name, arm.name, err)
				continue
			}
			answered++
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("%s / %s:\n  %s\n    = %v\n  %s\n    = %v", p.name, arm.name, p.got, got, p.want, want)
			}
		}
	}
	if want := (len(constants) + len(pairs)) * len(arms); answered != want {
		t.Errorf("%d of %d (cell, arm) pairs answered", answered, want)
	}
}
