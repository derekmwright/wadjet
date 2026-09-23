// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestArcPCRound3AnswersAsPostgreSQLOnBothPaths is arc PC round 3's B7 and
// B1b gate on the single-process and the stage-DAG arms. Every want is
// PostgreSQL 17.11's, measured live; each cell failed at fc1f32f0.
//
//   - An ARRAY[…] constructor's elements have ONE type (PostgreSQL §10.5):
//     a set-returning item materialized every element through the FIRST
//     element's, so `unnest(ARRAY[1,2.5])` read 1, 2.
//   - A predicate above a derived table whose SELECT list expands a set was
//     pushed BELOW the expansion, where the set-returning output is still the
//     array: `(r.k).x = 3` over `_pg_expandarray` answered zero rows,
//     `r.u = 3` over `unnest` failed to plan (pgJDBC getPrimaryKeys filters
//     exactly so: `result.A_ATTNUM = (result.KEYS).x`).
//   - `x op ANY(typed array)` pairs x with the array's ELEMENT type by
//     PostgreSQL's rule: no operator between the classes is 42883 — it
//     answered false.
//   - An escape string (E'…') combines a UTF-16 surrogate pair into one code point and refuses a
//     malformed escape with PostgreSQL's SQLSTATE.
func TestArcPCRound3AnswersAsPostgreSQLOnBothPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := dajArms(t, ctx)
	for _, c := range []struct {
		name, sql string
		cols      []string
		want      string // dajDigest of PostgreSQL's rows
		state     string // PostgreSQL's SQLSTATE, when it refuses
		sentence  string // and its sentence
	}{
		// B7 (1): the constructor's common element type.
		{name: "unnest/mixed-numeric", sql: `SELECT unnest(ARRAY[1,2.5]) AS v`,
			cols: []string{"v"}, want: "2 rows: 1;2.5;"},
		{name: "unnest/fraction-first", sql: `SELECT unnest(ARRAY[2.5,1]) AS v`,
			cols: []string{"v"}, want: "2 rows: 2.5;1;"},
		{name: "unnest/null-between", sql: `SELECT unnest(ARRAY[1,NULL,2.5]) AS v`,
			cols: []string{"v"}, want: "3 rows: 1;;2.5;"},
		{name: "expandarray/x", sql: `SELECT (information_schema._pg_expandarray(ARRAY[1,2.5])).x AS v`,
			cols: []string{"v"}, want: "2 rows: 1;2.5;"},
		{name: "unnest/integers-stay-integers", sql: `SELECT unnest(ARRAY[1, 3000000000]) AS v`,
			cols: []string{"v"}, want: "2 rows: 1;3000000000;"},

		// B1b: a predicate over a set-returning output stays above the set.
		{name: "filter/field-of-expanded", sql: `SELECT id, (r.k).n AS n FROM (SELECT id, ` +
			`information_schema._pg_expandarray(ARRAY[1,2,3]) AS k FROM (VALUES (1),(2),(3)) v(id)) r ` +
			`WHERE r.id = (r.k).x ORDER BY id`,
			cols: []string{"id", "n"}, want: "3 rows: 1|1;2|2;3|3;"},
		{name: "filter/qualified-unnest-output", sql: `SELECT r.id, r.u FROM (SELECT id, ` +
			`unnest(ARRAY[1,2,3]) AS u FROM (VALUES (1),(2)) v(id)) r WHERE r.u >= r.id + 1 ORDER BY 1, 2`,
			cols: []string{"id", "u"}, want: "3 rows: 1|2;1|3;2|3;"},
		{name: "filter/counted", sql: `SELECT count(*) AS c FROM (SELECT unnest(ARRAY[1,2,3]) AS u) r WHERE r.u <> 2`,
			cols: []string{"c"}, want: "1 rows: 2;"},
		{name: "filter/passthrough-and-set", sql: `SELECT a, u FROM (SELECT 1 AS a, unnest(ARRAY[3,4]) AS u) r ` +
			`WHERE r.a = 1 AND u > 3`,
			cols: []string{"a", "u"}, want: "1 rows: 1|4;"},

		// B7 (2): the scalar against the ELEMENT type.
		{name: "any/text-bigint-array", sql: `SELECT 'x'::text = ANY(ARRAY[1,2]::bigint[]) AS v`,
			state: "42883", sentence: "operator does not exist: text = bigint"},
		{name: "all/text-bigint-array", sql: `SELECT 'x'::text <> ALL(ARRAY[1,2]::bigint[]) AS v`,
			state: "42883", sentence: "operator does not exist: text <> bigint"},
		{name: "any/bool-bigint-array", sql: `SELECT true = ANY(ARRAY[1,2]::bigint[]) AS v`,
			state: "42883", sentence: "operator does not exist: boolean = bigint"},
		{name: "any/integer-text-array", sql: `SELECT 1 = ANY(ARRAY['x','y']::text[]) AS v`,
			state: "42883", sentence: "operator does not exist: integer = text"},
		{name: "any/stored-text-array-against-bigint", sql: `SELECT count(*) AS c FROM typemx_nested WHERE id = ANY(c_arr)`,
			state: "42883", sentence: "operator does not exist: bigint = text"},
		{name: "any/control-same-class", sql: `SELECT 1 = ANY(ARRAY[1,2]::bigint[]) AS v`,
			cols: []string{"v"}, want: "1 rows: true;"},
		{name: "any/control-unknown-literal", sql: `SELECT 'x' = ANY(ARRAY['x','y']::text[]) AS v`,
			cols: []string{"v"}, want: "1 rows: true;"},

		// B7 (3): E'' surrogate pairs and malformed escapes.
		{name: "escape/surrogate-pair", sql: `SELECT E'\uD83D\uDE00' AS v`,
			cols: []string{"v"}, want: "1 rows: \U0001F600;"},
		{name: "escape/surrogate-pair-long-form", sql: `SELECT E'a\U0000D83D\U0000DE00b' AS v`,
			cols: []string{"v"}, want: "1 rows: a\U0001F600b;"},
		{name: "escape/lone-high-surrogate", sql: `SELECT E'\uD83Dx' AS v`,
			state: "42601", sentence: "invalid Unicode surrogate pair"},
		{name: "escape/lone-low-surrogate", sql: `SELECT E'\uDE00' AS v`,
			state: "42601", sentence: "invalid Unicode surrogate pair"},
		{name: "escape/short", sql: `SELECT E'\u12' AS v`,
			state: "22025", sentence: "invalid Unicode escape"},
		{name: "escape/out-of-range", sql: `SELECT E'\U00110000' AS v`,
			state: "42601", sentence: "invalid Unicode escape value"},
		{name: "escape/invalid-byte", sql: `SELECT E'\xD8' AS v`,
			state: "22021", sentence: `invalid byte sequence for encoding "UTF8": 0xd8`},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(c.sql)
				if c.state != "" {
					if err == nil {
						t.Errorf("%s arm answered %s; PostgreSQL raises %s %q\n  SQL: %s",
							arm.name, dajDigest(res, res.Columns), c.state, c.sentence, c.sql)
						continue
					}
					if got := sqlerr.StateOf(err); got != c.state || sqlerr.SentenceOf(err) != c.sentence {
						t.Errorf("%s arm raised %s %q; PostgreSQL raises %s %q\n  SQL: %s",
							arm.name, got, sqlerr.SentenceOf(err), c.state, c.sentence, c.sql)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s arm refused: %v\n  SQL: %s", arm.name, err, c.sql)
					continue
				}
				if got := dajDigest(res, c.cols); got != c.want {
					t.Errorf("%s arm answered %s\n  PostgreSQL 17.11: %s\n  SQL: %s", arm.name, got, c.want, c.sql)
				}
			}
		})
	}
}
