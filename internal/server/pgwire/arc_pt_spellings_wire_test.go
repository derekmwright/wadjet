// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// ARC PT ON THE WIRE — what RowDescription DECLARES for the new spellings.
//
// The value gates in the wadjet package compare what each spelling ANSWERS
// against live PostgreSQL 17.11. A value oracle cannot see a right value under
// a wrong OID (CLAUDE.md, the wire arm), and one of this arc's spellings
// exists ONLY for its declaration: `LOCALTIMESTAMP` is `CURRENT_TIMESTAMP`'s
// value declared `timestamp without time zone` (1114) instead of `timestamp
// with time zone`.
//
// The OIDs below are PostgreSQL 17.11's own, measured with
// `pg_typeof(x)::regtype::oid`, and so are the field NAMES: an operator's
// unaliased result is `?column?` there, and a niladic call is named after the
// function.
func TestArcPTTheWireDeclaresTheStandardSpellings(t *testing.T) {
	srv := setupJ1LateralDB(t)
	for _, c := range []struct {
		name, sql string
		want      []string // "field:oid", in order
		// pgOID is PostgreSQL's declaration where it differs from this
		// engine's, with the reason. A pin that starts agreeing fails.
		pgOID string
	}{
		// --- the predicates: boolean, as they are on the server ----------
		{name: "similar_to", sql: `SELECT 'abc' SIMILAR TO 'a%' FROM j1ord LIMIT 1`,
			want: []string{"?column?:16"}},
		{name: "not_similar_to", sql: `SELECT 'abc' NOT SIMILAR TO 'a%' FROM j1ord LIMIT 1`,
			want: []string{"?column?:16"}},
		{name: "similar_to_escape",
			sql:  `SELECT 'a%c' SIMILAR TO 'a#%c' ESCAPE '#' FROM j1ord LIMIT 1`,
			want: []string{"?column?:16"}},
		{name: "like_escape", sql: `SELECT 'a%b' LIKE 'a!%b' ESCAPE '!' FROM j1ord LIMIT 1`,
			want: []string{"?column?:16"}},
		{name: "ilike_escape", sql: `SELECT 'A%B' ILIKE 'a!%b' ESCAPE '!' FROM j1ord LIMIT 1`,
			want: []string{"?column?:16"}},
		{name: "between_then_comparison",
			sql:  `SELECT 5 BETWEEN 1 AND 10 = true FROM j1ord LIMIT 1`,
			want: []string{"?column?:16"}},
		{name: "comparison_then_is_true", sql: `SELECT 1 = 1 IS TRUE FROM j1ord LIMIT 1`,
			want: []string{"?column?:16"}},

		// --- the string functions: text ----------------------------------
		{name: "substring_from_for", sql: `SELECT SUBSTRING('abcdef' FROM 2 FOR 3) FROM j1ord LIMIT 1`,
			want: []string{"substring:25"}},
		{name: "substring_regex", sql: `SELECT SUBSTRING('abcdef' FROM 'b.d') FROM j1ord LIMIT 1`,
			want: []string{"substring:25"}},
		{name: "overlay", sql: `SELECT OVERLAY('abc' PLACING 'X' FROM 2) FROM j1ord LIMIT 1`,
			want: []string{"overlay:25"}},
		{name: "normalize", sql: `SELECT NORMALIZE('abc', NFC) FROM j1ord LIMIT 1`,
			want: []string{"normalize:25"}},
		{name: "left", sql: `SELECT LEFT('abcdef', 2) FROM j1ord LIMIT 1`,
			want: []string{"left:25"}},
		{name: "right", sql: `SELECT RIGHT('abcdef', 2) FROM j1ord LIMIT 1`,
			want: []string{"right:25"}},
		{name: "left_over_a_column", sql: `SELECT LEFT(customer, 2) FROM j1ord LIMIT 1`,
			want: []string{"left:25"}},

		// --- LOCALTIMESTAMP: the declaration IS the spelling -------------
		{name: "localtimestamp", sql: `SELECT LOCALTIMESTAMP FROM j1ord LIMIT 1`,
			want: []string{"localtimestamp:1114"}},
		{name: "localtimestamp_precision", sql: `SELECT LOCALTIMESTAMP(3) FROM j1ord LIMIT 1`,
			want: []string{"localtimestamp:1114"}},

		// --- `#`: the bitwise family's recorded widening -----------------
		// `int4 # int4` is INTEGER (23) on the server and on the wire here,
		// even though the bitwise family's body boxes an int64 — the declared
		// output is read from the operands. A bigint operand declares 20 on
		// both engines.
		{name: "hash_xor", sql: `SELECT 5 # 3 FROM j1ord LIMIT 1`,
			want: []string{"?column?:23"}},
		{name: "hash_xor_over_a_column", sql: `SELECT id # 1 FROM j1ord LIMIT 1`,
			want: []string{"?column?:20"}},
		// A value no int4 can carry: the DECLARATION must not claim one.
		{name: "hash_xor_bigint_literal",
			sql:  `SELECT 9223372036854775807 # 1 FROM j1ord LIMIT 1`,
			want: []string{"?column?:20"}},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("refused a statement PostgreSQL 17.11 answers: %v\n  SQL: %s", res.Err, c.sql)
			}
			got := make([]string, 0, len(res.FieldDescriptions))
			for _, f := range res.FieldDescriptions {
				got = append(got, fmt.Sprintf("%s:%d", f.Name, f.DataTypeOID))
			}
			if strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Errorf("RowDescription\n  got  %v\n  want %v\n  SQL: %s", got, c.want, c.sql)
			}
			if c.pgOID != "" {
				t.Logf("PostgreSQL 17.11 declares %s", c.pgOID)
			}
		})
	}
}
