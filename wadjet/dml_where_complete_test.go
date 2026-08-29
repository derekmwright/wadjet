package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A DML WHERE clause must parse IN FULL, or the statement is refused.
//
// This is the #686 class reached by a second route, and it was found by the
// review of the alias fix. `plansql.ParseExpression` stops at the first token
// the expression grammar cannot use and reports SUCCESS, so a WHERE clause
// became its own PREFIX — and the tail it dropped was a conjunct that would
// have NARROWED the statement. The result is a DELETE that removes every row
// the surviving prefix matches:
//
//	DELETE FROM pr WHERE id > 0 AND name @@ 'zzz'      -> DELETE 3, table EMPTIED
//	DELETE FROM pr WHERE id > 0 AND n # 3              -> DELETE 3, table EMPTIED
//	DELETE FROM pr WHERE name <> 'zzz' AND id ISNULL   -> DELETE 3, table EMPTIED
//	DELETE FROM pr WHERE (id = 1) garbage AND name = 'zzz' -> DELETE 1
//
// PostgreSQL 17.11 deletes NO rows for any of them: it answers the first and
// third with DELETE 0 (it implements `@@` over text and the `ISNULL` suffix,
// and neither matches), the second with 42804 and the fourth with 42601. So
// the ROW SET agreed with PostgreSQL only after this fix; before it, three of
// these four emptied a table PostgreSQL left untouched.
//
// Where wadjet refuses syntax PostgreSQL implements, the refusal is the
// honest answer for a clause this server cannot read (ADR-0019, protocol
// item 8) — and it is the same row set either way.
func TestDMLWhereMustParseInFull(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"unsupported text-search operator", `DELETE FROM pr WHERE id > 0 AND name @@ 'zzz'`},
		{"unsupported arithmetic operator", `DELETE FROM pr WHERE id > 0 AND n # 3`},
		{"PostgreSQL ISNULL suffix", `DELETE FROM pr WHERE name <> 'zzz' AND id ISNULL`},
		{"stray token after a parenthesised term", `DELETE FROM pr WHERE (id = 1) garbage AND name = 'zzz'`},
		{"stray token after the whole predicate", `DELETE FROM pr WHERE id = 1 garbage`},
		{"regex match operator", `DELETE FROM pr WHERE name ~ 'zzz' AND id = 1`},
		{"COLLATE", `DELETE FROM pr WHERE id > 0 AND name = 'zzz' COLLATE "C"`},
		{"SIMILAR TO ... ESCAPE", `DELETE FROM pr WHERE id > 0 AND name SIMILAR TO 'zzz' ESCAPE '\'`},
		{"LIMIT on a DELETE", `DELETE FROM pr WHERE id = 1 LIMIT 1`},
		// A second statement after the first is not part of the first
		// statement's WHERE. It used to be swallowed by the clause text and
		// then silently truncated away, so the DELETE ran and the second
		// statement vanished without a word.
		{"a second statement", `DELETE FROM pr WHERE id = 1; DELETE FROM pr WHERE id = 2`},
		{"UPDATE, stray token in the WHERE", `UPDATE pr SET n = 1 WHERE id = 1 garbage AND name = 'zzz'`},
		{"UPDATE, stray token in a SET value", `UPDATE pr SET n = 1 garbage`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := aliasDB686(t)
			before := aliasRows686(t, db)
			res, err := db.Execute(context.Background(), tc.sql)
			if err == nil {
				t.Fatalf("%s answered %s %d; a clause this server cannot read in full must be refused. "+
					"pr is now %v", tc.sql, res.Command, res.RowsAffected, aliasRows686(t, db))
			}
			if got := sqlerr.StateOf(err); got != "42601" {
				t.Errorf("%s: SQLSTATE %q, want 42601 (err: %v)", tc.sql, got, err)
			}
			if after := aliasRows686(t, db); strings.Join(after, " ") != strings.Join(before, " ") {
				t.Errorf("the refused statement changed pr: %v -> %v", before, after)
			}
		})
	}
}

// The clauses this server DOES read in full still run, so the completeness
// requirement did not turn ordinary DML into refusals.
func TestOrdinaryDMLWhereStillRuns(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		tag  string
		rows []string
	}{
		{"DELETE FROM pr WHERE id = 1", "DELETE 1", []string{"2:20:b", "3:30:c"}},
		{"DELETE FROM pr WHERE (id = 1)", "DELETE 1", []string{"2:20:b", "3:30:c"}},
		{"DELETE FROM pr WHERE ((id = 1) AND (n = 10))", "DELETE 1", []string{"2:20:b", "3:30:c"}},
		{"DELETE FROM pr WHERE id = 1 OR id = 2", "DELETE 2", []string{"3:30:c"}},
		{"DELETE FROM pr WHERE name IN ('a', 'b')", "DELETE 2", []string{"3:30:c"}},
		{"DELETE FROM pr WHERE id BETWEEN 1 AND 2", "DELETE 2", []string{"3:30:c"}},
		{"DELETE FROM pr WHERE name LIKE 'a%'", "DELETE 1", []string{"2:20:b", "3:30:c"}},
		{"DELETE FROM pr WHERE UPPER(name) = 'A'", "DELETE 1", []string{"2:20:b", "3:30:c"}},
		{"DELETE FROM pr WHERE id IS NOT NULL AND n > 25", "DELETE 1", []string{"1:10:a", "2:20:b"}},
		{"DELETE FROM pr WHERE CASE WHEN id = 1 THEN true ELSE false END", "DELETE 1", []string{"2:20:b", "3:30:c"}},
		{"DELETE FROM pr AS a WHERE a.id = 1 AND a.n = 10", "DELETE 1", []string{"2:20:b", "3:30:c"}},
		{"UPDATE pr SET n = n + 1 WHERE id = 1", "UPDATE 1", []string{"1:11:a", "2:20:b", "3:30:c"}},
		{"UPDATE pr SET n = -5 WHERE id = 1", "UPDATE 1", []string{"1:-5:a", "2:20:b", "3:30:c"}},
		{"UPDATE pr SET name = UPPER(name) WHERE id = 1", "UPDATE 1", []string{"1:10:A", "2:20:b", "3:30:c"}},
		{"UPDATE pr AS a SET n = a.n * 2 WHERE a.id = 1", "UPDATE 1", []string{"1:20:a", "2:20:b", "3:30:c"}},
		// The false-positive risk of requiring a COMPLETE parse: text that is
		// lexically whitespace, and the trailing semicolon every client sends.
		// The lexer skips comments as whitespace, so the completeness check
		// never sees them; a refusal here would break ordinary psql traffic.
		{"DELETE FROM pr WHERE id = 1;", "DELETE 1", []string{"2:20:b", "3:30:c"}},
		{"DELETE FROM pr WHERE id = 1 ;", "DELETE 1", []string{"2:20:b", "3:30:c"}},
		{"DELETE FROM pr AS a WHERE a.id = 1;", "DELETE 1", []string{"2:20:b", "3:30:c"}},
		{"DELETE FROM pr WHERE id = 1 -- trailing comment", "DELETE 1", []string{"2:20:b", "3:30:c"}},
		{"DELETE FROM pr WHERE id = 1 /* trailing comment */", "DELETE 1", []string{"2:20:b", "3:30:c"}},
		{"DELETE FROM pr WHERE /* inline */ id = 1", "DELETE 1", []string{"2:20:b", "3:30:c"}},
		{"UPDATE pr SET n = 9 WHERE id = 1;", "UPDATE 1", []string{"1:9:a", "2:20:b", "3:30:c"}},
		{"UPDATE pr SET n = 9 -- note", "UPDATE 3", []string{"1:9:a", "2:9:b", "3:9:c"}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			db := aliasDB686(t)
			res, err := db.Execute(context.Background(), tc.sql)
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			if got := fmt.Sprintf("%s %d", res.Command, res.RowsAffected); got != tc.tag {
				t.Errorf("%s: command tag %q, want %q", tc.sql, got, tc.tag)
			}
			if got := aliasRows686(t, db); strings.Join(got, " ") != strings.Join(tc.rows, " ") {
				t.Errorf("%s left pr as %v, want %v", tc.sql, got, tc.rows)
			}
		})
	}
}
