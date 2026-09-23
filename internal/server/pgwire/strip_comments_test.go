// SPDX-License-Identifier: MIT

package pgwire

import (
	"strings"
	"testing"
)

// Comments are not part of the statement this layer reasons about.
func TestStripSQLComments(t *testing.T) {
	sql := "/* with T as (\n  select T.oid from pg_catalog.pg_class T\n) */\nselect ind_head.indexrelid from pg_catalog.pg_index ind_head"
	got := stripSQLComments(sql)
	if strings.Contains(strings.ToUpper(got), "PG_CLASS") {
		t.Fatalf("commented-out CTE survived: %q", got)
	}
	// A comment marker inside a literal is data.
	lit := stripSQLComments("select * from t where c = '-- not a comment'")
	if !strings.Contains(lit, "-- not a comment") {
		t.Fatalf("literal was stripped: %q", lit)
	}
	// Line comments end at the newline, not at the end of the statement.
	line := stripSQLComments("select a, -- why\nb from t")
	if !strings.Contains(line, "b from t") {
		t.Fatalf("line comment ate the rest: %q", line)
	}
}

// PostgreSQL's CommandComplete tag carries the row count alone; only INSERT
// prefixes an OID. Every command went out in the INSERT form, so psql answered
// a DELETE with "could not interpret result from server: DELETE 0 0".
func TestCommandTag(t *testing.T) {
	for _, tt := range []struct{ cmd, want string }{
		{"DELETE", "DELETE 3"},
		{"UPDATE", "UPDATE 3"},
		{"INSERT", "INSERT 0 3"},
		{"delete", "DELETE 3"},
		{"", "SELECT 3"},
	} {
		if got := commandTag(tt.cmd, 3); got != tt.want {
			t.Errorf("commandTag(%q, 3) = %q, want %q", tt.cmd, got, tt.want)
		}
	}
}
