package expr

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// THE SENTENCE A CLIENT GETS NAMES THE CLAUSES THE REBUILD ACTUALLY WRITES.
//
// `UnsubstitutedOuterRefError` is the refusal a correlated subquery gets when
// a term the re-run substitutes still renders as a bare numeric literal, which
// a GROUP BY or an ORDER BY reads as a select-list POSITION. Its text enumerated
// THREE clauses — the SELECT list, the WHERE and the HAVING — for two rounds
// after the rebuild grew to six, and that text reaches the client verbatim
// (round-4 review, B1). The enumeration is asserted here rather than through a
// query because plansql.ClauseTermText leaves the refusal with no members: it
// is the post-condition of the rendering, not a shape a user can write.
func TestTheUnsubstitutedRefusalNamesTheClausesTheRebuildWrites(t *testing.T) {
	err := &UnsubstitutedOuterRefError{
		Kind: "scalar",
		SQL:  "SELECT x.visits FROM c2users x ORDER BY u.id LIMIT 1",
		Refs: []plansql.OuterRef{{Table: "u", Column: "id"}},
	}
	msg := err.Error()
	for _, clause := range []string{
		"SELECT list", "WHERE", "HAVING", "GROUP BY", "ORDER BY", "ON condition",
	} {
		if !strings.Contains(msg, clause) {
			t.Errorf("the refusal does not name the %s the rebuild writes:\n  %s", clause, msg)
		}
	}
	if !strings.Contains(msg, "u.id") {
		t.Errorf("the refusal does not name the reference:\n  %s", msg)
	}
	if !strings.Contains(msg, "bare numeric literal") {
		t.Errorf("the refusal does not state what it refuses:\n  %s", msg)
	}
	if got := err.SQLState(); got != "0A000" {
		t.Errorf("SQLState = %q, want 0A000 (feature_not_supported)", got)
	}
}
