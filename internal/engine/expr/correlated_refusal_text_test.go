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
// AND IT STATES THE REASON THE SITE THAT RAISED IT FOUND (#1072). One message
// for two post-conditions sent the reader to the wrong mechanism: a query
// refused because its body holds a SET OPERATION one level down was told a
// substituted term rendered as a bare numeric literal in a GROUP BY.
func TestTheUnsubstitutedRefusalNamesTheClausesTheRebuildWrites(t *testing.T) {
	cases := []struct {
		name, reason, want string
	}{
		{
			name:   "clause-post-condition",
			reason: "the substituted term still renders as a bare numeric literal, which a GROUP BY or an ORDER BY reads as a select-list POSITION",
			want:   "bare numeric literal",
		},
		{
			name:   "nested-set-operation",
			reason: "its body holds a SET OPERATION one level down, which the rebuild renders no arm for, so a reference written in an ARM is re-emitted as written",
			want:   "SET OPERATION one level down",
		},
		{
			name:   "no reason given",
			reason: "",
			want:   "survives in the rebuilt statement",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := &UnsubstitutedOuterRefError{
				Kind:   "scalar",
				SQL:    "SELECT x.visits FROM c2users x ORDER BY u.id LIMIT 1",
				Refs:   []plansql.OuterRef{{Table: "u", Column: "id"}},
				Reason: tc.reason,
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
			if !strings.Contains(msg, tc.want) {
				t.Errorf("the refusal does not state what it refuses (%q):\n  %s", tc.want, msg)
			}
			if got := err.SQLState(); got != "0A000" {
				t.Errorf("SQLState = %q, want 0A000 (feature_not_supported)", got)
			}
		})
	}
}
