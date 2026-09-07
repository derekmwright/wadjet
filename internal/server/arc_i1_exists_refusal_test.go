package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
)

// AN EXISTS OVER A DENIED RELATION IS THE TABLE DECISION'S OWN SENTENCE —
// round-1 review P1, on every door and under both provider shapes.
//
// The coordinator evaluates an UNCORRELATED `EXISTS` once at plan time, so a
// relation the identity may not read is refused there — `buildSubqueryPipelineFor`
// asks the shared access lookup for every relation the subquery's plan reads
// and deliberately preserves the SQLSTATE. The arm that added the evaluation
// discarded the error (`if err != nil { return node }`), so the filter shipped
// to the worker and the DAG arms answered
//
//	EXISTS subquery requires a SubqueryRunner
//
// where the scalar and IN spellings of the same denial say `permission denied
// for table "e7other"`. ADR-0034 item 6 is that the decision's sentence reaches
// the client, and one of three siblings at one site did not honour it.
//
// Both sides are asserted, as in the #945/#946 censuses: an identity that MAY
// read the relation answers through every one of these spellings.
func TestArcI1AnExistsOverADeniedRelationIsRefusedNotFailed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	// The four positions the round-1 fix reaches: a bare EXISTS, its NOT
	// spelling, one under an OR (which no arm reached before) and one under a
	// parenthesized NOT.
	shapes := []struct{ name, sql string }{
		{"exists", "SELECT COUNT(*) AS n FROM " + pmTable + " WHERE EXISTS " +
			"(SELECT 1 FROM " + pmOther + " WHERE id < 10)"},
		{"not_exists", "SELECT COUNT(*) AS n FROM " + pmTable + " WHERE NOT EXISTS " +
			"(SELECT 1 FROM " + pmOther + " WHERE id < 10)"},
		{"exists_under_or", "SELECT COUNT(*) AS n FROM " + pmTable + " WHERE id < 0 OR EXISTS " +
			"(SELECT 1 FROM " + pmOther + " WHERE id < 10)"},
		{"parenthesized_not_exists", "SELECT COUNT(*) AS n FROM " + pmTable +
			" WHERE NOT (EXISTS (SELECT 1 FROM " + pmOther + " WHERE id < 10))"},
	}

	for _, shape := range []struct {
		name     string
		provider func(*testing.T) *auth.Provider
	}{{"legacy", sec5LegacyProvider}, {"abac", sec5ABACProvider}} {
		t.Run(shape.name, func(t *testing.T) {
			rig := pmRigUpWith(t, ctx, shape.provider(t))
			want := fmt.Sprintf("permission denied for table %q", pmOther)
			for _, door := range rig.doors {
				for _, tc := range shapes {
					t.Run(door.name+"/denied/"+tc.name, func(t *testing.T) {
						res, err := door.run(t, "reader-key", tc.sql)
						if err == nil {
							t.Fatalf("a statement over a relation the identity may not read "+
								"was answered: %s\n  %v", tc.sql, res.rows)
						}
						if !strings.Contains(err.Error(), want) {
							t.Fatalf("the refusal is not the table decision's sentence\n"+
								"  sql:  %s\n  want: %s\n  got:  %v", tc.sql, want, err)
						}
						// The failure this cell exists for, named so a
						// regression reads as itself rather than as a
						// message mismatch.
						for _, leak := range []string{"requires a SubqueryRunner",
							"secret-internal-rule-name", "an operator note"} {
							if strings.Contains(err.Error(), leak) {
								t.Errorf("the refusal was swallowed and the query failed as %q "+
									"instead\n  sql: %s\n  got: %v", leak, tc.sql, err)
							}
						}
						if len(res.rows) != 0 {
							t.Errorf("a refused statement still produced %d rows", len(res.rows))
						}
					})
					t.Run(door.name+"/allowed/"+tc.name, func(t *testing.T) {
						if _, err := door.run(t, "wide-key", tc.sql); err != nil {
							t.Fatalf("an identity that MAY read the relation was refused: %s\n  %v",
								tc.sql, err)
						}
					})
				}
			}
		})
	}
}
