package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/auth"
)

// THE UNKNOWN-COLUMN ERROR DOES NOT PUBLISH A DENIED RELATION'S COLUMNS —
// #946, on every door and under both provider shapes.
//
// `SELECT nocol FROM e7other` answered
//
//	unknown column "nocol" (available: id, note)
//
// for an identity that may not read `e7other` at all — 42703 with the
// relation's whole column list, where the table decision would have answered
// 42501 with nothing. `docs/security.md` and ADR-0034 say an identity that may
// not read a table may not read its schema either, and an error's hint is
// schema.
//
// The binder POOLS the schemas of every relation a statement resolves before
// it writes that hint, which is why the fix is per-relation and not one call
// in front of one function: `SELECT id FROM e7emp WHERE id = (SELECT
// MAX(nocol) FROM e7other)` published `acct, amt, dept, id, note, salary, ssn`
// — e7emp's columns UNIONED with e7other's `note`.
//
// Every cell is asserted from BOTH sides. A denied relation is 42501 carrying
// the shared decision's own sentence and NO column name; an allowed one is
// still 42703 carrying the list, because "the hint is metadata" is a rule
// about the DECISION and not about hints.
func TestArcH1AnUnknownColumnPublishesNoDeniedRelationsSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	// Five spellings. The FROM list is the filing's own; the other four are
	// the positions the binder reaches that a FROM-list guard would not —
	// a subquery block, a POOLED statement whose outer relation is allowed,
	// a CTE body and a derived table.
	shapes := []struct{ name, sql string }{
		{"from-list", "SELECT nocol FROM " + pmOther},
		{"subquery", "SELECT (SELECT MAX(nocol) FROM " + pmOther + ") AS m"},
		{"pooled-with-an-allowed-outer-relation",
			"SELECT id FROM " + pmTable + " WHERE id = (SELECT MAX(nocol) FROM " + pmOther + ")"},
		{"cte-body", "WITH c AS (SELECT nocol FROM " + pmOther + ") SELECT * FROM c"},
		{"derived-table", "SELECT d.x FROM (SELECT nocol AS x FROM " + pmOther + ") d"},
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
						_, err := door.run(t, "reader-key", tc.sql)
						if err == nil {
							t.Fatalf("a statement over a relation the identity may not read "+
								"was answered: %s", tc.sql)
						}
						if !strings.Contains(err.Error(), want) {
							t.Fatalf("refusal does not carry the table decision's sentence\n"+
								"  sql:  %s\n  want: %s\n  got:  %v", tc.sql, want, err)
						}
						// The whole of #946: no column of the denied relation
						// is named, and neither is the pooled union of every
						// other relation the statement touches.
						for _, leak := range []string{"available:", "note", "unknown column",
							"salary", "ssn", "acct", "dept"} {
							if strings.Contains(err.Error(), leak) {
								t.Errorf("the refusal publishes %q, which is schema the table "+
									"decision hides\n  sql: %s\n  got: %v", leak, tc.sql, err)
							}
						}
					})
					t.Run(door.name+"/allowed/"+tc.name, func(t *testing.T) {
						_, err := door.run(t, "wide-key", tc.sql)
						if err == nil {
							t.Fatalf("a reference to a column that does not exist was "+
								"answered: %s", tc.sql)
						}
						// The other side, and it is what says the change is
						// about the DECISION and not about hints: an identity
						// that MAY read the relation still gets PostgreSQL's
						// 42703 with the column list.
						for _, keep := range []string{`unknown column "nocol"`, "available:", "note"} {
							if !strings.Contains(err.Error(), keep) {
								t.Errorf("an allowed identity lost the unknown-column hint "+
									"(%q missing)\n  sql: %s\n  got: %v", keep, tc.sql, err)
							}
						}
						if strings.Contains(err.Error(), "permission denied") {
							t.Errorf("an identity that MAY read the relation was refused: %v", err)
						}
					})
				}
				// A relation that does not EXIST keeps PostgreSQL's 42P01:
				// the decision is asked only once the catalog has found the
				// table, so "denied" and "absent" stay different answers.
				t.Run(door.name+"/absent-relation-is-not-a-refusal", func(t *testing.T) {
					_, err := door.run(t, "reader-key", "SELECT nocol FROM e7nosuchtable")
					if err == nil {
						t.Fatal("a statement over a relation that does not exist was answered")
					}
					if strings.Contains(err.Error(), "permission denied") {
						t.Errorf("a relation that does not exist was refused as denied: %v", err)
					}
				})
			}
		})
	}
}

// The gRPC door, which pmRigUpWith does not carry: the same claim, mapped to
// codes.PermissionDenied.
func TestArcH1AnUnknownColumnPublishesNoDeniedSchemaOnGRPC(t *testing.T) {
	for _, shape := range []string{grpcAuthzLegacy, grpcAuthzABAC} {
		t.Run(shape, func(t *testing.T) {
			rig := grpcAuthzUp(t, shape, grpcAuthzConfig())
			w := grpcAuthzCtx("writer-key")
			for _, sql := range []string{
				"INSERT INTO allowed (id, ssn) VALUES (1, 'a'), (2, 'b')",
				"INSERT INTO secret (id, ssn) VALUES (7, 'x'), (8, 'y')",
			} {
				if _, err := rig.client.Query(w, &wadjetv1.QueryRequest{Sql: sql}); err != nil {
					t.Fatalf("seeding %q: %v", sql, err)
				}
			}
			want := `permission denied for table "secret"`
			for _, tc := range []struct{ name, sql string }{
				{"from-list", "SELECT nocol FROM secret"},
				{"subquery", "SELECT (SELECT MAX(nocol) FROM secret) AS m"},
				{"pooled", "SELECT id FROM allowed WHERE id = (SELECT MAX(nocol) FROM secret)"},
			} {
				tc := tc
				t.Run("denied/"+tc.name, func(t *testing.T) {
					out, qerr := rig.client.Query(grpcAuthzCtx("reader-key"),
						&wadjetv1.QueryRequest{Sql: tc.sql})
					if qerr == nil {
						t.Fatalf("the denied relation's schema was published: %v", out.GetRows())
					}
					if got := grpcAuthzCode(qerr); got != codes.PermissionDenied {
						t.Errorf("code = %s, want PermissionDenied", got)
					}
					if !strings.Contains(qerr.Error(), want) {
						t.Errorf("refusal does not carry the decision's sentence: %v", qerr)
					}
					for _, leak := range []string{"available:", "unknown column", "ssn"} {
						if strings.Contains(qerr.Error(), leak) {
							t.Errorf("the refusal publishes %q: %v", leak, qerr)
						}
					}
				})
				t.Run("allowed/"+tc.name, func(t *testing.T) {
					_, qerr := rig.client.Query(w, &wadjetv1.QueryRequest{Sql: tc.sql})
					if qerr == nil {
						t.Fatal("a reference to a column that does not exist was answered")
					}
					if !strings.Contains(qerr.Error(), `unknown column "nocol"`) {
						t.Errorf("an allowed identity lost the unknown-column hint: %v", qerr)
					}
				})
			}
		})
	}
}
