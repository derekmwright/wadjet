package server

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
)

// The gRPC cell of #945's census (round-1 P2).
//
// The brief's gate names four doors — embedded, pgwire, HTTP and gRPC — and the
// rig the rest of the census runs on (`pmRigUpWith`) carries eight doors, none
// of them gRPC. This is that door, driven through the REAL one: a bufconn
// server carrying the production interceptors, both provider shapes, with an
// identity that is AUTHENTICATED and not authorized on the relation the
// subquery names.
//
// THE FIXTURE MUST HOLD ROWS. The single-process arm decides a scalar
// subquery's relation while EVALUATING it, and an empty outer relation never
// evaluates the expression at all — so a gate written over the empty tables
// `grpcAuthzUp` creates would answer 0 rows and pass while proving nothing.
// The rows go in through the door itself, as the writer.
func TestAScalarSubqueryAsksTheTableDecisionOnGRPC(t *testing.T) {
	for _, shape := range []string{grpcAuthzLegacy, grpcAuthzABAC} {
		t.Run(shape, func(t *testing.T) {
			rig := grpcAuthzUp(t, shape, grpcAuthzConfig())
			// Seed both relations as the writer, whose role lists them all.
			w := grpcAuthzCtx("writer-key")
			for _, sql := range []string{
				"INSERT INTO allowed (id, ssn) VALUES (1, 'a'), (2, 'b')",
				"INSERT INTO secret (id, ssn) VALUES (7, 'x'), (8, 'y')",
			} {
				if _, err := rig.client.Query(w, &wadjetv1.QueryRequest{Sql: sql}); err != nil {
					t.Fatalf("seeding %q: %v", sql, err)
				}
			}
			// The control: the fixture really has rows, so the reader's own
			// relation answers and the subquery below is reached.
			res, err := rig.client.Query(grpcAuthzCtx("reader-key"),
				&wadjetv1.QueryRequest{Sql: "SELECT COUNT(*) AS c FROM allowed"})
			if err != nil {
				t.Fatalf("the reader's own relation was refused: %v", err)
			}
			if len(res.GetRows()) == 0 {
				t.Fatal("test setup: the outer relation is EMPTY, so no subquery is ever evaluated")
			}

			want := `permission denied for table "secret"`
			for _, tc := range []struct{ name, sql string }{
				{"select_list", "SELECT (SELECT MAX(id) FROM secret) AS m"},
				{"select_list_beside_a_permitted_scan",
					"SELECT id, (SELECT MAX(id) FROM secret) AS m FROM allowed"},
				{"where", "SELECT id FROM allowed WHERE id = (SELECT MAX(id) FROM secret)"},
				{"case", "SELECT CASE WHEN (SELECT MAX(id) FROM secret) > 0 THEN 1 ELSE 0 END AS m"},
				{"cte_body", "WITH c AS (SELECT (SELECT MAX(id) FROM secret) AS m) SELECT m FROM c"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					out, qerr := rig.client.Query(grpcAuthzCtx("reader-key"),
						&wadjetv1.QueryRequest{Sql: tc.sql})
					if qerr == nil {
						t.Fatalf("the relation the identity may not read was SERVED: %v",
							out.GetRows())
					}
					if got := grpcAuthzCode(qerr); got != codes.PermissionDenied {
						t.Errorf("code = %s, want PermissionDenied — an authorization "+
							"refusal a client cannot tell from a server fault is not one",
							got)
					}
					if !strings.Contains(qerr.Error(), want) {
						t.Errorf("refusal does not carry the shared decision's text\n"+
							"  want: %s\n  got:  %v", want, qerr)
					}
					for _, leak := range []string{"physical plan", "local execution",
						"subquery could not be executed", "scanning file"} {
						if strings.Contains(qerr.Error(), leak) {
							t.Errorf("the refusal names an internal route (%q): %v", leak, qerr)
						}
					}
				})
				// The other side: an identity that MAY read `secret` still
				// gets the value through the same spelling on the same door.
				t.Run(tc.name+"/allowed", func(t *testing.T) {
					if _, err := rig.client.Query(grpcAuthzCtx("writer-key"),
						&wadjetv1.QueryRequest{Sql: tc.sql}); err != nil {
						t.Fatalf("an identity that MAY read the relation was refused: %v", err)
					}
				})
			}
		})
	}
}

// QueryStream is the second data RPC and it drains its result separately, so a
// refusal raised while the rows are read has its own mapping path (#934's
// drain half). One spelling is enough to say the class is the same there.
func TestAScalarSubqueryRefusesOnTheGRPCStreamToo(t *testing.T) {
	rig := grpcAuthzUp(t, grpcAuthzABAC, grpcAuthzConfig())
	w := grpcAuthzCtx("writer-key")
	for _, sql := range []string{
		"INSERT INTO allowed (id, ssn) VALUES (1, 'a')",
		"INSERT INTO secret (id, ssn) VALUES (7, 'x')",
	} {
		if _, err := rig.client.Query(w, &wadjetv1.QueryRequest{Sql: sql}); err != nil {
			t.Fatalf("seeding %q: %v", sql, err)
		}
	}
	stream, err := rig.client.QueryStream(grpcAuthzCtx("reader-key"),
		&wadjetv1.QueryRequest{Sql: "SELECT id, (SELECT MAX(id) FROM secret) AS m FROM allowed"})
	if err == nil {
		_, err = stream.Recv()
	}
	if err == nil {
		t.Fatal("QueryStream served a relation the identity may not read")
	}
	if got := grpcAuthzCode(err); got != codes.PermissionDenied {
		t.Errorf("code = %s, want PermissionDenied", got)
	}
	if !strings.Contains(err.Error(), `permission denied for table "secret"`) {
		t.Errorf("refusal does not carry the shared decision's text: %v", err)
	}
}
