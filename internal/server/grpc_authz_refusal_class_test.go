package server

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
)

// TestGRPCQueryRefusalCarriesPermissionDenied holds the refusal CLASS on the
// two SQL RPCs.
//
// Both wrapped every engine error as codes.Internal, so a statement refused
// with SQLSTATE 42501 before a row was touched reached the client as "the
// server failed". A client cannot tell "you may not do that" from "we broke":
// it retries the second and gives up on the first, and an operator reading the
// code sees an outage where there is a working control. The other doors carry
// the class (HTTP 403, pgwire 42501).
//
// The cells are DML refusals, which are the ones that carry 42501 today:
// auth.EnforceDMLPolicies refuses an ActionWrite before anything is read or
// written. A table-level SELECT denial still crosses as Internal because
// internal/auth/plan_enforce.go returns it without a class on ANY door — that
// is recorded as a residual, not asserted here, and this mapping needs no edit
// when it is fixed.
func TestGRPCQueryRefusalCarriesPermissionDenied(t *testing.T) {
	rig := grpcAuthzUp(t, grpcAuthzABAC, grpcAuthzConfig())

	for _, sql := range []string{
		"DELETE FROM allowed WHERE id = 1",
		"INSERT INTO allowed (id, ssn) VALUES (1, 'x')",
		"UPDATE allowed SET ssn = 'y' WHERE id = 1",
	} {
		_, err := rig.client.Query(grpcAuthzCtx("reader-key"), &wadjetv1.QueryRequest{Sql: sql})
		if got := grpcAuthzCode(err); got != codes.PermissionDenied {
			t.Errorf("reader Query(%q): code %v, want PermissionDenied (err %v)", sql, got, err)
		}
		// The message is the refusal's own, not "query error: ..." — a
		// refusal is not an execution failure.
		if err != nil && strings.Contains(err.Error(), "query error:") {
			t.Errorf("reader Query(%q) message %v\n  want the refusal's own text", sql, err)
		}
		if err == nil || !strings.Contains(err.Error(), "permission denied for table") {
			t.Errorf("reader Query(%q) message %v\n  want the 42501 text", sql, err)
		}
	}

	// The same statements from an identity that may write are unaffected —
	// the mapping is on the refusal, not on the statement.
	if _, err := rig.client.Query(grpcAuthzCtx("writer-key"),
		&wadjetv1.QueryRequest{Sql: "DELETE FROM allowed WHERE id = 1"}); err != nil {
		t.Errorf("writer Query(DELETE): %v", err)
	}

	// An ordinary execution error keeps codes.Internal: this narrows the
	// Internal bucket, it does not empty it.
	_, err := rig.client.Query(grpcAuthzCtx("admin-key"),
		&wadjetv1.QueryRequest{Sql: "SELECT * FROM no_such_table"})
	if got := grpcAuthzCode(err); got != codes.Internal {
		t.Errorf("admin Query(missing table): code %v, want Internal (err %v)", got, err)
	}
}
