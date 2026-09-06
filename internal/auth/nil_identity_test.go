package auth

import (
	"context"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// mustParse is the DML fixture's parse step.
func mustParse(t *testing.T, sql string) *plansql.ParsedQuery {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	return parsed
}

// TestAMissingIdentityIsRefusedOnTheDataPathsToo.
//
// The shared plan and DML paths returned early on `identity == nil`, so an
// embedded caller that attached an enabled provider and then queried without
// stamping an identity could SELECT and INSERT — while DESCRIBE, DDL and the
// shared table-access decision all refused it. One boundary, one answer.
func TestAMissingIdentityIsRefusedOnTheDataPathsToo(t *testing.T) {
	ctx := context.Background()
	cat := peCatalog(t, ctx)
	p := envProvider(t, taABAC())

	if _, _, err := envEnforce(t, ctx, p, cat, "SELECT id FROM pe_emp"); err == nil {
		t.Fatal("a SELECT with no identity ran under an enabled provider")
	}
	parsed := mustParse(t, "DELETE FROM pe_emp WHERE id = 1")
	if err := EnforceDMLPolicies(ctx, p, cat, parsed, "embedded"); err == nil {
		t.Fatal("a DELETE with no identity ran under an enabled provider")
	}
	// The metadata decision already refused it; now all three agree.
	if err := TableAccess(ctx, p, "pe_emp", ActionRead); err == nil {
		t.Fatal("the shared decision allowed a missing identity")
	}

	// Without a provider — the embedded shape that never called
	// SetAuthProvider — nothing is enforced and nothing changes.
	if _, _, err := envEnforce(t, ctx, nil, cat, "SELECT id FROM pe_emp"); err != nil {
		t.Fatalf("a nil provider refused a read: %v", err)
	}
	if err := EnforceDMLPolicies(ctx, nil, cat, parsed, "embedded"); err != nil {
		t.Fatalf("a nil provider refused a write: %v", err)
	}
}
