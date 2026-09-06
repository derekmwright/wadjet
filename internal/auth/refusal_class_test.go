package auth

import (
	"context"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestADeniedReadAndADeniedWriteRefuseInTheSameClass.
//
// The same identity, the same relation, the same policy: one reads it and one
// writes it, and both are authorization refusals. They were not the same
// refusal. `EnforceDMLPolicies` returned a `sqlerr` 42501; `EnforcePlanPolicies`
// returned a BARE `fmt.Errorf`, so the denied SELECT reached pgwire with no
// SQLSTATE of its own (the generic 42000) and gRPC as `codes.Internal` — an
// authorization decision a client could not tell from a server fault.
//
// A refusal has a class, and the same operation refuses with the same class on
// every door (ADR-0034).
func TestADeniedReadAndADeniedWriteRefuseInTheSameClass(t *testing.T) {
	ctx := context.Background()
	cat := peCatalog(t, ctx)
	// `analyst` is granted nothing at all: every rule below names `auditor`,
	// so both decisions land on the evaluator's default deny.
	p := envProvider(t, []AccessControlPolicy{{
		Name: "other-role-only", Version: 1, Enabled: true,
		Rules: []PolicyRule{{
			ID: "auditor-only", EffectStr: "allow", Priority: 10,
			Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "auditor"}},
			Actions:  []Action{ActionRead, ActionWrite},
		}},
	}})
	idCtx := ContextWithIdentity(ctx, &Identity{Name: "analyst", Role: "analyst", Method: "apikey"})

	_, _, readErr := envEnforce(t, idCtx, p, cat, "SELECT id FROM pe_emp")
	if readErr == nil {
		t.Fatal("a denied read was allowed")
	}
	parsed, err := plansql.Parse("DELETE FROM pe_emp WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	writeErr := EnforceDMLPolicies(idCtx, p, cat, parsed, "embedded")
	if writeErr == nil {
		t.Fatal("a denied write was allowed")
	}

	if got := sqlerr.StateOf(readErr); got != "42501" {
		t.Errorf("denied READ carries SQLSTATE %q, want 42501 (the write carries %q)",
			got, sqlerr.StateOf(writeErr))
	}
	if got := sqlerr.StateOf(writeErr); got != "42501" {
		t.Errorf("denied WRITE carries SQLSTATE %q, want 42501", got)
	}
	for _, tc := range []struct {
		name string
		err  error
	}{{"read", readErr}, {"write", writeErr}} {
		if !strings.Contains(tc.err.Error(), `permission denied for table "pe_emp"`) {
			t.Errorf("denied %s says %q, want PostgreSQL's "+
				`permission denied for table "pe_emp"`, tc.name, tc.err.Error())
		}
	}

	// And the shared table-access decision, which a metadata door asks, says
	// the same thing about the same relation — metadata and data agree.
	metaErr := TableAccess(idCtx, p, "pe_emp", ActionRead)
	if metaErr == nil {
		t.Fatal("the shared decision allowed a relation the plan path denies")
	}
	if got := sqlerr.StateOf(metaErr); got != "42501" {
		t.Errorf("the shared decision carries SQLSTATE %q, want 42501", got)
	}
}
