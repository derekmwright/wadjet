package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
)

// TestADeniedSelectReachesPGWireAs42501.
//
// A denied SELECT left `EnforcePlanPolicies` as a bare `fmt.Errorf`, so the
// wire carried the generic 42000 — indistinguishable from a syntax-class
// fault — while the SAME identity's DELETE on the SAME relation carried 42501.
// A refusal has a class, and it is the same one on every door (ADR-0034).
func TestADeniedSelectReachesPGWireAs42501(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// `analyst` is granted nothing: every rule names `auditor`.
	authn, authz, err := auth.Build(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "analyst-key", Name: "analyst", Role: "analyst"}},
		Roles: []auth.RoleConfig{
			{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read", "write"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "auditor-only", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{{
			ID: "auditor-only", EffectStr: "allow", Priority: 10,
			Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "auditor"}},
			Actions:  []auth.Action{auth.ActionRead, auth.ActionWrite},
		}},
	}}))

	db := envDB(t, ctx, provider)
	pg := pgwire.NewServer(db, pgwire.Config{AuthProvider: provider}, nil)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Shutdown)

	conn, cerr := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://wadjet:analyst-key@%s/wadjet?sslmode=disable", pg.Addr()))
	if cerr != nil {
		t.Fatalf("connect: %v", cerr)
	}
	defer conn.Close(ctx)

	for _, tc := range []struct{ name, sql string }{
		{"select", "SELECT id FROM " + pmTable},
		{"delete", "DELETE FROM " + pmTable + " WHERE id = 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, qerr := conn.Exec(ctx, tc.sql)
			if qerr == nil {
				t.Fatalf("%s was allowed under a policy that grants this identity nothing", tc.name)
			}
			var pgErr *pgconn.PgError
			if !errors.As(qerr, &pgErr) {
				t.Fatalf("refusal is not a PostgreSQL error: %v", qerr)
			}
			if pgErr.Code != "42501" {
				t.Errorf("SQLSTATE = %q, want 42501 (insufficient_privilege): %s",
					pgErr.Code, pgErr.Message)
			}
			if !strings.Contains(pgErr.Message, `permission denied for table "`+pmTable+`"`) {
				t.Errorf("message = %q, want PostgreSQL's permission-denied shape", pgErr.Message)
			}
		})
	}
}
