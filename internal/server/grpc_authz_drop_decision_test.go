package server

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/auth"
)

// grpcAuthzDenySecret is an explicit ABAC deny on `secret` for a named role,
// covering every action — including the ones a DROP is made of.
func grpcAuthzDenySecret(role string) auth.AccessControlPolicy {
	return auth.AccessControlPolicy{
		Name:    "deny-secret-to-" + role,
		Version: 1,
		Enabled: true,
		Rules: []auth.PolicyRule{{
			ID:          "deny-secret-to-" + role,
			EffectStr:   "deny",
			Priority:    1,
			Description: role + " may not touch the secret table",
			Subjects:    []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: role}},
			Resources:   []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "secret"}},
			Actions: []auth.Action{
				auth.ActionRead, auth.ActionWrite, auth.ActionDescribe,
				auth.ActionCreate, auth.ActionDrop,
			},
		}},
	}
}

// TestGRPCDropTableAsksTheTableDecision holds the round-1 decision: DDL on an
// EXISTING relation asks the TABLE decision, not only the permission.
//
// `HasPermission(id, "write")` alone answered "may this identity write
// something", never "may it write THIS". So an identity the evaluator refused
// to show a single column of — `DescribeTable(secret)` PermissionDenied,
// `DELETE FROM secret` 42501 — could still destroy the whole relation with
// `DropTable(secret)`, and a role scoped to `tables: [allowed]` could drop
// every other table on the server. A control that governs reading a row has to
// govern removing every row.
//
// CREATE keeps asking the permission alone: there is no existing relation for a
// table-scoped rule to match, which is the line PostgreSQL draws too.
func TestGRPCDropTableAsksTheTableDecision(t *testing.T) {
	// --- the ABAC arm: an explicit deny on a relation the role otherwise owns.
	t.Run("explicit-abac-deny", func(t *testing.T) {
		cfg := grpcAuthzConfig() // writer holds tables: ["*"], allow: [read, write]
		rig := grpcAuthzUp(t, grpcAuthzABAC, cfg, grpcAuthzDenySecret("writer"))

		_, err := rig.client.DropTable(grpcAuthzCtx("writer-key"), &wadjetv1.DropTableRequest{Name: "secret"})
		if got := grpcAuthzCode(err); got != codes.PermissionDenied {
			t.Errorf("writer DropTable(secret) under an explicit deny: code %v, want PermissionDenied (err %v)", got, err)
		}
		if !grpcAuthzHasTable(t, rig.cat, "secret") {
			t.Error("writer DropTable(secret) was refused but the table is GONE: the deny lost to the drop")
		}
		if err == nil || !strings.Contains(err.Error(), `permission denied for table "secret"`) {
			t.Errorf("message %v\n  want the shared helper's 42501 text naming the table", err)
		}

		// if_exists must not launder it either.
		_, err = rig.client.DropTable(grpcAuthzCtx("writer-key"),
			&wadjetv1.DropTableRequest{Name: "secret", IfExists: true})
		if got := grpcAuthzCode(err); got != codes.PermissionDenied {
			t.Errorf("writer DropTable(secret, if_exists): code %v, want PermissionDenied (err %v)", got, err)
		}
		if !grpcAuthzHasTable(t, rig.cat, "secret") {
			t.Error("writer DropTable(secret, if_exists) was refused but the table is gone")
		}

		// The same identity, a relation the deny does not name: allowed, with
		// the side effect. The rule narrows one table, not the role.
		if _, err := rig.client.DropTable(grpcAuthzCtx("writer-key"),
			&wadjetv1.DropTableRequest{Name: "protected"}); err != nil {
			t.Fatalf("writer DropTable(protected): %v", err)
		}
		if grpcAuthzHasTable(t, rig.cat, "protected") {
			t.Error("writer DropTable(protected) returned OK but the table still exists")
		}

		// CREATE is permission-only: the denied identity still creates a NEW
		// relation, which is the line this decision deliberately draws.
		if _, err := rig.client.CreateTable(grpcAuthzCtx("writer-key"), &wadjetv1.CreateTableRequest{
			Name:    "brand_new",
			Columns: []*wadjetv1.ColumnDef{{Name: "id", Type: "BIGINT"}},
		}); err != nil {
			t.Fatalf("writer CreateTable(brand_new) under a deny on another table: %v", err)
		}
		if !grpcAuthzHasTable(t, rig.cat, "brand_new") {
			t.Error("writer CreateTable(brand_new) returned OK but the table does not exist")
		}
	})

	// --- the legacy arm: a role SCOPED to one table. No evaluator at all, so
	// this is the half of the decision `CanAccessTable` carries — and it is
	// exactly the cell `HasPermission` alone could not see.
	for _, shape := range []string{grpcAuthzLegacy, grpcAuthzABAC} {
		t.Run("scoped-writer/"+shape, func(t *testing.T) {
			cfg := grpcAuthzConfig()
			cfg.APIKeys = append(cfg.APIKeys,
				auth.APIKeyDef{Key: "scoped-writer-key", Name: "scoped", Role: "scoped_writer"})
			cfg.Roles = append(cfg.Roles,
				auth.RoleConfig{Name: "scoped_writer", Tables: []string{"allowed"}, Allow: []string{"read", "write"}})
			rig := grpcAuthzUp(t, shape, cfg)

			_, err := rig.client.DropTable(grpcAuthzCtx("scoped-writer-key"),
				&wadjetv1.DropTableRequest{Name: "secret"})
			if got := grpcAuthzCode(err); got != codes.PermissionDenied {
				t.Errorf("scoped writer DropTable(secret): code %v, want PermissionDenied (err %v)", got, err)
			}
			if !grpcAuthzHasTable(t, rig.cat, "secret") {
				t.Error("scoped writer dropped a table outside its role's table list")
			}

			// Its OWN table still drops.
			if _, err := rig.client.DropTable(grpcAuthzCtx("scoped-writer-key"),
				&wadjetv1.DropTableRequest{Name: "allowed"}); err != nil {
				t.Fatalf("scoped writer DropTable(allowed): %v", err)
			}
			if grpcAuthzHasTable(t, rig.cat, "allowed") {
				t.Error("scoped writer DropTable(allowed) returned OK but the table still exists")
			}
		})
	}

	// --- the wildcard identities are untouched: an authorized drop still works
	// on every shape, which is the half of the boundary a tightening breaks.
	for _, shape := range []string{grpcAuthzLegacy, grpcAuthzABAC} {
		t.Run("wildcard-unaffected/"+shape, func(t *testing.T) {
			rig := grpcAuthzUp(t, shape, grpcAuthzConfig())
			for _, key := range []string{"writer-key", "admin-key"} {
				name := "protected"
				if key == "admin-key" {
					name = "secret"
				}
				if _, err := rig.client.DropTable(grpcAuthzCtx(key), &wadjetv1.DropTableRequest{Name: name}); err != nil {
					t.Fatalf("%s DropTable(%s): %v", key, name, err)
				}
				if grpcAuthzHasTable(t, rig.cat, name) {
					t.Errorf("%s DropTable(%s) returned OK but the table still exists", key, name)
				}
			}
		})
	}
}
