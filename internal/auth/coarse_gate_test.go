package auth

import (
	"context"
	"testing"
)

// TestTheRolesAllowListIsACoarseGateUnderABAC.
//
// A policy NARROWS what a role may do; it never widens it. Under an explicit
// `abac_policies:` block the evaluator used to answer alone, so a role written
// `allow: [read]` that a policy permitted to write could write — while DDL on
// the same door, which asks `RequirePermission`, demanded the permission. Two
// doors disagreeing about the same identity is what this decision removes.
func TestTheRolesAllowListIsACoarseGateUnderABAC(t *testing.T) {
	// The policy set says `reader` may WRITE `scratch`. The role says
	// `allow: [read]`.
	p := taProvider(t, taRoles(), taABAC())
	ctx := ContextWithIdentity(context.Background(), taIdentity("reader"))

	if err := TableAccess(ctx, p, "scratch", ActionWrite); err == nil {
		t.Fatal("a policy widened a role: `allow: [read]` wrote a relation the policy permitted")
	}
	// The same policy still GRANTS what the role does allow.
	if err := TableAccess(ctx, p, "events", ActionRead); err != nil {
		t.Fatalf("the coarse gate refused a read the role allows: %v", err)
	}
	// And a role that holds the permission is still narrowed BY the policy:
	// `writer` may write, but not a relation no rule names.
	wctx := ContextWithIdentity(context.Background(), taIdentity("writer"))
	if err := TableAccess(wctx, p, "scratch", ActionWrite); err != nil {
		t.Fatalf("a role holding write was refused a relation the policy allows: %v", err)
	}
	if err := TableAccess(wctx, p, "secrets", ActionWrite); err == nil {
		t.Fatal("the policy stopped narrowing: a relation no rule names was written")
	}
}

// `admin` grants everything, as `HasPermission` has always said, so the coarse
// gate does not take an administrator's access away.
func TestTheCoarseGateLetsAdminThrough(t *testing.T) {
	p := taProvider(t, taRoles(), []AccessControlPolicy{{
		Name: "root", Version: 1, Enabled: true,
		Rules: []PolicyRule{{
			ID: "root-all", EffectStr: "allow", Priority: 10,
			Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "root"}},
			Actions:  []Action{ActionRead, ActionWrite, ActionAdmin},
		}},
	}})
	root := &Identity{Name: "root", Role: "root", Method: "apikey",
		Tables: []string{"*"}, Perms: []string{"admin"}}
	ctx := ContextWithIdentity(context.Background(), root)
	for _, action := range []Action{ActionRead, ActionWrite, ActionAdmin} {
		if err := TableAccess(ctx, p, "events", action); err != nil {
			t.Errorf("admin refused %s: %v", action, err)
		}
	}
}

// An identity whose role the configuration does not define carries no
// permissions, so the coarse gate refuses it before any policy is consulted —
// which is what a JWT naming an unknown role produces.
func TestAnIdentityWithNoPermissionsIsRefusedBeforeThePolicy(t *testing.T) {
	p := taProvider(t, taRoles(), []AccessControlPolicy{{
		Name: "wide-open", Version: 1, Enabled: true,
		Rules: []PolicyRule{{
			ID: "everyone", EffectStr: "allow", Priority: 10,
			Actions: []Action{ActionRead, ActionWrite},
		}},
	}})
	ctx := ContextWithIdentity(context.Background(), taIdentity("stranger"))
	if err := TableAccess(ctx, p, "events", ActionRead); err == nil {
		t.Fatal("an identity with no permissions was granted by a policy that matches everyone")
	}
}
