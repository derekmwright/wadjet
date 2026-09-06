package auth

import (
	"context"
	"testing"
)

// A definer's GRANTS are re-resolved from the current role definitions, never
// replayed from the snapshot.
//
// `IdentitySnapshot` records who the definer WAS — name, role, method,
// attributes — and deliberately not what they could do. Before this, the
// identity it rebuilt carried an empty `Perms` and `Tables`, which the coarse
// gate and `CanAccessTable` read: on a `roles:`-only deployment every
// scheduled alert stopped running under its own creator. Re-resolving means
// the CURRENT configuration decides on every tick, so the persisted snapshot
// can never carry a stale grant either.

func definerProvider(t *testing.T, roles []RoleConfig) *Provider {
	t.Helper()
	authn, authz, err := Build(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "ops", Role: "ops"}},
		Roles:   roles,
	})
	if err != nil {
		t.Fatal(err)
	}
	return NewProvider(authn, authz, nil, nil)
}

func TestStampDefinerResolvesTheRolesCurrentGrants(t *testing.T) {
	p := definerProvider(t, []RoleConfig{
		{Name: "ops", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
	})
	snap := IdentitySnapshot{Name: "ops", Role: "ops", Method: "apikey"}

	ctx, attributed := StampDefiner(context.Background(), p, snap)
	if !attributed {
		t.Fatal("a snapshot with a name and a role is attributed")
	}
	id := IdentityFromContext(ctx)
	if id == nil {
		t.Fatal("no identity was stamped")
	}
	if len(id.Perms) == 0 || len(id.Tables) == 0 {
		t.Fatalf("the definer carries no grants: Perms=%v Tables=%v", id.Perms, id.Tables)
	}
	// Which is what the decision reads.
	for _, action := range []Action{ActionRead, ActionWrite, ActionAdmin} {
		if err := TableAccess(ctx, p, "anything", action); err != nil {
			t.Errorf("the definer was refused %s: %v", action, err)
		}
	}
}

// The other direction, and the reason to re-resolve rather than persist: a
// role narrowed since the alert was created takes effect on its next tick.
func TestADefinerWhoseRoleWasNarrowedIsRefused(t *testing.T) {
	snap := IdentitySnapshot{Name: "ops", Role: "ops", Method: "apikey"}

	narrowed := definerProvider(t, []RoleConfig{
		{Name: "ops", Tables: []string{"*"}, Allow: []string{"read"}},
	})
	ctx, _ := StampDefiner(context.Background(), narrowed, snap)
	if err := TableAccess(ctx, narrowed, "anything", ActionRead); err != nil {
		t.Fatalf("the narrowed definer lost the read it still holds: %v", err)
	}
	if err := TableAccess(ctx, narrowed, "anything", ActionWrite); err == nil {
		t.Fatal("a definer whose role lost `write` still writes")
	}

	// A role deleted outright holds nothing at all. The api key is renamed
	// with it: a key naming a role the configuration does not define is
	// itself a load refusal (#931), which is not the shape under test.
	authn, authz, err := Build(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "other", Role: "someone-else"}},
		Roles:   []RoleConfig{{Name: "someone-else", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	gone := NewProvider(authn, authz, nil, nil)
	ctx, _ = StampDefiner(context.Background(), gone, snap)
	if err := TableAccess(ctx, gone, "anything", ActionRead); err == nil {
		t.Fatal("a definer whose role no longer exists still reads")
	}
}

// A legacy snapshot with no role at all is still stamped, still unattributed,
// and still refused — the fail-closed shape StampDefiner was written for.
func TestAnUnattributedDefinerHoldsNothing(t *testing.T) {
	p := definerProvider(t, []RoleConfig{
		{Name: "ops", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
	})
	ctx, attributed := StampDefiner(context.Background(), p, IdentitySnapshot{})
	if attributed {
		t.Fatal("an empty snapshot reported an attributed definer")
	}
	if IdentityFromContext(ctx) == nil {
		t.Fatal("no identity was stamped: a nil identity is refused as 'authentication required', " +
			"which tells an operator nothing about the alert that needs recreating")
	}
	if err := TableAccess(ctx, p, "anything", ActionRead); err == nil {
		t.Fatal("an unattributed definer was granted a read")
	}
}

// With no provider, or auth disabled, nothing is stamped and nothing changes.
func TestStampDefinerChangesNothingWithoutAuth(t *testing.T) {
	ctx, attributed := StampDefiner(context.Background(), nil,
		IdentitySnapshot{Name: "ops", Role: "ops"})
	if !attributed {
		t.Fatal("a nil provider must report the definer attributed")
	}
	if IdentityFromContext(ctx) != nil {
		t.Fatal("a nil provider stamped an identity")
	}
}
