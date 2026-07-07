package auth

import (
	"context"
	"errors"
	"testing"
)

func newTestProvider(t *testing.T, enabled bool) *Provider {
	t.Helper()
	cfg := Config{Enabled: enabled}
	if enabled {
		cfg.APIKeys = []APIKeyDef{{Key: "k", Name: "svc", Role: "admin"}}
		cfg.Roles = []RoleConfig{
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"admin"}},
			{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}},
		}
	}
	authn, authz := New(cfg)
	return NewProvider(authn, authz, nil, nil)
}

func TestRequirePermission(t *testing.T) {
	secured := newTestProvider(t, true)
	if !secured.Enabled() {
		t.Fatal("provider with an API key should be Enabled()")
	}

	adminID := &Identity{Name: "svc", Role: "admin", Perms: []string{"admin"}}
	readerID := &Identity{Name: "bob", Role: "reader", Perms: []string{"read"}}

	t.Run("nil provider is a no-op", func(t *testing.T) {
		if err := RequirePermission(nil, context.Background(), "admin"); err != nil {
			t.Fatalf("nil provider should permit, got %v", err)
		}
	})

	t.Run("disabled provider is a no-op", func(t *testing.T) {
		if err := RequirePermission(newTestProvider(t, false), context.Background(), "admin"); err != nil {
			t.Fatalf("disabled provider should permit, got %v", err)
		}
	})

	t.Run("enabled + no identity is rejected", func(t *testing.T) {
		err := RequirePermission(secured, context.Background(), "admin")
		if err == nil {
			t.Fatal("expected rejection with no identity")
		}
		if !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("expected ErrUnauthorized, got %v", err)
		}
	})

	t.Run("enabled + wrong permission is rejected", func(t *testing.T) {
		ctx := ContextWithIdentity(context.Background(), readerID)
		if err := RequirePermission(secured, ctx, "admin"); err == nil {
			t.Fatal("reader should not hold admin")
		}
	})

	t.Run("enabled + correct permission passes", func(t *testing.T) {
		ctx := ContextWithIdentity(context.Background(), adminID)
		if err := RequirePermission(secured, ctx, "admin"); err != nil {
			t.Fatalf("admin should pass, got %v", err)
		}
	})
}

func TestIdentitySnapshotRoundTrip(t *testing.T) {
	id := &Identity{
		Name:       "alice",
		Role:       "analyst",
		Method:     "apikey",
		Attributes: Attributes{"clearance": "secret", "team": "blue"},
	}
	ctx := ContextWithIdentity(context.Background(), id)

	snap := SnapshotIdentity(ctx)
	if snap.Empty() {
		t.Fatal("snapshot of a real identity should not be Empty()")
	}
	if snap.Name != "alice" || snap.Role != "analyst" || snap.Method != "apikey" {
		t.Fatalf("snapshot lost core fields: %+v", snap)
	}
	if snap.Attributes["clearance"] != "secret" || snap.Attributes["team"] != "blue" {
		t.Fatalf("snapshot lost attributes: %+v", snap.Attributes)
	}

	// Round-trip through ToIdentity → ToSubject must carry role + attributes,
	// which is what the ABAC evaluator keys on.
	sub := snap.ToIdentity().ToSubject()
	if sub.Attributes["role"] != "analyst" || sub.Attributes["clearance"] != "secret" {
		t.Fatalf("reconstructed subject lost attributes: %+v", sub.Attributes)
	}
}

func TestSnapshotIdentityNoIdentity(t *testing.T) {
	if snap := SnapshotIdentity(context.Background()); !snap.Empty() {
		t.Fatalf("snapshot with no identity should be Empty(), got %+v", snap)
	}
}

func TestStampDefiner(t *testing.T) {
	secured := newTestProvider(t, true)

	t.Run("disabled provider leaves context unchanged", func(t *testing.T) {
		snap := IdentitySnapshot{Name: "alice", Role: "analyst"}
		ctx, attributed := StampDefiner(context.Background(), nil, snap)
		if !attributed {
			t.Fatal("nil provider should report attributed=true (nothing to enforce)")
		}
		if IdentityFromContext(ctx) != nil {
			t.Fatal("nil provider should not stamp an identity")
		}
	})

	t.Run("enabled + attributed stamps the reconstructed identity", func(t *testing.T) {
		snap := IdentitySnapshot{Name: "alice", Role: "analyst", Method: "apikey"}
		ctx, attributed := StampDefiner(context.Background(), secured, snap)
		if !attributed {
			t.Fatal("a non-empty snapshot should be attributed")
		}
		got := IdentityFromContext(ctx)
		if got == nil || got.Role != "analyst" {
			t.Fatalf("expected analyst identity stamped, got %+v", got)
		}
	})

	t.Run("enabled + empty snapshot fails closed with a non-nil identity", func(t *testing.T) {
		// The critical invariant: a legacy alert (empty snapshot) must stamp a
		// NON-NIL identity so EnforcePlanPolicies does not fail-open on a nil
		// identity — a role-less identity routes into ABAC default-deny.
		ctx, attributed := StampDefiner(context.Background(), secured, IdentitySnapshot{})
		if attributed {
			t.Fatal("empty snapshot must report attributed=false")
		}
		got := IdentityFromContext(ctx)
		if got == nil {
			t.Fatal("empty snapshot under enabled auth must still stamp a non-nil identity (else ABAC fail-opens)")
		}
		if got.Role != "" {
			t.Fatalf("expected a role-less identity, got role %q", got.Role)
		}
	})
}
