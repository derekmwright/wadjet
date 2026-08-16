package main

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
)

// TestResolveMCPAuth is the fail-closed guard for the MCP entrypoint: when an
// auth provider is configured, an MCP session must present a valid credential
// or the server refuses to start. This is the fix for MCP bypassing ABAC —
// before it, MCP opened an unauthenticated DB regardless of config.
func TestResolveMCPAuth(t *testing.T) {
	// A provider with an API key configured → Enabled() true.
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "good-key", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	secured := auth.NewProvider(authn, authz, nil, nil)
	if !secured.Enabled() {
		t.Fatal("test setup: provider with an API key should report Enabled()")
	}

	// A provider with no credential mechanism → Enabled() false.
	openAuthn, openAuthz := auth.New(auth.Config{Enabled: false})
	openProvider := auth.NewProvider(openAuthn, openAuthz, nil, nil)
	if openProvider.Enabled() {
		t.Fatal("test setup: provider with no mechanism should report !Enabled()")
	}

	t.Run("secured_missing_token_fails_closed", func(t *testing.T) {
		id, err := resolveMCPAuth(secured, "")
		if err == nil {
			t.Fatal("expected error when auth enabled and no token supplied")
		}
		if id != nil {
			t.Fatalf("expected nil identity on failure, got %v", id)
		}
	})

	t.Run("secured_bad_token_fails_closed", func(t *testing.T) {
		id, err := resolveMCPAuth(secured, "wrong-key")
		if err == nil {
			t.Fatal("expected error for an invalid credential")
		}
		if id != nil {
			t.Fatalf("expected nil identity on failure, got %v", id)
		}
	})

	t.Run("secured_good_token_yields_identity", func(t *testing.T) {
		id, err := resolveMCPAuth(secured, "good-key")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id == nil || id.Role != "analyst" {
			t.Fatalf("expected analyst identity, got %v", id)
		}
	})

	t.Run("no_provider_runs_unauthenticated", func(t *testing.T) {
		id, err := resolveMCPAuth(nil, "")
		if err != nil || id != nil {
			t.Fatalf("nil provider should yield (nil, nil), got (%v, %v)", id, err)
		}
	})

	t.Run("auth_disabled_runs_unauthenticated", func(t *testing.T) {
		// No mechanism configured: nothing to enforce, no token required.
		id, err := resolveMCPAuth(openProvider, "")
		if err != nil || id != nil {
			t.Fatalf("disabled-auth provider should yield (nil, nil), got (%v, %v)", id, err)
		}
	})
}
