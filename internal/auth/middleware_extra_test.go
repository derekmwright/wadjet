package auth

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestContextWithTableDecisions(t *testing.T) {
	ctx := context.Background()
	decisions := TableDecisions{
		"events": &TableDecision{Allowed: true, RuleID: "r1"},
		"users":  &TableDecision{Allowed: false, Reason: "denied"},
	}

	ctx = ContextWithTableDecisions(ctx, decisions)
	got := TableDecisionsFromContext(ctx)
	if got == nil {
		t.Fatal("expected non-nil table decisions")
	}
	if got["events"] == nil || !got["events"].Allowed {
		t.Error("expected events to be allowed")
	}
	if got["users"] == nil || got["users"].Allowed {
		t.Error("expected users to be denied")
	}
}

func TestTableDecisionsFromContextEmpty(t *testing.T) {
	ctx := context.Background()
	got := TableDecisionsFromContext(ctx)
	if got != nil {
		t.Fatal("expected nil from empty context")
	}
}

func TestContextWithRowFilters(t *testing.T) {
	ctx := context.Background()
	filters := RowFilters{
		"events": "department = 'engineering'",
		"users":  "region = 'us-east'",
	}

	ctx = ContextWithRowFilters(ctx, filters)
	got := RowFiltersFromContext(ctx)
	if got == nil {
		t.Fatal("expected non-nil row filters")
	}
	if got["events"] != "department = 'engineering'" {
		t.Errorf("unexpected events filter: %s", got["events"])
	}
	if got["users"] != "region = 'us-east'" {
		t.Errorf("unexpected users filter: %s", got["users"])
	}
}

func TestRowFiltersFromContextEmpty(t *testing.T) {
	ctx := context.Background()
	got := RowFiltersFromContext(ctx)
	if got != nil {
		t.Fatal("expected nil from empty context")
	}
}

func TestProviderMiddleware(t *testing.T) {
	cfg := Config{
		Enabled: true,
		APIKeys: []APIKeyDef{
			{Key: "pm-key-123", Name: "test-user", Role: "reader"},
		},
		Roles: []RoleConfig{
			{Name: "reader", Tables: []string{"events"}, Allow: []string{"read"}},
		},
	}
	authn, authz := New(cfg)
	provider := NewProvider(authn, authz, nil, nil)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := IdentityFromContext(r.Context())
		if id != nil {
			fmt.Fprintf(w, "hello %s", id.Name)
		} else {
			fmt.Fprint(w, "no auth")
		}
	})

	wrapped := ProviderMiddleware(provider, nil)(handler)

	t.Run("health check bypasses auth", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/v1/health", nil)
		w := httptest.NewRecorder()
		wrapped.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("expected 200 for health, got %d", w.Code)
		}
	})

	t.Run("valid key passes", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/v1/query", nil)
		r.Header.Set("Authorization", "Bearer pm-key-123")
		w := httptest.NewRecorder()
		wrapped.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		if w.Body.String() != "hello test-user" {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("invalid key rejected", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/v1/query", nil)
		r.Header.Set("Authorization", "Bearer wrong-key")
		w := httptest.NewRecorder()
		wrapped.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatalf("expected 401, got %d", w.Code)
		}
	})
}

func TestProviderMiddlewareDisabled(t *testing.T) {
	provider := NewProvider(nil, nil, nil, nil)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})

	wrapped := ProviderMiddleware(provider, nil)(handler)

	// When provider is disabled, all requests pass through
	r := httptest.NewRequest("GET", "/v1/query", nil)
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("expected 200 when disabled, got %d", w.Code)
	}
}

func TestAuthenticateToken(t *testing.T) {
	cfg := testConfig()
	authn, _ := New(cfg)

	t.Run("valid API key", func(t *testing.T) {
		id, err := authn.AuthenticateToken("key-admin-123")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id.Role != "admin" {
			t.Fatalf("expected admin, got %s", id.Role)
		}
	})

	t.Run("empty token", func(t *testing.T) {
		_, err := authn.AuthenticateToken("")
		if err != ErrNoCredentials {
			t.Fatalf("expected ErrNoCredentials, got %v", err)
		}
	})

	t.Run("invalid token", func(t *testing.T) {
		_, err := authn.AuthenticateToken("invalid-token")
		if err != ErrUnauthorized {
			t.Fatalf("expected ErrUnauthorized, got %v", err)
		}
	})
}

func TestAuthenticateTokenWithJWT(t *testing.T) {
	secret := "test-secret-32bytes-long!!!!!!"
	cfg := testConfig()
	cfg.JWT = JWTConfig{
		Enabled:   true,
		Secret:    secret,
		RoleClaim: "role",
	}
	authn, _ := New(cfg)

	token := signHS256(map[string]any{
		"sub":  "user-1",
		"name": "Alice",
		"role": "reader",
		"exp":  9999999999,
	}, secret)

	id, err := authn.AuthenticateToken(token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id.Name != "Alice" {
		t.Fatalf("expected Alice, got %s", id.Name)
	}
	if id.Method != "jwt" {
		t.Fatalf("expected jwt method, got %s", id.Method)
	}
}

func TestEffectString(t *testing.T) {
	if EffectAllow.String() != "allow" {
		t.Fatalf("expected allow, got %s", EffectAllow.String())
	}
	if EffectDeny.String() != "deny" {
		t.Fatalf("expected deny, got %s", EffectDeny.String())
	}
}

func TestAttributesGet(t *testing.T) {
	a := Attributes{"key": "value", "num": 42}
	if a.Get("key") != "value" {
		t.Error("expected value for key")
	}
	if a.Get("missing") != nil {
		t.Error("expected nil for missing key")
	}

	// Nil attributes
	var nilA Attributes
	if nilA.Get("anything") != nil {
		t.Error("expected nil from nil Attributes")
	}
}

func TestIdentityToSubjectEmptyFields(t *testing.T) {
	id := &Identity{
		Attributes: Attributes{"clearance": "SECRET"},
	}
	s := id.ToSubject()
	// Empty Role/Name/Method should not be set
	if _, exists := s.Attributes["role"]; exists {
		t.Error("empty role should not be set")
	}
	if _, exists := s.Attributes["name"]; exists {
		t.Error("empty name should not be set")
	}
	if _, exists := s.Attributes["method"]; exists {
		t.Error("empty method should not be set")
	}
	if s.Attributes.GetString("clearance") != "SECRET" {
		t.Error("expected clearance attribute")
	}
}
