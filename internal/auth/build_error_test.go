package auth

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

// brokenJWTConfig is the issue's configuration: the operator asked for
// authentication and named a JWT mechanism that cannot be built.
func brokenJWTConfig() Config {
	return Config{
		Enabled: true,
		JWT:     JWTConfig{Enabled: true}, // missing secret / public key
		Roles:   []RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	}
}

// TestBrokenAuthConfigDoesNotDisableAuthentication is the issue's HTTP
// reproducer (#931). A JWT verifier that fails to build used to leave
// `Enabled()` false, and every door reads that as permission to serve a
// request with no credentials at all.
func TestBrokenAuthConfigDoesNotDisableAuthentication(t *testing.T) {
	authn, authz, err := Build(brokenJWTConfig())
	if err == nil {
		t.Fatal("a JWT config with no secret and no key built without an error")
	}
	if !authn.Enabled() {
		t.Fatal("a broken authentication configuration reported DISABLED")
	}
	p := NewProvider(authn, authz, nil, nil)
	if !p.Enabled() {
		t.Fatal("the provider reported disabled for a broken configuration")
	}

	called := false
	h := ProviderMiddleware(p, nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/query", nil))
	if called {
		t.Fatal("unauthenticated request reached the handler")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

// A credential that WOULD be valid under a working configuration is refused
// too: the failure is the configuration, not the token.
func TestABrokenConfigRefusesEveryCredential(t *testing.T) {
	cfg := brokenJWTConfig()
	cfg.APIKeys = nil
	authn, _, _ := Build(cfg)
	if _, err := authn.AuthenticateToken("anything"); err == nil {
		t.Fatal("a token was accepted under a broken configuration")
	} else if !strings.Contains(err.Error(), "could not be built") {
		t.Fatalf("refusal %q does not say the configuration is broken", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/query", nil)
	req.Header.Set("Authorization", "Bearer anything")
	if _, err := authn.Authenticate(req); err == nil {
		t.Fatal("an HTTP credential was accepted under a broken configuration")
	}
}

// An unreadable / missing JWT public key file is the same class.
func TestAnUnreadableJWTKeyFileIsAConfigurationError(t *testing.T) {
	_, _, err := Build(Config{
		Enabled: true,
		JWT:     JWTConfig{Enabled: true, PublicKeyFile: filepath.Join(t.TempDir(), "nope.pem")},
	})
	if err == nil {
		t.Fatal("a missing JWT public key file built without an error")
	}
	if !strings.Contains(err.Error(), "jwt") {
		t.Fatalf("error %q does not name the mechanism that failed", err)
	}
}

// `enabled: true` with roles but NO credential mechanism is an error: an
// operator who wrote it did not ask for an open server.
func TestEnabledWithNoMechanismIsAConfigurationError(t *testing.T) {
	authn, _, err := Build(Config{
		Enabled: true,
		Roles:   []RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	if err == nil {
		t.Fatal("enabled: true with no api_keys, jwt or mtls built without an error")
	}
	if !authn.Enabled() {
		t.Fatal("it reported DISABLED, which every door reads as open")
	}
}

// The shapes that must keep working, exactly as before: a config with a
// mechanism, and a config with auth off.
func TestAWorkingConfigIsUnchanged(t *testing.T) {
	authn, authz, err := Build(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "reader", Role: "reader"}},
		Roles:   []RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	if err != nil {
		t.Fatalf("a working config was refused: %v", err)
	}
	if !authn.Enabled() {
		t.Fatal("a working config reported disabled")
	}
	id, aerr := authn.AuthenticateToken("k")
	if aerr != nil {
		t.Fatalf("a valid API key was refused: %v", aerr)
	}
	if !authz.HasPermission(id, "read") {
		t.Fatal("the identity lost its permissions")
	}

	// api_keys WITHOUT `enabled: true` has always been enforced; it still is.
	implicit, _, err := Build(Config{APIKeys: []APIKeyDef{{Key: "k", Name: "r", Role: "reader"}}})
	if err != nil {
		t.Fatalf("an implicit-enable config was refused: %v", err)
	}
	if !implicit.Enabled() {
		t.Fatal("api_keys without enabled: true stopped being enforced")
	}

	// Auth genuinely off stays off — the embedded and dev shape.
	off, _, err := Build(Config{})
	if err != nil {
		t.Fatalf("an empty (auth off) config was refused: %v", err)
	}
	if off.Enabled() {
		t.Fatal("an empty config turned authentication ON")
	}
}

// TestHotReloadOfABrokenAuthConfigKeepsTheRunningState is the issue's second
// reproducer: `UpdateFromConfig` returned nil and INSTALLED the disabled
// state over a working authenticator.
func TestHotReloadOfABrokenAuthConfigKeepsTheRunningState(t *testing.T) {
	working := Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "good-key", Name: "reader", Role: "reader"}},
		Roles:   []RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	}
	authn, authz, err := Build(working)
	if err != nil {
		t.Fatal(err)
	}
	p := NewProvider(authn, authz, nil, nil)
	before := p.Authenticator()

	if err := p.UpdateFromConfig(brokenJWTConfig(), nil); err == nil {
		t.Fatal("a broken auth config was installed by hot reload")
	}
	if !p.Enabled() {
		t.Fatal("a bad hot reload disabled authentication")
	}
	if p.Authenticator() != before {
		t.Fatal("the refused configuration replaced the running authenticator")
	}
	// The credential that worked before the reload still works.
	if _, aerr := p.Authenticator().AuthenticateToken("good-key"); aerr != nil {
		t.Fatalf("the surviving authenticator stopped accepting its API key: %v", aerr)
	}
	// And an unauthenticated request is still refused.
	called := false
	h := ProviderMiddleware(p, nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/query", nil))
	if called {
		t.Fatal("an unauthenticated request reached the handler after the refused reload")
	}
}

// A hot reload to a DIFFERENT working configuration still swaps — the refusal
// is for configurations that cannot be built, not for every reload.
func TestHotReloadToAWorkingConfigStillSwaps(t *testing.T) {
	first := Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "old-key", Name: "reader", Role: "reader"}},
		Roles:   []RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	}
	authn, authz, err := Build(first)
	if err != nil {
		t.Fatal(err)
	}
	p := NewProvider(authn, authz, nil, nil)

	second := first
	second.APIKeys = []APIKeyDef{{Key: "new-key", Name: "reader", Role: "reader"}}
	if err := p.UpdateFromConfig(second, nil); err != nil {
		t.Fatalf("a working reload was refused: %v", err)
	}
	if _, aerr := p.Authenticator().AuthenticateToken("new-key"); aerr != nil {
		t.Fatalf("the new key does not work after the reload: %v", aerr)
	}
	if _, aerr := p.Authenticator().AuthenticateToken("old-key"); aerr == nil {
		t.Fatal("the retired key still works after the reload")
	}
}
