package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
)

// The startup half of #931: a configuration error is an error, and the server
// does not start. `buildAuth` used to swallow it and hand back an
// Authenticator reporting DISABLED, which every frontend reads as permission
// to serve without credentials.

func brokenJWTAuthBlock() config.Auth {
	return config.Auth{
		Enabled: true,
		JWT:     config.AuthJWT{Enabled: true}, // no secret, no public key file
		Roles:   []config.AuthRole{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	}
}

func TestBuildAuthReportsABrokenJWTConfig(t *testing.T) {
	authn, _, err := buildAuth(brokenJWTAuthBlock())
	if err == nil {
		t.Fatal("buildAuth accepted a JWT config with no secret and no key")
	}
	if !authn.Enabled() {
		t.Fatal("the authenticator reported DISABLED, which every door reads as open")
	}
}

func TestBuildAuthReportsAMissingJWTKeyFile(t *testing.T) {
	cfg := brokenJWTAuthBlock()
	cfg.JWT.PublicKeyFile = filepath.Join(t.TempDir(), "absent.pem")
	if _, _, err := buildAuth(cfg); err == nil {
		t.Fatal("buildAuth accepted an unreadable JWT public key file")
	}
}

func TestBuildAuthReportsEnabledWithNoMechanism(t *testing.T) {
	if _, _, err := buildAuth(config.Auth{
		Enabled: true,
		Roles:   []config.AuthRole{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	}); err == nil {
		t.Fatal("buildAuth accepted enabled: true with no api_keys, jwt or mtls")
	}
}

// The provider construction every serve mode and the MCP command go through
// refuses too, so the process exits instead of serving.
func TestBuildProviderFromConfigRefusesABrokenAuthBlock(t *testing.T) {
	_, err := buildProviderFromConfig(&config.Config{Auth: brokenJWTAuthBlock()}, nil)
	if err == nil {
		t.Fatal("the server would have started with a broken authentication configuration")
	}
	if !strings.Contains(err.Error(), "auth configuration") {
		t.Fatalf("startup refusal %q does not name the auth configuration", err)
	}
}

// A working configuration still builds — the refusal is for configurations
// that cannot be honoured, not for every one.
func TestBuildProviderFromConfigAcceptsAWorkingAuthBlock(t *testing.T) {
	p, err := buildProviderFromConfig(&config.Config{Auth: config.Auth{
		Enabled: true,
		APIKeys: []config.AuthAPIKey{{Key: "k", Name: "reader", Role: "reader"}},
		Roles:   []config.AuthRole{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	}}, nil)
	if err != nil {
		t.Fatalf("a working auth block was refused: %v", err)
	}
	if !p.Enabled() {
		t.Fatal("a working auth block produced a disabled provider")
	}
	if _, aerr := p.Authenticator().AuthenticateToken("k"); aerr != nil {
		t.Fatalf("the API key does not authenticate: %v", aerr)
	}
}

// A config with no auth block at all is the dev/embedded shape and is
// untouched: nothing is enforced and nothing refuses.
func TestBuildProviderFromConfigLeavesAnUnauthenticatedDeploymentAlone(t *testing.T) {
	p, err := buildProviderFromConfig(&config.Config{}, nil)
	if err != nil {
		t.Fatalf("a config with no auth block was refused: %v", err)
	}
	if p.Enabled() {
		t.Fatal("a config with no auth block turned authentication on")
	}
}

// The MCP door reads the same `Provider.Enabled()` the other three do, so a
// broken configuration must not let it serve unauthenticated.
func TestMCPRefusesToServeUnderABrokenAuthConfig(t *testing.T) {
	authn, authz, err := auth.Build(auth.Config{
		Enabled: true,
		JWT:     auth.JWTConfig{Enabled: true},
		Roles:   []auth.RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	if err == nil {
		t.Fatal("test setup: the config should not build")
	}
	p := auth.NewProvider(authn, authz, nil, nil)

	// No credential: refused, rather than "auth disabled, serve everything".
	if _, merr := resolveMCPAuth(p, ""); merr == nil {
		t.Fatal("MCP would have served with no credential under a broken auth config")
	}
	// A credential: also refused — the failure is the configuration.
	if _, merr := resolveMCPAuth(p, "any-token"); merr == nil {
		t.Fatal("MCP accepted a credential under a broken auth config")
	}
}
