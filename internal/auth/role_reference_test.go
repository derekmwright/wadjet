package auth

import (
	"strings"
	"testing"
)

// A credential that names a role the configuration does not define
// authenticates and then holds NO permission at all: an empty `Perms` and an
// empty `Tables`, which every door's coarse gate refuses. That is the right
// runtime answer and a terrible thing to find in production — and on base it
// was worse, because the data path consulted no role when no ABAC evaluator
// was installed, so exactly this credential could read every table while the
// metadata doors refused it. A configuration that cannot mean what the
// operator wrote refuses at LOAD.

func TestAnAPIKeyNamingAnUndefinedRoleIsAConfigurationError(t *testing.T) {
	_, _, err := Build(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "ingest-pipeline", Role: "writter"}},
		Roles:   []RoleConfig{{Name: "writer", Tables: []string{"*"}, Allow: []string{"write"}}},
	})
	if err == nil {
		t.Fatal("an api key naming an undefined role built without an error")
	}
	if !strings.Contains(err.Error(), "ingest-pipeline") || !strings.Contains(err.Error(), "writter") {
		t.Fatalf("refusal %q names neither the key nor the role", err)
	}
}

func TestAnAPIKeyWithNoRoleIsAConfigurationError(t *testing.T) {
	_, _, err := Build(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "k", Name: "anonymous"}},
		Roles:   []RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	if err == nil {
		t.Fatal("an api key with no role built without an error")
	}
	if !strings.Contains(err.Error(), "anonymous") {
		t.Fatalf("refusal %q does not name the key", err)
	}
}

func TestCredentialsWithNoRolesAtAllAreAConfigurationError(t *testing.T) {
	authn, _, err := Build(Config{
		Enabled: true,
		JWT:     JWTConfig{Enabled: true, Secret: "s3cr3t-for-the-test-only-not-a-key"},
	})
	if err == nil {
		t.Fatal("enabled: true with a credential mechanism and no roles built without an error")
	}
	if !strings.Contains(err.Error(), "no roles are defined") {
		t.Fatalf("refusal %q does not say what is missing", err)
	}
	// Fail closed, as every configuration error does.
	if !authn.Enabled() {
		t.Fatal("it reported DISABLED, which every door reads as open")
	}
	if _, aerr := authn.AuthenticateToken("anything"); aerr == nil {
		t.Fatal("a credential was accepted under a configuration with no roles")
	}
}

// mTLS maps certificate subjects to roles in CONFIGURATION, so those names can
// be checked at load — unlike a JWT, whose role arrives in the token's claim.
func TestMTLSRoleMappingsMustNameDefinedRoles(t *testing.T) {
	base := func() Config {
		return Config{
			Enabled: true,
			Roles:   []RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
			MTLS: MTLSConfig{
				Enabled: true,
				RoleMap: map[string]string{"CN=grafana": "reader"},
			},
		}
	}
	if _, _, err := Build(base()); err != nil {
		t.Fatalf("a valid mtls mapping was refused: %v", err)
	}

	bad := base()
	bad.MTLS.RoleMap = map[string]string{"CN=grafana": "raeder"}
	err := Build2Err(bad)
	if err == nil {
		t.Fatal("an mtls role_map naming an undefined role built without an error")
	}
	if !strings.Contains(err.Error(), "raeder") || !strings.Contains(err.Error(), "CN=grafana") {
		t.Fatalf("refusal %q names neither the subject nor the role", err)
	}

	badDefault := base()
	badDefault.MTLS.DefaultRole = "nobody"
	if err := Build2Err(badDefault); err == nil {
		t.Fatal("an mtls default_role naming an undefined role built without an error")
	}
}

// Build2Err is Build's error alone, for the table-shaped cells above.
func Build2Err(cfg Config) error {
	_, _, err := Build(cfg)
	return err
}

// A hot reload carrying one of these refuses and swaps nothing.
func TestAHotReloadNamingAnUndefinedRoleKeepsTheRunningState(t *testing.T) {
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

	broken := working
	broken.APIKeys = []APIKeyDef{{Key: "new-key", Name: "reader", Role: "raeder"}}
	if err := p.UpdateFromConfig(broken, nil); err == nil {
		t.Fatal("a reload naming an undefined role was installed")
	}
	if p.Authenticator() != before {
		t.Fatal("the refused reload replaced the running authenticator")
	}
	if _, aerr := p.Authenticator().AuthenticateToken("good-key"); aerr != nil {
		t.Fatalf("the surviving authenticator stopped accepting its key: %v", aerr)
	}
}

// The configurations that must keep loading: every credential names a role
// that exists, and auth-off is untouched.
func TestValidRoleReferencesStillLoad(t *testing.T) {
	if _, _, err := Build(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{
			{Key: "a", Name: "ingest", Role: "writer"},
			{Key: "b", Name: "dash", Role: "reader"},
		},
		JWT: JWTConfig{Enabled: true, Secret: "s3cr3t-for-the-test-only-not-a-key"},
		MTLS: MTLSConfig{Enabled: true,
			RoleMap:     map[string]string{"CN=ingest": "writer"},
			DefaultRole: "reader"},
		Roles: []RoleConfig{
			{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "writer", Tables: []string{"*"}, Allow: []string{"read", "write"}},
		},
	}); err != nil {
		t.Fatalf("a fully valid configuration was refused: %v", err)
	}
	// Auth off, with nothing configured: untouched.
	if _, _, err := Build(Config{}); err != nil {
		t.Fatalf("an auth-off configuration was refused: %v", err)
	}
}
