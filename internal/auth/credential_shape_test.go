package auth

import (
	"strings"
	"testing"
	"time"
)

// An `api_keys` entry with an empty `key:` is not a credential, and it is
// refused at load beside the one with no role.
//
// It could authenticate nobody — every door refuses an empty token before the
// lookup — but it DID satisfy the "some mechanism is configured" check, so
// `enabled: true` with only such an entry loaded with nothing usable; and
// `lookupAPIKey("")` resolves to it, one removed guard away from a bypass.
func TestAnAPIKeyWithNoKeyIsAConfigurationError(t *testing.T) {
	_, _, err := Build(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Name: "ghost", Role: "reader"}},
		Roles:   []RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	if err == nil {
		t.Fatal("an api key with an empty key built without an error")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("refusal %q does not name the entry", err)
	}
	// Whitespace is not a key either.
	if _, _, err := Build(Config{
		Enabled: true,
		APIKeys: []APIKeyDef{{Key: "   ", Name: "spaces", Role: "reader"}},
		Roles:   []RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	}); err == nil {
		t.Fatal("an api key whose key is whitespace built without an error")
	}
}

// The JWT exemption's REAL mechanism: the VERIFIER refuses a token whose role
// the configuration does not define, so there is nothing at load to check and
// nothing downstream to catch. The earlier reasoning — "it authenticates with
// no permissions and the coarse gate refuses it" — described a path that does
// not exist.
func TestAJWTNamingAnUndefinedRoleIsRefusedByTheVerifier(t *testing.T) {
	const secret = "s3cr3t-for-the-test-only-not-a-key"
	authn, _, err := Build(Config{
		Enabled: true,
		JWT:     JWTConfig{Enabled: true, Secret: secret},
		Roles:   []RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	if err != nil {
		t.Fatalf("a valid JWT configuration was refused: %v", err)
	}
	token := signHS256(map[string]any{
		"sub":  "someone",
		"role": "raeder", // not defined
		"exp":  time.Now().Add(time.Hour).Unix(),
	}, secret)
	if _, aerr := authn.AuthenticateToken(token); aerr == nil {
		t.Fatal("a token naming an undefined role authenticated")
	}
	// And one naming a defined role still works, carrying the role's grants.
	good := signHS256(map[string]any{
		"sub":  "someone",
		"role": "reader",
		"exp":  time.Now().Add(time.Hour).Unix(),
	}, secret)
	id, aerr := authn.AuthenticateToken(good)
	if aerr != nil {
		t.Fatalf("a token naming a defined role was refused: %v", aerr)
	}
	if len(id.Perms) == 0 {
		t.Fatal("the JWT identity carries no permissions")
	}
}
