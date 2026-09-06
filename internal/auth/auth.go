// Package auth provides authentication and authorization for Wadjet's HTTP API.
//
// Supports three authentication methods:
//   - API keys: Simple bearer tokens for internal tools (Grafana, ETL pipelines)
//   - JWT: HMAC-SHA256 or RSA-signed tokens for service-to-service auth
//   - mTLS: Client certificate authentication for zero-trust deployments
//
// Authorization is role-based with table-level granularity.
package auth

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
)

// Errors returned by authentication.
var (
	ErrNoCredentials = errors.New("no credentials provided")
	ErrUnauthorized  = errors.New("unauthorized")
)

// Identity represents an authenticated caller.
type Identity struct {
	Name       string     // human-readable label (e.g. "grafana-prod", "alice@example.com")
	Role       string     // resolved role name
	Method     string     // how they authenticated: "apikey", "jwt", "mtls"
	Tables     []string   // allowed tables from role (["*"] = all)
	Perms      []string   // allowed permissions from role
	Attributes Attributes // extended attributes for ABAC (from JWT claims, mTLS cert, config)
}

// Authenticator verifies caller identity from HTTP requests.
type Authenticator struct {
	apiKeys map[string]*Identity // key string -> identity
	jwt     *JWTVerifier         // nil if JWT disabled
	mtls    *MTLSVerifier        // nil if mTLS disabled
	roles   map[string]*RoleDef  // role name -> definition

	// enabled is the OPERATOR'S INTENT — `auth.enabled` as written in the
	// configuration — and not a census of which mechanisms happened to
	// build. Enabled() used to be the census alone, so a JWT verifier that
	// failed to construct reported "authentication disabled" and every door
	// read that as permission to serve without credentials (#931).
	enabled bool

	// buildErr is a configuration error this Authenticator was built from.
	// It is not a stored warning: an Authenticator carrying one is ENABLED
	// and REFUSES EVERY CREDENTIAL with it. That is the only fail-closed
	// reading of "the operator asked for authentication and it could not be
	// built" — the alternative, which shipped, was no authentication at all.
	buildErr error
}

// RoleDef is the resolved definition of a role.
type RoleDef struct {
	Name   string
	Tables []string
	Perms  []string
}

// Config holds authentication configuration.
type Config struct {
	Enabled bool         `yaml:"enabled"`
	APIKeys []APIKeyDef  `yaml:"api_keys"`
	JWT     JWTConfig    `yaml:"jwt"`
	MTLS    MTLSConfig   `yaml:"mtls"`
	Roles   []RoleConfig `yaml:"roles"`
}

// APIKeyDef defines an API key in configuration.
type APIKeyDef struct {
	Key        string            `yaml:"key"`        // the bearer token value
	Name       string            `yaml:"name"`       // human label
	Role       string            `yaml:"role"`       // role name reference
	Attributes map[string]string `yaml:"attributes"` // optional ABAC attributes
}

// RoleConfig defines a role in configuration.
type RoleConfig struct {
	Name   string   `yaml:"name"`   // e.g. "admin", "reader", "analyst"
	Tables []string `yaml:"tables"` // table names or "*" for all
	Allow  []string `yaml:"allow"`  // permissions: "read", "write", "admin"
}

// New creates an Authenticator and Authorizer from configuration.
//
// It is Build with the error folded into the Authenticator rather than
// returned: on a configuration error the Authenticator is ENABLED and refuses
// every credential (see Build). The signature is kept because it is what the
// embedded API and a long tail of tests call; a caller that can report the
// error should call Build.
func New(cfg Config) (*Authenticator, *Authorizer) {
	authn, authz, _ := Build(cfg)
	return authn, authz
}

// Build creates an Authenticator and Authorizer from configuration, reporting
// a configuration it cannot honour.
//
// A configuration error is an ERROR, never "authentication disabled" (#931):
//
//   - `jwt.enabled` with no secret and no readable public key, an unparseable
//     key file, a typo in the path — `NewJWTVerifier`'s error used to be
//     DISCARDED and the verifier left nil.
//   - `enabled: true` with no usable mechanism at all: no API keys, no JWT, no
//     mTLS. An operator who wrote `enabled: true` did not ask for an open
//     server.
//
// On error the returned Authenticator is not a disabled one. It reports
// Enabled() and refuses EVERY credential with the configuration error, so a
// caller that ignores the error — and every door reads `Provider.Enabled()`,
// not this — is closed rather than open. Startup and hot reload both report
// it: `cmd/wadjet` refuses to start, and `Provider.UpdateFromConfig` refuses
// the swap and keeps the running state.
func Build(cfg Config) (*Authenticator, *Authorizer, error) {
	roles := make(map[string]*RoleDef, len(cfg.Roles))
	for _, r := range cfg.Roles {
		roles[r.Name] = &RoleDef{
			Name:   r.Name,
			Tables: r.Tables,
			Perms:  r.Allow,
		}
	}

	authn := &Authenticator{
		apiKeys: make(map[string]*Identity, len(cfg.APIKeys)),
		roles:   roles,
		enabled: cfg.Enabled,
	}

	// Build API key lookup
	for _, ak := range cfg.APIKeys {
		role := roles[ak.Role]
		attrs := make(Attributes, len(ak.Attributes))
		for k, v := range ak.Attributes {
			attrs[k] = v
		}
		id := &Identity{
			Name:       ak.Name,
			Role:       ak.Role,
			Method:     "apikey",
			Attributes: attrs,
		}
		if role != nil {
			id.Tables = role.Tables
			id.Perms = role.Perms
		}
		authn.apiKeys[ak.Key] = id
	}

	// Initialize JWT verifier
	var buildErr error
	if cfg.JWT.Enabled {
		v, err := NewJWTVerifier(cfg.JWT, roles)
		if err != nil {
			buildErr = fmt.Errorf("jwt: %w", err)
		} else {
			authn.jwt = v
		}
	}

	// Initialize mTLS verifier
	if cfg.MTLS.Enabled {
		authn.mtls = NewMTLSVerifier(cfg.MTLS, roles)
	}

	if buildErr == nil && cfg.Enabled &&
		len(authn.apiKeys) == 0 && authn.jwt == nil && authn.mtls == nil {
		buildErr = errors.New("auth.enabled is true but no credential mechanism is " +
			"configured: give it api_keys, jwt or mtls, or set auth.enabled: false")
	}

	authz := &Authorizer{roles: roles}
	if buildErr != nil {
		// Enabled, and refusing. An operator who asked for authentication and
		// misconfigured it gets a server nobody can reach, not one everybody
		// can.
		authn.enabled = true
		authn.buildErr = buildErr
		return authn, authz, buildErr
	}
	return authn, authz, nil
}

// Enabled reports whether authentication is active.
//
// It is the operator's `auth.enabled` intent OR the presence of a mechanism —
// the second half because a configuration that lists api_keys without saying
// `enabled: true` has always been enforced, and dropping that would open every
// such deployment. What it is NOT any more is a census that a FAILED mechanism
// can turn off: an Authenticator built from a broken configuration reports
// enabled and refuses everything (#931).
func (a *Authenticator) Enabled() bool {
	if a == nil {
		return false
	}
	return a.enabled || len(a.apiKeys) > 0 || a.jwt != nil || a.mtls != nil
}

// ConfigError is the configuration error this Authenticator was built from, or
// nil. Non-nil means every credential is refused.
func (a *Authenticator) ConfigError() error {
	if a == nil {
		return nil
	}
	return a.buildErr
}

// Authenticate extracts and verifies identity from an HTTP request.
// It tries methods in order: mTLS (from TLS state), API key (Bearer token), JWT (Bearer token).
func (a *Authenticator) Authenticate(r *http.Request) (*Identity, error) {
	if a.buildErr != nil {
		return nil, a.configRefusal()
	}
	// 1. Try mTLS — check TLS peer certificates
	if a.mtls != nil && r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		id, err := a.mtls.Verify(r.TLS.PeerCertificates[0])
		if err == nil {
			return id, nil
		}
		// mTLS failed — fall through to other methods
	}

	// 2. Extract Bearer token from Authorization header
	token := extractBearerToken(r)
	if token == "" {
		// No token and no mTLS — unauthorized
		return nil, ErrNoCredentials
	}

	// 3. Try API key (constant-time lookup)
	if id := a.lookupAPIKey(token); id != nil {
		return id, nil
	}

	// 4. Try JWT
	if a.jwt != nil {
		id, err := a.jwt.Verify(token)
		if err == nil {
			return id, nil
		}
	}

	return nil, ErrUnauthorized
}

// AuthenticateToken verifies identity from a raw bearer token (API key or JWT).
// Used by non-HTTP frontends (pgwire, gRPC) where there is no http.Request.
func (a *Authenticator) AuthenticateToken(token string) (*Identity, error) {
	if a.buildErr != nil {
		return nil, a.configRefusal()
	}
	if token == "" {
		return nil, ErrNoCredentials
	}
	if id := a.lookupAPIKey(token); id != nil {
		return id, nil
	}
	if a.jwt != nil {
		id, err := a.jwt.Verify(token)
		if err == nil {
			return id, nil
		}
	}
	return nil, ErrUnauthorized
}

// configRefusal is what a broken authentication configuration answers every
// credential with. It is ErrUnauthorized so every door renders it in its own
// authentication class (HTTP 401, pgwire 28000, gRPC Unauthenticated).
func (a *Authenticator) configRefusal() error {
	return fmt.Errorf("%w: the authentication configuration could not be built: %v",
		ErrUnauthorized, a.buildErr)
}

func (a *Authenticator) lookupAPIKey(token string) *Identity {
	for key, id := range a.apiKeys {
		if subtle.ConstantTimeCompare([]byte(key), []byte(token)) == 1 {
			return id
		}
	}
	return nil
}

func extractBearerToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if len(auth) > 7 && auth[:7] == "Bearer " {
		return auth[7:]
	}
	return ""
}

// String returns a human-readable representation of the identity.
func (id *Identity) String() string {
	return fmt.Sprintf("%s (role=%s, method=%s)", id.Name, id.Role, id.Method)
}
