// Package auth provides authentication and authorization for Caelum's HTTP API.
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
	Name   string   // human-readable label (e.g. "grafana-prod", "alice@example.com")
	Role   string   // resolved role name
	Method string   // how they authenticated: "apikey", "jwt", "mtls"
	Tables []string // allowed tables from role (["*"] = all)
	Perms  []string // allowed permissions from role
}

// Authenticator verifies caller identity from HTTP requests.
type Authenticator struct {
	apiKeys map[string]*Identity // key string -> identity
	jwt     *JWTVerifier         // nil if JWT disabled
	mtls    *MTLSVerifier        // nil if mTLS disabled
	roles   map[string]*RoleDef  // role name -> definition
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
	Key  string `yaml:"key"`  // the bearer token value
	Name string `yaml:"name"` // human label
	Role string `yaml:"role"` // role name reference
}

// RoleConfig defines a role in configuration.
type RoleConfig struct {
	Name   string   `yaml:"name"`   // e.g. "admin", "reader", "analyst"
	Tables []string `yaml:"tables"` // table names or "*" for all
	Allow  []string `yaml:"allow"`  // permissions: "read", "write", "admin"
}

// New creates an Authenticator and Authorizer from configuration.
func New(cfg Config) (*Authenticator, *Authorizer) {
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
	}

	// Build API key lookup
	for _, ak := range cfg.APIKeys {
		role := roles[ak.Role]
		id := &Identity{
			Name:   ak.Name,
			Role:   ak.Role,
			Method: "apikey",
		}
		if role != nil {
			id.Tables = role.Tables
			id.Perms = role.Perms
		}
		authn.apiKeys[ak.Key] = id
	}

	// Initialize JWT verifier
	if cfg.JWT.Enabled {
		v, err := NewJWTVerifier(cfg.JWT, roles)
		if err == nil {
			authn.jwt = v
		}
	}

	// Initialize mTLS verifier
	if cfg.MTLS.Enabled {
		authn.mtls = NewMTLSVerifier(cfg.MTLS, roles)
	}

	authz := &Authorizer{roles: roles}
	return authn, authz
}

// Enabled returns true if authentication is configured.
func (a *Authenticator) Enabled() bool {
	return a != nil && (len(a.apiKeys) > 0 || a.jwt != nil || a.mtls != nil)
}

// Authenticate extracts and verifies identity from an HTTP request.
// It tries methods in order: mTLS (from TLS state), API key (Bearer token), JWT (Bearer token).
func (a *Authenticator) Authenticate(r *http.Request) (*Identity, error) {
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
