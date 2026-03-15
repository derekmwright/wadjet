package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		Enabled: true,
		APIKeys: []APIKeyDef{
			{Key: "key-admin-123", Name: "admin-tool", Role: "admin"},
			{Key: "key-reader-456", Name: "grafana", Role: "reader"},
			{Key: "key-analyst-789", Name: "jupyter", Role: "analyst"},
		},
		Roles: []RoleConfig{
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
			{Name: "reader", Tables: []string{"events", "metrics"}, Allow: []string{"read"}},
			{Name: "analyst", Tables: []string{"events"}, Allow: []string{"read"}},
		},
	}
}

// --- API Key Authentication ---

func TestAPIKeyAuth(t *testing.T) {
	authn, _ := New(testConfig())

	tests := []struct {
		name    string
		key     string
		wantErr bool
		role    string
	}{
		{"valid admin key", "key-admin-123", false, "admin"},
		{"valid reader key", "key-reader-456", false, "reader"},
		{"invalid key", "key-invalid", true, ""},
		{"empty key", "", true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/v1/tables", nil)
			if tt.key != "" {
				r.Header.Set("Authorization", "Bearer "+tt.key)
			}

			id, err := authn.Authenticate(r)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if id.Role != tt.role {
				t.Fatalf("expected role %q, got %q", tt.role, id.Role)
			}
			if id.Method != "apikey" {
				t.Fatalf("expected method apikey, got %q", id.Method)
			}
		})
	}
}

func TestAPIKeyIdentityFields(t *testing.T) {
	authn, _ := New(testConfig())

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer key-reader-456")

	id, err := authn.Authenticate(r)
	if err != nil {
		t.Fatal(err)
	}

	if id.Name != "grafana" {
		t.Fatalf("expected name=grafana, got %q", id.Name)
	}
	if len(id.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(id.Tables))
	}
	if len(id.Perms) != 1 || id.Perms[0] != "read" {
		t.Fatalf("expected perms=[read], got %v", id.Perms)
	}
}

// --- JWT Authentication ---

func signHS256(claims map[string]any, secret string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claimsJSON, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(claimsJSON)
	signingInput := header + "." + payload
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig
}

func TestJWTAuth(t *testing.T) {
	secret := "test-secret-key-32bytes-long!!"
	cfg := testConfig()
	cfg.JWT = JWTConfig{
		Enabled:   true,
		Secret:    secret,
		RoleClaim: "role",
	}
	authn, _ := New(cfg)

	t.Run("valid token", func(t *testing.T) {
		token := signHS256(map[string]any{
			"sub":  "user-1",
			"name": "Alice",
			"role": "reader",
			"exp":  time.Now().Add(time.Hour).Unix(),
		}, secret)

		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+token)

		id, err := authn.Authenticate(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id.Name != "Alice" {
			t.Fatalf("expected name=Alice, got %q", id.Name)
		}
		if id.Role != "reader" {
			t.Fatalf("expected role=reader, got %q", id.Role)
		}
		if id.Method != "jwt" {
			t.Fatalf("expected method=jwt, got %q", id.Method)
		}
	})

	t.Run("expired token", func(t *testing.T) {
		token := signHS256(map[string]any{
			"sub":  "user-1",
			"role": "reader",
			"exp":  time.Now().Add(-time.Hour).Unix(),
		}, secret)

		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+token)

		_, err := authn.Authenticate(r)
		if err == nil {
			t.Fatal("expected error for expired token")
		}
	})

	t.Run("wrong signature", func(t *testing.T) {
		token := signHS256(map[string]any{
			"sub":  "user-1",
			"role": "reader",
			"exp":  time.Now().Add(time.Hour).Unix(),
		}, "wrong-secret")

		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+token)

		_, err := authn.Authenticate(r)
		if err == nil {
			t.Fatal("expected error for wrong signature")
		}
	})

	t.Run("unknown role in token", func(t *testing.T) {
		token := signHS256(map[string]any{
			"sub":  "user-1",
			"role": "superuser",
			"exp":  time.Now().Add(time.Hour).Unix(),
		}, secret)

		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+token)

		_, err := authn.Authenticate(r)
		if err == nil {
			t.Fatal("expected error for unknown role")
		}
	})

	t.Run("missing role claim", func(t *testing.T) {
		token := signHS256(map[string]any{
			"sub": "user-1",
			"exp": time.Now().Add(time.Hour).Unix(),
		}, secret)

		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+token)

		_, err := authn.Authenticate(r)
		if err == nil {
			t.Fatal("expected error for missing role")
		}
	})

	t.Run("custom role claim", func(t *testing.T) {
		cfg2 := testConfig()
		cfg2.JWT = JWTConfig{
			Enabled:   true,
			Secret:    secret,
			RoleClaim: "caelum_role",
		}
		authn2, _ := New(cfg2)

		token := signHS256(map[string]any{
			"sub":         "user-1",
			"name":        "Bob",
			"caelum_role": "admin",
			"exp":         time.Now().Add(time.Hour).Unix(),
		}, secret)

		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+token)

		id, err := authn2.Authenticate(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id.Role != "admin" {
			t.Fatalf("expected role=admin, got %q", id.Role)
		}
	})

	t.Run("issuer validation", func(t *testing.T) {
		cfg2 := testConfig()
		cfg2.JWT = JWTConfig{
			Enabled: true,
			Secret:  secret,
			Issuer:  "caelum-auth",
		}
		authn2, _ := New(cfg2)

		// Correct issuer
		token := signHS256(map[string]any{
			"sub":  "user-1",
			"role": "reader",
			"iss":  "caelum-auth",
			"exp":  time.Now().Add(time.Hour).Unix(),
		}, secret)

		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+token)

		_, err := authn2.Authenticate(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Wrong issuer
		token2 := signHS256(map[string]any{
			"sub":  "user-1",
			"role": "reader",
			"iss":  "other-service",
			"exp":  time.Now().Add(time.Hour).Unix(),
		}, secret)

		r2 := httptest.NewRequest("GET", "/", nil)
		r2.Header.Set("Authorization", "Bearer "+token2)

		_, err = authn2.Authenticate(r2)
		if err == nil {
			t.Fatal("expected error for wrong issuer")
		}
	})
}

func TestJWTRSA256(t *testing.T) {
	// Generate RSA key pair
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	// Write public key to temp file
	dir := t.TempDir()
	pubKeyPath := filepath.Join(dir, "public.pem")
	pubBytes, err := x509.MarshalPKIXPublicKey(&privKey.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubBytes})
	if err := os.WriteFile(pubKeyPath, pubPEM, 0644); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	cfg.JWT = JWTConfig{
		Enabled:       true,
		PublicKeyFile: pubKeyPath,
	}
	authn, _ := New(cfg)

	// Sign a token with RSA
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]any{
		"sub":  "service-1",
		"name": "ETL Pipeline",
		"role": "admin",
		"exp":  time.Now().Add(time.Hour).Unix(),
	})
	payload := base64.RawURLEncoding.EncodeToString(claims)
	signingInput := header + "." + payload

	hash := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, privKey, 0x05, hash[:]) // crypto.SHA256 = 0x05
	if err != nil {
		t.Fatal(err)
	}
	token := signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)

	id, err := authn.Authenticate(r)
	if err != nil {
		t.Fatalf("RSA256 auth failed: %v", err)
	}
	if id.Name != "ETL Pipeline" {
		t.Fatalf("expected name='ETL Pipeline', got %q", id.Name)
	}
	if id.Method != "jwt" {
		t.Fatalf("expected method=jwt, got %q", id.Method)
	}
}

// --- mTLS Authentication ---

func TestMTLSAuth(t *testing.T) {
	cfg := testConfig()
	cfg.MTLS = MTLSConfig{
		Enabled: true,
		RoleMap: map[string]string{
			"grafana.internal":    "reader",
			"etl.internal":       "admin",
			"alice@example.com":  "analyst",
		},
		DefaultRole: "reader",
	}
	authn, _ := New(cfg)

	t.Run("CN match", func(t *testing.T) {
		cert := &x509.Certificate{
			Subject: pkix.Name{CommonName: "etl.internal"},
		}
		id, err := authn.mtls.Verify(cert)
		if err != nil {
			t.Fatal(err)
		}
		if id.Role != "admin" {
			t.Fatalf("expected role=admin, got %q", id.Role)
		}
		if id.Method != "mtls" {
			t.Fatalf("expected method=mtls, got %q", id.Method)
		}
	})

	t.Run("SAN DNS match", func(t *testing.T) {
		cert := &x509.Certificate{
			Subject:  pkix.Name{CommonName: "unknown-cn"},
			DNSNames: []string{"grafana.internal"},
		}
		id, err := authn.mtls.Verify(cert)
		if err != nil {
			t.Fatal(err)
		}
		if id.Role != "reader" {
			t.Fatalf("expected role=reader, got %q", id.Role)
		}
	})

	t.Run("SAN email match", func(t *testing.T) {
		cert := &x509.Certificate{
			Subject:        pkix.Name{CommonName: "unknown-cn"},
			EmailAddresses: []string{"alice@example.com"},
		}
		id, err := authn.mtls.Verify(cert)
		if err != nil {
			t.Fatal(err)
		}
		if id.Role != "analyst" {
			t.Fatalf("expected role=analyst, got %q", id.Role)
		}
	})

	t.Run("default role fallback", func(t *testing.T) {
		cert := &x509.Certificate{
			Subject: pkix.Name{CommonName: "unknown-service"},
		}
		id, err := authn.mtls.Verify(cert)
		if err != nil {
			t.Fatal(err)
		}
		if id.Role != "reader" {
			t.Fatalf("expected default role=reader, got %q", id.Role)
		}
	})

	t.Run("no match no default", func(t *testing.T) {
		cfg2 := testConfig()
		cfg2.MTLS = MTLSConfig{
			Enabled: true,
			RoleMap: map[string]string{"specific.host": "admin"},
			// no default role
		}
		_, authz := New(cfg2)
		_ = authz
		authn2, _ := New(cfg2)

		cert := &x509.Certificate{
			Subject: pkix.Name{CommonName: "unknown"},
		}
		_, err := authn2.mtls.Verify(cert)
		if err == nil {
			t.Fatal("expected error when no role mapping and no default")
		}
	})
}

// --- Authorizer ---

func TestAuthorizer(t *testing.T) {
	_, authz := New(testConfig())

	admin := &Identity{Role: "admin", Tables: []string{"*"}, Perms: []string{"read", "write", "admin"}}
	reader := &Identity{Role: "reader", Tables: []string{"events", "metrics"}, Perms: []string{"read"}}
	analyst := &Identity{Role: "analyst", Tables: []string{"events"}, Perms: []string{"read"}}

	t.Run("admin has all permissions", func(t *testing.T) {
		for _, perm := range []string{"read", "write", "admin"} {
			if !authz.HasPermission(admin, perm) {
				t.Fatalf("admin should have %s permission", perm)
			}
		}
	})

	t.Run("reader has only read", func(t *testing.T) {
		if !authz.HasPermission(reader, "read") {
			t.Fatal("reader should have read permission")
		}
		if authz.HasPermission(reader, "write") {
			t.Fatal("reader should NOT have write permission")
		}
	})

	t.Run("admin accesses all tables", func(t *testing.T) {
		for _, table := range []string{"events", "users", "anything"} {
			if !authz.CanAccessTable(admin, table) {
				t.Fatalf("admin should access table %s", table)
			}
		}
	})

	t.Run("reader accesses only allowed tables", func(t *testing.T) {
		if !authz.CanAccessTable(reader, "events") {
			t.Fatal("reader should access events")
		}
		if authz.CanAccessTable(reader, "users") {
			t.Fatal("reader should NOT access users")
		}
	})

	t.Run("filter tables", func(t *testing.T) {
		allTables := []string{"events", "metrics", "users", "logs"}

		adminFiltered := authz.FilterTables(admin, allTables)
		if len(adminFiltered) != 4 {
			t.Fatalf("admin should see all 4 tables, got %d", len(adminFiltered))
		}

		readerFiltered := authz.FilterTables(reader, allTables)
		if len(readerFiltered) != 2 {
			t.Fatalf("reader should see 2 tables, got %d: %v", len(readerFiltered), readerFiltered)
		}

		analystFiltered := authz.FilterTables(analyst, allTables)
		if len(analystFiltered) != 1 || analystFiltered[0] != "events" {
			t.Fatalf("analyst should see [events], got %v", analystFiltered)
		}
	})

	t.Run("nil identity denied", func(t *testing.T) {
		if authz.HasPermission(nil, "read") {
			t.Fatal("nil identity should be denied")
		}
		if authz.CanAccessTable(nil, "events") {
			t.Fatal("nil identity should not access tables")
		}
	})
}

// --- Cell-Level Access Policies ---

func TestAccessPolicyMask(t *testing.T) {
	policy := &AccessPolicy{
		Table: "users",
		Role:  "analyst",
		Columns: map[string]ColumnPolicy{
			"name":  ColumnAllow,
			"email": ColumnMask,
			"ssn":   ColumnDeny,
		},
	}

	row := map[string]any{
		"name":  "Alice Smith",
		"email": "alice@example.com",
		"ssn":   "123-45-6789",
		"age":   int64(30), // no policy — allowed by default
	}

	result := policy.ApplyToRow(row)

	if result["name"] != "Alice Smith" {
		t.Fatalf("expected name=Alice Smith, got %v", result["name"])
	}
	if result["email"] != "***" {
		t.Fatalf("expected masked email=***, got %v", result["email"])
	}
	if _, exists := result["ssn"]; exists {
		t.Fatal("ssn should be denied/omitted")
	}
	if result["age"] != int64(30) {
		t.Fatalf("age should be allowed by default, got %v", result["age"])
	}
}

func TestAccessPolicyMaskTypes(t *testing.T) {
	policy := &AccessPolicy{
		Columns: map[string]ColumnPolicy{
			"name":   ColumnMask,
			"score":  ColumnMask,
			"count":  ColumnMask,
			"active": ColumnMask,
		},
	}

	row := map[string]any{
		"name":   "Bob",
		"score":  99.5,
		"count":  int64(42),
		"active": true,
	}

	result := policy.ApplyToRow(row)

	if result["name"] != "***" {
		t.Fatalf("expected masked string, got %v", result["name"])
	}
	if result["score"] != float64(0) {
		t.Fatalf("expected masked float=0, got %v", result["score"])
	}
	if result["count"] != int64(0) {
		t.Fatalf("expected masked int=0, got %v", result["count"])
	}
	if result["active"] != false {
		t.Fatalf("expected masked bool=false, got %v", result["active"])
	}
}

func TestAccessPolicyCustomMask(t *testing.T) {
	policy := &AccessPolicy{
		Columns: map[string]ColumnPolicy{
			"email": ColumnMask,
		},
		MaskValues: map[string]CellMaskFunc{
			"email": func(val any) any {
				s, ok := val.(string)
				if !ok {
					return "***"
				}
				parts := strings.SplitN(s, "@", 2)
				if len(parts) != 2 {
					return "***"
				}
				return "***@" + parts[1]
			},
		},
	}

	row := map[string]any{"email": "alice@example.com"}
	result := policy.ApplyToRow(row)

	if result["email"] != "***@example.com" {
		t.Fatalf("expected ***@example.com, got %v", result["email"])
	}
}

func TestAccessPolicyApplyToRows(t *testing.T) {
	policy := &AccessPolicy{
		Columns: map[string]ColumnPolicy{
			"ssn": ColumnDeny,
		},
	}

	rows := []map[string]any{
		{"name": "Alice", "ssn": "111"},
		{"name": "Bob", "ssn": "222"},
	}

	result := policy.ApplyToRows(rows)
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
	for _, row := range result {
		if _, exists := row["ssn"]; exists {
			t.Fatal("ssn should be denied")
		}
		if row["name"] == nil {
			t.Fatal("name should be present")
		}
	}
}

func TestParsePolicies(t *testing.T) {
	configs := []PolicyConfig{
		{
			Table: "users",
			Role:  "analyst",
			Columns: map[string]string{
				"name":  "allow",
				"email": "mask",
				"ssn":   "deny",
			},
			RowFilter: "department = 'engineering'",
		},
		{
			Table: "events",
			Role:  "reader",
			Columns: map[string]string{
				"user_id": "mask",
			},
		},
	}

	ps := ParsePolicies(configs)

	p := ps.Lookup("users", "analyst")
	if p == nil {
		t.Fatal("expected policy for users:analyst")
	}
	if p.Columns["ssn"] != ColumnDeny {
		t.Fatalf("expected ssn=deny, got %d", p.Columns["ssn"])
	}
	if p.RowFilter != "department = 'engineering'" {
		t.Fatalf("expected row filter, got %q", p.RowFilter)
	}

	p2 := ps.Lookup("events", "reader")
	if p2 == nil {
		t.Fatal("expected policy for events:reader")
	}

	// No policy
	p3 := ps.Lookup("events", "admin")
	if p3 != nil {
		t.Fatal("expected nil for events:admin")
	}
}

func TestPolicySetWildcardTable(t *testing.T) {
	ps := NewPolicySet()
	ps.Add(&AccessPolicy{
		Table:   "*",
		Role:    "intern",
		Columns: map[string]ColumnPolicy{"salary": ColumnDeny},
	})

	// Should match any table for role "intern"
	p := ps.Lookup("anything", "intern")
	if p == nil {
		t.Fatal("wildcard table policy should match")
	}
	if p.Columns["salary"] != ColumnDeny {
		t.Fatal("salary should be denied")
	}
}

// --- Middleware ---

func TestMiddleware(t *testing.T) {
	cfg := testConfig()
	authn, _ := New(cfg)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := IdentityFromContext(r.Context())
		if id != nil {
			fmt.Fprintf(w, "hello %s", id.Name)
		} else {
			fmt.Fprint(w, "no auth")
		}
	})

	wrapped := Middleware(authn, nil)(handler)

	t.Run("valid key passes", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/v1/tables", nil)
		r.Header.Set("Authorization", "Bearer key-admin-123")
		w := httptest.NewRecorder()

		wrapped.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "admin-tool") {
			t.Fatalf("expected identity in response, got %q", w.Body.String())
		}
	})

	t.Run("invalid key rejected", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/v1/tables", nil)
		r.Header.Set("Authorization", "Bearer invalid-key")
		w := httptest.NewRecorder()

		wrapped.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatalf("expected 401, got %d", w.Code)
		}
	})

	t.Run("no auth rejected", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/v1/tables", nil)
		w := httptest.NewRecorder()

		wrapped.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatalf("expected 401, got %d", w.Code)
		}
	})

	t.Run("health check bypasses auth", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/v1/health", nil)
		w := httptest.NewRecorder()

		wrapped.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("expected 200 for health check, got %d", w.Code)
		}
	})
}

// --- Authenticator.Enabled ---

func TestAuthenticatorEnabled(t *testing.T) {
	var nilAuthn *Authenticator
	if nilAuthn.Enabled() {
		t.Fatal("nil authenticator should not be enabled")
	}

	emptyAuthn := &Authenticator{apiKeys: map[string]*Identity{}}
	if emptyAuthn.Enabled() {
		t.Fatal("empty authenticator should not be enabled")
	}

	authn, _ := New(testConfig())
	if !authn.Enabled() {
		t.Fatal("configured authenticator should be enabled")
	}
}

// --- Auth priority: mTLS > API key > JWT ---

func TestAuthPriority(t *testing.T) {
	secret := "test-secret"
	cfg := testConfig()
	cfg.JWT = JWTConfig{Enabled: true, Secret: secret}
	cfg.MTLS = MTLSConfig{
		Enabled:     true,
		RoleMap:     map[string]string{"service.internal": "admin"},
		DefaultRole: "",
	}
	authn, _ := New(cfg)

	// When both API key and JWT provided, API key wins
	token := signHS256(map[string]any{
		"sub":  "jwt-user",
		"role": "reader",
		"exp":  time.Now().Add(time.Hour).Unix(),
	}, secret)

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer key-admin-123") // API key

	id, err := authn.Authenticate(r)
	if err != nil {
		t.Fatal(err)
	}
	if id.Method != "apikey" {
		t.Fatalf("API key should take priority, got method=%q", id.Method)
	}

	// When only JWT provided
	r2 := httptest.NewRequest("GET", "/", nil)
	r2.Header.Set("Authorization", "Bearer "+token)

	id2, err := authn.Authenticate(r2)
	if err != nil {
		t.Fatal(err)
	}
	if id2.Method != "jwt" {
		t.Fatalf("expected jwt method, got %q", id2.Method)
	}
}

// --- Government Clearance Use Case ---

func TestClearanceLevelPolicies(t *testing.T) {
	// Simulates government clearance levels:
	// TOP SECRET can see everything
	// SECRET sees classified columns masked
	// UNCLASSIFIED has classified columns denied entirely

	configs := []PolicyConfig{
		{
			Table: "intelligence",
			Role:  "unclassified",
			Columns: map[string]string{
				"id":             "allow",
				"date":           "allow",
				"source":         "deny",
				"content":        "deny",
				"classification": "allow",
			},
			RowFilter: "classification = 'UNCLASSIFIED'",
		},
		{
			Table: "intelligence",
			Role:  "secret",
			Columns: map[string]string{
				"id":             "allow",
				"date":           "allow",
				"source":         "mask",
				"content":        "allow",
				"classification": "allow",
			},
			RowFilter: "classification in ('UNCLASSIFIED', 'SECRET')",
		},
		// top_secret has no policy — full access by default
	}

	ps := ParsePolicies(configs)

	row := map[string]any{
		"id":             int64(1),
		"date":           "2026-03-15",
		"source":         "HUMINT-ALPHA",
		"content":        "Operational details...",
		"classification": "SECRET",
	}

	// UNCLASSIFIED clearance
	unclass := ps.Lookup("intelligence", "unclassified")
	result := unclass.ApplyToRow(row)
	if _, exists := result["source"]; exists {
		t.Fatal("unclassified should not see source")
	}
	if _, exists := result["content"]; exists {
		t.Fatal("unclassified should not see content")
	}
	if result["id"] != int64(1) {
		t.Fatal("unclassified should see id")
	}

	// SECRET clearance
	secret := ps.Lookup("intelligence", "secret")
	result2 := secret.ApplyToRow(row)
	if result2["source"] != "***" {
		t.Fatalf("secret should see masked source, got %v", result2["source"])
	}
	if result2["content"] != "Operational details..." {
		t.Fatal("secret should see content")
	}

	// TOP SECRET — no policy, full access
	topSecret := ps.Lookup("intelligence", "top_secret")
	if topSecret != nil {
		t.Fatal("top_secret should have no policy (full access)")
	}
	// No policy means ApplyToRow returns original
	result3 := topSecret.ApplyToRow(row)
	if result3["source"] != "HUMINT-ALPHA" {
		t.Fatalf("top_secret should see full source, got %v", result3["source"])
	}
}

func TestGenerateRSACert(t *testing.T) {
	// Test that LoadClientCA works with a generated CA cert
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "Test CA"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	if err := os.WriteFile(caPath, caPEM, 0644); err != nil {
		t.Fatal(err)
	}

	pool, err := LoadClientCA(caPath)
	if err != nil {
		t.Fatalf("LoadClientCA failed: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil cert pool")
	}
}
