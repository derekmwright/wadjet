package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/config"
)

func newTestAdminAPI(t *testing.T) (*AdminAPI, *config.Manager, chi.Router) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	initial := &config.Config{
		Mode: "standalone",
		Worker: config.Worker{
			MaxConcurrent: 4,
			CacheBytes:    64 * 1024 * 1024,
		},
		Parquet: config.Parquet{
			Compression:    "snappy",
			RowGroupSize:   10000,
			PageBufferSize: 1024,
		},
	}
	mgr := config.NewManager(initial, logger)

	api := NewAdminAPI(mgr, nil, logger)
	r := chi.NewRouter()
	api.RegisterRoutes(r)

	return api, mgr, r
}

func newTestAdminAPIWithAuth(t *testing.T) (*AdminAPI, chi.Router, *auth.Provider) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	initial := &config.Config{
		Mode: "standalone",
		Worker: config.Worker{
			MaxConcurrent: 4,
			CacheBytes:    64 * 1024 * 1024,
		},
	}
	mgr := config.NewManager(initial, logger)

	cfg := auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "admin-key", Name: "admin-user", Role: "admin"},
			{Key: "reader-key", Name: "reader-user", Role: "reader"},
		},
		Roles: []auth.RoleConfig{
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
			{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}},
		},
	}
	authn, authz := auth.New(cfg)
	provider := auth.NewProvider(authn, authz, nil, nil)

	api := NewAdminAPI(mgr, provider, logger)
	r := chi.NewRouter()
	api.RegisterRoutes(r)

	return api, r, provider
}

// Helper to add admin identity to request context
func withAdminCtx(r *http.Request) *http.Request {
	id := &auth.Identity{Name: "admin-user", Role: "admin"}
	return r.WithContext(auth.ContextWithIdentity(r.Context(), id))
}

// Helper to add reader identity to request context
func withReaderCtx(r *http.Request) *http.Request {
	id := &auth.Identity{Name: "reader-user", Role: "reader"}
	return r.WithContext(auth.ContextWithIdentity(r.Context(), id))
}

// --- NewAdminAPI ---

func TestNewAdminAPI_NilLogger(t *testing.T) {
	mgr := config.NewManager(&config.Config{Mode: "standalone", Worker: config.Worker{MaxConcurrent: 1}}, nil)
	api := NewAdminAPI(mgr, nil, nil)
	if api == nil {
		t.Fatal("expected non-nil AdminAPI")
	}
}

// --- requireAdmin ---

func TestRequireAdmin_NoIdentity(t *testing.T) {
	_, _, router := newTestAdminAPI(t)
	ts := httptest.NewServer(router)
	defer ts.Close()

	// No identity in context - requireAdmin should reject
	// But since there's no provider, requireAdmin allows through
	// because the check is: id == nil -> unauthorized
	req, _ := http.NewRequest("GET", ts.URL+"/v1/admin/config", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	// With no auth middleware, identity is nil -> 401
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without identity, got %d", resp.StatusCode)
	}
}

func TestRequireAdmin_ForbiddenNonAdmin(t *testing.T) {
	_, router, _ := newTestAdminAPIWithAuth(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/v1/admin/config", nil)
	req = withReaderCtx(req) // reader, not admin
	router.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-admin, got %d", w.Code)
	}
}

// --- handleGetConfig ---

func TestHandleGetConfig(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/v1/admin/config", nil)
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var body map[string]any
	json.NewDecoder(w.Body).Decode(&body)
	if body["mode"] != "standalone" {
		t.Errorf("expected mode=standalone, got %v", body["mode"])
	}
}

// --- handleUpdateConfig ---

func TestHandleUpdateConfig_Success(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	cfg := config.Config{
		Mode:   "standalone",
		Worker: config.Worker{MaxConcurrent: 8, CacheBytes: 128 * 1024 * 1024},
	}
	body, _ := json.Marshal(cfg)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/v1/admin/config", bytes.NewReader(body))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		var errBody map[string]string
		json.NewDecoder(w.Body).Decode(&errBody)
		t.Fatalf("expected 200, got %d: %v", w.Code, errBody)
	}
}

func TestHandleUpdateConfig_InvalidBody(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/v1/admin/config", bytes.NewBufferString("not json"))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleUpdateConfig_ValidationError(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	// MaxConcurrent = 0 should fail validation
	cfg := config.Config{
		Worker: config.Worker{MaxConcurrent: 0},
	}
	body, _ := json.Marshal(cfg)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/v1/admin/config", bytes.NewReader(body))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// --- handleReload ---

func TestHandleReload_MissingPath(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/admin/config/reload", bytes.NewBufferString(`{}`))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty path, got %d", w.Code)
	}
}

func TestHandleReload_InvalidBody(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/admin/config/reload", bytes.NewBufferString("bad"))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleReload_NonexistentFile(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/admin/config/reload",
		bytes.NewBufferString(`{"path":"/nonexistent/config.yaml"}`))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for nonexistent file, got %d", w.Code)
	}
}

// --- handleAddAPIKey ---

func TestHandleAddAPIKey_MissingFields(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/admin/auth/keys",
		bytes.NewBufferString(`{"key":"abc"}`))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing fields, got %d", w.Code)
	}
}

func TestHandleAddAPIKey_InvalidBody(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/admin/auth/keys",
		bytes.NewBufferString("not json"))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid body, got %d", w.Code)
	}
}

func TestHandleAddAPIKey_Success(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	body := `{"key":"new-key","name":"new-user","role":"analyst"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/admin/auth/keys", bytes.NewBufferString(body))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		var errBody map[string]string
		json.NewDecoder(w.Body).Decode(&errBody)
		t.Fatalf("expected 201, got %d: %v", w.Code, errBody)
	}
}

// --- handleRemoveAPIKey ---

func TestHandleRemoveAPIKey_NotFound(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("DELETE", "/v1/admin/auth/keys/nonexistent", nil)
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandleRemoveAPIKey_Success(t *testing.T) {
	_, mgr, router := newTestAdminAPI(t)

	// Add a key first
	cfg := *mgr.Current()
	cfg.Auth.APIKeys = append(cfg.Auth.APIKeys, config.AuthAPIKey{Key: "rm-key", Name: "rm-user", Role: "reader"})
	mgr.Apply(&cfg)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("DELETE", "/v1/admin/auth/keys/rm-user", nil)
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		var errBody map[string]string
		json.NewDecoder(w.Body).Decode(&errBody)
		t.Fatalf("expected 200, got %d: %v", w.Code, errBody)
	}
}

// --- handleListRoles ---

func TestHandleListRoles(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/v1/admin/auth/roles", nil)
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// --- handleUpdateRoles ---

func TestHandleUpdateRoles_Success(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	body := `{"roles":[{"name":"analyst","tables":["*"],"allow":["read"]}]}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/v1/admin/auth/roles", bytes.NewBufferString(body))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		var errBody map[string]string
		json.NewDecoder(w.Body).Decode(&errBody)
		t.Fatalf("expected 200, got %d: %v", w.Code, errBody)
	}
}

func TestHandleUpdateRoles_InvalidBody(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/v1/admin/auth/roles", bytes.NewBufferString("bad"))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// --- handleUpdatePolicies ---

func TestHandleUpdatePolicies_Success(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	body := `{"policies":[]}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/v1/admin/auth/policies", bytes.NewBufferString(body))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleUpdatePolicies_InvalidBody(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/v1/admin/auth/policies", bytes.NewBufferString("bad"))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// --- handleUpdateTuning ---

func TestHandleUpdateTuning_Success(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	maxC := 16
	body := map[string]any{"max_concurrent": maxC}
	b, _ := json.Marshal(body)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/v1/admin/tuning", bytes.NewReader(b))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		var errBody map[string]string
		json.NewDecoder(w.Body).Decode(&errBody)
		t.Fatalf("expected 200, got %d: %v", w.Code, errBody)
	}
}

func TestHandleUpdateTuning_CacheBytes(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	body := `{"cache_bytes": 256000000}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/v1/admin/tuning", bytes.NewBufferString(body))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleUpdateTuning_Compression(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	body := `{"compression": "zstd"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/v1/admin/tuning", bytes.NewBufferString(body))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleUpdateTuning_InvalidBody(t *testing.T) {
	_, _, router := newTestAdminAPI(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/v1/admin/tuning", bytes.NewBufferString("bad"))
	req = withAdminCtx(req)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// Ensure unused import is satisfied
var _ = context.Background
