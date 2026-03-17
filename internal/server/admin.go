package server

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/config"
)

// AdminAPI provides REST endpoints for runtime configuration management.
// All admin endpoints require the "admin" permission.
type AdminAPI struct {
	manager  *config.Manager
	provider *auth.Provider
	logger   *slog.Logger
}

// NewAdminAPI creates an admin API handler.
func NewAdminAPI(manager *config.Manager, provider *auth.Provider, logger *slog.Logger) *AdminAPI {
	if logger == nil {
		logger = slog.Default()
	}
	return &AdminAPI{
		manager:  manager,
		provider: provider,
		logger:   logger,
	}
}

// RegisterRoutes adds admin routes to the given router.
func (a *AdminAPI) RegisterRoutes(r chi.Router) {
	r.Get("/v1/admin/config", a.handleGetConfig)
	r.Put("/v1/admin/config", a.handleUpdateConfig)
	r.Post("/v1/admin/config/reload", a.handleReload)
	r.Post("/v1/admin/auth/keys", a.handleAddAPIKey)
	r.Delete("/v1/admin/auth/keys/{name}", a.handleRemoveAPIKey)
	r.Get("/v1/admin/auth/roles", a.handleListRoles)
	r.Put("/v1/admin/auth/roles", a.handleUpdateRoles)
	r.Put("/v1/admin/auth/policies", a.handleUpdatePolicies)
	r.Put("/v1/admin/tuning", a.handleUpdateTuning)
}

func (a *AdminAPI) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	id := auth.IdentityFromContext(r.Context())
	if id == nil {
		writeError(w, http.StatusUnauthorized, "authentication required")
		return false
	}
	if a.provider != nil {
		if authz := a.provider.Authorizer(); authz != nil {
			if !authz.HasPermission(id, "admin") {
				writeError(w, http.StatusForbidden, "admin permission required")
				return false
			}
		}
	}
	return true
}

// GET /v1/admin/config — returns current hot-reloadable config (auth, worker, parquet sections).
func (a *AdminAPI) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	if !a.requireAdmin(w, r) {
		return
	}
	cfg := a.manager.Current()

	// Return only hot-reloadable sections (exclude sensitive secrets)
	resp := map[string]any{
		"mode": cfg.Mode,
		"auth": map[string]any{
			"enabled":    cfg.Auth.Enabled,
			"num_keys":   len(cfg.Auth.APIKeys),
			"jwt":        cfg.Auth.JWT.Enabled,
			"mtls":       cfg.Auth.MTLS.Enabled,
			"num_roles":  len(cfg.Auth.Roles),
			"num_policies": len(cfg.Auth.Policies),
		},
		"worker": map[string]any{
			"max_concurrent": cfg.Worker.MaxConcurrent,
			"cache_bytes":    cfg.Worker.CacheBytes,
		},
		"parquet": map[string]any{
			"compression":      cfg.Parquet.Compression,
			"row_group_size":   cfg.Parquet.RowGroupSize,
			"page_buffer_size": cfg.Parquet.PageBufferSize,
		},
	}
	writeJSON(w, http.StatusOK, resp)
}

// PUT /v1/admin/config — applies a full config update (hot-reloadable fields only).
func (a *AdminAPI) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	if !a.requireAdmin(w, r) {
		return
	}
	var cfg config.Config
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeError(w, http.StatusBadRequest, "invalid config: "+err.Error())
		return
	}
	if err := a.manager.Apply(&cfg); err != nil {
		writeError(w, http.StatusBadRequest, "config validation: "+err.Error())
		return
	}
	a.logger.Info("config updated via admin API",
		"identity", auth.IdentityFromContext(r.Context()).String())
	writeJSON(w, http.StatusOK, map[string]string{"status": "applied"})
}

// POST /v1/admin/config/reload — reload config from the file on disk.
func (a *AdminAPI) handleReload(w http.ResponseWriter, r *http.Request) {
	if !a.requireAdmin(w, r) {
		return
	}
	var req struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Path == "" {
		writeError(w, http.StatusBadRequest, "path field is required")
		return
	}
	if err := a.manager.Reload(req.Path); err != nil {
		writeError(w, http.StatusInternalServerError, "reload failed: "+err.Error())
		return
	}
	a.logger.Info("config reloaded from file via admin API", "path", req.Path)
	writeJSON(w, http.StatusOK, map[string]string{"status": "reloaded"})
}

// POST /v1/admin/auth/keys — add a new API key.
func (a *AdminAPI) handleAddAPIKey(w http.ResponseWriter, r *http.Request) {
	if !a.requireAdmin(w, r) {
		return
	}
	var key config.AuthAPIKey
	if err := json.NewDecoder(r.Body).Decode(&key); err != nil {
		writeError(w, http.StatusBadRequest, "invalid key: "+err.Error())
		return
	}
	if key.Key == "" || key.Name == "" || key.Role == "" {
		writeError(w, http.StatusBadRequest, "key, name, and role are required")
		return
	}

	cfg := *a.manager.Current()
	cfg.Auth.APIKeys = append(cfg.Auth.APIKeys, key)
	if err := a.manager.Apply(&cfg); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.logger.Info("API key added via admin API", "name", key.Name)
	writeJSON(w, http.StatusCreated, map[string]string{"status": "created", "name": key.Name})
}

// DELETE /v1/admin/auth/keys/{name} — remove an API key by name.
func (a *AdminAPI) handleRemoveAPIKey(w http.ResponseWriter, r *http.Request) {
	if !a.requireAdmin(w, r) {
		return
	}
	name := chi.URLParam(r, "name")

	cfg := *a.manager.Current()
	found := false
	keys := make([]config.AuthAPIKey, 0, len(cfg.Auth.APIKeys))
	for _, k := range cfg.Auth.APIKeys {
		if k.Name == name {
			found = true
			continue
		}
		keys = append(keys, k)
	}
	if !found {
		writeError(w, http.StatusNotFound, "key not found: "+name)
		return
	}
	cfg.Auth.APIKeys = keys
	if err := a.manager.Apply(&cfg); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.logger.Info("API key removed via admin API", "name", name)
	writeJSON(w, http.StatusOK, map[string]string{"status": "removed", "name": name})
}

// GET /v1/admin/auth/roles — list current roles.
func (a *AdminAPI) handleListRoles(w http.ResponseWriter, r *http.Request) {
	if !a.requireAdmin(w, r) {
		return
	}
	cfg := a.manager.Current()
	writeJSON(w, http.StatusOK, map[string]any{"roles": cfg.Auth.Roles})
}

// PUT /v1/admin/auth/roles — replace all roles.
func (a *AdminAPI) handleUpdateRoles(w http.ResponseWriter, r *http.Request) {
	if !a.requireAdmin(w, r) {
		return
	}
	var req struct {
		Roles []config.AuthRole `json:"roles"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid roles: "+err.Error())
		return
	}
	cfg := *a.manager.Current()
	cfg.Auth.Roles = req.Roles
	if err := a.manager.Apply(&cfg); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.logger.Info("roles updated via admin API", "count", len(req.Roles))
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

// PUT /v1/admin/auth/policies — replace all cell-level policies.
func (a *AdminAPI) handleUpdatePolicies(w http.ResponseWriter, r *http.Request) {
	if !a.requireAdmin(w, r) {
		return
	}
	var req struct {
		Policies []config.AuthPolicy `json:"policies"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid policies: "+err.Error())
		return
	}
	cfg := *a.manager.Current()
	cfg.Auth.Policies = req.Policies
	if err := a.manager.Apply(&cfg); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.logger.Info("policies updated via admin API", "count", len(req.Policies))
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

// PUT /v1/admin/tuning — update runtime tuning parameters.
func (a *AdminAPI) handleUpdateTuning(w http.ResponseWriter, r *http.Request) {
	if !a.requireAdmin(w, r) {
		return
	}
	var req struct {
		MaxConcurrent *int   `json:"max_concurrent,omitempty"`
		CacheBytes    *int64 `json:"cache_bytes,omitempty"`
		Compression   *string `json:"compression,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid tuning: "+err.Error())
		return
	}

	cfg := *a.manager.Current()
	if req.MaxConcurrent != nil {
		cfg.Worker.MaxConcurrent = *req.MaxConcurrent
	}
	if req.CacheBytes != nil {
		cfg.Worker.CacheBytes = *req.CacheBytes
	}
	if req.Compression != nil {
		cfg.Parquet.Compression = *req.Compression
	}
	if err := a.manager.Apply(&cfg); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.logger.Info("tuning updated via admin API")
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}
