package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
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

// GET /v1/admin/config — the EFFECTIVE configuration, key by key, each with
// the tier it came from and whether it can be changed at runtime.
//
// It used to report `manager.Current()` — the config file merged over the
// defaults — while the process ran on the flag variables, so an operator
// read a configuration that was not the running one and could not tell
// which tier had won (#828). Every key the resolver knows is reported now,
// with its source (flag / env / file / default / admin).
//
// A secret's VALUE is never echoed back; its source is, because "where did
// this credential come from" is the question an operator actually has and
// it leaks nothing.
func (a *AdminAPI) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	if !a.requireAdmin(w, r) {
		return
	}
	res := a.manager.Resolution()
	cfg := res.Config()

	entries := make(map[string]any, len(config.Keys()))
	order := make([]string, 0, len(config.Keys()))
	for _, k := range config.Keys() {
		entry := map[string]any{
			"source":         string(res.Source(k.Name)),
			"hot_reloadable": a.manager.HotReloadable(k.Name),
		}
		if k.Secret {
			entry["redacted"] = true
		} else {
			entry["value"] = k.Get(cfg)
		}
		if k.Deferred {
			// Rule 11: a key with no runtime consumer says so, rather than
			// reporting a value the process is not acting on.
			entry["reaches_runtime"] = false
			entry["deferred_reason"] = k.DeferredWhy
		}
		if k.Env != "" {
			entry["env"] = k.Env
		}
		if k.Flag != "" {
			entry["flag"] = "--" + k.Flag
		}
		entries[k.Name] = entry
		order = append(order, k.Name)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"mode":  cfg.Mode,
		"keys":  entries,
		"order": order,
		"auth": map[string]any{
			"enabled":      cfg.Auth.Enabled,
			"num_keys":     len(cfg.Auth.APIKeys),
			"jwt":          cfg.Auth.JWT.Enabled,
			"mtls":         cfg.Auth.MTLS.Enabled,
			"num_roles":    len(cfg.Auth.Roles),
			"num_policies": len(cfg.Auth.Policies),
		},
	})
}

// refuseNotHotReloadable answers 409 naming every registry key the request
// would change that no subscriber applies at runtime, and reports whether it
// wrote a response.
//
// This is #828's other half. `Apply` used to silently FREEZE Mode, HTTP.Addr
// and the NATS fields and accept everything else, so a PUT of
// worker.max_concurrent returned {"status":"applied"} and changed nothing —
// the only Manager subscriber in the tree is the auth reload. A value the
// process will not act on is refused with its name, never accepted quietly.
func (a *AdminAPI) refuseNotHotReloadable(w http.ResponseWriter, current, proposed *config.Config) bool {
	var refused []string
	for _, name := range config.ChangedKeys(current, proposed) {
		if !a.manager.HotReloadable(name) {
			refused = append(refused, name)
		}
	}
	if len(refused) == 0 {
		return false
	}
	writeError(w, http.StatusConflict,
		"not hot-reloadable, nothing consumes a runtime change to: "+strings.Join(refused, ", ")+
			" — restart with the flag, the environment variable or the config file instead")
	return true
}

// PUT /v1/admin/config — applies a config update, or refuses it by name.
//
// The body is decoded ONTO the current configuration rather than into a zero
// one: a PUT that mentions three fields used to blank every field it did not
// mention, which then read as a change to each of them.
func (a *AdminAPI) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	if !a.requireAdmin(w, r) {
		return
	}
	current := a.manager.Current()
	cfg := *current
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeError(w, http.StatusBadRequest, "invalid config: "+err.Error())
		return
	}
	if a.refuseNotHotReloadable(w, current, &cfg) {
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
	// The keys the file changed that nothing re-reads are REPORTED, not
	// silently dropped. A file reload cannot refuse the way PUT does — the
	// file legitimately carries startup-only keys for the next start — but
	// answering a bare "reloaded" is how an operator comes to believe an
	// edit to worker.max_concurrent took effect (#828).
	ignored, err := a.manager.ReloadWithReport(req.Path)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "reload failed: "+err.Error())
		return
	}
	a.logger.Info("config reloaded from file via admin API", "path", req.Path)
	resp := map[string]any{"status": "reloaded"}
	if len(ignored) > 0 {
		resp["not_applied"] = ignored
		resp["not_applied_reason"] = "these keys take effect only at startup; " +
			"the running process was not changed"
	}
	writeJSON(w, http.StatusOK, resp)
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
		MaxConcurrent *int    `json:"max_concurrent,omitempty"`
		CacheBytes    *int64  `json:"cache_bytes,omitempty"`
		Compression   *string `json:"compression,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid tuning: "+err.Error())
		return
	}

	current := a.manager.Current()
	cfg := *current
	if req.MaxConcurrent != nil {
		cfg.Worker.MaxConcurrent = *req.MaxConcurrent
	}
	if req.CacheBytes != nil {
		cfg.Worker.CacheBytes = *req.CacheBytes
	}
	if req.Compression != nil {
		cfg.Parquet.Compression = *req.Compression
	}
	// Same refusal as PUT /v1/admin/config: worker.* and parquet.* have no
	// subscriber, so a "tuning" write here changed a value nothing reads and
	// answered {"status":"updated"} (#828).
	if a.refuseNotHotReloadable(w, current, &cfg) {
		return
	}
	if err := a.manager.Apply(&cfg); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.logger.Info("tuning updated via admin API")
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}
