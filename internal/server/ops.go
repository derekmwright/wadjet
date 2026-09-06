package server

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/coordinator"
)

// OpsAPI provides operational endpoints for monitoring and cleanup.
//
// Every route it registers requires the `admin` permission, the way the admin
// API's do: they read the cluster's operational state or destroy stored
// artifacts, and neither is something an ordinary query identity may do.
type OpsAPI struct {
	coord    *coordinator.Coordinator
	provider *auth.Provider // nil = no auth enforcement
}

// NewOpsAPI creates operational API endpoints. The provider is what the
// routes authorize against; nil (or auth disabled) enforces nothing.
func NewOpsAPI(coord *coordinator.Coordinator, provider *auth.Provider) *OpsAPI {
	return &OpsAPI{coord: coord, provider: provider}
}

// requireAdmin refuses a caller that does not hold the `admin` permission,
// and answers 403 with the shared authorizer's own text so the refusal reads
// the same here, on pgwire (42501) and on gRPC (PermissionDenied).
//
// It cannot live in the authentication middleware: the query endpoints on the
// same mux accept ordinary identities, so the check belongs to the operation,
// not to the door. The comment that claimed otherwise sat in
// handleDeleteResults and was never true — `auth.ProviderMiddleware` resolves
// an identity and checks no permission at all (#937).
func (o *OpsAPI) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	if err := auth.RequirePermission(o.provider, r.Context(), "admin"); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return false
	}
	return true
}

// RegisterRoutes adds operational routes to the given router.
func (o *OpsAPI) RegisterRoutes(r chi.Router) {
	r.Get("/v1/workers", o.handleWorkers)
	r.Delete("/v1/results/{queryID}", o.handleDeleteResults)
	r.Post("/v1/results/cleanup", o.handleCleanupResults)
}

// GET /v1/workers — list active workers. Admin: it publishes the cluster's
// membership and each worker's memory, which is operational state.
func (o *OpsAPI) handleWorkers(w http.ResponseWriter, r *http.Request) {
	if !o.requireAdmin(w, r) {
		return
	}
	workers := o.coord.Workers().ActiveWorkers()
	type workerView struct {
		WorkerID    string `json:"worker_id"`
		MemoryUsed  int64  `json:"memory_used"`
		MemoryTotal int64  `json:"memory_total"`
		LastSeen    string `json:"last_seen"`
	}
	var views []workerView
	for _, wr := range workers {
		views = append(views, workerView{
			WorkerID:    wr.WorkerID,
			MemoryUsed:  wr.MemoryUsed,
			MemoryTotal: wr.MemoryTotal,
			LastSeen:    wr.LastSeen.Format("2006-01-02T15:04:05Z"),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"workers": views,
		"count":   len(views),
	})
}

// DELETE /v1/results/{queryID} — delete all result files for a query.
func (o *OpsAPI) handleDeleteResults(w http.ResponseWriter, r *http.Request) {
	if !o.requireAdmin(w, r) {
		return
	}

	queryID := chi.URLParam(r, "queryID")
	if queryID == "" {
		writeError(w, http.StatusBadRequest, "queryID is required")
		return
	}

	cleaner := o.coord.Cleaner(nil, "")
	if !cleaner.Configured() {
		writeError(w, http.StatusServiceUnavailable, "result cleaner not available")
		return
	}

	deleted, err := cleaner.CleanQuery(r.Context(), queryID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"query_id": queryID,
		"deleted":  deleted,
	})
}

// POST /v1/results/cleanup — trigger stale result cleanup.
func (o *OpsAPI) handleCleanupResults(w http.ResponseWriter, r *http.Request) {
	if !o.requireAdmin(w, r) {
		return
	}

	var req struct {
		TTLHours int `json:"ttl_hours"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	cleaner := o.coord.Cleaner(nil, "")
	if !cleaner.Configured() {
		writeError(w, http.StatusServiceUnavailable, "result cleaner not available")
		return
	}

	deleted, err := cleaner.CleanStale(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"deleted": deleted,
	})
}
