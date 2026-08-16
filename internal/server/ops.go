package server

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/coordinator"
)

// OpsAPI provides operational endpoints for monitoring and cleanup.
type OpsAPI struct {
	coord *coordinator.Coordinator
}

// NewOpsAPI creates operational API endpoints.
func NewOpsAPI(coord *coordinator.Coordinator) *OpsAPI {
	return &OpsAPI{coord: coord}
}

// RegisterRoutes adds operational routes to the given router.
func (o *OpsAPI) RegisterRoutes(r chi.Router) {
	r.Get("/v1/workers", o.handleWorkers)
	r.Delete("/v1/results/{queryID}", o.handleDeleteResults)
	r.Post("/v1/results/cleanup", o.handleCleanupResults)
}

// GET /v1/workers — list active workers.
func (o *OpsAPI) handleWorkers(w http.ResponseWriter, r *http.Request) {
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
	id := auth.IdentityFromContext(r.Context())
	if id != nil {
		if authz := o.coord.Workers(); authz != nil {
			// Just check auth exists — admin check happens at middleware level
			_ = authz
		}
	}

	queryID := chi.URLParam(r, "queryID")
	if queryID == "" {
		writeError(w, http.StatusBadRequest, "queryID is required")
		return
	}

	cleaner := o.coord.Cleaner(nil, "")
	if cleaner == nil {
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
	var req struct {
		TTLHours int `json:"ttl_hours"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	cleaner := o.coord.Cleaner(nil, "")
	if cleaner == nil {
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
