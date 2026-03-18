package server

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

// AsyncQueryResponse is returned when a query is submitted asynchronously.
type AsyncQueryResponse struct {
	QueryID string `json:"query_id"`
	State   string `json:"state"`
	Plan    string `json:"plan,omitempty"`
}

// QueryStatusResponse is returned when checking query status.
type QueryStatusResponse struct {
	QueryID   string              `json:"query_id"`
	SQL       string              `json:"sql"`
	State     string              `json:"state"`
	Stages    []StageStatusView   `json:"stages,omitempty"`
	Elapsed   string              `json:"elapsed"`
	TotalRows int64               `json:"total_rows"`
	Error     string              `json:"error,omitempty"`
}

// StageStatusView is the JSON representation of a stage's progress.
type StageStatusView struct {
	StageID     string `json:"stage_id"`
	Type        string `json:"type"`
	TotalTasks  int    `json:"total_tasks"`
	DoneTasks   int    `json:"done_tasks"`
	FailedTasks int    `json:"failed_tasks"`
}

// handleAsyncQuery submits a query for async execution and returns immediately.
func (s *Server) handleAsyncQuery(w http.ResponseWriter, r *http.Request) {
	if s.coord == nil {
		writeError(w, http.StatusServiceUnavailable,
			"async queries require distributed mode (coordinator)")
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.SQL == "" {
		writeError(w, http.StatusBadRequest, "sql field is required")
		return
	}

	queryID, planStr, err := s.coord.SubmitSQL(r.Context(), req.SQL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, AsyncQueryResponse{
		QueryID: queryID,
		State:   "running",
		Plan:    planStr,
	})
}

// handleGetQueryStatus returns the current status of a query.
func (s *Server) handleGetQueryStatus(w http.ResponseWriter, r *http.Request) {
	if s.coord == nil {
		writeError(w, http.StatusServiceUnavailable,
			"query status requires distributed mode (coordinator)")
		return
	}

	queryID := chi.URLParam(r, "queryID")
	if queryID == "" {
		writeError(w, http.StatusBadRequest, "queryID is required")
		return
	}

	status, err := s.coord.GetQueryStatus(queryID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	resp := QueryStatusResponse{
		QueryID:   status.QueryID,
		SQL:       status.SQL,
		State:     status.State,
		Elapsed:   status.Elapsed.Round(time.Millisecond).String(),
		TotalRows: status.TotalRows,
		Error:     status.Error,
	}
	for _, stage := range status.Stages {
		resp.Stages = append(resp.Stages, StageStatusView{
			StageID:     stage.StageID,
			Type:        stage.Type,
			TotalTasks:  stage.TotalTasks,
			DoneTasks:   stage.DoneTasks,
			FailedTasks: stage.FailedTasks,
		})
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleGetQueryResults returns the results for a completed query.
func (s *Server) handleGetQueryResults(w http.ResponseWriter, r *http.Request) {
	if s.coord == nil {
		writeError(w, http.StatusServiceUnavailable,
			"query results require distributed mode (coordinator)")
		return
	}

	queryID := chi.URLParam(r, "queryID")
	if queryID == "" {
		writeError(w, http.StatusBadRequest, "queryID is required")
		return
	}

	result, err := s.coord.GetQueryResults(r.Context(), queryID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	if result.Error != "" {
		writeJSON(w, http.StatusOK, QueryResponse{
			QueryID: result.QueryID,
			Stats: QueryStats{
				Elapsed: result.Elapsed.Round(time.Millisecond).String(),
				Plan:    result.Plan,
			},
			Error: result.Error,
		})
		return
	}

	writeJSON(w, http.StatusOK, QueryResponse{
		QueryID: result.QueryID,
		Columns: result.Columns,
		Rows:    result.Rows(),
		Stats: QueryStats{
			Elapsed:     result.Elapsed.Round(time.Millisecond).String(),
			RowsScanned: result.TotalRows,
			Plan:        result.Plan,
		},
	})
}

// handleCancelQuery cancels a running query.
func (s *Server) handleCancelQuery(w http.ResponseWriter, r *http.Request) {
	if s.coord == nil {
		writeError(w, http.StatusServiceUnavailable,
			"query cancellation requires distributed mode (coordinator)")
		return
	}

	queryID := chi.URLParam(r, "queryID")
	if queryID == "" {
		writeError(w, http.StatusBadRequest, "queryID is required")
		return
	}

	if err := s.coord.CancelQuery(queryID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"query_id": queryID,
		"state":    "cancelled",
	})
}

// handleListQueries returns all tracked queries.
func (s *Server) handleListQueries(w http.ResponseWriter, _ *http.Request) {
	if s.coord == nil {
		writeError(w, http.StatusServiceUnavailable,
			"query listing requires distributed mode (coordinator)")
		return
	}

	queries := s.coord.ListQueries()
	views := make([]QueryStatusResponse, 0, len(queries))
	for _, q := range queries {
		views = append(views, QueryStatusResponse{
			QueryID:   q.QueryID,
			SQL:       q.SQL,
			State:     q.State,
			Elapsed:   q.Elapsed.Round(time.Millisecond).String(),
			TotalRows: q.TotalRows,
			Error:     q.Error,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"queries": views,
		"count":   len(views),
	})
}
