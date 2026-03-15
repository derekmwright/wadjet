// Package server provides the HTTP API for Caelum.
package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/derekmwright/caelum/internal/auth"
	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/exec"
	"github.com/derekmwright/caelum/internal/metrics"
	"github.com/derekmwright/caelum/internal/planner/logical"
	"github.com/derekmwright/caelum/internal/planner/physical"
	plansql "github.com/derekmwright/caelum/internal/planner/sql"
	"github.com/derekmwright/caelum/internal/storage/catalog"
)

// Config holds server configuration.
type Config struct {
	Addr      string
	Catalog   *catalog.Catalog
	Auth      *auth.Authenticator // nil = no authentication (static mode)
	Authz     *auth.Authorizer    // nil = no authorization (static mode)
	Policies  *auth.PolicySet     // nil = no cell-level policies (static mode)
	Provider  *auth.Provider      // nil = use static Auth/Authz/Policies above
	Metrics   *metrics.Metrics    // nil = no metrics collection
	TLSConfig *tls.Config         // nil = plain HTTP
}

// Server is the Caelum HTTP API server.
type Server struct {
	config   Config
	catalog  *catalog.Catalog
	planner  *physical.Planner
	logger   *slog.Logger
	mux      *http.ServeMux
	server   *http.Server
	provider *auth.Provider   // hot-reloadable auth (nil = static)
	authz    *auth.Authorizer
	policies *auth.PolicySet
	metrics  *metrics.Metrics // nil = no metrics
}

// New creates a new HTTP server.
func New(cfg Config, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	s := &Server{
		config:   cfg,
		catalog:  cfg.Catalog,
		planner:  physical.NewPlanner(cfg.Catalog),
		logger:   logger,
		mux:      http.NewServeMux(),
		provider: cfg.Provider,
		authz:    cfg.Authz,
		policies: cfg.Policies,
		metrics:  cfg.Metrics,
	}

	s.mux.HandleFunc("POST /v1/queries", s.handleQuery)
	s.mux.HandleFunc("GET /v1/tables", s.handleListTables)
	s.mux.HandleFunc("GET /v1/tables/{name}", s.handleGetTable)
	s.mux.HandleFunc("GET /v1/health", s.handleHealth)
	if s.metrics != nil {
		s.mux.Handle("GET /metrics", s.metrics.Handler())
	}

	return s
}

// Mux returns the underlying ServeMux for registering additional routes (e.g. admin API).
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	var handler http.Handler = s.mux
	if s.provider != nil {
		// Hot-reloadable auth via Provider
		handler = auth.ProviderMiddleware(s.provider, s.logger)(handler)
	} else if s.config.Auth != nil && s.config.Auth.Enabled() {
		// Static auth (backwards compatible)
		handler = auth.Middleware(s.config.Auth, s.logger)(handler)
	}

	s.server = &http.Server{
		Addr:         s.config.Addr,
		Handler:      handler,
		TLSConfig:    s.config.TLSConfig,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Minute,
	}

	s.logger.Info("HTTP server starting", "addr", s.config.Addr,
		"auth", s.config.Auth != nil && s.config.Auth.Enabled(),
		"tls", s.config.TLSConfig != nil,
	)

	if s.config.TLSConfig != nil {
		return s.server.ListenAndServeTLS("", "")
	}
	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// QueryRequest is the request body for POST /v1/queries.
type QueryRequest struct {
	SQL string `json:"sql"`
}

// QueryResponse is the response for POST /v1/queries.
type QueryResponse struct {
	QueryID string           `json:"query_id"`
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Stats   QueryStats       `json:"stats"`
	Error   string           `json:"error,omitempty"`
}

// QueryStats contains execution statistics.
type QueryStats struct {
	Elapsed     string `json:"elapsed"`
	RowsScanned int64  `json:"rows_scanned"`
	Plan        string `json:"plan,omitempty"`
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.SQL == "" {
		writeError(w, http.StatusBadRequest, "sql field is required")
		return
	}

	start := time.Now()

	// Instrument with metrics
	done := func() {} // no-op if metrics disabled
	if s.metrics != nil {
		done = s.metrics.QueryTimer("sql")
	}

	// Parse SQL
	parsed, err := plansql.Parse(req.SQL)
	if err != nil {
		writeError(w, http.StatusBadRequest, "SQL parse error: "+err.Error())
		return
	}

	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		writeError(w, http.StatusBadRequest, "SQL extraction error: "+err.Error())
		return
	}

	// Authorization: check table access after parsing, before planning
	identity := auth.IdentityFromContext(r.Context())
	authz := s.getAuthz()
	if identity != nil && authz != nil {
		if !authz.HasPermission(identity, "read") {
			writeError(w, http.StatusForbidden, "insufficient permissions")
			return
		}
		for _, table := range selectInfo.Tables {
			if !authz.CanAccessTable(identity, table.Name) {
				writeError(w, http.StatusForbidden,
					fmt.Sprintf("access denied to table %q", table.Name))
				return
			}
		}
		for _, join := range selectInfo.Joins {
			if !authz.CanAccessTable(identity, join.RightTable) {
				writeError(w, http.StatusForbidden,
					fmt.Sprintf("access denied to table %q", join.RightTable))
				return
			}
		}
	}

	// Build logical plan
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		writeError(w, http.StatusBadRequest, "plan build error: "+err.Error())
		return
	}

	// Optimize
	logicalPlan = logical.Optimize(logicalPlan)

	// Build physical plan
	physPlan, err := s.planner.Plan(r.Context(), logicalPlan)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "physical plan error: "+err.Error())
		return
	}

	// Execute
	pipeline := physPlan.Pipeline
	if err := pipeline.Run(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "execution error: "+err.Error())
		return
	}
	defer pipeline.Close()

	// Collect results
	var rows []map[string]any
	if collectSink, ok := pipeline.Sink.(*exec.CollectSink); ok {
		rows = collectSink.Rows
	}

	// Apply cell-level access policies (column masking/denial)
	policies := s.getPolicies()
	if identity != nil && policies != nil && len(rows) > 0 {
		// Find the queried table — use first table for single-table queries
		tableName := ""
		if len(selectInfo.Tables) > 0 {
			tableName = selectInfo.Tables[0].Name
		}
		if policy := policies.Lookup(tableName, identity.Role); policy != nil {
			rows = policy.ApplyToRows(rows)
		}
	}

	// Extract column names
	var columns []string
	if len(rows) > 0 {
		for k := range rows[0] {
			columns = append(columns, k)
		}
	}

	resp := QueryResponse{
		QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
		Columns: columns,
		Rows:    rows,
		Stats: QueryStats{
			Elapsed:     time.Since(start).String(),
			RowsScanned: int64(len(rows)),
			Plan:        logicalPlan.PrettyPrint(0),
		},
	}

	// Record metrics
	if s.metrics != nil {
		s.metrics.QueriesTotal.WithLabelValues("success").Inc()
		s.metrics.RowsOutput.Add(float64(len(rows)))
	}
	if done != nil {
		done()
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	tables, err := s.catalog.ListTables(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Filter tables by role access
	if identity := auth.IdentityFromContext(r.Context()); identity != nil {
		if authz := s.getAuthz(); authz != nil {
			tables = authz.FilterTables(identity, tables)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"tables": tables})
}

func (s *Server) handleGetTable(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	// Check table access
	if identity := auth.IdentityFromContext(r.Context()); identity != nil {
		if authz := s.getAuthz(); authz != nil && !authz.CanAccessTable(identity, name) {
			writeError(w, http.StatusForbidden,
				fmt.Sprintf("access denied to table %q", name))
			return
		}
	}

	table, err := s.catalog.GetTable(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, table)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// getAuthz returns the current authorizer (from Provider if hot-reloadable, else static).
func (s *Server) getAuthz() *auth.Authorizer {
	if s.provider != nil {
		return s.provider.Authorizer()
	}
	return s.authz
}

// getPolicies returns the current policy set (from Provider if hot-reloadable, else static).
func (s *Server) getPolicies() *auth.PolicySet {
	if s.provider != nil {
		return s.provider.Policies()
	}
	return s.policies
}

// Ensure batch is used (it's referenced via types flowing through)
var _ = batch.DefaultBatchSize
