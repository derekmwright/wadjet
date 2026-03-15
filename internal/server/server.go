// Package server provides the HTTP API for Caelum.
package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/derekmwright/caelum/internal/auth"
	"github.com/derekmwright/caelum/internal/coordinator"
	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/exec"
	"github.com/derekmwright/caelum/internal/engine/expr"
	"github.com/derekmwright/caelum/internal/metrics"
	"github.com/derekmwright/caelum/internal/planner/logical"
	"github.com/derekmwright/caelum/internal/planner/physical"
	plansql "github.com/derekmwright/caelum/internal/planner/sql"
	"github.com/derekmwright/caelum/internal/storage/catalog"
)

// Config holds server configuration.
type Config struct {
	Addr        string
	Catalog     *catalog.Catalog
	Coordinator *coordinator.Coordinator // nil = local execution only
	Auth        *auth.Authenticator      // nil = no authentication (static mode)
	Authz       *auth.Authorizer         // nil = no authorization (static mode)
	Policies    *auth.PolicySet          // nil = no cell-level policies (static mode)
	Provider    *auth.Provider           // nil = use static Auth/Authz/Policies above
	Metrics     *metrics.Metrics         // nil = no metrics collection
	TLSConfig   *tls.Config              // nil = plain HTTP
}

// Server is the Caelum HTTP API server.
type Server struct {
	config   Config
	catalog  *catalog.Catalog
	planner  *physical.Planner
	coord    *coordinator.Coordinator // nil = local execution
	logger   *slog.Logger
	mux      chi.Router
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
		coord:    cfg.Coordinator,
		logger:   logger,
		mux:      chi.NewRouter(),
		provider: cfg.Provider,
		authz:    cfg.Authz,
		policies: cfg.Policies,
		metrics:  cfg.Metrics,
	}

	s.mux.Post("/v1/queries", s.handleQuery)
	s.mux.Get("/v1/queries", s.handleListQueries)
	s.mux.Post("/v1/queries/async", s.handleAsyncQuery)
	s.mux.Get("/v1/queries/{queryID}", s.handleGetQueryStatus)
	s.mux.Get("/v1/queries/{queryID}/results", s.handleGetQueryResults)
	s.mux.Delete("/v1/queries/{queryID}", s.handleCancelQuery)
	s.mux.Get("/v1/tables", s.handleListTables)
	s.mux.Get("/v1/tables/{name}", s.handleGetTable)
	s.mux.Get("/v1/health", s.handleHealth)
	if s.metrics != nil {
		s.mux.Handle("/metrics", s.metrics.Handler())
	}

	return s
}

// Mux returns the underlying chi router for registering additional routes (e.g. admin API).
func (s *Server) Mux() chi.Router {
	return s.mux
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	if s.provider != nil {
		// Hot-reloadable auth via Provider
		s.mux.Use(auth.ProviderMiddleware(s.provider, s.logger))
	} else if s.config.Auth != nil && s.config.Auth.Enabled() {
		// Static auth (backwards compatible)
		s.mux.Use(auth.Middleware(s.config.Auth, s.logger))
	}

	s.server = &http.Server{
		Addr:         s.config.Addr,
		Handler:      s.mux,
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

	// Handle DESCRIBE/SHOW COLUMNS
	if parsed.Type == plansql.QueryDescribe {
		s.handleDescribe(w, r, parsed, start)
		return
	}

	// Handle EXPLAIN
	if parsed.Type == plansql.QueryExplain {
		s.handleExplain(w, r, parsed, start)
		return
	}

	// Handle CREATE/DROP/SHOW FUNCTION
	if parsed.Type == plansql.QueryCreateFunction {
		s.handleCreateFunction(w, r, parsed, start)
		return
	}
	if parsed.Type == plansql.QueryDropFunction {
		s.handleDropFunction(w, r, parsed, start)
		return
	}
	if parsed.Type == plansql.QueryShowFunctions {
		s.handleShowFunctions(w, r, start)
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

	// Distributed execution path: use coordinator if available
	if s.coord != nil {
		result, err := s.coord.ExecuteSQL(r.Context(), req.SQL)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "distributed execution error: "+err.Error())
			return
		}

		rows := result.Rows

		// Apply cell-level access policies
		policies := s.getPolicies()
		if identity != nil && policies != nil && len(rows) > 0 {
			tableName := ""
			if len(selectInfo.Tables) > 0 {
				tableName = selectInfo.Tables[0].Name
			}
			if policy := policies.Lookup(tableName, identity.Role); policy != nil {
				rows = policy.ApplyToRows(rows)
			}
		}

		resp := QueryResponse{
			QueryID: result.QueryID,
			Columns: result.Columns,
			Rows:    rows,
			Stats: QueryStats{
				Elapsed:     result.Elapsed.String(),
				RowsScanned: result.TotalRows,
				Plan:        result.Plan,
			},
		}

		if s.metrics != nil {
			s.metrics.QueriesTotal.WithLabelValues("success").Inc()
			s.metrics.RowsOutput.Add(float64(len(rows)))
		}
		if done != nil {
			done()
		}

		writeJSON(w, http.StatusOK, resp)
		return
	}

	// Local execution path (standalone without coordinator)

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
	name := chi.URLParam(r, "name")

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

func (s *Server) handleDescribe(w http.ResponseWriter, r *http.Request, parsed *plansql.ParsedQuery, start time.Time) {
	tableName := parsed.Describe.TableName

	// Check table access
	if identity := auth.IdentityFromContext(r.Context()); identity != nil {
		if authz := s.getAuthz(); authz != nil && !authz.CanAccessTable(identity, tableName) {
			writeError(w, http.StatusForbidden, fmt.Sprintf("access denied to table %q", tableName))
			return
		}
	}

	table, err := s.catalog.GetTable(r.Context(), tableName)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("table %q: %s", tableName, err.Error()))
		return
	}

	columns := []string{"column_name", "type", "nullable"}
	rows := make([]map[string]any, len(table.Schema.Columns))
	for i, col := range table.Schema.Columns {
		nullable := "NO"
		if col.Nullable {
			nullable = "YES"
		}
		rows[i] = map[string]any{
			"column_name": col.Name,
			"type":        col.Type.String(),
			"nullable":    nullable,
		}
	}

	writeJSON(w, http.StatusOK, QueryResponse{
		QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
		Columns: columns,
		Rows:    rows,
		Stats:   QueryStats{Elapsed: time.Since(start).String()},
	})
}

func (s *Server) handleExplain(w http.ResponseWriter, r *http.Request, parsed *plansql.ParsedQuery, start time.Time) {
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		writeError(w, http.StatusBadRequest, "SQL extraction error: "+err.Error())
		return
	}

	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		writeError(w, http.StatusBadRequest, "plan build error: "+err.Error())
		return
	}
	logicalPlan = logical.Optimize(logicalPlan)
	planStr := logicalPlan.PrettyPrint(0)

	if parsed.Explain.Verbose {
		physPlan, err := s.planner.Plan(r.Context(), logicalPlan)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "physical plan error: "+err.Error())
			return
		}
		planStr += "\n\n-- Physical Plan --\n" + physPlan.PrettyPrint()
	}

	rows := []map[string]any{{"plan": planStr}}

	writeJSON(w, http.StatusOK, QueryResponse{
		QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
		Columns: []string{"plan"},
		Rows:    rows,
		Stats: QueryStats{
			Elapsed: time.Since(start).String(),
			Plan:    planStr,
		},
	})
}

func (s *Server) handleCreateFunction(w http.ResponseWriter, r *http.Request, parsed *plansql.ParsedQuery, start time.Time) {
	cf := parsed.CreateFunction
	identity := auth.IdentityFromContext(r.Context())

	owner := ""
	isAdmin := false
	if identity != nil {
		owner = identity.Name
		isAdmin = identity.Role == "admin"
	}

	def := expr.UDFDef{
		Name:   cf.Name,
		Params: cf.Params,
		Body:   cf.Body,
		Owner:  owner,
		Locked: cf.Locked,
	}

	// Check if function exists and this is not OR REPLACE
	if !cf.Replace {
		if _, exists := expr.DefaultUDFs.Get(def.Name); exists {
			writeError(w, http.StatusConflict,
				fmt.Sprintf("function %q already exists (use CREATE OR REPLACE to overwrite)", cf.Name))
			return
		}
	}

	if err := expr.DefaultUDFs.Register(def, isAdmin); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, QueryResponse{
		QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": fmt.Sprintf("Function %q created", cf.Name)}},
		Stats:   QueryStats{Elapsed: time.Since(start).String()},
	})
}

func (s *Server) handleDropFunction(w http.ResponseWriter, r *http.Request, parsed *plansql.ParsedQuery, start time.Time) {
	df := parsed.DropFunction
	identity := auth.IdentityFromContext(r.Context())

	caller := ""
	isAdmin := false
	if identity != nil {
		caller = identity.Name
		isAdmin = identity.Role == "admin"
	}

	err := expr.DefaultUDFs.Unregister(df.Name, caller, isAdmin)
	if err != nil {
		if df.IfExists {
			// IF EXISTS — don't error if not found
			writeJSON(w, http.StatusOK, QueryResponse{
				QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
				Columns: []string{"result"},
				Rows:    []map[string]any{{"result": fmt.Sprintf("Function %q does not exist (no-op)", df.Name)}},
				Stats:   QueryStats{Elapsed: time.Since(start).String()},
			})
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, QueryResponse{
		QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": fmt.Sprintf("Function %q dropped", df.Name)}},
		Stats:   QueryStats{Elapsed: time.Since(start).String()},
	})
}

func (s *Server) handleShowFunctions(w http.ResponseWriter, _ *http.Request, start time.Time) {
	udfs := expr.DefaultUDFs.List()

	rows := make([]map[string]any, len(udfs))
	for i, udf := range udfs {
		params := "(" + strings.Join(udf.Params, ", ") + ")"
		locked := "NO"
		if udf.Locked {
			locked = "YES"
		}
		rows[i] = map[string]any{
			"name":   udf.Name,
			"params": params,
			"body":   udf.Body,
			"owner":  udf.Owner,
			"locked": locked,
		}
	}

	writeJSON(w, http.StatusOK, QueryResponse{
		QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
		Columns: []string{"name", "params", "body", "owner", "locked"},
		Rows:    rows,
		Stats:   QueryStats{Elapsed: time.Since(start).String()},
	})
}

// Ensure batch is used (it's referenced via types flowing through)
var _ = batch.DefaultBatchSize
