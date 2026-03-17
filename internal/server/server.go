// Package server provides the HTTP API for Caelum.
package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"golang.org/x/net/netutil"

	"github.com/derekmwright/caelum/internal/auth"
	"github.com/derekmwright/caelum/internal/config"
	"github.com/derekmwright/caelum/internal/coordinator"
	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/exec"
	"github.com/derekmwright/caelum/internal/engine/expr"
	"github.com/derekmwright/caelum/internal/metrics"
	"github.com/derekmwright/caelum/internal/planner/logical"
	"github.com/derekmwright/caelum/internal/planner/physical"
	plansql "github.com/derekmwright/caelum/internal/planner/sql"
	"github.com/derekmwright/caelum/internal/storage/catalog"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

// Config holds server configuration.
type Config struct {
	Addr              string
	Catalog           *catalog.Catalog
	Coordinator       *coordinator.Coordinator // nil = local execution only
	Auth              *auth.Authenticator      // nil = no authentication (static mode)
	Authz             *auth.Authorizer         // nil = no authorization (static mode)
	Policies          *auth.PolicySet          // nil = no cell-level policies (static mode)
	Provider          *auth.Provider           // nil = use static Auth/Authz/Policies above
	Metrics           *metrics.Metrics         // nil = no metrics collection
	TLSConfig         *tls.Config              // nil = plain HTTP
	MaxConnections     int                      // 0 = unlimited
	SlowQueryThreshold time.Duration            // 0 = disabled, log queries exceeding this
	ShutdownTimeout    time.Duration            // graceful shutdown drain timeout (default 30s)
	QueryLimits        *config.QueryLimits      // global cost-based query limits (nil = unlimited)
	RoleLimits         map[string]*config.QueryLimits // per-role overrides (nil = use global)
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
	audit    *auth.AuditLogger
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
		audit:    auth.NewAuditLogger(logger),
	}

	s.mux.Post("/v1/queries", s.handleQuery)
	s.mux.Get("/v1/queries", s.handleListQueries)
	s.mux.Post("/v1/queries/async", s.handleAsyncQuery)
	s.mux.Get("/v1/queries/{queryID}", s.handleGetQueryStatus)
	s.mux.Get("/v1/queries/{queryID}/results", s.handleGetQueryResults)
	s.mux.Delete("/v1/queries/{queryID}", s.handleCancelQuery)
	s.mux.Get("/v1/tables", s.handleListTables)
	s.mux.Post("/v1/tables", s.handleCreateTable)
	s.mux.Get("/v1/tables/{name}", s.handleGetTable)
	s.mux.Delete("/v1/tables/{name}", s.handleDeleteTable)
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
		Handler:      s.mux,
		TLSConfig:    s.config.TLSConfig,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Minute,
		IdleTimeout:  120 * time.Second,
	}

	ln, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		return fmt.Errorf("http listen: %w", err)
	}

	if s.config.MaxConnections > 0 {
		ln = netutil.LimitListener(ln, s.config.MaxConnections)
	}

	s.logger.Info("HTTP server starting", "addr", s.config.Addr,
		"auth", s.config.Auth != nil && s.config.Auth.Enabled(),
		"tls", s.config.TLSConfig != nil,
		"max_connections", s.config.MaxConnections,
	)

	if s.config.TLSConfig != nil {
		return s.server.ServeTLS(ln, "", "")
	}
	return s.server.Serve(ln)
}

// Shutdown gracefully shuts down the server, draining in-flight requests.
func (s *Server) Shutdown(ctx context.Context) error {
	timeout := s.config.ShutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	drainCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	s.logger.Info("HTTP server shutting down", "drain_timeout", timeout)
	return s.server.Shutdown(drainCtx)
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

	// Handle CREATE TABLE
	if parsed.Type == plansql.QueryCreateTable {
		s.handleCreateTableSQL(w, r, parsed, start)
		return
	}

	// Handle DROP TABLE
	if parsed.Type == plansql.QueryDropTable {
		s.handleDropTableSQL(w, r, parsed, start)
		return
	}

	// Handle SHOW TABLES
	if parsed.Type == plansql.QueryShowTables {
		s.handleShowTables(w, r, start)
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

	// Collect row-level security filters from access policies
	var rowFilters auth.RowFilters
	policies := s.getPolicies()
	if identity != nil && policies != nil {
		for _, table := range selectInfo.Tables {
			if policy := policies.Lookup(table.Name, identity.Role); policy != nil && policy.RowFilter != "" {
				if rowFilters == nil {
					rowFilters = make(auth.RowFilters)
				}
				rowFilters[table.Name] = policy.RowFilter
				if s.audit != nil {
					s.audit.LogRowFilterApplied(identity, table.Name, policy.RowFilter)
				}
			}
		}
	}

	// Distributed execution path: use coordinator if available
	if s.coord != nil {
		// Pass row filters and identity through context for the coordinator
		execCtx := r.Context()
		if len(rowFilters) > 0 {
			execCtx = auth.ContextWithRowFilters(execCtx, rowFilters)
		}
		result, err := s.coord.ExecuteSQL(execCtx, req.SQL)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "distributed execution error: "+err.Error())
			return
		}

		rows := result.Rows

		// Apply cell-level access policies (column masking/denial)
		if identity != nil && policies != nil && len(rows) > 0 {
			tableName := ""
			if len(selectInfo.Tables) > 0 {
				tableName = selectInfo.Tables[0].Name
			}
			if policy := policies.Lookup(tableName, identity.Role); policy != nil {
				rows = policy.ApplyToRows(rows)
				s.auditColumnPolicy(identity, tableName, policy)
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
		s.logSlowQuery(req.SQL, result.Elapsed, len(rows))

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

	// Inject row-level security filters into logical plan
	for table, filter := range rowFilters {
		logicalPlan = logical.InjectRowFilter(logicalPlan, table, filter)
	}

	// Optimize
	logicalPlan = logical.Optimize(logicalPlan)

	// Apply query cost limits (per-role or global)
	s.planner.QueryLimits = s.resolveQueryLimits(r)

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
		rows = collectSink.ToRows()
	}

	// Apply cell-level access policies (column masking/denial)
	if identity != nil && policies != nil && len(rows) > 0 {
		// Find the queried table — use first table for single-table queries
		tableName := ""
		if len(selectInfo.Tables) > 0 {
			tableName = selectInfo.Tables[0].Name
		}
		if policy := policies.Lookup(tableName, identity.Role); policy != nil {
			rows = policy.ApplyToRows(rows)
			s.auditColumnPolicy(identity, tableName, policy)
		}
	}

	// Extract column names
	var columns []string
	if len(rows) > 0 {
		for k := range rows[0] {
			columns = append(columns, k)
		}
	}

	// Extract actual rows scanned from the pipeline source
	var rowsScanned int64
	if sp, ok := pipeline.Source.(exec.ScanStatsProvider); ok {
		rowsScanned = sp.RowsScanned()
	}
	if rowsScanned == 0 {
		rowsScanned = int64(len(rows)) // fallback for non-scan sources
	}

	resp := QueryResponse{
		QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
		Columns: columns,
		Rows:    rows,
		Stats: QueryStats{
			Elapsed:     time.Since(start).String(),
			RowsScanned: rowsScanned,
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
	s.logSlowQuery(req.SQL, time.Since(start), len(rows))

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

// resolveQueryLimits returns the effective query limits for the request identity.
// Per-role limits override global limits when set.
func (s *Server) resolveQueryLimits(r *http.Request) *config.QueryLimits {
	if identity := auth.IdentityFromContext(r.Context()); identity != nil && s.config.RoleLimits != nil {
		if limits, ok := s.config.RoleLimits[identity.Role]; ok {
			return limits
		}
	}
	return s.config.QueryLimits
}

// logSlowQuery logs a warning if the query exceeded the slow query threshold.
func (s *Server) logSlowQuery(sql string, elapsed time.Duration, rows int) {
	if s.config.SlowQueryThreshold > 0 && elapsed >= s.config.SlowQueryThreshold {
		s.logger.Warn("slow query",
			"sql", sql,
			"elapsed", elapsed.String(),
			"rows", rows,
		)
	}
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

// auditColumnPolicy logs which columns were masked or denied for this query.
func (s *Server) auditColumnPolicy(identity *auth.Identity, table string, policy *auth.AccessPolicy) {
	if s.audit == nil || policy == nil {
		return
	}
	var masked, denied []string
	for col, cp := range policy.Columns {
		switch cp {
		case auth.ColumnMask:
			masked = append(masked, col)
		case auth.ColumnDeny:
			denied = append(denied, col)
		}
	}
	s.audit.LogColumnPolicy(identity, table, masked, denied)
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

// handleCreateTableSQL handles CREATE TABLE via SQL.
func (s *Server) handleCreateTableSQL(w http.ResponseWriter, r *http.Request, parsed *plansql.ParsedQuery, start time.Time) {
	ct := parsed.CreateTable

	// Check write permission
	identity := auth.IdentityFromContext(r.Context())
	if identity != nil {
		if authz := s.getAuthz(); authz != nil && !authz.HasPermission(identity, "write") {
			writeError(w, http.StatusForbidden, "insufficient permissions to create tables")
			return
		}
	}

	schema, err := columnDefsToSchema(ct.Columns)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.catalog.CreateTable(r.Context(), ct.Name, schema, ct.PartitionKeys); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, QueryResponse{
		QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": fmt.Sprintf("Table %q created", ct.Name)}},
		Stats:   QueryStats{Elapsed: time.Since(start).String()},
	})
}

// handleDropTableSQL handles DROP TABLE via SQL.
func (s *Server) handleDropTableSQL(w http.ResponseWriter, r *http.Request, parsed *plansql.ParsedQuery, start time.Time) {
	dt := parsed.DropTable

	// Check write permission
	identity := auth.IdentityFromContext(r.Context())
	if identity != nil {
		if authz := s.getAuthz(); authz != nil && !authz.HasPermission(identity, "write") {
			writeError(w, http.StatusForbidden, "insufficient permissions to drop tables")
			return
		}
	}

	err := s.catalog.DropTable(r.Context(), dt.Name)
	if err != nil {
		if dt.IfExists {
			writeJSON(w, http.StatusOK, QueryResponse{
				QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
				Columns: []string{"result"},
				Rows:    []map[string]any{{"result": fmt.Sprintf("Table %q does not exist (no-op)", dt.Name)}},
				Stats:   QueryStats{Elapsed: time.Since(start).String()},
			})
			return
		}
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, QueryResponse{
		QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": fmt.Sprintf("Table %q dropped", dt.Name)}},
		Stats:   QueryStats{Elapsed: time.Since(start).String()},
	})
}

// handleShowTables handles SHOW TABLES via SQL.
func (s *Server) handleShowTables(w http.ResponseWriter, r *http.Request, start time.Time) {
	tables, err := s.catalog.ListTables(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Filter by role access
	if identity := auth.IdentityFromContext(r.Context()); identity != nil {
		if authz := s.getAuthz(); authz != nil {
			tables = authz.FilterTables(identity, tables)
		}
	}

	rows := make([]map[string]any, len(tables))
	for i, t := range tables {
		rows[i] = map[string]any{"table_name": t}
	}

	writeJSON(w, http.StatusOK, QueryResponse{
		QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
		Columns: []string{"table_name"},
		Rows:    rows,
		Stats:   QueryStats{Elapsed: time.Since(start).String()},
	})
}

// CreateTableRequest is the request body for POST /v1/tables.
type CreateTableRequest struct {
	Name          string              `json:"name"`
	Columns       []CreateTableColumn `json:"columns"`
	PartitionKeys []string            `json:"partition_keys,omitempty"`
}

// CreateTableColumn defines a column in a REST table creation request.
type CreateTableColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable *bool  `json:"nullable,omitempty"` // default true
}

// handleCreateTable handles POST /v1/tables (REST endpoint).
func (s *Server) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	// Check write permission
	identity := auth.IdentityFromContext(r.Context())
	if identity != nil {
		if authz := s.getAuthz(); authz != nil && !authz.HasPermission(identity, "write") {
			writeError(w, http.StatusForbidden, "insufficient permissions to create tables")
			return
		}
	}

	var req CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "table name is required")
		return
	}
	if len(req.Columns) == 0 {
		writeError(w, http.StatusBadRequest, "at least one column is required")
		return
	}

	// Convert to ColumnDef for reuse
	defs := make([]plansql.ColumnDef, len(req.Columns))
	for i, c := range req.Columns {
		nullable := true
		if c.Nullable != nil {
			nullable = *c.Nullable
		}
		defs[i] = plansql.ColumnDef{
			Name:     c.Name,
			Type:     c.Type,
			Nullable: nullable,
		}
	}

	schema, err := columnDefsToSchema(defs)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.catalog.CreateTable(r.Context(), req.Name, schema, req.PartitionKeys); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"result": fmt.Sprintf("Table %q created", req.Name),
	})
}

// handleDeleteTable handles DELETE /v1/tables/{name}.
func (s *Server) handleDeleteTable(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	// Check write permission
	identity := auth.IdentityFromContext(r.Context())
	if identity != nil {
		if authz := s.getAuthz(); authz != nil && !authz.HasPermission(identity, "write") {
			writeError(w, http.StatusForbidden, "insufficient permissions to drop tables")
			return
		}
	}

	if err := s.catalog.DropTable(r.Context(), name); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"result": fmt.Sprintf("Table %q dropped", name),
	})
}

// columnDefsToSchema converts parsed column definitions to a parquet.Schema.
func columnDefsToSchema(defs []plansql.ColumnDef) (parquet.Schema, error) {
	columns := make([]parquet.Column, len(defs))
	for i, d := range defs {
		typeID, err := parquet.ParseTypeID(d.Type)
		if err != nil {
			return parquet.Schema{}, fmt.Errorf("column %q: %w", d.Name, err)
		}
		columns[i] = parquet.Column{
			Name:     strings.ToLower(d.Name),
			Type:     typeID,
			Nullable: d.Nullable,
		}
	}
	return parquet.Schema{Columns: columns}, nil
}

// Ensure batch is used (it's referenced via types flowing through)
var _ = batch.DefaultBatchSize
