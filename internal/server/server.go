// Package server provides the HTTP API for Wadjet.
package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"golang.org/x/net/netutil"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/metrics"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// Config holds server configuration.
type Config struct {
	Addr                string
	Catalog             *catalog.Catalog
	Coordinator         *coordinator.Coordinator       // nil = local execution only
	DLQ                 *coordinator.DLQ               // nil = no DLQ (standalone mode)
	Auth                *auth.Authenticator            // nil = no authentication (static mode)
	Authz               *auth.Authorizer               // nil = no authorization (static mode)
	Policies            *auth.PolicySet                // nil = no cell-level policies (static mode)
	Provider            *auth.Provider                 // nil = use static Auth/Authz/Policies above
	Metrics             *metrics.Metrics               // nil = no metrics collection
	TLSConfig           *tls.Config                    // nil = plain HTTP
	MaxConnections      int                            // 0 = unlimited
	SlowQueryThreshold  time.Duration                  // 0 = disabled, log queries exceeding this
	ShutdownTimeout     time.Duration                  // graceful shutdown drain timeout (default 30s)
	QueryLimits         *config.QueryLimits            // global cost-based query limits (nil = unlimited)
	RoleLimits          map[string]*config.QueryLimits // per-role overrides (nil = use global)
	SortMergeJoinBytes  int64                          // local sort-merge-join gate (0 = disabled)
	LateMaterialization bool                           // view-column join output, deferred gather (default off)
}

// Server is the Wadjet HTTP API server.
type Server struct {
	config   Config
	catalog  *catalog.Catalog
	coord    *coordinator.Coordinator // nil = local execution
	dlq      *coordinator.DLQ         // nil = no DLQ
	logger   *slog.Logger
	mux      chi.Router
	server   *http.Server
	provider *auth.Provider // hot-reloadable auth (nil = static)
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
		coord:    cfg.Coordinator,
		dlq:      cfg.DLQ,
		logger:   logger,
		mux:      chi.NewRouter(),
		provider: cfg.Provider,
		authz:    cfg.Authz,
		policies: cfg.Policies,
		metrics:  cfg.Metrics,
		audit:    auth.NewAuditLogger(logger),
	}

	// Middleware BEFORE routes: chi v5 panics ("all middlewares must be
	// defined before routes on a mux") when Use runs after the first route
	// is registered, so the auth middleware cannot be installed in Start()
	// (#801). Everything Use needs — Provider, Auth — is already in cfg
	// here, and routes registered later through Mux() (admin, ops) inherit
	// the stack, which is what an authenticated deployment wants anyway.
	if s.provider != nil {
		// Hot-reloadable auth via Provider
		s.mux.Use(auth.ProviderMiddleware(s.provider, s.logger))
	} else if cfg.Auth != nil && cfg.Auth.Enabled() {
		// Static auth (backwards compatible)
		s.mux.Use(auth.Middleware(cfg.Auth, s.logger))
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
	s.mux.Get("/v1/ready", s.handleReady)
	s.mux.Get("/v1/dlq", s.handleListDLQ)
	s.mux.Get("/v1/dlq/{entryID}", s.handleGetDLQ)
	s.mux.Delete("/v1/dlq", s.handlePurgeDLQ)
	if s.metrics != nil {
		s.mux.Handle("/metrics", s.metrics.Handler())
	}

	s.mux.HandleFunc("/debug/pprof/", pprof.Index)
	s.mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	s.mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	s.mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	s.mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	s.mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	s.mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	s.mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))

	return s
}

// Mux returns the underlying chi router for registering additional routes (e.g. admin API).
func (s *Server) Mux() chi.Router {
	return s.mux
}

// newPlanner creates a fresh Planner for a single request. The Planner carries
// per-query mutable state (scanCounter, scanCache, cteCache) so it must not be
// shared across concurrent requests.
//
// It installs the cost guard here rather than at its callers, the same way
// wadjet.DB.newPlanner does: the EXPLAIN caller below used to omit it, and
// while EXPLAIN plans without scanning — so no spend escaped — a
// construction site that only sometimes carries the guard is the shape the
// #803 wiring exists to remove.
func (s *Server) newPlanner(r *http.Request) *physical.Planner {
	p := physical.NewPlanner(s.catalog)
	p.SortMergeJoinBytes = s.config.SortMergeJoinBytes
	p.LateMaterialization = s.config.LateMaterialization
	p.QueryLimits = s.resolveQueryLimits(r)
	return p
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	// Auth middleware is installed in New() — see the comment there (#801).
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

	// Parse SQL.
	//
	// writeSQLError, not writeError: a parse failure carries a SQLSTATE
	// (plansql.Parse wraps everything stateless as 42601) and this was the one
	// refusal class on this door that dropped it, so a client could branch on
	// `sqlstate` for a bad column and not for bad syntax. The two-door gate
	// caught it when #711 moved the multi-statement refusal from execute time
	// to parse time and the HTTP door stopped reporting its class.
	parsed, err := plansql.Parse(req.SQL)
	if err != nil {
		writeSQLError(w, http.StatusBadRequest, "SQL parse error: "+err.Error(), err)
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

	// Handle ANALYZE TABLE
	if parsed.Type == plansql.QueryAnalyzeTable {
		s.handleAnalyzeTableSQL(w, r, parsed, start)
		return
	}

	// Handle SHOW TABLES
	if parsed.Type == plansql.QueryShowTables {
		s.handleShowTables(w, r, start)
		return
	}

	// Handle DML (INSERT/UPDATE/DELETE)
	if parsed.Type == plansql.QueryInsert || parsed.Type == plansql.QueryUpdate || parsed.Type == plansql.QueryDelete || parsed.Type == plansql.QueryMerge {
		s.handleDML(w, r, req.SQL, start)
		return
	}

	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		writeSQLError(w, http.StatusBadRequest, "SQL extraction error: "+err.Error(), err)
		return
	}

	// Authorization: ABAC evaluation with legacy RBAC fallback
	identity := auth.IdentityFromContext(r.Context())
	var rowFilters auth.RowFilters
	var tableDecisions auth.TableDecisions

	if identity != nil {
		evaluator := s.getEvaluator()
		if evaluator != nil {
			// ABAC path: evaluate policies per table
			subject := identity.ToSubject()
			env := auth.Environment{
				SourceIP: r.RemoteAddr,
				Protocol: "http",
			}
			tableDecisions = make(auth.TableDecisions)

			// Only the FROM-list names the catalog knows as TABLES: a
			// derived table is listed under its own subquery text and a CTE
			// reference under the CTE's name, and handing those to a
			// default-deny evaluator refused every query containing one
			// (#859). The base tables behind them are policed by
			// auth.EnforcePlanPolicies, which walks the PLAN.
			for _, tableName := range auth.StatementBaseTables(r.Context(), s.catalog, selectInfo) {
				td := evaluator.EvaluateTableAccess(subject, tableName, auth.ActionRead, env)
				if !td.Allowed {
					writeError(w, http.StatusForbidden,
						fmt.Sprintf("access denied to table %q: %s", tableName, td.Reason))
					return
				}
				tableDecisions[tableName] = td
				// Audit the column policy at DECISION time. It used to be
				// logged from applyColumnPolicies, over the result rows, so a
				// query that returned nothing logged nothing even though the
				// policy had shaped the plan (#859).
				if s.audit != nil {
					var masked, denied []string
					for _, col := range td.Columns {
						if !col.Allowed {
							denied = append(denied, col.Column)
						} else if col.MaskFunc != "" || col.MaskExpr != "" {
							masked = append(masked, col.Column)
						}
					}
					if len(masked) > 0 || len(denied) > 0 {
						s.audit.LogColumnPolicy(identity, tableName, masked, denied)
					}
				}
				if td.RowFilter != "" {
					if rowFilters == nil {
						rowFilters = make(auth.RowFilters)
					}
					rowFilters[tableName] = td.RowFilter
					if s.audit != nil {
						s.audit.LogRowFilterApplied(identity, tableName, td.RowFilter)
					}
				}
			}
		} else if authz := s.getAuthz(); authz != nil {
			// Legacy RBAC fallback
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

			// Legacy row filter collection
			policies := s.getPolicies()
			if policies != nil {
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
		}
	}

	// Distributed execution path: use coordinator if available
	if s.coord != nil {
		// Pass row filters, table decisions, and identity through context
		execCtx := r.Context()
		if len(rowFilters) > 0 {
			execCtx = auth.ContextWithRowFilters(execCtx, rowFilters)
		}
		if len(tableDecisions) > 0 {
			execCtx = auth.ContextWithTableDecisions(execCtx, tableDecisions)
		}
		result, err := s.coord.ExecuteSQL(execCtx, req.SQL)
		if err != nil {
			writeSQLError(w, http.StatusInternalServerError, "distributed execution error: "+err.Error(), err)
			return
		}

		rows, rowsErr := result.Rows()
		if rowsErr != nil {
			writeSQLError(w, http.StatusInternalServerError, "reading result batches: "+rowsErr.Error(), rowsErr)
			return
		}

		// No result-row masking here. Column policy is enforced at the SCAN,
		// inside coord.ExecuteSQL, for every consumer above it (#859) — a
		// second pass over the rows could only ever mask an output column
		// that still carries the policed column's name, and never the
		// aggregate, group key or join key computed from it.

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
		writeSQLError(w, http.StatusBadRequest, "plan build error: "+err.Error(), err)
		return
	}

	// Annotate scan columns from catalog so optimizer can resolve unqualified refs
	planner := s.newPlanner(r)

	// Reject references to columns that resolve to no source (plan-time name
	// binding), under the calling identity's schema: a denied column is not
	// in this caller's table, so it is not in the "available:" hint (#859).
	if err := auth.ValidateStatementColumns(r.Context(), s.provider, s.catalog, selectInfo, "http"); err != nil {
		writeSQLError(w, http.StatusBadRequest, err.Error(), err)
		return
	}

	execCtx := r.Context()
	planner.AnnotateScanColumns(execCtx, logicalPlan)

	// ABAC at plan level, through the SAME auth.EnforcePlanPolicies the
	// embedded engine and the coordinator use. This door used to plan without
	// it and mask the RESULT ROWS afterwards instead (applyColumnPolicies),
	// which cannot reach a value the pipeline already consumed: `SUM(acct)`,
	// `COUNT(DISTINCT ssn)`, `GROUP BY ssn`, a join on a masked column and
	// `WHERE ssn = '<true value>'` were all answered from the raw column, and
	// only a result column that still carried the policed column's NAME was
	// masked at all (#859). One enforcement path, at the scan, for every door.
	execCtx, logicalPlan, err = auth.EnforcePlanPolicies(execCtx, s.provider, s.catalog,
		selectInfo, logicalPlan, "http")
	if err != nil {
		writeSQLError(w, http.StatusForbidden, err.Error(), err)
		return
	}
	if s.getEvaluator() == nil {
		// Static (non-ABAC) deployment: the legacy PolicySet's row filters,
		// collected above, are the only ones there are.
		for table, filter := range rowFilters {
			logicalPlan = logical.InjectRowFilter(logicalPlan, table, filter)
		}
	}

	// Optimize — pass scan annotator for new scans created during IN decorrelation
	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		planner.AnnotateScanColumns(execCtx, plan)
	})

	// Build physical plan
	physPlan, err := planner.Plan(execCtx, logicalPlan)
	if err != nil {
		writeSQLError(w, http.StatusInternalServerError, "physical plan error: "+err.Error(), err)
		return
	}
	if physPlan.Cleanup != nil {
		defer physPlan.Cleanup()
	}

	// Execute
	pipeline := physPlan.Pipeline
	// Registered before Run: a defer below a failing Run's error check never
	// runs, so an aborted HTTP query kept its spill scratch (#625 M1).
	defer pipeline.Close()
	if err := pipeline.Run(execCtx); err != nil {
		writeSQLError(w, http.StatusInternalServerError, "execution error: "+err.Error(), err)
		return
	}

	// Collect results
	var rows []map[string]any
	if collectSink, ok := pipeline.Sink.(*exec.CollectSink); ok {
		rows = collectSink.ToRows()
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

func (s *Server) handleDML(w http.ResponseWriter, r *http.Request, sql string, start time.Time) {
	ctx := r.Context()

	// Authorization: check write permission
	identity := auth.IdentityFromContext(ctx)
	authz := s.getAuthz()
	if identity != nil && authz != nil {
		if !authz.HasPermission(identity, "write") {
			writeError(w, http.StatusForbidden, "insufficient permissions for DML operation")
			return
		}
	}

	result, err := s.dml().Execute(ctx, sql)
	if err != nil {
		writeSQLError(w, http.StatusInternalServerError, "DML execution error: "+err.Error(), err)
		return
	}

	resp := QueryResponse{
		QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
		Columns: []string{"result"},
		// result.Tag(), not a local format: PostgreSQL's INSERT tag has an oid
		// field and pgwire renders it, so building the tag here made the same
		// statement `INSERT 0 3` over the wire and `INSERT 3` over REST —
		// under a doc line promising the tag does not depend on the door
		// (review B8).
		Rows:  []map[string]any{{"result": result.Tag()}},
		Stats: QueryStats{Elapsed: time.Since(start).String()},
	}

	if s.metrics != nil {
		s.metrics.QueriesTotal.WithLabelValues("success").Inc()
	}
	if done := func() {}; done != nil {
		done()
	}
	s.logSlowQuery(sql, time.Since(start), 0)

	writeJSON(w, http.StatusOK, resp)
}

// dml returns the DML door: the SAME implementation the embedded and pgwire
// doors run.
//
// This server used to carry its own INSERT/UPDATE/DELETE executors over the
// catalog, and no MERGE — a second copy that had drifted into the same defects
// as the first and had to be fixed twice or left wrong (#815). wadjet.Attach
// wraps the catalog this server already owns, so there is one executor, one
// command tag and one SQLSTATE per statement whichever door a user reaches.
//
// The panic boundary the old copy installed lives inside DB.Execute
// (RecoverQueryPanic, #677), so a statement that panics still reaches the
// client as a SQLSTATE rather than as a dropped connection.
func (s *Server) dml() *wadjet.DB {
	db := wadjet.Attach(s.catalog)
	// The cost guard #803 installed on this server's SELECT path. MERGE reads
	// its source through db.Query — an arbitrary SELECT — so a door that
	// gained MERGE here would otherwise run one with no limit at all, on the
	// one entry point #803 was filed about (review P3). A door that runs an
	// unbounded SELECT is the defect that flag exists to prevent.
	db.SetQueryLimits(s.config.QueryLimits, s.config.RoleLimits)
	return db
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

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	resp := map[string]any{"status": "ready"}
	code := http.StatusOK

	// Check catalog availability
	if s.catalog != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := s.catalog.ListTables(ctx); err != nil {
			resp["status"] = "not_ready"
			resp["catalog"] = err.Error()
			code = http.StatusServiceUnavailable
		}
	}

	// Check worker availability (distributed mode)
	if s.coord != nil {
		workers := s.coord.Workers().Count()
		resp["workers"] = workers
		if workers == 0 {
			resp["status"] = "not_ready"
			resp["workers_error"] = "no active workers"
			code = http.StatusServiceUnavailable
		}
	}

	writeJSON(w, code, resp)
}

func (s *Server) handleListDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		writeJSON(w, http.StatusOK, map[string]any{"entries": []any{}, "count": 0})
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	entries, err := s.dlq.List(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if entries == nil {
		entries = []distributed.DLQEntry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries, "count": len(entries)})
}

func (s *Server) handleGetDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		writeError(w, http.StatusNotFound, "DLQ not available")
		return
	}
	entryID := chi.URLParam(r, "entryID")
	entry, err := s.dlq.Get(r.Context(), entryID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, entry)
}

func (s *Server) handlePurgeDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	if err := s.dlq.Purge(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "purged"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// writeSQLError is writeError for a failure that may carry a SQLSTATE, and it
// is the ONLY place this door decides an HTTP status from one.
//
// Every refusal a STATEMENT earns comes through here — handleQuery's own
// paths, handleDML's, handleExplain's, and the DDL sub-handlers the same POST
// reaches by statement type. What does not, and correctly does not, is a
// failure the statement did not cause: a malformed request body, a missing
// `sql` field, and an authorization denial, none of which carry a SQLSTATE to
// report. The round-1 review blocked on this claim being written wider than
// the code: handleExplain's name resolution still called writeError, so
// `EXPLAIN SELECT nosuchcol FROM t` dropped the 42703 the same statement
// reports without the EXPLAIN.
//
// A statement refused for what it CONTAINS is the client's error, not the
// server's: a DECIMAL literal past the column's precision (22003) or text
// naming no number (22P02) came back as 500 Internal Server Error with the
// code nowhere in the body, so an HTTP client could neither see that its own
// input was wrong nor branch on why (#647 re-review). #848 is the same defect
// one class wider: `SELECT nosuchcol FROM t` and `SELECT * FROM nosuchtable`
// answered 500 with no `sqlstate` at all, because the name-resolution, plan
// and execution paths still called writeError. Every refusal on this door now
// comes through here.
//
// The class → status table, which is also docs/api-reference.md's:
//
//	0A  feature not supported            400
//	22  data exception                   400  (2201x, 22003, 22012, 22P02, …)
//	23  integrity constraint violation   400
//	42  syntax error / access rule       400  (42601, 42703, 42P01, 42883, …)
//	anything else                        the caller's status — 500 for XX
//	                                     (internal), 58 (storage/system) and
//	                                     any class this engine has not placed.
//
// The promotion applies to a SERVER status only: a caller that chose 5xx was
// blaming the server for something the client's statement caused, which is
// the whole defect. A caller that chose 404 or 409 made a considered
// statement about the RESOURCE — `DESCRIBE nope` is 404 and a duplicate
// CREATE TABLE is 409 — and that stands, with the class now beside it. The
// first round-2 pass promoted those too and turned `DESCRIBE nope` from 404
// into 400, which is a contract change no issue asked for.
//
// The MESSAGE for a classified error is err.Error() verbatim: the same bytes
// the pgwire door puts in the ErrorResponse 'M' field for the same statement,
// so a client that reads one door's message can compare it with the other's.
// The caller's contextual prefix ("execution error: ") survives only on the
// unclassified path, where naming the stage is the only localization there is.
//
// There is deliberately no second SQLSTATE table here: the code comes from
// sqlerr.StateOf, which is the same call pgwire's sendQueryError makes.
func writeSQLError(w http.ResponseWriter, status int, msg string, err error) {
	state := sqlerr.StateOf(err)
	if state == "" {
		writeError(w, status, msg)
		return
	}
	if status >= http.StatusInternalServerError && sqlStateIsClientFault(state) {
		status = http.StatusBadRequest
	}
	writeJSON(w, status, map[string]string{"error": err.Error(), "sqlstate": state})
}

// sqlStateIsClientFault reports whether a SQLSTATE names something wrong with
// the statement rather than with the server. See writeSQLError's table.
func sqlStateIsClientFault(state string) bool {
	switch {
	case strings.HasPrefix(state, "0A"),
		strings.HasPrefix(state, "22"),
		strings.HasPrefix(state, "23"),
		strings.HasPrefix(state, "42"):
		return true
	}
	return false
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

func (s *Server) getEvaluator() *auth.PolicyEvaluator {
	if s.provider != nil {
		return s.provider.Evaluator()
	}
	return nil
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
		writeSQLError(w, http.StatusNotFound, fmt.Sprintf("table %q: %s", tableName, err.Error()), err)
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
		writeSQLError(w, http.StatusBadRequest, "SQL extraction error: "+err.Error(), err)
		return
	}

	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		writeSQLError(w, http.StatusBadRequest, "plan build error: "+err.Error(), err)
		return
	}
	planner := s.newPlanner(r)
	if err := auth.ValidateStatementColumns(r.Context(), s.provider, s.catalog, selectInfo, "http"); err != nil {
		writeSQLError(w, http.StatusBadRequest, err.Error(), err)
		return
	}
	planner.AnnotateScanColumns(r.Context(), logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		planner.AnnotateScanColumns(r.Context(), plan)
	})
	planStr := logicalPlan.PrettyPrint(0)

	if parsed.Explain.Verbose {
		physPlan, err := planner.Plan(r.Context(), logicalPlan)
		if err != nil {
			writeSQLError(w, http.StatusInternalServerError, "physical plan error: "+err.Error(), err)
			return
		}
		// The pipeline is never run for EXPLAIN, but planning may have
		// materialized CTEs to spill scratch — release it now.
		if physPlan.Cleanup != nil {
			physPlan.Cleanup()
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
		writeSQLError(w, http.StatusBadRequest, err.Error(), err)
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
		writeSQLError(w, http.StatusBadRequest, err.Error(), err)
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
		writeSQLError(w, http.StatusBadRequest, err.Error(), err)
		return
	}

	if err := s.catalog.CreateTable(r.Context(), ct.Name, schema, ct.PartitionKeys); err != nil {
		writeSQLError(w, http.StatusConflict, err.Error(), err)
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
// handleAnalyzeTableSQL refreshes planner column statistics (HLL NDV +
// histograms) for a table — the SQL surface for catalog.AnalyzeTable.
func (s *Server) handleAnalyzeTableSQL(w http.ResponseWriter, r *http.Request, parsed *plansql.ParsedQuery, start time.Time) {
	at := parsed.AnalyzeTable

	identity := auth.IdentityFromContext(r.Context())
	if identity != nil {
		if authz := s.getAuthz(); authz != nil && !authz.HasPermission(identity, "write") {
			writeError(w, http.StatusForbidden, "insufficient permissions to analyze tables")
			return
		}
	}

	n, err := s.catalog.AnalyzeTable(r.Context(), at.Name)
	if err != nil {
		writeSQLError(w, http.StatusNotFound, err.Error(), err)
		return
	}

	writeJSON(w, http.StatusOK, QueryResponse{
		QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": fmt.Sprintf("Table %q analyzed (%d files)", at.Name, n)}},
		Stats:   QueryStats{Elapsed: time.Since(start).String()},
	})
}

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
		writeSQLError(w, http.StatusNotFound, err.Error(), err)
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
		// parquet.DeclaredColumn, not ParseTypeID: a DECIMAL's precision and
		// scale live in the type text, and reading only the TypeID here gave
		// every DECIMAL column created over HTTP a Precision 0, Scale 0
		// declaration (#647 review).
		col, err := parquet.DeclaredColumn(d.Name, d.Type, d.Nullable)
		if err != nil {
			return parquet.Schema{}, fmt.Errorf("column %q: %w", d.Name, err)
		}
		columns[i] = col
	}
	return parquet.Schema{Columns: columns}, nil
}
