// Package server provides the HTTP API for Wadjet.
package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"golang.org/x/net/netutil"

	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/config"
	"github.com/citc-tech/wadjet/internal/coordinator"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/expr"
	"github.com/citc-tech/wadjet/internal/engine/scan"
	"github.com/citc-tech/wadjet/internal/metrics"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Config holds server configuration.
type Config struct {
	Addr              string
	Catalog           *catalog.Catalog
	Coordinator       *coordinator.Coordinator // nil = local execution only
	DLQ               *coordinator.DLQ         // nil = no DLQ (standalone mode)
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
	SortMergeJoinBytes int64                    // local sort-merge-join gate (0 = disabled)
	LateMaterialization bool                    // view-column join output, deferred gather (default off)
}

// Server is the Wadjet HTTP API server.
type Server struct {
	config   Config
	catalog *catalog.Catalog
	coord   *coordinator.Coordinator // nil = local execution
	dlq      *coordinator.DLQ         // nil = no DLQ
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
		catalog: cfg.Catalog,
		coord:   cfg.Coordinator,
		dlq:      cfg.DLQ,
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
func (s *Server) newPlanner() *physical.Planner {
	p := physical.NewPlanner(s.catalog)
	p.SortMergeJoinBytes = s.config.SortMergeJoinBytes
	p.LateMaterialization = s.config.LateMaterialization
	return p
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
		writeError(w, http.StatusBadRequest, "SQL extraction error: "+err.Error())
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

			allTables := make([]string, 0, len(selectInfo.Tables)+len(selectInfo.Joins))
			for _, t := range selectInfo.Tables {
				allTables = append(allTables, t.Name)
			}
			for _, j := range selectInfo.Joins {
				allTables = append(allTables, j.RightTable)
			}

			for _, tableName := range allTables {
				td := evaluator.EvaluateTableAccess(subject, tableName, auth.ActionRead, env)
				if !td.Allowed {
					writeError(w, http.StatusForbidden,
						fmt.Sprintf("access denied to table %q: %s", tableName, td.Reason))
					return
				}
				tableDecisions[tableName] = td
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
			writeError(w, http.StatusInternalServerError, "distributed execution error: "+err.Error())
			return
		}

		rows, rowsErr := result.Rows()
		if rowsErr != nil {
			writeError(w, http.StatusInternalServerError, "reading result batches: "+rowsErr.Error())
			return
		}

		// Apply cell-level access policies (column masking/denial)
		if identity != nil && len(rows) > 0 {
			tableName := ""
			if len(selectInfo.Tables) > 0 {
				tableName = selectInfo.Tables[0].Name
			}
			rows = s.applyColumnPolicies(identity, tableName, tableDecisions, rows)
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

	// Annotate scan columns from catalog so optimizer can resolve unqualified refs
	planner := s.newPlanner()

	// Reject references to columns that resolve to no source (plan-time name binding).
	if err := planner.ValidateColumns(r.Context(), selectInfo); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	planner.AnnotateScanColumns(r.Context(), logicalPlan)

	// Optimize — pass scan annotator for new scans created during IN decorrelation
	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		planner.AnnotateScanColumns(r.Context(), plan)
	})

	// Apply query cost limits (per-role or global)
	planner.QueryLimits = s.resolveQueryLimits(r)

	// Build physical plan
	physPlan, err := planner.Plan(r.Context(), logicalPlan)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "physical plan error: "+err.Error())
		return
	}
	if physPlan.Cleanup != nil {
		defer physPlan.Cleanup()
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
	if identity != nil && len(rows) > 0 {
		tableName := ""
		if len(selectInfo.Tables) > 0 {
			tableName = selectInfo.Tables[0].Name
		}
		rows = s.applyColumnPolicies(identity, tableName, tableDecisions, rows)
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

	parsed, err := plansql.Parse(sql)
	if err != nil {
		writeError(w, http.StatusBadRequest, "SQL parse error: "+err.Error())
		return
	}

	var result *dmlResult
	switch parsed.Type {
	case plansql.QueryInsert:
		result, err = executeDMLInsert(ctx, s.catalog, parsed.Insert)
	case plansql.QueryUpdate:
		result, err = executeDMLUpdate(ctx, s.catalog, parsed.Update)
	case plansql.QueryDelete:
		result, err = executeDMLDelete(ctx, s.catalog, parsed.Delete)
	default:
		writeError(w, http.StatusBadRequest, "unsupported DML type")
		return
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, "DML execution error: "+err.Error())
		return
	}

	resp := QueryResponse{
		QueryID: fmt.Sprintf("q-%d", start.UnixMilli()),
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": fmt.Sprintf("%s %d", result.command, result.rowsAffected)}},
		Stats:   QueryStats{Elapsed: time.Since(start).String()},
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

type dmlResult struct {
	rowsAffected int64
	command      string
}

func executeDMLInsert(ctx context.Context, cat *catalog.Catalog, info *plansql.InsertInfo) (*dmlResult, error) {
	tableMeta, err := cat.GetTable(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("table %q: %w", info.Table, err)
	}

	columns := info.Columns
	if len(columns) == 0 {
		columns = make([]string, len(tableMeta.Schema.Columns))
		for i, col := range tableMeta.Schema.Columns {
			columns[i] = col.Name
		}
	}

	typeMap := make(map[string]parquet.TypeID, len(tableMeta.Schema.Columns))
	for _, col := range tableMeta.Schema.Columns {
		typeMap[col.Name] = col.Type
	}

	var rows []map[string]any
	for rowIdx, vals := range info.Values {
		if len(vals) != len(columns) {
			return nil, fmt.Errorf("row %d: expected %d values, got %d", rowIdx, len(columns), len(vals))
		}
		row := make(map[string]any, len(columns))
		for i, colName := range columns {
			v, err := convertDMLValue(vals[i], typeMap[colName])
			if err != nil {
				return nil, fmt.Errorf("row %d, column %q: %w", rowIdx, colName, err)
			}
			row[colName] = v
		}
		rows = append(rows, row)
	}

	ing := ingest.New(cat, info.Table, tableMeta.Schema, tableMeta.PartitionKeys, ingest.DefaultConfig())
	if err := ing.Ingest(ctx, rows); err != nil {
		return nil, fmt.Errorf("ingesting rows: %w", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		return nil, fmt.Errorf("flushing rows: %w", err)
	}

	return &dmlResult{rowsAffected: int64(len(rows)), command: "INSERT"}, nil
}

func executeDMLDelete(ctx context.Context, cat *catalog.Catalog, info *plansql.DeleteInfo) (*dmlResult, error) {
	tableMeta, err := cat.GetTable(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("table %q: %w", info.Table, err)
	}

	manifest, err := cat.GetManifest(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}

	var predicate func(b *batch.RecordBatch, row int) bool
	if info.WhereSQL != "" {
		node, parseErr := plansql.ParseExpression(info.WhereSQL)
		if parseErr != nil {
			return nil, fmt.Errorf("parsing WHERE: %w", parseErr)
		}
		compiled, compErr := expr.Compile(node)
		if compErr != nil {
			return nil, fmt.Errorf("compiling WHERE: %w", compErr)
		}
		predicate = func(b *batch.RecordBatch, row int) bool {
			v := compiled.Eval(b, row)
			bv, ok := v.(bool)
			return ok && bv
		}
	}

	var totalDeleted int64
	var markers []catalog.DeleteMarker
	schema := tableMeta.Schema.Columns

	for _, part := range manifest.Partitions {
		for _, file := range part.Files {
			b, err := readDMLFile(ctx, cat, file.Path, schema)
			if err != nil {
				return nil, fmt.Errorf("reading %s: %w", file.Path, err)
			}
			if b == nil {
				continue
			}
			var indices []int64
			for i := 0; i < b.Len; i++ {
				if predicate == nil || predicate(b, i) {
					indices = append(indices, int64(i))
				}
			}
			if len(indices) > 0 {
				markers = append(markers, catalog.DeleteMarker{FilePath: file.Path, RowIndices: indices})
				totalDeleted += int64(len(indices))
			}
		}
	}

	if len(markers) > 0 {
		if err := cat.AddDeleteMarkers(ctx, info.Table, markers); err != nil {
			return nil, fmt.Errorf("recording delete markers: %w", err)
		}
	}

	return &dmlResult{rowsAffected: totalDeleted, command: "DELETE"}, nil
}

func executeDMLUpdate(ctx context.Context, cat *catalog.Catalog, info *plansql.UpdateInfo) (*dmlResult, error) {
	tableMeta, err := cat.GetTable(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("table %q: %w", info.Table, err)
	}

	manifest, err := cat.GetManifest(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}

	var predicate func(b *batch.RecordBatch, row int) bool
	if info.WhereSQL != "" {
		node, parseErr := plansql.ParseExpression(info.WhereSQL)
		if parseErr != nil {
			return nil, fmt.Errorf("parsing WHERE: %w", parseErr)
		}
		compiled, compErr := expr.Compile(node)
		if compErr != nil {
			return nil, fmt.Errorf("compiling WHERE: %w", compErr)
		}
		predicate = func(b *batch.RecordBatch, row int) bool {
			v := compiled.Eval(b, row)
			bv, ok := v.(bool)
			return ok && bv
		}
	}

	typeMap := make(map[string]parquet.TypeID, len(tableMeta.Schema.Columns))
	for _, col := range tableMeta.Schema.Columns {
		typeMap[col.Name] = col.Type
	}

	schema := tableMeta.Schema.Columns
	var totalUpdated int64
	var ing *ingest.Ingester

	// Per-file streaming: box only the matched rows (the previous ToRows
	// boxed every row of every file even at zero WHERE selectivity), commit
	// that file's delete markers, then ingest its updated rows so the
	// ingester's auto-flush bounds memory — accumulating updatedRows
	// table-wide held the whole table as boxed maps on a broad UPDATE.
	// Markers commit before the matching re-ingest, preserving the original
	// failure direction (a crash loses that file's updated rows; it never
	// duplicates them). The transactional marker+ingest commit is a known
	// separate issue.
	for _, part := range manifest.Partitions {
		for _, file := range part.Files {
			b, err := readDMLFile(ctx, cat, file.Path, schema)
			if err != nil {
				return nil, fmt.Errorf("reading %s: %w", file.Path, err)
			}
			if b == nil {
				continue
			}
			var indices []int64
			for i := 0; i < b.Len; i++ {
				if predicate == nil || predicate(b, i) {
					indices = append(indices, int64(i))
				}
			}
			if len(indices) == 0 {
				continue
			}
			updatedRows := make([]map[string]any, 0, len(indices))
			for _, idx := range indices {
				row := b.RowAt(int(idx))
				for _, sc := range info.SetClauses {
					v, convErr := convertDMLValue(sc.Value, typeMap[sc.Column])
					if convErr != nil {
						return nil, fmt.Errorf("SET %s: %w", sc.Column, convErr)
					}
					row[sc.Column] = v
				}
				updatedRows = append(updatedRows, row)
			}
			marker := catalog.DeleteMarker{FilePath: file.Path, RowIndices: indices}
			if err := cat.AddDeleteMarkers(ctx, info.Table, []catalog.DeleteMarker{marker}); err != nil {
				return nil, fmt.Errorf("recording delete markers: %w", err)
			}
			if ing == nil {
				ing = ingest.New(cat, info.Table, tableMeta.Schema, tableMeta.PartitionKeys, ingest.DefaultConfig())
			}
			if err := ing.Ingest(ctx, updatedRows); err != nil {
				return nil, fmt.Errorf("inserting updated rows: %w", err)
			}
			totalUpdated += int64(len(indices))
		}
	}

	if ing != nil {
		if err := ing.FlushAll(ctx); err != nil {
			return nil, fmt.Errorf("flushing updated rows: %w", err)
		}
	}

	return &dmlResult{rowsAffected: totalUpdated, command: "UPDATE"}, nil
}

func readDMLFile(ctx context.Context, cat *catalog.Catalog, filePath string, schema []parquet.Column) (*batch.RecordBatch, error) {
	store := cat.Store()
	if ras, ok := store.(objstore.ReaderAtStore); ok {
		ra, size, err := ras.GetReaderAt(ctx, cat.Bucket(), filePath)
		if err != nil {
			return nil, err
		}
		defer ra.Close()
		reader, err := parquet.NewReader(ra, size)
		if err != nil {
			return nil, err
		}
		return scan.ReadFileColumnar(reader, schema)
	}

	rc, _, err := store.Get(ctx, cat.Bucket(), filePath)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, rc); err != nil {
		return nil, err
	}
	data := buf.Bytes()
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	return scan.ReadFileColumnar(reader, schema)
}

func convertDMLValue(s string, typ parquet.TypeID) (any, error) {
	s = strings.TrimSpace(s)
	if strings.EqualFold(s, "null") {
		return nil, nil
	}
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		s = s[1 : len(s)-1]
	}
	switch typ {
	case parquet.TypeBool:
		return strconv.ParseBool(s)
	case parquet.TypeInt32:
		v, err := strconv.ParseInt(s, 10, 32)
		return int32(v), err
	case parquet.TypeInt64:
		return strconv.ParseInt(s, 10, 64)
	case parquet.TypeFloat32:
		v, err := strconv.ParseFloat(s, 32)
		return float32(v), err
	case parquet.TypeFloat64:
		return strconv.ParseFloat(s, 64)
	case parquet.TypeString:
		return s, nil
	case parquet.TypeTimestamp:
		for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02"} {
			if t, err := time.Parse(layout, s); err == nil {
				return t, nil
			}
		}
		return nil, fmt.Errorf("cannot parse timestamp %q", s)
	case parquet.TypeDate:
		t, err := time.Parse("2006-01-02", s)
		return t, err
	default:
		return s, nil
	}
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

// auditColumnPolicy logs which columns were masked or denied for this query.
// applyColumnPolicies applies ABAC table decisions or falls back to legacy PolicySet
// for column masking/denial on result rows.
func (s *Server) applyColumnPolicies(identity *auth.Identity, tableName string, decisions auth.TableDecisions, rows []map[string]any) []map[string]any {
	if len(rows) == 0 {
		return rows
	}

	// ABAC path: use pre-evaluated table decisions
	if td, ok := decisions[tableName]; ok && len(td.Columns) > 0 {
		var masked, denied []string
		for _, col := range td.Columns {
			if !col.Allowed {
				denied = append(denied, col.Column)
			} else if col.MaskFunc != "" {
				masked = append(masked, col.Column)
			}
		}
		if len(masked) > 0 || len(denied) > 0 {
			if s.audit != nil {
				s.audit.LogColumnPolicy(identity, tableName, masked, denied)
			}
			// Build deny/mask sets for fast lookup
			denySet := make(map[string]bool, len(denied))
			for _, c := range denied {
				denySet[c] = true
			}
			maskSet := make(map[string]bool, len(masked))
			for _, c := range masked {
				maskSet[c] = true
			}
			result := make([]map[string]any, len(rows))
			for i, row := range rows {
				filtered := make(map[string]any, len(row))
				for col, val := range row {
					if denySet[col] {
						continue
					}
					if maskSet[col] {
						filtered[col] = defaultMaskValue(val)
					} else {
						filtered[col] = val
					}
				}
				result[i] = filtered
			}
			return result
		}
		return rows
	}

	// Legacy fallback: use PolicySet
	if policies := s.getPolicies(); policies != nil {
		if policy := policies.Lookup(tableName, identity.Role); policy != nil {
			rows = policy.ApplyToRows(rows)
			s.auditColumnPolicy(identity, tableName, policy)
		}
	}
	return rows
}

// defaultMaskValue returns a redacted placeholder based on value type.
func defaultMaskValue(val any) any {
	if val == nil {
		return nil
	}
	switch val.(type) {
	case string:
		return "***"
	case int, int32, int64:
		return int64(0)
	case float32, float64:
		return float64(0)
	case bool:
		return false
	default:
		return "***"
	}
}

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
	planner := s.newPlanner()
	if err := planner.ValidateColumns(r.Context(), selectInfo); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
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
			writeError(w, http.StatusInternalServerError, "physical plan error: "+err.Error())
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
		writeError(w, http.StatusNotFound, err.Error())
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
