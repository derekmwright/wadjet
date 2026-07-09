// Package wadjet provides the public embeddable API for Wadjet.
package wadjet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/alerts"
	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/expr"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// DB is the main entry point for embedded usage of Wadjet.
type DB struct {
	store              objstore.Store
	catalog            *catalog.Catalog
	bucket             string
	memoryBudget       int64
	spillDir           string
	logger             *slog.Logger
	authProvider       *auth.Provider    // nil = no auth enforcement
	alertScheduler     *alerts.Scheduler // non-nil when EnableAlerts is set
	alertSchedulerStop context.CancelFunc
	sortMergeJoinBytes int64
	lateMaterialization bool
}

// Config holds configuration for creating a DB instance.
type Config struct {
	Store        objstore.Store
	Bucket       string
	Logger       *slog.Logger
	MetaKV       catalog.MetaKV // optional: NATS KV for production, nil = in-memory
	MemoryBudget int64          // per-query memory budget in bytes (0 = unlimited)
	SpillDir     string         // directory for spill-to-disk files (empty = os temp dir)
	AuthProvider *auth.Provider // optional: enables ABAC enforcement at query level
	// SortMergeJoinBytes routes inner equi-joins whose sides BOTH exceed this
	// estimated size through the sort-merge join instead of the hash join
	// (docs/design/sort-merge-join.md). 0 = disabled (default).
	SortMergeJoinBytes int64
	// LateMaterialization emits inner/left hash-join output as view
	// (dictionary) columns with the gather deferred to first touch
	// (docs/design/late-materialization.md). Off by default.
	LateMaterialization bool
	// EnableAlerts turns on the CREATE ALERT scheduler in embedded mode.
	// When true, Open() creates a Scheduler that evaluates alerts on cadence.
	EnableAlerts bool
}

// Open creates and initializes a new Wadjet database.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	var cat *catalog.Catalog
	if cfg.MetaKV != nil {
		cat = catalog.New(cfg.MetaKV, cfg.Store, cfg.Bucket)
	} else {
		cat = catalog.NewWithStore(cfg.Store, cfg.Bucket)
	}
	if err := cat.Init(ctx); err != nil {
		return nil, fmt.Errorf("initializing catalog: %w", err)
	}

	db := &DB{
		store:              cfg.Store,
		catalog:            cat,
		bucket:             cfg.Bucket,
		memoryBudget:       cfg.MemoryBudget,
		spillDir:           cfg.SpillDir,
		logger:             cfg.Logger,
		authProvider:       cfg.AuthProvider,
		sortMergeJoinBytes: cfg.SortMergeJoinBytes,
		lateMaterialization: cfg.LateMaterialization,
	}

	if cfg.EnableAlerts {
		ex := &dbExecutor{db: db}
		sinkFactory := alerts.SinkFactory(func(m catalog.AlertMeta) []alerts.AlertSink {
			var sinks []alerts.AlertSink
			if m.WebhookURL != "" {
				sinks = append(sinks, alerts.NewWebhookSink(m.Name, m.WebhookURL, m.WebhookHeaders, 10*time.Second))
			}
			if m.InsertIntoTable != "" {
				sinks = append(sinks, &alerts.TableSink{Executor: ex})
			}
			return sinks
		})
		sched := alerts.NewScheduler(cat, ex, sinkFactory, db.alertEvalDecorator())
		schedCtx, cancel := context.WithCancel(context.Background())
		db.alertScheduler = sched
		db.alertSchedulerStop = cancel
		sched.Start(schedCtx)
	}

	return db, nil
}

// Close shuts down any background goroutines started by Open (e.g. alert scheduler).
// It is safe to call Close multiple times.
func (db *DB) Close() {
	if db.alertSchedulerStop != nil {
		db.alertSchedulerStop()
		db.alertSchedulerStop = nil
	}
	if db.alertScheduler != nil {
		db.alertScheduler.Wait()
		db.alertScheduler = nil
	}
}

// dbExecutor adapts *DB to the alerts.SQLExecutor interface so the alert
// scheduler can run queries and insert history rows without importing coordinator.
type dbExecutor struct{ db *DB }

func (e *dbExecutor) Execute(ctx context.Context, sql string) error {
	_, err := e.db.Execute(ctx, sql)
	return err
}

func (e *dbExecutor) Query(ctx context.Context, sqlText string, limit int) ([]map[string]any, []alerts.ColumnMeta, int64, bool, error) {
	res, err := e.db.Query(ctx, sqlText)
	if err != nil {
		return nil, nil, 0, false, err
	}
	all := res.Rows
	schema := make([]alerts.ColumnMeta, 0, len(res.ColumnMetas))
	for _, cm := range res.ColumnMetas {
		schema = append(schema, alerts.ColumnMeta{Name: cm.Name, Type: cm.TypeName})
	}
	truncated := false
	if limit > 0 && len(all) > limit {
		all = all[:limit]
		truncated = true
	}
	return all, schema, int64(len(res.Rows)), truncated, nil
}

// newPlanner creates a fresh Planner for a single query. The Planner carries
// per-query mutable state (scanCounter, scanCache, planCtx) so it must not be
// shared across concurrent calls.
func (db *DB) newPlanner() *physical.Planner {
	p := physical.NewPlanner(db.catalog)
	p.MemoryBudget = db.memoryBudget
	p.SpillDir = db.spillDir
	p.SortMergeJoinBytes = db.sortMergeJoinBytes
	p.LateMaterialization = db.lateMaterialization
	return p
}

// CreateTable creates a new table with the given schema and partition keys.
func (db *DB) CreateTable(ctx context.Context, name string, schema parquet.Schema, partitionKeys []string) error {
	return db.catalog.CreateTable(ctx, name, schema, partitionKeys)
}

// DropTable removes a table from the catalog.
func (db *DB) DropTable(ctx context.Context, name string) error {
	return db.catalog.DropTable(ctx, name)
}

// ListTables returns all table names.
func (db *DB) ListTables(ctx context.Context) ([]string, error) {
	return db.catalog.ListTables(ctx)
}

// NewIngester creates a micro-batch ingester for the given table.
func (db *DB) NewIngester(tableName string, schema parquet.Schema, partitionKeys []string, cfg ingest.Config) *ingest.Ingester {
	return ingest.New(db.catalog, tableName, schema, partitionKeys, cfg)
}

// ColumnMeta describes a result column's type information.
type ColumnMeta struct {
	Name     string
	TypeName string         // Wadjet type name (e.g., "INT64", "STRING")
	TypeID   parquet.TypeID // Wadjet type ID
	Nullable bool
}

// QueryResult contains the result of a SQL query.
type QueryResult struct {
	Columns     []string
	ColumnMetas []ColumnMeta // typed column metadata (may be nil for introspection queries)
	Rows        []map[string]any
	Plan        string
}

// Query executes a SQL query and returns the results.
func (db *DB) Query(ctx context.Context, sql string) (*QueryResult, error) {
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parsing SQL: %w", err)
	}

	switch parsed.Type {
	case plansql.QueryExplain:
		return db.explain(ctx, parsed)
	case plansql.QueryDescribe:
		return db.describe(ctx, parsed.Describe.TableName)
	case plansql.QueryCreateFunction:
		return db.createFunction(parsed.CreateFunction)
	case plansql.QueryDropFunction:
		return db.dropFunction(parsed.DropFunction)
	case plansql.QueryShowFunctions:
		return db.showFunctions()
	case plansql.QueryCreateTable:
		return db.createTableSQL(ctx, parsed.CreateTable)
	case plansql.QueryDropTable:
		return db.dropTableSQL(ctx, parsed.DropTable)
	case plansql.QueryAnalyzeTable:
		return db.analyzeTableSQL(ctx, parsed.AnalyzeTable)
	case plansql.QueryShowTables:
		return db.showTables(ctx)
	case plansql.QueryCreateAlert:
		return db.createAlertSQL(ctx, parsed.CreateAlert, sql)
	case plansql.QueryDropAlert:
		return db.dropAlertSQL(ctx, parsed.DropAlert)
	case plansql.QueryAlterAlert:
		return db.alterAlertSQL(ctx, parsed.AlterAlert)
	case plansql.QueryInsert, plansql.QueryUpdate, plansql.QueryDelete, plansql.QueryMerge:
		result, err := db.Execute(ctx, sql)
		if err != nil {
			return nil, err
		}
		return &QueryResult{
			Columns: []string{"result"},
			Rows: []map[string]any{{
				"result": fmt.Sprintf("%s %d", result.Command, result.RowsAffected),
			}},
		}, nil
	}

	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil, fmt.Errorf("extracting SELECT: %w", err)
	}

	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		return nil, fmt.Errorf("building logical plan: %w", err)
	}

	planner := db.newPlanner()

	// Reject references to columns that resolve to no source (plan-time name
	// binding) before annotation/optimization rewrite the plan.
	if err := planner.ValidateColumns(ctx, selectInfo); err != nil {
		return nil, err
	}

	// Annotate scan columns before ABAC enforcement so column policies can resolve
	planner.AnnotateScanColumns(ctx, logicalPlan)

	// ABAC enforcement: inject row filters and column policies at plan level
	logicalPlan, err = db.enforceAccessPolicies(ctx, selectInfo, logicalPlan)
	if err != nil {
		return nil, err
	}
	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		planner.AnnotateScanColumns(ctx, plan)
	})
	planStr := logicalPlan.PrettyPrint(0)

	physPlan, err := planner.Plan(ctx, logicalPlan)
	if err != nil {
		return nil, fmt.Errorf("building physical plan: %w", err)
	}
	if physPlan.Cleanup != nil {
		defer physPlan.Cleanup()
	}

	pipeline := physPlan.Pipeline
	if err := pipeline.Run(ctx); err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}
	defer pipeline.Close()

	var rows []map[string]any
	if collectSink, ok := pipeline.Sink.(*exec.CollectSink); ok {
		rows = collectSink.ToRows()
	}

	// Derive column order from the projection list, not map iteration
	columns := deriveColumns(selectInfo, rows)

	// Derive typed column metadata
	var metas []ColumnMeta
	if len(columns) > 0 {
		metas = deriveColumnMetas(columns, rows, db.catalog)
	}

	return &QueryResult{
		Columns:     columns,
		ColumnMetas: metas,
		Rows:        rows,
		Plan:        planStr,
	}, nil
}

func (db *DB) explain(ctx context.Context, parsed *plansql.ParsedQuery) (*QueryResult, error) {
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil, fmt.Errorf("extracting SELECT: %w", err)
	}

	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		return nil, fmt.Errorf("building logical plan: %w", err)
	}

	planner := db.newPlanner()
	if err := planner.ValidateColumns(ctx, selectInfo); err != nil {
		return nil, err
	}
	planner.AnnotateScanColumns(ctx, logicalPlan)

	// ABAC enforcement: inject row filters and column policies at plan level
	logicalPlan, err = db.enforceAccessPolicies(ctx, selectInfo, logicalPlan)
	if err != nil {
		return nil, err
	}
	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		planner.AnnotateScanColumns(ctx, plan)
	})
	plan := logicalPlan.PrettyPrint(0)

	if parsed.Explain.Analyze {
		return db.explainAnalyze(ctx, logicalPlan, plan, parsed.Explain.Verbose, planner)
	}

	if parsed.Explain.Verbose {
		physPlan, err := planner.Plan(ctx, logicalPlan)
		if err != nil {
			return nil, fmt.Errorf("building physical plan: %w", err)
		}
		// The pipeline is never run for EXPLAIN, but planning may have
		// materialized CTEs to spill scratch - release it now.
		if physPlan.Cleanup != nil {
			physPlan.Cleanup()
		}
		plan += "\n\n-- Physical Plan --\n" + physPlan.PrettyPrint()
	}

	// Return plan as rows with a "plan" column (one row per line)
	lines := strings.Split(plan, "\n")
	rows := make([]map[string]any, len(lines))
	for i, line := range lines {
		rows[i] = map[string]any{"plan": line}
	}

	return &QueryResult{
		Columns: []string{"plan"},
		Rows:    rows,
		Plan:    plan,
	}, nil
}

// explainAnalyze builds and executes the query with profiling wrappers,
// then returns the plan annotated with actual execution statistics.
func (db *DB) explainAnalyze(ctx context.Context, logicalPlan *logical.Node, logicalStr string, verbose bool, planner *physical.Planner) (*QueryResult, error) {
	physPlan, err := planner.Plan(ctx, logicalPlan)
	if err != nil {
		return nil, fmt.Errorf("building physical plan: %w", err)
	}
	if physPlan.Cleanup != nil {
		defer physPlan.Cleanup()
	}

	pipeline := physPlan.Pipeline

	// Wrap all operators with profiling decorators before execution
	collector := exec.WrapPipeline(pipeline)

	if err := pipeline.Run(ctx); err != nil {
		return nil, fmt.Errorf("executing query for EXPLAIN ANALYZE: %w", err)
	}
	defer pipeline.Close()

	// Count result rows from the sink
	var totalRows int64
	if profiledSink, ok := pipeline.Sink.(*exec.ProfiledSink); ok {
		if inner, ok := profiledSink.Inner().(*exec.CollectSink); ok {
			totalRows = int64(len(inner.ToRows()))
		}
	}

	// Build annotated output
	var out strings.Builder
	out.WriteString("-- Logical Plan --\n")
	out.WriteString(logicalStr)

	if verbose {
		out.WriteString("\n\n-- Physical Plan --\n")
		out.WriteString(physPlan.PrettyPrint())
	}

	out.WriteString("\n\n-- Execution Stats --\n")
	for _, line := range exec.FormatAnalyzeStats(collector.Stats()) {
		out.WriteString(line)
		out.WriteString("\n")
	}
	out.WriteString(fmt.Sprintf("\nTotal rows returned: %d", totalRows))

	planStr := out.String()
	lines := strings.Split(planStr, "\n")
	rows := make([]map[string]any, len(lines))
	for i, line := range lines {
		rows[i] = map[string]any{"plan": line}
	}

	return &QueryResult{
		Columns: []string{"plan"},
		Rows:    rows,
		Plan:    planStr,
	}, nil
}

func (db *DB) describe(ctx context.Context, tableName string) (*QueryResult, error) {
	tableName = strings.ToLower(tableName)
	table, err := db.catalog.GetTable(ctx, tableName)
	if err != nil {
		// Fallback: discover schema from Parquet files in object storage.
		// This handles cases where catalog metadata was lost (e.g., restart
		// without persistent KV) but data files still exist.
		if schema, discoverErr := db.discoverTableSchema(ctx, tableName); discoverErr == nil {
			return db.describeSchema(tableName, schema, nil), nil
		}
		return nil, fmt.Errorf("table %q: %w", tableName, err)
	}

	return db.describeSchema(tableName, table.Schema, table.PartitionKeys), nil
}

// describeSchema formats a schema as a DESCRIBE result.
func (db *DB) describeSchema(tableName string, schema parquet.Schema, partitionKeys []string) *QueryResult {
	columns := []string{"column_name", "type", "nullable"}
	rows := make([]map[string]any, len(schema.Columns))
	for i, col := range schema.Columns {
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

	if len(partitionKeys) > 0 {
		rows = append(rows, map[string]any{
			"column_name": "",
			"type":        "",
			"nullable":    "",
		})
		rows = append(rows, map[string]any{
			"column_name": "Partition Keys",
			"type":        strings.Join(partitionKeys, ", "),
			"nullable":    "",
		})
	}

	return &QueryResult{
		Columns: columns,
		Rows:    rows,
	}
}

// discoverTableSchema reads schema from the first Parquet file found for a table.
func (db *DB) discoverTableSchema(ctx context.Context, tableName string) (parquet.Schema, error) {
	prefix := "tables/" + tableName + "/"
	objects, err := db.store.List(ctx, db.bucket, objstore.ListOptions{Prefix: prefix})
	if err != nil {
		return parquet.Schema{}, err
	}
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".parquet") {
			continue
		}
		rc, _, err := db.store.Get(ctx, db.bucket, obj.Key)
		if err != nil {
			continue
		}
		data, readErr := io.ReadAll(rc)
		rc.Close()
		if readErr != nil {
			continue
		}
		rdr, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			continue
		}
		return rdr.Schema(), nil
	}
	return parquet.Schema{}, fmt.Errorf("no parquet files found for table %q", tableName)
}

// deriveColumns extracts ordered column names from the SELECT projection.
func deriveColumns(info *plansql.SelectInfo, rows []map[string]any) []string {
	// Try to derive from projection
	if info != nil && len(info.Columns) > 0 {
		// Check for star expansion — need to get columns from result rows
		hasStar := false
		for _, col := range info.Columns {
			if col.Star {
				hasStar = true
				break
			}
		}
		if !hasStar {
			cols := make([]string, 0, len(info.Columns))
			for _, col := range info.Columns {
				name := col.Alias
				if name == "" {
					if col.IsAgg {
						name = col.Expr
					} else if col.ColumnRef != "" {
						name = col.ColumnRef
					} else {
						name = col.Expr
					}
				}
				cols = append(cols, name)
			}
			return cols
		}
	}

	// Fallback: get from first row. Sort for deterministic column ordering —
	// Go map iteration is non-deterministic, which caused column order
	// mismatches between RowDescription and DataRow in the pgwire Extended
	// Query Protocol (GitHub issue #9).
	if len(rows) > 0 {
		cols := make([]string, 0, len(rows[0]))
		for k := range rows[0] {
			cols = append(cols, k)
		}
		sort.Strings(cols)
		return cols
	}
	return nil
}

// deriveColumnMetas infers column type metadata from result data and catalog schemas.
func deriveColumnMetas(columns []string, rows []map[string]any, cat *catalog.Catalog) []ColumnMeta {
	metas := make([]ColumnMeta, len(columns))

	// Try to match columns against catalog table schemas
	ctx := context.Background()
	schemaMap := make(map[string]parquet.TypeID)
	if cat != nil {
		if tableNames, err := cat.ListTables(ctx); err == nil {
			for _, tableName := range tableNames {
				if table, err := cat.GetTable(ctx, tableName); err == nil && table != nil {
					for _, col := range table.Schema.Columns {
						schemaMap[col.Name] = col.Type
					}
				}
			}
		}
	}

	for i, name := range columns {
		metas[i] = ColumnMeta{Name: name, Nullable: true}

		// Try catalog schema first
		if tid, ok := schemaMap[name]; ok {
			metas[i].TypeID = tid
			metas[i].TypeName = tid.String()
			continue
		}

		// Infer from first non-null value in results
		for _, row := range rows {
			val := row[name]
			if val == nil {
				continue
			}
			switch val.(type) {
			case bool:
				metas[i].TypeID = parquet.TypeBool
			case int32:
				metas[i].TypeID = parquet.TypeInt32
			case int64:
				metas[i].TypeID = parquet.TypeInt64
			case int:
				metas[i].TypeID = parquet.TypeInt64
			case float32:
				metas[i].TypeID = parquet.TypeFloat32
			case float64:
				metas[i].TypeID = parquet.TypeFloat64
			case string:
				metas[i].TypeID = parquet.TypeString
			default:
				metas[i].TypeID = parquet.TypeString
			}
			metas[i].TypeName = metas[i].TypeID.String()
			break
		}

		// Default to STRING if no data
		if metas[i].TypeName == "" {
			metas[i].TypeID = parquet.TypeString
			metas[i].TypeName = "STRING"
		}
	}
	return metas
}

func (db *DB) createFunction(cf *plansql.CreateFunctionInfo) (*QueryResult, error) {
	def := expr.UDFDef{
		Name:   cf.Name,
		Params: cf.Params,
		Body:   cf.Body,
		Locked: cf.Locked,
	}

	if !cf.Replace {
		if _, exists := expr.DefaultUDFs.Get(def.Name); exists {
			return nil, fmt.Errorf("function %q already exists (use CREATE OR REPLACE to overwrite)", cf.Name)
		}
	}

	if err := expr.DefaultUDFs.Register(def, true); err != nil {
		return nil, err
	}

	return &QueryResult{
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": fmt.Sprintf("Function %q created", cf.Name)}},
	}, nil
}

func (db *DB) dropFunction(df *plansql.DropFunctionInfo) (*QueryResult, error) {
	err := expr.DefaultUDFs.Unregister(df.Name, "", true)
	if err != nil {
		if df.IfExists {
			return &QueryResult{
				Columns: []string{"result"},
				Rows:    []map[string]any{{"result": fmt.Sprintf("Function %q does not exist (no-op)", df.Name)}},
			}, nil
		}
		return nil, err
	}

	return &QueryResult{
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": fmt.Sprintf("Function %q dropped", df.Name)}},
	}, nil
}

func (db *DB) showFunctions() (*QueryResult, error) {
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

	return &QueryResult{
		Columns: []string{"name", "params", "body", "owner", "locked"},
		Rows:    rows,
	}, nil
}

func (db *DB) createTableSQL(ctx context.Context, ct *plansql.CreateTableInfo) (*QueryResult, error) {
	schema, err := columnDefsToSchema(ct.Columns)
	if err != nil {
		return nil, err
	}
	if err := db.catalog.CreateTable(ctx, ct.Name, schema, ct.PartitionKeys); err != nil {
		return nil, err
	}
	return &QueryResult{
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": fmt.Sprintf("Table %q created", ct.Name)}},
	}, nil
}

func (db *DB) dropTableSQL(ctx context.Context, dt *plansql.DropTableInfo) (*QueryResult, error) {
	err := db.catalog.DropTable(ctx, dt.Name)
	if err != nil {
		if dt.IfExists {
			return &QueryResult{
				Columns: []string{"result"},
				Rows:    []map[string]any{{"result": fmt.Sprintf("Table %q does not exist (no-op)", dt.Name)}},
			}, nil
		}
		return nil, err
	}
	return &QueryResult{
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": fmt.Sprintf("Table %q dropped", dt.Name)}},
	}, nil
}

// analyzeTableSQL refreshes the planner's column statistics (per-column HLL NDV
// + reservoir-sample histograms) for a table by walking its parquet files. The
// stats engine already exists (catalog.AnalyzeTable); this is its SQL surface.
func (db *DB) analyzeTableSQL(ctx context.Context, at *plansql.AnalyzeTableInfo) (*QueryResult, error) {
	n, err := db.catalog.AnalyzeTable(ctx, at.Name)
	if err != nil {
		return nil, err
	}
	return &QueryResult{
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": fmt.Sprintf("Table %q analyzed (%d files)", at.Name, n)}},
	}, nil
}

func (db *DB) showTables(ctx context.Context) (*QueryResult, error) {
	tables, err := db.catalog.ListTables(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, len(tables))
	for i, t := range tables {
		rows[i] = map[string]any{"table_name": t}
	}
	return &QueryResult{
		Columns: []string{"table_name"},
		Rows:    rows,
	}, nil
}

// alertEvalDecorator returns the scheduler's per-alert context decorator that
// runs each alert query under its creator's identity (definer's rights). Auth
// disabled → context untouched; legacy alerts with no stored identity run
// fail-closed under ABAC and are warned about once per process.
func (db *DB) alertEvalDecorator() alerts.EvalContextFunc {
	var warned sync.Map
	return func(ctx context.Context, m catalog.AlertMeta) context.Context {
		snap := auth.IdentitySnapshot{
			Name:       m.CreatedBy,
			Role:       m.CreatedByRole,
			Method:     m.CreatedByMethod,
			Attributes: m.CreatedByAttrs,
		}
		newCtx, attributed := auth.StampDefiner(ctx, db.authProvider, snap)
		if !attributed {
			if _, seen := warned.LoadOrStore(m.Name, true); !seen {
				db.logger.Warn("alert has no stored creator identity; running fail-closed under ABAC — recreate it to attribute a definer",
					"alert", m.Name)
			}
		}
		return newCtx
	}
}

// createAlertSQL handles CREATE ALERT DDL in embedded mode.
func (db *DB) createAlertSQL(ctx context.Context, info *plansql.CreateAlertInfo, _ string) (*QueryResult, error) {
	if db.alertScheduler == nil {
		return nil, fmt.Errorf("alerts are disabled; set Config.EnableAlerts=true")
	}
	if err := auth.RequirePermission(db.authProvider, ctx, "admin"); err != nil {
		return nil, err
	}
	if err := alerts.EnsureHistoryTable(ctx, db.catalog); err != nil {
		return nil, fmt.Errorf("ensuring alert_history: %w", err)
	}
	// Snapshot the creator's identity for definer's-rights scheduled runs.
	snap := auth.SnapshotIdentity(ctx)
	m := catalog.AlertMeta{
		Name:            info.Name,
		QueryText:       info.QueryText,
		IntervalSeconds: int64(info.Interval / time.Second),
		WebhookURL:      info.WebhookURL,
		WebhookHeaders:  info.Headers,
		InsertIntoTable: info.InsertInto,
		Enabled:         true,
		CreatedAt:       time.Now().UTC(),
		CreatedBy:       snap.Name,
		CreatedByRole:   snap.Role,
		CreatedByMethod: snap.Method,
		CreatedByAttrs:  snap.Attributes,
	}
	if err := db.catalog.CreateAlert(ctx, m); err != nil {
		return nil, err
	}
	return &QueryResult{
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": "CREATE ALERT"}},
	}, nil
}

// dropAlertSQL handles DROP ALERT [IF EXISTS] DDL in embedded mode.
func (db *DB) dropAlertSQL(ctx context.Context, info *plansql.DropAlertInfo) (*QueryResult, error) {
	if db.alertScheduler == nil {
		return nil, fmt.Errorf("alerts are disabled; set Config.EnableAlerts=true")
	}
	if err := auth.RequirePermission(db.authProvider, ctx, "admin"); err != nil {
		return nil, err
	}
	if _, err := db.catalog.GetAlert(ctx, info.Name); err != nil {
		if info.IfExists {
			return &QueryResult{
				Columns: []string{"result"},
				Rows:    []map[string]any{{"result": "DROP ALERT"}},
			}, nil
		}
		return nil, err
	}
	if err := db.catalog.DropAlert(ctx, info.Name); err != nil {
		return nil, err
	}
	return &QueryResult{
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": "DROP ALERT"}},
	}, nil
}

// alterAlertSQL handles ALTER ALERT DDL in embedded mode.
func (db *DB) alterAlertSQL(ctx context.Context, info *plansql.AlterAlertInfo) (*QueryResult, error) {
	if db.alertScheduler == nil {
		return nil, fmt.Errorf("alerts are disabled; set Config.EnableAlerts=true")
	}
	if err := auth.RequirePermission(db.authProvider, ctx, "admin"); err != nil {
		return nil, err
	}
	if err := db.catalog.SetAlertEnabled(ctx, info.Name, info.Enable); err != nil {
		return nil, err
	}
	return &QueryResult{
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": "ALTER ALERT"}},
	}, nil
}

// columnDefsToSchema converts parsed column definitions to a parquet.Schema.
func columnDefsToSchema(defs []plansql.ColumnDef) (parquet.Schema, error) {
	columns := make([]parquet.Column, len(defs))
	for i, d := range defs {
		typeID, err := parquet.ParseTypeID(d.Type)
		if err != nil {
			return parquet.Schema{}, fmt.Errorf("column %q: %w", d.Name, err)
		}
		col := parquet.Column{
			Name:     strings.ToLower(d.Name),
			Type:     typeID,
			Nullable: d.Nullable,
		}
		if typeID == parquet.TypeDecimal {
			col.Precision, col.Scale = parquet.ParseDecimalParams(d.Type)
		}
		columns[i] = col
	}
	return parquet.Schema{Columns: columns}, nil
}

// SetAuthProvider sets the auth provider for ABAC enforcement.
// This allows wiring auth after DB creation (e.g., when the provider depends on config reload).
func (db *DB) SetAuthProvider(p *auth.Provider) {
	db.authProvider = p
}

// enforceAccessPolicies applies ABAC (table denial, row filters, column
// deny/mask) at plan level via the shared auth.EnforcePlanPolicies — the
// same enforcement the coordinator's native-DAG path applies, so embedded
// and distributed execution see identical policy behavior.
func (db *DB) enforceAccessPolicies(ctx context.Context, selectInfo *plansql.SelectInfo, plan *logical.Node) (*logical.Node, error) {
	return auth.EnforcePlanPolicies(ctx, db.authProvider, selectInfo, plan, "embedded")
}

// Catalog returns the underlying catalog for advanced usage.
func (db *DB) Catalog() *catalog.Catalog {
	return db.catalog
}

// Store returns the underlying object store.
func (db *DB) Store() objstore.Store {
	return db.store
}
