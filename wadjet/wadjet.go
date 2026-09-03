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

	"github.com/derekmwright/wadjet/internal/alerts"
	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// DB is the main entry point for embedded usage of Wadjet.
type DB struct {
	store               objstore.Store
	catalog             *catalog.Catalog
	bucket              string
	memoryBudget        int64
	spillDir            string
	logger              *slog.Logger
	authProvider        *auth.Provider    // nil = no auth enforcement
	alertScheduler      *alerts.Scheduler // non-nil when EnableAlerts is set
	alertSchedulerStop  context.CancelFunc
	sortMergeJoinBytes  int64
	lateMaterialization bool
	queryLimits         *config.QueryLimits
	roleQueryLimits     map[string]*config.QueryLimits
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
	// BushyJoinReorder lets the cost-based join reorder emit bushy plans
	// when strictly cheaper than every left-deep order
	// (docs/design/bushy-join-cbo.md). PROCESS-WIDE: the logical optimizer
	// has no per-query config surface, so Open stores this into a package
	// flag shared by every DB in the process. Off by default.
	BushyJoinReorder bool
	// EnableAlerts turns on the CREATE ALERT scheduler in embedded mode.
	// When true, Open() creates a Scheduler that evaluates alerts on cadence.
	EnableAlerts bool
	// QueryLimits / RoleLimits are the cost guard (docs/security.md,
	// "Query Cost Estimation and Guards"): the global limits, and the
	// per-role overrides keyed by role name (an entry present with a nil
	// value means that role is unlimited).
	//
	// The embedded planner is the LAST entry point that could reach a scan
	// without them. Every other one — HTTP, gRPC, and the pgwire statements
	// the coordinator answers — goes through Coordinator.ExecuteSQL, which
	// carries the same limits; but pgwire falls back to this DB for any
	// statement its routing gate declines (a leading comment, TABLE, VALUES)
	// and for every statement when a provider is present but disabled, so a
	// guard that stopped at the coordinator left the PostgreSQL wire — the
	// protocol the BI clients use — unbounded (#803).
	QueryLimits *config.QueryLimits
	RoleLimits  map[string]*config.QueryLimits
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
		store:               cfg.Store,
		catalog:             cat,
		bucket:              cfg.Bucket,
		memoryBudget:        cfg.MemoryBudget,
		spillDir:            cfg.SpillDir,
		logger:              cfg.Logger,
		authProvider:        cfg.AuthProvider,
		queryLimits:         cfg.QueryLimits,
		roleQueryLimits:     cfg.RoleLimits,
		sortMergeJoinBytes:  cfg.SortMergeJoinBytes,
		lateMaterialization: cfg.LateMaterialization,
	}

	if cfg.BushyJoinReorder {
		// Process-wide planner knob — see the Config field doc.
		logical.BushyJoinReorder.Store(true)
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
//
// It is the embedded engine's ONE planner-construction site, which is why the
// cost guard is installed here rather than at each caller: a future query
// entry point gets the limits by construction instead of by remembering.
func (db *DB) newPlanner(ctx context.Context) *physical.Planner {
	p := physical.NewPlanner(db.catalog)
	p.MemoryBudget = db.memoryBudget
	p.SpillDir = db.spillDir
	p.SortMergeJoinBytes = db.sortMergeJoinBytes
	p.LateMaterialization = db.lateMaterialization
	p.QueryLimits = db.resolveQueryLimits(ctx)
	return p
}

// SetQueryLimits installs the cost guard after Open, for callers that build
// the DB before they have read the config (the `serve` command opens the
// pgwire DB alongside the coordinator). Call before serving traffic, the same
// contract as Coordinator.SetQueryLimits.
func (db *DB) SetQueryLimits(global *config.QueryLimits, perRole map[string]*config.QueryLimits) {
	db.queryLimits = global
	db.roleQueryLimits = perRole
}

// resolveQueryLimits returns the limits for the identity in ctx: the per-role
// override when the role names one (a nil entry = unlimited, which is how a
// role that declares no query_limits overrides the global cap), else the
// global limits.
func (db *DB) resolveQueryLimits(ctx context.Context) *config.QueryLimits {
	if db.roleQueryLimits != nil {
		if id := auth.IdentityFromContext(ctx); id != nil {
			if limits, ok := db.roleQueryLimits[id.Role]; ok {
				return limits
			}
		}
	}
	return db.queryLimits
}

// CreateTable creates a new table with the given schema and partition keys.
//
// This is one of the RESERVED-NAMESPACE doors. A column whose name is in a
// hidden-slot family (`__win_N`, `__sortkey_N`, …) is refused here, 42939,
// because this is where the name is being CREATED. Reading such a column is
// never refused — a table that already has one stays readable, and the planner
// renumbers its own slot instead (physical.renameCollidingSlots).
func (db *DB) CreateTable(ctx context.Context, name string, schema parquet.Schema, partitionKeys []string) error {
	if err := refuseReservedSchemaNames(schema, "column of new table "+name); err != nil {
		return err
	}
	return db.catalog.CreateTable(ctx, name, schema, partitionKeys)
}

// refuseReservedSchemaNames is the reserved-namespace door check for a schema
// a caller is creating. Shared by CreateTable, CREATE TABLE and NewIngester so
// the three doors cannot disagree about what is admissible.
func refuseReservedSchemaNames(schema parquet.Schema, where string) error {
	names := make([]string, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		names = append(names, c.Name)
	}
	return physical.RefuseReservedSlotNames(names, where)
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
//
// The ingest door of the reserved namespace: an Ingester's schema CREATES the
// table when it does not exist, so a slot-family column name is refused here
// the way CreateTable refuses it. The error is deferred to the first Ingest
// call because this constructor returns no error — see Ingester.Ingest.
func (db *DB) NewIngester(tableName string, schema parquet.Schema, partitionKeys []string, cfg ingest.Config) *ingest.Ingester {
	ing := ingest.New(db.catalog, tableName, schema, partitionKeys, cfg)
	if err := refuseReservedSchemaNames(schema, "column of ingested table "+tableName); err != nil {
		ing.RefuseWith(err)
	}
	return ing
}

// ColumnMeta describes a result column's type information.
//
// Precision and Scale are the DECLARATION, not the value: a bare TypeID is
// not a type for a DECIMAL, and the pgwire layer needs them to fill
// RowDescription's type modifier — PostgreSQL packs a numeric's precision and
// scale there, and it is where a JDBC or ODBC client reads
// ResultSetMetaData.getPrecision()/getScale() from. Sending the constant -1
// declares an unconstrained numeric, so a tool that sizes a display column or
// round-trips DDL from a result set got it wrong for every DECIMAL(p,s)
// column (#454). They are zero for every other type.
type ColumnMeta struct {
	Name      string
	TypeName  string         // Wadjet type name (e.g., "INT64", "STRING")
	TypeID    parquet.TypeID // Wadjet type ID
	Nullable  bool
	Precision int // DECIMAL: declared max digits
	Scale     int // DECIMAL: declared digits after the point
	// WireUnconstrained is true for a DECIMAL column produced by an
	// aggregate function (MIN/MAX/MIN_BY/MAX_BY/SUM/AVG and any other
	// DECIMAL-producing aggregate): Precision/Scale above still carry the
	// real declaration for callers that want it, but PostgreSQL's own wire
	// protocol reports typmod -1 ("unconstrained numeric") for any such
	// column — verified against live postgres:17-alpine's \gdesc, which
	// keeps a real typmod only for a BARE column reference. pgTypeMod
	// treats this the same as Precision <= 0 (FIX 2, #457/#458 fold-in).
	WireUnconstrained bool
}

// QueryResult contains the result of a SQL query.
type QueryResult struct {
	Columns     []string
	ColumnMetas []ColumnMeta // typed column metadata (may be nil for introspection queries)
	// Rows is the result keyed by column NAME, and it is a convenience: a
	// result may legally carry two columns of the same name (PostgreSQL
	// answers `SELECT abs(a), abs(b)` with two columns called `abs`, and
	// #513 made this engine agree), and a map cannot hold both — the LAST
	// one wins and the earlier value is not represented. Columns still lists
	// every column, so len(Rows[i]) < len(Columns) is how a caller detects
	// it. Read RowValues when the values matter.
	Rows []map[string]any
	// RowValues is the same result POSITIONALLY, cells aligned with Columns,
	// and it is populated ONLY when Rows would lose a value — that is, when
	// two output columns share a name. nil means the names are unique and
	// Rows is exact. Nothing that transports values (the pgwire DataRow
	// path) may read Rows without consulting this first.
	RowValues [][]any
	Plan      string
}

// Cells returns row i positionally, whether or not the result needed
// RowValues: from RowValues when duplicate column names made the map lossy,
// and otherwise by looking each column up in Rows, which is exact there.
// Returns nil when i is out of range.
func (r *QueryResult) Cells(i int) []any {
	// The range check comes FIRST and covers both forms: a negative index
	// passes `i < len(RowValues)` and would index the slice with it.
	if r == nil || i < 0 {
		return nil
	}
	if i < len(r.RowValues) {
		return r.RowValues[i]
	}
	if i >= len(r.Rows) {
		return nil
	}
	cells := make([]any, len(r.Columns))
	for j, col := range r.Columns {
		cells[j] = r.Rows[i][col]
	}
	return cells
}

// Query executes a SQL query and returns the results.
func (db *DB) Query(ctx context.Context, sql string) (res *QueryResult, err error) {
	// The embedded API's query boundary. Panics that carry a query ERROR
	// (exec.FatalEvalPanic — including batch.TypeMismatchError, #361's
	// silent-write guard) become that error here: this entry reaches
	// batch-writing code outside Pipeline.Run's own recover (result
	// assembly, catalog-backed synthetic tables). Since #511 an UNEXPECTED
	// panic becomes an internal error too — an embedded caller must get an
	// error back, never a process exit taken on its behalf.
	defer func() {
		if r := recover(); r != nil {
			err = exec.RecoverQueryPanic(ctx, "embedded query", r)
		}
	}()
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

	planner := db.newPlanner(ctx)

	// Reject references to columns that resolve to no source (plan-time name
	// binding) before annotation/optimization rewrite the plan.
	//
	// This runs BEFORE the logical build, as the coordinator's entry point
	// already does: the builder refuses some of the same statements on its
	// own — an ORDER BY term a GROUP BY does not carry is one — and does it
	// with a message that carries no SQLSTATE, so building first meant the
	// two entry points answered the same statement with different errors
	// (#590).
	if err := planner.ValidateColumns(ctx, selectInfo); err != nil {
		return nil, err
	}

	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		return nil, fmt.Errorf("building logical plan: %w", err)
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
	// The defer is registered BEFORE Run, not after: a cancelled or failing
	// Run returns from this function, and a `defer` STATEMENT placed below
	// the error check never executes at all. That is how a cancelled query
	// left every operator's spill scratch on disk — sort runs, aggregate
	// partial-state files and window runs are created with a bare os.Create
	// and are never registered with the SpillManager, so physPlan.Cleanup
	// cannot see them; only the operator's own Close() removes them
	// (#625 M1, ADR-0028). The correct form already existed at
	// internal/coordinator/local_fastpath.go and was never applied here.
	defer pipeline.Close()
	if err := pipeline.Run(ctx); err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}

	var rows []map[string]any
	var rowValues [][]any
	if collectSink, ok := pipeline.Sink.(*exec.CollectSink); ok {
		rows = collectSink.ToRows()
		// Non-nil only when two output columns share a name, which is
		// exactly when rows above cannot represent the answer.
		rowValues = collectSink.ToRowValues()
	}

	// Derive column order from the projection list, not map iteration. The
	// executed plan's output schema is the authority for `SELECT *`, where
	// the projection list names no columns of its own.
	var outSchema []parquet.Column
	var wireUnconstrained map[string]bool
	if collectSink, ok := pipeline.Sink.(*exec.CollectSink); ok {
		outSchema = collectSink.Schema()
		// Plan-time, not row-count-dependent (FIX 2, #457/#458 fold-in) —
		// consulted whether or not Consume ever ran.
		wireUnconstrained = collectSink.SchemaHintWireUnconstrainedDecimal
	}
	columns := deriveColumns(selectInfo, rows, outSchema)

	// Derive typed column metadata
	var metas []ColumnMeta
	if len(columns) > 0 {
		metas = deriveColumnMetas(columns, rows, outSchema, db.catalog, wireUnconstrained)
	}

	return &QueryResult{
		Columns:     columns,
		ColumnMetas: metas,
		Rows:        rows,
		RowValues:   rowValues,
		Plan:        planStr,
	}, nil
}

func (db *DB) explain(ctx context.Context, parsed *plansql.ParsedQuery) (*QueryResult, error) {
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil, fmt.Errorf("extracting SELECT: %w", err)
	}

	planner := db.newPlanner(ctx)
	// Before the build, for the reason Query's own call site records: the
	// builder's own refusals carry no SQLSTATE (#590).
	if err := planner.ValidateColumns(ctx, selectInfo); err != nil {
		return nil, err
	}

	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		return nil, fmt.Errorf("building logical plan: %w", err)
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

	// Registered before Run for the same reason as the Query path: a defer
	// below a failing Run's error check never runs, so a cancelled query
	// keeps its spill scratch (#625 M1).
	defer pipeline.Close()
	if err := pipeline.Run(ctx); err != nil {
		return nil, fmt.Errorf("executing query for EXPLAIN ANALYZE: %w", err)
	}

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
//
// schema is the executed plan's output schema and outranks the rows for
// ORDER: a `SELECT *` names no columns in its projection list, and the rows
// are map[string]any, whose iteration order Go randomizes. Sorting those keys
// made column order alphabetical rather than the table's own — values landed
// in the right names and the wrong positions, which anything reading results
// positionally (a CSV export, INSERT ... SELECT *, a driver binding by index)
// silently transposes (#342).
func deriveColumns(info *plansql.SelectInfo, rows []map[string]any, schema []parquet.Column) []string {
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
					switch {
					case col.IsWindow:
						// The name the logical builder's projection really
						// publishes. PostgreSQL calls an unaliased
						// `SUM(a) OVER ()` "sum"; naming it from the
						// expression TEXT here asked the result schema for a
						// column the projection does not emit, and every row
						// came back NULL.
						name = plansql.WindowOutputName(col)
					case col.IsAgg:
						name = col.Expr
					case col.ColumnRef != "":
						name = col.ColumnRef
					default:
						name = col.Expr
					}
				}
				cols = append(cols, reconcileColumnName(name, rows))
			}
			return cols
		}
	}

	// The plan's own output schema: ordered, and what `SELECT *` expanded to.
	if len(schema) > 0 {
		cols := make([]string, 0, len(schema))
		for _, c := range schema {
			cols = append(cols, c.Name)
		}
		return cols
	}

	// Fallback: get from first row. Sort for deterministic column ordering —
	// Go map iteration is non-deterministic, which caused column order
	// mismatches between RowDescription and DataRow in the pgwire Extended
	// Query Protocol (GitHub issue #9). Reached only when the plan exposed
	// no schema, so alphabetical is the best available answer rather than
	// the right one.
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

// reconcileColumnName keeps the reported column name in step with the keys
// the executed plan actually produced. A projection name only needs
// repairing when it is absent from the result rows — a column reference
// whose name carries a dot (a flat JSON column such as Zeek's id.orig_h,
// referenced either qualified or as a delimited identifier) can be emitted
// by the aggregate under its qualifier-stripped spelling, and reporting the
// other spelling would leave every client looking up a key that is not
// there. Names present in the rows, and every query with no rows, are left
// exactly as the projection declared them.
func reconcileColumnName(name string, rows []map[string]any) string {
	if len(rows) == 0 {
		return name
	}
	if _, ok := rows[0][name]; ok {
		return name
	}
	if dot := strings.IndexByte(name, '.'); dot >= 0 {
		if _, ok := rows[0][name[dot+1:]]; ok {
			return name[dot+1:]
		}
	}
	return name
}

// deriveColumnMetas infers column type metadata from the executed plan's
// output schema, falling back to catalog schemas and then to the result data.
//
// outSchema is the authority when it names the column: it is the schema of the
// vectors the values were STORED in, and it is the only source that keeps a
// computed column's declared type. Value inference cannot — the row box loses
// the distinction on purpose (a DATE boxes as its rendered text, a TIMESTAMP
// as bare epoch milliseconds; see Vector.GetValue), so `CAST(x AS date)` was
// reported STRING and `CAST(x AS timestamp)` INT64, and the wire declared
// OID 25 / 20 where PostgreSQL declares 1082 / 1114 (#363).
// wireUnconstrainedDecimal names the DECIMAL columns (by output name) whose
// wire typmod must say "unconstrained" (-1) regardless of what Precision/
// Scale this function resolves for them below — an aggregate function
// call, on live PostgreSQL, never keeps its argument's typmod (FIX 2,
// #457/#458 fold-in). May be nil.
func deriveColumnMetas(columns []string, rows []map[string]any, outSchema []parquet.Column, cat *catalog.Catalog, wireUnconstrainedDecimal map[string]bool) []ColumnMeta {
	metas := make([]ColumnMeta, len(columns))

	// The executed output schema, keyed by column name. The whole Column,
	// not just its TypeID: a DECIMAL's precision and scale are part of the
	// declaration the wire has to carry (#454), and keying by TypeID threw
	// them away here before anything downstream could ask.
	outMap := make(map[string]parquet.Column, len(outSchema))
	for _, c := range outSchema {
		outMap[c.Name] = c
	}

	// Try to match columns against catalog table schemas
	ctx := context.Background()
	schemaMap := make(map[string]parquet.Column)
	if cat != nil {
		if tableNames, err := cat.ListTables(ctx); err == nil {
			for _, tableName := range tableNames {
				if table, err := cat.GetTable(ctx, tableName); err == nil && table != nil {
					for _, col := range table.Schema.Columns {
						schemaMap[col.Name] = col
					}
				}
			}
		}
	}

	for i, name := range columns {
		metas[i] = ColumnMeta{Name: name, Nullable: true, WireUnconstrained: wireUnconstrainedDecimal[name]}

		// The executed plan's output schema first — it is per-result rather
		// than a cross-table name match, and it sees computed columns.
		if col, ok := outMap[name]; ok {
			metas[i].TypeID = col.Type
			metas[i].TypeName = col.Type.String()
			metas[i].Precision, metas[i].Scale = col.Precision, col.Scale
			continue
		}

		// Then the catalog schema
		if col, ok := schemaMap[name]; ok {
			metas[i].TypeID = col.Type
			metas[i].TypeName = col.Type.String()
			metas[i].Precision, metas[i].Scale = col.Precision, col.Scale
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
	// WireUnconstrained means what its name and its doc say: a DECIMAL whose
	// wire type modifier must be -1. The PLAN's map answers the typmod
	// question without a type gate — it cannot have one, because the plan
	// cannot always type an aggregate output
	// (physical.declaredWireUnconstrainedDecimal) — so the gate is applied
	// here, where the resolved type is finally known. pgTypeMod reads the
	// field only on the DECIMAL arm either way, so this keeps the FIELD's
	// contract rather than changing the wire's behaviour.
	for i := range metas {
		if metas[i].TypeID != parquet.TypeDecimal {
			metas[i].WireUnconstrained = false
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
	// The SQL door of the reserved namespace (see CreateTable).
	if err := refuseReservedSchemaNames(schema, "column of new table "+ct.Name); err != nil {
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
		col, err := parquet.DeclaredColumn(d.Name, d.Type, d.Nullable)
		if err != nil {
			return parquet.Schema{}, fmt.Errorf("column %q: %w", d.Name, err)
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

// Attach wraps an ALREADY-INITIALIZED catalog as a DB, without creating a
// second catalog or re-running Init.
//
// It exists so a process that already owns a *catalog.Catalog — the HTTP API
// server in coordinator mode — reaches DML through the SAME implementation the
// embedded and pgwire doors use, instead of keeping its own copy of the
// executors. internal/server carried an independent INSERT/UPDATE/DELETE that
// had drifted into the same defects as this package's (#815): an unresolved
// INSERT column list (#814), the marker/manifest window (#691), the literal
// kind discarded in SET (#690) — and no MERGE at all. A fix written once has
// to be reachable from both doors, and this is that reach.
//
// The returned DB owns no background goroutines (Open's alert scheduler is not
// started), so it needs no Close; the caller keeps ownership of the catalog.
func Attach(cat *catalog.Catalog) *DB {
	return &DB{
		store:   cat.Store(),
		catalog: cat,
		bucket:  cat.Bucket(),
		logger:  slog.Default(),
	}
}

// Store returns the underlying object store.
func (db *DB) Store() objstore.Store {
	return db.store
}
