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
	store        objstore.Store
	catalog      *catalog.Catalog
	bucket       string
	planner      *physical.Planner
	logger       *slog.Logger
	authProvider *auth.Provider // nil = no auth enforcement
}

// Config holds configuration for creating a DB instance.
type Config struct {
	Store        objstore.Store
	Bucket       string
	Logger       *slog.Logger
	MetaKV       catalog.MetaKV  // optional: NATS KV for production, nil = in-memory
	MemoryBudget int64           // per-query memory budget in bytes (0 = unlimited)
	SpillDir     string          // directory for spill-to-disk files (empty = os temp dir)
	AuthProvider *auth.Provider  // optional: enables ABAC enforcement at query level
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

	planner := physical.NewPlanner(cat)
	planner.MemoryBudget = cfg.MemoryBudget
	planner.SpillDir = cfg.SpillDir

	return &DB{
		store:        cfg.Store,
		catalog:      cat,
		bucket:       cfg.Bucket,
		planner:      planner,
		logger:       cfg.Logger,
		authProvider: cfg.AuthProvider,
	}, nil
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
	case plansql.QueryShowTables:
		return db.showTables(ctx)
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

	// Annotate scan columns before ABAC enforcement so column policies can resolve
	db.planner.AnnotateScanColumns(ctx, logicalPlan)

	// ABAC enforcement: inject row filters and column policies at plan level
	logicalPlan, err = db.enforceAccessPolicies(ctx, selectInfo, logicalPlan)
	if err != nil {
		return nil, err
	}
	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		db.planner.AnnotateScanColumns(ctx, plan)
	})
	planStr := logicalPlan.PrettyPrint(0)

	physPlan, err := db.planner.Plan(ctx, logicalPlan)
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

	db.planner.AnnotateScanColumns(ctx, logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		db.planner.AnnotateScanColumns(ctx, plan)
	})
	plan := logicalPlan.PrettyPrint(0)

	if parsed.Explain.Analyze {
		return db.explainAnalyze(ctx, logicalPlan, plan, parsed.Explain.Verbose)
	}

	if parsed.Explain.Verbose {
		physPlan, err := db.planner.Plan(ctx, logicalPlan)
		if err != nil {
			return nil, fmt.Errorf("building physical plan: %w", err)
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
func (db *DB) explainAnalyze(ctx context.Context, logicalPlan *logical.Node, logicalStr string, verbose bool) (*QueryResult, error) {
	physPlan, err := db.planner.Plan(ctx, logicalPlan)
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

// enforceAccessPolicies evaluates ABAC policies for the current identity
// and injects row filters and column policies into the logical plan.
// If no identity is in context or no auth provider is configured, this is a no-op.
func (db *DB) enforceAccessPolicies(ctx context.Context, selectInfo *plansql.SelectInfo, plan *logical.Node) (*logical.Node, error) {
	if db.authProvider == nil || !db.authProvider.Enabled() {
		return plan, nil
	}
	identity := auth.IdentityFromContext(ctx)
	if identity == nil {
		return plan, nil
	}

	evaluator := db.authProvider.Evaluator()
	if evaluator == nil {
		return plan, nil
	}

	subject := identity.ToSubject()
	env := auth.Environment{Protocol: "embedded"}

	// Collect all tables referenced in the query
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
			return nil, fmt.Errorf("access denied to table %q: %s", tableName, td.Reason)
		}

		// Inject row filter into logical plan
		if td.RowFilter != "" {
			plan = logical.InjectRowFilter(plan, tableName, td.RowFilter)
		}

		// Inject column policies into logical plan
		if len(td.Columns) > 0 {
			var colPolicies []logical.ColumnPolicy
			for _, col := range td.Columns {
				if !col.Allowed {
					colPolicies = append(colPolicies, logical.ColumnPolicy{
						Column: col.Column,
						Denied: true,
					})
				} else if col.MaskFunc != "" || col.MaskExpr != "" {
					mask := col.MaskExpr
					if mask == "" {
						mask = "'***'"
					}
					colPolicies = append(colPolicies, logical.ColumnPolicy{
						Column:   col.Column,
						MaskExpr: mask,
					})
				}
			}
			if len(colPolicies) > 0 {
				plan = logical.InjectColumnPolicies(plan, tableName, colPolicies)
			}
		}
	}

	return plan, nil
}

// Catalog returns the underlying catalog for advanced usage.
func (db *DB) Catalog() *catalog.Catalog {
	return db.catalog
}

// Store returns the underlying object store.
func (db *DB) Store() objstore.Store {
	return db.store
}
