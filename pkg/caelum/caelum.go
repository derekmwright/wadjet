// Package caelum provides the public embeddable API for Caelum.
package caelum

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/derekmwright/caelum/internal/engine/exec"
	"github.com/derekmwright/caelum/internal/planner/logical"
	"github.com/derekmwright/caelum/internal/planner/physical"
	plansql "github.com/derekmwright/caelum/internal/planner/sql"
	"github.com/derekmwright/caelum/internal/storage/catalog"
	"github.com/derekmwright/caelum/internal/storage/ingest"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

// DB is the main entry point for embedded usage of Caelum.
type DB struct {
	store   objstore.Store
	catalog *catalog.Catalog
	bucket  string
	planner *physical.Planner
	logger  *slog.Logger
}

// Config holds configuration for creating a DB instance.
type Config struct {
	Store  objstore.Store
	Bucket string
	Logger *slog.Logger
}

// Open creates and initializes a new Caelum database.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	cat := catalog.New(cfg.Store, cfg.Bucket)
	if err := cat.Init(ctx); err != nil {
		return nil, fmt.Errorf("initializing catalog: %w", err)
	}

	return &DB{
		store:   cfg.Store,
		catalog: cat,
		bucket:  cfg.Bucket,
		planner: physical.NewPlanner(cat),
		logger:  cfg.Logger,
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

// QueryResult contains the result of a SQL query.
type QueryResult struct {
	Columns []string
	Rows    []map[string]any
	Plan    string
}

// Query executes a SQL query and returns the results.
func (db *DB) Query(ctx context.Context, sql string) (*QueryResult, error) {
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parsing SQL: %w", err)
	}

	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil, fmt.Errorf("extracting SELECT: %w", err)
	}

	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		return nil, fmt.Errorf("building logical plan: %w", err)
	}

	logicalPlan = logical.Optimize(logicalPlan)
	planStr := logicalPlan.PrettyPrint(0)

	physPlan, err := db.planner.Plan(ctx, logicalPlan)
	if err != nil {
		return nil, fmt.Errorf("building physical plan: %w", err)
	}

	pipeline := physPlan.Pipeline
	if err := pipeline.Run(ctx); err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}
	defer pipeline.Close()

	var rows []map[string]any
	if collectSink, ok := pipeline.Sink.(*exec.CollectSink); ok {
		rows = collectSink.Rows
	}

	var columns []string
	if len(rows) > 0 {
		for k := range rows[0] {
			columns = append(columns, k)
		}
	}

	return &QueryResult{
		Columns: columns,
		Rows:    rows,
		Plan:    planStr,
	}, nil
}

// Catalog returns the underlying catalog for advanced usage.
func (db *DB) Catalog() *catalog.Catalog {
	return db.catalog
}

// Store returns the underlying object store.
func (db *DB) Store() objstore.Store {
	return db.store
}
