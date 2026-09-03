package server

import (
	"context"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The HTTP API server no longer carries its own INSERT/UPDATE/DELETE
// executors: handleDML delegates to the one implementation (#815). These
// shims keep the gates that were written against the old names driving THE
// SAME door handleDML runs, so what they proved about the HTTP door they still
// prove — and each of them is now also a statement about the embedded door,
// which is the point of merging the two copies.

func runHTTPDML(ctx context.Context, cat *catalog.Catalog, parsed *plansql.ParsedQuery) (*wadjet.ExecResult, error) {
	return wadjet.Attach(cat).ExecuteParsed(ctx, parsed)
}

func executeDMLInsert(ctx context.Context, cat *catalog.Catalog, info *plansql.InsertInfo) (*wadjet.ExecResult, error) {
	return runHTTPDML(ctx, cat, &plansql.ParsedQuery{Type: plansql.QueryInsert, Insert: info})
}

func executeDMLUpdate(ctx context.Context, cat *catalog.Catalog, info *plansql.UpdateInfo) (*wadjet.ExecResult, error) {
	return runHTTPDML(ctx, cat, &plansql.ParsedQuery{Type: plansql.QueryUpdate, Update: info})
}

func executeDMLDelete(ctx context.Context, cat *catalog.Catalog, info *plansql.DeleteInfo) (*wadjet.ExecResult, error) {
	return runHTTPDML(ctx, cat, &plansql.ParsedQuery{Type: plansql.QueryDelete, Delete: info})
}

func readDMLFile(ctx context.Context, cat *catalog.Catalog, filePath string, schema []parquet.Column) (*batch.RecordBatch, error) {
	return wadjet.Attach(cat).ReadDataFile(ctx, filePath, schema)
}
