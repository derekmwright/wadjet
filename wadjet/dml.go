package wadjet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/expr"
	"github.com/citc-tech/wadjet/internal/engine/scan"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// ExecResult contains the result of a DML operation (INSERT/UPDATE/DELETE).
type ExecResult struct {
	RowsAffected int64
	Command      string // INSERT, UPDATE, DELETE
}

// Execute runs a DML statement (INSERT/UPDATE/DELETE) and returns the result.
func (db *DB) Execute(ctx context.Context, sql string) (*ExecResult, error) {
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parsing SQL: %w", err)
	}

	switch parsed.Type {
	case plansql.QueryInsert:
		return db.executeInsert(ctx, parsed.Insert)
	case plansql.QueryDelete:
		return db.executeDelete(ctx, parsed.Delete)
	case plansql.QueryUpdate:
		return db.executeUpdate(ctx, parsed.Update)
	default:
		return nil, fmt.Errorf("Execute only supports INSERT/UPDATE/DELETE, got %v", parsed.Type)
	}
}

// executeInsert handles INSERT INTO table [(cols)] VALUES (v1, v2), ...
func (db *DB) executeInsert(ctx context.Context, info *plansql.InsertInfo) (*ExecResult, error) {
	tableMeta, err := db.catalog.GetTable(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("table %q: %w", info.Table, err)
	}

	// Determine column ordering
	columns := info.Columns
	if len(columns) == 0 {
		// No explicit columns — use schema order
		columns = make([]string, len(tableMeta.Schema.Columns))
		for i, col := range tableMeta.Schema.Columns {
			columns[i] = col.Name
		}
	}

	// Build column type map for value conversion
	typeMap := make(map[string]parquet.TypeID, len(tableMeta.Schema.Columns))
	for _, col := range tableMeta.Schema.Columns {
		typeMap[col.Name] = col.Type
	}

	// Convert parsed string values to typed rows
	var rows []map[string]any
	for rowIdx, vals := range info.Values {
		if len(vals) != len(columns) {
			return nil, fmt.Errorf("row %d: expected %d values, got %d", rowIdx, len(columns), len(vals))
		}
		row := make(map[string]any, len(columns))
		for i, colName := range columns {
			v, err := convertValue(vals[i], typeMap[colName])
			if err != nil {
				return nil, fmt.Errorf("row %d, column %q: %w", rowIdx, colName, err)
			}
			row[colName] = v
		}
		rows = append(rows, row)
	}

	// Use ingester to write rows
	ing := ingest.New(db.catalog, info.Table, tableMeta.Schema, tableMeta.PartitionKeys, ingest.DefaultConfig())
	if err := ing.Ingest(ctx, rows); err != nil {
		return nil, fmt.Errorf("ingesting rows: %w", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		return nil, fmt.Errorf("flushing rows: %w", err)
	}

	return &ExecResult{
		RowsAffected: int64(len(rows)),
		Command:      "INSERT",
	}, nil
}

// executeDelete handles DELETE FROM table [WHERE condition]
func (db *DB) executeDelete(ctx context.Context, info *plansql.DeleteInfo) (*ExecResult, error) {
	tableMeta, err := db.catalog.GetTable(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("table %q: %w", info.Table, err)
	}
	schema := tableMeta.Schema.Columns

	manifest, err := db.catalog.GetManifest(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("reading manifest for %q: %w", info.Table, err)
	}

	// Build WHERE predicate if present
	var predicate func(b *batch.RecordBatch, row int) bool
	if info.WhereSQL != "" {
		predicate, err = buildWherePredicate(info.WhereSQL)
		if err != nil {
			return nil, fmt.Errorf("parsing WHERE clause: %w", err)
		}
	}

	// Scan each file to find matching rows
	var totalDeleted int64
	var markers []catalog.DeleteMarker

	for _, part := range manifest.Partitions {
		for _, file := range part.Files {
			deleted, err := db.scanFileForDeletes(ctx, file.Path, schema, predicate)
			if err != nil {
				return nil, fmt.Errorf("scanning file %s: %w", file.Path, err)
			}
			if len(deleted) > 0 {
				markers = append(markers, catalog.DeleteMarker{
					FilePath:   file.Path,
					RowIndices: deleted,
				})
				totalDeleted += int64(len(deleted))
			}
		}
	}

	if len(markers) > 0 {
		if err := db.catalog.AddDeleteMarkers(ctx, info.Table, markers); err != nil {
			return nil, fmt.Errorf("recording delete markers: %w", err)
		}
	}

	return &ExecResult{
		RowsAffected: totalDeleted,
		Command:      "DELETE",
	}, nil
}

// executeUpdate handles UPDATE table SET col=val [WHERE condition]
func (db *DB) executeUpdate(ctx context.Context, info *plansql.UpdateInfo) (*ExecResult, error) {
	tableMeta, err := db.catalog.GetTable(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("table %q: %w", info.Table, err)
	}
	schema := tableMeta.Schema.Columns

	manifest, err := db.catalog.GetManifest(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("reading manifest for %q: %w", info.Table, err)
	}

	// Build WHERE predicate if present
	var predicate func(b *batch.RecordBatch, row int) bool
	if info.WhereSQL != "" {
		predicate, err = buildWherePredicate(info.WhereSQL)
		if err != nil {
			return nil, fmt.Errorf("parsing WHERE clause: %w", err)
		}
	}

	// Build column type map for SET clause value conversion
	typeMap := make(map[string]parquet.TypeID, len(schema))
	for _, col := range schema {
		typeMap[col.Name] = col.Type
	}

	// Scan each file: delete matching rows, collect modified rows for re-insert
	var totalUpdated int64
	var markers []catalog.DeleteMarker
	var updatedRows []map[string]any

	for _, part := range manifest.Partitions {
		for _, file := range part.Files {
			fileRows, matchedIndices, err := db.scanFileForUpdates(ctx, file.Path, schema, predicate)
			if err != nil {
				return nil, fmt.Errorf("scanning file %s: %w", file.Path, err)
			}
			if len(matchedIndices) == 0 {
				continue
			}

			markers = append(markers, catalog.DeleteMarker{
				FilePath:   file.Path,
				RowIndices: matchedIndices,
			})

			// Apply SET clauses to matched rows
			for _, idx := range matchedIndices {
				row := fileRows[idx]
				for _, sc := range info.SetClauses {
					v, err := convertValue(sc.Value, typeMap[sc.Column])
					if err != nil {
						return nil, fmt.Errorf("SET %s: %w", sc.Column, err)
					}
					row[sc.Column] = v
				}
				updatedRows = append(updatedRows, row)
			}
			totalUpdated += int64(len(matchedIndices))
		}
	}

	// Record delete markers for old rows
	if len(markers) > 0 {
		if err := db.catalog.AddDeleteMarkers(ctx, info.Table, markers); err != nil {
			return nil, fmt.Errorf("recording delete markers: %w", err)
		}
	}

	// Insert updated rows
	if len(updatedRows) > 0 {
		ing := ingest.New(db.catalog, info.Table, tableMeta.Schema, tableMeta.PartitionKeys, ingest.DefaultConfig())
		if err := ing.Ingest(ctx, updatedRows); err != nil {
			return nil, fmt.Errorf("inserting updated rows: %w", err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			return nil, fmt.Errorf("flushing updated rows: %w", err)
		}
	}

	return &ExecResult{
		RowsAffected: totalUpdated,
		Command:      "UPDATE",
	}, nil
}

// scanFileForDeletes reads a Parquet file and returns indices of rows matching the predicate.
// If predicate is nil (no WHERE), all rows are matched.
func (db *DB) scanFileForDeletes(ctx context.Context, filePath string, schema []parquet.Column, predicate func(*batch.RecordBatch, int) bool) ([]int64, error) {
	b, err := db.readParquetFile(ctx, filePath, schema)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}

	var indices []int64
	for i := 0; i < b.Len; i++ {
		if predicate == nil || predicate(b, i) {
			indices = append(indices, int64(i))
		}
	}
	return indices, nil
}

// scanFileForUpdates reads a Parquet file and returns all rows as maps plus indices of matching rows.
func (db *DB) scanFileForUpdates(ctx context.Context, filePath string, schema []parquet.Column, predicate func(*batch.RecordBatch, int) bool) ([]map[string]any, []int64, error) {
	b, err := db.readParquetFile(ctx, filePath, schema)
	if err != nil {
		return nil, nil, err
	}
	if b == nil {
		return nil, nil, nil
	}

	// Convert all rows to maps (needed for UPDATE to apply SET clauses)
	allRows := b.ToRows()

	var indices []int64
	for i := 0; i < b.Len; i++ {
		if predicate == nil || predicate(b, i) {
			indices = append(indices, int64(i))
		}
	}
	return allRows, indices, nil
}

// readParquetFile downloads and decodes a Parquet file into a RecordBatch.
func (db *DB) readParquetFile(ctx context.Context, filePath string, schema []parquet.Column) (*batch.RecordBatch, error) {
	store := db.catalog.Store()

	// Try random-access path first
	if ras, ok := store.(objstore.ReaderAtStore); ok {
		ra, size, err := ras.GetReaderAt(ctx, db.catalog.Bucket(), filePath)
		if err != nil {
			return nil, fmt.Errorf("opening file: %w", err)
		}
		defer ra.Close()

		reader, err := parquet.NewReader(ra, size)
		if err != nil {
			return nil, fmt.Errorf("opening parquet reader: %w", err)
		}
		return scan.ReadFileColumnar(reader, schema)
	}

	// Fallback: download entire file
	rc, _, err := store.Get(ctx, db.catalog.Bucket(), filePath)
	if err != nil {
		return nil, fmt.Errorf("downloading file: %w", err)
	}
	defer rc.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, rc); err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}
	data := buf.Bytes()

	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("opening parquet reader: %w", err)
	}
	return scan.ReadFileColumnar(reader, schema)
}

// buildWherePredicate parses a WHERE clause string into a batch-level predicate function.
func buildWherePredicate(whereSQL string) (func(*batch.RecordBatch, int) bool, error) {
	node, err := plansql.ParseExpression(whereSQL)
	if err != nil {
		return nil, fmt.Errorf("parsing expression %q: %w", whereSQL, err)
	}

	compiled, err := expr.Compile(node)
	if err != nil {
		return nil, fmt.Errorf("compiling expression: %w", err)
	}

	return func(b *batch.RecordBatch, row int) bool {
		v := compiled.Eval(b, row)
		if v == nil {
			return false
		}
		bv, ok := v.(bool)
		return ok && bv
	}, nil
}

// convertValue converts a string value from the parser to the appropriate Go type
// based on the target column's type.
func convertValue(s string, typ parquet.TypeID) (any, error) {
	s = strings.TrimSpace(s)

	// Handle NULL
	if strings.EqualFold(s, "null") {
		return nil, nil
	}

	// Strip quotes for string literals
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		s = s[1 : len(s)-1]
	}

	switch typ {
	case parquet.TypeBool:
		return strconv.ParseBool(s)
	case parquet.TypeInt32:
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, err
		}
		return int32(v), nil
	case parquet.TypeInt64:
		return strconv.ParseInt(s, 10, 64)
	case parquet.TypeFloat32:
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return nil, err
		}
		return float32(v), nil
	case parquet.TypeFloat64:
		return strconv.ParseFloat(s, 64)
	case parquet.TypeString:
		return s, nil
	case parquet.TypeTimestamp:
		// Try standard formats
		for _, layout := range []string{
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, s); err == nil {
				return t, nil
			}
		}
		return nil, fmt.Errorf("cannot parse timestamp %q", s)
	case parquet.TypeDate:
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			return nil, fmt.Errorf("cannot parse date %q: %w", s, err)
		}
		return t, nil
	default:
		return s, nil
	}
}
