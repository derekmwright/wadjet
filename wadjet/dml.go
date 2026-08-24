package wadjet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ExecResult contains the result of a DML operation (INSERT/UPDATE/DELETE).
type ExecResult struct {
	RowsAffected int64
	Command      string // INSERT, UPDATE, DELETE
}

// Execute runs a DML statement (INSERT/UPDATE/DELETE) and returns the result.
func (db *DB) Execute(ctx context.Context, sql string) (res *ExecResult, err error) {
	// Same seam as DB.Query: DML builds row batches (batch.FromRows) with
	// user-supplied values, so batch.TypeMismatchError (#361's guard) must
	// come back as an error, never a process exit — and since #511 so must
	// any other panic this statement reaches.
	defer func() {
		if r := recover(); r != nil {
			err = exec.RecoverQueryPanic(ctx, "embedded statement", r)
		}
	}()
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
	case plansql.QueryMerge:
		return db.executeMerge(ctx, parsed.Merge)
	default:
		return nil, fmt.Errorf("Execute only supports INSERT/UPDATE/DELETE/MERGE, got %v", parsed.Type)
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

	// Per-file streaming: box only the matched rows, commit that file's
	// delete markers, then ingest its updated rows so the ingester's
	// auto-flush bounds memory. The previous shape boxed every row of
	// every file (even at zero WHERE selectivity) and accumulated all
	// updated rows table-wide before one Ingest — a broad UPDATE held the
	// whole table as boxed maps. Markers commit before the matching
	// re-ingest, preserving the original failure direction (a crash loses
	// that file's updated rows; it never duplicates them).
	var totalUpdated int64
	var ing *ingest.Ingester

	for _, part := range manifest.Partitions {
		for _, file := range part.Files {
			b, err := db.readParquetFile(ctx, file.Path, schema)
			if err != nil {
				return nil, fmt.Errorf("scanning file %s: %w", file.Path, err)
			}
			if b == nil {
				continue
			}
			var matchedIndices []int64
			for i := 0; i < b.Len; i++ {
				if predicate == nil || predicate(b, i) {
					matchedIndices = append(matchedIndices, int64(i))
				}
			}
			if len(matchedIndices) == 0 {
				continue
			}

			// Apply SET clauses to matched rows
			updatedRows := make([]map[string]any, 0, len(matchedIndices))
			for _, idx := range matchedIndices {
				row := b.RowAt(int(idx))
				for _, sc := range info.SetClauses {
					v, err := convertValue(sc.Value, typeMap[sc.Column])
					if err != nil {
						return nil, fmt.Errorf("SET %s: %w", sc.Column, err)
					}
					row[sc.Column] = v
				}
				updatedRows = append(updatedRows, row)
			}

			marker := catalog.DeleteMarker{FilePath: file.Path, RowIndices: matchedIndices}
			if err := db.catalog.AddDeleteMarkers(ctx, info.Table, []catalog.DeleteMarker{marker}); err != nil {
				return nil, fmt.Errorf("recording delete markers: %w", err)
			}
			if ing == nil {
				ing = ingest.New(db.catalog, info.Table, tableMeta.Schema, tableMeta.PartitionKeys, ingest.DefaultConfig())
			}
			if err := ing.Ingest(ctx, updatedRows); err != nil {
				return nil, fmt.Errorf("inserting updated rows: %w", err)
			}
			totalUpdated += int64(len(matchedIndices))
		}
	}

	if ing != nil {
		if err := ing.FlushAll(ctx); err != nil {
			return nil, fmt.Errorf("flushing updated rows: %w", err)
		}
	}

	return &ExecResult{
		RowsAffected: totalUpdated,
		Command:      "UPDATE",
	}, nil
}

// executeMerge handles MERGE INTO target USING source ON condition WHEN ...
// It reads both target and source tables, joins on the ON condition, then applies
// WHEN MATCHED (UPDATE/DELETE) and WHEN NOT MATCHED (INSERT) clauses.
func (db *DB) executeMerge(ctx context.Context, info *plansql.MergeInfo) (*ExecResult, error) {
	targetMeta, err := db.catalog.GetTable(ctx, info.Target)
	if err != nil {
		return nil, fmt.Errorf("target table %q: %w", info.Target, err)
	}

	// Read all source rows via a query
	sourceSQL := fmt.Sprintf("SELECT * FROM %s", info.Source)
	sourceResult, err := db.Query(ctx, sourceSQL)
	if err != nil {
		return nil, fmt.Errorf("reading source %q: %w", info.Source, err)
	}

	// Read all target rows via a query
	targetSQL := fmt.Sprintf("SELECT * FROM %s", info.Target)
	targetResult, err := db.Query(ctx, targetSQL)
	if err != nil {
		return nil, fmt.Errorf("reading target %q: %w", info.Target, err)
	}

	// Build type map for value conversion in SET/VALUES clauses
	typeMap := make(map[string]parquet.TypeID, len(targetMeta.Schema.Columns))
	for _, col := range targetMeta.Schema.Columns {
		typeMap[col.Name] = col.Type
	}

	targetAlias := info.TargetAlias
	if targetAlias == "" {
		targetAlias = info.Target
	}
	sourceAlias := info.SourceAlias
	if sourceAlias == "" {
		sourceAlias = info.Source
	}

	// Parse ON condition into equality key pairs for row matching
	onKeys, err := parseOnKeys(info.OnCondition, targetAlias, sourceAlias)
	if err != nil {
		return nil, fmt.Errorf("parsing ON condition: %w", err)
	}

	// For each source row, check if it matches any target row
	matchedTargetIndices := make(map[int]bool)
	var rowsAffected int64
	var insertRows []map[string]any
	var deleteMarkers []catalog.DeleteMarker
	var updateRows []map[string]any

	for _, srcRow := range sourceResult.Rows {
		matched := false
		for tIdx, tgtRow := range targetResult.Rows {
			if matchByKeys(srcRow, tgtRow, onKeys) {
				matched = true
				merged := buildMergedRow(srcRow, sourceAlias, tgtRow, targetAlias)
				// Apply first matching WHEN MATCHED clause
				for _, wc := range info.WhenClauses {
					if !wc.Matched {
						continue
					}
					switch strings.ToUpper(wc.Action) {
					case "UPDATE":
						matchedTargetIndices[tIdx] = true
						updatedRow := make(map[string]any, len(tgtRow))
						for k, v := range tgtRow {
							updatedRow[k] = v
						}
						if err := applySetClauses(updatedRow, wc.SQL, merged, typeMap); err != nil {
							return nil, fmt.Errorf("applying SET: %w", err)
						}
						updateRows = append(updateRows, updatedRow)
						rowsAffected++
					case "DELETE":
						matchedTargetIndices[tIdx] = true
						rowsAffected++
					}
					break
				}
			}
		}
		if !matched {
			for _, wc := range info.WhenClauses {
				if wc.Matched {
					continue
				}
				if strings.ToUpper(wc.Action) == "INSERT" {
					newRow, err := buildInsertRow(wc.SQL, srcRow, sourceAlias, typeMap)
					if err != nil {
						return nil, fmt.Errorf("building INSERT row: %w", err)
					}
					insertRows = append(insertRows, newRow)
					rowsAffected++
				}
				break
			}
		}
	}

	// Delete all matched target rows (they'll be re-inserted if UPDATE, or dropped if DELETE)
	if len(matchedTargetIndices) > 0 {
		// We need to find the actual file positions of matched target rows.
		// Re-scan the target table to map logical row indices to file positions.
		manifest, err := db.catalog.GetManifest(ctx, info.Target)
		if err != nil {
			return nil, fmt.Errorf("reading manifest: %w", err)
		}

		logicalIdx := 0
		for _, part := range manifest.Partitions {
			for _, file := range part.Files {
				b, err := db.readParquetFile(ctx, file.Path, targetMeta.Schema.Columns)
				if err != nil {
					return nil, fmt.Errorf("reading file %s: %w", file.Path, err)
				}
				if b == nil {
					continue
				}
				var fileIndices []int64
				for i := 0; i < b.Len; i++ {
					if matchedTargetIndices[logicalIdx] {
						fileIndices = append(fileIndices, int64(i))
					}
					logicalIdx++
				}
				if len(fileIndices) > 0 {
					deleteMarkers = append(deleteMarkers, catalog.DeleteMarker{
						FilePath:   file.Path,
						RowIndices: fileIndices,
					})
				}
			}
		}

		if len(deleteMarkers) > 0 {
			if err := db.catalog.AddDeleteMarkers(ctx, info.Target, deleteMarkers); err != nil {
				return nil, fmt.Errorf("recording delete markers: %w", err)
			}
		}
	}

	// Insert new/updated rows
	allInserts := append(updateRows, insertRows...)
	if len(allInserts) > 0 {
		ing := ingest.New(db.catalog, info.Target, targetMeta.Schema, targetMeta.PartitionKeys, ingest.DefaultConfig())
		if err := ing.Ingest(ctx, allInserts); err != nil {
			return nil, fmt.Errorf("ingesting rows: %w", err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			return nil, fmt.Errorf("flushing rows: %w", err)
		}
	}

	return &ExecResult{
		RowsAffected: rowsAffected,
		Command:      "MERGE",
	}, nil
}

// buildMergedRow creates a row with columns from both source and target,
// qualified with their aliases for ON condition evaluation.
func buildMergedRow(srcRow map[string]any, srcAlias string, tgtRow map[string]any, tgtAlias string) map[string]any {
	merged := make(map[string]any, len(srcRow)+len(tgtRow))
	for k, v := range tgtRow {
		merged[k] = v
		merged[tgtAlias+"."+k] = v
	}
	for k, v := range srcRow {
		merged[k] = v
		merged[srcAlias+"."+k] = v
	}
	return merged
}

// buildAliasedRow creates a row with alias-qualified column names.
func buildAliasedRow(row map[string]any, alias string) map[string]any {
	result := make(map[string]any, len(row)*2)
	for k, v := range row {
		result[k] = v
		result[alias+"."+k] = v
	}
	return result
}

// onKeyPair represents an equality condition extracted from MERGE ON: target.col = source.col.
type onKeyPair struct {
	TargetCol string
	SourceCol string
}

// parseOnKeys parses the ON condition into equality key pairs.
// Handles "t.id = s.id" and "t.id = s.id AND t.name = s.name".
func parseOnKeys(onCond, targetAlias, sourceAlias string) ([]onKeyPair, error) {
	cond := strings.TrimSpace(onCond)
	// Split on top-level AND
	var parts []string
	upper := strings.ToUpper(cond)
	for {
		idx := strings.Index(upper, " AND ")
		if idx < 0 {
			parts = append(parts, strings.TrimSpace(cond))
			break
		}
		parts = append(parts, strings.TrimSpace(cond[:idx]))
		cond = cond[idx+5:]
		upper = upper[idx+5:]
	}

	var keys []onKeyPair
	for _, part := range parts {
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			return nil, fmt.Errorf("unsupported ON condition (expected equality): %s", part)
		}
		left := strings.TrimSpace(part[:eqIdx])
		right := strings.TrimSpace(part[eqIdx+1:])

		lAlias, lCol := splitQualifiedCol(left)
		rAlias, rCol := splitQualifiedCol(right)

		var pair onKeyPair
		if lAlias == targetAlias && rAlias == sourceAlias {
			pair = onKeyPair{TargetCol: lCol, SourceCol: rCol}
		} else if lAlias == sourceAlias && rAlias == targetAlias {
			pair = onKeyPair{TargetCol: rCol, SourceCol: lCol}
		} else {
			return nil, fmt.Errorf("ON condition columns must reference target (%s) and source (%s): %s", targetAlias, sourceAlias, part)
		}
		keys = append(keys, pair)
	}
	return keys, nil
}

// splitQualifiedCol splits "alias.col" into ("alias", "col"). Returns ("", col) if unqualified.
func splitQualifiedCol(col string) (string, string) {
	if dotIdx := strings.LastIndex(col, "."); dotIdx >= 0 {
		return col[:dotIdx], col[dotIdx+1:]
	}
	return "", col
}

// matchByKeys checks if a source row and target row match on all ON key pairs.
func matchByKeys(srcRow, tgtRow map[string]any, keys []onKeyPair) bool {
	for _, k := range keys {
		if srcRow[k.SourceCol] != tgtRow[k.TargetCol] {
			return false
		}
	}
	return true
}

// applySetClauses applies "SET col = expr, ..." to a row.
// The merged row provides both source and target column values for expressions.
func applySetClauses(row map[string]any, setSQL string, merged map[string]any, typeMap map[string]parquet.TypeID) error {
	// Strip SET keyword prefix (parser includes it in the raw SQL)
	sql := strings.TrimSpace(setSQL)
	if strings.HasPrefix(strings.ToUpper(sql), "SET ") {
		sql = sql[4:]
	}
	// Parse "col1 = expr1, col2 = expr2" from SET clause
	parts := splitSetClauses(sql)
	for _, part := range parts {
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			continue
		}
		col := strings.TrimSpace(part[:eqIdx])
		// Strip table alias prefix (e.g., "t.name" → "name")
		if dotIdx := strings.LastIndex(col, "."); dotIdx >= 0 {
			col = col[dotIdx+1:]
		}
		valExpr := strings.TrimSpace(part[eqIdx+1:])

		// Resolve the value from the merged row
		val := resolveSetValue(valExpr, merged, typeMap[col])
		row[col] = val
	}
	return nil
}

// splitSetClauses splits "col1 = val1, col2 = val2" respecting parentheses.
func splitSetClauses(s string) []string {
	var parts []string
	depth := 0
	inStr := false
	start := 0
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inStr {
			if ch == '\'' {
				inStr = false
			}
			continue
		}
		if ch == '\'' {
			inStr = true
		} else if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
		} else if ch == ',' && depth == 0 {
			parts = append(parts, strings.TrimSpace(s[start:i]))
			start = i + 1
		}
	}
	if start < len(s) {
		parts = append(parts, strings.TrimSpace(s[start:]))
	}
	return parts
}

// resolveSetValue resolves a SET expression value from the merged row context.
func resolveSetValue(expr string, merged map[string]any, targetType parquet.TypeID) any {
	expr = strings.TrimSpace(expr)

	// Try direct column reference (e.g., "s.name")
	if v, ok := merged[expr]; ok {
		return v
	}
	// Try without quotes
	if len(expr) >= 2 && expr[0] == '\'' && expr[len(expr)-1] == '\'' {
		v, _ := convertValue(expr, targetType)
		return v
	}
	// Try as literal
	v, err := convertValue(expr, targetType)
	if err == nil {
		return v
	}
	return expr
}

// buildInsertRow builds a new row from the INSERT clause of a WHEN NOT MATCHED.
// SQL format: "(col1, col2) VALUES (expr1, expr2)"
func buildInsertRow(insertSQL string, srcRow map[string]any, srcAlias string, typeMap map[string]parquet.TypeID) (map[string]any, error) {
	merged := buildAliasedRow(srcRow, srcAlias)
	sql := strings.TrimSpace(insertSQL)

	// Parse column list
	colStart := strings.Index(sql, "(")
	colEnd := strings.Index(sql, ")")
	if colStart < 0 || colEnd < 0 {
		return nil, fmt.Errorf("invalid INSERT syntax: %s", sql)
	}
	colList := sql[colStart+1 : colEnd]
	columns := splitSetClauses(colList)
	for i := range columns {
		columns[i] = strings.TrimSpace(columns[i])
	}

	// Parse VALUES
	rest := sql[colEnd+1:]
	valIdx := strings.Index(strings.ToUpper(rest), "VALUES")
	if valIdx < 0 {
		return nil, fmt.Errorf("expected VALUES in INSERT: %s", sql)
	}
	valSQL := rest[valIdx+6:]
	valStart := strings.Index(valSQL, "(")
	valEnd := strings.LastIndex(valSQL, ")")
	if valStart < 0 || valEnd < 0 {
		return nil, fmt.Errorf("invalid VALUES syntax: %s", valSQL)
	}
	values := splitSetClauses(valSQL[valStart+1 : valEnd])

	if len(columns) != len(values) {
		return nil, fmt.Errorf("column count (%d) != value count (%d)", len(columns), len(values))
	}

	row := make(map[string]any, len(columns))
	for i, col := range columns {
		val := resolveSetValue(strings.TrimSpace(values[i]), merged, typeMap[col])
		row[col] = val
	}
	return row, nil
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
// ConvertValue converts a string value to the appropriate Go type for a given Parquet type.
func ConvertValue(s string, typ parquet.TypeID) (any, error) {
	return convertValue(s, typ)
}

// convertValue's default case (return the trimmed, unquoted string as-is)
// is exactly right for six of the fourteen types that have no explicit case
// below, so they are deliberately left to it rather than given a
// pass-through case that would say nothing extra:
//
//   - TypeBytes: ingest.checkType accepts a string for BYTES, and the
//     writer's toBytes/convertStringToBytes takes the string's raw bytes.
//   - TypeIPv4, TypeIPv6, TypeMAC, TypeCIDR, TypeUUID: the writer's
//     decomposeLeaf/convertNetworkLiteral (file_writer.go) is the
//     authoritative text→binary conversion for these — it already runs on
//     whatever string reaches it, validates the literal, and raises a
//     descriptive error for a bad one (ADR-0012: PostgreSQL decides what an
//     invalid literal means, and there it is an error). Converting here too
//     would either duplicate that logic or race two different validators
//     over the same literal; TypeCIDR stores its text form directly and
//     needs no conversion either way.
//   - TypeDecimal: the writer's decimalUnscaledInt64 already parses a
//     numeric string at the column's declared scale (float64 branch),
//     matching batch.Vector.SetValue's own string case.
//
// TypePort and TypeProtocol (BUG: INSERT into either always failed) and
// TypeDuration (BUG: silently wrote 0 — see below) are NOT in that set:
// their writer-side converters only accept already-numeric Go values
// (writer.go's prepareRows int/int32/float64 switch has no string case for
// any of the three, and file_writer.go's convertStringToInt64 — the network
// string→int64 path decomposeLeaf delegates to — only knows TypeIPv4 and
// TypeMAC), so a bare string reaching them is silently read as int64(0) by
// toInt64's default arm. There is also no established literal syntax
// anywhere in the system (parser, ingest, or a named-form registration like
// Port/Protocol never got) beyond a plain integer for these three, so that
// is the form parsed here — matching checkType's accepted Go types
// (int/int32/int64/...) and TypeDuration's schema.go contract ("nanoseconds,
// stored as int64").
//
// TypeArray, TypeRow, TypeMap, TypeVector have no case and are not
// "mechanical": the INSERT VALUES parser itself (dml_parser.go's
// insertValueText) accepts only a single literal token per value — an
// array/row/map/vector literal is a composite expression it explicitly
// refuses ("Anything else is an EXPRESSION, and this path has no
// evaluator"), so convertValue never even receives one today. Supporting
// them would start at the parser grammar, not here.
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
	case parquet.TypePort, parquet.TypeProtocol:
		// Both are INT32-backed (schema.go: PORT a uint16, PROTOCOL a
		// uint8, both widened into Int32Data) and, like TypeInt32 above,
		// only ever reached a plain integer literal's text — the parser
		// has no other literal form for them and neither does the writer.
		// ParseInt(s, 10, 32) alone accepts anything an int32 holds, but
		// the STORED width is narrower — uint16 for PORT, uint8 for
		// PROTOCOL — and nothing downstream (checkType, the writer's
		// toInt32/convertStringToInt64) re-checks that narrower range, so
		// an out-of-range literal (99999, -1) silently succeeded and read
		// back verbatim: a value no real port or protocol number can be.
		// Range-check against the STORED type here, loudly, the same way
		// an out-of-range int32 literal already fails ParseInt above.
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %s value %q: %w", typ, s, err)
		}
		var lo, hi int64
		if typ == parquet.TypePort {
			lo, hi = 0, 65535
		} else {
			lo, hi = 0, 255
		}
		if v < lo || v > hi {
			return nil, fmt.Errorf("%s value %d out of range [%d, %d]", typ, v, lo, hi)
		}
		return int32(v), nil
	case parquet.TypeDuration:
		// schema.go: "nanoseconds, stored as int64" — the same unit
		// batch.Vector.GetValue reads back and toInt64/convertStringToInt64
		// (file_writer.go) do NOT special-case, unlike TypeDate/TypeTimestamp
		// below. A bare integer literal is the only accepted form.
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot parse duration value %q (nanoseconds): %w", s, err)
		}
		return v, nil
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
