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

	// Build a column map for value conversion. The whole COLUMN, not its
	// TypeID: a DECIMAL literal is judged against the declared (p, s), and
	// refusing it here rather than at the flush names the row that carried it.
	colByName := make(map[string]parquet.Column, len(tableMeta.Schema.Columns))
	for _, col := range tableMeta.Schema.Columns {
		colByName[col.Name] = col
	}

	// Convert parsed string values to typed rows
	var rows []map[string]any
	for rowIdx, vals := range info.Values {
		if len(vals) != len(columns) {
			return nil, fmt.Errorf("row %d: expected %d values, got %d", rowIdx, len(columns), len(vals))
		}
		row := make(map[string]any, len(columns))
		for i, colName := range columns {
			v, err := ConvertValueForColumn(vals[i], colByName[colName])
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

	predicate, err := BuildDMLPredicate(info.WhereSQL)
	if err != nil {
		return nil, fmt.Errorf("parsing WHERE clause: %w", err)
	}

	// Rows an earlier statement already removed are not rows this one can
	// match (#674).
	gone := catalog.DeletedRowsByFile(manifest.DeleteMarkers)

	// Scan each file to find matching rows
	var totalDeleted int64
	var markers []catalog.DeleteMarker

	for _, part := range manifest.Partitions {
		for _, file := range part.Files {
			deleted, err := db.scanFileForDeletes(ctx, file.Path, schema, predicate, gone[file.Path])
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

	predicate, err := BuildDMLPredicate(info.WhereSQL)
	if err != nil {
		return nil, fmt.Errorf("parsing WHERE clause: %w", err)
	}

	// Resolve every SET clause ONCE, against the column's full declaration,
	// BEFORE the loop below touches a file. A conversion that can fail must
	// not run after a delete marker is committed (see ConvertValueForColumn),
	// and resolving up front also means the per-row work below is a map
	// assignment that cannot fail at all.
	colByName := make(map[string]parquet.Column, len(schema))
	for _, col := range schema {
		colByName[col.Name] = col
	}
	setValues := make(map[string]any, len(info.SetClauses))
	for _, sc := range info.SetClauses {
		v, err := ConvertValueForColumn(sc.Value, colByName[sc.Column])
		if err != nil {
			return nil, fmt.Errorf("SET %s: %w", sc.Column, err)
		}
		setValues[sc.Column] = v
	}

	// Per-file streaming: box only the matched rows, hand them to the
	// ingester, then commit that file's delete markers. The previous shape
	// boxed every row of every file (even at zero WHERE selectivity) and
	// accumulated all updated rows table-wide before one Ingest — a broad
	// UPDATE held the whole table as boxed maps.
	//
	// EVERY REPLACEMENT ROW IS DURABLE BEFORE ANY MARKER IS COMMITTED. The
	// markers accumulate across the whole statement, one FlushAll follows the
	// loop, and only then does a single AddDeleteMarkers commit them.
	//
	// Committing a file's marker inside the loop is what made this per-FILE
	// rather than per-STATEMENT. Ingest only BUFFERS, so with the marker for
	// file 1 already durable and its replacement rows still in RAM, a failure
	// on file 2 — a legacy value past the column's precision, or an
	// object-store error inside the auto-flush that bounds memory — returned
	// without ever flushing, and file 1's matched rows were simply gone
	// (#647 re-review). Marker-first, the shape before that, lost them on the
	// FIRST file.
	//
	// What remains is duplication, never loss: an auto-flush that already
	// landed some replacement rows followed by a failure leaves those rows
	// beside the originals the uncommitted markers would have deleted. The
	// transactional marker+ingest commit is a known separate issue.
	var totalUpdated int64
	var ing *ingest.Ingester
	var markers []catalog.DeleteMarker

	// Rows an earlier statement already removed are not rows this one can
	// match. Without this an UPDATE re-emitted every superseded copy beside
	// the live one and marked its file again, so re-updating one row produced
	// 1, then 2, then 4 rows (#674).
	gone := catalog.DeletedRowsByFile(manifest.DeleteMarkers)

	for _, part := range manifest.Partitions {
		for _, file := range part.Files {
			b, err := db.readParquetFile(ctx, file.Path, schema)
			if err != nil {
				return nil, fmt.Errorf("scanning file %s: %w", file.Path, err)
			}
			if b == nil {
				continue
			}
			matchedIndices, err := MatchDMLRows(ctx, b, predicate, gone[file.Path])
			if err != nil {
				// A predicate that cannot answer fails the STATEMENT, before
				// any marker is committed.
				return nil, err
			}
			if len(matchedIndices) == 0 {
				continue
			}

			// Apply the already-resolved SET values to the matched rows.
			updatedRows := make([]map[string]any, 0, len(matchedIndices))
			for _, idx := range matchedIndices {
				row := b.RowAt(int(idx))
				for col, v := range setValues {
					row[col] = v
				}
				updatedRows = append(updatedRows, row)
			}

			if ing == nil {
				ing = ingest.New(db.catalog, info.Table, tableMeta.Schema, tableMeta.PartitionKeys, ingest.DefaultConfig())
			}
			if err := ing.Ingest(ctx, updatedRows); err != nil {
				return nil, fmt.Errorf("inserting updated rows: %w", err)
			}
			markers = append(markers, catalog.DeleteMarker{FilePath: file.Path, RowIndices: matchedIndices})
			totalUpdated += int64(len(matchedIndices))
		}
	}

	if ing != nil {
		if err := ing.FlushAll(ctx); err != nil {
			// No markers are committed on this path: every row this statement
			// matched is still where it was.
			return nil, fmt.Errorf("flushing updated rows: %w", err)
		}
	}

	if len(markers) > 0 {
		if err := db.catalog.AddDeleteMarkers(ctx, info.Table, markers); err != nil {
			return nil, fmt.Errorf("recording delete markers: %w", err)
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

	// Read the target's LIVE rows, each one carrying the (file, row-in-file)
	// it came from.
	//
	// It used to be `SELECT * FROM target`, and the matched rows were recorded
	// as indices into THAT result's order, while the delete-marker loop
	// re-derived physical positions by walking the manifest's file order.
	// Nothing made the two orders agree. Over a single-file target they
	// happened to; over a three-file target 8 of 12 runs deleted the WRONG
	// PHYSICAL ROW — id=2 destroyed and id=1 duplicated, on a MERGE that
	// returned success (#676). Carrying the position with the row is the only
	// way the two ends can refer to the same thing; there is no global row
	// index to be right about.
	//
	// The scan also skips rows a delete marker has already removed, which is
	// #674's rule for MERGE: a superseded copy is not a row to match, and
	// counting it would shift every position after it.
	targetRows, err := db.readMergeTarget(ctx, info.Target, targetMeta.Schema.Columns)
	if err != nil {
		return nil, err
	}

	// Build type map for value conversion in SET/VALUES clauses
	// The whole COLUMN, not its TypeID: a MERGE value is judged against the
	// target's declared (p, s) as it is resolved, before any marker is
	// written (#647 re-review).
	colByName := make(map[string]parquet.Column, len(targetMeta.Schema.Columns))
	for _, col := range targetMeta.Schema.Columns {
		colByName[col.Name] = col
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
		for tIdx := range targetRows {
			tgtRow := targetRows[tIdx].row
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
						if err := applySetClauses(updatedRow, wc.SQL, merged, colByName); err != nil {
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
					newRow, err := buildInsertRow(wc.SQL, srcRow, sourceAlias, colByName)
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

	// Mark the matched target rows (they are re-inserted if UPDATE, dropped if
	// DELETE). The position comes from the row that matched — no second scan,
	// no second ordering to disagree with the first.
	if len(matchedTargetIndices) > 0 {
		byFile := make(map[string][]int64)
		var order []string
		for tIdx := range matchedTargetIndices {
			tr := targetRows[tIdx]
			if _, seen := byFile[tr.file]; !seen {
				order = append(order, tr.file)
			}
			byFile[tr.file] = append(byFile[tr.file], tr.pos)
		}
		for _, path := range order {
			deleteMarkers = append(deleteMarkers, catalog.DeleteMarker{
				FilePath:   path,
				RowIndices: byFile[path],
			})
		}
	}

	// Insert new/updated rows BEFORE the markers that delete what they
	// replace, for the reason executeUpdate does: a row the target cannot
	// hold fails here, and committing the markers first would delete the
	// matched rows and then refuse to write their replacements (#647 review).
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

	if len(deleteMarkers) > 0 {
		if err := db.catalog.AddDeleteMarkers(ctx, info.Target, deleteMarkers); err != nil {
			return nil, fmt.Errorf("recording delete markers: %w", err)
		}
	}

	return &ExecResult{
		RowsAffected: rowsAffected,
		Command:      "MERGE",
	}, nil
}

// mergeTargetRow is one live row of a MERGE target together with WHERE IT IS.
//
// The position travels with the row because the two ends of a MERGE — the
// match and the delete marker — have to name the same physical row, and the
// only thing that can make them agree is carrying the identity rather than
// re-deriving it. Re-deriving it is what #676 was: matched rows were indices
// into `SELECT *` order and markers were re-derived from manifest order, two
// orders nothing reconciled.
type mergeTargetRow struct {
	row  map[string]any
	file string
	pos  int64 // row index WITHIN file, which is what a DeleteMarker stores
}

// readMergeTarget reads a table's live rows in manifest order, each carrying
// its (file, row-in-file).
//
// Live means the delete markers are applied, the same filter the SELECT path
// applies and the DML match scans now apply (#674): a superseded copy is not a
// row a MERGE can match, and letting it occupy a position would shift every
// row after it.
func (db *DB) readMergeTarget(ctx context.Context, table string, schema []parquet.Column) ([]mergeTargetRow, error) {
	manifest, err := db.catalog.GetManifest(ctx, table)
	if err != nil {
		return nil, fmt.Errorf("reading manifest for %q: %w", table, err)
	}
	gone := catalog.DeletedRowsByFile(manifest.DeleteMarkers)

	var out []mergeTargetRow
	for _, part := range manifest.Partitions {
		for _, file := range part.Files {
			b, err := db.readParquetFile(ctx, file.Path, schema)
			if err != nil {
				return nil, fmt.Errorf("reading file %s: %w", file.Path, err)
			}
			if b == nil {
				continue
			}
			removed := gone[file.Path]
			for i := 0; i < b.Len; i++ {
				if removed[int64(i)] {
					continue
				}
				out = append(out, mergeTargetRow{row: b.RowAt(i), file: file.Path, pos: int64(i)})
			}
		}
	}
	return out, nil
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
func applySetClauses(row map[string]any, setSQL string, merged map[string]any, colByName map[string]parquet.Column) error {
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
		val, err := resolveSetValue(valExpr, merged, colByName[col])
		if err != nil {
			return fmt.Errorf("SET %s: %w", col, err)
		}
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

// resolveSetValue resolves a SET expression value from the merged row context,
// against the TARGET COLUMN's full declaration.
//
// It used to take a TypeID and SWALLOW every conversion failure — the quoted
// arm discarded the error outright and the literal arm answered the raw
// expression TEXT — so a MERGE value the target could not hold was first
// judged at the parquet leaf, after the statement had decided what to delete
// (#647 re-review). A column REFERENCE is checked too: its box comes from the
// source table and may be a DECIMAL at another scale or past the target's
// precision, which is exactly the shape a MERGE exists to move.
func resolveSetValue(expr string, merged map[string]any, col parquet.Column) (any, error) {
	expr = strings.TrimSpace(expr)

	// Try direct column reference (e.g., "s.name")
	if v, ok := merged[expr]; ok {
		return v, checkValueForColumn(v, col)
	}
	// Try without quotes
	if len(expr) >= 2 && expr[0] == '\'' && expr[len(expr)-1] == '\'' {
		return ConvertValueForColumn(expr, col)
	}
	// Try as literal
	v, err := ConvertValueForColumn(expr, col)
	if err == nil {
		return v, nil
	}
	// Not a column reference and not a literal this converter reads. The raw
	// text is what this path has always answered for an expression it cannot
	// evaluate; it stays for the STRING targets where the text IS the value,
	// and is reported for every typed column, where storing the expression's
	// source text is a wrong value rather than an unevaluated one.
	if col.Type == parquet.TypeString {
		return expr, nil
	}
	return nil, err
}

// checkValueForColumn is ConvertValueForColumn's half for a value that is
// already a Go box rather than literal text: it validates and returns nothing,
// because there is nothing to convert.
func checkValueForColumn(v any, col parquet.Column) error {
	if v == nil || col.Type != parquet.TypeDecimal {
		return nil
	}
	_, err := parquet.DecimalValueFromBox(v, col.Precision, col.Scale)
	return err
}

// buildInsertRow builds a new row from the INSERT clause of a WHEN NOT MATCHED.
// SQL format: "(col1, col2) VALUES (expr1, expr2)"
func buildInsertRow(insertSQL string, srcRow map[string]any, srcAlias string, colByName map[string]parquet.Column) (map[string]any, error) {
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
		val, err := resolveSetValue(strings.TrimSpace(values[i]), merged, colByName[col])
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col, err)
		}
		row[col] = val
	}
	return row, nil
}

// scanFileForDeletes reads a Parquet file and returns indices of rows matching the predicate.
// If predicate is nil (no WHERE), all rows are matched.
func (db *DB) scanFileForDeletes(ctx context.Context, filePath string, schema []parquet.Column, predicate DMLPredicate, deleted map[int64]bool) ([]int64, error) {
	b, err := db.readParquetFile(ctx, filePath, schema)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	return MatchDMLRows(ctx, b, predicate, deleted)
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

// DMLPredicate is a compiled DML WHERE clause: it answers, per row of a
// scanned file, whether the statement matches it. A nil DMLPredicate matches
// every row, which is what a DML statement with no WHERE means.
type DMLPredicate func(*batch.RecordBatch, int) bool

// BuildDMLPredicate compiles a DML WHERE clause. An empty clause compiles to
// nil — "every row".
//
// It is exported because the HTTP DML executors are a second copy of the
// embedded ones and had a third and fourth copy of this compile step. A
// predicate is only half of the contract, though; MatchDMLRows is the other
// half and is the one that must be used to RUN it.
func BuildDMLPredicate(whereSQL string) (DMLPredicate, error) {
	if strings.TrimSpace(whereSQL) == "" {
		return nil, nil
	}
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

// MatchDMLRows returns the indices of b's rows the statement matches, and is
// the ONLY place a DMLPredicate is allowed to be called.
//
// deleted is the set of row positions in THIS file that a delete marker has
// already removed (catalog.DeletedRowsByFile), and passing it is not optional
// — a nil map means "this file has no markers", not "do not check". The
// parameter exists rather than a second entry point precisely because the
// defect was that every DML match scan simply did not look: an UPDATE matched
// rows its own earlier UPDATEs had superseded, re-ingested them beside the
// live copy and marked the source file again, so re-updating one row produced
// 1, then 2, then 4 rows — silently, on plain INT64 columns (#674). The
// SELECT path has applied this filter all along (scan.Scanner), which is why
// the row COUNT a client saw was wrong and the DML's own view was internally
// consistent.
//
// Expression evaluation has no error return (ADR-0019): the one class of
// condition that cannot answer with a value and must not answer with NULL —
// a division by zero, an invalid cast — raises a panic carrying a
// FatalEvalPanic, and a driver converts it back into an error with
// PostgreSQL's SQLSTATE. Every DML match scan called Eval with NO such
// boundary, so `DELETE FROM t WHERE 1/0 = 1` over HTTP returned a transport
// EOF and a goroutine dump instead of 22012 — net/http's own recover, which
// drops the connection (#677). The embedded and pgwire doors survived only
// because DB.Execute's own boundary caught it several frames up, and the
// error a caller got there named the statement rather than the predicate.
//
// The boundary is per FILE SCAN, not per row: one deferred call for a whole
// batch, no per-row cost. It owns nothing — no lock, no channel, no
// reservation — so discharging its obligations (ADR-0019 §2a) is exactly
// returning the error.
func MatchDMLRows(ctx context.Context, b *batch.RecordBatch, predicate DMLPredicate, deleted map[int64]bool) (matched []int64, err error) {
	if predicate == nil {
		matched = make([]int64, 0, b.Len)
		for i := 0; i < b.Len; i++ {
			if !deleted[int64(i)] {
				matched = append(matched, int64(i))
			}
		}
		return matched, nil
	}
	defer func() {
		if r := recover(); r != nil {
			matched, err = nil, exec.RecoverQueryPanic(ctx, "DML WHERE predicate", r)
		}
	}()
	for i := 0; i < b.Len; i++ {
		// Visibility BEFORE the predicate: a deleted row is not a row, so it
		// is not one the predicate is entitled to be asked about either.
		if deleted[int64(i)] {
			continue
		}
		if predicate(b, i) {
			matched = append(matched, int64(i))
		}
	}
	return matched, nil
}

// convertValue converts a string value from the parser to the appropriate Go type
// based on the target column's type.
// ConvertValue converts a string value to the appropriate Go type for a given Parquet type.
func ConvertValue(s string, typ parquet.TypeID) (any, error) {
	return convertValue(s, typ)
}

// convertTemporalValue reads a DATE, TIMESTAMP or DURATION literal through the
// parquet package's accept-set for that type and returns the box the writer and
// the partition-key formatter both understand.
func convertTemporalValue(s string, typ parquet.TypeID) (any, error) {
	switch typ {
	case parquet.TypeDate:
		days, err := parquet.ParseDateDays(s)
		if err != nil {
			return nil, err
		}
		return time.Unix(int64(days)*86400, 0).UTC(), nil
	case parquet.TypeTimestamp:
		ms, err := parquet.ParseTimestampMillis(s)
		if err != nil {
			return nil, err
		}
		return time.UnixMilli(ms).UTC(), nil
	default:
		return parquet.ParseDurationNanos(s)
	}
}

// ConvertValueForColumn converts a literal's text against a column's FULL
// declaration rather than its TypeID alone, so a value the column cannot hold
// is refused HERE — before the caller commits anything destructive.
//
// ConvertValue is handed a TypeID and nothing else, so a DECIMAL literal
// passes through it as text and is first judged at the parquet leaf, where the
// declared (p, s) is known. That was harmless while nothing could refuse it,
// and became DATA LOSS the moment something could: executeUpdate wrote a
// file's delete markers and ingested afterwards, so
// `UPDATE u SET d = 99999999999999999999.99` answered 22003 with the matched
// rows already deleted, and three such failures emptied a three-row table
// (#647 review). A value conversion that can fail must run before the first
// irreversible step, and this is where the declaration to check against lives.
//
// The value is VALIDATED, not rewritten: the box returned is the box the
// writer receives, so what is checked here is what is stored. Returning the
// resolved Decimal128 instead would change the box a DECIMAL partition key is
// formatted from, and the check costs one parse per literal, not per row.
func ConvertValueForColumn(s string, col parquet.Column) (any, error) {
	v, err := convertValue(s, col.Type)
	if err != nil || v == nil || col.Type != parquet.TypeDecimal {
		return v, err
	}
	if _, err := parquet.DecimalValueFromBox(v, col.Precision, col.Scale); err != nil {
		return nil, err
	}
	return v, nil
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
//   - TypeDecimal: the literal's TEXT is the exact carrier and is passed
//     through unchanged. Reading it into a number here would be wrong twice
//     over: this function is handed the column's TypeID and nothing else, so
//     it does not know the (p, s) the value has to land at, and an integer
//     literal converted to an int64 box would then be read as the ALREADY-
//     UNSCALED value ADR-0018 §4 defines (INSERT 5 into DECIMAL(9,2) would
//     store 0.05, not 5.00). parquet.DecimalValueFromBox, at the leaf where
//     the declared (p, s) is known, is the one checked converter — it parses
//     the text exactly, rounds to the column's scale as PostgreSQL does on
//     assignment, and raises 22003/22P02 rather than storing a wrapped int64
//     or a zero (#647).
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
	case parquet.TypeDuration, parquet.TypeTimestamp, parquet.TypeDate:
		// One accept-set per temporal type, shared with the writer and the
		// ingest boundary (parquet.ParseDurationNanos / ParseTimestampMillis /
		// ParseDateDays), so a literal this door takes is a literal the leaf
		// stores and a literal it refuses carries PostgreSQL's SQLSTATE.
		//
		// Three copies existed and all three were narrower than the engine's:
		// DATE took only "2006-01-02" where the filter path and the writer take
		// every unambiguous year-first spelling, and both DATE and TIMESTAMP
		// failed with a bare error carrying no code. Worse, the DATE arm's
		// time.Time box then reached a writer that had no case for it and
		// stored the EPOCH — `INSERT INTO t VALUES (1, '2020-01-01')` read back
		// as 1970-01-01 while ingest.Ingester with the same text stored the
		// date (#673).
		//
		// The BOX each returns is deliberately unchanged: a DATE and a
		// TIMESTAMP stay a time.Time, because ingest.formatPartitionValue
		// formats a temporal PARTITION KEY from that box and an integer there
		// would rename every partition directory.
		return convertTemporalValue(s, typ)
	default:
		return s, nil
	}
}
