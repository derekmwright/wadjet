package wadjet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
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
	if err := CheckDMLQualifier(info.DMLTarget); err != nil {
		return nil, err
	}
	tableMeta, err := db.catalog.GetTable(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("table %q: %w", info.Table, err)
	}
	schema := tableMeta.Schema.Columns

	manifest, err := db.catalog.GetManifest(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("reading manifest for %q: %w", info.Table, err)
	}

	predicate, err := BuildDMLPredicate(info.DMLTarget, schema)
	if err != nil {
		return nil, err
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
	if err := CheckDMLQualifier(info.DMLTarget); err != nil {
		return nil, err
	}
	tableMeta, err := db.catalog.GetTable(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("table %q: %w", info.Table, err)
	}
	schema := tableMeta.Schema.Columns

	manifest, err := db.catalog.GetManifest(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("reading manifest for %q: %w", info.Table, err)
	}

	predicate, err := BuildDMLPredicate(info.DMLTarget, schema)
	if err != nil {
		return nil, err
	}

	// Resolve every SET clause ONCE, against the schema and the column's full
	// declaration, BEFORE the loop below touches a file: an unknown target is
	// 42703 and a literal a column cannot hold is refused here rather than
	// after a delete marker is committed (#647, #678).
	assigns, err := ResolveDMLSetClauses(info.SetClauses, info.DMLTarget, schema)
	if err != nil {
		return nil, err
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

			// Apply the already-resolved SET assignments to the matched rows.
			updatedRows, err := BuildUpdatedRows(ctx, b, matchedIndices, assigns)
			if err != nil {
				return nil, err
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

	targetAlias := info.TargetAlias
	if targetAlias == "" {
		targetAlias = info.Target
	}
	sourceAlias := info.SourceAlias
	if sourceAlias == "" {
		sourceAlias = info.Source
	}

	// The resolver for every SET / VALUES expression. It carries the target's
	// declared columns — a MERGE value is judged against the target's declared
	// (p, s) as it is resolved, before any marker is written (#647 re-review)
	// — and the merged namespace an expression is evaluated in (#678).
	ev := db.buildMergeEvaluator(ctx, info, targetMeta.Schema.Columns, targetAlias, sourceAlias)

	// Parse ON condition into equality key pairs for row matching
	onKeys, err := parseOnKeys(info.OnCondition, targetAlias, sourceAlias)
	if err != nil {
		return nil, fmt.Errorf("parsing ON condition: %w", err)
	}
	// And RESOLVE them. A key column that does not exist used to match no row
	// at all, so the statement reported success having done nothing — a wrong
	// answer dressed as a no-op (#678 review, residual 3).
	if err := ev.checkOnKeys(onKeys); err != nil {
		return nil, err
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
						if err := applySetClauses(updatedRow, wc.SQL, merged, ev); err != nil {
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
					newRow, err := buildInsertRow(wc.SQL, srcRow, sourceAlias, ev)
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
			// A qualifier naming neither relation is 42P01 — the same code the
			// SET half raises for it. This failed with no SQLSTATE at all, and
			// it fails HERE, before checkOnKeys ever runs, so the code has to
			// be carried at this site (#678 re-review N2).
			for _, a := range []string{lAlias, rAlias} {
				if a != "" && a != targetAlias && a != sourceAlias {
					return nil, sqlerr.New("42P01", "missing FROM-clause entry for table %q", a)
				}
			}
			return nil, sqlerr.New("42601",
				"ON condition columns must reference target (%s) and source (%s): %s",
				targetAlias, sourceAlias, part)
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
func applySetClauses(row map[string]any, setSQL string, merged map[string]any, ev *mergeEvaluator) error {
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

		// The target column must EXIST. It used to be looked up in a map whose
		// zero value is a Column of type BOOL with an empty name, so
		// `SET nosuchcol = 1` silently assigned into a key nothing reads and
		// the statement reported success (#678).
		target, err := ev.targetColumn(col)
		if err != nil {
			return err
		}
		val, err := ev.value(valExpr, merged, target)
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

// mergeEvaluator resolves a MERGE's SET / INSERT VALUES expressions against
// the target's declared columns and the merged (source + target) row.
//
// It replaces a resolver that answered an expression it could not evaluate
// with the expression's own SOURCE TEXT. For a typed column that was reported
// as an error, but the STRING arm of the literal converter cannot fail, so for
// a STRING target the text always won: `SET s = UPPER(s.name)` stored the
// twelve characters "UPPER(s.name)" (#678). PostgreSQL evaluates it, and so
// does this.
type mergeEvaluator struct {
	target       string
	source       string
	targetAlias  string
	sourceAlias  string
	colByName    map[string]parquet.Column // the TARGET's columns, by lowercase name
	srcByName    map[string]parquet.Column // the SOURCE's columns, by lowercase name
	mergedCols   []parquet.Column          // the merged row's batch schema
	mergedByName map[string]parquet.Column // the same, by the spelling it is keyed on
	sourceKnown  bool                      // false when the source's declared schema is unavailable
}

// buildMergeEvaluator assembles the merged namespace from the two tables'
// DECLARED schemas, so an evaluated expression sees the same types the storage
// does — no inference from Go boxes, which is where a DECIMAL or a DATE (both
// boxed as strings) would be silently mistyped.
//
// The plain (unqualified) name of a column present in both resolves to the
// SOURCE, because that is what buildMergedRow's map holds: it writes the
// target's names first and the source's over them.
func (db *DB) buildMergeEvaluator(ctx context.Context, info *plansql.MergeInfo,
	targetCols []parquet.Column, targetAlias, sourceAlias string) *mergeEvaluator {

	ev := &mergeEvaluator{
		target:       info.Target,
		source:       info.Source,
		targetAlias:  strings.ToLower(targetAlias),
		sourceAlias:  strings.ToLower(sourceAlias),
		colByName:    make(map[string]parquet.Column, len(targetCols)),
		srcByName:    map[string]parquet.Column{},
		mergedByName: map[string]parquet.Column{},
	}
	for _, c := range targetCols {
		ev.colByName[strings.ToLower(c.Name)] = c
	}

	var sourceCols []parquet.Column
	if srcMeta, err := db.catalog.GetTable(ctx, info.Source); err == nil {
		sourceCols = srcMeta.Schema.Columns
		ev.sourceKnown = true
	}
	for _, c := range sourceCols {
		ev.srcByName[strings.ToLower(c.Name)] = c
	}

	plain := make(map[string]bool, len(targetCols)+len(sourceCols))
	add := func(c parquet.Column, name string) {
		c.Name = name
		ev.mergedCols = append(ev.mergedCols, c)
		if _, seen := ev.mergedByName[name]; !seen {
			ev.mergedByName[name] = c
		}
	}
	for _, c := range sourceCols {
		add(c, strings.ToLower(c.Name))
		add(c, ev.sourceAlias+"."+strings.ToLower(c.Name))
		plain[strings.ToLower(c.Name)] = true
	}
	for _, c := range targetCols {
		if !plain[strings.ToLower(c.Name)] {
			add(c, strings.ToLower(c.Name))
		}
		add(c, ev.targetAlias+"."+strings.ToLower(c.Name))
	}
	return ev
}

// checkOnKeys resolves the ON condition's key columns against the two tables.
//
// parseOnKeys already refuses a qualifier that is neither alias; what it never
// did was ask whether the COLUMN exists. `ON t.nosuchcol = s.id` matched
// nothing and the MERGE reported success with zero rows affected — a wrong
// answer dressed as a no-op, where PostgreSQL raises 42703 naming
// `t.nosuchcol` (#678 review, residual 3).
func (ev *mergeEvaluator) checkOnKeys(keys []onKeyPair) error {
	for _, k := range keys {
		if _, ok := ev.colByName[strings.ToLower(k.TargetCol)]; !ok {
			return sqlerr.New("42703", "column %s.%s does not exist", ev.targetAlias, k.TargetCol)
		}
		if ev.sourceKnown {
			if _, ok := ev.srcByName[strings.ToLower(k.SourceCol)]; !ok {
				return sqlerr.New("42703", "column %s.%s does not exist", ev.sourceAlias, k.SourceCol)
			}
		}
	}
	return nil
}

// resolveRef resolves one column reference against the merged namespace and
// returns the spelling the merged ROW is keyed on.
//
// A QUALIFIER is honoured rather than dropped. It used to be stripped —
// `merged[ref.Column]` — so `SET n = other.k` read the source's k and stored
// it, for a relation the statement does not have; PostgreSQL raises 42P01
// (#678 review R2). An unqualified name must exist somewhere in the merged
// namespace, or it is 42703 rather than the NULL it used to evaluate to.
func (ev *mergeEvaluator) resolveRef(ref *plansql.ColRef) (parquet.Column, string, error) {
	col := strings.ToLower(ref.Column)
	if ref.Table == "" {
		// A name BOTH relations spell is AMBIGUOUS, and silently picking one
		// is the worst of the three possible answers. mergedByName is filled
		// source-first, so an unqualified reference used to take the SOURCE's
		// column without a word — and the shape that decides it is the
		// canonical one: `MERGE INTO dim d USING stg s ON d.id = s.id WHEN
		// MATCHED THEN UPDATE SET name = name`, where the writer means the
		// source's and no reader of the statement can tell. PostgreSQL raises
		// 42702 and so does this (#678 re-review N1).
		_, inTarget := ev.colByName[col]
		_, inSource := ev.srcByName[col]
		if inTarget && inSource {
			return parquet.Column{}, "", sqlerr.New("42702",
				"column reference %q is ambiguous", ref.Column)
		}
		c, ok := ev.mergedByName[col]
		if !ok {
			return parquet.Column{}, "", sqlerr.New("42703", "column %q does not exist", ref.Column)
		}
		return c, col, nil
	}

	qual := strings.ToLower(ref.Table)
	var side map[string]parquet.Column
	switch qual {
	case ev.targetAlias:
		side = ev.colByName
	case ev.sourceAlias:
		if !ev.sourceKnown {
			// Nothing to check against; the row still carries the value.
			return parquet.Column{}, qual + "." + col, nil
		}
		side = ev.srcByName
	default:
		// A ROW FIELD PATH looks qualified and is not one (ADR-0022).
		if parent, ok := ev.mergedByName[qual]; ok && parent.Type == parquet.TypeRow {
			return parquet.Column{}, "", errMergeRefIsFieldPath
		}
		return parquet.Column{}, "", sqlerr.New("42P01",
			"missing FROM-clause entry for table %q", ref.Table)
	}
	c, ok := side[col]
	if !ok {
		return parquet.Column{}, "", sqlerr.New("42703", "column %s.%s does not exist", qual, ref.Column)
	}
	return c, qual + "." + col, nil
}

// errMergeRefIsFieldPath says a reference is a ROW field path, which
// resolveRef cannot answer for and the expression evaluator can.
var errMergeRefIsFieldPath = errors.New("row field path")

// checkMergeColumns resolves every column an expression names, before the
// statement writes anything.
func (ev *mergeEvaluator) checkMergeColumns(node plansql.Node) error {
	refs, err := plansql.ColumnRefs(node)
	if err != nil {
		return sqlerr.Wrap("0A000", err)
	}
	for _, ref := range refs {
		if _, _, err := ev.resolveRef(ref); err != nil && err != errMergeRefIsFieldPath {
			return err
		}
	}
	return nil
}

// targetColumn resolves a SET / INSERT target name against the target table.
func (ev *mergeEvaluator) targetColumn(name string) (parquet.Column, error) {
	col, ok := ev.colByName[strings.ToLower(strings.TrimSpace(name))]
	if !ok {
		return parquet.Column{}, sqlerr.New("42703",
			"column %q of relation %q does not exist", name, ev.target)
	}
	return col, nil
}

// value resolves one SET / VALUES expression against the merged row.
//
// A column REFERENCE is checked as well as converted: its box comes from the
// source table and may be a DECIMAL at another scale or past the target's
// precision, which is exactly the shape a MERGE exists to move (#647).
func (ev *mergeEvaluator) value(text string, merged map[string]any, col parquet.Column) (any, error) {
	text = strings.TrimSpace(text)

	node, err := plansql.ParseExpression(text)
	if err != nil {
		return nil, fmt.Errorf("parsing %q: %w", text, err)
	}
	if lit, isLit := dmlLiteralText(node); isLit {
		return assignLiteralToColumn(lit, col)
	}
	// A bare reference is read straight out of the merged row — but RESOLVED
	// first, so an unknown name is 42703 and an unknown qualifier is 42P01
	// rather than a NULL or another relation's value (#678 review R2), and
	// then ASSIGNED, so an integer box reaching a DECIMAL column is the value
	// and not the unscaled carrier (R1).
	if ref, ok := unwrapDMLParens(node).(*plansql.ColRef); ok {
		_, spelling, rerr := ev.resolveRef(ref)
		if rerr == nil {
			v := merged[spelling]
			if v == nil {
				// The merged row spells an unqualified name it also holds
				// qualified, and vice versa; either is the same value.
				for _, alt := range []string{strings.ToLower(ref.Column),
					strings.ToLower(ref.Table) + "." + strings.ToLower(ref.Column)} {
					if av, ok := merged[alt]; ok && av != nil {
						v = av
						break
					}
				}
			}
			cast, cerr := assignEvaluatedValue(v, col)
			if cerr != nil {
				return nil, cerr
			}
			return cast, checkValueForColumn(cast, col)
		}
		if rerr != errMergeRefIsFieldPath {
			return nil, rerr
		}
	}
	if err := ev.checkMergeColumns(node); err != nil {
		return nil, err
	}

	if !ev.sourceKnown {
		// Evaluating needs the source's DECLARED types; inferring them from
		// the boxed values is how a DECIMAL or a DATE (both boxed as strings)
		// would be silently mistyped. Refusing is the honest answer, and it is
		// strictly better than the source text this used to store.
		return nil, sqlerr.New("0A000",
			"MERGE cannot evaluate %q: the source %q has no declared schema to resolve it against",
			text, ev.target)
	}
	compiled, err := expr.Compile(node)
	if err != nil {
		return nil, fmt.Errorf("compiling %q: %w", text, err)
	}
	b := batch.FromRows(ev.mergedCols, []map[string]any{lowercaseKeys(merged)})
	v, err := assignEvaluatedValue(compiled.Eval(b, 0), col)
	if err != nil {
		return nil, err
	}
	if err := checkValueForColumn(v, col); err != nil {
		return nil, err
	}
	return v, nil
}

// lowercaseKeys re-spells a merged row's keys the way the merged batch schema
// names its columns, so a qualified reference written in any case resolves.
func lowercaseKeys(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[strings.ToLower(k)] = v
	}
	return out
}

// assignEvaluatedValue applies PostgreSQL's ASSIGNMENT CAST to a value the
// expression engine produced, turning it into the box the target column's
// writer stores.
//
// The rule it exists to enforce, and the one whose absence was a silent wrong
// answer: **an evaluated value is a VALUE, never a carrier.** ADR-0018 §4
// defines a STORED integer box in a DECIMAL column as the already-unscaled
// carrier — the int64 325 in a DECIMAL(9,2) column is 3.25 — and an evaluated
// int64 is nothing of the sort, it is the number itself at scale 0. Handing
// one straight to DecimalValueFromBox reopened exactly the trap the #647 arc
// closed: `UPDATE t SET d = n` with n = 10 stored 0.10, `SET d = 1 + 1` stored
// 0.02, and both returned success (#678 review R1).
//
// The whole matrix below was read off postgres:17-alpine rather than
// remembered; each arm names the rows it implements.
//
//	target INT64      5 -> 5    2.4 -> 2    2.5 -> 3    -2.5 -> -3
//	                  d (numeric 1.50) -> 2    1 + 1.4 -> 2    ABS(0-3) -> 3
//	                  3000000000 into INT32 -> 22003
//	target NUMERIC    5 -> 5.00    2.567 -> 2.57    n (bigint 10) -> 10.00
//	                  1 + 1 -> 2.00    d * 2 -> 3.00    n + 1 -> 11.00
//	                  99999999.99 into (9,2) -> 22003
//	target FLOAT8     n -> 10    d -> 1.5    1 + 1 -> 2
//	target TEXT       5 -> '5'   n -> '10'   d -> '1.50'   UPPER(s) -> 'X'
func assignEvaluatedValue(v any, col parquet.Column) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch col.Type {
	case parquet.TypeDecimal:
		return assignDecimalValue(v, col)
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypePort, parquet.TypeProtocol:
		return assignIntegerValue(v, col)
	case parquet.TypeFloat32, parquet.TypeFloat64:
		return assignFloatValue(v, col)
	case parquet.TypeString:
		return assignTextValue(v, col)
	}
	return v, nil
}

// assignDecimalValue is the R1 fix. An INTEGER box is rendered to its decimal
// TEXT before it reaches DecimalValueFromBox, because that function reads an
// integer as the UNSCALED carrier (ADR-0018 §4) and reads text as the VALUE.
// Every other box the engine produces for a numeric expression — a float64
// from real arithmetic, the numeric text a DECIMAL column reads back as — is
// already on the value path and is left alone.
func assignDecimalValue(v any, col parquet.Column) (any, error) {
	switch t := v.(type) {
	case bool:
		return nil, datatypeMismatch(v, col)
	case int:
		v = strconv.FormatInt(int64(t), 10)
	case int8:
		v = strconv.FormatInt(int64(t), 10)
	case int16:
		v = strconv.FormatInt(int64(t), 10)
	case int32:
		v = strconv.FormatInt(int64(t), 10)
	case int64:
		v = strconv.FormatInt(t, 10)
	}
	// Resolved and validated here so the failure names the SET clause rather
	// than a flush, and rounded to the column's scale by the one checked
	// converter (#647).
	d, err := parquet.DecimalValueFromBox(v, col.Precision, col.Scale)
	if err != nil {
		return nil, err
	}
	// And handed on as the CANONICAL text at the column's scale, not as
	// whatever box arrived. Two things read this box, and only one of them
	// re-derives the value: the writer parses it again, but
	// ingest.formatPartitionValue prints it VERBATIM into the partition
	// directory name. So an INSERT of 10.00 wrote `d=10.00` while
	// `UPDATE SET d = n` with n = 10 wrote `d=10` — one value, two
	// directories, and a scan that has to read both to answer for either
	// (#678 re-review N4). Rendering here makes the two paths agree by
	// construction; PostgreSQL renders numeric(9,2) the same way
	// (10::bigint::numeric(9,2) is 10.00).
	return d.Text(col.Scale), nil
}

// datatypeMismatch is PostgreSQL's 42804 for a value whose TYPE the column
// cannot take at all — as opposed to 22P02 (the text does not spell a value of
// that type) or 22003 (it does, and the column cannot hold it).
//
// BOOL is the case that reaches it: `SET n = b` used to fail at
// ingest.checkType with "expected integer, got bool" and no SQLSTATE, and
// `SET d = b` reached DecimalValueFromBox's default and answered 22P02, where
// PostgreSQL says 42804 for both (#678 re-review N3). A bool assigned to a
// TEXT column is NOT here: PostgreSQL accepts it and stores 'true'.
func datatypeMismatch(v any, col parquet.Column) error {
	return sqlerr.New("42804", "column %q is of type %s but expression is of type %s",
		col.Name, col.Type, dmlBoxTypeName(v))
}

func dmlBoxTypeName(v any) string {
	switch v.(type) {
	case bool:
		return "boolean"
	case int, int8, int16, int32, int64:
		return "bigint"
	case float32, float64:
		return "double precision"
	case string:
		return "text"
	}
	return fmt.Sprintf("%T", v)
}

// assignIntegerValue rounds, ranges and narrows a value into an integer
// column.
//
// A fractional value ROUNDS half away from zero and only a value outside the
// column's range (NaN and the infinities included) is 22003 — PostgreSQL's
// numeric-to-integer assignment cast.
//
// TODO(#699): PostgreSQL rounds a float8 half to EVEN and a numeric half AWAY
// from zero, and this engine boxes both families as float64, so one rule has
// to serve both. Half-away-from-zero is kept because it matches the far
// commoner spelling (`SET n = 0 - 2.5` is -3 in PostgreSQL); `SET n = f` with
// a FLOAT64 column holding exactly 2.5 stores 3 where PostgreSQL stores 2.
// Closing it needs the source expression's declared TYPE, which expr.Expr does
// not carry. The divergence is PINNED, not merely described, by
// TestFloatHalfRoundingIsPinnedToTheNumericRule — changing this rule fails
// that test in either direction.
//
// The range check reaches PORT (uint16) and PROTOCOL (uint8) too, because
// nothing below this line re-checks either — convertValue does, but only for
// literals — so an out-of-range computed value would truncate into a port no
// real port can be.
func assignIntegerValue(v any, col parquet.Column) (any, error) {
	var n int64
	switch t := v.(type) {
	case bool:
		return nil, datatypeMismatch(v, col)
	case int64:
		n = t
	case int32:
		n = int64(t)
	case int:
		n = int64(t)
	case float64:
		r := math.Round(t) // Go's Round is half AWAY FROM ZERO, like PostgreSQL numeric's
		if math.IsNaN(r) || math.IsInf(r, 0) || r < -9.223372036854776e18 || r > 9.223372036854776e18 {
			return nil, sqlerr.New("22003", "%s out of range", col.Type)
		}
		n = int64(r)
	case float32:
		return assignIntegerValue(float64(t), col)
	case string:
		// The box a DECIMAL column reads back as. DecimalValueFromText at
		// scale 0 IS the rounding rule, exactly, and it refuses text that
		// names no number (22P02) and a magnitude no int64 holds (22003).
		d, err := parquet.DecimalValueFromText(t, parquet.MaxDecimalDigits, 0)
		if err != nil {
			return nil, err
		}
		i, fits := d.Int64()
		if !fits {
			return nil, sqlerr.New("22003", "%s out of range", col.Type)
		}
		n = i
	default:
		return v, nil
	}
	lo, hi := int64(math.MinInt64), int64(math.MaxInt64)
	switch col.Type {
	case parquet.TypeInt32:
		lo, hi = math.MinInt32, math.MaxInt32
	case parquet.TypePort:
		lo, hi = 0, 65535
	case parquet.TypeProtocol:
		lo, hi = 0, 255
	}
	if n < lo || n > hi {
		return nil, sqlerr.New("22003", "%s value %d out of range [%d, %d]", col.Type, n, lo, hi)
	}
	if col.Type == parquet.TypeInt64 {
		return n, nil
	}
	return int32(n), nil
}

// assignFloatValue accepts the numeric text a DECIMAL column reads back as;
// every other numeric box a float column can already hold (ingest.checkType
// takes float32, float64, int, int32, int64).
func assignFloatValue(v any, col parquet.Column) (any, error) {
	if _, isBool := v.(bool); isBool {
		return nil, datatypeMismatch(v, col)
	}
	s, ok := v.(string)
	if !ok {
		return v, nil
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return nil, sqlerr.New("22P02", "invalid input syntax for type %s: %q", col.Type, s)
	}
	return f, nil
}

// assignTextValue renders a numeric box as the text PostgreSQL assigns:
// `SET s = n` stores '10' and `SET s = d` stores '1.50'. A float is rendered
// shortest-round-trip, which is what PostgreSQL prints for float8 at its
// default extra_float_digits.
func assignTextValue(v any, _ parquet.Column) (any, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case int32:
		return strconv.FormatInt(int64(t), 10), nil
	case int:
		return strconv.FormatInt(int64(t), 10), nil
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(t), 'g', -1, 32), nil
	case bool:
		if t {
			return "true", nil
		}
		return "false", nil
	}
	return v, nil
}

// assignLiteralToColumn converts a SET literal, falling back to the assignment
// cast for the one class the literal converter cannot read.
//
// ConvertValueForColumn is the literal path, and it is the one that carries
// #647's declaration checks (a DECIMAL literal parsed exactly from its digits,
// the temporal accept-sets), so it goes first and its answer wins whenever it
// has one. It has no rule for a FRACTIONAL literal assigned to an integer
// column, though: `SET n = 2.4` failed in strconv.ParseInt where PostgreSQL
// rounds it to 2 — and the same value written as an expression (`SET n =
// 1 + 1.4`) already rounded, so one value had two answers depending on how it
// was spelled (#678 review). The fallback is the same assignment cast the
// expression path uses, so now it has one.
func assignLiteralToColumn(text string, col parquet.Column) (any, error) {
	v, err := ConvertValueForColumn(text, col)
	if err == nil {
		return v, nil
	}
	switch col.Type {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypePort, parquet.TypeProtocol:
		// The cast's answer wins outright here, error included: strconv's
		// "invalid syntax" and "value out of range" carry no SQLSTATE, while
		// the cast raises PostgreSQL's own 22P02 for text naming no number
		// and 22003 for a magnitude the column cannot hold.
		return assignEvaluatedValue(text, col)
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
func buildInsertRow(insertSQL string, srcRow map[string]any, srcAlias string, ev *mergeEvaluator) (map[string]any, error) {
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
		target, err := ev.targetColumn(col)
		if err != nil {
			return nil, err
		}
		val, err := ev.value(strings.TrimSpace(values[i]), merged, target)
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

// CheckDMLQualifier accepts the schema/catalog qualifier of a DML relation
// when it names this server's own schema, and refuses any other.
//
// `DELETE FROM public.orders` is what a PostgreSQL client writes by default,
// and the DML parser used to read only the first identifier — so the
// statement addressed a table named "public". This is the SELECT path's rule
// (logical/builder.go, buildScan) applied to the DML doors; PostgreSQL 17
// answers an unknown qualifier with 42P01.
func CheckDMLQualifier(target plansql.DMLTarget) error {
	switch q := strings.ToLower(target.Qualifier); q {
	case "", expr.SessionSchema, expr.SessionCatalog, expr.SessionCatalog + "." + expr.SessionSchema:
		return nil
	default:
		return sqlerr.New("42P01", "relation %q does not exist: this server has one schema, %q, in database %q",
			target.Qualifier+"."+target.Table, expr.SessionSchema, expr.SessionCatalog)
	}
}

// checkDMLColumns resolves every column an expression names against a table's
// declared schema, raising PostgreSQL's 42703 for one that does not exist.
//
// A qualifier is accepted only when it names the relation itself; anything
// else (`other.col`) is a reference to a relation the statement does not
// have, which PostgreSQL reports as 42P01. A ROW FIELD PATH (`rw.f`) is a
// qualified-looking name that is NOT a table reference (ADR-0022), so a
// qualifier matching a ROW column of this table resolves as one.
//
// An ALIAS HIDES THE TABLE NAME, which is PostgreSQL's rule and not a detail:
// once `DELETE FROM pr AS a` is written, `pr.id` names nothing and PG answers
// 42P01 with the hint that `a` was meant. Accepting both spellings would let
// the same statement mean two things depending on which one resolved (#686).
func checkDMLColumns(node plansql.Node, target plansql.DMLTarget, schema []parquet.Column) error {
	refs, err := plansql.ColumnRefs(node)
	if err != nil {
		return sqlerr.Wrap("0A000", err)
	}
	byName := make(map[string]parquet.Column, len(schema))
	for _, c := range schema {
		byName[strings.ToLower(c.Name)] = c
	}
	// The one name a qualifier may spell: the alias when there is one, the
	// table when there is not.
	relation := target.Table
	if target.Alias != "" {
		relation = target.Alias
	}
	for _, ref := range refs {
		if ref.Table != "" && !strings.EqualFold(ref.Table, relation) {
			// A ROW field path, not a relation: `rw.f` where rw is a ROW
			// column of this table.
			if parent, ok := byName[strings.ToLower(ref.Table)]; ok && parent.Type == parquet.TypeRow {
				continue
			}
			if target.Alias != "" && strings.EqualFold(ref.Table, target.Table) {
				return sqlerr.New("42P01",
					"invalid reference to FROM-clause entry for table %q; perhaps you meant to reference the alias %q",
					ref.Table, target.Alias)
			}
			return sqlerr.New("42P01", "missing FROM-clause entry for table %q", ref.Table)
		}
		if _, ok := byName[strings.ToLower(ref.Column)]; !ok {
			// PostgreSQL's system columns resolve to NULL here rather than to
			// an address this engine cannot honour, which is what makes a
			// client's `DELETE ... WHERE ctid = '(0,1)'` match nothing instead
			// of addressing a row the user never saw (physical.validate).
			// The query path makes that allowance; so must this one.
			if physical.IsPGSystemColumn(ref.Column) {
				continue
			}
			return sqlerr.New("42703", "column %q does not exist", ref.Column)
		}
	}
	return nil
}

// BuildDMLPredicate compiles a DML WHERE clause against a table's schema. An
// empty clause compiles to nil — "every row".
//
// The SCHEMA is a parameter, not an optional extra, because the DML doors do
// not go through the planner and so had no name-resolution step at all:
// `UPDATE t SET n = 1 WHERE nosuchcol = 1` compiled fine, evaluated to NULL on
// every row and reported "UPDATE 0", where PostgreSQL raises 42703 (#678).
// Every column the clause names is resolved here, before anything executes.
//
// It is exported because the HTTP DML executors are a second copy of the
// embedded ones and had a third and fourth copy of this compile step. A
// predicate is only half of the contract, though; MatchDMLRows is the other
// half and is the one that must be used to RUN it.
//
// THE EMPTY-PREDICATE BACKSTOP. A nil predicate is the widest answer this
// function can give — every row of the table — so "the statement had no
// WHERE" and "the parser dropped the statement's WHERE" must not look the
// same here. They did, and the second one emptied tables: a DELETE with an
// aliased table returned an empty WhereSQL and deleted everything (#686). The
// check below is not about that spelling, which the parser now reads; it
// makes the CLASS unreachable, so the next clause any parser path fails to
// carry fails the STATEMENT instead of widening it (ADR-0019, correctness-fix
// protocol item 8: loud beats plausible).
func BuildDMLPredicate(target plansql.DMLTarget, schema []parquet.Column) (DMLPredicate, error) {
	whereSQL := strings.TrimSpace(target.WhereSQL)
	if whereSQL == "" {
		if plansql.HasTopLevelWhereToken(target.StmtSQL) {
			return nil, sqlerr.New("XX000",
				"refusing to run %q unconditionally: it writes a WHERE clause that this server parsed to nothing",
				target.StmtSQL)
		}
		return nil, nil
	}
	// COMPLETE, not "as much as the grammar could use". A WHERE that parses
	// to a PREFIX is the #686 class by another route: the dropped tail is a
	// conjunct that would have NARROWED the statement, so running the prefix
	// deletes rows the written predicate excludes. `id > 0 AND name @@ 'zzz'`
	// emptied the table (#686 review). PostgreSQL answers 42601.
	node, err := plansql.ParseExpressionComplete(whereSQL)
	if err != nil {
		return nil, sqlerr.Wrap("42601", fmt.Errorf("parsing WHERE %q: %w", whereSQL, err))
	}
	if err := checkDMLColumns(node, target, schema); err != nil {
		return nil, err
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

// DMLAssignment is one resolved `SET column = ...`: the target column's full
// declaration, plus EITHER a constant or a compiled expression.
type DMLAssignment struct {
	Column   string
	col      parquet.Column
	constant any       // used when expr is nil
	expr     expr.Expr // per-row evaluation
}

// ResolveDMLSetClauses resolves an UPDATE's SET list against the table's
// schema, before anything executes.
//
// Two defects met here (#678). `UPDATE t SET nosuchcol = 1` reported
// "UPDATE 1": the assignment was dropped into a map nothing read and the
// matched rows were rewritten unchanged, where PostgreSQL raises 42703. And
// the value was read ONLY as a literal, through a converter whose STRING arm
// cannot fail — so `SET s = UPPER(s)` stored the seven characters "UPPER(s)"
// into the column. PostgreSQL evaluates it, and so does this now.
//
// Whether a SET value is a literal is decided from its PARSE, not from
// whether a conversion succeeded, because for a STRING column the conversion
// always succeeds and the literal path always won. A `*plansql.Lit` takes the
// constant path — which is what keeps #647's declaration checks
// (ConvertValueForColumn's DECIMAL precision, the temporal accept-sets)
// running on the values that have them; anything else is compiled and
// evaluated per row against the file's own batch, which carries the table's
// declared types.
//
// A SET VALUE resolves against the same relation name the WHERE does, so
// `UPDATE pr AS a SET n = a.n + 1` reads a.n and `SET n = pr.n` under that
// alias is 42P01 — PostgreSQL's answer for both (#686).
func ResolveDMLSetClauses(clauses []plansql.SetClause, target plansql.DMLTarget, schema []parquet.Column) ([]DMLAssignment, error) {
	byName := make(map[string]parquet.Column, len(schema))
	for _, c := range schema {
		byName[strings.ToLower(c.Name)] = c
	}
	out := make([]DMLAssignment, 0, len(clauses))
	for _, sc := range clauses {
		// A QUALIFIED target (`SET t.n = 1`) never arrives here: the UPDATE
		// parser requires `=` after the column name and refuses the dot.
		// PostgreSQL refuses it too, reading the qualifier as a column of the
		// relation and raising 42703 where this raises 42601; both refuse,
		// and the statement writes nothing either way. MERGE spells its own
		// qualified targets and strips them in applySetClauses.
		name := strings.ToLower(strings.TrimSpace(sc.Column))
		col, ok := byName[name]
		if !ok {
			// The RELATION is named, not the alias: PostgreSQL reports
			// `column "nosuch" of relation "pr" does not exist` for
			// `UPDATE pr AS a SET nosuch = 1` (verified on 17.11).
			return nil, sqlerr.New("42703", "column %q of relation %q does not exist", name, target.Table)
		}

		// COMPLETE, for the reason BuildDMLPredicate gives: a SET value that
		// parses to a prefix stored the prefix's value and reported success.
		node, err := plansql.ParseExpressionComplete(sc.Value)
		if err != nil {
			return nil, sqlerr.Wrap("42601", fmt.Errorf("SET %s: parsing %q: %w", name, sc.Value, err))
		}
		if text, isLit := dmlLiteralText(node); isLit {
			v, err := assignLiteralToColumn(text, col)
			if err != nil {
				return nil, fmt.Errorf("SET %s: %w", name, err)
			}
			out = append(out, DMLAssignment{Column: name, col: col, constant: v})
			continue
		}
		if err := checkDMLColumns(node, target, schema); err != nil {
			return nil, fmt.Errorf("SET %s: %w", name, err)
		}
		compiled, err := expr.Compile(node)
		if err != nil {
			return nil, fmt.Errorf("SET %s: compiling %q: %w", name, sc.Value, err)
		}
		out = append(out, DMLAssignment{Column: name, col: col, expr: compiled})
	}
	return out, nil
}

// dmlLiteralText reports whether an expression is a CONSTANT, and if so gives
// the text ConvertValueForColumn should read.
//
// The text comes from the parsed node, not from the clause's source, because
// the two are not the same string: the lexer resolves a string literal's
// doubled-apostrophe escapes, so a literal spelling `it` + escape + `s`
// re-quoted for parsing still carries the escape while its VALUE is `it's`.
// Reading it back off the node is exact.
//
// A sign in front of a number is part of the constant. The SET text is
// rebuilt from tokens, which puts a space there (`- 1.50`), and routing that
// through the expression evaluator instead would resolve a DECIMAL through a
// float64 — inexact for values a float cannot hold, where the literal path
// parses the digits (ADR-0018 §4).
func dmlLiteralText(n plansql.Node) (string, bool) {
	switch e := unwrapDMLParens(n).(type) {
	case *plansql.Lit:
		if e.Kind == plansql.LitNull {
			return "NULL", true
		}
		return e.Value, true
	case *plansql.UnaryOp:
		if e.Op != "-" && e.Op != "+" {
			return "", false
		}
		inner, ok := unwrapDMLParens(e.Inner).(*plansql.Lit)
		if !ok || inner.Kind != plansql.LitNumber {
			return "", false
		}
		if e.Op == "+" {
			return inner.Value, true
		}
		return "-" + inner.Value, true
	}
	return "", false
}

func unwrapDMLParens(n plansql.Node) plansql.Node {
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok {
			return n
		}
		n = p.Inner
	}
}

// BuildUpdatedRows boxes b's matched rows with the assignments applied.
//
// It carries the same panic boundary MatchDMLRows does and for the same
// reason: a SET expression is evaluated by the same engine as a WHERE, so
// `SET n = 1/0` raises a FatalEvalPanic that must become 22012 on the
// statement rather than a dead connection (ADR-0019, #677). One deferred call
// per file, nothing per row.
//
// A value the target column cannot hold is refused HERE, before any delete
// marker is committed — the rule #647 established for literals, applied to
// computed values too.
func BuildUpdatedRows(ctx context.Context, b *batch.RecordBatch, matched []int64, assigns []DMLAssignment) (rows []map[string]any, err error) {
	defer func() {
		if r := recover(); r != nil {
			rows, err = nil, exec.RecoverQueryPanic(ctx, "DML SET expression", r)
		}
	}()
	rows = make([]map[string]any, 0, len(matched))
	for _, idx := range matched {
		row := b.RowAt(int(idx))
		for i := range assigns {
			a := &assigns[i]
			if a.expr == nil {
				row[a.Column] = a.constant
				continue
			}
			v, err := assignEvaluatedValue(a.expr.Eval(b, int(idx)), a.col)
			if err != nil {
				return nil, fmt.Errorf("SET %s: %w", a.Column, err)
			}
			if err := checkValueForColumn(v, a.col); err != nil {
				return nil, fmt.Errorf("SET %s: %w", a.Column, err)
			}
			row[a.Column] = v
		}
		rows = append(rows, row)
	}
	return rows, nil
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
