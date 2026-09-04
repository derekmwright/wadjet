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

// Tag renders this result the way PostgreSQL's CommandComplete does.
//
// It is a method on the result rather than a private helper in one door
// because the doors DISAGREED: pgwire special-cased INSERT to PostgreSQL's
// three-field `INSERT <oid> <rows>` form and the HTTP door rendered
// `fmt.Sprintf("%s %d", …)`, so the same statement was `INSERT 0 3` over the
// wire and `INSERT 3` over REST — while docs/api-reference.md claimed the tag
// does not depend on the door (review B8). One renderer is the only way that
// claim can be true.
func (r *ExecResult) Tag() string { return CommandTag(r.Command, r.RowsAffected) }

// CommandTag is Tag for a caller holding the verb and the count separately.
//
// PostgreSQL's INSERT tag carries an oid field that has been fixed at 0 since
// 12; every other verb is `VERB <rows>`. Measured, not remembered.
func CommandTag(command string, rows int64) string {
	cmd := strings.ToUpper(strings.TrimSpace(command))
	if cmd == "" {
		cmd = "SELECT"
	}
	if cmd == "INSERT" {
		return fmt.Sprintf("INSERT 0 %d", rows)
	}
	return fmt.Sprintf("%s %d", cmd, rows)
}

// Execute runs a DML statement (INSERT/UPDATE/DELETE/MERGE) and returns the
// result.
func (db *DB) Execute(ctx context.Context, sql string) (*ExecResult, error) {
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parsing SQL: %w", err)
	}
	return db.ExecuteParsed(ctx, parsed)
}

// ExecuteParsed runs an already-parsed DML statement.
//
// It is the ONE DML entry point (#815): the embedded door, the pgwire door and
// the HTTP API server all reach the executors below through it, so a fix is
// written once and every door carries the same table state, command tag and
// SQLSTATE. It is exported for the callers that have already parsed — the HTTP
// handler routes on the statement type before dispatching — and for the tests
// that drive a synthesized statement no text can spell.
func (db *DB) ExecuteParsed(ctx context.Context, parsed *plansql.ParsedQuery) (res *ExecResult, err error) {
	// Same seam as DB.Query: DML builds row batches (batch.FromRows) with
	// user-supplied values, so batch.TypeMismatchError (#361's guard) must
	// come back as an error, never a process exit — and since #511 so must
	// any other panic this statement reaches. The HTTP door relies on this
	// boundary too: net/http answers a panic by dropping the connection, so
	// without it a panicking statement reached that client as a transport EOF
	// instead of a SQLSTATE (#677).
	defer func() {
		if r := recover(); r != nil {
			err = exec.RecoverQueryPanic(ctx, "embedded statement", r)
		}
	}()

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

// resolveInsertColumns turns an INSERT's column list into the columns it
// names, refusing one the table does not have.
//
// It returns the STORED name for each position (so the row map is keyed the
// way the schema is) beside the whole parquet.Column (so a DECIMAL literal is
// judged against the declared (p, s) here rather than at the flush, which is
// what names the row that carried it — #647).
//
// The message and the class are PostgreSQL's, and the lookup is
// case-insensitive for the same reason ResolveDMLSetClauses' is: INSERT was
// the one DML clause that resolved case-SENSITIVELY, so `INSERT INTO t (ID)`
// failed on a table whose column is `id` while `UPDATE t SET ID = …`
// succeeded.
func resolveInsertColumns(named []string, table string, schema []parquet.Column) ([]string, []parquet.Column, error) {
	if len(named) == 0 {
		// No explicit list: schema order, every column.
		names := make([]string, len(schema))
		cols := make([]parquet.Column, len(schema))
		for i, col := range schema {
			names[i], cols[i] = col.Name, col
		}
		return names, cols, nil
	}
	byName := make(map[string]parquet.Column, len(schema))
	for _, col := range schema {
		byName[strings.ToLower(col.Name)] = col
	}
	names := make([]string, len(named))
	cols := make([]parquet.Column, len(named))
	seen := make(map[string]bool, len(named))
	for i, raw := range named {
		name := strings.ToLower(strings.TrimSpace(raw))
		col, ok := byName[name]
		if !ok {
			return nil, nil, sqlerr.New("42703", "column %q of relation %q does not exist", name, table)
		}
		if seen[name] {
			// PostgreSQL: 42701, `column "x" specified more than once`.
			// Without this the second value silently overwrote the first in
			// the row map and the statement reported success.
			return nil, nil, sqlerr.New("42701", "column %q specified more than once", name)
		}
		seen[name] = true
		names[i], cols[i] = col.Name, col
	}
	return names, cols, nil
}

// dmlRelationError is a DML door's table lookup reported the way the SELECT
// door already reports it.
//
// #719: all four doors wrapped catalog.GetTable's miss with %w and handed the
// client `table "x": table "x" not found` and NO SQLSTATE, while
// `MERGE ... USING nosuchtable` — which reaches the relation through db.Query
// and therefore through the planner — answered 42P01 with PostgreSQL's own
// wording. One statement class, two dispositions, and they disagreed on the
// MESSAGE as well as the class. PostgreSQL 17 says
// `relation "nosuchtable" does not exist`; so does this now, on every door.
//
// A transport failure is deliberately NOT 42P01: the table's existence is
// unknown then, which is the same distinction physical.validate makes on the
// read path.
func dmlRelationError(name string, err error) error {
	if errors.Is(err, catalog.ErrTableNotFound) {
		return sqlerr.New("42P01", "relation %q does not exist", name)
	}
	return fmt.Errorf("table %q: %w", name, err)
}

// executeInsert handles INSERT INTO table [(cols)] VALUES (v1, v2), ...
func (db *DB) executeInsert(ctx context.Context, info *plansql.InsertInfo) (*ExecResult, error) {
	// A DML statement REFERENCES an existing table, so it takes the same READ
	// concession a SELECT does (catalog.ResolveTableName): a mixed-case name
	// created through parquet or ingest stays reachable unquoted. Rewriting
	// info.Table in place is what makes the WRITE land on the table the read
	// resolved — a door that conceded on the lookup and then keyed the
	// manifest byte-exact would write somewhere else. CreateTable does NOT
	// concede: minting a name is not referencing one.
	info.Table = db.catalog.ResolveTableName(info.Table)
	tableMeta, err := db.catalog.GetTable(ctx, info.Table)
	if err != nil {
		return nil, dmlRelationError(info.Table, err)
	}

	// RESOLVE the column list against the table, before a single value is
	// converted and long before anything is written.
	//
	// This used to be `columns := info.Columns` taken verbatim, and the
	// lookup below was `colByName[colName]` with no `ok`. A miss yielded the
	// ZERO parquet.Column, whose Type is TypeBool (the zero of the iota
	// block), so convertValue took `strconv.ParseBool`: `ParseBool("1")`
	// SUCCEEDS, the row was built with a key no column has, and
	// ingest.validateRow iterates the SCHEMA rather than the row so an extra
	// key is structurally invisible to it. `INSERT INTO pr (id, nosuchcol)
	// VALUES (9, 1)` therefore answered `INSERT 0 1` having silently dropped
	// the typo'd column and written a row with a missing value — a user's
	// typo becoming data (#814). The mild face, when ParseBool also failed,
	// was `strconv.ParseBool: parsing "zz"` for a column that does not exist.
	columns, cols, err := resolveInsertColumns(info.Columns, info.Table, tableMeta.Schema.Columns)
	if err != nil {
		return nil, err
	}

	// Convert parsed string values to typed rows
	var rows []map[string]any
	for rowIdx, vals := range info.Values {
		if len(vals) != len(columns) {
			// PostgreSQL: 42601, `INSERT has more target columns than
			// expressions` / `more expressions than target columns`.
			return nil, sqlerr.New("42601",
				"row %d: INSERT has %d target column(s) and %d expression(s)",
				rowIdx, len(columns), len(vals))
		}
		row := make(map[string]any, len(columns))
		for i, colName := range columns {
			// assignLiteralToColumn, not ConvertValueForColumn: the ASSIGNMENT
			// CAST is part of what a literal means, and INSERT was the one
			// verb that did not get it. `INSERT INTO t (n) VALUES (2.5)` into
			// an INT64 column failed with `strconv.ParseInt: parsing "2.5"`
			// and NO SQLSTATE, while `UPDATE t SET n = 2.5` stored 3 — an
			// INSERT-vs-UPDATE split inside one engine, and a divergence from
			// PostgreSQL, which stores 3 (review P6). It also carries the
			// classes the cast raises, so an out-of-range INSERT is 22003 and
			// unreadable text is 22P02 rather than the blanket 42000 (P18).
			v, err := assignLiteralToColumn(vals[i], cols[i])
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

// dmlCommitAttempts bounds how many times a DML statement re-reads the
// manifest and redoes its scan after finding that the table changed under it.
//
// A retry is not a fallback: the statement observed a manifest, matched rows
// in files that manifest named, and CommitDML refused — because compaction
// rewrote those files (#691) or because another STATEMENT already superseded
// a row this one is about to supersede (#835). Redoing the scan against the
// manifest that replaced it is the only way to answer the statement
// correctly, and it is what "one CAS against the revision you read" means for
// a statement that must also write. A statement that keeps losing the race
// reports 40001, the class PostgreSQL gives a client that should retry.
const dmlCommitAttempts = 5

// retryConflicted runs one DML statement, redoing it whole while CommitDML
// reports that the manifest it read no longer describes the rows it matched.
//
// Both refusals are the same answer — "you read a state that is gone, read
// again" — and both are redone the same way. The redo is what makes the
// outcome one of the serial orders PostgreSQL could have produced: a second
// UPDATE of one row re-reads the row the first one wrote and replaces THAT,
// so the key stays unique; a DELETE whose row a concurrent statement already
// removed re-scans, matches nothing, and reports `DELETE 0`.
func (db *DB) retryConflicted(table string, once func() (*ExecResult, error)) (*ExecResult, error) {
	var err error
	for attempt := 0; attempt < dmlCommitAttempts; attempt++ {
		var res *ExecResult
		res, err = once()
		if err == nil || !dmlNeedsRedo(err) {
			return res, err
		}
		db.dmlRedos.Add(1)
	}
	return nil, sqlerr.Wrap("40001", fmt.Errorf(
		"table %q changed under this statement %d times; retry it: %w",
		table, dmlCommitAttempts, err))
}

// dmlNeedsRedo reports whether a commit failure means "read again and redo".
func dmlNeedsRedo(err error) bool {
	return errors.Is(err, catalog.ErrDMLTargetMoved) || errors.Is(err, catalog.ErrDMLRowSuperseded)
}

// DMLRedos counts the DML statements this DB has redone because the table
// changed under them between the manifest read and the commit — a compaction
// that rewrote the files they read, or another statement that superseded a
// row they were superseding.
//
// It is exported because a gate cannot otherwise tell "both statements
// committed on their first attempt because their rows were disjoint" from
// "the second one redid itself and got the same answer": the rows look
// identical either way, and a boundary is a claim that needs its own
// assertion (docs/design/correctness-fix-protocol.md item 11). It is a
// process-lifetime counter, never reset.
func (db *DB) DMLRedos() uint64 { return db.dmlRedos.Load() }

// executeDelete handles DELETE FROM table [WHERE condition]
func (db *DB) executeDelete(ctx context.Context, info *plansql.DeleteInfo) (*ExecResult, error) {
	return db.retryConflicted(info.Table, func() (*ExecResult, error) {
		return db.deleteOnce(ctx, info)
	})
}

func (db *DB) deleteOnce(ctx context.Context, info *plansql.DeleteInfo) (*ExecResult, error) {
	if err := CheckDMLQualifier(info.DMLTarget); err != nil {
		return nil, err
	}
	// A DML statement REFERENCES an existing table, so it takes the same READ
	// concession a SELECT does (catalog.ResolveTableName): a mixed-case name
	// created through parquet or ingest stays reachable unquoted. Rewriting
	// info.Table in place is what makes the WRITE land on the table the read
	// resolved — a door that conceded on the lookup and then keyed the
	// manifest byte-exact would write somewhere else. CreateTable does NOT
	// concede: minting a name is not referencing one.
	info.Table = db.catalog.ResolveTableName(info.Table)
	tableMeta, err := db.catalog.GetTable(ctx, info.Table)
	if err != nil {
		return nil, dmlRelationError(info.Table, err)
	}
	schema := tableMeta.Schema.Columns

	manifest, err := db.catalog.GetManifest(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("reading manifest for %q: %w", info.Table, err)
	}

	predicate, err := BuildDMLPredicate(info.DMLTarget, schema, db.dmlSubqueryEnv(ctx))
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

	// One CAS, validated against the manifest it commits into: a marker for a
	// file compaction rewrote while this statement was scanning is refused
	// rather than committed against nothing (#691).
	if len(markers) > 0 {
		if err := db.catalog.CommitDML(ctx, info.Table, nil, markers); err != nil {
			if dmlNeedsRedo(err) {
				return nil, err // the caller redoes the statement
			}
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
	return db.retryConflicted(info.Table, func() (*ExecResult, error) {
		return db.updateOnce(ctx, info)
	})
}

func (db *DB) updateOnce(ctx context.Context, info *plansql.UpdateInfo) (*ExecResult, error) {
	if err := CheckDMLQualifier(info.DMLTarget); err != nil {
		return nil, err
	}
	// A DML statement REFERENCES an existing table, so it takes the same READ
	// concession a SELECT does (catalog.ResolveTableName): a mixed-case name
	// created through parquet or ingest stays reachable unquoted. Rewriting
	// info.Table in place is what makes the WRITE land on the table the read
	// resolved — a door that conceded on the lookup and then keyed the
	// manifest byte-exact would write somewhere else. CreateTable does NOT
	// concede: minting a name is not referencing one.
	info.Table = db.catalog.ResolveTableName(info.Table)
	tableMeta, err := db.catalog.GetTable(ctx, info.Table)
	if err != nil {
		return nil, dmlRelationError(info.Table, err)
	}
	schema := tableMeta.Schema.Columns

	manifest, err := db.catalog.GetManifest(ctx, info.Table)
	if err != nil {
		return nil, fmt.Errorf("reading manifest for %q: %w", info.Table, err)
	}

	predicate, err := BuildDMLPredicate(info.DMLTarget, schema, db.dmlSubqueryEnv(ctx))
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
	// loop, and only then does a single CommitDML commit them.
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
	// The remaining duplication is closed by DeferManifestCommit: the
	// ingester holds its flushed files OUT of the manifest and they land in
	// the SAME CAS as the markers, so a refused commit has published nothing
	// and the statement is simply redone (#691). What an interrupted attempt
	// leaves behind is unreferenced objects in the store, never a row.
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
				ing.DeferManifestCommit()
			}
			if err := ing.Ingest(ctx, updatedRows); err != nil {
				return nil, fmt.Errorf("inserting updated rows: %w", err)
			}
			markers = append(markers, catalog.DeleteMarker{FilePath: file.Path, RowIndices: matchedIndices})
			totalUpdated += int64(len(matchedIndices))
		}
	}

	var pending []catalog.PendingFile
	if ing != nil {
		if err := ing.FlushAll(ctx); err != nil {
			// No markers are committed on this path: every row this statement
			// matched is still where it was.
			return nil, fmt.Errorf("flushing updated rows: %w", err)
		}
		pending = ing.PendingFiles()
	}

	// The replacement rows and the markers that supersede what they replace,
	// in ONE CAS, validated against the manifest it commits into (#691).
	if len(pending) > 0 || len(markers) > 0 {
		if err := db.catalog.CommitDML(ctx, info.Table, pending, markers); err != nil {
			if dmlNeedsRedo(err) {
				return nil, err // the caller redoes the statement
			}
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
	return db.retryConflicted(info.Target, func() (*ExecResult, error) {
		return db.mergeOnce(ctx, info)
	})
}

func (db *DB) mergeOnce(ctx context.Context, info *plansql.MergeInfo) (*ExecResult, error) {
	if err := CheckDMLQualifier(plansql.DMLTarget{
		Table:     info.Target,
		Qualifier: info.TargetQualifier,
	}); err != nil {
		return nil, err
	}
	// A DML statement REFERENCES an existing table, so it takes the same READ
	// concession a SELECT does (catalog.ResolveTableName): a mixed-case name
	// created through parquet or ingest stays reachable unquoted. Rewriting
	// info.Target in place is what makes the WRITE land on the table the read
	// resolved — a door that conceded on the lookup and then keyed the
	// manifest byte-exact would write somewhere else. CreateTable does NOT
	// concede: minting a name is not referencing one.
	info.Target = db.catalog.ResolveTableName(info.Target)
	targetMeta, err := db.catalog.GetTable(ctx, info.Target)
	if err != nil {
		return nil, dmlRelationError(info.Target, err)
	}

	// Read all source rows via a query
	sourceSQL := fmt.Sprintf("SELECT * FROM %s", info.Source)
	sourceResult, err := db.Query(ctx, sourceSQL)
	if err != nil {
		return nil, fmt.Errorf("reading source %q: %w", info.Source, err)
	}

	// The two relations must have DIFFERENT exposed names, and the check is
	// here — after both relations resolve, before anything is written —
	// because that is where PostgreSQL puts it: a missing relation is 42P01
	// first, and only then is a duplicate name 42712 (measured, both orders).
	targetAlias, sourceAlias, err := mergeExposedNames(info)
	if err != nil {
		return nil, err
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

	// The resolver for every SET / VALUES expression. It carries the target's
	// declared columns — a MERGE value is judged against the target's declared
	// (p, s) as it is resolved, before any marker is written (#647 re-review)
	// — and the merged namespace an expression is evaluated in (#678).
	sourceColNames := make([]string, 0, len(sourceResult.ColumnMetas))
	for _, cm := range sourceResult.ColumnMetas {
		sourceColNames = append(sourceColNames, cm.Name)
	}
	ev := db.buildMergeEvaluator(ctx, info, targetMeta.Schema.Columns, targetAlias, sourceAlias, sourceColNames)

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
				// The first WHEN MATCHED clause whose AND condition HOLDS —
				// not simply the first one written (#686 review F2).
				ci, cerr := firstFiringClause(info.WhenClauses, true, ev, merged)
				if cerr != nil {
					return nil, cerr
				}
				if ci >= 0 {
					// A TARGET ROW MAY BE AFFECTED ONCE.
					//
					// matchedTargetIndices was written by both arms below and
					// never read, and because it is a SET a target hit twice
					// contributed ONE delete marker while updateRows — a
					// slice — got TWO appends. So one original was marked
					// deleted and two replacements were ingested, and
					// `MERGE … USING dup ON t.id = s.id WHEN MATCHED THEN
					// UPDATE` reported `MERGE 2` over a table that now held
					// the row TWICE, with different values (#689).
					//
					// The check is here, above the switch, rather than inside
					// either arm: UPDATE-then-DELETE on one target is equally
					// a second affect, and PostgreSQL refuses that too. It is
					// also before any write, so the statement leaves the table
					// exactly as it found it. PostgreSQL: 21000,
					// cardinality_violation — the codebase's first.
					if matchedTargetIndices[tIdx] {
						return nil, sqlerr.New("21000",
							"MERGE command cannot affect row a second time (target %q); "+
								"ensure that not more than one source row matches any one target row",
							info.Target)
					}
					wc := info.WhenClauses[ci]
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
				}
			}
		}
		if !matched {
			// A NOT MATCHED condition sees the SOURCE row only — there is no
			// target row for it to reference, which is also PostgreSQL's rule.
			srcOnly := buildAliasedRow(srcRow, sourceAlias)
			ci, cerr := firstFiringClause(info.WhenClauses, false, ev, srcOnly)
			if cerr != nil {
				return nil, cerr
			}
			if ci >= 0 {
				wc := info.WhenClauses[ci]
				if strings.ToUpper(wc.Action) == "INSERT" {
					newRow, err := buildInsertRow(wc.SQL, srcRow, sourceAlias, ev)
					if err != nil {
						return nil, fmt.Errorf("building INSERT row: %w", err)
					}
					insertRows = append(insertRows, newRow)
					rowsAffected++
				}
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
	var pending []catalog.PendingFile
	if len(allInserts) > 0 {
		ing := ingest.New(db.catalog, info.Target, targetMeta.Schema, targetMeta.PartitionKeys, ingest.DefaultConfig())
		// The new rows land in the SAME CAS as the markers that remove the
		// rows they replace, or neither does (#691).
		ing.DeferManifestCommit()
		if err := ing.Ingest(ctx, allInserts); err != nil {
			return nil, fmt.Errorf("ingesting rows: %w", err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			return nil, fmt.Errorf("flushing rows: %w", err)
		}
		pending = ing.PendingFiles()
	}

	if len(pending) > 0 || len(deleteMarkers) > 0 {
		if err := db.catalog.CommitDML(ctx, info.Target, pending, deleteMarkers); err != nil {
			if dmlNeedsRedo(err) {
				return nil, err // the caller redoes the statement
			}
			return nil, fmt.Errorf("recording delete markers: %w", err)
		}
	}

	return &ExecResult{
		RowsAffected: rowsAffected,
		Command:      "MERGE",
	}, nil
}

// mergeExposedNames returns the names a MERGE's ON condition and its SET /
// VALUES expressions resolve against — the alias where one is written, the
// relation's own name otherwise — and refuses the statement when they are the
// same name.
//
// PostgreSQL: 42712, `name "t" specified more than once`, DETAIL "The name is
// used both as MERGE target table and data source", raised in
// transformMergeStmt BEFORE anything is written. Wadjet answered `MERGE 1` and
// WROTE, and the self-merge spelling `MERGE INTO t USING t ON t.id = t.id`
// EMPTIED the table where PostgreSQL refuses the statement (#837).
//
// The mechanism is buildMergedRow: it writes both relations' columns into one
// map under `exposedName + "." + column`, so when the two exposed names
// collide the source's values overwrite the target's at every qualified key.
// `ON t.id = t.id` then compares a source column with itself — a tautology
// that matches every pair of rows — instead of resolving to the ambiguity
// PostgreSQL reports. Same family as #689's `sourceNamed`.
//
// The rule is over EXPOSED names, which is not the same as "the source is not
// the target table". `MERGE INTO t AS x USING t AS y` is legal and wadjet
// already answers it correctly, and so is `MERGE INTO t AS x USING s AS t` —
// PostgreSQL accepts both, measured.
func mergeExposedNames(info *plansql.MergeInfo) (target, source string, err error) {
	target = info.TargetAlias
	if target == "" {
		target = info.Target
	}
	source = info.SourceAlias
	if source == "" {
		source = info.Source
	}
	// BYTE-EXACT, on the names the parser gives us. PostgreSQL folds UNQUOTED
	// identifiers to lower case and then compares the results exactly, so
	// quoting defeats the collision: `MERGE INTO t USING s AS "T"` is legal
	// (measured on 17.11 — MERGE 1) while `USING s AS T` is 42712. An
	// EqualFold here would refuse both, which is a refusal PostgreSQL does not
	// make.
	//
	// It does not fully close that shape, and the reason is one layer up: the
	// MERGE parser lower-cases EVERY relation name and alias it reads,
	// delimited ones included (parser.go, `strings.ToLower(aliasTok.val)`), so
	// `"T"` has already become `t` by the time it reaches here and this
	// function cannot tell it from an unquoted one. Identifier folding is Arc
	// D4's territory; the census carries the shape with PostgreSQL's answer
	// beside it, pinned, so it fails the day folding preserves quoting.
	if target == source {
		return "", "", sqlerr.New("42712",
			"name %q specified more than once (it is used both as MERGE target table and data source)",
			target)
	}
	return target, source, nil
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

// parseOnKeys extracts the equi-join keys a MERGE matches rows on from the ON
// condition's PARSE, not from its text.
//
// It used to split the raw string on the literal " AND " and then on the first
// "=", which is #336's mechanism one clause over: `ON t.id <= s.id` split at
// the "=" inside "<=" and produced a column named `t.id <`, reported as
// 42703 "column t.id < does not exist"; `ON t.id = s.id garbage` produced
// `s.id garbage`. Both messages named a column nobody wrote (#686 review F3b).
// A string literal containing " AND " or "=" would have split too.
//
// The ON condition must parse IN FULL (PostgreSQL answers a trailing token
// with 42601) and must be a conjunction of equalities between the two
// relations. PostgreSQL ACCEPTS any boolean ON — `ON t.id <= s.id` is legal
// there and fails only if it matches a target row twice — so a non-equi
// condition is 0A000 (this server has not implemented it), never a syntax
// error.
func parseOnKeys(onCond, targetAlias, sourceAlias string) ([]onKeyPair, error) {
	text := strings.TrimSpace(onCond)
	if text == "" {
		return nil, sqlerr.New("42601", "MERGE: ON requires a condition")
	}
	node, err := plansql.ParseExpressionComplete(text)
	if err != nil {
		return nil, sqlerr.Wrap("42601", fmt.Errorf("MERGE: parsing ON %q: %w", text, err))
	}

	var keys []onKeyPair
	var walk func(plansql.Node) error
	walk = func(n plansql.Node) error {
		n = unwrapDMLParens(n)
		// A conjunction is AndNode and a comparison is CmpExpr; BinaryOp is
		// arithmetic only (ast.go). Matching the wrong node type here refused
		// every MERGE, which is how this rewrite was caught.
		if and, ok := n.(*plansql.AndNode); ok {
			if err := walk(and.Left); err != nil {
				return err
			}
			return walk(and.Right)
		}
		cmp, ok := n.(*plansql.CmpExpr)
		if !ok || cmp.Op != "=" {
			return sqlerr.New("0A000",
				"MERGE ON supports only equality between the target and the source, not %q", n.String())
		}

		left, lok := unwrapDMLParens(cmp.Left).(*plansql.ColRef)
		right, rok := unwrapDMLParens(cmp.Right).(*plansql.ColRef)
		if !lok || !rok {
			return sqlerr.New("0A000",
				"MERGE ON supports only equality between two columns, not %q", n.String())
		}
		lAlias, rAlias := strings.ToLower(left.Table), strings.ToLower(right.Table)
		// A qualifier naming neither relation is 42P01 — the same code the SET
		// half raises for it, and it is decided HERE, before checkOnKeys runs
		// (#678 re-review N2).
		for _, a := range []string{lAlias, rAlias} {
			if a != "" && a != strings.ToLower(targetAlias) && a != strings.ToLower(sourceAlias) {
				return sqlerr.New("42P01", "missing FROM-clause entry for table %q", a)
			}
		}
		switch {
		case lAlias == strings.ToLower(targetAlias) && rAlias == strings.ToLower(sourceAlias):
			keys = append(keys, onKeyPair{TargetCol: left.Column, SourceCol: right.Column})
		case lAlias == strings.ToLower(sourceAlias) && rAlias == strings.ToLower(targetAlias):
			keys = append(keys, onKeyPair{TargetCol: right.Column, SourceCol: left.Column})
		default:
			return sqlerr.New("42601",
				"ON condition columns must reference target (%s) and source (%s): %s",
				targetAlias, sourceAlias, n.String())
		}
		return nil
	}
	if err := walk(node); err != nil {
		return nil, err
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
		val, err := ev.value(valExpr, merged, target, true)
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
	// sourceNamed is true when the source's COLUMN NAMES are known even
	// though its declared TYPES are not — a subquery source, whose rows the
	// statement has already read. It gates name RESOLUTION (checkOnKeys) and
	// nothing else: every use that needs a declared type stays behind
	// sourceKnown, because inferring a type from a boxed value is how a
	// DECIMAL or a DATE (both boxed as strings) gets silently mistyped.
	sourceNamed bool
	// sub is what a WHEN condition needs to ANSWER a subquery inside it
	// rather than refuse one (#688). Its outer scope is the MERGED row —
	// target and source together, which is what a WHEN condition names — so a
	// subquery correlated to either side compiles as correlated. nil keeps
	// the 0A000.
	sub *DMLSubqueryEnv
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
	targetCols []parquet.Column, targetAlias, sourceAlias string,
	sourceColNames []string) *mergeEvaluator {

	ev := &mergeEvaluator{
		target:       info.Target,
		source:       info.Source,
		targetAlias:  strings.ToLower(targetAlias),
		sourceAlias:  strings.ToLower(sourceAlias),
		colByName:    make(map[string]parquet.Column, len(targetCols)),
		srcByName:    map[string]parquet.Column{},
		mergedByName: map[string]parquet.Column{},
		sub:          db.dmlSubqueryEnv(ctx),
	}
	for _, c := range targetCols {
		ev.colByName[strings.ToLower(c.Name)] = c
	}

	var sourceCols []parquet.Column
	info.Source = db.catalog.ResolveTableName(info.Source)
	if srcMeta, err := db.catalog.GetTable(ctx, info.Source); err == nil {
		sourceCols = srcMeta.Schema.Columns
		ev.sourceKnown = true
		for _, c := range sourceCols {
			ev.srcByName[strings.ToLower(c.Name)] = c
		}
	} else if len(sourceColNames) > 0 {
		// A SUBQUERY source has no catalog entry, so the source half of
		// checkOnKeys was skipped entirely and
		// `USING (SELECT …) s ON t.id = s.nosuchcol` matched nothing and
		// reported `MERGE 0` — a wrong answer dressed as a no-op, where
		// PostgreSQL raises 42703 (#689 part 2's residual).
		//
		// The statement has ALREADY RUN the subquery by the time this is
		// built, so its output column NAMES are known even though no
		// declaration exists for their types. Only the names are recorded,
		// and only in srcByName: the merged namespace below stays built from
		// `sourceCols`, so no untyped column can reach the value resolution
		// that judges a literal against a declaration.
		ev.sourceNamed = true
		for _, n := range sourceColNames {
			ev.srcByName[strings.ToLower(n)] = parquet.Column{Name: n}
		}
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
		if ev.sourceKnown || ev.sourceNamed {
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
	return ev.resolveRefIn(ref, true)
}

// resolveRefIn resolves a reference in the scope the CLAUSE has.
//
// A WHEN NOT MATCHED clause has no target row, and PostgreSQL does not merely
// forbid naming the target there — it removes the target from SCOPE
// altogether. That is observable on the BARE names, which is the half the
// first implementation missed: it rejected `t.n` and then still resolved
// against the merged namespace, so a bare `n` that both tables spell came
// back 42702 "ambiguous" where PostgreSQL resolves it to the SOURCE and runs
// the statement (#686 R3-1).
//
//	MERGE INTO pr USING src ON pr.id = src.id
//	  WHEN NOT MATCHED AND n > 1 THEN INSERT (id, n) VALUES (src.id, src.n)
//
// is MERGE 1 in PostgreSQL 17.11. Under a MATCHED clause the same bare `n` IS
// ambiguous (42702), because there both relations are in scope — so the rule
// is per clause kind, not per statement.
func (ev *mergeEvaluator) resolveRefIn(ref *plansql.ColRef, matched bool) (parquet.Column, string, error) {
	col := strings.ToLower(ref.Column)
	if !matched {
		// Source-only scope.
		if ref.Table != "" && strings.EqualFold(ref.Table, ev.targetAlias) {
			return parquet.Column{}, "", sqlerr.New("42P01",
				"invalid reference to FROM-clause entry for table %q: a WHEN NOT MATCHED clause has no target row",
				ref.Table)
		}
		if ref.Table == "" {
			if !ev.sourceKnown {
				return parquet.Column{}, col, nil
			}
			c, ok := ev.srcByName[col]
			if !ok {
				return parquet.Column{}, "", sqlerr.New("42703", "column %q does not exist", ref.Column)
			}
			return c, col, nil
		}
		// A qualified name that is not the target falls through to the shared
		// path below, which handles the source alias and a ROW field path.
	}
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
			// A SUBQUERY source: its output column NAMES are known (the
			// statement has already run it) even though its declared TYPES
			// are not. The names are enough to REFUSE a mistyped one, and
			// refusing is what this has to do: without it `SET n = s.nosuchcol`
			// resolved to a spelling the merged row does not hold, `ev.value`
			// read nil, and the statement WROTE NULL OVER A GOOD VALUE and
			// reported MERGE 1 — on exactly the surface this arc extended,
			// while the named-table spelling of the same mistake was already
			// 42703 (review B6).
			if ev.sourceNamed {
				if _, ok := ev.srcByName[col]; !ok {
					return parquet.Column{}, "", sqlerr.New("42703",
						"column %s.%s does not exist", qual, ref.Column)
				}
			}
			// The row carries the value; the TYPE is still unknown, which is
			// what keeps ev.value's declared-schema refusal in place.
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
	return ev.checkClauseColumns(node, true)
}

// checkClauseColumns resolves an expression's columns against the namespace
// the CLAUSE actually has.
//
// A WHEN NOT MATCHED clause has no target row — that is what "not matched"
// means — so it may name the SOURCE only, and PostgreSQL raises 42P01
// ("invalid reference to FROM-clause entry for table t") for a target
// reference in its condition or in its INSERT values. Resolving both clause
// kinds against the merged namespace instead let `t.n` resolve and then
// evaluate to NULL against the source-only row, so the condition quietly came
// out false and the clause did not fire: a silent skip on a statement
// PostgreSQL refuses (#686 R2-2).
func (ev *mergeEvaluator) checkClauseColumns(node plansql.Node, matched bool) error {
	// OUTSIDE the subqueries. A subquery's own names are resolved by its own
	// planning, and a reference to the merged row from inside one is resolved
	// by the correlated evaluator against the scope ev.compile hands the
	// compiler (#688).
	refs, err := plansql.ColumnRefsOutsideSubqueries(node)
	if err != nil {
		return sqlerr.Wrap("0A000", err)
	}
	for _, ref := range refs {
		if _, _, err := ev.resolveRefIn(ref, matched); err != nil && err != errMergeRefIsFieldPath {
			return err
		}
	}
	return nil
}

// compile builds a MERGE expression, with the SUBQUERY support the statement
// has when the database could build an environment for one (#688).
//
// An expression with no subquery compiles exactly as it always did, so nothing
// about MERGE's existing behaviour depends on this. One WITH a subquery gets
// the MERGED row as its outer scope — target and source together, under both
// their names and their aliases, which is the namespace a WHEN condition
// already resolves against — so `WHEN MATCHED AND t.id IN (SELECT …)` answers
// and a subquery correlated to either side compiles as correlated.
func (ev *mergeEvaluator) compile(node plansql.Node) (expr.Expr, error) {
	if !dmlClauseHasSubquery(node) {
		return expr.Compile(node)
	}
	if ev.sub == nil || ev.sub.Runner == nil {
		return nil, sqlerr.New("0A000",
			"a subquery in a MERGE clause needs a query environment this caller did not provide")
	}
	outerTables := map[string]bool{}
	for _, n := range []string{ev.target, ev.source, ev.targetAlias, ev.sourceAlias} {
		if n != "" {
			outerTables[strings.ToLower(n)] = true
		}
	}
	outerCols := make(map[string]string, len(ev.mergedCols))
	for _, c := range ev.mergedCols {
		outerCols[strings.ToLower(c.Name)] = strings.ToLower(ev.target)
	}
	return expr.CompileWithScopeResolver(node, ev.sub.Runner, outerTables, outerCols,
		ev.sub.InnerCols, ev.sub.Opts...)
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

// dmlSourceIsFloat reports whether a SET expression's DECLARED family is a
// FLOAT, which is what decides PostgreSQL's assignment-cast rounding (#699).
//
// An explicit CAST decides it outright, before the declared-type layer is
// asked. That layer resolves `f::numeric` from the OPERAND — a float8 column —
// and answers FLOAT, so the half-to-even rule was applied to an expression
// whose PostgreSQL source type is numeric and 5 of 8 rows differed from
// PostgreSQL (review P5). A cast is the user saying which family this is.
func dmlSourceIsFloat(node plansql.Node, schema []parquet.Column) bool {
	if c, ok := unwrapDMLParens(node).(*plansql.CastNode); ok {
		switch strings.ToLower(strings.TrimSpace(c.TypeName)) {
		case "float", "float4", "float8", "real", "double precision", "double",
			"float32", "float64":
			return true
		}
		return false
	}
	decl, conf := physical.DeclaredTypeOfNode(node, schema)
	return conf == expr.Decided &&
		(decl.ID == parquet.TypeFloat32 || decl.ID == parquet.TypeFloat64)
}

// sourceIsFloat reports whether an expression's DECLARED type is a FLOAT,
// which is what decides PostgreSQL's assignment-cast rounding (#699). The
// namespace is the merged one, so `s.f` resolves to the source's declaration
// exactly as the expression evaluator resolves it.
func (ev *mergeEvaluator) sourceIsFloat(node plansql.Node) bool {
	return dmlSourceIsFloat(node, ev.mergedCols)
}

// value resolves one SET / VALUES expression against the merged row.
//
// A column REFERENCE is checked as well as converted: its box comes from the
// source table and may be a DECIMAL at another scale or past the target's
// precision, which is exactly the shape a MERGE exists to move (#647).
func (ev *mergeEvaluator) value(text string, merged map[string]any, col parquet.Column, matched bool) (any, error) {
	text = strings.TrimSpace(text)

	// COMPLETE, for the reason BuildDMLPredicate gives: `SET n = s.n garbage`
	// parsed to `s.n`, stored it and reported MERGE 1 where PostgreSQL raises
	// 42601 (#686 review F3a).
	node, err := plansql.ParseExpressionComplete(text)
	if err != nil {
		return nil, sqlerr.Wrap("42601", fmt.Errorf("parsing %q: %w", text, err))
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
		_, spelling, rerr := ev.resolveRefIn(ref, matched)
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
			cast, cerr := assignEvaluatedValue(v, col, ev.sourceIsFloat(node))
			if cerr != nil {
				return nil, cerr
			}
			return cast, checkValueForColumn(cast, col)
		}
		if rerr != errMergeRefIsFieldPath {
			return nil, rerr
		}
	}
	if err := ev.checkClauseColumns(node, matched); err != nil {
		return nil, err
	}

	if !ev.sourceKnown {
		// Evaluating needs the source's DECLARED types; inferring them from
		// the boxed values is how a DECIMAL or a DATE (both boxed as strings)
		// would be silently mistyped. Refusing is the honest answer, and it is
		// strictly better than the source text this used to store.
		return nil, sqlerr.New("0A000",
			"MERGE cannot evaluate %q: the source %q has no declared schema to resolve it against",
			text, ev.source)
	}
	compiled, err := ev.compile(node)
	if err != nil {
		return nil, fmt.Errorf("compiling %q: %w", text, err)
	}
	b := batch.FromRows(ev.mergedCols, []map[string]any{lowercaseKeys(merged)})
	v, err := assignEvaluatedValue(compiled.Eval(b, 0), col, ev.sourceIsFloat(node))
	if err != nil {
		return nil, err
	}
	if err := checkValueForColumn(v, col); err != nil {
		return nil, err
	}
	return v, nil
}

// condition answers whether a WHEN clause's `AND <cond>` holds for one row.
//
// The condition was PARSED and then never read: parseMerge stored it on the
// clause and executeMerge fired the first clause of the right kind whatever it
// said. `WHEN MATCHED AND s.n > 1000 THEN DELETE` deleted the row for a
// condition that is false, reporting MERGE 1 where PostgreSQL reports MERGE 0
// (#686 review F2) — a silent wrong answer on ordinary MERGE syntax.
//
// An empty condition is an unconditional clause and always holds. Anything
// that is not TRUE — false, and NULL, which PostgreSQL also declines to fire
// on — does not.
func (ev *mergeEvaluator) condition(text string, row map[string]any, matched bool) (bool, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return true, nil
	}
	node, err := plansql.ParseExpressionComplete(text)
	if err != nil {
		return false, sqlerr.Wrap("42601", fmt.Errorf("parsing WHEN condition %q: %w", text, err))
	}
	// A NOT MATCHED clause has no target row, so its condition may name the
	// SOURCE only. Resolving it against the merged namespace let `t.n > 1`
	// resolve and then evaluate to NULL against a source-only row, so the
	// clause silently did not fire; PostgreSQL raises 42P01 (#686 R2-2).
	if err := ev.checkClauseColumns(node, matched); err != nil {
		return false, err
	}
	// The TYPE is checked before any row is touched: a non-boolean condition
	// used to be read as FALSE, so the clause did not fire and the NEXT one
	// did — `WHEN MATCHED AND s.n THEN DELETE WHEN MATCHED THEN UPDATE ...`
	// rewrote the row where PostgreSQL raises 42804 and writes nothing
	// (#686 R2-1).
	if err := ev.checkConditionType(node, matched); err != nil {
		return false, err
	}
	// The same operand-pair refusal a DELETE's and an UPDATE's WHERE gets.
	// It reached BuildDMLPredicate only, whose callers are deleteOnce and
	// updateOnce, so MERGE — the fourth DML verb — kept the behaviour #721
	// was filed for: `WHEN MATCHED AND t.name > 5 THEN DELETE` DESTROYED a row
	// PostgreSQL refuses with 42883, and it is the very predicate the fix's
	// own commit body calls out as having emptied a table (review B4).
	if err := refuseDMLLiteralPairs(node, ev.mergedCols); err != nil {
		return false, err
	}
	// An untyped string literal is CAST to boolean rather than evaluated:
	// PostgreSQL fires on `AND 'true'` and not on `AND 'false'`, and the
	// expression engine would hand back the string itself.
	if lit, ok := unwrapDMLParens(node).(*plansql.Lit); ok && lit.Kind == plansql.LitString {
		v, _ := parseSQLBoolText(lit.Value)
		return v, nil
	}
	if !ev.sourceKnown {
		// Same rule ev.value applies: evaluating needs the source's DECLARED
		// types, and inferring them from boxed values silently mistypes a
		// DECIMAL or a DATE. Refusing beats firing a clause on a guess.
		return false, sqlerr.New("0A000",
			"MERGE cannot evaluate the WHEN condition %q: the source %q has no declared schema to resolve it against",
			text, ev.source)
	}
	compiled, err := ev.compile(node)
	if err != nil {
		return false, fmt.Errorf("compiling WHEN condition %q: %w", text, err)
	}
	b := batch.FromRows(ev.mergedCols, []map[string]any{lowercaseKeys(row)})
	raw := compiled.Eval(b, 0)
	if raw == nil {
		// NULL is not TRUE, and PostgreSQL does not fire on it.
		return false, nil
	}
	v, ok := raw.(bool)
	if !ok {
		// checkConditionType could not infer this shape statically (a
		// function call, say). MERGE stages every write until after the row
		// loop, so failing here still writes nothing.
		return false, sqlerr.New("42804",
			"argument of WHEN must be type boolean, not type %T, in %q", raw, text)
	}
	return v, nil
}

// checkConditionType refuses a WHEN condition that is not BOOLEAN, at PLAN
// time — before the row loop, so nothing is written on the way to the error.
//
// PostgreSQL's answers, read off 17.11: a non-boolean typed expression is
// 42804 ("argument of WHEN must be type boolean, not type bigint"), while an
// untyped STRING literal is cast to boolean instead, so `AND 'true'` fires,
// `AND 'false'` does not, and `AND 'x'` is 22P02.
//
// A shape whose type cannot be decided from the AST (a function call) returns
// nil here and is caught by the runtime check in condition.
func (ev *mergeEvaluator) checkConditionType(node plansql.Node, matched bool) error {
	switch n := unwrapDMLParens(node).(type) {
	case *plansql.CmpExpr, *plansql.AndNode, *plansql.OrNode, *plansql.NotNode,
		*plansql.IsExpr, *plansql.InExpr, *plansql.BetweenExpr, *plansql.LikeExpr,
		*plansql.ExistsNode, *plansql.AnyAllExpr:
		return nil
	case *plansql.Lit:
		switch n.Kind {
		case plansql.LitBool, plansql.LitNull:
			return nil
		case plansql.LitString:
			if _, ok := parseSQLBoolText(n.Value); !ok {
				return sqlerr.New("22P02", "invalid input syntax for type boolean: %q", n.Value)
			}
			return nil
		default:
			return sqlerr.New("42804", "argument of WHEN must be type boolean, not type numeric")
		}
	case *plansql.ColRef:
		col, _, err := ev.resolveRefIn(n, matched)
		if err != nil {
			// A field path or an unresolved name is not this check's business;
			// checkClauseColumns already ruled on it.
			return nil
		}
		if col.Type != parquet.TypeBool {
			return sqlerr.New("42804", "argument of WHEN must be type boolean, not type %s", col.Type)
		}
		return nil
	case *plansql.BinaryOp:
		// Arithmetic and concatenation are never boolean.
		return sqlerr.New("42804", "argument of WHEN must be type boolean, not type %s",
			binaryOpResultName(n.Op))
	case *plansql.CastNode:
		if strings.EqualFold(strings.TrimSpace(n.TypeName), "BOOL") ||
			strings.EqualFold(strings.TrimSpace(n.TypeName), "BOOLEAN") {
			return nil
		}
		return sqlerr.New("42804", "argument of WHEN must be type boolean, not type %s", n.TypeName)
	}
	return nil
}

// parseSQLBoolText reads the spellings PostgreSQL accepts when it casts an
// untyped literal to boolean.
func parseSQLBoolText(v string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "t", "true", "y", "yes", "on", "1":
		return true, true
	case "f", "false", "n", "no", "off", "0":
		return false, true
	}
	return false, false
}

// binaryOpResultName names the type an arithmetic operator produces, for the
// 42804 message only.
func binaryOpResultName(op string) string {
	if op == "||" {
		return "text"
	}
	return "numeric"
}

// firstFiringClause is PostgreSQL's clause-selection rule: the WHEN clauses of
// the right kind are tried IN ORDER and the first whose condition holds fires.
// None firing is not an error — the row is simply left alone.
//
// Returning the index rather than the clause keeps "nothing fired" distinct
// from "the zero clause fired".
func firstFiringClause(clauses []plansql.MergeWhenClause, matched bool,
	ev *mergeEvaluator, row map[string]any) (int, error) {

	for i, wc := range clauses {
		if wc.Matched != matched {
			continue
		}
		ok, err := ev.condition(wc.Condition, row, matched)
		if err != nil {
			return -1, err
		}
		if ok {
			return i, nil
		}
	}
	return -1, nil
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
func assignEvaluatedValue(v any, col parquet.Column, srcFloat bool) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch col.Type {
	case parquet.TypeDecimal:
		return assignDecimalValue(v, col)
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypePort, parquet.TypeProtocol:
		return assignIntegerValue(v, col, srcFloat)
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
// A fractional value ROUNDS the way PostgreSQL's assignment cast rounds, and
// which way that is depends on the SOURCE's declared type: a float8 rounds
// half to EVEN (C's rint) and a numeric half AWAY FROM ZERO. Only a value
// outside the column's range (NaN and the infinities included) is 22003.
//
// This engine boxes both families as float64, so the BOX cannot decide it:
// `SET n = f` over a FLOAT64 column and `SET n = 0 - 2.5` arrive here as the
// same Go type and want opposite answers — 2 and -3. One rule served both, and
// it was the numeric one, so `UPDATE fl SET n = f` over 2.5, -2.5, 0.5, 3.5,
// 1.5 stored 3, -3, 1, 4, 2 where PostgreSQL stores 2, -2, 0, 4, 2 — three of
// five rows wrong, silently (#699).
//
// srcFloat is the DECLARATION, resolved once per SET clause through
// physical.DeclaredTypeOfNode — the same declared-type layer the query path
// reads, not a private approximation of it. An expression whose type the layer
// declines to decide keeps the numeric rule, which is what it had.
//
// The range check reaches PORT (uint16) and PROTOCOL (uint8) too, because
// nothing below this line re-checks either — convertValue does, but only for
// literals — so an out-of-range computed value would truncate into a port no
// real port can be.
func assignIntegerValue(v any, col parquet.Column, srcFloat bool) (any, error) {
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
		// math.Round is half AWAY FROM ZERO (PostgreSQL numeric's rule);
		// math.RoundToEven is half TO EVEN (PostgreSQL float8's, C's rint).
		r := math.Round(t)
		if srcFloat {
			r = math.RoundToEven(t)
		}
		if math.IsNaN(r) || math.IsInf(r, 0) || r < -9.223372036854776e18 || r > 9.223372036854776e18 {
			return nil, sqlerr.New("22003", "%s out of range", col.Type)
		}
		n = int64(r)
	case float32:
		return assignIntegerValue(float64(t), col, srcFloat)
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
		return nil, sqlerr.New("22P02", "invalid input syntax for type %s: %s", col.Type, sqlerr.Quote(s))
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
		// A LITERAL, so the numeric rule: PostgreSQL reads an unadorned
		// `2.5` as numeric and rounds it half away from zero (#699).
		return assignEvaluatedValue(text, col, false)
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
		val, err := ev.value(strings.TrimSpace(values[i]), merged, target, false)
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
// ReadDataFile reads one of a table's data files as a columnar batch, through
// the exact path the DML executors read it.
//
// Exported for the gates that assert what a statement left in a specific FILE
// rather than what a query returns: delete markers are metadata, so a file's
// surviving rows can only be seen by reading the file and applying them (#815
// folded the HTTP door's own copy of this reader into this one).
func (db *DB) ReadDataFile(ctx context.Context, filePath string, schema []parquet.Column) (*batch.RecordBatch, error) {
	return db.readParquetFile(ctx, filePath, schema)
}

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
	// OUTSIDE the subqueries. A subquery's own names are resolved by its own
	// planning, and a correlated reference to the TARGET inside one is
	// resolved by the correlated evaluator against the outer scope
	// BuildDMLPredicate hands the compiler (#688). Refusing the whole tree
	// because one operand is a subquery is what made every DML subquery an
	// 0A000.
	refs, err := plansql.ColumnRefsOutsideSubqueries(node)
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

// refuseDMLLiteralPairs raises, BEFORE any row is read, for a comparison whose
// operand pair PostgreSQL's overload resolution refuses.
//
// A QUALIFYING PREDICATE IS NOT A PROJECTION. ADR-0012 item 12 records a
// deliberate divergence: PostgreSQL refuses `text = numeric` outright (42883),
// and wadjet — having one generic comparison operator and no overload set to
// fail resolution against — gives the pair the column's own rule, comparing
// the STRING column's bytes against the literal's source text. Every answer
// that produces is PostgreSQL's answer to the QUOTED spelling of the same
// predicate, which is a defensible concession when the consequence is a
// COUNT. It is not one when the consequence is a WRITE:
//
//	DELETE FROM pr WHERE name > 5     PG: 42883.  wadjet: DELETE 3, table EMPTIED
//
// `"a" > "5"` is true for every row because 0x61 > 0x35, so wadjet answered
// PostgreSQL's answer to a DIFFERENT predicate and destroyed a three-row
// table (#721). Nobody wrote that consequence down because no fixture
// attempted it: the ADR entry was reasoned entirely about the read path.
//
// So the divergence stays where its reasoning holds — a SELECT still gets the
// byte rule — and a DML statement's qualifying predicate refuses the pair.
// The asymmetry is recorded in ADR-0012 item 12 rather than left implicit.
//
// Three pairs, and the bound is deliberate:
//
//   - a STRING or BYTES column against an unquoted NUMBER literal (42883).
//     This is the shape above, and the one that loses rows.
//   - any non-BOOL column against a BOOLEAN literal (42883). `id = true` is
//     PostgreSQL's `bigint = boolean`; wadjet answered `DELETE 0` on the DML
//     door and 22P02 on the SELECT door — two doors disagreeing about one
//     predicate.
//   - a numeric column against a QUOTED literal naming no value of it. The
//     runtime already refuses this (22P02, #536/#646), but the refusal needs
//     a ROW to reach it, so `DELETE FROM empty WHERE id = 'abc'` answered
//     `DELETE 0` where PostgreSQL raises. The test is
//     expr.RefuseNumericLiteral — the SAME predicate the runtime uses, so the
//     two cannot disagree about which strings name a value.
//
// Temporal and network columns against a number are deliberately NOT refused
// here: those pairs have their own accept-sets (parquet.ParseTimestampMillis
// and friends), wadjet's network literal parsers are STRICTER than
// PostgreSQL's input grammar, and refusing on them would reject input
// PostgreSQL accepts — the one thing ADR-0012 item 1 forbids. The boundary
// carries fixtures either way.
func refuseDMLLiteralPairs(node plansql.Node, schema []parquet.Column) error {
	byName := make(map[string]parquet.Column, len(schema))
	for _, c := range schema {
		byName[strings.ToLower(c.Name)] = c
	}
	var walk func(plansql.Node) error
	check := func(a, b plansql.Node, op string) error {
		col, lit, ok := dmlColumnLiteralPair(a, b, byName)
		if !ok {
			return nil
		}
		return refuseDMLPair(col, lit, op)
	}
	walk = func(n plansql.Node) error {
		switch e := n.(type) {
		case *plansql.CmpExpr:
			if err := check(e.Left, e.Right, e.Op); err != nil {
				return err
			}
			return errors.Join(walk(e.Left), walk(e.Right))
		case *plansql.InExpr:
			for _, v := range e.Values {
				if err := check(e.Left, v, "="); err != nil {
					return err
				}
			}
			return walk(e.Left)
		case *plansql.BetweenExpr:
			if err := check(e.Left, e.Low, "<="); err != nil {
				return err
			}
			if err := check(e.Left, e.High, "<="); err != nil {
				return err
			}
			return walk(e.Left)
		case *plansql.AndNode:
			return errors.Join(walk(e.Left), walk(e.Right))
		case *plansql.OrNode:
			return errors.Join(walk(e.Left), walk(e.Right))
		case *plansql.NotNode:
			return walk(e.Inner)
		case *plansql.ParenNode:
			return walk(e.Inner)
		}
		return nil
	}
	return walk(node)
}

// dmlColumnLiteralPair reports the (column, literal) pair of a comparison,
// in either operand order.
func dmlColumnLiteralPair(a, b plansql.Node, byName map[string]parquet.Column) (parquet.Column, *plansql.Lit, bool) {
	colOf := func(n plansql.Node) (parquet.Column, bool) {
		ref, ok := unwrapDMLParens(n).(*plansql.ColRef)
		if !ok {
			return parquet.Column{}, false
		}
		c, ok := byName[strings.ToLower(ref.Column)]
		return c, ok
	}
	litOf := func(n plansql.Node) (*plansql.Lit, bool) {
		l, ok := unwrapDMLParens(n).(*plansql.Lit)
		return l, ok
	}
	if c, ok := colOf(a); ok {
		if l, ok := litOf(b); ok {
			return c, l, true
		}
	}
	if c, ok := colOf(b); ok {
		if l, ok := litOf(a); ok {
			return c, l, true
		}
	}
	return parquet.Column{}, nil, false
}

func refuseDMLPair(col parquet.Column, lit *plansql.Lit, op string) error {
	switch lit.Kind {
	case plansql.LitNumber:
		if refusesNumericLiteral(col.Type) {
			return sqlerr.New("42883",
				"operator does not exist: %s %s numeric",
				pgOperandTypeName(col.Type), op)
		}
	case plansql.LitBool:
		if col.Type != parquet.TypeBool {
			return sqlerr.New("42883",
				"operator does not exist: %s %s boolean",
				pgOperandTypeName(col.Type), op)
		}
	case plansql.LitString:
		// The runtime's own predicate, run early. A type with no rule
		// returns nil, so this is silent for everything but the numeric
		// family.
		return expr.RefuseNumericLiteral(col.Type, lit.Value)
	}
	return nil
}

// refusesNumericLiteral reports whether PostgreSQL has no operator between a
// column of this type and an unquoted NUMBER.
//
// The first pass listed only String and Bytes, and justified the omission with
// "those parsers are stricter than PostgreSQL's input grammar, so refusing
// would reject input PostgreSQL accepts". That argument is about a QUOTED
// literal reaching a type's input function; it cannot apply to an unquoted
// number, which is never input to a timestamp or inet parser at all —
// PostgreSQL refuses the OPERATOR categorically. The measurement refuted the
// exclusion outright: `DELETE FROM t WHERE ts > 5` EMPTIED a TIMESTAMP table,
// and so did the BOOL and IPv4 spellings, where PostgreSQL raises 42883
// (review B5).
//
// The wadjet-native numeric-ish types are the deliberate exception: PORT,
// PROTOCOL and DURATION are stored as integers, PostgreSQL has no such type
// and therefore no opinion, and ADR-0012's superset rule keeps them answering.
// The containers are refused because a number cannot be compared to one under
// any reading.
func refusesNumericLiteral(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64,
		parquet.TypeFloat32, parquet.TypeFloat64, parquet.TypeDecimal,
		parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration:
		return false
	}
	return true
}

// pgOperandTypeName renders a column type the way PostgreSQL names it in an
// "operator does not exist" message, so a client reading the text sees the
// type it declared rather than an internal name.
func pgOperandTypeName(t parquet.TypeID) string {
	switch t {
	case parquet.TypeString:
		return "text"
	case parquet.TypeBytes:
		return "bytea"
	case parquet.TypeInt32:
		return "integer"
	case parquet.TypeInt64:
		return "bigint"
	case parquet.TypeFloat32:
		return "real"
	case parquet.TypeFloat64:
		return "double precision"
	case parquet.TypeDecimal:
		return "numeric"
	case parquet.TypeBool:
		return "boolean"
	case parquet.TypeTimestamp:
		return "timestamp without time zone"
	case parquet.TypeDate:
		return "date"
	case parquet.TypeIPv4, parquet.TypeIPv6:
		return "inet"
	case parquet.TypeCIDR:
		return "cidr"
	case parquet.TypeMAC:
		return "macaddr"
	case parquet.TypeUUID:
		return "uuid"
	default:
		return t.String()
	}
}

// DMLSubqueryEnv is what a DML predicate needs in order to ANSWER a subquery
// inside it rather than refuse one: the runner that executes the subquery as
// an ordinary SELECT, the resolver for the subquery's own FROM columns, and
// the compile options that carry a scalar subquery's declared output type.
//
// physical.(*Planner).SubqueryEnv builds all three from one planner, so the
// DML door and the query path answer "what does this subquery mean" the same
// way rather than twice. A nil *DMLSubqueryEnv keeps the old behaviour — a
// subquery in the clause is refused — which is what a caller that has no
// catalog to plan against must get.
type DMLSubqueryEnv struct {
	Runner    expr.SubqueryRunner
	InnerCols plansql.TableColumns
	Opts      []expr.CompileOption
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
// It is exported because MatchDMLRows is the other half of the contract and
// is the one that must be used to RUN a predicate. (The HTTP door is no longer
// a second caller: since #815 it reaches the executors through
// DB.ExecuteParsed like everything else.)
//
// A SUBQUERY IN A DML PREDICATE (#688).
//
// `DELETE … WHERE id IN (SELECT …)`, `NOT IN (SELECT …)`, a scalar subquery
// and a correlated `EXISTS` were all 0A000 here. The reason was structural:
// this function is not a planner. It parsed, resolved the column names against
// the target's schema, and called `expr.Compile` with a NIL runner and no
// outer scope, so every planner-resident guarantee was absent on this door.
//
// It is answered now, and the shape of the answer is what makes it not the
// bounded repair ADR-0031 forbade. That one was `expr.CompileWithRunner` — a
// runner and nothing else — which closes `IN`, `NOT IN` and the scalar
// subquery and leaves CORRELATED `EXISTS` refused, because a compile site with
// no outer scope cannot classify a subquery as correlated in the first place.
// The scope is the missing half, and a DML statement has the simplest one
// there is: exactly ONE relation, the target, under its alias when it has one
// and its own name when it does not, with the columns of the schema this
// function was already handed. Given that scope,
// `expr.CompileWithScopeResolver` builds the same correlated evaluators the
// query path builds, and `EXISTS (SELECT 1 FROM s WHERE s.id = t.id)` — the
// shape #688's own body names first — answers.
//
// THE PREDICATE IS STILL COMPILED AND NOT PLANNED, which is ADR-0031's
// position and is unchanged: the door still walks its files and evaluates the
// clause per row, and the structural DELETE-as-a-planned-SELECT design that
// record blocks on a projectable row identity is still blocked and still
// unnecessary here. What is planned is the SUBQUERY, through the ordinary
// SELECT path.
//
// TWO CONSEQUENCES ARE THE QUERY PATH'S, INHERITED RATHER THAN INVENTED.
// An uncorrelated subquery is executed ONCE and memoized; a correlated one is
// re-run per outer row with the outer values substituted as typed literals
// (ADR-0021 §1e), so an outer value with no literal spelling is 0A000 there as
// it is in a SELECT. And a subquery that cannot be RUN fails the statement
// rather than deciding it (§1c) — which on a WRITE door is the difference
// between refusing and deleting the wrong rows.
//
// THE SNAPSHOT. The subquery runs against the manifest the catalog holds while
// the statement is scanning, and a DML statement commits its markers at the
// end (ADR-0030), so a subquery over the TARGET TABLE reads the pre-statement
// state — which is what PostgreSQL does. `DELETE FROM t WHERE id IN (SELECT id
// FROM t WHERE …)` is in the census with PostgreSQL's answer beside it.
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
func BuildDMLPredicate(target plansql.DMLTarget, schema []parquet.Column, sub *DMLSubqueryEnv) (DMLPredicate, error) {
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
	if err := refuseDMLLiteralPairs(node, schema); err != nil {
		return nil, err
	}

	compiled, err := compileDMLPredicate(node, target, schema, sub)
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

// compileDMLPredicate compiles a resolved DML WHERE clause, with the SUBQUERY
// support the door has when its caller could build one.
//
// A clause with no subquery in it compiles exactly as it always did — one
// call, no scope, no runner — so the overwhelmingly common statement pays
// nothing for this and cannot behave differently because of it.
//
// A clause WITH one compiles against the outer scope a DML statement has: one
// relation, the target, under its alias when it has one and its own name when
// it does not, carrying the columns of the schema this door was handed. That
// scope is what separates this from the bounded repair ADR-0031 forbade — a
// runner alone leaves a correlated EXISTS unrecognised as correlated, and a
// correlated EXISTS is the shape #688 was filed for.
//
// A subquery with NO environment to run it stays 0A000, which is the answer
// for a caller with no catalog to plan against.
func compileDMLPredicate(node plansql.Node, target plansql.DMLTarget,
	schema []parquet.Column, sub *DMLSubqueryEnv) (expr.Expr, error) {
	if !dmlClauseHasSubquery(node) {
		return expr.Compile(node)
	}
	if sub == nil || sub.Runner == nil {
		return nil, sqlerr.New("0A000",
			"a subquery in a DML predicate needs a query environment this caller did not provide")
	}
	relation := target.Table
	if target.Alias != "" {
		relation = target.Alias
	}
	outerTables := map[string]bool{strings.ToLower(target.Table): true}
	if target.Alias != "" {
		outerTables[strings.ToLower(target.Alias)] = true
	}
	outerCols := make(map[string]string, len(schema))
	for _, c := range schema {
		outerCols[strings.ToLower(c.Name)] = relation
	}
	return expr.CompileWithScopeResolver(node, sub.Runner, outerTables, outerCols,
		sub.InnerCols, sub.Opts...)
}

// dmlClauseHasSubquery reports whether a resolved WHERE clause holds a
// subquery of any kind. It is the switch between the compile this door has
// always made and the scoped one: a clause without one must not change
// behaviour because subqueries became possible.
//
// It asks plansql, not the tree: ColumnRefs REFUSES the three raw-SQL nodes
// and ColumnRefsOutsideSubqueries walks past them, so the difference between
// the two answers IS the question.
func dmlClauseHasSubquery(node plansql.Node) bool {
	if _, err := plansql.ColumnRefs(node); err != nil {
		if _, err2 := plansql.ColumnRefsOutsideSubqueries(node); err2 == nil {
			return true
		}
	}
	return false
}

// DMLAssignment is one resolved `SET column = ...`: the target column's full
// declaration, plus EITHER a constant or a compiled expression.
type DMLAssignment struct {
	Column   string
	col      parquet.Column
	constant any       // used when expr is nil
	expr     expr.Expr // per-row evaluation
	// srcFloat is the source expression's DECLARED family: true for float4 /
	// float8, which round half to EVEN on assignment to an integer column,
	// false for everything else, which rounds half away from zero. The
	// compiled expr cannot carry it — expr.Expr is one method, Eval — and the
	// BOX cannot decide it, because this engine boxes both families as
	// float64 (#699).
	srcFloat bool
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
		// A SUBQUERY in the SET list is refused HERE and explicitly. It used
		// to be refused incidentally, by checkDMLColumns declining to walk a
		// subquery at all; that check now walks PAST one, because a DML
		// PREDICATE answers subqueries (#688), and this site does not. An
		// incidental refusal that stops refusing is a silent no-op — the
		// UPDATE reported success and wrote nothing — so the refusal is
		// stated rather than inherited.
		if dmlClauseHasSubquery(node) {
			return nil, sqlerr.New("0A000",
				"SET %s: a subquery in an UPDATE's SET list is not supported", name)
		}
		compiled, err := expr.Compile(node)
		if err != nil {
			return nil, fmt.Errorf("SET %s: compiling %q: %w", name, sc.Value, err)
		}
		// The AST is still in hand here, and it is the only place the source's
		// DECLARED family can be read — one line above where it used to be
		// thrown away at expr.Compile (#699).
		out = append(out, DMLAssignment{Column: name, col: col, expr: compiled,
			srcFloat: dmlSourceIsFloat(node, schema)})
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
		if e.Kind == plansql.LitString {
			// RE-QUOTED, not bare. This returned e.Value for every kind,
			// which collapsed LitNull and LitString{"NULL"} into the
			// byte-identical text "NULL" — so `SET name = 'NULL'` stored a
			// SQL NULL and `SET name = '''a'''` stored `a`, both reported as
			// success (#690). Lit.String is the faithful re-quoting renderer
			// that was already in the AST and unused here, and convertValue
			// reverses it exactly: it tests the NULL keyword before stripping
			// the quotes, and un-doubles what the re-quote doubled.
			return e.String(), true
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
			v, err := assignEvaluatedValue(a.expr.Eval(b, int(idx)), a.col, a.srcFloat)
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
	return decimalChecked(convertValue(s, col.Type))(col)
}

// ConvertTextForColumn converts RAW TEXT — not a SQL literal — to the box a
// column stores.
//
// The difference from ConvertValueForColumn is the two rules that belong to
// LITERAL text and to nothing else: the word `null` is the SQL keyword, and a
// leading and trailing apostrophe are quoting. A COPY field is neither. It
// used to go through the literal converter, so a COPY field spelled `NULL`
// became a SQL NULL — even though COPY's own NULL marker is `\N` and is
// handled before this — and a field whose text happened to begin and end with
// an apostrophe silently lost both (#690's third site).
func ConvertTextForColumn(s string, col parquet.Column) (any, error) {
	// No TrimSpace here: convertUnquoted trims per TYPE, so a COPY field into a
	// TEXT column keeps its padding — PostgreSQL's `  spaced  ` stays
	// `  spaced  ` — while a field into a numeric column still parses. Trimming
	// here was the third literal rule the commit meant to remove and did not
	// (review P7).
	return decimalChecked(convertUnquoted(s, col.Type))(col)
}

// decimalChecked is the shared tail of the two converters: a DECIMAL value is
// judged against the column's declared (p, s) HERE, before the caller commits
// anything destructive.
func decimalChecked(v any, err error) func(parquet.Column) (any, error) {
	return func(col parquet.Column) (any, error) {
		if err != nil || v == nil || col.Type != parquet.TypeDecimal {
			return v, err
		}
		if _, derr := parquet.DecimalValueFromBox(v, col.Precision, col.Scale); derr != nil {
			return nil, derr
		}
		return v, nil
	}
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

	// The NULL KEYWORD — and it is tested BEFORE the quotes come off, which
	// is the whole of one half of #690: `'NULL'` is a three-letter STRING and
	// must survive as one. It only survives if the text that reaches here is
	// still quoted, which is why dmlLiteralText and insertValueText re-quote
	// a string literal instead of handing over its bare value.
	if strings.EqualFold(s, "null") {
		return nil, nil
	}

	// Strip the quoting, and un-double the apostrophes the quoting doubled.
	// The two are one transformation and neither is correct alone: stripping
	// by itself is lossless only for values with no apostrophe in them, so
	// `'''a'''` — the SQL spelling of the value `'a'` — lost one layer and
	// stored `''a''` (#690).
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		s = strings.ReplaceAll(s[1:len(s)-1], "''", "'")
	}

	return convertUnquoted(s, typ)
}

// convertUnquoted is convertValue's body: the text is already a VALUE, with
// no SQL keyword and no quoting left in it.
//
// WHITESPACE IS DATA FOR TEXT AND IGNORABLE FOR EVERYTHING ELSE, which is
// PostgreSQL's rule and, once the quotes come off, the only place it can be
// applied. `TrimSpace` used to run on the OUTSIDE of the quotes, so it was a
// no-op for `'  7  '` and the padding reached strconv: an INSERT or UPDATE of
// a padded numeric literal was REFUSED where PostgreSQL — and this engine
// before #690 — stores 7 (`'  7  '::bigint` is 7, and `'  spaced  '::text`
// keeps both runs). Trimming per TYPE rather than per literal is what makes
// both true at once (review B1, P7).
func convertUnquoted(s string, typ parquet.TypeID) (any, error) {
	switch typ {
	case parquet.TypeString, parquet.TypeBytes:
		// The value is the bytes. Nothing is trimmed.
	default:
		s = strings.TrimSpace(s)
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

// dmlSubqueryEnv builds the environment a DML predicate needs to ANSWER a
// subquery inside it rather than refuse one (#688).
//
// It is a fresh planner per statement, for the same reason db.newPlanner is:
// a Planner carries per-query mutable state. The predicate this environment is
// compiled into is evaluated on every row of every file the statement scans,
// so an UNCORRELATED subquery runs once and memoizes, while a CORRELATED one
// re-runs per outer row with the outer values substituted as typed literals —
// the query path's own cost model, not a new one.
//
// The subquery reads the catalog's current manifest, and a DML statement
// commits its markers at the end (ADR-0030), so a subquery over the TARGET
// TABLE sees the pre-statement state — which is PostgreSQL's rule.
func (db *DB) dmlSubqueryEnv(ctx context.Context) *DMLSubqueryEnv {
	_, innerCols, opts := db.newPlanner(ctx).SubqueryEnv(ctx)
	return &DMLSubqueryEnv{Runner: db.dmlSubqueryRunner(ctx), InnerCols: innerCols, Opts: opts}
}

// dmlSubqueryRunner executes a DML predicate's subquery through DB.Query — the
// SAME door a client's SELECT goes through — rather than through the planner's
// internal executeSubquery.
//
// The two do not refuse the same things, and the difference is a WRITE door's
// to care about: `executeSubquery` builds a pipeline for a scan of a table the
// catalog has never heard of, which yields ZERO BATCHES with no error (the
// #571 shape), so `DELETE FROM t WHERE id IN (SELECT id FROM nosuchtable)`
// would answer `DELETE 0` where PostgreSQL raises 42P01 — and `NOT IN` would
// have deleted every row. DB.Query validates the relation and raises. One
// door, one answer to "what does this subquery mean".
func (db *DB) dmlSubqueryRunner(ctx context.Context) expr.SubqueryRunner {
	return func(sql string) ([]map[string]any, error) {
		res, err := db.Query(ctx, sql)
		if err != nil {
			return nil, err
		}
		return res.Rows, nil
	}
}
