// SPDX-License-Identifier: MIT

package wadjet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
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
	if cmd == CommandCreateTableAs {
		// PostgreSQL's DDL tags carry no row count, and this is the one that
		// reaches an ExecResult: a `CREATE TABLE … AS SELECT` that did not run
		// its query (`WITH NO DATA`, or an `IF NOT EXISTS` skip) is the bare
		// `CREATE TABLE AS`. The same statement WITH data is `SELECT <n>` and
		// takes the line above. Measured on 17.11, both shapes.
		return cmd
	}
	return fmt.Sprintf("%s %d", cmd, rows)
}

// Execute runs a DML statement (INSERT/UPDATE/DELETE/MERGE) and returns the
// result.
func (db *DB) Execute(ctx context.Context, sql string) (*ExecResult, error) {
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return nil, stageError("parsing SQL", err)
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
		err = sqlerr.Sentence(err)
	}()

	// ABAC, before any row is read or written. This is the one DML entry
	// point, so every door carries it: a write needs an ActionWrite grant
	// (42501 without one), a denied column does not exist inside the
	// statement (42703), and a masked column reads as its mask — a DML
	// predicate is compiled rather than planned (ADR-0031), so the security
	// projection is applied to the statement's own expressions (#859).
	if err := auth.EnforceDMLPolicies(ctx, db.authProvider, db.catalog, parsed, "embedded"); err != nil {
		return nil, err
	}

	switch parsed.Type {
	case plansql.QueryCreateTable:
		// A CTAS reaches the DML entry point and a declared CREATE TABLE does
		// not, because a CTAS is a WRITE: it runs a query, drives rows into
		// the ingest path and commits against a manifest. Routing it here is
		// what makes every door — embedded, pgwire, HTTP — carry one
		// implementation of it, the rule #815 settled for the four verbs.
		if parsed.CreateTable == nil || parsed.CreateTable.AsSelect == nil {
			return nil, fmt.Errorf("Execute only supports CREATE TABLE ... AS SELECT, not a declared CREATE TABLE")
		}
		return db.executeCreateTableAs(ctx, parsed.CreateTable)
	case plansql.QueryInsert:
		if parsed.Insert != nil && parsed.Insert.Select != nil {
			return db.executeInsertSelect(ctx, parsed.Insert)
		}
		return db.executeInsert(ctx, parsed.Insert)
	case plansql.QueryDelete:
		return db.executeDelete(ctx, parsed.Delete)
	case plansql.QueryUpdate:
		return db.executeUpdate(ctx, parsed.Update)
	case plansql.QueryMerge:
		return db.executeMerge(ctx, parsed.Merge)
	default:
		return nil, fmt.Errorf("Execute only supports INSERT/UPDATE/DELETE/MERGE and CREATE TABLE ... AS SELECT, got %v", parsed.Type)
	}
}

// resolveInsertColumns returns each INSERT position's STORED column name
// and full parquet.Column, so values are checked against declared (p,s) (#647).
// Resolve through batch.ResolveSchemaIndex, never a fold-keyed map:
// unquoted names arrive folded; delimited names retain their bytes.
// Missing columns raise 42703 with the relation and reference named.
// No list means every column in schema order; duplicate resolved columns
// raise 42701 rather than overwrite one row-map key.
// See docs/internals/insert-column-resolved-spelling.md for the design.
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
	names := make([]string, len(named))
	cols := make([]parquet.Column, len(named))
	seen := make(map[string]bool, len(named))
	for i, raw := range named {
		name := strings.TrimSpace(raw)
		idx := batch.ResolveSchemaIndex(schema, name)
		if idx < 0 {
			return nil, nil, sqlerr.New("42703", "column %q of relation %q does not exist", name, table)
		}
		col := schema[idx]
		// PostgreSQL: 42701, `column "x" specified more than once`. Without
		// this the second value silently overwrote the first in the row map
		// and the statement reported success. Keyed on the RESOLVED column,
		// not on the reference: two spellings of one column are one column,
		// and `("WatchID", watchid)` has to be caught the same way `(a, a)`
		// is.
		if seen[col.Name] {
			return nil, nil, sqlerr.New("42701", "column %q specified more than once", name)
		}
		seen[col.Name] = true
		names[i], cols[i] = col.Name, col
	}
	return names, cols, nil
}

// dmlRelationError gives DML the SELECT door's lookup disposition (#719):
// a missing table is 42P01, "relation ... does not exist".
// A transport error must retain its cause, never claim nonexistence.
// If no byte-exact match exists and multiple case-insensitive names match,
// use catalog.AmbiguousTableError, shared with readers (#858).
// See docs/internals/dml-relation-lookup-errors.md for the design.
func (db *DB) dmlRelationError(name string, err error) error {
	if errors.Is(err, catalog.ErrTableNotFound) {
		if cands := db.catalog.AmbiguousTableNames(name); len(cands) > 1 {
			return catalog.AmbiguousTableError(name, cands)
		}
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
		return nil, db.dmlRelationError(info.Table, err)
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
			// assignInsertValue, not ConvertValueForColumn: the ASSIGNMENT
			// CAST is part of what a literal means, and INSERT was the one
			// verb that did not get it. `INSERT INTO t (n) VALUES (2.5)` into
			// an INT64 column failed with `strconv.ParseInt: parsing "2.5"`
			// and NO SQLSTATE, while `UPDATE t SET n = 2.5` stored 3 — an
			// INSERT-vs-UPDATE split inside one engine, and a divergence from
			// PostgreSQL, which stores 3 (review P6). It also carries the
			// classes the cast raises, so an out-of-range INSERT is 22003 and
			// unreadable text is 22P02 rather than the blanket 42000 (P18).
			v, err := assignInsertValue(vals[i], cols[i])
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
		return nil, db.dmlRelationError(info.Table, err)
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
				return nil, dmlScanError(file.Path, err)
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
		return nil, db.dmlRelationError(info.Table, err)
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

	// Stream files and box only matched rows into the ingester.
	// Every replacement row must be durable before any marker is committed:
	// accumulate markers, FlushAll once after the loop, then one CommitDML (#647).
	// DeferManifestCommit keeps flushed files out of the manifest until the SAME
	// CAS publishes replacement files and markers (#691).
	// A refused commit publishes nothing and can be redone; interruption may
	// leave unreferenced objects, never published partial rows.
	// See docs/internals/update-replacement-and-marker-commit.md for the design.
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
				return nil, dmlScanError(file.Path, err)
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
		return nil, db.dmlRelationError(info.Target, err)
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

// mergeExposedNames uses aliases when present, relation names otherwise.
// Target and source exposed names must differ; collide before writing with
// 42712, naming their use as both MERGE target and source (#837).
// Otherwise buildMergedRow would overwrite qualified keys (#689).
// The rule concerns exposed names, not table identity: self-MERGE with
// distinct aliases and an alias equal to the other relation's hidden name
// are valid.
// See docs/internals/merge-exposed-name-collision.md for the design.
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
		if !mergeKeyEqual(srcRow[k.SourceCol], tgtRow[k.TargetCol]) {
			return false
		}
	}
	return true
}

// mergeKeyEqual is MERGE's ON-key equality, and it compares NUMBERS rather
// than Go BOXES.
//
// `srcRow[k] != tgtRow[k]` over two `any` values is an INTERFACE comparison:
// it is false the moment the two sides carry different dynamic types, so an
// int4 source key matched against an int8 target column matched NOTHING and
// the statement reported success having changed no row — a wrong answer
// dressed as a no-op, and a silent one.
//
// Nothing produced that pair until a numeric LITERAL began declaring integer
// rather than bigint (#1070), so `MERGE … USING (SELECT 2 AS sid) s ON
// t.id = s.sid` over a bigint target went from matching to not. The box
// agreeing was the accident; this is ADR-0023's invariant one door over —
// "compares equal" and "keys alike" have to name one relation.
//
// Deliberately narrow: the INTEGER family is compared as int64 and the FLOAT
// family as float64, and every other pair keeps the interface comparison it
// had. A mixed integer/float key is untouched — PostgreSQL resolves that pair
// to float8 and this door never has — and widening it here would change which
// rows a MERGE matches for a reason no issue has measured.
func mergeKeyEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if ai, ok := mergeKeyInt(a); ok {
		if bi, ok := mergeKeyInt(b); ok {
			return ai == bi
		}
	}
	if af, ok := mergeKeyFloat(a); ok {
		if bf, ok := mergeKeyFloat(b); ok {
			return af == bf
		}
	}
	return a == b
}

// mergeKeyInt and mergeKeyFloat name the two families mergeKeyEqual compares
// as one number. They are boxes this engine actually produces for an integer
// or a float column; a DECIMAL boxes as its rendered TEXT and is compared as
// that text, which is exact for one declared scale and is what the door did
// before.
func mergeKeyInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	}
	return 0, false
}

func mergeKeyFloat(v any) (float64, bool) {
	switch f := v.(type) {
	case float64:
		return f, true
	case float32:
		return float64(f), true
	}
	return 0, false
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
		// And then do what the LEXER would have done to it, because this
		// clause never went past the lexer: `scanMergeClauseUntil` hands back
		// `l.input[start:l.pos]`, the source bytes, so a delimited target
		// still carries its QUOTES and an unquoted one is still UNFOLDED.
		col = dmlIdent(col)
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
		// Write under the resolved SCHEMA spelling in target.Name, as UPDATE does.
		// row is keyed by catalog names, while SET references arrive folded (#731).
		// Writing the reference spelling can add an unread key and report an
		// assignment while the writer re-emits the unchanged stored column.
		// See docs/internals/merge-assignment-stored-column-key.md for the design.
		row[target.Name] = val
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

// dmlIdent applies lexer identifier rules to raw MERGE SET/INSERT names.
// Trim outer whitespace; unquoted names fold, delimited names retain bytes
// with outer quotes removed and doubled quotes decoded.
// Do this BEFORE batch.ResolveSchemaIndex: the resolver relies on the
// reference already carrying the lexer's folded-versus-delimited reading.
// Do not replace schema resolution with this normalization.
// See docs/internals/raw-dml-identifier-folding.md for the design.
func dmlIdent(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		// Delimited: the bytes are the name. `""` inside is one quote.
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	}
	return batch.FoldIdent(s)
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
	target      string
	source      string
	targetAlias string
	sourceAlias string
	colByName   map[string]parquet.Column // the TARGET's columns, by lowercase name
	srcByName   map[string]parquet.Column // the SOURCE's columns, by lowercase name
	// targetCols and srcCols are those two schemas UNFOLDED — the spellings
	// the catalog stores, in order. A map keyed by the fold cannot tell a
	// folded reference from a delimited one, because building it threw the
	// distinction away; batch.ResolveSchemaIndex decides that from the
	// reference and the schema, so it needs the schema.
	targetCols   []parquet.Column
	srcCols      []parquet.Column
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
	ev.targetCols = targetCols
	for _, c := range targetCols {
		ev.colByName[strings.ToLower(c.Name)] = c
	}

	var sourceCols []parquet.Column
	info.Source = db.catalog.ResolveTableName(info.Source)
	if srcMeta, err := db.catalog.GetTable(ctx, info.Source); err == nil {
		sourceCols = srcMeta.Schema.Columns
		ev.sourceKnown = true
		ev.srcCols = sourceCols
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
			ev.srcCols = append(ev.srcCols, parquet.Column{Name: n})
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

// checkOnKeys validates MERGE ON key columns and rewrites them to STORED
// schema spellings, so matchByKeys can read catalog-keyed rows (#678).
// parseOnKeys has already checked qualifiers. Missing columns raise 42703.
// Use batch.ResolveSchemaIndex, never a fold-keyed map: unquoted references
// arrive folded (#731), delimited references retain their bytes.
// Resolve target keys always; resolve source keys when sourceKnown or
// sourceNamed, preserving a nonempty published source spelling.
// See batch/schema.go items 1–4 for the name-resolution rule.
// See docs/internals/merge-on-key-schema-resolution.md for the design.
func (ev *mergeEvaluator) checkOnKeys(keys []onKeyPair) error {
	for i := range keys {
		k := &keys[i]
		ti := batch.ResolveSchemaIndex(ev.targetCols, k.TargetCol)
		if ti < 0 {
			return sqlerr.New("42703", "column %s.%s does not exist", ev.targetAlias, k.TargetCol)
		}
		k.TargetCol = ev.targetCols[ti].Name
		if ev.sourceKnown || ev.sourceNamed {
			si := batch.ResolveSchemaIndex(ev.srcCols, k.SourceCol)
			if si < 0 {
				return sqlerr.New("42703", "column %s.%s does not exist", ev.sourceAlias, k.SourceCol)
			}
			if n := ev.srcCols[si].Name; n != "" {
				k.SourceCol = n
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
	if err := dmlExpressionTyping(node, "", ev.mergedCols); err != nil {
		return nil, err
	}
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
//
// Through batch.ResolveSchemaIndex, not through a map keyed by the fold, for
// checkOnKeys' reason one function over: the fold is only half the rule. By
// the time a name reaches here it has been through dmlIdent, so an unquoted
// target is folded and a target still carrying an upper-case letter can only
// have been DELIMITED. A lowercasing lookup answers both the same, so
// `SET "USERAGENT" = 'X'` would bind to `UserAgent` and WRITE, where
// PostgreSQL raises 42703 for a delimited name that is not the column's own
// bytes — the disposition ResolveDMLSetClauses already gives the UPDATE door.
// A FOLDED `useragent` still resolves: that is the concession a parquet-born
// CamelCase schema needs, and the only one (batch/schema.go items 1-4).
func (ev *mergeEvaluator) targetColumn(name string) (parquet.Column, error) {
	name = strings.TrimSpace(name)
	idx := batch.ResolveSchemaIndex(ev.targetCols, name)
	if idx < 0 {
		return parquet.Column{}, sqlerr.New("42703",
			"column %q of relation %q does not exist", name, ev.target)
	}
	return ev.targetCols[idx], nil
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

// dmlSourceDeclaredType is the OTHER fact a computed assignment needs beside
// srcFloat: not "is this a float" but WHAT the source declares outright, so
// assignEvaluatedValue can pick DATE, TIMESTAMP, BOOL and the network/UUID
// arms' RULE from the expression's type rather than guess it from the Go box
// shape (round-2 review B1/P2). The box alone cannot tell a DATE-declared
// expression's int32/int64 day count from a plain INTEGER expression that
// boxes the identical shape — `DATE '1970-01-06'` and `2 + 3` both carry
// int64(5) — which is exactly how `(d date) VALUES (2 + 3)` stored a date
// instead of raising PostgreSQL's 42804.
//
// physical.DeclaredTypeOfNode already resolves an explicit CAST from its own
// destination type NAME (declared_output.go's CastNode case) rather than the
// operand it casts, which is the rule assignment wants here too:
// `CAST(x AS DATE)` declares DATE even when x itself cannot be typed. No
// CAST-first shortcut is needed the way dmlSourceIsFloat's is — that one
// exists for a narrower rounding-mode nuance (a bare `::numeric` over a float
// operand), not for the type CLASS this function answers.
//
// ok is false for an UNDECIDED source (a shape this layer cannot type at
// all), and every caller's contract for that case is to fall back to the
// pre-arc box-shape reading rather than refuse: declining to refuse is safer
// than guessing wrong on a shape this fix cannot yet name.
func dmlSourceDeclaredType(node plansql.Node, schema []parquet.Column) (parquet.TypeID, bool) {
	decl, conf := physical.DeclaredTypeOfNode(node, schema)
	if conf != expr.Decided {
		return 0, false
	}
	return decl.ID, true
}

// assignSource is everything the ONE assignment function needs to know about
// the value a write door assigns to a column, and every door builds it the
// same way (assignSourceOf) from the same fact — the source EXPRESSION as the
// statement wrote it:
//
//   - a bare constant (a number, a quoted literal, TRUE/FALSE, NULL, a signed
//     number): its SQL text, read by the TARGET's input rules exactly as
//     PostgreSQL types an unknown or numeric literal from its target — so
//     `2.50` into TEXT is `2.50` and `2.5` into INTEGER is 3 on EVERY door;
//   - a bare call the registry declares TEXT for a network or UUID value
//     (expr.DeclaresTextForTypedValue): read as an unknown-typed literal, the
//     documented superset;
//   - anything else: its DECLARED type (and whether that is a float, which
//     decides the integer rounding), read by assignEvaluatedValue.
//
// Round 3 had one TABLE but two converters: VALUES / SET / MERGE read a
// constant through its literal text, INSERT … SELECT through the value the
// SELECT list had already evaluated — a numeric literal there is a float, so
// `SELECT 2.50` into TEXT stored `2.5` and `SELECT 2.5` into INTEGER stored 2,
// and a quoted `'t'` into BOOLEAN stored on VALUES and was 42804 on INSERT …
// SELECT (round-3 review B2 / P2). Now INSERT … SELECT classifies each select
// item's AST through the same assignSourceOf (selectItemSources), and every
// door calls check then assign on the result.
type assignSource struct {
	literal   string // the constant's SQL text when isLiteral
	isLiteral bool
	typedText bool
	declType  parquet.TypeID
	declKnown bool
	declFloat bool
	// declScale is a DECIMAL source's declared scale: a numeric constant
	// evaluated inside an expression (`CASE … THEN 2.50 END`) arrives as a
	// float box and is numeric at that scale (arc VL round 5).
	declScale int
}

// assignSourceOf classifies a source expression. schema resolves column
// references (nil for a VALUES cell, which has none).
func assignSourceOf(node plansql.Node, schema []parquet.Column) assignSource {
	if lit, ok := dmlLiteralText(node); ok {
		src := assignSource{literal: lit, isLiteral: true}
		src.declType, src.declKnown = literalDeclaredType(node)
		return src
	}
	if dmlTypedTextSource(node) {
		return assignSource{typedText: true}
	}
	decl, conf := physical.DeclaredTypeOfNode(node, schema)
	if conf != expr.Decided {
		return assignSource{declFloat: dmlSourceIsFloat(node, schema)}
	}
	return assignSource{declType: decl.ID, declKnown: true, declFloat: dmlSourceIsFloat(node, schema),
		declScale: decl.Scale}
}

// declaredSource is the source of a query output column whose expression is
// not a constant: its declaration from the plan.
func declaredSource(c parquet.Column) assignSource {
	return assignSource{declType: c.Type, declKnown: true, declScale: c.Scale,
		declFloat: c.Type == parquet.TypeFloat32 || c.Type == parquet.TypeFloat64}
}

// literalDeclaredType is a constant's own type for the assignment TABLE:
// PostgreSQL's literal rule — an integer literal is integer when it fits and
// bigint otherwise, any other number numeric, TRUE/FALSE boolean. A quoted
// literal and NULL are SQL's unknown, typed from the target: no declaration.
func literalDeclaredType(node plansql.Node) (parquet.TypeID, bool) {
	n := unwrapDMLParens(node)
	if u, ok := n.(*plansql.UnaryOp); ok {
		n = unwrapDMLParens(u.Inner)
	}
	lit, ok := n.(*plansql.Lit)
	if !ok {
		return 0, false
	}
	switch lit.Kind {
	case plansql.LitBool:
		return parquet.TypeBool, true
	case plansql.LitNumber:
		if i, err := strconv.ParseInt(lit.Value, 10, 64); err == nil {
			if i >= math.MinInt32 && i <= math.MaxInt32 {
				return parquet.TypeInt32, true
			}
			return parquet.TypeInt64, true
		}
		return parquet.TypeDecimal, true
	}
	return 0, false
}

// numericLiteralText renders a numeric literal as PostgreSQL's numeric output
// does: exact, at the literal's display scale — the digits after the point
// less the exponent, never below zero.
func numericLiteralText(lit string) string {
	r, ok := new(big.Rat).SetString(lit)
	if !ok {
		return lit
	}
	mant, exp := lit, 0
	if i := strings.IndexAny(lit, "eE"); i >= 0 {
		mant = lit[:i]
		if e, err := strconv.Atoi(lit[i+1:]); err == nil {
			exp = e
		}
	}
	scale := 0
	if i := strings.IndexByte(mant, '.'); i >= 0 {
		scale = len(mant) - i - 1
	}
	scale -= exp
	if scale < 0 {
		scale = 0
	}
	return r.FloatString(scale)
}

// check is the ONE assignment table (ingest.AssignableToColumn, PostgreSQL's
// assignment casts) asked of a source before any row is read — PostgreSQL's
// order: a type mismatch refuses the statement even when no row matches.
//
// An unknown-typed constant (quoted, NULL) is typed from the target and has
// nothing to ask here: the target's input function answers at assign. A
// typed-text call asks AssignableFromUnknownLiteral. A source the declaration
// walk cannot type keeps its value path unchanged. The pairs outside the
// classified scalar set (containers, VECTOR, BYTES, DURATION) are asked of
// the table too when the source is DECLARED by a plan's output (INSERT …
// SELECT), which is where they reach a write.
func (s assignSource) check(col parquet.Column) error {
	if s.typedText {
		return ingest.AssignableFromUnknownLiteral(col)
	}
	if !s.declKnown {
		return nil
	}
	if !assignmentClassified(s.declType) || !assignmentClassified(col.Type) {
		if s.declType == col.Type || s.isLiteral {
			// A constant into a type outside the classified set (a number
			// into DURATION) is read by that column's own input rule.
			return nil
		}
		return ingest.AssignableToColumn(parquet.Column{Type: s.declType}, col)
	}
	if err := ingest.AssignableToColumn(parquet.Column{Type: s.declType, Precision: col.Precision, Scale: col.Scale}, col); err != nil {
		return datatypeMismatchNamed(col, s.declType)
	}
	return nil
}

// assign converts one value of the source to the column's box — the ONE
// converter. v is ignored for a constant, whose SQL text is the value.
func (s assignSource) assign(v any, col parquet.Column) (any, error) {
	switch {
	case s.isLiteral && col.Type == parquet.TypeString && s.declKnown && s.declType != parquet.TypeBool:
		// A NUMERIC literal is a numeric value, and its text is numeric's
		// output: its own digits at its own scale (`2.50` stays `2.50`), an
		// exponent spelled out (`1e3` is `1000`) — PostgreSQL's numeric
		// assignment to text.
		return numericLiteralText(s.literal), nil
	case s.isLiteral:
		return assignLiteralToColumn(s.literal, col)
	case s.typedText:
		return assignUnknownLiteral(v, col)
	}
	if s.declKnown && s.declType == parquet.TypeDecimal {
		v = decimalDeclaredText(v, s.declScale)
	}
	return assignEvaluatedValue(v, col, s.declFloat, s.declType, s.declKnown)
}

// decimalDeclaredText is a DECIMAL-declared value carried in a float or an
// integer box — a numeric constant under CASE / COALESCE / GREATEST in a
// VALUES cell, a SET, a MERGE clause — as the text a DECIMAL column's box
// already is: the numeric at its declared scale, so every door assigns what
// the SELECT-list doors store from their DECIMAL vector (`2.50` into text, 3
// into integer — numeric rounds half away from zero — never the double's
// `2.5` / 2; arc VL round 5, round-4 review B2). Any other box is returned as
// it is.
func decimalDeclaredText(v any, scale int) any {
	switch x := v.(type) {
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return v
		}
		return strconv.FormatFloat(x, 'f', scale, 64)
	case int64, int32, int:
		text := fmt.Sprint(x)
		if scale > 0 {
			text += "." + strings.Repeat("0", scale)
		}
		return text
	}
	return v
}

// assignmentClassified is the scalar set the one assignment table decides:
// the numeric family, TEXT, BOOL, DATE, TIMESTAMP, the address types and
// UUID.
func assignmentClassified(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypePort, parquet.TypeProtocol,
		parquet.TypeFloat32, parquet.TypeFloat64, parquet.TypeDecimal,
		parquet.TypeString, parquet.TypeBool, parquet.TypeDate, parquet.TypeTimestamp,
		parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeMAC, parquet.TypeUUID:
		return true
	}
	return false
}

// dmlExpressionTyping is the expression-typing rule every DML door runs on
// every expression it evaluates — a VALUES cell, a SET value, a WHERE, a
// MERGE clause — before it compiles it: the rule the SELECT binder applies to
// every clause (physical.RefuseTemporalArithmetic, the date/timestamp `+` /
// `-` pairs PostgreSQL has no operator for, 42883). Round 3 installed that
// rule in the binder only, so the same expression was refused on SELECT and
// INSERT … SELECT and stored on VALUES / UPDATE (round-3 review B3).
func dmlExpressionTyping(node plansql.Node, alias string, schema []parquet.Column) error {
	return physical.RefuseTemporalArithmetic(node, alias, schema)
}

// dmlTypedTextSource reports a bare call to a function the registry declares
// TEXT for a network or UUID value (expr.DeclaresTextForTypedValue).
func dmlTypedTextSource(node plansql.Node) bool {
	fc, ok := unwrapDMLParens(node).(*plansql.FuncCallNode)
	return ok && expr.DeclaresTextForTypedValue(fc.Name)
}

// datatypeMismatchNamed is PostgreSQL's 42804 sentence for a declared source
// type the column cannot take.
func datatypeMismatchNamed(col parquet.Column, src parquet.TypeID) error {
	return sqlerr.New("42804", "column %q is of type %s but expression is of type %s",
		col.Name, physical.PgTypeName(col.Type), physical.PgTypeName(src))
}

// sourceDeclaredType is dmlSourceDeclaredType resolved against MERGE's merged
// namespace, the same split sourceIsFloat keeps for the same reason.
func (ev *mergeEvaluator) sourceDeclaredType(node plansql.Node) (parquet.TypeID, bool) {
	return dmlSourceDeclaredType(node, ev.mergedCols)
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
	if err := dmlExpressionTyping(node, "", ev.mergedCols); err != nil {
		return nil, err
	}
	src := assignSourceOf(node, ev.mergedCols)
	if err := src.check(col); err != nil {
		return nil, err
	}
	if src.isLiteral {
		return src.assign(nil, col)
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
			cast, cerr := src.assign(v, col)
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
	v, err := src.assign(compiled.Eval(b, 0), col)
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
	}
	// Every OTHER node kind is typed by the walk the other three DML verbs
	// use, with WHEN as the site: a CALL, a CASE, a CAST, an ARRAY, an
	// INTERVAL, a polymorphic call and a scalar subquery all reach it, and a
	// misplaced aggregate or window is its own class. The bespoke arms above
	// stay because they resolve a MERGE-qualified name (`s.n`) that a plain
	// column scope cannot (#1179 round 2).
	if err := physical.RefuseAggregateInADMLPredicate(node); err != nil {
		return err
	}
	return physical.RefuseNonBooleanClause(node, "WHEN", ev.mergedCols)
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

// assignEvaluatedValue applies assignment casts to VALUES, never carriers
// (ADR-0018 §4; #647, #678). An evaluated integer contributes at scale 0;
// never hand it to DECIMAL storage as an already-unscaled integer.
// DECIMAL assignments round to declared scale and enforce precision;
// integer assignments round by source domain and enforce target range.
// FLOAT targets receive numeric values; TEXT receives rendered values.
// NULL remains NULL; unsupported target families keep the original box.
// See docs/internals/dml-evaluated-assignment-value-domain.md for the design.
//
// srcType/srcKnown are the SOURCE expression's declared type (assignSource,
// via assignSourceOf) — the ONE assignment-cast table every target arm below
// reads (round-2 review B1/B2/P2): the Go box a DATE, a TIMESTAMP and a
// plain INTEGER expression produce collide (int32/int64 day counts and
// epoch-ms counts are indistinguishable at the box from a number that
// happens to share the shape), so only the DECLARATION can tell
// `CAST(x AS DATE)` from `2 + 3` apart, and PostgreSQL's assignment rule is
// keyed on that declaration, never on how the value happens to be carried.
// srcKnown == false (an UNDECIDED source — a shape this layer cannot type at
// all) keeps every arm's pre-arc box-shape reading, unchanged: declining to
// refuse is safer than guessing wrong on a shape this fix cannot yet name.
func assignEvaluatedValue(v any, col parquet.Column, srcFloat bool, srcType parquet.TypeID, srcKnown bool) (any, error) {
	if v == nil {
		return nil, nil
	}
	if iv, isInterval := v.(expr.IntervalValue); isInterval {
		// There is no INTERVAL column type. A TEXT column takes an interval's
		// text (PostgreSQL's assignment cast through interval's output — the
		// value INSERT … SELECT already stored); every other column is
		// PostgreSQL's 42804. Both used to reach the writer's box validation
		// and fail there with no SQLSTATE (arc VL round-4 review P1).
		if col.Type == parquet.TypeString {
			return iv.String(), nil
		}
		return nil, sqlerr.New("42804", "column %q is of type %s but expression is of type interval",
			col.Name, physical.PgTypeName(col.Type))
	}
	switch col.Type {
	case parquet.TypeDecimal:
		return assignDecimalValue(v, col, srcType, srcKnown)
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypePort, parquet.TypeProtocol:
		if s, isText := v.(string); isText &&
			(col.Type == parquet.TypePort || col.Type == parquet.TypeProtocol) {
			// The type's own TEXT form first, so an unknown-typed literal
			// assigned to a PROTOCOL column reads the IANA name (#986, #1088).
			// A SYNTAX miss falls through to the numeric assignment, which has
			// a reading for a fractional literal that this grammar does not.
			switch n, st, _ := parquet.NetworkTextValue(col.Type, s); st {
			case parquet.NetTextOK:
				return n, nil
			case parquet.NetTextRange:
				return nil, parquet.NetworkTextError(col.Type, s, st)
			}
		}
		return assignIntegerValue(v, col, srcFloat, srcType, srcKnown)
	case parquet.TypeFloat32, parquet.TypeFloat64:
		return assignFloatValue(v, col, srcType, srcKnown)
	case parquet.TypeString:
		return assignTextValue(v, col, srcType, srcKnown)
	case parquet.TypeBool:
		// No target arm existed for BOOL at all: a computed value of any
		// other shape reached ingest.checkType raw, "expected bool, got
		// int64" with no SQLSTATE, where PostgreSQL raises 42804 (round-2
		// review P2). The box IS the type here — a Go bool only ever comes
		// from a genuinely boolean-typed expression — so no declared-type
		// lookup is needed to tell an assignable value from a mismatched one.
		if _, isBool := v.(bool); !isBool {
			return nil, datatypeMismatchDeclared(v, col, srcType, srcKnown)
		}
	case parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeMAC, parquet.TypeUUID:
		// The same rule convertUnquoted applies to a VALUES literal: a SQL
		// text value is read by the type's grammar here, so `''` is 22P02
		// rather than the writer's ABSENCE, and a malformed literal is
		// refused naming the column rather than at the flush.
		if s, isText := v.(string); isText {
			if _, st, _ := parquet.NetworkTextValue(col.Type, s); st != parquet.NetTextOK {
				return nil, parquet.NetworkTextError(col.Type, s, st)
			}
		} else if srcKnown && srcType == col.Type && nativeNetworkBox(v, col.Type) {
			// The column's own native carrier (an IPv4 or MAC column's encoded
			// int64, `SET ip = ip`): the writer takes the address TEXT, so the
			// carrier is rendered — it reached ingest raw and failed there
			// with no SQLSTATE (the arc VL round-3 assignment gate's cell).
			if col.Type == parquet.TypeIPv4 {
				return batch.FormatIPv4(uint32(v.(int64))), nil
			}
			return batch.FormatMAC(uint64(v.(int64))), nil
		} else if !srcKnown && !nativeNetworkBox(v, col.Type) {
			// An undecided source whose box is neither text nor this type's own
			// native carrier (a float from temporal arithmetic, a bool) has no
			// reading here: 42804, not a code-less writer error (review P1).
			return nil, datatypeMismatch(v, col)
		} else if srcKnown && srcType != col.Type {
			// A non-text box whose declared source is NOT this same network
			// family (an INTEGER expression, a different address family, a
			// UUID into an IPv4 column, …) has no PostgreSQL assignment cast
			// into inet/macaddr/uuid — round-2 review P2's `1 + 1` into IPv4
			// case. A box the layer could not TYPE at all, or one whose
			// source genuinely IS this column's own network family (an
			// UPDATE/MERGE column-to-column move, whose native box is not
			// text), is unchanged from before this arc.
			return nil, datatypeMismatchDeclared(v, col, srcType, srcKnown)
		}
	case parquet.TypeDate:
		return assignDateValue(v, col, srcType, srcKnown)
	case parquet.TypeTimestamp:
		return assignTimestampValue(v, col, srcType, srcKnown)
	}
	return v, nil
}

// nativeNetworkBox reports whether v is the non-text carrier a column of type
// t boxes as: the encoded int64 of an IPv4 or MAC column.
func nativeNetworkBox(v any, t parquet.TypeID) bool {
	_, isInt := v.(int64)
	return isInt && (t == parquet.TypeIPv4 || t == parquet.TypeMAC)
}

// nonNumericAssignmentSource reports whether a declared source type PostgreSQL
// refuses OUTRIGHT as an assignment into any numeric column family — DATE,
// TIMESTAMP, a network family, UUID, or a container, none of which PostgreSQL
// has an assignment cast from into a number (round-2 review B1: `(n bigint)
// VALUES (DATE '2026-01-01')` stored 20454 where PostgreSQL raises 42804).
//
// TEXT is not on this list because a TEXT source never reaches these arms
// declared: assignSource.check (the one assignment table) refuses it 42804
// before any value is read, on every door. The arms' own text readings are
// for the boxes that ARE numbers — a DECIMAL source boxes its value as
// canonical text (assignDecimalValue's own doc) — and for sources the
// declaration walk cannot type.
func nonNumericAssignmentSource(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeDate, parquet.TypeTimestamp,
		parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeMAC, parquet.TypeUUID,
		parquet.TypeArray, parquet.TypeRow, parquet.TypeMap, parquet.TypeVector:
		return true
	}
	return false
}

// dateBoxToDays reads a DATE-declared expression's box as its epoch-day
// count: every DATE producer boxes an int64 day count (a column, a cast, a
// date/time function, date arithmetic — arc VL round 3); the INSERT … SELECT
// door hands its rows' rendered text.
//
// It never narrows on its own: a day count outside PostgreSQL's DATE range is
// the ONE range rule's 22008 (expr.DateDaysInRange), asked before the int32
// the carrier is. It used to be `int32(t)`, which stored `-5877585-08-22` for
// `SET d = d + 2147483647` (arc VL round-3 review B1, #911's family).
func dateBoxToDays(v any) (int32, error) {
	var n int64
	switch t := v.(type) {
	case int32:
		n = int64(t)
	case int64:
		n = t
	case int:
		n = int64(t)
	case string:
		d, err := parquet.ParseDateDays(t)
		if err != nil {
			return 0, err
		}
		n = int64(d)
	default:
		return 0, fmt.Errorf("unexpected DATE-declared box %T", v)
	}
	if err := expr.DateDaysInRange(n); err != nil {
		return 0, err
	}
	return int32(n), nil
}

// timestampBoxToMillis is dateBoxToDays's TIMESTAMP twin: every TIMESTAMP
// producer boxes epoch milliseconds, held to the same range rule
// (expr.TimestampMillisInRange).
func timestampBoxToMillis(v any) (int64, error) {
	var ms int64
	switch t := v.(type) {
	case int64:
		ms = t
	case string:
		m, err := parquet.ParseTimestampMillis(t)
		if err != nil {
			return 0, err
		}
		ms = m
	default:
		return 0, fmt.Errorf("unexpected TIMESTAMP-declared box %T", v)
	}
	if err := expr.TimestampMillisInRange(ms); err != nil {
		return 0, err
	}
	return ms, nil
}

// assignDateValue normalizes a computed expression's box to the SAME shape
// the literal path already returns for DATE (convertTemporalValue):
// time.Time, so ingest.formatPartitionValue's temporal case — which reads
// ONLY a time.Time and falls back to `%v` for anything else — names a
// PARTITION KEY the same way for `CAST(x AS DATE)` as it does for a
// `DATE '...'` literal assigned to the same column (#1252). Every DATE
// producer boxes the int64 day count the matching COLUMN carries
// (expr.producedTemporal, arc VL round 3).
//
// The RULE, not only the box (round-2 review B1): a TIMESTAMP source
// truncates to its calendar day, matching PostgreSQL's `date(timestamp)`;
// anything else this layer manages to TYPE (INTEGER, FLOAT, DECIMAL, BOOL,
// TEXT, a network family, UUID) has no PostgreSQL assignment cast into DATE
// at all and is refused 42804 rather than read through whichever box arm it
// happens to match. A DATE source, or one this layer could not type
// (srcKnown == false), keeps the box-shape reading below, unchanged.
func assignDateValue(v any, col parquet.Column, srcType parquet.TypeID, srcKnown bool) (any, error) {
	if srcKnown && srcType == parquet.TypeTimestamp {
		ms, err := timestampBoxToMillis(v)
		if err != nil {
			return nil, err
		}
		t := time.UnixMilli(ms).UTC()
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
	}
	if srcKnown && srcType != parquet.TypeDate {
		return nil, datatypeMismatchDeclared(v, col, srcType, srcKnown)
	}
	if _, isBool := v.(bool); isBool {
		return nil, datatypeMismatch(v, col)
	}
	days, err := dateBoxToDays(v)
	if sqlerr.StateOf(err) == "22008" {
		return nil, err // the range rule's answer, not a type mismatch
	}
	if err != nil {
		// A box no DATE reading exists for — a float from `now() - now()`,
		// say — is PostgreSQL's 42804, never a raw box handed to the writer
		// to fail there with no SQLSTATE (round-2 review P1).
		return nil, datatypeMismatchDeclared(v, col, srcType, srcKnown)
	}
	return time.Unix(int64(days)*86400, 0).UTC(), nil
}

// assignTimestampValue is assignDateValue's TIMESTAMP twin: every TIMESTAMP
// producer boxes the int64 epoch-millisecond count the matching COLUMN
// carries.
//
// A DATE source answers that date's midnight, matching PostgreSQL's
// `date::timestamp`; everything else this layer can TYPE besides TIMESTAMP
// itself is refused 42804, the assignDateValue rule mirrored the other way
// (round-2 review B1's `(ts) VALUES (2 + 3)` cell).
func assignTimestampValue(v any, col parquet.Column, srcType parquet.TypeID, srcKnown bool) (any, error) {
	if srcKnown && srcType == parquet.TypeDate {
		days, err := dateBoxToDays(v)
		if err != nil {
			return nil, err
		}
		if expr.TimestampMillisInRange(int64(days)*86400000) != nil {
			return nil, sqlerr.New("22008", "date out of range for timestamp")
		}
		return time.Unix(int64(days)*86400, 0).UTC(), nil
	}
	if srcKnown && srcType != parquet.TypeTimestamp {
		return nil, datatypeMismatchDeclared(v, col, srcType, srcKnown)
	}
	if _, isBool := v.(bool); isBool {
		return nil, datatypeMismatch(v, col)
	}
	ms, err := timestampBoxToMillis(v)
	if sqlerr.StateOf(err) == "22008" {
		return nil, err
	}
	if err != nil {
		// assignDateValue's rule: an unreadable box is 42804 (review P1).
		return nil, datatypeMismatchDeclared(v, col, srcType, srcKnown)
	}
	return time.UnixMilli(ms).UTC(), nil
}

// assignDecimalValue is the R1 fix. An INTEGER box is rendered to its decimal
// TEXT before it reaches DecimalValueFromBox, because that function reads an
// integer as the UNSCALED carrier (ADR-0018 §4) and reads text as the VALUE.
// Every other box the engine produces for a numeric expression — a float64
// from real arithmetic, the numeric text a DECIMAL column reads back as — is
// already on the value path and is left alone.
//
// A DECIDED source nonNumericAssignmentSource names — DATE, TIMESTAMP, a
// network family, UUID, a container — is refused 42804 before any box is
// read (round-2 review B1's `(dec numeric(10,2)) VALUES (DATE '2026-01-01')`
// cell, which stored 20454.00).
func assignDecimalValue(v any, col parquet.Column, srcType parquet.TypeID, srcKnown bool) (any, error) {
	if srcKnown && nonNumericAssignmentSource(srcType) {
		return nil, datatypeMismatchDeclared(v, col, srcType, srcKnown)
	}
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
// BOOL is the usual case that reaches it (so does an undecided source's box
// with no reading for the column, and datatypeMismatchDeclared's fallback):
// `SET n = b` used to fail at ingest.checkType with "expected integer, got
// bool" and no SQLSTATE, and
// `SET d = b` reached DecimalValueFromBox's default and answered 22P02, where
// PostgreSQL says 42804 for both (#678 re-review N3). A bool assigned to a
// TEXT column is NOT here: PostgreSQL accepts it and stores 'true'. A bare
// `VALUES (TRUE)` into a numeric column reaches it too (#1252) — the LITERAL
// keyword, not only a computed bool.
//
// physical.PgTypeName, not col.Type's own stringer: PostgreSQL's message
// names the column "bigint", and col.Type.String() answers the wadjet-
// internal spelling "INT64" — a message about the SAME SQLSTATE a query
// author can already see get PostgreSQL's own wording elsewhere
// (validate_boolean.go, validate_comparison_types.go) had not been carried
// to this door.
func datatypeMismatch(v any, col parquet.Column) error {
	return sqlerr.New("42804", "column %q is of type %s but expression is of type %s",
		col.Name, physical.PgTypeName(col.Type), dmlBoxTypeName(v))
}

// datatypeMismatchDeclared is datatypeMismatch's round-2 sibling: it names
// the expression side from the SOURCE's declared type when one is known,
// not from the Go box dmlBoxTypeName reads.
//
// The box collides across families the declared type does not: a DATE
// source boxes as the SAME int32/int64 shape a plain INTEGER expression
// does, so `datatypeMismatch` alone — asked about a DATE-declared source
// refused into a bigint column — answered "column \"n\" is of type bigint
// but expression is of type bigint", naming the SAME word on both sides of
// a message about two DIFFERENT types (measured: `UPDATE ... SET n = DATE
// '2026-01-01'`). physical.PgTypeName(srcType) is the same renderer the
// column side already uses, so "date"/"timestamp without time zone"/"inet"/
// "uuid" now appear on the expression side exactly as PostgreSQL spells them.
//
// srcKnown == false falls back to datatypeMismatch's box-based guess
// unchanged — the path an undecided source takes (assignDateValue,
// assignTimestampValue, the BOOL target arm) when its box has no DATE /
// TIMESTAMP reading.
func datatypeMismatchDeclared(v any, col parquet.Column, srcType parquet.TypeID, srcKnown bool) error {
	if !srcKnown {
		return datatypeMismatch(v, col)
	}
	return sqlerr.New("42804", "column %q is of type %s but expression is of type %s",
		col.Name, physical.PgTypeName(col.Type), physical.PgTypeName(srcType))
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

// assignIntegerValue rounds, range-checks and narrows to the target integer.
// Float sources round half to EVEN, numeric sources half AWAY FROM ZERO
// (#699); srcFloat is the declaration from physical.DeclaredTypeOfNode,
// not a guess from the Go box. Undecided sources retain numeric rounding.
// Out-of-range values, NaN and infinities must raise 22003.
// Enforce PORT uint16 and PROTOCOL uint8 ranges here too: computed values
// bypass the literal converter and no later writer rechecks those widths.
//
// A DECIDED source nonNumericAssignmentSource names is refused 42804 before
// any box is read (round-2 review B1's `(n bigint) VALUES (DATE …)` cell,
// which stored the day count as a plain integer).
// See docs/internals/dml-integer-assignment-rounding.md for the design.
func assignIntegerValue(v any, col parquet.Column, srcFloat bool, srcType parquet.TypeID, srcKnown bool) (any, error) {
	if srcKnown && nonNumericAssignmentSource(srcType) {
		return nil, datatypeMismatchDeclared(v, col, srcType, srcKnown)
	}
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
		return assignIntegerValue(float64(t), col, srcFloat, srcType, srcKnown)
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
	case parquet.TypePort, parquet.TypeProtocol:
		// The type boundary's bound, in the ONE place it is written down
		// (parquet.NetworkIntBounds). It used to be spelled out here and again
		// at the VALUES literal door, and a third door had none at all.
		lo, hi = parquet.NetworkIntBounds(col.Type)
	}
	if n < lo || n > hi {
		if col.Type == parquet.TypePort || col.Type == parquet.TypeProtocol {
			return nil, parquet.NetworkIntRangeError(col.Type, n)
		}
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
//
// A DECIDED source nonNumericAssignmentSource names is refused 42804 before
// any box is read (round-2 review B1's `(f double) VALUES (DATE …)` cell).
func assignFloatValue(v any, col parquet.Column, srcType parquet.TypeID, srcKnown bool) (any, error) {
	if srcKnown && nonNumericAssignmentSource(srcType) {
		return nil, datatypeMismatchDeclared(v, col, srcType, srcKnown)
	}
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
//
// A DATE or TIMESTAMP source renders through the SAME text PostgreSQL's
// assignment cast to text answers — `batch.FormatDate`/`FormatTimestamp`,
// the one renderer every other door already uses — rather than falling to
// the generic box switch below, which had no DATE/TIMESTAMP arm at all and
// printed the raw day count or epoch-ms number instead (round-2 review B1's
// `(s text) VALUES (DATE '2026-01-01')` cell, and B2's `INSERT … SELECT
// CURRENT_DATE` into a TEXT column, the same rule at the other call site).
func assignTextValue(v any, _ parquet.Column, srcType parquet.TypeID, srcKnown bool) (any, error) {
	if srcKnown && srcType == parquet.TypeDate {
		days, err := dateBoxToDays(v)
		if sqlerr.StateOf(err) == "22008" {
			return nil, err
		}
		if err != nil {
			return v, nil
		}
		return batch.FormatDate(days), nil
	}
	if srcKnown && srcType == parquet.TypeTimestamp {
		ms, err := timestampBoxToMillis(v)
		if sqlerr.StateOf(err) == "22008" {
			return nil, err
		}
		if err != nil {
			return v, nil
		}
		return batch.FormatTimestamp(ms), nil
	}
	if srcKnown {
		if s, ok := networkAssignmentText(v, srcType); ok {
			return s, nil
		}
	}
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
		return batch.FormatFloat8Text(t, 64), nil
	case float32:
		return batch.FormatFloat8Text(float64(t), 32), nil
	case bool:
		if t {
			return "true", nil
		}
		return "false", nil
	}
	return v, nil
}

// networkAssignmentText renders an address or MAC source as PostgreSQL's
// assignment cast to text does: an inet HOST carries its mask (`10.0.0.1/32`,
// `::1/128` — text(inet), measured on 17.11), a network its own, a MAC its
// colon form. The box is the column's native carrier (the encoded int64 of an
// IPv4 or MAC) or the address text; either renders.
func networkAssignmentText(v any, t parquet.TypeID) (string, bool) {
	var s string
	switch b := v.(type) {
	case string:
		s = b
	case int64:
		switch t {
		case parquet.TypeIPv4:
			s = batch.FormatIPv4(uint32(b))
		case parquet.TypeMAC:
			return batch.FormatMAC(uint64(b)), true
		default:
			return "", false
		}
	default:
		return "", false
	}
	switch t {
	case parquet.TypeIPv4:
		if !strings.Contains(s, "/") {
			s += "/32"
		}
		return s, true
	case parquet.TypeIPv6:
		if !strings.Contains(s, "/") {
			s += "/128"
		}
		return s, true
	case parquet.TypeCIDR, parquet.TypeMAC, parquet.TypeUUID:
		return s, true
	}
	return "", false
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
	if sqlerr.StateOf(err) != "" {
		// The column's own input grammar has already CLASSIFIED this literal —
		// 22003 for a PORT past 65535, 22P02 for text naming no address — and
		// a classified refusal is an answer, not a hint to try a second
		// reading. Falling through re-read the STILL-QUOTED literal through
		// the numeric cast, which reported `invalid input syntax for type
		// numeric: "'65536'"` (quotes included) for a value the type boundary
		// had already refused with the right code and the right words.
		return nil, err
	}
	switch col.Type {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypePort, parquet.TypeProtocol:
		// The BOOLEAN KEYWORD, unquoted, is a TYPE the column cannot take at
		// all — PostgreSQL's 42804 — not a NUMBER it cannot parse: `SELECT
		// true::bigint` is 42804 `cannot cast type boolean to bigint`, where
		// `SELECT 'true'::bigint` (a STRING) is 22P02, the reading the
		// fallback below still gives a QUOTED 'true' or 'false'. Checked here,
		// on the literal's own TEXT, because the second reading below hands
		// this branch only TEXT, never the Go bool assignIntegerValue's own
		// `case bool` already catches for a COMPUTED value (#1252).
		if strings.EqualFold(text, "true") || strings.EqualFold(text, "false") {
			return nil, datatypeMismatch(false, col)
		}
		// The cast's answer wins outright here, error included: strconv's
		// "invalid syntax" and "value out of range" carry no SQLSTATE, while
		// the cast raises PostgreSQL's own 22P02 for text naming no number
		// and 22003 for a magnitude the column cannot hold.
		// A LITERAL, so the numeric rule: PostgreSQL reads an unadorned
		// `2.5` as numeric and rounds it half away from zero (#699). No
		// declared source type: the literal's own TEXT is being tried
		// against the numeric reading directly, not an expression with a
		// declaration to name.
		v, cerr := assignEvaluatedValue(text, col, false, 0, false)
		if cerr == nil {
			return v, nil
		}
		// BOTH readings failed, so the one the user is told about is the
		// TYPE'S OWN. The second reader is handed the STILL-QUOTED text and
		// names its own type, so `VALUES ('zzz')` into a PORT column reported
		// `invalid input syntax for type numeric: "'zzz'"` — a different type
		// name, and the literal with its quotes in it — where every other
		// door says `invalid input syntax for type integer: "zzz"`. One
		// refusal has one sentence (round-1 review, N1).
		//
		// Only for a QUOTED literal: an unquoted number is a NUMBER and the
		// cast's refusal is the right one for it, the same split #1141 makes
		// at the CAST door.
		if inner, quoted := dmlUnquotedLiteral(text); quoted {
			// ONLY a type whose own text reader exists. parquet.NetworkTextError
			// names NetworkTextTypeName(typ), which falls back to the INTERNAL
			// identifier for everything else — so an INT64 column reported
			// `invalid input syntax for type INT64`, a name no client can look
			// up in pg_type and not the `bigint` this engine's other doors
			// give for the same column (round-2 review).
			if st, known := netTextStatusOf(col.Type, inner); known {
				if nerr := parquet.NetworkTextError(col.Type, inner, st); nerr != nil {
					return nil, nerr
				}
			}
			// int4in / int8in's own sentence, on the unquoted text: a bound
			// `$1 = '2.5'` into an integer said `for type numeric: "'2.5'"`
			// (round-4 review N4).
			if (col.Type == parquet.TypeInt32 || col.Type == parquet.TypeInt64) && sqlerr.StateOf(cerr) == "22P02" {
				if n, perr := strconv.ParseInt(strings.TrimSpace(inner), 10, 64); errors.Is(perr, strconv.ErrRange) ||
					(perr == nil && col.Type == parquet.TypeInt32 && (n < math.MinInt32 || n > math.MaxInt32)) {
					return nil, sqlerr.New("22003", "value %s is out of range for type %s",
						sqlerr.Quote(inner), physical.PgTypeName(col.Type))
				}
				return nil, sqlerr.New("22P02", "invalid input syntax for type %s: %s",
					physical.PgTypeName(col.Type), sqlerr.Quote(inner))
			}
		}
		return nil, cerr
	}
	return nil, err
}

// assignInsertValue resolves one VALUES cell against its target column
// (#1252). insertValueText hands over the cell's SOURCE TEXT — no longer
// only a bare literal's — because VALUES accepts any scalar expression
// PostgreSQL's does: a typed literal (`TIMESTAMP '...'`), a function call
// (`now()`), arithmetic (`1 + 1`), a CAST. It is evaluated through the SAME
// expression compiler SELECT uses (expr.Compile), no second evaluator. Every
// cell is first classified and checked by the one assignment table
// (assignSourceOf, assignSource.check); a bare literal (including a signed
// number) is then read from its SQL text (assignSource.assign →
// assignLiteralToColumn), keeping DECIMAL exactness and that path's SQLSTATEs.
//
// DEFAULT is the one keyword this clause does not compile: no column this
// catalog describes ever carries an explicit default (parquet.Column has no
// such field), so PostgreSQL's own rule for a column with none — the value
// is NULL — applies uniformly, and NULL is exactly what an explicit
// `VALUES (NULL)` already resolves to two lines below.
//
// A column reference and a subquery are refused, not evaluated: VALUES has
// no FROM to resolve either against, and PostgreSQL raises 42703 for the
// first for the same reason. The second it DOES accept (a scalar subquery is
// a constant to it), a query environment this evaluator has none of, so it
// names the gap with 0A000 instead of silently answering NULL.
func assignInsertValue(text string, col parquet.Column) (any, error) {
	trimmed := strings.TrimSpace(text)
	if strings.EqualFold(trimmed, "default") {
		return nil, nil
	}
	node, err := plansql.ParseExpressionComplete(trimmed)
	if err != nil {
		return nil, sqlerr.Wrap("42601", fmt.Errorf("parsing %q: %w", trimmed, err))
	}
	// The expression is typed before it is assigned — PostgreSQL's order.
	if err := dmlExpressionTyping(node, "", nil); err != nil {
		return nil, err
	}
	src := assignSourceOf(node, nil)
	if err := src.check(col); err != nil {
		return nil, err
	}
	if src.isLiteral {
		return src.assign(nil, col)
	}
	if err := refuseNonConstantValuesExpr(node); err != nil {
		return nil, err
	}
	compiled, err := expr.Compile(node)
	if err != nil {
		return nil, fmt.Errorf("compiling %q: %w", trimmed, err)
	}
	v, err := src.assign(compiled.Eval(&batch.RecordBatch{Len: 1}, 0), col)
	if err != nil {
		return nil, err
	}
	return v, checkValueForColumn(v, col)
}

// refuseNonConstantValuesExpr refuses the two shapes a VALUES cell can name
// that assignInsertValue cannot evaluate with no FROM and no query
// environment: see assignInsertValue's own doc for why each is refused
// rather than answered.
func refuseNonConstantValuesExpr(node plansql.Node) error {
	if dmlClauseHasSubquery(node) {
		return sqlerr.New("0A000",
			"a subquery in an INSERT ... VALUES expression is not supported")
	}
	refs, err := plansql.ColumnRefsOutsideSubqueries(node)
	if err != nil {
		return sqlerr.Wrap("0A000", err)
	}
	if len(refs) > 0 {
		return sqlerr.New("42703", "column %q does not exist", refs[0].Column)
	}
	return nil
}

// dmlUnquotedLiteral is convertValue's quoting rule, asked of a literal's
// source text: a leading and trailing apostrophe are quoting, and the
// apostrophes inside were doubled by it. It reports false for everything that
// is not a quoted literal, which is what separates `'zzz'` — text, and read by
// the column type's own input function — from `2.5`, a number the assignment
// cast still has a reading for.
func dmlUnquotedLiteral(text string) (string, bool) {
	s := strings.TrimSpace(text)
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return "", false
	}
	return strings.ReplaceAll(s[1:len(s)-1], "''", "'"), true
}

// netTextStatusOf re-asks the column type's own reader for the status of a
// literal it has already refused, so the message names what that reader
// refused it for — 22P02 for text naming no value, 22003 for a number outside
// the type's range.
func netTextStatusOf(typ parquet.TypeID, s string) (parquet.NetTextStatus, bool) {
	_, st, ok := parquet.NetworkTextValue(typ, s)
	if !ok {
		return parquet.NetTextSyntax, false
	}
	return st, true
}

// checkValueForColumn is ConvertValueForColumn's half for a value that is
// already a Go box rather than literal text: it validates and returns nothing,
// because there is nothing to convert.
//
// It asks the SAME question columnChecked asks, through the same code, so the
// two halves cannot answer differently — it had its own copy of the DECIMAL
// rule and therefore missed the VECTOR one when that arrived, which is the
// shape of defect a second copy always produces.
func checkValueForColumn(v any, col parquet.Column) error {
	_, err := columnChecked(v, nil)(col)
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
		// dmlIdent, not TrimSpace: this list is raw SQL text for the reason
		// applySetClauses gives, so `("WatchID", ...)` arrived here as the
		// eleven bytes including the quotes and matched no column at all.
		columns[i] = dmlIdent(columns[i])
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
		// The SCHEMA's spelling, for the reason applySetClauses gives: this
		// row is INGESTED, and the writer reads `row[col.Name]` byte-exactly.
		// Here the miss is PER COLUMN, so a mixed-case schema loses exactly
		// the columns whose spelling the reference does not match. Measured
		// over `hits(WatchID, counterid, UserAgent)`:
		//
		//	WHEN NOT MATCHED THEN INSERT (watchid, counterid, useragent)
		//	  VALUES (42, 7, 'INSERTED')
		//	before: MERGE 1, row stored as [<nil> 7 <nil>]
		//	after:  MERGE 1, row stored as [42 7 INSERTED]
		//
		// `counterid` — the one column this schema already spells folded —
		// survived; the two CamelCase columns became NULL. Under a NOT NULL
		// schema the same statement was refused instead, with a
		// `null value in column "WatchID" violates not-null constraint` that
		// names a column the statement did supply a value for.
		row[target.Name] = val
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
	// The #686 rule reaches INSIDE a subquery too. An alias hides the table
	// name, so a subquery that spells the target by its table name while the
	// statement declared an alias names no relation in scope — PostgreSQL
	// raises 42P01 with a hint naming the alias, and the check has to live
	// here because a subquery's own text is opaque to the ref walk above.
	// Without it the reference reached the correlated evaluator's dangling
	// guard, which is loud (0A000) but says something else entirely.
	if target.Alias != "" {
		for _, sub := range dmlSubquerySQLs(node) {
			for _, d := range plansql.DanglingTableRefs(sub) {
				if strings.EqualFold(d.Table, target.Table) {
					return sqlerr.New("42P01",
						"invalid reference to FROM-clause entry for table %q; "+
							"perhaps you meant to reference the table alias %q",
						target.Table, target.Alias)
				}
			}
		}
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
		// RESOLVED, not lowercased. `ref.Column` comes from the WHERE
		// clause's parse, so it carries the lexer's verdict — folded when it
		// was written unquoted, its own bytes when it was delimited — and a
		// map keyed by the fold cannot see the difference. So a delimited
		// wrong-case name was ACCEPTED here and then failed to resolve
		// against the batch at evaluation time, where the resolver does apply
		// the rule: the predicate answered false for every row and the
		// statement reported a truthful-looking no-op.
		//
		//	UPDATE hits SET UserAgent = 'Z' WHERE "WATCHID" = 1
		//	before: OK, rows=0        after: 42703, column "WATCHID" ...
		//	DELETE FROM hits WHERE "WATCHID" = 3
		//	before: OK, rows=0        after: 42703
		//
		// A column that is genuinely absent was already refused, so the two
		// spellings of "this column is not here" now answer alike — which is
		// PostgreSQL's disposition for both. An unquoted `watchid` resolves
		// case-insensitively against a CamelCase schema, as everywhere else.
		if batch.ResolveSchemaIndex(schema, ref.Column) < 0 {
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

// dmlSubquerySQLs returns the SQL text of every subquery embedded in a DML
// predicate, at this level. It does not descend into a subquery's own text:
// plansql.DanglingTableRefs already walks nesting, so a two-deep reference
// surfaces at the outermost subquery.
func dmlSubquerySQLs(n plansql.Node) []string {
	var out []string
	var walk func(plansql.Node)
	walk = func(n plansql.Node) {
		switch e := n.(type) {
		case nil:
			return
		case *plansql.SubqueryNode:
			out = append(out, e.SQL)
		case *plansql.ExistsNode:
			out = append(out, e.SQL)
		case *plansql.CmpExpr:
			walk(e.Left)
			walk(e.Right)
		case *plansql.BinaryOp:
			walk(e.Left)
			walk(e.Right)
		case *plansql.AndNode:
			walk(e.Left)
			walk(e.Right)
		case *plansql.OrNode:
			walk(e.Left)
			walk(e.Right)
		case *plansql.NotNode:
			walk(e.Inner)
		case *plansql.ParenNode:
			walk(e.Inner)
		case *plansql.UnaryOp:
			walk(e.Inner)
		case *plansql.CastNode:
			walk(e.Inner)
		case *plansql.IsExpr:
			walk(e.Left)
		case *plansql.LikeExpr:
			walk(e.Left)
			walk(e.Pattern)
		case *plansql.BetweenExpr:
			walk(e.Left)
			walk(e.Low)
			walk(e.High)
		case *plansql.InExpr:
			walk(e.Left)
			for _, v := range e.Values {
				walk(v)
			}
		case *plansql.AnyAllExpr:
			walk(e.Left)
			for _, v := range e.Values {
				walk(v)
			}
		case *plansql.FuncCallNode:
			for _, a := range e.Args {
				walk(a)
			}
		case *plansql.CaseNode:
			walk(e.Subject)
			walk(e.Else)
			for _, w := range e.Whens {
				walk(w.Cond)
				walk(w.Result)
			}
		}
	}
	walk(n)
	return out
}

// refuseDMLLiteralPairs rejects unsupported qualifying pairs before any row
// is read (#721, ADR-0012 item 12): STRING/BYTES versus unquoted NUMBER,
// and non-BOOL versus BOOLEAN, raise 42883.
// Numeric versus invalid QUOTED literals uses expr.RefuseNumericLiteral,
// the runtime's same predicate, even on empty tables (#536, #646).
// SELECT retains its byte-comparison concession; writes refuse these pairs.
// Temporal/network versus NUMBER is outside this guard: their accept-sets
// differ, and stricter local parsers must not reject server-accepted input
// (ADR-0012 item 1). Fixtures must probe both sides of that boundary.
// See docs/internals/dml-literal-comparison-refusal-boundary.md for the design.
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

// BuildDMLPredicate resolves every name against schema before execution
// (#678); run it only through MatchDMLRows, on all doors (#815).
// Empty WHERE means all rows only if StmtSQL has no top-level WHERE token;
// a dropped WHERE must fail, never widen a write (#686, ADR-0019 item 8).
// Predicates are compiled per-row, not planned (ADR-0031); subqueries use
// ordinary SELECT planning and target/alias scope (#688).
// Uncorrelated subqueries memoize; correlated ones rerun with typed outer
// literals, refuse unrenderable values and propagate failures (ADR-0021 §1e, §1c).
// Target-table subqueries precede this statement's marker commit (ADR-0030).
// See docs/internals/dml-predicate-compilation-and-subqueries.md for the design.
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
	// A quoted literal in a truth context is read with PostgreSQL's boolean
	// INPUT function, exactly as a SELECT's WHERE reads it: `WHERE 'true'`
	// removes every row, `WHERE 'no'` removes none, and `WHERE 'abc'` is
	// 22P02. This clause never reached that walk (ADR-0031), so all three
	// removed nothing (#1179 round 2).
	node, err = plansql.CoerceBooleanNode(node)
	if err != nil {
		return nil, err
	}
	// A WINDOW is refused BEFORE the columns are resolved and an AGGREGATE
	// AFTER — the server's own order, and the order the SELECT door keeps
	// (round-2 review, P3-r2).
	if err := physical.RefuseWindowInADMLPredicate(node); err != nil {
		return nil, err
	}
	if err := checkDMLColumns(node, target, schema); err != nil {
		return nil, err
	}
	if err := physical.RefuseAggregateInADMLPredicate(node); err != nil {
		return nil, err
	}
	if err := refuseDMLLiteralPairs(node, schema); err != nil {
		return nil, err
	}
	if err := dmlExpressionTyping(node, target.Alias, schema); err != nil {
		return nil, err
	}
	// A TRUTH CONTEXT, held to the same rule a SELECT's WHERE is held to:
	// `DELETE FROM t WHERE id > 0 AND n # 3` is 42804 on the server and
	// deleted every row here, because the per-row closure reads a non-boolean
	// as false only at the TOP of the clause (#1179).
	if err := physical.RefuseNonBooleanDMLPredicate(node, target.Alias, schema); err != nil {
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
	// The ONE name the target answers to, and it IS one name: an ALIAS HIDES
	// the table name. `DELETE FROM pr AS a WHERE pr.id = 1` is 42P01 in
	// PostgreSQL and here — checkDMLColumns says so and says why: accepting
	// both spellings would let one statement mean two things depending on
	// which resolved (#686). Putting BOTH into the subquery's outer scope
	// held that rule outside a subquery and broke it inside one, so
	// `DELETE FROM pr AS a WHERE EXISTS (… WHERE s.id = pr.id)` answered
	// where PostgreSQL raises 42P01 with a HINT naming the alias.
	relation := target.Table
	if target.Alias != "" {
		relation = target.Alias
	}
	outerTables := map[string]bool{strings.ToLower(relation): true}
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
	// src is the source expression as the one assignment function reads it
	// (assignSourceOf): its declared type and float-ness (#699, round-2
	// review B1), or the typed-text reading. The compiled expr cannot carry
	// any of it — expr.Expr is one method, Eval — and the BOX cannot decide
	// it, because a DATE, a TIMESTAMP and a plain INTEGER collide there.
	src assignSource
}

// ResolveDMLSetClauses resolves targets against schema before execution;
// missing columns raise 42703 and expressions must be evaluated (#678).
// Choose the constant path by parsed literal shape, never successful string
// conversion; preserve declared DECIMAL/temporal checks there (#647).
// Compile other values against the file batch's declared types.
// SET expressions use the same target/alias scope as WHERE: under alias a,
// a.n resolves and the hidden relation name does not (42P01; #686).
// See docs/internals/update-set-resolution-and-evaluation.md for the design.
func ResolveDMLSetClauses(clauses []plansql.SetClause, target plansql.DMLTarget, schema []parquet.Column) ([]DMLAssignment, error) {
	out := make([]DMLAssignment, 0, len(clauses))
	for _, sc := range clauses {
		// UPDATE's parser rejects qualified SET targets before this point (42601;
		// PostgreSQL reports 42703). MERGE strips its own qualified targets.
		// Resolve the lexer-folded reference against the UNFOLDED schema (#731).
		// Carry col.Name into assignments so the writer reads the key actually
		// updated (#881); never write a second folded key beside the stored name.
		// Use batch.ResolveSchemaIndex, not a lowercase map, so delimited names
		// must resolve under their own bytes and missing targets raise 42703.
		// See docs/internals/update-set-target-identity.md for the design.
		name := strings.TrimSpace(sc.Column)
		idx := batch.ResolveSchemaIndex(schema, name)
		if idx < 0 {
			// The RELATION is named, not the alias: PostgreSQL reports
			// `column "nosuch" of relation "pr" does not exist` for
			// `UPDATE pr AS a SET nosuch = 1` (verified on 17.11).
			return nil, sqlerr.New("42703", "column %q of relation %q does not exist", name, target.Table)
		}
		col := schema[idx]

		// COMPLETE, for the reason BuildDMLPredicate gives: a SET value that
		// parses to a prefix stored the prefix's value and reported success.
		node, err := plansql.ParseExpressionComplete(sc.Value)
		if err != nil {
			return nil, sqlerr.Wrap("42601", fmt.Errorf("SET %s: parsing %q: %w", name, sc.Value, err))
		}
		if src := assignSourceOf(node, schema); src.isLiteral {
			if err := src.check(col); err != nil {
				return nil, fmt.Errorf("SET %s: %w", name, err)
			}
			v, err := src.assign(nil, col)
			if err != nil {
				return nil, fmt.Errorf("SET %s: %w", name, err)
			}
			out = append(out, DMLAssignment{Column: col.Name, col: col, constant: v})
			continue
		}
		if err := checkDMLColumns(node, target, schema); err != nil {
			return nil, fmt.Errorf("SET %s: %w", name, err)
		}
		if err := dmlExpressionTyping(node, target.Alias, schema); err != nil {
			return nil, fmt.Errorf("SET %s: %w", name, err)
		}
		// The one assignment table, asked once per clause before any row —
		// which is also PostgreSQL's order: a type mismatch refuses the
		// statement even when no row matches.
		src := assignSourceOf(node, schema)
		if err := src.check(col); err != nil {
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
		out = append(out, DMLAssignment{Column: col.Name, col: col, expr: compiled, src: src})
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
			v, err := a.src.assign(a.expr.Eval(b, int(idx)), a.col)
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

// MatchDMLRows is the ONLY caller of a DMLPredicate and returns matched
// positions after excluding this file's deleted rows (#674).
// Passing deleted is mandatory; nil means this file has no delete markers.
// Check visibility before evaluating even a nil/all-rows predicate.
// Recover FatalEvalPanic once per file scan into a SQLSTATE-bearing error,
// never NULL or a transport failure (#677, ADR-0019).
// The boundary owns no locks/channels/reservations, so returning the error
// fully discharges its obligations (ADR-0019 §2a).
// See docs/internals/dml-row-visibility-and-panic-boundary.md for the design.
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
	return columnChecked(convertValue(s, col.Type))(col)
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
	v, err := convertUnquoted(s, col.Type)
	if err != nil && sqlerr.StateOf(err) == "" {
		// This door has no second reading to fall back to — `assignLiteralToColumn`
		// has one and that is why convertUnquoted leaves a SYNTAX miss
		// unclassified — so an unclassified refusal would cross the wire as the
		// blanket class. A COPY field that names no value of the column's type
		// is 22P02, like every other door (review NT, the COPY column of the
		// grammar census).
		return nil, sqlerr.New("22P02", "invalid input syntax for type %s: %s",
			parquet.NetworkTextTypeName(col.Type), sqlerr.Quote(s))
	}
	return columnChecked(v, err)(col)
}

// columnChecked is the shared tail of the two converters: a value whose
// admissibility depends on the column's DECLARATION rather than its type alone
// is judged HERE, before the caller commits anything destructive.
//
// Two rules live here because two declarations carry a bound the TypeID does
// not: a DECIMAL's (p, s) and a VECTOR's dimension. It was named
// decimalChecked while it had one.
func columnChecked(v any, err error) func(parquet.Column) (any, error) {
	return func(col parquet.Column) (any, error) {
		if err != nil || v == nil {
			return v, err
		}
		switch col.Type {
		case parquet.TypeDecimal:
			if _, derr := parquet.DecimalValueFromBox(v, col.Precision, col.Scale); derr != nil {
				return nil, derr
			}
		case parquet.TypeVector:
			// A VECTOR(N) value has exactly N components — the same rule
			// batch.Vector.SetVector enforces on the in-memory write path
			// (#900), raised with the SAME error type so the door and the
			// vector cannot answer differently. Before this the door had NO
			// vector rule at all: convertValue's default arm handed the
			// literal's raw TEXT through, the writer wrote those bytes into a
			// FIXED_LEN_BYTE_ARRAY(N*4) leaf, and the table became
			// unreadable — for a RIGHT-width literal as much as a wrong one.
			if col.Dimension <= 0 {
				return nil, sqlerr.New("22000",
					"column %q declares VECTOR with no dimension, which has no fixed width", col.Name)
			}
			vec, ok := v.([]float32)
			if !ok {
				return nil, sqlerr.New("22P02",
					"invalid input syntax for type vector: %s", sqlerr.Quote(fmt.Sprint(v)))
			}
			if len(vec) != col.Dimension {
				return nil, &batch.VectorWidthError{Dim: col.Dimension, Got: len(vec)}
			}
		}
		return v, nil
	}
}

// convertValue decodes literal text, then convertUnquoted selects its type.
// BYTES passes raw string bytes; network/UUID text uses the authoritative
// writer conversion. DECIMAL stays exact TEXT until declared (p,s) is known;
// an integer box would mean an unscaled carrier (ADR-0018 §4; #647).
// The checked leaf converter rounds scale and refuses 22003/22P02, never
// wraps or substitutes zero (ADR-0012).
// PORT/PROTOCOL parse and range-check integers; temporal types use shared
// parsers, DURATION as nanoseconds. ARRAY/ROW/MAP lack this literal path.
// VECTOR parses quoted [1,2] text; columnChecked enforces declared width.
// See docs/internals/dml-literal-conversion-type-boundary.md for the design.
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
	case parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeMAC,
		parquet.TypeUUID, parquet.TypePort, parquet.TypeProtocol:
		// The TYPE's own grammar decides what whitespace means: macaddr_in
		// skips it and inet/uuid refuse it. Trimming here made this ONE door
		// take `' 10.0.0.1'` and `'<uuid> '`, which PostgreSQL refuses and
		// every other door in this engine refuses (review NT B1).
	default:
		s = strings.TrimSpace(s)
	}

	switch typ {
	case parquet.TypeBool:
		// PostgreSQL's boolean INPUT function (boolin): t/true/y/yes/on/1 and
		// their negations, any unique prefix, case- and space-insensitive —
		// the reading a truth context already gives a quoted literal. It was
		// strconv.ParseBool, which refused `'yes'`, `'no'` and `'off'` with no
		// SQLSTATE (round-3 review P2) and took `'T'`/`'F'` spellings boolin
		// does not share with it.
		if v, ok := plansql.ParseBoolText(s); ok {
			return v, nil
		}
		return nil, sqlerr.New("22P02", "invalid input syntax for type boolean: %s", sqlerr.Quote(s))
	case parquet.TypeInt32:
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, err
		}
		return int32(v), nil
	case parquet.TypeInt64:
		return strconv.ParseInt(s, 10, 64)
	case parquet.TypeFloat32, parquet.TypeFloat64:
		// float4in / float8in's two refusals, classified: text naming no
		// number is 22P02 and a magnitude the type cannot hold 22003. The raw
		// strconv error crossed every door with no SQLSTATE (`'yes'` into a
		// double precision column; arc VL round 4's door-diff table).
		bits := 64
		if typ == parquet.TypeFloat32 {
			bits = 32
		}
		v, err := strconv.ParseFloat(s, bits)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return nil, sqlerr.New("22003", "%s is out of range for type %s", sqlerr.Quote(s), pgOperandTypeName(typ))
			}
			return nil, sqlerr.New("22P02", "invalid input syntax for type %s: %s", pgOperandTypeName(typ), sqlerr.Quote(s))
		}
		if bits == 32 {
			return float32(v), nil
		}
		return v, nil
	case parquet.TypeString:
		return s, nil
	case parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeMAC, parquet.TypeUUID:
		// The five text-formed network types, validated HERE rather than left
		// to the writer, because the writer's `""` means ABSENCE — the empty
		// CSV or JSON field — while a SQL literal `''` is a VALUE the type
		// cannot read (`''::inet` is 22P02 on 17.11). Without this arm
		// `INSERT INTO t (ip) VALUES ('')` stored a NULL nobody wrote.
		// The TEXT is handed on, not the parsed box: an IPV4/IPV6/CIDR/MAC/
		// UUID column takes its text through the ingest boundary and the
		// writer converts it there, once.
		if _, st, _ := parquet.NetworkTextValue(typ, s); st != parquet.NetTextOK {
			return nil, parquet.NetworkTextError(typ, s, st)
		}
		return s, nil
	case parquet.TypePort, parquet.TypeProtocol:
		// The type's OWN text form, read by the one grammar every boundary
		// reads (#986): a decimal number in the type's range, and for
		// PROTOCOL the IANA name `protocol_name()` prints. Three copies of
		// this bound existed and one door had no bound at all, so an
		// out-of-range literal read back verbatim — a value no real port or
		// protocol number can be.
		//
		// A RANGE failure is final and carries its class; a SYNTAX one is not,
		// because the assignment CAST above this still has a reading for a
		// fractional literal (`VALUES (2.5)` into an int4-backed column rounds
		// on the server, #699) and must get its turn.
		n, st, _ := parquet.NetworkTextValue(typ, s)
		switch st {
		case parquet.NetTextOK:
			return n, nil
		case parquet.NetTextRange:
			return nil, parquet.NetworkTextError(typ, s, st)
		}
		return nil, fmt.Errorf("cannot parse %s value %q", typ, s)
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
	case parquet.TypeVector:
		// pgvector's text form, which is the spelling a client already knows
		// and the one docs/data-types.md documents. The WIDTH is not checked
		// here — this function is handed a TypeID and nothing else, so it
		// cannot know N; columnChecked does it where the declaration is.
		return parseVectorLiteral(s)
	default:
		return s, nil
	}
}

// parseVectorLiteral is batch.ParseVectorText, pgvector's vector_in — the
// one reader CAST(text AS VECTOR(n)) uses too (arc CW round 2).
func parseVectorLiteral(s string) ([]float32, error) {
	return batch.ParseVectorText(s)
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
	p := db.newPlanner(ctx)
	// The planner's memory tracker exists only once something has asked for
	// it, and nothing on this door ever calls Plan(). Asking here is what
	// makes SubqueryEnv's budget option non-nil, so an IN-subquery's
	// membership map is CHARGED to this statement's budget the way the query
	// path charges it (ADR-0006, #531). Without it the map was the one
	// allocation on a write door that nothing accounted for.
	p.EnsureMemoryTracker()
	_, innerCols, opts := p.SubqueryEnv(ctx)
	// THE SET IS BOUNDED HERE because nothing else bounds it. The query
	// path's `WADJET_IN_SET_MAX` guards the planner's INLINING of an IN-set
	// into filter text (physical/in_subquery_set.go); this door takes no such
	// path — `expr.InSubquery` builds a membership map from whatever the
	// runner returns — so before this line
	// `DELETE FROM t WHERE id IN (SELECT id FROM huge)` pulled every id into
	// coordinator memory with no budget and no cap, on a WRITE door, where
	// the arc's own base had no set at all.
	//
	// On the CONSTRUCT and not in the runner: a runner sees SQL text and
	// cannot tell whether IN, EXISTS or a scalar comparison asked for it, so
	// bounding there refused `DELETE ... WHERE EXISTS (SELECT 1 FROM big)` —
	// a question one row answers — and reported a multi-row scalar subquery
	// as 54000 where this engine's own rule is 21000.
	opts = append(opts, expr.WithSetRowBound(physical.MaxInlinedInSetRows()))
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
//
// What it does NOT do is decide how many rows a subquery may return. That is
// the CONSTRUCT's question — IN wants a set and is bounded, EXISTS wants a row
// and a scalar subquery is an error past one — and this function cannot see
// which one asked (dmlSubqueryEnv, expr.WithSetRowBound).
func (db *DB) dmlSubqueryRunner(ctx context.Context) expr.SubqueryRunner {
	return func(sql string) ([]map[string]any, error) {
		res, err := db.Query(ctx, sql)
		if err != nil {
			return nil, err
		}
		return res.Rows, nil
	}
}

// dmlScanError names the file a DML statement could not read — except when the
// failure is an AUTHORIZATION refusal, which is not about the file.
//
// A DML predicate is COMPILED, not planned (ADR-0031), so a subquery inside it
// is evaluated while the statement scans, and a relation the identity may not
// read refuses THERE. Wrapped, that refusal reached the client as
// `scanning file tables/t/chunk_<uuid>.parquet: permission denied for table
// "other"` — the shared decision's text behind a storage path, which is both a
// second wording for one refusal (ADR-0034 item 6) and an internal object key
// handed to a caller who has just been told they may not read the data (#945).
func dmlScanError(path string, err error) error {
	if sqlerr.StateOf(err) == "42501" {
		return err
	}
	return fmt.Errorf("scanning file %s: %w", path, err)
}
