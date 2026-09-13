package wadjet

import (
	"context"
	"errors"
	"fmt"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// CommandCreateTableAs is the verb an ExecResult carries for a
// `CREATE TABLE … AS SELECT` that did NOT run its query — `WITH NO DATA`, or
// an `IF NOT EXISTS` over a name that already exists.
//
// PostgreSQL 17.11 sends exactly two tags for this statement and the split is
// the query, not the DDL: it is `SELECT <n>` when the query ran and wrote n
// rows, and the bare `CREATE TABLE AS` when it did not. Measured on both
// shapes.
const CommandCreateTableAs = "CREATE TABLE AS"

// executeCreateTableAs runs `CREATE TABLE [IF NOT EXISTS] t [(a, b)] AS
// <select> [WITH [NO] DATA]` (#1024).
//
// The order of the steps is PostgreSQL's, and it is observable:
//
//  1. the write permission and the relation decision on the NEW name;
//  2. the existence test — 42P07, or the `IF NOT EXISTS` no-op, BEFORE the
//     query runs. PostgreSQL's `CreateTableAsRelExists` is likewise ahead of
//     execution, so a CTAS onto an existing name costs nothing and has no
//     chance to fail halfway;
//  3. the query, through the ORDINARY planner — the same plan, optimizer,
//     security projection and execution arm the identical bare SELECT takes;
//  4. the schema, from that plan's declared output;
//  5. the write, which creates the catalog entry HOLDING the rows.
//
// A failure at any step leaves no table and reclaims whatever was written
// (ingest.WriteQueryRows).
func (db *DB) executeCreateTableAs(ctx context.Context, ct *plansql.CreateTableInfo) (*ExecResult, error) {
	if err := auth.RequirePermission(db.authProvider, ctx, "write"); err != nil {
		return nil, err
	}
	// The decision about THIS relation, which plain `CREATE TABLE` does not
	// make: CREATE has no existing relation to decide about, so it asks the
	// schema-level permission alone (grpc.go's CreateTable records the line
	// PostgreSQL draws). A CTAS is not in that position — it is a WRITE whose
	// content comes from relations this identity is being policed on, and the
	// name it mints is a relation a policy may already scope. Asking
	// TableAccess here costs nothing where no policy names the name and
	// refuses where one does.
	if err := auth.TableAccess(ctx, db.authProvider, ct.Name, auth.ActionWrite); err != nil {
		return nil, err
	}

	exists, err := db.tableExists(ctx, ct.Name)
	if err != nil {
		return nil, err
	}
	if exists {
		if ct.IfNotExists {
			// PostgreSQL emits a NOTICE and the `CREATE TABLE AS` tag, and it
			// does NOT run the query. Wadjet has no NOTICE channel on every
			// door, so the tag is the whole answer — which is what a client
			// branches on either way.
			return &ExecResult{Command: CommandCreateTableAs}, nil
		}
		return nil, sqlerr.Wrap("42P07", fmt.Errorf("relation %q already exists", ct.Name))
	}

	if !ct.WithData {
		// `WITH NO DATA` creates the table with the declared schema and does
		// not execute the query — PostgreSQL's rule, and the point of the
		// clause. The declaration comes from the PLAN, which is the same walk
		// the executed pipeline stamps as its output schema.
		declared, err := db.declaredOutputFor(ctx, ct.AsSelect)
		if err != nil {
			return nil, err
		}
		schema, err := db.ctasSchema(ct, declared)
		if err != nil {
			return nil, err
		}
		if err := db.catalog.CreateTable(ctx, ct.Name, schema, nil); err != nil {
			return nil, err
		}
		return &ExecResult{Command: CommandCreateTableAs}, nil
	}

	res, err := db.Query(ctx, ct.AsSelect.SQL)
	if err != nil {
		return nil, querySourceError(err)
	}
	schema, err := db.ctasSchema(ct, res.OutputSchema)
	if err != nil {
		return nil, err
	}

	n, err := ingest.WriteQueryRows(ctx, db.catalog, ingest.QueryWrite{
		Table:   ct.Name,
		Schema:  schema,
		Columns: schema.ColumnNames(),
		Create:  true,
	}, resultCells(res))
	if err != nil {
		return nil, err
	}
	// `SELECT <n>`: the tag PostgreSQL sends for a CTAS that ran its query.
	return &ExecResult{Command: "SELECT", RowsAffected: n}, nil
}

// ctasSchema is the ONE derivation of a new table's schema from a query's
// declared output, shared by the WITH DATA and WITH NO DATA arms so the two
// cannot declare the same statement differently.
func (db *DB) ctasSchema(ct *plansql.CreateTableInfo, declared []parquet.Column) (parquet.Schema, error) {
	schema, err := ingest.TableSchemaForQuery(declared, ct.AsColumnNames)
	if err != nil {
		return parquet.Schema{}, err
	}
	// The SQL door of the reserved namespace, the same check the declared
	// `CREATE TABLE` form takes (see DB.CreateTable): a query may publish a
	// column named like a planner slot, and this is where the name is being
	// CREATED.
	if err := refuseReservedSchemaNames(schema, "column of new table "+ct.Name); err != nil {
		return parquet.Schema{}, err
	}
	return schema, nil
}

// executeInsertSelect runs `INSERT INTO t [(a, b)] <select>` (#1024).
//
// The mapping is POSITIONAL and the check is made before a single row is read:
// item j of the query feeds target column j of the list (or of the table's own
// order), an item count above the target's is 42601, and a pair the writer
// cannot store is 42804. PostgreSQL's arity messages are reproduced verbatim —
// `INSERT has more expressions than target columns` and its mirror — because
// they are what a client's error handling has been reading since 7.x.
func (db *DB) executeInsertSelect(ctx context.Context, info *plansql.InsertInfo) (*ExecResult, error) {
	// A DML statement REFERENCES an existing table, so it takes the READ
	// concession a SELECT does (see executeInsert).
	info.Table = db.catalog.ResolveTableName(info.Table)
	tableMeta, err := db.catalog.GetTable(ctx, info.Table)
	if err != nil {
		return nil, db.dmlRelationError(info.Table, err)
	}
	columns, cols, err := resolveInsertColumns(info.Columns, info.Table, tableMeta.Schema.Columns)
	if err != nil {
		return nil, err
	}
	// The incarnation the STATEMENT read, taken here rather than let the
	// ingester bind its own at the first buffered row: a DROP+CREATE landing
	// between this lookup and that first row would bind the NEW table and
	// write these rows into it (#919, ADR-0030).
	incarnation, err := db.catalog.TableIncarnation(ctx, info.Table)
	if err != nil && !errors.Is(err, catalog.ErrTableNotFound) {
		return nil, err
	}

	res, err := db.Query(ctx, info.Select.SQL)
	if err != nil {
		return nil, querySourceError(err)
	}

	if err := checkInsertSelectShape(res.OutputSchema, cols, len(info.Columns) > 0); err != nil {
		return nil, err
	}

	n, err := ingest.WriteQueryRows(ctx, db.catalog, ingest.QueryWrite{
		Table:         info.Table,
		Schema:        tableMeta.Schema,
		Columns:       columns[:len(res.OutputSchema)],
		PartitionKeys: tableMeta.PartitionKeys,
		Incarnation:   incarnation,
	}, resultCells(res))
	if err != nil {
		return nil, err
	}
	return &ExecResult{RowsAffected: n, Command: "INSERT"}, nil
}

// checkInsertSelectShape compares a query's declared output with the target
// columns it feeds, in PostgreSQL's order: arity first, then type per
// position.
//
// A query with FEWER items than the target has columns is ACCEPTED when the
// statement wrote NO column list — the columns it does not reach take NULL,
// which is what PostgreSQL 17.11 does (measured: `INSERT INTO t(id,n,s) SELECT
// id, n FROM src` is `INSERT 0 3` with `s` NULL). With an EXPLICIT list the
// same shortfall is 42601 there, under its own message, because the list is a
// promise about how many values follow.
func checkInsertSelectShape(declared []parquet.Column, cols []parquet.Column, explicitList bool) error {
	switch {
	case len(declared) > len(cols):
		return sqlerr.New("42601", "INSERT has more expressions than target columns")
	case len(declared) < len(cols) && explicitList:
		return sqlerr.New("42601", "INSERT has more target columns than expressions")
	}
	for i, d := range declared {
		if err := ingest.AssignableToColumn(d, cols[i]); err != nil {
			return err
		}
	}
	return nil
}

// resultCells boxes a result POSITIONALLY, one []any per row aligned with the
// declared output.
//
// QueryResult.Rows is a map and a result may legally publish two columns of
// one name, so the map form cannot carry the answer; Cells is the accessor
// that is right either way.
func resultCells(res *QueryResult) [][]any {
	if res == nil {
		return nil
	}
	n := len(res.Rows)
	if len(res.RowValues) > n {
		n = len(res.RowValues)
	}
	out := make([][]any, n)
	for i := 0; i < n; i++ {
		out[i] = res.Cells(i)
	}
	return out
}

// tableExists reports whether the catalog holds this exact name.
//
// BYTE-EXACT, with no `ResolveTableName` concession: minting a name is not
// referencing one (the rule DB.CreateTable already keeps), so
// `CREATE TABLE "Foo" AS …` beside an existing `foo` creates a second
// relation here exactly as it does through the declared form.
func (db *DB) tableExists(ctx context.Context, name string) (bool, error) {
	_, err := db.catalog.GetTable(ctx, name)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, catalog.ErrTableNotFound):
		return false, nil
	default:
		return false, fmt.Errorf("looking up table %q: %w", name, err)
	}
}

// declaredOutputFor is a statement's output columns WITHOUT running it.
//
// `WITH NO DATA` is the caller: PostgreSQL creates the table with the query's
// declared schema and never executes the query, so an expression that would
// fail on row five — a division by zero, a cast a value cannot take — does not
// fail the statement there and must not fail it here. Running the query and
// discarding its rows would be a different statement.
//
// It is the same build-enforce-optimize sequence DB.Query runs, ending in
// Planner.DeclaredOutputSchema — the walk whose answer Plan stamps on a
// single-process pipeline as Plan.OutputSchema, and the one the async door
// already describes a zero-row result from (#1008). The enforcement runs
// BEFORE the declaration is taken, so the columns declared are the columns
// this identity may see (ADR-0034).
func (db *DB) declaredOutputFor(ctx context.Context, parsed *plansql.ParsedQuery) ([]parquet.Column, error) {
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil, fmt.Errorf("extracting SELECT: %w", err)
	}
	planner := db.newPlanner(ctx)
	if err := auth.ValidateStatementColumns(ctx, db.authProvider, db.catalog, selectInfo, "embedded"); err != nil {
		return nil, err
	}
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		return nil, fmt.Errorf("building logical plan: %w", err)
	}
	planner.AnnotateScanColumns(ctx, logicalPlan)
	ctx, logicalPlan, err = db.enforceAccessPolicies(ctx, selectInfo, logicalPlan)
	if err != nil {
		return nil, err
	}
	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		planner.AnnotateScanColumns(ctx, plan)
	})
	logicalPlan, err = auth.EnforceOptimizedPlan(ctx, db.catalog, logicalPlan)
	if err != nil {
		return nil, err
	}
	return planner.DeclaredOutputSchema(logicalPlan), nil
}

// execAsQueryResult runs a WRITE through the one DML entry point and boxes its
// answer as the single-cell result this door has always returned for a
// statement that produces no rows.
//
// The cell is now the COMMAND TAG — `INSERT 0 3`, `SELECT 5`, `CREATE TABLE
// AS` — and not `fmt.Sprintf("%s %d", …)`, which is the renderer
// ExecResult.Tag exists to be the only copy of. The inline form here rendered
// `INSERT 3` while the identical statement over pgwire and REST rendered
// `INSERT 0 3`, so the embedded API and the gRPC Query RPC (which comes
// through here) disagreed with the other two doors about PostgreSQL's oid
// field.
func (db *DB) execAsQueryResult(ctx context.Context, parsed *plansql.ParsedQuery) (*QueryResult, error) {
	result, err := db.ExecuteParsed(ctx, parsed)
	if err != nil {
		return nil, err
	}
	return &QueryResult{
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": result.Tag()}},
	}, nil
}

// querySourceError classifies a failure of the QUERY a write took its rows
// from.
//
// One class is not the query's own: the result budget. This first version
// gathers the whole result in the process that runs the statement and writes
// it from there (#1024 — the per-worker parallel write is the follow-up), so a
// result past that budget is a RESOURCE refusal and not a statement that means
// nothing. It carries 53400, which is the class this engine already gives a
// configured limit it will not exceed (physical.QueryLimitSQLState), and the
// message says which statement could not be completed. Without it the refusal
// reached the wire as the blanket 42000 — "your SQL is malformed" for a
// statement that is not.
func querySourceError(err error) error {
	if errors.Is(err, exec.ErrCollectBudget) {
		return sqlerr.Wrap(physical.QueryLimitSQLState, fmt.Errorf(
			"the query's result does not fit this statement's result budget, "+
				"and a write whose source is a query gathers the whole result "+
				"before it writes: %w", err))
	}
	return err
}
