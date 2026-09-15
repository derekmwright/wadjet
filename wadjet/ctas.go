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

	res, err := db.query(ctx, ct.AsSelect.SQL, db.querySourcedWriteBudget())
	if err != nil {
		return nil, querySourceError(err, db.querySourcedWriteBudget())
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
	}, resultRows(res, nil, nil))
	if err != nil {
		return nil, err
	}
	// `SELECT <n>`: the tag PostgreSQL sends for a CTAS that ran its query.
	return &ExecResult{Command: "SELECT", RowsAffected: n}, nil
}

// ctasSchema is the last step both arms share: the rename list, the reserved
// namespace and the writer's rules, applied to a declared output the two arms
// have already agreed on.
//
// It cannot make them agree — a name that differs by the time it gets here is a
// name it renames or refuses, not one it repairs. The agreement is made
// upstream, in `declaredOutputFor`, which takes the same published NAMES the
// executed arm's sink applies — `physical.PublishedOutputNames`, a walk over
// the logical plan — and its TYPES from `Planner.DeclaredOutputSchema`. Four
// rounds of review were spent on those two lists: on a name taken from the type
// walk (round-1 B7, round-2 B1), on reading the names from the wrong place —
// asking `Plan` for the sink itself RUNS every CTE body and every hash join's
// build side, on the arm documented not to execute the query (round-3 B1) — and
// on a type the walk could not resolve because only `Plan` seeded the WITH list
// (round-4 B1).
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

	res, err := db.query(ctx, info.Select.SQL, db.querySourcedWriteBudget())
	if err != nil {
		return nil, querySourceError(err, db.querySourcedWriteBudget())
	}

	if err := checkInsertSelectShape(res.OutputSchema, cols, len(info.Columns) > 0,
		unknownTypedSelectItems(info.Select, len(res.OutputSchema))); err != nil {
		return nil, err
	}

	n, err := ingest.WriteQueryRows(ctx, db.catalog, ingest.QueryWrite{
		Table:         info.Table,
		Schema:        tableMeta.Schema,
		Columns:       columns[:len(res.OutputSchema)],
		PartitionKeys: tableMeta.PartitionKeys,
		Incarnation:   incarnation,
	}, resultRows(res, res.OutputSchema, cols))
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
func checkInsertSelectShape(declared []parquet.Column, cols []parquet.Column, explicitList bool,
	unknownLit []ntUnknownKind) error {
	switch {
	case len(declared) > len(cols):
		return sqlerr.New("42601", "INSERT has more expressions than target columns")
	case len(declared) < len(cols) && explicitList:
		return sqlerr.New("42601", "INSERT has more target columns than expressions")
	}
	for i, d := range declared {
		if i < len(unknownLit) && unknownLit[i] != ntNotUnknown {
			// SQL's `unknown`, typed FROM the target rather than compared
			// against it (#1088). A NULL literal is unknown-typed too and
			// needs no grammar at all — it produces no value — so it is
			// assignable to EVERY declaration, which is what PostgreSQL does
			// with `INSERT INTO t (c) SELECT NULL` (review NT N1).
			if unknownLit[i] == ntUnknownNull {
				continue
			}
			if err := ingest.AssignableFromUnknownLiteral(cols[i]); err != nil {
				return err
			}
			continue
		}
		if err := ingest.AssignableToColumn(d, cols[i]); err != nil {
			return err
		}
	}
	return nil
}

// unknownTypedSelectItems marks the select-list positions written as a BARE
// QUOTED LITERAL — SQL's `unknown`, which PostgreSQL types from the INSERT's
// target column rather than from itself.
//
// It answers only for the shape it can PROVE: a single SELECT block whose item
// count matches the declared output, with no star and no set operation. A
// literal reached through a UNION, a CTE or a derived table is typed by that
// construct's own fold before it ever meets the target, and guessing here
// would put a wrong rule on it — so those positions stay false and keep the
// 42804 they have today (the honest answer: this walk cannot see them).
func unknownTypedSelectItems(q *plansql.ParsedQuery, n int) []ntUnknownKind {
	if q == nil || n == 0 {
		return nil
	}
	info, err := plansql.ExtractSelect(q)
	if err != nil || info == nil || info.Union != nil || len(info.Columns) != n {
		return nil
	}
	out := make([]ntUnknownKind, n)
	for i, c := range info.Columns {
		if c.Star {
			return nil
		}
		// PARENTHESES carry no meaning past grouping, so `SELECT ('10.0.0.1')`
		// is the same item as `SELECT '10.0.0.1'` and was 42804 while its twin
		// inserted a row (review NT N1) — the same rule physical.unwrapParens
		// states for the refusal side.
		e := c.ASTExpr
		for {
			pn, ok := e.(*plansql.ParenNode)
			if !ok || pn.Inner == nil {
				break
			}
			e = pn.Inner
		}
		lit, ok := e.(*plansql.Lit)
		switch {
		case ok && lit.Kind == plansql.LitString:
			out[i] = ntUnknownText
		case ok && lit.Kind == plansql.LitNull:
			out[i] = ntUnknownNull
		}
	}
	return out
}

// ntUnknownKind classifies a select-list item as SQL's `unknown`: a bare
// quoted literal, whose TEXT the target's input function reads, or a NULL,
// which produces no value and so needs no grammar.
type ntUnknownKind int

const (
	ntNotUnknown ntUnknownKind = iota
	ntUnknownText
	ntUnknownNull
)

// resultRows reads a result POSITIONALLY, one row at a time, converting each
// row to the target's declared types as it goes.
//
// One row at a time and not a materialized `[][]any`: the result is already
// held twice by the time it gets here (the collected batches, and the
// name-keyed row maps `DB.Query` boxes them into), and a third full copy is
// the allocation pattern CollectSink's own comment records as having held
// 21 GB of live heap at SF10 Q18. Cells is the accessor that is right whether
// or not two output columns share a name, which the map form is not.
//
// `declared` and `target`, when given, are the APPEND's two type lists and
// every cell goes through the engine's one assignment conversion between them
// — see assignQueryCells. A CREATE passes neither: its target columns ARE the
// query's declared output, so there is nothing to convert.
func resultRows(res *QueryResult, declared, target []parquet.Column) ingest.RowSource {
	if res == nil {
		return ingest.RowSource{}
	}
	n := len(res.Rows)
	if len(res.RowValues) > n {
		n = len(res.RowValues)
	}
	return ingest.RowSource{N: n, At: func(i int) ([]any, error) {
		cells := res.Cells(i)
		if target == nil {
			return cells, nil
		}
		return assignQueryCells(cells, declared, target)
	}}
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
	declared := planner.DeclaredOutputSchema(logicalPlan)

	// The NAMES come from the one rule, not from this walk.
	//
	// `Planner.DeclaredOutputSchema` names an unaliased output column by its
	// expression TEXT, because its caller — the async door describing a
	// zero-row result — asks it for TYPES. The executed plan publishes
	// PostgreSQL's name instead: `?column?` for an operator expression or a
	// literal, the function's own name for a call (#732,
	// `plansql.OutputColumnName`). Left alone, the two arms of one statement
	// declared two different tables: `WITH DATA` gave `?column?` and
	// `WITH NO DATA` gave `"n + 1"`, and with it went the duplicate rule —
	// `SELECT id, n+1, s||'x', 42 … WITH NO DATA` was CREATED where the same
	// statement `WITH DATA` and PostgreSQL 17.11 both answer 42701 (measured;
	// round-2 review B7).
	//
	// `deriveColumns` is that rule, and it is the SAME call `DB.Query` makes
	// on the executed plan — asked here with no rows, which is exactly what
	// this arm has. A star falls through it to the plan's own schema names,
	// which is what the executed arm publishes for a star too.
	if names := deriveColumns(selectInfo, nil, declared); len(names) == len(declared) {
		for i := range declared {
			declared[i].Name = names[i]
		}
	}

	// A STAR lists no items, so the loop above renamed nothing and the names
	// are still the plan-time walk's — which spells an INNER unaliased
	// projection item by its expression TEXT. The comment this replaces said a
	// star "falls through to the plan's own schema names, which is what the
	// executed arm publishes for a star too"; it is not. The executed arm
	// publishes the SINK's names, and for `SELECT * FROM (SELECT id, n + 1
	// FROM s) x` those are `id, ?column?` where the walk says `id, "n + 1"`
	// (#732; round-2 review B1). The duplicate rule goes with it: the same
	// star over `(SELECT n+1, n+2 …)` is 42701 on the executed arm and on
	// PostgreSQL 17.11, and was CREATED here.
	//
	// The sink's names are a walk over the LOGICAL plan —
	// `publishedOutputNames(findOutputProjectionNode(…))`, the same list `Plan`
	// stamps on the sink as `CollectSink.OutputNames` — and
	// `physical.PublishedOutputNames` exports it.
	//
	// Asking `Plan` for them instead, which is what round 3 did, READS THE
	// TABLE: `Plan` materializes every CTE body by RUNNING a pipeline
	// (`materializeCTEs`) and builds every hash join's build side, so a
	// statement documented not to execute the query executed the part of it
	// that costs the most — measured at 2 data objects on five of nine shapes,
	// 58 ms at 400k rows, 123 MiB peak (round-3 review B1). The clause exists
	// so that a query which would fail on row five still declares its table;
	// a declaration that reads the rows is not that clause.
	//
	// An empty entry means "this position publishes what it always did", so
	// only the named positions are renamed.
	if names := physical.PublishedOutputNames(logicalPlan); len(names) == len(declared) {
		for i := range declared {
			if names[i] != "" {
				declared[i].Name = names[i]
			}
		}
	}
	return declared, nil
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

// DefaultQuerySourcedWriteBytes bounds the RESULT a `CREATE TABLE … AS SELECT`
// or an `INSERT INTO … SELECT` gathers before it writes.
//
// This first version reads the whole result on the process running the
// statement and writes it from there (#1024; ADR-0036 records the streaming
// writer as the named next step), so without a bound a large query is bounded
// by nothing but the heap — measured at 860 MiB peak for a 99 MiB result, and
// no refusal. The bound makes the refusal the docs promise reachable, and it is
// the SAME field and the SAME class the coordinator's local fast path already
// uses for a gathered result.
//
// 64 MiB is `DefaultLocalFastPathBytes`, which is this engine's existing answer
// to "how much result may one process gather", so a write does not get a second
// number. `Config.MemoryBudget`, when set, replaces it: an embedder that has
// told the engine what it may use has already answered the question.
const DefaultQuerySourcedWriteBytes = 64 << 20

// querySourcedWriteBudget is the bound this DB applies to a query-sourced
// write's gather.
func (db *DB) querySourcedWriteBudget() int64 {
	if db.memoryBudget > 0 {
		return db.memoryBudget
	}
	return DefaultQuerySourcedWriteBytes
}

// querySourceError classifies a failure of the QUERY a write took its rows
// from.
//
// One class is not the query's own: the result budget. A statement whose source
// is a query gathers the whole result before it writes, so a result past that
// budget is a RESOURCE refusal and not a statement that means nothing. It
// carries 53400, the class this engine already gives a configured limit it will
// not exceed (physical.QueryLimitSQLState), and the message names the bound and
// how to raise it. Without it the refusal reached the wire as the blanket
// 42000 — "your SQL is malformed" for a statement that is not.
func querySourceError(err error, budget int64) error {
	if errors.Is(err, exec.ErrCollectBudget) {
		return sqlerr.Wrap(physical.QueryLimitSQLState, fmt.Errorf(
			"the query's result exceeds the %d-byte budget a write whose source is a "+
				"query may gather before it writes; narrow the query, or raise "+
				"Config.MemoryBudget: %w", budget, err))
	}
	return err
}

// assignQueryCells applies the engine's ONE assignment conversion to every cell
// of ONE row an `INSERT INTO … SELECT` writes — `assignEvaluatedValue`, the converter
// `INSERT … VALUES` and `UPDATE … SET` have used since #647/#678
// (docs/internals/dml-evaluated-assignment-value-domain.md).
//
// It is the difference between a value and a CARRIER. Without it the query's
// boxes reach the writer raw, and `parquet.DecimalValueFromBox` reads an
// integer box as the already-UNSCALED carrier (ADR-0018 §4): a BIGINT 5 into a
// DECIMAL(18,4) column stored 0.0005 where PostgreSQL 17.11 and this engine's
// own VALUES door store 5.0000 (measured, both). That is the exact hazard
// `ingest.AssignableToColumn`'s comment refuses DECIMAL→DECIMAL for, and its
// integer arm has no scale either.
//
// It is also the only place that narrows PORT to uint16 and PROTOCOL to uint8.
// The writer's leaf check range-checks an int32 carrier, not the stored width,
// so `INSERT INTO t (p) SELECT i * 100000` stored PORT 500000 — a value no port
// number can be — where the literal door (#814) and the computed door both
// answer 22003. One converter, one answer, every door.
//
// The conversion belongs HERE and not at the writer, which is what ADR-0036
// rejected: this is the one place that holds BOTH facts, the source's declared
// type (the plan's output schema) and the target's (the catalog).
func assignQueryCells(row []any, declared, target []parquet.Column) ([]any, error) {
	for j := range row {
		if j >= len(target) || j >= len(declared) {
			break
		}
		// srcFloat tells the integer converter whether a fractional source
		// rounds (a float does, PostgreSQL's float→int assignment cast) or
		// refuses; the declared output is where that fact lives.
		srcFloat := declared[j].Type == parquet.TypeFloat32 || declared[j].Type == parquet.TypeFloat64
		v, err := assignEvaluatedValue(row[j], target[j], srcFloat)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", target[j].Name, err)
		}
		row[j] = v
	}
	return row, nil
}
