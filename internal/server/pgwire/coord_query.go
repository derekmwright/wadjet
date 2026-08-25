package pgwire

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// shouldRouteThroughCoord reports whether a SQL statement should go through
// coord.ExecuteSQL (the native-DAG executor). True for SELECT/WITH; false for
// DDL, DML, DESCRIBE, EXPLAIN, SET, BEGIN, etc., which the coord doesn't
// handle today and which need the legacy db.Query path.
//
// Detection is keyword-based rather than full-parse: pgwire already knows
// it's a query statement at this point, but extended-query Bind/Execute
// reuses the cached SQL across many code paths and a full Parse here would
// be wasted CPU when the legacy path is the right answer anyway.
func shouldRouteThroughCoord(sql string) bool {
	s := strings.TrimLeft(sql, " \t\n\r(")
	if len(s) < 6 {
		return false
	}
	upper := strings.ToUpper(s[:6])
	switch upper {
	case "SELECT":
		return true
	}
	// WITH ... SELECT (CTE)
	if len(s) >= 5 && strings.EqualFold(s[:5], "WITH ") {
		return true
	}
	return false
}

// canBypassDB reports whether the coord routing path is safe for this
// connection. SECURITY GATE: db.Query enforces ABAC (row filters, column
// policies, table-level access denial) via auth.EnforcePlanPolicies. The
// coordinator applies the SAME enforcement in ExecuteSQL when an auth
// provider is wired into it (Coordinator.SetAuthProvider → EnforcesABAC) —
// queryContext already attaches the connection's identity, so authed
// connections can route through the native-DAG executor and the local fast
// path. Without coordinator-side enforcement, only unauthenticated
// dev/harness connections may bypass db.Query.
func (c *pgConn) canBypassDB() bool {
	if c.authProvider == nil && c.identity == nil {
		return true
	}
	return c.coord != nil && c.coord.EnforcesABAC()
}

// queryViaCoord executes sql through coord.ExecuteSQL. The result batches
// are returned as a consuming BatchStream, NOT boxed into QueryResult.Rows
// — the send path boxes one batch (~2K rows) at a time via sendResultRows.
// Results that exceeded the coordinator gather budget arrive as a lazy
// stream replaying from local scratch, so even an over-budget result never
// materializes fully in coordinator heap. The caller owns the stream and
// must drain it or Close it (spill scratch lives until then).
//
// For Q18 SF10 (60M intermediate rows reduced to 100 by LIMIT), boxing here
// only ever sees the 100 final rows because coord's native-DAG executor
// produces the post-LIMIT batches at Gather. Legacy db.Query materialized
// the full pre-LIMIT pipeline output.
func (c *pgConn) queryViaCoord(ctx context.Context, sql string) (*wadjet.QueryResult, coordinator.BatchStream, *nestedFieldSchema, error) {
	res, err := c.coord.ExecuteSQL(ctx, sql)
	if err != nil {
		res.Close()
		return nil, nil, nil, err
	}
	// Read the schema BEFORE Stream() detaches the batches.
	metas := coordColumnMetas(res)
	nestedSchema := nestedSchemaByName(res.OutputSchema())
	return &wadjet.QueryResult{Columns: res.Columns, ColumnMetas: metas}, res.Stream(), nestedSchema, nil
}

// nestedFieldSchema is a query output column's declared ROW/ARRAY/MAP
// structure, resolved by output column NAME with an optional positional
// fallback — nestedColumnFor's two lookups.
//
// ordered is set only by the coord path (nestedSchemaByName): OutputSchema()
// and SQLResult.Columns are two views of the SAME query result and so agree
// on position (coordinator.go's SQLResult.Schema doc: "the declared type of
// each output column, in Columns order"), the exact invariant
// coordColumnMetas already relies on for ITS positional fallback. A renamed
// output column (the gather's renamer; an alias; a computed expression) can
// lose its name from byName while keeping its slot in ordered, mirroring
// coordColumnMetas' rule: "keeps its position but not its name" (#471
// resurfacing as #464/#471 fold-in review item FIX 4 — a renamed ROW column
// fell back to formatPgComposite's schema-less path, sorted-key order,
// instead of its declared field order).
//
// The legacy catalog lookup (nestedColumnSchemas, paraminfer.go) leaves
// ordered nil: its entries come from whichever catalog table columns happen
// to share a name with something in the SQL text, which has no positional
// relationship to the output column list at all — a fallback there would
// attach a random table column's schema to an unrelated output position.
type nestedFieldSchema struct {
	byName  map[string]parquet.Column
	ordered []parquet.Column
}

// nestedSchemaByName indexes a query's output schema by column name so
// formatPgValueTyped can look up a ROW/ARRAY/MAP column's declared field
// order and element type by the same name sendDataRow already keys rows on.
// Every column is kept, not just the nested-typed ones: a scalar entry is
// simply never read by formatPgValueTyped, and filtering it out here would
// just be a second pass over the same slice for no benefit.
func nestedSchemaByName(schema []parquet.Column) *nestedFieldSchema {
	if len(schema) == 0 {
		return nil
	}
	byName := make(map[string]parquet.Column, len(schema))
	for _, col := range schema {
		byName[col.Name] = col
	}
	return &nestedFieldSchema{byName: byName, ordered: schema}
}

// coordColumnMetas is the coord path's answer to wadjet.deriveColumnMetas:
// the typed column metadata sendTypedRowDescription needs, taken from the
// schema of the vectors the values were actually stored in.
//
// Without it this path declared OID 25 (text) for every column — the
// all-text fallback in sendRowDescription — while the SAME query through the
// embedded API declared real OIDs. Two entry points, two RowDescriptions,
// and a typed client (DataGrip, JDBC, pgx binary format) sees whichever one
// it happened to reach (#396's wire face; the mapping itself is #305).
//
// Nil when the schema does not cover every column: a partial declaration is
// worse than the uniform fallback, because a client cannot tell which
// columns were guessed.
func coordColumnMetas(res *coordinator.SQLResult) []wadjet.ColumnMeta {
	schema := res.OutputSchema()
	if len(schema) == 0 || len(res.Columns) == 0 {
		return nil
	}
	byName := make(map[string]parquet.Column, len(schema))
	for _, col := range schema {
		byName[col.Name] = col
	}
	metas := make([]wadjet.ColumnMeta, len(res.Columns))
	for i, name := range res.Columns {
		col, ok := byName[name]
		if !ok {
			// Positional fallback: a renamed output column (the gather's
			// renamer) keeps its position but not its name.
			if i >= len(schema) {
				return nil
			}
			col = schema[i]
		}
		metas[i] = wadjet.ColumnMeta{
			Name:      name,
			TypeName:  col.Type.String(),
			TypeID:    col.Type,
			Nullable:  col.Nullable,
			Precision: col.Precision,
			Scale:     col.Scale,
			// A plan property (FIX 2, #457/#458 fold-in): which DECIMAL
			// columns are aggregate output, and so must declare typmod -1
			// on the wire regardless of the real Precision/Scale above.
			WireUnconstrained: res.WireUnconstrainedDecimal[name],
		}
	}
	return metas
}

// sendResultRows emits a DataRow per result row and returns the count sent.
// Columnar batches (coord path) are boxed one batch at a time, dropping each
// batch reference once sent so peak boxed residency stays one batch; boxed
// rows (legacy db path) are sent as-is. fmtCodes selects the formatted
// variant used by the extended protocol; nil sends text-format rows.
//
// The stream is always fully closed before returning, including on a
// mid-stream error — the error is returned so the caller can surface an
// ErrorResponse after the partial DataRows (legal in the v3 protocol).
// ctx is the statement's context: a CancelRequest (or statement_timeout)
// mid-send stops the stream instead of sending the remaining rows.
func (c *pgConn) sendResultRows(ctx context.Context, columns []string, stream coordinator.BatchStream, boxed *wadjet.QueryResult, fmtCodes []int16, metas []wadjet.ColumnMeta, nestedSchema *nestedFieldSchema) (int, error) {
	sent := 0
	// Resolved once per result, not per row: the value a client reads has
	// to match the type the RowDescription declared, and only the metas
	// carry that (see timestampColumns).
	colTypes := sendColumnTypes(columns, metas)
	send := func(cells []any) {
		if len(fmtCodes) > 0 {
			c.sendDataRowFormatted(columns, cells, fmtCodes, colTypes, nestedSchema)
		} else {
			c.sendDataRow(columns, cells, colTypes, nestedSchema)
		}
		sent++
	}
	if stream != nil {
		defer stream.Close()
		for {
			if err := ctx.Err(); err != nil {
				return sent, err
			}
			b, err := stream.Next(ctx)
			if err != nil {
				return sent, err
			}
			if b == nil {
				break
			}
			// ToRowValues, not ToRows: the batch is already positional and
			// a name-keyed box cannot represent two columns of the same
			// name (#513 follow-up). This is also strictly cheaper — no
			// map per row.
			for _, cells := range b.ToRowValues() {
				send(cells)
			}
		}
	}
	if boxed != nil {
		for i := range boxed.Rows {
			// Boxed results (legacy db.Query path) are already in memory,
			// but a large one still takes real time to write out; check for
			// cancellation once per batch-sized run rather than per row.
			if i%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return sent, err
				}
			}
			// Cells reads the positional form when the result has one —
			// which is exactly when its column names collide — and boxes
			// the map by name otherwise, where that is exact.
			send(boxed.Cells(i))
		}
	}
	return sent, nil
}

// closeDescribeCache releases a cached Describe result stream that Execute
// never consumed (client error between Describe and Execute, statement
// re-Parse, connection teardown). Without this an over-budget result's
// spill scratch would outlive the portal.
func (c *pgConn) closeDescribeCache() {
	if c.describeStream != nil {
		c.describeStream.Close()
		c.describeStream = nil
	}
	c.describeResult = nil
	c.describeNestedSchema = nil
	c.describeErr = nil
	c.describeCancel = ""
	c.describeSynth = nil
	c.describedSQL = ""
}
