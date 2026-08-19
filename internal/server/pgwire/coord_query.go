package pgwire

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/coordinator"
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
func (c *pgConn) queryViaCoord(ctx context.Context, sql string) (*wadjet.QueryResult, coordinator.BatchStream, error) {
	res, err := c.coord.ExecuteSQL(ctx, sql)
	if err != nil {
		res.Close()
		return nil, nil, err
	}
	return &wadjet.QueryResult{Columns: res.Columns}, res.Stream(), nil
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
func (c *pgConn) sendResultRows(ctx context.Context, columns []string, stream coordinator.BatchStream, rows []map[string]any, fmtCodes []int16, metas []wadjet.ColumnMeta) (int, error) {
	sent := 0
	// Resolved once per result, not per row: the value a client reads has
	// to match the type the RowDescription declared, and only the metas
	// carry that (see timestampColumns).
	colTypes := sendColumnTypes(columns, metas)
	send := func(row map[string]any) {
		if len(fmtCodes) > 0 {
			c.sendDataRowFormatted(columns, row, fmtCodes, colTypes)
		} else {
			c.sendDataRow(columns, row, colTypes)
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
			for _, row := range b.ToRows() {
				send(row)
			}
		}
	}
	for i, row := range rows {
		// Boxed results (legacy db.Query path) are already in memory, but a
		// large one still takes real time to write out; check for
		// cancellation once per batch-sized run rather than per row.
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return sent, err
			}
		}
		send(row)
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
	c.describeErr = nil
	c.describeCancel = ""
	c.describeSynth = nil
	c.describedSQL = ""
}
