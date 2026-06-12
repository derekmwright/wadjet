package pgwire

import (
	"context"
	"strings"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/wadjet"
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
// policies, table-level access denial in wadjet.enforceAccessPolicies),
// while coord.ExecuteSQL today does not call into the same enforcement
// machinery. Until coord enforces ABAC, only bypass db.Query when the
// connection has no authenticated identity AND no auth provider is wired
// — i.e. the dev/harness path. Production deploys with auth keep the
// legacy path until coord-side ABAC is plumbed.
func (c *pgConn) canBypassDB() bool {
	return c.authProvider == nil && c.identity == nil
}

// queryViaCoord executes sql through coord.ExecuteSQL. The columnar batches
// are returned as-is, NOT boxed into QueryResult.Rows — the send path boxes
// one batch (~2K rows) at a time via sendResultRows. The previous shape
// (batchesToRows up front) materialized the full row set (~10x the columnar
// footprint) on top of the already-held Batches before writing a single
// DataRow, on the no-auth standalone/edge production profile — exactly what
// SQLResult's doc forbids.
//
// For Q18 SF10 (60M intermediate rows reduced to 100 by LIMIT), boxing here
// only ever sees the 100 final rows because coord's native-DAG executor
// produces the post-LIMIT batches at Gather. Legacy db.Query materialized
// the full pre-LIMIT pipeline output.
func (c *pgConn) queryViaCoord(ctx context.Context, sql string) (*wadjet.QueryResult, []*batch.RecordBatch, error) {
	res, err := c.coord.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	batches := res.Batches
	res.Batches = nil
	return &wadjet.QueryResult{Columns: res.Columns}, batches, nil
}

// sendResultRows emits a DataRow per result row and returns the count sent.
// Columnar batches (coord path) are boxed one batch at a time, dropping each
// batch reference once sent so peak boxed residency stays one batch; boxed
// rows (legacy db path) are sent as-is. fmtCodes selects the formatted
// variant used by the extended protocol; nil sends text-format rows.
func (c *pgConn) sendResultRows(columns []string, batches []*batch.RecordBatch, rows []map[string]any, fmtCodes []int16) int {
	sent := 0
	send := func(row map[string]any) {
		if len(fmtCodes) > 0 {
			c.sendDataRowFormatted(columns, row, fmtCodes)
		} else {
			c.sendDataRow(columns, row)
		}
		sent++
	}
	for i := range batches {
		for _, row := range batches[i].ToRows() {
			send(row)
		}
		batches[i] = nil
	}
	for _, row := range rows {
		send(row)
	}
	return sent
}
