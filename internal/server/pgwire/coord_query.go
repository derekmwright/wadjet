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

// queryViaCoord executes sql through coord.ExecuteSQL and adapts the result
// into a *wadjet.QueryResult shape that the existing pgwire send-row path
// can consume. Rows are materialized batch-by-batch via batch.RecordBatch.ToRows
// — which is bounded by the batch size (~2K rows) per call but the returned
// slice is the full row set. The OOM win is that we no longer flow through
// the legacy CollectSink.Finalize path that allocates the same rows once at
// the planner pipeline and once at db.Query.
//
// For Q18 SF10 (60M intermediate rows reduced to 100 by LIMIT), the ToRows
// allocation in this adapter only sees the 100 final rows because coord's
// native-DAG executor produces the post-LIMIT batches at Gather. Legacy
// db.Query materialized the full pre-LIMIT pipeline output.
func (c *pgConn) queryViaCoord(ctx context.Context, sql string) (*wadjet.QueryResult, error) {
	res, err := c.coord.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, err
	}
	rows := batchesToRows(res.Batches)
	return &wadjet.QueryResult{
		Columns: res.Columns,
		Rows:    rows,
	}, nil
}

// batchesToRows materializes columnar batches into row form. Bounded
// allocation per batch (one slice grow per batch instead of per row).
func batchesToRows(batches []*batch.RecordBatch) []map[string]any {
	if len(batches) == 0 {
		return nil
	}
	total := 0
	for _, b := range batches {
		total += b.ActiveLen()
	}
	out := make([]map[string]any, 0, total)
	for _, b := range batches {
		out = append(out, b.ToRows()...)
	}
	return out
}
