# Dml row visibility and panic boundary

Source: wadjet/dml.go — func MatchDMLRows(ctx context.Context, b *batch.RecordBatch, predicate DMLPredicate, deleted map[int64]bool) (matched []int64, err error) {, moved 2026-09-11 (#1026)

MatchDMLRows returns the indices of b's rows the statement matches, and is
the ONLY place a DMLPredicate is allowed to be called.

deleted is the set of row positions in THIS file that a delete marker has
already removed (catalog.DeletedRowsByFile), and passing it is not optional
— a nil map means "this file has no markers", not "do not check". The
parameter exists rather than a second entry point precisely because the
defect was that every DML match scan simply did not look: an UPDATE matched
rows its own earlier UPDATEs had superseded, re-ingested them beside the
live copy and marked the source file again, so re-updating one row produced
1, then 2, then 4 rows — silently, on plain INT64 columns (#674). The
SELECT path has applied this filter all along (scan.Scanner), which is why
the row COUNT a client saw was wrong and the DML's own view was internally
consistent.

Expression evaluation has no error return (ADR-0019): the one class of
condition that cannot answer with a value and must not answer with NULL —
a division by zero, an invalid cast — raises a panic carrying a
FatalEvalPanic, and a driver converts it back into an error with
PostgreSQL's SQLSTATE. Every DML match scan called Eval with NO such
boundary, so `DELETE FROM t WHERE 1/0 = 1` over HTTP returned a transport
EOF and a goroutine dump instead of 22012 — net/http's own recover, which
drops the connection (#677). The embedded and pgwire doors survived only
because DB.Execute's own boundary caught it several frames up, and the
error a caller got there named the statement rather than the predicate.

The boundary is per FILE SCAN, not per row: one deferred call for a whole
batch, no per-row cost. It owns nothing — no lock, no channel, no
reservation — so discharging its obligations (ADR-0019 §2a) is exactly
returning the error.
