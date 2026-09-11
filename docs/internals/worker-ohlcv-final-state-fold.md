# Worker ohlcv final state fold

Source: internal/worker/ohlcv_fold.go — func applyOhlcvFold(batches []*batch.RecordBatch, aggs []distributed.AggSpec) ([]*batch.RecordBatch, error) {, moved 2026-09-11 (#1026)

applyOhlcvFold replaces every `__ohlcv_state#X` column with a ROW column
named X holding the finished bar for each group.

It is a sibling of applyVarFold rather than a caller of applyStateFold,
because a bar's answer is a ROW and applyStateFold's `finalize` returns a
float64. Sharing the walk would mean widening that signature for every
caller; a bar is the first state whose answer is not a number and it will not
be the last (ADR-0035 names TDIGEST and TOP_K), so the shape to converge on
is a value-returning fold rather than a float-returning one — a change to
make when the second such state arrives, with two callers to test it
against, not with one.

Called from the final_aggregate fragment only, on the same spec.FoldAvg
gate: intermediate merge_aggregate stages MUST keep shipping the state, or
the stage above them would re-aggregate finished bars — which is a MAX over
two ROWs, the defect the decomposition exists to remove.

The declaration comes from the PLAN — `distributed.AggSpec.OutputFields`,
derived once by `physical.aggOhlcvOutputFields` from the input columns'
declared types, for a bare argument and a COMPUTED one alike. The encoded
state's own header is the fallback, for a spec that carried none.

Neither is invented. A fold that reached this point with no declaration used
to substitute FLOAT64 for every field, which is how the same statement
declared DECIMAL(18,4) against a standalone server and FLOAT64 against a
coordinator — OID 1700 against 701 in the RowDescription (#965 round 2, B1).
