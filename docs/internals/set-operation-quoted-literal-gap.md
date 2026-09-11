# Set operation quoted literal gap

Source: internal/planner/physical/set_op_stages.go — setOpQuotedLiteralGap, moved 2026-09-11 (#1026)

```go
// setOpQuotedLiteralGap is the refusal for an UNKNOWN-typed literal — a
// QUOTED string — whose resolved type this engine cannot build from text.
//
// PostgreSQL types such a literal from the other arms and coerces it with THAT
// type's input function, so `SELECT c_ts … UNION ALL SELECT '2010-01-01
// 00:00:00'` is timestamp, `c_bool ∪ 'true'` boolean and `c_port ∪ 'notaport'`
// 22P02. Wadjet's literal arm produces a STRING box and that box reaches the
// result column's vector unchanged, so the nine types batch.VectorAcceptsText
// says no to — BOOL, the four numeric machine types, TIMESTAMP, PORT, PROTOCOL
// and DURATION — failed with the #361 silent-write guard: no SQLSTATE at all
// (the pgwire door then says XX000, "the server broke"), mid-execution on the
// single-process path and after THREE retries of a deterministic parse failure
// on the stage DAG.
//
// So it is refused at PLAN time instead, with the same 0A000 the carrier gap
// takes and for the same reason: PostgreSQL answers the query and this engine
// does not yet. A bare NULL is unaffected — a NULL has no text to parse and
// every vector takes one — and so is an UNQUOTED literal, which the evaluator
// already types.
//
// Closing it means giving the literal its resolved type at PLAN time rather
// than at the vector: parse the text into the target type's own box in the
// arm's rows (the single path, beside setOpLiteralRows) and rewrite the arm's
// projection expression to the target's literal spelling (the DAG, beside
// reconcileSetOpArmTypes' stamp), which also makes unparseable text 22P02 the
// way PostgreSQL reports it. Recorded in ADR-0012 item 12.
```
