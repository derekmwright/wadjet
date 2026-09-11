# Local set operation common schema

Source: internal/planner/physical/set_op_schema.go — unifySetOpSchemas, moved 2026-09-11 (#1026)

```go
// unifySetOpSchemas is the result type of a set operation: the first arm's
// column NAMES — SQL says the result takes them — over the COMMON TYPE of the
// two arms per position.
//
// The common type is the stage DAG's, not a second rule of this path's own:
// setOpWiden is the ladder (INT32 → INT64 → DECIMAL → FLOAT64, pinned against
// live postgres:17 by TestSetOpWidenLadder) and setOpDecimalTarget is the
// (p,s) the DECIMAL rung resolves to. Both are called here, for EVERY rung,
// so the two execution paths cannot disagree about what the output type IS —
// which is a wire fact as well as an engine one, since a client reads the
// column's OID (#541 shape 3).
//
// The rungs and what each one costs when it is NOT reconciled:
//
//   - DECIMAL over DECIMAL. The rows reach batch.FromRows as their rendered
//     decimal TEXT, boxed at each arm's own scale, and FromRows re-reads that
//     text at the schema's scale — so handing it the first arm's scale
//     truncated the second arm's values: over `DECIMAL(9,2) UNION ALL
//     DECIMAL(18,4)`, 12.7501 came back as 12.75 and 12.7499 as 12.74, and
//     the UNION then counted 8 distinct values where PostgreSQL counts 9
//     (#532). The scale is the max over the arms — the only choice that moves
//     no value — and the precision is REBUILT from the widest integer part,
//     the DAG's rule, because max(precision) is not a bound on the widened
//     values: DECIMAL(18,2) alongside DECIMAL(9,4) needs 16 integer digits at
//     scale 4, i.e. 20, where max(precision) declares 18 and the type is too
//     small for its own values.
//
//   - DECIMAL over INTEGER. `numeric ∪ bigint` is `numeric` in PostgreSQL, so
//     the integer arm widens INTO the DECIMAL at the DECIMAL's scale. Left
//     unreconciled this was the silent corruption of #547: the integer arm's
//     box is an int64, NOT text, so FromRows read it into the DECIMAL vector
//     as an UNSCALED carrier and divided every integer by 10^scale (1 came
//     back as 0.01).
//
//   - A FLOAT over anything numeric. float4 and float8 are both PREFERRED
//     types of PostgreSQL's numeric category, so each beats the exact types
//     it meets and only float8 beats float4: `numeric ∪ double precision` is
//     double precision, `numeric ∪ real` is real, in EITHER arm order.
//     Unreconciled, the arm order decided the answer: with the DECIMAL arm
//     first the result stayed DECIMAL (a wrong OID on the wire, right-looking
//     values), and with the FLOAT arm first the DECIMAL arm's rendered text
//     was stored into a float vector and the #361 guard failed the query
//     outright — the two halves of #541. Keeping real REAL is a value
//     question as well as an OID one: a real column holding 0.1 renders 0.1,
//     and the same value widened to double precision renders
//     0.10000000149011612.
//
//   - INT32 over INT64. `integer ∪ bigint` is bigint. No VALUE moves here,
//     which is why this rung used to be skipped; the OID does, and a client
//     reading int4 for a column carrying int64 values is the same class of
//     defect as the DECIMAL one, one type family over.
//
// Anything else — a non-numeric type, or a DECIMAL whose (p,s) nothing could
// resolve — is left exactly as it was. A computed DECIMAL expression carries
// no declared (p,s) (#555, being fixed in the declared-type layer), and
// guessing one here would move values under a type nobody stated.
```
