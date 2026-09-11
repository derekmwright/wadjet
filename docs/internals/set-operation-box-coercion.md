# Set operation box coercion

Source: internal/planner/physical/set_op_schema.go — coerceSetOpArmRows, moved 2026-09-11 (#1026)

```go
// coerceSetOpArmRows rewrites an arm's boxed rows so they carry the VALUE the
// unified column expects, for every rung of the ladder that moves one.
//
// The boxes are not uniform across types and that asymmetry is the whole
// problem: a DECIMAL boxes as its rendered TEXT (Vector.GetValue), an integer
// as a raw int64, a float as a float64. Handing those to batch.FromRows under
// a schema they were not boxed for is how the single-process path answered
// wrongly, or refused, depending on which arm came first:
//
//   - integer box → DECIMAL column: read as an UNSCALED carrier, dividing
//     every integer by 10^scale (1 → 0.01, #547). Rewritten to the integer's
//     decimal TEXT, which routes it through the same exact text path a native
//     DECIMAL box takes (ParseDecimalString at FromRows, DecimalTextAt at the
//     dedup key), so it arrives at its true value and keys the same as an
//     equal DECIMAL value.
//   - DECIMAL text box → FLOAT column: the #361 silent-write guard refuses
//     the store and the whole query fails, where PostgreSQL answers (#541
//     shape 2). Converted to the float the widened column holds — narrowed to
//     float32 in the BOX for a real result, so the dedup key sees the same
//     number the already-real arm produces.
//   - DECIMAL text box → wider DECIMAL column: exact as text, but the value
//     may not FIT the widened (p,s) — the union's own type decision can put a
//     value out of range that both arms held comfortably (#552). Checked, not
//     assumed.
//   - integer / float box → FLOAT32, FLOAT64 or INT64 column: converted here
//     rather than relying on SetValue's own conversions, so the dedup key
//     sees one box shape per column.
//
// Every value the unified DECIMAL cannot hold is a "numeric field overflow"
// ERROR carrying SQLSTATE 22003, worded to match the stage DAG
// (exec.coerceDecimalVector) so both paths refuse the same input the same way
// — NOT a silently saturated Int128Max, which is what routing an out-of-range
// value through DecimalTextAt's comparison-oriented saturating parser
// produces (#553). Wadjet's finite DECIMAL carrier cannot hold PostgreSQL's
// unconstrained numeric, so in the overflow band wadjet errors where
// PostgreSQL answers; ADR-0024 item 7 records that residual (#552) as the
// accepted cost of item 1's finite carrier.
//
// srcSchema is the arm's OWN schema; target is the unified result schema.
// They correspond by POSITION, and the arm's rows are still keyed by
// srcSchema's names (they have not been re-aligned to the result names yet),
// so the rewrite is applied before alignSetOpRows.
```
