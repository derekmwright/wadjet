# Set operation numeric widening

Source: internal/planner/physical/set_op_stages.go — setOpWiden, moved 2026-09-11 (#1026)

```go
// setOpWiden is the numeric ladder: INT32 → INT64 → DECIMAL → FLOAT32 →
// FLOAT64.
//
// Every rung is PostgreSQL's, verified against postgres:17-alpine with
// pg_typeof over the union itself:
//
//	`numeric UNION ALL bigint`          → numeric
//	`numeric UNION ALL double precision`→ double precision
//	`real    UNION ALL integer/bigint`  → real
//	`real    UNION ALL numeric`         → real
//	`real    UNION ALL double precision`→ double precision
//
// Arm ORDER changes none of them, and changes none of them here.
//
// FLOAT32 gets its OWN rung rather than sharing FLOAT64's. Both are PREFERRED
// types of PostgreSQL's numeric category, so each beats the exact types
// (integer, numeric) it meets and only float8 beats float4 — and the
// difference is a VALUE, not just an OID: a real column holding 0.1 renders
// 0.1, and the same column widened to double precision renders
// 0.10000000149011612, which is the float32 value spelled to float64
// precision and is not what either engine holds. `CREATE TABLE t (x FLOAT)`
// declares a FLOAT32 column here, so this is reachable from plain DDL.
```
