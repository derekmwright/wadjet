# Real operand array cast typing

Source: internal/planner/physical/real_list_refusal.go — realTypedNode, moved 2026-09-11 (#1026)

```go
// realTypedNode reports whether an operand's own type is REAL, which is what
// decides the array cast — not whether it is a bare column.
//
// PostgreSQL resolves the list's element type over the members AND the probed
// expression, so any real-typed left operand pulls the array to real[]
// (EXPLAIN VERBOSE, postgres:17):
//
//	-r_val IN (-3.1, -7.1)          -> ((- r_val) = ANY ('{-3.1,-7.1}'::real[]))
//	CAST(d_val AS REAL) IN (3.1,…)  -> ((d_val)::real = ANY ('{3.1,7.1}'::real[]))
//	(r_val + 0) IN (3.1, 7.1)       -> (… = ANY ('{3.1,7.1}'::double precision[]))
//
// The third is why this cannot simply follow the operand down to a column: an
// integer literal added to a real gives DOUBLE PRECISION in PostgreSQL
// (pg_typeof(r_val + 0) is `double precision`), so that shape must stay
// widened. Unary ± is the one operator that preserves real.
//
// It is NOT nodeDeclaredType. That function deliberately collapses FLOAT32 to
// FLOAT64 for unary ± — it types the COLUMN a projection allocates, where the
// engine materializes `-f32col` as a float64 — and reading it here would
// answer "double" for the very shape PostgreSQL calls real. The two questions
// are different; expr.realTypedOperand is this one's runtime twin and the two
// must keep answering alike.
```
