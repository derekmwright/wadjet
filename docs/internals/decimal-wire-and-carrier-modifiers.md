# Decimal wire and carrier modifiers

Source: internal/planner/physical/output_declared_schema.go — declaredWireUnconstrainedDecimal, moved 2026-09-11 (#1026)

```go
// declaredWireUnconstrainedDecimal names the DECIMAL output columns whose
// PostgreSQL wire typmod must declare "unconstrained" (-1) even though this
// engine's own declared/exec schema keeps a real (p,s) for them.
//
// Verified live against postgres:17-alpine's \gdesc: an aggregate function
// call NEVER carries its argument's typmod through — MIN(n)/MAX(n)/
// MIN_BY(x,n)/MAX_BY(x,n)/SUM(n)/AVG(n) over a numeric(p,s) column all
// report an unconstrained numeric, and only a BARE column reference in the
// SELECT list keeps (p,s). declaredOutputSchema's own Precision/Scale answer
// stays real for these columns — internal/engine/exec/aggregate.go's DECIMAL
// vector allocation and internal/storage/parquet's file writer both key
// physical decisions off Precision/Scale (18-digit INT64 vs 38-digit
// FixedLenByteArray encoding), so zeroing it there would risk silently
// mis-encoding a materialized MIN/MAX-of-DECIMAL(38,s) result — this is
// wire-metadata ONLY, consulted solely by pgTypeMod (fold-in to #457/#458,
// FIX 2).
//
// The gate is PostgreSQL's select_common_typmod: a numeric result KEEPS a
// typmod when every input it is resolved from carries the SAME one, and is
// unconstrained otherwise — verified live against 17.11's \gdesc, where
// GREATEST(a, a), COALESCE(a, a), CASE … THEN a ELSE a and NULLIF(a, b) over
// numeric(9,2) a and numeric(18,4) b all describe as numeric(9,2), NULLIF(b,
// a) and LEAST(b, b) as numeric(18,4), and GREATEST(a, b) as plain numeric.
//
// It is emphatically NOT "computed ⇒ unconstrained": that reading is wrong in
// both directions, dropping the typmod PostgreSQL keeps for a choice over one
// column and keeping the one it drops for a set operation over a computed
// arm. What carries a typmod is a BARE COLUMN REFERENCE and the choice
// constructs folded over bare references; an aggregate, a window function,
// arithmetic, a CAST and every other function call carry -1, and one -1
// anywhere in the fold makes the result -1 (#587, #542, ADR-0024 item 5).
```
