# Typed constant aggregate lift

Source: internal/planner/logical/const_arith_agg_typed.go — liftConstArithAggsWithTypes, moved 2026-09-11 (#1026)

The constant-arithmetic aggregate lift, decided from the column's TYPE
(#850).

#841 stopped the syntactic lift from moving a per-row 22003 out of the row
where it belongs: `SUM(x * k)` → `SUM(x) * k` answers where the per-row form
must raise, and PostgreSQL raises for the input expression in every
position. It declined for EVERY integer literal, because the builder runs
before any type is known, and the ClickBench Q30 shape — 90 × `SUM(col + k)`
over one integer column — went 7.6 ms to 342 ms.

The recovery is not a threshold and not a heuristic: the lift is SAFE
exactly when the per-row arithmetic CANNOT refuse, and that is decidable at
plan time from the column's declared type and the manifest's min/max. This
pass runs inside logical.Optimize, which every caller invokes AFTER
physical.AnnotateScanColumns — so `Node.ScanColTypes`, `Node.ScanColStats`
and `Node.ScanRowEstimate` are on the Scan by the time it looks.

# What has to be proven, and it is BOTH forms

The obvious half is the per-row form: `col op k` must not leave int64 for
any row. `+`, `-` and `*` are monotone in col for a fixed k, and |col*k| is
maximal at an extreme, so checking the column's MIN and MAX is exact rather
than conservative.

The half that is easy to miss is the LIFTED form, which has arithmetic the
per-row form does not: `SUM(x) + k*COUNT(x)` multiplies the literal by the
row count. With k near int64's edge that product refuses where the per-row
`x + k` over small x does not — the same defect as #841, pointing the other
way. So the pass bounds `|k| × N` too (N is the manifest's row count, an
upper bound on COUNT since a filter only removes rows), and bounds the
numeric carrier the aggregate's own result rides on: SUM over an integer is
an exact numeric(38,0) and AVG a numeric(38,4), so the lifted expression has
38 and 34 integer digits to fit in.

# Statistics are read ONCE per column per query

Q30's shape has ninety aggregates over a handful of columns. The decision is
cached per (column, scan) for the pass, because re-walking the plan to the
scan for each aggregate is the cost the recovery exists to remove.

# What still declines, and why

  - No statistics for an INT64 column: min/max is what proves the bound, and
    a table that has never been ANALYZEd (or a manifest with no per-column
    stats) has none. Right and slower.
  - A DECIMAL column: the engine's own 128-bit carrier can refuse where
    PostgreSQL answers, and the lifted and per-row forms round at different
    scales. Unchanged from the #841 state.
  - Anything below the Aggregate that can rebind a name — a Project, a join,
    a set operation. The walk stops there exactly as strictIntArithCols does,
    and for the same reason: a wrong type claim here is a wrong ANSWER.

The kill switch is the same one: constArithAggToggle (WADJET_CONST_ARITH_AGG
=0), so the optimization-invariance oracle covers this pass for free.
