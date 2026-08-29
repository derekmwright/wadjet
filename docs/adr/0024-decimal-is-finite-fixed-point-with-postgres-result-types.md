# ADR-0024: DECIMAL is a finite 128-bit fixed-point type that follows PostgreSQL's result-TYPE rules

Status: Accepted (2026-08-29, opening the numeric-parity arc after v0.18.4).
Extends ADR-0012 items 9 and 12, which settled the aggregate and set-operation
halves of this question piecemeal; this record settles the whole type.

## Context

Wadjet's DECIMAL is an `Int128` unscaled integer at a per-column scale
(`internal/engine/batch/decimal.go`), 16 fixed bytes per row in every
representation the value travels through: the vector, the parquet FLBA leaf,
the `.wshf` shuffle chunk, the spill run. PostgreSQL's `numeric` is unbounded
in both precision and scale, and every computed `numeric` result is declared
with typmod −1.

Thirteen open correctness issues sit on the difference (#552, #541, #551,
#553, #554, #555, #529, #586, #587, #475, #534, #542, and #636's int/int
half). A survey of the implementation on 2026-08-29 (main 8a294d92) found
that they are **two problems, not one**:

1. **Typing.** The planner's declared-type layer is `(parquet.TypeID,
   expr.Confidence)` — it has no room for `(p,s)` — so `colRefDeclaredType`
   answers `Undecided` for every DECIMAL column, and everything downstream
   falls to its non-DECIMAL default: arithmetic declares FLOAT64
   unconditionally, `CAST(x AS DECIMAL(10,2))` is a no-op declared STRING,
   COALESCE/NULLIF/GREATEST/LEAST are `RetSameAsArg(TypeFloat64)`, window
   SUM/AVG accumulate in float64, every scalar math function returns float64.
   The engine has **no decimal arithmetic at all** — `Int128` has
   `Add/AddChecked/Sub/MulPow10/Cmp` and nothing else. Eleven of the thirteen
   issues live here, and they are fixed the same way under ANY carrier.
2. **Range.** The carrier is finite (38 digits) and has no NaN. Only #552 and
   #534 live here.

Two facts that shaped the decision:

- **No benchmark in the tree exercises DECIMAL.** `benchmarks/tpch/schema.go`
  declares every monetary column FLOAT64 ("DECIMAL would be ideal but Wadjet
  uses float"); ClickBench has no decimal. The TPC-H specification declares
  those columns `DECIMAL(15,2)`. So the "arbitrary precision is too slow"
  argument has no measured target, and the spec-conformant TPC-H schema is
  itself the missing correctness gate and benchmark.
- DuckDB — the performance goal and second oracle (ADR-0012) — is also a
  finite width-38 Int128 DECIMAL that errors on overflow. Finite fixed-point
  is the position of every vectorized engine, not a shortcut.

## Decision

### 1. The carrier stays finite: Int128, 38 digits, one scale per column

Fixed 16-byte SoA is what ADR-0002's typed kernels, ADR-0006's memory
accounting, ADR-0010's shuffle formats and the parquet leaf all assume. An
arbitrary-precision or hybrid carrier (options B and C in the survey) touches
every kernel, both wire formats and the key encoding of ADR-0023, and the
hybrid adds a discriminant that every accessor must check — a new silent-wrong
class of exactly the kind this arc exists to close. Reopening this requires a
workload that hits the 38-digit bound, measured; none has.

### 2. The TYPE of every DECIMAL expression is computed from its operands, PostgreSQL-style

The declared-type layer carries `(TypeID, DecimalMeta{Precision, Scale})`.
Category resolution is PostgreSQL's, already pinned for set operations by
`TestSetOpWidenLadder` and now applied everywhere:

    INT32 → INT64 → DECIMAL → FLOAT64
    numeric ⊕ integer   → numeric      (an integer is DECIMAL(10,0) / (19,0))
    numeric ⊕ float8    → float8       (float8 is the category's preferred type)
    int ⊕ int           → int          (truncating division, as PostgreSQL)
    CASE / COALESCE / NULLIF / GREATEST / LEAST / IF over any DECIMAL branch → DECIMAL
    MIN/MAX/FIRST_VALUE/LAG/… keep the input's (p,s); SUM → (38,s); AVG → (38, min(s+4,38))
    windowed SUM/AVG answer what the grouped ones answer, exactly

### 3. The (p,s) of a computed result follows the finite-decimal industry rule

PostgreSQL has no `(p,s)` rule — numeric is unbounded. A finite carrier needs
one, and the one every finite-38 engine converged on (SQL Server, Spark, Hive)
is adopted verbatim so the choice is not wadjet's own:

    e1 + e2, e1 - e2 : p = max(s1,s2) + max(p1-s1, p2-s2) + 1 ; s = max(s1,s2)
    e1 * e2          : p = p1 + p2 + 1                        ; s = s1 + s2
    e1 / e2          : s = max(6, s1 + p2 + 1) ; p = p1 - s1 + s2 + s
    e1 % e2          : p = min(p1-s1, p2-s2) + max(s1,s2)     ; s = max(s1,s2)
    CAST(x AS DECIMAL(p,s)) : exactly (p,s), rounded half away from zero, 22003 past it
    CAST(x AS DECIMAL)      : the operand's own (p,s); (38, 0) from an integer

When `p > 38`: `intDigits = p − s; s = max(38 − intDigits, min(s, 6)); p = 38`.
Fractional digits are given up first, but never below `min(s, 6)`; only once
the fraction is at that floor does the integer part shrink (`(40,4)` →
`(38,4)`, not `(38,2)`). This is Spark's `adjustPrecisionScale` verbatim.
It is a **documented divergence in the number of digits kept**, the same class ADR-0012 item 9 already accepts for AVG:
both engines are exact to the digits they keep and agree to `min(scale)`.

Scale reduction and division round half away from zero, PostgreSQL's numeric
rounding.

### 4. A value with no exact carrier at its declared type is a 22003 ERROR — never saturated, wrapped, narrowed to float, or zeroed

ADR-0012 items 9 and 12 said this for SUM and for set-operation coercion. It
now holds at every value-producing site: arithmetic overflow, CAST, the
single-process set-operation adapter (`ParseDecimalString` discarding
`ScaledDecimal.Sat` is #553 — one line), window accumulators, ingest of a
literal too wide for its column. Every such error carries SQLSTATE `22003`
`numeric_value_out_of_range` through `internal/sqlerr` — the four existing
overflow sites are bare `fmt.Errorf` and reach clients as an internal error
today. Non-numeric text into a DECIMAL is `22P02`, never zero.

Saturation stays where it was built for and is correct: a COMPARISON literal
outside the column's range is a bound and orders above or below every stored
value (#462). The value-producing callers of `DecimalTextAt` are the ones
that must honour `Sat`.

### 5. On the wire, a DECIMAL carries the typmod its inputs AGREE on — `select_common_typmod`

PostgreSQL does not gate on "computed". It runs `select_common_typmod` over
the inputs a result is resolved from: the typmod survives when every one of
them carries the SAME one, and the result is unconstrained otherwise.

A BARE COLUMN REFERENCE carries its column's typmod. An aggregate call, a
window function, an operator, a CAST and every other function call carry
−1 — so one of those anywhere in the fold makes the whole result −1. The
CHOICE constructs fold their branches over exactly the arguments their TYPE
resolution folds, which is why `NULLIF(numeric(9,2), numeric(18,4))` keeps
`numeric(9,2)` (it mirrors argument 0 alone) while `GREATEST` over the same
pair drops to plain `numeric`. A NULL branch — explicit, or a CASE's missing
ELSE — is coerced into the common type carrying typmod −1, so it DROPS the
modifier: this is where the typmod fold parts company with the TYPE fold,
which skips a NULL outright (`COALESCE(a, NULL)` is `numeric(9,2)` as a type
and plain `numeric` on the wire). Verified live against 17.11's `\gdesc`:

    GREATEST(a,a), GREATEST(a), COALESCE(a,a),
    CASE … THEN a ELSE a, NULLIF(a,b)      -> numeric(9,2)
    NULLIF(b,a), LEAST(b,b)                -> numeric(18,4)
    GREATEST(a,b), MIN(a), MIN(a) OVER ()  -> numeric
    COALESCE(a,NULL), CASE … THEN a END    -> numeric
    (SELECT MIN(a) …) UNION ALL (SELECT a …) -> numeric

Wadjet's internal `(p,s)` is an engine fact that sizes vectors and parquet
leaves; it is not the client's contract. `declaredWireUnconstrainedDecimal`
gated on `proj.IsAgg` alone (#587, #542); it runs the fold above after this.
"Not a bare column reference" — the first correction — is wrong in BOTH
directions: it drops the typmod PostgreSQL keeps for a choice over one
column, and keeps the one PostgreSQL drops for a set operation over a
computed arm.

### 6. NaN is a comparison literal, not a stored value

PostgreSQL's numeric has NaN (greater than every non-NaN, equal only to
itself) and, since 14, ±Infinity. Int128 has no bit pattern for them and the
parquet DECIMAL annotation has none either. So: `d = 'NaN'`, `d <> 'NaN'`,
`d < 'NaN'` are accepted and answer by PostgreSQL's order over a column that
holds no NaN (0 rows, all rows, all rows); `ORDER BY` needs nothing.
`CAST('NaN' AS DECIMAL)` as a VALUE, and ingesting one, are `22003` with a
message naming this record. Same for the infinities. A documented divergence:
wadjet can compare against a NaN it cannot store.

### 7. The 38-digit set-operation cap (#552) is closed as a recorded divergence of item 1

A set operation moves stored values, so item 3's fractional-digit reduction
would drop digits a row actually holds; the only honest answers are the
error (current, pinned by `TestSetOpDecimalCapIsARangeReduction`) or a wider
carrier (item 1's reopen clause). The shape — `DECIMAL(38,0)` beside
`DECIMAL(11,10)` — is the pathological corner of the cap, not a BI query.

## Consequences

- One table of rules replaces five independently-derived ones (grouped
  aggregate, set-op DAG, set-op local, window, cast). The local set-operation
  path's `max(precision)` rule (`set_op_schema.go`) and the DAG's rebuilt
  integer part (`set_op_decimal.go`) become one function.
- New kernels: `Int128.Mul` (256-bit intermediate, checked), `Int128.QuoRem`
  with rescale and half-away-from-zero rounding, decimal arithmetic mode in
  `BinOpNumeric`, exact window SUM/AVG including the sliding-frame subtract.
- The TPC-H benchmark gains a spec-conformant `DECIMAL(15,2)` schema variant.
  It is a correctness gate first (22 queries with decimal arithmetic,
  aggregation, comparison and ORDER BY on both execution paths) and the
  decimal performance baseline second; the FLOAT64 schema stays the
  published-number benchmark until a release decides otherwise.
- The pg-oracle gains a corpus entry per rule in item 3 (exact comparison
  to `min(scale)`) and per row of item 5's table, the type-matrix gains a
  computed-DECIMAL column class,
  and every `knownBug`/`pins` entry naming #529/#542/#587/#555 must fail —
  deleting them is the proof.
- Deliberate divergences from PostgreSQL, all recorded in ADR-0012 item 12's
  list: digits kept past 38 (item 3); the 38-digit range on stored values
  (item 7); NaN/Infinity not storable (item 6); STDDEV/VARIANCE/CORR/COVAR
  /MEDIAN/PERCENTILE over DECIMAL stay float64 (ADR-0012 item 9).
- Refuted premise, recorded: "DECIMAL would be ideal but Wadjet uses float"
  in the TPC-H schema was an engine limitation, not a design choice, and its
  presence meant the type had no benchmark and no 22-query gate for two
  years of development.

## Alternatives rejected

- **Arbitrary precision (big.Int / base-10000 digit vectors).** Closes #552
  and #534 outright; costs the fixed-width layout every kernel, the memory
  ledger, both shuffle formats and the parquet leaf assume, plus a
  per-value allocation. Unmeasured, because nothing measures DECIMAL — and
  that gap is closed by this arc's benchmark, so this alternative can be
  re-argued on evidence later.
- **Hybrid Int128 with per-vector promotion.** Every accessor and kernel
  gains a discriminant branch; ADR-0023's key encoding must be identical
  across representations or a join between a promoted and an unpromoted
  vector silently misses. Rejected for adding a silent-wrong class.
- **Resolve to FLOAT64 when the exact type does not exist.** Answers instead
  of failing, and loses digits nobody can see. The project's standing rule is
  the loud error.
- **PostgreSQL's magnitude-dependent division scale (≥16 significant
  digits).** The digits kept would depend on the values, so the same query
  over more rows could change the scale of its own output column. The fixed
  rule keeps the output type a function of the input types.
