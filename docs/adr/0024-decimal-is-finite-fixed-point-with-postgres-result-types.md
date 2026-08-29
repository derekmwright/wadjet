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

The value half of that rule reaches only the CHECKED reader so far — see the
implementation note below for which sites it covers today and which two do
not.

**Implemented 2026-08-29 (#534).** The mechanism is the one the carrier
already had: `ScaledDecimal.Sat`, the flag a finite literal wider than Int128
sets so it orders past every value the column can hold (#462). NaN and
Infinity saturate ABOVE, -Infinity BELOW — exactly where PostgreSQL's total
order puts them relative to anything a DECIMAL column can hold, so its
`NaN > Infinity` is simply not observable here. `batch.DecimalBoundTextAt` is
the comparison reader (specials, then `DecimalTextAt`) and the ONLY one that
accepts them: `DecimalTextAt` still refuses, so nothing value-producing can
reach a bound by accident, and `ParseDecimalStringChecked` turns the three
into the `22003` above.

**What the 22003 covers today, and what it does not.** It covers every site
that produces a value through the checked reader: `ParseDecimalStringChecked`,
`Vector.SetValueChecked`, `FromRowsChecked`, and through them the
single-process set-operation adapter. `physical.setOpCheckedDecimalText`
classifies through the same function, so the DAG's arm agrees. Two sites do
NOT, and this record does not claim them: `CAST(x AS DECIMAL(p,s))` is still
item 1's declared-STRING no-op — `CAST('NaN' AS DECIMAL(9,2))` yields the
string "NaN", `CAST('Infinity' AS DECIMAL(9,2))` the string "Infinity" (where
PostgreSQL raises 22003), and `CAST('NaN' AS DECIMAL)` a float64 NaN — until
the CAST evaluator lands (#555); and the UNCHECKED WRITE PATHS store 0 for all
three exactly as they do for `'abc'`, which is item 4's residual over the whole
type rather than anything specific to these values.

There were TWO unchecked write paths and both are named here, because only one
is on the line a user's INSERT actually takes. `batch.ParseDecimalString` via
`Vector.SetValue` is the row-to-batch adapter, and it is STILL the unchecked
one by contract — `Vector.SetValueChecked` is the sibling every value-producing
caller takes. `parquet.decimalUnscaledInt64` (and `decimalFLBABytes` beside it)
was the FILE WRITER, and it is the one an ingested row reaches: its string arm
sent every value through `strconv.ParseFloat`, so unparseable text stored 0, a
NaN or an infinity stored 0 through the float arm's own guard, `' 3.50 '`
stored 0 because ParseFloat refuses the surrounding space, a literal wider than
the column WRAPPED the int64 (`99999999999999999999.99` into a `DECIMAL(9,2)`
stored `-92233720368547758.08`), and anything past float64's ~16 significant
digits lost its exactness on the way in. One predicate serves both refusal
sites — the plan-time one (`physical.refuseLiteralForType` →
`expr.IsNumericLiteralText`) and the runtime one
(`kernel.DecimalLiteral.Numeric`) — so the accept-set cannot differ between
them; the row-group prune withholds for these literals
(`kernel.StatsDomainValue`), which costs a prune and cannot cost a row.

**The writer half was closed 2026-08-29 (#647).** `parquet.DecimalValueFromBox`
replaces `decimalUnscaledInt64` as the one door every DECIMAL box takes into a
leaf, called from `NativeWriter.decomposeLeaf` where the column, its declared
`(p, s)` and the row number are all still known — the same site and the same
`nw.fail` channel the DATE literal already used (#560). It parses text exactly
with no float64 anywhere, rounds a finer-scale literal to the column's scale
half away from zero as PostgreSQL does ON ASSIGNMENT (`INSERT 1.239` into
`numeric(9,2)` is `1.24` there, verified live — this is where a value STORE
parts company with a COMPARISON, which keeps the dropped digits as a residual),
spells a float box through its shortest round-trip text at the width the box
arrived in, treats an integer box as the already-unscaled carrier of §4 above,
and holds every result to the declared precision with PostgreSQL's own message
("a field with precision 9, scale 2 must round to an absolute value less than
10^7"). `internal/storage/ingest.checkType` calls the same function at the
ingest boundary so a bad row fails its INSERT rather than the buffer's later
flush. Both INSERT executors (`wadjet/dml.go` for the embedded API and pgwire,
`internal/server/server.go` for the HTTP server) and COPY reach it through
`ingest`; the stage DAG has no ingest path of its own.

The GRAMMAR moved with it. `batch.DecimalTextAt`, `batch.DecimalSpecialText`
and `batch.DecimalSpecialValueError` now read through
`parquet.DecimalTextParts` / `parquet.DecimalSpecialText` /
`parquet.DecimalSpecialValueError`: `batch` imports `parquet`, so the lower
package is the only place ONE accept-set can sit — the reason `ParseDateDays`
lives there too. `batch.TestDecimalGrammarMatchesBatch` is the gate that keeps
the two sides one function.

The accept-set is PostgreSQL 17.11's numeric input grammar MINUS digit
separators and radix prefixes, taken from a live transcript. PostgreSQL 16
added both to `numeric_in`, so 17.11 accepts `1_000`, `1_0.5`, `0x10`, `0b101`
and `0o17` where wadjet answers 22P02 — tracked as #634 and deferred. It is a
refusal of input PostgreSQL takes, never a different value for input both
accept. What the two agree on: C whitespace trimmed; `nan` case-insensitive and with NO sign
(`'+NaN'` and `'-NaN'` are 22P02 there); `infinity` and `inf`
case-insensitive with an optional IMMEDIATELY-adjacent `+`/`-`. Nothing is a
prefix match, so `'Infin'` and `'infinit'` stay refused. Two deliberate
non-widenings: a FLOAT BOX that is NaN still refuses at
`kernel.DecimalConstText` (that pair is `numeric <op> double precision`, which
PostgreSQL answers by casting the numeric — a float comparison this path has
no kernel for), and `-'NaN'` stays the compile-time refusal `-'abc'` is,
because PostgreSQL has no reading of an unknown-typed literal under unary
minus either (42725) and the negated text `'-NaN'` is not in the accept-set.

### 7. The 38-digit set-operation cap (#552) is closed as a recorded divergence of item 1

A set operation moves stored values, so item 3's fractional-digit reduction
would drop digits a row actually holds; the only honest answers are the
error (current, pinned by `TestSetOpDecimalCapIsARangeReduction`) or a wider
carrier (item 1's reopen clause). The shape — `DECIMAL(38,0)` beside
`DECIMAL(11,10)` — is the pathological corner of the cap, not a BI query.

**Amended 2026-08-29 (#665): a numeric LITERAL arm is a second trigger, and it
does not need a wide column to reach.** Once the literal carries its spelling's
`(p,s)` (item 2's rule applied to a constant), its SCALE enters the common
type: `SELECT d380 FROM t UNION ALL SELECT 0.1234567890 FROM t` over a
`DECIMAL(38,0)` resolves `DECIMAL(38,10)`, and a `10^30` the column holds
comfortably has no carrier at that scale. PostgreSQL answers all four rows.
This is the same range reduction and the same position — the error, never a
wrapped or silently narrowed value — but the trigger is now a constant a query
writes rather than a second wide column it joins to, so it is more reachable
than the shape this item was written for. `TestSetOpDecimalCapIsARangeReduction`
pins the literal form beside the two-column one.

### 8. A stored declaration with `Precision: 0` is the BARE `DECIMAL`, which is `DECIMAL(38, 0)` — a read-side default, not a refusal

Added 2026-08-29 (#675), for the tables the pre-#647 HTTP and gRPC doors
created. Those doors read only the TypeID out of the type text, so
`DECIMAL(9,2)` was persisted into the manifest as `Precision: 0, Scale: 0`.
#647 fixed the doors; it did not and could not fix the manifests already
written, and `ALTER TABLE` has no `ALTER COLUMN TYPE` to migrate them with.

**Those columns are read as `DECIMAL(38, 0)`.** That is not a new rule
invented for the migration — it is the rule already in the code and already in
the grammar, stated:

- `ParseDecimalParams` defines a bare `DECIMAL` (no parameters) as `(38, 0)`,
  so `Precision: 0` and "the user wrote `DECIMAL`" are the *same* stored
  column; there is nothing in a manifest that distinguishes them.
- `decimalEffectivePrecision` already maps a non-positive precision to 38
  everywhere the writer annotates a leaf, so this is what those files have
  always been written as.

The consequence a reader should expect is scale, not range: a value assigned
to such a column ROUNDS to zero fractional digits, exactly as PostgreSQL's
`numeric(38,0)` does. `12.34` stores 12. That is correct behaviour for the
declaration the manifest carries and the wrong behaviour for the declaration
the operator *typed*, and no read-side rule can recover the second — the
digits were discarded at write time, by the door, before the value reached
storage.

**A refusal was considered and rejected.** Making a `Precision: 0` column an
error at read would take every table created through those doors offline,
including tables whose columns really were declared bare, and would do it for
data that is not corrupt: the stored unscaled integers are exactly what
`DECIMAL(38,0)` means. ADR-0018's principle is that a value the engine cannot
represent fails the WRITE; there is no such value here.

**The remediation is a rewrite, and it is the operator's call**: create a table
with the intended declaration and `INSERT INTO … SELECT` into it. Rounded
fractional digits do not come back — the source table never held them — so this
recovers the declaration, not the data. `TestLegacyPrecisionZeroDecimalReadsAsBareDecimal`
(`internal/server`) pins the reading in both directions so it cannot drift into
an accidental refusal or an accidental different scale.

### 9. A DDL type parameter is spelled with PARENTHESES, and the container spellings are a documented SUPERSET

Added 2026-08-29 (#675, #678 review). Wadjet's `CREATE TABLE` takes a type's
parameters in parentheses, for every parameterized type:

```sql
CREATE TABLE t (
  d  DECIMAL(9,2),
  v  VECTOR(384),
  a  ARRAY(STRING),
  a2 ARRAY(DECIMAL(9,2)),
  r  ROW(a INT64, d DECIMAL(9,2)),
  m  MAP(STRING, DECIMAL(9,2))
)
```

**Angle brackets are a syntax error.** `ARRAY<STRING>` and `MAP<STRING, INT64>`
— the Hive/Trino/Spark spelling — do not parse, and deliberately are not
accepted: `<` and `>` are comparison operators in this grammar, so admitting
them as type brackets would make the lexer's decision depend on parser context,
which is the kind of ambiguity a hand-written recursive-descent parser
(ADR-0003) exists to avoid. `parquet.ResolveColumn` is the single reader of the
parenthesised grammar and `collectTypeParams` the single collector, so there is
one place to change if that trade is ever reopened.

The three container types are a **superset**, not a divergence to fix:
PostgreSQL has arrays (`text[]`) and composite types (`CREATE TYPE`) with
different syntax and different semantics, and no MAP at all, so there is no
PostgreSQL answer to follow here (ADR-0012's rule applies only where there is
one). `DECIMAL(p,s)`/`NUMERIC(p,s)` and the parenthesised `VECTOR(N)` are the
two that DO have or resemble a PostgreSQL spelling, and both match it.

What the DDL grammar can spell, `parquet.DeclaredColumn` must resolve
completely — element types, ROW fields and MAP key/value included, at every
depth. That is item 8's neighbour and the substance of #675: a declaration the
parser accepts and the schema layer half-reads produces a table nothing can
write. `TestEveryDDLDoorResolvesEveryParameterizedType` asserts the three doors
agree byte-for-byte over the full 22-type matrix, and
`TestSQLDeclaredParameterizedTypesRoundTrip` writes and reads one value per
parameterized type through SQL DDL rather than through a programmatic schema —
which is why the defect was invisible for as long as it was.

## Consequences

- One table of rules replaces five independently-derived ones (grouped
  aggregate, set-op DAG, set-op local, window, cast). The local set-operation
  path's `max(precision)` rule (`set_op_schema.go`) and the DAG's rebuilt
  integer part (`set_op_decimal.go`) become one function.
- New kernels: `Int128.Mul` (256-bit intermediate, checked), `Int128.QuoRem`
  with rescale and half-away-from-zero rounding, decimal arithmetic mode in
  `BinOpNumeric`, exact window SUM/AVG including the sliding-frame subtract.
  Landed 2026-08-29 for arithmetic, CAST and the scalar family:
  `kernel.DecimalArithVec` / `DecimalScalarVec` resolve the operand SHAPE once
  per batch and run allocation-free (0 allocs/op over 2048 rows, 15-25
  ns/row), with the boxed path answering the value's rendered TEXT — the same
  box a DECIMAL COLUMN produces, so no consumer of a boxed value needs
  teaching. A vectorized DECIMAL arm REPORTS whether it wrote the batch:
  writing nothing and saying nothing leaves the output vector's zeros
  standing, which reads back as the value 0 on every row.
- The TPC-H benchmark gains a spec-conformant `DECIMAL(15,2)` schema variant.
  It is a correctness gate first (22 queries with decimal arithmetic,
  aggregation, comparison and ORDER BY on both execution paths) and the
  decimal performance baseline second; the FLOAT64 schema stays the
  published-number benchmark until a release decides otherwise.

  **Landed and MEASURED 2026-08-29** (`benchmarks/tpch/schema_decimal.go`,
  `decimal_variant_test.go`, `decimal_perf_test.go`; full numbers in
  `docs/benchmarks/tpch-decimal-baseline-2026-08-29.md`). The prediction above
  — that the cost would be "low, and unmeasurable on the current benchmark
  suite" — is half right, and the half that is wrong is the interesting one.
  At SF1, both fixtures built in one process from the same generator draws and
  the queries interleaved over three repetitions, two independent samples put
  the geomean of DECIMAL/FLOAT64 wall time over the 20 runnable queries at
  **1.0393 and 1.0921**, with bytes at rest **0.9982** in both — an INT64 leaf
  against a DOUBLE leaf, and the scaled integer compresses marginally better.
  The cost is not spread: 12 to 14 of the 20 sit within ±10%, and removing Q01
  alone drops the geomean to **0.9881 / 1.0286**. Q01 pays **2.71× and 3.41×
  wall, 2.33× and 2.57× CPU**, because it is the only query in the corpus that
  multiplies decimals in its inner loop — two 128-bit multiplications and two
  additions per row over 6M rows, then a second SUM at scale 4 and two AVGs
  dividing at scale 6. Q10 is second at 1.32–1.35× and does no decimal
  arithmetic at all: it carries a 16-byte group value and a 16-byte sort key
  where the float fixture carries 8. So the honest statement is that exact
  decimals cost under 10% across a mixed analytics workload — within noise
  once the one arithmetic-bound query is set aside — and about 3× on a
  decimal-multiply-bound loop. **The measurement also produced a lead this
  record did not anticipate**: the allocation OBJECT count, not the byte
  count, is where the two carriers part company. Q01 allocates **1868× as many
  objects** on decimals for 1.81× the bytes (Q06 27×, Q15 21×, Q11 6×). A
  large count of very small allocations is a value being BOXED one at a time,
  which is upstream of the kernels this record measured allocation-free over a
  batch; that is where a future pass should look, and no tuning should precede
  understanding it. The distributed exchange cost is still unmeasured.
  Correctness-wise the variant paid for itself on the first run: #695, #696
  and #697 are three defects the FLOAT64 fixture is structurally unable to
  express, all of them silent or newly loud, and #696's single-process half
  was invisible to the DuckDB gate (a count-only entry) and caught by the
  PostgreSQL wire arm.
- The pg-oracle gains a corpus entry per rule in item 3 (exact comparison
  to `min(scale)`) and per row of item 5's table, the type-matrix gains a
  computed-DECIMAL column class,
  and every `knownBug`/`pins` entry naming #529/#542/#587/#555 must fail —
  deleting them is the proof.
- Deliberate divergences from PostgreSQL, all recorded in ADR-0012 item 12's
  list: digits kept past 38 (item 3); the 38-digit range on stored values
  (item 7); NaN/Infinity not storable (item 6); STDDEV/VARIANCE/CORR/COVAR
  /MEDIAN/PERCENTILE over DECIMAL stay float64 (ADR-0012 item 9); and the DDL
  refusal below.
- **A float box is spelled shortest-round-trip, PostgreSQL's cast uses
  `%.15g`** (added 2026-08-29 with #647). `4611686018427387904::float8::numeric`
  is `4611686018427390000` in PostgreSQL 17.11 and `4611686018427388000` here:
  wadjet keeps the 17 significant digits that identify the float, PostgreSQL
  keeps 15. Shortest-round-trip is the only rendering that names the float it
  came from, and it is what `batch.setCheckedDecimalFloat` already does, so the
  two conversion paths cannot disagree about one value. No SQL surface reaches
  this yet — a float box arrives through the embedded or HTTP API, and
  `CAST(x AS DECIMAL(p,s))` is still item 6's declared-STRING no-op — so the
  CAST evaluator (#555) decides separately whether the SQL cast follows
  PostgreSQL's rendering.
- **DDL refuses a `(p, s)` PostgreSQL accepts** (added 2026-08-29 with #647).
  `parquet.ParseDecimalParams` now holds a declaration to `1 <= p <= 38` and
  `0 <= s <= p`, raising `22023 invalid_parameter_value` in PostgreSQL's own
  message shape ("NUMERIC precision 50 must be between 1 and 38"). PostgreSQL
  accepts `numeric(p, s)` to p = 1000 and a scale from -1000 to 1000, `s > p`
  included, because its numeric is unbounded. Item 1's carrier makes
  `DECIMAL(50,2)` a column no value can satisfy, and the writer's answer to
  one was a 16-byte FIXED_LEN_BYTE_ARRAY leaf annotated `DECIMAL(50, s)` — an
  annotation the payload cannot hold, in a file the Apache implementation
  refuses to open. The scale half of the bound is the PARQUET FORMAT's, not
  wadjet's: its DECIMAL logical type has no form for a negative scale or for a
  scale past the precision. Refusing the DECLARATION is the honest answer; the
  alternative is a column that lies about itself in every file it writes.
  `Precision <= 0` — the in-Go "unconstrained" sentinel, which no DDL now
  produces — reads as 38 on BOTH halves of a column's definition (the physical
  type the writer picks and the annotation it writes), where it used to be an
  INT64 leaf annotated `DECIMAL(38, s)`.
- Three more, added 2026-08-29 when items 3 and 4 were implemented, each
  pinned by a test rather than left to be rediscovered:
  - **The transcendental functions stay float64.** PostgreSQL answers
    `sqrt`/`exp`/`ln`/`log`/`power` over a numeric in numeric. They need an
    exact fixed-point tower, which is a different piece of work from a result
    type — the same reason ADR-0012 item 9 gives for STDDEV. The seven that
    answer in their argument's OWN domain (abs/ceil/floor/round/trunc/sign
    /mod) are exact. Pinned by
    `wadjet.TestTranscendentalFunctionsStayFloat64`.
  - **A numeric literal is an EXACT operand of ARITHMETIC, and float8 as a
    bare projection.** In an expression the literal's spelling IS its (p,s),
    trailing zeros included — `d * 100.0` is scale 3 because the literal
    contributed one, and `0.1 + 0.2` is exactly `0.3`, as PostgreSQL answers.
    A literal PROJECTED on its own (`SELECT 1.5`) still declares FLOAT64, and
    so does a scalar function over nothing but constants (`ROUND(0.5)`);
    closing that is a change to the literal's own declaration at every
    projection, comparison and set-operation arm, and covering half of it is
    worse than covering none — the first cut made `ROUND(-0.5)` decimal while
    `ROUND(0.5)` stayed float, and the two halves of one query disagreed about
    their own type. An INTEGER literal is not a decimal operand at all:
    integer arithmetic owns it, and `7 / 2` must stay 3.
  - **A DIVISION between two CONSTANTS stays float8.** `10.0 / 3` is
    3.3333333333333335 here and numeric there. Item 3's division scale is a
    policy FLOOR of six fraction digits, chosen for column operands whose own
    precision drives it past that; between two narrow literals the floor is
    all there is, so an exact answer would keep SIX digits where the double it
    replaces keeps sixteen. Every other operator is exact at a scale derived
    from the operands' own scales and drops nothing, which is why only this
    one declines.
  - **SUM and AVG over a COMPUTED decimal input clamp the scale to 6.**
    `SUM(d92 + 0.00000005)` answers 11.550000 where PostgreSQL answers
    11.55000020; a bare column and MIN/MAX keep their scale. The aggregate
    takes its input (p,s) from the expression only for the shapes that carry
    `AggSpec.InputPrecision/InputScale`, and item 3's adjustment floor applies
    where it does not. OPEN, not settled — the fix is to carry the
    expression's (p,s) at every aggregate input.
  - **`CAST(text AS DECIMAL)` over WIDE text answers a double.** The bare
    destination declines to name a scale (below), so a 20-digit numeric string
    comes back as 1.2345678901234567e+19 rather than the exact numeric
    PostgreSQL gives. The parameterized spelling is exact from the same text.
  - **`CAST(x AS DECIMAL)` over a FLOAT or TEXT operand stays float8.** A bare
    destination takes the operand's own scale (item 3), and a float has none
    — any fixed choice would either truncate the value or invent digits. The
    VALUE follows the declaration there, which is the correction the first cut
    needed: it produced a decimal box for a TEXT operand under a FLOAT64
    declaration, and the store refused the engine's own value. A destination
    that NAMES its (p,s) is exact from every source.
  - **`%` over a FLOAT operand answers `math.Mod` where PostgreSQL has no
    operator at all** (`double precision % numeric` is 42883). A superset, the
    class ADR-0012 records as acceptable — but only since it stopped
    truncating both operands to integers first, which answered 0 for
    `x % 1.5` and divided by the zero that truncation created for `x % 0.5`.
  - **Every integer spelling computes and is declared INT64.** `CAST(x AS
    INTEGER)` and `int / int` reach a client under int8 where PostgreSQL says
    int4, and `int32 ⊕ int32` answers past 2^31 where PostgreSQL raises
    `integer out of range`. The RANGE each cast spelling names is still
    enforced FROM EVERY SOURCE (22003 past int2's and int4's), so the
    divergence is the OID and the extra values arithmetic accepts, never a
    wrapped one.
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
