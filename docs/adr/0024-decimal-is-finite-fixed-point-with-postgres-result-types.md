# ADR-0024: DECIMAL is a finite 128-bit fixed-point type that follows PostgreSQL's result-TYPE rules

Status: Accepted (2026-08-29, opening the numeric-parity arc after v0.18.4).
Extends ADR-0012 items 9 and 12, which settled the aggregate and set-operation
halves of this question piecemeal; this record settles the whole type. Amended
2026-09-03 (#712) with the measured door census for the declared-precision
band: every door where the `(p, s)` is a DECLARATION already enforces it and
agrees with PostgreSQL, the shapes the issue named as unguarded are the ones
PostgreSQL leaves UNCONSTRAINED, and the real residual is the opposite
divergence — see "The second of those two, measured 2026-09-03".

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
    SUM(int2/int4) → bigint ; SUM(int8) → numeric ; AVG(int*) → numeric
    numeric ⊕ integer   → numeric      (an integer is DECIMAL(10,0) / (19,0))
    numeric ⊕ float8    → float8       (float8 is the category's preferred type)
    int ⊕ int           → int          (truncating division, as PostgreSQL)
    CASE / COALESCE / NULLIF / GREATEST / LEAST / IF over any DECIMAL branch → DECIMAL
    MIN/MAX/FIRST_VALUE/LAG/… keep the input's (p,s); SUM → (38,s); AVG → (38, min(s+4,38))
    windowed SUM/AVG answer what the grouped ones answer, exactly

**The INTEGER half of the choice rule landed 2026-08-29 (#695), and the BOX is
what it took.** The type fold was the easy half: an integer contributes its
whole range at scale 0 and a numeric LITERAL its own spelling, so
`GREATEST(d92, 100)` is DECIMAL(9,2) while `GREATEST(d92, i64)` is
DECIMAL(21,2). What blocked it for a release was the value: an integer box
written into a DECIMAL vector is the ALREADY-SCALED carrier of §4 below, so
`GREATEST(d, 5)` would have read back as 0.05 and `Vector.SetValueChecked`
refuses one outright. A choice whose arms fold to a DECIMAL therefore RENDERS
its chosen box as the value's TEXT — the box a DECIMAL column and exact
arithmetic already produce — and the store resolves it at the output vector's
own scale through the checked parser. Only an INTEGER box is rewritten: a
float box beside a decimal one is a pair the boxed-comparison layer already
classifies, and handing it text would replace the literal's exact source text
(ADR-0012 item 6) with a rounded rendering.

Two limits on a CONSTANT arm, both deliberate. It **contributes** to the fold
and never **triggers** it, so `GREATEST(-2.5, -7.5)` keeps the float8 a bare
numeric literal declares — the deferral recorded under "a numeric literal is
an EXACT operand of ARITHMETIC" below, which this record does not reopen. And
it contributes only when the box `compileLit` built carries its spelling
exactly, which is what leaves
`GREATEST(d_wide, 493827160549382.7160549350)` where it was.

**Where it was is a SILENT float, not a loud refusal, and the first draft of
this paragraph said otherwise.** That expression declares FLOAT64 and ANSWERS
`4.938271605493827e+14` where PostgreSQL answers the literal's digits: past a
double's ~17 significant digits `compileLit`'s box has already lost them, and a
choice hands over whatever box the winning arm produced. Arithmetic over the
same literal is exact because it reads `Lit.Text` (ADR-0012 item 6); giving the
choice constructs the same exact-text path is what would close it. Recorded as
a silent loss of digits rather than described as something safer, and pinned by
`wadjet.TestWideNumericLiteralInAChoiceStaysFloat`.

A DECIMAL beside a FLOAT declares double precision, which is right, and used to
FAIL at the #361 store guard on the rows the decimal wins: the box was that
branch's text and the vector a float one. **That half closed with #724**
(2026-08-30): a choice whose arms fold to a non-DECIMAL number reads a string
box as the number that type names, which is the mirror of the integer rule
above and is `expr.choiceNumberBox`. Two arms can produce such a box — a
DECIMAL column, whose value IS its rendered text, and a QUOTED literal, which
arrives as the characters the query spelled — and GREATEST/LEAST had answered
the second since #646 through `extremumArms.materialize`, which is why nothing
in the corpus saw that COALESCE and CASE had not.
`wadjet.TestDecimalBesideAFloatStaysDoublePrecision` asserts the values now.

**A QUOTED string literal is `unknown` in the fold, and the fold is a LADDER,
not the first argument** (#724, 2026-08-30). PostgreSQL types a quoted literal
`unknown` and resolves it from the other operands; wadjet typed it
`Decl(TypeString), Decided`, so every polymorphic call holding one had a
non-numeric decider, `expr.CommonDeclType` declined the fold, and the call was
declared from `decided[0]` — its FIRST argument. A declaration narrower than
the value the call produces does not narrow that value into the output vector,
it WRAPS it: `GREATEST(bigint, real, double, '1e39')` is double precision in
PostgreSQL and was int64's MINIMUM here, a number from nowhere that ADR-0012
item 6 forbids and item 4 below makes a 22003, and it reached GROUP BY keys
built from the same projection.

So `expr.DeclType` carries `Quoted` (SQL's `unknown`), and the numeric deciders
fold through PostgreSQL's `select_common_type` ladder — INT32 → INT64 → DECIMAL
→ FLOAT32 → FLOAT64, the ladder item 2 already names, with FLOAT32 its own rung
per `setOpWiden` — rather than through "the first decider wins". Three layers
now run one ladder over one composite: `physical.foldArgTypes` for the
plan-time literal refusal (#646), `expr.joinFoldKinds` for the boxed
comparison, and `expr.CommonDeclType` for the declaration. They answer
different questions — can this literal be read at all, which argument wins,
what vector the winner is stored in — and a disagreement between them is a
value narrowed or wrapped on the way out, which is why
`physical.TestDeclaredFoldAgreesWithTheComparisonFold` asserts the first and
third agree over every ordered pair of the six widths.

**A LITERAL contributes its SPELLING to a DECIMAL fold, scale included, and
the QUOTED spelling contributes the same thing** (#724). The scale is max over
the arms — item 12 of ADR-0012's rule for a set operation, and for the same
reason: it is the only choice that moves no value, since a narrower one drops
digits a wider arm holds. `CASE … THEN numeric(9,2) ELSE 0.125 END` needs
scale 3 or the 0.125 has nowhere to go, and PostgreSQL answers 0.125 there.

**What that costs is the RENDERING, and it is recorded** (#764). PostgreSQL
gives a numeric CONSTANT typmod −1, so `select_common_typmod` over a column and
a constant answers −1: the fold is unconstrained numeric and every value prints
at ITS OWN scale — 12.75 for the column's rows, 12.3456789012345 for the
literal's, in one column. A wadjet vector has one scale for the whole column
(§4) and cannot print two, so the columns' rows come back
`12.7500000000000`: the same number, with trailing zeros that track the
literal's fractional length.

The alternative was implemented during #724's review round 1 and reverted with
evidence. Taking the scale from the DECLARED operands alone keeps the columns'
rows byte-identical to PostgreSQL and leaves a SELECTED literal finer than that
scale with nowhere to go, so `CASE WHEN g < 3 THEN numeric(9,2) ELSE 0.125 END`
— 200 rows on PostgreSQL, and answered by this engine since #695 — became a
22003. It failed the pg-oracle corpus and #695's own gate. **A trailing zero is
the same number; a refused query is no answer at all.** Closing the rendering
gap for real needs a DECIMAL projection that prints each value at its own scale,
which is a vector-level change and not a typing one (#764).

One refusal remains and it is the CARRIER, not the scale: a literal past 38
digits has no exact value at any scale an Int128 can hold, so the row that
SELECTS it is a 22003 by item 4. It is a STORE check, per row — the same
composite over a range that excludes that row answers, and so does a WHERE over
it, which projects nothing — so an arm no row selects never costs a query its
answer. When such a literal would push the fold past the carrier the constants
are dropped from it and the DECLARED operands decide alone
(`expr.foldDecimalMetas`), which is what keeps
`COALESCE(numeric(15,2), numeric(38,10), '<forty digits>')` answering on the
rows its two columns supply. `coordinator.TestLiteralScaleInADecimalFold` and
`TestNumericFoldRefusalIsPerRow` hold both halves.

Two more limits, both deliberate. A composite whose every argument is quoted
stays `text`, as PostgreSQL resolves it. And a CONSTANT is folded at
PostgreSQL's rung for its spelling only when there is a TYPED operand to
resolve it against: `CASE … THEN int4_col ELSE 0 END` is `integer` on both
engines, while `GREATEST(0.5, 1.5)` keeps the FLOAT64 a bare numeric literal
declares — the literal deferral recorded above, which #724 does not reopen.

**THE STORE, not the classification, is what makes the box rule hold.** The
runtime fold classifies arms by NODE KIND, and the declared fold takes any arm
whose `DeclType` is INT32/INT64 — so the two disagreed the moment an integer
arm was neither a constant nor a bare column. A `CAST(i AS BIGINT)`, a nested
choice of integers and a registry function declared integer all made the
runtime decline while the plan had already allocated a DECIMAL vector, and the
integer box then raised 22003 for eleven shapes PostgreSQL answers
(projection, GROUP BY, ORDER BY, aggregate input, DISTINCT, window partition
key, set-operation arm, and the choice-above-an-aggregate forms). The
classification learned those kinds, but the rule that keeps it from mattering
is the second one: `batch.Vector.SetComputedChecked` reads an INTEGER box from
an EXPRESSION as a value at scale 0 and scales it, where
`Vector.SetValueChecked` refuses one. The refusal is right for its own callers
— a row→batch adapter's integer box IS §4's already-scaled carrier — and wrong
for an expression, which has no such spelling: a DECIMAL column reference boxes
rendered TEXT and so does exact arithmetic. A node kind this layer has not
learned now costs a narrower declared TYPE, never the query.

**Item 3's p>38 ADJUSTMENT does NOT apply to a choice**, and the reason is item
7's. A choice's result IS one of its operands' stored values, so giving up
fraction digits drops digits a row actually holds: over
`GREATEST(numeric(38,0), numeric(11,10))` the adjustment reduces the scale from
10 to 6 and silently truncates the second column's `0.0000000001`. The scale
therefore stays where `DecimalCommon` puts it and the precision cap alone is
the rule, with a value that has no carrier raising a per-value 22003 at the
store rather than refusing the query at plan time — which is what lets
`GREATEST(numeric(38,30), bigint)` answer for every value that fits, as
PostgreSQL does. Arithmetic is where the adjustment belongs, because a computed
scale is derived rather than carried.

**NULLIF resolves its TYPE over both arguments and its TYPMOD over argument 0.**
PostgreSQL runs `select_common_type` over the pair — they have to be comparable
— while the result is argument 0's value. Folding both questions over the
typmod list made `NULLIF(0, numeric(9,2))` an INT64 column. The widening here
is narrow on purpose: it fires only when the candidate list's answer is not a
DECIMAL and another argument decided one, because folding every argument would
widen `NULLIF(a, b)` to the wider column's scale for a result that is always
a's value, and would re-open the Guessed/Decided contract of #331/#333.

**The AGGREGATE's input needed the same walk the SELECT list got in #529.**
TPC-H Q08 is `SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END)` over a
DERIVED TABLE that computes `volume`, and both pre-aggregate projections — the
single-process one and the `AggSpec.InputType` the worker rebuilds from the
expression text — resolved their column types with `inputColDecls`, which
STOPS at a subquery's Project. So `volume` decided nothing, the CASE declared
its integer ELSE, and the query failed at the store on the first row the
branch fired. Both now use `emittedColDecls`, the walk that crosses a derived
table and the one `declaredOutputSchema` already uses, so the aggregate's
input, the SELECT list and the plan-declared schema answer from one map.

**Residual, and it is not about DECIMAL: on the stage DAG an aggregate whose
INPUT EXPRESSION names a DERIVED column reads that column as NULL.** The
worker builds the pre-aggregate projection from the expression TEXT against
the batch its stage hands it, and that batch carries the SCAN's columns rather
than the derived table's — so `SUM(CASE … THEN volume ELSE 0 END)` sums only
its ELSE branch. Over an INTEGER derived column the DAG answers 0 where the
single-process path answers 2, and a plain rename (`id AS idr`) is enough; no
DECIMAL is involved and it predates this record. EVERY arm is a silent wrong
number, DECIMAL included: the DECIMAL one was briefly loud, because the input's
declared (p,s) reached the worker while the derived column did not and the
ELSE branch's integer box met a DECIMAL vector, and that incidental 22003 went
away once the store learned to read an integer box from an expression as a
value at scale 0. Nothing about the defect changed; one symptom stopped being
visible. Filed as #709 and pinned by
`coordinator.TestAggregateOverADerivedColumnTwoPath`, each pin failing when
the DAG starts agreeing.

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

**Amended 2026-09-02 (#749): the reduction applies to DIVISION only. For
`+ - * %` the precision is capped at 38 and the SCALE IS KEPT.**

The rule above spends fraction digits to buy integer ones. For an operator
whose scale is EXACT — the scale at which the result has no rounding at all —
those are digits the answer has, and spending them produces a correctly
ROUNDED value of the wrong type, which is indistinguishable from an exact one.
Over a `DECIMAL(38,10)` column it made `dw + 1` scale 9 and `dw * 2` scale 8
where PostgreSQL keeps all ten:

    dw      997333333445533.3129445454
    dw + 1  997333333445534.3129445454   was  ...534.312944545
    dw * 2  1994666666891066.6258890908  was  ...066.62588909

Item 4 is what replaces it: a value with no exact carrier at its declared type
is a loud 22003, never a silently narrower one (correctness-fix protocol rule
8, "loud beats plausible"). So an exact operator whose computed precision
exceeds 38 caps the precision and keeps the scale, whenever the scale itself is
one the carrier can declare; a scale past 38 keeps the old reduction, because
there is no exact type to preserve.

DIVISION is untouched. Its scale is `max(6, s1 + p2 + 1)` — a floor this
project chose, not a fact about the operands — so reducing it drops no digit
the answer had.

**What it costs, recorded rather than left to be discovered.** RANGE, and the
carrier's own: at scale s an Int128 holds `38 − s` integer digits.
`DECIMAL(38,10) × DECIMAL(38,10)` is `(38,20)` now rather than `(38,6)`, so a
product past 10^18 raises 22003 where it used to come back rounded to six
fraction digits, and where PostgreSQL's unbounded numeric answers. That is a
query which stopped answering, it is the same trade item 7 makes for the
set-operation cap, and it is pinned in the PostgreSQL corpus as
`WideDecimalSquaredRowCount` under the kind `pgDivergenceCarrier` — a pin that
ratchets, so a rule change that makes wadjet answer it again re-opens this
paragraph. TPC-H's `DECIMAL(15,2)` arithmetic never reaches p > 38, so the
benchmark fixture is unaffected.

`batch.DecimalResultType` is the one function; `batch.TestDecimalResultTypeFollowsADR0024`
carries a row per operator for both sides of the boundary.

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

**"At every value-producing site" includes every POSITION the same expression
can be written in.** (Added 2026-09-03, #841.) An expression has ONE
disposition: `bigint * bigint` overflow is 22003 whether the product is
projected, summed, averaged, taken as a MIN or a MAX, used as a GROUP BY key or
computed inside a window — measured live on PostgreSQL 17.11 for all seven.
`SUM(big * 2)` answered an exact 18446744073709551614 while the projected
`big * 2` raised on the same row, and the cause was not a typing rule but an
algebraic one: `logical.rewriteConstArithAggs` lifts a constant out of an
aggregate — `SUM(x op k)` becomes `agg(x) op k` — which is an identity over
VALUES and not over DISPOSITIONS, because the per-row form can raise and the
lifted form cannot. A rewrite that moves an expression from one position to
another may not move its disposition with it; that pass now declines for the
operand pairs PostgreSQL computes in integers, and carries an `optswitch` kill
switch so the invariance oracle can disable it.

**"Its declared type" means a precision somebody DECLARED, not one a fold
inferred.** (Added 2026-09-05, #712 — measured, and the recorded position is
the opposite of what that issue asked for.) The proposal was to carry the
declared precision on `batch.DecimalColumn` and refuse past `10^p` at
`SetValueChecked` / `SetComputedChecked`. Implemented, it turns

```
SELECT GREATEST(numeric(38,30) column, 100000000)
```

into a 22003, and PostgreSQL 17.11 answers `100000000` under a bare `numeric`
(`pg_typeof` measured on the server): a GREATEST/LEAST/COALESCE/CASE fold is
UNCONSTRAINED there, so there is no `10^p` for the value to exceed. A right
answer turned loud is the one direction ADR-0012 does not permit.

Every site where a precision IS a constraint somebody wrote already enforces it
and already agrees with the server: `CAST(100000000 AS DECIMAL(38,30))` and
`CAST(12.75 AS DECIMAL(3,2))` are both 22003 on both engines,
`exec.DecimalCoerce` enforces the set-operation unified type, and
`parquet.DecimalValueFromBox` enforces the stored column's at ingest.

What is actually wrong is a DECLARATION: the fold caps its result at
DECIMAL(38,30) internally while item 5 already declares typmod −1 for it on the
wire, so the vector carries a bound nothing intends to enforce. The fix worth
making is for a composite's vector to carry NO precision — unconstrained,
matching its own wire declaration — and for the writers to enforce one only
where a CAST, a set-operation unified type or a stored column named it. That is
a change to what `exec.ProjectColumn.Precision` MEANS (a cap, or a hint), and
it is not made here.

### 5. On the wire, a DECIMAL carries the typmod its inputs AGREE on — `select_common_typmod`

PostgreSQL does not gate on "computed". It runs `select_common_typmod` over
the inputs a result is resolved from: the typmod survives when every one of
them carries the SAME one, and the result is unconstrained otherwise.

A BARE COLUMN REFERENCE carries its column's typmod. An aggregate call, a
window function, an operator and every other function call carry
−1 — so one of those anywhere in the fold makes the whole result −1. A CAST
is NOT one of them and this item said it was: see the correction below, which
is now the rule the code implements rather than a divergence it records. The
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

**A JOIN is not one of the constructs that drop it, and it was (#697, fixed
2026-08-29).** A bare DECIMAL column reference in a statement that also
contains a subquery — `WHERE id IN (SELECT …)`, a correlated scalar
subquery, a semi-join to a GROUP BY — went out unconstrained where
PostgreSQL keeps `numeric(15,2)`, TPC-H Q02's `s_acctbal` and Q18's
`o_totalprice` among them. The trigger was never the subquery: it was a JOIN
whose SIDE is a node the declared-type walk had no arm for. `emittedColTypes`
fell through to `inputColTypes` for a `NodeJoin`, that function's join arm
recurses with ITSELF, and it has no Project / Aggregate / Window arm — so a
decorrelated subquery (a Join over an Aggregate) or a plain DERIVED TABLE (a
Join over a Project, no subquery predicate anywhere) made one side nil and
its `left == nil || right == nil` rule nilled the WHOLE map. Every column of
the query lost its declaration, and `declaredTypmod` read "could not resolve
this column" as "this column carries no typmod". A ZERO-ROW result behind any
of those shapes was the same nil showing as the wrong TYPE rather than the
wrong modifier: described from the plan alone, every column went out STRING
(OID 25) — #416's failure mode over the shape #416 did not reach.
`emittedColTypes`, `emittedColDecimal` and `emittedComputedCols` each cross a
join now: the first two merge the sides and drop a name they declare
differently (`inputColTypes`' own rule, with a nil side tolerated rather than
fatal), and the third unions them, which is what keeps a window output, an
aggregate output, arithmetic or a CAST below a join unconstrained now that
the map beneath it resolves at all.

**Correction, and it is now IMPLEMENTED: a CAST that NAMES a (p,s) KEEPS
it.** The first version of this item listed a CAST among the constructs that
carry −1, and postgres 17.11 disagrees — `CAST(a AS numeric(18,4))` and
`a::numeric(9,2)` both describe with their destination's modifier, and only a
BARE `CAST(a AS numeric)` drops to plain numeric. This is the cast's own
typmod being imposed on the result, not `select_common_typmod`, which is why
the arm does not recurse into the operand.

Wadjet sent −1 for both parameterized spellings — `declaredTypmod` had no
CAST arm — while the TYPE side (`castDeclaredDecimal`) already resolved the
destination's (p,s). The two halves of one declaration disagreed, and a JDBC
client read `getPrecision()` as 0 for a column its own query declares
DECIMAL(9,2). Fixed 2026-09-03 (#708): `declaredTypmod` has a `*plansql.CastNode`
arm answering the destination's own (p,s) when it names one and falling through
to unconstrained when it does not. The pin
`wadjet.TestCastTypmodIsUnconstrained` is DELETED — it failed on the fix,
which was its proof — and four wire-corpus entries replace it
(`CastToParameterizedDecimal`, `CastToWiderParameterizedDecimal`,
`CastToParameterizedDecimalColonColon`, and the bare-destination control
`CastToBareDecimalStaysUnconstrained`), because only the WIRE arm can see this
at all: the values agree on every path.

A cast to a type whose modifier wadjet does not send is NOT covered and does
not need to be: `pgwire.TypeMod` answers −1 for every TypeID but DECIMAL, so
a `VARCHAR(n)` or `TIME(n)` destination is unconstrained here and on the wire,
and the two agree. That is a site this record names as uncovered rather than
one it claims (protocol method 9).

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

**Two limits of the CHECKED WRITERS, recorded 2026-08-29 with #695's review.**
Neither is reachable from SQL today and neither is claimed closed. A box of a
type a DECIMAL column cannot take at all — a bool, a `[]byte` — falls through
to `SetValue`, whose `mismatch()` PANICS, so the query boundary reports an
internal error where PostgreSQL raises `42804 datatype_mismatch`; the type fold
declines for every non-numeric arm, so no such box reaches a DECIMAL vector
through a query. And neither writer enforces the DECLARED PRECISION, only the
scale, because `batch.DecimalColumn` carries `Scale` and no precision: a value
inside the Int128 but past the type's own `10^p` band is stored, so
`GREATEST(numeric(38,30), 100000000::bigint)` writes 39 digits under a type
capped at 38. Item 4 makes the declared precision the bound that matters and
the set-operation coercion (`physical.setOpCheckedDecimalText`) is the only
door enforcing it today.

**The second of those two, measured 2026-09-03 (#712): the record was right
about the mechanism and wrong about which way it points, and closing it as
written would be a regression.** The door census below is a live
postgres:17-alpine transcript over `CREATE TEMP TABLE (a numeric(38,30))`
holding 1.5:

| shape | wadjet | PostgreSQL 17.11 |
|---|---|---|
| `INSERT INTO d(a) VALUES (100000000)` | 22003 | 22003 |
| `UPDATE d SET a = 100000000` | 22003 | 22003 |
| `UPDATE d SET a = GREATEST(a,100000000)` | 22003 | 22003 |
| `CAST(100000000 AS DECIMAL(38,30))`, `::DECIMAL(38,30)` | 22003 | 22003 |
| `GREATEST(a, 100000000)` | `100000000.000…0` | `100000000` |
| `LEAST(a, -100000000)` | `-100000000.000…0` | `-100000000` |
| `CASE WHEN true THEN 100000000 ELSE a END` | `100000000.000…0` | `100000000` |
| `a + 100000000` | **22003** | `100000001.500…0` |
| `SUM(a) + 100000000` | **22003** | `100000001.500…0` |
| `SELECT a FROM d UNION ALL SELECT 100000000` | **22003** | 2 rows |

`pg_typeof` on every one of the computed shapes is bare `numeric` — typmod
−1. **PostgreSQL does not constrain a computed expression to a column's
typmod**, so the band exists there only where the type is a DECLARATION: a
store or an explicit cast. Every such door in wadjet already enforces it, all
five agree with PostgreSQL digit for digit, and there is no unguarded store
(`INSERT … SELECT` is unparsed, 42601, so that door does not exist).

The three shapes #712 names as the defect are exactly the ones PostgreSQL
leaves unconstrained, and their VALUES are right today —
`ColumnMeta.WireUnconstrained` already reports typmod −1 for them, so the wire
agrees too. Adding a `10^p` check at `SetValueChecked`/`SetComputedChecked`
would turn three right answers into 22003 and widen an ADR-0012 item 1
violation instead of closing one. It is not shipped.

The residual that IS real points the other way, and it is the
`pgDivergenceCarrier` class §3 already records for `*` and §7 for set
operations, now measured for `+` and for an aggregate result: the fold
SYNTHESISES a constrained `(38,30)` where PostgreSQL is unbounded, and the
arithmetic then enforces that synthesised band as a declaration
(`expr.resolveDecimalMode` → `batch.DecimalResultType` → `DecimalAddAt` →
`decimalAtPrecision` → `numericFieldOverflow`). Closing it means giving the
computed DECIMAL domain the CARRIER bound instead of a synthesised `10^p`
band — representing "unconstrained numeric" in the VALUE domain the way
`WireUnconstrained` already represents it on the wire. That is the numeric
typing layer, not storage, and it is its own arc. `batch.DecimalColumn` gains
no `Precision` field here: with enforcement it is the regression above, and
without it, it is dead weight.

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
and `0o17` where wadjet answers 22P02 — tracked as #634 and deferred.
(Amended 2026-08-29, #646: #634 is CLOSED for the INTEGER family, whose
`kernel.parseIntText` now reads PostgreSQL's whole `pg_strtoint*` grammar, and
it stays open HERE. The two are separate parsers on purpose — the accept-sets
genuinely differ, `'0x1p3'` being a float and neither an integer nor a numeric
— and closing this half means moving `parquet.DecimalTextParts`, the one
function `batch` and the writer share, which is a change to the STORED grammar
and not only to a comparison. ADR-0012 item 13 carries the per-type table.) It is a
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
  the queries interleaved with the pair's ORDER swapped on alternate
  repetitions, two samples put the geomean of DECIMAL/FLOAT64 wall time over
  the 20 runnable queries at **1.1334 and 1.1432**, with bytes at rest
  **0.9982** — an INT64 leaf against a DOUBLE leaf, and the scaled integer
  compresses marginally better. Removing Q01 alone drops it to **1.0674 /
  1.0710**, so the cost is concentrated but not confined: Q01 pays **3.53×
  and 3.96× wall, 2.70×–3.41× CPU**, being the only query in the corpus that
  multiplies decimals in its inner loop (two 128-bit multiplications and two
  additions per row over 6M rows, then a second SUM at scale 4 and two AVGs
  dividing at scale 6), and roughly 7% remains across everything else. Q10 is
  second at 1.46× and does no decimal arithmetic at all: it carries a 16-byte
  group value and a 16-byte sort key where the float fixture carries 8, which
  makes the carrier's WIDTH a second mechanism beside its arithmetic. So the
  honest statement is that exact decimals cost about 13% across a mixed
  analytics workload, about 7% away from the arithmetic-bound query, and about
  3.5× on a decimal-multiply-bound loop.

  **Two methodological findings came with it, and both changed a conclusion.**
  First, the ORDER of the pair matters: three earlier samples that ran float
  first every repetition reported 1.0393, 1.0921 and 1.0803 and made the
  non-Q01 corpus look free, because the arm running second collects the
  benefit of the first one's warm-up. The swap is now part of the protocol
  (ADR-0011), and it also dissolved two apparent decimal WINS — Q12's CPU
  ratio moved from 0.57–0.61 to 0.954–0.988 and Q21's from 0.69–0.85 to
  0.921–0.935 — which is worth remembering as a shape: a ratio that reproduces
  across repetitions is not a real one when every repetition shares the same
  bias. Second, **the allocation OBJECT count, not the byte count, is where
  the two carriers part company**: Q01 allocates **~1871× as many objects** on
  decimals for 1.77× the bytes (Q06 27×, Q15 21×, Q11 6×), reproducing to four
  significant figures across every sample. A large count of very small
  allocations is a value being BOXED one at a time, which is upstream of the
  kernels this record measured allocation-free over a batch; that is where a
  future pass should look, and no tuning should precede understanding it. The
  distributed exchange cost is still unmeasured.

  Correctness-wise the variant paid for itself on the first run: #695, #696
  and #697 are three defects the FLOAT64 fixture is structurally unable to
  express, all of them silent or newly loud. Two lessons about the GATES came
  with them. #696's single-process half was reported green by the decimal gate
  as first written, because that corpus had inherited the float gate's
  count-only relaxation for Q22 — whose stated reason, that borderline rows
  shift with accumulation order, is FALSE on exact fixed-point, where the
  threshold is one exact value. A relaxation is an assertion about the data
  and does not survive a change of carrier just because the SQL is unchanged;
  it is gone from the decimal corpus. And the two-path suite's "single-process
  arm" was the fast-path COORDINATOR, which declines Q15's and Q22's plans and
  answers from the stage DAG — so for exactly the queries that diverged it was
  comparing the DAG with itself. That arm is now the embedded engine.
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
