# ADR-0012 divergences: Aggregates and windows

Aggregate and window-function divergences: accepted extra types and argument types, declared result types, and refusals of window forms PostgreSQL answers. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**MIN/MAX extensions cannot be gated against PostgreSQL** (E13, E15)
PostgreSQL's `min`/`max` exist over 22 input types enumerated from `pg_proc`; `boolean`, `uuid`, `macaddr`, `bytea` and `record` are not among them, so the extensions over BOOL, UUID, MAC and BYTES have no PostgreSQL answer in any shape. `internal/oracle/typematrix` is their differential coverage (the stage DAG, the kill switches and the pooled/poisoned batch arms); CIDR, IPV6, IPV4 and DECIMAL are gated live in `benchmarks/tpch`'s `net_probe`/`dec_probe` fixtures. BYTES orders bytewise, as PostgreSQL's bytea operators do, and the output declares the input type. ROW was removed from the list (arc BR, #1061) because its declaration differed by arm; it is 42883 `function min(record) does not exist`, PostgreSQL's own refusal, until the aggregate's ROW declaration is carried on the single path. `bool_and`/`bool_or` remain the PostgreSQL-idiomatic boolean spelling.

**A window's MIN/MAX and the grouped form answer under one type** (E16)
`exec.WindowMinMaxType` names every type the engine has, as `exec.minMaxOutputType` does for the grouped form, and the value is chosen in the DECLARED type's order, never by a boxed comparator reading a rendered address or formatted decimal (`internal/engine/exec/compare_boxed.go`). The entry records the grouped form widening INT32 and FLOAT32 to their wider accumulator types; the tip measures `integer` and `real` for both spellings, matching PostgreSQL.

**Window SUM/AVG over integers is exact and declared as PostgreSQL declares** (E17, A01, E18)
`exec.IntegerAccOutputType` is the single rule (`sum(int4) -> bigint`, `sum(int8) -> numeric`, `avg(int*) -> numeric`), asked by `physical.aggIntegerOutputType`, `physical.windowSpecOutputType`, `exec.windowAccOutputType` and `exec.aggIntExact`. `exec.windowExactFrames` accumulates in the Int128 carrier the grouped spelling uses, in every frame form including the sliding frame's exit subtraction and both spilled evaluators. A total the declaration cannot hold is 22003. `exec.TestTheWindowAndGroupedIntegerSumWriteTheSameValue` is the seam between the two producers; `coordinator.TestH2TheWindowDeclaredTypeCensus` asserts PostgreSQL's types and digits on four arms.

**A computed or cast SUM argument is typed by its own width** (A01)
The grouped path walks the argument AST (`physical.aggComputedInputDecl` over `aggInputIsWideInteger`); `logical.WindowExpr` carries `InputExpr` so `physical.windowComputedArgDecl` asks the same functions. One int8 operand makes the whole expression numeric in both spellings. A CAST's width is its TARGET's name (`physical.castTargetIsWideInteger`), since every integer cast lands on INT64 in `nodeDeclaredType`; a non-integer target leaves the integer table. The operator's runtime correction honors a bigint declaration over an int64 carrier because only the plan can tell `SUM(i32 * 1)` from `SUM(i64 * 1)`; a total past it is 22003, never a wrapped number.

**SUM/AVG over REAL** (A02, A03)
`sum(real)` accumulates at float4 width on every arm (grouped, distinct, derived table, windowed) and declares real (OID 700) in both spellings (#950, #1118); `avg(real)` stays double precision, as on PostgreSQL. The three layers that must agree are `physical.windowSpecOutputType`, `exec.windowAccOutputType` and the operator's output-vector guard, with `exec.windowWriteFloat` as the shared writer; `coordinator.TestNDDeclarationsMatchPostgres` holds it. A real total's last digits move with fold order, which is ADR-0013 nondeterminism class 9.

**DISTINCT inside a window call is refused, as on PostgreSQL** (E20)
PostgreSQL 17.11 raises `0A000 DISTINCT is not implemented for window functions`; the engine previously dropped the keyword and answered the raw total. The refusal is raised at `sql.parseWindowFunc`, the one site where a call becomes a window call, so it covers every door and nested spellings; `sql.wrapParseFailure` passes a refusal that carries its own code through with PostgreSQL's sentence once. Gated by `coordinator.TestAWindowFunctionRefusesDISTINCT` on four arms.

**Network types under SUM/AVG arithmetic** (A06)
`expr.operandIsInt` and `physical.intArithAllInt` once kept PORT and PROTOCOL on the float path, so `c_proto / 2` answered 127.5 and grouped `SUM(c_proto)` answered NULL. Arithmetic over a port is int4 arithmetic: `physical.intArithColumnType` is the set both kernel and declaration read, and `expr.NumericDomainResult` answers ABS and MOD in the integer domain. DATE, TIMESTAMP and DURATION stay on float64 in both spellings; DURATION declaring bigint on the wire is a filing candidate for `numeric`.

**A window function's argument list is read once, as PostgreSQL types it** (r18, r19)
A bare literal VALUE argument is materialized as a `__winkey_N` column like any expression (`physical.resolveWindowKeys`); before 2026-09-29 it was skipped, so `SUM(2.5) OVER ()` read no column and answered NULL (#1394). The INTEGER argument (LAG / LEAD's offset, NTILE's and NTH_VALUE's n) is read by `sql.WindowIntegerArgument` at the parser and again at `physical.windowExecColumn`: an int4 literal, signed (`-2147483648` is one literal, as PostgreSQL's grammar folds it), quoted, cast to a 4-byte integer, NULL, or a constant expression folded by the planner's constant fold (`logical.constantArg` + `expr.Compile`, the table-function arguments' fold) and typed by its operands as PostgreSQL types it (a bigint or numeric operand or result 42883, an int4 result past int4 22003); the operator honors 0 as the current row, a negative offset as the other direction and NULL as NULL on every row, and raises 22014 / 22016 for a non-positive n per evaluated partition (#1399). An offset past the input's edge answers the default without allocating by the offset (the spilled streamer's look-back ring is sized only within the input's row count). A per-row integer argument — a column, an expression over one, a subquery — is refused 0A000 (r19); a bound parameter is read as the literal it renders to (r20). Gated by `coordinator.TestArcWAWindowArgumentsEveryArm` on five arms, `exec.TestArcWAWindowArgumentsOnTheSpilledPaths`, `exec.TestArcWALagLeadOffsetPastTheInputAllocatesNoRing` and `pgwire.TestAWindowArgumentParameterAnswers`. Over a GROUP BY the list is respelled to the aggregate's outputs one argument at a time (`logical.respellWindowArguments`) and every argument's aggregates are computed; before 2026-10-02 the whole list was parsed as one expression, so `LAG(SUM(b), 2)` read offset 1, `NTH_VALUE(SUM(b), 2)` raised 22016 and an aggregate in LAG / LEAD's default was never computed (`coordinator.TestArcWDWindowDefaultEveryArm` agg/*).

**LAG / LEAD's result is the common type of the value and the default** (r21, r22)
PostgreSQL declares `lag(anycompatible, integer, anycompatible)`. `physical.lagLeadWidening` is the one reading: the declaration (`physical.windowSpecOutputType`) is `expr.CommonDeclType` over the value's and the default's declarations — the fold CASE and COALESCE arms declare through — with `batch.TemporalCommonType`'s DATE → TIMESTAMP rung in front; `physical.resolveWindowKeys` materializes the default, and the value where it widens, as `__winkey_N` columns CAST to that type; `exec.WindowColumn.LagLeadDefaultCol` names the default's column on the single-process pipeline, the DAG stage spec, the wire message and the worker, and the columnar, spilled-partition and streaming evaluators copy the current row's default out of it. `LAG(b, 1, 2.5)` over a bigint is numeric and answers 2.5 on the rows the default fills; a column default reads the row it fills (`LAG(b, 1, d)` over a double is double precision); a quoted literal takes the value's type (`'7'` is 7, `'a'` is 22P02); a pair of different classes (`LAG(s, 1, 2.5)` over text, `LAG(bo, 1, 7)` over boolean, a DATE value with an integer default) is PostgreSQL's 42883 `function lag(text, integer, numeric) does not exist`, raised by the binder (`physical.refuseLagLeadDefaultType`). Until 2026-10-02 the default was a float64 or its SQL text written into a vector of the value's type: 2.5 truncated to 2, a text, column, CAST or `1 + 1` default failed the write, and a DATE value's default answered NULL (#1435, #1436). A numeric result carries one scale per column (numeric-decimal r18): `LAG(b, 1, 2.5)` prints `10.0` where PostgreSQL prints `10`.

**The window refusal replaces a crash** (E80)
`exec.ParseWindowFunc` returned `(WinRowNumber, false)` for an unknown name and the planner discarded the flag, so 23 of 28 aggregates panicked with an index-out-of-range. `exec.RefuseUnsupportedWindowFunc` is raised by both doors (`physical.refuseUnwindowable` single-process, the worker's fragment builder on the DAG). The gate walks `plansql.IsAggregate`, so a new aggregate joins it and a name that gains a window form fails it until the list is edited.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `SELECT AVG(c_i32) FROM t` | 7497.6449875724937862 numeric | 7497.6450 numeric (no typmod; scale 4 for integer input, min(s+4,38) for DECIMAL(p,s)) | — | value divergence | — · P001 | — | — |
| **r2** `SELECT STDDEV(d) FROM t` | numeric result computed in numeric | result computed in float64 (STDDEV, VARIANCE, CORR, COVAR, MEDIAN, PERCENTILE over DECIMAL) | — | value divergence | — · P002 | — | — |
| **r3** `SELECT SUM(d) FROM so -- DECIMAL(38,0) rows +9e37, +9e37, -9e37` | 90000000000000000000000000000000000000 numeric | ERROR 22003 SUM over a DECIMAL column overflowed the 128-bit exact accumulator (measured) | 22003 | refusal | — · P069 | — | — |
| **r4** `SELECT SUM(b * 1), SUM(ABS(b)) FROM sb -- b BIGINT rows 9e18, 9e18` | numeric 18000000000000000000 | numeric 18000000000000000000 for both (measured); an argument whose width is not inferred stays bigint and raises 22003 | 22003 | documented gap | 2026-09-08 · P070, [A01](../0012-amendments.md#a01) | #987 | — |
| **r5** `SELECT SUM(c_port), AVG(c_port) FROM du` | no PORT type; over the int4 the wire declares: bigint, numeric | bigint 545, numeric 181.6667; same rule windowed and under arithmetic (c_proto / 2 truncates) | — | kept superset | 2026-09-07 · [E21](#e21), [A06](../0012-amendments.md#a06), P090 | #953, #1000 | `coordinator.TestH2TheWindowDeclaredTypeCensus`, `pgwire.TestAComputedIntegerWindowArgumentDeclaresPostgresOID` |
| **r6** `SELECT SUM(dur), AVG(dur), STDDEV(dur) FROM du -- dur DURATION, wire bigint` | no DURATION type; sum over the int8 the wire declares is numeric; interval has no STDDEV | double precision 16200000000000, 5400000000000, 1800000000000 (measured; window SUM also float8) | — | value divergence | 2026-09-14 · [A06](../0012-amendments.md#a06), P090 | #1000 | `coordinator.TestH2TheWindowDeclaredTypeCensus` |
| **r7** `SELECT mode(x), percentile_cont(0.5, x), percentile_disc(0.5, x) FROM t` | ERROR 42809 (plain call of an ordered-set aggregate) | answers over numbers; MEDIAN and QUANTILE_* also answer | — | kept superset | — · P090 | #1249 | — |
| **r8** `SELECT STRING_AGG(c_bool, ',') FROM t` | ERROR 42883 | text of each value (BOOL, numbers, network values, UUID, DATE); TIMESTAMP or a container raises 42883 | — | kept superset | — · P090 | #1249 | — |
| **r9** `SELECT STRING_AGG(c_bytes, ',') FROM t` | answers (string_agg over bytea) | ERROR 0A000 | 0A000 | refusal | — · P090 | #1249 | — |
| **r10** `SELECT MIN(c_bool), MAX(c_uuid), MIN(c_mac) FROM t` | ERROR 42883 function min(boolean) does not exist (likewise uuid, macaddr) | the extreme in the type's defined order; MAP and VECTOR also accepted; ROW is 42883 as on PostgreSQL | — | kept superset | 2026-08-25 · [E13](#e13), P089 | #569, #1061 | — |
| **r11** `SELECT MIN(c_bytes) FROM t` | ERROR 42883 function min(bytea) does not exist | bytewise minimum, declared bytea (OID 17) | — | kept superset | 2026-08-25 · [E15](#e15), P089 | #570 | `ByteaMinMax (pgwire wire-arm error list)` |
| **r13** `SELECT MEDIAN(DISTINCT a), APPROX_DISTINCT(DISTINCT a), MIN_BY(DISTINCT a, b) FROM t` | ERROR cannot use DISTINCT with WITHIN GROUP; no approx_distinct or min_by | deduplicated answers | — | kept superset | 2026-09-02 · [E51](#e51), P096 | #703 | — |
| **r14** `SELECT STDDEV(x) OVER (PARTITION BY g ORDER BY x) FROM t` | answers (any aggregate is a window function) | ERROR 0A000 listing the sixteen window functions (aggregates: SUM, COUNT, AVG, MIN, MAX) | 0A000 | refusal | 2026-09-08 · [E80](#e80), P142 | #965 | `wadjet.TestAnAggregateWithNoWindowFormRefusesRatherThanCrashing` |
| **r15** `SELECT g, COUNT(*) FROM ob GROUP BY g ORDER BY COUNT(*) * 2` | answers, sorted | ERROR 0A000 an aggregate expression that is not itself a select item cannot be sorted on (measured) | 0A000 | refusal | — · P151 | — | — |
| **r16** `SELECT id, name FROM t GROUP BY id` | answers when id is the PRIMARY KEY; ERROR 42803 otherwise | ERROR 42803 (no primary keys, so no functional-dependency relaxation) | 42803 | documented gap | 2026-08-25 · [E44](#e44) | #590 | — |
| **r17** `SELECT g, x FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY x) = 1` | ERROR (no QUALIFY clause) | rows filtered after windows, DuckDB 1.1.3 semantics | — | kept superset | — · P083 | #1076 | — |
| **r18** `SELECT FIRST_VALUE('b') OVER (ORDER BY id), LAG(NULL) OVER (ORDER BY id) FROM t` | ERROR 42804 could not determine polymorphic type because input has type unknown (likewise LAST_VALUE, NTH_VALUE, LEAD) | 'b' as text on every row; NULL on every row | — | kept superset | 2026-09-29 · [WA](../0012-amendments.md#2026-09-29-a-window-functions-argument-list-arc-wa-1394-1399) | #1394 | `coordinator.TestArcWAWindowArgumentsEveryArm` (arg/{first,last,nth}_value, lag, lead × text, null) |
| **r19** `SELECT LAG(x, o) OVER (ORDER BY id), LAG(x, o + 1) OVER (ORDER BY id), NTILE(o) OVER (ORDER BY id), LAG(x, (SELECT 1)) OVER (ORDER BY id) FROM t` | evaluates the argument: LAG / LEAD read a per-row offset per row, NTILE / NTH_VALUE read it once per partition | ERROR 0A000 the integer argument of lag must be a constant here; a constant expression is folded and answers (`LAG(x, 2 - 1)`), except `LAG(x, '1' + 1)`, typed numeric by the fold: 42883 where PostgreSQL reads 2 | 0A000 | refusal | 2026-09-29 · [WA](../0012-amendments.md#2026-09-29-a-window-functions-argument-list-arc-wa-1394-1399) | #1399, #1440 | `coordinator.TestArcWAWindowArgumentsEveryArm` (off/*/{col,colexpr,subq,q_plus}, n/*/{col,colexpr}) |
| **r20** `PREPARE p(int8) AS SELECT LAG(x, $1) OVER (ORDER BY id) FROM t; EXECUTE p(1)` (likewise `text`, `numeric`) | ERROR 42883 function lag(bigint, bigint) does not exist | LAG(x, 1): the parameter is read as the literal it renders to | — | kept superset | 2026-09-29 · [WA](../0012-amendments.md#2026-09-29-a-window-functions-argument-list-arc-wa-1394-1399) | #1399, #1439 | `pgwire.TestAWindowArgumentParameterAnswers` (offset/{int8,text,numeric}/1) |
| **r21** `SELECT LAG(b, 1, 10 / (id - 2)) OVER (ORDER BY id) FROM t` | evaluates the default only on the rows it fills: -10, 10, 20, NULL, 40, 50 | ERROR 22012 division by zero: the default is materialized as a column and evaluated on every row, so a default that raises on a row it does not fill raises the query | 22012 | documented gap | 2026-10-02 · [WD](../0012-amendments.md#2026-10-02-a-lag--lead-default-widens-the-result-arc-wd-1435) | #1435 | `coordinator.TestArcWDWindowDefaultEveryArm` (gap/default_every_row) |
| **r22** `SELECT LAG(b, 1, 'a') OVER (ORDER BY id) FROM t WHERE id > 100` (no row) | ERROR 22P02 invalid input syntax for type bigint: "a" (the unknown literal is coerced when the query is planned) | no rows: the default is coerced when a row reads it; over rows it is 22P02 as on PostgreSQL. A quoted literal default of an ARRAY value raises `cannot store string into ARRAY vector` (no SQLSTATE) where PostgreSQL raises 22P02 malformed array literal | — | kept superset | 2026-10-02 · [WD](../0012-amendments.md#2026-10-02-a-lag--lead-default-widens-the-result-arc-wd-1435) | #1435 | `coordinator.TestArcWDWindowDefaultEveryArm` (gap/no_rows_text, type/*/array/{text,qnum}) |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E13

ADR lines 568-596. Catalog rows: r10. Stated in [Mechanisms](#mechanisms).

- **MIN/MAX over BOOL, UUID, MACADDR and BYTEA.** (Widened
  2026-08-25, #569: BOOL was the only one recorded, and the rest are the
  same class. ROW LEFT this list 2026-09-22, arc BR, #1061: its value was
  a whole-row lexicographic extreme, but the field path over it declared
  STRING on the single-process arms and FLOAT64 on the DAG, and the plain
  aggregate declared a ROW with no fields on one arm and with them on the
  other — an answer that differs by arm. It is 42883 `function min(record)
  does not exist`, PostgreSQL's own refusal; re-admitting it needs the
  aggregate's ROW declaration carried on the single path and the field
  path over an aggregate slot typed from it.) PostgreSQL's `min`/`max` are defined over exactly 22 input
  types, enumerated live from `pg_proc` on postgres:17-alpine:
  `anyarray`, `anyenum`, `bigint`, `character`, `date`, `double
  precision`, `inet`, `integer`, `interval`, `money`, `numeric`, `oid`,
  `pg_lsn`, `real`, `smallint`, `text`, `tid`, `time`/`timetz`,
  `timestamp`/`timestamptz`, `xid8`. `boolean`, `uuid`, `macaddr` and
  `bytea` are NOT among them — each errors with "function min(...) does
  not exist" — and neither is `record`: `min(ROW(…))` errors the same way
  (verified live). The four are EXTENSIONS, not divergences PostgreSQL
  took a position on. `bool_and`/`bool_or` remain available and are still the
  PostgreSQL-idiomatic spelling for the boolean question.

  The consequence for the gates is the part worth writing down: those
  four types cannot be gated against PostgreSQL at all, in any shape —
  grouped, windowed or otherwise. `internal/oracle/typematrix` is their
  differential coverage (wadjet against itself across the stage DAG, the
  kill switches and the pooled/poisoned batch arms), and the four
  PostgreSQL DOES have among wadjet's network types — CIDR, IPV6 and IPV4,
  all of which map onto `inet`, plus DECIMAL onto `numeric` — are gated
  live in `benchmarks/tpch`'s `net_probe`/`dec_probe` fixtures.

### E15

ADR lines 607-619. Catalog rows: r11. Stated in [Mechanisms](#mechanisms).

- **MIN/MAX over BYTES.** (Corrected 2026-08-25, #570. The original said
  this matched "PostgreSQL's own `min(bytea)`", which does not exist:
  verified live, `min(bytea)` raises "function min(bytea) does not
  exist", exactly as `min(boolean)` does.) So this is the same kind of
  deliberate extension the BOOL bullet above is, over a type whose order
  wadjet defines anyway — bytewise, which is what every bytea comparison
  uses and what PostgreSQL's own `bytea` operators use. The output type
  still follows the INPUT type and declares bytea (OID 17), which is what
  the extension has to do to be self-consistent; early declared-schema
  code guessed STRING for every MIN/MAX before the input-typed fix.
  `ByteaMinMax` in the wire arm's error list pins it, so the claim is
  checkable rather than remembered.

### E16

ADR lines 620-633. Stated in [Mechanisms](#mechanisms).

- **A window's MIN/MAX declares its input's type too.** (Added
  2026-08-25, #569.) `MIN(c) OVER (…)` and `MIN(c) … GROUP BY g` are the
  same question asked twice and must answer under the same type, so
  `exec.WindowMinMaxType` names every type the engine has, exactly as
  `exec.minMaxOutputType` does for the grouped form. They differ on TWO
  types, INT32 and FLOAT32: the grouped aggregate widens INT32 to INT64
  and FLOAT32 to FLOAT64 because its accumulator is the wider type, while
  the window copies the value and keeps INT32 and FLOAT32 — which are
  PostgreSQL's own answers, `min(integer)` returning `integer` and
  `min(real)` returning `real`.
  Choosing the value in the DECLARED type's order is the other half of the
  same rule; a boxed comparator that reads a rendered address or a
  formatted decimal is not that order (`internal/engine/exec/
  compare_boxed.go`).

### E17

ADR lines 634-711. Stated in [Mechanisms](#mechanisms). Moved to the log: [A01](../0012-amendments.md#a01).

  *(Moved to the amendment log: [A01](../0012-amendments.md#a01).)*

### E18

ADR lines 712-795. Stated in [Mechanisms](#mechanisms). Moved to the log: [A02](../0012-amendments.md#a02), [A03](../0012-amendments.md#a03).

- **A window SUM/AVG over an INTEGER column answers in float64 — wrong
  DIGITS, not only a wrong declaration.** (Added 2026-09-04, #813, arc F1;
  CORRECTED 2026-09-05 after the arc's round-1 review, which measured what
  the first version of this entry asserted without measuring. CLOSED
  2026-09-07 — see the entry above.) PostgreSQL
  declares `sum(int4) over ()` bigint and `sum(int8) over ()` /
  `avg(int) over ()` numeric, and since #784 the GROUPED spelling of each
  answers exactly that — so wadjet's two spellings of one question
  disagree, which is the thing `windowSpecOutputType`'s own comment says
  must not happen.

  The disagreement is not confined to the type. `exec.windowAccOutputType`
  gives an integer input a FLOAT64 accumulator, so past 2^53 the window
  spelling loses digits the grouped spelling keeps. Measured over the
  `numwidth` fixture, whose `w_i64` deliberately carries values past that
  range, against live postgres:17-alpine:

  | query | wadjet, all arms | PostgreSQL 17 |
  |---|---|---|
  | `AVG(w_i64) OVER ()` | `1000800157666874.2` float8 | `1000800157666874.2222` numeric |
  | `AVG(w_i64)` (grouped control) | `1000800157666874.2222` DECIMAL(38,4) | the same |
  | `SUM(w_i64) OVER (ORDER BY … ROWS …)`, row 4 | `9007199271518226` | `9007199271518227` |
  | `SUM(w_i64)` (grouped control) | `9007201419001868` DECIMAL(38,0) | the same |

  A VALUE divergence is never allowed by this ADR, and this one was not
  allowed — it was RECORDED, pinned fail-on-agree, and deferred with its
  mechanism, because the repair is an exact integer accumulator in the
  window operator and not a declaration. Declaring the exact type over the
  float carrier would have been the #361 silent-write class on top of it.
  The mechanism the deferral named is the one that shipped, and #987 added
  the fact the filing did not have: the float error is ORDER-DEPENDENT, so
  the census's own `CAST(SUM(int8) OVER () AS BIGINT)` cell answered
  9007201419001864 on about one routed-DAG run in twenty and
  9007201419001868 on the rest. A wrong number that is not even the same
  wrong number each time.

  Pinned on the VALUE, not on the declaration, in
  `coordinator.TestF1AWindowDeclaresTheSameTypeThroughADerivedTable` — the
  cells asserted the float64 digits wadjet answered and named PostgreSQL's,
  so they failed the day the accumulator became exact, and deleting them is
  the fix's proof.

  CENSUSED 2026-09-06 by arc H2, which replaced those two sampled pins with
  the full cross of five window aggregates and six numeric widths over
  `numwidth`, on all four arms, in
  `coordinator.TestH2TheWindowDeclaredTypeCensus`. Each divergent cell
  names PostgreSQL's own answer beside wadjet's and each agreeing cell is
  asserted as a control, so the deferral is measured rather than sampled
  and the eventual fix's proof is deleting a table of cells. The census
  found one width the filing does not name: a FLOAT32 input takes the same
  float64 accumulator, so `SUM(real) OVER ()` declares float8 and answers
  `1.67772251e+07` where PostgreSQL declares real and answers
  `1.6777224e+07` — and where wadjet's OWN grouped spelling declares
  FLOAT32 and answers `1.6777226e+07`. Three answers to one question, so
  the repair has to settle the grouped side too.

  *(Moved to the amendment log: [A02](../0012-amendments.md#a02).)*

  *(Moved to the amendment log: [A03](../0012-amendments.md#a03).)*

### E20

ADR lines 903-927. Stated in [Mechanisms](#mechanisms).

- **`DISTINCT` inside a window call is REFUSED, not answered.** (Added
  2026-09-07, #987 review P4.) PostgreSQL 17.11 does not implement the
  feature and says so: `ERROR: 0A000: DISTINCT is not implemented for
  window functions`. Wadjet dropped the keyword — both structures that turn
  a parsed window call into a plan build the argument list from the
  argument NODES and never read `FuncCallNode.Distinct` — so
  `SUM(DISTINCT c_proto) OVER ()` answered 621435, the RAW total, where the
  grouped `SUM(DISTINCT c_proto)` answers 32640. This is not a divergence
  entry: it is wadjet following PostgreSQL, recorded because the previous
  behaviour was a plausible wrong number rather than a missing feature, and
  "loud beats plausible" is the rule that decides it. Refused at
  `sql.parseWindowFunc`, the one site where a call becomes a window call,
  so it covers every door and both the bare and the nested spelling; gated
  by `coordinator.TestAWindowFunctionRefusesDISTINCT` on four arms.

  The MESSAGE is PostgreSQL's sentence, once (amended 2026-09-08, #987
  review P3). It arrived as `parsing SQL: parsing SQL: DISTINCT is not
  implemented for window functions (sum)`, because the refusal is raised
  inside the recursive descent and `parseDispatch` prefixed everything that
  came out of it. `sql.wrapParseFailure` applies to the TEXT the rule
  `sql.Parse` already applies to the SQLSTATE — an error that carries its
  own code keeps it — so a parse FAILURE still gets a stage label and a
  deliberate REFUSAL passes through as written. The single remaining label
  is the door's own and is a recorded difference between the doors
  (`server/http_door_sqlstate_test.go`).

### E21

ADR lines 928-978. Catalog rows: r5. Moved to the log: [A06](../0012-amendments.md#a06).

- **`SUM`/`AVG` over PORT and PROTOCOL follow `int4`'s result types.**
  (Added 2026-09-07, #953, arc K2.) PostgreSQL has neither type, so this is
  an EXTENSION rather than a divergence — but it is not a free choice
  either: since #834 both DECLARE `integer` (OID 23) on the wire, so a
  client sees an int4 column and `sum(<that column>)` has exactly one
  PostgreSQL answer, `bigint`. `AVG` is `numeric(38,4)`, as it is for int4.
  Both spellings, grouped and windowed, take the same rule from
  `exec.IntegerAccOutputType` — for a **bare** argument. Under ARITHMETIC
  they do not, and that is a recorded GAP rather than a rule (added
  2026-09-08, #987 review round 3, P1): `SUM(c_port * 1)`,
  `SUM(ABS(c_proto))` and `AVG(c_proto * 1)` answer float8 in BOTH
  spellings, where the bare column answers bigint and numeric(38,4).

  *(Moved to the amendment log: [A06](../0012-amendments.md#a06).)*

### E44

ADR lines 1856-1868. Catalog rows: r16.

- **The grouping rule has no functional-dependency escape here.** (Added
  2026-08-25, #590.) PostgreSQL refuses a non-aggregated SELECT / HAVING /
  ORDER BY expression that is not one of the grouped expressions — 42803 —
  with one relaxation: a column FUNCTIONALLY DEPENDENT on a grouped
  PRIMARY KEY is allowed, so `SELECT id, name FROM t GROUP BY id` works
  when `id` is the primary key and fails when it is not. Wadjet has no
  primary keys and no unique constraints, so there is nothing for the
  relaxation to apply to and every such reference is refused. This is the
  STRICTER end of PostgreSQL's own rule, not a divergence from it: every
  query wadjet refuses here, a PostgreSQL table without the matching
  primary key refuses too. Should key constraints ever arrive, the
  relaxation arrives with them rather than being invented separately.

### E51

ADR lines 2002-2014. Catalog rows: r13.

- **DISTINCT is accepted for aggregates PostgreSQL refuses it for.** (Added
  2026-09-02, #703.) Wadjet honours `AGG(DISTINCT x)` for every aggregate
  its parser accepts it on. Three of those PostgreSQL either refuses or does
  not have: `MEDIAN(DISTINCT a)`, `APPROX_DISTINCT(DISTINCT a)` and
  `MIN_BY(DISTINCT a, b)` — PostgreSQL spells the first with `WITHIN GROUP`
  and answers `cannot use DISTINCT with WITHIN GROUP`, and has neither of
  the others. A strict SUPERSET: an error there, an answer here, never a
  different value for a query both engines accept. The DISTINCT semantics
  for the aggregates PostgreSQL DOES accept it on are PostgreSQL's, values
  and ordering both — `STRING_AGG(DISTINCT s, ',')` sorts the distinct
  values, which is what PostgreSQL's dedup produces and what wadjet emits
  since #703's review round.

### E80

ADR lines 3192-3220. Catalog rows: r14. Stated in [Mechanisms](#mechanisms).

- **An aggregate with no window form is REFUSED (0A000) where PostgreSQL
  answers.** (Added 2026-09-08, arc A1, #965.) In PostgreSQL any aggregate
  may be used as a window function; here the window operator implements
  sixteen — ROW_NUMBER, RANK, DENSE_RANK, SUM, COUNT, AVG, MIN, MAX, LAG,
  LEAD, FIRST_VALUE, LAST_VALUE, NTILE, PERCENT_RANK, CUME_DIST, NTH_VALUE
  — and every other name is refused in one sentence that lists those.

  It is recorded here because the alternative shipped for a long time and
  was worse than a divergence. `exec.ParseWindowFunc` answers
  `(WinRowNumber, false)` for a name it has no arm for and the planner
  DISCARDED the second value, so the plan reached the operator as
  ROW_NUMBER with an output vector typed for the function nobody
  recognized. Census over the 28 names `plansql.IsAggregate` accepts,
  spelled `<agg> OVER (PARTITION BY g ORDER BY x)`, measured 2026-09-08 on
  the v0.18.64 tip: 5 answered right (the five that have a window form), 23
  PANICKED — "internal error in pipeline: runtime error: index out of range
  [0] with length 0", ADR-0019's boundary failing the query with no
  SQLSTATE a client can act on — and 0 answered wrong. Loud beats plausible,
  but a class beats a crash: the refusal is `exec.RefuseUnsupportedWindow
  Func`, raised by BOTH doors (`physical.refuseUnwindowable` on the
  single-process plan, the worker's fragment builder on the DAG) so one
  sentence and one SQLSTATE reach a client whichever path planned the
  query. Gated in
  `wadjet.TestAnAggregateWithNoWindowFormRefusesRatherThanCrashing`, which
  walks `plansql.IsAggregate` rather than a list, so a new aggregate joins
  the gate for free and cannot ship with the crash — and which fails if a
  name in its refusal list acquires a window form, so lifting the
  divergence is a deliberate edit.
