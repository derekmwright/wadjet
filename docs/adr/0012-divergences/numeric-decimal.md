# ADR-0012 divergences: Numbers and DECIMAL

Numeric and DECIMAL divergences: integer widening, float4 rounding positions, decimal carrier limits, casts and operator spellings PostgreSQL declines. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**Arithmetic over two REALs declares real** (E19, A04)
`real op real` declares real and the float64 carrier's exact sum, difference or product is rounded once into the float4 output vector, which is the correctly rounded float4 answer; `r + 1.0::real` is 1.6777216e+07, `f4 * 10.0::real` is 22003 and `f4 + f4` is 2e+38, as on PostgreSQL (#1117). `batch.FloatRangeError` is the store's range rule, carrying PostgreSQL's `value out of range: overflow` and `underflow`; an infinity that arrives is a value. `real + 1.0`, `real + 1` and `real + float8` stay double precision, `-real` and `sum(real)` over a real expression are real, `avg(real)` is double precision, and `%` is untouched.

**Float4 rounding happens only at a projected store** (A05)
A nested step, a predicate and a CASE condition have no float4 output vector, so the float64 carrier's digits reach the next operator where PostgreSQL rounds at every float4 operator. Closing it means rounding at the operator (an AST rewrite wrapping each `real op real` in `CAST(… AS REAL)`), which must also travel in the stage's emitted SQL text for workers; a half-landed version was backed out. Spelling the CAST by hand answers correctly on all five arms. The set-operation arm is closed: `setOpCastExpr` narrows an arm to real before `setOpWiden` widens it.

**Integer arithmetic and integer CASTs declare bigint** (E78)
A literal that fits int4, MIN/MAX of an int4 (#951) and the bitwise family over int4 (#1018) declare integer. int4 arithmetic and integer CASTs were narrowed and put back because the stage arms type an expression over an aggregate or window slot (`__agg_N`, `__win_N`) from a coarser source than the single-process walk, and one expression with two declarations between the engine's own paths is worse than a stated divergence from PostgreSQL. ADR-0024 §2b carries the rule and the measurements. `CAST(SUM(x) OVER () AS BIGINT)` declares INT64 on all arms; the DAG reaches it by refusing the plan and routing to the coordinator's local pipeline.

**An integer result that does not fit its declared type fails at the store** (E57)
`ABS(<int4 column>)` at -2147483648 raises 22003, as PostgreSQL does. The check is `batch.IntegerRangeError` at `batch.SetValue`, with the exec.FatalEvalPanic contract, not inside ABS: `ColRef.Eval` widens int4 to an int64 box on purpose, and the vectorized ABS arms are unreachable from a projection because the registry declares `abs` float64. `batch.TestInt32StoreKeepsTheNumberOrRefuses` and `pgwire.TestIntegerOutOfRangeReaches22003OnTheWire` gate the seam and the wire.

**CAST to BOOLEAN** (E46)
The int4 rule (0 is false, other values true, NULL is NULL) applies to INT32 and INT64; FLOAT and DECIMAL are refused with 42846, as on PostgreSQL. STRING follows PostgreSQL's `parse_bool_with_len` exactly (`expr.parseBoolText`): case-insensitive, trimmed, any non-empty prefix of true/false/yes/no, on/off, 1/0; anything else is 22P02. The rule is chosen from the operand's DECLARATION (`expr.castBoolDeclared`), never its Go box, so a DECIMAL or DATE column cannot take the integer or string arm.

**NaN and ±Infinity are DECIMAL comparison literals only** (E47)
`d = 'NaN'`, `d < 'NaN'`, `d <= 'Infinity'` and `d > '-Infinity'` are accepted and answer by PostgreSQL's order over a column that can hold none of them, via the same `ScaledDecimal.Sat` bound a wide finite literal takes. As values they are 22003 at the checked reader (`batch.ParseDecimalStringChecked`, `Vector.SetValueChecked`, `FromRowsChecked`). Accepted spellings are PostgreSQL's input grammar only: unsigned case-insensitive `nan`, `infinity`/`inf` with an optional sign; `'+NaN'`, `'Infin'` and `'abc'` are 22P02. The entry records CAST and the unchecked write paths as storing 0 or a string (#555, #647); the tip measures 22003 for both INSERT and CAST.

**The `^` spellings and exact-decimal precision** (E30, E50)
`^` shares POWER()'s values, 2201F/22003 error classes and declared type; the two extra spellings are recorded rather than narrowed, since refusing them needs a second precedence table. `+ - * %` over DECIMAL keep PostgreSQL's scale and cap precision at 38 (ADR-0024 §3), so a result needing more digits is 22003 rather than a value at a reduced scale.

**Per-value decimal scale is deferred on cost** (E64, A13)
PostgreSQL's unconstrained numeric carries a per-value scale; `batch.DecimalColumn` carries one per column, and the scale is lost in the projection at `expr.EvalDecimalInto` -> `batch.Vector.SetComputedChecked`. The `.wshf` exchange carries per-column scale and precision, and `CAST(COALESCE(d152, 12.3456789012345) AS VARCHAR)` prints PostgreSQL's per-row text on every arm. A per-value scale touches 74 non-test `DecimalData.Scale` sites; trimming trailing zeros would break correct cells such as `1.00`.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `SELECT CAST(SUM(a) OVER () AS INTEGER) FROM ni` | integer (OID 23) | bigint (OID 20) (measured; MAX(c_i32) + 0 and c_i32 + 1 also bigint, with rows or none: `SELECT t.i - t.i FROM t WHERE t.id = 99` (no row) declares 20; inside a scalar subquery int4 arithmetic declares integer and raises 22003 past its range, as on PostgreSQL); an integer CAST of a float-carried numeric declares 20 with PostgreSQL's value, `CAST(5 / 2.0 AS INTEGER)` 3 (measured 2026-10-02, wire w/rcSix; 2 at v0.25.3); an alias does not change the width, `SELECT 7 * t.i AS x` declares 20 as `7 * t.i` does (c39858f3's and 6184761c's single-process arms declared 23 there, from the column `i` the text named after its first dot), and `ABS(-1)` declares 20 where 6184761c declared double precision (701; `MOD(7, 3)` declares 23 as on PostgreSQL) (measured 2026-10-03, al/litMulI/alias, ac/abs, ac/mod); `t.i % 3` declares integer (OID 23) as `MOD(t.i, 3)` does, `%` being MOD's spelling (8ccfa832: bigint, OID 20, g/proj/i_l3; measured 2026-10-04) | — | value divergence | 2026-09-15 · [E78](#e78), P025 | #1070 | `pgwire.TestArcNXNumericCarrierOnTheWire` (kept w/rcSix, w/rcBig); `pgwire.TestArcMOPercentIsModOnTheWire` g/proj/i_l3 |
| **r2** `SELECT CAST(a AS SMALLINT) FROM ni` | smallint | bigint (measured; no int16 carrier); CAST(99999 AS SMALLINT) is 22003 on both | — | value divergence | 2026-09-15 · [E78](#e78), P025 | #1070, #901 | — |
| **r3** `SELECT 2147483647 + 1` | ERROR 22003 integer out of range | 2147483648 (measured); an integer CAST is the same int4 arithmetic, `CAST(t.i AS INTEGER) + 2147483647` over 3 is 2147483650 (measured 2026-10-02 at the tip and at c39858f3 alike: the integer CAST beside an integer stays integer arithmetic, only beside a numeric is it numeric) | PG 22003 | kept superset | 2026-09-15 · [E78](#e78), P073 | #1070 | — |
| **r4** `SELECT -c_i32, c_i32 * 2 FROM intmin -- c_i32 = -2147483648` | ERROR 22003 integer out of range | 2147483648, -4294967296 in int64 | PG 22003 | kept superset | 2026-09-04 · [E57](#e57), P073 | — | `coordinator.TestIntegerMinimumIsLoudOnEveryArm` |
| **r5** `SELECT * FROM intmin WHERE ABS(c_i32) > 2147483646` | ERROR 22003 integer out of range | the row (widened int64 comparison); the int8 twin raises 22003 | PG 22003 | kept superset | 2026-09-04 · [E57](#e57), P073 | — | `coordinator.TestIntegerMinimumIsLoudOnEveryArm` |
| **r6** `SELECT (r + 1.0::real) + 1.0::real FROM t -- r = 16777216` | 16777216 | 16777218 | — | value divergence | 2026-09-15 · [E19](#e19), [A05](../0012-amendments.md#a05) | #1117 | `1117/nested_real_arith (census cell)` |
| **r7** `SELECT r FROM t WHERE r + 1.0::real > 16777216::real` | no rows | one row | — | value divergence | 2026-09-15 · [A05](../0012-amendments.md#a05) | #1117 | `1117/real_arith_in_a_predicate (census cell)` |
| **r8** `SELECT r FROM t WHERE r + 1.0::real = 16777216::real` | one row | no rows | — | value divergence | 2026-09-15 · [A05](../0012-amendments.md#a05) | #1117 | `1117/real_arith_in_an_equality_predicate (census cell)` |
| **r9** `SELECT CASE WHEN r + 1.0::real = 16777216::real THEN 'eq' ELSE 'ne' END FROM t` | eq | ne | — | value divergence | 2026-09-15 · [A05](../0012-amendments.md#a05) | #1117 | `1117/real_arith_in_a_case_condition (census cell)` |
| **r10** `SELECT 2 ^ -1` | ERROR operator does not exist: integer ^- integer | 0.5 | PG 42883 | kept superset | 2026-09-18 · [E30](#e30), P086 | #1155 | — |
| **r11** `SELECT 2 ^ 3 % 5` | ERROR no double precision % integer operator | 3 (`%` is MOD's spelling, 2026-10-04: over a double operand both compute the float remainder, a zero remainder an unsigned 0 — `t.f % 2.5` over -2.5 printed -0 at 8ccfa832, g/proj/f_ln) | PG 42883 | kept superset | 2026-09-18 · [E30](#e30), P086 | #1155 | `coordinator.TestArcMOPercentIsModEveryArm` g/\*/f_\* (kept); `pgwire.TestArcMOPercentIsModOnTheWire` g/proj/f_ln |
| **r12** `SELECT 5.0 # 3, 'a' # 'b'` | ERROR 42883; ERROR 42725 | answers (operands read as integers); int4 # int4 is integer, a bigint operand bigint, as on PostgreSQL | PG 42883 | kept superset | — · P103 | #1179 | — |
| **r13** `SELECT -'5'` | ERROR 42725 operator is not unique: - unknown | -5 double precision (OID 701, measured 2026-10-05); -'abc' raises 22P02 | PG 42725 | kept superset | 2026-08-24 · [E43](#e43), P061 | #505 | — |
| **r14** `SELECT CAST(1::bigint AS BOOLEAN)` | ERROR 42846 cannot cast type bigint to boolean | true (0 is false, other values true, NULL is NULL) | PG 42846 | kept superset | 2026-08-25 · [E46](#e46), P063 | #592 | — |
| **r15** `INSERT INTO nd VALUES ('NaN') -- nd.d DECIMAL(9,2)` | stores NaN | ERROR 22003 numeric field overflow: "NaN" has no DECIMAL value (measured; 'Infinity' also 22003, as PG does for constrained numeric) | 22003 | refusal | 2026-08-29 · [E47](#e47), P064 | #534 | — |
| **r16** `SELECT CAST('NaN' AS DECIMAL(9,2))` | NaN | ERROR 22003 "NaN" has no DECIMAL value (measured) | 22003 | refusal | 2026-08-29 · [E47](#e47), P064 | #534, #555 | — |
| **r17** `SELECT a * b FROM t -- a, b DECIMAL(38,10), product past 10^18`; likewise `v * v` over a column created `NUMERIC` (ADR-0024 §10) holding 1234567890 | numeric value (1524157875019052100) | ERROR 22003 (result type DECIMAL(38,20)); a column created `NUMERIC` is DECIMAL(38,10), so the product of two of them keeps 18 integer digits, where 8e681724's DECIMAL(38,0) column answered 1524157875019052100; `CAST(v AS NUMERIC(38,0)) * v` answers 1524157875019052100.0000000000, and a product beside a bigint (`v * b`, DECIMAL(38,10)) answers (measured 2026-10-05) | 22003 | refusal | 2026-09-02 · [E50](#e50), P068 | #749, #1541 | `WideDecimalSquaredRowCount (PostgreSQL corpus`, `kind pgDivergenceCarrier)`, `coordinator.TestArcUNUnconstrainedColumnEveryArm` x1_vv |
| **r18** `SELECT COALESCE(d152, 12.3456789012345) FROM t -- d152 numeric(15,2)` | 12.75 | 12.7500000000000 (one scale per column; wire typmod -1); the elements of a numeric[] array share one scale too: `ARRAY[n, 1]` over a numeric(10,2) is {2.25,1.00} where PostgreSQL prints {2.25,1} (measured, r12/b2/arrN1); a numeric constant a double cannot carry is its own DECIMAL in a fold, so `COALESCE(t.b, 14.0000000000000000001)` over a bigint prints 30.0000000000000000000 for PostgreSQL's 30 and 14.0000000000000000001 on the NULL row, and `CASE WHEN 14.0000000000000000001 > 14 THEN 14 ELSE 13.25 END` prints 14.00 for 14 (measured 2026-10-02, nx/ncCoalesceCol/*, nx/ncCaseChoice/*; at c39858f3 the first was the double 14 on the NULL row and the second 13.25, the wrong arm) | — | value divergence | 2026-09-05 · [E64](#e64), [A13](../0012-amendments.md#a13), P007 | #764, #1386 | `coordinator.TestLiteralScaleInADecimalFold`, `coordinator.TestArcNXNumericCarrierEveryArm` |
| **r19** `SELECT m / i FROM t -- m numeric(10,2) = 1.25, i integer = 3` | 0.41666666666666666667 | 0.4166666666667 (measured, the same at v0.25.3): a DECIMAL quotient keeps max(6, s1 + p2 + 1) fraction digits, the divisor's precision its column type's (an integer column DECIMAL(10,0), a bigint DECIMAL(19,0), a numeric(10,2) its 10); an integer LITERAL divisor is its digit count, so `m / 3` keeps 6 (0.416667). A correlated subquery's outer value is typed as its column, so `(SELECT x.m / o.i …)` and `(SELECT x.v / o.n …)` answer the column forms' digits exactly; inside a scalar subquery an integral EXTRACT divisor takes the int64 range at scale 0, so `(SELECT x.m / extract(year FROM o.d) …)` keeps 22 fraction digits, the value correctly rounded (0.0006175889328063241107; PostgreSQL 0.00061758893280632411; v0.25.3 double precision 0.0006175889328063241 — coordinator r9/a_mDivYear). A quotient over an operand a cast made exact keeps the float8 rung, OID 701 — an integer CAST the query wrote, bare or under NULLIF, COALESCE, CASE, GREATEST, LEAST, abs, unary minus, integer arithmetic or numeric arithmetic (`(CAST(t.i AS INTEGER) * 1.0) / t.n`); a bare `CAST(… AS NUMERIC)` (`CAST(t.i AS NUMERIC) / t.n`, `t.n / CAST(t.b AS NUMERIC)`, `CAST(t.n AS NUMERIC) / 3`); either of those under a function whose result is its argument's exact type — abs, mod, round, ceil, ceiling, floor, trunc, sign, one list the plan and the kernel read (`ceil(CAST(t.i AS NUMERIC)) / t.n`, `round(CAST(t.i AS INTEGER) * 1.0) / t.n`; qf/*): `CAST(t.i AS INTEGER) / t.n` is the double 1.3333333333333333, 2, 0.5, 100 where PostgreSQL answers numeric 1.3333333333333333, 2.0000000000000000, 0.50000000000000000000, 100.0000000000000000 (measured 2026-10-02, issue/1450/div; the same at c39858f3), because this rule's 11 fraction digits would drop the double's 16 significant digits, and `CAST(t.n AS NUMERIC) / 3` is 0.75, 0.0033333333333333335 (the double, as at c39858f3; this rule's 6 digits would print 0.003333, bareCast/numOperand). Every other operator over such an operand is exact (#1450), so `t.n / NULLIF(CAST(t.b AS BIGINT), 0)` = 0.0000000011111111111111111111 selects PostgreSQL's row 3 and a `PARTITION BY` over it partitions as PostgreSQL's (nx/icBigDiv/cmp, nx/icBigDiv/winKey, nx/wq*/cmp, nx/wn*/cmp, nx/wq*/winKey, nx/wn*/winKey, qn/*/cmp: the same at c39858f3), and the projection of the wrapped cast is the double 0.075 … 1.111111111111111e-09 where c39858f3 raised 22003 numeric field overflow (declared at this rule, computed as the double; nx/icBigDiv/proj), and `ceil(CAST(t.i AS NUMERIC)) / t.n = 1.3333333333333333` selects PostgreSQL's row 1 as at c39858f3 (qf/ceil/bareNum/cmp and the other qf/*/cmp, qf/*/win, qf/*/group, qf/*/inList). A quotient of two constants keeps the double too — an integer literal past int64 among them, `9223372036854775808 / 2` 4.611686018427388e+18 where PostgreSQL answers numeric 4611686018427387904 (negLit/pastInt64Quot, as at c39858f3) — while the same literal over a column is this rule's one-scale numeric: `t.n / 9223372036854775808` 0.0000000000000000002439 where PostgreSQL answers 0.000000000000000000243945488809238498 (negLit/colOverPastInt64, as at c39858f3), and `9223372036854775808 / t.n` 4099276460824344803.55555555556 where PostgreSQL answers 4099276460824344803.56 (alias/pastInt64QuotNoAlias). An aliased item is the same quotient: `7 / t.n AS x` prints 3.11111111111 and `9223372036854775808 / t.n AS x` 4099276460824344803.55555555556 on every arm (alias/smallQuot, alias/pastInt64Quot); c39858f3's single-process arms typed such an item by the column its text named after the first dot and printed 3.11 and 4099276460824344803.56 | — | value divergence | 2026-09-30 · ADR-0024 §3 | #1422, #1450 | `coordinator.TestArcSSOperandClassAndWidthEveryArm`, `coordinator.TestArcNXNumericCarrierEveryArm` |
| **r20** `CREATE TABLE t (c DOUBLE)`; `CAST(1 AS "float")`, `CAST(1 AS "real")` | ERROR 42704 type "double" / "float" / "real" does not exist (a quoted name resolves against pg_type.typname: `"float4"` and `"float8"` are accepted) | double precision, double precision, real: DOUBLE is this engine's float8 spelling, and a type name is resolved by its text, so the quoted keywords read as FLOAT and REAL; FLOAT32 and FLOAT64 are its own names (measured) | PG 42704 | kept superset | 2026-10-03 · [the FT block](../0012-amendments.md#2026-10-03-the-floating-point-type-names-1464-1405) | #1464 | `coordinator.TestArcFTFloatTypeNamesEveryArm` ddl/double/\*, ddl/floatQuoted/\*, ddl/realQuoted/\* |
| **r21** `SELECT MOD(8, '2.5')` | ERROR 22P02 invalid input syntax for type integer: "2.5" (the unknown literal resolves to the integer argument's type) | 0.5, double precision (OID 701) (measured): beside an integer, a quoted literal whose text is not an integer is read as the number it spells, as `8 + '2.5'` is 10.5 and `MOD(t.b, '2.5')` over -70 is 0; a quoted integer keeps the integer type, `MOD(t.i, '3')` is integer (OID 23) as on PostgreSQL. The `%` operator is the `MOD` function, so the two spellings match each other where an expression must match a grouping key: `SELECT t.i % 2, count(*) FROM t GROUP BY mod(t.i, 2)` answers the groups here (measured 2026-10-04) where PostgreSQL raises 42803. 9420d256 answered `MOD(8, '2.5')` 0.5 and `8 + '2.5'` 10.5 the same, and `MOD(t.i, '2.5')` over 3 the integer 0; the `%` spelling is MOD (2026-10-04, #1527): `8 % '2.5'` is the same double 0.5 and `t.i % '2.5'` over 3, -7, 5 is 0.5, -2, 0 as `MOD(t.i, '2.5')` is (8ccfa832: `8 % '2.5'` 0 on the single-process arms and 0.5 on the DAG arms, `t.i % '2.5'` 1, -1, 1 single-process and 0.5, -2, 0 on the DAG, g/proj/l8_q25, g/proj/i_q25) | PG 22P02 | kept superset | 2026-10-03 · ADR-0024 §3 | — | `coordinator.TestArcNXNumericCarrierEveryArm` m/modLitQuoted\*, m/modColQuoted\*; `pgwire.TestArcNXNumericCarrierOnTheWire` w/modLitQuotedFracTbl, w/modColQuotedFracB; `coordinator.TestArcMOPercentIsModEveryArm` g/\*/i_q25, g/\*/l8_q25 (kept, both spellings) |
| **r22** `SELECT round(f, 1) FROM t -- f double precision = 0.25` | ERROR 42883 function round(double precision, integer) does not exist (for a column, a cast literal and n = 0 alike) | 0.2 (measured): the operand's own rule, f·10ⁿ rounded half to even and scaled back — the rule `round(f)` has (a REAL operand alike; `round(f, 0)` over 2.5 is 2, `round(f, 2)` over 0.125 is 0.12). A NUMERIC operand is PostgreSQL's round(numeric, integer), 0.3 for 0.25 on both. 89cea148 answered 0.3 over a column and 0.2 over `CAST(0.25 AS DOUBLE PRECISION)` | PG 42883 | kept superset | 2026-10-04 · ADR-0024 §2c | #381 | `wadjet.TestArcRERoundWithDigitsOverAFloatTakesTheFloatRule` |
| **r23** `CREATE TABLE t (v NUMERIC); INSERT INTO t VALUES (0.00000000005), (1e-11), (12345678901234567890123456789)` | stores 0.00000000005, 0.00000000001 and the 29-digit integer | 0.0000000001, 0 and ERROR 22003: a column created from an unconstrained numeric keeps 10 fraction digits (more round half away from zero) and 28 integer digits (ADR-0024 §10); `UPDATE t SET v = v / 3` over 10 stores 3.3333333333 (PostgreSQL 3.3333333333333333) (measured 2026-10-05) | 22003 | value divergence | 2026-10-05 · ADR-0024 §10 | #1541 | `wadjet.TestArcUNUnconstrainedColumnEnumeration` (r23 rows) |
| **r24** `CREATE TABLE t AS SELECT 2.50 AS v; SELECT v FROM t` | 2.50 (the value's own scale) | 2.5: a column created from an unconstrained numeric prints without the stored scale's trailing zeros, so a trailing zero the source carried is not printed; `1.10`, `-2.50`, `SUM(n)` over numeric(10,2) (3.5 for PostgreSQL's 3.50) and `n * m` alike; a value that has no trailing zero prints PostgreSQL's text (`1.25`, `1`, `0.755`) (measured 2026-10-05). A column created by CTAS from a computed numeric key whose select term and GROUP BY key are spelled differently (`SELECT 2 * t.n … GROUP BY 2 * n`) is the same column (ADR-0047) and prints the same way (`wadjet.TestArcGKEmbeddedGroupKeySpelling` gk_c2 ordered, `pgwire.TestArcGKGroupKeySpellingOnTheWire` store/ctas/{s,e,b}, `coordinator.TestArcGKCTASNumericTextEveryArm` mixedLength/alikeLength). The text is one printer for every rendering of the column's value: the result rows and `CAST(v AS TEXT)`, `v || ''`, `concat`, `concat_ws`, `format('%s', v)`, `quote_literal`, `json_build_object`, `array_to_string(ARRAY[v], ',')` alike, through a derived table, a CTE, a GROUP BY key, a join, DISTINCT, a LATERAL body and a set operation whose result is marked (ADR-0024 §10's table: beside such a column neutral arms permit the mark; a NUMERIC(p,s) column or non-NULL CAST with s > 0, or a literal spelled with trailing zeros, vetoes it, so an expression arm over a numeric(10,2) column, `n + 0`, prints 2.5 where PostgreSQL prints 2.50); a `COALESCE` / `CASE` / `GREATEST` / `NULLIF` that answers the column's value renders it so as text too (`CAST(COALESCE(v, n) AS TEXT)` is 1.25, PostgreSQL 1.25) while the SELECT list prints that expression at its one scale (r18). The text is the same on every arm and every door after an exchange — a GROUP BY, a DISTINCT, a set operation whose result is marked, a window, a join, the asynchronous door's result. The stage DAG still omits the outer projection for `CAST(v AS TEXT) AS x` over UNION (distinct), publishing `v` instead of `x` (UN-F9); the result schema now preserves the numeric batch's mark, so HTTP, gRPC-coordinator and pgwire-coordinator all print its trimmed text (`server.TestArcUNSetOperationTextEveryDoor`) (measured 2026-10-05; at 742965c1 only `CAST(v AS TEXT)` over a bare column trimmed, so `CAST(v AS TEXT) = v || ''` was false) | — | value divergence | 2026-10-05 · ADR-0024 §10 | #1541 | `wadjet.TestAssignmentDoorsAgree` (CTAS 2.50, 1.10, -2.50, 1.10 + 0), `wadjet.TestArcUNUnconstrainedColumnEnumeration` (r24 rows), `wadjet.TestArcUNOneTextPrinter`, `coordinator.TestArcUNUnconstrainedColumnEveryArm` (r_* cells), `server.TestArcUNUnconstrainedColumnOnTheWire` (cat, concat, format, grpc-*), `coordinator.TestArcUNExchangeKeepsThePrinterEveryArm`, `coordinator.TestArcUNInBandMarkEveryArm`, `server.TestArcUNInBandMarkEveryDoor`, `coordinator.TestArcUNSetOperationMatrix` |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E19

ADR lines 796-902. Catalog rows: r6. Stated in [Mechanisms](#mechanisms). Moved to the log: [A04](../0012-amendments.md#a04), [A05](../0012-amendments.md#a05).

- **ARITHMETIC over two REALs computes in `double precision`, where
  PostgreSQL computes it in `real`.** (Added 2026-09-14, found by
  #950's seam enumeration and NOT closed by it.) Measured on 17.11 over a
  `real` column holding 16777216 — 2^24, where a real stops counting by
  ones:

  ```
  r + 1.0::real      PostgreSQL 1.6777216e+07   wadjet 1.6777217e+07
  f4 * 10.0::real    PostgreSQL 22003 overflow  wadjet 9.999999680285692e+38
  f4 + f4            PostgreSQL 2e+38           wadjet 1.9999999360571385e+38
  ```

  The first is a different NUMBER; the second is an answer where the server
  refuses (float4's range is 3.4e38); the third is the same number printed
  at float8's width because that is the type wadjet declares for it. The
  ACCUMULATOR half of this family closed with #950 — a SUM over a real
  column totals at float4's width — but the arithmetic that produces a real
  operand does not, so `SUM(r * 1.0::real)` is a float8 sum of float8
  products.

  Deferred rather than fixed with #950: the value half is a float32 kernel
  in `expr` (a resolve step plus a typed arm, the shape the DECIMAL and
  INTEGER arms already have), and the other half is the DECLARATION —
  `physical.inferProjectionTypeCols` would have to answer FLOAT32 for
  arithmetic over two reals — which belongs to the projection-typing layer. Doing only the first would produce a float32 value under a float8
  declaration, which is the class this list exists to prevent. Filed as a
  candidate with the numeric value measurements.

  *(Moved to the amendment log: [A04](../0012-amendments.md#a04).)*

  *(Moved to the amendment log: [A05](../0012-amendments.md#a05).)*

### E30

ADR lines 1123-1138. Catalog rows: r10, r11. Stated in [Mechanisms](#mechanisms).

- **The `^` operator answers two spellings PostgreSQL's lexer and operator
  table reject.** (Added 2026-09-18 by the earlier name-scope measurement, N10; recorded
  2026-09-18, #1155.)

  `SELECT 2 ^ -1` is 0.5 here. PostgreSQL lexes `^-` as ONE operator name —
  operator characters run together — and answers `operator does not exist:
  integer ^- integer`; the spelling it reads is `2 ^ (-1)`, which both
  engines answer. `SELECT 2 ^ 3 % 5` is 3 here: `^` binds tighter, so `%`
  takes the result, and this engine's `%` accepts a float operand where
  PostgreSQL has no `double precision % integer`. Both are SUPERSETS — a
  statement PostgreSQL declines is answered, never answered differently —
  and neither touches what `^` shares with `POWER()`: the values, the
  2201F/22003 error classes and the declared type. Recorded rather than
  narrowed: refusing them would mean a second precedence table for an
  operator whose whole point is that it is `POWER()` under another spelling.

### E43

ADR lines 1837-1855. Catalog rows: r13.

- **Unary minus over a QUOTED string literal.** (Added 2026-08-24, #505.)
  PostgreSQL refuses EVERY `-'…'` form, numeric-looking or not, with
  42725 "operator is not unique: - unknown" — verified live: both
  `SELECT -'5'` and `SELECT -'abc'` error there, because unary minus
  resolves before the comparison can type the literal from context and
  several overloads match an unknown-typed operand equally well. Wadjet
  has ONE generic unary-minus operator and so has no overload ambiguity
  to report. It folds the literal instead: a numeric-looking string
  becomes a negated `Lit` carrying its exact text (so `d = -'5.00'`
  enters the same exact-DECIMAL path `d = -5.00` does, per item 6), and a
  non-numeric one is refused with 22P02. The visible consequence is that
  `SELECT -'5'` SUCCEEDS here and answers `-5` typed **varchar** — the
  literal keeps its own type, since nothing in that statement asks for a
  number — where PostgreSQL errors and, in the shapes where it does
  resolve (`-'5'::numeric`), answers a numeric. This is a deliberate
  extension of item 6's convention for the column type it exists for, not
  a position against PostgreSQL's overload resolution; reproducing 42725
  would mean building the overload ambiguity first.

### E46

ADR lines 1886-1927. Catalog rows: r14. Stated in [Mechanisms](#mechanisms).

- **CAST(<integer> AS BOOLEAN) over the WHOLE integer family.** (Added
  2026-08-25, #592.) PostgreSQL HAS this cast, for exactly one width:
  `1::int4::boolean` is `t`, while `int8`, `int2`, `float8` and `numeric`
  to boolean are all 42846 "cannot cast type ... to boolean" (verified
  live on postgres:17-alpine). Wadjet applies the int4 rule — 0 is FALSE,
  every other value TRUE, NULL is NULL — to INT32 and INT64 alike, and
  refuses everything else with the same 42846 PostgreSQL raises.

  The int8 half is the divergence, and it is a divergence from an
  OMISSION rather than from a position: PostgreSQL's own `1::bigint::int::bool`
  answers, so nothing about the meaning of the cast changes with the
  width — only whether a pg_cast row exists. Refusing it would mean
  `CAST(c AS BOOLEAN)` erroring on a BIGINT column and answering on an
  INTEGER one holding the identical values, and BIGINT is what wadjet's
  `BIGINT` declaration and its integer literals produce, so the cast
  would be unreachable for most columns while its twin worked.

  **FLOAT and DECIMAL are refused, not extended.** That asymmetry is the
  point: PostgreSQL declines float truthiness deliberately (there is no
  cast to omit — a float has no int4 twin whose rule this would be
  borrowing), and the value wadjet used to answer there was one nothing
  agreed on. `SELECT (f)::BOOLEAN` came back TRUE/FALSE through
  `Vector.SetValue`'s coercion while `WHERE (f)::BOOLEAN` excluded every
  row, which is the same two-path failure this item closes.

  **STRING is PostgreSQL's boolean input function exactly**
  (`parse_bool_with_len`, `expr.parseBoolText`): case-insensitive, C
  whitespace trimmed, any non-empty PREFIX of "true"/"false"/"yes"/"no",
  plus "on"/"off" and the single characters "1" and "0". `'tr'` and
  `'fals'` ARE values there; `'o'` alone is not, because it cannot choose
  between "on" and "off". A string that names no boolean is SQLSTATE
  22P02, never a value and never a match-nothing predicate — #463's rule
  for DECIMAL, one type family over.

  **The rule is selected from the operand's DECLARATION, never from its
  Go box** — item 8's boxed-value rule. A DECIMAL column and a STRING
  column both box as a Go string, so a box-driven cast would give
  `DECIMAL(9,0)` holding 1 the answer TRUE where PostgreSQL refuses
  outright, and DATE/IPv4/MAC (boxed as their raw integer encodings)
  would take the integer arm. `expr.castBoolDeclared` resolves the
  declaration from the batch and caches it, the way `boxedPair` does.

### E47

ADR lines 1928-1967. Catalog rows: r15, r16. Stated in [Mechanisms](#mechanisms).

- **NaN and ±Infinity are DECIMAL comparison literals only, never stored
  values.** (Added 2026-08-29, #534; the rule is ADR-0024 item 6 and this
  is the divergence it produces.) PostgreSQL's `numeric` HAS all three —
  NaN above every non-NaN and equal only to itself, ±Infinity since
  PostgreSQL 14 — and wadjet's finite Int128-at-a-fixed-scale carrier has
  no bit pattern for any of them. So `d = 'NaN'`, `d < 'NaN'`,
  `d <= 'Infinity'` and `d > '-Infinity'` are ACCEPTED and answer by
  PostgreSQL's order over a column that holds none of them (0 rows, every
  non-NULL row, every non-NULL row, every non-NULL row), resolved through
  the same `ScaledDecimal.Sat` bound item 6 gives a finite literal wider
  than the carrier.

  As a VALUE the three are refused with 22003 and a message naming
  ADR-0024, at the sites that produce one through the CHECKED reader:
  `batch.ParseDecimalStringChecked` and everything above it —
  `Vector.SetValueChecked`, `FromRowsChecked`, and through them the
  single-process set-operation adapter. PostgreSQL raises that same 22003
  for the infinities against a constrained `numeric(p,s)` ("cannot hold an
  infinite value", verified live); NaN it stores, and refusing that is the
  divergence. Two sites do NOT reach that reader yet and are recorded
  rather than claimed: `CAST(x AS DECIMAL(p,s))` is still ADR-0024 item 1's
  declared-STRING no-op, so `CAST('NaN' AS DECIMAL(9,2))` yields the string
  "NaN" and `CAST('NaN' AS DECIMAL)` a float64 NaN until the CAST evaluator
  lands (#555); and the UNCHECKED WRITE PATHS store 0 for them exactly as
  they do for `'abc'` — ADR-0024 item 4's residual, which is a property of
  the whole type and not of these three. There are TWO of those paths and
  both are named because only one of them is on the line a user's INSERT
  actually takes: `batch.ParseDecimalString` via `Vector.SetValue` (the
  row-to-batch adapter), and `parquet.decimalUnscaledInt64` /
  `decimalFLBABytes` in the file writer, whose string arm routes every
  value through `strconv.ParseFloat` — 0 on error, 0 for a NaN or an
  infinity, 0 for `' 3.50 '` because ParseFloat refuses the surrounding
  space, and float64's ~16 significant digits for everything else. That
  second one is the ingest path a client reaches and it is tracked as
  #647. The accepted spellings are PostgreSQL's own input
  grammar and nothing wider — case-insensitive `nan` with NO sign,
  case-insensitive `infinity`/`inf` with an optional adjacent sign, C
  whitespace trimmed — so `'+NaN'`, `'Infin'` and `'abc'` all stay 22P02.
  `ORDER BY` and `GROUP BY` need nothing: no such value can be stored, so
  no comparator or key ever meets one.

### E50

ADR lines 1990-2001. Catalog rows: r17. Stated in [Mechanisms](#mechanisms).

- **An EXACT decimal operator raises where PostgreSQL answers, past the
  carrier.** (Added 2026-09-02, #749.) `+ - * %` keep PostgreSQL's scale
  and cap the precision at 38 (ADR-0024 §3's 2026-09-02 amendment), so a
  result needing more than 38 digits at that scale is a 22003 rather than a
  value rounded to fewer fraction digits. The reachable case is
  `DECIMAL(38,10) × DECIMAL(38,10)`, whose type is `(38,20)`: a product past
  10^18 raises where PostgreSQL's unbounded numeric answers. It is item 1's
  finite carrier, not a new position, and it is the trade #749 makes
  deliberately — a right value of the wrong type is worse than an error.
  Pinned in the PostgreSQL corpus as `WideDecimalSquaredRowCount` under the
  kind `pgDivergenceCarrier`, which ratchets.

### E57

ADR lines 2327-2372. Catalog rows: r4, r5. Stated in [Mechanisms](#mechanisms).

- **An integer result with no room in its declared type FAILS; it is never
  a wrapped number — and the check is at the STORE, not in the kernel.**
  (Added 2026-09-04, earlier measurement P1.)

  `ABS(<int4 column>)` at -2147483648 answered -2147483648 where PostgreSQL
  17.11 raises `integer out of range` (22003), while the int8 twin already
  raised. Neither evaluator was wrong. `ColRef.Eval` widens an INT32 column
  to an int64 box on purpose (this list's "every integer spelling is INT64"
  superset), so the kernel computed 2147483648 — correct arithmetic — and
  `batch.SetValue`'s TypeInt32 arm narrowed it back with a bare
  `int32(tv)`. A different number wearing the right type: item 9's class.

  The refusal therefore belongs at the seam every such kernel crosses, and
  `batch.IntegerRangeError` raises there with the exec.FatalEvalPanic
  contract #361's TypeMismatchError already uses. A check inside ABS would
  have covered one function and, in the vectorized half, would have been
  DEAD CODE: the registry declares `abs` float64 while the planner declares
  the projection int4, so `FuncCall.EvalVec`'s `vecOutputHolds` guard sends
  every such call to the per-row path and `vecAbsDomain`'s narrow arms are
  unreachable from a projection. Measured, not inferred.

  **What stays a superset**: `-<int4 column>` at the floor and
  `<int4 column> * 2` still ANSWER, in int64, where PostgreSQL raises —
  this list's widening divergence, unchanged and now pinned in the same
  gate so the new refusal cannot swallow it silently.

  **And the PREDICATE position**, which is the same superset reached a
  different way: `WHERE ABS(<int4 column>) > 2147483646` at the floor
  ANSWERS the row where PostgreSQL raises `integer out of range`
  (measured). A predicate has no int4-declared output column for the
  operand to be stored into, so it never crosses `batch.SetValue` and the
  store's refusal cannot reach it; the comparison sees the widened int64
  the kernel computed, which is the exact value. Base-identical, and
  deliberately not changed — refusing there would reject a row wadjet can
  evaluate exactly, the wrong direction for this list. The int8 twin DOES
  raise in the predicate position, because `expr.absKeepsDomain` raises
  wherever it runs, so the two widths disagree here on purpose. Pinned by
  the same gate's `predicate_position_answers_the_widened_value` cell,
  whose bound is chosen so a wrapped value fails it as loudly as a refusal
  would.

  Gated by `coordinator.TestIntegerMinimumIsLoudOnEveryArm` (nine shapes ×
  five arms over the `intmin` fixture), `batch.TestInt32StoreKeepsTheNumberOrRefuses`
  (the seam with no plan) and `pgwire.TestIntegerOutOfRangeReaches22003OnTheWire`
  (the wire).

### E64

ADR lines 2648-2682. Catalog rows: r18. Stated in [Mechanisms](#mechanisms). Moved to the log: [A13](../0012-amendments.md#a13).

- **A DECIMAL composite renders every row at the FOLD's scale, not at each
  value's own.** (Added 2026-09-05, #764 — measured and DEFERRED, not
  fixed.) `COALESCE(numeric(15,2), 12.3456789012345)` prints
  `12.7500000000000` for the column's rows where the server prints
  `12.75`. Every cell is the same NUMBER; only the trailing zeros differ,
  and the WIRE already declares typmod −1 for these composites (ADR-0024
  item 5), so a client is told nothing about a scale it did not get.

  PostgreSQL's unconstrained `numeric` carries a per-VALUE scale;
  `batch.DecimalColumn` carries one scale for the whole column (ADR-0018
  §4). The scale is discarded in the PROJECTION, at
  `expr.EvalDecimalInto` → `batch.Vector.SetComputedChecked`, which parses
  the box's text at the vector's single `DecimalData.Scale` — long before
  any stage boundary.

  *(Moved to the amendment log: [A13](../0012-amendments.md#a13).)*

### E78

ADR lines 3112-3144. Catalog rows: r1, r2, r3. Stated in [Mechanisms](#mechanisms).

- **int4 ARITHMETIC and an integer CAST declare `bigint` where PostgreSQL
  declares `integer`, and the reason is a TWO-PATH one.** (Added
  2026-09-15, #1070; the divergence itself predates it.) A LITERAL
  that fits int4 declares `integer` now, and so do `MIN`/`MAX` of an int4
  (#951) and the bitwise family over int4 operands (#1018) — all measured
  identical on the single arm, the spilled arm and all three DAG arms. The
  other two sites were narrowed, measured, and put back:

  ```
  CAST(SUM(a) OVER () AS INTEGER)   single int4   dag int8   dag-shuffled int8
  MAX(c_i32) + 0                    single int4   dag int8   dag-shuffled int8
  ```

  The stage arms type an expression written over a published SLOT — an
  aggregate's `__agg_N`, a window's `__win_N` — from something coarser than
  the single-process walk reads. One expression with two declarations is
  #813 item 1's own class, and shipping it to close an OID gap would trade a
  stated divergence from PostgreSQL for an unstated one between this
  engine's own two paths. Two repairs were measured and neither closed it:
  letting a known carrier beat an `intWidthUnknown` entry in
  `declaredIntWidth`'s ColRef arm, and giving the three `colDecls` builders
  above an aggregate's output projection the width their types imply
  (#1029's proposal). ADR-0024 §2b carries the rule and the measurements.

  `SMALLINT` and `INT2` ride with the CAST and have a second reason of their
  own: this engine has no int16 carrier, so int4 would be the narrowest
  declaration available even if the cast narrowed. The VALUE is unchanged
  either way — `castIntInRange` has enforced each spelling's own range since
  #901, so `CAST(99999 AS SMALLINT)` is `22003` on both engines.

  ADR-0024's recorded int4 SUPERSET therefore stands: `2147483647 + 1`
  answers 2147483648 here and raises there.
