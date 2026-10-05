# ADR-0012 divergences: Extension functions

Functions PostgreSQL lacks (TIME_BUCKET, OHLCV, TCP flags, bitwise helpers, semver): each names its PostgreSQL spelling or its external authority, and where it departs. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**The oracle for an extension is PostgreSQL spelled out** (E77)
`TIME_BUCKET(stride, ts[, origin])` is `date_bin` under another name; its answers and its 0A000 (months or years) and 22008 (non-positive stride) refusals are the server's. `OHLCV(ts, price, volume)` is oracled by the bar spelled out per field under the same row filter a multi-argument aggregate applies, and each field declares the type of the aggregate it is. `(OHLCV(…)).open` in one query block answers since the A3b fixed-ROW seam (2026-09-12): the parser rewrites `(call).field` to `row_field(call,'field')`. No gate covers the one-block spelling for an aggregate container, so the derived table or CTE remains the recommended spelling.

**The TCP flag predicates are defined by the bit arithmetic** (E81)
`tcp_flags_has_all/any/none` and `tcp_flag_mask` are specified as `(f & m) = m`, `(f & m) <> 0`, `(f & m) = 0` and the literal mask. A negative flags value is a bit pattern; nothing special-cases the sign. Names are case-insensitive; `NS` is an input alias for `AE`, the canonical rendering. `tcp_flags`/`tcp_flags_text` name only the nine bits of RFC 9293 §3.1 plus bit 8; any other bit is not named, as a rendering contract rather than a refusal.

**A flag name is folded before a NULL flags argument, and a literal name before any row** (E81)
The names are converted into the mask operand, so a misspelling outranks a NULL flags value, as `NULL::bigint & 'x'::bigint` is 22P02 on PostgreSQL 17.11; a NULL name is a NULL mask and answers NULL. A flag name written as a string literal, and an empty name list, are refused from the declaration by `expr.RefuseUnknownTCPFlagNameLiterals`. The deciding layer is the binder: `physical.refuseUnknownFlagNames`, from `binder.checkExpr`, reached by `Plan` and `PlanDistributed` through `auth.ValidateStatementColumns` before any stage exists; compilation is a backstop only where it is reached. A name from a column or expression is checked per row.

**Every expression position reaches one walk** (E81)
`walkExpr` descends through a `WindowFuncNode` (argument, PARTITION BY/ORDER BY terms, frame offsets) for all three collectors, and `binder.blockSubqueries` also asks GROUP BY and each re-parsed ORDER BY item. `binder.registerCTE` validates a recursive body before rows, both UNION arms included. Named residuals: expression-valued frame bounds, named windows, LIMIT/OFFSET, table-function arguments, TABLESAMPLE and INSERT expressions are 42601 before binding; lazily compiled MERGE WHEN clauses and empty DAG policy filters are not guaranteed (#1043); `cte_shadowed_body` and `set_order_by` are pinned coverage residuals. Gates: `TestTCPFlagValidationDoors`, `TestTCPFlagASTCoverage`, `pgwire.TestPGWireRefusesAnInvalidFlagNameInEveryExpressionPosition`.

**The legacy flag functions share the one name table** (E84)
`has_tcp_flag`, `tcp_flags_from_string`, `tcp_flags_to_string`, `is_tcp_handshake` and `is_tcp_reset` read the nine-name table rather than an eight-bit one, so `tcp_flags_to_string(256)` is `AE` and `has_tcp_flag(f,'AE')` answers. An unknown name, and a flags argument that is not an integer (previously read as zero), are 22023 naming the name or the type.

**The bitwise family reads its argument exactly and declares an integer** (E82)
Twelve functions (`bitwise_and/or/xor/not`, the three shifts, `to_hex`, `to_base`, `bit_count`, `from_hex`, `from_base`) no longer carry an integer through float64, so bits above 2^53 survive; `numericFuncCall.EvalInt64` converts exactly. AND/OR/XOR/NOT over int4 operands declare `integer` since 2026-09-15 (#1018, arc ND), decided by `expr.PGIntegerResult.FitsOperands`; the shifts stay bigint because PostgreSQL's int4 shift is modular. The operator spellings `&`, `<<` and `>>` are not parsed (42601, measured); the functions are the spelling.

**An aggregate over an integer function takes PostgreSQL's result width from one table** (E82)
`expr.PGIntegerResultWidth`, read only by `physical.aggInputIsWideInteger`, gives each integer-returning function PostgreSQL's measured `pg_typeof`, or else the width that holds its whole domain; the bitwise family follows its operands. So `SUM(BITWISE_AND(int4_col,18))` is bigint and the int8 form numeric, as on 17.11. `expr.TestEveryIntegerDeclaredFunctionNamesItsPostgresResultWidth` holds the table and the registry to the same set.

**The declared width survives materialization** (E82)
The int4/int8 domain rides beside the INT64 carrier as `physical.colDecls.intWidth`, filled by `physical.emittedColIntWidth`: a call takes its table row, arithmetic and bitwise take the widest operand, a column its catalog type, a CAST its target, MIN/MAX the argument's width, a set operation the widest arm (nothing if any arm is silent). A scalar subquery's column is stamped once by `physical.annotateSubqueryColumnDecls` and read by `physical.buildAggregate` and `physical.resolveWindowKeys`; `refuseScalarSubqueryProjections` refuses to stage a window term holding a subquery. Gates: `pgwire.TestPGWireDeclaresSumOverAMaterializedIntegerColumn`, `pgwire.TestPGWireDeclaresSumOverAScalarSubqueryColumn`.

**`tcp_flags` declares text[]** (E83)
Closed 2026-09-24 (arc CW, #1017, ADR-0045): registry container returns carry their element, so `SELECT tcp_flags(f)` declares `text[]` (1009) and renders `{SYN,ACK}` on every door (measured). The earlier TEXT projection pin was deleted.

**Semver: the specification decides precedence, NULL for data, loud for the query's text** (E85)
PostgreSQL 17.11 has no semver function, so SemVer 2.0.0 §11 decides precedence; where a pure-SQL spelling exists (the integer-array core) it was measured as the oracle. Every function answers NULL for a string that is not a version; `semver_normalize_strict`/`semver_parse_strict` raise 22023. `semver_prerelease`/`semver_build` answer '' for a valid version without one. Components declare int8 (the int64 acceptance bound); `semver_cmp` declares int4.

**`semver_sort_key` is the ordering, as bytes** (E85)
The key's byte order equals §11 precedence, so ORDER BY, MIN/MAX, GROUP BY, the DAG merge and external sort need no comparator. Build metadata is not in the key; only its order is a contract, and `semver_normalize` is the canonical form. The identifier separator is `,` (0x2C), below `-` (0x2D), the minimum identifier byte. Gates: `expr.TestTheSortKeysStructuralBytesOrderBelowEveryIdentifierByte`, `expr.TestTheSortKeysByteOrderIsPrecedenceOverAGeneratedCorpus`.

**`semver_satisfies` reads node-semver's published range grammar; a literal range is decided by the binder** (E85)
The README's expansions are the oracle (`expr.TestTheRangeGrammarMatchesTheNodeSemverTable`), including the `-0` on exclusive upper bounds and the per-tuple pre-release rule; `includePrerelease` is not implemented. The range is read before the version, so a malformed range is 22023 over a NULL version; a NULL range answers NULL. `expr.RefuseInvalidSemverRangeLiterals` is asked at plan time by `physical.refuseInvalidSemverRanges` and again by `expr.compileFuncCallNamed` as backstop; the compiled-range memo caches refusals.

**The trivial lower bound `>=0.0.0` is deleted, and a raised bound saturates** (E85)
As node-semver's `replaceGTE0` does, `>=0.0.0` (build metadata ignored, `>=v0.0.0` included; `>=0.0.0-0` kept) is removed from every comparator set on the parsed comparator, so an opted-in pre-release of 0.0.0 is admitted (Go pseudo-versions). A bound raised past int64's maximum is rewritten exactly — `<X.(Y+1).0-0` to `<=X.Y.max`, `>=X.(Y+1).0` to `>X.Y.max` — rather than wrapped or refused; `expr.TestTheBoundTableCoversEveryRaiseSite` holds all sixteen raise sites.

**`semver_parse` returns a fixed ROW** (E85)
`semver_parse`/`semver_parse_strict` return `(major bigint, minor bigint, patch bigint, prerelease text, build text)` through the A3b declared-output seam, including aggregate keys, set-operation arms, window keys and worker/gather projections. Invalid input is NULL, or 22023 in the strict form.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `SELECT TIME_BUCKET(INTERVAL '1 hour', ts) FROM trades` | ERROR 42883: date_bin has no two-argument form; an origin is required | bins from the default origin 1970-01-01 (05:30 -> 05:00), as date_bin with that origin | — | kept superset | 2026-09-08 · [E77](#e77), P097 | #965 | `wadjet.TestTimeBucket*` |
| **r2** `SELECT TIME_BUCKET('1 hour', ts) FROM trades` | date_bin('1 hour', ts, origin) answers | ERROR 42804 stride must be an INTERVAL literal; an interval-typed CAST(s AS INTERVAL) is accepted (measured) | 42804 | refusal | 2026-09-08 · [E77](#e77), P097 | #965 | — |
| **r3** `SELECT TIME_BUCKET(INTERVAL '1 day 6 hours', ts) FROM trades` | date_bin bins with a 30-hour stride | ERROR 22007 invalid input syntax for type interval (measured): the parser's interval grammar decides | 22007 | refusal | 2026-09-08 · [E77](#e77), P097 | #965 | — |
| **r4** `SELECT OHLCV(ts, price, size) FROM trades` | ERROR 42883; the bar is min/max/sum/avg spelled out per field | one ROW bar per group; each field declares its aggregate's type (min of price, sum(volume), avg(price)) | — | kept superset | 2026-09-08 · [E77](#e77), P098 | #965 | `coordinator.TestTheBarIsTheSameOnEveryArm`, `coordinator.TestTheBarsDeclaredTypeIsTheSameOnEveryArm`, `pgwire.TestPGWireRendersTheBarAsAPostgresComposite` |
| **r5** `SELECT (b).vwap FROM (SELECT OHLCV(ts, price, size) AS b FROM trades WHERE size = 0) s` | SUM(price*size)/SUM(size) is ERROR 22012 | NULL vwap; the other five fields are PostgreSQL's and other bars survive | — | kept superset | 2026-09-08 · [E77](#e77), P098 | #965 | `zero_volume_has_no_vwap` |
| **r6** `SELECT OHLCV(DISTINCT ts, price, size) FROM trades` | the per-field DISTINCT spellings answer | ERROR 0A000 | 0A000 | refusal | 2026-09-08 · [E77](#e77), P098 | #965 | — |
| **r7** `SELECT OHLCV(ts, price, size) OVER () FROM trades` | the per-field window spellings answer | ERROR 0A000 (no window arm) | 0A000 | refusal | 2026-09-08 · [E77](#e77), P098 | #965 | — |
| **r8** `SELECT (b).open FROM (SELECT OHLCV(ts, price, size) AS b FROM trades) s` | numeric, typmod -1 (min over DECIMAL(9,2)) | numeric(9,2), typmod 589830 (measured); vwap declares numeric(38,6) | — | value divergence | — · [E77](#e77), P032 | #965 | — |
| **r9** `SELECT tcp_flags_has_all(f, 'SYN', 'ACK') FROM tcpflow` | ERROR 42883; the spelling is (f & 18) = 18 | the same values as (f & 18) = 18, NULL flags give NULL | — | kept superset | 2026-09-08 · [E81](#e81), P099 | #966 | `expr.TestTheTCPFlagPredicatesAreTheBitArithmetic`, `wadjet.TestTheTCPFlagFamilyAnswersPostgresBitArithmeticEndToEnd`, `coordinator.TestTheTCPFlagFamilyAnswersPostgresBitArithmetic` |
| **r10** `SELECT tcp_flags_has_all(f) FROM tcpflow` | (f & 0) = 0 is t for every row | ERROR 22023, refused before any row | 22023 | refusal | 2026-09-08 · [E81](#e81), P099 | #966, #1018 | — |
| **r11** `SELECT tcp_flags_has_all(f, 'SYN', 'ACKK') FROM tcpflow` | no equivalent failure: the mask is a number | ERROR 22023 naming 'ACKK' and listing the nine names; a literal is refused with no rows, before a NULL flags value | 22023 | refusal | 2026-09-11 · [E81](#e81), [E84](#e84), P099 | #966, #1018 | `expr.TestAnUnknownFlagNameOutranksANullFlagsArgument`, `pgwire.TestPGWireRefusesAnInvalidFlagNameWithNoRows` |
| **r12** `SELECT tcp_flags(18), tcp_flags(1024)` | ERROR 42883; no named-bit function | {SYN,ACK} declared text[] (1009); a bit outside the nine names is not named and not refused | — | kept superset | 2026-09-24 · [E81](#e81), [E83](#e83) | #966, #1017 | `wadjet.TestTheTCPFlagFamilyAnswersPostgresBitArithmeticEndToEnd` |
| **r13** `SELECT tcp_flags_from_string(''), tcp_flags_from_string('SYN,,ACK')` | ERROR 42883 | '' is 0 (the mask of no names); an empty element is ERROR 22023 naming its position | 22023 | kept superset | 2026-09-08 · [E81](#e81) | #966 | — |
| **r14** `SELECT has_tcp_flag(256, 'AE'), tcp_flags_to_string(256)` | ERROR 42883 | t, 'AE' (measured); an unknown name or a non-integer flags argument is ERROR 22023 | 22023 | kept superset | 2026-09-08 · [E84](#e84) | #966 | — |
| **r15** `SELECT BITWISE_LEFT_SHIFT(2147483647, 2)` | 2147483647 << 2 is -4, integer (modular int4 shift) | 8589934588 declared bigint; the << operator spelling is 42601 here (measured) | — | value divergence | 2026-09-15 · [E82](#e82), P009 | #1018 | `coordinator.TestNDDeclarationsMatchPostgres` |
| **r16** `SELECT BITWISE_RIGHT_SHIFT(-1, 1)` | (-1)::int8 >> 1 is -1 (arithmetic) | 9223372036854775807 (logical, Trino's); BITWISE_ARITHMETIC_SHIFT_RIGHT(-1, 1) is -1 | — | value divergence | 2026-09-08 · [E82](#e82), P100 | #966 | `expr.TestTheWholeBitwiseFamilyIsExact` |
| **r17** `SELECT BITWISE_LEFT_SHIFT(1, 64)` | 1::int8 << 64 is 1 (count taken modulo the width) | NULL for a count outside [0,64) | — | value divergence | 2026-09-08 · [E82](#e82), P100 | #966 | `expr.TestTheWholeBitwiseFamilyIsExact` |
| **r18** `SELECT TO_HEX(f4) FROM t -- f4 int = -1` | ffffffff | ffffffffffffffff (the 64-bit word); an untyped negative literal also renders 16 digits | — | value divergence | 2026-09-08 · [E82](#e82), P010 | #966 | `expr.TestToHexRendersTheArgumentsOwnWidth` |
| **r19** `SELECT TO_BASE(-255, 16)` | ERROR 42883: no to_base | -ff (signed rendering, Trino's) | — | kept superset | 2026-09-08 · [E82](#e82), P100 | #966 | `expr.TestTheWholeBitwiseFamilyIsExact` |
| **r20** `SELECT FROM_HEX('12zz')` | decode('12zz','hex') is ERROR 22023 | NULL (parse-or-NULL; the whole string is required) | — | kept superset | 2026-09-08 · [E82](#e82), P079 | #966 | `expr.TestFromHexRequiresTheWholeString` |
| **r21** `SELECT REGEXP_COUNT('abab', 'a')` | declared integer (OID 23) | declared bigint (OID 20), measured; SUM over it declares bigint as PostgreSQL does | — | value divergence | 2026-09-11 · [E82](#e82) | — | — |
| **r22** `SELECT MIN((SELECT CAST(1 AS INT))) FROM users` | 1, declared integer (OID 23) | 1, declared bigint (OID 20) | — | value divergence | 2026-09-11 · [E82](#e82) | #1037 | `TestScalarSubqueryAggregateMatrix` |
| **r23** `SELECT SUM((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) FROM users -- 3 rows` | 27021597764222979.75 | 27021597764222982, numeric: the literal rounds through float64 before the aggregate | — | documented gap | 2026-09-11 · [E82](#e82) | #1037 | `TestScalarSubqueryAggregateMatrix` |
| **r24** `SELECT (SELECT id FROM users LIMIT 1) UNION ALL SELECT CAST(1 AS BIGINT)` | answers, bigint | ERROR 42804: the subquery arm is declared text beside a bigint arm | 42804 | refusal | 2026-09-11 · [E82](#e82) | #874 | — |
| **r25** `SELECT semver_cmp('1.2.3', '1.10.0')` | ERROR 42883: no semver in core | -1 (SemVer 2.0.0 §11 precedence; declares int4) | — | kept superset | 2026-09-11 · [E85](#e85), P101 | #967 | `expr.TestSemverPrecedenceIsTheSpecificationsOwnExample` |
| **r26** `SELECT semver_major('v1.2.3'), semver_major('01.2.3')` | ERROR 42883 | 1, NULL: a leading v/V is accepted; a leading zero is not a version; declares bigint | — | kept superset | 2026-09-11 · [E85](#e85), P101 | #967 | `expr.TestSemverParseAcceptsTheGrammarAndRefusesEverythingElse` |
| **r27** `SELECT semver_major('99999999999999999999.0.0')` | ERROR 42883 | NULL (a component past int64 is not a version); semver_normalize_strict raises 22023 | — | kept superset | 2026-09-11 · [E85](#e85), P101 | #967 | `expr.TestSemverParseAcceptsTheGrammarAndRefusesEverythingElse` |
| **r28** `SELECT semver_normalize('1.2'), semver_normalize_strict('1.2')` | ERROR 42883 | NULL; the strict twin is ERROR 22023 quoting the string; a NULL argument is NULL in both | 22023 | kept superset | 2026-09-11 · [E85](#e85), P101 | #967 | `expr.TestTheStrictTwinRaisesWhereTheLenientOneAnswersNull`, `expr.TestEverySemverFunctionIsStrictOnNull` |
| **r29** `SELECT semver_satisfies('1.2.3', '')` | ERROR 42883 (node-semver reads '' as *) | ERROR 22023 the version range is empty; write * (measured) | 22023 | kept superset | 2026-09-11 · [E85](#e85), P101 | #967 | `expr.TestARangeOffTheGrammarIsRefused` |
| **r30** `SELECT semver_satisfies('1.2.3-beta', '1.2.x-beta')` | ERROR 42883 (node-semver ignores the suffix) | ERROR 22023; a leading zero or a component past int64 inside a range is 22023 too, refused before any row | 22023 | kept superset | 2026-09-11 · [E85](#e85), P101 | #967 | `expr.TestARangeOffTheGrammarIsRefused`, `physical.TestTheBinderRefusesAConstantSemverRangeInEveryExpressionPosition` |
| **r31** `SELECT semver_satisfies('0.0.0-alpha', '>=0.0.0 <=0.0.0-alpha')` | ERROR 42883 (node-semver 7.7.3: true) | t (measured): >=0.0.0 is deleted from every comparator set, as node-semver deletes it | — | kept superset | 2026-09-11 · [E85](#e85) | #967 | `expr.TestTheTrivialLowerBoundAnswersWhatNodeSemverAnswers`, `wadjet.TestARangeWithATrivialLowerBoundAnswersWhatNodeSemverAnswers` |
| **r32** `SELECT semver_satisfies('9223372036854775807.5.0', '^9223372036854775807.0.0')` | ERROR 42883 (node-semver refuses components past 2^53-1) | t (measured): a generated bound at the top of int64 saturates rather than wrapping | — | kept superset | 2026-09-11 · [E85](#e85) | #967 | `expr.TestEveryGeneratedBoundSaturatesAtTheAcceptanceBound`, `wadjet.TestARangeAtTheAcceptanceBoundKeepsTheRowsItNames` |
| **r33** `SELECT semver_parse('1.2.3-rc.1+b')` | ERROR 42883 | ROW(1, 2, 3, 'rc.1', 'b') as (major bigint, minor bigint, patch bigint, prerelease text, build text) | — | kept superset | 2026-09-12 · [E85](#e85) | #967, #1017 | — |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E77

ADR lines 3052-3111. Catalog rows: r1, r2, r3, r4, r5, r6, r7, r8. Stated in [Mechanisms](#mechanisms).

- **`OHLCV` and `TIME_BUCKET` are EXTENSIONS, and their oracle is
  PostgreSQL spelled out.** (Added 2026-09-08, arc A1, #965, ADR-0035.)

  `TIME_BUCKET(stride, ts[, origin])` IS `date_bin(stride, ts, origin)`
  under another name, and every one of its answers and both of its refusals
  (0A000 for a stride containing months or years, 22008 for a non-positive
  one) are the server's, measured on 17.11. Two differences are wadjet's
  and both are narrowing: the default origin is `1970-01-01` where
  `date_bin` has no two-argument form at all, and the stride must be an
  INTERVAL literal (42804 otherwise) because the accepted interval grammar
  is the SQL parser's and a second one in the expression layer would agree
  with the first only by inspection — so `INTERVAL '1 day 6 hours'`, which
  the server bins with, is not spellable here.

  `OHLCV(ts, price, volume)` has no PostgreSQL equivalent, and its value
  oracle is the bar spelled out per field with the SAME row filter a
  multi-argument aggregate applies (`regr_count(y,x)` over
  `(1,1),(2,NULL),(NULL,3),(4,4)` is 2, measured). The declared field types
  are the server's for the aggregates the fields ARE — `min` of the price
  column's type, `sum(volume)`'s type, `avg(price)`'s type. Three shapes
  PostgreSQL would answer are REFUSED rather than approximated:
  `OHLCV(DISTINCT …)` (0A000 — the server dedupes on the whole argument
  tuple and this engine's distinct set for a multi-argument aggregate is
  keyed on two columns), `OHLCV(…) OVER (…)` (0A000, with every other
  aggregate that has no window arm — see the entry below).

  `(OHLCV(…)).open` inside ONE query block was refused 42809 at the
  parser when this entry was written. It ANSWERS since the A3b fixed-ROW
  seam (2026-09-12): the parser rewrites `(call).field` into
  `row_field(call,'field')` rather than refusing it, and the binder
  refuses only a container whose resolved type is decided and is not ROW
  — `aggOutputType("ohlcv")` is `TypeRow`. Measured at that revision,
  `SELECT (OHLCV(ts, price, size)).open FROM trades` answers the bar's
  open. Nothing gates the one-block spelling for an AGGREGATE container
  (the A3b gates cover a scalar one), so the derived table or CTE remains
  the spelling this ADR recommends.

  One SUPERSET: a bar whose total volume is ZERO answers a NULL `vwap`
  and keeps its four prices, where PostgreSQL's `SUM(px*vol)/SUM(vol)`
  raises 22012 (division_by_zero). A weighted mean over zero total weight
  is undefined, which is what NULL says, and raising would fail the whole
  query — every other bucket's bar with it — for one group whose volumes
  happened to cancel. The other five fields of that bar are PostgreSQL's
  exactly. Gated as `zero_volume_has_no_vwap`.

  Gated in `coordinator.TestTheBarIsTheSameOnEveryArm` (25 cells on three
  arms, including a COMPUTED argument — which is where the planner's
  declaration runs out and where three arm divergences were found),
  `coordinator.TestTheBarsDeclaredTypeIsTheSameOnEveryArm` (the DECLARATION
  rather than the values: 11 price × volume cells on five arms, with rows
  and over an empty input), `coordinator.TestABarBreaksATieByValueWhenThe
  TieIsSplitAcrossTasks` (the tiebreak with each tied row in its own file),
  `exec.TestTheBars*` (the merge law, the encoding, the tiebreak, the
  declared types, the state's self-description), `wadjet.TestTimeBucket*`,
  the `ohlcv_*` cells of `wadjet.TestTypeMatrixAnswersTheSameUnderEvery
  MemoryBudget`, and on the wire
  `pgwire.TestPGWireRendersTheBarAsAPostgresComposite`,
  `pgwire.TestPGWireDeclaresABarFieldTheSameWithRowsAndWithout` and
  `server.TestTheBarDeclaresTheSameThingOnBothWireDoors`.

### E81

ADR lines 3221-3435. Catalog rows: r9, r10, r11, r12, r13. Stated in [Mechanisms](#mechanisms).

- **The TCP flag family is an EXTENSION, and each function names the
  PostgreSQL spelling it is equivalent to.** (Added 2026-09-08, arc A2,
  #966.) PostgreSQL has no `tcp_flags_has_all`; it has `&`. The six
  functions are defined BY that arithmetic and gated against it, so the
  equivalence is the specification rather than a resemblance:

  | wadjet | PostgreSQL 17.11 |
  |---|---|
  | `tcp_flags_has_all(f, 'SYN','ACK')` | `(f & 18) = 18` |
  | `tcp_flags_has_any(f, 'SYN','ACK')` | `(f & 18) <> 0` |
  | `tcp_flags_has_none(f, 'SYN','ACK')` | `(f & 18) = 0` |
  | `tcp_flag_mask('SYN','ACK')` | the literal `18` |
  | `tcp_flags(f)` / `tcp_flags_text(f)` | no equivalent; a name↔bit join |

  Measured over `f4 int` / `f8 bigint` holding 0, 2, 18, 16, 4, 511, 24,
  NULL, 20, 256 and over the 44-row `a2_tcpflow` the arm census uses. NULL
  flags give NULL for all three, has_none included; `(-1) & 18 = 18` on both
  engines, so a negative flags value is a bit pattern and nothing
  special-cases the sign.

  TWO DELIBERATE DIVERGENCES from that equivalence, both refusals:

  - **An empty name list is 22023 where PostgreSQL's zero mask is vacuously
    true.** `(f & 0) = 0` is `t` and `(f & 0) <> 0` is `f` for every row
    there; `tcp_flags_has_all(f)` raises here. A name list with nothing in
    it is a query that meant something and did not say it, and answering
    "every row" for it is the plausible answer, not the right one.
  - **An unrecognized name is 22023 naming it, and listing the nine
    spellings.** The bit spelling has no equivalent failure — a mask is a
    number — but the alternative here is to drop the bit, which turns
    `has_all('SYN','ACKK')` into `has_all('SYN')`: a strictly LARGER row set
    that nothing downstream can tell from the intended one. A name written
    as a CONSTANT is refused BEFORE ANY ROW (see "an invalid literal name is
    refused from the declaration" below); a name supplied by a column or an
    expression is refused per row, where it first exists.

  **A NAME THE TABLE DOES NOT KNOW OUTRANKS A NULL FLAGS ARGUMENT**
  (decided 2026-09-08, round 3, #966 P4). The family says both "NULL flags
  give NULL" and "an unknown name is 22023", and did not say which wins:
  the three predicates folded the mask first and raised, while the legacy
  `has_tcp_flag` checked both arguments for NULL first and answered NULL, so
  whether a typo was an ERROR depended on the row the evaluator was on.
  PostgreSQL decides it for the operator equivalent — a malformed mask
  operand, measured on 17.11:

  | probe | PostgreSQL 17.11 |
  |---|---|
  | `SELECT NULL::bigint & 'x'::bigint` | `ERROR 22P02 invalid input syntax` |
  | `SELECT 'x'::int FROM (VALUES (1)) t WHERE false` | the same ERROR |
  | `SELECT date_trunc('BOGUS', NULL::timestamp)` | `NULL` |

  The first two are the shape here: the names are CONVERTED into the mask
  operand, and neither a NULL other operand nor an empty row set excuses a
  spelling that cannot be converted. (`date_trunc` is the strict-function
  shape, where the bad thing is an argument to a function that is never
  called; it is not this.) So the names are folded first everywhere, and a
  NULL *name* — a NULL mask operand, `NULL & NULL` — still answers NULL.
  Gated in `expr.TestAnUnknownFlagNameOutranksANullFlagsArgument` and on
  five arms by the census's `unknown_name_on_a_null_row` cells.

  **AN INVALID LITERAL NAME IS REFUSED FROM THE DECLARATION, ROWS OR NO
  ROWS** (decided 2026-09-11, round 5, #1018 B2). The paragraph above was
  written for the NULL case and stated the whole rule — "neither a NULL
  operand nor an empty row set excuses it" — while the code folded the
  names per row and the entry's own preceding sentence said so. The two
  sentences contradicted each other, and the CODE was the weaker one:
  `SELECT tcp_flags_has_all(f8,'BOGUS') FROM tcpflow WHERE id < 0` returned
  zero rows and no error on all five arms and in both wire formats, while
  the same typo over a reached row was 22023. Whether a typo was an ERROR
  depended on the data — which is the same defect P4 fixed for a NULL row,
  one step earlier.

  The rule is the binder's existing one (ADR-0012 item 1, #517/#631): a
  literal that names no value of the type its context demands is refused
  from the DECLARATION, before any row exists. So a flag-NAME argument
  written as a STRING LITERAL — to `tcp_flags_has_all/any/none`,
  `has_tcp_flag`, `tcp_flag_mask` and `tcp_flags_from_string` — is folded
  by `expr.RefuseUnknownTCPFlagNameLiterals`, with the same SQLSTATE and the
  same sentence as the evaluator's `expr.errUnknownTCPFlagName`: one
  refusal, two layers, never two rules. An EMPTY name list is refused with
  it, for the same reason — an arity is known without rows.

  **THE DECIDING LAYER IS THE BINDER, NOT COMPILATION** (amended
  2026-09-11, round 6, #1018 B1). This entry's first version put the fold at
  COMPILE time and called that one seam for every door, planned or not. That
  is true of the SINGLE-PROCESS path, where `Plan` compiles the whole
  expression tree while it builds the physical plan, and FALSE of the stage
  DAG, where a stage's fragment compiles its own expressions WHEN A TASK
  RUNS. Which expressions the coordinator compiles
  while planning and which it defers into a fragment that may never run
  differs BY POSITION, so a fold living only at compilation refused a
  misspelling on `single` and `single+budget` and answered ZERO ROWS on
  `dag`, `dag-shuffled` and `dag-morsel4` — with every local-routing counter
  flat, so the query really did run as a DAG — in four positions: `HAVING`,
  an `ORDER BY` key, a set-operation arm, and a projection above a
  `GROUP BY`. A subquery body was worse: an `EXISTS` holding the misspelling
  handed the client the coordinator's own "EXISTS subquery requires a
  SubqueryRunner" instead of any SQLSTATE, and a SCALAR subquery answered
  zero rows on all five arms. The version that introduced the fold therefore
  introduced a single-vs-DAG DISAGREEMENT where the base had agreement.

  The refusal is now asked at PLAN time by the BINDER —
  `physical.refuseUnknownFlagNames`, from `binder.checkExpr` over every
  expression position reached by the query-block walk, which `Plan` AND
  `PlanDistributed` both reach through `auth.ValidateStatementColumns`
  before any stage exists. It is deliberately NOT gated on a closed scope
  the way `checkLiteralTypes` is: it asks the scope nothing, because a name
  that names no flag names no flag whatever the FROM list turns out to be.
  The compile-time call remains a backstop **when compilation is reached**.
  It covers ADR-0031's empty-input UPDATE/DELETE predicates and UPDATE SET.
  It does not guarantee validation of an empty DAG policy filter, or an
  MERGE WHEN clause no row reaches — a MATCHED SET or AND-DELETE condition
  over an ON that matches nothing — whose compilation is lazy (#1043).
  Catalog-less table-less plans refuse distribution and compile locally.
  An ORDER BY term the binder cannot re-parse is refused by the logical
  builder with 42601. These doors are independently pinned by
  `TestTCPFlagValidationDoors` and `TestTCPFlagASTCoverage`.

  **RECURSIVE CTE BODIES ARE VALIDATED BEFORE ROWS** (2026-09-11, round 8,
  B1). `binder.registerCTE` registers the recursive self-reference as open,
  then calls the existing `validateBlock` over its body. Both UNION arms
  are visited, even with an empty seed or an unused CTE. An open schema
  prevents uncertain column-name diagnoses; it does not excuse a misspelled
  literal flag name. Previously the body was skipped, and
  `materializeRecursiveCTE` could swallow an `executeSubquery` error, so
  even reached-input seeds could lose the compile-time refusal.
  `TestTCPFlagASTCoverage` pins seed, recursive term and unused-body
  spellings over empty and reached inputs, on five arms with zero route
  deltas and both wire formats. Reverting body validation fails all six
  shapes. No declaration walk is added.

  Coverage is limited by parsed syntax: expression-valued frame bounds,
  named windows, LIMIT/OFFSET, table-function arguments, TABLESAMPLE and
  INSERT expressions in the coverage fixture are 42601 before binding.
  MERGE ON supports column equalities only (0A000). DML has no DAG planning
  path (0A000); its wire door executes locally. These are named residuals,
  not claims that every possible SQL expression reaches this walk.
  The round-8 inventory also pins `cte_shadowed_body`: `registerCTE`'s
  additive name map skips a nested body when its name already exists.
  The unused shadowing-body fixture answers zero rows on all five arms
  and both formats. It pins `set_order_by` too: validateBlock returns
  after the UNION arms without checking the wrapper ORDER BY, and
  buildSetOpPlan carries the term as a column name, not a compiled
  expression. All five arms and both formats answer zero rows; the DAG
  arms take UnreachableOutputLocalRoutes +1 before the local answer. Both reproduce with the round-8 binder fix removed. These are
  scope/set-operation coverage residuals, not recursive-body regressions.

  What stays per row is what is not knowable from the declaration: a name
  supplied by a COLUMN or by an expression, which is not a constant, and a
  NULL name, which is a NULL mask operand rather than a misspelling. Gated
  on five arms by the census's `unknown_name_with_no_rows_at_all*` and
  `position_*` cells (each position over an EMPTY input and over a REACHED
  one — the pair is the claim, since "the refusal appears the moment a row
  reaches the stage" is the defect) with their `control/position_*_valid`
  answers beside them, and on the wire in both formats by
  `pgwire.TestPGWireRefusesAnInvalidFlagNameWithNoRows` and
  `pgwire.TestPGWireRefusesAnInvalidFlagNameInEveryExpressionPosition`.
  Removing the binder call answers zero rows again on the three DAG arms for
  `position_having_empty`, `position_order_by_empty`,
  `position_union_arm_empty`, `position_projection_above_group_by_empty`,
  `position_exists_subquery_empty`, `position_derived_body_empty`,
  `position_cte_body_empty` and `position_window_argument_empty`, and on ALL
  FIVE for `position_scalar_subquery_empty`.

  **"EVERY EXPRESSION POSITION" IS A CLAIM ABOUT THE WALK, AND THE WALK HAD
  THREE BLIND SPOTS** (amended 2026-09-11, round 7, #1018 B1). The sentence
  above was written for the positions the BINDER re-parses; the positions a
  SUBQUERY BODY can hide in are decided by a different collector, and that
  collector — `binder.blockSubqueries` — walked WHERE, HAVING, QUALIFY, the
  SELECT items and the JOIN conditions and NOT `GROUP BY` or `ORDER BY`,
  while `walkExpr` stopped dead at a `WindowFuncNode`. So a subquery
  written in one of those three positions was reached by no walk at all:
  `… ORDER BY (SELECT TCP_FLAG_MASK('BOGUS') …)`, the `GROUP BY` spelling of
  it and `SUM((SELECT TCP_FLAG_MASK('BOGUS') …)) OVER ()` each answered ZERO
  ROWS AND NO ERROR in both wire formats over an empty input. The same
  stop hid a window with no subquery in it at all once the call was wrapped:
  `1 + SUM(TCP_FLAG_MASK('BOGUS')) OVER ()` put the window node one level
  below the top-level special case the refusal carried, and answered zero
  rows on the three DAG arms with every routing counter flat.

  There is now ONE walk and no special case: `walkExpr` descends through a
  `WindowFuncNode` — its argument, its PARTITION BY / ORDER BY terms and its
  frame offsets — for all three of its collectors, and `blockSubqueries`
  asks it of GROUP BY and of each re-parsed ORDER BY item as well.
  Descending for NAME RESOLUTION costs no concession: PostgreSQL 17.11
  resolves an OVER term against the INPUT relation, where an output alias is
  `column "a" does not exist`, which is the scope the binder already passes.
  Gated by `refusal/position_orderby_scalar_subquery_*`,
  `position_groupby_scalar_subquery_*`,
  `position_window_arg_scalar_subquery_*`,
  `position_nested_window_in_arithmetic_*` and
  `position_orderby_nested_scalar_subquery_empty` on five arms, and by the
  same five names in both wire formats. Narrowing the walk back fails eight
  census cells and eight wire subtests.

  `tcp_flags_from_string`, which reads a COMMA-SEPARATED list rather than an
  argument list, splits the two cases and answers the arithmetic where it
  can (decided 2026-09-08, round 2): an EMPTY string is a list of no names
  and answers `0` — the mask of no names, which is what it answered before
  this arc and what a telemetry column spelling "no flags" as the empty
  string needs — while an empty ELEMENT (`'SYN,'`, `'SYN,,ACK'`) is 22023
  naming the POSITION, because a list that names something and then names
  nothing is a slip, and quoting the name would quote nothing.

  Names are case-insensitive; `NS` is accepted as an input alias for `AE`
  and `AE` is the canonical rendering. RFC 9293 §3.1 defines the eight
  control bits CWR..FIN and a four-bit reserved field; bit 8 is RFC 3540's
  `NS` (Historic per RFC 8311), reused as `AE` by the Accurate ECN work. `tcp_flags` / `tcp_flags_text` name only those nine bits: a bit
  outside the table is not a TCP flag and is not named, which is a rendering
  contract and deliberately NOT a refusal — a garbage byte in one row of a
  telemetry column must not fail the query. Gated in
  `expr.TestTheTCPFlagPredicatesAreTheBitArithmetic`,
  `wadjet.TestTheTCPFlagFamilyAnswersPostgresBitArithmeticEndToEnd` and
  `coordinator.TestTheTCPFlagFamilyAnswersPostgresBitArithmetic` (five arms).

### E82

ADR lines 3436-3784. Catalog rows: r15, r16, r17, r18, r19, r20, r21, r22, r23, r24. Stated in [Mechanisms](#mechanisms).

- **The bitwise family reads its argument exactly and answers an integer;
  `bigint` where PostgreSQL answers `int4` for int4 operands.**
  (Added 2026-09-08, arc A2, #966; extended the same day in round 2 to the
  whole family.) `pg_typeof(2::int4 & 18::int4)` is `integer` and
  `pg_typeof(2::int8 & 18::int8)` is `bigint`, measured on 17.11; this
  engine declares bigint for both. A value-preserving widening.

  It is recorded because the VALUE half was a real divergence and is fixed.
  TWELVE functions carried an integer argument through a float64, so above
  2^53 the low bits were rounded away before the operation ran:
  `bitwise_and/or/xor/not`, the three shifts, `to_hex`, `to_base`,
  `bit_count`, and — through their FLOAT64 declaration rather than their
  body — `from_hex` and `from_base`. Measured against 17.11 over
  `4611686018427387922` (2^62 | 18):

  | spelling | PostgreSQL | before |
  |---|---|---|
  | `x & 18` | `18` | `0` |
  | `x >> 1` | `2305843009213693961` | `2305843009213693952` |
  | `x << 1` | `-9223372036854775772` | `-9223372036854775808` |
  | `x >> 0` (the IDENTITY) | `4611686018427387922` | `4611686018427387904` |
  | `to_hex(x)` | `4000000000000012` | `4000000000000000` |
  | `bit_count(x::bit(64))` | `3` | `1`, boxed float64 |
  | `from_hex('4000000000000012')` | — | `4.611686018427388e+18` |

  The rule is now one rule: an integer argument is read exactly, and a
  function whose answer is an integer declares one. `numericFuncCall.
  EvalInt64` — the seam integer ARITHMETIC reads such a function through —
  converts exactly too, because the declaration change opened it:
  `BITWISE_OR(f8,1)` answered `4611686018427387923` and
  `BITWISE_OR(f8,1) + 0` answered `4611686018427387904`, two spellings of
  one value disagreeing.

  FIVE RESIDUAL DIVERGENCES in that family, stated rather than glossed.
  They are LIMITATIONS, not cells where this engine was measured to agree
  with PostgreSQL: no gate counts any of them as agreement, and the arm
  census normalizes or labels each one where it appears (#966 round 2, N3).
  The int4-operand widening at the head of this entry was filed as #1018
  and **CLOSED 2026-09-15 (arc ND)**: `BITWISE_AND/OR/XOR/NOT` over int4
  operands declares `integer` (OID 23) now, following its operands the way
  `+ - *` already did. AND, OR, XOR and NOT over two int4 values produce an
  int4 value by construction, so the narrower output vector can hold every
  answer they have — which is why the three SHIFTS are NOT in it and stay
  bigint (see the shift entries below): this engine shifts on the int64
  carrier and PostgreSQL's int4 shift is MODULAR, so `2147483647 << 2` is
  8589934588 here and -4 there, and declaring int4 for it would turn a
  query the server answers into a 22003. `expr.PGIntegerResult.FitsOperands`
  is where that distinction lives, and
  `coordinator.TestNDDeclarationsMatchPostgres`'s `1018/*` cells hold both
  halves. `SUM(BITWISE_AND(int4_col, 18))` was already PostgreSQL's
  `bigint` and is unchanged.

  - **`BITWISE_RIGHT_SHIFT` is Trino's LOGICAL shift, not PostgreSQL's
    `>>`.** PostgreSQL's `>>` on an integer is arithmetic (sign-preserving):
    `(-1)::int8 >> 1` is `-1` there. That is
    `BITWISE_ARITHMETIC_SHIFT_RIGHT` here, and it agrees with PostgreSQL
    value for value. The name `bitwise_right_shift` comes from Trino, which
    has both, and it keeps Trino's meaning.
  - **A shift COUNT outside `[0,64)` answers NULL where PostgreSQL answers a
    number.** PostgreSQL takes the count modulo the width — `1::int8 << 64`
    is `1`, `1::int8 << 65` is `2`, `8 >> -1` is `0`. NULL is visible rather
    than plausible, and changing it is a Trino-vs-PostgreSQL semantics
    decision this arc did not take.
  - **`TO_HEX` renders the SIXTY-FOUR-BIT word for every column argument.**
    The width comes from the value's box, and an INT32 column's value
    reaches a scalar function here as an int64, so `to_hex(int4_col)` over a
    negative renders sixteen sign-extended digits where PostgreSQL renders
    eight (`ffffffffffffffff` vs `ffffffff`). The NUMBER is the same two's
    complement and a non-negative argument renders identically; only the
    leading `f`s differ. The same applies to an untyped negative LITERAL,
    which is `integer` in PostgreSQL and bigint here. Measured on all five
    arms (`coordinator.…/to_hex_of_an_int32_column_*`), so a change to the
    boxing would be noticed rather than assumed.
  - **`TO_BASE` renders a negative value as a signed string** (`-ff`), which
    is Trino's rendering; PostgreSQL has no `to_base`. `TO_HEX` is
    PostgreSQL's function and renders the machine word, so the two disagree
    on a negative argument by design.
  - **`FROM_HEX` answers NULL for text that is not hexadecimal, where
    PostgreSQL's decoder RAISES.** (Decided 2026-09-08, round 3, #966 N2.)
    `decode('12zz','hex')` is 22023 on 17.11; `FROM_HEX('12zz')` is NULL
    here, which is what this engine's own `FROM_BASE('12z',16)` answers and
    what the rest of its parse-or-NULL family does. What it must NOT do is
    what it did: `fmt.Sscanf(..., "%x")` consumed the valid PREFIX and
    reported success, so `FROM_HEX('12zz')` was 18 — a number derived from
    text that is not a number, and the opposite of the sibling function's
    answer for the same input. The whole string is required now. A 16-digit
    word with the top bit set stays NULL: the result is a signed int64 and
    the value does not fit one, which is also FROM_BASE's answer. Gated in
    `expr.TestFromHexRequiresTheWholeString`, which checks both spellings on
    every cell.

  Gated in `expr.TestABitwiseOperatorIsExactOverASixtyFourBitPattern`,
  `expr.TestTheWholeBitwiseFamilyIsExact` (a twelve-row PostgreSQL
  transcript × ten functions, including both int64 extremes and the identity
  shift), `expr.TestToHexRendersTheArgumentsOwnWidth`,
  `expr.TestTheBitwiseFamilyDeclaresIntegers`,
  `expr.TestAnIntegerFunctionsValueSurvivesTheArithmeticAboveIt`, and the
  `bitwise_*` cells of the five-arm census.

  **The knock-on of the declaration change is wider than the four
  functions, and the aggregate half of it was WRONG when it was first
  written here.** `BITWISE_AND(3,3)/2` was `1.5` and is `1`, which is what
  PostgreSQL answers for integer division; that half was right.

  The aggregate half said `SUM(BITWISE_AND(f,18))` comes back `bigint` and
  called that PostgreSQL-correct. It is not. PostgreSQL's rule is by the
  OPERAND'S WIDTH — `sum(int4)` is bigint, `sum(int8)` is numeric — and
  `f8 & 18` is bigint there, so its SUM is NUMERIC (measured on 17.11:
  `pg_typeof(sum(f8 & 18))` is `numeric`, `pg_typeof(sum(f4 & 18))` is
  `bigint`). Taking the bigint accumulator for an int8-domain operand does
  not merely mislabel the answer: over two rows of 2^62 it REFUSES with
  22003 where PostgreSQL answers 9223372036854775808, and the same
  expression answered 2^63 as a float64 before the declaration changed — a
  right value turned into an error (#966 round 2, B1).

  The first repair of this read the FUNCTION'S OWN `Ret` DECLARATION and
  was wrong in the other direction (#966 round 3 review, B1). `Ret` names
  the VECTOR a result is stored in, not the type PostgreSQL calls it, and
  every integer in this engine computes in an int64 (ADR-0024's widening) —
  so `regexp_count`, whose PostgreSQL result is `integer`, declares
  `RetInt64` exactly as `bit_count`, whose PostgreSQL result is `bigint`,
  does. Reading the carrier as a width made `SUM(REGEXP_COUNT(…))`,
  `SUM(PREFIX_LENGTH(…))` and `SUM(PAYLOAD_LENGTH(…))` declare NUMERIC
  where PostgreSQL declares BIGINT, grouped and windowed, in both wire
  formats.

  **The rule is PostgreSQL's result width for the FUNCTION, from one
  table**: `expr.PGIntegerResultWidth`, which
  `physical.aggInputIsWideInteger` is the only reader of, so the grouped
  and the windowed spelling — which already share that walk — cannot
  disagree. An entry is decided in this order: PostgreSQL's measured
  `pg_typeof` where PostgreSQL has the function, and otherwise the width
  that holds the function's whole DOMAIN, which is the criterion
  PostgreSQL applied to its own (`masklen` is `integer` because a prefix
  length is 0..128; `bit_count` is `bigint` because a bytea's bit count is
  not bounded by int4). Measured on 17.11:

  | expression | PostgreSQL result | `sum(…)` |
  |---|---|---|
  | `regexp_count('abab','a')` | `integer` | `bigint` |
  | `masklen('10.0.0.0/24'::cidr)` | `integer` | `bigint` |
  | `octet_length('abc')` | `integer` | `bigint` |
  | `length(s)`, `strpos`, `cardinality`, `ascii` | `integer` | `bigint` |
  | `pg_backend_pid()` | `integer` | `bigint` |
  | `bit_count('\x0102'::bytea)` | `bigint` | `numeric` |
  | `txid_current()` | `bigint` | `numeric` |
  | `f4 & 18`, `f4 \| 18`, `~f4`, `f4 << 2` | `integer` | `bigint` |
  | `f8 & 18`, `f8 \| 18`, `~f8`, `f8 << 2` | `bigint` | `numeric` |

  The last two rows are the BITWISE family, and they are why the table has
  a third answer beside int4 and int8: that family's width **follows its
  operands**, exactly as arithmetic does, and a SHIFT follows the value it
  shifts rather than the count. So `SUM(BITWISE_AND(int4_col, 18))` is
  `bigint` and `SUM(BITWISE_AND(int8_col, 18))` is `numeric` — both
  PostgreSQL's answers, which retires the divergence the first repair
  recorded here.

  A function that returns an integer and has no row in that table is how
  the defect comes back, so
  `expr.TestEveryIntegerDeclaredFunctionNamesItsPostgresResultWidth`
  asserts the table and the registry name the same set in BOTH directions,
  and `expr.TestThePostgresResultWidthTableMatchesTheMeasuredTranscript`
  holds the rows against the transcript above. On the wire:
  `pgwire.TestPGWireDeclaresSumOverAnIntegerFunction` — thirteen cells, OID
  1700 for the int8 side, OID 20 for the int4 side, and the AVG controls
  numeric on both. On five arms:
  `coordinator.TestTheTCPFlagFamilyAnswersPostgresBitArithmetic`'s `sum_*`
  cells.

  **THE WIDTH IS A PROPERTY OF THE COLUMN'S DECLARATION AND SURVIVES
  MATERIALIZATION** (decided 2026-09-11, round 5, #1018 B1). The repair
  above reads the table while the CALL is still visible in the AST, and
  that is not everywhere the width is needed: a derived table, a CTE, a
  set-operation arm and a window slot MATERIALIZE the expression into a
  column, and every integer materializes as INT64 (ADR-0024's recorded
  widening). So the direct `SUM(BITWISE_AND(id,3))` declared bigint and
  `SELECT SUM(v) FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s`
  declared numeric — the same number in two boxes, depending only on
  whether the call had been materialized. Measured against 17.11:

  | shape | PostgreSQL | before | now |
  |---|---|---|---|
  | `SUM(id & 3)` | `bigint` | `bigint` | `bigint` |
  | `SUM(v)` over `(SELECT id & 3 AS v)` | `bigint` | **`numeric`** | `bigint` |
  | `SUM(v) OVER ()` over the same | `bigint` | **`numeric`** | `bigint` |
  | `SUM(v)` over `(SELECT regexp_count(name,'a') AS v)` | `bigint` | **`numeric`** | `bigint` |
  | `SUM(v)` over `(SELECT id * 2 AS v)` | `bigint` | **`numeric`** | `bigint` |
  | a CTE over that derived table | `bigint` | **`numeric`** | `bigint` |
  | a UNION ALL of two int4 arms | `bigint` | **`numeric`** | `bigint` |
  | `SUM(v)` over `(SELECT visits & 18 AS v)` | `numeric` | `numeric` | `numeric` |
  | a UNION ALL with ONE int8 arm | `numeric` | `numeric` | `numeric` |
  | `SUM(v)` over `(SELECT CAST(id AS BIGINT) AS v)` | `numeric` | `numeric` | `numeric` |

  **DECLARED WIDTH**, precisely, is the int4/int8 domain an output column
  carries BESIDE its carrier, exactly as a DECIMAL's (p,s) rides beside
  `TypeDecimal`: `physical.colDecls.intWidth`, filled by
  `physical.emittedColIntWidth` for every node kind `emittedColTypes`
  walks, and read by `declaredIntWidth`'s ColRef arm and by
  `aggIntegerInputWidth` / `windowBareArgWidth` for a bare argument. It is
  THREE-valued — int4, int8, and UNKNOWN — and unknown is not int4: a
  declaration that says nothing leaves the reader on the carrier, which
  for a base column IS the catalog's storage width. The rules, one per
  producer: a CALL takes `expr.PGIntegerResultWidth`'s row; arithmetic and
  the bitwise family take the WIDEST integer operand; a bare column takes
  its catalog type (INT32/PORT/PROTOCOL are int4, INT64/DURATION int8); a
  CAST takes its TARGET NAME; an integer literal is int4 unless it does not
  fit; an aggregate takes its own declared result's width, except
  MIN/MAX/MIN_BY/MAX_BY, which hand back a value the input HELD and keep
  its width — the ARGUMENT's width, whether the argument is a bare column
  or an EXPRESSION, which is the same walk one level down (amended
  2026-09-11, round 6, P1: the computed case was declined, and declining
  was not silence, because the caller then recorded the INT64 CARRIER, so
  `MIN(BITWISE_AND(int4_col,3))` positively claimed int8 and its SUM went
  out numeric where PostgreSQL 17.11 answers `integer` for the MIN and
  `bigint` for the SUM — measured for `min(id & 3)`, `max(id & 3)`,
  `min(regexp_count(name,'a'))`, grouped and `OVER ()`); a set operation
  takes the WIDEST arm and records nothing at all if any arm is silent,
  because narrowing on incomplete information is how a SUM that should be
  numeric comes back as a bigint that can overflow.

  The WINDOW spelling of MIN/MAX had the same gap one layer earlier: over a
  computed argument its slot declared FLOAT8 — not in the integer family at
  all, so no width could be recorded for it and `SUM(m)` over a derived
  `MIN(f & 18) OVER ()` went out as OID 701 where PostgreSQL declares
  bigint (int4 argument) or numeric (int8). `windowSpecOutputType`'s
  undecided arm now types the computed argument with the SAME
  `windowComputedArgDecl` its SUM/AVG arm reads and vets it through
  `exec.WindowMinMaxType`, so the planner and the operator cannot come to
  different conclusions.

  What it is NOT is an INT32 vector. The engine computes every integer
  expression in an int64, and declaring the narrow carrier for it would put
  every such value in front of the #361 store guard. The width is metadata
  beside the carrier and never the carrier.

  Gated on all five arms by
  `coordinator.TestTheTCPFlagFamilyAnswersPostgresBitArithmetic`'s
  `derived_*`, `cte_over_*`, `a_union_all_*` and `windowed_sum_over_*`
  cells, and on the wire in both formats by
  `pgwire.TestPGWireDeclaresSumOverAMaterializedIntegerColumn` (22 cells
  x 2 formats). Reverting the ColRef arm to the carrier fails the derived
  cells while the direct cells still pass.

  **A SCALAR SUBQUERY'S COLUMN CARRIES ITS DECLARATION THROUGH A
  MATERIALIZATION TOO** (added 2026-09-11, round 6, P2). A subquery is a
  whole second query whose type lives in the CATALOG, so only a Planner can
  answer it — and the declaration walks are free functions over the logical
  tree that hold none. `colDecls.subqueryDecl` was nil in every one of them
  and the only caller that passed a resolver was `declaredOutputSchema` at
  the OUTPUT projection, so a scalar-subquery column MATERIALIZED one level
  down — by a derived table, a CTE, a window slot — was declared STRING and
  every reader above it fell to float8. `SELECT SUM(v) FROM (SELECT (SELECT
  c & 3 FROM u) AS v FROM t) s` declared OID 701 on all five arms and in
  BOTH wire formats where PostgreSQL declares bigint, and the int8, bare
  int8 column and `COUNT(*)` forms declared 701 where PostgreSQL declares
  numeric. The binary rendering confirmed a real float8 on the wire, and a
  float64 accumulator over a wide bigint drops digits past 2^53 — the class
  this ADR's numeric rules exist to prevent.

  The declaration is now STAMPED on the plan: `physical.
  annotateSubqueryColumnDecls` runs once, beside the pass that puts
  ScanColTypes on a Scan, resolves every scalar subquery's output column
  with the SAME `subqueryOutputColumn` the boxed comparison already uses,
  and shares ONE map — keyed by the subquery's SQL, carrying type, (p,s) and
  the INTEGER WIDTH — with every node of the plan, so whichever node a walk
  holds can ask. Threading a resolver through the walks instead would make
  each of them plan a second query at every level of a nested derived table,
  which is the cost #1034 records against them; the stamp adds no walk, and
  the resolution is memoized per Planner by SQL text with an in-flight
  sentinel, since resolving a subquery plans it and planning it annotates
  ITS projections in turn.

  Gated in both wire formats by
  `pgwire.TestPGWireDeclaresSumOverAScalarSubqueryColumn` and on five arms
  by the census's `scalar_subquery/*` cells, which assert the ROUTE beside
  the rows (`ScalarProjectionLocalRoutes`, and nothing else, moves: the DAG
  refuses to stage this shape and runs it in process, and rows alone cannot
  tell that from execution). Disabling the stamp fails 14 wire subtests and
  five census cells on all five arms ("v=float:72" for PostgreSQL's
  "v=int64:72").

  **AND WRITTEN DIRECTLY AS THE AGGREGATE'S ARGUMENT** (added 2026-09-11,
  round 7, B3). Round 6 left `SUM((SELECT …))` as a residual and described
  it as "LOUD-or-declaration rather than a wrong value". That description
  was FALSE, and the round-6 review measured it: over three rows
  `SELECT SUM((SELECT CAST(9007199254740993 AS BIGINT))) AS v FROM users`
  answered **27021597764222976** as float8 where PostgreSQL 17.11 answers
  the exact **27021597764222979** as numeric — a WRONG VALUE past 2^53,
  the class this ADR's numeric rules exist to prevent, in both wire
  formats. The mechanism was one line short of the stamp above: an
  aggregate whose argument is an expression is materialized into a
  synthetic pre-aggregate column, and `physical.buildAggregate` typed that
  column with `emittedColDecls` alone — no subquery resolver — so a bare
  `SubqueryNode` argument took `inferProjectionDeclType`'s FLOAT64 fallback
  and the accumulator ran over a float64 vector. The DERIVED spelling of
  the same query was already exact, because there the stamp reaches the
  column through the Project.

  The WINDOW spelling had a second copy of the same gap, with a different
  fallback: `physical.resolveWindowKeys` materializes a computed window
  ARGUMENT as a `__winkey_N` column and typed it from a walk with no
  subquery resolver either, whose fallback is STRING — so
  `SUM((SELECT MAX(id) FROM t)) OVER ()` materialized a TEXT vector and the
  window aggregate read NULL out of it in every row where PostgreSQL
  answers 9, while `MAX` of the same argument answered the right digits
  under OID 25. Both sites now read the stamp. On the stage DAG that shape
  additionally staged a window key a worker cannot compile — a worker's
  fragment has no SubqueryRunner — and handed the client
  "compile window key …: subqueries require a SubqueryRunner" after three
  attempts; `refuseScalarSubqueryProjections` now refuses the PLAN for a
  window term holding a subquery, the same answer and the same local route
  it has always given a SELECT-list item holding one.

  Gated by `pgwire.TestPGWireDeclaresSumOverAScalarSubqueryColumn`'s seven
  `direct_*` / `windowed_direct_*` cells in both formats and by the
  census's `scalar_subquery/direct_*` and `nested_scalar_subquery_*` cells
  on five arms. Disabling the two reads fails 14 wire subtests and seven
  census cells; disabling the window staging refusal alone fails the two
  windowed census cells on the three DAG arms.

  **The BIGINT case above is exact; a wide DECIMAL literal is not**
  (round 8, N1, #1037). `SUM((SELECT CAST(9007199254740993.25 AS
  DECIMAL(30,2))))` over three users returns **27021597764222982** under
  numeric OID 1700; PostgreSQL 17.11 returns **27021597764222979.75**.
  The literal has already rounded through float64 before the aggregate
  reads it. The reviewer reproduced the direct CAST at base `6fc99b39`:
  **9007199254740994.00**, versus PostgreSQL's **9007199254740993.25**.
  This pre-existing literal-ingestion defect is not repaired by declaring
  the scalar-subquery argument. `TestScalarSubqueryAggregateMatrix` pins
  all 24 divergent SUM/AVG/MIN/MAX plain/grouped/window wire cells, fails
  on agreement, and asserts the other 156 values. Quoted DECIMAL input is
  an exact control. The int4 MIN/MAX scalar argument also retains the
  existing bigint declaration (OID 20 versus PostgreSQL 23); its values
  agree and the declaration is pinned separately in the same fixture.

  NOT closed, and recorded rather than quietly dropped: a SET-OPERATION ARM
  holding a scalar subquery is still declared TEXT beside a bigint arm, so
  that UNION is refused 42804 where PostgreSQL answers. It is pre-existing,
  it is a LOUD refusal rather than a wrong value, its declaration comes from
  a walk the stamp does not reach (`setOpArmSchemas`), and it belongs to
  #874's family.

  The SCALAR declaration is a separate, pre-existing divergence and this
  entry does not close it: `SELECT REGEXP_COUNT('abab','a')` still declares
  OID 20 where PostgreSQL declares OID 23, because the engine has no int4
  result carrier for a function body that returns a Go `int64`. Only the
  aggregate's width was ever decided from it, and that is what now reads
  PostgreSQL's table instead.

### E83

ADR lines 3785-3807. Catalog rows: r12. Stated in [Mechanisms](#mechanisms).

- **`tcp_flags` declares an ARRAY and a top-level projection of it is
  TEXT.** (Added 2026-09-08, arc A2, #966; the limitation predates it.)
  `physical.funcReturnType` (unexported, `internal/planner/physical/
  declared_output.go`) declines ARRAY/MAP-returning functions and ROW functions without fixed fields — a projection has no element type to size the child vector with
  — so `SELECT tcp_flags(f)` is declared TEXT and the client is handed Go's
  rendering of the slice (`[SYN ACK]`) rather than a slice or PostgreSQL's
  `{SYN,ACK}`. `map_keys`, `map_values` and `map_entries` have answered that
  way since they were added. The value IS an ARRAY where a consumer reads it
  as one: `element_at(tcp_flags(f), 1)` is `SYN` and `array_length` is 2.
  On the wire it is OID 25 — and since #992 that is no longer what an ARRAY
  COLUMN declares, so this projection is now text where a stored array of
  the same element would be `text[]` (1009). Pinned in
  `wadjet.TestATopLevelTCPFlagsProjectionIsTextToday`, which FAILS when a
  projection can carry an ARRAY declaration. Fixed-schema ROW declarations
  travel through the complete column declaration independently (A3b);
  ARRAY/MAP element declarations remain #1017's open class.
  **CLOSED 2026-09-24 (arc CW, #1017):** the registry's container returns
  carry their element (`tcp_flags`, `current_schemas`: text; the MAP
  functions: derived from the argument's key/value), so `SELECT
  tcp_flags(f)` declares `text[]` (1009) and renders `{SYN,ACK}` on every
  door; the pin `wadjet.TestATopLevelTCPFlagsProjectionIsTextToday` was
  deleted as the proof (ADR-0045).

### E84

ADR lines 3808-3827. Catalog rows: r11, r14. Stated in [Mechanisms](#mechanisms).

- **`has_tcp_flag` and `tcp_flags_from_string` now REFUSE a name they do not
  know.** (Added 2026-09-08, arc A2, #966.) They predate the family above
  and carried their own eight-entry name table in a `byte`. Three
  consequences, all fixed by folding them onto the one table:
  `tcp_flags_to_string(256)` answered the empty string for a value with the
  AE bit set (a set flag rendered as no flag at all);
  `has_tcp_flag(f,'AE')` answered NULL, indistinguishable from "the flags
  value is NULL"; and `tcp_flags_from_string('SYN,ACKK')` answered 2, a
  silently smaller mask. The first is a value fix; the other two are
  refusals (22023) where NULL and a wrong number used to be returned.
  `is_tcp_handshake` and `is_tcp_reset` are value-identical — their masks
  are all below 256, so the truncation never reached them.

  All five also refuse a flags argument that is not an integer (22023, with
  the type named) where they used to read it as zero and answer "no flags
  set" for every row — `byte(ToInt64(v))` turned a TEXT column into 0. Same
  rule as the new family's, and the same reason: a plausible answer for an
  argument the function cannot read is the one shape a caller cannot
  detect.

### E85

ADR lines 3828-4160. Catalog rows: r25, r26, r27, r28, r29, r30, r31, r32, r33. Stated in [Mechanisms](#mechanisms).

- **The semver family is an EXTENSION, and the SPECIFICATION is its
  oracle because PostgreSQL has no semver at all.** (Added 2026-09-11, arc
  A3, #967.) `SELECT count(*) FROM pg_proc WHERE proname ILIKE '%semver%'`
  is **0** on 17.11, measured. There is a third-party `semver` extension;
  it is not core, and this ADR's authority rule is about the PostgreSQL a
  client connects to. So Semantic Versioning 2.0.0 §11 decides precedence.

  Where a PURE-SQL SPELLING exists it is the value oracle and was measured
  as one. The version CORE is an integer tuple and PostgreSQL compares
  integer arrays element-wise:

  | wadjet | PostgreSQL 17.11 |
  |---|---|
  | `semver_major/minor/patch(v)` | `(string_to_array(v,'.')::int[])[1..3]` |
  | `semver_cmp(a,b) < 0` (core only) | `string_to_array(a,'.')::int[] < string_to_array(b,'.')::int[]` |
  | `semver_valid`, `semver_prerelease`, `semver_build` | no equivalent |
  | `semver_sort_key`, `semver_normalize` | no equivalent |

  `(string_to_array('1.2.3','.')::int[]) < (string_to_array('1.10.0','.')::int[])`
  is `t` there, and so is the `1.2.10` pair — the two rows a TEXT sort gets
  backwards, and the reason this family exists. PostgreSQL has no spelling
  at all for §11.3 (a pre-release ranks below its release) or §11.4
  (identifier-by-identifier comparison), so the specification decides those
  and the gates quote its own example chain.

  **NULL FOR DATA, LOUD FOR THE QUERY'S OWN TEXT.** Every function answers
  NULL for a string that is not a version — a `WHERE` over a version column
  collected from the wild must filter rather than abort, and such a column
  always holds junk. `semver_normalize_strict` is the loud twin, 22023 with
  the string quoted, for a job that asserts instead of filtering. A NULL
  argument is NULL in both, because a NULL is an absent value and not a
  malformed one, which is what every strict function in PostgreSQL does.
  `semver_prerelease` and `semver_build` answer the EMPTY STRING for a
  valid version that has none, so `IS NULL` keeps its one meaning: not a
  version.

  TWO DELIBERATE DIVERGENCES FROM THE SPECIFICATION, both recorded rather
  than hidden:

  - **A leading `v` or `V` is accepted.** The specification says the `v`
    prefix is not part of a semantic version. Every real dataset has it —
    git tags, GitHub releases, Go module versions — and accepting it cannot
    produce a wrong value, because `v1.2.3` and `1.2.3` ARE the same
    version and `semver_normalize` renders the specification's spelling.
    It is the only concession: `' 1.2.3'`, `'1.2'` and `'01.2.3'` are NULL.
  - **A numeric identifier past `int64` is not a version here.** §9 bounds
    a numeric identifier at nothing, so `99999999999999999999.0.0` is valid
    there and NULL here (22023 in the strict form). The alternative is to
    carry it as text and compare it as text, which is the defect the family
    exists to fix. That bound is also the WIDTH `semver_major`,
    `semver_minor` and `semver_patch` declare, so the acceptance rule and
    the declaration are one number.

  **THE THREE COMPONENT FUNCTIONS DECLARE int8, NOT int4** — and the choice
  is recorded because int4 was the default named for "a component" when
  this arc was briefed. `expr.PGIntegerResultWidth` decides an entry by
  PostgreSQL's measured `pg_typeof` where PostgreSQL has the function and
  otherwise by the width that HOLDS THE FUNCTION'S WHOLE DOMAIN. PostgreSQL
  names no width of its own here — `split_part(v,'.',1)::int` is `integer`
  and `::bigint` is `bigint`, the user's own cast deciding — and the domain
  is the int64 one above, so int4 does not hold it. Declaring int4 over an
  int8 domain puts `SUM(semver_major(v))` in an int64 accumulator that
  refuses with 22003 where the true total is representable, which is
  exactly why PostgreSQL's own `sum(int8)` is `numeric`. `semver_cmp`
  declares **int4**: its domain is exactly {-1, 0, 1}.

  **`semver_sort_key` IS THE ORDERING, AS BYTES.** It renders a version as
  printable ASCII whose byte order equals §11 precedence, so `ORDER BY`,
  `MIN`/`MAX`, a `GROUP BY` key, the DAG's merge and a spilled external
  sort all order versions with no comparator of their own and nothing new
  for the distributed path to learn. Two consequences are contracts:
  build metadata is NOT in the key (§10 gives it no precedence, so
  `1.0.0+a` and `1.0.0+b` produce the same key and compare equal), and the
  key is therefore not a canonical form — `semver_normalize` is. Only the
  key's ORDER is a contract; its bytes are not.

  The one property the key rests on is an ordering between its structural
  bytes and the identifier alphabet `[0-9A-Za-z-]`, whose minimum byte is
  `-` (0x2D): the identifier SEPARATOR must sort BELOW it, and `,` (0x2C)
  is the one used. A key that joins identifiers with `.` (0x2E) instead is
  right on almost every pair and inverts `1.0.0-alpha.1` against
  `1.0.0-alpha-x`, because `-` sorts under `.` as bytes while §11.4
  compares identifier by identifier. PostgreSQL confirms the trap under
  the `C` collation the oracle database uses: `'alpha-x' > 'alpha.1'` is
  `f` there, and `1.0.0-alpha.1 < 1.0.0-alpha-x` by the specification.

  Gated in `expr.TestSemverPrecedenceIsTheSpecificationsOwnExample` (the
  §11.4 chain, every ordered pair and the equality diagonal),
  `expr.TestBuildMetadataHasNoPrecedence`,
  `expr.TestSemverParseAcceptsTheGrammarAndRefusesEverythingElse` (the
  boundary from both sides), `expr.TestEverySemverFunctionIsStrictOnNull`,
  `expr.TestTheStrictTwinRaisesWhereTheLenientOneAnswersNull`,
  `expr.TestTheSortKeysStructuralBytesOrderBelowEveryIdentifierByte` and
  `expr.TestTheSortKeysByteOrderIsPrecedenceOverAGeneratedCorpus` (5000
  seeded versions, both sortings compared element by element plus 200k
  sampled pairs).

  **`semver_satisfies` IMPLEMENTS node-semver's PUBLISHED RANGE GRAMMAR,
  because the specification has none.** (Added 2026-09-11, arc A3, #967.)
  Semantic Versioning 2.0.0 defines PRECEDENCE and no range syntax at all.
  The syntax people actually write is node-semver's — a `package.json`
  dependency, a Dependabot alert, a Renovate rule, an advisory's
  affected-versions field — and its README's "Advanced Range Syntax"
  publishes an EXPANSION for every spelling. Those expansions are the
  oracle, transcribed into
  `expr.TestTheRangeGrammarMatchesTheNodeSemverTable`, which checks each
  one twice: the desugaring RENDERS to the published text, and the range
  and its published expansion admit the same versions over a corpus. The
  pre-release rule is the README's own sentence — a version with a
  pre-release tag satisfies a comparator set only if some comparator of
  that set names the same [major, minor, patch] AND itself has a
  pre-release — gated in `expr.TestTheNodeSemverPrereleaseRule`.

  The `-0` on every exclusive upper bound (`<2.0.0-0`, not `<2.0.0`) is
  part of the published expansion and is load-bearing: `2.0.0-beta` is
  below `2.0.0` by §11.3 and above `2.0.0-0` by §11.4, so dropping it
  admits pre-releases of the next major into `^1.2.3`. `includePrerelease`
  is not implemented; a query that wants pre-releases writes a comparator
  that has one.

  A RANGE THIS GRAMMAR DOES NOT KNOW IS 22023 NAMING IT, NEVER A SILENT
  FALSE. The version argument is DATA and keeps the family's NULL; the
  range is the QUERY AUTHOR'S OWN TEXT, so a spelling nobody implements is
  a property of the query, and `false` for it drops every row the author
  meant to select while looking exactly like an empty table. The range is
  read FIRST — before a NULL or unparseable version argument is consulted —
  which is the same ordering arc A2 settled for a flag NAME against a NULL
  flags argument, and for the same reason: whether a typo is an error must
  not depend on the rows. A NULL range is a NULL operand and answers NULL,
  because a NULL is not a misspelling.

  THREE DELIBERATE DIVERGENCES FROM node-semver, all refusals:

  - **The EMPTY range is 22023 where node reads `''` as `*`.** Same
    position as A2's empty flag list: a range with nothing in it is a query
    that meant something and did not say it, and "every row" is the
    plausible answer rather than the right one. `*` is accepted and is the
    explicit spelling.
  - **A pre-release or build on a PARTIAL version (`1.2.x-beta`) is
    22023.** node's regex captures it and then silently ignores it, which
    is the same silently-larger-set failure as dropping a misspelled flag
    bit.
  - **A numeric identifier past int64, and a leading zero, are refused
    inside a range exactly as they are in a version.** One reader decides
    what a version is on both sides of the predicate.

  Gated in `expr.TestARangeOffTheGrammarIsRefused` (SQLSTATE, the function
  named, the range quoted, and the refusal reaching the function even over
  a NULL version), `expr.TestTheRangeSpellingsThisGrammarAccepts` (so the
  refusal list cannot quietly grow to swallow a range people write) and
  `expr.TestTheRangeMemoIsBoundedAndRemembersRefusals` — the compiled-range
  memo caches the REFUSAL as well as the parse, so a malformed range raises
  the same sentence on every row rather than only on the one that missed.

  **AN INVALID LITERAL RANGE IS REFUSED FROM THE DECLARATION, ROWS OR NO
  ROWS, AND THE DECIDING LAYER IS THE BINDER.** The rule and both layers
  are the flag family's, above: `expr.RefuseInvalidSemverRangeLiterals` is
  asked at PLAN time by `physical.refuseInvalidSemverRanges`, which `Plan`
  and `PlanDistributed` both reach through `auth.ValidateStatementColumns`
  before any stage exists, and again by `expr.compileFuncCallNamed` as the
  BACKSTOP for the doors the binder does not see (ADR-0031's DML predicate,
  a policy row filter, a catalog-less entry point). Compilation alone is
  not one seam, because a DAG stage compiles its fragment only when a TASK
  RUNS. Both layers call `ParseSemverRange`, so they cannot disagree about
  which ranges exist. Gated by
  `physical.TestSemverRangeValidationDoors`,
  `physical.TestTheBinderRefusesAConstantSemverRangeInEveryExpressionPosition`
  (twelve positions over an input that reaches no rows),
  `physical.TestTheBinderLeavesANonConstantSemverRangeAlone` and
  `expr.TestCompilingACallWithAConstantRangeRefusesBeforeAnyRow`. Removing
  the binder call fails thirteen of those subtests.

  **THE TRIVIAL LOWER BOUND `>=0.0.0` IS DELETED FROM EVERY COMPARATOR
  SET, AS node-semver DELETES IT.** (Added 2026-09-11, arc A3 round 3,
  #967.) Three facts decide this, and they are worth separating because the
  first is not in play at all:

  1. **The SPECIFICATION defines precedence and no range syntax.** §9 puts
     every pre-release of `0.0.0` BELOW `0.0.0`, and §4 makes `0.y.z` the
     initial-development band. `semver_cmp` and `semver_sort_key` implement
     that exactly and are untouched by this entry — `0.0.0-alpha < 0.0.0`
     here, before and after.
  2. **RANGE semantics are node-semver's policy**, which this function
     implements by the arc's own choice, because the specification has none
     and node's is what a `package.json`, a Dependabot alert and an
     advisory's affected-versions field are written in. Two published rules
     of that policy meet here: the README's identity `*` := `>=0.0.0` (and
     `""` := `*` := `>=0.0.0`), and the per-tuple pre-release OPT-IN — a
     comparator that carries a pre-release admits that tuple's
     pre-releases.
  3. **The two readings disagree on exactly one shape**: a set that
     contains BOTH `>=0.0.0` and a comparator carrying a pre-release of
     `0.0.0`. Read numerically, `>=0.0.0` is false for every pre-release of
     `0.0.0`, so the set is EMPTY — no version is both at least `0.0.0` and
     at most `0.0.0-alpha` — and a range whose author explicitly opted in
     answers zero rows with no refusal. Read as node reads it, the trivial
     bound is not there and the opted-in pre-releases are admitted.

  THE PROVENANCE IS A DECISION, NOT A QUIRK. node's `replaceGTE0`
  (`classes/range.js:139`) arrived in commit `100f07aa` (isaacs,
  2020-04-10, shipped in 7.3.0) whose message says it "removes `>=0.0.0`
  (or `>=0.0.0-0` in `includePrerelease` mode) from the comparators in a
  range set, because that is equivalent to a `*`", over a token commented
  `// >=0.0.0 is like a star`. It was a side change of the `subset()` work
  and it states the README's identity. The pre-release consequence is
  EMERGENT rather than designed: `testSet` skips the ANY comparator when it
  looks for the per-tuple opt-in, so deleting `>=0.0.0` leaves the opt-in
  comparator (`<=0.0.0-alpha`) as the one that decides.

  WHAT IT LOOKS LIKE TO A USER. Go module pseudo-versions are literally
  `v0.0.0-yyyymmddhhmmss-hash` and fill a `go.sum` and every SBOM built
  from one, so `>=0.0.0 <0.0.0-20220101000000-000000000000` — "every
  pseudo-version built before 2022" — is a range a person writes. node
  answers it; the numeric reading answered nothing at all, silently.

  THE DELETION IS DECIDED ON THE PARSED COMPARATOR, not on its text, and
  the difference is recorded: node's rule is a regex over the desugared
  text and has been tightened once, because unescaped dots made `>=09090`
  match the `>=0.0.0` pattern (node-semver `11494f14`, #432). Here a
  leading zero is not a numeric identifier, so that spelling is refused
  before a comparator exists and the boundary is unreachable rather than
  guarded. BUILD METADATA is ignored (`>=0.0.0+b` is deleted, as it is
  there), because §10 gives it no precedence; a PRE-RELEASE is not
  (`>=0.0.0-0` is a different comparator and is kept, which is node's
  `includePrerelease` case).

  ONE CONSEQUENCE OF THE `v` CONCESSION, recorded beside it: node deletes on
  the comparator's TEXT before the prefix is normalized away, so `>=v0.0.0`
  survives there and `>=0.0.0` does not. Here the prefix carries no meaning
  at all — that is what the concession says — so both are the same
  comparator and both are deleted. Measured: `new Range('>=v0.0.0').range`
  is `>=0.0.0` on 7.7.3, which is the same comparator it declines to
  delete.

  The published expansions in `docs/sql-reference.md` stay the README's and
  still describe the same set of RELEASES; what the deletion changes is a
  PRE-RELEASE of `0.0.0`. node's own parser renders four of those rows
  without the trivial bound (`~0`, `^0.0`, `^0.0.x`, `^0.x` are
  `<1.0.0-0` / `<0.1.0-0`), which is why
  `expr.TestTheRangeGrammarMatchesTheNodeSemverTable` carries a `renders`
  column beside the published `expansion` and compares MEMBERSHIP against
  the published text.

  Gated against the LIBRARY rather than the README, because this is where
  the two differ: `expr.TestTheTrivialLowerBoundAnswersWhatNodeSemverAnswers`
  compares 2,970 cells (99 ranges × 30 versions) against
  `testdata/node_semver_gte0.tsv`, captured from node-semver 7.7.3 by
  `testdata/node_semver_gte0.js`, whose range and version lists the gate
  rebuilds and asserts position by position;
  `expr.TestNoAlternativeKeepsATrivialLowerBound` reads the parsed sets;
  `expr.TestTheStripAppliesToTheParsedComparator` is the boundary from both
  sides; `expr.TestARangeOverGoModulePseudoVersions` is the reachable shape,
  with the no-opt-in control that `*` and `>=0.0.0` alone still admit no
  pre-release; and `wadjet.TestARangeWithATrivialLowerBoundAnswersWhatNodeSemverAnswers`
  is the SQL door. Removing the strip fails 40 of the 2,970 cells — 38 that
  answer false here and true there, 2 the other way through the `||` rule.

  **A GENERATED BOUND AT THE TOP OF THE DOMAIN IS SATURATED, NOT WRAPPED
  AND NOT REFUSED.** (Added 2026-09-11, arc A3 round 2, #967.) Every range
  spelling except an exact version closes its band by raising ONE component
  by one — `^1.2.3` is `>=1.2.3 <2.0.0-0`, `>1.2.x` is `>=1.3.0` — and a
  component is accepted up to int64's MAXIMUM, which is the acceptance
  bound recorded above. The raise therefore has a reachable edge, and the
  shipped corpus draws components from exactly that value. Wrapped, `+1`
  there is a NEGATIVE number and the range answers the wrong boolean in
  both directions: `^9223372036854775807.0.0` desugared to
  `<-9223372036854775808.0.0-0`, which is below every version, so it
  dropped every row it named; `>9223372036854775807.x` desugared to
  `>=-9223372036854775808.0.0`, so it admitted every row. Found by the
  round-1 adversarial review.

  TWO POSITIONS WERE AVAILABLE AND THE SATURATING ONE IS TAKEN. Refusing
  the range (22023, the loud half this family already has) is what
  node-semver does — it refuses any component past `2^53-1`, in a version
  and in a range alike. It is rejected here because THIS family accepts
  such a version: `semver_major('9223372036854775807.0.0')` answers
  `9223372036854775807`, so refusing `^9223372036854775807.0.0` would make
  the acceptance bound depend on which function was asked, and the refusal
  doctrine above is about ranges whose MEANING is unknown, which this one's
  is not. The bound is rewritten instead, and the rewrite is EXACT rather
  than a best effort: every component is bounded by int64, so NO version
  exists between `X.Y.max` and the unspellable `X.(Y+1).0`, and therefore
  over the versions that exist

  | the bound the expansion names | what it is here |
  |---|---|
  | `<X.(Y+1).0-0` | `<=X.Y.9223372036854775807` |
  | `>=X.(Y+1).0` | `>X.Y.9223372036854775807` |

  with the maximum in every component below the raised one. The `-0` is not
  lost with it: a `-0` bound can never be the comparator that ADMITS a
  pre-release under the published pre-release rule, because nothing sorts
  below the lowest pre-release of its own core, and the saturated form
  carries no pre-release at all. So `^9223372036854775807.0.0` keeps its
  rows, `>9223372036854775807.x` matches nothing — there is nothing above
  the top of the domain — and `1.9223372036854775807.x` still REFUSES
  `2.0.0`, which is the cell a bound "made unbounded" instead of saturated
  would fail.

  THE ACCEPTANCE BAND IS WIDER THAN node-semver's AND EXACT ACROSS IT.
  node-semver is JavaScript and refuses a component past `2^53-1` because
  it cannot represent one exactly; every component from `2^53` to `2^63-1`
  is a version here and compares exactly, which is closer to the
  specification (§9 bounds a numeric identifier at nothing) and is the
  measured difference between the two implementations over a 5,341-string
  corpus. Past `2^63-1` it is NULL, as recorded above.

  Gated per SITE rather than per report: `expr.svBoundSites` in
  `expr.TestEveryGeneratedBoundSaturatesAtTheAcceptanceBound` holds a row
  for each of the sixteen places a component is raised, with the
  desugaring it must render and probe versions on both sides of the band;
  `expr.TestTheBoundTableCoversEveryRaiseSite` reads the source so a
  seventeenth site cannot arrive without a cell;
  `expr.TestNoDesugaringOverTheCorpusRendersANegativeComponent` asks the
  same claim blind over every operator form applied to the shipped corpus;
  `wadjet.TestARangeAtTheAcceptanceBoundKeepsTheRowsItNames` is the SQL
  door, and the five-arm census carries three band cells whose expectations
  are computed from the fixture's strings. Reverting the five bound
  constructors to the wrapping form fails all sixteen site cells.

  **`semver_parse` and `semver_parse_strict` return a fixed ROW**
  `(major bigint, minor bigint, patch bigint, prerelease text, build text)`.
  A3b carries its schema inside the declared-output seam, through aggregate
  keys, set-operation arms, window keys and worker/gather projections.
  The parser builds the ROW once per input value; absent pre-release/build
  are empty strings. Invalid strings return NULL, or 22023 naming the string
  in the strict form. NULL remains NULL in either form.
  This covers #1017's fixed-schema ROW case and #1055's derived stored-field
  grouping. ARRAY/MAP scalar declarations were closed by arc CW
  (2026-09-24, ADR-0045). Composite scalar-subquery transport retains its existing
  local route; declarations and values now survive it. See
  [fixed ROW declarations](../../internals/scalar-row-declarations.md).
