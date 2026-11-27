# The first-class INTERVAL as an execution type: definition draft

Design draft for epic #1680, item 1680b, round 3. **This is a draft for the
architect to turn into an ADR. It decides nothing the maintainer has not
ruled.** The maintainer's ruling of 2026-11-26 authorizes an execution-only
first-class interval (separate months, days and microseconds, typed identity
through every execution boundary) and requires its equality, hashing,
ordering, calendar arithmetic, normalization and output behaviour to be
defined against PostgreSQL 17.11 before any of it is implemented. Persistent
storage, format migration and wire-protocol changes are not authorized in
this stage.

Round 2 answered the judge's review of round 1 and was written to the
architect's rulings of 2026-11-27: rulings 1 to 9 are applied as decided;
rulings 10 (type identity placement) and 11 (parallel `sum` overflow) are
**pending with the maintainer** ([§10](#10-pending-with-the-maintainer)).
Round 3 answers the second judge record (verdict NOT READY, six
corrections): residual evidence claims (§12 lists every sentence changed),
the manifest's outcome checks and stage ownership (§9), the typed-lineage
contract (§9.1), the complete boundary inventory (§6), the transport
lifetime and compatibility account (§10) and the publication and overflow
policies against the manifest (§7, §10).

Six labels keep the kinds of evidence apart:

- **[measured]** PostgreSQL 17.11 answered it (postgres:17-alpine, `initdb
  --locale=C --encoding=UTF8`, `--memory=4g`; `SELECT version()` = `PostgreSQL
  17.11 on x86_64-pc-linux-musl, compiled by gcc (Alpine 15.2.0) 15.2.0,
  64-bit`; datcollate and datctype `C`; defaults IntervalStyle `postgres`,
  DateStyle `ISO, MDY`, TimeZone `UTC`; a cell's settings column overrides
  them for that cell only). The environment line heads every `_pg17.tsv`.
- **[pg-source]** read in PostgreSQL's source at tag `REL_17_11`, cited by file
  and function (`src/backend/utils/adt/timestamp.c`, `datetime.c`,
  `src/include/datatype/timestamp.h`, `src/backend/optimizer/prep/prepunion.c`).
- **[wadjet-source]** verified in this engine's source at `6ef333fb` (the
  engine code is unchanged on this branch), cited by file and function.
- **[inferred]** follows from the above but was not run.
- **[proposed]** a design choice for the architect. Nothing proposed here is
  measured behaviour.
- **[unresolved]** open, with the experiment that would settle it.

A corpus cell is cited as `section:name` (`03_order:order/asc`); a cast
variant as `section_cast:name~text`.

## 0. The corpus, its tests, and what they show

| file | role |
|---|---|
| `internal/coordinator/arc_1680b_interval_corpus_cells_test.go` | sections 01 to 09 (round 1) |
| `internal/coordinator/arc_1680b_interval_corpus_r2_cells_test.go` | sections 10 to 12, the cast-wrapped variants (round 2) and round 3's cells |
| `internal/coordinator/arc_1680b_interval_corpus_test.go` | the recorders, the baseline, the well-formedness check, the reference model |
| `internal/coordinator/arc_1680b_interval_manifest_test.go` | the stage manifest |
| `internal/coordinator/arc_1680b_interval_manifest_rules_test.go` | the named refusals, the internal-failure exclusion, answer comparison, the representative specs, the stage rule (round 3) |
| `internal/coordinator/arc_1680b_interval_outcome_checks_test.go` | the adversarial outcome checks, the representative proofs, the stage-rule cases (round 3) |
| `internal/coordinator/testdata/arc_1680b/<stem>.statements.tsv` | the statements as run: `name`, PostgreSQL settings, kind (`sql`, `ddl` fixture, `write`, `pg-only` plan), SQL |
| `…/<stem>_pg17.tsv` | PostgreSQL's answer: rows under a `column:type(oid)` header, a zero-row answer as `header \| (0 rows)`, `ERR <SQLSTATE>: message`, then any ` !! <severity> <SQLSTATE>: message` notice PostgreSQL raised |
| `…/<stem>_engine.tsv` | this engine's answer on each of six arms (`single`, `spilled512k`, `dag`, `dag-shuffled`, `dag-morsel4`, `dag-fastpath`; `*` when all agree) and a class |
| `…/<stem>_manifest.tsv` | the owning stage, the outcome, whether today's baseline meets it, and its legal alternatives |
| `internal/server/pgwire/interval_client_evidence_test.go` with `testdata/interval_client_evidence_{pg17,engine}.tsv` | the client transcript (§7) and the IntervalStyle door assertions (§9) |

**1,398 statements**: 1,308 queries and 23 writes under test, 34 fixtures and
33 PostgreSQL plan observations. Round 3 added 61 statements under test
(`10_definitions:age/dateUTC`, `age/dateMonthNY`, `age/dateGapUTC`,
`age/dateGapHavana` and their cast variants, `setop/*`; 42
`11_relational:neg/*`; 6 `12_writes:w/neg/*`) and one plan
(`10_definitions:explain/age/date`). Round 4 added one
(`01_input:cast/toChar`, §7). Re-measuring left every earlier
PostgreSQL answer byte-identical, and every earlier engine answer is as
recorded (`TestArc1680bEngineBaseline`).

Tests:

- `TestArc1680bMeasure` (`IV80_PG_DSN=…`) re-measures PostgreSQL; it keeps a
  zero-row result's types and PostgreSQL's notices (round 2's recorder fixes).
- `TestArc1680bRecordEngine` / `TestArc1680bEngineBaseline` record and hold
  the engine's answers with a class (`agree`, `agree-value`, `differs`,
  `answers-pg-error`, `pg-error`, `refusal` (0A000), `wrong-sqlstate`,
  `error`, `store-guard`, `internal`, `panic`). The baseline is a record, not
  a gate. Counted as distinct (cell, class) pairs: 75 agree, 3 agree in value,
  22 differ, 56 give PostgreSQL's error, 430 refuse 0A000, 57 give another
  SQLSTATE than PostgreSQL's error, 663 raise a coded error where PostgreSQL
  answers (297 of them 42601 and 294 of them 22007: the engine's literal
  grammar), and **none** is a store-guard error, an internal error or a panic.
  The harness names a string column declared with a length channel
  `character varying`, as PostgreSQL's header does (round 3; it moved no
  recorded answer).
- `TestArc1680bCorpusWellFormed`: the statements files are the cells as
  written; one typed PostgreSQL answer per statement; an engine line per arm;
  each line's class is the classification of its answer.
- `TestArc1680bReferenceModel`: the 128-bit comparison model (§2) against
  PostgreSQL's ORDER BY, GROUP BY (hash and sort plans) and `interval_hash`
  over 108 generated values, every generated id required exactly once.
- `TestArc1680bStageManifest`, `TestArc1680bOutcomeChecks`,
  `TestArc1680bRepExpectations`, `TestArc1680bStageRules` (§9).

**What the corpus does and does not show:**

- Where a claim rests on PostgreSQL's source alone it is labelled
  [pg-source]; the wire and IntervalStyle claims rest on the client
  transcript (§7).
- The generated model values (`09_model`) are finite and non-NULL; NULL and
  the infinities are covered by the hand-written cells (`03_order:order/*`,
  `04_keys:*`, `11_relational:pm/*`, `sw/*`), not by the model.
- Normalized storage would not fail every `02_fields` row: rows for zero,
  NULL and already-normalized values do not discriminate. The discriminating
  rows are named in §1.
- The engine arms are named by configuration. A configuration name is not
  proof that a path ran: the spilled arm forces aggregate drains and small
  sort runs, but join spill, window spill, gather replay, skew splitting and
  bloom engagement are not asserted by this corpus. Asserting them is owned,
  path by path, by the stages listed in §9's engagement table.
- PostgreSQL settings are applied on the reference only; the engine runs the
  statement as written. A cell whose meaning depends on IntervalStyle is
  decided by the engine's answer to `SET IntervalStyle` at the door (§9); a
  cell whose meaning depends on TimeZone is refused (`timestamptz`).

## 1. Representation

**[pg-source]** `src/include/datatype/timestamp.h`: `Interval` is `{ TimeOffset
time; int32 day; int32 month; }`, `time` in microseconds (int64).

**Field bounds.** months `[-2³¹, 2³¹−1]`, days `[-2³¹, 2³¹−1]`, microseconds
`[-2⁶³, 2⁶³−1]`, asymmetric as the integer types are, minus two **reserved
triples**: `INTERVAL_NOBEGIN` = (INT32_MIN, INT32_MIN, INT64_MIN) is
`-infinity`; `INTERVAL_NOEND` = (INT32_MAX, INT32_MAX, INT64_MAX) is
`infinity` (`INTERVAL_IS_NOBEGIN` / `INTERVAL_IS_NOEND` test all three fields;
`INTERVAL_NOT_FINITE` is either).

**Three construction policies**, which differ [pg-source] and [measured]:

| path | policy | evidence |
|---|---|---|
| text decoding (`interval_in` → `DecodeInterval` / `DecodeISO8601Interval` → `itmin2interval`) | each field range-checked (22015 `interval field value out of range`, 22008 for year arithmetic); `itmin2interval` does **not** test `INTERVAL_NOT_FINITE`, so text spelling a reserved triple IS that infinity | `10_definitions:in/allMaxUnits` and `01_input:lit/79` print `infinity`; `10_definitions:in/allMin` prints `-infinity`; `in/allMinIsNegInf` = `t,f,t` (equal to `-infinity`, not finite; the triple one microsecond above is finite); `in/allMinCastText` the same through a text cast |
| the signed integer unit path against the signed clock path | `strtoi64` of `-9223372036854775808 microseconds` is INT64_MIN and `AdjustMicroseconds(val, 0, 1)` keeps it: **accepted**. A signed clock `-2562047788:00:54.775808` is `DTK_TZ`: `DecodeTimeForInterval` of the unsigned part overflows (2⁶³), so it is refused 22015 | `10_definitions:in/usMinUnit` = `-2562047788:00:54.775808`; `01_input:lit/77`, `lit/80` refused; `in/usOverUnit`, `in/usUnderUnit` 22015 |
| `ago` | negates every field; INT_MIN / INT64_MIN fields cannot be negated: 22015 | `10_definitions:ago/-2147483648 days ago`, `ago/-2147483648 months ago` 22015; `in/usMinUnitAgo` |
| arithmetic (`finite_interval_pl`, `finite_interval_mi`, `interval_um_internal`, `interval_mul`, `interval_div`, `interval_justify_*`) | every field overflow-checked, and a finite result equal to a reserved triple is 22008 `interval out of range` | `05_arith:plus/toInfinityFields`, `10_definitions:sentinel/minusToNegInf`, `sentinel/plusToNegInf`, `sentinel/sumToNegInf` (22008); `05_arith:neg/minTimeBuilt` (negating INT64_MIN time) 22008; negation maps the infinities to each other (`05_arith:neg/inf`, `10_definitions:sentinel/negOfInfinityTriple` = `-infinity`). Near the sentinels: one microsecond below `infinity` negates to a finite value (`sentinel/negOfNearMax` = `-178956970 years -7 mons -2147483647 days -2562047788:00:54.775806`); a value whose months field is INT32_MIN cannot be negated, so `sentinel/negOfNearMin` is **22008** (the months negation overflows int32 before any sentinel test) |
| numeric construction (`make_interval`) | overflow-checked per field; `secs` infinite or NaN is 22008; a result equal to a reserved triple is 22008 ([pg-source] `make_interval`: `if (INTERVAL_NOT_FINITE(result)) goto out_of_range`) | `01_input:make/secsInf` 22008; `10_definitions:in/allMinMake` (finite: months and days at INT32_MIN, time 0) |

**[measured] Fields are kept as given, not normalized**, in print and in
extract. The rows that discriminate a normalizing store: `02_fields:fields/0…2`
and `print/postgres` rows 0–3 (`1 mon`, `30 days`, `720:00:00`, `36 hours`
stay as written), `fields/7` (`1 mon -30 days` keeps both fields), `fields/18`
(`14 months` is one months field printed `1 year 2 mons`), `fields/17`
(`1.5 months` is `1 mon 15 days`). Zero, NULL and already-normalized values
(`fields/8`, `fields/22`, `fields/4`) do not discriminate.

**[proposed] The vector** stores the triple exactly: a fixed-width column of
`{Months int32; Days int32; Micros int64}` (16 bytes a row), NULL in the
vector's null bitmap, the infinities as the reserved triples. No flag, no
normalized copy beside it. Text decoding follows PostgreSQL's text policy;
arithmetic and `make_interval` follow theirs.

## 2. Equality, ordering and hashing

**[pg-source]** `timestamp.c`, `interval_cmp_value`:

```c
days = interval->month * INT64CONST(30);
days += interval->day;
span = int64_to_int128(interval->time);
int128_add_int64_mul_int64(&span, days, USECS_PER_DAY);
```

so `N = (months × 30 + days) × 86 400 000 000 + micros`. `interval_cmp_internal`
(and through it `=`, `<>`, `<`, `<=`, `>`, `>=`, `interval_cmp`,
`interval_smaller`, `interval_larger`) and `interval_sign` compare N.
`interval_hash` hashes the low 64 bits of N with `hashint8`;
`interval_hash_extended` hashes them with `hashint8extended`. The comment
above them says equal-comparing values must hash equal: the hash is of N, not
of the fields.

**Width.** |N| ≤ (2³¹ × 30 + 2³¹) × 8.64 × 10¹⁰ + 2⁶³ ≈ 5.76 × 10²¹, which
needs about 74 signed bits: 64 are not enough, and 128 is the available carrier
(the engine has Int128 for DECIMAL, `internal/engine/batch/decimal.go`), not a
uniquely necessary width. **[measured]** the cells where a 64-bit N is wrong:
`03_order:cmp/106751991 days 04:00:54.775808~0` (N = 2⁶³: `>`, wraps to `<`),
`cmp/213503982 days 08:01:49.551616~0` (N = 2⁶⁴: `<>`, wraps to `=`),
`order/wrap`, `filter/gtWrap`, `04_keys:wrap/group` (2 groups; 1 under a
wrap), `wrap/join` (a count of 0 matches; 1 under a wrap), and
`wrap/hashCollide` (PostgreSQL's own `interval_hash` of the 2⁶⁴ value equals
that of `0`, while `cmp/213503982 days 08:01:49.551616~0` shows the two are
not equal), `09_model:model/order`.

**Equality and order are not field-wise; printing and arithmetic are
field-preserving, with one exception.** [measured] `1 mon = 30 days =
720:00:00`, `1 mon -30 days = 0`, `1 year = 360 days`, `-1 mon 1 day = -29
days` (`03_order:cmp/*`); BETWEEN, IN, IS DISTINCT FROM follow N. Arithmetic
never consults N **except through `interval_sign`**: a finite interval times
±Infinity is ±infinity by the sign of N, and 22008 when N is 0
(`10_definitions:ar/infFactorZeroSign` 22008 for `1 mon -30 days`,
`ar/infFactorNegSign` = `-infinity` for `1 mon -31 days`, `ar/infFactorPosSign`
= `infinity` for `-1 mon 31 days`); and a RANGE frame's offset is checked by
`interval_sign` (§4d).

**[measured] The model reproduces PostgreSQL** (`TestArc1680bReferenceModel`):
108 values, 47 groups, 41 of them with members of different fields; the
order is the 128-bit N then id, the groups (hash and sort plans) are equality
of N, PostgreSQL's hash is equal within each group.

**[proposed]** Equality, comparison, sort, top-N, min/max, join, grouping,
DISTINCT and set-operation keys use N as a signed 128-bit integer. The hash is
of N only (the engine's hash values need not equal PostgreSQL's), so SQL-equal
implies hash-equal by construction; the generator of
`TestArc1680bReferenceModel` becomes a property test over the engine's key
encoding, extended with NULL and both infinities. A byte-comparable sort key
is N as **16 bytes, big-endian, two's complement with the sign bit flipped**;
the fields ride beside it and are never compared.

## 3. The representative of a group, and the SQL-tie rule

**[measured] PostgreSQL's printed representative depends on the plan and the
input order.**

| statement (same rows; reversed rows) | hash plan | `enable_hashagg = off` | reversed, hash | reversed, sort |
|---|---|---|---|---|
| GROUP BY over `1 mon`, `30 days`, `720:00:00` | `1 mon` | `30 days` | `720:00:00` | `720:00:00` |
| GROUP BY over `1 mon -30 days`, `0`, `00:00:00` | `1 mon -30 days` | `00:00:00` | `00:00:00` | `00:00:00` |
| GROUP BY over `-1 day`, `-24:00:00` | `-1 days` | `-24:00:00` | `-24:00:00` | `-1 days` |

(`04_keys:group/*`, plans in `04_keys:explain/group/*`.) UNION does not
necessarily choose what GROUP BY chooses (the reversed sorted UNION prints
`1 day` and `360 days`, `04_keys:union/iv80_kr/sortagg`). Whole-tuple
selection is PostgreSQL's: `SELECT DISTINCT a, b` over (`1 mon`, `24 hours`)
and (`30 days`, `1 day`) keeps one whole row, the first met, under both plans
(`11_relational:tuple/distinct/*`, `tuple/distinctRev/*`, `tuple/table/*`);
the multi-column key with NUMERIC and float ties keeps the first row's
`1 mon`, `1.0`, `0` (`11_relational:mixkey/*`).

**Set operations** [measured and pg-source]. PostgreSQL's INTERSECT
representative is not always a left-arm member: under the sorted SetOp either
arm's member leads (`04_keys:adr0024/intersectNumericSort`), and under
HashSetOp the first input is the one with fewer estimated groups
(`prepunion.c`, `generate_nonunion_paths`: "we prefer to put the smaller input
first"; EXCEPT keeps the left first): `11_relational:set/intersect/bigSmall/default`
prints the right arm's `1 mon` with plan `HashSetOp Intersect -> Append ->
"*SELECT* 2" …`; `set/intersectAll/dupRight/*` prints the right's
`720:00:00` twice. EXCEPT ALL prints the group's representative for every
surviving copy (`set/exceptAll/dupRight1/*`, `04_keys:setLit/exceptAll/*`:
`1 mon; 1 mon` for a left of `1 mon`, `1 mon`, `30 days`).

**Ordered aggregates and representation-dependent values** [measured]:
`min(v ORDER BY id)` / `max(v ORDER BY id DESC)` answer by the stated order
(`11_relational:ordagg/minMax` = `30 days`, `1 mon`; `ordagg/minMaxRev` =
`1 mon`, `30 days`); unordered `min` / `max` keep the later argument on a tie
(`03_order:order/minTieFwd` = `720:00:00`, `minTieRev` = `1 mon`; that it is
the LATER argument is read in `interval_smaller` / `interval_larger`
[pg-source]); `GREATEST` / `LEAST` keep the first argument
(`03_order:order/greatestTie`). `sum(DISTINCT v)` depends on which member
represents a group: `1 mon 1 day` or `31 days` by input order
(`11_relational:distagg/sumAvg/*`, `distagg/sumAvgRev/*`). A representative
changes a later calendar result: `TIMESTAMP '2024-01-31' + v` over `SELECT
DISTINCT v` of `1 mon` and `30 days` is `2024-02-29` or `2024-03-01` by input
order (`11_relational:reparith/*`, `reparithRev/*`).

**The engine's rule (architect's ruling 3).** INTERVAL joins ADR-0024 §12 as
a type with many representations of one SQL value. Among the members of one
group, the representative is the **minimum under ascending lexicographic
order over (months, days, micros)**: `720:00:00` (0, 0, 2.592 × 10¹²) beats
`30 days` (0, 30, 0) beats `1 mon` (1, 0, 0); `00:00:00` beats `1 mon -30
days`; `-1 days` (0, −1, 0) beats `-24:00:00` (0, 0, −8.64 × 10¹⁰). It is
applied exactly as ADR-0024 §12 applies its rule: over the **whole key tuple**
with the whole winning tuple kept; eligibility before comparison (a left-arm
member for INTERSECT / EXCEPT); every surviving copy of an EXCEPT ALL /
INTERSECT ALL group prints the representative; the representative and the
multiplicities survive precombine, clone merging, spill and the final merge
together. Selection never normalizes. It never prints a value absent from the
input: the minimum of a non-empty set under a total order is a member.

**Executable form (round 3).** Every representative-divergence cell carries
its explicit rule-3 answer (`iv80RepSpecs`): which columns are key or MIN /
MAX units, the eligible input members of each, and the rows.
`TestArc1680bRepExpectations` proves each against PostgreSQL's answer (same
rows by count; every other column exactly PostgreSQL's; each unit column
SQL-equal; each printed tuple an eligible member and the rule-3 minimum of
its SQL-equal members). The manifest then requires that exact answer, with
PostgreSQL's types. Examples: `tuple/distinct/*` must print `30 days,1 day`
(the whole second row), never the fabricated `30 days,24:00:00` nor
PostgreSQL's `1 mon,24:00:00`; `03_order_cast:order/minTieRev~text` must print
`720:00:00,720:00:00`; `11_relational:pm/null` keeps `sum` and `avg` at
PostgreSQL's `1 day` (no latitude outside the units); `set/intersectAll/dupRight/*`
prints `30 days; 30 days` (a left member, twice).

- Unordered `min` / `max` over SQL-tied extremes print the rule-3
  representative (ruling 4); `min(v ORDER BY …)`, `max(v ORDER BY …)` and
  every ordered or ordered-set aggregate over an interval are refused 0A000
  (ruling 4).
- `sum(DISTINCT iv)` and `avg(DISTINCT iv)` are refused 0A000; `count(DISTINCT
  iv)` is allowed (ruling 5).
- `GREATEST` / `LEAST` keep PostgreSQL's first-argument rule: the statement
  decides it (their outcome is `supported`).
- A value computed from a representative is computed from the rule-3 member:
  `reparith/*` must answer `2024-03-01`.

**The SQL-tie rule (ADR-0013).** The representative order must never become a
SQL order inside an equal key. [measured] PostgreSQL orders SQL-equal
intervals by the next key only (`03_order:order/asc`, `order/idDesc`,
`04_keys:orderTie/*`); window peers are SQL-equal rows (`04_keys:peers/*`,
`partition/*`); `FETCH FIRST n ROWS WITH TIES` takes the whole SQL-equal group,
and the order inside it is a permitted alternative, not a deterministic
expectation (`04_keys:withTies/*` = `8; 7` forward, `7; 8` reversed). `ORDER
BY iv, id`, peers, rank, dense_rank and WITH TIES use N only.

## 4. Arithmetic, calendar arithmetic, EXTRACT, frames

### 4a. Interval arithmetic

| operation | algorithm [pg-source] | cells [measured] |
|---|---|---|
| `i ± j` | `finite_interval_pl` / `_mi`: field-wise, each field overflow-checked, a result equal to a reserved triple 22008; no carry between fields | `05_arith:plus/*`, `minus/*` (`20:00:00 + 20:00:00 = 40:00:00`) |
| infinite operands of `±` | ∞ ± finite = ∞; `infinity - infinity`, `infinity + -infinity` 22008; `infinity - -infinity` = `infinity` | `05_arith:plus/inf*`, `10_definitions:infop/ivInfMinusNegInf` |
| `-i` | `interval_um_internal`: infinities swap; each field negated with overflow check | `05_arith:neg/*`, `10_definitions:sentinel/negOfNearMin` (22008) |
| `i * f` (f float8; integer, numeric, real, unknown cast to float8) | `interval_mul`: NaN 22008. Infinite i: f = 0 is 22008, f < 0 negates, else i. Infinite f: `interval_sign(i)` = 0 is 22008, else ±infinity by sign(f) × sign(N). Finite: (1) `month = (int32) trunc(months × f)` and `day = (int32) trunc(days × f)`, **each truncated separately**, 22008 if out of int32; (2) `month_remainder_days = TSROUND((months × f − month) × 30)` where `TSROUND(x) = rint(x × 10⁶) / 10⁶`; (3) `sec_remainder = TSROUND((days × f − day + month_remainder_days − trunc(month_remainder_days)) × 86400)`; (4) if \|sec_remainder\| ≥ 86400, carry `trunc(sec_remainder / 86400)` whole days into `day` and keep the rest; (5) `day += trunc(month_remainder_days)`; (6) `time = rint(time × f + sec_remainder × 10⁶)`, 22008 if out of int64 or a reserved triple | `1 mon -1 day` × 1.5: month 1, day −1, month_remainder_days 15, sec_remainder −43200 → `1 mon 14 days -12:00:00` (`05_arith:mul/mixedSign`, `10_definitions:ar/mulMixed`); `ar/mulCarry` = `23 days 06:00:00` (a whole day carried); 1 µs × 1.5 = 2 µs, × 2.5 = 2 µs, × 0.5 = 0 (rint ties to even, `05_arith:mul/usHalfEven1…3`) |
| `i / f` | `interval_div`: f = 0 is 22012; NaN 22008; infinite i: infinite f 22008, f < 0 negates; finite i: the same six steps with **`months / f`, `days / f`, `time / f`** — each component divided by f, never multiplied by a computed reciprocal | `10_definitions:ar/divRecipMonths`: `49 mons / 49` = `1 mon`, `49 mons × (1/49)` = `30 days`. The pairs `ar/divRecipJudge` and `ar/divRecipDays` do **not** discriminate. `1 mon -1 day / 2` = `15 days -12:00:00` (`ar/divMixed2`); `1 year / 7` = `1 mon 21 days 10:17:08.5344` (`05_arith:div/yearBy7`) |
| `justify_days(i)` | `interval_justify_days`: infinite unchanged; `wholemonth = day / 30` (C truncation) moved into months (overflow 22008); then if months > 0 and days < 0, days += 30, months −= 1, and the mirror | `05_arith:justify/*` |
| `justify_hours(i)` | `interval_justify_hours`: infinite unchanged; `TMODULO(time, wholeday, 86400 × 10⁶)` into days (overflow 22008); then days and time made the same sign | `05_arith:justify/*` |
| `justify_interval(i)` | `interval_justify_interval`, its own algorithm: infinite unchanged; **pre-normalize** days into months when days and time have the same sign; TMODULO time into days; days / 30 into months (overflow 22008); then month/day sign agreement treating `day = 0` by the time's sign; then day/time sign agreement | `10_definitions:justify/bigDays24h`: `5965232 years 4 mons 8 days`, while `justify_days(justify_hours(…))` is 22008 (`/daysOfHours`); `justify/monMinus25h`: `28 days 23:00:00`, against both compositions |
| `sum` | `do_interval_accum` / `interval_sum`: a field-wise running sum in an `Interval` (int32, int32, int64), each addition checked, so an intermediate overflow is 22008 even when later rows would bring it back; finite and infinite values counted apart; both infinity signs 22008; one sign gives that infinity | `05_arith:agg/sumOverflow*`, `agg/sumInf*`; `11_relational:ov/*` (`ov/winForward` 22008, `ov/winBackward` answers; `ov/aggUnordered` 22008), `pm/*` (`pm/bothInf` 22008) |
| `avg` | `interval_avg`: the same accumulator, then `interval_div` by the finite count: `avg(2147483647 days, 2147483647 days)` is 22008 though the mean fits | `05_arith:agg/avg*` |
| sliding `sum` / `avg` | `interval_avg_accum_inv`: field-wise subtraction, infinity counters decremented; a frame holding both infinities is 22008 | `05_arith:agg/avgWindowSliding`; `11_relational:sw/*` (`sw/all` 22008) |

### 4b. Calendar arithmetic and age

| operation | algorithm [pg-source] | cells [measured] |
|---|---|---|
| `timestamp + interval` | `timestamp_pl_interval`: if the interval is `-infinity`: a `+infinity` timestamp is 22008, else `-infinity`; if `+infinity`: a `-infinity` timestamp is 22008, else `+infinity`; else an infinite timestamp is kept; else **months** first (calendar month, the day clamped to the target month's length), then **days** (calendar days via Julian), then **time** (exact microseconds) | `06_calendar:ts/*`; `10_definitions:infop/tsInfPlusNegInf`, `infop/tsNegInfPlusInf`, `infop/tsNegInfMinusNegInf`, `infop/dateInfPlusNegInf` 22008; `infop/tsNegInfMinusInf` = `-infinity`, `infop/tsInfPlusFinite` = `infinity`, `infop/dateFinitePlusInf` = `infinity` |
| `timestamp - interval` | `timestamp_mi_interval`: `timestamp + (-interval)` | `06_calendar:ts/orderMDNeg` |
| `timestamp - timestamp` | `timestamp_mi`: microseconds into time, months 0, days 0, then `interval_justify_hours`: days and time, never months; one infinite operand gives that infinity, two equal infinities are 22008 | `06_calendar:tsdiff/*` (`tsdiff/inf` = `infinity`, `-infinity`; `tsdiff/infInf` 22008) |
| `timestamp - date`, `date - timestamp` | the date read as a timestamp at midnight; `timestamp_mi` | `10_definitions:types/tsMinusDate` = `1 day 01:00:00`, `types/dateMinusTs`; `11_relational:tsdate/*` |
| `age(timestamp, timestamp)` | `timestamp_age`: infinities first: `age(-inf, -inf)`, `age(inf, inf)` 22008; `-inf` left gives `-infinity`, `+inf` left `infinity`, `-inf` right `infinity`, `+inf` right `-infinity`. Finite: subtract each broken-down field; if dt1 < dt2 negate them all; propagate negatives upward; a negative mday borrows **the length of the earlier timestamp's month** and decrements mon; negative months borrow 12 from years; then restore the sign if dt1 < dt2; `itm2interval` (22008 on overflow). NULL in, NULL out | `10_definitions:age/borrow` (`1 mon 1 day`, reversed `-1 mons -1 days`; `15 days`, reversed `-15 days`; `1 mon 15 days`), `age/timeBorrow`, `age/null`, `age/infLeft`, `age/infRight`, `age/infOpposed`, `age/infInf` and `age/negInfNegInf` 22008, `age/bc`; `06_calendar:age/*` |
| `age(timestamptz, timestamptz)` | `timestamptz_age`: the same on the fields **in the session TimeZone**, ignoring a difference of offsets | `10_definitions:age/tz/UTC/fallBack` = `1 day 01:00:00` against `age/tz/America/New_York/fallBack` = `1 day`; `06_calendar:tz/*/age` (`1 mon 9 days 23:00:00` under UTC against `1 mon 10 days` under New York) |
| `age(date, date)` | **resolves to the timestamptz overload**: both dates are cast to `timestamptz` at local midnight (`10_definitions:explain/age/date`: `age(('2024-03-01'::date)::timestamp with time zone, …)`), so the answer is the session TimeZone's. For the same pair it is alike under UTC and New York (`age/date` and `age/dateMonthNY` = `1 mon 1 day`; `age/dateNY` and `age/dateUTC` = `2 days`), but not in a zone whose midnight falls in a DST gap: `age(DATE '2024-03-11', DATE '2024-03-10')` is `1 day` under UTC (`age/dateGapUTC`) and `23:00:00` under America/Havana (`age/dateGapHavana`, where 2024-03-10 00:00 does not exist). Ruling 7: refused with the timestamptz age (`refusal timestamptz`) | `10_definitions:age/date*`, `explain/age/date` |
| `timestamptz ± interval` | the same order in the session TimeZone: a day keeps the wall clock across DST, 24 hours do not; a target time in a DST gap moves forward, a target in a fold takes the later offset | `06_calendar:tz/*`; `10_definitions:dst/*` |
| `time ± interval` | `time_pl_interval`: the time part modulo 24 h; months and days ignored | `06_calendar:time/plus` |

**Result types** [measured], the header type of each cell:

| expression | result | cell (`10_definitions:types/…`) |
|---|---|---|
| `date ± integer`, `integer + date` | `date` | `datePlusInt`, `dateMinusInt`, `intPlusDate` |
| `date − date` | `integer` (OID 23) | `dateMinusDate` (the engine answers `bigint`: `agree-value`) |
| `date ± interval`, `interval + date` | `timestamp` | `datePlusInterval`, `dateMinusInterval`, `intervalPlusDate` |
| `date ± time` | `timestamp` | `datePlusTime`, `dateMinusTime` |
| `timestamp ± interval`, `interval + timestamp` | `timestamp` | `tsPlusInterval`, `tsMinusInterval`, `intervalPlusTs` |
| `timestamp − timestamp`, `timestamp − date`, `date − timestamp` | `interval` | `tsMinusTs`, `tsMinusDate`, `dateMinusTs` |
| `timestamptz − timestamptz`, `timestamptz − timestamp` | `interval` | `tstzMinusTstz`, `tstzMinusTs` |
| `timestamptz + interval` | `timestamp with time zone` | `tstzPlusInterval` |
| `time − time` | `interval` | `timeMinusTime` |
| `time ± interval`, `interval + time` | `time` | `timePlusInterval`, `timeMinusInterval`, `intervalPlusTime` |
| `interval × integer`, `integer × interval`, `interval / integer`, `−interval` | `interval` | `intervalTimesInt`, `intTimesInterval`, `intervalDivInt`, `negInterval` |
| `interval − date`, `interval − timestamp`, `timestamp + timestamp`, `date + date`, `date × integer`, `timestamp + integer` | 42883 | `intervalMinusDate`, `intervalMinusTs`, `tsPlusTs`, `datePlusDate`, `dateTimesInt`, `tsPlusInt` |

### 4c. EXTRACT and date_part

**EXTRACT epoch is not N.** [pg-source] `interval_part_common`, `DTK_EPOCH`:
`epoch = ((4 × 365.25) × (months / 12) + (4 × 30) × (months % 12) + 4 ×
days) × (86400 / 4) + time / 10⁶`, in integer arithmetic (numeric on int64
overflow): **a year is 365.25 days in the epoch and 360 days in N**.
[measured] `10_definitions:epoch/yearVs360`: `extract(epoch FROM '1 year')` =
`31557600.000000`, `'360 days'` = `31104000.000000`, while `'1 year' = '360
days'` is `t`; `'13 mons'` = `34149600`. `date_part('epoch', …)` is the float8
form (`epoch/yearVs360Float`). The other fields are the broken-down fields of
`interval2itm` (`07_functions:extract/*`, numeric for EXTRACT, float8 for
date_part; `week`, `dow`, `doy`, `isoyear`, `julian`, `timezone` are 0A000 on
PostgreSQL itself, `07_functions:extract/week` … `extract/timezone`). For an
infinite interval `NonFiniteIntervalPart` answers ±Infinity for year, day,
hour, epoch and similar fields and NULL for month, minute, second,
microsecond (`07_functions:extract/inf`, `02_fields:fields/20/infinity`). A
large **finite** epoch: `10_definitions:epoch/bigFinite` =
`5842218453753654.775806`, `epoch/bigFiniteNeg`.

**Ruling 6:** an EXTRACT whose numeric result PostgreSQL gives as ±Infinity is
refused 0A000; `date_part`'s float8 ±Infinity is supported.

### 4d. Functions and frames: implement or refuse

| function | assignment [proposed, within rulings 6 and 7] | definition / cells |
|---|---|---|
| `make_interval`, `justify_*`, `isfinite`, `age(timestamp, timestamp)`, `date_part`, `EXTRACT` (finite results) | implement, stage C | §1, §4a, §4b, §4c |
| `age(date, date)`, `age(timestamptz, timestamptz)` | refuse 0A000 (`timestamptz`, ruling 7): both are the timestamptz age | §4b |
| `date_bin(stride, timestamp, origin)` | implement, stage C: [pg-source] `timestamp_bin`: an infinite timestamp is kept (`fn/binInfSource`); an infinite origin 22008; an infinite stride 22008 (`fn/binInfStride`); a stride with months 0A000 (`07_functions:bin/month`); a stride ≤ 0 22008 (`bin/zero`, `bin/neg`); else floor((ts − origin) / stride) × stride + origin with the days counted as 24 h (`fn/binDaysStride`, `fn/binBeforeOrigin`); a sub-millisecond result is refused (`bin/us`, E36) | `07_functions:bin/*`, `10_definitions:fn/bin*` |
| `date_trunc(unit, interval)` | refuse 0A000 (`function-date_trunc`), own item | `07_functions:trunc/*` (PostgreSQL's own 0A000 for `week`: the named refusal is still met, §9), `10_definitions:fn/truncMixedSign` |
| `generate_series(ts, ts, interval)` | refuse 0A000 (`function-generate_series`), own item: each step is applied to the previous value, so a month step drifts at month ends (`fn/seriesTsMonthEnd`: `01-31, 02-29, 03-29, 04-29`) | `07_functions:series/*` |
| `to_char(interval, fmt)` | refuse 0A000 (`function-to_char`), own item | `07_functions:tochar/*`, `fn/tocharNeg` |
| `OVERLAPS` with an interval end | refuse 0A000 (`function-overlaps`), own item | `07_functions:overlaps/*`, `fn/overlaps*` |
| `width_bucket(interval, …)` | PostgreSQL's own 42883 for the four-argument form; the array form is a container (refused) | `07_functions:widthBucket`, `widthBucketArray` |
| ordered-set and ordered aggregates over an interval | refuse 0A000 (ruling 4) | §3 |
| RANGE frames with an interval offset | refuse 0A000 (`range-offset-frame`) while offset RANGE frames are refused for every type (42601 here today); when implemented: [pg-source] `in_range_timestamp_interval` / `in_range_interval_interval`: a negative offset by `interval_sign` is 22013 (`frame/negOffset`, `frame/negInfOffset`); a NULL offset is 22004 (`frame/nullOffset`); `infinity PRECEDING` includes every earlier row (`frame/infOffset`, `frame/infFollowing`); an offset SQL-equal to zero is not a zero offset: `1 mon -30 days PRECEDING` leaves 2024-03-01's own row outside its frame (`frame/zeroNormalized` row 3 counts 0) | `07_functions:frame/*`, `11_relational:frame/*` |

## 5. Input and output text

**[measured and pg-source] Two input rounding paths:**

- A **unit's fraction** (`1.5 microseconds`, `0.0000015 seconds`) goes through
  `AdjustFractMicroseconds`: truncate, then add one only when the remaining
  fraction is **strictly above** 0.5 (half toward zero).
- A **clock's fraction** (`00:00:00.0000015`) goes through
  `ParseFractionalSecond`: `rint(frac × 10⁶)`, **half to even**.

`10_definitions:frac/1.5us`: clock `00:00:00.000002`, unit `00:00:00.000001`;
`frac/3.5us`: clock 4 µs, unit 3 µs; `frac/2.5us`: both 2 µs;
`frac/clockNearSecond`: clock `00:00:01.5`, unit `00:00:01.499999`.

- A **year, decade, century or millennium fraction** becomes whole months by
  `rint` (`AdjustFractYears`): `0.125 years` = `2 mons`, `0.375 years` =
  `4 mons`, `-0.125 years` = `-2 mons` (`frac/years1`, `frac/years2`).
- A **month fraction** becomes days at 30 (`AdjustFractDays(frac, 30)`), the day
  fraction then microseconds by the unit path: `0.5 months` = `15 days`,
  `-0.0333333 months` = `-23:59:59.9136` (`05_arith:frac/months`).
- A **week fraction** becomes days first (`AdjustFractDays(frac, 7)`), then
  microseconds: `0.5 weeks` = `3 days 12:00:00` (`01_input:lit/34`), `1.5
  weeks` = `10 days 12:00:00`, `0.1 weeks` = `16:48:00` (`frac/weeks1`).
- A day, hour, minute or second fraction becomes microseconds by the unit
  path.

**`ago`** [measured]: accepted only as the last token of the postgres grammar
(`1 day 02:00:00 ago`, `1-2 ago`, `3 4:05:06 ago`, `-1 day +02:00:00 ago`, `@ 1
mon ago`); **refused 22007** after an ISO 8601 form (`P1D ago`, `P1Y2M ago`),
after `infinity` or `-infinity`, twice (`1 day ago ago`) or alone (`ago`)
(`10_definitions:ago/*`). [pg-source] `DecodeInterval`: `AGO` only at
`i == nf - 1`; `RESERV` (infinity) must be the last field; ISO input is tried
only after the postgres grammar fails on the whole string.

**Typmods change interpretation, not only truncation** [measured]: the
qualifier decides which field a bare number or a two-part clock fills:
`'1:30'` is `01:30:00` bare and `HOUR TO MINUTE`, `00:01:30` as `MINUTE TO
SECOND`; `'90'` is 90 seconds bare and 90 minutes as `MINUTE`; `'2' DAY TO
HOUR` is `02:00:00`; `'1.5' HOUR TO MINUTE` is `00:01:00`
(`10_definitions:typmod/*`, `01_input:std/*`). Then `AdjustIntervalForTypmod`
zeroes the fields right of the last one named and rounds the time to the
precision **half away from zero** (`01_input:prec/iv0` = `00:00:02`,
`00:00:03`, `-00:00:02`). A precision above 6 is clamped to 6 with `WARNING
22023: INTERVAL(7) precision reduced to maximum allowed, 6` (`typmod/prec7*`,
`01_input:prec/iv7`).

**`IntervalStyle` changes input too:** under `sql_standard`, `-1 2:03:04`
applies the leading sign to every field (`02_fields:input/sqlstd/mixed`).

**Casts** [measured], exactly the directions measured: `text → interval`
(`cast/fromText`, `cast/colon`), `interval → text` (`cast/toText`, NULL
`cast/toTextNull`), `interval → varchar` (`cast/toVarchar`, typed `character
varying`, OID 1043), `time → interval` (`cast/fromTime`), `interval → time`
(modulo 24 h, `cast/toTime`), `interval → char(10)` (`cast/toChar`, typed
`character`, OID 1042, blank-padded; outside the display path, §7); `integer → interval`, `numeric → interval`,
`interval → bigint`, `interval → boolean` are 42846 (`cast/fromInt`,
`cast/fromNumeric`, `cast/toBigint`, `07_functions:operand/castBool`).

**Output** [measured] (`02_fields:print/*`): four styles, field-preserving;
`postgres` prints `1 year 2 mons 3 days 04:05:06.789`, `-1 days +02:03:04.5`.
DateStyle does not change it (`02_fields:print/dateStyleSQL`).

**First stage (ruling 7):** input is PostgreSQL's interval grammar under
`IntervalStyle = postgres` with both rounding paths as above; output is the
`postgres` style only; every typmod-qualified declaration is refused 0A000;
`SET IntervalStyle` to anything but `postgres` is refused at the door (today
it is accepted and ignored: §7, §11).

## 6. Boundaries in the engine: the complete inventory

Every place a value of the new type must be handled, enumerated from the
source at `6ef333fb` [wadjet-source]; every file and function below was
re-verified in round 3 (a script checked each function's definition exists
in the named file). This is round 1's inventory, its refinements and round
2's omissions table merged into one list. Columns: **site** (file and
function), **today**, **change** (what the stage must do), **owner** (the
stage, §9), **if forgotten** [inferred].

### 6.1 The type, the vector, selection and copy

| site | today | change | owner | if forgotten |
|---|---|---|---|---|
| `storage/parquet/schema.go` `TypeID` enum, `TypeID.String`, `ParseTypeID` | no INTERVAL id; `ParseTypeID("INTERVAL")` answers `TypeDuration` for the CAST machinery; `DeclaredColumn` refuses a new INTERVAL column (#1713d) | append one execution-only id (ruling 10, pending); `ParseTypeID("INTERVAL")` keeps answering DURATION for DDL refusal and casts | A (declares), B (carrier) | DDL starts accepting INTERVAL, or existing DURATION data is reinterpreted |
| `engine/batch/vector.go` `NewVectorWithScale`, `EnsureLen`, `ResetForWrite`, `GetValue`, `SetValue`, `CopyValueFrom`, `AppendFrom`, `growForAppend`, `memBytes`, `IntStorageType`, `WriteNullAt` | no storage; `SetValue` refuses an `IntervalBox` into anything but DURATION | a 16-byte triple column; every accessor, growth, copy, typed NULL, memory size | B | guard errors, or a DURATION-shaped copy that loses fields silently |
| `engine/batch/batch.go` `newVectorFromColumn`, `(*RecordBatch).Reset`, `resetVectorForReuse`; `bitmap.go` `NewBitmapAllNull`; `pool.go` `BatchPool.Get`, `GetForSize` | allocation by type id; the pool has no type switch | allocation, reset, pool reuse, empty and all-NULL batches (the pool's obligations are allocation, reset, ownership, accounting) | B | a wrong declaration hidden while every value is NULL (`11_relational:null/*`, `empty/*`, `oj/*`); stale triples on reuse |
| `engine/batch/view.go` `Flatten`; `engine/exec/late_mat.go` `FlattenForConsumer` | type switches without INTERVAL | flatten selections and late-materialized views | B (carrier), D (late materialization engaged) | a bad copy silently corrupts output |
| `engine/exec/sort.go` `copyVectorValue`, `gatherVector`, `gatherSortVector`; `exec/join.go` `gatherBuildVector`, `gatherCrossBuildVector`; `exec/window.go` `windowCopyVectorRange`, `windowGatherVector`; `exec/project.go` `projectCopyColumn`, `projectGatherColumn`; `worker/partitioned_shuffle_sink.go` `appendBatchRowsBulk`, `growBatchTo` | type switches without INTERVAL | a case each (both the typed fast path and the generic fallback) | B | guard error, NULL loss, or a field dropped in a copy |
| `engine/batch/type_mismatch.go` `IntervalBox`, `DurationNanoser` | the guard that keeps an interval out of every vector | relaxed for the INTERVAL vector only | B | an interval written into a DURATION or STRING vector |
| `engine/batch/pgtext.go` `FormatPGText` | no interval | postgres-style text (`EncodeInterval`) | C | wrong text for the explicit cast |
| `engine/batch/representative.go` `RepCompareBox`, `RepEligibleType` (ADR-0024 §12) | INTERVAL listed as "not here" | INTERVAL eligible with the §3 order | D | a first-met or plan-dependent representative |
| memory accounting (ADR-0006): the 16-byte triple per row; the canonical key (N, 16 bytes) and representative triple in group, distinct, join and set-operation tables; the aggregate state (triple sum, finite count, two infinity counts); spill and WSHF codec scratch | none | charged to the owning operator, released by it, spillable where its breaker spills | B (carrier), D (keys), E (states) | unbounded memory, or a budget refusal at the wrong place |

### 6.2 Expressions

| site | today | change | owner | if forgotten |
|---|---|---|---|---|
| `engine/expr/expr_interval.go` `IntervalValue` (opaque `text`), `compareIntervals`, `intervalOrder`, `cmpSeconds`, `cmpSecondsBig`, `addInterval`, `castToIntervalText`, `pgIntervalInput`, `CheckIntervalLiteralText`, `DurationNanos`, `raiseIntervalNot*` | single-unit, whole seconds; multi-field input is opaque and refused when applied, compared or rendered | replaced by the triple; N in 128 bits; every producer and consumer migrated together | C | two interval representations alive at once |
| `engine/expr/compile.go` `intervalValueOf`, `isIntervalLit`, `compileBinOp` | literal of one unit; sub-second refused 0A000 | the full grammar (§5) | C | 42601 / 22007 where PostgreSQL answers |
| `engine/expr/interval_arith.go` `intervalArith`, `intervalAdd`, `intervalNeg`, `intervalScale`, `intervalOfParts`, `pgParts`, `IsIntervalInputText`, `isIntervalBox` | arithmetic in whole seconds; a sub-second or infinite result refused (r29) | §4a exactly | C | wrong cascade, rounding or overflow |
| `engine/expr/interval_part.go` `intervalExtract`, `intervalPart`; `expr_cast.go` `castToInterval`, `castStringRender`; `cast_container.go` `leafTypeName`; `cmp_container.go` `boxShape`; `expr_helpers.go` `compare` | date_part float8; EXTRACT refused | EXTRACT numeric per field, date_part float8; casts in the measured directions | C | wrong field or type |
| `engine/expr/expr_session_fns.go` `dateShift`, `intervalShift`; `time_bucket.go` `fnTimeBucket`, `timeBucketStrideMillis`; `expr_math_fns.go` `fnMod` | date_add / time_bucket over the single-unit box | §4b temporal application (months, days, time order); sub-millisecond results refused | C | a calendar result applied in the wrong order or as 24 h days |
| `engine/expr/timestamp_difference_guard.go` and the timestamp difference (temporal r16: a double of milliseconds) | refused as an interval producer (#1713d) | `timestamp - timestamp` and `timestamp - date` produce the interval (`timestamp_mi` then `justify_hours`); the guard is not removed wholesale | C | a timestamp difference reaching the numeric fallback |
| `engine/exec/kernel/compare.go` `ResolveFilterKernel`, `ResolveColColFilterKernel`, `ResolveInFilterKernelArity` | no INTERVAL kernel | comparison by N | C | a field-wise comparison: silent wrong |
| `engine/expr/expr_leaf.go` `ColRef.Eval`, `boxVectorValue` | no INTERVAL column; `ColRef.Eval` falls back to `Vector.GetValue` | box the triple (no dedicated arm needed once `GetValue` boxes it) | B | a column reference reading a wrong box |
| `engine/expr/temporal_producer.go` `producesInterval`; `planner/physical/declared_output.go` `nodeIsInterval` (ignores its `decls` argument today); `engine/expr/interval_node.go` `IsIntervalNode` | interval recognition by expression **syntax** | recognition by the declared type of the bound slot (§9.1) | A (declares), D (relies on it) | a column reference to an interval-typed derived column not seen as an interval |
| `engine/expr/rettype.go` `CommonDeclType`; `declared_output.go` `caseDeclaredType`; `engine/expr/expr_special.go` `(*Case).boxMode` | CASE / COALESCE common-type resolution | INTERVAL as its own common type (PostgreSQL's category T) | A (declares), C (evaluates) | a fallback declaration or a wrong coercion (`10_definitions:closure/*`) |
| `engine/expr/interval_constant.go` `ConstantEvalError`; `planner/logical/table_func_args.go` `FoldTableFuncArg`; `engine/expr/outer_literal.go` `unrenderableOuterValue` | constant diagnostics, table-function string folding, correlated re-runs substituting typed literals | an interval outer value renders as `CAST('<postgres text>' AS INTERVAL)` (or is refused) | C (constants), D (correlated subqueries) | the type or the representation erased in a correlated re-run |

### 6.3 Keys, hashing, comparison and sort

| site | today | change | owner | if forgotten |
|---|---|---|---|---|
| `engine/exec/agg_key_encoding.go` `appendColumnValue`; `agg_group_key.go` `buildKeySerCols`, `decodeSerializedKey`, `decodeSerializedKeyIntoColumns`, `genericKeyBoxingDeferrable` | no INTERVAL; an unknown partial key falls back to a string | canonical key = N; the triple rides beside it as the representative; an interval key is never "boxing deferrable" (N cannot reconstruct the triple) | D | SQL-equal values in two groups, or a lost representative |
| `engine/exec/packed_hash.go` `packedKeyWidth` | no INTERVAL | exclude INTERVAL (the generic key path handles it) | D | a 16-byte value truncated into a packed key |
| `engine/exec/partitioned_agg.go` `PartitionSelectors`, `buildRoutePlan`, `legacyCompositeHash`; `exec/distinct_set.go` `newDistinctSetFor` | no INTERVAL | hash of N | D | SQL-equal values routed to different partitions: duplicate groups |
| `engine/exec/join.go` `isIntKeyColumn`, `intKeyFromVector`, `consolidateBuild`, `updateKeyMinMax`; `exec/join_key_width.go` `keyEncodingClass`, `widenKeyFloat64` | no INTERVAL | join key of N; no float widening | D | missed or wrong join matches |
| `engine/exec/sort_merge_join.go` `resolveCompareKernels`, `compareLR`, `keysEqual` | a missing kernel fails loudly | N comparator; equal-key runs by N | D | a field-wise comparator silently misses SQL-equal matches |
| `engine/exec/kernel/sort.go` `ResolveSortCompare`, `ResolveSortCompareNoNulls`, `ResolveSortCompareNullsLast`; `exec/sort_external.go` `resolveSortCompareForKey`, `sortEntriesLessFunc`; `exec/window.go` `compareVectorValues`; `coordinator/coordinator.go` `compareBatchRows` | no INTERVAL | compare N only (the SQL-tie rule) | D | the representative order leaking into SQL order |
| `engine/exec/sort.go` `(*Sort).compactTopKLocked`, `(*topNHeap).Less`; `planner/physical/topn_late_mat.go` | no INTERVAL | top-N by N; late-materialized selections | D | a missing comparator refuses; a bad copy corrupts output |
| `engine/exec/bloom_filter_op.go` `appendBloomKeyAt`; `dynamic_filter_emit.go` `dynamicFilterIntKey`, `(*DynamicFilterEmitOp).Execute` | bloom keys; dynamic filters decline non-integer keys | the bloom key from the canonical N encoding; keep the dynamic-filter exclusion unless implemented | D | **false negatives**: rows dropped silently |
| `planner/physical/set_op_types.go` `setOpTypeCategory`, `setOpWiden`; `set_op_arm_decls.go` `setOpArmDecls`; `set_op_result_column.go` `setOpResultColumn`; `set_op_plan.go` `setOpNullLayout` | no INTERVAL; DURATION sits in the numeric category | INTERVAL its own category (no widening with any other type); unknown and NULL arms take the interval (§9.1) | D | an interval arm widened with a number or text, or mistyped NULL arms |

### 6.4 Aggregates and windows

| site | today | change | owner | if forgotten |
|---|---|---|---|---|
| `engine/exec/kernel/agg.go` `ResolveBatchMin`, `ResolveBatchMax`, `ResolveBatchSum` (and the row forms); `exec/agg_accumulators.go` `initFlatAccums`, `isFlatSumType`; `exec/agg_scatter.go` `scatterFlatAggUpdate*` | refused at plan time (r30, `validate_interval_use.go`) | min / max by N with the rule-3 representative; sum / avg with PostgreSQL's field-wise accumulator and overflow | E | wrong aggregates or lost overflow |
| `engine/exec/agg_accumulator_merge.go` `mergeFlatAccumRow`; `agg_partial_merge.go` `(*HashAggregate).mergeSinkState` | clone merging of SoA accumulators | the interval state (triple sum, count, infinity counts), overflow, NULL and representative merge | E | wrong merged aggregates |
| `engine/exec/aggregate_partial_spill.go` `mapBatchTypeToParquet`, `writePartialKeyToColumn`, `writeIntKeyToColumn`, `emitAcc`, `readAcc`, `setPartialKeyFromAny` (its default arm turns an unknown boxed key into a string tag), `(*kWayMerger).Next`; `exec/agg_spill.go` | no INTERVAL | key (N and triple), representative, accumulator state | D (keys), E (states) | a guard error, or a wrong state or representative after spill |
| `worker/avg_fold.go` `writeAvgColumn`; `coordinator/coordinator.go` `reAggregatePartials`; the DAG partial / final merge | no interval partials | the partial is (sum triple, count, +∞ count, −∞ count); the final applies `interval_div` once | E | a re-aggregated average or sum differing from the single path |
| `worker/shuffle_partial_agg.go` `(*cappedPartialAgg).resolveAgainst`; `shuffle_local_accum.go` `(*partitionedShuffleSink).consumeLocalAccum` | sender precombine and local accumulation | key-only precombine (D), aggregate precombine (E), eligibility kept | D, E | wrong aggregates or lost eligibility despite a correct final merge |
| `engine/exec/window.go` `WindowMinMaxType`, `resolveWindowNumeric`; `exec/window_external.go` `windowDiskPass` | refused | window aggregates, the sliding inverse (`do_interval_discard`), window keys by N | E | wrong frames; inverse transitions out of step |
| `planner/physical/aggregate_declared_output.go` `minMaxDeclaredType`; `engine/exec/agg_output.go` `minMaxOutputType` (added in 1680c, the third judge) | the planner's mirror of the executor's MIN / MAX output table; an input type it does not list falls to its default, **FLOAT64** (the executor answers "keep the planner's declaration" for it) | an interval input declares the interval, never the FLOAT64 fallback; until E admits the family the argument is refused before either table is read (stage A's lineage declares MIN / MAX over an interval as an interval, §9.1) | A (declares), E (evaluates) | a MIN / MAX over an interval declared and printed as a double |
| `planner/physical/validate_aggregate_args.go` `aggArgPositions`, `aggArgFits` (1680c) | the aggregate argument classes per function; no interval class, so an interval argument fits no class or the class of its carrier | an interval argument fits only the families E admits, each named; the rest are refused under their identities | A (refuses), E (admits) | an aggregate admitted over an interval by its carrier's class |
| `planner/physical/validate_comparison_types.go` `comparisonClass` (1680c) | DURATION sits in the number class; no interval class | interval its own class: comparable only with an interval, an unknown literal or NULL (PostgreSQL's 42883 otherwise) | A (declares), C (evaluates) | an interval compared with a number through DURATION's class |
| `planner/physical/validate_boolean.go` `provableNonBooleanType`, `checkBooleanContext` (1680c) | a clause's operand typed by the scope's declarations; no interval type to name | an interval operand in a boolean context is PostgreSQL's 42804 `... not type interval`, read from the slot's lineage, not from the carrier | A | a boolean-context message naming the carrier (text, bigint) where PostgreSQL names interval |

### 6.5 Spill, shuffle, gather and the caches

| site | today | change | owner | if forgotten |
|---|---|---|---|---|
| `engine/exec/join_spill.go` `writeColumnData`, `readColumnData`, `newSpillBatchWriter` — the columnar run codec under sort runs (`sort_external.go` `writeSortedRun`), the CTE spool (`shared_spool.go` `drainLocked`, `readRunLocked`), window passes (`window_external.go` `windowDiskPass`) and join partitions | no INTERVAL | 16 bytes a row; the run header's type id validated on read (§9.1) | B | a guard error, or a mistyped read-back |
| `engine/batch/container_codec.go` `encodeVectorBody`, `decodeVectorBody` | an ARRAY of INTERVAL would be a DURATION array (`physical.arrayLitDeclaredType`) | stays refused: no interval element in a container | A | fields collapsed into nanoseconds |
| `worker/shuffle_format.go` `writeColumnData`, `(*shuffleWriter).writeHeader`; `wshf/wshf.go` `FixedTypeLen`; `wshf/decode.go` `ParseHeader`, `ReadColumn`, `DecodeBatches`; `wshf/schema_guard.go` `(*SchemaGuard).check` | no INTERVAL; `ParseHeader` accepts any type byte; an unknown type fails at `FixedTypeLen` on a NON-EMPTY chunk only (an empty chunk is skipped before decoding) | the 16-byte column; `ParseHeader` refuses an unknown id itself; the guard holds the declared id (§9.1, §10) | B (codec), D (admission) | decode or schema refusal, or mistyping; an older binary accepting an empty stream |
| `worker/partitioned_shuffle_sink.go` `hashRowsIntoPartitions`, `hashVectorValue` | no INTERVAL | partition hash of N, so SQL-equal intervals land in one partition | D | duplicate groups and missed joins across partitions |
| `coordinator/skew_split.go` `planSkewSplitTasks` | skew sub-tasks split probe files, replicate build files | partitions by N, the split engaged and asserted | D | lost or duplicated matches |
| `worker/gather_reply_sink.go` `(*gatherReplySink).Consume`; `coordinator/gather_receiver.go` `admitLocked`; `gather_replay.go` `(*gatherReplayStream).Next`; `dag_merge.go` `(*batchRenamer).apply`; `exec/batch_collector.go`; `coordinator/coordinator.go` `copyVectorValue`, `extractValue`, `setValueFromAny` | gather's WSHF payload, replay, renaming, identity; no INTERVAL | a case each; replay engaged and asserted | B (codec), D (admission) | an explicit gather refusal, or duplicate / incomplete results |
| `worker/executor.go` `getFileData` with `worker/cache.go` `(*LRUCache).Put` / `Get` | shuffle input read through tiers (local stage cache, NATS KV, the LRU, S3); the LRU caches bytes by `bucket/path`, evicts by capacity, has **no per-query cleanup** | nothing to change for the type; the lifetime account must include it (§10) | B (owner from 1680c: the carrier's lifetime account) | a claim that no cached byte outlives its query |
| `worker/resultstore.go` `ResultStore` (a 30-minute TTL, `CleanupQuery`); the `wadjet_results_data` NATS KV bucket (`coordinator.go`: a 5-minute TTL, purged at query end by `cleanupQuery`, best effort); `worker/local_stage_cache.go` `LocalStageCache` (`CleanupQuery`, wiped at construction); `worker/upload_manager.go` `Flush`, `releasePending`; `coordinator/cleanup.go` `CleanStale`, `StartPeriodicCleanup` | per-query transient copies with best-effort cleanup. A TTL here is a retention bound the store applies when it next expires or sweeps, not a deletion time: an entry can outlive its TTL until that sweep runs, and a process that exits first takes the in-memory copies with it [inferred from the cited code; not measured] | nothing for the type; the lifetime account (§10) | B (owner from 1680c: the carrier's lifetime account) | as above |

### 6.6 Doors, writes, containers and the planner

| site | today | change / stays refused | owner | if forgotten |
|---|---|---|---|---|
| `server/pgwire/server.go` `sendTypedRowDescription`, `pgColumnOID`, `pgTypeOID`, `pgTypeSize`, `sendColumnTypes`, `sendDataRow`, `appendBinaryCell`, `handleDescribe`, `describeSQL`, `TypeMod` | a bare interval result refused 0A000 (r20); Describe of two interval forms answers NoData (§7) | stays refused: a bare interval result at Describe, execution and zero rows | A (enforced), F (audit) | OID 1186 or a text-typed interval leaking onto the wire |
| `server/server.go` (HTTP) `(*Server).handleQuery`, `handleDescribe`, `writeJSON`; `server/grpc.go` `anyToProtoValue`, `streamResultBatches`; `server/mcp/server.go` `(*Server).toolQuery`; `wadjet/` `QueryResult`, `ColumnMeta`, `(*QueryResult).Cells` | the same r20 refusal applies before any door encodes | stays refused at every door (`server.TestIntervalRuleOnEveryDoor`) | A (enforced), F (audit) | an interval published through a door the pgwire test does not cover |
| `server/pgwire/bindparams.go` `renderTextParam`, `renderBinaryParam`; `paramtypes.go` | a text interval parameter typed 1186 is rendered as `CAST(… AS INTERVAL)`; binary refused | unchanged (ruling 2) | A | a new parameter capability by accident |
| `server/pgwire/server.go` `handleSet`, `showDefaults` | `SET IntervalStyle` stored and ignored | refuse a non-`postgres` IntervalStyle 0A000 naming it | A | answers in the wrong style silently (§11) |
| `wadjet/dml.go` `assignEvaluatedValue`, `assignSourceOf`, `dmlSourceDeclaredType`, over `physical.RefuseIntervalWrite` / `IsIntervalValuedWith`; CTAS | an interval value written is refused | stays refused: INSERT, UPDATE, MERGE, CTAS through every boundary (`12_writes:w/*`, `w/neg/*`) | A | an interval stored as text or DURATION |
| containers: `physical.arrayLitDeclaredType`, `batch/vector.go` `SetValue`'s `DurationNanoser` arm, ROW and MAP elements, `array_agg` | an interval element is refused by the interval rule | stays refused (`11_relational:neg/*/arrayAgg`, `arrayCtor`) | A | fields collapsed into a DURATION element |
| DURATION (`TypeDuration`, `parquet.ParseDurationNanos`) | a separate nanosecond type | unchanged; no implicit conversion | A | DURATION data reinterpreted |
| `planner/physical/validate_interval_use.go` (the interval rule) and `declared_output.go` (`colDecl`, `boundPos` in `declared_output_positions.go`, `logical/output_ids.go` `OutputColumnsWith`) | the A+ interim; declarations by bound position where the output list is enumerable | relaxed stage by stage over typed lineage (§9.1) | A, then each stage | an unsupported consumer reached by a column the rule cannot see |
| `storage/parquet/file_writer.go` `validateWriteColumn` (1680c, the third judge) | checks the container shapes (MAP, ARRAY, ROW) and returns nil for every scalar type id it does not list, so an id the writer has no physical encoding for is not refused there | an explicit refusal of the execution-only interval id, nested declarations included, before any page is written (ADR-0052 §6 condition 1) | B (with the id) | a file finalized with a column the reader cannot read, or an interval stored under another id |
| `storage/parquet/schema_tree.go` `refuseIntervalAnnotation`; Iceberg's interval refusal | readers refuse an interval annotation | unchanged; plus a whole encoded-Parquet refusal integration test | B (before the new id can reach storage) | a file with an interval column read as something else |
| `server/pgwire/server.go` `handleDescribe`, `describeSQL`, `TypeMod` | Describe and typmods | false metadata on empty and prepared statements (§7) | A | NoData where a client expects a refusal |
| `internal/oracle/typematrix/typematrix.go` `Columns` | the type matrix's catalog | an execution-derived interval fixture, not a stored column | D | the oracle stays green because INTERVAL is absent |

**Existing state** [wadjet-source]:

- **DURATION aliasing.** `ParseTypeID` maps `INTERVAL` to `TypeDuration` (the
  CAST machinery's alias), while `DeclaredColumn` refuses a **new** column
  spelled INTERVAL at any depth (0A000, #1713d);
  `parquet.TestDeclaredColumnRefusesANewIntervalColumn` pins both.
- **Containers are already refused** by the interval rule
  (`04_keys:array/group` 0A000 today, where PostgreSQL answers 1 group).
- **Parquet and Iceberg guards exist** (`refuseIntervalAnnotation` at every
  reader open, `parquet.TestReaderRefusesTheIntervalAnnotation`;
  `iceberg.TestTableReaderRefusesAnIntervalField`). A test that reads a whole
  encoded Parquet file with an INTERVAL column is still absent (owned by B).
- **Parameter tests exist** (`pgwire.TestIntervalRuleOverTheWire`).
- **`timestamp - date`** is refused today with 42883 by the planner;
  PostgreSQL answers an interval. Stage C implements it.
- **`lag(NULL, 1, ts)`**: PostgreSQL types the window result by the default.
  The inline form (`11_relational:lag/tsDiff`) needs C (the window over a
  timestamp exists today); the derived form (`lag/tsDiffCompare`) needs D.
- **The opaque `IntervalValue`**: replaced by the triple in C, every producer
  and consumer together.
- **A derived interval column was declared TEXT until stage A.** [measured on
  the engine at d489da4f] `11_relational:neg/derived/distinctSum` (and the
  CTE, set-operation and join forms) failed 42883 `function sum(text) does not
  exist`: the column `v` of `(SELECT CAST(s AS INTERVAL) AS v …)` reached the
  outer block typed by the STRING fallback. Since 1680c the slot is declared
  interval by lineage (`binder.intervalOut`, forwarded to every reading
  scope) and the aggregate-argument check does not read its carrier: the
  cells are refused `distinct-aggregate`. The carrier declaration itself
  (`DeclaredOutputSchema`, the plan's) still says TEXT; nothing reads it as
  an interval's type before stage D, because nothing is admitted.

## 7. What crosses a door

**Ruling 1 (decided): a bare interval result stays refused.** Describe,
execution, every door and zero-row results obey the same contract.
**Display path.** Ruling 1, as the architect's addition (a) clarifies it:
the display path is an **explicit scalar cast to a character type the engine
declares correctly**, each measured against PostgreSQL's OID. The admitted
set is exactly two types:

| target | PostgreSQL [measured] | this engine's declaration | admitted |
|---|---|---|---|
| `TEXT` | `text` (25), `cast/toText`, `cast/toTextNull` | `text` (25) | yes (C) |
| `VARCHAR` | `character varying` (1043), `cast/toVarchar` | a TEXT carrier with a length channel, sent as 1043 (`pgColumnOID`); the corpus harness names it `character varying`, so the type is checked on every arm | yes (C) |
| `CHAR(n)` / `CHARACTER(n)` / `bpchar` | `character` (1042), blank-padded to n: `01_input:cast/toChar` (`CAST(INTERVAL '1 mon' AS CHAR(10))`) answers `a:character(1042) \| 1 mon     ` (ten characters) | **1043, unpadded**: `pgColumnOID` deliberately never sends 1042 because the engine implements neither bpchar's padding nor its trailing-blank rules (`internal/server/pgwire/server.go`, the comment above `pgColumnOID`; the residual is `docs/adr/0012-divergences/text-collation.md`) | **no**: refused, reason `char-display`, owned by A |

CHAR is excluded because a stage admitting it would publish the wrong wire
type (1043 for PostgreSQL's 1042) and an unpadded value, a silent wrong
answer on both counts; that is a pre-existing CHAR gap, not an interval one,
and it is not this program's to close. Today the engine answers
`cast/toChar` with 42846 `cannot cast type interval to char(10)` on every
arm (loud, the wrong SQLSTATE for the named refusal: the cell is `unmet`
until A installs the `char-display` refusal). `VARCHAR` here means the
measured unconstrained form; a length-qualified `VARCHAR(n)` (which truncates
in PostgreSQL) is not measured by this record, and a stage admitting it
measures it first.

**Ruling 2 (decided): parameters are preserved as they are**: text and NULL
interval parameters keep their behaviour; binary interval parameters stay
refused; no new parameter capability.

**The client transcript** (`pgwire.TestIntervalClientEvidence`; PostgreSQL's
side recorded with `IV80_PG_DSN`) [measured], PostgreSQL against this engine
today:

| step | PostgreSQL 17.11 | this engine |
|---|---|---|
| Describe of a bare interval result | RowDescription `oid=1186 typmod=-1 len=16` | 0A000 |
| Describe / execute of `CAST(… AS TEXT)` | `oid=25`, rows `1 day; NULL; 30 days` | the same |
| the text cast with a binary result requested | `fmt=1`, the text bytes | the same |
| a bare interval, text or binary format | text rows; binary `0x…0000000100000000` | 0A000 |
| NULL through the text cast | `NULL`, oid 25 | the same |
| an empty result: Describe and execute of the text cast | `oid=25`, zero rows | the same |
| an empty result of a bare interval | `oid=1186`, zero rows | 0A000 at Describe and at execution |
| a parameter typed 1186: text `24 hours`, NULL, binary | rows `1`, none, `1` | rows `1`, none; binary 0A000 |
| a text parameter `1 mon` (multi-field) | row `3` | 0A000 (opaque) |
| `$1` compared with `CAST(s AS INTERVAL)`, untyped | parameter inferred 1186 | inferred 25 (text) |
| Describe before Bind of `SELECT CAST($1 AS INTERVAL)` | parameter 1186, field `oid=1186` | parameter 1186, **no fields (NoData)**; execution 0A000 |
| Describe of `CAST(… AS INTERVAL(3))` / `INTERVAL DAY` | typmod `2147418115` / `589823` | **no fields (NoData)** |
| ParameterStatus `IntervalStyle` at connect | `postgres` | `postgres` |
| `SET IntervalStyle = 'iso_8601'`; the text cast of `1 day` | `P1D` | **`1 day`** (accepted and ignored) |
| `SET IntervalStyle` to `postgres_verbose`, `sql_standard`, `iso_8601`, `postgres` (`door/IntervalStyle/*`, round 3) | `ok` each | `ok` each (stage A must refuse the first three, naming IntervalStyle) |

Three rows are pre-existing defects outside this stage's contract (§11). The
judge cited pgJDBC 42.7.5's `PgResultSet` source for `getObject` on a
character type; that is the judge's reading, not re-verified here
[unresolved for this record; no JDBC client was run].

## 8. What stays out

| out of this stage | pinned by |
|---|---|
| a column spelled INTERVAL in DDL, CTAS of an interval, INSERT / UPDATE / MERGE of an interval value, including through derived columns, a CTE, a subquery, a set-operation arm, a join, zero rows and `WITH NO DATA` | `parquet.TestDeclaredColumnRefusesANewIntervalColumn`; `coordinator.TestArc1713cIntervalOrderEveryArm` (`ddl/intervalColumn`); `server.TestIntervalRuleOnEveryDoor`; `12_writes:w/*`, `w/neg/*` (manifest `refusal interval-write`, owned by A; `w/ctasText`, a CTAS of a text cast, is `supported` in C) |
| Parquet and Iceberg interval columns | `parquet.TestReaderRefusesTheIntervalAnnotation`, `iceberg.TestTableReaderRefusesAnIntervalField` |
| the wire OID 1186 and the binary format | §7; `pgwire.TestIntervalClientEvidence`, `server.TestIntervalRuleOnEveryDoor` |
| new parameter capability (binary interval parameters) | `pgwire.TestIntervalRuleOverTheWire`, `pgwire.TestIntervalClientEvidence` (`param/binary`) |
| IntervalStyle other than postgres, typmods, timestamptz (including `age(date, date)`), TIME | the manifest's refusals, owned by A; the SET refusal by the door assertions (§9) |

## 9. Staging and the stage manifest

**[proposed throughout.]** Every intermediate state answers PostgreSQL's value
or refuses; none answers differently in silence. A stage that lands adds its
letter to `iv80LandedStages`; from then on every cell it owns must be met on
all six arms.

**The four outcomes** (`<stem>_manifest.tsv`, `TestArc1680bStageManifest`),
checked by `iv80Meets` (round 3):

- **The internal-failure exclusion comes first**, decided by PROVENANCE
  (corrected in 1680c; round 3 decided it by a word blacklist over the
  sentence): a panic, an error without a SQLSTATE (the batch store's guards
  are such errors), XX000, or a code attached by a layer that did not raise
  it (`sqlerr.Attached`: `unsupported shuffle type: 255` under 0A000) meets
  **no** outcome, PostgreSQL's own error included. The recorder reads the
  provenance off the error's type chain, and a worker's failure carries it
  across the DAG (`ResultNotification.SQLAttached`).
- `supported`: the column names, the declared types and the rows equal
  PostgreSQL's, **in order when the statement's top level orders, as a
  multiset otherwise**; a ROLLUP whose ORDER BY leaves ties compares ordered
  between distinct values of its visible leading key and as a multiset
  within a tie (corrected in 1680c; round 3 compared it wholly unordered).
- `pg-error <SQLSTATE>`: PostgreSQL's own error, the same SQLSTATE.
- `refusal <reason>`: 0A000 **with the named reason's identity**, which the
  engine emits beside the sentence (`sqlerr.Refuse`, read by
  `sqlerr.ReasonOf`; recorded as `ERR 0A000 {<reason>}: …`; corrected in
  1680c: round 3 matched a word pattern over the sentence, so `time-type`
  matched a timestamptz sentence and `function-date_trunc` any sentence
  naming date_trunc). An unrelated 0A000 earns nothing. Where PostgreSQL
  itself answers 0A000 (`07_functions:trunc/week`), the named refusal is
  still met.
- `rep-divergence`: the cell's explicit rule-3 answer (§3), PostgreSQL's
  types, and every column outside the spec's units exactly PostgreSQL's. A
  GROUP BY beside SUM / AVG with no interval key is `supported`
  (`05_arith_cast:agg/sumGroup~text`, round 2's `rep-divergence`, corrected).
- **Legal alternatives** (fifth manifest column): a plan-dependent outcome
  PostgreSQL's semantics permit (ADR-0013), written to ruling 11's
  recommended option, pending: `11_relational:ov/aggUnordered` meets
  PostgreSQL's 22008 **or** the exact sum `2147483647 days` another
  combination order of the unordered sum gives (`ov/aggDesc` measures that
  order); `05_arith:agg/sumOverflowThenBack` (the same values, a bare result)
  meets 22008 or `refusal bare-interval-result`. A window's prescribed order
  (`ov/winForward`, `sw/all`) has no alternative. A wrong finite sum meets
  nothing.

`TestArc1680bOutcomeChecks` holds each of these with adversarial cases (an
unrelated 0A000, the wrong capability refusal, a runtime error presented as
0A000, a store guard coded like PostgreSQL's error, trunc/week, a tuple
fabricated from two rows, PostgreSQL's non-winning representative, the wrong
direction, SUM / AVG changed beside MIN / MAX, the rule-3 value under the
wrong type, unordered and ordered permutations, a doubled row, the ruling-11
alternative and a wrong finite sum).

**The architect's rulings on 1680c's conflicts (r3)** amend the manifest
(`iv80Ruled`), for a cell the rules gave an A refusal that the baseline did
not meet: (1) a cell right on main on every arm is `supported`, owned by A
(8 cells: the typmod display casts `INTERVAL '5' YEAR … ~text`); (2) a cell
this engine's parser rejects (`plansql.Parse`: the SQL grammar, 42601, and
the interval literal reader, 22007) is **outside the program**: outcome
`not-parsed <SQLSTATE>`, no owning stage, held only to the parser's
refusal (325 cells); (3) the six TIME cells (`TIME - TIME`, `DATE ± TIME`)
stay A refusals `time-type`. A owned 645 cells before the rulings and 320
after.

**Ownership: the earliest stage that admits what the cell needs**
(`iv80EarliestStage`, `TestArc1680bStageRules`). Every refusal is owned by
**A**: a named refusal holds at every stage from the first, so the
bare-result, write, container, ordered- and DISTINCT-aggregate and
offset-frame refusals are enforced from A onward (the bare-result refusal
was F's and the aggregate and frame refusals E's in round 2). Every other
cell is owned by the latest of what it needs, read from its SQL: **E** if an
aggregate or window function takes an interval argument, or a window's
PARTITION BY / ORDER BY is an interval; else **D** if an interval crosses a
block boundary (a derived table's, CTE's, subquery's, set-operation arm's or
VALUES list's interval column), or is a GROUP BY / DISTINCT / ORDER BY /
join key, or a correlated interval equality (a join key once decorrelated);
else **C**. The judge's five cells move: `12_writes:w/ctasText` A → C,
`10_definitions:closure/unionNull` and `closure/whereFalseText` C → D,
`11_relational:sub/scalarMax` and `sub/correlatedMin` D → E. The rule's sweep
moved 16 more: `02_fields:print/text/postgres` C → D (a VALUES list's
interval column), `11_relational:lag/tsDiffCompare` E → D; `04_keys:withTies/*`
and `withTiesCount/*` E → D (a top-N key); to C (inline only):
`03_order_cast:order/greatestTie~text`, `04_keys:adr0024/intersectNumeric*`
(no interval), `07_functions:widthBucket` (PostgreSQL's 42883),
`11_relational:lag/tsDiff`, `lag/tsDiffBare~text`, `empty/text`,
`empty/bare~text`, `oj/leftText` (the interval is computed after the join
from text), `frame/tsInfRowsShift`.

**F owns no cell.** It audits the publication policy that A enforces, with
the client transcript.

**IntervalStyle cells are reachable.** The corpus applies a cell's
IntervalStyle on the reference only; such a cell (`02_fields:print/<style>`,
`input/sqlstd/mixed`) is now decided by the engine's answer to `SET
IntervalStyle = '<style>'` at the door (`pgwire.TestIntervalClientEvidence`,
`door/IntervalStyle/<style>`): met when that answer is 0A000 naming
IntervalStyle. Today it is `ok` (unmet). (Negative control: a door answer of
`ERR 0A000: SET IntervalStyle = 'sql_standard' is not supported` met the
three sql_standard cells; a door answer of 0A000 with the bare-result
sentence did not meet the iso_8601 cells.)

**Today** (1,331 statements under test): A owns 645 (all refusals; 102 met:
the bare-result refusal 92, writes 3, generate_series 4, container 1,
date_trunc 1, interval_hash 1; 543 unmet, most of them refused today with
another reason's sentence or another SQLSTATE); C owns 536 (supported 67 met,
361 unmet; PostgreSQL errors 54 met, 54 unmet); D owns 98 (52 supported, 44
rep-divergence, 2 PostgreSQL errors; none met); E owns 52 (29 supported, 13
rep-divergence, 10 PostgreSQL errors, one met through its legal alternative).

**The interim validator's assumption.** `validate_interval_use.go` states
that "a column is never [interval-valued]: a block that would publish an
interval is refused, so no derived table, CTE, set-operation arm, VALUES row
or subquery can hand one on". Admitting internal interval columns (stage D)
breaks that premise; §9.1 replaces it.

| stage | admits | interim refusal it lifts | invariant test it carries | why no consumer sees an interval column it does not handle |
|---|---|---|---|---|
| **A** declarations and guards | nothing | none | every interval producer and typed slot is recognized and declared (§9.1); every named refusal of the manifest is 0A000 with its reason, decided before rewrites and again at run time; the bare-result and write refusals at every door and zero rows; `SET IntervalStyle` other than postgres refused; manifest stage A cells met | no interval column exists yet: the interim rule still refuses every block that would publish one |
| **B** carrier and transport | nothing | none | the carrier work of the engagement table below | the type exists only in the carrier's own tests; the planner never declares it |
| **C** inline expressions | one proven scalar operation at a time over values that never become a column: input grammar, comparison by N, §4a, §4b for `timestamp` / `date`, `timestamp - timestamp` and `timestamp - date`, EXTRACT (finite) / date_part, `justify_*`, `make_interval`, `age(timestamp, timestamp)`, `date_bin`, each displayed through the explicit character-type cast | the opaque-interval refusals and r29 for each operation it proves; the timestamp-difference producer refusal operation by operation | every emitted result has the declared type; manifest stage C cells met | the result is consumed inline by an allowlisted parent or by the explicit cast; a block that would publish it is still refused |
| **D** internal columns and keys | interval columns on the CARRY allowlist (§9.1), interval keys on the EVALUATE allowlist: filter, project, sort and top-N keys, derived tables, CTEs, set-operation arms, subqueries, outer-join padding, GROUP BY / DISTINCT / set-operation keys with the rule-3 representative, hash and sort-merge join keys, their spill, the DAG shuffle and partitioner | r28 (interval keys); the internal-publication parts of r20 | **no interval-typed vector reaches an operator outside the allowlist**, checked on the optimized plan, on every stage plan, and at runtime batch entry of each operator; a negative control removes one allowlist check and shows each of the three checks catch it; manifest stage D cells met (`11_relational:neg/*` stay refused) | typed lineage (§9.1) |
| **E** aggregates and windows, one family at a time | `count`, unordered `min` / `max`, `sum` / `avg` with PostgreSQL's accumulator, window aggregates with the inverse transition, `lag` / `lead` defaults, window ORDER BY and PARTITION BY over intervals | r30 for exactly the family admitted | the engagement table's E work; manifest stage E cells met | an aggregate not yet admitted refuses its interval argument by the declared type |
| **F** publication audit | nothing | none | `pgwire.TestIntervalClientEvidence` and `server.TestIntervalRuleOnEveryDoor` re-run against the final policy | it audits; A enforces |

**Execution-path engagement: which stage owns it.** None of this is done in
this item; each line is a gate of the stage named, and a carrier test in B
cannot discharge a relational line of D or an aggregate line of E.

| stage | engagement work it owns |
|---|---|
| **B** | the triple and NULL carrier through selection, views, `Flatten` and pool reuse; spill round trips (sort runs, CTE spool, window passes, join partitions, the partial-aggregate codec); WSHF and gather round trips, empty streams included; the architect's two conditions on ruling 10 (§10): (1) every shuffle and spill reader validates each column's type byte in the HEADER, empty streams included, refusing an unknown id with SQLSTATE XX001 and the sentence §10 gives, and (2) the enforced homogeneous-version or capability contract that distributed admission of the type (D) requires; memory charged to its owner (ADR-0006); the encoded-Parquet refusal integration test (before the new id can reach storage) |
| **C** | `make_interval`'s numeric-constructor sentinel handling (a result equal to a reserved triple is 22008, [pg-source] only today), with focused implementation checks; every scalar operation on both rounding paths |
| **D** | asserted hash and sort-merge joins over interval keys; bloom on and off; key partitioning by N; top-N and late materialization; join spill; skew split; gather replay; key-only grouping / distinct precombine; representative provenance and eligibility through precombine, clone merging, spill and the final merge |
| **E** | SUM / AVG states; clone merging of accumulators; aggregate precombine; partial-state spill; the distributed partial / final combine; window passes (frames, the inverse transition, window spill), family by family |

### 9.1 Typed lineage: the contract

[wadjet-source] for what exists, [proposed] for the rest.

**1. The authoritative slot.** A column's type is the declaration of its
**bound output slot**: `declPos` in
`planner/physical/declared_output_positions.go`, one per output position of a
logical node, carrying the binding it is (`plansql.Binding{Rel, Ord}`) and its
declaration (`parquet.Column`). A bound column reference
(`plansql.ColRef.Bound`) is declared by the slot whose binding it holds
(`ColDecls.boundPos`; `ColDecls.colDecl` consults it before any name rule;
ADR-0047 stage 2). The positional lists come from
`logical.OutputColumnsWith` (`logical/output_ids.go`):

- a scan's columns, a derived table's root or a CTE reference answer for
  their instance (`n.Rel`, `RelCols`): each position is that instance's
  column, so a CTE reference and a derived table forward their body's
  declaration by position;
- a projection's items are its own positions; an item that IS a bound column
  reference (an alias, a pass-through) carries that column's binding, so an
  alias forwards the declaration unchanged;
- Filter, Sort, Limit and Distinct pass their input's positions through; a
  join concatenates its arms' (a semi or anti join the left's only); a window
  is its input's plus its own slots; an aggregate's GROUP BY keys carry the
  key's binding, its aggregates are its own.
- **What it does not cover today:** a set operation, an unexpanded star and
  any node kind the list does not describe answer `nil`, and a reference into
  them falls back to the **name** rules, whose last resort is the STRING
  fallback (`colRefDeclaredType`; measured above: today a derived interval
  column reaches the outer block as TEXT). `nodeIsInterval` ignores its
  `decls` argument and is syntactic (`expr.IsIntervalNode`).

**The contract** [proposed]: (a) the INTERVAL type id is the declaration of
every slot an interval producer fills (literal, cast, arithmetic, timestamp
difference (including one whose operand is a `lag` / `lead` typed TIMESTAMP
by its default: the window function itself produces a timestamp, the
subtraction produces the interval), `timestamp - date`, CASE / COALESCE /
NULLIF / GREATEST / LEAST over an interval, MIN / MAX over one);
(b) `nodeIsInterval` and `producesInterval` answer from the bound slot's
declaration for a column reference, from the syntax only for an expression
that is not one; (c) a set operation gets a positional list (its result
positions are the reconciled declarations below) before stage D admits an
interval through one; (d) **the STRING fallback never applies to a slot an
interval producer could fill**: a reference whose slot is unresolved, where
any producer of the column could be interval, is refused 0A000 (a named
reason `interval-lineage-unresolved` [proposed; no corpus cell, since no
statement reaches it today]); (e) scalar and correlated subqueries: the
subquery's single output slot carries the declaration to the outer
expression; a correlated re-run substitutes the outer value as a
column-typed literal (`engine/expr/outer_literal.go`), so an interval outer
value renders as `CAST('<postgres text>' AS INTERVAL)` or is refused there,
never as a DURATION or bare text.

**2. Set operations.** [measured] An interval arm reconciles with an unknown
literal arm and a NULL arm to interval (`10_definitions:setop/unknownArm` =
`1 day; 2 days`, `setop/nullFirstArm` = `NULL; 1 day`, `closure/unionNull`),
with a timestamp-difference arm to interval (`setop/tsDiffArm`), and with an
integer or a text arm not at all: 42804 (`setop/intArm`, `setop/textArm`).
[proposed] INTERVAL is its own category in `setOpTypeCategory` (PostgreSQL's
category T): `setOpWiden` widens it with nothing; a NULL or unknown arm takes
the interval declaration (`setOpNullLayout` allocates an interval NULL
vector); any other pairing is PostgreSQL's 42804 at plan time.

**3. Transport carries and validates the identity.** [wadjet-source] The
stage DAG's schemas are `[]parquet.Column` (`dagplan/stage_types.go`:
`ScanSchema`, `JoinProbeSchema`, `JoinBuildSchema`, `OutputSchema`), the task
wire's column type is an integer id (`distributed/messages.go` `ColumnSpec`,
`AggFieldSpec`), the WSHF header carries one type byte per column
(`shuffleWriter.writeHeader` / `wshf.ParseHeader`), the columnar spill run
carries its schema (`newSpillBatchWriter`), and decoded vectors are built
from the decoded schema (`batch.NewRecordBatch`). Today `wshf.SchemaGuard`
holds every file of one stage input to the **first** file's schema, not to
the planner's declaration. [proposed] (a) every codec writes the interval id
and every reader validates it against the slot's declaration: the WSHF and
spill readers refuse an unknown id at the header, before any chunk (so an
empty stream is refused too); (b) `SchemaGuard` is seeded with the stage
input's declared schema, so a decoded column whose id disagrees with its
declaration is refused by name; (c) a runtime batch whose column type
disagrees with the operator's declared input (checked at each allowlisted
operator's batch entry in D) is a **loud internal error, XX000**, never a
coercion: it is a planner or executor defect, and the manifest's exclusion
already gives it no credit; (d) an unresolved declaration is the 0A000 of 1(d)
above.

**4. Carrying is not evaluating.** An operator that CARRIES an interval
payload moves its 16 bytes without looking at them; one that EVALUATES an
interval key or expression compares, hashes or computes with N or the fields.
D admits carrying broadly and evaluation narrowly:

| operator | carries an interval payload | evaluates an interval key / expression |
|---|---|---|
| scan | never (no stored interval) | never |
| project | D (copy, gather) | C: the admitted scalar operations only |
| filter | D | C: comparison predicates by N |
| sort, top-N, late materialization | D | D: sort key by N (SQL-tie rule) |
| limit, offset | D | — |
| hash aggregate / distinct | D: as the key's representative | D: grouping / distinct key by N; E: as an aggregate argument, family by family |
| hash join, sort-merge join | D: both sides' payload columns | D: join key by N |
| cross / nested-loop join | D | C: admitted predicate expressions only |
| set operation | D: arm columns | D: UNION / INTERSECT / EXCEPT key by N with left-arm eligibility |
| window | D: pass-through columns | E: PARTITION BY / ORDER BY and aggregate arguments |
| bloom / dynamic filter | — | D: bloom key from N only; dynamic filters keep declining |
| spill (sort, join, aggregate, window, CTE spool) | B (codec), admitted with its operator | — |
| shuffle, gather, partitioner | B (codec), D (admission) | D: partition hash by N |
| UDFs, table functions | never | never (refused) |
| containers (ARRAY / ROW / MAP, array_agg) | never | never (refused) |
| writes (INSERT / UPDATE / MERGE / CTAS) | never | never (refused) |
| doors (bare results) | never | never (refused) |

**Negative cells** (round 3, `11_relational:neg/<boundary>/<consumer>` and
`12_writes:w/neg/<boundary>`): an interval COLUMN reaching, through a
projection, a derived table, a CTE, a scalar subquery, a set-operation arm
and a join output (each on all six arms, so through spill and shuffle too),
`to_char` (`function-to_char`), `date_trunc` (`function-date_trunc`), a cast
to TIME (`time-type`), `mode() WITHIN GROUP` (`ordered-aggregate`),
`sum(DISTINCT …)` (`distinct-aggregate`), `array_agg` and an ARRAY
constructor (`container`), and an INSERT (`interval-write`). Each is a named
refusal owned by A, so every stage must keep it. Today each is refused, but
with the bare-result sentence, a 42601 or the 42883 above: unmet.

**Bare-result and write protection are enforced from stage A onward**; F only
audits them.

## 10. Pending with the maintainer

### Ruling 10: where the type identity lives

| option | what it costs |
|---|---|
| **append one execution-only type id to the shared enum** (`storage/parquet/schema.go` `TypeID`), existing ids and DURATION's meaning unchanged, guarded so it can never be written to Parquet, Iceberg, the catalog or the wire as a column type (**recommended, architect**) | every codec keyed on the shared id (vectors, hashing, sort, spill, WSHF, gather) gains one case, and the transient codecs (WSHF, spill) gain a type byte value; the storage writers, the catalog and the doors each refuse it; `ParseTypeID("INTERVAL")` keeps answering DURATION |
| a separate execution-type layer | a second identity beside the shared id. It need not duplicate every codec: adapters could translate at the boundary and share the physical codecs (a 16-byte fixed-width column is a shape several codecs already handle). The cost is the translation itself: every boundary between the layers (planner declarations, task wire, WSHF header, spill header, decoded vectors, doors) is a place where the identity must be mapped and could be lost |

**The architect's two conditions on the recommended option**, both
**stage-B obligations** (the B row of the stage table and the engagement
table, §9):

1. **The shuffle and spill readers validate the type byte in the header,
   empty streams included, with a coded refusal.** Every reader of a
   transient stream (WSHF `ParseHeader` and the worker's
   `shuffle_format.go` reader; the partial-aggregate spill header
   `partialSpillHeader`; the columnar spill format of `join_spill.go`
   `writeColumnarBatch` / `readColumnDecl`, shared by join partitions, sort
   runs, CTE spool and window passes) checks each column's type byte against
   the ids this binary knows **when it reads the header**, before and
   independently of any chunk decode, so a stream with no rows is refused the
   same as one with rows. [wadjet-source] The columnar spill format writes
   the type byte per batch, not once per file, so a spill file with no
   batches carries no type byte at all; B therefore gives the columnar spill
   format a schema header (each column's type byte, written once per file)
   that the reader validates before any batch, empty files included. Spill
   is written and read by one process within one query, so for spill this is
   defence against corruption and against a mistyped reader, not against
   another binary; the obligation is the same either way. The refusal is SQLSTATE **XX001** (`data_corrupted`, the code
   PostgreSQL raises when a file it reads is not in the format it expects),
   sentence `<shuffle|spill> stream <path> column <i> has type id <n>, which
   this binary does not know` [proposed, for the stage-B brief]. Not 0A000,
   because the manifest credits 0A000 only as a named capability refusal a
   user statement provokes (§9); not XX000 and not an uncoded error, because
   the manifest's internal-failure exclusion rejects both (today's `unsupported
   shuffle type` has no SQLSTATE).
2. **Distributed admission of the type requires an enforced
   homogeneous-version or capability contract.** Condition (1) protects only
   a reader built with it; an already-deployed older reader still skips an
   empty chunk and fails uncoded on a non-empty one (below). So before any
   plan that moves an interval column between processes (stage D's shuffle,
   gather and stage outputs) may run, the coordinator must know that every
   process that can read it understands the id: each worker advertises its
   build or an interval-capability marker, and the coordinator admits an
   interval-carrying distributed plan only when every live worker it can
   dispatch to advertises it, refusing loudly otherwise. ADR-0010's
   wholesale-deploy rule states the policy but nothing enforces it, and no
   worker capability or build advertisement exists today [wadjet-source:
   none found in `internal/worker`, `internal/coordinator`,
   `internal/distributed`]; B builds the enforcement, D depends on it.

The recommendation's own clause (not one of the architect's conditions):
approve the transient codec extension (a new WSHF and spill type byte value)
explicitly as part of the ruling, since it is a format change even though not
a persistent one.

**Can a file carrying the new type byte outlive the query or be read by
another binary?** [wadjet-source, corrected in round 3]

- **Spill files** (join partitions, sort runs, CTE spool, window passes,
  aggregate partial state) are written under the task's spill directory and
  read back by the same process during the same query.
- **Stage outputs and WSHF shuffle files** are written to the object store
  under `queries/<id>/…` and may also sit in the worker's `LocalStageCache`,
  the worker's in-memory `ResultStore` and the `wadjet_results_data` NATS KV
  bucket. Cleanup is **best effort**, not a guarantee of deletion within one
  TTL: at query end `Coordinator.cleanupQuery` deletes the query's objects,
  purges its KV keys asynchronously (errors ignored) and broadcasts
  completion so workers drop `LocalStageCache` and `ResultStore` entries
  (`coordinator.go`: "workers that miss the message will eventually leak
  entries until process exit"). What survives is removed by
  `ResultCleaner.CleanStale` (`coordinator/cleanup.go`), which runs
  periodically (`StartPeriodicCleanup`, every 10 minutes by default),
  selects objects by **object age** past its TTL (one hour by default),
  skips in-flight queries, and leaves an object whose delete fails for a
  later run: retention is extended by the period and by every failed delete.
  The KV bucket has its own TTL (5 minutes) and `ResultStore` its own (30
  minutes); `LocalStageCache` is wiped when a worker process starts.
- **Lazy uploads** under `--shuffle-durability=lazy` are queued until
  demanded and elided at query end, but they are also **released by a
  graceful worker drain** (`uploadManager.Flush`, called by `Worker.Drain`,
  releases every queued lazy upload so consumers keep a durable copy): a lazy
  file can be written without a consumer asking.
- **The worker's LRU** (`worker/cache.go`, filled by
  `Executor.getFileData`, through which shuffle input is read) caches the
  bytes of KV and S3 reads by `bucket/path`, evicts by capacity only, and has
  **no per-query cleanup**. Round 2's "result caches: none survives a query"
  was **false**. What it does not imply: every key carries the query's id
  (`queries/<id>/…`), so no other query addresses those bytes [inferred from
  the path layout]; the cache is in-process memory, so no other binary reads
  it. Retained bytes are a memory cost and a lifetime fact, not cross-query
  reuse.
- **A different binary** reads these files only within one query, when the
  coordinator and workers run different builds. ADR-0010 states that a
  cluster's coordinator and workers are **deployed wholesale, never
  rolling** (round 2 called the cluster policy unstated; it is stated, as a
  deploy rule, but nothing enforces it). WSHF has no format version;
  `wshf.ParseHeader` reads each column's type byte without validating it, and
  an older reader rejects an unknown type only when it decodes a **non-empty**
  chunk (`decodeChunkAt` → `minChunkBytes` → `FixedTypeLen`: `unsupported
  shuffle type`, an error with no SQLSTATE); an empty chunk is skipped before
  any decode (`decode.go`), so an older reader can accept an empty interval
  stream without complaint. Hence conditions (1) and (2) above.

### Ruling 11: interval `sum` / `avg` overflow under parallel execution

| option | what it costs | |
|---|---|---|
| **keep PostgreSQL's finite-width checked accumulator and refuse on overflow** | which inputs overflow can depend on combination order under parallel and distributed execution: a loud, plan-dependent 22008, never a wrong value; recorded as a class of legal nondeterminism under ADR-0013, with the window's prescribed order kept. [measured] PostgreSQL's own answer already depends on order: `11_relational:ov/winForward` 22008, `ov/winBackward` answers; `ov/aggDesc` = `2147483647 days`, `ov/aggUnordered` 22008 | **recommended (architect)** |
| a wider accumulator state that never overflows where PostgreSQL would | answers where PostgreSQL refuses: a deliberate divergence, and a final narrowing step that can still refuse | not taken |
| constrain execution order (no parallel combine for interval `sum`) | loses parallel aggregation for the type; still refuses where PostgreSQL's serial order refuses | |

**Reconciled with the manifest** (§9): under the recommended option the
unordered cells carry their legal alternative (`ov/aggUnordered`: 22008 or the
exact `2147483647 days`; `05_arith:agg/sumOverflowThenBack`: 22008 or the
bare-result refusal). Under the second option those cells' 22008 would no
longer be an expected outcome; under the third, only 22008 would. The
alternatives are written as data (`iv80Alternatives`) so the ruling changes
two lines.

## 11. Existing temporal wrong answers that meet interval paths (epic #1680)

Ruling 8: these pre-existing wrong answers are not this program's to fix; they
are listed as lines for epic #1680, and **wherever an interval operation would
produce or consume a value through one of these paths, the interval program
refuses the statement** (the manifest's `refusal time-type` and `refusal
timestamptz`, owned by A). Every answer below is the same on all six arms.

| statement | PostgreSQL 17.11 | this engine |
|---|---|---|
| `SELECT TIME '01:00:00' - TIME '23:00:00'` (`06_calendar:time/minusTime`, `10_definitions:types/timeMinusTime`) | interval `-22:00:00` | double precision `-22` |
| `SELECT INTERVAL '1 hour' + TIME '23:30:00'` (`07_functions:operand/plusTime`) | time `00:30:00` | text NULL |
| `SELECT TIME '23:00:00' + INTERVAL '2 hours'`, `INTERVAL '2 hours' + TIME '23:00:00'`, `TIME '01:00:00' - INTERVAL '2 hours'` (`types/timePlusInterval`, `intervalPlusTime`, `timeMinusInterval`) | time `01:00:00`, `01:00:00`, `23:00:00` | text NULL each |
| `SELECT DATE '2024-01-31' + TIME '01:02:03'`, `DATE '2024-01-31' - TIME '01:02:03'` (`types/datePlusTime`, `dateMinusTime`) | timestamp `2024-01-31 01:02:03`, `2024-01-30 22:57:57` | text `19754`, `19752` |
| `SELECT TIMESTAMPTZ '2024-03-09 12:00:00-05' + INTERVAL '1 day'`, `… + INTERVAL '24 hours'` (`06_calendar:tz/*/day`) | UTC: `2024-03-10 17:00:00+00` both; New York: `2024-03-10 12:00:00-04`, `13:00:00-04` | timestamp `2024-03-10 12:00:00` both (offset discarded, E36) |
| `SELECT TIMESTAMPTZ '2024-11-02 12:00:00-04' + INTERVAL '1 day'`, `… '24 hours'` (`tz/*/fallBack`) | UTC `2024-11-03 16:00:00+00` both; New York `12:00:00-05`, `11:00:00-05` | timestamp `2024-11-03 12:00:00` both |
| `SELECT INTERVAL '1 day' + TIMESTAMPTZ '2024-01-01 00:00:00+00'`, `TIMESTAMPTZ '2024-01-02 00:00:00+00' + INTERVAL '1 day'` (`operand/plusTimestamptz`, `types/tstzPlusInterval`) | timestamptz `2024-01-02 00:00:00+00`, `2024-01-03 00:00:00+00` | timestamp `2024-01-02 00:00:00`, `2024-01-03 00:00:00` |
| under TimeZone America/New_York: `TIMESTAMPTZ '2024-03-09 02:30:00' + INTERVAL '1 day'`, `TIMESTAMPTZ '2024-03-10 02:30:00'` (`dst/gapTarget`, `dst/gapInput`) | `2024-03-10 03:30:00-04` | timestamp `2024-03-10 02:30:00` |
| `TIMESTAMPTZ '2024-11-02 01:30:00' + INTERVAL '1 day'`, `TIMESTAMPTZ '2024-11-04 01:30:00' - INTERVAL '1 day'` (`dst/foldTarget`, `dst/foldMinus`) | `2024-11-03 01:30:00-05` | timestamp `2024-11-03 01:30:00` |
| `TIMESTAMPTZ '2024-11-03 00:30:00' + INTERVAL '1 hour'`, `… + INTERVAL '2 hours'` (`dst/hoursAcrossFold`) | `01:30:00-04`, `01:30:00-05` | timestamp `01:30:00`, `02:30:00` |
| over pgwire: `SET IntervalStyle = 'iso_8601'`, then `SELECT CAST(INTERVAL '1 day' AS TEXT)` (`pgwire.TestIntervalClientEvidence` `intervalstyle/text`) | `P1D` | `1 day` |

Loud defects found on the same paths (not silent, also for #1680): Describe
before Bind of `SELECT CAST($1 AS INTERVAL)` and Describe of a typmod-qualified
interval answer NoData (§7); an untyped parameter compared with an interval is
inferred text, PostgreSQL infers interval (§7); `timestamp - date` is 42883
`operator does not exist` where PostgreSQL answers (§6); `age` is 42883
`unknown function` (`10_definitions:age/*`); a derived interval column is
typed TEXT (`11_relational:neg/*/distinctSum` 42883, §6).

**ADR-0024 §12 item 4's statement about PostgreSQL** is not general: the
INTERSECT representative is the first input's member under HashSetOp, and the
first input is the one with fewer estimated groups (§3); under the sorted
SetOp either arm's member leads. The ADR's factual sentence needs the
architect's correction; this item does not edit it.

## 12. Round 3: sentences whose cited cell did not establish them

Each was re-read against its cell; the corrected text is above.

| where | round 2 said | the cell shows | now |
|---|---|---|---|
| §1, arithmetic row | a near-sentinel finite value negates to a finite value (`sentinel/negOfNearMax`, `sentinel/negOfNearMin`) | `negOfNearMin` is **22008** (its months field is INT32_MIN) | only `negOfNearMax` negates; `negOfNearMin` is 22008 |
| §4b, `age(date, date)` | "resolves to a timestamp overload of midnight values; the measured answers are the same under UTC and New York" (`age/date`, `age/dateNY`) | the two cells use **different date pairs**; the plan (`explain/age/date`, round 3) shows the **timestamptz** overload; the same pair differs between UTC and America/Havana (`age/dateGapUTC` `1 day`, `age/dateGapHavana` `23:00:00`) | the timestamptz overload, alike for the same pairs under UTC and New York (`age/dateUTC`, `age/dateMonthNY`), zone-dependent in general; refused `timestamptz` (ruling 7) |
| §4b, `timestamp - timestamp` | "an infinite operand: `infinity - infinity` 22008", citing `tsdiff/*` | `tsdiff/inf` answers `infinity`, `-infinity`; the 22008 is `tsdiff/infInf` | one infinite operand gives that infinity; two equal infinities 22008, each cell named |
| §2, width | `wrap/join` "(0 rows, …)" | the cell is a count: one row, `0` | a count of 0 matches |
| §2, width | `wrap/hashCollide` "(PostgreSQL's own hashes collide while `=` is false)" | the cell measures only the hashes; `=` is in `cmp/213503982 days 08:01:49.551616~0` | both cells cited |
| §4c | `02_fields:fields/20` | the cell is `fields/20/infinity` | name corrected |
| §4a | `mul/usHalfEven*`; `1 year / 7` uncited | `05_arith:mul/usHalfEven1…3`; `05_arith:div/yearBy7` | cells cited |
| §5, casts | `interval → varchar` beside `interval → text` | `cast/toVarchar` is typed `character varying` (1043) | the type stated (§7) |
| §6 | "Round 1's table stands" (it had been removed) | — | the complete inventory restored and merged |
| §10 | "Result caches: none survives a query"; cleanup "removed … after its TTL"; "a lazy file is never written at all unless a consumer asks"; "an older binary meets the new type byte at chunk decode"; the cluster policy "not stated in the source" | `getFileData`'s LRU has no per-query cleanup; cleanup is periodic and best effort; `Worker.Drain` releases lazy uploads; an empty chunk is never decoded; ADR-0010 states the wholesale-deploy rule | corrected (§10) |
| §9 | the outcome checks reject "a representative that is not SQL-equal" (round 2's whole representative check) | SQL-equality admitted fabricated tuples, PostgreSQL's non-winning member and the wrong direction | the explicit rule-3 answer (§3, §9) |
| §9 | "the bare-result refusal … stage F" | the policy must hold from A | owned by A; F audits |
