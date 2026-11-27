# The first-class INTERVAL as an execution type: definition draft

Design draft for epic #1680, item 1680b, round 2. **This is a draft for the
architect to turn into an ADR. It decides nothing the maintainer has not
ruled.** The maintainer's ruling of 2026-11-26 authorizes an execution-only
first-class interval (separate months, days and microseconds, typed identity
through every execution boundary) and requires its equality, hashing,
ordering, calendar arithmetic, normalization and output behaviour to be
defined against PostgreSQL 17.11 before any of it is implemented. Persistent
storage, format migration and wire-protocol changes are not authorized in
this stage.

Round 2 answers the judge's review of round 1 (verdict NOT READY, eight
required corrections) and is written to the architect's rulings of
2026-11-27: rulings 1 to 9 are applied as decided; rulings 10 (type identity
placement) and 11 (parallel `sum` overflow) are **pending with the
maintainer** and are presented in [§10](#10-pending-with-the-maintainer) with
their options and costs. The definition is written to their recommended
options and says so where it depends on them.

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
| `internal/coordinator/arc_1680b_interval_corpus_r2_cells_test.go` | sections 10 to 12 and the cast-wrapped variants (round 2) |
| `internal/coordinator/arc_1680b_interval_corpus_test.go` | the recorders, the baseline, the well-formedness check, the reference model |
| `internal/coordinator/arc_1680b_interval_manifest_test.go` | the stage manifest and its outcome checks |
| `internal/coordinator/testdata/arc_1680b/<stem>.statements.tsv` | the statements as run: `name`, PostgreSQL settings, kind (`sql`, `ddl` fixture, `write`, `pg-only` plan), SQL |
| `…/<stem>_pg17.tsv` | PostgreSQL's answer: rows under a `column:type(oid)` header, a zero-row answer as `header \| (0 rows)`, `ERR <SQLSTATE>: message`, then any ` !! <severity> <SQLSTATE>: message` notice PostgreSQL raised |
| `…/<stem>_engine.tsv` | this engine's answer on each of six arms (`single`, `spilled512k`, `dag`, `dag-shuffled`, `dag-morsel4`, `dag-fastpath`; `*` when all agree) and a class |
| `…/<stem>_manifest.tsv` | the owning stage, the outcome, and whether today's baseline meets it |
| `internal/server/pgwire/interval_client_evidence_test.go` with `testdata/interval_client_evidence_{pg17,engine}.tsv` | the client transcript (§7) |

Sections: `01_input` 156, `02_fields` 57, `03_order` 51, `04_keys` 97,
`05_arith` 108, `06_calendar` 63, `07_functions` 96, `09_model` 7,
`10_definitions` 177, `11_relational` 163, `12_writes` 19, and the
cast-wrapped variants `01_input_cast` 120, `03_order_cast` 6, `04_keys_cast`
6, `05_arith_cast` 77, `06_calendar_cast` 16, `07_functions_cast` 18,
`10_definitions_cast` 93, `11_relational_cast` 5: **1,335 statements**, of
which 1,252 queries and 17 writes are under test, 34 are fixtures and 32 are
PostgreSQL plan observations.

Tests:

- `TestArc1680bMeasure` (`IV80_PG_DSN=…`) re-measures PostgreSQL. Round 2
  fixed two recorder defects the judge found: pgconn's `ResultReader.Read`
  copies field descriptions inside its row loop only, so a zero-row result
  came back untyped (`iv80Read` keeps them; two round-1 answers,
  `04_keys:setLit/except/*`, now carry `x:interval(1186) | (0 rows)`); and
  notices were dropped (`01_input:prec/iv7` now carries `WARNING 22023:
  INTERVAL(7) precision reduced to maximum allowed, 6`). Every other round-1
  PostgreSQL answer is byte-identical.
- `TestArc1680bRecordEngine` / `TestArc1680bEngineBaseline` record and hold
  the engine's answers. The class keeps apart what a gate must keep apart:
  `agree`, `agree-value`, `differs`, `answers-pg-error` (answered where
  PostgreSQL raised an error), `pg-error` (same SQLSTATE), `refusal` (0A000),
  `wrong-sqlstate`, `error` (a coded non-0A000 error where PostgreSQL
  answered), `store-guard` (the batch store's #361 guard), `internal` (no
  SQLSTATE), `panic`. The engine answer now carries the error's sentence, so
  an intentional refusal is distinguishable from an internal error by what it
  says. The baseline is a record, not a gate: today 75 agree, 3 agree in
  value, 22 differ, 56 give PostgreSQL's error, 417 refuse 0A000, 58 give
  another SQLSTATE than PostgreSQL's error, 638 raise a coded error where
  PostgreSQL answers (285 of them 42601 and 294 of them 22007: the engine's
  literal grammar), and **none** is a store-guard error, an internal error or
  a panic. The 635 round-1 statements answer exactly as recorded at
  `6ef333fb` (checked line by line against the round-1 files).
- `TestArc1680bCorpusWellFormed`: the statements files are the cells as
  written; one typed PostgreSQL answer per statement (a zero-row answer
  without types fails); an engine line per arm; each line's class is the
  classification of its answer.
- `TestArc1680bReferenceModel`: the 128-bit comparison model (§2) against
  PostgreSQL's ORDER BY, GROUP BY (hash and sort plans) and `interval_hash`
  over 108 generated values, now with **every generated id required exactly
  once** in each of `model/order`, `model/groups`, `model/groupsSort` and
  `model/hash`.
- `TestArc1680bStageManifest` and `TestArc1680bOutcomeChecks` (§9).

**What the corpus does and does not show** (the judge's list of unsupported
claims, corrected):

- Not every statement below rests on a live cell. Where a claim rests on
  PostgreSQL's source alone it is labelled [pg-source]; the round-1 wire and
  IntervalStyle claims that rested on deleted probes now rest on the client
  transcript (§7).
- The generated model values (`09_model`) are finite and non-NULL; NULL and
  the infinities are covered by the hand-written cells (`03_order:order/*`,
  `04_keys:*`, `11_relational:pm/*`, `sw/*`), not by the model.
- Normalized storage would not fail every `02_fields` row: rows for zero,
  NULL and already-normalized values do not discriminate. The discriminating
  rows are named in §1.
- `ago` is not accepted on every form (§5).
- The engine arms are named by configuration. A configuration name is not
  proof that a path ran: the spilled arm forces aggregate drains and small
  sort runs, but join spill, window spill, gather replay, skew splitting and
  bloom engagement are not asserted by this corpus. Asserting them is part of
  stage B's and D's gates (§9), not something this corpus can claim.
- PostgreSQL settings are applied on the reference only; the engine runs the
  statement as written. A cell whose meaning depends on a setting is judged by
  the manifest's `door` mark (IntervalStyle) or refused (TimeZone with
  timestamptz).

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
| the signed integer unit path against the signed clock path | `strtoi64` of `-9223372036854775808 microseconds` is INT64_MIN and `AdjustMicroseconds(val, 0, 1)` keeps it: **accepted**. A signed clock `-2562047788:00:54.775808` is `DTK_TZ`: `DecodeTimeForInterval` of the unsigned part overflows (2⁶³), so it is refused 22015 | `10_definitions:in/usMinUnit` = `-2562047788:00:54.775808`; `01_input:lit/77`, `lit/80` refused; `in/usOverUnit`, `in/usUnderUnit` 22015. Round 1's "text cannot spell an INT64_MIN time" was wrong: only the clock syntax cannot. |
| `ago` | negates every field; INT_MIN / INT64_MIN fields cannot be negated: 22015 | `10_definitions:ago/-2147483648 days ago`, `ago/-2147483648 months ago` 22015; `in/usMinUnitAgo` |
| arithmetic (`finite_interval_pl`, `finite_interval_mi`, `interval_um_internal`, `interval_mul`, `interval_div`, `interval_justify_*`) | every field overflow-checked, and a finite result equal to a reserved triple is 22008 `interval out of range` | `05_arith:plus/toInfinityFields`, `10_definitions:sentinel/minusToNegInf`, `sentinel/plusToNegInf`, `sentinel/sumToNegInf` (22008); `05_arith:neg/minTimeBuilt` (negating INT64_MIN time) 22008; negation maps the infinities to each other (`05_arith:neg/inf`, `10_definitions:sentinel/negOfInfinityTriple` = `-infinity`), and a near-sentinel finite value negates to a finite value (`sentinel/negOfNearMax`, `sentinel/negOfNearMin`) |
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
normalized copy beside it. Text decoding follows PostgreSQL's text policy
(the reserved triple spelled as fields IS the infinity); arithmetic and
`make_interval` follow theirs (a finite result equal to a reserved triple is
22008).

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
`interval_hash_extended` hashes them with `hashint8extended` (round 1 named
only `hashint8`). The comment above them says equal-comparing values must hash
equal: the hash is of N, not of the fields.

**Width.** |N| ≤ (2³¹ × 30 + 2³¹) × 8.64 × 10¹⁰ + 2⁶³ ≈ 5.76 × 10²¹, which
needs about 74 signed bits: 64 are not enough, and 128 is the available carrier
(the engine has Int128 for DECIMAL, `internal/engine/batch/decimal.go`), not a
uniquely necessary width. **[measured]** the cells where a 64-bit N is wrong:
`03_order:cmp/106751991 days 04:00:54.775808~0` (N = 2⁶³: `>`, wraps to `<`),
`cmp/213503982 days 08:01:49.551616~0` (N = 2⁶⁴: `<>`, wraps to `=`),
`order/wrap`, `filter/gtWrap`, `04_keys:wrap/group` (2 groups, 1 under a wrap),
`wrap/join` (0 rows, 1 under a wrap), `wrap/hashCollide` (PostgreSQL's own
hashes collide while `=` is false), `09_model:model/order`.

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

**[measured] The model reproduces PostgreSQL** (`TestArc1680bReferenceModel`,
§0): 108 values, 47 groups, 41 of them with members of different fields; the
order is the 128-bit N then id, the groups (hash and sort plans) are equality
of N, PostgreSQL's hash is equal within each group. Negative controls: a month
of 31 days fails the order and both groupings; a wrapping int64 N fails the
order; a missing hash row fails the coverage check.

**[proposed]** Equality, comparison, sort, top-N, min/max, join, grouping,
DISTINCT and set-operation keys use N as a signed 128-bit integer. The hash is
of N only (both halves or the low 64 bits; the engine's hash values need not
equal PostgreSQL's), so SQL-equal implies hash-equal by construction; the
generator of `TestArc1680bReferenceModel` becomes a property test over the
engine's key encoding, extended with NULL and both infinities. A
byte-comparable sort key is N as **16 bytes, big-endian, two's complement with
the sign bit flipped**; the fields ride beside it and are never compared.

## 3. The representative of a group, and the SQL-tie rule

**[measured] PostgreSQL's printed representative depends on the plan and the
input order.**

| statement (same rows; reversed rows) | hash plan | `enable_hashagg = off` | reversed, hash | reversed, sort |
|---|---|---|---|---|
| GROUP BY over `1 mon`, `30 days`, `720:00:00` | `1 mon` | `30 days` | `720:00:00` | `720:00:00` |
| GROUP BY over `1 mon -30 days`, `0`, `00:00:00` | `1 mon -30 days` | `00:00:00` | `00:00:00` | `00:00:00` |
| GROUP BY over `-1 day`, `-24:00:00` | `-1 days` | `-24:00:00` | `-24:00:00` | `-1 days` |

(`04_keys:group/*`, plans in `04_keys:explain/group/*`.) DISTINCT follows these
cases; UNION does not necessarily choose what GROUP BY chooses (reversed sorted
UNION prints `1 day` and `360 days`, `04_keys:union/iv80_kr/sortagg`).
Whole-tuple selection is PostgreSQL's: `SELECT DISTINCT a, b` over (`1 mon`,
`24 hours`) and (`30 days`, `1 day`) keeps one whole row, the first met, under
both plans (`11_relational:tuple/distinct/*`, `tuple/distinctRev/*`,
`tuple/table/*`); the multi-column key with NUMERIC and float ties keeps the
first row's `1 mon`, `1.0`, `0` (`11_relational:mixkey/*`).

**Set operations** [measured and pg-source]. Round 1 corrected ADR-0024 §12
item 4 (PostgreSQL's INTERSECT representative is not always a left-arm
member: under the sorted SetOp either arm's member leads,
`04_keys:adr0024/intersectNumericSort`). Round 2 measures the hash plan with
asymmetric cardinality and finds that **HashSetOp's first input is not the
syntactic left either**: `prepunion.c`, `generate_nonunion_paths`, puts the
input with fewer estimated groups first for INTERSECT ("we prefer to put the
smaller input first in order to minimize the size of the hash table"; EXCEPT
keeps the left first), and the representative comes from the first input.
`11_relational:set/intersect/bigSmall/default` (left five rows, right one row
`1 mon`) prints `1 mon`, a right-arm member absent from the left, with plan
`HashSetOp Intersect -> Append -> "*SELECT* 2" …` (`explain/set/intersect/bigSmall/default`);
`set/intersectAll/dupRight/*` prints the right's `720:00:00` twice. EXCEPT ALL
prints the group's representative for **every** surviving copy, replacing the
text of the left's other members: `set/exceptAll/dupRight1/*` and
`04_keys:setLit/exceptAll/*` answer `1 mon; 1 mon` for a left of `1 mon`,
`1 mon`, `30 days` (and `04_keys:exceptAll/iv80_k/default` likewise). Nothing
here generalizes beyond the measured plans.

**Ordered aggregates and representation-dependent values** [measured]:
`min(v ORDER BY id)` / `max(v ORDER BY id DESC)` answer by the stated order
(`11_relational:ordagg/minMax` = `30 days`, `1 mon`; reversed `ordagg/minMaxRev`
= `1 mon`, `30 days`); unordered `min` / `max` keep the later argument on a tie
(`03_order:order/minTieFwd` = `720:00:00`, `minTieRev` = `1 mon`;
`interval_smaller` / `interval_larger` [pg-source]); `GREATEST` / `LEAST` keep
the first argument (`03_order:order/greatestTie`). `sum(DISTINCT v)` depends on
which member represents a group: `1 mon 1 day` or `31 days` by input order
(`11_relational:distagg/sumAvg/*`, `distagg/sumAvgRev/*`). A representative
changes a later calendar result: `TIMESTAMP '2024-01-31' + v` over `SELECT
DISTINCT v` of `1 mon` and `30 days` is `2024-02-29` or `2024-03-01` by input
order (`11_relational:reparith/*`, `reparithRev/*`).

**The engine's rule (architect's ruling 3; [proposed] details).** INTERVAL
joins ADR-0024 §12 as a type with many representations of one SQL value. Among
the members of one group, the representative is the **minimum under ascending
lexicographic order over (months, days, micros)**: `720:00:00` (0, 0, 2.592 ×
10¹²) beats `30 days` (0, 30, 0) beats `1 mon` (1, 0, 0); `00:00:00` beats
`1 mon -30 days`; `-1 days` (0, −1, 0) beats `-24:00:00` (0, 0, −8.64 × 10¹⁰).
It is applied exactly as ADR-0024 §12 applies its rule: over the **whole key
tuple** with the whole winning tuple kept; eligibility before comparison for
set operations (a left-arm member for INTERSECT / EXCEPT under the engine's
own rule); every surviving copy of an EXCEPT ALL / INTERSECT ALL group prints
the representative; the representative and the multiplicities survive
precombine, clone merging, spill and the final merge together. Selection never
normalizes. It never prints a value absent from the input: the minimum of a
non-empty finite set under a total order is a member, and ADR-0024's
`batch.TestRepresentativeOrderIsATotalOrder` (antisymmetry, totality,
transitivity, every fold order and reduction tree) extends to the triple.

- Unordered `min` / `max` over SQL-tied extremes print the rule-3
  representative (ruling 4); `min(v ORDER BY …)`, `max(v ORDER BY …)` and
  every ordered or ordered-set aggregate over an interval are refused 0A000
  (ruling 4; `11_relational:ordagg/*`, `ov/aggAsc`, `07_functions:agg/modeTie`,
  `agg/stringAgg`).
- `sum(DISTINCT iv)` and `avg(DISTINCT iv)` are refused 0A000; `count(DISTINCT
  iv)` is allowed (ruling 5).
- `GREATEST` / `LEAST` keep PostgreSQL's first-argument rule: the statement
  decides it, so it is not a legal nondeterminism to replace (their manifest
  outcome is `supported`, not `rep-divergence`).
- A value computed from a representative is computed from the rule-3 member:
  `reparith/*` must answer `2024-03-01` (the manifest says so explicitly).

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
| `-i` | `interval_um_internal`: infinities swap; each field negated with overflow check | `05_arith:neg/*` |
| `i * f` (f float8; integer, numeric, real, unknown cast to float8) | `interval_mul`: NaN 22008. Infinite i: f = 0 is 22008, f < 0 negates, else i. Infinite f: `interval_sign(i)` = 0 is 22008, else ±infinity by sign(f) × sign(N). Finite: (1) `month = (int32) trunc(months × f)` and `day = (int32) trunc(days × f)`, **each truncated separately**, 22008 if out of int32; (2) `month_remainder_days = TSROUND((months × f − month) × 30)` where `TSROUND(x) = rint(x × 10⁶) / 10⁶`; (3) `sec_remainder = TSROUND((days × f − day + month_remainder_days − trunc(month_remainder_days)) × 86400)`; (4) if \|sec_remainder\| ≥ 86400, carry `trunc(sec_remainder / 86400)` whole days into `day` and keep the rest; (5) `day += trunc(month_remainder_days)`; (6) `time = rint(time × f + sec_remainder × 10⁶)`, 22008 if out of int64 or a reserved triple | `1 mon -1 day` × 1.5: month 1, day −1, month_remainder_days 15, sec_remainder −43200 → `1 mon 14 days -12:00:00` (`05_arith:mul/mixedSign`, `10_definitions:ar/mulMixed`); `1 mon 1 day` × 0.75 carries a day: `23 days 06:00:00` (`ar/mulCarry`); 1 µs × 1.5 = 2 µs, × 2.5 = 2 µs, × 0.5 = 0 (rint ties to even, `mul/usHalfEven*`) |
| `i / f` | `interval_div`: f = 0 is 22012; NaN 22008; infinite i: infinite f 22008, f < 0 negates; finite i: the same six steps with **`months / f`, `days / f`, `time / f`** — each component divided by f, never multiplied by a computed reciprocal | `10_definitions:ar/divRecipMonths`: `49 mons / 49` = `1 mon`, `49 mons × (1/49)` = `30 days` (49 × (1/49.0) = 0.9999999999999999 truncates to 0 months). The judge's proposed pair (`9007199254740993 microseconds` / 3 against × (1/3), `ar/divRecipJudge`) and the days pair (`ar/divRecipDays`) do **not** discriminate: both forms answer alike. `1 mon -1 day / 2` = `15 days -12:00:00` (`ar/divMixed2`); `1 year / 7` = `1 mon 21 days 10:17:08.5344` |
| `justify_days(i)` | `interval_justify_days`: infinite unchanged; `wholemonth = day / 30` (C truncation) moved into months (overflow 22008); then if months > 0 and days < 0, days += 30, months −= 1, and the mirror | `05_arith:justify/*` |
| `justify_hours(i)` | `interval_justify_hours`: infinite unchanged; `TMODULO(time, wholeday, 86400 × 10⁶)` into days (overflow 22008); then days and time made the same sign | `05_arith:justify/*` |
| `justify_interval(i)` | `interval_justify_interval`, its own algorithm: infinite unchanged; **pre-normalize** days into months when days and time have the same sign (so the later day addition cannot overflow); TMODULO time into days; days / 30 into months (overflow 22008); then month/day sign agreement treating `day = 0` by the time's sign; then day/time sign agreement | `10_definitions:justify/bigDays24h`: `justify_interval` = `5965232 years 4 mons 8 days`, while `justify_days(justify_hours(…))` is 22008 (`/daysOfHours`); `justify/monMinus25h`: `justify_interval` = `28 days 23:00:00`, `justify_hours(justify_days(…))` = `1 mon -1 days -01:00:00`, `justify_days(justify_hours(…))` = `29 days -01:00:00`: neither composition is `justify_interval` |
| `sum` | `do_interval_accum` / `interval_sum`: a field-wise running sum in an `Interval` (int32, int32, int64), each addition checked, so an intermediate overflow is 22008 even when later rows would bring it back; finite and infinite values counted apart (`N` and `pInfcount` / `nInfcount`); both infinity signs 22008; one sign gives that infinity | `05_arith:agg/sumOverflow*`, `agg/sumInf*`; `11_relational:ov/*` (overflow depends on order: `ov/winForward` 22008, `ov/winBackward` answers; `ov/aggUnordered` 22008), `pm/*` (NULL ignored; `pm/bothInf` 22008) |
| `avg` | `interval_avg`: the same accumulator, then `interval_div` by the finite count: `avg(2147483647 days, 2147483647 days)` is 22008 though the mean fits | `05_arith:agg/avg*` |
| sliding `sum` / `avg` | `interval_avg_accum_inv`: field-wise subtraction, infinity counters decremented; a frame holding both infinities is 22008 | `05_arith:agg/avgWindowSliding`; `11_relational:sw/rows2`, `sw/rows3`, `sw/following` (infinities enter and leave), `sw/all` 22008 |

### 4b. Calendar arithmetic and age

| operation | algorithm [pg-source] | cells [measured] |
|---|---|---|
| `timestamp + interval` | `timestamp_pl_interval`: if the interval is `-infinity`: a `+infinity` timestamp is 22008 `timestamp out of range`, else `-infinity`; if `+infinity`: a `-infinity` timestamp is 22008, else `+infinity`; else an infinite timestamp is kept; else **months** first (calendar month, the day clamped to the target month's length), then **days** (calendar days via Julian), then **time** (exact microseconds) | `06_calendar:ts/*`; opposing infinities 22008: `10_definitions:infop/tsInfPlusNegInf`, `infop/tsNegInfPlusInf`, `infop/tsNegInfMinusNegInf`, `infop/dateInfPlusNegInf`; one infinity wins: `infop/tsNegInfMinusInf` = `-infinity`, `infop/tsInfPlusFinite` = `infinity`, `infop/dateFinitePlusInf` = `infinity` |
| `timestamp - interval` | `timestamp_mi_interval`: `timestamp + (-interval)` | `06_calendar:ts/orderMDNeg` |
| `timestamp - timestamp` | `timestamp_mi`: microseconds into time, months 0, days 0, then `interval_justify_hours`: days and time, never months; an infinite operand: `infinity - infinity` 22008 | `06_calendar:tsdiff/*` |
| `timestamp - date`, `date - timestamp` | the date read as a timestamp at midnight; `timestamp_mi` | `10_definitions:types/tsMinusDate` = `1 day 01:00:00`, `types/dateMinusTs`; `11_relational:tsdate/*` over rows |
| `age(timestamp, timestamp)` | `timestamp_age`: infinities first: `age(-inf, -inf)`, `age(inf, inf)` 22008; `-inf` left gives `-infinity`, `+inf` left `infinity`, `-inf` right `infinity`, `+inf` right `-infinity`. Finite: subtract each broken-down field (µs, s, min, h, mday, mon, year); if dt1 < dt2 negate them all; propagate negatives upward (µs into s, s into min, min into h, h into mday); a negative mday borrows **the length of the earlier timestamp's month** (`day_tab` of tm1's month when dt1 < dt2, else tm2's) and decrements mon; negative months borrow 12 from years; then restore the sign if dt1 < dt2; `itm2interval` (22008 on overflow). NULL in, NULL out | `10_definitions:age/borrow` (`2023-03-01` vs `2023-01-31` = `1 mon 1 day`, reversed `-1 mons -1 days`; `2024-03-01` vs `2024-02-15` = `15 days`, reversed `-15 days`; `2024-03-15` vs `2024-01-31` = `1 mon 15 days`), `age/timeBorrow`, `age/null`, `age/infLeft`, `age/infRight`, `age/infOpposed`, `age/infInf` and `age/negInfNegInf` 22008, `age/bc`; `06_calendar:age/*` |
| `age(timestamptz, timestamptz)` | `timestamptz_age`: the same on the fields **in the session TimeZone**, ignoring a difference of offsets | `10_definitions:age/tz/UTC/fallBack` = `1 day 01:00:00` against `age/tz/America/New_York/fallBack` = `1 day`; `06_calendar:tz/*/age` (`1 mon 9 days 23:00:00` under UTC against `1 mon 10 days` under New York) |
| `age(date, date)` | resolves to a timestamp overload of midnight values; the measured answers are the same under UTC and New York | `10_definitions:age/date`, `age/dateNY` |
| `timestamptz ± interval` | the same order in the session TimeZone: a day keeps the wall clock across DST, 24 hours do not; a target time in a DST gap moves forward (`2024-03-09 02:30` + 1 day = `03:30-04`), a target in a fold takes the later offset (`2024-11-02 01:30` + 1 day = `01:30-05`) | `06_calendar:tz/*`; `10_definitions:dst/*`; `date_add` / `date_subtract` with a zone argument and `generate_series` with a zone (`dst/dateAddZone`, `dst/seriesZone`) |
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
days'` is `t`; `'13 mons'` = `34149600` (one year of 365.25 days plus one
month of 30). `date_part('epoch', …)` is the float8 form
(`epoch/yearVs360Float`). The other fields are the broken-down fields of
`interval2itm` (`07_functions:extract/*`, numeric for EXTRACT, float8 for
date_part; `week`, `dow`, `doy`, `isoyear`, `julian`, `timezone` are 0A000 on
PostgreSQL itself). For an infinite interval `NonFiniteIntervalPart` answers
±Infinity for year, day, hour, epoch and similar fields and NULL for month,
minute, second, microsecond (`07_functions:extract/inf`, `02_fields:fields/20`).
A large **finite** epoch: `10_definitions:epoch/bigFinite` =
`5842218453753654.775806` one microsecond below the positive sentinel,
`epoch/bigFiniteNeg` (round 1's `extract/bigEpoch` was the sentinel itself,
not a finite value).

**Ruling 6:** an EXTRACT whose numeric result PostgreSQL gives as ±Infinity is
refused 0A000 (the finite DECIMAL carrier cannot hold it); `date_part`'s
float8 ±Infinity is supported.

### 4d. Functions and frames: implement or refuse

| function | assignment [proposed, within rulings 6 and 7] | definition / cells |
|---|---|---|
| `make_interval`, `justify_*`, `isfinite`, `age(timestamp, timestamp)`, `date_part`, `EXTRACT` (finite results) | implement, stage C | §1, §4a, §4b, §4c |
| `date_bin(stride, timestamp, origin)` | implement, stage C: [pg-source] `timestamp_bin`: an infinite timestamp is kept (`fn/binInfSource`); an infinite origin 22008; an infinite stride 22008 (`fn/binInfStride`); a stride with months 0A000 (`07_functions:bin/month`); a stride ≤ 0 22008 (`bin/zero`, `bin/neg`); else floor((ts − origin) / stride) × stride + origin with the days counted as 24 h (`fn/binDaysStride`, `fn/binBeforeOrigin`); a sub-millisecond result is refused (`bin/us`, E36) | `07_functions:bin/*`, `10_definitions:fn/bin*` |
| `date_trunc(unit, interval)` | refuse 0A000 (`function-date_trunc`), own item | measured `07_functions:trunc/*`, `10_definitions:fn/truncMixedSign` |
| `generate_series(ts, ts, interval)` | refuse 0A000 (`function-generate_series`), own item: each step is applied to the previous value, so a month step drifts at month ends (`fn/seriesTsMonthEnd`: `01-31, 02-29, 03-29, 04-29`), and the timestamptz form drifts across a DST gap (`dst/seriesNoZone`) | `07_functions:series/*` |
| `to_char(interval, fmt)` | refuse 0A000 (`function-to_char`), own item | `07_functions:tochar/*`, `fn/tocharNeg` |
| `OVERLAPS` with an interval end | refuse 0A000 (`function-overlaps`), own item | `07_functions:overlaps/*`, `fn/overlaps*` |
| `width_bucket(interval, …)` | PostgreSQL's own 42883 for the four-argument form; the array form is a container (refused) | `07_functions:widthBucket`, `widthBucketArray` |
| ordered-set and ordered aggregates over an interval | refuse 0A000 (ruling 4) | §3 |
| RANGE frames with an interval offset | refuse 0A000 (`range-offset-frame`) while offset RANGE frames are refused for every type (42601 here today); when implemented: [pg-source] `in_range_timestamp_interval` / `in_range_interval_interval`: a negative offset by `interval_sign` is 22013 (`frame/negOffset`, `frame/negInfOffset`); a NULL offset is 22004 (`frame/nullOffset`); `infinity PRECEDING` includes every earlier row, and against an infinite base the special case answers true rather than computing infinity − infinity (`frame/infOffset`, `frame/infFollowing`); the bound is base ∓ offset by calendar arithmetic, so an offset SQL-equal to zero is not a zero offset: `1 mon -30 days PRECEDING` leaves 2024-03-01's own row outside its frame (`frame/zeroNormalized` row 3 counts 0) | `07_functions:frame/*`, `11_relational:frame/*` |

## 5. Input and output text

**[measured and pg-source] Two input rounding paths:**

- A **unit's fraction** (`1.5 microseconds`, `0.0000015 seconds`) goes through
  `AdjustFractMicroseconds`: truncate, then add one only when the remaining
  fraction is **strictly above** 0.5 (half toward zero).
- A **clock's fraction** (`00:00:00.0000015`) goes through
  `ParseFractionalSecond`: `rint(frac × 10⁶)`, **half to even**.

`10_definitions:frac/1.5us`: clock `00:00:00.000002`, unit `00:00:00.000001`;
`frac/3.5us`: clock 4 µs, unit 3 µs; `frac/2.5us`: both 2 µs (ties to even and
toward zero agree); `frac/clockNearSecond`: clock `00:00:01.5`, unit
`00:00:01.499999`. Round 1's "input rounds half toward zero" held only for the
unit path.

- A **year, decade, century or millennium fraction** becomes whole months by
  `rint` (`AdjustFractYears`): `0.125 years` = `2 mons` (1.5 to even),
  `0.375 years` = `4 mons` (4.5 to even), `-0.125 years` = `-2 mons`
  (`frac/years1`, `frac/years2`). Round 1 cited `1.5 years`, which does not
  discriminate.
- A **month fraction** becomes days at 30 (`AdjustFractDays(frac, 30)`), the day
  fraction then microseconds by the unit path: `0.5 months` = `15 days`,
  `-0.0333333 months` = `-23:59:59.9136`.
- A **week fraction** becomes days first (`AdjustFractDays(frac, 7)`), then
  microseconds: `0.5 weeks` = `3 days 12:00:00` (`01_input:lit/34`), `1.5
  weeks` = `10 days 12:00:00`, `0.1 weeks` = `16:48:00` (`frac/weeks1`).
- A day, hour, minute or second fraction becomes microseconds by the unit
  path.

**`ago`** [measured]: accepted only as the last token of the postgres grammar
(`1 day 02:00:00 ago`, `1-2 ago`, `3 4:05:06 ago`, `-1 day +02:00:00 ago`, `@ 1
mon ago`); **refused 22007** after an ISO 8601 form (`P1D ago`, `P1Y2M ago`),
after `infinity` or `-infinity` (`infinity ago`), twice (`1 day ago ago`) or
alone (`ago`) (`10_definitions:ago/*`). [pg-source] `DecodeInterval`: `AGO`
only at `i == nf - 1`; `RESERV` (infinity) must be the last field; ISO input is
tried only after the postgres grammar fails on the whole string.

**Typmods change interpretation, not only truncation** [measured]: the
qualifier decides which field a bare number or a two-part clock fills
(`DecodeInterval`'s `range` switch): `'1:30'` is `01:30:00` bare and `HOUR TO
MINUTE`, `00:01:30` as `MINUTE TO SECOND`; `'90'` is 90 seconds bare and 90
minutes as `MINUTE`; `'2' DAY TO HOUR` is `02:00:00`; `'1.5' HOUR TO MINUTE`
is `00:01:00` (a minute and a half, then truncated to the minute)
(`10_definitions:typmod/*`, `01_input:std/*`). Then `AdjustIntervalForTypmod`
zeroes the fields right of the last one named and rounds the time to the
precision **half away from zero** (`IntervalOffsets`: `01_input:prec/iv0` =
`00:00:02`, `00:00:03`, `-00:00:02`). A precision above 6 is clamped to 6 with
`WARNING 22023: INTERVAL(7) precision reduced to maximum allowed, 6`
(`typmod/prec7`, `typmod/prec7Literal`, `typmod/prec7Half`, `01_input:prec/iv7`).

**`IntervalStyle` changes input too:** under `sql_standard`, `-1 2:03:04`
applies the leading sign to every field (`02_fields:input/sqlstd/mixed`).

**Casts** [measured], exactly the directions measured: `text → interval`
(`cast/fromText`, `cast/colon`), `interval → text`, `interval → varchar`
(`cast/toText`, `cast/toVarchar`, NULL `cast/toTextNull`), `time → interval`
(`cast/fromTime`), `interval → time` (modulo 24 h, `cast/toTime`); `integer →
interval`, `numeric → interval`, `interval → bigint`, `interval → boolean` are
42846 (`cast/fromInt`, `cast/fromNumeric`, `cast/toBigint`,
`07_functions:operand/castBool`). No other direction is claimed.

**Output** [measured] (`02_fields:print/*`): four styles, field-preserving;
`postgres` prints `1 year 2 mons 3 days 04:05:06.789`, `-1 days +02:03:04.5`.
DateStyle does not change it (`02_fields:print/dateStyleSQL`).

**First stage (ruling 7):** input is PostgreSQL's interval grammar under
`IntervalStyle = postgres` with both rounding paths as above; output is the
`postgres` style only; every typmod-qualified declaration is refused 0A000
(the record does not yet implement `AdjustIntervalForTypmod` and the
qualifier-dependent decoding exactly); `SET IntervalStyle` to anything but
`postgres` is refused at the door (today it is silently accepted and ignored:
§7, §11).

## 6. Boundaries in the engine

Every place a value of the new type must be handled [wadjet-source, at
`6ef333fb`]. Round 1's table stands with the refinements below; the judge's
omissions follow, each re-verified in the source (file and function), with
what goes wrong if it is forgotten [inferred].

**Refinements of round 1's entries.**

- `engine/expr/expr_leaf.go` `ColRef.Eval` already falls back to
  `Vector.GetValue`; no dedicated arm is needed once `boxVectorValue` /
  `GetValue` box the triple.
- `engine/exec/packed_hash.go` `packedKeyWidth` can exclude INTERVAL and leave
  it to the generic key path.
- `engine/batch/pool.go` `BatchPool.Get` / `GetForSize` have no type switch;
  the obligations are allocation, reset, ownership and accounting (below).
- `engine/exec/agg_group_key.go` `genericKeyBoxingDeferrable`: N cannot
  reconstruct the triple, so an interval key must not be "boxing deferrable"
  merely because N is fixed-width: the key carries N **and** the triple.

**Omitted sites** (the judge's finding 4):

| site | what it does today | if forgotten |
|---|---|---|
| `engine/expr/temporal_producer.go` `producesInterval`; `planner/physical/declared_output.go` `nodeIsInterval`; `engine/expr/interval_node.go` (`IsIntervalNode`) | interval recognition by expression **syntax** | a column reference to an interval-typed derived column is not seen as an interval: silent wrong or a guard error once derived columns are admitted |
| `engine/expr/rettype.go` `CommonDeclType`; `planner/physical/declared_output.go` `caseDeclaredType`; `engine/expr/expr_special.go` `(*Case).boxMode` | CASE / COALESCE common-type resolution | a fallback declaration or a wrong coercion: guard error, mistyped NULL (`10_definitions:closure/*`) |
| `engine/expr/interval_constant.go` `ConstantEvalError`; `planner/logical/table_func_args.go` `FoldTableFuncArg`; `engine/expr/outer_literal.go` `unrenderableOuterValue` | constant diagnostics, table-function string folding, correlated typed-NULL stand-ins | the type or the representation erased: wrong declaration, diagnostic or refusal |
| `planner/physical/set_op_arm_decls.go` `setOpArmDecls`; `set_op_result_column.go` `setOpResultColumn`; `set_op_plan.go` `setOpNullLayout` | arm alignment and NULL layout (beside `setOpWiden`) | mistyped vectors, codec refusal, guard error |
| `engine/exec/project.go` `projectCopyColumn`, `projectGatherColumn` | bulk projection and selected-row gathering | NULL loss or guard error; test both the typed fast path and the generic fallback |
| `engine/batch/batch.go` `newVectorFromColumn`, `(*RecordBatch).Reset`, `resetVectorForReuse`; `vector.go` `WriteNullAt`; `bitmap.go` `NewBitmapAllNull` | allocation, pool reuse, typed NULLs, outer padding, empty batches | a wrong declaration hidden while every value is NULL (`11_relational:null/*`, `empty/*`, `oj/*`) |
| `engine/exec/agg_accumulator_merge.go` `mergeFlatAccumRow`; `agg_partial_merge.go` `(*HashAggregate).mergeSinkState` | clone merging of SoA accumulators | wrong merged aggregates; the interval state (triple sum, count, infinity counts), overflow, NULL and representative must merge |
| `engine/exec/aggregate_partial_spill.go` `emitAcc`, `readAcc`, `setPartialKeyFromAny` (its default arm turns an unknown boxed key into a string tag), `(*kWayMerger).Next` | the partial-state codec and the k-way merge | guard error, or a wrong state or representative after spill |
| `worker/shuffle_partial_agg.go` `(*cappedPartialAgg).resolveAgainst`; `shuffle_local_accum.go` `(*partitionedShuffleSink).consumeLocalAccum` | sender precombine and local accumulation | wrong aggregates or lost eligibility despite a correct final merge |
| `engine/exec/sort.go` `(*Sort).compactTopKLocked`, `(*topNHeap).Less`; `late_mat.go` `FlattenForConsumer`; `planner/physical/topn_late_mat.go` | top-N compaction, late-materialized selections | a missing comparator refuses; a bad copy silently corrupts output |
| `engine/exec/sort_merge_join.go` `resolveCompareKernels`, `compareLR`, `keysEqual` | kernel admission and equal-key runs | a missing kernel is a guard error; a field-wise comparator silently misses SQL-equal matches |
| `worker/shuffle_format.go` `(*shuffleWriter).writeHeader`; `wshf/decode.go` `ParseHeader`; `wshf/schema_guard.go` `(*SchemaGuard).check` | the WSHF header's type byte per column | decode or schema refusal, or mistyping; existing type ids must stay as they are (§10) |
| `engine/exec/bloom_filter_op.go` `appendBloomKeyAt`; `dynamic_filter_emit.go` `dynamicFilterIntKey`, `(*DynamicFilterEmitOp).Execute` | bloom keys; dynamic filters decline non-integer keys | a bloom key not built from the canonical join encoding gives **false negatives**; keep the dynamic-filter exclusion unless implemented |
| `coordinator/skew_split.go` `planSkewSplitTasks` | skew sub-tasks split probe files, replicate build files | lost or duplicated matches or broken memory bounds if partitions are not by N |
| `worker/gather_reply_sink.go` `(*gatherReplySink).Consume`; `coordinator/gather_receiver.go` `admitLocked`; `gather_replay.go` `(*gatherReplayStream).Next`; `dag_merge.go` `(*batchRenamer).apply` | gather's WSHF payload, replay, renaming, task / execution / sequence identity | an explicit gather refusal, or duplicate / incomplete results if bypassed |
| `server/pgwire/server.go` `handleDescribe`, `describeSQL`, `TypeMod` | Describe and typmods | false metadata on empty and prepared statements (§7 records two cases today) |
| `internal/oracle/typematrix/typematrix.go` `Columns` | the type matrix's catalog | the oracle stays green because INTERVAL is absent: add an execution-derived interval fixture, not a stored column |

**Memory accounting (ADR-0006).** Single-owner accounting and spillable
breakers apply to every new allocation: the 16-byte triple per row; the
canonical key (N, 16 bytes) and the representative triple carried beside it
in group, distinct, join and set-operation tables; the aggregate state (a
triple sum of 16 bytes, a finite count, two infinity counts); and the codec
scratch of the spill and WSHF writers. Each is charged to the operator that
owns it, released by that owner, and spillable where its breaker spills; a
16-byte vector entry alone does not discharge that obligation.

**Existing state, corrected** (the judge's finding 7) [wadjet-source]:

- **DURATION aliasing.** `storage/parquet/schema.go` `ParseTypeID` still maps
  the spelling `INTERVAL` to `TypeDuration` (the CAST machinery's alias,
  `ResolveColumn`), while `DeclaredColumn` refuses a **new** column spelled
  INTERVAL at any depth (0A000, #1713d) and leaves DURATION columns untouched;
  `parquet.TestDeclaredColumnRefusesANewIntervalColumn` pins both. Adding the
  type must keep execution casts apart from DDL and never reinterpret existing
  DURATION data.
- **Containers are already refused.** The interval rule refuses an interval
  array or row element (`planner/physical/validate_interval_use.go`;
  `04_keys:array/group` 0A000 today), though `declared_output.go`
  `arrayLitDeclaredType` would declare an ARRAY of INTERVAL as DURATION if it
  were reached. The refusal stays for typed column elements, nested casts, MAP
  keys and values and `array_agg`.
- **Parquet and Iceberg guards exist.** `refuseIntervalAnnotation`
  (`storage/parquet/schema_tree.go`) is called at every reader open
  (`file_reader.go`, four sites, and `footer_cache.go`), pinned by
  `parquet.TestReaderRefusesTheIntervalAnnotation`; Iceberg refuses an interval
  field, pinned by `iceberg.TestTableReaderRefusesAnIntervalField`. A test that
  reads a whole encoded Parquet file with an INTERVAL column is still absent.
- **Parameter tests exist.** `pgwire.TestIntervalRuleOverTheWire`
  (`internal/server/pgwire/interval_rule_wire_test.go`) pins text comparison
  and shift, publication refusal, invalid text (22007), NULL, binary refusal
  (0A000) and Describe; `bindparams.go` `renderTextParam` keeps `CAST(… AS
  INTERVAL)` for a text parameter typed 1186 and `renderBinaryParam` refuses a
  binary one. Round 1 said there were none; that was wrong.
- **`timestamp - date`** is refused today with 42883 `operator does not exist`
  by the planner (`10_definitions:types/tsMinusDate`, `11_relational:tsdate/*`);
  PostgreSQL answers an interval. `expr/timestamp_difference_guard.go` also
  covers it at run time. Disposition: refused until stage C implements it as
  `timestamp_mi` over a midnight timestamp; the guard is not removed wholesale
  when `timestamp - timestamp` lands.
- **`lag(NULL, 1, ts)`**: PostgreSQL types the window result by the default,
  so `lag(NULL, 1, ts) OVER (…) - ts` is an interval (`11_relational:lag/*`).
  The interval rule recognizes it (`withTimestampLeaves`); the window output
  must carry the default's type to the subtraction. Disposition: stage E.
- **The opaque `IntervalValue`** (`engine/expr/expr_interval.go`): a runtime
  `CAST(s AS INTERVAL)` of text the single-unit fields cannot hold is boxed
  opaque and refused 0A000 where it is applied, compared or rendered
  (`11_relational:opaque/*`, `rt/*`). Its replacement by the triple migrates
  every producer and consumer together (AST recognition `IsIntervalNode` and
  `producesInterval` are separate mechanisms; both change); unsupported row
  text keeps raising a SQL error. Disposition: stage C.

## 7. What crosses a door

**Ruling 1 (decided): a bare interval result stays refused.** An explicit
scalar `CAST(<interval> AS TEXT)` is the only display path in this stage;
Describe, execution, every door and zero-row results obey the same contract.
**Ruling 2 (decided): parameters are preserved as they are**: text and NULL
interval parameters keep their behaviour and restrictions; binary interval
parameters stay refused; no new parameter capability.

**The client transcript** (`pgwire.TestIntervalClientEvidence`; PostgreSQL's
side recorded with `IV80_PG_DSN`) [measured], PostgreSQL against this engine
today:

| step | PostgreSQL 17.11 | this engine |
|---|---|---|
| Describe of a bare interval result | RowDescription `oid=1186 typmod=-1 len=16` | 0A000 |
| Describe / execute of `CAST(… AS TEXT)` | `oid=25`, rows `1 day; NULL; 30 days` | the same |
| the text cast with a binary result requested | `fmt=1`, the text bytes | the same |
| a bare interval, text or binary format | text rows; binary `0x…0000000100000000` (time int64, day int32, month int32, big-endian) | 0A000 |
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

Three rows are pre-existing defects outside this stage's contract (§11): the
NoData Describe answers (a client told "no result columns" for a statement
that returns one), the untyped parameter's text inference, and the ignored
IntervalStyle. The judge cited pgJDBC 42.7.5's `PgResultSet` source for
`getObject` on a character type calling `getString`; that is the judge's
reading, not re-verified here [unresolved for this record; no JDBC client was
run]. A client or query builder can also adapt explicitly and visibly: cast to
text, or select total months, days and the whole time field as separate
numeric columns (not `extract(month)` alone, which is the month within the
year).

## 8. What stays out

| out of this stage | pinned by |
|---|---|
| a column spelled INTERVAL in DDL, CTAS of an interval, INSERT / UPDATE / MERGE of an interval value, including through derived columns, a subquery, zero rows and `WITH NO DATA` | `parquet.TestDeclaredColumnRefusesANewIntervalColumn`; `coordinator.TestArc1713cIntervalOrderEveryArm` (`ddl/intervalColumn`); `server.TestIntervalRuleOnEveryDoor`; `12_writes:w/*` (all refused 0A000 today, manifest `refusal interval-write` met; `w/ctasText`, a CTAS of a text cast, is `supported` and refused today only because the multi-field text is opaque) |
| Parquet and Iceberg interval columns | `parquet.TestReaderRefusesTheIntervalAnnotation`, `iceberg.TestTableReaderRefusesAnIntervalField` |
| the wire OID 1186 and the binary format | §7; `pgwire.TestIntervalClientEvidence`, `server.TestIntervalRuleOnEveryDoor` |
| new parameter capability (binary interval parameters) | `pgwire.TestIntervalRuleOverTheWire` (`binary` 0A000), `pgwire.TestIntervalClientEvidence` (`param/binary`) |
| IntervalStyle other than postgres, typmods, timestamptz, TIME | the manifest's refusals (§9); the SET refusal is to be added by stage A (today silently ignored, §11) |

## 9. Staging and the stage manifest

**[proposed throughout.]** Every intermediate state answers PostgreSQL's value
or refuses; none answers differently in silence. Each stage owns manifest
cells; a stage that lands adds its letter to `iv80LandedStages` in
`arc_1680b_interval_manifest_test.go`, and from then on every cell it owns must
be met on all six arms.

**The manifest** (`<stem>_manifest.tsv`, `TestArc1680bStageManifest`): every
statement under test has one owning stage and exactly one of four outcomes:
`supported` (value and declared types equal PostgreSQL's), `pg-error
<SQLSTATE>` (PostgreSQL's own error, same SQLSTATE), `refusal <reason>` (0A000,
the reason one of the named list in the test), `rep-divergence` (the same rows
and types, every printed interval SQL-equal to PostgreSQL's; where a value is
computed from the representative, the explicit rule-3 answer). The rules that
derive it are in `iv80ManifestOutcome`: an error PostgreSQL decides while
parsing or typing (42601, 42725, 42804, 42846, 42883) is `pg-error` first; then the named capability refusals
(writes, IntervalStyle, timestamptz, TIME, typmods, containers, PostgreSQL's
internal hash functions, ordered and DISTINCT aggregates, the refused
functions, offset RANGE frames, sub-millisecond timestamps, numeric
infinities); then a bare interval result (`bare-interval-result`, stage F);
then PostgreSQL's other errors; then the representative families; else
`supported`. A capability refusal takes precedence over a PostgreSQL error
that the refused capability would have raised (a RANGE frame's 22013 is
refused 0A000 while offset frames are refused). The original statements and
transcripts stay as recorded; the cast-wrapped variants are separate cells.
The outcome checks (`TestArc1680bOutcomeChecks`) reject a store-guard error, a
panic, an `XX000`, a 42601 or a 22003 where 0A000 or PostgreSQL's SQLSTATE is
required, and a representative that is not SQL-equal.

Today (1,269 statements): stage A owns 242 (234 refusals, 7 of them `door`,
54 met; 1 supported), C 530 (64 supported met, 359 unmet; 54 PostgreSQL
errors met, 53 unmet), D 93 (45 rep-divergence, 48 supported, 3 met), E 91,
F 313 (135 met: refused 0A000 already; 178 unmet: refused today with another
SQLSTATE, mostly the literal grammar's 22007 / 42601).

**The interim validator's assumption.** `validate_interval_use.go` states
that "a column is never [interval-valued]: a block that would publish an
interval is refused, so no derived table, CTE, set-operation arm, VALUES row
or subquery can hand one on — which is why no lineage needs to be carried past
a block's output". Admitting internal interval columns (stage D) breaks that
premise. **What replaces it:** typed lineage on declared columns. Every
column the planner declares (`ColDecls`, the physical plan's output schema,
stage schemas) carries the INTERVAL type id, so an interval column is known by
its declaration, never by the expression that once produced it; the
validator's walk resolves a column reference to its declaration and applies
the operator allowlist to it exactly as to an inline interval expression.

| stage | admits | interim refusal it lifts | invariant test it carries | why no consumer sees an interval column it does not handle |
|---|---|---|---|---|
| **A** declarations and guards | nothing | none | every interval producer and typed slot is recognized (literal, cast, arithmetic, timestamp difference, `timestamp - date`, `lag` with a timestamp default, CASE / COALESCE / NULLIF / GREATEST / LEAST); every named refusal of the manifest is 0A000 with its reason, decided before rewrites and again at run time; `SET IntervalStyle` other than postgres refused; manifest stage A cells met | no interval column exists yet: the interim rule still refuses every block that would publish one |
| **B** carrier and transport | nothing | none | the triple and NULL round-trip through selection, views, `Flatten`, pool reuse, project copy and gather, the columnar run codec (sort runs, CTE spool, window passes, join partitions), the partial-aggregate codec, WSHF and gather replay; an unknown type id fails loudly; memory charged to its owner (ADR-0006); asserted engagement of join spill, window spill, gather replay, skew split and bloom on the carrier tests | the type exists only in the carrier's own tests; the planner never declares it |
| **C** inline expressions | one proven scalar operation at a time over values that never become a column: input grammar, comparison by N, §4a, §4b for `timestamp` / `date` (sub-millisecond results refused), `timestamp - timestamp` and `timestamp - date` as intervals, EXTRACT (finite) / date_part, `justify_*`, `make_interval`, `age(timestamp, timestamp)`, `date_bin`, each displayed through the explicit text cast | the opaque-interval refusals and r29 for each operation it proves; the timestamp-difference producer refusal (r16 / #1713d) operation by operation | every emitted result has the declared type; a timestamp difference never reaches the numeric fallback; manifest stage C cells (including their runtime-column forms `11_relational:rt/*`, `opaque/*`) met | the result of a stage-C operation is consumed inline by an allowlisted parent or by the explicit text cast; a block that would publish it is still refused |
| **D** internal columns and keys | interval columns between operators on an **explicit operator allowlist**: filter, project, sort and top-N keys, derived tables, CTEs, set-operation arms, subqueries, outer-join padding, GROUP BY / DISTINCT / set-operation keys with the rule-3 representative, hash and sort-merge join keys, their spill, the DAG shuffle and partitioner | r28 (interval keys); the internal-publication parts of r20 (derived table, CTE, set-operation arm, subquery column) | **no interval-typed vector reaches an operator outside the allowlist**, checked on the optimized plan, on every stage plan, and at runtime batch entry of each operator; a negative control removes one allowlist check and shows each of the three checks catch it; manifest stage D cells met, `rep-divergence` cells by rule 3 | typed lineage: the validator sees a column by its declared type, so a downstream function, aggregate, container, cast or write that is not on the allowlist refuses it; the root, write and container guards are kept |
| **E** aggregates and windows, one family at a time | `count`, unordered `min` / `max` (rule-3 representative), `sum` / `avg` with PostgreSQL's accumulator, window aggregates with the inverse transition, `lag` / `lead` defaults, window ORDER BY and PARTITION BY over intervals | r30 for exactly the family admitted (never wholesale: `array_agg`, ordered and ordered-set aggregates, DISTINCT `sum` / `avg`, value windows each keep their refusal until admitted) | transition, combine, inverse and final states agree across local, clone, spill, precombine and distributed paths, including NULL, both infinities and overflow; manifest stage E cells met | an aggregate not yet admitted refuses its interval argument by the declared type |
| **F** publication policy | nothing new: continued refusal of a bare interval result (ruling 1) | none | Describe, execution, every door and zero-row results refuse a bare interval result (`pgwire.TestIntervalClientEvidence`); manifest stage F cells met | it is the policy itself |

The order is the judge's safer order: nothing is admitted until the
declarations, guards and the carrier are in place; inline expressions before
columns; columns before aggregates; and no aggregate family before the keys it
groups by hash by N.

## 10. Pending with the maintainer

### Ruling 10: where the type identity lives

| option | what it costs | |
|---|---|---|
| **append one execution-only type id to the shared enum** (`storage/parquet/schema.go` `TypeID`), existing ids and DURATION's meaning unchanged, guarded so it can never be written to Parquet, Iceberg, the catalog or the wire as a column type | every codec that keys on the shared id (vectors, hashing, sort, spill, WSHF, gather) gains one case; the storage writers, the catalog and the doors must each refuse it, and `ParseTypeID("INTERVAL")` must keep answering DURATION for the CAST machinery and DDL refusal | **recommended (architect)** |
| a separate execution type layer | a parallel type identity through every codec: each of the above duplicated, and every boundary between the layers a place to lose the type | |

**Can a file carrying the new type byte outlive the query or be read by
another binary?** [wadjet-source]

- **Spill files** (join partitions, sort runs, CTE spool, window passes,
  aggregate partial state) are written under the task's spill directory and
  read back by the same process during the same query; they do not outlive
  it except as garbage, and no other binary reads them.
- **WSHF shuffle and stage outputs** are written to the object store under
  `queries/<id>/…` (`coordinator/dag_shuffle.go`, `dag_compute.go`,
  `aggregate_shuffle.go`, `build_cache.go`'s pre-scanned build caches) and to
  the worker's `LocalStageCache` per-query directory. They are removed when
  the query ends (`Coordinator.cleanupQuery`, `LocalStageCache.CleanupQuery`
  with a tombstone for late adopters, `broadcastJoinCache.CleanupQuery`,
  `ResultStore.CleanupQuery`); an object a failed cleanup leaves behind is
  removed by `ResultCleaner.CleanStale` after its TTL (one hour by default),
  skipping in-flight queries. Under `--shuffle-durability=lazy` an upload is
  queued until demanded and elided at query end, so a lazy file is never
  written at all unless a consumer asks for it within the query. So a file can
  outlive its query by up to the cleaner's TTL, but no reader opens it after
  the query: every key is under that query's id, and nothing reuses another
  query's stage output.
- **A different binary** can read one within a query when the coordinator and
  workers run different builds (a rolling upgrade). WSHF has **no format
  version** (`wshf.ParseHeader` checks the four-byte magic and reads each
  column's type byte without validating it); an older binary meets the new
  type byte at chunk decode, where `wshf.FixedTypeLen` answers `unsupported
  shuffle type` — loud, but an error with no SQLSTATE, after the header was
  accepted. [proposed] With option 1 the new id must be one no older binary
  assigns (appended), `ParseHeader` should refuse an unknown type id itself,
  and the error should carry a SQLSTATE; a mixed-version cluster is otherwise
  outside any guarantee this record can make [unresolved: whether the cluster
  is required to be single-version is not stated in the source].
- **Result caches**: none survives a query (the ones above are query-scoped).

### Ruling 11: interval `sum` / `avg` overflow under parallel execution

| option | what it costs | |
|---|---|---|
| **keep PostgreSQL's finite-width checked accumulator and refuse on overflow** | which inputs overflow can depend on combination order under parallel and distributed execution: a loud, plan-dependent 22008, never a wrong value; recorded as a class of legal nondeterminism under ADR-0013. [measured] PostgreSQL's own answer already depends on order: `11_relational:ov/winForward` 22008, `ov/winBackward` answers; `ov/aggUnordered` 22008 | **recommended (architect)** |
| a wider accumulator state (e.g. Int128 per field) that never overflows where PostgreSQL would | answers where PostgreSQL refuses: a deliberate divergence from the reference, and a final narrowing step that can still refuse | not taken |
| constrain execution order (no parallel combine for interval `sum`) | loses parallel aggregation for the type; still refuses where PostgreSQL's serial order refuses | |

## 11. Existing temporal wrong answers that meet interval paths (epic #1680)

Ruling 8: these pre-existing wrong answers are not this program's to fix; they
are listed as lines for epic #1680, and **wherever an interval operation would
produce or consume a value through one of these paths, the interval program
refuses the statement** (the manifest's `refusal time-type` and `refusal
timestamptz`, stage A: every statement with a TIME, TIMETZ or TIMESTAMPTZ
operand, and `date_add` / `date_subtract` with a zone). Every answer below is
the same on all six arms.

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
`unknown function` (`10_definitions:age/*`).

**ADR-0024 §12 item 4's statement about PostgreSQL** is not general: the
INTERSECT representative is the first input's member under HashSetOp, and the
first input is the one with fewer estimated groups (§3); under the sorted
SetOp either arm's member leads. The ADR's factual sentence needs the
architect's correction; this item does not edit it.
