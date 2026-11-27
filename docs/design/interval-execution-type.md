# The first-class INTERVAL as an execution type: definition draft

Design draft for epic #1680, item 1680b. **This is a draft for the architect to
turn into an ADR. It does not decide anything.** The maintainer's ruling of
2026-11-26 authorizes an execution-only first-class interval (separate months,
days and microseconds, typed identity through every execution boundary) and
requires its equality, hashing, ordering, calendar arithmetic, normalization
and output behaviour to be defined against PostgreSQL 17.11 before any of it is
implemented. Persistent storage, format migration and wire-protocol changes are
not authorized in this stage.

Every behavioural statement below is backed by a measured cell of the corpus
in `internal/coordinator/testdata/arc_1680b/` (statements, PostgreSQL's answers
with result type OIDs, and this engine's current disposition per arm). A cell is
cited as `section:name`, for example `03_order:order/asc`. Six labels keep the
kinds of evidence apart:

- **[measured]** PostgreSQL 17.11 answered it (postgres:17-alpine, `initdb
  --locale=C --encoding=UTF8`, `--memory=4g`; `SELECT version()` = `PostgreSQL
  17.11 on x86_64-pc-linux-musl, compiled by gcc (Alpine 15.2.0) 15.2.0,
  64-bit`; datcollate and datctype `C`; defaults IntervalStyle `postgres`,
  DateStyle `ISO, MDY`, TimeZone `UTC`; a cell's settings column overrides
  them for that cell only). The environment line heads every `_pg17.tsv`.
- **[pg-source]** read in PostgreSQL's source at tag `REL_17_11`, cited by file
  and function.
- **[wadjet-source]** verified in this engine's source at `6ef333fb`, cited by
  file and function.
- **[inferred]** follows from the above but was not run.
- **[proposed]** a design choice for the architect. Nothing proposed here is
  measured behaviour.
- **[unresolved]** open, with the experiment that would settle it.

## The corpus and its tests

| file | role |
|---|---|
| `internal/coordinator/arc_1680b_interval_corpus_cells_test.go` | the statements, by section (Go, so the generated and bounded values are written once) |
| `internal/coordinator/arc_1680b_interval_corpus_test.go` | the recorders and the tests below |
| `testdata/arc_1680b/<NN>_<section>.statements.tsv` | the statements as run: `name`, PostgreSQL settings, kind (`sql`, `ddl`, `pg-only`), SQL |
| `testdata/arc_1680b/<NN>_<section>_pg17.tsv` | PostgreSQL's answer per statement: rows with `column:type(oid)` headers, or `ERR <SQLSTATE>: message` |
| `testdata/arc_1680b/<NN>_<section>_engine.tsv` | this engine's answer per statement on each of the six arms (`single`, `spilled512k`, `dag`, `dag-shuffled`, `dag-morsel4`, `dag-fastpath`), `*` when all six agree, and a class: `agree`, `agree-value`, `refused`, `differs`, `ok`, `pg-only` |

- `TestArc1680bMeasure` (`IV80_PG_DSN=postgres://…`) re-measures PostgreSQL and
  rewrites the statements and answers files.
- `TestArc1680bRecordEngine` (`IV80_RECORD_ENGINE=1`) rewrites the engine files.
- `TestArc1680bEngineBaseline` holds the engine files: every line must be what
  was recorded. It asserts nothing about agreement with PostgreSQL. The
  implementation items move it cell by cell and re-record it, naming every
  moved line.
- `TestArc1680bCorpusWellFormed` checks that the statements file is the cells
  as written, that every statement has exactly one typed PostgreSQL answer, and
  that every statement has an engine disposition on every arm.
- `TestArc1680bReferenceModel` checks the 128-bit comparison model (§2) against
  PostgreSQL's measured ORDER BY and GROUP BY over 108 generated values.

Sections: `01_input` (156 statements), `02_fields` (57), `03_order` (51),
`04_keys` (97), `05_arith` (108), `06_calendar` (63), `07_functions` (96),
`09_model` (7): 635 in all. Every section covers NULL, zero, negative, mixed-sign,
microsecond and near-bound values. Section 8 of the brief (result types) is
in every answer header: each column carries PostgreSQL's type OID.

Today's engine disposition [measured, base `6ef333fb`, identical on all six
arms for every cell]: 572 of the 635 statements are refused (0A000 202, 42601
162, 22007 156, 42883 23, 22008 14, 42846 5, 22015 5, 42804 2, 22012 2, 42704
1); 19 agree with PostgreSQL; 2 agree in value with a different declared type
(`date - date` is bigint here, integer there); 8 differ (the ADR-0024 cell of
§3 and the cells under [Findings](#findings-outside-this-item)); 16 are DDL
accepted and 18 are PostgreSQL-only plan observations.

## 1. Representation

**[pg-source]** `src/include/datatype/timestamp.h`: `Interval` is `{ TimeOffset
time; int32 day; int32 month; }`, `time` in microseconds (int64). Infinity is a
reserved triple, not a flag: `INTERVAL_NOBEGIN` sets `month = INT32_MIN, day =
INT32_MIN, time = INT64_MIN`; `INTERVAL_NOEND` sets all three to their maximum;
`INTERVAL_IS_NOBEGIN` / `INTERVAL_IS_NOEND` test all three.

**[measured]**

| claim | cells |
|---|---|
| The fields are kept as written, not normalized: `1 mon`, `30 days` and `720:00:00` extract to (0,1,0…), (0,0,30…), hour 720 | `02_fields:fields/0…2`, `02_fields:print/postgres` |
| Months is one field; years are printed from it (`14 months` prints `1 year 2 mons`) | `02_fields:fields/18/14 months`, `02_fields:text/18/14 months` |
| The microseconds field holds any int64 time, unbounded by 24 hours (`2562047788:00:54.775807` is int64 max) | `01_input:lit/75`, `02_fields:fields/15` |
| Each field's bound: months ±(2³¹), days ±(2³¹), time up to 2⁶³−1; one past refuses 22015 (field) or 22008 (year arithmetic in input) | `01_input:lit/63…76`, `01_input:std/yearOver`, `01_input:std/dayOver` |
| The all-maximum triple **is** `infinity`: input of `2147483647 months 2147483647 days 2562047788:00:54.775807` prints `infinity` and equals `INTERVAL 'infinity'` | `01_input:lit/79`, `03_order:cmp/2147483647 months 2147483647 days 2562047788:00:54.775807~infinity` |
| An INT64_MIN time alone is finite (months and days not at their minimum): `-2562047788:00:54.775807 - 00:00:00.000001` answers `-2562047788:00:54.775808`; negating it is 22008 | `05_arith:neg/minTimeBuiltAlone`, `05_arith:neg/minTimeBuilt` |
| The text input cannot spell an INT64_MIN time (22007) | `01_input:lit/77`, `01_input:lit/80` |
| Arithmetic whose result would be the all-maximum triple refuses rather than turning finite into infinity | `05_arith:plus/toInfinityFields` |
| `infinity` extracts NULL for the month, minute, second and microsecond fields and ±Infinity for year, day, hour, epoch | `02_fields:fields/20`, `07_functions:extract/inf` |

**[proposed]** The vector stores the triple exactly as PostgreSQL does: a
fixed-width column of `{Months int32; Days int32; Micros int64}` (16 bytes a
row), with NULL in the vector's existing null bitmap and the two infinities as
PostgreSQL's reserved triples. No flag, no normalized copy beside it. Every
constructor that can produce the all-maximum or all-minimum triple from finite
inputs must refuse 22008 as PostgreSQL does (`interval_mul`,
`finite_interval_pl` / `finite_interval_mi` check `INTERVAL_NOT_FINITE(result)`
[pg-source]).

Cells that fail if it is done otherwise: storing a normalized value fails every
`02_fields:print/*` row and `02_fields:fields/*`; a separate infinity flag
without the reserved-triple equivalence fails `01_input:lit/79`; allowing the
all-maximum triple as a finite result fails `05_arith:plus/toInfinityFields`.

## 2. Equality, ordering and hashing

**[pg-source]** `src/backend/utils/adt/timestamp.c`, `interval_cmp_value`:

```c
days = interval->month * INT64CONST(30);
days += interval->day;
span = int64_to_int128(interval->time);
int128_add_int64_mul_int64(&span, days, USECS_PER_DAY);
```

so the comparison value is `N = (months × 30 + days) × 86 400 000 000 +
micros`, in **128 bits**. `interval_cmp_internal`, and through it `interval_eq`,
`_ne`, `_lt`, `_le`, `_gt`, `_ge`, `interval_cmp`, `interval_smaller` and
`interval_larger`, compare N. Infinity needs no special case: the reserved
triples are the extreme N values. `interval_hash` and
`interval_hash_extended` hash **the low 64 bits of N** with `hashint8`; the
comment says equal-comparing values must hash equal, so the hash is of N, not
of the fields.

**Width.** The largest |N| is (2³¹ × 30 + 2³¹) × 8.64 × 10¹⁰ + 2⁶³ ≈ 5.76 ×
10²¹, past 2⁶³ ≈ 9.22 × 10¹⁸ and below 2⁷³: 64 bits are not enough, 128 are.
**[measured]** The cells where 64-bit arithmetic gives a wrong answer:

| cell | PostgreSQL | a wrapping int64 N |
|---|---|---|
| `03_order:cmp/106751991 days 04:00:54.775808~0` (N = 2⁶³) | `>` | `<` (wraps negative) |
| `03_order:cmp/213503982 days 08:01:49.551616~0` (N = 2⁶⁴) | `<>`, `>` | `=` (wraps to 0) |
| `03_order:cmp/178956970 years 7 mons~178956970 years 6 mons 30 days` | `=` | equal by accident of wrap, but see next row |
| `03_order:cmp/178956970 years 7 mons~-178956970 years -8 mons` | `>` | depends on the wrap |
| `03_order:order/wrap`, `03_order:filter/gtWrap` | ordered by the true N | out of order |
| `04_keys:wrap/group` (`213503982 days 08:01:49.551616` beside `0`) | 2 groups | 1 group |
| `04_keys:wrap/join` | 0 rows | 1 row |
| `04_keys:wrap/hashCollide` | the two values' `interval_hash` collide (t), yet `=` is false | — |
| `09_model:model/order` | the 128-bit order | a different order (`TestArc1680bReferenceModel` asserts the generated set discriminates) |

**Equality and order are not field-wise.** **[measured]** `1 mon = 30 days =
720:00:00`, `1 mon -30 days = 0`, `1 year = 360 days`, `1 day -1 second =
23:59:59`, `-1 mon 1 day = -29 days` (`03_order:cmp/*`); BETWEEN, IN, IS
DISTINCT FROM follow N (`03_order:between`, `03_order:in`, `03_order:cmp/null`);
a filter on `= INTERVAL '30 days'` returns all three spellings
(`03_order:filter/eq`). Printing and arithmetic **are** field-preserving (§4).

**[measured] The model reproduces PostgreSQL.** `TestArc1680bReferenceModel`
generates 108 values built to collide (each base value re-split into months,
days and microseconds several ways, with mixed signs, single microseconds and
values past 2⁶⁴), has PostgreSQL store, order and group them (`09_model:*`),
and checks: the stored fields are the generated ones; PostgreSQL's `ORDER BY
v, id` is exactly the order of the 128-bit N then id; PostgreSQL's GROUP BY
(hash and sort plans, `09_model:model/groups`, `09_model:model/groupsSort`) is
exactly equality of N (47 groups, 41 of them with members of different fields);
PostgreSQL's `interval_hash` is equal within every group (`09_model:model/hash`).
Its negative controls: a month as 31 days fails the order and both groupings; a
wrapping int64 N fails the order.

**[proposed]** Equality, comparison, sort, top-N, min/max, join, grouping,
DISTINCT and set-operation keys use N as a signed 128-bit integer (the engine
already carries Int128 for DECIMAL, `internal/engine/batch/decimal.go`). The
hash is of N (both halves, or the low 64 bits as PostgreSQL; the engine's hash
values never need to equal PostgreSQL's). The invariant **SQL-equal implies
hash-equal** holds by construction because nothing but N enters the hash; the
implementation item keeps `TestArc1680bReferenceModel`'s generator as a property
test over the engine's own key encoding (equal N ⇒ equal encoded key and equal
hash; a field-wise encoding fails on the 41 mixed groups). A sort key that wants
a byte-comparable encoding uses N's two's-complement 128 bits with the sign bit
flipped; the fields are carried beside it, never compared.

## 3. The printed representative of a group

**[measured] PostgreSQL's representative is plan- and order-dependent.** Over
the same rows (`iv80_k`) and the same rows inserted in reverse (`iv80_kr`):

| statement | default plan (HashAggregate) | `enable_hashagg = off` (GroupAggregate over Sort) | reversed input, default |
|---|---|---|---|
| GROUP BY over `1 mon`, `30 days`, `720:00:00` | `1 mon` | `30 days` | `720:00:00` |
| GROUP BY over `1 mon -30 days`, `0`, `00:00:00` | `1 mon -30 days` | `00:00:00` | `00:00:00` |
| GROUP BY over `-1 day`, `-24:00:00` | `-1 days` | `-24:00:00` | `-24:00:00` (sort plan: `-1 days`) |

(`04_keys:group/*`, with the plans in `04_keys:explain/group/*`; DISTINCT and
UNION vary the same way, `04_keys:distinct/*`, `04_keys:union/*`.) The hash
plan keeps the first member it meets; the sort plan keeps whatever its
(unstable) sort leaves first. `min` / `max` keep the **later** argument on a
tie (`interval_smaller` / `interval_larger` return the second argument unless
strictly smaller / larger [pg-source]), so they too depend on input order:
`03_order:order/minTieFwd` prints `720:00:00`, `03_order:order/minTieRev`
prints `1 mon`. `GREATEST` / `LEAST` keep the **first** argument on a tie
(`03_order:order/greatestTie`): that depends on the statement, not the plan.

**Evidence against an existing claim.** ADR-0024 §12 item 4 says PostgreSQL's
INTERSECT / EXCEPT representative is a left-arm member "under both measured
plans". **[measured]** Under the sorted SetOp, PostgreSQL 17.11 sorts the
appended arms on the key alone (`04_keys:explain/adr0024/intersectNumericSort`:
`Sort Key: "*SELECT* 1".k`, no arm flag), and prints right-arm members:
`04_keys:intersect/iv80_kr/sortagg` prints `-1 days`, `1 mon -30 days`, `1
day`, `1 year` (right-arm ids 7, 4, 11, 15), and the NUMERIC cell
`04_keys:adr0024/intersectNumericSort` prints `1; 2; 3; 4.00; 5; 6.00` where
the left arm holds `1.00 … 6.00`. The hash plan prints left members
(`04_keys:adr0024/intersectNumericHash`). Corrected statement: PostgreSQL's
INTERSECT representative is a left member under HashSetOp and either arm's
under the sorted SetOp. The engine's left-arm rule remains a choice of its own
(it answers `1.00 … 6.00` on every arm), not a copy of PostgreSQL; ADR-0024's
sentence about PostgreSQL needs the architect's correction. Nothing in this
item changes it.

**[proposed] Extending ADR-0024 §12 to INTERVAL.** INTERVAL joins the
eligibility table as a type with many representations of one SQL value. Its
order over the members of one group is **lexicographic over (months, days,
micros), coarsest field first, larger first** — the member with the most whole
months, then the most whole days — so `1 mon` beats `30 days` beats
`720:00:00`, and `1 day` beats `24:00:00`. Consequences, stated so they can be
judged: in the zero group, `1 mon -30 days` beats `00:00:00`; in the
`-1 day` / `-24:00:00` group, `-24:00:00` (days 0) beats `-1 days` (days −1).
The opposite direction (smallest first) is equally total and would print
`720:00:00` and `00:00:00`; this draft does not have a measurement that prefers
either, and the architect chooses.

- It never prints a value absent from the input: the representative is the
  minimum of the group's members under a total order, which is one of them
  (the min of a finite non-empty set under a total order is an element of it);
  ADR-0024's `batch.TestRepresentativeOrderIsATotalOrder` (antisymmetry,
  totality, transitivity, every fold order and reduction tree) extends to the
  triple. Two members with the same triple print the same text, so the order
  is total on what can coexist.
- Whole row (item 3) and left-arm eligibility (item 4) apply unchanged.
- **[proposed]** `min` / `max` on a tie print the same representative (ADR-0024
  §12 excluded MIN / MAX of NUMERIC; for INTERVAL the accumulator is new, so it
  can follow the rule from the start). This diverges from PostgreSQL's
  later-argument rule exactly where PostgreSQL's own answer depends on input
  order (`03_order:order/minTie*`, `03_order:order/minTieTable`).
- `GREATEST` / `LEAST` must keep PostgreSQL's first-argument rule: it is
  determined by the statement, so it is not a legal nondeterminism to replace.

**The SQL-tie rule (ADR-0013).** The representative order must never become a
SQL order inside an equal key. **[measured]** PostgreSQL orders SQL-equal
intervals by the next key only: `03_order:order/asc` lists `1 mon` (id 1),
`30 days` (2), `720:00:00` (3) by id; `03_order:order/idDesc` lists them 3, 2,
1; `04_keys:orderTie/*` prints `1 year`, `360 days`, `1 year` (ids 15, 16, 17)
in id order whatever the fields. Window peers are SQL-equal rows:
`04_keys:peers/*` gives every member of a group one rank, one dense rank and
one running count; `04_keys:partition/*` one partition. `FETCH FIRST n ROWS
WITH TIES` takes the whole SQL-equal group (`04_keys:withTiesCount/*` = 1 for
a singleton maximum; `04_keys:withTies/*` returns both members of the minimum
group, in an order PostgreSQL leaves to the scan: `8; 7` forward, `7; 8`
reversed — ADR-0013 legal nondeterminism). A sort that broke interval ties by
the fields would fail `03_order:order/idDesc`, `04_keys:orderTie/*` and
`04_keys:peers/*`.

## 4. Arithmetic and calendar arithmetic

Two separate tables: interval arithmetic keeps the fields and cascades
fractions downward only; calendar arithmetic applies each field in calendar
terms. Neither uses N.

### 4a. Interval arithmetic

| operation | rule | [pg-source] | cells |
|---|---|---|---|
| `i ± j` | field-wise, each field checked for overflow (22008); no carry between fields (`20:00:00 + 20:00:00 = 40:00:00`, `25 days + 10 days = 35 days`) | `finite_interval_pl`, `finite_interval_mi` | `05_arith:plus/*`, `05_arith:minus/*` |
| `-i` | each field negated; INT32_MIN months or days, or INT64_MIN time, is 22008 | `interval_um_internal` | `05_arith:neg/*` |
| `i * f` (f float8; integer, numeric, real, unknown are cast) | months × f truncated to int32; the fraction cascades to days at **30**; days × f plus that cascade truncated; the remaining fraction to seconds at **86 400**; a cascaded ≥ 24 hours is folded back into days; time = `rint(time × f + seconds × 10⁶)` (round half to even) | `interval_mul` (`TSROUND`, `rint`) | `05_arith:mul/*` (`1 mon × 0.5 = 15 days`; `1 mon × 0.333333 = 9 days 23:59:59.136`; `7 mons × 0.3 = 2 mons 3 days`; `1 mon -1 day × 1.5 = 1 mon 14 days -12:00:00`; 1 µs × 1.5 = 2 µs, × 2.5 = 2 µs, × 0.5 = 0) |
| `i / f` | as multiplication by 1/f, the same cascade; f = 0 is 22012 | `interval_div` | `05_arith:div/*` (`1 mon / 3 = 10 days`; `1 year / 7 = 1 mon 21 days 10:17:08.5344`; 1 µs / 2 = 0, 3 µs / 2 = 2 µs) |
| round trips | not exact: `(1 day / 7) × 7 = 24:00:00.000001` | — | `05_arith:div/roundTrip` |
| infinities | ∞ ± finite = ∞; ∞ − ∞ and ∞ + (−∞) are 22008; ∞ × 0, 0 × ∞, NaN factor are 22008; ∞ × negative = −∞; finite × ±Infinity = ±∞ by the sign of N; finite / ±Infinity = 0 | `interval_pl`, `interval_mul`, `interval_div` | `05_arith:plus/inf*`, `05_arith:mul/inf*`, `05_arith:div/inf*` |
| `justify_days` | days ÷ 30 into months; then signs of months and days made to agree | `interval_justify_days` | `05_arith:justify/*` |
| `justify_hours` | time ÷ 24 h into days; then signs of days and time made to agree | `interval_justify_hours` | `05_arith:justify/*` |
| `justify_interval` | both, all three signs agree; overflow 22008 | `interval_justify_interval` | `05_arith:justify/mixed*`, `05_arith:justify/bigDays` |
| `sum` | field-wise running sum in an `Interval` (int32, int32, int64): an intermediate overflow is 22008 even when later rows would bring it back | `do_interval_accum`, `interval_sum` | `05_arith:agg/sumOverflowDays`, `05_arith:agg/sumOverflowThenBack` |
| `avg` | that field-wise sum, then `interval_div` by the count: **`avg(2147483647 days, 2147483647 days)` is 22008** although the mean fits; `avg(1 mon, 0) = 15 days`; `avg(1 µs, 0) = 0` | `interval_avg` | `05_arith:agg/avg*` |
| `sum` / `avg` over infinities | counted apart: one sign gives that infinity, both signs 22008 | `do_interval_accum`, `interval_avg` | `05_arith:agg/sumInf*`, `05_arith:agg/avgInfNegInf` |
| sliding window `avg` | has an inverse transition (field-wise subtraction) | `do_interval_discard` | `05_arith:agg/avgWindowSliding` |

### 4b. Calendar arithmetic

| operation | rule | [pg-source] | cells |
|---|---|---|---|
| `timestamp + interval` | **months first** (calendar month, day clamped to the month's last day), **then days** (calendar days, via Julian), **then time** (exact microseconds) | `timestamp_pl_interval` | `06_calendar:ts/orderMD` (`2024-01-30 + 1 mon 2 days = 2024-03-02`), `06_calendar:ts/monthClamp*`, `06_calendar:ts/mixedSign*`, `06_calendar:ts/orderDT` |
| `timestamp - interval` | `timestamp + (-interval)` | `timestamp_mi_interval` | `06_calendar:ts/orderMDNeg`, `06_calendar:ts/monthBack` |
| `date ± interval` | the date at midnight, as timestamp; the result type is timestamp | (implicit cast) | `06_calendar:date/plus` |
| `date ± integer`, `date - date` | integer days; `date - date` is integer (OID 23) | — | `06_calendar:date/plusInt`, `06_calendar:date/minusDate` |
| `timestamptz ± interval` | the same order in the session TimeZone: `1 day` keeps the wall-clock time across DST, `24 hours` does not | `timestamptz_pl_interval_internal` | `06_calendar:tz/America/New_York/day` (`12:00-04` against `13:00-04`), `06_calendar:tz/*/fallBack`, `06_calendar:tz/*/month` |
| `time ± interval` | the time part modulo 24 h; months and days ignored | `time_pl_interval` | `06_calendar:time/plus` |
| `timestamp - timestamp` | microsecond difference, months 0, then `justify_hours`: **days and time, never months** (`2024-03-01 - 2024-01-01 = 60 days`) | `timestamp_mi` | `06_calendar:tsdiff/*` |
| `timestamptz - timestamptz` | the same, on the absolute instants: `1 day 11:00:00` in both zones | `timestamp_mi` (timestamptz alias) | `06_calendar:tz/*/minus` |
| `age(a, b)` | years and months by calendar fields | `timestamp_age` | `06_calendar:age/*` |
| round trips that do not hold | `(2024-01-31 + 1 mon) - 1 mon = 2024-01-29`; `ts + 1 mon + 1 mon ≠ ts + 2 mons` (`03-29` against `03-31`); `1 mon = 30 days` yet `ts + 1 mon ≠ ts + 30 days` | — | `06_calendar:trip/*` |
| overflow | `timestamp out of range` 22008 | — | `06_calendar:ts/overflow` |
| infinities | `timestamp + ±∞ interval = ±∞`; `infinity - infinity` 22008 | `timestamp_pl_interval` | `06_calendar:ts/inf*` |

**This engine's TIMESTAMP is a millisecond carrier** ([wadjet-source]:
`docs/adr/0012-divergences/temporal.md` E36, `parquet.ParseTimestampMillis`).
An interval's microseconds field applied to a timestamp yields sub-millisecond
digits whenever `micros mod 1000 ≠ 0`. **[proposed]** The execution-only stage
refuses (0A000) a timestamp ± interval whose result would carry sub-millisecond
digits, until TIMESTAMP is microsecond; `06_calendar:ts/us` and
`06_calendar:rows/plus` row 4 are the cells. This engine has no timestamptz
(E36, the offset is discarded), so the timestamptz rows stay the documented gap.

## 5. Input and output text

**Input [measured]** (`01_input`): PostgreSQL reads one field and several
(`1 day 2 hours`), the SQL-standard forms (`1-2`, `3 4:05:06`, `1-2 3 4:05:06`,
`4:05:06`, `4:05`), ISO 8601 designator and alternative forms (`P1Y2M3DT4H5M6S`,
`P0001-02-03T04:05:06`, `PT36H`, `P1W`, `P-1DT-1H`, but not `-P1D`), the
postgres_verbose spellings (`@ … ago`, `mins`, `secs`), `ago` on any form,
mixed per-field signs, a bare number as seconds (`100` is `00:01:40`),
`infinity` / `-infinity` / `+infinity` in any case, and refuses empty text,
unknown units, repeated units and trailing fields with 22007. Fractions cascade
down only and by different rounding per path ([pg-source] `datetime.c`):

- a year fraction becomes whole months by `rint` (`AdjustFractYears`):
  `1.5 years = 1 year 6 mons`;
- a month fraction becomes days at 30, then microseconds (`AdjustFractDays`):
  `1.5 months = 1 mon 15 days`, `1.0333333 months = 1 mon 23:59:59.9136`;
- a day or week fraction becomes microseconds;
- a sub-microsecond fraction is rounded **half toward zero** (`frac > 0.5`
  strictly, `AdjustFractMicroseconds`): `1.5 microseconds = 1 µs`,
  `0.0000005 seconds = 0`.

This is three rounding rules in one type: input rounds half toward zero,
`interval_mul` / `interval_div` round half to even (§4a), and a precision typmod
rounds half away from zero (`AdjustIntervalForTypmod` adds `IntervalOffsets`:
`01_input:prec/iv0` gives 1.5 s → 2, 2.5 s → 3, −1.5 s → −2).

Typmods [measured]: a field restriction truncates the lower fields after
reading (`INTERVAL '1 day 2 hours 3 minutes' DAY = 1 day`, `'1 year 2 months'
YEAR = 1 year`, `'1.5' YEAR = 1 year`); a precision rounds the time; precision
above 6 is reduced to 6 (`01_input:prec/iv7`). `make_interval` builds the
triple from integers and a float8 `secs` (`01_input:make/*`). Casts: text ↔
interval, time → interval and interval → time (modulo 24 h); no cast to or from
integer, numeric, bigint or boolean (42846) (`01_input:cast/*`).

`IntervalStyle` changes input too: under `sql_standard`, `-1 2:03:04` applies
the leading sign to every field (`02_fields:input/sqlstd/mixed`: day −1, hour
−2) where `postgres` reads hour +2 (`02_fields:input/postgres/mixed`).

**Output [measured]** (`02_fields:print/*`): four styles, all
field-preserving: `postgres` (`1 year 2 mons 3 days 04:05:06.789`, `-1 days
+02:03:04.5`), `postgres_verbose` (`@ 1 year 2 mons -3 days 4 hours 5 mins 6
secs ago`), `sql_standard` (`+1-2 +3 +4:05:06.789`, `0-1`), `iso_8601`
(`P1Y2M3DT4H5M6.789S`, `PT0S`). DateStyle does not change the postgres style's
output (`02_fields:print/dateStyleSQL`). [pg-source] `EncodeInterval`.

**[proposed] First stage.**

- Input: PostgreSQL's whole interval grammar under `IntervalStyle = postgres`
  (`DecodeInterval` and `DecodeISO8601Interval` are self-contained; the 94
  `01_input:lit/*` cells and the `std/*` cells are its gate), with the three
  rounding rules exactly as above.
- Typmods (`INTERVAL(p)`, `INTERVAL DAY`, `… TO SECOND(p)`): refused 0A000 in
  the first stage. They affect values (truncation, rounding) and, at a door, the
  column's declared typmod; implementing them is bounded (`AdjustIntervalForTypmod`)
  and is a later item.
- Output: the `postgres` style only. `SET IntervalStyle` to anything else must
  be refused at the door until the other styles are implemented: today it is
  silently accepted (see [Findings](#findings-outside-this-item)).
- `make_interval`, `justify_*`, `isfinite`: implemented with the arithmetic.

## 6. Boundaries in the engine

Every place a value of the new type must be handled, enumerated from the
source at `6ef333fb` [wadjet-source]. The list follows where DURATION (the one
existing int64 temporal type) and DECIMAL (the one existing 16-byte type) are
handled, plus every interval-specific site. "Stays refused" is the
execution-only stage's position. Each line is a unit of an implementation item.

**The type and the vector**

| site | today | must change | stays refused |
|---|---|---|---|
| `storage/parquet/schema.go`: `TypeID` enum, `TypeID.String`, `ParseTypeID` | no INTERVAL type id; DDL `INTERVAL` refused by the interval rule (`ddl/intervalColumn`, #1713d) | a type id the execution layer uses (the enum lives in the storage package; an alternative is an execution-only id outside it — architect's call) | `ParseTypeID("INTERVAL")` for DDL; every parquet writer and reader (`file_writer.go`, `writer.go`, `reader.go`) |
| `engine/batch/vector.go`: `Vector` fields, `NewVectorWithScale`, `EnsureLen`, `ResetForWrite`, `GetValue`, `SetValue`, `CopyValueFrom`, `AppendFrom`, `growForAppend`, `memBytes`, `IntStorageType` | no storage; `SetValue` refuses an `IntervalBox` into anything but DURATION | a 16-byte column (§1) and each accessor | — |
| `engine/batch/view.go`: `Flatten`; `batch/pool.go`; selection and copy (`exec/sort.go`: `copyVectorValue`, `gatherVector`, `gatherSortVector`; `exec/join.go`: `gatherBuildVector`, `gatherCrossBuildVector`; `exec/window.go`: `windowCopyVectorRange`, `windowGatherVector`; `worker/partitioned_shuffle_sink.go`: `appendBatchRowsBulk`, `growBatchTo`) | type switches without INTERVAL | a case each | — |
| `engine/batch/type_mismatch.go`: `IntervalBox`, `DurationNanoser` | the guard that keeps an interval out of every vector | relaxed for the INTERVAL vector only | kept for every other type |
| `engine/batch/pgtext.go`: `FormatPGText` | no interval | postgres-style text (`EncodeInterval`) | other styles |

**Expressions**

| site | today | must change | stays refused |
|---|---|---|---|
| `engine/expr/expr_interval.go`: `IntervalValue` (years … whole seconds, opaque `text`), `compareIntervals`, `intervalOrder`, `cmpSeconds`, `cmpSecondsBig`, `addInterval`, `castToIntervalText`, `pgIntervalInput`, `CheckIntervalLiteralText`, `DurationNanos`, `raiseIntervalNot*` | single-unit, whole seconds; multi-field input is opaque and refused when applied; comparison by N in seconds | replaced by the triple; N in 128 bits | — |
| `engine/expr/compile.go`: `intervalValueOf`, `isIntervalLit`, `compileBinOp` | literal of one unit; sub-second refused 0A000 | the full grammar (§5) | typmods |
| `engine/expr/interval_arith.go`: `intervalArith`, `intervalAdd`, `intervalNeg`, `intervalScale`, `intervalOfParts`, `pgParts`, `IsIntervalInputText`, `isIntervalBox` | arithmetic in whole seconds; a sub-second or infinite result refused (r29) | §4a exactly, with the cascade and the three rounding rules | — |
| `engine/expr/interval_part.go`: `intervalExtract`, `intervalPart`; `expr_cast.go`: `castToInterval`, `castStringRender`; `cast_container.go`: `leafTypeName`; `cmp_container.go`: `boxShape`; `expr_helpers.go`: `compare` | date_part float8; EXTRACT refused (numeric there) | EXTRACT as numeric (OID 1700) per field, date_part float8 (`07_functions:extract/*`) | — |
| `engine/expr/expr_session_fns.go`: `dateShift`, `intervalShift`; `time_bucket.go`: `fnTimeBucket`, `timeBucketStrideMillis`; `expr_math_fns.go`: `fnMod` | date_add / time_bucket over the single-unit box | §4b (months, days, time order) | sub-millisecond results (§4b) |
| `engine/expr/timestamp_difference_guard.go` and the timestamp difference (temporal r16: a double of milliseconds) | refused as an interval producer (#1713d) | `timestamp - timestamp` produces the interval (`timestamp_mi`: microseconds then `justify_hours`) | — |
| `engine/exec/kernel/compare.go`: `ResolveFilterKernel`, `ResolveColColFilterKernel`, `ResolveInFilterKernelArity` | no INTERVAL kernel | comparison by N | — |
| `engine/expr/expr_leaf.go`: `ColRef.Eval`, `boxVectorValue` | no INTERVAL column | box the triple | — |

**Keys, hashing, sort**

| site | today | must change |
|---|---|---|
| `exec/agg_key_encoding.go`: `appendColumnValue`; `exec/agg_group_key.go`: `buildKeySerCols`, `decodeSerializedKey`, `decodeSerializedKeyIntoColumns`, `genericKeyBoxingDeferrable` | no INTERVAL | the key is N; the fields ride as the representative (§3) |
| `exec/packed_hash.go`: `packedKeyWidth`; `exec/partitioned_agg.go`: `PartitionSelectors`, `buildRoutePlan`, `legacyCompositeHash`; `exec/distinct_set.go`: `newDistinctSetFor` | no INTERVAL | hash of N |
| `exec/join.go`: `isIntKeyColumn`, `intKeyFromVector`, `consolidateBuild`, `updateKeyMinMax`; `exec/join_key_width.go`: `keyEncodingClass`, `widenKeyFloat64` | no INTERVAL | join key of N; no float widening |
| `engine/batch/representative.go`: `RepCompareBox`, `RepEligibleType` (ADR-0024 §12) | INTERVAL listed as "not here" | INTERVAL eligible with the §3 order |
| `exec/kernel/sort.go`: `ResolveSortCompare`, `ResolveSortCompareNoNulls`, `ResolveSortCompareNullsLast`; `exec/sort_external.go`: `resolveSortCompareForKey`, `sortEntriesLessFunc`; `exec/window.go`: `compareVectorValues`; `coordinator/coordinator.go`: `compareBatchRows` | no INTERVAL | compare N only (§3's SQL-tie rule) |
| `planner/physical/set_op_types.go`: `setOpTypeCategory`, `setOpWiden` | no INTERVAL | its own category; no widening with other types |

**Aggregates and windows**

| site | today | must change |
|---|---|---|
| `exec/kernel/agg.go`: `ResolveBatchMin/Max/Sum`, `ResolveRowMin/Max/Sum*`; `exec/agg_accumulators.go`: `initFlatAccums`, `isFlatSumType`; `exec/agg_scatter.go`: `scatterFlatAggUpdate*` | refused at plan time (r30, the interval rule in `planner/physical/validate_interval_use.go`) | min / max by N with the §3 representative on a tie; sum / avg with PostgreSQL's field-wise accumulator and its overflow (§4a) |
| `worker/avg_fold.go`: `writeAvgColumn`; `coordinator/coordinator.go`: `reAggregatePartials`; the DAG partial / final merge | no interval partials | the partial is (sum triple, count, +∞ count, −∞ count); the final applies `interval_div` once |
| `exec/window.go`: `WindowMinMaxType`, `resolveWindowNumeric`; `exec/window_external.go` | refused | window aggregates, the sliding inverse (`do_interval_discard`), RANGE offset frames over timestamp (`07_functions:frame/*`; offset RANGE frames are 42601 here today for every type) |

**Spill and transport**

| site | today | must change |
|---|---|---|
| `exec/join_spill.go`: `writeColumnData`, `readColumnData`, `newSpillBatchWriter` — the columnar run codec under sort runs (`sort_external.go`: `writeSortedRun`), the CTE spool (`shared_spool.go`: `drainLocked`, `readRunLocked`), window passes (`window_external.go`: `windowDiskPass`) and join partitions | no INTERVAL | 16 bytes a row |
| `exec/aggregate_partial_spill.go`: `mapBatchTypeToParquet`, `writePartialKeyToColumn`, `writeIntKeyToColumn`; `exec/agg_spill.go` | no INTERVAL | key, representative and accumulator state |
| `engine/batch/container_codec.go`: `encodeVectorBody`, `decodeVectorBody` | an ARRAY of INTERVAL is a DURATION array (ADR-0045 §1, `physical.arrayLitDeclaredType`) | see containers |
| `worker/shuffle_format.go`: `writeColumnData`; `wshf/wshf.go`: `FixedTypeLen`; `wshf/decode.go`: `ReadColumn`; `worker/partitioned_shuffle_sink.go`: `hashRowsIntoPartitions`, `hashVectorValue` | no INTERVAL | the `.wshf` column (16 bytes) and the partition hash of N, so SQL-equal intervals land in one partition |
| `exec/batch_collector.go`; the coordinator's gather (`coordinator.go`: `copyVectorValue`, `extractValue`, `setValueFromAny`) | no INTERVAL | a case each |

**Doors, writes, containers, planner**

| site | today | stays refused in this stage |
|---|---|---|
| `server/pgwire/server.go`: `sendTypedRowDescription`, `pgColumnOID`, `pgTypeOID`, `pgTypeSize`, `sendColumnTypes`, `sendDataRow`, `appendBinaryCell`; `server/server.go` (HTTP), `server/grpc.go`: `anyToProtoValue`; `server/mcp/server.go`; `wadjet/` `QueryResult` / `ColumnMeta` | a bare interval result is refused 0A000 by the interval rule (r20) | see §7 |
| `server/pgwire/bindparams.go` (`oidInterval`, `renderTextParam`, `renderBinaryParam`), `paramtypes.go` | an interval parameter is rendered as text into the statement | interval parameters (§8) |
| `server/pgwire/server.go`: `handleSet`, `showDefaults` | `SET IntervalStyle` stored and ignored | non-`postgres` IntervalStyle |
| `wadjet/dml.go` over `physical.RefuseIntervalWrite` / `IsIntervalValuedWith`, `assignEvaluatedValue`, `assignSourceOf`, `dmlSourceDeclaredType`; CTAS | an interval value written is refused | storing an interval anywhere: INSERT, UPDATE, MERGE, CTAS, COPY |
| containers: `physical.arrayLitDeclaredType` (ARRAY of INTERVAL declares DURATION), `batch/vector.go` `SetValue`'s `DurationNanoser` arm, ROW and MAP elements | an interval element is a DURATION (normalized nanoseconds: fields lost) | **[proposed]** an interval element is refused 0A000 once the type exists, rather than collapsed into a DURATION, until a container item carries the triple |
| DURATION (`TypeDuration`, `parquet.ParseDurationNanos`) | a separate nanosecond type | unchanged; no implicit conversion between DURATION and INTERVAL |
| `planner/physical/validate_interval_use.go` (the interval rule) and `declared_output.go` | the A+ interim: every use outside the allowlist refused | relaxed stage by stage (§9) |

## 7. What crosses a door

No wire change is authorized. **[measured]** What PostgreSQL sends for an
interval result (extended and simple protocol, pgconn): RowDescription OID
1186, typlen 16, typmod −1 for a bare interval, typmod `2147418115` for
`INTERVAL(3)` and `589823` for `INTERVAL DAY`; the text format is the
IntervalStyle output; the binary format is 16 bytes, time (int64) then day
(int32) then month (int32), big-endian. `CAST(… AS TEXT)` is OID 25.
`IntervalStyle` is a reported parameter (ParameterStatus `postgres`), which
this engine also sends (`server.go`).

**[wadjet-source / measured]** Today a bare interval result is refused 0A000 on
every door, and `CAST(<interval> AS TEXT)` answers OID 25 in the postgres style.

The options for the execution-only stage:

1. **Keep refusing a bare interval result** until the wire stage; the explicit
   `CAST(… AS TEXT)` stays the display path. psql and any extended-protocol
   client see either PostgreSQL's text under OID 25 or a clear 0A000. No
   client is told a type it does not get.
2. **Publish a bare interval as text, OID 25**, in the postgres style. psql
   prints the same characters as PostgreSQL. A JDBC-style client keyed on the
   OID gets a `String` where PostgreSQL gives an interval object; a client that
   reads the declared type to build a table (DataGrip, a CTAS through a client)
   records text. This is the "declaration invented here" that
   `pgArrayColumnOID`'s comment and #992 rejected for arrays.
3. **Publish OID 1186 with the text format only**: a wire change, not
   authorized here.

**[proposed recommendation, not a decision]** Option 1. It keeps every door
truthful and needs no compatibility contract; option 2's mislabelled OID is
exactly what #992 removed for arrays. This is the architect's and the
maintainer's call. **[unresolved]** What pgJDBC's `getObject` does with an OID
25 interval text, and whether its `PGInterval` parser reads every postgres-style
output, was not measured (no JDBC client was run); the experiment is a pgJDBC
42.x client against PostgreSQL and against option 2.

## 8. What stays out

Each fail-loud, with what pins it today:

| out of this stage | pinned by |
|---|---|
| persistent INTERVAL storage: a column spelled INTERVAL in CREATE TABLE / ALTER, CTAS of an interval, INSERT / UPDATE / MERGE / COPY of an interval value | `coordinator.TestArc1713cIntervalOrderEveryArm` (`ddl/intervalColumn` 0A000), `server.TestIntervalRuleOnEveryDoor`, and the write doors' `physical.RefuseIntervalWrite` / `RefuseIntervalWriteWith` |
| Parquet and Iceberg interval columns (read or write) | no reader maps an interval logical type today [inferred from `storage/parquet/schema.go`: no INTERVAL TypeID]; **[unresolved]** a test that reads a Parquet file carrying an INTERVAL / fixed_len_byte_array(12) interval column and asserts a refusal does not exist yet; the implementation item that adds the TypeID must add it |
| the wire OID 1186 and the binary format | §7; `server.TestIntervalRuleOnEveryDoor` (bare result 0A000 on every door) |
| interval parameters (`$1` declared interval) | `server/pgwire/bindparams.go` renders them as text; **[unresolved]** no test pins a refusal or an answer for a bound interval parameter; to be added with the stage that changes the literal grammar |
| IntervalStyle other than postgres, typmods | §5 (refusals to be added by the first expression item) |

## 9. Staging proposal

**[proposed throughout.]** Every intermediate state answers PostgreSQL's value
or refuses; none answers silently differently. Each stage's gate is a set of
corpus cells that must move from their recorded disposition to `agree` on all
six arms (and no other cell may move, `TestArc1680bEngineBaseline`).

| stage | contents | gate (cells that must agree) | interim refusal it lifts |
|---|---|---|---|
| S1 the value | the triple and its vector (§1, §6 "type and vector"), the columnar run codec and `.wshf` column, the batch tests; no SQL behaviour changes | batch round-trip and copy tests; `TestArc1680bReferenceModel` re-pointed at the engine's key encoding | none (no visible change: the baseline must not move) |
| S2 expressions | the full input grammar, postgres output, comparison by 128-bit N, §4a arithmetic with its three rounding rules, §4b timestamp / date arithmetic (sub-millisecond results refused), `timestamp - timestamp` as an interval, EXTRACT numeric / date_part, justify_*, make_interval; results still published only through `CAST(… AS TEXT)` | `01_input:lit/*` and `std/*` through a text cast; `02_fields:text/*`; `03_order:cmp/*`, `between`, `in`; `05_arith:plus/*`, `minus/*`, `neg/*`, `mul/*`, `div/*`, `justify/*` through a text cast; `06_calendar:ts/*`, `date/*`, `tsdiff/*`, `trip/*`; `07_functions:extract/*` | the opaque-interval refusals (multi-field input), r29 (sub-second and infinite results), the timestamp-difference producer refusal (r16 / #1713d round 6), `box/subsecond` |
| S3 keys | an interval as a column BETWEEN operators (never a result): filter, project, sort and top-N keys, PARTITION BY and window ORDER BY, GROUP BY / DISTINCT / set-operation keys with the §3 representative, hash and sort-merge join keys, spill of each, the DAG shuffle and partitioner | `03_order:order/ascNullsFirst`, `descNullsLast`, `topN`, `wrap`, `filter/*`; `04_keys:groupCount/*`, `distinctCount/*`, `countDistinct/*`, `partition/*`, `peers/*`, `withTiesCount/*`, `join/*`, `wrap/group`, `wrap/join`, `row/group`; `09_model:model/order`, `model/groups` | r28 (interval keys), the derived-table / CTE / set-operation-arm parts of r20 (internal publication), the subquery-comparison refusal |
| S4 aggregates | min / max (representative on a tie), sum / avg with the field-wise accumulator, the DAG partial / final merge, window aggregates with the sliding inverse | `05_arith:agg/*`, `07_functions:agg/all` (through a text cast) | r30 |
| S5 the door | §7's decision; `SET IntervalStyle` other than postgres refused | `02_fields:print/postgres`, `02_fields:print/text/postgres` (under option 2, or their text-cast form under option 1) | r20's result-column part (option 2 only) |
| later, separate contract | typmods, other IntervalStyles, containers of interval, timestamptz, microsecond TIMESTAMP, storage, wire OID 1186, parameters | — | — |

The order matters for one reason: S3 before S4 keeps every aggregate refused
until the keys it groups by hash by N, so no intermediate state can group
SQL-equal intervals apart.

## Findings outside this item

Observed while recording the engine's disposition at `6ef333fb` (this item
changes no engine code, so base and tip answer alike). Each is pre-existing.

- **`SET IntervalStyle` is silently ignored.** Over pgwire (embedded server),
  after `SET IntervalStyle = 'iso_8601'`, `SHOW IntervalStyle` answers
  `iso_8601` and `SELECT CAST(INTERVAL '1 day' AS TEXT)` answers `1 day`;
  PostgreSQL answers `P1D` (`02_fields:print/text/iso_8601`), and `sql_standard`
  likewise. [wadjet-source] `pgConn.handleSet` stores any variable; nothing
  reads `intervalstyle`. Measured with a throwaway pgwire test, not kept.
- **`TIME` literals answer silently.** ADR-0012 temporal r23 says there is no
  TIME type, yet `TIME '01:00:00' - TIME '23:00:00'` answers double precision
  `-22` where PostgreSQL answers interval `-22:00:00` (`06_calendar:time/minusTime`),
  and `INTERVAL '1 hour' + TIME '23:30:00'` answers text NULL where PostgreSQL
  answers time `00:30:00` (`07_functions:operand/plusTime`).
- **timestamptz literals** answer as timestamp with the offset discarded
  (`06_calendar:tz/*/day`, `tz/*/fallBack`, `07_functions:operand/plusTimestamptz`):
  the documented E36 gap, recorded here because the interval's calendar rules
  meet it.
- **ADR-0024 §12 item 4's statement about PostgreSQL** is not general (§3).
