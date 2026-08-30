# TPC-H on exact decimals: the first measurement

2026-08-29 · ADR-0024 phase 3 · `benchmarks/tpch/decimal_perf_test.go`

ADR-0024 settled that wadjet's DECIMAL is a finite 128-bit fixed-point type
following PostgreSQL's result-type rules, and predicted that the cost of
computing on it would be "low, and unmeasurable on the current benchmark
suite". The second half of that sentence was true for a structural reason
rather than a performance one: **no benchmark in the tree exercised DECIMAL at
all.** `benchmarks/tpch/schema.go` declared every monetary column FLOAT64 with
the comment "DECIMAL would be ideal but Wadjet uses float", and ClickBench has
no decimal column.

This is the measurement that sentence was missing.

## The fixture

The TPC-H specification (v3, §1.3) declares eight columns `DECIMAL(15,2)`:
`s_acctbal`, `p_retailprice`, `ps_supplycost`, `c_acctbal`, `o_totalprice`,
`l_extendedprice`, `l_discount`, `l_tax`. `benchmarks/tpch/schema_decimal.go`
adds a fixture variant in which those eight carry that type and **nothing else
changes**. It is BUILT by rewriting the FLOAT64 schema rather than written out
again, so a column added to `schema.go` appears in both and the two cannot
drift.

`l_quantity` is decimal in the specification too and stays FLOAT64 in BOTH
fixtures, so the only difference between them is those eight columns. It holds
whole numbers in this generator, so no exactness rides on it.

The generator was changed to compute every monetary value as an exact integer
number of cents and convert at the boundary: `f.money(cents)` renders decimal
TEXT for the decimal fixture and divides the same integer by 100 for the float
one. The float arm is bit-for-bit what it produced before — the SF0.01 and SF1
baselines are unmoved — and the decimal arm never passes through a float64 on
the way in. The text goes through `internal/storage/ingest` and
`parquet.DecimalValueFromBox`, the same door an `INSERT` takes.

The FLOAT64 schema remains the default and the published-number benchmark.
The variant is opt-in: `TPCH_DECIMAL=1`, or an explicit `Fixture` argument.

## Correctness first

The variant is a gate before it is a benchmark, and it earned that billing on
the first run — see "What the gate found" below. The timings here cover the 20
of 22 queries the DECIMAL carrier can currently run; Q08 and Q14 are held out
by #695.

The gate compares DIGIT FOR DIGIT wherever the answer is decimal, at the scale
`decimalOutputTypes` records — each engine's declared scale where they agree,
`min(scale)` where PostgreSQL's rules and DuckDB's keep a different number of
them (AVG and `/` over numeric, ADR-0012 item 9). That scale is DERIVED from
the two declarations and asserted against the table, so it cannot become a
hand-tuned tolerance. The float gate's six-significant-digit quantum exists
because two correct engines summing float64 in different orders disagree past
it; on an exact type it accepts an error of about a thousand on this fixture's
`SUM(l_extendedprice)` of 2152189760.47. `TestDecimalGateRejectsOneCent` is
the proof, and it READS its value out of the fixture rather than carrying a
constant: it runs Q01, takes `sum_base_price`, adds one penny, and asserts the
decimal digest separates the two where the float digest does not.

**The corpus carries no count-only relaxation for Q02 and Q22**, unlike the
float gate. The reason given there — "borderline rows shift with accumulation
order" — is false on exact fixed-point, where the threshold is one exact value
and every row is definitively on one side of it. Carrying it over reported
Q22's single-process arm GREEN on an answer with five of seven groups wrong.
That was caught in review, not by the gate, and it is the sharpest lesson
here: a relaxation is an assertion about the data, and it does not survive a
change of carrier just because the SQL is unchanged.

## SF1, same machine, same window, interleaved

Protocol per ADR-0011 and the standing four-metric rule. Both fixtures are
built in ONE process from the same generator draws. Queries are interleaved —
float, decimal, float, decimal — inside each repetition, so drift over the run
lands on both arms alike; A/B across windows is not a comparison. **The pair's
ORDER swaps on odd repetitions**, so neither arm always pays the first-run
cost of a pair; with four repetitions after a discarded warm-up, each arm
leads exactly half the time. Wall is reported as mean and max, CPU as the
`getrusage` user+system delta, allocation as both `TotalAlloc` bytes and the
`Mallocs` object count.

Machine: 24 threads, 31 GiB, Linux 5.15 (WSL2), go1.26.1, single-process
embedded engine over `objstore.MemStore`.

**The order swap is not a detail — it moved the answer.** Three earlier
samples ran float-first every repetition and reported geomeans of **1.0393,
1.0921 and 1.0803** (the last taken independently by a reviewer). Two
order-swapped samples report **1.1334 and 1.1432**. The gap is in exactly the
direction an ordering bias predicts: whichever arm runs second in a pair
collects the benefit of the first one's warm-up, and running float first every
time handed that to the decimal arm as a constant. The swapped numbers are the
ones below, and they are the ones to quote.

| query | rows | float wall (mean/max) | dec wall (mean/max) | wall D/F | float CPU | dec CPU | CPU D/F | float alloc | dec alloc | alloc D/F | objects D/F |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Q01 | 6 | 351ms / 379ms | 1.241s / 1.458s | **3.534** | 4.535s | 12.253s | 2.702 | 1125.9 MiB | 1995.2 MiB | 1.772 | **1871.7** |
| Q02 | 100 | 262ms / 301ms | 257ms / 298ms | 0.982 | 1.822s | 1.916s | 1.052 | 838.6 MiB | 911.3 MiB | 1.087 | 1.039 |
| Q03 | 10 | 596ms / 709ms | 699ms / 862ms | 1.173 | 2.709s | 3.201s | 1.181 | 1344.9 MiB | 1551.5 MiB | 1.154 | 3.045 |
| Q04 | 5 | 542ms / 593ms | 561ms / 635ms | 1.035 | 3.324s | 3.29s | 0.990 | 992.4 MiB | 963.7 MiB | 0.971 | 0.992 |
| Q05 | 5 | 530ms / 552ms | 567ms / 656ms | 1.069 | 1.767s | 1.897s | 1.074 | 1410.1 MiB | 1669.1 MiB | 1.184 | 1.704 |
| Q06 | 1 | 129ms / 145ms | 151ms / 169ms | 1.168 | 2.495s | 2.974s | 1.192 | 792.4 MiB | 929.2 MiB | 1.173 | 27.374 |
| Q07 | 4 | 449ms / 468ms | 482ms / 537ms | 1.072 | 4.241s | 4.935s | 1.164 | 1369.0 MiB | 1466.0 MiB | 1.071 | 1.385 |
| Q08 | — | — | — | — | — | — | — | — | — | — | not runnable (#695) |
| Q09 | 150 | 504ms / 556ms | 556ms / 647ms | 1.102 | 4.123s | 4.674s | 1.134 | 1473.2 MiB | 1674.8 MiB | 1.137 | 0.999 |
| Q10 | 20 | 493ms / 534ms | 720ms / 800ms | **1.459** | 2.146s | 2.254s | 1.050 | 1168.5 MiB | 1428.1 MiB | 1.222 | 3.794 |
| Q11 | 758 | 174ms / 195ms | 206ms / 234ms | 1.182 | 547ms | 594ms | 1.084 | 393.4 MiB | 434.8 MiB | 1.105 | 6.230 |
| Q12 | 2 | 342ms / 427ms | 334ms / 389ms | 0.977 | 4.697s | 4.641s | 0.988 | 1619.8 MiB | 1637.1 MiB | 1.011 | 0.999 |
| Q13 | 100 | 404ms / 420ms | 407ms / 423ms | 1.007 | 1.624s | 1.576s | 0.971 | 359.1 MiB | 327.5 MiB | 0.912 | 1.002 |
| Q14 | — | — | — | — | — | — | — | — | — | — | not runnable (#695) |
| Q15 | 1 | 193ms / 233ms | 191ms / 200ms | 0.992 | 2.734s | 2.73s | 0.998 | 837.2 MiB | 914.6 MiB | 1.092 | 21.355 |
| Q16 | 18153 | 65ms / 66ms | 63ms / 66ms | 0.978 | 334ms | 317ms | 0.949 | 249.8 MiB | 232.9 MiB | 0.932 | 1.000 |
| Q17 | 1 | 146ms / 152ms | 156ms / 165ms | 1.070 | 1.551s | 1.692s | 1.091 | 592.5 MiB | 622.1 MiB | 1.050 | 0.998 |
| Q18 | 70 | 671ms / 762ms | 678ms / 716ms | 1.011 | 2.924s | 2.974s | 1.017 | 1085.6 MiB | 1071.2 MiB | 0.987 | 0.994 |
| Q19 | 1 | 152ms / 165ms | 169ms / 187ms | 1.111 | 2.546s | 2.716s | 1.067 | 766.8 MiB | 797.6 MiB | 1.040 | 1.032 |
| Q20 | 155 | 271ms / 333ms | 275ms / 329ms | 1.015 | 2.38s | 2.298s | 0.966 | 904.3 MiB | 851.5 MiB | 0.942 | 1.010 |
| Q21 | 100 | 1.079s / 1.392s | 1.016s / 1.156s | 0.942 | 6.213s | 5.809s | 0.935 | 1839.0 MiB | 1815.3 MiB | 0.987 | 1.000 |
| Q22 | 7 | 184ms / 218ms | 192ms / 221ms | 1.039 | 551ms | 530ms | 0.961 | 220.3 MiB | 224.9 MiB | 1.021 | 1.031 |

**Geomean wall DECIMAL/FLOAT over the 20 measured queries: 1.1334** (the other
swapped sample: 1.1432). **Without Q01: 1.0674** (1.0710).

**Bytes at rest**: float 551.8 MiB in 90 objects, decimal 550.8 MiB in 90
objects — ratio **0.9982**, identical in all five samples. A `DECIMAL(15,2)`
is an INT64 leaf and a `FLOAT64` is a DOUBLE leaf; both are eight bytes, and
the decimal side compresses fractionally better because the low digits of a
scaled integer are less entropic than a double's mantissa. This is the honest
byte metric for a single in-process run — there is no wire.

## Reading the number

The whole-corpus cost is **about +13% wall**, and the distribution matters as
much as the geomean:

- **One query carries most of it: Q01, at 3.53× and 3.96× wall across the two
  swapped samples and 2.70×–3.41× CPU.** Q01 is the only query in the corpus
  that does decimal ARITHMETIC at volume:
  `SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))` is two 128-bit
  multiplications and two additions per row over 6M rows, plus a second SUM at
  scale 4 and two AVGs dividing at scale 6. Everything else in TPC-H either
  sums a bare column (one add per row) or filters on one.
- **But the rest is not free, and the first version of this document said it
  was.** Excluding Q01 the geomean is **1.0674 / 1.0710** — about +7%, not the
  ~0% the float-first samples reported. That claim was the ordering bias, and
  it is the single most useful thing the order swap bought.
- **Q10 at 1.46×** is second and does no decimal arithmetic at all: it groups
  150k customers with a `DECIMAL(15,2)` in the group VALUE and sorts on a
  `DECIMAL(38,4)`. A 16-byte group value against an 8-byte one, and a 16-byte
  sort key against an 8-byte one. This is the carrier's WIDTH, not its
  arithmetic, and it is the second mechanism worth chasing.
- **Two apparent decimal WINS did not survive the swap.** Q12 reported
  0.77–0.88 wall and 0.57–0.61 CPU across the three float-first samples, and
  Q21 0.93–0.97 wall and 0.69–0.85 CPU — reproducible enough to look like a
  mechanism. Under the swapped protocol they are **Q12 CPU 0.954 / 0.988** and
  **Q21 CPU 0.921 / 0.935**. A ratio that reproduces across repetitions is not
  a real one when every repetition shares the same bias.
- **The allocation OBJECT count is the loudest number in the table and the
  clearest lead.** Q01 allocates **~1871× as many objects** on decimals for
  1.77× the bytes; Q06 27×, Q15 21×, Q11 6×, Q10 3.8×, Q03 3.0×. That shape —
  an enormous count of very small allocations against a modest byte increase —
  is a value being BOXED one at a time, not a wider column being carried, and
  it is upstream of ADR-0024's kernels, which measure allocation-free over a
  batch. It reproduces to four significant figures across every sample.

So the ADR's prediction is **half right, and now measurable**. Exact decimals
cost about 13% across this workload and about 7% once the one
arithmetic-bound query is set aside — low, but not the "unmeasurable" the
record hoped for. A query that multiplies decimals in its inner loop pays
about 3.5×, and TPC-H contains exactly one such query; a workload of Q01s
would pay that rate.

### What this measurement does NOT say

- It is single-process. The distributed path adds `.wshf` shuffle bytes for
  16-byte values against 8-byte ones, which this run cannot see. The
  bytes-at-rest ratio (0.998) is the closest available proxy and it says the
  storage side is free; the exchange side is unmeasured.
- Two queries are missing (#695). Q14 is a decimal-arithmetic query, so the
  geomean over a fixed engine would likely rise a little.
- No optimization was attempted, and none should be until the object-count
  finding above is understood — a ~1870× allocation-count ratio is a mechanism
  to find, not a constant to tune around. This document is the baseline that
  pass should be measured against.
- The DAG arm of the correctness gate loads through `parquet.NewWriter`
  directly, so it does not cross `ingest.checkType`. The single-process arm
  does, so the ingest boundary is covered for the same rows — by the other
  arm, not by that one.

## What the gate found

The correctness half was the point of the exercise, and it produced three
defects that every existing gate was structurally blind to, because none of
them had a DECIMAL column to look at. All three are silent or newly loud on
the spec-conformant carrier and all three are green on the FLOAT64 one.

| # | what | where it shows |
|---|---|---|
| [#695](https://github.com/derekmwright/wadjet/issues/695) | A `CASE`/`COALESCE`/`GREATEST`/`LEAST` over a DECIMAL column and a numeric LITERAL declares the LITERAL's type. Loud where the decimal branch fires (`cannot store string into INT64 vector`), SILENT where it does not — a wrong declared type under right values. | Q14 errors on both paths; Q08 declares `FLOAT64` where PostgreSQL and DuckDB both say numeric |
| [#696](https://github.com/derekmwright/wadjet/issues/696) | Substituting a scalar subquery's DECIMAL value into an outer comparison loses the value. Both paths, wrong in different directions: the stage DAG compares against the UNSCALED carrier, the single-process path admits rows far below the threshold. | Q15 answers 0 rows on the DAG; Q22's group memberships are inflated on both |
| [#697](https://github.com/derekmwright/wadjet/issues/697) | A subquery anywhere in the statement drops the typmod of every BARE DECIMAL output column, so `RowDescription` carries −1 where PostgreSQL carries `numeric(15,2)`. | Q02 and Q18; Q10 is the control that keeps it |

Two observations about how they were caught are worth keeping:

- **#696's single-process half was invisible to the DuckDB gate as first
  written, and visible to the PostgreSQL one.** Q22's entry had inherited the
  float gate's count-only relaxation, so the number of GROUPS matched and the
  gate was green while five of the seven groups had the wrong membership. The
  pg-oracle's wire arm compares cells, and it said so. The relaxation is now
  gone from the decimal corpus and Q22's single-process arm is a pinned
  ratchet — see "Correctness first" above.
- **The two-path suite's "single-process arm" was the DAG for exactly the
  queries that mattered.** Arm A was the coordinator with its fast path
  enabled; for Q15 and Q22 that path declines the plan and answers from the
  stage DAG, so the suite was comparing the DAG against itself and reported
  both queries as agreeing while the real single-process engine disagreed
  with both. Arm A is now the embedded engine, which is unambiguously not the
  DAG. The float-fixture sibling still uses the fast-path coordinator and has
  the same blind spot for the same plans.
- **#695 is data-dependent.** The same defect errors on Q14 (whose decimal
  branch fires) and answers on Q08 (whose does not, at SF0.01). A gate that
  only checked VALUES would have called Q08 correct. The declared-type gate is
  what caught it, which is the argument for having one.

## Running it

```bash
# The correctness gate: 22 queries, both paths, exact against DuckDB.
go test -run TestTPCHQueriesDecimal ./benchmarks/tpch/
go test -run TestTPCHDecimalDeclaredTypes ./benchmarks/tpch/

# The invariance suites over the decimal plans.
go test -run TestTPCHOptimizationInvarianceDecimal ./benchmarks/tpch/
go test -run TestTwoPathInvarianceDecimal ./benchmarks/tpch/

# PostgreSQL, both arms, over the decimal fixture.
task pg-oracle:test-decimal

# This measurement.
TPCH_SCALE=1 go test -v -run TestTPCHDecimalPerformanceBaseline -timeout 60m ./benchmarks/tpch/

# Regenerate the DuckDB ground truth (needs /tmp/duckdb).
WADJET_REGENERATE_DECIMAL_BASELINE=1 go test -run TestTPCHQueriesDecimal ./benchmarks/tpch/
```
