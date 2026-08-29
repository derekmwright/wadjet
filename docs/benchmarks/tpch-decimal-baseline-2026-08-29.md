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
them (AVG and `/` over numeric, ADR-0012 item 9). The float gate's
six-significant-digit quantum exists because two correct engines summing
float64 in different orders disagree past it; on an exact type it would accept
an error of about a thousand on `SUM(l_extendedprice)`.
`TestDecimalGateRejectsOneCent` is the proof: it asserts the decimal digest
separates 3027140810.74 from 3027140810.75, and that the float digest does
not.

## SF1, same machine, same window, interleaved

Protocol per ADR-0011 and the standing four-metric rule. Both fixtures are
built in ONE process from the same generator draws. Queries are interleaved —
float, decimal, float, decimal — inside each repetition, so drift over the run
lands on both arms alike; A/B across windows is not a comparison. Three
repetitions after a discarded warm-up. Wall is reported as mean and max, CPU
as the `getrusage` user+system delta, allocation as both `TotalAlloc` bytes
and the `Mallocs` object count.

Machine: 24 threads, 31 GiB, Linux 5.15 (WSL2), go1.26.1, single-process
embedded engine over `objstore.MemStore`. **Two independent samples were
taken**; the table is the second, and the first is summarised below it.

| query | rows | float wall (mean/max) | dec wall (mean/max) | wall D/F | float CPU | dec CPU | CPU D/F | float alloc | dec alloc | alloc D/F | objects D/F |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Q01 | 6 | 319ms / 355ms | 1.086s / 1.197s | **3.406** | 4.466s | 11.478s | 2.570 | 1092.4 MiB | 1973.4 MiB | 1.807 | **1868.2** |
| Q02 | 100 | 206ms / 217ms | 228ms / 264ms | 1.107 | 1.741s | 1.909s | 1.097 | 843.4 MiB | 902.4 MiB | 1.070 | 1.024 |
| Q03 | 10 | 496ms / 535ms | 556ms / 585ms | 1.122 | 2.6s | 2.965s | 1.141 | 1373.4 MiB | 1553.4 MiB | 1.131 | 3.044 |
| Q04 | 5 | 463ms / 495ms | 464ms / 487ms | 1.002 | 3.164s | 3.069s | 0.970 | 991.7 MiB | 901.5 MiB | 0.909 | 0.991 |
| Q05 | 5 | 517ms / 564ms | 522ms / 539ms | 1.010 | 1.741s | 1.78s | 1.022 | 1407.1 MiB | 1657.1 MiB | 1.178 | 1.703 |
| Q06 | 1 | 125ms / 127ms | 139ms / 148ms | 1.112 | 2.549s | 2.863s | 1.123 | 833.1 MiB | 878.2 MiB | 1.054 | 27.366 |
| Q07 | 4 | 417ms / 438ms | 424ms / 448ms | 1.017 | 4.318s | 4.327s | 1.002 | 1379.7 MiB | 1459.0 MiB | 1.058 | 1.381 |
| Q08 | — | — | — | — | — | — | — | — | — | — | not runnable (#695) |
| Q09 | 150 | 483ms / 505ms | 496ms / 520ms | 1.027 | 4.482s | 4.507s | 1.006 | 1481.1 MiB | 1683.5 MiB | 1.137 | 0.993 |
| Q10 | 20 | 491ms / 539ms | 646ms / 670ms | 1.316 | 2.079s | 2.106s | 1.013 | 1165.5 MiB | 1412.7 MiB | 1.212 | 3.739 |
| Q11 | 758 | 188ms / 209ms | 209ms / 230ms | 1.113 | 494ms | 593ms | 1.201 | 406.4 MiB | 458.2 MiB | 1.127 | 6.237 |
| Q12 | 2 | 379ms / 401ms | 290ms / 337ms | 0.766 | 5.571s | 3.191s | 0.573 | 1725.3 MiB | 1547.7 MiB | 0.897 | 0.999 |
| Q13 | 100 | 396ms / 461ms | 380ms / 414ms | 0.959 | 1.5s | 1.494s | 0.996 | 361.0 MiB | 331.4 MiB | 0.918 | 1.002 |
| Q14 | — | — | — | — | — | — | — | — | — | — | not runnable (#695) |
| Q15 | 1 | 245ms / 306ms | 223ms / 258ms | 0.908 | 2.571s | 2.635s | 1.025 | 816.9 MiB | 924.8 MiB | 1.132 | 21.365 |
| Q16 | 18153 | 67ms / 75ms | 63ms / 69ms | 0.940 | 316ms | 301ms | 0.950 | 248.7 MiB | 224.8 MiB | 0.904 | 1.000 |
| Q17 | 1 | 149ms / 161ms | 145ms / 156ms | 0.969 | 1.597s | 1.497s | 0.937 | 608.1 MiB | 542.0 MiB | 0.891 | 0.998 |
| Q18 | 70 | 641ms / 689ms | 634ms / 786ms | 0.989 | 2.78s | 2.403s | 0.864 | 1111.4 MiB | 994.1 MiB | 0.894 | 0.995 |
| Q19 | 1 | 152ms / 173ms | 162ms / 188ms | 1.071 | 2.508s | 2.729s | 1.088 | 746.6 MiB | 730.2 MiB | 0.978 | 1.027 |
| Q20 | 155 | 252ms / 303ms | 247ms / 278ms | 0.983 | 2.322s | 2.275s | 0.980 | 875.3 MiB | 877.3 MiB | 1.002 | 0.996 |
| Q21 | 100 | 1.047s / 1.247s | 971ms / 1.242s | 0.928 | 6.779s | 4.681s | 0.691 | 1839.8 MiB | 1805.5 MiB | 0.981 | 1.000 |
| Q22 | 7 | 192ms / 200ms | 261ms / 463ms | 1.362 | 500ms | 589ms | 1.178 | 197.3 MiB | 238.7 MiB | 1.210 | 1.031 |

**Geomean wall DECIMAL/FLOAT over the 20 measured queries: 1.0921**; sample 1
gave **1.0393**. Without Q01: **1.0286** and **0.9881**. Q01 alone: **3.41×**
and **2.71×**.

**Bytes at rest**: float 551.8 MiB in 90 objects, decimal 550.8 MiB in 90
objects — ratio **0.9982**, identical in both samples. A `DECIMAL(15,2)` is an
INT64 leaf and a `FLOAT64` is a DOUBLE leaf; both are eight bytes, and the
decimal side compresses fractionally better because the low digits of a scaled
integer are less entropic than a double's mantissa. This is the honest byte
metric for a single in-process run — there is no wire.

## Reading the number

The aggregate cost is **+4% to +9% wall**, and the distribution matters far
more than the geomean:

- **12 to 14 of the 20 queries are within ±10%**, and 8 to 11 of them are
  FASTER on decimals than on floats. Those deltas are the run's noise floor,
  not a decimal effect in either direction — which is also why the two
  samples' geomeans differ by five points while every conclusion below holds
  in both.
- **One query carries essentially all of the cost: Q01, at 2.71× and 3.41×
  wall across the two samples, 2.33× and 2.57× CPU, 1.86× and 1.81×
  allocated bytes.** Q01 is the only query in the corpus that does decimal
  ARITHMETIC at volume: `SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))`
  is two 128-bit multiplications and two additions per row over 6M rows, plus
  a second SUM at scale 4 and two AVGs dividing at scale 6. Everything else in
  TPC-H either sums a bare column (one add per row) or filters on one. Drop
  Q01 and the geomean over the remaining 19 is 0.9881 and 1.0286 — decimals
  are within noise of floats on everything else in the corpus.
- **Q10 at 1.32–1.35×** is second, and it does no decimal arithmetic at all:
  it groups 150k customers with a `DECIMAL(15,2)` in the group VALUE and sorts
  on a `DECIMAL(38,4)`. A 16-byte group value against an 8-byte one, and a
  16-byte sort key against an 8-byte one.
- CPU tracks wall on Q01 and nowhere else, which is what a genuinely
  compute-bound difference looks like against noise.
- **The allocation OBJECT count is the loudest signal in the table, and the
  clearest lead for a future pass.** Q01 allocates **1868× as many objects**
  on decimals for only 1.81× the bytes; Q06 27×, Q15 21×, Q11 6×, Q10 3.7×,
  Q03 3.0×. A huge count of very small allocations is a value being BOXED one
  at a time, not a wider column being carried — and it is upstream of
  ADR-0024's kernels, which measure allocation-free over a batch.

So the ADR's prediction is **half right, and now measurable**. The cost of
exact decimals across a mixed analytics workload is low — under 10% here, and
within noise once the one arithmetic-bound query is set aside. It is not
uniformly low: a query that multiplies decimals in its inner loop pays about
3× on that loop, and TPC-H contains exactly one such query. A workload of
Q01s would pay that rate.

### What this measurement does NOT say

- It is single-process. The distributed path adds `.wshf` shuffle bytes for
  16-byte values against 8-byte ones, which this run cannot see. The
  bytes-at-rest ratio (0.998) is the closest available proxy and it says the
  storage side is free; the exchange side is unmeasured.
- Two queries are missing (#695). Q14 is a decimal-arithmetic query, so the
  geomean over a fixed engine would likely rise a little.
- No optimization was attempted, and none should be until the object-count
  finding above is understood — a 1868× allocation-count ratio is a mechanism
  to find, not a constant to tune around. This document is the baseline that
  pass should be measured against.

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

- **#696's single-process half was invisible to the DuckDB gate and visible to
  the PostgreSQL one.** Q22's DuckDB entry is count-only — rows tied at an
  aggregate threshold are interchangeable — so the number of GROUPS matched
  and the gate was green while four of the seven groups had the wrong
  membership. The pg-oracle's wire arm compares cells, and it said so.
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
