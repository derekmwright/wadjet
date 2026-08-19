# SF100 value-level correctness: opaque fingerprints

`benchmarks/tpch/baseline-sf100.json` gates **row counts**. All 22 are
populated and externally validated against Trino 470 over the identical
parquet, and its `row_checksum` field has been empty since the day it was
written.

Every wrong-value bug this project found in 2026-08 passes a row-count gate:

- **#312** — a join predicate dropped on the DAG path inflated Q05 revenues
  ~25x. Identical row count.
- MEDIAN returned SUM under distributed execution. Identical row count.
- STDDEV was computed over one parallel clone's rows. Identical row count.

This document describes the arm that closes that: a per-query fingerprint of
the **values**, captured from an external engine, stored opaquely.

## What is stored, and why nothing readable

`benchmarks/tpch/fingerprint-sf100.json` holds, per query:

| field | why it is allowed |
|---|---|
| `row_count` | structural, already externally validated; a digest with no size is not a gate |
| `columns` | structural; a reordered or renamed SELECT list is a divergence worth naming |
| `fine` / `coarse` | truncated SHA-256 of the canonical row rendering at 6 and 4 significant float digits |
| `key_fine` / `key_coarse` | the same, over the ORDER BY key columns only (key-sequence mode) |
| `engine`, `engine_version`, `dataset`, `captured_at` | provenance |
| `mode`, `tiebreak`, `order_keys`, `why` | what a passing digest proves |

Nothing else. **No sums, no min/max, no sample rows.** A readable expectation
invites two failure modes that cost this project real time: implementing
toward the answer key, and "fixing" a red gate by editing the expectation. The
harness's `ValueSig` (a per-column numeric SUM) is deliberately not stored
here for exactly that reason — it is a readable value.

The digest is `internal/oracle.Fingerprint`, the same type whose 175 entries
gate SF0.01 today. `TestGroundTruthFileCarriesNoValues` enforces the field
allowlist above, and the loader parses with `DisallowUnknownFields`, so a
hand-added `"sums"` key fails to load rather than quietly riding along.

## Provenance: what may write an entry

`ParseFingerprintFile(data, KindGroundTruth)` refuses:

- an entry stamped `engine: "wadjet"`, **by name**, with the message *"this
  file is NOT ground truth"*;
- an entry with no engine, or an engine outside the reference set
  (`duckdb`, `trino`);
- an entry naming an engine but no version or dataset — an expectation that
  cannot be traced back to the bytes it was read from is not ground truth;
- a whole file whose `kind` is `regression` rather than `ground-truth`.

Wadjet's own fingerprints are legitimate — as **regression detection**. They
live in a separate `kind: "regression"` file written by `tpch-bench
--fingerprint-out`, and the distinction is stated in the file's own note: a
self-fingerprint can say an answer *changed* between two builds; it can never
say an answer is *right*. This mirrors `duckdb_compare_test.go`'s
`"this baseline is NOT ground truth"` check, which exists because a wrong Q05
answer once became the baseline.

## Order sensitivity without tie-brittleness

Every SF100 fingerprint is **order-sensitive**. An unordered digest sorts rows
before hashing, so a result that silently dropped its ORDER BY passes it —
that is the #320 shape, and the old `hasTopLevelOrderBy` heuristic would have
exempted it.

But comparing positionally when the ORDER BY is not a **total** order produces
a flaky gate, not a strong one: SQL leaves the order of tied rows unspecified,
so two correct engines may legally differ, and at SF100 a false failure is
expensive enough to teach everyone to ignore the gate.

Each query therefore declares one of two order-sensitive modes, recorded in
its stored entry (`benchmarks/tpch/fingerprint.go`):

**`ordered-total`** — the ORDER BY is a total order and every key is
projected, so rows are digested positionally. Where the canonical text leaves
ties open, the **correctness variant** appends a deterministic tiebreaker that
the query already projects:

| query | canonical ORDER BY | appended | why it is total |
|---|---|---|---|
| q03 | `revenue DESC, o_orderdate` | `l_orderkey` | GROUP BY key, projected |
| q05 | `revenue DESC` | `n_name` | GROUP BY key, projected |
| q10 | `revenue DESC` | `c_custkey` | GROUP BY key, projected |
| q11 | `value DESC` | `ps_partkey` | GROUP BY key, projected |
| q18 | `o_totalprice DESC, o_orderdate` | `o_orderkey` | GROUP BY key, projected |

The other 17 are already total: their ORDER BY is the GROUP BY key list
(q01, q04, q07, q08, q09, q12, q13, q16, q22), or includes a column unique in
the generated data (q02 `s_name`+`p_partkey`, q20/q21 `s_name`, q15
`s_suppkey`), or the result is a single row (q06, q14, q17, q19).

**`multiset+keyseq`** — the fallback for a query where a tiebreaker is
impossible or would distort the query: digest the row **multiset** plus the
positional sequence of the **ORDER BY key values**. Tied rows carry equal
keys, so tie order may vary freely while a dropped or inverted ORDER BY still
changes the key sequence. This is the scheme `internal/oracle/compare.go`
already applies to generated queries (`CmpUnordered` + `OrderKeys`). No TPC-H
query needs it today — all 22 admit a projected total order — but it is
implemented, tested (`TestKeySeqModeIsOrderSensitiveButTieImmune`), and
available for the query whose sort key turns out to be too unstable for a
positional digest.

### Correctness and performance run different variants

`tpch.GetQuery` returns the canonical TPC-H text, and that is what the
benchmark **times** — untouched. `tpch.CorrectnessQueries` returns the variant
the gate **digests**. They read the same tables and mean the same thing; only
the order of tied rows differs, which is precisely what SQL leaves open and
what a positional digest must not be allowed to guess at.
`TestCorrectnessVariants` pins every variant: it parses, its tiebreaker is
projected, and the perf text is unchanged.

## Float stability: the measurement

A SUM at SF100 accumulates over ~600M rows, and the order of accumulation
varies with partitioning, worker count and run. A digest is brittle to exactly
that. The dual-precision design (6 and 4 significant digits, match at either)
is the intended mitigation, but whether 4 digits is *enough* at SF100 was an
open question, so it was measured rather than assumed.

`TestFingerprintStability` runs the correctness variants repeatedly under
configurations that genuinely change accumulation order:

- **single-process arm** — seven configurations: the default, plus
  `partitioned-agg`, `parallel-emit`, `two-level-ht` and `agg-fast-paths`
  individually disabled (each changes *how* a sum is accumulated), plus
  `GOMAXPROCS` at 1 and 4;
- **distributed arm** — the stage DAG at 1, 2 and 3 workers, which is the
  SF100 shape: each worker aggregates its own partition and the coordinator
  merges partials.

Each configuration runs the suite twice. Alongside the fine and coarse
digests, the report records an **exact digest** (full float precision) as a
control: if the exact digest never moves, nothing was reordered and a stable
fine digest would prove nothing. It also records the **worst relative
deviation** between any two samples of the same query, which is the number
that extrapolates.

### Results

Measured on a 24-core host, 2 repeats per configuration:

| scale | arms | samples/query | queries whose exact answer moved | queries whose *fine* digest moved | queries the gate would reject | worst relative deviation |
|---|---|---|---|---|---|---|
| SF0.01 | single | 14 | 0 of 22 | 0 | 0 | 0 (nothing reordered) |
| SF0.1 | single + DAG | 20 | 12 of 22 | 0 | 0 | — |
| SF1 | single | 14 | 11 of 22 | 0 | **1 (q09)** | 2.90e-12 (q01) |
| SF1, after the remedy below | single | 14 | 11 of 22 | 0 | **0** | 2.89e-12 (q01) |

At SF0.1 twelve queries produced up to **15 distinct full-precision answers
out of 20 samples** — q01, q05, q07, q09 and q10 reordered heavily — and the
6-significant-digit rendering did not move once. At SF1 the same held, with
the worst relative deviation between two samples of the same query measured at
**2.9e-12** (q01; most queries sat at 1e-15 to 1e-16). Against
the fine quantum of 1e-6 that is a per-cell flip probability of ~3e-6, and
~8e-14 for both renderings at once.

**So the quantization was never the problem. One query failed anyway, and for
a different reason.**

### The instability the measurement found: q09, and the integer branch

At SF1, q09 produced two distinct digests at **both** precisions while its
numeric cells agreed to 3.3e-15 — a signature no rounding boundary explains.
Reproduced directly (8 alternating runs, `partitioned-agg` on and off), the
cause is one row:

```
row 108 renders "PERU|1997|4.32617e+07"           in 7 of 8 runs
row 108 renders "PERU|1997|43261724"              in 1 of 8 runs
```

The true value of that group's `sum_profit` is the whole number 43261724.
Under most accumulation orders the float lands a few ULP off it and renders
quantized; under one order it lands *exactly* on it, and the cell rendering's
exact-integer branch (`oracle.fingerprintFloat`: an integral float renders as
its full digits, so a SUM one engine reports as BIGINT and another as DOUBLE
still agree, and large keys stay distinguishable) renders it as `43261724`
instead. Two values one ULP apart, rendered completely differently — **at
every precision**, which is exactly the case dual precision cannot absorb.

The remedy, in `SignatureOf` (`benchmarks/tpch/fingerprint.go`): before
digesting, snap any float cell within **1e-9 relative** of a whole number to
that whole number. The window sits three orders of magnitude above the worst
accumulation noise measured at SF1 (2.9e-12) and three below the fine quantum
(1e-6) — margin on both sides — so the
branch is now taken consistently or not at all, on both sides — the reference
engine's answer is digested through the same function. It cannot mask a real
error the digest would otherwise catch, since the fine quantum already absorbs
a thousand times more. `TestSignatureSnapsNearIntegers` pins both directions,
including a self-check that fails if the underlying discontinuity ever
disappears (at which point the snap should be deleted rather than kept).

**Only q09 needed it, and only at SF1** — but nothing about the mechanism is
specific to q09. Any large float SUM whose true value is a whole number can hit
it, and the odds rise with the number of groups, so q11 (~93k rows at SF100),
q16 (~28k) and q20 (~18k) are the queries most exposed at SF100. That is the
argument for fixing the rendering rather than pinning q09.

With the snap in place, the SF1 measurement re-run is clean: the same 11
queries reorder (q01, q05, q07, q08 and q09 each produce **14 distinct
full-precision answers out of 14 samples** — every single run differs), no
fine digest moves, and nothing the gate would reject.

### The ordering risk, which no precision tier absorbs

A positional digest also depends on rows not *swapping*, and two rows swap when
the accumulation noise exceeds the gap between their sort keys — a failure no
quantization can absorb, because the rows themselves move. The measurement
therefore also reports the tightest gap between adjacent sort-key values:

```
tightest gap between adjacent sort-key values: 6.17e-07 (q11)
against 2.89e-12 of noise — safety factor 2.1e+05
```

q11 is the tightest because it sorts ~758 rows (SF1) by a float SUM. At SF100
it sorts ~93k rows over the same value range, so the minimum gap shrinks by
roughly 100x while the noise grows ~10x: a safety factor near 2e+02 — three
orders thinner, still comfortably above 1. This is the number to re-check when
the SF100 ground truth is first captured; if it ever approaches 1, q11 is the
query to move to `multiset+keyseq`, which is why that mode is implemented and
tested rather than merely described.

### What this implies at SF100

The accumulation noise is a *relative* quantity, so it scales with the number
of additions roughly as √N, not with the value magnitude. A rendered digit
flips only when a value sits within that noise of a rounding boundary, so the
per-cell flip probability is ≈ noise / quantum, and the probability that
**both** renderings flip on the same cell is the product of two independent
lotteries (1e-6 and 1e-4 quanta). Going from SF1 to SF100 multiplies the row
count by 100 and the noise by ~10 — call it 3e-11 — so a large-result query
(q11 ~93k rows, q16 ~28k, q20 ~18k) carries a few-percent chance per run that
*some* cell moves its fine digest, and ~1e-9 that the same cell moves both.
The fine digest of those queries is therefore the part expected to move first,
and it is expected to move.

**This is why the dual-precision policy is load-bearing at SF100 and a single
digest would not be.** A gate storing only the 6-digit digest would flake on
the large-result queries; a gate storing only the 4-digit digest would absorb
a real error up to ~1e-4 relative. Storing both, and accepting a match at
either, is what makes the digest usable at this scale.

Two named residual risks, neither of them digest precision:

1. **q11's HAVING threshold.** `HAVING SUM(...) > (SELECT SUM(...) * fraction)`
   decides row *membership* by a float comparison. A partkey whose value sits
   within accumulation noise of the threshold can legitimately enter or leave
   the result, changing the row count — which no precision tier absorbs. This
   is the same class the SF0.01 corpus already relaxes for q02/q22
   (`oracle.Query.CountOnly`). It did not move in any sample measured here; if
   it moves at SF100, the remedy is a row-count tolerance for q11 specifically,
   stated in its entry, not a coarser digest.
2. **q15's float equality.** `WHERE total_revenue = (SELECT MAX(total_revenue)
   FROM revenue)` compares two float sums for exact equality. Trino has been
   observed to flake 0/1 rows on its own float tie here (see
   `baseline-sf100.json`'s `captured_on`). The CTE is materialized once in
   Wadjet, so both sides come from the same accumulation and the equality
   holds; this is a property of the plan, not of the digest.

## Generating the ground truth

**DuckDB, not Trino.** DuckDB is already this repo's reference engine
(`benchmarks/tpch/duckdb-setup/`, the SF0.01 gate and its stored
fingerprints), so both scales agree on what ground truth means and on how a
cell is rendered. It is a single binary with no cluster to stand up, which
matters for a job that must fit in a benchmark window. And it reads the
benchmark's own parquet in place via httpfs, so "the same bytes" is structural
rather than a claim. Trino remains the second opinion — it produced the
externally-validated row counts in `baseline-sf100.json` — and the loader
accepts `engine: "trino"` for that reason.

**This does not run on a laptop.** The bucket is 280 GiB. Generation belongs
on an in-region instance during a benchmark window, on the host that already
has the data local to it.

```bash
# 1. DuckDB CLI on the benchmark host, at the path the generator expects:
wget -q https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip -O /tmp/duckdb.zip
unzip -d /tmp /tmp/duckdb.zip     # produces /tmp/duckdb

# 2. Capture. Reads s3://wadjet-bench-sf100-use2/tables/ — the same prefix
#    tpch-bench --skip-load discovers — through httpfs with the instance role.
WADJET_FP_GENERATE=1 \
WADJET_FP_DATA=s3://wadjet-bench-sf100-use2/tables/ \
WADJET_FP_S3_REGION=us-east-2 \
WADJET_FP_DUCKDB_MEMORY=48GB \
WADJET_FP_DUCKDB_TMP=/mnt/nvme/duckdb-tmp \
  go test -run TestGenerateFingerprintGroundTruth -timeout 6h -v ./benchmarks/tpch/

# 3. Commit the regenerated benchmarks/tpch/fingerprint-sf100.json.
```

The generator (`benchmarks/tpch/fingerprint_gen_test.go`) is the only writer
of a ground-truth file. It stamps every entry with the DuckDB version, the
dataset URI and the capture time; it round-trips the result through the loader
before writing, so a file the gate would refuse never reaches disk; and when
the file it is replacing already has entries, it **re-verifies** them and
fails with *"STORED FINGERPRINT IS NOT DUCKDB'S ANSWER"* if the stored digest
no longer matches live DuckDB — the check that catches a data, query-text or
DuckDB-version drift instead of blaming Wadjet.

Date columns are cast to VARCHAR in the generated views: the SF100 parquet
stores them as DATE while the local fixtures store strings, and Wadjet renders
a DATE cell as `YYYY-MM-DD` (`batch.FormatDate`) either way, so the cast makes
DuckDB's rendering identical rather than type-dependent.

### The SF0.01 sibling proves the apparatus

`benchmarks/tpch/fingerprint-sf001.json` is the same thing at a scale CI can
run: DuckDB fingerprints of the same correctness variants over the committed
`duckdb-data` fixtures. `TestFingerprintGateAgainstDuckDBSF001` holds Wadjet
against it on every test run, so the tiebroken ORDER BYs, the column names and
the cross-engine cell rendering are proven continuously — instead of first
meeting reality on a 280 GiB bucket. Regenerate it with:

```bash
WADJET_FP_GENERATE=1 WADJET_FP_DATA=duckdb-data WADJET_FP_SCALE=0.01 \
WADJET_FP_OUT=fingerprint-sf001.json \
  go test -run TestGenerateFingerprintGroundTruth ./benchmarks/tpch/
```

## Using it in a benchmark run

```bash
tpch-bench --scale=100 --skip-load ... --fingerprint \
           --fingerprint-out=/mnt/results/fingerprint-wadjet.json
```

The pass runs **after** the timed runs — the timed numbers are the
deployment's primary artifact and must not pay for row materialization or
digesting — and emits one greppable line per query:

```
FINGERPRINT-GATE q05: matches duckdb ground truth (5 rows, ordered-total)
FINGERPRINT-GATE q11: DIVERGES from duckdb ground truth — content digest ... (fine/coarse, 92698 rows either way)
FINGERPRINT-GATE: FAIL — 1/22 queries diverge from ground truth
```

Three states are distinguished on purpose:

- **PASS/FAIL** — the gate is active (SF100 run, ground truth populated and
  covering the corpus).
- **NOT ACTIVE** — the ground truth is unpopulated, stale, or the run is at a
  different scale. The pass records signatures and says out loud that it has
  no correctness verdict, rather than reporting a pass.
- **UNVERIFIED** (per query) — the result could not be fully materialized. A
  large result can be left on S3 with only its row count returned; digesting
  the batches that happened to come back inline would be a fingerprint of part
  of the answer, i.e. a false pass. That case is reported, never signed.
