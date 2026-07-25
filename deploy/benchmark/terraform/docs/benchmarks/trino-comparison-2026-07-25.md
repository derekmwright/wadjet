# Trino 470 vs Wadjet — SF100 TPC-H, identical hardware (2026-07-25)

Same 4-node shape for both engines (1× c7g.2xlarge coordinator + 3×
c7gd.4xlarge workers, on-demand, us-east-2), same parquet
(s3://wadjet-bench-sf100-use2, BIGINT/DOUBLE/DATE schema), back-to-back
runs in one evening window. Harness: deploy/benchmark/trino/ (PR #271).
Wadjet: main de1ec92 (post stage-chain-fusion), profile defaults
(locality + peer-wire + async purge, eager durability, 150 GB NVMe
base-table cache), results/20260725-180829. Trino: 470, Corretto 23,
Glue-backed Hive catalog, per-role JVM sizing (module defaults), spill
enabled on NVMe; FTE arm = retry-policy=TASK + S3 exchange spooling
(results/trino-20260725-174050 spill, -175239 FTE).

All walls below are RUN 2 of 2 (page-cache-warm; wadjet's NVMe cache also
warm — its advantage, and noted).

| Q | Trino streaming+spill | Trino FTE | Wadjet | W/T-FTE | rows T / W |
|---|---|---|---|---|---|
| q01 | 5.4s | 6.6s | 9.8s | 1.48× | ✓ |
| q02 | 3.9s | 6.0s | 9.8s | 1.65× | ✓ |
| q03 | 9.2s | 9.3s | 46.4s | 4.97× | ✓ |
| q04 | 4.9s | 6.3s | 19.7s | 3.12× | ✓ |
| q05 | 13.4s | 12.3s | 39.5s | 3.22× | ✓ |
| q06 | 3.9s | 5.1s | 1.9s | 0.38× | ✓ |
| q07 | 22.5s | 20.1s | 31.4s | 1.56× | **4 vs 1462** |
| q08 | 15.7s | 14.1s | 45.9s | 3.26× | **2 vs 731** |
| q09 | 36.0s | 17.3s | 49.2s | 2.85× | **175 vs 50250** |
| q10 | 8.7s | 11.3s | 47.1s | 4.17× | ✓ |
| q11 | 3.6s | 4.7s | 29.2s | 6.24× | **0 vs 92698** |
| q12 | 6.1s | 8.5s | 12.8s | 1.51× | ✓ |
| q13 | 5.8s | 5.8s | 33.0s | 5.68× | ✓ |
| q14 | 5.1s | 6.9s | 14.7s | 2.12× | ✓ |
| q15 | 7.2s | 9.7s | 9.5s | 0.97× | ✓ |
| q16 | 3.7s | 4.7s | 17.1s | 3.63× | ✓ |
| q17 | 11.0s | 12.2s | 23.3s | 1.91× | ✓ |
| q18 | 17.0s | 22.1s | 63.7s | 2.89× | ✓ |
| q19 | 7.4s | 8.2s | 16.4s | 2.01× | ✓ |
| q20 | 7.7s | 10.5s | 40.9s | 3.91× | ✓ |
| q21 | 34.1s | 20.4s | 73.2s | 3.59× | ✓ |
| q22 | 2.6s | 3.7s | 14.5s | 3.94× | ✓ |
| **TOTAL** | **235.0s** | **225.7s** | **648.9s** | **2.87×** | |

## Reading it honestly

- **Trino wins the suite ~2.9× in this window** (225.7s FTE vs 648.9s).
  This window was wadjet's worst of the day (evening degradation; its
  steady ran WORSE than cold, 10m48.9 vs 8m35.3). Against wadjet's best
  same-day steady (7m21.6 = 441.6s, midday window, results/20260725-
  162139) the gap is **1.96×**. The true number is between; more windows
  would tighten it.
- **Fairest architectural column is Trino FTE** (task retries + S3
  exchange spooling = wadjet's durability class). Trino's FTE tax on
  this suite is ≈0 (225.7 vs 235.0 streaming+spill) — its exchange
  volumes are small at this scale; q09 actually IMPROVED under FTE
  (36.0 → 17.3s; spooled partitioned builds beat in-memory + spill).
- **Wadjet wins q06 (0.38×) and ties q15**; the widest gaps (q13 5.7×,
  q03 5.0×, q10 4.2×, q22 3.9×) are repartition-heavy shapes — the
  remaining materialization boundaries fusion could not elide (true re-key
  exchanges) plus small-query dispatch floor.
- **Row parity 18/22.** The four divergences are all WADJET bugs found
  BY this comparison — the first non-self-referential row oracle in the
  gate chain: #272 (Q11 scalar-subquery threshold ~100× small at SF100;
  correct answer 0 rows, wadjet 92,698) and #273 (SUBSTR over date32
  renders day-granular at SF100: Q07 1462 vs 4, Q08 731 vs 2, Q09
  50,250 vs 175). All four inflated baselines were "44/44 OK" all along
  because baselines were wadjet-derived.
- Trino quirk observed: q15 flipped 0↔1 rows across its own runs (float
  tie on the CTE MAX — the divergence class wadjet eliminated via CTE
  materialization).
- Config notes: Trino has no base-table cache tier (page cache only);
  wadjet run-2 walls benefit from its warmed 150 GB NVMe cache. Wadjet
  carries eager S3 upload insurance in all arms. Streaming-Trino without
  spill OOM-killed q09/q21 at 14 GB per-node (20-query totals 167.6/
  157.5s, results/trino-20260725-172528, reference only).

## Next levers this table points at

1. Fix #272/#273 (correctness first).
2. The q03/q10/q13/q22 class: repartition-boundary materialization +
   small-query floor — the exchange links fusion couldn't touch.
3. Window variance itself (7m22–10m49 same binary same day) is now the
   biggest measurement problem AND possibly a real tail problem worth
   its own arc.
