# Trino re-comparison — 2026-08-14: the verdict flips

**Wadjet steady-state now beats Trino 470 FTE at SF100 on identical
hardware**: suite sum −10% (198.5 vs 221.2s), per-query geomean −19%
(7.20 vs 8.89s), 12/22 queries won. Cold is a tie (sum +2% slower,
geomean −7% faster). July's verdict was Trino ahead 1.96–2.87×
([trino-comparison-2026-07-25.md](trino-comparison-2026-07-25.md));
the reversal is entirely wadjet's improvement arc — Trino reproduced
its July number almost exactly (R2 227.7s today vs 225.7s July).

## Arms (same-day adjacent, 2026-08-14 midday US-East-2)

| | wadjet | Trino |
|---|---|---|
| results | `results/20260814-121720` | `results/trino-20260814-131038` |
| binary/version | b88159e (engine ≡ 7433d11) | Trino 470, Corretto 23 |
| config | `sf100-distributed.tfvars` as pinned (zstd envelope, 150 MB/s pacing, base-table + decoded caches, locality, skew-split, mc=4, grpc) | FTE (`retry-policy=TASK` + S3 exchange spooling), module-carried (no SSM hand-config), no spill (see incident) |
| hardware | c7g.2xlarge + 3× c7gd.4xlarge | identical |
| suites | 4 (R1 cold-with-cache, R3/R4 steady) | 4 (R1 cold, page-cache warm after) |
| rows | 88/88 vs `baseline-sf100.json` | 87/88 (q15 quirk below), lineitem 600,037,902 verified |

Timeline: wadjet 12:17–12:32, Trino 13:11–13:27 (one burned deploy
between them, see incident). Same-window rule satisfied; zero
stall-watchdog firings in the wadjet arm.

## Suite walls (seconds)

| Suite | Trino FTE | wadjet | Δ |
|---|---|---|---|
| R1 | 247.4 | 252.9 | +2% (tie-ish) |
| R2 | 227.7 | 202.4 | **−11%** |
| R3 | 220.8 | 209.9 | **−5%** |
| R4 | 221.6 | **187.2** | **−16%** (best suite ever on this config) |

Trino is remarkably flat across suites (its only warm state is page
cache; S3 re-read every run). Wadjet's R1→R4 slope is the NVMe
base-table + decoded-cache population arc.

## Per-query steady state (mean of R3/R4, seconds)

| q | Trino | wadjet | W/T | July W/T | note |
|---|---|---|---|---|---|
| q01 | 6.6 | 5.6 | 0.86 | 1.5 | |
| q02 | 6.1 | 4.6 | 0.75 | 2.9 | |
| q03 | 9.7 | 10.0 | 1.03 | **5.0** | watchlist: closed |
| q04 | 6.6 | 7.8 | 1.18 | 2.6 | |
| q05 | 11.9 | 7.7 | 0.65 | 2.2 | |
| q06 | 5.2 | 1.5 | 0.29 | 0.38 | wadjet win held |
| q07 | 14.8 | 8.4 | 0.57 | 2.5 | |
| q08 | 14.0 | 28.9 | **2.07** | 2.7 | worst residual |
| q09 | 17.3 | 20.4 | 1.18 | 2.1 | |
| q10 | 11.5 | 12.8 | 1.12 | **4.2** | watchlist: closed |
| q11 | 5.2 | 8.6 | **1.63** | 2.5 | residual |
| q12 | 8.1 | 6.3 | 0.77 | 2.2 | |
| q13 | 5.5 | 6.9 | 1.25 | **5.7** | watchlist: closed |
| q14 | 6.7 | 3.1 | 0.47 | 1.9 | |
| q15 | 9.4 | 2.3 | 0.24 | ~1 | q15 quirk below |
| q16 | 4.5 | 5.0 | 1.10 | 1.4 | |
| q17 | 11.6 | 16.9 | **1.45** | 2.4 | residual |
| q18 | 20.2 | 12.0 | 0.59 | 3.0 | |
| q19 | 8.3 | 5.0 | 0.60 | 1.6 | |
| q20 | 9.7 | 10.8 | 1.11 | 2.0 | |
| q21 | 24.5 | 10.9 | 0.45 | 2.4 | |
| q22 | 3.7 | 3.1 | 0.84 | **3.9** | watchlist: flipped to win |

(July W/T from the 2026-07-25 memo's same-window table; those were
against the stall-taxed engine.)

- **Watchlist resolution**: the July repartition-heavy gap class is
  gone — q13 5.7→1.25, q03 5.0→1.03, q10 4.2→1.12, q22 3.9→0.84.
  The stall-family closure + zstd/pacing arc closed it; the remaining
  per-stage-dispatch + S3-materialization tax is no longer the
  aggregate story.
- **Remaining Trino wins (roadmap input)**: q08 2.07 (wadjet's worst
  at 28.9s steady), q11 1.63, q17 1.45, q13 1.25, then a 1.0–1.2 band
  (q03/q04/q09/q10/q16/q20). These are the next diagnosis targets;
  exchange spooling vs streaming positions are settled in docs/adr/ —
  read before proposing structural changes.
- **q15 rows**: Trino returned 0/1/0/1 across its four runs — the
  known Trino CTE MAX float-tie flip (July memo; wadjet fixed this
  class via CTE materialization). Not a wadjet bug; wadjet's 1 row is
  the externally-validated answer.

## Attribution (mechanism, not window variance)

Trino's number is stable July→now (225.7 → 220.8–247.4 across all
suites). Wadjet went 441.6s (best-of-day July) → 187–210s steady:
the dispatch-stall family closure (ReadMemStats STW storms + journald
log-jam, `stall-family-postmortem-2026-08-14.md`), WSHZ zstd upload
envelope (−19% upload bytes), 150 MB/s upload pacing, decoded-chunk +
base-table caches, and reap-grace riding on top of the July engine.

## Deploy incident: spill + FTE are incompatible (one cluster burned)

The module fold-in (b88159e) enabled NVMe spill unconditionally,
assuming it harmless under FTE. Wrong: `spill-enabled=true` +
`retry-policy=TASK` fails **every join query** with internal error
`spillable not yet set` after ~190s of exchange.s3 retries
(GENERIC_INTERNAL_ERROR, ~3s running / ~190s "finishing"); q01/q06
pass because they have no spillable operators. July never hit it
because spill and FTE were SSM-applied to *separate* clusters. Fixed
in 53d03fd: spill config is written only when `fte=false`. Diagnostic
route that worked: query REST API `/v1/query/<id>` failureInfo — the
CLI stderr is jline-noise-masked.

## Residuals / next time

- **ENA utilization-shape comparison not judged this round**: the
  ena-poll sampler is now on Trino nodes (b88159e) and journaled all
  run, but worker journals were not shipped to S3 before teardown —
  the runner uploads only its own txt. Add journal shipping (coord +
  workers) to the Trino harness before the next run if the NIC story
  matters.
- Trino FTE config is now fully module-carried (`fte` var, default
  true) — no SSM hand-config; `-var=fte=false` gives streaming+spill.
- The runner now purges `trino-exchange/` post-suite (verified empty
  at teardown).
