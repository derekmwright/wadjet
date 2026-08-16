# Readahead confirm window: RECORDS at steady, R2 judgment failed and re-attributed to the straggler mode (2026-08-16)

**Verdict: KEEP index reader + readahead as default (99f8552).** The
window set a NEW SUITE RECORD — R4 2m35.9s (old 158.6) — and the best
q09 ever (16.1s), with R3/R4 q08 in fast mode (17.8/18.1s) and rows +
vsigs exact. The pre-registered R2 judgment (q08 ~18s) FAILED — R2 ran
24.6s — but the wlogs re-attribute that elevation to the documented
RUN-LEVEL STRAGGLER MODE landing on R2 (one join-6 task at eff 4.2 /
dry 9.1 plus eff-8 mid-shapes, the exact signature recorded since
175927), while R3/R4 run the clean eff-12 shape. The fallback (reader
defaults to walk) is NOT taken: its premise — index ≤ walk everywhere
— is false at the steady regime this project treats as the headline
metric, where index+readahead now holds the record.

Window `results/20260816-000900`, bin `99f8552`, standard 4-run
config, on-demand, deploy 00:09Z, torn down before analysis, EC2 zero.

## Walls

| run | suite | q08 | q09 |
|---|---|---|---|
| 1 (cold) | 3m21.0s | 25.5s | 18.6s |
| 2 | 3m06.3s | 24.6s | 23.0s |
| 3 | 2m45.0s | 17.8s | 18.9s |
| 4 | **2m35.9s** | 18.1s | **16.1s** |

R4 beats every suite ever recorded on this config (prior records
158.6 / 160.2 / 160.6). R3 165.0s sits in the record cluster.
Correctness: 88/88 rows and vsigs identical to the prior windows.

## Two honest retractions

1. **The readahead mechanism story for R2 is retracted.** Shuffle
   files are written fresh every run and are therefore page-resident
   when read — kernel readahead was never the R2 variable. Whether
   the readahead advice contributed to the deep-warm record
   (155.9/165.0 vs the no-readahead index window's 167.5/169.3) or
   that is window luck remains unmeasured; the advice is advisory
   syscalls at ~zero cost and stays.
2. **The "index R2 penalty" from the 3-arm memo is downgraded to
   CONFOUNDED.** This window's R2 elevation is the straggler mode
   (present in walk windows too — 204851 R1, 175927 R1/R3, arm-B R1),
   and with 2-run arms the mode landing on R2 in the index arms may
   have been draw luck. The 3-arm walls stand as data; the causal
   read does not.

## The actual residual, promoted to #1

The run-level straggler mode is now the only mechanism standing
between the current config and record-cluster suites on EVERY warm
run: it costs ~15–20s when it lands, it lands on ~40% of runs across
every reader mode and every lever shipped this arc, and its signature
is stable (one or two join-6 tasks at eff ≈ 3–4 / dry ≈ 9–11 while
siblings run eff 12). Six same-config windows from 08-15/16 are on
disk locally (204851, 222607, 225843, 231122, 232354, 233347,
000900) — enough corpus for a log-first attribution pass (which
worker, which files, dispenser cadence, sink, placement, co-running
stages) with no further deploys.

## Window log

Preflight clean, on-demand, sha-pinned 99f8552 staged+verified.
Startup + engagement verified at T+6min via SSM (indexed_files 5.4k,
stage_ms 109ms, readahead_advise_bytes counting). Completion monitor
(zsh-safe variant) fired correctly. Destroyed before analysis;
describe-instances zero.
