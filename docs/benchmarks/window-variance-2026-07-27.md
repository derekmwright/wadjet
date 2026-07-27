# SF100 window variance: what we know (2026-07-27)

**Question:** the same binary swings 7m22–10m49 across same-day windows;
steady passes sometimes run slower than cold; per-query deltas up to
±80% appear on queries the arms' config difference cannot touch. This
noise has degraded the interpretability of every A/B pair this month.

**Data:** 57 local SF100 run logs (2026-07-17 → 07-27). Core sample:
five comparable flag-off steady/cold pairs on post-fusion binaries
(runs 20260725-162139, -210807, 20260726-103757, -122025,
20260727-202225). Analyzer: `~/.cache/wadjet-abc/sf100/scripts/run_matrix.py`.

## Findings

1. **Cold is stable, steady is volatile.** Cold totals 469–526s (±6%);
   steady totals 415–545s (±14%). The slow tail includes
   steady-slower-than-cold (103757: 544.7s steady vs 499.4s cold).
   Cold is paced by S3 reads (stable); steady runs against cluster
   state (cache, background uploads, sensor state) — the volatility
   lives in that state.

2. **Slow runs are broad-based, worst on small queries.** 103757 ran
   above per-query median on 16/22 queries, with the largest relative
   inflations on the shortest queries (Q22 +192%, Q16 +187%, Q14
   +104%, Q12 +93%, Q15 +81%). A few seconds of run-wide drag taxes
   everything; small queries feel it proportionally hardest. This is a
   diffuse mechanism, not one bad query.

3. **The uniform-slow straggler class persists (bounded).** Small
   broadcast-probe stages show one task at ~3-5× its siblings (Q16
   join-2: straggler 18.8s vs siblings ~4s in the slow run, 13.6s in a
   fast run). This is the page-cache pressure-floor collapse — episode-
   capped since #260 (10s default cap) but still throttling for up to
   the cap per activation. Straggler-stage census across the five runs:
   3 / 2 / 1 / 0 / 0 stages — correlated with run slowness but far too
   few to explain the diffuse drag alone.

4. **Episode cap tuning candidate.** refaultEpisodeCap defaults to 10s;
   the #260 investigation measured CAUSAL episodes (self-displacement)
   going quiet in ~2s. Ambient (non-causal) episodes therefore throttle
   scans for up to 10s each before being declared non-causal. Reducing
   the default toward ~3s would preserve the protective function while
   cutting the non-causal throttle tax ~70%. NEEDS an SF100 A/B
   (WADJET_REFAULT_EPISODE_CAP env already exists for the arms) —
   queued for the next EC2 window.

5. **The attribution gap is log retention, not instrumentation.** The
   per-query decode-ahead sweep already logs pressure_stall_ms per
   query on each worker — the exact per-query sensor-tax attribution
   this investigation needed — but benchmark teardown discards worker
   logs, so none of the five runs can be decomposed. Fixed going
   forward: `deploy/benchmark/grab-worker-logs.sh` uploads worker
   journals to the run's results prefix in one command; run it after
   the coordinator self-stops, before destroy, on every measured run.

## Standing protocol implications

- Same-window pairs remain necessary but not sufficient: a ±14% steady
  band means single-pair per-query deltas under ~15-20% are unreadable.
  Either compare COLD passes (tighter band) alongside steady, run ≥2
  pairs, or wait for the episode-cap A/B to (maybe) collapse the band.
- Every future run: grab worker logs. The next variance session should
  start by decomposing per-query pressure_stall_ms across runs.
