# Shuffle decode-ahead SF100 window: q08 −25%, q09 −30%, plateau half-closed (2026-08-15)

**Verdict: KEEP (default stays ON).** Single-arm SF100 window
`results/20260815-154200`, bin `fedb90d`, standard config
(benchmark_runs=4, on-demand, 3× c7gd.4xlarge). 88/88 executions OK,
rows and vsigs exact every run (Q08 2 rows, Q09 175), zero reaps, zero
watchdog firings, EC2 zero after teardown.

## Walls

| run | suite | q08 | q09 |
|---|---|---|---|
| 1 (cold-with-cache) | 3m37.6s | 25.0s | 29.2s |
| 2 | 2m49.4s | 23.8s | 19.5s |
| 3 | 2m53.2s | 18.3s | 19.9s |
| 4 | 2m44.0s | 18.1s | 16.9s |

Steady q08 18.1–18.3s (prior windows ~24–26s; the attribution memo's
5–8s estimate landed) and q09 16.9s — both far outside window noise.
Suite R4 164.0s sits with the best-ever cluster (record 160.2s,
same-window rule bars a precise cross-window wall claim).

## Mechanism counters (the actual judgment)

join-6 done-lines (216 tasks, all k=15, `dispenser_producer_wait_ms=0`
throughout, parents 772/795 — same shape as the attribution window):

| task shape | before (135022) | after (154200) |
|---|---|---|
| mid (task-A shape) | 19.1s, eff 8.7, dry 4.9 widths | ~13.5–14.4s, **eff 11.8–12.4**, dry 1.3–2.1 |
| warm (task-B shape) | 14.7s, eff 3.7, dry 10.6 widths | ~8.0–8.4s, **eff 6.1–6.6**, dry 7.1–7.6 |

The single-producer ceiling is gone as the dominant pacer for the mid
shape (dry-wait collapsed 4.9 → ~1.7 widths). Warm tasks halved in
elapsed but still park ~7 widths dry — the plateau is half-closed.

## Residual pacer: the pressure hook throttles decode-ahead width

Per-worker decode-ahead markers over the suite (w0 final sample):
`chunks=766k`, `stage_ms=28.3s`, `decode_ms=22.4s`,
`window_full_ms=5.7s`, `token_stall_ms=24.5s`,
**`pressure_stall_ms=91.7s`** — the dominant stall class by ~4×.

SF100 workers sustain 22–50k refaults/s continuously (scan-decode-ahead
memo §9.3), so `scanDecodeAheadPressure` reads true through most of
steady state and the occupancy-floored rule holds WSHF admission to
~1 chunk ahead — exactly when warm probes drain fastest. This is the
same regime split §9.4/§9.5 resolved for the parquet window (empty
window + producer-bound ⇒ collapse is pure serialization). The scanner
is NOT yet the floor (stage 28s < pressure 92s; stage/decode are
comparable, so skip-walk staging stays a later lever).

**Next lever (ranked #1 for q08/q09):** occupancy-aware pressure
handling for the shuffle decode-ahead admission — the WSHF window holds
exact-size staged chunks (~1–16 MB), so a §9.5-style floor keyed on
actual held bytes (not chunk count), or exempting the shuffle path from
the refault channel on non-edge envelopes (its held bytes are bounded
by 128 MiB and retire within one probe pass), should recover most of
the remaining ~7 dry widths on warm tasks. Needs its own window.

## Window log

Deploy ~15:15Z, suite 15:20–15:52Z, torn down 15:5xZ immediately on the
wlog-request marker, EC2 verified zero (incl. stopped). Monitor note:
the completion watch must filter results/ prefixes to the date shape —
`trino-*` sorts after every date prefix and silently shadows "latest".
