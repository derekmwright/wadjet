# The dispatch-stall family: two mechanisms, found and fixed

**Status:** fixes merged (`10efb1b`, `6b26a15`, `6ea848e`, `c49bf6d`,
watchdog `20020ab`/`260b883`); SF100 validation arm on `c49bf6d`
pending. **Date:** 2026-08-14. **Arc:** dispatch-stall specimens 1-8
(2026-08-10 → 08-13) + 11 trap captures (08-13/14).

Every SF100 arm since 2026-08-10 risked wall poisoning from
intermittent worker stalls: ~5s to ~3m45 episodes where a worker
stopped responding, then self-healed or was reaped. The family turned
out to be **two distinct mechanisms**, both self-inflicted, both
amplified by the same environmental backdrop.

## The backdrop: stretched stop-the-world under writeback pressure

The August memory levers (150 GB base-table NVMe cache, decoded-chunk
cache, touch/populate) keep the page cache full and the NVMe writeback
pipeline busy. Threads blocked in D-state (`wbt_wait`, seen directly in
the `.threads` captures) cannot park promptly, so **any stop-the-world
pause stretches** from microseconds to potentially seconds. Neither
mechanism below is new code — both became pathological when STWs got
expensive.

## Mechanism 1 — frozen-spin (CPU-hot): the ReadMemStats STW storm

`taskPeakHeapTracker` (since `8d2533a`, 2026-04-10) called
`runtime.ReadMemStats` — a full STW — on a **50 ms ticker per active
task**, plus once at task start and once at task end. At
`max_concurrent=4` that is up to **80 STWs/second**, forever. Under the
backdrop, the process lives inside stop-the-world: metrics port frozen,
runnable goroutines starved, ~1-3 cores burning in runtime machinery.

**Evidence:** six trap firings on the 2026-08-13 creep run
(`results/20260813-232615`); every SIGABRT `GOTRACEBACK=all` dump
caught the tracker goroutine in `[stopping the world]` inside
`ReadMemStats`. Per-TID CPU deltas (the `.threads` capture, `3d049a6`)
showed a single userspace-hot thread with all else parked — the shape
specimens 1-8 had shown without attribution since 08-10.

**Fixes:**
- `10efb1b` — tracker + task-end heap reads moved to `runtime/metrics`
  (same counters, no STW). The 30s stats refresher and on-demand heap
  profiler keep `ReadMemStats` (infrequent, isolated by design).
- `6b26a15` — per-task forced `runtime.GC()` (join/agg/sort/window
  completions) coalesced to one per 2s via CAS: stage drains complete
  ~40 tasks/minute in bursts, and every forced GC after the first
  collects almost nothing while paying 2 STWs + a mark cycle. The
  pressure profiler's sidecar stats also moved to `runtime/metrics`.

**Validation so far:** run `20260814-002639` (bin `260b883`): firings
6 → 4, all four gracefully captured without kills, and **R1 = 259.1s —
the fastest suite ever recorded on this config** (prior R1s: 319.3 /
330.8 / 380.2). Every benchmark all along was paying the storm tax.

## Mechanism 2 — quiet-stall (CPU-quiet-ish): the journald log jam

The worker logs to stderr → 64 KB pipe → `sed` relay → journald. When
journald stalls (disk-durable journal on the EBS root volume, plus the
watchdog itself `logger`-ing 350 KB pprof dumps through syslog), the
pipe fills, the next `slog` write blocks **while holding the
TextHandler mutex**, and every goroutine that logs parks behind it — a
whole-process freeze that self-heals in one burst when journald
catches up.

**Evidence:** validation run, Q06-R4 (normally 2-3s, took 3m32):
coordinator task results flowed until 00:57:28, dead for 211s, then a
36-result burst at 01:00:59. The 30s liveness markers (`d85cff4`)
prove worker `-0c34` froze whole-process for 190s while the other two
workers ticked perfectly; host idle, PSI clean. journald **arrival**
lagged **emission** by 1-2 minutes in the same window, and the
watchdog's own shell loop wedged on `logger`. Q06/Q11/Q16/Q20-R4 each
lost ~3m30-3m45 to this — the same duration band as the original
q22-R2 cousins (whose "app-log fully silent" symptom was partly a
block-buffered `sed` artifact, fixed by `sed -u` in `20020ab`).

**Local repro (decisive, no EC2):** `wadjet serve` with a deliberately
stalled stderr reader. `WADJET_SYNC_LOG=1` (old behavior): frozen at
query ~400, right at pipe capacity — reproduced twice. Async default:
**2000/2000 queries** served against the same stalled pipe.

**Fixes:**
- `6ea848e` — `internal/logio.AsyncWriter`: bounded channel (8192
  records), single drainer, drop-and-count on overflow with in-band
  drop reporting. Wired into `wadjet serve` and `tpch-bench` (the
  bench coordinator shares the journald pipe). Execution never gates
  on the log sink. `WADJET_SYNC_LOG=1` restores direct writes.
- `c49bf6d` + follow-up — watchdog no longer ships dumps through
  syslog (they ride the S3 wlog uploader); `thread_capture` records
  journald/sed state (`LOGPIPE` lines) at fire time; journald on bench
  boxes runs `Storage=volatile` (RAM journal — no EBS writeback in the
  log path) with raised rate limits.

## Watchdog signature coverage (deploy-side, `main.tf` user-data)

| Signature | Trigger | Response |
|---|---|---|
| `stall-watchdog` (frozen-spin) | port dead >4s AND CPU accruing | `.threads` + pprof, SIGABRT only if port still dead post-capture (`260b883` grace) |
| `silent-stall` | port alive AND zero `[wN]` journal lines in 75s | evidence-only `.threads` + pprof |
| `quiet-freeze` | port dead >75s AND CPU quiet | evidence-only `.threads` |

All captures land in `/var/log/stall-*` and auto-ship via the wlog
uploader. The liveness marker (30s, unconditional) is the ground truth
for per-worker freeze windows in any wlog.

## Addendum: validation run `20260814-020153` (bin `eba88ad`)

**State accumulation: FIXED.** R1 249.4s (new best-ever), R2 448.7,
R3 309.9, **R4 243.7s — faster than R1, on the fourth consecutive
suite** (previous run: 259 → 672 → 822 → 1105 monotonic). The volatile
journald + async sink removed the accumulating drag. 88/88 correct.
LOGPIPE capture during a firing showed sed parked in `pipe_read`
waiting for data — the log path was empty, confirming the jam is gone.

**Residual firings (4) attributed and fixed (`5d9746f`):** the SIGABRT
dump named a third `ReadMemStats` caller missed by the first sweep —
`SpillManager.ShouldSpillFor → memory.heapPressureExceeded`
(`spill.go`), in the pipeline-breaker consume path behind 100ms caches:
up to 20 STW/s combined, refreshed exactly when the heap is under
pressure. Both heap-pressure gates (plus the long-task sidecar and the
`ProcessRSS` fallback) now ride `runtime/metrics`. Zero `ReadMemStats`
remain outside the two deliberate isolated sites (30s stats refresher,
on-demand profile envelope).

## Open items

1. **Validation arm on `5d9746f`** (benchmark_runs=4): expect ZERO
   firings on all three signatures. If clean: close the dispatch-stall
   arc (specimens 1-8 attributed: CPU-hot shape → mechanism 1 incl.
   the spill-gate storm; silent q22-R2 shape → mechanism 2) and re-run
   the barrier-overlap eager pair on the new clean-wall baseline
   (R1 ≈ 249s / R4 ≈ 244s).
