# Frozen-spin / no-kill drag: GC-cycle seizures under GOGC=100 (2026-08-14 night)

Evidence: 58 stall specimens (threads + kernel stacks + pprof dumps)
across `results/20260814-{195058,204612,221930,225424}/wlogs/`,
worker heap-pressure profiler lines, analyzer
`analyze_stalls.py` (session scratchpad; trivially rebuildable — parses
the watchdog's stat1/stat2/kstack format from main.tf `thread_capture`).

## Mechanism (named to the GC-cycle level)

1. **Every stall firing coincides with a worker GC cycle at the
   GOGC=100 trigger point.** Example (trt arm, worker
   i-055b0722ee1752f11 — every firing on the box):

   | pressure GC (heap before→after, MB) | stall firing |
   |---|---|
   | 20:52:11 — 15,213→7,344 | 20:52:29 |
   | 20:56:40 — 17,023→8,453 | 20:56:52 |
   | 20:58:18 / 20:58:51 — 15,746→7,706 / 16,899→7,287 | 20:58:14, 20:59:02 |
   | 21:44:19 — 17,202→9,419 | 21:43:34, 21:44:07, 21:45:16 |

2. **The specimen signature is the runtime's GC/preemption machinery,
   not application code**: per-TID 1s CPU deltas show ~10 threads
   futex-churning (park/unpark), one thread spinning `sched_yield`
   (+123 jiffies — a preemption/STW wait loop), R-state threads in user
   code with empty kernel stacks, memcg slab-charge traces; SIGABRT
   dumps show **zero runnable and zero GC-assist-labelled goroutines
   while process CPU accrues** — the seizure lives below the scheduler
   in mark/STW handoff.
3. **Workers run GOGC=100** (deploy tfvar `gogc` default "100" →
   `WADJET_GOGC=100`; verified in wlogs: `set GOMEMLIMIT …
   go_mem_limit=22288993689 gogc=100`). Live set is ~7-10 GB (6 GiB
   decoded cache + pool state), so every cycle marks that at 2× —
   `cmd/wadjet/main.go` documents this exact trade ("GC assist tax
   with GOGC=100 caused 2-3x query slowdowns once the LRU cache was
   populated") as the reason the binary's big-envelope DEFAULT is
   GOGC=off. The profile overrides the binary's default and buys the
   assist-seizure failure mode instead of the off-mode
   accumulation-burst failure mode. Coordinator runs off (unaffected;
   small heap).
4. **Severity spectrum, one mechanism**: seizure >4s + pprof dead →
   watchdog SIGABRT (storm arms; then the output-loss/re-execution
   cascade, and pre-bbdb985 the corpse-placement 30m hang); seizure
   under threshold → partial captures + the broad "no-kill drag"
   (18 pressure cycles on one worker in one arm ≈ 1-4s each scattered
   through every steady run — matches steady-worse-than-cold: the
   decoded cache fills in R1, moving steady runs ~6 GiB closer to the
   trigger).
5. **Why some same-binary windows are clean** (12:17 morning
   187-210s; the 16:48/17:16 headline pair zero-firings): morning was
   pre-width-fix (b88159e — lower allocation rate); the headline pair
   ran the width binary clean, so cycle seizures sit NEAR the visible
   threshold and modest environmental drag (S3/EBS latency, neighbor
   effects — unproven, but now bounded to "tips a marginal seizure
   over") decides whether a window looks clean, draggy, or stormy.
   This dissolves the former "window variance" bucket into a
   measurable quantity: pressure-GC count × seizure duration.

## Next (instrumentation first, then the knob)

1. Ship per-cycle GC visibility into wlogs: `GODEBUG=gctrace=1` on
   workers (stderr → journald → wlogs rides existing plumbing), so
   every window carries assist/STW ms per cycle. Zero-risk.
2. A/B the existing knob on a real window: `-var=gogc=300` (fewer,
   larger cycles; bounded by GOMEMLIMIT backstop) vs 100, judged on
   firing count + pressure-cycle count + q-walls. The binary-default
   arm (`off`) is the third pole if 300 disappoints; the edge regime
   keeps GOGC=100 per the envelope-aware default.
3. Structural lever if the knob plateaus: shrink what each cycle
   marks — the decoded cache's in-heap representation (pointer density
   decides mark cost; measure via the banked .pb.gz pressure profiles
   before touching ADR-0006 territory — the arena rejection stands
   unless these profiles show pointer-dense cache internals).

Amplification path is already fixed (bbdb985: corpse placement +
stuck-clock re-arm), so a seizure now costs seconds, not 30-minute
queries.

## UPDATE 2026-08-15: gogc=off probe — the deeper disease is MemoryHigh==GOMEMLIMIT

Single-arm probe (b32e13b, `-var=gogc=off`, gctrace on, killed after
~1.5 runs — verdict measured, config known-bad; evidence in session
scratchpad `gogcoff-evidence/`, partial benchmark log + per-worker
gctrace/cgroup snapshots):

- gctrace kills the "monster cycle" theory for off-mode pauses: cycles
  at the 20.3GB goal are CHEAP (2-12ms clocks, live 4-5GB) — but they
  run back-to-back during allocation-heavy phases, holding the heap at
  the limit.
- The real cost is the cgroup: worker scopes ran
  `MemoryHigh == GOMEMLIMIT` (20.7GB), and memcg charges heap + mmap'd
  file pages. A gogc=off heap CAMPS at GOMEMLIMIT, so
  memory.current > memory.high permanently: **memory.events
  high=670,494; PSI memory full avg10=10.04%** — the kernel
  direct-reclaim-throttles the worker continuously. Q02 cold 45.6s vs
  11.8s same-binary gogc=100 (~4×). No watchdog firings — the
  throttle is smooth, not seizing.
- This also re-frames the gogc=100 arms: heap oscillates 8→16-17GB and
  crosses the same line at peaks (plus cache file pages) — the memcg
  slab/stock frames in the seizure specimens are this throttle
  contributing to the freeze windows. GC cycles and reclaim throttle
  compose.

**Answer to "what should GOGC be": 100 stays; `off` is measurably
wrong under the current cgroup shape.** FIX SHIPPED (deploy):
MemoryHigh removed from worker scopes — MemoryMax remains the OOM
guard and memcg evicts clean cache pages before killing, which is the
graceful path ADR-0006 wants. Next window (any config) validates:
expect memory.events high ≈ 0, PSI full ≈ 0, and re-judge firing
counts at gogc=100 without the throttle composing; re-open the GOGC
question only if seizures persist with clean PSI.
