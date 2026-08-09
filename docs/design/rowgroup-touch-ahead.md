# Row-group touch-ahead (forced page-in behind WILLNEED)

## Problem

rowgroup-readahead.md left the steady regime at 1.47× cold (its own
verdict's residual), with two candidate explanations: decode spans
stretched by inline faults the stall counters can't see, or a raw NVMe
bandwidth floor. Neither was measurable with the instrumentation of the
time — the stall-duration counters only observe **parked waiters**,
while a decode worker that keeps its CPU token and faults inline
reports nothing anywhere.

## Diagnosis (2026-08-08/09)

Two new instruments (commit cad7401):

- **Decode spans**: wall-ns inside `ReadRowGroupNative` + projected
  compressed bytes per decoded group (`DecodeAheadIter.DecodeSpans`),
  folded per query and lifetime (`decode_ms` / `decode_bytes` wlog
  keys). ns/byte over an identically-shaped hot run isolates inline
  fault time.
- **proc io stats** marker: cumulative majflt/minflt
  (`/proc/self/stat`), process storage-layer bytes (`/proc/self/io`),
  and whole-device nvme bytes (`/proc/diskstats`), every 60s and at
  shutdown (both Stop and Drain — 78af3f5).

A zero-EC2 steady-regime testbed (tpch-harness `--runs=2`, 3ad381b,
SF10 under 2 GiB docker caps via cap-wrapper) reproduced the
signature: run-2 decode +23% ns/byte, token stalls 14×, majflt
climbing all run — **with WILLNEED fully engaged**, at read rates far
below the device. The SF100 off-arm confirmed at scale: run-2 decode
ns/byte 10.0 → 19.1 (+91%), while nvme reads ran ~26 MB/s/worker
against a ~2.8 GB/s device — **the bandwidth floor is refuted**; the
residual is fault latency. Under a saturated page-cache LRU the kernel
throttles or skips advisory readahead, and advised pages can be
re-evicted before decode reaches them; MADV_WILLNEED cannot promise
residency.

## Mechanism

`worker.rangeToucher` (rowgroup_touch.go): a per-mmap goroutine fed
the same advise ranges as the WILLNEED closure. It physically faults
each range in (one byte read per page) behind the advisory readahead —
a residency guarantee the kernel cannot throttle away. It is
deliberately **outside the CPU-token budget**: page-fault wait is I/O,
not compute, and the entire point is removing that wait from
token-holding decode spans.

- Enqueue never blocks: a full queue (1024 ranges) degrades to
  WILLNEED-only for that range, counted in `touch_drops`.
- `stop()` abandons the backlog immediately and joins the goroutine;
  it is sequenced after `iter.Close` (which joins the enqueueing
  decode workers) and before munmap on both release paths.
- Prune-aware advises (bbf6c6d): the advise wave runs the same
  metadata-only range/bloom checks decode's `pruneGroup` applies, and
  withholds ranges for groups that will prune. Under WILLNEED alone,
  advising those was merely useless; the toucher made it forced NVMe
  I/O + page-cache displacement for groups no decode ever reads — the
  SF100 pair's single regression (Q17, the heaviest dyn-filter pruner,
  +96% steady) before the skip.

## Observability & kill switches

- `touch_bytes` / `touch_drops` beside `readahead_advise_bytes` on the
  drop-behind stats wlog lines. Healthy engagement:
  touch_bytes ≈ readahead_advise_bytes, drops ≈ 0.
- `WADJET_ROWGROUP_TOUCH=0` — toucher off, WILLNEED-only (the
  pre-touch behavior). Forwarded by cap-wrapper.sh and terraform
  (`-var=rowgroup_touch=0`).
- `WADJET_ROWGROUP_READAHEAD=0` — kills the whole advise seam
  including the toucher.

## SF100 verdict (2026-08-09, same-window kill-switch pair, bin 80ee83b)

Pair 20260809-003952 (off) / 010133 (on), runs=2 each, wlogs grabbed
both arms, all EC2 destroyed.

- **Steady −17.6%**: 536.4 → 441.9s; steady/cold ratio 1.483 → 1.242.
- **Cold in-band**: 361.8 → 355.8s (−1.7%).
- **Movers broad**: Q07 −51%, Q18 −47%, Q22 −41%, Q05 −30%, Q08 −28%;
  14 of 22 queries ≥ −13%.
- **Mechanism markers**: touch_bytes == advise_bytes (~170 GB/run
  across workers, 0 drops); run-2 majflt 638k → 180k (−72%); run-2
  nvme reads 41.8 → 30.9 GB. Decode ns/byte did NOT drop (18.8 vs
  19.1) — with residency guaranteed the pipeline runs hotter and
  decode spans queue on CPU instead (window-fulls up, wall down):
  ns/byte discriminates only when CPUs are idle; majflt + wall is the
  authoritative pair at full utilization.
- **Correctness**: rows + value sigs identical 44/44 across arms.

## SF100 third arm (bbf6c6d) — variance calibration + a finding

A third arm (touch on + the prune-aware advise skip) measured cold
326.9s / steady 501.7s. Its wlogs corrected two things:

- **The prune-skip is INERT on the EC2 SF100 path**: total advised
  bytes are byte-identical across all three arms (342,779,905,650) —
  the iterator-level dynamic filters never attach on this path, so
  neither the skip nor iterator prune checks fire. (They DO attach on
  the local harness path: the SF10 capped runs pruned ~6k groups and
  the skip's regression test passes.) The bbf6c6d commit message's
  attribution of Q17 to forced prunable-group I/O is therefore WRONG.
- **Tonight's window variance is elevated**: arms B and C are
  identical code, 30 min apart, and measured steady 441.9 vs 501.7s
  (±7% suite-level; Q17 alone swung 38.2 → 20.3s, Q18 31 → 53s). The
  honest wall claim is therefore: steady improvement in the −6% to
  −18% band (both on-samples beat the off arm), −17.6% in the clean
  same-window pair. The mechanism claim (majflt −72%, forced
  engagement, bandwidth-floor refutation) is code-caused and does not
  depend on the wall samples.

The prune-aware skip stays (sound, tested, engages where iterator
filters attach). **Follow-up (RESOLVED 2026-08-09)**: iterator-level
dynamic filters never attached on that path because attach-on-arrival
(the default consume mode) delivered resolved blooms to the ROW level
only — deferred specs skip `materializeDynamicFilters`, and nothing
routed the late bloom to the iterator layer (shuffle tasks ignored
deferred specs entirely). Fixed by full-layer delivery (e413f37); the
SF100 A/B then showed the deeper truth: with delivery fixed, group
pruning STILL fires zero times at SF100 — TPC-H join keys are uniform
across row groups, so group stats carry no selectivity. The 110k
decoded-group constant was stats-powerlessness, not recoverable wall.
See docs/design/attach-on-arrival-dynamic-filters.md §Full-layer
delivery for the full verdict.
