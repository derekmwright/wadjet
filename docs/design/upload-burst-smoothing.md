# Outbound burst smoothing: pacing background PUT bytes below the NIC allowance

Status: implemented 2026-08-13; **SF100-validated same day (§6) and
pinned at 150 MB/s in the SF100 bench profile**. Engine default stays
env-off (`WADJET_UPLOAD_PACE_MBPS`, terraform `upload_pace_mbps`; 0
reproduces unpaced baselines).

Barrier-overlap arc step 2. Evidence base: the 2026-08-13 ENA×stage
attribution (`deploy/benchmark/attribute-ena.py`, results/20260813-012354
and the eagerpair2 arms).

## 1. The measured problem

c7gd.4xlarge workers have a burst NIC ("up to 15 Gbps") whose measured
baseline allowance is ~1.875 Gbps. The ENA×stage-timeline attribution
answered where `bw_out_allowance_exceeded` events actually land:

- **68% of ~661K out-exceeded events sit inside join-stage windows**
  (barrier gaps + no-query time own only ~7%). The bursts fire DURING
  stages, not between them.
- The mechanism is the **completion wave**: when a task wave finishes, up
  to 8 background upload streams start simultaneously (compress + PUT at
  full speed), peaking ~2.5 Gbps against the 1.875 allowance — top single
  interval 59K exceeded events/10s.
- The ENA clamp that follows throttles the **whole NIC**, not just the
  PUTs: peer-fetch streams (consumer critical path), NATS heartbeats and
  task progress (liveness — see the dispatch-stall arc's silent-network
  family), and result gathers all degrade during the clamp.
- Meanwhile the PUTs being protected are **pure insurance**: ADR-0007
  measured ~425 GB of stage-output PUT per SF100 suite pair with zero
  bytes ever read back. Averaged over a pair the PUT rate is ~60-90 MB/s
  per worker — far under any sane cap. Only the peaks are the problem.

## 2. Mechanism

A worker-global token bucket (`bytePacer`, `internal/worker/upload_pacer.go`)
charged by **PUT-body wire bytes** (post-compression). The existing v4
`governedReader` already chunks every background upload stream at 1 MiB;
the PUT-body instance (and only that instance) post-charges each chunk
and sleeps off any debt. Refill rate = `WADJET_UPLOAD_PACE_MBPS`; burst
capacity = 250 ms of rate.

What is deliberately **not** paced:

- **Compression-source reads** — s2 can run ahead of the wire; pacing CPU
  buys nothing and stretches drain time.
- **Synchronous uploads** (`jobYieldNs == nil`: scalar-subquery outputs,
  adoption-failure fallback) — the query is blocked on them; they were
  never governed and stay ungoverned.
- **Urgent roots** (demand release via `SubjectUploadRelease`) — a
  consumer or the coordinator is waiting on the durable copy; stretching
  it converts smoothing into barrier latency (the q18 +6s trap from the
  QoS v4 arc).
- **Peer-serve streams** — consumer critical path, different subsystem.

Progress is guaranteed by construction: the pacer has no freeze state
(the v1-QoS 209s trap), per-chunk sleep is bounded by chunk/rate (~6.7 ms
at 150 MB/s), and aggregate PUT throughput is exactly the configured
rate.

## 3. Why this shouldn't cost barrier time

Under streaming exchange (ADR-0004) consumers fetch stage outputs from
the producer's local disk over gRPC; S3 durability is background. The
paths where anyone actually waits on a PUT all bypass the pacer (sync
uploads, urgent releases). The exposure that IS lengthened: the
fault-tolerance window during which outputs exist only on the producer
(`producer dead before upload landed`). At 150 MB/s a typical 390 MB
completion wave drains in ~2.6s instead of ~1.2s of clamped chaos —
an acceptable trade, and reap-grace work (stall arc) addresses that
window independently.

## 4. Prior art and why it isn't this

- **Upload QoS v1-v4** (`docs/design/upload-foreground-qos.md`): protects
  the NEXT query's foreground scans from the PREVIOUS query's drain via
  time-windowed pauses (CPU/NVMe contention). v4 is inert in sequential
  suites (boundary uploads get cancelled; in-window uploads are the
  window-root's own and exempt). Smoothing is orthogonal: it acts DURING
  the producing query's own windows, on wire rate, not on windows.
- **shuffle-durability=lazy** (ADR-0007): removes the PUTs entirely —
  measured NEGATIVE (deferred-release re-reads tripled inbound throttle;
  page-cache side effects). Smoothing keeps eager durability, just
  spreads it.
- **Bigger NICs** (c7gn): ruled out as a lever by bench-consistency
  rails; the throttle-tax control arm remains a PM call.

## 5. A/B protocol (needs deploy approval)

Control `upload_pace_mbps=0` vs treatment `=150`, adjacent arms, runs=2,
judged by: (a) rows + vsig identical; (b) `upload_pace_wait_ms` > 0 in
treatment wlogs (engagement) with `upload_failed=0`; (c) ENA out-exceeded
totals and burst peaks down in join windows (attribute-ena.py); (d)
walls — R1 vs R1 primary if any stall contaminates an R2. Success looks
like: out-exc collapse in join signatures with neutral-or-better walls.
Failure mode to watch: drain backlog at query end (pace too low) —
`upload s3 done` timestamps stretching past query completion into the
next window, which would recreate the v1-QoS backlog snowball at the
wire layer. 150 MB/s sits ~2x above the measured average PUT rate, so
backlog growth means the rate is mis-set, not the mechanism wrong.

## 6. A/B result (2026-08-13, bin d2036be, adjacent arms, runs=2)

Control `upload_pace_mbps=0` (results/20260813-195410) vs treatment
`=150` (results/20260813-201428), fresh on-demand cluster each,
destroyed + EC2-zero verified.

- **Correctness**: rows 88/88 and vsig identical; `upload_failed=0`
  both arms; upload-cancelled counts comparable (no drain-backlog
  signal — the §5 failure mode did not appear).
- **Engagement**: `upload_pace_wait_ms` = 56.1s / 93.6s / 69.1s per
  worker (0 in control) — the pacer slept off real debt all suite.
- **ENA (the designed judge)**: `bw_out_allowance_exceeded` **−30% at
  equal bytes** (735K → 517K per arm, ~231 vs ~222 GB tx). By
  signature: join-window rate 365→312/s (−14%), scan 308→188/s
  (−40%), no-query 209→55/s (−74%). The completion-wave peaks are
  exactly what got clipped.
- **Walls: neutral-or-better within contamination.** Pair −3.6% raw.
  Each arm had one contaminated run in opposite directions: control R2
  showed the recurring cross-arm R2/R1 creep (1.62, broad, no single
  stall); treatment R1 absorbed a frozen-spin episode (Q20-R1 135.7s,
  stall-arc specimen 8 — full stack dump shipped by the new wlog
  uploader). Treatment's clean R2 = 301.3s, the fastest R2 recorded on
  this config (prior best 378.1).
- **Hypothesis logged, not claimed**: the unattributed cross-arm R2/R1
  creep (1.08→1.6+ across 08-12/08-13 controls) did not appear in the
  paced arm (R2/R1 0.686). If the creep is throttle/backlog state
  accumulating across runs, pacing removes its cause. Discriminator:
  R2/R1 on future paced arms.

VERDICT: §5 success criteria met (out-exc collapse, correctness
perfect, walls neutral-or-better) — pinned at 150 MB/s in the SF100
bench profile; engine default stays env-off. Every future SF100 arm
doubles as a confirm run; revisit the rate only with ENA evidence
(e.g. if peer-serve growth eats the 84 MB/s headroom).

## 7. Future

- Rate from instance metadata (baseline allowance lookup) instead of a
  knob, once validated.
- Coordinator-side pacing for result/journal writes if attribution ever
  implicates them (today they're noise).
- Foreground-aware dynamic rate (pace harder while peer-serve is active)
  only with evidence — static first, per the no-over-engineering rule.
