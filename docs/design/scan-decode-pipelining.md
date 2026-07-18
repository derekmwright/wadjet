# Scan decode pipelining (parallel row-group decode-ahead)

Status: IMPLEMENTED (PR #228, flag default off). First SF100 pair
(2026-07-16, §8) validated correctness + the scan-band mechanism but
came out WALL-FLAT: source-lifetime cpuToken holds starved concurrent
join fragments (Q20 +54%, Q05 +17%) and ate the scan wins (Q16 −63%,
Q01 −32%). Fixed by per-row-group token accounting (§8.1);
single-arm revalidation pending.

Originally: PROPOSED. Follow-up predicted by
`docs/design/scan-decompress-parallelism.md` §5 ("scan decode becomes
CPU-parallel instead of slot-serial" fixed the *decoder*; the *producer*
stayed serial) and by the morsel cap comment itself
(`executor_fragment.go:461-463`: "past ~8 consumers the single producer
(source decode) is the bottleneck").

Evidence: SF100 streaming-ON profiles `results/arm-b-treatment`
(2026-07-14 pair, main-equivalent bin `5820244`, 2 suites/worker).

## 1. The structure

On the distributed worker scan path, **row-group decode is fully
serial per fragment**:

- `cachedFileStreamSource.Next` yields one decoded row group per call
  from a lazy iterator (`stream_source.go:218-225`); files within a
  scan leg advance one at a time (`openNextFile`,
  `stream_source.go:264`, `:336-337`).
- `scan.RowGroupIter` advances `cur` one row group at a time and calls
  `ReadRowGroupNative` per group (`rowgroup_iter.go:41`, `:160`).
- Morsel consumers do NOT decode. One producer goroutine pulls from the
  source and feeds k ≤ 8 consumers zero-copy views of already-decoded
  batches (`morsel_dispenser.go:40-49`, `executor_fragment.go:334-341`,
  `:507-520`). The cap exists *because* the serial producer saturates
  first.

The only decode parallelism today is **within a single row group**:
the per-column errgroup in `ReadRowGroupNative`
(`columnar_native.go:130-135`, limit = min(#projected columns,
GOMAXPROCS)) and zstd's internal ≤ 16 states (`decompress.go:53-70`,
PR #216). Narrow projections (Q06 reads 4 lineitem columns) leave most
of that width unused, and page decode of row group N+1 never overlaps
the operator chain over row group N beyond what the single dispenser
producer pipeline gives.

## 2. Evidence from the 2026-07-14 streaming-ON pair

All three workers agree within a point (per worker, 2 suites,
~8.9 ks CPU samples on ~3.1 ks wall = **2.9 of 16 cores**):

| Class | Cum CPU | Reading |
|---|---|---|
| parquet zstd page decode (`decompressZstd`) | **~20%** | top real work; sits on the serial producer |
| `runtime.memmove` (flat) | ~17% | split: shuffle-write append ~6%, join gathers ~3.5%, zstd inherent ~4% |
| s2 shuffle encode | ~8.7% | shuffle-write class (candidate B, §7) |
| mallocgc | ~5.4% | |
| fragment-input waits (old #1 block class) | **collapsed** | streaming shuffle read did its job |

The exchange-arrival wait class is gone (PR #225/#226), base-table
fetch is gone (base-table cache arc), the zstd decoder is
CPU-parallel (PR #216) — and utilization still sits at 2.9/16 with
decode as the largest real-work class. The remaining serialization is
structural: **one goroutine's decode throughput bounds every scan
leg**, exactly as the morsel cap comment predicted. NVMe cache hits
make bytes arrive instantly and then re-decode them on one core per
fragment.

## 3. Design

Apply the validated prefetcher shape (filePrefetcher / D2
shuffle-input prefetch: **window + ordered delivery + bypass**) one
level up — from "bytes staged ahead of consumption" to "row groups
decoded ahead of consumption":

- A `decodeAheadIter` wraps the current per-file `RowGroupIter`
  inside `cachedFileStreamSource`: k decode workers pull row-group
  indices (file-ordered), each runs the existing
  `ReadRowGroupNative` for its group, results deliver **in source
  order** by sequence number. The consumer-facing `Next` contract is
  byte-identical to today — in-order delivery means no downstream
  semantics change at all, independent of sink order-tolerance.
- **Cross-file continuation**: when the tail of file F is in flight,
  workers continue into file F+1 (already NVMe-staged by
  scanPrefetch; opening its reader early is metadata-cheap). Without
  this, files with few row groups cap the win. The mmap-release
  discipline in `stream_source.go:225-233` moves from "release when
  iterator exhausts" to "release when the last in-flight group of
  that file has delivered" (refcount per file, broadcastJoinCache
  pattern).
- **Byte-bounded window**: today "only one row group's worth of
  memory is live at a time per source" (`stream_source.go:216-217`).
  Decode-ahead relaxes this to W bytes of decoded batches in flight,
  estimated from row-group metadata (TotalByteSize), default
  256 MiB per fragment source (same order as the scanPrefetch byte
  window; the morsel dispenser's 512 MiB budget already bounds the
  next stage downstream). Window full ⇒ workers block; consumer
  drains ⇒ they resume. Estimation error is bounded by one row group
  per worker.
- **CPU-token integration**: decode workers beyond the first draw
  from the same `cpuTokens` budget as morsel consumers
  (`morselFragmentWorkers` shape: non-blocking TryAcquire, degrade
  toward serial under exhaustion, k=1 ⇒ exactly today's behavior).
  This keeps fragments×workers from oversubscribing 16 vCPUs — scan
  decode and morsel consumption compete for the same physical cores
  and must share one budget.
- Scope v1 = the worker DAG scan path (`cachedFileStreamSource`).
  The single-process/fastpath scanner (`scanSourceInner`,
  planner/physical/util.go) has a different shape and its own arc if
  the profile ever ranks it.

## 4. Alternatives considered

- **Transcode cached base tables to a cheaper codec** (snappy or
  uncompressed on BaseTableCache insert). Rejected for v1: it shrinks
  the serial section instead of removing the serialization (utilization
  stays producer-bound); it only helps cache *hits* (cold S3 and
  non-cached deployments keep full-cost decode); the cache tee is
  synchronous on the read path (`base_table_cache.go:512-542`) so it
  needs a background transcode worker; and it puts the parquet
  *writer* on the critical data path with the full
  parquet-safety-critical burden (the writer can only emit
  uncompressed/snappy/zstd/gzip today, `file_writer.go:1340-1369`,
  and the existing reader→writer round trip is the row-based
  compaction path, `compactor.go:317-369`, unproven for this use).
  Decode pipelining composes with it if decode CPU ever ranks #1
  again *after* the serialization is gone.
- **Out-of-order delivery to the dispenser**: sinks tolerate it on the
  DAG path (morsel consumers already interleave,
  `docs/design/morsel-execution.md` §sink-concurrency), but it buys
  little over an ordered window of the same byte size and forfeits the
  "no semantics change anywhere" property (SMJ, if ever revived, needs
  order). Deferred; the sequence-number plumbing makes it a flag-flip
  later if profiles justify it.
- **Raise the per-column errgroup width / row-group size tuning**:
  helps only wide projections / does nothing for the overlap
  structure. Narrow-projection scan legs (the TPC-H scan band) stay
  serial. Rejected as the primary lever.
- **Decode inside morsel consumers** (consumers pull row-group indices
  instead of decoded batches): erases the producer/consumer split the
  morsel arc is built on (clone/merge machinery assumes decoded input
  views), and couples decode width to op-chain width — they have
  different optimal k. Rejected.

## 5. Flags and rollout

- `--scan-decode-ahead` (bool, default **false** until the SF100 pair;
  kill switch thereafter — the streaming-shuffle-read arc convention).
  Gates the decodeAheadIter + cross-file continuation together.
- `--scan-decode-ahead-bytes` (window, default 256 MiB) — sizing knob,
  not expected to need tuning (no threshold-tweak campaigns; if the
  default is wrong the design is wrong).
- Terraform var `scan_decode_ahead` mirroring the flag for the A/B.
- Markers from day one (§8 convention): per-worker counters
  `ScanDecodeAheadGroups` / `WindowFullStalls` / `TokenDegrades` +
  per-fragment chosen k; DEBUG line per fragment on degrade-to-serial.

## 6. Slices and gates

- S1: decodeAheadIter (in-file window, ordered delivery, byte budget)
  behind the flag; unit tests = ordered-delivery contract, window
  accounting, error propagation (decode error on group N surfaces at
  the consumer exactly where serial would), shard interaction
  (`SetShard` ranges, `columnar_native.go:63-79`).
- S2: cross-file continuation + mmap refcount release; tests for file
  lifetime (batch from file F alive while F+1 decodes), release-order
  regression (the 2026-05-22 mmap-lifetime comment in
  `stream_source.go:225-233` becomes a test).
- S3: cpuTokens integration + degrade; `-race` on the whole feature
  (this is made of data races waiting to happen — morsel gate list
  applies verbatim).
- Gates in order: full unit + worker `-race`; SF0.01 22/22; `tpch-harness
  --mode=local` both flag states, rows + checksums identical, plus
  DAG-forced arm (`--local-fastpath-bytes=0`); SF100 same-window pair
  (needs deploy approval), benchmark_runs=2 both arms, block+CPU
  profiling both arms. Decisive markers in order: **utilization vs
  2.9/16 ref** → decode-ahead engagement counters → scan-band wall
  (Q01/Q06/Q12/Q14/Q15/Q19) → suite wall LAST. Success = utilization
  and scan band move together; wall flat + utilization up = re-rank,
  not failure (`feedback_no_revert_on_serial_clog`).

## 7. Honest bounds and what this does NOT fix

- The 20% decode share is CPU, not wall; the wall claim is structural
  (scan legs stop being bounded by one core's decode throughput) and
  only the SF100 pair prices it. Queries whose scan legs are already
  short (exchange-dominated Q17/Q18) should move little.
- The **shuffle-write class (~16% combined: row-wise
  `appendBatchRowsBulk`/`SetFrom` ~6%, s2 encode ~8.7%, bufio ~1%) is
  untouched** — that is candidate B (bulk bytes scatter: run-detection
  over ascending per-partition rows + the existing `BulkCopy`
  primitive, `vector.go:144-152`; the unpartitioned no-Sel path is
  already a dense 0..n range and a single BulkCopy per column,
  `unpartitioned_stage_sink.go:212-215`). B is a separate, smaller,
  micro-bench-gated arc (`feedback_no_ab_on_architectural_perf`
  shape); its null-bitmap edge is the c0c58ea class and needs
  null-aware run detection.
- Join-gather memmove (~3.5%) and mallocgc (~5.4%) are untouched.
- Memory: +W bytes decoded-ahead per active scan fragment is real heap
  the ledger does not charge today (same accounting posture as the
  morsel dispenser's 512 MiB). The window default keeps
  fragments×window inside the envelope on c7gd.4xlarge; a
  memory-pressure collapse hook (drop to k=1, drain window) is listed
  in S3 and must be in place before default-ON is proposed.

## 8. SF100 pair results (2026-07-16) and the token fix

Same-window pair on main 983d1ae (control `results/20260716-222612`,
treatment `results/20260716-232648`), benchmark_runs=2 both arms,
block+CPU profiling both arms, 3× c7gd.4xlarge, base cache 150 GB.

- **Correctness**: 0 row mismatches across all 88 query executions.
- **Engagement**: groups ≈ 38-40k/worker/2 suites; fallback-free.
- **Scan band moved exactly as designed** (steady-state run 2):
  Q16 −63%, Q01 −32%, Q06 −27%, Q12 −24%, Q15 −22%, Q02 −20%.
- **Join-legged regressions ate the wins**: Q20 +54%, Q05 +17%,
  Q22 +11%, Q07 +9%, Q03 +8%. Suite wall FLAT (26m53s → 27m13s,
  +1.2%); utilization FLAT (2.85 → 2.84 of 16 cores).
- **Conviction**: `window_fulls ≈ 35-41k` on ≈ 40k groups — decode
  workers spent most of their lives stalled on a full window while
  HOLDING source-lifetime cpuTokens (`token_degrades 211-235/worker`
  = the pool was drained), narrowing concurrent join fragments'
  morsel width. Decode-ahead moved wall between bands instead of
  adding compute.

### 8.1 Fix: per-row-group token accounting

Tokens are acquired per DECODE, not per worker lifetime: a worker
takes one token at admission of a non-cursor group and releases it
when the decoded group parks. Workers stalled on the window, on
pressure, or between groups hold nothing; the delivery-cursor group
stays token-exempt (serial progress always allowed — the morsel
"first consumer free" rule). Hoarding is structurally impossible;
decode width becomes adaptive to actual token availability per group.
Marker `token_degrades` is replaced by `token_stalls` (per-group
admissions deferred; the group still decodes, serially at worst).

Revalidation: single treatment-only SF100 run (control reference =
`results/20260716-222612`), per the cost call on full A/Bs.

## 9. Q05/Q07 residual: window occupancy under memory pressure (2026-07-17)

Local reproduction (no EC2): SF10, DAG-forced, Q05 only, under
`deploy/edge/cap-wrapper.sh` (EDGE_CAP_MB=2048, EDGE_CPUS=8). Uncapped
the regression does not exist (decode-ahead slightly faster); capped it
reproduces hard and scales with the WINDOW, not the machinery:

| arm (medians, n=4-6)      | wall   |
|---------------------------|--------|
| flag off                  | 36.2s  |
| window=1 (serial decode)  | 32.8s  |
| window=32 MiB             | 49.4s  |
| window=256 MiB (default)  | 56.4s  |

The law: cost ∝ decoded-bytes-in-flight under memory pressure. With
window=1 — cursor-only admission, every other mechanism (workers,
cross-file pre-open, per-group tokens) still active — decode-ahead
BEATS serial even under the cap. The held batches displace the memory
the scan itself needs (page cache for the mmap'd file bytes, and GC
headroom under GOMEMLIMIT); `heapPressureActive` never fires because
Go heap never exceeds its target — the kernel absorbs the pressure
silently. This is exactly §7's "real heap the ledger does not charge"
landmine, and it is the SF100 Q05 (+13.5%, scan/repartition phase) and
plausibly Q07 (+8.5%, mid-join) residual: c7gd workers run at partial
page-cache residency (26 GB working set vs ~16 GB cache).

### 9.1 Fix direction (needs review before implementation)

Charge decoded-ahead window bytes to the task's memory ledger and
derive the effective occupancy cap from actual budget headroom, with
the existing cursor-exempt admission as the floor (serial decode-ahead
is SAFE — measured faster than the flag-off path even under a 2 GiB
cap). Properties: no new thresholds (budget-derived); reuses the
validated ledger/pressure machinery; preserves full width when memory
is plentiful (the SF100 scan band keeps Q16 −64% / Q06 −42%); collapses
toward the measured-safe serial shape as headroom vanishes.

Alternatives considered: fixed smaller default (32 MiB still +37%
under the cap — size alone is not safe, and it forfeits width
elsewhere); width-sized window (k×group estimate ≈ 32 MiB case —
same result); page-cache-aware pressure sensor (OS-specific, and the
ledger route subsumes it).

Honest bound: the exact displacement channel at SF100 (page-cache
re-reads vs GC-cycle frequency) is not isolated — the capped repro
cannot distinguish them. The fix's SF100 pair is the test; markers to
watch: decode-ahead ledger charges/denials, GC cycle counts, scan-band
retention, Q05/Q07 vs the 20260716-222612 control.

### 9.2 Implementation and what the capped repro falsified (2026-07-17)

Both §9.1 pieces are implemented on `fix/decode-ahead-ledger-window`:

**Ledger charge (e2e80a6).** `DecodeWindow` carries an optional
`scan.WindowLedger` (satisfied directly by `*memory.Tracker`); the
worker attaches its shared pool tracker. Non-cursor admission must
clear the window ceiling, the pressure hook, `ledger.Reserve`, and a
cpu token (token denial rolls the reserve back); the delivery-cursor
group is force-charged but never denied. Inflight and ledger move in
lockstep, balancing to zero per source on delivery and Close. New
marker: `ledger_stalls`.

**What the same-window capped repro then falsified**: §9.1's claim
that the ledger route subsumes a page-cache sensor. Interleaved arms
(A = off, B = main + flag, C = ledger + flag; EDGE_CAP_MB=2048):
A 36.9 s, B 55.2 s (+49 %, regression reproduced), C 48.5 s (+31 % —
partial) with `ledger_stalls=0`. The pool auto-sizes to
goMemLimit − cache = 1.45 GiB under the cap, so a 256 MiB window never
draws a denial: the pool bounds OPERATOR co-tenancy (its real job, and
the charge stays — at SF100 the 5.4 GiB pool carries multi-GB join
builds, a topology the capped rig cannot reproduce), but no Go-side
ledger sees the kernel-level displacement.

**Refault-rate sensor (5a35fdf).** The kernel publishes the
displacement directly: `workingset_refault` counts faults on
recently-evicted pages — thrash by definition; cold sequential reads
are first-touch faults and do not count. Measured: ~0/s quiet baseline
vs 15k–95k pages/s during capped thrash — four orders of magnitude.
`memory.PageCachePressureActive()` samples the process's cgroup-v2
`memory.stat` (host `/proc/vmstat` fallback; source absence disables)
at 1 s cadence; active when the rate exceeds 1000 pages/s (~4 MB/s,
env-overridable) on two consecutive samples. Decode-ahead admission
consults it alongside the Go-heap gauge — no iterator changes; the
existing pressure path collapses to the cursor-exempt serial floor.
Safe on both ends: under thrash, width is worthless anyway (the
I/O-bound repro logged `window_fulls=0` — decode cannot outrun a
saturated disk); on a healthy box the sensor is silent. Markers:
`refault_rate`, `refault_activations`.

**Environment caveat for future capped repros**: the rig only
discriminates when the flag-off arm shows a LOW refault rate (dataset
host-cache-warm). After a host reboot every arm ran ~90k refaults/s
and A ≈ B ≈ C — saturated disk thrash masks the window effect
entirely. Check the flag-off refault rate before trusting any arm.

**Residual honest bound** (one cell no local rig can test): SF100
partial-residency background refaults co-occurring with a productively
full window — the sensor would trade scan-band width for cache relief.
The SF100 pair remains the test; watch scan-band retention
(Q16/Q06/Q12) alongside Q05/Q07, `refault_activations`, and
`ledger_stalls`.

**Sensor visibility bound (measured 2026-07-17)**: cgroup-v1 memcg
reclaim does not feed the global `workingset_refault` counters on the
WSL2 5.15 kernel — a capped v1 container thrashing 1.5 GiB of its own
file pages through a 512 MiB limit moved the counter by ~0
(`cmd/refault-probe --thrash` reproduces this). The sensor is therefore
blind to v1-container-INTERNAL displacement, which is exactly the
cap-wrapper simulator's shape — so the capped rig cannot fire it
end-to-end. This is a simulator artifact, not a product gap: SF100
workers run bare on cgroup-v2 AL2023 (host-level reclaim feeds
/proc/vmstat AND the v2 memory.stat both), and real edge boxes are
bare small machines or v2 containers. Fail-safe by construction:
counter source absent or silent → sensor inactive → current behavior.
`cmd/refault-probe` prints raw counter + sensor state side by side and
generates workingset-shaped thrash for field diagnosis; the sensor
logs its bound source at init.

### 9.3 SF100 pair results (2026-07-17, bin 3d5be22): steady-state −7.3 %

Same-window pair, fresh cluster per arm, benchmark_runs=2, block+mutex
profiling, 0/44 row mismatches. Control results/20260717-221138,
treatment results/20260717-232056.

| | cold-with-cache | steady-state |
|---|---|---|
| control (flag off) | 26.2 m | 28.1 m |
| treatment (ledger+sensor) | 26.7 m (+2.0 %) | **26.0 m (−7.3 %)** |

Best decode-ahead suite result of the arc (pre-fix pair: −3.6 %).
Steady-state per-query: Q08 −54 %, Q06 −43 %, Q01 −36 %, Q02 −19 %,
Q21 −15 %, Q20 −13 %, Q19 −13 %, **Q07 −9.7 % (residual FIXED**, was
+8.5 %); regressions: **Q05 +19.2 %** (117.0→139.5 s; the one
survivor — note today's control Q05 ran unusually fast, vs the
2026-07-16 control it is +8.9 %), Q14 +12.6 %, Q09 +7.6 %.

Sensor field data (first empirical confirmation of the §9 displacement
theory at SF100): all three workers bound the per-scope cgroup-v2
memory.stat (workers run under systemd-run MemoryMax scopes) and
sustained **22k–50k refaulted pages/s** in steady-state — the partial
page-cache residency is real and continuous. Markers per worker:
~39–40k groups, refault_activations 35–45, pressure_stalls ~170k
(sensor actively collapsing width), window_fulls 6–7.6k,
token_stalls 18–28k, **ledger_stalls = 0** — at SF100 the 5.4 GiB pool
never binds either; the fixed ceiling plus the sensor do all the
bounding. The ledger charge remains correct co-tenancy accounting (and
is what spill decisions see), but denial-based collapse is a no-op at
both tested scales.

Q05 residual reading: the sensor collapse did not fix Q05's
scan/repartition phase and may partially throttle it (the §9.2 honest
bound — background refaults + productive width co-occur there), while
the suite-wide effect of yielding width under refault pressure is
strongly positive (−7.3 %). Q05 is now the isolated remainder:
phase-local, instrumented (per-query refault/stall attribution is the
next lever if it is pursued), and no longer masks the aggregate win.

Rollout: flag default remains the PM call — the aggregate case for
default-ON is now strong (steady −7.3 %, 0 mismatches across four
SF100 suites, kill switch retained); the counter-case is Q05 +19 %.
