# Shuffle decode-ahead (parallel WSHF chunk decode)

Status: IMPLEMENTED (2026-08-15, this arc). Flag `--shuffle-decode-ahead`,
default true; `=false` is the kill switch restoring the serial streaming
reader unchanged.

Predicted by the width-plateau attribution
(`docs/benchmarks/q08-width-plateau-attribution-2026-08-15.md`): q08
join-6's effective width plateaus at 8.7/3.7 of 15 admitted consumers
with dry-wait at 5–10.6 widths, token wait ≤0.7, and
`dispenser_producer_wait_ms = 0` — the single-threaded producer
(`src.Next`, probe-input WSHF decode, ~40 parents/s) is the fragment's
pacer. The parquet side of the same structure was fixed by
`docs/design/scan-decode-pipelining.md`; this memo is that design applied
to the WSHF chunk path, which today decodes fully serially in the morsel
producer goroutine whatever the consumer width.

## 1. The structure

All production WSHF consumption flows through `streamingShuffleReader`
(`--streaming-shuffle-read` default true routes transport bodies there;
`WADJET_SHUFFLE_PREAD` default on routes local staged/cached files there
too — the mmap `shuffleChunkReader` survives only behind that kill
switch). Its `Next` is two phases per chunk:

1. **Stage** — `readChunkBytes`: walk the chunk's length-prefixed column
   segments off the stream into scratch. Sequential I/O + memcpy; this is
   also where chunk boundaries are discovered (a WSHF chunk's extent is
   only known by walking its column headers).
2. **Decode** — `batch.NewRecordBatch` + the `readColumnData` column
   loop: bitmap copies, fixed-width memcpys, BytesColumn append, decimal
   transform. Allocation- and memmove-heavy; the dominant cost.

Both phases run on the one producer goroutine, so k morsel consumers
starve behind one core's staging+decode throughput. Warm runs starve
HARDER (probe processing gets faster; decode does not — task B eff 3.7).

## 2. Design

Split the two phases at the seam they already have:

- **Scanner (one goroutine)** owns the stream exactly as today: reads
  row-count words, stages each chunk's raw bytes via `readChunkBytes`
  into a **pooled per-chunk buffer** (instead of the shared scratch),
  and emits one slot per non-empty chunk — to an ordered delivery queue
  (consumer side) and a work queue (decode side).
- **k decode workers** take slots and run the exact serial decode
  (`decodeShuffleChunk`, extracted so the two paths cannot diverge),
  parking the batch in the slot. Workers never touch the stream and
  never block on anything but the work queue.
- **`Next` delivers strictly in order** (slot FIFO, wait on each slot's
  done channel). Same batches, same order, same error at the same
  position as the serial reader; `Delivered()` (the transport-fallback
  skip count) counts delivered batches exactly as before.

Admission control mirrors the validated scan-decode-ahead rules:

- **Byte window** (`shuffleDecodeAheadWindowBytes`, 128 MiB): staged +
  decoded-but-undelivered bytes, charged at exact staged size (no
  estimation — WSHF is uncompressed, decoded ≈ wire bytes). The chunk at
  the delivery cursor is always admitted (an oversized chunk alternates,
  never deadlocks). Downstream, the morsel dispenser's 512 MiB budget
  bounds the next stage as before.
- **CPU tokens, cursor-exempt, per chunk** (the §8.1 lesson taken as a
  birth defect to avoid, not a fix to repeat): the scanner acquires one
  token per non-cursor chunk before dispatching it; the worker releases
  it when the decoded batch parks. No goroutine ever holds a token while
  stalled. Token exhaustion degrades to cursor-only staging = today's
  serial alternation.
- **Pressure** (`scanDecodeAheadPressure`, the same Go-heap +
  refault-sensor hook, occupancy-floored with the same strict/edge
  variant): under pressure non-cursor admission waits for deliveries.
  The held bytes here are the same displacement class the §9 arc
  measured; there is no reason WSHF bytes would be exempt.

GC-safety is preserved by construction: the frozen-spin fix moved WSHF
decode off mmap so no decode span faults on file pages
(`shuffle_pread.go`); the scanner still does all file I/O through read
syscalls (GC-safe park points) and workers decode pure heap buffers.
Extent-parallel decode over an mmap — the obvious alternative — would
have reintroduced exactly the fault-inside-decode-span class that
produced the 2026-08-11/12 SIGABRTs, on more threads.

Engagement: both `streamingShuffleReader` construction sites
(`openShuffleStreaming` for transport bodies, incl. the WSHC/WSHZ
compressed framings — the codec stream is sequential and stays on the
scanner; `openShuffleFromFileStreaming` for local pread), gated on
`numChunks >= 4` (nothing to overlap below that; keeps the gather/reply
class serial for free). Workers default 4 (`GOMAXPROCS` cap); width is
governed at runtime by the token pool, so the constant is a ceiling, not
a promise.

## 2.1 Pressure regime amendment (2026-08-15, post-validation window)

The first SF100 window (memo
`shuffle-decode-ahead-sf100-2026-08-15.md`) measured
`pressure_stall_ms` ≈ 92s/worker against `window_full_ms` 5.7s — the
refault channel was denying admission to a near-empty window, the
scan memo's §9.4 pathology reborn on the WSHF path. At SF100 partial
residency the refault rate is ambient (dataset vs cache) and WSHF
admission holds nothing worth shedding: exact-charged bytes ≤128 MiB
per reader, retired within one probe pass.

`shuffleDecodeAheadPressure` therefore drops the refault channel on
non-edge envelopes: the Go-heap tide gauge always binds (staged chunks
are our own heap), and strict/edge envelopes keep the full coupling
(the capped repro measured even one extra in-flight unit harmful
there). `WADJET_SHUFFLE_DA_REFAULT=1` is the same-binary kill switch
restoring the coupled behavior. The parquet scan windows keep their
sensor unchanged — their units are 10–20× larger and their
displacement was measured (§9.5).

## 2.2 Producer token donation (2026-08-15, post-exemption window)

The pressure-exemption window (memo
`shuffle-da-pressure-exemption-2026-08-15.md`) re-attributed the warm
join-6 dry-wait to a token **priority inversion**: with the refault
channel gone, non-cursor admission is denied almost exclusively by
`cpuTokens.TryAcquire`, and §4.2.1's "queued waiters beat TryAcquire"
rule is exactly backwards on a producer-starved fragment. Consumers
park as FIFO token waiters because the dispenser is dry; their queued
presence pins every scanner at cursor-only width; starved decode dries
the dispenser further. The system oscillates (token_stall 24.5→43.5s
after the exemption, dry-wait ~7 widths unchanged).

The fix changes who a yielding consumer gives its token to — not how
the pool grants. When `widthGate.yield` returns a **pool token** (never
the fragment baseline slot) and the same fragment's decode-ahead
scanner is at that moment parked in a token stall, the token transfers
directly to the scanner (`tryDonate`: deposit into the reader's
`donated` counter + condvar broadcast, accepted only while
`tokenStalled` is set under the reader lock). The scanner's admit
consumes donated tokens before consulting the pool; the decode worker
releases them to the pool exactly like TryAcquire'd ones. If the
scanner takes any path other than token-gated admission — pressure
wait, stop, end of file — leftover donations are flushed to the pool
(release inside the reader lock; the only compound order is reader
lock → pool lock, and the pool never calls into the reader).

Why this ranks "at least equal": a starved consumer's token does
nothing until decode runs. The donation precondition — this fragment's
consumer went dispenser-dry while holding a pool token AND this
fragment's scanner is token-stalled — is precisely the state where the
marginal token produces more progress on the producer side, and the
token being redirected is the fragment's *own* held capacity, not a
pool grant taken from anyone's queue position.

**No-wedge argument (re-established for donation):**

- *Pool FIFO semantics are formally unchanged.* Donation never grants
  pool capacity: the token moves consumer → `donated` counter → slot →
  decode worker → pool without re-entering the pool in between.
  Queued waiters' order among pool grants is untouched; their service
  is delayed by at most one bounded chunk decode per donation, and
  donations are rate-limited by the donating fragment's own yield
  events (each requires a full claim→process→go-dry cycle).
- *No goroutine ever waits holding a token.* The `donated` counter is
  drained on the very next scanner wake (a broadcast accompanies every
  deposit) or flushed before any non-token wait and at scan exit
  (deferred flush covers error paths; `stop` covers a scanner parked at
  deposit time — the stopped branch flushes before returning). Deposits
  are impossible once `tokenStalled` clears, so nothing can strand a
  token after the scanner moves on.
- *The §4.2.1 baseline guarantee is untouched.* Only `slotToken` yields
  donate; the fragment baseline slot always returns to the fragment, so
  every fragment keeps a token-free runnable consumer.
- *The cursor exemption is untouched.* Serial-floor progress is never
  gated on donations; a donated token found at the cursor branch is
  simply attached to the cursor chunk (released by its decode worker)
  rather than left parked.

Kill switch: `WADJET_WIDTH_DONATE=0` restores yield-to-pool. The
parquet scan path has the same tension (scan memo §9.3, token_stalls
18–28k) but binds far softer there; wiring `DecodeAheadIter` into the
same donor interface is a follow-up gated on its own counters.

SF100 pricing (memo
`shuffle-da-token-donation-sf100-2026-08-15.md`): KEPT — 96.6k
donations/window, record fast runs, rows/vsigs identical. Donation
does not reach the DEEP starvation mode (consumers parked slot-less in
claim have nothing to donate — join-6 width_donations = 0); that mode
is addressed by §2.3 below; the stage-walk serial floor (§4) is the
remaining recorded follow-up.

## 2.3 Claim-path donation (2026-08-15, post-donation window)

The donation window measured the §2.2 gap precisely: join-6's warm
shape ran `width_donations = 0` on every task while its dry-wait held
at ~7 widths. In that DEEP starvation mode no consumer ever *holds* a
pool token at a dry moment — the fleet is parked slot-less inside
`widthGate.claim` as FIFO token waiters, and their queued presence is
itself what pins the scanner at cursor-only width (`TryAcquire`
returns 0 while waiters are queued, and the §4.2.1 rule intends that).
The yield-path precondition can never occur; the fragment oscillates
at serial producer cadence.

The fix uses the one resource those consumers do hold: their queue
position. When a parked claim's grant lands (`w.ch`) and the same
fragment's decode-ahead scanner is at that moment parked token-stalled,
the consumer cedes the granted token to the scanner (the same
`tryDonateToken` seam as §2.2) and re-enqueues at the FIFO tail — at
most once per claim, so the second grant always sticks and the held
morsel's delay is bounded at one extra FIFO wait. The redirected token
follows the §2.2 ownership chain unchanged: grant → `donated` counter →
chunk slot → decode worker → pool, never re-entering the pool in
between; every §2.2 flush path applies as-is.

Why this closes the deep mode: each morsel-claim can now ferry one
token to producer admission, so the donation supply scales with the
morsel rate rather than with held-token dry events (zero in this
mode), and each donated chunk decode yields multiple morsels — positive
feedback that lifts the fragment until the window or dispenser budget
fills, at which point the scanner stops token-stalling and grants stick
to consumers again. The transfer is self-limiting at decode width: a
scanner blocked on the jobs queue or the byte window is not
token-stalled and refuses, so consumer width can never collapse below
(pool − decode workers), and the baseline consumer is untouched by
construction.

No-wedge addendum (over §2.2's argument, which covers the token's
lifecycle): the redirecting consumer re-parks as an ordinary FIFO
waiter — identical state to before its grant — and its bounded-progress
justification is unchanged (capacity is freed by other goroutines'
bounded holds, including the donated token's own bounded decode).
Pool FIFO order among waiters is untouched: the grant went to the head
waiter; ceding it is the grantee's use of its own token. Context
cancellation paths are the existing claim ones.

Kill switch: `WADJET_CLAIM_DONATE=0` restores grant-always-sticks
(`WADJET_WIDTH_DONATE=0` disables both donation paths). Marker:
`width_claim_donations` on the fragment line; reader-side `donated`
counts both paths. Success judgment at SF100: join-6
`width_claim_donations > 0`, warm-shape `consumer_dry_wait_ms`
collapse, then q08/q09 wall.

SF100 pricing (memo
`shuffle-da-claim-donation-sf100-2026-08-15.md`): KEPT — 50.4k claim
donations (total donated +50%), rows identical, zero reaps, first
window with no warm q08 slow-mode run. The warm join-6 trio did NOT
collapse, and the counters re-attribute it: those scanners are
stage-walk-bound (stage_ms > decode_ms, token stalls absent), so no
donation path can reach them — the residual is §4 extent skip-walk
staging, not admission.

## 3. Alternatives rejected

- **N producer goroutines over disjoint file slices** (the other option
  in the attribution memo): parallelism is capped by per-task file
  count — probe-split tasks hold few files — and it multiplies the
  tiered-fetch/staging state machines and makes the morsel dispenser's
  single-writer admit multi-producer. Chunk granularity (~40+ chunks per
  probe file) parallelizes within one file and composes with the
  existing file prefetcher for cross-file overlap.
- **Extent-parallel decode over the mmap reader**: WSHF extents are
  cheaply skippable (every column segment is length-prefixed), but the
  mmap path is the frozen-spin fault class the pread arc just closed;
  building the new parallelism on it would be architectural regression.
  The mmap reader stays as the untouched kill-switch path.
- **Decode inside morsel consumers**: rejected for the same reason as in
  scan-decode-pipelining §4 — it erases the producer/consumer split the
  morsel machinery assumes and couples decode width to op-chain width.
- **Scanner joins the token FIFO** (the exemption memo's other
  candidate for §2.2): enqueue the scanner as a blocking waiter with a
  went-empty escape back to the cursor exemption. Rejected on
  complexity-for-equal-coverage: the escape needs a grant-notification
  bridge between the pool's waiter channels and the reader's condvar
  (or deferred-notify surgery in `cpuTokens` to avoid a pool→reader
  lock inversion), it queues the scanner behind every other fragment's
  consumers even when its own consumers are the starved ones, and it
  weakens the "serial progress is never gated" invariant while parked.
  Donation triggers on exactly the state the re-attribution measured —
  own-fragment consumers going dry while holding width — and touches
  neither the pool nor the cursor rule. Revisited after the donation
  window for the deep-starvation mode: still rejected — §2.3 reaches
  that mode through the consumers' own queue positions with none of
  the pool/reader notification bridging this shape requires.
- **Out-of-order delivery**: the dispenser doesn't need order, but
  in-order forfeits nothing here (the window bounds occupancy either
  way), preserves `Delivered()`/truncation-position semantics for the
  transport fallback unchanged, and keeps drop-behind trivially correct.

## 4. Honest bounds

- The scanner's sequential stage walk is the new serial floor: staging
  is syscall+memcpy at page-cache bandwidth, so the ceiling moves from
  ~stage+decode to ~stage — a structural several-fold lift, priced only
  by the SF100 window. If the scanner itself becomes the measured pacer,
  the follow-up is extent skip-walk staging (headers only, bulk pread by
  workers), not wider decode.
- Queries whose probe fragments are already producer-fed at consumer
  speed move little; this targets the q08/q09 broadcast probe-split
  shape (`consumer_dry_wait_ms` collapse is the success marker, before
  wall).
- Pooled staging buffers add (workers + queue) × chunk-size transient
  heap (≤ the byte window by construction) that the ledger does not
  charge — the same accounting posture as the morsel dispenser budget,
  and bounded well below it.

## 5. Markers

Per-worker counters, folded from each reader at Close and logged by the
existing worker marker loop + final summary: `chunks`, `window_full_ns`,
`token_stall_ns` (stalls where a non-cursor chunk waited), `pressure_ns`,
`stage_ns` (scanner readChunkBytes time — the serial-floor watch item),
`decode_ns`, `donated` (tokens accepted via §2.2 yield-path or §2.3
claim-path donation — expect token_stall_ns to fall as this rises). The
width gate's fragment line adds `width_donations` (yield-path) and
`width_claim_donations` (claim-path — the deep-starvation engagement
marker). The morsel done-line's `consumer_dry_wait_ms` /
`dispenser_parents` (already Info) are the before/after judgment
counters for the width plateau itself.
