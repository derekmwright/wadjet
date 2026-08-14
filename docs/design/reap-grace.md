# Output-aware reap grace

Status: SHIPPED (2026-08-14). Default ON, grace 90s.
Kill switch: `WADJET_REAP_GRACE_SECONDS=0`.

## 1. Problem

Both worker-liveness signals — the 10s global heartbeat and the 2s
per-task progress — ride NATS. Total NIC silence therefore looks
identical to the coordinator whether the process is dead or healthy
behind a stalled network path. At the 90s stale TTL the reaper
declares the worker dead, and `classifyFatalResult` /
`fetchStageOutputData` treat its not-yet-durable stage outputs as
lost ("producer dead before upload landed"), forcing a
producer-stage re-run.

Observed (2026-08-13, floor=0 eager arm, Q03-R2): worker-605de12d
stayed alive and logging while heartbeats AND peer serving went
network-silent for ~105s — barely past the TTL. The coordinator
reaped it, invalidated its un-uploaded scan-1 shuffle outputs, and
recovery cost 2m56. A silence that outlasted the TTL by ~15s cost a
multi-minute re-dispatch.

The asymmetry: reaping a worker that holds the ONLY copy of stage
outputs costs a producer re-run even when the reap is right; waiting
a bounded extra window costs nothing when the worker recovers and
only delays an already-expensive recovery when it doesn't.

This class also grows with upload pacing (upload-burst-smoothing,
pinned 150 MB/s): paced PUTs stretch the window during which outputs
exist only on the producer — ADR-noted as "longer producer-dead FTE
exposure," covered by exactly this grace.

## 2. Mechanism

Grace engages only when ALL of:

1. Worker silent past the stale TTL (90s) but within TTL+grace
   (default grace 90s → hard bound 180s).
2. The worker holds outputs whose durable copy has not landed:
   `peerFileRegistry.PendingNonDurableFor(workerID) > 0`.
3. Streaming exchange is on (the callback is wired only then; nil
   callback = pre-grace behavior).

Deferral is per reap tick (15s cadence) and logs
`worker reap deferred (grace): pending non-durable outputs` with the
pending count and grace deadline. Past TTL+grace the worker is
reaped unconditionally — a truly dead worker's recovery is delayed
by at most one grace window, and only when a reap would have
invalidated outputs anyway.

### Pending-key accounting (precise, not inferred)

`ResultNotification.UploadPendingKeys` — new, additive — lists
exactly the result keys whose Phase-B background upload was still in
flight at notification time. Worker side: populated in
`finishStageOutputAsync` and the unpartitioned async path.
Coordinator side: `peerFileRegistry.RecordPending` marks them; a key
stops counting when `UploadComplete` flips its durable bit
(`pending[k] && !durable[k]`). Consequences:

- Sync-uploaded outputs (pipeline/gather tasks, async-adoption
  fallbacks) never count — no false grace from keys that are already
  durable but never receive UploadComplete.
- Old workers never send the field → no pending marks → grace
  disengages. Mixed-version safe, degrades to current behavior.
- UploadComplete with Failed=true keeps keys pending — correct: the
  worker abandoned uploads, so the local copies really are the only
  ones.
- Lazy/off durability: deferred uploads stay pending until released
  → grace correctly covers the whole non-durable window.
- `CleanupQuery` drops the group at query end, so elided uploads
  (terminal roots, no UploadComplete ever) stop granting grace.

### Keeping consumers retryable during grace (the other half)

Deferring the reap is useless if the input-lost classifiers still
declare the producer dead at TTL. New predicate
`WorkerRegistry.MayRecover` (registered AND silent < TTL+grace)
replaces `IsAlive` in:

- `classifyFatalResult`: a grace-window producer keeps missing-input
  failures retryable (retries re-hit the peer or S3; the lazy-release
  nudge still fires). Once the grace expires and the reaper removes
  the worker, MayRecover goes false and the next failure classifies
  fatal exactly as before.
- `fetchStageOutputData` (coordinator scalar reads): the 15s re-poll
  keeps polling while the producer may recover; at deadline it still
  classifies input-lost (not an opaque fetch error) if the producer
  is silent past TTL, preserving the streaming-disabled re-execution
  path.

## 3. Bounds and non-goals

- Truly-dead-with-outputs recovery: +grace (90s) worst case, on top
  of the existing TTL. Dead workers with nothing pending reap at TTL
  exactly as before.
- Consumer retry budget is unchanged (maxTaskAttempts=3, each
  missing-input attempt burns ≥15s of durable-wait). Against a
  ~105s silence that is marginal: retries may exhaust and take the
  input-lost re-execution anyway. v1 accepts this — the grace makes
  recovery possible where it was impossible, and never worse. If a
  live incident shows retry exhaustion racing a recovery, add
  missing-input retry backoff then; do not pre-build it.
- No liveness-policy change: heartbeat cadence, TTL, drain, stuck
  sweep all untouched. Grace is a reap-time filter only.
- Not a substitute for durability: eager uploads remain the default
  (lazy re-proposal stays rejected, 08-09 negative result).

## 4. Validation

- Unit: `TestReapStaleGraceDefersWorkerWithPendingOutputs` (the
  105s-silence regression), `TestReapStaleGraceExhaustedReaps...`
  (bound), `TestReapStaleNoGraceWithoutCallback` (kill path),
  `TestPendingNonDurableFor` (+cleanup), and
  `TestClassifyFatalResultGraceWindow` (retryable-during-grace, and
  grace=0 reproduces pre-grace classification).
- Existing reap/liveness/annotator suites unchanged and green
  (registry literals carry grace=0 → prior semantics).
- Harness `--mode=local` gate before any EC2 use (coordinator-path
  change rule).
- SF100: no dedicated arm — the lever is a rare-event recovery
  improvement, invisible in clean walls. Judge from incident logs when the
  next network-silence episode hits: expect `reap deferred (grace)`
  followed by either recovery (uploads land, no invalidation) or a
  bounded reap 90s later.
