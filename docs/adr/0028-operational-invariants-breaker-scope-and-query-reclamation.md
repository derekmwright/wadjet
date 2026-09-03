# ADR-0028: A breaker is scoped by operation class, and every exit path of a query reclaims the same way

Status: Accepted (2026-09-03, the operational-lifecycle arc: #798, #820, #821, #822)

## Context

Two operational defects kept recurring in different clothes, and both are
failures of SCOPE — a mechanism whose blast radius is wider than the
condition that triggers it.

**The object-store circuit breaker.** `CircuitStore` counts consecutive
failures and, past a threshold, fast-fails every later request until a
reset window elapses. It was built to stop a dead S3 from turning every
query into a slow timeout. What it actually did four times was turn a
healthy S3 into a dead one:

| date | trigger | consequence | fix shipped then |
|---|---|---|---|
| 2026-06-11, SF10 | compaction deleted chunks under three dispatched scan tasks → "object not found" ×5 | breaker open → every later query failed | `DeleteGrace = 30 min` (`compaction/compactor.go`) |
| 2026-07-12, SF100 | the worker's own `uploadManager.CancelQuery` aborted 5+ queued uploads at a query boundary | breaker open on every worker → Q21/Q22 failed terminally | exclude `context.Canceled` |
| 2026-08-05, SF100 | streaming-exchange S3 fallbacks probe not-yet-uploaded keys BY DESIGN | repeated 404s opened the breaker → Q06/Q08 steady FAIL | exclude `ErrNotFound` |
| 2026-09-02 (#798) | one `ResultCleaner.CleanQuery` whose 30 s deadline expired: every remaining `Delete` returns `DeadlineExceeded` instantly | breaker open → **every base-table READ** fast-fails with "circuit breaker open: S3 unavailable" | this ADR |

Each fix excluded one more error class from the counter, and the defect
came back in the next class. Round-0 measured the fourth instance through
the production `ResultCleaner` and `CircuitStore` (no AWS, no MinIO): five
consecutive `DeadlineExceeded` deletes opened it; the next `Get`, `Head`
and `List` on an existing key all returned `ErrCircuitOpen`. It also
measured that the breaker then **blocked its own cleanup** — only 5 of 12
deletes reached the store — and that `CleanQuery` returned
`deleted=0, err=<nil>`, so the caller was told the reclamation succeeded.

Two facts settle the scope question:

- `NewCircuitStore` is constructed **exactly once** in the tree
  (`cmd/wadjet/main.go`), wrapping the whole `Store`. The failure counter
  is therefore process-wide across every bucket AND every operation class —
  the filing's "per-bucket breaker" framing was wrong, and the correction
  is why one scratch-prefix cleanup can fail reads of base tables.
- The traffic the breaker must PROTECT and the traffic that TRIGGERS it are
  not the same traffic. A query reads to answer; it writes stage output and
  deletes scratch off the critical path.

A NotFound was, until this arc, merely neutral: it did not count as a
failure and did not clear the counter either. Measured consequence (#821):
five failing deletes interleaved with by-design NotFound probes still
opened the breaker, while forty interleaved with successful reads did not.
A round trip that ANSWERED was invisible.

Finally, none of it was configurable or observable: `DefaultCircuitConfig()`
was passed literally, there was no flag, config key, env var or kill
switch, and no metric recorded that a breaker had opened (#822).

## Decision

1. **The breaker is scoped by operation class — read, write, delete — with
   an independent counter and state machine per class.** The invariant,
   stated once instead of enumerated one error at a time:

   > A failure on a non-read, off-critical-path operation never fast-fails
   > the read path.

   `OpRead` covers `Get`, `GetReaderAt`, `Head`, `List`, `BucketExists`;
   `OpWrite` covers `Put`, `PutIfMatch`, `MakeBucket`; `OpDelete` covers
   `Delete`. A genuinely unreachable S3 still fast-fails reads — it now
   takes five READ failures to do it, which is the correct semantics and
   is gated by a mirror arm so the invariant cannot be satisfied by
   disabling the breaker.

2. **A completed round trip clears the counter, whatever its answer.** A
   NotFound (or BucketNotFound) is a definitive, healthy response, so it
   calls `onSuccess` for its class. `context.Canceled` stays neutral: it
   comes from our own callers and is evidence of nothing.

3. **A cleanup loop reports what it reclaimed and stops at the first
   context error.** `CleanQuery`, `CleanAll` and `CleanStale` return an
   error naming how many objects remain, and abandon the loop once the
   caller's deadline has expired instead of issuing N−1 more instantly
   failing deletes. This is the same defect from the other side: the burst
   that opens the breaker is manufactured by the loop that ignores its own
   context (#820).

4. **The breaker's thresholds and the intermediates' TTL are operator
   surface, and opening is a metric.** `--storage-circuit-threshold`,
   `--storage-circuit-reset`, `--storage-circuit-request-timeout`,
   `--query-intermediate-ttl` and `--query-intermediate-sweep` are flags;
   `wadjet_circuit_breaker_opened_total{class}` counts transitions into
   open, labelled by class, so an operator can see that DELETEs are
   tripping without concluding that reads are.

5. **The single construction site is pinned.** Every assertion about the
   breaker's behaviour assumes one process-wide instance;
   `TestCircuitBreakerHasExactlyOneConstructionSite` fails if a second
   `NewCircuitStore` call appears, so a second breaker has to be argued
   for rather than discovered later.

## Alternatives rejected

- **Exclude `DeadlineExceeded` on deletes.** The smallest diff, and the
  pattern that has now failed three times: it leaves slow UPLOADS able to
  fail reads, and the next incident is in whatever class is left.
- **Count a read as a failure only on read errors** (i.e. protect reads,
  leave one counter). Equivalent for the read path, but it leaves the
  question "does an upload failure still open the write breaker?"
  unanswered and unasserted. Three counters answer it.
- **Per-bucket breakers.** The filing's framing. It does not address the
  defect: one bucket carries both the scratch prefix and the base tables
  in every deployment we ship.
- **Removing the breaker.** A dead S3 must still fast-fail rather than
  turn every query into a pile of timeouts; the mirror arm exists to keep
  that property from being traded away.

## Consequences and boundaries

- A genuinely dead S3 now needs five READ failures before reads fast-fail,
  where before an unrelated delete burst could get there first. Reads see
  at most four extra real timeouts in that window; that is the price of
  not fabricating outages, and it is deliberate.
- `errors.Is(err, objstore.ErrCircuitOpen)` remains a `retryOnDAG` reason
  on the local fast path. With per-class scoping, a delete-class open no
  longer causes a whole query to re-run on the DAG and fail there too.
- The config FILE does not reach `storage.*` (that is #808, deferred to a
  later arc as a product decision on precedence), so the breaker's knobs
  are **flags and defaults only**. `docs/configuration.md` says so
  explicitly rather than implying a YAML block that would be inert.
- The breaker still shares one instance across buckets. That is now a
  pinned, argued property rather than an accident.
