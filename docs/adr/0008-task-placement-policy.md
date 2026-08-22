# ADR-0008: Task placement — eager reservation → cache affinity → input locality → memory binpack → round-robin, under a same-batch anti-clump cap

Status: Accepted (locality tier validated 2026-07-24; SF100 profile default since 62966b3; amended 2026-08-22: affinity tier recorded, probe-split joins it)

## Context

Targeted gRPC dispatch (ADR-0005) has no work stealing, so placement
mistakes are sticky. Three forces compete: memory safety (put big tasks
where the pool has room), locality (put consumers where their bytes
are), and spread (never stack one stage's fan-out on one worker — the
2026-07-20 Q20 diagnosis measured +24s serialization from one clumped
fan-out).

## Decision

`pickWorkerFor` is an ordered policy stack:

1. **Eager-consumer reservation** — manifest-blocked consumers spread so
   they cannot starve producer lanes.
2. **Base-table cache affinity** (`Task.AffinityWorkerID`, added
   2026-08-08 with `docs/design/scan-affinity.md`): a task whose
   base-table files rendezvous-hash to one worker's NVMe cache goes
   there. Every dispatcher that chooses a task's base-table file set at
   dispatch sets it — scan fan-outs and shuffle scan sides since
   2026-08-08, **broadcast-join probe-split since 2026-08-22** (the
   probe slice was the one remaining dispatch-chosen base-table file
   set placed by binpack; its misses were served by the peer tier at
   2-3 s per file on the task's critical path, the SF100 "straggler
   tier" — see the memo's §probe-split affinity). Ahead of locality
   since 2026-08-22: a probe-split task's locality hints (when present)
   point only at its small, replicated broadcast build's single
   producer, while its bytes are the base-table probe slice — placing
   by locality first put the whole task on the build producer, off the
   cache that holds what it actually reads. Tier order is gated by
   `affinityBeforeLocality` (`WADJET_AFFINITY_BEFORE_LOCALITY=0` restores
   the pre-2026-08-22 locality-then-affinity order).
3. **Input locality** (`--locality-placement`): a task whose
   streaming-exchange hints all point at one connected worker goes
   there. This is exactly the 1:1 stage-chain class (consumer task *i*
   reading producer task *i*'s output). Analysis first proved
   repartition fan-ins have **no locality headroom** — hash
   partitioning spreads every producer's contribution uniformly, so
   that peer traffic is irreducible; only the 1:1/single-producer
   classes are winnable.
4. **Memory binpack** for estimated tasks, against heartbeat pool stats.
5. Round-robin.

All tiers respect a **same-batch cap** (`ceil(batch/workers)`), which is
what keeps locality from re-creating the Q20 clump when a whole fan-out
hints at one producer.

Validated at SF100 in two windows (2026-07-24): cluster read split
37→50% and 32→49% local (~76-108 GB of peer streams converted to local
mmaps per pair), fan-out skew 1.00 throughout, rows identical, Q18
steady −15.3% in the clean window. Enabled by default in the SF100
benchmark profile; engine default staged until the profile accumulates
runs.

## Consequences

- Placement quality is observable per dispatch line
  (`placement=eager/local/binpack/rr`, `spread=`).
- Locality is a preference, never correctness: hints are best-effort and
  every read tier still falls back.
- Affinity is the rule for dispatch-chosen base-table file sets: a
  dispatcher that slices base-table files across tasks without grouping
  by rendezvous owner is re-creating the straggler tier. The peer tier
  is the fallback for readers whose files are NOT chosen at dispatch
  (late-materialization gathers, builds), not a substitute for
  placement.
- The irreducibility result redirects future exchange-bandwidth work to
  the wire itself (ADR-0010's compress-on-serve) or to plan structure —
  not to smarter placement of repartition consumers.

References: `docs/design/locality-placement.md`, `docs/design/scan-affinity.md`.
