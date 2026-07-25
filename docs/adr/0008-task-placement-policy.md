# ADR-0008: Task placement — eager reservation → input locality → memory binpack → round-robin, under a same-batch anti-clump cap

Status: Accepted (locality tier validated 2026-07-24; SF100 profile default since 62966b3)

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
2. **Input locality** (`--locality-placement`): a task whose
   streaming-exchange hints all point at one connected worker goes
   there. This is exactly the 1:1 stage-chain class (consumer task *i*
   reading producer task *i*'s output). Analysis first proved
   repartition fan-ins have **no locality headroom** — hash
   partitioning spreads every producer's contribution uniformly, so
   that peer traffic is irreducible; only the 1:1/single-producer
   classes are winnable.
3. **Memory binpack** for estimated tasks, against heartbeat pool stats.
4. Round-robin.

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
- The irreducibility result redirects future exchange-bandwidth work to
  the wire itself (ADR-0010's compress-on-serve) or to plan structure —
  not to smarter placement of repartition consumers.

References: `docs/design/locality-placement.md`.
