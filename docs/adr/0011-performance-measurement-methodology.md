# ADR-0011: Performance measurement methodology at SF100

Status: Accepted (2026-07-24, codifying rules earned 2026-07-21 → 07-24)

## Context

A week of A/Bs quantified how treacherous suite-wall benchmarking is on
shared-tenancy EC2 + S3: four behaviorally-identical baselines in one
day spanned steady 8m07–8m35 (~5%); the same binary has moved −24%
across windows; one lever measured −7.1% then +16.1% in two clean
same-window pairs; and a whole window degraded monotonically *during* a
pair, polluting both arms. Naive single-pair wall comparisons produced
wrong verdicts repeatedly.

## Decision

Performance claims at SF100 follow these rules:

1. **Same-window pairs only.** Both arms deploy back-to-back on
   identical cluster shapes (locked instance types: c7g.2xlarge coord,
   3× c7gd.4xlarge workers, on-demand). Cross-window number comparisons
   are window-stamped context, never evidence.
2. **Mechanism metrics decide; walls corroborate.** Every lever must
   name an observable mechanism metric before the run (read-tier byte
   splits, placement counts, upload/elision ledgers, purge timings,
   spread skew, refault markers) and is judged primarily on it. A
   single-pair suite wall inside the demonstrated noise band decides
   nothing on its own.
3. **Kill-switch-on-treatment-binary controls.** Both arms run the same
   binary; the control arm disables the lever by flag/env. This removes
   build drift and makes the discriminator exactly one variable.
4. **Rows gate everything**: 44/44 result rows identical (row *counts*
   for the float-order-nondeterministic queries; checksums where
   stable). Zero-row results fail the run.
5. **Correctness ladder before any deploy**: unit + `-race`, TPC-H
   SF0.01, tpch-harness local SF0.01+SF1 both arms. Local runs are
   correctness screens only — never performance evidence, and known to
   lie for disk/memory-pressure classes.
6. **Instrumentation rides permanently.** Ledgers and markers ship
   default-on in INFO logs (shuffle-io, upload, purge, decode-ahead
   stats), so every future run is also a measurement and stochastic
   effects accumulate evidence without dedicated deploys.
7. Contradictory pairs end the wall question until a mechanism
   hypothesis exists — more pairs of the same experiment do not settle
   a bimodal effect.

## Consequences

- Verdicts are slower but stop flip-flopping; two contradictory pairs
  (ADR-0007) were correctly resolved as "unstable, mechanism unknown"
  instead of shipping a wrong default.
- Every lever must be built with its kill switch and its mechanism
  metric — this shapes implementation, not just evaluation.
- Suite-wall wins are only claimed when they exceed the noise band or
  reproduce across windows with the mechanism metric aligned.
