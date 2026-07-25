# ADR-0006: Never-OOM memory — shared pool, ownership ledger, spill-everywhere

Status: Accepted (accounting overhaul landed 2026-05-30, 35f6730; recorded 2026-07-25)

## Context

SF100 on 32 GB workers is a memory-constrained regime by design
(deliberate priority: memory-constrained scaling primitives over bigger
instances). Early incidents were OOM kills, GC thrash spirals, and
heartbeat starvation followed by reap/redispatch loops.

## Decision

- **One shared per-process pool budget** with per-task charges; the
  coordinator admits and bin-packs against heartbeat-reported pool
  stats, and workers hold task starts until the pool has room.
- **Tiered ownership ledger** for accounting: every buffer has one owner
  tier; shared cache vectors are charged once (re-adding a tracker
  charge on shared cache vectors is a known regression, do not).
- **Every pipeline breaker spills**: HashJoin grace-partitions on
  arrival, HashAggregate merges partial states k-way, Sort/Window run
  external sorted-run merges. Degradation is graceful slowdown, never
  process death.
- **OS-facing relief valves**, each with a kill switch: mmap relief
  (RSS ceiling via MADV_DONTNEED), bounded dirty writes, the page-cache
  refault sensor with its episode cap, GOGC=100 + GOMEMLIMIT.
- Rejected alternatives stay rejected without new evidence: global
  admission-control throttles (2026-05-18 memo) and a custom arena
  allocator (shelved 2026-06).

## Consequences

- "Harness-green ≠ SF100-safe" for anything touching disk IO or memory
  pressure — the validation ladder requires SF100 runs for this class.
- The memory-tight deploy profile is `--mmap-relief` +
  `--bounded-dirty-writes`, never `--spill-floating-budget`
  (2026-06-10 postmortem).
- Peak-memory bounds, not averages, drive planning decisions (skew
  split exists to bound the largest single task, not the mean).
