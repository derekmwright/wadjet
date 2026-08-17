# ADR-0006: Never-OOM memory — shared pool, ownership ledger, spill-everywhere

Status: Accepted (accounting overhaul landed 2026-05-30, 35f6730; recorded 2026-07-25; amended 2026-08-17 — see Amendments)

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

## Amendments

### 2026-08-17: off-heap group-state arrays (narrow arena exception)

The "custom arena allocator" rejection is narrowed, on new evidence,
to admit exactly one shape: whole pointer-free SoA arrays for typed
aggregate group state, backed by anonymous `MAP_NORESERVE`
reservations that grow in place (`memory/offheap_linux.go`, kill
switch `WADJET_OFFHEAP_AGG`).

Evidence: at 100M-group scale (ClickBench Q33) heap-grown state
measured 22.3 GB heap on 12 GB live — append-doubling copies and
rehash garbage stacked between GC cycles — and whether a cold try fit
under GOMEMLIMIT or spiraled through the pressure-valve spill path
was GC-timing luck (108 s vs 21 s cold on identical binaries,
attributed by a same-window binary control, 2026-08-17). The
GC-friendly alternative (chunked arrays) measured +16-54% on
cache-resident scatter. With the arena: 12.8 GB peak heap, in-place
growth, state invisible to the collector, tries deterministic.

Boundary of the exception (everything outside it stays rejected):
one owner per array, no object lifetimes, no per-value layout, no
sharing; ownership transfers whole-registry on merge adoption; the
engine's tracker keeps accounting the bytes (len-based) so spill
triggers are unchanged. The 2026-06-09 shelving of the BytesColumn
decode arena is NOT reopened — that design managed per-value
lifetimes inside shared buffers, the complexity this exception
excludes.
