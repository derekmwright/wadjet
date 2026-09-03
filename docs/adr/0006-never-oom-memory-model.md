# ADR-0006: Never-OOM memory — shared pool, ownership ledger, spill-everywhere

Status: Accepted (accounting overhaul landed 2026-05-30, 35f6730; recorded 2026-07-25; amended 2026-08-17 and 2026-09-03 — see Amendments)

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
  process death. What a drained group's BYTES have to be is
  [ADR-0023](0023-group-key-and-group-value-are-two-encodings.md): the
  merge key and the group's value are two encodings, and neither is
  derived from the other.
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

### 2026-09-03: the ForceReserve census, and #789 left OPEN

`Tracker.ForceReserve` (`memory/tracker.go`) cannot fail and has no ceiling.
Every argument in this arc rests on knowing who calls it, so here is the
enumeration rather than a remembered number. **Seven producers charge a QUERY
tracker:**

| # | producer | what | loud? | released |
|---|---|---|---|---|
| 1 | `memory.ReserveOrForce` (`memory/acquire.go:33`, `:50`) | a scan's whole-file load | WARNs at `:50` | when the file's LAST row group is decoded (`fileSlot.releaseRG`) |
| 2 | `fileSlot.ensureLoaded`'s pool reconcile (`planner/physical/util.go:484`) | the pooled buffer's real capacity above the file size | silent | with (1) |
| 3 | `scanSourceInner.trackScanBatch` (`planner/physical/plan.go`) | every decoded row-group batch | silent | when the batch leaves through `next()` |
| 4 | `scanSourceInner.trackPooledBuf` (same file) | the EAGER scan path's whole-file buffers, `cap(buf)`, no ceiling | silent | at scan close (`releasePooledBufs`) — coarser than (1) |
| 5 | `HashJoin.reconcileHashMemory` (`engine/exec/join.go`) | the hash arena and index | WARNs once when it crosses the budget (this arc) | at join Close; a grace eviction does NOT free it (#823) |
| 6 | `HashJoin.freezeAccum` (`engine/exec/join_partition_arrival.go`) | a per-partition accumulator's fixed-capacity excess over its filled rows | silent | with the partition (spill releases `partMemory`), rest at Close |
| 7 | `SpillManager.TrackBatch` (`engine/memory/spill.go`) | an operator batch charged past the budget so `ShouldSpill` can see it | silent | by the caller's own `ReleaseTracking` |

Two more exist on OTHER ledgers and are not part of a query's floor: the
worker's file cache (`worker/cached_store.go`) on the worker tracker, and
`scan.DecodeAheadIter`'s delivery-cursor group (`engine/scan/decode_ahead.go`)
on the decode window's ledger.

**#598's refusal of "reserve-and-overcommit" stands on the corrected count.**
Deliberately reserving past the budget for an arrival batch would be an EIGHTH
producer on a query tracker, and the argument was never about the ordinal: the
overcommitted bytes join the floor every downstream `Reserve` is measured
against, and the join's own share of them is provably unreleasable (5 above).
Seven silent-or-loud producers is a reason to add none, not a reason the next
one is cheap. A build that cannot reserve a whole arrival batch splits it.

**#789 — a query's outcome at a fixed budget still follows the scan's
read-ahead. OPEN; this is a residual, not a decision.**

Producer 3 charges every decoded row-group batch and releases it when the
consumer takes the batch, so the floor at the build's first `Reserve` carries
however many row groups the scan decoded ahead. Measured on the type-matrix
self-join at 512 KiB: `k x 17,888` bytes, `k in 0..3`, and the join fits for
`k <= 2` and not for `k = 3` — 2 refusals in 20 runs of an identical query, 0
in 20 with `GOMAXPROCS=1`. Two bounds were implemented in this arc and **both
are refused on measurement**:

- **By the budget's free HEADROOM** — the design `scan.DecodeWindow`'s ledger
  already uses on the worker path (`NewDecodeWindowWithLedger`, admission by
  `ledger.Reserve` with the delivery cursor always forced through). On the
  embedded scan path it INVERTS the result: 3 of 20 runs answered where 17 of
  20 answered without it, and the `GOMAXPROCS=1` control went from 20 answers
  to 0. The reason is producer 1's release granularity: `used` contains the
  scan's own whole-file buffer, which is freed only when the file's LAST row
  group has been decoded, so a headroom-derived bound throttles exactly the
  decoding that would release it. A bound whose input is dominated by the
  charge it is shrinking is a feedback loop.
- **By a fixed SHARE of the budget** (`budget/N` per scan source, one-batch
  floor). This removes the feedback, but the value decides the answer: swept at
  512 KiB, 20 runs per column, `N = 2 / 4 / 8 / 16` moves which of the eighteen
  join keys are deterministic (`c_bytes` and `c_cidr` answer 20/20 at `N = 8`;
  all eighteen do at `N = 16`) while the census wall goes from 39-67 s to
  638 s, because once the allowance falls below one batch the source decodes
  serially. That is a knob trading determinism against decode parallelism, not
  an invariant, and this ADR does not encode knobs.

**What the fix requires, in code terms.** Headroom admission is the right
design and it is already in the tree; what blocks it here is that the embedded
scan's file charge is all-or-nothing. `fileSlot` holds ONE pooled `[]byte` for
the whole parquet file (`ensureLoaded`) and the decoder reads row groups out of
it, so the charge cannot be released per row group without the bytes being
released per row group — which means per-column-chunk ranged reads. That path
exists (`docs/design/scan-pread-reads.md`, staged pread) but is taken only when
the store hands back an `*os.File`; the whole-file read for object stores is a
recorded decision with its own measurements ("one object GET beats per-chunk
ranged GETs"). Reopening it is a scan-architecture arc, not a rider on an
accounting fix, so #789 is deferred here under the correctness-fix protocol's
rule 11 with its mechanism written down rather than bounded by a constant.

**How the residual is pinned, so a fix cannot land quietly.** The type-matrix
join family keeps `spillMxJoinBudget`, and that raise now RATCHETS: the sweep
runs every raised cell at `spillMxBudget` and fails the family if all of them
answer on every run. `wadjet.TestAScansDecodedReadAheadStillOverdrawsTheBudget`
pins the loud face of the same residual — a shape whose row groups are big
enough that read-ahead alone carries the query to 1.4-1.7x its budget, refusing
on every run — and fails if it answers or if the refusal is issued from under
the budget.

**What this arc DID settle about the ledger.** A downstream operator inherits
an overdrawn tracker, and that is correct: there is one ledger per query and a
forced charge is resident memory, so an operator reserving against a floor that
excludes somebody else's bytes reserves against a fiction. What must not happen
is a charge for memory that does not exist yet — producer 5 pre-sized its index
from the planner's estimate of the whole build and charged `cap()` for it,
191,072 bytes on a 512 KiB ledger for a batch of 20 rows. Pre-sized capacity is
now bounded by the room that exists (#823), and producer 5 says so out loud
when its forced delta crosses the budget. Producer 5's charge for rows that
HAVE arrived remains unreleasable by a grace eviction, which is #823's open
half: the arena is global across the 64 partitions, so evicting one frees its
column data and leaves its chain entries. Per-partition index state is the fix
and it is its own arc.
