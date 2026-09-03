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
  derived from the other. The one operator "every breaker spills" does
  NOT cover is a CROSS join — see the 2026-09-03 amendment on the routed
  probe: grace partitioning is sound only for a probe that routes by the
  partition key, and a cross join's does not, so its build must fit the
  budget and REFUSES loudly when it cannot.
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

### 2026-09-03: the ForceReserve census, and #789 (file half closed, decode half deferred)

`Tracker.ForceReserve` (`memory/tracker.go`) cannot fail and has no ceiling.
Every argument in this arc rests on knowing who calls it, so here is the
enumeration rather than a remembered number. **Seven producers charge a QUERY
tracker:**

| # | producer | what | loud? | released |
|---|---|---|---|---|
| 1 | `memory.ReserveOrForce` (`memory/acquire.go:33`, `:50`) | a scan's file load — ONE ROW GROUP at a time where the footer is already decoded, the whole file otherwise | WARNs at `:50` | when THAT ROW GROUP is decoded (`fileSlot.releaseRG`), or, on the whole-file path, when the file's last one is |
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

**#789 — a query's outcome at a fixed budget followed the scan's read-ahead.
The FILE half is closed (2026-09-03); the DECODE half is deferred with its
mechanism.**

Two charges moved the floor between identical runs of one query.

**The file, closed.** Producer 1 charged the WHOLE parquet file and producer 1's
release point was the file's LAST row group, so for the length of a scan every
other operator admitted against a floor that was the scan's file — 412,074
bytes of a 512 KiB budget on the type-matrix fixture, 79% of it. Measured with a
fresh database per run, 20 runs per column, all 18 flat columns: `c_cidr`
refused 20/20 with the scheduler free and answered 20/20 at `GOMAXPROCS=1`;
`c_str` answered 2 of 20 and `c_bytes` 1 of 20 with the scheduler free and
20/20 serially.

The scan now lands its file into ONE BUFFER PER ROW GROUP, out of the same
single object GET, cut at the byte ranges the footer already carries
(`parquet.FileReader.SetRowGroupBytes`, `planner/physical/scan_rowgroup_load.go`).
Each buffer is charged when it lands and released when its row group has been
decoded, and the read is demand-driven with each row group admitted by
`ReserveOrForce` before it is read — no share, no count, no new constant: what
a scan holds follows the BUDGET. All 18 columns now answer 20/20 on both arms,
and the doubled budget the join family carried, with the ratchet that watched
it, are deleted. Kill switch `WADJET_SCAN_RG_BUFFERS=0`.

Two paths keep the whole-file read, deliberately: a file whose row-group
metadata came from the catalog's ANALYZE-written blob has no decoded footer, so
its byte ranges would cost a second request per file — the one thing the
recorded whole-file-GET decision forbids (`docs/design/scan-pread-reads.md`);
and local-fd stores keep staged pread, which holds no whole-file buffer at all.
Carrying byte ranges in the RG-metadata blob would close the first, and is a
catalog-format change, not this one.

**The decode, deferred.** Producer 3 charges a decoded batch AFTER the decode
and releases it when the consumer takes it. A charge taken after the allocation
bounds nothing, so a shape whose row groups are big enough carries itself past
its budget on read-ahead alone; `wadjet.TestAScansDecodedReadAheadStillOverdrawsTheBudget`
pins one (4,000 rows of 512-byte strings, row groups of 512, 1 MiB) that
refuses on every run holding 1.04x-1.73x its budget.

The fix is admission BEFORE the decode, against the projected columns'
uncompressed bytes from the footer — the class ADR-0015 records for the
worker's decode window. It was implemented and MEASURED here, twice, and both
forms are refused on the measurement:

- with `ReserveOrForce`'s bounded-wait-then-force, the pinned shape answered
  1 of 20 runs and took 156 s against 0.25 s;
- with the wait ended by a CONDITION rather than a clock (wait while this
  source holds decoded bytes its consumer has not taken; force when it holds
  nothing, one forcer at a time), it answered 17-19 of 20 and took ~90 s.

Both replace a deterministic refusal with a nondeterministic answer — the
defect class — at 350x the wall. The reason is structural: at a budget that
tight the scan serializes, and part of what it then waits on is the join's hash
index, which a grace eviction cannot free (#823's deferred half, the arena is
global across the 64 partitions). **Admission cannot bound what it cannot cause
to be released.** So the decode half waits on #823's per-partition index state,
and this ADR records it as deferred rather than bounded by something that
measures worse. The earlier bounds this section carried — a headroom bound
that inverted the result 17/20 -> 3/20, and a `budget/N` share whose value
decided which shapes were deterministic — stay REJECTED; the headroom one was
inverted by producer 1's granularity, which is the half now closed, and it is
the CONDITION-ended form above that supersedes it.

**How the deferred half is pinned, so a fix cannot land quietly.**
`wadjet.TestAScansDecodedReadAheadStillOverdrawsTheBudget` runs the pinned
shape 20 times and fails if ANY run answers — an answer means the read-ahead is
bounded. It also carries the two measurements above, so the next reader does
not re-derive the appealing version. The file half's own gates are
`wadjet.TestAJoinAtAFixedBudgetIsDecidedByTheQueryNotTheScheduler` (the census
as an assertion, with the `GOMAXPROCS=1` control it must agree with) and
`wadjet.TestAScanIssuesOneObjectGetPerFile` (the request count the design had
to keep).

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

### 2026-09-03 (the spilled-arm arc): grace partitioning requires a routed probe

`spillOneInMemoryPartition` frees an evicted partition's build batches by
setting `h.buildBatches[i] = nil`, and its own comment says why that is safe:
"unreachable on the in-memory probe path because HashJoinProbe.Execute routes
spilled-partition rows to disk before any hash lookup". That argument is about
a KEY-ROUTED probe, and it was written as though every probe were one.

A **CROSS join's is not.** It has no key: `computeBuildPartitionRows` sends
every build row to one partition, `HashJoinProbe.Execute` returns to
`nextCrossChunk` before the routing runs at all, and `nextCrossChunk` walks
EVERY entry of `buildBatches` for every probe row. One eviction therefore nils
the slot it is about to read. That is #832 — a join whose `ON` equates two
expressions rather than two columns has no equi-key, is planned as a cross join
with the condition as a filter above it, and panicked on the spilled arm with
`invalid memory address or nil pointer dereference` while the single-process,
DAG and DAG-shuffled arms and PostgreSQL all answered. Not a race: a cross join
whose build evicts ALWAYS reads a nil slot.

**A join whose probe does not route by the partition key does not take the
partitioned build** (`HashJoin.probeRoutesByPartition`, checked at the build
dispatch and again at the eviction site). It takes the flat build, which
reserves per arrival batch and REFUSES when the budget cannot hold it —
ADR-0006's "degrade or fail loudly", with a message naming the reason rather
than the flat path's older "no spill configured", which was a lie to this
caller.

**What is NOT fixed, and is recorded rather than bounded away.** Such a build
cannot spill AT ALL, because there is no blockwise nested-loop join to spill
into: the emitter would have to re-read the spilled build per probe batch, and
that operator does not exist. A cross join whose build exceeds the budget
therefore refuses. Skipping the nil slots instead would have been the bandaid —
it turns a loud crash into a silently missing row, which is strictly worse
(rule 8). The residual is pinned as a loud failure in the type-matrix spill
sweep (`join_computed_wide_refuses_under_the_smaller_budget`), so it fails the
moment a spilling nested-loop join makes the shape answer.

The producers table above is unchanged in count: producer 5 charges the index
of a build that reaches `reconcileHashMemory` on either path.

### 2026-09-03 (the spilled-arm arc): #823's reclaim half, MEASURED and DEFERRED

Producer 5's "released" column above says a grace eviction does not free the
index. Here is what that costs, so the next arc starts from a number instead
of an argument. A partition-on-arrival build of 2,000 rows in 256-row arrivals
against a 1 MiB budget, then every partition evicted — 64 of 64, all 64
`buildBatches` slots nil, no build column data left in memory at all:

| | bytes | share of the residual |
|---|---|---|
| `Tracker.Used()` | 106,320 | — |
| the hash index total | 98,304 | 92% |
| ...of which the hash TABLE | 65,536 | 62% |
| ...of which arena + chain | 28,672 | 27% |
| ...of which the bloom filter | 4,096 | 4% |

At 20,000 rows the same shape gives `used = 562,359` with 696,320 gross index,
524,288 of it hash table. **The dominant term is the hash TABLE, not the
arena.**

**Why per-partition ARENAS alone are not the fix**, even though they are the
contained change. A key's partition is a function of the KEY
(`spillPartition`), so a per-partition arena needs no widening of what the
hash table stores — `intIndex.Get(key)` can index `arena[spillPartition(key)]`
with the same int32. That is a real, bounded refactor. It reclaims the 27%,
and leaves the 62%: `used` after a full eviction would fall from 106,320 to
about 77,648 on the fixture above, which does not return the floor to anything
like zero. Rule 11 forbids shipping it: it is bounded by a model the same
commit knows is incomplete, and it leaves #823's own headline shape — `used`
far above the floor after 64 evictions — exactly where it was.

**What the whole fix is.** Per-partition hash TABLES: `intIndex` and
`strIndex` become one per partition, and the arenas follow for free because a
partition's table only ever addresses its own. Eviction then frees a
partition's table, arena and chain with its columns, and the only thing that
must survive is the bloom filter, which is 4% and covers spilled keys on
purpose. The cost is that the probe's inner loop — `inlineIntProbe` and the
typed emit switch, the hottest code in the engine — gains a partition
selection per row, and 64 tables sized independently change the load factors
and the growth path the current single table was tuned for. That is a
performance-bearing change to a hot kernel and needs its own arc with its own
A/B, not a rider on a correctness arc.

**How the residual is pinned.**
`exec.TestEvictingEveryPartitionLeavesTheIndexCharged` measures the floor after
a full eviction and fails in BOTH directions: if `used` falls below the index's
own bytes the reclaim has landed and the pin must be deleted (with this section
and producer 5's row); if what survives stops being mostly index, the pin has
stopped measuring #823 and says so.
