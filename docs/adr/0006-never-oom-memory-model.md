# ADR-0006: Never-OOM memory — shared pool, ownership ledger, spill-everywhere

Status: Accepted (accounting overhaul landed 2026-05-30, 35f6730; recorded 2026-07-25; amended 2026-08-17, 2026-09-03 and 2026-09-04 — see Amendments)

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

| # | producer | what | loud? | purpose (2026-09-04) | released |
|---|---|---|---|---|---|
| 1 | `memory.ReserveOrForce` (`memory/acquire.go`) | a scan's file load — ONE ROW GROUP at a time where the footer is already decoded, the whole file otherwise | WARNs | `scan file load` | when THAT ROW GROUP is decoded (`fileSlot.releaseRG`), or, on the whole-file path, when the file's last one is |
| 2 | the pool reconcile after (1)'s reservation (`planner/physical/util.go`) | the pooled buffer's real capacity above what was reserved — WHOLE-FILE path only; see the row-group note below | silent | `scan file load` | with (1) |
| 3 | `scanSourceInner.trackScanBatch` (`planner/physical/plan.go`) | every decoded row-group batch | silent | `scan decoded batch` | when the batch leaves through `next()` |
| 4 | `scanSourceInner.trackPooledBuf` (same file) | the EAGER scan path's whole-file buffers, `cap(buf)`, no ceiling | silent | `scan pooled buffer` | at scan close (`releasePooledBufs`) — coarser than (1) |
| 5 | `HashJoin.reconcileHashMemory` (`engine/exec/join.go`) | the hash tables, arenas, chains and bloom | WARNs once when it crosses the budget | `hash join index` | **with the partition it belongs to** — the index is per grace partition, so an eviction frees it (#823, closed 2026-09-04); the rest at join Close |
| 6 | `HashJoin.freezeAccum` / `reconcileArrivalCharge` (`engine/exec/join_partition_arrival.go`) | a per-partition accumulator's fixed-capacity excess, and a batch's per-partition pieces above the batch itself | silent | `hash join partition store` | with the partition (`releaseStoreBytes`), rest at Close |
| 7 | `SpillManager.TrackBatch` (`engine/memory/spill.go`) | an operator batch charged past the budget so `ShouldSpill` can see it | silent | `spill tracking` | by the caller's own `ReleaseTracking` |

Two more exist on OTHER ledgers and are not part of a query's floor: the
worker's file cache (`worker/cached_store.go`) on the worker tracker, and
`scan.DecodeAheadIter`'s delivery-cursor group (`engine/scan/decode_ahead.go`)
on the decode window's ledger.

**Producer 2 on the ROW-GROUP path: the charge is the row group, and the slack
is bounded rather than reconciled.** The whole-file read reserves the file's
size and then reconciles up to the pooled buffer's `cap()`, because that buffer
is held for the file's whole decode and its excess is indistinguishable from
the file's own bytes. The row-group read does not reconcile. It charges the row
group's own byte range and holds it in a buffer from a pool bucketed by the
POWER-OF-TWO SIZE CLASS of that range, so the buffer is at most twice the row
group — no floor, no chosen number, the class derived from the row group
itself. The slack between the two is pool capacity: bounded by the row group,
handed to the next row group of the same class, and not this query's memory.
The ledger therefore understates a scan's resident bytes by at most one row
group per row group in flight, which is stated here rather than hidden, and
`physical.TestARowGroupIsHeldInABufferAtMostTwiceItsSize` is what holds the
bound.

FOUR other bucket rules were measured and are recorded so the next reader does
not re-derive them. Each is refused by a number, not a preference:

| rule | why it is refused | measured |
|---|---|---|
| the process-wide `readBufPool`, whose only rule is "big enough" | it also holds whole-FILE buffers, so a row group draws one | a 332-byte row group charged **105,900 bytes** — 319x the row group, 53x its file |
| the parquet chunk pool's size classes | a 64 KiB FLOOR is a tuning constant with a pool's manners | a 5 KiB row group held in **64 KiB** |
| bucket = the file's EXACT largest row group | compression makes that byte count unique per file, so no two files share a bucket and nothing is reused | TPC-H SF1 suite heap **+29.2%**, separated over five base/tip pairs |
| bucket = a size class, with buffers ALLOCATED AT the class | it does make every member of a bucket serve every request in it — one Get, no miss — but `sync.Pool` sheds at every GC, so the rounding is paid again and again rather than once per class | TPC-H SF1 suite heap **+6.6%**, separated over five base/tip pairs (its +10.9% wall in the same run was an ordering artefact: alternating the arms gave −1.1%) |

What ships is the fourth rule's bucket with the third's allocation: a
power-of-two class OF THE ROW GROUP's own byte range decides which bucket a
buffer is reused from, and a fresh buffer is allocated at the row group's own
size. A bucket's members can therefore differ in capacity, so one Get can miss
and allocate — the accepted cost, which is one allocation of exactly what the
row group needs, against a fraction of every buffer forever.

**#598's refusal of "reserve-and-overcommit" stands on the corrected count.**
Deliberately reserving past the budget for an arrival batch would be an EIGHTH
producer on a query tracker, and the argument was never about the ordinal: the
overcommitted bytes join the floor every downstream `Reserve` is measured
against. Seven silent-or-loud producers is a reason to add none, not a reason
the next one is cheap. A build that cannot reserve a whole arrival batch splits
it. (The half of that argument that rested on producer 5 being unreleasable is
retired by the 2026-09-04 amendment; the rest stands as written.)

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
to be released.** So the decode half waited on #823's per-partition index
state, and this ADR recorded it as deferred rather than bounded by something
that measures worse. **That dependency is discharged (2026-09-04): a grace
eviction now does release the join's index, so the admission-before-decode
direction can be re-measured on a tree where what it waits on can be freed.**
The earlier bounds this section carried — a headroom bound
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
HAVE arrived was unreleasable by a grace eviction, which was #823's open half:
the arena was global across the 64 partitions, so evicting one freed its column
data and left its chain entries. Per-partition index state was the fix, and it
is the 2026-09-04 amendment below.

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

### 2026-09-03 (the spilled-arm arc): #823's reclaim half, MEASURED and DEFERRED — **CLOSED 2026-09-04, see below**

Producer 5's "released" column said a grace eviction does not free the index.
Here is what that cost, so the next arc starts from a number instead of an
argument. A partition-on-arrival build of 2,000 rows in 256-row arrivals against
a 1 MiB budget, then every partition evicted — 64 of 64, all 64 `buildBatches`
slots nil, no build column data left in memory at all:

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

**Why per-partition ARENAS alone were not the fix**, even though they are the
contained change. A key's partition is a function of the KEY (`spillPartition`),
so a per-partition arena needs no widening of what the hash table stores —
`intIndex.Get(key)` can index `arena[spillPartition(key)]` with the same int32.
That is a real, bounded refactor. It reclaims the 27% and leaves the 62%: `used`
after a full eviction would fall from 106,320 to about 77,648, which is not a
floor. Rule 11 forbade shipping it: bounded by a model the same commit knows is
incomplete, and it leaves #823's own headline shape exactly where it was.

The whole fix — per-partition hash TABLES — is the amendment below.

### 2026-09-04 (the memory arc): the index is PER GRACE PARTITION, the ledger is conserved, and a request larger than the budget does not wait

Three things, one theme: what a query's tracker says must be what the query
holds.

**1. #823's reclaim half is CLOSED.** `intIndex` and `strIndex`, the arena, the
chain and the matched bitmap are one `joinIndexPart` PER GRACE PARTITION
(`engine/exec/join_index_parts.go`). A partition's table only ever addresses its
own arena, because a key's partition is a function of the key and the index uses
the SAME `spillPartition` the build's scatter and the probe's spilled-row
routing use — so the arenas follow the tables for free, and one routing function
makes "indexed in one partition, looked up in another" unrepresentable rather
than merely untrue. `spillOneInMemoryPartition` frees the evicted partition's
table, arena, chain and matched bitmap with its columns, and
`reconcileHashMemory` — now bidirectional — turns the smaller `indexBytes()`
into a tracker release under the purpose the growth was charged with.

What an eviction does NOT free is the BLOOM FILTER, and that is the whole of
what survives. On the fixture above, after evicting all 64 partitions:

| | before | after |
|---|---|---|
| `Tracker.Used()` (2,000 rows) | 106,320 | **9,728** |
| ...of which partition headers | — | 5,632 (64 × 88 B, fixed) |
| ...of which bloom | — | 4,096 |
| `Tracker.Used()` (20,000 rows) | 562,359 | **13,824** |

The floor is DERIVED (`evictedIndexFloor`), not measured, and the gate asserts
the derived number: `exec.TestEvictingEveryPartitionFreesTheIndex` and
`exec.TestEvictingEveryPartitionReleasesTheIndexCharge`. The bloom scales with
the key count and the headers do not, which is why the gate asserts the headers
are identical across an order of magnitude of build rows and the whole floor is
under 5 bytes per build row, against the 53 the defect measured.

A build that CANNOT evict keeps one table, one arena and one chain: `partMask`
is 0 for the flat build, the spilled-partition replay and the key-only builds,
which folds the partition selection to part 0. The two paths share one body,
because a probe that indexed a different table from the one the build wrote is a
silently wrong answer.

**2. The arrival reservation is RECONCILED to what the build kept, and a
negative ledger is a defect that says so.** One arrival batch is charged once,
as `hashBuildBytes(b)`, and its rows were then released — or not — by two
different formulas, neither of them a share of that charge. Rows written
straight to an already-spilled partition released `hashBuildBytes` of a freshly
minted per-partition batch, paying the per-column fixed overhead once per
partition against a batch that paid it once: 24,932 bytes charged, 30,372
released over 63 partitions, 1.22x, over-releasing 5,440 bytes EVERY BATCH. Rows
appended to an in-memory partition charged `partMemory` the tight per-row data
bytes, which is less than their share, leaking about 1,000 bytes per batch the
other way. Which way a build drifted followed how many partitions had spilled by
the time each batch arrived — pressure and timing, not the query — and at
100,000 build rows `Tracker.Used()` reached **−867,561** against a 1 MiB budget
while the join's index was 802,816 bytes of live heap.

`partitionAndIndexBatch` now returns what it left RESIDENT and
`reconcileArrivalCharge` brings the one reservation to that figure.
`Tracker.Release` WARNs once per tracker when it drives `used` below zero; it is
NOT clamped, because a clamp hides the producer. Gates:
`exec.TestAGraceBuildsLedgerIsConserved` (four build sizes; `used == trackedMem
== partMemory + index charge`, never negative, zero at Close) and
`wadjet.TestNoQueryOverReleasesItsMemoryLedger` (the WARN counted end to end).

**3. Every forced charge NAMES its purpose, and a refusal reports it.**
`ForceReserveFor(n, purpose)` / `ReleaseForced(n, purpose)` count OUTSTANDING
forced bytes per producer (`memory/forced.go`), and a refusal appends
`, of which forced=412074 by "scan file load"` — the total plus the largest when
several purposes hold bytes, and nothing at all when nothing was forced. This is
the instrument #789's investigation did not have: `used=465738` with no way to
say whose bytes those were. `Transfer` moves bytes without entering the census,
because a transfer is not a new charge.

**4. #853: a reservation larger than the WHOLE budget does not wait.** Relief
cannot free what does not exist and `ReserveBlocking` polls a condition that is
false for every value `used` can take, so `n > budget` takes the documented
forced path immediately — same path, same WARN, same ledger effect, without the
2 s per parquet file load that
`coordinator.TestCorrelatedRerunPaysTheFullReserveWaitPerOuterRow` measured as
4.10 s for two outer rows. The wait stays for `n <= budget`.

**What #789 has left, and what it does not.** After S2a closed the file half and
this arc closed the ledger drift, the remaining run-to-run variation in `used`
at a fixed instant is the scan's LIVE read-ahead: the row groups in flight are
resident, charged and bounded by the budget, and at `GOMAXPROCS=1` the floor is
one number on every run. That is ADR-0013's legal nondeterminism, not a defect,
and the property that must not vary — the VERDICT — is gated by
`wadjet.TestAJoinAtAFixedBudgetIsDecidedByTheQueryNotTheScheduler`. The DECODE
half (producer 3 charging after the allocation) stays deferred, and its
dependency on #823 is now discharged: an admission before the decode can be
retried on top of a join index that a grace eviction really does release.

**A build that SPILLED does not publish its bloom.** The bloom is built at the
end of the build from what the index holds, and a spilling build's index does
not hold every key the build saw — rows whose partition had already spilled are
never indexed, and (since this amendment) an evicted partition's table is freed.
`BloomPushdownOp` runs UPSTREAM of the partition router, so a probe row it
rejects never reaches the join at all, not even to be written to its partition's
probe file. Measured at the operator pair on a 40,000-row build at a 1 MiB
budget with 60 partitions evicted: 25,262 of 40,000 probe rows whose key IS on
the build side were rejected. The bloom stays valid for the IN-MEMORY probe
path, whose key set is exactly what the index holds, so what declines is the
pushdown and not the filter (`exec.TestASpilledBuildDoesNotPublishItsBloom`).

