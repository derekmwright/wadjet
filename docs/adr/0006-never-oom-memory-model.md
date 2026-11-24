# ADR-0006: Never-OOM memory — shared pool, ownership ledger, spill-everywhere

Status: Accepted (accounting overhaul landed 2026-05-30, 35f6730; recorded 2026-07-25; amended 2026-08-17, 2026-09-03, 2026-09-04, 2026-09-18, 2026-09-19, 2026-11-21, 2026-11-22 and 2026-11-23 (twice) — see Amendments)

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
| 3 | `scanSourceInner.trackScanBatch` (`planner/physical/scanner_source.go`) | every decoded row-group batch | silent | `scan decoded batch` | when the batch leaves through `next()` |
| 4 | `scanSourceInner.trackPooledBuf` (same file) | the EAGER scan path's whole-file buffers, `cap(buf)`, no ceiling | silent | `scan pooled buffer` | at scan close (`releasePooledBufs`) — coarser than (1) |
| 5 | `HashJoin.reconcileHashMemory` (`engine/exec/join.go`) | the hash tables, arenas, chains and bloom | WARNs once when it crosses the budget | `hash join index` | **with the partition it belongs to** — the index is per grace partition, so an eviction frees it (#823, closed 2026-09-04); the rest at join Close |
| 6 | `HashJoin.freezeAccum` / `reconcileArrivalCharge` (`engine/exec/join_partition_arrival.go`) | a per-partition accumulator's fixed-capacity excess, and a batch's per-partition pieces above the batch itself | silent | `hash join partition store` | with the partition (`releaseStoreBytes`), rest at Close |
| 7 | `SpillManager.TrackBatch` (`engine/memory/spill.go`) | an operator batch charged past the budget so `ShouldSpill` can see it | silent | `spill tracking` | by the caller's own `ReleaseTracking` |

Two more exist on OTHER ledgers and are not part of a query's floor: the
worker's file cache (`worker/cached_store.go`) on the worker tracker, and
`scan.DecodeAheadIter`'s delivery-cursor group (`engine/scan/decode_ahead.go`)
on the decode window's ledger.

**Producer 7's "released by the caller's own `ReleaseTracking`" is a PAIRING,
and one caller released without charging** (#862, fixed 2026-09-04). The
HashAggregate's legacy raw-row buffer accumulated `spillBufferBytes` at its
append site and never called `TrackBatch`, while THREE sites released it —
`flushSpillBuffer`, `Finalize`'s drain of the unflushed tail, and `Close`. So
every query that took that branch gave the tracker back bytes it never took;
measured at 931,840 released against zero charged, `used` at -165,652. A
negative ledger is not cosmetic: from there every admission is measured against
a floor below the memory that exists, which is the one thing this ADR's model
cannot survive. The rows are the operator's (`ToRows` copies them out of a
batch the pipeline releases immediately), so the missing half was the charge,
and adding it also makes the buffer visible to the spill trigger — which is
what the release sites' own comments already assumed. Gated by
`exec.TestTheRawRowBufferChargesWhatItReleases`, which asserts conservation and
that a reservation the budget cannot hold is still REFUSED after the operator
closes, and by the aggregate cells `wadjet.TestNoQueryOverReleasesItsMemoryLedger`
could not carry while the defect was open.

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

### 2026-09-18: an outer join's ON RESIDUAL is evaluated AT the join, crosses a stage boundary by IDENTITY, and a keyless outer join is not a cross join

Two positions this arc settled, both about WHERE a predicate runs and therefore
about which build the memory model has to account for.

**The residual's evaluation point.** PostgreSQL evaluates ANY `ON` expression
for any join kind. An INNER join may lift a non-equality conjunct into a filter
ABOVE the join, because an inner join has no padded rows to lose; an OUTER join
may not, and may not push it into the preserved side's scan either — both
delete rows the join owes. The conjunct is therefore evaluated **at the join,
per probe row against each candidate build row, before the padding is
decided**: a probe row whose whole candidate chain fails the residual is
UNMATCHED and a LEFT/FULL join still emits it NULL-padded, and a build row
counts as matched only when some probe row passed BOTH the key and the
residual, which is what the RIGHT/FULL unmatched flush consults.

That point is not negotiable, so the only question left is what may be
evaluated there. The answer is the ENGINE'S OWN EXPRESSION COMPILER — the same
`expr.Compile` the inner join's lifted filter runs above the join — rather than
a second, smaller interpreter beside it. A seam with two evaluators has two
semantics, and which one a query reaches is then decided by its join kind
(#1153: a `CAST`, a function call and `LIKE` were a plan refusal on all three
outer kinds and answered on the inner one). What remains unevaluable is a
predicate whose value depends on a RELATION the join does not have — a subquery
in `ON` — and that refuses loudly, naming the construct.

**The residual crosses the stage boundary by IDENTITY.** The evaluation point
above is WHERE the predicate runs; what it runs OVER is the other half, and on
the distributed path the two are decided in different places. A residual is the
only part of an `ON` clause that travels to a worker as TEXT, and a `Project`
emits no stage — so a residual over a renamed relation (a derived table, a CTE)
arrived at a fragment whose sides publish the BASE column names while the text
still spelled the subquery's aliases. Nothing resolved, the evaluator's unbound
slot is SQL NULL, and a LEFT join answered its whole preserved side padded, in
silence, on shapes that had been a loud refusal.

The rule is `docs/design/window-key-ownership.md`'s: a reference binds by the
OCCURRENCE that produced it, and a name is derived from the identity for
publication, never the reverse. The join's equi-keys already made this trip
re-spelled (`resolveShuffleKey`); the residual's leaves take the same path, and
the SIDE travels with the name, because two arms of one join routinely re-spell
to the same source column and a bare name binds probe-first. Under it,
`physical.JoinResidualUnresolved` refuses at the fragment when a leaf the
rewrite could not reach does not resolve against either DECLARED side: an
unbound residual leaf is a REFUSAL, never a NULL at run time.

**A keyless outer join is a hash join, not a cross join.** The 2026-09-03
amendment above says grace partitioning requires a routed probe, and that a
CROSS join's does not route. An outer join whose `ON` contains no bare-column
equality is keyless, which LOOKS like the same shape and is not: the build
degenerates to ONE empty-key chain, the probe still looks that key up, so
`probeRoutesByPartition` (`JoinType != CrossJoin`) holds and the build
grace-partitions, evicts and spills exactly as a keyed one does. Measured at
the boundary: the keyless outer residual ANSWERS at the 256 KiB budget where
#832's cross join refuses, both cells in the type-matrix spill sweep
(`join_residual_keyless_at_the_cross_join_budget` beside
`join_computed_wide_refuses_when_the_build_does_not_fit`).

The cross join's refusal is unchanged, and so is its reason: an INNER join on
an expression has its ON lifted into a filter above a CROSS join, whose probe
reads every build row, so its build must fit the budget and says so loudly when
it cannot. Widening what a residual may contain does not move that boundary —
it moves shapes off the refusal and onto the routed-probe side of it.

### 2026-09-19: a join's build OWNS the row set it stores (#1189)

The 2026-09-03 amendment above establishes that a CROSS join's probe does not
route by a key and therefore walks `buildBatches` DIRECTLY, entry by entry, for
every probe row. That is a second consumer of the stored build, and it reads
something the arena-driven consumers never look at: each stored batch's
SELECTION VECTOR.

A build batch's live rows ARE its selection vector. A filter pushed onto the
build side marks the rejected rows rather than copying the survivors out
(CLAUDE.md, "selection vectors over copying"), and every predicate over one
side of a cross join is pushed there — `EXPLAIN SELECT … FROM c2 CROSS JOIN c1
WHERE c1.f` plans `Join: cross join` over `Filter: [c1.f]` over `Scan: c1`.

**Decision: the build owns the row set from the moment it stores the batch,
and every rewrite or re-walk of the stored batches reads that same row set.**
Identity here is ADR-0026 §4's — the producing relation and row, never a slot
number that drifts — applied to the build relation as a whole: which ROWS the
build holds is a property of the build, not of whichever operator produced the
batch or of the representation the build later chooses for it.

Four sites touch those batches, and three of them broke the rule:

| site | what it did | what it does |
|---|---|---|
| the flat store (`join.go`, `Build`) | kept the PRODUCER's selection slice — `exec.Filter` hands out a slice of one reusable buffer and overwrites it on the next batch; `Detach` claims the column vectors (ADR-0016) and says nothing about the row set | copies the row set into the join's own memory (`ownBuildRowSet`) |
| `consolidateBuild` | merged the batches, copying RAW rows at cumulative offsets so the arena's refs stay valid, and dropped the selection vector | carries a merged ascending selection vector forward |
| `PruneBuildColumns` | rebuilt each stored batch without its `Sel` | carries it |
| the key-swap rebuild (`FixKeyAssignment`) | re-indexed raw rows | walks the row set (`buildRowAt`) |
| partition-on-arrival (`join_partition_arrival.go`) | already compacts rows out of the arrival, so a frozen batch is dense by construction | unchanged |

What it cost while it was open: `b2 CROSS JOIN b1 WHERE b1.f` over a
three-file build answered 52 rows where PostgreSQL 17.11 answers 39 — every
RAW row of every batch that REACHED the build, which is four of that
relation's five rows, since the third file holds one FALSE row and a batch the
filter empties never arrives at all (`exec.Filter` returns nil for it). `GROUP
BY b1.f` reports thirteen surviving rows whose own `f` is FALSE: one rejected
build row per probe row. Where every arriving batch keeps at least one row —
the ordinary case, and the one measured by the four pinned outer-join cells — what
comes back IS the whole unfiltered relation per probe row. (The 52 is 13 x 4,
not 13 x 5 — measured by the earlier measurement of this amendment.) Fifty-seven of
the sixty-six NoREC row-count mismatches a 200-database SQLancer run reached
were this one shape; sixty-five of the sixty-six are gone.

**Boundary, measured.** A policed relation's build side was never at risk: the
security projection standing between the scan and the join materialises the
filtered rows, so those batches are dense and carry no selection vector at all
(the same relation, predicate and doors answer 15 rows as a masked analyst at
`1c2b4d25` and 24 as the admin). The refusal this amendment's 2026-09-03
predecessor describes is unchanged — the merged selection vector is bounded by
the LIVE row count, so a build that fit the budget still fits, and one that did
not still refuses, re-measured five times over a filtered build in
`exec.TestACrossJoinBuildOverTheBudgetStillRefusesLoudly`.

Gates: `exec.TestAJoinsBuildOwnsTheRowSetItStores` (one cell per site above),
`coordinator.TestCJTheBuildRowSetSurvivesTheBatchBoundary` (45 cells on five
arms against PostgreSQL 17.11),
`server.TestCJALiftedFilterOverAPolicedBuildPublishesOnlyThePolicysRows` and
`server.TestCJARowFilterOverACrossJoinsBuildKeepsItsRowsOut` (nine doors),
`pgwire.TestCJACrossJoinsBuildColumnDeclaresItsOwnType` (both format codes).

### 2026-10-03: a partition replay emits the join's own output, whatever the other side's size (#1359)

The grace hash join emits an evicted build partition's rows from a REPLAY at
the end of the probe (`HashJoinProbe.NextFlush`): a temporary join over the
partition's build file, probed with the partition's probe file. For a RIGHT or
FULL join, the replay is the only place an evicted partition's unmatched build
rows — the preserved side — are emitted (the main join's own flush skips
evicted entries).

**Decision: the drain emits every preserved row of a partition exactly once,
in the join's declared output shape, regardless of the other side's size —
zero included — and the in-memory and replayed partitions read ONE rule for
that shape.** The rule is the one `FlushUnmatchedRows` already read: the probe
schema the join recorded under its lock (`recordProbeSchema`), else the
plan-declared `ProbeSchemaHint`; the flushing probe's own output narrowing
(`OutputFilter`, `OutputExcludeProbe/Build`, plan settings every clone carries
from construction); and every naming input of the join's output schema
(`BuildTableAlias`, `BuildColOrigins`, `QualifyAllBuildCols`).

What it cost while it was open: the replay took the probe schema and the
narrowing from fields the FIRST probe batch copied onto the join. A probe side
that produced no rows copies nothing, so the replay's unmatched rows came out
with no probe columns and every build column, which the consumer read
positionally: under a forced eviction at 4 MiB,
`SELECT p.id, p.k_int, p.v_dec, p.v_arr, e.k_int, e.ey FROM sj_e e FULL JOIN
sj_p_one p ON e.k_int = p.k_int` answered `1, 1, 1.25, {1,2}, 1, NULL` (the
preserved key read again as the empty side's) where PostgreSQL 17.11 answers
`1, 1, 1.25, {1,2}, NULL, NULL`, and the RIGHT join over the 300-row `sj_p_nullk` with
`ORDER BY p.id LIMIT 4` refused with `cannot store int64 into ARRAY vector`. Whether a
partition is evicted at all is the budget's and the moment's, which is why it
was reported as intermittent; with the eviction forced it failed every time.
The temporary join also dropped `BuildColOrigins` and `QualifyAllBuildCols`,
so even with probe rows the replayed rows named build columns differently from
the in-memory partitions' rows of the same join.

LEFT and INNER joins over an empty side, and RIGHT / FULL joins whose BUILD
side is the empty one, were measured on the same grid and never differed: an
empty build evicts nothing, and a probe-preserved row is emitted by `Execute`
from the batch that carries it.

Gates: `exec.TestArcSJGraceDrainEmitsPreservedRowsOverAnEmptySide` (1 600
operator cells, the eviction asserted), `exec.TestArcSJReplayNamesBuildColumnsAsTheJoinDoes`,
`wadjet.TestArcSJEmptySideJoinEveryBudget` / `…Intermittent` /
`…BareColumnsForced` (640 cells against PostgreSQL 17.11 at four budgets, with
and without the forcing knob), `coordinator.TestArcSJEmptySideJoinEveryArm`
(five arms), `server.TestArcSJAnEmptySidedSpilledJoinPublishesThePolicysValueOnEveryDoor`
(nine doors).

### 2026-11-21: a scan's read-ahead is admitted by what its decode will hold (#1706; #789's decode half narrowed, not closed)

**The mechanism, measured.** At a 512 KiB budget, `SELECT DISTINCT * FROM typemx
a JOIN typemx b ON b.id = a.id ORDER BY a.id DESC LIMIT 3000` — #1706's
statement, the spilled arm of `coordinator.TestM1AMergedOrderIsTheQuerysOrderAtScale`
— answered or refused on the same data depending on which of two goroutines
reached the query's tracker first. One decoded row group of that 22-column
`SELECT *` is 250,628 bytes, 48% of the budget. The scan's worker decodes the
next row group while the hash join build is still taking the last one, and
charges it AFTER the decode, forced (producer 3); the build `Reserve`s for the
batch it has just taken (294,628 bytes). When the read-ahead's charge landed
first, the build's reservation did not fit, eviction had nothing built to free,
the build split the batch, and a SECOND read-ahead row group — admitted by its
load because its COMPRESSED bytes (88,195) fit beside a still-small build —
landed between the chunks: two decoded row groups held 502,758 bytes and the
build refused with `forced=508390, largest 502758 "scan decoded batch"`. When
the build's reservation landed first, the read-ahead's charge went over the
budget instead, the next load's reservation asked the build for relief, and the
query answered.

The two events were ordered without a sleep by a test-only hook
(`physical.ForceScanReadAheadOrder`): DecodeFirst lets every scan worker run
ahead as far as it can before EACH arrival reservation of the grace build;
ConsumerFirst lets a worker decode only what its consumer has asked for. With
the base's read-ahead (the rule's kill switch off, where every read-ahead is
admitted as it was at the base), DecodeFirst refused the two #1706 statements
and a wide `SELECT *` self-join under a LIMIT on 50 of 50 runs each, and
ConsumerFirst answered 50 of 50, in EACH of eight configurations — GOMAXPROCS
1, 2, 4 and 24, each with and without busy goroutines — the same pair the
diagnosis measured with a 2 ms sleep on either side (40/40 and 0/40). Free, the
base refused 146 of 4,800 runs of those three statements — 0 to 18 of 200 per
statement across the same eight configurations.

The hook's first form did not order every run, and the figures above are the
corrected hook's. It counted a worker as parked when it was inside a send on a
full channel; but a receive from a full channel moves the blocked sender's item
into the buffer and makes the sender runnable while it still counts as sending,
and on one P the sender need not run before the check, so the hook returned
with that worker's next decode not started. With the kill switch off,
DecodeFirst then ANSWERED in 15 of 15 fresh processes at GOMAXPROCS 1, and in 11
of 20 at GOMAXPROCS 24 under 24 busy loops. The DecodeFirst cells first measured
with it at GOMAXPROCS 1 proved nothing: there the evidence was the free order
alone, the base refusing 32 of 600 runs and the rule 0 of 600. The hook now also
requires that no receive is in flight, that the buffer holds exactly the items
sent and not yet received, and that every worker has started; with it the
kill-switch-off contrast refused in 30 of 30 fresh processes and 20 of 20 warm
runs at each of GOMAXPROCS 1, 2, 4 and 24, and in every fresh process under busy
loops at each.

**Why compressed-byte admission let the read-ahead take the budget.** The load
admission (`rgSlabs.advance`, `ReserveOrForce`) was the only gate in front of a
decode, and it admitted a row group on its byte range, 35% of what the decode
then forced; and its floor — "a scan holding nothing never waits" — counted
resident row-group BUFFERS, which are released when the decode ends, so a single
decode worker always looked empty and was always admitted. Nothing in front of
the decode measured the decode.

**The rule now** (`planner/physical/scan_decode_admission.go`, one rule for the
row-group and the whole-file load). A worker may decode a row group AHEAD of its
consumer only when the room for what that decode will hold exists now — the
bytes it still has to land, plus the decoded batch, plus the decode's transient.
The admission is a DECISION, not a reservation: an admitted decode takes exactly
the charges it took before (the load's, then the decoded batch's), so admitting
it puts nothing on the ledger the base did not. A first form that reserved the
estimate for the length of the decode was measured and dropped: a reservation
standing in for a decode, sized for its peak, refused a CROSS join's build —
which cannot spill — on `SELECT COUNT(*) FROM typemx a JOIN typemx b ON
UPPER(a.c_str) = UPPER(b.c_str)` at 512 KiB in 2 of 10 runs, and eleven
computed-key cells of `wadjet.TestTypeMatrixAnswersTheSameUnderEveryMemoryBudget`,
all of which answer at the base. When the room is not there, or the footer
cannot bound the decode, the worker neither waits nor forces: it hands the row
group to the consumer undecoded, the consumer decodes it on its own goroutine
when it asks for it, and the scan decodes every remaining row group on demand.
The floor: a decode the consumer is waiting for is never refused and never
waits on the budget — the same load, the same forced and REPORTED `scan decoded
batch` charge as before, except that a scan on demand also skips the load
admission's bounded wait (2 s, then force; `rgSlabs.advance` and the whole-file
`fileSlot.ensureLoaded`) and takes the relief-then-force path at once. That
wait was there to overlap a READ-AHEAD load with the consumer's work; on the
consumer's own goroutine it is the pipeline stalling for the whole wait.
Measured on TPC-H SF0.01 at a 2 MiB budget before that change: Q05, Q08, Q09
and Q10 took 2.06, 4.05, 4.07 and 4.09 s — whole multiples of the wait —
against 48, 41, 38 and 338 ms at the base; after it, 35, 43, 41 and 91 ms. Without a budget the rule does not touch
the ledger. Kill switch `WADJET_SCAN_READAHEAD_ADMISSION=0`.

The estimate is the footer's, and it covers the PEAK, not the final batch:
per projected column, the decoded vector (fixed width × rows, or offsets plus
the chunk's uncompressed bytes for a PLAIN byte array) and its null bitmap,
plus a transient of `3 × uncompressed + 16 × rows` for each of the GOMAXPROCS
largest columns, because the native decoder decodes that many columns at once.
Measured per column type, one row group, everything the decode ALLOCATED
(garbage included) against the estimate: every flat type of the type matrix,
all-NULL string / BIGINT / DECIMAL(38,10) columns, 512-byte strings and the
DuckDB-written TPC-H customer file's PLAIN columns are covered, the decoded half
exactly (it equals `MemBytes` for a PLAIN chunk) and the whole at 1.2-1.5x what
the decode allocated (the tightest, `c_str`: 74,352 against 58,656). A whole
22-column row group allocated 652,096-690,640 bytes across GOMAXPROCS 1-24
against an estimate of 428,171 at 1 and 1,373,276 at 24: at low parallelism
the allocation total counts columns' garbage that is never live together, so
there the per-column measurement and the decoder's concurrency limit are what
bound the live peak — inferred from those two, not sampled. A byte-array chunk
with a DICTIONARY page has no estimate — its decoded bytes are the entries
expanded per row, which no footer field carries (DuckDB's `c_mktsegment`:
2,514 uncompressed bytes decoding to 21,196) — and a scan that meets one is on
demand. A wrong estimate stays safe: the decode is still charged, forced and
reported, after it, exactly as before.

**How it differs from the two attempts refused above.** Both reserved before
the decode and WAITED for the budget — on a clock, or until the source held no
untaken decoded bytes — so a decode that did not fit stalled the scan (156 s and
~90 s against 0.25 s) and, because the second form resumed the moment the
consumer TOOK the previous batch, the decode still raced the consumer's
reservation for it (17-19 of 20). This rule waits for nothing: a decode that
does not fit is moved to the consumer's goroutine, where it runs after the
consumer has finished with the previous batch, reservation included — the one
order that answered. The pinned overdraw shape those attempts were measured on
(4,000 rows of 512-byte strings, 1 MiB) now answers 20 of 20 free and in both
forced orders, with the unbudgeted engine's answer, the whole gate (26 budgeted
runs, an unbudgeted reference and the contrast) in under 4 s
(`wadjet.TestAScansDecodedReadAheadNoLongerOverdrawsTheBudget`, which replaces
`TestAScansDecodedReadAheadStillOverdrawsTheBudget` and keeps its refusal as
the kill-switch-off contrast arm).

**Correction.** "What #789 has left" above says the property that must not vary
at a fixed budget — the VERDICT — is decided by the query, not the scheduler,
gated by `wadjet.TestAJoinAtAFixedBudgetIsDecidedByTheQueryNotTheScheduler`.
That was true for the narrow projections that gate and its floor twin measure
(the floor gate holds 35,776 decoded bytes at the build's first reserve) and
FALSE at the base whenever one decoded row group was a
large part of the budget: the wide shapes above. With this rule it holds for
them too: over the same eight configurations, 0 refusals in 8,800 runs — 200
per statement free, 50 per statement in each forced order (measured with the
corrected hook, against the base's 50 of 50 DecodeFirst refusals in every
configuration), plus 50 each of the aggregate, the sort and two cross joins,
free — every answer the unbudgeted engine's. Gates:
`wadjet.TestAWideScansVerdictAtAFixedBudgetDoesNotFollowTheReadAheadOrder` (five
wide shapes, both forced orders and the scheduler free, against the unbudgeted
engine), `wadjet.TestWithoutReadAheadAdmissionTheDecodeFirstOrderRefuses` (the
hook still orders: the base's verdict, kept live),
`physical.TestTheFooterEstimateCoversADecodesPeak`,
`physical.TestADictionaryEncodedByteArrayHasNoFooterEstimate`,
`physical.TestWithoutABudgetEveryReadAheadIsAdmittedForFree`.

**What it costs.** Under a budget, a scan that defers once loses its decode
overlap for the rest of its life: its row groups decode serially on the
consumer's goroutine. Measured on TPC-H SF0.01, 22 queries, three interleaved
base/tip rounds of two passes each: with NO budget 555 ms base against 547 ms
tip (the rule is one branch there); at 8 MiB 750 against 711 ms; at 2 MiB
1.95-8.01 s with one query refused in five of six passes at the base, against
1.57-1.70 s with none refused at the tip — the base's time there was the load
admission's 2 s waits behind its read-ahead. The micro-benchmarks of the
changed packages (`internal/planner/physical`, the `HashJoin` benchmarks of
`internal/engine/exec`) moved within their noise (no benchmark at p < 0.05,
geomean +3% on a shared host). Verdicts move only from refusal to answer on
what was measured: the embedded `forced_refusal` fixture (a 60,000-row
self-join), which refused at every budget from 384 KiB down at the base — every
refusal `largest: … by "scan decoded batch"` — answers at every budget from
1 MiB down to 12 KiB, 30 of 30 runs each with the unbudgeted row count, and
refuses from 10 KiB down on the join's own forced partition headers;
`wadjet.TestAQueryThatRefusesNamesWhatWasForced` and
`pgwire.TestAForcedRefusalReachesTheWire` read their refusal at 8 KiB now.

**What it does not cover.** The file-level scan that nested (ARRAY / ROW / MAP)
projections and pushed-down LIMITs take (`scanWorker`) decodes a whole FILE per
batch and is unchanged; it has no row-group read-ahead to admit. The forced
charge itself is unchanged — producer 3 still charges after the decode, and a
decode on demand can still carry a query past its budget when the consumer
leaves no room for one row group; that charge now lands on the consumer's own
goroutine, after its reservations, so the scheduler no longer orders the two.
A read-ahead admitted while a consumer has taken a batch and not yet reserved
for it still races that reservation, once: the admission sees the room the
consumer is about to claim. A further decode is then admitted only against room
that exists beside the first — the base admitted it on its compressed bytes
alone, which is what its refusals needed — and the measured shapes answer in
both orders — but the window is narrowed, not closed, for a consumer that
cannot proceed in the budget minus one row group. The refusal `coordinator.TestF1AKeylessJoinAsksForWhatItNeeds/480`
pins is not this mechanism: it refuses at the build's first arrival against
3.5 MB of forced `spill tracking` held by the finished DISTINCT aggregate's
partition states, on every run at the base and the tip, in both orders.

### 2026-11-22: an aggregate's presize under a budget is bounded by its input estimate (#1706, the third site)

Producer 7 (`spill tracking`) is charged by `HashAggregate.reconcileGroupMemory`
at the CAPACITY of the group state, and the unhinted presize is 4096 slots. On
the typed-generic path (a STRING key beside an INT64 key) that is 458,796
bytes, 88% of a 512 KiB budget, for a table holding two groups. A partitioned
aggregate runs one clone per worker, and every hash join's build runs on its
own goroutine, so the two grouped LATERAL bodies of
`coordinator.TestN1ATwoGroupedLateralsPublishTheirOwnColumns` ran six clones
at once: each charged 458,796 on its first two-row batch, crossed its own
clone drain bound, wrote a run file and released. A join build whose first
arrival reservation landed inside that window refused with the gate's exact
figures (`used=1376347, requested=24692, … forced=1376347 by "spill
tracking"`); one landing after it answered. Holding one aggregate's release
for 100 ms refused 319 of 320 runs (either aggregate, both statements,
GOMAXPROCS 1, 2, 4 and 24, 20 runs each); delaying the join's reservation
50 ms instead answered 160 of 160. The verdict was the scheduler's.

This is the rule this record already applies to producer 5 ("a charge for
memory that does not exist yet", 2026-09-03): pre-sized capacity the input
cannot fill. Under a budget the aggregate's initial capacity — its group
index, key mirrors, typed-key index and flat accumulators, and the tables a
drain resets to — is now capped at the planner's input-row estimate
(`HashAggregate.presizeBound`; since the second 2026-11-23 amendment, each of
the k partitions of a partitioned aggregate — the primary and its clones — is
capped at its share of that estimate, ceil(estimate / k), as a cap only). It is
a cap on an allocation, not a bound on the groups or a new constant: every
structure still grows on demand, and an estimate that is low costs doublings,
never an answer. Without a budget nothing changes. With it the two statements'
clones charge about 750 bytes each, never drain, and answered 800 of 800 runs
over the same configurations, free and in all four forced orders. Gate:
`exec.TestABudgetedAggregatePresizesNoMoreThanItsInputCanFill`.

**What it does not cover.** The class is wider than the presize: a forced
`spill tracking` charge held by one breaker's clones when a concurrently
building join first reserves decides that join's verdict, and the cap removes
the presize part of it, not every refusal over a small input. Measured at this
change, not fixed:

- `coordinator.TestArcLTACorrelatedBodyIsEvaluatedPerOuterRowOnEveryArm` over
  its tiny `lt_i` fixture, spilled arm: the cap closed the `groupLimit/eq`
  cells (refused in each of 6 runs before it, in none of 6 after), but the
  DISTINCT lateral cells (`distinct/eq`, `distinct/mixed` under LATERAL, LEFT
  and comma) still refuse `memory budget exceeded` at the tip on a subset that
  changes from run to run: 6, 5, 4, 6, 4 and 3 of those six cells over 6 runs
  (5, 5 and 3 in the base runs counted cell by cell). The input estimate is
  small there, so this is a counterexample to "small estimate, closed"; its
  mechanism is unresolved;

- the same two statements over a 10,000-row `lat_item` (six groups; the
  estimate is above 4096, so the presize stands) answer free and refuse 10 of
  10 with one aggregate's release held;
- `coordinator.TestF1AKeylessJoinAsksForWhatItNeeds/480`'s pinned refusal is
  this class with REAL group state (DISTINCT `c_i32` over 5,000 rows, about
  118 KB per clone): free it refused 10 of 10; with the join's reservation
  delayed 100 ms it answered PostgreSQL's 3 in 4 of 10. The 2026-11-21
  amendment's "on every run at the base and the tip, in both orders" held for
  the two read-ahead orders it measured and is false for this one; the one
  sighting of that pin answering is this ordering, not an unexplained
  accounting drift.

Making those verdicts independent of the order — what a join's first
reservation may wait for, or what an upstream breaker's clones may hold
while a sibling build admits — is a decision this record has not taken.

### 2026-11-23: the group-state pool's first chunk takes the same bound (#1706, the fourth site)

**The mechanism, measured.** After the presize cap above, a budgeted
aggregate's index, key mirrors and flat accumulators followed its input
estimate, but its group-state pool did not: `groupStatePool.alloc` appended a
4096-slot chunk (98,304 bytes) on the first group, whatever the input, and
`groupMemoryUsage` charges chunk capacity, so `reconcileGroupMemory` forced
about 107 KB of `spill tracking` for ONE group — 98,304 of it the pool. The
lateral cells of `coordinator.TestArc1681dBoolCorrelatedBodyEveryArm`
(`outer/<col>/eq/lateral`, a correlated `CROSS JOIN LATERAL (SELECT count(*)
…)`) and the DISTINCT lateral cells of
`coordinator.TestArcLTACorrelatedBodyIsEvaluatedPerOuterRowOnEveryArm`
(`distinct/eq`, `distinct/mixed` under LATERAL, LEFT and comma) run that
aggregate partitioned: the clone count is the pipeline's worker count,
`runtime.NumCPU()` (24 on the measuring host; GOMAXPROCS does not change it),
and every clone that receives a row charges. Six or seven clones received a
group each — 535-643 KB forced on a 512 KiB budget before the join above them
first reserved. The clones release in `HashAggregate.Close` as the drain
emits each partition, on the drain's goroutines, while the join build already
consumes the first partitions' rows; the first event that differs between a
refusing and an answering run is how many of those releases land before the
join's first arrival reservation (one: `used=535461, requested=24632`,
refused; two: `used=428380`, answered). The 2026-11-22 amendment's
"mechanism unresolved" for the LT DISTINCT cells is this one: the DISTINCT
state there is one group of a few bytes per clone; the charge was the pool.

**The rule now.** Under a budget the pool's first chunk is the aggregate's
`presizeBound(4096)` (the input estimate, or a clone's parent estimate; for a
partitioned aggregate, its share of it since the second 2026-11-23 amendment), and
later chunks double from it up to 4096; without a budget, or with an estimate
of 4096 or more, the chunks are exactly the base's. A clone of those cells now
charges 9,173 bytes, so even all 24 clones and the join's first arrival fit
in 512 KiB whichever releases first: the verdict no longer has an order to
depend on. Base against tip, 30 fresh processes per cell, at GOMAXPROCS 1, 2,
4 and the default, with the join's arrival delayed 100 ms, and with every
aggregate release held 100 ms (also at GOMAXPROCS 1): the 1681d lateral cells
refused in every base run of the free and held orders (160-232 refusals over
30 runs; 12 of 30 runs under the delay) and in no tip run; the six LT DISTINCT
cells refused in every base run of the free and held orders (155-180 over 30
runs; none under the delay) and in no tip run, and none of the 33 aggregated
LATERAL cells that gate tolerated a refusal on refused at the tip. Every
answer is PostgreSQL's, so the two gates' tolerated-refusal lists
(`bcBudgetBounded`, `ltBudgetBounded`) are gone and their spilled arms assert
the rows. Gate: `exec.TestABudgetedAggregatesGroupStatePoolIsBoundedByItsInput`.

**What it does not cover, and a correction.**
`coordinator.TestF1AKeylessJoinAsksForWhatItNeeds/480`'s control (`SELECT
COUNT(*) FROM (SELECT DISTINCT c_i32 FROM typemx) x JOIN (SELECT DISTINCT k
FROM typemx_dim) y ON x.a = y.b`) is unchanged by this rule: its estimate is
5,000 rows, so the pool keeps its 4096-slot chunk. The 2026-11-22 amendment
called its charge "REAL group state … about 118 KB per clone"; measured, a
clone of it holds about 200 groups in about 155 KB, of which 98,304 is that
pool chunk (4,800 bytes used), 16,384 the string index, about 13 KB the key
mirrors and about 24 KB the per-group state. The 24 clones hold about 4,800
groups and 3.7 MB forced; they run on the generic map (a NULL key leaves the
integer path), where `canUseExternalMerge` is false, so the clone drain bound
(`PartialDrainBytes`, 16,384 at 24 workers) never fires. Without the pool
chunk the clones would still hold about 1.4 MB, so a smaller first chunk would
not decide this verdict. At the base and at this change alike (its
aggregates' charges are byte-identical in the trace) it refused every free
run (30 at each GOMAXPROCS) and every run with the releases held, and with
the join's arrival delayed 100 ms answered PostgreSQL's 3 in 12 of 30 base
and 22 of 30 tip runs, 13 and 16 of 30 when the two were interleaved in one
binary. With one pipeline worker (`WADJET_SCAN_WORKERS=1`) it answers
PostgreSQL's 3: in 30 of 30 runs at the base and 30 of 30 at this change, and
in the run traced, where the serial aggregate holds 336,340 bytes forced, the
join build spills partitions until it can reserve and the statement answers
(which fails the pinned refusal). With two or four workers it refuses, 15 of
15 runs each at the base and at this change. Its verdict therefore follows
the worker count and the release order. The measurement suggests that not
partitioning an aggregate that cannot drain under a budget would give this
statement a deterministic answer, not a refusal; that is measured at one
statement and decides nothing. Making the verdict independent of the worker
count and the release order — bounding a generic-path clone (a drain for the
generic map), not partitioning an aggregate that cannot drain under a budget,
or ordering a join's first reservation after an upstream breaker's release —
is the decision the 2026-11-22 amendment left open; it is still not taken.
The pin stays a refusal; it is the order class, not an accounting drift. Nor
do the same test's cross-join refusal pins hold in every free run: in 360
fresh processes per binary (GOMAXPROCS 1, 2, 4 and the default) one answered
at the base (`480 the same rows DESCENDING`, GOMAXPROCS 1) and one at this
change (`480 the same join with its rows ORDERED`, GOMAXPROCS 2), each with
PostgreSQL's 12 rows, which fails the pinned refusal — about 1 in 360 free
runs, at the base and this change alike, too rare for a 30-run row to see.

### 2026-11-23 (second): a partitioned aggregate's presize cap is its share of the estimate (#1706, line 1706f)

**What was true.** The 2026-11-22 rule capped a budgeted aggregate's initial
capacity at its input estimate, and "a clone inherits its parent's estimate as
a cap only": every one of the k sinks of a partitioned aggregate — the primary
(`workerSinks[0]`) and its k-1 clones, each owning a hash partition of the key
space — was capped at the WHOLE estimate. A LATERAL body over the 5,000-row
`typemx` therefore kept the 4096-slot presize in every partition: each clone
that received one of the seven groups force-charged about 107 KB of `spill
tracking` (the 98,304-byte pool chunk and the index; about 238 KB where the
string-keyed index keeps its 4096 slots too) for ONE group, five to seven of
them were outstanding when the hash join above made its first reservation, and
the verdict was how many had been released by then (line 1706e).

**The rule now.** Under a budget, a partitioned aggregate's cap is each sink's
share of the estimate, ceil(estimate / k), computed once in
`HashAggregate.presizeBound` from `cloneNDVDivisor` (which the pipeline sets on
the primary before cloning and `CloneSink` copies only to partitioned clones),
so the primary is covered and nothing is divided twice. A positive estimate
never rounds to the zero that means "unknown"; an unknown estimate stays
unknown; a non-partitioned aggregate keeps the whole estimate (its clones may
each see every key). Only the sizing cap is divided: `InputRowHint`, the exact
`inputRowBound` and the epoch byte cap keep their meaning, and the share is not
a bound on a partition's groups. Under skew one partition receives every group
and grows, and is charged for its growth, as an aggregate whose estimate was
low does (gate: every presizing path — typed integer, packed, compact, string,
typed-generic and the generic map over the group-state pool — as primary and
as clone, all 6,000 groups into one partition, answers the whole-estimate
presize's rows with its tracked charge equal to its measured state after every
batch). Without a budget, allocation is byte for byte unchanged (gated on every
path). An engine given no budget still has one: the planner's spill manager
falls back to `memory.DetectBudget`, else 75% of physical memory, so the share
(like the 2026-11-22 cap) acts in the default configuration too. TPC-H SF0.01
wall, base against this change interleaved, six runs each: 537 / 515 ms
(medians) at that default, 1,783 / 1,732 ms at 2 MiB and 759 / 715 ms at
8 MiB, every query's row count unchanged; a skewed budgeted partition (100,000
groups into one of 24, estimate 200,000) showed no time change over the noise
and, on its string-keyed shape, 3% (8 MiB) and 16% (64 MiB) fewer bytes
allocated. Gates: `exec.TestAPartitionedAggregatesPresizeCapIsItsShareOfTheEstimate`,
`…ShareBindsOnEveryPresizingPathAndSkewStillAnswers`,
`exec.TestAnUnbudgetedPartitionedAggregateAllocatesWhatItDid`,
`exec.TestARealPartitionedPipelineSizesEveryPartitionToItsShare`.

**Measured.** Base against this change, 30 fresh processes per cell at
GOMAXPROCS 1, 2, 4 and the default, in three orders: free; the join's first
arrival reservation delayed 100 ms (the releases first); and every adopted
clone's release held 100 ms (the join first, with every clone's charge
outstanding). The 21 budgeted cells of `coordinator.TestArcH1A…`,
`TestArcJ1A…` and `TestArcJ1TheReservedNamespace…`, CW2's `lateral-aggregate`
and CW3's `lateral-aggregate` and `left-join-aggregate` over rows refused in
2,853 of 2,880 (cell, run) pairs free, 451 of 2,880 delayed and 2,880 of
2,880 held at the base, and in none of the 8,640 at this change; every answer is
PostgreSQL's (re-measured on 17.11, `--locale=C`). Those cells' tolerance of
the refusal is removed and their spilled arms assert the rows.

**What it does not cover.** This narrows the window in which a join's first
reservation races partitions' releases; it does not remove the class. A
partitioned aggregate whose REAL state per clone is large still holds it,
force-charged, until each clone's Close: CW3's `left-join-lateral` over rows
(5,000 groups over 24 clones) refused every free and every held run at the
base and at this change alike and answered every delayed run; at the refusal
the clones held about 2.0 MB forced where the whole-estimate presize held 5.1
to 7.5 MB. It stays tolerated (`cw3CloneStateRefusal`). Larger estimates,
under-estimates, skew and several concurrent aggregates can again leave enough
forced charges outstanding to make a join's first reservation depend on
release order. `coordinator.TestF1AKeylessJoinAsksForWhatItNeeds` reaches
this code (its DISTINCT aggregates' 24 clones now take a 209-slot first chunk,
not the 4096 the 2026-11-23 amendment above describes) and its pins did not
change verdict: over 120 fresh processes per GOMAXPROCS (1, 2, 4, default)
per binary, base and this change interleaved, the five cross-join pins and
the 480 control refused in every run but three, all at GOMAXPROCS 1 and each
with PostgreSQL's rows — at the base the 480 control once (#1706's original
sighting, reproduced), at this change `480 the same shape spelled DISTINCT`
once and `480 the same rows DESCENDING` once: the same rare order-dependent
answer, at the base and this change alike. The R0 `h2_base2_control` flap pins do not reach this
code (no partitioned aggregate presizes there) and keep their rates. Making
these verdicts independent of the order — what a join's first reservation may
wait for, or what a clone that cannot drain may hold — is still the decision
the 2026-11-22 amendment left open; this change does not take it.

### 2026-11-23 (correction to 2026-11-21): a scan's consumer never waits in the load gate for bytes only it can release (#1706)

**What the 2026-11-21 amendment got wrong.** It says a decode the consumer is
waiting for "never waits on the budget", and that a scan on demand skips the
load admission's bounded wait. Both were true of the tracker reservation
(`ReserveOrForce`) and false of the scan's LOAD GATE, a byte bound on
concurrent file loads sized at a quarter of the budget (`loadGate`,
`buildRGUnits`), in front of which the consumer's own load still waited, with
no bound. A slot's gate bytes come back only when its LAST row group has been
decoded (`releaseRG`), and once a scan is on demand its remaining row groups
are placeholders only the consumer decodes. So the consumer, taking a
placeholder of a file not yet loaded, could wait for bytes held by a file
whose last row group was a placeholder behind it in the batch channel: a wait
on itself, while the workers waited on the full channel. TPC-H Q09, Q16 and
Q18 at SF1 under a 16 MiB budget never returned in 5 of 10 fresh runs (0 of 5
without a budget). Observed in an instrumented run (Q16, `partsupp`): the
consumer waited for 5,510,808 bytes against a 4,194,304-byte gate held
entirely by one slot, loaded by a worker, whose one remaining row group was a
placeholder not yet decoded. A second form held the same cycle through a
mutex: a read-ahead admitted before the scan went on demand waited in the
gate while holding its slot, and the consumer, decoding that slot's next
placeholder, waited for the slot.

**The rule now.** No goroutine waits in the gate for room whose release
needs the scan's consumer to make progress. The consumer — the goroutine
calling `Next`, or a parallel pipeline's workers, which share the source —
loads as before while its bytes would fit once the holders the workers alone
will release have released (a holder each of whose owed row groups is an
admitted read-ahead being decoded), and is admitted at once, past the gate's
bound, when the room it needs is held for row groups only the consumer
decodes. A read-ahead waiting in the gate holds its slot while it waits; it
goes on waiting as before unless the consumer comes for that slot while the
room it waits for is held for row groups only the consumer decodes, and then
it stops, admitting nothing, and hands its row group to the consumer.

Under a parallel pipeline the base's consumer waits often ended through a
sibling consumer: in one instrumented Q09 at SF1 under 16 MiB the base's
lineitem consumers waited in the gate 24 times, many of the waits
overlapping, and each ended within 0.3–40 ms.
The gate cannot tell whether a sibling will make that progress or is itself
parked behind the waiter (in the gate, or downstream in a partitioned
aggregate's queues), so room owed to the consumer counts as room that does
not come back while a consumer waits. That is a change from the base in runs
the base answered: there the consumers' loads past the gate overlap where
they used to follow one another. They are charged as before — the gate is not
the ledger; every load and every decoded batch is charged to the tracker
exactly as it was, so what the scan holds is what it charges, and only the
gate's own count passes its bound. Without a budget a scan is never on demand
and nothing changes.

A first form that admitted every consumer load at once, and stopped every
waiting read-ahead as soon as its scan went on demand, also ended the hang
but moved verdicts the base answered: over that form and two intermediate
ones Q09 alone at 16 MiB refused `memory budget exceeded` at a hash join
build in 5 of 345 executions, one of them with no consumer load past the gate
at all (the eager hand-over alone). The rule above refused none of 90, and
TPC-H Q09, Q16 and Q18 at 16 MiB returned with the expected answers in 30 of
30 fresh processes (24, 4 and 1 scan workers). The 90 against 345 does not
separate the two rates by itself; what the rule removes is the hand-over in
runs where no wait could be on the consumer.

Gates: `physical.TestAConsumerNeverWaitsInTheLoadGateForBytesOnlyItCanRelease`
and `physical.TestAConsumerNeverWaitsOnASlotWhoseLoaderWaitsInTheLoadGate`
(the two cycles, built from their ingredients on the real decode path, on the
row-group and the whole-file load; both hang at the base),
`physical.TestLoadGate_AConsumerWaitsOnlyForBytesTheWorkersWillRelease`,
`physical.TestLoadGate_AConsumerWaitsWhenTheWorkersAloneFreeEnough`,
`physical.TestLoadGate_AReadAheadYieldsOnlyWhenTheConsumerNeedsItsSlotAndItsRoom`.
