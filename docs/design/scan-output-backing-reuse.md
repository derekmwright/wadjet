# Scan row-group output backing reuse

Status: implemented 2026-08-22, `WADJET_SCAN_BACKING_REUSE=0` kill switch
(`optswitch` `scan-backing-reuse`).

This is the **scan half** of the vector-backing-reuse lever. The join half
shipped as `69aecbb` / ADR-0016 ("`Detach` is the ownership claim") and
*deliberately excluded* the scan side with this note:

> **Reuse the scan row-group output** — 473 GB/run of `NewRecordBatch` plus
> 166 GB/run of `PreAllocBytes` under `readColumnNative`, a *larger* number
> than the join path. Deliberately excluded: those batches are held
> concurrently in the decode-ahead ring and offered to the decoded-chunk
> cache, so their lifetime is neither single-consumer nor bounded by one
> call, and the `Detach` claim does not describe them. **It needs its own
> ownership statement before any reuse.**

This memo is that ownership statement, and the mechanism built on it.

---

## 1. The evidence

`docs/benchmarks/sf100-window3-analysis-2026-08-22.md` §6, heap-lock callers
on arm B (the shipped `69aecbb` binary), CPU-s of mutex delay over 3 workers ×
4 runs:

| caller | B cand | per worker-run | what it allocates |
|---|---|---|---|
| **`scan.readRowGroupNative.func2`** | **130.8 s** | **10.9 s** | per-column decode output |
| ↳ `scan.readColumnNative` | 100.9 | 8.4 | the BYTES arena (`PreAllocBytes`) |
| ↳ `batch.NewVectorWithScale` | 61.5 | 5.1 | typed slices + null bitmaps |
| ↳ `batch.newVectorFromColumn` | 44.0 | 3.7 | |
| `parquet.ColumnPageReader.*` | 94.6 | 7.9 | page buffers (**not** this lever) |
| `batch.NewRecordBatch` | 43.8 | 3.7 | batch shells |

§4 gives the allocation behind it, per suite run (3 workers × 4 runs, arm B):
`readRowGroupNative` (cum) **546.7 GB/run**, `NewVectorWithScale`
**551.9 GB/run**, `PreAllocBytes` **195.1 GB/run**. §8.3 names it as the next
lever:

> Third, and unrelated to variance: **pool the scan row-group output backing**
> … It is now the #1 named heap-lock caller at 130.8 s (10.9 s per worker per
> suite run) behind ~1 140 GB/run of allocation.

The structural reason none of it is reused is the same one the join half had:
`BatchPool` recycles only what a driver hands back with `Release()`, and the
worker's `fragmentDriver` never opts into `ReleaseInputs`. Both row-group
iterators (`RowGroupIter`, `DecodeAheadIter`) pass `nil` for the pool argument
`readRowGroupNative` already accepts, so **every row group on the distributed
path is a fresh `NewRecordBatch` plus a fresh `PreAllocBytes` arena per BYTES
column.** At SF100 lineitem widths one row group is ~280 MB, so every one of
those allocations is a large object that takes the Go heap lock directly.

---

## 2. Why `Detach` alone is not enough here (the reason this was deferred)

ADR-0016's rule is *"a producer may overwrite the backing of a batch it emitted
for as long as no consumer has claimed it"*. On the join emit path that is
complete, because `HashJoinProbe` emits **one batch at a time to one
consumer**: by the time the probe is asked for the next batch, the previous one
is either claimed or provably dead.

A scan source is not that shape. Three facts break the single-consumer,
one-call-lifetime assumption:

1. **The decode-ahead ring decodes ahead.** `DecodeAheadIter` runs `w` decode
   workers concurrently and parks up to `WindowBytes` of decoded-but-undelivered
   groups (ADR-0015). Group `N+1` is already decoded while group `N` is being
   consumed, so "the producer's next call" is not a point in time after the
   consumer finished — it is *concurrent with* it.
2. **The morsel dispenser fans one row group out to `k` consumers.** A
   ~280 MB parent is split into ~4·k zero-copy views over the *same* `*Vector`
   pointers (`morsel_dispenser.go`, `docs/design/morsel-execution.md` §4.1.1).
   Nobody claims those views — they are read and dropped — yet they are live
   readers of the backing on `k` goroutines.
3. **Late materialization keeps transitive references.** `emitViewOutput`
   mints view columns whose `Vector.Base` *is* the scan column, and pins the
   input with `DetachPool` (no claim) precisely because the reference dies with
   the probe's own output batch. So on the dominant SF100 join shape the scan
   batch's columns are read long after `HashJoinProbe.Execute` returned, with
   no claim recorded anywhere.

A claim check alone would therefore hand a live backing to a decode worker.
What is missing is a **release** signal: a statement from the consumer side
that everything reading this row group is done.

That signal already exists and is already load-bearing: **`morsel.retire`**.
Its contract (`morsel_dispenser.go`, and the call site in
`runFragmentLinearParallel`) is:

> Consumers MUST call retire exactly once, after they are done reading the
> batch (post op-chain + sink consume) […] The morsel is retired only after
> the whole chain — including every resumed slice of a suspended fan-out — is
> done with it: a suspended probe still reads its probe-side columns.

and the dispenser already reference-counts the sibling views so the parent's
bytes are returned exactly once, when the *last* sibling retires. That is the
release edge this lever needs, and it is strictly later than the
`ReleaseInputs` edge (`ChainDriver`'s "the operator is done reading it") that
`DetachPool` exists to defend against.

---

## 3. Ownership statement

> **The scan source owns the decoded backing of a row group. It may hand that
> backing to a later decode only when BOTH of two independent facts hold:**
>
> 1. **RELEASED** — the consumer side has said it is finished: the dispenser's
>    `retire` fired for the parent (i.e. for every zero-copy view minted over
>    it), or, on the serial fragment path, the driver's `push` returned, which
>    is after the whole operator chain *and* the sink consume.
> 2. **UNCLAIMED** — nobody kept it: neither `RecordBatch.Detach` on the batch
>    the scan emitted nor `Vector.Claim` on any of its column vectors —
>    including a claim that arrived transitively, through a derived batch
>    (`ColumnPrune`, set-op emit, `selView`) or through `Vector.Base` from a
>    late-materialization view minted downstream over one of our columns.
>
> **The release is the liveness signal; the claim is the retention veto.
> Neither is inferred — `retire` is the consumer's explicit statement,
> `Detach` is the retainer's.** Release without the claim check is unsound
> (Sort, Window, the hash-join build and the spillable collector all retain
> past `retire`, and all of them `Detach`). The claim check without release is
> unsound (`k` morsel consumers and the probe's view columns read one parent
> concurrently with the ring's next decode, and none of them claim).
>
> **The backing is surrendered whole or not at all.** One claimed column
> surrenders the entire batch — every column of it is reachable from the batch
> a consumer kept — and the surrender is permanent for that backing: the next
> decode mints fresh. Claims are sticky (ADR-0016) and this rule inherits that.
>
> **A backing is only ever offered back to the pool that minted it.** The pool
> tracks the batches it handed out; a batch it did not mint (a WSHF shuffle
> chunk, a row-based fallback batch, a batch already recycled) is ignored, so
> no second owner can be created for storage someone else recycles.

### 3.1 What this does *not* change

`DetachPool` keeps its exact meaning and its exact call site. It severs the
`BatchPool` link so a `ReleaseInputs` driver cannot recycle an input while the
probe's views still point into it. This lever does not route through
`RecordBatch.pool` at all — deliberately, because `emitViewOutput` calls
`DetachPool` on *every* late-materialized probe input, which is the dominant
SF100 scan shape; hanging reuse off `b.pool` would disable it exactly where the
130.8 s lives. Instead the scan pool holds its own registry of outstanding
backings, and the retire edge — which is later than the `ReleaseInputs` edge —
is what makes that safe.

### 3.2 Preconditions this rests on, and where they are audited

| precondition | where it is established |
|---|---|
| retaining consumers `Detach` **inside** the call that handed the batch over | ADR-0016; `sort.go:105`, `window.go:289`, `join.go:1014/1783`, `batch_collector.go:50`, `sort_merge_join.go:284`, `partitioned_agg.go:134` |
| `Detach` records the claim on every **column vector**, recursing through `Base`, `Child`, `Children` | `batch.Detach` → `Vector.Claim` (`69aecbb`) |
| linear-path operators never write input column storage, never append to or replace `in.Columns` | `docs/design/morsel-execution.md` §4.1.1 (audited list) |
| `retire` fires after the whole chain **and** the sink, once per parent | `morsel_dispenser.go` `dispatch`; `executor_fragment.go` consumer bodies |
| a reset vector is bit-identical to a fresh one | `Vector.ResetForWrite` / `resizeCleared` / `Bitmap.ResetNonNull` (all clear what they resize) |
| the decoded-chunk cache owns its own copy | `decoded_cache.go` `cloneChunkVector` deep-copies on `Offer`; `fillFromCache` copies *into* our vector |

---

## 4. Mechanism

### 4.1 `scan.BackingPool`

A per-**source** (not per-file, not per-iterator) free list of whole decoded
row-group batches, in `internal/engine/scan/backing_pool.go`.

```
get(schema, numRows) → *batch.RecordBatch | nil     // called by the decode worker
Recycle(b)                                          // called on the release edge
```

* `get` pops an idle backing whose **shape** matches the read schema (column
  count, per-column `Type`, DECIMAL `Scale`, VECTOR `Dimension`), calls
  `Vector.ResetForWrite(numRows)` on every column and `Sel = nil`, `Len =
  numRows` on the shell, and records it as *outstanding*. A miss returns nil
  and the caller mints `batch.NewRecordBatch` as before.
* `Recycle` looks the batch up in the outstanding set (foreign batches are
  ignored — §3), removes it, and admits it to the idle list only if
  `!b.Retained()` and no column reports `Claimed()`, and the caps below allow
  it.

**Reset, not re-allocate, is what wins.** `ResetForWrite` retains every backing
array's capacity: the typed slice is re-sliced and cleared (the same memclr
`make` was already paying), the null bitmap is re-sliced and set to all-valid,
and — the big one — `BytesColumn.ResetForWrite` empties the arena with
`Data = Data[:0]` while keeping `cap`, so `readColumnNative`'s
`PreAllocBytes(est)` becomes a no-op once the arena has reached its high-water
mark instead of a fresh multi-hundred-KB-to-MB span per column per row group.

**Caps.** The idle set is bounded by count (`MaxIdle`, default 4 — one per
default decode worker) and by bytes (`MaxIdleBytes`, default
`morselDispenserBudgetBytes` = 512 MiB — the byte budget one fragment is
already allowed to hold in decoded source bytes in flight). One backing is
always keepable regardless of size, mirroring the ring's "the delivery-cursor
group is always admitted" rule: without that escape a single row group larger
than the cap would disable the mechanism on exactly the table that motivates
it (SF100 `lineitem`, ~280 MB/group).

### 4.2 Where it is wired

```
cachedFileStreamSource            owns *scan.BackingPool for its whole life
  ├─ DecodeAheadOpts.Backing  ───► DecodeAheadIter.decodeLoop
  │                                  └─ ReadRowGroupNativeBacked(...)
  └─ RowGroupIter.SetBackingPool ─► RowGroupIter.Next
                                     └─ ReadRowGroupNativeBacked(...)

morselDispenser.dispatch retire ─► src.RecycleBatch(parent)   [parallel path]
runFragmentLinear after push    ─► src.RecycleBatch(b)        [serial path]
```

The pool lives on the **source**, not the iterator, so it survives the
cross-file transition (a batch from file *F* may still be in flight when the
source has moved on to *F+1*; both files of a table share one read schema, so
the backing is reusable across the boundary and returning it after its
iterator closed is harmless).

`morselDispenser` finds the hook by asserting an optional interface on the
fragment source, unwrapping the two fragment-path wrappers (`timedSource`,
`bloomFilteredSource`). A source that does not implement it — a shuffle
stage input, a memory source, the row-based fallback — gets today's behavior
exactly.

### 4.3 Kill switch and counters

`WADJET_SCAN_BACKING_REUSE=0` (`optswitch` `scan-backing-reuse`) makes `get`
always miss and `Recycle` always drop: every row group is a fresh
`NewRecordBatch` again, byte-for-byte the pre-2026-08-22 path. The switch is
the invariance-oracle arm.

Counters ride the periodic 30 s `worker stats` line — never only a drain-time
line (ADR-0011 §6, the lesson from the decode-admission counters):
`backing_hits`, `backing_misses`, `backing_claimed` (release rejected by a
claim), `backing_idle_mb`.

---

## 5. Scope: what is covered and what is not

**Covered.** Every flat leaf column type the native reader materializes:
BOOL, INT32/PORT/PROTOCOL/DATE, INT64/TIMESTAMP/IPv4/MAC/DURATION, FLOAT32/64,
STRING/BYTES/IPv6/CIDR/UUID (the variable-width `BytesColumn` arena — the
`PreAllocBytes` 195.1 GB/run), DECIMAL, and fixed-dimension VECTOR.

**Explicitly out of scope, with reasons:**

* **ROW columns (and their children).** `Vector.ResetForWrite` refuses nested
  columns by design; a ROW parent's children are per-row parallel vectors that
  would need a recursive reset with its own correctness argument, and ROW
  schemas are not what the profile names. A batch with any nested column is
  never admitted to the pool — the shape check rejects it and the decode mints
  fresh, exactly as today. (ARRAY/MAP never reach here at all:
  `HasUnsupportedColumnarTypes` routes them to the row-based fallback.)
* **`parquet.ColumnPageReader` page buffers** (94.6 s of heap-lock delay,
  `DecodeBitPacked` 118.7 GB/run, `getZstdBuffer` 109.4 GB/run). A separate
  seam inside the **safety-critical** parquet package, with its own pooling
  (`page.Release`, `colReadScratchPool`) and its own round-trip test burden.
  Pooling at the scan/batch boundary leaves it untouched; it stays the next
  named candidate after this one.
* **Eager/manifest stage inputs** (`manifestStreamSource`). It creates a new
  inner `cachedFileStreamSource` per resolved file set, so a release arriving
  after the swap would reach the wrong pool — where the mint registry would
  ignore it anyway. Its inputs are shuffle chunks rather than parquet row
  groups, so there is nothing to win; forwarding the hook through it would need
  a lock around `inner` for no measured gain.
* **The decoded-chunk cache's clones.** `cloneChunkVector` allocates the
  cache's *own* copy on `Offer`; the cache must own it, and that independence
  is what makes reusing our backing safe.
* **The single-process `exec.Pipeline` path**, and with it the sel-pruned
  decode (`readColumnNativeSel`) and the lengths-only decode
  (`readColumnNativeLengths`). Both are reached only through
  `ReadRowGroupNativeShaped`, which is `physical/util.go`'s entry point; that
  path already has `BatchPool` and a `ReleaseInputs` driver and is not what
  the SF100 profile names. Nothing about them is incompatible — the lengths
  decode already documents that "a pooled vector arrives with its arena reset
  but non-nil" — so extending the pool there later needs no new ownership
  argument, only the same release edge.

---

## 6. Measured (micro) and expected (SF100)

### 6.1 Micro-arm, measured

`BenchmarkReadRowGroupBacking` decodes every row group of a 4-column
(int64 / string / float64 / string) multi-row-group parquet file and releases
each batch immediately — the steady state of a fragment whose consumer keeps
nothing. `pool=false` is the `WADJET_SCAN_BACKING_REUSE=0` shape. benchstat,
n=6, same binary:

| | B/op | allocs/op | sec/op |
|---|---|---|---|
| rows=100 000 (4 groups) | 11.535 Mi → 6.634 Mi **−42.5 %** | 317.5 → 247.0 **−22.2 %** | 2.658 m → 2.123 m **−20.1 %** |
| rows=400 000 (16 groups) | 46.13 Mi → 26.53 Mi **−42.5 %** | 1 247 → 977 **−21.7 %** | 10.067 m → 7.686 m **−23.7 %** |
| geomean | **−42.49 %** (p=0.002) | **−21.93 %** (p=0.002) | **−21.92 %** (p=0.002) |

The remaining 57 % of B/op is the out-of-scope half: `ColumnPageReader` page
buffers, `DecodeBitPacked` and the decompressor's scratch, all inside
`internal/storage/parquet`.

Wall moved too, which the join half's arm did not: a reused BYTES arena turns
`PreAllocBytes` from a `make` + `copy` of the whole arena into a no-op, so this
lever removes work as well as allocations.

**Control:** every other benchmark in `internal/engine/scan` and
`internal/engine/batch` (`ReadColumnar`, `ReadColumnarNullable`,
`ReadProjection`, `DecodedChunkRead`, `LengthsDecode`, `NewRecordBatch`,
the vector kernels), switch-on vs switch-off, n=3: **B/op and allocs/op
identical to the byte in every arm.** The change is confined to the path that
opts in.

### 6.2 Expected shape at SF100

Predicted from the window-3 table (§1), 3 workers × 4 runs, against arm B:

| signal | B (measured) | predicted with this lever |
|---|---|---|
| `readRowGroupNative.func2` heap-lock | 130.8 s (10.9 s/worker-run) | **40–70 s** (3.3–5.8 s/worker-run) |
| ↳ `readColumnNative` (the arena) | 100.9 s | **25–45 s** |
| ↳ `NewVectorWithScale` / `newVectorFromColumn` | 61.5 / 44.0 s | **15–25 / 5–15 s** (residual = decoded-cache clones + non-reused shapes) |
| `batch.NewRecordBatch` | 43.8 s | **< 10 s** |
| `parquet.ColumnPageReader.*` | 94.6 s | **unchanged** (out of scope) |
| alloc_space total | 7 518.8 GB (12 runs) | **−350 to −550 GB per suite run** |
| ↳ `readRowGroupNative` cum | 546.7 GB/run | **−60 to −80 %** |
| ↳ `PreAllocBytes` | 195.1 GB/run | **−70 to −90 %** |
| worker mutex delay | 65.9 s/worker-run | **−5 to −9 s/worker-run** |
| GC cycles (`gc_delta`) | 1 466 | **−5 to −12 %** |
| worker CPU | — | **flat** (the memclr is the same; the allocation is gone) |
| wall | — | **neutral to slightly better** (the micro arm moved −22 %, but SF100 decode is bandwidth- and page-fault-bound as often as it is allocation-bound) |

The join half moved −260 GB/run of allocation and −13.1 s/worker-run of
heap-lock delay for **zero** wall (ADR-0016). This half is a comparable number
on the same lock and should be judged the same way (ADR-0011): allocation,
lock delay and GC, not the stopwatch. The residual after this lever is the
parquet page buffers.

**Unverified until an SF100 window runs it.** The micro arm (§6.1) is the only
measurement in hand; every number in the table above is a prediction from the
window-3 profile, and the reuse RATE on real fragment shapes — how often a
consumer claims — is the first thing the `backing_hits` / `backing_claimed`
counters should be read for.

Reuse **rate** is the thing to read first in the counters. Fragments whose sink
retains — a hash-join build, a Sort breaker fed straight from a scan — will show
`backing_claimed` ≈ `backing_misses` and near-zero hits, and that is the
mechanism working: those batches genuinely outlive the call.

---

## 7. What could break

* **A retaining consumer that does not `Detach`.** It reads storage a decode
  worker has legitimately overwritten — silent wrong answers, the same failure
  mode ADR-0016 introduced and the same mitigation: the invariant is stated,
  the consumer matrix is enumerated, and the adversarial test
  (`TestBackingReuseDoesNotAliasLiveBatch`) drives a held batch against a
  following decode under `-race`. Extend it when a consumer kind is added.
* **A future consumer that retires early.** `retire` is a *promise*. Anything
  that calls it before the sink is done with the batch turns this lever into
  corruption. The contract is already load-bearing for the dispenser's byte
  budget, but it is now load-bearing for correctness too, and the comment at
  the retire sites says so.
* **Resident-memory increase from the idle set.** Bounded by `MaxIdleBytes`
  (512 MiB) per source, or one backing when a single group exceeds it. It
  substitutes for garbage the GC was already carrying at a far higher rate, and
  `gc_delta` is the signal that says which way the trade went. It is *not*
  charged to the task ledger; if a future envelope needs it to be, the seam is
  `BackingPool.IdleBytes()`.
* **Cross-file shape drift.** Two files of the "same" table with different
  DECIMAL scales or VECTOR dimensions. The shape check compares type, scale and
  dimension per column and rejects a mismatch, so the worst case is a missed
  reuse, not a mis-typed read.
* **Schema evolution / missing columns.** A column absent from the file is
  left all-null by `readRowGroupNative`. On a fresh batch that is
  `NewBitmap`-valid then `SetNull` per row; on a reset batch it is
  `ResetNonNull` then the same `SetNull` loop — identical. The typed slots
  underneath are cleared either way.

---

## 8. Related

* ADR-0016 (`Detach` is the ownership claim) — amended 2026-08-22 with the
  release-plus-claim rule for scan output.
* ADR-0015 (decode-ahead is an admission class) — the ring whose concurrency is
  the reason a claim alone is not enough.
* ADR-0006 (never-OOM) — the idle-set cap.
* ADR-0011 (measurement) — why this ships on allocation and lock delay.
* `docs/design/morsel-execution.md` §4.1.1 — the view-safety audit and the
  `retire` contract.
* `docs/design/scan-decode-pipelining.md` — the decode-ahead ring.
* `docs/design/decoded-rowgroup-cache.md` — the cache that owns its own copies.
* `docs/design/late-materialization.md` §4/§4.1 — the join half's ownership and
  emit-storage reuse.
* `docs/benchmarks/sf100-window3-analysis-2026-08-22.md` §4, §6, §8.3.
