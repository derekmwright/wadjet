# Decoded row-group cache (cross-query decoded-column cache tier)

> **Status:** design + implementation (2026-08-12). Default **OFF**
> (`--decoded-cache-bytes=0`); SF100 same-window pair is the default-flip
> gate (deploy approval required).
> **Verified against:** main @ 58365bb. Anchors below were confirmed
> against that commit; they drift.
> **Origin:** `docs/benchmarks/lever-ranking-2026-08-12.md` lever #4 —
> "zstd bill … decoded-rowgroup cache for cache-hot re-reads (R2
> re-decompresses the same bytes today) … Interacts with the memory
> ledger — design first."

## 1. Goal and evidence

zstd parquet decompression is the largest single CPU subsystem on SF100
workers: **24.2% cum (1,983 CPU-s over 3 workers/suite)**, with parquet
decode kernels another ~7% (2026-08-12 re-profile, bin ebd006f,
`~/wadjet-artifacts/20260812-reprofile/`). The base-table NVMe cache
(91% hit-bytes) eliminated the S3 fetch; every read still pays full
decompress + decode. Nothing in the tree retains anything past
decompression:

- Every existing cache tier — heap LRU (`internal/worker/cache.go`),
  base-table NVMe cache (`internal/storage/objstore/base_table_cache.go`),
  peer tier, prefetcher, pread staging — traffics in **compressed file
  bytes**.
- `DecodeAheadIter` is a pipeline, not a cache: window slots are deleted
  on delivery (`internal/engine/scan/decode_ahead.go:466-469`).
- Dictionary pages are re-decompressed and re-decoded on **every**
  column-chunk read (`internal/storage/parquet/page_reader.go:210-241`),
  and their decompressed buffer is never pooled
  (`DictionaryData` has no `rawBuf`/`Release`).

The reuse is real and two-dimensional: **cross-query within a run**
(lineitem appears in 17/22 TPC-H queries; hot columns like
`l_shipdate`/`l_quantity`/`l_extendedprice`/`l_discount` recur across
overlapping projections) and **cross-run** (benchmark_runs=2; R2
re-reads exactly R1's bytes). Scan affinity
(`internal/coordinator/scan_affinity.go`) makes file→worker placement
deterministic across queries, which is what makes a worker-local cache
hit rate real.

## 2. What the code does today (verified)

Read path (worker/SF100): `cachedFileStreamSource.Next` →
`buildParquetFromLocal` (`internal/worker/stream_source.go:1241`) →
`scan.OpenDecodeAheadIter` → `ReadRowGroupNative`
(`internal/engine/scan/columnar_native.go:83`) → per-column errgroup →
`readColumnNative` (`columnar_native.go:197`) → `ColumnPages` →
`NextDictionary`/`NextPage` → `parquet.Decompress`
(`internal/storage/parquet/decompress.go:17`, three call sites in
`page_reader.go`: dictionary :232, data-V1 :250, data-V2 :361) →
`copyNativeData*`/`resolveNativeDictionary` into `*batch.Vector`.

Load-bearing properties:

1. **Decoded vectors alias nothing.** Every copy path materializes into
   vector-owned storage (`copy()` for fixed width, `BytesData.BulkSet`
   arena appends). That is why `PageData.Release()` and
   `ColumnPageReader.Close()` are safe today — and it means a decoded
   row group is already a self-contained, indefinitely-retainable
   object. No mmap, chunk-buffer, or zstd-pool pinning to solve.
2. **Column chunks are read whole or not at all.** There is no
   page-index pruning; predicate/dynamic-filter pushdown drops entire
   row groups (`decode_ahead.go` `pruneGroup`), sharding partitions row
   groups disjointly. Cache keys are all-or-nothing; pushdown only
   changes *which* keys are requested, never their shape.
3. **Projections vary per consumer.** The same row group is legitimately
   read narrow by one task and full-width by another
   (`stream_source.go:1032-1071` all-or-nothing projection guard;
   late-materialization gathers use different column sets). A
   whole-row-group cache keyed by column set would thrash; a per-column
   cache accumulates union coverage.
4. **Decode output depends on the catalog type**, not just the file
   type (`coerce`, `columnar_native.go:229`; DECIMAL scale) — the type
   belongs in the cache key.
5. **Identity does not reach the decode layer.** `parquet.FileReader`
   carries no path/key. Identity is available one level up at the open
   sites (`stream_source.go:978/1003`: bucket, object key, size). ETag
   is unnecessary: `(bucket, key)` is content-stable by construction —
   ingest writes UUID-named chunks, compaction swaps keys via the
   manifest (same argument as `base_table_cache.go:89-94` and
   `catalog/rgmeta.go:29-32`). An entry can never be wrong, only
   superfluous.
6. **Downstream mutates batches in place** (`exec.Filter.Execute` sets
   `in.Sel`, `internal/engine/exec/filter.go:70`; delete markers
   likewise). A cached object cannot be handed to consumers by pointer
   without a mutation audit of every scan-path operator.
7. **The worker decode path allocates unpooled batches**
   (`pool = nil` at `decode_ahead.go:572` / `rowgroup_iter.go:163`;
   `ownerID = 0`, `Release()` no-op). Only the single-node `Scanner`
   passes a `BatchPool` — cached storage must never retain
   pool-owned vectors.

## 3. Design

### 3.1 Unit and key

Cache **decoded column chunks**: one entry per

```
key   = (bucket, objectKey, fileSize, rowGroupIdx, leafColIdx, catalogTypeID, decimalScale)
value = cache-owned *batch.Vector clone (Len == rg.NumRows) + byte size
```

`fileSize` is a free paranoia check available at both open sites.
Leaf column *index* (not catalog name) is the stable coordinate —
alias resolution maps names to leaf indices before decode.

Entry sizes at SF100 are LRU-friendly: ~950K rows/row group → ~7.6 MB
for an int64/date column, ~30 MB for a bytes column (`l_comment`).

### 3.2 Hook point

`ReadRowGroupNative`'s per-column fan-out (`columnar_native.go:130-190`):

- **Hit:** skip `readColumnNative`; copy the cached vector into the
  batch's vector slot (one `BulkCopy`/`copy` pass). Touch the entry.
- **Miss:** run `readColumnNative` as today; then (admission permitting)
  clone the freshly decoded vector into cache-owned storage.

Identity plumbing: `FileReader` gains an opaque `cacheIdentity` string
set by new keyed open variants; `stream_source.go` open sites pass
`bucket + "/" + key + "#" + size`. Empty identity ⇒ caching disabled
(tests, single-node Scanner, in-memory fallback) with zero behavior
change. This is a passive field on the parquet package — no
encode/decode path changes (parquet-safety gate still applies:
SF0.01 22/22 before merge).

### 3.3 Copy discipline (v1: correctness-provable, no mutation audit)

Consumer-visible semantics are **identical to today** in both
directions: consumers always receive freshly-materialized vectors they
may mutate; the cache always owns private clones nobody else references.
Cost = one memmove per admitted/hit entry — memmove runs ~10× faster
than the zstd decompress + decode it replaces, so a hit recovers ~90%
of the per-chunk cost. (Copying does feed the memmove bill —
lever-ranking rank 2 — accepted for v1; see §7 for the zero-copy v2.)

Rejected shapes:

- **Zero-copy shared vectors** (hand out `*Vector` pointers, fresh
  `RecordBatch` shell): the cheapest steady state, but correctness
  rests on "no scan-path operator ever mutates vector storage" — an
  audit-dependent invariant with silent wrong-results as the failure
  mode. v2 candidate behind the same flag, not v1.
- **Decompressed-page cache** (wrap `Decompress`): keeps the ~7% decode
  kernels on every read, entangles with the zstd `sync.Pool` (a cached
  buffer must not be `putZstdBuf`'d), and for dictionary-encoded
  columns the decompressed pages are *larger* than the decoded vector.
  Strictly dominated.
- **Whole-row-group batch cache**: key must include the ordered column
  set → thrashes under projection variance (§2.3).

### 3.4 Memory ledger integration

Per ADR-0006 ("shared cache vectors are charged once; re-adding a
tracker charge on shared cache vectors is a known regression, do not"
— the 2026-07-06 Q21 8m35s incident):

- The cache **owns** its bytes as a **hard system reservoir**:
  `memory.NewReservoirFunc("decoded-rowgroup-cache", cap, cache.Size)`
  registered alongside `lru-cache/parquet-metadata`
  (`internal/worker/worker.go:283`). The cap enters the boot invariant
  (`Σcaps + 64 MiB + gcHeadroom ≤ GOMEMLIMIT`) and shrinks
  `Available()` automatically. No hot-path Reserve/Release.
- **Consumers are never charged** for cached bytes: they receive
  copies, which are ordinary scan-output batches charged exactly as
  today (decode-ahead `WindowLedger` estimate for the undelivered
  window, nothing after delivery). No double ownership exists by
  construction — the incident's failure shape (cache re-charging bytes
  hash-join builds already reserve) cannot arise.
- The clone held by the cache is new heap the ledger sees **once**, via
  the reservoir's live accessor.

### 3.5 Eviction, admission, pressure relief

- **Segmented LRU** (probation/protected) with **frequency-gated
  admission** (TinyLFU-lite, §9.2): a key's first decode registers a
  ghost carrying a touch count; below cap, the second touch admits;
  **at cap, a candidate must beat the eviction victim's frequency by a
  margin of 2** — ties and lockstep near-ties go to the incumbent, so
  a scan working set larger than the cap stabilizes a resident subset
  instead of thrashing (the 2026-08-12 pair's churn mechanism). The
  clone runs only after a positive admission decision; a rejected
  offer costs a map touch, not a memmove. Evicted entries re-ghost
  with their accumulated frequency so displaced-hot chunks can win
  back in; frequencies saturate at 63 and halve every 2^18 touches so
  incumbents stay displaceable when the workload shifts. Entries
  larger than cap/8 are never admitted.
- **Pressure relief (never-OOM):** the cache registers with the
  SpillManager as an `AccountedOperator`
  (`memory.RegisterAccounted`, `spill.go:163`): `Inspect` reports
  cache size as `SpillableBytes`, `SpillSome(target)` evicts
  coldest-first and returns bytes freed. `RequestRelief` therefore
  sheds cache — the cheapest relief in the process, it is *a cache* —
  before any operator pays a real spill. This is the first
  pressure-driven cache-eviction hook in the tree; it reuses the
  existing relief registry rather than inventing a parallel one
  (mmap-relief's RSS loop stays untouched — that valve is for
  non-heap bytes).
- **GC interaction:** vector storage is pointer-free (`[]byte`,
  `[]uint32`, typed slices, bitmap words), so the GC does not scan
  entry contents; mark cost is bounded by the entry map. The real cost
  of residency is a higher `HeapAlloc` baseline → the 70% backpressure
  and 95% pressure gauges fire earlier. That is intended (the cap is
  inside the boot invariant), and relief eviction is the pressure
  escape valve.

### 3.6 Invalidation

None needed. Keys are immutable by construction (§2.5); compaction/GC
retire object keys, leaving entries superfluous-but-correct until LRU
ages them out. Delete markers and dynamic filters operate on batch
shells / row-group selection above this layer and never mutate decoded
chunk content.

## 4. Config surface

- `--decoded-cache-bytes` (int64, default **0 = disabled**) — cache
  budget per process. The 0 default is the kill switch (new-feature
  convention: default-off until SF100-validated, cf.
  `--base-table-cache-bytes`).
- tpch-bench: `WADJET_DECODED_CACHE_BYTES` (default-off explicit parse,
  the `cmd/tpch-bench/main.go:477` pattern).
- Terraform: nullable `decoded_cache_bytes` var + profile fallback
  (the `eff_base_cache` pattern, `deploy/benchmark/terraform/main.tf:62`),
  flag emitted next to `--base-table-cache-bytes`.
- Counters (60s ticker, change-only, the base-cache pattern): hits,
  misses, hit-bytes, admitted, ghost-promotions, evictions,
  relief-evictions, size.

## 5. Sizing (SF100, honest math)

Per worker (scan affinity ≈ ⅓ of ~630 lineitem row groups ≈ 210):
one numeric lineitem column ≈ 1.6 GB; the 7 recurring hot columns
≈ 11–13 GB; the cross-query union including orders/part/partsupp is
larger. On a 32 GB node the boot invariant leaves roughly 4–8 GB of
safely cappable headroom after `gcHeadroom` (~20%), the metadata LRU,
and the result store.

**Proposed SF100 cap: 6 GiB** — holds the 3–4 hottest full lineitem
columns plus change. Segmented LRU + second-touch admission converges
the protected segment onto the highest-frequency columns; the cache
does not need to hold the working set to pay — it needs to hold the
*hot* subset. Expected recovery is therefore a **fraction** of the
24.2% zstd + ~7% decode bill, strongest on the scan-band queries
(Q01/Q06/Q12/Q14/Q15/Q19 class — the same band the NVMe cache moved),
near-zero on exchange-bound queries (Q17/Q18). If the hot-column
footprint proves to exceed any safe cap, the honest outcome is "heap
tier insufficient — NVMe decoded tier or nothing" (§7), decided by the
pair's hit-byte counters, not by wall alone.

## 6. What does NOT change

- Miss-path decode semantics, batch shapes, projection behavior,
  pushdown, sharding: byte-identical output whether the cache is
  off, cold, or hot.
- The parquet package's encode/decode paths (identity field is
  passive).
- Single-node `Scanner`/embedded API: no identity plumbed → cache
  never engages.
- mmap-relief, decode-ahead window ledger, spill thresholds.

## 7. Non-goals (v1) / rejected alternatives

- **Zero-copy hand-out** (fresh shells over shared vectors): v2 lever
  after a scan-path vector-mutation audit; removes the copy-on-hit
  memmove.
- **NVMe decoded-columnar tier** (spill the cache's cold tail to the
  idle NVMe in the columnar run format): v2 candidate if hit-byte
  counters show the heap cap is the binding constraint; trades zstd
  CPU for NVMe read + copy.
- **Write-side codec/level experiment**: rejected. The SF100 bucket is
  externally-produced zstd — re-encoding the benchmark dataset changes
  the measured workload (and does nothing for real deployments whose
  data arrives zstd); Wadjet's own writer already emits snappy.
- **Dictionary-only cache**: the per-chunk dictionary re-decompress +
  never-pooled dictionary buffer (§1) is a strictly smaller subset of
  this lever; the full cache subsumes it. The buffer-release fix is a
  separate small parquet change, not bundled here.
- Cross-process / coordinator-side caching; query-scoped pinning;
  cache warming.

## 8. Slices and gates

- **S1 — cache core + identity plumbing** (`internal/engine/scan/`
  decoded cache; `parquet.FileReader` identity field): unit tests
  (hit/miss/admission/eviction/relief/concurrency), `-race`,
  SF0.01 22/22 (parquet-safety gate).
- **S2 — worker wiring**: flag, reservoir registration,
  `RegisterAccounted` relief, counters, tpch-bench env, terraform var.
  Gates: full unit + `-race` (scan/worker/memory), SF0.01 22/22,
  tpch-harness `--mode=local` both flag states rows+checksums
  identical, cache arm also DAG-forced (`--local-fastpath-bytes=0`).
  Mergeable default-off on green (inert), per standing rules.
- **S3 — SF100 same-window pair** (deploy-gated, explicit approval):
  control `decoded_cache_bytes=0` vs treatment 6 GiB, runs=2, standard
  shapes. Decisive markers in order: hit-byte ratio and
  ghost-promotion counters (is the hot set cacheable?), zstd
  `decodeSync` CPU share collapse in the profile, scan-band walls,
  suite wall LAST. Rows 44/44 exact. ADR-0006: this class (memory
  pressure) is not SF100-safe on harness-green alone — the pair is
  mandatory before any default flip.

## 9. Implementation and local validation (2026-08-12)

Implemented as designed (S1+S2 in one branch): cache core + SLRU +
second-touch ghosts + AccountedOperator relief in
`internal/engine/scan/decoded_cache.go`; hook in
`ReadRowGroupNativeCached` (`columnar_native.go`); passive
`FileReader.SetCacheIdentity`; identity + wiring in
`internal/worker/stream_source.go` (eligibility mirrors the base-table
cache: `.parquet` and not `queries/`); executor/worker wiring with the
setter-order self-healing pattern; `--decoded-cache-bytes` flag,
terraform `decoded_cache_bytes` var, worker stats-ticker marker
(`"decoded-rowgroup cache stats"`, change-only).

Local gates, all green:

- Unit + `-race`: scan (lifecycle, isolation-under-mutation, eviction,
  relief, oversize rejection, 8-goroutine concurrency, decimal/null
  clone arms), worker (full-path engagement ghosts→admit→hit on both
  the decode-ahead and serial iterators; scratch-key ineligibility),
  parquet, full `./internal/...` suite.
- TPC-H SF0.01 22/22 (parquet-safety gate for the passive identity
  field).
- tpch-harness `--mode=local --slice=small`: PASS on default,
  cache-on (256 MiB), and cache-on DAG-forced
  (`--local-fastpath-bytes=0`) arms.
- Micro-bench (zstd row group, 64K rows, 4 columns):
  decode 2.65 ms/op → cache hit 0.41 ms/op (**−84%**), alloc bytes
  −56%, alloc count −65% (`BenchmarkDecodedChunkRead`).

Merged default-off (flag 0 = disabled ⇒ inert; standing default-off
merge rule). §8 S3 — the SF100 same-window pair — remains open and
deploy-gated; it decides any default flip and the tfvars cap.

### 9.1 SF100 same-window pair (2026-08-12 evening — split verdict, default stays OFF)

Control results/20260812-195726 (cache=0) vs treatment
results/20260812-201647 (6 GiB/worker), bin 26aaa55, runs=2, block rate
20000, rows **44/44 identical** across all four arms.

- **Steady state pays**: R2 322.1s → 264.6s (**−17.9%**), R2/R1
  1.077 → 0.715. Hit-bytes 37–76 GB served per worker; zstd
  `decodeSync` cum 697.9s → 483.1s (**−31%**). Mechanism-marked wins:
  Q08-R2 −87%, Q18-R2 −63%, Q12-R2 −47%, Q04-R2 −39%.
- **Cold pass pays a tax**: R1 299.1s → 370.1s (+23.7%). Named
  mechanism (worker counters): **admission churn** — 59–75K admissions
  vs 55–73K evictions per worker at the 6 GiB cap; ~95% of admission
  clones (multi-MB memmoves) evicted before any reuse; only ~3K entries
  live. Q16-R1 5.5s → 51.5s = a mid-query admission storm. Relief never
  fired (relief_mb=0); no rejections, no pressure events.
- Pair total +2.2% ⇒ **no default flip**; flag stays 0.

The cache mechanism is validated (R2 + zstd collapse); the admission
policy is the defect. Next iteration: **churn-controlled admission** —
at-cap admissions must beat the victim on ghost frequency (TinyLFU-lite
filter), or admit only while below cap and let ghost frequency promote;
either kills the wasted-clone tax while keeping the surviving hot set.
Re-pair after that lands.

### 9.2 Churn-controlled admission (2026-08-12, same evening)

Implemented as §9.1 prescribed, one refinement deeper: strict
greater-than alone still churns under a sequential flood, because
resident hits and ghost touches advance in lockstep — a candidate whose
scan position precedes the victim's transiently leads by one and would
displace the exact chunk about to be hit. At-cap admission therefore
requires `candidate ≥ victim + 2` (admitFreqMargin). Clone moved after
the admission decision (rejections cost no memmove); evictions re-ghost
with accumulated frequency; saturate-at-63 + halve-every-2^18-touches
aging. New counter: freq_rejected (worker stats line). Local gates all
green (race, full suite, SF0.01, harness 3 arms); churn-resistance and
hot-displacement regression tests added. SF100 re-pair verdict appended
below when run.

### 9.3 Re-pair verdict + pressure-yield valve (2026-08-12 night)

Re-pair on bin 5e5da01 (control results/20260812-210645 vs treatment
results/20260812-213009, 6 GiB, runs=2, profilers on). Rows 44/44
identical. **Churn fix validated**: admissions 59-75K → 4.2-4.9K
(−94%), evictions 55-73K → 0.7-1.2K, freq_rejected 26-126K carrying
the load, Q16-R1 storm gone (6.9→7.5s), R1 delta collapsed to +2.3%
(≈noise). Hit path again decisive where reuse exists: Q05-R2 −78%,
Q09-R2 −74%, Q06 −62/−73% (899ms — first sub-second SF100 query),
Q08-R2 −27%, Q21 −29%; hit-bytes rose to 101-106 GB/worker (stable
resident set serves more).

Pair wall +2.8% — **new named residual: pressure coupling.** Treatment
wlogs show 39-52 pressure events/worker (control 1-4): 6 GiB of
resident cache heap raises HeapAlloc and displaces page cache, firing
the heap-backpressure gauge and the refault sensor; decode-ahead
collapses (pressure_stall_ms ~9s/worker, refault_rate to 191K/s) and
producers pause — while the cache held every byte, because relief only
ran on the RequestRelief operator path (relief_mb=0), not the gauge
channels. Window caveats logged: control's own R2/R1 was 1.237
(evening ENA state), ctl-Q17-R2 a 1.4s freak-fast outlier, trt-Q07-R2
a 54.5s pressure-collapsed straggler.

**Valve shipped same night**: the worker stats loop now sheds the
cache to cap/2 while either pressure channel is active
(`ShedUnderPressure`, WARN marker "decoded-rowgroup cache pressure
shed"); evicted entries re-ghost with frequency and re-admit through
the gate when pressure clears. The cheapest bytes in the process yield
first. Default still OFF; the next same-window pair (deploy-gated)
judges the valve and the flip. If pressure-shed proves insufficient,
the fallback lever is a smaller cap (4 GiB) — the boot-invariant math
in §5 was optimistic about gauge headroom, not about reservoir caps.

### 9.4 Third pair: KEEPER — SF100 benchmark config flipped on

Third same-window pair (bin f251178 with the pressure valve; control
results/20260812-221530 vs treatment results/20260812-224230; the
slowest conditions of the night — control's own R2/R1 was 1.406, cause
unattributed, see the cross-arm residual below):

- Rows **44/44 identical** (third consecutive pair).
- **Pair −14.4%** (891.0 → 762.8s); R1 −20.4% (370.4 → 294.8), R2
  −10.1% (520.6 → 468.1). First decisively negative pair.
- Valve engaged: 10 "pressure shed" WARNs, ~15 GB relief-evicted,
  re-admission through the gate afterwards (admitted 15.3K — shed
  cycles, not churn; freq_rejected 111K still holding the flood).
- Hit-bytes 87 GB/worker; zstd `decodeSync` cum 18.7% (baseline 24.4%).

**Open residual (logged, does not block the flip):** treatment still
pays a pressure tax the valve only partially removes —
`pressure_stall_ms` 13s/141s/142s per worker vs control's **0**.
Candidate follow-ups if it ranks: lower shed low-water (cap/4),
pausing admission during pressure episodes, or a 4 GiB cap arm.

**Decision:** per the §8 judge order (rows → markers → walls), the
SF100 benchmark config pins `decoded_cache_bytes: 6442450944`
(profile + tfvars, the base-table-cache precedent — engine default
stays 0/opt-in). Cache-less repro: `-var=decoded_cache_bytes=0`.

**Cross-arm residual (open, correctly unattributed):** control R2/R1
degraded 1.077 → 1.237 → 1.406 across the evening's three pairs. ENA
counters were NOT polled on any arm (a doctrine miss —
network-bound-diagnosis-2026-08-09 requires reading wall deltas
against them), so "evening ENA state" claims made during the session
were labels, not measurements. Post-hoc CloudWatch (5-min buckets,
survives termination): per-worker byte volumes are flat across all
three controls (~42-48 GB in / 68-85 GB out) — the degradation is
lower realized throughput at equal bytes, not more bytes — and 5-min
average peaks sit below the c7gd baseline allowance, weakening pure
credit exhaustion. Remaining candidates: supply-side S3/network
latency (the day-vs-night S3 PUT effect observed 2026-07-12) vs
per-deploy placement lottery. Discrimination needs the ENA counters,
now sampled to journald every 60s by the worker user_data poller
(rides the auto-wlog); the clean-window re-baseline reads them.

## 10. Open questions

- Cap auto-derivation (fraction of GOMEMLIMIT once validated) — same
  open question the base-table cache carries; keep opt-in until then.
- Whether ghost admission should be bypassed for R2-style repeat runs
  (first-touch admit when the reservoir is mostly empty) — measure
  ghost-promotion latency in the pair first.
