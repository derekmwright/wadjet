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

- **Segmented LRU** (probation/protected) with **second-touch
  admission** via ghost keys: a key's first decode registers a
  ghost (key + size only); the clone is stored on the second touch.
  Sequential scan floods — the classic LRU killer, and exactly what a
  TPC-H suite's cold first pass is — cannot evict the protected
  segment, and single-touch keys never displace proven-hot entries.
  Entries larger than cap/8 are never admitted.
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

## 10. Open questions

- Cap auto-derivation (fraction of GOMEMLIMIT once validated) — same
  open question the base-table cache carries; keep opt-in until then.
- Whether ghost admission should be bypassed for R2-style repeat runs
  (first-touch admit when the reservoir is mostly empty) — measure
  ghost-promotion latency in the pair first.
