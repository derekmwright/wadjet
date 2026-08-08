# Base-table NVMe cache (cross-query parquet cache tier)

> **Status:** DESIGN FOR REVIEW — no code. **Date:** 2026-07-12.
> **Verified against:** main @ `562a358`. Anchors below were confirmed
> against that commit; they drift.
> **Context:** primary lever after the eager-dispatch arc verdict
> (results/20260712-201624/-205316): the pipeline is
> producer-throughput-bound, and the dominant producer-side block is
> base-table S3 fetch (`filePrefetcher` GETs ~11.6k goroutine-s +
> `acquireWindow` ~8.4k goroutine-s per suite, Σ3 workers) — unchanged by
> every 2026-07 lever because all of them sit downstream of the fetch.

## 1. Goal and evidence

Stop re-downloading immutable base-table parquet from S3 on every query.
Workers on the SF100 shape (c7gd.4xlarge) carry a 237 GB local NVMe
instance store of which only the spill directory is used; the SF100
working set (~100 GB parquet) fits on it whole. A cross-query, disk-backed,
read-through cache means each worker pays the S3 GET for a given file
once per cluster lifetime instead of once per query.

Evidence this is the binding constraint:

- Post-SE profile (results/20260712-034241): workers at **2.5 of 16
  cores**; critical-path block classes are barrier waits and
  `filePrefetcher` base-table S3 GETs + window waits.
- Eager-dispatch profiled pair (results/20260712-201624/-205316): removing
  barrier wait (−24.9% block) left utilization **flat** — consumers just
  moved their wait to producer output, and producers are gated on S3
  fetch/upload. Base-table fetch is the input half of that gate.
- The suite re-reads the same tables 22 times: lineitem alone is touched
  by 17 of 22 TPC-H queries. Today every touch is a full S3 re-download
  (the worker's only cross-query cache is the in-heap `LRUCache`, bounded
  to GOMEMLIMIT/10 ≈ 2 GB — 2% of the SF100 working set).
- Secondary availability win: the 2026-07-12 daytime incident class
  (S3 PUT latency → breaker pressure) gets smaller the fewer GETs we
  issue; cache hits bypass S3 entirely.

What this does NOT fix: shuffle/stage-output upload+fetch (the other half
of the producer gate) and the first, cold touch of every file. §9 bounds
the win honestly.

## 2. What the code does today (verified)

Three read paths, all resolving through `objstore.Store`:

- **Worker DAG scan (the SF100 path):** scan fragments read
  `spec.InputFiles` (base-table parquet keys) via
  `buildFragmentSource` → `sourceForAliasWithProjection` →
  `cachedFileStreamSource` (`executor_fragment.go:1639`). Each file is
  whole-object GET → streamed to a spill-dir temp → mmap'd → decoded →
  **unlinked** (`stream_source.go:291`). The `filePrefetcher`
  (`scan_prefetch.go`) overlaps the next files' GETs with decode
  (4 concurrent GETs per source, 256 MB window), also into throwaway
  temps. Whole-object GETs are deliberate: per-column ranged reads were
  tried 2026-03-20 and reverted for S3 throttling (`fe52a79`,
  `scan_prefetch.go:69-72`).
- **Single-process scan (standalone + local fast path):**
  `scanSourceInner.ensureLoaded` (`internal/planner/physical/util.go:341`)
  does `cat.Store().Get` → whole file into a pooled heap buffer →
  `parquet.NewReaderFromBytes`. Footer/row-group metadata reads use
  `GetReaderAt` when available (`util.go:576-625`).
- **Alt scanner:** `internal/engine/scan/scanner.go` uses
  `ReaderAtStore` column-chunk range reads when the store supports it
  (`scanner.go:181`).

Store construction and decorator chain:

- Base store built once in `cmd/wadjet/main.go` (`newStore()`), wrapped
  with `CircuitStore` at `main.go:405` — the single process-wide seam.
- The worker additionally wraps per-query with `CachedStore` (in-heap
  LRU, `executor.go:539`) before building the catalog, so both scan-side
  paths see `CachedStore → CircuitStore → MinIO`.
- `filePrefetcher` and `cachedFileStreamSource` call `executor.store`
  directly (`scan_prefetch.go:152`, `stream_source.go:400`) — i.e.
  `CircuitStore → MinIO`, no heap LRU.

File identity is immutable per key (the invalidation story):

- Ingest writes UUID-named chunks (`ingest.go:300`) and adds them to the
  manifest; nothing overwrites a live key.
- Compaction/GC replace files by writing **new** keys and swapping the
  manifest (`catalog.go:662` `SwapFileForGC`, `compactor.go:391`), then
  deleting old keys. A `(bucket, key)` pair is content-stable for its
  lifetime — the same assumption the in-heap `CachedStore` already makes
  (`cached_store.go:74`).

Disk today:

- SF100 workers are c7gd.4xlarge; cloud-init mounts the instance store at
  `/mnt/nvme` (model-string device selection, `main.tf:574-599`) and uses
  only `/mnt/nvme/spill/w$idx` (spill, `stage-cache`, prefetch temps).
- SF10 benchmark workers are c7g.4xlarge (no instance store; spill on the
  200 GB gp3 EBS volume). The cache works there too, just against EBS.

Prior art reused by this design: `CachedStore` (decorator + key
convention), `LocalStageCache` (disk-backed cache lifecycle, adopt-by-
rename, startup sweep — `local_stage_cache.go`), `filePrefetcher` (window
accounting), `diskio` write classes.

## 3. Design: read-through decorator at the store seam

One new type, `objstore`-level:

```go
// BaseTableCache is a read-through, disk-backed, whole-file cache for
// immutable base-table parquet objects. It decorates an inner Store;
// non-eligible keys pass through untouched.
type BaseTableCache struct { inner objstore.Store /* + dir, budget, lru */ }
```

Inserted **once, process-wide**, directly above the circuit breaker at
`main.go:405`:

```
worker per-query:  CachedStore(heap LRU) → BaseTableCache(NVMe) → CircuitStore → MinIO
direct callers:                            BaseTableCache(NVMe) → CircuitStore → MinIO
```

- Above `CircuitStore` so cache **hits never consult the breaker** (a
  warm cache keeps working through an S3 brownout) while population
  misses keep full breaker protection.
- Below `CachedStore` so the heap LRU stays the fastest tier; its misses
  now fall to NVMe instead of S3.
- One seam serves all three read paths plus the prefetcher, standalone
  mode included. No scan-path code changes in slice 1.

**Eligibility filter:** `strings.HasSuffix(key, ".parquet") &&
!strings.HasPrefix(key, "queries/")`. Query-scratch objects (stage
outputs, aggregate-cache, build-cache `.wshf`) live under `queries/<id>/`
(`cleanup.go:47`) and already have their own tiers (LocalStageCache, KV,
peer fetch); shuffle keys must not be blind-cached (they race async
upload — same rationale as `scan_prefetch.go:60-63`). Everything else
passes through verbatim.

**Hit path:**

- `Get`: open the cache file, return it as the `ReadCloser` with
  `ObjectInfo{Size: stat.Size()}`. (Callers on these paths use only
  `Size`; ETag/LastModified are best-effort absent, matching `FileStore`
  behavior.)
- `GetReaderAt` (`ReaderAtStore`): return the `*os.File` directly —
  column-chunk range reads become local pread. Footer-only reads in
  `buildRGUnits` get the same benefit.

**Miss path (read-through):** `Get` on an eligible key GETs from inner
and returns a body that **tees** to `<dir>/tmp/<rand>.tmp` while the
consumer reads. On clean EOF with byte count matching the advertised
`ObjectInfo.Size`, fsync + rename into place (atomic on same fs; the
`LocalStageCache.Adopt` pattern) and admit to the LRU index. On any
error, short read, or early close: discard the temp. Strictly
best-effort — the cache can never change results, only skip GETs.

- No blocking single-flight: concurrent misses on one key each stream
  from S3 independently; an in-flight set makes only the **first** tee
  (the rest stream without teeing), and rename is idempotent anyway.
  Rationale: sharing one download across consumers couples their
  lifetimes and re-creates the cancellation-poisoning class the breaker
  incident just taught us (PR #220); duplicate GETs during warmup are
  bounded and vanish once the entry lands. (One carve-out, later: the
  peer tier's owner read-through — `ReadThrough`, scan-affinity.md
  §first-touch single-flight — DOES single-flight, but only the
  detached owner-side populate serving peer fetches; local consumer
  streams stay uncoupled.)
- `GetReaderAt` misses pass through to inner **without** populating
  (population needs the whole object; ranged misses are footer-sized).
  The whole-file `Get` that follows on every scan path populates.
- Population writes use `diskio.NewWriter` with the page-cache-dropping
  class (NOT `KeepResident`): the consumer is reading the teed stream,
  not the cache file; keeping 100 GB of write-back pages resident would
  fight the decode working set (bounded-dirty-writes lesson).

## 4. Lifecycle: eviction, restart, stale keys

- **Layout:** `<cache-dir>/<bucket>/<key>` mirroring object paths
  (FileStore convention) — debuggable with `ls`, and the startup walk
  recovers `(bucket, key)` without sidecar metadata.
- **Eviction:** LRU by bytes against `--base-table-cache-bytes`
  (mirror of `internal/worker/cache.go`, disk-backed). Admission evicts
  from the tail until the new entry fits; an entry larger than the whole
  budget is never admitted. Recency is the in-memory list; no atime
  dependence.
- **Restart:** walk the dir at startup, delete `tmp/`, rebuild the index
  (recency seeded by mtime). Cache survives process restarts — unlike
  `LocalStageCache` there is no `RemoveAll`, because entries are
  query-independent. Instance-store NVMe is wiped by instance stop, which
  is fine (cold start = cold cache).
- **Stale keys** (compaction/GC deleted the S3 object): harmless — no new
  manifest references them, so nothing ever Gets them again; LRU evicts
  them eventually. No manifest cross-check in v1 (a janitor is easy to
  add later if idle-cluster disk occupancy ever matters).
- **Concurrent eviction vs. open readers:** eviction unlinks; POSIX keeps
  the inode alive for already-open FDs/mmaps, so in-flight reads are
  safe. Disk usage can transiently exceed budget by the unlinked-but-open
  bytes; bounded by open-file count × file size, same exposure the spill
  path already has.

## 5. Config surface

- `--base-table-cache-bytes` (int64, **default 0 = disabled**) — the
  rollout kill switch, consistent with every 2026 arc (flag off until the
  SF100 pair validates).
- `--base-table-cache-dir` (string, default
  `filepath.Join(spillDir, "base-cache")`) — inherits the NVMe mount with
  zero terraform changes; override for setups where cache and spill
  should live on different volumes.
- Plumbing mirrors `--cache-bytes`: flag vars + registration next to
  `main.go:152-153`; since the decorator is constructed in `main.go`
  (not `worker.New`), the values do not need to ride `worker.Config` —
  standalone and worker modes share the one seam.
- Terraform: `base_table_cache_bytes` var, default 0; the SF100 tfvars
  set it (proposed: 150 GB per worker process — SF100 working set
  ~100 GB, leaves ~85 GB of the 237 GB NVMe for spill; spill peak in
  recent SF100 runs is far below that, and `workers_per_node` is 1
  everywhere current so no per-process split is needed).
- tpch-bench: `WADJET_BASE_TABLE_CACHE_BYTES` pass-through, default-off
  parse (the `WADJET_EAGER_DISPATCH` convention, not `envBoolDefaultOn` —
  this flag is off in the engine too).

## 6. Non-goals (v1)

- **No range/block-granular caching.** Whole files only — matches the
  whole-object GET shape the scan paths already use and the fe52a79
  throttling lesson. The `GetReaderAt` hit path gives range *serving*
  from whole-file entries for free.
- **No cross-process shared cache.** One cache per worker process. With
  `workers_per_node > 1` this duplicates hot tables per process; every
  current tfvars runs 1 process/node. Cross-process sharing needs
  flock/ownership design — deferred until a shape needs it.
- **No shuffle/stage-output caching.** Different lifecycle (query-scoped,
  racing async upload), already tiered.
- **No proactive warming.** First touch pays S3; the suite warms itself.
- **No spill/cache unified disk budget.** Static byte budget in v1;
  reactive eviction on ENOSPC is a follow-up if real runs ever hit it.

## 7. Slices

- **S1 — decorator:** `BaseTableCache` in `objstore` (or a sibling
  package if the LRU import direction gets awkward), seam wiring in
  `main.go`, flags, counters. Unit tests: round-trip, eligibility filter,
  short-read/error discard, size-mismatch discard, eviction order,
  budget-overflow admission, startup rebuild (incl. tmp sweep),
  concurrent same-key misses, `GetReaderAt` hit correctness,
  pass-through for non-eligible keys. Race-enabled.
- **S2 — mmap-in-place hit tier (worker):** `cachedFileStreamSource` and
  `filePrefetcher.fetch` check the cache for a local path *before*
  spending a GET or a disk→disk copy, mmap the cache file directly, and
  leave `s.localPath` empty (cache owns the file — exactly the
  `LocalStageCache` tier-0 pattern, `stream_source.go:334-347`). Needs a
  narrow optional interface (`LocalPath(bucket, key) (path string, ok
  bool)` + refcount pin so eviction skips mapped entries). Without S2 a
  worker hit still copies cache→spill-temp→mmap; that copy is
  NVMe-speed (not S3-speed) but is ~100 GB of avoidable writes per
  suite. S2 is where the worker win gets clean; S1 alone already serves
  the single-process paths at full value.
- **S3 — validation + terraform:** gates in §8, terraform var + tfvars,
  memo update with results.

S1 and S2 land behind the same flag; there is no useful intermediate
default-on state.

## 8. Gates and validation

Local (before any EC2, per the standing rule):

- Full `./internal/...` unit + race suites.
- SF0.01 `TestTPCHQueries` 22/22.
- `tpch-harness --mode=local` both flag states: 25/25, rows + checksums
  identical; cache-on arm additionally DAG-forced
  (`--local-fastpath-bytes=0`) so the worker path engages.
- Marker greps: `BaseTableCacheHits/Misses/HitBytes/MissBytes/Evictions`
  counters + a debug log line on first hit per source; harness summary
  prints hit-byte ratio per arm.

SF100 pair (same-window, control first, teardown discipline):

- Control = cache off, treatment = cache on (150 GB). 22/22 both arms,
  rows identical to results/20260712-025644 baseline.
- Decisive markers, in order of authority: (1) hit-byte ratio per worker
  (expect →~95%+ after each table's first touch; lineitem is re-read by
  17 queries), (2) `filePrefetcher` GET + `acquireWindow` block time
  (expect the 11.6k + 8.4k goroutine-s to collapse to the cold-touch
  residue), (3) per-worker core utilization vs the 2.5/16 reference,
  (4) suite wall. Wall is the last signal, not the first
  (`feedback_no_revert_on_serial_clog`); if blocks collapse and
  utilization rises but wall is flat, the next gate has revealed itself
  (upload side / decode) and the profile re-rank says where — that is
  a finding, not a failure.
- Run the treatment suite **twice in-process** (same cluster, back-to-back
  harness invocations): first pass = cold+warming, second = steady-state.
  Report both walls; the second pass is the number that represents a
  long-lived cluster, which is the deployment reality this feature
  models.

## 9. Expected effect (honest bounds)

- Upper bound on wall: the share of critical-path time that is base-table
  GET latency/bandwidth. The post-SE profile puts prefetcher GET block at
  ~3.9 k goroutine-s/worker critical-path (plus window waits); with scan
  legs at 2.5/16-core utilization, S3→NVMe read bandwidth (multi-GB/s vs
  ~200-400 MB/s effective per source today) should raise producer
  throughput materially on scan-heavy queries (Q01/Q06-class first, then
  every join's scan legs).
- The eager-arc verdict predicts the residual: once fetch is local,
  either utilization rises (fetch was the gate — win) or the block moves
  to upload/decode (next lever identified from the same profile). Both
  outcomes are decision-grade; the pair is worth running either way.
- Not measured by TPC-H but real: warm-cache availability during S3
  latency events, and S3 GET cost reduction on long-lived clusters
  (the AtumForge deployment model — same tables queried all day).

## 10. SF100 validation results (2026-07-13, added post-pair)

Same-window pair on bin `c32fbc6` (PR #222), clusters destroyed+verified:

- **Control** (cache off) `results/20260713-013357`: 22/22, **31m42s**.
- **Treatment** (150 GB, `benchmark_runs=2`) `results/20260713-021126`:
  run 1 (cold+warming) **29m54s = −5.7%**, run 2 (steady-state)
  **28m50s = −9.0%**. 22/22 both runs, **row counts identical to control
  on every query in all three suites**.
- Steady-state scan-band deltas vs control: Q19 −32%, Q06 −29%, Q08
  −29%, Q20 −29%, Q16 −24%, Q12 −23%, Q09 −21%, Q05 −17%, Q15 −15%,
  Q21 −12%. Exchange-bound stayed flat exactly as §9 predicted: Q17
  +1.5%, Q03 −1.7%, Q18 −3.3%. Positive movers (Q02 +15%, Q13 +13.5%)
  are the known day-window-volatile pair, not cache-marked.
- Engagement: worker base-cache reached **24 GB ≈ the entire SF100
  parquet footprint** mid-run-1 (live `du` via SSM); run-1's own tail
  already benefited (Q06 −31% cold — lineitem was resident by Q06 from
  earlier queries' populations).
- Not captured: per-worker hit-byte ratios (worker logs went down with
  the prompt teardown; the stats ticker lines weren't pulled first).
  The wall pattern + cache-size evidence is decisive enough that the
  pair wasn't re-run for it; capture stats lines before teardown next
  time.
- New SF100 steady-state reference: **28m50s** (prior best 30m38s,
  results/20260712-025644).

## 11. Open questions for review

1. **Benchmark reporting convention (PM):** a warm cache changes what the
   suite measures — cold-S3 has been the implicit contract for every
   baseline to date. Proposal: keep cold-suite (cache-on, first pass) as
   the comparable headline number, report steady-state second-pass
   alongside. OK?
2. **Default-on criteria (PM):** flag stays off until the SF100 pair;
   assuming row-identity + no regression, does default-on also want the
   flag exercised on the SF10 shape (EBS-backed cache) first?
3. **S2 scope check (technical, decide-and-execute unless objected):**
   the refcount-pin + optional-interface complexity is justified by
   avoiding ~100 GB/suite of NVMe write traffic on the hit path; the
   alternative (S1-only, accept the disk→disk copy) is simpler and still
   captures most of the S3 win. Current plan says do S2.
