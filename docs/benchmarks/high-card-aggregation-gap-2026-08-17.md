# High-cardinality aggregation: gap analysis and attack order

2026-08-17. Profiled on hits_0.parquet (1M rows) locally + c6a.4xlarge wave-4b
metal numbers; external mechanisms verified against ClickHouse master and
DuckDB main sources. Companion to the per-query-floor falsification on #285.

## The gap

ClickBench Q33 (`GROUP BY WatchID, ClientIP` — ~100M near-unique groups,
WatchID is 1.0 distinct/row): we run 21.6s hot on c6a.4xlarge = 3454
ns/row/core. A 1s answer needs 160. Field reference on the same machine:
Umbra 212, DuckDB 326, ClickHouse 457 ns/row/core. Our *cache-resident
instruction cost alone* measures 334 ns/row/core — already 2x over budget
with a hot table — so both the instruction path and the memory/serial
regime must be fixed; neither alone closes it.

Path confirmation: Q33 = dual-int SoA path (consumeBatchDualIntGroup,
62.8% of CPU); Q34/Q35 = single-string path (consumeBatchStrGroup,
69-72%); Q19 (int,int-expr,string) = generic typed-lookup path.
ORDER BY count DESC LIMIT 10 is <2% everywhere — the Top-N is already
right. The emit phase is single-threaded and 16-35% of WALL clock
(partitions are aggregated in parallel, then streamed out serially at
~46ns/group: >=4.6s irreducibly serial at Q33 scale).

## Ranked gaps (G1 cheapest/largest first)

- **G1 appendGroup branch storm** (agg_scatter.go:36-71): ~33 branch
  evaluations per row at 1:1 group:row; 22.4% of Q33 CPU. Plus redundant
  per-aggregate count[] (16B/group) and []*groupState of pure nils used
  as a counter (8B/group, GC-scanned). Batch growth + shared counts +
  int counter. Zero row-set risk.
- **G2 string arena realloc + double key copy** (str_hash.go): append-grown
  arena = 10x write amplification, 24.8% of Q34 CPU; `string(key)` second
  copy into serializedKeys = 7.3% + 1.6GB dup at 18M URLs. Chunked arena,
  arena-backed key refs. BUG found: Init() hard-codes strGroupIndex=4096
  (aggregate.go:631) making the NDV presize branches dead code.
- **G3 packed <=16B composite keys, single-probe insert** — SHIPPED
  (`packed_hash.go`). The dual-int path is REPLACED, not supplemented: a
  `packedHashTable` holds the whole 128-bit key inline in a 24-byte entry
  (`{keyLo, keyHi uint64, val int32}`), so one probe resolves a group
  against data already in the loaded cache line — no chain array, no
  Get-then-Put, and the three per-group SoA arrays (keysA/keysB/next, 20
  B/group) collapse to one `[]packedKey` (16 B/group). Empty slots are
  marked by `val == -1`, not by a key pattern: all 2^128 keys are legal, so
  the presence bit has to live on the value side (group ids are never
  negative). NULL keys still migrate the whole aggregate to the generic path
  before consumption, which is what lets the packing be total — no bit
  pattern is reserved for NULL. Routing covers every old dual-int shape (two
  int-class columns are at most 8+8 B) plus 3-4 narrow-int shapes gated on
  `WADJET_PACKED_KEYS`. Measured on the near-unique bench (5900X, 32K rows ×
  16 batches, COUNT+SUM+AVG): two-int64 3.89 → 2.56 ms (−34%), four-int32
  5.16 → 2.75 ms (−47%) with 4.7 MB and 32.8K allocs/op falling to 12.7 KB
  and 28 (that shape used to serialize keys on the generic path).
- **G4 parallel emit** — SHIPPED (`aggregate_parallel_emit.go`): one drain
  goroutine per adopted partition (plus the primary's own state), batches
  handed to the downstream over a bounded channel. Kill switch
  `WADJET_PARALLEL_EMIT`. Spilled aggregates (streaming partial-state
  merger) keep the serial path. Measured on 1M near-unique int groups ×8
  partitions: emission 30.6→11.6 ms (2.6x), emission into ORDER BY cnt
  DESC LIMIT 10 39.0→17.1 ms (2.3x), whole GROUP BY + Top-N pipeline
  73.8→47.9 ms (−35% wall). The residual is the still-serial consumer:
  per-worker Top-N (feeding `Sort.CloneSink`'s existing per-clone top-K
  from k emit workers) would take another ~5 ms of the 17, but it requires
  making `aggSourceAdapter` a concurrency-safe source for EVERY aggregate
  pipeline, so it is deliberately left to G6's bucket-parallel emit.
- **G5 hash once + batched salted probe**: router (partitioned_agg.go:134-163)
  and sink both hash every row; `% parts` is a hardware divide. Thread the
  hash through partitionItem, mask, two-pass probe, L2-gated look-ahead.
- **G6 two-level/radix table**: 256-bucket conversion past ~100K keys
  (ClickHouse) / radix partitions (DuckDB). Incremental sub-table rehash,
  bucket-parallel merge+emit, stats-free sizing. Subsumes G4's shape.
- **G7 adaptive near-unique bail-out** (the Q33 endgame): DuckDB skips
  probing entirely past 95% uniqueness (HLL over group hashes) and dedups
  once at emit; ClickHouse freezes tables at 16K keys and routes new keys
  to bucket backlogs. Probe-every-row cannot reach the leaders' numbers
  on a 100%-unique key. Kill switch + invariance oracle mandatory.
- **G8 dictionary-aware string aggregation**: measured smaller than the
  literature suggests on hits (URL dict 56K-226K entries/RG exceeds
  DuckDB's own 20K gate); second wave.

Explicitly NOT the explanation: JIT, software prefetch alone, Top-N.
extract(minute) is 16.7% of Q19 — real, but a scalar-vectorization item.

Shared taxes: every recorded ClickBench run has GroupNDVHint=0 (harness
registers without ANALYZE), so tables start at 64K and rehash up to 8x.

## Attack order

G1 -> G2 -> G3 (instruction path; validate on the controlled-cardinality
harness) -> G4 -> G5 -> G6 (memory/serial regime) -> G7 (endgame).
G1+G2 delegated for implementation 2026-08-17; G3 and G4 shipped the same
day. Next: G5 (hash once + batched salted probe), then G6.
