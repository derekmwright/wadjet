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

## Post-wave-6 rerank (hot 99.4s era) and the DuckDB-parity ceiling

Field-best ratios on Q18/Q25-27 (~0ms answers) are index-system mirages;
the achievable bar for the data-lake shape is DuckDB-on-Parquet. At
per-query DuckDB parity our hot geomean goes x15.7 -> x6.0 (hot rank
#67 -> ~#21) with zero index building — that is the whole remaining
engine gap.

Ranked by vs-DuckDB gap (wave-6): Q27 9.3x, Q33 8.7x (G6/G7), Q25 7.5x,
Q24 7.2x, Q28 7.0x, Q36 6.3x (G-queue), Q35 3.7x, Q26 3.2x, Q18/Q23
2.7x, Q40 2.1x.

NEXT ARC after G5-G7: the string-TopN/materialization family
(Q24/25/26/27/28) — filter -> ORDER BY x LIMIT k shapes that
materialize far too much before the TopN decides. Specific lever:
LENGTH(byte-array col) is an offsets subtraction — Q28's
AVG(LENGTH(URL)) never needs the URL bytes (projection pushdown of
length()). Q24 is SELECT-* TopN: late-materialize everything behind
the TopN decision.

## Corrections from external review (2026-08-17 evening, verified)

- COMBINED at hot-parity is #8/132 (x4.83), not "top 30": the banked
  cold #18 position (0.2 weight) means hot improvements convert to
  combined rank at close to full value. Staged: G5-G7 alone ~ combined
  #40; + string-TopN family ~ #30; full DuckDB hot-parity #8.
- The Q28 LENGTH() lever generalizes to an OFFSETS-SHAPE EVALUATION
  CLASS: any consumer of a var-length column's shape rather than its
  contents — LENGTH/octet_length, IS NULL, = '' / <> '' comparisons,
  COUNT(col) — can run off the offsets array with zero byte decode.
  Two landing sites: (a) expr kernels over decoded BytesColumn reading
  Offsets only; (b) a lengths-only column decode mode that parses
  length prefixes and never memcpys values (composes with #299
  sel-decode). Note WHERE SearchPhrase <> '' appears across Q22-27:
  dict pages already answer it per-entry at the scan filter; the class
  pays on PLAIN pages and on post-scan expression evaluation.
- Q29 anomaly: hot (13.52 = min of two warm tries) is SLOWER than cold
  (13.21) in wave-6 — the only query that regresses warm. Suspect
  nondeterminism (GC timing / allocation growth across tries / the
  GOMEMLIMIT margin class from the Q33 postmortem), not throughput.
  Investigate before any ClickBench submission: a bimodal spike in a
  published run is the reproducibility risk.
