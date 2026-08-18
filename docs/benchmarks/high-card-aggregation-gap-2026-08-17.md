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
- **G5 hash once + mask, not mod** — SHIPPED (`partitioned_agg.go`). Under
  partitioned aggregation every row was hashed TWICE: the router hashed the
  whole key (mix64/fnv1a) to choose an owner, then the owning sink's table
  hashed the same key again (fibHash / packedHash / strHash) to choose a slot
  — the same ~88-byte URL through two full passes on Q34. The router now
  computes THE SINK'S OWN hash and threads it through `partitionItem`; the two
  consumers read disjoint bit windows of that one value: partition = the top
  `ceil(log2(parts))` bits, slot = the low `log2(cap)` bits, unchanged from
  what each table already masked. Unifying in this direction (tables keep
  their hash, router adopts it) is what makes the change invariant — every
  table sees bit-for-bit the slot sequence it saw before, so dense integer
  ids keep fibHash's collision-free low-bit bijection.
  Owner selection is `bits.Mul64(hash, parts)` (Lemire multiply-shift), not
  `% parts`: `parts` is `runtime.NumCPU()`, never a power of two, and a
  hardware 64-bit divide per row is 20-40 unpipelined cycles. For a
  power-of-two `parts` the multiply-shift is bit-identical to DuckDB's
  `hash >> (64 - radix_bits)`. Routing also became a counting sort, so each
  batch takes ONE pooled buffer instead of `parts` append chains.
  Kill switch `WADJET_HASH_ONCE` (oracle-swept). Measured on the isolated
  router+probe bench (5900X, 1M keys, 8 partitions, min of 10 interleaved
  rounds), the x4 arms replaying the input so 75% of probes are lookups —
  Q34's real ratio: single-string 664.8 → 449.7 ms (**−32%**), two-int64
  packed 366.9 → 326.9 ms (−11%, p25 −16%), single-int64 216.2 → 198.6 ms
  (−8%); at 1:1 insert ratio −13%, −21%, −5%. Isolated table probe with a
  supplied hash vs recomputing strHash over 90 bytes: 426.6 → 302.4 ms
  per 1M probes (−29%).
  The routed and self-hashing loops are written out SEPARATELY in all three
  consume paths. A per-row `if provided != nil` is perfectly predicted and
  still cost +4% on the serial packed path (same-window A/B against the
  parent commit, two-int64 2.22 → 2.32 ms) — at ~4 ns/row one extra compare
  is real money. Hoisted, the self-hashing loop compiles to 71 instructions
  against the baseline's 74. The routing fields also sit LAST in
  HashAggregate: inserting them mid-struct shifts every hot consume-loop
  field's offset, which measured on its own.
- **G5b batched probe** — MEASURED, NOT SHIPPED. Restructuring phase 1 into a
  branchless slot-index pass over the batch followed by a probe pass does not
  win outside noise, and an explicit look-ahead load actively hurts
  (`BenchmarkPackedProbeShapes`, min of 5 rounds): at 1M keys onepass 24.0 ms,
  twopass 25.2, look-ahead d=8/16 29.8/29.8; at 20M keys onepass 859.9,
  twopass 887.1, look-ahead 1003.9/1078.1. Go's codegen turns the "prefetch"
  into a real bounds-checked load the out-of-order window was already
  covering. The consume loops keep their one-pass shape.
- **G6 two-level/radix table** — SHIPPED for the int and packed key modes
  (`two_level_hash.go`, kill switch `WADJET_TWO_LEVEL_HT`). 256 sub-tables
  selected by a hash window DISJOINT from the partition and slot windows;
  each grows by its own doubling, so a rehash touches 1/256 of the entries
  and the growth transient stops being 1.5x the whole table. Adaptive: a
  sink starts flat (byte-identical to G5) and converts once it holds
  > `twoLevelConvertAt` live entries AND the batch that crossed the line was
  still minting groups for a quarter of its rows — the second test is what
  stops a saturated table paying for a conversion it can never amortize.
  Three findings worth carrying forward:
  (a) the bucket window must be the LOW 8 bits, not ClickHouse's middle
  window. Ours is not an avalanching hash: `fibHash` is multiply-only, so
  ANY high-ish window selects a near-arithmetic subsequence of a dense key
  range and collapses it — 6.46 average probes per insert at 8M keys, worse
  with scale, against exactly 1.00 for the low window. With the low window
  (bucket, slot) is bit-for-bit the index a flat table of 256*subcap slots
  computes, so every spread property of the flat tables carries over
  unchanged.
  (b) a bucket's entry array may only go off-heap once it is at least a
  huge page (2 MiB). Below that a per-bucket mapping is 4 KiB-paged and the
  fault + dTLB cost swamps the structure: 8M int groups measured 281 ms with
  a 2 MiB gate, 427 ms with a 64 KiB gate, against 306 ms flat.
  (c) the payoff on THIS stack is smaller than the literature suggests,
  because our flat table already avoids what the literature's does not — its
  entries live in a MAP_NORESERVE reservation, so a doubling is one
  huge-page-backed mmap and never Go-heap garbage. Consume-path measurement
  (5900X, near-unique, min of 5): single-int 8M groups 783 -> 758 ms
  (-3.2%), packed-two-int64 8M 1144 -> 1170 ms (+2.2%); peak RSS filling 32M
  int keys 1809 -> 1685 MB. The threshold default is therefore 1M entries
  per sink — below it nothing changes at all — and the remaining structural
  wins (bucket-parallel emit, bucket-parallel merge, stats-free sizing) are
  still on the table. The string mode is NOT converted yet; its arena needs
  the split described in `two_level_hash.go`.
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
G1+G2 delegated for implementation 2026-08-17; G3, G4, G5 and G6 shipped the
same day (G5's batched-probe rider measured and declined; G6's string mode and
bucket-parallel emit deferred). Next: G7, then G6's riders — bucket-parallel
emit (the emit drain can now fan out per BUCKET within a partition, which is
what would let per-worker Top-N feed `Sort.CloneSink` as G4 wanted) and the
string-mode conversion.

Found while sweeping the unified hash for spread, recorded in
`TestFibHashStrideCollapse`: `fibHash` is multiply-only, so the low k bits of
`key * phi` depend only on the low k bits of the key. A single-int GROUP BY
whose keys are all multiples of 2^s puts EVERY key on one probe chain in any
table with <= 2^s slots. Pre-existing (this is intHashTable's own indexing,
untouched by G5) and invisible to the partition bits, which come off the top
of the same product. Fixing it means re-spreading every int-keyed table and
giving up the collision-free bijection dense ids currently enjoy — a separate
decision with its own A/B, not a G5 rider.

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
