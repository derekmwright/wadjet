# Performance Analysis — April 2026

> Static code analysis of the Wadjet execution engine, planner, storage layer, and competitive positioning. Builds on the existing `performance-bottlenecks.md` and `competitive-gaps.md` with fresh findings from the latest codebase.

## Overall Assessment

Wadjet's core execution engine is **well-optimized** for the hot path. Typed kernels, SoA accumulators, Fibonacci hashing, arena-based allocation, bloom filter pre-filtering, and selection-vector-based filtering are all in place. The codebase shows evidence of careful, iterative performance tuning.

**The remaining performance gaps are not in the hot path — they're in secondary paths (spill, shuffle, distinct), I/O strategy (full-file downloads, no page-level pruning), and planner intelligence (missing constant folding, limited cardinality estimation).**

---

## 1. Execution Engine Findings

### 1.1 Hot Path — Well Optimized

| Technique | Location | Status |
|-----------|----------|--------|
| Typed sort kernels (resolve once per batch) | `sort.go:171-305` | Done |
| Inline hash join probe (saves ~2.5GB allocs at SF1) | `join.go:1686-1791` | Done |
| SoA aggregate accumulators (~16MB vs ~192MB for 2M groups) | `agg_scatter.go` | Done |
| Fibonacci hashing for integer keys | `int_hash.go` | Done |
| Arena-stored string keys with hash tag fast-reject | `str_hash.go` | Done |
| Adaptive bloom filter (auto-disable at <5% rejection) | `bloom_filter_op.go:186-199` | Done |
| Top-K heap sort for LIMIT queries | `sort.go:249-280` | Done |
| Chunk allocator for group state (1.5M→366 heap objects) | `aggregate.go:150-170` | Done |
| Semi/anti join key-only mode (2-4x less memory) | `join.go:85-89` | Done |
| NoNulls kernel variants (skip bitmap checks) | `kernel/agg.go:68-71` | Done |
| Selection vector reuse in KernelFilter | `filter.go:379-381` | Done |

### 1.2 Recurring Anti-Pattern: Row-Oriented Fallbacks

**Three separate code paths** fall back to `map[string]any` row format under memory pressure or during distribution. This is the single largest systemic issue:

| Path | Location | Trigger | Impact |
|------|----------|---------|--------|
| Sort spill | `sort.go:102-145` | ORDER BY exceeds memory budget | 3-10x slower than in-memory sort |
| Aggregate spill | `aggregate.go:323-331` | GROUP BY exceeds memory budget | 2-5x slower; massive GC pressure |
| Worker shuffle | `executor.go:672` | Distributed hash redistribution | 20-40% overhead on every distributed query |

All three should use the columnar serialization already implemented in `join_spill.go:347-541`. This is the **highest-ROI engineering investment** — one columnar spill format unifies three paths.

### 1.3 Specific Operator Issues

**Distinct (`distinct.go:61-71`)**: `string(d.keyBuf)` allocates a new string for every row. The aggregate operator solved this with `strHashTable` arena-stored keys — same pattern should be applied. Est. 15-30% improvement on Q16, Q21.

**Window (`window.go:276`)**: `b.Compact()` always called before materialization, even when `Sel==nil`. Unnecessary allocation + type-switched copy. Should short-circuit when no selection vector is present.

**Filter — ColumnCompare (`filter.go:109-155`)**: Per-row type dispatch still active in some code paths. KernelFilter resolves type once per batch. All ColumnCompare usages should migrate.

**Batch Compact (`batch.go:132-183`)**: Row-major iteration (outer loop over selection indices, inner loop over columns). Cache-unfriendly for wide schemas. Should iterate columns-outer for vectorized copy.

### 1.4 Expression Evaluation

**Column references always use generic path** (`compile.go:347-403`): `compileCmp()` promotes to typed comparisons for literals, but ColRef expressions fall through to generic evaluation. This means JOIN and WHERE predicates on column references pay interface dispatch overhead per row.

**Function dispatch for non-numeric functions** (`expr.go:1259+`): String-based lookup in `funcTable` map. Numeric functions are wrapped in typed interfaces, but string/network/timestamp functions go through reflection-based dispatch.

---

## 2. Query Planner Findings

### 2.1 What's Implemented

| Optimization | Location | Quality |
|-------------|----------|---------|
| Subquery decorrelation (scalar → LEFT JOIN + Agg, IN → SemiJoin) | `optimizer.go:410-1012` | Solid |
| Predicate pushdown through joins | `optimizer.go:1015-1200` | Solid |
| Partition filter extraction (Hive-style) | `optimizer.go:1802-1900` | Solid |
| DP-based join reordering (≤16 relations, greedy fallback) | `optimizer.go:2427-2920` | Good |
| Projection pruning (column-level requirements) | `optimizer.go:2397-2420` | Good |
| OR predicate decomposition to DNF | `optimizer.go:1257-1400` | Good |
| NDV-based cardinality estimation | `stats.go` | Basic |

### 2.2 What's Missing (vs. DuckDB / ClickHouse)

| Missing Optimization | Impact | Effort | Notes |
|---------------------|--------|--------|-------|
| **Constant folding** | Low-Medium | Small | `SELECT 1+1` remains unevaluated; easy win |
| **Common subexpression elimination** | Medium | Medium | Duplicate subqueries evaluated multiple times |
| **Filter-through-aggregate** | Medium | Small | Filters on GROUP BY keys never pushed below aggregates |
| **Aggregate pushdown below joins** | Medium | Medium | Safe when aggregating on join key |
| **Merge join for pre-sorted data** | Medium | Medium | Only hash joins available; sorted data gets no benefit |
| **Size-aware broadcast decision** | High | Small | Fixed 10-file threshold (`plan.go:1984`); should use bytes |
| **Recursive CTE optimization** | Low | Large | Parsed but treated as opaque |
| **Grouping sets single-pass** | Low | Medium | Executed as separate GROUP BY scans |
| **Window partition distribution** | Medium | Medium | No influence on parallel execution order |

**Highest impact**: Size-aware broadcast join decision. The current 10-file heuristic can broadcast a 100GB table or partition-scan a 10KB lookup table. DuckDB uses actual cardinality/memory estimates.

### 2.3 Statistics Quality

Cardinality estimation uses heuristic NDV with fixed selectivity factors (0.33 for predicates, 0.25 for partitions). No actual table statistics (histogram, HLL sketch, min/max). This means:

- Join reordering may pick suboptimal order for skewed data
- Aggregate hash tables start at 4096 entries regardless of actual cardinality
- Broadcast join decisions ignore actual data size

**Fix**: Propagate Parquet-level statistics (row count, distinct count, min/max per column) through the planner. This is a medium effort with broad impact across join ordering, broadcast decisions, and hash table sizing.

---

## 3. Storage & I/O Findings

### 3.1 Parquet Reader

**Strengths**: Column projection, row-group statistics pruning, dictionary encoding with zero-copy unsafe.Slice(), nested type support with definition/repetition levels, all major compression codecs (Snappy, Gzip, Zstd, LZ4).

**Gaps**:

| Gap | Location | Impact |
|-----|----------|--------|
| No page-level predicate pruning | `reader.go:86-120` | 10-40% scan reduction on sorted/time-range data |
| No bloom filter exploitation | — | Could skip row groups for equality predicates |
| Row-group stats re-parsed on every call | `reader.go:66-126` | Minor CPU overhead on multi-predicate queries |
| Nested types fall back to row-oriented | `columnar.go:19-21` | Array/Map columns lose columnar benefits |

### 3.2 Object Store I/O

**Full-file download strategy** (`physical/util.go:454-461`): Downloads entire Parquet file even when projecting 2 of 30 columns. The code comments acknowledge this tradeoff ("a single S3 GET is cheaper than N range reads").

**Recommendation**: Add a cost heuristic — if `projected_columns / total_columns < 0.3`, use column-level range requests via the Parquet footer's byte offsets. Est. 50-80% bandwidth reduction on narrow queries.

**No HTTP response caching**: Repeated reads of the same file incur identical network costs. Worker-level LRU exists for file data but HTTP-layer caching is absent.

**No column-level I/O parallelism** (`physical/util.go:141-165`): Column pages within a row group are read sequentially. Parallelizing column reads would overlap I/O with decompression.

### 3.3 Spill-to-Disk

Row-oriented binary format with 64KB buffered writes (`spill.go:85-179`). Triggers at 60% memory budget exhaustion. **No columnar compression** — spilled data is 2-3x larger than equivalent Parquet. This is the same row-oriented fallback anti-pattern from section 1.2.

---

## 4. Competitive Positioning vs. SMB Targets

### 4.1 Engine-Level Comparison

| Capability | Wadjet | DuckDB | ClickHouse |
|-----------|--------|--------|------------|
| Vectorized execution | Yes (2048 batch) | Yes (2048 batch) | Yes (8192 batch) |
| Selection vectors | Yes | Yes | Yes (via filter masks) |
| Typed kernels | Yes | Yes | Yes |
| Morsel-driven parallelism | Partial (mutex source) | Yes | Yes |
| Adaptive query execution | No | Yes (AQE since v1.1) | Partial |
| JIT/codegen | No | No | Partial (LLVM optional) |
| Page-level Parquet pruning | No | Yes | Yes |
| Bloom filter index | No | Yes | Yes |
| Columnar spill | Partial (join only) | Yes (all operators) | Yes |
| Zone maps / min-max index | Row-group only | Row-group + segment | Yes (per-granule) |
| Join algorithms | Hash only | Hash + merge + IE | Hash + merge + partial |
| Constant folding | No | Yes | Yes |
| CTE materialization | No | Yes | Yes |
| Streaming aggregation | No | Yes (for sorted input) | Yes |

### 4.2 Where Wadjet Wins

1. **Network-native types**: 21 types including IPv4/IPv6/CIDR/MAC/Port/Protocol with dedicated storage. No competitor matches this.
2. **80+ network functions**: JA3, DNS, TLS, HTTP, TCP flags, GeoIP built-in.
3. **Single Go binary, 512MB viable**: Runs on edge hardware. ClickHouse needs 4+ GB.
4. **Distributed via NATS + S3**: Zero-coordinator-bottleneck. DuckDB is single-node.
5. **PostgreSQL wire protocol**: Works with psql, Superset, DBeaver, JDBC out of the box.
6. **RBAC + ABAC with cell-level policies**: Rare in embedded engines.
7. **MCP server for AI agents**: No competitor offers this.

### 4.3 Where Wadjet Loses

1. **No managed cloud offering**: Blocker for SMB adoption.
2. **No alerting/detection rules engine**: Every SIEM competitor has this.
3. **No materialized views / continuous aggregation**: Required for dashboards and alerting.
4. **No full-text search**: LIKE/regex only; no inverted index.
5. **No streaming ingestion from message queues**: Kafka/syslog require external Bento pipeline.
6. **Row-oriented spill paths**: Performance cliff under memory pressure.
7. **Missing planner optimizations**: No constant folding, no merge joins, no adaptive execution.

---

## 5. Prioritized Improvement Roadmap

### Tier 1: Highest ROI (address first)

| # | Improvement | Est. Impact | Effort |
|---|------------|-------------|--------|
| 1 | **Unified columnar spill format** (sort + aggregate + shuffle) | Eliminates row-format fallback across 3 paths; 2-10x on spill queries | Medium |
| 2 | **Column-selective S3 range requests** | 50-80% bandwidth reduction on narrow queries | Medium |
| 3 | **Page-level predicate pruning** in Parquet reader | 10-40% scan reduction on time-range queries | Medium |
| 4 | **Size-aware broadcast join decision** (bytes, not file count) | Prevents broadcasting large tables; prevents scanning small lookups | Small |

### Tier 2: Medium ROI

| # | Improvement | Est. Impact | Effort |
|---|------------|-------------|--------|
| 5 | Distinct operator hash-based dedup (arena keys) | 15-30% on DISTINCT queries (Q16, Q21) | Small |
| 6 | Column-level I/O parallelism within row groups | 15-30% on wide-table scans | Medium |
| 7 | Source→channel decoupling in parallel pipelines | 5-15% on I/O-bound parallel execution | Small |
| 8 | Constant folding in logical optimizer | Easy win; eliminates runtime evaluation of literals | Small |
| 9 | Propagate Parquet statistics through planner | Better join ordering, hash table sizing, broadcast decisions | Medium |
| 10 | Merge join for pre-sorted inputs | Avoids hash table build for already-sorted data | Medium |

### Tier 3: Polish

| # | Improvement | Est. Impact | Effort |
|---|------------|-------------|--------|
| 11 | Replace ColumnCompare with KernelFilter everywhere | 2-5% on filter-heavy queries | Small |
| 12 | Window operator Compact() short-circuit | Saves allocation when Sel==nil | Small |
| 13 | NFA-based LIKE matching | Prevents pathological backtracking | Small |
| 14 | Batch pool sharding for high concurrency | 1-3% at 8+ workers | Small |
| 15 | Filter schema caching in scanner | Minor; saves per-file map allocation | Small |

---

## 6. Summary

Wadjet's performance story is strong for an early-stage columnar engine in Go. The hot-path optimizations (typed kernels, SoA accumulators, Fibonacci hashing, selection vectors) are on par with what you'd expect from DuckDB's architecture. The gaps are in:

1. **Secondary paths** — spill, shuffle, and distinct fall back to row-oriented format
2. **I/O intelligence** — full-file downloads and no page-level pruning leave bandwidth on the table
3. **Planner sophistication** — missing constant folding, merge joins, and size-aware broadcast decisions

The **single highest-ROI investment** is a unified columnar spill format that replaces the `map[string]any` fallback in sort, aggregate, and worker shuffle. This eliminates the performance cliff when queries exceed memory budget — exactly the scenario that matters at SF10+ and in production.

For competitive positioning, Wadjet's network-native type system and single-binary deployment model are **genuinely unique differentiators** that no competitor matches. The engine gaps (materialized views, alerting, full-text search) are product-level concerns that can be addressed incrementally without architectural changes.
