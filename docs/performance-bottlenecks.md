# Performance Bottleneck Analysis

> Deep analysis of Wadjet's execution engine, scan layer, join/sort/aggregate operators, memory management, and distributed execution. Findings prioritized by estimated impact on TPC-H SF1+ workloads.

## Executive Summary

Wadjet's execution engine is well-optimized with typed kernels, SoA accumulators, Fibonacci hashing, bloom filter pre-filtering, and arena-based allocation. The codebase shows evidence of careful performance tuning (inline probing saves ~2.5GB allocations at SF1, SoA reduces aggregate working set from ~192MB to ~16MB for 2M groups).

The remaining bottlenecks fall into three categories:
1. **Scan I/O patterns** — full-file downloads, no column-level parallelism, no page-level pruning
2. **Allocation pressure in secondary paths** — Distinct, spill, OFFSET, shuffle
3. **Scalability ceilings** — single-mutex batch pool, source lock in parallel pipelines

---

## P0: High-Impact Bottlenecks

### 1. Distinct Operator: Per-Row String Allocation

**Location:** `internal/engine/exec/distinct.go:61-71`

```go
func (d *Distinct) rowKey(b *batch.RecordBatch, row int) string {
    d.keyBuf = d.keyBuf[:0]
    for _, col := range b.Columns { ... }
    return string(d.keyBuf)  // allocates every row
}
```

The `string(d.keyBuf)` conversion allocates a new string for every row to use as a map key. For a 10M-row DISTINCT query, this creates 10M heap objects.

**Fix:** Hash `keyBuf` with xxHash/FNV and use `map[uint64]struct{}` with collision chain, or use `unsafe.String()` (Go 1.20+) for the map lookup path. The aggregate operator already solves this with `strHashTable`'s arena-stored keys — the same pattern should be applied here.

**Estimated impact:** 15-30% improvement on DISTINCT-heavy queries (TPC-H Q16, Q21).

---

### 2. Spill Sort Falls Back to Row-Oriented Format

**Location:** `internal/engine/exec/sort.go:102-145`

When sort data exceeds memory and spills to disk, the operator falls back to `map[string]any` row format with `compareAny()` per comparison — losing all benefits of typed kernels and columnar layout.

```go
sort.SliceStable(rows, func(i, j int) bool {
    for _, key := range s.Keys {
        vi := rows[i][key.Column]  // map lookup per comparison
        vj := rows[j][key.Column]
        cmp := compareAny(vi, vj)  // interface comparison
```

The in-memory columnar path (lines 171-305) resolves typed kernels once and uses index-based sorting. The spill path doesn't.

**Fix:** Spill in columnar format (the join operator already does this with `writeColumnarBatch()`). Read spilled batches back as RecordBatches and merge-sort with typed kernels.

**Estimated impact:** 3-10x improvement on spilled sorts (queries exceeding memory budget with ORDER BY).

---

### 3. Aggregate Spill Uses ToRows() — Massive GC Pressure

**Location:** `internal/engine/exec/aggregate.go:323-331`

```go
rows := b.ToRows()  // creates map[string]any per row
if err := h.Spill.WriteRows(rows, h.outputSchema); err != nil { ... }
```

When aggregation spills, `ToRows()` converts columnar batches to row-oriented maps. For a 2M-group aggregation that spills, this creates 2M `map[string]any` objects.

**Fix:** Use columnar spill format (same as join's `writeColumnarBatch()`). The aggregate operator's SoA accumulators are already columnar — serialize them directly.

**Estimated impact:** 2-5x improvement on spilled aggregations.

---

### 4. No Page-Level Predicate Pruning in Parquet

**Location:** `internal/storage/parquet/reader.go:86-120`

Row-group statistics are aggregated from page-level data, but page-level predicates are never evaluated at read time. Entire pages are deserialized even when their min/max statistics prove all rows would be filtered.

The data is already available — `RowGroupStats()` reads `ci.MinValue(p)` and `ci.MaxValue(p)` per page — but discards page boundaries.

**Fix:** Add page-level filtering in the scan layer. When a predicate eliminates an entire page, skip deserialization. Parquet column indexes provide per-page min/max natively.

**Estimated impact:** 10-40% scan reduction on time-range queries with sorted data (common in security analytics: `WHERE timestamp > X`).

---

### 5. Full-File S3 Download Strategy

**Location:** `internal/planner/physical/util.go:454-461`

```go
rc, _, err := inner.cat.Store().Get(ctx, inner.cat.Bucket(), entry.Path)
data, err := readAll(rc)  // downloads entire file into memory
```

The comment explains the rationale: "a single S3 GET is far cheaper than N range reads per column page." This is true for wide projections but wasteful for narrow ones.

For `SELECT src_ip, dst_ip FROM flows WHERE timestamp > X` projecting 2 of 30 columns, 93% of downloaded bytes are discarded.

**Fix:** Add a cost heuristic: if `projected_columns / total_columns < 0.3`, use column-level range requests. Otherwise, download the full file. The Parquet footer provides byte offsets for each column chunk.

**Estimated impact:** 50-80% bandwidth reduction and 30-60% latency improvement on narrow-projection queries.

---

## P1: Medium-Impact Bottlenecks

### 6. Per-Row Type Dispatch in ColumnCompare Filter

**Location:** `internal/engine/exec/filter.go:109-155`

`ColumnCompare` executes a type switch for every row:

```go
switch v.Type {
    case batch.TypeInt64, batch.TypeTimestamp:
        return compareInt64(v.Int64Data[row], toInt64(value), op)
    // ... 15 more cases
}
```

The `KernelFilter` (lines 354-366) resolves the type once per batch and dispatches to a typed function. But `ColumnCompare` is still used in some code paths.

**Fix:** Replace remaining `ColumnCompare` usages with `KernelFilter` or add type caching to `ColumnCompare`.

**Estimated impact:** 2-5% on filter-heavy queries.

---

### 7. LIKE Pattern Matching: Exponential Backtracking

**Location:** `internal/engine/exec/filter.go:223-251`

`matchLikeRecur()` uses recursive backtracking without memoization. Patterns like `%a%b%c%` on long strings can cause exponential time complexity.

```go
for i := si; i <= len(s); i++ {
    if matchLikeRecur(s, pattern, i, pi) {
        return true  // tries every position
    }
}
```

**Fix:** Replace with NFA-based matcher or compile common patterns to direct string operations (prefix/suffix/contains). The `LikeFilter` at lines 454-503 partially does this but falls back to the recursive matcher.

**Estimated impact:** Prevents pathological slowdowns on LIKE queries; 2-5% on typical LIKE workloads.

---

### 8. Selection Vector Allocation in OFFSET/LIMIT

**Location:** `internal/engine/exec/limit.go:48-58, 73-85`

Both OFFSET skip and LIMIT truncation allocate new selection vectors per batch:

```go
sel := make([]uint32, 0, activeLen-int64(skip))
```

The Filter operator reuses a `selBuf` scratch buffer. Limit doesn't.

**Fix:** Add `selBuf []uint32` to Limit struct, reuse across batches.

**Estimated impact:** Minor allocation reduction; matters for high-batch-count paginated queries.

---

### 9. No Column-Level I/O Parallelism

**Location:** `internal/planner/physical/util.go:141-165`

Column pages within a row group are read sequentially:

```go
for _, rg := range activeRGs {
    pages := chunks[m.fileIdx].Pages()
    for {
        page, err := pages.ReadPage()  // sequential
```

For wide tables with many columns, parallelizing column reads within a row group would overlap I/O with deserialization.

**Fix:** Issue column page reads concurrently (up to a bounded parallelism limit), deserialize as they arrive.

**Estimated impact:** 15-30% improvement on wide-table scans (10+ projected columns).

---

### 10. Worker Shuffle Uses ToRows()

**Location:** `internal/worker/executor.go:672`

```go
rows := pb.ToRows()  // map[string]any per row
if err := partWriters[pid].WriteRows(rows); err != nil { ... }
```

Shuffle (redistributing data across partitions) converts columnar batches to row maps, then writes. This happens for every batch in every partition during distributed execution.

**Fix:** Write columnar batches directly to partition files. The spill infrastructure (`writeColumnarBatch()` in `join_spill.go`) already has the serialization code.

**Estimated impact:** 20-40% improvement on distributed shuffle-heavy queries (multi-stage joins).

---

## P2: Lower-Impact / Scalability Issues

### 11. Batch Pool Single-Mutex Contention

**Location:** `internal/engine/batch/pool.go:30`

All `Get()`/`Put()` operations on a pool contend on a single `sync.Mutex`. With 4+ concurrent pipeline workers, this becomes a serialization point.

**Fix:** Shard the pool by goroutine ID or use per-worker pools. Alternative: wait for Go arena allocator (noted in pool.go:9-14 as a planned optimization).

**Estimated impact:** 1-3% under high concurrency (8+ workers); negligible at default 4 workers.

---

### 12. Source.Next() Mutex in Parallel Pipelines

**Location:** `internal/engine/exec/pipeline.go:287-289`

```go
sourceMu.Lock()
b, err := p.Source.Next(workerCtx)
sourceMu.Unlock()
```

All parallel pipeline workers serialize on `sourceMu` for every batch fetch. If `Source.Next()` is slow (e.g., Parquet scan with decompression), workers idle waiting for the lock.

**Fix:** Use a batch-buffered channel instead of mutex-protected source. Source produces batches into a bounded channel; workers consume from it. This decouples source I/O from operator execution.

**Estimated impact:** 5-15% improvement on I/O-bound parallel pipelines.

---

### 13. Filter Schema Recomputed Per File

**Location:** `internal/engine/scan/scanner.go:297, 422-434`

`filterSchema()` builds a map and filters columns for every file read:

```go
selSet := make(map[string]bool, len(selected))
for _, s := range selected { selSet[s] = true }
```

The schema is static for the entire scan.

**Fix:** Compute filtered schema once in `Init()` and cache it.

**Estimated impact:** Minor; saves N map allocations where N = number of files scanned.

---

### 14. Row-Group Statistics Re-Aggregated Per Query

**Location:** `internal/storage/parquet/reader.go:66-126`

`RowGroupStats()` re-reads page-level statistics every time it's called. For queries with multiple predicates, the same row group's stats may be parsed multiple times.

**Fix:** Cache `ColumnStats` per row group after first computation.

**Estimated impact:** Minor CPU reduction on multi-predicate scan planning.

---

### 15. Aggregate Hash Table Pre-Sizing Without Cardinality Hints

**Location:** `internal/engine/exec/aggregate.go:450-461`

Without `InputRowHint`, the hash table starts at 4096 entries. TPC-H Q17 has 2M distinct keys, requiring 8+ resize-and-rehash cycles.

```go
htInitSize := 4096
if h.InputRowHint > int64(htInitSize)*8 {
    est := int(h.InputRowHint / 8)
```

The comment acknowledges this. The physical planner should provide cardinality estimates from Parquet metadata (distinct count statistics, row count / selectivity).

**Fix:** Propagate Parquet distinct-count statistics through the planner as `InputRowHint`.

**Estimated impact:** 5-10% on high-cardinality GROUP BY when hints are missing.

---

### 16. Context Check Overhead in Parallel Execution

**Location:** `internal/engine/exec/pipeline.go:278-285`

Parallel workers check `ctx.Err()` and all `DoneSignaler`s on every batch iteration. Serial execution only checks every 64 batches.

**Fix:** Sample every 32-64 batches in parallel mode too.

**Estimated impact:** 1-2% on high-throughput parallel pipelines.

---

## Architecture-Level Observations

### What's Already Well-Optimized

| Area | Technique | Location |
|------|-----------|----------|
| Hash join probe | Fully inlined, saves ~2.5GB allocs at SF1 | `join.go:1686-1791` |
| Aggregate accumulators | SoA layout, ~16MB vs ~192MB for 2M groups | `agg_scatter.go:8-11` |
| Integer hash table | Fibonacci hashing, 70% load, 16B entries | `int_hash.go` |
| String hash table | Arena-stored keys, hash tag fast-reject | `str_hash.go` |
| Bloom filter | Adaptive disable at <5% rejection | `bloom_filter_op.go:186-199` |
| Sort | Top-K heap, typed kernels, index-based | `sort.go:249-280` |
| Batch pooling | Size-class buckets, bitmap reuse | `batch/pool.go` |
| Group state | Chunk allocator: 1.5M→366 heap objects | `aggregate.go:150-170` |
| Semi/anti join | Key-only mode: 2-4x less memory | `join.go:85-89` |
| Null handling | NoNulls kernel variants skip bitmap checks | `kernel/agg.go:68-71` |

### Recurring Anti-Pattern: Row-Oriented Fallbacks

Three separate paths fall back to `map[string]any` row format:
1. **Sort spill** (`sort.go:102-145`)
2. **Aggregate spill** (`aggregate.go:323-331`)
3. **Worker shuffle** (`executor.go:672`)

All three could use the columnar serialization from `join_spill.go:347-541`. Unifying on a columnar spill format would:
- Eliminate the largest source of GC pressure under memory pressure
- Enable typed operations on spilled data (merge-sort with typed kernels)
- Reduce spill file sizes (columnar compresses better)

### Recommended Priority Order

| # | Bottleneck | Effort | Impact |
|---|-----------|--------|--------|
| 1 | Columnar spill format (sort + agg + shuffle) | Medium | High: eliminates row-format fallback across 3 paths |
| 2 | Column-selective S3 range requests | Medium | High: 50-80% bandwidth reduction on narrow queries |
| 3 | Page-level predicate pruning | Medium | High: 10-40% scan reduction on sorted data |
| 4 | Distinct hash-based dedup | Small | Medium: 15-30% on DISTINCT queries |
| 5 | Column-level I/O parallelism | Medium | Medium: 15-30% on wide scans |
| 6 | Source→channel decoupling in parallel pipelines | Small | Medium: 5-15% on I/O-bound parallel execution |
| 7 | Replace ColumnCompare with KernelFilter | Small | Low-Medium: 2-5% on filter-heavy queries |
| 8 | NFA-based LIKE matching | Small | Low: prevents pathological cases |
| 9 | Aggregate cardinality hints from Parquet stats | Small | Low-Medium: 5-10% on high-cardinality GROUP BY |
| 10 | Batch pool sharding | Small | Low: 1-3% at high concurrency |
