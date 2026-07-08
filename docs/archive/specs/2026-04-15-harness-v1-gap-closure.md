# Harness V1 Gap Closure — Design

**Status:** Draft
**Author:** Derek Wright
**Date:** 2026-04-15
**Parent:** [Distributed Test Harness Design](2026-04-08-distributed-test-harness-design.md)

## Context

The harness v1 (`harness/v1` branch, PR #35) is ~85% complete. Core orchestration, measurement, baseline comparison, and reporting all work. Three gaps remain before merge:

1. **Goroutine dump on hang** — hang detection fires but produces no debugging output
2. **Micro-benchmarks are stubs** — `micro_reverse_bloom` is a no-op; `MicroGraceHashJoin` and `MicroHashAggHighCardinality` don't exist
3. **Micro data seeding** — micros need synthetic tables in the catalog

## Gap 1: Goroutine dump on hang

### Problem

When a query hangs (hard timeout or monotonic goroutine growth), the harness marks `Hung: true` and moves on. There's no goroutine dump to diagnose *why* it hung. The original spec called for SIGQUIT, but workers already trap SIGQUIT for graceful drain — sending it would trigger drain, not a dump.

### Approach: pprof endpoint

Hit `GET /debug/pprof/goroutine?debug=2` on each process's HTTP port. No signal, no process death, cluster stays alive for the next query.

### Prerequisites

Neither the coordinator's HTTP server (`internal/server/server.go`) nor the worker's metrics server (`cmd/wadjet/main.go:1137`) registers pprof handlers. We need to add `net/http/pprof` registration to both.

### Changes

**`internal/server/server.go`** — register pprof routes on the existing mux:
```go
import _ "net/http/pprof"
// In server setup:
s.mux.Handle("/debug/pprof/", http.DefaultServeMux)
```

**`cmd/wadjet/main.go`** (worker metrics server) — register pprof on the worker's metrics mux:
```go
import "net/http/pprof"
// After metricsMux creation:
metricsMux.HandleFunc("/debug/pprof/", pprof.Index)
metricsMux.HandleFunc("/debug/pprof/goroutine", pprof.Handler("goroutine").ServeHTTP)
```

**`internal/harness/cluster.go`**:
- Workers currently don't pass `--metrics-addr`, so all default to `:9100` and conflict. Fix: allocate a random free port per worker and pass `--metrics-addr=:<port>`. Track in `managedProcess.debugPort`.
- Add `DebugPorts() map[string]int` method returning `{"coord": httpPort, "worker-0": metricsPort0, ...}`.

**`internal/harness/harness.go`**:
- New function `captureGoroutineDumps(cluster *Cluster, query string, runDir string) string`:
  - For each port in `cluster.DebugPorts()`, GET `/debug/pprof/goroutine?debug=2` with 5s timeout
  - Write response to `<runDir>/logs/hang-<query>-<role>.txt`
  - Return the directory path (set on `m.HangDumpPath`)
- Call it in `runOneQuery` when the query context times out or hang detector fires

### Behavior

```
Query Q03 timeout after 50s (10x baseline projection)
  → cancel pgx query context
  → GET coordinator:8080/debug/pprof/goroutine?debug=2 → hang-q03-coord.txt
  → GET worker-0:9100/debug/pprof/goroutine?debug=2   → hang-q03-worker-0.txt
  → GET worker-1:9101/debug/pprof/goroutine?debug=2   → hang-q03-worker-1.txt
  → m.Hung = true, m.HangDumpPath = "<runDir>/logs/"
  → continue to Q04
```

If a pprof request itself times out (process is truly stuck, not just slow), log the failure and continue — a missing dump is acceptable, a harness hang is not.

## Gap 2: Real micro-benchmarks

### Approach

Scale micros to the local cluster's resources rather than SF100 volumes. The goal is exercising code paths (spill, grace hash partitioning, high-cardinality aggregation), not reproducing production data volumes. With a 4GB GOMEMLIMIT, even modest data sizes trigger spill when operator budgets are tight.

### Data seeding

All micro tables are seeded in `loadSampleData` alongside TPC-H tables. Five synthetic tables (serving three micros):

| Table | Schema | Rows | Purpose |
|---|---|---|---|
| `micro_lineitem` | 3 cols: `l_orderkey INT64, l_partkey INT64, l_quantity FLOAT64` | 200,000 | Build side for reverse bloom micro |
| `micro_orders` | 2 cols: `o_orderkey INT64, o_totalprice FLOAT64` | 20,000 | Probe side for reverse bloom micro |
| `micro_build` | 3 cols: `build_key INT64, build_val INT64, build_pad STRING` | 500,000 | Build side for grace hash join micro (high cardinality keys, padded to inflate memory) |
| `micro_probe` | 2 cols: `probe_key INT64, probe_val INT64` | 50,000 | Probe side for grace hash join |
| `micro_agg` | 2 cols: `group_key STRING, value INT64` | 200,000 (100,000 distinct keys) | High-cardinality aggregation micro |

Tables are generated deterministically (seeded PRNG) and written as a single parquet chunk each. Total added data: <50MB, <1s generation time.

The synthetic schemas are defined in `internal/harness/micros.go` (not in `benchmarks/tpch/`) since they're harness-specific, not TPC-H tables.

### `MicroReverseBloom`

**Goal:** Force the `reverseBloomBridge` into its spill path and verify spill files are created.

**Query:**
```sql
SELECT o.o_orderkey, SUM(l.l_quantity)
FROM micro_lineitem l
JOIN micro_orders o ON l.l_orderkey = o.o_orderkey
GROUP BY o.o_orderkey
```

The build side (micro_lineitem, 200K rows) is 10x larger than the probe side (micro_orders, 20K rows). This matches the reverseBloomBridge's trigger condition: inner/build side larger than outer.

**Assertions:**
- Query succeeds (no hang, no error)
- `row_count > 0`
- `spill_bytes > 0` (the bridge spilled)

### `MicroGraceHashJoin`

**Goal:** Force grace hash join partitioning by exceeding the per-operator memory budget on the build side.

**Query:**
```sql
SELECT b.build_key, b.build_val, p.probe_val
FROM micro_build b
JOIN micro_probe p ON b.build_key = p.probe_key
```

The build table (500K rows with padded strings) is sized to exceed the operator's memory partition threshold at the configured GOMEMLIMIT, forcing grace hash join to partition and spill.

**Assertions:**
- Query succeeds
- `row_count > 0`
- `spill_bytes > 0` (grace hash partitions spilled)

### `MicroHashAggHighCardinality`

**Goal:** Verify that high-cardinality GROUP BY doesn't leak allocations (the `groupState` issue from the GC audit).

**Query:**
```sql
SELECT group_key, COUNT(*), SUM(value)
FROM micro_agg
GROUP BY group_key
```

100,000 distinct group keys with 200,000 total rows (2 rows per group on average).

**Assertions:**
- Query succeeds
- `row_count == 100000` (one row per distinct key)
- `alloc_count < 4 * 100000` (allocation discipline — no per-row allocation leak)

### Micro execution flow

Each `RunMicro*` function:
1. Calls `collector.StartWindow(name)`
2. Opens pgx connection to coordinator
3. Executes query with 2-minute timeout
4. Streams rows, computes row count + checksum
5. Calls `collector.EndWindow(name)` to capture heap/alloc/spill metrics
6. Runs assertions; returns error if any fail

Micros are NOT subject to baseline projection — they assert against absolute thresholds because their inputs are synthetic and deterministic.

### Changes

**`internal/harness/micros.go`**:
- Define micro table schemas and `generateMicroData() map[string]microTable` (deterministic PRNG)
- Replace `RunMicroReverseBloom` stub with real implementation
- Add `RunMicroGraceHashJoin` and `RunMicroHashAggHighCardinality`

**`internal/harness/harness.go`**:
- `loadSampleData`: call `generateMicroData()`, write parquet files and register in catalog alongside TPC-H tables
- `runOneQuery`: dispatch to the correct micro function by name

**`internal/harness/suite.go`**:
- Add `micro_grace_hash_join` and `micro_hash_agg_high_card` to the default query list in `SelectQueries`

## Testing

### Unit tests

- `micros_test.go`: test `generateMicroData` produces expected row counts and deterministic output
- `cluster_test.go`: test `DebugPorts()` returns correct port mapping

### Existing tests

All existing 24 tests continue to pass unchanged. The goroutine dump and micro changes are additive.

### Manual verification

- `./tpch-harness --mode=local --slice=small --queries=micro_reverse_bloom --no-compare` — verify spill assertion passes
- `./tpch-harness --mode=local --slice=small --no-compare` — full suite including all 3 micros

## What this does NOT change

- Baseline file remains empty (populated from future golden run)
- TPC-H query execution unchanged
- No changes to the engine itself (except pprof registration)
- No new dependencies
