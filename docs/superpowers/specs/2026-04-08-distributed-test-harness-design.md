# Distributed Test Harness — Design

**Status:** Draft (pending user review)
**Author:** Derek Wright
**Date:** 2026-04-08
**Sub-project:** SF100 scalability work, item #1 of 6

## Context and motivation

Wadjet's SF100 distributed work has been blocked by a recurring failure pattern: a fix passes the in-process `TestDistributedTPCHBuildCacheSF100Sample` test locally, gets committed, and then breaks in the real EC2 deploy. The 2026-04-08 `reverseBloomBridge` streaming-spill rewrite is the canonical example — local 1-process MemStore tests said "Q05 fixed, 3.5× peak heap reduction, bit-identical results," and the deploy hung Q03 for 30 minutes until query timeout.

The local test lied because:

1. It runs in **one Go process** holding all workers, so multi-process scheduling and per-process memory isolation are not exercised
2. It uses **`objstore.MemStore`** instead of real disk-backed object storage, so I/O timing and the spill path's interaction with real storage are absent
3. It runs against a **small data slice** (1/15 of SF100), which keeps the overall workload small enough that lock contention and resource pressure don't manifest at the levels they reach in production
4. It has **no behavioral assertions beyond final correctness** — a 30-minute hang in deploy looks identical to a "still running" state from outside

The result has been a session-cost loop: local fix → deploy regression → revert. Per `feedback_local_repro_lies.md`, the user has explicitly flagged this as not making meaningful progress.

This sub-project builds the regression gate that prevents that loop. It is a prerequisite for every other sub-project in the SF100 work that touches bridge, spill, or memory paths.

## Goals

1. **A trustworthy fast-iteration gate** that runs on the dev box in minutes, not hours, and that catches the class of bugs the in-process sample test missed
2. **A clear separation between "engine regressed" and "harness broke"** so false alarms don't train us to ignore real ones
3. **A reusable golden source of truth** captured from real EC2 deploys, refreshable from a single golden run, that the local gate calibrates against
4. **Detection of behavior, not just performance** — hangs, correctness divergence, missing spill, allocation pressure, not just wall time

## Non-goals

- Replacing the EC2 deploy as the source of truth. The harness *uses* the deploy via `--mode=golden`; it does not eliminate it.
- Cross-platform support. Linux only.
- Running multiple harness invocations in parallel on the same dev box.
- Graceful handling of disk-full or NATS-network-failure conditions (these manifest as cluster crash → exit 2).
- Validating the engine's correctness or performance — the harness *measures*, the engine is the system under test.

## Architecture

A single Go binary (`cmd/tpch-harness`) with two modes — `local` (orchestrates a multi-process cluster on the dev box) and `golden` (drives a pre-existing EC2 cluster). Both modes emit a structured JSON measurement file in the same schema. A committed baseline file (captured from `golden`) is the source of truth for what "no regression" means; `local` runs project their measurements onto that baseline using per-metric scaling factors and fail when drift exceeds tolerance.

```
cmd/tpch-harness/         — main binary; flag parsing, mode dispatch, exit codes
  main.go
internal/harness/         — orchestration logic, importable, unit-testable
  harness.go              — top-level orchestrator
  cluster.go              — process supervisor for coordinator + N workers (local mode)
  measure.go              — heap/alloc/goroutine samplers, spill-bytes accounting
  baseline.go             — load/save/compare calibration JSON
  micros.go               — synthetic micro-queries (bridge, grace-join, hash-agg)
  suite.go                — TPC-H query selection + slice configs
benchmarks/tpch/baseline-sf100.json    — committed golden baseline
```

The current test helpers in `internal/coordinator/distributed_tpch_test.go` (NATS embed, catalog setup, store loading at lines 538-604) are extracted into `internal/harness/` so there is exactly one implementation shared between the harness and the existing tests.

### Two run modes, one code path

| Mode | Cluster | Storage | Data | Output |
|---|---|---|---|---|
| `local` | Multi-process: spawns real `wadjet` worker binaries via `os/exec`, 1 coordinator + 2 workers, embedded NATS in the harness process, real local NVMe spill, GOMEMLIMIT enforced via env | `FileStore` rooted at `/tmp/wadjet-harness/run-X/data`. MinIO is intentionally NOT used; FileStore is sufficient because the harness's job is to exercise spill and contention paths, not to test the S3 client. | Slice of SF100 sample, pre-downloaded to `/tmp/sf100-sample` | Writes `result.json`, compares against `baseline-sf100.json`, exits non-zero on regression |
| `golden` | Connects to a pre-existing cluster via `--coord-url`. Does NOT orchestrate. Intended to run on the EC2 deploy under `tofu apply`. | Real S3 (`wadjet-bench-sf100-use2`) | Full SF100 | Writes `golden-result.json`, intended to be copied back as the new `baseline-sf100.json` |

The `local`/`golden` asymmetry is intentional: on EC2 the cluster is already running via terraform, so the harness is purely a *driver*. On the dev box the cluster doesn't exist yet, so the harness must also be an *orchestrator*. Conflating the two roles into "always orchestrates" would force the harness to reinvent terraform; conflating them as "never orchestrates" would require the dev box user to manually start a cluster every iteration.

## Components

### `cmd/tpch-harness/main.go`

Thin shim. Flag parsing, dispatches to `internal/harness`.

```
--mode=local|golden        (required)
--slice=small|large        (local only; default small)
--coord-url=nats://...     (golden only; required in that mode)
--data-dir=/tmp/sf100-sample   (local only; default; auto-skip if missing)
--baseline=path              (default: benchmarks/tpch/baseline-sf100.json)
--out=path                   (default: ./harness-result.json)
--queries=q01,q05,...        (default: all 22 + micros)
--update-baseline            (golden only; writes result directly to --baseline path)
--no-compare                 (skip regression check, just emit measurements)
```

Exit codes:
- `0` — pass
- `1` — performance regression OR missing spill paths OR hang
- `2` — setup or cluster error (binary missing, port in use, worker crashed mid-run, panic in harness)
- `3` — correctness failure (row count or checksum diverged)

Exit code 3 outranks 1 outranks 0; the highest-numbered failure across all queries determines the run's exit code.

### `internal/harness/cluster.go`

Process supervisor used only in `local` mode.

Responsibilities:
- Locate the `wadjet` binary (env `WADJET_BIN` or `go build -o /tmp/wadjet ./cmd/wadjet`)
- Start an embedded NATS in the harness process (workers are separate processes, NATS is shared infra)
- Spawn 1 coordinator + N (default 2) worker processes via `os/exec` with:
  - `GOMEMLIMIT` set per slice config
  - `WADJET_SPILL_DIR=/tmp/wadjet-harness/run-X/spill/<role>-<pid>`
  - Stdout/stderr piped to per-process log files
  - Process group set so a single `kill -TERM -<pgid>` cleans up cleanly
- Health-check at startup: poll until each worker registers with the coordinator (timeout 30s, exit 2 on failure)
- **Continuous health monitoring**: a goroutine `os.Process.Wait`s on each child; if any returns before the run is done, the harness immediately stops submitting queries, captures logs, and shuts down
- On any exit path (success, failure, panic, SIGINT): `defer cluster.Shutdown()` runs `Shutdown()`, which is idempotent, sends SIGTERM, waits 5s, sends SIGKILL, then verifies via `pgrep -f wadjet` that no orphaned processes remain
- On clean PASS: `os.RemoveAll` the spill dirs and the run dir
- On any non-PASS: preserve the run dir with a `MANIFEST.txt` listing what each artifact is

### `internal/harness/measure.go`

Captures the seven required signals: wall time (a), peak heap (b), allocation count + bytes (c), spill bytes written/read (d), row count + checksum (e), hang detection (g), and a lightweight goroutine-count-over-time signal (light h).

Two collection points:

1. **In-cluster sampling (b, c, light h)** — Coordinator and worker processes already publish heartbeats containing `RSS`, `NumGoroutines`, `SpillDiskUsed` (per commit `25b6782`). The harness subscribes to the heartbeat NATS subject and time-series-records every heartbeat to `measurements/<role>-<pid>.jsonl`. The heartbeat payload is extended with `HeapAlloc` and `Mallocs` (small change to `worker/worker.go` and `coordinator/coordinator.go`).

2. **Per-task peak tracking (b — exact)** — The 1Hz heartbeat is too coarse for queries under 5s. Workers track their own peak heap with an atomic max, reset per task, and report the peak on `ResultNotification`. ~10 lines added to `worker/executor.go`. The heartbeat sampler is for time-series shape; the per-task report is for the exact peak number compared against the baseline.

3. **Per-query result wrapping (a, d, e, g)** — The harness submits each query, times it end-to-end, captures row count + columnar checksum, and reads spill bytes from the post-query stats already published on `ResultNotification`.

**Hang detection (g)** has two triggers:
- **Hard timeout**: query elapsed > 10× baseline projection → SIGQUIT all workers, capture goroutine dumps to `hang-<query>.txt`, mark query `Hung: true`, continue to next query
- **Goroutine monotonic growth**: light h. Sample goroutine count every 1s; if count grows monotonically for >30s with no result, trigger the same SIGQUIT dump

**Continuing past hangs is a deliberate choice.** Aborting the run on the first hang would have prevented us from learning "Q03 hangs AND Q05 still works" vs "Q03 hangs AND Q05 also slow" — those are very different diagnoses. Worst-case wall time is `10× baseline-projection × #hangs`, which is acceptable.

### `internal/harness/baseline.go`

Calibration table format:

```json
{
  "version": 1,
  "captured_at": "2026-04-08T12:00:00Z",
  "captured_on": "EC2 c7g.2xlarge coord + 3x c7gd.4xlarge workers, sf100",  // example value
  "queries": {
    "q05": {
      "wall_ms_p50": 118000,
      "wall_ms_tolerance_pct": 25,
      "peak_heap_mb": 14336,
      "peak_heap_tolerance_pct": 15,
      "allocs_bytes": 47000000000,
      "allocs_tolerance_pct": 20,
      "spill_bytes_written": 8500000000,
      "spill_tolerance_pct": 30,
      "row_count": 5,
      "row_checksum": "abc123..."
    }
  },
  "projection_factors": {
    "small_slice": {
      "wall_ms_multiplier": 0.04,
      "heap_multiplier": 0.18,
      "allocs_multiplier": 0.16,
      "spill_multiplier": 0.10
    },
    "large_slice": {
      "wall_ms_multiplier": 0.20,
      "heap_multiplier": 0.55,
      "allocs_multiplier": 0.50,
      "spill_multiplier": 0.45
    }
  }
}
```

The `projection_factors` are how local maps to golden. When `local --slice=large` measures `q05.wall_ms = 24000`, the harness divides by `0.20` to project a golden value of `120000`, then compares to `q05.wall_ms_p50 ± wall_ms_tolerance_pct`. Tolerances are wide enough to absorb machine variance but tight enough to catch a 2× regression.

Per-metric (not per-query) projection factors are sufficient at this stage. Per-query factors would be over-engineering and would require many more golden runs to characterize.

### `internal/harness/micros.go`

Synthetic micro-queries that target individual operator paths with controlled inputs and tight assertions. Catch root causes directly so a TPC-H regression points at exactly which operator is responsible.

- **`MicroReverseBloom`** — 50M-row "lineitem" + 5M-row "orders", forces `ReverseBloomInnerThreshold=1`, runs the join, asserts spill file exists and total spill bytes are >0 and <2GB
- **`MicroGraceHashJoin`** — 100M-row build × 1M-row probe, forces grace hash join via memory budget, asserts at least 4 of 64 partitions spilled
- **`MicroHashAggHighCardinality`** — 50M groups, asserts allocation count < 4× the group count (catches the `groupState` leak identified in the GC audit)

Each micro completes in <30s and runs as part of the suite. Micros are NOT subject to projection factor scaling — they assert against absolute, hand-set thresholds because their inputs are also hand-set.

### `internal/harness/suite.go`

Maps `--queries=...` to TPC-H query SQLs (extracted from `benchmarks/tpch/queries.go`) plus the micros. Slice configs:

```go
var SliceConfigs = map[string]SliceConfig{
    "small": {LineitemFiles: 4,  OrdersFiles: 1, GoMemLimit: 4 * GB, ExpectSpill: false},
    "large": {LineitemFiles: 12, OrdersFiles: 3, GoMemLimit: 8 * GB, ExpectSpill: true},
}
```

`ExpectSpill: true` is asserted at the end of the run. If the slice expects spill and total spill bytes across all queries is zero, the run fails with "spill paths not exercised." The whole point of the large slice is to fire spill paths; silent skipping is treated as a setup error.

The 8 GB GOMEMLIMIT for the large slice is sized for the 32 GB WSL dev box: 8 × 2 workers (16 GB) + coordinator (~2 GB) + harness (~1 GB) + ~4 GB OS headroom = ~23 GB, leaving ~9 GB margin so the OS never reaches swap. The large-slice data volume (12 lineitem files + 3 orders files) is sized to force grace hash join and reverseBloomBridge to spill at this budget.

## Data flow

```
1. Setup
   ├─ Parse flags → harness.Config
   ├─ Verify --data-dir exists, list expected sample files, fail fast if missing
   ├─ Verify wadjet binary exists (or build it)
   ├─ Create run dir: /tmp/wadjet-harness/run-<timestamp>/
   │     ├─ logs/{coord,worker-0,worker-1}.log
   │     ├─ measurements/{coord,worker-0,worker-1}.jsonl
   │     ├─ spill/<role>-<pid>/...
   │     └─ result.json (final output)
   ├─ Pre-flight resource checks (free RAM, free disk, no orphaned wadjet, ports free)
   └─ Load baseline-sf100.json into memory

2. Cluster start (local mode only)
   ├─ Embed NATS in the harness process
   ├─ Spawn coordinator process with GOMEMLIMIT, WADJET_SPILL_DIR, log redirect
   ├─ Spawn 2 worker processes (same pattern)
   ├─ Subscribe to heartbeat subject; begin recording per-process .jsonl
   ├─ Health check: poll until both workers register with coordinator (timeout 30s)
   └─ Setup catalog: load each TPC-H table from sample dir into the FileStore catalog

3. Per-query loop (for each query in suite + micros)
   ├─ Reset measurement window: mark t0, snapshot baseline heap from heartbeats
   ├─ Submit query via coordinator's pgwire (or NATS request)
   ├─ While query runs:
   │     ├─ Heartbeats stream into measurements/ at 1/s
   │     ├─ Hang detector: monotonic goroutine growth >30s → SIGQUIT
   │     └─ Hard timeout: elapsed > 10× baseline projection → SIGQUIT
   ├─ Collect result: row count, checksum, post-query stats (incl. exact peak heap)
   ├─ Build QueryMeasurement{wall_ms, peak_heap_mb, allocs_bytes, spill_bytes,
   │                          row_count, checksum, goroutine_peak, hung, hang_dump}
   └─ Append to results slice

4. Comparison
   ├─ For each query: project local→golden via projection_factors
   ├─ For each metric: check projection within baseline ± tolerance
   ├─ Build PerQueryDelta{query, metric, baseline, projected, drift_pct, status}
   ├─ Aggregate into RunResult{queries, passed, regressions, hangs}
   └─ If --slice=large and total spill bytes == 0 → fail with "spill paths not exercised"

5. Output
   ├─ Write result.json (machine-readable)
   ├─ Print human-readable summary table to stdout
   └─ Exit code 0 / 1 / 2 / 3

6. Cleanup (deferred from step 2; runs on every exit path)
   ├─ SIGTERM all child processes; wait 5s; SIGKILL stragglers
   ├─ Verify no orphaned wadjet processes (pgrep) → fail loudly if found
   ├─ os.RemoveAll spill dirs (only on clean PASS)
   └─ Run dir preserved if regressions or hangs occurred, deleted on clean PASS
```

### Key data shapes

```go
type QueryMeasurement struct {
    Query         string
    WallMs        int64
    PeakHeapMB    int64
    AllocsBytes   int64
    SpillBytes    int64
    RowCount      int64
    RowChecksum   string
    GoroutinePeak int
    Hung          bool
    HangDumpPath  string  // populated if Hung
}

type RunResult struct {
    Mode         string  // "local" or "golden"
    Slice        string  // "small" / "large" / ""
    StartedAt    time.Time
    DurationMs   int64
    Queries      []QueryMeasurement
    BaselinePath string
    Regressions  []QueryDelta
    Hangs        []string
    Passed       bool
}
```

## Error handling

### Failure taxonomy

| Class | Cause | Exit | Action |
|---|---|---|---|
| **Setup** | sample data missing, binary missing, port in use, NATS won't start, worker won't register, FileStore unwritable | 2 | Fail fast before running any query, print remediation hint |
| **Cluster crash** | A worker process exits non-zero mid-run | 2 | Capture last 200 lines of that worker's log, dump to result.json, mark whole run as failed-setup (NOT regression) |
| **Hang** | Per-query hard timeout OR goroutine monotonic-growth detector | 1 | SIGQUIT to dump goroutines, capture to `hang-<query>.txt`, mark query `Hung: true`, continue to next query |
| **Correctness divergence** | row count mismatch OR checksum mismatch | 3 | Captured per-query, run continues; whole run exits 3 (3 outranks 1) |
| **Performance regression** | metric projection drift > tolerance | 1 | Run completes, exit 1, print drift table |
| **Spill paths not exercised** | `--slice=large` and total spill bytes == 0 | 1 | Same as regression |
| **Internal harness bug** | nil pointer in measurement code, sampler deadlock, etc | 2 | Recover panic, log stack, dump partial results, exit 2 |

### Cleanup discipline

- A single `defer cluster.Shutdown()` in `main` runs on every exit path
- `Shutdown()` is **idempotent**
- After shutdown, the harness runs `pgrep -f wadjet` and **fails loudly if any wadjet process is still alive** — this catches bugs in our own shutdown logic
- Spill dirs are removed only on clean PASS; non-PASS preserves them with a `MANIFEST.txt`
- A `task harness:clean` command nukes `/tmp/wadjet-harness/` between sessions

### Resource pre-checks (setup failures, exit 2)

- Free RAM ≥ `(GoMemLimit × workerCount) + 3 GB coordinator/harness + 4 GB OS headroom`
- Free disk ≥ 20 GB at the spill dir mount point
- No existing wadjet processes (`pgrep -f wadjet` clean)
- Specific TCP ports free (NATS embed handles its own; coordinator pgwire port chosen randomly from free pool)
- A PID lockfile at `/tmp/wadjet-harness/.lock` prevents parallel harness runs on the same box

For the 32 GB WSL dev box with `--slice=large`: `(8 × 2) + 3 + 4 = 23 GB` required free. The check is intentionally strict — if the box doesn't have it, fail at step 1 with a precise "free X GB before re-running" message rather than silently swap-thrashing into a useless measurement.

### Explicitly not handled

- **Disk full mid-run**: spill writes fail, worker crashes, caught as cluster crash (exit 2). No graceful degradation. Documented as known limitation.
- **NATS packet loss**: NATS is in-process locally, this can't happen. Real NATS in `golden` mode has different failure modes documented separately.
- **Heartbeat lag spikes** that cause the heap sampler to undercount: solved by per-task in-worker peak tracking. The sampler is for time-series shape, not for the peak number.
- **Multiple harness invocations in parallel** on the same dev box: unsupported. PID lockfile refuses to start.

## Testing the harness itself

Three layers, in increasing fidelity and cost.

### Layer 1: Unit tests for pure-logic pieces

Standard `_test.go` files in `internal/harness/`. Run in CI on every push. No cluster needed.

- `baseline_test.go` — round-trip JSON encode/decode, projection math, tolerance comparison logic, malformed baseline handling
- `measure_test.go` — peak heap aggregation from a fake heartbeat stream, allocation delta computation, hang detector state machine fed synthetic goroutine series
- `suite_test.go` — slice config validation, query selection, micro-query SQL generation

Cheap, fast, deterministic. Catches off-by-one bugs in projection math, JSON schema mismatches, and other dumb errors that would silently corrupt every result.

### Layer 2: Smoke tests with a fake cluster

A test that runs the *full harness pipeline* against a fake cluster: a pair of in-process goroutines that pretend to be coordinator + worker, accept queries, stream synthetic heartbeats, return canned results, and can be configured to misbehave (hang, crash, return wrong row count).

- `TestRunsCleanThroughFakeCluster` — happy path, exit 0
- `TestDetectsHangViaTimeout` — fake cluster never replies to query 3, harness marks `Hung: true`, exit 1
- `TestDetectsCorrectnessRegression` — fake returns wrong row count, exit 3
- `TestDetectsPerfRegression` — fake reports doubled wall_ms, exit 1
- `TestDetectsClusterCrash` — fake goroutine returns mid-run, exit 2 with crash details
- `TestCleanupOnPanic` — induce a panic in the measurement loop, assert spill dirs removed and no orphaned goroutines

This layer doubles as documentation: each smoke test is an *example* of one failure path.

### Layer 3: Real cluster smoke at SF0.01

Once a session before trusting `--slice=large`, run `tpch-harness --slice=tiny` against the existing SF0.01 TPC-H dataset. Real cluster, real spawning, real spill, but tiny data — a 30-second sanity check that orchestration end-to-end is healthy. Wired as `task harness:smoke`.

### What we explicitly do NOT test

- **Whether the baseline numbers are right** — captured from `golden`, that's ground truth by construction
- **The wadjet engine's correctness or performance** — that's the harness's *output*, not its responsibility
- **Cross-platform behavior** — Linux only, fail with clear error if `runtime.GOOS != "linux"`

## Taskfile entries

The harness is invoked via the project Taskfile:

```yaml
tasks:
  harness:smoke:    # SF0.01 real-cluster smoke, ~30s, run before larger slices
  harness:local:    # --mode=local --slice=small, the default fast gate
  harness:large:    # --mode=local --slice=large, pre-merge gate for bridge/spill changes
  harness:golden:   # --mode=golden, run on EC2 deploy, requires --coord-url
  harness:clean:    # nuke /tmp/wadjet-harness/ entirely
```

These thin Taskfile entries exist so contributors don't have to remember flag combinations and so CI can invoke the same names humans use.

## Calibration workflow

Manual for now. `task harness:refresh-baseline` automation is a future improvement.

```
1. Engineer wants to refresh the baseline (e.g., after a major perf merge)
2. tofu apply -var-file=sf100.tfvars   (deploys EC2 cluster)
3. ssh to coord, run: tpch-harness --mode=golden --update-baseline=./new-baseline.json
4. scp new-baseline.json back to dev box
5. On dev box: tpch-harness --mode=local --slice=large --baseline=./new-baseline.json
6. Confirm the local-large run still passes against the new baseline
   (proves projection_factors are still valid after whatever changed)
7. If yes: replace benchmarks/tpch/baseline-sf100.json with new-baseline.json, commit
8. tofu destroy
```

Step 6 is the critical one: **a baseline refresh is only valid if a local-large run reproduces the SF100 numbers via the projection factors.** If projection factors have drifted (e.g., because a code change shifted the local↔SF100 ratio), the local gate stops being trustworthy and step 6 fails. That's a forcing function: any code change that breaks projection factors must come with a same-session baseline refresh.

## Open questions

None at draft time. The user has reviewed and approved Sections 1–5 inline during brainstorming.

## Risks

1. **Projection factor drift** — code changes that affect local-vs-golden ratio asymmetrically (e.g., scan-side optimizations that benefit large data more than small data) will silently invalidate the projection. Mitigation: the calibration workflow forces step 6 validation, and the periodic golden refresh catches drift.
2. **WSL2 disk performance variance** — the dev box's NVMe-via-WSL2 is slower than bare metal, which could cause `--slice=large` wall times to drift unpredictably. Mitigation: the projection factors are calibrated against this dev box specifically, and tolerances are wide (±25%).
3. **Process supervision flakiness** — `os/exec` + signal handling is the part most likely to be buggy. Mitigation: heavy unit testing in Layer 1, smoke testing in Layer 2, and the `pgrep` post-shutdown check.
4. **Sample data drift** — if `/tmp/sf100-sample` files are regenerated and differ from the baseline, results will diverge. Mitigation: the harness records a SHA256 of the loaded sample files and asserts it matches a hash recorded in the baseline.

## Sequencing

This is sub-project #1 of 6 in the SF100 scalability work. Order:

1. **Distributed test harness** ← this spec
2. Tracker visibility for bridges (`TrackBatch` in `BatchSink`/`CollectSink`)
3. GC pools: `groupState` pool + parallel-join sel-vector pre-sizing
4. Per-row-group memory gate in scan
5. Operational hardening: spill sweep on boot, `spillDir` assert
6. `reverseBloomBridge` / `deferredJoinBridge` spill fix (the Q05 unlock)

Items 2–6 all depend on this harness existing. The bridge fix (#6) is sequenced last because it is the highest-risk change and benefits most from having the rest of the engine stabilized first.
