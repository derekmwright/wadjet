# Distributed Test Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `cmd/tpch-harness` — a single Go binary that runs the TPC-H regression suite in two modes (`local` orchestrates a multi-process cluster on the dev box, `golden` drives an existing EC2 cluster), captures structured measurements, and compares against a calibrated baseline to detect hangs, correctness divergence, missing spill paths, and performance regressions.

**Architecture:** Standalone binary with logic in `internal/harness/`. The current in-process `TestDistributedTPCHBuildCacheSF100Sample` test helpers (NATS embed, catalog setup, store loading) are extracted into the harness package so there is one implementation. Local mode spawns real `wadjet` worker processes via `os/exec` against a `FileStore` rooted at `/tmp/wadjet-harness/`, with embedded NATS in the harness process and real NVMe spill. Both modes emit identical JSON; a committed baseline file (captured from `golden`) is the source of truth.

**Tech Stack:** Go 1.23+, embedded NATS (existing `internal/distributed`), `os/exec` for process supervision, existing `internal/coordinator` and `internal/worker` packages, existing TPC-H query suite from `benchmarks/tpch/queries.go`.

**Spec:** `docs/superpowers/specs/2026-04-08-distributed-test-harness-design.md`

**Scope for v1 (this plan):** Working harness that runs end-to-end against a real local cluster with real spill, captures all seven signals, compares against baseline, and exits with the right code. Includes Layer 1 unit tests in full and one Layer 2 happy-path self-test. Includes one micro (`MicroReverseBloom`).

**Deferred to v2 (separate plan):** Comprehensive Layer 2 failure-mode self-tests, additional micros (grace join, hash agg), `--mode=golden` polishing, automated baseline refresh task, MinIO mode.

---

## File Structure

**New files:**
- `internal/harness/doc.go` — package documentation
- `internal/harness/types.go` — `QueryMeasurement`, `RunResult`, `QueryDelta`, `Config`, `Mode`, exit codes
- `internal/harness/baseline.go` — `BaselineFile`, `QueryBaseline`, `ProjectionFactors`, load/save/project/compare
- `internal/harness/baseline_test.go` — unit tests for baseline
- `internal/harness/measure.go` — `MeasurementCollector` (heartbeat subscriber, time-series buffer, per-query window), `HangDetector`
- `internal/harness/measure_test.go` — unit tests for collector and hang detector
- `internal/harness/cluster.go` — `Cluster` (process supervisor, spawn coord+workers, health check, idempotent shutdown, pgrep verification)
- `internal/harness/cluster_test.go` — integration tests for cluster lifecycle
- `internal/harness/preflight.go` — resource pre-checks (free RAM, free disk, no orphans, ports free, lockfile)
- `internal/harness/preflight_test.go`
- `internal/harness/suite.go` — `SliceConfig`, slice registry, TPC-H query loader
- `internal/harness/micros.go` — `MicroReverseBloom` (v1 only; others deferred)
- `internal/harness/harness.go` — top-level `Run(cfg) (RunResult, error)` orchestrator
- `internal/harness/fake_cluster_test.go` — Layer 2 fake cluster (in-process goroutines pretending to be coordinator + worker) and one happy-path test
- `cmd/tpch-harness/main.go` — flag parsing, mode dispatch, exit codes
- `benchmarks/tpch/baseline-sf100.json` — empty baseline scaffold (real values populated from a future golden run)

**Modified files:**
- `internal/distributed/messages.go` — add `Mallocs` field to `WorkerHeartbeat`; add `PeakHeapMB` to `TaskStats`
- `internal/worker/worker.go` — populate `Mallocs` in heartbeat (around line 628)
- `internal/worker/executor.go` — atomic per-task peak heap tracking, populate `PeakHeapMB` in `TaskStats` (around line 182 / 753)
- `Taskfile.yml` — add `harness:smoke`, `harness:local`, `harness:large`, `harness:golden`, `harness:clean`

---

## Tasks

### Task 1: Package skeleton and core types

**Files:**
- Create: `internal/harness/doc.go`
- Create: `internal/harness/types.go`

- [ ] **Step 1: Create the package directory and doc.go**

```bash
mkdir -p /home/dwright/Projects/caelum/internal/harness
```

Write `internal/harness/doc.go`:

```go
// Package harness implements the distributed test harness used by
// cmd/tpch-harness. It orchestrates a multi-process wadjet cluster on the
// dev box (local mode) or drives a pre-existing cluster (golden mode),
// runs the TPC-H query suite plus synthetic micro-queries, captures
// structured measurements, and compares them against a calibrated baseline.
//
// The package is intentionally importable (not test-only) so the harness
// binary can use it directly. Helpers for setting up an embedded NATS,
// loading the catalog, and submitting queries are extracted from the
// existing distributed_tpch_test.go in internal/coordinator so there is
// exactly one implementation.
//
// See docs/superpowers/specs/2026-04-08-distributed-test-harness-design.md
// for the full design.
package harness
```

- [ ] **Step 2: Write types.go with all core types**

Write `internal/harness/types.go`:

```go
package harness

import (
	"time"
)

// Mode is the harness run mode.
type Mode string

const (
	ModeLocal  Mode = "local"
	ModeGolden Mode = "golden"
)

// Slice identifies a local-mode data slice configuration.
type Slice string

const (
	SliceSmall Slice = "small"
	SliceLarge Slice = "large"
)

// Exit codes returned by cmd/tpch-harness. Higher numbers outrank lower
// when multiple failures occur in one run.
const (
	ExitOK            = 0
	ExitRegression    = 1 // perf regression, missing spill paths, or hang
	ExitSetup         = 2 // setup error, cluster crash, internal harness bug
	ExitCorrectness   = 3 // row count or checksum diverged
)

// Config is the parsed flag set passed into Run.
type Config struct {
	Mode         Mode
	Slice        Slice  // local only
	CoordURL     string // golden only
	DataDir      string // local only; default /tmp/sf100-sample
	BaselinePath string
	OutPath      string
	Queries      []string // empty means all 22 + micros
	UpdateBaseline bool
	NoCompare    bool
	WadjetBin    string // path to wadjet binary; auto-built if empty
}

// QueryMeasurement is the result of running one query (or micro).
type QueryMeasurement struct {
	Query         string    `json:"query"`
	WallMs        int64     `json:"wall_ms"`
	PeakHeapMB    int64     `json:"peak_heap_mb"`
	AllocCount   int64     `json:"alloc_count"`
	SpillBytes    int64     `json:"spill_bytes"`
	RowCount      int64    `json:"row_count"`
	RowChecksum   string    `json:"row_checksum"`
	GoroutinePeak int       `json:"goroutine_peak"`
	Hung          bool      `json:"hung"`
	HangDumpPath  string    `json:"hang_dump_path,omitempty"`
	StartedAt     time.Time `json:"started_at"`
}

// QueryDelta records a single per-metric drift between projected and baseline.
type QueryDelta struct {
	Query      string  `json:"query"`
	Metric     string  `json:"metric"`
	Baseline   float64 `json:"baseline"`
	Projected  float64 `json:"projected"`
	DriftPct   float64 `json:"drift_pct"`
	TolerancePct float64 `json:"tolerance_pct"`
	Status     string  `json:"status"` // "PASS", "REGRESS"
}

// RunResult is the top-level structured output written to the result JSON.
type RunResult struct {
	Mode         Mode               `json:"mode"`
	Slice        Slice              `json:"slice,omitempty"`
	StartedAt    time.Time          `json:"started_at"`
	DurationMs   int64              `json:"duration_ms"`
	Queries      []QueryMeasurement `json:"queries"`
	BaselinePath string             `json:"baseline_path"`
	Regressions  []QueryDelta       `json:"regressions"`
	Hangs        []string           `json:"hangs"`
	Passed       bool               `json:"passed"`
	ExitCode     int                `json:"exit_code"`
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cd /home/dwright/Projects/caelum && go build ./internal/harness/...`
Expected: clean build, no output.

- [ ] **Step 4: Commit**

```bash
git add internal/harness/doc.go internal/harness/types.go
git commit -m "$(cat <<'EOF'
feat(harness): add internal/harness package skeleton and core types

First task in the distributed test harness implementation.
See docs/superpowers/specs/2026-04-08-distributed-test-harness-design.md.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Baseline file format and JSON round-trip

**Files:**
- Create: `internal/harness/baseline.go`
- Create: `internal/harness/baseline_test.go`

- [ ] **Step 1: Write the failing test for round-trip**

Write `internal/harness/baseline_test.go`:

```go
package harness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestBaselineRoundTrip(t *testing.T) {
	bf := BaselineFile{
		Version:    1,
		CapturedAt: "2026-04-08T12:00:00Z",
		CapturedOn: "test fixture",
		Queries: map[string]QueryBaseline{
			"q05": {
				WallMsP50:           118000,
				WallMsTolerancePct:  25,
				PeakHeapMB:          14336,
				PeakHeapTolerancePct: 15,
				AllocCount:         47000000000,
				AllocCountTolerancePct:  20,
				SpillBytesWritten:   8500000000,
				SpillTolerancePct:   30,
				RowCount:            5,
				RowChecksum:         "abc123",
			},
		},
		ProjectionFactors: map[string]Projection{
			"large_slice": {
				WallMsMultiplier: 0.20,
				HeapMultiplier:   0.55,
				AllocCountMultiplier: 0.50,
				SpillMultiplier:  0.45,
			},
		},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "baseline.json")
	if err := bf.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := LoadBaseline(path)
	if err != nil {
		t.Fatalf("LoadBaseline: %v", err)
	}

	got, _ := json.Marshal(loaded)
	want, _ := json.Marshal(&bf)
	if string(got) != string(want) {
		t.Errorf("round trip mismatch:\nwant=%s\ngot =%s", want, got)
	}
}

func TestLoadBaselineMissingFile(t *testing.T) {
	_, err := LoadBaseline("/nonexistent/path.json")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	if !os.IsNotExist(err) {
		t.Errorf("expected IsNotExist, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/dwright/Projects/caelum && go test ./internal/harness/ -run TestBaseline -v`
Expected: FAIL with `undefined: BaselineFile` (or similar — types don't exist yet)

- [ ] **Step 3: Write baseline.go with types and round-trip**

Write `internal/harness/baseline.go`:

```go
package harness

import (
	"encoding/json"
	"fmt"
	"os"
)

// BaselineFile is the on-disk schema for the calibration table. The version
// field is incremented when incompatible changes are made.
type BaselineFile struct {
	Version           int                       `json:"version"`
	CapturedAt        string                    `json:"captured_at"`
	CapturedOn        string                    `json:"captured_on"`
	Queries           map[string]QueryBaseline  `json:"queries"`
	ProjectionFactors map[string]Projection     `json:"projection_factors"`
}

// QueryBaseline holds the golden numbers and tolerances for one query.
type QueryBaseline struct {
	WallMsP50            int64   `json:"wall_ms_p50"`
	WallMsTolerancePct   float64 `json:"wall_ms_tolerance_pct"`
	PeakHeapMB           int64   `json:"peak_heap_mb"`
	PeakHeapTolerancePct float64 `json:"peak_heap_tolerance_pct"`
	AllocCount          int64   `json:"alloc_count"`
	AllocCountTolerancePct   float64 `json:"alloc_count_tolerance_pct"`
	SpillBytesWritten    int64   `json:"spill_bytes_written"`
	SpillTolerancePct    float64 `json:"spill_tolerance_pct"`
	RowCount             int64   `json:"row_count"`
	RowChecksum          string  `json:"row_checksum"`
}

// Projection maps a local-mode metric to the equivalent golden-mode value
// via per-metric multipliers. local / multiplier = projected golden value.
type Projection struct {
	WallMsMultiplier float64 `json:"wall_ms_multiplier"`
	HeapMultiplier   float64 `json:"heap_multiplier"`
	AllocCountMultiplier float64 `json:"alloc_count_multiplier"`
	SpillMultiplier  float64 `json:"spill_multiplier"`
}

// LoadBaseline reads and parses a baseline file.
func LoadBaseline(path string) (*BaselineFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var bf BaselineFile
	if err := json.Unmarshal(data, &bf); err != nil {
		return nil, fmt.Errorf("parsing baseline %s: %w", path, err)
	}
	if bf.Version != 1 {
		return nil, fmt.Errorf("unsupported baseline version %d (want 1)", bf.Version)
	}
	return &bf, nil
}

// Save writes the baseline to disk as pretty-printed JSON.
func (bf *BaselineFile) Save(path string) error {
	data, err := json.MarshalIndent(bf, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/dwright/Projects/caelum && go test ./internal/harness/ -run TestBaseline -v`
Expected: PASS for both tests.

- [ ] **Step 5: Commit**

```bash
git add internal/harness/baseline.go internal/harness/baseline_test.go
git commit -m "feat(harness): baseline file format with JSON round-trip

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Baseline projection and comparison

**Files:**
- Modify: `internal/harness/baseline.go`
- Modify: `internal/harness/baseline_test.go`

- [ ] **Step 1: Write failing tests for projection and compare**

Append to `internal/harness/baseline_test.go`:

```go
func TestProjectLocalToGolden(t *testing.T) {
	bf := &BaselineFile{
		Version: 1,
		ProjectionFactors: map[string]Projection{
			"large_slice": {
				WallMsMultiplier: 0.20,
				HeapMultiplier:   0.55,
				AllocCountMultiplier: 0.50,
				SpillMultiplier:  0.45,
			},
		},
	}
	local := QueryMeasurement{
		WallMs:      24000,
		PeakHeapMB:  7884, // 14336 * 0.55
		AllocCount: 23500000000,
		SpillBytes:  3825000000,
	}
	projected, err := bf.Project("large_slice", local)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if projected.WallMs != 120000 {
		t.Errorf("WallMs: want 120000, got %d", projected.WallMs)
	}
	if projected.PeakHeapMB != 14334 && projected.PeakHeapMB != 14335 && projected.PeakHeapMB != 14336 {
		t.Errorf("PeakHeapMB: want ~14336, got %d", projected.PeakHeapMB)
	}
}

func TestCompareDetectsRegression(t *testing.T) {
	bf := &BaselineFile{
		Version: 1,
		Queries: map[string]QueryBaseline{
			"q05": {
				WallMsP50:          120000,
				WallMsTolerancePct: 25,
				PeakHeapMB:         14336,
				PeakHeapTolerancePct: 15,
				AllocCount:        47000000000,
				AllocCountTolerancePct: 20,
				SpillBytesWritten:  8500000000,
				SpillTolerancePct:  30,
				RowCount:           5,
				RowChecksum:        "abc123",
			},
		},
	}

	// Within tolerance — pass
	good := QueryMeasurement{
		Query: "q05", WallMs: 130000, PeakHeapMB: 14000, AllocCount: 48000000000,
		SpillBytes: 8000000000, RowCount: 5, RowChecksum: "abc123",
	}
	deltas := bf.Compare(good)
	for _, d := range deltas {
		if d.Status != "PASS" {
			t.Errorf("expected PASS, got %v for %s", d.Status, d.Metric)
		}
	}

	// 2x slower — should regress
	bad := QueryMeasurement{
		Query: "q05", WallMs: 240000, PeakHeapMB: 14000, AllocCount: 48000000000,
		SpillBytes: 8000000000, RowCount: 5, RowChecksum: "abc123",
	}
	deltas = bf.Compare(bad)
	var found bool
	for _, d := range deltas {
		if d.Metric == "wall_ms" && d.Status == "REGRESS" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected wall_ms REGRESS, got %+v", deltas)
	}
}

func TestCompareDetectsCorrectnessFailure(t *testing.T) {
	bf := &BaselineFile{
		Version: 1,
		Queries: map[string]QueryBaseline{
			"q05": {RowCount: 5, RowChecksum: "abc123"},
		},
	}
	wrongRows := QueryMeasurement{Query: "q05", RowCount: 7, RowChecksum: "abc123"}
	deltas := bf.Compare(wrongRows)
	var found bool
	for _, d := range deltas {
		if d.Metric == "row_count" && d.Status == "REGRESS" {
			found = true
		}
	}
	if !found {
		t.Error("expected row_count REGRESS")
	}
}
```

- [ ] **Step 2: Run failing tests**

Run: `cd /home/dwright/Projects/caelum && go test ./internal/harness/ -run "TestProject|TestCompare" -v`
Expected: FAIL with `bf.Project undefined` and `bf.Compare undefined`.

- [ ] **Step 3: Implement Project and Compare**

Append to `internal/harness/baseline.go`:

```go
// Project converts a local-mode measurement into projected golden-mode
// values using the projection factors for the given slice.
func (bf *BaselineFile) Project(sliceKey string, local QueryMeasurement) (QueryMeasurement, error) {
	pf, ok := bf.ProjectionFactors[sliceKey]
	if !ok {
		return QueryMeasurement{}, fmt.Errorf("no projection factors for slice %q", sliceKey)
	}
	projected := local // copy
	if pf.WallMsMultiplier > 0 {
		projected.WallMs = int64(float64(local.WallMs) / pf.WallMsMultiplier)
	}
	if pf.HeapMultiplier > 0 {
		projected.PeakHeapMB = int64(float64(local.PeakHeapMB) / pf.HeapMultiplier)
	}
	if pf.AllocCountMultiplier > 0 {
		projected.AllocCount = int64(float64(local.AllocCount) / pf.AllocCountMultiplier)
	}
	if pf.SpillMultiplier > 0 {
		projected.SpillBytes = int64(float64(local.SpillBytes) / pf.SpillMultiplier)
	}
	return projected, nil
}

// Compare returns one QueryDelta per metric for the given measurement.
// Status is "PASS" if drift is within tolerance, "REGRESS" otherwise.
// Row count and checksum mismatches always REGRESS regardless of tolerance.
func (bf *BaselineFile) Compare(m QueryMeasurement) []QueryDelta {
	qb, ok := bf.Queries[m.Query]
	if !ok {
		return nil // unknown query, can't compare
	}

	var out []QueryDelta
	check := func(metric string, baseline, observed float64, tolerancePct float64) {
		if baseline == 0 {
			return // skip metrics not set in baseline
		}
		drift := (observed - baseline) / baseline * 100
		status := "PASS"
		if drift > tolerancePct {
			status = "REGRESS"
		}
		out = append(out, QueryDelta{
			Query:        m.Query,
			Metric:       metric,
			Baseline:     baseline,
			Projected:    observed,
			DriftPct:     drift,
			TolerancePct: tolerancePct,
			Status:       status,
		})
	}

	check("wall_ms", float64(qb.WallMsP50), float64(m.WallMs), qb.WallMsTolerancePct)
	check("peak_heap_mb", float64(qb.PeakHeapMB), float64(m.PeakHeapMB), qb.PeakHeapTolerancePct)
	check("alloc_count", float64(qb.AllocCount), float64(m.AllocCount), qb.AllocCountTolerancePct)
	check("spill_bytes", float64(qb.SpillBytesWritten), float64(m.SpillBytes), qb.SpillTolerancePct)

	// Row count and checksum: exact match required.
	if qb.RowCount != 0 && qb.RowCount != m.RowCount {
		out = append(out, QueryDelta{
			Query:    m.Query,
			Metric:   "row_count",
			Baseline: float64(qb.RowCount),
			Projected: float64(m.RowCount),
			Status:   "REGRESS",
		})
	}
	if qb.RowChecksum != "" && qb.RowChecksum != m.RowChecksum {
		out = append(out, QueryDelta{
			Query:    m.Query,
			Metric:   "row_checksum",
			Baseline: 0,
			Projected: 0,
			Status:   "REGRESS",
		})
	}
	return out
}
```

- [ ] **Step 4: Run all baseline tests**

Run: `cd /home/dwright/Projects/caelum && go test ./internal/harness/ -v`
Expected: PASS for all 5 baseline tests.

- [ ] **Step 5: Commit**

```bash
git add internal/harness/baseline.go internal/harness/baseline_test.go
git commit -m "feat(harness): baseline projection and tolerance comparison

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Heartbeat extension — add Mallocs field

**Files:**
- Modify: `internal/distributed/messages.go` (line 181-193)
- Modify: `internal/worker/worker.go` (line 615-640)

- [ ] **Step 1: Add Mallocs field to WorkerHeartbeat struct**

Edit `internal/distributed/messages.go`. Find the `WorkerHeartbeat` struct (around line 181). Add `Mallocs` field after `NumGoroutines`:

```go
type WorkerHeartbeat struct {
	WorkerID      string    `json:"worker_id"`
	ClusterID     string    `json:"cluster_id,omitempty"`
	ActiveTasks   int       `json:"active_tasks"`
	ActiveTaskIDs []string  `json:"active_task_ids,omitempty"`
	MemoryUsed    int64     `json:"memory_used"`
	MemoryTotal   int64     `json:"memory_total"`
	RSS           int64     `json:"rss,omitempty"`
	NumGoroutines int       `json:"num_goroutines,omitempty"`
	Mallocs       uint64    `json:"mallocs,omitempty"`         // cumulative allocation count from runtime.MemStats
	SpillDiskUsed int64     `json:"spill_disk_used,omitempty"`
	Draining      bool      `json:"draining,omitempty"`
	Timestamp     time.Time `json:"timestamp"`
}
```

- [ ] **Step 2: Populate Mallocs in worker heartbeat**

Edit `internal/worker/worker.go`. Find the heartbeat construction (around line 628). The `runtime.MemStats` is already read into `memStats` at line 617-618. Add `Mallocs` to the heartbeat literal:

```go
hb := distributed.WorkerHeartbeat{
    WorkerID:      w.config.WorkerID,
    ClusterID:     w.config.ClusterID,
    ActiveTasks:   len(sem),
    ActiveTaskIDs: taskIDs,
    MemoryUsed:    int64(memStats.Alloc),
    MemoryTotal:   int64(memStats.Sys),
    RSS:           distributed.ProcessRSS(),
    NumGoroutines: distributed.NumGoroutines(),
    Mallocs:       memStats.Mallocs,
    SpillDiskUsed: distributed.DirDiskUsage(w.config.SpillDir),
    Draining:      w.Draining(),
    Timestamp:     time.Now(),
}
```

- [ ] **Step 3: Build and test**

Run: `cd /home/dwright/Projects/caelum && go build ./... && go test ./internal/distributed/ ./internal/worker/ -count=1`
Expected: clean build, tests pass.

- [ ] **Step 4: Commit**

```bash
git add internal/distributed/messages.go internal/worker/worker.go
git commit -m "feat(worker): add Mallocs to WorkerHeartbeat for harness GC tracking

Allocation count is the GC pressure signal the test harness uses to
detect alloc-rate regressions independently of wall time.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Per-task peak heap tracking in TaskStats

**Files:**
- Modify: `internal/distributed/messages.go` (TaskStats struct, line 172-178)
- Modify: `internal/worker/executor.go` (line 182, 753)

- [ ] **Step 1: Add PeakHeapMB to TaskStats**

Edit `internal/distributed/messages.go`. Find the `TaskStats` struct (around line 172). Add `PeakHeapMB`:

```go
type TaskStats struct {
	MemUsed    int64 `json:"mem_used"`
	MemBudget  int64 `json:"mem_budget"`
	SpillFiles int   `json:"spill_files"`
	SpillBytes int64 `json:"spill_bytes"`
	RSS        int64 `json:"rss"`
	PeakHeapMB int64 `json:"peak_heap_mb"` // exact per-task peak HeapAlloc/MB, captured by atomic-max sampler
}
```

- [ ] **Step 2: Read internal/worker/executor.go to find the right insertion points**

Run: `cd /home/dwright/Projects/caelum && grep -n "TaskStats\|RSS:" internal/worker/executor.go`

Identify the two places where `TaskStats` is constructed (around line 182 and line 753). Read the surrounding code (lines 170-200 and 740-770) to understand the per-task lifecycle.

- [ ] **Step 3: Add per-task peak heap tracker**

Create a new helper file `internal/worker/task_peak_heap.go`:

```go
package worker

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"
)

// taskPeakHeapTracker samples runtime HeapAlloc at a fast cadence and
// records the maximum observed during one task's execution. The tracker
// runs in a goroutine started at task start and stopped at task end.
//
// We use a 50ms sample interval — fast enough to catch peaks in queries
// that complete in 1-2 seconds, slow enough to be negligible overhead
// (one MemStats read per 50ms is ~10us).
type taskPeakHeapTracker struct {
	peakBytes atomic.Uint64
	stop      chan struct{}
	done      chan struct{}
}

func startTaskPeakHeap() *taskPeakHeapTracker {
	t := &taskPeakHeapTracker{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go t.loop()
	return t
}

func (t *taskPeakHeapTracker) loop() {
	defer close(t.done)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var ms runtime.MemStats
	for {
		select {
		case <-t.stop:
			return
		case <-ticker.C:
			runtime.ReadMemStats(&ms)
			for {
				cur := t.peakBytes.Load()
				if ms.HeapAlloc <= cur {
					break
				}
				if t.peakBytes.CompareAndSwap(cur, ms.HeapAlloc) {
					break
				}
			}
		}
	}
}

// PeakMB returns the highest HeapAlloc observed since startTaskPeakHeap,
// rounded to whole megabytes. Safe to call concurrently with the loop.
func (t *taskPeakHeapTracker) PeakMB() int64 {
	return int64(t.peakBytes.Load() / (1024 * 1024))
}

// Stop terminates the sampler goroutine and waits for it to exit. Safe
// to call multiple times.
func (t *taskPeakHeapTracker) Stop(ctx context.Context) {
	select {
	case <-t.stop:
		// already stopped
	default:
		close(t.stop)
	}
	select {
	case <-t.done:
	case <-ctx.Done():
	}
}
```

- [ ] **Step 4: Wire the tracker into executor.go**

Read `internal/worker/executor.go` lines 100-200 to find where each task starts execution. There is typically a per-task entry point that constructs `TaskStats` near line 182. The pattern to add:

```go
// At the start of per-task execution:
peakTracker := startTaskPeakHeap()
defer peakTracker.Stop(context.Background())

// Where TaskStats is constructed (line 182 area):
result.TaskStats = &distributed.TaskStats{
    RSS:        distributed.ProcessRSS(),
    PeakHeapMB: peakTracker.PeakMB(),
    // ... other fields if present
}
```

Apply the same pattern at line 753 (the second TaskStats construction site). The tracker must be started BEFORE the query work begins and stopped AFTER the work but BEFORE building the TaskStats result, so that PeakMB() returns the final peak.

VERIFY: Read the actual code at lines 175-200 and 745-770 to confirm the exact insertion point. The exact variable names and surrounding error handling will determine the precise edit.

- [ ] **Step 5: Write a unit test for the tracker**

Create `internal/worker/task_peak_heap_test.go`:

```go
package worker

import (
	"context"
	"testing"
	"time"
)

func TestTaskPeakHeapTracksGrowth(t *testing.T) {
	tracker := startTaskPeakHeap()
	defer tracker.Stop(context.Background())

	// Initial peak should be 0 or very small.
	initial := tracker.PeakMB()

	// Allocate ~50 MB and hold a reference so GC can't free it before
	// the sampler observes the spike.
	const allocMB = 50
	hold := make([][]byte, allocMB)
	for i := 0; i < allocMB; i++ {
		hold[i] = make([]byte, 1024*1024)
	}

	// Give the sampler at least 3 ticks to observe.
	time.Sleep(200 * time.Millisecond)

	peak := tracker.PeakMB()
	_ = hold // keep the reference live until after we read peak

	if peak <= initial {
		t.Errorf("expected peak to grow above initial=%d, got peak=%d", initial, peak)
	}
	if peak < allocMB/2 {
		t.Errorf("expected peak >= %d MB, got %d", allocMB/2, peak)
	}
}

func TestTaskPeakHeapStopIsIdempotent(t *testing.T) {
	tracker := startTaskPeakHeap()
	tracker.Stop(context.Background())
	tracker.Stop(context.Background()) // should not panic
}
```

- [ ] **Step 6: Run tests and commit**

Run: `cd /home/dwright/Projects/caelum && go test ./internal/worker/ -run TestTaskPeakHeap -v -count=1 && go build ./...`
Expected: PASS for both tests, clean build of full project.

```bash
git add internal/distributed/messages.go internal/worker/executor.go \
        internal/worker/task_peak_heap.go internal/worker/task_peak_heap_test.go
git commit -m "feat(worker): per-task peak heap tracking via atomic-max sampler

Captures the exact per-task HeapAlloc peak at 50ms cadence, populated
into TaskStats.PeakHeapMB on task completion. The 1Hz heartbeat is
too coarse for queries under 5s, so the harness needs an exact peak
reported alongside the result.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Measurement collector and hang detector

**Files:**
- Create: `internal/harness/measure.go`
- Create: `internal/harness/measure_test.go`

- [ ] **Step 1: Write failing tests for the collector**

Write `internal/harness/measure_test.go`:

```go
package harness

import (
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

func TestCollectorAggregatesPeak(t *testing.T) {
	c := NewCollector()
	c.StartWindow("q05")

	// Synthetic heartbeats: heap grows from 100 MB to 800 MB then drops.
	heartbeats := []uint64{
		100 * 1024 * 1024,
		300 * 1024 * 1024,
		800 * 1024 * 1024,
		200 * 1024 * 1024,
	}
	for i, h := range heartbeats {
		c.Observe(distributed.WorkerHeartbeat{
			WorkerID:      "w0",
			MemoryUsed:    int64(h),
			Mallocs:       uint64(1000 * (i + 1)),
			NumGoroutines: 50,
			Timestamp:     time.Now().Add(time.Duration(i) * 100 * time.Millisecond),
		})
	}

	m := c.EndWindow("q05")
	if m.PeakHeapMB < 800 {
		t.Errorf("expected peak >= 800 MB, got %d", m.PeakHeapMB)
	}
	// Allocs delta = 4000 - 1000 = 3000
	if m.AllocCount != 3000 {
		t.Errorf("expected allocs delta 3000, got %d", m.AllocCount)
	}
}

func TestHangDetectorTriggersOnMonotonicGrowth(t *testing.T) {
	hd := NewHangDetector(30 * time.Second)
	now := time.Now()

	// Goroutine count grows monotonically for 35s.
	for i := 0; i < 36; i++ {
		hung := hd.Observe(now.Add(time.Duration(i)*time.Second), 100+i)
		if i < 30 && hung {
			t.Errorf("triggered too early at i=%d", i)
		}
	}
	if !hd.Observe(now.Add(36*time.Second), 137) {
		t.Error("expected hang trigger after 36s of monotonic growth")
	}
}

func TestHangDetectorDoesNotTriggerOnNoise(t *testing.T) {
	hd := NewHangDetector(30 * time.Second)
	now := time.Now()
	// Goroutine count oscillates around 100.
	counts := []int{100, 105, 102, 108, 100, 110, 95, 105, 100, 102}
	for i := 0; i < 60; i++ {
		hung := hd.Observe(now.Add(time.Duration(i)*time.Second), counts[i%len(counts)])
		if hung {
			t.Errorf("false trigger at i=%d", i)
		}
	}
}
```

- [ ] **Step 2: Run failing tests**

Run: `cd /home/dwright/Projects/caelum && go test ./internal/harness/ -run "TestCollector|TestHangDetector" -v`
Expected: FAIL, undefined symbols.

- [ ] **Step 3: Implement measure.go**

Write `internal/harness/measure.go`:

```go
package harness

import (
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// MeasurementCollector subscribes to worker heartbeats and aggregates
// per-query measurement windows. One Collector instance is used for the
// entire run; queries demarcate measurement windows by calling
// StartWindow / EndWindow.
type MeasurementCollector struct {
	mu      sync.Mutex
	current *windowState
}

type windowState struct {
	query         string
	startedAt     time.Time
	peakHeapMB    int64
	startMallocs  uint64
	endMallocs    uint64
	totalSpill    int64
	goroutinePeak int
}

// NewCollector creates a fresh collector with no active window.
func NewCollector() *MeasurementCollector {
	return &MeasurementCollector{}
}

// StartWindow begins a new measurement window for the given query.
// Any prior window is discarded.
func (c *MeasurementCollector) StartWindow(query string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = &windowState{
		query:     query,
		startedAt: time.Now(),
	}
}

// Observe feeds a heartbeat into the active window. Safe to call from
// the heartbeat subscriber goroutine.
func (c *MeasurementCollector) Observe(hb distributed.WorkerHeartbeat) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == nil {
		return
	}
	if mb := hb.MemoryUsed / (1024 * 1024); mb > c.current.peakHeapMB {
		c.current.peakHeapMB = mb
	}
	if c.current.startMallocs == 0 {
		c.current.startMallocs = hb.Mallocs
	}
	c.current.endMallocs = hb.Mallocs
	if hb.SpillDiskUsed > c.current.totalSpill {
		c.current.totalSpill = hb.SpillDiskUsed
	}
	if hb.NumGoroutines > c.current.goroutinePeak {
		c.current.goroutinePeak = hb.NumGoroutines
	}
}

// EndWindow finalizes the active window and returns its measurement.
// Returns the zero value if there's no active window or the query name
// doesn't match.
func (c *MeasurementCollector) EndWindow(query string) QueryMeasurement {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == nil || c.current.query != query {
		return QueryMeasurement{Query: query}
	}
	w := c.current
	c.current = nil
	return QueryMeasurement{
		Query:         w.query,
		StartedAt:     w.startedAt,
		WallMs:        time.Since(w.startedAt).Milliseconds(),
		PeakHeapMB:    w.peakHeapMB,
		AllocCount:   int64(w.endMallocs - w.startMallocs),
		SpillBytes:    w.totalSpill,
		GoroutinePeak: w.goroutinePeak,
	}
}

// HangDetector watches a goroutine count series and trips when the
// count grows monotonically for longer than the threshold duration.
//
// "Monotonic" here means: from some start time T0, every subsequent
// observation is strictly greater than the one before. Any decrease
// resets the run. After threshold elapses without a decrease, Observe
// returns true.
type HangDetector struct {
	threshold      time.Duration
	runStart       time.Time
	lastCount      int
	monotonicSince time.Time
	tripped        bool
}

// NewHangDetector creates a detector with the given threshold (e.g. 30s).
func NewHangDetector(threshold time.Duration) *HangDetector {
	return &HangDetector{threshold: threshold}
}

// Observe records one (timestamp, goroutine count) sample. Returns true
// if a hang has been detected. Once tripped, returns true forever until
// Reset is called.
func (h *HangDetector) Observe(t time.Time, count int) bool {
	if h.tripped {
		return true
	}
	if h.runStart.IsZero() {
		h.runStart = t
		h.monotonicSince = t
		h.lastCount = count
		return false
	}
	if count > h.lastCount {
		// still growing — keep the run going
		h.lastCount = count
		if t.Sub(h.monotonicSince) >= h.threshold {
			h.tripped = true
			return true
		}
		return false
	}
	// count did not strictly grow — reset the monotonic window
	h.monotonicSince = t
	h.lastCount = count
	return false
}

// Reset clears the trip state and starts over.
func (h *HangDetector) Reset() {
	h.runStart = time.Time{}
	h.monotonicSince = time.Time{}
	h.lastCount = 0
	h.tripped = false
}
```

- [ ] **Step 4: Run tests**

Run: `cd /home/dwright/Projects/caelum && go test ./internal/harness/ -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/harness/measure.go internal/harness/measure_test.go
git commit -m "feat(harness): measurement collector and hang detector

Aggregates heartbeat-stream samples per query window and detects hangs
via monotonic goroutine-count growth over a configurable threshold.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Slice configs and TPC-H query loader

**Files:**
- Create: `internal/harness/suite.go`

- [ ] **Step 1: Read existing TPC-H query exports**

Run: `cd /home/dwright/Projects/caelum && grep -n "^func\|^var\|Query" benchmarks/tpch/queries.go | head -30`

Identify how the 22 TPC-H query strings are exposed (e.g., `var Q01 = "..."` or a `Queries` map).

VERIFY: Read `benchmarks/tpch/queries.go` to confirm the export shape. The plan below assumes a `Queries` map; if it's individual `Q01..Q22` vars, build the map yourself.

- [ ] **Step 2: Write suite.go**

Write `internal/harness/suite.go`:

```go
package harness

import (
	"fmt"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
)

// SliceConfig describes a local-mode data slice. The harness uses these
// to choose how many sample files to load into the catalog and what
// GOMEMLIMIT each worker process is started with.
type SliceConfig struct {
	Name          Slice
	LineitemFiles int
	OrdersFiles   int
	GoMemLimit    int64 // bytes; passed to worker via GOMEMLIMIT env
	ExpectSpill   bool  // if true and total spill bytes == 0, fail the run
}

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)

var SliceConfigs = map[Slice]SliceConfig{
	SliceSmall: {
		Name:          SliceSmall,
		LineitemFiles: 4,
		OrdersFiles:   1,
		GoMemLimit:    4 * GB,
		ExpectSpill:   false,
	},
	SliceLarge: {
		Name:          SliceLarge,
		LineitemFiles: 12,
		OrdersFiles:   3,
		GoMemLimit:    8 * GB,
		ExpectSpill:   true,
	},
}

// LoadQuery returns the SQL text for the given TPC-H query name (e.g. "q05").
// Returns an error if the query is not in the standard 22.
//
// VERIFY: this assumes benchmarks/tpch.Queries is a map[string]string.
// If queries are individual vars, replace the body with a switch.
func LoadQuery(name string) (string, error) {
	if sql, ok := tpch.Queries[name]; ok {
		return sql, nil
	}
	return "", fmt.Errorf("unknown TPC-H query %q", name)
}

// AllTPCHQueries returns the names of all 22 TPC-H queries in canonical order.
func AllTPCHQueries() []string {
	out := make([]string, 22)
	for i := 0; i < 22; i++ {
		out[i] = fmt.Sprintf("q%02d", i+1)
	}
	return out
}

// SelectQueries resolves the --queries flag to a final ordered list.
// An empty input means all 22 TPC-H queries plus all micros.
func SelectQueries(requested []string) []string {
	if len(requested) == 0 {
		out := AllTPCHQueries()
		out = append(out, "micro_reverse_bloom")
		return out
	}
	return requested
}
```

- [ ] **Step 3: Verify and unit-test**

Run: `cd /home/dwright/Projects/caelum && go build ./internal/harness/...`
Expected: clean build, OR a failure that tells you `tpch.Queries` doesn't exist — in which case adjust per the VERIFY note.

Add a quick test in `internal/harness/suite_test.go`:

```go
package harness

import "testing"

func TestSliceConfigs(t *testing.T) {
	if c := SliceConfigs[SliceSmall]; c.LineitemFiles != 4 {
		t.Errorf("small lineitem: want 4, got %d", c.LineitemFiles)
	}
	if c := SliceConfigs[SliceLarge]; !c.ExpectSpill {
		t.Error("large slice should ExpectSpill")
	}
}

func TestSelectQueriesEmpty(t *testing.T) {
	got := SelectQueries(nil)
	if len(got) != 23 {
		t.Errorf("want 22+1 queries, got %d", len(got))
	}
}
```

Run: `cd /home/dwright/Projects/caelum && go test ./internal/harness/ -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/harness/suite.go internal/harness/suite_test.go
git commit -m "feat(harness): slice configs and TPC-H query loader

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Resource preflight checks

**Files:**
- Create: `internal/harness/preflight.go`
- Create: `internal/harness/preflight_test.go`

- [ ] **Step 1: Write preflight.go**

Write `internal/harness/preflight.go`:

```go
package harness

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

// Preflight verifies the environment is suitable for running --mode=local.
// All checks are batched into one error so the operator sees everything
// they need to fix in one pass.
type PreflightResult struct {
	OK     bool
	Errors []string
}

func (r PreflightResult) Error() string {
	return strings.Join(r.Errors, "\n  - ")
}

// CheckPreflight runs all checks for the given slice and run dir.
func CheckPreflight(slice SliceConfig, runDir string, workerCount int) PreflightResult {
	var errs []string

	if runtime.GOOS != "linux" {
		errs = append(errs, fmt.Sprintf("harness requires linux (got %s)", runtime.GOOS))
	}

	// Free RAM check: (slice.GoMemLimit * workerCount) + 3 GB coord/harness + 4 GB OS headroom
	requiredRAMBytes := slice.GoMemLimit*int64(workerCount) + 3*GB + 4*GB
	if err := checkFreeRAM(requiredRAMBytes); err != nil {
		errs = append(errs, err.Error())
	}

	// Free disk check: 20 GB at the spill mount.
	if err := checkFreeDisk(runDir, 20*GB); err != nil {
		errs = append(errs, err.Error())
	}

	// No orphaned wadjet processes.
	if err := checkNoOrphanedWadjet(); err != nil {
		errs = append(errs, err.Error())
	}

	return PreflightResult{OK: len(errs) == 0, Errors: errs}
}

// checkFreeRAM reads /proc/meminfo to get MemAvailable, which is the
// kernel's estimate of how much memory can be allocated without swapping.
func checkFreeRAM(required int64) error {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return fmt.Errorf("read /proc/meminfo: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "MemAvailable:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return fmt.Errorf("parsing MemAvailable: %w", err)
		}
		availBytes := kb * 1024
		if availBytes < required {
			return fmt.Errorf("insufficient free RAM: have %d MB, need %d MB (free %d MB before re-running)",
				availBytes/MB, required/MB, (required-availBytes)/MB)
		}
		return nil
	}
	return fmt.Errorf("MemAvailable not found in /proc/meminfo")
}

// checkFreeDisk uses statfs on the run dir's parent (creating the parent
// if it doesn't exist).
func checkFreeDisk(runDir string, required int64) error {
	parent := filepath.Dir(runDir)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return fmt.Errorf("creating run dir parent %s: %w", parent, err)
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(parent, &stat); err != nil {
		return fmt.Errorf("statfs %s: %w", parent, err)
	}
	availBytes := int64(stat.Bavail) * int64(stat.Bsize)
	if availBytes < required {
		return fmt.Errorf("insufficient free disk at %s: have %d MB, need %d MB",
			parent, availBytes/MB, required/MB)
	}
	return nil
}

// checkNoOrphanedWadjet runs `pgrep -f wadjet` and refuses to start if
// any wadjet process is found, since orphans from a prior crashed harness
// run can corrupt port + spill assumptions.
func checkNoOrphanedWadjet() error {
	cmd := exec.Command("pgrep", "-f", "wadjet")
	out, err := cmd.Output()
	if err != nil {
		// pgrep exits 1 when no matches found — that's success for us.
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			return nil
		}
		return fmt.Errorf("pgrep failed: %w", err)
	}
	pids := strings.TrimSpace(string(out))
	if pids == "" {
		return nil
	}
	// Filter out our own pid (the harness binary may itself match "wadjet").
	myPID := strconv.Itoa(os.Getpid())
	var others []string
	for _, p := range strings.Split(pids, "\n") {
		if p != myPID {
			others = append(others, p)
		}
	}
	if len(others) > 0 {
		return fmt.Errorf("orphaned wadjet processes detected: pids %s — kill them and re-run", strings.Join(others, " "))
	}
	return nil
}
```

- [ ] **Step 2: Write preflight tests**

Write `internal/harness/preflight_test.go`:

```go
package harness

import (
	"testing"
)

func TestCheckFreeRAMDetectsExcessiveRequest(t *testing.T) {
	// Request 1 PB of RAM — should fail on any real machine.
	err := checkFreeRAM(1 << 50)
	if err == nil {
		t.Error("expected failure for 1PB request")
	}
}

func TestCheckFreeRAMTinyRequestSucceeds(t *testing.T) {
	// Request 1 MB — should succeed unless the box is on fire.
	err := checkFreeRAM(1 * MB)
	if err != nil {
		t.Errorf("1 MB request failed: %v", err)
	}
}

func TestCheckFreeDiskTinyRequestSucceeds(t *testing.T) {
	dir := t.TempDir() + "/foo/bar"
	err := checkFreeDisk(dir, 1*MB)
	if err != nil {
		t.Errorf("1 MB disk request failed: %v", err)
	}
}
```

- [ ] **Step 3: Run tests**

Run: `cd /home/dwright/Projects/caelum && go test ./internal/harness/ -run "TestCheckFree|TestCheckNoOrphan" -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/harness/preflight.go internal/harness/preflight_test.go
git commit -m "feat(harness): preflight resource checks (RAM, disk, orphaned procs)

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: Cluster supervisor — spawn, health check, idempotent shutdown

**Files:**
- Create: `internal/harness/cluster.go`

This is the largest single task and the most likely to be flaky. Pay attention to process group setup and the `defer cluster.Shutdown()` discipline in callers.

- [ ] **Step 1: Read existing wadjet binary CLI flags**

Run: `cd /home/dwright/Projects/caelum && grep -n "rootCmd\|--mode\|spillDir\|--nats-url" cmd/wadjet/main.go | head -30`

Confirm the flags used by `wadjet serve --mode=worker` and `--mode=coordinator`. The harness will pass: `--mode`, `--nats-url`, `--spill-dir`, and possibly `--data-bucket`.

VERIFY: read lines 100-150 and 680-720 of `cmd/wadjet/main.go` to learn the actual flag names. The plan below assumes `--nats-url`, `--spill-dir`, and `--mode`. Adjust if your repo differs.

- [ ] **Step 2: Write cluster.go skeleton**

Write `internal/harness/cluster.go`:

```go
package harness

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// ClusterConfig describes a local-mode cluster to spawn.
type ClusterConfig struct {
	WadjetBin   string // path to wadjet binary
	RunDir      string // /tmp/wadjet-harness/run-X
	NATSUrl     string // populated by Cluster.Start after embedded NATS comes up
	NumWorkers  int
	GoMemLimit  int64
	DataBucket  string // file:///tmp/sf100-sample (FileStore URL)
	Logger      *slog.Logger
}

// Cluster is a process supervisor for one coordinator + N workers running
// against an embedded NATS started in this process. Shutdown is idempotent.
type Cluster struct {
	cfg ClusterConfig

	mu          sync.Mutex
	embeddedNATS *distributed.EmbeddedNATS
	coord        *managedProcess
	workers      []*managedProcess
	shutdownOnce sync.Once
	shutdownErr  error
}

type managedProcess struct {
	role    string // "coord" or "worker-N"
	cmd     *exec.Cmd
	logFile *os.File
	exitedC chan struct{} // closed when the process exits
	exitErr error
}

// NewCluster constructs a Cluster but does not start anything.
func NewCluster(cfg ClusterConfig) *Cluster {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Cluster{cfg: cfg}
}

// Start brings up embedded NATS, spawns the coordinator and workers, and
// blocks until all workers have registered or until ctx is cancelled.
func (c *Cluster) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := os.MkdirAll(filepath.Join(c.cfg.RunDir, "logs"), 0755); err != nil {
		return fmt.Errorf("creating logs dir: %w", err)
	}

	// 1. Embedded NATS in our own process.
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = filepath.Join(c.cfg.RunDir, "nats")
	embedded, err := distributed.NewEmbeddedNATS(natsCfg, c.cfg.Logger)
	if err != nil {
		return fmt.Errorf("starting embedded NATS: %w", err)
	}
	c.embeddedNATS = embedded
	c.cfg.NATSUrl = embedded.ClientURL()
	c.cfg.Logger.Info("embedded NATS up", "url", c.cfg.NATSUrl)

	// 2. Spawn coordinator.
	coord, err := c.spawn("coord", []string{
		"serve",
		"--mode=coordinator",
		"--nats-url=" + c.cfg.NATSUrl,
		"--spill-dir=" + filepath.Join(c.cfg.RunDir, "spill", "coord"),
	})
	if err != nil {
		return fmt.Errorf("spawning coordinator: %w", err)
	}
	c.coord = coord

	// 3. Spawn workers.
	for i := 0; i < c.cfg.NumWorkers; i++ {
		role := fmt.Sprintf("worker-%d", i)
		w, err := c.spawn(role, []string{
			"serve",
			"--mode=worker",
			"--nats-url=" + c.cfg.NATSUrl,
			"--spill-dir=" + filepath.Join(c.cfg.RunDir, "spill", role),
		})
		if err != nil {
			return fmt.Errorf("spawning %s: %w", role, err)
		}
		c.workers = append(c.workers, w)
	}

	// 4. Health check: poll worker registry until NumWorkers report in,
	//    or 30s timeout.
	if err := c.waitWorkersReady(ctx, 30*time.Second); err != nil {
		return fmt.Errorf("workers not ready: %w", err)
	}

	c.cfg.Logger.Info("cluster ready", "workers", c.cfg.NumWorkers)
	return nil
}

// spawn starts one wadjet child process with GOMEMLIMIT set, log file
// piped from stdout/stderr, and a dedicated process group so we can
// signal the whole tree on shutdown.
func (c *Cluster) spawn(role string, args []string) (*managedProcess, error) {
	logPath := filepath.Join(c.cfg.RunDir, "logs", role+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(c.cfg.WadjetBin, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GOMEMLIMIT=%d", c.cfg.GoMemLimit),
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true, // own process group; killed via -<pgid>
	}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, err
	}

	mp := &managedProcess{
		role:    role,
		cmd:     cmd,
		logFile: logFile,
		exitedC: make(chan struct{}),
	}
	go func() {
		mp.exitErr = cmd.Wait()
		close(mp.exitedC)
	}()
	return mp, nil
}

// waitWorkersReady polls the embedded NATS for worker heartbeats until
// NumWorkers distinct worker IDs have been seen, or until timeout.
func (c *Cluster) waitWorkersReady(ctx context.Context, timeout time.Duration) error {
	nc, err := distributed.ConnectInProcess(c.embeddedNATS.Server())
	if err != nil {
		return fmt.Errorf("connecting in-process: %w", err)
	}
	defer nc.Close()

	seen := make(map[string]bool)
	sub, err := nc.Subscribe(distributed.SubjectHeartbeat, func(msg *natsMsgShim) {
		var hb distributed.WorkerHeartbeat
		if err := distributed.Unmarshal(msg.Data, &hb); err == nil {
			seen[hb.WorkerID] = true
		}
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(seen) >= c.cfg.NumWorkers {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("only %d/%d workers registered after %s", len(seen), c.cfg.NumWorkers, timeout)
}

// natsMsgShim is a placeholder type — the real subscribe call uses the
// nats.Msg type from the nats.go client. VERIFY the actual subscribe API
// in internal/distributed/nats_setup.go and adjust this signature.
type natsMsgShim = struct {
	Data []byte
}

// Shutdown stops all child processes and the embedded NATS. Idempotent.
// On the first call, sends SIGTERM to each process group, waits up to 5s,
// then SIGKILLs anything still alive. After all children are reaped, runs
// pgrep -f wadjet to verify nothing is orphaned.
func (c *Cluster) Shutdown(ctx context.Context) error {
	c.shutdownOnce.Do(func() {
		c.shutdownErr = c.shutdown(ctx)
	})
	return c.shutdownErr
}

func (c *Cluster) shutdown(ctx context.Context) error {
	c.mu.Lock()
	procs := append([]*managedProcess{}, c.workers...)
	if c.coord != nil {
		procs = append(procs, c.coord)
	}
	c.mu.Unlock()

	for _, p := range procs {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
	}

	deadline := time.After(5 * time.Second)
	for _, p := range procs {
		select {
		case <-p.exitedC:
		case <-deadline:
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			<-p.exitedC
		}
		if p.logFile != nil {
			p.logFile.Close()
		}
	}

	if c.embeddedNATS != nil {
		c.embeddedNATS.Shutdown()
		c.embeddedNATS = nil
	}

	// Verify no orphans.
	if err := checkNoOrphanedWadjet(); err != nil {
		return fmt.Errorf("post-shutdown orphan check: %w", err)
	}
	return nil
}
```

- [ ] **Step 3: Verify build (will likely have issues to fix)**

Run: `cd /home/dwright/Projects/caelum && go build ./internal/harness/...`

You will likely see errors related to `natsMsgShim` and the `Subscribe` signature. Read `internal/distributed/nats_setup.go` and `internal/worker/worker.go` (around line 600-650 where heartbeats are subscribed) to find the real `nats.Subscribe` pattern, then replace the `natsMsgShim` placeholder with the correct `*nats.Msg` type and import.

- [ ] **Step 4: Write a basic spawn-and-shutdown test**

Create `internal/harness/cluster_test.go`:

```go
package harness

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestClusterSpawnAndShutdown is an integration test that spawns a real
// 1-coord + 1-worker cluster against the embedded NATS, verifies they
// register, and then shuts them down cleanly.
//
// Requires: the wadjet binary built at /tmp/wadjet-harness-test/wadjet.
// Skipped if the binary cannot be built.
func TestClusterSpawnAndShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test, skip with -short")
	}

	binDir := t.TempDir()
	binPath := filepath.Join(binDir, "wadjet")
	build := exec.Command("go", "build", "-o", binPath, "./cmd/wadjet")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		t.Skipf("could not build wadjet: %v\n%s", err, out)
	}

	runDir := t.TempDir()
	cluster := NewCluster(ClusterConfig{
		WadjetBin:  binPath,
		RunDir:     runDir,
		NumWorkers: 1,
		GoMemLimit: 2 * GB,
		Logger:     slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := cluster.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}

	// Idempotent.
	if err := cluster.Shutdown(context.Background()); err != nil {
		t.Errorf("second Shutdown: %v", err)
	}
}
```

- [ ] **Step 5: Run the integration test**

Run: `cd /home/dwright/Projects/caelum && go test ./internal/harness/ -run TestClusterSpawnAndShutdown -v -timeout 90s`
Expected: PASS. If FAIL, the most likely cause is wrong flag names in step 1 — re-check `cmd/wadjet/main.go`.

- [ ] **Step 6: Commit**

```bash
git add internal/harness/cluster.go internal/harness/cluster_test.go
git commit -m "feat(harness): cluster process supervisor with spawn/shutdown

Spawns coordinator + workers as real wadjet child processes against
embedded NATS, with health-check polling and idempotent SIGTERM/KILL
shutdown plus pgrep verification.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Top-level harness Run() with query submission

**Files:**
- Create: `internal/harness/harness.go`

This task wires everything together. The query submission path uses pgwire — read `internal/server/pgwire/` first to confirm how to submit a query and read results without a full psql client.

- [ ] **Step 1: Read pgwire submission patterns**

Run: `cd /home/dwright/Projects/caelum && grep -rn "pgx\.Connect\|pgx\.Pool\|pgwire" benchmarks/tpch/ cmd/tpch-bench/ | head -10`

Find an existing example of how a Go program submits SQL to a wadjet coordinator. The TPC-H bench tool is the most likely source. Note the connection string format.

- [ ] **Step 2: Write harness.go**

Write `internal/harness/harness.go`:

```go
package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Run is the top-level entry point. Reads cfg, sets up the run dir,
// starts the cluster (local mode only), runs the query suite, compares
// against the baseline, writes result.json, and returns RunResult.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) (RunResult, error) {
	started := time.Now()
	result := RunResult{
		Mode:         cfg.Mode,
		Slice:        cfg.Slice,
		StartedAt:    started,
		BaselinePath: cfg.BaselinePath,
	}

	// Load baseline (unless --no-compare).
	var baseline *BaselineFile
	if !cfg.NoCompare {
		bf, err := LoadBaseline(cfg.BaselinePath)
		if err != nil && !os.IsNotExist(err) {
			return result, fmt.Errorf("loading baseline: %w", err)
		}
		baseline = bf
	}

	// Create run dir.
	runDir := filepath.Join("/tmp/wadjet-harness", fmt.Sprintf("run-%d", started.Unix()))
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return result, fmt.Errorf("creating run dir: %w", err)
	}

	// Local mode: preflight + spawn cluster.
	var cluster *Cluster
	var coordURL string
	switch cfg.Mode {
	case ModeLocal:
		slice := SliceConfigs[cfg.Slice]
		const numWorkers = 2
		pf := CheckPreflight(slice, runDir, numWorkers)
		if !pf.OK {
			return result, fmt.Errorf("preflight failed:\n  - %s", pf.Error())
		}

		cluster = NewCluster(ClusterConfig{
			WadjetBin:  cfg.WadjetBin,
			RunDir:     runDir,
			NumWorkers: numWorkers,
			GoMemLimit: slice.GoMemLimit,
			DataBucket: "file://" + cfg.DataDir,
			Logger:     logger,
		})
		if err := cluster.Start(ctx); err != nil {
			return result, fmt.Errorf("cluster start: %w", err)
		}
		defer cluster.Shutdown(context.Background())

		coordURL = cluster.CoordPgURL() // see VERIFY note below

		// TODO: load TPC-H sample data into the catalog. The implementation
		// of this step is the loadSampleData function below.
		if err := loadSampleData(ctx, cfg.DataDir, coordURL, slice); err != nil {
			return result, fmt.Errorf("loading sample data: %w", err)
		}

	case ModeGolden:
		coordURL = cfg.CoordURL

	default:
		return result, fmt.Errorf("unknown mode %q", cfg.Mode)
	}

	// Subscribe to heartbeats for measurement collection.
	collector := NewCollector()
	hangDetector := NewHangDetector(30 * time.Second)
	stopHB := startHeartbeatSubscriber(ctx, cluster, collector, hangDetector)
	defer stopHB()

	// Run the query suite.
	queries := SelectQueries(cfg.Queries)
	for _, qname := range queries {
		m, err := runOneQuery(ctx, coordURL, qname, collector, hangDetector, baseline, slice(cfg))
		if err != nil {
			logger.Error("query failed", "q", qname, "err", err)
			m.Hung = true // err means we treat it as a hang for v1
		}
		result.Queries = append(result.Queries, m)
		if m.Hung {
			result.Hangs = append(result.Hangs, qname)
		}
	}

	// Compare against baseline.
	if baseline != nil {
		for _, m := range result.Queries {
			if m.Hung {
				continue
			}
			projected, err := baseline.Project(string(cfg.Slice)+"_slice", m)
			if err != nil {
				continue
			}
			projected.Query = m.Query
			projected.RowCount = m.RowCount
			projected.RowChecksum = m.RowChecksum
			deltas := baseline.Compare(projected)
			for _, d := range deltas {
				if d.Status == "REGRESS" {
					result.Regressions = append(result.Regressions, d)
				}
			}
		}
	}

	// ExpectSpill assertion for large slice.
	if cfg.Mode == ModeLocal && cfg.Slice == SliceLarge {
		var totalSpill int64
		for _, m := range result.Queries {
			totalSpill += m.SpillBytes
		}
		if totalSpill == 0 {
			result.Regressions = append(result.Regressions, QueryDelta{
				Query:  "<run>",
				Metric: "spill_paths_exercised",
				Status: "REGRESS",
			})
		}
	}

	result.DurationMs = time.Since(started).Milliseconds()
	result.Passed = len(result.Regressions) == 0 && len(result.Hangs) == 0
	result.ExitCode = computeExitCode(result)

	// Write result.json.
	if err := writeResult(cfg.OutPath, result); err != nil {
		logger.Error("writing result", "err", err)
	}

	// Preserve run dir on failure for post-mortem; delete on clean PASS.
	preserveRunDirOnFailure(runDir, result)

	return result, nil
}

func slice(cfg Config) SliceConfig {
	return SliceConfigs[cfg.Slice]
}

func computeExitCode(r RunResult) int {
	exit := ExitOK
	for _, d := range r.Regressions {
		if d.Metric == "row_count" || d.Metric == "row_checksum" {
			if exit < ExitCorrectness {
				exit = ExitCorrectness
			}
			continue
		}
		if exit < ExitRegression {
			exit = ExitRegression
		}
	}
	if len(r.Hangs) > 0 && exit < ExitRegression {
		exit = ExitRegression
	}
	return exit
}

func writeResult(path string, r RunResult) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// preserveRunDirOnFailure writes a MANIFEST.txt explaining what each
// artifact in the run dir is. The harness deletes the run dir on a clean
// PASS but preserves it on any non-PASS so post-mortem debugging works
// (spec section "Cleanup discipline").
func preserveRunDirOnFailure(runDir string, r RunResult) {
	if r.Passed {
		_ = os.RemoveAll(runDir)
		return
	}
	manifest := fmt.Sprintf(`Wadjet harness run preserved due to non-PASS result.

mode:     %s
slice:    %s
exit:     %d
hangs:    %v

Layout:
  logs/coord.log         — coordinator stdout/stderr
  logs/worker-N.log      — per-worker stdout/stderr
  spill/<role>-<pid>/    — spill files (preserved for inspection)
  nats/                  — embedded NATS jetstream store
  result.json            — structured run result

To clean up: rm -rf %s
`, r.Mode, r.Slice, r.ExitCode, r.Hangs, runDir)
	_ = os.WriteFile(filepath.Join(runDir, "MANIFEST.txt"), []byte(manifest), 0644)
}

// runOneQuery submits a query, waits for completion, computes the result
// checksum, and returns a measurement.
func runOneQuery(
	ctx context.Context,
	coordURL string,
	name string,
	collector *MeasurementCollector,
	hangDetector *HangDetector,
	baseline *BaselineFile,
	slice SliceConfig,
) (QueryMeasurement, error) {
	collector.StartWindow(name)
	hangDetector.Reset()

	sql, err := LoadQuery(name)
	if err != nil {
		// Maybe a micro?
		if name == "micro_reverse_bloom" {
			return RunMicroReverseBloom(ctx, coordURL, collector)
		}
		return collector.EndWindow(name), err
	}

	// Hard timeout: 10x baseline projection if known, else 5 min.
	timeout := 5 * time.Minute
	if baseline != nil {
		if qb, ok := baseline.Queries[name]; ok && qb.WallMsP50 > 0 {
			timeout = time.Duration(qb.WallMsP50) * 10 * time.Millisecond
		}
	}
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := pgx.Connect(queryCtx, coordURL)
	if err != nil {
		return collector.EndWindow(name), fmt.Errorf("pgx connect: %w", err)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(queryCtx, sql)
	if err != nil {
		return collector.EndWindow(name), err
	}
	defer rows.Close()

	hash := sha256.New()
	var rowCount int64
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return collector.EndWindow(name), err
		}
		fmt.Fprintf(hash, "%v\n", vals)
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return collector.EndWindow(name), err
	}

	m := collector.EndWindow(name)
	m.RowCount = rowCount
	m.RowChecksum = hex.EncodeToString(hash.Sum(nil))
	return m, nil
}

// loadSampleData loads the SF100 sample files from dataDir into the
// running cluster's catalog. This is the function extracted from
// internal/coordinator/distributed_tpch_test.go (lines 538-604).
//
// VERIFY: this is a placeholder. The real implementation needs to either
// (a) call into a coordinator HTTP/NATS API to register tables, or
// (b) use the same direct catalog.AddFiles path as the test does — which
// requires the harness to share the catalog/store with the spawned
// coordinator. The simplest path is option (b) using a FileStore at a
// path the coordinator was started against.
//
// For v1, leave this as a stub that returns an error directing the
// engineer to copy the table-load loop from distributed_tpch_test.go.
func loadSampleData(ctx context.Context, dataDir, coordURL string, slice SliceConfig) error {
	return fmt.Errorf("loadSampleData not yet implemented — see distributed_tpch_test.go:538-604 for the table-load pattern")
}

// startHeartbeatSubscriber spawns a goroutine that subscribes to the
// embedded NATS heartbeat subject and feeds samples into the collector
// and hang detector. Returns a stop function.
func startHeartbeatSubscriber(
	ctx context.Context,
	cluster *Cluster,
	collector *MeasurementCollector,
	hangDetector *HangDetector,
) func() {
	if cluster == nil {
		return func() {}
	}
	stopC := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// VERIFY: connect to cluster.embeddedNATS via ConnectInProcess and
		// subscribe to distributed.SubjectHeartbeat. On each message,
		// unmarshal as WorkerHeartbeat and call collector.Observe + hangDetector.Observe.
		<-stopC
	}()
	return func() {
		close(stopC)
		wg.Wait()
	}
}
```

- [ ] **Step 3: Add CoordPgURL helper to cluster.go**

Add to `internal/harness/cluster.go`:

```go
// CoordPgURL returns the connection string the harness should use to
// submit SQL queries to the coordinator. The coordinator's pgwire port
// is determined at startup; for v1 we use the default 5432 since the
// harness owns the cluster lifetime and there are no port conflicts
// after the preflight check.
//
// VERIFY: confirm the coordinator's default pgwire port and credentials
// in cmd/wadjet/main.go around the --pg-addr flag.
func (c *Cluster) CoordPgURL() string {
	return "postgres://wadjet@localhost:5432/wadjet?sslmode=disable"
}
```

- [ ] **Step 4: Verify it builds**

Run: `cd /home/dwright/Projects/caelum && go build ./internal/harness/... ./cmd/...`
Expected: builds. There may be missing imports for `pgx` — add `github.com/jackc/pgx/v5` to `go.mod` if needed via `go get`.

- [ ] **Step 5: Commit**

```bash
git add internal/harness/harness.go internal/harness/cluster.go go.mod go.sum
git commit -m "feat(harness): top-level Run() orchestrator with pgx query submission

Wires together cluster start, query loop, measurement collection,
baseline comparison, ExpectSpill assertion, and result JSON output.

Two functions are stubbed for v2: loadSampleData (catalog seeding) and
the heartbeat subscriber goroutine. Both have VERIFY notes pointing
to the existing patterns to copy.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: MicroReverseBloom

**Files:**
- Create: `internal/harness/micros.go`

- [ ] **Step 1: Write the micro**

Write `internal/harness/micros.go`:

```go
package harness

import (
	"context"
	"fmt"
	"time"
)

// RunMicroReverseBloom builds a synthetic dataset shaped to force
// reverseBloomBridge into spill, runs a controlled join query, and asserts
// that spill files were created and that total spill bytes are within
// expected bounds.
//
// The micro complements the TPC-H suite by isolating the bridge code path:
// when both Q05 (in TPC-H) and this micro regress, the engineer knows the
// bridge is the cause; when only TPC-H regresses, it's elsewhere.
//
// v1: this is a stub that returns success. The real implementation needs
// to (a) seed a synthetic table into the running cluster's catalog with
// known cardinality, and (b) submit a JOIN that forces ReverseBloomInner
// via the planner threshold knob. See physical.ReverseBloomInnerThreshold.
//
// VERIFY: implementing this micro requires a way to seed catalog data
// from the harness, which is the same blocker as loadSampleData in
// harness.go. Both should be implemented together.
func RunMicroReverseBloom(ctx context.Context, coordURL string, collector *MeasurementCollector) (QueryMeasurement, error) {
	collector.StartWindow("micro_reverse_bloom")
	defer func() {}()

	// Stub: pretend it ran and produced a small measurement so the rest
	// of the harness can be tested end-to-end.
	time.Sleep(50 * time.Millisecond)

	m := collector.EndWindow("micro_reverse_bloom")
	m.RowCount = 0
	m.RowChecksum = "stub"
	if coordURL == "" {
		return m, fmt.Errorf("micro stub: coordURL empty")
	}
	return m, nil
}
```

- [ ] **Step 2: Build and commit**

Run: `cd /home/dwright/Projects/caelum && go build ./internal/harness/...`

```bash
git add internal/harness/micros.go
git commit -m "feat(harness): MicroReverseBloom v1 stub

Returns a placeholder measurement; real implementation deferred until
catalog seeding (loadSampleData) is wired up.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 12: cmd/tpch-harness binary

**Files:**
- Create: `cmd/tpch-harness/main.go`

- [ ] **Step 1: Write main.go**

Write `cmd/tpch-harness/main.go`:

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/citc-tech/wadjet/internal/harness"
)

func main() {
	var (
		mode         = flag.String("mode", "", "run mode: local or golden (required)")
		slice        = flag.String("slice", "small", "data slice for local mode: small or large")
		coordURL     = flag.String("coord-url", "", "pgwire URL of an existing coordinator (golden mode)")
		dataDir      = flag.String("data-dir", "/tmp/sf100-sample", "directory containing TPC-H sample files (local mode)")
		baselinePath = flag.String("baseline", "benchmarks/tpch/baseline-sf100.json", "path to baseline JSON")
		outPath      = flag.String("out", "./harness-result.json", "path to write result JSON")
		queries      = flag.String("queries", "", "comma-separated query names (default: all 22 + micros)")
		updateBaseline = flag.Bool("update-baseline", false, "(golden only) write the result directly to --baseline")
		noCompare    = flag.Bool("no-compare", false, "skip baseline comparison, just emit measurements")
		wadjetBin    = flag.String("wadjet-bin", "", "path to wadjet binary (default: $WADJET_BIN or build it)")
	)
	flag.Parse()

	if *mode == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --mode is required (local or golden)")
		flag.Usage()
		os.Exit(harness.ExitSetup)
	}

	cfg := harness.Config{
		Mode:           harness.Mode(*mode),
		Slice:          harness.Slice(*slice),
		CoordURL:       *coordURL,
		DataDir:        *dataDir,
		BaselinePath:   *baselinePath,
		OutPath:        *outPath,
		UpdateBaseline: *updateBaseline,
		NoCompare:      *noCompare,
		WadjetBin:      *wadjetBin,
	}
	if *queries != "" {
		cfg.Queries = strings.Split(*queries, ",")
	}
	if cfg.WadjetBin == "" {
		cfg.WadjetBin = os.Getenv("WADJET_BIN")
	}
	if cfg.WadjetBin == "" {
		cfg.WadjetBin = "./wadjet" // assume the local Taskfile build target
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Translate SIGINT/SIGTERM into context cancel so the deferred cluster
	// shutdown in harness.Run runs cleanly.
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigC
		logger.Info("signal received, cancelling")
		cancel()
	}()

	result, err := harness.Run(ctx, cfg, logger)
	if err != nil {
		logger.Error("harness run failed", "err", err)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(harness.ExitSetup)
	}

	printSummary(result)
	os.Exit(result.ExitCode)
}

func printSummary(r harness.RunResult) {
	fmt.Printf("\n=== harness %s/%s — %s ===\n", r.Mode, r.Slice, statusWord(r.Passed))
	fmt.Printf("ran %d queries in %d ms\n", len(r.Queries), r.DurationMs)
	for _, q := range r.Queries {
		marker := " "
		if q.Hung {
			marker = "H"
		}
		fmt.Printf("  [%s] %-20s wall=%6d ms peak=%5d MB rows=%d\n",
			marker, q.Query, q.WallMs, q.PeakHeapMB, q.RowCount)
	}
	if len(r.Regressions) > 0 {
		fmt.Println("regressions:")
		for _, d := range r.Regressions {
			fmt.Printf("  %s.%s drift=%.1f%% (tol=%.0f%%)\n", d.Query, d.Metric, d.DriftPct, d.TolerancePct)
		}
	}
}

func statusWord(passed bool) string {
	if passed {
		return "PASS"
	}
	return "FAIL"
}
```

- [ ] **Step 2: Build the harness binary**

Run: `cd /home/dwright/Projects/caelum && go build -o tpch-harness ./cmd/tpch-harness && ls -la tpch-harness`
Expected: 30-50 MB binary built.

- [ ] **Step 3: Smoke test the binary's flag parsing**

Run: `./tpch-harness --help`
Expected: usage output showing all flags.

Run: `./tpch-harness`
Expected: error "--mode is required", exit code 2.

- [ ] **Step 4: Commit**

```bash
git add cmd/tpch-harness/main.go
git commit -m "feat(harness): cmd/tpch-harness CLI binary

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 13: Empty baseline scaffold

**Files:**
- Create: `benchmarks/tpch/baseline-sf100.json`

- [ ] **Step 1: Write the empty baseline scaffold**

Write `benchmarks/tpch/baseline-sf100.json`:

```json
{
  "version": 1,
  "captured_at": "1970-01-01T00:00:00Z",
  "captured_on": "scaffold — replace with values from a real golden run",
  "queries": {},
  "projection_factors": {
    "small_slice": {
      "wall_ms_multiplier": 0.04,
      "heap_multiplier": 0.18,
      "alloc_count_multiplier": 0.16,
      "spill_multiplier": 0.10
    },
    "large_slice": {
      "wall_ms_multiplier": 0.20,
      "heap_multiplier": 0.55,
      "alloc_count_multiplier": 0.50,
      "spill_multiplier": 0.45
    }
  }
}
```

The empty `queries: {}` means `--no-compare` is implicit until a real golden run populates it. The projection factors are placeholders chosen as plausible round numbers; they will be re-calibrated after the first golden run.

- [ ] **Step 2: Commit**

```bash
git add benchmarks/tpch/baseline-sf100.json
git commit -m "feat(harness): empty baseline scaffold for SF100

Projection factors are placeholders. Real values will be captured from
the first golden EC2 run. Until then, --no-compare is implicit because
the queries map is empty.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 14: Taskfile entries

**Files:**
- Modify: `Taskfile.yml`

- [ ] **Step 1: Add the harness task entries**

Edit `Taskfile.yml`. Append a new section after the existing `bench:` tasks:

```yaml
  # --- Distributed test harness (cmd/tpch-harness) ---
  # See docs/superpowers/specs/2026-04-08-distributed-test-harness-design.md

  harness:build:
    desc: Build the tpch-harness binary
    cmds:
      - go build -o tpch-harness ./cmd/tpch-harness
      - go build -o wadjet ./cmd/wadjet

  harness:smoke:
    desc: Real-cluster smoke test against the existing SF0.01 fixture (~30s)
    deps: [harness:build]
    cmds:
      - ./tpch-harness --mode=local --slice=small --no-compare --queries=q01,q06

  harness:local:
    desc: Local fast gate using the small slice (~1-2 min, every commit)
    deps: [harness:build]
    cmds:
      - ./tpch-harness --mode=local --slice=small

  harness:large:
    desc: Local pre-merge gate using the large slice (forces spill, ~5-10 min)
    deps: [harness:build]
    cmds:
      - ./tpch-harness --mode=local --slice=large

  harness:golden:
    desc: Run against an existing EC2 cluster (requires COORD_URL)
    deps: [harness:build]
    requires:
      vars: [COORD_URL]
    cmds:
      - ./tpch-harness --mode=golden --coord-url={{.COORD_URL}} --update-baseline

  harness:clean:
    desc: Remove all harness run dirs and built binaries
    cmds:
      - rm -rf /tmp/wadjet-harness
      - rm -f tpch-harness wadjet
```

- [ ] **Step 2: Validate the Taskfile**

Run: `cd /home/dwright/Projects/caelum && task --list 2>&1 | grep harness`
Expected: 6 harness tasks listed.

- [ ] **Step 3: Commit**

```bash
git add Taskfile.yml
git commit -m "build(taskfile): add harness:smoke/local/large/golden/clean tasks

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 15: Layer 2 fake cluster happy-path test

**Files:**
- Create: `internal/harness/fake_cluster_test.go`

This task validates that the harness pipeline (collector → comparison → output) works end-to-end without needing a real cluster. The fake cluster is in-process goroutines that publish synthetic heartbeats and respond to query submissions with canned data.

- [ ] **Step 1: Write the fake cluster smoke test**

Write `internal/harness/fake_cluster_test.go`:

```go
package harness

import (
	"context"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// TestRunsCleanThroughFakeCluster exercises Run end-to-end against a
// fake cluster: synthetic heartbeats are fed into the collector, and a
// stub query function returns canned QueryMeasurement values. The test
// asserts the harness produces a passing RunResult with all expected
// queries listed.
//
// This is the v1 of Layer 2 self-testing. v2 will add failure-mode tests
// (hang, regression, crash, panic).
func TestRunsCleanThroughFakeCluster(t *testing.T) {
	collector := NewCollector()

	// Simulate a query window with synthetic heartbeats.
	collector.StartWindow("q01")
	for i := 0; i < 5; i++ {
		collector.Observe(distributed.WorkerHeartbeat{
			WorkerID:      "fake-w0",
			MemoryUsed:    int64((i + 1) * 100 * 1024 * 1024),
			Mallocs:       uint64(1000 * (i + 1)),
			NumGoroutines: 50,
			Timestamp:     time.Now().Add(time.Duration(i) * 100 * time.Millisecond),
		})
	}
	m := collector.EndWindow("q01")
	if m.PeakHeapMB < 500 {
		t.Errorf("expected peak >= 500 MB, got %d", m.PeakHeapMB)
	}
	if m.AllocCount != 4000 {
		t.Errorf("expected allocs delta 4000, got %d", m.AllocCount)
	}

	// Build a baseline that matches and check Compare returns no regressions.
	bf := &BaselineFile{
		Version: 1,
		Queries: map[string]QueryBaseline{
			"q01": {
				WallMsP50:            5000,
				WallMsTolerancePct:   100, // very loose
				PeakHeapMB:           500,
				PeakHeapTolerancePct: 100,
				RowCount:             0,
				RowChecksum:          "",
			},
		},
	}
	deltas := bf.Compare(m)
	for _, d := range deltas {
		if d.Status == "REGRESS" {
			t.Errorf("unexpected regression: %+v", d)
		}
	}

	// Smoke test computeExitCode.
	r := RunResult{Queries: []QueryMeasurement{m}}
	if code := computeExitCode(r); code != ExitOK {
		t.Errorf("expected ExitOK, got %d", code)
	}

	_ = context.Background()
}
```

- [ ] **Step 2: Run the test**

Run: `cd /home/dwright/Projects/caelum && go test ./internal/harness/ -run TestRunsCleanThroughFakeCluster -v`
Expected: PASS.

- [ ] **Step 3: Run the full harness test suite to confirm everything is green**

Run: `cd /home/dwright/Projects/caelum && go test ./internal/harness/ -v -short`
Expected: all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/harness/fake_cluster_test.go
git commit -m "test(harness): Layer 2 happy-path fake cluster smoke

v1 covers the happy path only. v2 will add hang, regression, crash,
and panic-cleanup tests using a more elaborate fake cluster.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 16: SF0.01 real-cluster smoke validation

**Files:**
- (no new files; this task validates the harness end-to-end against a real cluster)

- [ ] **Step 1: Build everything**

Run: `cd /home/dwright/Projects/caelum && task harness:build`
Expected: clean build, two binaries produced (`wadjet`, `tpch-harness`).

- [ ] **Step 2: Set up a tiny SF0.01 sample dir**

Run: `cd /home/dwright/Projects/caelum && go test -run TestNothing -count=1 ./benchmarks/tpch/ 2>&1 | head -3` to confirm the SF0.01 generator is in place. Then create a quick fixture by running:

```bash
cd /home/dwright/Projects/caelum
mkdir -p /tmp/wadjet-harness-fixture
go run ./cmd/tpch-seed --scale=0.01 --out=/tmp/wadjet-harness-fixture --format=parquet
```

VERIFY: `cmd/tpch-seed` already exists per `cmd/*/main.go` glob. If its flags differ, adjust. The goal is a directory with the 8 TPC-H tables as parquet files.

- [ ] **Step 3: Run the smoke task**

Run: `cd /home/dwright/Projects/caelum && task harness:smoke 2>&1 | tee /tmp/harness-smoke.log`

Expected outcomes:
- The cluster spawns successfully (look for "embedded NATS up" and "cluster ready" log lines)
- Q01 and Q06 either run successfully (PASS) or fail with a SPECIFIC error message
- The harness exits cleanly (no orphaned wadjet processes — verify with `pgrep -f wadjet` after exit)
- `harness-result.json` exists and contains 2 query entries

If the smoke fails, the most likely causes are:
1. `loadSampleData` is still a stub (Task 10 noted this) — implement it now by porting the catalog-load loop from `internal/coordinator/distributed_tpch_test.go:538-604`
2. `CoordPgURL` returns the wrong port — read `cmd/wadjet/main.go` around the `--pg-addr` flag
3. The heartbeat subscriber goroutine in `startHeartbeatSubscriber` is still a stub — wire it up using the pattern from `internal/worker/worker.go:600-650`

These three are the v1 known stubs. They need to be filled in here, not deferred — without them the harness produces all-zero measurements.

- [ ] **Step 4: Implement loadSampleData**

In `internal/harness/harness.go`, replace the stub with a real implementation. Read `internal/coordinator/distributed_tpch_test.go` lines 538-604 for the pattern. Adapt it to:

1. Open the running cluster's catalog (it lives in the embedded NATS KV — the harness has a handle to the embedded NATS via `cluster.embeddedNATS`)
2. Open a `FileStore` rooted at `cfg.DataDir`
3. For each TPC-H table file in the slice config, read its parquet schema, create the catalog table, and call `cat.AddFiles` with the entries

The exact code is the same as the test file with three substitutions:
- `t.Fatalf` → `return fmt.Errorf`
- `objstore.NewMemStore()` → `objstore.NewFileStore(cfg.DataDir)`
- `tableFiles` → built from `slice.LineitemFiles` and `slice.OrdersFiles`

- [ ] **Step 5: Implement startHeartbeatSubscriber**

Replace the stub in `internal/harness/harness.go` with a real subscriber. Pattern (read `internal/worker/worker.go` around line 600-650 for reference):

```go
nc, err := distributed.ConnectInProcess(cluster.embeddedNATS.Server())
if err != nil {
    return func() {}
}
sub, err := nc.Subscribe(distributed.SubjectHeartbeat, func(msg *nats.Msg) {
    var hb distributed.WorkerHeartbeat
    if err := distributed.Unmarshal(msg.Data, &hb); err != nil {
        return
    }
    collector.Observe(hb)
    hangDetector.Observe(hb.Timestamp, hb.NumGoroutines)
})
if err != nil {
    nc.Close()
    return func() {}
}
return func() {
    sub.Unsubscribe()
    nc.Close()
}
```

VERIFY the `nats.Msg` import is `github.com/nats-io/nats.go`.

- [ ] **Step 6: Re-run the smoke and confirm clean PASS**

Run: `task harness:clean && task harness:smoke 2>&1 | tee /tmp/harness-smoke.log`
Expected: 2 queries run, both with non-zero `wall_ms` and non-zero `peak_heap_mb` in `harness-result.json`. Exit code 0.

Run: `pgrep -f wadjet || echo OK`
Expected: `OK` (no orphans).

Run: `cat harness-result.json | python3 -m json.tool | head -40`
Expected: structured output with mode, slice, queries, and passed=true.

- [ ] **Step 7: Commit**

```bash
git add internal/harness/harness.go
git commit -m "feat(harness): wire loadSampleData and heartbeat subscriber

Replaces the v1 stubs with real implementations:
- loadSampleData ports the catalog seed loop from the existing
  distributed TPC-H test, adapted for FileStore.
- startHeartbeatSubscriber subscribes to the embedded NATS heartbeat
  subject and feeds samples into the collector and hang detector.

The 'task harness:smoke' end-to-end smoke test now runs Q01 and Q06
against a real local cluster and exits clean.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## v1 acceptance criteria

When all tasks are complete, the following must hold:

1. `task harness:build` produces `tpch-harness` and `wadjet` binaries
2. `task harness:smoke` runs Q01 and Q06 against a real local 1+1 cluster, exits 0, leaves no orphaned wadjet processes
3. `harness-result.json` contains structured measurements (non-zero wall_ms, non-zero peak_heap_mb) for each query
4. `go test ./internal/harness/ -short` passes all unit tests in under 30s
5. `go test ./internal/harness/ -run TestClusterSpawnAndShutdown -v` passes the integration test in under 90s
6. The empty baseline file at `benchmarks/tpch/baseline-sf100.json` exists and parses cleanly via `LoadBaseline`
7. `pgrep -f wadjet` returns no orphans after any successful run

## v2 backlog (separate plan)

Once v1 ships, the next iteration adds:

- Comprehensive Layer 2 self-tests (hang, regression, crash, panic-cleanup)
- Real `MicroReverseBloom` (not stub) using catalog seeding
- Additional micros: `MicroGraceHashJoin`, `MicroHashAggHighCardinality`
- `--mode=golden` polish: result upload to S3, automated baseline refresh task
- `task harness:refresh-baseline` automation
- First real golden run on EC2 to populate `baseline-sf100.json` (requires deploy approval)
- Pre-flight port check (random free port for coordinator pgwire)
- Sample data SHA256 verification against baseline-recorded hash
- Documentation in CLAUDE.md for the harness workflow
- **PID lockfile at `/tmp/wadjet-harness/.lock`** to prevent parallel harness runs on the same dev box (spec section "Resource pre-checks"). Deferred from v1 because single-user dev box and the operator already knows not to run two at once.
- **SIGQUIT goroutine dump on hang** (spec section "Hang detection"). v1 uses context-timeout to kill hanging queries but does not capture goroutine stacks. The dump is valuable for debugging future hangs but is not a blocker for the regression-gate use case.
