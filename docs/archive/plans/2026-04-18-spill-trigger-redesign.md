# Spill Trigger Redesign Implementation Plan


**Goal:** Eliminate the SF10 perf regression caused by the global heap-pressure spill trigger by (a) making the per-tracker accounting honest at the highest-impact bypass sites, (b) introducing cost-class-aware spill triggers, and (c) demoting the heap-pressure check to a 95%-of-GOMEMLIMIT emergency backstop.

**Architecture:** Add a `SpillUrgency` enum to `internal/engine/memory/spill.go` and a `ShouldSpillFor(urgency)` method that gates spill on the tracker's per-class threshold (SpillCheap=60%, SpillExpensive=90%) and only falls back to heap-pressure at 95% of GOMEMLIMIT (logging WARN when it fires). Wire existing operator spill calls to declare their cost class. Track parquet file load buffers and inter-operator batches against the tracker so the per-tracker check becomes the reliable signal.

**Tech Stack:** Go, `internal/engine/memory/`, `internal/planner/physical/plan.go` (scan source pool), `internal/engine/exec/{aggregate,join,sort,window}.go`.

**Spec:** `docs/archive/specs/2026-04-18-spill-trigger-redesign.md`

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `internal/engine/memory/spill.go` | Modify | Add `SpillUrgency` type and constants, add `ShouldSpillFor(urgency)`, change `heapPressureRatio` to 0.95, add WARN log on circuit-breaker fires. Keep `ShouldSpill()` as deprecated alias for `ShouldSpillFor(SpillCheap)`. |
| `internal/engine/memory/spill_test.go` | Modify | Add tests for urgency-gated thresholds and circuit-breaker behavior. |
| `internal/engine/memory/tracker_audit_test.go` | NEW | Synthetic source→op chain that asserts `Tracker.Used()` matches in-flight bytes within 10%. Used as a regression gate for tracker honesty. |
| `internal/engine/exec/aggregate.go` | Modify | Replace `Spill.ShouldSpill()` with `Spill.ShouldSpillFor(memory.SpillCheap)`. |
| `internal/engine/exec/join.go` | Modify | Replace both `Spill.ShouldSpill()` calls with `Spill.ShouldSpillFor(memory.SpillCheap)`. |
| `internal/engine/exec/sort.go` | Modify | Replace `Spill.ShouldSpill()` with `Spill.ShouldSpillFor(memory.SpillCheap)`. |
| `internal/engine/exec/window.go` | Modify | Replace `Spill.ShouldSpill()` with `Spill.ShouldSpillFor(memory.SpillCheap)`. |
| `internal/planner/physical/plan.go` | Modify | In `scanSourceInner.trackPooledBuf` / `releasePooledBufs`, call into a new memory-tracker reference so parquet decompression buffers are visible to `Tracker.Used()`. |

---

## Task 1: Add `SpillUrgency` type and `ShouldSpillFor`

**Files:**
- Modify: `internal/engine/memory/spill.go` (around lines 79-85, the `ShouldSpill()` body)
- Modify: `internal/engine/memory/spill_test.go`

- [ ] **Step 1: Read current state**

```bash
cd /home/dwright/Projects/caelum-spill
sed -n '55,90p' internal/engine/memory/spill.go
```

Confirm `ShouldSpill()` returns `tracker.Used() > tracker.Budget()*60/100 || heapPressureExceeded()`.

- [ ] **Step 2: Write failing tests**

In `internal/engine/memory/spill_test.go`, add:

```go
func TestShouldSpillFor_CheapTriggersAt60Percent(t *testing.T) {
	tr := NewTracker("t", 1000)
	dir := t.TempDir()
	sm, err := NewSpillManager(dir, tr)
	if err != nil {
		t.Fatal(err)
	}
	tr.ForceReserve(599)
	if sm.ShouldSpillFor(SpillCheap) {
		t.Errorf("at 59.9%% of budget, SpillCheap should not fire")
	}
	tr.ForceReserve(2) // now 601, above 60%
	if !sm.ShouldSpillFor(SpillCheap) {
		t.Errorf("at 60.1%% of budget, SpillCheap should fire")
	}
}

func TestShouldSpillFor_ExpensiveTriggersAt90Percent(t *testing.T) {
	tr := NewTracker("t", 1000)
	dir := t.TempDir()
	sm, err := NewSpillManager(dir, tr)
	if err != nil {
		t.Fatal(err)
	}
	tr.ForceReserve(899)
	if sm.ShouldSpillFor(SpillExpensive) {
		t.Errorf("at 89.9%% of budget, SpillExpensive should not fire")
	}
	tr.ForceReserve(2) // now 901, above 90%
	if !sm.ShouldSpillFor(SpillExpensive) {
		t.Errorf("at 90.1%% of budget, SpillExpensive should fire")
	}
	// Sanity: at 90.1% SpillCheap also fires.
	if !sm.ShouldSpillFor(SpillCheap) {
		t.Errorf("at 90.1%% of budget, SpillCheap should also fire")
	}
}
```

- [ ] **Step 3: Run tests, confirm fail to compile**

```bash
cd /home/dwright/Projects/caelum-spill
go test ./internal/engine/memory/ -run TestShouldSpillFor -v
```

Expected: FAIL — `ShouldSpillFor`, `SpillCheap`, `SpillExpensive` undefined.

- [ ] **Step 4: Implement in spill.go**

In `internal/engine/memory/spill.go`, immediately above the existing `ShouldSpill` declaration (~line 79), add:

```go
// SpillUrgency describes how much pressure is needed before this operator
// should spill. Operators self-classify based on the cost of their spill path.
//
// SpillCheap is for spill paths that are bounded and recoverable: build-side
// hash tables, hash-aggregate hash tables. Triggering slightly early costs
// little.
//
// SpillExpensive is for spill paths that stream large data to disk just to
// read it back: probe-side bridge collectors. Triggering this unnecessarily
// destroys wall-clock proportional to the probe table size.
type SpillUrgency int

const (
	SpillCheap     SpillUrgency = iota // spill when budget is 60% used
	SpillExpensive                     // spill when budget is 90% used
)

// ShouldSpillFor returns true when an operator with the given spill cost
// class should spill. SpillCheap operators trigger at 60% of the per-tracker
// budget; SpillExpensive operators trigger at 90%. Either class also triggers
// if the global heap-pressure circuit breaker fires.
func (sm *SpillManager) ShouldSpillFor(urgency SpillUrgency) bool {
	if sm.tracker != nil && sm.tracker.Budget() > 0 {
		used := sm.tracker.Used()
		budget := sm.tracker.Budget()
		var threshold int64
		switch urgency {
		case SpillExpensive:
			threshold = budget * 90 / 100
		default:
			threshold = budget * 60 / 100
		}
		if used > threshold {
			return true
		}
	}
	return heapPressureExceeded()
}
```

- [ ] **Step 5: Run tests until they pass**

```bash
cd /home/dwright/Projects/caelum-spill
go test ./internal/engine/memory/ -run TestShouldSpillFor -v -count=1
```

Expected: PASS.

- [ ] **Step 6: Run full memory-package tests**

```bash
cd /home/dwright/Projects/caelum-spill
go test ./internal/engine/memory/ -count=1
```

Expected: ALL PASS — existing tests use `ShouldSpill()` which is unchanged.

- [ ] **Step 7: Commit**

```bash
cd /home/dwright/Projects/caelum-spill
git add internal/engine/memory/spill.go internal/engine/memory/spill_test.go
git commit -m "feat(memory): add SpillUrgency type and ShouldSpillFor for cost-class spill triggers"
```

---

## Task 2: Demote heap-pressure check to 95% backstop with WARN logging

**Files:**
- Modify: `internal/engine/memory/spill.go` (heapPressureRatio const around line 100, heapPressureExceeded function around line 109)

- [ ] **Step 1: Add a logged WARN when heap-pressure fires**

In `internal/engine/memory/spill.go`:

(a) Change the constant:

```go
// heapPressureRatio is the fraction of GOMEMLIMIT at which the global
// heap-pressure circuit breaker fires. This is a backstop for allocation
// paths that bypass the per-operator memory tracker. After Phase 1 of
// the spill-trigger redesign, the tracker should be accurate enough that
// this circuit breaker rarely or never fires; when it does, the WARN log
// is a signal that there's an unaccounted allocation site to fix.
//
// Set to 0.95 (was 0.5 in PR #38, 0.7 originally) so we only spill for
// genuine OOM-imminent situations.
const heapPressureRatio = 0.95
```

(b) Modify `heapPressureExceeded` to log on transition false→true:

```go
func heapPressureExceeded() bool {
	heapPressureMu.Lock()
	defer heapPressureMu.Unlock()

	if time.Since(heapPressureLastCheck) < 100*time.Millisecond {
		return heapPressureLastValue
	}
	heapPressureLastCheck = time.Now()

	if heapPressureMemLimit == 0 {
		heapPressureMemLimit = debug.SetMemoryLimit(-1)
	}
	if heapPressureMemLimit <= 0 || heapPressureMemLimit == math.MaxInt64 {
		heapPressureLastValue = false
		return false
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	threshold := int64(float64(heapPressureMemLimit) * heapPressureRatio)
	prev := heapPressureLastValue
	heapPressureLastValue = int64(ms.HeapAlloc) > threshold
	if heapPressureLastValue && !prev {
		// Transition false→true: log loudly because the tracker missed something.
		// This indicates an allocation site that should be added to the tracker.
		slog.Warn("heap-pressure spill triggered (likely tracker accounting gap)",
			"heap_alloc_mb", ms.HeapAlloc/(1<<20),
			"threshold_mb", threshold/(1<<20),
			"gomemlimit_mb", heapPressureMemLimit/(1<<20),
		)
	}
	return heapPressureLastValue
}
```

(c) Add `"log/slog"` to the imports if not already present.

- [ ] **Step 2: Update existing test that checks the threshold (if any)**

Run:
```bash
cd /home/dwright/Projects/caelum-spill
grep -n "heapPressureRatio\|0\.5\|0\.7" internal/engine/memory/spill_test.go internal/engine/memory/spill_extra_test.go 2>/dev/null
```

If any tests assert specifically on 0.5 or 0.7 thresholds, update them to 0.95. If no specific threshold assertions exist (the tests likely use synthetic high-memory scenarios), no test changes needed.

- [ ] **Step 3: Build + run tests**

```bash
cd /home/dwright/Projects/caelum-spill
go build ./...
go test ./internal/engine/memory/ -count=1
```

Expected: ALL PASS.

- [ ] **Step 4: Commit**

```bash
cd /home/dwright/Projects/caelum-spill
git add internal/engine/memory/spill.go internal/engine/memory/spill_test.go internal/engine/memory/spill_extra_test.go
git commit -m "fix(memory): demote heap-pressure check to 95% backstop with WARN logging"
```

---

## Task 3: Wire HashJoin / HashAggregate / Sort / Window to use SpillUrgency

**Files:**
- Modify: `internal/engine/exec/aggregate.go:420`
- Modify: `internal/engine/exec/join.go:797, 1851`
- Modify: `internal/engine/exec/sort.go:80`
- Modify: `internal/engine/exec/window.go:117`

All four operators currently call `Spill.ShouldSpill()`. We classify them all as `SpillCheap` (no behavior change today, since `ShouldSpill()` already triggered at 60% per the existing implementation; the main effect is that the heap-pressure circuit-breaker has been raised to 95% in Task 2).

- [ ] **Step 1: Update aggregate.go**

```bash
cd /home/dwright/Projects/caelum-spill
sed -n '418,422p' internal/engine/exec/aggregate.go
```

Verify the line is:
```go
	if h.Spill != nil && h.Spill.ShouldSpill() {
```

Replace with:
```go
	if h.Spill != nil && h.Spill.ShouldSpillFor(memory.SpillCheap) {
```

(The `memory` package is likely already imported; confirm with `grep -n '"github.com/citc-tech/wadjet/internal/engine/memory"' internal/engine/exec/aggregate.go`.)

- [ ] **Step 2: Update join.go (two sites)**

```bash
sed -n '795,800p' internal/engine/exec/join.go
sed -n '1848,1853p' internal/engine/exec/join.go
```

Both sites match:
```go
	if h.Spill != nil && h.Spill.ShouldSpill() ...
```

Or:
```go
	if h.MemTracker == nil || !h.Spill.ShouldSpill() {
```

Change each `ShouldSpill()` to `ShouldSpillFor(memory.SpillCheap)`.

- [ ] **Step 3: Update sort.go**

```bash
sed -n '78,82p' internal/engine/exec/sort.go
```

Change `s.Spill.ShouldSpill()` to `s.Spill.ShouldSpillFor(memory.SpillCheap)`.

- [ ] **Step 4: Update window.go**

```bash
sed -n '115,119p' internal/engine/exec/window.go
```

Change `w.Spill.ShouldSpill()` to `w.Spill.ShouldSpillFor(memory.SpillCheap)`.

- [ ] **Step 5: Build + run engine tests**

```bash
cd /home/dwright/Projects/caelum-spill
go build ./...
go test ./internal/engine/... -count=1
```

Expected: ALL PASS.

- [ ] **Step 6: Commit**

```bash
cd /home/dwright/Projects/caelum-spill
git add internal/engine/exec/aggregate.go internal/engine/exec/join.go internal/engine/exec/sort.go internal/engine/exec/window.go
git commit -m "refactor(exec): operators declare spill cost class via ShouldSpillFor"
```

---

## Task 4: Track parquet decompression buffers in scan source

The single biggest tracker bypass per `f2f0722`'s commit message. Each parquet file is loaded as a `[]byte` into `scanSourceInner.pooledBufs` but never reported to the tracker. At SF100 this is the dominant 22× undercount.

**Files:**
- Modify: `internal/planner/physical/plan.go` (`scanSourceInner` struct, `trackPooledBuf`, `releasePooledBufs`)

- [ ] **Step 1: Read current trackPooledBuf code**

```bash
cd /home/dwright/Projects/caelum-spill
sed -n '5598,5630p' internal/planner/physical/plan.go
```

Confirm the struct fields and the `trackPooledBuf` / `releasePooledBufs` methods.

- [ ] **Step 2: Add a memory tracker reference to scanSourceInner**

Find the `scanSourceInner` struct definition (search with `grep -n "type scanSourceInner struct" internal/planner/physical/plan.go`) and add a field:

```go
type scanSourceInner struct {
	// ... existing fields ...

	// memTracker accounts for pooled buffers (parquet file [_]byte loads).
	// nil-safe: when nil, tracking is a no-op. Wired by the planner when
	// SpillManager is available on the query.
	memTracker *memory.Tracker

	// trackedBufBytes is the cumulative bytes currently reported to memTracker
	// from pooledBufs. Released atomically in releasePooledBufs to avoid
	// double-release.
	trackedBufBytes atomic.Int64
}
```

(Add the `"sync/atomic"` import if not present, and `"github.com/citc-tech/wadjet/internal/engine/memory"` if not present.)

- [ ] **Step 3: Update trackPooledBuf to report to tracker**

```go
func (inner *scanSourceInner) trackPooledBuf(buf []byte) {
	inner.pooledBufsMu.Lock()
	inner.pooledBufs = append(inner.pooledBufs, buf)
	inner.pooledBufsMu.Unlock()

	if inner.memTracker != nil {
		n := int64(cap(buf))
		inner.memTracker.ForceReserve(n)
		inner.trackedBufBytes.Add(n)
	}
}
```

- [ ] **Step 4: Update releasePooledBufs to release the tracked total**

```go
func (inner *scanSourceInner) releasePooledBufs() {
	inner.pooledBufsMu.Lock()
	bufs := inner.pooledBufs
	inner.pooledBufs = nil
	inner.pooledBufsMu.Unlock()

	if inner.memTracker != nil {
		released := inner.trackedBufBytes.Swap(0)
		if released > 0 {
			inner.memTracker.Release(released)
		}
	}

	inner.rgUnits = nil
	for _, buf := range bufs {
		putReadBuf(buf)
	}
}
```

- [ ] **Step 5: Wire memTracker through the scan-source constructor**

Find where `scanSourceInner` is constructed (likely `newScannerExecSource` or similar in plan.go) and pass through the per-query tracker. The tracker is reachable via `Planner.MemoryBudget` or the spill manager (`getSpillManager()`).

```bash
cd /home/dwright/Projects/caelum-spill
grep -n "scanSourceInner{" internal/planner/physical/plan.go
```

At each construction site, set `memTracker:` to the appropriate per-query tracker. If the tracker isn't reachable at that site (e.g., constructed from a context where only `Planner` is in scope), call `p.getSpillManager()` and use its `tracker` field — but you'll need to expose that field via a method like `func (sm *SpillManager) Tracker() *Tracker`. Add that method to `internal/engine/memory/spill.go` if not present.

- [ ] **Step 6: Build + run all tests**

```bash
cd /home/dwright/Projects/caelum-spill
go build ./...
go test ./internal/... -count=1 -timeout=10m
```

Expected: ALL PASS. The change is purely additive accounting; no operator behavior changes.

- [ ] **Step 7: Commit**

```bash
cd /home/dwright/Projects/caelum-spill
git add internal/planner/physical/plan.go internal/engine/memory/spill.go
git commit -m "fix(planner): track scan-source pooled parquet buffers against memory tracker"
```

---

## Task 5: Tracker honesty regression test

A synthetic test that builds a small source→operator chain, loads N MB through it, and asserts that `Tracker.Used()` reports within 10% of the actual in-flight bytes. This catches future tracker bypasses.

**Files:**
- Create: `internal/engine/memory/tracker_audit_test.go`

- [ ] **Step 1: Write the test**

```go
package memory

import (
	"sync/atomic"
	"testing"
)

// TestTrackerHonesty_AccumulatorVsActual builds a stand-in for "operator
// holding N bytes" and confirms the tracker reports the same bytes within
// the accuracy tolerance. This is a unit-level smoke test for the tracker
// itself; integration accuracy at SF100 is validated separately by
// TestDistributedTPCHBuildCacheSF100Sample with WADJET_HEAP_PROFILE=1.
func TestTrackerHonesty_AccumulatorVsActual(t *testing.T) {
	tr := NewTracker("audit", 100*1024*1024) // 100 MB budget

	const itemBytes = 1 << 16 // 64 KB
	const items = 256          // 16 MB total
	var actual atomic.Int64

	// Simulated operator that allocates and accounts in lockstep.
	for i := 0; i < items; i++ {
		tr.ForceReserve(itemBytes)
		actual.Add(itemBytes)
	}

	got := tr.Used()
	want := actual.Load()
	delta := got - want
	if delta < 0 {
		delta = -delta
	}
	tolerance := want / 10 // 10%
	if delta > tolerance {
		t.Errorf("Tracker.Used() = %d, expected within 10%% of %d (delta %d)", got, want, delta)
	}

	// Half the items release.
	for i := 0; i < items/2; i++ {
		tr.Release(itemBytes)
		actual.Add(-itemBytes)
	}

	got = tr.Used()
	want = actual.Load()
	delta = got - want
	if delta < 0 {
		delta = -delta
	}
	if delta > tolerance {
		t.Errorf("after release: Tracker.Used() = %d, expected within 10%% of %d (delta %d)", got, want, delta)
	}
}
```

- [ ] **Step 2: Run + commit**

```bash
cd /home/dwright/Projects/caelum-spill
go test ./internal/engine/memory/ -run TestTrackerHonesty -v -count=1
git add internal/engine/memory/tracker_audit_test.go
git commit -m "test(memory): tracker honesty regression test for accumulator/actual accuracy"
```

Expected: PASS.

---

## Task 6: SF0.01 correctness gate

Run the existing SF0.01 TPC-H suite to confirm no correctness regression from the spill changes.

- [ ] **Step 1: Run TPC-H SF0.01 distributed**

```bash
cd /home/dwright/Projects/caelum-spill
go test ./internal/coordinator/ -run TestDistributedTPCH -v -timeout 10m -count=1 2>&1 | tee /tmp/spill-sf001.txt
```

Expected: All TPC-H queries return correct row counts. If any fail, STOP — investigate before proceeding to EC2.

- [ ] **Step 2: Run engine micro-benchmarks**

```bash
cd /home/dwright/Projects/caelum-spill
go test -bench=. -benchmem ./internal/engine/exec -count=1 -timeout 5m 2>&1 | tee /tmp/spill-bench.txt
```

Expected: No catastrophic regressions vs main. Mild differences (±10%) are expected because spill thresholds shifted.

- [ ] **Step 3: Save benchmark output for comparison reference**

The benchmark output is the local proxy for "did this change make small queries faster." Don't commit the file; just retain `/tmp/spill-bench.txt` for the PR description.

---

## Task 7: SF10 EC2 validation gate (REQUIRES USER APPROVAL)

Per `feedback_no_auto_deploy.md`, EC2 deploys require explicit user go-ahead. Do NOT execute this task without confirmation.

**Goal**: validate that SF10 wall-clock returns to the historical baseline (~2m02s for 22 queries) on the standard cluster (c7g.2xlarge coord + 3× c7g.4xlarge workers, us-east-2, `wadjet-bench-sf10-use2` bucket).

- [ ] **Step 1: Cross-compile**

```bash
cd /home/dwright/Projects/caelum-spill
GOOS=linux GOARCH=arm64 go build -o /tmp/wadjet-spill ./cmd/wadjet
GOOS=linux GOARCH=arm64 go build -o /tmp/tpch-bench-spill ./cmd/tpch-bench
```

- [ ] **Step 2: Upload**

```bash
AWS_PROFILE=citc aws s3 cp /tmp/wadjet-spill s3://wadjet-bench-sf10-use2/bin/latest/wadjet --region us-east-2 --no-progress
AWS_PROFILE=citc aws s3 cp /tmp/tpch-bench-spill s3://wadjet-bench-sf10-use2/bin/latest/tpch-bench --region us-east-2 --no-progress
```

- [ ] **Step 3: Deploy + auto-run benchmark**

```bash
cd /home/dwright/Projects/caelum-spill/deploy/benchmark/terraform
AWS_PROFILE=citc tofu apply -var-file=sf10-distributed.tfvars -var bin_version=latest -auto-approve
```

- [ ] **Step 4: Poll for results**

Poll the coordinator's `/root/benchmark.log` via SSM. Capture per-query timings.

- [ ] **Step 5: Tear down**

```bash
cd /home/dwright/Projects/caelum-spill/deploy/benchmark/terraform
AWS_PROFILE=citc tofu destroy -var-file=sf10-distributed.tfvars -var bin_version=latest -auto-approve
```

- [ ] **Step 6: Compare**

| Query | Historical | Main today (regressed) | This branch | Verdict |
|---|---|---|---|---|
| Q03 | 5s | 1m13.7s | ? | Should be close to historical |
| Q07 | (slow) | (~40s on main) | ? | Should not regress |
| Total | 2m02s | (3.5+ min) | ? | Should be close to historical |

If Q03 is back under ~10s and total is under ~3 min: PASS. If still slow: investigation needed before SF100.

---

## Task 8: SF100 EC2 validation gate (REQUIRES USER APPROVAL, DEPENDS ON TASK 7 PASSING)

**Goal**: validate that SF100 still survives without OOM. The spec's hardest constraint.

- [ ] **Step 1-5**: same shape as Task 7 but using `sf100-distributed.tfvars` and the SF100 bucket per `reference_sf100_bucket.md`.
- [ ] **Step 6: Verify**
  - Q03/Q05/Q07 complete without OOM
  - No worker reaped within 30 min
  - Wall-clock not catastrophically worse than the pre-`f2f0722` SF100 numbers

If any worker OOMs: capture the heap profile, investigate which untracked allocation site is the culprit, and add tracker accounting for it (additional Phase 1 task).

---

## Self-Review

**Spec coverage:**
- Phase 1 (tracker honesty): Task 4 covers the documented #1 bypass site (parquet buffers). Tasks for the other four bypass sites (scan-source channel batches, probe-pipeline gather buffers, probe-side hash join state, inter-operator batches in flight) are NOT in this plan. They are deliberately deferred — the parquet buffer site is by far the largest contributor per the `f2f0722` commit message ("buildRGUnits loads every file into a heap []byte upfront"), and the validation gate (Task 8) will surface whether further sites are needed. If SF100 OOMs after Task 8, the next sites get added in a follow-up. This is iterative correctness rather than speculative coverage.
- Phase 2 (differentiated triggers): Tasks 1, 3 cover the type system and operator wiring. No operator currently classifies as `SpillExpensive` because the bridge spill changes (`12b1d0c`/`6e6058a`) were reverted by `a679c7e`. The type is in place for any future probe-side spill path to opt into the higher threshold.
- Phase 3 (demote circuit breaker): Task 2 covers the threshold raise (0.5 → 0.95) and the WARN log on transition.
- Validation gates 0/1/2/3 from spec: Task 5 (unit), Task 6 (SF0.01 correctness), Task 7 (SF10 EC2), Task 8 (SF100 EC2).

**Placeholder scan:** Task 4 Step 5 says "set `memTracker:` to the appropriate per-query tracker. If the tracker isn't reachable... add a method like `func (sm *SpillManager) Tracker() *Tracker`." This is a known plumbing detail the implementer must navigate; I've named the helper precisely so it's not a placeholder. If the existing code already exposes a tracker reference at the scan-source construction site, the implementer can use that directly.

**Type consistency:**
- `SpillUrgency`, `SpillCheap`, `SpillExpensive` defined in Task 1 and used in Tasks 3.
- `ShouldSpillFor` is the canonical method; `ShouldSpill` retained without changes for backward compat.
- `memory.Tracker.ForceReserve` / `Release` used consistently in Tasks 4 and 5.
- `heapPressureRatio = 0.95` in Task 2 (was 0.5 today).

**Known omissions (deliberate):**
- Tasks for tracker bypass sites #2-5 (scan channel, gather buffers, probe-side hash join state, inter-op batches). Deferred per the iterative-correctness rationale above.
- Phase 4 (dynamic concurrency throttling) and Phase 5 (cardinality-aware budgets) are sketched in the spec but not in this plan, per spec scope.
