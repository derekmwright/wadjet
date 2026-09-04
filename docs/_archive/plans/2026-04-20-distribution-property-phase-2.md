> **ARCHIVED — superseded design note.** Kept for design lineage only; it does not describe the current code. Current positions: `docs/adr/` (decisions), `docs/internals/` (code maps), `docs/design/` (active memos). Search skips `docs/_archive/` by default (`.ignore`); use `rg --no-ignore` to include it.

# Distribution-Property Phase 2 Implementation Plan


**Goal:** Replace the coordinator-level four-mode routing switch with a property-based `EnsureDistribution` pass that inserts three first-class Exchange stage types, and delete the heuristic routing functions.

**Architecture:** Bottom-up planner pass compares `child.OutputDistribution` against `parent.RequiredChildDistribution` (both from Phase 1) and splices `exchange-repartition` / `exchange-replicate` / `exchange-gather` stages where the satisfaction check fails. Coordinator becomes a stage-DAG executor dispatching per stage-`Type` string to renamed/repurposed orchestrator backends. Behaviour-preserving — every TPC-H query produces the same execution as today, derived rather than declared.

**Tech Stack:** Go 1.22+; existing Wadjet planner/coordinator; NATS JetStream for task dispatch; TPC-H SF0.01 / SF1 / SF10 for validation.

**Spec:** `docs/_archive/specs/2026-04-20-distribution-property-phase-2-design.md`

---

## Prerequisites

- Phase 1 (`feat/distribution-property-phase-1`, HEAD `21e7b26`) is **merged to main** before starting this plan. Without Phase 1, the helpers `RequiredChildDistribution`, `OutputDistribution`, `assignStageDistributions`, `AssertExchangeConsistency`, and the `BehaviorPreservingMode` package var do not exist.
- Verify before starting:
  ```bash
  git checkout main && git pull
  grep -n "func RequiredChildDistribution" internal/planner/physical/distribution.go
  grep -n "func OutputDistribution"       internal/planner/physical/distribution.go
  grep -n "func assignStageDistributions" internal/planner/physical/distribution.go
  grep -n "func AssertExchangeConsistency" internal/planner/physical/distribution.go
  grep -n "var BehaviorPreservingMode"    internal/planner/physical/distribution.go
  ```
  All five `grep`s must return a match. If any fails, **stop** — Phase 1 is not on main.
- Create the work branch:
  ```bash
  git checkout -b feat/distribution-property-phase-2
  ```

## File Map

**New files:**
- `internal/planner/physical/exchange.go` — Exchange stage constants + `ExchangeStage` payload shape
- `internal/planner/physical/ensure_distribution.go` — the `EnsureDistribution` pass
- `internal/planner/physical/ensure_distribution_test.go` — unit tests for the pass
- `internal/coordinator/orchestrate_gather.go` — new Gather backend
- `internal/coordinator/orchestrate_gather_test.go`
- `internal/coordinator/orchestrator_multibuild.go` — dormant multi-build shuffle (ported from `feat/broadcast-hazard-mitigation`)
- `internal/coordinator/orchestrator_multibuild_test.go` — keeps dormant code compiling
- `internal/coordinator/parity_harness_test.go` — PARITY=1 dual-path result comparison (deleted in final task)
- `internal/planner/physical/testdata/ensure_distribution/q{01..22}.golden` — snapshot files

**Modified files:**
- `internal/planner/physical/plan.go` — add `StageExchange*` constants, `UseEnsureDistribution` field on `Planner`
- `internal/planner/physical/distribution.go` — flip call to strict mode when flag enabled
- `internal/coordinator/coordinator.go` — add stage-DAG dispatch alongside switch, then delete switch
- `internal/coordinator/shuffle_orchestrator.go` — rename `orchestrateShuffleStages` → `orchestrateRepartition`
- `internal/coordinator/build_cache.go` — reshape `preScanBuildTables` → `orchestrateReplicate`

**Deleted files/symbols (final task):**
- `internal/planner/physical/plan.go` — `PickShuffleCandidate`, `ShuffleCandidate` (lines ~1087–1309)
- `internal/planner/physical/plan.go` — `CanProbeSplit` (lines ~1044–1085)
- `internal/planner/physical/aggregate_shuffle.go` — `PickAggregateShuffleCandidate`
- `internal/planner/physical/plan.go` — `MergeInfo`, `ExtractMergeInfo`, `has_merge` threading
- `internal/coordinator/coordinator.go` — four-mode switch (lines ~472–605)
- `internal/coordinator/aggregate_shuffle.go` — `preComputeDerivedAggregate`
- `internal/coordinator/parity_harness_test.go` — parity harness (entire file)
- `internal/planner/physical/plan.go` — `UseEnsureDistribution` field on `Planner`

---

## Task 1: Introduce Exchange stage-type constants

Phase 1's `Stage.Type` is a plain `string` (see `plan.go:37`). Rather than refactor to a typed enum, we add exported string constants matching the existing informal taxonomy ("scan", "aggregate", "shuffle", …) and rename the existing "shuffle" string to "exchange-repartition".

**Files:**
- Create: `internal/planner/physical/exchange.go`
- Modify: `internal/planner/physical/plan.go` (line 37 comment), wherever `"shuffle"` string literals appear

- [ ] **Step 1: Write the failing test**

Create `internal/planner/physical/exchange_test.go`:

```go
package physical

import "testing"

func TestStageTypeConstants(t *testing.T) {
	cases := []struct{ got, want string }{
		{StageScan, "scan"},
		{StageAggregate, "aggregate"},
		{StageSort, "sort"},
		{StageHashJoin, "hash_join"},
		{StageBroadcastJoin, "broadcast_join"},
		{StageWindow, "window"},
		{StagePipeline, "pipeline"},
		{StageExchangeRepartition, "exchange-repartition"},
		{StageExchangeReplicate, "exchange-replicate"},
		{StageExchangeGather, "exchange-gather"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("got %q want %q", c.got, c.want)
		}
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
go test ./internal/planner/physical/ -run TestStageTypeConstants -v
```
Expected: FAIL (`undefined: StageScan` or similar).

- [ ] **Step 3: Create `exchange.go` with the constants**

```go
// Package physical defines stage-type string constants used by
// Stage.Type. Exchange stages are inserted by EnsureDistribution
// to bridge distribution mismatches between child output and parent
// required input.
package physical

const (
	StageScan          = "scan"
	StageAggregate     = "aggregate"
	StageSort          = "sort"
	StageHashJoin      = "hash_join"
	StageBroadcastJoin = "broadcast_join"
	StageWindow        = "window"
	StagePipeline      = "pipeline"

	// Exchange stages — inserted by EnsureDistribution.
	// Repartition is the rename of the legacy "shuffle" type; the string
	// value changes so that the old name does not silently leak through.
	StageExchangeRepartition = "exchange-repartition"
	StageExchangeReplicate   = "exchange-replicate"
	StageExchangeGather      = "exchange-gather"
)

// ExchangeStage carries the per-variant payload attached to an Exchange
// Stage. Stored on the Stage itself (not embedded) to keep Stage a flat
// value type.
type ExchangeStage struct {
	Keys     []string     // Repartition only
	Count    int          // Repartition only
	Ordering []SortKeySpec // Gather only (optional sort-merge gather)
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
go test ./internal/planner/physical/ -run TestStageTypeConstants -v
```
Expected: PASS.

- [ ] **Step 5: Replace "shuffle" string literals with StageExchangeRepartition**

Find every `"shuffle"` and `fmt.Sprintf("shuffle-%d", …)` in the planner and coordinator.

```bash
grep -rn '"shuffle"' internal/planner/physical/ internal/coordinator/ internal/worker/
grep -rn 'shuffle-%d\|"shuffle-' internal/planner/physical/ internal/coordinator/ internal/worker/
```

Replace **only the stage-type string `"shuffle"`** (not function/variable/file names) with `StageExchangeRepartition`. The ID pattern `"shuffle-%d"` should become `"exchange-repartition-%d"`. Leave `orchestrateShuffleStages`, `ShuffleKeys`, `ShuffleCandidate`, `shuffle_orchestrator.go`, `shuffle-%d` tests untouched in this task — they're renamed/deleted later.

After the replacement, add a short comment to `plan.go:37`:
```go
	Type         string // see exchange.go constants (scan, aggregate, sort, hash_join, broadcast_join, window, pipeline, exchange-repartition, exchange-replicate, exchange-gather)
```

- [ ] **Step 6: Update tests that reference the literal "shuffle" string**

```bash
grep -rn '"shuffle"' internal/planner/physical/*_test.go internal/coordinator/*_test.go
```

Replace each with `physical.StageExchangeRepartition` (coordinator) or `StageExchangeRepartition` (planner package).

- [ ] **Step 7: Run full planner + coordinator suites**

```bash
go test ./internal/planner/physical/... ./internal/coordinator/... ./internal/worker/... 2>&1 | tail -30
```
Expected: all pass. If a test hardcodes "shuffle" in a NATS subject or task queue name, leave the NATS name unchanged — it's a wire-protocol identifier, not a stage-type. (Stage-type `Type` field and wire subject are logically independent.)

- [ ] **Step 8: Commit**

```bash
git add internal/planner/physical/exchange.go \
        internal/planner/physical/exchange_test.go \
        internal/planner/physical/plan.go \
        internal/planner/physical/*_test.go \
        internal/coordinator/*.go \
        internal/coordinator/*_test.go \
        internal/worker/
git commit -m "feat(planner): add Exchange stage-type constants; rename shuffle -> exchange-repartition"
```

---

## Task 2: Port dormant `orchestrateMultiBuildShuffle`

Keeps the multi-build shuffle code from the parked branch compiling and under test, ready for Phase 3 wiring. Not called from Phase 2 dispatch.

**Files:**
- Create: `internal/coordinator/orchestrator_multibuild.go`
- Create: `internal/coordinator/orchestrator_multibuild_test.go`

- [ ] **Step 1: Inspect the parked branch**

```bash
git show feat/broadcast-hazard-mitigation:internal/coordinator/orchestrator_multibuild.go 2>/dev/null | head -80 || \
  git log --all --oneline --grep='multi.build\|MultiBuild\|multibuild' | head
```

Locate the file containing `orchestrateMultiBuildShuffle` on the `feat/broadcast-hazard-mitigation` branch. If the file name differs, adjust the path below. If the function is inlined in `shuffle_orchestrator.go` on that branch, extract only that function body.

- [ ] **Step 2: Copy the file to the new location on this branch**

```bash
git show feat/broadcast-hazard-mitigation:internal/coordinator/<source-path> > internal/coordinator/orchestrator_multibuild.go
```

Fix the package declaration if needed (must be `package coordinator`). Remove any imports that reference symbols this branch doesn't have (hazard detection, `BroadcastSideSelector`, etc.) — `orchestrateMultiBuildShuffle` itself takes a pre-picked set of builds to shuffle; the selection logic is Phase 3.

If the ported file depends on symbols that don't exist on main (e.g. a `BroadcastHazard` struct), stub the dependency with a Phase-2-local minimal type and add a `// TODO(phase-3): replace with real hazard type` comment. Keep the stub in the same file.

- [ ] **Step 3: Write a smoke test**

Create `internal/coordinator/orchestrator_multibuild_test.go`:

```go
package coordinator

import "testing"

// TestMultiBuildShuffle_Dormant ensures orchestrateMultiBuildShuffle
// compiles and is reachable. It is not wired into dispatch until Phase 3.
func TestMultiBuildShuffle_Dormant(t *testing.T) {
	// Verify the function exists and accepts the expected shape.
	// A real end-to-end test lives in Phase 3 when the orchestrator
	// is called from coordinator dispatch.
	var _ = orchestrateMultiBuildShuffle
}
```

- [ ] **Step 4: Run**

```bash
go test ./internal/coordinator/ -run TestMultiBuildShuffle_Dormant -v
go build ./...
```
Expected: PASS + clean build.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator/orchestrator_multibuild.go \
        internal/coordinator/orchestrator_multibuild_test.go
git commit -m "feat(coordinator): port orchestrateMultiBuildShuffle (dormant, for phase 3)"
```

---

## Task 3: Add `EnsureDistribution` variant mapping helper

Small pure function mapping a `RequiredDistribution` to the Exchange stage that satisfies it. Keeps Task 4 readable.

**Files:**
- Create: `internal/planner/physical/ensure_distribution.go`
- Create: `internal/planner/physical/ensure_distribution_test.go`

- [ ] **Step 1: Write the failing test**

```go
package physical

import (
	"reflect"
	"testing"
)

func TestExchangeVariantFor(t *testing.T) {
	cases := []struct {
		name string
		req  RequiredDistribution
		want Stage // only Type and ExchangeStage-ish fields checked
	}{
		{
			name: "broadcast -> replicate",
			req:  RequiredDistribution{Kind: RequiredBroadcast},
			want: Stage{Type: StageExchangeReplicate},
		},
		{
			name: "singleton -> gather",
			req:  RequiredDistribution{Kind: RequiredSingleton},
			want: Stage{Type: StageExchangeGather},
		},
		{
			name: "hash-partitioned -> repartition",
			req:  RequiredDistribution{Kind: RequiredHashPartitionedOn, Keys: []string{"a", "b"}, Count: 4},
			want: Stage{Type: StageExchangeRepartition, ShuffleKeys: []string{"a", "b"}, NumPartitions: 4},
		},
		{
			name: "clustered-on -> repartition",
			req:  RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"x"}, Count: 4},
			want: Stage{Type: StageExchangeRepartition, ShuffleKeys: []string{"x"}, NumPartitions: 4},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := exchangeVariantFor(c.req)
			if !ok {
				t.Fatalf("exchangeVariantFor returned ok=false")
			}
			if got.Type != c.want.Type {
				t.Errorf("Type: got %q want %q", got.Type, c.want.Type)
			}
			if c.want.Type == StageExchangeRepartition {
				if !reflect.DeepEqual(got.ShuffleKeys, c.want.ShuffleKeys) {
					t.Errorf("ShuffleKeys: got %v want %v", got.ShuffleKeys, c.want.ShuffleKeys)
				}
				if got.NumPartitions != c.want.NumPartitions {
					t.Errorf("NumPartitions: got %d want %d", got.NumPartitions, c.want.NumPartitions)
				}
			}
		})
	}
}

func TestExchangeVariantFor_Any_NoInsertion(t *testing.T) {
	_, ok := exchangeVariantFor(RequiredDistribution{Kind: RequiredAny})
	if ok {
		t.Fatal("RequiredAny should return ok=false (no exchange needed)")
	}
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
go test ./internal/planner/physical/ -run TestExchangeVariantFor -v
```
Expected: FAIL (`undefined: exchangeVariantFor`).

- [ ] **Step 3: Implement**

Create `internal/planner/physical/ensure_distribution.go`:

```go
package physical

// exchangeVariantFor returns a skeleton Exchange Stage whose Type and
// payload satisfy the given required distribution. It does not set ID,
// Dependencies, ClusterID, Tasks, or Distribution — callers fill those
// in. Returns ok=false when the requirement is RequiredAny (no exchange
// needed).
func exchangeVariantFor(req RequiredDistribution) (Stage, bool) {
	switch req.Kind {
	case RequiredAny:
		return Stage{}, false
	case RequiredBroadcast:
		return Stage{Type: StageExchangeReplicate}, true
	case RequiredSingleton:
		return Stage{Type: StageExchangeGather}, true
	case RequiredHashPartitionedOn, RequiredClusteredOn:
		return Stage{
			Type:          StageExchangeRepartition,
			ShuffleKeys:   append([]string(nil), req.Keys...),
			NumPartitions: req.Count,
		}, true
	default:
		return Stage{}, false
	}
}
```

- [ ] **Step 4: Run to verify it passes**

```bash
go test ./internal/planner/physical/ -run TestExchangeVariantFor -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/ensure_distribution.go \
        internal/planner/physical/ensure_distribution_test.go
git commit -m "feat(planner): add exchangeVariantFor helper for distribution-property pass"
```

---

## Task 4: `EnsureDistribution` — single-edge insertion

Insert one Exchange at a time for one mismatched edge. Build up to multi-edge in later tasks.

**Files:**
- Modify: `internal/planner/physical/ensure_distribution.go`
- Modify: `internal/planner/physical/ensure_distribution_test.go`

- [ ] **Step 1: Write the failing test**

Append to `ensure_distribution_test.go`:

```go
func TestEnsureDistribution_NoOp(t *testing.T) {
	// Stage DAG: one scan (Singleton) → pipeline (requires Any).
	stages := []Stage{
		{ID: "scan-0", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{ID: "pipe-0", Type: StagePipeline, Dependencies: []string{"scan-0"}, Distribution: Distribution{Kind: DistSingleton}},
	}
	got, err := EnsureDistribution(stages, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(got))
	}
}

func TestEnsureDistribution_InsertsReplicate(t *testing.T) {
	// Scan (Singleton) feeds a hash-join build (requires Broadcast).
	stages := []Stage{
		{ID: "scan-build", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{ID: "scan-probe", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{
			ID:            "join-0",
			Type:          StageHashJoin,
			LeftDepStage:  "scan-probe",
			RightDepStage: "scan-build",
			JoinLeftKeys:  []string{"a"},
			JoinRightKeys: []string{"a"},
			Distribution:  Distribution{Kind: DistSingleton},
		},
	}
	got, err := EnsureDistribution(stages, 4)
	if err != nil {
		t.Fatal(err)
	}
	// Expect one StageExchangeReplicate inserted between scan-build and join-0.
	var inserted *Stage
	for i := range got {
		if got[i].Type == StageExchangeReplicate {
			inserted = &got[i]
		}
	}
	if inserted == nil {
		t.Fatalf("no StageExchangeReplicate inserted; got types: %v", stageTypes(got))
	}
	// The join stage's RightDepStage should now point at the exchange, not the raw scan.
	var join *Stage
	for i := range got {
		if got[i].ID == "join-0" {
			join = &got[i]
		}
	}
	if join.RightDepStage != inserted.ID {
		t.Errorf("join-0.RightDepStage: got %q want %q", join.RightDepStage, inserted.ID)
	}
	if inserted.Distribution.Kind != DistBroadcast {
		t.Errorf("inserted exchange Distribution.Kind: got %v want DistBroadcast", inserted.Distribution.Kind)
	}
}

func stageTypes(s []Stage) []string {
	out := make([]string, len(s))
	for i, st := range s {
		out[i] = st.Type
	}
	return out
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
go test ./internal/planner/physical/ -run TestEnsureDistribution -v
```
Expected: FAIL (`undefined: EnsureDistribution`).

- [ ] **Step 3: Implement**

Append to `ensure_distribution.go`:

```go
import "fmt"

// EnsureDistribution walks the stage DAG and inserts Exchange stages
// wherever a child's OutputDistribution does not satisfy its parent's
// RequiredChildDistribution. Returns a new []Stage; does not mutate
// the input slice.
func EnsureDistribution(stages []Stage, workerCount int) ([]Stage, error) {
	out := make([]Stage, 0, len(stages)+4)
	byID := make(map[string]int, len(stages))
	for i := range stages {
		byID[stages[i].ID] = len(out)
		out = append(out, stages[i])
	}

	// Walk in dependency order. `stages` is assumed topo-sorted by the
	// planner (current invariant — dependencies appear before dependents).
	// For each stage, inspect each input slot; splice an exchange if the
	// satisfaction check fails.
	for i := range out {
		parent := &out[i]
		slots := dependencySlots(parent)
		for _, slot := range slots {
			childID := slot.get(parent)
			if childID == "" {
				continue
			}
			childIdx, ok := byID[childID]
			if !ok {
				return nil, fmt.Errorf("ensure distribution: parent %q references unknown child %q", parent.ID, childID)
			}
			req := RequiredChildDistribution(*parent, slot.name)
			actual := out[childIdx].Distribution
			if actual.Satisfies(req) {
				continue
			}
			exch, ok := exchangeVariantFor(req)
			if !ok {
				return nil, fmt.Errorf(
					"ensure distribution: no exchange variant satisfies %v from %v (parent=%s slot=%s)",
					req, actual, parent.ID, slot.name,
				)
			}
			exch.ID = fmt.Sprintf("%s-%s-%d", exch.Type, parent.ID, i)
			exch.Dependencies = []string{childID}
			exch.Distribution = distributionFromRequired(req, workerCount)
			// Route parent's slot at the exchange.
			slot.set(parent, exch.ID)
			// Update parent's Dependencies too (Stage.Dependencies is the
			// canonical DAG edge set; slot fields are per-operator views).
			parent.Dependencies = replaceOne(parent.Dependencies, childID, exch.ID)
			byID[exch.ID] = len(out)
			out = append(out, exch)
		}
	}
	return out, nil
}

// dependencySlots returns the per-operator input slots on a stage. Each
// slot knows its name (used for RequiredChildDistribution lookup) and
// how to read/write the child ID on the parent Stage.
type dependencySlot struct {
	name string
	get  func(*Stage) string
	set  func(*Stage, string)
}

func dependencySlots(s *Stage) []dependencySlot {
	switch s.Type {
	case StageHashJoin, StageBroadcastJoin:
		return []dependencySlot{
			{"probe", func(s *Stage) string { return s.LeftDepStage }, func(s *Stage, id string) { s.LeftDepStage = id }},
			{"build", func(s *Stage) string { return s.RightDepStage }, func(s *Stage, id string) { s.RightDepStage = id }},
		}
	}
	// Default: single-input stages consume their first dependency.
	if len(s.Dependencies) == 0 {
		return nil
	}
	return []dependencySlot{{
		name: "input",
		get:  func(s *Stage) string { return s.Dependencies[0] },
		set:  func(s *Stage, id string) { s.Dependencies[0] = id },
	}}
}

// distributionFromRequired produces a concrete Distribution that
// satisfies the given RequiredDistribution. Used when synthesising
// an Exchange stage's output properties.
func distributionFromRequired(req RequiredDistribution, workerCount int) Distribution {
	switch req.Kind {
	case RequiredBroadcast:
		return Distribution{Kind: DistBroadcast}
	case RequiredSingleton:
		return Distribution{Kind: DistSingleton}
	case RequiredHashPartitionedOn, RequiredClusteredOn:
		n := req.Count
		if n == 0 {
			n = workerCount
		}
		return Distribution{Kind: DistHashPartitioned, Keys: append([]string(nil), req.Keys...), Count: n}
	default:
		return Distribution{Kind: DistSingleton}
	}
}

func replaceOne(xs []string, old, new string) []string {
	out := make([]string, len(xs))
	copy(out, xs)
	for i, v := range out {
		if v == old {
			out[i] = new
			return out
		}
	}
	return out
}
```

**Contract note:** `dependencySlots` intentionally returns `nil` for stages with no dependencies (scans). For multi-input stages beyond joins (e.g. window with lookup, future union), extend the switch. Phase 2 scope: joins + single-input operators.

- [ ] **Step 4: Run to verify the tests pass**

```bash
go test ./internal/planner/physical/ -run TestEnsureDistribution -v
```
Expected: PASS for both `_NoOp` and `_InsertsReplicate`.

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/ensure_distribution.go \
        internal/planner/physical/ensure_distribution_test.go
git commit -m "feat(planner): add EnsureDistribution pass with replicate insertion"
```

---

## Task 5: `EnsureDistribution` — repartition + gather insertion

Cover the two remaining variants.

**Files:**
- Modify: `internal/planner/physical/ensure_distribution_test.go`

- [ ] **Step 1: Write the failing tests**

Append:

```go
func TestEnsureDistribution_InsertsRepartition(t *testing.T) {
	// Scan produces HashPartitioned on "a" / 4; parent join requires
	// HashPartitioned on "b" / 4 for its probe side.
	stages := []Stage{
		{ID: "scan-probe", Type: StageScan,
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"a"}, Count: 4}},
		{ID: "scan-build", Type: StageScan,
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"b"}, Count: 4}},
		{
			ID:            "join-0",
			Type:          StageHashJoin,
			LeftDepStage:  "scan-probe",
			RightDepStage: "scan-build",
			JoinLeftKeys:  []string{"b"},
			JoinRightKeys: []string{"b"},
			Distribution:  Distribution{Kind: DistHashPartitioned, Keys: []string{"b"}, Count: 4},
		},
	}
	got, err := EnsureDistribution(stages, 4)
	if err != nil {
		t.Fatal(err)
	}
	// Expect exactly one repartition inserted — between scan-probe and join-0.
	var rep *Stage
	for i := range got {
		if got[i].Type == StageExchangeRepartition {
			rep = &got[i]
		}
	}
	if rep == nil {
		t.Fatalf("no repartition inserted; got: %v", stageTypes(got))
	}
	if rep.NumPartitions != 4 || len(rep.ShuffleKeys) != 1 || rep.ShuffleKeys[0] != "b" {
		t.Errorf("repartition payload: keys=%v count=%d", rep.ShuffleKeys, rep.NumPartitions)
	}
}

func TestEnsureDistribution_InsertsGather(t *testing.T) {
	// Root stage is hash-partitioned; an implicit final Singleton is
	// required. EnsureDistribution must append a Gather above it.
	stages := []Stage{
		{ID: "scan-0", Type: StageScan,
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"a"}, Count: 4}},
		{
			ID:           "agg-0",
			Type:         StageAggregate,
			Dependencies: []string{"scan-0"},
			GroupByCols:  []string{"a"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"a"}, Count: 4},
		},
	}
	got, err := EnsureDistribution(stages, 4)
	if err != nil {
		t.Fatal(err)
	}
	last := got[len(got)-1]
	if last.Type != StageExchangeGather {
		t.Fatalf("final stage type: got %q want %q", last.Type, StageExchangeGather)
	}
	if last.Distribution.Kind != DistSingleton {
		t.Errorf("gather output distribution: got %v want Singleton", last.Distribution.Kind)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

```bash
go test ./internal/planner/physical/ -run TestEnsureDistribution_Inserts -v
```
Expected: repartition test passes (covered by Task 4 generic path); gather test FAILS (no final-gather logic yet).

- [ ] **Step 3: Add the root-gather step**

In `ensure_distribution.go`, after the per-stage slot loop, append:

```go
	// If the final stage's output isn't Singleton, the query root needs a
	// Gather so the coordinator sees one output stream.
	if len(out) == 0 {
		return out, nil
	}
	root := &out[len(out)-1]
	if root.Distribution.Kind != DistSingleton && root.Type != StageExchangeGather {
		gather := Stage{
			Type:         StageExchangeGather,
			ID:           fmt.Sprintf("%s-%s", StageExchangeGather, root.ID),
			Dependencies: []string{root.ID},
			Distribution: Distribution{Kind: DistSingleton},
		}
		out = append(out, gather)
	}
	return out, nil
```

(Remove the plain `return out, nil` at the end of the previous task's body so only this one remains.)

- [ ] **Step 4: Run the full `EnsureDistribution` test set**

```bash
go test ./internal/planner/physical/ -run TestEnsureDistribution -v
```
Expected: all four pass.

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/ensure_distribution.go \
        internal/planner/physical/ensure_distribution_test.go
git commit -m "feat(planner): add repartition + gather insertion to EnsureDistribution"
```

---

## Task 6: `EnsureDistribution` — multi-consumer DAG sharing + idempotence

**Files:**
- Modify: `internal/planner/physical/ensure_distribution.go`
- Modify: `internal/planner/physical/ensure_distribution_test.go`

- [ ] **Step 1: Write the failing tests**

```go
func TestEnsureDistribution_MultiConsumerReuse(t *testing.T) {
	// Two joins each consume the same build-scan as their RightDep.
	// Both require Broadcast. Expect ONE replicate inserted, shared.
	stages := []Stage{
		{ID: "probe-a", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{ID: "probe-b", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{ID: "build-shared", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{ID: "join-a", Type: StageHashJoin,
			LeftDepStage: "probe-a", RightDepStage: "build-shared",
			JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			Distribution: Distribution{Kind: DistSingleton}},
		{ID: "join-b", Type: StageHashJoin,
			LeftDepStage: "probe-b", RightDepStage: "build-shared",
			JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			Distribution: Distribution{Kind: DistSingleton}},
	}
	got, err := EnsureDistribution(stages, 4)
	if err != nil {
		t.Fatal(err)
	}
	var replicates int
	var sharedID string
	for _, s := range got {
		if s.Type == StageExchangeReplicate {
			replicates++
			sharedID = s.ID
		}
	}
	if replicates != 1 {
		t.Fatalf("expected 1 replicate (shared), got %d", replicates)
	}
	// Both joins' RightDepStage should point at the same shared replicate.
	var seen []string
	for _, s := range got {
		if s.Type == StageHashJoin {
			seen = append(seen, s.RightDepStage)
		}
	}
	if len(seen) != 2 || seen[0] != sharedID || seen[1] != sharedID {
		t.Errorf("joins should both reference %q; got %v", sharedID, seen)
	}
}

func TestEnsureDistribution_Idempotent(t *testing.T) {
	stages := []Stage{
		{ID: "scan-build", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{ID: "scan-probe", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{ID: "join-0", Type: StageHashJoin,
			LeftDepStage: "scan-probe", RightDepStage: "scan-build",
			JoinLeftKeys: []string{"a"}, JoinRightKeys: []string{"a"},
			Distribution: Distribution{Kind: DistSingleton}},
	}
	once, err := EnsureDistribution(stages, 4)
	if err != nil {
		t.Fatal(err)
	}
	twice, err := EnsureDistribution(once, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(once) != len(twice) {
		t.Errorf("second pass changed stage count: %d -> %d", len(once), len(twice))
	}
}
```

- [ ] **Step 2: Run to verify they fail**

```bash
go test ./internal/planner/physical/ -run 'TestEnsureDistribution_(MultiConsumerReuse|Idempotent)' -v
```
Expected: `MultiConsumerReuse` FAILS (current impl creates one Exchange per parent slot; the second join sees the raw scan and synthesises its own); `Idempotent` FAILS (each pass adds another gather because an existing exchange-repartition root wouldn't show Singleton).

- [ ] **Step 3: Add per-(childID, required) deduplication**

In `EnsureDistribution`, before the `exchangeVariantFor` call, check a cache:

```go
	type cacheKey struct {
		childID string
		kind    RequiredKind
		keys    string // joined
		count   int
	}
	cache := make(map[cacheKey]string) // -> exchange stage ID
```

(declared above the `for i := range out` loop)

Inside the per-slot block, after computing `req`:

```go
			key := cacheKey{childID: childID, kind: req.Kind, keys: strings.Join(req.Keys, ","), count: req.Count}
			if existing, ok := cache[key]; ok {
				slot.set(parent, existing)
				parent.Dependencies = replaceOne(parent.Dependencies, childID, existing)
				continue
			}
```

After creating and appending the exchange, `cache[key] = exch.ID`.

For **idempotence**, add this at the top of the function:

```go
	// If the input already terminates in a Gather (prior EnsureDistribution
	// run), skip the final gather append at the end.
	alreadyGathered := len(stages) > 0 && stages[len(stages)-1].Type == StageExchangeGather
```

And gate the final-gather append:
```go
	if !alreadyGathered && root.Distribution.Kind != DistSingleton && root.Type != StageExchangeGather {
		...
	}
```

Add `"strings"` to the import block.

- [ ] **Step 4: Run to verify**

```bash
go test ./internal/planner/physical/ -run TestEnsureDistribution -v
```
Expected: all six `TestEnsureDistribution_*` tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/ensure_distribution.go \
        internal/planner/physical/ensure_distribution_test.go
git commit -m "feat(planner): dedupe multi-consumer exchanges; make EnsureDistribution idempotent"
```

---

## Task 7: `EnsureDistribution` — strict-mode failure on unsatisfiable edge

**Files:**
- Modify: `internal/planner/physical/ensure_distribution_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestEnsureDistribution_StrictModeFailsOnGap(t *testing.T) {
	// Synthetic: a parent whose required kind is an unknown value.
	// (Exercised via a RequiredKind that exchangeVariantFor rejects.)
	stages := []Stage{
		{ID: "scan-0", Type: StageScan, Distribution: Distribution{Kind: DistSingleton}},
		{ID: "bad-parent", Type: "__test_unsatisfiable", Dependencies: []string{"scan-0"},
			Distribution: Distribution{Kind: DistSingleton}},
	}
	// Force RequiredChildDistribution to return an unknown kind by
	// registering a test-only hook. See ensureDistributionTestHook below.
	origHook := requiredChildDistributionForTest
	t.Cleanup(func() { requiredChildDistributionForTest = origHook })
	requiredChildDistributionForTest = func(s Stage, slot string) (RequiredDistribution, bool) {
		if s.Type == "__test_unsatisfiable" {
			return RequiredDistribution{Kind: RequiredKind(-1)}, true
		}
		return RequiredDistribution{}, false
	}

	_, err := EnsureDistribution(stages, 4)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
go test ./internal/planner/physical/ -run TestEnsureDistribution_StrictMode -v
```
Expected: FAIL (`undefined: requiredChildDistributionForTest`).

- [ ] **Step 3: Add the hook + use it**

In `ensure_distribution.go`:

```go
// requiredChildDistributionForTest lets tests override the real
// RequiredChildDistribution to exercise error paths. Returns (req, true)
// to supply a value; (_, false) to fall through to the production helper.
var requiredChildDistributionForTest func(Stage, string) (RequiredDistribution, bool)

func requiredChildDistribution(s Stage, slot string) RequiredDistribution {
	if requiredChildDistributionForTest != nil {
		if req, ok := requiredChildDistributionForTest(s, slot); ok {
			return req
		}
	}
	return RequiredChildDistribution(s, slot)
}
```

Replace the call inside `EnsureDistribution`:

```go
	req := requiredChildDistribution(*parent, slot.name)
```

- [ ] **Step 4: Run**

```bash
go test ./internal/planner/physical/ -run TestEnsureDistribution -v
```
Expected: all pass, including `_StrictModeFailsOnGap`.

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/ensure_distribution.go \
        internal/planner/physical/ensure_distribution_test.go
git commit -m "feat(planner): surface unsatisfiable-edge errors from EnsureDistribution"
```

---

## Task 8: `Planner.UseEnsureDistribution` flag; wire into `PlanDistributed`

Run `EnsureDistribution` behind a feature flag. Default **off** at this task. Flag flips to on in Task 14 after downstream dispatch is ready.

**Files:**
- Modify: `internal/planner/physical/plan.go`
- Modify: `internal/planner/physical/plan_test.go` (or nearest planner test file)

- [ ] **Step 1: Locate `Planner` struct and `PlanDistributed`**

```bash
grep -n "type Planner struct" internal/planner/physical/plan.go
grep -n "func .*PlanDistributed" internal/planner/physical/plan.go
grep -n "assignStageDistributions(" internal/planner/physical/plan.go
grep -n "AssertExchangeConsistency(" internal/planner/physical/plan.go
```

Note the line numbers reported — subsequent edits target them.

- [ ] **Step 2: Write the failing test**

Append to `internal/planner/physical/plan_tpch_test.go` (or the nearest test file):

```go
func TestPlanDistributed_WithEnsureDistribution_InsertsExchanges(t *testing.T) {
	// Q05-shaped toy: probe scan + build scan + join + aggregate.
	// With UseEnsureDistribution=true, the resulting plan must include
	// at least one StageExchangeReplicate.
	p := NewPlanner(/* existing constructor args — match plan_test.go */)
	p.UseEnsureDistribution = true
	// Build a logical plan. Reuse the pattern from an existing test
	// in this file (grep for "logical.Join" or "buildPhysicalPlan").
	// ...logical plan construction...
	phys, err := p.PlanDistributed(/* same args as existing tests */)
	if err != nil {
		t.Fatal(err)
	}
	var sawReplicate bool
	for _, s := range phys.Stages {
		if s.Type == StageExchangeReplicate {
			sawReplicate = true
		}
	}
	if !sawReplicate {
		t.Fatal("expected at least one exchange-replicate stage")
	}
}
```

**Note:** This test's constructor/logical-plan construction must match whatever pattern exists in the same file. If the file has a helper like `planQ(t, "Q05")`, reuse it instead of inlining. Do not write test boilerplate that doesn't match the file's conventions. If the Phase 1 test `TestTPCHDistributionConsistency` uses a known harness, mirror it.

- [ ] **Step 3: Run to verify it fails**

```bash
go test ./internal/planner/physical/ -run TestPlanDistributed_WithEnsureDistribution -v
```
Expected: FAIL (`Planner has no field UseEnsureDistribution`).

- [ ] **Step 4: Add the field and wire it up**

In `plan.go`, add to the `Planner` struct:

```go
	// UseEnsureDistribution runs the Phase-2 EnsureDistribution pass after
	// assignStageDistributions. When true, AssertExchangeConsistency runs
	// in strict mode (BehaviorPreservingMode=false) for this plan call.
	// Temporary flag: deleted after Phase 2 merges and the legacy switch
	// is removed.
	UseEnsureDistribution bool
```

In `PlanDistributed`, immediately after the existing `assignStageDistributions(...)` call and before the existing `AssertExchangeConsistency(...)` call, insert:

```go
	if p.UseEnsureDistribution {
		stages, err := EnsureDistribution(phys.Stages, p.WorkerCount)
		if err != nil {
			return nil, fmt.Errorf("ensure distribution: %w", err)
		}
		phys.Stages = stages
	}
```

Then change the `AssertExchangeConsistency` call to flip the mode flag only for this call:

```go
	if p.UseEnsureDistribution {
		prev := BehaviorPreservingMode
		BehaviorPreservingMode = false
		defer func() { BehaviorPreservingMode = prev }()
	}
	if err := AssertExchangeConsistency(phys.Stages); err != nil {
		return nil, fmt.Errorf("assert exchange consistency: %w", err)
	}
```

**Alternative if `BehaviorPreservingMode` is not a package var but a method arg:** pass `!p.UseEnsureDistribution` to the appropriate function. Match whatever Phase 1 actually shipped.

- [ ] **Step 5: Run the test**

```bash
go test ./internal/planner/physical/ -run TestPlanDistributed_WithEnsureDistribution -v
```
Expected: PASS.

- [ ] **Step 6: Run existing tests to confirm default-off behaviour is unchanged**

```bash
go test ./internal/planner/physical/... 2>&1 | tail -20
```
Expected: all green (the flag defaults to `false`, so no existing test sees behaviour change).

- [ ] **Step 7: Commit**

```bash
git add internal/planner/physical/plan.go \
        internal/planner/physical/plan_tpch_test.go
git commit -m "feat(planner): add UseEnsureDistribution flag (default off) wiring EnsureDistribution"
```

---

## Task 9: TPC-H SF0.01 under `UseEnsureDistribution=true`

Gate: every query plans successfully, `AssertExchangeConsistency` passes strict, row-level correctness preserved.

**Files:**
- Modify: `benchmarks/tpch/*_test.go` (add a run-mode flag to the harness) or add a new test file adjacent to `plan_tpch_test.go`
- Modify: `internal/planner/physical/plan_tpch_test.go`

- [ ] **Step 1: Locate the SF0.01 correctness gate**

```bash
grep -rn 'TestTPCHQueries\b\|TestTPCHDistributionConsistency' benchmarks/tpch/ internal/planner/physical/
```

Note both file paths and the patterns they use to construct/run queries.

- [ ] **Step 2: Write a failing acceptance test**

Append to `internal/planner/physical/plan_tpch_test.go` (use the same harness pattern as `TestTPCHDistributionConsistency`):

```go
func TestTPCH_EnsureDistribution_PlannerParity(t *testing.T) {
	for _, q := range tpchQueryList() { // reuse existing helper
		t.Run(q.Name, func(t *testing.T) {
			p := newTPCHPlanner(t) // same helper used by TestTPCHDistributionConsistency
			p.UseEnsureDistribution = true
			phys, err := p.PlanDistributed(q.SQL, PlanOpts{WorkerCount: 4})
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if err := AssertExchangeConsistency(phys.Stages); err != nil {
				t.Fatalf("strict consistency: %v", err)
			}
			// Every Exchange stage's Distribution matches its payload.
			for _, s := range phys.Stages {
				switch s.Type {
				case StageExchangeRepartition:
					if s.Distribution.Kind != DistHashPartitioned {
						t.Errorf("%s has wrong Distribution.Kind %v", s.ID, s.Distribution.Kind)
					}
				case StageExchangeReplicate:
					if s.Distribution.Kind != DistBroadcast {
						t.Errorf("%s has wrong Distribution.Kind %v", s.ID, s.Distribution.Kind)
					}
				case StageExchangeGather:
					if s.Distribution.Kind != DistSingleton {
						t.Errorf("%s has wrong Distribution.Kind %v", s.ID, s.Distribution.Kind)
					}
				}
			}
		})
	}
}
```

If `tpchQueryList()` and `newTPCHPlanner(t)` helpers don't exist with those names, inspect Phase 1's `TestTPCHDistributionConsistency` and reuse whatever helpers it uses — don't invent new ones.

- [ ] **Step 3: Run to verify it fails (or reveals bugs)**

```bash
go test ./internal/planner/physical/ -run TestTPCH_EnsureDistribution_PlannerParity -v 2>&1 | tail -60
```
Expected: one of three outcomes.
- (a) All 22 pass → go to Step 5.
- (b) Strict-consistency error on some query → Phase 1's `RequiredChildDistribution` / `OutputDistribution` for some stage type is missing a case. Fix by extending the Phase 1 helpers (in `distribution.go`) so the satisfaction check succeeds.
- (c) `exchangeVariantFor` returns `ok=false` for a variant we didn't expect → extend `exchangeVariantFor`.

Every fix in (b)/(c) should come with its own unit test added to `distribution_test.go` or `ensure_distribution_test.go` before the fix.

- [ ] **Step 4: Iterate to green**

Repeat fix-test-run until all 22 queries pass. Each fix is its own commit:

```bash
git commit -m "fix(planner): RequiredChildDistribution for <stage type> (uncovered by Qxx)"
```

- [ ] **Step 5: Run correctness at SF0.01**

```bash
WADJET_PLANNER_USE_ENSURE_DIST=1 go test -v -run TestTPCHQueries ./benchmarks/tpch/ 2>&1 | tail -40
```

The env var must be piped through to the planner. If the TPC-H harness doesn't read it, add:
```go
// benchmarks/tpch/harness.go (or wherever the planner is constructed)
p.UseEnsureDistribution = os.Getenv("WADJET_PLANNER_USE_ENSURE_DIST") == "1"
```

Expected: all 22 queries return correct row counts.

- [ ] **Step 6: Commit**

```bash
git add benchmarks/tpch/ internal/planner/physical/plan_tpch_test.go
git commit -m "test(tpch): EnsureDistribution parity gate at SF0.01"
```

---

## Task 10: Golden snapshots for each TPC-H query's Exchange DAG

Makes the coordinator-switch→DAG translation reviewable. Each future change that alters exchange placement will diff the snapshot.

**Files:**
- Create: `internal/planner/physical/testdata/ensure_distribution/q{01..22}.golden`
- Modify: `internal/planner/physical/plan_tpch_test.go`

- [ ] **Step 1: Add snapshot test**

Append to `plan_tpch_test.go`:

```go
func TestTPCH_EnsureDistribution_Snapshot(t *testing.T) {
	for _, q := range tpchQueryList() {
		t.Run(q.Name, func(t *testing.T) {
			p := newTPCHPlanner(t)
			p.UseEnsureDistribution = true
			phys, err := p.PlanDistributed(q.SQL, PlanOpts{WorkerCount: 4})
			if err != nil {
				t.Fatal(err)
			}
			var buf strings.Builder
			for _, s := range phys.Stages {
				fmt.Fprintf(&buf, "%s\t%s\t%s", s.ID, s.Type, distSummary(s.Distribution))
				if len(s.Dependencies) > 0 {
					fmt.Fprintf(&buf, "\tdeps=%s", strings.Join(s.Dependencies, ","))
				}
				buf.WriteByte('\n')
			}
			path := filepath.Join("testdata", "ensure_distribution", strings.ToLower(q.Name)+".golden")
			if *updateGolden {
				os.MkdirAll(filepath.Dir(path), 0o755)
				os.WriteFile(path, []byte(buf.String()), 0o644)
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden: %v (run with -update to create)", err)
			}
			if got := buf.String(); got != string(want) {
				t.Errorf("snapshot diff for %s:\n--- want\n%s--- got\n%s", q.Name, want, got)
			}
		})
	}
}

var updateGolden = flag.Bool("update", false, "update golden snapshots")

func distSummary(d Distribution) string {
	switch d.Kind {
	case DistSingleton:
		return "Singleton"
	case DistBroadcast:
		return "Broadcast"
	case DistHashPartitioned:
		return fmt.Sprintf("Hash(%s)/%d", strings.Join(d.Keys, ","), d.Count)
	}
	return "?"
}
```

(Add `"flag"`, `"os"`, `"path/filepath"`, `"strings"`, `"fmt"` imports as needed.)

- [ ] **Step 2: Generate goldens**

```bash
go test ./internal/planner/physical/ -run TestTPCH_EnsureDistribution_Snapshot -update
```

Expected: 22 files created under `internal/planner/physical/testdata/ensure_distribution/`.

- [ ] **Step 3: Inspect at least three goldens manually**

```bash
cat internal/planner/physical/testdata/ensure_distribution/q01.golden
cat internal/planner/physical/testdata/ensure_distribution/q05.golden
cat internal/planner/physical/testdata/ensure_distribution/q17.golden
```

Sanity-check against §"Data Flow" in the spec:
- Q01: no Exchange stages at all.
- Q05: one Replicate per build scan + one Gather at root.
- Q17: duplicated derived-agg subtree + Replicate per build (no pre-compute yet).

If any diverges materially from the spec's expected shape, **stop**: a Phase 1 helper is wrong. Fix before committing.

- [ ] **Step 4: Re-run without -update; verify all pass**

```bash
go test ./internal/planner/physical/ -run TestTPCH_EnsureDistribution_Snapshot -v
```

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/plan_tpch_test.go \
        internal/planner/physical/testdata/ensure_distribution/
git commit -m "test(planner): golden snapshots of EnsureDistribution DAG for TPC-H"
```

---

## Task 11: Rename `orchestrateShuffleStages` → `orchestrateRepartition`

Pure source rename. Zero behavioural change.

**Files:**
- Modify: `internal/coordinator/shuffle_orchestrator.go`
- Modify: `internal/coordinator/shuffle_orchestrator_test.go`
- Modify: any caller in `internal/coordinator/coordinator.go`

- [ ] **Step 1: Rename the function**

```bash
grep -rln 'orchestrateShuffleStages\b' internal/coordinator/ | xargs sed -i 's/\borchestrateShuffleStages\b/orchestrateRepartition/g'
```

- [ ] **Step 2: Rename the file**

```bash
git mv internal/coordinator/shuffle_orchestrator.go internal/coordinator/orchestrate_repartition.go
git mv internal/coordinator/shuffle_orchestrator_test.go internal/coordinator/orchestrate_repartition_test.go
```

- [ ] **Step 3: Rename any top-of-file `// Package foo shuffle…` comment**

```bash
grep -n '^// shuffle' internal/coordinator/orchestrate_repartition.go
```

Update header comments that narrate the file's purpose to mention "repartition" / "exchange-repartition" instead of "shuffle".

- [ ] **Step 4: Run**

```bash
go build ./...
go test ./internal/coordinator/... 2>&1 | tail -20
```
Expected: clean build, all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator/orchestrate_repartition.go \
        internal/coordinator/orchestrate_repartition_test.go
git commit -m "refactor(coordinator): rename orchestrateShuffleStages -> orchestrateRepartition"
```

---

## Task 12: Reshape `preScanBuildTables` → `orchestrateReplicate`

Accept a `*physical.Stage` instead of ad-hoc side-channel hints; preserve runtime behaviour.

**Files:**
- Modify: `internal/coordinator/build_cache.go` (or wherever `preScanBuildTables` lives)
- Modify: its callers in `internal/coordinator/coordinator.go`
- Modify: corresponding test file

- [ ] **Step 1: Locate the current callers**

```bash
grep -rn 'preScanBuildTables\b' internal/coordinator/
```

Record every call site — there are likely one or two.

- [ ] **Step 2: Write a failing test first**

Identify the existing test for `preScanBuildTables` (grep for `TestPreScan` or `TestBuildCache`). Add a new test file `internal/coordinator/orchestrate_replicate_test.go`:

```go
package coordinator

import (
	"context"
	"testing"

	"wadjet/internal/planner/physical"
	"wadjet/internal/storage/objstore"
)

// TestOrchestrateReplicate_MirrorsPreScanBuildTables asserts that the
// refactored entrypoint produces the same cache file paths as the
// pre-refactor function for a canonical build-cache scenario.
func TestOrchestrateReplicate_MirrorsPreScanBuildTables(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	coord := newTestCoordinator(t, store) // reuse existing test helper

	stage := physical.Stage{
		ID:         "exchange-replicate-join-0-4",
		Type:       physical.StageExchangeReplicate,
		Dependencies: []string{"scan-build"},
		// ... populate whatever orchestrateReplicate needs ...
	}

	out, err := coord.orchestrateReplicate(ctx, stage /* other args */)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("expected cache paths")
	}
}
```

If `newTestCoordinator` doesn't exist, use whatever the existing `preScanBuildTables` test uses. Match the existing fixture helper — don't build new scaffolding.

- [ ] **Step 3: Run to verify it fails**

```bash
go test ./internal/coordinator/ -run TestOrchestrateReplicate -v
```
Expected: FAIL (`undefined: (*Coordinator).orchestrateReplicate`).

- [ ] **Step 4: Implement the adapter**

In the file currently containing `preScanBuildTables`:

```go
// orchestrateReplicate dispatches a StageExchangeReplicate. It adapts
// the pre-scan implementation (preScanBuildTables) to the stage-DAG
// dispatch interface. The pre-scan function stays as the implementation
// detail; this wrapper reads the parameters it needs off the stage.
func (c *Coordinator) orchestrateReplicate(
	ctx context.Context,
	stage physical.Stage,
	/* other args currently passed to preScanBuildTables */
) (/* same return types */, error) {
	if stage.Type != physical.StageExchangeReplicate {
		return /* zero */, fmt.Errorf("orchestrate replicate: wrong stage type %q", stage.Type)
	}
	// Extract the build-table parameters from stage.Dependencies / scan
	// stage lookup. Then call preScanBuildTables with its existing
	// arg list. For now this is a thin shim — preScanBuildTables is
	// retained until Task 16 (final cleanup).
	return c.preScanBuildTables(ctx, /* args */)
}
```

- [ ] **Step 5: Run the test**

```bash
go test ./internal/coordinator/ -run TestOrchestrateReplicate -v
go test ./internal/coordinator/... 2>&1 | tail -20
```
Expected: new test PASS; all existing tests still green (because `preScanBuildTables` is still present as the implementation).

- [ ] **Step 6: Commit**

```bash
git add internal/coordinator/
git commit -m "feat(coordinator): add orchestrateReplicate shim over preScanBuildTables"
```

---

## Task 13: Add `orchestrateGather` backend

Wraps the existing coordinator-side merge behind a stage-typed entrypoint.

**Files:**
- Create: `internal/coordinator/orchestrate_gather.go`
- Create: `internal/coordinator/orchestrate_gather_test.go`

- [ ] **Step 1: Locate the current merge path**

Today the coordinator merges partial results after each orchestrator returns. Find that code:

```bash
grep -n 'merge\|Merge\|gather\|Gather' internal/coordinator/coordinator.go | head -30
```

Note the merge call site (often called inside the `default:` case or after `orchestrateShuffleStages`).

- [ ] **Step 2: Write the failing test**

```go
// internal/coordinator/orchestrate_gather_test.go
package coordinator

import (
	"context"
	"testing"

	"wadjet/internal/planner/physical"
)

func TestOrchestrateGather_MergesPartials(t *testing.T) {
	ctx := context.Background()
	coord := newTestCoordinator(t, nil)
	stage := physical.Stage{
		ID:           "exchange-gather-agg-0",
		Type:         physical.StageExchangeGather,
		Dependencies: []string{"agg-0"},
	}
	// Supply two synthetic partial batches keyed by dep "agg-0".
	partials := map[string][]byte{"agg-0-worker-0": /* batch bytes */, "agg-0-worker-1": /* batch bytes */}

	out, err := coord.orchestrateGather(ctx, stage, partials)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("expected merged output")
	}
}
```

(If the existing merge helper expects a different argument shape, adapt accordingly — reuse the same shape.)

- [ ] **Step 3: Run to verify it fails**

```bash
go test ./internal/coordinator/ -run TestOrchestrateGather -v
```
Expected: FAIL (`undefined`).

- [ ] **Step 4: Implement**

```go
// internal/coordinator/orchestrate_gather.go
package coordinator

import (
	"context"
	"fmt"

	"wadjet/internal/planner/physical"
)

// orchestrateGather is the coordinator-side backend for
// StageExchangeGather. It wraps the pre-existing merge path so that
// stage-DAG dispatch can call a named backend per Exchange type.
func (c *Coordinator) orchestrateGather(
	ctx context.Context,
	stage physical.Stage,
	partials /* match existing merge-helper arg type */,
) (/* match existing merge-helper return type */, error) {
	if stage.Type != physical.StageExchangeGather {
		return nil, fmt.Errorf("orchestrate gather: wrong stage type %q", stage.Type)
	}
	// Delegate to the existing merge helper. No new merge logic in this phase.
	return c.mergePartials(ctx, partials /* or whatever the helper is called */)
}
```

- [ ] **Step 5: Run the test**

```bash
go test ./internal/coordinator/ -run TestOrchestrateGather -v
go test ./internal/coordinator/... 2>&1 | tail -20
```
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add internal/coordinator/orchestrate_gather.go \
        internal/coordinator/orchestrate_gather_test.go
git commit -m "feat(coordinator): add orchestrateGather backend wrapping existing merge path"
```

---

## Task 14: Stage-DAG dispatch in coordinator (behind the flag)

Add a new dispatch path that iterates stages and dispatches per `Type`. Select between dispatch and legacy switch by `p.UseEnsureDistribution` (read at query-plan time and threaded through the plan).

**Files:**
- Modify: `internal/coordinator/coordinator.go`
- Modify: `internal/coordinator/coordinator_coverage_test.go` (or add a new file)

- [ ] **Step 1: Read the current switch**

```bash
sed -n '460,610p' internal/coordinator/coordinator.go
```

Record the exact function boundary and the shape of the input the switch consumes.

- [ ] **Step 2: Write the failing test**

```go
// internal/coordinator/dag_dispatch_test.go
package coordinator

import (
	"context"
	"testing"

	"wadjet/internal/planner/physical"
)

func TestDispatchStageDAG_RoutesPerType(t *testing.T) {
	ctx := context.Background()
	coord := newTestCoordinator(t, nil)
	plan := &physical.PhysicalPlan{
		Stages: []physical.Stage{
			{ID: "scan-0", Type: physical.StageScan},
			{ID: "rep-0", Type: physical.StageExchangeReplicate, Dependencies: []string{"scan-0"}},
			{ID: "gather-0", Type: physical.StageExchangeGather, Dependencies: []string{"rep-0"}},
		},
	}
	calls := map[string]int{}
	coord.dispatchHooks = &dispatchHooks{
		onRepartition: func(physical.Stage) { calls["rep"]++ },
		onReplicate:   func(physical.Stage) { calls["repl"]++ },
		onGather:      func(physical.Stage) { calls["gather"]++ },
		onPipeline:    func(physical.Stage) { calls["pipe"]++ },
		onScan:        func(physical.Stage) { calls["scan"]++ },
	}
	if err := coord.dispatchStageDAG(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if calls["repl"] != 1 || calls["gather"] != 1 {
		t.Errorf("dispatch counts: %v", calls)
	}
}
```

- [ ] **Step 3: Run to verify it fails**

```bash
go test ./internal/coordinator/ -run TestDispatchStageDAG -v
```
Expected: FAIL.

- [ ] **Step 4: Implement `dispatchStageDAG`**

In `internal/coordinator/coordinator.go`, add:

```go
// dispatchHooks is a test seam for observing per-stage dispatch
// routing. Production has it nil; tests wire per-type callbacks.
type dispatchHooks struct {
	onRepartition func(physical.Stage)
	onReplicate   func(physical.Stage)
	onGather      func(physical.Stage)
	onPipeline    func(physical.Stage)
	onScan        func(physical.Stage)
}

// dispatchStageDAG walks plan.Stages in dependency order (already
// topo-sorted by the planner) and dispatches each stage to the
// appropriate backend by Type. Replaces the four-mode switch for
// plans produced with UseEnsureDistribution=true.
func (c *Coordinator) dispatchStageDAG(ctx context.Context, plan *physical.PhysicalPlan) error {
	for _, s := range plan.Stages {
		if hook := c.hookFor(s.Type); hook != nil {
			hook(s)
		}
		switch s.Type {
		case physical.StageExchangeRepartition:
			if _, err := c.orchestrateRepartition(ctx, s /* … */); err != nil {
				return fmt.Errorf("repartition %s: %w", s.ID, err)
			}
		case physical.StageExchangeReplicate:
			if _, err := c.orchestrateReplicate(ctx, s /* … */); err != nil {
				return fmt.Errorf("replicate %s: %w", s.ID, err)
			}
		case physical.StageExchangeGather:
			if _, err := c.orchestrateGather(ctx, s /* … */); err != nil {
				return fmt.Errorf("gather %s: %w", s.ID, err)
			}
		case physical.StagePipeline, physical.StageScan, physical.StageAggregate,
			physical.StageSort, physical.StageHashJoin, physical.StageBroadcastJoin,
			physical.StageWindow:
			if err := c.dispatchPipelineStage(ctx, s /* … */); err != nil {
				return fmt.Errorf("pipeline %s: %w", s.ID, err)
			}
		default:
			return fmt.Errorf("dispatchStageDAG: unknown stage type %q (stage %s)", s.Type, s.ID)
		}
	}
	return nil
}

func (c *Coordinator) hookFor(t string) func(physical.Stage) {
	if c.dispatchHooks == nil {
		return nil
	}
	switch t {
	case physical.StageExchangeRepartition:
		return c.dispatchHooks.onRepartition
	case physical.StageExchangeReplicate:
		return c.dispatchHooks.onReplicate
	case physical.StageExchangeGather:
		return c.dispatchHooks.onGather
	case physical.StagePipeline:
		return c.dispatchHooks.onPipeline
	case physical.StageScan:
		return c.dispatchHooks.onScan
	}
	return nil
}
```

And add a `dispatchHooks *dispatchHooks` field to the `Coordinator` struct.

Route the caller: where the coordinator currently enters the four-mode switch, wrap:

```go
if plan.UsedEnsureDistribution {
    return c.dispatchStageDAG(ctx, plan)
}
// ... existing switch unchanged ...
```

`UsedEnsureDistribution` is a new boolean on `physical.PhysicalPlan` set by `PlanDistributed` when `p.UseEnsureDistribution` was true.

- [ ] **Step 5: Add the plan flag**

In `plan.go`:

```go
type PhysicalPlan struct {
	Pipeline              *exec.Pipeline
	Stages                []Stage
	Cleanup               func()
	UsedEnsureDistribution bool // set by PlanDistributed when UseEnsureDistribution=true
}
```

Set it in `PlanDistributed` just after the `EnsureDistribution` call:

```go
	if p.UseEnsureDistribution {
		// …existing EnsureDistribution call…
		phys.UsedEnsureDistribution = true
	}
```

- [ ] **Step 6: Run the tests**

```bash
go test ./internal/coordinator/ -run TestDispatchStageDAG -v
go test ./... 2>&1 | tail -30
```
Expected: new test passes; existing tests still green (default flag path unchanged).

- [ ] **Step 7: Commit**

```bash
git add internal/coordinator/coordinator.go \
        internal/coordinator/dag_dispatch_test.go \
        internal/planner/physical/plan.go
git commit -m "feat(coordinator): add dispatchStageDAG behind UseEnsureDistribution flag"
```

---

## Task 15: Parity harness — dual-path execution for every TPC-H query

Runs each query under **both** legacy-switch and new-dispatch paths; asserts bit-identical result batches.

**Files:**
- Create: `internal/coordinator/parity_harness_test.go`

- [ ] **Step 1: Write the harness**

```go
// internal/coordinator/parity_harness_test.go
package coordinator

import (
	"context"
	"os"
	"reflect"
	"testing"

	"wadjet/benchmarks/tpch"
)

// TestParity_LegacyVsEnsureDistribution runs each TPC-H query through
// both the legacy four-mode switch and the new EnsureDistribution +
// dispatchStageDAG path, asserting bit-identical row batches.
//
// Gated behind PARITY=1 to avoid doubling SF0.01 test time in default
// runs. Removed in the final phase-2 cleanup commit.
func TestParity_LegacyVsEnsureDistribution(t *testing.T) {
	if os.Getenv("PARITY") != "1" {
		t.Skip("PARITY=1 not set")
	}
	ctx := context.Background()
	for _, q := range tpch.QueryList() {
		t.Run(q.Name, func(t *testing.T) {
			legacy := runQueryWithFlag(t, ctx, q, false)
			modern := runQueryWithFlag(t, ctx, q, true)
			if !reflect.DeepEqual(legacy, modern) {
				t.Fatalf("%s: result batches diverge\n--- legacy\n%#v\n--- modern\n%#v",
					q.Name, legacy, modern)
			}
		})
	}
}

func runQueryWithFlag(t *testing.T, ctx context.Context, q tpch.Query, useED bool) [][]any {
	t.Helper()
	coord := newTPCHCoordinator(t) // reuse TPC-H test harness
	coord.planner.UseEnsureDistribution = useED
	return coord.runAndCollect(ctx, q.SQL)
}
```

Adapt argument shapes to whatever the TPC-H test harness actually exports. If `tpch.QueryList()` / `runAndCollect` don't exist verbatim, use the corresponding helpers from `benchmarks/tpch/` — **do not invent new harness code; reuse what's there.**

- [ ] **Step 2: Run both modes**

```bash
PARITY=1 go test ./internal/coordinator/ -run TestParity -v 2>&1 | tail -40
```
Expected: all 22 pairwise comparisons PASS.

If a query diverges: `EnsureDistribution` emits a plan whose execution produces different rows than the switch. This is a **bug** in either `RequiredChildDistribution`, `OutputDistribution`, or `EnsureDistribution`. Iterate — each fix gets its own commit with a regression test added to `ensure_distribution_test.go`.

- [ ] **Step 3: Commit**

```bash
git add internal/coordinator/parity_harness_test.go
git commit -m "test(coordinator): PARITY=1 dual-path harness for EnsureDistribution"
```

---

## Task 16: SF1 local benchmark parity gate

**Files:** none created — this is a benchmarking step.

- [ ] **Step 1: Baseline on main**

```bash
git stash
git checkout main
TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/ 2>&1 | tee /tmp/bench-main.txt
git checkout feat/distribution-property-phase-2
git stash pop || true
```

- [ ] **Step 2: Branch (flag off) — regression-ness check**

```bash
TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/ 2>&1 | tee /tmp/bench-branch-off.txt
```

Expected: within ±2% of main (flag off → legacy switch → no functional change).

- [ ] **Step 3: Branch (flag on)**

```bash
WADJET_PLANNER_USE_ENSURE_DIST=1 TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/ 2>&1 | tee /tmp/bench-branch-on.txt
```

Compare per-query wall time against `/tmp/bench-main.txt`. Build a simple diff table (awk/script or spreadsheet). Record the result in the commit message of the next task.

- [ ] **Step 4: Decide gate outcome**

- If **every query within ±5%** of main: proceed to Task 17.
- If **Q17 regresses >5%** (likely, due to deleted `preComputeDerivedAggregate`): insert **Task 16.5 (Phase 2.5)** — port `preComputeDerivedAggregate` as a logical-plan rewrite rule producing a `MaterializedAggregate` node that `EnsureDistribution` handles natively. Write the rule + tests + re-run the SF1 gate. This is a separate in-band plan extension; do not skip.
- If **any other query regresses >5%**: bug in the DAG translation. Fix, add regression test, re-run. Do not proceed.

- [ ] **Step 5: Commit the bench artefacts (if they're checked-in style in the repo — otherwise save to desktop notes)**

If the repo convention records benchmark runs somewhere like `docs/perf/`:
```bash
cp /tmp/bench-branch-on.txt docs/perf/sf1-phase2-flag-on.txt
git add docs/perf/sf1-phase2-flag-on.txt
git commit -m "perf(tpch): SF1 parity gate for phase 2 (within 5% of main baseline)"
```

If not, skip the commit; record the outcome in the PR description.

---

## Task 17: Flip default: `UseEnsureDistribution = true`

**Files:**
- Modify: `internal/planner/physical/plan.go`

- [ ] **Step 1: Change the `Planner` constructor (or zero-value) so the default is true**

```bash
grep -n 'func NewPlanner\|UseEnsureDistribution' internal/planner/physical/plan.go
```

Modify so that `Planner{}.UseEnsureDistribution == true` unless explicitly set otherwise. Concrete pattern: make the field `DisableEnsureDistribution` (inverted), or have `NewPlanner` set `p.UseEnsureDistribution = true`.

If inverting: rename throughout tests that explicitly set it.

- [ ] **Step 2: Run full suite**

```bash
go test ./... 2>&1 | tail -40
TPCH_SCALE=0.01 go test -v -run TestTPCHQueries ./benchmarks/tpch/ 2>&1 | tail -20
```
Expected: all pass.

- [ ] **Step 3: Commit**

```bash
git add internal/planner/physical/plan.go
git commit -m "feat(planner): default UseEnsureDistribution=true"
```

---

## Task 18: SF10 EC2 parity gate

Per `feedback_deploy_preflight.md` and `feedback_baseline_first.md`: deploy main baseline first, then this branch. Teardown immediately after per `feedback_ec2_teardown_discipline.md`. Get explicit user approval first per `feedback_no_auto_deploy.md`.

- [ ] **Step 1: Ask the user for explicit approval to spend ~$2 on SF10 runs**

Post the deploy plan to the user. Wait for approval. Do not proceed without it.

- [ ] **Step 2: Cross-compile `cmd/tpch-bench` for linux/arm64**

```bash
GOOS=linux GOARCH=arm64 go build -o /tmp/tpch-bench ./cmd/tpch-bench
ls -lh /tmp/tpch-bench
```
Expected size: ~41 MB (per `feedback_deploy_binary.md`).

- [ ] **Step 3: Upload to S3**

```bash
AWS_PROFILE=citc aws s3 cp /tmp/tpch-bench s3://<bucket>/bin/latest/tpch-bench
```
Per `feedback_s3_binary_path.md`: path is `bin/latest/`.

- [ ] **Step 4: Deploy main baseline first**

Check out main, build, upload, deploy. Instance types per `feedback_benchmark_consistency.md`: coordinator = `c7g.2xlarge`, workers = `c7g.4xlarge` × 3, on-demand (`-var=use_spot=false`). Record per-query wall times.

- [ ] **Step 5: Tear down main cluster immediately after results collect**

- [ ] **Step 6: Deploy branch, collect per-query times, tear down**

- [ ] **Step 7: Compare; record outcome**

Gate: every query within ±5% of baseline (22/22 correct). Record result table in the Phase 2 PR description.

- [ ] **Step 8: If gated, commit the result table**

```bash
echo "<table>" > docs/perf/sf10-phase2-vs-main-2026-04-XX.md
git add docs/perf/sf10-phase2-vs-main-2026-04-XX.md
git commit -m "perf(tpch): SF10 EC2 parity run phase 2 vs main baseline"
```

If SF10 reveals a regression unseen at SF1: Phase 2 is not ready. Triage; fix; re-run. Do not proceed to the deletion task until green.

---

## Task 19: Delete the legacy switch and heuristic routing functions

**Files:**
- Modify: `internal/coordinator/coordinator.go`
- Delete / modify: `internal/planner/physical/plan.go` (`CanProbeSplit`, `PickShuffleCandidate`, `ShuffleCandidate`, `MergeInfo`, `ExtractMergeInfo`, `has_merge` threading)
- Delete / modify: `internal/planner/physical/aggregate_shuffle.go` (`PickAggregateShuffleCandidate`)
- Delete / modify: `internal/coordinator/aggregate_shuffle.go` (`preComputeDerivedAggregate`)

- [ ] **Step 1: Remove the four-mode switch**

```bash
grep -n 'shuffleApplicable\|canProbeSplit\|mergeInfo ' internal/coordinator/coordinator.go
```

Delete lines that constitute the switch (roughly `:472–605`). Replace with an unconditional call to `dispatchStageDAG`:

```go
return c.dispatchStageDAG(ctx, plan)
```

Any helpers the switch called exclusively (`extractMergeInfo`, etc.) are now dead.

- [ ] **Step 2: Delete `CanProbeSplit`**

```bash
grep -n 'CanProbeSplit\|canProbeSplit' internal/planner/physical/
```

Remove the function (and its callers, which should all be in the now-deleted switch). Remove the test file section testing it, but keep a single **regression test** in `ensure_distribution_test.go` that asserts the equivalent DAG shape emerges (probe-split source + broadcast builds).

- [ ] **Step 3: Delete `PickShuffleCandidate` + `ShuffleCandidate`**

```bash
grep -n 'PickShuffleCandidate\|ShuffleCandidate' internal/planner/physical/ internal/coordinator/
```

Remove. Retain one regression test in `ensure_distribution_test.go`: a scenario where the old pick would have chosen side X, and assert the new plan emits a Repartition on side X.

- [ ] **Step 4: Delete `PickAggregateShuffleCandidate` + the `aggregate_shuffle.go` routing file**

```bash
git rm internal/planner/physical/aggregate_shuffle.go
grep -rn 'PickAggregateShuffleCandidate' internal/
```

Any remaining references must die. Retain the empirical detection helpers (`IsDerivedAggregateBuild`, etc.) if `EnsureDistribution` uses them — otherwise delete.

- [ ] **Step 5: Delete `preComputeDerivedAggregate`**

```bash
git rm internal/coordinator/aggregate_shuffle.go
grep -rn 'preComputeDerivedAggregate\|PreComputedAggregate' internal/
```

Also delete the `PreComputedAggregates` field from `Stage` if nothing else references it.

- [ ] **Step 6: Delete `MergeInfo`, `ExtractMergeInfo`, `has_merge` field**

```bash
grep -rn 'MergeInfo\|ExtractMergeInfo\|has_merge\|HasMerge' internal/
```

Remove from `plan.go`. Any references in the coordinator are either in the now-deleted switch or need to migrate to "the stage DAG terminates in a Gather iff a Gather is needed."

- [ ] **Step 7: Run everything**

```bash
go build ./...
go test ./... 2>&1 | tail -40
TPCH_SCALE=0.01 go test -v -run TestTPCHQueries ./benchmarks/tpch/ 2>&1 | tail -20
```
Expected: all green. Any red = a deletion removed behaviour the switch covered that `EnsureDistribution` doesn't. Add the missing case to `EnsureDistribution` / `RequiredChildDistribution` with a test, then continue deletion.

- [ ] **Step 8: Commit (one deletion, one commit — not all at once)**

Split into ~4 commits for reviewability:

```bash
git commit -am "refactor(coordinator): delete four-mode routing switch"
git commit -am "refactor(planner): delete CanProbeSplit and PickShuffleCandidate"
git commit -am "refactor(planner): delete PickAggregateShuffleCandidate and PreComputedAggregates"
git commit -am "refactor(planner,coordinator): delete MergeInfo and preComputeDerivedAggregate"
```

---

## Task 20: Delete the feature flag and parity harness

Final cleanup. No dual path.

**Files:**
- Modify: `internal/planner/physical/plan.go` (drop `UseEnsureDistribution` / `UsedEnsureDistribution`)
- Modify: callers that set the flag
- Modify: `internal/coordinator/coordinator.go` (drop the flag check)
- Delete: `internal/coordinator/parity_harness_test.go`

- [ ] **Step 1: Remove the flag**

```bash
grep -rn 'UseEnsureDistribution\|UsedEnsureDistribution\|DisableEnsureDistribution' internal/ benchmarks/ cmd/
```

Remove the field. Remove every caller that sets it. Remove the conditional in `dispatchStageDAG` routing (it's unconditional now).

- [ ] **Step 2: Remove `BehaviorPreservingMode` toggle**

After this point, `AssertExchangeConsistency` should always run strict. Delete the per-call toggle in `PlanDistributed`. Consider deleting `BehaviorPreservingMode` itself if no other caller flips it (`grep` first).

- [ ] **Step 3: Delete the parity harness**

```bash
git rm internal/coordinator/parity_harness_test.go
```

- [ ] **Step 4: Run the whole suite + SF0.01 correctness**

```bash
go test ./... 2>&1 | tail -40
TPCH_SCALE=0.01 go test -v -run TestTPCHQueries ./benchmarks/tpch/ 2>&1 | tail -20
```
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: drop UseEnsureDistribution flag and parity harness"
```

---

## Task 21: Final SF1 run + PR

- [ ] **Step 1: SF1 one more time**

```bash
TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/ 2>&1 | tee /tmp/bench-final.txt
```

Expected: ±5% of the original main baseline.

- [ ] **Step 2: Push branch + open PR**

```bash
git push -u origin feat/distribution-property-phase-2
gh pr create --title "Distribution-property phase 2: exchange insertion + retire switch" --body "$(cat <<'EOF'
## Summary

Implements Phase 2 of the distribution-property pass
(spec: `docs/_archive/specs/2026-04-20-distribution-property-phase-2-design.md`).

Replaces the coordinator-level four-mode routing switch with a
property-based Exchange insertion pass (`EnsureDistribution`). Three
first-class Exchange stage types added. Four heuristic routing functions
deleted.

Behaviour-preserving refactor. SF1 ±5%; SF10 parity validated on EC2
(see `docs/perf/sf10-phase2-vs-main-*.md`).

## Test plan

- [ ] `go test ./...` green
- [ ] TPC-H SF0.01 22/22
- [ ] TPC-H SF1 ±5% of main (per `/tmp/bench-final.txt`)
- [ ] TPC-H SF10 EC2 parity (per commit in this branch)
- [ ] No `UseEnsureDistribution` flag remaining
- [ ] No `parity_harness_test.go` remaining
- [ ] Golden snapshots under `internal/planner/physical/testdata/ensure_distribution/` match current planner output

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Record memory for next session**

Write `/home/dwright/.claude/projects/-home-dwright-Projects-caelum/memory/project_distribution_phase_2_ready_<date>.md` pointing at the PR.

Add an index line to `MEMORY.md`.
