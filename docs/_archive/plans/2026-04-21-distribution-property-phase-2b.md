> **ARCHIVED — superseded design note.** Kept for design lineage only; it does not describe the current code. Current positions: `docs/adr/` (decisions), `docs/internals/` (code maps), `docs/design/` (active memos). Search skips `docs/_archive/` by default (`.ignore`); use `rg --no-ignore` to include it.

# Distribution Property Phase 2b: dispatchStageDAG Execution Integration


**Goal:** Delete the legacy four-mode switch at `internal/coordinator/coordinator.go:541-605` and route all distributed queries through a lowering pass (`lowerExchangeDAG`) that consumes Exchange stages produced by the Phase 2a `EnsureDistribution` planner pass.

**Architecture:** Fat structural Exchange stages (planner decides distribution; stages carry `BuildAlias`, `ProbeAlias`, `BuildBytes`, keys, partition count). Coordinator lowers the Exchange DAG into the existing single-`pipeline-0` runtime shape plus side channels (`shuffleTasks`, `mergeInfo`). Pipeline executor is untouched. Pickers (`PickShuffleCandidate`, `CanProbeSplit`) are deleted — their logic moves into `EnsureDistribution`. `UseEnsureDistribution` flag is deleted at the end; rollback is `git revert`.

**Tech Stack:** Go 1.22+, table-driven unit tests, TPC-H parity gates (SF0.01, SF1 local, SF10 EC2 A/B).

**Spec:** `docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md` (commit `b42c4ed`).

**Base branch:** `feat/distribution-property-phase-2` (contains Phase 2a — PR #45). All commits from this plan land on the same branch.

---

## Task 1: Add `Exchange *ExchangeStage` to `Stage`; extend the payload struct

**Files:**
- Modify: `internal/planner/physical/exchange.go`
- Modify: `internal/planner/physical/plan.go:35-121` (Stage struct)

- [ ] **Step 1: Extend the `ExchangeStage` payload struct**

In `internal/planner/physical/exchange.go`, replace the existing `ExchangeStage` definition (lines 24-31) with:

```go
// ExchangeStage carries the per-variant payload attached to an Exchange
// Stage. Stored on Stage.Exchange (pointer) so non-Exchange stages pay
// no memory cost.
//
// Keys, Count are Repartition-only. Ordering is Gather-only.
// BuildAlias, ProbeAlias, BuildBytes are populated by EnsureDistribution
// on Repartition and (BuildAlias/ProbeAlias only) Replicate stages, so
// the coordinator lowering pass can synthesize ShuffleCandidate without
// calling PickShuffleCandidate.
type ExchangeStage struct {
	Keys       []string      // Repartition only
	Count      int           // Repartition only
	Ordering   []SortKeySpec // Gather only (optional sort-merge gather)
	BuildAlias string        // Repartition, Replicate
	ProbeAlias string        // Repartition, Replicate
	BuildBytes int64         // Repartition (for logging / threshold checks)
}
```

- [ ] **Step 2: Add the pointer field to Stage**

In `internal/planner/physical/plan.go`, add this field to `Stage` immediately after the `Distribution` field (currently the last field, at line 120):

```go
	// Exchange carries per-variant metadata for StageExchange* stages.
	// nil for non-Exchange stages.
	Exchange *ExchangeStage
```

- [ ] **Step 3: Verify the package still builds**

Run: `go build ./internal/planner/physical/...`
Expected: no errors. No tests run yet — this is a pure additive change.

- [ ] **Step 4: Commit**

```bash
git add internal/planner/physical/exchange.go internal/planner/physical/plan.go
git commit -m "feat(planner): add Stage.Exchange + BuildAlias/ProbeAlias/BuildBytes fields

Exchange *ExchangeStage carries per-variant metadata for StageExchange* stages.
Adds BuildAlias, ProbeAlias, BuildBytes to the payload so the Phase 2b
coordinator lowering can synthesize ShuffleCandidate without calling pickers.

Phase 2b spec: docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md"
```

---

## Task 2: Migrate `ShuffleKeys` / `NumPartitions` from flat Stage fields onto `Stage.Exchange`

This is a mechanical rename. The flat fields today are read in ~8 places; each moves to `stage.Exchange.Keys` / `stage.Exchange.Count`. Non-Exchange stages never read these fields, so the migration is confined.

**Files:**
- Modify: `internal/planner/physical/plan.go:74-76` (remove flat fields)
- Modify: `internal/planner/physical/exchange.go` (already has Keys/Count — good)
- Modify: `internal/planner/physical/ensure_distribution.go:198-206` (update `exchangeVariantFor`)
- Modify: `internal/planner/physical/distribution.go:228-232` (update OutputDistribution extraction)
- Modify: `internal/planner/physical/aggregate_shuffle.go` (update references)
- Modify: `internal/planner/physical/plan_tpch_test.go:215-220` (update assertions)
- Modify: `internal/planner/physical/ensure_distribution_test.go:27-49` (update test fixtures)
- Modify: `internal/planner/physical/shuffle_insertion_test.go` (if it reads ShuffleKeys/NumPartitions)
- Update: any remaining `stage.ShuffleKeys` / `stage.NumPartitions` readers under `internal/`

- [ ] **Step 1: Find every reader of the flat fields**

Run: `grep -rn "\.ShuffleKeys\|\.NumPartitions" internal/ --include='*.go' | grep -v '_test.go' > /tmp/readers.txt; cat /tmp/readers.txt`
Expected: a finite list (likely 10-20 lines). Each line is either a read that must move to `.Exchange.Keys`/`.Exchange.Count` or a write that must be redirected.

Also list test readers: `grep -rn "\.ShuffleKeys\|\.NumPartitions" internal/ --include='*_test.go' >> /tmp/readers.txt`

- [ ] **Step 2: Update `exchangeVariantFor` to populate `Exchange` instead of flat fields**

In `internal/planner/physical/ensure_distribution.go`, replace the `RequiredHashPartitionedOn, RequiredClusteredOn` branch in `exchangeVariantFor` (lines 201-206):

```go
	case RequiredHashPartitionedOn, RequiredClusteredOn:
		return Stage{
			Type: StageExchangeRepartition,
			Exchange: &ExchangeStage{
				Keys:  append([]string(nil), req.Keys...),
				Count: req.Count,
			},
		}, true
```

- [ ] **Step 3: Update the `OutputDistribution` extraction for Repartition**

In `internal/planner/physical/distribution.go`, find the block near line 228 that reads `stage.ShuffleKeys` / `stage.NumPartitions` and replace with reads from `stage.Exchange`:

```go
	case StageExchangeRepartition:
		if stage.Exchange == nil {
			return Distribution{Kind: DistHashPartitioned}
		}
		return Distribution{
			Kind:  DistHashPartitioned,
			Keys:  stage.Exchange.Keys,
			Count: stage.Exchange.Count,
		}
```

If there is a similar block near line 165 reading the same fields, apply the same transformation.

- [ ] **Step 4: Update `aggregate_shuffle.go` to read `Exchange.Keys` where it currently reads `ShuffleKeys`**

In `internal/planner/physical/aggregate_shuffle.go`, find any `s.ShuffleKeys` / `s.NumPartitions` references and route them through `s.Exchange`:

```go
if s.Type == StageExchangeRepartition && s.Exchange != nil {
    keys := s.Exchange.Keys
    // ...
}
```

If a reference appears inside the switch at `aggregate_shuffle.go:216`, it is a type-check only (`s.Type == StageExchangeRepartition`) — no field read to update.

- [ ] **Step 5: Delete the flat fields from Stage**

In `internal/planner/physical/plan.go`, remove the `// Shuffle metadata` block (lines 74-76):

```go
	// Shuffle metadata
	ShuffleKeys   []string // columns to hash-partition on
	NumPartitions int      // number of output partitions (also used on join stages)
```

**Note:** the comment claims `NumPartitions` is also used on join stages. If your grep from Step 1 shows any `join_stage.NumPartitions` reader, that reader needs to be re-homed too. Join-stage partition count is a Phase 3 concern; for Phase 2b, if a join reader exists and is not on a StageExchangeRepartition path, migrate it to a per-stage field on Stage (e.g., `JoinPartitionCount int`). Do not conflate with Exchange.Count.

- [ ] **Step 6: Update test assertions**

In `internal/planner/physical/plan_tpch_test.go:215-220`:

```go
if s.Type == StageExchangeRepartition {
    if s.Exchange == nil {
        t.Errorf("%s: repartition stage %s has nil Exchange payload", queryName, s.ID)
        continue
    }
    if len(s.Exchange.Keys) == 0 {
        t.Errorf("%s: shuffle stage %s has no ShuffleKeys", queryName, s.ID)
    }
    if s.Exchange.Count < 2 {
        t.Errorf("%s: shuffle stage %s has <2 partitions: %d", queryName, s.ID, s.Exchange.Count)
    }
}
```

In `internal/planner/physical/ensure_distribution_test.go:27-49`, convert the fixture to use `Exchange`:

```go
{
    req: RequiredDistribution{Kind: RequiredHashPartitionedOn, Keys: []string{"a", "b"}, Count: 4},
    want: Stage{
        Type: StageExchangeRepartition,
        Exchange: &ExchangeStage{Keys: []string{"a", "b"}, Count: 4},
    },
},
```

Update the comparator block:

```go
if c.want.Type == StageExchangeRepartition {
    if c.want.Exchange == nil || got.Exchange == nil {
        t.Errorf("Exchange payload nil: got=%v want=%v", got.Exchange, c.want.Exchange)
    } else {
        if !reflect.DeepEqual(got.Exchange.Keys, c.want.Exchange.Keys) {
            t.Errorf("Exchange.Keys: got %v want %v", got.Exchange.Keys, c.want.Exchange.Keys)
        }
        if got.Exchange.Count != c.want.Exchange.Count {
            t.Errorf("Exchange.Count: got %d want %d", got.Exchange.Count, c.want.Exchange.Count)
        }
    }
}
```

Apply the same pattern to any other test fixture with `ShuffleKeys:`/`NumPartitions:`.

- [ ] **Step 7: Regenerate the 22 golden snapshots**

Phase 2a committed goldens at `internal/planner/physical/testdata/tpch-ensure-distribution/Q*.golden` (verify path). The snapshot format changes because Exchange payload now lives under `Exchange` instead of flat fields.

Run: `UPDATE_GOLDENS=1 go test ./internal/planner/physical/... -run Golden`
Expected: 22 goldens rewritten. Diff should show field relocations only, no structural DAG changes.

If goldens live at a different path, locate them via `git show --stat d1e8175` (the Phase 2a golden commit).

- [ ] **Step 8: Run planner tests**

Run: `go test ./internal/planner/...`
Expected: PASS (22 tests, zero failures).

- [ ] **Step 9: Run planner with race detector**

Run: `go test -race ./internal/planner/...`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add -u internal/planner/physical/ internal/planner/physical/testdata/
git commit -m "refactor(planner): migrate Stage.ShuffleKeys/NumPartitions onto Stage.Exchange

Mechanical rename ahead of Phase 2b coordinator lowering pass. Exchange
payload now lives on a dedicated struct; non-Exchange stages pay no cost.
Regenerates 22 TPC-H goldens to reflect the new field layout.

Phase 2b spec: docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md"
```

---

## Task 3: Populate `BuildAlias`, `ProbeAlias`, `BuildBytes` in `EnsureDistribution`

Today `EnsureDistribution` inserts an Exchange stage via `exchangeVariantFor(req)` without access to the parent stage's build/probe role metadata. For Phase 2b, the parent context is known at insertion time — the parent is the consumer (a join, pipeline, or aggregate), and the slot index (`0` = left/probe, `1` = right/build) tells us which side the Exchange stage feeds.

**Files:**
- Modify: `internal/planner/physical/ensure_distribution.go` (insertion site + slot handling)
- Create: `internal/planner/physical/ensure_distribution_fields_test.go`

- [ ] **Step 1: Write the failing test for Repartition field population**

Create `internal/planner/physical/ensure_distribution_fields_test.go`:

```go
package physical

import (
	"testing"
)

// TestEnsureDistributionPopulatesExchangeFields verifies that when
// EnsureDistribution inserts a StageExchangeRepartition above a join's
// build-side scan, the inserted stage's Exchange.BuildAlias matches the
// scan alias and Exchange.ProbeAlias matches the sibling scan alias.
func TestEnsureDistributionPopulatesExchangeFields_Repartition(t *testing.T) {
	// Two scans feeding a hash join. Both below shuffle threshold so
	// EnsureDistribution is the only source of shuffle insertion here
	// (this test does not depend on PickShuffleCandidate).
	stages := []Stage{
		{
			ID:             "scan-orders",
			Type:           StageScan,
			ScanAlias:      "orders",
			EstimatedBytes: 1 << 30,
			Distribution:   Distribution{Kind: DistSingleton},
		},
		{
			ID:             "scan-lineitem",
			Type:           StageScan,
			ScanAlias:      "lineitem",
			EstimatedBytes: 4 << 30,
			Distribution:   Distribution{Kind: DistSingleton},
		},
		{
			ID:            "join-0",
			Type:          StageHashJoin,
			LeftDepStage:  "scan-lineitem",
			RightDepStage: "scan-orders",
			JoinLeftKeys:  []string{"l_orderkey"},
			JoinRightKeys: []string{"o_orderkey"},
			Dependencies:  []string{"scan-lineitem", "scan-orders"},
		},
	}

	// Force EnsureDistribution to insert Repartition on the build side by
	// overriding RequiredChildDistribution for slot 1 of join-0.
	prev := requiredChildDistributionForTest
	t.Cleanup(func() { requiredChildDistributionForTest = prev })
	requiredChildDistributionForTest = func(s Stage, slot int) (RequiredDistribution, bool) {
		if s.ID == "join-0" && slot == 1 {
			return RequiredDistribution{
				Kind:  RequiredHashPartitionedOn,
				Keys:  []string{"o_orderkey"},
				Count: 4,
			}, true
		}
		return RequiredDistribution{}, false
	}

	out, err := EnsureDistribution(stages, 4)
	if err != nil {
		t.Fatalf("EnsureDistribution: %v", err)
	}

	var repart *Stage
	for i := range out {
		if out[i].Type == StageExchangeRepartition {
			repart = &out[i]
			break
		}
	}
	if repart == nil {
		t.Fatalf("no StageExchangeRepartition inserted; stages: %+v", stageTypes(out))
	}
	if repart.Exchange == nil {
		t.Fatalf("Repartition.Exchange is nil")
	}
	if repart.Exchange.BuildAlias != "orders" {
		t.Errorf("Exchange.BuildAlias = %q, want %q", repart.Exchange.BuildAlias, "orders")
	}
	if repart.Exchange.ProbeAlias != "lineitem" {
		t.Errorf("Exchange.ProbeAlias = %q, want %q", repart.Exchange.ProbeAlias, "lineitem")
	}
	if repart.Exchange.BuildBytes != (1 << 30) {
		t.Errorf("Exchange.BuildBytes = %d, want %d", repart.Exchange.BuildBytes, 1<<30)
	}
}

// TestEnsureDistributionPopulatesExchangeFields_Replicate verifies that
// a StageExchangeReplicate inserted on a broadcast-required join slot
// carries BuildAlias (the replicated scan) and ProbeAlias (the sibling).
func TestEnsureDistributionPopulatesExchangeFields_Replicate(t *testing.T) {
	stages := []Stage{
		{
			ID: "scan-nation", Type: StageScan, ScanAlias: "nation",
			EstimatedBytes: 1 << 10,
			Distribution:   Distribution{Kind: DistSingleton},
		},
		{
			ID: "scan-customer", Type: StageScan, ScanAlias: "customer",
			EstimatedBytes: 1 << 30,
			Distribution:   Distribution{Kind: DistSingleton},
		},
		{
			ID: "join-0", Type: StageHashJoin,
			LeftDepStage:  "scan-customer",
			RightDepStage: "scan-nation",
			Dependencies:  []string{"scan-customer", "scan-nation"},
		},
	}

	prev := requiredChildDistributionForTest
	t.Cleanup(func() { requiredChildDistributionForTest = prev })
	requiredChildDistributionForTest = func(s Stage, slot int) (RequiredDistribution, bool) {
		if s.ID == "join-0" && slot == 1 {
			return RequiredDistribution{Kind: RequiredBroadcast}, true
		}
		return RequiredDistribution{}, false
	}

	out, err := EnsureDistribution(stages, 4)
	if err != nil {
		t.Fatalf("EnsureDistribution: %v", err)
	}

	var repl *Stage
	for i := range out {
		if out[i].Type == StageExchangeReplicate {
			repl = &out[i]
			break
		}
	}
	if repl == nil {
		t.Fatalf("no StageExchangeReplicate inserted; stages: %+v", stageTypes(out))
	}
	if repl.Exchange == nil {
		t.Fatalf("Replicate.Exchange is nil")
	}
	if repl.Exchange.BuildAlias != "nation" {
		t.Errorf("Exchange.BuildAlias = %q, want %q", repl.Exchange.BuildAlias, "nation")
	}
	if repl.Exchange.ProbeAlias != "customer" {
		t.Errorf("Exchange.ProbeAlias = %q, want %q", repl.Exchange.ProbeAlias, "customer")
	}
}
```

- [ ] **Step 2: Run tests, verify they fail**

Run: `go test ./internal/planner/physical/ -run TestEnsureDistributionPopulatesExchangeFields -v`
Expected: FAIL. The inserted Exchange stages have `Exchange.BuildAlias == ""` because `exchangeVariantFor` does not yet populate it.

- [ ] **Step 3: Add a helper to resolve build/probe aliases from the parent context**

In `internal/planner/physical/ensure_distribution.go`, add this helper near the bottom of the file (after `exchangeVariantFor`):

```go
// resolveExchangeRoles walks the stages slice to find the scan alias for
// a given child stage ID and its sibling (the scan on the other input
// slot of the parent). Used by EnsureDistribution to populate
// Exchange.BuildAlias / Exchange.ProbeAlias at insertion time.
//
// For a parent join with LeftDepStage=probe-scan, RightDepStage=build-scan:
//   slot=1 (build side) → BuildAlias=scanAliasOf(build-scan), ProbeAlias=scanAliasOf(probe-scan)
//   slot=0 (probe side) → BuildAlias=scanAliasOf(probe-scan), ProbeAlias=scanAliasOf(build-scan)
//
// Returns ("","",0) when the child or sibling cannot be resolved to a
// scan (e.g., when the child is a derived stage, not a direct scan).
// Non-fatal: the lowering pass tolerates empty aliases by falling back
// to PickShuffleCandidate equivalent logic during the equivalence window.
func resolveExchangeRoles(stages []Stage, parent Stage, slot int) (buildAlias, probeAlias string, buildBytes int64) {
	slots := dependencySlots(&parent)
	if len(slots) < 2 {
		// Single-input stage (e.g., pipeline, aggregate). Treat the one
		// input as the build side; there is no sibling probe.
		if len(slots) == 1 {
			childID := slots[0].get(&parent)
			if scan, ok := findScanByStageID(stages, childID); ok {
				return scan.ScanAlias, "", scan.EstimatedBytes
			}
		}
		return "", "", 0
	}
	var childID, siblingID string
	for _, s := range slots {
		if s.idx == slot {
			childID = s.get(&parent)
		} else {
			siblingID = s.get(&parent)
		}
	}
	buildScan, bok := findScanByStageID(stages, childID)
	probeScan, pok := findScanByStageID(stages, siblingID)
	if bok {
		buildAlias = buildScan.ScanAlias
		buildBytes = buildScan.EstimatedBytes
	}
	if pok {
		probeAlias = probeScan.ScanAlias
	}
	return buildAlias, probeAlias, buildBytes
}

// findScanByStageID returns the Stage with the given ID iff it is a scan.
// Returns (Stage{}, false) otherwise.
func findScanByStageID(stages []Stage, id string) (Stage, bool) {
	for _, s := range stages {
		if s.ID == id && s.Type == StageScan {
			return s, true
		}
	}
	return Stage{}, false
}
```

- [ ] **Step 4: Wire the helper into the insertion site**

In `EnsureDistribution`, between the `exch, ok := exchangeVariantFor(req)` call and `exch.ID = ...` (around line 86-93), inject field population:

```go
	exch, ok := exchangeVariantFor(req)
	if !ok {
		return nil, fmt.Errorf(
			"ensure distribution: no exchange variant satisfies %v from %v (parent=%s slot=%d)",
			req, actual, out[i].ID, slot.idx,
		)
	}
	// Populate structural role fields so the Phase 2b coordinator lowering
	// can synthesize ShuffleCandidate without calling PickShuffleCandidate.
	if exch.Type == StageExchangeRepartition || exch.Type == StageExchangeReplicate {
		if exch.Exchange == nil {
			exch.Exchange = &ExchangeStage{}
		}
		buildAlias, probeAlias, buildBytes := resolveExchangeRoles(out, parentSnapshot, slot.idx)
		exch.Exchange.BuildAlias = buildAlias
		exch.Exchange.ProbeAlias = probeAlias
		exch.Exchange.BuildBytes = buildBytes
	}
	exch.ID = fmt.Sprintf("%s-%s-%d", exch.Type, out[i].ID, i)
```

- [ ] **Step 5: Run the new tests, verify they pass**

Run: `go test ./internal/planner/physical/ -run TestEnsureDistributionPopulatesExchangeFields -v`
Expected: PASS (2 tests).

- [ ] **Step 6: Run full planner test suite**

Run: `go test ./internal/planner/...`
Expected: PASS. No existing tests should regress — the new fields are populated but not yet read.

- [ ] **Step 7: Commit**

```bash
git add internal/planner/physical/ensure_distribution.go internal/planner/physical/ensure_distribution_fields_test.go
git commit -m "feat(planner): populate Exchange.BuildAlias/ProbeAlias/BuildBytes in EnsureDistribution

Resolves scan aliases for inserted Exchange stages by walking parent
dependency slots. Populates the fields the Phase 2b coordinator lowering
pass will read instead of calling PickShuffleCandidate at dispatch time.

Phase 2b spec: docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md"
```

---

## Task 4: Equivalence test — `EnsureDistribution` fields vs. `PickShuffleCandidate` output across 22 TPC-H plans

Before deleting the pickers, prove that the fields populated by `EnsureDistribution` match what `PickShuffleCandidate` and `CanProbeSplit` return today. Any divergence means a query will reroute.

**Files:**
- Create: `internal/planner/physical/exchange_equivalence_test.go`

- [ ] **Step 1: Write the equivalence test**

Create `internal/planner/physical/exchange_equivalence_test.go`:

```go
package physical

import (
	"testing"
)

// TestExchangeFieldsMatchPickers walks all 22 TPC-H plans and asserts
// that for every StageExchangeRepartition inserted by EnsureDistribution,
// the stage's Exchange.BuildAlias / ProbeAlias / BuildBytes agree with
// what PickShuffleCandidate would return on the same plan.
//
// A one-shot pre-deletion check. After Phase 2b lands this test is
// obsolete (the pickers are gone); delete it in the same commit that
// deletes PickShuffleCandidate.
func TestExchangeFieldsMatchPickers(t *testing.T) {
	// Reuse the TPC-H harness used by the existing Phase 2a tests.
	// `tpchPlans()` returns a map[string][]Stage of post-EnsureDistribution
	// plans keyed by query name. If the harness helper lives under a
	// different name, substitute it.
	plans := tpchPlansWithEnsureDistribution(t)

	for name, stages := range plans {
		t.Run(name, func(t *testing.T) {
			// Find the first (and only, for Phase 2b) Repartition stage.
			var repart *Stage
			for i := range stages {
				if stages[i].Type == StageExchangeRepartition {
					repart = &stages[i]
					break
				}
			}
			if repart == nil {
				return // Not all queries shuffle.
			}
			if repart.Exchange == nil {
				t.Fatalf("Repartition stage has nil Exchange payload")
			}

			// Call the picker on the pre-EnsureDistribution plan.
			// PickShuffleCandidate is tolerant of Exchange stages in the
			// input — it filters by scan type — so feeding it the post-
			// EnsureDistribution slice is correct.
			cand, ok := PickShuffleCandidate(stages, shuffleBuildThresholdForTest())
			if !ok {
				t.Fatalf("PickShuffleCandidate returned false but EnsureDistribution inserted a Repartition")
			}
			if cand.BuildAlias != repart.Exchange.BuildAlias {
				t.Errorf("BuildAlias: picker=%q exchange=%q", cand.BuildAlias, repart.Exchange.BuildAlias)
			}
			if cand.ProbeAlias != repart.Exchange.ProbeAlias {
				t.Errorf("ProbeAlias: picker=%q exchange=%q", cand.ProbeAlias, repart.Exchange.ProbeAlias)
			}
			if cand.BuildBytes != repart.Exchange.BuildBytes {
				t.Errorf("BuildBytes: picker=%d exchange=%d", cand.BuildBytes, repart.Exchange.BuildBytes)
			}
		})
	}
}

// Test helper: returns all 22 TPC-H plans run through EnsureDistribution.
// If a suitable harness already exists under plan_tpch_test.go (which
// contains the SF0.01 acceptance gate), extract/rename. Otherwise
// implement minimally here.
func tpchPlansWithEnsureDistribution(t *testing.T) map[string][]Stage {
	t.Helper()
	// Placeholder: if plan_tpch_test.go already has a loop over Q01..Q22
	// calling Planner.PlanDistributed, reuse it. Implement as a shared
	// helper in a new file if the existing test doesn't expose one.
	t.Skip("tpchPlansWithEnsureDistribution helper not yet extracted; wire it up before running")
	return nil
}

// shuffleBuildThresholdForTest mirrors the coordinator's default so the
// picker sees the same threshold the runtime will see.
func shuffleBuildThresholdForTest() int64 {
	return 4 << 30 // 4 GiB, matches coordinator.shuffleBuildThreshold default.
}
```

- [ ] **Step 2: Locate or extract the TPC-H plan harness**

Run: `grep -n "Planner.UseEnsureDistribution = true" internal/planner/physical/plan_tpch_test.go`
Expected: line ~815-975 already loops over Q01..Q22. Locate the harness and either:
  - extract a shared helper `tpchPlansWithEnsureDistribution(t *testing.T)` into `internal/planner/physical/tpch_harness_test.go`, or
  - inline the loop into `TestExchangeFieldsMatchPickers` directly.

If extracting, the helper should return `map[string][]Stage` keyed by query name.

- [ ] **Step 3: Remove the `t.Skip` stub in the test**

Replace the placeholder `tpchPlansWithEnsureDistribution` body with a real call to the extracted harness (or inline the loop).

- [ ] **Step 4: Run the equivalence test**

Run: `go test ./internal/planner/physical/ -run TestExchangeFieldsMatchPickers -v`
Expected: PASS on all 22 queries. Any FAIL indicates Task 3's `resolveExchangeRoles` disagrees with `PickShuffleCandidate` — debug before proceeding.

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/exchange_equivalence_test.go internal/planner/physical/tpch_harness_test.go
git commit -m "test(planner): equivalence test for Exchange fields vs PickShuffleCandidate

One-shot pre-deletion check. Verifies that the fields EnsureDistribution
populates on inserted Repartition stages agree with PickShuffleCandidate's
output on the same 22 TPC-H plans. Delete when pickers are removed.

Phase 2b spec: docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md"
```

---

## Task 5: Implement `lowerExchangeDAG` — Repartition branch

**Files:**
- Modify: `internal/coordinator/dag_dispatch.go` (rename to `dag_lower.go` in Task 8; stay named for now)
- Create: `internal/coordinator/dag_lower_test.go`

- [ ] **Step 1: Write a failing test for the Repartition-only lowering**

Create `internal/coordinator/dag_lower_test.go`:

```go
package coordinator

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// TestLowerExchangeDAG_Repartition feeds a plan with one Repartition
// stage and asserts the output tuple matches what the legacy
// shuffle branch produces today: one pipeline-0 stage + shuffleTasks
// keyed by "pipeline-0" with layout-derived probe tasks.
func TestLowerExchangeDAG_Repartition(t *testing.T) {
	c := newTestCoordinator(t)
	stages := []physical.Stage{
		{ID: "scan-orders", Type: physical.StageScan, ScanAlias: "orders", EstimatedBytes: 1 << 30},
		{ID: "scan-lineitem", Type: physical.StageScan, ScanAlias: "lineitem", EstimatedBytes: 4 << 30},
		{
			ID:           "exchange-repartition-join-0-2",
			Type:         physical.StageExchangeRepartition,
			Dependencies: []string{"scan-orders"},
			Exchange: &physical.ExchangeStage{
				Keys:       []string{"o_orderkey"},
				Count:      8,
				BuildAlias: "orders",
				ProbeAlias: "lineitem",
				BuildBytes: 1 << 30,
			},
		},
		{
			ID:            "join-0",
			Type:          physical.StageHashJoin,
			LeftDepStage:  "scan-lineitem",
			RightDepStage: "exchange-repartition-join-0-2",
			JoinLeftKeys:  []string{"l_orderkey"},
			JoinRightKeys: []string{"o_orderkey"},
			Dependencies:  []string{"scan-lineitem", "exchange-repartition-join-0-2"},
		},
	}

	lowered, shuffleTasks, mergeInfo, err := c.lowerExchangeDAG(
		context.Background(), "q-test", "SELECT 1", stages, nil /*logicalPlan*/, 4 /*workerCount*/, nil /*preComputedAggregates*/)
	if err != nil {
		t.Fatalf("lowerExchangeDAG: %v", err)
	}
	if len(lowered) != 1 || lowered[0].ID != "pipeline-0" {
		t.Fatalf("lowered stages: got %+v, want single pipeline-0", lowered)
	}
	if mergeInfo != nil {
		t.Errorf("mergeInfo: got %+v, want nil (no Gather in this plan)", mergeInfo)
	}
	tasks, ok := shuffleTasks["pipeline-0"]
	if !ok || len(tasks) == 0 {
		t.Fatalf("shuffleTasks[pipeline-0]: got %v, want non-empty", tasks)
	}
	// Probe tasks are sized by NumPartitions (8), not workerCount (4).
	// buildShufflePipelineTasks materializes one task per partition.
	if len(tasks) != 8 {
		t.Errorf("probe task count: got %d, want 8 (NumPartitions)", len(tasks))
	}
}
```

The `newTestCoordinator(t)` helper must exist somewhere in the coordinator test package — if not, inline a minimal constructor that returns a `*Coordinator` with `c.workers` stubbed to 4 workers and `c.config.ResultBucket = "test-bucket"`.

- [ ] **Step 2: Verify the test fails**

Run: `go test ./internal/coordinator/ -run TestLowerExchangeDAG_Repartition -v`
Expected: FAIL — `lowerExchangeDAG` is not yet defined (current `dispatchStageDAG` has empty case bodies and wrong signature).

- [ ] **Step 3: Rewrite `dispatchStageDAG` → `lowerExchangeDAG` with the Repartition branch**

Replace the current body of `internal/coordinator/dag_dispatch.go` (keeping the file path for this task; rename in Task 8):

```go
package coordinator

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// hasExchangeStages reports whether the given plan contains any
// StageExchange* stage. Retained for callsites that still gate on it;
// Phase 2b makes it always-true in practice but the helper stays.
func hasExchangeStages(stages []physical.Stage) bool {
	for _, s := range stages {
		switch s.Type {
		case physical.StageExchangeRepartition,
			physical.StageExchangeReplicate,
			physical.StageExchangeGather:
			return true
		}
	}
	return false
}

// lowerExchangeDAG walks the Exchange-annotated stage DAG and produces
// the coordinator runtime's single-pipeline-0 shape plus side channels.
// Replaces the legacy four-mode switch at coordinator.go:541-605.
//
// Output tuple matches the pre-Phase-2b switch:
//   - loweredStages: []{pipeline-0 with ProbeSplit/BuildCache/PreComputedAggregates}
//   - shuffleTasks:  map["pipeline-0"] → probe tasks (non-nil only when a Repartition ran)
//   - mergeInfo:     coordinator-side merge metadata (non-nil iff a Gather stage is present)
func (c *Coordinator) lowerExchangeDAG(
	ctx context.Context,
	queryID, sql string,
	stages []physical.Stage,
	logicalPlan *logical.Node,
	workerCount int,
	preComputedAggregates []physical.PreComputedAggregateMeta,
) (loweredStages []physical.Stage,
	shuffleTasks map[string][]distributed.Task,
	mergeInfo *logical.MergeInfo,
	err error) {

	// Accumulate side-channel state as we walk the DAG.
	var probeSplitAlias string
	var probeSplitFiles []string
	var buildCache map[string][]string
	var hasRepartition bool
	var hasReplicate bool
	var hasGather bool
	var layout *ShuffleLayout

	for _, s := range stages {
		switch s.Type {
		case physical.StageExchangeRepartition:
			if s.Exchange == nil {
				return nil, nil, nil, fmt.Errorf("lowerExchangeDAG: Repartition stage %s has nil Exchange payload", s.ID)
			}
			// When Q17 pre-compute has populated preComputedAggregates,
			// the Repartition cannot run — its key derivation assumes a
			// pristine build scan that has been replaced by a cache.
			// Fall through to the probe-split-with-cache path handled
			// by the Replicate branch below.
			if len(preComputedAggregates) > 0 {
				c.logger.Info("lowerExchangeDAG: skipping Repartition due to preComputedAggregates",
					"query", queryID, "stage", s.ID)
				continue
			}
			cand := physical.ShuffleCandidate{
				BuildAlias: s.Exchange.BuildAlias,
				ProbeAlias: s.Exchange.ProbeAlias,
				BuildKeys:  s.Exchange.Keys,
				ProbeKeys:  s.Exchange.Keys, // Phase 2b: same key set, mirrors PickShuffleCandidate's JoinKeys.
				JoinKeys:   s.Exchange.Keys,
				BuildBytes: s.Exchange.BuildBytes,
			}
			layout, err = c.orchestrateRepartition(ctx, queryID, cand, stages, workerCount)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("lowerExchangeDAG: orchestrateRepartition: %w", err)
			}
			hasRepartition = true

		case physical.StageExchangeReplicate:
			hasReplicate = true
			// Implementation arrives in Task 6.
			_ = s

		case physical.StageExchangeGather:
			hasGather = true
			// Implementation arrives in Task 7.
		}
	}

	// Build the single pipeline-0 stage the runtime expects.
	ps := physical.Stage{ID: "pipeline-0", Type: physical.StagePipeline}

	switch {
	case hasRepartition:
		probeTasks := buildShufflePipelineTasks(queryID, sql, c.config.ResultBucket, layout, workerCount)
		ps.Tasks = len(probeTasks)
		shuffleTasks = map[string][]distributed.Task{"pipeline-0": probeTasks}
	case hasReplicate:
		ps.Tasks = workerCount
		ps.ProbeSplitAlias = probeSplitAlias
		ps.ProbeSplitFiles = probeSplitFiles
		ps.BuildCachePreScans = buildCache
		ps.PreComputedAggregates = preComputedAggregates
	default:
		ps.Tasks = 1
	}

	_ = hasGather // Consumed by Task 7 (mergeInfo extraction).

	loweredStages = []physical.Stage{ps}
	return loweredStages, shuffleTasks, mergeInfo, nil
}
```

Delete the old `dispatchStageDAG` function, the `dispatchHooks` struct, and the `hookFor` method from this file — they are superseded by `lowerExchangeDAG`'s per-branch unit tests. The existing dispatch-hooks tests in `dag_dispatch_test.go` will need corresponding deletion in Task 9.

- [ ] **Step 4: Remove broken references to `dispatchHooks`**

Run: `grep -rn "dispatchHooks" internal/coordinator/`
Expected: references in `coordinator.go:97-99` (struct field) and `dag_dispatch_test.go`. Remove the `dispatchHooks *dispatchHooks` field from `Coordinator`. The test file will fail to compile — that's fine; Task 9 deletes it wholesale.

For now, comment out the body of `internal/coordinator/dag_dispatch_test.go` by wrapping the package-level content in `//go:build ignore_phase2b_transitional`, or delete the file contents temporarily:

```bash
printf 'package coordinator\n' > internal/coordinator/dag_dispatch_test.go
```

- [ ] **Step 5: Re-run the Repartition test**

Run: `go test ./internal/coordinator/ -run TestLowerExchangeDAG_Repartition -v`
Expected: PASS.

- [ ] **Step 6: Run the full coordinator test suite**

Run: `go test ./internal/coordinator/...`
Expected: PASS (or no new failures beyond the emptied `dag_dispatch_test.go`).

- [ ] **Step 7: Commit**

```bash
git add internal/coordinator/dag_dispatch.go internal/coordinator/dag_dispatch_test.go internal/coordinator/dag_lower_test.go internal/coordinator/coordinator.go
git commit -m "feat(coordinator): lowerExchangeDAG Repartition branch

Replaces dispatchStageDAG skeleton with a real lowering pass. Walks
the Exchange-annotated DAG, synthesizes a ShuffleCandidate from
StageExchangeRepartition's Exchange fields, runs the existing
orchestrateRepartition backend, and emits a pipeline-0 stage plus
shuffleTasks map matching the legacy switch output. Replicate and
Gather branches follow in subsequent commits.

Phase 2b spec: docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md"
```

---

## Task 6: Implement `lowerExchangeDAG` — Replicate branch

**Files:**
- Modify: `internal/coordinator/dag_dispatch.go`
- Modify: `internal/coordinator/dag_lower_test.go`

- [ ] **Step 1: Write a failing test for Replicate-only lowering**

Append to `internal/coordinator/dag_lower_test.go`:

```go
// TestLowerExchangeDAG_Replicate feeds a plan with one Replicate
// stage and asserts the output is a single pipeline-0 stage carrying
// ProbeSplitAlias / ProbeSplitFiles / BuildCachePreScans. No
// shuffleTasks are emitted.
func TestLowerExchangeDAG_Replicate(t *testing.T) {
	c := newTestCoordinator(t)
	stages := []physical.Stage{
		{
			ID: "scan-orders", Type: physical.StageScan, ScanAlias: "orders",
			EstimatedBytes: 15 << 30,
			ScanFiles:      []string{"s3://bench/orders/0001.parquet", "s3://bench/orders/0002.parquet"},
		},
		{
			ID: "scan-lineitem", Type: physical.StageScan, ScanAlias: "lineitem",
			EstimatedBytes: 60 << 30,
			ScanFiles:      []string{"s3://bench/lineitem/0001.parquet", "s3://bench/lineitem/0002.parquet"},
		},
		{
			ID:           "exchange-replicate-join-0-2",
			Type:         physical.StageExchangeReplicate,
			Dependencies: []string{"scan-orders"},
			Exchange: &physical.ExchangeStage{
				BuildAlias: "orders",
				ProbeAlias: "lineitem",
			},
		},
		{
			ID:            "join-0",
			Type:          physical.StageHashJoin,
			LeftDepStage:  "scan-lineitem",
			RightDepStage: "exchange-replicate-join-0-2",
			Dependencies:  []string{"scan-lineitem", "exchange-replicate-join-0-2"},
		},
	}

	lowered, shuffleTasks, mergeInfo, err := c.lowerExchangeDAG(
		context.Background(), "q-test", "SELECT 1", stages, nil, 4, nil)
	if err != nil {
		t.Fatalf("lowerExchangeDAG: %v", err)
	}
	if len(lowered) != 1 || lowered[0].ID != "pipeline-0" {
		t.Fatalf("lowered stages: got %+v, want single pipeline-0", lowered)
	}
	if len(shuffleTasks) != 0 {
		t.Errorf("shuffleTasks: got %v, want empty", shuffleTasks)
	}
	if mergeInfo != nil {
		t.Errorf("mergeInfo: got %+v, want nil", mergeInfo)
	}
	p := lowered[0]
	if p.ProbeSplitAlias != "lineitem" {
		t.Errorf("ProbeSplitAlias: got %q, want %q", p.ProbeSplitAlias, "lineitem")
	}
	if len(p.ProbeSplitFiles) != 2 {
		t.Errorf("ProbeSplitFiles: got %d, want 2", len(p.ProbeSplitFiles))
	}
	if p.Tasks != 4 {
		t.Errorf("Tasks: got %d, want 4 (workerCount)", p.Tasks)
	}
}
```

- [ ] **Step 2: Verify the test fails**

Run: `go test ./internal/coordinator/ -run TestLowerExchangeDAG_Replicate -v`
Expected: FAIL — Replicate branch is a no-op in Task 5's code.

- [ ] **Step 3: Implement the Replicate branch**

In `internal/coordinator/dag_dispatch.go`, replace the `case physical.StageExchangeReplicate:` body inside `lowerExchangeDAG` with:

```go
		case physical.StageExchangeReplicate:
			hasReplicate = true
			if s.Exchange == nil {
				return nil, nil, nil, fmt.Errorf("lowerExchangeDAG: Replicate stage %s has nil Exchange payload", s.ID)
			}
			probeSplitAlias = s.Exchange.ProbeAlias
			// Enumerate probe files from the probe scan stage.
			for _, ss := range stages {
				if ss.Type == physical.StageScan && ss.ScanAlias == probeSplitAlias {
					probeSplitFiles = append([]string(nil), ss.ScanFiles...)
					break
				}
			}
			// Pre-scan build tables once; workers consume the cache
			// instead of re-scanning from S3 N times.
			cache, cacheErr := c.orchestrateReplicate(ctx, s, queryID, sql, stages, probeSplitAlias)
			if cacheErr != nil {
				return nil, nil, nil, fmt.Errorf("lowerExchangeDAG: orchestrateReplicate: %w", cacheErr)
			}
			if buildCache == nil {
				buildCache = cache
			} else {
				for k, v := range cache {
					buildCache[k] = v
				}
			}
```

Note: check `orchestrateReplicate`'s current signature in `internal/coordinator/build_cache.go:346`. If it differs from `(ctx, stage, queryID, sql, allStages, probeAlias) (map[string][]string, error)`, adapt the call — the spec named this function as a thin wrapper over `preScanBuildTables` already extant.

- [ ] **Step 4: Re-run the test**

Run: `go test ./internal/coordinator/ -run TestLowerExchangeDAG_Replicate -v`
Expected: PASS.

- [ ] **Step 5: Run the full coordinator suite**

Run: `go test ./internal/coordinator/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/coordinator/dag_dispatch.go internal/coordinator/dag_lower_test.go
git commit -m "feat(coordinator): lowerExchangeDAG Replicate branch

Emits pipeline-0 with ProbeSplitAlias/ProbeSplitFiles/BuildCachePreScans
derived from StageExchangeReplicate's Exchange payload. Calls existing
orchestrateReplicate shim. No shuffleTasks emitted in this path.

Phase 2b spec: docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md"
```

---

## Task 7: Implement `lowerExchangeDAG` — Gather branch

**Files:**
- Modify: `internal/coordinator/dag_dispatch.go`
- Modify: `internal/coordinator/dag_lower_test.go`

- [ ] **Step 1: Write a failing test for Gather branch**

Append to `internal/coordinator/dag_lower_test.go`:

```go
import (
	// ... existing imports ...
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// TestLowerExchangeDAG_Gather feeds a plan with a final Gather stage
// and asserts mergeInfo is populated from the logical plan via
// ExtractMergeInfo. The actual gather runs post-execution.
func TestLowerExchangeDAG_Gather(t *testing.T) {
	c := newTestCoordinator(t)
	logicalPlan := &logical.Node{
		// Minimal logical plan with an aggregate + order + limit that
		// ExtractMergeInfo will see. Shape depends on logical.Node's
		// constructors; if a helper like logical.NewTestAggregateOrderLimit
		// exists, prefer that. Otherwise construct directly:
		Type: logical.NodeLimit,
		Limit: 10,
		Children: []*logical.Node{
			{
				Type:    logical.NodeSort,
				OrderBy: []logical.OrderExpr{{Col: "revenue", Desc: true}},
			},
		},
	}
	stages := []physical.Stage{
		{ID: "scan-0", Type: physical.StageScan, ScanAlias: "t"},
		{
			ID:           "exchange-gather-scan-0",
			Type:         physical.StageExchangeGather,
			Dependencies: []string{"scan-0"},
			Exchange:     &physical.ExchangeStage{},
		},
	}

	_, _, mergeInfo, err := c.lowerExchangeDAG(
		context.Background(), "q-test", "SELECT ...", stages, logicalPlan, 4, nil)
	if err != nil {
		t.Fatalf("lowerExchangeDAG: %v", err)
	}
	if mergeInfo == nil {
		t.Fatalf("mergeInfo: nil, want populated (Gather stage present)")
	}
	if mergeInfo.Limit != 10 {
		t.Errorf("mergeInfo.Limit: got %d, want 10", mergeInfo.Limit)
	}
}
```

If the `logical.Node` literal is wrong for this codebase, consult `internal/planner/logical/plan.go` and adjust. The test's job is: a Gather stage + a non-nil logical plan produces a non-nil `mergeInfo`.

- [ ] **Step 2: Verify the test fails**

Run: `go test ./internal/coordinator/ -run TestLowerExchangeDAG_Gather -v`
Expected: FAIL — Gather branch currently does nothing.

- [ ] **Step 3: Implement the Gather branch**

In `dag_dispatch.go`, replace the `case physical.StageExchangeGather:` body:

```go
		case physical.StageExchangeGather:
			hasGather = true
			if logicalPlan == nil {
				// Defensive: some tests may not provide a logical plan.
				// Leave mergeInfo nil — downstream merge path treats
				// nil as "no merge metadata, just concatenate partials".
				continue
			}
			if mergeInfo == nil {
				mergeInfo = logical.ExtractMergeInfo(logicalPlan)
			}
```

Remove the now-unused `_ = hasGather` line at the bottom of `lowerExchangeDAG`.

- [ ] **Step 4: Re-run the test**

Run: `go test ./internal/coordinator/ -run TestLowerExchangeDAG_Gather -v`
Expected: PASS.

- [ ] **Step 5: Run the full coordinator suite**

Run: `go test ./internal/coordinator/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/coordinator/dag_dispatch.go internal/coordinator/dag_lower_test.go
git commit -m "feat(coordinator): lowerExchangeDAG Gather branch

Populates mergeInfo from the logical plan via ExtractMergeInfo when a
StageExchangeGather is present in the DAG. The gather itself runs
post-execution via the existing mergeProbePartials path; this branch
only surfaces the side-channel metadata.

Phase 2b spec: docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md"
```

---

## Task 8: Rename file and add single-worker fallback test

**Files:**
- Rename: `internal/coordinator/dag_dispatch.go` → `internal/coordinator/dag_lower.go`
- Rename: `internal/coordinator/dag_dispatch_test.go` → (delete; already emptied in Task 5)
- Modify: `internal/coordinator/dag_lower_test.go`

- [ ] **Step 1: Rename the file**

```bash
git mv internal/coordinator/dag_dispatch.go internal/coordinator/dag_lower.go
git rm internal/coordinator/dag_dispatch_test.go
```

- [ ] **Step 2: Write a failing test for the single-worker fallback**

Append to `internal/coordinator/dag_lower_test.go`:

```go
// TestLowerExchangeDAG_SingleWorkerFallback asserts that when the DAG
// contains no Exchange stages (small query, coordinator-only path),
// the lowering emits a single pipeline-0 stage with Tasks=1 and no
// side channels. Mirrors the legacy default switch branch.
func TestLowerExchangeDAG_SingleWorkerFallback(t *testing.T) {
	c := newTestCoordinator(t)
	stages := []physical.Stage{
		{ID: "scan-0", Type: physical.StageScan, ScanAlias: "t"},
	}

	lowered, shuffleTasks, mergeInfo, err := c.lowerExchangeDAG(
		context.Background(), "q-test", "SELECT 1", stages, nil, 4, nil)
	if err != nil {
		t.Fatalf("lowerExchangeDAG: %v", err)
	}
	if len(lowered) != 1 {
		t.Fatalf("lowered stages: got %d, want 1", len(lowered))
	}
	if lowered[0].Tasks != 1 {
		t.Errorf("Tasks: got %d, want 1 (single-worker fallback)", lowered[0].Tasks)
	}
	if len(shuffleTasks) != 0 {
		t.Errorf("shuffleTasks: got %v, want empty", shuffleTasks)
	}
	if mergeInfo != nil {
		t.Errorf("mergeInfo: got %+v, want nil", mergeInfo)
	}
}
```

- [ ] **Step 3: Verify it passes**

Run: `go test ./internal/coordinator/ -run TestLowerExchangeDAG_SingleWorkerFallback -v`
Expected: PASS — the `default` branch in the switch at the end of `lowerExchangeDAG` already handles this shape.

- [ ] **Step 4: Run the full coordinator suite**

Run: `go test ./internal/coordinator/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator/dag_lower.go internal/coordinator/dag_lower_test.go
git commit -m "refactor(coordinator): rename dag_dispatch → dag_lower; add no-exchange fallback test

File rename reflects Phase 2b's role change (test-seam dispatcher → production
lowering pass). Adds a regression test for the zero-Exchange-stage case so
the single-worker fallback cannot silently regress to workerCount tasks.

Phase 2b spec: docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md"
```

---

## Task 9: Wire `lowerExchangeDAG` into `executeDistributed`; delete the four-mode switch

This is the cut-over commit. High blast radius; keep the diff minimal and surgical.

**Files:**
- Modify: `internal/coordinator/coordinator.go:476-609` (replace switch with lowerExchangeDAG call)

- [ ] **Step 1: Replace the switch block**

In `internal/coordinator/coordinator.go`, delete lines 476-609 (from the `// Route all queries through pipeline execution.` comment through the closing brace of the `default` branch) and replace with:

```go
	// Route all queries through the Exchange DAG lowering pass (Phase 2b).
	// The pass consumes Exchange stages inserted by EnsureDistribution and
	// emits the single-pipeline-0 runtime shape the executor below expects,
	// plus shuffleTasks and mergeInfo side channels.
	workerCount := c.workers.Count()

	// Q17 / aggregate-shuffle pre-compute runs BEFORE lowering: its
	// output (preComputedAggregates) is attached to the pipeline-0 stage
	// produced by the Replicate branch. Preserved inline for C-minimal
	// scope; lift into a logical rewrite in a later phase.
	var preComputedAggregates []physical.PreComputedAggregateMeta
	if mergeInfoForQ17 := logical.ExtractMergeInfo(logicalPlan); mergeInfoForQ17 != nil {
		if diag := physical.PickAggregateShuffleCandidateDiag(physStages, aggregateShuffleThreshold); diag.Reason == physical.AggShuffleRejectNone {
			aggCand := diag.Candidate
			c.logger.Info("aggregate-shuffle candidate matched, dispatching pre-compute",
				"query", queryID,
				"agg_stage", aggCand.AggregateStageID,
				"input_scan", aggCand.InputScanAlias,
				"input_bytes", aggCand.InputScanBytes,
				"threshold", aggregateShuffleThreshold,
				"group_by", aggCand.GroupByKeys)
			cacheFiles, preErr := c.preComputeDerivedAggregate(ctx, queryID, aggCand, physStages)
			if preErr != nil {
				c.logger.Warn("aggregate pre-compute failed, falling back to in-plan execution",
					"query", queryID, "err", preErr)
			} else if len(cacheFiles) > 0 {
				meta, metaErr := buildPreComputedAggregateMeta(aggCand, physStages, cacheFiles)
				if metaErr != nil {
					c.logger.Warn("aggregate-shuffle metadata construction failed, falling back",
						"query", queryID, "err", metaErr)
				} else {
					preComputedAggregates = append(preComputedAggregates, meta)
				}
			}
		}
	}

	loweredStages, shuffleTasks, probeSplitMergeInfo, lowerErr := c.lowerExchangeDAG(
		ctx, queryID, sql, physStages, logicalPlan, workerCount, preComputedAggregates)
	if lowerErr != nil {
		return nil, fmt.Errorf("lowerExchangeDAG failed for query %s: %w", queryID, lowerErr)
	}
	physStages = loweredStages
```

- [ ] **Step 2: Verify all downstream references still resolve**

Run: `go build ./internal/coordinator/...`
Expected: build succeeds. If `shuffleTasks` or `probeSplitMergeInfo` is referenced but not declared elsewhere in `executeDistributed`, the variable names above must match the existing downstream code exactly. Grep: `grep -n "shuffleTasks\|probeSplitMergeInfo" internal/coordinator/coordinator.go` and confirm the names match.

- [ ] **Step 3: Run the coordinator unit tests**

Run: `go test ./internal/coordinator/...`
Expected: PASS.

- [ ] **Step 4: Run the SF0.01 TPC-H correctness gate**

Run: `go test -v -run TestTPCHQueries ./benchmarks/tpch/`
Expected: 22/22 PASS. This is the load-bearing correctness check for Task 9.

If any query fails: do not proceed. Bisect which Exchange stage type's lowering is wrong. Likely suspects: `Repartition.BuildKeys`/`ProbeKeys` synthesized identically in Task 5 (may need separate left/right keys); Replicate's probe file enumeration.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator/coordinator.go
git commit -m "feat(coordinator): replace four-mode switch with lowerExchangeDAG

Routes every distributed query through the Exchange DAG lowering pass.
The inline Q17 aggregate-shuffle pre-compute branch stays (C-minimal
scope). lowerExchangeDAG synthesizes ShuffleCandidate from Exchange
stage fields and reproduces the legacy switch's pipeline-0 shape +
shuffleTasks + mergeInfo side channels. Pipeline executor unchanged.

Phase 2b spec: docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md"
```

---

## Task 10: Delete `UseEnsureDistribution` flag and dispatcher-role pickers

The flag served only to gate Phase 2a's planner-side changes. Phase 2b makes the lowering unconditional; the flag is noise.

**Files:**
- Modify: `internal/planner/physical/plan.go:191-196` (remove flag definition)
- Modify: `internal/planner/physical/plan.go:999` (remove flag guard around EnsureDistribution call)
- Modify: `internal/planner/physical/plan_tpch_test.go:731-975` (remove flag sets from tests)
- Modify: `internal/coordinator/dag_lower.go` (remove flag reference in comment)
- Modify: `benchmarks/tpch/parity_test.go:9-24` (remove flag-not-reachable note)
- Delete: `internal/planner/physical/plan.go:1079-1115` (`CanProbeSplit` function)
- Delete: `internal/planner/physical/plan.go:1150-???` (`PickShuffleCandidate` function)
- Modify: any call-sites of the two deleted pickers (should be none after Task 9; verify)

- [ ] **Step 1: Verify the pickers have no remaining callers**

Run: `grep -rn "PickShuffleCandidate\|CanProbeSplit" internal/ benchmarks/ --include='*.go' | grep -v '_test.go'`
Expected: empty (production callers are gone after Task 9).

Run: `grep -rn "PickShuffleCandidate\|CanProbeSplit" internal/ benchmarks/ --include='*_test.go'`
Expected: only the equivalence test from Task 4 (`exchange_equivalence_test.go`) and possibly unit tests for the pickers themselves.

- [ ] **Step 2: Delete the equivalence test (it served its one-shot purpose)**

```bash
git rm internal/planner/physical/exchange_equivalence_test.go
```

- [ ] **Step 3: Delete `PickShuffleCandidate` and its unit tests**

Remove the function body from `internal/planner/physical/plan.go` (starting at the `// PickShuffleCandidate identifies` comment and running to the function's closing brace). Delete `internal/planner/physical/shuffle_insertion_test.go` if it tests only the picker; otherwise delete the test functions that directly exercise `PickShuffleCandidate`.

- [ ] **Step 4: Delete `CanProbeSplit`**

Remove the function from `internal/planner/physical/plan.go` (lines 1079-end-of-function). Delete any unit tests that exercise it directly.

- [ ] **Step 5: Delete `ShuffleCandidate` struct**

Remove the `type ShuffleCandidate struct { ... }` definition from `internal/planner/physical/plan.go`. Callers (if any remain in coordinator) should construct it inline — but Task 9 already did that via `orchestrateRepartition`'s signature. Verify: `grep -rn "ShuffleCandidate" internal/ benchmarks/ --include='*.go'`. Expected: only `orchestrateRepartition`'s signature reference in coordinator + the inline construction in `lowerExchangeDAG`. If so, also delete the `physical.ShuffleCandidate` parameter name and inline its fields on `orchestrateRepartition`, OR keep `ShuffleCandidate` as a coordinator-internal struct. Simplest: keep the type but move it to `internal/coordinator/orchestrate_repartition.go` as `type shuffleCandidate struct` (unexported). Update `lowerExchangeDAG`'s construction accordingly.

Pick the minimum-churn approach: if ShuffleCandidate is only read by `orchestrateRepartition` after pickers die, move the type to the coordinator package and make it unexported.

- [ ] **Step 6: Delete the `UseEnsureDistribution` flag**

In `internal/planner/physical/plan.go`, remove the flag field and its doc comment (around lines 191-196). In the `PlanDistributed` method (around line 999), remove the `if p.UseEnsureDistribution { ... }` guard and always run `EnsureDistribution`:

```go
	// EnsureDistribution is unconditional after Phase 2b. The legacy
	// flag-gated path has been deleted; the coordinator's lowerExchangeDAG
	// requires Exchange-annotated plans.
	stages, err = EnsureDistribution(stages, p.WorkerCount)
	if err != nil {
		return nil, fmt.Errorf("ensure distribution: %w", err)
	}
```

- [ ] **Step 7: Remove flag sets from tests**

In `internal/planner/physical/plan_tpch_test.go`, delete every `planner.UseEnsureDistribution = true` line. The calls become noise because the behavior is always on.

In `benchmarks/tpch/parity_test.go`, update the comment block at line 9-24 to reflect that the flag is gone.

- [ ] **Step 8: Verify build**

Run: `go build ./...`
Expected: build succeeds.

- [ ] **Step 9: Run the full test suite**

Run: `go test ./internal/... ./benchmarks/tpch/...`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add -u
git commit -m "refactor: delete UseEnsureDistribution flag and dispatcher-role pickers

Phase 2b makes the lowering unconditional. The flag gated the planner-side
insertion of Exchange stages; now that the coordinator requires them, the
flag has no meaningful off-state. PickShuffleCandidate and CanProbeSplit
are gone — their logic moved into EnsureDistribution (via Task 3).

PickAggregateShuffleCandidateDiag remains: the Q17 pre-compute inline
branch still consults it. That will move in Phase 3 when the aggregate-
shuffle rewrite becomes a logical pass.

Phase 2b spec: docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md"
```

---

## Task 11: Activate PARITY=1 row-equality harness

The harness exists as a stub at `benchmarks/tpch/parity_test.go` (commit `9399371`). Task 15 from the Phase 2a plan deferred row-equality to this point.

**Files:**
- Modify: `benchmarks/tpch/parity_test.go`

- [ ] **Step 1: Read the current harness**

Run: `cat benchmarks/tpch/parity_test.go` (use `Read` tool).

Find the stub that notes "Task 15 Note: the Planner.UseEnsureDistribution flag is not reachable through db". After Task 10 that caveat is obsolete (EnsureDistribution is always on). The harness now needs to run each TPC-H query, capture its output, compare row-by-row with a baseline.

- [ ] **Step 2: Implement row-equality for PARITY=1**

Replace the stub body with a real implementation. The parity gate: run each query via `wadjet.DB.Query`, collect rows into a canonical representation (sorted by the query's ORDER BY — or by all columns if the query has no ORDER BY), compare against a baseline recorded from a known-good run.

Minimum viable shape:

```go
// +build parity

package tpch

import (
	"os"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet"
)

// TestTPCHParity runs each TPC-H query at SF0.01 with EnsureDistribution
// active (the only path now) and compares row-sorted output against a
// baseline in testdata/parity/.
//
// Gate: env PARITY=1 required to run (otherwise skipped).
func TestTPCHParity(t *testing.T) {
	if os.Getenv("PARITY") != "1" {
		t.Skip("set PARITY=1 to run row-equality check")
	}
	db := openTPCHDB(t)         // existing helper
	queries := loadTPCHQueries(t) // existing helper: map["Q01"]->SQL

	for name, sql := range queries {
		t.Run(name, func(t *testing.T) {
			rows, err := db.Query(context.Background(), sql)
			if err != nil {
				t.Fatalf("%s: query: %v", name, err)
			}
			got := collectRows(t, rows)
			want := loadBaselineRows(t, name)
			if !equalRowsIgnoringOrder(got, want) {
				t.Errorf("%s: row mismatch\nwant=%v\ngot=%v", name, want, got)
			}
		})
	}
}

func collectRows(t *testing.T, rows *wadjet.Rows) [][]any {
	t.Helper()
	var out [][]any
	for rows.Next() {
		cols, err := rows.Values()
		if err != nil {
			t.Fatalf("Values: %v", err)
		}
		out = append(out, cols)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return out
}

func loadBaselineRows(t *testing.T, queryName string) [][]any {
	t.Helper()
	// Baselines live at benchmarks/tpch/testdata/parity/{queryName}.json.
	// Generated one-shot from main via UPDATE_PARITY_BASELINES=1.
	// Implementation detail: JSON-decode into [][]any.
	path := "testdata/parity/" + queryName + ".json"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("baseline %s: %v", path, err)
	}
	var rows [][]any
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatalf("unmarshal %s: %v", path, err)
	}
	return rows
}

func equalRowsIgnoringOrder(a, b [][]any) bool {
	if len(a) != len(b) {
		return false
	}
	ac := canonicalizeRows(a)
	bc := canonicalizeRows(b)
	for i := range ac {
		if !rowsEqual(ac[i], bc[i]) {
			return false
		}
	}
	return true
}

func canonicalizeRows(rows [][]any) [][]any {
	out := make([][]any, len(rows))
	copy(out, rows)
	sort.Slice(out, func(i, j int) bool {
		return cmpRow(out[i], out[j]) < 0
	})
	return out
}

// cmpRow compares two rows lexicographically using fmt.Sprintf-based
// string comparison. Sufficient for stable ordering in the PARITY gate
// (all values at SF0.01 are deterministic). Typed-kernel comparison is
// not required here.
func cmpRow(a, b []any) int { /* ... */ }

func rowsEqual(a, b []any) bool { /* ... */ }
```

The `// +build parity` tag gates the file; tests run only under `go test -tags parity`. If the repo convention uses env vars instead of build tags, switch to `os.Getenv("PARITY") == "1"` with no build tag (matches other gated tests in the repo — verify via `grep -rn "PARITY" benchmarks/`).

- [ ] **Step 3: Generate baseline from `main`**

```bash
git stash
git checkout main
mkdir -p benchmarks/tpch/testdata/parity
UPDATE_PARITY_BASELINES=1 PARITY=1 go test -v -run TestTPCHParity ./benchmarks/tpch/
git checkout feat/distribution-property-phase-2
git stash pop
```

Expected: 22 JSON files written to `benchmarks/tpch/testdata/parity/`. If `UPDATE_PARITY_BASELINES=1` is not recognized, implement the update-path inside `TestTPCHParity`: when env is set, write baselines instead of comparing.

- [ ] **Step 4: Run parity on the branch**

```bash
PARITY=1 go test -v -run TestTPCHParity ./benchmarks/tpch/
```

Expected: 22/22 PASS. Any mismatch means the Phase 2b lowering returned different rows — hard failure; do not merge.

- [ ] **Step 5: Commit the baselines and harness**

```bash
git add benchmarks/tpch/parity_test.go benchmarks/tpch/testdata/parity/
git commit -m "test(tpch): activate PARITY=1 row-equality harness for all 22 queries

Replaces the stub from commit 9399371 with real row-equality comparison
against a baseline captured from main. Gates Phase 2b correctness beyond
the SF0.01 result-count check.

Phase 2b spec: docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md"
```

---

## Task 12: SF1 local performance gate

**Files:** none (verification only)

- [ ] **Step 1: Run SF1 on `main` as baseline**

```bash
git stash
git checkout main
TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/ 2>&1 | tee /tmp/bench-sf1-main.txt
git checkout feat/distribution-property-phase-2
git stash pop
```

Expected: 22/22 PASS on main. Record per-query timings.

- [ ] **Step 2: Run SF1 on the Phase 2b branch**

```bash
TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/ 2>&1 | tee /tmp/bench-sf1-phase2b.txt
```

Expected: 22/22 PASS.

- [ ] **Step 3: Diff per-query timings**

Compare `/tmp/bench-sf1-main.txt` and `/tmp/bench-sf1-phase2b.txt` query-by-query. Gate: no query more than 2× slower on the branch.

Mechanical check:

```bash
grep -E "Q[0-9]+ .* [0-9]+\.[0-9]+s" /tmp/bench-sf1-main.txt | sort > /tmp/main-times.txt
grep -E "Q[0-9]+ .* [0-9]+\.[0-9]+s" /tmp/bench-sf1-phase2b.txt | sort > /tmp/branch-times.txt
paste /tmp/main-times.txt /tmp/branch-times.txt
```

Eyeball the output. Any query >2× slower: investigate before SF10.

- [ ] **Step 4: Commit only if there are relevant artifacts to keep**

The benchmark outputs are in `/tmp/`; do not commit them. If this task surfaces a regression fix, commit that separately with a clear `perf(coordinator)` scope.

---

## Task 13: SF10 EC2 A/B (T2 gate)

**Files:** none (deployment)

- [ ] **Step 1: Verify no AWS cluster is currently running**

```bash
AWS_PROFILE=citc aws ec2 describe-instances --filters "Name=tag:Name,Values=wadjet-bench-*" "Name=instance-state-name,Values=running,pending" --query 'Reservations[].Instances[].InstanceId' --output text
```

Expected: empty. If not empty: tear down before proceeding (see `memory/feedback_ec2_teardown_discipline.md`).

- [ ] **Step 2: Read the deploy preflight**

Review `memory/feedback_deploy_preflight.md` (canonical command, on-demand not spot, `--profile citc`, instance types locked, active health monitoring at T+60s / T+2min, destroy as part of run).

Review `memory/feedback_baseline_first.md`: deploy `main` as baseline FIRST; cost of skipping is ambiguous regression attribution.

- [ ] **Step 3: Get explicit user authorization to deploy**

Per `memory/feedback_no_auto_deploy.md`, never deploy without explicit user approval. Ask the user: "Phase 2b SF10 A/B is ready. This will deploy `main` as baseline then `feat/distribution-property-phase-2` as branch, ~$1.20 total, ~2h wall time. Approve?"

- [ ] **Step 4: Deploy `main` baseline**

Follow the canonical command from `feedback_deploy_preflight.md`. Do not paraphrase it here — the deploy script is the source of truth. Cross-compile locally, upload to `s3://<bucket>/bin/latest/` (per `feedback_s3_binary_path.md`), build `cmd/tpch-bench` (41MB self-contained, per `feedback_deploy_binary.md`), deploy with `-var=use_spot=false`.

Monitor health at T+60s and T+2min (`feedback_deploy_monitoring.md`). Capture the result JSON. Destroy the cluster as part of the run.

- [ ] **Step 5: Deploy the Phase 2b branch**

Same procedure on `feat/distribution-property-phase-2`.

- [ ] **Step 6: Compare per-query SF10 timings**

Pull the two result JSON files. Gate: no query more than 10% slower on the branch (T2 threshold per spec).

- [ ] **Step 7: Record outcome**

If all gates pass: Phase 2b is mergeable. Update PR #45 description from "Phase 2a only" to reflect that Phase 2b has also landed (or open a new PR for Phase 2b separately if you want reviewer-friendly scoping — the spec didn't mandate this).

If a regression surfaces: do not merge. File a memory note with the observed regression, the suspected cause, and next debugging step. Do not ship broken main.

---

## Self-review notes

Risks to watch during execution:

1. **ShuffleCandidate.BuildKeys vs ProbeKeys.** Task 5 synthesizes both from `Exchange.Keys`. `PickShuffleCandidate` today separates them (`cand.BuildKeys` = join's right keys, `cand.ProbeKeys` = join's left keys). If any query uses asymmetric keys, Task 5's shortcut is wrong. If SF0.01 fails in Task 9, this is the first place to look — populate separate BuildKeys/ProbeKeys on `ExchangeStage` in Task 3 and thread through.

2. **`orchestrateReplicate` signature drift.** The spec assumed it accepts `(ctx, stage, queryID, sql, allStages, probeAlias)` and returns `(map[string][]string, error)`. The actual signature at `internal/coordinator/build_cache.go:346` may differ — verify in Task 6 Step 3 and adapt.

3. **`logical.Node` literal in Task 7 test.** The concrete shape may not match how logical plans are constructed in the codebase. Consult `internal/planner/logical/plan.go` and use a real constructor.

4. **Golden regeneration.** Task 2's golden rewrite may surface unrelated snapshot noise (field ordering). If so, stabilize the snapshot encoder before committing.

5. **PreComputedAggregates suppression.** Task 5's Repartition branch short-circuits when `len(preComputedAggregates) > 0`. Today's switch does the same at `coordinator.go:541-543`. Verify the short-circuit reaches the Replicate path — if the Q17 query has no StageExchangeReplicate in its plan, the short-circuit produces a `default` fallback instead of the probe-split shape. If that happens at SF0.01, `EnsureDistribution` needs to insert a Replicate for Q17 shapes — investigate before assuming it's a lowering bug.
