# Distribution-Property Pass — Phase 1 Implementation Plan


**Goal:** Wire up the existing 58-line `internal/planner/physical/distribution.go` scaffolding into a populated, queried planner property — `Distribution.Satisfies(RequiredDistribution)` — and assert exchange consistency for every stage emitted by `PlanDistributed`. Phase 1 is purely additive and behavior-preserving; the heuristic switch in `coordinator.go:541-605` continues to make every routing decision.

**Architecture:** Extend `internal/planner/physical/distribution.go` with `RequiredKind` enum, `RequiredDistribution` struct, `Distribution.Satisfies` predicate, `RequiredChildDistribution`, `OutputDistribution`, `assignStageDistributions`, `AssertExchangeConsistency`, and a `BehaviorPreservingMode` package var. Wire the assignment + assertion into `PlanDistributed` after `enforceQueryLimits`. Add `TestTPCHDistributionConsistency` as the acceptance gate that runs every Q1-Q22 through the new pass with hard assertions. Preserve `SatisfiesJoinKeys` as a thin wrapper over `Satisfies` for existing callers. No coordinator, worker, or executor changes.

**Tech Stack:** Go 1.22+, existing `internal/planner/physical` package (Stage / Distribution / DistKind), existing TPC-H test harness in `plan_tpch_test.go` (`tpchPlanQueryMap`, `setupTPCHCatalog`, `sqlToStages`), `testing` package with table-driven tests, `benchmarks/tpch/` `TestTPCHQueries` for SF0.01 correctness verification.

**Spec:** `docs/archive/specs/2026-04-20-distribution-property-phase-1.md`

**Worktree:** `/home/dwright/Projects/caelum/.worktrees/distribution-phase-1` on branch `feat/distribution-property-phase-1`, parent commit `0b78c4e`.

---

## File Structure

| File | Change |
|---|---|
| `internal/planner/physical/distribution.go` | **Extend.** Add `RequiredKind` enum, `RequiredDistribution` struct, `Satisfies`, `RequiredChildDistribution`, `OutputDistribution`, `assignStageDistributions`, `AssertExchangeConsistency`, `BehaviorPreservingMode`. Refactor `SatisfiesJoinKeys` to delegate to `Satisfies`. |
| `internal/planner/physical/distribution_test.go` | **Extend.** Add `TestDistributionSatisfies` table-driven test, `TestRequiredChildDistribution` per-type tests, `TestOutputDistribution` per-type tests, `TestAssignStageDistributions`, `TestAssertExchangeConsistency`. |
| `internal/planner/physical/plan.go` | **Three-line wire-up.** Inside `PlanDistributed`, after `enforceQueryLimits`, call `assignStageDistributions(stages, p.WorkerCount)` and `AssertExchangeConsistency(stages)`. |
| `internal/planner/physical/plan_tpch_test.go` | **Add `TestTPCHDistributionConsistency`** — runs every Q1-Q22 through `PlanDistributed` with `WorkerCount=4` and `BehaviorPreservingMode=false`, asserting every stage's `Distribution` is populated and `AssertExchangeConsistency(stages) == nil`. |
| `internal/coordinator/`, `internal/worker/`, executor packages | **No changes.** |

Total churn: ~400 added lines, ~3 modified lines. No file deletions.

---

## Task 1: Add `RequiredKind` enum and `RequiredDistribution` struct

**Files:**
- Modify: `internal/planner/physical/distribution.go`

- [ ] **Step 1: Append the `RequiredKind` enum, `RequiredDistribution` struct, and a `String()` method on `RequiredKind` to the end of `distribution.go`**

```go
// RequiredKind enumerates the partitioning a consumer needs from each input.
// Mirrors Spark's Distribution trait subclasses; see the Phase 1 spec
// (docs/archive/specs/2026-04-20-distribution-property-phase-1.md) §"The
// property algebra" for the satisfaction truth table.
type RequiredKind int

const (
	RequiredAny               RequiredKind = iota // no constraint
	RequiredSingleton                             // exactly one partition (final result, coordinator merge)
	RequiredBroadcast                             // every worker has every row
	RequiredClusteredOn                           // co-partitioned on Keys, any partition count
	RequiredHashPartitionedOn                     // hash-partitioned on Keys with exactly Count partitions
)

// String renders a RequiredKind for log lines and assertion error messages.
// Stable text identifiers — do not change without checking telemetry consumers.
func (r RequiredKind) String() string {
	switch r {
	case RequiredAny:
		return "any"
	case RequiredSingleton:
		return "singleton"
	case RequiredBroadcast:
		return "broadcast"
	case RequiredClusteredOn:
		return "clustered_on"
	case RequiredHashPartitionedOn:
		return "hash_partitioned_on"
	default:
		return "unknown"
	}
}

// RequiredDistribution describes what a consumer stage requires of each input.
// Derived from existing stage fields (JoinLeftKeys, JoinRightKeys, GroupByCols,
// ShuffleKeys) by RequiredChildDistribution; never stored on Stage.
type RequiredDistribution struct {
	Kind  RequiredKind
	Keys  []string
	Count int
}
```

- [ ] **Step 2: Verify the file compiles**

Run: `go build ./internal/planner/physical/`
Expected: success (no output, exit 0)

- [ ] **Step 3: Commit**

```bash
git add internal/planner/physical/distribution.go
git commit -m "$(cat <<'EOF'
feat(planner): add RequiredKind enum and RequiredDistribution struct

Phase 1 of the distribution-property pass. Adds the consumer-side type that
mirrors Spark's Distribution trait subclasses. Pure type additions; no
behavior change. Truth table for the predicate is documented in the spec.

Spec: docs/archive/specs/2026-04-20-distribution-property-phase-1.md
EOF
)"
```

---

## Task 2: Add `Distribution.Satisfies(req RequiredDistribution) bool` with full truth table

**Files:**
- Modify: `internal/planner/physical/distribution_test.go`
- Modify: `internal/planner/physical/distribution.go`

- [ ] **Step 1: Append the failing table-driven test to `distribution_test.go`**

```go
func TestDistributionSatisfies(t *testing.T) {
	hashOrderkey := Distribution{Kind: DistHashPartitioned, Keys: []string{"orderkey"}, Count: 12}
	hashCustkey := Distribution{Kind: DistHashPartitioned, Keys: []string{"custkey"}, Count: 12}
	hashOrderkey24 := Distribution{Kind: DistHashPartitioned, Keys: []string{"orderkey"}, Count: 24}
	singleton := Distribution{Kind: DistSingleton}
	bcast := Distribution{Kind: DistBroadcast}

	reqAny := RequiredDistribution{Kind: RequiredAny}
	reqSingleton := RequiredDistribution{Kind: RequiredSingleton}
	reqBcast := RequiredDistribution{Kind: RequiredBroadcast}
	reqClusterOrderkey := RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"orderkey"}}
	reqClusterCustkey := RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"custkey"}}
	reqHashOrderkey12 := RequiredDistribution{Kind: RequiredHashPartitionedOn, Keys: []string{"orderkey"}, Count: 12}
	reqHashOrderkey24 := RequiredDistribution{Kind: RequiredHashPartitionedOn, Keys: []string{"orderkey"}, Count: 24}

	tests := []struct {
		name string
		dist Distribution
		req  RequiredDistribution
		want bool
	}{
		// RequiredAny is satisfied by everything
		{"singleton sat any", singleton, reqAny, true},
		{"broadcast sat any", bcast, reqAny, true},
		{"hash sat any", hashOrderkey, reqAny, true},

		// RequiredSingleton: only DistSingleton
		{"singleton sat singleton", singleton, reqSingleton, true},
		{"broadcast not sat singleton", bcast, reqSingleton, false},
		{"hash not sat singleton", hashOrderkey, reqSingleton, false},

		// RequiredBroadcast: only DistBroadcast
		{"broadcast sat broadcast", bcast, reqBcast, true},
		{"singleton not sat broadcast", singleton, reqBcast, false},
		{"hash not sat broadcast", hashOrderkey, reqBcast, false},

		// RequiredClusteredOn(K): broadcast yes, singleton yes, hash iff Keys==K
		{"broadcast sat clustered", bcast, reqClusterOrderkey, true},
		{"singleton sat clustered", singleton, reqClusterOrderkey, true},
		{"hash on K sat clustered K", hashOrderkey, reqClusterOrderkey, true},
		{"hash on K not sat clustered K2", hashOrderkey, reqClusterCustkey, false},
		{"hash on K2 not sat clustered K", hashCustkey, reqClusterOrderkey, false},

		// RequiredHashPartitionedOn(K, N): only hash with same K and N
		{"hash K N sat hash K N", hashOrderkey, reqHashOrderkey12, true},
		{"hash K N' not sat hash K N", hashOrderkey24, reqHashOrderkey12, false},
		{"hash K N sat hash K N'", hashOrderkey, reqHashOrderkey24, false},
		{"hash K not sat hash K2 N", hashCustkey, reqHashOrderkey12, false},
		{"singleton not sat hash K N", singleton, reqHashOrderkey12, false},
		{"broadcast not sat hash K N", bcast, reqHashOrderkey12, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.dist.Satisfies(tt.req)
			if got != tt.want {
				t.Errorf("(%v).Satisfies(%v) = %v, want %v", tt.dist, tt.req, got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails (Satisfies not yet defined)**

Run: `go test -run TestDistributionSatisfies ./internal/planner/physical/`
Expected: build failure with `tt.dist.Satisfies undefined (type Distribution has no field or method Satisfies)`

- [ ] **Step 3: Add the `Satisfies` method to `distribution.go`**

Append after the `RequiredDistribution` struct added in Task 1:

```go
// Satisfies reports whether this distribution meets a consumer's required
// distribution. Single mechanical predicate that mirrors Spark's
// Partitioning.satisfies(Distribution). The truth table is documented in
// the Phase 1 spec §"The property algebra".
//
//   RequiredAny:                    always true.
//   RequiredSingleton:              only DistSingleton.
//   RequiredBroadcast:              only DistBroadcast.
//   RequiredClusteredOn(K):         DistBroadcast yes; DistSingleton yes;
//                                   DistHashPartitioned iff Keys==K.
//   RequiredHashPartitionedOn(K, N): only DistHashPartitioned with Keys==K
//                                   and Count==N.
func (d Distribution) Satisfies(req RequiredDistribution) bool {
	switch req.Kind {
	case RequiredAny:
		return true
	case RequiredSingleton:
		return d.Kind == DistSingleton
	case RequiredBroadcast:
		return d.Kind == DistBroadcast
	case RequiredClusteredOn:
		switch d.Kind {
		case DistBroadcast, DistSingleton:
			return true
		case DistHashPartitioned:
			return keysEqual(d.Keys, req.Keys)
		default:
			return false
		}
	case RequiredHashPartitionedOn:
		if d.Kind != DistHashPartitioned {
			return false
		}
		if d.Count != req.Count {
			return false
		}
		return keysEqual(d.Keys, req.Keys)
	default:
		return false
	}
}

// keysEqual reports whether two ordered key slices are identical.
func keysEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -run TestDistributionSatisfies ./internal/planner/physical/`
Expected: `PASS` with all 19 subtests passing, `ok  github.com/derekmwright/wadjet/internal/planner/physical`

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/distribution.go internal/planner/physical/distribution_test.go
git commit -m "$(cat <<'EOF'
feat(planner): add Distribution.Satisfies predicate

The single mechanical predicate that drives the property algebra.
Implements all five RequiredKind cases per the Phase 1 spec truth table.
Table-driven test covers the full DistKind × RequiredKind matrix (19 cases).

Spec: docs/archive/specs/2026-04-20-distribution-property-phase-1.md
EOF
)"
```

---

## Task 3: Refactor `SatisfiesJoinKeys` to delegate to `Satisfies`

**Files:**
- Modify: `internal/planner/physical/distribution.go`

- [ ] **Step 1: Verify the existing `TestDistributionSatisfiesJoin` test still exists and passes (baseline)**

Run: `go test -run TestDistributionSatisfiesJoin ./internal/planner/physical/`
Expected: `PASS`, `ok  github.com/derekmwright/wadjet/internal/planner/physical`

- [ ] **Step 2: Replace the body of `SatisfiesJoinKeys` to delegate to `Satisfies`**

Replace the existing function (currently lines ~38-58 of `distribution.go`):

```go
// SatisfiesJoinKeys reports whether this distribution allows a co-located
// join on the given keys without re-shuffling. Preserved as a thin wrapper
// over Satisfies for existing callers.
func (d Distribution) SatisfiesJoinKeys(joinKeys []string) bool {
	return d.Satisfies(RequiredDistribution{Kind: RequiredClusteredOn, Keys: joinKeys})
}
```

- [ ] **Step 3: Run the existing test to verify behavior is preserved**

Run: `go test -run TestDistributionSatisfiesJoin ./internal/planner/physical/`
Expected: `PASS` — same outcome as before refactor

- [ ] **Step 4: Run full distribution test file to verify nothing else broke**

Run: `go test -run 'TestDistribution' ./internal/planner/physical/`
Expected: `PASS` for `TestDistributionEquals`, `TestDistributionSatisfiesJoin`, `TestDistributionSatisfies`

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/distribution.go
git commit -m "$(cat <<'EOF'
refactor(planner): SatisfiesJoinKeys delegates to Satisfies

Preserve the existing API for callers (no signature change) while
collapsing its semantics into the new mechanical predicate. The wrapper
is the RequiredClusteredOn projection of Satisfies.
EOF
)"
```

---

## Task 4: Add `RequiredChildDistribution(stage Stage, slot int)` returning per-slot requirements

**Files:**
- Modify: `internal/planner/physical/distribution_test.go`
- Modify: `internal/planner/physical/distribution.go`

- [ ] **Step 1: Append the failing test to `distribution_test.go`**

```go
func TestRequiredChildDistribution(t *testing.T) {
	tests := []struct {
		name  string
		stage Stage
		slot  int
		want  RequiredDistribution
	}{
		{
			name:  "scan has no inputs returns any",
			stage: Stage{ID: "scan-0", Type: "scan"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "dual has no inputs returns any",
			stage: Stage{ID: "dual-0", Type: "dual"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "shuffle accepts any input",
			stage: Stage{ID: "shuffle-0", Type: "shuffle", ShuffleKeys: []string{"k"}, NumPartitions: 16},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name: "hash_join probe slot requires clustered on left keys",
			stage: Stage{
				ID: "join-0", Type: "hash_join",
				JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
				LeftDepStage: "shuffle-l", RightDepStage: "shuffle-r",
				Dependencies: []string{"shuffle-l", "shuffle-r"},
			},
			slot: 0,
			want: RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"l_orderkey"}},
		},
		{
			name: "hash_join build slot requires clustered on right keys",
			stage: Stage{
				ID: "join-0", Type: "hash_join",
				JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
				LeftDepStage: "shuffle-l", RightDepStage: "shuffle-r",
				Dependencies: []string{"shuffle-l", "shuffle-r"},
			},
			slot: 1,
			want: RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"o_orderkey"}},
		},
		{
			name: "broadcast_join probe slot requires any (Phase 1)",
			stage: Stage{
				ID: "join-0", Type: "broadcast_join",
				JoinLeftKeys: []string{"l_partkey"}, JoinRightKeys: []string{"p_partkey"},
				LeftDepStage: "scan-l", RightDepStage: "scan-r",
				Dependencies: []string{"scan-l", "scan-r"},
			},
			slot: 0,
			want: RequiredDistribution{Kind: RequiredAny},
		},
		{
			name: "broadcast_join build slot requires any (Phase 1)",
			stage: Stage{
				ID: "join-0", Type: "broadcast_join",
				JoinLeftKeys: []string{"l_partkey"}, JoinRightKeys: []string{"p_partkey"},
				LeftDepStage: "scan-l", RightDepStage: "scan-r",
				Dependencies: []string{"scan-l", "scan-r"},
			},
			slot: 1,
			want: RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "aggregate requires any (Phase 1 conservative — see Risk #1)",
			stage: Stage{ID: "aggregate-0", Type: "aggregate", GroupByCols: []string{"l_returnflag"}},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "final_aggregate requires any",
			stage: Stage{ID: "final_aggregate-0", Type: "final_aggregate", GroupByCols: []string{"l_returnflag"}},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "sort requires any",
			stage: Stage{ID: "sort-0", Type: "sort"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "merge_sort requires any",
			stage: Stage{ID: "merge_sort-0", Type: "merge_sort"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name: "window with PartitionBy requires clustered on partition keys",
			stage: Stage{
				ID: "window-0", Type: "window",
				WindowCols: []WindowColSpec{{Func: "row_number", PartitionBy: []string{"o_custkey"}}},
			},
			slot: 0,
			want: RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"o_custkey"}},
		},
		{
			name: "window without PartitionBy requires any",
			stage: Stage{
				ID: "window-0", Type: "window",
				WindowCols: []WindowColSpec{{Func: "row_number"}},
			},
			slot: 0,
			want: RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "pipeline requires any",
			stage: Stage{ID: "pipeline-0", Type: "pipeline"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "table_func requires any",
			stage: Stage{ID: "tf-0", Type: "table_func"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
		{
			name:  "unknown stage type requires any",
			stage: Stage{ID: "?-0", Type: "mystery"},
			slot:  0,
			want:  RequiredDistribution{Kind: RequiredAny},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RequiredChildDistribution(tt.stage, tt.slot)
			if got.Kind != tt.want.Kind {
				t.Fatalf("Kind = %v, want %v", got.Kind, tt.want.Kind)
			}
			if !keysEqual(got.Keys, tt.want.Keys) {
				t.Fatalf("Keys = %v, want %v", got.Keys, tt.want.Keys)
			}
			if got.Count != tt.want.Count {
				t.Fatalf("Count = %v, want %v", got.Count, tt.want.Count)
			}
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails (function not yet defined)**

Run: `go test -run TestRequiredChildDistribution ./internal/planner/physical/`
Expected: build failure with `undefined: RequiredChildDistribution`

- [ ] **Step 3: Add `RequiredChildDistribution` to `distribution.go`**

Append after the `Satisfies` method:

```go
// RequiredChildDistribution returns the per-slot required distribution for a
// stage's input. `slot` indexes into the stage's logical input list:
//   - For joins: slot 0 is the probe (LeftDepStage), slot 1 is the build (RightDepStage).
//   - For unary stages: slot 0 is the sole input.
//   - For stages with no inputs (scan, dual): RequiredAny is returned for any slot.
//
// Never stored on Stage; recomputed by AssertExchangeConsistency. Rules are
// derived from how walkStages already implicitly constructs the plan; see
// the Phase 1 spec §"RequiredChildDistribution" for the per-stage table.
//
// Unknown stage types return RequiredAny (no constraint asserted). New stage
// types added to the planner must add their rule here or accept the no-op
// default.
func RequiredChildDistribution(stage Stage, slot int) RequiredDistribution {
	switch stage.Type {
	case "scan", "dual":
		// No inputs — any slot is RequiredAny by definition.
		return RequiredDistribution{Kind: RequiredAny}
	case "shuffle":
		// Shuffle accepts any input and re-partitions.
		return RequiredDistribution{Kind: RequiredAny}
	case "hash_join":
		switch slot {
		case 0:
			return RequiredDistribution{Kind: RequiredClusteredOn, Keys: stage.JoinLeftKeys}
		case 1:
			return RequiredDistribution{Kind: RequiredClusteredOn, Keys: stage.JoinRightKeys}
		default:
			return RequiredDistribution{Kind: RequiredAny}
		}
	case "broadcast_join":
		// Phase 1 leaves both slots at RequiredAny — the executor handles
		// broadcast in-process today (no explicit broadcast Exchange stage).
		// Phase 2 inserts Exchange{Type: Replicate} between scan and the
		// build slot, at which point the build requirement strengthens to
		// RequiredBroadcast. See spec Risk #4.
		return RequiredDistribution{Kind: RequiredAny}
	case "aggregate":
		// Phase 1 conservative: today's two-phase distributed aggregate
		// runs the partial stage on RequiredAny inputs (the partial does
		// not require pre-clustering — it produces partials that the final
		// stage merges). See spec Risk #1.
		return RequiredDistribution{Kind: RequiredAny}
	case "final_aggregate", "merge_aggregate":
		return RequiredDistribution{Kind: RequiredAny}
	case "sort", "merge_sort":
		return RequiredDistribution{Kind: RequiredAny}
	case "window":
		// If any window column declares a PartitionBy, the input must be
		// clustered on those keys. Take the first PartitionBy as the
		// requirement (today's planner emits a single window stage per
		// partition spec; multiple PartitionBy clauses become separate
		// window stages).
		for _, wc := range stage.WindowCols {
			if len(wc.PartitionBy) > 0 {
				return RequiredDistribution{Kind: RequiredClusteredOn, Keys: wc.PartitionBy}
			}
		}
		return RequiredDistribution{Kind: RequiredAny}
	case "pipeline", "table_func":
		return RequiredDistribution{Kind: RequiredAny}
	default:
		return RequiredDistribution{Kind: RequiredAny}
	}
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -run TestRequiredChildDistribution ./internal/planner/physical/`
Expected: `PASS` with all 16 subtests passing

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/distribution.go internal/planner/physical/distribution_test.go
git commit -m "$(cat <<'EOF'
feat(planner): add RequiredChildDistribution per stage type

Derives a consumer's per-slot required distribution from existing Stage
fields (JoinLeftKeys, JoinRightKeys, WindowCols.PartitionBy). Pure
function; never stored. Per-stage rules track how walkStages already
implicitly constructs the plan. Phase 1 conservative defaults for
broadcast_join build slot (RequiredAny) and aggregate input (RequiredAny)
documented inline; spec Risks #1 and #4 explain Phase 2 evolution.
EOF
)"
```

---

## Task 5: Add `OutputDistribution(stage Stage, deps map[string]Distribution)` returning per-stage outputs

**Files:**
- Modify: `internal/planner/physical/distribution_test.go`
- Modify: `internal/planner/physical/distribution.go`

- [ ] **Step 1: Append the failing test to `distribution_test.go`**

```go
func TestOutputDistribution(t *testing.T) {
	tests := []struct {
		name  string
		stage Stage
		deps  map[string]Distribution
		want  Distribution
	}{
		{
			name:  "scan emits singleton",
			stage: Stage{ID: "scan-0", Type: "scan", ScanAlias: "lineitem"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name:  "dual emits singleton",
			stage: Stage{ID: "dual-0", Type: "dual"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name: "shuffle emits hash partitioned",
			stage: Stage{
				ID: "shuffle-0", Type: "shuffle",
				ShuffleKeys: []string{"l_orderkey"}, NumPartitions: 16,
			},
			deps: nil,
			want: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
		{
			name: "hash_join inherits probe distribution",
			stage: Stage{
				ID: "join-0", Type: "hash_join",
				LeftDepStage: "shuffle-l", RightDepStage: "shuffle-r",
				Dependencies: []string{"shuffle-l", "shuffle-r"},
			},
			deps: map[string]Distribution{
				"shuffle-l": {Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
				"shuffle-r": {Kind: DistHashPartitioned, Keys: []string{"o_orderkey"}, Count: 16},
			},
			want: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
		{
			name: "broadcast_join inherits probe distribution",
			stage: Stage{
				ID: "join-0", Type: "broadcast_join",
				LeftDepStage: "scan-l", RightDepStage: "scan-r",
				Dependencies: []string{"scan-l", "scan-r"},
			},
			deps: map[string]Distribution{
				"scan-l": {Kind: DistSingleton},
				"scan-r": {Kind: DistSingleton},
			},
			want: Distribution{Kind: DistSingleton},
		},
		{
			name:  "aggregate emits singleton",
			stage: Stage{ID: "aggregate-0", Type: "aggregate", GroupByCols: []string{"l_returnflag"}},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name: "final_aggregate lone reducer emits singleton",
			stage: Stage{
				ID: "final_aggregate-0", Type: "final_aggregate",
				GroupByCols: []string{"l_returnflag"},
			},
			deps: nil,
			want: Distribution{Kind: DistSingleton},
		},
		{
			name: "final_aggregate merge group emits hash partitioned",
			stage: Stage{
				ID: "merge_aggregate-0-0", Type: "final_aggregate",
				GroupByCols:     []string{"l_returnflag"},
				MergeGroup:      0,
				MergeGroupCount: 4,
			},
			deps: nil,
			want: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_returnflag"}, Count: 4},
		},
		{
			name:  "sort emits singleton",
			stage: Stage{ID: "sort-0", Type: "sort"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name:  "merge_sort lone emits singleton",
			stage: Stage{ID: "merge_sort-0", Type: "merge_sort"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name: "merge_sort merge group emits hash partitioned",
			stage: Stage{
				ID: "merge_sort-0-0", Type: "merge_sort",
				SortKeys:        []SortKeySpec{{Column: "l_orderkey"}},
				MergeGroup:      0,
				MergeGroupCount: 4,
			},
			deps: nil,
			want: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 4},
		},
		{
			name:  "window emits singleton",
			stage: Stage{ID: "window-0", Type: "window"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name:  "pipeline emits singleton",
			stage: Stage{ID: "pipeline-0", Type: "pipeline"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name:  "table_func emits singleton",
			stage: Stage{ID: "tf-0", Type: "table_func"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
		{
			name:  "unknown stage type emits singleton",
			stage: Stage{ID: "?-0", Type: "mystery"},
			deps:  nil,
			want:  Distribution{Kind: DistSingleton},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OutputDistribution(tt.stage, tt.deps)
			if !got.Equals(tt.want) {
				t.Fatalf("OutputDistribution = %+v, want %+v", got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -run TestOutputDistribution ./internal/planner/physical/`
Expected: build failure with `undefined: OutputDistribution`

- [ ] **Step 3: Add `OutputDistribution` to `distribution.go`**

Append after `RequiredChildDistribution`:

```go
// OutputDistribution computes the partitioning a stage's output has, given
// the resolved distributions of its dependencies. Pure function over
// stage fields + dep map. Rules track how today's planner emits stages;
// see the Phase 1 spec §"OutputDistribution" for the per-stage table.
//
// Phase 1 deliberately labels probe-split scans (Tasks > 1) as DistSingleton
// because the per-worker file-list is opaque to the property algebra.
// Phase 2 adds a richer label (e.g. DistRoundRobin) when the executor wires
// scan partitioning into the property graph. See spec Risk #2.
func OutputDistribution(stage Stage, deps map[string]Distribution) Distribution {
	switch stage.Type {
	case "scan":
		return Distribution{Kind: DistSingleton}
	case "dual":
		return Distribution{Kind: DistSingleton}
	case "shuffle":
		return Distribution{
			Kind:  DistHashPartitioned,
			Keys:  stage.ShuffleKeys,
			Count: stage.NumPartitions,
		}
	case "hash_join", "broadcast_join":
		// The join inherits the probe (left) input's distribution — the
		// join itself does not re-partition the joined output, it just
		// pairs probe rows with matching build rows.
		if probe, ok := deps[stage.LeftDepStage]; ok {
			return probe
		}
		return Distribution{Kind: DistSingleton}
	case "aggregate":
		return Distribution{Kind: DistSingleton}
	case "final_aggregate", "merge_aggregate":
		// Per spec §"OutputDistribution": merge-grouped finals are labeled
		// hash-partitioned on group-by cols with Count=MergeGroupCount for
		// symmetry with Trino/Spark. Lone reducers (MergeGroupCount == 0)
		// are singleton. See spec Risk #1.
		if stage.MergeGroupCount > 0 {
			return Distribution{
				Kind:  DistHashPartitioned,
				Keys:  stage.GroupByCols,
				Count: stage.MergeGroupCount,
			}
		}
		return Distribution{Kind: DistSingleton}
	case "sort":
		return Distribution{Kind: DistSingleton}
	case "merge_sort":
		// Merge-grouped intermediate merges are labeled hash-partitioned on
		// the sort keys (column names) for symmetry. Final merge of
		// intermediates (MergeGroupCount == 0) is singleton.
		if stage.MergeGroupCount > 0 {
			keys := make([]string, len(stage.SortKeys))
			for i, sk := range stage.SortKeys {
				keys[i] = sk.Column
			}
			return Distribution{
				Kind:  DistHashPartitioned,
				Keys:  keys,
				Count: stage.MergeGroupCount,
			}
		}
		return Distribution{Kind: DistSingleton}
	case "window", "pipeline", "table_func":
		return Distribution{Kind: DistSingleton}
	default:
		return Distribution{Kind: DistSingleton}
	}
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -run TestOutputDistribution ./internal/planner/physical/`
Expected: `PASS` with all 15 subtests passing

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/distribution.go internal/planner/physical/distribution_test.go
git commit -m "$(cat <<'EOF'
feat(planner): add OutputDistribution per stage type

Computes per-stage output partitioning from stage fields and resolved
dependency distributions. Joins inherit the probe input distribution
(no re-partition by the join itself). Shuffle stages emit hash-partitioned
on ShuffleKeys × NumPartitions. Merge-grouped finals/sorts are labeled
hash-partitioned (symmetry with Trino/Spark; spec Risk #1).

Phase 1 conservatively labels probe-split scans as DistSingleton; spec
Risk #2 explains Phase 2 evolution.
EOF
)"
```

---

## Task 6: Add `assignStageDistributions` to populate `Stage.Distribution` for every stage

**Files:**
- Modify: `internal/planner/physical/distribution_test.go`
- Modify: `internal/planner/physical/distribution.go`

- [ ] **Step 1: Append the failing test to `distribution_test.go`**

```go
func TestAssignStageDistributions(t *testing.T) {
	// Synthetic 3-stage plan: scan -> shuffle -> join (with another scan + shuffle as build)
	stages := []Stage{
		{ID: "scan-0", Type: "scan", ScanAlias: "lineitem"},
		{ID: "scan-1", Type: "scan", ScanAlias: "orders"},
		{
			ID: "shuffle-2", Type: "shuffle",
			ShuffleKeys: []string{"l_orderkey"}, NumPartitions: 16,
			Dependencies: []string{"scan-0"},
		},
		{
			ID: "shuffle-3", Type: "shuffle",
			ShuffleKeys: []string{"o_orderkey"}, NumPartitions: 16,
			Dependencies: []string{"scan-1"},
		},
		{
			ID: "join-4", Type: "hash_join",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "shuffle-2", RightDepStage: "shuffle-3",
			Dependencies: []string{"shuffle-2", "shuffle-3"},
		},
	}

	assignStageDistributions(stages, 4)

	want := map[string]Distribution{
		"scan-0":    {Kind: DistSingleton},
		"scan-1":    {Kind: DistSingleton},
		"shuffle-2": {Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		"shuffle-3": {Kind: DistHashPartitioned, Keys: []string{"o_orderkey"}, Count: 16},
		// Hash join inherits probe-side distribution (shuffle-2)
		"join-4": {Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
	}
	for _, s := range stages {
		got := s.Distribution
		w := want[s.ID]
		if !got.Equals(w) {
			t.Errorf("stage %s: Distribution = %+v, want %+v", s.ID, got, w)
		}
	}
}

func TestAssignStageDistributions_OutOfOrderInput(t *testing.T) {
	// Stages provided out of topological order. The pass must still resolve
	// dependencies (e.g. join-4 declared before its shuffle deps).
	stages := []Stage{
		{
			ID: "join-4", Type: "hash_join",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "shuffle-2", RightDepStage: "shuffle-3",
			Dependencies: []string{"shuffle-2", "shuffle-3"},
		},
		{
			ID: "shuffle-2", Type: "shuffle",
			ShuffleKeys: []string{"l_orderkey"}, NumPartitions: 16,
			Dependencies: []string{"scan-0"},
		},
		{
			ID: "shuffle-3", Type: "shuffle",
			ShuffleKeys: []string{"o_orderkey"}, NumPartitions: 16,
			Dependencies: []string{"scan-1"},
		},
		{ID: "scan-0", Type: "scan", ScanAlias: "lineitem"},
		{ID: "scan-1", Type: "scan", ScanAlias: "orders"},
	}

	assignStageDistributions(stages, 4)

	for _, s := range stages {
		if s.ID == "join-4" {
			want := Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16}
			if !s.Distribution.Equals(want) {
				t.Errorf("join-4 Distribution = %+v, want %+v (dep resolution should not depend on input order)", s.Distribution, want)
			}
		}
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -run TestAssignStageDistributions ./internal/planner/physical/`
Expected: build failure with `undefined: assignStageDistributions`

- [ ] **Step 3: Add `assignStageDistributions` to `distribution.go`**

Append after `OutputDistribution`:

```go
// assignStageDistributions walks the stages slice in dependency order and
// populates Stage.Distribution per the OutputDistribution rules. The walk
// is by-ID so stages provided out of topological order are still resolved
// correctly (matters because fuseJoinStages rewires deps after walkStages).
//
// workerCount is reserved for future rules (e.g. probe-split scan
// distribution) — Phase 1 ignores it but threads it through to keep the
// signature stable for Phase 2.
func assignStageDistributions(stages []Stage, workerCount int) {
	_ = workerCount // reserved for Phase 2 probe-split distribution rules

	// Build an ID → index lookup so we can mutate stages in place.
	idx := make(map[string]int, len(stages))
	for i, s := range stages {
		idx[s.ID] = i
	}

	// Track resolved distributions by stage ID. A stage is resolvable once
	// all its dependencies have been resolved.
	resolved := make(map[string]Distribution, len(stages))

	// Iterate until every stage has a resolved distribution. The loop runs
	// at most len(stages) times because each pass resolves at least one
	// stage (the dependency graph is a DAG validated by walkStages).
	for pass := 0; pass < len(stages) && len(resolved) < len(stages); pass++ {
		for i := range stages {
			s := &stages[i]
			if _, done := resolved[s.ID]; done {
				continue
			}
			// Check that every dependency has been resolved.
			ready := true
			for _, dep := range s.Dependencies {
				if _, ok := resolved[dep]; !ok {
					ready = false
					break
				}
			}
			if !ready {
				continue
			}
			// Build the per-dep distribution map for OutputDistribution.
			depMap := make(map[string]Distribution, len(s.Dependencies))
			for _, dep := range s.Dependencies {
				depMap[dep] = resolved[dep]
			}
			d := OutputDistribution(*s, depMap)
			s.Distribution = d
			resolved[s.ID] = d
			_ = idx // idx kept for Phase 2 stages that need cross-references
		}
	}
}
```

- [ ] **Step 4: Run the tests to verify both pass**

Run: `go test -run TestAssignStageDistributions ./internal/planner/physical/`
Expected: `PASS` with both subtests passing (`TestAssignStageDistributions` and `TestAssignStageDistributions_OutOfOrderInput`)

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/distribution.go internal/planner/physical/distribution_test.go
git commit -m "$(cat <<'EOF'
feat(planner): add assignStageDistributions

Walks the stages slice and populates Stage.Distribution per the
OutputDistribution rules. Dependency resolution is by-ID so stages
provided out of topological order are still resolved correctly —
fuseJoinStages rewires deps after walkStages, and the assignment must
not assume slice order matches dependency order.

workerCount reserved for Phase 2 probe-split distribution rules.
EOF
)"
```

---

## Task 7: Add `BehaviorPreservingMode` package var and `AssertExchangeConsistency` walker

**Files:**
- Modify: `internal/planner/physical/distribution_test.go`
- Modify: `internal/planner/physical/distribution.go`

- [ ] **Step 1: Append the failing tests to `distribution_test.go`**

```go
func TestAssertExchangeConsistency_ConsistentPlan(t *testing.T) {
	// scan -> shuffle (on l_orderkey) -> join.probe (RequiredClusteredOn l_orderkey)
	// scan -> shuffle (on o_orderkey) -> join.build (RequiredClusteredOn o_orderkey)
	// All edges satisfy: hash-partitioned-on-K satisfies clustered-on-K.
	stages := []Stage{
		{ID: "scan-0", Type: "scan", Distribution: Distribution{Kind: DistSingleton}},
		{ID: "scan-1", Type: "scan", Distribution: Distribution{Kind: DistSingleton}},
		{
			ID: "shuffle-2", Type: "shuffle",
			ShuffleKeys: []string{"l_orderkey"}, NumPartitions: 16,
			Dependencies: []string{"scan-0"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
		{
			ID: "shuffle-3", Type: "shuffle",
			ShuffleKeys: []string{"o_orderkey"}, NumPartitions: 16,
			Dependencies: []string{"scan-1"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"o_orderkey"}, Count: 16},
		},
		{
			ID: "join-4", Type: "hash_join",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "shuffle-2", RightDepStage: "shuffle-3",
			Dependencies: []string{"shuffle-2", "shuffle-3"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
	}

	if err := AssertExchangeConsistency(stages); err != nil {
		t.Fatalf("expected no error on consistent plan, got: %v", err)
	}
}

func TestAssertExchangeConsistency_BrokenPlan_StrictMode(t *testing.T) {
	// Save and restore the package var.
	prev := BehaviorPreservingMode
	BehaviorPreservingMode = false
	defer func() { BehaviorPreservingMode = prev }()

	// join requires its build slot clustered on o_orderkey, but the build
	// dependency is hash-partitioned on c_custkey — violation.
	stages := []Stage{
		{ID: "scan-0", Type: "scan", Distribution: Distribution{Kind: DistSingleton}},
		{
			ID: "shuffle-1", Type: "shuffle",
			ShuffleKeys: []string{"l_orderkey"}, NumPartitions: 16,
			Dependencies: []string{"scan-0"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
		{ID: "scan-2", Type: "scan", Distribution: Distribution{Kind: DistSingleton}},
		{
			ID: "shuffle-3", Type: "shuffle",
			ShuffleKeys: []string{"c_custkey"}, NumPartitions: 16,
			Dependencies: []string{"scan-2"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"c_custkey"}, Count: 16},
		},
		{
			ID: "join-4", Type: "hash_join",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "shuffle-1", RightDepStage: "shuffle-3",
			Dependencies: []string{"shuffle-1", "shuffle-3"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
	}

	err := AssertExchangeConsistency(stages)
	if err == nil {
		t.Fatal("expected error on broken plan in strict mode, got nil")
	}
	if !strings.Contains(err.Error(), "join-4") {
		t.Errorf("error should mention violating consumer stage join-4, got: %v", err)
	}
	if !strings.Contains(err.Error(), "shuffle-3") {
		t.Errorf("error should mention violating producer stage shuffle-3, got: %v", err)
	}
}

func TestAssertExchangeConsistency_BrokenPlan_BehaviorPreservingMode(t *testing.T) {
	prev := BehaviorPreservingMode
	BehaviorPreservingMode = true
	defer func() { BehaviorPreservingMode = prev }()

	// Same broken plan as the strict-mode test.
	stages := []Stage{
		{ID: "scan-0", Type: "scan", Distribution: Distribution{Kind: DistSingleton}},
		{
			ID: "shuffle-1", Type: "shuffle",
			ShuffleKeys: []string{"l_orderkey"}, NumPartitions: 16,
			Dependencies: []string{"scan-0"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
		{ID: "scan-2", Type: "scan", Distribution: Distribution{Kind: DistSingleton}},
		{
			ID: "shuffle-3", Type: "shuffle",
			ShuffleKeys: []string{"c_custkey"}, NumPartitions: 16,
			Dependencies: []string{"scan-2"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"c_custkey"}, Count: 16},
		},
		{
			ID: "join-4", Type: "hash_join",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "shuffle-1", RightDepStage: "shuffle-3",
			Dependencies: []string{"shuffle-1", "shuffle-3"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"l_orderkey"}, Count: 16},
		},
	}

	// In BehaviorPreservingMode, AssertExchangeConsistency returns nil —
	// the violation is logged at WARN but does not bubble up as an error.
	if err := AssertExchangeConsistency(stages); err != nil {
		t.Fatalf("expected nil in BehaviorPreservingMode (warn-only), got: %v", err)
	}
}
```

- [ ] **Step 2: Add the `strings` import to `distribution_test.go` if not already present**

Run: `go test -run TestAssertExchangeConsistency ./internal/planner/physical/`
Expected: build failure mentioning `undefined: AssertExchangeConsistency`, `undefined: BehaviorPreservingMode`, and possibly `undefined: strings`

If `strings` is not already imported, add it. Open `internal/planner/physical/distribution_test.go` and ensure the import block includes `"strings"`:

```go
import (
	"strings"
	"testing"
)
```

- [ ] **Step 3: Add `BehaviorPreservingMode`, `AssertExchangeConsistency`, and supporting types to `distribution.go`**

First, add the `log` import. At the top of `distribution.go`, change the package declaration to include imports:

```go
package physical

import (
	"fmt"
	"log"
)
```

Then append after `assignStageDistributions`:

```go
// BehaviorPreservingMode controls assertion hardness. When true (Phase 1
// default), AssertExchangeConsistency logs violations at WARN and returns
// nil — every existing distributed plan continues to execute unchanged
// even if the property algebra rules in this file are wrong. When false
// (tests, and Phase 2 onward), violations are returned as errors and
// callers must handle them.
//
// Phase 2 deletes this var and makes the assertion always strict — the
// EnsureDistribution rule guarantees no violation can survive into the
// emitted plan.
var BehaviorPreservingMode = true

// joinSlot derives the per-dependency slot index for a join stage by
// matching dependency IDs against LeftDepStage / RightDepStage. Returns
// 0 for the probe (left), 1 for the build (right), -1 if dep matches
// neither (which means the dep is auxiliary, e.g. a fused-join build).
func joinSlot(stage Stage, depID string) int {
	if depID == stage.LeftDepStage {
		return 0
	}
	if depID == stage.RightDepStage {
		return 1
	}
	return -1
}

// AssertExchangeConsistency walks every (producer, consumer, slot) edge in
// the stages slice and asserts that producer.Distribution.Satisfies(
// RequiredChildDistribution(consumer, slot)). Returns the first violation
// as an error, or nil if all edges are consistent.
//
// In BehaviorPreservingMode, violations are logged at WARN and nil is
// returned — Phase 1 is purely additive and must not block any plan that
// the heuristic switch would otherwise accept.
//
// Phase 2 promotes this to the satisfaction check that drives Exchange
// insertion: a violation triggers an Exchange stage being added, not a
// plan rejection.
func AssertExchangeConsistency(stages []Stage) error {
	byID := make(map[string]Stage, len(stages))
	for _, s := range stages {
		byID[s.ID] = s
	}

	for _, consumer := range stages {
		for _, depID := range consumer.Dependencies {
			producer, ok := byID[depID]
			if !ok {
				// Dangling dep — not a Phase 1 concern (validateStageGraph
				// already covers this). Skip silently.
				continue
			}

			// Determine the slot. For join stages, derive from
			// LeftDepStage / RightDepStage. For non-join consumers, slot 0
			// (single-input) is the only meaningful index — Phase 1 does
			// not assert non-join multi-input requirements.
			slot := 0
			if consumer.Type == "hash_join" || consumer.Type == "broadcast_join" {
				s := joinSlot(consumer, depID)
				if s < 0 {
					// Auxiliary dep (e.g. fused-join build). Skip — no
					// Phase 1 rule constrains it.
					continue
				}
				slot = s
			}

			req := RequiredChildDistribution(consumer, slot)
			if !producer.Distribution.Satisfies(req) {
				violation := fmt.Errorf(
					"exchange consistency violation: consumer=%s (type=%s, slot=%d) requires %s%v "+
						"but producer=%s emits Distribution{Kind=%v, Keys=%v, Count=%d}",
					consumer.ID, consumer.Type, slot,
					req.Kind, req.Keys,
					producer.ID, producer.Distribution.Kind, producer.Distribution.Keys, producer.Distribution.Count,
				)
				if BehaviorPreservingMode {
					log.Printf("WARN: %v", violation)
					continue
				}
				return violation
			}
		}
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify all three pass**

Run: `go test -run TestAssertExchangeConsistency ./internal/planner/physical/`
Expected: `PASS` with three subtests passing (`_ConsistentPlan`, `_BrokenPlan_StrictMode`, `_BrokenPlan_BehaviorPreservingMode`)

- [ ] **Step 5: Run the full distribution test file to verify nothing else broke**

Run: `go test -run 'TestDistribution|TestRequired|TestOutput|TestAssign|TestAssert' ./internal/planner/physical/`
Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add internal/planner/physical/distribution.go internal/planner/physical/distribution_test.go
git commit -m "$(cat <<'EOF'
feat(planner): add AssertExchangeConsistency + BehaviorPreservingMode

Walks every (producer, consumer, slot) edge and asserts the producer's
Distribution satisfies the consumer's RequiredChildDistribution. Phase 1
ships in BehaviorPreservingMode=true: violations are logged at WARN and
nil is returned, preserving every plan the heuristic switch produces
today even if the property algebra rules are wrong. Tests flip the var
to assert hard.

Phase 2 deletes BehaviorPreservingMode; the EnsureDistribution rule
guarantees no violation can survive into the emitted plan.
EOF
)"
```

---

## Task 8: Wire `assignStageDistributions` + `AssertExchangeConsistency` into `PlanDistributed`

**Files:**
- Modify: `internal/planner/physical/distribution_test.go`
- Modify: `internal/planner/physical/plan.go:975-988` (add three lines after `enforceQueryLimits`)

- [ ] **Step 1: Append the failing integration test to `distribution_test.go`**

```go
func TestPlanDistributed_PopulatesStageDistribution(t *testing.T) {
	// Confirm PlanDistributed populates Stage.Distribution on every stage
	// it emits. Uses the TPC-H test catalog from plan_tpch_test.go (same
	// package) and a synthetic 2-table join.
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT l_orderkey, o_orderdate
		FROM lineitem JOIN orders ON l_orderkey = o_orderkey
		WHERE o_orderdate >= '1995-01-01'`

	stages := sqlToStages(t, cat, ctx, sql, 4)

	for _, s := range stages {
		// Every stage must have a populated Distribution. The zero value
		// is {Kind: DistSingleton, Keys: nil, Count: 0} which is a valid
		// "populated" value for stages that emit singleton output. We
		// detect "unpopulated" by checking that the wire-up actually ran
		// — for shuffle stages, the non-zero Count is a reliable proof.
		if s.Type == "shuffle" {
			if s.Distribution.Kind != DistHashPartitioned {
				t.Errorf("shuffle stage %s: Distribution.Kind = %v, want DistHashPartitioned", s.ID, s.Distribution.Kind)
			}
			if s.Distribution.Count == 0 {
				t.Errorf("shuffle stage %s: Distribution.Count = 0 (assignStageDistributions not wired in)", s.ID)
			}
			if len(s.Distribution.Keys) == 0 {
				t.Errorf("shuffle stage %s: Distribution.Keys empty (assignStageDistributions not wired in)", s.ID)
			}
		}
	}
}
```

- [ ] **Step 2: Run the test to verify it fails (wire-up not yet in place)**

Run: `go test -run TestPlanDistributed_PopulatesStageDistribution ./internal/planner/physical/`
Expected: `FAIL` — at least one shuffle stage has `Distribution.Kind = 0` (DistSingleton, the zero value) and `Count = 0`

- [ ] **Step 3: Wire the calls into `PlanDistributed`**

Open `internal/planner/physical/plan.go`. Find the existing `PlanDistributed` (around line 975):

```go
func (p *Planner) PlanDistributed(ctx context.Context, node *logical.Node) ([]Stage, error) {
	p.planCtx = ctx // store for scalar subquery evaluation during stage generation
	if len(node.CTEs) > 0 {
		p.ctes = node.CTEs
	}
	// Ensure scan nodes have column metadata — needed by fixJoinKeyOrder
	// to assign shuffle keys to the correct child side.
	p.AnnotateScanColumns(ctx, node)
	stages := p.generateStages(node)
	if err := p.enforceQueryLimits(stages, node); err != nil {
		return nil, err
	}
	return stages, nil
}
```

Replace the body so the final lines become:

```go
func (p *Planner) PlanDistributed(ctx context.Context, node *logical.Node) ([]Stage, error) {
	p.planCtx = ctx // store for scalar subquery evaluation during stage generation
	if len(node.CTEs) > 0 {
		p.ctes = node.CTEs
	}
	// Ensure scan nodes have column metadata — needed by fixJoinKeyOrder
	// to assign shuffle keys to the correct child side.
	p.AnnotateScanColumns(ctx, node)
	stages := p.generateStages(node)
	if err := p.enforceQueryLimits(stages, node); err != nil {
		return nil, err
	}
	// Phase 1 distribution-property pass: populate Stage.Distribution for
	// every stage and assert exchange consistency. In BehaviorPreservingMode
	// (default), violations are logged but do not fail planning. See
	// docs/archive/specs/2026-04-20-distribution-property-phase-1.md.
	assignStageDistributions(stages, p.WorkerCount)
	if err := AssertExchangeConsistency(stages); err != nil {
		// In strict mode (Phase 2 onward, or test override) this is a
		// hard failure. BehaviorPreservingMode swallows the error inside
		// AssertExchangeConsistency, so reaching this branch implies the
		// caller flipped the var.
		return nil, fmt.Errorf("exchange consistency: %w", err)
	}
	return stages, nil
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -run TestPlanDistributed_PopulatesStageDistribution ./internal/planner/physical/`
Expected: `PASS`

- [ ] **Step 5: Run the full distribution test file again**

Run: `go test -run 'TestDistribution|TestRequired|TestOutput|TestAssign|TestAssert|TestPlanDistributed_Populates' ./internal/planner/physical/`
Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add internal/planner/physical/distribution.go internal/planner/physical/distribution_test.go internal/planner/physical/plan.go
git commit -m "$(cat <<'EOF'
feat(planner): wire distribution-property pass into PlanDistributed

After enforceQueryLimits, call assignStageDistributions to populate
Stage.Distribution on every stage and AssertExchangeConsistency to
verify producer/consumer edges are consistent. In BehaviorPreservingMode
(Phase 1 default), violations are logged at WARN and PlanDistributed
returns the stages unchanged — the existing heuristic switch in
coordinator.go continues to drive every routing decision.

Integration test confirms shuffle stages carry populated DistHashPartitioned.
EOF
)"
```

---

## Task 9: Add `TestTPCHDistributionConsistency` — the Phase 1 acceptance gate

**Files:**
- Modify: `internal/planner/physical/plan_tpch_test.go`

- [ ] **Step 1: Append the acceptance-gate test to `plan_tpch_test.go`**

```go
// TestTPCHDistributionConsistency is the Phase 1 acceptance gate for the
// distribution-property pass. For every Q1-Q22, runs PlanDistributed with
// WorkerCount=4 in strict mode (BehaviorPreservingMode=false) and asserts:
//   1. Every stage has a populated Distribution (shuffle stages explicitly
//      DistHashPartitioned with non-zero Count).
//   2. AssertExchangeConsistency(stages) == nil.
//
// Failure on any query means either the OutputDistribution /
// RequiredChildDistribution rules are wrong or PlanDistributed emits an
// inconsistent plan. Either way, the spec's load-bearing invariant is
// broken and Phase 2 cannot proceed safely.
//
// Spec: docs/archive/specs/2026-04-20-distribution-property-phase-1.md
func TestTPCHDistributionConsistency(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	// Strict mode: any consistency violation must surface as an error.
	prev := BehaviorPreservingMode
	BehaviorPreservingMode = false
	defer func() { BehaviorPreservingMode = prev }()

	for qNum := 1; qNum <= 22; qNum++ {
		sql, ok := tpchPlanQueryMap[qNum]
		if !ok {
			t.Logf("Q%02d not in plan query map; skipping", qNum)
			continue
		}
		name := fmt.Sprintf("Q%02d", qNum)
		t.Run(name, func(t *testing.T) {
			parsed, err := plansql.Parse(sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			selectInfo, err := plansql.ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			logicalPlan, err := logical.BuildFromSelect(selectInfo)
			if err != nil {
				t.Fatalf("logical plan: %v", err)
			}
			scanAnnotator := func(plan *logical.Node) {
				NewPlanner(cat).AnnotateScanColumns(ctx, plan)
			}
			scanAnnotator(logicalPlan)
			logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

			planner := NewPlanner(cat)
			planner.WorkerCount = 4
			stages, err := planner.PlanDistributed(ctx, logicalPlan)
			if err != nil {
				// In strict mode, exchange-consistency violations come back
				// as errors prefixed "exchange consistency: ...".
				t.Fatalf("PlanDistributed failed: %v", err)
			}

			// Assertion 1: every stage has a populated Distribution. The
			// zero value DistSingleton is "populated" for stages that emit
			// singleton output, but shuffle stages must carry the
			// hash-partitioned label with non-zero Count and non-empty Keys.
			for _, s := range stages {
				if s.Type == "shuffle" {
					if s.Distribution.Kind != DistHashPartitioned {
						t.Errorf("%s shuffle stage %s: Distribution.Kind = %v, want DistHashPartitioned",
							name, s.ID, s.Distribution.Kind)
					}
					if s.Distribution.Count == 0 {
						t.Errorf("%s shuffle stage %s: Distribution.Count = 0 (not populated)",
							name, s.ID)
					}
					if len(s.Distribution.Keys) == 0 {
						t.Errorf("%s shuffle stage %s: Distribution.Keys empty (not populated)",
							name, s.ID)
					}
				}
			}

			// Assertion 2: in strict mode, AssertExchangeConsistency
			// already ran inside PlanDistributed — if it returned an
			// error it would have aborted above. Re-run defensively to
			// log per-stage detail on failure.
			if err := AssertExchangeConsistency(stages); err != nil {
				for _, s := range stages {
					t.Logf("  %-24s type=%-16s dist=%+v",
						s.ID, s.Type, s.Distribution)
				}
				t.Fatalf("%s: %v", name, err)
			}
		})
	}
}
```

- [ ] **Step 2: Run the acceptance gate**

Run: `go test -v -run TestTPCHDistributionConsistency ./internal/planner/physical/`
Expected: `PASS` for every Q01-Q22 subtest. If any subtest fails, the failure log shows the per-stage Distribution detail — read it carefully. Likely root causes (in priority order):
  - **Merge-group label mismatch (spec Risk #1):** if a merge-grouped `final_aggregate` consumer flags a violation against its `final_aggregate-i-g` producer, the rule in `OutputDistribution` (currently `DistHashPartitioned`) needs to flip to `DistSingleton` for merge-grouped finals. Update the rule, re-run the test.
  - **Probe-split scan label opaqueness (spec Risk #2):** unlikely to fire because `RequiredAny` is the default consumer requirement for scans; passes by default.
  - **Fused-join dep skipped incorrectly:** if `joinSlot` returns -1 for a fused-join build dep that should be checked, the assertion is over-skipping. Phase 1 deliberately skips these (auxiliary deps); document any false negatives in the test log but do not fail.

- [ ] **Step 3: If a violation surfaces, choose remediation and re-run**

If the failure is the merge-group label (Risk #1), edit `OutputDistribution` in `distribution.go`. Replace the `final_aggregate, merge_aggregate` arm:

```go
	case "final_aggregate", "merge_aggregate":
		// Merge-grouped finals: today's coordinator routes results by
		// MergeGroup index, not by hash partitioning. Phase 1 labels
		// them DistSingleton per group — the routing scheme makes the
		// "partition" concept inapplicable to the property algebra.
		// (Spec Risk #1 documented Phase 1's symmetry-with-Trino label
		// as a fallback; the test above is the authoritative source.)
		return Distribution{Kind: DistSingleton}
```

Then re-run `go test -v -run TestTPCHDistributionConsistency ./internal/planner/physical/`. Expected: `PASS` for every Q01-Q22 subtest.

- [ ] **Step 4: Commit (commit the test even if you also had to amend OutputDistribution)**

```bash
git add internal/planner/physical/plan_tpch_test.go internal/planner/physical/distribution.go
git commit -m "$(cat <<'EOF'
test(planner): TPC-H distribution-consistency acceptance gate

For every Q1-Q22, runs PlanDistributed with WorkerCount=4 in strict
mode (BehaviorPreservingMode=false) and asserts every stage has a
populated Distribution and AssertExchangeConsistency returns nil.
This is the Phase 1 acceptance gate — Phase 2 cannot proceed if this
fails.

If the merge-group rule from spec §"OutputDistribution" needed
adjustment, the OutputDistribution change is included here too with
the updated Risk #1 documentation inline.
EOF
)"
```

---

## Task 10: Verify TPC-H SF0.01 row-checksum correctness (no behavior change)

**Files:** None modified. Verification step only.

- [ ] **Step 1: Run TPC-H SF0.01 correctness suite**

Run: `go test -v -run TestTPCHQueries ./benchmarks/tpch/ 2>&1 | tee /tmp/tpch-sf001-after.txt`
Expected: every query passes — same row counts and (where checked) checksums as on the parent commit (`0b78c4e`). Total runtime ~5-15 seconds.

- [ ] **Step 2: Verify the test summary line**

Run: `grep -E '^(--- FAIL|FAIL|PASS|ok)' /tmp/tpch-sf001-after.txt | head -5`
Expected output ends with `PASS` and `ok  github.com/derekmwright/wadjet/benchmarks/tpch`. No `FAIL` lines.

- [ ] **Step 3: If any failure, halt and report**

If any TPC-H SF0.01 query fails, **stop immediately**. The Phase 1 plan is purely additive and the only added work is one O(stages) walk plus one O(edges) assertion. A behavioral change indicates either:
  - The wire-up in `PlanDistributed` accidentally mutated something other than `Stage.Distribution` (audit `assignStageDistributions` for unintended writes).
  - `AssertExchangeConsistency` returned a non-nil error in BehaviorPreservingMode (it should never — verify the code path).

Open an issue or report back with the failure detail; do not commit further.

- [ ] **Step 4: No commit — verification step only**

This task records the verification artifact at `/tmp/tpch-sf001-after.txt`. No file changes; nothing to commit.

---

## Task 11: Run the full project test suite to confirm no regressions

**Files:** None modified. Verification step only.

- [ ] **Step 1: Run the full internal test suite**

Run: `go test ./internal/... 2>&1 | tee /tmp/internal-tests-after.txt`
Expected: `ok` for every package. Total runtime 1-3 minutes.

- [ ] **Step 2: Inspect for any FAIL lines**

Run: `grep -E '^(FAIL|--- FAIL)' /tmp/internal-tests-after.txt`
Expected: empty output. Any FAIL line indicates a regression.

- [ ] **Step 3: If any failure, isolate and fix**

If a non-physical-package test failed, the most likely cause is a callsite that depended on the old `SatisfiesJoinKeys` behavior. Open the failing test, inspect the assertion, and verify the new `Satisfies` delegation produces the same boolean. The truth table in Task 2's `TestDistributionSatisfies` is authoritative.

If a physical-package test failed that wasn't covered above (e.g. a `plan_coverage_test.go` test), inspect the failure: the new pass populates `Stage.Distribution` on every stage, which previously was always the zero value. If the test asserts `s.Distribution.Equals(Distribution{})` or similar zero-value checks, the test needs updating to reflect populated distributions — make the change, re-run, commit with `test(planner): update Distribution zero-value assertions for Phase 1`.

- [ ] **Step 4: Run `go vet` for static analysis cleanliness**

Run: `go vet ./internal/planner/physical/...`
Expected: empty output (no warnings).

- [ ] **Step 5: No commit — verification step only (unless a test was updated in Step 3)**

If Step 3 required test updates, commit them as their own `test(planner): ...` commit. Otherwise no commit.

---

## Task 12: Final summary check — confirm Phase 1 acceptance criteria

**Files:** None modified. Audit step only.

- [ ] **Step 1: Audit the spec's acceptance criteria against the implementation**

Re-read `docs/archive/specs/2026-04-20-distribution-property-phase-1.md` §"Goals" (numbered 1-6). Confirm each is satisfied:

| Goal | Verification |
|---|---|
| 1. Promote Distribution to populated property | Task 8 wire-up + Task 9 acceptance gate |
| 2. RequiredDistribution as derived view | Task 4 `RequiredChildDistribution` |
| 3. `Satisfies` predicate generalising `SatisfiesJoinKeys` | Tasks 2 + 3 |
| 4. Planner-layer assertion runs after `PlanDistributed` | Tasks 7 + 8 + 9 |
| 5. SF0.01 identical row checksums | Task 10 |
| 6. SF1 wall-time within ±5% | Out-of-scope for this plan; spec calls out as a release-gate, not a per-PR gate. Operator may defer to CI. |

- [ ] **Step 2: Verify branch state**

Run: `git log --oneline 0b78c4e..HEAD`
Expected: 8-10 commits all prefixed `feat(planner)`, `refactor(planner)`, or `test(planner)` per the Conventional Commits convention.

Run: `git status`
Expected: `nothing to commit, working tree clean`.

- [ ] **Step 3: Verify no out-of-scope changes**

Run: `git diff --stat 0b78c4e..HEAD`
Expected: changes only under `internal/planner/physical/`. No changes to `internal/coordinator/`, `internal/worker/`, `internal/engine/`, or any other package.

If any out-of-scope file appears in the diff, **stop and revert that change**: Phase 1 is planner-only by spec.

- [ ] **Step 4: Phase 1 complete**

The branch `feat/distribution-property-phase-1` is ready for PR. PR title: `feat(planner): distribution-property pass phase 1 — wire up scaffolding`. PR body should reference the spec and the acceptance gate (`TestTPCHDistributionConsistency`).

No commit for this audit step.

---

## Summary

12 tasks, ~50 steps total (TDD-disciplined: each new function gets a failing test → run-fail → implement → run-pass → commit). The acceptance gate is `TestTPCHDistributionConsistency` (Task 9) which runs every Q01-Q22 through `PlanDistributed` in strict mode. The behavior-preservation gate is `TestTPCHQueries` at SF0.01 (Task 10) — every query must produce identical results to the parent commit `0b78c4e`.

Out of scope (per spec §"Non-Goals"): coordinator changes, worker changes, executor changes, cost-based decisions, Exchange stage type, adaptive runtime re-planning. These are Phase 2-4 and explicitly forbidden in this PR.
