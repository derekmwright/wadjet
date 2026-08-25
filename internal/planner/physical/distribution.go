package physical

import (
	"fmt"
	"log"
	"strings"
)

// DistKind is the kind of partitioning a stage's output has.
type DistKind int

const (
	DistSingleton       DistKind = iota // single worker has all rows
	DistBroadcast                       // every worker has all rows
	DistHashPartitioned                 // rows partitioned by hash(Keys) % Count
	// DistRoundRobin: multiple parallel tasks, no key clustering. Used to
	// model multi-task partial aggregates / multi-task scans where each task
	// emits its own subset of the input rows. Unlike DistSingleton, this kind
	// does NOT trivially satisfy RequiredClusteredOn — downstream consumers
	// that need keys co-located must shuffle. The Phase 1 spec deferred this
	// label to Phase 2/3; needed once the executor wires real fan-out into
	// the property graph (e.g. dispatchScanAggregateStage at workerCount > 1).
	DistRoundRobin
)

// Distribution describes how a stage's output is partitioned across workers.
type Distribution struct {
	Kind  DistKind
	Keys  []string // for DistHashPartitioned
	Count int      // for DistHashPartitioned
}

// Equals reports whether two Distributions are identical.
func (d Distribution) Equals(other Distribution) bool {
	if d.Kind != other.Kind {
		return false
	}
	if d.Kind != DistHashPartitioned {
		return true
	}
	if d.Count != other.Count || len(d.Keys) != len(other.Keys) {
		return false
	}
	for i := range d.Keys {
		if d.Keys[i] != other.Keys[i] {
			return false
		}
	}
	return true
}

// SatisfiesJoinKeys reports whether this distribution allows a co-located
// join on the given keys without re-shuffling. Preserved as a thin wrapper
// over Satisfies for existing callers.
func (d Distribution) SatisfiesJoinKeys(joinKeys []string) bool {
	return d.Satisfies(RequiredDistribution{Kind: RequiredClusteredOn, Keys: joinKeys})
}

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

// Satisfies reports whether this distribution meets a consumer's required
// distribution. Single mechanical predicate that mirrors Spark's
// Partitioning.satisfies(Distribution). The truth table is documented in
// the Phase 1 spec §"The property algebra".
//
//	RequiredAny:                    always true.
//	RequiredSingleton:              only DistSingleton.
//	RequiredBroadcast:              only DistBroadcast.
//	RequiredClusteredOn(K):         DistBroadcast yes; DistSingleton yes;
//	                                DistHashPartitioned iff Keys==K;
//	                                DistRoundRobin no (multi-task, unclustered).
//	RequiredHashPartitionedOn(K, N): only DistHashPartitioned with Keys==K
//	                                and Count==N.
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
	case StageScan, "dual":
		// No inputs — any slot is RequiredAny by definition.
		return RequiredDistribution{Kind: RequiredAny}
	case StageExchangeRepartition:
		// Exchange-repartition accepts any input and re-partitions.
		return RequiredDistribution{Kind: RequiredAny}
	case StageHashJoin, StageSortMergeJoin:
		switch slot {
		case 0:
			return RequiredDistribution{Kind: RequiredClusteredOn, Keys: stage.JoinLeftKeys}
		case 1:
			return RequiredDistribution{Kind: RequiredClusteredOn, Keys: stage.JoinRightKeys}
		default:
			return RequiredDistribution{Kind: RequiredAny}
		}
	case StageBroadcastJoin:
		// Slot 1 (build) requires Broadcast: every worker that runs the
		// join needs the full build set. EnsureDistribution splices a
		// StageExchangeReplicate ahead of the build when the upstream
		// isn't already DistBroadcast; dispatchReplicateStage materializes
		// the upstream once into a single broadcast file that all
		// probe-split tasks read in parallel, eliminating N× duplicated
		// probe-side parquet reads.
		//
		// Slot 0 (probe) stays RequiredAny — the dispatcher slices probe
		// files across tasks (broadcastJoinProbeSplit), which is more
		// efficient than asking the planner to insert a shuffle.
		switch slot {
		case 1:
			return RequiredDistribution{Kind: RequiredBroadcast}
		default:
			return RequiredDistribution{Kind: RequiredAny}
		}
	case StageAggregate:
		// Phase 1 conservative: today's two-phase distributed aggregate
		// runs the partial stage on RequiredAny inputs (the partial does
		// not require pre-clustering — it produces partials that the final
		// stage merges). See spec Risk #1.
		return RequiredDistribution{Kind: RequiredAny}
	case "final_aggregate", "merge_aggregate":
		// Grouped final/merge aggregates require their input to be clustered
		// on the GROUP BY keys so the merge fans out across workers — each
		// downstream task sees a disjoint slice of keys instead of the full
		// hash table. Without this, dispatchFinalAggregateFanout creates N
		// intermediate tasks that each materialise ALL groups (e.g. Q18 SF10
		// final_aggregate-46 with group_by=l_orderkey: every intermediate
		// builds a ~15 M-group hash table, OOM-kills the worker, JetStream
		// redelivers, repeat).
		//
		// SortKeys / Limit / scalar (no GroupByCols) stay on RequiredAny:
		// SortKeys force serial execution anyway, Limit relies on the
		// pre-existing single-task collapse, and scalar aggregates produce
		// one row regardless of input distribution. Exception: a
		// SortShardLocal final carries shard-local SortKeys/Limit (each
		// shard top-Ks its own disjoint groups; the surviving downstream
		// sort merges), so it clusters like a sort-free grouped final.
		if len(stage.GroupByCols) > 0 &&
			(stage.SortShardLocal || (len(stage.SortKeys) == 0 && !stage.HasLimit)) {
			return RequiredDistribution{Kind: RequiredClusteredOn, Keys: stage.GroupByCols}
		}
		return RequiredDistribution{Kind: RequiredAny}
	case StageSort, "merge_sort":
		return RequiredDistribution{Kind: RequiredAny}
	case StageWindow:
		// A window over PARTITION BY k is computable one partition at a
		// time, so clustering on k is exactly what lets the stage fan out.
		// windowPartitionKeys declines the shapes where that is not true —
		// a column with no PARTITION BY (global: every row must meet), or
		// two columns partitioned on different keys (one exchange cannot
		// cluster both, and clustering on the first would slice the second
		// one's partitions across tasks). Those run Singleton, where
		// RequiredAny is honest: one task reads everything.
		if keys, ok := windowPartitionKeys(stage); ok {
			return RequiredDistribution{Kind: RequiredClusteredOn, Keys: keys}
		}
		return RequiredDistribution{Kind: RequiredAny}
	case StageLimit:
		// RequiredAny, and honestly so: a Singleton stage's single task
		// reads EVERY partition of its input (partitionFilesForWorker with
		// workerCount == 1), which is exactly what makes the bound global.
		// Asking for RequiredSingleton would splice in a gather exchange
		// that moves the same bytes to the same one task.
		return RequiredDistribution{Kind: RequiredAny}
	case StagePipeline, "table_func":
		return RequiredDistribution{Kind: RequiredAny}
	default:
		return RequiredDistribution{Kind: RequiredAny}
	}
}

// OutputDistribution computes the partitioning a stage's output has, given
// the resolved distributions of its dependencies. Pure function over
// stage fields + dep map + cluster size. Rules track how today's planner
// emits stages; see the Phase 1 spec §"OutputDistribution" for the per-stage
// table.
//
// workerCount is needed to distinguish single-task (Singleton) from multi-task
// (RoundRobin) variants of stages whose dispatcher fans out at runtime
// (e.g. dispatchScanAggregateStage). Phase 1 punted on this and labeled
// everything Singleton (see spec Risk #2); Phase 3 wires the real label so
// the property algebra correctly forces hash-shuffles ahead of grouped
// finals.
func OutputDistribution(stage Stage, deps map[string]Distribution, workerCount int) Distribution {
	switch stage.Type {
	case StageScan:
		// Scans with fused partial aggregation (canFuseScanAggregate fires
		// when the GROUP BY has no join in between) fan out across
		// workerCount tasks at runtime via dispatchScanAggregateStage.
		// Each task emits one partial-aggregate file containing all keys
		// it saw — no key clustering across files. Label RoundRobin so
		// grouped final_aggregate's RequiredClusteredOn triggers a
		// hash-shuffle. Without this, final_aggregate's fanout intermediates
		// each see the full key space → high-cardinality grouped finals OOM
		// (Q18 SF10 inner subquery: GROUP BY l_orderkey, ~15M groups).
		// Plain scans stay Singleton; probe-split / fan-out for plain scans
		// is a separate Phase concern.
		if len(stage.FusedAggGroupBy) > 0 && workerCount > 1 {
			return Distribution{Kind: DistRoundRobin}
		}
		return Distribution{Kind: DistSingleton}
	case "dual":
		return Distribution{Kind: DistSingleton}
	case StageExchangeRepartition:
		if stage.Exchange == nil {
			return Distribution{Kind: DistHashPartitioned}
		}
		return Distribution{
			Kind:  DistHashPartitioned,
			Keys:  stage.Exchange.Keys,
			Count: stage.Exchange.Count,
		}
	case StageExchangeReplicate:
		return Distribution{Kind: DistBroadcast}
	case StageExchangeGather:
		return Distribution{Kind: DistSingleton}
	case StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
		// The join inherits the probe (left) input's distribution — the
		// join itself does not re-partition the joined output, it just
		// pairs probe rows with matching build rows.
		if probe, ok := deps[stage.LeftDepStage]; ok {
			return probe
		}
		return Distribution{Kind: DistSingleton}
	case StageAggregate:
		// Grouped partial aggregate runs as N parallel tasks at runtime
		// (dispatchScanAggregateStage fans out across workerCount). Each
		// task emits one file containing partials for all keys it saw —
		// no key clustering across files. Label this multi-task output
		// as DistRoundRobin so the downstream final_aggregate's
		// RequiredClusteredOn (group keys) triggers a hash-shuffle.
		// Without this label, Singleton trivially satisfies clustered_on
		// and the final_aggregate sees N files all carrying the full key
		// space → high-cardinality grouped finals OOM (Q18 SF10).
		// Single-worker mode keeps Singleton: one task, no shuffle needed.
		if len(stage.GroupByCols) > 0 && workerCount > 1 {
			return Distribution{Kind: DistRoundRobin}
		}
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
		// Grouped final/merge over hash-partitioned input: each task processes
		// one partition slice and emits its own output, mirroring the input's
		// partition layout. This is what makes dispatchComputeStage emit
		// numTasks=Distribution.Count parallel final tasks (each consuming a
		// disjoint slice of group keys), instead of one OOM-prone task that
		// merges all keys serially. Only mirror when SortKeys/Limit are unset
		// — those force serial output today (single-task Sort + Limit) —
		// or when they are SortShardLocal (each shard sorts/limits its own
		// disjoint groups; the surviving downstream sort stage merges).
		if len(stage.GroupByCols) > 0 &&
			(stage.SortShardLocal || (len(stage.SortKeys) == 0 && !stage.HasLimit)) &&
			len(stage.Dependencies) == 1 {
			if depDist, ok := deps[stage.Dependencies[0]]; ok &&
				depDist.Kind == DistHashPartitioned &&
				keysEqual(depDist.Keys, stage.GroupByCols) {
				return depDist
			}
		}
		return Distribution{Kind: DistSingleton}
	case StageSort:
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
	case StageUnion:
		// One task per arm, each emitting that arm's rows whole — no key
		// clustering across the outputs, which is exactly RoundRobin.
		// dispatchComputeStage derives the task count from UnionArms (not
		// from this label) and flattens the per-task files into one
		// OutputSinglePart list, so every downstream consumer reads the
		// whole concatenation.
		return Distribution{Kind: DistRoundRobin}
	case StageWindow:
		// Partition-parallel window: when the input already arrives
		// hash-partitioned on exactly this stage's PARTITION BY keys, every
		// partition of every window group is whole inside one input
		// partition, so task w windows its slice and emits it — the output
		// keeps the input's partitioning. Mirrors the grouped-final rule
		// above, and for the same reason: disjoint keys make per-partition
		// computation exact.
		//
		// Everything else is Singleton — one task holding all the rows.
		// That is the honest label for a GLOBAL window (no PARTITION BY:
		// ROW_NUMBER() OVER (ORDER BY x) numbers rows against every other
		// row) and for a stage whose columns disagree on their partition
		// keys. It is a scalability bound, not a correctness one: a
		// Singleton stage's single task reads every partition of its input
		// (partitionFilesForWorker with workerCount==1), so the answer is
		// the same one the single-process pipeline gives.
		if keys, ok := windowPartitionKeys(stage); ok && len(stage.Dependencies) == 1 {
			if depDist, ok := deps[stage.Dependencies[0]]; ok &&
				depDist.Kind == DistHashPartitioned &&
				keysEqual(depDist.Keys, keys) {
				return depDist
			}
		}
		return Distribution{Kind: DistSingleton}
	case StageLimit:
		// One task holding the whole bounded result. Not a scalability
		// bound worth removing: the stage's OUTPUT is at most LIMIT rows,
		// and there is no sharded way to take the first n of a union
		// anyway — that is the property the stage exists to provide.
		return Distribution{Kind: DistSingleton}
	case StagePipeline, "table_func":
		return Distribution{Kind: DistSingleton}
	default:
		return Distribution{Kind: DistSingleton}
	}
}

// windowPartitionKeys returns the PARTITION BY keys the whole window stage
// can be distributed on, and ok=false when the stage has no such key set.
//
// A window function's partition is its universe: PARTITION BY k means row r's
// value depends only on rows sharing r's k, so hash-partitioning the input on
// k makes each task's slice self-sufficient. Two conditions break that, and
// both answer ok=false rather than picking a key set that is right for part
// of the stage:
//
//   - A column with NO PARTITION BY is a global window — its universe is the
//     entire input, and no exchange makes a task self-sufficient.
//   - Columns partitioned on DIFFERENT keys cannot share one exchange:
//     clustering on the first spec's keys spreads the second spec's
//     partitions across tasks, and each task would then compute the second
//     column over a fragment of its partition. That is a wrong answer, not a
//     slow one.
//
// One window stage can carry several columns with different OVER clauses
// (exec.Window groups them by PartitionBy+OrderBy internally), so the mixed
// case is a real shape, not a theoretical one.
func windowPartitionKeys(stage Stage) ([]string, bool) {
	if len(stage.WindowCols) == 0 {
		return nil, false
	}
	keys := stage.WindowCols[0].PartitionBy
	if len(keys) == 0 {
		return nil, false
	}
	for _, wc := range stage.WindowCols[1:] {
		if !keysEqual(wc.PartitionBy, keys) {
			return nil, false
		}
	}
	// A key the window COMPUTES is not a key the exchange can cluster on:
	// __winkey_N exists only inside the window fragment, after its own
	// projection runs, so an exchange asked to hash-partition on that name
	// would find no such column upstream. Today that is caught by the
	// exchange-consistency validator and the query fails at plan time; what
	// it must not become is a partition spread across tasks, each windowing
	// a fragment of somebody else's — a wrong answer, not a slow one. Those
	// stages run Singleton, which is what the no-PARTITION-BY case above
	// already does and is honest for the same reason: one task reads
	// everything (#585).
	for _, k := range keys {
		for _, m := range stage.WindowKeyExprs {
			if strings.EqualFold(m.Name, k) {
				return nil, false
			}
		}
	}
	return keys, true
}

// assignStageDistributions walks the stages slice in dependency order and
// populates Stage.Distribution per the OutputDistribution rules. The walk
// is by-ID so stages provided out of topological order are still resolved
// correctly (matters because fuseJoinStages rewires deps after walkStages).
//
// workerCount distinguishes single-task vs multi-task variants of stages
// whose dispatcher fans out at runtime (e.g. partial aggregate becomes
// DistRoundRobin when workerCount > 1).
func assignStageDistributions(stages []Stage, workerCount int) {
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
			d := OutputDistribution(*s, depMap, workerCount)
			s.Distribution = d
			resolved[s.ID] = d
			_ = idx // idx kept for Phase 2 stages that need cross-references
		}
	}
}

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
			if consumer.Type == StageHashJoin || consumer.Type == StageBroadcastJoin || consumer.Type == StageSortMergeJoin {
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
