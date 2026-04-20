package physical

// DistKind is the kind of partitioning a stage's output has.
type DistKind int

const (
	DistSingleton       DistKind = iota // single worker has all rows
	DistBroadcast                       // every worker has all rows
	DistHashPartitioned                 // rows partitioned by hash(Keys) % Count
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
// (docs/superpowers/specs/2026-04-20-distribution-property-phase-1.md) §"The
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
