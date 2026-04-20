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
// join on the given keys without re-shuffling. Broadcast always satisfies;
// hash-partitioned satisfies iff the keys match exactly (in order).
func (d Distribution) SatisfiesJoinKeys(joinKeys []string) bool {
	switch d.Kind {
	case DistBroadcast:
		return true
	case DistHashPartitioned:
		if len(d.Keys) != len(joinKeys) {
			return false
		}
		for i := range d.Keys {
			if d.Keys[i] != joinKeys[i] {
				return false
			}
		}
		return true
	default:
		return false
	}
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
