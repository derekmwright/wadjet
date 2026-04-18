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
