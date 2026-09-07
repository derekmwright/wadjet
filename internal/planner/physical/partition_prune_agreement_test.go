package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/partition"
)

// The two partition prunes answer the same question the same way (#904).
//
// There are two: `physical.matchesPartitionFilter`, which the live scan source
// uses, and `partition.MatchesFilter`, which `partition.PrunePartitions` and
// `scan.Scanner` use. They disagreed about an ABSENT key — the physical one
// skips it and keeps the partition, the storage one read "" out of the map,
// compared it against the filter value and pruned EVERY partition. One of them
// decides which files a query reads, so a disagreement here is an empty answer
// for a query that has rows, and it is invisible to any test that exercises
// only one of them.
//
// This gate is the reason the storage copy was FIXED rather than deleted: its
// caller (`scan.Scanner`) is unreachable today — `scan.NewScanner` has no
// callers — but "unreachable" is a claim about the current tree, and the two
// functions are one rule.
func TestBothPartitionPrunesAgreeOnAnAbsentKey(t *testing.T) {
	for _, c := range []struct {
		name       string
		partValues map[string]string
		filter     map[string]string
		want       bool
	}{
		{"absent key", map[string]string{"year": "2026"},
			map[string]string{"month": "03"}, true},
		{"absent key beside a matching one", map[string]string{"year": "2026"},
			map[string]string{"year": "2026", "month": "03"}, true},
		{"absent key beside a mismatching one", map[string]string{"year": "2026"},
			map[string]string{"year": "2025", "month": "03"}, false},
		{"all present and matching", map[string]string{"year": "2026", "month": "03"},
			map[string]string{"year": "2026", "month": "03"}, true},
		{"all present, one mismatch", map[string]string{"year": "2026", "month": "03"},
			map[string]string{"year": "2026", "month": "04"}, false},
		{"empty filter", map[string]string{"year": "2026"},
			map[string]string{}, true},
		{"partition carries nothing", map[string]string{},
			map[string]string{"year": "2026"}, true},
		// An EMPTY filter value against a key the partition carries as empty
		// is a real match, and against one it does not carry is the absent
		// case — the pair the "" sentinel conflated.
		{"empty value present", map[string]string{"year": ""},
			map[string]string{"year": ""}, true},
		{"empty value absent", map[string]string{"month": "03"},
			map[string]string{"year": ""}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			phys := matchesPartitionFilter(c.partValues, c.filter)
			store := partition.MatchesFilter(c.partValues, c.filter)
			if phys != store {
				t.Fatalf("physical.matchesPartitionFilter = %v, partition.MatchesFilter = %v "+
					"— two arms, one question, and the one that prunes decides which files "+
					"a query reads", phys, store)
			}
			if phys != c.want {
				t.Errorf("= %v, want %v", phys, c.want)
			}
		})
	}
}
