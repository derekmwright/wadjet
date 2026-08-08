package coordinator

import (
	"fmt"
	"testing"
)

func testFiles(n int) []string {
	fs := make([]string, n)
	for i := range fs {
		fs[i] = fmt.Sprintf("lineitem/part-%03d.parquet", i)
	}
	return fs
}

// Every file lands in exactly one task, every task's files share one
// rendezvous owner, and the mapping is deterministic across calls and
// input orderings — the property that makes per-worker first touches
// happen once cluster-wide.
func TestAffineFileSetsCoverageAndDeterminism(t *testing.T) {
	files := testFiles(63)
	workers := []string{"w-b", "w-a", "w-c"}
	sets, owners := affineFileSets(files, workers, 12)
	if sets == nil || len(sets) != len(owners) {
		t.Fatalf("affine sets missing/misaligned: %d vs %d", len(sets), len(owners))
	}
	seen := map[string]string{}
	for i, set := range sets {
		for _, f := range set {
			if prev, dup := seen[f]; dup {
				t.Fatalf("file %s in two tasks (%s, %s)", f, prev, owners[i])
			}
			seen[f] = owners[i]
			if got := affinityOwner(f, []string{"w-a", "w-b", "w-c"}); got != owners[i] {
				t.Fatalf("task owner %s != rendezvous owner %s for %s", owners[i], got, f)
			}
		}
	}
	if len(seen) != len(files) {
		t.Fatalf("coverage: %d of %d files assigned", len(seen), len(files))
	}
	// Same mapping when the worker list arrives in a different order.
	sets2, owners2 := affineFileSets(files, []string{"w-c", "w-a", "w-b"}, 12)
	seen2 := map[string]string{}
	for i, set := range sets2 {
		for _, f := range set {
			seen2[f] = owners2[i]
		}
	}
	for f, o := range seen {
		if seen2[f] != o {
			t.Fatalf("owner of %s changed with worker order: %s vs %s", f, o, seen2[f])
		}
	}
	// Task count stays near the requested fan-out.
	if len(sets) < 3 || len(sets) > 15 {
		t.Fatalf("task count %d far from requested 12", len(sets))
	}
}

// Removing one worker remaps ONLY the departed worker's files — the
// rendezvous property that keeps caches warm across membership churn.
func TestAffinityOwnerMinimalRemap(t *testing.T) {
	files := testFiles(200)
	all := []string{"w-a", "w-b", "w-c"}
	without := []string{"w-a", "w-b"}
	moved := 0
	for _, f := range files {
		before := affinityOwner(f, all)
		after := affinityOwner(f, without)
		if before != "w-c" && before != after {
			t.Fatalf("file %s owned by surviving worker %s remapped to %s", f, before, after)
		}
		if before == "w-c" {
			moved++
		}
	}
	if moved == 0 || moved == len(files) {
		t.Fatalf("implausible ownership distribution: %d of %d on w-c", moved, len(files))
	}
}

// Degenerate shapes fall back: no workers, few files (single-file shard
// fan-outs must not serialize onto one owner), kill switch off.
func TestAffineFileSetsFallbacks(t *testing.T) {
	if sets, _ := affineFileSets(testFiles(63), nil, 12); sets != nil {
		t.Fatal("no workers must fall back")
	}
	if sets, _ := affineFileSets(testFiles(4), []string{"a", "b", "c"}, 4); sets != nil {
		t.Fatal("fewer than 2x workers files must fall back")
	}
	old := scanAffinityEnabled
	scanAffinityEnabled = false
	defer func() { scanAffinityEnabled = old }()
	if sets, _ := affineFileSets(testFiles(63), []string{"a", "b"}, 8); sets != nil {
		t.Fatal("kill switch must fall back")
	}
}

// The scheduler honors the hint only for connected workers with batch
// headroom; everything else falls through to the existing placement.
func TestPickAffinityWorkerFrom(t *testing.T) {
	connected := []string{"w-a", "w-b", "w-c"}
	if id, ok := pickAffinityWorkerFrom("w-b", connected, map[string]int{}, 12); !ok || id != "w-b" {
		t.Fatalf("hint not honored: %s %t", id, ok)
	}
	if _, ok := pickAffinityWorkerFrom("w-x", connected, map[string]int{}, 12); ok {
		t.Fatal("disconnected worker must fall through")
	}
	if _, ok := pickAffinityWorkerFrom("w-b", connected, map[string]int{"w-b": 4}, 12); ok {
		t.Fatal("same-batch cap must fall through")
	}
	if _, ok := pickAffinityWorkerFrom("", connected, map[string]int{}, 12); ok {
		t.Fatal("empty hint must fall through")
	}
}
