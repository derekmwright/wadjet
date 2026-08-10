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
	old := scanAffinityEnabled
	scanAffinityEnabled = true
	defer func() { scanAffinityEnabled = old }()
	files := testFiles(63)
	workers := []string{"w-b", "w-a", "w-c"}
	sets, owners, _ := affineFileSets(files, nil, workers, 12)
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
	sets2, owners2, _ := affineFileSets(files, nil, []string{"w-c", "w-a", "w-b"}, 12)
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
	oldOn := scanAffinityEnabled
	scanAffinityEnabled = true
	defer func() { scanAffinityEnabled = oldOn }()
	if sets, _, _ := affineFileSets(testFiles(63), nil, nil, 12); sets != nil {
		t.Fatal("no workers must fall back")
	}
	if sets, _, _ := affineFileSets(testFiles(4), nil, []string{"a", "b", "c"}, 4); sets != nil {
		t.Fatal("fewer than 2x workers files must fall back")
	}
	old := scanAffinityEnabled
	scanAffinityEnabled = false
	defer func() { scanAffinityEnabled = old }()
	if sets, _, _ := affineFileSets(testFiles(63), nil, []string{"a", "b"}, 8); sets != nil {
		t.Fatal("kill switch must fall back")
	}
}

// Byte-balance shedding: with sizes attached, no worker's byte load may
// exceed the tolerance band above the fair share (whole-file granularity
// permitting), unshed files keep their rendezvous owner, shed files land
// on a live worker, coverage is total, and the outcome is deterministic
// across call repetition and worker input order — the property that lets
// shed files converge warm in the recipients' caches across queries.
func TestAffineFileSetsByteBalance(t *testing.T) {
	oldAff, oldBal := scanAffinityEnabled, affinityByteBalanceEnabled
	scanAffinityEnabled, affinityByteBalanceEnabled = true, true
	defer func() { scanAffinityEnabled, affinityByteBalanceEnabled = oldAff, oldBal }()

	workers := []string{"w-a", "w-b", "w-c"}
	files := testFiles(63)
	// Real-shape sizes: ~283 MB parquet files with mild jitter, the SF100
	// lineitem profile whose rendezvous draw produced a 43/18/39% byte
	// split (2026-08-10 arm1).
	sizes := make([]int64, len(files))
	var total int64
	for i := range sizes {
		sizes[i] = 283<<20 + int64(i%7)*(1<<20)
		total += sizes[i]
	}

	assign := func(ws []string) (map[string]string, *affineBalance) {
		sets, owners, bal := affineFileSets(files, sizes, ws, 12)
		if sets == nil {
			t.Fatal("affine path not taken")
		}
		m := map[string]string{}
		for i, set := range sets {
			for _, f := range set {
				if prev, dup := m[f]; dup {
					t.Fatalf("file %s in two tasks (%s, %s)", f, prev, owners[i])
				}
				m[f] = owners[i]
			}
		}
		return m, bal
	}

	got, bal := assign(workers)
	if len(got) != len(files) {
		t.Fatalf("coverage: %d of %d files assigned", len(got), len(files))
	}
	// Per-worker byte loads inside the band (one-file slack: a move is
	// only refused when no recipient can take the file).
	fair := float64(total) / float64(len(workers))
	band := fair * (1 + affinityBalanceTolerance)
	loads := map[string]int64{}
	for i, f := range files {
		w := got[f]
		if w != "w-a" && w != "w-b" && w != "w-c" {
			t.Fatalf("file %s assigned to unknown worker %s", f, w)
		}
		loads[w] += sizes[i]
	}
	var maxSize int64 = 290 << 20
	for w, l := range loads {
		if float64(l) > band+float64(maxSize) {
			t.Fatalf("worker %s load %.2f GB exceeds band %.2f GB", w, float64(l)/1e9, (band+float64(maxSize))/1e9)
		}
	}
	// Unshed files keep their rendezvous owner; shed files went somewhere
	// else knowingly (balance reports them).
	shed := 0
	for _, f := range files {
		if got[f] != affinityOwner(f, workers) {
			shed++
		}
	}
	if bal == nil || bal.ShedFiles != shed || shed == 0 {
		t.Fatalf("balance stats mismatch: reported %+v, observed %d shed", bal, shed)
	}
	if bal.MaxShareAfter >= bal.MaxShareBefore {
		t.Fatalf("shedding did not improve max share: %+v", bal)
	}
	// Deterministic across repetition and worker input order.
	got2, _ := assign([]string{"w-c", "w-b", "w-a"})
	for f, w := range got {
		if got2[f] != w {
			t.Fatalf("assignment of %s changed across worker order: %s vs %s", f, w, got2[f])
		}
	}
}

// Small stages (< affinityBalanceMinBytes) never shed — the peer-fetch
// churn cannot pay for itself on latency-bound fan-outs — and the
// kill switch restores pure count-based grouping.
func TestAffineFileSetsByteBalanceGates(t *testing.T) {
	oldAff, oldBal := scanAffinityEnabled, affinityByteBalanceEnabled
	scanAffinityEnabled, affinityByteBalanceEnabled = true, true
	defer func() { scanAffinityEnabled, affinityByteBalanceEnabled = oldAff, oldBal }()

	workers := []string{"w-a", "w-b", "w-c"}
	files := testFiles(63)
	small := make([]int64, len(files))
	for i := range small {
		small[i] = 64 << 10 // 4 MB total, far under the gate
	}
	if _, _, bal := affineFileSets(files, small, workers, 12); bal != nil {
		t.Fatalf("sub-threshold stage shed anyway: %+v", bal)
	}

	big := make([]int64, len(files))
	for i := range big {
		big[i] = 283 << 20
	}
	affinityByteBalanceEnabled = false
	sets, owners, bal := affineFileSets(files, big, workers, 12)
	if bal != nil {
		t.Fatalf("kill switch did not disable shedding: %+v", bal)
	}
	for i, set := range sets {
		for _, f := range set {
			if owners[i] != affinityOwner(f, workers) {
				t.Fatalf("kill switch: file %s off its rendezvous owner", f)
			}
		}
	}
	// Misaligned sizes degrade to count-based behavior, never panic.
	affinityByteBalanceEnabled = true
	if _, _, bal := affineFileSets(files, big[:10], workers, 12); bal != nil {
		t.Fatalf("misaligned sizes shed anyway: %+v", bal)
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
