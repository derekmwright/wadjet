package coordinator

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// Probe-split affinity (scan_affinity.go probeSplitAffineSets): a
// broadcast_join's probe slice is grouped by rendezvous owner so each task
// reads files its worker's NVMe cache already holds. Before 2026-08-22 the
// slice was splitFilesEvenly + binpack, and every file that landed on a
// non-owner cost a whole-object peer transfer on the task's critical path
// (SF100 window-3: Q09/Q08/Q17 join-* stragglers, 2-3 s per file).

func withProbeSplitAffinity(t *testing.T, on bool) {
	t.Helper()
	prev := probeSplitAffinity.Set(on)
	oldScan := scanAffinityEnabled
	scanAffinityEnabled = true
	t.Cleanup(func() {
		probeSplitAffinity.Set(prev)
		scanAffinityEnabled = oldScan
	})
}

func TestProbeSplitAffineSetsGroupByOwner(t *testing.T) {
	withProbeSplitAffinity(t, true)
	files := testFiles(12)
	workers := []string{"w-c", "w-a", "w-b"}
	sets, owners, _ := probeSplitAffineSets(files, nil, workers)
	if sets == nil || len(sets) != len(owners) {
		t.Fatalf("sets missing/misaligned: %d vs %d", len(sets), len(owners))
	}
	if len(sets) > len(workers) {
		t.Fatalf("%d sets for %d workers: probe-split must stay at most one task per worker", len(sets), len(workers))
	}
	seen := map[string]string{}
	sorted := []string{"w-a", "w-b", "w-c"}
	for i, set := range sets {
		if len(set) == 0 {
			t.Fatalf("set %d for %s is empty", i, owners[i])
		}
		for _, f := range set {
			if prev, dup := seen[f]; dup {
				t.Fatalf("file %s in two tasks (%s, %s)", f, prev, owners[i])
			}
			seen[f] = owners[i]
			if got := affinityOwner(f, sorted); got != owners[i] {
				t.Fatalf("task owner %s != rendezvous owner %s for %s", owners[i], got, f)
			}
		}
	}
	if len(seen) != len(files) {
		t.Fatalf("coverage: %d of %d files assigned", len(seen), len(files))
	}
	// Same grouping regardless of worker-list order: the same file lands
	// on the same owner every dispatch, which is what lets the caches
	// converge instead of re-drawing the transfer lottery per query.
	sets2, owners2, _ := probeSplitAffineSets(files, nil, []string{"w-b", "w-c", "w-a"})
	if !reflect.DeepEqual(sets, sets2) || !reflect.DeepEqual(owners, owners2) {
		t.Fatalf("grouping depends on worker-list order:\n%v %v\n%v %v", sets, owners, sets2, owners2)
	}
}

func TestProbeSplitAffineSetsFallbacks(t *testing.T) {
	files := testFiles(12)
	workers := []string{"w-a", "w-b", "w-c"}

	withProbeSplitAffinity(t, false)
	if sets, _, _ := probeSplitAffineSets(files, nil, workers); sets != nil {
		t.Fatalf("switch off must fall back to the even split, got %v", sets)
	}

	withProbeSplitAffinity(t, true)
	if sets, _, _ := probeSplitAffineSets(files, nil, nil); sets != nil {
		t.Fatalf("empty worker set must fall back, got %v", sets)
	}
	// A materialized / shuffled probe is not a base-table read: its files
	// carry peer-location hints and locality placement owns them.
	wshf := []string{"queries/q1/scan-0/part-0.wshf", "queries/q1/scan-0/part-1.wshf", "queries/q1/scan-0/part-2.wshf"}
	if sets, _, _ := probeSplitAffineSets(wshf, nil, workers); sets != nil {
		t.Fatalf("shuffle probe files must fall back, got %v", sets)
	}
	mixed := append(append([]string(nil), files[:6]...), "queries/q1/replicate/cache.parquet")
	if sets, _, _ := probeSplitAffineSets(mixed, nil, workers); sets != nil {
		t.Fatalf("query-scratch parquet must fall back, got %v", sets)
	}
	// Unlike scan fan-outs there is no 2×workers floor, but the grouping
	// must never yield fewer tasks than the even split
	// (min(len(workers), len(files))) would: a probe-split task's decode
	// parallelism is never reduced by affinity. Whether the 2-file probe
	// clears that floor depends on how many distinct owners the draw
	// produces, so compute it rather than assume either outcome.
	distinctOwners := map[string]bool{}
	for _, f := range files[:2] {
		distinctOwners[affinityOwner(f, workers)] = true
	}
	sets, owners, _ := probeSplitAffineSets(files[:2], nil, workers)
	switch len(distinctOwners) {
	case 2:
		if sets == nil || len(sets) != 2 {
			t.Fatalf("2 distinct owners over 2 files: expected exactly 2 sets, got %v (%v)", sets, owners)
		}
	case 1:
		if sets != nil {
			t.Fatalf("1 distinct owner over 2 files is below the even-split floor of min(3,2)=2: expected nil, got %v (%v)", sets, owners)
		}
	default:
		t.Fatalf("unexpected distinct owner count %d for 2 files", len(distinctOwners))
	}

	// testFiles(12) on 3 workers: the even-split floor is
	// min(3,12)=3, so the grouping must yield exactly 3 sets IF the draw
	// produces 3 distinct owners (computed, not assumed).
	distinct12 := map[string]bool{}
	for _, f := range files {
		distinct12[affinityOwner(f, workers)] = true
	}
	sets12, owners12, _ := probeSplitAffineSets(files, nil, workers)
	if len(distinct12) == 3 {
		if sets12 == nil || len(sets12) != 3 {
			t.Fatalf("3 distinct owners over 12 files: expected exactly 3 sets, got %v (%v)", sets12, owners12)
		}
	}
}

// Byte-balance shedding rides along when the pass-through scan carried
// catalog sizes: a skewed ownership draw stays inside the tolerance band.
func TestProbeSplitAffineSetsByteBalance(t *testing.T) {
	withProbeSplitAffinity(t, true)
	oldBal := affinityByteBalanceEnabled
	affinityByteBalanceEnabled = true
	t.Cleanup(func() { affinityByteBalanceEnabled = oldBal })
	files := testFiles(12)
	workers := []string{"w-a", "w-b", "w-c"}
	// Inflate the files one owner draws so the count-balanced split is
	// byte-skewed.
	owner0 := affinityOwner(files[0], workers)
	sizes := make([]int64, len(files))
	var total int64
	for i, f := range files {
		sizes[i] = 100 << 20
		if affinityOwner(f, workers) == owner0 {
			sizes[i] = 400 << 20
		}
		total += sizes[i]
	}
	sets, owners, bal := probeSplitAffineSets(files, sizes, workers)
	if sets == nil {
		t.Fatal("expected affine sets")
	}
	fair := float64(total) / float64(len(workers))
	bySet := map[string]int64{}
	idx := map[string]int{}
	for i, f := range files {
		idx[f] = i
	}
	for i, set := range sets {
		for _, f := range set {
			bySet[owners[i]] += sizes[idx[f]]
		}
	}
	if bal == nil {
		t.Fatalf("expected a shed on a skewed draw, shares %v", bySet)
	}
	for w, b := range bySet {
		if float64(b) > fair*(1+affinityBalanceTolerance)+float64(400<<20) {
			t.Fatalf("%s holds %d bytes, far above the band (fair %.0f); shares %v", w, b, fair, bySet)
		}
	}
}

// probeSplitTaskInputs binds the full build set plus the explicit probe
// slice; buildTaskInputsForBroadcastJoinSplitProbe remains the even-split
// front door.
func TestProbeSplitTaskInputs(t *testing.T) {
	stage := physical.Stage{
		ID:              "join-6",
		Type:            physical.StageBroadcastJoin,
		Dependencies:    []string{"scan-0", "build-1"},
		LeftDepStage:    "scan-0",
		RightDepStage:   "build-1",
		BuildTableAlias: "s",
	}
	probe := testFiles(6)
	inputs := map[string]StageOutput{
		"scan-0":  {Kind: OutputSinglePart, Files: [][]string{probe}},
		"build-1": {Kind: OutputReplicated, Files: [][]string{{"queries/q/build-1/cache.wshf"}}},
	}
	got, err := probeSplitTaskInputs(stage, inputs, probe[2:4])
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"s":     {"queries/q/build-1/cache.wshf"},
		"probe": probe[2:4],
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("inputs = %v, want %v", got, want)
	}
	// The even-split front door covers every file exactly once.
	var all []string
	for w := 0; w < 3; w++ {
		in, err := buildTaskInputsForBroadcastJoinSplitProbe(stage, inputs, w, 3)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, in["probe"]...)
	}
	sort.Strings(all)
	if fmt.Sprint(all) != fmt.Sprint(probe) {
		t.Fatalf("even split covers %v, want %v", all, probe)
	}
}
