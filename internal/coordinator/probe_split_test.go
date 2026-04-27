package coordinator

import (
	"reflect"
	"sort"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// TestBroadcastJoinProbeSplit_Trigger verifies the dispatcher's decision to
// fan out a Singleton broadcast_join: triggers only when the cluster has
// spare workers, the planner picked numTasks=1, and the probe upstream is a
// multi-file OutputSinglePart. Mirrors the conditions enforced in
// dispatchComputeStage.
func TestBroadcastJoinProbeSplit_Trigger(t *testing.T) {
	stage := physical.Stage{
		Type:          physical.StageBroadcastJoin,
		Dependencies:  []string{"probe", "build"},
		LeftDepStage:  "probe",
		RightDepStage: "build",
	}
	multi := map[string]StageOutput{
		"probe": {Kind: OutputSinglePart, Files: [][]string{{"p0", "p1", "p2", "p3"}}},
		"build": {Kind: OutputSinglePart, Files: [][]string{{"b0"}}},
	}

	cases := []struct {
		name      string
		stage     physical.Stage
		inputs    map[string]StageOutput
		workers   int
		curTasks  int
		wantOK    bool
		wantTasks int
	}{
		{name: "happy_path", stage: stage, inputs: multi, workers: 4, curTasks: 1, wantOK: true, wantTasks: 4},
		{name: "fewer_files_than_workers", stage: stage, inputs: map[string]StageOutput{
			"probe": {Kind: OutputSinglePart, Files: [][]string{{"p0", "p1"}}},
			"build": {Kind: OutputSinglePart, Files: [][]string{{"b0"}}},
		}, workers: 8, curTasks: 1, wantOK: true, wantTasks: 2},
		{name: "single_probe_file", stage: stage, inputs: map[string]StageOutput{
			"probe": {Kind: OutputSinglePart, Files: [][]string{{"p0"}}},
			"build": {Kind: OutputSinglePart, Files: [][]string{{"b0"}}},
		}, workers: 4, curTasks: 1, wantOK: false},
		{name: "single_worker", stage: stage, inputs: multi, workers: 1, curTasks: 1, wantOK: false},
		{name: "already_parallel", stage: stage, inputs: multi, workers: 4, curTasks: 4, wantOK: false},
		{name: "non_broadcast_stage", stage: physical.Stage{Type: physical.StageHashJoin, Dependencies: []string{"probe", "build"}, LeftDepStage: "probe", RightDepStage: "build"}, inputs: multi, workers: 4, curTasks: 1, wantOK: false},
		{name: "probe_partitioned", stage: stage, inputs: map[string]StageOutput{
			"probe": {Kind: OutputPartitioned, NumPartitions: 4, Files: [][]string{{"p0"}, {"p1"}, {"p2"}, {"p3"}}},
			"build": {Kind: OutputSinglePart, Files: [][]string{{"b0"}}},
		}, workers: 4, curTasks: 1, wantOK: false},
		{name: "probe_replicated", stage: stage, inputs: map[string]StageOutput{
			"probe": {Kind: OutputReplicated, Files: [][]string{{"p0", "p1"}}},
			"build": {Kind: OutputSinglePart, Files: [][]string{{"b0"}}},
		}, workers: 4, curTasks: 1, wantOK: false},
		{name: "missing_probe", stage: stage, inputs: map[string]StageOutput{
			"build": {Kind: OutputSinglePart, Files: [][]string{{"b0"}}},
		}, workers: 4, curTasks: 1, wantOK: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n, ok := broadcastJoinProbeSplit(tc.stage, tc.inputs, tc.workers, tc.curTasks)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && n != tc.wantTasks {
				t.Errorf("numTasks = %d, want %d", n, tc.wantTasks)
			}
		})
	}
}

// TestBuildTaskInputsForBroadcastJoinSplitProbe verifies the helper slices
// probe files across tasks while replicating the build set to every task.
func TestBuildTaskInputsForBroadcastJoinSplitProbe(t *testing.T) {
	stage := physical.Stage{
		ID:              "bj-1",
		Type:            physical.StageBroadcastJoin,
		Dependencies:    []string{"probe", "build"},
		LeftDepStage:    "probe",
		RightDepStage:   "build",
		BuildTableAlias: "n",
	}
	upstreams := map[string]StageOutput{
		"probe": {
			Kind:  OutputSinglePart,
			Files: [][]string{{"p0.parquet", "p1.parquet", "p2.parquet", "p3.parquet"}},
		},
		"build": {
			Kind:  OutputSinglePart,
			Files: [][]string{{"b0.wshf", "b1.wshf"}},
		},
	}

	const numTasks = 3
	got := make(map[int]map[string][]string)
	for w := 0; w < numTasks; w++ {
		in, err := buildTaskInputsForBroadcastJoinSplitProbe(stage, upstreams, w, numTasks)
		if err != nil {
			t.Fatalf("worker %d: %v", w, err)
		}
		got[w] = in
	}

	// Build alias must be present at every task with the full broadcast set.
	for w := 0; w < numTasks; w++ {
		if !reflect.DeepEqual(got[w]["n"], []string{"b0.wshf", "b1.wshf"}) {
			t.Errorf("worker %d build slot = %v, want full broadcast", w, got[w]["n"])
		}
	}

	// Probe alias must cover all 4 files exactly once across tasks (a partition).
	var seen []string
	for w := 0; w < numTasks; w++ {
		seen = append(seen, got[w]["probe"]...)
	}
	sort.Strings(seen)
	want := []string{"p0.parquet", "p1.parquet", "p2.parquet", "p3.parquet"}
	if !reflect.DeepEqual(seen, want) {
		t.Errorf("probe slices union = %v, want %v (each file exactly once)", seen, want)
	}

	// No two workers may receive the same probe file.
	dedup := make(map[string]int)
	for _, f := range seen {
		dedup[f]++
		if dedup[f] > 1 {
			t.Errorf("probe file %s assigned to multiple workers", f)
		}
	}
}

// TestBuildTaskInputsForBroadcastJoinSplitProbe_FewerFilesThanTasks: when probe
// has fewer files than numTasks, splitFilesEvenly returns fewer slices, and
// the helper must not panic for trailing workers.
func TestBuildTaskInputsForBroadcastJoinSplitProbe_FewerFilesThanTasks(t *testing.T) {
	stage := physical.Stage{
		ID:            "bj-1",
		Type:          physical.StageBroadcastJoin,
		Dependencies:  []string{"probe", "build"},
		LeftDepStage:  "probe",
		RightDepStage: "build",
	}
	upstreams := map[string]StageOutput{
		"probe": {Kind: OutputSinglePart, Files: [][]string{{"p0.parquet"}}},
		"build": {Kind: OutputSinglePart, Files: [][]string{{"b0.wshf"}}},
	}

	in0, err := buildTaskInputsForBroadcastJoinSplitProbe(stage, upstreams, 0, 4)
	if err != nil {
		t.Fatalf("worker 0: %v", err)
	}
	if !reflect.DeepEqual(in0["probe"], []string{"p0.parquet"}) {
		t.Errorf("worker 0 probe = %v, want [p0.parquet]", in0["probe"])
	}
	// Workers 1-3 should get empty probe (no out-of-bounds panic).
	for w := 1; w < 4; w++ {
		in, err := buildTaskInputsForBroadcastJoinSplitProbe(stage, upstreams, w, 4)
		if err != nil {
			t.Fatalf("worker %d: %v", w, err)
		}
		if len(in["probe"]) != 0 {
			t.Errorf("worker %d probe = %v, want empty", w, in["probe"])
		}
		// Build still replicated.
		if !reflect.DeepEqual(in["build"], []string{"b0.wshf"}) {
			t.Errorf("worker %d build = %v, want [b0.wshf]", w, in["build"])
		}
	}
}

// TestBuildTaskInputsForBroadcastJoinSplitProbe_MissingDeps: rejects stages
// without exactly two dependencies.
func TestBuildTaskInputsForBroadcastJoinSplitProbe_MissingDeps(t *testing.T) {
	stage := physical.Stage{
		ID:           "bj-1",
		Type:         physical.StageBroadcastJoin,
		Dependencies: []string{"only-one"},
	}
	if _, err := buildTaskInputsForBroadcastJoinSplitProbe(stage, nil, 0, 2); err == nil {
		t.Fatal("expected error for stage with 1 dep, got nil")
	}
}

// TestBuildTaskInputsForBroadcastJoinSplitProbe_BuildAliasFallback: when
// stage.BuildTableAlias is empty, the helper must fall back to "build".
func TestBuildTaskInputsForBroadcastJoinSplitProbe_BuildAliasFallback(t *testing.T) {
	stage := physical.Stage{
		ID:            "bj-1",
		Type:          physical.StageBroadcastJoin,
		Dependencies:  []string{"probe", "build"},
		LeftDepStage:  "probe",
		RightDepStage: "build",
	}
	upstreams := map[string]StageOutput{
		"probe": {Kind: OutputSinglePart, Files: [][]string{{"p0", "p1"}}},
		"build": {Kind: OutputSinglePart, Files: [][]string{{"b0"}}},
	}
	in, err := buildTaskInputsForBroadcastJoinSplitProbe(stage, upstreams, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := in["build"]; !reflect.DeepEqual(got, []string{"b0"}) {
		t.Errorf("default build alias missing, got map = %v", in)
	}
	if got := in["probe"]; !reflect.DeepEqual(got, []string{"p0"}) {
		t.Errorf("probe slice 0 = %v, want [p0]", got)
	}
}
