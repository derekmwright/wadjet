package coordinator

import (
	"reflect"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// TestAggregatePartialSplit_Trigger verifies the dispatcher's decision to
// fan out a DistRoundRobin partial aggregate: triggers only for the
// partial "aggregate" stage type with the RoundRobin label, spare workers,
// the 1-task default, a single non-eager dep, and a divisible upstream.
// Mirrors the conditions enforced in dispatchComputeStage.
func TestAggregatePartialSplit_Trigger(t *testing.T) {
	rrStage := physical.Stage{
		Type:         physical.StageAggregate,
		GroupByCols:  []string{"k"},
		Dependencies: []string{"join-1"},
		Distribution: physical.Distribution{Kind: physical.DistRoundRobin},
	}
	partitioned := map[string]StageOutput{
		"join-1": {Kind: OutputPartitioned, NumPartitions: 4,
			Files: [][]string{{"p0a", "p0b"}, {"p1"}, nil, {"p3"}}},
	}
	singlePart := map[string]StageOutput{
		"join-1": {Kind: OutputSinglePart, Files: [][]string{{"f0", "f1", "f2", "f3"}}},
	}

	withField := func(mut func(*physical.Stage)) physical.Stage {
		s := rrStage
		mut(&s)
		return s
	}

	cases := []struct {
		name       string
		stage      physical.Stage
		inputs     map[string]StageOutput
		workers    int
		curTasks   int
		wantOK     bool
		wantGroups [][]string
	}{
		{
			// Partitioned upstream: one group per NON-EMPTY partition, so
			// no task is dispatched with zero input files.
			name: "partitioned_upstream_group_per_partition", stage: rrStage,
			inputs: partitioned, workers: 3, curTasks: 1, wantOK: true,
			wantGroups: [][]string{{"p0a", "p0b"}, {"p1"}, {"p3"}},
		},
		{
			// Single-part multi-file upstream: even split capped at workerCount.
			name: "single_part_even_split", stage: rrStage,
			inputs: singlePart, workers: 2, curTasks: 1, wantOK: true,
			wantGroups: [][]string{{"f0", "f1"}, {"f2", "f3"}},
		},
		{name: "single_worker", stage: rrStage, inputs: partitioned, workers: 1, curTasks: 1, wantOK: false},
		{name: "already_parallel", stage: rrStage, inputs: partitioned, workers: 3, curTasks: 4, wantOK: false},
		{name: "single_file_upstream", stage: rrStage, inputs: map[string]StageOutput{
			"join-1": {Kind: OutputSinglePart, Files: [][]string{{"f0"}}},
		}, workers: 3, curTasks: 1, wantOK: false},
		{name: "one_nonempty_partition", stage: rrStage, inputs: map[string]StageOutput{
			"join-1": {Kind: OutputPartitioned, NumPartitions: 2, Files: [][]string{{"p0"}, nil}},
		}, workers: 3, curTasks: 1, wantOK: false},
		{name: "missing_dep_output", stage: rrStage, inputs: map[string]StageOutput{}, workers: 3, curTasks: 1, wantOK: false},
		{
			name:   "not_roundrobin",
			stage:  withField(func(s *physical.Stage) { s.Distribution = physical.Distribution{Kind: physical.DistSingleton} }),
			inputs: partitioned, workers: 3, curTasks: 1, wantOK: false,
		},
		{
			name:   "final_aggregate_excluded",
			stage:  withField(func(s *physical.Stage) { s.Type = "final_aggregate" }),
			inputs: partitioned, workers: 3, curTasks: 1, wantOK: false,
		},
		{
			// A sort, limit, or post-filter applied per-slice would be wrong
			// before the merge has seen all partials.
			name:   "sort_keys_excluded",
			stage:  withField(func(s *physical.Stage) { s.SortKeys = []physical.SortKeySpec{{Column: "k"}} }),
			inputs: partitioned, workers: 3, curTasks: 1, wantOK: false,
		},
		{
			name:   "limit_excluded",
			stage:  withField(func(s *physical.Stage) { s.Limit = 10 }),
			inputs: partitioned, workers: 3, curTasks: 1, wantOK: false,
		},
		{
			name:   "post_filter_excluded",
			stage:  withField(func(s *physical.Stage) { s.FilterExprs = []string{"cnt > 3"} }),
			inputs: partitioned, workers: 3, curTasks: 1, wantOK: false,
		},
		{
			name:   "multi_dep_excluded",
			stage:  withField(func(s *physical.Stage) { s.Dependencies = []string{"join-1", "join-2"} }),
			inputs: partitioned, workers: 3, curTasks: 1, wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dep, groups, ok := aggregatePartialSplit(tc.stage, tc.inputs, tc.workers, tc.curTasks)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if dep != "join-1" {
				t.Errorf("depID = %q, want join-1", dep)
			}
			if !reflect.DeepEqual(groups, tc.wantGroups) {
				t.Errorf("groups = %v, want %v", groups, tc.wantGroups)
			}
		})
	}
}

// TestAggregatePartialSplit_EagerExcluded: provisional (eager) upstream
// outputs use the manifest-fed partition-range convention, which does not
// match custom file groups — the split must decline.
func TestAggregatePartialSplit_EagerExcluded(t *testing.T) {
	stage := physical.Stage{
		Type:         physical.StageAggregate,
		GroupByCols:  []string{"k"},
		Dependencies: []string{"join-1"},
		Distribution: physical.Distribution{Kind: physical.DistRoundRobin},
	}
	inputs := map[string]StageOutput{
		"join-1": {Kind: OutputPartitioned, NumPartitions: 2,
			Files: [][]string{{"p0"}, {"p1"}}, eager: &eagerFeed{}},
	}
	if _, _, ok := aggregatePartialSplit(stage, inputs, 3, 1); ok {
		t.Fatal("split must decline eager provisional inputs")
	}
}
