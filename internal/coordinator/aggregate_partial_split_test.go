package coordinator

import (
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// TestAggregatePartialSplit_Trigger verifies the dispatcher's decision to
// fan out a DistRoundRobin partial aggregate: triggers only for the
// partial "aggregate" stage type with the RoundRobin label, spare workers,
// the 1-task default, a single non-eager dep, a non-trivial upstream
// (>= aggSplitMinBytes reported bytes), and >= 2 upstream files. Task
// count caps at workerCount (probe-split shape) — the 2026-07-19 SF100
// run showed per-partition granularity produces swarms of ~2ms tasks
// whose scheduling overhead regresses the whole suite.
func TestAggregatePartialSplit_Trigger(t *testing.T) {
	rrStage := physical.Stage{
		Type:         physical.StageAggregate,
		GroupByCols:  []string{"k"},
		Dependencies: []string{"join-1"},
		Distribution: physical.Distribution{Kind: physical.DistRoundRobin},
	}
	big := int64(aggSplitMinBytes)
	partitioned := map[string]StageOutput{
		"join-1": {Kind: OutputPartitioned, NumPartitions: 4, Bytes: big,
			Files: [][]string{{"p0a", "p0b"}, {"p1"}, nil, {"p3"}}},
	}
	singlePart := map[string]StageOutput{
		"join-1": {Kind: OutputSinglePart, Bytes: big,
			Files: [][]string{{"f0", "f1", "f2", "f3"}}},
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
			// Partitioned upstream: flattened files split across at most
			// workerCount tasks — NOT one task per partition.
			name: "partitioned_upstream_capped_at_workers", stage: rrStage,
			inputs: partitioned, workers: 2, curTasks: 1, wantOK: true,
			wantGroups: [][]string{{"p0a", "p0b"}, {"p1", "p3"}},
		},
		{
			// Single-part multi-file upstream: even split capped at workerCount.
			name: "single_part_even_split", stage: rrStage,
			inputs: singlePart, workers: 2, curTasks: 1, wantOK: true,
			wantGroups: [][]string{{"f0", "f1"}, {"f2", "f3"}},
		},
		{
			// More workers than files: one task per file, no empty tasks.
			name: "more_workers_than_files", stage: rrStage,
			inputs: singlePart, workers: 8, curTasks: 1, wantOK: true,
			wantGroups: [][]string{{"f0"}, {"f1"}, {"f2"}, {"f3"}},
		},
		{
			// Below the bytes floor the single-task partial is already cheap;
			// splitting is pure scheduling overhead (the 2ms-task swarm).
			name: "small_upstream_excluded", stage: rrStage,
			inputs: map[string]StageOutput{
				"join-1": {Kind: OutputPartitioned, NumPartitions: 2, Bytes: aggSplitMinBytes - 1,
					Files: [][]string{{"p0"}, {"p1"}}},
			}, workers: 3, curTasks: 1, wantOK: false,
		},
		{
			// Unknown bytes (legacy worker) declines — degrade to the
			// pre-split shape, never to a regression.
			name: "unknown_bytes_excluded", stage: rrStage,
			inputs: map[string]StageOutput{
				"join-1": {Kind: OutputPartitioned, NumPartitions: 2,
					Files: [][]string{{"p0"}, {"p1"}}},
			}, workers: 3, curTasks: 1, wantOK: false,
		},
		{name: "single_worker", stage: rrStage, inputs: partitioned, workers: 1, curTasks: 1, wantOK: false},
		{name: "already_parallel", stage: rrStage, inputs: partitioned, workers: 3, curTasks: 4, wantOK: false},
		{name: "single_file_upstream", stage: rrStage, inputs: map[string]StageOutput{
			"join-1": {Kind: OutputSinglePart, Bytes: big, Files: [][]string{{"f0"}}},
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
			stage:  withField(func(s *physical.Stage) { s.Limit = 10; s.HasLimit = true }),
			inputs: partitioned, workers: 3, curTasks: 1, wantOK: false,
		},
		{
			// #481: a real LIMIT 0 must exclude the split exactly like any
			// other limit — HasLimit is what the guard now consults, never
			// `Limit != 0`, which would have missed this case entirely.
			name:   "limit_zero_excluded",
			stage:  withField(func(s *physical.Stage) { s.Limit = 0; s.HasLimit = true }),
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
		"join-1": {Kind: OutputPartitioned, NumPartitions: 2, Bytes: aggSplitMinBytes,
			Files: [][]string{{"p0"}, {"p1"}}, eager: &eagerFeed{}},
	}
	if _, _, ok := aggregatePartialSplit(stage, inputs, 3, 1); ok {
		t.Fatal("split must decline eager provisional inputs")
	}
}
