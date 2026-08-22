package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// The group-index layout of a DAG aggregate is decided from the EXACT number
// of rows its task will read (exec/two_level_hash.go, twoLevelAmortizeMultiple).
// aggregateInputRowBound is the one place that number is computed, and its
// contract is asymmetric: over-stating only keeps the adaptive path, but a
// bound that reads LOW where the truth is high pins a genuinely
// high-cardinality index flat. Every uncertain case must therefore return 0.
func TestAggregateInputRowBound(t *testing.T) {
	partitioned := func(rows []int64) StageOutput {
		files := make([][]string, len(rows))
		for i := range files {
			files[i] = []string{"p.wshf"}
		}
		return StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: len(rows),
			Files:         files,
			PartitionRows: rows,
		}
	}
	stage := physical.Stage{
		ID:           "final_aggregate-7",
		Type:         "final_aggregate",
		Dependencies: []string{"repartition-11"},
		GroupByCols:  []string{"l_orderkey"},
	}

	cases := []struct {
		name     string
		stage    physical.Stage
		inputs   map[string]StageOutput
		w        int
		numTasks int
		want     int64
	}{
		{
			// Q18's shape: one task per shuffle partition.
			name:     "one partition per task",
			stage:    stage,
			inputs:   map[string]StageOutput{"repartition-11": partitioned([]int64{10, 20, 30, 40})},
			w:        2,
			numTasks: 4,
			want:     30,
		},
		{
			name:     "contiguous partition range sums",
			stage:    stage,
			inputs:   map[string]StageOutput{"repartition-11": partitioned([]int64{10, 20, 30, 40})},
			w:        0,
			numTasks: 2,
			want:     30,
		},
		{
			// partitionRangeForWorker gives the last task the remainder;
			// the bound must follow the same slicing the inputs did.
			name:     "last task absorbs the remainder",
			stage:    stage,
			inputs:   map[string]StageOutput{"repartition-11": partitioned([]int64{10, 20, 30, 40, 50})},
			w:        1,
			numTasks: 2,
			want:     120,
		},
		{
			name:     "unpartitioned upstream is unknown",
			stage:    stage,
			inputs:   map[string]StageOutput{"repartition-11": {Kind: OutputSinglePart, Files: [][]string{{"a.wshf"}}}},
			w:        0,
			numTasks: 1,
			want:     0,
		},
		{
			name:     "legacy worker reported no partition rows",
			stage:    stage,
			inputs:   map[string]StageOutput{"repartition-11": {Kind: OutputPartitioned, NumPartitions: 4, Files: make([][]string, 4)}},
			w:        0,
			numTasks: 4,
			want:     0,
		},
		{
			name:     "partition-row vector misaligned with the layout",
			stage:    stage,
			inputs:   map[string]StageOutput{"repartition-11": {Kind: OutputPartitioned, NumPartitions: 4, Files: make([][]string, 4), PartitionRows: []int64{1, 2}}},
			w:        0,
			numTasks: 4,
			want:     0,
		},
		{
			name: "multiple dependencies do not compose",
			stage: physical.Stage{
				Type:         "final_aggregate",
				Dependencies: []string{"a", "b"},
			},
			inputs: map[string]StageOutput{
				"a": partitioned([]int64{10}),
				"b": partitioned([]int64{10}),
			},
			w: 0, numTasks: 1, want: 0,
		},
		{
			name:     "missing dependency output",
			stage:    stage,
			inputs:   map[string]StageOutput{},
			w:        0,
			numTasks: 1,
			want:     0,
		},
		{
			name:     "empty upstream is unknown, not zero-bound",
			stage:    stage,
			inputs:   map[string]StageOutput{"repartition-11": partitioned([]int64{0, 0})},
			w:        0,
			numTasks: 2,
			want:     0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := aggregateInputRowBound(tc.stage, tc.inputs, tc.w, tc.numTasks); got != tc.want {
				t.Fatalf("aggregateInputRowBound = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestAggregateFragmentCarriesRowBound: the bound has to reach the wire spec
// the worker reads, and only the aggregate op in the chain.
func TestAggregateFragmentCarriesRowBound(t *testing.T) {
	stage := physical.Stage{
		ID:           "final_aggregate-7",
		Type:         "final_aggregate",
		Dependencies: []string{"repartition-11"},
		GroupByCols:  []string{"l_orderkey"},
	}
	task := &distributed.Task{DataBucket: "buk"}
	aggs := []distributed.AggSpec{{Func: "sum", InputCol: "q", OutputCol: "q"}}
	ops, err := buildAggregateFragment(stage, task,
		map[string][]string{"repartition-11": {"p0.wshf"}}, aggs, nil, "", 6_250_000)
	if err != nil {
		t.Fatalf("buildAggregateFragment: %v", err)
	}
	seen := 0
	for _, op := range ops {
		if op.Type == distributed.OpHashAggregate {
			seen++
			if op.InputRowBound != 6_250_000 {
				t.Errorf("InputRowBound = %d, want 6250000", op.InputRowBound)
			}
		} else if op.InputRowBound != 0 {
			t.Errorf("%s op carries InputRowBound = %d, want 0", op.Type, op.InputRowBound)
		}
	}
	if seen != 1 {
		t.Fatalf("found %d hash-aggregate ops, want 1", seen)
	}

	// 0 in, absent on the wire: an unknown bound must stay unknown rather
	// than serialize as a bound of zero rows.
	ops, err = buildAggregateFragment(stage, task,
		map[string][]string{"repartition-11": {"p0.wshf"}}, aggs, nil, "", 0)
	if err != nil {
		t.Fatalf("buildAggregateFragment: %v", err)
	}
	for _, op := range ops {
		if op.InputRowBound != 0 {
			t.Fatalf("%s op carries InputRowBound = %d with no bound declared", op.Type, op.InputRowBound)
		}
	}
}
