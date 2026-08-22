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

// TestAggregateRowBoundTotalAgreesWithDispatchGuard pins
// aggregateRowBoundTotal (the dispatch log line's Σ-over-all-tasks
// computation) against the guard canMigrateAggregate's per-task rowBound
// uses: both must agree that a round-robin fan-out stage has NO usable
// bound, and the total must sum across every task rather than sampling
// task 0 — otherwise an empty partition 0 reads as "no bound" in the log
// even though every other task has one.
func TestAggregateRowBoundTotalAgreesWithDispatchGuard(t *testing.T) {
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

	t.Run("round-robin fan-out yields no bound", func(t *testing.T) {
		rows := make([]int64, 4)
		for i := range rows {
			rows[i] = 1_000_000
		}
		rrAggGroups := [][]string{{"a"}, {"b"}, {"c"}, {"d"}}
		total, tasksWithBound, ok := aggregateRowBoundTotal(stage,
			map[string]StageOutput{"repartition-11": partitioned(rows)},
			4, nil, false, rrAggGroups, nil)
		if ok {
			t.Fatalf("ok = true for a round-robin fan-out stage; want false (rrAggGroups slices files, not partition ranges)")
		}
		if total != 0 || tasksWithBound != 0 {
			t.Fatalf("total=%d tasksWithBound=%d, want 0,0 when ok is false", total, tasksWithBound)
		}
	})

	t.Run("probe-affine, probe-split and skew-split also yield no bound", func(t *testing.T) {
		base := map[string]StageOutput{"repartition-11": partitioned([]int64{10, 20, 30, 40})}
		if _, _, ok := aggregateRowBoundTotal(stage, base, 4, [][]string{{"a"}}, false, nil, nil); ok {
			t.Error("probeSets set: ok should be false")
		}
		if _, _, ok := aggregateRowBoundTotal(stage, base, 4, nil, true, nil, nil); ok {
			t.Error("probeSplit set: ok should be false")
		}
		if _, _, ok := aggregateRowBoundTotal(stage, base, 4, nil, false, nil, []skewTaskAssignment{{group: 0}}); ok {
			t.Error("skewAssign set: ok should be false")
		}
	})

	t.Run("empty partition 0 still yields a nonzero total", func(t *testing.T) {
		const numPartitions = 24
		rows := make([]int64, numPartitions)
		rows[0] = 0 // e.g. a hash bucket nothing landed in
		for i := 1; i < numPartitions; i++ {
			rows[i] = 260_000 // 23 * 260_000 = 5_980_000
		}
		total, tasksWithBound, ok := aggregateRowBoundTotal(stage,
			map[string]StageOutput{"repartition-11": partitioned(rows)},
			numPartitions, nil, false, nil, nil)
		if !ok {
			t.Fatal("ok = false for a plain partition-range stage; want true")
		}
		const want = 23 * 260_000
		if total != want {
			t.Errorf("total = %d, want %d", total, want)
		}
		if tasksWithBound != 23 {
			t.Errorf("tasksWithBound = %d, want 23 (task 0's bound is 0, the rest aren't)", tasksWithBound)
		}
		// The old w=0-only sample would have logged nothing at all here.
		if b := aggregateInputRowBound(stage, map[string]StageOutput{"repartition-11": partitioned(rows)}, 0, numPartitions); b != 0 {
			t.Fatalf("test setup: task 0's own bound should be 0, got %d", b)
		}
	})
}

// TestAggregateInputRowBoundExactCover: aggregateInputRowBound's per-task
// slicing (partitionRangeForWorker) must be an exact, non-overlapping,
// gap-free cover of every partition — summing it across every task in
// [0, numTasks) must reproduce the upstream stage's total PartitionRows,
// including when numPartitions < numTasks (some tasks get an empty range).
func TestAggregateInputRowBoundExactCover(t *testing.T) {
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
		Type:         "final_aggregate",
		Dependencies: []string{"repartition-11"},
	}

	cases := []struct {
		name          string
		numPartitions int
		numTasks      int
	}{
		{"tasks divide partitions evenly", 10, 2},
		{"tasks divide with remainder", 10, 3},
		{"one task per partition", 4, 4},
		{"more tasks than partitions", 3, 5},
		{"far more tasks than partitions", 3, 8},
		{"single task reads everything", 7, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]int64, tc.numPartitions)
			var want int64
			for p := range rows {
				rows[p] = int64(11 + 7*p)
				want += rows[p]
			}
			inputs := map[string]StageOutput{"repartition-11": partitioned(rows)}
			seen := make([]bool, tc.numPartitions)
			var got int64
			for w := 0; w < tc.numTasks; w++ {
				got += aggregateInputRowBound(stage, inputs, w, tc.numTasks)
				start, end := partitionRangeForWorker(tc.numPartitions, w, tc.numTasks)
				for p := start; p < end; p++ {
					if seen[p] {
						t.Fatalf("partition %d covered by more than one task", p)
					}
					seen[p] = true
				}
			}
			if got != want {
				t.Errorf("summed bound = %d, want %d (sum of PartitionRows)", got, want)
			}
			for p, s := range seen {
				if !s {
					t.Errorf("partition %d never covered by any task", p)
				}
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
