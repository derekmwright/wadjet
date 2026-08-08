package coordinator

import (
	"reflect"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/physical"
)

func TestCollectInputs(t *testing.T) {
	outputs := map[string]StageOutput{
		"scan-0":   {Kind: OutputSinglePart, Files: [][]string{{"tables/o/p.parquet"}}},
		"repart-0": {Kind: OutputPartitioned, NumPartitions: 4, Files: make([][]string, 4)},
	}
	stage := physical.Stage{ID: "join-0", Dependencies: []string{"scan-0", "repart-0"}}

	got, err := collectInputs(stage, outputs)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !reflect.DeepEqual(got["scan-0"], outputs["scan-0"]) {
		t.Errorf("scan-0: got %+v want %+v", got["scan-0"], outputs["scan-0"])
	}
	if !reflect.DeepEqual(got["repart-0"], outputs["repart-0"]) {
		t.Errorf("repart-0: got %+v want %+v", got["repart-0"], outputs["repart-0"])
	}

	missing := physical.Stage{ID: "j", Dependencies: []string{"unknown"}}
	if _, err := collectInputs(missing, outputs); err == nil {
		t.Error("expected error for unknown dep")
	}
}

func TestPartitionFilesForWorker_Partitioned(t *testing.T) {
	out := StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: 8,
		Files: [][]string{
			{"p0.wshf"}, {"p1.wshf"}, {"p2.wshf"}, {"p3.wshf"},
			{"p4.wshf"}, {"p5.wshf"}, {"p6.wshf"}, {"p7.wshf"},
		},
	}
	cases := []struct {
		name  string
		w, wc int
		want  []string
	}{
		{"worker 0 of 2", 0, 2, []string{"p0.wshf", "p1.wshf", "p2.wshf", "p3.wshf"}},
		{"worker 1 of 2", 1, 2, []string{"p4.wshf", "p5.wshf", "p6.wshf", "p7.wshf"}},
		{"worker 0 of 4", 0, 4, []string{"p0.wshf", "p1.wshf"}},
		{"worker 3 of 4 (absorbs remainder)", 3, 4, []string{"p6.wshf", "p7.wshf"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := partitionFilesForWorker(out, c.w, c.wc)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("got %v want %v", got, c.want)
			}
		})
	}
}

func TestPartitionFilesForWorker_RemainderAbsorbed(t *testing.T) {
	// 10 partitions across 3 workers: 3/3/4
	out := StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: 10,
		Files: [][]string{
			{"0"}, {"1"}, {"2"}, {"3"}, {"4"},
			{"5"}, {"6"}, {"7"}, {"8"}, {"9"},
		},
	}
	w0 := partitionFilesForWorker(out, 0, 3)
	w1 := partitionFilesForWorker(out, 1, 3)
	w2 := partitionFilesForWorker(out, 2, 3)
	if !reflect.DeepEqual(w0, []string{"0", "1", "2"}) {
		t.Errorf("w0 got %v", w0)
	}
	if !reflect.DeepEqual(w1, []string{"3", "4", "5"}) {
		t.Errorf("w1 got %v", w1)
	}
	if !reflect.DeepEqual(w2, []string{"6", "7", "8", "9"}) {
		t.Errorf("w2 (absorbs remainder) got %v", w2)
	}
}

func TestPartitionFilesForWorker_NonPartitioned(t *testing.T) {
	rep := StageOutput{Kind: OutputReplicated, Files: [][]string{{"broadcast.wshf"}}}
	if got := partitionFilesForWorker(rep, 0, 4); !reflect.DeepEqual(got, []string{"broadcast.wshf"}) {
		t.Errorf("replicated worker 0: got %v", got)
	}
	if got := partitionFilesForWorker(rep, 3, 4); !reflect.DeepEqual(got, []string{"broadcast.wshf"}) {
		t.Errorf("replicated worker 3 should get same broadcast files: got %v", got)
	}

	single := StageOutput{Kind: OutputSinglePart, Files: [][]string{{"out.wshf"}}}
	if got := partitionFilesForWorker(single, 0, 1); !reflect.DeepEqual(got, []string{"out.wshf"}) {
		t.Errorf("single-part: got %v", got)
	}
}

func TestPartitionFilesForWorker_EdgeCases(t *testing.T) {
	empty := StageOutput{Kind: OutputPartitioned, NumPartitions: 0}
	if got := partitionFilesForWorker(empty, 0, 2); got != nil {
		t.Errorf("empty partitions: got %v", got)
	}
	out := StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: 4,
		Files:         [][]string{{"0"}, {"1"}, {"2"}, {"3"}},
	}
	if got := partitionFilesForWorker(out, 0, 0); got != nil {
		t.Errorf("zero workers: got %v", got)
	}
	if got := partitionFilesForWorker(out, -1, 2); got != nil {
		t.Errorf("negative worker index: got %v", got)
	}
	if got := partitionFilesForWorker(out, 5, 2); got != nil {
		t.Errorf("worker index >= count: got %v", got)
	}
}

func TestFlattenStageFiles(t *testing.T) {
	out := StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: 3,
		Files:         [][]string{{"a", "b"}, {"c"}, {"d", "e"}},
	}
	got := flattenStageFiles(out)
	want := []string{"a", "b", "c", "d", "e"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}

	empty := StageOutput{Kind: OutputPartitioned, NumPartitions: 2, Files: [][]string{nil, nil}}
	if got := flattenStageFiles(empty); got != nil {
		t.Errorf("empty: got %v", got)
	}
}
