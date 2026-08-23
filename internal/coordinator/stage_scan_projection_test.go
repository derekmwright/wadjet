package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #410: a compute stage whose dependency is a pass-through leaf scan reads
// BASE-TABLE parquet through its OpShuffleSource, and that source carried no
// projection at all — so the DAG read every column of the table while the
// single-process path read the three the query named. That is a correctness
// difference and not only a bytes one: the scanner decides between the
// columnar decoder and the row fallback on the columns the read asks for, so
// one MAP or ARRAY column the query never mentions changes how the whole
// file is decoded.
func TestStageInputScanColumnsProjectsPassThroughScans(t *testing.T) {
	ctx, coord, _ := setupDistributed(t)

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "c_map", Type: parquet.TypeMap},
		{Name: "c_vec", Type: parquet.TypeVector},
	}
	if err := coord.catalog.CreateTable(ctx, "proj_test", parquet.Schema{Columns: schema}, nil); err != nil {
		t.Fatalf("creating table: %v", err)
	}

	// A pass-through leaf scan hands its projection down in StageOutput;
	// a compute upstream (its own .wshf output) has no ScanTable at all.
	leaf := StageOutput{
		Kind:        OutputSinglePart,
		Files:       [][]string{{"tables/proj_test/a.parquet", "tables/proj_test/b.parquet"}},
		ScanTable:   "proj_test",
		ScanColumns: []string{"id", "g", "c_vec", "not_a_column"},
	}
	compute := StageOutput{
		Kind:  OutputSinglePart,
		Files: [][]string{{"queries/q/agg-1/part-0.wshf"}},
	}
	upstreams := map[string]StageOutput{"scan-0": leaf, "agg-1": compute}

	t.Run("aggregate over a leaf scan", func(t *testing.T) {
		stage := physical.Stage{ID: "agg-2", Type: "aggregate", Dependencies: []string{"scan-0"}}
		got := coord.stageInputScanColumns(ctx, stage, upstreams)
		want := []string{"id", "g", "c_vec"} // "not_a_column" is not on the table
		assertCols(t, got["scan-0"], want)

		ops := []distributed.OpSpec{
			{Type: distributed.OpShuffleSource, InputAlias: "scan-0"},
			{Type: distributed.OpHashAggregate},
			{Type: distributed.OpUnpartitionedSink},
		}
		applySourceProjection(ops, got)
		assertCols(t, ops[0].Columns, want)
		if len(ops[1].Columns) != 0 {
			t.Fatalf("projection leaked onto a non-source op: %v", ops[1].Columns)
		}
	})

	t.Run("aggregate over a compute stage keeps reading whole", func(t *testing.T) {
		stage := physical.Stage{ID: "agg-2", Type: "final_aggregate", Dependencies: []string{"agg-1"}}
		got := coord.stageInputScanColumns(ctx, stage, upstreams)
		if len(got) != 0 {
			t.Fatalf("a .wshf upstream produced a projection: %v", got)
		}
	})

	t.Run("join sides are keyed the way the file map is", func(t *testing.T) {
		stage := physical.Stage{
			ID: "join-3", Type: physical.StageBroadcastJoin,
			Dependencies: []string{"scan-0", "agg-1"},
			LeftDepStage: "scan-0", RightDepStage: "agg-1",
		}
		cols := coord.stageInputScanColumns(ctx, stage, upstreams)
		files, err := buildTaskInputsForStage(stage, upstreams, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		for alias := range cols {
			if _, ok := files[alias]; !ok {
				t.Fatalf("projection alias %q is absent from the file map %v — the projection "+
					"would be silently dropped", alias, keysOf(files))
			}
		}
		assertCols(t, cols["probe"], []string{"id", "g", "c_vec"})
		if _, ok := cols["build"]; ok {
			t.Fatalf("the .wshf build side got a projection: %v", cols["build"])
		}
	})

	t.Run("an explicit projection is never overwritten", func(t *testing.T) {
		stage := physical.Stage{ID: "agg-2", Type: "aggregate", Dependencies: []string{"scan-0"}}
		ops := []distributed.OpSpec{
			{Type: distributed.OpScan, InputAlias: "scan-0", Columns: []string{"id"}},
		}
		applySourceProjection(ops, coord.stageInputScanColumns(ctx, stage, upstreams))
		assertCols(t, ops[0].Columns, []string{"id"})
	})
}

func assertCols(tb testing.TB, got, want []string) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("columns = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			tb.Fatalf("columns = %v, want %v", got, want)
		}
	}
}

func keysOf(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
