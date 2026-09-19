// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestTwoCoordinatorsPlanByTheirOwnBushyOption is the distributed half of
// #1223: two coordinators in ONE process, over one catalog and one worker
// pool, with opposite Config.BushyJoinReorder. Each must plan by its own.
//
// It fails at 6960cd27, where the option was a package atomic.Bool that
// nothing stored false: whichever coordinator asked for bushy enumeration
// enabled it for the other, and the answer the default one returned came
// from a join order it did not ask for.
//
// The fixture is the expanding chain — fact_a ⋈ fact_b is a many-to-many
// explosion and each fact carries a small dimension — so the optimal order
// is one only a bushy plan can express and the two readings differ.
func TestTwoCoordinatorsPlanByTheirOwnBushyOption(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the two-coordinator DAG gate in short mode")
	}
	ctx, plain, store := setupDistributed(t)
	cat := plain.catalog

	twoCols := func(a, b string) []parquet.Column {
		return []parquet.Column{
			{Name: a, Type: parquet.TypeInt64},
			{Name: b, Type: parquet.TypeInt64},
		}
	}
	const factRows = 1000
	factA := make([]map[string]any, factRows)
	factB := make([]map[string]any, factRows)
	for i := 0; i < factRows; i++ {
		factA[i] = map[string]any{"a_id": int64(i % 10), "a_x": int64(i)}
		factB[i] = map[string]any{"b_id": int64(i % 10), "b_y": int64(i)}
	}
	dimX := make([]map[string]any, 10)
	dimY := make([]map[string]any, 10)
	for i := 0; i < 10; i++ {
		dimX[i] = map[string]any{"x_id": int64(i), "x_v": int64(i)}
		dimY[i] = map[string]any{"y_id": int64(i), "y_v": int64(i)}
	}
	ingestTestData(t, ctx, store, cat, "fact_a", twoCols("a_id", "a_x"), factA)
	ingestTestData(t, ctx, store, cat, "dim_x", twoCols("x_id", "x_v"), dimX)
	ingestTestData(t, ctx, store, cat, "fact_b", twoCols("b_id", "b_y"), factB)
	ingestTestData(t, ctx, store, cat, "dim_y", twoCols("y_id", "y_v"), dimY)
	// The DP cost model reads per-column NDV, which only ANALYZE computes.
	for _, table := range []string{"fact_a", "dim_x", "fact_b", "dim_y"} {
		if _, err := cat.AnalyzeTable(ctx, table); err != nil {
			t.Fatalf("ANALYZE %s: %v", table, err)
		}
	}

	// The second coordinator: same NATS, same catalog, same workers, the
	// opposite option. Both fields Coordinator.New derives from the config
	// are set the same way here.
	bushy := New(Config{
		NATSUrl:          plain.config.NATSUrl,
		ResultBucket:     "test",
		BushyJoinReorder: true,
	}, cat, plain.nc, plain.js, plain.logger)

	const q = `SELECT count(*) AS n FROM fact_a ` +
		`JOIN dim_x ON a_x = x_id ` +
		`JOIN fact_b ON a_id = b_id ` +
		`JOIN dim_y ON b_y = y_id`

	run := func(t *testing.T, c *Coordinator) (string, int64) {
		t.Helper()
		before := logical.BushyJoinsPlanned.Load()
		res, err := c.ExecuteSQL(ctx, q)
		if err != nil {
			t.Fatalf("ExecuteSQL: %v", err)
		}
		defer res.Close()
		var rows []map[string]any
		for _, b := range res.Batches {
			rows = append(rows, batchRows(b)...)
		}
		if len(rows) != 1 {
			t.Fatalf("expanding chain returned %d rows, want 1", len(rows))
		}
		return fmt.Sprint(rows[0]["n"]), logical.BushyJoinsPlanned.Load() - before
	}

	// Interleaved, so neither ordering of the QUERIES can be what makes the
	// cells agree, and both orders of "who ran first".
	for round := 0; round < 2; round++ {
		bushyAnswer, bushyPlanned := run(t, bushy)
		plainAnswer, plainPlanned := run(t, plain)
		if bushyPlanned == 0 {
			t.Fatalf("round %d: the BushyJoinReorder:true coordinator planned no bushy join —"+
				" the fixture proves nothing", round)
		}
		if plainPlanned != 0 {
			t.Fatalf("round %d: the BushyJoinReorder:false coordinator planned %d bushy joins —"+
				" it is planning by another instance's setting", round, plainPlanned)
		}
		if bushyAnswer != plainAnswer {
			t.Fatalf("round %d: the two join orders answer differently: bushy %s vs left-deep %s",
				round, bushyAnswer, plainAnswer)
		}
		if bushyAnswer != "10" {
			t.Fatalf("round %d: expanding chain count = %s, want 10", round, bushyAnswer)
		}
	}
}

// TestAPipelineTaskCarriesTheCoordinatorsPlannerOption pins the ONE
// cross-process half of #1223. worker.Executor.executePipeline re-plans a
// whole query from Task.SQLText, and the coordinator chose that task's probe
// split and build sides from a plan made under its own option — so the
// option must reach the worker with the task rather than from the worker
// process's own configuration.
//
// The stamp lives at Scheduler.PublishTasks, the choke point every
// dispatcher passes through (seven sites build a pipeline task), so this
// asserts the choke point rather than the seven builders: a task that
// carries SQL text carries the option, and one that does not is untouched.
func TestAPipelineTaskCarriesTheCoordinatorsPlannerOption(t *testing.T) {
	for _, tc := range []struct {
		name       string
		coordinate bool
		task       distributed.Task
		want       bool
	}{
		{name: "a re-planned pipeline task under a bushy coordinator", coordinate: true,
			task: distributed.Task{Type: distributed.TaskTypePipeline, SQLText: "SELECT 1"}, want: true},
		{name: "the same task under a default coordinator", coordinate: false,
			task: distributed.Task{Type: distributed.TaskTypePipeline, SQLText: "SELECT 1"}, want: false},
		{name: "a fragment task carries no SQL text and re-plans nothing", coordinate: true,
			task: distributed.Task{Type: distributed.TaskTypeStage, StageType: "scan"}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := tc.task
			stampTaskPlannerOptions(&task, tc.coordinate)
			if task.BushyJoinReorder != tc.want {
				t.Fatalf("Task.BushyJoinReorder = %v, want %v", task.BushyJoinReorder, tc.want)
			}
			// …and it survives the wire, which is where the worker reads it.
			data, err := distributed.Marshal(task)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var back distributed.Task
			if err := distributed.Unmarshal(data, &back); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if back.BushyJoinReorder != tc.want {
				t.Fatalf("after the wire, Task.BushyJoinReorder = %v, want %v",
					back.BushyJoinReorder, tc.want)
			}
		})
	}
}
