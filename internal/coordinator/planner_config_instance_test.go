// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

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

// TestTheWireCarriesTheCoordinatorsPlannerOption is the END of the carrier:
// a coordinator built by New publishes a pipeline task, and the bytes that
// actually leave carry its option. It exists because the two cells below and
// the DAG bushy suite all survive the deletion of `Coordinator.New`'s
// `c.scheduler.BushyJoinReorder = cfg.BushyJoinReorder` — they reach the
// stamp another way — and a gate that survives the deletion of the thing it
// guards is not a gate (round-1 review, N3). This one fails there, because
// the only path from Config to the wire runs through that line.
func TestTheWireCarriesTheCoordinatorsPlannerOption(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the wire-carrier gate in short mode")
	}
	ctx, plain, _ := setupDistributed(t)
	bushy := New(Config{
		NATSUrl:          plain.config.NATSUrl,
		ResultBucket:     "test",
		BushyJoinReorder: true,
	}, plain.catalog, plain.nc, plain.js, plain.logger)

	var mu sync.Mutex
	seen := map[string][]byte{}
	sub, err := plain.nc.Subscribe("wadjet.tasks.>", func(m *nats.Msg) {
		var task distributed.Task
		if err := distributed.Unmarshal(m.Data, &task); err != nil {
			return
		}
		mu.Lock()
		seen[task.ID] = append([]byte(nil), m.Data...)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck // test teardown

	publish := func(c *Coordinator, id string) {
		t.Helper()
		task := distributed.Task{
			ID: id, QueryID: "q-" + id, StageID: "s-0",
			Type: distributed.TaskTypePipeline, SQLText: "SELECT 1",
			ResultBucket: "test", ResultPrefix: "queries/q-" + id + "/",
		}
		if err := c.scheduler.PublishTasks(ctx, []distributed.Task{task}); err != nil {
			t.Fatalf("PublishTasks(%s): %v", id, err)
		}
	}
	publish(bushy, "bushy")
	publish(plain, "plain")

	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, want := range []struct {
		id     string
		option bool
	}{{"bushy", true}, {"plain", false}} {
		data, ok := seen[want.id]
		if !ok {
			t.Fatalf("no published payload captured for the %s coordinator's task", want.id)
		}
		var back distributed.Task
		if err := distributed.Unmarshal(data, &back); err != nil {
			t.Fatalf("unmarshal %s: %v", want.id, err)
		}
		if back.BushyJoinReorder != want.option {
			t.Fatalf("the %s coordinator published a pipeline task with BushyJoinReorder=%v, want %v —"+
				" Config did not reach the wire (Coordinator.New wires the scheduler; #1223)",
				want.id, back.BushyJoinReorder, want.option)
		}
		// The key is omitted, not written false, so a worker predating the
		// field reads the shipped default.
		if !want.option && strings.Contains(string(data), "bushy_join_reorder") {
			t.Fatalf("the default coordinator's payload names bushy_join_reorder: %s", data)
		}
	}
}

// TestAPipelineTaskCarriesTheCoordinatorsPlannerOption pins the stamp itself:
// which tasks it touches, and that the value survives the wire both ways.
// The coordinator-to-wire half is TestTheWireCarriesTheCoordinatorsPlannerOption
// above — this one deliberately calls the stamp directly, so it stays a
// statement about the stamp's RULE rather than about the wiring.
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
