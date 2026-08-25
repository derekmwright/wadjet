package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/wshf"
)

// TestContainerGroupKeySpillsOnAWorker is the STAGE-DAG half of #566/#576's
// gate: a GROUP BY over an ARRAY, ROW, MAP or VECTOR column, run as a worker
// stage task under a 1 KiB shared budget, must answer exactly what the same
// task answers with no budget at all.
//
// It exists because the embedded-API arm cannot force this. A morsel-parallel
// clone charges a TRACKING-ONLY SpillManager view, whose ShouldSpillFor is
// hard-wired to false, so an in-process container GROUP BY reaches a drain
// only through the clone-to-primary handoff — which the three row-reader
// container columns never take. Under a worker's SharedSpillMgr the budget is
// real, so this is where "past a spill" is actually gated for all four types.
//
// ContainerKeyDrainWrites is asserted to move: a gate over a spill path that
// silently stopped spilling would keep passing while testing nothing, which
// is exactly what the first version of the embedded arm did.
func TestContainerGroupKeySpillsOnAWorker(t *testing.T) {
	for _, tc := range []struct {
		name string
		col  parquet.Column
		val  func(g int64) any
	}{
		{
			name: "array of string",
			col: parquet.Column{Name: "k", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
			val: func(g int64) any { return []any{fmt.Sprintf("a%05d", g), nil} },
		},
		{
			name: "row with a string field",
			col: parquet.Column{Name: "k", Type: parquet.TypeRow, Nullable: true,
				Fields: []parquet.Column{
					{Name: "n", Type: parquet.TypeInt64, Nullable: true},
					{Name: "s", Type: parquet.TypeString, Nullable: true},
				}},
			val: func(g int64) any { return map[string]any{"n": g, "s": fmt.Sprintf("s%05d", g)} },
		},
		{
			name: "map",
			col: parquet.Column{Name: "k", Type: parquet.TypeMap, Nullable: true,
				ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
					{Name: "key", Type: parquet.TypeString},
					{Name: "value", Type: parquet.TypeInt64, Nullable: true},
				}}},
			val: func(g int64) any { return map[string]any{fmt.Sprintf("k%05d", g): g} },
		},
		{
			name: "vector",
			col:  parquet.Column{Name: "k", Type: parquet.TypeVector, Nullable: true, Dimension: 3},
			val:  func(g int64) any { return []float32{float32(g), float32(g) + 0.5, -float32(g)} },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			// Enough rows to cross several 2048-row batch boundaries: the
			// pressure check runs at the TOP of Consume, so a fixture that
			// fits in one batch never reaches it however small the budget.
			const numGroups, perGroup = 200, 125
			// A LEADING int64 group column puts the NULL container key in the
			// middle of the merge order rather than last, which is where the
			// offsets-on-NULL desync shows.
			schema := []parquet.Column{
				{Name: "a", Type: parquet.TypeInt64, Nullable: true},
				tc.col,
				{Name: "v", Type: parquet.TypeInt64, Nullable: true},
			}
			// One CHUNK per pass, not one big one: the shuffle reader hands
			// a chunk to the pipeline as a batch, and the aggregate's
			// pressure check runs once per Consume.
			chunks := make([][]map[string]any, 0, perGroup)
			for r := 0; r < perGroup; r++ {
				rows := make([]map[string]any, 0, numGroups)
				for g := int64(0); g < numGroups; g++ {
					var k any
					if g%17 != 3 {
						k = tc.val(g)
					}
					rows = append(rows, map[string]any{"a": g, "k": k, "v": g*10 + int64(r)})
				}
				chunks = append(chunks, rows)
			}
			data := containerWshf(t, schema, chunks)

			run := func(budget int64) ([]string, int64) {
				t.Helper()
				bucket := "cgk-" + tc.name
				store := objstore.NewMemStore()
				if err := store.MakeBucket(ctx, bucket); err != nil {
					t.Fatalf("MakeBucket: %v", err)
				}
				key := "in/cgk/t0.wshf"
				if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data),
					int64(len(data)), "application/octet-stream"); err != nil {
					t.Fatalf("Put: %v", err)
				}
				ex := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
				if budget > 0 {
					ex.SetMemoryBudget(budget, t.TempDir())
				}
				task := distributed.Task{
					ID: "cgk-0", QueryID: "q-cgk", StageID: "cgk", Type: distributed.TaskTypeStage,
					DataBucket: bucket, ResultBucket: bucket, ResultPrefix: "out/cgk/",
					Operators: []distributed.OpSpec{
						{Type: distributed.OpScan, InputAlias: "src", InputFiles: []string{key},
							InputBucket: bucket, Columns: []string{"a", "k", "v"}},
						{Type: distributed.OpHashAggregate, GroupByCols: []string{"a", "k"},
							Aggregates: []distributed.AggSpec{{Func: "sum", InputCol: "v", OutputCol: "total"}}},
						{Type: distributed.OpUnpartitionedSink},
					},
				}
				before := exec.ContainerKeyDrainWrites.Load()
				result := &distributed.ResultNotification{TaskID: task.ID}
				if err := ex.executeStage(ctx, task, result); err != nil {
					t.Fatalf("executeStage (budget=%d): %v", budget, err)
				}
				drains := exec.ContainerKeyDrainWrites.Load() - before
				return readStageRows(ctx, t, store, bucket, result), drains
			}

			want, wantDrains := run(0)
			if len(want) != numGroups {
				t.Fatalf("unbudgeted run produced %d groups, want %d", len(want), numGroups)
			}
			got, gotDrains := run(1024)
			if gotDrains == 0 {
				t.Fatalf("the budgeted run wrote NO container group key to a partial-state run "+
					"(unbudgeted wrote %d) — this gate would pass with the drain path deleted. "+
					"Find out what stopped spilling before relaxing it.", wantDrains)
			}
			if len(got) != len(want) {
				t.Fatalf("budgeted run produced %d groups, want %d", len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("row %d differs (%d drains)\n  budgeted:   %s\n  unbudgeted: %s",
						i, gotDrains, got[i], want[i])
				}
			}
		})
	}
}

// containerWshf writes rows in the shuffle format, going through
// batch.FromRows so the container vectors are built exactly as a scan builds
// them.
func containerWshf(t *testing.T, schema []parquet.Column, chunks [][]map[string]any) []byte {
	t.Helper()
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatalf("writeHeader: %v", err)
	}
	for _, rows := range chunks {
		b := batch.FromRows(schema, rows)
		if err := sw.writeChunk(b.Columns, nil, b.Len); err != nil {
			t.Fatalf("writeChunk: %v", err)
		}
	}
	data := buf.Bytes()
	data[4] = byte(sw.numChunks)
	return data
}

// readStageRows reads back every row the stage sink wrote, rendered and
// sorted so container values (not comparable, not orderable) can be compared.
func readStageRows(ctx context.Context, t *testing.T, store objstore.Store, bucket string,
	result *distributed.ResultNotification) []string {
	t.Helper()
	var out []string
	for _, f := range result.ResultFiles {
		rc, _, err := store.Get(ctx, bucket, f)
		if err != nil {
			t.Fatalf("get %s: %v", f, err)
		}
		raw, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		r, err := wshf.NewChunkReader(raw)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		for {
			b, err := r.Next()
			if err != nil {
				t.Fatalf("chunk next: %v", err)
			}
			if b == nil {
				break
			}
			for _, row := range b.ToRows() {
				out = append(out, fmt.Sprintf("a=%v k=%v total=%v", row["a"], row["k"], row["total"]))
			}
		}
	}
	sort.Strings(out)
	return out
}
